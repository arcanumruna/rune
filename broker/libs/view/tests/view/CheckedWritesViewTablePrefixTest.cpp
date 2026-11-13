// Part 07 Task 6: the ViewTable prefix index (each prefix -> a hashset of the keys of its rows) and versioned scans.
#include <gtest/gtest.h>
#include <AllocWindow.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewKeyHash.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

using namespace rune::view;
using rune::codec::DescriptorBuilder;
using rune::codec::FieldValue;
using rune::codec::PrimType;

namespace {

RowLayout hpLayout() {
    return buildRowLayout(DescriptorBuilder("v", 1).add("id", PrimType::Int64, 1, true).add("hp", PrimType::Int32).build());
}
void setHp(uint8_t* row, const RowLayout& L, int32_t hp) {
    FieldValue v{};
    v.type = PrimType::Int32;
    v.present = true;
    v.i = hp;
    writeSlot(row, L, 1, v);
}

// [prefix bytes][suffix bytes]: `pre` fills the first prefixWidth bytes, `suf` the rest (little-endian, truncated).
ViewKey mkKey(size_t keyWidth, size_t prefixWidth, uint64_t pre, uint64_t suf) {
    ViewKey k;
    k.len = static_cast<uint8_t>(keyWidth);
    std::memcpy(k.bytes.data(), &pre, std::min<size_t>(prefixWidth, 8));
    std::memcpy(k.bytes.data() + prefixWidth, &suf, std::min<size_t>(keyWidth - prefixWidth, 8));
    return k;
}
std::string str(const ViewKey& k) { return std::string(reinterpret_cast<const char*>(k.bytes.data()), k.len); }
std::string str(std::span<const uint8_t> b) { return std::string(reinterpret_cast<const char*>(b.data()), b.size()); }
ViewKey prefixOf(const ViewKey& k, size_t pw) { return ViewKey::fromBytes({k.bytes.data(), pw}); }

using Token = std::array<uint8_t, kScanTokenBytes>;

// Scans up to `limit` rows from `resume`; appends the visited keys to `out`.
PrefixScan scanSome(const ViewTable& t, const ViewKey& prefix, std::span<const uint8_t> resume, Token& next, size_t limit,
                    std::vector<std::string>& out) {
    size_t taken = 0;
    return t.scanPrefix(prefix.span(), resume, next, [&](std::span<const uint8_t> key, const uint8_t* payload) {
        if (taken == limit) return false;
        EXPECT_EQ(t.get(ViewKey::fromBytes(key)), payload);
        ++taken;
        out.push_back(str(key));
        return true;
    }, [&](size_t group) { return taken + group <= limit; });   // equal hash-order positions are returned whole
}

std::set<std::string> scanAll(const ViewTable& t, const ViewKey& prefix, PrefixScan* result = nullptr) {
    std::vector<std::string> out;
    Token next{};
    // A page may end on the slot budget with no row limit reached: keep going from its token.
    PrefixScan r = scanSome(t, prefix, {}, next, SIZE_MAX, out);
    for (int guard = 0; r == PrefixScan::Stopped && guard < 100000; ++guard) {
        const Token resume = next;
        r = scanSome(t, prefix, resume, next, SIZE_MAX, out);
    }
    if (result) *result = r;
    std::set<std::string> s(out.begin(), out.end());
    EXPECT_EQ(s.size(), out.size()) << "a key was visited twice";
    return s;
}

struct Fixture {
    RowLayout L = hpLayout();
    size_t k, pw;
    ViewTable t;
    std::map<uint64_t, std::set<std::string>> model;   // prefix id -> keys
    Fixture(size_t keyWidth, size_t prefixWidth, uint64_t mask = ~uint64_t{0}, size_t slots = 16)
        : k(keyWidth), pw(prefixWidth),
          t(L, slots, 4096, ViewTableKeys{.keyWidth = static_cast<uint8_t>(keyWidth), .prefixWidth = static_cast<uint8_t>(prefixWidth), .hashSeed = 7, .hashMaskForTest = mask}) {}
    ViewKey key(uint64_t pre, uint64_t suf) const { return mkKey(k, pw, pre, suf); }
    void put(uint64_t pre, uint64_t suf) { t.put(key(pre, suf)); model[pre].insert(str(key(pre, suf))); }
    bool erase(uint64_t pre, uint64_t suf) {
        const bool had = t.erase(key(pre, suf));
        EXPECT_EQ(had, model[pre].erase(str(key(pre, suf))) == 1);
        return had;
    }
    void expectExact(uint64_t pre) {
        PrefixScan r;
        const auto got = scanAll(t, prefixOf(key(pre, 0), pw), &r);
        EXPECT_EQ(got, model[pre]) << "prefix " << pre;
        EXPECT_EQ(r, PrefixScan::End);
        EXPECT_EQ(t.prefixSetSizeForTest(prefixOf(key(pre, 0), pw).span()), model[pre].size());
    }
};

} // namespace

TEST(CheckedWritesViewTablePrefix, ScanReturnsExactlyThePrefixRows) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    for (uint64_t i = 0; i < 5; ++i) f.put(2, i);
    for (uint64_t i = 0; i < 300; ++i) f.put(3, i);
    PrefixScan r;
    EXPECT_EQ(scanAll(f.t, prefixOf(f.key(1, 0), 8), &r), f.model[1]);
    EXPECT_EQ(r, PrefixScan::End);
    f.expectExact(2);
    f.expectExact(3);
}

TEST(CheckedWritesViewTablePrefix, EmptyPrefixScansNothing) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    Token next{};
    bool visited = false;
    EXPECT_EQ(f.t.scanPrefix(prefixOf(f.key(99, 0), 8).span(), {}, next, [&](std::span<const uint8_t>, const uint8_t*) { visited = true; return true; }),
              PrefixScan::End);
    EXPECT_FALSE(visited);
}

TEST(CheckedWritesViewTablePrefix, ResumeOnAnUnchangedSetContinues) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 300; ++i) f.put(3, i);
    const ViewKey pre = prefixOf(f.key(3, 0), 8);
    std::vector<std::string> out;
    Token next{};
    PrefixScan r = scanSome(f.t, pre, {}, next, 100, out);
    ASSERT_EQ(r, PrefixScan::Stopped);
    EXPECT_EQ(out.size(), 100u);
    int pages = 1;
    while (r == PrefixScan::Stopped) {
        const Token resume = next;
        r = scanSome(f.t, pre, resume, next, 100, out);
        ++pages;
        ASSERT_LE(pages, 4);
    }
    EXPECT_EQ(r, PrefixScan::End);
    EXPECT_EQ(out.size(), 300u);
    EXPECT_EQ(std::set<std::string>(out.begin(), out.end()), f.model[3]);
}

TEST(CheckedWritesViewTablePrefix, ErasesAndInsertsKeepTheSetExact) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    for (uint64_t i = 0; i < 5; ++i) f.put(2, i);
    for (uint64_t i : {3, 7, 11}) {
        EXPECT_TRUE(f.erase(1, i));
        f.expectExact(1);
    }
    for (uint64_t i : {100, 101}) {
        f.put(1, i);
        f.expectExact(1);
    }
    EXPECT_FALSE(f.erase(1, 3));
    f.expectExact(1);
    f.expectExact(2);
}

TEST(CheckedWritesViewTablePrefix, NarrowCompositeTableHasAPrefixIndex) {
    Fixture f(6, 2);
    for (uint64_t p = 1; p <= 3; ++p)
        for (uint64_t i = 0; i < 20 * p; ++i) f.put(p, i);
    for (uint64_t p = 1; p <= 3; ++p) f.expectExact(p);
    EXPECT_TRUE(f.erase(2, 5));
    f.expectExact(2);
    const ViewKey k = f.key(3, 7);
    ASSERT_NE(f.t.get(k), nullptr);
    EXPECT_EQ(f.t.keyOfRow(f.t.get(k)).size(), 0u);   // the main index still compares the asU64() word
}

TEST(CheckedWritesViewTablePrefixRegression, RowsCarryNoIndexStateRegression) {
    for (size_t k : {size_t{16}, size_t{6}}) {
        const size_t pw = k == 16 ? 8 : 2;
        const RowLayout L = hpLayout();
        ViewTable on(L, 16, 4096, ViewTableKeys{.keyWidth = static_cast<uint8_t>(k), .prefixWidth = static_cast<uint8_t>(pw), .hashSeed = 3});
        ViewTable off(L, 16, 4096, ViewTableKeys{.keyWidth = static_cast<uint8_t>(k), .hashSeed = 3});
        for (uint64_t i = 0; i < 200; ++i) {
            const ViewKey key = mkKey(k, pw, i % 7, i);
            setHp(on.put(key), L, static_cast<int32_t>(i));
            setHp(off.put(key), L, static_cast<int32_t>(i));
        }
        for (uint64_t i = 0; i < 200; ++i) {
            const ViewKey key = mkKey(k, pw, i % 7, i);
            const uint8_t* a = on.get(key);
            const uint8_t* b = off.get(key);
            ASSERT_NE(a, nullptr);
            ASSERT_NE(b, nullptr);
            EXPECT_EQ(std::memcmp(a, b, L.rowLen), 0);
            EXPECT_EQ(on.keyOfRow(a).size(), k == 16 ? 16u : 0u);
            EXPECT_EQ(on.keyOfRow(a).size(), off.keyOfRow(b).size());
            if (k == 16) EXPECT_EQ(std::memcmp(on.keyOfRow(a).data(), key.bytes.data(), 16), 0);
        }
    }
}

TEST(CheckedWritesViewTablePrefixRegression, ChangesUnderThePrefixNeverInvalidateTheTokenRegression) {
    // Was: any insert or erase under the prefix invalidated the token.  Now the scan continues and keeps its
    // guarantee: every row that stayed under the prefix is returned exactly once.
    auto run = [](auto&& change, std::set<std::string> mustSee) {
        Fixture f(16, 8);
        for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
        for (uint64_t i = 0; i < 4; ++i) f.put(2, i);
        const ViewKey A = prefixOf(f.key(1, 0), 8);
        std::vector<std::string> out;
        Token next{};
        ASSERT_EQ(scanSome(f.t, A, {}, next, 4, out), PrefixScan::Stopped);
        const std::set<std::string> before(out.begin(), out.end());
        change(f);
        const Token resume = next;
        const PrefixScan r = scanSome(f.t, A, resume, next, SIZE_MAX, out);
        EXPECT_EQ(r, PrefixScan::End);
        const std::set<std::string> got(out.begin(), out.end());
        EXPECT_EQ(got.size(), out.size()) << "a row was returned twice";
        for (const std::string& k : mustSee) EXPECT_TRUE(got.count(k)) << "a stable row was skipped";
        for (const std::string& k : got) EXPECT_TRUE(f.model[1].count(k) || before.count(k)) << "a row that never existed";
    };
    auto stable = [](std::initializer_list<uint64_t> except) {
        Fixture f(16, 8);
        std::set<std::string> s;
        for (uint64_t i = 0; i < 12; ++i) s.insert(str(f.key(1, i)));
        for (uint64_t e : except) s.erase(str(f.key(1, e)));
        return s;
    };
    run([](Fixture& f) { f.put(1, 500); }, stable({}));
    run([](Fixture& f) { f.erase(1, 5); }, stable({5}));
    run([](Fixture& f) { f.erase(1, 5); f.put(1, 5); }, stable({5}));
    run([](Fixture& f) { f.t.put(f.key(1, 5)); }, stable({}));
    run([](Fixture& f) { f.put(2, 500); f.erase(2, 1); }, stable({}));
}

TEST(CheckedWritesViewTablePrefixRegression, ATokenIsAPositionNotAVersionRegression) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    for (uint64_t i = 0; i < 12; ++i) f.put(2, i);
    const ViewKey A = prefixOf(f.key(1, 0), 8);
    const ViewKey B = prefixOf(f.key(2, 0), 8);
    std::vector<std::string> out;
    Token tokA{}, tokB{}, scratch{};
    ASSERT_EQ(scanSome(f.t, A, {}, tokA, 4, out), PrefixScan::Stopped);
    ASSERT_EQ(scanSome(f.t, B, {}, tokB, 4, out), PrefixScan::Stopped);
    for (uint64_t i = 0; i < 12; ++i) f.erase(1, i);
    out.clear();
    EXPECT_EQ(scanSome(f.t, A, tokA, scratch, SIZE_MAX, out), PrefixScan::End);   // no set: nothing left, not an error
    EXPECT_TRUE(out.empty());
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    out.clear();
    EXPECT_EQ(scanSome(f.t, A, tokA, scratch, SIZE_MAX, out), PrefixScan::End);   // recreated: rows above the position
    EXPECT_EQ(std::set<std::string>(out.begin(), out.end()).size(), out.size());
    std::vector<std::string> all;
    EXPECT_EQ(scanSome(f.t, A, {}, scratch, SIZE_MAX, all), PrefixScan::End);
    EXPECT_EQ(all.size(), 12u);
    EXPECT_LE(out.size(), 12u);
    const std::array<uint8_t, 5> shortToken{};
    EXPECT_EQ(scanSome(f.t, A, shortToken, scratch, SIZE_MAX, out), PrefixScan::CursorGone);   // malformed length only
    f.expectExact(1);
}

TEST(CheckedWritesViewTablePrefixRegression, SetGrowsAndShrinksIncrementallyRegression) {
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 50; ++i) f.put(2, i);
    const ViewKey X = prefixOf(f.key(1, 0), 8);
    uint32_t maxCap = 0;
    int ops = 0;
    auto check = [&](uint64_t suf) {
        ++ops;
        maxCap = std::max(maxCap, f.t.prefixSetCapacityForTest(X.span()));
        EXPECT_LE(f.t.maxSetEntriesMovedPerOpForTest(), ViewTable::kSetMigrate);
        if (ops % 97 == 0) {
            for (const std::string& s : f.model[1]) ASSERT_NE(f.t.get(ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(s.data()), s.size()})), nullptr);
            f.expectExact(2);
        } else if (f.model[1].count(str(f.key(1, suf)))) {
            ASSERT_NE(f.t.get(f.key(1, suf)), nullptr);
        } else {
            ASSERT_EQ(f.t.get(f.key(1, suf)), nullptr);
        }
    };
    for (uint64_t i = 0; i < 5000; ++i) {
        f.put(1, i);
        check(i);
    }
    f.expectExact(1);
    EXPECT_GE(maxCap, 5000u);
    for (uint64_t i = 5000; i-- > 1;) {
        f.erase(1, i);
        check(i);
    }
    EXPECT_EQ(f.t.prefixSetSizeForTest(X.span()), 1u);
    EXPECT_EQ(f.t.prefixSetCapacityForTest(X.span()), 16u);
    f.expectExact(1);
    f.erase(1, 0);
    EXPECT_EQ(f.t.prefixSetSizeForTest(X.span()), 0u);
    EXPECT_EQ(f.t.prefixSetCapacityForTest(X.span()), 0u);
    PrefixScan r;
    EXPECT_TRUE(scanAll(f.t, X, &r).empty());
    EXPECT_EQ(r, PrefixScan::End);
    f.expectExact(2);
}

TEST(CheckedWritesViewTablePrefixRegression, PoolBlocksNeverMoveRegression) {
    Fixture f(16, 8);
    size_t chunks = f.t.prefixPoolChunksForTest();
    for (uint64_t p = 0; p < 2000; ++p) {
        for (uint64_t i = 0; i < 20; ++i) f.put(p, i);
        if (f.t.prefixPoolChunksForTest() != chunks) {
            chunks = f.t.prefixPoolChunksForTest();
            for (uint64_t q = 0; q <= p; q += 37) f.expectExact(q);
        }
    }
    EXPECT_GE(f.t.prefixPoolChunksForTest(), 2u);
    for (uint64_t p = 0; p < 2000; ++p) f.expectExact(p);
}

TEST(CheckedWritesViewTablePrefixRegression, CollidingPrefixesNeverMixRegression) {
    Fixture f(16, 8, /*mask*/ 0, /*slots*/ 16);
    for (uint64_t p = 0; p < 64; ++p)
        for (uint64_t i = 0; i < 8; ++i) f.put(p, i);
    for (uint64_t p = 0; p < 64; ++p) f.expectExact(p);
    for (uint64_t p = 0; p < 64; p += 2)
        for (uint64_t i = 0; i < 8; ++i) f.erase(p, i);
    for (uint64_t p = 0; p < 64; ++p) f.expectExact(p);
}

TEST(CheckedWritesViewTableModelRegression, RandomOpsWithPrefixMatchAMapModel) {
    struct Cfg { size_t k, pw; uint64_t mask; };
    for (Cfg c : {Cfg{16, 8, ~uint64_t{0}}, Cfg{6, 2, ~uint64_t{0}}, Cfg{40, 12, 0xff}, Cfg{9, 3, 0}, Cfg{3, 1, ~uint64_t{0}}}) {
        SCOPED_TRACE(int(c.k));
        Fixture f(c.k, c.pw, c.mask);
        std::mt19937_64 rng(c.k);
        const uint64_t prefixes = 30;
        const uint64_t suffixes = c.k - c.pw >= 2 ? 120 : 200;
        for (int op = 0; op < 12000; ++op) {
            // a skewed prefix mix, so one set grows large while others stay small
            const uint64_t pre = (rng() % 3 == 0) ? 0 : rng() % prefixes;
            const uint64_t suf = rng() % std::min<uint64_t>(suffixes, (c.k - c.pw) >= 8 ? ~uint64_t{0} : (uint64_t{1} << (8 * (c.k - c.pw))));
            const ViewKey key = f.key(pre, suf);
            switch (rng() % 3) {
            case 0: f.put(pre, suf); break;
            case 1: ASSERT_EQ(f.t.erase(key), f.model[pre].erase(str(key)) == 1); break;
            default: ASSERT_EQ(f.t.get(key) != nullptr, f.model[pre].count(str(key)) == 1);
            }
            if (op % 500 == 0)
                for (uint64_t p = 0; p < prefixes; ++p) f.expectExact(p);
        }
        for (uint64_t p = 0; p < prefixes; ++p) f.expectExact(p);
    }
}

TEST(CheckedWritesViewTablePrefixRegression, PoolChunkIsAnEighthOfChunkBytesRegression) {
    const RowLayout L = hpLayout();
    ViewTable t(L, 16, size_t{32} << 20, ViewTableKeys{.keyWidth = 16, .prefixWidth = 8, .hashSeed = 1});
    ViewTable plain(L, 16, size_t{32} << 20, ViewTableKeys{.keyWidth = 16, .hashSeed = 1});
    const size_t before = t.memoryBytes();
    t.put(mkKey(16, 8, 1, 1));
    plain.put(mkKey(16, 8, 1, 1));
    EXPECT_LE(t.memoryBytes() - before, (size_t{4} << 20) + 16 * 13);
    EXPECT_GT(t.memoryBytes(), plain.memoryBytes());
}

TEST(CheckedWritesViewTablePrefixRegression, MemoryBytesCountsThePrefixIndexRegression) {
    const RowLayout L = hpLayout();
    ViewTable t(L, 4096, 4096, ViewTableKeys{.keyWidth = 16, .prefixWidth = 8, .hashSeed = 1});
    ViewTable plain(L, 4096, 4096, ViewTableKeys{.keyWidth = 16, .hashSeed = 1});
    for (uint64_t i = 0; i < 20; ++i) {
        t.put(mkKey(16, 8, i, 1));
        plain.put(mkKey(16, 8, i, 1));
    }
    EXPECT_GT(t.memoryBytes(), plain.memoryBytes());
    size_t last = t.memoryBytes();
    size_t chunks = t.prefixPoolChunksForTest();
    for (uint64_t i = 20; i < 2000; ++i) {
        t.put(mkKey(16, 8, i, 1));
        if (t.prefixPoolChunksForTest() != chunks) {
            chunks = t.prefixPoolChunksForTest();
            EXPECT_GT(t.memoryBytes(), last);
        }
        last = t.memoryBytes();
    }
    EXPECT_GE(chunks, 2u);
}

// Emptying prefixes returns their set ids to setFree_ on the worker: the push must not allocate however many sets are free at once.
TEST(CheckedWritesViewTablePrefix, EmptyingEveryPrefixAllocatesNothingRegression) {
    const RowLayout L = hpLayout();
    ViewTable t(L, 4096, 32u << 20, ViewTableKeys{.keyWidth = 16, .prefixWidth = 8, .hashSeed = 1});
    constexpr uint64_t kPrefixes = 200;   // under deleted_'s warm reserve of 256
    std::vector<uint64_t> order(kPrefixes);
    for (uint64_t i = 0; i < kPrefixes; ++i) {
        order[i] = i;
        t.put(mkKey(16, 8, i, 1));
    }
    std::mt19937_64 rng(5);
    std::shuffle(order.begin(), order.end(), rng);
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow w;
        for (const uint64_t i : order) EXPECT_TRUE(t.erase(mkKey(16, 8, i, 1)));
        allocs = w.count();
    }
    EXPECT_EQ(allocs, 0u);
    EXPECT_EQ(t.size(), 0u);
}

// --- #573: a scan resumes by a stable hash-order position, so writes never invalidate it ---------------------------------

namespace {

struct Pos { uint32_t r; uint64_t w; };
Pos decodeToken(const Token& t) {
    Pos p{};
    std::memcpy(&p.r, t.data(), 4);
    std::memcpy(&p.w, t.data() + 4, 8);
    return p;
}
bool posLess(const Pos& a, const Pos& b) { return a.r != b.r ? a.r < b.r : a.w < b.w; }

// Runs a whole scan with pages of `page` rows, calling `between()` after every page (the writes), and returns every
// key visited.  `maxPages` is a hard cap so a scan that never finishes fails instead of hanging.
template <class Between>
std::vector<std::string> pagedScan(const ViewTable& t, const ViewKey& prefix, size_t page, size_t maxPages, Between&& between,
                                   bool* finished = nullptr, size_t* pages = nullptr) {
    std::vector<std::string> out;
    Token next{};
    bool haveToken = false;
    size_t n = 0;
    bool done = false;
    while (n < maxPages) {
        ++n;
        const Token resume = next;
        const PrefixScan r = scanSome(t, prefix, haveToken ? std::span<const uint8_t>(resume) : std::span<const uint8_t>(), next, page, out);
        if (r == PrefixScan::End) { done = true; break; }
        if (r != PrefixScan::Stopped) break;   // CursorGone: the old behaviour
        haveToken = true;
        between();
    }
    if (finished) *finished = done;
    if (pages) *pages = n;
    return out;
}

} // namespace

TEST(CheckedWritesViewTablePrefixRegression, AScanOfAHotPrefixCompletesUnderConstantWritesRegression) {
    Fixture f(16, 8);
    const ViewKey X = prefixOf(f.key(1, 0), 8);
    std::mt19937_64 rng(573);
    std::vector<uint64_t> live;
    std::unordered_set<std::string> stable, ever;
    uint64_t next = 0;
    for (; next < 20000; ++next) {
        f.put(1, next);
        live.push_back(next);
    }
    for (uint64_t s : live) { stable.insert(str(f.key(1, s))); ever.insert(str(f.key(1, s))); }
    const uint32_t cap0 = f.t.prefixSetCapacityForTest(X.span());
    uint32_t maxCap = cap0;
    bool grew = false, shrank = false, scannedMigrating = false;
    size_t page = 0;
    auto insertN = [&](size_t n) {
        for (size_t i = 0; i < n; ++i) {
            f.put(1, next);
            live.push_back(next);
            ever.insert(str(f.key(1, next)));
            ++next;
        }
    };
    auto eraseN = [&](size_t n) {
        for (size_t i = 0; i < n && live.size() > 100; ++i) {
            const size_t at = rng() % live.size();
            const uint64_t s = live[at];
            live[at] = live.back();
            live.pop_back();
            f.erase(1, s);
            stable.erase(str(f.key(1, s)));
        }
    };
    auto between = [&] {
        ++page;
        insertN(50);
        eraseN(50);
        if (page >= 3 && page < 15) insertN(1000);    // grow past 27853 live rows
        if (page >= 15 && page < 40) eraseN(1000);    // then shrink well below an eighth of the grown capacity
        const uint32_t cap = f.t.prefixSetCapacityForTest(X.span());
        grew = grew || cap > cap0;
        shrank = shrank || (cap < maxCap);
        maxCap = std::max(maxCap, cap);
        scannedMigrating = scannedMigrating || f.t.prefixSetMigratingForTest(X.span());
    };
    bool finished = false;
    size_t pages = 0;
    const std::vector<std::string> out = pagedScan(f.t, X, 256, 5000, between, &finished, &pages);
    ASSERT_TRUE(finished) << "the scan did not finish in " << pages << " pages";
    EXPECT_TRUE(grew);
    EXPECT_TRUE(shrank);
    EXPECT_TRUE(scannedMigrating);
    const std::unordered_set<std::string> got(out.begin(), out.end());
    EXPECT_EQ(got.size(), out.size()) << "a row was returned twice";
    size_t missing = 0, ghosts = 0;
    for (const std::string& k : stable) missing += got.count(k) ? 0 : 1;
    for (const std::string& k : got) ghosts += ever.count(k) ? 0 : 1;
    EXPECT_GT(stable.size(), 1000u);
    EXPECT_EQ(missing, 0u) << "a row that stayed under the prefix was skipped";
    EXPECT_EQ(ghosts, 0u) << "a row that never existed was returned";
}

TEST(CheckedWritesViewTablePrefixRegression, AScanWithoutWritesReturnsEveryRowOnceRegression) {
    for (const bool wide : {true, false}) {
        for (const size_t n : {size_t{0}, size_t{1}, size_t{15}, size_t{16}, size_t{17}, size_t{255}, size_t{256}, size_t{257}, size_t{100000}}) {
            Fixture f(wide ? 16 : 8, wide ? 8 : 4);
            for (uint64_t i = 0; i < n; ++i) f.put(1, i);
            const ViewKey X = prefixOf(f.key(1, 0), f.pw);
            for (const size_t pageRows : {size_t{1}, size_t{7}, size_t{256}}) {
                if (n == 100000 && pageRows == 1 && !wide) continue;   // same code path as wide; keep the run short
                std::vector<std::string> out;
                Token tok{};
                bool haveToken = false, done = false;
                Pos last{};
                bool haveLast = false;
                for (size_t guard = 0; guard < 200000 && !done; ++guard) {
                    const Token resume = tok;
                    const size_t before = out.size();
                    const PrefixScan r = scanSome(f.t, X, haveToken ? std::span<const uint8_t>(resume) : std::span<const uint8_t>(), tok, pageRows, out);
                    if (r == PrefixScan::End) { done = true; break; }
                    ASSERT_EQ(r, PrefixScan::Stopped);
                    const Pos p = decodeToken(tok);
                    if (haveLast) ASSERT_TRUE(posLess(last, p)) << "tokens must strictly increase";
                    if (wide && out.size() > before) {
                        // The token of a full page is the hash-order position of the last row it returned.
                        const std::string& k = out.back();
                        const uint64_t h = viewKeyHash({reinterpret_cast<const uint8_t*>(k.data()), k.size()}, 7);
                        ASSERT_EQ(p.w, h);
                        ASSERT_EQ(p.r, ViewTable::bitreverse32(static_cast<uint32_t>(h)));
                    }
                    last = p;
                    haveLast = haveToken = true;
                }
                ASSERT_TRUE(done) << "n=" << n << " page=" << pageRows;
                EXPECT_EQ(out.size(), n);
                EXPECT_EQ(std::set<std::string>(out.begin(), out.end()), f.model[1]) << "n=" << n << " page=" << pageRows;
            }
        }
    }
}

TEST(CheckedWritesViewTablePrefixRegression, TheSlotBudgetBoundsAPageRegression) {
    // Grown to 20 000 rows, then erased to just above an eighth of the capacity: the set keeps its large block.
    Fixture f(16, 8);
    for (uint64_t i = 0; i < 20000; ++i) f.put(1, i);
    for (uint64_t i = 4100; i < 20000; ++i) f.erase(1, i);
    const ViewKey X = prefixOf(f.key(1, 0), 8);
    ASSERT_GE(f.t.prefixSetCapacityForTest(X.span()), 32768u);
    for (const size_t budget : {ViewTable::kScanSlotBudget, size_t{8}}) {
        f.t.setScanSlotBudgetForTest(budget);
        std::vector<std::string> out;
        Token tok{};
        bool haveToken = false, done = false;
        size_t pages = 0, emptyPages = 0, maxVisited = 0;
        Pos last{};
        for (; pages < 1000000 && !done; ++pages) {
            const Token resume = tok;
            const size_t before = out.size();
            const PrefixScan r = scanSome(f.t, X, haveToken ? std::span<const uint8_t>(resume) : std::span<const uint8_t>(), tok, 256, out);
            maxVisited = std::max(maxVisited, f.t.lastScanSlotsVisitedForTest());
            if (r == PrefixScan::End) { done = true; break; }
            ASSERT_EQ(r, PrefixScan::Stopped);
            const Pos p = decodeToken(tok);
            if (haveToken) ASSERT_TRUE(posLess(last, p));
            last = p;
            if (out.size() == before) ++emptyPages;
            haveToken = true;
        }
        ASSERT_TRUE(done) << "budget " << budget;
        EXPECT_EQ(std::set<std::string>(out.begin(), out.end()), f.model[1]);
        EXPECT_EQ(out.size(), f.model[1].size());
        // A page looks at the budget plus, at most, the one probe run it was inside when the budget ran out.
        EXPECT_GT(maxVisited, 0u);
        EXPECT_LE(maxVisited, budget + 64) << "budget " << budget;
        EXPECT_GT(pages, 1u);
        if (budget == 8) EXPECT_GT(emptyPages, 0u) << "a sparse set must produce pages with no rows but a token";
    }
}

TEST(CheckedWritesViewTablePrefixRegression, EqualHashGroupsAreNeverSplitRegression) {
    // Mask 0xF: 16 distinct hashes for ~200 rows.  Wide keys: the slot word is the hash, so rows share (r, word) in
    // groups; narrow keys: rows share r and differ in word.  A group is emitted whole or not at all.
    for (const bool wide : {true, false}) {
        Fixture f(wide ? 16 : 8, wide ? 8 : 4, 0xF);
        for (uint64_t i = 0; i < 200; ++i) f.put(1, i);
        const ViewKey X = prefixOf(f.key(1, 0), f.pw);
        size_t biggestGroup = 1;
        if (wide) {
            std::map<uint64_t, size_t> perHash;
            for (const std::string& k : f.model[1]) ++perHash[viewKeyHash({reinterpret_cast<const uint8_t*>(k.data()), k.size()}, 7) & 0xF];
            for (auto& [h, c] : perHash) biggestGroup = std::max(biggestGroup, c);
            ASSERT_GT(biggestGroup, 4u) << "the seam must produce real groups";
        }
        for (const size_t pageRows : {biggestGroup, biggestGroup + 1, size_t{50}, size_t{256}}) {
            std::vector<std::string> out;
            Token tok{};
            bool haveToken = false, done = false;
            for (size_t guard = 0; guard < 10000 && !done; ++guard) {
                const Token resume = tok;
                size_t taken = 0;
                const PrefixScan r = f.t.scanPrefix(
                    X.span(), haveToken ? std::span<const uint8_t>(resume) : std::span<const uint8_t>(), tok,
                    [&](std::span<const uint8_t> key, const uint8_t*) {
                        if (taken == pageRows) return false;
                        ++taken;
                        out.push_back(str(key));
                        return true;
                    },
                    [&](size_t group) { return taken + group <= pageRows; });
                if (r == PrefixScan::End) { done = true; break; }
                ASSERT_EQ(r, PrefixScan::Stopped);
                haveToken = true;
            }
            ASSERT_TRUE(done) << "wide=" << wide << " page=" << pageRows;
            EXPECT_EQ(out.size(), 200u) << "wide=" << wide << " page=" << pageRows;
            EXPECT_EQ(std::set<std::string>(out.begin(), out.end()), f.model[1]) << "wide=" << wide << " page=" << pageRows;
        }
    }
}

TEST(CheckedWritesViewTablePrefixRegression, ModelCheckedStressKeepsTheStableRowGuaranteeRegression) {
    struct Scan {
        uint64_t prefix;
        Token tok{};
        bool haveToken = false;
        std::vector<std::string> out;
        std::set<std::string> stable;      // rows under the prefix at the start that were never erased since
        std::set<std::string> maybe;       // stable + rows inserted since
    };
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        std::mt19937_64 rng(seed * 7919);
        Fixture f(seed % 2 ? 16 : 8, seed % 2 ? 8 : 4, seed % 5 == 0 ? 0xFFF : ~uint64_t{0});
        std::vector<Scan> scans;
        auto finish = [&](size_t idx) {
            Scan& sc = scans[idx];
            const std::set<std::string> got(sc.out.begin(), sc.out.end());
            EXPECT_EQ(got.size(), sc.out.size()) << "seed " << seed << ": a row was returned twice";
            for (const std::string& k : sc.stable) EXPECT_TRUE(got.count(k)) << "seed " << seed << ": a stable row was skipped";
            for (const std::string& k : got) EXPECT_TRUE(sc.maybe.count(k)) << "seed " << seed << ": a row that never existed";
            scans.erase(scans.begin() + static_cast<std::ptrdiff_t>(idx));
        };
        auto page = [&](size_t idx, size_t limit) {
            Scan& sc = scans[idx];
            const Token resume = sc.tok;
            const PrefixScan r = scanSome(f.t, prefixOf(f.key(sc.prefix, 0), f.pw),
                                          sc.haveToken ? std::span<const uint8_t>(resume) : std::span<const uint8_t>(), sc.tok, limit, sc.out);
            EXPECT_NE(r, PrefixScan::CursorGone) << "seed " << seed;
            if (r == PrefixScan::End || r == PrefixScan::CursorGone) finish(idx);
            else sc.haveToken = true;
        };
        for (int step = 0; step < 6000; ++step) {
            const uint64_t op = rng() % 100;
            const uint64_t pre = rng() % 8;
            const uint64_t range = pre == 0 ? 3000 : 120;
            if (op < 45) {
                const uint64_t suf = rng() % range;
                f.put(pre, suf);
                for (Scan& sc : scans) if (sc.prefix == pre) sc.maybe.insert(str(f.key(pre, suf)));
            } else if (op < 75) {
                const uint64_t suf = rng() % range;
                f.erase(pre, suf);
                for (Scan& sc : scans) if (sc.prefix == pre) sc.stable.erase(str(f.key(pre, suf)));
            } else if (op < 80 && scans.size() < 6) {
                Scan sc;
                sc.prefix = pre;
                sc.stable = f.model[pre];
                sc.maybe = sc.stable;
                scans.push_back(std::move(sc));
            } else if (!scans.empty()) {
                page(rng() % scans.size(), 16 + rng() % 40);
            }
        }
        for (size_t guard = 0; guard < 100000 && !scans.empty(); ++guard) page(scans.size() - 1, 64);
        ASSERT_TRUE(scans.empty());
        for (uint64_t p = 0; p < 8; ++p) f.expectExact(p);
    }
}
