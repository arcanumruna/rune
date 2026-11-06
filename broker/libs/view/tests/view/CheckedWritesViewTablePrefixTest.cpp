// Part 07 Task 6: the ViewTable prefix index (each prefix -> a hashset of the keys of its rows) and versioned scans.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/view/ViewKey.h>
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
    });
}

std::set<std::string> scanAll(const ViewTable& t, const ViewKey& prefix, PrefixScan* result = nullptr) {
    std::vector<std::string> out;
    Token next{};
    const PrefixScan r = scanSome(t, prefix, {}, next, SIZE_MAX, out);
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

TEST(CheckedWritesViewTablePrefixRegression, AnyChangeUnderThePrefixInvalidatesTheTokenRegression) {
    struct Case { const char* name; bool valid; };
    auto run = [](auto&& change, bool valid) {
        Fixture f(16, 8);
        for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
        for (uint64_t i = 0; i < 4; ++i) f.put(2, i);
        const ViewKey A = prefixOf(f.key(1, 0), 8);
        std::vector<std::string> out;
        Token next{};
        ASSERT_EQ(scanSome(f.t, A, {}, next, 4, out), PrefixScan::Stopped);
        change(f);
        const Token resume = next;
        const PrefixScan r = scanSome(f.t, A, resume, next, SIZE_MAX, out);
        if (!valid) {
            EXPECT_EQ(r, PrefixScan::CursorGone);
        } else {
            EXPECT_EQ(r, PrefixScan::End);
            EXPECT_EQ(out.size(), 12u);
            EXPECT_EQ(std::set<std::string>(out.begin(), out.end()), f.model[1]);
        }
    };
    run([](Fixture& f) { f.put(1, 500); }, false);
    run([](Fixture& f) { f.erase(1, 5); }, false);
    run([](Fixture& f) { f.erase(1, 5); f.put(1, 5); }, false);
    run([](Fixture& f) { f.t.put(f.key(1, 5)); }, true);
    run([](Fixture& f) { f.put(2, 500); f.erase(2, 1); }, true);
}

TEST(CheckedWritesViewTablePrefixRegression, RecreatedSetNeverReusesAVersionRegression) {
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
    EXPECT_EQ(scanSome(f.t, A, tokA, scratch, SIZE_MAX, out), PrefixScan::CursorGone);   // no set
    for (uint64_t i = 0; i < 12; ++i) f.put(1, i);
    EXPECT_EQ(scanSome(f.t, A, tokA, scratch, SIZE_MAX, out), PrefixScan::CursorGone);   // recreated
    EXPECT_EQ(scanSome(f.t, A, tokB, scratch, SIZE_MAX, out), PrefixScan::CursorGone);   // another set's token
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
