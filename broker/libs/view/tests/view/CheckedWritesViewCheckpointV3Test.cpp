// Part 07 Task 7: checkpoint v3 - K-byte keys per table.
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <rune/protocol/Crc32c.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

using namespace rune::view;

namespace {

RowLayout rawLayout(uint32_t rowLen) {
    RowLayout L;
    L.rowLen = rowLen;
    return L;
}

struct Spec {
    uint8_t  keyLen;
    uint8_t  prefixLen;
    uint32_t rowLen;
};

ViewTable makeTable(const Spec& s) {
    return ViewTable(rawLayout(s.rowLen), 64, 1u << 20, ViewTableKeys{.keyWidth = s.keyLen, .prefixWidth = s.prefixLen, .hashSeed = 0x9e3779b97f4a7c15ull});
}

ViewKey randomKey(std::mt19937_64& rng, size_t k) {
    ViewKey key;
    key.len = static_cast<uint8_t>(k);
    for (size_t i = 0; i < k; ++i) key.bytes[i] = static_cast<uint8_t>(rng());
    return key;
}

void fillRow(uint8_t* row, uint32_t rowLen, uint64_t seed) {
    std::mt19937_64 rng(seed);
    for (uint32_t i = 0; i < rowLen; ++i) row[i] = static_cast<uint8_t>(rng());
}

// A group of tables plus the per-table wire parameters.
struct Rig {
    std::vector<Spec>      specs;
    std::vector<ViewTable> tables;
    std::vector<uint32_t>  rowLens;
    std::vector<uint8_t>   keyLens;
    std::vector<ViewTable*> ptrs;

    explicit Rig(std::vector<Spec> s) : specs(std::move(s)) {
        tables.reserve(specs.size());
        for (const Spec& sp : specs) {
            tables.push_back(makeTable(sp));
            rowLens.push_back(sp.rowLen);
            keyLens.push_back(sp.keyLen);
        }
        for (auto& t : tables) ptrs.push_back(&t);
    }
    std::optional<AppliedDelta> apply(std::span<const uint8_t> rec) { return applyKeyedDelta(ptrs, rowLens, keyLens, rec); }
};

// Drains every table not in `skip` into one v3 record (tables with nothing dirty are written empty).
std::vector<uint8_t> drainRecord(Rig& r, int64_t O, const std::set<size_t>& skip = {}) {
    struct Buf {
        std::vector<KeyedDirtyEntry>       entries;
        std::vector<std::vector<uint8_t>>  keys, rows;
    };
    std::vector<Buf> bufs(r.tables.size());
    for (size_t i = 0; i < r.tables.size(); ++i) {
        if (skip.count(i)) continue;
        Buf& b = bufs[i];
        r.tables[i].drainDirtyKeyed([&](uint32_t rid, const uint8_t* payload, bool del, std::span<const uint8_t> key) {
            b.keys.emplace_back(key.begin(), key.end());
            b.rows.emplace_back(del ? std::vector<uint8_t>{} : std::vector<uint8_t>(payload, payload + r.specs[i].rowLen));
            b.entries.push_back({rid, del, b.keys.back(), del ? nullptr : b.rows.back().data()});
        });
    }
    std::vector<KeyedTableDelta> tds;
    for (size_t i = 0; i < r.tables.size(); ++i) {
        if (skip.count(i)) continue;
        tds.push_back({static_cast<uint8_t>(i), r.specs[i].keyLen, r.specs[i].rowLen, bufs[i].entries});
    }
    std::vector<uint8_t> out;
    encodeKeyedDelta(tds, O, out);
    return out;
}

using Contents = std::map<std::string, std::string>;

Contents contentsOf(ViewTable& t, uint32_t rowLen, const std::vector<ViewKey>& universe) {
    Contents c;
    for (const ViewKey& k : universe) {
        const uint8_t* row = t.get(k);
        if (row) c[std::string(reinterpret_cast<const char*>(k.bytes.data()), k.len)] = std::string(reinterpret_cast<const char*>(row), rowLen);
    }
    return c;
}

std::set<std::string> prefixSet(const ViewTable& t, const ViewKey& prefix) {
    std::set<std::string> out;
    std::array<uint8_t, kScanTokenBytes> next{};
    t.scanPrefix(prefix.span(), {}, next, [&](std::span<const uint8_t> key, const uint8_t*) {
        out.insert(std::string(reinterpret_cast<const char*>(key.data()), key.size()));
        return true;
    });
    return out;
}

void putRandom(ViewTable& t, const ViewKey& k, uint32_t rowLen, uint64_t seed) { fillRow(t.put(k), rowLen, seed); }

bool sameTables(Rig& a, Rig& b, const std::vector<std::vector<ViewKey>>& universes) {
    for (size_t i = 0; i < a.tables.size(); ++i) {
        if (a.tables[i].size() != b.tables[i].size()) return false;
        if (contentsOf(a.tables[i], a.specs[i].rowLen, universes[i]) != contentsOf(b.tables[i], b.specs[i].rowLen, universes[i])) return false;
    }
    return true;
}

void putLE32(std::vector<uint8_t>& b, size_t at, uint32_t v) {
    for (int i = 0; i < 4; ++i) b[at + i] = static_cast<uint8_t>(v >> (i * 8));
}
void refreshCrc(std::vector<uint8_t>& rec) {
    putLE32(rec, rec.size() - 4, rune::protocol::crc32c(rec.data(), rec.size() - 4));
}

} // namespace


namespace {

// [prefix from a pool of `pool` prefixes][8 random bytes]
ViewKey poolKey(std::mt19937_64& rng, size_t K, size_t P, size_t pool) {
    ViewKey k = randomKey(rng, K);
    const uint64_t pre = rng() % pool;
    std::memset(k.bytes.data(), 0, P);
    std::memcpy(k.bytes.data(), &pre, 8);
    return k;
}

std::span<const uint8_t> sp(const std::vector<uint8_t>& v) { return v; }

} // namespace

TEST(CheckedWritesViewCheckpointV3, RoundTripTwoTables) {
    const std::vector<Spec> specs{{32, 24, 16}, {8, 0, 32}};
    Rig src(specs), dst(specs);
    std::mt19937_64 rng(7);
    std::vector<std::vector<ViewKey>> universe(2);
    std::set<std::string> seen;
    for (int i = 0; i < 1000; ++i) {
        ViewKey vk;
        do { vk = poolKey(rng, 32, 24, 40); } while (!seen.insert(std::string(reinterpret_cast<const char*>(vk.bytes.data()), 32)).second);
        universe[0].push_back(vk);
        putRandom(src.tables[0], vk, 16, i);
        const ViewKey ok = ViewKey::fromU64(i * 7919ull + 1);
        universe[1].push_back(ok);
        putRandom(src.tables[1], ok, 32, i + 5000);
    }
    const auto base = drainRecord(src, 100);
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(src.tables[0].erase(universe[0][i * 7]));
        ASSERT_TRUE(src.tables[1].erase(universe[1][i * 7]));
    }
    for (int i = 0; i < 50; ++i) {
        putRandom(src.tables[0], universe[0][i * 14], 16, 90000 + i);
        putRandom(src.tables[1], universe[1][i * 14], 32, 91000 + i);
    }
    const auto delta = drainRecord(src, 200);

    const std::span<const uint8_t> recs[] = {sp(base), sp(delta)};
    EXPECT_EQ(replayKeyed(dst.ptrs, dst.rowLens, dst.keyLens, recs), 200);
    EXPECT_TRUE(sameTables(src, dst, universe));
    EXPECT_EQ(dst.tables[0].size(), src.tables[0].size());
    std::set<std::string> prefixes;
    for (const ViewKey& k : universe[0]) prefixes.insert(std::string(reinterpret_cast<const char*>(k.bytes.data()), 24));
    for (const std::string& p : prefixes) {
        const ViewKey pk = ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(p.data()), 24});
        EXPECT_EQ(prefixSet(src.tables[0], pk), prefixSet(dst.tables[0], pk));
    }
}

TEST(CheckedWritesViewCheckpointV3, SixTablesRoundTrip) {
    const std::vector<Spec> specs{{40, 32, 12}, {8, 0, 8}, {8, 0, 16}, {8, 0, 24}, {8, 0, 20}, {40, 0, 8}};
    Rig src(specs), dst(specs);
    std::mt19937_64 rng(11);
    std::vector<std::vector<ViewKey>> universe(6);
    for (size_t t : {0u, 2u, 3u, 4u, 5u}) {
        for (int i = 0; i < 200; ++i) {
            const ViewKey k = specs[t].keyLen == 8 ? ViewKey::fromU64(rng()) : poolKey(rng, specs[t].keyLen, 32, 10);
            universe[t].push_back(k);
            putRandom(src.tables[t], k, specs[t].rowLen, rng());
        }
    }
    const auto rec = drainRecord(src, 33, {1});
    const std::span<const uint8_t> recs[] = {sp(rec)};
    EXPECT_EQ(replayKeyed(dst.ptrs, dst.rowLens, dst.keyLens, recs), 33);
    EXPECT_TRUE(sameTables(src, dst, universe));
    EXPECT_EQ(dst.tables[1].size(), 0u);
}

TEST(CheckedWritesViewCheckpointV3Regression, EightTablesRoundTripRegression) {
    std::vector<Spec> specs{{40, 32, 12}, {8, 0, 8}, {8, 0, 16}, {8, 0, 24}, {8, 0, 20}, {40, 0, 8}, {24, 0, 8}, {24, 0, 8}};
    Rig src(specs), dst(specs);
    std::mt19937_64 rng(13);
    std::vector<std::vector<ViewKey>> universe(8);
    for (size_t t = 0; t < 8; ++t) {
        for (int i = 0; i < 50; ++i) {
            const ViewKey k = specs[t].keyLen == 8 ? ViewKey::fromU64(rng()) : poolKey(rng, specs[t].keyLen, 8, 10);
            universe[t].push_back(k);
            putRandom(src.tables[t], k, specs[t].rowLen, rng());
        }
    }
    const auto rec = drainRecord(src, 9);
    const std::span<const uint8_t> recs[] = {sp(rec)};
    EXPECT_EQ(replayKeyed(dst.ptrs, dst.rowLens, dst.keyLens, recs), 9);
    EXPECT_TRUE(sameTables(src, dst, universe));

    // Seven tables: tables 6 and 7 are unknown to the caller, so the record is refused and nothing is touched.
    Rig seven(std::vector<Spec>(specs.begin(), specs.begin() + 7));
    EXPECT_FALSE(seven.apply(rec).has_value());
    for (auto& t : seven.tables) EXPECT_EQ(t.size(), 0u);

    // A ninth table (id 8) in the record, eight tables at the caller.
    specs.push_back({8, 0, 8});
    Rig nine(specs);
    putRandom(nine.tables[8], ViewKey::fromU64(1), 8, 1);
    const auto rec9 = drainRecord(nine, 5, {0, 1, 2, 3, 4, 5, 6, 7});
    Rig eight(std::vector<Spec>(specs.begin(), specs.begin() + 8));
    EXPECT_FALSE(eight.apply(rec9).has_value());
    for (auto& t : eight.tables) EXPECT_EQ(t.size(), 0u);
}

TEST(CheckedWritesViewCheckpointV3, MagicAndOffsetPlacement) {
    Rig src({{16, 0, 8}});
    putRandom(src.tables[0], randomKey(*std::make_unique<std::mt19937_64>(1), 16), 8, 1);
    const int64_t O = 0x0102030405060708ll;
    const auto rec = drainRecord(src, O);
    ASSERT_GE(rec.size(), 12u);
    EXPECT_EQ(rec[0], 0x33);
    EXPECT_EQ(rec[1], 0x43);
    EXPECT_EQ(rec[2], 0x4B);
    EXPECT_EQ(rec[3], 0x56);
    for (int i = 0; i < 8; ++i) EXPECT_EQ(rec[4 + i], static_cast<uint8_t>(O >> (8 * i)));
}

TEST(CheckedWritesViewCheckpointV3Regression, CorruptOrTruncatedNeverMutatesRegression) {
    const std::vector<Spec> specs{{32, 24, 16}, {8, 0, 32}};
    Rig src(specs);
    std::mt19937_64 rng(3);
    for (int i = 0; i < 20; ++i) {
        putRandom(src.tables[0], poolKey(rng, 32, 24, 5), 16, i);
        putRandom(src.tables[1], ViewKey::fromU64(i + 1), 32, i);
    }
    const auto good = drainRecord(src, 7);
    const size_t table1At = kCheckpointV3HeaderBytes + kCheckpointV3TableHeaderBytes + src.tables[0].size() * (5 + 32 + 16);

    auto untouched = [&](Rig& r) { return r.tables[0].size() == 0 && r.tables[1].size() == 0; };
    {
        Rig probe(specs);
        ASSERT_TRUE(probe.apply(good).has_value());   // the unmodified record applies
    }
    for (size_t n = 0; n < good.size(); ++n) {
        Rig dst(specs);
        EXPECT_FALSE(dst.apply(std::span<const uint8_t>(good.data(), n)).has_value()) << "prefix " << n;
        EXPECT_TRUE(untouched(dst)) << "prefix " << n;
    }
    auto mutated = [&](auto&& edit, const char* what) {
        auto rec = good;
        edit(rec);
        Rig dst(specs);
        EXPECT_FALSE(dst.apply(rec).has_value()) << what;
        EXPECT_TRUE(untouched(dst)) << what;
    };
    mutated([](auto& r) { r[r.size() - 1] ^= 0x01; }, "flipped CRC byte");
    mutated([](auto& r) { putLE32(r, 19, 0xFFFFFFFFu); refreshCrc(r); }, "bogus count in table 0");
    mutated([](auto& r) { putLE32(r, 19, 0xFFFFFFFFu); }, "bogus count, stale CRC");
    mutated([&](auto& r) { putLE32(r, table1At + 6, 0xFFFFFFFFu); refreshCrc(r); }, "bogus count in table 1");
    mutated([&](auto& r) { r[table1At] = 0; refreshCrc(r); }, "duplicated table id");
    mutated([](auto& r) { r[14] = 31; refreshCrc(r); }, "wrong keyLen");
    mutated([](auto& r) { putLE32(r, 15, 17); refreshCrc(r); }, "wrong rowLen");
    mutated([](auto& r) { r[27] = 2; refreshCrc(r); }, "bad flags byte");
    mutated([](auto& r) { r.insert(r.end() - 4, 0); refreshCrc(r); }, "trailing junk before CRC");
}

TEST(CheckedWritesViewCheckpointV3Regression, DuplicateIdBetweenIdenticalTablesRegression) {
    const std::vector<Spec> specs{{16, 0, 8}, {16, 0, 8}};
    Rig src(specs);
    putRandom(src.tables[0], ViewKey::fromBytes(std::array<uint8_t, 16>{1}), 8, 1);
    putRandom(src.tables[1], ViewKey::fromBytes(std::array<uint8_t, 16>{2}), 8, 2);
    auto rec = drainRecord(src, 4);
    const size_t table1At = kCheckpointV3HeaderBytes + kCheckpointV3TableHeaderBytes + (5 + 16 + 8);
    rec[table1At] = 0;   // both tables now claim id 0 with identical key/row lengths
    refreshCrc(rec);
    Rig dst(specs);
    EXPECT_FALSE(dst.apply(rec).has_value());
    EXPECT_EQ(dst.tables[0].size(), 0u);
    EXPECT_EQ(dst.tables[1].size(), 0u);
}

TEST(CheckedWritesViewCheckpointV3, DeleteThenReinsertInOneIntervalSurvives) {
    const std::vector<Spec> specs{{16, 0, 8}};
    Rig src(specs), dst(specs);
    const ViewKey a = ViewKey::fromBytes(std::array<uint8_t, 16>{1, 2, 3});
    const ViewKey b = ViewKey::fromBytes(std::array<uint8_t, 16>{9, 9, 9});
    putRandom(src.tables[0], a, 8, 1);
    putRandom(src.tables[0], b, 8, 2);
    const auto base = drainRecord(src, 1);
    ASSERT_TRUE(src.tables[0].erase(a));
    putRandom(src.tables[0], a, 8, 3);
    ASSERT_TRUE(src.tables[0].erase(b));
    const auto delta = drainRecord(src, 2);
    const std::span<const uint8_t> recs[] = {sp(base), sp(delta)};
    EXPECT_EQ(replayKeyed(dst.ptrs, dst.rowLens, dst.keyLens, recs), 2);
    ASSERT_NE(dst.tables[0].get(a), nullptr);
    EXPECT_EQ(std::memcmp(dst.tables[0].get(a), src.tables[0].get(a), 8), 0);
    EXPECT_EQ(dst.tables[0].get(b), nullptr);
}
