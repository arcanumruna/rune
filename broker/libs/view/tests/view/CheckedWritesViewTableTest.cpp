// Part 07 Task 5: ViewTable takes ViewKeys up to 128 bytes.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/ViewKeyEncoding.h>
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
int32_t hpOf(const uint8_t* row, const RowLayout& L) { return static_cast<int32_t>(readSlot(row, L, 1).i); }

ViewKey randomKey(std::mt19937_64& rng, size_t k) {
    ViewKey key;
    key.len = static_cast<uint8_t>(k);
    for (size_t i = 0; i < k; ++i) key.bytes[i] = static_cast<uint8_t>(rng());
    return key;
}

std::string str(const ViewKey& k) { return std::string(reinterpret_cast<const char*>(k.bytes.data()), k.len); }

bool sameBytes(std::span<const uint8_t> a, const ViewKey& k) {
    return a.size() == k.len && std::memcmp(a.data(), k.bytes.data(), k.len) == 0;
}

} // namespace

static_assert(rune::view::kMaxViewKeyBytes == rune::codec::kMaxViewKeyBytes);

TEST(CheckedWritesViewTable, ViewKeyNames) {
    for (uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{1} << 63, UINT64_MAX}) EXPECT_EQ(ViewKey::fromU64(v).asU64(), v);
    const std::array<uint8_t, 6> six{1, 2, 3, 4, 5, 6};
    const ViewKey k = ViewKey::fromBytes(six);
    EXPECT_EQ(k.len, 6);
    EXPECT_EQ(k.asU64(), 0x060504030201ull);
    EXPECT_TRUE(ViewKey::fromBytes(six) == ViewKey::fromBytes(six));
    const std::array<uint8_t, 7> seven{1, 2, 3, 4, 5, 6, 0};
    EXPECT_FALSE(ViewKey::fromBytes(six) == ViewKey::fromBytes(seven));
    EXPECT_EQ(ViewKey::fromBytes(six).asU64(), ViewKey::fromBytes(seven).asU64());
}

TEST(CheckedWritesViewTable, NarrowViewKeyIsTheU64Path) {
    const RowLayout L = hpLayout();
    ViewTable t(L);
    uint8_t* row = t.put(ViewKey::fromU64(42));
    EXPECT_EQ(t.get(42), row);
    EXPECT_EQ(t.get(ViewKey::fromU64(42)), row);
    EXPECT_TRUE(t.erase(ViewKey::fromU64(42)));
    EXPECT_EQ(t.get(42), nullptr);
}

TEST(CheckedWritesViewTableRegression, SeedZeroIsBitIdenticalRegression) {
    const RowLayout L = hpLayout();
    ViewTable a(L, 64);
    ViewTable b(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 8, .hashSeed = 0});
    std::mt19937_64 rng(5);
    std::vector<std::tuple<uint32_t, bool, uint64_t>> da, db;
    std::vector<uint64_t> used;
    for (int i = 0; i < 10000; ++i) {
        const uint64_t k = rng() % 1500;
        used.push_back(k);
        if (rng() % 3 == 0) {
            EXPECT_EQ(a.erase(k), b.erase(k));
        } else {
            setHp(a.put(k), L, i);
            setHp(b.put(k), L, i);
        }
        if (i % 997 == 0) {
            a.drainDirty([&](uint32_t rid, const uint8_t*, bool del, uint64_t key) { da.emplace_back(rid, del, key); });
            b.drainDirty([&](uint32_t rid, const uint8_t*, bool del, uint64_t key) { db.emplace_back(rid, del, key); });
        }
    }
    a.drainDirty([&](uint32_t rid, const uint8_t*, bool del, uint64_t key) { da.emplace_back(rid, del, key); });
    b.drainDirty([&](uint32_t rid, const uint8_t*, bool del, uint64_t key) { db.emplace_back(rid, del, key); });
    EXPECT_EQ(da, db);
    EXPECT_EQ(a.size(), b.size());
    EXPECT_EQ(a.chunkCount(), b.chunkCount());
    EXPECT_EQ(a.indexSlotsForTest(), b.indexSlotsForTest());
    for (uint64_t k : used) {
        const uint8_t* ra = a.get(k);
        const uint8_t* rb = b.get(k);
        ASSERT_EQ(ra == nullptr, rb == nullptr);
        if (ra) {
            EXPECT_EQ(hpOf(ra, L), hpOf(rb, L));
        }
    }
    EXPECT_EQ(a.slotOrderForTest(), b.slotOrderForTest());
}

TEST(CheckedWritesViewTable, WideKeysOfEveryWidth) {
    const RowLayout L = hpLayout();
    for (size_t K : {9u, 16u, 24u, 66u, 74u, 120u, 128u}) {
        SCOPED_TRACE(K);
        ViewTable t(L, 64, 32u << 20, ViewTableKeys{.keyWidth = static_cast<uint8_t>(K), .hashSeed = 0x9e3779b97f4a7c15ull});
        std::mt19937_64 rng(K);
        std::vector<ViewKey> keys;
        std::set<std::string> seen;
        while (keys.size() < 50000) {
            ViewKey k = randomKey(rng, K);
            if (seen.insert(str(k)).second) keys.push_back(k);
        }
        std::vector<const uint8_t*> ptr(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            uint8_t* row = t.put(keys[i]);
            setHp(row, L, static_cast<int32_t>(i));
            ptr[i] = row;
        }
        EXPECT_GE(t.indexSlotsForTest(), 64u * 4);
        for (size_t i = 0; i < keys.size(); i += 2) ASSERT_TRUE(t.erase(keys[i]));
        EXPECT_EQ(t.size(), keys.size() / 2);
        for (size_t i = 0; i < keys.size(); ++i) {
            const uint8_t* row = t.get(keys[i]);
            if (i % 2 == 0) {
                ASSERT_EQ(row, nullptr);
                continue;
            }
            ASSERT_EQ(row, ptr[i]);
            ASSERT_EQ(hpOf(row, L), static_cast<int32_t>(i));
            ASSERT_TRUE(sameBytes(t.keyOfRow(row), keys[i]));
        }
    }
}

TEST(CheckedWritesViewTableRegression, FullCollisionsStayCorrectAcrossGrowthRegression) {
    const RowLayout L = hpLayout();
    constexpr size_t K = 24, N = 4000;
    ViewTable t(L, 64, 32u << 20, ViewTableKeys{.keyWidth = K, .hashSeed = 7, .hashMaskForTest = 0});
    std::vector<ViewKey> keys;
    for (size_t i = 0; i < N; ++i) {
        ViewKey k;
        k.len = K;
        k.bytes[23] = static_cast<uint8_t>(i);   // first 23 bytes shared; 256 values only, so spread into byte 22 too
        k.bytes[22] = static_cast<uint8_t>(i >> 8);
        keys.push_back(k);
    }
    for (size_t i = 0; i < N; ++i) {
        setHp(t.put(keys[i]), L, static_cast<int32_t>(i));
        if (i % 97 == 0) {
            for (size_t j = 0; j <= i; j += 13) ASSERT_NE(t.get(keys[j]), nullptr) << i << " " << j;
        }
        ASSERT_EQ(t.size(), i + 1);
    }
    EXPECT_GT(t.indexSlotsForTest(), 64u);
    size_t live = N;
    for (size_t i = 0; i < N; i += 3) {
        ASSERT_TRUE(t.erase(keys[i]));
        --live;
        ASSERT_EQ(t.size(), live);
        ASSERT_EQ(t.get(keys[i]), nullptr);
        ASSERT_FALSE(t.erase(keys[i]));
    }
    for (size_t i = 0; i < N; ++i) {
        const uint8_t* row = t.get(keys[i]);
        if (i % 3 == 0) {
            ASSERT_EQ(row, nullptr);
        } else {
            ASSERT_NE(row, nullptr);
            ASSERT_EQ(hpOf(row, L), static_cast<int32_t>(i));
        }
    }
}

TEST(CheckedWritesViewTableRegression, WideBackshiftNeverReadsTheArenaRegression) {
    const RowLayout L = hpLayout();
    ViewTable t(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 16, .hashSeed = 3});
    std::mt19937_64 rng(11);
    std::vector<ViewKey> keys;
    for (size_t i = 0; i < 5000; ++i) {
        keys.push_back(randomKey(rng, 16));
        t.put(keys.back());
    }
    const uint64_t before = t.arenaKeyReadsForTest();
    for (size_t i = 0; i < 1000; ++i) ASSERT_TRUE(t.erase(keys[i]));
    EXPECT_EQ(t.arenaKeyReadsForTest() - before, 1000u);
    for (size_t i = 1000; i < keys.size(); ++i) ASSERT_NE(t.get(keys[i]), nullptr);
}

TEST(CheckedWritesViewTable, DrainDirtyKeyedEmitsKBytes) {
    const RowLayout L = hpLayout();
    using Seen = std::vector<std::pair<bool, std::string>>;
    auto drain = [](ViewTable& t) {
        Seen out;
        t.drainDirtyKeyed([&](uint32_t, const uint8_t* payload, bool deleted, std::span<const uint8_t> key) {
            EXPECT_EQ(payload == nullptr, deleted);
            out.emplace_back(deleted, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
        });
        return out;
    };
    {
        ViewTable t(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 16, .hashSeed = 1});
        std::mt19937_64 rng(2);
        const ViewKey a = randomKey(rng, 16), b = randomKey(rng, 16);
        t.put(a);
        t.put(b);
        EXPECT_TRUE(t.erase(a));
        t.put(a);
        const Seen s = drain(t);
        ASSERT_EQ(s.size(), 3u);
        EXPECT_TRUE(s[0].first);
        EXPECT_EQ(s[0].second, str(a));
        EXPECT_FALSE(s[1].first);
        EXPECT_FALSE(s[2].first);
        EXPECT_EQ(std::set<std::string>({s[1].second, s[2].second}), std::set<std::string>({str(a), str(b)}));
        EXPECT_EQ(t.pendingDeleteCount(), 0u);
        EXPECT_TRUE(drain(t).empty());
    }
    {
        ViewTable t(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 6});
        const std::array<uint8_t, 6> raw{9, 8, 7, 6, 5, 4};
        const ViewKey k = ViewKey::fromBytes(raw);
        t.put(k);
        Seen s = drain(t);
        ASSERT_EQ(s.size(), 1u);
        EXPECT_EQ(s[0].second, str(k));
        EXPECT_TRUE(t.erase(k));
        s = drain(t);
        ASSERT_EQ(s.size(), 1u);
        EXPECT_TRUE(s[0].first);
        EXPECT_EQ(s[0].second, str(k));
    }
}

TEST(CheckedWritesViewTable, SeedChangesPlacementNotResults) {
    const RowLayout L = hpLayout();
    ViewTable a(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 16, .hashSeed = 1});
    ViewTable b(L, 64, 32u << 20, ViewTableKeys{.keyWidth = 16, .hashSeed = 2});
    std::mt19937_64 rng(4);
    std::vector<ViewKey> keys;
    for (size_t i = 0; i < 40; ++i) {
        keys.push_back(randomKey(rng, 16));
        setHp(a.put(keys.back()), L, static_cast<int32_t>(i));
        setHp(b.put(keys.back()), L, static_cast<int32_t>(i));
    }
    EXPECT_NE(a.slotOrderForTest(), b.slotOrderForTest());
    EXPECT_EQ(a.size(), b.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_NE(a.get(keys[i]), nullptr);
        ASSERT_NE(b.get(keys[i]), nullptr);
        EXPECT_EQ(hpOf(a.get(keys[i]), L), hpOf(b.get(keys[i]), L));
    }
}

TEST(CheckedWritesViewTableModelRegression, RandomOpsMatchAMapModel) {
    const RowLayout L = hpLayout();
    struct Cfg { uint8_t width; uint64_t mask; };
    for (Cfg c : {Cfg{8, ~uint64_t{0}}, Cfg{3, ~uint64_t{0}}, Cfg{16, ~uint64_t{0}}, Cfg{40, 0xff}, Cfg{9, 0}}) {
        SCOPED_TRACE(int(c.width));
        ViewTable t(L, 16, 4096, ViewTableKeys{.keyWidth = c.width, .hashSeed = 99, .hashMaskForTest = c.mask});
        std::mt19937_64 rng(c.width);
        std::vector<ViewKey> pool;
        for (int i = 0; i < 1200; ++i) {
            ViewKey k = randomKey(rng, c.width);
            k.bytes[0] = static_cast<uint8_t>(i);
            k.bytes[c.width > 1 ? 1 : 0] ^= static_cast<uint8_t>(i >> 8);
            pool.push_back(k);
        }
        std::map<std::string, int32_t> model;
        for (int op = 0; op < 20000; ++op) {
            const ViewKey& k = pool[rng() % pool.size()];
            switch (rng() % 3) {
            case 0: {
                setHp(t.put(k), L, op);
                model[str(k)] = op;
                break;
            }
            case 1:
                ASSERT_EQ(t.erase(k), model.erase(str(k)) == 1);
                break;
            default: {
                const uint8_t* row = t.get(k);
                auto it = model.find(str(k));
                ASSERT_EQ(row != nullptr, it != model.end());
                if (row) ASSERT_EQ(hpOf(row, L), it->second);
            }
            }
            ASSERT_EQ(t.size(), model.size());
        }
    }
}
