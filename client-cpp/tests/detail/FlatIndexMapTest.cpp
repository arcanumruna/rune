// tests/detail/FlatIndexMapTest.cpp
#include <rune/client/detail/FlatIndexMap.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <unordered_map>

using rune::client::detail::FlatIndexMap;

namespace {
int64_t partKey(int32_t topic, int32_t partition) {
    return (static_cast<int64_t>(topic) << 32) | static_cast<uint32_t>(partition);
}
} // namespace

TEST(FlatIndexMap, FindOnEmptyReturnsNone) {
    FlatIndexMap m;
    EXPECT_EQ(m.find(42), FlatIndexMap::kNone);
    EXPECT_EQ(m.size(), 0u);
}

TEST(FlatIndexMap, InsertFindOverwrite) {
    FlatIndexMap m;
    EXPECT_TRUE(m.insert(partKey(1, 0), 7));
    EXPECT_FALSE(m.insert(partKey(1, 0), 9));   // overwrite, not new
    EXPECT_EQ(m.find(partKey(1, 0)), 9u);
    EXPECT_EQ(m.size(), 1u);
}

TEST(FlatIndexMap, ExtremeKeys) {
    FlatIndexMap m;
    const int64_t keys[] = {0, -1, std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<int64_t>::max(), partKey(-5, 3)};
    uint32_t v = 0;
    for (int64_t k : keys) m.insert(k, v++);
    v = 0;
    for (int64_t k : keys) EXPECT_EQ(m.find(k), v++);
}

TEST(FlatIndexMap, GrowsAndKeepsEveryKey) {
    FlatIndexMap m(4);
    for (int32_t t = 0; t < 10; ++t)
        for (int32_t p = 0; p < 1000; ++p)
            m.insert(partKey(t, p), static_cast<uint32_t>(t * 1000 + p));
    EXPECT_EQ(m.size(), 10'000u);
    EXPECT_LE(m.size() * 2, m.capacity());   // load factor <= 1/2
    for (int32_t t = 0; t < 10; ++t)
        for (int32_t p = 0; p < 1000; ++p)
            ASSERT_EQ(m.find(partKey(t, p)), static_cast<uint32_t>(t * 1000 + p));
}

// Backward-shift deletion must keep every probe run reachable.  Compare against a
// reference map over 200k random operations on a small key space (many collisions).
TEST(FlatIndexMap, RandomisedAgainstReference) {
    FlatIndexMap m(8);
    std::unordered_map<int64_t, uint32_t> ref;
    std::mt19937_64 rng(42);
    for (int op = 0; op < 200'000; ++op) {
        const int64_t k = static_cast<int64_t>(rng() % 4096) - 2048;
        switch (rng() % 3) {
        case 0: {
            const uint32_t v = static_cast<uint32_t>(rng() % 1'000'000);
            EXPECT_EQ(m.insert(k, v), ref.find(k) == ref.end());
            ref[k] = v;
            break;
        }
        case 1:
            EXPECT_EQ(m.erase(k), ref.erase(k) == 1);
            break;
        default: {
            const auto it = ref.find(k);
            ASSERT_EQ(m.find(k), it == ref.end() ? FlatIndexMap::kNone : it->second);
        }
        }
    }
    EXPECT_EQ(m.size(), ref.size());
    for (const auto& [k, v] : ref) ASSERT_EQ(m.find(k), v);
}

TEST(FlatIndexMap, ForEachVisitsEveryLiveEntryOnce) {
    FlatIndexMap m;
    for (uint32_t i = 0; i < 50; ++i) m.insert(i * 7919, i);
    for (uint32_t i = 0; i < 50; i += 2) m.erase(i * 7919);
    uint32_t seen = 0, sum = 0;
    m.forEach([&](int64_t, uint32_t v) { ++seen; sum += v; });
    EXPECT_EQ(seen, 25u);
    EXPECT_EQ(sum, 25u * 25u);   // 1 + 3 + ... + 49
}

// Regression (Task 7 review minor b): AeronTransport::stop() is noexcept and used
// to reset the map by assigning a freshly allocated FlatIndexMap.  clear() must
// empty it in place (keeping capacity, no allocation) and leave it fully usable.
TEST(FlatIndexMap, ClearEmptiesInPlaceWithoutReallocatingRegression) {
    FlatIndexMap m;
    for (int64_t k = 0; k < 100; ++k) m.insert(k, static_cast<uint32_t>(k));
    const uint32_t cap = m.capacity();
    static_assert(noexcept(m.clear()));
    m.clear();
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.capacity(), cap) << "clear keeps the slot array (no allocation)";
    for (int64_t k = 0; k < 100; ++k) EXPECT_EQ(m.find(k), FlatIndexMap::kNone);
    EXPECT_TRUE(m.insert(7, 70));
    EXPECT_EQ(m.find(7), 70u);
    EXPECT_EQ(m.size(), 1u);
}
