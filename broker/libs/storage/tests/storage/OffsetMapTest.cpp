#include <gtest/gtest.h>
#include <rune/storage/OffsetMap.h>
#include <string>

using namespace rune::storage;

// OM-1: get on empty map returns nullopt.
TEST(OffsetMap, EmptyGetReturnsNullopt) {
    OffsetMap m;
    EXPECT_FALSE(m.get("k").has_value());
    EXPECT_EQ(m.size(), 0u);
}

// OM-2: put + get round-trip for a single key.
TEST(OffsetMap, PutGetRoundTrip) {
    OffsetMap m;
    m.put("hello", 42);
    auto v = m.get("hello");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 42);
    EXPECT_EQ(m.size(), 1u);
}

// OM-3: later (larger) offset wins — put overwrites.
TEST(OffsetMap, LaterOffsetWins) {
    OffsetMap m;
    m.put("k", 10);
    m.put("k", 20);
    EXPECT_EQ(*m.get("k"), 20);
    EXPECT_EQ(m.size(), 1u);
}

// OM-4: earlier (smaller) offset does NOT overwrite.
TEST(OffsetMap, EarlierOffsetDoesNotOverwrite) {
    OffsetMap m;
    m.put("k", 100);
    m.put("k", 50);   // smaller — must be ignored
    EXPECT_EQ(*m.get("k"), 100);
    EXPECT_EQ(m.size(), 1u);
}

// OM-5: same offset is a no-op (idempotent).
TEST(OffsetMap, SameOffsetIsNoOp) {
    OffsetMap m;
    m.put("k", 7);
    m.put("k", 7);
    EXPECT_EQ(*m.get("k"), 7);
    EXPECT_EQ(m.size(), 1u);
}

// OM-6: distinct keys are tracked independently.
TEST(OffsetMap, DistinctKeysAreIndependent) {
    OffsetMap m;
    m.put("a", 1);
    m.put("b", 2);
    m.put("c", 3);
    EXPECT_EQ(*m.get("a"), 1);
    EXPECT_EQ(*m.get("b"), 2);
    EXPECT_EQ(*m.get("c"), 3);
    EXPECT_EQ(m.size(), 3u);
}

// OM-7: interleaved puts for multiple keys preserve per-key latest.
TEST(OffsetMap, InterleavedPutsPerKeyLatest) {
    OffsetMap m;
    m.put("x", 0);
    m.put("y", 1);
    m.put("x", 5);
    m.put("y", 3);
    m.put("x", 2); // smaller than 5 — ignored
    EXPECT_EQ(*m.get("x"), 5);
    EXPECT_EQ(*m.get("y"), 3);
}

// OM-8: clear resets size to 0 and makes all keys absent.
TEST(OffsetMap, ClearResetsMap) {
    OffsetMap m;
    m.put("a", 10);
    m.put("b", 20);
    EXPECT_EQ(m.size(), 2u);
    m.clear();
    EXPECT_EQ(m.size(), 0u);
    EXPECT_FALSE(m.get("a").has_value());
    EXPECT_FALSE(m.get("b").has_value());
}

// OM-9: clear then reuse (put after clear works).
TEST(OffsetMap, ClearThenReuse) {
    OffsetMap m;
    m.put("k", 99);
    m.clear();
    m.put("k", 1);
    EXPECT_EQ(*m.get("k"), 1);
    EXPECT_EQ(m.size(), 1u);
}

// OM-10: many keys (stress — 1000 entries).
TEST(OffsetMap, ManyKeys) {
    OffsetMap m;
    for (int i = 0; i < 1000; ++i) {
        m.put("key_" + std::to_string(i), static_cast<Offset>(i * 2));
    }
    EXPECT_EQ(m.size(), 1000u);
    for (int i = 0; i < 1000; ++i) {
        auto v = m.get("key_" + std::to_string(i));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, static_cast<Offset>(i * 2));
    }
}

// OM-11: update half the keys to larger offsets, verify rest unchanged.
TEST(OffsetMap, PartialUpdate) {
    OffsetMap m;
    for (int i = 0; i < 10; ++i) m.put("k" + std::to_string(i), i);
    // Update even-indexed keys.
    for (int i = 0; i < 10; i += 2) m.put("k" + std::to_string(i), i + 100);
    for (int i = 0; i < 10; ++i) {
        int64_t expect = (i % 2 == 0) ? i + 100 : i;
        EXPECT_EQ(*m.get("k" + std::to_string(i)), expect);
    }
    EXPECT_EQ(m.size(), 10u);
}

// OM-12: key with offset 0 is valid.
TEST(OffsetMap, OffsetZeroIsValid) {
    OffsetMap m;
    m.put("k", 0);
    ASSERT_TRUE(m.get("k").has_value());
    EXPECT_EQ(*m.get("k"), 0);
}

// OM-13: binary key data (bytes with NULs).
TEST(OffsetMap, BinaryKey) {
    OffsetMap m;
    std::string binKey = {'\x00', '\x01', '\x02', '\xFF'};
    m.put(std::string_view(binKey.data(), binKey.size()), 99);
    auto v = m.get(std::string_view(binKey.data(), binKey.size()));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 99);
}

// OM-14: very long key.
TEST(OffsetMap, LongKey) {
    OffsetMap m;
    std::string longKey(10000, 'x');
    m.put(longKey, 42);
    ASSERT_TRUE(m.get(longKey).has_value());
    EXPECT_EQ(*m.get(longKey), 42);
}

// OM-15: monotone sequence of puts (ascending offsets).
TEST(OffsetMap, MonotoneAscendingPuts) {
    OffsetMap m;
    for (int64_t i = 0; i < 50; ++i) {
        m.put("single_key", i);
    }
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(*m.get("single_key"), 49);
}
