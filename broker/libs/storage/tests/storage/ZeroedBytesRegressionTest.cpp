// ZeroedBytesRegressionTest.cpp — 2026-10-15 (regression-speed): the fixed scratch / ring buffers that were
// value-filled std::vectors (constructed and destroyed byte by byte in Debug / sanitizer builds — ~1 s per
// test fixture) are ZeroedBytes / memset-backed arrays.  Pins what the vectors guaranteed: zeroed, exact size,
// owned, a moved-from buffer empty; and the SkimpyOffsetMap table that left std::vector keeps its semantics.
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/ZeroedBytes.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>

using rune::storage::SkimpyOffsetMap;
using rune::storage::ZeroedBytes;

TEST(ZeroedBytesRegression, ZeroedExactSizeAndMoveLeavesTheSourceEmpty) {
    ZeroedBytes b(3u << 20);
    ASSERT_EQ(b.size(), 3u << 20);
    ASSERT_NE(b.data(), nullptr);
    std::size_t nonZero = 0;
    for (std::size_t i = 0; i < b.size(); ++i) nonZero += b[i] != 0;
    EXPECT_EQ(nonZero, 0u);
    b[7] = 42;
    ZeroedBytes m(std::move(b));
    EXPECT_EQ(m.size(), 3u << 20);
    EXPECT_EQ(m[7], 42);
    EXPECT_EQ(b.size(), 0u);   // NOLINT(bugprone-use-after-move): the moved-from contract
    EXPECT_TRUE(b.empty());
    m.reset(64);
    EXPECT_EQ(m.size(), 64u);
    EXPECT_EQ(m[7], 0) << "reset re-zeroes";
    ZeroedBytes none;
    EXPECT_TRUE(none.empty());
    EXPECT_EQ(none.data(), nullptr);
}

TEST(ZeroedBytesRegression, A32MiBBufferIsBuiltInWellUnderASecondEvenUnoptimized) {
    // The value-filled vector took ~0.6 s for one worker's rings in a Debug build; a memset takes a few ms.
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 4; ++i) { ZeroedBytes b(32u << 20); ASSERT_EQ(b[b.size() - 1], 0); }
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2));
}

TEST(ZeroedBytesRegression, SkimpyOffsetMapTableKeepsItsSemanticsAcrossClearAndMove) {
    SkimpyOffsetMap m(1000);
    for (int i = 0; i < 900; ++i) ASSERT_TRUE(m.put("k" + std::to_string(i), i));
    EXPECT_EQ(m.size(), 900u);
    EXPECT_EQ(m.get("k5").value_or(-7), 5);
    EXPECT_FALSE(m.get("absent").has_value());
    EXPECT_TRUE(m.put("k5", 4));            // older offset: kept at 5
    EXPECT_EQ(m.get("k5").value_or(-7), 5);
    m.clear();
    EXPECT_EQ(m.size(), 0u);
    EXPECT_FALSE(m.get("k5").has_value()) << "clear empties every slot";
    EXPECT_EQ(m.latestOffset(), -1);
    ASSERT_TRUE(m.put("k5", 11));
    SkimpyOffsetMap moved(std::move(m));
    EXPECT_EQ(moved.get("k5").value_or(-7), 11);
    moved.clear();
    EXPECT_FALSE(moved.get("k5").has_value());
}
