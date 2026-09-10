#include <gtest/gtest.h>
#include <rune/storage/OffsetIndex.h>
#include <filesystem>
using namespace rune::storage;

TEST(OffsetIndex, AppendAndLookupLargestLowerBound) {
    auto p = (std::filesystem::temp_directory_path() / "rune.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 100 /*baseOffset*/, 8 * 1024);
    idx.append(105, 0);
    idx.append(110, 200);
    idx.append(120, 512);
    EXPECT_EQ(idx.entries(), 3);
    EXPECT_EQ(idx.lookup(104).position, 0);     // below first entry -> base
    EXPECT_EQ(idx.lookup(105).offset, 105);
    EXPECT_EQ(idx.lookup(115).offset, 110);     // largest <= 115
    EXPECT_EQ(idx.lookup(999).offset, 120);
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmSectionBoundaryCorrectness) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i * 32); // 4000 entries > 1024 warm
    EXPECT_EQ(idx.lookup(5).offset, 0);
    EXPECT_EQ(idx.lookup(10 * 500 + 3).offset, 10 * 500);     // cold section
    EXPECT_EQ(idx.lookup(10 * 3999 + 1).offset, 10 * 3999);   // warm section (tail)
    EXPECT_EQ(idx.lookup(10 * 3000).offset, 10 * 3000);       // near boundary
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmLookupSearchesTailOnly) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm2.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i);
    (void)idx.lookup(10 * 3999);
    EXPECT_GE(idx.lastSearchLow(), 4000 - 1024 - 1); // searched only the warm tail
    std::filesystem::remove(p);
}
