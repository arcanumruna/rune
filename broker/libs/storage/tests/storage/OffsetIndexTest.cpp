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
