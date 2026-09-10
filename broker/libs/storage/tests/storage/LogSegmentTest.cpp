#include <gtest/gtest.h>
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
using namespace rune::storage; using namespace rune::protocol;

TEST(LogSegment, AppendThenReadBack) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0);

    RecordBatchBuilder b0(0, 1000); b0.append("k", "v0", 1000);
    int64_t next = seg.append(b0.build(), 0);
    EXPECT_EQ(next, 1);
    RecordBatchBuilder b1(1, 1001); b1.append("k", "v1", 1001); b1.append("k", "v2", 1002);
    next = seg.append(b1.build(), 2);
    EXPECT_EQ(next, 3);
    EXPECT_EQ(seg.nextOffset(), 3);

    auto bytes = seg.read(1, 1 << 20);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 1);
    std::filesystem::remove_all(dir);
}
