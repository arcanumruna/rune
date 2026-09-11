#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <unistd.h>
using namespace rune::storage; using namespace rune::protocol;

class RetentionTest : public ::testing::Test {
protected:
    void SetUp() override {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        testDir_ = std::filesystem::temp_directory_path() / ("rune_ret_test_" + uniq);
    }
    void TearDown() override {
        if (std::filesystem::exists(testDir_)) {
            std::filesystem::remove_all(testDir_);
        }
    }
    std::filesystem::path testDir_;
};

TEST_F(RetentionTest, DeletesOldestSegmentsBySize) {
    auto dir = testDir_;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, i);
        b.append("k", "payloadpayload" + std::to_string(i), i);
        off = log.append(b.build(), off);
    }
    size_t before = log.segmentBaseOffsets().size();
    int deleted = log.deleteOldSegments(/*retainBytes*/ 256);
    EXPECT_GT(deleted, 0);
    EXPECT_LT(log.segmentBaseOffsets().size(), before);
    EXPECT_LE(log.totalBytes(), 256 + 128); // within one active segment of target
}

TEST_F(RetentionTest, RetainsActiveSegment) {
    auto dir = testDir_;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, i);
        b.append("k", "payloadpayload" + std::to_string(i), i);
        off = log.append(b.build(), off);
    }
    auto bases = log.segmentBaseOffsets();
    int activeSegmentBaseOffset = bases.back();
    // Even with retainBytes=0, at least one segment (the active) should remain
    int deleted = log.deleteOldSegments(/*retainBytes*/ 0);
    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_EQ(basesAfter.size(), 1);
    EXPECT_EQ(basesAfter.back(), activeSegmentBaseOffset);
    // Log should still be readable and appendable
    int64_t nextOff = log.logEndOffset();
    RecordBatchBuilder b(nextOff, 100);
    b.append("k", "still_appending", 100);
    EXPECT_NO_THROW(log.append(b.build(), nextOff));
}

TEST_F(RetentionTest, NoOpWhenUnderBudget) {
    auto dir = testDir_;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 10; ++i) {
        RecordBatchBuilder b(off, i);
        b.append("k", "p", i);
        off = log.append(b.build(), off);
    }
    auto bases = log.segmentBaseOffsets();
    size_t sizeBefore = bases.size();
    // retainBytes is much larger than totalBytes
    int deleted = log.deleteOldSegments(/*retainBytes*/ 100000);
    EXPECT_EQ(deleted, 0);
    EXPECT_EQ(log.segmentBaseOffsets().size(), sizeBefore);
}

TEST_F(RetentionTest, DeletedSegmentFilesRemovedFromDisk) {
    auto dir = testDir_;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, i);
        b.append("k", "payloadpayload" + std::to_string(i), i);
        off = log.append(b.build(), off);
    }

    // Snapshot actual .log files on disk before deletion
    std::vector<std::filesystem::path> logsBefore;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".log") {
            logsBefore.push_back(entry.path());
        }
    }
    std::sort(logsBefore.begin(), logsBefore.end());

    // Baseline assertions: we have multiple segments and they exist
    ASSERT_GT(logsBefore.size(), 1u);
    for (const auto& p : logsBefore) {
        EXPECT_TRUE(std::filesystem::exists(p));
    }

    int deleted = log.deleteOldSegments(/*retainBytes*/ 256);
    EXPECT_GT(deleted, 0);

    // Snapshot actual .log files after deletion
    std::vector<std::filesystem::path> logsAfter;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".log") {
            logsAfter.push_back(entry.path());
        }
    }
    std::sort(logsAfter.begin(), logsAfter.end());

    // Assert count dropped by exactly deleted
    EXPECT_EQ(logsAfter.size(), logsBefore.size() - deleted);

    // Verify the oldest 'deleted' files are gone
    for (size_t i = 0; i < static_cast<size_t>(deleted); ++i) {
        const auto& oldestPath = logsBefore[i];
        EXPECT_FALSE(std::filesystem::exists(oldestPath)) << "Oldest file should be deleted: " << oldestPath.string();
        // Also check .index file is gone
        auto indexPath = oldestPath.parent_path() / (oldestPath.stem().string() + ".index");
        EXPECT_FALSE(std::filesystem::exists(indexPath)) << "Index file should be deleted: " << indexPath.string();
    }

    // Verify the newest file still exists
    EXPECT_TRUE(std::filesystem::exists(logsBefore.back())) << "Newest file should remain: " << logsBefore.back().string();
}

TEST_F(RetentionTest, TotalBytesAccounting) {
    auto dir = testDir_;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, i);
        b.append("k", "payloadpayload" + std::to_string(i), i);
        off = log.append(b.build(), off);
    }

    int deleted = log.deleteOldSegments(/*retainBytes*/ 256);

    // Compute expected totalBytes by summing on-disk .log file sizes
    int64_t expectedBytes = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".log") {
            expectedBytes += std::filesystem::file_size(entry.path());
        }
    }

    // Assert that totalBytes() matches the real on-disk data
    EXPECT_EQ(log.totalBytes(), expectedBytes);
}
