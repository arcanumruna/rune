// ============================================================================
// RetentionIntegrationTest.cpp
//
// Integration tests: Log::deleteOldSegments() retention behaviour on the real
// filesystem.  Uses a tiny segmentRollBytes to force many segment rolls.
//
// Key invariants exercised:
//   - Oldest whole segments' .log files are gone; newest remain on disk.
//   - totalBytes() == sum of surviving .log file sizes after retention.
//   - The active (last) segment is NEVER deleted.
//   - Reads of offsets that lived only in deleted segments return empty.
//   - Reads of surviving offsets still return valid batches.
//   - Appending after retention keeps the log consistent.
//   - Retention is idempotent (second call with same budget deletes 0).
//   - PartitionApi-level: consume(survivingOffset) returns correct suffix.
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/Log.h>
#include <rune/storage/PartitionApi.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <filesystem>
#include <numeric>
#include <set>

namespace fs = std::filesystem;
using namespace rune::storage;

// ---------------------------------------------------------------------------
// Helper: build and append a single-record batch to `log` and return its
// (baseOffset, value) so callers can validate reads.
// ---------------------------------------------------------------------------
static void appendOne(Log& log, int64_t offset, const std::string& key,
                      const std::string& value, int64_t ts) {
    rune::protocol::RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    auto batch = b.build();
    log.append(batch, offset); // lastOffset == offset for single-record batch
}

// ---------------------------------------------------------------------------
// Helper: sum the sizes of on-disk .log files under dir.
// ---------------------------------------------------------------------------
static int64_t sumLogFileSizes(const fs::path& dir) {
    int64_t total = 0;
    for (auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".log")
            total += static_cast<int64_t>(fs::file_size(e.path()));
    return total;
}

// ---------------------------------------------------------------------------
// 1. Produce many records forcing many rolls; run retention; verify oldest
//    .log files are gone and newest remain on disk.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionDeletesOldestSegmentsFromDisk) {
    // Small roll threshold so we get many segments.
    constexpr int64_t ROLL = 1024; // 1 KiB
    constexpr int N = 200;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 48), 1000 + i);

    int segsBefore = countLogSegments(testDir_);
    ASSERT_GT(segsBefore, 3) << "expected multiple segments before retention";

    // Snapshot paths before deletion.
    auto pathsBefore = logFilePaths(testDir_);

    // Retain only ~1/3 of total data — should delete several oldest segments.
    int64_t retainBytes = log.totalBytes() / 3;
    int deleted = log.deleteOldSegments(retainBytes);
    EXPECT_GT(deleted, 0) << "expected at least one segment deleted";

    // Snapshot paths after deletion.
    auto pathsAfter = logFilePaths(testDir_);
    EXPECT_LT(static_cast<int>(pathsAfter.size()), segsBefore);

    // Order-independent set-difference: compute which paths were deleted.
    std::set<std::string> beforeSet(pathsBefore.begin(), pathsBefore.end());
    std::set<std::string> afterSet(pathsAfter.begin(), pathsAfter.end());
    std::vector<std::string> deletedPaths;
    std::set_difference(beforeSet.begin(), beforeSet.end(),
                        afterSet.begin(), afterSet.end(),
                        std::back_inserter(deletedPaths));

    // Every deleted path must not exist on disk.
    EXPECT_FALSE(deletedPaths.empty()) << "deletion actually happened";
    for (const auto& path : deletedPaths) {
        EXPECT_FALSE(fs::exists(path))
            << "deleted segment should not exist: " << path;
    }

    // Every surviving path must exist on disk.
    for (const auto& path : pathsAfter) {
        EXPECT_TRUE(fs::exists(path))
            << "surviving segment must exist: " << path;
    }

    // The active (last) segment must be in the surviving set.
    fs::path activePath = activeLogPath(testDir_);
    EXPECT_TRUE(fs::exists(activePath))
        << "active segment must not be deleted";
    EXPECT_TRUE(afterSet.count(activePath.string()) > 0)
        << "active segment must be in surviving set";
}

// ---------------------------------------------------------------------------
// 2. totalBytes() == sum of surviving on-disk .log file sizes after retention.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionTotalBytesMatchesDiskAfterDelete) {
    constexpr int64_t ROLL = 512;
    constexpr int N = 150;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 32), 2000 + i);

    log.deleteOldSegments(log.totalBytes() / 2);

    int64_t apiBytes  = log.totalBytes();
    int64_t diskBytes = sumLogFileSizes(testDir_);

    EXPECT_EQ(apiBytes, diskBytes)
        << "totalBytes() must match sum of on-disk .log file sizes after retention";
    EXPECT_GT(apiBytes, 0) << "something must remain after retention";
}

// ---------------------------------------------------------------------------
// 3. Active segment is NEVER deleted regardless of retainBytes budget.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionNeverDeletesActiveSegment) {
    constexpr int64_t ROLL = 256;
    constexpr int N = 80;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 20), 3000 + i);

    fs::path activeBefore = activeLogPath(testDir_);
    ASSERT_FALSE(activeBefore.empty());

    // Request zero bytes retained — should still keep the active segment.
    log.deleteOldSegments(0);

    EXPECT_EQ(countLogSegments(testDir_), 1)
        << "only active segment must survive when retainBytes=0";
    EXPECT_TRUE(fs::exists(activeBefore))
        << "active segment file must still exist after aggressive retention";
}

// ---------------------------------------------------------------------------
// 4. Reads of offsets in deleted segments return empty; reads in surviving
//    segments still return valid batches with the correct baseOffset.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionDeletedOffsetReadsEmptySurvivingValid) {
    constexpr int64_t ROLL = 512;
    constexpr int N = 120;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 24), 4000 + i);

    // Record segment boundaries before deletion.
    auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GT(static_cast<int>(basesBefore.size()), 2);

    // Keep only the last 40% of bytes.
    int64_t retainBytes = log.totalBytes() * 2 / 5;
    int deleted = log.deleteOldSegments(retainBytes);
    ASSERT_GT(deleted, 0);

    auto basesAfter = log.segmentBaseOffsets();
    int64_t lowestSurviving = basesAfter.front();

    // Offsets in deleted region: expect empty read.
    // Offset 0 was in the first segment — should be deleted.
    if (lowestSurviving > 0) {
        auto raw = log.read(0, 1 << 20);
        EXPECT_TRUE(raw.empty())
            << "read of offset 0 (deleted segment) should return empty";
    }

    // First surviving offset should return a non-empty batch with correct baseOffset.
    auto rawSurv = log.read(lowestSurviving, 1 << 20);
    ASSERT_FALSE(rawSurv.empty())
        << "read of lowest surviving offset must return data";
    rune::protocol::RecordBatchReader reader(rawSurv.data(), rawSurv.size());
    EXPECT_TRUE(reader.ensureValid()) << "surviving batch must pass CRC check";
    EXPECT_EQ(reader.baseOffset(), lowestSurviving)
        << "surviving batch baseOffset must match the segment boundary";
}

// ---------------------------------------------------------------------------
// 5. After retention, append continues and the log remains consistent
//    (logEndOffset increases, new batches readable).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionThenAppendKeepsLogConsistent) {
    constexpr int64_t ROLL = 512;
    constexpr int N = 100;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 20), 5000 + i);

    int64_t hwBefore = log.logEndOffset();
    EXPECT_EQ(hwBefore, static_cast<int64_t>(N));

    log.deleteOldSegments(log.totalBytes() / 3);

    // Append 20 more records after retention.
    constexpr int EXTRA = 20;
    for (int i = N; i < N + EXTRA; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 20), 5000 + i);

    EXPECT_EQ(log.logEndOffset(), static_cast<int64_t>(N + EXTRA))
        << "logEndOffset must reflect all appended records";

    // The new records must be readable.
    auto raw = log.read(static_cast<int64_t>(N), 1 << 20);
    ASSERT_FALSE(raw.empty()) << "records appended after retention must be readable";
    rune::protocol::RecordBatchReader reader(raw.data(), raw.size());
    EXPECT_TRUE(reader.ensureValid());
    EXPECT_EQ(reader.baseOffset(), static_cast<int64_t>(N));
}

// ---------------------------------------------------------------------------
// 6. Retention is idempotent: a second call with the same budget deletes 0.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionIdempotentSecondCallDeletesZero) {
    constexpr int64_t ROLL = 512;
    constexpr int N = 100;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 20), 6000 + i);

    int64_t budget = log.totalBytes() / 2;
    int first  = log.deleteOldSegments(budget);
    int second = log.deleteOldSegments(budget);

    EXPECT_GT(first, 0)  << "first retention should delete something";
    EXPECT_EQ(second, 0) << "second retention with same budget should delete 0";
}

// ---------------------------------------------------------------------------
// 7. Retention with budget >= totalBytes() deletes 0 (nothing to remove).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionBudgetExceedsTotalBytesDeletesNothing) {
    constexpr int64_t ROLL = 512;
    constexpr int N = 60;

    Log log(testDir_.string(), ROLL);
    for (int i = 0; i < N; ++i)
        appendOne(log, static_cast<int64_t>(i), makeKey(i), makeValue(i, 20), 7000 + i);

    int64_t total = log.totalBytes();
    int deleted = log.deleteOldSegments(total * 2);
    EXPECT_EQ(deleted, 0) << "budget exceeds total — nothing to delete";
    EXPECT_EQ(log.totalBytes(), total) << "totalBytes must be unchanged";
}

// ---------------------------------------------------------------------------
// 8. PartitionApi-level: produce, retain, then consume(survivingOffset)
//    returns the correct suffix byte-exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RetentionPartitionApiConsumeAfterRetain) {
    // Use PartitionApi directly — it owns the Log with default 64 MiB roll,
    // so we produce enough data that multiple segments exist only at the Log
    // level with tiny roll.  Since PartitionApi doesn't expose segmentRollBytes,
    // we use a separate Log subdir to do retention and verify via PartitionApi
    // in the same directory (PartitionApi wraps Log at the same path).
    //
    // Strategy: use a sub-directory for the Log; produce via PartitionApi;
    // then use a fresh Log(same dir, tinyRoll) to delete segments — PartitionApi
    // reopened afterward must honour the deletion.

    fs::path dir = testDir_ / "retain_api";
    fs::create_directories(dir);

    constexpr int N = 300;
    std::vector<ProducedRecord> produced;

    // Produce via Log with tiny roll so we get many segments.
    {
        Log log(dir.string(), 512);
        for (int i = 0; i < N; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeValue(i, 24);
            rec.timestamp = static_cast<int64_t>(8000 + i);
            rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), rec.timestamp);
            b.append(rec.key, rec.value, rec.timestamp);
            log.append(b.build(), static_cast<int64_t>(i));
            produced.push_back(std::move(rec));
        }

        // Delete all but last ~1/3 of data.
        log.deleteOldSegments(log.totalBytes() / 3);
    }

    // Reopen the same directory via PartitionApi.
    PartitionApi api(dir.string());

    // HWM must still be N (logEndOffset not affected by retention).
    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));

    // Find the lowest offset that is actually readable by trying consume(0)
    // — if deleted, it returns empty; scan forward to the first surviving offset.
    // The surviving base offsets come from the segment names.
    std::vector<int64_t> survivingBases;
    for (auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".log")
            survivingBases.push_back(std::stoll(e.path().stem().string()));
    std::sort(survivingBases.begin(), survivingBases.end());
    ASSERT_FALSE(survivingBases.empty());
    int64_t firstSurviving = survivingBases.front();

    if (firstSurviving > 0) {
        // Consume from an offset that was deleted must return empty.
        auto recs = api.consume(0);
        EXPECT_TRUE(recs.empty())
            << "consume(0) after retention of segment-0 should return empty";
    }

    // Consume from the first surviving offset must return records with
    // correct values.
    auto recs = api.consume(firstSurviving);
    ASSERT_FALSE(recs.empty())
        << "consume from surviving offset must return records";

    for (size_t i = 0; i < recs.size(); ++i) {
        int64_t absIdx = recs[i].offset;
        ASSERT_GE(absIdx, 0);
        ASSERT_LT(absIdx, N);
        EXPECT_EQ(recs[i].key,   produced[static_cast<size_t>(absIdx)].key)
            << "key mismatch at offset " << absIdx;
        EXPECT_EQ(recs[i].value, produced[static_cast<size_t>(absIdx)].value)
            << "value mismatch at offset " << absIdx;
        EXPECT_EQ(recs[i].timestamp, produced[static_cast<size_t>(absIdx)].timestamp)
            << "timestamp mismatch at offset " << absIdx;
    }
}
