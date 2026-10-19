// ---------------------------------------------------------------------------
// PartitionRecorderTest.cpp
//
// Unit tests for PartitionRecorder and FileRecordingStore.
//
// Design constraints (CLAUDE.md §4 / plan P3-4):
//   - Bounded, deterministic tests only.
//   - No busy-spin. No wall-clock waits.
//   - File-backed store is the deliverable; Archive gated separately.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionRecorder.h>
#include <rune/replication/PartitionReplay.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>
#include <climits>
#include <atomic>
#include <chrono>
#include <unistd.h>

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_recorder_test_" + suffix + "_"
               + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

struct TempFile {
    std::string path;
    explicit TempFile(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_store_test_" + suffix + "_"
               + makeUniqSuffix() + ".bin";
    }
    ~TempFile() {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }
};

// Build a proper RecordBatch at the given offset with a fill-byte value payload.
static std::vector<uint8_t> makeBatch(int64_t offset, uint8_t fill, std::size_t valueSize = 16)
{
    rune::protocol::RecordBatchBuilder b(offset, /*baseTimestamp=*/0);
    std::string key  = "k";
    std::string val(valueSize, static_cast<char>(fill));
    b.append(key, val, 0);
    return b.build();
}

/// Build a RecordBatch at the given baseOffset containing recordCount records.
/// Each record has a distinct value byte to ensure content uniqueness.
/// lastOffsetDelta = recordCount - 1, so the batch covers offsets
///   [baseOffset, baseOffset + recordCount - 1].
static std::vector<uint8_t> makeMultiRecordBatch(int64_t baseOffset, int32_t recordCount)
{
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp=*/0);
    for (int32_t i = 0; i < recordCount; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string val(8, static_cast<char>(static_cast<uint8_t>(i + 1)));
        b.append(key, val, static_cast<int64_t>(i));
    }
    return b.build();
}

// Append a properly-formatted RecordBatch to a Log.
// Returns the Log's new logEndOffset (= offset + 1 for single-record batch).
static int64_t appendToLog(rune::storage::Log& log, uint8_t fill, int64_t offset,
                             std::size_t valueSize = 16)
{
    auto batch = makeBatch(offset, fill, valueSize);
    return log.append(batch, offset);
}

/// Append a multi-record batch to a Log.
/// Returns the Log's new logEndOffset (= baseOffset + recordCount).
static int64_t appendMultiToLog(rune::storage::Log& log, int64_t baseOffset, int32_t recordCount)
{
    auto batch = makeMultiRecordBatch(baseOffset, recordCount);
    int64_t lastOffset = baseOffset + recordCount - 1;
    return log.append(batch, lastOffset);
}

// ============================================================================
// FileRecordingStore — basic tests
// ============================================================================

TEST(FileRecordingStore, InitiallyEmpty)
{
    TempFile f("init");
    FileRecordingStore store(f.path);

    EXPECT_EQ(store.recordingPosition(), 0);
    EXPECT_EQ(store.batchCount(), 0);
}

TEST(FileRecordingStore, AppendSingleBatch)
{
    TempFile f("single");
    FileRecordingStore store(f.path);

    auto bytes = makeBatch(0, 0xAB);
    store.append(0, std::span<const uint8_t>(bytes.data(), bytes.size()));

    EXPECT_EQ(store.recordingPosition(), 1);  // baseOffset 0 + 1
    EXPECT_EQ(store.batchCount(), 1);
}

TEST(FileRecordingStore, AppendMultipleBatches_PositionAdvances)
{
    TempFile f("multi");
    FileRecordingStore store(f.path);

    for (int i = 0; i < 5; ++i) {
        auto bytes = makeBatch(i, static_cast<uint8_t>(i));
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(bytes.data(), bytes.size()));
    }

    EXPECT_EQ(store.batchCount(), 5);
    EXPECT_EQ(store.recordingPosition(), 5);  // last baseOffset=4, so 4+1=5
}

TEST(FileRecordingStore, ScanAllBatches)
{
    TempFile f("scan_all");
    FileRecordingStore store(f.path);

    std::vector<std::vector<uint8_t>> written;
    for (int i = 0; i < 4; ++i) {
        auto bytes = makeBatch(i, static_cast<uint8_t>(i * 10));
        written.push_back(bytes);
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(bytes.data(), bytes.size()));
    }

    int64_t idx = 0;
    store.scan(0, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, idx);
        EXPECT_EQ(data.size(), written[idx].size());
        EXPECT_TRUE(std::equal(data.begin(), data.end(), written[idx].begin()))
            << "byte mismatch at batch " << idx;
        ++idx;
        return true;
    });
    EXPECT_EQ(idx, 4);
}

TEST(FileRecordingStore, ScanRangeFilter)
{
    TempFile f("range_filter");
    FileRecordingStore store(f.path);

    for (int i = 0; i < 6; ++i) {
        auto bytes = makeBatch(i, static_cast<uint8_t>(i));
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(bytes.data(), bytes.size()));
    }

    // Scan [2, 4) — should see offsets 2 and 3 only.
    std::vector<int64_t> seen;
    store.scan(2, 4, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], 2);
    EXPECT_EQ(seen[1], 3);
}

TEST(FileRecordingStore, ScanEmptyRange)
{
    TempFile f("empty_range");
    FileRecordingStore store(f.path);

    auto bytes = makeBatch(5, 0xFF);
    store.append(5, std::span<const uint8_t>(bytes.data(), bytes.size()));

    // Scan [10, 20) — no match since only offset 5 is stored.
    int count = 0;
    store.scan(10, 20, [&](int64_t, std::span<const uint8_t>) {
        ++count;
        return true;
    });
    EXPECT_EQ(count, 0);
}

TEST(FileRecordingStore, ScanEarlyAbort)
{
    TempFile f("early_abort");
    FileRecordingStore store(f.path);

    for (int i = 0; i < 5; ++i) {
        auto bytes = makeBatch(i, static_cast<uint8_t>(i));
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(bytes.data(), bytes.size()));
    }

    // Abort after first batch.
    int count = 0;
    store.scan(0, INT64_MAX, [&](int64_t, std::span<const uint8_t>) -> bool {
        ++count;
        return false;  // stop
    });
    EXPECT_EQ(count, 1);
}

TEST(FileRecordingStore, Durability_ReopenRestoresContent)
{
    TempFile f("reopen");
    std::vector<std::vector<uint8_t>> written;
    {
        FileRecordingStore store(f.path);
        for (int i = 0; i < 3; ++i) {
            auto bytes = makeBatch(i, static_cast<uint8_t>(i + 1));
            written.push_back(bytes);
            store.append(static_cast<int64_t>(i),
                         std::span<const uint8_t>(bytes.data(), bytes.size()));
        }
        EXPECT_EQ(store.batchCount(), 3);
        EXPECT_EQ(store.recordingPosition(), 3);
    }

    // Reopen — should find 3 batches with same content.
    {
        FileRecordingStore store(f.path);
        EXPECT_EQ(store.batchCount(), 3);
        EXPECT_EQ(store.recordingPosition(), 3);

        int64_t idx = 0;
        store.scan(0, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t> data) {
            EXPECT_EQ(baseOffset, idx);
            EXPECT_EQ(data.size(), written[idx].size());
            EXPECT_TRUE(std::equal(data.begin(), data.end(), written[idx].begin()))
                << "content mismatch at batch " << idx;
            ++idx;
            return true;
        });
        EXPECT_EQ(idx, 3);
    }
}

TEST(FileRecordingStore, TornRecording_PartialHeader_HandledGracefully)
{
    TempFile f("torn_header");
    {
        FileRecordingStore store(f.path);
        auto b = makeBatch(0, 0x01);
        store.append(0, std::span<const uint8_t>(b.data(), b.size()));
        store.append(1, std::span<const uint8_t>(b.data(), b.size()));
    }

    // Manually corrupt: append only 3 bytes (partial header).
    {
        std::ofstream ofs(f.path, std::ios::binary | std::ios::app);
        uint8_t garbage[3] = {0xFF, 0xFF, 0xFF};
        ofs.write(reinterpret_cast<const char*>(garbage), 3);
    }

    // Reopen — should recover 2 complete batches, ignore partial header.
    FileRecordingStore store(f.path);
    EXPECT_EQ(store.batchCount(), 2);
    EXPECT_EQ(store.recordingPosition(), 2);
}

TEST(FileRecordingStore, TornRecording_PartialPayload_HandledGracefully)
{
    TempFile f("torn_payload");
    {
        FileRecordingStore store(f.path);
        auto b = makeBatch(0, 0x02);
        store.append(0, std::span<const uint8_t>(b.data(), b.size()));
    }

    // Append a complete header claiming 20 bytes payload, but only write 5.
    {
        std::ofstream ofs(f.path, std::ios::binary | std::ios::app);
        uint64_t len = 20;
        int64_t  off = 1;
        uint8_t hdr[16];
        for (int i = 0; i < 8; ++i) hdr[i]     = (len >> (8*i)) & 0xFF;
        for (int i = 0; i < 8; ++i) hdr[8+i]   = (static_cast<uint64_t>(off) >> (8*i)) & 0xFF;
        ofs.write(reinterpret_cast<const char*>(hdr), 16);
        uint8_t partial[5] = {0xDE, 0xAD, 0xBE, 0xEF, 0x00};
        ofs.write(reinterpret_cast<const char*>(partial), 5);
    }

    // Reopen — should recover 1 complete batch, truncate torn frame.
    FileRecordingStore store(f.path);
    EXPECT_EQ(store.batchCount(), 1);
    EXPECT_EQ(store.recordingPosition(), 1);
}

TEST(FileRecordingStore, AppendAfterReopen)
{
    TempFile f("append_after_reopen");
    auto bA = makeBatch(0, 0xAA);
    auto bB = makeBatch(1, 0xBB);
    {
        FileRecordingStore store(f.path);
        store.append(0, std::span<const uint8_t>(bA.data(), bA.size()));
    }
    {
        FileRecordingStore store(f.path);
        store.append(1, std::span<const uint8_t>(bB.data(), bB.size()));
        EXPECT_EQ(store.batchCount(), 2);
        EXPECT_EQ(store.recordingPosition(), 2);
    }
    // Verify both batches present with correct content.
    {
        FileRecordingStore store(f.path);
        EXPECT_EQ(store.batchCount(), 2);
        std::vector<std::vector<uint8_t>> expected = {bA, bB};
        int64_t idx = 0;
        store.scan(0, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t> data) {
            EXPECT_EQ(baseOffset, idx);
            EXPECT_TRUE(std::equal(data.begin(), data.end(), expected[idx].begin()))
                << "content mismatch at " << idx;
            ++idx;
            return true;
        });
        EXPECT_EQ(idx, 2);
    }
}

TEST(FileRecordingStore, LargeBatchSurvivesRoundTrip)
{
    TempFile f("large");
    FileRecordingStore store(f.path);

    // Use a larger value but still within what RecordBatchBuilder can handle.
    auto batch = makeBatch(0, 0x77, 1024);  // 1 KiB value
    store.append(0, std::span<const uint8_t>(batch.data(), batch.size()));

    int count = 0;
    store.scan(0, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, 0);
        EXPECT_EQ(data.size(), batch.size());
        EXPECT_TRUE(std::equal(data.begin(), data.end(), batch.begin()));
        ++count;
        return true;
    });
    EXPECT_EQ(count, 1);
}

// ============================================================================
// PartitionRecorder — tests
// ============================================================================

class PartitionRecorderTest : public ::testing::Test {
protected:
    PartitionRecorderTest()
        : logDir("recorder_log")
        , storeFile("recorder_store")
        , log(logDir.path)
    {}

    TempDir             logDir;
    TempFile            storeFile;
    rune::storage::Log  log;
};

TEST_F(PartitionRecorderTest, InitiallyNotRecording)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    EXPECT_FALSE(recorder.isRecording());
    EXPECT_EQ(recorder.getRecordingPosition(), 0);
}

TEST_F(PartitionRecorderTest, StartRecording_EnablesRecording)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    recorder.startRecording(0);
    EXPECT_TRUE(recorder.isRecording());
}

TEST_F(PartitionRecorderTest, StopRecording_DisablesRecording)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    recorder.startRecording(0);
    recorder.stopRecording();
    EXPECT_FALSE(recorder.isRecording());
}

TEST_F(PartitionRecorderTest, OnCommitted_WhileNotRecording_IsNoOp)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    appendToLog(log, 0xAA, 0);
    recorder.onCommitted(0, 1);  // not started — no-op

    EXPECT_EQ(store.batchCount(), 0);
    EXPECT_EQ(recorder.getRecordingPosition(), 0);
}

TEST_F(PartitionRecorderTest, OnCommitted_AppendsBatchToStore)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    appendToLog(log, 0xAA, 0);
    recorder.startRecording(0);
    recorder.onCommitted(0, 1);

    EXPECT_EQ(store.batchCount(), 1);
    EXPECT_EQ(recorder.getRecordingPosition(), 1);
}

TEST_F(PartitionRecorderTest, RecordingPosition_AdvancesWithEachCommit)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    recorder.startRecording(0);

    for (int i = 0; i < 5; ++i) {
        appendToLog(log, static_cast<uint8_t>(i), static_cast<int64_t>(i));
        recorder.onCommitted(i, i + 1);
        EXPECT_EQ(recorder.getRecordingPosition(), i + 1)
            << "position wrong after commit " << i;
    }
}

TEST_F(PartitionRecorderTest, OnCommitted_BeforeFromOffset_IsSkipped)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    appendToLog(log, 0xAA, 0);
    appendToLog(log, 0xBB, 1);

    recorder.startRecording(/*fromOffset=*/1);  // only record from offset 1
    recorder.onCommitted(0, 1);  // before fromOffset — skip

    EXPECT_EQ(store.batchCount(), 0);

    recorder.onCommitted(1, 2);  // at fromOffset — record
    EXPECT_EQ(store.batchCount(), 1);
    EXPECT_EQ(recorder.getRecordingPosition(), 2);
}

TEST_F(PartitionRecorderTest, StartRecording_Idempotent)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    recorder.startRecording(0);
    recorder.startRecording(0);  // second call is no-op
    EXPECT_TRUE(recorder.isRecording());

    appendToLog(log, 0xAA, 0);
    recorder.onCommitted(0, 1);
    EXPECT_EQ(store.batchCount(), 1);
}

TEST_F(PartitionRecorderTest, StopRecording_Idempotent)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder recorder(0, &log, &store);

    recorder.stopRecording();  // no-op
    recorder.stopRecording();  // no-op
    EXPECT_FALSE(recorder.isRecording());
}

TEST_F(PartitionRecorderTest, Durability_RecordingStoredToDisk)
{
    // Record 3 batches, destroy recorder+store, reopen store — content intact.
    {
        FileRecordingStore store(storeFile.path);
        PartitionRecorder recorder(0, &log, &store);
        recorder.startRecording(0);

        for (int i = 0; i < 3; ++i) {
            appendToLog(log, static_cast<uint8_t>(i + 1), i);
            recorder.onCommitted(i, i + 1);
        }
        EXPECT_EQ(store.batchCount(), 3);
    }

    // Reopen store — should still have 3 batches.
    FileRecordingStore store2(storeFile.path);
    EXPECT_EQ(store2.batchCount(), 3);
    EXPECT_EQ(store2.recordingPosition(), 3);
}

TEST_F(PartitionRecorderTest, MultiplePartitionsRecordedToSeparateStores)
{
    TempFile storeFile2("recorder_store2");
    TempDir  logDir2("recorder_log2");
    rune::storage::Log log2(logDir2.path);

    FileRecordingStore store1(storeFile.path);
    FileRecordingStore store2(storeFile2.path);
    PartitionRecorder recorder1(0, &log,  &store1);
    PartitionRecorder recorder2(1, &log2, &store2);

    recorder1.startRecording(0);
    recorder2.startRecording(0);

    appendToLog(log,  0xAA, 0);
    appendToLog(log,  0xBB, 1);
    appendToLog(log2, 0xCC, 0);

    recorder1.onCommitted(0, 1);
    recorder1.onCommitted(1, 2);
    recorder2.onCommitted(0, 1);

    EXPECT_EQ(store1.batchCount(), 2);
    EXPECT_EQ(store2.batchCount(), 1);
    EXPECT_EQ(recorder1.getRecordingPosition(), 2);
    EXPECT_EQ(recorder2.getRecordingPosition(), 1);
}

// ============================================================================
// PartitionRecorder + PartitionReplay — round-trip tests
// ============================================================================

class RecorderReplayRoundTripTest : public ::testing::Test {
protected:
    RecorderReplayRoundTripTest()
        : logDir("rr_log")
        , storeFile("rr_store")
        , log(logDir.path)
    {}

    TempDir             logDir;
    TempFile            storeFile;
    rune::storage::Log  log;
};

TEST_F(RecorderReplayRoundTripTest, RoundTrip_SingleBatch_ByteExact)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    appendToLog(log, 0xDE, 0, 48);
    recorder.startRecording(0);
    recorder.onCommitted(0, 1);

    // Replay to sink and assert byte-exact match with what the store holds.
    std::vector<uint8_t> replayed;
    int64_t count = replay.replay(0, 1, [&](int64_t baseOffset,
                                             std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, 0);
        replayed.assign(data.begin(), data.end());
        return true;
    });

    EXPECT_EQ(count, 1);
    EXPECT_FALSE(replayed.empty());

    // The replay bytes should equal what Log::read returns (what the recorder stored).
    auto logBytes = log.read(0, 1 << 20);
    ASSERT_EQ(replayed.size(), logBytes.size());
    EXPECT_EQ(replayed, logBytes);
}

TEST_F(RecorderReplayRoundTripTest, RoundTrip_MultipleBatches_ByteExact)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    for (int i = 0; i < 6; ++i) {
        appendToLog(log, static_cast<uint8_t>(i + 1), i, 16 + i * 4);
        recorder.onCommitted(i, i + 1);
    }

    // Replay all and check offsets are in order.
    std::vector<int64_t> replayedOffsets;
    int64_t count = replay.replayAll([&](int64_t baseOffset, std::span<const uint8_t>) {
        replayedOffsets.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 6);
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(replayedOffsets[i], i) << "wrong offset at position " << i;
    }
}

TEST_F(RecorderReplayRoundTripTest, PartialRangeReplay)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    for (int i = 0; i < 8; ++i) {
        appendToLog(log, static_cast<uint8_t>(i), i);
        recorder.onCommitted(i, i + 1);
    }

    // Replay only [3, 6).
    std::vector<int64_t> seen;
    int64_t count = replay.replay(3, 6, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 3);
    ASSERT_EQ(seen.size(), 3u);
    EXPECT_EQ(seen[0], 3);
    EXPECT_EQ(seen[1], 4);
    EXPECT_EQ(seen[2], 5);
}

TEST_F(RecorderReplayRoundTripTest, EmptyRangeReplay_ReturnsZero)
{
    FileRecordingStore store(storeFile.path);
    PartitionReplay    replay(&store);

    int sinkCalled = 0;
    int64_t count = replay.replay(0, 10, [&](int64_t, std::span<const uint8_t>) {
        ++sinkCalled;
        return true;
    });

    EXPECT_EQ(count, 0);
    EXPECT_EQ(sinkCalled, 0);
}

TEST_F(RecorderReplayRoundTripTest, RecordThenReplayIntoFreshLog)
{
    // Write to a log, record it, replay into a fresh log, assert content equal.
    TempDir   freshDir("rr_fresh");
    rune::storage::Log freshLog(freshDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    for (int i = 0; i < 4; ++i) {
        appendToLog(log, static_cast<uint8_t>(i + 10), i, 24);
        recorder.onCommitted(i, i + 1);
    }

    int64_t batches = replay.replayIntoLog(freshLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 4);

    // Both logs should have the same content.
    auto origBytes  = log.read(0, 1 << 20);
    auto freshBytes = freshLog.read(0, 1 << 20);
    EXPECT_FALSE(origBytes.empty());
    EXPECT_EQ(origBytes, freshBytes) << "log content mismatch after replay";
}

TEST_F(RecorderReplayRoundTripTest, ReplayOffsetExactness)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    std::vector<int64_t> expectedOffsets = {0, 1, 2, 3, 4, 5, 6, 7};
    for (auto off : expectedOffsets) {
        appendToLog(log, static_cast<uint8_t>(off), off);
        recorder.onCommitted(off, off + 1);
    }

    std::vector<int64_t> actualOffsets;
    replay.replayAll([&](int64_t baseOffset, std::span<const uint8_t>) {
        actualOffsets.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(actualOffsets, expectedOffsets);
}

TEST_F(RecorderReplayRoundTripTest, RoundTrip_AfterReopen)
{
    {
        FileRecordingStore store(storeFile.path);
        PartitionRecorder  recorder(0, &log, &store);
        recorder.startRecording(0);

        for (int i = 0; i < 3; ++i) {
            appendToLog(log, static_cast<uint8_t>(i + 5), i);
            recorder.onCommitted(i, i + 1);
        }
    }

    // Reopen and replay.
    FileRecordingStore store2(storeFile.path);
    PartitionReplay    replay2(&store2);

    std::vector<int64_t> offsets;
    replay2.replayAll([&](int64_t baseOffset, std::span<const uint8_t>) {
        offsets.push_back(baseOffset);
        return true;
    });

    ASSERT_EQ(offsets.size(), 3u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(offsets[i], i);
    }
}

// ============================================================================
// Follower cold catch-up simulation
// ============================================================================

TEST_F(RecorderReplayRoundTripTest, FollowerColdCatchUp_Contiguous_NoGap)
{
    TempDir  followerDir("follower_coldcatchup");
    rune::storage::Log followerLog(followerDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    // Leader commits 5 batches (offsets 0..4).
    for (int i = 0; i < 5; ++i) {
        appendToLog(log, static_cast<uint8_t>(i + 1), i);
        recorder.onCommitted(i, i + 1);
    }

    // Follower cold catch-up: replay offsets [0, 5) into follower log.
    int64_t replayed = replay.replayIntoLog(followerLog, 0, 5);
    EXPECT_EQ(replayed, 5);
    EXPECT_EQ(followerLog.logEndOffset(), 5);  // nextOffset after offset 4 = 5

    // "Live fetch" delivers batch at offset 5.
    auto p5 = makeBatch(5, 0x06);
    log.append(p5, 5);
    followerLog.append(p5, 5);

    // Follower and leader log end should match.
    EXPECT_EQ(followerLog.logEndOffset(), log.logEndOffset());

    // Content byte-exact.
    auto leaderBytes   = log.read(0, 1 << 20);
    auto followerBytes = followerLog.read(0, 1 << 20);
    EXPECT_EQ(leaderBytes, followerBytes) << "follower log diverges from leader";
}

TEST_F(RecorderReplayRoundTripTest, FollowerColdCatchUp_OffsetsContinuous)
{
    TempDir  fDir("follower_contiguous");
    rune::storage::Log fLog(fDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    recorder.startRecording(0);

    int64_t N = 10;
    for (int64_t i = 0; i < N; ++i) {
        appendToLog(log, static_cast<uint8_t>(i), i);
        recorder.onCommitted(i, i + 1);
    }

    // Replay into follower.
    std::vector<int64_t> seenOffsets;
    replay.replay(0, N, [&](int64_t baseOffset, std::span<const uint8_t> data) {
        seenOffsets.push_back(baseOffset);
        std::vector<uint8_t> buf(data.begin(), data.end());
        fLog.append(buf, baseOffset);
        return true;
    });

    ASSERT_EQ(static_cast<int64_t>(seenOffsets.size()), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(seenOffsets[i], i) << "gap at offset " << i;
    }
    // Follower log end = nextOffset after last appended offset.
    EXPECT_EQ(fLog.logEndOffset(), N);  // N-1 was last offset, nextOffset = N
}

// ============================================================================
// RecordingPosition exactness
// ============================================================================

TEST_F(RecorderReplayRoundTripTest, RecordingPosition_ExactlyBaseOffsetPlusOne)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    recorder.startRecording(0);

    for (int64_t i = 0; i < 5; ++i) {
        appendToLog(log, static_cast<uint8_t>(i), i);
        recorder.onCommitted(i, i + 1);
        EXPECT_EQ(recorder.getRecordingPosition(), i + 1)
            << "recording position wrong after offset " << i;
    }
}

TEST_F(RecorderReplayRoundTripTest, RecordingPosition_NeverRegresses)
{
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    recorder.startRecording(0);

    int64_t lastPosition = 0;
    for (int64_t i = 0; i < 8; ++i) {
        appendToLog(log, static_cast<uint8_t>(i), i);
        recorder.onCommitted(i, i + 1);
        int64_t pos = recorder.getRecordingPosition();
        EXPECT_GE(pos, lastPosition) << "position regressed at offset " << i;
        lastPosition = pos;
    }
}

// ============================================================================
// Multi-record batch tests — BUG 1 and BUG 2 regression guards
// ============================================================================

// ---------------------------------------------------------------------------
// FileRecordingStore: multi-record batch recordingPosition correctness
// ---------------------------------------------------------------------------

TEST(FileRecordingStore_MultiRecord, RecordingPosition_MultiRecordBatch)
{
    // A batch with 4 records at baseOffset=0 covers offsets 0,1,2,3.
    // recordingPosition() must be 4, not 1.
    TempFile f("mr_pos");
    FileRecordingStore store(f.path);

    auto batch = makeMultiRecordBatch(0, 4);
    store.append(0, std::span<const uint8_t>(batch.data(), batch.size()));

    EXPECT_EQ(store.recordingPosition(), 4)
        << "BUG: recordingPosition should be baseOffset + lastOffsetDelta + 1 = 4";
    EXPECT_EQ(store.batchCount(), 1);
}

TEST(FileRecordingStore_MultiRecord, RecordingPosition_MultipleMultiRecordBatches)
{
    // batch0: baseOffset=0, 3 records → covers 0,1,2 → position after = 3
    // batch1: baseOffset=3, 2 records → covers 3,4   → position after = 5
    TempFile f("mr_multi_pos");
    FileRecordingStore store(f.path);

    auto b0 = makeMultiRecordBatch(0, 3);
    auto b1 = makeMultiRecordBatch(3, 2);
    store.append(0, std::span<const uint8_t>(b0.data(), b0.size()));
    store.append(3, std::span<const uint8_t>(b1.data(), b1.size()));

    EXPECT_EQ(store.batchCount(), 2);
    EXPECT_EQ(store.recordingPosition(), 5);
}

TEST(FileRecordingStore_MultiRecord, Durability_RecordingPosition_RestoredOnReopen)
{
    // After reopen, recordingPosition_ must reflect lastOffsetDelta, not +1 per batch.
    TempFile f("mr_reopen");
    {
        FileRecordingStore store(f.path);
        auto b = makeMultiRecordBatch(0, 5);  // covers offsets 0..4
        store.append(0, std::span<const uint8_t>(b.data(), b.size()));
        EXPECT_EQ(store.recordingPosition(), 5);
    }

    FileRecordingStore store2(f.path);
    EXPECT_EQ(store2.batchCount(), 1);
    EXPECT_EQ(store2.recordingPosition(), 5)
        << "BUG: reopen must restore position to baseOffset+lastOffsetDelta+1";
}

// ---------------------------------------------------------------------------
// PartitionRecorder: BUG 1 — all batches in committed range are recorded
// ---------------------------------------------------------------------------

class PartitionRecorderMultiRecordTest : public ::testing::Test {
protected:
    PartitionRecorderMultiRecordTest()
        : logDir("mr_recorder_log")
        , storeFile("mr_recorder_store")
        , log(logDir.path)
    {}

    TempDir             logDir;
    TempFile            storeFile;
    rune::storage::Log  log;
};

TEST_F(PartitionRecorderMultiRecordTest, OnCommitted_MultiBatch_AllBatchesRecorded)
{
    // BUG 1 regression test:
    // Append 3 single-record batches to the log (offsets 0, 1, 2).
    // Call onCommitted(0, 3) ONCE spanning all 3.
    // All 3 batches must be in the store; position must be 3.
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);

    appendToLog(log, 0x01, 0);
    appendToLog(log, 0x02, 1);
    appendToLog(log, 0x03, 2);

    recorder.startRecording(0);
    recorder.onCommitted(0, 3);  // one call spanning 3 batches

    EXPECT_EQ(store.batchCount(), 3)
        << "BUG 1: onCommitted spanning 3 batches must record all 3, not just the first";
    EXPECT_EQ(recorder.getRecordingPosition(), 3)
        << "BUG 1: getRecordingPosition must equal toOffset after recording all batches";
}

TEST_F(PartitionRecorderMultiRecordTest, OnCommitted_MultiRecordBatches_AllRecorded)
{
    // Append multi-record batches and commit them all at once.
    // batch0: baseOffset=0, 3 records → covers 0,1,2
    // batch1: baseOffset=3, 2 records → covers 3,4
    // Call onCommitted(0, 5) — must record both batches; position = 5.
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);

    appendMultiToLog(log, 0, 3);   // baseOffset=0, 3 records, lastOffset=2
    appendMultiToLog(log, 3, 2);   // baseOffset=3, 2 records, lastOffset=4

    recorder.startRecording(0);
    recorder.onCommitted(0, 5);

    EXPECT_EQ(store.batchCount(), 2)
        << "BUG 1+2: multi-record batches in committed range must each be separately recorded";
    EXPECT_EQ(recorder.getRecordingPosition(), 5)
        << "BUG 1: position must be 5 (last batch ends at offset 4, next = 5)";
}

TEST_F(PartitionRecorderMultiRecordTest, OnCommitted_FiveBatches_SingleCall_AllRecorded)
{
    // Worst-case: 5 multi-record batches committed in a single onCommitted call.
    // Each batch has 4 records:
    //   b0: offsets 0-3   (baseOffset=0,  lastOffsetDelta=3)
    //   b1: offsets 4-7   (baseOffset=4,  lastOffsetDelta=3)
    //   b2: offsets 8-11  (baseOffset=8,  lastOffsetDelta=3)
    //   b3: offsets 12-15 (baseOffset=12, lastOffsetDelta=3)
    //   b4: offsets 16-19 (baseOffset=16, lastOffsetDelta=3)
    // onCommitted(0, 20) must record all 5 and position = 20.
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);

    int64_t base = 0;
    const int32_t recsPerBatch = 4;
    const int numBatches = 5;
    for (int i = 0; i < numBatches; ++i) {
        appendMultiToLog(log, base, recsPerBatch);
        base += recsPerBatch;
    }

    recorder.startRecording(0);
    recorder.onCommitted(0, base);  // base = 20

    EXPECT_EQ(store.batchCount(), numBatches)
        << "BUG 1: all " << numBatches << " batches must be stored";
    EXPECT_EQ(recorder.getRecordingPosition(), base)
        << "BUG 1: position must equal toOffset = " << base;

    // Verify each batch's baseOffset is stored correctly.
    std::vector<int64_t> storedOffsets;
    store.scan(0, INT64_MAX, [&](int64_t off, std::span<const uint8_t>) {
        storedOffsets.push_back(off);
        return true;
    });
    ASSERT_EQ(static_cast<int>(storedOffsets.size()), numBatches);
    for (int i = 0; i < numBatches; ++i) {
        EXPECT_EQ(storedOffsets[i], static_cast<int64_t>(i * recsPerBatch))
            << "wrong baseOffset at stored batch " << i;
    }
}

// ---------------------------------------------------------------------------
// PartitionReplay: BUG 2 — multi-record batches replay with correct offsets
// ---------------------------------------------------------------------------

class PartitionReplayMultiRecordTest : public ::testing::Test {
protected:
    PartitionReplayMultiRecordTest()
        : logDir("mr_replay_log")
        , storeFile("mr_replay_store")
        , log(logDir.path)
    {}

    TempDir             logDir;
    TempFile            storeFile;
    rune::storage::Log  log;
};

TEST_F(PartitionReplayMultiRecordTest, ReplayIntoLog_MultiRecordBatch_CorrectLEO)
{
    // A single batch with 4 records at baseOffset=0.
    // After replayIntoLog, logEndOffset must be 4 (not 1).
    TempDir freshDir("mr_fresh1");
    rune::storage::Log freshLog(freshDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionReplay    replay(&store);

    auto b = makeMultiRecordBatch(0, 4);
    store.append(0, std::span<const uint8_t>(b.data(), b.size()));

    int64_t batches = replay.replayIntoLog(freshLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 1);
    EXPECT_EQ(freshLog.logEndOffset(), 4)
        << "BUG 2: replayIntoLog must set LEO to baseOffset+lastOffsetDelta+1=4, not 1";
}

TEST_F(PartitionReplayMultiRecordTest, ReplayIntoLog_MultiBatches_ContiguousLEO)
{
    // batch0: baseOffset=0, 3 records → LEO after = 3
    // batch1: baseOffset=3, 2 records → LEO after = 5
    // After replayIntoLog, logEndOffset must be 5.
    TempDir freshDir("mr_fresh2");
    rune::storage::Log freshLog(freshDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionReplay    replay(&store);

    auto b0 = makeMultiRecordBatch(0, 3);
    auto b1 = makeMultiRecordBatch(3, 2);
    store.append(0, std::span<const uint8_t>(b0.data(), b0.size()));
    store.append(3, std::span<const uint8_t>(b1.data(), b1.size()));

    int64_t batches = replay.replayIntoLog(freshLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 2);
    EXPECT_EQ(freshLog.logEndOffset(), 5)
        << "BUG 2: LEO after replaying two multi-record batches must be 5";
}

TEST_F(PartitionReplayMultiRecordTest, FullRoundTrip_MultiRecord_ByteExact_LEOCorrect)
{
    // Record multi-record batches via PartitionRecorder with a single onCommitted call,
    // then replay into a fresh log; assert byte-exact match and correct LEO.
    TempDir freshDir("mr_rt_fresh");
    rune::storage::Log freshLog(freshDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);
    PartitionReplay    replay(&store);

    // 4 batches of 3 records each: offsets 0-2, 3-5, 6-8, 9-11.
    int64_t base = 0;
    const int32_t recsPerBatch = 3;
    const int numBatches = 4;
    for (int i = 0; i < numBatches; ++i) {
        appendMultiToLog(log, base, recsPerBatch);
        base += recsPerBatch;
    }
    // base = 12

    recorder.startRecording(0);
    recorder.onCommitted(0, base);  // single call spanning all 4 batches

    EXPECT_EQ(store.batchCount(), numBatches);
    EXPECT_EQ(recorder.getRecordingPosition(), base);

    // Replay into fresh log.
    int64_t batches = replay.replayIntoLog(freshLog, 0, INT64_MAX);
    EXPECT_EQ(batches, numBatches);
    EXPECT_EQ(freshLog.logEndOffset(), base)
        << "LEO after replay must equal total offsets = " << base;

    // Byte-exact check.
    auto origBytes  = log.read(0, 1 << 20);
    auto freshBytes = freshLog.read(0, 1 << 20);
    EXPECT_FALSE(origBytes.empty());
    EXPECT_EQ(origBytes, freshBytes) << "log content mismatch after multi-record replay";
}

TEST_F(PartitionReplayMultiRecordTest, PartialRange_MultiRecordBatch_BoundaryCorrect)
{
    // Batches at baseOffsets 0 (3 recs), 3 (3 recs), 6 (3 recs).
    // Replay [3, 6): only batch at baseOffset=3 should be delivered.
    FileRecordingStore store(storeFile.path);
    PartitionReplay    replay(&store);

    auto b0 = makeMultiRecordBatch(0, 3);  // covers 0,1,2
    auto b1 = makeMultiRecordBatch(3, 3);  // covers 3,4,5
    auto b2 = makeMultiRecordBatch(6, 3);  // covers 6,7,8
    store.append(0, std::span<const uint8_t>(b0.data(), b0.size()));
    store.append(3, std::span<const uint8_t>(b1.data(), b1.size()));
    store.append(6, std::span<const uint8_t>(b2.data(), b2.size()));

    std::vector<int64_t> seen;
    int64_t count = replay.replay(3, 6, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 1);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], 3)
        << "partial range [3,6) on multi-record batches must return only batch at baseOffset=3";
}

TEST_F(PartitionReplayMultiRecordTest, ReplayIntoLog_MultiRecord_MatchesLogAppend)
{
    // Verify that replaying a multi-record batch into a fresh log produces the same
    // logEndOffset as directly appending the same batch to a log.
    TempDir freshDir("mr_direct");
    rune::storage::Log freshLog(freshDir.path);

    TempDir directDir("mr_direct2");
    rune::storage::Log directLog(directDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionReplay    replay(&store);

    const int32_t N = 7;
    auto batch = makeMultiRecordBatch(0, N);
    // Directly append to directLog.
    directLog.append(batch, N - 1);  // lastOffset = baseOffset + N - 1

    // Record and replay into freshLog.
    store.append(0, std::span<const uint8_t>(batch.data(), batch.size()));
    replay.replayIntoLog(freshLog, 0, INT64_MAX);

    EXPECT_EQ(freshLog.logEndOffset(), directLog.logEndOffset())
        << "BUG 2: replayIntoLog LEO must match direct append LEO for multi-record batch";
    EXPECT_EQ(freshLog.logEndOffset(), static_cast<int64_t>(N));
}

// ============================================================================
// S3 fix: recorder byte-budget — large-payload batches fully recorded
// ============================================================================

TEST_F(PartitionRecorderMultiRecordTest, S3_LargePayloadBatch_FullyRecorded) {
    // S3 regression: previously the byte budget was endOffset - recordFrom
    // (offset count), which for a batch with 1 large-payload record is 1 byte
    // budget vs potentially thousands of bytes of actual batch data.
    // The fix uses a proper 64 MiB byte budget.
    //
    // This test creates a batch with a large value payload (4 KiB) at offset 0
    // (so offset count = 1 but byte size >> 1).  onCommitted(0, 1) must record
    // the FULL batch — not truncate at byte 1.
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);

    // Build and append a batch with a 4 KiB value payload.
    constexpr std::size_t kValueSize = 4096;
    auto batch = makeBatch(0, 0xBE, kValueSize); // 4 KiB value
    log.append(batch, /*lastOffset=*/0); // single-record: baseOffset=0, lastOffsetDelta=0

    ASSERT_GT(static_cast<int64_t>(batch.size()), 1)
        << "Batch size must be >> 1 byte to exercise the S3 bug";

    recorder.startRecording(0);
    recorder.onCommitted(0, 1); // offset range = 1, but batch is >> 1 byte

    // S3 fix: the full batch must be recorded (not just 1 byte).
    EXPECT_EQ(store.batchCount(), 1)
        << "S3: large-payload batch must be fully recorded; byte-budget bug causes 0 batches";
    EXPECT_EQ(recorder.getRecordingPosition(), 1)
        << "S3: recording position must be 1 after recording offset 0..0";

    // Verify the stored batch is byte-equal to what we appended.
    int batchesSeen = 0;
    store.scan(0, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, 0);
        EXPECT_EQ(data.size(), batch.size())
            << "S3: stored batch size must match original; got " << data.size()
            << " want " << batch.size();
        EXPECT_TRUE(std::equal(data.begin(), data.end(), batch.begin()))
            << "S3: stored batch content must be byte-equal to original";
        ++batchesSeen;
        return true;
    });
    EXPECT_EQ(batchesSeen, 1);
}

TEST_F(PartitionRecorderMultiRecordTest, S3_MultipleLargePayloadBatches_AllRecorded) {
    // S3: multiple batches each with a large payload, committed in one call.
    // Each batch occupies far more bytes than its offset count = 1.
    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &log, &store);

    constexpr int     kBatches   = 5;
    constexpr std::size_t kValue = 2048; // 2 KiB per batch

    for (int i = 0; i < kBatches; ++i) {
        auto batch = makeBatch(static_cast<int64_t>(i), static_cast<uint8_t>(i + 1), kValue);
        log.append(batch, static_cast<int64_t>(i));
    }

    recorder.startRecording(0);
    recorder.onCommitted(0, kBatches); // offset count = 5; each batch >> 5 bytes

    EXPECT_EQ(store.batchCount(), kBatches)
        << "S3: all " << kBatches << " large-payload batches must be recorded";
    EXPECT_EQ(recorder.getRecordingPosition(), kBatches)
        << "S3: recording position must be " << kBatches;
}
