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

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_recorder_test_" + suffix + "_"
               + std::to_string(reinterpret_cast<uintptr_t>(this));
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
               + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".bin";
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

// Append a properly-formatted RecordBatch to a Log.
// Returns the Log's new logEndOffset (= offset + 1 for single-record batch).
static int64_t appendToLog(rune::storage::Log& log, uint8_t fill, int64_t offset,
                             std::size_t valueSize = 16)
{
    auto batch = makeBatch(offset, fill, valueSize);
    return log.append(batch, offset);
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
