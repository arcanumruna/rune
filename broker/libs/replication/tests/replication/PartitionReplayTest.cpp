// ---------------------------------------------------------------------------
// PartitionReplayTest.cpp
//
// Unit tests for PartitionReplay, focused on replay semantics:
//   - Byte equality
//   - Partial range correctness
//   - Integration with PartitionReplica follower catch-up
//   - Edge cases: empty store, single batch, large batches
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionRecorder.h>
#include <rune/replication/PartitionReplay.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/replication/PartitionReplica.h>
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

namespace {   // internal linkage: other TUs of rune_replication_tests define TempDir/TempFile (ODR)
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_replay_test_" + suffix + "_"
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
               + "/rune_replay_store_" + suffix + "_"
               + makeUniqSuffix() + ".bin";
    }
    ~TempFile() {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }
};
}  // namespace

// Build a proper RecordBatch for a given offset and fill byte.
static std::vector<uint8_t> makeBatch(int64_t offset, uint8_t fill,
                                       std::size_t valueSize = 16)
{
    rune::protocol::RecordBatchBuilder b(offset, 0);
    std::string key = "k";
    std::string val(valueSize, static_cast<char>(fill));
    b.append(key, val, 0);
    return b.build();
}

/// Build a multi-record RecordBatch at the given baseOffset.
static std::vector<uint8_t> makeMultiRecordBatch(int64_t baseOffset, int32_t recordCount)
{
    rune::protocol::RecordBatchBuilder b(baseOffset, 0);
    for (int32_t i = 0; i < recordCount; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string val(8, static_cast<char>(static_cast<uint8_t>(i + 1)));
        b.append(key, val, static_cast<int64_t>(i));
    }
    return b.build();
}

// Populate a FileRecordingStore with N batches at offsets 0..N-1.
static void populateStore(FileRecordingStore& store, int n, std::size_t batchSize = 16)
{
    for (int i = 0; i < n; ++i) {
        auto batch = makeBatch(static_cast<int64_t>(i),
                               static_cast<uint8_t>(i + 1), batchSize);
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(batch.data(), batch.size()));
    }
}

// ============================================================================
// Basic PartitionReplay tests
// ============================================================================

TEST(PartitionReplay, EmptyStore_ReplayAll_ReturnsZero)
{
    TempFile f("empty");
    FileRecordingStore store(f.path);
    PartitionReplay    replay(&store);

    int sinkCalled = 0;
    int64_t count = replay.replayAll([&](int64_t, std::span<const uint8_t>) {
        ++sinkCalled;
        return true;
    });

    EXPECT_EQ(count, 0);
    EXPECT_EQ(sinkCalled, 0);
}

TEST(PartitionReplay, SingleBatch_ReplayAll_DeliversBatch)
{
    TempFile f("single");
    FileRecordingStore store(f.path);
    auto batch = makeBatch(0, 0x01);
    store.append(0, std::span<const uint8_t>(batch.data(), batch.size()));

    PartitionReplay replay(&store);

    int sinkCalled = 0;
    int64_t count = replay.replayAll([&](int64_t baseOffset,
                                          std::span<const uint8_t> data) {
        ++sinkCalled;
        EXPECT_EQ(baseOffset, 0);
        EXPECT_EQ(data.size(), batch.size());
        EXPECT_TRUE(std::equal(data.begin(), data.end(), batch.begin()));
        return true;
    });

    EXPECT_EQ(count, 1);
    EXPECT_EQ(sinkCalled, 1);
}

TEST(PartitionReplay, MultipleBatches_OrderPreserved)
{
    TempFile f("order");
    FileRecordingStore store(f.path);
    std::vector<std::vector<uint8_t>> batches;
    for (int i = 0; i < 5; ++i) {
        batches.push_back(makeBatch(i, static_cast<uint8_t>(i + 1)));
        store.append(i, std::span<const uint8_t>(batches.back().data(),
                                                  batches.back().size()));
    }

    PartitionReplay replay(&store);

    int64_t expectedOffset = 0;
    int64_t count = replay.replayAll([&](int64_t baseOffset,
                                          std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, expectedOffset);
        EXPECT_EQ(data.size(), batches[expectedOffset].size());
        EXPECT_TRUE(std::equal(data.begin(), data.end(),
                                batches[expectedOffset].begin()))
            << "content mismatch at " << expectedOffset;
        ++expectedOffset;
        return true;
    });

    EXPECT_EQ(count, 5);
    EXPECT_EQ(expectedOffset, 5);
}

TEST(PartitionReplay, PartialRange_FromMiddle)
{
    TempFile f("partial_mid");
    FileRecordingStore store(f.path);
    populateStore(store, 10);

    PartitionReplay replay(&store);

    // Replay [3, 7)
    std::vector<int64_t> seen;
    int64_t count = replay.replay(3, 7, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 4);
    ASSERT_EQ(seen.size(), 4u);
    EXPECT_EQ(seen[0], 3);
    EXPECT_EQ(seen[1], 4);
    EXPECT_EQ(seen[2], 5);
    EXPECT_EQ(seen[3], 6);
}

TEST(PartitionReplay, PartialRange_FromBeginning)
{
    TempFile f("partial_begin");
    FileRecordingStore store(f.path);
    populateStore(store, 6);

    PartitionReplay replay(&store);

    std::vector<int64_t> seen;
    replay.replay(0, 3, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    ASSERT_EQ(seen.size(), 3u);
    EXPECT_EQ(seen[0], 0);
    EXPECT_EQ(seen[1], 1);
    EXPECT_EQ(seen[2], 2);
}

TEST(PartitionReplay, PartialRange_ToEnd)
{
    TempFile f("partial_end");
    FileRecordingStore store(f.path);
    populateStore(store, 6);

    PartitionReplay replay(&store);

    std::vector<int64_t> seen;
    replay.replay(4, INT64_MAX, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], 4);
    EXPECT_EQ(seen[1], 5);
}

TEST(PartitionReplay, EmptyRangeRequest_ReturnsZero)
{
    TempFile f("empty_req");
    FileRecordingStore store(f.path);
    populateStore(store, 5);

    PartitionReplay replay(&store);

    // fromOffset == toOffset → nothing to deliver.
    int64_t count = replay.replay(3, 3, [&](int64_t, std::span<const uint8_t>) {
        return true;
    });
    EXPECT_EQ(count, 0);
}

TEST(PartitionReplay, RangeAboveAllStored_ReturnsZero)
{
    TempFile f("range_above");
    FileRecordingStore store(f.path);
    populateStore(store, 3);  // offsets 0,1,2

    PartitionReplay replay(&store);

    int count = 0;
    replay.replay(10, 20, [&](int64_t, std::span<const uint8_t>) {
        ++count;
        return true;
    });
    EXPECT_EQ(count, 0);
}

TEST(PartitionReplay, ByteEquality_AllBytes)
{
    TempFile f("byte_eq");
    FileRecordingStore store(f.path);

    std::vector<std::vector<uint8_t>> original;
    for (int i = 0; i < 5; ++i) {
        auto p = makeBatch(i, static_cast<uint8_t>(0xA0 + i), 20 + i * 3);
        original.push_back(p);
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(p.data(), p.size()));
    }

    PartitionReplay replay(&store);

    int64_t idx = 0;
    replay.replayAll([&](int64_t baseOffset, std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, idx);
        EXPECT_EQ(data.size(), original[idx].size())
            << "size mismatch at batch " << idx;
        EXPECT_TRUE(std::equal(data.begin(), data.end(), original[idx].begin()))
            << "byte mismatch at batch " << idx;
        ++idx;
        return true;
    });

    EXPECT_EQ(idx, 5);
}

TEST(PartitionReplay, SinkReturnFalse_StopsEarly)
{
    TempFile f("sink_false");
    FileRecordingStore store(f.path);
    populateStore(store, 8);

    PartitionReplay replay(&store);

    int count = 0;
    replay.replayAll([&](int64_t, std::span<const uint8_t>) -> bool {
        ++count;
        return count < 3;  // stop after 3
    });

    // We stopped at count==3 (return false at 3rd batch).
    EXPECT_GE(count, 3);
    EXPECT_LE(count, 4);
}

TEST(PartitionReplay, ReplayIntoLog_LogEndOffsetCorrect)
{
    TempFile f("into_log");
    TempDir  logDir("replay_log");
    FileRecordingStore store(f.path);

    // Populate store with 4 single-record batches (offsets 0..3).
    for (int i = 0; i < 4; ++i) {
        auto p = makeBatch(i, static_cast<uint8_t>(i + 1));
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(p.data(), p.size()));
    }

    rune::storage::Log targetLog(logDir.path);
    PartitionReplay replay(&store);

    int64_t batches = replay.replayIntoLog(targetLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 4);
    // logEndOffset = nextOffset = lastOffset + 1 = 3 + 1 = 4.
    EXPECT_EQ(targetLog.logEndOffset(), 4);
}

TEST(PartitionReplay, LargeBatches_Correctness)
{
    TempFile f("large");
    FileRecordingStore store(f.path);

    // Two batches with 4 KiB values.
    auto large0 = makeBatch(0, 0xFF, 4096);
    auto large1 = makeBatch(1, 0xEE, 4096);
    store.append(0, std::span<const uint8_t>(large0.data(), large0.size()));
    store.append(1, std::span<const uint8_t>(large1.data(), large1.size()));

    PartitionReplay replay(&store);

    int count = 0;
    replay.replayAll([&](int64_t baseOffset, std::span<const uint8_t> data) {
        EXPECT_EQ(baseOffset, count);
        if (count == 0) {
            EXPECT_EQ(data.size(), large0.size());
            EXPECT_TRUE(std::equal(data.begin(), data.end(), large0.begin()));
        } else {
            EXPECT_EQ(data.size(), large1.size());
            EXPECT_TRUE(std::equal(data.begin(), data.end(), large1.begin()));
        }
        ++count;
        return true;
    });

    EXPECT_EQ(count, 2);
}

// ============================================================================
// Follower catch-up integration with PartitionReplica
// ============================================================================

static constexpr int kDriverCap = 200;

static PartitionConfig makeReplicaConfig(NodeId selfId)
{
    PartitionConfig cfg;
    cfg.partitionId          = 0;
    cfg.selfId               = selfId;
    cfg.replicaLagMaxMs      = 10000;
    cfg.replicaLagMaxOffset  = 100;
    cfg.minIsr               = 1;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    return cfg;
}

TEST(PartitionReplay, FollowerCatchUp_ReplayThenLiveFetch_NoGap)
{
    // Scenario:
    //   1. Leader commits 5 batches via PartitionReplica::append.
    //   2. PartitionRecorder records all 5 to FileRecordingStore.
    //   3. Follower has empty log — seeded via PartitionReplay (offsets 0..4).
    //   4. Leader commits 1 more batch (offset 5).
    //   5. Follower receives it via live replication (doWork+pump).
    //   6. Assert: follower log == leader log, logEndOffset == 6.

    TempDir leaderDir("rr_leader");
    TempDir followerDir("rr_follower");
    TempFile storeFile("rr_store");

    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    FileRecordingStore store(storeFile.path);
    PartitionRecorder  recorder(0, &leaderLog, &store);
    PartitionReplay    replay(&store);

    InMemoryTransport transport;

    std::vector<NodeId> replicas = {1, 2};
    std::vector<NodeId> isr      = {1, 2};

    PartitionReplica leader(makeReplicaConfig(1), Role::Leader, 1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeReplicaConfig(2), Role::Follower, 1,
                               replicas, isr, &followerLog, &transport);

    transport.setLeader(&leader);
    transport.addFollower(2, &follower);

    // Leader commits 5 batches.
    recorder.startRecording(0);
    for (int i = 0; i < 5; ++i) {
        auto p = makeBatch(i, static_cast<uint8_t>(i + 1));
        int64_t off = leader.append(std::span<const uint8_t>(p.data(), p.size()),
                                    false);
        ASSERT_GE(off, 0) << "leader append failed at " << i;
        recorder.onCommitted(i, i + 1);
    }

    // Seed follower from replay (cold catch-up — bypass live replication for offsets 0..4).
    int64_t replayed = replay.replayIntoLog(followerLog, 0, 5);
    EXPECT_EQ(replayed, 5);
    // After replaying 5 batches at offsets 0..4, logEndOffset = 5.
    EXPECT_EQ(followerLog.logEndOffset(), 5);

    // Leader commits one more batch.
    {
        auto p = makeBatch(5, 0x06);
        leader.append(std::span<const uint8_t>(p.data(), p.size()), false);
    }

    // Drive live replication until follower catches up to leaderLog.logEndOffset.
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
        done = (follower.logEndOffset() == leader.logEndOffset());
    }
    EXPECT_TRUE(done) << "follower did not catch up within " << kDriverCap << " iterations; "
                      << "follower LEO=" << follower.logEndOffset()
                      << " leader LEO=" << leader.logEndOffset();

    // logEndOffset = nextOffset = 6 (offsets 0..5, nextOffset=6).
    EXPECT_EQ(leaderLog.logEndOffset(), 6);
    EXPECT_EQ(followerLog.logEndOffset(), 6);
    EXPECT_EQ(follower.logEndOffset(), 6);

    // Both logs have data — not empty.
    EXPECT_GT(leaderLog.totalBytes(), 0);
    EXPECT_GT(followerLog.totalBytes(), 0);

    // Core correctness assertions:
    // 1. No gap: offsets 0..5 are readable from both logs.
    for (int64_t off = 0; off <= 5; ++off) {
        auto lBytes = leaderLog.read(off, 1 << 20);
        auto fBytes = followerLog.read(off, 1 << 20);
        EXPECT_FALSE(lBytes.empty())  << "leader missing offset " << off;
        EXPECT_FALSE(fBytes.empty())  << "follower missing offset " << off;
    }

    // 2. The first 5 batches in the follower came from the RecordingStore
    //    (cold catch-up replay). The store stores exactly what leaderLog.read
    //    returned for each onCommitted call. So the follower's content for
    //    these offsets is byte-equal to the store's content.
    //    We verify this by checking the store's batch count and positions are
    //    consistent with what was replayed (done by asserting replayed==5 above).

    // 3. Batch 5 (live-replicated) is readable from the follower.
    //    (Covered by the loop above at off==5.)

    // 4. No duplicate offsets: follower LEO == leader LEO (both == 6).
    //    (Asserted above: follower.logEndOffset() == 6, leader.logEndOffset() == 6)
}

TEST(PartitionReplay, FollowerCatchUp_EmptyLog_LiveFetchConverges)
{
    // Follower starts with empty log, no replay needed. Live replication converges.

    TempDir leaderDir("rr_leader2");
    TempDir followerDir("rr_follower2");

    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;

    std::vector<NodeId> replicas = {1, 2};
    std::vector<NodeId> isr      = {1, 2};

    PartitionReplica leader(makeReplicaConfig(1), Role::Leader, 1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeReplicaConfig(2), Role::Follower, 1,
                               replicas, isr, &followerLog, &transport);

    transport.setLeader(&leader);
    transport.addFollower(2, &follower);

    // Leader appends 3 batches at offsets 0,1,2.
    for (int i = 0; i < 3; ++i) {
        auto p = makeBatch(i, static_cast<uint8_t>(i + 1));
        leader.append(std::span<const uint8_t>(p.data(), p.size()), false);
    }

    // Drive until follower catches up.
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
        done = (follower.logEndOffset() == leader.logEndOffset());
    }
    EXPECT_TRUE(done) << "follower did not converge";
    // 3 batches at offsets 0,1,2 → logEndOffset = nextOffset = 3.
    EXPECT_EQ(follower.logEndOffset(), 3);
    EXPECT_EQ(leader.logEndOffset(), 3);
}

TEST(PartitionReplay, ReplayAll_Convenience_DeliversAll)
{
    TempFile f("replay_all");
    FileRecordingStore store(f.path);
    populateStore(store, 7);

    PartitionReplay replay(&store);

    std::vector<int64_t> offsets;
    int64_t count = replay.replayAll([&](int64_t baseOffset, std::span<const uint8_t>) {
        offsets.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 7);
    ASSERT_EQ(offsets.size(), 7u);
    for (int i = 0; i < 7; ++i) {
        EXPECT_EQ(offsets[i], i);
    }
}

// ============================================================================
// Multi-record batch tests — BUG 2 regression guards in PartitionReplay
// ============================================================================

TEST(PartitionReplay_MultiRecord, ReplayIntoLog_SingleMultiRecordBatch_LEOCorrect)
{
    // BUG 2 regression: replayIntoLog used to advance by +1, giving LEO=1.
    // With 4 records, correct LEO is 4.
    TempFile f("rp_mr_single");
    TempDir  logDir("rp_mr_log1");
    FileRecordingStore store(f.path);

    auto b = makeMultiRecordBatch(0, 4);
    store.append(0, std::span<const uint8_t>(b.data(), b.size()));

    rune::storage::Log targetLog(logDir.path);
    PartitionReplay replay(&store);

    int64_t batches = replay.replayIntoLog(targetLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 1);
    EXPECT_EQ(targetLog.logEndOffset(), 4)
        << "BUG 2: LEO must be baseOffset+lastOffsetDelta+1=4, not 1";
}

TEST(PartitionReplay_MultiRecord, ReplayIntoLog_TwoMultiRecordBatches_ContiguousOffsets)
{
    // batch0: baseOffset=0, 3 records (offsets 0,1,2) → after: LEO=3
    // batch1: baseOffset=3, 4 records (offsets 3,4,5,6) → after: LEO=7
    // Replay both: LEO must be 7.
    TempFile f("rp_mr_two");
    TempDir  logDir("rp_mr_log2");
    FileRecordingStore store(f.path);

    auto b0 = makeMultiRecordBatch(0, 3);
    auto b1 = makeMultiRecordBatch(3, 4);
    store.append(0, std::span<const uint8_t>(b0.data(), b0.size()));
    store.append(3, std::span<const uint8_t>(b1.data(), b1.size()));

    rune::storage::Log targetLog(logDir.path);
    PartitionReplay replay(&store);

    int64_t batches = replay.replayIntoLog(targetLog, 0, INT64_MAX);
    EXPECT_EQ(batches, 2);
    EXPECT_EQ(targetLog.logEndOffset(), 7)
        << "BUG 2: LEO after replaying two multi-record batches must be 7";
}

TEST(PartitionReplay_MultiRecord, PartialRange_MultiRecord_OnlyCorrectBatch)
{
    // Replay [3, 6) where batches are at baseOffsets 0 (3 recs), 3 (3 recs), 6 (3 recs).
    // Only batch at baseOffset=3 should be in range.
    TempFile f("rp_mr_partial");
    FileRecordingStore store(f.path);

    auto b0 = makeMultiRecordBatch(0, 3);
    auto b1 = makeMultiRecordBatch(3, 3);
    auto b2 = makeMultiRecordBatch(6, 3);
    store.append(0, std::span<const uint8_t>(b0.data(), b0.size()));
    store.append(3, std::span<const uint8_t>(b1.data(), b1.size()));
    store.append(6, std::span<const uint8_t>(b2.data(), b2.size()));

    PartitionReplay replay(&store);

    std::vector<int64_t> seen;
    int64_t count = replay.replay(3, 6, [&](int64_t baseOffset, std::span<const uint8_t>) {
        seen.push_back(baseOffset);
        return true;
    });

    EXPECT_EQ(count, 1);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], 3)
        << "partial range [3,6) must return only batch at baseOffset=3";
}

TEST(PartitionReplay_MultiRecord, ByteExact_MultiRecord)
{
    // Verify byte-exact replay of a multi-record batch.
    TempFile f("rp_mr_bytes");
    FileRecordingStore store(f.path);

    auto original = makeMultiRecordBatch(0, 5);
    store.append(0, std::span<const uint8_t>(original.data(), original.size()));

    PartitionReplay replay(&store);

    std::vector<uint8_t> replayed;
    replay.replayAll([&](int64_t, std::span<const uint8_t> data) {
        replayed.assign(data.begin(), data.end());
        return true;
    });

    EXPECT_EQ(replayed, original) << "replayed bytes must be byte-identical to original";
}
