// ---------------------------------------------------------------------------
// LeaderEpochCacheRestartTest.cpp
//
// Task 9: Verify that LeaderEpochCache persists across a full node restart.
//
// Constructs a leader PartitionReplica, appends records across two epochs,
// asserts the checkpoint file exists, then constructs a fresh replica over
// the SAME log directory and asserts that its epoch cache (exposed via
// leaderEpochCacheForTest()) has both epoch boundaries after construction.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;

static constexpr NodeId kSingleNode = 1;

static std::string makeUniqTag() {
    static std::atomic<uint64_t> seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TempLogDir {
    std::string path;
    explicit TempLogDir(const std::string& tag) {
        path = (std::filesystem::temp_directory_path() / ("rune_lec_restart_" + tag)).string();
        std::filesystem::create_directories(path);
    }
    ~TempLogDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// Null transport: no messages needed — this is a single-node test exercising
// only the persistence / recover path.
// ---------------------------------------------------------------------------
class NullTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

static PartitionConfig singleNodeConfig(NodeId id)
{
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = id;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.fastAckOnApply      = false;
    return cfg;
}

// ---------------------------------------------------------------------------
// Task 9 — Restart test
//
// 1. Build a leader; becomeLeader(epoch=1), append, becomeLeader(epoch=2),
//    append.  The replica must have written a checkpoint.
// 2. Verify the checkpoint file exists next to the log dir.
// 3. Destroy the replica ("node restart") but keep the log directory.
// 4. Construct a fresh replica over the same log dir.
// 5. After construction (which runs restoreFromFile internally), verify
//    leaderEpochCacheForTest() contains both epoch boundaries.
// ---------------------------------------------------------------------------
TEST(LeaderEpochCacheRestart, CheckpointedEpochsBothPresentAfterRestart) {
    TempLogDir d(makeUniqTag());
    NullTransport transport;

    std::vector<NodeId> replicas = {kSingleNode};
    std::vector<NodeId> isr      = {kSingleNode};
    const std::string ckptPath = d.path + "/leader-epoch-checkpoint";

    // --- Phase A: first lifetime ---
    {
        rune::storage::Log log(d.path);

        PartitionReplica leader(singleNodeConfig(kSingleNode),
                                Role::Leader, /*epoch=*/1,
                                replicas, isr, &log, &transport);

        // Epoch 1: append a record.
        std::vector<uint8_t> payload1 = {0x01, 0x02};
        auto off1 = leader.append(payload1, /*acksAll=*/false);
        EXPECT_GE(off1, 0);

        // Promote to epoch 2.
        leader.becomeLeader(/*newEpoch=*/2, isr);

        // Epoch 2: append a record.
        std::vector<uint8_t> payload2 = {0x03, 0x04};
        auto off2 = leader.append(payload2, /*acksAll=*/false);
        EXPECT_GE(off2, 0);

        // The checkpoint file must exist after both becomeLeader calls.
        ASSERT_TRUE(std::filesystem::exists(ckptPath))
            << "Checkpoint file not written: " << ckptPath;
    }
    // Replica destroyed — simulates the node going down.

    // --- Phase B: second lifetime (restart) ---
    {
        rune::storage::Log log2(d.path);

        PartitionReplica leader2(singleNodeConfig(kSingleNode),
                                 Role::Leader, /*epoch=*/2,
                                 replicas, isr, &log2, &transport);

        // leaderEpochCacheForTest() exposes the internal epoch cache.
        const auto& cache = leader2.leaderEpochCacheForTest();

        // Both epoch boundaries must survive the restart.
        EXPECT_EQ(cache.startOffsetFor(1), 0)
            << "Epoch 1 should begin at offset 0";
        EXPECT_GE(cache.startOffsetFor(2), 1)
            << "Epoch 2 should begin at offset >= 1 (after epoch-1 records)";
        EXPECT_EQ(cache.latestEpoch(), 2)
            << "Latest epoch must be 2 after restore";

        // KIP-101 walk-back still correct post-restore: asking for epoch 1
        // must return endOffset = startOffset(epoch 2).
        int64_t ep2Start = cache.startOffsetFor(2);
        auto [respEpoch, endOff] = cache.endOffsetForEpoch(1, log2.logEndOffset());
        EXPECT_EQ(respEpoch, 1) << "responseEpoch should be 1 (shared epoch)";
        EXPECT_EQ(endOff, ep2Start) << "endOffset of epoch 1 should == start of epoch 2";
    }
}

// ---------------------------------------------------------------------------
// Memory-backed partition: checkpointToFile must be a no-op (no file written).
// ---------------------------------------------------------------------------
TEST(LeaderEpochCacheRestart, MemoryBackedSkipsPersistence) {
    TempLogDir d(makeUniqTag());
    NullTransport transport;

    std::vector<NodeId> replicas = {kSingleNode};
    std::vector<NodeId> isr      = {kSingleNode};
    const std::string ckptPath = d.path + "/leader-epoch-checkpoint";

    rune::storage::InMemoryLog memLog(1 << 20, 10000);

    PartitionReplica memLeader(singleNodeConfig(kSingleNode),
                               Role::Leader, /*epoch=*/1,
                               replicas, isr, &memLog, &transport);

    memLeader.becomeLeader(/*newEpoch=*/2, isr);

    // Memory-backed: NO checkpoint file should exist.
    EXPECT_FALSE(std::filesystem::exists(ckptPath))
        << "Memory-backed partition must not write a checkpoint file";
}
