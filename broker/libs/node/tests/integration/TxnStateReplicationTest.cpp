// ---------------------------------------------------------------------------
// TxnStateReplicationTest.cpp — Integration test for ReplicatedTxnStateStore.
//
// Scenario (__transaction_state durability across leader failover):
//   1. Stand up a 3-in-process-node cluster (leader + 2 followers) using
//      InMemoryTransport (same pattern as OffsetReplicationTest).
//   2. Leader puts a txn-state record via ReplicatedTxnStateStore (acksAll=true).
//   3. Drive replication until the put is majority-durable (HW >= 1 on node2).
//   4. "Kill" the leader: remove it from the transport; promote node2 to leader.
//   5. New leader constructs a ReplicatedTxnStateStore and calls rebuildFromLog().
//   6. Assert: the txn-state metadata is present after rebuild — NO data loss.
//
// NO Docker.  All waits are bounded (kDriverCap + ASSERT_TRUE).  No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::transaction;
using namespace rune::replication;

// ===========================================================================
// Shared test helpers
// ===========================================================================

static constexpr int kDriverCap = 600;

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TxnRepDir {
    std::string path;
    explicit TxnRepDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_trt_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TxnRepDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

static PartitionConfig makeTxnRepConfig(NodeId selfId, int minIsr = 2) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

// Helper: build a TransactionMetadata.
static TransactionMetadata makeMeta(const std::string& txnId,
                                     ProducerId pid,
                                     ProducerEpoch epoch,
                                     TxnState state)
{
    TransactionMetadata m;
    m.txnId         = txnId;
    m.producerId    = pid;
    m.producerEpoch = epoch;
    m.state         = state;
    m.txnTimeoutMs  = 30'000;
    m.txnStartNs    = (state == TxnState::Ongoing) ? 1'000'000'000LL : 0;
    return m;
}

// ===========================================================================
// Fixture: 3-in-process-node cluster using InMemoryTransport
// ===========================================================================
class TxnStateReplicationTest : public ::testing::Test {
protected:
    static constexpr NodeId kNode1 = 1;  // initial leader
    static constexpr NodeId kNode2 = 2;  // follower → new leader after failover
    static constexpr NodeId kNode3 = 3;  // follower

    TxnStateReplicationTest()
        : dir1("n1"), dir2("n2"), dir3("n3")
        , log1(dir1.path), log2(dir2.path), log3(dir3.path)
    {}

    void SetUp() override {
        const std::vector<NodeId> replicas = {kNode1, kNode2, kNode3};
        const std::vector<NodeId> isr      = {kNode1, kNode2, kNode3};

        node1_ = std::make_unique<PartitionReplica>(
            makeTxnRepConfig(kNode1, /*minIsr=*/2),
            Role::Leader, /*epoch=*/1, replicas, isr, &log1, &transport_);

        node2_ = std::make_unique<PartitionReplica>(
            makeTxnRepConfig(kNode2, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log2, &transport_);

        node3_ = std::make_unique<PartitionReplica>(
            makeTxnRepConfig(kNode3, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log3, &transport_);

        transport_.setLeader(node1_.get());
        transport_.addFollower(kNode2, node2_.get());
        transport_.addFollower(kNode3, node3_.get());
    }

    bool driveUntil(std::function<bool()> done, int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock_ += stepNs;
            node1_->doWork(clock_);
            node2_->doWork(clock_);
            node3_->doWork(clock_);
            transport_.pump(clock_);
        }
        return done();
    }

    TxnRepDir dir1, dir2, dir3;
    rune::storage::Log log1, log2, log3;

    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> node1_, node2_, node3_;
    int64_t clock_ = 0;
};

// ===========================================================================
// Test 1: Put on leader → kill leader → new leader rebuildFromLog → no loss
// ===========================================================================
TEST_F(TxnStateReplicationTest, TxnStateSurvivesLeaderKillAndRebuild) {
    // 1. Put a txn-state record on the leader (acksAll=true → majority-durable).
    ReplicatedTxnStateStore leaderStore(node1_.get());
    auto m = makeMeta("tx_test1", /*pid=*/42, /*epoch=*/0, TxnState::Ongoing);
    m.partitions.insert(TopicPartition{"orders", 0});
    m.partitions.insert(TopicPartition{"orders", 1});

    bool ok = leaderStore.put(m, /*coordEpoch=*/1);
    ASSERT_TRUE(ok) << "Initial put must succeed (ISR >= minIsr=2 at start)";

    // Verify leader's in-memory state.
    const auto* got = leaderStore.get("tx_test1");
    ASSERT_TRUE(got);
    EXPECT_EQ(got->state, TxnState::Ongoing);
    EXPECT_EQ(got->producerId, 42);
    EXPECT_EQ(got->partitions.size(), 2u);

    // 2. Drive replication until node2 has the batch.
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 1; }))
        << "node2 HW must reach 1 within " << kDriverCap << " iterations";

    // 3. Kill the leader: remove from transport; promote node2.
    transport_.setLeader(nullptr);
    transport_.removeFollower(kNode2);

    node2_->becomeLeader(/*newEpoch=*/2, {kNode2, kNode3});
    transport_.setLeader(node2_.get());
    node3_->becomeFollower(/*newEpoch=*/2, kNode2);

    // 4. New leader builds a ReplicatedTxnStateStore and replays the Log.
    ReplicatedTxnStateStore newLeaderStore(node2_.get());
    newLeaderStore.rebuildFromLog();

    // 5. Assert: txn-state metadata is intact — NO data loss.
    const auto* rebuilt = newLeaderStore.get("tx_test1");
    ASSERT_TRUE(rebuilt) << "Txn state must survive leader kill + Log replay";
    EXPECT_EQ(rebuilt->state, TxnState::Ongoing);
    EXPECT_EQ(rebuilt->producerId, 42);
    EXPECT_EQ(rebuilt->producerEpoch, 0);
    EXPECT_EQ(rebuilt->partitions.size(), 2u);
}

// ===========================================================================
// Test 2: Multiple state transitions all survive failover (last-write-wins)
// ===========================================================================
TEST_F(TxnStateReplicationTest, MultipleTransitionsLastWriteWinsSurvivesFailover) {
    ReplicatedTxnStateStore leaderStore(node1_.get());

    auto m1 = makeMeta("txA", 1, 0, TxnState::Ongoing);
    auto m2 = makeMeta("txB", 2, 0, TxnState::PrepareCommit);
    // Two transitions for txA: Ongoing → PrepareCommit.
    auto m3 = makeMeta("txA", 1, 0, TxnState::PrepareCommit);

    ASSERT_TRUE(leaderStore.put(m1, 1));
    ASSERT_TRUE(leaderStore.put(m2, 1));
    ASSERT_TRUE(leaderStore.put(m3, 1));

    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 3; }))
        << "node2 HW must reach 3 within " << kDriverCap << " iterations";

    // Kill leader, promote node2.
    transport_.setLeader(nullptr);
    transport_.removeFollower(kNode2);
    node2_->becomeLeader(2, {kNode2, kNode3});
    transport_.setLeader(node2_.get());
    node3_->becomeFollower(2, kNode2);

    ReplicatedTxnStateStore rebuilt(node2_.get());
    rebuilt.rebuildFromLog();

    // txA: last write was PrepareCommit.
    const auto* ra = rebuilt.get("txA");
    ASSERT_TRUE(ra);
    EXPECT_EQ(ra->state, TxnState::PrepareCommit);

    // txB: PrepareCommit.
    const auto* rb = rebuilt.get("txB");
    ASSERT_TRUE(rb);
    EXPECT_EQ(rb->state, TxnState::PrepareCommit);

    // Unknown txn returns nullptr.
    EXPECT_EQ(rebuilt.get("txC"), nullptr);
}
