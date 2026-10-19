// ---------------------------------------------------------------------------
// OffsetReplicationTest.cpp — Integration test for CG-2 ReplicatedOffsetStore.
//
// Scenario (Kafka __consumer_offsets durability):
//   1. Stand up a 3-in-process-node cluster (leader + 2 followers) using
//      InMemoryTransport (same pattern as GatewayFailoverReplicationTest).
//   2. Leader commits an offset via ReplicatedOffsetStore with acksAll=true.
//   3. Drive replication until the commit is majority-durable (HW >= 1 on
//      both followers).
//   4. "Kill" the leader: remove it from the transport, promote follower2 to
//      leader with a new epoch.
//   5. New leader constructs a ReplicatedOffsetStore over its own replica
//      and calls rebuildFromLog().
//   6. Assert: the committed offset is present after rebuild — NO data loss.
//
// NO Docker.  All waits are bounded (kDriverCap + ASSERT_TRUE).  No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::coordination;
using namespace rune::replication;

// ===========================================================================
// Shared test helpers (mirrors PartitionReplicaTest.cpp helpers)
// ===========================================================================

static constexpr int kDriverCap = 600;

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct OrtTempDir {
    std::string path;
    explicit OrtTempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_ort_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~OrtTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

static PartitionConfig makeOrtConfig(NodeId selfId,
                                      int    minIsr           = 2,
                                      int64_t fetchIntervalNs = 1'000'000LL,
                                      int64_t isrCheckNs      = 1'000'000LL)
{
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = fetchIntervalNs;
    cfg.isrCheckIntervalNs  = isrCheckNs;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

static TopicPartition makeTp(std::string topic, int32_t part) {
    return {std::move(topic), part};
}

// ===========================================================================
// Integration fixture: 3 in-process nodes using InMemoryTransport
// ===========================================================================
class OffsetReplicationTest : public ::testing::Test {
protected:
    static constexpr NodeId kNode1 = 1;  // initial leader
    static constexpr NodeId kNode2 = 2;  // follower → new leader after failover
    static constexpr NodeId kNode3 = 3;  // follower

    OffsetReplicationTest()
        : dir1("node1"), dir2("node2"), dir3("node3")
        , log1(dir1.path), log2(dir2.path), log3(dir3.path)
    {}

    void SetUp() override {
        const std::vector<NodeId> replicas = {kNode1, kNode2, kNode3};
        const std::vector<NodeId> isr      = {kNode1, kNode2, kNode3};

        // minIsr=2 so acks=All requires 2 of 3 nodes.
        node1_ = std::make_unique<PartitionReplica>(
            makeOrtConfig(kNode1, /*minIsr=*/2),
            Role::Leader, /*epoch=*/1, replicas, isr, &log1, &transport_);

        node2_ = std::make_unique<PartitionReplica>(
            makeOrtConfig(kNode2, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log2, &transport_);

        node3_ = std::make_unique<PartitionReplica>(
            makeOrtConfig(kNode3, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log3, &transport_);

        transport_.setLeader(node1_.get());
        transport_.addFollower(kNode2, node2_.get());
        transport_.addFollower(kNode3, node3_.get());
    }

    // Drive all nodes until done() returns true or we exhaust kDriverCap.
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

    OrtTempDir dir1, dir2, dir3;
    rune::storage::Log log1, log2, log3;

    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> node1_, node2_, node3_;
    int64_t clock_ = 0;
};

// ===========================================================================
// Test: offset survives leader kill + rebuild on new leader
// ===========================================================================
TEST_F(OffsetReplicationTest, OffsetSurvivesLeaderKillAndRebuild) {
    // -----------------------------------------------------------------------
    // 1. Commit an offset on the leader (acks=All → majority-durable).
    // -----------------------------------------------------------------------
    ReplicatedOffsetStore leaderStore(node1_.get());

    const GroupId     group   = "test-group";
    const TopicPartition partition = makeTp("my-topic", 0);
    const Offset      committed = 12345;
    const std::string meta      = "some-metadata";
    const int64_t     tsNs      = 9'000'000'000LL;

    bool commitOk = leaderStore.commit(group, partition, committed, meta, tsNs);
    ASSERT_TRUE(commitOk) << "Initial commit must succeed (ISR >= minIsr=2 at start)";

    // Verify the leader's own in-memory map was updated.
    auto lco = leaderStore.fetch(group, partition);
    ASSERT_TRUE(lco.has_value());
    EXPECT_EQ(lco->offset, committed);

    // -----------------------------------------------------------------------
    // 2. Drive replication until node2 has the batch in its log (HW >= 1 on
    //    node2 means the commit is majority-durable on at least 2 nodes).
    // -----------------------------------------------------------------------
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 1; }))
        << "node2 HW must reach 1 within " << kDriverCap << " iterations";

    // Also ensure node1 (leader) sees HW >= 1.
    ASSERT_GE(node1_->highWatermark(), 1);

    // -----------------------------------------------------------------------
    // 3. "Kill" the leader: remove node1 from the transport.  Promote node2.
    // -----------------------------------------------------------------------
    transport_.setLeader(nullptr);
    transport_.removeFollower(kNode2);

    const Epoch newEpoch = 2;
    node2_->becomeLeader(newEpoch, {kNode2, kNode3});
    transport_.setLeader(node2_.get());
    // node3 stays as a follower under the new leader.
    node3_->becomeFollower(newEpoch, kNode2);

    // -----------------------------------------------------------------------
    // 4. New leader builds a ReplicatedOffsetStore and replays the Log.
    // -----------------------------------------------------------------------
    ReplicatedOffsetStore newLeaderStore(node2_.get());
    newLeaderStore.rebuildFromLog();

    // -----------------------------------------------------------------------
    // 5. Assert: the committed offset is intact — NO data loss.
    // -----------------------------------------------------------------------
    auto co = newLeaderStore.fetch(group, partition);
    ASSERT_TRUE(co.has_value())
        << "Committed offset must survive leader kill + Log replay";
    EXPECT_EQ(co->offset, committed);
    EXPECT_EQ(co->metadata, meta);
    EXPECT_EQ(co->commitTimestampNs, tsNs);
}

// ===========================================================================
// Test: multiple offsets for different groups/partitions all survive failover
// ===========================================================================
TEST_F(OffsetReplicationTest, MultipleOffsetsAllSurviveFailover) {
    ReplicatedOffsetStore leaderStore(node1_.get());

    ASSERT_TRUE(leaderStore.commit("g1", makeTp("t1", 0), 100, "m1", 1'000));
    ASSERT_TRUE(leaderStore.commit("g1", makeTp("t1", 1), 200, "m2", 2'000));
    ASSERT_TRUE(leaderStore.commit("g2", makeTp("t2", 0), 300, "m3", 3'000));
    // Lower-offset overwrite for g1/t1/p0: 100 → 50.
    ASSERT_TRUE(leaderStore.commit("g1", makeTp("t1", 0),  50, "m4", 4'000));

    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 4; }))
        << "node2 HW must reach 4 within " << kDriverCap << " iterations";

    // Kill leader, promote node2.
    transport_.setLeader(nullptr);
    transport_.removeFollower(kNode2);
    node2_->becomeLeader(2, {kNode2, kNode3});
    transport_.setLeader(node2_.get());
    node3_->becomeFollower(2, kNode2);

    ReplicatedOffsetStore rebuilt(node2_.get());
    rebuilt.rebuildFromLog();

    // g1/t1/p0: last write was offset=50 (lower-offset overwrite must win).
    auto co1 = rebuilt.fetch("g1", makeTp("t1", 0));
    ASSERT_TRUE(co1.has_value());
    EXPECT_EQ(co1->offset, 50);
    EXPECT_EQ(co1->metadata, "m4");

    auto co2 = rebuilt.fetch("g1", makeTp("t1", 1));
    ASSERT_TRUE(co2.has_value());
    EXPECT_EQ(co2->offset, 200);

    auto co3 = rebuilt.fetch("g2", makeTp("t2", 0));
    ASSERT_TRUE(co3.has_value());
    EXPECT_EQ(co3->offset, 300);

    // fetchAll for g1 must return 2 partitions.
    auto allG1 = rebuilt.fetchAll("g1");
    EXPECT_EQ(allG1.size(), 2u);
}
