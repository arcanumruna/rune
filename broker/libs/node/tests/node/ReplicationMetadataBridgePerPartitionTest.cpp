// ---------------------------------------------------------------------------
// ReplicationMetadataBridgePerPartitionTest.cpp
//
// TDD test suite for per-partition role application in ReplicationMetadataBridge.
//
// FINDING-1 (CONTROLLER RULING):
//   - INTERNAL partitions (topicId < 0): follow cluster-consensus leadership.
//   - USER partitions (topicId >= 0): follow the committed assignment map
//     (per-partition epoch + ISR, NOT the cluster term).
//
// Test cases:
//   1. UserPartition_ThisNode_BecomeLeader
//      Committed map assigns this node as leader → becomeLeader(perPartEpoch, perPartISR).
//      Per-partition leaderEpoch (5) ≠ cluster epoch (10): epoch assert FAILS if
//      the bridge accidentally uses the cluster term instead.
//
//   2. UserPartition_OtherNode_BecomeFollower
//      Committed map assigns another node as leader → becomeFollower(perPartEpoch, leaderNode).
//      Per-partition leaderEpoch (3) ≠ cluster epoch (10): epoch assert FAILS if
//      the bridge accidentally uses the cluster term.
//
//   3. DifferentUserPartitions_DifferentLeaders
//      Two user partitions with DISTINCT per-partition epochs (7 and 9, both ≠
//      cluster epoch 10) → each gets its own correct epoch AND role in one
//      doWork pass.
//
//   4. InternalPartitions_FollowClusterLeadership
//      Internal partitions (-1/-2/-3) use cluster-consensus leadership
//      regardless of any map content.
//
//   5. ApplyCommittedOnly_PendingMapNotApplied
//      An assignment above HW (not yet committed) is invisible to the bridge.
//
//   6. NoOpFastPath_UnchangedMapAndLeadership
//      Unchanged committed map + unchanged cluster leader → 0 work, no becomeX calls.
//
//   7. UserPartition_RemovedFromNode_BecomeFollower
//      A user partition previously led by this node, now reassigned → becomeFollower.
//
// All tests drive the bridge through the PRODUCTION path:
//   MetadataStateMachine msm; msm.apply(...); msm.commit(...); bridge.setMsm(&msm).
// No test-only directMap_ / deriveLocalDeltaFromMap copy is used.
//
// Single-writer / no-Aeron tests.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/metadata/MetadataStateMachine.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::replication;
using namespace rune::metadata;
using namespace rune::protocol::metadata;

// ---------------------------------------------------------------------------
// TempDir helper
// ---------------------------------------------------------------------------
struct BridgeTmpDir {
    std::string path;
    explicit BridgeTmpDir(std::string_view tag) {
        static std::atomic<int> seq{0};
        path = std::filesystem::temp_directory_path().string()
             + "/rune_bridge_pp_" + std::string(tag)
             + "_" + std::to_string(::getpid())
             + "_" + std::to_string(seq.fetch_add(1));
        std::filesystem::create_directories(path);
    }
    ~BridgeTmpDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// MockTransport — minimal no-op.
// ---------------------------------------------------------------------------
class MockTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&)                              override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&)                    override { return true; }
    bool sendOffsetsForLeaderEpochRequest(NodeId, const OffsetsForLeaderEpochReqMsg&)  override { return true; }
    bool sendOffsetsForLeaderEpochResponse(NodeId, const OffsetsForLeaderEpochRespMsg&) override { return true; }
    bool sendReconcileRequest(NodeId, const FetchRequestMsg&)                  override { return true; }
    bool sendReconcileResponse(NodeId, const FetchResponseMsg&)                override { return true; }
};

// ---------------------------------------------------------------------------
// Helper: minimal PartitionConfig for (topicId, partId, selfId).
// ---------------------------------------------------------------------------
static PartitionConfig makeCfg(int32_t topicId, PartitionId partId, NodeId selfId) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = partId;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.fastAckOnApply      = false;
    return cfg;
}

// ---------------------------------------------------------------------------
// Wire-encoding helpers for MetadataStateMachine::apply().
// Mirrors the helpers in MetadataStateMachineTest.cpp.
// ---------------------------------------------------------------------------

static std::pair<std::array<uint8_t, 256>, std::size_t> encodeCreate(
    int32_t topicId, int32_t partitionCount, int16_t rf,
    std::string_view name = "t")
{
    CreateTopicRecord rec;
    rec.topicId        = topicId;
    rec.partitionCount = partitionCount;
    rec.rf             = rf;
    rec.name           = name;
    rec.partitionKey   = "";
    rec.placementMode  = 0;
    rec.coPartitionGroup = "";

    std::array<uint8_t, 256> buf{};
    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

struct UpdateArgs {
    int32_t topicId;
    int32_t partition;
    int32_t leaderNode;
    std::vector<int32_t> isr;
    std::vector<int32_t> replicas;
    int32_t leaderEpoch    = 0;
    int32_t partitionEpoch = 0;
};

static std::pair<std::array<uint8_t, 512>, std::size_t> encodeUpdate(const UpdateArgs& a)
{
    std::array<uint8_t, 512> buf{};

    std::vector<uint8_t> isrBytes(a.isr.size() * 4u);
    for (std::size_t i = 0; i < a.isr.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.isr[i]);
        isrBytes[i*4+0] = static_cast<uint8_t>(v);
        isrBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        isrBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        isrBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }
    std::vector<uint8_t> repBytes(a.replicas.size() * 4u);
    for (std::size_t i = 0; i < a.replicas.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.replicas[i]);
        repBytes[i*4+0] = static_cast<uint8_t>(v);
        repBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        repBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        repBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }

    UpdatePartitionAssignmentRecord rec;
    rec.topicId        = a.topicId;
    rec.partition      = a.partition;
    rec.leaderNode     = a.leaderNode;
    rec.leaderEpoch    = a.leaderEpoch;
    rec.partitionEpoch = a.partitionEpoch;
    rec.isr     = Int32ArrayView(reinterpret_cast<const int32_t*>(isrBytes.data()),
                                 static_cast<uint16_t>(a.isr.size()));
    rec.replicas = Int32ArrayView(reinterpret_cast<const int32_t*>(repBytes.data()),
                                  static_cast<uint16_t>(a.replicas.size()));

    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

// ===========================================================================
// Test fixture
// ===========================================================================
class BridgePerPartitionTest : public ::testing::Test {
protected:
    static constexpr NodeId kSelf  = 1;
    static constexpr NodeId kNode2 = 2;
    static constexpr NodeId kNode3 = 3;

    BridgePerPartitionTest() : transport_() {}

    // Add a replica to the registry.
    PartitionReplica* addReplica(int32_t topicId, int32_t partitionId, NodeId selfId,
                                 Role initialRole = Role::Follower,
                                 Epoch epoch = 1)
    {
        auto dir = std::make_unique<BridgeTmpDir>(
            "t" + std::to_string(topicId) + "p" + std::to_string(partitionId));
        auto log = std::make_unique<rune::storage::Log>(dir->path);
        log->recover();

        const std::vector<NodeId> all = {kSelf, kNode2, kNode3};
        auto replica = std::make_unique<PartitionReplica>(
            makeCfg(topicId, partitionId, selfId),
            initialRole, epoch, all, all, log.get(), &transport_);

        PartitionReplica* raw = replica.get();
        registry_.add(topicId, partitionId, std::move(replica));
        logs_.push_back(std::move(log));
        dirs_.push_back(std::move(dir));
        return raw;
    }

    // Build a bridge with an attached MSM (production path).
    std::unique_ptr<ReplicationMetadataBridge> makeBridge(
        const MetadataStateMachine* msm = nullptr)
    {
        const std::vector<NodeId> allNodes = {kSelf, kNode2, kNode3};
        auto bridge = std::make_unique<ReplicationMetadataBridge>(kSelf, allNodes, registry_);
        if (msm) {
            bridge->setMsm(msm);
        }
        return bridge;
    }

    MockTransport transport_;
    ReplicaRegistry registry_;
    std::vector<std::unique_ptr<rune::storage::Log>> logs_;
    std::vector<std::unique_ptr<BridgeTmpDir>> dirs_;
};

// ===========================================================================
// Test 1: USER partition — this node is the assigned leader
//         → becomeLeader(perPartitionEpoch, perPartitionISR)
//
// Per-partition leaderEpoch = 5; cluster epoch = 10.
// EXPECT_EQ(rep->epoch(), 5u) FAILS if the bridge uses the cluster epoch instead.
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_ThisNode_BecomeLeader) {
    // Replica starts as Follower.
    PartitionReplica* rep = addReplica(/*topicId=*/0, 0, kSelf, Role::Follower, 1);

    // Populate the real MSM: topic=0, partition=0, leader=kSelf,
    // per-partition leaderEpoch=5 (deliberately different from cluster epoch=10).
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(0, 1, 1);
    msm.apply(cbuf.data(), csz, /*pos=*/0);
    msm.commit(0);

    auto [ubuf, usz] = encodeUpdate({0, 0, kSelf,
                                     {kSelf, kNode2},     // ISR: 2 nodes
                                     {kSelf, kNode2},
                                     /*leaderEpoch=*/5,
                                     /*partitionEpoch=*/7});
    msm.apply(ubuf.data(), usz, /*pos=*/1);
    msm.commit(1);

    auto bridge = makeBridge(&msm);

    // doWork with cluster-leader=kSelf, clusterEpoch=10.
    // User partitions must use the map epoch (5), not the cluster epoch (10).
    const int work = bridge->doWork(kSelf, /*clusterEpoch=*/10);

    EXPECT_GE(work, 1) << "bridge must report work done on first call with a new map";
    EXPECT_EQ(rep->role(), Role::Leader)
        << "user partition must become Leader when committed map names this node";

    // Epoch must be the PER-PARTITION leaderEpoch (5), not the cluster epoch (10).
    EXPECT_EQ(rep->epoch(), 5u)
        << "epoch must be the per-partition leaderEpoch from the MSM, not the cluster term";

    // ISR must come from the map (size 2: {kSelf, kNode2}), not allReplicaIds (size 3).
    EXPECT_EQ(rep->isr().size(), 2u) << "ISR must match the per-partition map ISR";
}

// ===========================================================================
// Test 2: USER partition — another node is the assigned leader
//         → becomeFollower(perPartitionEpoch, leaderNode)
//
// Per-partition leaderEpoch = 3; cluster epoch = 10.
// EXPECT_EQ(rep->epoch(), 3u) FAILS if the bridge uses the cluster epoch instead.
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_OtherNode_BecomeFollower) {
    // Replica starts as Leader.
    PartitionReplica* rep = addReplica(/*topicId=*/0, 0, kSelf, Role::Leader, 1);

    // Committed map: leader is kNode2 (not this node), per-partition leaderEpoch=3.
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(0, 1, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);

    auto [ubuf, usz] = encodeUpdate({0, 0, kNode2,
                                     {kSelf, kNode2, kNode3},  // ISR: all 3
                                     {kSelf, kNode2, kNode3},
                                     /*leaderEpoch=*/3,
                                     /*partitionEpoch=*/4});
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    auto bridge = makeBridge(&msm);

    // Even if cluster-leader = kSelf, user partitions use the map.
    const int work = bridge->doWork(kSelf, /*clusterEpoch=*/10);

    EXPECT_GE(work, 1) << "bridge must report work done when map changes role";
    EXPECT_EQ(rep->role(), Role::Follower)
        << "user partition must become Follower when committed map names another node";

    // Epoch must be the PER-PARTITION leaderEpoch (3), not the cluster epoch (10).
    EXPECT_EQ(rep->epoch(), 3u)
        << "epoch must be the per-partition leaderEpoch from the MSM, not the cluster term";
}

// ===========================================================================
// Test 3: Different user partitions → different leaders AND different epochs
//         in ONE doWork pass.
//
// Partition 0: per-partition leaderEpoch=7, leader=kSelf.
// Partition 1: per-partition leaderEpoch=9, leader=kNode2.
// Cluster epoch = 10 (differs from BOTH per-partition epochs).
// Each assert on rep->epoch() FAILS if the cluster term is used for either.
// ===========================================================================
TEST_F(BridgePerPartitionTest, DifferentUserPartitions_DifferentLeaders) {
    PartitionReplica* rep0 = addReplica(0, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rep1 = addReplica(0, 1, kSelf, Role::Leader,   1);

    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(0, 2, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);

    // Partition 0: this node is leader, leaderEpoch=7 (≠ cluster epoch 10).
    auto [ubuf0, usz0] = encodeUpdate({0, 0, kSelf,
                                       {kSelf, kNode2},
                                       {kSelf, kNode2},
                                       /*leaderEpoch=*/7,
                                       /*partitionEpoch=*/8});
    msm.apply(ubuf0.data(), usz0, 1);
    msm.commit(1);

    // Partition 1: kNode2 is leader, leaderEpoch=9 (≠ cluster epoch 10, ≠ p0 epoch 7).
    auto [ubuf1, usz1] = encodeUpdate({0, 1, kNode2,
                                       {kSelf, kNode2, kNode3},
                                       {kSelf, kNode2, kNode3},
                                       /*leaderEpoch=*/9,
                                       /*partitionEpoch=*/11});
    msm.apply(ubuf1.data(), usz1, 2);
    msm.commit(2);

    auto bridge = makeBridge(&msm);
    const int work = bridge->doWork(kSelf, /*clusterEpoch=*/10);

    EXPECT_GE(work, 1);

    // Partition 0: this node is leader in the map → Leader, epoch must be 7.
    EXPECT_EQ(rep0->role(), Role::Leader)
        << "partition 0: this node is leader in the map → must be Leader";
    EXPECT_EQ(rep0->epoch(), 7u)
        << "partition 0: epoch must be per-partition leaderEpoch=7, not cluster epoch=10";

    // Partition 1: kNode2 is leader → this node is Follower, epoch must be 9.
    EXPECT_EQ(rep1->role(), Role::Follower)
        << "partition 1: kNode2 is leader in the map → this node must be Follower";
    EXPECT_EQ(rep1->epoch(), 9u)
        << "partition 1: epoch must be per-partition leaderEpoch=9, not cluster epoch=10";
}

// ===========================================================================
// Test 4: INTERNAL partitions (-1/-2/-3) follow CLUSTER consensus leadership,
//          NOT the assignment map.
// ===========================================================================
TEST_F(BridgePerPartitionTest, InternalPartitions_FollowClusterLeadership) {
    PartitionReplica* rOfs  = addReplica(-1, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rTxn  = addReplica(-2, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rMeta = addReplica(-3, 0, kSelf, Role::Follower, 1);

    // MSM is empty (no user assignments) — internal partitions must still respond
    // to cluster leadership even with no MSM assignments.
    MetadataStateMachine msm;

    auto bridge = makeBridge(&msm);

    // Cluster leader = kSelf → internal partitions must all become Leader.
    const int work1 = bridge->doWork(kSelf, 5);
    EXPECT_GE(work1, 1);
    EXPECT_EQ(rOfs->role(),  Role::Leader) << "__consumer_offsets must follow cluster leader";
    EXPECT_EQ(rTxn->role(),  Role::Leader) << "__transaction_state must follow cluster leader";
    EXPECT_EQ(rMeta->role(), Role::Leader) << "__cluster_metadata must follow cluster leader";

    // Cluster leader switches to kNode2 → internal partitions become Follower.
    const int work2 = bridge->doWork(kNode2, 6);
    EXPECT_GE(work2, 1);
    EXPECT_EQ(rOfs->role(),  Role::Follower) << "__consumer_offsets must become Follower";
    EXPECT_EQ(rTxn->role(),  Role::Follower) << "__transaction_state must become Follower";
    EXPECT_EQ(rMeta->role(), Role::Follower) << "__cluster_metadata must become Follower";
}

// ===========================================================================
// Test 5: Apply-committed-only — uncommitted assignment NOT visible.
//          Record is applied at pos=5 but NOT committed (committedPos remains
//          below 5) → bridge sees an empty map → no role change.
// ===========================================================================
TEST_F(BridgePerPartitionTest, ApplyCommittedOnly_PendingMapNotApplied) {
    PartitionReplica* rep = addReplica(0, 0, kSelf, Role::Follower, 1);

    // Apply a CreateTopic and an assignment, but do NOT commit the assignment.
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(0, 1, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);  // CreateTopic committed

    // Assignment at pos=5: applied but NOT committed.
    auto [ubuf, usz] = encodeUpdate({0, 0, kSelf,
                                     {kSelf, kNode2},
                                     {kSelf, kNode2},
                                     /*leaderEpoch=*/3,
                                     /*partitionEpoch=*/4});
    msm.apply(ubuf.data(), usz, /*pos=*/5);
    // No msm.commit(5) — the record remains pending.

    auto bridge = makeBridge(&msm);

    // cluster-leader = kSelf; but user partition has no committed assignment.
    // No internal partitions registered either.
    const int work = bridge->doWork(kSelf, 5);

    // Only internal partitions react to cluster leadership; there are none registered.
    // User partition has no committed assignment → no becomeLeader call.
    EXPECT_EQ(work, 0)
        << "bridge must do 0 work when committed map is empty and no internal partitions exist";
    EXPECT_EQ(rep->role(), Role::Follower)
        << "user partition must NOT change role when its assignment is not yet committed";
}

// ===========================================================================
// Test 6: No-op fast path — same committed map AND same cluster leader.
// ===========================================================================
TEST_F(BridgePerPartitionTest, NoOpFastPath_UnchangedMapAndLeadership) {
    PartitionReplica* rep = addReplica(0, 0, kSelf, Role::Follower, 1);

    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(0, 1, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);

    auto [ubuf, usz] = encodeUpdate({0, 0, kSelf,
                                     {kSelf, kNode2},
                                     {kSelf, kNode2},
                                     /*leaderEpoch=*/2,
                                     /*partitionEpoch=*/3});
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    auto bridge = makeBridge(&msm);

    // First call: applies the map → role flips to Leader.
    const int work1 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work1, 1);
    EXPECT_EQ(rep->role(), Role::Leader);

    // Second call: same MSM (no new commits) AND same cluster leader → 0 work.
    // Delta is empty because prevMap_ was snapshotted after the first call.
    const int work2 = bridge->doWork(kSelf, 10);
    EXPECT_EQ(work2, 0)
        << "unchanged MSM + unchanged cluster leader must return 0 work (no-op fast path)";
}

// ===========================================================================
// Test 7: User partition reassigned away from this node → becomeFollower.
// Two separate MSM instances represent the before/after committed state.
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_RemovedFromNode_BecomeFollower) {
    // Start as Leader on this node.
    PartitionReplica* rep = addReplica(0, 0, kSelf, Role::Leader, 1);

    // MSM1: initial assignment — this node is leader, leaderEpoch=1.
    MetadataStateMachine msm1;
    {
        auto [cbuf, csz] = encodeCreate(0, 1, 1);
        msm1.apply(cbuf.data(), csz, 0);
        msm1.commit(0);
        auto [ubuf, usz] = encodeUpdate({0, 0, kSelf,
                                         {kSelf, kNode2},
                                         {kSelf, kNode2},
                                         /*leaderEpoch=*/1,
                                         /*partitionEpoch=*/1});
        msm1.apply(ubuf.data(), usz, 1);
        msm1.commit(1);
    }

    auto bridge = makeBridge(&msm1);

    const int work1 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work1, 1);
    EXPECT_EQ(rep->role(), Role::Leader) << "initially this node is leader per MSM";

    // MSM2: reassignment — kNode2 becomes leader, epochs bumped.
    MetadataStateMachine msm2;
    {
        auto [cbuf, csz] = encodeCreate(0, 1, 1);
        msm2.apply(cbuf.data(), csz, 0);
        msm2.commit(0);
        auto [ubuf, usz] = encodeUpdate({0, 0, kNode2,
                                         {kSelf, kNode2, kNode3},
                                         {kSelf, kNode2, kNode3},
                                         /*leaderEpoch=*/2,
                                         /*partitionEpoch=*/2});
        msm2.apply(ubuf.data(), usz, 1);
        msm2.commit(1);
    }

    // Switch the bridge to the new MSM (simulates the committed map advancing).
    bridge->setMsm(&msm2);

    const int work2 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work2, 1);
    EXPECT_EQ(rep->role(), Role::Follower)
        << "user partition must become Follower after being reassigned to kNode2";
}
