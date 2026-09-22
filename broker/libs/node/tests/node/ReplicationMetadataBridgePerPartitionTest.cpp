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
//
//   2. UserPartition_OtherNode_BecomeFollower
//      Committed map assigns another node as leader → becomeFollower(perPartEpoch, leaderNode).
//
//   3. DifferentUserPartitions_DifferentLeaders
//      Two user partitions with different leaders → each gets the correct role
//      in one doWork pass (per-partition, NOT uniform).
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
    void sendFetchRequest(const FetchRequestMsg&)                              override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&)                    override {}
    void sendOffsetsForLeaderEpochRequest(NodeId, const OffsetsForLeaderEpochReqMsg&)  override {}
    void sendOffsetsForLeaderEpochResponse(NodeId, const OffsetsForLeaderEpochRespMsg&) override {}
    void sendReconcileRequest(NodeId, const FetchRequestMsg&)                  override {}
    void sendReconcileResponse(NodeId, const FetchResponseMsg&)                override {}
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
// Helper: build a PartitionAssignment.
// ---------------------------------------------------------------------------
static PartitionAssignment makePA(int32_t leaderNode,
                                   int32_t leaderEpoch,
                                   int32_t partitionEpoch,
                                   std::vector<int32_t> isr,
                                   std::vector<int32_t> replicas = {}) {
    PartitionAssignment pa;
    pa.leaderNode      = leaderNode;
    pa.leaderEpoch     = leaderEpoch;
    pa.partitionEpoch  = partitionEpoch;
    pa.isr             = std::move(isr);
    pa.replicas        = replicas.empty() ? pa.isr : std::move(replicas);
    return pa;
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

    // Build a bridge (no initial map).
    std::unique_ptr<ReplicationMetadataBridge> makeBridge() {
        const std::vector<NodeId> allNodes = {kSelf, kNode2, kNode3};
        return std::make_unique<ReplicationMetadataBridge>(kSelf, allNodes, registry_);
    }

    MockTransport transport_;
    ReplicaRegistry registry_;
    std::vector<std::unique_ptr<rune::storage::Log>> logs_;
    std::vector<std::unique_ptr<BridgeTmpDir>> dirs_;
};

// ===========================================================================
// Test 1: USER partition — this node is the assigned leader
//         → becomeLeader(perPartitionEpoch, perPartitionISR)
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_ThisNode_BecomeLeader) {
    // Replica starts as Follower.
    PartitionReplica* rep = addReplica(/*topicId=*/0, 0, kSelf, Role::Follower, 1);

    // Build committed map: topic=0, partition=0, leader=kSelf.
    AssignmentMap cmap;
    cmap.put(0, 0, makePA(kSelf, /*leaderEpoch=*/5, /*partitionEpoch=*/7,
                           {kSelf, kNode2}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&cmap);

    // Call doWork. cluster-leader = kSelf, but user partitions use the map.
    const int work = bridge->doWork(kSelf, /*clusterEpoch=*/10);

    EXPECT_GE(work, 1) << "bridge must report work done on first call with a new map";
    EXPECT_EQ(rep->role(), Role::Leader)
        << "user partition must become Leader when committed map names this node";

    // ISR must come from the map (size 2: {kSelf, kNode2}), not allReplicaIds (size 3).
    EXPECT_EQ(rep->isr().size(), 2u) << "ISR must match the per-partition map ISR";
}

// ===========================================================================
// Test 2: USER partition — another node is the assigned leader
//         → becomeFollower(perPartitionEpoch, leaderNode)
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_OtherNode_BecomeFollower) {
    // Replica starts as Leader.
    PartitionReplica* rep = addReplica(/*topicId=*/0, 0, kSelf, Role::Leader, 1);

    // Build committed map: leader is kNode2 (not this node).
    AssignmentMap cmap;
    cmap.put(0, 0, makePA(kNode2, /*leaderEpoch=*/3, /*partitionEpoch=*/4,
                           {kSelf, kNode2, kNode3}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&cmap);

    // Even if cluster-leader = kSelf, user partitions use the map.
    const int work = bridge->doWork(kSelf, /*clusterEpoch=*/10);

    EXPECT_GE(work, 1) << "bridge must report work done when map changes role";
    EXPECT_EQ(rep->role(), Role::Follower)
        << "user partition must become Follower when committed map names another node";
}

// ===========================================================================
// Test 3: Different user partitions → different leaders in ONE doWork pass
// ===========================================================================
TEST_F(BridgePerPartitionTest, DifferentUserPartitions_DifferentLeaders) {
    // Partition 0: leader = kSelf. Partition 1: leader = kNode2.
    PartitionReplica* rep0 = addReplica(0, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rep1 = addReplica(0, 1, kSelf, Role::Leader,   1);

    AssignmentMap cmap;
    cmap.put(0, 0, makePA(kSelf,  2, 3, {kSelf, kNode2}));
    cmap.put(0, 1, makePA(kNode2, 2, 3, {kSelf, kNode2, kNode3}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&cmap);
    const int work = bridge->doWork(kSelf, 10);

    EXPECT_GE(work, 1);
    EXPECT_EQ(rep0->role(), Role::Leader)
        << "partition 0: this node is leader in the map → must be Leader";
    EXPECT_EQ(rep1->role(), Role::Follower)
        << "partition 1: kNode2 is leader in the map → this node must be Follower";
}

// ===========================================================================
// Test 4: INTERNAL partitions (-1/-2/-3) follow CLUSTER consensus leadership,
//          NOT the assignment map.
// ===========================================================================
TEST_F(BridgePerPartitionTest, InternalPartitions_FollowClusterLeadership) {
    PartitionReplica* rOfs  = addReplica(-1, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rTxn  = addReplica(-2, 0, kSelf, Role::Follower, 1);
    PartitionReplica* rMeta = addReplica(-3, 0, kSelf, Role::Follower, 1);

    // Committed map with bogus internal entries claiming kNode2 is leader.
    // Bridge MUST ignore these for internal partitions.
    AssignmentMap cmap;
    cmap.put(-1, 0, makePA(kNode2, 1, 1, {kNode2}));
    cmap.put(-2, 0, makePA(kNode2, 1, 1, {kNode2}));
    cmap.put(-3, 0, makePA(kNode2, 1, 1, {kNode2}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&cmap);

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
// Test 5: Apply-committed-only — empty committed map → no role change for
//          user partition.
// ===========================================================================
TEST_F(BridgePerPartitionTest, ApplyCommittedOnly_PendingMapNotApplied) {
    PartitionReplica* rep = addReplica(0, 0, kSelf, Role::Follower, 1);

    // Empty committed map — no user assignments committed yet.
    AssignmentMap emptyMap;

    auto bridge = makeBridge();
    bridge->setCommittedMap(&emptyMap);

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

    AssignmentMap cmap;
    cmap.put(0, 0, makePA(kSelf, 2, 3, {kSelf, kNode2}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&cmap);

    // First call: applies the map → role flips to Leader.
    const int work1 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work1, 1);
    EXPECT_EQ(rep->role(), Role::Leader);

    // Second call: same map pointer AND same cluster leader → 0 work.
    // Delta is empty because prevMap_ was snapshotted after the first call.
    const int work2 = bridge->doWork(kSelf, 10);
    EXPECT_EQ(work2, 0)
        << "unchanged map + unchanged cluster leader must return 0 work (no-op fast path)";
}

// ===========================================================================
// Test 7: User partition reassigned away from this node → becomeFollower
// ===========================================================================
TEST_F(BridgePerPartitionTest, UserPartition_RemovedFromNode_BecomeFollower) {
    // Start as Leader on this node.
    PartitionReplica* rep = addReplica(0, 0, kSelf, Role::Leader, 1);

    // Initial map: this node is leader.
    AssignmentMap map1;
    map1.put(0, 0, makePA(kSelf, 1, 1, {kSelf, kNode2}));

    auto bridge = makeBridge();
    bridge->setCommittedMap(&map1);

    const int work1 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work1, 1);
    EXPECT_EQ(rep->role(), Role::Leader) << "initially this node is leader per map";

    // Reassign: kNode2 becomes leader (bump epochs).
    AssignmentMap map2;
    map2.put(0, 0, makePA(kNode2, 2, 2, {kSelf, kNode2, kNode3}));

    // Point the bridge at the new map.
    bridge->setCommittedMap(&map2);

    const int work2 = bridge->doWork(kSelf, 10);
    EXPECT_GE(work2, 1);
    EXPECT_EQ(rep->role(), Role::Follower)
        << "user partition must become Follower after being reassigned to kNode2";
}
