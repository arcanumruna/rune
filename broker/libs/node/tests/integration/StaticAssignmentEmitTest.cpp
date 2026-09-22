// ---------------------------------------------------------------------------
// StaticAssignmentEmitTest.cpp — MD1 Task 6: on becoming the metadata-consensus
// leader, run StaticBalancer::assign() over configured topics and append the
// resulting assignment records to the replicated metadata log in bounded batches.
//
// Test cases:
//   1. SingleLeader_EmitsBalancedAssignmentOnLeaderClaim
//      3-node setup; after the metadata-replica leader transitions Follower→Leader
//      and doWork() fires the emission path, ALL nodes' MSMs show the balanced
//      + co-located assignment map (co-partition group co-location asserted).
//
//   2. Idempotent_NoDoubleEmitOnSameMembership
//      After the first emission the MSM already has assignments.  Re-running the
//      leader-claim edge (simulate by calling emitStaticAssignmentIfNeeded()
//      again) must produce ZERO additional appends — the idempotency guard fires.
//
//   3. BoundedBatching_LargeTopicSetSplitIntoBatches
//      64 single-partition topics with batchSize=16 → ≥4 batches emitted, none
//      exceeding 16 records.  Asserts >1 batch (no single oversized mega-append).
//
//   4. FollowerDoesNotEmit
//      A follower MetadataService must NOT call emitStaticAssignmentIfNeeded();
//      the MSM on a follower stays empty until records replicate.
//
// Dependencies:
//   - MetadataService::configure(topics, liveNodes, batchSize) — called by
//     BrokerNode after construction to wire topic+node context into the service.
//   - MetadataService::emitStaticAssignmentIfNeeded() — internal helper exposed
//     for test-only inspection; called by doWork() on the Follower→Leader edge.
//   - MetadataService::emitBatchCount() — test-only counter: total batch appends
//     performed since last reset; used by test 3.
//
// Uses InMemoryTransport (no Aeron required).
// NO busy-spin: all waits use bounded drive loops.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::node;
using namespace rune::metadata;
namespace pmd = rune::protocol::metadata;

// ===========================================================================
// Test helpers
// ===========================================================================

static constexpr int kDriveCap = 2000;

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct SaeTempDir {
    std::string path;
    explicit SaeTempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_sae_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~SaeTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Build a default PartitionConfig for the metadata replica (topicId=-3).
static PartitionConfig makeMetaCfg(NodeId selfId, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.topicId             = -3;  // sentinel: __cluster_metadata
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

// ===========================================================================
// Test 1: 3-node fresh cluster → metadata leader emits → all MSMs show
//         balanced + co-located assignment.
// ===========================================================================
TEST(StaticAssignmentEmitTest, SingleLeader_EmitsBalancedAssignmentOnLeaderClaim) {
    SaeTempDir dir1("t1n1"), dir2("t1n2"), dir3("t1n3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();

    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};

    // Use minIsr=2 so replication to at least one follower is required.
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode1, 2), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode2, 2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode3, 2), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());

    // Configure topics: two user topics in a co-partition group + one standalone.
    // Topics are filtered: only user (non-negative topicId) topics are assigned.
    std::vector<TopicSpec> topics = {
        TopicSpec{/*topicId=*/0, /*partitionCount=*/3, /*rf=*/1, /*coPartitionGroup=*/"grp", "orders"},
        TopicSpec{/*topicId=*/1, /*partitionCount=*/3, /*rf=*/1, /*coPartitionGroup=*/"grp", "orders"},
        TopicSpec{/*topicId=*/2, /*partitionCount=*/2, /*rf=*/1, /*coPartitionGroup=*/"",    "chat"},
    };

    // Build MetadataService for the leader (node1).
    // configure() tells the service which topics + nodes to assign.
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure(topics, allNodes, /*batchSize=*/64);

    // Trigger leader-claim emission:
    // The wasLeader_ gate fires when we first call doWork() as a Leader.
    int64_t clock = 0;

    // Drive replica + transport so the leader starts with HW=0 but is ready.
    for (int i = 0; i < 10; ++i) {
        clock += 2'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
    }

    // First doWork on the service with replica in Leader role → triggers emission.
    svc1.doWork(clock);

    // Verify the emission happened: the metadata replica must have appended records.
    // The leader replica's LEO should be > 0 (records appended).
    EXPECT_GT(r1->logEndOffset(), 0)
        << "leader must have appended assignment records after leader-claim";

    // Now drive replication until followers catch up (majority HW).
    bool replicated = false;
    for (int i = 0; i < kDriveCap && !replicated; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        // Majority = leader + at least one follower with HW >= LEO.
        if (r2->highWatermark() >= r1->logEndOffset() ||
            r3->highWatermark() >= r1->logEndOffset()) {
            replicated = true;
        }
    }
    ASSERT_TRUE(replicated) << "replication must reach majority within " << kDriveCap << " iterations";

    // Apply committed records on leader + follower services.
    MetadataService svc2(r2.get(), kNode2);
    MetadataService svc3(r3.get(), kNode3);
    svc1.doWork(clock);
    svc2.doWork(clock);
    svc3.doWork(clock);

    // --- Assert balanced assignment visible on ALL nodes ---
    // Topic 0: 3 partitions, RF=1. Leader spread across 3 nodes = 1 each.
    // Topic 1: co-partitioned with topic 0 → same leader per partition index.
    // Topic 2: 2 partitions, standalone.

    for (auto* svc : {&svc1, &svc2, &svc3}) {
        for (int p = 0; p < 3; ++p) {
            const auto* pa0 = svc->assignment(0, p);
            ASSERT_NE(pa0, nullptr)
                << "topic 0 partition " << p << " must be assigned on all nodes";

            const auto* pa1 = svc->assignment(1, p);
            ASSERT_NE(pa1, nullptr)
                << "topic 1 partition " << p << " must be assigned on all nodes";

            // Co-location: same partition index in the same co-partition group
            // must share the same leader node.
            EXPECT_EQ(pa0->leaderNode, pa1->leaderNode)
                << "topic 0 and topic 1 partition " << p
                << " must share a leader (co-location)";
        }

        for (int p = 0; p < 2; ++p) {
            const auto* pa2 = svc->assignment(2, p);
            ASSERT_NE(pa2, nullptr)
                << "topic 2 partition " << p << " must be assigned on all nodes";
        }

        // Internal topics must NOT appear in the assignment map.
        EXPECT_EQ(svc->assignment(-1, 0), nullptr)
            << "__consumer_offsets must NOT be assigned via balancer";
        EXPECT_EQ(svc->assignment(-2, 0), nullptr)
            << "__transaction_state must NOT be assigned via balancer";
        EXPECT_EQ(svc->assignment(-3, 0), nullptr)
            << "__cluster_metadata must NOT be assigned via balancer";
    }

    // Balance check for topic 0 (3 partitions, 3 nodes, RF=1 → 1 each).
    std::map<int32_t,int32_t> leaderCount;
    for (int p = 0; p < 3; ++p) {
        const auto* pa = svc1.assignment(0, p);
        ASSERT_NE(pa, nullptr);
        leaderCount[pa->leaderNode]++;
    }
    int32_t minCnt = INT32_MAX, maxCnt = INT32_MIN;
    for (auto& [node, cnt] : leaderCount) {
        minCnt = std::min(minCnt, cnt);
        maxCnt = std::max(maxCnt, cnt);
    }
    EXPECT_LE(maxCnt - minCnt, 1)
        << "leader spread for topic 0 must be ≤1 across nodes";
}

// ===========================================================================
// Test 2: Idempotency — re-running on the same membership produces no
//         duplicate/divergent assignment.
// ===========================================================================
TEST(StaticAssignmentEmitTest, Idempotent_NoDoubleEmitOnSameMembership) {
    SaeTempDir dir("t2");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    std::vector<TopicSpec> topics = {
        TopicSpec{0, 2, 1, "", "key"},
        TopicSpec{1, 3, 1, "", "key"},
    };

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes, /*batchSize=*/64);

    int64_t clock = 0;

    // First doWork: triggers leader claim → emission.
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);

    // Drive to advance HW.
    for (int i = 0; i < 50; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
    }
    svc.doWork(clock);

    // Record the LEO after first emission.
    const Offset leoAfterFirst = replica->logEndOffset();
    ASSERT_GT(leoAfterFirst, 0) << "first emission must produce records";

    // Verify assignments are visible.
    for (int p = 0; p < 2; ++p) {
        ASSERT_NE(svc.assignment(0, p), nullptr) << "topic 0 partition " << p << " must be visible";
    }
    for (int p = 0; p < 3; ++p) {
        ASSERT_NE(svc.assignment(1, p), nullptr) << "topic 1 partition " << p << " must be visible";
    }

    // Simulate a second leader-claim edge by calling emitStaticAssignmentIfNeeded()
    // directly (the idempotency guard must suppress it).
    svc.emitStaticAssignmentIfNeeded();

    const Offset leoAfterSecond = replica->logEndOffset();
    EXPECT_EQ(leoAfterFirst, leoAfterSecond)
        << "second call must not append any new records (idempotency guard)";

    // Also drive doWork() once more — the wasLeader_ gate should NOT re-fire.
    svc.doWork(clock);
    EXPECT_EQ(replica->logEndOffset(), leoAfterFirst)
        << "doWork() must not re-emit when already assigned (idempotency)";
}

// ===========================================================================
// Test 3: Bounded batching — large topic set is split into ≤batchSize batches.
// ===========================================================================
TEST(StaticAssignmentEmitTest, BoundedBatching_LargeTopicSetSplitIntoBatches) {
    SaeTempDir dir("t3");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    // 64 single-partition topics → 64 records total.
    // With batchSize=16 → at least 4 batches required.
    std::vector<TopicSpec> topics;
    topics.reserve(64);
    for (int i = 0; i < 64; ++i) {
        topics.push_back(TopicSpec{i, /*partitionCount=*/1, /*rf=*/1, /*coPartitionGroup=*/"", "key"});
    }

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes, /*batchSize=*/16);

    int64_t clock = 0;

    // Trigger emission.
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);

    // Drive until all records are committed.
    for (int i = 0; i < kDriveCap; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
        if (replica->highWatermark() >= replica->logEndOffset() && replica->logEndOffset() > 0)
            break;
    }
    svc.doWork(clock);

    // All 64 topics must have assignments.
    for (int t = 0; t < 64; ++t) {
        ASSERT_NE(svc.assignment(t, 0), nullptr)
            << "topic " << t << " partition 0 must be assigned";
    }

    // The emit batch count must be > 1 (i.e., NOT a single mega-append).
    // batchSize=16, 64 records → exactly 4 batches.
    EXPECT_GE(svc.emitBatchCount(), 4u)
        << "64 records with batchSize=16 must produce ≥4 batches (bounded batching §14)";
}

// ===========================================================================
// Test 4: Follower does NOT emit.
// ===========================================================================
TEST(StaticAssignmentEmitTest, FollowerDoesNotEmit) {
    SaeTempDir dir("t4");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 2;
    const NodeId leaderId = 1;
    const std::vector<NodeId> nodes = {leaderId, selfId};
    InMemoryTransport transport;

    // This node is a Follower.
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Follower, 1, nodes, nodes, &log, &transport);
    // No setLeader() — this replica is a follower.
    transport.addFollower(selfId, replica.get());

    std::vector<TopicSpec> topics = {
        TopicSpec{0, 3, 1, "", "key"},
    };

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes, /*batchSize=*/64);

    int64_t clock = 0;
    for (int i = 0; i < 10; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
    }

    // doWork() on a Follower must NOT trigger emission.
    svc.doWork(clock);

    // The follower must not have appended anything.
    EXPECT_EQ(replica->logEndOffset(), 0)
        << "follower must NOT append any assignment records";

    // MSM must remain empty (no records replicated from a non-existent leader).
    EXPECT_EQ(svc.assignment(0, 0), nullptr)
        << "follower MSM must remain empty (no leader records replicated)";

    // emitStaticAssignmentIfNeeded() on a follower must also be a no-op.
    svc.emitStaticAssignmentIfNeeded();
    EXPECT_EQ(replica->logEndOffset(), 0)
        << "emitStaticAssignmentIfNeeded() on follower must be a no-op";
}
