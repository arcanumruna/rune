// ---------------------------------------------------------------------------
// StaticAssignmentEmitTest.cpp — MD1 Task 6: on becoming the metadata-consensus
// leader, run StaticBalancer::assign() over configured topics and append the
// resulting assignment records to the replicated metadata log.
//
// Test cases:
//   1. SingleLeader_EmitsBalancedAssignmentOnLeaderClaim
//      3-node setup; after the metadata-replica leader transitions Follower→Leader
//      and doWork() fires the emission path, ALL nodes' MSMs show the balanced
//      + co-located assignment map (co-partition group co-location asserted).
//
//   2. Idempotent_NoDoubleEmitOnRePromotion
//      Drive a genuine demotion→re-promotion flap (Leader→Follower→Leader).
//      After the first emission, demote the replica to Follower (re-arms the
//      assigned_ / wasLeader_ guards), then promote a fresh MetadataService
//      over the same already-populated log (restart scenario) and assert that
//      the second leader-claim produces ZERO additional log entries —
//      msmAlreadyHasAllAssignments() must fire and suppress re-emission.
//      The test FAILS if the guard doesn't prevent re-emit.
//
//   3. OneRecordPerAppend_LargeTopicSetNeverMegaAppend
//      Real one-record-per-append invariant: emit for 64 single-partition
//      topics and assert logEndOffset() advanced by EXACTLY 64 (one log entry
//      per assignment record).  If records were concatenated into a single
//      mega-append, logEndOffset() would advance by 1 and this test would fail.
//
//   4. FollowerDoesNotEmit
//      A follower MetadataService must NOT call emitStaticAssignmentIfNeeded();
//      the MSM on a follower stays empty until records replicate.
//
//   5. BalancerError_IncrementCounterEmitsNothing
//      A misconfigured topic set (RF > liveNodes) trips balancerErrorCount()
//      and leaves the log empty.
//
// Dependencies:
//   - MetadataService::configure(topics, liveNodes) — called by BrokerNode
//     after construction to wire topic+node context into the service.
//   - MetadataService::emitStaticAssignmentIfNeeded() — internal helper exposed
//     for test-only inspection; called by doWork() on the Follower→Leader edge.
//   - MetadataService::balancerErrorCount() — diagnostic counter; incremented
//     when BalancerError is caught inside emitStaticAssignmentIfNeeded().
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
    svc1.configure(topics, allNodes);

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
        // Kafka model: committed once on every ISR member; both followers learn it.
        if (r2->highWatermark() >= r1->logEndOffset() &&
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
// Test 2: Genuine idempotency via demotion→re-promotion flap.
//
// Drives: Leader (emit) → Follower (re-arms guards) → Leader again.
// The second leader-claim must NOT produce additional log entries because
// msmAlreadyHasAllAssignments() fires on rebuild from the already-populated log.
//
// Failure mode: if the guard is broken, logEndOffset() advances after the
// second promotion and this test fails.
// ===========================================================================
TEST(StaticAssignmentEmitTest, Idempotent_NoDoubleEmitOnRePromotion) {
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
    svc.configure(topics, nodes);

    int64_t clock = 0;

    // --- First leadership term: emit ---
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);  // Follower→Leader edge: emits records

    // Drive until all records are committed (HW advances).
    for (int i = 0; i < 200; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
    }
    svc.doWork(clock);

    const Offset leoAfterFirst = replica->logEndOffset();
    ASSERT_GT(leoAfterFirst, 0) << "first emission must produce records";

    // Verify all assignments are visible.
    for (int p = 0; p < 2; ++p) {
        ASSERT_NE(svc.assignment(0, p), nullptr) << "topic 0 partition " << p << " must be visible";
    }
    for (int p = 0; p < 3; ++p) {
        ASSERT_NE(svc.assignment(1, p), nullptr) << "topic 1 partition " << p << " must be visible";
    }

    // --- Demotion: flip replica to Follower (re-arms assigned_ / wasLeader_) ---
    // becomeFollower() on the replica changes its role; doWork() on the service
    // will detect !amLeader && wasLeader_ and reset wasLeader_ + assigned_.
    replica->becomeFollower(/*epoch=*/2, /*leaderId=*/99);
    svc.doWork(clock);  // service detects demotion, re-arms

    // Confirm re-arm: wasLeader_ is reset so the next Leader transition will
    // fire the leader-edge again.

    // --- Re-promotion: construct a FRESH MetadataService over the same log ---
    // This is the most stringent test: a fresh service has no memory of the
    // first emit.  On becoming leader it calls rebuildFromLog() then
    // emitStaticAssignmentIfNeeded().  The msmAlreadyHasAllAssignments() guard
    // must detect the committed records from the first term and skip re-emission.
    replica->becomeLeader(/*epoch=*/3, /*isr=*/nodes);
    transport.setLeader(replica.get()); // re-wire transport

    MetadataService svc2(replica.get(), selfId);
    svc2.configure(topics, nodes);

    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc2.doWork(clock);  // Follower→Leader edge on the fresh service

    // Drive a few more cycles so any (erroneous) appends could commit.
    for (int i = 0; i < 50; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
    }
    svc2.doWork(clock);

    const Offset leoAfterRepromotion = replica->logEndOffset();
    EXPECT_EQ(leoAfterFirst, leoAfterRepromotion)
        << "re-promoted service must NOT append any new records "
           "(msmAlreadyHasAllAssignments guard must fire)";

    // All assignments must still be visible on the new service (built from replay).
    for (int p = 0; p < 2; ++p) {
        EXPECT_NE(svc2.assignment(0, p), nullptr) << "topic 0 partition " << p << " must be visible after re-promotion";
    }
    for (int p = 0; p < 3; ++p) {
        EXPECT_NE(svc2.assignment(1, p), nullptr) << "topic 1 partition " << p << " must be visible after re-promotion";
    }
}

// ===========================================================================
// Test 3: One-record-per-append invariant — large topic set must NOT become
//         a single mega-append.
//
// Emits for 64 single-partition topics and asserts logEndOffset() advanced by
// EXACTLY 64 (one log entry per metadata record).
//
// Failure mode: if records were concatenated into one mega-append payload,
// logEndOffset() would advance by 1 (not 64) and this test would fail.
// This is the discriminating test for the "one append per record" invariant.
// ===========================================================================
TEST(StaticAssignmentEmitTest, OneRecordPerAppend_LargeTopicSetNeverMegaAppend) {
    SaeTempDir dir("t3");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    // 64 single-partition topics → expect exactly 64 log entries after emission.
    constexpr int kTopicCount = 64;
    std::vector<TopicSpec> topics;
    topics.reserve(kTopicCount);
    for (int i = 0; i < kTopicCount; ++i) {
        topics.push_back(TopicSpec{i, /*partitionCount=*/1, /*rf=*/1, /*coPartitionGroup=*/"", "key"});
    }

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes);

    int64_t clock = 0;

    // Trigger emission (Follower→Leader edge).
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);

    // LEO immediately after append: should be exactly kTopicCount if each record
    // got its own append call.  If a mega-append was used, this would be 1.
    const Offset leoAfterEmit = replica->logEndOffset();
    EXPECT_EQ(leoAfterEmit, static_cast<Offset>(kTopicCount))
        << "logEndOffset must advance by exactly " << kTopicCount
        << " (one log entry per assignment record); got " << leoAfterEmit
        << " — a value of 1 means records were incorrectly mega-appended";

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
    for (int t = 0; t < kTopicCount; ++t) {
        ASSERT_NE(svc.assignment(t, 0), nullptr)
            << "topic " << t << " partition 0 must be assigned";
    }

    // Confirm: HW must equal the number of records (not 1).
    EXPECT_GE(replica->highWatermark(), static_cast<Offset>(kTopicCount))
        << "highWatermark must cover all " << kTopicCount << " records";
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
    svc.configure(topics, nodes);

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

// ===========================================================================
// Test 5: BalancerError increments counter and emits nothing.
//
// A misconfigured topic set (RF > liveNodes) must:
//   - NOT append any records to the log.
//   - Increment balancerErrorCount() by at least 1.
//
// Failure mode: if the error is swallowed silently without incrementing the
// counter, balancerErrorCount() returns 0 and this test fails.
// ===========================================================================
TEST(StaticAssignmentEmitTest, BalancerError_IncrementCounterEmitsNothing) {
    SaeTempDir dir("t5");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};  // only 1 node
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    // rf=3 but only 1 live node → StaticBalancer must throw BalancerError.
    std::vector<TopicSpec> topics = {
        TopicSpec{0, 2, /*rf=*/3, "", "key"},  // RF=3 > 1 node: invalid
    };

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes);

    int64_t clock = 0;
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);  // triggers emitStaticAssignmentIfNeeded(); balancer must throw

    // No records must have been appended.
    EXPECT_EQ(replica->logEndOffset(), 0)
        << "no assignment records must be appended when BalancerError is thrown";

    // The error counter must be non-zero.
    EXPECT_GT(svc.balancerErrorCount(), 0u)
        << "balancerErrorCount() must be non-zero after a BalancerError "
           "(verify StaticBalancer throws for RF > liveNodes)";

    // MSM must remain empty.
    EXPECT_EQ(svc.assignment(0, 0), nullptr)
        << "assignment must be absent when balancer threw an error";
}

// ---------------------------------------------------------------------------
// CRITICAL-2 (review of 0ff9322) — the metadata partition is led by the
// consensus leader.  Under the majority-of-ISR HW + leader-completeness gate a
// new metadata leader appended nothing until a validated majority reported in,
// and could never complete when a survivor held an uncommitted tail: the
// control plane froze.  Kafka model: HW = min over the ISR, the internal ISR
// never lag-shrinks a live member — the new metadata leader holds every
// committed record and is writable AT ONCE: static emit, control ticks and
// dynamic records are appended immediately (never an ignored append result) and
// commit as soon as the ISR acknowledges them.
// ---------------------------------------------------------------------------
TEST(MetadataGateRegression, NewMetadataLeaderWritableAtOnce_NoCompletenessFreeze) {
    SaeTempDir dir1("mg1"), dir2("mg2"), dir3("mg3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode1), Role::Follower, 0, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode2), Role::Follower, 0, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode3), Role::Follower, 0, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    r1->becomeLeader(1, allNodes);               // no follower has reported yet
    ASSERT_TRUE(r1->acceptsProduce()) << "a new metadata leader must be writable at once";

    std::vector<TopicSpec> topics = {TopicSpec{0, 3, 1, "", "t"}};
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure(topics, allNodes);
    svc1.armControlTick(/*deadlineNs=*/0);
    int64_t clock = 1'000'000LL;
    bool ticked = false;
    for (int i = 0; i < 5; ++i) {
        clock += 1'000'000LL;
        (void)svc1.doWork(clock);
        svc1.emitStaticAssignmentIfNeeded();
        ticked = svc1.pollAndAppendControlTick(clock) || ticked;
    }
    EXPECT_GT(r1->logEndOffset(), 0) << "the metadata leader appended nothing (frozen)";
    EXPECT_TRUE(ticked) << "the control tick was never appended";

    // The followers fetch → the records commit (HW = min over the ISR).
    r2->becomeFollower(1, kNode1);
    r3->becomeFollower(1, kNode1);
    bool done = false;
    for (int i = 0; i < kDriveCap && !done; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        (void)svc1.doWork(clock);
        (void)svc1.pollAndAppendControlTick(clock);
        done = svc1.assignment(0, 0) != nullptr && svc1.assignment(0, 2) != nullptr &&
               svc1.lastEmittedTickSeq() >= 0;
    }
    ASSERT_TRUE(done) << "leo=" << r1->logEndOffset() << " hw=" << r1->highWatermark();
    EXPECT_GT(r1->highWatermark(), 0);
}

// ---------------------------------------------------------------------------
// Fix round 1 (Kafka AlterPartition, controller side): MetadataService accepts
// an ISR proposal only from the CURRENT leader at the CURRENT leaderEpoch, with
// the leader in the ISR and every member a replica, and only when no other
// change for that partition is in flight; the committed record keeps the
// leader / leaderEpoch and bumps partitionEpoch.
// ---------------------------------------------------------------------------
TEST(AlterIsrControllerRegression, ControllerCommitsOnlyValidProposalsFromTheCurrentLeader) {
    SaeTempDir dir1("ai1"), dir2("ai2"), dir3("ai3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    const int32_t leader = pa->leaderNode, le = pa->leaderEpoch, pe = pa->partitionEpoch;
    ASSERT_EQ(pa->isr.size(), 3u);
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = leader; m.leaderEpoch = le;
    m.isrCount = 2;
    m.isr[0] = leader; m.isr[1] = (leader == 1) ? 2 : 1;
    m.partitionEpoch = pe;                       // derived from the committed {1,2,3}
    // Stale epoch / not the leader / leader missing from the ISR / non-replica: rejected.
    auto stale = m; stale.leaderEpoch = le - 1;
    EXPECT_FALSE(svc1.onAlterIsr(stale));
    auto notLeader = m; notLeader.leaderId = (leader == 3) ? 2 : 3;
    EXPECT_FALSE(svc1.onAlterIsr(notLeader));
    auto noLeader = m; noLeader.isr[0] = (leader == 3) ? 2 : 3; noLeader.isr[1] = (leader == 1) ? 2 : 1;
    if (noLeader.isr[0] == noLeader.isr[1]) noLeader.isr[1] = 99;
    EXPECT_FALSE(svc1.onAlterIsr(noLeader));
    auto alien = m; alien.isr[1] = 42;
    EXPECT_FALSE(svc1.onAlterIsr(alien));
    // Valid: accepted once; a duplicate while in flight is not re-appended.
    EXPECT_TRUE(svc1.onAlterIsr(m));
    EXPECT_FALSE(svc1.onAlterIsr(m));
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->isr.size(), 2u);
    EXPECT_EQ(pa->leaderNode, leader);
    EXPECT_EQ(pa->leaderEpoch, le) << "an ISR change never bumps the leader epoch";
    EXPECT_EQ(pa->partitionEpoch, pe + 1);
    EXPECT_FALSE(svc1.onAlterIsr(m)) << "already committed";

    // Compare-and-set (Kafka's partitionEpoch check): a proposal derived from an
    // OLDER committed assignment (a stale / re-ordered AlterIsr — here one
    // re-adding the member the committed shrink removed) is never committed and
    // is answered INVALID_UPDATE_VERSION; the same proposal derived from the
    // current committed assignment is committed.
    const int32_t out = (m.isr[0] != 1 && m.isr[1] != 1) ? 1 : (m.isr[0] != 2 && m.isr[1] != 2) ? 2 : 3;
    auto readd = m; readd.isrCount = 3; readd.isr[2] = out;
    auto staleBase = readd;                      // partitionEpoch of the old {1,2,3}
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    EXPECT_FALSE(svc1.onAlterIsr(staleBase, &resp, &answered)) << "committed a proposal derived from a stale ISR";
    EXPECT_TRUE(answered);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrInvalidUpdateVersion);
    EXPECT_EQ(resp.partitionEpoch, pe + 1);
    readd.partitionEpoch = pe + 1;
    EXPECT_TRUE(svc1.onAlterIsr(readd));
}

// Kafka broker fencing (controller side): a node the cluster considers dead
// leaves the COMMITTED ISR of every partition whose leader is alive — with HW =
// min over the ISR it would otherwise pin the HW (acks=all) until the leader's
// own lag check — and the leader / leaderEpoch stay unchanged (partitionEpoch
// + 1).  The fenced node re-enters only through its leader's AlterIsr.
TEST(AlterIsrControllerRegression, ControllerFencesDeadMembersOutOfTheCommittedIsr) {
    SaeTempDir dir1("fe1"), dir2("fe2"), dir3("fe3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    ASSERT_EQ(pa->isr.size(), 3u);
    const int32_t leader = pa->leaderNode, le = pa->leaderEpoch, pe = pa->partitionEpoch;
    const int32_t dead = (leader == 3) ? 2 : 3;
    std::vector<rune::metadata::NodeId> live;
    for (int32_t n : allNodes) if (n != dead) live.push_back(n);
    EXPECT_GE(svc1.runDynamicRebalanceOnTick(live, nullptr), 1u);
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(std::find(pa->isr.begin(), pa->isr.end(), dead), pa->isr.end()) << "a dead member stayed in the committed ISR";
    EXPECT_EQ(pa->leaderNode, leader);
    EXPECT_EQ(pa->leaderEpoch, le);
    EXPECT_EQ(pa->partitionEpoch, pe + 1);
    EXPECT_EQ(svc1.runDynamicRebalanceOnTick(live, nullptr), 0u) << "steady state must emit nothing";
}

// Review 2026-10-04 MEDIUM-1 (ABA): the AlterIsr compare-and-set compared ISR
// SETS.  Committed {L,Y,X} → shrink to {L,Y}; the leader proposes P1 = add X
// (base {L,Y}); Y is fenced ({L}) and re-admitted ({L,Y}); a LATE copy of P1
// then matches the committed set again and committed X — which the HW had
// stopped counting.  Kafka compares the monotonic partitionEpoch: never.
TEST(AlterIsrControllerRegression, PartitionEpochCas_LateProposalNeverCommitsAfterAba) {
    SaeTempDir dir1("aba1"), dir2("aba2"), dir3("aba3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    ASSERT_EQ(pa->isr.size(), 3u);
    const int32_t L = pa->leaderNode, le = pa->leaderEpoch;
    const int32_t Y = (L == 1) ? 2 : 1;
    const int32_t X = 6 - L - Y;
    auto propose = [&](std::vector<int32_t> isr, const rune::metadata::PartitionAssignment& base) {
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = le;
        m.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) m.isr[i] = isr[i];
        m.partitionEpoch = base.partitionEpoch;
        return m;
    };
    ASSERT_TRUE(svc1.onAlterIsr(propose({L, Y}, *pa)));          // shrink X
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_EQ(pa->isr.size(), 2u);
    const auto p1 = propose({L, Y, X}, *pa);                       // P1: add X (base {L,Y})
    ASSERT_TRUE(svc1.onAlterIsr(propose({L}, *pa)));              // Y fenced
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_EQ(pa->isr.size(), 1u);
    ASSERT_TRUE(svc1.onAlterIsr(propose({L, Y}, *pa)));           // Y re-admitted
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_EQ(pa->isr.size(), 2u);
    EXPECT_FALSE(svc1.onAlterIsr(p1)) << "a late proposal committed after an ISR ABA";
    drive(50);
    pa = svc1.assignment(0, 0);
    EXPECT_EQ(std::find(pa->isr.begin(), pa->isr.end(), X), pa->isr.end())
        << "X entered the committed ISR off a stale proposal";
}

// Review 2026-10-04 MEDIUM-2 (KIP-841): the controller never ADDS a node it
// considers dead (fenced) to a committed ISR — otherwise fencing and the
// leader's re-admission alternate forever under an asymmetric partition (two
// metadata records per partition per cycle).
TEST(AlterIsrControllerRegression, ControllerNeverAddsAFencedReplica) {
    SaeTempDir dir1("ine1"), dir2("ine2"), dir3("ine3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    const int32_t L = pa->leaderNode, le = pa->leaderEpoch;
    const int32_t dead = (L == 3) ? 2 : 3;
    std::vector<rune::metadata::NodeId> live;
    for (int32_t n : allNodes) if (n != dead) live.push_back(n);
    ASSERT_GE(svc1.runDynamicRebalanceOnTick(live, nullptr), 1u);   // fences `dead`
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_EQ(std::find(pa->isr.begin(), pa->isr.end(), dead), pa->isr.end());
    // The leader (which still sees `dead` fetching) proposes to re-add it.
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = le;
    m.isrCount = static_cast<int32_t>(pa->isr.size()) + 1;
    for (std::size_t i = 0; i < pa->isr.size(); ++i) m.isr[i] = pa->isr[i];
    m.isr[pa->isr.size()] = dead;
    m.partitionEpoch = pa->partitionEpoch;
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    EXPECT_FALSE(svc1.onAlterIsr(m, &resp, &answered)) << "the controller committed a fenced replica into the ISR";
    EXPECT_TRUE(answered) << "an ineligible-only proposal must be answered (else it pins the leader)";
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrIneligibleReplica);
    EXPECT_EQ(resp.partitionEpoch, pa->partitionEpoch);
    drive(50);
    pa = svc1.assignment(0, 0);
    EXPECT_EQ(std::find(pa->isr.begin(), pa->isr.end(), dead), pa->isr.end());
}


// ===========================================================================
// Re-review 2026-10-04 (fix round 2) MEDIUM-B: the coordinator partitions
// (__consumer_offsets -1, __transaction_state -2) moved from the internal-ISR /
// consensus-candidacy path to the controller-ISR (user-partition) path: the
// controller commits their ISR (AlterIsr) and elects their leader from it, so
// one of their ISRs can no longer take the whole control plane down.  The
// coordinator lives on the consensus leader, so the controller prefers itself
// — but only when it is in the committed ISR (clean); otherwise a live ISR
// member leads (the controller catches up from it, is admitted by its AlterIsr,
// then leadership moves to the controller) or the partition is offline.
// ===========================================================================
namespace {
rune::metadata::PartitionAssignment coordPa(int32_t leader, std::vector<int32_t> isr,
                                            int32_t le = 5, int32_t pe = 9) {
    rune::metadata::PartitionAssignment pa;
    pa.leaderNode = leader; pa.isr = std::move(isr); pa.replicas = {1, 2, 3};
    pa.leaderEpoch = le; pa.partitionEpoch = pe;
    return pa;
}
} // namespace

TEST(CoordinatorPartitionRegression, DecisionFollowsUserPartitionRules) {
    using rune::metadata::NO_LEADER;
    const std::vector<int32_t> replicas = {1, 2, 3};
    auto decide = [&](int32_t self, const rune::metadata::PartitionAssignment* cur,
                      std::vector<int32_t> eligible) {
        return MetadataService::decideCoordinatorPartition(-1, 0, self, cur, eligible, nullptr,
                                                           /*bootstrapEpoch=*/4, replicas);
    };
    {   // No assignment yet (fresh cluster): the controller leads, every replica seeds the ISR.
        const auto d = decide(2, nullptr, {1, 2, 3});
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->leaderNode, 2);
        EXPECT_EQ(d->isr.size(), 3u);
        EXPECT_EQ(d->leaderEpoch, 4);
    }
    {   // Already the controller's: nothing to do.
        const auto pa = coordPa(2, {2, 3});
        EXPECT_FALSE(decide(2, &pa, {1, 2, 3}).has_value());
    }
    {   // MEDIUM-B scenario: L (1) dead, ISR {1,3}, controller A (2) NOT in it:
        // elect the live ISR member B (3) — never the controller (it lacks
        // committed offsets records).
        const auto pa = coordPa(1, {1, 3});
        const auto d = decide(2, &pa, {2, 3});
        ASSERT_TRUE(d.has_value()) << "the partition was left on a dead leader";
        EXPECT_EQ(d->leaderNode, 3) << "elected a node outside the committed ISR";
        EXPECT_EQ(d->isr, (std::vector<int32_t>{3}));
        EXPECT_EQ(d->leaderEpoch, 6);
        EXPECT_EQ(d->partitionEpoch, 10);
    }
    {   // ... and B dead too: offline (NO_LEADER), the ISR lineage kept.
        const auto pa = coordPa(1, {1, 3});
        const auto d = decide(2, &pa, {2});
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->leaderNode, NO_LEADER) << "unclean election of the controller";
        EXPECT_EQ(d->isr, (std::vector<int32_t>{1, 3}));
        const auto again = coordPa(NO_LEADER, {1, 3}, 6, 10);
        EXPECT_FALSE(decide(2, &again, {2}).has_value()) << "steady offline state must emit nothing";
    }
    {   // B leads, the controller A caught up and was admitted: move to A.
        const auto pa = coordPa(3, {3, 2});
        const auto d = decide(2, &pa, {2, 3});
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->leaderNode, 2);
        EXPECT_EQ(d->leaderEpoch, 6);
        EXPECT_EQ(d->isr.front(), 2);
        EXPECT_EQ(d->isr.size(), 2u);
    }
    {   // B leads, A not yet admitted: leave it (A catches up from B first).
        const auto pa = coordPa(3, {3});
        EXPECT_FALSE(decide(2, &pa, {2, 3}).has_value());
    }
    {   // The controller is in the ISR and the old leader died: straight to the controller.
        const auto pa = coordPa(1, {1, 2, 3});
        const auto d = decide(2, &pa, {2, 3});
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->leaderNode, 2);
    }
}

// The controller bootstraps the coordinator partitions' assignments, commits
// their ISR changes (AlterIsr — internal topic ids no longer refused) and the
// bridge drives their replicas from the committed map, NOT from consensus
// leadership.
TEST(CoordinatorPartitionRegression, ControllerCommitsCoordinatorIsrAndBridgeFollowsTheMap) {
    SaeTempDir dir1("cp1"), dir2("cp2"), dir3("cp3"), dirO("cpo");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path), logO(dirO.path);
    log1.recover(); log2.recover(); log3.recover(); logO.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    svc1.setCoordinatorPartitions({{-1, 0}, {-2, 0}}, {1, 2, 3});
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    (void)svc1.runDynamicRebalanceOnTick({1, 2, 3}, nullptr);
    drive(50);
    const auto* pa = svc1.assignment(-1, 0);
    ASSERT_NE(pa, nullptr) << "the controller never assigned __consumer_offsets";
    ASSERT_NE(svc1.assignment(-2, 0), nullptr) << "the controller never assigned __transaction_state";
    EXPECT_EQ(pa->leaderNode, kNode1);
    EXPECT_EQ(pa->isr.size(), 3u);
    // AlterIsr for the coordinator partition: committed by the controller.
    rune::replication::AlterIsrMsg m{};
    m.topicId = -1; m.partition = 0; m.leaderId = kNode1; m.leaderEpoch = pa->leaderEpoch;
    m.partitionEpoch = pa->partitionEpoch;
    m.isrCount = 2; m.isr[0] = 1; m.isr[1] = 3;
    EXPECT_TRUE(svc1.onAlterIsr(m)) << "the controller refused a coordinator-partition AlterIsr";
    drive(50);
    pa = svc1.assignment(-1, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->isr, (std::vector<int32_t>{1, 3}));
    EXPECT_EQ(svc1.runDynamicRebalanceOnTick({1, 2, 3}, nullptr), 0u) << "steady state must emit nothing";
    // Node 2's offsets replica, driven by node 2's bridge over the committed map.
    ReplicaRegistry reg;
    PartitionConfig ocfg = makeMetaCfg(kNode2);
    ocfg.topicId = -1;
    reg.add(-1, 0, std::make_unique<PartitionReplica>(ocfg, Role::Follower, 0, allNodes, allNodes, &logO, &transport));
    ReplicationMetadataBridge bridge(kNode2, allNodes, reg, &svc1.msmForTest());
    bridge.setCoordinatorPartitionsOnController(true);
    (void)bridge.doWork(/*consensusLeader=*/kNode1, /*term=*/7);
    PartitionReplica* off = reg.lookup(-1, 0);
    ASSERT_NE(off, nullptr);
    EXPECT_EQ(off->role(), Role::Follower);
    EXPECT_EQ(off->epoch(), pa->leaderEpoch) << "the coordinator replica took the consensus term, not the map's epoch";
    // Node 2 becomes the CONSENSUS leader while it is outside the committed
    // offsets ISR {1,3}: it must NOT lead the offsets partition (it lacks
    // committed records) — the map decides.
    (void)bridge.doWork(/*consensusLeader=*/kNode2, /*term=*/8);
    EXPECT_EQ(off->role(), Role::Follower)
        << "the consensus leader led a coordinator partition outside its committed ISR";
}

// ===========================================================================
// Epoch-restart fix 2026-10-05 (controller side).
// ===========================================================================

// A committed leader that refused its committed epoch after a restart (the
// PartitionReplica restart fence) asks for a new one: AlterIsr with
// isrCount == AlterIsrMsg::kLeaderEpochBump.  The controller commits the SAME
// assignment (leader, ISR, replicas) at leaderEpoch + 1 / partitionEpoch + 1 —
// once: a re-send while in flight, a stale request (old epoch) or one from a
// non-leader commits nothing.
TEST(EpochRestartControllerRegression, BumpRequestCommitsSameAssignmentAtNextEpoch) {
    SaeTempDir dir1("eb1"), dir2("eb2"), dir3("eb3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    const std::vector<NodeId> allNodes = {1, 2, 3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(2, r2.get());
    transport.addFollower(3, r3.get());
    MetadataService svc1(r1.get(), 1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(50);
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    const int32_t leader = pa->leaderNode, le = pa->leaderEpoch, pe = pa->partitionEpoch;
    const std::vector<int32_t> isr = pa->isr;
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = leader; m.leaderEpoch = le;
    m.isrCount = rune::replication::AlterIsrMsg::kLeaderEpochBump;
    m.partitionEpoch = pe;
    auto notLeader = m; notLeader.leaderId = leader == 3 ? 2 : 3;
    EXPECT_FALSE(svc1.onAlterIsr(notLeader));
    auto staleEpoch = m; staleEpoch.leaderEpoch = le - 1;
    EXPECT_FALSE(svc1.onAlterIsr(staleEpoch));
    EXPECT_TRUE(svc1.onAlterIsr(m)) << "the controller ignored a leader-epoch bump request";
    EXPECT_FALSE(svc1.onAlterIsr(m)) << "a re-send while in flight bumped twice";
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->leaderNode, leader) << "a bump keeps the leader";
    EXPECT_EQ(pa->leaderEpoch, le + 1);
    EXPECT_EQ(pa->partitionEpoch, pe + 1);
    EXPECT_EQ(pa->isr, isr) << "a bump keeps the ISR";
    EXPECT_FALSE(svc1.onAlterIsr(m)) << "a late re-send of the old request bumped again";
}

// A new controller decides nothing before its MSM reflects its WHOLE log as of
// the claim.  After a cold restart the HW is 0 at the claim, while the log
// holds the previous incarnation's decisions (here: the static assignment AND a
// later committed-ISR change).  Deciding on the empty / partial map re-emitted
// the static epoch-0 assignment after them — the committed map then regressed
// (ISR, partitionEpoch; with a failover in between, the LEADER EPOCH and the
// leader) and partitions re-led at epoch 0 (the DR-12 ground truth).
TEST(EpochRestartControllerRegression, NewControllerNeverReemitsOverItsUncommittedLogRegression) {
    SaeTempDir dir1("pc1"), dir2("pc2"), dir3("pc3");
    const std::vector<NodeId> allNodes = {1, 2, 3};
    int32_t le = -1, pe = -1;
    std::size_t isrSize = 0;
    Offset leo = -1;
    {   // Incarnation 1: static assignment, then an ISR change, all committed.
        rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
        log1.recover(); log2.recover(); log3.recover();
        InMemoryTransport transport;
        auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, allNodes, allNodes, &log1, &transport);
        auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, allNodes, allNodes, &log2, &transport);
        auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, allNodes, allNodes, &log3, &transport);
        transport.setLeader(r1.get());
        transport.addFollower(2, r2.get());
        transport.addFollower(3, r3.get());
        MetadataService svc1(r1.get(), 1);
        svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
        int64_t clock = 0;
        auto drive = [&](int n) {
            for (int i = 0; i < n; ++i) {
                clock += 1'000'000LL;
                r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
                transport.pump(clock);
                (void)svc1.doWork(clock);
            }
        };
        drive(50);
        const auto* pa = svc1.assignment(0, 0);
        ASSERT_NE(pa, nullptr);
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = pa->leaderNode; m.leaderEpoch = pa->leaderEpoch;
        m.isrCount = 2; m.isr[0] = pa->leaderNode; m.isr[1] = pa->leaderNode == 1 ? 2 : 1;
        m.partitionEpoch = pa->partitionEpoch;
        ASSERT_TRUE(svc1.onAlterIsr(m));
        drive(50);
        pa = svc1.assignment(0, 0);
        ASSERT_NE(pa, nullptr);
        le = pa->leaderEpoch; pe = pa->partitionEpoch; isrSize = pa->isr.size();
        ASSERT_EQ(isrSize, 2u);
        leo = r1->logEndOffset();
        ASSERT_EQ(r1->highWatermark(), leo);
    }
    // Incarnation 2 (cold restart): node 1 is elected again (consensus term 2).
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Follower, 0, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 0, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 0, allNodes, allNodes, &log3, &transport);
    r1->becomeLeader(2, allNodes);
    r2->becomeFollower(2, 1);
    r3->becomeFollower(2, 1);
    transport.setLeader(r1.get());
    transport.addFollower(2, r2.get());
    transport.addFollower(3, r3.get());
    ASSERT_LT(r1->highWatermark(), leo) << "precondition: the restarted controller's HW is behind its log";
    MetadataService svc1(r1.get(), 1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    for (int i = 0; i < 100; ++i) {
        clock += 1'000'000LL;
        (void)svc1.doWork(clock);      // the claim edge runs before any fetch
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(r1->logEndOffset(), leo) << "the new controller re-emitted assignments over its uncommitted log";
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->leaderEpoch, le);
    EXPECT_EQ(pa->partitionEpoch, pe) << "the committed assignment regressed";
    EXPECT_EQ(pa->isr.size(), isrSize);
}
