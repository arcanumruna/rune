// ---------------------------------------------------------------------------
// CatalogAssignmentEmitTest.cpp — MD1 Task 6 (renamed by catalog-history Task 18): on becoming the metadata-consensus
// leader, run StaticBalancer::assign() over the boot topics — the catalog-recovered topics
// BrokerNode feeds MetadataService::configure from bootTopics_, which come from __catalog — and append the
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

#include <rune/node/ControlledShutdown.h>
#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <algorithm>
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
TEST(CatalogAssignmentEmitTest, SingleLeader_EmitsBalancedAssignmentOnLeaderClaim) {
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
        TopicSpec{/*topicId=*/0, /*partitionCount=*/3, /*rf*/1, /*coPartitionGroup=*/"grp", "orders"},
        TopicSpec{/*topicId=*/1, /*partitionCount=*/3, /*rf*/1, /*coPartitionGroup=*/"grp", "orders"},
        TopicSpec{/*topicId=*/2, /*partitionCount=*/2, /*rf*/1, /*coPartitionGroup=*/"",    "chat"},
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

    // Now drive replication until followers catch up (majority HW).  The leader service runs in the loop: since the
    // controller-term record (Raft section 5.4.2) the claim emits its assignments only once that record has COMMITTED.
    bool replicated = false;
    for (int i = 0; i < kDriveCap && !replicated; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        svc1.doWork(clock);
        // Majority = leader + at least one follower with HW >= LEO.
        // Kafka model: committed once on every ISR member; both followers learn it.
        if (r2->highWatermark() >= r1->logEndOffset() &&
            r3->highWatermark() >= r1->logEndOffset() && svc1.assignment(2, 1) != nullptr) {
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
TEST(CatalogAssignmentEmitTest, Idempotent_NoDoubleEmitOnRePromotion) {
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
    EXPECT_EQ(leoAfterFirst + 1, leoAfterRepromotion)   // +1: the controller-term record (Raft §5.4.2)
        << "re-promoted service must NOT append any new records but its controller-term record "
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
TEST(CatalogAssignmentEmitTest, OneRecordPerAppend_LargeTopicSetNeverMegaAppend) {
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
        topics.push_back(TopicSpec{i, /*partitionCount=*/1, /*rf*/1, /*coPartitionGroup=*/"", "key"});
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
    // D15: + the fresh log's bootstrap FeatureLevelRecord (its own entry).
    EXPECT_EQ(leoAfterEmit, static_cast<Offset>(kTopicCount) + 2)   // +1: the controller-term record (Raft §5.4.2)
        << "logEndOffset must advance by exactly " << kTopicCount << " + 2"
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
TEST(CatalogAssignmentEmitTest, FollowerDoesNotEmit) {
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
TEST(CatalogAssignmentEmitTest, BalancerError_IncrementCounterEmitsNothing) {
    SaeTempDir dir("t5");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};  // only 1 node
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    // rf 3 but only 1 live node → StaticBalancer must throw BalancerError.
    std::vector<TopicSpec> topics = {
        TopicSpec{0, 2, /*rf*/3, "", "key"},  // RF 3 > 1 node: invalid
    };

    MetadataService svc(replica.get(), selfId);
    svc.configure(topics, nodes);

    int64_t clock = 0;
    clock += 2'000'000LL;
    replica->doWork(clock);
    transport.pump(clock);
    svc.doWork(clock);  // triggers emitStaticAssignmentIfNeeded(); balancer must throw

    // No assignment records must have been appended (D15: only the fresh
    // log's bootstrap FeatureLevelRecord).
    EXPECT_EQ(replica->logEndOffset(), 2)   // +1: the controller-term record (Raft §5.4.2)
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

// ISR protocol (answer correlation): EVERY content answer echoes the request's proposalId and sendSeq and
// carries the answering controller's term (setControllerEpoch).
TEST(AlterIsrControllerRegression, EveryAnswerEchoesIdSeqAndControllerEpochRegression) {
    SaeTempDir dir1("ec1"), dir2("ec2"), dir3("ec3");
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
    svc1.setControllerEpoch(9);
    int64_t clock = 0;
    for (int i = 0; i < 50; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        (void)svc1.doWork(clock);
    }
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    ASSERT_EQ(pa->isr.size(), 3u);
    rune::replication::AlterIsrMsg base{};
    base.topicId = 0; base.partition = 0; base.leaderId = pa->leaderNode; base.leaderEpoch = pa->leaderEpoch;
    base.partitionEpoch = pa->partitionEpoch;
    base.isrCount = 3;
    for (int i = 0; i < 3; ++i) base.isr[static_cast<std::size_t>(i)] = pa->isr[static_cast<std::size_t>(i)];
    auto ask = [&](rune::replication::AlterIsrMsg m, int64_t id, int32_t seq, int16_t want) {
        m.proposalId = id; m.sendSeq = seq;
        rune::replication::AlterIsrResponseMsg resp{};
        bool answered = false;
        (void)svc1.onAlterIsr(m, &resp, &answered);
        ASSERT_TRUE(answered) << "request " << id << " was not answered";
        EXPECT_EQ(resp.errorCode, want) << "request " << id;
        EXPECT_EQ(resp.proposalId, id) << "proposalId not echoed";
        EXPECT_EQ(resp.sendSeq, seq) << "sendSeq not echoed";
        EXPECT_EQ(resp.controllerEpoch, 9) << "controllerEpoch not stamped";
    };
    auto staleLeader = base; staleLeader.leaderEpoch = base.leaderEpoch - 1;
    ask(staleLeader, 101, 1, rune::replication::kAlterIsrFencedLeaderEpoch);
    auto stalePe = base; stalePe.partitionEpoch = base.partitionEpoch - 1;
    ask(stalePe, 102, 2, rune::replication::kAlterIsrInvalidUpdateVersion);
    auto alien = base; alien.isr[2] = 42;
    ask(alien, 103, 3, rune::replication::kAlterIsrInvalidRequest);
    ask(base, 104, 4, rune::replication::kAlterIsrOk);   // exactly the committed set
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


// DR-bugs fix round 1, review A-I1: while a record for the partition is in flight, the controller answers NO
// content ("nothing to commit" / Ineligible / InvalidRequest) — such an answer describes a state the leader's
// proposal is about to leave, and the leader would clear a proposal that then commits.  RED before the fix: the
// re-send of an in-flight re-admission was answered Ineligible (the member had been fenced meanwhile) while the
// first send went on to commit it.
TEST(AlterIsrControllerRegression, IneligibleIsNeverAnsweredWhileTheProposalIsInFlightRegression) {
    SaeTempDir dir1("inf1"), dir2("inf2"), dir3("inf3");
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
    std::vector<rune::metadata::NodeId> live, all(allNodes.begin(), allNodes.end());
    for (int32_t n : allNodes) if (n != dead) live.push_back(n);
    ASSERT_GE(svc1.runDynamicRebalanceOnTick(live, nullptr), 1u);   // fence `dead`
    drive(50);
    pa = svc1.assignment(0, 0);
    ASSERT_EQ(std::find(pa->isr.begin(), pa->isr.end(), dead), pa->isr.end());
    (void)svc1.runDynamicRebalanceOnTick(all, nullptr);   // `dead` is eligible again
    const int32_t pe0 = pa->partitionEpoch;
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = le;
    m.isrCount = static_cast<int32_t>(pa->isr.size()) + 1;
    for (std::size_t i = 0; i < pa->isr.size(); ++i) m.isr[i] = pa->isr[i];
    m.isr[pa->isr.size()] = dead;
    m.partitionEpoch = pe0;
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    ASSERT_TRUE(svc1.onAlterIsr(m, &resp, &answered)) << "the re-admission must be enqueued";
    // Not driven: the record is in flight.  Case 2 first (guards the move): an identical re-send, `dead` still
    // eligible — never answered.
    rune::replication::AlterIsrResponseMsg resp2{};
    bool answered2 = false;
    EXPECT_FALSE(svc1.onAlterIsr(m, &resp2, &answered2));
    EXPECT_FALSE(answered2) << "answered (code " << resp2.errorCode << ") while the proposal was in flight";
    // Case 1: `dead` turns ineligible while the record is in flight; the re-send must NOT be answered Ineligible.
    (void)svc1.runDynamicRebalanceOnTick(live, nullptr);
    rune::replication::AlterIsrResponseMsg resp3{};
    bool answered3 = false;
    EXPECT_FALSE(svc1.onAlterIsr(m, &resp3, &answered3));
    EXPECT_FALSE(answered3) << "answered (code " << resp3.errorCode << ") while the proposal was in flight — the "
                            "leader would clear a proposal that commits";
    // The first send commits at pe0 + 1 with `dead` in the ISR.
    bool sawCommit = false;
    for (int i = 0; i < 200 && !sawCommit; ++i) {
        drive(1);
        pa = svc1.assignment(0, 0);
        if (pa != nullptr && pa->partitionEpoch == pe0 + 1) {
            sawCommit = true;
            EXPECT_NE(std::find(pa->isr.begin(), pa->isr.end(), dead), pa->isr.end())
                << "the in-flight re-admission did not commit";
        }
    }
    EXPECT_TRUE(sawCommit) << "no commit at pe0+1";
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

namespace {
/// A 3-node metadata rig (InMemoryTransport) with svc on node 1, one topic
/// t(0) x1 partition RF 3, driven until the static assignment committed.
struct CtlRig {
    SaeTempDir d1{"cr1"}, d2{"cr2"}, d3{"cr3"};
    rune::storage::Log log1{d1.path}, log2{d2.path}, log3{d3.path};
    std::vector<NodeId> all{1, 2, 3};
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> r1, r2, r3;
    std::unique_ptr<MetadataService> svc;
    int64_t clock = 0;
    /// `userTopics` false: no user topic, the coordinator partition (-1, 0)
    /// on the controller path only (a cluster with no user topics).
    std::vector<TopicSpec> topics{TopicSpec{0, 1, 3, "", "t"}};
    explicit CtlRig(bool userTopics = true, std::vector<TopicSpec> ts = {}, int16_t supportedLevel = -1) {
        if (!ts.empty()) topics = std::move(ts);
        log1.recover(); log2.recover(); log3.recover();
        r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, all, all, &log1, &transport);
        r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, all, all, &log2, &transport);
        r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, all, all, &log3, &transport);
        transport.setLeader(r1.get());
        transport.addFollower(2, r2.get());
        transport.addFollower(3, r3.get());
        svc = std::make_unique<MetadataService>(r1.get(), 1);
        if (supportedLevel >= 0) svc->setSupportedFeatureLevel(supportedLevel);   // ISR v3: a level-1 rig
        if (userTopics) {
            svc->configure(topics, all);
        } else {
            svc->configure({}, all);
            svc->setCoordinatorPartitions({rune::metadata::PartitionKey{-1, 0}}, all);
        }
        drive(50);
    }
    void drive(int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc->doWork(clock);
        }
    }
    /// One controller tick over `eligible` + drive until committed.
    void tick(std::vector<NodeId> eligible = {1, 2, 3}) {
        (void)svc->reassignDeadPartitionLeaders(eligible, 0);
        drive(50);
    }
    const PartitionAssignment& pa() { return *svc->assignment(0, 0); }
    const PartitionAssignment& paOf(int32_t topic, int32_t part = 0) { return *svc->assignment(topic, part); }
    /// ISR v3 Step 7: tick with only `n` eligible until `topic` is led by `n` (the balancer fails over to n and
    /// fences the others out of the ISR: leader n, ISR {n}).
    void forceLeader(int32_t n, int32_t topic = 0) {
        // At least one tick: when n already leads, the fencing pass still shrinks the ISR to {n}.
        for (int i = 0; i < 10 && (i == 0 || paOf(topic).leaderNode != n); ++i) tick({n});
        ASSERT_EQ(paOf(topic).leaderNode, n);
        ASSERT_EQ(paOf(topic).isr, std::vector<int32_t>{n});
    }
    /// The leader of `topic` proposes pa().isr + n at pa().partitionEpoch (isrStorage all 0); n must end in the ISR.
    /// A full-set tick first: forceLeader left only the forced node eligible.
    void readmit(int32_t n, int32_t topic = 0) {
        tick();
        const auto& a = paOf(topic);
        rune::replication::AlterIsrMsg m{};
        m.topicId = topic; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
        std::vector<int32_t> isr = a.isr;
        isr.push_back(n);
        m.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) m.isr[i] = isr[i];
        m.partitionEpoch = a.partitionEpoch;
        (void)svc->onAlterIsr(m);
        drive(50);
        const auto& b = paOf(topic);
        ASSERT_TRUE(std::find(b.isr.begin(), b.isr.end(), n) != b.isr.end()) << "node " << n << " was not admitted";
    }
    /// Leader loss and regain on the SAME node (no new register).
    void bounceLeadership() {
        const int32_t e = r1->epoch();
        r1->becomeFollower(e + 1, NodeId{-1});
        drive(5);
        r1->becomeLeader(e + 2, all);
        r2->becomeFollower(e + 2, 1);
        r3->becomeFollower(e + 2, 1);
        drive(100);
        ASSERT_TRUE(svc->controllerCaughtUpForTest());
    }
    /// One tick with a record budget (reassignDeadPartitionLeaders cannot pass one).
    void tickMax(int maxRecords) {
        (void)svc->runDynamicRebalanceOnTick({1, 2, 3}, nullptr, 1000, maxRecords);
        drive(50);
    }
    /// Shrink the committed ISR to {leader}.
    void soleIsr() {
        const auto& a = pa();
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = 1; m.isr[0] = a.leaderNode; m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(svc->onAlterIsr(m));
        drive(50);
        ASSERT_EQ(pa().isr.size(), 1u);
    }
    rune::replication::AlterIsrMsg bump(int32_t fence = -1, bool memory = false) {
        const auto& a = pa();
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = rune::replication::AlterIsrMsg::kLeaderEpochBump;
        m.partitionEpoch = a.partitionEpoch;
        m.isr[0] = fence;
        m.isr[1] = (fence >= 0 ? rune::replication::AlterIsrMsg::kBumpFenceValid : 0) |
                   (memory ? rune::replication::AlterIsrMsg::kBumpMemoryBacked : 0);
        return m;
    }
};
} // namespace

// MAJOR-1: a restarted ex-leader asking for a new epoch may have lost acked
// records (power loss / memory log): another live ISR member leads (epoch + 1)
// and the requester leaves the ISR — it rejoins as a follower and truncates to
// the new leader.  A re-send / stale request decides nothing twice.
TEST(EpochRestartControllerRegression, BumpFailsOverToAnotherLiveIsrMemberRegression) {
    CtlRig c;
    const int32_t requester = c.pa().leaderNode, le = c.pa().leaderEpoch;
    const auto m = c.bump();
    auto notLeader = m; notLeader.leaderId = requester == 3 ? 2 : 3;
    EXPECT_FALSE(c.svc->onAlterIsr(notLeader));
    (void)c.svc->onAlterIsr(m);
    (void)c.svc->onAlterIsr(m);   // re-send
    c.tick();
    const auto& pa = c.pa();
    EXPECT_NE(pa.leaderNode, requester) << "the restarted ex-leader was re-elected over a live ISR member";
    EXPECT_EQ(pa.leaderEpoch, le + 1);
    EXPECT_EQ(std::count(pa.isr.begin(), pa.isr.end(), requester), 0) << "the requester stayed in the ISR";
    EXPECT_EQ(pa.isr.size(), 2u);
    (void)c.svc->onAlterIsr(m);   // late re-send of the old request
    c.tick();
    EXPECT_EQ(c.pa().leaderEpoch, le + 1) << "a stale bump request decided again";
}

// MAJOR-1: the requester is the SOLE in-sync replica: no clean alternative —
// it is re-elected, at an epoch past its fence (L2: one round trip).
TEST(EpochRestartControllerRegression, SoleIsrBumpReelectsSameLeaderPastFenceRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t requester = c.pa().leaderNode, le = c.pa().leaderEpoch;
    (void)c.svc->onAlterIsr(c.bump(le + 5));
    c.tick();
    EXPECT_EQ(c.pa().leaderNode, requester);
    EXPECT_EQ(c.pa().leaderEpoch, le + 6) << "the bump did not jump past the requester's fence";
    EXPECT_EQ(c.pa().isr.size(), 1u);
}

namespace {
rune::replication::AlterIsrMsg leaderlessReport(int32_t node, int32_t epoch, int32_t leo, int64_t inc,
                                                bool logDirFailed = false) {
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = node; m.leaderEpoch = epoch;
    m.isrCount = rune::replication::AlterIsrMsg::kLeaderEpochBump;
    m.partitionEpoch = -1;
    m.isr[1] = rune::replication::AlterIsrMsg::kBumpFollowerReport |
               (logDirFailed ? rune::replication::AlterIsrMsg::kBumpLogDirFailed : 0);
    m.isr[2] = leo;
    m.isr[3] = static_cast<int32_t>(inc);
    return m;
}
} // namespace

// MAJOR-1 + fix round 2 N2: a sole-ISR MEMORY leader restarts EMPTY.  It is
// not re-elected blind (it would wipe followers that hold records): the
// partition goes leaderless and the next leader is decided on FRESH reports of
// every live replica.  A restarted (empty) replica with a STALE high position
// report is never chosen over one that holds data; electing a non-ISR replica
// is an unclean election — only with survivor election allowed, else the
// partition is held leaderless with no record churn.
TEST(EpochRestartControllerRegression, MemoryEmptyLeaderNeverElectsAStaleReportOverRealDataRegression) {
    for (const bool survivor : {false, true}) {
        SCOPED_TRACE(survivor ? "survivor election ON" : "survivor election OFF");
        CtlRig c;
        c.svc->setAllowSurvivorElection(survivor);
        c.soleIsr();
        const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
        const int32_t F = L == 1 ? 2 : 1;            // restarts empty, stale HIGH report
        const int32_t G = 6 - L - F;                 // holds data, lower stale report
        c.svc->reportReplicaPosition(0, 0, F, le, 100);
        c.svc->reportReplicaPosition(0, 0, G, le, 10);
        auto bump = c.bump(le, /*memory=*/true);
        bump.isr[3] = 2;                             // L's new incarnation
        (void)c.svc->onAlterIsr(bump);
        c.tick();
        ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER) << "the empty memory leader was re-elected blind";
        const int32_t lle = c.pa().leaderEpoch;
        (void)c.svc->onAlterIsr(leaderlessReport(L, lle, 0, 2));
        (void)c.svc->onAlterIsr(leaderlessReport(F, lle, 0, 7));    // restarted: empty
        c.tick();
        EXPECT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER) << "decided before every live replica reported";
        (void)c.svc->onAlterIsr(leaderlessReport(G, lle, 10, 1));
        c.tick();
        EXPECT_NE(c.pa().leaderNode, F) << "the empty replica with the stale high report was elected";
        if (survivor) {
            EXPECT_EQ(c.pa().leaderNode, G);
            EXPECT_EQ(c.pa().isr, std::vector<int32_t>{G});
        } else {
            EXPECT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
            const auto leo = c.r1->logEndOffset();
            for (int i = 0; i < 5; ++i) c.tick();
            EXPECT_EQ(c.r1->logEndOffset(), leo) << "a held leaderless partition churned metadata records";
            EXPECT_EQ(c.svc->leaderlessHeldForTest(), 2u);
        }
    }
}

// Fix round 2 N1 (KIP-112): the sole-ISR leader's log dir went offline (its
// fence cannot be written).  The controller never re-grants it: one record
// makes the partition leaderless (ISR kept), and the replica's re-sent bumps
// and reports append NOTHING more (no livelock, no epoch inflation).  A report
// from a NEW incarnation with a healthy dir re-registers it and it is elected.
TEST(EpochRestartControllerRegression, LogDirOfflineSoleIsrHeldLeaderlessNoChurnRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    auto bump = c.bump();
    bump.isr[1] |= rune::replication::AlterIsrMsg::kBumpLogDirFailed;
    bump.isr[3] = 5;
    (void)c.svc->onAlterIsr(bump);
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER) << "a replica with an offline log dir was re-granted";
    EXPECT_EQ(c.pa().leaderEpoch, le + 1);
    const auto leo = c.r1->logEndOffset();
    for (int i = 0; i < 10; ++i) {
        (void)c.svc->onAlterIsr(bump);                                          // stale re-send
        (void)c.svc->onAlterIsr(leaderlessReport(L, le + 1, 3, 5, /*failed=*/true));
        c.tick();
    }
    EXPECT_EQ(c.r1->logEndOffset(), leo) << "metadata records churned for an offline replica";
    EXPECT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    (void)c.svc->onAlterIsr(leaderlessReport(L, le + 1, 3, 6));               // restarted, healthy
    c.tick();
    EXPECT_EQ(c.pa().leaderNode, L) << "the re-registered replica was never elected";
    EXPECT_EQ(c.pa().leaderEpoch, le + 2);
}

// Fix round 2 N3: after a restart failover, the returned preferred replica
// gets its leadership back exactly ONCE when it re-enters the ISR (caught up:
// the leader's AlterIsr admits it at >= HW); further committed changes and
// passes never move it again (no ping-pong).
TEST(EpochRestartControllerRegression, PreferredMoveBackOnceNoPingPongRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    (void)c.svc->onAlterIsr(c.bump());
    c.tick();
    ASSERT_NE(c.pa().leaderNode, L);
    ASSERT_EQ(c.pa().leaderEpoch, le + 1);
    auto alter = [&](std::vector<int32_t> isr) {
        const auto& a = c.pa();
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) m.isr[i] = isr[i];
        m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(c.svc->onAlterIsr(m));
        c.drive(50);
    };
    std::vector<int32_t> isr = c.pa().isr;
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 1024);   // L not in the ISR yet: no move
    c.drive(50);
    EXPECT_NE(c.pa().leaderNode, L) << "moved back before L re-entered the ISR";
    isr.push_back(L);
    alter(isr);                                                    // L caught up: re-admitted
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 1024);
    c.drive(50);
    EXPECT_EQ(c.pa().leaderNode, L) << "leadership never returned to the caught-up preferred replica";
    const int32_t settledEpoch = c.pa().leaderEpoch;
    EXPECT_EQ(settledEpoch, le + 2);
    for (int i = 0; i < 5; ++i) {
        (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 1024);
        c.drive(20);
    }
    EXPECT_EQ(c.pa().leaderNode, L);
    EXPECT_EQ(c.pa().leaderEpoch, settledEpoch) << "leadership ping-ponged";
}

// M4.1: the controller fails over between the bump decision and its commit —
// the record is (a) lost with the old controller's uncommitted tail or (b) on
// the new controller's uncommitted tail.  Either way the partition ends at
// exactly ONE new epoch (the requester re-sends; the CAS on the committed
// (leaderEpoch, partitionEpoch) never lets it apply twice).
TEST(EpochRestartControllerRegression, ControllerFailoverMidBumpBumpsExactlyOnceRegression) {
    for (const bool replicated : {false, true}) {
        SCOPED_TRACE(replicated ? "bump record on the new controller's tail" : "bump record lost");
        CtlRig c;
        c.soleIsr();
        const int32_t le = c.pa().leaderEpoch;
        const auto m = c.bump();
        (void)c.svc->onAlterIsr(m);
        (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 0);   // decided + appended on node 1
        if (replicated) c.transport.pump(c.clock);                 // reaches the followers only
        // Controller moves to node 2 (consensus term 2).
        c.r1->becomeFollower(2, 2);
        c.r3->becomeFollower(2, 2);
        c.r2->becomeLeader(2, c.all);
        auto& t2 = c.transport;
        t2.removeFollower(2);
        t2.setLeader(c.r2.get());
        t2.addFollower(1, c.r1.get());
        MetadataService svc2(c.r2.get(), 2);
        svc2.configure({TopicSpec{0, 1, 3, "", "t"}}, c.all);
        auto drive2 = [&](int n) {
            for (int i = 0; i < n; ++i) {
                c.clock += 1'000'000LL;
                (void)svc2.doWork(c.clock);
                c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
                t2.pump(c.clock);
            }
        };
        drive2(100);
        ASSERT_TRUE(svc2.controllerCaughtUpForTest());
        (void)svc2.onAlterIsr(m);   // the requester re-sends its (now maybe stale) request
        (void)svc2.reassignDeadPartitionLeaders({1, 2, 3}, 0);
        drive2(100);
        const auto* pa = svc2.assignment(0, 0);
        ASSERT_NE(pa, nullptr);
        EXPECT_EQ(pa->leaderEpoch, le + 1) << "bumped " << (pa->leaderEpoch - le) << " times";
    }
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
    EXPECT_EQ(r1->logEndOffset(), leo + 1)   // +1: the controller-term record (Raft §5.4.2)
        << "the new controller re-emitted assignments over its uncommitted log";
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->leaderEpoch, le);
    EXPECT_EQ(pa->partitionEpoch, pe) << "the committed assignment regressed";
    EXPECT_EQ(pa->isr.size(), isrSize);
}

// M1: a runtime topic created while a new controller is still catching up on
// its log must not make the catch-up static emit re-assign the EXISTING
// partitions at epoch 0 — only the new topic's partitions get a static record
// (and the MSM never installs an assignment that moves a partition back).
TEST(EpochRestartControllerRegression, RuntimeTopicDuringCatchUpNeverReassignsExistingRegression) {
    SaeTempDir dir1("rt1"), dir2("rt2"), dir3("rt3");
    const std::vector<NodeId> allNodes = {1, 2, 3};
    int32_t le = -1, pe = -1;
    std::size_t isrSize = 0;
    {
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
        le = pa->leaderEpoch; pe = pa->partitionEpoch; isrSize = pa->isr.size();
    }
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
    MetadataService svc1(r1.get(), 1);
    svc1.configure({TopicSpec{0, 1, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    clock += 1'000'000LL;
    (void)svc1.doWork(clock);   // the claim: HW behind the log
    ASSERT_FALSE(svc1.controllerCaughtUpForTest()) << "precondition: claimed before catching up";
    svc1.addRuntimeTopic(TopicSpec{1, 1, 3, "", "t2"});   // during the catch-up window
    for (int i = 0; i < 100; ++i) {
        clock += 1'000'000LL;
        (void)svc1.doWork(clock);
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
    }
    const auto* pa = svc1.assignment(0, 0);
    ASSERT_NE(pa, nullptr);
    EXPECT_EQ(pa->leaderEpoch, le);
    EXPECT_EQ(pa->partitionEpoch, pe) << "an existing partition was re-assigned by the static path";
    EXPECT_EQ(pa->isr.size(), isrSize);
    EXPECT_NE(svc1.assignment(1, 0), nullptr) << "the runtime topic was never assigned";
}

// ===========================================================================
// Controlled shutdown (Kafka parity) — controller side.  A node stopping
// gracefully sends a NODE-level request (AlterIsr channel, topicId =
// kNodeControlTopicId, kNodeShutdown); the controller then never elects it,
// moves every partition it leads to another live in-sync replica (new epoch,
// the stopping node leaves the ISR), drops it from the ISRs it follows in, and
// refuses to re-admit it — until a NEW incarnation registers (kNodeRegister).
// A partition whose only in-sync replica is the stopping leader stays (no
// clean alternative; never an unclean election, no record churn).
// ===========================================================================
namespace {
rune::replication::AlterIsrMsg nodeControl(int32_t node, int64_t inc, bool shutdown) {
    using rune::replication::AlterIsrMsg;
    AlterIsrMsg m{};
    m.topicId = AlterIsrMsg::kNodeControlTopicId; m.partition = 0;
    m.leaderId = node; m.leaderEpoch = 0; m.partitionEpoch = 0;
    m.isrCount = AlterIsrMsg::kLeaderEpochBump;
    m.isr[1] = shutdown ? AlterIsrMsg::kNodeShutdown : AlterIsrMsg::kNodeRegister;
    m.isr[3] = static_cast<int32_t>(static_cast<uint32_t>(inc));
    m.isr[4] = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(inc) >> 32));
    return m;
}
/// The leader proposes `isr` (an AlterIsr on the committed state); true = enqueued.
bool proposeIsr(CtlRig& c, std::vector<int32_t> isr) {
    const auto& a = c.pa();
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
    m.isrCount = static_cast<int32_t>(isr.size());
    for (std::size_t i = 0; i < isr.size(); ++i) m.isr[i] = isr[i];
    m.partitionEpoch = a.partitionEpoch;
    const bool ok = c.svc->onAlterIsr(m);
    c.drive(50);
    return ok;
}
} // namespace

TEST(ControlledShutdownControllerRegression, StoppingLeaderHandsOffToAnIsrMemberAtANewEpochRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    for (int32_t n : {1, 2, 3}) if (n != L) (void)c.svc->onAlterIsr(nodeControl(n, 20 + n, false));   // members register
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(L, 7, /*shutdown=*/true), &resp, &answer);
    EXPECT_TRUE(answer) << "the controller never acked the shutdown request";
    EXPECT_EQ(resp.topicId, rune::replication::AlterIsrMsg::kNodeControlTopicId);
    EXPECT_EQ(resp.leaderId, L);
    EXPECT_EQ(resp.leaderEpoch, 7) << "the ack must carry the requester's incarnation";
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE while the stopping node still leads";
    c.tick();
    // Moved in the controller's map, but not COMPLETE until every other live
    // member confirmed its own map routes around the stopping node.
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(L, 7, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE before the other members confirmed";
    EXPECT_EQ(resp.partition, 1) << "the answer names the responding controller";
    for (int32_t member : {1, 2, 3}) {
        if (member == L) continue;
        auto conf = nodeControl(L, 7, false);
        conf.isr[1] = rune::replication::AlterIsrMsg::kNodeShutdownConfirm;   // NOLINT
        conf.isr[2] = member;
        conf.isr[5] = rune::replication::AlterIsrMsg::kConfirmUserClear | rune::replication::AlterIsrMsg::kConfirmAllClear;
        bool a = false;
        (void)c.svc->onAlterIsr(conf, &resp, &a);
        EXPECT_FALSE(a) << "a confirmation is never answered";
    }
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(L, 7, true), &resp, &answer);
    EXPECT_TRUE(answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk) << "the hand-off was never confirmed COMPLETE";
    const auto& pa = c.pa();
    EXPECT_NE(pa.leaderNode, L) << "the stopping node still leads";
    EXPECT_EQ(pa.leaderEpoch, le + 1) << "the hand-off is a new leader epoch";
    EXPECT_EQ(std::count(pa.isr.begin(), pa.isr.end(), L), 0) << "the stopping node stayed in the ISR";
    EXPECT_EQ(pa.isr.size(), 2u);
    // Preferred-leader passes never move it back while it is stopping.
    for (int i = 0; i < 5; ++i) {
        (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 1024);
        c.drive(20);
    }
    EXPECT_NE(c.pa().leaderNode, L);
    EXPECT_EQ(c.pa().leaderEpoch, le + 1);
}

// Merge fix (fix/dr-bugs -> feat/catalog-history, 2026-10-16): a controller that has not yet learnt the live set
// (no rebalance tick since it became controller: a successor right after the old controller resigned) must not
// push COMPLETE.  With the eligible set unknown, shutdownComplete_ waited for NOBODY: COMPLETE went out before a
// single member confirmed that its own map routes around the stopping node, and the stopping node exited while a
// survivor still routed the coordinator partitions to it (ControlledShutdownRegression.ControllerGraceful...
// failed about 1 run in 10).  RED: the gate removed (COMPLETE at once, no confirmation).
TEST(ControlledShutdownControllerRegression, ControllerWithAnUnknownLiveSetNeverConfirmsAShutdownRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t F = L == 1 ? 2 : 1;
    const int32_t O = 6 - L - F;
    for (int32_t n : {1, 2, 3}) (void)c.svc->onAlterIsr(nodeControl(n, 20 + n, false));   // every member speaks the protocol
    ASSERT_TRUE(proposeIsr(c, {L, O}));   // F already left every ISR: its map is clear with no controller tick
    ASSERT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 0);
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, /*shutdown=*/true), &resp, &answer);
    ASSERT_TRUE(answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE from a controller that does not know the live set yet (no member confirmed)";
    c.tick();   // the live set is known now: still the members' confirmations to wait for
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending) << "COMPLETE before the members confirmed";
    for (int32_t member : {L, O}) {
        if (member == 1) continue;   // the controller itself
        auto conf = nodeControl(F, 7, false);
        conf.isr[1] = rune::replication::AlterIsrMsg::kNodeShutdownConfirm;
        conf.isr[2] = member;
        conf.isr[5] = rune::replication::AlterIsrMsg::kConfirmUserClear | rune::replication::AlterIsrMsg::kConfirmAllClear;
        (void)c.svc->onAlterIsr(conf);
    }
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk) << "never COMPLETE once the live set is known and confirmed";
}

// CP1 review I2 (sibling of the merge fix above): the live set is learnt per CLAIM.  A controller that re-claims
// leadership kept the previous leadership's `eligible_` ({1,2} while node 3 was down) and pushed COMPLETE against
// it: the loop skipped self and the stopping node and waited for nobody.  stepDownController_() now forgets the set.
// RED: delete `eligible_.clear(); eligibleKnown_ = false;` in stepDownController_ (the first answer is COMPLETE).
TEST(ControlledShutdownControllerRegression, ReclaimedControllerRelearnsTheLiveSetBeforeConfirmingRegression) {
    CtlRig c;
    ASSERT_EQ(c.pa().leaderNode, 1) << "the rig's controller must lead the partition for this scenario";
    const int32_t F = 2, O = 3;
    for (int32_t n : {1, 2, 3}) (void)c.svc->onAlterIsr(nodeControl(n, 20 + n, false));   // every member speaks the protocol
    ASSERT_TRUE(proposeIsr(c, {1, O}));   // F already left every ISR: its map is clear with no further tick
    ASSERT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 0);
    c.tick({1, F});   // node 3 is down at THIS leadership: eligible_ = {1, 2}
    // The controller loses and regains the claim.
    c.svc->setConsensusLeads(false);
    c.drive(5);
    ASSERT_FALSE(c.svc->controllerReady());
    c.svc->setConsensusLeads(true);
    for (int i = 0; i < 500 && !c.svc->controllerReady(); ++i) c.drive(1);
    ASSERT_TRUE(c.svc->controllerReady());
    // Node 2 stops.  No tick yet: this claim has not learnt the live set.
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, /*shutdown=*/true), &resp, &answer);
    ASSERT_TRUE(answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE on the previous leadership's live set";
    c.tick({1, 2, 3});   // learnt now: node 3 is live and has not confirmed
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending) << "COMPLETE before node 3 confirmed";
    auto conf = nodeControl(F, 7, false);
    conf.isr[1] = rune::replication::AlterIsrMsg::kNodeShutdownConfirm;
    conf.isr[2] = O;
    conf.isr[5] = rune::replication::AlterIsrMsg::kConfirmUserClear | rune::replication::AlterIsrMsg::kConfirmAllClear;
    (void)c.svc->onAlterIsr(conf);
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk) << "never COMPLETE once the live set is known and confirmed";
}

// Merge fix (2): the user-only scope belongs to a stopping node that IS the controller (its pre-resign view).  Its
// successor still held the mark the old controller's Draining broadcasts left (userOnly), and pushed COMPLETE on
// the members' USER-clear confirmations alone — the stopping node exited while a survivor still routed the
// coordinator partitions to it.  A controller judging ANOTHER node's shutdown always uses the full scope.
// RED: the scope taken from the mark (COMPLETE on user-clear confirmations).
TEST(ControlledShutdownControllerRegression, SuccessorNeverConfirmsAFormerControllersShutdownOnUserPartitionsAloneRegression) {
    CtlRig c(/*userTopics=*/false);   // the coordinator partition (-1, 0) only
    for (int32_t n : {1, 2, 3}) (void)c.svc->onAlterIsr(nodeControl(n, 20 + n, false));
    c.tick();
    const int32_t F = 2, O = 3;   // this controller is node 1
    auto req = nodeControl(F, 7, /*shutdown=*/true);
    req.isr[1] |= rune::replication::AlterIsrMsg::kNodeShutdownUserOnly;   // the mark of F's pre-resign Draining
    (void)c.svc->onAlterIsr(req);
    for (int i = 0; i < 3; ++i) c.tick();
    auto conf = nodeControl(F, 7, false);
    conf.isr[1] = rune::replication::AlterIsrMsg::kNodeShutdownConfirm;
    conf.isr[2] = O;
    conf.isr[5] = rune::replication::AlterIsrMsg::kConfirmUserClear;   // O still routes a coordinator partition to F
    (void)c.svc->onAlterIsr(conf);
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(req, &resp, &answer);
    ASSERT_TRUE(answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE for another node's shutdown on user-partition confirmations alone";
    conf.isr[5] = rune::replication::AlterIsrMsg::kConfirmUserClear | rune::replication::AlterIsrMsg::kConfirmAllClear;
    (void)c.svc->onAlterIsr(conf);
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(req, &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk) << "never COMPLETE once every member routes around F";
}

TEST(ControlledShutdownControllerRegression, SoleIsrPartitionStaysWithTheStoppingLeaderRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch, pe = c.pa().partitionEpoch;
    (void)c.svc->onAlterIsr(nodeControl(L, 7, true));
    const auto leo = c.r1->logEndOffset();
    for (int i = 0; i < 5; ++i) c.tick();
    EXPECT_EQ(c.pa().leaderNode, L) << "a sole-ISR partition was moved (unclean) or taken offline";
    EXPECT_EQ(c.pa().leaderEpoch, le);
    EXPECT_EQ(c.pa().partitionEpoch, pe);
    EXPECT_EQ(c.pa().isr, std::vector<int32_t>{L});
    EXPECT_EQ(c.r1->logEndOffset(), leo) << "record churn for a partition that cannot move";
    // A follower that catches up (the leader re-admits it) makes it movable.
    const int32_t F = L == 1 ? 2 : 1;
    ASSERT_TRUE(proposeIsr(c, {L, F}));
    c.tick();
    EXPECT_EQ(c.pa().leaderNode, F) << "the partition became movable but stayed with the stopping node";
}

TEST(ControlledShutdownControllerRegression, StoppingFollowerLeavesTheIsrAndIsNotReadmittedRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    const int32_t F = L == 1 ? 2 : 1;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true));
    c.tick();
    EXPECT_EQ(c.pa().leaderNode, L) << "a stopping FOLLOWER moved the leadership";
    EXPECT_EQ(c.pa().leaderEpoch, le);
    EXPECT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 0) << "the stopping follower stayed in the ISR";
    std::vector<int32_t> isr = c.pa().isr;
    isr.push_back(F);
    EXPECT_FALSE(proposeIsr(c, isr)) << "the stopping follower was re-admitted";
    EXPECT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 0);
}

TEST(ControlledShutdownControllerRegression, NewIncarnationRegisterClearsTheShutdownMarkRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t F = L == 1 ? 2 : 1;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true));
    c.tick();
    ASSERT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 0);
    // A stale register of the stopping incarnation clears nothing.
    (void)c.svc->onAlterIsr(nodeControl(F, 7, false));
    std::vector<int32_t> isr = c.pa().isr;
    isr.push_back(F);
    EXPECT_FALSE(proposeIsr(c, isr)) << "the stopping incarnation's own register cleared its mark";
    // The restarted node (incarnation 8) registers: it is a normal member again.
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 8, false), &resp, &answer);
    EXPECT_TRUE(answer);
    EXPECT_EQ(resp.leaderEpoch, 8);
    EXPECT_TRUE(proposeIsr(c, isr)) << "the restarted node was never re-admitted";
    EXPECT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 1);
    // An older incarnation's late shutdown request is ignored.
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true));
    c.tick();
    EXPECT_EQ(std::count(c.pa().isr.begin(), c.pa().isr.end(), F), 1) << "a dead incarnation's request applied";
}

// Node side: what keeps a stopping node from exiting — every ISR it is in,
// EXCEPT a partition it leads as the SOLE in-sync replica (cannot move without
// an unclean election: it stays and goes offline with the node, no data loss).
// The controller's own pre-resign view counts user partitions only.
TEST(ControlledShutdownControllerRegression, NodeSideDoneConditionLetsSoleIsrPartitionsStayRegression) {
    rune::metadata::AssignmentMap m;
    m.put(0, 0, PartitionAssignment{1, {1}, {1, 2, 3}, 3, 5});        // sole ISR: stays
    m.put(0, 1, PartitionAssignment{1, {1, 2}, {1, 2, 3}, 3, 5});     // movable: blocks
    m.put(0, 2, PartitionAssignment{2, {2, 1}, {1, 2, 3}, 3, 5});     // follower in ISR: blocks
    m.put(0, 3, PartitionAssignment{2, {2, 3}, {1, 2, 3}, 3, 5});     // not in ISR: free
    m.put(-1, 0, PartitionAssignment{1, {1, 2}, {1, 2, 3}, 3, 5});    // coordinator
    EXPECT_EQ(rune::node::ControlledShutdown::blocking(m, 1, /*userOnly=*/false), 3);
    EXPECT_EQ(rune::node::ControlledShutdown::blocking(m, 1, /*userOnly=*/true), 2);
    EXPECT_EQ(rune::node::ControlledShutdown::blocking(m, 3, /*userOnly=*/false), 1);
}

// ===========================================================================
// Controlled shutdown — fix round 1 (review 2026-10-05).
// ===========================================================================
namespace {
rune::replication::AlterIsrMsg confirmation(int32_t stopping, int64_t nonce, int32_t member, bool clear = true) {
    auto c = nodeControl(stopping, nonce, false);
    c.isr[1] = rune::replication::AlterIsrMsg::kNodeShutdownConfirm;
    c.isr[2] = member;
    c.isr[5] = clear ? (rune::replication::AlterIsrMsg::kConfirmUserClear | rune::replication::AlterIsrMsg::kConfirmAllClear) : 0;
    return c;
}
bool isrHas(const PartitionAssignment& pa, int32_t n) {
    return std::count(pa.isr.begin(), pa.isr.end(), n) != 0;
}
} // namespace

// M1 (MAJOR): the process identity is compared by IDENTITY, never by order —
// a restart whose identity is LOWER than the marked one (a wiped data dir, a
// rescheduled pod, a wall-clock fallback) must clear the shutdown mark.
TEST(ControlledShutdownControllerRegression, RegisterFromALowerIdentityClearsTheMarkRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t F = L == 1 ? 2 : 1;
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true));
    c.tick();
    ASSERT_FALSE(isrHas(c.pa(), F));
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 1, false), &resp, &answer);   // the restarted process
    EXPECT_TRUE(answer) << "the restarted (lower-identity) process was dropped as a dead one";
    std::vector<int32_t> isr = c.pa().isr;
    isr.push_back(F);
    EXPECT_TRUE(proposeIsr(c, isr)) << "a restarted node with a lower identity stays fenced forever";
    // The stopped process's identity is superseded: its late requests change nothing.
    (void)c.svc->onAlterIsr(nodeControl(F, 7, true));
    c.tick();
    EXPECT_TRUE(isrHas(c.pa(), F)) << "a superseded process's late shutdown request applied";
}

// D1: COMPLETE is an EVENT — the last member confirmation alone pushes it to
// the stopping node (no wait for the stopping node's loss-recovery re-send).
TEST(ControlledShutdownControllerRegression, LastConfirmationPushesCompleteWithoutAResendRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    for (int32_t n : {1, 2, 3}) (void)c.svc->onAlterIsr(nodeControl(n, 10 + n, false));   // every member registers
    (void)c.svc->onAlterIsr(nodeControl(L, 10 + L, true));
    c.tick();
    ASSERT_NE(c.pa().leaderNode, L);
    auto& out = c.svc->nodeControlResponseOutbox();
    out.clear();
    for (int32_t m : {1, 2, 3}) {
        if (m == L || m == 1) continue;   // 1 is the controller itself
        (void)c.svc->onAlterIsr(confirmation(L, 10 + L, m));
    }
    bool pushed = false;
    for (const auto& [to, r] : out)
        pushed = pushed || (to == L && r.leaderId == L && r.errorCode == rune::replication::kAlterIsrOk);
    EXPECT_TRUE(pushed) << "the last confirmation did not push COMPLETE to the stopping node";
}

// D3: with NO user topics the stopping node must still leave the coordinator
// ISRs (else its stop always burns the whole budget).
TEST(ControlledShutdownControllerRegression, NoUserTopicsStoppingNodeLeavesCoordinatorIsrsRegression) {
    CtlRig c(/*userTopics=*/false);
    c.tick();
    const auto* pa = c.svc->assignment(-1, 0);
    ASSERT_NE(pa, nullptr);
    ASSERT_EQ(pa->isr.size(), 3u);
    const int32_t F = pa->leaderNode == 2 ? 3 : 2;
    (void)c.svc->onAlterIsr(nodeControl(F, 5, true));
    c.tick();
    pa = c.svc->assignment(-1, 0);
    EXPECT_FALSE(isrHas(*pa, F)) << "the stopping node stayed in the coordinator ISR";
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    for (int32_t m : {2, 3}) if (m != F) (void)c.svc->onAlterIsr(confirmation(F, 5, m));
    (void)c.svc->onAlterIsr(nodeControl(F, 5, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk);
}

// D4: a member that never spoke the protocol (an older binary mid-roll) is not
// waited for; one that registered is.
TEST(ControlledShutdownControllerRegression, SilentOldMemberDoesNotHoldCompleteRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;             // stopping
    const int32_t newer = L == 2 ? 3 : 2;            // registered + confirms
    // The third member never sends anything (an older binary).
    (void)c.svc->onAlterIsr(nodeControl(newer, 42, false));
    (void)c.svc->onAlterIsr(nodeControl(L, 77, true));
    c.tick();
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(L, 77, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeShutdownPending)
        << "COMPLETE before the registered member confirmed";
    (void)c.svc->onAlterIsr(confirmation(L, 77, newer));
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(L, 77, true), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk)
        << "a silent (pre-upgrade) member held the hand-off";
}

// D5: a DEAD leader's partition fails over to a live NON-stopping in-sync
// replica (never via the stopping node: no extra hop); with only the stopping
// node left in sync it is still elected (better than offline).
TEST(ControlledShutdownControllerRegression, DeadLeaderFailsOverToANonStoppingIsrMemberRegression) {
    {
        CtlRig c;
        const int32_t L = c.pa().leaderNode;
        std::vector<int32_t> others;
        for (int32_t n : {1, 2, 3}) if (n != L) others.push_back(n);
        const int32_t S = others[0], Y = others[1];   // S = lowest id: the default pick
        (void)c.svc->onAlterIsr(nodeControl(S, 9, true));
        c.tick({S, Y});                               // L is dead
        EXPECT_EQ(c.pa().leaderNode, Y) << "failed over to the stopping node (an extra hop)";
    }
    {
        CtlRig c;
        const int32_t L = c.pa().leaderNode;
        std::vector<int32_t> others;
        for (int32_t n : {1, 2, 3}) if (n != L) others.push_back(n);
        const int32_t S = others[0], Y = others[1];
        ASSERT_TRUE(proposeIsr(c, {L, S}));           // Y out of sync
        (void)c.svc->onAlterIsr(nodeControl(S, 9, true));
        c.tick({S, Y});
        EXPECT_EQ(c.pa().leaderNode, S) << "the only live in-sync survivor (stopping) was not elected";
    }
}

// Two nodes stopping at once: each hands off to the remaining in-sync member;
// neither is ever a target for the other's partitions.
TEST(ControlledShutdownControllerRegression, TwoNodesStoppingAtOnceRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    std::vector<int32_t> others;
    for (int32_t n : {1, 2, 3}) if (n != L) others.push_back(n);
    (void)c.svc->onAlterIsr(nodeControl(L, 3, true));
    (void)c.svc->onAlterIsr(nodeControl(others[0], 4, true));
    c.tick();
    c.tick();
    EXPECT_EQ(c.pa().leaderNode, others[1]);
    EXPECT_EQ(c.pa().isr, std::vector<int32_t>{others[1]});
}

// ---------------------------------------------------------------------------
// Node side (ControlledShutdown state machine) — deterministic, no transport:
// a recording send function and a real Election driven by hand.
// ---------------------------------------------------------------------------
namespace {
struct NullPublisher final : rune::consensus::ConsensusPublisher {
    void canvassPosition(NodeId, rune::consensus::Term, int64_t, rune::consensus::Term, NodeId) override {}
    void requestVote(NodeId, rune::consensus::Term, int64_t, rune::consensus::Term, NodeId) override {}
    void placeVote(NodeId, rune::consensus::Term, rune::consensus::Term, int64_t, NodeId, NodeId, bool) override {}
    void newLeadershipTerm(NodeId, rune::consensus::Term, rune::consensus::Term, int64_t, int64_t,
                           rune::consensus::Term, int64_t, int64_t, int64_t, int64_t, NodeId, int32_t, bool,
                           int64_t) override {}
    void appendPosition(NodeId, rune::consensus::Term, int64_t, NodeId, uint8_t) override {}
    void commitPosition(NodeId, rune::consensus::Term, int64_t, NodeId) override {}
};
struct NodeSideRig {
    SaeTempDir d{"csnode"};
    std::string base() const { return d.path; }
    rune::consensus::NodeStateFile nsf{d.path + "/nsf"};
    rune::consensus::TermLog       tl{d.path + "/tl"};
    NullPublisher pub;
    std::vector<rune::consensus::ClusterMember> members() {
        std::vector<rune::consensus::ClusterMember> ms(3);
        for (int i = 0; i < 3; ++i) { ms[i].id = i; ms[i].logPosition = 0; }
        return ms;
    }
    rune::consensus::Election e{true, -1, 0, 0, 0, 0, members(), 0, pub, nsf, tl,
                                150'000'000LL, 50'000'000LL, 300'000'000LL, 42};
    MetadataService md{nullptr, 0};
    rune::node::ControlledShutdown cs;
    int sends = 0;
    static bool send(void* self, int32_t, const rune::replication::AlterIsrMsg&) noexcept {
        ++static_cast<NodeSideRig*>(self)->sends;
        return true;
    }
    NodeSideRig() { cs.bind(0, 0x1234, &NodeSideRig::send, this, &md, &e, {0, 1, 2}); }
    /// Follow leader 1 (a CLOSED follower).
    void followLeader1(int64_t& now) {
        e.doWork(now);
        e.onRequestVote(0, 0, 1, 1, now);
        e.onNewLeadershipTerm(now, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 0);
        for (int i = 0; i < 20 && !e.isClosed(); ++i) e.doWork(++now);
        ASSERT_TRUE(e.isClosed());
        ASSERT_EQ(e.leaderMemberId(), 1);
    }
};
} // namespace

// LOW-3: a graceful stop before any controller is known (and leading nothing)
// completes at once — there is nobody to hand anything to.
TEST(ControlledShutdownNodeRegression, StopWithNoControllerKnownCompletesAtOnceRegression) {
    NodeSideRig r;
    ASSERT_EQ(r.e.leaderMemberId(), -1);
    ASSERT_TRUE(r.cs.begin());
    (void)r.cs.doWork(1'000'000);
    EXPECT_EQ(r.cs.phaseOnControl(), rune::node::ControlledShutdown::Phase::Done)
        << "a stop with no controller known waited for one";
}

// LOW-2: the Register request to a controller that never answers (an older
// binary) backs off — bounded traffic, not 20 rounds/s forever.
TEST(ControlledShutdownNodeRegression, RegisterBacksOffAgainstASilentControllerRegression) {
    NodeSideRig r;
    int64_t now = 1'000'000;
    r.followLeader1(now);
    for (int64_t t = 0; t < 60'000; t += 10) (void)r.cs.doWork(now + t * 1'000'000LL);   // 60 s, 10 ms ticks
    EXPECT_LT(r.sends, 3 * 100) << "unbounded Register re-sends to a silent controller";
    EXPECT_GT(r.sends, 0);
}

// LOW-5: the rolling-restart readiness gate — a node reports how many of its
// partitions it is NOT in sync for (committed map); 0 = safe to restart the next.
TEST(ControlledShutdownNodeRegression, ReportsOutOfSyncPartitionsForTheReadinessGateRegression) {
    rune::metadata::AssignmentMap m;
    m.put(0, 0, PartitionAssignment{1, {1, 0}, {0, 1, 2}, 3, 5});
    m.put(0, 1, PartitionAssignment{1, {1, 2}, {0, 1, 2}, 3, 5});   // 0 out of sync
    m.put(0, 2, PartitionAssignment{2, {2, 1}, {1, 2}, 3, 5});      // 0 not a replica
    EXPECT_EQ(rune::node::ControlledShutdown::outOfSync(m, 0), 1);
    EXPECT_EQ(rune::node::ControlledShutdown::outOfSync(m, 1), 0);
}

// ===========================================================================
// Controlled shutdown — fix round 2 (re-review 2026-10-05).
// ===========================================================================

// N1: a late request from a process this member never recorded (P1) must not
// fence the LIVE process (P2) forever: P2's next request is answered
// kNodeSuperseded (an event), P2 rotates to a fresh identity (P3) and P3 is
// adopted — the stale mark is cleared, with no controller change.
TEST(ControlledShutdownControllerRegression, LateUnseenOldProcessNeverFencesTheLiveOneRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t F = L == 1 ? 2 : 1;
    (void)c.svc->onAlterIsr(nodeControl(F, /*P2*/ 22, false));
    (void)c.svc->onAlterIsr(nodeControl(F, /*P1*/ 11, true));    // late frame of the dead process
    c.tick();
    ASSERT_FALSE(isrHas(c.pa(), F));
    rune::replication::AlterIsrResponseMsg resp{};
    bool answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, 22, false), &resp, &answer);   // the live process
    ASSERT_TRUE(answer) << "the live process's request was dropped silently (fenced until restart)";
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeSuperseded);
    EXPECT_EQ(resp.leaderEpoch, 22) << "the answer must echo the superseded identity";
    resp = {}; answer = false;
    (void)c.svc->onAlterIsr(nodeControl(F, /*P3*/ 33, false), &resp, &answer);
    EXPECT_EQ(resp.errorCode, rune::replication::kAlterIsrOk);
    std::vector<int32_t> isr = c.pa().isr;
    isr.push_back(F);
    EXPECT_TRUE(proposeIsr(c, isr)) << "the live node stayed fenced";
}

// N5: COMPLETE is pushed again when completion goes true → false → true.
TEST(ControlledShutdownControllerRegression, CompleteIsPushedAgainAfterItFlippedRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    for (int32_t n : {1, 2, 3}) (void)c.svc->onAlterIsr(nodeControl(n, 10 + n, false));
    (void)c.svc->onAlterIsr(nodeControl(L, 10 + L, true));
    c.tick();
    auto& out = c.svc->nodeControlResponseOutbox();
    auto confirmAll = [&](bool clear) {
        for (int32_t m : {2, 3}) if (m != L) (void)c.svc->onAlterIsr(confirmation(L, 10 + L, m, clear));
    };
    auto pushes = [&] {
        int n = 0;
        for (const auto& [to, r] : out) n += (to == L && r.errorCode == rune::replication::kAlterIsrOk) ? 1 : 0;
        return n;
    };
    confirmAll(true);
    ASSERT_EQ(pushes(), 1);
    confirmAll(false);   // a member's map no longer clear
    (void)c.svc->onAlterIsr(nodeControl(L, 10 + L, true));
    confirmAll(true);
    EXPECT_EQ(pushes(), 2) << "COMPLETE was not re-pushed after completion flipped back";
}

// N2 (controller side): a halted controller decides nothing — no move to
// itself while its data plane is going down.
TEST(ControlledShutdownControllerRegression, HaltedControllerDecidesNothingRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    c.svc->halt();
    (void)c.svc->onAlterIsr(nodeControl(L, 5, true));
    c.tick({1, 2, 3});
    c.tick({1});   // the others look dead: a live controller would fail over to itself
    EXPECT_EQ(c.pa().leaderNode, L);
    EXPECT_EQ(c.pa().leaderEpoch, le) << "a halted controller still decided";
}

namespace {
struct RecordingRig : NodeSideRig {
    std::vector<rune::replication::AlterIsrMsg> sent;
    static bool rec(void* self, int32_t, const rune::replication::AlterIsrMsg& m) noexcept {
        static_cast<RecordingRig*>(self)->sent.push_back(m);
        return true;
    }
    RecordingRig() { cs.bind(0, 0x1234, &RecordingRig::rec, this, &md, &e, {0, 1, 2}); }
    static int64_t nonceOf(const rune::replication::AlterIsrMsg& m) {
        return static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(m.isr[4])) << 32) |
                                    static_cast<uint32_t>(m.isr[3]));
    }
    void answer(int16_t code) {
        rune::replication::AlterIsrResponseMsg r{};
        r.topicId = rune::replication::AlterIsrMsg::kNodeControlTopicId;
        r.partition = 1; r.leaderId = 0;
        const int64_t n = cs.nonceOnControl();
        r.leaderEpoch = static_cast<int32_t>(static_cast<uint32_t>(n));
        r.partitionEpoch = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(n) >> 32));
        r.errorCode = code;
        cs.onResponse(r);
    }
};
} // namespace

// N1 (node side): kNodeSuperseded → a fresh identity, re-sent at once.
TEST(ControlledShutdownNodeRegression, SupersededAnswerRotatesTheIdentityAndResendsRegression) {
    RecordingRig r;
    int64_t now = 1'000'000;
    r.followLeader1(now);
    (void)r.cs.doWork(now);
    ASSERT_FALSE(r.sent.empty());
    const int64_t before = r.cs.nonceOnControl();
    r.answer(rune::replication::AlterIsrMsg::kNodeSuperseded);
    r.sent.clear();
    (void)r.cs.doWork(now + 1);   // well inside the 50 ms re-send window
    ASSERT_FALSE(r.sent.empty()) << "no immediate re-send after kNodeSuperseded";
    EXPECT_NE(RecordingRig::nonceOf(r.sent.back()), before) << "the identity was not rotated";
    EXPECT_EQ(RecordingRig::nonceOf(r.sent.back()), r.cs.nonceOnControl());
}

// N4: the readiness gate turns unknown (-1) when the controller is lost.
TEST(ControlledShutdownNodeRegression, ReadinessGateUnknownWithoutAControllerRegression) {
    RecordingRig r;
    int64_t now = 1'000'000;
    r.followLeader1(now);
    (void)r.cs.doWork(now);
    r.answer(rune::replication::kAlterIsrOk);   // registered with controller 1
    (void)r.cs.doWork(++now);
    (void)r.cs.doWork(++now);
    ASSERT_EQ(r.cs.observe().phase, rune::node::ControlledShutdown::Phase::Running);
    ASSERT_EQ(r.cs.observe().remaining, 0);
    r.e.onStandDown(++now, r.e.leadershipTermId(), 1);   // controller gone (election)
    ASSERT_EQ(r.e.leaderMemberId(), -1);
    (void)r.cs.doWork(++now);
    EXPECT_EQ(r.cs.observe().remaining, -1) << "the readiness gate kept a stale value without a controller";
}

// N2 (node side): halt — never stands again, resigns if it leads, tells the
// members it is going (they avoid it at once), acks Halted.
TEST(ControlledShutdownNodeRegression, HaltStopsDecidingAndAcksRegression) {
    RecordingRig r;
    int64_t now = 1'000'000;
    r.followLeader1(now);
    ASSERT_TRUE(r.cs.halt());
    r.sent.clear();
    (void)r.cs.doWork(++now);
    EXPECT_EQ(r.cs.observe().phase, rune::node::ControlledShutdown::Phase::Halted);
    EXPECT_TRUE(r.e.resigned()) << "the halted node may still stand for election";
    bool told = false;
    for (const auto& m : r.sent) told = told || (m.isr[1] & rune::replication::AlterIsrMsg::kNodeShutdown) != 0;
    EXPECT_TRUE(told) << "the members were not told the node is going";
}

// D17 / DR-10 review MAJOR-1: a consensus leader that stepped down (quorum loss,
// higher-term NLT, granted RequestVote — or a resumed zombie before it does) keeps
// its __cluster_metadata replica's Leader role until the bridge learns a NEW
// leader.  Its controller must stop deciding the moment consensus leadership is
// gone: otherwise it reassigns every partition it leads over an eligible set
// that no longer contains itself, and those records COMMIT while the followers
// still fetch from it.
TEST(ControllerConsensusGateRegression, SteppedDownConsensusLeaderEmitsNoReassignmentRegression) {
    SaeTempDir dir1("cg1"), dir2("cg2"), dir3("cg3");
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
    svc1.configure({TopicSpec{0, 3, 3, "", "t"}}, allNodes);
    int64_t clock = 0;
    for (int i = 0; i < 50; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        (void)svc1.doWork(clock);
    }
    ASSERT_TRUE(svc1.controllerReady());
    const Offset leoBefore = r1->logEndOffset();

    // Consensus leadership is gone; the metadata replica still reads Leader.
    svc1.setConsensusLeads(false);
    EXPECT_FALSE(svc1.controllerReady()) << "a non-consensus-leader must not decide";
    EXPECT_EQ(svc1.runDynamicRebalanceOnTick({kNode2, kNode3}, nullptr), 0u)
        << "a stepped-down controller reassigned partitions over an eligible set without itself";
    EXPECT_EQ(r1->logEndOffset(), leoBefore) << "a stepped-down controller appended to the metadata log";

    svc1.setConsensusLeads(true);
    EXPECT_TRUE(svc1.controllerReady()) << "regaining consensus leadership restores deciding";
}


// ===========================================================================
// D15 (2026-10-07): memory-backed replica epoch edge cases.
// ===========================================================================

namespace {
/// The metadata controller moves from node 1 to node 2 (consensus term 2):
/// a NEW MetadataService on node 2 that knows only the replicated log.
struct Failover {
    CtlRig& c;
    MetadataService svc2;
    explicit Failover(CtlRig& rig, bool survivor = false) : c(rig), svc2(rig.r2.get(), 2) {
        c.r1->becomeFollower(2, 2);
        c.r3->becomeFollower(2, 2);
        c.r2->becomeLeader(2, c.all);
        c.transport.removeFollower(2);
        c.transport.setLeader(c.r2.get());
        c.transport.addFollower(1, c.r1.get());
        svc2.configure(c.topics, c.all);
        svc2.setAllowSurvivorElection(survivor);
        drive(100);
    }
    void drive(int n) {
        for (int i = 0; i < n; ++i) {
            c.clock += 1'000'000LL;
            (void)svc2.doWork(c.clock);
            c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
            c.transport.pump(c.clock);
        }
    }
    void tick(std::vector<NodeId> eligible = {1, 2, 3}) {
        (void)svc2.reassignDeadPartitionLeaders(eligible, 0);
        drive(50);
    }
    const PartitionAssignment& pa(int32_t t = 0) { return *svc2.assignment(t, 0); }
};
rune::replication::AlterIsrMsg reportOf(int32_t topic, int32_t node, int32_t epoch, int32_t leo, int64_t inc,
                                        bool logDirFailed = false) {
    auto m = leaderlessReport(node, epoch, leo, inc, logDirFailed);
    m.topicId = topic;
    return m;
}
rune::replication::AlterIsrMsg bumpOf(const PartitionAssignment& a, int32_t topic, int64_t inc, bool memory,
                                      int32_t leo = 0, bool logDirFailed = false) {
    using rune::replication::AlterIsrMsg;
    AlterIsrMsg m{};
    m.topicId = topic; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
    m.isrCount = AlterIsrMsg::kLeaderEpochBump;
    m.partitionEpoch = a.partitionEpoch;
    m.isr[0] = a.leaderEpoch;
    m.isr[1] = AlterIsrMsg::kBumpFenceValid | (memory ? AlterIsrMsg::kBumpMemoryBacked : 0) |
               (logDirFailed ? AlterIsrMsg::kBumpLogDirFailed : 0);
    m.isr[2] = leo;
    m.isr[3] = static_cast<int32_t>(inc);
    return m;
}
} // namespace

// D15 item 1: a sole-ISR MEMORY leader restarted EMPTY and its partition was
// made leaderless, to be decided on fresh reports.  The controller then FAILS
// OVER.  The successor knew nothing of it (the mark was controller memory):
// its balancer re-elected the empty leader blind — wiping the replica that
// holds the data.  The mark is committed with the leaderless record: the
// successor re-derives it, re-arms the partition at a new leaderless epoch
// (every replica reports to IT), and decides on those fresh reports.
TEST(D15MemoryEpochRegression, EmptyMemoryLeaderMarkSurvivesControllerFailoverRegression) {
    CtlRig c;
    c.svc->setAllowSurvivorElection(true);
    c.soleIsr();
    const int32_t L = c.pa().leaderNode;
    ASSERT_NE(L, 2) << "precondition: the empty leader is not the successor controller";
    const int32_t G = 6 - L - 2;                       // holds the data (not in the ISR)
    (void)c.svc->onAlterIsr(bumpOf(c.pa(), 0, /*inc=*/2, /*memory=*/true));
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, NO_LEADER);
    const int32_t lle = c.pa().leaderEpoch;
    Failover f(c, /*survivor=*/true);
    ASSERT_TRUE(f.svc2.controllerCaughtUpForTest());
    f.tick();
    EXPECT_NE(f.pa().leaderNode, L) << "the successor controller re-elected the empty memory leader blind";
    ASSERT_EQ(f.pa().leaderNode, NO_LEADER);
    const int32_t rearmed = f.pa().leaderEpoch;
    EXPECT_EQ(rearmed, lle + 1) << "the successor did not re-arm the leaderless partition exactly once";
    // Reports to the OLD leaderless epoch (answered by the dead controller) decide nothing.
    (void)f.svc2.onAlterIsr(reportOf(0, L, lle, 0, 2));
    (void)f.svc2.onAlterIsr(reportOf(0, 2, lle, 0, 1));
    (void)f.svc2.onAlterIsr(reportOf(0, G, lle, 10, 1));
    f.tick();
    EXPECT_EQ(f.pa().leaderNode, NO_LEADER) << "decided on reports of a previous leaderless epoch";
    (void)f.svc2.onAlterIsr(reportOf(0, L, rearmed, 0, 2));
    (void)f.svc2.onAlterIsr(reportOf(0, 2, rearmed, 0, 1));
    f.tick();
    EXPECT_EQ(f.pa().leaderNode, NO_LEADER) << "decided before every live replica reported";
    (void)f.svc2.onAlterIsr(reportOf(0, G, rearmed, 10, 1));
    f.tick();
    EXPECT_EQ(f.pa().leaderNode, G) << "the replica holding the data was not elected";
    EXPECT_EQ(f.pa().leaderEpoch, rearmed + 1);
    const auto leo = c.r2->logEndOffset();
    for (int i = 0; i < 5; ++i) f.tick();
    EXPECT_EQ(c.r2->logEndOffset(), leo) << "the decided partition kept churning records";
}

// D15 item 1 (N1 across a failover): the sole-ISR leader's log dir went
// offline; the partition was made leaderless.  The successor controller must
// not re-grant the offline replica (it refused again + one more NO_LEADER
// record per controller change before); a report from its NEW incarnation
// (healthy dir) — sent because the successor re-armed the partition —
// re-registers it and it is elected.
TEST(D15MemoryEpochRegression, LogDirOfflineMarkSurvivesControllerFailoverRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t L = c.pa().leaderNode;
    ASSERT_NE(L, 2);
    (void)c.svc->onAlterIsr(bumpOf(c.pa(), 0, /*inc=*/5, /*memory=*/false, /*leo=*/3, /*failed=*/true));
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, NO_LEADER);
    Failover f(c);
    f.tick();
    EXPECT_NE(f.pa().leaderNode, L) << "the successor controller re-granted a replica whose log dir is offline";
    ASSERT_EQ(f.pa().leaderNode, NO_LEADER);
    const int32_t rearmed = f.pa().leaderEpoch;
    (void)f.svc2.onAlterIsr(reportOf(0, L, rearmed, 3, 5, /*failed=*/true));   // same incarnation: still offline
    f.tick();
    EXPECT_EQ(f.pa().leaderNode, NO_LEADER);
    (void)f.svc2.onAlterIsr(reportOf(0, L, rearmed, 3, 6));                    // restarted, healthy
    f.tick();
    EXPECT_EQ(f.pa().leaderNode, L) << "the re-registered replica was never elected";
    EXPECT_EQ(f.pa().leaderEpoch, rearmed + 1);
}

// D15 item 2: the two same-index partitions of a co-partition group are led
// by one MEMORY node that restarts empty; both go leaderless.  Topic 0's only
// ISR member is that node (empty: a clean choice for it); topic 1's ISR holds
// replicas WITH data.  Decided per partition, the unit split (topic 0 → the
// empty node, topic 1 → a data holder).  Decided as a unit: no leader is
// clean for both — held as a unit (survivor election off), or ONE survivor
// for both (on).
TEST(D15MemoryEpochRegression, CoPartitionUnitLeaderlessDecidedAsOneUnitRegression) {
    for (const bool survivor : {false, true}) {
        SCOPED_TRACE(survivor ? "survivor election ON" : "survivor election OFF");
        CtlRig c(true, {TopicSpec{0, 1, 3, "g", "k"}, TopicSpec{1, 1, 3, "g", "k"}});
        c.svc->setAllowSurvivorElection(survivor);
        const PartitionAssignment a0 = *c.svc->assignment(0, 0);
        const int32_t L = a0.leaderNode;
        ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L) << "precondition: the unit is colocated";
        {   // topic 0: ISR shrinks to {L}
            rune::replication::AlterIsrMsg m{};
            m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = a0.leaderEpoch;
            m.isrCount = 1; m.isr[0] = L; m.partitionEpoch = a0.partitionEpoch;
            ASSERT_TRUE(c.svc->onAlterIsr(m));
            c.drive(50);
        }
        (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(0, 0), 0, 2, /*memory=*/true));
        (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(1, 0), 1, 2, /*memory=*/true));
        // The data holders are not live at the bump: no clean failover exists.
        c.tick({L});
        ASSERT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER);
        ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, NO_LEADER);
        const int32_t e0 = c.svc->assignment(0, 0)->leaderEpoch, e1 = c.svc->assignment(1, 0)->leaderEpoch;
        for (int32_t n : {1, 2, 3}) {
            (void)c.svc->onAlterIsr(reportOf(0, n, e0, 0, n == L ? 2 : 1));
            (void)c.svc->onAlterIsr(reportOf(1, n, e1, n == L ? 0 : 10, n == L ? 2 : 1));
        }
        c.tick();
        // Review M2 ruling: data safety beats colocation — no leader is clean
        // for both, so each member gets ITS clean leader (no unclean election
        // for either; the preferred-leader pass restores colocation later).
        const int32_t l0 = c.svc->assignment(0, 0)->leaderNode, l1 = c.svc->assignment(1, 0)->leaderNode;
        EXPECT_EQ(l0, L) << "topic 0's only clean candidate is its sole ISR member";
        EXPECT_NE(l1, L) << "the empty node was elected over replicas holding topic 1's data";
        EXPECT_NE(l1, NO_LEADER) << "topic 1 has clean candidates, yet it was held";
        EXPECT_EQ(c.svc->assignment(1, 0)->isr.size(), 3u) << "topic 1 was elected uncleanly";
    }
}

// D15 review M2: each member of a co-partition unit has a DIFFERENT clean
// leader and none is common.  Survivor (unclean) election must not elect one
// replica for both — the member it is not clean for would lose committed
// records; each member gets its own clean leader instead.
TEST(D15MemoryEpochRegression, CoPartitionUnitNeverUncleanWhenEachMemberHasACleanLeaderRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "g", "k"}, TopicSpec{1, 1, 3, "g", "k"}});
    c.svc->setAllowSurvivorElection(true);
    const int32_t L = c.svc->assignment(0, 0)->leaderNode;
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L);
    const int32_t A = L == 1 ? 2 : 1, B = 6 - L - A;
    auto shrink = [&](int32_t topic, int32_t other) {
        const PartitionAssignment a = *c.svc->assignment(topic, 0);
        rune::replication::AlterIsrMsg m{};
        m.topicId = topic; m.partition = 0; m.leaderId = L; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = 2; m.isr[0] = L; m.isr[1] = other; m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(c.svc->onAlterIsr(m));
        c.drive(50);
    };
    shrink(0, A);   // topic 0 ISR {L, A}
    shrink(1, B);   // topic 1 ISR {L, B}
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(0, 0), 0, 2, /*memory=*/true));
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(1, 0), 1, 2, /*memory=*/true));
    c.tick({L});
    ASSERT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER);
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, NO_LEADER);
    const int32_t e0 = c.svc->assignment(0, 0)->leaderEpoch, e1 = c.svc->assignment(1, 0)->leaderEpoch;
    // topic 0: A holds 10 (clean), B 5;  topic 1: B holds 10 (clean), A 5.
    (void)c.svc->onAlterIsr(reportOf(0, L, e0, 0, 2));
    (void)c.svc->onAlterIsr(reportOf(0, A, e0, 10, 1));
    (void)c.svc->onAlterIsr(reportOf(0, B, e0, 5, 1));
    (void)c.svc->onAlterIsr(reportOf(1, L, e1, 0, 2));
    (void)c.svc->onAlterIsr(reportOf(1, A, e1, 5, 1));
    (void)c.svc->onAlterIsr(reportOf(1, B, e1, 10, 1));
    c.tick();
    EXPECT_EQ(c.svc->assignment(0, 0)->leaderNode, A) << "topic 0 lost committed records (unclean election)";
    EXPECT_EQ(c.svc->assignment(1, 0)->leaderNode, B) << "topic 1 lost committed records (unclean election)";
}

// D15 review m3: a co-partition sibling leaderless for ANOTHER reason (its
// leader's log dir went offline) is decided WITH the unit's empty-memory
// member — before, only the memory member was decided and the sibling, whose
// group ISR intersection excluded the offline node, stayed leaderless although
// a clean candidate of its own was live.
TEST(D15MemoryEpochRegression, CoPartitionUnitDecidesSiblingsLeaderlessForOtherReasonsRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "g", "k"}, TopicSpec{1, 1, 3, "g", "k"}});
    const int32_t L = c.svc->assignment(0, 0)->leaderNode;
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L);
    const int32_t A = L == 1 ? 2 : 1, B = 6 - L - A;
    auto shrink = [&](int32_t topic, std::vector<int32_t> isr) {
        const PartitionAssignment a = *c.svc->assignment(topic, 0);
        rune::replication::AlterIsrMsg m{};
        m.topicId = topic; m.partition = 0; m.leaderId = L; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) m.isr[i] = isr[i];
        m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(c.svc->onAlterIsr(m));
        c.drive(50);
    };
    shrink(0, {L});      // topic 0 (memory): sole ISR
    shrink(1, {L, A});   // topic 1 (disk)
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(0, 0), 0, 2, /*memory=*/true));
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(1, 0), 1, 2, /*memory=*/false, /*leo=*/7, /*failed=*/true));
    c.tick({L});
    ASSERT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER);
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, NO_LEADER);
    const int32_t e0 = c.svc->assignment(0, 0)->leaderEpoch, e1 = c.svc->assignment(1, 0)->leaderEpoch;
    for (int32_t n : {L, A, B}) (void)c.svc->onAlterIsr(reportOf(0, n, e0, 0, n == L ? 2 : 1));
    (void)c.svc->onAlterIsr(reportOf(1, L, e1, 7, 2, /*failed=*/true));
    (void)c.svc->onAlterIsr(reportOf(1, A, e1, 7, 1));
    (void)c.svc->onAlterIsr(reportOf(1, B, e1, 3, 1));
    c.tick();
    c.tick();
    EXPECT_EQ(c.svc->assignment(0, 0)->leaderNode, L);
    EXPECT_EQ(c.svc->assignment(1, 0)->leaderNode, A)
        << "the sibling leaderless for another reason was never decided with its unit";
}

// D15 review M1: the leader-epoch FENCE protocol is unconditional — a log
// written by pre-D15 binaries (no FeatureLevelRecord: level 0) and a member
// that never re-registers (the level can never be raised) must keep main's
// 9c80eab behaviour.  A memory-backed sole-ISR leader restarted EMPTY is
// told (bridge, committed map) to lead the epoch it already led: it refuses,
// asks for a new epoch, and the controller makes the partition leaderless at
// a NEW epoch (decided on fresh reports) — never a second history under the
// old epoch (committed loss).  At 2cccff5 the level-0 replica led it.
TEST(D15FeatureGateRegression, FenceProtocolUnconditionalAtLevelZeroRegression) {
    SaeTempDir d1("fz1"), d2("fz2"), d3("fz3"), df("fzf");
    rune::storage::Log log1(d1.path), log2(d2.path), log3(d3.path);
    log1.recover(); log2.recover(); log3.recover();
    const std::vector<NodeId> all{1, 2, 3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, all, all, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, all, all, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, all, all, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(2, r2.get());
    transport.addFollower(3, r3.get());
    MetadataService svc(r1.get(), 1);
    svc.setSupportedFeatureLevel(MetadataFeature::kBase);   // the log predates D15: never bootstrapped
    svc.configure({TopicSpec{0, 1, 3, "", "t"}}, all);
    svc.setClusterMembers(all);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            (void)svc.doWork(clock);
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
        }
    };
    drive(50);
    auto reg = [&](int32_t node, int64_t inc, int16_t level) {
        auto m = nodeControl(node, inc, false);
        m.isr[AlterIsrMsg::kNodeFeatureLevelSlot] = level;
        (void)svc.onAlterIsr(m);
    };
    reg(2, 11, MetadataFeature::kLatest);   // node 3 never re-registers: the level stays 0
    drive(20);
    ASSERT_EQ(svc.featureLevel(), MetadataFeature::kBase);
    const PartitionAssignment a = *svc.assignment(0, 0);
    const int32_t L = a.leaderNode;
    {   // sole ISR {L}
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = 1; m.isr[0] = L; m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(svc.onAlterIsr(m));
        drive(50);
    }
    const int32_t le = svc.assignment(0, 0)->leaderEpoch;
    // L restarted: an EMPTY memory log, its fence says it led `le`.
    {
        std::FILE* fp = std::fopen((df.path + "/fence").c_str(), "w");
        ASSERT_NE(fp, nullptr);
        std::fprintf(fp, "%d\n", le);
        std::fclose(fp);
    }
    struct Capture final : InMemoryTransport {
        std::vector<AlterIsrMsg> sent;
        bool sendAlterIsr(const AlterIsrMsg& m) override { sent.push_back(m); return true; }
    } cap;
    rune::storage::InMemoryLog mem(1 << 20, 10'000);
    ReplicaRegistry regy;
    PartitionConfig cfg = makeMetaCfg(L);
    cfg.topicId = 0;
    regy.add(0, 0, std::make_unique<PartitionReplica>(cfg, Role::Follower, 0, all, all, &mem, &cap));
    PartitionReplica* r = regy.lookup(0, 0);
    r->setIsrChannel(&cap);
    r->setProcessIncarnation(2);
    r->setLeaderEpochFenceFile(df.path + "/fence");
    ReplicationMetadataBridge bridge(L, all, regy, &svc.msmForTest());
    (void)bridge.doWork(1, 1);
    EXPECT_NE(r->role(), Role::Leader) << "led epoch " << le << " again with an EMPTY log at feature level 0";
    for (int i = 0; i < 3 && cap.sent.empty(); ++i) r->doWork(clock += 100'000'000LL);
    ASSERT_FALSE(cap.sent.empty()) << "no new epoch requested at feature level 0";
    (void)svc.onAlterIsr(cap.sent.back());
    (void)svc.reassignDeadPartitionLeaders(all, 0);
    drive(50);
    const auto* pa = svc.assignment(0, 0);
    EXPECT_GT(pa->leaderEpoch, le) << "the controller never moved the partition past the fenced epoch";
    EXPECT_EQ(pa->leaderNode, NO_LEADER) << "the empty memory leader was re-elected blind";
    (void)bridge.doWork(1, 1);
    EXPECT_NE(r->role(), Role::Leader);
    EXPECT_EQ(svc.featureLevel(), MetadataFeature::kBase);
}

// D15 item 3 (controller side): the level is raised only once EVERY cluster
// member advertised support for it; a brand-new metadata log is bootstrapped
// at the latest level; a log written by older binaries stays at the base
// level while any member is an older binary (or has not registered).
TEST(D15FeatureGateRegression, LevelRaisedOnlyOnceEveryMemberSupportsItRegression) {
    {
        CtlRig fresh;
        EXPECT_EQ(fresh.svc->featureLevel(), MetadataFeature::kLatest) << "a fresh log was not bootstrapped";
    }
    // A log written by an older binary: no bootstrap record.
    SaeTempDir d1("fl1"), d2("fl2"), d3("fl3");
    rune::storage::Log log1(d1.path), log2(d2.path), log3(d3.path);
    log1.recover(); log2.recover(); log3.recover();
    const std::vector<NodeId> all{1, 2, 3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, all, all, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, all, all, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, all, all, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(2, r2.get());
    transport.addFollower(3, r3.get());
    MetadataService svc(r1.get(), 1);
    svc.setSupportedFeatureLevel(MetadataFeature::kBase);   // emulates the older binary
    svc.configure({TopicSpec{0, 1, 3, "", "t"}}, all);
    svc.setClusterMembers(all);
    int64_t clock = 0;
    auto drive = [&](MetadataService& s, int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            (void)s.doWork(clock);
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
        }
    };
    drive(svc, 50);
    ASSERT_NE(svc.assignment(0, 0), nullptr);
    ASSERT_EQ(svc.featureLevel(), MetadataFeature::kBase);
    // Node 1 is upgraded (consensus term 2 on the same log).
    r1->becomeFollower(2, 1);
    r1->becomeLeader(2, all);
    r2->becomeFollower(2, 1);
    r3->becomeFollower(2, 1);
    MetadataService up(r1.get(), 1);
    up.configure({TopicSpec{0, 1, 3, "", "t"}}, all);
    up.setClusterMembers(all);
    drive(up, 50);
    ASSERT_TRUE(up.controllerCaughtUpForTest());
    auto reg = [&](int32_t node, int64_t inc, int16_t level) {
        auto m = nodeControl(node, inc, false);
        m.isr[AlterIsrMsg::kNodeFeatureLevelSlot] = level;
        (void)up.onAlterIsr(m);
    };
    const auto leo0 = r1->logEndOffset();
    reg(2, 11, MetadataFeature::kLatest);
    reg(3, 21, MetadataFeature::kBase);                        // node 3 still runs the older binary
    drive(up, 50);
    EXPECT_EQ(up.featureLevel(), MetadataFeature::kBase) << "raised while a member is an older binary";
    EXPECT_EQ(r1->logEndOffset(), leo0);
    reg(3, 22, MetadataFeature::kLatest);                      // node 3 restarted upgraded
    drive(up, 50);
    EXPECT_EQ(up.featureLevel(), MetadataFeature::kLatest) << "never raised once every member supports it";
    EXPECT_EQ(r1->logEndOffset(), leo0 + 1) << "exactly one FeatureLevelRecord";
    reg(3, 23, MetadataFeature::kBase);                        // a downgrade attempt: monotonic
    drive(up, 50);
    EXPECT_EQ(up.featureLevel(), MetadataFeature::kLatest);
}

// D15 re-review MAJOR (m3): while one member of a co-partition unit is HELD
// leaderless (an empty memory leader, no clean candidate, survivor election
// off), a sibling whose live leader dies must still fail over normally — the
// balancer filter may hold back only the leaderless member's record.
TEST(D15MemoryEpochRegression, HeldUnitMemberNeverBlocksSiblingFailoverRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "g", "k"}, TopicSpec{1, 1, 3, "g", "k"}});
    const int32_t L = c.svc->assignment(0, 0)->leaderNode;
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L);
    const int32_t A = L == 1 ? 2 : 1, B = 6 - L - A;
    {   // topic 0 (memory): ISR {L, A}
        const PartitionAssignment a = *c.svc->assignment(0, 0);
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = L; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = 2; m.isr[0] = L; m.isr[1] = A; m.partitionEpoch = a.partitionEpoch;
        ASSERT_TRUE(c.svc->onAlterIsr(m));
        c.drive(50);
    }
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(0, 0), 0, 2, /*memory=*/true));
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(1, 0), 1, 2, /*memory=*/false, /*leo=*/5));
    c.tick({L});   // A, B not live: topic 0 goes leaderless, topic 1 stays with L
    ASSERT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER);
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L);
    const int32_t e0 = c.svc->assignment(0, 0)->leaderEpoch;
    (void)c.svc->onAlterIsr(reportOf(0, L, e0, 0, 2));
    (void)c.svc->onAlterIsr(reportOf(0, A, e0, 0, 1));
    (void)c.svc->onAlterIsr(reportOf(0, B, e0, 10, 1));   // non-ISR holds more: no clean candidate
    c.tick({A, B});   // L dies
    c.tick({A, B});
    EXPECT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER) << "the held member was elected uncleanly";
    EXPECT_EQ(c.svc->assignment(1, 0)->leaderNode, A)
        << "the sibling's dead leader was never failed over while a unit member is held";
}

// D15b l1: a successor controller restores MORE marks at its claim than the
// claim cycle drains (kMaxRecordsPerBatch = 256 per drain; the claim cycle
// drains twice): the re-arms past those are still QUEUED when the first
// rebalance (recheckOnCatchUp_) runs.  RED at 4b0b6a6: 188 duplicates.  That
// rebalance filtered only inflight_, so the balancer elected a leader for a
// still-queued key at the SAME leaderEpoch as its queued re-arm — a wasted
// record the MSM rejects.  Keys in the pending queue are decided next tick.
TEST(D15MemoryEpochRegression, MoreMarksThanOneBatchFirstRebalanceSkipsQueuedKeysRegression) {
    constexpr int32_t kParts = 700;   // > the claim cycle's drains (2 x kMaxRecordsPerBatch)
    static_assert(kParts > 2 * static_cast<int32_t>(MetadataService::kMaxRecordsPerBatch));
    CtlRig c(true, {TopicSpec{0, kParts, 3, "", "t"}});
    for (int32_t p = 0; p < kParts; ++p) ASSERT_NE(c.svc->assignment(0, p), nullptr);
    // The predecessor made every partition leaderless: its leader's log dir is
    // offline (mark committed with the record), the ISR lineage kept.
    std::vector<BalancerRecord> recs;
    for (int32_t p = 0; p < kParts; ++p) {
        const PartitionAssignment a = *c.svc->assignment(0, p);
        BalancerRecord br;
        br.isr      = a.isr;
        br.replicas = a.replicas;
        br.mark     = LeaderlessMark{LeaderlessMark::Kind::LogDirOffline, a.leaderNode, 5};
        br.rec.topicId        = 0;
        br.rec.partition      = p;
        br.rec.leaderNode     = NO_LEADER;
        br.rec.leaderEpoch    = a.leaderEpoch + 1;
        br.rec.partitionEpoch = std::max(a.partitionEpoch + 1, br.rec.leaderEpoch);
        br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
        br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
        recs.push_back(std::move(br));
    }
    c.svc->emitDynamicAssignment(recs);
    c.drive(50);
    for (int32_t p = 0; p < kParts; ++p) ASSERT_EQ(c.svc->assignment(0, p)->leaderNode, NO_LEADER);
    // The controller moves to node 2; drive ONLY until it caught up (the claim
    // restores the marks and drains the first batch).
    MetadataService svc2(c.r2.get(), 2);
    c.r1->becomeFollower(2, 2);
    c.r3->becomeFollower(2, 2);
    c.r2->becomeLeader(2, c.all);
    c.transport.removeFollower(2);
    c.transport.setLeader(c.r2.get());
    c.transport.addFollower(1, c.r1.get());
    svc2.configure(c.topics, c.all);
    for (int i = 0; i < kDriveCap; ++i) {   // (caught-up reads true before the first cycle)
        c.clock += 1'000'000LL;
        (void)svc2.doWork(c.clock);
        if (svc2.controllerCaughtUpForTest()) break;
        c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
        c.transport.pump(c.clock);
    }
    ASSERT_TRUE(svc2.controllerCaughtUpForTest());
    // The first rebalance, while kParts - 256 re-arms are still queued: every
    // key has a record in flight or queued — nothing to decide on it now.
    ASSERT_TRUE(svc2.takePreferredRecheck());
    EXPECT_EQ(svc2.reassignDeadPartitionLeaders({1, 2, 3}), 0u)
        << "the first rebalance re-decided keys whose re-arm is still queued (same-epoch duplicate)";
    // Then the partitions are decided normally on the committed state.
    for (int i = 0; i < 4; ++i) {
        for (int k = 0; k < 50; ++k) {
            c.clock += 1'000'000LL;
            (void)svc2.doWork(c.clock);
            c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
            c.transport.pump(c.clock);
        }
        (void)svc2.reassignDeadPartitionLeaders({1, 2, 3});
    }
    for (int k = 0; k < 50; ++k) {
        c.clock += 1'000'000LL;
        (void)svc2.doWork(c.clock);
        c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
        c.transport.pump(c.clock);
    }
    for (int32_t p = 0; p < kParts; ++p) {
        const auto* pa = svc2.assignment(0, p);
        ASSERT_NE(pa, nullptr);
        EXPECT_NE(pa->leaderNode, NO_LEADER) << "partition " << p << " never decided";
    }
}

// D15b l2: a co-partition unit is decided as ONE unit, so a unit whose
// leaderless members exceed the remaining per-tick record budget waits for the
// next tick (the runtime re-ticks on the very next cycle after any emit:
// SlowTick::done).  But a unit LARGER than the whole budget never fit — not
// even on an empty tick — and stayed leaderless forever.  An empty tick now
// always admits the unit (the drain stays bounded by kMaxRecordsPerBatch).
TEST(D15MemoryEpochRegression, UnitLargerThanTheRecordBudgetIsStillDecidedRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "g", "k"}, TopicSpec{1, 1, 3, "g", "k"}});
    const int32_t L = c.svc->assignment(0, 0)->leaderNode;
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, L);
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(0, 0), 0, 2, /*memory=*/true));
    (void)c.svc->onAlterIsr(bumpOf(*c.svc->assignment(1, 0), 1, 2, /*memory=*/true));
    c.tick({L});   // the data holders are not live: both members go leaderless
    ASSERT_EQ(c.svc->assignment(0, 0)->leaderNode, NO_LEADER);
    ASSERT_EQ(c.svc->assignment(1, 0)->leaderNode, NO_LEADER);
    const int32_t e0 = c.svc->assignment(0, 0)->leaderEpoch, e1 = c.svc->assignment(1, 0)->leaderEpoch;
    for (int32_t n : {1, 2, 3}) {
        (void)c.svc->onAlterIsr(reportOf(0, n, e0, n == L ? 0 : 10, n == L ? 2 : 1));
        (void)c.svc->onAlterIsr(reportOf(1, n, e1, n == L ? 0 : 10, n == L ? 2 : 1));
    }
    // A record budget of 1 per tick: the 2-member unit never fits a tick.
    for (int i = 0; i < 4; ++i) {
        (void)c.svc->runDynamicRebalanceOnTick({1, 2, 3}, nullptr, 0, /*maxRecordsPerCycle=*/1);
        c.drive(50);
    }
    const int32_t l0 = c.svc->assignment(0, 0)->leaderNode, l1 = c.svc->assignment(1, 0)->leaderNode;
    EXPECT_NE(l0, NO_LEADER) << "a unit larger than the per-tick record budget was never decided";
    EXPECT_EQ(l0, l1) << "the unit was split";
    EXPECT_NE(l0, L) << "the empty memory leader was elected over the replicas holding the data";
}

// D15b l5 (ruling pinned): a NO_LEADER record written by a pre-D15 (main)
// controller carries no mark.  The successor has nothing to restore: it
// invents no mark and does not re-arm the partition (no extra record) — the
// balancer decides it on the committed ISR exactly as main does.
TEST(D15MemoryEpochRegression, UnmarkedLegacyLeaderlessRecordDecidedAsOnMainRegression) {
    CtlRig c;
    const PartitionAssignment a = c.pa();
    {
        std::vector<BalancerRecord> recs(1);
        BalancerRecord& br = recs[0];
        br.isr      = a.isr;
        br.replicas = a.replicas;   // br.mark: Kind::None — no trailer, as main writes it
        br.rec.topicId        = 0;
        br.rec.partition      = 0;
        br.rec.leaderNode     = NO_LEADER;
        br.rec.leaderEpoch    = a.leaderEpoch + 1;
        br.rec.partitionEpoch = std::max(a.partitionEpoch + 1, br.rec.leaderEpoch);
        br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
        br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
        c.svc->emitDynamicAssignment(recs);
        c.drive(50);
    }
    ASSERT_EQ(c.pa().leaderNode, NO_LEADER);
    ASSERT_EQ(c.pa().mark.kind, LeaderlessMark::Kind::None);
    const int32_t lle = c.pa().leaderEpoch;
    Failover f(c);
    ASSERT_TRUE(f.svc2.controllerCaughtUpForTest());
    EXPECT_EQ(f.pa().leaderEpoch, lle) << "an unmarked leaderless partition was re-armed at the claim";
    EXPECT_EQ(f.pa().mark.kind, LeaderlessMark::Kind::None);
    f.tick();
    EXPECT_NE(f.pa().leaderNode, NO_LEADER) << "the balancer did not decide the legacy leaderless partition";
    EXPECT_EQ(f.pa().leaderEpoch, lle + 1);
}

// ===========================================================================
// E2 final fix wave (C1 multi-node, USER DECISION option b): the controller
// commits a TRANSIENT partition's data lineage per leadership.  A leader's
// report is answered kAlterIsrLineageConfirmed only once the record is
// COMMITTED (re-sends until then); the lineage is MINTED for an unborn ring,
// KEPT across a handover whose new leader fetched from the confirmed
// leadership, MINTED again after a full restart; a report of a superseded
// leadership is fenced.  RED: the report was never committed / answered.
// ===========================================================================
namespace {
rune::replication::AlterIsrMsg lineageReport(const PartitionAssignment& a, int32_t src) {
    rune::replication::AlterIsrMsg m{};
    m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
    m.isrCount = rune::replication::AlterIsrMsg::kLeaderEpochBump;
    m.partitionEpoch = a.partitionEpoch;
    m.isr[1] = rune::replication::AlterIsrMsg::kLineageReport;
    m.isr[2] = src;
    return m;
}
int16_t answer(MetadataService& svc, const rune::replication::AlterIsrMsg& m) {
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    (void)svc.onAlterIsr(m, &resp, &answered);
    return answered ? resp.errorCode : int16_t{-1};
}
}  // namespace

TEST(TransientLineageControllerRegression, LineageIsMintedKeptAndReMintedPerLeadershipRegression) {
    CtlRig c;
    const auto first = c.pa();
    // Unborn ring (first leadership): not answered until committed, then confirmed.
    EXPECT_EQ(answer(*c.svc, lineageReport(first, -1)), -1) << "confirmed before the record committed";
    c.drive(50);
    EXPECT_EQ(answer(*c.svc, lineageReport(first, -1)), rune::replication::kAlterIsrLineageConfirmed);
    const auto* l1 = c.svc->transientLineage(0, 0);
    ASSERT_NE(l1, nullptr) << "no lineage committed";
    const uint64_t token1 = l1->lineage;
    EXPECT_NE(token1, 0u);
    EXPECT_EQ(l1->mintEpoch, first.leaderEpoch);
    EXPECT_EQ(l1->confirmedEpoch, first.leaderEpoch);
    EXPECT_EQ(l1->confirmedLeader, first.leaderNode);
    EXPECT_EQ(c.svc->lineagesMintedForTest(), 1u);

    // Handover (the old leader asks for a new epoch; another ISR member leads):
    // the new leader fetched from the confirmed leadership -> KEPT.
    (void)c.svc->onAlterIsr(c.bump());
    c.tick();
    const auto second = c.pa();
    ASSERT_NE(second.leaderNode, first.leaderNode);
    EXPECT_EQ(answer(*c.svc, lineageReport(first, -1)), rune::replication::kAlterIsrFencedLeaderEpoch)
        << "a superseded leadership's report was not fenced";
    (void)answer(*c.svc, lineageReport(second, first.leaderEpoch));
    c.drive(50);
    EXPECT_EQ(answer(*c.svc, lineageReport(second, first.leaderEpoch)), rune::replication::kAlterIsrLineageConfirmed);
    const auto* l2 = c.svc->transientLineage(0, 0);
    ASSERT_NE(l2, nullptr);
    EXPECT_EQ(l2->lineage, token1) << "a handover that kept the data minted a new lineage (false 'none')";
    EXPECT_EQ(l2->confirmedEpoch, second.leaderEpoch);
    EXPECT_EQ(l2->mintEpoch, first.leaderEpoch);
    EXPECT_EQ(c.svc->lineagesKeptForTest(), 1u);

    // Full restart (every ring unborn): the next leadership re-mints.
    (void)c.svc->onAlterIsr(c.bump());
    c.tick();
    const auto third = c.pa();
    (void)answer(*c.svc, lineageReport(third, -1));
    c.drive(50);
    EXPECT_EQ(answer(*c.svc, lineageReport(third, -1)), rune::replication::kAlterIsrLineageConfirmed);
    const auto* l3 = c.svc->transientLineage(0, 0);
    ASSERT_NE(l3, nullptr);
    EXPECT_NE(l3->lineage, token1) << "a leadership with an unborn ring kept the old lineage (offsets resumed into new data)";
    EXPECT_EQ(l3->mintEpoch, third.leaderEpoch);
    EXPECT_EQ(c.svc->lineagesMintedForTest(), 2u);
}

// E2 final fix wave, residual round 3 — N1 (Critical): the coordinator trusts a
// committed lineage only while the confirmed leader's LIVE process identity
// (its node-control registration nonce) is the one committed with it.  A full
// restart keeps the committed (leader, epoch) until its bump commits; the
// leader's NEW process (or no registration yet) must be "untrusted" — never the
// old lineage.  RED (identity not checked): the old lineage was trusted.
namespace {
void registerNode(MetadataService& svc, int32_t node, int64_t nonce) {
    rune::replication::AlterIsrMsg m{};
    m.topicId  = rune::replication::AlterIsrMsg::kNodeControlTopicId;
    m.leaderId = node;
    m.isrCount = rune::replication::AlterIsrMsg::kLeaderEpochBump;
    m.isr[1]   = rune::replication::AlterIsrMsg::kNodeRegister;
    m.isr[3]   = static_cast<int32_t>(static_cast<uint64_t>(nonce) & 0xffffffffu);
    m.isr[4]   = static_cast<int32_t>(static_cast<uint64_t>(nonce) >> 32);
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    (void)svc.onAlterIsr(m, &resp, &answered);
}
}  // namespace

TEST(TransientLineageControllerRegression, LineageIsTrustedOnlyWhileItsLeadersProcessLivesRegression) {
    CtlRig c;
    const auto pa = c.pa();
    const int64_t nonce1 = 0x1111'2222'3333'4444LL;
    auto rep = lineageReport(pa, -1);
    rep.isr[3] = static_cast<int32_t>(static_cast<uint64_t>(nonce1) & 0xffffffffu);
    rep.isr[4] = static_cast<int32_t>(static_cast<uint64_t>(nonce1) >> 32);
    (void)answer(*c.svc, rep);
    c.drive(50);
    ASSERT_EQ(answer(*c.svc, rep), rune::replication::kAlterIsrLineageConfirmed);
    const uint64_t token = c.svc->transientLineage(0, 0)->lineage;
    EXPECT_EQ(c.svc->transientLineage(0, 0)->incarnation, nonce1) << "the record must carry the leader's process";

    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), rune::metadata::kLineageUntrusted)
        << "trusted before the leader's process registered (a restarted controller knows nobody yet)";
    registerNode(*c.svc, pa.leaderNode, nonce1);
    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), token) << "the live, confirming process is not trusted";

    // The leader's process restarted (a new nonce); the committed assignment is
    // unchanged — the stale (L, E) a full restart leaves until the bump commits.
    registerNode(*c.svc, pa.leaderNode, 0x5555'6666'7777'8888LL);
    EXPECT_EQ(c.pa().leaderEpoch, pa.leaderEpoch) << "precondition: the leadership has not moved";
    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), rune::metadata::kLineageUntrusted)
        << "SILENT LOSS: the dead process's lineage was trusted for the stale committed leadership";
}

// E2 final fix wave, residual round 3 — N3: leadership passing to a replica
// that was NOT in the ISR (an unclean / survivor election) may lack committed
// records even though it fetched from the confirmed leadership (it lagged out
// of the ISR).  Such a leadership MINTS a new lineage — committed offsets are
// answered "none", never resumed past data the new leader does not hold.
// RED (the clean-election bit ignored): the survivor KEPT the lineage.
TEST(TransientLineageControllerRegression, UncleanElectionToANonIsrReplicaMintsANewLineageRegression) {
    CtlRig c;
    c.svc->setAllowSurvivorElection(true);
    const auto first = c.pa();
    (void)answer(*c.svc, lineageReport(first, -1));
    c.drive(50);
    ASSERT_EQ(answer(*c.svc, lineageReport(first, -1)), rune::replication::kAlterIsrLineageConfirmed);
    const uint64_t token = c.svc->transientLineage(0, 0)->lineage;
    EXPECT_TRUE(c.pa().cleanElection);

    // The followers lag out of the ISR; the sole-ISR memory leader restarts
    // empty -> leaderless -> a survivor (non-ISR) election.
    c.soleIsr();
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    const int32_t F = L == 1 ? 2 : 1;
    const int32_t G = 6 - L - F;
    c.svc->reportReplicaPosition(0, 0, G, le, 10);
    auto bump = c.bump(le, /*memory=*/true);
    bump.isr[3] = 2;
    (void)c.svc->onAlterIsr(bump);
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    const int32_t lle = c.pa().leaderEpoch;
    (void)c.svc->onAlterIsr(leaderlessReport(L, lle, 0, 2));
    (void)c.svc->onAlterIsr(leaderlessReport(F, lle, 0, 7));
    (void)c.svc->onAlterIsr(leaderlessReport(G, lle, 10, 1));
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, G) << "precondition: the survivor (non-ISR) replica leads";
    EXPECT_FALSE(c.pa().cleanElection) << "a non-ISR leader was derived as a clean election";

    // G fetched from the confirmed leadership (src = le): a clean handover
    // would KEEP; an unclean one must MINT.
    const auto unclean = c.pa();
    (void)answer(*c.svc, lineageReport(unclean, le));
    c.drive(50);
    ASSERT_EQ(answer(*c.svc, lineageReport(unclean, le)), rune::replication::kAlterIsrLineageConfirmed);
    const auto* l2 = c.svc->transientLineage(0, 0);
    ASSERT_NE(l2, nullptr);
    EXPECT_NE(l2->lineage, token)
        << "SILENT LOSS: an unclean election kept the lineage (offsets resume past data the leader lacks)";
    EXPECT_EQ(l2->mintEpoch, unclean.leaderEpoch);
}

// E2 final fix wave, residual round 5 — R1 (controller half): when the failure
// detector finds a node dead (it leaves the heartbeat-eligible set), the
// remembered process identity of that node no longer vouches for a lineage —
// cleared for trust at once, even if the committed leadership has not moved
// yet; a NEW process registering restores trust only for ITS lineages.  RED
// (identity not cleared on death): the dead process's lineage stayed trusted.
TEST(TransientLineageControllerRegression, DeathDetectionClearsTheRememberedLeaderIdentityRegression) {
    CtlRig c;
    // The partition leader must not be the controller (node 1) itself.
    for (int i = 0; i < 4 && c.pa().leaderNode == 1; ++i) { (void)c.svc->onAlterIsr(c.bump()); c.tick(); }
    ASSERT_NE(c.pa().leaderNode, 1) << "precondition: a partition leader other than the controller";
    c.svc->setUserDecisionHoldForTest(INT64_MAX / 4);   // keep the committed leadership: isolate the identity rule
    const auto pa = c.pa();
    const int64_t nonce1 = 0x1111'2222'3333'4444LL;
    auto rep = lineageReport(pa, -1);
    rep.isr[3] = static_cast<int32_t>(static_cast<uint64_t>(nonce1) & 0xffffffffu);
    rep.isr[4] = static_cast<int32_t>(static_cast<uint64_t>(nonce1) >> 32);
    (void)answer(*c.svc, rep);
    c.drive(50);
    ASSERT_EQ(answer(*c.svc, rep), rune::replication::kAlterIsrLineageConfirmed);
    registerNode(*c.svc, pa.leaderNode, nonce1);
    const uint64_t token = c.svc->transientLineage(0, 0)->lineage;
    ASSERT_EQ(c.svc->trustedTransientLineage(0, 0), token) << "precondition: the live leader's lineage is trusted";

    // The failure detector: the leader is out of the eligible set.
    std::vector<int32_t> live;
    for (int32_t n : {1, 2, 3}) if (n != pa.leaderNode) live.push_back(n);
    (void)c.svc->reassignDeadPartitionLeaders(live, 0);
    c.drive(5);
    EXPECT_EQ(c.pa().leaderNode, pa.leaderNode) << "precondition: the committed leadership has not moved (held)";
    EXPECT_EQ(c.svc->knownProcessIdentity(pa.leaderNode), 0) << "the dead node's identity is still remembered";
    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), rune::metadata::kLineageUntrusted)
        << "SILENT LOSS: a lineage was vouched for by a process the failure detector found dead";
    EXPECT_GE(c.svc->lineageIdentitiesRevokedForTest(), 1u);

    // Back in the eligible set (heartbeats only): still untrusted — liveness
    // is not proof that the SAME process came back.
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 0);
    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), rune::metadata::kLineageUntrusted)
        << "heartbeats alone restored trust (a restarted process heartbeats before it registers)";
    // A report from the live, confirming process for the current leadership restores it.
    (void)answer(*c.svc, rep);
    EXPECT_EQ(c.svc->trustedTransientLineage(0, 0), token) << "the live process's own report did not restore trust";
}

// ---------------------------------------------------------------------------
// Catalog history Task 8: placement removal of a DROPPED topic is never
// stranded.  The controller drops a topic whose static assignment is appended
// but NOT committed yet (no follower fetched): there is nothing committed to
// remove at the drop — yet once the assignment commits, the controller still
// removes it (RemoveTopicPlacementRecord), and every broker forgets nothing it
// may still need.  A follower appends nothing.  A second removal is a no-op.
// RED: the dropped topic forgotten at the drop (nothing committed then) — its
// assignment commits afterwards and stays placed forever.
// ---------------------------------------------------------------------------
TEST(MetadataRemoveTopicRegression, DroppedTopicWhoseAssignmentCommitsLaterIsStillRemovedRegression) {
    SaeTempDir dir1("rm1"), dir2("rm2"), dir3("rm3");
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode1), Role::Follower, 0, allNodes, allNodes, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode2), Role::Follower, 0, allNodes, allNodes, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(kNode3), Role::Follower, 0, allNodes, allNodes, &log3, &transport);
    transport.setLeader(r1.get());
    transport.addFollower(kNode2, r2.get());
    transport.addFollower(kNode3, r3.get());
    r1->becomeLeader(1, allNodes);

    std::vector<TopicSpec> topics = {TopicSpec{0, 2, 3, "", "t"}};
    MetadataService svc1(r1.get(), kNode1);
    svc1.configure(topics, allNodes);
    MetadataService svc2(r2.get(), kNode2);   // a follower broker's service
    svc2.configure(topics, allNodes);
    int64_t clock = 1'000'000LL;
    for (int i = 0; i < 5; ++i) { clock += 1'000'000LL; (void)svc1.doWork(clock); }
    const auto leoAppended = r1->logEndOffset();
    ASSERT_GT(leoAppended, 0) << "the static assignment is appended";
    ASSERT_EQ(svc1.assignment(0, 0), nullptr) << "non-vacuous: not committed yet (no follower fetched)";

    // The catalog drops topic 0 on every broker.
    svc1.removeRuntimeTopic(0);
    svc2.removeRuntimeTopic(0);
    EXPECT_EQ(r1->logEndOffset(), leoAppended) << "nothing committed to remove yet";
    EXPECT_EQ(r2->logEndOffset(), 0) << "a follower never appends";

    // The followers fetch: the assignment commits, then the controller removes it.
    r2->becomeFollower(1, kNode1);
    r3->becomeFollower(1, kNode1);
    bool placed = false, removed = false;
    for (int i = 0; i < kDriveCap && !removed; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        (void)svc1.doWork(clock);
        (void)svc2.doWork(clock);
        placed  = placed || svc1.assignment(0, 0) != nullptr;
        removed = r1->logEndOffset() > leoAppended && svc1.assignment(0, 0) == nullptr &&
                  svc1.assignment(0, 1) == nullptr && svc2.assignment(0, 0) == nullptr &&
                  svc2.assignment(0, 1) == nullptr && svc1.removalsInFlightForTest() == 0;
    }
    ASSERT_TRUE(removed) << "stranded: placed=" << placed << " leo=" << r1->logEndOffset()
                         << " hw=" << r1->highWatermark();

    // Idempotent: the same drop again appends nothing more.
    const auto leoAfter = r1->logEndOffset();
    svc1.removeRuntimeTopic(0);
    for (int i = 0; i < 20; ++i) {
        clock += 1'000'000LL;
        r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
        transport.pump(clock);
        (void)svc1.doWork(clock);
    }
    EXPECT_EQ(r1->logEndOffset(), leoAfter) << "a second removal appended a record";
    EXPECT_EQ(svc1.assignment(0, 0), nullptr);
}

// ===========================================================================
// Catalog history Task 11 fix round 0 — catalog-halted nodes (kNodeCatalogHalted
// on the node-control register, sent to every member).
// ===========================================================================
namespace {
rune::replication::AlterIsrMsg haltedRegister(int32_t node, int64_t inc) {
    auto m = nodeControl(node, inc, /*shutdown=*/false);
    m.isr[1] |= rune::replication::AlterIsrMsg::kNodeCatalogHalted;
    return m;
}
} // namespace

// Ruling 2: the controller never places a NEW partition on a halted node; rf 0
// ("every broker") means every PLACEABLE broker.  A new process of the node
// clears the mark.  RED: the halted node is in the new topic's replicas.
TEST(CatalogHaltedPlacementRegression, ControllerPlacesNoNewPartitionOnAHaltedNodeRegression) {
    CtlRig c;
    (void)c.svc->onAlterIsr(haltedRegister(3, 33));
    ASSERT_TRUE(c.svc->catalogHalted(3));
    EXPECT_FALSE(c.svc->catalogHalted(2));
    EXPECT_EQ(c.svc->placementNodes(), (std::vector<int32_t>{1, 2}));
    c.svc->addRuntimeTopic(TopicSpec{5, 2, 0, "", "fresh"});   // rf 0 = every placeable broker
    c.drive(100);
    for (int32_t p = 0; p < 2; ++p) {
        const auto* pa = c.svc->assignment(5, p);
        ASSERT_NE(pa, nullptr) << p;
        EXPECT_EQ(pa->replicas.size(), 2u);
        EXPECT_EQ(std::count(pa->replicas.begin(), pa->replicas.end(), 3), 0) << "placed on the halted node";
        EXPECT_NE(pa->leaderNode, 3);
    }
    // An explicit rf above the placeable brokers: the explicit Unplaceable state.
    c.svc->addRuntimeTopic(TopicSpec{6, 1, 3, "", "rf3"});
    c.drive(50);
    EXPECT_EQ(c.svc->assignment(6, 0), nullptr);
    EXPECT_EQ(c.svc->unplaceableTopicCount(), 1u);
    EXPECT_EQ(c.svc->unplaceableEvents(), 1u) << "logged once, not per retry";
    // A new process of node 3 (another nonce) is placeable again — and topic 6 placed.
    (void)c.svc->onAlterIsr(nodeControl(3, 34, false));
    EXPECT_FALSE(c.svc->catalogHalted(3));
    EXPECT_EQ(c.svc->placementNodes(), (std::vector<int32_t>{1, 2, 3}));
    c.drive(50);
    EXPECT_NE(c.svc->assignment(6, 0), nullptr);
    EXPECT_EQ(c.svc->unplaceableTopicCount(), 0u);
}

// Ruling 1: a halted CONTROLLER keeps the data plane but places and unplaces no
// catalog topic; a halted controller hands over only to a live healthy peer.
// RED: the halted controller placed / removed from its stale catalog.
TEST(CatalogHaltedPlacementRegression, HaltedControllerPlacesAndUnplacesNoCatalogTopicRegression) {
    CtlRig c;
    c.svc->addRuntimeTopic(TopicSpec{5, 1, 0, "", "before"});
    c.drive(100);
    ASSERT_NE(c.svc->assignment(5, 0), nullptr);
    const std::vector<int32_t> live{1, 2, 3};
    EXPECT_TRUE(c.svc->healthyPeerAmong(live));
    (void)c.svc->onAlterIsr(haltedRegister(1, 11));   // the controller (node 1) itself halted
    EXPECT_TRUE(c.svc->healthyPeerAmong(live)) << "2 and 3 are healthy: hand over";
    c.svc->addRuntimeTopic(TopicSpec{6, 1, 0, "", "after"});
    c.svc->removeRuntimeTopic(5);
    c.drive(100);
    EXPECT_EQ(c.svc->assignment(6, 0), nullptr) << "no placement from a halted catalog";
    EXPECT_NE(c.svc->assignment(5, 0), nullptr) << "no unplacement from a halted catalog";
    EXPECT_GE(c.svc->catalogPlacementsSkippedHalted(), 2u);
    (void)c.svc->onAlterIsr(haltedRegister(2, 22));
    (void)c.svc->onAlterIsr(haltedRegister(3, 33));
    EXPECT_FALSE(c.svc->healthyPeerAmong(live)) << "every broker halted: stay the controller";
    EXPECT_FALSE(c.svc->healthyPeerAmong(std::vector<int32_t>{1})) << "no live peer";
    // (The data-plane failover under an all-halted cluster is pinned on real
    // brokers: CatalogLeadershipBrokerRegression.AllBrokersHalted….)
}

// Task 16 fix round 1 (R3): the boot feeds every catalog topic through configure(); the STATIC emit path of
// a halted controller must place nothing either.  RED (guard removed): topic 6 is placed from the stale catalog.
TEST(CatalogHaltedPlacementRegression, HaltedControllerEmitsNoStaticAssignmentForConfiguredCatalogTopicsRegression) {
    CtlRig c;
    (void)c.svc->onAlterIsr(haltedRegister(1, 11));   // the controller (node 1) itself halted
    c.svc->configure({TopicSpec{6, 1, 0, "", "late"}}, c.all);
    c.svc->emitStaticAssignmentIfNeeded();
    c.drive(100);
    EXPECT_EQ(c.svc->assignment(6, 0), nullptr) << "no placement from a halted catalog";
    EXPECT_GE(c.svc->catalogPlacementsSkippedHalted(), 1u);
}

// Review I1 (fix round 1): placement is per unit.  A SUCCESSOR controller catches
// up with two pending topics — one unplaceable (explicit rf 3 with broker 3
// halted), one placeable — and an already-placed explicit-rf-3 topic.  The
// placeable one is placed; the unplaceable one is held explicitly (reason,
// count) and placed once broker 3 restarts healthy.  RED (one batch over every
// configured topic): the placed rf-3 topic throws for the batch and NOTHING is
// placed, silently.
TEST(CatalogHaltedPlacementRegression, OneUnplaceableTopicNeverBlocksAnotherOnASuccessorControllerRegression) {
    CtlRig c;                                    // topic 0 (rf 3) placed by controller 1
    ASSERT_NE(c.svc->assignment(0, 0), nullptr);
    MetadataService svc2(c.r2.get(), 2);         // broker 2: a follower today
    svc2.configure(c.topics, c.all);
    auto drive2 = [&](int n) {
        for (int i = 0; i < n; ++i) {
            c.clock += 1'000'000LL;
            c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
            c.transport.pump(c.clock);
            (void)svc2.doWork(c.clock);
        }
    };
    drive2(50);
    ASSERT_NE(svc2.assignment(0, 0), nullptr) << "the follower applied the committed map";
    (void)svc2.onAlterIsr(haltedRegister(3, 33));                // broker 3 halted
    svc2.addRuntimeTopic(TopicSpec{7, 1, 3, "", "rf3"});           // applied while not the controller
    svc2.addRuntimeTopic(TopicSpec{8, 1, 0, "", "fine"});
    EXPECT_EQ(svc2.assignment(7, 0), nullptr);
    EXPECT_EQ(svc2.assignment(8, 0), nullptr) << "a follower places nothing";

    // Broker 2 becomes the controller (a new epoch).
    c.r1->becomeFollower(2, 2);
    c.r2->becomeLeader(2, c.all);
    c.r3->becomeFollower(2, 2);
    c.transport.setLeader(c.r2.get());
    c.transport.removeFollower(2);
    c.transport.addFollower(1, c.r1.get());
    drive2(200);
    ASSERT_TRUE(svc2.controllerCaughtUpForTest());
    ASSERT_NE(svc2.assignment(8, 0), nullptr) << "the placeable topic is placed — never blocked";
    EXPECT_EQ(std::count(svc2.assignment(8, 0)->replicas.begin(), svc2.assignment(8, 0)->replicas.end(), 3), 0);
    EXPECT_EQ(svc2.assignment(7, 0), nullptr) << "rf 3 cannot be placed on 2 placeable brokers";
    EXPECT_EQ(svc2.unplaceableTopicCount(), 1u);
    EXPECT_NE(svc2.unplaceableReason(7).find("catalog apply halted on broker(s) 3"), std::string::npos)
        << svc2.unplaceableReason(7);
    EXPECT_NE(svc2.assignment(0, 0), nullptr) << "the already-placed rf-3 topic is untouched";
    // Retried on the input change: broker 3 restarts healthy (a new process).
    (void)svc2.onAlterIsr(nodeControl(3, 34, false));
    drive2(100);
    ASSERT_NE(svc2.assignment(7, 0), nullptr) << "placed once broker 3 is placeable again";
    EXPECT_EQ(svc2.unplaceableTopicCount(), 0u);
}

// ===========================================================================
// Task 11 fix round 4 — a replica that cannot serve a partition (a LIVE,
// periodic kBumpReplicaUnservable report) is excluded from that partition's
// leader ELECTION only: no replica-set or ISR edit, no metadata record.
// ===========================================================================
namespace {
rune::replication::AlterIsrMsg unservable(int32_t topicId, int32_t partition, int32_t node) {
    using rune::replication::AlterIsrMsg;
    AlterIsrMsg m{};
    m.topicId = topicId; m.partition = partition; m.leaderId = node;
    m.isrCount = AlterIsrMsg::kLeaderEpochBump;
    m.isr[1] = AlterIsrMsg::kBumpReplicaUnservable;
    return m;
}
bool has(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }
} // namespace

// A follower's report writes NOTHING to the metadata log and changes nothing in
// the assignment.  RED (rounds 2–3): the report removed it / committed a mark.
TEST(CatalogUnservableRegression, AReportWritesZeroMetadataRecordsRegression) {
    CtlRig c;
    const auto before = c.pa();
    const int32_t F = before.replicas[0] == before.leaderNode ? before.replicas[1] : before.replicas[0];
    const auto leo = c.r1->logEndOffset();
    for (int i = 0; i < 10; ++i) { (void)c.svc->onAlterIsr(unservable(0, 0, F)); c.tick(); }
    EXPECT_TRUE(c.svc->reportedUnservable(0, 0, F));
    EXPECT_EQ(c.r1->logEndOffset(), leo) << "no metadata record";
    EXPECT_EQ(c.pa().replicas, before.replicas);
    EXPECT_EQ(c.pa().isr, before.isr);
    EXPECT_EQ(c.pa().partitionEpoch, before.partitionEpoch);
    EXPECT_EQ(c.svc->blockedPartitionCount(), 0u);
}

// An unservable LEADER fails over to a live in-sync member — and stays in the
// ISR and the replicas (it may hold the data; only lag rules ever remove it).
TEST(CatalogUnservableRegression, UnservableLeaderFailsOverWithoutAnIsrEditRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const auto replicas = c.pa().replicas;
    (void)c.svc->onAlterIsr(unservable(0, 0, L));
    c.tick();
    EXPECT_NE(c.pa().leaderNode, L) << "never its leader while it reports";
    EXPECT_TRUE(has(c.pa().isr, L)) << "the report never removes it from the ISR";
    EXPECT_EQ(c.pa().replicas, replicas);
    for (int i = 0; i < 5; ++i) { (void)c.svc->onAlterIsr(unservable(0, 0, L)); c.tick(); }
    EXPECT_NE(c.pa().leaderNode, L) << "never re-elected (preferred included) while it reports";
}

// The sole in-sync member cannot serve: the partition is BLOCKED (explicit,
// counted), no other replica is elected, and once it is leaderless it is never
// re-elected while it reports (an empty replica never leads it).  It recovers
// when its reports stop (it serves again).  RED: re-elected at once (the
// holder came back empty and led from offset 0).
TEST(CatalogUnservableRegression, SoleUnservableIsrMemberStaysBlockedAndIsNeverReElectedEmptyRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t L = c.pa().leaderNode;
    std::vector<NodeId> others;
    for (int32_t n : {1, 2, 3}) if (n != L) others.push_back(n);
    c.tick(others);                                   // L dies: leaderless, the ISR lineage kept
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    ASSERT_EQ(c.pa().isr, std::vector<int32_t>{L});
    const auto leo = c.r1->logEndOffset();
    for (int i = 0; i < 5; ++i) {                    // L is back, live — but cannot serve it
        (void)c.svc->onAlterIsr(unservable(0, 0, L));
        c.tick({1, 2, 3});
    }
    EXPECT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER) << "never re-elected while it cannot serve";
    EXPECT_EQ(c.pa().isr, std::vector<int32_t>{L}) << "the lineage is kept";
    EXPECT_TRUE(c.svc->blocked(0, 0));
    EXPECT_EQ(c.svc->blockedPartitionCount(), 1u);
    EXPECT_EQ(c.svc->unplaceableTopicCount(), 0u) << "review M5: blocked partitions have their own gauge";
    EXPECT_EQ(c.r1->logEndOffset(), leo) << "blocked: no record";
    // It serves again: the reports stop; past the TTL the block clears and it leads.
    c.clock += MetadataService::kUnservableReportTtlNs + 1'000'000;
    c.drive(5);
    EXPECT_FALSE(c.svc->reportedUnservable(0, 0, L));
    c.tick({1, 2, 3});
    EXPECT_EQ(c.pa().leaderNode, L) << "recovers when the holder serves again";
    EXPECT_FALSE(c.svc->blocked(0, 0));
}

// After a controller handover the exclusions are REBUILT from the next live
// reports (nothing was committed, nothing goes stale).
TEST(CatalogUnservableRegression, ExclusionsAreRebuiltFromReportsAfterAControllerHandoverRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    (void)c.svc->onAlterIsr(unservable(0, 0, 3));
    EXPECT_TRUE(c.svc->reportedUnservable(0, 0, 3));
    MetadataService svc2(c.r2.get(), 2);
    svc2.configure(c.topics, c.all);
    auto drive2 = [&](int n) {
        for (int i = 0; i < n; ++i) {
            c.clock += 1'000'000LL;
            c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
            c.transport.pump(c.clock);
            (void)svc2.doWork(c.clock);
        }
    };
    drive2(50);
    c.r1->becomeFollower(2, 2);
    c.r2->becomeLeader(2, c.all);
    c.r3->becomeFollower(2, 2);
    c.transport.setLeader(c.r2.get());
    c.transport.removeFollower(2);
    c.transport.addFollower(1, c.r1.get());
    drive2(200);
    ASSERT_TRUE(svc2.controllerCaughtUpForTest());
    EXPECT_EQ(svc2.unservableReportCount(), 0u) << "a successor starts with nothing stale";
    (void)svc2.onAlterIsr(unservable(0, 0, 3));          // the next live report
    EXPECT_TRUE(svc2.reportedUnservable(0, 0, 3)) << "rebuilt from the report";
    (void)L;
    for (int i = 0; i < 5; ++i) {
        (void)svc2.reassignDeadPartitionLeaders({1, 2, 3}, 1024);
        drive2(50);
        (void)svc2.onAlterIsr(unservable(0, 0, 3));
        EXPECT_NE(svc2.assignment(0, 0)->leaderNode, 3) << "the rebuilt exclusion holds";
    }
}

// ===========================================================================
// Task 11 fix round 5
// ===========================================================================
namespace {
/// A successor MetadataService on broker 2 that takes the controllership from 1.
struct Successor {
    CtlRig& c;
    MetadataService svc2;
    explicit Successor(CtlRig& rig) : c(rig), svc2(rig.r2.get(), 2) { svc2.configure(c.topics, c.all); }
    void drive(int n) {
        for (int i = 0; i < n; ++i) {
            c.clock += 1'000'000LL;
            c.r1->doWork(c.clock); c.r2->doWork(c.clock); c.r3->doWork(c.clock);
            c.transport.pump(c.clock);
            (void)svc2.doWork(c.clock);
        }
    }
    void takeOver() {
        c.r1->becomeFollower(2, 2);
        c.r2->becomeLeader(2, c.all);
        c.r3->becomeFollower(2, 2);
        c.transport.setLeader(c.r2.get());
        c.transport.removeFollower(2);
        c.transport.addFollower(1, c.r1.get());
        drive(200);
    }
};
} // namespace

// Review I1: the "cannot serve" reports are BROADCAST to every metadata member,
// each keeping its TTL'd copy — a successor already holds h's report at its
// FIRST decision.  Partition ISR {L, h}; L dies right at the handover.  RED
// (reports kept by the controller only): the successor's first tick elects h —
// the sole live in-sync member, without its data.
TEST(CatalogUnservableRegression, SuccessorHoldsTheBroadcastReportAndNeverElectsTheUnservableBrokerRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t h = L == 3 ? 1 : 3;
    ASSERT_TRUE(proposeIsr(c, {L, h}));
    ASSERT_EQ(c.pa().isr.size(), 2u);
    Successor s(c);
    s.drive(50);
    (void)s.svc2.onAlterIsr(unservable(0, 0, h));   // the broadcast copy, as a FOLLOWER member
    EXPECT_TRUE(s.svc2.reportedUnservable(0, 0, h)) << "every member records it";
    s.takeOver();
    ASSERT_TRUE(s.svc2.controllerCaughtUpForTest());
    std::vector<NodeId> live;
    for (int32_t n : {1, 2, 3}) if (n != L) live.push_back(n);
    for (int i = 0; i < 5; ++i) {                     // the successor's first ticks
        (void)s.svc2.reassignDeadPartitionLeaders(live, 1024);
        s.drive(20);
        (void)s.svc2.onAlterIsr(unservable(0, 0, h));
        EXPECT_NE(s.svc2.assignment(0, 0)->leaderNode, h) << "tick " << i;
    }
    EXPECT_EQ(s.svc2.blockedPartitionCount(), 1u) << "no live in-sync replica can serve it";
}

// Review I1 (second rule): a controller PROCESS younger than one report TTL may
// lack a report — it decides no user-partition leadership until it has held
// them a full TTL.  RED (no warm-up): its first tick elects h.
TEST(CatalogUnservableRegression, YoungControllerDefersUserElectionsUntilItHeldTheReportsRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    const int32_t h = L == 3 ? 1 : 3;
    ASSERT_TRUE(proposeIsr(c, {L, h}));
    Successor s(c);
    s.svc2.setUnservableReportWarmup(MetadataService::kUnservableReportTtlNs);
    s.takeOver();                                       // a fresh process: no report yet
    ASSERT_TRUE(s.svc2.controllerCaughtUpForTest());
    ASSERT_TRUE(s.svc2.unservableWarmingUp());
    std::vector<NodeId> live;
    for (int32_t n : {1, 2, 3}) if (n != L) live.push_back(n);
    (void)s.svc2.reassignDeadPartitionLeaders(live, 1024);
    s.drive(20);
    EXPECT_NE(s.svc2.assignment(0, 0)->leaderNode, h) << "no election while warming up";
    (void)s.svc2.onAlterIsr(unservable(0, 0, h));     // the next period's report arrives
    c.clock += MetadataService::kUnservableReportTtlNs;
    s.drive(1);
    (void)s.svc2.onAlterIsr(unservable(0, 0, h));     // and keeps coming (live)
    ASSERT_FALSE(s.svc2.unservableWarmingUp());
    (void)s.svc2.reassignDeadPartitionLeaders(live, 1024);
    s.drive(20);
    EXPECT_NE(s.svc2.assignment(0, 0)->leaderNode, h) << "the held report excludes it";
}

// Review I2: a TRANSIENT partition whose sole-ISR leader restarts EMPTY is
// decided on the fresh reports of its live replicas — never waiting for (nor
// electing) a replica that reports it cannot serve (it has no PartitionReplica:
// it never sends a follower report).  RED: held forever, waiting for h.
TEST(CatalogUnservableRegression, LeaderlessDecisionNeverWaitsForAnUnservableReplicaRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t L = c.pa().leaderNode, le = c.pa().leaderEpoch;
    const int32_t F = L == 1 ? 2 : 1;
    const int32_t h = 6 - L - F;
    auto bump = c.bump(le, /*memory=*/true);
    bump.isr[3] = 2;
    (void)c.svc->onAlterIsr(bump);
    c.tick();
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    const int32_t lle = c.pa().leaderEpoch;
    (void)c.svc->onAlterIsr(unservable(0, 0, h));    // h cannot serve: it never sends a follower report
    (void)c.svc->onAlterIsr(leaderlessReport(L, lle, 0, 2));
    (void)c.svc->onAlterIsr(leaderlessReport(F, lle, 0, 7));
    c.tick();
    EXPECT_NE(c.pa().leaderNode, rune::metadata::NO_LEADER) << "decided without h's report";
    EXPECT_NE(c.pa().leaderNode, h) << "h never leads";
    EXPECT_EQ(c.pa().leaderNode, L) << "the clean in-sync candidate";
}

// Review M4 (round 5): a report of an OLDER process of the node is dropped, and
// a report about a partition the node is not a replica of is ignored.
TEST(CatalogUnservableRegression, StaleProcessAndNonReplicaReportsAreIgnoredRegression) {
    CtlRig c;
    c.svc->addRuntimeTopic(TopicSpec{5, 1, 2, "", "rf2"});
    c.drive(100);
    const auto* pa = c.svc->assignment(5, 0);
    ASSERT_NE(pa, nullptr);
    int32_t outsider = -1;
    for (int32_t n : {1, 2, 3}) if (!has(pa->replicas, n)) outsider = n;
    ASSERT_GT(outsider, 0);
    (void)c.svc->onAlterIsr(unservable(5, 0, outsider));
    EXPECT_FALSE(c.svc->reportedUnservable(5, 0, outsider)) << "not a replica: ignored";
    const int32_t r = pa->replicas[0];
    (void)c.svc->onAlterIsr(nodeControl(r, 40, false));            // r's process 40 ...
    (void)c.svc->onAlterIsr(nodeControl(r, 41, false));            // ... superseded by 41
    auto stale = unservable(5, 0, r);
    stale.isr[3] = 40;                                              // an older process
    (void)c.svc->onAlterIsr(stale);
    EXPECT_FALSE(c.svc->reportedUnservable(5, 0, r)) << "an older process's late report";
    auto live = unservable(5, 0, r);
    live.isr[3] = 41;
    (void)c.svc->onAlterIsr(live);
    EXPECT_TRUE(c.svc->reportedUnservable(5, 0, r));
}

// Fix round 6 (r5 check, blocking): a RESTARTED broker's first report comes from
// its NEW process — possibly before its node-control register.  It is kept and
// excludes the broker; only a superseded (older) process's report is dropped.
// RED (round 5's `nonce != inc`): the new process's report was dropped and the
// restarted, unservable broker was electable again (I1 re-opened).
TEST(CatalogUnservableRegression, ARestartedBrokersFirstReportFromItsNewProcessIsKeptRegression) {
    CtlRig c;
    const int32_t L = c.pa().leaderNode;
    (void)c.svc->onAlterIsr(nodeControl(L, 41, false));   // L's process 41 is known
    auto fresh = unservable(0, 0, L);
    fresh.isr[3] = 42;                                     // its restart: process 42, not registered yet
    (void)c.svc->onAlterIsr(fresh);
    EXPECT_TRUE(c.svc->reportedUnservable(0, 0, L)) << "the newer process's report is kept";
    c.tick();
    EXPECT_NE(c.pa().leaderNode, L) << "and excludes it from the election";
    // Once process 42 registers, 41 is superseded: a late 41 report is dropped.
    (void)c.svc->onAlterIsr(nodeControl(L, 42, false));
    c.clock += MetadataService::kUnservableReportTtlNs + 1'000'000;
    c.drive(2);
    ASSERT_FALSE(c.svc->reportedUnservable(0, 0, L)) << "expired";
    auto old = unservable(0, 0, L);
    old.isr[3] = 41;
    (void)c.svc->onAlterIsr(old);
    EXPECT_FALSE(c.svc->reportedUnservable(0, 0, L)) << "a superseded process's late report";
}

// ===========================================================================
// F-B (2026-10-16): the controller claim is replica leadership + consensus leadership + not halted, at ONE
// replica epoch.  A consensus step-down with no known successor leaves the metadata replica Leader (the bridge
// acts only on a known leader); a same-node re-win is then a Leader->Leader epoch bump with no role edge.  Before
// the fix no claim-time action ran on that re-win, so every placement recorded in the gap was lost forever
// (a committed CREATE answered "in doubt" every 5 s, without end).
// ===========================================================================
namespace {
struct ReclaimRig {
    SaeTempDir d1{"fb1"}, d2{"fb2"}, d3{"fb3"};
    rune::storage::Log log1{d1.path}, log2{d2.path}, log3{d3.path};
    const std::vector<NodeId> all{1, 2, 3};
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> r1, r2, r3;
    std::unique_ptr<MetadataService> svc1;
    int64_t clock = 0;
    explicit ReclaimRig(Role r1Role = Role::Leader, Epoch e = 1) {
        log1.recover(); log2.recover(); log3.recover();
        r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), r1Role, e, all, all, &log1, &transport);
        r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, e, all, all, &log2, &transport);
        r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, e, all, all, &log3, &transport);
        transport.setLeader(r1.get());
        transport.addFollower(2, r2.get());
        transport.addFollower(3, r3.get());
        svc1 = std::make_unique<MetadataService>(r1.get(), 1);
    }
    void drive(int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1->doWork(clock);
        }
    }
    /// The same node wins consensus again: the bridge bumps the (still Leader) replica's epoch.
    void rewin(Epoch e) {
        svc1->setConsensusLeads(true);
        r1->becomeLeader(e, all);
        r2->becomeFollower(e, 1);
        r3->becomeFollower(e, 1);
    }
};
}   // namespace

TEST(ControllerReclaimRegression, RuntimeTopicAppliedInConsensusGapIsPlacedOnSameNodeReclaimRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    ASSERT_TRUE(g.svc1->controllerReady());
    g.svc1->setConsensusLeads(false);
    g.drive(5);
    ASSERT_FALSE(g.svc1->controllerReady());
    g.svc1->addRuntimeTopic(TopicSpec{0, 1, 3, "", "ready"});   // the committed CREATE applied in the gap
    g.drive(5);
    EXPECT_EQ(g.svc1->assignment(0, 0), nullptr) << "a deposed controller placed a topic";
    g.rewin(3);
    g.drive(200);
    EXPECT_NE(g.svc1->assignment(0, 0), nullptr) << "a committed CREATE applied in the consensus gap is never placed";
}

TEST(ControllerReclaimRegression, CoPartitionGroupAppliedInConsensusGapIsPlacedWholeRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    ASSERT_TRUE(g.svc1->controllerReady());
    g.svc1->setConsensusLeads(false);
    g.drive(5);
    g.svc1->addRuntimeTopic(TopicSpec{0, 3, 1, "grp", "k"});
    g.svc1->addRuntimeTopic(TopicSpec{1, 3, 1, "grp", "k"});
    g.drive(5);
    g.rewin(3);
    g.drive(200);
    for (int32_t p = 0; p < 3; ++p) {
        const auto* a = g.svc1->assignment(0, p);
        const auto* b = g.svc1->assignment(1, p);
        ASSERT_NE(a, nullptr) << "partition " << p << " of the first member was never placed";
        ASSERT_NE(b, nullptr) << "partition " << p << " of the second member was never placed";
        EXPECT_EQ(a->replicas, b->replicas) << "the co-partition unit was not placed whole at partition " << p;
        EXPECT_EQ(a->leaderNode, b->leaderNode);
    }
}

TEST(ControllerReclaimRegression, DropInConsensusGapRemovesPlacementOnSameNodeReclaimRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    g.svc1->addRuntimeTopic(TopicSpec{0, 1, 3, "", "t"});
    g.svc1->addRuntimeTopic(TopicSpec{1, 1, 3, "", "u"});
    g.drive(100);
    ASSERT_NE(g.svc1->assignment(0, 0), nullptr) << "precondition: placed while ready";
    ASSERT_NE(g.svc1->assignment(1, 0), nullptr) << "precondition: placed while ready";
    // A drop reconciled while ready: the reconcile latches the committed map version it last ran at, so a later
    // non-forced reconcile at an unchanged map does nothing - only the claim's forced reconcile can act.
    g.svc1->removeRuntimeTopic(1);
    g.drive(100);
    ASSERT_EQ(g.svc1->assignment(1, 0), nullptr) << "precondition: a drop while ready is removed";
    ASSERT_EQ(g.svc1->removalsInFlightForTest(), 0u);
    g.svc1->setConsensusLeads(false);
    g.drive(5);
    g.svc1->removeRuntimeTopic(0);   // the committed DROP applied in the gap
    g.drive(5);
    EXPECT_NE(g.svc1->assignment(0, 0), nullptr) << "a deposed controller removed a placement";
    g.rewin(3);
    g.drive(200);
    EXPECT_EQ(g.svc1->assignment(0, 0), nullptr) << "a DROP applied in the consensus gap never removes the placement";
    EXPECT_EQ(g.svc1->removalsInFlightForTest(), 0u);
}

TEST(ControllerReclaimRegression, CatchUpReachedWhileConsensusDeposedStillPlacesRegression) {
    SaeTempDir dir1("fbc1"), dir2("fbc2"), dir3("fbc3");
    const std::vector<NodeId> all = {1, 2, 3};
    {   // Incarnation 1: a committed log (topic 5 placed) so the restarted controller claims with its HW behind.
        rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
        log1.recover(); log2.recover(); log3.recover();
        InMemoryTransport transport;
        auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Leader, 1, all, all, &log1, &transport);
        auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 1, all, all, &log2, &transport);
        auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 1, all, all, &log3, &transport);
        transport.setLeader(r1.get());
        transport.addFollower(2, r2.get());
        transport.addFollower(3, r3.get());
        MetadataService svc(r1.get(), 1);
        svc.configure({TopicSpec{5, 1, 3, "", "old"}}, all);
        int64_t clock = 0;
        for (int i = 0; i < 100; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc.doWork(clock);
        }
        ASSERT_NE(svc.assignment(5, 0), nullptr);
    }
    rune::storage::Log log1(dir1.path), log2(dir2.path), log3(dir3.path);
    log1.recover(); log2.recover(); log3.recover();
    InMemoryTransport transport;
    auto r1 = std::make_unique<PartitionReplica>(makeMetaCfg(1), Role::Follower, 0, all, all, &log1, &transport);
    auto r2 = std::make_unique<PartitionReplica>(makeMetaCfg(2), Role::Follower, 0, all, all, &log2, &transport);
    auto r3 = std::make_unique<PartitionReplica>(makeMetaCfg(3), Role::Follower, 0, all, all, &log3, &transport);
    r1->becomeLeader(2, all);
    r2->becomeFollower(2, 1);
    r3->becomeFollower(2, 1);
    transport.setLeader(r1.get());
    transport.addFollower(2, r2.get());
    transport.addFollower(3, r3.get());
    MetadataService svc1(r1.get(), 1);
    svc1.configure({TopicSpec{5, 1, 3, "", "old"}, TopicSpec{0, 1, 3, "", "t"}}, all);
    int64_t clock = 1'000'000LL;
    (void)svc1.doWork(clock);   // the claim: HW behind the log
    ASSERT_FALSE(svc1.controllerCaughtUpForTest()) << "precondition: claimed before catching up";
    svc1.setConsensusLeads(false);   // deposed before the catch-up
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            r1->doWork(clock); r2->doWork(clock); r3->doWork(clock);
            transport.pump(clock);
            (void)svc1.doWork(clock);
        }
    };
    drive(100);   // the HW reaches the claim LEO while consensus is elsewhere
    ASSERT_EQ(r1->highWatermark(), r1->logEndOffset()) << "precondition: the claim LEO was committed in the gap";
    EXPECT_EQ(svc1.assignment(0, 0), nullptr) << "a deposed controller placed a topic";
    svc1.setConsensusLeads(true);
    r1->becomeLeader(4, all);
    r2->becomeFollower(4, 1);
    r3->becomeFollower(4, 1);
    drive(200);
    EXPECT_NE(svc1.assignment(0, 0), nullptr) << "a catch-up spent while deposed lost the claim-time placement";
}

TEST(ControllerReclaimRegression, EpochJumpWithoutObservedGapIsANewClaimRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    ASSERT_TRUE(g.svc1->controllerReady());
    // A record appended but not yet committed (a frozen control thread: the term it never observed may have had
    // another leader) - so the claim LEO is ahead of the HW at the re-claim.
    g.svc1->addRuntimeTopic(TopicSpec{0, 1, 3, "", "t"});
    ASSERT_LT(g.r1->highWatermark(), g.r1->logEndOffset()) << "precondition: an uncommitted record";
    g.r1->becomeLeader(3, g.all);   // consensusLeads_ stays true throughout: no observed gap
    g.r2->becomeFollower(3, 1);
    g.r3->becomeFollower(3, 1);
    g.clock += 1'000'000LL;
    (void)g.svc1->doWork(g.clock);
    EXPECT_FALSE(g.svc1->controllerCaughtUpForTest())
        << "a new replica epoch was treated as a continuation of the old claim (no claim-LEO wait)";
    EXPECT_FALSE(g.svc1->controllerReady());
    g.drive(200);
    EXPECT_TRUE(g.svc1->controllerCaughtUpForTest());
    EXPECT_NE(g.svc1->assignment(0, 0), nullptr);
}

TEST(ControllerReclaimRegression, HoldReleasedInConsensusGapPlacesOnReclaimRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    ASSERT_TRUE(g.svc1->controllerReady());
    g.svc1->holdPlacementForTest(true);
    g.svc1->addRuntimeTopic(TopicSpec{0, 1, 3, "", "t"});
    g.drive(5);
    ASSERT_EQ(g.svc1->assignment(0, 0), nullptr) << "precondition: held";
    g.svc1->setConsensusLeads(false);
    g.drive(5);
    g.svc1->holdPlacementForTest(false);   // released in the gap
    g.drive(5);
    EXPECT_EQ(g.svc1->assignment(0, 0), nullptr) << "a deposed controller placed a topic";
    g.rewin(3);
    g.drive(200);
    EXPECT_NE(g.svc1->assignment(0, 0), nullptr) << "a hold released in the consensus gap is never placed";
}

TEST(ControllerReclaimRegression, DeposedControllerNeverDrainsQueuedRecordsRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    ASSERT_TRUE(g.svc1->controllerReady());
    // 300 partitions: one drain appends kMaxRecordsPerBatch (256), the rest stays queued under this claim.
    constexpr int32_t kParts = 300;
    g.svc1->addRuntimeTopic(TopicSpec{0, kParts, 1, "", "t"});
    ASSERT_GT(g.svc1->pendingDecisionsForTest(), 0u) << "precondition: decisions queued, not yet appended";
    g.svc1->setConsensusLeads(false);
    const Offset leoDeposed = g.r1->logEndOffset();
    g.drive(50);
    EXPECT_EQ(g.svc1->pendingDecisionsForTest(), 0u)
        << "decisions taken under the lost claim survived the deposition (a zombie controller's queue)";
    EXPECT_EQ(g.r1->logEndOffset(), leoDeposed) << "a deposed controller appended queued decisions";
    // Re-win at the SAME replica epoch first (consensus is pushed every cycle, the bridge's epoch bump can lag a
    // cycle): it is a NEW claim, never a continuation that drains the old queue.
    g.svc1->setConsensusLeads(true);
    g.drive(1);
    g.rewin(3);
    g.drive(300);
    for (int32_t p = 0; p < kParts; ++p) ASSERT_NE(g.svc1->assignment(0, p), nullptr) << "partition " << p;
    std::vector<int32_t> epochs;
    for (int32_t p = 0; p < kParts; ++p) epochs.push_back(g.svc1->assignment(0, p)->partitionEpoch);
    const Offset leo = g.r1->logEndOffset();
    g.drive(50);
    for (int32_t p = 0; p < kParts; ++p)
        EXPECT_EQ(g.svc1->assignment(0, p)->partitionEpoch, epochs[p]) << "partition " << p << " placed twice";
    EXPECT_EQ(g.r1->logEndOffset(), leo) << "placement records kept being appended after the re-claim";
}

// ===========================================================================
// F-A review I1 (2026-10-16): a coordinator partition (__catalog, __group, __txn) MOVED to a new controller or FAILED
// OVER never keeps a member the controller knows is stopping.  Under the leader grace every peer reads live, so the
// unfiltered ISR kept a cleanly stopped broker in the __catalog ISR (HW = min over the ISR) across every flip.
// ===========================================================================
TEST(CoordinatorPartitionRegression, MoveToNewControllerDropsAShuttingMemberRegression) {
    const std::vector<int32_t> replicas = {1, 2, 3}, shutting = {3};
    const auto cur = coordPa(2, {2, 1, 3});
    const auto d = MetadataService::decideCoordinatorPartition(-6, 0, /*self=*/1, &cur, {1, 2, 3}, nullptr,
                                                               /*bootstrapEpoch=*/4, replicas, false, nullptr, &shutting);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->leaderNode, 1);
    EXPECT_EQ(d->isr, (std::vector<int32_t>{1, 2})) << "the move kept the stopping member 3 in the ISR";
}

TEST(CoordinatorPartitionRegression, FailoverDropsAShuttingMemberRegression) {
    const std::vector<int32_t> replicas = {1, 2, 3, 4}, shutting = {3};
    rune::metadata::PartitionAssignment cur = coordPa(2, {2, 3, 4});
    cur.replicas = replicas;
    // leader 2 dead (not eligible), the controller (1) not in the ISR: fail over among the live ISR members.
    const auto d = MetadataService::decideCoordinatorPartition(-6, 0, /*self=*/1, &cur, {1, 3, 4}, nullptr,
                                                               4, replicas, false, nullptr, &shutting);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->leaderNode, 4) << "the failover elected the stopping member";
    EXPECT_EQ(std::count(d->isr.begin(), d->isr.end(), 3), 0) << "the failover kept the stopping member 3 in the ISR";
    // The only in-sync survivor is stopping: it still leads (never NO_LEADER / unclean for want of a clean member).
    const auto onlyStopping = coordPa(2, {2, 3});
    const auto e = MetadataService::decideCoordinatorPartition(-6, 0, 1, &onlyStopping, {1, 3}, nullptr, 4, {1, 2, 3},
                                                               false, nullptr, &shutting);
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->leaderNode, 3);
}

TEST(CoordinatorPartitionRegression, CatalogMovedBackAfterAFlipShedsTheStoppingMemberInOneRecordRegression) {
    CtlRig c(/*userTopics=*/false);
    const rune::metadata::PartitionKey cat{-6, 0};
    c.svc->setCoordinatorPartitions({cat}, c.all);
    (void)c.svc->runDynamicRebalanceOnTick({1, 2, 3}, nullptr);
    c.drive(50);
    ASSERT_NE(c.svc->assignment(-6, 0), nullptr) << "precondition: __catalog bootstrapped";
    // Consensus moves to node 2: its controller takes __catalog (ISR {2,1,3}).
    {
        Failover f(c);
        f.svc2.setCoordinatorPartitions({cat}, c.all);
        (void)f.svc2.runDynamicRebalanceOnTick({1, 2, 3}, nullptr);
        f.drive(50);
        ASSERT_EQ(f.svc2.assignment(-6, 0)->leaderNode, 2) << "precondition: moved to node 2";
        ASSERT_EQ(f.svc2.assignment(-6, 0)->isr.size(), 3u);
    }
    // ... and back to node 1 (a flip); node 3 is stopping (its controlled-shutdown notice reached node 1).
    c.r2->becomeFollower(3, 1);
    c.r3->becomeFollower(3, 1);
    c.r1->becomeLeader(3, c.all);
    c.transport.removeFollower(1);
    c.transport.setLeader(c.r1.get());
    c.transport.addFollower(2, c.r2.get());
    c.drive(100);
    ASSERT_TRUE(c.svc->controllerReady()) << "precondition: node 1 re-claimed";
    (void)c.svc->onAlterIsr(nodeControl(3, 33, /*shutdown=*/true));
    c.drive(20);
    (void)c.svc->runDynamicRebalanceOnTick({1, 2, 3}, nullptr);   // ONE tick: the move record itself
    c.drive(50);
    const auto* a = c.svc->assignment(-6, 0);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->leaderNode, 1);
    EXPECT_EQ(std::count(a->isr.begin(), a->isr.end(), 3), 0)
        << "the move record kept the stopping member: __catalog commits stall until a later fence round";
}

// Review M1: a consensus-only deposition (the replica still leads, the log unchanged) keeps the MSM's apply cursor -
// re-applying the whole log onto the live map re-bumped its committed version (a routing rebuild) on every flap.
TEST(ControllerReclaimRegression, ConsensusOnlyDepositionNeverReappliesTheLogRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    g.svc1->addRuntimeTopic(TopicSpec{0, 3, 3, "", "t"});
    g.drive(100);
    ASSERT_NE(g.svc1->assignment(0, 2), nullptr);
    const uint64_t v = g.svc1->msmForTest().committedVersion();
    const uint64_t rr = g.svc1->msmForTest().regressionsRejected();
    g.svc1->setConsensusLeads(false);
    g.drive(30);
    EXPECT_EQ(g.svc1->msmForTest().committedVersion(), v) << "the deposition re-applied the log onto the live map";
    EXPECT_EQ(g.svc1->msmForTest().regressionsRejected(), rr);
    g.rewin(3);
    g.drive(100);
    EXPECT_TRUE(g.svc1->controllerReady());
    EXPECT_NE(g.svc1->assignment(0, 2), nullptr);
}

// Review M4: the control tick has the drain's gate - a consensus-deposed controller whose replica still leads
// appends no tick.
TEST(ControllerReclaimRegression, DeposedControllerAppendsNoControlTickRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    g.drive(50);
    g.svc1->armControlTick(/*deadlineNs=*/0);
    g.svc1->setConsensusLeads(false);
    g.drive(2);
    const Offset leo = g.r1->logEndOffset();
    EXPECT_FALSE(g.svc1->pollAndAppendControlTick(g.clock)) << "a deposed controller appended a control tick";
    EXPECT_EQ(g.r1->logEndOffset(), leo);
}

// Review M5: before its first claim edge (no doWork yet) a controller decides nothing, even with its replica Leader
// and no consensus layer - it has not rebuilt its map nor learned its claim LEO.
TEST(ControllerReclaimRegression, NoDecisionBeforeTheFirstClaimEdgeRegression) {
    ReclaimRig g;
    g.svc1->configure({}, g.all);
    EXPECT_FALSE(g.svc1->controllerReady()) << "ready before any claim (empty map)";
    g.svc1->addRuntimeTopic(TopicSpec{0, 1, 3, "", "t"});
    EXPECT_EQ(g.r1->logEndOffset(), 0) << "placed on an empty map before the claim";
    g.drive(100);
    EXPECT_TRUE(g.svc1->controllerReady());
    EXPECT_NE(g.svc1->assignment(0, 0), nullptr) << "the claim places what was recorded before it";
}

// ===========================================================================
// ISR v3 Step 7 - controller storage tracking.
// ===========================================================================
namespace {
constexpr uint8_t kStorageOriginFounder = rune::replication::AlterIsrMsg::kStorageOriginFounder;
constexpr uint8_t kStorageOriginLegacy  = rune::replication::AlterIsrMsg::kStorageOriginLegacy;
constexpr uint8_t kStorageOriginJoiner  = rune::replication::AlterIsrMsg::kStorageOriginJoiner;

/// A node-control register frame carrying storage `id` / `origin` and feature slot 2 (what ControlledShutdown sends).
rune::replication::AlterIsrResponseMsg registerStorage(MetadataService& svc, int32_t node, int64_t id, uint8_t origin,
                                                       int64_t nonce) {
    using rune::replication::AlterIsrMsg;
    AlterIsrMsg m = nodeControl(node, nonce, false);
    m.isr[AlterIsrMsg::kNodeFeatureLevelSlot] = 2;
    m.isr[AlterIsrMsg::kNodeStorageLoSlot]     = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(id)));
    m.isr[AlterIsrMsg::kNodeStorageHiSlot]     = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(id) >> 32));
    m.isr[AlterIsrMsg::kNodeStorageOriginSlot] = origin;
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    (void)svc.onAlterIsr(m, &resp, &answered);
    return resp;
}

struct LoggedRec { int64_t offset; uint16_t type; int32_t a; int32_t b; int64_t id; };   // 0x8004: a=node id=storage; assignment: a=topic b=partition
/// Every 0x8004 / 0x8005 record and every assignment record in the metadata log, by offset.
std::vector<LoggedRec> recordsInLog(rune::storage::Log& log) {
    std::vector<LoggedRec> out;
    int64_t off = 0;
    while (off < log.logEndOffset()) {
        const auto v = log.readView(off, 64 * 1024);
        if (v.empty()) break;
        std::size_t pos = 0;
        int64_t next = off;
        while (pos < v.len) {
            rune::protocol::RecordBatchReader reader(v.data + pos, v.len - pos);
            if (!reader.ensureValid()) break;
            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > v.len - pos) break;
            reader.forEachRecord([&](std::string_view, std::string_view val, int64_t logOffset, int64_t) {
                if (val.size() < 2) return;
                const auto* b = reinterpret_cast<const uint8_t*>(val.data());
                const uint16_t t = static_cast<uint16_t>(b[0] | (b[1] << 8));
                if (t == rune::metadata::kNodeStorageRecordType) {
                    const auto r = rune::metadata::NodeStorageRecord::decode(b, val.size());
                    if (r) out.push_back({logOffset, t, r->node, 0, r->storageId});
                } else if (t == rune::metadata::kFeatureLevelRecordType) {
                    out.push_back({logOffset, t, 0, 0, 0});
                } else if (t == rune::metadata::kControllerTermRecordType) {
                    const auto r = rune::metadata::ControllerTermRecord::decode(b, val.size());
                    if (r) out.push_back({logOffset, t, 0, 0, r->term});
                } else if (const auto ty = rune::protocol::metadata::peekRecordType(b, val.size());
                           ty && *ty == rune::protocol::metadata::MetadataRecordType::UpdatePartitionAssignment) {
                    const auto r = rune::protocol::metadata::UpdatePartitionAssignmentRecord::decode(b, val.size());
                    if (r) out.push_back({logOffset, 0x0002, r->topicId, r->partition, 0});
                }
            });
            next = std::max<int64_t>(next, reader.baseOffset() + reader.lastOffsetDelta() + 1);
            pos += static_cast<std::size_t>(tl);
        }
        if (next <= off) break;
        off = next;
    }
    return out;
}
std::vector<LoggedRec> storageRecordsInLog(rune::storage::Log& log) {
    std::vector<LoggedRec> out;
    for (const auto& r : recordsInLog(log)) if (r.type == 0x8004 || r.type == 0x8005) out.push_back(r);
    return out;
}
std::size_t count8004(rune::storage::Log& log) {
    std::size_t n = 0;
    for (const auto& r : storageRecordsInLog(log)) n += r.type == 0x8004 ? 1 : 0;
    return n;
}
int64_t last8005(rune::storage::Log& log) {
    int64_t o = -1;
    for (const auto& r : storageRecordsInLog(log)) if (r.type == 0x8005) o = r.offset;
    return o;
}
int64_t only8004(rune::storage::Log& log) {
    int64_t o = -1;
    for (const auto& r : storageRecordsInLog(log)) if (r.type == 0x8004) o = r.offset;
    return o;
}
int64_t lastAssignment(rune::storage::Log& log, int32_t topic) {
    int64_t o = -1;
    for (const auto& r : recordsInLog(log)) if (r.type == 0x0002 && r.a == topic) o = r.offset;
    return o;
}
/// The leader of (topic, 0) proposes `isr` with per-member storage captures; returns {accepted, errorCode, answered}.
struct ProposeResult { bool accepted; int16_t error; bool answered; };
ProposeResult proposeWithCapture(CtlRig& c, std::vector<int32_t> isr, std::map<int32_t, int64_t> caps, int32_t topic = 0,
                                 bool drive = true) {
    const auto& a = c.paOf(topic);
    rune::replication::AlterIsrMsg m{};
    m.topicId = topic; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
    m.isrCount = static_cast<int32_t>(isr.size());
    for (std::size_t i = 0; i < isr.size(); ++i) {
        m.isr[i] = isr[i];
        const auto it = caps.find(isr[i]);
        m.isrStorage[i] = it == caps.end() ? 0 : it->second;
    }
    m.partitionEpoch = a.partitionEpoch;
    rune::replication::AlterIsrResponseMsg resp{};
    bool answered = false;
    const bool ok = c.svc->onAlterIsr(m, &resp, &answered);
    if (drive) c.drive(50);
    return {ok, resp.errorCode, answered};
}
bool isrHasNode(const PartitionAssignment& pa, int32_t n) { return std::find(pa.isr.begin(), pa.isr.end(), n) != pa.isr.end(); }
/// Two topics, topic 0 led by a node != 3 and topic 1 led by 3, node 3 in both ISRs (the full replica set in each).
void twoTopicsWithNode3Everywhere(CtlRig& c) {
    c.forceLeader(3, 1);                 // both topics: leader 3, ISR {3}
    for (int32_t t : {0, 1}) { c.readmit(1, t); c.readmit(2, t); }
    ASSERT_EQ(c.paOf(0).isr.size(), 3u);
    ASSERT_EQ(c.paOf(1).isr.size(), 3u);
    (void)c.svc->onAlterIsr(c.bump());   // topic 0's leader (3) asks for a new epoch: another ISR member leads, 3 leaves
    c.tick();
    ASSERT_NE(c.paOf(0).leaderNode, 3);
    c.readmit(3, 0);
    ASSERT_TRUE(isrHasNode(c.paOf(0), 3));
    ASSERT_EQ(c.paOf(1).leaderNode, 3);
    ASSERT_TRUE(isrHasNode(c.paOf(1), 3));
}
}  // namespace

// B-C1: a node WITHOUT a committed storage is still admitted (the check applies only when one is committed).
// RED: the strict `cap == 0 -> mismatch` clause.
TEST(StorageRegistrationRegression, NodeWithoutACommittedStorageIsStillAdmittedRegression) {
    CtlRig c;
    c.soleIsr();
    const int32_t n = c.pa().leaderNode == 1 ? 2 : 1;
    c.readmit(n);
    EXPECT_TRUE(isrHasNode(c.pa(), n));
}

// Trust on first use: Legacy and Founder storages commit without any strip.  RED: strip Legacy.
TEST(StorageRegistrationRegression, LegacyAndFounderAreTrustedOnFirstUseRegression) {
    CtlRig c;
    const int32_t pe = c.pa().partitionEpoch;
    (void)registerStorage(*c.svc, 2, 0xA, kStorageOriginLegacy, 7);
    c.drive(50);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(2), 0xA);
    EXPECT_EQ(c.pa().partitionEpoch, pe) << "a trust-on-first-use registration stripped the node";
    (void)registerStorage(*c.svc, 3, 0xC, kStorageOriginFounder, 8);
    c.drive(50);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xC);
    EXPECT_EQ(c.pa().partitionEpoch, pe);
}

// A Joiner is stripped from EVERY ISR and leadership BEFORE its storage commits (I-S2): the strips precede the single
// NodeStorageRecord in the log.  RED: queue the NodeStorageRecord in noteStorageRegistration_ itself.
TEST(StorageRegistrationRegression, JoinerIsStrippedFromEveryIsrBeforeItsStorageCommitsRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "", "t0"}, TopicSpec{1, 1, 3, "", "t1"}});
    twoTopicsWithNode3Everywhere(c);
    const int32_t l0 = c.paOf(0).leaderNode;
    const int32_t le1 = c.paOf(1).leaderEpoch;
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(100);   // the strips
    EXPECT_FALSE(isrHasNode(c.paOf(0), 3));
    EXPECT_EQ(c.paOf(0).leaderNode, l0) << "a follower's strip changed the leader";
    EXPECT_FALSE(isrHasNode(c.paOf(1), 3));
    EXPECT_TRUE(c.paOf(1).leaderNode == 1 || c.paOf(1).leaderNode == 2);
    EXPECT_EQ(c.paOf(1).leaderEpoch, le1 + 1);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0) << "committed before the node left every ISR";
    c.tick(); c.drive(100);   // the confirmation (the map names the node nowhere now)
    EXPECT_EQ(count8004(c.log1), 1u);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
    const int64_t at = only8004(c.log1);
    EXPECT_LT(lastAssignment(c.log1, 0), at);
    EXPECT_LT(lastAssignment(c.log1, 1), at);
}

// The catalog partition (-6) is stripped like a user partition, and the confirmation waits for it.  (fenceIsrs_ removes
// the same follower from the ISR, so the strip pass alone cannot be isolated here: the test pins the OUTCOME - no
// partition names the node, and its storage commits.)
TEST(StorageRegistrationRegression, CatalogPartitionIsStrippedTooRegression) {
    CtlRig c(false);
    c.svc->setCoordinatorPartitions({{-1, 0}, {-6, 0}}, c.all);
    c.drive(50);
    c.tick();
    ASSERT_NE(c.svc->assignment(-6, 0), nullptr);
    ASSERT_TRUE(isrHasNode(*c.svc->assignment(-6, 0), 3));
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.tick(); c.drive(100);
    EXPECT_FALSE(isrHasNode(*c.svc->assignment(-6, 0), 3));
    EXPECT_FALSE(isrHasNode(*c.svc->assignment(-1, 0), 3));
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
}

// The sole in-sync leader loses its storage: the partition goes leaderless WITHOUT it in the lineage and is NOT
// re-elected by the next ticks.  RED: let the balancer's record through (its no-survivor rule keeps {3}).
TEST(StorageRegistrationRegression, SoleIsrLeaderWithLostStorageGoesLeaderlessWithoutItInTheLineageRegression) {
    CtlRig c;
    c.forceLeader(3);
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(100);
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    EXPECT_TRUE(c.pa().isr.empty());
    EXPECT_EQ(c.pa().mark.kind, LeaderlessMark::Kind::StorageReset);
    EXPECT_EQ(c.pa().mark.node, 3);
    for (int i = 0; i < 3; ++i) c.tick();
    EXPECT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER) << "the wiped node was re-elected";
}

// A-I10: a TRANSIENT partition keeps its lineage ({3}) and the empty-memory mark.  RED: StorageReset for every partition.
TEST(StorageRegistrationRegression, WipedSoleLeaderOfATransientPartitionKeepsItsLineageRegression) {
    CtlRig c;
    c.svc->setTransientTopicPredicate(+[](void*, int32_t t) noexcept { return t == 0; }, nullptr);
    c.forceLeader(3);
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(100);
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    EXPECT_EQ(c.pa().isr, std::vector<int32_t>{3});
    EXPECT_EQ(c.pa().mark.kind, LeaderlessMark::Kind::EmptyMemoryLeader);
}

// A NO_LEADER lineage {3} drops the reset node.  RED: skip NO_LEADER partitions in the strip.
TEST(StorageRegistrationRegression, LeaderlessLineageDropsTheResetNodeRegression) {
    CtlRig c;
    c.forceLeader(3);
    c.tick({1, 2});   // 3 dead: no survivor -> NO_LEADER, lineage {3}
    ASSERT_EQ(c.pa().leaderNode, rune::metadata::NO_LEADER);
    ASSERT_EQ(c.pa().isr, std::vector<int32_t>{3});
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick({1, 2}); c.drive(100);
    EXPECT_TRUE(c.pa().isr.empty());
}

// M13: a pending node is erased from eligible_ AT registration, so an AlterIsr adding it before the next tick is
// refused.  RED: no erase from eligible_ at registration.
TEST(StorageRegistrationRegression, PendingNodeIsNeverAddedNorElectedRegression) {
    CtlRig c;
    c.forceLeader(1);
    c.tick();   // the full live set: eligible_ = {1,2,3}
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);   // NO tick
    const auto r = proposeWithCapture(c, {1, 3}, {});
    EXPECT_TRUE(r.answered);
    EXPECT_EQ(r.error, rune::replication::kAlterIsrIneligibleReplica);
    EXPECT_FALSE(isrHasNode(c.pa(), 3));
}

// A proposal captured under the OLD storage is Ineligible once the new one is committed; the new capture is admitted.
// Decided by a SUCCESSOR controller that never saw the node's registration (its `live` storage is unknown), so only
// the committed clause can refuse.  RED: no committed clause.
TEST(StorageRegistrationRegression, StaleProposalCapturedUnderTheOldStorageIsIneligibleRegression) {
    CtlRig c;
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    for (int i = 0; i < 6 && c.svc->msmForTest().nodeStorage(3) != 0xB; ++i) c.tick();
    ASSERT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
    Failover f(c);
    ASSERT_TRUE(f.svc2.controllerCaughtUpForTest());
    f.tick();   // the successor learns the live set; node 3 is effective again
    ASSERT_FALSE(isrHasNode(f.pa(), 3));
    const auto propose = [&](int64_t cap) {
        const auto& a = f.pa();
        rune::replication::AlterIsrMsg m{};
        m.topicId = 0; m.partition = 0; m.leaderId = a.leaderNode; m.leaderEpoch = a.leaderEpoch;
        m.isrCount = static_cast<int32_t>(a.isr.size()) + 1;
        std::size_t i = 0;
        for (int32_t n : a.isr) m.isr[i++] = n;
        m.isr[i] = 3; m.isrStorage[i] = cap;
        m.partitionEpoch = a.partitionEpoch;
        rune::replication::AlterIsrResponseMsg resp{};
        bool answered = false;
        const bool ok = f.svc2.onAlterIsr(m, &resp, &answered);
        f.drive(50);
        return ProposeResult{ok, resp.errorCode, answered};
    };
    const auto stale = propose(0xA);
    EXPECT_EQ(stale.error, rune::replication::kAlterIsrIneligibleReplica);
    EXPECT_FALSE(isrHasNode(f.pa(), 3));
    EXPECT_TRUE(propose(0xB).accepted);
    EXPECT_TRUE(isrHasNode(f.pa(), 3));
}

// A-M2: a proposal under a storage the node DISOWNED (it registered a newer one) is Ineligible before the strip even
// runs.  eligible_ is re-added for the test so only the `live` clause can refuse.  RED: drop the `live` clause.
TEST(StorageRegistrationRegression, ProposalUnderADisownedStorageIsIneligibleBeforeTheStripRegression) {
    CtlRig c;
    (void)registerStorage(*c.svc, 3, 0xA, kStorageOriginLegacy, 7);
    c.drive(50);
    ASSERT_EQ(c.svc->msmForTest().nodeStorage(3), 0xA);
    c.forceLeader(1);
    c.tick();
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 10);   // no tick
    c.svc->eligibleInsertForTest(3);
    const auto r = proposeWithCapture(c, {1, 3}, {{3, 0xA}});
    EXPECT_EQ(r.error, rune::replication::kAlterIsrIneligibleReplica);
    EXPECT_FALSE(isrHasNode(c.pa(), 3));
}

// A-M1: an in-flight record that names only OTHER nodes never blocks the confirmation.  RED: block on any in-flight
// record of a partition the node replicates.
TEST(StorageRegistrationRegression, UnrelatedInFlightRecordNeverBlocksConfirmationRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "", "t0"}, TopicSpec{1, 1, 3, "", "t1"}});
    ASSERT_TRUE(proposeWithCapture(c, {1, 2}, {}, /*topic=*/1).accepted);   // topic 1: ISR {1,2}; topic 0 keeps {1,2,3}
    ASSERT_TRUE(isrHasNode(c.paOf(0), 3));
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick();   // strips topic 0
    ASSERT_FALSE(isrHasNode(c.paOf(0), 3));
    ASSERT_FALSE(isrHasNode(c.paOf(1), 3));
    // A shrink of topic 1 to {1}: accepted and NOT driven - in flight, naming only {1}.
    ASSERT_TRUE(proposeWithCapture(c, {1}, {}, 1, /*drive=*/false).accepted);
    const std::size_t before = count8004(c.log1);
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 0);   // the tick, without the drive
    c.drive(50);
    EXPECT_GT(count8004(c.log1), before) << "an unrelated in-flight record blocked the confirmation";
}

// v2-review I4: an in-flight record that ADDS the node blocks its confirmation; after it commits the node is stripped
// again and then confirmed.  RED: namesNodeInFlightOrQueued_ checks queued records only.
TEST(StorageRegistrationRegression, InFlightExpansionAddingTheNodeBlocksConfirmationRegression) {
    CtlRig c;
    c.forceLeader(1);
    c.tick();
    ASSERT_TRUE(proposeWithCapture(c, {1, 3}, {}, 0, /*drive=*/false).accepted);   // accepted, in flight, names 3
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 0);   // tick, no drive: the expansion is still in flight
    EXPECT_EQ(count8004(c.log1), 0u) << "confirmed while an in-flight record names the node";
    c.drive(50);                                                  // the expansion commits
    EXPECT_TRUE(isrHasNode(c.pa(), 3));
    c.tick(); c.drive(100);                                       // stripped again
    EXPECT_FALSE(isrHasNode(c.pa(), 3));
    c.tick(); c.drive(100);                                       // then confirmed
    EXPECT_EQ(count8004(c.log1), 1u);
    EXPECT_LT(lastAssignment(c.log1, 0), only8004(c.log1));
}

// A-I2: the same controller regaining leadership rebuilds the strip from the registrations it holds (no new register).
// RED: no replay in maybeCatchUp_.
TEST(StorageRegistrationRegression, SameControllerRegainingLeadershipRebuildsTheStripRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "", "t0"}, TopicSpec{1, 1, 3, "", "t1"}});
    ASSERT_TRUE(isrHasNode(c.paOf(0), 3));
    ASSERT_TRUE(isrHasNode(c.paOf(1), 3));
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tickMax(1);   // one strip
    c.bounceLeadership();
    c.tick(); c.drive(100);   // the second strip
    EXPECT_FALSE(isrHasNode(c.paOf(0), 3));
    EXPECT_FALSE(isrHasNode(c.paOf(1), 3));
    c.tick(); c.drive(100);   // the confirmation
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
}

// A late frame of a dead process never erases the live process's strip, and the live process is told it was
// superseded.  The late frame carries a nonce this controller never saw (the controller keeps its node-control
// identities across a bounce, so a SEEN dead nonce is rejected before the table).  RED: erase on committed == reported
// without the nonce check.
TEST(StorageRegistrationRegression, LateFrameOfTheDeadProcessNeverErasesTheStripRegression) {
    CtlRig c;
    (void)registerStorage(*c.svc, 3, 0xA, kStorageOriginLegacy, 11);
    c.drive(50);
    ASSERT_EQ(c.svc->msmForTest().nodeStorage(3), 0xA);
    c.bounceLeadership();
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 12);
    (void)registerStorage(*c.svc, 3, 0xA, kStorageOriginLegacy, 13);   // the late frame (committed 0xA == reported)
    EXPECT_STREQ(c.svc->storageRegStateForTest(3), "Stripping");
    const auto& el = c.svc->eligibleForTest();
    EXPECT_EQ(std::count(el.begin(), el.end(), 3), 0);
    const auto resp = registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 12);
    EXPECT_EQ(resp.errorCode, rune::replication::AlterIsrMsg::kNodeSuperseded);
}

// A-I2 / v2-review I9: a successor controller redoes an interrupted strip from the registration it is told again.
// RED: skip registration when any strip record already exists in the committed map.
TEST(StorageRegistrationRegression, SuccessorControllerRedoesAnInterruptedStripRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "", "t0"}, TopicSpec{1, 1, 3, "", "t1"}});
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tickMax(1);   // one strip
    Failover f(c);
    ASSERT_TRUE(f.svc2.controllerCaughtUpForTest());
    (void)registerStorage(f.svc2, 3, 0xB, kStorageOriginJoiner, 9);
    f.tick();       // the remaining strip
    f.tick();       // the confirmation
    EXPECT_EQ(f.svc2.msmForTest().nodeStorage(3), 0xB);
    EXPECT_EQ(count8004(c.log2), 1u);
}

// R10: below feature level 2 nothing is decided; raising the level replays the registrations.  RED: no level check.
TEST(StorageRegistrationRegression, NothingIsDecidedBelowFeatureLevelTwoRegression) {
    CtlRig c(true, {}, rune::replication::MetadataFeature::kBase);
    ASSERT_LT(c.svc->featureLevel(), 2);
    c.svc->setClusterMembers({1, 2, 3});
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(50);
    EXPECT_EQ(count8004(c.log1), 0u);
    EXPECT_TRUE(isrHasNode(c.pa(), 3));
    c.svc->setSupportedFeatureLevel(rune::replication::MetadataFeature::kLatest);
    (void)registerStorage(*c.svc, 1, 0x11, kStorageOriginLegacy, 21);
    (void)registerStorage(*c.svc, 2, 0x12, kStorageOriginLegacy, 22);
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    for (int i = 0; i < 20 && c.svc->featureLevel() < 2; ++i) c.drive(50);
    ASSERT_EQ(c.svc->featureLevel(), 2);
    c.tick(); c.drive(100);
    c.tick(); c.drive(100);
    EXPECT_FALSE(isrHasNode(c.pa(), 3));
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
}

// A second disk loss before the confirmation restarts the strip under the NEW storage; the first one is never committed.
// RED: keep the first entry.
TEST(StorageRegistrationRegression, SecondDiskLossBeforeConfirmationRestartsTheStripRegression) {
    CtlRig c(true, {TopicSpec{0, 1, 3, "", "t0"}, TopicSpec{1, 1, 3, "", "t1"}});
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tickMax(1);
    (void)registerStorage(*c.svc, 3, 0xC, kStorageOriginJoiner, 11);
    c.tick(); c.drive(100);
    c.tick(); c.drive(100);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xC);
    for (const auto& r : storageRecordsInLog(c.log1)) EXPECT_FALSE(r.type == 0x8004 && r.id == 0xB) << "the first storage was committed";
}

// B-I7: a confirmation record that was lost (truncated) is re-decided: Committing -> Stripping -> a new record.
// RED: no re-queue in pruneStorageRegs_.
TEST(StorageRegistrationRegression, LostConfirmationRecordIsRequeuedRegression) {
    CtlRig c;
    c.svc->dropNextStorageAppendForTest();
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(100);   // the strip
    c.tick(); c.drive(100);   // the confirmation is decided and lost
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0);
    EXPECT_STREQ(c.svc->storageRegStateForTest(3), "Committing");
    c.tick(); c.drive(50);
    c.tick(); c.drive(50);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
    EXPECT_GE(c.svc->storageConfirmRequeuedForTest(), 1u);
}

// A lost trust-on-first-use record is re-queued by the next register (committed == 0).  RED: "unless queued or in
// flight" without the committed == 0 re-queue (a register never queues twice).
TEST(StorageRegistrationRegression, LostTrustOnFirstUseRecordIsRequeuedByTheNextRegisterRegression) {
    CtlRig c;
    c.svc->dropNextStorageAppendForTest();
    (void)registerStorage(*c.svc, 2, 0xA, kStorageOriginLegacy, 5);
    c.drive(50);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(2), 0);
    (void)registerStorage(*c.svc, 2, 0xA, kStorageOriginLegacy, 5);   // the same process re-registers
    c.drive(50);
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(2), 0xA);
}

// v2-review I9: a confirmation queued in an earlier term is never appended in a later one.
// RED: keep pendingStorageRecords_ across the leader loss.
TEST(StorageRegistrationRegression, ConfirmationQueuedInAnEarlierTermIsNeverAppendedLaterRegression) {
    CtlRig c;
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    c.tick(); c.drive(100);   // strip: no partition names 3 any more
    ASSERT_FALSE(isrHasNode(c.pa(), 3));
    c.r1->setAppendRefusedForTest(true);
    (void)c.svc->reassignDeadPartitionLeaders({1, 2, 3}, 0);   // the confirmation is queued; its append is refused
    c.drive(20);
    ASSERT_EQ(count8004(c.log1), 0u);
    c.r1->setAppendRefusedForTest(false);
    c.bounceLeadership();
    c.tick(); c.drive(100);
    EXPECT_EQ(count8004(c.log1), 1u) << "the stale queued record was appended as well";
    EXPECT_GT(only8004(c.log1), last8005(c.log1)) << "the confirmation predates the regained term's record";
    EXPECT_EQ(c.svc->msmForTest().nodeStorage(3), 0xB);
}

// v2-review I9: halt() clears the storage soft state.  RED: no clear in halt().
TEST(StorageRegistrationRegression, HaltClearsTheStorageSoftStateRegression) {
    CtlRig c;
    (void)registerStorage(*c.svc, 3, 0xB, kStorageOriginJoiner, 9);
    ASSERT_STREQ(c.svc->storageRegStateForTest(3), "Stripping");
    ASSERT_EQ(c.svc->storagePendingCount(), 1u);
    c.svc->halt();
    EXPECT_STREQ(c.svc->storageRegStateForTest(3), "None");
    EXPECT_EQ(c.svc->storagePendingCount(), 0u);
}

// ===========================================================================
// ISR v3 Step 8 - controller-term record + bootstrap guard.
// ===========================================================================

// R9 / I-P2: a claim is ready only once a record of ITS OWN term has committed.  With no follower reachable the
// term record is appended but cannot commit.  RED: set ClaimReady at the claim LEO without appending the record.
TEST(MetadataServiceRegression, ReadinessWaitsForAThisTermRecordRegression) {
    CtlRig c;
    const int64_t base = c.r1->logEndOffset();
    c.transport.removeFollower(2);
    c.transport.removeFollower(3);
    c.r1->becomeFollower(c.r1->epoch() + 1, NodeId{-1});
    c.drive(5);
    c.r1->becomeLeader(c.r1->epoch() + 1, c.all);
    c.r2->becomeFollower(c.r1->epoch(), 1);
    c.r3->becomeFollower(c.r1->epoch(), 1);
    c.drive(50);
    EXPECT_FALSE(c.svc->controllerReady()) << "ready before a record of this term committed";
    bool found = false;
    for (const auto& r : storageRecordsInLog(c.log1)) found = found || (r.type == 0x8005 && r.offset == base);
    EXPECT_TRUE(found) << "no controller-term record at the claim LEO";
    c.transport.addFollower(2, c.r2.get());
    c.transport.addFollower(3, c.r3.get());
    c.drive(100);
    EXPECT_TRUE(c.svc->controllerReady());
}

namespace {
/// A single-node controller over an EMPTY metadata log (the Idempotent test's rig), driven to its claim.
struct SoleController {
    SaeTempDir dir{"mb"};
    rune::storage::Log log{dir.path};
    std::vector<NodeId> nodes{1};
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<MetadataService> svc;
    int64_t clock = 0;
    SoleController() {
        log.recover();
        replica = std::make_unique<PartitionReplica>(makeMetaCfg(1, 1), Role::Leader, 1, nodes, nodes, &log, &transport);
        transport.setLeader(replica.get());
        svc = std::make_unique<MetadataService>(replica.get(), 1);
        svc->configure({TopicSpec{0, 1, 1, "", "t"}}, nodes);
    }
    void drive(int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            replica->doWork(clock);
            transport.pump(clock);
            (void)svc->doWork(clock);
        }
    }
};
}  // namespace

// R17 (v2-review C1): mayBootstrap is read at the claim, from the LIVE source - never copied when it is set.  A node
// whose storage is Undecided at construction and founds before it claims must still bootstrap.
// RED: copy the thunk's value into a member inside setMayBootstrap (it reads false and halts).
TEST(MetadataServiceRegression, MayBootstrapIsReadAtTheClaimNotAtConstructionRegression) {
    SoleController s;
    bool flag = false;
    s.svc->setMayBootstrap(+[](void* f) noexcept { return *static_cast<bool*>(f); }, &flag);
    flag = true;   // after construction, before the first doWork
    s.drive(100);
    EXPECT_TRUE(s.svc->controllerReady());
    const auto recs = recordsInLog(s.log);
    ASSERT_GE(recs.size(), 2u);
    EXPECT_EQ(recs[0].type, 0x8005);                               // the term record first
    EXPECT_EQ(recs[1].type, rune::metadata::kFeatureLevelRecordType);   // then the bootstrap level
}

// R17: an empty claim on a formed storage never bootstraps: no level record, no static emit, not ready, resign.
// RED: no guard.
TEST(MetadataServiceRegression, EmptyClaimNeverBootstrapsOnAFormedStorageRegression) {
    SoleController s;
    bool flag = false;
    s.svc->setMayBootstrap(+[](void* f) noexcept { return *static_cast<bool*>(f); }, &flag);
    s.drive(100);
    EXPECT_FALSE(s.svc->controllerReady());
    EXPECT_TRUE(s.svc->resignRequestedForTest());
    EXPECT_EQ(s.svc->assignment(0, 0), nullptr) << "a static assignment was emitted";
    for (const auto& r : recordsInLog(s.log)) EXPECT_NE(r.type, rune::metadata::kFeatureLevelRecordType);
}

// R17 + the term record: the controller's OWN claim record is the first record a Founder's empty metadata log ever
// applies.  It must not count as "data on this storage" (the manifest's MetadataLog mark), or every Founder's first
// claim would read mayBootstrap == false at the ready edge and halt (found by ShardModeRegression.WorkerSideAcks*).
// RED: the first-apply hook fires for the ControllerTermRecord.
TEST(MetadataServiceRegression, FoundersOwnTermRecordIsNotDataOnItsStorageRegression) {
    SoleController s;
    bool fired = false;
    s.svc->setFirstApplyHook(+[](void* f) noexcept { *static_cast<bool*>(f) = true; }, &fired);
    s.svc->setMayBootstrap(+[](void* f) noexcept { return !*static_cast<bool*>(f); }, &fired);   // as BrokerNode's thunk
    s.drive(100);
    EXPECT_TRUE(s.svc->controllerReady()) << "the founder's first claim halted on its own term record";
    EXPECT_FALSE(s.svc->resignRequestedForTest());
    bool level = false;
    for (const auto& r : recordsInLog(s.log)) level = level || r.type == rune::metadata::kFeatureLevelRecordType;
    EXPECT_TRUE(level) << "the founder never bootstrapped the metadata log";
    EXPECT_TRUE(fired) << "the bootstrap records themselves are data on the storage";
}

// ISR v3 Step 9: the node-control register carries the storage (slots 7/8 = id, 9 = origin) and, until the storage is
// confirmed (gate open AND a committed record), is re-sent every 5 s; a new consensus term of the SAME controller
// registers again.  RED: no resend, or the id-only controller check.
TEST(ControlledShutdownRegression, RegisterCarriesStorageAndReRegistersUntilConfirmedRegression) {
    RecordingRig r;
    bool open = false;
    r.cs.setStorage(0xB, 3);
    r.cs.setStorageOpenFn(+[](void* o) noexcept { return *static_cast<bool*>(o); }, &open);
    int64_t now = 1'000'000;
    r.followLeader1(now);
    (void)r.cs.doWork(now);
    ASSERT_FALSE(r.sent.empty());
    for (const auto& m : r.sent) {
        EXPECT_EQ(m.isr[rune::replication::AlterIsrMsg::kNodeStorageLoSlot], 0xB);
        EXPECT_EQ(m.isr[rune::replication::AlterIsrMsg::kNodeStorageHiSlot], 0);
        EXPECT_EQ(m.isr[rune::replication::AlterIsrMsg::kNodeStorageOriginSlot], 3);
    }
    r.answer(rune::replication::kAlterIsrOk);
    (void)r.cs.doWork(++now);
    ASSERT_EQ(r.cs.phaseOnControl(), rune::node::ControlledShutdown::Phase::Running);
    const std::size_t perRegister = 3;   // one frame per member {0, 1, 2}
    const int64_t t0 = now;
    r.sent.clear();
    (void)r.cs.doWork(t0 + 4'900'000'000LL);
    EXPECT_TRUE(r.sent.empty()) << "re-registered before the 5 s cadence";
    (void)r.cs.doWork(t0 + 5'000'000'000LL);
    EXPECT_EQ(r.sent.size(), perRegister) << "no register at +5 s";
    (void)r.cs.doWork(t0 + 6'000'000'000LL);
    EXPECT_EQ(r.sent.size(), perRegister) << "re-registered inside the cadence";
    (void)r.cs.doWork(t0 + 10'000'000'000LL);
    EXPECT_EQ(r.sent.size(), 2 * perRegister) << "no register at +10 s";
    open = true;   // the gate opened; with no committed-storage requirement below level 2 nothing more is needed
    (void)r.cs.doWork(t0 + 15'000'000'000LL);
    EXPECT_EQ(r.sent.size(), 2 * perRegister) << "still registering after the storage was confirmed";
    // A new term of the SAME controller: the registration is lost with the old term's soft state.
    r.cs.setRegisteredTermForTest(r.e.leadershipTermId() - 1);
    (void)r.cs.doWork(t0 + 16'000'000'000LL);
    EXPECT_EQ(r.cs.phaseOnControl(), rune::node::ControlledShutdown::Phase::Registering);
}

// v2-review I3: the DECIDED origin rides the next register (a gate that founded / joined re-registers at once).
TEST(ControlledShutdownRegression, DecidedOriginRidesTheNextRegisterRegression) {
    RecordingRig r;
    r.cs.setStorage(0xB, 4);   // Undecided
    int64_t now = 1'000'000;
    r.followLeader1(now);
    (void)r.cs.doWork(now);
    r.answer(rune::replication::kAlterIsrOk);
    (void)r.cs.doWork(++now);
    ASSERT_EQ(r.cs.phaseOnControl(), rune::node::ControlledShutdown::Phase::Running);
    r.sent.clear();
    r.cs.setStorage(0xB, 3);   // decided: Joiner
    (void)r.cs.reportStorageDecided();
    (void)r.cs.doWork(++now);
    ASSERT_FALSE(r.sent.empty());
    for (const auto& m : r.sent) EXPECT_EQ(m.isr[rune::replication::AlterIsrMsg::kNodeStorageOriginSlot], 3);
}

// ISR v3 Step 10 (R3, R10): at feature level 1 nothing tracks storage, but a HELD leader role still fails over BY BUMP:
// the replica's restart-fence bump moves the leadership to another live ISR member and the requester leaves the ISR.
// RED: in decideBumps_ take the same-leader re-grant branch for a Fence bump even when another live ISR member exists
// (the leader stays 3).
TEST(StorageRegistrationRegression, HeldLeaderFailsOverBelowFeatureLevelTwoRegression) {
    CtlRig c(true, {}, rune::replication::MetadataFeature::kBase);
    ASSERT_LT(c.svc->featureLevel(), 2);
    c.forceLeader(3);
    c.readmit(1);
    c.readmit(2);
    ASSERT_EQ(c.pa().leaderNode, 3);
    ASSERT_EQ(c.pa().isr.size(), 3u);
    const int32_t le = c.pa().leaderEpoch;
    (void)c.svc->onAlterIsr(c.bump());   // the restart-fence bump a held leader role sends
    c.tick();
    EXPECT_NE(c.pa().leaderNode, 3) << "the held leader was re-granted its leadership";
    EXPECT_EQ(c.pa().leaderEpoch, le + 1);
    EXPECT_FALSE(isrHasNode(c.pa(), 3));
}
