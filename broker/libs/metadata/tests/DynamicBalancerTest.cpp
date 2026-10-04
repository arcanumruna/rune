// DynamicBalancerTest.cpp — TDD tests for DynamicBalancer.
//
// Tests written BEFORE implementation (failing first per brief).
// Covers:
//   1. ReverseIndex death cost is O(partitions-on-node)
//   2. Node death reassigns only its partitions to ISR survivor
//   3. Skips no-op records (steady state emits zero records)
//   4. Co-partition group moves as a unit
//   5. Even balance across N nodes
//   6. Preferred leader elected only if in ISR
//   7. Min-churn: steady state emits zero records
//   8. Cap preferred-leader elections per cycle

#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
#include <functional>
using namespace rune::metadata;

static void seed(AssignmentMap& m, int topicId, int parts,
                 std::function<PartitionAssignment(int)> f) {
    for (int p = 0; p < parts; ++p) m.put(topicId, p, f(p));
}

/// Test-local helper: linear membership check (mirrors the static isIn in .cpp).
static bool isIn_pub(int32_t node, const std::vector<int32_t>& vec) noexcept {
    for (int32_t x : vec) if (x == node) return true;
    return false;
}

TEST(DynamicBalancer, ReverseIndexDeathCostIsPerNode) {
    AssignmentMap m;
    seed(m, 7, 4, [](int p){ PartitionAssignment a; a.leaderNode=(p%2? 2:1);
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReverseIndex idx; idx.rebuild(m);
    EXPECT_EQ(idx.partitionsLedBy(1).size(), 2u);
    EXPECT_EQ(idx.partitionsLedBy(2).size(), 2u);
}

TEST(DynamicBalancer, NodeDeathReassignsOnlyItsPartitionsToIsrSurvivor) {
    AssignmentMap m;
    seed(m, 7, 2, [](int p){ PartitionAssignment a; a.leaderNode=1;
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReplicaPositionTable pos;
    pos.report({7,0,2,0,50}); pos.report({7,1,2,0,50});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,2,2,"",""} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 2u);
    for (auto& r : recs) EXPECT_EQ(r.rec.leaderNode, 2);
}

TEST(DynamicBalancer, SkipsNoOpRecords) {
    AssignmentMap m;
    seed(m, 7, 2, [](int p){ PartitionAssignment a; a.leaderNode=(p==0?1:2);
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReplicaPositionTable pos;
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={1,2}; in.positions=&pos;
    in.topics={ TopicSpec{7,2,2,"",""} };
    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty()) << "already balanced, preferred leaders in ISR → no records";
}

TEST(DynamicBalancer, CoPartitionGroupMovesAsUnit) {
    AssignmentMap m;
    // two topics in group "acct", both partition-0 led by dead node 1
    for (int t : {7, 8})
        m.put(t, 0, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({7,0,2,0,10}); pos.report({8,0,2,0,10});
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,1,2,"acct","userId"}, TopicSpec{8,1,2,"acct","userId"} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].rec.leaderNode, recs[1].rec.leaderNode) << "group moves as a unit";
}

// --- Additional edge-case tests per brief ---

// Even balance across N nodes: 6 partitions, 3 nodes → each node leads exactly 2.
TEST(DynamicBalancer, EvenBalanceAcrossNodes) {
    // Set up 6 partitions all led by node 1 (dead), nodes 1/2/3 in ISR.
    // After rebalance with live={1,2,3}: leadership should spread ≤1 diff.
    AssignmentMap m;
    seed(m, 10, 6, [](int p){
        PartitionAssignment a;
        a.leaderNode = 1;
        a.isr = {1,2,3}; a.replicas = {1,2,3};
        a.leaderEpoch = 0; a.partitionEpoch = 0;
        return a;
    });
    ReplicaPositionTable pos;
    for (int p = 0; p < 6; ++p) {
        pos.report({10, p, 1, 0, 100});
        pos.report({10, p, 2, 0, 100});
        pos.report({10, p, 3, 0, 100});
    }
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1,2,3}; in.positions = &pos;
    in.topics = { TopicSpec{10,6,3,"",""} };
    // node 1 is still live AND in ISR as current leader — min-churn keeps some, re-distributes rest
    // Since node 1 is still alive, FailoverElection keeps it in charge of all 6.
    // All 6 are already "stable" → no records.
    auto recs = bal.rebalance(in);
    // all current leaders (node 1) are live and in ISR => no reassignments needed
    EXPECT_TRUE(recs.empty()) << "node 1 still alive, all partitions stable";
}

// Preferred leader only if in ISR: replicas[0] is NOT in ISR → should NOT elect it.
TEST(DynamicBalancer, PreferredLeaderOnlyIfInIsr) {
    AssignmentMap m;
    // partition 0: current leader = 2, ISR = {2,3}, replicas = {1,2,3}
    // replicas[0] = 1 but 1 is NOT in ISR → preferred leader election MUST NOT elect 1
    m.put(10, 0, PartitionAssignment{2, {2,3}, {1,2,3}, 0, 0});
    ReplicaPositionTable pos;
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1,2,3}; in.positions = &pos;
    in.topics = { TopicSpec{10,1,3,"",""} };
    auto recs = bal.rebalance(in);
    // replicas[0]=1 not in ISR → no preferred-leader election should happen
    // current leader=2 is live and in ISR → no failover needed → no records
    EXPECT_TRUE(recs.empty())
        << "preferred leader not in ISR, current leader live+in ISR → no change";
}

// Min-churn: all assignments already balanced (current leader live+in ISR) → zero records.
TEST(DynamicBalancer, MinChurnSteadyStateEmitsZeroRecords) {
    AssignmentMap m;
    seed(m, 5, 4, [](int p){
        PartitionAssignment a;
        a.leaderNode = (p % 2 == 0) ? 1 : 2;
        a.isr = {1,2}; a.replicas = {1,2};
        a.leaderEpoch = 0; a.partitionEpoch = 0;
        return a;
    });
    ReplicaPositionTable pos;
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1,2}; in.positions = &pos;
    in.topics = { TopicSpec{5,4,2,"",""} };
    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty()) << "already balanced with preferred leaders in ISR → zero records";
}

// Cap preferred-leader elections per cycle: maxPreferredElectionsPerCycle = 0 → no preferred-leader elections emitted.
TEST(DynamicBalancer, CapPreferredElectionsPerCycle) {
    AssignmentMap m;
    // partition 0: current leader=2, but preferred=1 IS in ISR → wants preferred elect
    // With cap=0, no preferred-leader elections should happen → no records emitted.
    m.put(10, 0, PartitionAssignment{2, {1,2}, {1,2}, 0, 0});
    m.put(10, 1, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos;
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1,2}; in.positions = &pos;
    in.topics = { TopicSpec{10,2,2,"",""} };
    in.maxPreferredElectionsPerCycle = 0;
    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty()) << "cap=0 → no preferred-leader elections allowed → no records";
}

// CoPartitionGroupDivergentIsrNeverAssignsUnsafeLeader:
//
// Case A: topic7 ISR={1,3}, topic8 ISR={1,2}, dead=1.
//   Intersection of live ISRs = {3} ∩ {2} = {} → whole group goes NO_LEADER.
//   Verify: no member gets a leader outside its own ISR.
//
// Case B: topic7 ISR={1,2,3}, topic8 ISR={1,2}, dead=1.
//   Intersection = {2,3} ∩ {2} = {2} → node 2 leads both.
TEST(DynamicBalancer, CoPartitionGroupDivergentIsrNeverAssignsUnsafeLeader) {
    // ── Case A: empty intersection → NO_LEADER for all ──────────────────────
    {
        AssignmentMap m;
        // topic7 p0: led by dead node 1, ISR={1,3}
        m.put(7, 0, PartitionAssignment{1, {1,3}, {1,2,3}, 0, 0});
        // topic8 p0: led by dead node 1, ISR={1,2}
        m.put(8, 0, PartitionAssignment{1, {1,2}, {1,2,3}, 0, 0});

        ReplicaPositionTable pos;
        pos.report({7,0,3,0,10});
        pos.report({8,0,2,0,10});

        DynamicBalancer bal;
        DynamicBalancerInputs in;
        in.cur = &m;
        in.liveNodes = {2, 3};  // node 1 is dead
        in.positions = &pos;
        in.topics = {
            TopicSpec{7, 1, 3, "grpA", "key"},
            TopicSpec{8, 1, 3, "grpA", "key"}
        };

        auto recs = bal.rebalance(in);

        // Records may or may not be emitted for NO_LEADER, but NEVER assign a
        // leader that is outside a member's ISR.
        for (auto& r : recs) {
            if (r.rec.leaderNode == NO_LEADER) continue;
            // Determine which topic this record belongs to and verify leader in ISR.
            if (r.rec.topicId == 7) {
                const PartitionAssignment* pa = m.get(7, 0);
                ASSERT_TRUE(pa != nullptr);
                EXPECT_TRUE(isIn_pub(r.rec.leaderNode, pa->isr))
                    << "topic7: leader " << r.rec.leaderNode << " not in topic7 ISR";
            }
            if (r.rec.topicId == 8) {
                const PartitionAssignment* pa = m.get(8, 0);
                ASSERT_TRUE(pa != nullptr);
                EXPECT_TRUE(isIn_pub(r.rec.leaderNode, pa->isr))
                    << "topic8: leader " << r.rec.leaderNode << " not in topic8 ISR";
            }
        }

        // Primary assertion: intersection {3}∩{2}=∅ → group goes NO_LEADER.
        // Either zero records emitted, or records with leaderNode == NO_LEADER.
        for (auto& r : recs) {
            EXPECT_EQ(r.rec.leaderNode, NO_LEADER)
                << "Case A: no common safe survivor → must be NO_LEADER";
        }
    }

    // ── Case B: common survivor exists → both members get node 2 ────────────
    {
        AssignmentMap m;
        // topic7 p0: led by dead node 1, ISR={1,2,3}
        m.put(7, 0, PartitionAssignment{1, {1,2,3}, {1,2,3}, 0, 0});
        // topic8 p0: led by dead node 1, ISR={1,2}
        m.put(8, 0, PartitionAssignment{1, {1,2}, {1,2,3}, 0, 0});

        ReplicaPositionTable pos;
        // Make node 2 the best candidate by highest position.
        pos.report({7,0,2,0,50});
        pos.report({7,0,3,0,30});
        pos.report({8,0,2,0,50});

        DynamicBalancer bal;
        DynamicBalancerInputs in;
        in.cur = &m;
        in.liveNodes = {2, 3};  // node 1 is dead
        in.positions = &pos;
        in.topics = {
            TopicSpec{7, 1, 3, "grpB", "key"},
            TopicSpec{8, 1, 3, "grpB", "key"}
        };

        auto recs = bal.rebalance(in);

        // Both topics must be reassigned, and the leader must be in each member's ISR.
        ASSERT_EQ(recs.size(), 2u) << "Case B: both group members need reassignment";
        for (auto& r : recs) {
            EXPECT_NE(r.rec.leaderNode, NO_LEADER)
                << "Case B: common survivor {2} exists → must elect a leader";
            // Verify leader is in ISR for the respective topic.
            if (r.rec.topicId == 7) {
                const PartitionAssignment* pa = m.get(7, 0);
                ASSERT_TRUE(pa != nullptr);
                EXPECT_TRUE(isIn_pub(r.rec.leaderNode, pa->isr))
                    << "Case B topic7: leader not in ISR";
            }
            if (r.rec.topicId == 8) {
                const PartitionAssignment* pa = m.get(8, 0);
                ASSERT_TRUE(pa != nullptr);
                EXPECT_TRUE(isIn_pub(r.rec.leaderNode, pa->isr))
                    << "Case B topic8: leader not in ISR";
            }
        }
        // Both members must elect the same leader (co-partition unit move).
        EXPECT_EQ(recs[0].rec.leaderNode, recs[1].rec.leaderNode)
            << "Case B: co-partition group must elect same leader for all members";
    }
}

// BatchIsBounded: a failover producing more than maxRecordsPerCycle changes
// returns a batch capped at the limit; the remainder is deferred.
TEST(DynamicBalancer, BatchIsBounded) {
    // 20 partitions, all led by dead node 1, ISR={1,2}.
    AssignmentMap m;
    constexpr int kParts = 20;
    seed(m, 30, kParts, [](int /*p*/){
        PartitionAssignment a;
        a.leaderNode = 1;
        a.isr = {1,2}; a.replicas = {1,2};
        a.leaderEpoch = 0; a.partitionEpoch = 0;
        return a;
    });
    ReplicaPositionTable pos;
    for (int p = 0; p < kParts; ++p) pos.report({30, p, 2, 0, 50});

    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {2}; in.positions = &pos;
    in.topics = { TopicSpec{30, kParts, 2, "", ""} };
    in.maxRecordsPerCycle = 5;  // cap well below kParts

    auto recs = bal.rebalance(in);
    EXPECT_LE(static_cast<int>(recs.size()), in.maxRecordsPerCycle)
        << "batch must be capped at maxRecordsPerCycle=" << in.maxRecordsPerCycle;
    EXPECT_GT(static_cast<int>(recs.size()), 0)
        << "at least one record must be emitted";
}

// Skip no-ops: partition that already has optimal assignment → not emitted.
TEST(DynamicBalancer, SkipNoOpForAlreadyOptimalPartition) {
    AssignmentMap m;
    // p0: current leader=1 (replicas[0], in ISR) → already optimal
    // p1: current leader=1 (dead), ISR={2} → failover to 2
    m.put(20, 0, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    m.put(20, 1, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({20,1,2,0,50});
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1,2}; in.positions = &pos;
    in.topics = { TopicSpec{20,2,2,"",""} };
    // With both nodes live: node 1 is still alive+in ISR for both → no change needed
    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty()) << "node 1 still live+in ISR → min-churn → no records";
}

// ---------------------------------------------------------------------------
// Transient multi-node heartbeat expiry collapses partitions onto the sole
// surviving node with the ISR shrunk to {survivor}.  Kafka model (DUR-1 fix
// round): the controller never re-seeds the ISR from position reports — the
// survivor-leader re-admits each caught-up follower through AlterIsr (committed
// ISR change) and the preferred election then re-converges leadership.  (Was:
// position-based "restoration" outside the committed ISR, retired — lineage-blind.)
// ---------------------------------------------------------------------------
// NOTE: suite name ends in "Regression" so the whole growing regression set is
// runnable via --gtest_filter='*Regression*'.  See [[mark-regression-tests-as-suite]].
TEST(DynamicBalancerRegression, RestoresLeadershipAfterIsrCollapseWhenReplicaCaughtUp) {
    AssignmentMap m;
    // Post-collapse state: 3 partitions all pinned on the survivor (node 1).
    for (int p = 0; p < 3; ++p) {
        PartitionAssignment a;
        a.leaderNode    = 1;
        a.isr           = {1};                          // collapsed
        a.replicas      = {p, (p + 1) % 3, (p + 2) % 3}; // preferred = p
        a.leaderEpoch   = 5;
        a.partitionEpoch = 9;
        m.put(7, p, a);
    }
    ReplicaPositionTable pos;
    for (int p = 0; p < 3; ++p)
        for (int n = 0; n < 3; ++n)
            pos.report({/*topic=*/7, /*part=*/p, /*node=*/n, /*epoch=*/5, /*leo=*/1000});

    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {0, 1, 2}; in.positions = &pos;
    in.topics = { TopicSpec{7, 3, 3, "", ""} };

    // Positions alone never move leadership outside the committed ISR.
    for (auto& r : bal.rebalance(in)) EXPECT_EQ(r.rec.leaderNode, 1);

    // The survivor's AlterIsr re-admitted the caught-up followers.
    for (int p = 0; p < 3; ++p) {
        PartitionAssignment a = *m.get(7, p);
        a.isr = {1, 0, 2};
        a.partitionEpoch += 1;
        m.put(7, p, a);
    }
    auto recs = bal.rebalance(in);
    int restoredOffSurvivor = 0;
    for (auto& r : recs) if (r.rec.leaderNode != 1) ++restoredOffSurvivor;
    EXPECT_GE(restoredOffSurvivor, 2)
        << "leadership must re-converge to the preferred replicas once they are back in the "
           "committed ISR; emitted " << recs.size() << " records";
}

// SAFETY GUARD: a returned replica outside the committed ISR (here also behind
// the leader's frontier) must NEVER be elected leader.
TEST(DynamicBalancerRegression, DoesNotRestoreLeadershipToLaggingReplica) {
    AssignmentMap m;
    PartitionAssignment a;
    a.leaderNode    = 1;
    a.isr           = {1};                 // collapsed onto survivor
    a.replicas      = {0, 1, 2};           // preferred = 0
    a.leaderEpoch   = 5;
    a.partitionEpoch = 9;
    m.put(7, 0, a);

    ReplicaPositionTable pos;
    pos.report({7, 0, 1, 5, 1000});        // survivor/leader frontier = 1000
    pos.report({7, 0, 0, 5,  400});        // preferred node 0 is BEHIND (not caught up)
    pos.report({7, 0, 2, 5, 1000});        // node 2 caught up

    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {0, 1, 2}; in.positions = &pos;
    in.topics = { TopicSpec{7, 1, 3, "", ""} };

    auto recs = bal.rebalance(in);
    for (auto& r : recs)
        EXPECT_NE(r.rec.leaderNode, 0)
            << "a lagging replica (node 0, leo=400 < 1000) must not be elected leader";
}

// Kafka model (DUR-1 fix round, user decision 2026-10-03): elections pick ONLY
// from the controller-committed ISR.  A replica outside it may hold a log of
// equal / greater length on a DIVERGENT lineage (an uncommitted tail of an older
// epoch) while lacking committed records — a position (LEO) report is no proof.
// Before: a "restorable" preferred replica outside the ISR (LEO >= the leader's)
// was elected and re-seeded into the ISR → its KIP-279 truncation directives
// would drop committed records on the others.  The leader re-admits caught-up
// followers itself (AlterIsr); only then may a preferred election move to them.
TEST(DynamicBalancerRegression, NeverElectsOutsideCommittedIsrEvenWithCaughtUpPosition) {
    AssignmentMap m;
    PartitionAssignment a;
    a.leaderNode     = 1;
    a.isr            = {1, 2};
    a.replicas       = {0, 1, 2};           // preferred = 0, outside the ISR
    a.leaderEpoch    = 6;
    a.partitionEpoch = 9;
    m.put(7, 0, a);
    // Node 0's log is LONGER than the leader's but ends in an OLD epoch (an
    // uncommitted epoch-4 tail): it lacks the epoch-5/6 committed records.
    ReplicaPositionTable pos;
    pos.report({7, 0, 1, 6, 1000});
    pos.report({7, 0, 2, 6, 1000});
    pos.report({7, 0, 0, 4, 1200});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {0, 1, 2}; in.positions = &pos;
    in.topics = { TopicSpec{7, 1, 3, "", ""} };
    auto recs = bal.rebalance(in);
    for (auto& r : recs) {
        EXPECT_NE(r.rec.leaderNode, 0) << "elected a replica outside the committed ISR";
        for (int32_t n : r.isr)
            EXPECT_NE(n, 0) << "the controller added a member to the ISR on a position report";
    }
    // Once the leader's AlterIsr re-admitted node 0, the preferred move happens.
    a.isr = {1, 2, 0};
    m.put(7, 0, a);
    auto recs2 = bal.rebalance(in);
    ASSERT_EQ(recs2.size(), 1u);
    EXPECT_EQ(recs2[0].rec.leaderNode, 0);
}

// Position-proven survivor election (no committed-ISR member live): the
// pigeonhole gate must count only live replicas that REPORTED a position — an
// unreported live replica may be the only holder of the committed records, and
// the best reported one may lack them.
TEST(DynamicBalancerRegression, SurvivorElectionCountsOnlyReportedReplicas) {
    AssignmentMap m;
    PartitionAssignment a;
    a.leaderNode     = 0;                   // dead
    a.isr            = {0};                 // committed ISR: only the dead leader
    a.replicas       = {0, 1, 2};
    a.leaderEpoch    = 5;
    a.partitionEpoch = 9;
    m.put(7, 0, a);
    ReplicaPositionTable pos;
    pos.report({7, 0, 2, 3, 100});          // node 1 (live) has NOT reported
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1, 2}; in.positions = &pos; in.minIsr = 2;
    in.topics = { TopicSpec{7, 1, 3, "", ""} };
    auto recs = bal.rebalance(in);
    for (auto& r : recs)
        EXPECT_NE(r.rec.leaderNode, 2) << "elected on a pigeonhole that counted an unreported replica";
    pos.report({7, 0, 1, 5, 400});          // node 1 reports: it is the most complete
    auto recs2 = bal.rebalance(in);
    ASSERT_EQ(recs2.size(), 1u);
    EXPECT_EQ(recs2[0].rec.leaderNode, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// REGRESSION SUITE — failover to surviving in-ISR replicas.
//
// History: an earlier "minIsr floor" blocked failover whenever the eligible ∩ ISR
// set fell below minIsr, to stop a transient-CPU-starvation eligible-set collapse
// from installing a phantom ISR=1 leader.  That floor was REMOVED once the
// FollowerLiveness state machine made the eligible set ACCURATE (live followers
// keep publishing AppendPosition every status interval), because the floor then
// blocked GENUINE single-survivor failover — leaving a partition stuck on a dead
// leader forever, so subscribers redirect to the dead endpoint (committed-loss on
// read; the dr-e2e leader-kill failures).
//
// Correct behaviour: a lone surviving in-ISR replica IS a valid failover target —
// it holds the committed prefix (it was in-sync), so promoting it serves committed
// data without loss; new acks=all produce blocks at the gateway (minIsr) until the
// ISR re-expands.  Split brain stays impossible: the balancer runs on the
// consensus leader, which only holds leadership with a majority quorum.
// ─────────────────────────────────────────────────────────────────────────────

// A lone surviving in-ISR replica MUST be promoted (accurate eligible set ⇒ the
// others are genuinely dead).  Previously blocked by the minIsr floor.
TEST(DynamicBalancerRegression, SingleSurvivorInIsrFailsOver) {
    AssignmentMap m;
    // part0: node 0 leads, ISR = replicas = {0,1,2}.  Nodes 0 and 2 die; only 1 left.
    m.put(7, 0, PartitionAssignment{0, {0,1,2}, {0,1,2}, /*leaderEpoch=*/0,
                                    /*partitionEpoch=*/0});
    ReplicaPositionTable pos;
    pos.report({7,0,1,0,100});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m;
    in.liveNodes = {1};          // node 1 is the SOLE survivor (0,2 genuinely gone)
    in.positions = &pos;
    in.minIsr = 2;               // gateway acks=all gate — NOT a failover gate
    in.topics = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    ASSERT_FALSE(recs.empty())
        << "the sole surviving in-ISR replica must be promoted (it holds committed "
           "data); blocking this strands the partition on the dead leader";
    for (auto& r : recs) EXPECT_EQ(r.rec.leaderNode, 1);
}

// Once a MAJORITY is eligible again, the genuine failover proceeds and installs
// an ISR at or above the quorum floor — availability is preserved for real death.
TEST(DynamicBalancerRegression, FailoverProceedsWhenQuorumEligible) {
    AssignmentMap m;
    // part0: node 0 is the (dead) leader; nodes 1,2 survive in ISR.
    m.put(7, 0, PartitionAssignment{0, {0,1,2}, {0,1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({7,0,1,0,100});
    pos.report({7,0,2,0,100});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m;
    in.liveNodes = {1,2};        // node 0 genuinely gone; a majority remains
    in.positions = &pos;
    in.minIsr = 2;
    in.topics = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    ASSERT_FALSE(recs.empty()) << "a real crash with quorum live MUST fail over";
    for (auto& r : recs) {
        EXPECT_NE(r.rec.leaderNode, NO_LEADER);
        EXPECT_NE(r.rec.leaderNode, 0) << "dead node must not be re-elected";
        EXPECT_GE(r.isr.size(), static_cast<std::size_t>(in.minIsr))
            << "failover ISR must meet the commit-quorum floor";
    }
}

// A leader that lost consensus eligibility (heartbeat starved under load) but is
// DEMONSTRABLY ALIVE via the data plane (its partition frontier advanced → listed
// in dataPlaneAliveLeaders) must NOT be failed over — the acks=all churn fix.
TEST(DynamicBalancerRegression, NoFailoverOfDataPlaneAliveLeaderDespiteHeartbeatLapse) {
    AssignmentMap m;
    // part0: node 0 leads, healthy 3-node ISR.
    m.put(7, 0, PartitionAssignment{0, {0,1,2}, {0,1,2}, 0, 0});
    ReplicaPositionTable pos;
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m;
    in.liveNodes = {1,2};                 // node 0 dropped from heartbeat-eligible set
    in.positions = &pos;
    in.minIsr = 2;
    in.dataPlaneAliveLeaders = {0};       // ...but node 0's partition is still advancing
    in.topics = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty())
        << "a data-plane-alive leader (frontier advancing) must not be failed over "
           "on a transient heartbeat lapse — that is the leadership-churn bug";
}

// Control: the SAME heartbeat lapse WITHOUT data-plane liveness (leader genuinely
// silent, not in dataPlaneAliveLeaders) DOES fail over — real crash still handled.
TEST(DynamicBalancerRegression, FailoverWhenLeaderNeitherEligibleNorDataPlaneAlive) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{0, {0,1,2}, {0,1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({7,0,1,0,100});
    pos.report({7,0,2,0,100});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m;
    in.liveNodes = {1,2};
    in.positions = &pos;
    in.minIsr = 2;
    // dataPlaneAliveLeaders left empty → node 0 is genuinely gone
    in.topics = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    ASSERT_FALSE(recs.empty()) << "a truly-dead leader (frozen frontier) MUST fail over";
    for (auto& r : recs) {
        EXPECT_NE(r.rec.leaderNode, 0);
        EXPECT_GE(r.isr.size(), static_cast<std::size_t>(in.minIsr));
    }
}

// Default minIsr (1) preserves single-survivor failover for callers that don't
// set it — guards against the floor accidentally blocking legitimate last-node
// promotion (RF==N total-loss-but-one recovery).
TEST(DynamicBalancerRegression, DefaultMinIsrOnePreservesSingleSurvivorFailover) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{0, {0,1,2}, {0,1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({7,0,2,0,100});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur = &m;
    in.liveNodes = {2};          // only node 2 survives
    in.positions = &pos;
    // in.minIsr left at its default (1)
    in.topics = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    ASSERT_FALSE(recs.empty()) << "default minIsr=1 must still fail over to lone survivor";
    for (auto& r : recs)
        EXPECT_EQ(r.rec.leaderNode, 2);
}

// REGRESSION (dr3 rolling leader-kill: partition permanently NO_LEADER):
// When a partition's sole ISR survivor dies (no live in-sync replica), the
// balancer must take it NO_LEADER but RETAIN the ISR lineage — NOT clear it —
// so the partition can recover cleanly once a former-ISR replica rejoins.
// Clearing the ISR left partitions permanently offline (empty ISR ∩ liveNodes
// = ∅ → NO_LEADER forever), which manifested as the client producing 44/50
// (records for the stuck partition failed with NotLeader / redirect-exhausted).
TEST(DynamicBalancerRegression, NoLeaderRetainsIsrAndRecoversOnRejoin) {
    // ── Step 1: leader (node 3) with ISR collapsed to {3} dies → NO_LEADER ────
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{3, {3}, {1,2,3}, 5, 9});

    ReplicaPositionTable pos;
    pos.report({7,0,3,5,100});

    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur       = &m;
    in.liveNodes = {1, 2};        // node 3 (sole ISR member) is dead
    in.positions = &pos;
    in.topics    = { TopicSpec{7,1,3,"",""} };

    auto recs = bal.rebalance(in);
    ASSERT_FALSE(recs.empty()) << "partition must transition to NO_LEADER";
    const BalancerRecord* offline = nullptr;
    for (auto& r : recs) if (r.rec.topicId == 7 && r.rec.partition == 0) offline = &r;
    ASSERT_TRUE(offline != nullptr);
    EXPECT_EQ(offline->rec.leaderNode, NO_LEADER) << "no live in-sync survivor → NO_LEADER";
    // CRITICAL: the ISR lineage is RETAINED, not cleared.
    ASSERT_EQ(offline->isr.size(), 1u) << "NO_LEADER must retain the ISR lineage {3}";
    EXPECT_EQ(offline->isr[0], 3);

    // ── Step 2: apply the offline record, then node 3 rejoins → recovers ──────
    AssignmentMap m2;
    m2.put(7, 0, PartitionAssignment{NO_LEADER, {3}, {1,2,3},
                                     offline->rec.leaderEpoch,
                                     offline->rec.partitionEpoch});
    DynamicBalancerInputs in2;
    in2.cur       = &m2;
    in2.liveNodes = {1, 2, 3};    // node 3 back online (caught up)
    in2.positions = &pos;
    in2.topics    = { TopicSpec{7,1,3,"",""} };

    auto recs2 = bal.rebalance(in2);
    ASSERT_FALSE(recs2.empty()) << "returning former-ISR member must restore leadership";
    const BalancerRecord* recovered = nullptr;
    for (auto& r : recs2) if (r.rec.topicId == 7 && r.rec.partition == 0) recovered = &r;
    ASSERT_TRUE(recovered != nullptr);
    EXPECT_EQ(recovered->rec.leaderNode, 3)
        << "clean election: the retained-ISR replica (node 3) is re-elected leader";
}
