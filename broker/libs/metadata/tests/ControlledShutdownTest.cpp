#include <rune/metadata/MembershipState.h>
#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/FailoverElection.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

TEST(ControlledShutdown, MovesLeadershipOffLiveNodeNeverUnclean) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos; pos.report({7,0,2,0,100});
    DynamicBalancer bal; DynamicBalancerInputs in;
    // node 1 is shutting down gracefully → excluded as a leader target but still alive
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,1,2,"",""} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].rec.leaderNode, 2) << "moved to in-ISR survivor, never unclean";
}
TEST(ControlledShutdown, CompleteWhenAllLiveConsumedTheMoves) {
    MembershipState mem; mem.onJoin(1,"h",1,10,0);
    mem.beginControlledShutdown(1, /*offset*/500);
    EXPECT_FALSE(mem.shutdownComplete(1, 400));
    EXPECT_TRUE (mem.shutdownComplete(1, 500));
    EXPECT_TRUE (mem.shutdownComplete(1, 600));
}

// ---------------------------------------------------------------------------
// New durability / mechanism tests
// ---------------------------------------------------------------------------

// Durability-critical: shutting-down node is the ONLY ISR member.
// Node 2 is live but NOT in ISR — it must NOT be elected (unclean).
// Expected outcome: NO_LEADER, wentNoLeader==true, recovering==false.
TEST(ControlledShutdown, ShutdownWithSoleIsrMemberGoesLeaderlessNotUnclean) {
    // Partition: leader=1, ISR={1}, replicas={1,2}.  Node 1 is draining.
    // eligibleNodes supplies {2} (node 1 excluded from eligible because it is
    // in ControlledShutdown; node 2 is live but never joined ISR).
    PartitionAssignment cur;
    cur.leaderNode  = 1;
    cur.isr         = {1};
    cur.replicas    = {1, 2};
    cur.leaderEpoch = 3;
    cur.partitionEpoch = 7;

    ReplicaPositionTable pos;
    // allowUnclean == false: the unclean path must remain closed on shutdown.
    ElectionInputs in{cur, /*liveNodes=*/{2}, &pos, /*allowUnclean=*/false};

    auto out = FailoverElection::elect(7, 0, in);

    EXPECT_TRUE (out.wentNoLeader)   << "sole-ISR-member draining → must go leaderless";
    EXPECT_EQ   (out.change.newLeader, NO_LEADER);
    EXPECT_FALSE(out.recovering)     << "must NOT perform an unclean election on shutdown";
}

// beginControlledShutdown sets state=ControlledShutdown, removes from
// eligibleNodes, but the node remains in liveNodes.
TEST(ControlledShutdown, BeginControlledShutdownSetsStateAndExcludesFromEligible) {
    MembershipState mem;
    // Join and catch up so the node is initially eligible.
    mem.onJoin(1, "h1", /*inc*/1, /*epoch*/10, /*nowNs*/0);
    mem.markCatchUpComplete(1);

    // Sanity: node 1 is live and eligible before drain.
    ASSERT_EQ(mem.liveNodes(0, 10'000).size(), 1u);
    ASSERT_EQ(mem.eligibleNodes(0, 10'000).size(), 1u);

    mem.beginControlledShutdown(1, /*offset*/200);

    // State must be ControlledShutdown.
    const auto* info = mem.find(1);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->state, NodeControlState::ControlledShutdown)
        << "beginControlledShutdown must set ControlledShutdown state";

    // Still appears in liveNodes (ControlledShutdown is not Fenced/ShutdownNow).
    auto live = mem.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 1u) << "draining node must still count as live";

    // Must NOT appear in eligibleNodes (state != Unfenced).
    auto eligible = mem.eligibleNodes(0, 10'000);
    EXPECT_TRUE(eligible.empty())
        << "draining node must be excluded from eligibleNodes";
}

// shutdownComplete returns false when there is no pending beginControlledShutdown
// record for the given nodeId.
TEST(ControlledShutdown, ShutdownCompleteFalseWhenNoPendingRecord) {
    MembershipState mem;
    mem.onJoin(1, "h1", 1, 10, 0);
    // No beginControlledShutdown called for node 1.
    EXPECT_FALSE(mem.shutdownComplete(1, 0))
        << "no pending record → shutdownComplete must return false";
    EXPECT_FALSE(mem.shutdownComplete(1, 1'000'000))
        << "no pending record → shutdownComplete must return false regardless of offset";
}

// beginControlledShutdown on an unknown nodeId is a no-op: no crash, no
// spurious entry created.
TEST(ControlledShutdown, BeginControlledShutdownUnknownNodeIsNoOp) {
    MembershipState mem;
    // nodeId 99 was never joined.
    EXPECT_NO_FATAL_FAILURE(mem.beginControlledShutdown(99, 100));
    // shutdownComplete must return false (no record was created).
    EXPECT_FALSE(mem.shutdownComplete(99, 100))
        << "unknown nodeId must not create a spurious pending record";
    // Membership table must remain empty.
    EXPECT_EQ(mem.size(), 0u);
}

// Controlled shutdown (2026-10-05): a node excluded as a leader (a stopping
// node) is never the target of a PREFERRED-leader election, even when it is
// replicas[0], live and in the ISR.
TEST(ControlledShutdownRegression, PreferredLeaderNeverMovesToAnExcludedNodeRegression) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{2, {2, 1, 3}, {1, 2, 3}, 4, 9});   // leader 2, preferred 1
    m.put(7, 1, PartitionAssignment{2, {2, 1, 3}, {2, 1, 3}, 4, 9});
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {1, 2, 3};
    in.topics = { TopicSpec{7, 2, 3, "", ""} };
    in.excludedLeaders = {{7, 0, 1}, {7, 1, 1}};
    for (const auto& r : bal.rebalance(in))
        EXPECT_NE(r.rec.leaderNode, 1) << "partition " << r.rec.partition << " moved to the excluded node";
    in.excludedLeaders.clear();
    bool moved = false;
    for (const auto& r : bal.rebalance(in)) moved = moved || r.rec.leaderNode == 1;
    EXPECT_TRUE(moved) << "control: without the exclusion the preferred election fires";
}

// Fix round 1 (D5): the SOFT avoid rule for stopping nodes.
TEST(ControlledShutdownRegression, AvoidedNodeHandsOffOrStaysOrSurvivesRegression) {
    DynamicBalancer bal;
    {   // a stopping leader hands off to a live in-sync replica, and leaves the ISR
        AssignmentMap m;
        m.put(7, 0, PartitionAssignment{1, {1, 2, 3}, {1, 2, 3}, 4, 9});
        DynamicBalancerInputs in; in.cur = &m; in.liveNodes = {1, 2, 3};
        in.topics = { TopicSpec{7, 1, 3, "", ""} }; in.avoidLeaders = {1};
        auto r = bal.rebalance(in);
        ASSERT_EQ(r.size(), 1u);
        EXPECT_NE(r[0].rec.leaderNode, 1);
        EXPECT_EQ(std::count(r[0].isr.begin(), r[0].isr.end(), 1), 0);
    }
    {   // sole in-sync replica: stays
        AssignmentMap m;
        m.put(7, 0, PartitionAssignment{1, {1}, {1, 2, 3}, 4, 9});
        DynamicBalancerInputs in; in.cur = &m; in.liveNodes = {1, 2, 3};
        in.topics = { TopicSpec{7, 1, 3, "", ""} }; in.avoidLeaders = {1};
        EXPECT_TRUE(bal.rebalance(in).empty());
    }
    {   // dead leader: a non-stopping survivor first (never the extra hop)
        AssignmentMap m;
        m.put(7, 0, PartitionAssignment{1, {1, 2, 3}, {1, 2, 3}, 4, 9});
        DynamicBalancerInputs in; in.cur = &m; in.liveNodes = {2, 3};
        in.topics = { TopicSpec{7, 1, 3, "", ""} }; in.avoidLeaders = {2};
        auto r = bal.rebalance(in);
        ASSERT_EQ(r.size(), 1u);
        EXPECT_EQ(r[0].rec.leaderNode, 3);
    }
    {   // dead leader, only the stopping survivor in sync: elected (not offline)
        AssignmentMap m;
        m.put(7, 0, PartitionAssignment{1, {1, 2}, {1, 2, 3}, 4, 9});
        DynamicBalancerInputs in; in.cur = &m; in.liveNodes = {2, 3};
        in.topics = { TopicSpec{7, 1, 3, "", ""} }; in.avoidLeaders = {2};
        auto r = bal.rebalance(in);
        ASSERT_EQ(r.size(), 1u);
        EXPECT_EQ(r[0].rec.leaderNode, 2);
    }
}

// Fix round 2 (N3): a stopping SOLE-ISR leader that is heartbeat-starved out of
// liveNodes but still serving (data-plane alive) keeps its partition — never
// failed over to NO_LEADER while it serves.
TEST(ControlledShutdownRegression, DataPlaneAliveStoppingSoleLeaderKeepsItsPartitionRegression) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{1, {1}, {1, 2, 3}, 4, 9});
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur = &m; in.liveNodes = {2, 3}; in.dataPlaneAliveLeaders = {1};
    in.topics = { TopicSpec{7, 1, 3, "", ""} }; in.avoidLeaders = {1};
    EXPECT_TRUE(bal.rebalance(in).empty()) << "a serving sole-ISR stopping leader was taken offline";
}
