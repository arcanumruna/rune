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
