#include <rune/metadata/MembershipState.h>
#include <rune/metadata/DynamicBalancer.h>
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
