#include <rune/node/WorkerReplicationValidator.h>

#include <gtest/gtest.h>

using rune::node::validateSymmetricWorkerCount;

TEST(WorkerReplicationValidator, AcceptsEqualCounts) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,4}});
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidator, ToleratesUnknownPeerCount) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,0}});   // peer 2 not yet advertised
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidatorRegression, RejectsMismatchNamingNode) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,2}});
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("node 2"), std::string::npos);
    EXPECT_NE(r.error.find("2"), std::string::npos);   // the offending count
}
TEST(WorkerReplicationValidatorRegression, RejectsOnMembershipChange) {
    // simulate a peer rejoining with a new count (same function, new peer list)
    auto r = validateSymmetricWorkerCount(4, {{1,4},{3,8}});
    EXPECT_FALSE(r.ok);
}
