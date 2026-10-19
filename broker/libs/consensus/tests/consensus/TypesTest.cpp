#include <gtest/gtest.h>
#include <rune/consensus/Types.h>

using namespace rune::consensus;

TEST(Types, DefaultAliases) {
    Term  t = 0;
    Index i = 0;
    NodeId n = kNoVote;
    EXPECT_EQ(t, 0);
    EXPECT_EQ(i, 0);
    EXPECT_EQ(n, -1);
}

TEST(Types, LogEntry) {
    LogEntry e{3, "set x=1"};
    EXPECT_EQ(e.term, 3);
    EXPECT_EQ(e.command, "set x=1");
}

TEST(Types, RaftRole) {
    RaftRole r = RaftRole::Follower;
    EXPECT_NE(r, RaftRole::Leader);
    r = RaftRole::Candidate;
    EXPECT_EQ(r, RaftRole::Candidate);
}

TEST(ClusterConfig, Majority3) {
    ClusterConfig cfg{1, {1, 2, 3}};
    EXPECT_EQ(cfg.size(), 3);
    EXPECT_EQ(cfg.majority(), 2);
}

TEST(ClusterConfig, Majority5) {
    ClusterConfig cfg{1, {1, 2, 3, 4, 5}};
    EXPECT_EQ(cfg.size(), 5);
    EXPECT_EQ(cfg.majority(), 3);
}

TEST(ClusterConfig, IsMember) {
    ClusterConfig cfg{1, {1, 2, 3}};
    EXPECT_TRUE(cfg.isMember(1));
    EXPECT_TRUE(cfg.isMember(2));
    EXPECT_TRUE(cfg.isMember(3));
    EXPECT_FALSE(cfg.isMember(4));
    EXPECT_FALSE(cfg.isMember(kNoVote));
}
