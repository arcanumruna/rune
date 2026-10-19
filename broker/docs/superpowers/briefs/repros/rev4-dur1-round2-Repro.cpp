#include "DrHarness.h"
#include <gtest/gtest.h>
using namespace rune::replication;
using namespace rune::replication::dr;
constexpr int kCap = 3000;

static std::string kn(DrCluster& c, NodeId id) {
    const auto* s = c.node(id).isrKnowledge->latest(-3, kPartId);
    if (!s) return "none";
    std::string r = "(" + std::to_string(s->leaderEpoch) + "," + std::to_string(s->version) + ")={";
    for (int i = 0; i < s->count; ++i) r += std::to_string(s->members[i]) + " ";
    return r + "}";
}

// Stale self-knowledge + self-gate => no node nominates although a majority is up and
// node 3 is fully caught up and a member of the newest COMMITTED internal ISR.
TEST(ReReview2, StaleSelfKnowledgeDeadlocksConsensus) {
    DrConfig cfg; cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).internalSeedPendingForTest(); }, kCap));
    std::printf("after seed: n1=%s n2=%s n3=%s\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str());
    // 3 consensus-dead (data plane fine): fenced out; it RECEIVES that state.
    c.cut(3, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).isrProposalPendingForTest() && !c.leaderIsrContains(3); }, kCap));
    c.run(20);
    std::printf("3 fenced:   n1=%s n2=%s n3=%s\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str());
    // 3's link from the leader drops (its fetches still reach 1); 3 live again, 2 now consensus-dead.
    c.cut(1, 3);
    c.heal(3, kControllerAddr);
    c.cut(2, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).isrProposalPendingForTest() && c.leaderIsrContains(3) && !c.leaderIsrContains(2); }, kCap)) << c.dump();
    c.run(50);
    std::printf("swap:       n1=%s n2=%s n3=%s  L.LEO=%lld n3.LEO=%lld\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str(),
                (long long)c.rep(1).logEndOffset(), (long long)c.rep(3).logEndOffset());
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    const NodeId nl = c.consensusElect(3);
    EXPECT_GE(nl, 0) << "headless: nobody stands although 2 of 3 are up and node 3 is caught up";
}
