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

// The candidate's own vote is unconditional (Aeron becomeCandidate) and the
// self-gate is gone: a node that ACKED its own removal (its durable knowledge
// excludes it) stands, votes for itself, and wins with one voter that missed
// the committed state -> leads __cluster_metadata without committed records.
TEST(ReReview3, SelfVoteOfExcludedNodeElectsReplicaMissingCommittedRecords) {
    DrConfig cfg; cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).internalSeedPendingForTest() &&
                                          !c.rep(1).isrProposalPendingForTest(); }, kCap));
    std::printf("seed:    n1=%s n2=%s n3=%s\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str());
    // 2 consensus-dead -> fenced out.  Node 2 receives + acks its own removal; the
    // frame to node 3 is lost (1->3 link blip).  Commits on {1,2}.
    c.cut(1, 3);
    c.cut(2, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).isrProposalPendingForTest() && !c.leaderIsrContains(2); }, kCap)) << c.dump();
    std::printf("shrunk:  n1=%s n2=%s n3=%s\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str());
    const auto* k2 = c.node(2).isrKnowledge->latest(-3, kPartId);
    ASSERT_TRUE(k2 && !k2->contains(2)) << "precondition: node 2 acked its own removal";
    const auto* k3 = c.node(3).isrKnowledge->latest(-3, kPartId);
    ASSERT_TRUE(k3 == nullptr || k3->contains(2)) << "precondition: node 3 missed it";
    // 2 stops fetching; 3's link heals but the next couple of anti-entropy
    // re-sends are lost too (same blip); records commit over {1,3}.
    c.cutBoth(1, 2);
    c.heal(1, 3);
    c.dropNext(Kind::InternalIsr, 2);
    ASSERT_EQ(c.produceAcked(5), 5);
    std::printf("acked:   n1=%s n2=%s n3=%s  LEO1=%lld LEO2=%lld LEO3=%lld\n", kn(c,1).c_str(), kn(c,2).c_str(), kn(c,3).c_str(),
                (long long)c.rep(1).logEndOffset(), (long long)c.rep(2).logEndOffset(), (long long)c.rep(3).logEndOffset());
    k3 = c.node(3).isrKnowledge->latest(-3, kPartId);
    ASSERT_TRUE(k3 == nullptr || k3->contains(2)) << "precondition: node 3 still missed it";
    ASSERT_LT(c.rep(2).logEndOffset(), c.rep(1).logEndOffset()) << "precondition: node 2 lacks committed records";
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    const NodeId nl = c.consensusElect(2);
    std::printf("elected: %d\n", nl);
    EXPECT_NE(nl, 2) << "node 2 (outside the newest COMMITTED metadata ISR, missing committed records) won "
                        "with its own unconditional self-vote + node 3's stale grant";
    if (nl == 2) {
        ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
        c.driveUntil([&] { return c.converged(); }, kCap);
        c.expectAckedPresent(3, "self-vote");   // acked records truncated?
        c.expectAckedPresent(2, "self-vote");
    }
}

// Term rule starvation: a DENIED RequestVote bumps the voter's candidateTermId
// (Election.cpp onRequestVote deny branch, any non-Init state incl. Closed).
// Modelled with consensusVote().  The live leader at term e can no longer
// commit any metadata ISR change: a dead follower is never shrunk -> HW pinned.
TEST(ReReview3, DeniedVoteBumpFreezesInternalIsrOfLiveLeader) {
    DrConfig cfg; cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).internalSeedPendingForTest() &&
                                          !c.rep(1).isrProposalPendingForTest(); }, kCap));
    // Node 3 (e.g. consensus-partitioned from 1, or ineligible) stands at e+1 and
    // is denied by 1 and 2 -> both bump candidateTermId to e+1 (Closed state).
    c.consensusVote(1, c.epoch() + 1);
    c.consensusVote(2, c.epoch() + 1);
    // Node 3 then crashes for good.  1 and 2 (a majority) are up, 1 still leads.
    c.crash(3, CrashKind::ProcessKill);
    c.run(400);   // far past replicaLagMaxMs + fencing
    std::printf("n1=%s n2=%s leaderIsrHas3=%d pending=%d\n", kn(c,1).c_str(), kn(c,2).c_str(),
                (int)c.leaderIsrContains(3), (int)c.rep(1).isrProposalPendingForTest());
    EXPECT_FALSE(c.leaderIsrContains(3)) << "dead node 3 never shrunk: the live leader's ISR change is refused by every node";
    const int n = c.produce(5);
    EXPECT_EQ(n, 5);
    EXPECT_TRUE(c.driveUntil([&] { return c.rep(1).highWatermark() >= c.rep(1).logEndOffset(); }, 1000))
        << "HW pinned by the dead member: metadata log frozen with a live majority\n" << c.dump();
}
