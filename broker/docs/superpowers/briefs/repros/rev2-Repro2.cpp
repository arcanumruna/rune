#include "DrHarness.h"
using namespace rune::replication;
using namespace rune::replication::dr;
namespace { constexpr int kCap = 3000; }

// KIP-279 shape with CLEAN elections (all in controller ISR):
// A(1) epoch1 writes [0,8) committed + [8,10) uncommitted (A only).
// B(2) epoch2 (LEO 8) writes [8,12) uncommitted (B only).
// A re-elected epoch3 (LEO 10).  B probes requestedEpoch=2.
TEST(Review2, CleanKip279_AdoptionCandidateStallsLeaderForever) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);          // uncommitted on A only
    c.run(2);
    c.moveLeader(2);                     // epoch 2, ISR {1,2,3}
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&]{ return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);          // uncommitted on B only
    c.run(2);
    c.moveLeader(1);                     // epoch 3, A leader (LEO 10), ISR still {1,2,3}
    c.healAll();
    const bool w = c.waitWritable(kCap);
    std::cerr << "writable=" << w << " leo1=" << c.rep(1).logEndOffset()
              << " leo2=" << c.rep(2).logEndOffset() << " leo3=" << c.rep(3).logEndOffset()
              << " complete1=" << c.rep(1).isLeaderComplete() << "\n" << c.dump();
    EXPECT_TRUE(w) << "leader never completes (B's uncommitted epoch-2 tail held as adoption candidate)";
}

// Same, but B's uncommitted epoch-2 tail is exactly as long as A's: B is not
// ahead, A answers authoritatively (responseEpoch 1 < requested 2, endOffset 10)
// and B truncates to min(LEO, 10) = 10 — KIP-279 says min(10, B's own end of
// epoch 1 = 8).  B keeps divergent [8,10) and is credited as a copy.
TEST(Review2, CleanKip279_SameLengthTail_Diverges) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(2);
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&]{ return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(1);
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.run(200);
    auto a = c.ids(1), b = c.ids(2);
    size_t n = std::min(a.size(), b.size()), diff = 0;
    for (size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    std::cerr << "n1 vs n2 differ at " << diff << " of " << n << " positions\n" << c.dump();
    EXPECT_EQ(diff, 0u);
    for (NodeId id = 1; id <= 3; ++id) c.expectAckedPresent(id, "kip279");
}

// B2 stickiness: a same-epoch re-assert (a partitionEpoch-only ISR change
// re-applies becomeLeader/becomeFollower with the SAME leaderEpoch) clears
// both the follower's wedge and the leader's `diverged` slot.
TEST(Review2, SameEpochReassert_UnwedgesDivergedFollower) {
    DrConfig cfg;
    cfg.minIsr = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({2}), 3);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    ASSERT_EQ(c.produceAcked(5), 5);
    c.applyMetadata(2);
    c.run(300);
    ASSERT_GE(c.rep(2).wedgedAtOffset(), 0);
    ASSERT_FALSE(c.leaderIsrContains(2));
    ASSERT_EQ(c.produceAcked(20), 20);           // leader 3 LEO 35 > node 2 LEO 30
    // partitionEpoch-only bump: same leaderEpoch re-applied everywhere
    c.applyMetadata(3);
    c.applyMetadata(2);
    c.run(300);
    if (c.waitWritable(kCap)) (void)c.produceAcked(5);
    c.run(300);
    auto a = c.ids(3), b = c.ids(2);
    size_t n = std::min(a.size(), b.size()), diff = 0;
    for (size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    std::cerr << "wedged2=" << c.rep(2).wedgedAtOffset() << " leo2=" << c.rep(2).logEndOffset()
              << " isrHas2=" << c.leaderIsrContains(2) << " diff=" << diff << "/" << n << "\n";
    EXPECT_FALSE(c.leaderIsrContains(2)) << "diverged follower re-admitted after same-epoch re-assert";
    EXPECT_EQ(diff, 0u) << "silent divergence";
}
