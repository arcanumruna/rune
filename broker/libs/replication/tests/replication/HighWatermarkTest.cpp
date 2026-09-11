// ---------------------------------------------------------------------------
// HighWatermarkTest.cpp — Unit tests for HighWatermark.h (ReplicaLeaderState).
//
// Tests cover:
//   • Single-replica ISR  → HW == leader LEO
//   • 3-replica ISR (leader + 2 followers) → HW = 2nd-highest (majority=2)
//   • HW advances as followers fetch forward
//   • HW is monotonic (stale/backward follower LEO does NOT lower HW)
//   • updateFollowerFetchOffset is monotonic per follower
//   • 5-replica ISR → majority=3 → HW = 3rd-highest
//   • ISR shrink (setIsr fewer members) recomputes majority correctly
//   • Leader-only ISR → HW = leader LEO
//   • ISR with unknown followers defaults to 0
//   • HW never regresses after ISR changes
//   • committedOffset == highWatermark
//   • Leader LEO query
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <rune/replication/HighWatermark.h>

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Helper: build a leader state with a given ISR (leader id always 0).
// ---------------------------------------------------------------------------
static ReplicaLeaderState makeState(std::vector<NodeId> isr, Offset leaderLeo = 0) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(leaderLeo);
    s.setIsr(std::move(isr));
    return s;
}

// ============================================================================
// Single-replica ISR (leader only)
// ============================================================================

TEST(HighWatermark, SingleReplicaIsrHwEqualsLeaderLeo) {
    // ISR = {leader}; majority = 1; HW = LEO
    auto s = makeState({0}, 100LL);
    // Trigger recomputation via a dummy update is not needed — we can call
    // setLeaderLogEndOffset + setIsr and then updateFollowerFetchOffset
    // to force a recompute.  Actually, the class recomputes on updateFollower,
    // but for leader-only we must call setLeaderLogEndOffset then a dummy
    // updateFollowerFetchOffset on a non-ISR member to trigger.
    // Simpler: expose via a fresh state build then call helper update.
    // Since the ISR is {0} (leader) only, majority=1, HW=leaderLeo.
    // We need to force a recompute: call updateFollowerFetchOffset on any node.
    s.updateFollowerFetchOffset(1, 0LL); // non-ISR member; forces recompute
    EXPECT_EQ(s.highWatermark(), 100LL);
}

TEST(HighWatermark, LeaderOnlyIsrFollowersDontMatter) {
    auto s = makeState({0}, 500LL);
    s.updateFollowerFetchOffset(1, 999LL); // follower 1 not in ISR
    s.updateFollowerFetchOffset(2, 999LL); // follower 2 not in ISR
    EXPECT_EQ(s.highWatermark(), 500LL);
}

TEST(HighWatermark, LeaderOnlyIsrHwTrackingLeaderLeo) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0});
    s.setLeaderLogEndOffset(10LL);
    s.updateFollowerFetchOffset(1, 0LL); // force recompute

    EXPECT_EQ(s.highWatermark(), 10LL);

    s.setLeaderLogEndOffset(20LL);
    s.updateFollowerFetchOffset(1, 0LL);
    EXPECT_EQ(s.highWatermark(), 20LL);
}

TEST(HighWatermark, CommittedOffsetEqualsHighWatermark) {
    auto s = makeState({0}, 42LL);
    s.updateFollowerFetchOffset(99, 0LL);
    EXPECT_EQ(s.committedOffset(), s.highWatermark());
}

// ============================================================================
// 3-replica ISR: leader + 2 followers, majority = 2
// ============================================================================

TEST(HighWatermark, ThreeReplicaHwIs2ndHighest) {
    // ISR={0,1,2}: LEOs=[100, 90, 80] → sorted desc=[100,90,80] → majority=2 → HW=90
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 90LL);
}

TEST(HighWatermark, ThreeReplicaHwBothFollowersBehind) {
    // LEOs=[100, 50, 50] → sorted=[100,50,50] → majority=2 → HW=50
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 50LL);
    s.updateFollowerFetchOffset(2, 50LL);
    EXPECT_EQ(s.highWatermark(), 50LL);
}

TEST(HighWatermark, ThreeReplicaHwFollowersEqualLeader) {
    // LEOs=[100,100,100] → HW=100
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 100LL);
    EXPECT_EQ(s.highWatermark(), 100LL);
}

TEST(HighWatermark, ThreeReplicaHwUnknownFollowerDefaultsToZero) {
    // ISR={0,1,2}: leader=100, follower1=90, follower2 unknown → 0
    // LEOs=[100,90,0] → sorted=[100,90,0] → majority=2 → HW=90
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL);
    // follower 2 never reported → defaults to 0
    EXPECT_EQ(s.highWatermark(), 90LL);
}

TEST(HighWatermark, ThreeReplicaBothFollowersUnknown) {
    // ISR={0,1,2}: leader=100, followers unknown → LEOs=[100,0,0]
    // sorted=[100,0,0] → majority=2 → HW=0
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(99, 0LL); // trigger recompute with non-ISR node
    EXPECT_EQ(s.highWatermark(), 0LL);
}

// ============================================================================
// HW advances as followers fetch forward
// ============================================================================

TEST(HighWatermark, HwAdvancesAsFollowersCatchUp) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});

    // Initially both followers unknown → HW = 0
    s.updateFollowerFetchOffset(1, 0LL);
    EXPECT_EQ(s.highWatermark(), 0LL);

    // follower 1 advances to 50
    s.updateFollowerFetchOffset(1, 50LL);
    // LEOs=[100,50,0] → majority=2 → HW=50
    EXPECT_EQ(s.highWatermark(), 50LL);

    // follower 2 advances to 75
    s.updateFollowerFetchOffset(2, 75LL);
    // LEOs=[100,75,50] → majority=2 → HW=75
    EXPECT_EQ(s.highWatermark(), 75LL);

    // follower 1 advances to 90
    s.updateFollowerFetchOffset(1, 90LL);
    // LEOs=[100,90,75] → majority=2 → HW=90
    EXPECT_EQ(s.highWatermark(), 90LL);

    // follower 2 reaches 100 (leader LEO)
    s.updateFollowerFetchOffset(2, 100LL);
    // LEOs=[100,100,90] → HW=100
    EXPECT_EQ(s.highWatermark(), 100LL);
}

TEST(HighWatermark, LeaderLeoAdvancesHwToMajority) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0, 1, 2});

    // Followers caught up to 50
    s.setLeaderLogEndOffset(50LL);
    s.updateFollowerFetchOffset(1, 50LL);
    s.updateFollowerFetchOffset(2, 50LL);
    EXPECT_EQ(s.highWatermark(), 50LL);

    // Leader advances to 100; followers stay at 50
    s.setLeaderLogEndOffset(100LL);
    s.updateFollowerFetchOffset(1, 50LL); // no change — still 50
    // LEOs=[100,50,50] → HW=50 (majority=2, 2nd highest=50)
    EXPECT_EQ(s.highWatermark(), 50LL);

    // follower 1 advances to 80
    s.updateFollowerFetchOffset(1, 80LL);
    // LEOs=[100,80,50] → HW=80
    EXPECT_EQ(s.highWatermark(), 80LL);
}

// ============================================================================
// HW is monotonic — never regresses
// ============================================================================

TEST(HighWatermark, HwMonotonicAfterFollowerRegression) {
    // updateFollowerFetchOffset is monotonic: a lower value is ignored.
    // Additionally, prevHw_ ensures HW never decreases globally.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});

    // Advance to HW=90
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 90LL);

    // "Late" update: follower 1 tries to report 50 (stale message)
    s.updateFollowerFetchOffset(1, 50LL); // must be ignored (monotonic)
    // follower 1 is still tracked at 90; HW must remain 90
    EXPECT_EQ(s.highWatermark(), 90LL);
}

TEST(HighWatermark, HwMonotonicAfterIsrShrink) {
    // After shrinking the ISR, HW must not decrease below prevHw_.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});

    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 90LL);

    // ISR shrinks to {0, 2}: only leader(100) + follower2(80) → majority=2 → HW=80.
    // But 80 < prevHw_(90), so monotonic floor keeps HW at 90.
    s.setIsr({0, 2});
    s.updateFollowerFetchOffset(2, 80LL); // trigger recompute
    EXPECT_GE(s.highWatermark(), 90LL);
}

TEST(HighWatermark, HwMonotonicGlobalFloor) {
    // Explicit test: after HW reaches X, no subsequent event may lower it.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(200LL);
    s.setIsr({0, 1});

    s.updateFollowerFetchOffset(1, 200LL);
    EXPECT_EQ(s.highWatermark(), 200LL);

    // Follower 1 sends a stale fetch at 10 — must be ignored.
    s.updateFollowerFetchOffset(1, 10LL);
    EXPECT_EQ(s.highWatermark(), 200LL);
}

TEST(HighWatermark, UpdateFollowerFetchOffsetIsMonotonicPerFollower) {
    // updateFollowerFetchOffset must track the MAX, not overwrite.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});

    s.updateFollowerFetchOffset(1, 60LL);
    s.updateFollowerFetchOffset(1, 30LL); // stale — must not lower follower1's LEO
    // follower2 unknown → 0; LEOs=[100,60,0] → majority=2 → HW=60
    EXPECT_EQ(s.highWatermark(), 60LL);
}

// ============================================================================
// 5-replica ISR: majority = 3 → HW = 3rd-highest
// ============================================================================

TEST(HighWatermark, FiveReplicaMajority3) {
    // ISR={0,1,2,3,4}: LEOs=[100,90,80,70,60] → majority=3 → HW=80
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    s.updateFollowerFetchOffset(3, 70LL);
    s.updateFollowerFetchOffset(4, 60LL);
    EXPECT_EQ(s.highWatermark(), 80LL);
}

TEST(HighWatermark, FiveReplicaAllCaughtUp) {
    // LEOs all 100 → HW=100
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 100LL);
    s.updateFollowerFetchOffset(3, 100LL);
    s.updateFollowerFetchOffset(4, 100LL);
    EXPECT_EQ(s.highWatermark(), 100LL);
}

TEST(HighWatermark, FiveReplicaMajority3TwoFollowersBehind) {
    // LEOs=[100,100,100,50,0] → sorted=[100,100,100,50,0] → majority=3 → HW=100
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 100LL);
    // followers 3 and 4 unknown → 0
    EXPECT_EQ(s.highWatermark(), 100LL);
}

TEST(HighWatermark, FiveReplicaMajority3ThreeFollowersBehind) {
    // LEOs=[100,100,50,0,0] → sorted=[100,100,50,0,0] → HW=50
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 50LL);
    // followers 3 and 4 unknown → 0
    EXPECT_EQ(s.highWatermark(), 50LL);
}

// ============================================================================
// ISR shrink — majority recomputed correctly
// ============================================================================

TEST(HighWatermark, IsrShrinkFrom5To3RecomputesMajority) {
    // Start 5 replicas; shrink to 3; re-check HW with new majority.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    s.updateFollowerFetchOffset(3, 70LL);
    s.updateFollowerFetchOffset(4, 60LL);
    EXPECT_EQ(s.highWatermark(), 80LL); // majority=3, 3rd-highest=80

    // Shrink ISR to {0,1,2}: LEOs=[100,90,80] → majority=2 → computed=90
    // But prevHw_=80; 90 > 80 → HW advances to 90.
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL); // trigger recompute
    EXPECT_EQ(s.highWatermark(), 90LL);
}

TEST(HighWatermark, IsrShrinkFrom3To1) {
    // Shrink to leader-only ISR: HW = leaderLeo
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 50LL);
    s.updateFollowerFetchOffset(2, 30LL);
    EXPECT_EQ(s.highWatermark(), 50LL);

    // Shrink to {0}; majority=1; HW = leaderLeo = 100
    s.setIsr({0});
    s.updateFollowerFetchOffset(1, 50LL); // non-ISR, trigger recompute
    EXPECT_EQ(s.highWatermark(), 100LL);
}

// ============================================================================
// 2-replica ISR (leader + 1 follower), majority = 2 → HW = min of the two
// ============================================================================

TEST(HighWatermark, TwoReplicaMajority2) {
    // ISR={0,1}: LEOs=[100,70] → majority=2 → HW = 70 (the 2nd = min)
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1});
    s.updateFollowerFetchOffset(1, 70LL);
    EXPECT_EQ(s.highWatermark(), 70LL);
}

TEST(HighWatermark, TwoReplicaBothAtSameOffset) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(50LL);
    s.setIsr({0, 1});
    s.updateFollowerFetchOffset(1, 50LL);
    EXPECT_EQ(s.highWatermark(), 50LL);
}

// ============================================================================
// Leader LEO accessor
// ============================================================================

TEST(HighWatermark, LeaderLogEndOffsetQuery) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(123LL);
    EXPECT_EQ(s.leaderLogEndOffset(), 123LL);
    s.setLeaderLogEndOffset(456LL);
    EXPECT_EQ(s.leaderLogEndOffset(), 456LL);
}

// ============================================================================
// Initial state before any updates
// ============================================================================

TEST(HighWatermark, InitialHwIsZero) {
    ReplicaLeaderState s;
    EXPECT_EQ(s.highWatermark(), 0LL);
    EXPECT_EQ(s.committedOffset(), 0LL);
}

TEST(HighWatermark, EmptyIsrHwIsZero) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({});
    s.updateFollowerFetchOffset(0, 100LL); // trigger recompute
    EXPECT_EQ(s.highWatermark(), 0LL);
}
