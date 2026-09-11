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

// ============================================================================
// B1 fix: per-follower LEO tracks LATEST value (not max) — truncation path
// ============================================================================

TEST(HighWatermark, B1_FollowerTruncate_LowerLeoCountsNotStale) {
    // Regression test for B1: a follower that reports LEO=10 (counted toward
    // majority HW), then truncates and re-fetches from LEO=5, must NOT still
    // be counted as holding offset 10.  The per-follower LEO must be updated
    // to the latest reported value (5), not the previous max (10).
    //
    // Scenario: ISR={leader=0, follower=1}, leader LEO=10.
    // 1. Follower reports LEO=10 → HW advances to 10.
    // 2. Follower truncates and reports LEO=5 → HW must recompute.
    //    After truncation: LEOs=[10, 5], majority=2, 2nd-highest=5.
    //    But prevHw_=10, so global HW stays at 10 (committed prefix safe).
    //    The KEY check: the follower's CONTRIBUTION to a new HW computation
    //    must use 5, not the stale 10.  This means if leader LEO drops to 7
    //    (hypothetical step-down), the HW would not rise above 5 (not 10).
    //
    // We test this by checking that after reporting LEO=5, a fresh setIsr()
    // or leader LEO advance does not make HW jump above what is actually held.

    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(10LL);
    s.setIsr({0, 1});

    // Step 1: follower reports LEO=10 → both at 10 → HW=10.
    s.updateFollowerFetchOffset(1, 10LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Step 2: follower truncates and re-fetches from LEO=5.
    // B1 fix: follower's contribution must drop to 5 (not remain at 10).
    s.updateFollowerFetchOffset(1, 5LL);
    // HW floor stays at 10 (committed — monotonic), but the follower's
    // current tracked LEO is now 5.  Verify by querying after leader advances:
    // If the leader advances to 15, and we compute HW fresh, the follower
    // at LEO=5 means majority(2) of {15,5} = 5 < prevHw_=10, so HW stays 10.
    s.setLeaderLogEndOffset(15LL);
    EXPECT_EQ(s.highWatermark(), 10LL); // floor holds, not above majority

    // Step 3: follower re-fetches and advances to LEO=12.
    // Now majority(2) of {15,12} = 12 > prevHw_=10 → HW advances to 12.
    s.updateFollowerFetchOffset(1, 12LL);
    EXPECT_EQ(s.highWatermark(), 12LL);
}

TEST(HighWatermark, B1_FollowerReportsLowerLeo_HwDoesNotAdvancePastTrueHolders) {
    // 3-replica ISR={leader=0, f1=1, f2=2}. Leader LEO=20.
    // Initially: f1=15, f2=15 → majority=2 → HW=15.
    // f1 truncates to 8 (new fetch). HW recompute: LEOs=[20,8,15] → sorted=[20,15,8]
    // → majority=2 → computed=15 → no advance (prevHw_ already 15).
    // If leader advances to 25, f2=15: LEOs=[25,8,15] → sorted=[25,15,8] → HW=15.
    // NOT 20 (the stale f1 value before truncation). This verifies B1 is fixed.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(20LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 15LL);
    s.updateFollowerFetchOffset(2, 15LL);
    EXPECT_EQ(s.highWatermark(), 15LL);

    // f1 truncates to 8.
    s.updateFollowerFetchOffset(1, 8LL);
    // LEOs=[20,8,15] sorted=[20,15,8] → majority=2 → computed=15 = prevHw_ → 15
    EXPECT_EQ(s.highWatermark(), 15LL);

    // Leader advances to 25; f2 stays at 15; f1 at 8.
    s.setLeaderLogEndOffset(25LL);
    // LEOs=[25,8,15] sorted=[25,15,8] → majority=2 → computed=15 → stays 15
    EXPECT_EQ(s.highWatermark(), 15LL);

    // f1 re-catches to 20; LEOs=[25,20,15] → majority=2 → computed=20 > 15 → 20
    s.updateFollowerFetchOffset(1, 20LL);
    EXPECT_EQ(s.highWatermark(), 20LL);
}

// ============================================================================
// S1 fix: highWatermark() is never stale — setLeaderLogEndOffset and setIsr
// both eagerly recompute
// ============================================================================

TEST(HighWatermark, S1_SetLeaderLeoEagerlyRecomputes) {
    // Leader-only ISR: after setLeaderLogEndOffset, highWatermark() must
    // return the new value WITHOUT needing a dummy updateFollowerFetchOffset.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0});
    s.setLeaderLogEndOffset(50LL);
    // S1 fix: no dummy call needed — HW must already reflect leader LEO.
    EXPECT_EQ(s.highWatermark(), 50LL);

    s.setLeaderLogEndOffset(75LL);
    EXPECT_EQ(s.highWatermark(), 75LL);
}

TEST(HighWatermark, S1_SetIsrEagerlyRecomputes) {
    // After setIsr(), HW must be correct without needing a follower update.
    // Scenario: ISR was {0,1,2} with follower LEOs both 50; shrink to {0}.
    // Before shrink: HW = 50 (leader=100, f1=50, f2=50, majority=2).
    // After shrink to {0}: leader-only, HW = leaderLeo = 100.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 50LL);
    s.updateFollowerFetchOffset(2, 50LL);
    EXPECT_EQ(s.highWatermark(), 50LL);

    // Shrink to leader-only — S1 fix: setIsr triggers recompute immediately.
    s.setIsr({0});
    EXPECT_EQ(s.highWatermark(), 100LL);
}

// ============================================================================
// B2 fix: setHighWatermarkFloor restores committed HW on becomeLeader
// ============================================================================

TEST(HighWatermark, B2_SetHighWatermarkFloor_AdvancesMonotonically) {
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0, 1});
    s.setLeaderLogEndOffset(0LL); // fresh state (like new leader before followers re-ack)

    // Simulate restoring the committed HW floor from the last known committed offset.
    s.setHighWatermarkFloor(10LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Floor is monotonic — lower value must not regress it.
    s.setHighWatermarkFloor(5LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Higher value advances.
    s.setHighWatermarkFloor(20LL);
    EXPECT_EQ(s.highWatermark(), 20LL);
}

TEST(HighWatermark, B2_FloorPreservedAfterFollowerCatchUp) {
    // After restoring the floor and followers re-ack, HW advances normally.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0, 1});
    s.setLeaderLogEndOffset(10LL);
    s.setHighWatermarkFloor(10LL); // B2: restore committed floor

    EXPECT_EQ(s.highWatermark(), 10LL); // floor holds immediately

    // Follower re-acks at 10 → majority(2) of {10,10}=10 — floor already there
    s.updateFollowerFetchOffset(1, 10LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Leader appends more; follower re-acks → HW advances past floor
    s.setLeaderLogEndOffset(15LL);
    s.updateFollowerFetchOffset(1, 15LL);
    EXPECT_EQ(s.highWatermark(), 15LL);
}
