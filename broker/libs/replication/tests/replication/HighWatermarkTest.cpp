// ---------------------------------------------------------------------------
// HighWatermarkTest.cpp — Unit tests for HighWatermark.h (ReplicaLeaderState).
//
// Tests cover:
//   • Single-replica ISR  → HW == leader LEO
//   • 3-replica ISR (leader + 2 followers) → HW = min LEO over the ISR
//   • HW advances as followers fetch forward
//   • HW is monotonic (stale/backward follower LEO does NOT lower HW)
//   • updateFollowerFetchOffset is monotonic per follower
//   • 5-replica ISR → HW = min LEO over the ISR
//   • ISR shrink (setIsr fewer members) recomputes the ISR minimum
//   • Leader-only ISR → HW = leader LEO
//   • ISR with unknown followers defaults to 0
//   • HW never regresses after ISR changes
//   • committedOffset == highWatermark
//   • Leader LEO query
//
// NOTE (Kafka model, user decision 2026-10-03): HW = min LEO over the ISR in
// every durability mode (the majority-of-ISR rule is retired).  The per-step
// arithmetic comments below use the min rule; "Kafka model" marks the EXPECTs
// that changed when the rule did.
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
    // ISR = {leader}; HW = min over {leader} = LEO
    auto s = makeState({0}, 100LL);
    // Trigger recomputation via a dummy update is not needed — we can call
    // setLeaderLogEndOffset + setIsr and then updateFollowerFetchOffset
    // to force a recompute.  Actually, the class recomputes on updateFollower,
    // but for leader-only we must call setLeaderLogEndOffset then a dummy
    // updateFollowerFetchOffset on a non-ISR member to trigger.
    // Simpler: expose via a fresh state build then call helper update.
    // Since the ISR is {0} (leader) only, HW = min over {leader} = leaderLeo.
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
// 3-replica ISR: leader + 2 followers, HW = min of the three
// ============================================================================

TEST(HighWatermarkRegression, ThreeReplicaHwIsMinOverIsr) {
    // ISR={0,1,2}: LEOs=[100, 90, 80] → HW = min = 80
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, ThreeReplicaHwBothFollowersBehind) {
    // LEOs=[100, 50, 50] → HW = min = 50
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
    // LEOs=[100,90,0] → HW = min = 0
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL);
    // follower 2 never reported → defaults to 0
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, ThreeReplicaBothFollowersUnknown) {
    // ISR={0,1,2}: leader=100, followers unknown → LEOs=[100,0,0]
    // HW = min = 0
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
    // LEOs=[100,50,0] → HW = min = 0
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR

    // follower 2 advances to 75
    s.updateFollowerFetchOffset(2, 75LL);
    // LEOs=[100,75,50] → HW = min = 50
    EXPECT_EQ(s.highWatermark(), 50LL);   // Kafka model: HW = min LEO over the ISR

    // follower 1 advances to 90
    s.updateFollowerFetchOffset(1, 90LL);
    // LEOs=[100,90,75] → HW = min = 75
    EXPECT_EQ(s.highWatermark(), 75LL);   // Kafka model: HW = min LEO over the ISR

    // follower 2 reaches 100 (leader LEO)
    s.updateFollowerFetchOffset(2, 100LL);
    // LEOs=[100,100,90] → HW=100
    EXPECT_EQ(s.highWatermark(), 90LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, LeaderLeoAdvancesHwToIsrMin) {
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
    // LEOs=[100,50,50] → HW=50 (min)
    EXPECT_EQ(s.highWatermark(), 50LL);

    // follower 1 advances to 80
    s.updateFollowerFetchOffset(1, 80LL);
    // LEOs=[100,80,50] → HW=80
    EXPECT_EQ(s.highWatermark(), 50LL);   // Kafka model: HW = min LEO over the ISR
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
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR

    // "Late" update: follower 1 tries to report 50 (stale message)
    s.updateFollowerFetchOffset(1, 50LL); // must be ignored (monotonic)
    // follower 1 is still tracked at 90; HW must remain 90
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, HwMonotonicAfterIsrShrink) {
    // After shrinking the ISR, HW must not decrease below prevHw_.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});

    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR

    // ISR shrinks to {0, 2}: leader(100) + follower2(80) → HW = min = 80
    // (unchanged: follower2 was already the minimum).
    s.setIsr({0, 2});
    s.updateFollowerFetchOffset(2, 80LL); // trigger recompute
    EXPECT_GE(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR
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
    // follower2 unknown → 0; LEOs=[100,60,0] → HW = min = 0
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
}

// ============================================================================
// 5-replica ISR: HW = min of the five
// ============================================================================

TEST(HighWatermark, FiveReplicaHwIsMinOverIsr) {
    // ISR={0,1,2,3,4}: LEOs=[100,90,80,70,60] → HW = min = 60
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    s.updateFollowerFetchOffset(3, 70LL);
    s.updateFollowerFetchOffset(4, 60LL);
    EXPECT_EQ(s.highWatermark(), 60LL);   // Kafka model: HW = min LEO over the ISR
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
    // LEOs=[100,100,100,50,0] → HW = min = 0
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 100LL);
    // followers 3 and 4 unknown → 0
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, FiveReplicaMajority3ThreeFollowersBehind) {
    // LEOs=[100,100,50,0,0] → HW = min = 0
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 100LL);
    s.updateFollowerFetchOffset(2, 50LL);
    // followers 3 and 4 unknown → 0
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
}

// ============================================================================
// ISR shrink — the ISR minimum recomputed correctly
// ============================================================================

TEST(HighWatermark, IsrShrinkFrom5To3RecomputesIsrMin) {
    // Start 5 replicas; shrink to 3; re-check HW over the new ISR.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    s.updateFollowerFetchOffset(3, 70LL);
    s.updateFollowerFetchOffset(4, 60LL);
    EXPECT_EQ(s.highWatermark(), 60LL);   // Kafka model: HW = min LEO over the ISR

    // Shrink ISR to {0,1,2}: LEOs=[100,90,80] → computed = min = 80 (> prevHw_ 60)
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL); // trigger recompute
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR
}

TEST(HighWatermark, IsrShrinkFrom3To1) {
    // Shrink to leader-only ISR: HW = leaderLeo
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 50LL);
    s.updateFollowerFetchOffset(2, 30LL);
    EXPECT_EQ(s.highWatermark(), 30LL);   // Kafka model: HW = min LEO over the ISR

    // Shrink to {0}; HW = min over {leader} = leaderLeo = 100
    s.setIsr({0});
    s.updateFollowerFetchOffset(1, 50LL); // non-ISR, trigger recompute
    EXPECT_EQ(s.highWatermark(), 100LL);
}

// ============================================================================
// 2-replica ISR (leader + 1 follower) → HW = min of the two
// ============================================================================

TEST(HighWatermark, TwoReplicaMajority2) {
    // ISR={0,1}: LEOs=[100,70] → HW = min = 70
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
    // the HW), then truncates and re-fetches from LEO=5, must NOT still
    // be counted as holding offset 10.  The per-follower LEO must be updated
    // to the latest reported value (5), not the previous max (10).
    //
    // Scenario: ISR={leader=0, follower=1}, leader LEO=10.
    // 1. Follower reports LEO=10 → HW advances to 10.
    // 2. Follower truncates and reports LEO=5 → HW must recompute.
    //    After truncation: LEOs=[10, 5], min=5.
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
    // at LEO=5 means min{15,5} = 5 < prevHw_=10, so HW stays 10.
    s.setLeaderLogEndOffset(15LL);
    EXPECT_EQ(s.highWatermark(), 10LL); // floor holds, not above the ISR minimum

    // Step 3: follower re-fetches and advances to LEO=12.
    // Now min{15,12} = 12 > prevHw_=10 → HW advances to 12.
    s.updateFollowerFetchOffset(1, 12LL);
    EXPECT_EQ(s.highWatermark(), 12LL);
}

TEST(HighWatermark, B1_FollowerReportsLowerLeo_HwDoesNotAdvancePastTrueHolders) {
    // 3-replica ISR={leader=0, f1=1, f2=2}. Leader LEO=20.
    // Initially: f1=15, f2=15 → HW = min = 15.
    // f1 truncates to 8 (new fetch). HW recompute: LEOs=[20,8,15] → computed = 8
    // → no advance (monotonic floor stays 15).
    // If leader advances to 25, f2=15: LEOs=[25,8,15] → computed = 8 → HW stays 15,
    // never computed from f1's stale pre-truncation value. This verifies B1.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(20LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 15LL);
    s.updateFollowerFetchOffset(2, 15LL);
    EXPECT_EQ(s.highWatermark(), 15LL);

    // f1 truncates to 8.
    s.updateFollowerFetchOffset(1, 8LL);
    // LEOs=[20,8,15] → computed = min = 8 < prevHw_ → stays 15
    EXPECT_EQ(s.highWatermark(), 15LL);

    // Leader advances to 25; f2 stays at 15; f1 at 8.
    s.setLeaderLogEndOffset(25LL);
    // LEOs=[25,8,15] computed = min = 8 → stays 15
    EXPECT_EQ(s.highWatermark(), 15LL);

    // f1 re-catches to 20; LEOs=[25,20,15] → computed = min = 15 → stays 15
    s.updateFollowerFetchOffset(1, 20LL);
    EXPECT_EQ(s.highWatermark(), 15LL);   // Kafka model: HW = min LEO over the ISR
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
    // Before shrink: HW = min = 50 (leader=100, f1=50, f2=50).
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

// ============================================================================
// B1 DISTINGUISHING test: per-follower SET (not max) changes HW outcome.
//
// The other B1 tests pass even under the old max() code because the monotonic
// floor (prevHw_) already reached the stale follower value before it truncated.
// Distinguishing needs the floor BELOW the stale value when the recompute
// fires: the stale value must be what would lift the ISR minimum.
//
// Scenario (HW = min LEO over the ISR): ISR {leader=0, f1=1, f2=2}, f2 far
// ahead (20) so the minimum is set by the leader / f1.
//   leader LEO 8, f1 reports 10      → min(8,10,20)  = 8  → prevHw_ = 8
//   f1 truncates, reports 5          → min(8,5,20)   = 5  → prevHw_ stays 8
//   leader LEO advances to 12:
//     SET   (correct): min(12,5,20)  = 5  < 8        → HW stays 8
//     max() (wrong):   min(12,10,20) = 10 > 8        → HW 10 — offset 10 is
//                      no longer held by f1 after its truncation.
// ============================================================================

TEST(HighWatermark, B1_Distinguishing_StaleMaxInflatesHwAboveFloor) {
    // This test FAILS under the old max() implementation and PASSES under SET.
    //
    // Scenario: 3-replica ISR {leader=0, f1=1, f2=2}.
    //   leaderLeo=8 (below f1's initial report). f2 at 20.
    //   (See the block comment above.)  Every ISR member must hold an offset for
    //   it to be committed; after f1 truncates to 5 it does not hold 10.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(8LL);
    s.setIsr({0, 1, 2});
    // (Kafka model, HW = min over the ISR: f2 is far ahead so the min is set by
    //  the leader / f1 and the SET-vs-max distinction still shows.)
    s.updateFollowerFetchOffset(2, 20LL);

    // f1 reports LEO=10 (ahead of leader's current LEO=8).
    // LEOs=[8,10,20] → min → computed=8 → prevHw_=8.
    s.updateFollowerFetchOffset(1, 10LL);
    ASSERT_EQ(s.highWatermark(), 8LL) << "Precondition: prevHw_=8 (not 10)";

    // f1 truncates and re-fetches from LEO=5.
    s.updateFollowerFetchOffset(1, 5LL);
    // Both SET and max() leave prevHw_=8 here (computed=5<8 or computed=8=8).
    ASSERT_EQ(s.highWatermark(), 8LL) << "Precondition: prevHw_ still 8 after truncation";

    // Leader advances LEO to 12 (setLeaderLogEndOffset eagerly recomputes).
    s.setLeaderLogEndOffset(12LL);

    // Under SET (correct): min(12,5,20)  = 5  < prevHw_=8 → HW=8.
    // Under max() (WRONG): min(12,10,20) = 10 > prevHw_=8 → HW=10.
    //                      f1 no longer holds offset 10 after truncation to 5!
    EXPECT_EQ(s.highWatermark(), 8LL)
        << "HW must remain 8: f1 truncated to 5, so not every ISR member holds >=10. "
           "Old max() code would wrongly return 10 here.";
}

TEST(HighWatermark, B2_FloorPreservedAfterFollowerCatchUp) {
    // After restoring the floor and followers re-ack, HW advances normally.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setIsr({0, 1});
    s.setLeaderLogEndOffset(10LL);
    s.setHighWatermarkFloor(10LL); // B2: restore committed floor

    EXPECT_EQ(s.highWatermark(), 10LL); // floor holds immediately

    // Follower re-acks at 10 → min{10,10} = 10 — floor already there
    s.updateFollowerFetchOffset(1, 10LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Leader appends more; follower re-acks → HW advances past floor
    s.setLeaderLogEndOffset(15LL);
    s.updateFollowerFetchOffset(1, 15LL);
    EXPECT_EQ(s.highWatermark(), 15LL);
}
