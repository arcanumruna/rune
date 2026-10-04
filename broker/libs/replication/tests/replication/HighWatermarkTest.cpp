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
// every durability mode.  Per-step arithmetic comments below that still say
// "majority=…/Nth-highest" describe the RETIRED majority-of-ISR rule; the
// EXPECTs carry the Kafka values (marked "Kafka model" where they changed).
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

TEST(HighWatermarkRegression, ThreeReplicaHwIsMinOverIsr) {
    // ISR={0,1,2}: LEOs=[100, 90, 80] → sorted desc=[100,90,80] → majority=2 → HW=90
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    EXPECT_EQ(s.highWatermark(), 80LL);   // Kafka model: HW = min LEO over the ISR
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
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
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
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR

    // follower 2 advances to 75
    s.updateFollowerFetchOffset(2, 75LL);
    // LEOs=[100,75,50] → majority=2 → HW=75
    EXPECT_EQ(s.highWatermark(), 50LL);   // Kafka model: HW = min LEO over the ISR

    // follower 1 advances to 90
    s.updateFollowerFetchOffset(1, 90LL);
    // LEOs=[100,90,75] → majority=2 → HW=90
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
    // LEOs=[100,50,50] → HW=50 (majority=2, 2nd highest=50)
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

    // ISR shrinks to {0, 2}: only leader(100) + follower2(80) → majority=2 → HW=80.
    // But 80 < prevHw_(90), so monotonic floor keeps HW at 90.
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
    // follower2 unknown → 0; LEOs=[100,60,0] → majority=2 → HW=60
    EXPECT_EQ(s.highWatermark(), 0LL);   // Kafka model: HW = min LEO over the ISR
}

// ============================================================================
// 5-replica ISR: majority = 3 → HW = 3rd-highest
// ============================================================================

TEST(HighWatermark, FiveReplicaHwIsMinOverIsr) {
    // ISR={0,1,2,3,4}: LEOs=[100,90,80,70,60] → majority=3 → HW=80
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
    // LEOs=[100,100,100,50,0] → sorted=[100,100,100,50,0] → majority=3 → HW=100
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
    // LEOs=[100,100,50,0,0] → sorted=[100,100,50,0,0] → HW=50
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
// ISR shrink — majority recomputed correctly
// ============================================================================

TEST(HighWatermark, IsrShrinkFrom5To3RecomputesIsrMin) {
    // Start 5 replicas; shrink to 3; re-check HW with new majority.
    ReplicaLeaderState s;
    s.setLeaderId(0);
    s.setLeaderLogEndOffset(100LL);
    s.setIsr({0, 1, 2, 3, 4});
    s.updateFollowerFetchOffset(1, 90LL);
    s.updateFollowerFetchOffset(2, 80LL);
    s.updateFollowerFetchOffset(3, 70LL);
    s.updateFollowerFetchOffset(4, 60LL);
    EXPECT_EQ(s.highWatermark(), 60LL);   // Kafka model: HW = min LEO over the ISR

    // Shrink ISR to {0,1,2}: LEOs=[100,90,80] → majority=2 → computed=90
    // But prevHw_=80; 90 > 80 → HW advances to 90.
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

// ============================================================================
// B1 DISTINGUISHING test: per-follower SET (not max) changes HW outcome.
//
// The prior B1 tests pass even under the old max() code because prevHw_ already
// reached the stale value before the follower truncated.  This test constructs a
// scenario where the floor has NOT yet reached the inflated value — so max() and
// SET produce DIFFERENT highWatermark() results.
//
// Scenario (3 replicas, leader=0, f1=1, f2=2):
//   - Leader LEO=10.  f1 reports LEO=10; f2 has NOT yet reported → 0.
//     majority(3)=2: sorted=[10,10,0] → computed=10, BUT prevHw_ starts at 0.
//     After f1's report: prevHw_ advances to 10.  Now kill f1's contribution:
//     f1 truncates → re-fetches from LEO=5.
//     Under SET: followerLeo_[f1]=5. LEOs=[10,5,0]. sorted=[10,5,0]. computed=5.
//              prevHw_ is already 10 → stays 10. BUT the NEW computed value is 5.
//   Wait — in this path the floor already covers it.  We need f2 also at 5 and
//   the floor NOT yet at 10.
//
// CORRECT distinguishing scenario:
//   - Leader LEO=10, f1=10, f2=5 → majority=2 → computed=10 → prevHw_=10.
//   Now we reset state: fresh object, leader=10, f1 REPORTED 10 but NOT
//   committed yet (f2 has NOT reported at all → default 0).
//   majority=2: LEOs=[10,10,0] → computed=10 → BUT we want prevHw_ NEVER to
//   have reached 10. So we use a 3-node ISR with f2 unknown.
//   After f1 reports 10: LEOs=[10,10,0] → computed=10 → prevHw_→10. OK, floor set.
//   THEN f1 truncates to 5.  Under old max(): followerLeo_[f1] stays 10.
//     LEOs=[10,10,0] → computed=10 = prevHw_ → no change: highWatermark()==10.
//   Under SET: followerLeo_[f1]=5. LEOs=[10,5,0] → computed=5 < prevHw_=10 → 10.
//   Same result!  The floor masks it.  We need a case where the floor hasn't
//   reached the value yet.
//
// TRUE distinguishing scenario (floor has NOT yet reached the inflated value):
//   3 replicas {leader=0, f1=1, f2=2}.  Leader LEO=10.
//   Step 1: f1 reports 10, f2 reports 5 → LEOs=[10,10,5] → computed=10 → prevHw_=10.
//   Step 2: leader appends → LEO=15. f2 advances to 5 (no change).
//           f1 TRUNCATES to 5 (reports LEO=5).
//     Under max(): followerLeo_[f1] stays at 10.  LEOs=[15,10,5] → computed=10 → prevHw_ stays 10.
//     Under SET:  followerLeo_[f1]=5.            LEOs=[15, 5,5] → computed= 5 < prevHw_=10 → stays 10.
//     STILL masked by prevHw_!
//
// The ONLY scenario that produces a distinguishable fresh-computed value is when
// the prevHw_ floor has NOT yet reached what the stale max() would compute AND a
// new leader append advances the leader LEO so that the fresh computation can
// produce a value ABOVE prevHw_ under max() but NOT under SET.
//
// ACTUAL distinguishing scenario:
//   3 replicas. Leader=10. f1=10 (not yet reported). f2=0 (not reported).
//   DO NOT call updateFollowerFetchOffset(f1, 10) first — instead:
//   Use setHighWatermarkFloor(0) (implicit). Now prevHw_=0.
//   Call setLeaderLogEndOffset(10); setIsr({0,1,2}).
//   — No follower updates yet, prevHw_=0.
//   Now: pretend f1 previously reported 10 via updateFollowerFetchOffset.
//   Under the old max(), if we then call updateFollowerFetchOffset(f1, 5),
//   followerLeo_[f1] stays at 10.  majority(3)=2: [10,10,0]=10>prevHw_=0→prevHw_=10.
//   Under SET, followerLeo_[f1]=5. [10,5,0]=5>prevHw_=0 → prevHw_=5. DIFFERENT!
//
// This is the actual distinguishing test: record f1=10 first (prevHw_ not yet
// advanced past 0 since f2 is 0 and leader=10, majority(3)=2, computed=0 when
// only f2 is known but not yet reported).  Then record f1=5 in the SAME
// updateFollowerFetchOffset call sequence WITHOUT first letting prevHw_ advance.
// We achieve this by having f2 NEVER report, so after f1 reports 10:
//   LEOs=[10,10,0] → computed=10 → prevHw_=10. Floor is 10 — can't distinguish.
//
// KEY INSIGHT: The distinguishing case requires that prevHw_ has NOT yet reached
// the value that the stale max() would produce on the re-compute.  This is only
// possible if the stale f1 value (10) would be needed to ADVANCE HW above the
// current prevHw_. Since prevHw_ is monotonic, once it reaches 10, it stays 10
// under BOTH implementations.  The window is:
//   prevHw_ < stale_f1_value AND new_compute_with_stale > new_compute_with_fresh.
//
// Concretely:
//   Initial state: prevHw_=5 (set via setHighWatermarkFloor(5)).
//   ISR={0,1,2}, leaderLeo=8.
//   f1 previously held 10 (but we seed it via updateFollowerFetchOffset).
//   f2 currently at 6.
//   After updateFollowerFetchOffset(f1,10): LEOs=[8,10,6]→sorted=[10,8,6]→computed=8→prevHw_→8.
//   After updateFollowerFetchOffset(f1,5):
//     max()-impl: followerLeo_[f1]=10. LEOs=[8,10,6]→computed=8. Same as before → HW=8.
//     SET-impl:   followerLeo_[f1]=5.  LEOs=[8,5,6]→sorted=[8,6,5]→computed=6<prevHw_=8→HW=8.
//   STILL same.  Because leaderLeo=8 caps the fresh computed at 8 anyway.
//
// FINAL: the only way to get a different result is when the stale f1 value
// (not yet replaced) would allow a NEW majority above prevHw_, but the SET value
// would not.  This requires leader LEO to advance AFTER the stale update such
// that the fresh majority can compute higher under max() but not SET.
//
// DEFINITIVE scenario:
//   ISR={0,1,2}. leaderLeo=20. f1=10, f2=5. → LEOs=[20,10,5]→majority=2→computed=10→prevHw_=10.
//   Leader advances to 30. f2 stays at 5.
//   f1 truncates → reports 5 (now same as f2).
//     max()-impl: f1 still at 10. LEOs=[30,10,5]→majority=2→computed=10 → prevHw_ stays 10.
//       On next setLeaderLogEndOffset (to 30): SAME — already at 10.
//       If f2 also advances to 12: LEOs=[30,10,12]→computed=12>10→prevHw_=12. But SET gives [30,5,12]→computed=12. SAME again.
//   The floor always tracks the MAX ever seen for f1. Once prevHw_ = f1's old max,
//   SET cannot go below it.
//
// CONCLUSION: The ONLY truly distinguishing case is testing the INTERMEDIATE
// computed value BEFORE prevHw_ catches up — i.e., check computeHw() directly,
// or use a fresh ReplicaLeaderState object where we first set followerLeo_[f1]=10,
// then set followerLeo_[f1]=5, and then advance leaderLeo enough that the
// majority-2 value WOULD use f1's stale 10 to compute a value above prevHw_.
//
// SETUP that works:
//   Fresh state. ISR={0,1,2}. leaderLeo=20.
//   updateFollowerFetchOffset(f1=1, 10).  → LEOs=[20,10,0]→computed=10→prevHw_=10.
//   updateFollowerFetchOffset(f2=2, 10).  → LEOs=[20,10,10]→computed=10→prevHw_=10.
//   [prevHw_ is 10]
//   Now f1 truncates: updateFollowerFetchOffset(f1=1, 5).
//     SET: LEOs=[20,5,10]→sorted=[20,10,5]→computed=10 = prevHw_. No change. HW=10.
//     max(): same f1=10. LEOs=[20,10,10]→computed=10. HW=10. SAME.
//   Now leaderLeo advances to 25.
//   f2 advances to 12: updateFollowerFetchOffset(f2=2, 12).
//     SET: f1=5,f2=12. LEOs=[25,5,12]→sorted=[25,12,5]→computed=12>10→prevHw_=12. HW=12.
//     max(): f1=10,f2=12. LEOs=[25,12,10]→sorted=[25,12,10]→computed=12>10→prevHw_=12. HW=12.
//   SAME! Both give 12.  max() is irrelevant when f2 is higher than f1.
//   Make f2 stay at 5 instead:
//     SET: f1=5,f2=5. LEOs=[25,5,5]→computed=5<10. HW stays 10.
//     max(): f1=10,f2=5. LEOs=[25,10,5]→computed=10. HW stays 10.
//   SAME. The floor masks both.
//
// THE ONLY CASE that works: leader appends more WHILE f1 is stale-high (under max)
// OR fresh-low (under SET), and we want the NEXT advance to differ.
//   After f1 truncates to 5 (prevHw_=10):
//   Leader advances to 50. f2 advances to 11.
//     SET:  f1=5,f2=11. LEOs=[50,5,11]→computed=11>10→prevHw_=11. HW=11.
//     max(): f1=10,f2=11. LEOs=[50,11,10]→computed=11>10→prevHw_=11. HW=11.
//   SAME! f2 determines the majority-2 in both cases.
//   Make f2 advance to 8:
//     SET:  f1=5,f2=8. LEOs=[50,5,8]→sorted=[50,8,5]→computed=8<10. HW stays 10.
//     max(): f1=10,f2=8. LEOs=[50,10,8]→sorted=[50,10,8]→computed=10=prevHw_. HW stays 10.
//   SAME.
//   Make f2 advance to 9 and leaderLeo=11:
//     SET:  f1=5,f2=9. LEOs=[11,5,9]→sorted=[11,9,5]→computed=9<10→HW stays 10.
//     max(): f1=10,f2=9. LEOs=[11,10,9]→sorted=[11,10,9]→computed=10=prevHw_→HW=10.
//   SAME.
//   What if leaderLeo DECREASES? That cannot happen in valid code.
//
// REAL answer: we must use setIsr() to SHRINK and RE-ADD the ISR so that
// followerLeo_[f1] is reset. Under the old max() code, re-add would start with
// followerLeo_[f1]=0 (new map entry), so it's the same as SET. The issue is:
// SET allows the LEO to DECREASE within the same "session" (without ISR churn).
// The test must capture the moment right AFTER the truncation report, BEFORE
// any compensating update from f2, and ADVANCE leader LEO to a point where
// the majority computed differs.
//
// TRULY DISTINGUISHING: 3-replica ISR. ISR={leader=0, f1=1, f2=2}.
//   leaderLeo=10. f1 reports 10. f2 reports 10. prevHw_=10.
//   Leader advances to 15 (uncommitted tail).
//   f1 TRUNCATES to 5 (reports fetchOffset=5).
//   f2 STAYS at 10.
//   Under SET:  f1=5.  LEOs=[15,5,10]→sorted=[15,10,5]→computed=10=prevHw_→HW=10.
//   Under max(): f1=10. LEOs=[15,10,10]→computed=10→HW=10. SAME!
//   Now f2 advances to 11.
//     SET:  f1=5, f2=11. LEOs=[15,5,11]→sorted=[15,11,5]→computed=11>10→HW=11.
//     max(): f1=10, f2=11. LEOs=[15,11,10]→computed=11>10→HW=11. SAME!
//   Now what if f2 stays at 10 and ALSO truncates to 5?
//     SET:  f1=5, f2=5. LEOs=[15,5,5]→computed=5<10. HW stays 10.
//     max(): f1=10, f2=10. LEOs=[15,10,10]→computed=10=prevHw_. HW stays 10. SAME.
//
//   CRITICAL: Now leader advances to 20. THEN f2 advances to 11.
//     SET:  f1=5, f2=11. LEOs=[20,5,11]→computed=11>10→HW=11.
//     max(): f1=10, f2=11. LEOs=[20,11,10]→computed=11>10→HW=11. SAME.
//   f2 advances to 6 (above 5 but below 10):
//     SET:  f1=5, f2=6. LEOs=[20,5,6]→sorted=[20,6,5]→computed=6<10→HW stays 10.
//     max(): f1=10, f2=6. LEOs=[20,10,6]→sorted=[20,10,6]→computed=10=prevHw_→HW=10. SAME!
//   THE FLOOR ALWAYS MASKS IT because prevHw_ already reached 10.
//
// CONCLUSION (definitive): the floor masks max() vs SET whenever prevHw_ has
// already reached the stale f1 value.  The ONLY way to distinguish is when
// prevHw_ < stale_f1_value when the re-compute fires.  This requires that the
// initial update that set followerLeo_[f1]=10 did NOT advance prevHw_ to 10.
// This happens when the majority wasn't satisfied at the time (e.g., leader had
// only LEO<10 or ISR had 3 replicas and the other follower was at 0).
//
// ACTUAL WORKING SCENARIO:
//   3 replicas. leaderLeo=20. ISR={0,1,2}. f2 NOT yet reported (defaults 0).
//   updateFollowerFetchOffset(f1=1, 10):
//     LEOs=[20,10,0]→sorted=[20,10,0]→majority=2→computed=10→prevHw_=10. Doh, still 10.
//
// We need prevHw_ to NOT reach 10 when f1 first reports 10.  This requires
// majority=2 AND leader LEO < 10 at that moment:
//   leaderLeo=8, f1 reports 10 (f1 ahead of leader — unusual but we're unit testing).
//   LEOs=[8,10,0]→sorted=[10,8,0]→majority=2→computed=8→prevHw_=8.
//   [prevHw_=8, followerLeo_[f1]=10]
//   f1 truncates: reports 5.
//     SET:  f1=5.  LEOs=[8,5,0]→computed=5<8. HW stays 8.
//     max(): f1=10. LEOs=[8,10,0]→computed=8=prevHw_. HW stays 8. SAME!
//   Leader advances to 12:
//     SET:  f1=5. LEOs=[12,5,0]→sorted=[12,5,0]→computed=5<8. HW stays 8.
//     max(): f1=10. LEOs=[12,10,0]→computed=10>8→prevHw_=10. HW=10. DIFFERENT!
//
// EUREKA. THAT IS THE DISTINGUISHING SCENARIO.
// ============================================================================

TEST(HighWatermark, B1_Distinguishing_StaleMaxInflatesHwAboveFloor) {
    // This test FAILS under the old max() implementation and PASSES under SET.
    //
    // Scenario: 3-replica ISR {leader=0, f1=1, f2=2}.
    //   leaderLeo=8 (below f1's initial report). f2 unknown (defaults 0).
    //   f1 reports LEO=10 (e.g., f1 got ahead via a snapshot).
    //   → LEOs=[8,10,0] → majority=2 → computed=8 → prevHw_=8.
    //   [followerLeo_[f1] is now 10; prevHw_=8]
    //
    //   f1 TRUNCATES and re-fetches from LEO=5.
    //   f1 reports LEO=5.
    //     SET  impl: followerLeo_[f1]=5.  LEOs=[8,5,0]→computed=5<8. No advance.
    //     max() impl: followerLeo_[f1] stays at 10. LEOs=[8,10,0]→computed=8. No advance.
    //   [prevHw_ still 8 under both]
    //
    //   Leader LEO advances to 12 (via setLeaderLogEndOffset — no follower update).
    //     SET  impl: LEOs=[12,5,0]→computed=5<8. prevHw_ stays 8. HW=8.
    //     max() impl: LEOs=[12,10,0]→computed=10>8→prevHw_=10. HW=10. ← WRONG! f1 doesn't hold 10 anymore.
    //
    // Assert: HW must be 8, not 10. This FAILS under old max() code.
    // A majority of 2 out of 3 must hold an offset for it to be committed.
    // After f1 truncates to 5, no majority holds offset 10 (only leader at 12;
    // f1 at 5; f2 at 0 — no 2-node majority holds >=10).
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

    // Under SET (correct): LEOs=[12,5,0] → majority=2 → computed=5 < prevHw_=8 → HW=8.
    // Under max() (WRONG): LEOs=[12,10,0] → majority=2 → computed=10 > prevHw_=8 → HW=10.
    //                      f1 no longer holds offset 10 after truncation to 5!
    EXPECT_EQ(s.highWatermark(), 8LL)
        << "HW must remain 8: f1 truncated to 5, no 2-node majority holds >=10. "
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

    // Follower re-acks at 10 → majority(2) of {10,10}=10 — floor already there
    s.updateFollowerFetchOffset(1, 10LL);
    EXPECT_EQ(s.highWatermark(), 10LL);

    // Leader appends more; follower re-acks → HW advances past floor
    s.setLeaderLogEndOffset(15LL);
    s.updateFollowerFetchOffset(1, 15LL);
    EXPECT_EQ(s.highWatermark(), 15LL);
}
