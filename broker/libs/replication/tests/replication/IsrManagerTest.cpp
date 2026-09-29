// ---------------------------------------------------------------------------
// IsrManagerTest.cpp — Unit tests for rune::replication::IsrManager
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>
#include <rune/replication/IsrManager.h>

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static constexpr NodeId kLeader   = 1;
static constexpr NodeId kFollower1 = 2;
static constexpr NodeId kFollower2 = 3;

// Build a standard 3-node manager {leader=1, follower1=2, follower2=3}.
static IsrManager make3() {
    return IsrManager({kLeader, kFollower1, kFollower2}, kLeader);
}

// ---------------------------------------------------------------------------
// Construction / initial state
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, InitialIsrContainsAllReplicas) {
    auto mgr = make3();
    EXPECT_EQ(mgr.isr().size(), 3u);
    EXPECT_TRUE(mgr.inIsr(kLeader));
    EXPECT_TRUE(mgr.inIsr(kFollower1));
    EXPECT_TRUE(mgr.inIsr(kFollower2));
}

TEST(IsrManagerTest, InitialIsrChangedIsFalse) {
    auto mgr = make3();
    EXPECT_FALSE(mgr.isrChanged());
}

TEST(IsrManagerTest, ReplicasMatchConstruction) {
    auto mgr = make3();
    EXPECT_EQ(mgr.replicas().size(), 3u);
}

// ---------------------------------------------------------------------------
// onFollowerFetch — state recording
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, OnFollowerFetchUpdatesState) {
    auto mgr = make3();
    mgr.onFollowerFetch(kFollower1, 50, 1'000'000);
    auto* s = mgr.followerState(kFollower1);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->lastFetchOffset, 50);
    EXPECT_EQ(s->lastFetchTimeNs, 1'000'000);
}

TEST(IsrManagerTest, OnFollowerFetchOverwritesPreviousState) {
    auto mgr = make3();
    mgr.onFollowerFetch(kFollower1, 50, 1'000'000);
    mgr.onFollowerFetch(kFollower1, 100, 2'000'000);
    auto* s = mgr.followerState(kFollower1);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->lastFetchOffset, 100);
    EXPECT_EQ(s->lastFetchTimeNs, 2'000'000);
}

// ---------------------------------------------------------------------------
// maybeShrinkIsr — time lag
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, ShrinkByTimeLag) {
    auto mgr = make3();
    // Follower1 last fetched at t=0; now is t=10s; maxLag=5s → 5000ms
    int64_t lastFetch = 0;
    int64_t now       = 10'000'000'000LL; // 10s in ns
    int64_t lagMs     = 5'000;            // 5s
    mgr.onFollowerFetch(kFollower1, 100, lastFetch);
    mgr.onFollowerFetch(kFollower2, 100, now); // follower2 is current

    auto newIsr = mgr.maybeShrinkIsr(now, /*leaderLeo=*/110, lagMs, /*lagOffset=*/50);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_FALSE(mgr.inIsr(kFollower1));  // evicted
    EXPECT_TRUE(mgr.inIsr(kLeader));      // leader always stays
    EXPECT_TRUE(mgr.inIsr(kFollower2));   // not lagging
    EXPECT_EQ(newIsr.size(), 2u);
}

TEST(IsrManagerTest, NoShrinkWhenTimeLagExactlyAtBoundary) {
    auto mgr = make3();
    // Follower1 last fetched exactly lagMs ago — NOT exceeding threshold.
    int64_t lagMs    = 5'000;
    int64_t now      = 5'000'000'000LL; // 5s in ns
    int64_t lastFetch = now - lagMs * 1'000'000LL; // exactly at boundary
    mgr.onFollowerFetch(kFollower1, 100, lastFetch);
    mgr.onFollowerFetch(kFollower2, 100, now);

    auto newIsr = mgr.maybeShrinkIsr(now, 110, lagMs, 50);
    EXPECT_FALSE(mgr.isrChanged());
    EXPECT_TRUE(mgr.inIsr(kFollower1)); // not evicted (equal, not greater)
    EXPECT_EQ(newIsr.size(), 3u);
}

TEST(IsrManagerTest, ShrinkWhenTimeLagExceedsBoundary) {
    auto mgr = make3();
    int64_t lagMs    = 5'000;
    int64_t now      = 5'001'000'000LL; // just over 5s
    int64_t lastFetch = 0;
    mgr.onFollowerFetch(kFollower1, 100, lastFetch);
    mgr.onFollowerFetch(kFollower2, 100, now);

    auto newIsr = mgr.maybeShrinkIsr(now, 110, lagMs, 50);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_FALSE(mgr.inIsr(kFollower1));
}

// ---------------------------------------------------------------------------
// maybeShrinkIsr — offset lag
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, ShrinkByOffsetLag) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;
    // Follower1 at offset 50, leader LEO 200, max lag 100 → gap=150 > 100
    mgr.onFollowerFetch(kFollower1,  50, now);
    mgr.onFollowerFetch(kFollower2, 190, now);

    auto newIsr = mgr.maybeShrinkIsr(now, /*leaderLeo=*/200, /*lagMs=*/60'000, /*lagOffset=*/100);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_FALSE(mgr.inIsr(kFollower1));
    EXPECT_TRUE(mgr.inIsr(kFollower2)); // gap=10 ≤ 100
    EXPECT_TRUE(mgr.inIsr(kLeader));
}

TEST(IsrManagerTest, NoShrinkWhenOffsetLagAtBoundary) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;
    mgr.onFollowerFetch(kFollower1, 100, now);
    mgr.onFollowerFetch(kFollower2, 100, now);

    // leaderLeo=200, lag=100: gap=100 NOT > 100 → no shrink
    auto newIsr = mgr.maybeShrinkIsr(now, 200, 60'000, 100);
    EXPECT_FALSE(mgr.isrChanged());
    EXPECT_EQ(newIsr.size(), 3u);
}

TEST(IsrManagerTest, ShrinkByOffsetLagExceedsBoundary) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;
    mgr.onFollowerFetch(kFollower1,  99, now);
    mgr.onFollowerFetch(kFollower2, 100, now);

    // leaderLeo=200, maxOffset=100 → follower1 gap=101 > 100
    auto newIsr = mgr.maybeShrinkIsr(now, 200, 60'000, 100);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_FALSE(mgr.inIsr(kFollower1));
    EXPECT_TRUE(mgr.inIsr(kFollower2));
}

// ---------------------------------------------------------------------------
// maybeShrinkIsr — leader always stays
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, LeaderNeverShrunkOut) {
    auto mgr = make3();
    // Massive time lag but leader should stay
    int64_t now = 999'999'999'000LL;
    mgr.onFollowerFetch(kFollower1, 100, now);
    mgr.onFollowerFetch(kFollower2, 100, now);

    // nowNs for leader is 0 (default, never updated) — but leader is protected
    auto newIsr = mgr.maybeShrinkIsr(now, 200, /*lagMs=*/1, /*lagOffset=*/1);
    EXPECT_TRUE(mgr.inIsr(kLeader));
}

// ---------------------------------------------------------------------------
// maybeExpandIsr
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, ExpandCaughtUpFollower) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;

    // Shrink follower1 out
    mgr.onFollowerFetch(kFollower1,  0, 0); // stale
    mgr.onFollowerFetch(kFollower2, 90, now);
    mgr.maybeShrinkIsr(now, 100, /*lagMs=*/5'000, /*lagOffset=*/50);
    ASSERT_FALSE(mgr.inIsr(kFollower1));

    // Follower1 catches up: fetch offset >= HW=80
    mgr.onFollowerFetch(kFollower1, 80, now);
    auto newIsr = mgr.maybeExpandIsr(now, /*highWatermark=*/80);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_TRUE(mgr.inIsr(kFollower1));
    EXPECT_EQ(newIsr.size(), 3u);
}

TEST(IsrManagerTest, NoExpandWhenFollowerBehindHW) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;

    mgr.onFollowerFetch(kFollower1,  0, 0); // stale
    mgr.onFollowerFetch(kFollower2, 90, now);
    mgr.maybeShrinkIsr(now, 100, 5'000, 50);
    ASSERT_FALSE(mgr.inIsr(kFollower1));

    // Follower1 still behind HW
    mgr.onFollowerFetch(kFollower1, 79, now); // HW=80 → not caught up
    auto newIsr = mgr.maybeExpandIsr(now, 80);
    EXPECT_FALSE(mgr.isrChanged());
    EXPECT_FALSE(mgr.inIsr(kFollower1));
    EXPECT_EQ(newIsr.size(), 2u);
}

TEST(IsrManagerTest, ExpandExactlyAtHW) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;

    mgr.onFollowerFetch(kFollower1, 0, 0);
    mgr.onFollowerFetch(kFollower2, 90, now);
    mgr.maybeShrinkIsr(now, 100, 5'000, 50);
    ASSERT_FALSE(mgr.inIsr(kFollower1));

    // Exactly at HW → should expand (>= HW)
    mgr.onFollowerFetch(kFollower1, 80, now);
    mgr.maybeExpandIsr(now, 80);
    EXPECT_TRUE(mgr.inIsr(kFollower1));
}

TEST(IsrManagerTest, ExpandDoesNotDuplicateIsrMembers) {
    auto mgr = make3();
    // All already in ISR; expand should not add duplicates
    int64_t now = 1'000'000'000LL;
    mgr.onFollowerFetch(kFollower1, 100, now);
    mgr.onFollowerFetch(kFollower2, 100, now);
    auto newIsr = mgr.maybeExpandIsr(now, 50);
    EXPECT_FALSE(mgr.isrChanged());
    EXPECT_EQ(newIsr.size(), 3u);
}

// ---------------------------------------------------------------------------
// Shrink → expand round-trip
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, ShrinkExpandRoundTrip) {
    auto mgr = make3();
    int64_t t0 = 0;
    int64_t t1 = 10'000'000'000LL; // 10s

    mgr.onFollowerFetch(kFollower1, 100, t0);
    mgr.onFollowerFetch(kFollower2, 100, t0);

    // Shrink both followers out by time lag (maxLag=5s)
    mgr.maybeShrinkIsr(t1, 200, 5'000, 10'000);
    EXPECT_EQ(mgr.isr().size(), 1u); // leader only
    EXPECT_TRUE(mgr.inIsr(kLeader));

    // Both followers catch up and fetch at t1
    mgr.onFollowerFetch(kFollower1, 200, t1);
    mgr.onFollowerFetch(kFollower2, 200, t1);

    // Expand
    mgr.maybeExpandIsr(t1, 200);
    EXPECT_EQ(mgr.isr().size(), 3u);
    EXPECT_TRUE(mgr.inIsr(kFollower1));
    EXPECT_TRUE(mgr.inIsr(kFollower2));
}

// ---------------------------------------------------------------------------
// canProduce (min-ISR gate)
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, CanProduceAcksLeaderAlwaysTrue) {
    auto mgr = make3();
    // Even if ISR is shrunk to 1 (leader only)
    int64_t now = 10'000'000'000LL;
    mgr.onFollowerFetch(kFollower1, 0, 0);
    mgr.onFollowerFetch(kFollower2, 0, 0);
    mgr.maybeShrinkIsr(now, 200, 1, 1);
    EXPECT_EQ(mgr.isr().size(), 1u);

    EXPECT_TRUE(mgr.canProduce(3, /*acksAll=*/false)); // leader-only ack → always
    EXPECT_TRUE(mgr.canProduce(1, false));
}

TEST(IsrManagerTest, CanProduceAcksAllRejectsWhenIsrBelowMin) {
    auto mgr = make3();
    int64_t now = 10'000'000'000LL;
    mgr.onFollowerFetch(kFollower1, 0, 0);
    mgr.onFollowerFetch(kFollower2, 0, 0);
    mgr.maybeShrinkIsr(now, 200, 1, 1);
    EXPECT_EQ(mgr.isr().size(), 1u);

    // minIsr=2 but |ISR|=1 → cannot produce with acks=all
    EXPECT_FALSE(mgr.canProduce(2, /*acksAll=*/true));
    EXPECT_FALSE(mgr.canProduce(3, true));
}

TEST(IsrManagerTest, CanProduceAcksAllAcceptsWhenIsrMeetsMin) {
    auto mgr = make3();
    int64_t now = 1'000'000'000LL;
    mgr.onFollowerFetch(kFollower1, 100, now);
    mgr.onFollowerFetch(kFollower2, 100, now);
    // ISR = 3

    EXPECT_TRUE(mgr.canProduce(1, /*acksAll=*/true));
    EXPECT_TRUE(mgr.canProduce(2, true));
    EXPECT_TRUE(mgr.canProduce(3, true));
    EXPECT_FALSE(mgr.canProduce(4, true)); // minIsr=4 > |ISR|=3
}

TEST(IsrManagerTest, CanProduceAcksAllMinIsrZero) {
    auto mgr = make3();
    // minIsr=0 → trivially satisfied
    EXPECT_TRUE(mgr.canProduce(0, true));
}

// ---------------------------------------------------------------------------
// Concurrent lag on multiple followers
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, BothFollowersLaggingSimultaneously) {
    auto mgr = make3();
    int64_t now = 10'000'000'000LL;

    mgr.onFollowerFetch(kFollower1, 0, 0);
    mgr.onFollowerFetch(kFollower2, 0, 0);

    auto newIsr = mgr.maybeShrinkIsr(now, 200, 5'000, 50);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_EQ(newIsr.size(), 1u);
    EXPECT_TRUE(mgr.inIsr(kLeader));
    EXPECT_FALSE(mgr.inIsr(kFollower1));
    EXPECT_FALSE(mgr.inIsr(kFollower2));
}

TEST(IsrManagerTest, PartialLaggingOnlyEvictsLaggingFollower) {
    auto mgr = make3();
    int64_t now = 10'000'000'000LL;

    mgr.onFollowerFetch(kFollower1, 0,   0);   // lagging by time
    mgr.onFollowerFetch(kFollower2, 190, now);  // fine: gap=10 ≤ 50

    auto newIsr = mgr.maybeShrinkIsr(now, 200, 5'000, 50);
    EXPECT_TRUE(mgr.isrChanged());
    EXPECT_EQ(newIsr.size(), 2u);
    EXPECT_FALSE(mgr.inIsr(kFollower1));
    EXPECT_TRUE(mgr.inIsr(kFollower2));
}

// ---------------------------------------------------------------------------
// setIsr replaces ISR and resets isrChanged
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, SetIsrReplacesIsr) {
    auto mgr = make3();
    mgr.setIsr({kLeader, kFollower1}); // evict follower2 externally
    EXPECT_EQ(mgr.isr().size(), 2u);
    EXPECT_FALSE(mgr.inIsr(kFollower2));
    EXPECT_FALSE(mgr.isrChanged()); // setIsr resets the flag
}

// ---------------------------------------------------------------------------
// Single-node replica set (leader only)
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, SingleNodeLeaderOnlyIsrNeverShrinks) {
    IsrManager mgr({kLeader}, kLeader);
    EXPECT_EQ(mgr.isr().size(), 1u);

    auto newIsr = mgr.maybeShrinkIsr(999'999'999'000LL, 200, 1, 1);
    EXPECT_EQ(newIsr.size(), 1u);
    EXPECT_FALSE(mgr.isrChanged());
    EXPECT_TRUE(mgr.inIsr(kLeader));
}

// ---------------------------------------------------------------------------
// Five-node cluster: multiple partial evictions
// ---------------------------------------------------------------------------

TEST(IsrManagerTest, FiveNodePartialEviction) {
    IsrManager mgr({1, 2, 3, 4, 5}, /*leader=*/1);
    int64_t now = 10'000'000'000LL;

    mgr.onFollowerFetch(2, 100, now);
    mgr.onFollowerFetch(3,  50, 0);   // time-lagging
    mgr.onFollowerFetch(4,  10, now); // offset-lagging (gap=190 > 100)
    mgr.onFollowerFetch(5,  80, now); // fine (gap=120 > 100? yes — evicted)

    // leaderLeo=200, maxOffset=100, lagMs=5s
    auto newIsr = mgr.maybeShrinkIsr(now, 200, 5'000, 100);
    EXPECT_TRUE(mgr.inIsr(1));  // leader
    EXPECT_TRUE(mgr.inIsr(2));  // gap=100 NOT > 100 → stays
    EXPECT_FALSE(mgr.inIsr(3)); // time lag
    EXPECT_FALSE(mgr.inIsr(4)); // offset lag (gap=190)
    EXPECT_FALSE(mgr.inIsr(5)); // offset lag (gap=120)
    EXPECT_EQ(newIsr.size(), 2u);
}

// ---------------------------------------------------------------------------
// REGRESSION (bug: acks=all collapse under pipelined produce).
// A follower that is merely `window`-records behind during normal pipelined
// produce — but has fetched RECENTLY (well within the time-lag window) — must
// NOT be shrunk from the ISR.  The old offset-lag gate (replicaLagMaxOffset
// hardcoded to 10'000) evicted such a follower on every burst deeper than the
// threshold, collapsing the ISR below minIsr → MinIsrViolation → produce failed.
// Fix mirrors Kafka's 0.9 removal of `replica.lag.max.messages`: rely on the
// TIME-based gate and make the offset gate effectively unbounded.
// Suite name ends in "Regression" so it runs via --gtest_filter='*Regression*'.
// ---------------------------------------------------------------------------
TEST(IsrManagerRegression, PipelineDepthLagDoesNotShrinkRecentlyFetchedFollower) {
    IsrManager mgr = make3();
    const int64_t now = 1'000'000'000LL;
    // Leader at 500000; followers are 200000 records behind (a deep but normal
    // pipeline backlog) yet fetched just now (recent).
    mgr.onFollowerFetch(kLeader,    500'000, now);
    mgr.onFollowerFetch(kFollower1, 300'000, now);
    mgr.onFollowerFetch(kFollower2, 300'000, now);

    // Production settings: time gate 10s, offset gate effectively unbounded.
    auto newIsr = mgr.maybeShrinkIsr(now, /*leaderLeo=*/500'000,
                                     /*replicaLagMaxMs=*/10'000,
                                     /*replicaLagMaxOffset=*/1'000'000'000LL);
    EXPECT_TRUE(mgr.inIsr(kFollower1))
        << "a recently-fetched follower merely behind by pipeline depth must stay in ISR";
    EXPECT_TRUE(mgr.inIsr(kFollower2));
    EXPECT_EQ(newIsr.size(), 3u) << "ISR must not collapse during normal pipelined produce";
}

// REGRESSION companion: a genuinely stalled follower (no recent fetch) is STILL
// shrunk via the time gate — the fix must not disable liveness-based eviction.
TEST(IsrManagerRegression, StalledFollowerStillShrunkByTimeGate) {
    IsrManager mgr = make3();
    const int64_t now = 20'000'000'000LL;
    mgr.onFollowerFetch(kLeader,    500'000, now);
    mgr.onFollowerFetch(kFollower1, 300'000, now);                 // recent
    mgr.onFollowerFetch(kFollower2, 300'000, now - 11'000'000'000LL); // 11s stale
    auto newIsr = mgr.maybeShrinkIsr(now, 500'000, /*lagMs=*/10'000,
                                     /*lagOffset=*/1'000'000'000LL);
    EXPECT_TRUE(mgr.inIsr(kFollower1));
    EXPECT_FALSE(mgr.inIsr(kFollower2)) << "a follower stalled > lag window is still evicted";
    EXPECT_EQ(newIsr.size(), 2u);
}
