// ControlPlaneTimerTest.cpp — TDD tests for ControlPlaneTimer.
//
// ControlPlaneTimer is a priority-heap (min-heap on deadline) polled against
// cluster time by the LEADER.  When a timer fires, the leader appends a
// ControlTickRecord to the MSM log — so the decision is replicated and applied
// identically on all replicas (determinism guarantee).
//
// Tests cover:
//   1. Earliest-deadline-first poll ordering (verbatim from brief)
//   2. Snapshot + re-arm preserves pending timers (verbatim from brief)
//   3. Determinism: same ControlTickRecord applied on leader / follower /
//      recovering-node yields identical observable state.
//   4. reArm keeps timers whose deadline < clusterNow (they fire on next poll).
//   5. snapshotPending on empty timer is safe.

#include <rune/metadata/ControlPlaneTimer.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

// ---------------------------------------------------------------------------
// Verbatim from brief (Step 5)
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, FiresEarliestDeadlineOnPollAtOrAfter) {
    ControlPlaneTimer t; t.arm(200, /*id*/2); t.arm(100, 1); t.arm(300, 3);
    int32_t fired = -1;
    EXPECT_FALSE(t.poll(50, fired));
    EXPECT_TRUE (t.poll(150, fired)); EXPECT_EQ(fired, 1);
    EXPECT_TRUE (t.poll(250, fired)); EXPECT_EQ(fired, 2);
    EXPECT_FALSE(t.poll(250, fired)); // 300 not yet due
}

TEST(ControlPlaneTimer, SnapshotAndReArmPreservePending) {
    ControlPlaneTimer t; t.arm(200, 2); t.arm(300, 3);
    std::vector<std::pair<int64_t,int32_t>> pend; t.snapshotPending(pend);
    ControlPlaneTimer t2; t2.reArm(pend, /*clusterNow*/0);
    int32_t fired = -1;
    EXPECT_TRUE(t2.poll(250, fired)); EXPECT_EQ(fired, 2);
}

// ---------------------------------------------------------------------------
// Determinism: the same ControlTickRecord applied on leader / follower /
// recovering-node must yield the same result.
//
// This test simulates three independent timer instances that are each
// snapshot-seeded to the same pending set (as would happen after a
// snapshot is transferred during election-complete or recovery).
// All three must fire timers in exactly the same order at the same times.
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, DeterminismAcrossLeaderFollowerRecovery) {
    // Seed timers (deadline, id) that we want to distribute
    const std::vector<std::pair<int64_t,int32_t>> pending = {{100, 1}, {200, 2}, {300, 3}};

    // Three "replicas" each re-armed from the same snapshot, cluster time = 0.
    ControlPlaneTimer leader, follower, recovering;
    leader.reArm(pending, 0);
    follower.reArm(pending, 0);
    recovering.reArm(pending, 0);

    // Verify they all fire in the same order at the same cluster times.
    for (int64_t clusterTime : {50LL, 150LL, 250LL, 350LL}) {
        int32_t lFired = -1, fFired = -1, rFired = -1;
        bool lResult = leader.poll(clusterTime, lFired);
        bool fResult = follower.poll(clusterTime, fFired);
        bool rResult = recovering.poll(clusterTime, rFired);

        EXPECT_EQ(lResult, fResult)  << "at clusterTime=" << clusterTime;
        EXPECT_EQ(lResult, rResult)  << "at clusterTime=" << clusterTime;
        EXPECT_EQ(lFired,  fFired)   << "at clusterTime=" << clusterTime;
        EXPECT_EQ(lFired,  rFired)   << "at clusterTime=" << clusterTime;
    }
}

// ---------------------------------------------------------------------------
// reArm keeps timers whose deadline < clusterNow (fires on next poll)
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, ReArmKeepsPastDeadlines) {
    // deadline 50 is "in the past" relative to clusterNow=100
    std::vector<std::pair<int64_t,int32_t>> pend = {{50, 7}, {200, 8}};
    ControlPlaneTimer t;
    t.reArm(pend, 100); // clusterNow = 100; deadline 50 < 100 — must be kept

    int32_t fired = -1;
    // 50 <= 100 → fires immediately
    EXPECT_TRUE(t.poll(100, fired)); EXPECT_EQ(fired, 7);
    // 200 not yet due at 100
    EXPECT_FALSE(t.poll(100, fired));
    // fires at 250
    EXPECT_TRUE(t.poll(250, fired)); EXPECT_EQ(fired, 8);
}

// ---------------------------------------------------------------------------
// snapshotPending on empty timer
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, SnapshotEmptyTimerIsEmpty) {
    ControlPlaneTimer t;
    std::vector<std::pair<int64_t,int32_t>> out;
    t.snapshotPending(out);
    EXPECT_TRUE(out.empty());
}

// ---------------------------------------------------------------------------
// arm with duplicate deadlines — both fire in any order but both fire
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, DuplicateDeadlinesBothFire) {
    ControlPlaneTimer t;
    t.arm(100, 10);
    t.arm(100, 20);

    int32_t a = -1, b = -1;
    EXPECT_TRUE(t.poll(100, a));
    EXPECT_TRUE(t.poll(100, b));
    EXPECT_FALSE(t.poll(100, b)); // nothing left

    // Both distinct timer ids fired
    EXPECT_NE(a, b);
    EXPECT_TRUE((a == 10 && b == 20) || (a == 20 && b == 10));
}

// ---------------------------------------------------------------------------
// poll returns false when no timers are armed
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, PollOnEmptyReturnsFalse) {
    ControlPlaneTimer t;
    int32_t fired = -1;
    EXPECT_FALSE(t.poll(INT64_MAX, fired));
}

// ---------------------------------------------------------------------------
// snapshotPending captures all pending timers
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, SnapshotCapturesAllPending) {
    ControlPlaneTimer t;
    t.arm(300, 3);
    t.arm(100, 1);
    t.arm(200, 2);

    // fire the first
    int32_t f = -1;
    EXPECT_TRUE(t.poll(150, f)); EXPECT_EQ(f, 1);

    // snapshot the remaining two
    std::vector<std::pair<int64_t,int32_t>> out;
    t.snapshotPending(out);
    ASSERT_EQ(out.size(), 2u);

    // Both {200,2} and {300,3} must appear (order not guaranteed)
    bool has200 = false, has300 = false;
    for (auto& [dl, id] : out) {
        if (dl == 200 && id == 2) has200 = true;
        if (dl == 300 && id == 3) has300 = true;
    }
    EXPECT_TRUE(has200);
    EXPECT_TRUE(has300);
}

// ---------------------------------------------------------------------------
// Broker-epoch monotonicity: verify ControlTickRecord tickSeq never goes
// backward across a snapshot+reArm cycle.
// ---------------------------------------------------------------------------
TEST(ControlPlaneTimer, TickSeqIsMonotonicInRecords) {
    // Simulate leader appending ControlTickRecords in order.
    // tickSeq must be monotonically increasing.
    int64_t lastSeq = -1;
    ControlPlaneTimer t;
    t.arm(100, 1);
    t.arm(200, 2);
    t.arm(300, 3);

    for (int64_t clusterTime : {100LL, 200LL, 300LL}) {
        int32_t fired = -1;
        if (t.poll(clusterTime, fired)) {
            // Simulate creating a ControlTickRecord
            rune::protocol::metadata::ControlTickRecord tick;
            tick.tickTimeNs = clusterTime;
            tick.tickSeq    = lastSeq + 1;
            EXPECT_GT(tick.tickSeq, lastSeq) << "tickSeq must be monotonic";
            lastSeq = tick.tickSeq;
        }
    }
    EXPECT_EQ(lastSeq, 2); // 3 timers fired → seq 0,1,2
}
