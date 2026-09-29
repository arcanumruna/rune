// FollowerLivenessTest.cpp — unit + REGRESSION tests for the CLOSED-follower
// liveness state machine (FollowerLiveness.h).
//
// REGRESSION CONTEXT (dr-e2e failover + multinode throughput churn):
//   A follower in the steady (CLOSED) state used to go SILENT after publishing its
//   AppendPosition once at election-close.  With no PERIODIC follower→leader
//   AppendPosition, the leader's per-member liveness went stale within one
//   heartbeat window → eligibleNodeIds() collapsed to the leader alone → partition
//   failover could never assemble a quorum (committed-loss on kill) AND the
//   balancer churned leadership (false "dead" peers, throughput collapse).
//   FollowerLiveness owns the periodic-send + dead-leader-detect duties as one
//   two-state machine (Unarmed/Following) that wraps its own clocks; these tests
//   pin every state + transition.

#include <gtest/gtest.h>
#include <rune/consensus/FollowerLiveness.h>

#include <cstdint>

using namespace rune::consensus;

namespace {
constexpr int64_t kSend    =  50'000'000LL; // 50ms status interval
constexpr int64_t kTimeout = 300'000'000LL; // 300ms leader-liveness timeout
} // namespace

// ── State: Unarmed (initial) ────────────────────────────────────────────────
TEST(FollowerLiveness, StartsUnarmedWithNoDuties) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    EXPECT_EQ(fl.state(), FollowerLiveness::State::Unarmed);
    auto a = fl.decide(10 * kTimeout);
    EXPECT_FALSE(a.sendAppendPosition) << "Unarmed must not send";
    EXPECT_FALSE(a.triggerReElection)  << "Unarmed has no leader to time out";
    EXPECT_EQ(fl.nextDeadlineNs(), INT64_MIN);
}

// A heartbeat while Unarmed is ignored (no leader accepted yet).
TEST(FollowerLiveness, HeartbeatIgnoredWhileUnarmed) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onLeaderHeartbeat(1'000);
    EXPECT_EQ(fl.state(), FollowerLiveness::State::Unarmed);
    EXPECT_FALSE(fl.decide(1'000).sendAppendPosition);
}

// ── Transition: Unarmed --onBecomeFollower--> Following ──────────────────────
TEST(FollowerLiveness, BecomeFollowerEntersFollowingAndSendsImmediately) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onBecomeFollower(1'000);
    EXPECT_EQ(fl.state(), FollowerLiveness::State::Following);
    auto a = fl.decide(1'000);
    EXPECT_TRUE(a.sendAppendPosition);
    EXPECT_FALSE(a.triggerReElection);
}

// ── REGRESSION: Following keeps sending on the interval FOREVER (never silent) ─
TEST(FollowerLivenessRegression, KeepsSendingEveryStatusIntervalForever) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    int64_t now = 1'000;
    fl.onBecomeFollower(now);

    int sends = 0;
    for (int i = 0; i < 200; ++i) {
        fl.onLeaderHeartbeat(now);          // leader still alive
        auto a = fl.decide(now);
        if (a.sendAppendPosition) ++sends;
        EXPECT_FALSE(a.triggerReElection) << "live leader must never trigger re-election";
        now += kSend;                        // advance exactly one interval
    }
    EXPECT_GE(sends, 199) << "follower must publish AppendPosition every interval, "
                             "never go silent (the eligible-set-collapse regression)";
}

// Following does NOT spam sends between intervals.
TEST(FollowerLiveness, DoesNotSendBetweenIntervals) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onBecomeFollower(1'000);
    EXPECT_TRUE(fl.decide(1'000).sendAppendPosition);              // first send
    EXPECT_FALSE(fl.decide(1'000 + kSend / 2).sendAppendPosition); // too soon
    EXPECT_TRUE(fl.decide(1'000 + kSend).sendAppendPosition);      // interval elapsed
}

// A live leader (heartbeats within timeout) never triggers re-election.
TEST(FollowerLiveness, LiveLeaderNoReElection) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    int64_t now = 0;
    fl.onBecomeFollower(now);
    for (int i = 0; i < 100; ++i) {
        now += kTimeout / 2;          // heartbeat well within the timeout
        fl.onLeaderHeartbeat(now);
        EXPECT_FALSE(fl.decide(now).triggerReElection);
    }
}

// ── Transition: Following --decide()==reElect--> (caller resets) ─────────────
TEST(FollowerLiveness, DeadLeaderTriggersReElection) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onBecomeFollower(0);
    EXPECT_FALSE(fl.decide(kTimeout - 1).triggerReElection); // just before deadline
    EXPECT_TRUE(fl.decide(kTimeout).triggerReElection);      // at deadline, silent
}

// ── Transition: Following --reset--> Unarmed ─────────────────────────────────
TEST(FollowerLiveness, ResetReturnsToUnarmed) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onBecomeFollower(0);
    EXPECT_TRUE(fl.armed());
    fl.reset();
    EXPECT_EQ(fl.state(), FollowerLiveness::State::Unarmed);
    EXPECT_FALSE(fl.armed());
    auto a = fl.decide(10 * kTimeout);
    EXPECT_FALSE(a.sendAppendPosition) << "Unarmed must not send after reset";
    EXPECT_FALSE(a.triggerReElection);
}

// nextDeadlineNs reflects the sooner of (next send, leader expiry).
TEST(FollowerLiveness, NextDeadlineIsSoonerOfSendAndExpiry) {
    FollowerLiveness fl;
    fl.configure(kSend, kTimeout);
    fl.onBecomeFollower(1'000);
    fl.decide(1'000);   // first send done → lastAppendSent = 1'000
    // next send = 1'000+kSend; leader expiry = 1'000+kTimeout; kSend < kTimeout.
    EXPECT_EQ(fl.nextDeadlineNs(), 1'000 + kSend);
}
