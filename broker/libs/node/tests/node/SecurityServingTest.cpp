// SecurityServingTest.cpp — issues #15 / #111 (decision M7): the two pure machines that decide whether a broker may
// serve logins, `__security` reads and security DDL (SecurityServing.h).  Deterministic: the clock is a plain number.
//   SecurityCatchUp  (admin thread)   - caught up since boot with the current leader, and the contact stays fresh;
//   SecurityServing  (control thread) - CatchingUp | Serving | Stale, the consensus contact, the session end after R.
#include <rune/node/SecurityServing.h>

#include <gtest/gtest.h>

using rune::node::SecurityCatchUp;
using rune::node::SecurityServing;

namespace {
constexpr int64_t kMs = 1'000'000;
constexpr int64_t kF  = 1'000 * kMs;
constexpr int64_t kR  = 10'000 * kMs;

/// A follower whose local log (a restart) already holds offsets [0, 10) applied - and whose leader has not answered.
SecurityCatchUp::Inputs restartedFollower(int64_t now) {
    SecurityCatchUp::Inputs in;
    in.nowNs = now; in.present = true; in.epoch = 3;
    in.appliedNext = 10; in.highWatermark = 0;   // a follower's HW reads 0 until its current leader answers
    in.leaderAnswers = 0; in.caughtUpWithLeader = false;
    return in;
}
}  // namespace

// Issue #15 (RED before the fix: nothing tracked this - a restarted broker served logins from its local prefix at once,
// e.g. the old password of a user DISABLEd while it was down).  Caught up only after its CURRENT leader answered since
// boot with nothing beyond its log end and every committed record applied.
TEST(SecurityReplicationRegression, CaughtUpIsPostedOnlyAfterALeaderFetchSinceBootRegression) {
    SecurityCatchUp c(kF);
    auto in = restartedFollower(0);
    EXPECT_FALSE(c.step(in)) << "the local prefix alone proves nothing";
    in.nowNs = 50 * kMs;
    EXPECT_FALSE(c.step(in));
    // The leader answers with records (we are behind: the DISABLE we missed): still not caught up.
    in.nowNs = 60 * kMs; in.leaderAnswers = 1; in.highWatermark = 12; in.appliedNext = 10;
    EXPECT_FALSE(c.step(in));
    // We hold everything (InSync) but have not applied the missed records yet.
    in.nowNs = 70 * kMs; in.leaderAnswers = 2; in.caughtUpWithLeader = true;
    EXPECT_FALSE(c.step(in)) << "applied 10 < hw 12";
    in.nowNs = 80 * kMs; in.appliedNext = 12;
    EXPECT_TRUE(c.step(in)) << "now it vouches for its state";
}

TEST(SecurityServingRegression, AWipedFollowerIsNotCaughtUpUntilItHoldsTheLeadersLogRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.appliedNext = 0; in.highWatermark = 0;
    for (int i = 0; i < 10; ++i) {   // the leader keeps answering with records: a wiped follower catching up
        in.nowNs = i * 10 * kMs; in.leaderAnswers = static_cast<uint64_t>(i + 1); in.highWatermark = 500;
        in.appliedNext = i * 40; in.caughtUpWithLeader = false;
        EXPECT_FALSE(c.step(in)) << i;
    }
    in.nowNs = 200 * kMs; in.leaderAnswers = 20; in.caughtUpWithLeader = true; in.appliedNext = 500;
    EXPECT_TRUE(c.step(in));
}

// Issue #111: a partitioned follower (no leader answer) and a follower whose applier stopped lose the verdict after F;
// the contact coming back is not enough - only a fresh full catch-up earns it again.
TEST(SecurityServingRegression, APartitionedFollowerLosesItAfterFAndEarnsItBackOnlyByCatchingUpRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.nowNs = 0; in.leaderAnswers = 1; in.caughtUpWithLeader = true; in.appliedNext = 5;
    in.highWatermark = 5;
    ASSERT_TRUE(c.step(in));
    in.nowNs = kF;                                  // silent for exactly F: still fresh
    EXPECT_TRUE(c.step(in));
    in.nowNs = kF + 1;                              // silent past F
    EXPECT_FALSE(c.step(in));
    in.nowNs = kF + 2 * kMs; in.leaderAnswers = 2; in.caughtUpWithLeader = false; in.highWatermark = 9;
    EXPECT_FALSE(c.step(in)) << "the leader answers again, with what we missed: not caught up yet";
    in.nowNs = kF + 4 * kMs; in.leaderAnswers = 3; in.caughtUpWithLeader = true; in.appliedNext = 9;
    EXPECT_TRUE(c.step(in));
}

TEST(SecurityServingRegression, AStuckApplierLosesItAfterFRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leaderAnswers = 1; in.caughtUpWithLeader = true; in.appliedNext = 5; in.highWatermark = 5;
    ASSERT_TRUE(c.step(in));
    for (int64_t t = 10 * kMs; t <= 3 * kF; t += 10 * kMs) {   // the leader keeps answering; the applier stops at 5
        in.nowNs = t; in.leaderAnswers += 1; in.highWatermark = 6;
        EXPECT_EQ(c.step(in), t <= kF) << t;
    }
}

// A leader: caught up once SecurityAdmin serves (its whole log committed and applied); a promotion keeps the
// follower's verdict for F (a failover never makes a healthy broker stale), a leader that never serves loses it.
TEST(SecurityServingRegression, ALeaderIsCaughtUpWhileItServesAndAFailoverWithinFKeepsItRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leader = true; in.leaderServing = false; in.epoch = 1;
    EXPECT_FALSE(c.step(in)) << "a fresh leader catching up";
    in.nowNs = 5 * kMs; in.leaderServing = true; in.appliedNext = 4; in.highWatermark = 4;
    EXPECT_TRUE(c.step(in));
    // Demoted, then a follower of the next epoch: its leader answers within F.
    in.nowNs = 20 * kMs; in.leader = false; in.leaderServing = false; in.epoch = 2; in.caughtUpWithLeader = false;
    EXPECT_TRUE(c.step(in)) << "within F of its last contact";
    in.nowNs = 30 * kMs; in.leaderAnswers = 1;
    EXPECT_TRUE(c.step(in));
    // Promoted again but never serving (cannot commit: a minority) - lost F after the last contact.
    in.leader = true; in.epoch = 3;
    in.nowNs = 30 * kMs + kF;
    EXPECT_TRUE(c.step(in));
    in.nowNs = 31 * kMs + kF;
    EXPECT_FALSE(c.step(in));
    in.nowNs = 40 * kMs + kF; in.leaderServing = true;
    EXPECT_TRUE(c.step(in));
}

TEST(SecurityServingRegression, AHaltedOrAbsentSecurityLogIsNeverCaughtUpRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leader = true; in.leaderServing = true;
    ASSERT_TRUE(c.step(in));
    in.halted = true;
    EXPECT_FALSE(c.step(in));
    SecurityCatchUp none(kF);
    SecurityCatchUp::Inputs absent;
    EXPECT_FALSE(none.step(absent));
}

// The control-thread machine.
TEST(SecurityServingRegression, ABootedBrokerServesOnlyOnceTheAdminThreadVouchesRegression) {
    SecurityServing s(kF, kR);
    EXPECT_FALSE(s.serving());
    EXPECT_STREQ(s.name(), "CatchingUp");
    EXPECT_FALSE(s.step(0, true).changed);
    EXPECT_FALSE(s.step(500 * kMs, true).endSessions) << "CatchingUp never ends sessions (there are none to trust yet)";
    EXPECT_FALSE(s.step(20 * kR, true).endSessions);
    s.onAdminCaughtUp(true, 7);
    const auto a = s.step(20 * kR + 1, true);
    EXPECT_TRUE(a.changed);
    EXPECT_TRUE(s.serving());
    EXPECT_EQ(s.leaderHw(), 7);
}

// Issue #111, the zombie leader: the admin thread still vouches (it believes it leads), but the consensus is gone - the
// broker goes Stale F after the last consensus contact and ends its sessions R later, exactly once; contact and catch-up
// back -> Serving.
TEST(SecurityServingRegression, AZombieLeaderGoesStaleWithoutTheConsensusThenEndsItsSessionsOnceRegression) {
    SecurityServing s(kF, kR);
    s.onAdminCaughtUp(true, 3);
    ASSERT_TRUE(s.step(0, true).changed);
    ASSERT_TRUE(s.serving());
    EXPECT_FALSE(s.step(kF, false).changed) << "within F of the last contact";
    EXPECT_TRUE(s.serving());
    const auto stale = s.step(kF + 1, false);
    EXPECT_TRUE(stale.changed);
    EXPECT_FALSE(s.serving());
    EXPECT_STREQ(s.name(), "Stale");
    EXPECT_FALSE(s.step(kF + 1 + kR, false).endSessions) << "exactly R: not yet";
    const auto end = s.step(kF + 2 + kR, false);
    EXPECT_TRUE(end.endSessions);
    EXPECT_FALSE(s.step(kF + 3 + kR, false).endSessions) << "once";
    EXPECT_TRUE(s.step(kF + 4 + kR, true).changed) << "contact back (admin still vouches): serving again";
    EXPECT_TRUE(s.serving());
}

TEST(SecurityServingRegression, AnAdminVerdictLossMakesItStaleAtOnceRegression) {
    SecurityServing s(kF, kR);
    s.onAdminCaughtUp(true, 3);
    ASSERT_TRUE(s.step(0, true).changed);
    s.onAdminCaughtUp(false, 3);
    EXPECT_TRUE(s.step(1, true).changed);
    EXPECT_FALSE(s.serving());
    s.onAdminCaughtUp(true, 4);
    EXPECT_TRUE(s.step(2, true).changed);
    EXPECT_TRUE(s.serving());
}
