// SecurityServingTest.cpp — issues #15 / #111 (decision M7): the two pure machines that decide whether a broker may
// serve logins, `__security` reads and security DDL (SecurityServing.h).  Deterministic: the clock is a plain number.
//   SecurityCatchUp  (admin thread)   - caught up since boot with the current leader, and the contact stays fresh;
//   SecurityServing  (control thread) - CatchingUp | Serving | Stale, the consensus contact, the session end after R.
#include <rune/node/AdminChannels.h>
#include <rune/node/SecurityServing.h>

#include <gtest/gtest.h>

#include <limits>

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
// A leader: caught up once SecurityAdmin serves (its whole log committed and applied).  PR #459 F2 (a deliberate change
// of this test, not a weakening): a DEMOTED leader re-earns the verdict as a follower - its leader-era verdict says
// nothing about the new leader's log.  A FOLLOWER's verdict survives a leader change within F (and follows the new
// epoch); a promoted follower keeps it for F while it catches up as leader, and loses it if it never serves.
TEST(SecurityServingRegression, ALeaderIsCaughtUpWhileItServesAndAFailoverWithinFKeepsItRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leader = true; in.leaderServing = false; in.epoch = 1;
    EXPECT_FALSE(c.step(in)) << "a fresh leader catching up";
    in.nowNs = 5 * kMs; in.leaderServing = true; in.appliedNext = 4; in.highWatermark = 4;
    EXPECT_TRUE(c.step(in));
    EXPECT_TRUE(c.verdictAsLeader());
    // Demoted: a follower of the next epoch must re-earn it.
    in.nowNs = 20 * kMs; in.leader = false; in.leaderServing = false; in.epoch = 2; in.caughtUpWithLeader = false;
    EXPECT_FALSE(c.step(in)) << "a leader-era verdict does not survive the demotion";
    in.nowNs = 30 * kMs; in.leaderAnswers = 1; in.caughtUpWithLeader = true;
    EXPECT_TRUE(c.step(in)) << "re-earned: its new leader answered and it holds and applied the leader's HW";
    EXPECT_EQ(c.verdictEpoch(), 2);
    // A follower's verdict survives a failover within F and follows the new epoch.
    in.nowNs = 40 * kMs; in.epoch = 3; in.caughtUpWithLeader = false;
    EXPECT_TRUE(c.step(in));
    EXPECT_EQ(c.verdictEpoch(), 3);
    // Promoted but never serving (cannot commit: a minority) - lost F after the last contact.
    in.leader = true; in.epoch = 4;
    in.nowNs = 30 * kMs + kF;
    EXPECT_TRUE(c.step(in));
    in.nowNs = 31 * kMs + kF;
    EXPECT_FALSE(c.step(in));
    in.nowNs = 40 * kMs + kF; in.leaderServing = true;
    EXPECT_TRUE(c.step(in));
}

// PR #459 F2: multi-node, a verdict counts only for the committed epoch and role - a healed zombie's leader-era verdict
// (an older epoch) serves nothing when the contact returns, until the admin thread vouches for the committed one.
TEST(SecurityServingRegression, AVerdictForAnOtherEpochOrRoleThanTheCommittedOneIsNotCurrentRegression) {
    SecurityServing s(kF, kR);
    s.onAdminCaughtUp(true, 3, /*epoch=*/1, /*asLeader=*/true);
    SecurityServing::Committed now{1, true, true, 5};
    ASSERT_TRUE(s.step(0, true, &now).changed);
    ASSERT_TRUE(s.serving());
    EXPECT_TRUE(s.step(kF + 1, false, &now).changed) << "cut off: stale";
    // Healed - the FIRST passes still see the old map (epoch 1, self leads): never current on it.
    EXPECT_FALSE(s.step(kF + 2, true, &now).changed);
    now.mapCurrent = false; now.mapAnswers = 6;
    EXPECT_FALSE(s.step(kF + 3, true, &now).changed);
    EXPECT_FALSE(s.serving()) << "the map has not caught up since the contact returned";
    SecurityServing::Committed healed{2, false, true, 9};   // the map caught up: epoch 2, another leader
    EXPECT_FALSE(s.step(kF + 4, true, &healed).changed);
    EXPECT_FALSE(s.serving()) << "contact back, map current, but the verdict is for epoch 1 as leader";
    s.onAdminCaughtUp(false, 3, -1, false);
    EXPECT_FALSE(s.step(kF + 5, true, &healed).changed);
    s.onAdminCaughtUp(true, 4, 2, false);
    EXPECT_TRUE(s.step(kF + 6, true, &healed).changed);
    EXPECT_TRUE(s.serving());
    s.onAdminCaughtUp(true, 4, 2, true);   // the right epoch, the wrong role
    EXPECT_TRUE(s.step(kF + 7, true, &healed).changed);
    EXPECT_FALSE(s.serving());
}

// PR #459 F2: after a lost consensus contact the map is trusted only once a metadata answer arrived AFTER the contact
// returned and the map is current - an answer counted before (the frozen map's own) never counts.
TEST(SecurityServingRegression, AfterALostContactTheMapIsTrustedOnlyAfterAFreshAnswerRegression) {
    SecurityServing s(kF, kR);
    s.onAdminCaughtUp(true, 3, 1, true);
    SecurityServing::Committed c{1, true, true, 10};
    ASSERT_TRUE(s.step(0, true, &c).changed);
    (void)s.step(kF + 1, false, &c);           // stale
    c.mapAnswers = 11;                          // an answer while still cut off (in flight): not after the return
    (void)s.step(kF + 2, false, &c);
    (void)s.step(kF + 3, true, &c);             // contact back: the baseline is taken now (11)
    EXPECT_FALSE(s.serving());
    (void)s.step(kF + 4, true, &c);
    EXPECT_FALSE(s.serving()) << "no answer since the return";
    c.mapAnswers = 12; c.mapCurrent = false;
    (void)s.step(kF + 5, true, &c);
    EXPECT_FALSE(s.serving()) << "answered, but not caught up";
    c.mapAnswers = 13; c.mapCurrent = true;
    EXPECT_TRUE(s.step(kF + 6, true, &c).changed);
    EXPECT_TRUE(s.serving());
    // A short blip (within F) owes nothing.
    (void)s.step(kF + 7, false, &c);
    EXPECT_TRUE(s.serving());
    (void)s.step(kF + 8, true, &c);
    EXPECT_TRUE(s.serving());
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

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #459 (issues #15 / #111).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Input bounds: the admin->control verdict frame, truncated at every length and with every first byte.
TEST(SecurityServingRegression, TheVerdictFrameTruncatedOrMalformedIsRefusedAdversarialRegression) {
    uint8_t buf[rune::node::kSecurityServingWireLen + 4]{};
    rune::node::writeSecurityServing(buf, true, 0x0102030405060708LL);
    bool up = false; int64_t hw = 0;
    for (std::size_t n = 0; n <= sizeof buf; ++n)
        EXPECT_EQ(rune::node::decodeSecurityServing(buf, n, up, hw), n == rune::node::kSecurityServingWireLen) << n;
    ASSERT_TRUE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw));
    EXPECT_TRUE(up);
    EXPECT_EQ(hw, 0x0102030405060708LL);
    for (int v = 2; v < 256; ++v) {
        buf[0] = static_cast<uint8_t>(v);
        EXPECT_FALSE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw)) << v;
    }
}

// The zombie heals (unit layer of AuthRestartAdversarialRegression.AHealedZombie...): it "led" the whole partition (its
// own contact refreshed every pass), then learns it was replaced and follows the new leader, whose log is AHEAD of its
// own (the events it missed).  Its leader-era verdict says nothing about the new leader's log: it must not stay caught up
// until it caught up with that leader and applied what it missed.
TEST(SecurityServingRegression, ADemotedZombieIsNotCaughtUpUntilItCaughtUpWithItsNewLeaderAdversarialRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leader = true; in.leaderServing = true; in.epoch = 1; in.appliedNext = 3; in.highWatermark = 3;
    for (int64_t t = 0; t <= 20 * kF; t += 100 * kMs) { in.nowNs = t; ASSERT_TRUE(c.step(in)); }   // cut off, "leading"
    // Healed: demoted to a follower of epoch 2; the new leader's first answer says it holds 4 committed records.
    in.nowNs = 20 * kF + kMs; in.leader = false; in.leaderServing = false; in.epoch = 2;
    in.leaderAnswers = 1; in.highWatermark = 4; in.caughtUpWithLeader = false;
    EXPECT_FALSE(c.step(in)) << "behind its new leader (applied 3 < hw 4): not caught up";
    in.nowNs += 10 * kMs; in.leaderAnswers = 2;
    EXPECT_FALSE(c.step(in));
    in.nowNs += 10 * kMs; in.leaderAnswers = 3; in.caughtUpWithLeader = true; in.appliedNext = 4;
    EXPECT_TRUE(c.step(in)) << "caught up with the new leader";
}

// Flapping: the consensus contact toggles every pass (closed / not) - never longer than F apart - the broker stays
// serving and never ends a session; R counts from the START of a stale episode, and each episode ends sessions once.
TEST(SecurityServingRegression, AFlappingContactNeverMakesItStaleAndEachEpisodeEndsSessionsOnceAdversarialRegression) {
    SecurityServing s(kF, kR);
    s.onAdminCaughtUp(true, 1);
    ASSERT_TRUE(s.step(0, true).changed);
    for (int64_t t = 1; t < 50 * kF; t += kF / 2) {
        const auto a = s.step(t, (t / (kF / 2)) % 2 == 0);
        EXPECT_FALSE(a.endSessions) << t;
        EXPECT_TRUE(s.serving()) << t;
    }
    int64_t t = 100 * kF;
    (void)s.step(t, true);
    int ends = 0;
    for (int episode = 0; episode < 3; ++episode) {
        const int64_t start = t;
        while (t < start + kF + kR + 5 * kMs) { t += kMs; ends += s.step(t, false).endSessions ? 1 : 0; }
        EXPECT_EQ(ends, episode + 1) << "one end per stale episode";
        t += kMs;
        EXPECT_TRUE(s.step(t, true).changed);
        EXPECT_TRUE(s.serving());
    }
}

// #205 adversarial review (PR #516): the knownEnd field round-trips at its bounds, the epoch / role survive beside it,
// and the asLeader byte is still range-checked in the full overload.
TEST(SecurityServingRegression, TheKnownEndFieldRoundTripsAtItsBoundsAdversarialRegression) {
    for (const int64_t k : {int64_t{-1}, int64_t{0}, int64_t{1}, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min()}) {
        uint8_t buf[rune::node::kSecurityServingWireLen + 4];
        rune::node::writeSecurityServing(buf, true, 77, 9, true, k);
        bool up = false, leader = false; int64_t hw = 0, known = 123; int32_t epoch = 0;
        for (std::size_t n = 0; n <= sizeof buf; ++n)
            EXPECT_EQ(rune::node::decodeSecurityServing(buf, n, up, hw, epoch, leader, known), n == rune::node::kSecurityServingWireLen) << n;
        ASSERT_TRUE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw, epoch, leader, known));
        EXPECT_TRUE(up); EXPECT_EQ(hw, 77); EXPECT_EQ(epoch, 9); EXPECT_TRUE(leader); EXPECT_EQ(known, k);
        buf[13] = 2;
        EXPECT_FALSE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw, epoch, leader, known));
    }
    uint8_t d[rune::node::kSecurityServingWireLen];
    rune::node::writeSecurityServing(d, false, 5);   // the defaulted arguments: knownEnd -1 (never "behind")
    bool up = true, leader = true; int64_t hw = 0, known = 0; int32_t epoch = 0;
    ASSERT_TRUE(rune::node::decodeSecurityServing(d, sizeof d, up, hw, epoch, leader, known));
    EXPECT_EQ(known, -1);
}
