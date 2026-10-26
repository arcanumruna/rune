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
    in.nowNs = kF + 1;                              // silent past F: issue #460 holds it (all it knows is applied);
    EXPECT_TRUE(c.step(in));                        // the control thread counts a hold only inside the ISR
    EXPECT_TRUE(c.held());
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
    EXPECT_TRUE(c.step(in)) << "issue #460: held (the control thread bounds it by the ISR and the consensus)";
    EXPECT_TRUE(c.held());
    in.highWatermark = 5;                           // a commit it has not applied: the hold ends
    EXPECT_FALSE(c.step(in));
    in.nowNs = 40 * kMs + kF; in.leaderServing = true;
    EXPECT_TRUE(c.step(in));
    EXPECT_FALSE(c.held());
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

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issue #603: SecurityBehind (admin thread) - is the applier below the committed HW this broker knows?  Stepped once per
// admin pass after the applier ran.  Every case also checks the edges: the verdict is posted on edges only.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
using rune::node::SecurityBehind;

// A held apply: the committed HW moves past what is applied - behind, ONE edge however many passes it lasts; the apply
// catching up clears it with one edge.  (RED before #603: nothing tracked it - a held follower served the old grants.)
TEST(SecurityBehindRegression, AHeldApplyIsBehindUntilItAppliedEveryCommittedRecordRegression) {
    SecurityBehind b;
    EXPECT_FALSE(b.step(true, false, 10, 10)) << "applied through the HW: current, no edge";
    EXPECT_FALSE(b.behind());
    EXPECT_TRUE(b.step(true, false, 10, 11)) << "a committed record the applier holds back: the edge";
    EXPECT_TRUE(b.behind());
    int edges = 0;
    for (int64_t hw = 11; hw < 1'000; ++hw) edges += b.step(true, false, 10, hw) ? 1 : 0;
    EXPECT_EQ(edges, 0) << "behind is posted once, never per pass";
    EXPECT_TRUE(b.behind());
    EXPECT_FALSE(b.step(true, false, 998, 999)) << "still one record short";
    EXPECT_TRUE(b.step(true, false, 999, 999)) << "applied everything: the clearing edge";
    EXPECT_FALSE(b.behind());
}

// Normal operation: a record arrives (the HW moves) and is applied in the SAME pass - the step sees both: never a flip.
TEST(SecurityBehindRegression, ARecordAppliedInTheSamePassNeverFlipsItRegression) {
    SecurityBehind b;
    int edges = 0;
    for (int64_t off = 0; off < 10'000; ++off) edges += b.step(true, false, off + 1, off + 1) ? 1 : 0;
    EXPECT_EQ(edges, 0);
    EXPECT_FALSE(b.behind());
    // Several records per pass, also applied in it.
    for (int64_t off = 10'000; off < 20'000; off += 7) edges += b.step(true, false, off, off) ? 1 : 0;
    EXPECT_EQ(edges, 0);
}

// A halted applier never applies again (D28): behind whatever the offsets say, and it stays behind.
TEST(SecurityBehindRegression, AHaltedApplierIsBehindForGoodRegression) {
    SecurityBehind b;
    ASSERT_FALSE(b.step(true, false, 5, 5));
    EXPECT_TRUE(b.step(true, true, -1, 5)) << "halted: behind";
    EXPECT_FALSE(b.step(true, true, -1, 5));
    EXPECT_FALSE(b.step(true, true, 5, 5)) << "still halted (a stale appliedNext changes nothing)";
    EXPECT_TRUE(b.behind());
}

// A LEADER with an unapplied tail (its own HW past what it applied - a slow apply, or a promotion's previous tail):
// behind exactly like a follower.
TEST(SecurityBehindRegression, ALeaderWithAnUnappliedTailIsBehindRegression) {
    SecurityBehind b;
    ASSERT_FALSE(b.step(true, false, 20, 20));
    EXPECT_TRUE(b.step(true, false, 20, 23)) << "its own HW 23, applied 20";
    EXPECT_FALSE(b.step(true, false, 22, 23));
    EXPECT_TRUE(b.behind());
    EXPECT_TRUE(b.step(true, false, 23, 23));
    EXPECT_FALSE(b.behind());
}

// A leader change: a follower's leaderCommittedHw reads 0 until its new leader answers.  That is no news that the held
// records went away (a committed offset never moves back) - it stays behind until it applied them.
TEST(SecurityBehindRegression, ALeaderChangeResettingTheAdvertisedHwNeverClearsItRegression) {
    SecurityBehind b;
    ASSERT_TRUE(b.step(true, false, 10, 12));
    EXPECT_FALSE(b.step(true, false, 10, 0)) << "the new leader has not answered yet";
    EXPECT_TRUE(b.behind());
    EXPECT_EQ(b.knownCommittedHw(), 12);
    EXPECT_FALSE(b.step(true, false, 11, 12)) << "the new leader's first answer";
    EXPECT_TRUE(b.step(true, false, 12, 0)) << "applied what it held: cleared even before the new leader answers";
}

// No `__security` replica / applier (auth off, a rig): never behind.
TEST(SecurityBehindRegression, NoSecurityReplicaIsNeverBehindRegression) {
    SecurityBehind b;
    for (int i = 0; i < 10; ++i) EXPECT_FALSE(b.step(false, false, -1, 100 + i));
    EXPECT_FALSE(b.behind());
}

// Table-driven: every (present, halted, applied vs hw) combination from a current start.
TEST(SecurityBehindRegression, EveryInputCombinationMatchesTheRuleRegression) {
    for (const bool present : {false, true})
        for (const bool halted : {false, true})
            for (const int64_t applied : {int64_t{-1}, int64_t{0}, int64_t{4}, int64_t{5}, int64_t{6}})
                for (const int64_t hw : {int64_t{0}, int64_t{5}}) {
                    SecurityBehind b;
                    const bool want = present && (halted || applied < hw);
                    EXPECT_EQ(b.step(present, halted, applied, hw), want) << present << halted << applied << hw;
                    EXPECT_EQ(b.behind(), want) << present << halted << applied << hw;
                }
}

// The wire: the behind byte round-trips, defaults to false, and is range-checked; a pre-#603 22-byte frame is refused
// (the control thread halts its mirror on it: a dropped "behind" would be fail-open).
TEST(SecurityBehindRegression, TheBehindByteRoundTripsAndIsRangeCheckedAdversarialRegression) {
    for (const bool behind : {false, true}) {
        uint8_t buf[rune::node::kSecurityServingWireLen + 4]{};
        rune::node::writeSecurityServing(buf, true, 7, 3, false, 9, behind);
        bool up = false, leader = true, got = !behind; int64_t hw = 0, known = 0; int32_t epoch = 0;
        for (std::size_t n = 0; n <= sizeof buf; ++n)
            EXPECT_EQ(rune::node::decodeSecurityServing(buf, n, up, hw, epoch, leader, known, got),
                      n == rune::node::kSecurityServingWireLen) << n;
        ASSERT_TRUE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw, epoch, leader, known, got));
        EXPECT_EQ(got, behind);
        EXPECT_EQ(known, 9);
        for (int v = 2; v < 256; ++v) {
            buf[22] = static_cast<uint8_t>(v);
            EXPECT_FALSE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw)) << v;
        }
    }
    EXPECT_EQ(rune::node::kSecurityServingWireLen, 24u);
    uint8_t d[rune::node::kSecurityServingWireLen];
    rune::node::writeSecurityServing(d, true, 1);
    bool up = false, leader = false, behind = true; int64_t hw = 0, known = 0; int32_t epoch = 0;
    ASSERT_TRUE(rune::node::decodeSecurityServing(d, sizeof d, up, hw, epoch, leader, known, behind));
    EXPECT_FALSE(behind) << "the defaulted argument";
    EXPECT_FALSE(rune::node::decodeSecurityServing(d, 22, up, hw)) << "the pre-#603 layout";
    EXPECT_FALSE(rune::node::decodeSecurityServing(d, 23, up, hw)) << "the pre-#460 layout";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issue #460: a `__security` leader crash must never make the survivors stale - the election plus the reassignment can
// exceed F, so a follower whose own apply is current HOLDS its verdict while its leader is silent, and the control
// thread counts a hold only while this broker is in the committed `__security` ISR of a current map.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
SecurityCatchUp::Inputs caughtUpFollower(int64_t now) {
    SecurityCatchUp::Inputs in;
    in.present = true; in.nowNs = now; in.epoch = 1; in.leaderAnswers = 1; in.caughtUpWithLeader = true;
    in.appliedNext = 5; in.highWatermark = 5;
    return in;
}
} // namespace

TEST(SecurityServingRegression, AFollowerHoldsItsVerdictWhileItsLeaderIsSilentAndEverythingIsAppliedRegression) {
    SecurityCatchUp c(kF);
    auto in = caughtUpFollower(0);
    ASSERT_TRUE(c.step(in));
    ASSERT_FALSE(c.held());
    for (int64_t t = 10 * kMs; t <= 5 * kF; t += 10 * kMs) {   // the leader is gone for 5F; nothing new committed
        in.nowNs = t;
        if (t == 2 * kF) in.epoch = 2;                         // a new leader elected, not answering yet
        ASSERT_TRUE(c.step(in)) << t;
        EXPECT_EQ(c.held(), t > kF) << t;
        EXPECT_FALSE(c.verdictAsLeader());
        EXPECT_EQ(c.verdictEpoch(), t >= 2 * kF ? 2 : 1) << t;
    }
    // The new leader answers with nothing beyond our log: a full verdict again.
    in.nowNs += kMs; in.leaderAnswers = 2;
    ASSERT_TRUE(c.step(in));
    EXPECT_FALSE(c.held());
    EXPECT_EQ(c.verdictEpoch(), 2);
}

TEST(SecurityServingRegression, AHoldEndsOnAnUnappliedCommitAHaltOrALostLogRegression) {
    for (int k = 0; k < 4; ++k) {
        SecurityCatchUp c(kF);
        auto in = caughtUpFollower(0);
        ASSERT_TRUE(c.step(in));
        in.nowNs = kF + 1;
        ASSERT_TRUE(c.step(in));
        ASSERT_TRUE(c.held()) << k;
        in.nowNs = kF + 2;
        switch (k) {
            case 0: in.highWatermark = 6; break;                  // a commit it has not applied (the answer came back)
            case 1: in.highWatermark = 6; in.leaderAnswers = 2; in.caughtUpWithLeader = false; break;
            case 2: in.halted = true; break;
            case 3: in.present = false; break;
        }
        EXPECT_FALSE(c.step(in)) << k;
        EXPECT_FALSE(c.held()) << k;
        EXPECT_FALSE(c.caughtUp()) << k;
        // And it is never resumed: the old state back is not a fresh catch-up (no new answer).
        // The answer counter and the HW are monotonic: keep both (no new answer, nothing un-committed).
        const uint64_t answers = in.leaderAnswers;
        const int64_t hw = in.highWatermark;
        in = caughtUpFollower(kF + 3);
        in.leaderAnswers = answers;
        in.highWatermark = hw;
        EXPECT_FALSE(c.step(in)) << k;
    }
}

TEST(SecurityServingRegression, AFollowerBehindWhenItsLeaderGoesSilentIsNeverHeldRegression) {
    SecurityCatchUp c(kF);
    auto in = caughtUpFollower(0);
    ASSERT_TRUE(c.step(in));
    in.nowNs = kF / 2; in.highWatermark = 7;   // a commit arrives, not applied yet, then the leader goes silent
    ASSERT_TRUE(c.step(in));
    in.nowNs = kF + 1;
    EXPECT_FALSE(c.step(in));
    EXPECT_FALSE(c.held());
}

TEST(SecurityServingRegression, ALeaderEraVerdictIsNeverHeldRegression) {
    SecurityCatchUp c(kF);
    SecurityCatchUp::Inputs in;
    in.present = true; in.leader = true; in.leaderServing = true; in.epoch = 1; in.appliedNext = 4; in.highWatermark = 4;
    ASSERT_TRUE(c.step(in));
    ASSERT_TRUE(c.verdictAsLeader());
    in.leaderServing = false;                          // still leads but no longer serves (a zombie losing its quorum)
    for (int64_t t = kMs; t <= 3 * kF; t += 10 * kMs) {
        in.nowNs = t;
        const bool v = c.step(in);
        EXPECT_FALSE(c.held()) << t;
        EXPECT_EQ(v, t <= kF) << t;
    }
}

// The control thread: a held verdict serves only inside the committed ISR of a current map, within F of the consensus.
TEST(SecurityServingRegression, AHeldVerdictServesOnlyInTheCommittedIsrOfACurrentMapRegression) {
    SecurityServing s(kF, kR);
    SecurityServing::Committed c{1, false, true, 10, false, true};
    s.onAdminCaughtUp(true, 5, 1, false);
    ASSERT_TRUE(s.step(0, true, &c).changed);
    ASSERT_TRUE(s.serving());
    // The leader crashed: the verdict is held, the epoch moves on, the election is open for less than F.
    s.onAdminCaughtUp(true, 5, 1, false, /*held=*/true);
    EXPECT_FALSE(s.step(kF / 2, true, &c).changed);
    EXPECT_TRUE(s.serving());
    c.epoch = 2;
    s.onAdminCaughtUp(true, 5, 2, false, true);
    for (int64_t t = kF / 2; t <= 5 * kF; t += 50 * kMs) {
        const bool closed = t < kF / 2 + 800 * kMs ? (t / kMs) % 2 == 0 : true;   // a re-election blip, never > F
        (void)s.step(t, closed, &c);
        EXPECT_TRUE(s.serving()) << t;
    }
    // Shrunk out of the ISR: stale at once.
    c.selfInIsr = false;
    EXPECT_TRUE(s.step(5 * kF + 1, true, &c).changed);
    EXPECT_FALSE(s.serving());
    c.selfInIsr = true;
    EXPECT_TRUE(s.step(5 * kF + 2, true, &c).changed);
    EXPECT_TRUE(s.serving());
    // The map not current: stale.
    c.mapCurrent = false;
    EXPECT_TRUE(s.step(5 * kF + 3, true, &c).changed);
    EXPECT_FALSE(s.serving());
    c.mapCurrent = true;
    ASSERT_TRUE(s.step(5 * kF + 4, true, &c).changed);
    // The consensus lost past F: stale (the ISR in a frozen map proves nothing).
    EXPECT_FALSE(s.step(6 * kF + 4, false, &c).changed);
    EXPECT_TRUE(s.step(6 * kF + 5, false, &c).changed);
    EXPECT_FALSE(s.serving());
    // A full (not held) verdict needs no ISR membership.
    SecurityServing f(kF, kR);
    SecurityServing::Committed out{2, false, true, 10, false, false};
    f.onAdminCaughtUp(true, 5, 2, false);
    EXPECT_TRUE(f.step(0, true, &out).changed);
    EXPECT_TRUE(f.serving());
    f.onAdminCaughtUp(true, 5, 2, false, true);
    EXPECT_TRUE(f.step(1, true, &out).changed);
    EXPECT_FALSE(f.serving()) << "held and not in the ISR";
    // `held` without `caughtUp` is no verdict at all.
    f.onAdminCaughtUp(false, 5, 2, false, true);
    out.selfInIsr = true;
    EXPECT_FALSE(f.step(2, true, &out).changed);
    EXPECT_FALSE(f.serving());
}

// The wire: the held byte round-trips, defaults to false and is range-checked; the pre-#460 23-byte frame is refused.
TEST(SecurityServingRegression, TheHeldByteRoundTripsAndIsRangeCheckedAdversarialRegression) {
    for (const bool held : {false, true}) {
        uint8_t buf[rune::node::kSecurityServingWireLen + 4]{};
        rune::node::writeSecurityServing(buf, true, 7, 3, false, 9, true, held);
        bool up = false, leader = true, behind = false, got = !held; int64_t hw = 0, known = 0; int32_t epoch = 0;
        for (std::size_t n = 0; n <= sizeof buf; ++n)
            EXPECT_EQ(rune::node::decodeSecurityServing(buf, n, up, hw, epoch, leader, known, behind, got),
                      n == rune::node::kSecurityServingWireLen) << n;
        ASSERT_TRUE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw, epoch, leader, known,
                                                      behind, got));
        EXPECT_EQ(got, held);
        EXPECT_TRUE(behind);
        EXPECT_EQ(known, 9);
        EXPECT_EQ(epoch, 3);
        for (int v = 2; v < 256; ++v) {
            buf[23] = static_cast<uint8_t>(v);
            EXPECT_FALSE(rune::node::decodeSecurityServing(buf, rune::node::kSecurityServingWireLen, up, hw)) << v;
        }
    }
    uint8_t d[rune::node::kSecurityServingWireLen];
    rune::node::writeSecurityServing(d, true, 1);
    bool up = false, leader = false, behind = true, held = true; int64_t hw = 0, known = 0; int32_t epoch = 0;
    ASSERT_TRUE(rune::node::decodeSecurityServing(d, sizeof d, up, hw, epoch, leader, known, behind, held));
    EXPECT_FALSE(held) << "the defaulted argument";
    EXPECT_FALSE(rune::node::decodeSecurityServing(d, 23, up, hw)) << "the pre-#460 layout";
}
