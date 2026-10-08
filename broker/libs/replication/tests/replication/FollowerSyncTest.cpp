// ---------------------------------------------------------------------------
// FollowerSyncTest.cpp — exhaustive unit tests for the follower replication
// sync state machine (rune::replication::FollowerSync).
//
// FollowerSync is pure logic (no clock/I/O/Aeron), so this test links ONLY the
// header + gtest — it builds and runs on any platform, unlike the rest of the
// replication test suite which needs a real Aeron/Linux toolchain.
//
// Coverage map (every branch + every documented edge case):
//   bootstrap                    → BootstrapEmitsDataFetchImmediately
//   fast-cadence throttle        → BehindThrottledByFastCadence,
//                                  DecideDoesNotDoubleEmitSameTick
//   single-outstanding gate      → BehindSingleOutstanding_NoRefetch,
//                                  SingleOutstandingInvariantOverResendWindow
//   lost-response resend         → BehindResendAfterTimeout,
//                                  ResendIntervalIsMaxOfFastAndResend
//   records ⟹ stay Behind + clear → ResponseWithRecordsStaysBehindClearsFetch
//   empty ⟹ InSync               → EmptyResponseTransitionsToInSync
//   InSync keepalive cadence     → InSyncPureAckKeepaliveCadence,
//                                  InSyncNoEmitBeforeKeepalive,
//                                  InSyncKeepaliveCarriesLastEmitAcrossTransition
//   push flips back to Behind    → NewRecordsFlipsInSyncBackToBehind
//   role change                  → ResetReturnsToBootstrap,
//                                  ResetClearsStaleDeadline
//   leader-HW observability      → LeaderHwIsMonotonic
//   armed reflects outstanding   → FetchArmedReflectsOutstanding
//   want-data flag per state     → WantDataFlagMatchesState
//   full lifecycle               → FullCatchUpThenSteadyStateSequence
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include "rune/replication/FollowerSync.h"

using rune::replication::FollowerSync;
using State  = FollowerSync::State;
using Action = FollowerSync::Action;

namespace {

// Cadence constants mirroring realistic broker values (ns).
constexpr int64_t kFast   = 5'000'000;    // 5 ms  fast catch-up cadence
constexpr int64_t kKeep   = 100'000'000;  // 100 ms InSync keepalive cadence
constexpr int64_t kResend = 20'000'000;   // 20 ms lost-response resend timeout
constexpr int64_t kBase   = 1'000'000'000'000LL;  // a realistic steady_clock ns

Action step(FollowerSync& fs, int64_t nowNs,
            int64_t fast = kFast, int64_t keep = kKeep, int64_t resend = kResend) {
    return fs.decide(nowNs, fast, keep, resend);
}

} // namespace

// ── Bootstrap ────────────────────────────────────────────────────────────────

TEST(FollowerSync, DefaultsToBehindBootstrap) {
    FollowerSync fs;
    EXPECT_EQ(fs.state(), State::Behind);
    EXPECT_FALSE(fs.inSync());
    EXPECT_FALSE(fs.fetchArmed());
    EXPECT_EQ(fs.leaderHw(), 0);
}

TEST(FollowerSync, BootstrapEmitsDataFetchImmediately) {
    FollowerSync fs;
    const Action a = step(fs, kBase);
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);                 // bootstrap must pull data
    EXPECT_TRUE(fs.fetchArmed());            // single-outstanding armed
    EXPECT_EQ(fs.state(), State::Behind);
}

// ── Fast-cadence throttle ────────────────────────────────────────────────────

TEST(FollowerSync, BehindThrottledByFastCadence) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);
    // Clear the outstanding fetch so ONLY the fast-cadence throttle is under test.
    fs.onResponse(/*hadRecords=*/true, 10);
    EXPECT_FALSE(fs.fetchArmed());
    // Just before the fast window elapses → no emit.
    const Action a = step(fs, kBase + kFast - 1);
    EXPECT_FALSE(a.emit);
    // Exactly at the fast window → emit.
    const Action b = step(fs, kBase + kFast);
    EXPECT_TRUE(b.emit);
    EXPECT_TRUE(b.wantData);
}

TEST(FollowerSync, DecideDoesNotDoubleEmitSameTick) {
    FollowerSync fs;
    EXPECT_TRUE(step(fs, kBase).emit);
    EXPECT_FALSE(step(fs, kBase).emit);      // same instant → throttled, no double fetch
}

// ── Single-outstanding gate ──────────────────────────────────────────────────

TEST(FollowerSync, BehindSingleOutstanding_NoRefetchWhileInFlight) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);       // armed until kBase + kResend
    // Past the fast window, but the data-fetch is still in flight → NO re-request.
    // This is the whole point: the follower's LEO hasn't advanced only because the
    // response is still in transit; re-requesting would flood its ingress ring.
    const Action a = step(fs, kBase + kFast + 1);
    EXPECT_FALSE(a.emit);
    EXPECT_TRUE(fs.fetchArmed());
}

TEST(FollowerSync, SingleOutstandingInvariantOverResendWindow) {
    FollowerSync fs;
    int emits = 0;
    if (step(fs, kBase).emit) ++emits;
    // Tick every 1 ms across the whole resend window with NO response: exactly one
    // fetch must have been emitted (the initial one) — never a flood.
    for (int64_t ms = 1; ms < kResend / 1'000'000; ++ms) {
        if (step(fs, kBase + ms * 1'000'000).emit) ++emits;
    }
    EXPECT_EQ(emits, 1);
}

// ── Lost-response resend ─────────────────────────────────────────────────────

TEST(FollowerSync, BehindResendAfterTimeout) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);       // armed until kBase + kResend
    // No response arrives; once the resend deadline passes, re-send the data-fetch.
    const Action a = step(fs, kBase + kResend + 1);
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);
    EXPECT_TRUE(fs.fetchArmed());            // re-armed
}

TEST(FollowerSync, ResendIntervalIsMaxOfFastAndResend) {
    // resend < fast: the effective resend interval is max(fast, resend) = fast.
    FollowerSync fs;
    constexpr int64_t kSmallResend = 1'000'000;  // 1 ms < 5 ms fast
    ASSERT_TRUE(step(fs, kBase, kFast, kKeep, kSmallResend).emit);
    // Past the (tiny) resend deadline but still within the fast window → no emit.
    EXPECT_FALSE(step(fs, kBase + 2'000'000, kFast, kKeep, kSmallResend).emit);
    // Past the fast window → emit.
    EXPECT_TRUE(step(fs, kBase + kFast, kFast, kKeep, kSmallResend).emit);
}

// ── records ⟹ stay Behind and clear outstanding ──────────────────────────────

TEST(FollowerSync, ResponseWithRecordsStaysBehindClearsFetch) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);
    fs.onResponse(/*hadRecords=*/true, 100);
    EXPECT_EQ(fs.state(), State::Behind);    // more to pull
    EXPECT_FALSE(fs.fetchArmed());           // leader answered → outstanding cleared
    // Next tick past the fast window issues a fresh fetch from the advanced LEO.
    const Action a = step(fs, kBase + kFast);
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);
}

// ── empty ⟹ InSync ───────────────────────────────────────────────────────────

TEST(FollowerSync, EmptyResponseTransitionsToInSync) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);
    fs.onResponse(/*hadRecords=*/false, 100);
    EXPECT_EQ(fs.state(), State::InSync);
    EXPECT_TRUE(fs.inSync());
    EXPECT_FALSE(fs.fetchArmed());
}

// ── InSync keepalive cadence ─────────────────────────────────────────────────

TEST(FollowerSync, InSyncDataKeepaliveCadence) {
    FollowerSync fs;
    fs.onResponse(/*hadRecords=*/false, 100);   // → InSync, lastEmit still 0
    const Action a = step(fs, kBase);           // kBase - 0 ≥ keep → emit
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);                     // DATA keepalive — pulls any missed push
    // Just before the keepalive window → no emit.
    EXPECT_FALSE(step(fs, kBase + kKeep - 1).emit);
    // At the keepalive window → emit again, still a data keepalive.
    const Action c = step(fs, kBase + kKeep);
    EXPECT_TRUE(c.emit);
    EXPECT_TRUE(c.wantData);
}

TEST(FollowerSync, InSyncNoEmitBeforeKeepalive) {
    FollowerSync fs;
    fs.onResponse(false, 100);
    ASSERT_TRUE(step(fs, kBase).emit);          // sets lastEmit = kBase
    EXPECT_FALSE(step(fs, kBase + 1).emit);     // immediately after → throttled
}

TEST(FollowerSync, InSyncKeepaliveCarriesLastEmitAcrossTransition) {
    // A fast empty response must NOT trigger a spurious immediate keepalive:
    // lastEmit is preserved across the Behind→InSync transition.
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);          // Behind data-fetch, lastEmit = kBase
    fs.onResponse(false, 100);                  // empty response arrives quickly → InSync
    // Only 1 ms later — well within the keepalive window measured from lastEmit.
    EXPECT_FALSE(step(fs, kBase + 1'000'000).emit);
    // Once a full keepalive window has elapsed since lastEmit → emit.
    EXPECT_TRUE(step(fs, kBase + kKeep).emit);
}

// ── push flips InSync back to Behind ─────────────────────────────────────────

TEST(FollowerSync, NewRecordsFlipsInSyncBackToBehind) {
    FollowerSync fs;
    fs.onResponse(false, 100);                  // InSync
    ASSERT_EQ(fs.state(), State::InSync);
    fs.onResponse(/*hadRecords=*/true, 150);    // new data (e.g. via GR-10 push apply)
    EXPECT_EQ(fs.state(), State::Behind);       // must resume catch-up fetches
    EXPECT_FALSE(fs.fetchArmed());
}

// ── role change (reset) ──────────────────────────────────────────────────────

TEST(FollowerSync, ResetReturnsToBootstrap) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);
    fs.onResponse(false, 500);                  // InSync + leaderHw = 500
    ASSERT_TRUE(fs.inSync());
    fs.reset();
    EXPECT_EQ(fs.state(), State::Behind);
    EXPECT_FALSE(fs.fetchArmed());
    EXPECT_EQ(fs.leaderHw(), 0);
    // A fresh follower re-bootstraps with an immediate data-fetch.
    const Action a = step(fs, kBase + 1'000'000'000);
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);
}

TEST(FollowerSync, ResetClearsStaleDeadline) {
    FollowerSync fs;
    ASSERT_TRUE(step(fs, kBase).emit);          // armed
    fs.reset();
    EXPECT_FALSE(fs.fetchArmed());              // stale deadline gone
    // Immediately after reset, the very next tick may fetch (not gated by old state).
    const Action a = step(fs, kBase + kFast + 1);
    EXPECT_TRUE(a.emit);
}

// ── leader-HW observability ──────────────────────────────────────────────────

TEST(FollowerSync, LeaderHwIsMonotonic) {
    FollowerSync fs;
    fs.onResponse(true, 100);
    EXPECT_EQ(fs.leaderHw(), 100);
    fs.onResponse(true, 50);                    // stale/lower → ignored
    EXPECT_EQ(fs.leaderHw(), 100);
    fs.onResponse(false, 200);
    EXPECT_EQ(fs.leaderHw(), 200);
}

// ── armed flag reflects outstanding fetch ────────────────────────────────────

TEST(FollowerSync, FetchArmedReflectsOutstanding) {
    FollowerSync fs;
    EXPECT_FALSE(fs.fetchArmed());
    step(fs, kBase);
    EXPECT_TRUE(fs.fetchArmed());               // data-fetch in flight
    fs.onResponse(true, 10);
    EXPECT_FALSE(fs.fetchArmed());              // response cleared it
}

// ── want-data flag per state ─────────────────────────────────────────────────

TEST(FollowerSync, WantDataFlagMatchesState) {
    FollowerSync fs;
    EXPECT_TRUE(step(fs, kBase).wantData);      // Behind → data
    fs.onResponse(false, 10);                   // → InSync
    EXPECT_TRUE(step(fs, kBase + kKeep).wantData);   // InSync → DATA keepalive
}

// ── full lifecycle ───────────────────────────────────────────────────────────

TEST(FollowerSync, FullCatchUpThenSteadyStateSequence) {
    FollowerSync fs;
    int64_t t = kBase;

    // 1) Bootstrap: fetch data.
    Action a = step(fs, t);
    ASSERT_TRUE(a.emit && a.wantData);

    // 2) Records arrive → still Behind, outstanding cleared.
    fs.onResponse(true, 1000);
    ASSERT_EQ(fs.state(), State::Behind);

    // 3) Next fast tick fetches again.
    t += kFast;
    a = step(fs, t);
    ASSERT_TRUE(a.emit && a.wantData);

    // 4) Empty response → InSync (caught up).
    fs.onResponse(false, 1000);
    ASSERT_EQ(fs.state(), State::InSync);

    // 5) Steady state: slow DATA keepalives (pull any missed push; empty when caught up).
    t += kKeep;
    a = step(fs, t);
    ASSERT_TRUE(a.emit);
    ASSERT_TRUE(a.wantData);

    // 6) A push delivers new data (applied) → back to Behind, resume fetching.
    fs.onResponse(true, 2000);
    ASSERT_EQ(fs.state(), State::Behind);
    t += kFast;
    a = step(fs, t);
    ASSERT_TRUE(a.emit && a.wantData);

    // 7) Leader now empty again → InSync, and leaderHw tracked the peak.
    fs.onResponse(false, 2000);
    ASSERT_EQ(fs.state(), State::InSync);
    EXPECT_EQ(fs.leaderHw(), 2000);
}

// ---------------------------------------------------------------------------
// REGRESSION (bug: acks=all "InSync stall" collapse).  Observed on the 3-node
// VM: a follower briefly caught up (empty response → InSync) then the leader
// burst ahead; the leader's GR-10 push of the new tail was dropped/back-pressured
// yet marked pushed, so the follower never received it.  With the OLD pure-ack
// keepalive (wantData=false) the follower NEVER pulled that gap — it stalled at
// its offset with the ISR still full, freezing HW → acks=all timed out → collapse.
// FIX: the InSync keepalive must be a DATA fetch so a silently-missed push is
// rediscovered and drained.  Suite ends in "Regression" for the suite filter.
// See [[rune-followersync-state-machine]], [[mark-regression-tests-as-suite]].
// ---------------------------------------------------------------------------
TEST(FollowerSyncRegression, InSyncKeepalivePullsDataToRecoverMissedPush) {
    FollowerSync fs;
    // Follower momentarily caught up → InSync (empty response).
    fs.onResponse(/*hadRecords=*/false, /*leaderHw=*/60646);
    ASSERT_EQ(fs.state(), FollowerSync::State::InSync);

    // Leader has since burst ahead and the push was lost.  The keepalive MUST pull
    // data (wantData=true), NOT a pure ack — otherwise the gap is never requested.
    const Action k = step(fs, kBase);
    EXPECT_TRUE(k.emit);
    EXPECT_TRUE(k.wantData)
        << "InSync keepalive must fetch data; a pure-ack keepalive stalls the follower "
           "forever behind a dropped push (the acks=all InSync-stall collapse)";

    // The keepalive discovers the missed tail → flips back to Behind → fast drain.
    fs.onResponse(/*hadRecords=*/true, /*leaderHw=*/212660);
    EXPECT_EQ(fs.state(), FollowerSync::State::Behind);
    const Action c = step(fs, kBase + kKeep + kFast);
    EXPECT_TRUE(c.emit);
    EXPECT_TRUE(c.wantData) << "must resume fast data fetches to drain the missed backlog";
}

// REGRESSION companion: an InSync follower that is GENUINELY caught up keeps issuing
// data keepalives on the keepalive cadence (they return empty) — it must never go
// silent (which is what stalled it), and must stay on the slow cadence (not spin).
TEST(FollowerSyncRegression, InSyncCaughtUpKeepsPollingOnCadence) {
    FollowerSync fs;
    fs.onResponse(false, 100);
    const Action a = step(fs, kBase);                 // first keepalive
    EXPECT_TRUE(a.emit && a.wantData);
    fs.onResponse(false, 100);                        // still empty → stays InSync
    EXPECT_EQ(fs.state(), FollowerSync::State::InSync);
    EXPECT_FALSE(step(fs, kBase + kKeep - 1).emit);   // not before the keepalive window
    EXPECT_TRUE(step(fs, kBase + kKeep).emit);        // polls again on cadence (never silent)
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item A: forceBehind() — used when the leader replies with
// kErrRecordTooLarge (a batch it can NEVER deliver over this transport).
// This is emphatically NOT "caught up": even a follower that was InSync (e.g.
// caught up to an OLDER, smaller LEO before this oversized batch appeared)
// must be forced to Behind, with the outstanding-fetch deadline cleared so
// the next duty tick retries at the FAST cadence immediately, not after a
// stale resend timer.
// ---------------------------------------------------------------------------
TEST(FollowerSyncRegression, ForceBehindFromInSyncClearsOutstandingAndStaysBehind) {
    FollowerSync fs;
    // Get to InSync first (the scenario the review was most worried about: a
    // stale InSync must not survive an error response).
    fs.onResponse(/*hadRecords=*/false, /*leaderHw=*/100);
    ASSERT_EQ(fs.state(), FollowerSync::State::InSync);

    fs.forceBehind();
    EXPECT_EQ(fs.state(), FollowerSync::State::Behind);
    EXPECT_FALSE(fs.fetchArmed()) << "no fetch should appear outstanding after forceBehind()";

    // The very next tick must emit a fast data fetch (not wait out a keepalive
    // or resend window) — forceBehind() must not leave any cadence timer armed
    // that would delay this.
    const Action a = step(fs, kBase);
    EXPECT_TRUE(a.emit);
    EXPECT_TRUE(a.wantData);
}

// Companion: forceBehind() from an already-Behind state (the more common case
// — the follower was mid-catch-up when the oversized batch was hit) must
// still clear the outstanding marker so the single-outstanding gate doesn't
// suppress the next retry waiting for a response that will never arrive
// (the error WAS the response, but onResponse() was never called for it).
TEST(FollowerSyncRegression, ForceBehindFromBehindClearsOutstandingFetch) {
    FollowerSync fs;   // bootstraps in Behind
    const Action first = step(fs, kBase);
    ASSERT_TRUE(first.emit && first.wantData);
    ASSERT_TRUE(fs.fetchArmed()) << "test setup: a fetch must be outstanding";

    fs.forceBehind();
    EXPECT_EQ(fs.state(), FollowerSync::State::Behind);
    EXPECT_FALSE(fs.fetchArmed());

    // Single-outstanding gate (fetchArmed) must not block the next attempt
    // now; only the ordinary fast-cadence throttle still applies (same as any
    // other back-to-back decide() call).
    const Action retry = step(fs, kBase + kFast);
    EXPECT_TRUE(retry.emit);
    EXPECT_TRUE(retry.wantData);
}

// Fix round 5/6: TruncatePending holds the target, emits nothing, ignores
// responses, and (N5-LOW-1) drops an owed fast-ack — the diverged LEO is never
// reported; reset() (role change / completion) leaves it.
TEST(FollowerSyncRegression, TruncatePendingEmitsNothingAndDropsOwedAckRegression) {
    FollowerSync fs;
    fs.onAckRefused();
    ASSERT_TRUE(fs.ackOwed());
    fs.enterTruncatePending(42);
    EXPECT_TRUE(fs.truncatePending());
    EXPECT_EQ(fs.truncateTarget(), 42);
    EXPECT_FALSE(fs.ackOwed()) << "an owed fast-ack would report the diverged LEO";
    const Action a = fs.decide(1'000'000'000, 1, 1, 1);
    EXPECT_FALSE(a.emit);
    fs.onResponse(/*hadRecords=*/true, 7);
    fs.forceBehind();
    EXPECT_TRUE(fs.truncatePending());
    fs.reset();
    EXPECT_FALSE(fs.truncatePending());
    EXPECT_EQ(fs.state(), State::Behind);
}
