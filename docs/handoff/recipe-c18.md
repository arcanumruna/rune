# Batch C18 — group consumer + metadata routing (issues #651, #676, #660, #685, #684)

Repo the repo root, branch claude/kind-bardeen-ze6yl0 (checked out — do NOT switch, commit, push or stash; the
coordinator commits). Read CLAUDE.md and client-cpp/CLAUDE.md first. Never spawn subagents.

Rules:
- Build named targets only, under the lock: `flock build-client/.build.lock ninja -C build-client <targets>`.
  Find the target of each test file in client-cpp/tests/CMakeLists.txt. Never a whole-tree build, never a new build
  dir, never reconfigure. If a build fails in a file you did not touch, wait and retry (other agents edit the tree).
- Touch only: client-cpp/src/Client.cpp + client-cpp/include/rune/client/Client.h (ONLY the metadata-refresh members
  and functions named below), client-cpp/src/group/GroupConsumerHub.cpp (+ its header), and the tests
  client-cpp/tests/client/GroupRoutingRegressionTest.cpp, client-cpp/tests/client/GroupSeekTest.cpp,
  client-cpp/tests/group/GroupConsumerTest.cpp (+ client-cpp/tests/support/GroupRig.h only if a helper is needed).
- Failing test FIRST (paste the RED line), then fix, then GREEN. New test names end in `Regression`.
- Every new assertion gets a mutation check (revert the fix line(s) → RED, restore). Leave no marker behind.
- Injected clock only, no sleeps, capped drive loops (GroupRig::drive has a cap).
  `python3 client-cpp/scripts/check_no_atomics_or_mutexes.py --tests client-cpp` must print OK.
- Short comments citing the issue (`// #651: ...`). No secrets, local paths or model names.

---------------------------------------------------------------------------------------------------------------------
## Part A — metadata refresh on the duty clock + #651 (lagging answers postpone the armed refresh)

Facts (verified):
- `Client::nextMetadataRefreshAt_` (Client.h ~2212) is a `std::chrono::steady_clock::time_point`; it is armed in
  `scheduleMetadataRefresh()` (Client.cpp ~6519) with `steady_clock::now()` and checked in `tickMetadataRefresh()`
  (~6577) with `steady_clock::now()`. Everything else on the duty thread uses `now()` (Client.h ~1708: the injected
  `cfg_.nanoClock`, else steady). So an injected clock does not drive the refresh — a rule-2 (one injected clock)
  violation, and the reason #676's tests can only use wall windows.
- `onMetadataResponse` (~6352) disarms (`= max()`) and then, if `metadataCache_.anyProvisional()`, calls
  `scheduleMetadataRefresh()` again — that bypasses the idempotent-while-armed guard and pushes the deadline out and
  doubles the backoff on every lagging answer (#651).

Changes:
1. Replace the member with `int64_t nextMetadataRefreshAtNs_ = kMetadataRefreshNotArmed;` where
   `static constexpr int64_t kMetadataRefreshNotArmed = std::numeric_limits<int64_t>::max();`. Arm with
   `now() + metadataBackoffNs_`; compare with `now() < nextMetadataRefreshAtNs_`; disarm by assigning the sentinel.
   Update every use (grep `nextMetadataRefreshAt_` — Client.cpp lines ~6352, 6525, 6528, 6574, 6577, 6581 and any
   other hit, including tests/test hooks if any reference it).
2. #651 in `onMetadataResponse`:
   ```
   if (metadataCache_.anyProvisional()) {
       scheduleMetadataRefresh();   // #651: idempotent while armed - a lagging answer never postpones the armed refresh
   } else {
       nextMetadataRefreshAtNs_ = kMetadataRefreshNotArmed;   // a fresh authoritative map: disarm
       metadataBackoffNs_       = kMetadataBackoffInitNs;
   }
   ```
   (The disarm moves INTO the else branch; keep the existing comments, adjusted.)

Tests:
- Enable `GroupRoutingRegression.DISABLED_LaggingMetadataAnswersNeverPostponeTheArmedRefreshAdversarialRegression`
  (GroupRoutingRegressionTest.cpp ~4063): remove `DISABLED_`. RED before change 2, GREEN after. Mutation: restore the
  unconditional disarm before the `if` → RED.
- New `GroupRoutingRegression.MetadataRefreshFollowsTheInjectedClockRegression`: RigOptions with `nanoClock` +
  `manualNs` + `stepNs = 0` (frozen clock; see GroupSeekTest.cpp for how C16 sets `nanoClock`/`manualNs`). Arm a
  refresh (the simplest existing trigger in this file: a subscribe whose leader is unknown / a redirect — reuse the
  setup of an existing refresh test). Drive N passes with the clock frozen: assert NO new MetadataRequest is sent
  (count with the gateway's metadata-request counter the file already uses). Then advance `*manualNs` past the backoff
  (e.g. +2 s) and drive: assert exactly one new MetadataRequest. RED today (the wall clock fires it while frozen).
  Mutation: change the tick back to `steady_clock` → RED.

---------------------------------------------------------------------------------------------------------------------
## Part B — #676: five GroupRoutingRegression tests measure wall-clock windows

After Part A every timer these tests depend on runs on the duty clock. Convert each test below to the injected clock:
RigOptions `nanoClock` (a static function returning a file-static `int64_t`), `manualNs` pointing at it, and a
`stepNs` (start with 1'000'000 = 1 ms per pass). Replace each `steady_clock` window with a duty-clock window
(`*manualNs` reaches start + window) and keep every existing count assertion unchanged in meaning.
- `OldLeaderNodeNeverPollsMetadataAndResumesAfterUpgradeRegression` (~1321-1326: a 1.5 s window).
- `MetadataIsAskedThroughALiveNodeSessionWhileTheSeedIsDownRegression` (~2778-2786: a 5 s deadline).
- `RedirectFromAnEarlierSessionNeverBindsASubscriptionTwiceRegression` (~2644: `s.finished()`).
- `ALeaderSilentThenBackStopsRefreshingAndKeepsItsReaderAdversarialRegression` (~3878: refreshes <= 1).
- `AnIdleButLiveLeaderDoesNotTriggerRefreshStormRegression` (~3754: metadataRequests == 4).
If a test needs a real-time component (the fake gateway's `delayedConnects` use steady_clock — InvokerGateway.h ~790),
do not change the gateway; choose `stepNs` so the duty clock still moves forward per pass and the predicate is
progress-driven. Never weaken a count; if one cannot be kept exactly, STOP and report why.
Verification (no mutation applies to a flake fix): in build-client run TWO of these at the same time
and paste both summaries:
`ctest -R GroupRoutingRegression -j8 --repeat until-fail:20` — every test 20/20.

---------------------------------------------------------------------------------------------------------------------
## Part C — #660: 24-bit ctlId can collide after 2^24 add() calls

`GroupConsumerHub::add()` (GroupConsumerHub.cpp ~84-93) assigns `(++nextCtlId_ & 0xFFFFFF)`, skipping 0, with no
in-use check. Fix: loop until the candidate is non-zero AND not equal to the `ctlId` of any consumer in `owned_`
(cold path; owned_ holds every consumer the hub still answers for). Bound the loop by 2^24 iterations; at most
`owned_.size()` ids are taken so it always ends — add a `// #660` comment saying so.
Test in client-cpp/tests/group/GroupConsumerTest.cpp: `GroupConsumerHubRegression.CtlIdNeverReusesALiveConsumersIdRegression`.
To force the wrap, add a test-only setter on the hub (`void testSetNextCtlId(uint32_t v) noexcept { nextCtlId_ = v; }`,
following the hub's existing test-hook naming if one exists — grep `test` in GroupConsumerHub.h). Add consumer A,
read A's ctlId, set nextCtlId_ so the next candidate equals A's id, add B: assert B.ctlId != A.ctlId and != 0.
Also: set nextCtlId_ = 0xFFFFFF - 1 with a live consumer holding 0xFFFFFF... (exercise the wrap past 0). Mutation:
remove the in-use check → RED.

---------------------------------------------------------------------------------------------------------------------
## Part D — #685: a session drop leaves offsetFetchPhase Awaiting until sessionTimeoutMs

On a session transition (`setSessionConnected`, ~371, the branch that runs when the session goes down or changes) and
in `onSessionDown()` (~1676), for every active consumer, reset the offset-fetch machine so the next pass after the
reconnect re-sends: add a private helper
```
// #685: a fetch in flight died with its session: the answer can never come - re-send on the first pass after reconnect.
static void resetOffsetFetch_(GroupConsumerImpl& impl) noexcept {
    impl.offsetFetchPhase     = GroupConsumerImpl::OffsetFetchPhase::Idle;
    impl.offsetFetchDueNs     = 0;
    impl.offsetFetchBackoffNs = 0;
}
```
and call it in both places (only when the phase is not already Idle is fine, but unconditional is also correct).
Do NOT clear `offsetFetchCorrId`: an answer still arriving on the old session is already fenced by session id; check
that with a grep of how the OffsetFetch answer is matched and report what you find.
Test in GroupSeekTest.cpp: `GroupSeekRegression.SeekToCommittedRefetchesRightAfterASessionDropRegression` per the
issue: injected clock, Stable consumer, `seekToCommitted()`, hold the OffsetFetch answer (the rig's hold knob, as the
existing seek tests do), drop the seed session and reconnect it, then assert a NEW OffsetFetch is sent before
`sessionTimeoutMs` of duty clock elapses (e.g. within 500 ms of duty time after the reconnect). Mutation: remove the
reset calls → RED (re-send only after sessionTimeoutMs).

---------------------------------------------------------------------------------------------------------------------
## Part E — #684: a seek superseding seekToCommitted leaves the other partitions to the 100 ms sweep

`driveRewinds` (~355) calls `retryOffsetFetchFor_` only while the rewind is `RewindAwaitFetch`. A seek that superseded
a seekToCommitted leaves `RewindResubscribe` while the other partitions are still offset-unknown, so their fetch waits
for the Client's 100 ms `retryOffsetFetch` sweep. Fix: after `advanceRewind_`, call `retryOffsetFetchFor_` when the
rewind is `RewindAwaitFetch` OR `RewindResubscribe` (it is a no-op when no partition is offset-unknown and is paced by
its own phase). Update the `#371` comment to cite #684.
Test in GroupSeekTest.cpp: `GroupSeekRegression.ASeekSupersedingSeekToCommittedFetchesTheOtherPartitionsAtOnceRegression`:
injected clock with `stepNs = 0` (frozen, so the 100 ms sweep cannot fire), P0 and P1 Stable, `seekToCommitted()`,
hold its OffsetFetch, `seek(P0, n)`, drive a few passes (frozen clock): assert an OffsetFetch naming P1 was sent.
Mutation: restore the AwaitFetch-only condition → RED.

---------------------------------------------------------------------------------------------------------------------
## Run before reporting
Build and run the executables holding GroupRoutingRegressionTest, GroupSeekTest and GroupConsumerTest (all their
tests, not only the new ones), plus `ctest -R "Group|Metadata" -j6` in build-client. Then the Part B load run.

## Report
Files changed; each test with RED and GREEN lines; each mutation result; any existing assertion you changed and why;
deviations with reasons.
