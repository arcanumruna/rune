# Batch M29 — security-state ordering (#608, #444, #518)

Repo arcanumruna/rune, working tree ., branch `bugfix/medium-batch-29` (the coordinator creates it off
origin/main before you start; stay on it). Read each issue body in full first (mcp__github__issue_read). The repo
CLAUDE.md and broker/CLAUDE.md bind you. Line numbers drift: locate by name.

Global rules:
- Build only named targets under the shared lock:
  `flock build-client/.build.lock ninja -C build <target>`. Never a whole-tree build,
  never a new build dir (disk is nearly full). Find the owning test executable of each test file in the CMakeLists of
  its lib (broker/libs/<lib>/CMakeLists.txt).
- Test FIRST: each new test is written and run RED against the unfixed code, then the fix, then GREEN. Report the red
  output line for each.
- New tests end in `Regression`. Deterministic: injected clock, no sleeps, every drive loop capped and failing rather
  than hanging. Every new assertion gets a mutation check (the smallest product change it must catch): red, revert,
  green. Report each.
- Hot-path rules: zero allocation in steady state, no virtual calls, no locks/atomics/thread_local; the only
  cross-thread hand-off is the existing rings.
- Never weaken an existing assertion. No secrets / IPs / absolute paths in the repo. Do not commit or push. Do not
  spawn subagents. If a design step below turns out impossible as written, STOP that item and report exactly why
  (do not invent another design).

## Task 1 — #608: a behind→caught-up clear never ships before the events fenced while behind are enforced

Where: `broker/libs/gateway/src/ClientGateway.cpp` `syncWorkersBehind_()` (+ fields in ClientGateway.h).
Today: `if (!securityBehind_ && !pendingEnforce_.empty()) return 0;` — the clear waits for an EMPTY pendingEnforce_,
so an event fenced AFTER the clear (B) delays it, and — the bug — ordering is by emptiness, not by the events that
were pending when the view caught up.
Algorithm: on the edge securityBehind_ true→false (seen in setSecurityBehind or the first syncWorkersBehind_ that sees
it), record `clearTarget_ = pendingEnforce_.empty() ? -1 : pendingEnforce_.back().offset` (the newest event fenced
before the clear). Ship the clear (workersBehind_ = false; permissionShipper_.securityBehind(...)) once
`clearTarget_ < 0 || pendingEnforce_.empty() || pendingEnforce_.front().offset > clearTarget_`. A new true edge resets
clearTarget_. Invariant: the workers never learn "caught up" before every event fenced up to the clear is enforced,
and events fenced after the clear never hold it.
Test (broker/libs/gateway/tests/gateway/SecurityEnforcementFenceTest.cpp, next to the existing fence tests, same
fixture): `SecurityEnforcementFenceRegression.ClearWaitsOnlyForEventsFencedBeforeItRegression`: behind; event A fenced;
setSecurityBehind(false); event B fenced; ack A only → `workersSecurityBehindForTest()` is false while B is still
pending. Plus the converse in the same test: before A's ack, workers are still behind. Mutation: restore the old
`!pendingEnforce_.empty()` check → red; ship on the edge without waiting for A → red.

## Task 2 — #444: a held `__security` frame waits for the mirror instead of cycling pause/resume

Where: `broker/libs/node/include/rune/node/CatalogReadFlow.h`, `ClientGateway::admitAdminLogFrame` (ClientGateway.cpp)
and the control-thread sink `CatalogReadSink` in `broker/libs/node/src/BrokerNode.cpp` (~5114), plus the fake sink
in the CatalogReadFlow / CatalogReadServer tests.
Design (rich states, hard rule 3):
- `admit` returns `struct AdmitVerdict { Admit verdict; int64_t heldUntil; }` (heldUntil only meaningful for Hold:
  the frame's newestEvent, or -1 when held only because `securityBehind_`). Change the gateway's
  `admitAdminLogFrame` to return it (keep its old enum available only if other callers need it — grep).
- Entry states: `PauseOwed{baseOffset, droppedMarker, hold}`, `AwaitingAck{hold}`, NEW `AwaitingRelease{heldUntil}`,
  `AwaitingRoom`, `ResumeOwed`, where `hold` is `std::optional<int64_t>` or an explicit `int64_t` with a named
  `kNotHeld` sentinel (pick one, document it). A frame paused for back-pressure (Deliver + BackPressured) is not held.
- Pause ack (len==0 frame): held entry → `AwaitingRelease{heldUntil}`; not held → `AwaitingRoom` as today.
- service(): `AwaitingRelease`: `sink.room(session)==Gone` → erase; else `sink.released(heldUntil)` → AwaitingRoom
  (and fall through to the room check in the same pass); else stay (cost: one call, no message).
- Gateway `bool adminLogHoldReleased(int64_t heldUntil) const`: true when auth mode is not Required, or
  `!securityBehind_ && (heldUntil < 0 || securityView_ == nullptr || securityView_->halted() ||
  heldUntil <= securityView_->appliedOffset())`. (A halted/absent view releases so the re-read is answered Withhold by
  admit: fail closed, never stuck.) The sink's `released` calls it.
Tests:
- Strengthen `CatalogReadServerRegression.AMirrorThatNeverAdvancesDeliversNothingAboveItAndStaysBoundedAdversarialRegression`
  (broker/libs/node/tests/node/CatalogReadServerTest.cpp): over 200 passes with the mirror held, `pauses() == 1` and
  `resumes() == 0` (today 100); then advance the mirror past the held event: exactly one resume, the frame delivered
  once, nothing above the mirror ever delivered. Do not drop any of its existing assertions.
- `CatalogReadFlowRegression.HeldForBehindReleasesOnTheClearNotOnRoomRegression` (CatalogReadFlow's own test file —
  grep for the existing CatalogReadFlow tests): heldUntil=-1, room Ready but `released` false → no resume over 50
  passes; `released` true → one resume.
- `CatalogReadFlowRegression.HeldEntryOfAGoneSessionIsErasedRegression`: AwaitingRelease + room Gone → erased,
  `paused()==0`.
- A back-pressure pause (not held) still resumes on room alone (existing tests must stay green; add one assertion
  if none covers it).
Mutations: release on room alone (skip AwaitingRelease) → the strengthened test red; never release → the advance leg
red; drop the Gone check in AwaitingRelease → the gone test red.

## Task 3 — #518: no acknowledgement of a `__security` event leaves before the control thread can know of it, and a
## failed proof is judged only after the control thread consumed everything posted before it arrived

Two parts; both needed (part A orders admin-side post before the ack; part B closes the window between the ring
write and the control thread's drain — the control thread can poll a client proof in the same cycle after it drained
the ring).

Part A — admin side (`broker/libs/replication/src/PartitionReplica.cpp` + header, `broker/libs/node/src/AdminWorker.cpp`
+ header, BrokerNode deps wiring):
- PartitionReplica gets an optional ack fence: `using AckFenceFn = bool (*)(void* ctx, Offset fetchOffset);`
  `void setAckFence(AckFenceFn fn, void* ctx) noexcept;` (function pointer + context: no std::function, no virtual).
  In `sendFetchToLeader_`, before sending, `if (ackFence_ && !ackFence_(ackFenceCtx_, req.fetchOffset))` → count
  `ackFenceHolds_` and return false (treated exactly like a refused send: the existing owed-ack / retry paths re-send
  on the next doWork). Verify by reading every caller of sendFetchToLeader_ that a `false` is retried on a later
  doWork (fast-ack: onAckRefused; periodic and the ~2667/~2699 paths) — if one is not, report it; do not invent a
  retry. Add `ackFenceHoldsForTest()`.
- AdminWorker sets the fence on the `__security` replica only (where it creates/attaches securityReplica_): a static
  trampoline calling `bool publishSecurityKnownEndThrough_(int64_t fetchOffset)`:
  ```
  const int64_t knownEnd = max(securityReplica_->logEndOffset(), securityReplica_->leaderCommittedHw());
  if (knownEnd > securityServingPostedKnownEnd_) {
      // the LAST POSTED verdict fields, only knownEnd moves - never step SecurityCatchUp/securityBehind_ here
      deps_.securityServing(securityServingPosted_ == 1, <last posted hw>, securityServingPostedEpoch_,
                            securityServingPostedLeader_, knownEnd, <last posted behind>, securityServingPostedHeld_);
      securityServingPostedKnownEnd_ = knownEnd;
  }
  return securityServingPostedKnownEnd_ >= fetchOffset && deps_.adminEffectsDelivered();
  ```
  Store the last posted hw and behind in stepSecurityServing_ (new fields) so they are re-posted unchanged. If
  `deps_.securityServing` is unset → return true (no gateway to order against).
- New dep `deps.adminEffectsDelivered` (BrokerNode wiring, admin thread): `catalogBacklog_.empty()` — the post went
  onto failureRing_ and not into the admin-side backlog (FIFO: an empty backlog means every earlier post is on the
  ring). A fetch held by it is re-tried next doWork; the backlog flushes as the control thread drains, so it always
  converges (no timer).
- Fix the now-wrong comment in stepSecurityServing_ (the "#518" sentence) to describe the fence.

Part B — control side (`ClientGateway`):
- `void setAdminEffectsRing(const rune::node::OneToOneByteRing* ring) noexcept` (BrokerNode wires `&failureRing_` at
  gateway construction). Null (gateway unit tests without a node) = today's behaviour.
- In `handleAuthClientFinal_`, at the point that would call `failLogin_` for BadProof/UnknownUser/Disabled after the
  #205 check passed (i.e. the terminal 44 path, NOT the rate-limit or malformed paths): if the ring is set and
  `ring->consumerPosition() < ring->producerPosition()`, do not answer yet: move the session to a new auth state
  `AuthFinalDeferred{user, correlationId, failure, ringTarget = producerPosition(), viewAppliedAtFirst,
  viewBehindAtFirst}` and push its sessionId on `deferredFinals_` (a vector reserved at construction, capacity 256;
  when full, answer 48 via unavailableLogin_ instead — retriable, never 44). The throttle charge and the failure
  counters stay where they are today (charged once, at the verdict).
- In doWork (one `empty()` branch when idle): for each deferred entry whose session still exists and is still
  AuthFinalDeferred: once `ring->consumerPosition() >= ringTarget`, re-run the #205 decision with the stored
  viewAppliedAtFirst / viewBehindAtFirst: `securityViewBehind_() || viewBehindAtFirst || viewAppliedAtFirst !=
  securityView_->appliedOffset()` or the view is null/halted → unavailableLogin_ (48); else failLogin_ (44). Erase
  the entry. A session that closed meanwhile is just erased. Every other auth handler must treat AuthFinalDeferred
  as "a login is in progress" (grep every std::get_if<AuthAwaitingFinal> / visit of session->auth: a second
  AuthClientFinal on a deferred session answers InvalidRequest like any non-AwaitingFinal state; check the
  session-close accounting of abandoned first legs treats it as finalLegSeen).
- BrokerNode: confirm the control thread consumes failureRing_ through the message handler that calls
  setSecurityKnownEnd synchronously (onSecurityServing_). Add a test seam `holdAdminEffectsDrainForTest(bool)` that
  makes the control thread skip draining failureRing_ while on (test-only; the `heldMirror_` seam is different: it
  consumes frames without applying them, so do not use it for this test).
Tests (write all RED first where the old code allows):
- Replication unit (the PartitionReplica tests dir, beside the fast-ack tests — grep `fastAckOnApply` in
  broker/libs/replication/tests): `PartitionReplicaRegression.AckFenceHoldsEveryFetchAboveItRegression`: follower
  with fastAckOnApply; fence returns false → after a push is applied no FetchRequest with fetchOffset > fence value
  reaches the leader transport over N doWork passes; flip the fence to true → the owed ack is sent on the next
  doWork with the new LEO. Mutation: ignore the fence → red.
- AdminWorker level (find the existing AdminWorker security tests, e.g. grep `securityServing` in
  broker/libs/node/tests): `AdminWorkerSecurityRegression.KnownEndIsPostedBeforeTheSecurityAckLeavesRegression`:
  record the order of (deps.securityServing knownEnd) and (fetches the security follower sends): every fetch with
  fetchOffset X is preceded by a post with knownEnd >= X; with `adminEffectsDelivered` false the fetch is held, then
  sent once it turns true. Mutation: post after the fence returns (or drop the post) → red.
- Gateway unit (SecurityGatewayTest.cpp, same fixture as the #205 tests): `SecurityGatewayRegression.AFailedProofWaitsForTheEffectsRingRegression`:
  a OneToOneByteRing with one unconsumed message → a failed final is not answered; consume it after calling
  setSecurityKnownEnd(beyond applied) → the answer is 48; a second case consumes it without moving knownEnd → 44.
  Also: deferred list full → 48 at once. Mutation: answer at once (skip the deferral) → red.
- Integration (broker/libs/node/tests/integration/AuthClusterIntegrationTest.cpp, beside
  `AuthClusterRegression.AJustCreatedUserCanLogInThroughALaggingFollowerRegression`):
  `AuthClusterRegression.AFollowerWhoseControlThreadLagsTheAdminAnswersRetriableNeverRejectedRegression`: 3-node
  AuthTrio; on follower F `holdAdminEffectsDrainForTest(true)`; CREATE USER on the leader → OK; log in on F → no
  final answer while held (capped drive); release → the answer is 48 (AuthUnavailable), never 44; a retry login on F
  then succeeds. RED before Part B (answers 44 at once). Mutation: Part B off → red.
- Run the existing #205 / #603 / #117 tests (grep `#205`, `SecurityBehind`, `AdminLogFrame` test names) and the
  replication fast-ack tests: all green.

## Interaction matrix (cover in the tests above, or say which cell is not reachable and why)
- #518: proof fails while {ring empty, ring has an unconsumed knownEnd, ring has an unconsumed applied event + knownEnd,
  backlog non-empty} × {first leg before / after the event} × {session closes while deferred, second final while
  deferred, view halts while deferred}.
- #444: held {for behind, for newestEvent} × {session gone while held, mirror advances, view halts, auth mode off}.
- #608: clear with {nothing pending, A pending, A pending + B fenced after the clear, re-behind before A acks}.

## Review focus
Part A must not delay data-topic replication (fence only on `__security`); Part B allocates nothing per failed proof
in steady state; no new timer or timeout anywhere; every new state handles every event.

## Report
Files changed; each test with its red line before the fix and green after; each mutation; the affected-area runs
(test executables and counts); anything stopped or not reachable, with the reason.
