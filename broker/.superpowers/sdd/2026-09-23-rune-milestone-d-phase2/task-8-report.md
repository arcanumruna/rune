## Task 8 — Graceful Controlled Shutdown: Report

**Status:** DONE (hardened)

**Commit:** `4e6e9cf` (original); `7795970` (test-hardening round, 2026-09-23) — rune-broker local main

**Test summary:** 6/6 ControlledShutdown tests PASS (build-release + build-tsan, 0 races). NOTE: the "3/3" figure in the original report was erroneous — `rune_controlled_shutdown_tests` only contained 2 tests at commit `4e6e9cf` (MovesLeadershipOffLiveNodeNeverUnclean + CompleteWhenAllLiveConsumedTheMoves). An Opus reviewer flagged thin coverage; this round adds 4 more tests (6 total). All pass clean.

**What was done:**

- Added `beginControlledShutdown(nodeId, offset)` and `shutdownComplete(nodeId, lowestActiveConsumedOffset)` to `MembershipState.h` (inline, no virtual calls). `beginControlledShutdown` sets `state = ControlledShutdown` and records the drain offset in a new `controlledShutdownOffsets_` map. `shutdownComplete` compares stored offset <= passed lowest consumed offset.
- No DynamicBalancer changes were needed: the test passes `in.liveNodes={2}` (node 1 absent), so the existing failover path already handles ControlledShutdown nodes correctly — the caller excludes the shutting-down node from `liveNodes` when invoking `rebalance`, which is the right separation (DynamicBalancer is a pure computation over the caller-supplied set per its carry-forward contract).
- Registered `rune_controlled_shutdown_tests` in `libs/metadata/CMakeLists.txt` linked against `rune_dynamic_balancer`.

**Edge cases verified:**
- Leadership drains off the shutting-down node to in-ISR survivor (node 2): PASS
- Shutdown complete gate: false at offset 400, true at 500, true at 600: PASS
- Never unclean: `allowUnclean=false` path taken (FailoverElection default); partition goes leaderless rather than unclean if no ISR survivor: covered by existing FailoverElection tests (test #106)
- No virtual calls introduced (all new code is inline header-only, no vtable)
- min-ISR unchanged as produce-time gate only (no modifications to IsrManager)

**Hardening round (commit `7795970`):**
- Added `ShutdownWithSoleIsrMemberGoesLeaderlessNotUnclean`: sole-ISR-member drain scenario, `allowUnclean=false`, asserts `wentNoLeader==true`, `change.newLeader==NO_LEADER`, `recovering==false`. Locks the never-unclean-on-shutdown invariant.
- Added `BeginControlledShutdownSetsStateAndExcludesFromEligible`: verifies state=ControlledShutdown after `beginControlledShutdown`, node excluded from `eligibleNodes()` but retained in `liveNodes()`.
- Added `ShutdownCompleteFalseWhenNoPendingRecord`: guard branch — `shutdownComplete` returns false for a node with no pending record.
- Added `BeginControlledShutdownUnknownNodeIsNoOp`: unknown nodeId is a no-op, no crash, no spurious entry.
- Fixed stale `///< reserved; currently unused` doc on `ElectionInputs::allowUnclean` in `FailoverElection.h` — replaced with accurate description of per-topic unclean opt-in (Task 6) and the shutdown invariant constraint.
- Added `controlledShutdownOffsets_.erase(nodeId)` to `MembershipState::onLeave()` so stale drain offsets cannot linger after a node fully departs.

**Concerns:** None.
