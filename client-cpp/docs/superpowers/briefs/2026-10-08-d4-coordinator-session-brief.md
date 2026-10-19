# D4 client — coordinator session (replace the seed-move approach)

## Where this fits
The Rune broker (sibling repo, already changed) answers a group / txn COORDINATOR request that lands on a
follower with `ErrorResponse{code=NotCoordinator(14), message=<coordinator client endpoint, e.g.
"aeron:udp?endpoint=rune-2:40200">, correlationId=<request corr (0 for group frames)>}`. The coordinator is the
consensus leader. Before D4, a group member bootstrapped against a follower re-joined the follower forever
(dockerized repro: 0 of 80 records consumed).

The first client fix (this branch, commits a0f61a1 + 25c2ae5) MOVES the seed session onto the coordinator
(retarget + reconnect + seed endpoint sync). It works end-to-end, but review
`/Users/vradu/Projects/rune-broker-wt26/docs/superpowers/briefs/2026-10-08-d4-client-review.md` found 3 MAJORs,
all caused by moving the seed:
- duplicate sessions per node, with the wrong crypto state;
- stale route session index 0;
- seed connect-corrId collision.

Read that review first.

## Required design (ruling — do not re-litigate)
1. **Revert the seed-move pieces.**
   - Drop `ClientAgent::syncSeedEndpoint_` and the identity / offerControlTo / releaseRoutePub_ changes (25c2ae5).
   - Drop the retarget + submitReconnect in `Client::onCoordinatorRedirect_` (a0f61a1).
   - The seed session NEVER moves for a coordinator answer.
   - KEEP `GroupConsumerHub::onConnected` re-joining Joining / Syncing members (first-review MAJOR-1). Rule on the
     ghost-member MEDIUM yourself and record the ruling.
2. **Coordinator route (Kafka's coordinator connection).** The Client keeps `coordinatorChannel_`. Empty = the seed
   (today's behaviour, byte-identical when no NotCoordinator+endpoint ever arrives).
   - On `ErrorResponse{NotCoordinator, ep}`: normalise `ep` to an aeron channel.
   - If it equals the current coordinator channel → no-op (review MEDIUM: a late answer never tears anything down).
   - Else set `coordinatorChannel_ = ep` and `groupConsumerHub_.onCoordinatorMoved()` (members → Rejoining).
   - Txn requests in flight must be re-sent to the new coordinator.
3. **Coordinator frames route to `sessionForEndpoint(coordinatorChannel_)`.** That is the same lazily opened,
   one-session-per-node session the partition routes use (crypto state per session, disjoint corrId range).
   - Coordinator frames are every group frame from GroupConsumerHub (JoinGroup / SyncGroup / Heartbeat / LeaveGroup /
     OffsetCommit / OffsetFetch) and every non-Produce txn frame from `txnSendFn_`.
   - Re-stamp the header sessionId with that session's id, exactly like `offerProduceToLeader_` does.
   - Not Connected yet → hold, and let the owner re-send: the group hub re-joins on that session's ConnectResponse;
     the txn producer re-sends when the session changes.
   - Add a single agent API, for example `offerToCoordinator(channel, buf, len)`.
   - Check the duty-thread send paths: group frames currently go `agent_.submitSend` (ring) or
     `directSendTrampoline_`; txn frames go `offerDefault`.
4. **The coordinator session's ConnectResponse{OK}.**
   - When the session that connected is the coordinator session (endpoint == `coordinatorChannel_`, non-seed), call
     `groupConsumerHub_.onConnected(thatSessionId, ...)` so members join there at once.
   - Do NOT call it for unrelated leader sessions. The existing code calls it only for the seed's connect; keep that
     for the seed when `coordinatorChannel_` is empty.
   - `GroupConsumerHub` stamps frames with the sessionId it was given. Make sure heartbeats / commits after the join
     carry the coordinator session's id: either stamp in the hub, or re-stamp in `offerToCoordinator`.
5. **Fallback.** If the coordinator session fails to connect (the session machine gives up → Disconnected / closed),
   reset `coordinatorChannel_` to empty (the seed). The seed node answers again with the current coordinator. No busy
   loop: bounded by the session machine's own backoff.
6. **Plain (non-group) subscriptions and produce are untouched.**

## Tests (RED-first, `*Regression*` names, in-process, condition-driven, no sleeps)
Use `tests/client/GroupRoutingRegressionTest.cpp` GroupRig + `tests/support/InvokerGateway.h` (`coordinatorRedirectTo`
already exists on this branch).
- Keep `MemberOnAFollowerJoinsTheCoordinatorRegression`: joined < 5 s, plus a partition the old seed node leads still
  consumed.
  - Add assertions: the seed session's id is unchanged (no reconnect); group frames after the join arrive on the
    coordinator endpoint; no session is duplicated per endpoint.
- A txn producer bootstrapped on a follower: InitProducerId → NotCoordinator+ep → InitProducerId re-sent on the
  coordinator session → OK. The InvokerGateway may need txn coordinator support; add the minimum.
- Coordinator failover while Stable: the old coordinator answers Heartbeat NotCoordinator+newEp → the member re-joins
  on the new coordinator.
- A late NotCoordinator naming the current coordinator is a no-op: no rebalance.
- The two-endpoint crypto case only if the existing crypto test rigs make it cheap; otherwise state why not.

## Constraints
- Duty thread only. No atomics / mutex outside the allowlist (ctest `NoAtomicsOrMutexesOutsideAllowlistRegression`).
- No allocation per frame on the produce hot path (coordinator frames are control-plane).
- Never busy-spin.
- Build: `/Users/vradu/Projects/rune-client-cpp-wt17/build` (already configured, Ninja): `cmake --build build -j6`.
  Targeted tests while iterating (`./build/rune_invoker_tests --gtest_filter=...`). Run the full `ctest -j6` once at
  the end.
- Commit on branch `fix/d4-coordinator-redirect` in wt17 (small logical commits; end each commit message with
  `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`). Do NOT merge, push or touch other
  worktrees. Do NOT dispatch subagents.

## Report
Write `/Users/vradu/Projects/rune-client-cpp-wt17/docs/superpowers/briefs/2026-10-08-d4-coordinator-session-report.md`:
- the design as built;
- each test, and its RED evidence (the test failing with the fix reverted);
- the review findings addressed, or ruled with a reason;
- the full ctest result.

Return only: status, commit SHAs, a one-line test summary and concerns.
