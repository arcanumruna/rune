# D4 client: coordinator session report

- **Branch:** `fix/d4-coordinator-redirect` (wt17)
- **Commits:**
  - `ff71132`: the fix
  - `d22a757`: tests and InvokerGateway
- **Built on:** a0f61a1 + 25c2ae5. The seed-move pieces of those commits are reverted inside ff71132.

## Design as built

### 1. Revert
The following seed-move pieces are gone:
- `ClientAgent::syncSeedEndpoint_`
- the seed-by-identity checks in `retargetToBootstrap` / `retargetToLastKnownLeader`
- the `offerControlTo` / `releaseRoutePub_` changes
- the retarget + `submitReconnect` in `Client::onCoordinatorRedirect_`

`src/agent/ClientAgent.{h,cpp}` are back to 42ff385, plus the additions below. The seed session never moves for a coordinator answer.

### 2. Coordinator route
State: `Client::coordinatorChannel_`, plus `coordIdx_`, the agent session index (0 = the seed).

`onCoordinatorRedirect_(ep)`:
1. Normalises `ep` with `toChannel`.
2. If `ep` matches the current coordinator (`sameEndpoint` against `coordinatorChannel_`, or the seed endpoint when the channel is empty), it is a no-op.
3. Otherwise:
   - `coordIdx_ = agent_.coordinatorSessionIndex(channel, now)`. This is that node's single session, the one `sessionForEndpoint` returns. It is lazily opened, and a Disconnected one is revived. If the result is 0 (the seed's node), the channel is cleared.
   - Records `coordFailBase_` (the session's connect-failure count at this point).
   - Calls `groupConsumerHub_.onCoordinatorMoved()`, which moves members to Rejoining.
   - Calls `syncCoordinatorSession_()`.

While no NotCoordinator answer names another node, `coordIdx_ == 0` and every path is the pre-D4 path.

### 3. Agent API
- `coordinatorSessionIndex(channel, nowNs)`
- `offerToCoordinator(index, buf, len)`:
  - Returns `Retry` (hold) while the session is not Connected.
  - Otherwise re-stamps the header sessionId with that session's id and calls `offerOnSession` (sealed under that session's own CryptoState).
- `sessionMachineAt(index)`

Callers in Client:
- `coordSendFn_` (built once) carries JoinGroup / SyncGroup / Heartbeat / OffsetFetch. It goes to the seed ring (`submitSend`) when `coordIdx_ == 0`, otherwise to `offerToCoordinator`.
- `coordDirectSendTrampoline_` carries OffsetCommit / LeaveGroup. It is `directSendTrampoline_` for the seed, otherwise `offerToCoordinator`.
- `txnSendFn_`:
  - Non-Produce txn frames go to `offerToCoordinator` when `coordIdx_ != 0`.
  - Produce frames are re-stamped with the seed's id when a coordinator session is in use, because the txn producer now stamps the coordinator's id. A routed partition is re-stamped by its leader anyway.
- `producerDuty_` passes the coordinator session's id to `tp->doWork`. A coordinator move is therefore a session change, and in-flight txn requests are re-sent on the new session.

Every group call site now passes `coordSessionId_()` and `coordSendFn_` instead of `stateAndSessionId()` and the ring lambda:
- `sweepTimeouts` (doWork / retryOffsetFetch)
- step 3b (flushRequestedCommits / retryPendingLeaves)
- the Join / Sync / Heartbeat / Assign handlers
- `joinIfConnected`

### 4. Coordinator ConnectResponse
`syncCoordinatorSession_()` runs from `syncSeedSession_` (at every transition point and once per cycle).
- It feeds the group hub with the coordinator session's `(connected, sid, episode)`. When the coordinator is the seed, this is the seed's tuple, as before. Step 3c's `onSessionDown` uses `lastCoordState_`.
- When a non-seed coordinator session becomes Connected, it calls `setBrokerVersion`, then `onConnected(thatSid, coordSendFn_)` and `reconcileFetch`.
- This is level-triggered, so it also catches a TLS session that connects on a later HandshakeContinue leg.

Other ConnectResponses:
- The seed's ConnectResponse calls `onConnected` only when `coordIdx_ == 0`.
- Unrelated leader sessions never call it.

The hub stamps the coordinator sid it was given, and `offerToCoordinator` re-stamps it as well.

### 5. Fallback
The coordinator session has given up when it is:
- Disconnected, Rejected or Closed, or
- in Backoff with a new connect failure (`connectFailures() != coordFailBase_`).

In that case the route clears to the seed: `coordinatorFallbacks_` is incremented and `onCoordinatorMoved` is called. The seed's node then answers again with the coordinator. The pacing is that session's own connect budget / backoff, with one event per failure.

This check also runs just before `watchGroupSessions_`, so that the watch cannot revive the session first.

### 6. Unchanged
Plain subscriptions, produce and metadata are untouched (they stay on the seed).

## Tests and RED evidence
All tests are in `tests/client/GroupRoutingRegressionTest.cpp`. They run in process, are condition-driven, and use no sleeps.

RED evidence came from reverting each fix piece behind a temporary environment switch (removed before commit):
- **A:** `onCoordinatorRedirect_` returns at once (no coordinator route).
- **B:** no MEDIUM-1 same-endpoint no-op.
- **C:** NotCoordinator routed to the machine again.
- **D:** onConnected re-joins Joining / Syncing members unconditionally (a0f61a1).

Under each switch, only the expected tests failed. I also ran the new tests against the old seed-move HEAD.

| Test | GREEN | RED evidence |
|---|---|---|
| `MemberOnAFollowerJoinsTheCoordinatorRegression` (new asserts: seed state Connected, `connectsAt[0]==1`, `connectsAt[n1]==1`, `testSessionCount()==2`, seed channel unchanged, every group frame after the first on n1 arrives on n1 with one non-seed sid, the old seed node's partition on the seed sid) | 0.5 s | **A:** "the member never joined the coordinator". **Seed-move HEAD:** `connectsAt[0]==2` (seed reconnected), seed retargeted, partition 1 on sid 1002 instead of the seed's 1000. |
| `TxnProducerOnAFollowerInitsOnTheCoordinatorRegression`: InitProducerId gets NotCoordinator+ep, is re-sent on n1's session, and returns OK 4242 in under 5 s with the seed unchanged | 0.4 s | **A:** "InitProducerId never completed". The seed-move HEAD passes it, as expected. |
| `CoordinatorFailoverWhileStableRejoinsTheNewCoordinatorRegression`: Stable on the seed, Heartbeat gets NotCoordinator+n1, gen 2 joins on n1; then n1 gets NotCoordinator+n2, gen 3 joins on n2; each node connected once | 1.6 s | **A:** "never re-joined the new coordinator" (88 NotCoordinator answers). **Seed-move HEAD:** `connectsAt[0]==2`. |
| `LateNotCoordinatorNamingTheCurrentCoordinatorIsANoOpRegression`: once Stable on n1, a follower ErrorResponse naming n1 causes no JoinGroup, no connect, no new session, generation unchanged | 1.1 s | **B:** joinGroups 3 vs 2. **Seed-move HEAD:** joinGroups and connects 3 vs 2. |
| `ResumeKeepingTheSessionIdKeepsTheHeldJoinRegression` (MEDIUM-3 ghost member): a held join, then SessionExpired, the seed resumes with the same sid, and the JoinGroup is not re-sent | 0.3 s | **D:** joinGroups 2 vs 1. |
| `NotCoordinatorWithAConnectCorrIdNeverFailsTheConnectRegression` (MAJOR-3): NotCoordinator carrying the seed's in-flight connect corrId must not fail the connect | 5.0 s (waits out one connect timeout) | **C:** the seed is Disconnected, "the seed never connected". |
| `UnreachableCoordinatorFallsBackToTheSeedAndRetriesRegression`: n1 is dead, its session gives up and falls back to the seed (`notCoordinatorAnswers <= fallbacks + 2`); once n1 is alive the member joins there; the seed is never moved | 1.5 s | **A:** fails. |

**Two-endpoint crypto case:** not added.
- `InvokerGateway` has no TLS handshake, and the crypto rigs (`tests/crypto`) are SessionMachine / codec level with no multi-node client.
- The design removes the hazard by construction. Coordinator frames go through `offerOnSession(idx)`, which seals under `sessions_[idx]`'s own CryptoState. The seed never aliases another node, so `offerControlTo`'s `sessionForEndpoint` lookup again resolves to the caller's own session.

**InvokerGateway changes:**
- Txn coordinator fence (`txnNotCoordinator_`, `coordinatorEndpoints`).
- The NotCoordinator ErrorResponse now echoes the request corrId and is sent from the arrival node (LOW-5).
- New `sendError` and `connectCorrIds`.

## Review findings

**MAJOR:**
- **MAJOR-1** (crypto / duplicate session per node): resolved by the revert. The seed never moves, so there is one session per node and each seals under its own state.
- **MAJOR-2** (stale route index 0): resolved by the revert. The seed's endpoint never changes, so a cached index 0 always means the bootstrap node.
- **MAJOR-3** (corrId collision): fixed. `dispatchEgressFrame` never hands NotCoordinator to a SessionMachine, and the seed no longer reconnects on a coordinator answer. Regression-tested.

**MEDIUM:**
- **MEDIUM-1:** fixed. An answer naming the current coordinator is a no-op. Tested.
- **MEDIUM-2:** no retarget remains, so there is no blocking resolve.
  - A coordinator move opens or revives a session asynchronously.
  - Repeated answers naming the same coordinator are no-ops.
  - A failed coordinator session falls back once per give-up, paced by the session machine's budget / backoff. Tested.
  - `redirectPending` is no longer armed by a coordinator move, so a failed coordinator connect can no longer `failAll` producers.
- **MEDIUM-3 ruling:**
  - Keep the re-join of Joining / Syncing members, but only when the in-flight Join / Sync went out on a different session id (`joinSessionId`; 0 means refused or held).
  - A connect that keeps the id (a resume) keeps the held join.
  - A changed id: the held join's answer is addressed to the old session (lost, or taken by the re-join). A ghost from the old join costs one rebalance after `sessionTimeoutMs`, the same as waiting out the join deadline. (Corrected in fix round 1: the broker reaps members by `sessionTimeoutMs`, not with the client session.)
  - A refused Join / Sync on a connected coordinator is re-offered on the next 100 ms pass instead of waiting out the join deadline.
  - Tested.
  - The KIP-394 MEMBER_ID_REQUIRED broker change is still the long-term fix.
- **MEDIUM-4** (plain subscriptions orphaned by a seed move): resolved. The seed never moves.
- **MEDIUM-5** (coverage): covered:
  - (a) txn
  - (b) Stable failover
  - (e) late answer
  - (f) unreachable coordinator

  Not covered: (c) crypto, for the reason given above, and (d) worker routes, which no longer apply because the seed never moves.

**LOW:**
- **LOW-1:** fixed (`toChannel`).
- **LOW-2:** `coordinatorRedirects_` and `coordinatorFallbacks_` now have test accessors and are asserted.
- **LOW-3:** moot (the seed does not move).
- **LOW-4:** reverted.
- **LOW-5:** fixed.
- **LOW-6:** not reproduced. It passed in the full run; it needs a ticket.

## Full ctest
`ctest -j6` on macOS `build/`: **100% passed, 0 failed out of 1040**. Three tests are skipped as platform-only: two `InvokerTransportWakeRegression` tests and `CpuAffinity.PinsTheCallingThreadOnLinux`. This includes `NoAtomicsOrMutexesOutsideAllowlistRegression`.

## Concerns / known edges
- **Rejected / Closed coordinator session.** Such a session can never be revived, so the fallback loop for it is paced only by the group hub's 100 ms sweep: the Join to the seed is answered NotCoordinator, which falls back again. This needs a non-retriable answer from that node (ProtocolVersion / HandshakeFailed). It is not a spin, but it is chatty.
- **Unrouted txn Produce while the coordinator is not the seed.**
  - These frames are held while the seed's sid is 0. Txn Produce frames to routed partitions are held in that state too.
  - A seed reconnect does not re-send unrouted txn data batches, because the txn producer tracks the coordinator's sid. They fall to the producer's own timeout / retry.
  - This is a narrow multi-node corner case.
- **Seed retargeted by a data-op Redirect (txn-only client) before a coordinator answer.** This case is pre-existing and was left alone. A NotCoordinator naming the bootstrap node is compared against the seed's bootstrap endpoint, not the transport's current channel.
- **A retried Join / Sync after a back-pressure refusal** is re-offered at the 100 ms sweep cadence, not immediately.

## Fix round 1 (re-review r2)

**Commit:** `437d252`, on top of `ff71132` and `d22a757`.

### MAJOR-1: coordinator on the bootstrap node while the seed sits elsewhere
**Ruling: route through a session of its own to that node. Do not move the seed back.**
- The seed was moved to L by a data-op Redirect: a plain Subscribe, a txn Produce, or a stick-to-leader retarget. Its data operations live on L.
- Moving it back with a retarget + reconnect would orphan the seed's established plain subscriptions on L. That is the r1 MEDIUM-4 class of bug, reintroduced.
- A separate session to the bootstrap node keeps one session physically per node (the seed is on L, not on B). It also keeps per-session crypto correct.

What changed:
1. **"The seed" means its physical channel.** The coordinator counts as "the seed" only where the seed's default publication points (`ClientAgent::seedChannel()`, the transport's current channel). Its bootstrap identity no longer counts. This applies both to the `onCoordinatorRedirect_` same-as-current check and to `coordinatorSessionIndex`.
2. **Coordinator session lookup.** `coordinatorSessionIndex(channel)`:
   - Returns 0 if the seed is physically on that node.
   - Otherwise returns that node's existing non-seed session.
   - Otherwise opens one with the new `openSession_`. This is also what happens for the seed's bootstrap node while the seed is elsewhere; `sessionForEndpoint` would have returned the seed by identity.
3. **Control frames go by owning session, not endpoint string.** This is required because such a session shares the bootstrap channel string with the seed's identity.
   - `SessionEndpointIo` now carries `owner`. `offerControlTo(Session&)` seals under the owner's own crypto state.
   - The seed sends on the default publication; any other session sends via `offerTo(own endpoint)`. `AeronTransport::offerTo` gives the bootstrap string its own publication while the default one points elsewhere.
   - The T15a `retargetToBootstrap` / `retargetToLastKnownLeader` now identify the seed by identity.
   - This also closes the r1 MAJOR-1 class of bug (sealing under a session looked up by endpoint) for good.

Byte-identical paths (verified by the full suite): single-node, and any client whose seed never left its bootstrap.

| Test | RED at d22a757 | GREEN |
|---|---|---|
| `TxnCoordinatorOnTheBootstrapAfterASeedRetargetRegression` | "InitProducerId never reached the bootstrap coordinator (NotCoordinator answers: 1)" | InitProducerId re-sent to node 0 and OK; the seed stays on node 1 with no reconnect |
| `GroupCoordinatorOnTheBootstrapAfterASeedRetargetRegression` | "never joined the bootstrap coordinator (NotCoordinator answers: 1, joins 1)" | joins on node 0 well inside the 30 s join deadline; the seed stays on node 1 |

### MEDIUM-1: a SessionExpired / UnknownSession reaching the seed
Routing rule in `dispatchEgressFrame`, for an ErrorResponse with code SessionExpired or UnknownSession:
- If it matches an in-flight connect corrId, it goes to that session (as before).
- Otherwise, if the header sessionId is non-zero, it goes to the non-seed session whose `machine.sessionId()` equals it. Ids are cluster-unique.
- Otherwise it goes to the seed (as before).

| Test | RED at d22a757 | GREEN |
|---|---|---|
| `SessionExpiredForTheCoordinatorSessionNeverHitsTheSeedRegression` | the coordinator session never resumed; `connectsAt[0]` was 2 (the seed resumed); the seed was not Connected | the coordinator session resumes (`connectsAt[n1]==2`), the seed is untouched |

### LOWs
- **LOW-1:** fixed. The MEDIUM-3 rationale is corrected in the hub comment and in this report; the behaviour is unchanged. The KIP-394 ticket stays open.
- **LOW-2:** fixed. The seed-sid hold and re-stamp in `txnSendFn_` now apply only when `!agent_.hasRoute(t, p)`. Ruled, with a ticket: per-batch "sent on session" tracking for unrouted txn data. Unrouted partitions exist only before the first MetadataResponse.
- **LOW-3:** fixed. When a coordinator session gives up as Rejected or Closed, its channel is remembered. A NotCoordinator naming it is not re-followed until a newer MetadataResponse arrives; it schedules a metadata refresh instead. The member then waits out its join deadline rather than looping every 100 ms. Not separately tested: there is no Rejected rig, since InvokerGateway has no non-retriable connect answer per endpoint.
- **LOW-4:** fixed (the stale test header).
- **LOW-5:** partly covered. `CommitSyncRidesTheCoordinatorSessionRegression` checks that a commitSync on a non-seed coordinator goes out through `coordDirectSendTrampoline_` stamped with the coordinator session's id. Not covered:
  - EndTxn across a move: it uses the same InFlight re-send as InitProducerId, which is tested.
  - A Rejected session: see LOW-3.
  - Crypto: unchanged; no multi-node TLS rig exists.
- **LOW-6:** ruled, ticket. A disjoint Client correlation-id base (for example `1<<60`) touches many tests' corrId expectations and every Client request. NotCoordinator, the only realistic collision, is already excluded.
- **LOW-7:** ruled, ticket. An endpoint alias (IP vs hostname) costs one extra session but stays functional. Resolving through `metadataCache_.nodeIdForEndpoint` belongs with the metadata work.

### Full ctest (fix round 1)
`ctest -j6`: **100% passed, 0 failed out of 1044**, with 3 platform skips.

**Caveat: `rune_cli_lib` / `rune_cli_live_tests` do not compile at this HEAD, for a reason unrelated to D4.**
- The sibling `rune-ql` (c17cd66, ALTER TOPIC) changed `parse()` to return `variant<TopicDef, AlterTopicDef>`, which breaks `src/cli/Cli.cpp:197/205`.
- I left that alone because the API is still changing in another session, and the client CLI change for ALTER TOPIC belongs with that work.
- The CLI test binaries that ctest ran are from before that rune-ql change.
