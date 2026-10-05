# Review: consumer-group routing, fix round 2 (redesign)

**Date:** 2026-10-04
**Broker:** `rune-broker-wt51`, `fix/cg-unique-ids`, `ea807f2..HEAD` = `fedf214` (cluster-unique ids, owner-only Unsubscribe) and `55ef094` (no auto-subscribe at SyncGroup)
**Client:** `rune-client-cpp-wt14`, `fix/group-subscribe-routing`, `main..HEAD`; round 2 = `5c3cd7b`, `2701b7b`, `a8e6812`
**Inputs:** `2026-10-04-cg-routing-review.md`, `2026-10-04-cg-routing-rereview.md`, and the "Fix round 2 (redesign)" section of `2026-10-04-cg-routing-report.md`

## Verdict: CHANGES REQUIRED

The design is sound. The five rulings are implemented faithfully, and every CRITICAL and MAJOR finding from the re-review is closed. Two new MAJOR findings block the merge. Neither one needs another redesign.

| Severity | Count |
|---|---|
| CRITICAL | 0 |
| MAJOR | 2 |
| MEDIUM | 2 |
| LOW | 9 |

- **MAJOR-1.** Before the SubscribeResponse binds a partition's new subscription, the client accepts a batch from any unretired subscription on the same broker session. That includes another local owner's subscription for the same partition: another GroupConsumer's, or a plain subscription. Such a batch moves the position past records this member never consumed, which is silent loss (C1).
- **MAJOR-2.** Mixed versions are unsafe in both directions, and nothing prevents them. An old client against the new broker does not "consume nothing" as the report says. It re-reads from offset 0 and leaks a subscription every generation, and it can lose data. A new client against an old broker loses data; it does not merely stall.

## Verification performed

- **Client.** `cmake --build build` reported no work to do. `ctest -j6 --timeout 300`: **969/969 passed**, with 3 platform skips (the same as main), in 51.3 s.
- **Broker.** `cmake --build build-fast` reported no work to do. `ctest -j8 --timeout 600`: **3738/3738 passed** in 392 s. 1 test is disabled (`DrIntegration.E1b…`) and 7 are platform skips, all pre-existing. All 7 `CG4DeliveryFixture` tests ran and passed.
- **Check scripts.** All four report OK:
  - client `check_no_atomics_or_mutexes` (67 files);
  - client `check_no_shared_ptr_handles` (67 files, 8 transport allowlist sites);
  - broker `check_no_atomics` (211 files, self-test 60 fixtures);
  - broker `check_single_driver_pumper` (self-test 9 fixtures).
- **RED evidence.** I did not re-run it. I checked each claimed RED by reading the code:
  - All 3 new broker regression tests must fail on `ea807f2`: main auto-creates subscriptions at SyncGroup, uses small ids, and lets any session Unsubscribe.
  - The client test `TransactionMarkerGapIsConsumedPastRegression` must fail on `14caf1c`, because of the gap→resubscribe→park rule.
  - No Docker run, per your instruction.

---

## Status of the re-review findings

| Re-review item | Status | Evidence |
|---|---|---|
| CRITICAL-1 (stall at a transaction marker, aborted batch or compaction hole) | **Resolved** | The gap rule is gone. `GroupConsumerHub.cpp:1174-1175`: a batch entirely below the position is skipped. `:1203` sets `pos = end` for any other batch. The broker strips markers and aborted batches (`Worker.h:2786`, `:2791`) and bounds read_committed by the LSO (`Worker.h:2714`). The member's explicit Subscribe is a plain subscription (`isGroup=false`, `ClientGateway.cpp:3025-3046`), so the pre-carrier cut can never apply. Covered by `TransactionMarkerGapIsConsumedPastRegression` and the hub test `GapAdvancesThePositionRegression`. The position uses `base + recordCount`, not the last offset + 1 (LOW-1). |
| MAJOR-1 (MDC-fallback records stalled) | **Resolved** | Records are demultiplexed by subscription id regardless of source (`Client.cpp:2342-2344`). The arrival-source machinery is fully removed: `currentSourceKey`, `sourceKey` and `recordsSource` have no remaining references. Covered by `RecordsFromANonSessionSourceAreConsumedRegression`. |
| MAJOR-2 (a stale ConnectResponse bumped the epoch or source) | **Resolved** | `ClientAgent.cpp:721-739`: `awaited` = the machine was Connecting with this correlationId. The epoch is bumped and the produce resend runs only when the response is accepted. `Client.cpp:1857` ignores a response nobody awaited. `:1858-1866`: a leader session's connect only reconciles fetch. Covered by `StaleConnectResponseChangesNothingRegression`. |
| MAJOR-3 (coordinator pushes before the OffsetFetch answer) | **Resolved**, both repos together | `GroupCoordinationService.cpp:463-472` no longer subscribes. The client subscribes only after `offsetKnown` (`GroupConsumerHub.cpp:736`). |
| MEDIUM-1a (one-shot metadata) | Resolved | `Client.cpp:3806` keeps a refresh armed while `fetchWaitsForSession()` (`GroupConsumerHub.cpp:819-831`). |
| MEDIUM-1b (a session already Disconnected) | Resolved | `reconcileImpl_` revives the target session (`GroupConsumerHub.cpp:739-743`). |
| MEDIUM-2 (pre-bind gap with `resetPending`) | Resolved | Batches from the right session are accepted pre-bind, and a gap at the log start is just a gap (`:1164-1166`, `:1419-1424`). |
| MEDIUM-3 (plain Subscribe while a group is active) | Resolved as documented | `SubscriptionFailReason::LeaderElsewhereWhileGroupActive`, documented on `Client::subscribe`. The comment is corrected. Per-partition routing of plain subscriptions is ticketed. |
| MEDIUM-4 (coordinator group-subscription bookkeeping) | Moot | No coordinator-owned subscriptions exist any more. |
| MEDIUM-5 (`retired_` unbounded) | Resolved, but see MEDIUM-1 | `forgetRetired_` runs on bind (`:843`) and prunes at episode end (`:696-710`). |
| Broker ticket: `handleUnsubscribe` session check | Done | `ClientGateway.cpp:3069-3078` |

---

## Id width audit

**The broker is clean.**

- **Allocation.** There are exactly three allocation sites, all prefixed:
  - `SessionTable::nextSessionId` (`SessionTable.h:88-92`), reached from `handleConnect` (`ClientGateway.cpp:1355`) and from `injectTestSession` (`ClientGateway.h:888`);
  - `allocSubscriptionId_` (`ClientGateway.h:1598`), reached from `handleSubscribe` (`ClientGateway.cpp:3026`) and `addGroupSubscription` (`:4912`).
- **One gateway per node.** There is one gateway per node (`BrokerNode.cpp:1703`), so shards do not get duplicate prefixes. The prefix comes from `thisNodeId` (`ClientGateway.cpp:100`). A negative id gives a prefix of 0 (`ClientGateway.h:432`). Consensus has at most 16 members, so `(nodeId+1) << 40` is always positive.
- **Wire.** `MessageHeader.sessionId` is int64 (`rune-protocol MessageHeader.h:47,55`). Every client message's `subscriptionId` uses `putLE64`/`getLE64` (`DataMessages.h:430,453,510,541,592,613`). The internal SBE fields `WorkerOp`/`WorkerReturn` `sessionId`/`subscriptionId` are int64 (`rune-internal-ring.xml:153,166,234,254`).
- **Storage.** All storage is keyed by int64:
  - `SessionTable` and `subscriptions_` are int64-keyed `unordered_map`s;
  - `groupMemberSubs_` is keyed by int64;
  - `PushReadArgs` (`Worker.h:2615-2616`), `WorkerOp.h:148,238,394,450`, `SessionAnnouncer`, `TransactionCoordinationService.h:234` and `SessionAckSink.h:109` all hold int64.
- **SessionAckSink hash.** `SessionAckSink.h:585-587` uses Fibonacci hashing (`sid * φ >> (64-kBits)`). A constant high prefix only adds a constant offset to the product, so consecutive counters spread exactly as before. The 0 sentinel cannot collide, because ids are always ≥ 2^40 (or ≥ 1 with no prefix).
- **Narrowing casts.** There is one: the FR marker `Worker.h:2630-2631`, `static_cast<uint32_t>(op.sessionId())` (LOW-3). The other int32 "session ids" (`FragmentAssembler`, `WorkerReplicationTransport`, consensus `logSessionId`) are Aeron image session ids, which are a different id space.
- **No index or arithmetic use.** No session or subscription id is used as an array index or in arithmetic anywhere in `libs/`.

**The client is clean.**

- There are no narrowing casts of either id in `src` or `include`. The only shifts are the LE64 byte writes at `ProducerImpl.cpp:2743` and `SubscriberHub.cpp:540`.
- No id-indexed arrays.
- **A10 keepalive.** Pong routing uses the per-session negative correlationId (`ClientAgent.cpp:83,932`), so it is independent of the broker id.
- **SessionMachine.** It stores `int64_t sessionId_` and resends it as the resume hint (`SessionMachine.h:739,490,519`). The broker ignores the hint.
- `rv.sessionId` and `fs.brokerSid` are int64 compares.

**Headroom.** The counter has 2^40 ≈ 1.1·10^12 values per process lifetime, which is decades at any plausible connect or subscribe rate. Overflow is unguarded, though, and `|` would carry into the next node's prefix (LOW-2).

---

## Removing auto-subscribe: what depended on it

- **Broker tests.** I grepped every test that touches SyncGroup or group subscriptions:
  - `GroupPushDeliveryTest` was updated to the explicit flow.
  - `GroupHandlerIntegrationTest`, `GroupCoordinationServiceTest` and the coordination tests check only the protocol.
  - `ReadCommittedPushTest` and `F15EosRegressionTest:3591` call `addGroupSubscription` directly, so they still pass, but they now test a path production no longer uses (LOW-6).
- **Client paths.**
  - The routed path never relied on auto-subscribe after round 1.
  - The un-routed `sendSubscribeForPartitions` path (`GroupConsumerHub.cpp:580-609`, `fromOffset=0`) remains only for hub tests that have no router, which is fine in tests. It is also exactly the old-client behaviour described in MAJOR-2.
- **Other clients.** No other repo (`rune-ql`, `rune-live`, `rune-codec`, `rune-fr`) speaks the group protocol.
- **Compatibility.** The break is not just "an old client consumes nothing", and it is not documented in either repo; see MAJOR-2.

---

## MAJOR

### MAJOR-1. Pre-bind acceptance is not exclusive: another local subscription to the same partition on the same broker session moves the position (silent loss)

**Where:**
- `GroupConsumerHub.cpp:1164-1166`. Pre-bind, the hub accepts `rv.sessionId == fs.brokerSid && !isRetired_(key, rv.subscriptionId)`.
- `Client.cpp:2342-2344` feeds every Records frame to both hubs.
- `:1159` and LOW-5 now `continue` to the next consumer, and the first consumer that matches takes the batch (`return` at `:1204`).
- `reconcileImpl_`'s "never two unanswered" guard (`:749-753`) checks only this partition's abandoned (`late_`) instances. It does not check other consumers' unbound instances or plain subscriptions.

**Scenario A: two group consumers in one Client.** This is an API-supported setup, and LOW-5 was fixed specifically for it.
1. GroupConsumer A (group gA) and B (group gB) are both assigned t/p1, which node 1 leads, and both subscribe on the node-1 session (same `brokerSid`).
2. B is bound (id X) and pushing at B's position, say 10 000.
3. A rejoins (or starts) and sends its Subscribe from its committed offset C = 2 000. Before A's SubscribeResponse arrives, B's batch 10 000 reaches the hub.
4. A is first in `impls_`. The batch has A's session id and an id A has not retired, so **A accepts it**: it is delivered to A's application and `pos` becomes 10 000+n.
5. A's own subscription then delivers 2 000..9 999. Those batches are "entirely below" the position and are skipped (`:1175`). A's next auto-commit writes 10 000+n. **A has silently lost 2 000..9 999.**
6. B never sees batch 10 000, because the hub returned at `:1204`. B's next batch is a "legitimate gap", so **B also silently loses that batch** and commits past it.

**Scenario B: a plain subscription on the seed.** A Client tails topic t/p0 with `client.subscribe(...)` from Latest, and is also a group member assigned t/p0, which the seed leads.
- During the member's pre-bind window, the plain subscription's batch (same seed `brokerSid`, an id the group never retired) is accepted by the group. The position jumps to the log end, and the group loses C..end.
- The report's concern 3 understates this case. A *bound* plain subscription is equally dangerous, not only one that is still pending.

**Why it matters.** Ruling 4 makes every gap legitimate, so instance tagging is the *only* C1 protection (ruling 2). The pre-bind rule identifies a session, not an instance.

**Fix.** Make pre-bind acceptance exclusive, with no allocation and no extra hot-path cost beyond one compare:
1. Hub-wide, never send a Subscribe for (tp, session) while **any** consumer's instance for that (tp, session) is unanswered. Extend the `late_` check to the other consumers' `fs.corrId != 0` entries for the same tp and session.
2. Pre-bind, reject an id that another consumer has bound for that tp.
3. Pre-bind, reject an id that the SubscriberHub has bound. Expose `SubscriberHub::owns(subscriptionId)`, which is already an `activeById_` lookup on that path.
4. A plain Subscribe for a tp on the seed that is still pending while a group consumer's instance for that tp is pre-bind is still ambiguous. Either defer one Subscribe until the other is answered, or document the restriction explicitly. Today the restriction is undocumented.
5. Add `*Regression*` tests for both scenarios. A should not consume B's batch, and positions should never jump past an id the consumer does not own.

### MAJOR-2. Mixed client and broker versions lose data in both directions; nothing gates them, and the report describes the old-client case wrongly

**Where:**
- **Old client** (`main`): `GroupConsumerHub.cpp:569` sends a carrier Subscribe with `fromOffset=0` at every SyncGroup or Assign. Main's `onRecords` seeds the position from the first batch it sees (main `:827-831`), and main never sends an Unsubscribe.
- **New broker:**
  - Carrier step 5b (`ClientGateway.cpp:2977-3021`) finds no group subscription, so it falls through to **a new plain subscription** at `max(0, logStart)` (`:2957-2975`, `:3025-3046`).
  - `GroupCoordinationService.cpp:463-472` no longer revokes anything.
- **Neither version is gated.** `kClientVersion` is still 2 (`rune-protocol Protocol.h:125`). The broker knows each session's `clientVersion` (the D6 gate in `SessionAckSink`/`injectTestSession`), but the coordinator never looks at it.

**Scenario 1: old client against the new broker.**
1. Every generation creates a new plain subscription from the log start for each seed-led partition. Each one re-delivers the whole retained history, so duplicates are unbounded.
2. None of these subscriptions is ever unsubscribed, so they accumulate, one leak per partition per generation, until the session ends.
3. Partitions on other nodes are Redirected, and the old client moves its seed: the original Defect 1 and Defect 2.
4. **Loss path.** A leaked subscription for partition p from generation g keeps pushing near the log end. When p is re-assigned to this member in g+k, main seeds the position from the first batch it sees, which can come from the leaked subscription. OffsetFetch never lowers the position, so the member commits past unconsumed records. This is the round-0 C1 again.

**Scenario 2: new client against an old broker.** The report's concern 1 says the result is a stall. Under ruling 4 it is **loss**:
1. The old coordinator auto-creates a group subscription at SyncGroup, from C. While the new client is still waiting for OffsetFetch (`fs.epoch == 0`), it drops those records.
2. The client's explicit Subscribe on the seed is then merged into that subscription as its carrier. It binds to the group subscription's id, whose cursor has already passed the dropped records.
3. The next batch is above the position, which is now a "legitimate gap", so the position advances past the dropped records. **Silent loss.**

**Fix (required before merge):**
- Bump the protocol `kClientVersion` to 3, meaning "explicit group subscriptions", and make the broker coordinator refuse JoinGroup from a session announcing < 3 with a distinct error. Alternatively, keep the legacy auto-subscribe path only for those sessions.
- Make the new client refuse to run a group against a broker that does not advertise the capability. For example, add a broker version or capability bit to ConnectResponse, or fail the group with a clear error.
- Document the break in both repos (CHANGELOG or protocol notes). Today it appears only in the report.

---

## MEDIUM

### MEDIUM-1. Binding forgets retired instances that are still alive, so an orphan's records become acceptable in a later pre-bind window

**Where:**
- `forgetRetired_(tp)` runs on every bind (`GroupConsumerHub.cpp:843`) and removes **all** retired entries for that tp, hub-wide (`:624-630`).
- A retired instance whose Unsubscribe is still in `pendingUnsubs_` (`:646`), or that returned `Fatal` (`:648-655`), is still pushing.
- The `Fatal` log line says "the instance stays retired … until its session episode ends". That is false after the next bind.

**Scenario:**
1. In generation g, S1 is retired and its Unsubscribe is refused (Retry; back-pressure under load).
2. In g+1, S2 binds on the same session, and S1 leaves `retired_`.
3. S1 is still alive. The exact-id match protects only while S2 is the bound instance.
4. In g+2, S2 is retired and S3 enters its pre-bind window. A stray S1 batch now passes `!isRetired_`. If S1's cursor is ahead, the position jumps. C1.

The window is narrow, because a refused Unsubscribe is re-offered on every pass, but `Fatal` makes it permanent.

**Fix:** in `forgetRetired_`, keep entries whose (session, epoch, id) is still in `pendingUnsubs_`, or whose Unsubscribe returned Fatal. That stays bounded, because those entries are pruned at episode end. Correct the log text.

### MEDIUM-2. A fetch Subscribe that never gets an answer has no timeout, and the gateway's SubscribeResponse is best-effort

**Where:**
- The broker sends the SubscribeResponse with `sendTo` → `sendOnPub`, a single `tryClaim` or offer. It returns false when the publication is not connected or is back-pressured, and nobody retries (`ClientGateway.cpp:2820-2830`, `:1709-1749`).
- On the client:
  - An unanswered `FetchSub` (`corrId != 0`, `subscriptionId == 0`) stays pre-bind forever.
  - If it is dropped, it goes to `late_` (`GroupConsumerHub.cpp:681`).
  - `reconcileImpl_` then refuses any new Subscribe for that tp on that session until the answer arrives (`:749-753`).
  - `late_` is pruned only at episode end (`:702-705`).

**Scenario:**
1. Under egress back-pressure, which is likely when the session publication also carries Records (inline push or MDC-fallback modes), the SubscribeResponse for p1's instance S is dropped.
2. p1 is still consumed through pre-bind acceptance, which keeps the MAJOR-1 window open permanently.
3. After a rebalance, S goes to `late_` with an unknown id, so it is never unsubscribed; it is orphaned for the session's lifetime.
4. When p1 is assigned back to this member on the same node, it is **never re-subscribed**. The partition stalls silently while the member heartbeats.

**Fix:**
- Client: give each unanswered fetch Subscribe a deadline on the existing 100 ms sweep, paced by backoff. When it expires, retire the episode-local unknown instance by re-subscribing on a fresh correlationId only after an explicit Unsubscribe-by-correlation.
  - The cleaner variant: the broker idempotently re-answers a duplicate Subscribe correlationId with the same subscription id, and the client re-sends with the same correlationId.
- Broker: retry control responses (ticket).
- Add a `*Regression*` test: the fake drops one SubscribeResponse, then forces a revoke and a re-assign.

---

## LOW

1. **The position uses `base + recordCount`, not the last offset + 1** (`GroupConsumerHub.cpp:1174`; `DataCodec.h:321-322`). Ruling 4 says "last offset + 1".
   - For a batch whose `lastOffsetDelta > recordCount-1` (compaction-style holes inside a batch), or an empty batch counted as 1, the position falls short, and the next rebalance re-delivers that batch.
   - Only duplicates, never loss. Parsing `lastOffsetDelta` in `decodeRecords` costs the same as today.
2. **Id overflow is unguarded** (`SessionTable.h:89`, `ClientGateway.h:1598`). If the counter ever reached 2^40, `prefix | counter` would produce another node's ids. Add a cold check with an error/halt.
3. **The FR marker truncates the session id to its low 32 bits** (`Worker.h:2630-2631`). Merged multi-node traces lose the node, and the counter part is still unique per node. Cosmetic.
4. **Ids are unique only per process lifetime.** A restarted node re-issues prefix|1, prefix|2 and so on for both sessions and subscriptions. The client uses connect epochs, so liveness is correct. A stale frame from the old process that is drained after the reconnect, with a re-issued id, would pass the pre-bind or exact match. This is negligible in practice. Seeding the counter from an incarnation number, such as the node's start term, would make ids unique across restarts at no cost.
5. **One indirect call per Records batch on the client hot path.** `router_.epoch(router_.ctx, …)` (`GroupConsumerHub.cpp:1163`) is a function-pointer trampoline into the agent. It is per batch, not per record, so it is cheap. Under the no-virtual-calls rule, consider caching it: reconcile already retires instances when the epoch changes.
6. **Production no longer uses the coordinator-subscription machinery, but it is still live:**
   - `addGroupSubscription`, `groupMemberSubs_` and `revokeGroupSubscriptions`;
   - carrier step 5b (`ClientGateway.cpp:2977-3021`), which runs on every Subscribe;
   - the worker's `preCarrier` cut (`Worker.h:2791-2795`);
   - `ReadCommittedPushTest`, which tests only this path.

   Remove it, or ticket its removal; otherwise it hides the compatibility semantics of MAJOR-2.
7. **Retention below the committed offset is now skipped silently.** The broker clamps `fromOffset` up to `logStart` (`ClientGateway.cpp:2969-2973`), and the client accepts that as a gap. This is consistent with ruling 4, but the previous error log is gone. Log it once per partition when the first batch's base is above the requested `fromOffset`, a single compare on bind.
8. **`StaleConnectResponseChangesNothingRegression` settles with a never-true predicate for 20 000 pump iterations** (`GroupRoutingRegressionTest.cpp`, MAJOR-2 test). It is bounded and deterministic in iteration count, but the negative assertion depends on timing. Assert on a positive marker instead: the gateway echoes a Ping after the stale frame.
9. **Carried over:** the watcher ignores session index > 64 (re-review LOW-4). The LOW-5 multi-group `continue` has no test, and MAJOR-1 shows it needs one.

---

## C1 safety: direct answers

- **Can a stray old-instance batch still move a position?**
  - From another node session: no, because `rv.sessionId != brokerSid` pre-bind and the id is exact after bind.
  - From the same session: no, while the instance is retired or abandoned (`late_` blocks a new Subscribe until its answer, then the instance is retired).
  - **Yes** in two cases: another local owner's live subscription on the same session (MAJOR-1), and an orphan forgotten at a later bind (MEDIUM-1).
- **Can records from before the OffsetFetch answer arrive and count?** No. Before the answer there is no `FetchSub` with `epoch != 0` (`:1161-1163`), the position is at the sentinel (`:1173`), and no Subscribe is sent (`:736`). A retried Subscribe that was refused (Retry) leaves no state and sends nothing. The exception is a pre-existing user subscription to the same tp, which can be counted *after* the answer, during the pre-bind window (MAJOR-1 B).
- **A stale OffsetFetchResponse** (correlationId 0, from an earlier generation or a duplicate answer to a retry) can only raise the position to a committed offset. A lower value means duplicates, never loss.

## Transactions: direct answers

- **read_committed through a group.** The member's subscription is a plain subscription with `isolation = cfg.isolation` (`GroupConsumerHub.cpp:757-759`). The owning worker bounds it by `min(HW, LSO)` (`Worker.h:2714`), strips markers (`:2786`) and strips aborted batches (`:2791`). read_uncommitted strips markers only.
- **Gaps.** Both kinds of gap are now consumed past, and the position advances (CRITICAL-1 is resolved).
- **The LSO bound.** It is respected, because a batch is cut at `last >= bound` (`Worker.h:2784`). When the RC state is not ready, the read is held (`:2700-2711`, hold at `:2706`), never served early.

## Hot paths

- `onRecords` does not allocate: two `std::map::find` calls, one indirect epoch call, and, only before bind, a linear `isRetired_` scan over a list bounded per partition.
- There are no new atomics, mutexes or `shared_ptr` handles; all four scripts pass.
- The broker diff touches only cold paths (allocation, Unsubscribe, SyncGroup).

## Tests

- **Quality.** Tests are condition-driven, use bounded pumps, have no sleeps, use kernel-picked free ports and are named `*Regression*`. The broker tests use the real gateway on embedded IPC.
- **RED.** RED is plausible by inspection for all new tests; I did not re-run it.
- **Missing coverage:**
  1. two consumers, or a plain and a group subscription, on the same tp and session during the pre-bind window (MAJOR-1);
  2. mixed versions (MAJOR-2): at minimum, a broker test that an old-version JoinGroup is refused;
  3. a forgotten retired orphan (MEDIUM-1);
  4. a lost SubscribeResponse followed by revoke and re-assign (MEDIUM-2);
  5. a client against the real in-process gateway for group + read_committed with markers. The client side uses the fake; the broker side covers plain read_committed subscriptions in `ReadCommittedPushTest` and `GatewaySubscribeTest`. Your DR run (`test_tx_read_committed`, `test_tx_eos`, plus the CG files) closes this gap end to end.

## Required before merge

- MAJOR-1 and MAJOR-2, each with a `*Regression*` test.
- MEDIUM-1, a small client change with a test.
- MEDIUM-2: the client deadline now, and a broker ticket for the response retry.
- LOWs to the backlog. LOW-1 is worth doing with MAJOR-1, since it is the same function.
- Then your Docker DR run with both images: broker `55ef094` or later, client `a8e6812` or later.
