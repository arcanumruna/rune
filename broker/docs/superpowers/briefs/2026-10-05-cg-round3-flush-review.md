# Review: CG routing fix round 3 (A) and TransactionalProducer flush (B)

**Date:** 2026-10-05
**A:** rune-protocol `8f6b38f`; broker `rune-broker-wt51` `ac34a9b` (on `806c259`); client `rune-client-cpp-wt14` `c9092f7` (on `a8e6812`).
**B:** client `rune-client-cpp-wt15` `6733c28` (on main `3c7e97b`).
**Inputs:** `2026-10-04-cg-redesign-review.md` and the "Fix round 3" section of `2026-10-04-cg-routing-report.md`.

## Verdicts

| | Verdict | CRITICAL | MAJOR | MEDIUM | LOW |
|---|---|---|---|---|---|
| **A** (CG round 3) | **CHANGES REQUIRED** | 0 | 2 | 3 | 5 |
| **B** (txn flush) | **APPROVE WITH CHANGES** (fix the MEDIUMs; no re-review needed) | 0 | 0 | 3 | 2 |

- **A MAJOR-1.** The broker's queued-Subscribe retry path (`trySubOp_`) builds the worker op without the correlationId. Every worker push for such a subscription carries correlationId 0, so the group member never matches it and the partition stalls silently. This path runs exactly when the worker ring is full, which is under load.
- **A MAJOR-2.** rune-protocol's own suite is red: 5 ConnectResponse tests fail at `8f6b38f`. The 42-byte form that every broker now sends has no golden vector. Client main, built against the new protocol, also fails 2 conformance tests.

## Verification performed

- **Client A** (`wt14`). `cmake --build build` reported no work to do. ctest: **976/976 passed** (3 platform skips), in 62 s.
- **Client B** (`wt15`). The build reported no work to do. ctest: 973/976.
  - `ClientCodecConformance.ConnectResponse` and `ClientCodecConformance.ConnectResponseRedirect` **FAIL**. They expect 38 bytes, and the sibling rune-protocol is now `8f6b38f`, which encodes 42; see A MAJOR-2 and the A/B section.
  - `ShardGroupRegression.UdpEgressAcksArriveOnEachShardsOwnPort` aborted once and passed on rerun. It is flaky, unrelated to flush, and pre-existing.
- **Broker** (`wt51`). `cmake --build build-fast` reported no work to do. ctest: **3741/3741 passed** in 411 s. 1 test is disabled (`DrIntegration.E1b…`) and 7 are platform skips, all pre-existing.
- **rune-protocol** (not in the brief, but it ships too). I ran `cmake --build build --target rune_protocol_tests` and then `./build/rune_protocol_tests`: **494/499**. These 5 FAIL:
  - `ConnectResponseCodec.RoundTrip`;
  - `ConnectResponseCodec.LargeAndNegativeValues`;
  - `ConnectResponseCodec.IntBoundaryValuesAllExtrema`;
  - `ConformanceGolden.ConnectResponse`;
  - `ConformanceGolden.ConnectResponseRedirect`.
- **Check scripts.** All OK:
  - client `check_no_atomics_or_mutexes` (67 files) and `check_no_shared_ptr_handles` (67 files, 8 allowlisted sites), in both wt14 and wt15;
  - broker `check_no_atomics` (211 files, self-test 60 fixtures);
  - broker `check_single_driver_pumper` (self-test 9 fixtures).
- **A/B merge.** `git merge-tree 6733c28 c9092f7` merges cleanly (the base is `3c7e97b`), and the two touch disjoint files. B adds `HubCommand::Op::TxnFlush = 37`; A adds no ops.
- No Docker, no VM, no conan.

---

# A. Consumer-group round 3

## Status of the rulings

| Ruling | Status | Evidence |
|---|---|---|
| **MAJOR-1, broker:** every push stamps the creating Subscribe's correlationId | **Partial: see MAJOR-1 below** | The correlationId is stored in `Subscription` (`ClientGateway.h:1079`, set at `ClientGateway.cpp:3056`), in `BufferedSubscribe` (`ClientGateway.h:1196`, `ClientGateway.cpp:2677`, `:3069`), in `WorkerOp.correlationId` (`enqueueSubscribeOp_`, `:2590`) and in `WorkerSubscription` (`Worker.h:759`, `:2236`, `:2256`). The push sites are: gateway inline `ClientGateway.cpp:3518`, PushRead `:4330`, worker `Worker.h:2497`. **Missing:** the retry path `trySubOp_`, `ClientGateway.cpp:2623-2656`. |
| **MAJOR-1, client:** binds by (session, correlationId); "any non-retired id" acceptance removed | **Resolved** | `GroupConsumerHub.cpp:1166` requires `rv.correlationId == fs.instanceCorr && rv.sessionId == fs.brokerSid` and the epoch. `:1168-1175` bind by the first push, then match the id exactly. `isRetired_`, `forgetRetired_` and `retired_` are gone. SubscriberHub: pending binds by push (`SubscriberHub.cpp:425-442`), and a bound subscription checks the correlationId (`:447`). |
| **MAJOR-2:** client v3, coordinator refuses JoinGroup below 3, client refuses an old broker, documented | **Resolved, with MEDIUM-3** | `Protocol.h` has `kClientVersion = 3` and `kBrokerVersion = 3`. Coordinator: `GroupCoordinationService.cpp:295-314` returns `ErrorCode::ProtocolVersion` and logs a warning. The session always exists, because `ClientGateway.cpp:4706` drops unknown sessions, so the `-1` bypass is unreachable. Client: `Client.cpp:1917` (seed OK only), `GroupConsumerHub.cpp:929` (joinIfConnected), `:1516` (onConnected), `:1252` (`ProtocolVersion` → `RefusedByBroker`). `GroupConsumer::failReason()` is carried in the final status snapshot. Documented in the design spec §7, rune-protocol `Protocol.h`, and `GroupConsumer.h` `GroupFailReason`. |
| **MEDIUM-1:** the retired list is per instance and never cleared on bind | **Moot / resolved** | No retired list exists. An abandoned instance can never match, because its correlationId differs. `late_` is cleaned up by the answer (`:856-862`) or by the first push (`learnLateFromRecords_`, `:615`), and pruned at episode end. The Fatal log text is corrected. |
| **MEDIUM-2:** no timeout; unanswered Subscribes re-sent on events | **Resolved, with MEDIUM-2** | No constant is added. `resendUnanswered` (`GroupConsumerHub.cpp:765`) is called from `Client.cpp:1864` and `:3323`. The "never two unanswered" guard is removed. Broker: `sendSubscribeAnswer_` (`ClientGateway.cpp:210-217`) queues answers through `enqueueOrBestEffort_` at `:2825` and `:2851`. |
| LOW-1, LOW-2, LOW-8 | Resolved | `lastOffsetDelta` (`DataCodec.h`, `GroupConsumerHub.cpp:1180`); the overflow aborts (`SessionTable.h:92`, `ClientGateway.h:1614`). |

## Specific questions

- **CorrelationId uniqueness.**
  - Group fetch Subscribes use `kFetchCorrTag (1<<61) | counter` (`GroupConsumerHub.h:256`), monotonic per hub. They are never reused across sessions, reconnects or episodes: `resendUnanswered` always allocates a new one.
  - Plain Subscribes use the Client's small counter. A re-sent *pending* plain Subscribe keeps its correlationId across sessions. That is safe for bound matching (by subscriptionId) and for pending matching, as long as the seed session id is non-zero (MEDIUM-1).
  - **Across clients**, correlationIds collide routinely: every client's hub starts at `2^61|1`, and plain counters are small. This matters because the worker's dynamic-MDC egress is shared across consumers (`WorkerEgress.h:3-18`), so a client receives other clients' Records. Exactness therefore relies on the broker session id, which is cluster-unique: the group path checks it, while the SubscriberHub pending path and `learnLateFromRecords_` do not always check it (MEDIUM-1, LOW-2).
- **Ping, A10 negative ids, and 0.**
  - There is no collision. Records carry only Subscribe correlationIds. A group instance's id always has bit 61 set, so it is never 0 or negative. A pending plain subscription must be found in `pendingByCorrelation_`, and Pongs are not Records.
  - correlationId 0 is accepted only for an already-bound plain subscription (`SubscriberHub.cpp:447`), and a group never accepts it. That is precisely why MAJOR-1 stalls a group but not a plain subscription.
- **Every push path stamps the id, after an ownership move or re-subscribe.**
  - A re-subscribe of the same subId updates it (`Worker.h:2236`).
  - A Subscribe that reaches a non-owner is registered parked and activated later with the same op data (`Worker.h:4437-4441`).
  - Resolving the egress waiters copies the `BufferedSubscribe` (`:2756`, `:2764`).
  - **Except** the queued retry path: MAJOR-1.
- **ConnectResponse 38→42 bytes.**
  - The encoder writes `brokerVersion` at `[38..41]` with blockLength 18.
  - Decode accepts at least 38 bytes and reads the version only when `blockLength >= 18 && avail >= 42` (`ControlMessages.h:143-162`). The backward decode is correct.
  - An old client decodes the 42-byte frame, because the old decoder checks `avail >= 38` and `checkFrame` (`Dispatch.h:108-139`) does not check blockLength.
- **Goldens staying 38 bytes.** Keeping the v0 golden is right *as a decode-compatibility vector*. Two things are wrong, though:
  1. rune-protocol's own tests were not updated, and 5 are red.
  2. The form that is actually on the wire now (42 bytes) has no golden anywhere. Only the client test `ConnectResponseCarriesTheBrokerVersionRegression` covers it, by comparing bytes 2..37 against the v0 golden.

  See MAJOR-2.
- **Version gate.**
  - The coordinator answers `ErrorCode::ProtocolVersion` with an empty member id.
  - The new client maps that to `GroupFailReason::RefusedByBroker` and finishes, so there is no rejoin storm.
  - An old (v2) client treats an unknown code as `toRejoining_` (main `GroupConsumerHub.cpp:874-876`). It loops on the rejoin backoff and gets an unthrottled warning on every attempt (LOW-3). It consumes nothing and loses nothing, as ruled.
- **Hot paths.**
  - Group `onRecords`: one `std::map::find`, compares, and one indirect epoch call. No allocation. `learnLateFromRecords_` runs only on the drop path.
  - SubscriberHub: a new `pendingByCorrelation_.find` on *every* group batch (LOW-1).
  - Broker: the stamp is a field copy. No new atomics or mutexes; the scripts are green.

---

## MAJOR

### A-MAJOR-1. The queued Subscribe retry drops the correlationId, so a group partition stalls silently

**Where:**
- `ClientGateway.cpp:2623-2656`. `trySubOp_` rebuilds the `WorkerOp` from `p.sub` and sets `subscriptionId`, `sessionId` (`:2646`), topic, partition, offset, isolation, `logPtr` and `egressPub`. It does **not** set `op.correlationId = s.subscribeCorrId`, even though `PendingSubOp::sub` carries it (`ClientGateway.h:1196`).
- It is reached through `pumpSubOps_` (`:2658`) for every Subscribe that `enqueueSubscribeOp_` queued (`:2603-2610`): the worker ring was full, or a lifecycle op for that worker was already queued (`behind`).
- The worker then stores `subscribeCorrId = 0` (`Worker.h:2256`), and every push carries 0 (`Worker.h:2497`).
- On the client, the group requires `rv.correlationId == fs.instanceCorr` (`GroupConsumerHub.cpp:1166`). Nothing matches, and each batch is counted in `droppedBatches_`.

**Scenario.**
1. Under the DR CG load, producers keep worker 2's op ring near full.
2. A rebalance assigns t/p3, owned by worker 2, to member M. M's Subscribe is answered OK: the gateway sends the SubscribeResponse at once, and M binds the id through `onFetchSubscribeResponse`.
3. The worker op is queued and later retried through `trySubOp_`.
4. All of p3's pushes carry correlationId 0, and M drops every one of them. M stays Stable and heartbeats, but nothing is delivered and nothing is committed. Nothing re-subscribes it either: the instance is *answered*, so `resendUnanswered` skips it.
5. This is a silent, permanent stall until the next rebalance. Plain subscriptions are unaffected, because 0 is accepted once bound.

**Why the tests missed it.** `WorkerPushFixture.RecordsCarryTheSubscribeCorrelationIdRegression` sets `op.correlationId` and enqueues it directly on the Worker, bypassing the gateway. `CG4DeliveryFixture.Gateway…` exercises the inline gateway push. Neither test reaches `enqueueSubscribeOp_` or `trySubOp_`. The test hook `forwardSubscribeForTest` (`ClientGateway.h:1806-1811`) cannot even pass a correlationId.

**Fix.**
- Add `op.correlationId = s.subscribeCorrId;` in `trySubOp_`.
- While you are in those 10 lines, compare with `enqueueSubscribeOp_` (`:2598`). `trySubOp_` also omits `op.replica`, which is pre-existing (LOW-4).
- Add a `*Regression*` test through `forwardSubscribeForTest`, with a correlationId parameter added to the hook. Fill the owner ring, or queue an op ahead, so the Subscribe goes through `pumpRetryQueuesForTest`. Then assert that the worker's `WorkerSubscription.subscribeCorrId`, or the pushed Records, carry the id.
- Also add one test for the PushRead stamp (`:4330`), which is untested today (LOW-5).

### A-MAJOR-2. rune-protocol's suite is red, and the wire form actually sent has no golden

**Where:**
- `rune-protocol/tests/protocol/client/ControlMessagesConnectTest.cpp:139,143,175` still assert `ENCODED_SIZE == 38` and `written == 38`.
- `ConformanceTest.cpp:177,196,210,229`:
  - `assertHeader(golden, …, BODY_FIXED)` now expects 18 against the golden's 14;
  - `encode()` returns 42, against the expected 38.
- **5 failures** at `8f6b38f`. The report says the goldens are "byte-identical", which is true, but the tests that read them were never run.
- Downstream, client main (`wt15`, B) compiles against the sibling rune-protocol. It fails `ClientCodecConformance.ConnectResponse` and `ClientCodecConformance.ConnectResponseRedirect` until A's `tests/ConformanceTest.cpp` change is merged.

**Why it matters.**
- A red suite ships in the protocol repo that both images build from.
- There is no conformance vector for the 42-byte frame that every v3 broker now emits, so a third-party decoder has nothing to pin against.
- The v0-only goldens are the right compatibility pin, but on their own they leave the current encoding unpinned.

**Fix.**
- In rune-protocol:
  - keep `connect_response.bin` and `connect_response_redirect.bin` as **v0 decode vectors**: assert blockLength `BODY_FIXED_V0`, `brokerVersion == 0`, and re-encode through the V0 truncation;
  - add `connect_response_v3.bin` (42 bytes, `brokerVersion = 3`) to `gen_vectors.cpp`, the manifest and `ConformanceTest`;
  - update the 3 `ConnectResponseCodec` tests to 42 bytes, and add a decode test for the 38-byte form.
- Re-run `rune_protocol_tests`.

---

## MEDIUM

### A-MEDIUM-1. SubscriberHub's push-bind is unchecked when the seed session id is 0, so it can bind another client's subscription

**Where:**
- `SubscriberHub.cpp:436`: `if (currentSessionId != 0 && rv.sessionId != currentSessionId) return;`.
- `Client.cpp:2346` passes `agent_.seedSession().machine.sessionId()`, which `SessionMachine::doConnect(resetSessionId=true)` sets to **0** for the whole fresh-connect window (`SessionMachine.h:471-474`).
- Pending plain Subscribes survive that window (they are re-sent on connect).
- Worker push uses a dynamic-MDC publication shared by consumers (`WorkerEgress.h:3-18`).

**Scenario.**
1. Two identical DR clients X and Y each send a plain Subscribe for topic T. Deterministic startup gives both the same small correlationId, say 7.
2. X's seed drops and X reconnects with a reset session id, so its seed session id is 0. X's Subscribe 7 is pending.
3. Y's push for its subscription (correlationId 7, Y's session) arrives on X's MDC image. X binds its pending subscription to **Y's subscriptionId** and consumes Y's cursor.
4. X's own Subscribe is re-sent and answered, but the pending entry is gone, so the answer is ignored. X's real subscription is orphaned on the broker, and X's handler sees Y's offsets, which are not X's `fromOffset`.

**Fix.** Treat 0 as "no binding by push": `if (currentSessionId == 0 || rv.sessionId != currentSessionId) return;`. The SubscribeResponse still binds. Update the hub tests that rely on the default argument, and add a `*Regression*` test: a pending plain subscription with session 0 receives another session's push with the same correlationId and must not bind.

### A-MEDIUM-2. `resendUnanswered` abandons in-flight Subscribes on every session, not just the one with the event

**Where:** `GroupConsumerHub.cpp:765-779` drops every Stable consumer's `fs` with `corrId != 0`, whatever `fs.session` is. It is called from `Client.cpp:1864`, on any leader session's accepted connect, and from `:3323`, on watcher events.

**Scenario.**
1. At startup, a member assigned 12 partitions over 3 nodes subscribes on node 1's session.
2. Node 2's leader session connects about 1 ms later. Every node-1 Subscribe whose answer is still in flight is abandoned to `late_` and re-sent.
3. Node 3 connects, and the same happens again.
4. Each abandoned instance is a **live broker subscription that replays from the committed offset** until its answer arrives and the Unsubscribe lands. The result is 2–3 times the read and egress load at startup and after every failover, for no correctness benefit.

Broker answers are now queued, so on a live session an answer is never lost (`sendSubscribeAnswer_`). Only instances on the session that ended or reconnected can need a re-send.

**Fix.** Pass the session index of the event, and drop or re-send only the unanswered instances with `fs.session == that index`, or those whose epoch is no longer current. `reconcileImpl_` already handles the epoch case. Add a test: a connect on session B leaves session A's unanswered instance alone.

### A-MEDIUM-3. The version gate covers only the seed and the coordinator; an old partition leader stalls silently

**Where:**
- `Client.cpp:1917` learns `brokerVersion` only on the **seed** ConnectResponse.
- The leader-session branch (`:1861-1866`) has `opt->brokerVersion` and ignores it.
- The docs (design spec §7, `GroupFailReason`) say "client and broker".

**Scenario.** In a rolling upgrade, the seed and coordinator node 1 run v3, while partition leader node 2 still runs an old version.
1. The member subscribes t/p1 on node 2.
2. The old broker pushes with correlationId 0 (and an unprefixed id).
3. The member drops every batch (`GroupConsumerHub.cpp:1166`). The partition stalls silently with `failReason() == None`, while everything else looks healthy.

**Fix.** Pick one of these:
- record `brokerVersion` per leader session and, below 3, park the partition with a logged reason, or finish the consumer `BrokerTooOld`;
- or state explicitly in all three places that the **whole cluster** must be at version 3 or later before any v3 GroupConsumer runs.

The first is better.

---

## LOW

1. **An extra lookup per group batch on the SubscriberHub hot path.** `SubscriberHub.cpp:425-442`: every group Records frame misses `activeById_` and then does `pendingByCorrelation_.find`. The comment says "Cold: once per subscription", which is wrong. Skip the lookup when `rv.correlationId` has the group tag (bit 61); it is a single compare.
2. **`learnLateFromRecords_` matches by correlationId only** (`GroupConsumerHub.cpp:615-625`). Through the shared MDC, another client's push with the same tagged correlationId removes our `late_` entry and sends an Unsubscribe for *its* id on our session. The broker ignores it, because only the owner may unsubscribe. Our abandoned instance then leaks until the episode ends. Store `brokerSid` in `LateSub` and require `rv.sessionId == brokerSid`.
3. **Old clients flood the broker log with warnings** (`GroupCoordinationService.cpp:301-303`). The JoinGroup refusal logs `spdlog::warn` on every attempt, and old clients rejoin forever. Rate-limit it per session, for example the first refusal and then every 2^n.
4. **`trySubOp_` also omits `op.replica`** (pre-existing; compare `:2598`). A retried Subscribe registers `replica = nullptr`. Whether `subReplica_` (`Worker.h:4401-4404`) then yields no RC or HW context depends on `incarnation`. Verify this while fixing MAJOR-1; it is the same 10 lines.
5. **The PushRead stamp (`ClientGateway.cpp:4330`) has no test.** The worker test bypasses the gateway, and the gateway test covers the inline path only. Covered by the test asked for in MAJOR-1.

(The report's backlog LOWs 3, 4, 5, 6, 7 and 9 are still open, as stated.)

---

# B. TransactionalProducer::flush / flushAndWait

## State-machine audit

| State when the flush is issued, or while it is pending | Behaviour | OK? |
|---|---|---|
| Uninitialized / Initializing / Ready | Nothing pending: `dataSettled_` is true, so Ok on the next `doWork` | Yes; Kafka flush() is a no-op here |
| InTransaction | Waits until no waiting records, no batches and no AddPartitions/AddOffsets/CommitOffsets remain, then completes Ok; the transaction stays open | Yes |
| Committing | Settles in the same cycle that `gateEndTxn_` sends EndTxn, so Ok. The flush never blocks the commit gate: it is not an AddOffsets or CommitOffsets phase, and `gateEndTxn_` looks only at its own op | Yes |
| **Aborting** (user abort, close abort, or abort after poison) | `dropWaiting_` discards unsent records, and the flush then completes **Ok** | **No: MEDIUM-1** |
| AbortableError | Completes at once with the poison status (`gateFlushes_`), even with batches in flight | Yes; documented in the header |
| FatalError | A new flush is refused Fenced (`TransactionalProducerImpl.cpp:183-185`). A pending one completes Fenced via `enterFatal_` | Yes |
| Closing / closed | A new flush is discarded Closed (`discardCommand`). A pending one completes Closed via `failAll`, **unless** it completed Ok first after the close abort dropped records (MEDIUM-1) | Partly |
| Op deadline | `sweep_` completes it Timeout, with no state change and no poison | Yes |

**Kafka parity.**
- Kafka `flush()` blocks until the accumulator drains, and it never reports record failures; those surface through per-record futures and callbacks.
- Here, `TxnSend` has no per-record completion, so the flush status is the only delivery signal. It is therefore reasonable for flush to fail with the poison status (and it is documented), but that makes an incorrect Ok worse than in Kafka.
- An abort dropping undrained records matches Kafka (`abortUndrainedBatches`), so flush-before-abort is the correct way to get aborted data into the log.

## MEDIUM

### B-MEDIUM-1. Flush completes Ok after an abort, a close or a poison has dropped the records it covered

**Where:**
- `gateFlushes_` (`TransactionalProducerImpl.cpp:760-772`) checks only `dataSettled_()` and `SAbortableError`.
- `dropWaiting_()` silently removes the waiting records:
  - on a user abort: `:328` (from InTransaction) and `:337` (from AbortableError);
  - on a close abort: `:1519` and `:1528`;
  - when poisoned: `:576` and `:584`.

**Scenario.**
1. `send(r1); send(r2); flush(); abortTransaction();` from the app thread. All four commands drain in one duty pass, before `doWork`'s produce loop. Alternatively, `close()` races a pending flush.
2. The abort moves the producer to `SAborting` and `dropWaiting_()` discards r1 and r2. `batches_` is empty.
3. `gateFlushes_` sees settled data and completes the flush **Ok**, although r1 and r2 were never produced.
4. The same happens when a poison is followed by an abort before the next `doWork`: the state is already `SAborting`, not `SAbortableError`.

The header's contract ("completes Ok once every record sent so far is produced and acked") is violated, and the DR reasoning ("the aborted records reach the log") would be silently false.

**Fix.** Keep a duty-side `droppedRecords_` counter, incremented in `dropWaiting_` and `eraseBatch_` by the number of records dropped. Snapshot it in the Flush `UserOp`, and complete with Aborted or DeliveryFailed when it has changed. Alternatively, complete pending flushes inside `dropWaiting_` with the abort's status. Add a `*Regression*` test for flush followed by abort in the same pass, and for close with a pending flush.

### B-MEDIUM-2. In the DR client, a flush failure skips the abort and is reported as an abort failure

**Where:** `examples/dr_client.cpp:565-570`. `flushAndWait` throws on any non-Ok result: a poisoned transaction (DeliveryFailed after a batch times out under netem or a crash) or a Timeout. The `catch` then prints `FAIL txn abortTransaction`, returns 1, and **never sends the abort**.

**Scenario.** In a DR fault test with `doCommit=false`, a leader crash makes a batch time out, so the transaction is AbortableError. Before this change the abort succeeded, and the test expected `outcome=aborted` with rc 0. Now the flush throws, rc is 1, and the broker transaction stays open until the coordinator times it out, which can delay the LSO for read_committed checks. Under faults this is a new DR failure mode.

**Fix.** Wrap the flush in its own try/catch, log `TXNFLUSH failed: …` (an expected outcome under faults), and then always call `abortTransactionAndWait`. Name the failing step correctly in the messages.

### B-MEDIUM-3. Tests cover only the happy path and the empty case

The 2 tests (`FlushWaitsForEveryRecordThenAbortDropsNothing`, `FlushWithNothingPendingCompletesAtOnce`) do not cover:
- completion with the poison status (`SAbortableError`) while batches are in flight;
- refusal in FatalError, and fencing while a flush is pending (`enterFatal_` → Fenced);
- close while a flush is pending (Closed);
- the op-deadline Timeout through `sweep_`, which must cause no poison;
- flush during Committing, which must not delay or duplicate EndTxn;
- MEDIUM-1.

Add one `*Regression*` test per row of the audit table, using the existing `TxnRig` helpers (`makeProduceResponse`, cycles). The cost is about 6 short tests.

## LOW

1. **`gateFlushes_` scans every duty cycle even with no flush pending.** It calls `dataSettled_()`, which is O(partitions + requests + ops), on every `doWork` (`:763-764`), before checking whether a Flush op exists. Check `hasPendingFlush` first, or keep a count, like `gateEndTxn_`'s early `id == 0` return.
2. **The completion loop rescans `ops_` from the start** (`:765-771`), which is O(n²) when several flushes complete together. It is cold and harmless; one pass with erase-by-index would do.

---

# A/B interaction

- **No textual conflict.** `git merge-tree` is clean, and the two change disjoint files.
- **Semantic.** B on its own, built against rune-protocol `8f6b38f`, is red: the 2 ConnectResponse conformance tests fail. A's `tests/ConformanceTest.cpp` fixes them. The combined tree must therefore include A's test change, which it will after the merge. Fix the protocol's own tests too (A-MAJOR-2).
- **Docker.** The images need rune-protocol `8f6b38f` or later, the broker at `ac34a9b` plus the MAJOR-1 fix, and a client containing A and B merged. The `dr_client` change in B affects only `txn_produce` aborts (B-MEDIUM-2), not the CG paths.

# Required before Docker / merge

- **A:** MAJOR-1 (`trySubOp_` correlationId, plus a regression test through the gateway queue) and MAJOR-2 (rune-protocol tests and a v3 golden). Then MEDIUMs 1–3 and LOW-4. A re-review is needed only for MAJOR-1's test; MAJOR-2 is mechanical.
- **B:** MEDIUMs 1–3. No re-review.
