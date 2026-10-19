# Re-review: consumer-group fetch routing, fix round 1 (`fix/group-subscribe-routing`)

**Date:** 2026-10-04
**Worktree:** `/Users/vradu/Projects/rune-client-cpp-wt14`
**Commits reviewed:** `3ad6fb8`, `187f129`, `14caf1c` (diff `6d2e013..HEAD`)
**Inputs:** `2026-10-04-cg-routing-review.md` (round-0 REJECT) and the "Fix round 1" section of `2026-10-04-cg-routing-report.md`
**Broker checked:** `rune-broker-wt26` @ `161de4c`

## Verdict: REJECT

| Severity | Count |
|---|---|
| CRITICAL | 1 |
| MAJOR | 3 |
| MEDIUM | 5 |
| LOW | 6 |

The four rulings are implemented as written, and most of the round-0 findings are closed. The blockers are mostly new:

1. The literal "a gap never advances the position" rule stalls every group partition at its first transaction control marker. The broker never pushes those markers, so every transactional topic hits this (CRITICAL).
2. Two of the foundations that C1 and M3 now rest on can be corrupted or bypassed:
   - The connect epoch and source key are bumped by ConnectResponses that the SessionMachine ignored.
   - Records sent through the broker's MDC fallback come from a source no session owns.
3. A coordinator-led partition whose records arrive before the OffsetFetch answer stalls until new data is produced.

## Verification performed

- **Build and tests.** `cmake --build build` reported no work to do. `ctest --test-dir build -j6 --timeout 300` gave **962/962 passed**, with 3 platform skips (the same as main), in 40.9 s.
- **Check scripts.**
  - `check_no_atomics_or_mutexes.py`: OK (67 files).
  - `check_no_shared_ptr_handles.py`: OK (67 files; 8 transport allowlist sites).
- **Hot path.**
  - There are no new atomics, mutexes or `shared_ptr` handles.
  - `onRecords`, `recordsSource` and `currentSourceKey` do not allocate per frame. The only allocation is the first frame of a new image (LOW-1 covers the scan cost).
- **Tests.**
  - No sleeps: the UDP tests use bounded pump loops.
  - Ports are kernel-picked (`GroupRoutingRegressionTest.cpp:39-50`). They are subject to the usual bind-then-close TOCTOU race, which is acceptable.
  - I did not re-run the RED evidence.
- **Broker code read (wt26):**
  - `BrokerNode.cpp`: gateway construction (1700), shard runtimes (3173-3330);
  - `ClientGateway.cpp`: connect (1356), ConnectResponse send (1540-1560), per-worker record pubs (2647-2745), carrier (2971-3012), Unsubscribe (3056-3075);
  - `WorkerEgress.h`;
  - `Worker.h` push loop (2389-2530);
  - `SessionAckSink.h:957-985`;
  - `GroupCoordinationService.cpp:478-545`.

---

## Rulings: status

### C1 (positions, current instance, gaps, rejoin)

| Rule | Status | Evidence |
|---|---|---|
| Seeded only by OffsetFetch or reset | Met | `GroupConsumerHub.cpp:1160`: a sentinel position drops the batch. `:1420-1431` seeds from OffsetFetch, or sets 0 plus `resetPending` when nothing is committed. `:715`: no Subscribe until `offsetKnown`. |
| Current instance of current generation only | Met, with caveats (MAJOR-1, MAJOR-2) | `:1143-1153`: session, epoch, and either the bound id or a pre-bind id that is not retired. `fetch` is cleared on Rejoining and on SyncGroup. |
| A gap never advances | Met literally, but wrong for the broker's legitimate skips | `:1163-1180`; see **CRITICAL-1** |
| Rejoining kills old subscriptions | Met | `:183-189`: `toRejoining_` calls `dropAllFetch_`, which retires every subscription and unsubscribes the ones the client owns (`selfOwned`). |

### M1 (dead leader session): mostly met

- `Client.cpp:3261-3292` (`watchGroupSessions_`) and `:1527-1530` handle a session that leaves Connected, or newly becomes Disconnected, while a group partition uses it. It requests metadata now, revives a Disconnected session (`ClientAgent.cpp:1105-1111`) and reconciles.
- There are no new timeout constants.
- The residual gaps are in MEDIUM-1.

### M2 (refused Unsubscribe): met for the stated rule

- A refused Unsubscribe is queued in `pendingUnsubs_` (`GroupConsumerHub.cpp:633-635`).
- It is re-offered on every duty pass (`Client.cpp:1526`, `GroupConsumerHub.cpp:639-651`).
- It is dropped when its epoch ends (`:627`).
- The premise "epoch ended = broker session gone" is broken by MAJOR-2, which ends an epoch while the broker session is still alive.

### M3 ((node session, subscriptionId) keying): met on the unicast path

- **Source capture.** `AeronTransport.cpp:463-487` keys each frame by the arrival image's `sourceIdentity`, cached per image.
- **Learning the source.** `ClientAgent.cpp:727-730` records a session's source from its ConnectResponse.
- **Resolution.** `ClientAgent.cpp:1081-1103` resolves (source, broker sessionId) to a session.
- **Consumers.** `Client.cpp:2316-2320` passes the source to both hubs. `SubscriberHub.cpp:425-436` adds the seed-only rule plus a topic/partition check.
- **Gaps.** See the arrival-source analysis and MAJOR-1/2.

### MEDIUM items from round 0

| Item | Status | Evidence |
|---|---|---|
| MEDIUM-1 (connect epoch) | Fixed, but see MAJOR-2 | `ClientAgent.cpp:727-730`, `GroupConsumerHub.cpp:707-710` |
| MEDIUM-2 (OffsetFetch retry) | Fixed | `GroupConsumerHub.cpp:879-904`; called from `Client.cpp:1794`. An errored entry leaves `offsetKnown=false`, so it is retried. |
| MEDIUM-3 (skip below position) | Fixed | `GroupConsumerHub.cpp:1162` |
| MEDIUM-4 (seed never retargeted) | Fixed, with a semantic cost | `Client.cpp:2071-2104`; see MEDIUM-3 below |
| MEDIUM-6 (free ports) | Fixed | `GroupRoutingRegressionTest.cpp:39-50` |
| MEDIUM-7 (join after connect) | Fixed | `Client.cpp:3392-3401`, `GroupConsumerHub.cpp:906-913` |

---

## The arrival-source question

**The assumption holds for the normal path.** All of a node's publications that carry Records or the ConnectResponse to a client share one send socket:

- **ConnectResponse publication.** Created by the gateway: `aeron_->addExclusivePublication(withSparseTerms(req.egressChannel), …)` at `ClientGateway.cpp:1356`.
- **Per-(session, worker) unicast Records publications.** Created by the **gateway**, not the worker: `aeron_->addExclusivePublication(sess->egressChannel, …)` at `ClientGateway.cpp:2686`. The worker only writes to them (`Worker.h:2400`, `s.egressPub`).
- **Same driver.** The gateway's `aeron_` is the node client (`BrokerNode.cpp:1700-1702`), so both kinds of publication live in the **node** driver. That holds in shard mode as well; in fused mode worker 0 also rides the node driver (`BrokerNode.cpp:3195-3198`).
- **Same socket.** Both use the same UDP channel. `sparse` and term length are publication parameters, not part of the endpoint's canonical form, so the Aeron driver reuses one SendChannelEndpoint, and the client sees one `sourceIdentity`.

**Each worker driver does send from its own socket.** In shard mode every worker w≥1 has its own invoker driver (`BrokerNode.cpp:3199-3214`). Two kinds of publication use it:

1. **`SessionAckSink`** (`SessionAckSink.h:960-966`): produce acks on the worker's own driver. These have a different source port, but they are ProduceResponses, which `recordsSource` never consults. They are unaffected.
2. **`WorkerEgress`** (`BrokerNode.cpp:3296-3297`, `WorkerEgress.h:70`): `aeron:udp?control-mode=dynamic|control=<host>:<ctrlBase+w>`. This is a dynamic-MDC publication bound to its own control endpoint, so its source differs from the session publication even in legacy, non-shard per-worker egress.

**Records do travel on (2).**
- `Worker.h:2400-2401`: `if (!egressPub && workerEgress_) egressPub = workerEgress_->pub();`.
- `s.egressPub` is null on the gateway's designed fallbacks:
  - no egress channel (`ClientGateway.cpp:2662-2665`);
  - the publication add threw (`:2688-2697`);
  - the resolve did not answer within `connectResolveTimeoutNs` (default 5 s, `:2735-2740`, "falling back to MDC").
- The client is subscribed to those MDC channels (`Client.cpp:3729-3739`).

**What an "unknown source" frame does** (`ClientAgent.cpp:1081-1103`):
- No session has learned that source, so `recordsSource` falls back to the broker session id alone.
- **If that id is unique among the client's Connected sessions**, the frame is attributed correctly and nothing breaks.
- **If it collides**, `recordsSource` returns `kUnknownSource`. Broker session ids are per-node counters that start at 0 (`SessionTable.h:86,153`), so a collision between the seed's id and the node-1 session's id is likely in DR.
  - For a partition on a leader node, `GroupConsumerHub.cpp:1148-1152` **drops every batch**. This happens before any gap or contiguity handling, so nothing re-subscribes, nothing asks for metadata and nothing logs. The only trace is the `droppedBatches_` counter.
  - The result is a **silent, permanent stall** of that partition until the next rebalance. Nothing is misdelivered. (MAJOR-1.)
- **MDC is a broadcast**, so other clients' Records also arrive. A foreign frame whose broker sessionId equals one of ours by chance is attributed to that session.
  - SubscriberHub is still protected by subscriptionId plus topic/partition.
  - The group hub is protected by subscriptionId plus contiguity.
  - This is pre-existing MDC behaviour and is narrower than on main (LOW).

---

## CRITICAL

### CRITICAL-1. Every group partition stalls permanently at its first transaction control marker (and at aborted or compacted offsets)

**Where:**
- `GroupConsumerHub.cpp:1163-1180`: `rv.baseOffset > pos` counts as a gap. Only the first batch of a fresh `resetPending` subscription is exempt.
- `:850-877` `onGap_`: the first gap re-subscribes from `pos`. A second gap at the same `pos` parks the partition and prints "records … are not readable (gap) … parked".
- **Broker:**
  - `Worker.h:2472-2476`: a control marker is "never delivered". The cursor jumps past the marker's offset.
  - `Worker.h:2480-2491`: read_committed skips aborted transactional batches.
  - The gateway's inline push path does the same.

**Scenario** (`test_tx_read_committed.py:86-95`, the `cons_ru` leg, which this branch will fail):
1. A committed transaction writes offsets 0..7, and its COMMIT marker lands at offset 8. An aborted transaction writes 9..16, and its ABORT marker lands at 17.
2. A group consumer delivers 0..7, so `pos = 8`. The next pushed batch has `baseOffset = 9` (the marker at 8 is skipped by the broker), which is a "gap".
3. `onGap_` re-subscribes from 8. On the seed it also force-unsubscribes the coordinator's group subscription. The broker skips 8 again and pushes 9, which is a second gap at `pos == 8`, so the partition is parked with a misleading "retention" error.
4. Every metadata refresh unparks the partition and the same thing happens again. The partition never advances.

**Impact.**
- Any topic written transactionally becomes unconsumable by a group past the first COMMIT or ABORT marker, for both isolation levels, because markers are skipped unconditionally.
- That covers EOS consume-transform-produce, and `test_tx_eos.py`, `test_tx_read_committed.py` and `test_tx_atomicity.py` with group consumers.
- Compacted topics, which have offset holes, have the same problem.
- Main delivered these correctly (`newPos > pos`).
- No test covers it: every fixture pushes dense offsets.

**Fix.** The ruling needs one refinement: "a gap from the **current, bound** subscription instance is the broker's skip and advances the position". The broker never skips silently otherwise; on an undeliverable batch it parks *at* the batch (`Worker.h:2512-2523`). Only gaps from a subscription whose start the client does not control are suspect, which means the coordinator's group subscription before its frontier is known.

Concretely:
- From a bound subscription that the client created at `pos` (`selfOwned`, or a carrier whose group subscription is known to start at `pos`), accept `baseOffset > pos` and set `pos = end`.
- Keep the gap-to-replace rule only for the coordinator's group subscription when records were dropped for that partition before the carrier (see MAJOR-3).
- Add a `*Regression*` test: a committed transaction, then a further batch past its marker offset, consumed by a group on both a leader-node partition and a seed partition.
- Alternatively, have the broker carry the cursor (`nextOffset` before the batch) in Records so that the client can tell a skip from a stale stream. That is a protocol change; ticket it.

---

## MAJOR

### MAJOR-1. Records that reach the client through the broker's MDC fallback stall a leader-node partition silently and permanently

**Where:** `ClientAgent.cpp:1081-1103` (id-only fallback, `kUnknownSource` on a collision), and `GroupConsumerHub.cpp:1148-1152`, where an unknown source is accepted only for seed partitions. The broker side is described in the arrival-source section above.

**Scenario.**
1. Under DR driver starvation (the fused node driver is slow, a known failure mode on this project), the per-(session, worker) record publication for the member's node-1 session does not resolve within 5 s. The gateway logs "falling back to MDC".
2. Node 1's worker w pushes p1 from its WorkerEgress socket. No session has learned that source.
3. The member's seed broker id equals its node-1 broker id (both are counters from 0), so the source resolves to `kUnknownSource`.
4. Every p1 batch is dropped. Nothing re-subscribes and nothing logs, so p1 is never consumed in this generation and the member keeps heartbeating.

**Why it is new.** On main, MDC frames matched by subscriptionId and were consumed. The report's concern 1 ("would stall rather than misdeliver; the DR run will show it") understates this: the DR run shows it only if the fallback fires.

**Fix**, any one of:
- Tag each transport subscription slot with the node it was opened for, and resolve images on that slot to that node's session. The client already knows `p.leaderId` when it calls `addEgressSubscription` (`Client.cpp:3729-3739`).
- Make the unknown-source drop for a routed partition an event: count it, and after the bound subscription has been silent while unknown-source batches arrive, re-subscribe or request metadata.
- Broker side: never fall back to MDC for a session that has a unicast egress channel. Answer the Subscribe with a retriable error instead.

Add a `*Regression*` test in which the fake node sends Records from a second socket and the broker ids collide.

### MAJOR-2. A ConnectResponse that the SessionMachine ignored still bumps the connect epoch and overwrites the session's source, and stale ones land on the seed

**Where:**
- `ClientAgent.cpp:727-730`: `if (opt->code == 0 && s.machine.state() == Connected) { ++s.connectEpoch; s.sourceKey = currentSourceKey(); }`.
- This runs *after* `onConnectResponse`, which returns early without doing anything when the state is not Connecting or the correlationId does not match (`SessionMachine.h:259-262`).
- `sessionForConnectCorrId` (`ClientAgent.cpp:939-950`) falls back to the **seed** for any correlationId that no Connecting session holds.

**Scenario.**
1. The node-1 session's Connect times out after 5 s; the broker's own ConnectResponse retry runs for up to `connectResolveTimeoutNs`, also 5 s. The session re-sends with a new correlationId.
2. The late OK for the *old* correlationId arrives. It matches no Connecting session, so it is dispatched to the seed. The seed is Connected, so its machine ignores the frame, but **the seed's epoch is bumped and its `sourceKey` is set to node 1's source**.
3. **Seed epoch bump.** Every seed `FetchSub` is considered dead:
   - Subscriptions the client owns on the seed (gap replacements) are retired, but `unsubscribe_` refuses because the epoch no longer matches (`GroupConsumerHub.cpp:627`). They are orphaned on a **live** broker session and push for that session's lifetime. That violates M2's "until sent or its session is gone".
   - Carriers are re-sent for every coordinator group subscription.
4. **Corrupted seed source.**
   - Frames from node 0 match no session's source and fall back to id-only matching.
   - Frames from node 1 see two sessions on that source. If the seed's id equals node 1's id, the result is ambiguous and resolves to `kUnknownSource`. Every node-1 partition then stalls (as in MAJOR-1), and SubscriberHub reverts to id plus topic/partition matching for node-1 frames.
5. A duplicate or late OK for the seed's own earlier correlationId does step 3 without any real reconnect.

**Fix:** bump the epoch and record the source only when this frame moved the machine from Connecting to Connected. For example, have `onConnectResponse` return whether it accepted the frame, or compare the state and session id before and after. Add a regression test that injects a stale-correlationId ConnectResponse OK while all sessions are Connected and asserts that both epochs and `sourceKey`s are unchanged.

### MAJOR-3. Coordinator-led partitions drop every record pushed before the OffsetFetch answer, then stall until new data is produced

**Where:**
- **Broker:** at SyncGroup, `GroupCoordinationService.cpp:478-545` creates the member's group subscriptions, and they push immediately from the committed offset. The comment at `:478-481` says so. Before the carrier, the worker stops only at aborted transactional batches (`Worker.h:2484-2489`).
- **Client:** until the OffsetFetch answer arrives, the seed `FetchSub` has `epoch == 0` and the position is at the sentinel, so `GroupConsumerHub.cpp:1149` and `:1160` drop every batch. The carrier is sent only after `offsetKnown` (`:715`).
- Recovery depends on the gap rule (`:1163-1180`), and that needs a *later* batch to arrive.

**Scenario** (`test_tx_read_committed` / `test_tx_eos` shape: produce, then a fresh group consumes):
1. The member syncs. The coordinator, which leads the partition, pushes all N committed records at once.
2. With the record publication already resolved (any second generation, or a client that also produced), the records reach the client before the SyncGroupResponse, OffsetFetch, OffsetFetchResponse round trip completes. All N are dropped.
3. The OffsetFetch answer sets `pos = C`, and the carrier binds to the group subscription, whose frontier is already at the log end.
4. Nothing more is produced, so there is no next batch, no gap and no re-subscribe. The consumer sees nothing until its timeout.

**Comparison with concern 2.** The report's concern 2 ("about once per generation, at the cost of one Unsubscribe+Subscribe") only holds under continuous production. Nothing is lost, because nothing is committed past C, but the stall is silent and unbounded on a quiescent partition.

**Fix:** when a seed partition drops batches before its carrier (sentinel position or `epoch == 0`), mark it. When the OffsetFetch answer arrives, replace the coordinator's group subscription eagerly: unsubscribe the group subscription id observed in the dropped frames, then send a plain Subscribe that the client owns, from `pos`. Do not wait for a gap. Alternatively, ticket the broker-side fix: create the group subscription at the member's OffsetFetch, or rewind it on the carrier. Add a regression test in which the fake coordinator pushes before answering OffsetFetch and then goes quiet.

---

## MEDIUM

### MEDIUM-1. M1 residuals: one-shot metadata, and a session that was already Disconnected is never revived

**Where:** `Client.cpp:3261-3292`.

- **(a) One-shot metadata.** "Lost" is a single edge. If the one metadata answer still names the dead node (the client's staleness detection beat the cluster's election), the default config only asks again after the resume and connect budget ends in Disconnected (about 15 s). With `reconnectForeverAfterLive=true` (`ShardConfig.h:147`) the session cycles through Backoff, never becomes Disconnected and never raises another event, so p1 stalls.
  - **Fix:** a metadata answer whose leader for an assigned partition has a session that is not Connected re-arms `scheduleMetadataRefresh()`. That is still event-driven and backoff-paced. Alternatively, treat each failed connect attempt of a session the group uses as an event.
- **(b) Already-Disconnected session.** A node session that became terminally Disconnected *before* the group used it (opened by producer routing, or by an earlier assignment) is not "newly" Disconnected, so it is never revived. When p1 is later assigned to that node, `reconcileImpl_` waits at `ep == 0` forever (`GroupConsumerHub.cpp:718`).
  - **Fix:** in the watcher, revive any Disconnected session that `fetchUses(i)` reports, not only newly Disconnected ones. Or revive it from `reconcileImpl_` when the target session is Disconnected.

### MEDIUM-2. The reset-policy exemption loses the head of the log when the first batch arrives before the SubscribeResponse

**Where:** `GroupConsumerHub.cpp:1168-1172`. The exemption requires `fs->subscriptionId != 0`. A pre-bind gap is dropped ("wait for its bind").

**Scenario.**
1. A fresh group (nothing committed) has `resetPending` set and `pos = 0`. Retention has moved the log start to L > 0.
2. The node-1 Subscribe from 0 is clamped by the broker to L (`ClientGateway.cpp:2961-2966`).
3. Batch L arrives on the worker publication before the SubscribeResponse, which travels on the session publication with no ordering between them, as the report itself notes. It is a gap and pre-bind, so it is dropped.
4. After the bind, batch L+n arrives as a gap. The exemption fires and sets `pos = L+n`. Records L..L+n-1 are skipped, and the next auto-commit makes the loss permanent.

**Fix:** on a pre-bind gap while `resetPending` is set, accept it from any non-retired id of the session episode (the same rule as pre-bind contiguous acceptance), or re-subscribe from the log start once bound.

### MEDIUM-3. MEDIUM-4's behaviour change: plain Subscribes on a partition the seed does not lead now fail permanently, the change is undocumented, and the comment overclaims

**Where:** `Client.cpp:2092-2104`.

- **Plain Subscribes can never succeed.** A plain Subscribe always goes to the seed (`SubscriberHub::sendSubscribeFrame_` uses `directSend_`, which is the seed's default publication). While a group is active, a Subscribe for a partition the seed does not lead is failed and can never succeed, however often the app retries.
- **The comment is wrong for transactional ops.** "fresh metadata routes the retry" is false for plain Subscribes. "A txn request … fail THAT op" is also false: a Redirect for a non-produce, non-subscribe op hits `retryRedirectedProduce` (a no-op for an unknown correlationId) and hangs until its own timeout.
- **No reason is reported.** The handle only sees `finished()`.
- **Nothing documents it.** The public API (`Client.h:568-591`) is silent.

**Acceptable as the minimum guard**, but before merge it needs:
- documentation on `Client::subscribe`;
- a distinct status or reason on the handle;
- a correct comment.

Also ticket per-partition routing of plain Subscribes, which is the real fix.

### MEDIUM-4. Replacing the coordinator's group subscription does not update the broker's group bookkeeping: carriers fall through to plain subscriptions that are never unsubscribed

**Where:**
- **Client:** `GroupConsumerHub.cpp:873-875`. `onGap_` force-unsubscribes the coordinator's group subscription and replaces it with one the client owns.
- **Broker:**
  - `handleUnsubscribe` (`ClientGateway.cpp:3056-3075`) erases the subscription from `subscriptions_` only.
  - `groupMemberSubs_` keeps the id, so Fix #4 (`GroupCoordinationService.cpp:483-500`) sees an unchanged count and does not recreate it at the next SyncGroup.
  - The carrier lookup (`ClientGateway.cpp:2987-3011`) skips the missing id and **falls through to step 6, a new plain subscription**.

**Scenario.**
1. A gap replacement happens in generation g.
2. A rebalance to g+1 has the same partition count.
3. The seed carrier creates a plain subscription. The client binds it as `selfOwned=false` (`:731`).
4. On revoke, `dropFetch_` does not unsubscribe it (not `selfOwned`), and the broker's `revokeGroupSubscriptions` does not know about it. It is an orphan for the session's lifetime: wasted pushing, filtered by the retired list.

**Related race.** If the force Unsubscribe is refused (and goes to `pendingUnsubs_`) but the replacement Subscribe is accepted, the broker treats the Subscribe as a carrier and returns the live group subscription's id. The retried Unsubscribe then deletes the subscription the client just bound. The next batch from it gaps at `gapAt`, the partition is parked, and the log shows the false "retention" error.

**Fix:**
- Client: send the replacement only after the Unsubscribe is accepted. Treat a carrier response that returns an id other than a known group subscription as `selfOwned`.
- Broker ticket: `handleUnsubscribe` must also prune `groupMemberSubs_`, and session-check it (this is already ticketed).

### MEDIUM-5. `retired_` grows without bound within a session episode and is scanned on the record path before the bind

**Where:**
- `GroupConsumerHub.cpp:657` and `:813` push entries.
- `pruneFetch_` (`:675-691`) removes an entry only when its epoch ends.
- `isRetired_` (`:625-631` area) is a linear scan, called per batch pre-bind (`:1151`).

**Scenario:** a long-lived member with 100 partitions that rebalances every few minutes adds 100 entries per rebalance, indefinitely. After a day that is tens of thousands of entries, scanned for every pre-bind batch.

**Fix:** bound it.
- Broker subscription ids are monotonic per process, so for each (session, epoch) the client only needs the retired ids above the oldest id that may still have frames in flight.
- Or keep a small ring per partition (the review's original suggestion), cleared when the partition is bound.

---

## LOW

1. **`currentSourceKey` scans `imageSources_` linearly on every Records frame.** The list holds up to 4096 entries and is never pruned for images that have gone away (`AeronTransport.cpp:463-487`). A one-entry "last image" cache would make the steady state one compare.
2. **`retryFetchUnsubscribes` allocates on every duty pass during back-pressure.** `due.swap(pendingUnsubs_)` leaves `pendingUnsubs_` without capacity, so each re-push allocates (`GroupConsumerHub.cpp:641-643`). Keep two vectors and swap them.
3. **An Unsubscribe that returns `Fatal` is silently dropped** (`:633`). Only `Retry` is queued. The retired list covers the records, but log it.
4. **`watchGroupSessions_` ignores sessions with index > 64** (`Client.cpp:3265`).
5. **Two group consumers in different groups assigned the same (topic, partition) in one client.** `onRecords` returns on the first impl that has the partition assigned (`GroupConsumerHub.cpp:1146-1152`), so the second group never gets its records. This is pre-existing (main `break`s the same way), but the identity check now makes a `continue` correct. It is a cheap fix.
6. **`sourceIds_` grows by one string per distinct source and is never pruned.** Node restarts produce new ephemeral ports. This is minor.

---

## Tests

- **Quality.** The new tests are deterministic, use condition-driven pumps with no sleeps and use free ports. `InvokerGateway` models a distinct source per node correctly for the unicast case.
- **Coverage still missing:**
  1. a transaction marker, aborted batch or compaction hole (CRITICAL-1);
  2. Records from a second socket of the same node with colliding broker ids (MAJOR-1);
  3. a stale or duplicate ConnectResponse (MAJOR-2);
  4. a coordinator group subscription that pushes before the OffsetFetch answer and then goes quiet (MAJOR-3);
  5. a stale first metadata answer, and a session that was already Disconnected (MEDIUM-1);
  6. a pre-bind gap with `resetPending` (MEDIUM-2).
- **DR.** Re-run `test_cg_rebalance`, `test_cg_atleastonce` and `test_cg_failover`, plus `test_tx_read_committed` and `test_tx_eos`. CRITICAL-1 predicts that the `cons_ru` leg of `test_tx_read_committed_*` fails on this branch.

## Required before merge

- CRITICAL-1 and MAJOR-1/2/3, each with a `*Regression*` test.
- MEDIUM-1, MEDIUM-2 and MEDIUM-4 (client side).
- The documentation, reason and comment for MEDIUM-3. Tickets for MEDIUM-5 and the broker items:
  - `handleUnsubscribe` must prune `groupMemberSubs_`;
  - no MDC fallback for unicast sessions;
  - create group subscriptions at OffsetFetch (or rewind them on the carrier);
  - optionally, a skip or cursor field in Records.
