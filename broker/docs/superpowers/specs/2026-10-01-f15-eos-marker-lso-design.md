# F15: EOS transaction-marker and LSO correctness (design note, revision 3)

**Date:** 2026-10-01
**Status:** DESIGN NOTE, revision 3 (revision 2 + the five blocking items of design re-review #2, verdict Approve-with-changes). No code changes. This note gates F15 (ruling R17).
**Base:** rune-broker `feat/f15-eos-correctness` in `~/Projects/rune-broker-wt10`: `b04f871` plus the S0 RED tests `63b5346`. rune-protocol @ `56df0c7`.
**Revision 2 answers:** design-gate review #1, verdict REWORK (`.superpowers/sdd/2026-09-30-self-contained-worker-shards-broker/f15-design-review-1.md`), and the S0 report (`…/f15-s0-report.md`, 25 RED tests plus the new defect **N4**).
**Motivating review:** Task 10 round-5 re-review (C1, I1, I3, I4, m1–m5). History is in `…/task-10-report.md` (rounds 3–5).
**Related:** `2026-09-30-self-contained-worker-shards-design.md` §3.4 (this note retires its "post-ack" LSO notification), `2026-09-16-rune-milestone-c-b-exactly-once-transactions-design.md`, client ticket F14.
**Binding laws:**
- shared-nothing: one owner thread per piece of state, and rings as the only cross-thread channel. The only exceptions are the log cleaner/compaction channel and ring internals. No atomics and no mutexes anywhere else;
- no `std::future`/`promise`;
- time-independent liveness;
- no allocation, virtual calls or `std::function` on hot paths;
- a failing `*Regression*` test before every fix;
- mirror Kafka and Aeron.

---

## Changes from revision 2 (re-review #2)

| Item | Revision 3 | Where |
|---|---|---|
| **I-A** | Legacy acks=all acks are parked on the **worker in every mode** (`pendingAcksAll_`), so the §3.2 per-pass validation covers the default config. The gateway's `pendingAcks_`/`deferAck` is deleted for worker-owned partitions. New `TxnRoleGateRegression.LegacyAcksAllDemotionFailsNotLeader`. | §3.2, §8 S2 |
| **I-B** | **The gateway RC mirror is deleted.** Every production mode already reads worker-owned logs through the `PushRead` op on the owning worker (`executePushReadWire_`); W now clamps that read to `min(op.hwBound, rc.tracker.lso(liveHw))` and strips aborted and control batches while copying. Both egress paths are decided on W (L5). `SET_RC_INTEREST`, the `RC_*` return kinds, interest ref-counting and the mirror proof are gone; no extra return traffic. | §2.4, §7, §9, §12 |
| **I-C** | The build cursor is clamped to `logStartOffset()` every pass, and an empty view below `target` skips to the next segment base, so retention during a build cannot stall it. New `TxnRcRegression.RetentionDuringBuildCompletes`. | §2.3 |
| **I-D** | `TxnReturn` takes template id **6** (1–5 are taken; 4 is `FlushCompletion`). Dropping WorkerOp field 49 changes WorkerOp's layout; that is accepted (Q5, both sides built together in-process), so the protocol commit is not purely additive. | §7 |
| **I-E** | The S4 retarget covers **five** tests (adds `ZombieBatchAfterAbortMarkerIsFencedShardIngress`). The mirror check is replaced by a behavioural check: the rig adds a gateway-pushed read_committed subscription (a declared scenario addition) and asserts it receives nothing from offset 0. On base the retargeted tests fail on the precondition `ASSERT` (stub returns 0); that is the documented RED. | §8 S4 |
| Minors | Build reset guarded by `status != Unbuilt`; tail list holds markers only; epoch bump with unchanged log generation keeps Ready; truncation point stored next to `logGeneration_`; L2 names the `OwnershipAck` bypass; startup check of ret_ max message length; Legacy op-drain pause noted for the DR matrix; coordinator routing invariant test; §2.6 Legacy internal-partition text; worker-push RC pointer re-resolved on SetOwnership; `BM_RcBuildStep` with a cold page cache. | inline |

## Changes from revision 1

| Review item | Revision 2 | Where |
|---|---|---|
| **K1**, **K2** | **Option B.** read_committed state (LsoTracker + AbortedTxnIndex) for worker-owned partitions lives in the **owning worker**, which already owns the appends, the markers, the HW and the per-worker push. `TxnBatchAppended` facts, the `TxnStateSnapshot` stream and the gateway's per-partition LSO for worker-owned partitions are **deleted**. (Revision 3: the gateway push path is clamped on W inside `PushRead`; the revision-2 passive mirror is deleted.) | §2 |
| **I2**, **I5** | The worker builds its RC state **incrementally, starting only once `leaderComplete_` holds**: a bounded number of bytes per pass, zero-copy `readView`, and a replay of the tail appended during the build. read_committed is held until the build is Ready. | §2.3 |
| **K3** | Markers are routed to the owning worker under **exactly** the produce-routing predicate, in every mode, through one shared function. `appendControlBatch` keeps `append()`'s flush request. | §2.6 |
| **K4** | The coordinator is **loaded** only when the txn-state replica is Leader, `leaderComplete_` holds and HW == LEO. It loads to HW. coordinatorEpoch is the txn-state replica's leader epoch. Until it is loaded, every txn API answers `NotCoordinator` and no timeouts run. Every "load to LEO" is gone. | §5.1 |
| **I1** | Every parked waiter re-checks role, epoch and log generation **on every pass**. No BecomeFollower op hooks. | §3.2 |
| **I3** | The PSM rebuild applies control batches as **epoch raises** (with no sequence). | §2.5 |
| **I4** | Timeout PrepareAbort and fence PrepareAbort are **persisted, and committed, before dispatch**: one decision gate covers every transition into Prepare\*. | §5.3 |
| **I6** | The worker `psm_` check runs in `executeProduceWire_` in **every mode**. The gateway PSM mirror is gone for worker-owned partitions. The §2.3 text from revision 1 is fixed. | §2.5 |
| **I7** | The pid **block ceiling** is persisted in `__transaction_state` (Kafka ProducerIdManager). | §5.6 |
| **I8** | `enqueueReturn_` no longer drops in Legacy mode or on a `tryClaim` exception. There is one FIFO in every mode. | §6 |
| **N4** | `LsoTracker::clear()` also clears `byFirstOffset_`, and the structure is replaced by a monotone flat FIFO. | §2.2 |
| Minors | All seven are applied (§2.6 flush, §3.2 epoch bump, §4.3 incarnation, §5.3 pre-transition copy, §6 comment fix, §7 constexpr CRC; TxnBatchAppended is moot). | inline |
| RED caveats | S2 debug-abort/NDEBUG note; the TSan RED is replaced by a behavioural and structural pair; the S6 late-duplicate RED uses the unknown-token path. | §8 |
| Perf | ProduceResult is unchanged: the new returns use a separate SBE template. The non-txn path is O(1), allocation-free and has no new ring traffic. The build scan is bounded per pass. | §9 |

---

## 0. Summary of decisions

| # | Defect | Decision |
|---|---|---|
| C1 | LSO "batch appended" is registered when the ack drains, not at append. Deferred acks are overtaken by the marker, and session-gone results are dropped. | **Option B.** The owning worker W updates its own `RcState` (LsoTracker + AbortedTxnIndex) **in the same call as the append**, for data batches and for markers. W also computes the read_committed bound and serves it. There is no cross-thread fact stream for worker-owned partitions. |
| K2 | The gateway HW (atomic snapshot) can overtake a queued txn fact. | Closed by Option B: W computes `LSO = min(firstOpen, HW)` from its own live state. The gateway push path reads through W's `PushRead`, which W clamps to its own LSO (§2.4). |
| C1+ | Zombie data arriving *after* an ABORT marker opens a txn that never closes. | The **partition producer epoch is fenced by the marker**, in W's `psm_` only: raised on receipt of the marker op, and made durable by the control batch itself (the PSM rebuild applies it, I3). |
| N2 | A marker appended through `PartitionReplica::append` is wrapped as a plain data batch (pid −1, no CONTROL bit). | `PartitionReplica::appendControlBatch`: restamps the prebuilt control batch in place, runs the role and min-ISR gates, keeps the flush request. |
| N3 | `ReadCommittedView::rebuild` treats every `pid ≥ 0` batch as transactional. | Require the TRANSACTIONAL attribute (0x10). |
| **N4** | `LsoTracker::clear()` leaves `byFirstOffset_` stale, so a stale first offset pins the LSO (test: LSO 10 instead of 50). | `clear()` clears both indexes. The tracker becomes a pid → slot table plus a monotone first-offset FIFO, and `clear()` resets both in one place (§2.2). |
| K3 | In the default (Legacy) config, produces go to the worker but markers are appended inline on the control thread: a write-write race, and the marker overtakes queued produces. | One predicate, `routesToWorker_(topicId)`, used by produce **and** marker routing in every mode (§2.6). |
| I1 | No role check before append on the worker. The gateway's `replica->role()` read is a race. | An authoritative role gate on W for produce (both paths) and markers. The gateway uses a control-owned **intended-role hint**: an absent hint means "route", and W decides. |
| N1 + I1(review) | Demotion, including **self-demotion** in `onFetchRequest`, does not fail parked acks. | Every parked waiter is re-validated **each pass** against `(role, logGeneration)`. A role loss, or a truncation below the waiter, fails it with `NotLeader`. An epoch bump while still leader continues waiting (§3.2). |
| I3 | Retriable marker failures count as landed. | Each (txn, partition) marker is a rich-state task, retried until **committed** (HW > markerOffset with no intervening truncation) or until the partition **incarnation** is gone. MinIsr parks on W and retries on an ISR change. Backoff is counted in cycles, never wall-clock. |
| K4 / I4 | No coordinator rebuild on failover. EndTxn is not idempotent. Reloading on the consensus-leader edge to a stale HW breaks atomicity. | The coordinator is **loaded** only when the txn-state replica is Leader, `leaderComplete_` holds and HW == LEO. It loads to **HW**, with coordinatorEpoch = that replica's leader epoch. Until loaded: `NotCoordinator`, no timeouts. Complete\* is the resting state, with an EndTxn idempotency table. Dispatch only after the Prepare\* record is committed, on **every** path to Prepare\* (EndTxn, timeout, fence). |
| I7 | pid block ceiling is never persisted, so idempotent pids are reissued after failover. | Persist the block ceiling in `__transaction_state`. Hand out pids only from a committed block. Reseed on load. |
| I6 / Q3 | Deleting the gateway PSM leaves gateway-received produces with no dedup or fence. | W `psm_` validates and updates in `executeProduceWire_` **and** in shard ingress, in every mode. The gateway PSM mirror is removed for every worker-owned partition (a superset of Q3). |
| m2 | An overflow fence hands out the new pid, but meta keeps the old pid, so the new pid gets InvalidPid. | `validateEpochAndPid` recognises `pendingNewProducerId/Epoch` while in Prepare\* and returns `kConcurrentTransactions`. |
| m3 | `awaiting_` is not cleared on loss of leadership. | On unload: answer deferred EndTxns `NotCoordinator`, drop all marker tasks, purge the gateway's queued markers for the old coordinatorEpoch. |
| m4 | Completion is never persisted. | `put(Complete*)` after `completeTransaction`, before the deferred EndTxn OK. |
| m5 / I8 | `retOverflow_` is unbounded and erases in O(n). The encode-error branch drops. `enqueueReturn_` drops in Legacy mode and on a `tryClaim` exception. | One fixed-capacity FIFO **in every mode**. The op drain pauses at a high-water mark. Any encode or claim exception is an invariant-violation halt. |
| m1 | Shard-ingress min-ISR ignores the runtime policy. | A `SetAdmissionPolicy` op, delivered with retry-until-delivered. The WorkerOp `minIsr` field is dropped (Q5). |

---

## 1. State ownership (who owns what)

| State | Owner thread | Written by | Read by others via |
|---|---|---|---|
| Partition log, `PartitionReplica` (role, epoch, ISR, HW, LEO, `leaderComplete_`, **`logGeneration_`**, **`leadershipGeneration_`** (new, S4)) | owning **worker W** | W only | nothing cross-thread, except the pre-existing cleaner/gateway HW snapshot (`gatewayHighWatermark`), which F15 **no longer uses for any read_committed decision**. |
| W `psm_` (idempotent dedup + **partition producer epoch**) | W | W | nothing |
| **W `RcState` per owned partition** (new): `LsoTracker`, `AbortedTxnIndex`, build state machine (§2.3), `(leaderEpoch, logGeneration, leadershipGeneration)` it was built for | W | W | the per-worker push and `executePushReadWire_` (same thread). Nothing is published cross-thread. |
| Parked acks (`pendingAcksAll_`, `durablePending_`), **parked markers** (`parkedMarkers_`, new), **marker commit waiters** (new) | W | W | returns on `ret_` |
| ~~Gateway RC mirror~~ (revision 2) | — | — | **deleted in revision 3** (I-B): the gateway holds no read_committed state for worker-owned partitions. |
| Bridge **intended role** per (tp, incarnation) (new hint map) | control | `ReplicationMetadataBridge` when it routes a role op | gateway (same thread) |
| Gateway `markersInFlight_`, `markerSendQueue_` (each entry tagged with coordinatorEpoch) | control | gateway | same thread |
| `TransactionCoordinator::txnMap_`, TCS **load state** (§5.1), **marker task table** (replaces `awaiting_`), **decision gate**, **pid block state** (§5.6), deferred EndTxn responses | control | TCS | same thread |
| `ReplicatedTxnStateStore` + `__transaction_state` replica (topicId −2) | **control** (verified: `BrokerNode.cpp` routes topicId < 0 replicas to the legacy transport and drives them from the control thread; not worker-owned) | TCS | same thread |

**Deleted:** `TxnBatchAppended` (and the reserved `PRODUCE_NOTIFY` slot), `TxnStateSnapshot`, gateway `partitionTxnStates_` for worker-owned partitions, gateway `lsoTracker.onProduceBatch` in `handleWorkerReturn_` (~ClientGateway.cpp:3158-3162) and in the produce path (~1811), the LSO block of `rebuildProducerStateFromLogs` (~489-517), `ClientGateway::notifyControlMarker` for worker-owned partitions, the gateway PSM mirror for worker-owned partitions.

**Gateway-owned partitions** (internal, topicId < 0, written on control): they keep the gateway inline path. Internal topics are written only by coordinators with pid −1, so they hold no transactional batches. S4 verifies this with a test (`TxnRcRegression.InternalPartitionsNeverCarryTxnBatches`), and `executePushReadWire_` bounds an internal partition by its HW only. Since it holds, the gateway `LsoTracker`/`AbortedTxnIndex` are **deleted outright**; there is no gateway-side read_committed state (the revision-2 RC mirror is deleted too, I-B). If it ever fails, the gateway keeps an `LsoTracker` for topicId < 0 only, fed inline on the control thread, which is a single thread with no race. **Schedule (controller ruling, S4 review m1):** S4 already removes every feed and read of the gateway state for worker-pool paths (every production mode); the remaining no-worker-pool inline rig path (unit rigs; the 8 `ReadCommittedPushTest` tests) is deleted in broker **T16 cleanup**, with those tests rewritten onto a worker pool.

All cross-thread traffic for F15 uses exactly two channel kinds:
- **op ring** (control → W): `OneToOneByteRing`, SPSC, FIFO, with control as its only producer.
- **return ring** `ret_` (W → control): SPSC, FIFO, fronted by W's local return FIFO (§6).

No new atomics, mutexes, futures or callbacks.

### 1.1 Ordering lemmas

- **L1 (single appender).** A user partition P has exactly one owning worker W for the lifetime of its incarnation. Evidence: F7 (`WorkerPool::assignFromRegistry` throws on a move, halting with 70) plus the incarnation ownership guard. **With K3 fixed, this holds in every mode**: the control thread never appends to a worker-owned log (§2.6). The only other writer is replication apply/reconcile, which also runs on W.
- **L2 (ordered emission, every mode).** Every return W emits, through `enqueueReturn_` or `enqueueOrderedReturn_` (which become one function, §6), reaches `ret_` in emission order:
  - it goes to `ret_` only if the local FIFO is empty, otherwise to its back;
  - the flush drains front-to-back and stops at the first refusal;
  - nothing is ever dropped (I8, m5).
  - The bypasses are `enqueueSignal_` for StorageFailure and for `OwnershipAck` (Worker.h ~1983). Neither carries F15 state; `OwnershipAck` is resolved by incarnation, so its reordering is harmless.
- **L3 (FIFO consumption).** Control drains each W's `ret_` sequentially and handles each return to completion. A partition's returns come from a single W (L1). Late returns from a removed incarnation are filtered by incarnation, as today.
- **L4 (control single thread).** Validating, enqueuing and dispatching on control are mutually atomic. One op ring per W is FIFO, so an op enqueued earlier by control is executed earlier by W.
- **L5 (RC state is W-local, new).** `RcState` for P is read and written only on W, in program order with P's appends, marker appends, truncations and HW changes. Every read_committed decision, for worker-pushed **and** gateway-pushed subscriptions (the latter through `PushRead`, §2.4), is made on W from that state and the live HW. **No cross-thread ordering argument is needed for either egress path.**

---

## 2. C1, K1, K2: worker-owned read_committed (Option B)

### 2.1 The worker updates RC state at append

W keeps `RcState rc` per owned replica, in the existing owned-partition slot (the same cache line group as `replica`/`log`, pre-allocated when ownership is applied). In the same function invocation as a successful append, before any return-producing statement:

1. **Data batch** (`executeProduceWire_` after `appendNoSyncRestamp`, ~Worker.h:1690; `handleWorkerIngressFrame_` after `handleProduceBatch` OK, ~3620):
   ```
   if ((attrs & TRANSACTIONAL) && !outcome.isDuplicate) [[unlikely]]
       rc.onTxnBatch(pid, baseOffset);       // insert only if pid not open
   ```
   - This cannot happen while the build is not Ready: `producerId ≥ 0` batches are refused until Ready (§2.3), so data-batch tail events do not exist.
   - Non-transactional batches pay one attribute-bit branch and nothing else.
   - The ack is unchanged: immediate, parked on fsync, or parked on HW. **The RC update never waits for the ack**, so it is independent of the ProduceResult and of the session (C1).
2. **Marker** (`executeAppendControlRecordWire_` after `appendControlBatch` OK):
   ```
   first = rc.tracker.onControlMarker(pid, markerOffset, commit);
   if (!commit && first >= 0) rc.aborted.onAbortMarker(pid, first, markerOffset);
   ```
   It then emits `ControlRecordAppended{token, base}` (ordered) and registers the commit waiter (§4). When the build is `Scanning`, the event goes to the tail list (§2.3).
3. **Truncation / foreign append** (follower truncation, leader-side `reconcileTruncation_`, OLE divergence, `applyReconcileResponse` appending old-term batches): these bump `PartitionReplica::logGeneration_` (new plain member, W-owned). W does not hook these sites. On the next pass, a changed `logGeneration` resets RC to `Unbuilt` (§2.3), which is the per-pass check style of I1. Alongside `logGeneration_`, the replica records `lastTruncationOffset_` (the offset the log was cut to), used by §3.2.

**Proof (batch covered by its abort).** Let B be a data batch of pid p and M an ABORT marker of p on P with `off(B) < off(M)`.
1. By L1, both are appended by W, and `a(B)` happens before `a(M)`.
2. RC is updated in the same invocation as each append (step 1, step 2), and RC is W-local (L5). So when M is applied, p's open entry holds `first ≤ off(B)`.
3. Then `onControlMarker` returns `first`, and `[first, off(M)]` covers B. ∎

The proof needs no lemma about rings, acks or sessions. Every case where the ack fails after the append (acks=all deadline, N1 `NotLeader`, StorageError fail-stop, session gone) leaves the RC update in place, because the batch *is* in the log. If a truncation later removes it, `logGeneration` changes and RC is rebuilt from the surviving log.

### 2.2 N4 and the compact tracker

`LsoTracker` keeps its public API (`onProduceBatch`, `onControlMarker`, `lso`, `openCount`, `firstOffsetOf`, `clear`), so the unit tests stay valid. Its storage changes from two `std::map`s, which allocate per insert:
- `open_`: a flat open-addressing table pid → slot index, pre-reserved to `rcOpenTxnCapacity` (config, default 1024 per partition);
- `fifo_`: a ring of `{firstOffset, pid, live}`, appended in offset order. Under L1, first offsets arrive monotonically, so the FIFO is sorted by construction;
- `onControlMarker`: marks the slot dead in O(1);
- `lso(hw)`: pops dead entries from the front (amortised O(1)), then returns `min(front.first, hw)`, or `hw` if the FIFO is empty;
- growth beyond capacity is a cold doubling, counted in `rcTrackerGrowths`. Steady state is allocation-free;
- **N4:** `clear()` resets `open_` **and** `fifo_` (head, tail, live count). It is one function, so the two indexes can never diverge again. The S0 test `LsoTrackerRegression.ClearDropsTheFirstOffsetIndex` pins this.

For the build replay only (§2.3), where first offsets can be presented out of order across the scan/tail boundary, `fifo_` is filled scan-first and tail-second, which keeps offset order (the tail lies wholly above the scan target).

`AbortedTxnIndex`:
- ranges are appended in marker order, which is the `lastOffset` order;
- `isAborted(pid, off)` is a binary search on `lastOffset ≥ off`, then a forward scan while `firstOffset ≤ off`. That is O(log R + k), where k is the number of overlapping aborted txns. Today it is O(R);
- `pruneBelow(logStartOffset)` runs on retention (cold);
- it is pre-reserved and grows only on the cold path.

### 2.3 I2 + I5: incremental build, starting at `leaderComplete_`

The RC state is a rich-state machine on W. Each state holds only its own variables:

```
Unbuilt                                   -- not serving read_committed
Scanning{cursor, target, epoch, logGen}   -- building [logStart, target) from the log
Ready{epoch, logGen}                      -- serving; append-time updates are live
```

Tail events recorded during `Scanning` live in a pre-reserved W-local list: `{pid, offset, commit}`. They are **markers only**: data batches with `producerId ≥ 0` are refused until Ready (produce gate below), and non-transactional batches carry no RC state. Events are recorded only in `Scanning`.

**Per-pass driver** (`rcAdvance_(slot)`, called for every owned replica in the existing per-replica loop, and once inline from the produce gate below):
1. If `status != Unbuilt` and (`role != Leader`, or `logGeneration` differs from the state's, **or `leadershipGeneration` differs from the state's**): → `Unbuilt`, and clear the tracker, the index (N4) and the tail list. **`leadershipGeneration` (S4, review m10)** is a W-owned plain counter on `PartitionReplica`, bumped on every Leader↔Follower transition (becomeLeader from a non-leader, becomeFollower, self-demotion in `onFetchRequest`) and never on an epoch bump while Leader. It closes a gap in the per-pass rule: BecomeFollower → a follower `ApplyReplicated` (appends, no truncation, so `logGeneration` is unchanged) → BecomeLeader can all run inside one op drain, after which `role == Leader` and `logGeneration` is unchanged while the log holds replicated batches RC never saw. The same check also runs at every serve and every gated produce (`revalidate`), since a role change can land between the per-pass driver and a read in the same pass. The `status != Unbuilt` guard means followers never pay the O(capacity) clear on every pass. This catches BecomeFollower, **self-demotion** in `onFetchRequest` (I1) and every truncation or reconcile append (I2), with no op hooks.
   - A leader-epoch bump **with an unchanged `logGeneration`** while still Leader keeps the state (`Ready` stays `Ready`, `Scanning` continues): the log is the same, so the RC state is still exact. The stored epoch is updated.
2. `Unbuilt`, and the replica is Leader with `isLeaderComplete()` (new accessor for `leaderComplete_`): → `Scanning{cursor=logStart, target=LEO, epoch, logGen}`.
   - Waiting for `leaderComplete_` means the deficient-leader reconcile has finished appending old-term batches before W scans (**I2**).
3. `Scanning` (**never blocks on disk** — S4 fix round 1, controller cold-cache bench: synchronous page faults on the worker cost 0.5–1 ms per step). The scan walks each segment's untouched read-only mapping sequentially by byte position (no index lookup and no header read before the gate), keeps `MADV_WILLNEED` read-ahead a few budgets in front of the cursor, asks a residency probe (`mincore`) how many bytes from the byte cursor are resident, and parses only whole batches inside that resident prefix and the budget (one batch larger than the budget is taken alone once wholly resident). A pass with nothing resident reads nothing and returns at once, so the per-pass cost is CPU-bounded; liveness is progress-driven (completed read-ahead), never time-driven. **Fix round 2 (re-review 1):** read-ahead is best-effort, so a cursor window that is not wholly resident is **re-requested on every pass** (RR1-1); the hint is issued **asynchronously** as `IORING_OP_FADVISE(WILLNEED)` on the segment fd through the worker's io_uring backend, **at most one in flight per building partition**, skipped for the pass when the SQ is full, and synchronous (`MADV_WILLNEED`, counted) only where io_uring is unavailable (RR1-4). Segment identity is the Log's install generation, never a pointer (N1). A replaced segment object (a compaction swap) restarts its byte walk at 0, skipping batches below the cursor. The steps below describe the same scan in offset terms: first `cursor = max(cursor, log->logStartOffset())` (**I-C**: retention runs on W between passes and may delete segments under the cursor; `readView` below the first segment returns `{}` forever, Log.cpp:496–497). Then read **at most `kRcScanBytesPerPass`** through `log->readView(cursor, kRcScanBytesPerPass)`. If the view is empty while `cursor < target`, set `cursor` to the next segment's base offset (a gap). Batches below the new `logStart` are gone, and so is any data they could hide, so skipping them is exact; aborted ranges with `last < logStart` are pruned anyway. That is zero-copy and allocates nothing. Feed each whole batch below `target` to the shared `RcBuilder::step` (N3: TRANSACTIONAL bit required; markers recognised only as real control batches, N2). Advance `cursor`.
   - When `cursor ≥ target`, replay the tail events in order, then → `Ready`, and clear the tail list.
   - **Liveness is time-independent:** `target` is fixed at entry, so the scan finishes in ⌈(target − logStart) bytes / kRcScanBytesPerPass⌉ passes regardless of the append rate. The tail list absorbs concurrent appends.
4. `Ready`: nothing to do. Append-time updates (§2.1) keep it exact.

`ReadCommittedView::rebuild(log, hw)` becomes a loop of `RcBuilder::step` over `log->readView`, so the unit tests exercise the same classification code the worker runs. It has no production caller for worker-owned partitions.

**Produce gate while not Ready (shared with §2.5):** a batch with `producerId ≥ 0` (idempotent or transactional) arriving while `rc.status != Ready` first calls `rcAdvance_` once inline. A fresh or small log completes in that single step. If the build is still not Ready, the batch is refused with `NotLeader`: no append, retriable. This matches Kafka, where a partition does not serve as leader until its producer state is loaded. `producerId < 0` batches are unaffected.

**read_committed while not Ready:** read_committed subscriptions get no data on either egress path: W-pushed ones are skipped, and a gateway `PushRead` for a read_committed sub is **held on W while the partition is Leader** (building) and answered once the state is Ready — a non-leader partition answers at once (`notReady`), including reads already held when leadership is lost (RR1-2) (S4 fix round 1, review I-2 / addendum R-3 "or RC readiness": one outstanding read per subscription, no gateway re-issue per cycle; each held read owes one return and is counted in the G-A6 reservation; ownership loss answers it "unanswered" so the gateway re-issues to the new owner; beyond the hold capacity W answers `notReady` and the gateway backs off 1, 2, 4 … 1024 push cycles). This holds data back; it never exposes it.

### 2.4 Serving read_committed

**Worker push (`pushOwnedSubscriptions_`, shard mode and any config with per-worker egress):**
- `s.isolation == ReadCommitted` and `rc.status != Ready` → skip this subscription for this pass.
- `bound = (s.isolation == ReadCommitted) ? rc.tracker.lso(hw) : hw`, where `hw = rep->highWatermark()` is W's live value. **This closes K2**, because the LSO and the HW are read on the same thread as the appends.
- Per batch: control batches are skipped, as today. **Only if `attrs & TRANSACTIONAL`** and read_committed: `rc.aborted.isAborted(pid, baseOffset)` → skip and advance the cursor. Non-transactional batches pay nothing new.
- The comment at Worker.h ~1395 ("read_committed NOT yet applied here") is replaced.

**Gateway push path (default config, `perWorkerEgressEnabled = false`; also the S0 rig):** whenever `workerPool_` is set, which is every production mode, the gateway does not read worker-owned logs itself. It enqueues a `PushRead` op (ClientGateway.cpp ~2597) carrying `nextOffset`, `hwBound` (from its HW snapshot) and `isolation`, and W reads the log in `executePushReadWire_` (Worker.h ~1741). The gateway only encodes the returned chunk. Revision 3 puts the read_committed decision there:
- `isolation == ReadCommitted` and `rc.status != Ready` → return an empty chunk, `newNextOffset = nextOffset`.
- Otherwise `bound = min(op.hwBound, rc.tracker.lso(liveHw))` for read_committed (and `min(op.hwBound, liveHw)` for read_uncommitted, which is only ever a tightening). The read is limited to batches whose `lastOffset < bound`.
- While copying the chunk, W **strips** control batches and, for read_committed, transactional batches with `rc.aborted.isAborted(pid, baseOffset)`. `newNextOffset` is set past the last batch examined (stripped or copied), so the gateway's cursor advances over filtered batches and never re-reads them.
- Non-transactional batches pay one attribute-bit check per batch inside the copy loop the read already performs.
- The gateway deletes its own read_committed bound computation for worker-owned partitions (the `partitionTxnStates_` / `lsoTracker.lso(gatewayHw)` block before ~2574). Its `sub.nextOffset >= bound` skip uses only the HW snapshot, which is a scheduling hint: W's clamp is authoritative.
- **The gateway never makes a read_committed decision.** `gatewayHighWatermark()` only ever loosens the *request*; W tightens the *answer*.

**Proof (neither egress path pushes aborted or open data).** Every read_committed byte that reaches a client was selected by W, in `pushOwnedSubscriptions_` or `executePushReadWire_`, from `rc` and the live HW, in program order with P's appends (L1, L5). By the §2.1 proof, at that moment an open txn's first offset bounds the LSO and every aborted batch below the LSO is inside a recorded range. So W selects neither. ∎

The ring-ordering lemmas L2/L3 are no longer needed for read_committed at all. The 512 KiB chunk bound is unchanged, so the `PushReadResult` size is unchanged.

**Worker-push subscription → RC pointer:** each worker-push subscription caches a pointer to its partition's `RcState`, re-resolved on SetOwnership in `rebuildFlushSlots_`, like the existing replica pointer.

### 2.5 C1+, I3, I6: the worker `psm_` is the sole producer-state authority

- **Validation in every mode (I6):** `executeProduceWire_` (gateway-received produces, Legacy and shard) runs the same `psm_.validate` → append → `psm_.update` sequence as `handleWorkerIngressFrame_`. Duplicates answer with the original offsets; fenced epochs answer `InvalidProducerEpoch (17)`.
  - The gateway PSM mirror (`producerStateManager_`) and its validate (~ClientGateway.cpp:1653), update (~3153) and log-scan rebuild (~485) are **removed for every worker-owned partition in every mode**. This honours Q3 and goes beyond it, because two authorities for one pid are a check-then-act race even in Legacy.
- **Marker fence (C1+):** on *receipt* of `AppendControlRecord`, before the role gate, the MinIsr park or the append, W calls `psm_.fenceEpoch(tp, pid, markerEpoch)` (raise only).
  - By L4, a gateway-routed produce enqueued before the marker op is validated first, lands before M, and is covered by the abort range.
  - One enqueued after it is fenced.
  - A shard-ingress produce polled before the marker op lands before M, so it is covered; one polled after is fenced.
  - **Fixed §2.3 text from revision 1:** there is no gateway-side fence. Revision 1's "gateway mirror fences at dispatch" is withdrawn.
- **Durable fence (I3):** `ProducerStateManager::rebuildFromLog` (and the RC build, which now feeds `psm_` from the same `RcBuilder::step` pass) applies every control batch as `fenceEpoch(pid, batch.producerEpoch)`, with no sequence update. Today it skips them (PSM.cpp ~553/573). An old-epoch zombie is therefore refused on the new leader too.
  - The PSM rebuild moves into the §2.3 incremental build, which is bounded per pass and starts at `leaderComplete_`. The synchronous `psm_.rebuildFromLog` in `executeBecomeLeaderWire_` (~Worker.h:1892-1911, shard-only, HW-bounded) is deleted. The build covers every mode and scans the user partition's whole log, through the tail replay. This concerns the user partition's producer state; it is unrelated to the coordinator load, which is HW-bounded (§5.1).
- **Residual:** in the m2 overflow-fence case the ABORT carries the old pid at epoch `INT16_MAX`, so a zombie at `INT16_MAX` is not epoch-fenced. The old pid is retired, and the FU1 safety net handles it (§10.1). Kafka without KIP-890 has the same class of gap.

### 2.6 N2 + K3: markers land as real control batches, on the worker, under the produce predicate

**K3 routing:**
```
bool ClientGateway::routesToWorker_(int32_t topicId) const noexcept {
    return workerPool_ && (!perWorkerIngressActive_ || topicId >= 0);   // today's produce predicate (~1587)
}
```
- The produce path and `appendControlRecord` both call it. Today the marker uses `perWorkerIngressActive_ && topicId >= 0 && replica && workerPool_` (~4166).
- Markers that route to a worker go through `markersInFlight_`/`markerSendQueue_` in **every** mode. They are queued FIFO behind earlier markers, never dropped.
- The inline branch remains only when there is no worker pool (unit rigs) and for gateway-owned internal partitions in shard mode. (In Legacy mode the predicate also routes internal partitions, topicId < 0, to a worker; internal partitions never carry markers, so marker routing is unaffected.)
- Under L4, a marker enqueued after a produce on the same op ring lands after it. That fixes C1 in the default config.

**N2:** `Offset PartitionReplica::appendControlBatch(std::span<const uint8_t> controlBatch, bool acksAll)`:
- role gate: not Leader → `kNotLeader` (no assert). The min-ISR gate is the same as `append`;
- restamps the base offset in place via `appendNoSyncRestamp`, never through `makeBatch_`;
- then performs `append()`'s post-append sequence: `setLeaderLogEndOffset`, `recomputeHw`, the push, **and the same flush request `append()` issues** (Minor). On the worker this records the log in `dirtyThisCycle_`, so `submitCoalescedFlushes_` covers it, and a Strict-tier HW (fsync-gated) is what the marker commit waiter observes.
- `append()` keeps its assert for internal non-marker callers.

### 2.7 N3

`RcBuilder::step` requires `producerId >= 0 && (attrs & TRANSACTIONAL)` to open a txn.

### 2.8 C1 failure modes

| Event | Outcome |
|---|---|
| Session gone before the ProduceResult drains | RC was updated at append (W-local). Only the response is lost. |
| Ack fails after the append (acks=all deadline, NotLeader, StorageError) | RC stands, because the batch is in the log. A later truncation bumps `logGeneration`, and RC rebuilds from the surviving log. |
| Demotion or self-demotion mid-flight | Next pass (or the next serve / gated produce, `revalidate`): RC → Unbuilt; read_committed on both egress paths returns nothing. The new leader builds from its own log at its `leaderComplete_`. |
| A follower period between two worker passes (BecomeFollower, replicated appends, BecomeLeader, no truncation) | `leadershipGeneration` changed: RC → Unbuilt and rebuilds from the log, which now holds the replicated batches (S4, `FollowerPeriodBetweenPassesRebuilds`). |
| Deficient-leader reconcile appends old-term txn data and markers | The build has not started (it waits for `leaderComplete_`). If a reconcile append happens later, `logGeneration` changes and RC rebuilds. |
| Partition removed or re-created | Its RC is freed with the slot. A `PushRead` for the old incarnation finds no log (or the new incarnation's fresh `Unbuilt` RC) and returns an empty chunk. |
| Coordinator failover; re-driven duplicate marker | `onControlMarker` on an untracked pid returns −1, so no range is added and nothing is reopened. |
| `ret_` full | `PushReadResult` and `CONTROL_RECORD_COMMITTED` go to the local FIFO, in order (§6). |
| Duplicate retry (`isDuplicate`) | No RC update. |

---

## 3. I1 + N1: role gate and parked-waiter validation on the owning worker

### 3.1 Role gate

The authority is W, which owns `role_`. Every check is a plain member read on the owning thread:
- `executeProduceWire_`: after the storage-failed check, `if (replica && replica->role() != Leader)` → `ProduceResult{NotLeader}`, log untouched.
- `handleWorkerIngressFrame_`: the same check after resolving `replica`.
- `executeAppendControlRecordWire_` → `appendControlBatch` → `kNotLeader` → `ControlRecordAppended{errorCode=NotLeader}` (retriable, §4).
- Cost: one load and compare on the hot `replica` line (`BM_WorkerRoleGate`, ≤ +0.2 ns).

**Gateway** (~ClientGateway.cpp:1439):
- delete the `replica->role()` read;
- look up the bridge's intended-role hint for (tp, inc):
  - `Follower` → fast `NotLeader`;
  - `Leader` **or absent** → route to W, which decides.
- The hint may lead or lag W. Both are safe, because W is authoritative and both answers are retriable.

### 3.2 Parked waiters are re-validated every pass (I1 review, N1, Minor)

**I-A: acks=all is parked on the worker in every mode.** Today, in the default (Legacy) config, acks=all produces are acked by the gateway: `deferAck` (ClientGateway.cpp ~1710) parks them in `pendingAcks_`, checked at ~3184 and ~3540–3600 against `gatewayHighWatermark() > lastOffset` and incarnation only. After a self-demotion (`followerHw_` published, PartitionReplica.cpp ~572–585), a truncation and a re-fetch, that HW can pass `lastOffset` while different data sits there: a false OK. Revision 3 parks every acks=all produce for a worker-owned partition on W (`pendingAcksAll_`, `flushOwedAcksAll_`, which exist for shard ingress) in every mode, and **deletes the gateway's `pendingAcks_`/`deferAck` path for worker-owned partitions**. The ProduceResult for a gateway-received acks=all produce is emitted by W when the waiter fires, exactly as for shard ingress. Gateway-owned internal partitions keep the gateway path (control-owned, no demotion race across threads).

Each parked entry stores `(replicaId, incarnation, offset, logGenerationAtPark)`. This covers `pendingAcksAll_` (now every mode), `durablePending_`, marker commit waiters and `parkedMarkers_`. In the existing per-pass flush loops (`flushOwedAcksAll_`, the durable reap, the commit-waiter scan, the parked-marker retry), each entry is checked before it can fire OK:

| Condition (evaluated each pass, on W) | Action |
|---|---|
| replica unresolvable, or incarnation mismatch (ownership lost) | fail `NotLeader` |
| `role != Leader` (BecomeFollower **or self-demotion in `onFetchRequest`**) | fail `NotLeader` |
| `logGeneration != logGenerationAtPark` and `offset ≥ lastTruncationOffset_` (stored with `logGeneration_`, §2.1; a reconcile append with no truncation records `lastTruncationOffset_ = LEO before the append`) | fail `NotLeader` (the content at `offset` may differ) |
| leader epoch bumped while still Leader, with `logGeneration` unchanged | **continue waiting**. The entry is still in this leader's log, and a later HW over it really commits it. Kafka's DelayedProduce does the same. |
| HW > offset (commit waiter), or the existing ack condition | fire OK |

- This is state-driven and independent of time and of which code path changed the role. There are no BecomeFollower hooks.
- Cost is O(#parked) per pass, inside loops that already iterate those entries.
- A Strict ack whose fsync completes after demotion (S0 test `DemotionFailsParkedStrictAckNotLeader`) is caught on the completion path by the same check, before it fires.

| Event | Outcome |
|---|---|
| Produce routed while BecomeFollower is in flight | Before W applies the op: appended as leader. After: `NotLeader`. |
| Bootstrap window (`committedMetadataReady_ == false`, replica Follower) | `NotLeader`, retriable (Q4 accepted). |
| Unregistered replica (incarnation 0, tests) | Gated on role too. Fixtures `becomeLeader` first (audit in S2). |

---

## 4. I3: marker delivery until committed (rich-state task)

### 4.1 Task state machine

The TCS (control) owns `MarkerTask`, keyed by `(producerId, topicId, partition)`. Fields: `coordinatorEpoch`, `producerEpoch`, `commit`, `txnId`, and **`incarnation` captured at creation** (Minor). Each state holds only its own variables:

```
Pending{attempt, eligibleAtCycle}          -- not yet handed to the gateway
InFlight{token}                            -- awaiting ControlRecordAppended
Appended{token, markerOffset}              -- in the log; awaiting ControlRecordCommitted
Committed                                  -- terminal: counts toward completion
AbandonedDeleted                           -- terminal: the partition incarnation is gone (Kafka)
AbandonedFenced                            -- terminal: this coordinator was fenced (FU2 path)
```

- `outstanding = #tasks not in {Committed, AbandonedDeleted}`; the txn completes when it reaches 0.
- Replies are matched by `(token, coordinatorEpoch)`. Unknown or superseded tokens are ignored and counted.
- Tasks are created only after the decision gate passes (§5.3).

### 4.2 Flow

1. `Pending` → `gateway.appendControlRecord(…, token, coordinatorEpoch)` → `InFlight`. It is routed by `routesToWorker_` (§2.6).
2. W, on receipt:
   - fence `psm_` (§2.5);
   - role gate;
   - min-ISR. On MinIsr, park in `parkedMarkers_` (FIFO per partition) and retry each pass when `isr().size()` changes. That is state-driven, and the parked entry is re-validated per §3.2.
   - Append OK → RC update (§2.1) → `ControlRecordAppended{token, base}` → commit waiter.
3. W, each pass: commit waiter → `ControlRecordCommitted{token, 0}` when `HW > base` and §3.2 allows, or `{token, NotLeader}` when §3.2 fails it. There is no deadline: Kafka writes markers with acks=all.
4. Gateway → TCS:
   - `Appended` → `Appended`;
   - `Committed(0)` → `Committed`;
   - `Committed(NotLeader)` → `Pending`.
   - Both returns are ordered (L2).
- **Test seam:** `setAppendFnOverride` (unit rigs with no gateway) treats the override's non-negative return as Appended and Committed in one step. A task is therefore dispatched once, not once per `doWork`, which keeps `PrepareCommitRedrivenAfterFailover`'s `markers == 1` exact.

### 4.3 Error classification

| Reply / condition | Class | Next |
|---|---|---|
| Appended → Committed OK | success | `Committed` |
| `NotLeader` (W gate, §3.2 failure, ownership refusal, not registered here) | retriable | `Pending` (§4.4) |
| `ControlRecordRefused` (stale incarnation) | retriable | the gateway re-routes as today; the task stays `InFlight` |
| `MinIsrViolation` | parked on W, never reaches the TCS | n/a |
| `Internal` (append threw) | retriable, rate-limited critical log and counter | `Pending` with backoff |
| `StorageError` | the node fail-stops | n/a |
| **Incarnation gone** (`registry.resolve(tp)` absent, or `incarnation ≠ task.incarnation`, checked at every attempt) | terminal | `AbandonedDeleted`. A re-created topic with the same name is a new partition and gets no marker. |
| Led by another node | retriable, interim | `Pending` (Q1 accepted; FU2) |
| `TransactionCoordinatorFenced` (FU2) | terminal | `AbandonedFenced` |

### 4.4 Re-drive scheduling (no wall clock)

- The TCS keeps a monotone `cycle_` counter, incremented once per `doWork`.
- `Pending` tasks with `cycle_ ≥ eligibleAtCycle` are re-dispatched, with `eligibleAtCycle = cycle_ + min(2^attempt, 1024)`.
- **Progress reset:** any of these makes affected tasks eligible immediately:
  - the registry generation changes;
  - the intended role for that tp becomes Leader;
  - the routing-image epoch changes.
- No `nowNs` is consulted.

### 4.5 Visible behaviour while a marker is retried

- **Producer:**
  - EndTxn stays deferred;
  - a same-direction retry gets 27;
  - AddPartitions, AddOffsets, TxnOffsetCommit and InitProducerId get 27;
  - an opposite-direction EndTxn gets 19;
  - Prepare\* never times out (Kafka).
- **read_committed:**
  - on a partition whose marker has not landed: the LSO stays pinned at that txn's first offset;
  - on partitions whose markers landed: the data becomes visible.
  - This is Kafka's per-partition visibility. Atomicity comes from the durable decision (§5.3).
- **Metrics:**
  - `txnMarkerTasksPending`;
  - `txnMarkerRetries{class}`;
  - oldest Pending age in cycles, with a rate-limited warning at each doubling.

---

## 5. K4, I4, I7 (+m2, m3, m4): coordinator load, durable decision, idempotency

### 5.1 Coordinator load (K4; replaces revision 1's "reload at Follower→Leader to LEO")

The TCS holds a control-owned load state machine:

```
Unloaded                                   -- every txn API → NotCoordinator; no timeouts; no dispatch
Loaded{coordinatorEpoch}                   -- serving
```

**Load predicate** (`loadable_()`, all plain reads: the txn-state replica is control-owned):
```
isLeaderFn_()                                  // FindCoordinator routes here (consensus leader)
&& txnReplica->role() == Leader
&& txnReplica->isLeaderComplete()              // leaderComplete_: served HW released from the stale floor
&& txnReplica->highWatermark() == txnReplica->logEndOffset()
```

**Why HW == LEO is time-independent:**
- while `Unloaded`, the TCS is the txn-state partition's **sole appender** and does not append, so LEO is fixed;
- HW reaches it as soon as the ISR replicates the tail. That is progress-driven, with no timer;
- if min-ISR is unmet, HW is clamped and the coordinator stays `Unloaded`. `NotCoordinator` is retriable, and liveness returns with the ISR (correct: we cannot know what is committed).

**Load** (`Unloaded` → `Loaded`):
1. `txnState_->rebuildFromLog(bound = HW)`. Since HW == LEO, nothing committed is above it, which honours Q2 (a new leader may truncate its tail, so bound by what is committed). Every committed Prepare\*, including one above a stale `completenessFloor_`, is loaded, because `leaderComplete_` released the floor first. This is cold and synchronous on control, as today.
2. `coordinatorEpoch = txnReplica->epoch()`: the txn-state replica's **leader epoch**. It is monotone across leaderships of that partition. The consensus term is no longer used.
3. `coordinator_.reload(snapshot, coordinatorEpoch, nowNs)`:
   - Prepare\* → `markersPending`, and the decision is marked already committed (it is ≤ HW);
   - Complete\* stays Complete\* (§5.4);
   - Ongoing gets its timeout clock restarted at `nowNs`, which only ever extends;
   - the allocator is reseeded (§5.6).
4. Clear the marker task table and the gateway's queued markers for older epochs (m3). The next `doWork` re-drives every Prepare\* (§4).

**Evaluation points:** at the top of `doWork` **and at the top of every txn request handler** (`handleInitProducerId`, `handleAddPartitions`, `handleAddOffsets`, `handleTxnOffsetCommit`, `handleEndTxn`). Load is synchronous, so a request that arrives while `loadable_()` holds is served in the same call. That is deterministic and works without a prior `doWork` (S0 rigs call `initPid()` first).

**Unload** (`Loaded` → `Unloaded`): when `isLeaderFn_()` is false, the replica role is not Leader, or `txnReplica->epoch() != coordinatorEpoch`, checked at the same points:
- answer every deferred EndTxn `NotCoordinator`;
- drop every task and decision gate;
- purge `markerSendQueue_` entries whose coordinatorEpoch is old;
- clear `coordinator_`.
- Late marker replies match no task and are ignored.
- An epoch change while still leader means unload, then load again under the new epoch.

**Routing invariant (Minor):** FindCoordinator routes to the consensus leader, while the coordinator loads only where the `__transaction_state` replica is Leader. Today these coincide, because leadership collapses to the consensus leader. S7 adds `TxnCoordinatorLoadRegression.CoordinatorRoutedToTxnStateLeader`, which pins the invariant; when multi-node partition leadership lands (with F15-FU2), FindCoordinator switches to the `__transaction_state` partition leader, as in Kafka. The load scan itself is a cold synchronous scan on control (bounded by the txn-state log, as today).

**Store-less configuration** (`txnState_ == nullptr`: in-memory unit rigs only; production always wires the store): the predicate is `isLeaderFn_()`, and coordinatorEpoch is a local counter bumped on each load. There is no decision gate (§5.3) because nothing is persisted.

**Timeouts** (`coordinator_.doWork(nowNs)`) run only while `Loaded`. The `put` in `persistTransition` is replaced by the decision gate (§5.3). The coordinator never writes on its own.

### 5.2 Completion persisted (m4)

`completeAwaiting_` runs `coordinator_.completeTransaction` (offsets first, as today), then `txnState_->put(meta in Complete*)`, then the deferred EndTxn OK. The Complete\* put does not need to commit before the response:
- if it is lost, the new coordinator loads Prepare\*;
- it re-drives the markers (duplicates are harmless) and re-applies the offsets (idempotent);
- it completes again.

### 5.3 Decision gate: every Prepare\* is committed before any marker is dispatched (I4b + I4 review)

**One gate for every transition into Prepare\*:** EndTxn, the **Ongoing timeout** (`coordinator_.doWork`) and the **InitProducerId fence** of an Ongoing txn (including the m2 overflow fence). All three go through `TCS::persistDecision_(txnId, preTransitionCopy)`:
1. `off = txnState_->put(meta)`, which returns the record offset or −1 (min-ISR).
2. On success: `Decision{AwaitingCommit{prepareOffset=off}}`. Each `doWork` checks `txnReplica->highWatermark() > prepareOffset`, a control-owned plain read. Once it holds, create the §4 marker tasks.
3. On failure:
   - **EndTxn:** restore the pre-transition copy (`coordinator_.restore(copy)`) and answer `MinIsrViolation (10)`, which is retriable.
     - **Concrete rollback mechanism (Minor):** before calling the transition, the TCS copies the `TxnMetadata` into a reusable member scratch (`decisionScratch_`), so there is no allocation in steady state. On a put failure, `restore` writes it back. Today the failure is ignored ("Ignore persist failure"), and that is removed.
   - **InitProducerId fence:** restore the copy and answer `ConcurrentTransactions (27)`, which is retriable (F14).
   - **Timeout:** there is no client to answer, so keep the in-memory PrepareAbort with `Decision{Unpersisted}` and retry the put on every `doWork`.
     - This is safe: until the put commits, the durable state is Ongoing.
     - If failover happens first, the new coordinator times the txn out again: same decision (ABORT), and no marker has been dispatched.
- **Reload:** loaded Prepare\* records are ≤ HW, so they are already committed and dispatch immediately.
- **Why HW, not LEO:** dispatch only when the decision is majority-durable, as Kafka does (markers are sent from the Prepare\* append callback with acks=all).

### 5.4 EndTxn idempotency table (Kafka-faithful)

**Complete\* becomes the resting state:**
- `completeTransaction` stays in `CompleteCommit`/`CompleteAbort`, keeping `producerEpoch`;
- addPartitions accepts Complete\* → Ongoing;
- InitProducerId treats Complete\* like Empty (bump), and Complete\* leaves its 27 set;
- the constructor and `reload` no longer collapse Complete\* to Empty (the overflow pid swap is still adopted).

The on-disk layout is unchanged.

| Current state (after the epoch/pid fence) | EndTxn(commit) | EndTxn(abort) |
|---|---|---|
| Ongoing | → PrepareCommit (deferred) | → PrepareAbort (deferred) |
| PrepareCommit | **27** | **19** |
| PrepareAbort | 19 | **27** |
| PrepareAbort from a fence, caller holds the new identity | 27 (m2) | 27 (m2) |
| CompleteCommit, same epoch | **0 OK** | 19 |
| CompleteAbort, same epoch | 19 (18 if the abort was a fence) | **0 OK** |
| Empty / Dead | 19 | 19 |
| stale epoch | 17 | 17 |

**m2:** in `validateEpochAndPid`, if `meta.state ∈ Prepare*` and `(pid, epoch) == (pendingNewProducerId, pendingNewProducerEpoch)`, return `kConcurrentTransactions` (internal 6 → wire 27). This applies to AddPartitions, AddOffsets, TxnOffsetCommit and EndTxn. `concurrentIfPreparing_` passes it through.

### 5.5 I4 failure modes

| Event | Outcome |
|---|---|
| Dies after Prepare\* is committed, before any marker | The new leader is not loaded until `leaderComplete_` and HW == LEO. It then loads the Prepare\*, re-drives, and the decision is unchanged. |
| Dies with a committed Prepare\* above the new leader's stale `completenessFloor_` | **K4 case.** `leaderComplete_` releases the floor before the load, so the Prepare\* is loaded and never timed out to ABORT. |
| Dies before Prepare\* is committed (EndTxn, timeout or fence) | No marker was dispatched (§5.3). The new coordinator sees Ongoing, a client retry or the timeout decides, and the outcome is atomic. |
| Old coordinator's queued markers after it is deposed | Purged locally (m3). Those already at W carry the durable decision and are idempotent. |
| Deferred EndTxn's session gone | Completion still happens. A retry gets 0 from Complete\*. |
| Retried EndTxn after the next txn began | Ends the new txn. The Kafka-without-KIP-890-part-2 hole (§10.1). |
| Consensus leader ≠ txn-state replica leader | `loadable_()` is false, so the node answers `NotCoordinator` and the client re-discovers. `put` never reaches a follower replica, which removes the appendKeyed follower-assert hazard. |

### 5.6 I7: persisted pid block ceiling

- `ProducerIdAllocator` hands out ids from a block `[base, ceiling)`, and the ceiling is persisted in `__transaction_state` as a reserved record. The key is a distinct record-type tag in the value header, so it can never collide with a transactional id; the layout is verified in S7. Compaction keeps the latest. This mirrors Kafka's ProducerIdManager.
- **Allocate:**
  - pids come only from a block whose ceiling record is **committed** (HW > its offset);
  - when the current block is half used, the TCS puts the next ceiling (prefetch), so steady state never waits;
  - if the block runs out before the next one commits, InitProducerId (transactional **and** idempotent) answers `ConcurrentTransactions (27)`, which is retriable (F14 treats 27 as retriable for InitProducerId).
- **Load:** `reseedAtLeast(max(persistedCeiling, maxPidInLog + 1, pendingNewProducerId + 1))`, then persist a fresh ceiling before the first allocation. Idempotent pids issued by the previous coordinator lie below its persisted ceiling, so they are never reissued.

---

## 6. m1, m5, I8

**m1:**
- `WorkerOp SetAdmissionPolicy{minIsr}` (OpType 17) is broadcast whenever control's `cachedMinIsr_` changes, and at startup before runners start. It is retried until delivered (`deliveredPolicyGen_`), and the latest value supersedes older ones.
- W stores `admissionMinIsr_` and uses it in `handleWorkerIngressFrame_` (~Worker.h:3594) and `executeProduceWire_`.
- The WorkerOp `minIsr` field (id 49) is **dropped** (Q5).

**m5 + I8, one return FIFO in every mode:**
- `enqueueReturn_` and `enqueueOrderedReturn_` become one function. If the FIFO is non-empty, append to it; else `tryClaim`; on a refusal, append to the FIFO.
  - The Legacy-mode silent drop (~Worker.h:936-945) is **removed**.
  - A `tryClaim` exception (~929-931), an encode error, or the `catch (...) { erase; continue; }` in `flushRetOverflow_` (~3326-3331) is an **invariant-violation halt**, never a drop.
- `retOverflow_` becomes a pre-allocated fixed-capacity ring of tagged entries (`WorkerReturn` | `TxnReturn`, §7), with O(1) pop-front instead of `vector::erase(begin)`.
- **Bound:** while the FIFO is non-empty:
  - shard client-ingress polling is gated (G-A6);
  - in **every mode** the op-ring drain stops once `size ≥ kOverflowHighWater`. Control-side producers already queue on a full op ring: `pendingRoleOps_`, `markerSendQueue_`, `pendingSubOps_`, and produce answers NotLeader.
- So `|FIFO| ≤ kOverflowHighWater + opDrainBatch·R + K + P`, where:
  - `R` is the maximum number of returns per op (1);
  - `P = |pendingAcksAll_| + |durablePending_| + |commit waiters| + |parkedMarkers_|`, each bounded by the G-A6 reservation, which now counts commit waiters and parked markers.
- The capacity is `static_assert`ed. Exceeding it halts.
- Because a `tryClaim` exception now halts, `workerReturnRingBytes` is a correctness setting: at startup W checks that `ret_.maxMsgLength()` covers one 512 KiB `PushReadResult` chunk plus its header, and refuses to start otherwise (the default, 4 MiB, does).
- In Legacy mode the op-drain pause also delays `ApplyReplicated` and `FetchRequest` ops. It is transient and bounded, but the DR matrix watches for ISR-lag interaction (`retFifoHighWaterPauses` counter).
- No deadlock: control drains returns every cycle and never waits on an op ring. In `fused` placement, both sides run sequentially.

**Minor cleanups:**
- `TransactionCoordinationService.cpp:17`'s header says `ConcurrentTransactions (20)`; fix it to **27**.
- `ReplicatedTxnStateStore.cpp:58` uses `std::call_once` for its CRC table, a sync primitive. Replace it with a `constexpr` table.

---

## 7. rune-protocol changes (one commit, regenerated with `tools/sbe-all-1.40.2.jar`)

The commit is additive except for dropping WorkerOp field 49 (Q5), which changes WorkerOp's layout. That is accepted: WorkerOp is an in-process ring message and both sides are built together.


- **WorkerReturn (ProduceResult carrier): unchanged in size.**
  - `PRODUCE_NOTIFY(2)` becomes `RESERVED_2`, never emitted.
  - No new fields, so the ProduceResult encode cost is unchanged (proved by `BM_ProduceResultEncode`, before and after).
- **New message `TxnReturn` (template id 6;** ids 1–5 are taken, 4 is `FlushCompletion` in `rune-internal-ring.xml`**)**, on the same `ret_` ring, discriminated by template id in `drainReturn`. It has a fixed block: `kind:u8, topicId:i32, partition:i32, incarnation:u32, leaderEpoch:i32, token:i64, errorCode:i16`, with kinds `CONTROL_RECORD_COMMITTED(1)` (and `TXN_HANGING(2)`, reserved for F15-FU1). The revision-2 `RC_*` kinds and the `ranges` group are dropped with the mirror.
- **OpType:** add `SET_ADMISSION_POLICY(17)`; 18 stays free (`SET_RC_INTEREST` dropped with the mirror). Remove the WorkerOp `minIsr` (id 49) per Q5.
- The `TxnBatchAppended`/`TxnStateSnapshot` types and fields 28-31 from revision 1 are **not** added (moot under Option B). The WorkerOp `coordinatorEpoch` is not added: the gateway tags its own queue entries.
- Round-trip regression test: `ProtocolRegression.TxnReturnRoundTrip`, plus `…WorkerReturnLayoutUnchanged` (`sbeBlockLength` pinned).

---

## 8. Implementation plan (ordered; S0 done; each step turns named S0 tests green)

**Rules for every step:**
- RED first, on the step's base commit.
- New tests land with a behaviour-preserving stub, so the first run is a behavioural RED, not a compile error.
- Every test lives in a `*Regression*` suite.
- **No S0 test is weakened.** The only permitted S0 edit is the observation-point retarget in S4, specified below.
- Gate per step:
  - `ctest -R Regression` green, plus every S0 test listed for that step and every earlier step;
  - the full `ctest -j2`;
  - benchmarks for hot-path steps;
  - TSan on S2, S3, S4 and S5.
- Heavy runs go under `/tmp/rune-heavy.lock`.

**S0 test ledger** (25 RED at `63b5346`; the step that turns each green):

| Step | S0 tests turned green | Count |
|---|---|---|
| S1 | `ControlBatchRegression.{ReplicaAppendedMarkerIsAControlBatchInTheLog, ReplicaAppendedAbortMarkerClosesTheTxnOnRebuild, ConsumerNeverReceivesMarkerBytes, ReplicaPathMarkerLandsAsControlBatch, GatewayInlineMarkerLandsAsControlBatch, RebuildIgnoresIdempotentNonTxnBatches}`, `LsoTrackerRegression.ClearDropsTheFirstOffsetIndex` | 7 |
| S2 | `TxnRoleGateRegression.{GatewayPathProduceOnFollowerIsNotLeader, ShardIngressProduceOnFollowerIsNotLeader, MarkerOnFollowerIsRetriableNotLeader, DemotionFailsParkedAcksAllNotLeader, DemotionFailsParkedStrictAckNotLeader}` | 5 |
| S3 | none (m5/I8 infrastructure that S4–S6 ordering relies on) | 0 |
| S4 | `TxnFactOrderRegression.{AcksAllParkedBatchBeforeAbortIsCovered, StrictDeferredBatchBeforeAbortIsCovered, SessionGoneBatchStillRegistered}` | 3 |
| S5 | `TxnFactOrderRegression.ZombieBatchAfterAbortMarkerIsFencedShardIngress` | 1 |
| S6 | `MarkerRetryRegression.MinIsrRefusedMarkerNeverCompletesTheTxn` | 1 |
| S7 | `TxnCoordinatorFailoverRegression.{PrepareCommitRedrivenAfterFailover, CompletionIsPersisted, EndTxnRetryAfterCompleteSameDirectionIsOk, EndTxnRetryAfterCompleteSameDirectionIsOkAtCoordinator, EndTxnOppositeDirectionIsInvalidTxnState, LossOfLeadershipAnswersNotCoordinatorAndClearsTasks, OverflowFenceNewPidGetsConcurrentTransactionsOnAddPartitions, OverflowFenceNewPidGetsConcurrentTransactionsOnEndTxn}` | 8 |
| S8–S10 | none (new tests only) | 0 |
| **Total** | | **25** |

| Step | Scope | New regression tests (beyond S0) | RED evidence expected on base |
|---|---|---|---|
| **S0** ✅ | 25 RED tests, `63b5346` | n/a | see the S0 report |
| **S1** | protocol §7; **N2** `appendControlBatch` (with flush); **N3**; **N4**; **K3** `routesToWorker_` for markers; `RcBuilder::step` extracted, with `rebuild` as a loop over it; constexpr CRC; header 20→27 | `MarkerRoutingRegression.LegacyModeMarkerAppendedByOwningWorker` (default config, `perWorkerIngressActive_ == false`, worker pool set: the control thread never appends; LEO unchanged until W runs) · `…LegacyMarkerLandsAfterQueuedProduce` (produce, then marker queued: marker offset > produce offset) · `ControlBatchRegression.MarkerAppendRequestsFlush` (Strict tier: the log is in `dirtyThisCycle_` after the marker) · `ProtocolRegression.TxnReturnRoundTrip` · `…WorkerReturnLayoutUnchanged` · `BM_ProduceResultEncode` | inline `replica->append` on control (LEO +1 before W runs) · marker offset < produce offset · no flush for the marker |
| **S2** | **I-A** acks=all parked on W in every mode, gateway `pendingAcks_` deleted for worker-owned partitions; **I1** worker role gate + gateway intended-role hint (absent → route); **N1/I1** per-pass waiter validation with `logGeneration_` + `isLeaderComplete()` accessors | `TxnRoleGateRegression.SelfDemotionFailsParkedAcks` (`onFetchRequest` with a higher epoch, no op: the parked ack fails `NotLeader` within one pass) · `…EpochBumpWhileLeaderKeepsWaiter` (becomeLeader at a higher epoch, same log: the waiter fires OK when HW passes) · `…TruncationBelowWaiterFailsIt` · `…GatewayUsesIntendedRoleHint` (**replaces the TSan RED**: hint Leader with replica `role_` Follower → the gateway routes to W and does not answer inline; hint Follower with replica Leader → inline `NotLeader`. On base the gateway reads `role()`, so both arms are RED) plus a structural compile-time guard: the gateway holds a `GatewayReplicaView` that has no `role()`, pinned by `static_assert(!requires(const GatewayReplicaView& v){ v.role(); })` · `…LegacyAcksAllDemotionFailsNotLeader` (**I-A**, default config: acks=all produce via the gateway, self-demotion, truncation + re-fetch past `lastOffset` with different data → the produce fails `NotLeader`, never OK) · `BM_WorkerRoleGate` | self-demoted waiter never answered · gateway answers from `role()` · Legacy gateway acks a truncated offset OK |
| **S3** | **m5 + I8**: one FIFO in every mode, op-drain high-water, halts on claim/encode exceptions | `ReturnOverflowRegression.LegacyFullRingNeverDropsProduceResult` · `…BoundedAndNeverDrops` · `…OpDrainPausesAtHighWater` · `…ClaimExceptionHalts` · `BM_ReturnFifo` | Legacy full ring logs "dropped" · encode error erases the entry |
| **S4** | **Option B** (§2.1–2.4): W `RcState`, incremental build at `leaderComplete_` (I2, I5), W-push read_committed, read_committed clamp + filter in `executePushReadWire_` for the gateway push path (I-B), deletion of gateway LSO feeds, scans and bound computation | **S0 retarget (the only S0 edit; five tests):** in the three tests above, in `ZombieBatchAfterAbortMarkerIsFencedShardIngress` (F15EosRegressionTest.cpp:389, its `gw.openTxnsForTest` check) and in `MinIsrRefusedMarkerNeverCompletesTheTxn`'s `openTxns == 1` check, `gw.abortedRangesForTest`/`gw.openTxnsForTest` are replaced by `w().rcAbortedRangesForTest`/`w().rcOpenTxnsForTest` (same scenarios, same expected values). Each also gets `ASSERT_EQ(rcOpenTxns, 1)` right after the data append, so the "0 after" checks cannot pass vacuously, plus a behavioural egress check: the rig adds a gateway-pushed read_committed subscription from offset 0 (a declared scenario addition, not a weakening) and asserts it receives no record of the aborted/open txn. The retarget is committed first and re-run on base: still RED, failing on the precondition `ASSERT_EQ(rcOpenTxns, 1)` because the stubbed worker accessors return 0 (documented RED mode). New: `TxnRcRegression.BuildWaitsForLeaderComplete` (deficient leader: the reconcile appends an old-term abort marker after BecomeLeader, and the build sees it) · `…BuildIsBoundedPerPass` (a log of k×`kRcScanBytesPerPass` takes ≥k passes; each pass reads ≤ the budget) · `…AppendsDuringBuildAreReplayed` · `…TruncationRebuilds` · `…SelfDemotionStopsServing` · `…WorkerPushReadCommittedHoldsOpenTxn` (per-worker egress arm: K1) · `…WorkerPushFiltersAbortedBatch` · `…GatewayPushUsesWorkerBoundNotGatewayHw` (K2/I-B: the gateway HW snapshot is past an open batch → `PushRead` returns nothing at or above the open first offset) · `…GatewayPushFiltersAbortedBatchAndAdvances` (aborted + control batches stripped; `newNextOffset` past them) · `…GatewayPushReadCommittedEmptyWhileBuilding` · `…RetentionDuringBuildCompletes` (I-C: retention deletes the segment under the cursor mid-build → Ready in bounded passes) · `…EpochBumpSameLogKeepsReady` · `…InternalPartitionsNeverCarryTxnBatches` · `…IdempotentProduceWhileBuildingIsNotLeader` · `BM_RcPushNonTxn`, `BM_RcBuildStep` (p99 per pass) | the abort range is missing (S0) · the build sees no marker · per-worker push delivers the open txn (HW-bounded) · `PushRead` returns the open txn (HW-bounded) · build stuck after retention |
| **S5** | **I6** W `psm_` in `executeProduceWire_` in every mode, gateway PSM mirror removed; **C1+** marker fence on receipt; **I3** PSM rebuild applies control batches as epoch raises (inside the S4 build); synchronous `psm_.rebuildFromLog` in BecomeLeader deleted | `TxnFenceRegression.ZombieBatchAfterAbortMarkerIsFencedGatewayPath` (Legacy) · `…MarkerEpochFenceSurvivesBecomeLeader` (I3: rebuild then zombie → 17) · `…GatewayPathDuplicateDedupedByWorker` (I6: the same batch twice via `executeProduceWire_` → one append) · `…LegacyModeDedupWithoutGatewayMirror` | zombie appended after BecomeLeader · double append |
| **S6** | **I3** marker task SM, commit waiter, MinIsr park, cycle backoff, incarnation-based abandonment | `MarkerRetryRegression.MinIsrMarkerParksAndLandsWhenIsrRecovers` · `…NotLeaderMarkerRedrivenAfterRoleRegained` · `…AppendedButNotCommittedBeforeDemotionIsRedriven` · `…InternalErrorRetriedWithCycleBackoff` (constant `nowNs`) · `…ProgressEventResetsBackoff` · `…RecreatedTopicIsTerminalByIncarnation` · `…LateDuplicateReplyNeverDoubleCompletes` (**RED caveat:** on base this reproduces only through the unknown-token path of `onControlRecordRefused_`: a duplicate `ControlRecordRefused` for a partition **not** registered on this node calls `onControlMarkerRefused` twice. For a registered partition, base halts on an invariant violation instead.) · `…ProducerSeesConcurrentTransactionsWhileRetrying` | refused marker completes the txn (S0) · double decrement via the unknown-token path |
| **S7** | **K4** load state machine + predicate + coordinatorEpoch = txn replica leader epoch; Complete\* resting; idempotency table; **m2 m3 m4**; **I7** pid block ceiling | `TxnCoordinatorLoadRegression.NotCoordinatorUntilLeaderComplete` · `…NotCoordinatorUntilHwEqualsLeo` · `…NoTimeoutsWhileUnloaded` · `…CommittedPrepareAboveStaleFloorIsNeverMissed` (**replaces revision 1's `RebuildLoadsCommittedPrepareAboveLocalHw`**: the new leader's served HW is held at the old floor; the coordinator loads only after `leaderComplete_` and sees PrepareCommit; no ABORT is written) · `…ConsensusLeaderButTxnReplicaFollowerIsNotCoordinator` (no put reaches a follower) · `…CoordinatorEpochIsTxnReplicaLeaderEpoch` · `…CoordinatorRoutedToTxnStateLeader` · `…EpochChangeUnloadsAndReloads` · `TxnPidBlockRegression.IdempotentPidsNotReissuedAfterFailover` · `…AllocationWaitsForCommittedCeiling` | the consensus-edge reload misses the Prepare\* · ABORT written · pid reissued |
| **S8** | **I4b + I4 review**: one decision gate for EndTxn, timeout and fence, with the pre-transition copy | `TxnDecisionDurabilityRegression.MarkersDispatchedOnlyAfterPrepareCommitted` · `…FailoverBeforePrepareCommitIsAtomic` · `…TimeoutAbortPersistedBeforeDispatch` · `…FenceAbortPersistedBeforeDispatch` · `…EndTxnPutFailureRollsBackAndAnswers10` · `…TimeoutPutFailureRetriesWithoutDispatch` | markers dispatched in the same call as the put · timeout dispatch with no put at all (TCS `txnLog_` null) |
| **S9** | **m1** admission policy op; drop WorkerOp `minIsr` | `ShardIngressMinIsrRegression.RuntimePolicyAppliesToShardIngress` · `…PolicyDeliveredBeforeFirstPoll` | append succeeds while ISR < runtime minIsr |
| **S10** | end-to-end proof (integration before E2E) | in-process: `EosEndToEndRegression.ZombieAbortReadCommittedNeverSeesAbortedData` (real gateway, worker threads and Aeron client; Strict and acks=all; **default config (gateway push through `PushRead`) and the shard flag (worker push)**) · `…CoordinatorFailoverMidCommitIsAtomic` (3-node in-process). Then the DR docker matrix on k3s-node-01 (`vm-clean.sh` first): kill the coordinator mid-commit; netem-partition the marker leader. | system proof. Each scenario first runs on base to record the failure. |

**Step caveats:**
- **S2 RED (debug abort):** `MarkerOnFollowerIsRetriableNotLeader` is RED by `assert` abort only in Debug builds (`build-fast` is Debug). Under NDEBUG the child appends on the follower and exits 1, which is still RED. The S2 gate runs both builds once.
- **S7 vs S0 rigs:** the S0 `TxnRig` calls `initPid()` before any `doWork`, so the load is also evaluated at handler entry (§5.1). The store-less `TxnRig` uses the store-less predicate. The store-backed rig's state replica is constructed as Leader with ISR {1}: `leaderComplete_` defaults to true, and `append` recomputes HW eagerly, so HW == LEO. Both S7 greens rely on this; the S7 RED run confirms it before any fix.
- **S8 must keep the S7 greens.** `CompletionIsPersisted` (store-backed, ISR {1}) passes the decision gate in the same `doWork`, because HW == LEO right after the put. If it does not, that is a defect in S8, not a reason to edit the test.
- **Order:**
  - S1 must come first: without real control batches, no RC or read-side assertion means anything;
  - S2 comes before S6 (retry needs a deterministic NotLeader);
  - S3 comes before S4 and S6 (`PushReadResult` and commit returns must never drop);
  - S4 comes before S5 (the PSM rebuild moves into the S4 build).
  - S7 and S8 can run in parallel with S4–S6, but S8 follows S7.

---

## 9. Performance (re-checked for revision 3; S4 numbers in the S4 report)

**Non-transactional produce, both paths, every mode: O(1), allocation-free, no new ring traffic.**

| Added work | Cost | Proof |
|---|---|---|
| role gate | one load and compare on the hot `replica` line | `BM_WorkerRoleGate` ≤ +0.2 ns |
| RC hook | one `attrs & TRANSACTIONAL` branch, `[[unlikely]]`, never taken | `BM_ProduceNonTxnPath` before/after, p99 unchanged |
| `rc.status != Ready` gate | taken only for `producerId ≥ 0`. pid −1 skips it (same branch as the existing PSM check) | same benchmark |
| `psm_` in `executeProduceWire_` (I6) | moved from the gateway thread to W. Net zero across the system; on W it is the same O(1) lookup shard ingress already does | `BM_GatewayPathProduce` before/after |
| ProduceResult | **unchanged** (new returns use `TxnReturn`) | `BM_ProduceResultEncode`, `…WorkerReturnLayoutUnchanged` |
| return FIFO | O(1) push/pop. The ring is pre-allocated, replacing `vector::erase(begin)` | `BM_ReturnFifo` |
| parked-waiter re-validation | O(#parked) inside loops that already iterate them: two loads and compares per entry | `BM_FlushOwedAcksAll` before/after |

**Worker-side LSO adds nothing per record for non-transactional batches:**
- the push loop computes `bound` once per subscription per pass;
- for read_uncommitted subscriptions, nothing changes;
- for read_committed subscriptions, `lso(hw)` costs O(1) amortised (front of the FIFO);
- `isAborted` runs only for batches with the TRANSACTIONAL bit.

`BM_RcPushNonTxn` (read_committed sub, non-txn data) must match the read_uncommitted push within noise.

**Transactional batches:**
- one flat-table probe per batch, plus an insert only on a txn's first batch, pre-reserved, with no allocation in steady state;
- markers: one O(1) close plus one range append.

**Build (the I5 p99 risk):**
- bounded by `kRcScanBytesPerPass` per pass, zero-copy;
- the budget is set by `BM_RcBuildStep`, **run with a cold page cache** (a byte budget does not bound page-fault time), so one step's p99 stays under the per-pass latency budget (target: ≤ 20 µs, within the 100 µs p99 law);
- the build never runs on the produce hot path except as one bounded inline step for an idempotent or transactional batch on a not-yet-Ready partition.

**Gateway push path:** no new ring traffic. The `PushRead` op already carries `isolation`; W adds one `lso(hw)` per read_committed read and one attribute check per batch in the existing copy loop. The revision-2 `RC_BOUND` traffic is gone with the mirror. **No empty-PushRead spin (addendum R-3):** the `PushReadResult` echoes W's effective bound and its RC readiness (in two fields a PushReadResult never otherwise uses — `lastOffset`, `deferAck` — so the WorkerReturn layout is unchanged); a subscription whose cursor reached that bound is parked at the gateway's HW snapshot and re-read only when the snapshot moves (or its isolation carrier arrives): at most one PushRead per HW change while an open txn pins the LSO. While RC is not Ready, W **holds** the read and answers it on Ready (§2.3): no re-issue per cycle. A result that answered nothing (a refusal, no log: effective bound −1) never parks the subscription (review I-1).

**Control thread:** the coordinator load, the decision gate and the pid block are all cold paths.

---

## 10. Follow-up tickets (designed here, not solved)

### 10.1 F15-FU1: per-partition AddPartitionsToTxn verification (KIP-890-like) and an LSO safety net

- **Verification:**
  - on AddPartitions OK, the TCS enqueues `VerifyTxnPartition{pid, epoch, tp}` to W before it responds (L4);
  - W keeps a pre-allocated verified set;
  - an unverified transactional batch is refused retriably (27);
  - the entry is cleared at the marker append.
- **Safety net:** under Option B, the hanging-txn detector runs **on W**. When `rc.tracker` holds an open pid that the TCS has not verified for that tp (per the verified set), W emits one `TxnHanging{pid, epoch, tp}` (a `TxnReturn` kind). The TCS schedules an `AbortHanging` marker task (KIP-664 semantics) through §4.
  - It is triggered by the append itself, with no scanner timer.
  - This partially closes the §2.5 overflow residual and the §5.5 "retry ends the new txn" class. Full closure needs KIP-890 part 2 (an epoch bump per txn).

### 10.2 F15-FU2: cross-node marker delivery (Q1 accepted: ticketed as a prerequisite for multi-node partition leadership)

Interim: a task for a partition led elsewhere stays `Pending`. That is unreachable today, because leadership collapses to the consensus leader.

Design:
1. The routing image names the leader node.
2. The task state becomes `InFlightRemote{node, token}`.
3. An internal `WriteTxnMarker{pid, epoch, coordinatorEpoch, commit, tp, token}` goes over the node-to-node envelope to that node's gateway, which runs §4.2 and replies `Appended` / `Committed` / `error`.
4. The receiving W fences on `coordinatorEpoch` per pid in `psm_`.
5. Duplicates are idempotent; retries are unchanged.

---

## 11. Client requirement: rune-client-cpp (ticket F14)

The client must treat **27 ConcurrentTransactions as retriable** for InitProducerId (now also when the pid block is not yet committed, §5.6), AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit and EndTxn. It must **stop ignoring AddPartitions errors**: data for p is enqueued only after p's AddPartitions result is OK. It must treat `NotCoordinator` as "re-discover, then retry", which now also happens while a new coordinator is loading (§5.1).

Interface:
- a retained request descriptor per pending txn op `{kind, encoded frame, attempt}`, owned by the client's single duty-cycle agent;
- on 27: re-send after `min(2^attempt, cap)` agent cycles. No wall-clock sleeps; the only time bound is a user-level deadline;
- EndTxn is retried until it gets 0, 19, 17 or 18.
- F8 alignment: the futures and mutex in `TransactionalProducerImpl` are F8's to remove. F14 adds none.

---

## 12. Risks and open questions

- Q1–Q6 are **decided** (see Decisions). This revision applies them:
  - Q1: tasks stay `Pending` (§4.3, §10.2);
  - **Q2: the load is bounded by HW, under the loaded predicate (§5.1); there is no "load to LEO" anywhere**;
  - Q3: the gateway PSM is removed, extended by I6 to every mode for worker-owned partitions (§2.5);
  - Q4: `NotLeader` in the bootstrap window (§3.2);
  - Q5: WorkerOp `minIsr` dropped (§6, §7);
  - Q6: EndTxn keeps deferred-until-committed timing until F14 ships and the full plan through the E2E step is green, then switches to Kafka timing. Revision 1's "S0–S8" is revision 2's S0–S10: the steps were renumbered, and the E2E step is now S10.
- **Risk: build latency on a large log.** The build is bounded per pass but proportional to the log size, so read_committed (and idempotent produce) on a newly elected leader waits ⌈size / budget⌉ passes. Kafka avoids this with producer snapshots and a `.txnindex` per segment. Follow-up: persist an RC/PSM snapshot at segment roll, so the build starts from the last snapshot instead of `logStart`. Not needed for correctness.
- ~~Risk: the gateway push path reads worker-owned log bytes on control~~ — **withdrawn (re-review #2, I-B):** with a worker pool (every production mode), the read already happens on W through `PushRead`. Only the no-pool unit-test path reads on control.
- **Risk: N2 changes the on-log bytes of markers.** Existing logs hold wrapped markers. Pre-production, so no reader tolerance is planned; confirm at the S1 review.
- **Risk: open-txn tracker capacity.** It is pre-reserved per partition (`rcOpenTxnCapacity`), and growth beyond that is a counted cold doubling. Watch `rcTrackerGrowths` in the DR matrix.

## Decisions (user, 2026-10-01 — accepted the recommendations)
- **Q1** Cross-node marker delivery: accept the Prepare* stall for a partition led by another node for now (unreachable today: leadership collapses to the consensus leader); ticket cross-node marker delivery as a prerequisite for multi-node partition leadership.
- **Q2** Assume a new `__transaction_state` leader CAN truncate its tail: coordinator reload is bounded by the COMMITTED offset (HW), not the log end.
- **Q3** Delete the gateway's PSM copy in shard mode: workers are the single authority for producer state.
- **Q4** Accept NotLeader for produces in the bootstrap window (Kafka-equivalent; clients retry).
- **Q5** Drop the WorkerOp `minIsr` field in favour of the runtime min-ISR policy op (m1).
- **Q6** After F14 (client retries ConcurrentTransactions) ships, switch EndTxn to Kafka timing (respond once Prepare* is durable) — after S0–S8 are green.
