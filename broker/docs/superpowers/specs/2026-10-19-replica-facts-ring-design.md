# Replica facts over rings (retire `published*` atomics) — design

Status: DRAFT 2026-10-19, carryover from F9 slice B. Design only; nothing implemented.
Branch read: `feat/auth` @ 9bff81b1.

## 1. Problem

`PartitionReplica` mirrors owner-thread state into `std::atomic` fields. Three other threads
acquire-load them: the control thread (NodeRuntimeAgent and the gateway) and the LogCleaner thread.
This breaks hard rule 1 (no atomics as coordination). It also costs the hot path: the owner
re-stores every field on every duty pass, for every replica.

### 1.1 Inventory (replication, worker and gateway paths)

| # | Field / site | Writer | Reader(s) | Allowlist cat. |
|---|---|---|---|---|
| A1 | `publishedReplicaLeo_[32]` (PartitionReplica.h:2077, stored at PartitionReplica.cpp:2765-2786, cleared by `clearPublishedFollowerPositions_` :2781) | owner (worker / control / admin) | control: `NodeRuntimeAgent.h:464-480` loops `allReplicas() × 32` **every scan** → `MetadataService::reportReplicaPosition` | SLICE-B |
| A2 | `publishedControllerEpoch_` (:2076, store :2763) | owner | control (same loop) | SLICE-B |
| A3 | `publishedHwForCleaner_` (:2064, store :2738) | owner | cleaner `LogCleanerAgent.h:434,852`; gateway `gatewayHighWatermark()` at `ClientGateway.cpp:873, 4731, 5396, 5402, 6774, 8042` | SLICE-B |
| A4 | `publishedRoleForCleaner_` (:2065) | owner | cleaner `LogCleanerAgent.h:395,616,709,820`; gateway `GatewayReplicaView::publishedLeader()` (GatewayReplicaView.h:64-67) | SLICE-B |
| A5 | `publishedIsrSizeForGateway_` (:2066) | owner | gateway `drainPendingAcks` `ClientGateway.cpp:6770` | SLICE-B |
| A6 | `publishedLsoForCleaner_` (:2068, `publishCleanerLso` :853, driven by `Worker::publishCleanerLso_` Worker.h:5584) | owner | cleaner `LogCleanerAgent.h:455`; **owner** `cleanerSwapBound()` (PartitionReplica.h:880) | CLEANER |
| A7 | `publishedMarkerBoundForCleaner_` (:2071, store .cpp:2757) | owner | cleaner `LogCleanerAgent.h:666` | CLEANER |
| A8 | `cleanerRewindGen_/To_/Ack_` (:1747-1749, PartitionReplica.h:913-928) | owner (gen/to), cleaner (ack) | each other | CLEANER |
| A9 | `lastCleanOffset_` (:1746) | cleaner | cleaner, control `NodeRuntimeAgent.h:841`, tests | SLICE-B |
| A10 | `ClientGateway::minIsr_` (ClientGateway.h:2849, `setMinIsr` :711) | control at boot (BrokerNode.cpp:2155), bench thread | control (`ClientGateway.cpp:667`) | SLICE-B |
| A11 | `InMemoryTransport::mu_` (PartitionReplica.h:2307), a **mutex** | test fake, and the production single-node fallback (BrokerNode.cpp:1173) | — | SLICE-B |
| A12 | `RoutingImageHolder` atomic `shared_ptr` (RoutingImage.h:222-257) | — | **no production user** (grep) | UNSCOPED |
| A13 | `#include <atomic>` with no use: Worker.h, GroupCoordinator.h | — | — | UNSCOPED |
| A14 | `LogCleanerAgent::cyclesDone_` (LogCleanerAgent.h:1095) | cleaner | tests | CLEANER |

Out of scope, and listed so they are not forgotten:
- The storage-level cleaner channel in `Log.h/.cpp` (swap/delete/msg/rename rings, the D22 triple
  buffer, `cleanerPersistedRp_`, `cleanerFailedErrno_`). These are ring implementations or the
  user-sanctioned CLEANER channel.
- The scalar pair `cleanerPersistedRp_` / `cleanerFailedErrno_` becomes follow-up FU-1 (convert onto
  the facts ring of §3.2).

### 1.2 Hot-path cost today

`PartitionReplica::doWork` ends with `publishCleanerState()` (.cpp:2663) on **every pass for every
replica**. That is 6 release stores, a loop over `configuredReplicas_`, and up to RF-1 follower
stores, whether or not anything changed. The control thread then does `replicas × 32` acquire loads
per scan (NodeRuntimeAgent.h:467-479). That is 32 768 loads per scan at 1024 replicas. Both scale
with *owned* replicas, not with *activity*. This feeds the T23 I3 idle-pass problem (see
`2026-10-19-activity-driven-worker-pass-design.md`).

## 2. Approach

**Chosen: change-gated, conflated *facts* records on dedicated one-to-one rings, one per owner
thread and consumer.** Each consumer keeps a plain table that only it owns.

- Owner → control: `FactsRing` (OneToOneByteRing, 256 KiB). There is one per worker and one for the
  admin thread. Control-owned replicas (internal partitions, the inline path) write the control table
  directly, because they are on the same thread and need no ring.
- Owner → cleaner: `CleanerFactsRing` (OneToOneByteRing, 256 KiB). There is one per worker, one for
  the admin thread and one for the control thread. This replaces A3, A4 (cleaner side), A6, A7 and A8.
- Control-side consumer: `ReplicaFactsTable`, a plain array indexed by registry id and stamped with
  the incarnation. Gateway and NodeRuntimeAgent read it on the control thread. Cleaner-side
  consumer: `CleanerReplicaState`, a plain field set on the cleaner's existing per-replica view
  entry.

Rejected:
- **Reuse `ret_`, the worker→control return ring.** `ret_` carries ProduceResult and PushRead
  answers, and its free-slot reservation gates intake (Worker.h:1604, 1721). Facts traffic would steal
  reservation and add head-of-line delay to acks. A separate ring isolates them.
- **Seqlock or triple buffer per replica.** It is still atomics as coordination, it needs one buffer
  per replica, and it does not remove the control thread's O(replicas) scan.
- **Request/response polling (control asks, worker answers).** It doubles traffic, and the control
  thread would still need a per-scan request loop.
- **Route cleaner facts via control.** It adds a hop and control load. The cleaner is a separate
  consumer, so it gets its own ring (shared-nothing, single producer).

## 3. Messages, coalescing and state machines

### 3.1 Owner → control `ReplicaFacts` (type 1, variable length)

```
u32 registryId | u32 incarnation | i32 leaderEpoch | u8 role | u8 flags | u16 isrSize
i64 servedHw | i64 selfLeo | u64 factsSeq | u8 nFollowers | pad
nFollowers × { i16 nodeId | pad | i64 leo }        // validated followers only (CRITICAL-1 rule)
```
- `flags`: bit0 `followersCleared` (replaces `clearPublishedFollowerPositions_`); bit1 `retired`
  (the final record when the owner drops the replica).
- `factsSeq`: monotone per replica. The consumer ignores a record whose
  `(registryId, incarnation, factsSeq)` is not newer than the one it holds.
- The size is at most 40 + 32×16 bytes, far below `maxMsgLength`.

### 3.2 Owner → cleaner (`CleanerFactsRing`)

| type | body | replaces |
|---|---|---|
| `CleanerFacts` (1) | regId, inc, role, servedHw, lso, markerBound, factsSeq | A3/A4/A6/A7 |
| `CleanerRewind` (2) | regId, inc, rewindTo | A8 (gen/ack protocol) |

Cleaner → control uses the existing `CleanerViewChannel::ack` ring:
- `CleanWatermark` (type 4): regId, inc, lastCleanOffset. This replaces A9 reads on control.
- `CycleDone` (type 5): a counter. This replaces A14, and tests wait on the control-side count.

### 3.3 Owner-side publisher: `FactsSlot` (per owned replica, preallocated in the worker's slot array)

The state machine is per slot and per consumer. Each state holds only what it needs:

```
Clean{ lastSent }                          – nothing to send
EdgeDirty{ lastSent }                      – an edge changed: send on this pass's flush
ProgressDirty{ lastSent, dueNs }           – only monotone progress changed: send at dueNs
Retired{ }                                 – final `retired` record is owed, then slot freed
```
- **Edge** changes: role, epoch, ISR size, a follower validated or cleared, `followersCleared`, any
  *decrease* of `lso` or `markerBound`, retirement.
- **Progress** changes: `servedHw`, `selfLeo` or follower LEO increasing, and `lso` or
  `markerBound` increasing.

**Change detection.** The 19 `publishCleanerState()` call sites in PartitionReplica.cpp (plus Worker callers) become `noteFacts_()`. It compares
the replica's current values with `lastSent` field by field, using plain loads from the owner's own
state, and moves the slot Clean→Edge/Progress. No store happens when nothing changed. The per-pass
call in `doWork` (.cpp:2663) is deleted. Every site that can change a fact already calls
`noteFacts_()`, which is the existing invariant of `publishCleanerState`.

**Flush.** This is one step at the end of the worker pass, walking the *dirty list* (an intrusive
index FIFO with no allocation). EdgeDirty slots are sent now. ProgressDirty slots are sent when
`now ≥ dueNs`. A `ProgressDirty` slot that gets an edge is promoted to EdgeDirty. The record encodes
the values **at flush time**, not at change time, so the latest value always wins.

**Conflation and backlog bound.** A slot is in the dirty list at most once. A progress record per
replica goes out at most once per `kProgressFactsNs` (1 ms default). A full ring stops the flush:
the slot stays dirty and keeps its latest values, so nothing is lost, it is only delayed. The ring
holds at most `owned × (1 + drainLatency / kProgressFactsNs)` records. With 256 KiB and 64-byte
records that is 4096 records: enough for 1024 owned replicas at a 3 ms control stall. The sizing is
pinned by a `static_assert` against `kMaxOwnedPerWorker`.

### 3.4 Consumers

`ReplicaFactsTable` (control) holds a `FactsEntry{incarnation, factsSeq, epoch, role, isrSize, hw,
selfLeo, followers[kMaxRf]}` per registry id, sized from registry capacity at boot. A
registry-capacity growth is cold: it reallocates on the control thread outside the scan.

On every record the control thread does two things:
- It applies the record.
- It calls `MetadataService::reportReplicaPosition` **only for the nodes in the record**, which
  replaces the `replicas × 32` scan. An incarnation mismatch, from a retired id reused, drops the
  record.

`GatewayReplicaView` loses its `replica_->…` snapshot accessors and reads the table instead. A
`static_assert(!ExposesPublishedAtomics<PartitionReplica>)` concept pins the removal (§6).

### 3.5 Gateway readers reclassified

| Site | Owner of the replica there | New source |
|---|---|---|
| ClientGateway.cpp:873 (RC rebuild scan) | control (worker-routed are skipped at :864) | live `highWatermark()` (same thread) |
| :6770/:6774 `drainPendingAcks` | control (the inline path, see :6752) | live `isr().size()` / `highWatermark()` |
| :4731 subscribe clamp, :8042 group-subscribe clamp | worker | **the clamp moves to the owning worker** (the op already goes there, `raiseFreshDiskCursor_`). The worker clamps with its live HW. A stale-low table HW would otherwise clamp a `fromOffset` downward and deliver records the client did not ask for. |
| :5396/:5402 gateway push loop | gateway-owned or transient | gateway-owned: live. Worker-owned: the worker pushes in shard mode. The implementation step asserts that the gateway never pushes a worker-owned partition. If it does, use the table HW, where stale-low is safe. |
| `publishedLeader()` | worker | table role. Stale semantics are unchanged (documented at GatewayReplicaView.h:58-63). |

### 3.6 Cleaner readers

`LogCleanerAgent` drains its CleanerFactsRings at pass start, in place of `applyCleanerRewind()`.
It then reads `CleanerReplicaState` plainly. Owner-side validation stays authoritative and is
widened, because ring staleness can exceed the old atomics' window:
- `cleanerSwapBound()` uses the owner's **live** LSO from RcState instead of A6.
- The swap record carries the `markerBound` the cleaner used. The owner rejects the swap if its live
  bound is lower, the same as an HW/LSO rejection, and posts a `CleanerRewind`.

## 4. Failure and timing analysis

- **Staleness bounds.** Edge facts reach control within one worker pass plus one control drain:
  ≤ 2 passes, typically < 20 µs fused and < 1 ms threaded. Progress facts arrive within
  `kProgressFactsNs` plus the drain. Cleaner facts add the cleaner idle backoff, up to its park
  (≤ 1 ms).
- **ISR shrink/expand timing: unchanged.** The ISR is decided on the owning worker from
  `onFetchRequest` times (`updateIsr_`, .cpp:2567). It never read A1-A5.
- **HW advance: unchanged.** HW is recomputed on the owner (`recomputeHw`, .cpp:2519). Acks=all fire
  on the worker (`flushOwedAcksAll_`). The inline-path drain now reads live state on its own thread,
  which removes the old one-pass delay.
- **Controller restoration (DynamicBalancer).** Positions arrive up to `kProgressFactsNs` later than
  before. The balancer's decisions are position-gated and idempotent per scan (NodeRuntimeAgent.h:
  452-463), so later positions mean later restoration, never a wrong one. Its windows are hundreds of
  ms against a 1 ms delay.
- **Retire and reuse race.** The worker sends `retired` before acking the ownership generation. The
  control thread frees the registry id only after the ack (the existing gate), so a record for an old
  incarnation either arrives before the free or is dropped on the incarnation mismatch.
- **Leadership change.** `followersCleared` is an *edge* bit that rides the next record, so control
  never mixes old-leadership follower LEOs with the new epoch. CRITICAL-1 is preserved: the record
  carries `leaderEpoch`, and positions are reported with that epoch.
- **Ring full / control stalled.** Records are delayed, never lost (latest value is held). There is
  no halt path, because a facts record is never an invariant.
- **Cleaner stalled.** The owner keeps its latest CleanerFacts. A rewind conflates as
  `min(pendingRewind, new)`, which is never lost and at worst conservative, the same as A8 today.

## 5. Migration order (each step green on its own)

1. Delete the dead code: A12 `RoutingImageHolder`, the A13 unused includes. Make A10 a plain `int`,
   set before the runners start; the bench sets it before start. Move A11 `InMemoryTransport` to
   `replication/testing/` (test target). The production single-node fallback becomes
   `NullPartitionTransport`, since it already drops every frame with no peers.
2. Add `FactsSlot`/`noteFacts_` and the control `FactsRing` + `ReplicaFactsTable`. NodeRuntimeAgent
   reads the table (A1/A2). The atomics stay written in parallel for one step, and a debug-build
   cross-check asserts table == atomic after a quiesced drain.
3. Move the gateway readers (§3.5), including the subscribe clamp to the worker. Delete A3-A5 gateway
   accessors.
4. Add the cleaner ring (§3.2, §3.6) and widen owner validation. Delete A3/A4 cleaner accessors and
   A6-A9, A14.
5. Delete the atomics and `publishCleanerState`, and remove every SLICE-B and the matching CLEANER
   allowlist lines.

## 6. Test plan

Regression tests (named `*Regression*`):
- `ReplicaFactsNoAtomicsRegression`: `scripts/check_no_atomics.py` with the SLICE-B entries
  removed (it already runs as `NoAtomicsOrMutexesOutsideAllowlistRegression`). Any reintroduced
  `std::atomic`/`memory_order`/`mutex` in PartitionReplica, ClientGateway or Worker fails ctest.
  Also concept guards: `static_assert(!requires(const PartitionReplica& r){ r.controllerReplicaLeo(0); })`,
  and the same for `gatewayHighWatermark`, `cleanerHighWatermark`, `publishCleanerState`. A deleted
  accessor cannot come back silently.
- `ReplicaFactsConflationRegression`: 10⁶ HW bumps with a stalled consumer. The ring never exceeds
  the bound, and after the drain the table equals the owner's final state.
- `ReplicaFactsIncarnationRegression`: retire and re-add the same (topic, partition) while a record
  is in flight. A stale record never lands.
- `SubscribeClampOnOwnerRegression`: produce, ack, then subscribe from `ack+1` immediately. The
  first delivered offset equals `ack+1`, never lower.
- `CleanerMarkerBoundStaleRegression`: a marker bound regresses (new leadership) while a cleaner
  pass holds the old bound. The owner rejects the swap, and a rewind follows.
- `CleanerLsoOpenTxnRegression`: a txn opens between the cleaner's facts read and its install. The
  install is rejected.

Sanitizers: the full `*Regression*` suite under TSan (broker build-tsan, client build_tsan) with
**no new suppressions**, plus ASan+UBSan.

### Interaction matrix (op × entity × timing)

- **Ops:** produce append; follower fetch; ISR shrink; ISR expand; becomeLeader; becomeFollower;
  epoch bump; txn open/commit/abort; compaction install/reject; retention; replica add; replica
  retire; incarnation reuse; subscribe; group subscribe.
- **Entities:** disk leader; disk follower; transient leader/follower; internal partition
  (control-owned); `__catalog`/`__security` (admin-owned); single-node; RF=3.
- **Timings:** before the first record; facts ring full; control stalled 5 ms; cleaner mid-pass;
  progress-interval boundary; the same pass as a role flip; after retire, before the ownership ack.

Every cell is a parameterized test in `ReplicaFactsMatrixTest`, unit-level against a fake owner and
consumer. The leadership, retire and stall columns also run at integration level, in a real
3-broker in-process cluster. A model-checked stress (randomized op/timing interleavings, 10⁵ seeds)
compares the consumer table against the owner oracle.

E2E (k3s VM, `flock /root/vm.lock`): kill and return a broker. Leadership is restored within
the balancer bound, the ISR re-expands, and no TSan output appears from the sanitizer image.

## 7. Bench gates

| Bench | Gate |
|---|---|
| `BM_WorkerPassIdle/owned:{1,64,1024}` (new, in FusedRuntimePassBench) | idle pass ≤ baseline − publishCleanerState share. 0 facts records when idle. |
| `BM_FactsFlush/dirty:{1,64,1024}` | ≤ 40 ns per dirty replica. 0 allocations (alloc-counting hook). |
| `BM_ControlPositionRefresh/replicas:1024` | ≥ 20× faster than the `×32` scan baseline |
| produce→deliver (MultiLeaderProduceBench, e2e) | p50 ≤ 50 µs, p99 ≤ 200 µs, no regression > 2 % vs the pre-change baseline |
| DutyPassLatencyGateTest | all budgets unchanged or better |

## 8. Implementation steps (for Sonnet)

1. Cleanup slice: A10-A13 (plain `minIsr`, `InMemoryTransport` → testing/ with
   `NullPartitionTransport` fallback, delete `RoutingImageHolder` and the unused includes).
   Update the allowlist and add the concept-guard test skeleton.
2. `FactsSlot` + dirty list + `noteFacts_` replacing every `publishCleanerState()` site (keep the
   atomics written in parallel). Unit tests and the conflation test.
3. Control `FactsRing` per worker/admin + `ReplicaFactsTable`. NodeRuntimeAgent position refresh
   from records, with the debug cross-check. Incarnation test.
4. Gateway readers (§3.5) plus the worker-side subscribe/group-subscribe clamp. Delete the gateway
   accessors. Clamp regression.
5. `CleanerFactsRing` + `CleanerReplicaState` + rewind/watermark/cycle messages. Widened owner
   validation. Cleaner regressions.
6. Delete the atomics and `publishCleanerState`, and shrink the allowlist. Matrix test, TSan/ASan
   full suite, benches, k3s e2e.

## 9. Decisions made (no user input needed)

- Separate facts rings, not `ret_`. One ring per (owner thread, consumer).
- 1 ms progress conflation. Edges are immediate.
- The subscribe clamp moves to the owning worker instead of reading a possibly-stale HW.
- The marker bound and LSO join the ring as well, even though the CLEANER category allows them: they
  are written at the same site, and the widened owner validation makes the ring staleness safe.
- `InMemoryTransport` leaves production. Single-node uses `NullPartitionTransport`.
- FU-1: `cleanerPersistedRp_` / `cleanerFailedErrno_` (storage) follow on the same cleaner ring
  later.
