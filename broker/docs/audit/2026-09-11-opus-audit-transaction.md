# Opus Audit — libs/transaction (Phase 5: P5-1..P5-3)

Date: 2026-09-11
Scope: `libs/transaction/{include,src,tests}` + cross-ref `libs/storage/…/ProducerStateManager.h`
Mode: read-only. No code modified.

## Verdict: **FINDINGS** (multiple, incl. 2 BLOCKER-class EOS/atomicity holes beyond the 3 known)

Known issues already being fixed (excluded from counts below):
- (a) TransactionLog not wired into coordinator commit path (durability seam).
- (b) near-vacuous `CoordinatorEpochPersisted` test.
- (c) SBE var-length readers lack bounds checks.

## Counts (additional issues)
- BLOCKER: 2
- CORRECTNESS: 4
- SILENT: 3
- CLEAN: 4
- TEST: 4

---

## BLOCKER

### B1 — Fenced-abort and timeout-abort write ZERO control markers → atomicity hole
`src/TransactionCoordinator.cpp:86-97` (initProducerId epoch-fence) and `:238-254` (doWork timeout).

Both paths set `state = PrepareAbort` **in place**, without returning the partition
set and without any hook to emit ABORT control batches. The ONLY marker-writing
path is `ControlMarker::writeMarkers` (`src/ControlMarker.cpp:238-268`), which calls
`coordinator.endTransaction(...)` — and `endTransaction` (`:184-187`) rejects any
state that is not `Ongoing` (returns `kInvalidTransition`, `writeMarkers` returns 0).

Consequence: a transaction aborted by (i) a zombie epoch-fence during re-init, or
(ii) a `doWork` timeout, transitions Ongoing→PrepareAbort, then `completeTransaction`
(`:222-230`) clears partitions and drops it to Empty — **without a single ABORT
marker ever written to the involved partitions.** Read-committed consumers never
see the txn resolved: the partition LSO is stuck (data hangs) or, depending on the
read-committed reader, the partial data leaks. This is a direct EOS/atomicity
violation and the highest-priority defect. Tests `ReInitOngoingThenCompleteResetsToEmpty`
and `FullLifecycleWithTimeoutAbort` "pass" only because they never assert markers were written.

Fix direction: fence/timeout aborts must surface the partition set (return it, or
enqueue an abort job) and drive marker emission with the *bumped* epoch before
`completeTransaction` clears state; `writeMarkers`/an equivalent must be reachable
from a `PrepareAbort` that was entered by a non-endTransaction path.

### B2 — Recovered Prepare* state is unresolvable (no epoch/coordinatorEpoch to stamp markers)
`src/ControlMarker.cpp:238-268`, `include/.../TransactionCoordinator.h` (no log field),
`src/TransactionLog.cpp` (never called by coordinator).

`writeMarkers` takes `producerId/producerEpoch/coordinatorEpoch` from the *live client
request*. After a coordinator restart mid-`PrepareCommit`/`PrepareAbort`, that request
no longer exists, and the coordinator holds no `TransactionLog`, so there is no way to
(a) rebuild txnMap on boot or (b) re-emit markers for an in-flight Prepare* with the
correct epoch. The recovery replay described in `TransactionLog.h:12-16,39-41` has no
producer. Even once (a)/known-fix wires the log, resolving a recovered Prepare* still
needs the epoch stored in metadata, not passed by a caller. Combined with B1 this
means neither crash nor failover can complete an in-flight abort.

Fix direction: coordinator owns a `TransactionLog`; append on every transition; on
boot replay to rebuild txnMap; a resolver walks Prepare* entries and emits markers
using the epoch/coordinatorEpoch from persisted metadata (add coordinatorEpoch to
`TransactionMetadata`).

---

## CORRECTNESS

### C1 — Epoch overflow wraps to 0, un-fencing zombies
`src/TransactionCoordinator.cpp:82-84`. At `INT16_MAX` the "saturating" branch resets
the epoch to **0**. A long-lived zombie holding epoch 0 (or any value the wrap now
collides with) is no longer fenced by `validateEpochAndPid` (`:311-324`). Kafka
allocates a NEW producerId when the epoch space is exhausted rather than wrapping.
Fix direction: on exhaustion allocate a fresh PID (and reset epoch to 0 for the new PID),
never reuse the old PID with a wrapped epoch.

### C2 — CompleteCommit/CompleteAbort states are never observable; log can't record them
`src/TransactionCoordinator.cpp:212-230`. `completeTransaction` writes
`state = CompleteCommit` then immediately overwrites with `state = Empty` (same for
Abort). The Complete* states are dead. When the log is wired (known fix a), the
coordinator will jump Prepare*→Empty and NEVER persist a Complete* transition — yet
`TransactionLogTest.FullLifecycleReplay`/`AbortLifecycleReplay` assert Complete* is a
real replayable state. The intended durable contract (Prepare→Complete→Empty as
separate records, so a crash between marker-write and Empty is recoverable) is broken:
there is no persisted "markers done" record distinct from "txn cleared".
Fix direction: persist the Complete* transition as its own record before clearing to
Empty (or keep Complete* as the terminal-until-next-init state as Kafka does).

### C3 — Epoch-bump while Prepare*/Complete* does not fence the in-flight producer's marker epoch
`src/TransactionCoordinator.cpp:99-111`. When re-init lands on `PrepareCommit`/
`PrepareAbort`/`CompleteCommit`/`CompleteAbort`, the epoch is bumped in metadata but the
in-flight transition is left to complete with `writeMarkers` still being called by the
original caller using the OLD epoch it holds. Because `validateEpochAndPid` now expects
the bumped epoch, the original `endTransaction`/marker completion for that in-flight txn
will be fenced (`kInvalidProducerEpoch`) — the in-flight commit/abort can get stuck
(never completed, partitions never cleared) if the new producer never drives it.
Fix direction: mirror Kafka — reject concurrent InitProducerId while a txn is in a
pending (Prepare*) state with `CONCURRENT_TRANSACTIONS` (the error code already exists,
`InitProducerIdResult::kConcurrentTransactions`, but is never returned anywhere), rather
than silently bumping the epoch mid-flight.

### C4 — `kConcurrentTransactions` defined but never returned; `initProducerId` never rejects a pending txn
`include/.../TransactionCoordinator.h:85`, `src/TransactionCoordinator.cpp:99-111`.
The documented `kConcurrentTransactions` path (init while non-terminal) is unimplemented;
init always succeeds and mutates state. Invariant "no new epoch while a txn is pending"
is not enforced. Ties into C3. Fix direction: return kConcurrentTransactions for
Prepare*/Complete* re-init (or the Kafka retry semantics).

---

## SILENT

### S1 — `isControlBatch`/`extract` never verify CRC or bounds → control vs data batch confusable
`src/ControlMarker.cpp:181-232`. `isControlBatch` checks only magic byte and the CONTROL
attribute bit; it does not verify the batch CRC (`CRC_OFFSET`) nor take a buffer length.
Any corrupt/truncated data batch whose byte 16 == 2 and whose byte 21 has bit 5 set is
accepted as a control batch, and `extract` then reads producerId/epoch/varints past the
end with no length guard (`read_varint_zigzag` at `:46-56` loops on the continuation bit
with no bound). Under read-committed this can mis-resolve a data batch as a
commit/abort marker (leak or hide records). Fix direction: pass an available-length,
bounds-check every field/varint, and verify the batch CRC before trusting the CONTROL bit.

### S2 — `controlType` also encodable in attribute bit 6, but decode ignores it (two sources of truth)
`include/.../ControlMarker.h:120-124`, `src/ControlMarker.cpp:138` vs `:213-221`.
The header reserves `kAttrCommit = 0x0040` (bit 6 = commit/abort) and comments describe
encoding controlType in attributes, but `build` never sets it and `extract` reads
controlType only from the record key. A peer/tool that trusts the attribute bit (as the
comments invite) would read every marker as COMMIT. Commit/abort become confusable across
implementations. Fix direction: pick ONE authority (record key), delete the bit-6
attribute path and its comment, or set+validate it consistently.

### S3 — `doWork` timeout compares against wall/monotonic `nowNs` with no lower bound on txnStartNs
`src/TransactionCoordinator.cpp:246-247`. `nowNs - meta.txnStartNs > timeoutNs` with
`txnStartNs` a plain injected value; if a replayed/reconstructed metadata carries
`txnStartNs == 0` while `state == Ongoing` (possible after a future log replay of an
Ongoing record), the first `doWork` with a real clock instantly aborts it. Units are
correct (ms→ns), boundary is correctly strict `>`. Fix direction: on replay, treat an
Ongoing entry's txnStartNs relative to recovery time, or persist an absolute deadline.

---

## CLEAN

### CL1 — Dead write in `completeTransaction`
`src/TransactionCoordinator.cpp:213,218` and `:223,228`: `state = CompleteCommit`/`CompleteAbort`
is immediately overwritten by `state = Empty`. Misleading (implies Complete* is entered). See C2.

### CL2 — `kMagic` comment wrong
`include/.../TransactionLog.h:20,116`: comment says `kMagic = 0x54584E4C = "TXNL"`; the
constant is `0x544B4E4Cu` ("LNKT" LE, doc says "TKNL"). Constant is self-consistent; the
`= "TXNL"` / `0x54584E4C` doc line is simply wrong. Cosmetic but confusing for on-disk debugging.

### CL3 — `TransactionLog.h` header layout comment does not match encoder
`include/.../TransactionLog.h:24-36` documents a `[27] uint8 coordinatorEpoch (truncated)`
plus a separate `[28..31] int32 coordinatorEpoch`, i.e. two coordinatorEpoch fields and
`kFixedHeaderSize=44` with pads at 40-43. The actual encoder (`src/TransactionLog.cpp:154-185`)
writes ONE coordinatorEpoch at [28..31], `p[27]=0` pad, pads at [40..43]. Header doc is stale.

### CL4 — `nowNs` parameter unused in `endTransaction`
`src/TransactionCoordinator.cpp:172` (`/*nowNs*/`). Fine as-is, but the marker/commit path
arguably should stamp `maxTimestamp`/deadline; the unused param signals an unfinished seam.

---

## TEST (vacuous / false-green)

### T1 — Abort/fence lifecycle tests never assert markers were written
`TransactionCoordinatorTest.cpp:189-207` (`ReInitOngoingThenCompleteResetsToEmpty`),
`:682-710` (`FullLifecycleWithTimeoutAbort`). Both reach PrepareAbort via a non-endTransaction
path then call `completeTransaction` and assert Empty — masking B1 (no markers ever produced).
Add an assertion that ABORT markers were emitted for each partition.

### T2 — `AddPartitionsWhenCompleteCommitIsInvalidTransition` is a no-op `SUCCEED()`
`TransactionCoordinatorTest.cpp:316-327`. Comment admits CompleteCommit is unreachable and
skips the test with `SUCCEED()`. Exercises nothing. (Also confirms C2: Complete* is unreachable.)

### T3 — `TransactionLogTest.CoordinatorEpochPersisted` does not verify coordinatorEpoch
`TransactionLogTest.cpp:388-405` (known b). Asserts only producerId/state; never reads back
coordinatorEpoch (which is not exposed on `TransactionMetadata`, so it CANNOT be verified —
the field is written to disk and dropped on replay: see `applyEntry` `:120-131` never sets it).
So coordinatorEpoch is effectively write-only / lost on recovery — a real recovery gap, not
just a weak test. Fix: add coordinatorEpoch to TransactionMetadata and assert round-trip.

### T4 — Control-marker tests never feed a corrupt/short buffer to `isControlBatch`/`extract`
`ControlMarkerTest.cpp`. `ZeroBytesNotControl`/`NormalBatchNotControl` use full-size zeroed
buffers; nothing tests a truncated buffer, a magic=2 + control-bit data batch with garbage
varints, or CRC corruption — the exact inputs that trigger S1. Add adversarial-buffer tests.

---

## Notes on ProducerStateManager cross-ref (P5-1)
Idempotent dedup epoch semantics (`>` resets, `<` fences) look consistent with the
coordinator's epoch model. One integration risk worth a test: the coordinator's epoch
wrap-to-0 (C1) would, via a reused PID+epoch-0, let `ProducerStateManager` treat a zombie
as a fresh session (epoch not `<` tracked) and accept its sequences — reinforcing C1's severity.

## Trace summary
initProducerId → addPartitions → endTransaction → writeMarkers → completeTransaction is
sound for the *happy commit/abort* path. The state machine breaks specifically on the two
abort entry points that bypass `endTransaction` (fence + timeout): they leave partitions
unmarked (B1) and, with no log wiring + no persisted epoch (B2/T3), are unrecoverable after
a crash/failover. Epoch overflow (C1) and the never-returned `kConcurrentTransactions`
(C3/C4) are the remaining correctness gaps.
