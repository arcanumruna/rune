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

---

# Re-audit (commit e66a07e)

Date: 2026-09-11. Mode: read-only, no code modified. Built + ran the suite:
`rune_transaction_tests` — **196/196 PASS**. Verified each fix against current source.

## Verdict: **FINDINGS REMAIN** — 1 NEW CORRECTNESS (narrow but reachable atomicity hole)

## Per-finding status

### B1 — FIXED
`initProducerId` Ongoing-fence (`TransactionCoordinator.cpp:136-150`) and `doWork` timeout
(`:299-318`) both set `state=PrepareAbort`, `markersPending=true`, and persist PrepareAbort
BEFORE any marker. `ControlMarker::resolvePendingMarkers` (`ControlMarker.cpp:307-331`) walks
`pendingMarkerTransactions()` and writes an ABORT batch to EVERY persisted partition using the
PERSISTED `producerEpoch`+`coordinatorEpoch` (`pendingMarkerTransactions`, `:323-342`), then
`completeTransaction` advances PrepareAbort→CompleteAbort→Empty. Tests are real and assert one
ABORT marker per partition with correct type + no-partition-left-unmarked
(`FenceAbortResolvePendingWritesAbortMarkersToAllPartitions`, `TimeoutAbort…`, `…NoPartitionLeftUnmarked`,
`_T1` upgrades). Resolve path is driven by the caller and reachable.

### B2 — FIXED
Coordinator ctor replays `txnLog_->all()` and stamps recovered PrepareAbort/PrepareCommit with
`markersPending=true` (`:38-47`). Recovered epochs are the PERSISTED ones (`applyEntry` sets
`coordinatorEpoch`, `TransactionLog.cpp:129`; producerEpoch `:124`). Failover tests
(`TxnCoordAuditB2Test.RecoveredPrepare{Abort,Commit}ResolvableWithPersistedEpochs`) assert the
pending descriptor + emitted marker carry the persisted producerEpoch AND coordinatorEpoch, not a
live request. Real.

### C1 — PARTIAL (see NEW-1)
At INT16_MAX a fresh PID (epoch 0) is allocated and the old PID is retired
(`:126-134`; tests `EpochOverflowAllocatesNewPid`, `EpochOverflowOldPidRetired` → old PID → kInvalidPid).
No wrap-to-0 on the same PID. BUT the overflow branch runs before the Ongoing-fence branch and,
when both fire together, stamps the ABORT marker with the NEW pid — see NEW-1.

### C2 — FIXED
`completeTransaction` (`:256-294`) writes CompleteCommit/CompleteAbort as a DISTINCT persisted
record before writing Empty. Recovery of a CompleteCommit state does NOT re-mark: it is not
Prepare*, so `pendingMarkerTransactions` skips it (`:329-330`) →
`RecoveryDoesNotDuplicateMarkersOnCompleteCommitState` asserts 0 markers on re-resolve. Real,
idempotent completion.

### C3/C4 — FIXED
`initProducerId` returns `kConcurrentTransactions` (=6) for Prepare*/Complete*(/PrepareEpochFence)
re-init without mutating state/epoch (`:113-121`). Tests: `ReInitWhilePrepareCommit…`,
`…PrepareAbort…`, `…CompleteCommit…ReturnsConcurrentTransactions` (state+epoch unchanged),
`ReInitAfterResolveSucceeds`. In-flight commit no longer stranded by silent epoch bump.
(Note: `PrepareEpochFence` is listed in the guard but never assigned anywhere — dead-but-harmless.)

### S1 — FIXED
`isControlBatch` (`ControlMarker.cpp:196-212`) requires `avail>=ENCODED_SIZE`, verifies CRC32C over
[21..75) BEFORE trusting the CONTROL bit, then checks the CONTROL bit. A normal DATA batch has the
CONTROL bit clear → rejected; a bit-flipped/garbage batch fails CRC → rejected. `extract`
(`:217-266`) uses bounds-checked zig-zag varints (`read_varint_zigzag_safe`, `:52-71`, 10-byte cap +
`pos>=end` guard) and length-guards every field. Adversarial tests real: Truncated, CrcCorrupted,
DataBatchWithControlBitButWrongCrc, ExtractTruncated/CrcFlipped. S2 (bit-6 dual source) also removed
(`ControlMarker.h:118-125` — bit 6 not set, not read).
Residual (LOW, pre-existing API shape): `isControlBatch`/`extract` default `avail = ENCODED_SIZE`
(`ControlMarker.h:163,180`); a caller that omits `avail` on a <75-byte buffer would over-read. All
in-tree callers pass full-size batches, so not exploitable today — but the default invites misuse.

## Deferral

### S3 — DEFERRAL-OK
Claim scrutinized: a recovered Ongoing with `txnStartNs==0` would be instantly timeout-aborted by
`doWork`. In practice (a) `txnStartNs` IS persisted and recovered (`TransactionLog.cpp:176,261,127`),
and `addPartitions` always sets it before persisting Empty→Ongoing (`:193-204`), so a real Ongoing
carries a non-zero start; `txnStartNs==0`+Ongoing is not reachable through the public API. (b) Even
if it occurred, aborting is SAFE and EOS-correct: `Ongoing` means `endTransaction` was never called,
so the client was never told commit began — the coordinator is free to abort and the client retries.
Atomicity is only at risk once state≥PrepareCommit, and those recover as Prepare* (resolved with
markers), never as Ongoing. No EOS/atomicity violation. Aggressive-but-correct stands. DEFERRAL-OK.

## Regression sweep (new fixes)

- Double markers (client path vs resolve): NONE in a single coordinator lifetime — `endTransaction`
  sets `markersPending=false`, so a client-driven txn never appears in `pendingMarkerTransactions`.
  A crash mid-marker-write re-emits markers to all partitions on recovery (markersPending derived from
  recovered Prepare* state), which is at-least-once/idempotent marker delivery (Kafka-faithful; consumer
  dedups by pid+epoch). Not a corruption path.
- Persisting Complete*: the normal EndTxn path never lands on Complete* via addPartitions/endTransaction
  (both reject non-Empty/non-Ongoing); Complete* is only produced by `completeTransaction` then
  immediately superseded by Empty in memory. No mishandling.
- Epoch-overflow→new-PID and txnId→PID mapping: mapping is by txnId key (unchanged); old PID fenced by
  `validateEpochAndPid` PID check. Fine EXCEPT the marker-stamping edge in NEW-1.
- Invalid transitions: none newly reachable (addPartitions/endTransaction/completeTransaction guards intact).

## NEW findings

### NEW-1 — Epoch-overflow DURING an Ongoing-fence stamps the ABORT marker with the NEW PID → in-flight data not aborted (CORRECTNESS / atomicity)
`TransactionCoordinator.cpp:126-150`. The overflow branch (`:128-131`) allocates `newPid` and sets
`newEpoch=0` BEFORE the Ongoing-fence branch (`:136-150`), which then sets `meta->producerId=newPid`,
`meta->producerEpoch=0`, `markersPending=true`. The subsequent ABORT marker is therefore built with
`newPid` (`pendingMarkerTransactions` reads `meta.producerId`), but the in-flight data on the involved
partitions was written under the OLD producerId. A read-committed consumer keys aborted-txn tracking by
producerId, so an ABORT marker carrying `newPid` does NOT abort the old PID's uncommitted records →
they can leak as committed (or the LSO logic misbehaves). Kafka avoids this: on overflow with an ongoing
txn it aborts using the EXISTING producerId (bumped/last epoch) and issues the new PID only after the
abort completes. Here the fresh PID is adopted before the abort marker is emitted.
Reachability: needs `producerEpoch==INT16_MAX` while `Ongoing` (≈32767 re-inits to exhaust the epoch,
then addPartitions, then one more init) — narrow but legitimately reachable for a long-lived
transactionalId; UNTESTED (C1 tests inject only `Empty` at INT16_MAX).
Fix direction: when fencing an Ongoing txn, emit the ABORT marker with the OLD producerId and the
old-epoch-derived fencing epoch FIRST (retain old pid for the pending-marker descriptor), and only
allocate/adopt the new PID after resolution — i.e. don't overwrite `meta->producerId` until the abort
markers for the old PID are done. Severity: CORRECTNESS (atomicity hole, narrow trigger).

## Re-audit counts
- FIXED: B1, B2, C2, C3, C4, S1(+S2)   PARTIAL: C1 (marker-PID edge)   DEFERRAL-OK: S3
- NEW CORRECTNESS: 1 (NEW-1)   NEW LOW: 1 (default `avail` over-read shape, S1 residual)
- Tests: real and asserting the right invariants; 196/196 pass.
Not CLEAN: NEW-1 is a reachable atomicity/leak path (abort marker with wrong producerId).

---

# Full re-audit + coverage (commit 207410f)

Date: 2026-09-11 · Mode: read-only, whole-library fresh audit (not a diff review) · Auditor: Opus
Scope: `libs/transaction/{include/rune/transaction/*.h, src/*.cpp, tests/**}` + cross-ref
`libs/storage/include/rune/storage/ProducerStateManager.h`.

## Part A — Bug audit (verdict: BUGS REMAIN)

Re-verified prior fixes: **B1, B2, C1, C2, C3/C4, S1/S2, NEW-1 all HOLD** with no regression.
The client-driven commit/abort path, fence-abort, timeout-abort, recovered-Prepare* resolution,
CompleteCommit/CompleteAbort persist-before-Empty, control-batch CRC/bounds/varint safety, and the
NEW-1 deferred-PID-swap-in-memory all behave as documented for the in-process (no-crash) case.
The findings below are whole-library issues, most NOT near the prior fixes.

### A1 — Allocator is NOT reseeded from the recovered log ⇒ producerId REUSE after failover (BLOCKER / EOS)
`TransactionCoordinator.cpp:25-48` (ctor) + `:86,95,130` (allocate calls).
The ctor replays `txnLog_->all()` into `txnMap_` but NEVER advances/seeds `allocator_` past the highest
recovered `producerId`. A production coordinator constructed as `TransactionCoordinator(ProducerIdAllocator{}, …, &log)`
(the default allocator starts at 0) will, after recovering a log whose live txns hold producerIds
0..N, hand out producerId 0,1,2,… AGAIN from `initProducerId` (new-txnId branch `:95`, overflow branch `:130`,
no-txnId branch `:86`). Two distinct producer sessions then share a producerId. `ProducerStateManager`
keys idempotent-dedup and aborted-txn tracking by `producerId` (ProducerStateManager.h:104-113, 163-168),
so a reused PID silently corrupts sequence/epoch dedup and read-committed LSO tracking → duplicate or
dropped records = exactly-once violation. The `ProducerIdAllocator` header even documents "P5-3 passes
recovered watermark" (`ProducerIdAllocator.h:44`) but no code computes or passes it — the wiring is absent.
The C1 overflow tests dodge this by hand-injecting `ProducerIdAllocator{100}`/`{200}`, which MASKS the bug
rather than exercising the default path. Fix direction: in the replay loop compute
`max(producerId)+1` (and ideally persist a durable PID high-water/next-block record) and seed
`allocator_` to it before serving any request. Severity: **BLOCKER** — silent EOS break on the core
failover path, reachable with the documented default construction.

### A2 — NEW-1 deferred PID swap (`pendingNewProducerId`) is in-memory only ⇒ lost across a crash mid-abort (CORRECTNESS)
`TransactionCoordinator.h:201-202`, set at `TransactionCoordinator.cpp:154-155`, consumed at `:316-321`;
NOT a field of `TxnLogEntry` (`TransactionLog.h:59-68`) and NOT written by `appendRecord`
(`TransactionLog.cpp:155-218`). Sequence: an epoch-overflow-during-Ongoing fence returns the FRESH pid to
the client (`:172`) and persists PrepareAbort under the OLD pid, stashing the fresh pid only in memory.
If the coordinator crashes before `completeTransaction`, recovery rebuilds PrepareAbort with the OLD pid and
`pendingNewProducerId == -1` (default). `resolvePendingMarkers`→`completeTransaction` then does NOT adopt
the fresh pid (`:316` guard false) and lands in Empty holding the OLD pid at epoch INT16_MAX. The client
(which was told it now owns the fresh pid) is fenced by `kInvalidPid` on its next call and must re-init;
the fresh pid returned pre-crash is orphaned and a subsequent re-init allocates yet another new pid
(compounding A1's leak). No aborted-record leak (the ABORT marker is correctly stamped with the OLD pid),
so this is CORRECTNESS not BLOCKER — but it is a stuck/ambiguous identity across the crash seam that Kafka
avoids by persisting the "next producer" intent. Fix: persist `pendingNewProducerId/Epoch` in the log
entry (or derive/adopt deterministically on recovery). UNTESTED (no crash-seam test injects Ongoing@INT16_MAX
then crashes before complete).

### A3 — Public SBE `decode()` methods use the UNSAFE readers ⇒ over-read on a truncated/corrupt frame (SILENT)
`TransactionMessages.h`: `AddPartitionsToTxnRequestMsg::decode` (`:359-369`), `EndTxnRequestMsg::decode`
(`:445-455`), `WriteTxnMarkersRequestMsg::decode` (`:542-554`), `InitProducerIdRequestMsg::decode`
(`:264-272`) all call the UNCHECKED `read_le`/`read_str`/`read_partitions` (`:84-131`) with no `end`/length
argument. The bounds-checked `read_str_safe`/`read_partitions_safe` (`:141-191`) exist but are wired into
NOTHING except direct unit tests. A short or malicious wire frame reaching any public `decode()` reads past
the buffer (heap over-read / crash / info leak). ControlMarker was hardened for exactly this class (S1) but
the SBE request decoders — the actual network ingress — were left on the unsafe path. Severity: SILENT
(over-read); becomes CORRECTNESS/DoS at the network boundary. Fix: give `decode()` a buffer length and route
through the `_safe` readers (return `optional`/bool), or delete the unsafe readers so they can't be called.

### A4 — `read_str` / `read_partitions` also unchecked against `blockLength`/header (SILENT, same root as A3)
`TransactionMessages.h:96-102, 118-131`. Even absent truncation, `read_partitions` trusts a wire-supplied
`uint16 count` and loops that many tuples with no cap; a crafted `count=65535` with a short body walks off
the buffer. Folded into A3's fix (route all decodes through the all-or-nothing `_safe` scan).

### A5 — Silent uint16 truncation of `txnIdLen` / `partitionCount` in the durable log and SBE (CORRECTNESS, boundary)
`TransactionLog.cpp:157-158` casts `txnId.size()`/`partitions.size()` to `uint16_t`; `TransactionMessages.h:90,108`
do the same for strings/partition counts. A txnId ≥ 65536 bytes or a partition set ≥ 65536 silently wraps the
length prefix → the record/message encodes a corrupt shorter length; on replay `crc32c` will (usually) reject
it as torn-tail and SILENTLY DISCARD the transition (data loss of a real state change, not a torn write).
No guard/assert rejects oversize inputs. Kafka bounds these; here it is unenforced. Severity: CORRECTNESS at
the (large but legal) boundary; SILENT truncation with no error return. Fix: validate sizes and fail loudly
before encoding.

### A6 — `crc32c` table init is not thread-safe; `crc32c_tbl_init`/`cm_crc32c_init` are plain `bool` (CLEAN-ish / latent)
`TransactionLog.cpp:37-50`, `ControlMarker.cpp:78-90`. Lazy one-time table fill guarded by a non-atomic bool.
The library is documented single-writer, so today this is benign; but `ControlMarker::build/extract` are
`static` and could plausibly be called from a non-writer thread (e.g. a read-committed consumer verifying a
batch), which would race the table init (torn table → wrong CRC → valid markers rejected). Prefer a
`static const` table built at load or a `std::once_flag`. Severity: CLEAN (latent; no current multi-thread caller found).

### A7 — Clean-code / invariant nits (CLEAN)
- `endTransaction` ignores `nowNs` (`TransactionCoordinator.cpp:240` param commented out): a commit/abort does
  NOT refresh/停 the timeout, and there is no re-validation that the txn hasn't already blown its deadline in
  the same tick. Harmless given doWork ordering, but the unused clock is a latent trap — document or use it.
- `doWork` mutates `meta` in place while iterating `txnMap_` (`:338-349`) — safe (no insert/erase), but relies
  on that invariant implicitly; a future "erase Dead txns" here would invalidate the iterator. Worth a comment.
- `InitProducerIdResult`/`AddPartitionsResult`/… duplicate the same error-code constants across four structs
  with slightly different subsets (e.g. `kConcurrentTransactions` only on Init) — a single shared enum would
  prevent divergence. Cosmetic.
- `TxnState::Dead` is defined, named, and handled as "re-create" in `initProducerId` (`:175` comment) but is
  NEVER entered anywhere (no expiry path). Dead code / unimplemented lifecycle. Not a bug, but the state
  machine is incomplete vs. the documented KIP-98 lifecycle (txnId expiry → Dead).

## Part B — Missing edge-case tests (count: 24 concrete gaps)

TransactionCoordinator / recovery:
1. **Allocator reseed after replay** (A1): construct `TransactionCoordinator(ProducerIdAllocator{}, …, &log)` over
   a log holding pid 0..N in a live txn, then `initProducerId(newTxnId)` — assert the new pid does NOT collide
   with any recovered pid. NO test (all durable tests inject non-colliding bases or use one txnId).
2. **Crash mid-overflow-fence before complete** (A2): inject Ongoing@INT16_MAX, `initProducerId` (returns fresh
   pid), destroy coordinator WITHOUT completeTransaction, reopen from log, resolvePendingMarkers — assert final
   pid/epoch and that the client-returned fresh pid is handled deterministically. NO test.
3. **re-init in EVERY state**: Empty✓, Ongoing✓, PrepareCommit✓(C3), PrepareAbort✓(C4), CompleteCommit✓, but
   **CompleteAbort**, **PrepareEpochFence**, and **Dead** re-init are UNTESTED.
4. **completeTransaction in CompleteCommit / CompleteAbort / PrepareEpochFence / Dead** — only Empty/Ongoing
   invalid-transition cases tested; the Complete*/Fence states are not.
5. **endTransaction with wrong `commit` after recovery** (recovered PrepareCommit, client sends abort) — mixed
   commit/abort intent on a recovered txn. NO test.
6. **resolvePendingMarkers with ZERO pending** returns 0 (tested indirectly) but **interleaved commit+abort**
   pending set (some txns PrepareCommit, some PrepareAbort in one resolve pass) is UNTESTED — verify each gets
   the correct control type.
7. **resolvePendingMarkers with MANY pending** (e.g. 100 txns) — throughput/ordering; only ≤4 tested.
8. **doWork abort where txnStartNs==0** (a Prepare* left Ongoing-like) and **doWork on a txn whose timeout is
   INT32_MAX ms** (overflow of `txnTimeoutMs * 1'000'000LL`?) — `int64_t` mul of INT32_MAX*1e6 ≈ 2.1e15 fits,
   but a boundary test is absent.
9. **timeout exactly at INT64 nowNs near overflow** / negative `nowNs` — clock-injection edge, untested.
10. **addPartitions with an EMPTY partition vector** — does Empty→Ongoing still fire and set txnStartNs with no
    partitions? (Kafka: AddPartitions with empty set.) UNTESTED.
11. **addPartitions idempotency**: same partition added twice across two calls returns kOk and does not
    re-persist a redundant Ongoing? Dedup at set-level tested; the second-call idempotent kOk is not asserted
    against the persisted log.
12. **endTransaction with a partition set that changed vs. what was persisted** — n/a to API but recovered-txn
    partition fidelity beyond size (exact set contents) is only checked in a couple tests.

Epoch / producerId boundaries:
13. **Epoch overflow at exactly INT16_MAX-1 (32766)** → next re-init yields 32767 (still same pid), and the
    32767→overflow step. Only the INT16_MAX (already-exhausted) injection is tested; the 32766→32767→overflow
    walk is not.
14. **Non-overflow Ongoing fence at a normal epoch** asserting the marker carries OLD pid + BUMPED epoch (the
    NEW-1 non-overflow branch, `:157-160`) — NEW-1 test only covers the overflow branch.
15. **ProducerIdAllocator exhaustion / wrap near INT64_MAX** (`allocate()` at `next_==INT64_MAX` overflows to
    negative) — no guard, no test. Block-boundary crossing IS tested; the int64 wrap is not.
16. **Allocator starting base at a block boundary** (`startingBase == kBlockSize`) and `startingBase` negative —
    untested corner of the ceiling math.

Control batch / marker format:
17. **extract with keyLength varint != 2** (e.g. 0, 4, or a max-size 10-byte varint) — the `keyLen==2` else-branch
    returns false; only the happy path + CRC-corruption is tested.
18. **extract with valueLength varint != 6**, and a **max-size (10-byte) varint** in each varint field — the
    `read_varint_zigzag_safe` 10-byte cap (`ControlMarker.cpp:59-70`) is never exercised at its limit.
19. **extract truncated at EACH field boundary** (after magic, after CRC, mid-key, mid-value, mid-headers-count) —
    only a couple of "avail = ENCODED_SIZE-5/-10" truncations are tested, not per-field.
20. **isControlBatch/extract at a non-zero `offset`** into a larger buffer (all tests pass offset 0) — verify no
    absolute-vs-relative index bug (note `extract` computes `end = ENCODED_SIZE` as if relative to `p`, which is
    correct only because reads use `p + …`; a test at offset>0 would lock this in).
21. **build at producerId = INT64_MIN / -1 and epoch = INT16_MIN** round-trip — only INT64_MAX/2 & INT16_MAX tested.

Durable log:
22. **Torn tail mid-`Complete*`**: write Ongoing+PrepareCommit fully, then a partial CompleteCommit record;
    reopen → assert state recovers to PrepareCommit (resolvable), not a half-applied Complete. Torn-tail is
    tested generically but not specifically straddling a Complete* transition.
23. **CRC corruption in the MIDDLE record** (not the tail): a good record, a CRC-flipped record, then a good
    record — replay must stop at the first bad one and truncate everything after (currently untested; only
    tail garbage is tested).
24. **Record with partitionCount > 0 but txnIdLen=0**, and **a topic string containing embedded NUL bytes** —
    length-prefixed handling of empty txnId is tested, but empty-txnId-with-partitions and binary topics are not.

SBE messages:
- (covered by A3/A4) **every `decode()` against a truncated frame** — the `_safe` readers are unit-tested in
  isolation but NO test decodes a real truncated `AddPartitions/EndTxn/WriteTxnMarkers` message through the
  public `decode()` and asserts no over-read (run under ASAN). This is the single most valuable missing test.
- **SBE messages with max fields**: 65535-byte txnId / 65535 partitions round-trip (truncation guard A5).
- **empty partition list on WriteTxnMarkers** IS tested; **empty txnId on AddPartitions/EndTxn/WriteTxnMarkers**
  is not.

## Re-audit counts (207410f)
- Prior fixes: B1,B2,C1,C2,C3,C4,S1,S2,NEW-1 — all HOLD, no regressions.
- NEW BLOCKER: 1 (A1 allocator-reuse-on-failover). NEW CORRECTNESS: 2 (A2 crash-seam PID swap, A5 uint16 trunc).
  NEW SILENT: 2 (A3/A4 unsafe SBE decode over-read). CLEAN/latent: A6 (CRC init race), A7 (nits, Dead dead-code).
- Part B: 24 concrete missing edge-case tests (most impactful: #1 allocator reseed, #2 crash-seam swap, #A3 ASAN decode).
- Verdict: **BUGS REMAIN** — not CLEAN. A1 is a silent exactly-once break on the documented default failover path.

---

# Full re-audit round 2 (commit dcd1bb0)

Fresh-eyes pass over the ENTIRE `libs/transaction/**` + cross-ref `libs/storage/ProducerStateManager.h`.
Verifying A1–A7 hold with no regression, plus an independent edge-case coverage assessment.

## Part A — full bug audit

### Verdict: **CLEAN** (no atomicity / PID-reuse / unresolved-txn / unsafe-decode / durability hole found)

### A1–A7 re-verification (all HOLD, no regressions)
- **A1 (allocator reseed on replay)** — `TransactionCoordinator.cpp:43-58`. The recovery loop scans `txnLog_->all()`,
  tracks `max(producerId, pendingNewProducerId)`, and calls `allocator_.reseedAtLeast(maxPid)`. Covers ALL recovery
  paths: Empty, Ongoing, Prepare*, Complete*, and the deferred `pendingNewProducerId`. `reseedAtLeast` is monotone
  (`if (next_ < target)`) so an injected allocator already past maxPid is not moved back — the transactionalId→PID
  map is untouched, no pids skipped. HOLDS.
- **A2 (52-byte header + deferred PID swap round-trips)** — `TransactionLog.cpp:216-217` (write @40/@48),
  `:301-302` (read @40/@48), `:335-336`+`:140-141` (apply). `kFixedHeaderSize=52`; layout is internally consistent
  (writer/reader/replay all agree). `pendingNewProducerId/Epoch` persist and recover exactly; the A2 crash-seam test
  confirms a mid-overflow-fence crash re-adopts the SAME fresh PID. HOLDS.
- **A3/A4 (bounds-checked public decode)** — all four INGRESS request decoders (`InitProducerIdRequest`,
  `AddPartitionsToTxnRequest`, `EndTxnRequest`, `WriteTxnMarkersRequest`) take `avail`, check the fixed block against
  `end`, and use `read_str_safe`/`read_partitions_safe` (all-or-nothing, per-tuple bounds). No ingress decode path
  uses an unchecked reader. The unsafe `read_str`/`read_partitions` remain ONLY in the three Response decoders
  (coordinator→client, template 2/4/6), which are not the network-ingress attack surface. HOLDS.
- **A5 (over-long rejection)** — `TransactionLog.cpp:173-185` throws `std::length_error` for txnId/partCount/topic
  over `kMaxUint16`; every SBE `encode()` returns 0 for the same. No silent-truncation path remains. HOLDS.
- **A6 (call_once CRC init)** — `TransactionLog.cpp:54-56` and `ControlMarker.cpp:92-94` both wrap the lazy table
  fill in `std::call_once`; static crc32c can be called cross-thread safely. HOLDS.
- **A7 (justified dead-code)** — `Dead`/`PrepareEpochFence` retained for enum/on-disk stability; `endTransaction`'s
  unused `nowNs` documented (deadline owned solely by `doWork`). Justified, not bugs. HOLDS.

### State-machine / EOS correctness (fresh review — no findings)
- Zombie fencing keyed by exact `(producerId, producerEpoch)` in `validateEpochAndPid` (`:450-463`): a stale epoch
  (`<`) is fenced as InvalidProducerEpoch; a spurious higher epoch is also rejected (conservative). No path lets a
  fenced producer mutate state.
- Concurrent-txn guard (`:125-133`) returns `kConcurrentTransactions` for re-init during Prepare*/Complete*/
  PrepareEpochFence — no silent epoch bump mid-resolution.
- NEW-1 overflow-fence (`:163-184`): on Ongoing-fence with epoch==INT16_MAX, the OLD PID/epoch are KEPT on the
  PrepareAbort (so the ABORT marker matches in-flight data, which ProducerStateManager keys by producerId —
  confirmed in `ProducerStateManager.h:26-30,104-113`), and the fresh PID is deferred to `pendingNewProducerId`,
  adopted only in `completeTransaction` (`:333-338`) AFTER the abort resolves. Non-overflow path bumps epoch on the
  same PID (also marker-consistent). Correct per KIP-98.
- Durable-before-side-effect ordering holds everywhere: `endTransaction` fsyncs PrepareCommit/PrepareAbort before
  returning partitions (`:288`); `completeTransaction` persists a DISTINCT Complete* record BEFORE clearing to Empty
  (`:308-316`, `:325-340`) — a crash between marker-write and Empty recovers as Complete* (not re-resolved).
- `doWork` (`:350-369`) samples nowNs once, transitions only Ongoing→PrepareAbort, sets markersPending, persists;
  no spin. Timeout math `int64_t * 1'000'000LL` cannot overflow for the clamped `[1, 900000]` ms range.
- Recovered Prepare* sets `markersPending=true` (`:48-50`); `pendingMarkerTransactions` + `resolvePendingMarkers`
  re-drive to Complete*→Empty idempotently (recovered CompleteCommit/Empty are NOT re-emitted — markersPending only
  set for Prepare*).

### Durability / wire safety (fresh review — no findings)
- Replay bounds checks (`TransactionLog.cpp:279-323`) are correct: `totalLength >= kFixedHeaderSize+kChecksumSize`
  guards the `totalLength - kChecksumSize` subtraction; per-field `vpos + len > totalLength - kChecksumSize` cannot
  under/overflow (size_t promotion). CRC verified before any field is trusted; first corrupt/torn record stops
  replay and ftruncates the remainder (append-only-correct). pwrite+fsync on every append.
- SBE `read_partitions_safe` reserves at most 65535 tuples (bounded) and validates each tuple's bytes before
  committing — a crafted count that fits uint16 but exceeds the buffer is rejected with no over-read.
- `ControlMarker::extract` caps reads at `end = ENCODED_SIZE` relative to `p`; varints are bounds-checked
  (`read_varint_zigzag_safe`, max 10 bytes). CRC verified in `isControlBatch` before trusting the CONTROL bit.

### LOW / non-blocking observations (not bugs)
- **L1** `TransactionLog.cpp:330` casts `stateRaw` straight to `TxnState` with no range check (values 0–7 legal).
  A CRC-valid record with a byte >7 yields an out-of-range enum. Only reachable via deliberate tampering that also
  fixes the CRC; recovery of a genuinely corrupt byte is already blocked by CRC. Cosmetic hardening at most.
- **L2** Response decoders (template 2/4/6) remain unchecked-reader — acceptable (client-side, non-ingress), but
  if a future path decodes a response from an untrusted peer it would need an `avail` overload. Documented risk only.

## Part B — edge-case test coverage assessment

Coverage is strong (106 coordinator tests + full log/messages/marker suites; the 24 round-1 gaps are now closed:
ASAN-worthy truncated public decodes #531-591, crafted-huge-count #556, mid-record CRC corruption #578,
torn-tail-mid-Complete #625, embedded-NUL topic #693, empty-txnId+partitions #677, reseed/INT64_MAX #231, epoch
walk 32766→overflow #2181, NEW-1 overflow-fence #1520, marker scope #2229, doWork boundary #2248).

Residual niche gaps still NOT covered (low severity — hardening, not correctness holes):
1. **Coordinator: reseed when recovered max == INT64_MAX** — the ALLOCATOR unit test parks-at-exhausted (#231), but
   no coordinator-level test recovers a log whose highest producerId == INT64_MAX and asserts the next
   `initProducerId(no-txnId)` returns `kExhausted` (pid -1) instead of a reused/negative pid.
2. **Interaction: overflow-fence + crash + reseed in ONE flow** — A2 covers crash-seam PID re-adoption and A1
   covers reseed, but not the combined path (recover a `pendingNewProducerId` that is also the recovered max, then
   confirm a *fresh* txnId allocation lands strictly above it and never equals the deferred pid).
3. **Idempotent re-resolve: marker written → crash → recover → resolve again** — recovered Prepare* resolvability is
   tested, but not a full "resolvePendingMarkers ran, then the SAME log is reopened (Complete* now persisted)" pass
   asserting `pendingMarkerTransactions()` is empty (no duplicate ABORT markers).
4. **Concurrent addPartitions during Prepare*** — `AddPartitionsWhenPrepareCommit/AbortIsInvalidTransition` cover
   the two Prepare states, but NOT addPartitions during `CompleteAbort` / `PrepareEpochFence` (CompleteCommit is
   covered by T2Test); a table-driven "addPartitions rejected in every non-{Empty,Ongoing} state" would close it.
5. **isControlBatch/extract at a non-zero offset** — noted round 1 (#20) and still open: all marker tests pass
   offset 0; a test at offset>0 would lock the relative-vs-absolute index contract in `extract` (`end=ENCODED_SIZE`).
6. **ControlMarker CRC boundary** — a batch whose CRC-covered region is exactly ENCODED_SIZE-CRC_DATA_START with a
   single flipped byte at the last covered index (off-by-one at the CRC region boundary) is not explicitly tested.
7. **Log record with partitionCount > 0 but a topic longer than the record body claims** — the replay per-tuple
   bounds break (`:315-317`) is exercised only via generic torn-tail; a hand-crafted CRC-valid header with an
   in-body topicLen that overruns `totalLength` is not directly tested.

### Round-2 conclusion
- Part A: **CLEAN** — A1–A7 and all prior fixes (B1,B2,C1,C2,C3,C4,S1,S2,NEW-1) hold; no new atomicity, PID-reuse,
  unresolved-txn, unsafe-ingress-decode, or durability defect. Two LOW hardening notes (L1 enum range, L2 response
  decoders) are non-blocking.
- Part B: coverage adequate for correctness; 7 residual niche/interaction tests listed above would raise it from
  "strong" to "exhaustive" (none guards an unproven correctness property — all are belt-and-suspenders).

---

## Final re-audit (commit 3714879)

**Scope.** Fresh whole-library pass over `libs/transaction/**` (Types, ProducerIdAllocator,
TransactionCoordinator, TransactionLog, TransactionMessages, ControlMarker — headers, src, and all 5 test files),
cross-referenced against `libs/storage/include/rune/storage/ProducerStateManager.h`. Verified the round-2 L1/L2
hardening and the 7 residual tests applied on top of the round-2-CLEAN state (dcd1bb0). No code modified.

### L1 — persisted state-byte validation (VERIFIED)
`TransactionLog.cpp:329-331` computes `kMaxValidState = uint8(PrepareEpochFence) = 7` and `break`s replay when
`stateRaw > 7`, BEFORE `applyEntry()`. `TxnState` is a `uint8_t` enum with contiguous enumerators 0..7, so every
byte 0-7 maps to a defined enumerator and 8..255 is rejected — no out-of-range enum is reachable. The check runs
after the partition loop, so a CRC-valid record with a bad state byte is stopped cleanly (torn-tail semantics,
identical to a CRC failure): tail truncated, prior good records preserved, log remains writable. Test
`InvalidStateByteRejectedEvenWithValidCrc` (flips state→0xFF, recomputes a valid CRC) confirms rejection + survival
of the preceding record + post-truncation writability. No regression: all valid states 0..7 still replay (250-test
suite green, including every existing replay/round-trip test).

### L2 — bounds-checked Response decoders (VERIFIED)
`InitProducerIdResponseMsg`, `AddPartitionsToTxnResponseMsg`, `EndTxnResponseMsg` each gained an `avail`-based
`decode()` overload (`TransactionMessages.h:336-349, 450-461, 559-570`) returning `std::optional`, each guarding
`pos + BLOCK_LENGTH > end`. All four Request decoders were already bounds-checked (A3/A4). Repo-wide grep confirms
these Response types have NO external consumers and the unchecked 2-param overloads have NO non-test callers, so no
public decode path (request or response) can over-read a truncated buffer. Legacy 2-param overloads are unchanged
and behaviour-preserving (`L2LegacyTwoParamDecodeUnchanged`). Truncation sweeps `avail ∈ [0,n)` return `nullopt`
for all three responses.

### 7 residual tests (ALL REAL, ALL GREEN)
Residual1 (INT64_MAX recovered pid → `reseedAtLeast` parks `next_` at INT64_MAX → `allocate()` returns
`kExhausted`, never a recycled pid); Residual2 (overflow-fence + crash-injected PrepareAbort with deferred
pendingNewProducerId + reseed: OLD pid on abort marker, deferred fresh pid adopted at Complete, next fresh alloc
strictly `> kFreshPid`); Residual3 (resolve → reopen same log → `pendingMarkerTransactions()` empty → second resolve
emits 0 markers = idempotent, no duplicate ABORTs); Residual4 (addPartitions → `kInvalidTransition` in BOTH
CompleteAbort and PrepareEpochFence); Residual6 `CrcBoundaryOffByOneHandledCorrectly` (avail=SIZE-1 rejected,
last-covered-byte flip caught by CRC, avail=SIZE+1 ok); Residual7 `TopicLenOverrunsTotalLengthRejected` (CRC-valid
header with in-body topicLen=0xFFFF → replay per-tuple bounds `break`, no over-read, record not applied);
Residual5 `ExtractAtNonZeroOffset` (already present, confirmed: extract at pad=37 offset round-trips). None guards
an unproven correctness property — all are belt-and-suspenders confirmations of already-correct code.

### Whole-library re-scan (A1–A7 + prior fixes still hold)
- **EOS / PID-reuse (A1/A2):** coordinator ctor reseeds allocator past `max(producerId, pendingNewProducerId)`
  across all recovered metadata; cross-ref confirms `ProducerStateManager` dedup/epoch is keyed by
  `(producerId, partition)`, so reseed is the correct guard against a silent EOS break. No path recycles a live pid.
- **Atomicity / durability (C2, endTransaction):** Prepare*/Complete* persisted+fsynced before markers and before
  Empty; overflow (NEW-1) keeps OLD identity through PrepareAbort and adopts the deferred pid only at CompleteAbort.
- **Overflow/exhaustion:** `allocate()` and `reseedAtLeast()` both clamp at INT64_MAX with no signed wrap to a
  negative pid that would collide with the -1 sentinel.
- **Unsafe decode / boundary (A3/A4/A5/S1/S2):** every public ingress decode is `avail`-bounded; all length-prefixed
  encoders reject `>uint16` inputs rather than truncating; `isControlBatch`/`extract` are CRC-first with
  bounds-checked varints and a required (no-default) `avail`; replay bounds use `totalLength - kChecksumSize` with
  the `totalLength >= kFixedHeaderSize + kChecksumSize` guard preventing underflow.
- **Concurrency:** CRC32C table init is `std::call_once` in both TransactionLog and ControlMarker (A6); single-writer
  model intact. Suite is stable across 3 shuffled seeds (no hidden shared-state coupling).

### Coverage
Now exhaustive for correctness: the round-1 residual gaps (#3,#4,#5,#6,#7 above) are all closed by the 7 new tests;
L1 and L2 each have dedicated pass/fail + boundary tests. No material property is left untested.

### Verdict
**CLEAN — libs/transaction section CERTIFIED DONE.** 250/250 transaction tests pass (seq and shuffled ×3; the
commit message's "175" undercounts the current binary — zero failures either way). Zero material findings.

---

# P5-4 review (commit a10d765)

Date: 2026-09-11. Reviewer: Opus. Scope: read-committed isolation — `ReadCommittedView.{h,cpp}`,
`ReadCommittedViewTest.cpp`, `ControlMarker.{h,cpp}`, storage `Log`, protocol `RecordBatch`. Read-only.

## Part A — bug audit: **CLEAN** (no correctness bugs; 1 cleanliness finding)

### LSO correctness — CORRECT
- `LsoTracker::lso()` returns `hw` when no open txns (ReadCommittedView.cpp:157-159) and `min(firstOffset)`
  over open txns via `byFirstOffset_.begin()` (:162), clamped to `[0,hw]` (:164-166). LSO ≤ HW always;
  first-open-wins; out-of-start-order commit does NOT advance past a still-open earlier txn (`byFirstOffset_`
  is a sorted min over VALUES, not keys — verified by `LsoIsMinFirstOffsetNotMinPid`, `ThreeProducers*`).
- Monotonicity: resolving a txn only erases entries, so `min(firstOffset)` can only rise or jump to HW.
  A reused PID's *next* txn opens at a higher offset (later append) → LSO non-decreasing.
- `onProduceBatch` keeps the earliest firstOffset for a multi-batch txn (:127-133). Correct.

### Aborted-txn index — CORRECT
- ABORT marker records range `[firstDataOffset, abortMarkerOffset]` (onAbortMarker :184; contains() inclusive,
  header:89-91). `isAborted` matches on `producerId` AND offset-in-range (:192-198), so a different producer's
  batch at the same offset is never wrongly filtered.
- Committed txns are never inserted (rebuild only calls `onAbortMarker` when `!commit`, :283-287). Verified
  `CommittedTxnNotInIndex`.
- **PID reuse across sequential txns is disambiguated by offset ranges, not epoch.** Producer epoch is NOT a
  key (openTxns_/index key on PID only). This is SAFE on a single partition: Kafka permits only one open txn
  per PID per partition, and a reused PID's second txn opens at a strictly higher offset than the first txn's
  abort-marker offset, so its data cannot fall inside the first (aborted) range. abort-then-commit-same-PID is
  correct by construction (though NOT directly tested — see Part B).

### read_committed / read_uncommitted filter — CORRECT
- `limit = lso(hw)` (committed) or `hw` (uncommitted) (read :326-328); `fetchOffset >= limit` short-circuits (:330).
- **Straddling boundary handled via `lastOff = base + lastOffsetDelta; if (lastOff >= limit) break;`**
  (:380-381) — identical to the replication NB3 fix (PartitionReplica.cpp:339). No record ≥ LSO/HW leaks.
- Control batches never emitted: `if (ctrl) { skip }` in BOTH modes (:384-387). Verified
  `ControlBatchesNeverReturnedInEitherMode`.
- Aborted data skipped only in ReadCommitted via `continue` (not break) so committed batches after an aborted
  one are still returned (:390-395; `ThreeProducersMixedCommitAbort`).
- Dual-endian producerId extraction is correct: BE at [43] for data (extractProducerId :51-58), LE via
  ControlMarker::extract for control (:45-49). `isControlBatch` requires avail≥75 + magic + CRC32C, so a data
  batch cannot be misclassified as control (CRC collision is cryptographically implausible).

### Incremental vs rebuild — SAME RESULT; **FINDING P5-4-C1 (cleanliness, LOW)**
- Incremental path (onProduceBatch/onControlMarker + onAbortMarker) and rebuild() converge — proven by
  `IncrementalUpdatesMatchRebuild`.
- `rebuild()` contains a **dead first pass** (ReadCommittedView.cpp:220-247): a full `walkBatches` scan whose
  control-marker branch is a self-admitted no-op ("But we lost the pointer here", :234-239) and whose results
  are unconditionally discarded by `clear()` at :251-252 before the real second pass (:254-309) runs.
  Truly dead — not a correctness bug — but it DOUBLES rebuild I/O (two full log reads of the whole partition)
  and is confusing. Recommend deleting lines 220-252's first block. Severity: LOW (perf/clean-code).

### Boundaries — CORRECT
- Empty partition (`hw<=0` early return, rebuild :211; read short-circuit), all-aborted, all-committed,
  fetchOffset>HW, read exactly at LSO, HW=0 all covered and pass. No offset overflow (int64 throughout; the
  only int32 cast is the maxBytes soft cap :400, bounded by maxBytes). Single-writer; no spin; no locks.

### CMake — NO CYCLE
- `rune_transaction` PUBLIC-links `rune_storage` (CMakeLists.txt:15). `rune_storage` links no internal lib;
  `rune_replication`→`rune_storage` only. No path back to `rune_transaction`. Confirmed acyclic.

## Part B — edge-case coverage: 8 niche cases NOT tested (all currently-correct by code, but unguarded)
1. **Abort-then-commit, SAME PID, sequential txns** (the PID-reuse disambiguation guarantee) — the single most
   valuable missing test; nothing currently pins that txn B's data escapes txn A's aborted range.
2. **Out-of-start-order COMMIT via the log/rebuild path** (LsoTracker unit tests cover it, but no rebuild()
   test writes marker-for-younger-txn-before-older and asserts LSO stays pinned).
3. **ABORT marker with NO data batches** (empty aborted txn): onAbortMarker gets firstOff==markerOff; harmless
   but untested.
4. **Oldest open txn's firstOffset == HW** (txn opens exactly at HW boundary) — LSO clamp interaction untested.
5. **Aborted-range boundary off-by-one at the marker offset** for a *data* batch landing exactly at last
   (only control lands there today, but no negative test at firstOffset-1 / lastOffset via read()).
6. **Control batch at offset 0** (partition begins with a marker for an unknown PID) — rebuild robustness.
7. **maxBytes soft cap** — no test exercises the multi-batch byte-cap/"always return ≥1 batch" path (:399-402).
8. **Very large aborted index** (O(ranges) linear `isAborted` scan) — no scale/perf guard.

## Verdict
Part A: **CLEAN** for exactly-once visibility — read_committed provably never exposes an uncommitted or aborted
record, and LSO is correct/monotone. One LOW cleanliness finding (P5-4-C1: dead first pass in rebuild(),
lines 220-252). Part B: 8 recommended tests (top: same-PID abort-then-commit; out-of-order commit via rebuild).

---

# P5-4 full re-audit + endianness (commit a3ba944)

## PRIORITY: control-batch endianness mismatch — VERDICT: **REAL BUG. BLOCKER. MUST FIX before P5-5.**

### The mismatch is real and cross-cutting
- `ControlMarker::build` encodes the entire control batch **little-endian** via `write_le<>`:
  batchLength at [8..11], magic [16], CRC [17..20], attributes [21..22], producerId [43..50], etc.
  (`libs/transaction/src/ControlMarker.cpp:152-191`). ENCODED_SIZE=75, batchLength=63.
- The storage/protocol reader `RecordBatchReader` decodes every field **big-endian** via `detail::getBE32/getBE64`
  (`rune-protocol/include/rune/protocol/RecordBatch.h:109-121`). `totalLength()` = `getBE32([8..11]) + 12`.
- Data batches produced by `RecordBatchBuilder::build()` are **big-endian** (`RecordBatch.h:74-96`, `appendBE*`).
- So data batches and control batches are in **DIFFERENT on-disk byte orders**. This diverges from Kafka KIP-98,
  where control batches use the identical RecordBatch wire format (same endianness) with only the CONTROL attribute
  bit set, so a single reader walks both. The header comment claiming "byte-compatible with the log format"
  (`ControlMarker.h:15-17`) is **false**.

### Why the production consumer path breaks
The real consumer/replication read path walks batches with the BE `RecordBatchReader` ONLY — it has no CONTROL-bit
demux:
- `LogSegment::read` skip-loop: `RecordBatchReader r(...); total = r.totalLength()` (`libs/storage/src/LogSegment.cpp:95-98`).
- `PartitionReplica::read` (leader committed read): `RecordBatchReader r(...)` (`libs/replication/src/PartitionReplica.cpp:327-329`).
- `PartitionRecorder::read` (replication/recording): `RecordBatchReader reader(...)` (`libs/replication/src/PartitionRecorder.cpp:91-96`).
- `Log::truncateTo` / `LogSegment::recover` walk with the same BE reader (`Log.cpp:113`, `LogSegment.cpp:136`).

Concrete failure: a LE control batch's batchLength field [8..11] = `63` LE = bytes `3F 00 00 00`, read BE =
`0x3F000000` = 1,056,964,608. `totalLength()` = +12 ≈ 1 GiB. In every walker the guard
`pos + total > buf.size()` fires → the loop `break`s. In `LogSegment::read`, once the window equals the segment
the function returns `{}`. **Net effect on a real partition that contains a control marker:**
- If a marker is the first batch in the read window, `log->read(...)` returns EMPTY → any committed data batches
  physically after that marker in the same window become **unreachable** → consumer hangs / never advances past LSO
  even though LSO advanced (corruption of the fetch contract).
- `recover()`/`truncateTo` treat the marker as a torn tail → **silently truncate the log at the marker**, destroying
  all data after it and corrupting nextOffset/LEO.

### ReadCommittedView is self-consistent but does NOT save the consumer
`ReadCommittedView::{rebuild,read}` carry their OWN LE-aware walker that demuxes on `ControlMarker::isControlBatch`
and uses fixed `ENCODED_SIZE` for control batches (`ReadCommittedView.cpp:74-113, 228-277, 314-377`). That is why
every P5-4 test passes. But `ReadCommittedView` is **not wired into any production read path** at a3ba944
(grep: no non-test caller of `ReadCommittedView`, `writeMarkers`, or `resolvePendingMarkers`). The consumer path
today is `PartitionReplica::read`/`Log::read`, which are BE-only. Even inside `ReadCommittedView::read`, the
underlying `log->read(fetchOffset,...)` → `LogSegment::read` BE skip-loop returns EMPTY whenever a control batch
sits at the start of the physical read window (see below), so the LE-aware walker never even receives the bytes.

### The bug is already documented in-tree and masked by test design
`ReadCommittedViewTest.cpp:1171-1178` (Edge 6) explicitly states: "LogSegment::read() uses RecordBatchReader (BE)
… cannot parse LE control batches. When the very first batch in a segment is a control batch, log->read(0,…)
returns empty — the committed data at offset 1 is unreachable via read(). This is a known storage-layer limitation."
The test deliberately **does not assert on read() output for that layout** and uses a mid-stream variant to dodge it.
So the suite is GREEN only because it avoids the failing path — the bug is latent, not absent.

### Fix direction (do before P5-5)
Unify the control-batch format with the data RecordBatch format so ONE reader handles both (the Kafka approach):
1. Re-encode `ControlMarker::build` **big-endian** for all batch-header + record fields (mirror `RecordBatchBuilder`:
   `appendBE*` and BE CRC over [21..end]); keep magic=2 and set the CONTROL attribute bit. Then a data batch and a
   control batch are byte-identical in layout/endianness and differ only by the attribute bit.
2. Make the storage/consumer walkers CONTROL-aware OR (simpler, preferred) rely on the unified format so
   `RecordBatchReader.totalLength()` parses control batches natively (their batchLength is already correct once BE).
   Then `PartitionReplica::read`/`PartitionRecorder`/`recover`/`truncateTo` must **skip** (not emit) control batches
   to the consumer but still count their bytes/offsets — add a CONTROL-bit check (attributes at [21..22] BE) to
   filter them from consumer output while walking past them correctly.
3. Update `ControlMarker::isControlBatch/extract` to BE and drop the LE branch in `ReadCommittedView`'s walker
   (both formats then share the BE reader).
4. Add a regression test that appends a data batch, then a marker, then reads back via `Log::read`/`PartitionReplica::read`
   (NOT via ReadCommittedView) and asserts the data batch is returned and the marker is skipped — the exact case
   Edge 6 currently refuses to assert.

## Read-committed re-audit — VERDICT: **CLEAN (logic), with the endianness BLOCKER as the one cross-cutting bug**

- P5-4-C1 cleanup verified via `git show a3ba944 -- …/ReadCommittedView.cpp`: the removed first pass was genuinely
  dead — its `onProduceBatch` calls were unconditionally wiped by the following `tracker_->clear()` before the real
  pass ran. The surviving single pass (`ReadCommittedView.cpp:228-277`) reproduces the old second pass exactly.
  Behavior-preserving. `SamePidAbortThenCommitRebuildMatchesIncremental` (test:1338-1393) pins rebuild==incremental.
- The 8 new edge tests (`ReadCommittedViewTest.cpp:1011-1332`) are real and correct: same-PID abort→commit offset-range
  disambiguation, out-of-order commit LSO-pinning, empty-abort no-spurious-range, firstOffset==HW clamp, aborted-range
  boundary via read, control-at-offset-0 rebuild robustness, maxBytes soft cap, 50-txn scale. They exercise
  `ReadCommittedView`'s own walker and pass legitimately for that component.
- LSO logic re-scanned: `lso()` returns `byFirstOffset_.begin()->first` (std::map ⇒ min firstOffset), clamped to
  [0,hw] (`ReadCommittedView.cpp:155-167`) — correct and monotone. `onProduceBatch` keeps the earliest offset for a
  PID (:127-134). `onControlMarker` for an untracked PID is a no-op returning -1, so `AbortedTxnIndex` gets no
  spurious range (:136-153, :247-256) — matches Edge 3/6. `AbortedTxnIndex.isAborted` is per-PID + inclusive range
  contains — correct; O(ranges) linear scan is a perf note (not correctness).
- No regression found in the read-committed/LSO/aborted-index logic itself. No other correctness bug identified in
  this surface beyond the endianness blocker.

## Bottom line
The read-committed component is internally sound, but the control-batch/data-batch format divergence is a genuine
end-to-end correctness bug that WILL break P5-5 the moment markers are appended to a partition Log and read back via
the production `LogSegment::read`/`PartitionReplica::read`/`recover` path. **Fix the format unification first.**

---

# LE unification review (broker e5662f7 / protocol df64c79)

**Verdict: CLEAN.** The BE→LE flip of the on-disk RecordBatch format is byte-correct and complete
across both repos; the prior BE/LE format divergence blocker identified in the earlier audit is now fixed.

## 1. LE flip byte-correct & complete
All 13 fixed header fields written AND read LE at identical offsets (RecordBatch.h:121-140 builder /
:163-186 reader): baseOffset i64@0, length i32@8 (=size-12), partitionLeaderEpoch i32@12,
magic@16, crc u32@17, attributes i16@21, lastOffsetDelta i32@23, baseTimestamp i64@27,
maxTimestamp i64@35, producerId i64@43, producerEpoch i16@51, baseSequence i32@53, recordCount i32@57.
`stampLeaderEpoch` writes LE @12 (RecordBatch.h:210-214), CRC-excluded (field @12 < kAttributesOffset 21).
CRC computed over [21..end] and stored/read LE both sides (build :139-140, ensureValid :182-185). ✅

## 2. No BE remnant anywhere
grep of both repos for getBE/appendBE/putBE/BE-shift-read of a batch field: **zero functional hits.**
Only matches: a test *filename* `ControlBatchBeFormatTest.cpp` + comment strings (historical "Be"),
and unrelated LE code in consensus TermLog.cpp / ConsensusMessages.h (own formats, not batch bytes). ✅
- Compaction producerId/epoch/baseSequence patch: LE (LogCleaner.cpp:266-294), CRC recomputed & stored LE.
- LogCleaner baseSequence read via `detail::getLE32` (:234). ✅
- ReadCommittedView::extractProducerId uses `RecordBatchReader::producerId()` (LE) (:44-54). ✅
- ControlMarker build/isControlBatch/extract all LE (ControlMarker.cpp:190-234, 256-264, 288-321). ✅
- ProducerStateManager snapshot is its own LE file format (wle/rle memcpy), unrelated to batch bytes — LE. ✅

## 3. CONTROL bit + skip
Control batch sets attrs 0x0030 (CONTROL 0x20 | TXN 0x10) (ControlMarker.cpp:195); `isControl()`
reads LE attrs & 0x20 (RecordBatch.h:173-176); data batch attrs=0 → not control, and vice-versa
(verified LE-7). `ControlMarker::isControlBatch` = avail≥75 + magic + CRC(LE) + CONTROL bit (LE +
CRC-gated, correct). `ReadCommittedView::read` SKIPS control batches in BOTH modes but still parses
totalLength to advance (ReadCommittedView.cpp:271-275) → data after marker reachable. recover /
truncateTo / LogSegment::read treat a control batch as a normal valid batch via unified
RecordBatchReader + ensureValid (LogSegment.cpp:135-147, Log.cpp:112-135, :94-104). ✅
- NOTE: `PartitionReplica::read` (:306-347) is the RAW replication path — it has NO isolation level and
  intentionally does NOT strip control batches (leader/follower logs must be byte-identical). The task's
  "PartitionReplica read_committed/read_uncommitted skip" actually lives in ReadCommittedView, which is
  correct. Not a defect.

## 4. Regression is real
`ControlBatchBeFormatTest.cpp` LE-12 appends data+data+COMMIT-marker+data+data to a REAL Log, asserts
5 batches parseable from offset 0, data-after-marker reachable via read(3), exactly 1 control + 4 data
(isControl), and recover()→LEO=5 (marker not torn tail). Under the pre-flip mismatch totalLength()
misreads → ~1 GiB → walk stops → recover truncates → the asserts fail. Non-vacuous. LE-4/5/10/11/13
cover Log::read, recover, ReadCommittedView both-sides, abort-indexing, LSO/abort filtering. ✅

## 5. read_committed intact
LSO = min open-txn firstOffset else HW (ReadCommittedView.cpp:95-107); aborted-range filtering by
per-PID inclusive contains (:132-139); markers in LE parsed by extract() and drive
onControlMarker/onAbortMarker (:188-201). LE-13 verifies aborted(0) filtered, committed(2) returned,
in-flight(4) hidden by LSO=4, controls(1,3) never emitted. ✅

## 6. No correctness/perf regression; tests converted not weakened
- LogCleanerTest CompactionPreservesProducerIdentity: old manual BE patch `(7-i)` + BE read replaced by
  `setProducerId/Epoch/BaseSequence` + LE read helpers; producerId/epoch/baseSequence EXPECT_EQ
  assertions RETAINED. ✅
- ControlMarkerTest CRC store/read flipped BE→LE; attributes read as LE int16; assertions retained. ✅
- ReadCommittedViewTest makeDataBatch now uses builder.setProducerId() (LE). ✅
- No test deleted or weakened to hide a regression. CRC still validates on every walk/recover.
- Minor (non-blocking): several stale comments in ReadCommittedView.cpp (:160-174, :252) still say "BE"
  though the code is LE — cosmetic only.

**Bottom line:** on-disk LE record format is byte-correct everywhere, no BE straggler, the
marker-in-log regression is genuinely covered, and existing tests were correctly migrated to LE.
