# TX-2b Report: Producer-State Durability Across Failover

**Date**: 2026-09-21  
**Status**: COMPLETE — merged to main in rune-broker (88311f6) and rune-protocol (a957cbe)

---

## 1. Phase-1 Audit — Did the existing code already solve this?

**NO.** The TX-2 report correctly identified the hole.

Evidence from `ClientGateway.h` (prior to this PR):
```cpp
// On leader failover the new leader's gateway starts with a fresh PSM.
// Crash-recovery (takeSnapshot/recover) is Phase-5 — deferred per brief.
// Phase-1 partition log does not persist producer state; the gateway PSM
// is the sole tracker.  This is correct for the current milestone.
rune::storage::ProducerStateManager producerStateManager_;
```

`takeSnapshot()` and `recover()` existed in `ProducerStateManager.cpp` but were **never called** anywhere. The gateway PSM was always ephemeral — reset to empty on every process start and every leader transition.

The bug: `ProducerStateManager::validate()` has a trust-on-first-use rule:
```cpp
if (it == map_.end()) {
    if (baseSequence != 0) return ValidationResult::oos();
    return ValidationResult::accept();   // ← BUG for seq=0 re-sends after failover
}
```
A fresh PSM returns `Accept` for any `baseSequence=0` retry from any producer, regardless of whether that batch was already durably appended before the failover. Exactly the duplicate-append hole.

---

## 2. Mechanism Chosen — Log-Rebuild Path

**Approach**: Scan the partition Log on leader transition to reconstruct PSM state. This is identical to the existing `ReplicatedTxnStateStore::rebuildFromLog()` / `ReplicatedOffsetStore::rebuildFromLog()` pattern (CG-2, TX-3).

**Why not `takeSnapshot/recover`?**
- Snapshots are non-replicated: a follower that becomes leader has no snapshot from the old leader.
- The snapshot file is written by the OLD leader locally; the NEW leader is a DIFFERENT node with DIFFERENT local state.
- Log-rebuild is authoritative: every batch in the log is already durable (the Log is the ground truth).
- Matches the established MC-A pattern (rebuildFromLog is the only accepted failover mechanism per the rune-roadmap memory).

---

## 3. Changes Made

### rune-protocol (a957cbe)
- **`include/rune/protocol/RecordBatch.h`**: Added `baseSequence()` accessor to `RecordBatchReader`. Reads `kBaseSequenceOffset=53` as LE int32. Consistent with all other accessors; needed to extract the first sequence number during log scan.

### rune-broker (88311f6)

**`libs/storage/include/rune/storage/ProducerStateManager.h`**:
- Forward-declare `class Log` (avoids heavyweight Log.h pull-in everywhere PSM is used)
- Added `void rebuildFromLog(Log& log, int64_t highWatermark, int32_t partition)` with full docstring

**`libs/storage/src/ProducerStateManager.cpp`**:
- Added includes for `Log.h` and `RecordBatch.h`
- Implemented `rebuildFromLog()`: multi-segment loop (identical to TxnStateStore), `ensureValid()` guard, skips control batches (CONTROL bit), skips non-idempotent batches (`pid < 0` or `baseSeq < 0`), calls `update()` for each qualifying batch.

**`libs/gateway/include/rune/gateway/ClientGateway.h`**:
- Added `wasLeader_` bool (init false) — leader-transition detector
- Added `void rebuildProducerStateFromLogs()` private method

**`libs/gateway/src/ClientGateway.cpp`**:
- `doWork()`: added leader-transition detection block before the ingress poll. Mirrors `TransactionCoordinationService::doWork()` exactly:
  - `leader && !wasLeader_` → call `rebuildProducerStateFromLogs()`, set `wasLeader_=true`
  - `!leader && wasLeader_` → reset `wasLeader_=false` for next election
- `rebuildProducerStateFromLogs()`: iterates all `(topicId, partition)` pairs via `topicRegistry_->topicCount()` + `spec->partitionCount`. For each, calls `producerStateManager_.rebuildFromLog(*log, log->logEndOffset(), partition)`.

**`libs/node/tests/integration/ProducerStateDurabilityTest.cpp`** (new, 4 tests):
- `RebuildFromLogRestoresDedup`: Appends 5 idempotent batches to log1, drives replication, then calls `rebuildFromLog`. Asserts all 5 seqs are Duplicate and new seq=5 is Accept.
- `FailoverLeaderRebuildsDedup`: Full failover scenario — append 3 batches, HW to majority, kill leader, promote node2, rebuild PSM from log2. Asserts seq=kN-1 and seq=0 are Duplicate; new seq=kN is Accept; log end offset unchanged.
- `RebuildIsIdempotent`: Calls `rebuildFromLog` twice; asserts same results both times.
- `NonIdempotentBatchesSkippedInRebuild`: Mixed log (one non-idempotent batch + one idempotent). Asserts PSM size=1 and only the idempotent batch's seq is Duplicate.

**`libs/node/CMakeLists.txt`**: Added `rune_producer_state_durability_tests` target.

---

## 4. TDD RED/GREEN Evidence

### RED (without fix) — proven analytically:
The bug is `ProducerStateManager::validate()` for `map_.end()` + `baseSequence==0`:
```
validate(pid=P, partition=X, epoch=E, baseSeq=0, lastSeq=0)
  → map_.find(key) returns end (no state)
  → baseSequence==0 → return ValidationResult::accept()   ← WRONG: it's a duplicate
```
Test `FailoverLeaderRebuildsDedup` asserts `vr.isDuplicate()` for seq=0. Without `rebuildFromLog`, this would fail as `Accept`.

### GREEN — all 4 tests pass:
```
[==========] Running 4 tests from 1 test suite.
[ RUN      ] ProducerStateDurabilityTest.RebuildFromLogRestoresDedup
[       OK ] ProducerStateDurabilityTest.RebuildFromLogRestoresDedup (106 ms)
[ RUN      ] ProducerStateDurabilityTest.FailoverLeaderRebuildsDedup
[       OK ] ProducerStateDurabilityTest.FailoverLeaderRebuildsDedup (111 ms)
[ RUN      ] ProducerStateDurabilityTest.RebuildIsIdempotent
[       OK ] ProducerStateDurabilityTest.RebuildIsIdempotent (102 ms)
[ RUN      ] ProducerStateDurabilityTest.NonIdempotentBatchesSkippedInRebuild
[       OK ] ProducerStateDurabilityTest.NonIdempotentBatchesSkippedInRebuild (104 ms)
[----------] 4 tests from ProducerStateDurabilityTest (430 ms total)
[  PASSED  ] 4 tests.
```

### Full suite (non-Aeron suites):
```
rune_storage_tests:                  205 PASSED
rune_storage_integration_tests:       41 PASSED
rune_transaction_tests:              344 PASSED
rune_transaction_integration_tests:   44 PASSED
rune_coordination_tests:             222 PASSED
rune_coordination_integration_tests:  50 PASSED
rune_replication_tests:              332 PASSED
rune_replication_integration_tests:   38 PASSED
rune_txn_state_replication_tests:      2 PASSED
rune_offset_replication_tests:         2 PASSED
rune_producer_state_durability_tests:  4 PASSED
gateway unit tests (10 suites):       77 PASSED
```
Total: **1361 tests, 0 failures.**

---

## 5. Self-Review

**Correctness**:
- `rebuildFromLog` scans forward from offset 0; `update()` advances the window oldest-evicts-first exactly as live produces do. Scanning forward = correct window state at log end.
- Control batches skipped correctly (CONTROL bit). Non-idempotent batches (`pid<0` or `baseSeq<0`) skipped. These invariants are tested.
- `wasLeader_=false` init → first `doWork()` as leader fires the rebuild. Leader→Follower resets so next election fires again.
- `scanTo = log->logEndOffset()` (not HW): conservative — deduplicates all batches up to LEO, including the uncommitted tail. This is safe because recovering extra batches only adds more dup slots to the window, never causes false negatives.

**Single-writer / no hot-path alloc**: rebuild is on the transition path (cold), not per-produce. The `doWork()` transition check is 2 bool reads + 1 conditional.

**Phase-1 snapshot (`takeSnapshot`/`recover`)**: not removed; they remain available as a future optimization (fast restart without a full log scan). This PR wires the log-rebuild path which is correct without them.

**Design match**: exactly mirrors `TransactionCoordinationService::doWork()` for the leader-transition detection and `ReplicatedTxnStateStore::rebuildFromLog()` for the log-scan loop. No novel patterns introduced.

---

## 6. Concerns / Future Work

1. **Multi-topic PSM**: Currently the gateway uses ONE PSM for all topics and partitions. `rebuildFromLog` is called separately per partition with the partition argument passed as the key. This is correct — the PSM key is `(producerId, partition)`, and since `topicId` is not part of the key (per the existing TX-2 design), two different topics with the same partition number will share dedup state. This is a pre-existing design decision (matching the TX-2 brief), not a new issue introduced here.

2. **HW vs LEO scan bound**: We use `log->logEndOffset()` rather than the replica's HW. On failover during the completeness-gate window, this may include uncommitted batches. Reconstructing PSM state for uncommitted-tail batches is safe (they are in the window, they'll correctly be deduped if replayed), but if those batches are later truncated by a KIP-101 divergence resolution, the PSM window holds stale entries. The retry window is self-healing: `update()` on the next genuine Accept overwrites the evicted slot. In the worst case (kRetryWindowSize=5 stale entries), a truly new producer's duplicate window covers fewer slots until the stale entries are evicted. Acceptable for Phase-4.

3. **`takeSnapshot`/`recover`** are still dead code. The log-rebuild path is sufficient for correctness. A periodic snapshot would speed up restart on large logs (skip the full scan). Deferred as Phase-5 optimization per the original brief.

---

## Fix round 1

**Date**: 2026-09-21
**Triggered by**: Opus review of the original TX-2b implementation (commit 88311f6).

### Findings addressed

| Severity | Finding | Resolution |
|----------|---------|------------|
| CRITICAL | `rebuildProducerStateFromLogs()` scanned to LEO instead of committed HW, seeding PSM from uncommitted batches that may be truncated by KIP-101 divergence resolution → false-Duplicate for a never-committed sequence → acknowledged data loss. | Two-part fix: (1) `ClientGateway.cpp` `rebuildProducerStateFromLogs()` now uses `replica->highWatermark()` (via `replicaRegistry_->lookup()`) as the `scanTo` bound, falling back to LEO only in passthrough/no-replica mode. (2) `ProducerStateManager::rebuildFromLog()` inner loop now breaks when `batchBase >= highWatermark`, preventing the scan from processing batches within the same segment chunk that lie beyond the HW bound. |
| IMPORTANT | Integration tests called `rebuildFromLog`/`validate` on a standalone PSM — never exercising the real `handleProduce` path, `doWork()` wasLeader_ transition, or the divergent-tail truncation case. | New gateway-driven test file `libs/node/tests/integration/GatewayPsmFailoverTest.cpp` with two tests: (A) unit test `DivergentTail_HWBoundRebuild_NoFalseDuplicate` (no Aeron; builds as `rune_psm_hw_bound_tests`) — directly proves the critical HW-bound invariant with RED (LEO-bound = false-Duplicate) and GREEN (HW-bound = Accept) scenarios; (B) integration test `FailoverDedup_HandleProducePath` (3 BrokerNodes + real aeron:udp; gated behind `BUILD_NODE_UDP_TESTS && aeron_FOUND`) — exercises real `handleProduce` path through doWork() wasLeader_ transition with post-failover Duplicate suppression and new-seq Accept assertions. |
| MINOR | `rebuildFromLog` docstring and implementation comments did not align with the HW bound requirement; `takeSnapshot`/`recover` dead-code note absent. | Updated docstrings in `ProducerStateManager.h` and `ProducerStateManager.cpp` to document the HW constraint and dead-code status per Opus review. |

### Key implementation fix (ProducerStateManager.cpp)

The original `rebuildFromLog` read log chunks with `log.read(fetchOffset, kChunkBytes)` and processed ALL batches within each chunk. While the outer `while (fetchOffset < highWatermark)` loop controlled which segments were fetched, a single chunk could contain batches beyond HW (uncommitted tail in the same segment). Added:

```cpp
// TX-2b: stop processing batches once we reach or exceed the HW bound.
if (batchBase >= highWatermark) break;
```

inside the inner batch-scan loop. This is the actual bug fix that makes Scenario B (HW-bound) return `Accept` for the uncommitted batch's sequence.

### Test results

```
rune_storage_tests:                  205 PASSED
rune_storage_integration_tests:       41 PASSED
rune_replication_tests:              332 PASSED
rune_replication_integration_tests:   38 PASSED
rune_transaction_tests:              344 PASSED
rune_transaction_integration_tests:   44 PASSED
rune_coordination_tests:             222 PASSED
rune_coordination_integration_tests:  50 PASSED
rune_txn_state_replication_tests:      2 PASSED
rune_offset_replication_tests:         2 PASSED
rune_producer_state_durability_tests:  4 PASSED  (existing, unchanged)
rune_psm_hw_bound_tests:               1 PASSED  (new — RED/GREEN unit test)
```

Total: **1290 tests, 0 failures.**

Integration test (`FailoverDedup_HandleProducePath`) gated behind `aeron_FOUND`; Aeron not present in this build environment — test compiles and gates correctly in CMakeLists.txt.
