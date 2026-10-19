# CG-2 Report — ReplicatedOffsetStore

**Status:** DONE  
**Commit:** `06dab66`  
**Tests:** 7 unit + 2 integration — all pass (220 coordination total, 332 replication, 2 integration)

---

## Files produced

| Path | Role |
|---|---|
| `libs/coordination/include/rune/coordination/ReplicatedOffsetStore.h` | Public interface (exact names from brief) |
| `libs/coordination/src/ReplicatedOffsetStore.cpp` | Implementation |
| `libs/coordination/tests/coordination/ReplicatedOffsetStoreTest.cpp` | 7 unit tests |
| `libs/node/tests/integration/OffsetReplicationTest.cpp` | 2 integration tests |
| `libs/coordination/CMakeLists.txt` | Wire new source + test + link `rune_replication` + `RUNE_PROTOCOL_INCLUDE` |
| `libs/node/CMakeLists.txt` | Wire `rune_offset_replication_tests` target (no Aeron gate) |

---

## Design decisions

### On-wire payload layout (key decision)

The brief says "build the batch → `replica->append(batchBytes)`".  However, `PartitionReplica::append()` (line 255 of `PartitionReplica.cpp`) **always** wraps whatever bytes the caller supplies as the VALUE of a single Kafka v2 record inside a fresh `RecordBatch` (via the internal `makeBatch()` helper).  The wrapper record uses an empty key.

Consequence: passing a pre-built `RecordBatchBuilder` output to `append()` would result in the Log containing `RecordBatch { record { key="", value=<inner RecordBatch bytes> } }`.  In `rebuildFromLog()` the `forEach` callback would receive `key=""` and `value=<inner RecordBatch bytes>` — my key/value encoding would be invisible at the outer level.

**Fix:** the payload passed to `append()` is a compact LE-encoded blob:

```
payload = keyLen(uint16) + key_bytes + valLen(uint16) + val_bytes

key_bytes = groupLen(uint16) + group(UTF-8) + topicLen(uint16) + topic(UTF-8) + partition(int32)
val_bytes = offset(int64) + tsNs(int64) + metaLen(uint16) + meta(bytes)
```

All fields are little-endian (consistent with the rest of the Rune wire protocol).

In `rebuildFromLog()` the outer `RecordBatchReader::forEach` yields records with:
- `key = ""` (the wrapper's empty key — ignored)
- `value = <our compact payload>`

`applyRecord(key, val)` ignores `key` and decodes the entire payload from `val`.

### rebuildFromLog() strategy

1. Guard on `highWatermark() <= 0` (nothing committed yet → return immediately).
2. Call `replica_->read(0, INT32_MAX)` to read all committed bytes `[0, HW)`.
3. Walk the returned buffer batch-by-batch using `RecordBatchReader`:
   - Validate each batch with `ensureValid()`.
   - Use `totalLength()` to advance the position cursor.
   - Call `forEach` to visit records.
4. For each record's value field, decode the compact payload and apply to `index_` with last-write-wins semantics (including lower-offset overwrites).

### In-memory index

Identical structure to `OffsetStore`:
```
std::unordered_map<std::string, std::unordered_map<TopicPartition, CommittedOffset>> index_;
```
`TopicPartition` is hashable via the existing `std::hash` specialisation in `Types.h`.

---

## Allocation behavior (zero-alloc analysis)

### commit() hot path

| Allocation | Source | Amortised? |
|---|---|---|
| `keyScratch_` growth | member vector, pre-reserved 256 bytes | Yes — grows only once for typical group+topic names |
| `valScratch_` growth | member vector, pre-reserved 64 bytes | Yes — 18 bytes + meta, rarely exceeds reserve |
| `batchScratch_` growth | member vector, pre-reserved 512 bytes | Yes — payload is always < 512 bytes for realistic inputs |
| `replica_->append()` internal `makeBatch()` | creates a `std::vector<uint8_t>` for the wrapper batch | **Not amortised** — one allocation per commit (inside the replica layer, not our code) |
| `in-memory map update` (`index_[group][tp] = ...`) | `std::string` copy for metadata + possible map rehash | First commit per (group,tp): map node + string alloc; subsequent commits: string assign (may reuse capacity) |

**Concern for a future zero-alloc pass:** The `PartitionReplica::append()` → `makeBatch()` path allocates a fresh `std::vector<uint8_t>` for the wrapper batch on every call (it cannot reuse a caller-supplied buffer because `makeBatch` returns by value).  A zero-alloc path would require a `PartitionReplica::appendRaw(span, outBuf)` variant that writes the RecordBatch into a caller-supplied buffer, or an internal pre-allocated batch-scratch in the replica that the caller can request reuse of.  This is out of scope for CG-2.

### fetch() / fetchAll() — cold path

Both are O(1) / O(n) map lookups.  `fetchAll` returns a `std::vector` allocated on call — acceptable for a cold path.

### rebuildFromLog() — cold path (called once on leader election)

Allocates the `data` vector from `replica_->read()` (proportional to the total committed log size), plus map insertions for each record.  Fully acceptable for a cold path.

---

## Test summary

### Unit tests — `libs/coordination/tests/coordination/ReplicatedOffsetStoreTest.cpp`

Single-node leader (minIsr=1, ISR={self}) — HW advances immediately after each `append()`.

| # | Test | What it covers |
|---|---|---|
| 1 | `CommitFetchRoundTrip` | offset + metadata + timestamp preserved |
| 2 | `OverwriteHigherOffset` | commit 42 then 99 → fetch 99 |
| 3 | `LowerOffsetOverwriteAllowed` | commit 99 then 5 → fetch 5 |
| 4 | `FetchUncommittedReturnsNullopt` | missing group / tp / partition → nullopt |
| 5 | `FetchAllReturnsGroupPairs` | fetchAll returns correct pairs; other group empty |
| 6 | `RebuildFromLogReconstructsState` | fresh store + rebuildFromLog() = identical state (incl. lower-offset overwrite) |
| 7 | `GatedReplicaTest.CommitReturnsFalseWhenGated` | minIsr=2 ISR={self} → commit returns false; map unchanged |

**Result:** 7/7 pass.

### Integration tests — `libs/node/tests/integration/OffsetReplicationTest.cpp`

3-in-process-node cluster using `InMemoryTransport` (no Aeron required).

| # | Test | What it covers |
|---|---|---|
| 1 | `OffsetSurvivesLeaderKillAndRebuild` | commit on leader (acks=All) → kill leader → new leader rebuildFromLog() → fetch returns committed offset (NO loss) |
| 2 | `MultipleOffsetsAllSurviveFailover` | 4 commits (incl. lower-offset overwrite) → failover → all reconstructed correctly |

**Result:** 2/2 pass.

### Full suite

- `rune_coordination_tests`: 220/220 (includes 7 new + 213 pre-existing)
- `rune_replication_tests`: 332/332 (no regressions)
- `rune_offset_replication_tests`: 2/2

---

## Build commands verified

```bash
export PATH="/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin:$PATH"
cd ~/Projects/rune-broker/build
cmake . -DBUILD_TESTING=ON
cmake --build . --target rune_coordination_tests rune_offset_replication_tests -j4
./libs/coordination/rune_coordination_tests
./libs/node/rune_offset_replication_tests
```

---

## Concerns / future work

1. **`PartitionReplica::append()` allocates per call** (the internal `makeBatch()` vector).  This is the dominant remaining allocation on the commit hot path.  A zero-alloc pass would need a `buildInto(span)` seam in the replica layer (the `RecordBatchBuilder::buildInto` seam exists; needs a `PartitionReplica::appendInto` wrapper).  Deferred to DR-9 M1 zero-copy card.
2. **No compaction** — the Log grows with every commit even for the same (group,tp) key.  A log-compaction pass (CG-6 or later) would compact the `__consumer_offsets` partition.
3. **`rebuildFromLog()` clears the index** before replaying.  If called on a live leader (not at startup) it would briefly expose an empty map.  The contract is "call once after becoming leader, before accepting commits" — callers must enforce this.

---

## Fix round 1 — cg2-fix1-findings.md (commit `bad535d`)

**Status:** DONE  
**Date:** 2026-09-16

### Finding #1 — rebuildFromLog must span ALL log segments (FIXED)

**Root cause:** `Log::read(startOffset, maxBytes)` resolves the single segment
whose base offset is `<= startOffset` via `upper_bound()` and reads only that one
segment.  The old `rebuildFromLog()` called `replica_->read(0, INT32_MAX)` once,
getting only the first segment's data.  Any commit written after the first segment
roll was silently dropped from the rebuild — committed offsets lost on failover.

**Fix in `ReplicatedOffsetStore.cpp::rebuildFromLog()`:** replaced the single
`read()` call with a loop:
```
fetchOffset = 0
while fetchOffset < hw:
    data = replica_->read(fetchOffset, 64 MiB)
    if data empty: break
    walk batches → applyRecord each; track lastBatchEnd = batchBase + lastOffDelta + 1
    if no progress: break
    fetchOffset = lastBatchEnd
```
Each iteration covers exactly one segment (or part of one).  The loop terminates
when `fetchOffset >= hw`, when `read()` returns empty, or when no valid batch is
decoded (corrupt leading batch guard).

**New test (`RebuildAcrossSegmentRoll/ReconstructsAllCommitsAcrossRoll`):**
constructs `Log` with `segmentRollBytes=512`, commits 10 records (each ~100-byte
RecordBatch), asserts `log.segmentBaseOffsets().size() > 1` (proves roll happened),
then constructs a fresh `ReplicatedOffsetStore` and calls `rebuildFromLog()`.
Asserts every committed (group, topic, partition) → offset is reconstructed with
correct value and metadata (last-write-wins across the segment boundary).

### Finding #2 — Guard length narrowing to uint16 (FIXED)

**Fix in `ReplicatedOffsetStore.cpp::commit()`:** added three guards at the top of
`commit()` — return false immediately if `group.size() > UINT16_MAX`,
`tp.topic.size() > UINT16_MAX`, or `metadata.size() > UINT16_MAX`.  This prevents
a silent cast truncation that would encode a wrong length and mis-frame every
record written after the offending one in `rebuildFromLog()`.

**New test (`CommitLengthGuard/RejectsOversizedFields`):** passes a 65536-character
string as each of the three guarded fields in turn, asserts `commit()` returns
false and the in-memory map is unchanged.  Also asserts that a normal commit
immediately afterward still succeeds.

### Finding #3 — Header allocation comment (FIXED)

**Fix in `ReplicatedOffsetStore.h` comment block:** replaced the inaccurate phrase
"amortised by the replica's own scratch" with an accurate description: CG-2's own
path (keyScratch_, valScratch_, batchScratch_) is alloc-free after warm-up; the
allocation is in `PartitionReplica::append()` → `makeBatch()` which returns a
fresh `std::vector<uint8_t>` by value (one heap alloc per commit).  Notes
`RecordBatchBuilder::buildInto` as the seam for a future zero-alloc path and
defers it to the DR-9 M1 zero-copy card.

### Test results after fix

| Suite | Before | After | Delta |
|---|---|---|---|
| `rune_coordination_tests` | 220/220 | 222/222 | +2 new |
| `rune_replication_tests` | 332/332 | 332/332 | 0 regressions |
| `rune_offset_replication_tests` | 2/2 | 2/2 | 0 regressions |

### Build commands

```bash
export PATH="/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin:$PATH"
cd ~/Projects/rune-broker/build
cmake --build . --target rune_coordination_tests rune_offset_replication_tests rune_replication_tests -j4
./libs/coordination/rune_coordination_tests
./libs/node/rune_offset_replication_tests
./libs/replication/rune_replication_tests
```

### Concerns (no new ones introduced)

No new concerns.  The prior concern about `INT32_MAX` as `maxBytes` is now moot
(the fix uses a 64 MiB chunk size per iteration, and the loop is naturally bounded
by `hw`).  All prior concerns carry forward unchanged.
