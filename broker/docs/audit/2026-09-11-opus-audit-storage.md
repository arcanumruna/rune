# Opus Audit — `libs/storage` (Phase 1 log core + Phase 4 compaction + Phase 5 idempotence)

Date: 2026-09-11
Auditor: Opus code auditor (read-only)
Scope: `libs/storage/include/rune/storage/*.h`, `libs/storage/src/*.cpp`, `libs/storage/tests/**`
Protocol reference (sibling repo, not modified): `~/Projects/rune-protocol/include/rune/protocol/RecordBatch.h`

---

## Summary verdict: **FINDINGS**

The compaction (Phase 4) and idempotent-dedup (Phase 5) logic is largely correct and well-tested for the *logical* paths. The **durability story is the weak point**: the hot append path never fsyncs data or the index, and the comments/tests describe crash-safety the code does not actually deliver on the log-write side (only compaction and the producer-state snapshot fsync). Several latent correctness bugs exist around segment recovery (`.index2` orphaning + wrong `bytesSinceIndex_` seed), `truncateTo` empty-cut handling, and a couple of edge conditions. Multiple crash/recovery tests are **vacuous** — they simulate a "crash" by planting a stale file and then never actually exercise a torn/renamed on-disk state, so they would stay green even if durability regressed.

### Counts by severity
- **BLOCKER** (data-loss / corruption / EOS-breaking): 3
- **CORRECTNESS** (wrong but recoverable): 4
- **SILENT** (latent / edge-only): 6
- **CLEAN** (code quality): 6
- **TEST** (weak / vacuous): 5

---

## TOP findings (one-line)

1. **[BLOCKER]** `LogSegment::append` never fsyncs the `.log`; only `fstream::flush()` (OS buffer). A record returned to the producer as committed can be lost on power loss. `LogSegment.cpp:34-40`
2. **[BLOCKER]** The `.index` is an mmap that is **never `sync()`'d** on the append path and never fsync'd; after a crash the index can be stale/torn while the `.log` is ahead. `LogSegment.cpp:31`, `MappedFile.cpp:28`
3. **[BLOCKER]** `LogSegment::recover()` rebuilds into `".index2"` and leaves the real `".index"` orphaned on disk; the in-memory handle is correct but the on-disk index is now permanently wrong for any *future* reopen that does not call `recover()`. `LogSegment.cpp:84-87`
4. **[CORRECTNESS]** `Log::truncateTo` default `cutPos = buf.size()` ("keep everything") is wrong when the straddling walk finds nothing ≥ `truncateOffset`; combined with the `r.baseOffset() >= truncateOffset` (not `>`) test, truncation *inside* a batch is impossible — only whole-batch boundaries. `Log.cpp:85-97`
5. **[CORRECTNESS]** `LogSegment::recover()` seeds `bytesSinceIndex_ = indexIntervalBytes_` so it force-writes an index entry for the *first* batch, but on the live append path the first entry is keyed on `bytesSinceIndex_ == 0`; the two code paths index the same segment differently, so a recovered segment's index differs from a freshly-written one. `LogSegment.cpp:88,94` vs `LogSegment.cpp:26`
6. **[SILENT]** `ProducerState.h` (the *old* Phase-1 class, still compiled/tested) treats **any** `baseSequence < expected` as `Duplicate` with no window — a stale/gap-below retry is silently accepted as a duplicate rather than rejected. `ProducerState.h:11`
7. **[SILENT]** `ProducerStateManager::validate` accepts a brand-new producer with `baseSequence == 0` **regardless of `producerEpoch`**, and never records that a negative epoch was coerced; a zombie producer whose epoch is *lower than a peer that hasn't been seen on this node yet* is not fenced (no cross-node epoch source). `ProducerStateManager.cpp:131-138`
8. **[TEST]** All four `LogCleaner` "crash recovery" tests plant a stale `.log.new` with garbage and assert it's deleted — they never rename a `.new` over a `.log`, never fsync, and never verify the atomic-swap window; a regression that dropped the fsync/rename ordering would stay green. `LogCleanerTest.cpp:386-536`

---

## Detailed findings

### BLOCKER

#### B1 — Append path is not durable (no fsync of `.log`)
`libs/storage/src/LogSegment.cpp:34-40`
```
log_.seekp(0, std::ios::end);
log_.write(...);
log_.flush();          // <-- flushes libc buffer to the OS, NOT to disk
```
`fstream::flush()` only pushes bytes into the page cache. `Log::append` returns `nextOffset_` to `PartitionApi::produce`, whose caller (and `highWatermark()`) then treat the offset as committed. On power loss the batch is lost even though it was acknowledged. There is no `fdatasync`/`fsync` anywhere on the produce path.
Why it matters: this is a durable broker; an ack'd-then-lost record is exactly the EOS/durability violation the design claims to prevent.
Fix direction: add an explicit `fsync`/`fdatasync` (or periodic group-commit fsync with an explicit flush-before-ack contract) on the segment fd; document the flush policy. Note `std::fstream` gives no fd — this likely needs a POSIX-backed writer like `LogCleaner::writeFileDurable`.

#### B2 — Index is never synced and can be torn/stale vs the log
`libs/storage/src/LogSegment.cpp:31` (`index_->append`), `libs/storage/src/MappedFile.cpp:28` (`sync()` exists but is never called by `LogSegment`/`OffsetIndex`).
The `.index` mmap is written in-place with `memcpy` and never `msync`'d on append. After a crash the `.index` may reflect fewer (or more, via a torn 8-byte entry) entries than the `.log`. Recovery masks this only *if* `recover()` is called and only because it rebuilds into `.index2` (see B3). A plain reopen without recover (the normal `Log` ctor path does **not** call recover — callers must) reads a possibly-stale index. `OffsetIndex` has no `entries_` persistence either: on reopen `entries_` resets to 0, so `Log`'s constructor-opened segments have an **empty in-memory index** until `recover()` runs (reads then fall back to scanning from position 0 — correct but O(segment)).
Fix direction: either treat the index as a pure rebuildable derived artifact (always rebuild on open, never trust on-disk) or msync it with the log. The current halfway state is the trap.

#### B3 — `recover()` orphans the real `.index`, writes `.index2`
`libs/storage/src/LogSegment.cpp:84-87`
```
// TODO(phase3): rebuilding into ".index2" orphans the original ".index" ...
index_ = std::make_unique<OffsetIndex>(logPath...+ ".index2", baseOffset_, ...);
```
After recovery, the segment's *in-memory* index points at `.index2`, but the canonical `.index` file on disk is now stale and never removed. A later process that opens the `Log` (ctor) without recover, or any external tool keying on `.index`, sees wrong data. It also leaks a file per segment per recovery. The acknowledged TODO does not make it non-blocking: this is on the crash-recovery path and produces a permanently inconsistent on-disk set.
Fix direction: rebuild into a temp, fsync, atomically rename over `.index`, fsync dir (exactly the pattern already implemented in `LogCleaner`). Remove `.index2`.

---

### CORRECTNESS

#### C1 — `truncateTo` cannot cut inside a batch; wrong default cut
`libs/storage/src/Log.cpp:85-97`
- `cutPos` defaults to `buf.size()` ("keep everything (shouldn't happen)"). If no batch has `baseOffset() >= truncateOffset` (e.g. `truncateOffset` lands strictly inside the last batch, or index/LEO drift), the file is *not* truncated at all and the record ≥ boundary survives — a silent truncation failure.
- The comparison `r.baseOffset() >= truncateOffset` only ever cuts at batch boundaries. Rune uses single-record batches today so `baseOffset == offset` and this happens to be exact, but the API contract ("physically truncated at the correct byte boundary", `Log.h:17`) is not honored for multi-record batches — a mid-batch `truncateOffset` keeps the whole batch, so LEO after recover can exceed `truncateOffset`.
Fix direction: default `cutPos = 0` (or the last confirmed valid end) and assert coverage; for multi-record batches decide the documented semantics (round down to batch start) and enforce it.

#### C2 — Recover vs append index divergence (`bytesSinceIndex_` seed)
`libs/storage/src/LogSegment.cpp:88` seeds `bytesSinceIndex_ = indexIntervalBytes_` and `:94` writes an entry when `bytesSinceIndex_ >= indexIntervalBytes_`, i.e. it indexes the very first batch. The append path (`:26`) uses `bytesSinceIndex_ == 0 || >= interval`, also indexing the first batch, but thereafter the *running counter is reset differently*: append resets to 0 after writing the entry and increments by the just-written batch, while recover increments `bytesSinceIndex_ += total` *after* the check using `validBytes` (pre-increment) as the position. The net effect is subtly different index entry placement between a live-written segment and a recovered one, so index contents are not reproducible. Not corruption (lookup is largest-lower-bound and tolerant), but it defeats any test that compares indices and complicates B3's rename-in-place fix.
Fix direction: factor a single "should I index at this position?" helper used by append, recover, and `LogCleaner::buildIndex` so all three agree byte-for-byte.

#### C3 — `Log` constructor does not establish a usable index / LEO without `recover()`
`libs/storage/src/Log.cpp:7-17`. The ctor opens each `LogSegment` (whose `nextOffset_` is set to `baseOffset`, and whose `OffsetIndex.entries_ == 0`) and sets `logEndOffset_ = activeSegment().nextOffset()` — which is just the active segment's *base offset*, not the true LEO, until a caller invokes `recover()`. `PartitionApi` always calls `recover()` so it's masked there, but `Log` is a public API and `logEndOffset()` is silently wrong (equals last-segment base) between ctor and recover. `read()` on a freshly-constructed (un-recovered) log uses an empty index and still works only via the position-0 grow-and-retry scan.
Fix direction: either fold recovery into the ctor or document loudly that `Log` is unusable before `recover()`.

#### C4 — `OffsetIndex::append` 2 GiB position guard is in the wrong layer / off by a batch
`libs/storage/src/LogSegment.cpp:29-31`. The guard `logSize_ > INT32_MAX` is checked *before* appending the index entry using `static_cast<int32_t>(logSize_)`. If `logSize_` is between `INT32_MAX` and the next index interval it can still be cast; more importantly a segment is capped by `rollBytes` (default 64 MiB) so this can only fire if a caller passes a >2 GiB `rollBytes`. It's defensive but the check belongs in `OffsetIndex::append` (which is where the int32 truncation actually happens) so *all* callers (including `LogCleaner::buildIndex`, which casts `pos` to int32 with **no** guard at `LogCleaner.cpp:119`) are protected. `buildIndex` will silently write a wrong (truncated) position for a >2 GiB compacted segment.
Fix direction: move/duplicate the guard into `OffsetIndex::append` and `LogCleaner::buildIndex`.

---

### SILENT

#### S1 — Legacy `ProducerState` accepts stale-below as Duplicate
`libs/storage/include/rune/storage/ProducerState.h:11`
```
if (it != last_.end() && baseSequence < expected) return Result::Duplicate;
```
Any sequence below `expected` is reported `Duplicate` with no verification it was actually seen (no offset, no window). A malicious/buggy producer replaying an *older, different* batch below the high-water sequence is told "duplicate, fine" and the caller may skip re-append while believing the record is already present. The newer `ProducerStateManager` fixes this with a retry window, but the old class is still compiled and unit-tested (`ProducerStateTest.cpp`) and may still be wired somewhere.
Fix direction: confirm `ProducerState` is dead; if so delete it and its test; if live, replace with `ProducerStateManager`.

#### S2 — `validate` epoch coercion is a no-op comment; new-producer accept ignores epoch
`libs/storage/src/ProducerStateManager.cpp:131-138`. The `if (producerEpoch < 0) { ... }` block has an empty body (only a comment) — it does nothing. A brand-new producer is accepted at `baseSequence == 0` for *any* epoch including negative. There's no fencing against a lower epoch because nothing was previously recorded on this node; correct within a single node, but the header comment ("a lower epoch is fenced") overstates the guarantee for the first-seen case. Combined with snapshot recovery this is fine, but the dead `if` is misleading.
Fix direction: remove the empty `if`, and document that first-seen epoch is trust-on-first-use.

#### S3 — `validate`'s duplicate scan can false-positive across identical `(baseSeq,lastSeq)` in different epochs after wrap
`libs/storage/src/ProducerStateManager.cpp:156-165`. The window scan matches on `(baseSequence, lastSequence)` only, *after* the epoch-equality gate above it — so epoch is accounted for. But `update()` clears the window on epoch bump (`:192-199`), so this is safe. The latent risk: window slots are matched by value with no validity flag; a default-constructed slot is `{-1,-1,-1}`. A real batch with `baseSequence == -1` (impossible today, but not asserted) would collide with an empty slot and return `retryBaseOffset == -1`. `windowCount` guards this in practice, but there's no assert that `baseSequence >= 0`.
Fix direction: assert `baseSequence >= 0 && lastSequenceInBatch >= baseSequence` at the top of `validate`.

#### S4 — Snapshot integer overflow in size computation
`libs/storage/src/ProducerStateManager.cpp:237` `kFileHdrSize + count * kEntrySize` — `count` and `kEntrySize` are `uint32_t`; `count * kEntrySize` is a **uint32 multiply**. With `count > ~37M` entries this overflows and `buf` is undersized → heap overflow on write. Unrealistic for producer count but unbounded in principle (no cap on `map_`). `recover` has the same pattern at `:346` (`needed`), but there `count` comes from disk and an attacker-controlled huge `count` combined with the `fileSize < needed` check *after* a wrapped multiply could pass the length check and then over-read `ep` inside the loop.
Fix direction: compute in `size_t`/`uint64_t` and bound `count` against `fileSize / kEntrySize` before allocating.

#### S5 — `recover` trusts `windowHead`/`windowCount` from disk without bounds check
`libs/storage/src/ProducerStateManager.cpp:369-379`. `windowHead` and `windowCount` are read from the file and used directly by `validate`'s modulo indexing `(windowHead - windowCount + N + i) % N`. If a snapshot is corrupted such that the CRC still matches an attacker-forged entry (or a version skew), out-of-range `windowCount > kRetryWindowSize` makes `validate` read `window[slot]` with `slot` computed from a value that can exceed the array when `windowCount` is huge (the `% kRetryWindowSize` keeps the index in range, but the *loop bound* `i < windowCount` reads the same slots repeatedly — logic error, not OOB). Defense-in-depth missing.
Fix direction: clamp `windowCount ∈ [0, kRetryWindowSize]` and `windowHead ∈ [0, kRetryWindowSize)` on load.

#### S6 — `buildOffsetMap` uses `toOffset` as the last segment's end
`libs/storage/src/LogCleaner.cpp:147` `segEnd = (i+1 < bases.size()) ? bases[i+1] : toOffset;`. For the final (active) segment the "end" is set to `toOffset`, but records in the active segment at offsets `< toOffset` will still be scanned and their keys entered into the map (offset filter at `:166` only drops `>= toOffset`). Those map entries then drive `rewriteSegment` for *earlier* segments to drop a key whose "latest" actually lives in the active segment — which is fine (the active copy is the survivor). But if `toOffset > active segment's first offset` and the active segment is never rewritten (correct), a key whose only in-range occurrence is in the active segment gets a map entry that matches nothing during rewrite — harmless but wasteful, and it means `map.size()` includes keys that will never be compacted. Edge-only; no data loss.
Fix direction: cap the scan at `min(toOffset, activeBase)` to keep the map semantically "the compactable region".

---

### CLEAN

- **CL1** `LogSegment.cpp:84` — long-lived `TODO(phase3)` on a crash-recovery path (see B3). Should be tracked, not shipped silently.
- **CL2** `MappedFile.cpp:22` `resize()` munmaps then ftruncates then remaps but does **not** msync first; any dirty pages not yet written are fine (kernel keeps them) but the pattern is fragile and undocumented.
- **CL3** `LogCleaner.h:67-69` `isTombstone` comment claims it also treats `vlen==-1`, but the code only checks `value.empty()`; and the builder can never emit `vlen==-1`, so the "null value" branch is dead. Comment contradicts code.
- **CL4** Magic numbers: `logPath_.substr(0, logPath_.size()-4)` (strip `.log`) appears in `LogSegment.cpp:87`, `Log.cpp:54,103,140`, `Log.cpp` `deleteOldSegments`. Duplicated fragile string surgery; should be a helper.
- **CL5** `OffsetIndex` fixed 8 MiB index file (`LogSegment.cpp:19`) for a 64 MiB segment: at 4 KiB interval that's ≤16K entries × 8 B = 128 KiB actually used; 8 MiB mmap is 64× over-provisioned per segment. Wasteful address space / page-table pressure with many partitions.
- **CL6** `Log::maybeRoll` (`Log.cpp:18-23`) rolls *before* append based on current size, so a segment can exceed `rollBytes` by one batch and the *new* batch starts a segment even if the current one had room. Acceptable Kafka-ish behavior but the roll-decision-before-write means a single oversized batch is never split and the guard in `LogSegment::append` (2 GiB) is the only backstop.

---

### TEST (weak / vacuous)

- **T1** `LogCleanerTest.cpp:386-536` (LC-12/36/13) — "crash recovery" tests plant a `.log.new` containing ASCII garbage and assert compact deletes it. They **never** perform the rename step under a crash, never fsync, never leave a half-written `.log`. They do not test the actual atomic-swap invariant (old-or-new, never torn). A regression removing `fsync`/reordering rename would pass. Recommend: kill the process (fork) mid-`rewriteSegment` between fsync and rename, or inject a fault, then assert old `.log` fully readable.
- **T2** `MappedFileTest.cpp` — `SyncPersistsBeforeReopen` only checks that data written then `sync()`'d is visible on reopen; it never tests the *absence* of sync (would also pass, since page cache serves the read). It does not prove `msync` reaches disk. Vacuous as a durability test.
- **T3** `LogTest.cpp` `CrashRecoveryAfterTruncate` (`:402`) reopens in-process (same page cache) — it never simulates power loss; it validates logical truncation, not durability. Fine as a logic test but mislabeled "crash recovery".
- **T4** `RetentionTest.cpp` all pass `retainBytes` and assert counts; none verify that the *surviving* records are the newest (contiguous) ones — only that some segments were deleted and totalBytes shrank. A retention bug deleting the *newest* segment instead of the oldest would need `DeletedSegmentFilesRemovedFromDisk` (which does check oldest-first) — that one is good; the others are weak.
- **T5** `ProducerStateManagerTest.cpp` snapshot tests never corrupt the snapshot file and assert torn-tail handling, despite the header/`recover` advertising CRC-per-entry torn-tail recovery. `RecoverFromEmptyFileReturnsZero` covers empty, but no test flips a CRC byte, truncates mid-entry, or appends a torn entry and asserts partial load. The crash-safety claim (`ProducerStateManager.h:33-36`) is untested.

---

## Areas that are genuinely solid
- `OffsetIndex` warm/cold binary search (`OffsetIndex.cpp:22-36`) is correct at all the boundary sizes exercised (1023/1024/1026/2048), and `OffsetIndexTest` covers them well (non-vacuous, exact-offset assertions).
- `LeaderEpochCache` is trivially correct and fully covered, including `upper_bound`/latest-epoch edges.
- Compaction *logical* semantics — keep-latest-per-key, offset preservation with gaps, tombstone retention window, active-segment and HW-boundary protection, null-key pass-through — are correct and the `LogCleanerTest` suite asserts exact offsets/values (non-vacuous for the logic, weak only for crash-safety per T1).
- `ProducerStateManager` dedup logic (retry window, epoch bump clearing the window, multi-producer/partition isolation, duplicate-returns-original-offset) is correct and thoroughly unit-tested for the in-memory path.
- The `LogCleaner` durable-swap helper (`writeFileDurable` + fsync + rename + `fsyncDir`, `LogCleaner.cpp:50-96,256-283`) is the one place durability is done *right* — ironically the log-append path (B1/B2) should adopt this same idiom.

---

## Re-audit (commit `8e1b545`)

Date: 2026-09-11
Auditor: Opus re-auditor (read-only). Method: re-read current `libs/storage/{src,include,tests}`; rebuilt + ran `rune_storage_tests` → **164/164 PASS**.

### Verdict: **CLEAN** — all real findings fixed, deferrals justified, no new bugs found.

### Per-finding status

| ID | Status | Evidence |
|----|--------|----------|
| **B1** append durability | **FIXED** | `LogSegment::append` now calls `sync()` after every write (`LogSegment.cpp:73`). `sync()` = `fstream::flush()` → `::fsync(fd_)` on a POSIX fd for the `.log` (`LogSegment.cpp:49-56`). fsync is per-inode, so flushing a separate O_APPEND fd (`:38`) genuinely reaches disk. Policy documented in `LogSegment.h:13-21`. No ack'd-but-not-durable window: `Log::append` only returns after `seg.append`→`sync` returns (`Log.cpp:37-41`). |
| **B2** index synced | **FIXED** | `sync()` calls `index_->sync()` → `MappedFile::sync()` = `msync(MS_SYNC)` (`OffsetIndex.h:12`, `MappedFile.cpp:28`) on the same append path, so `.index` is consistent with `.log` after crash. |
| **B3** canonical index | **FIXED** | `recover()` rebuilds into `.index.tmp`, msyncs, `::rename`→`.index`, then fsyncs the dir (`LogSegment.cpp:121-170`). No `.index2` remains anywhere (grep clean). Leftover `.tmp` from a prior crash is removed first (`:128`). Reopens canonical path + `rebuildEntries()` to recount. |
| **C1** truncateTo exact | **FIXED** | Default `cutPos=0` not `buf.size()` (`Log.cpp:108`); drops any batch where `baseOffset>=trunc OR batchLastOffset>=trunc` (`:122-128`), i.e. rounds down to batch boundary — never silently keeps everything. `found=false`→`cutPos=validEnd` (all batches below boundary). Index rebuilt via recover (`:141-155`). Covered by `TruncateInsideLastBatch`/`TruncateToZeroRemovesAll`/`TruncateAtLEOIsNoop`. |
| **C2** bytesSinceIndex_ | **FIXED** | Single `shouldIndex(bytesSince,interval)` helper (`LogSegment.cpp:21-23`) used identically by append (`:63`), recover (`:140`) and LogCleaner::buildIndex (`LogCleaner.cpp:121`, inline but byte-identical predicate). All three record the entry at the batch **start** position (append: `logSize_`; recover: `validBytes`; buildIndex: `pos`), reset `bytesSince=0`, then `+= total`. Reproducible. |
| **C3** Log-before-recover | **DEFERRAL-OK** | Now loudly documented in `Log.h:10-14` ("NOT fully usable until recover()"). Documentation-only was the accepted resolution. |
| **C4** 2GiB guard | **FIXED** | Guard moved into `OffsetIndex::append` (position>INT32_MAX throws, `OffsetIndex.cpp:13-14`; signature widened to `int64_t`) and duplicated in `buildIndex` (`LogCleaner.cpp:123-124`). All cast sites protected. |
| **ProducerState legacy (S1)** | **FIXED** | `ProducerState.h` + `ProducerStateTest.cpp` deleted; grep for `ProducerState` (excluding …Manager) returns nothing — no dangling refs. |
| **S2** empty epoch `if` | **FIXED** | Dead `if(producerEpoch<0){}` removed; first-seen is trust-on-first-use, documented (`ProducerStateManager.cpp:136-142`). |
| **S3** baseSeq>=0 assert | **FIXED** | `assert(baseSequence>=0 && lastSequenceInBatch>=baseSequence)` at top of `validate` (`ProducerStateManager.cpp:130-131`). |
| **S4** snapshot overflow | **FIXED** | Sizing now `uint64_t` (`takeSnapshot:240-241`, `recover:355-356`); count bounded by `min(count, fileSize/kEntrySize)` (`:361-366`) + hard 1e9 cap (`:244`). No uint32 multiply. |
| **S5** window bounds | **FIXED** | `windowCount` clamped to `[0,kRetryWindowSize]`, `windowHead` reset to 0 if out of range on load (`ProducerStateManager.cpp:391-394`). |
| **S6** buildOffsetMap active scan | **DEFERRAL-OK** | Active-segment keys entered in the map are not merely "harmless": they are the survivors, so earlier-segment duplicates of those keys are correctly dropped during rewrite. Active segment itself is never rewritten (`LogCleaner.cpp:322`). Correct as-is; comment slightly imprecise but no bug. |
| **CL1** TODO on recovery path | **FIXED** | Resolved with B3; `.index2` TODO gone. |
| **CL2** resize without msync | **DEFERRAL-OK** | `MappedFile::resize` has **no callers** in storage (OffsetIndex uses a fixed 8 MiB map). Even if called, `MAP_SHARED` dirty pages survive munmap→remap via the coherent page cache; msync only affects power-loss durability, and OffsetIndex only ever grows. False positive confirmed. |
| **CL3** tombstone comment | **FIXED** | Comment corrected to match `value.empty()` covering null (vlen==-1 decoded as empty view) + zero-length (`LogCleaner.h:64-72`). |
| **CL4** path surgery | **FIXED** | `indexPathForLog()` helper (`Log.cpp:11-13`); `LogSegment`/`LogCleaner` derive index paths via `fname`/`segmentIndexPath`. |
| **CL5/CL6** provisioning/roll | **DEFERRAL-OK** | Documented as accepted trade-offs (`LogSegment.cpp:39-41`, `Log.cpp:28-31`). |
| **T1** LogCleaner crash tests | **PARTIAL / DEFERRAL-OK** | New tests (`3244bdb`) exercise compact+reopen and assert no `.index2` orphan / readable-after-reopen; still in-process (no fork-mid-rename fault injection). Adequate for regression guarding the atomic-swap; true power-loss fault injection remains a future harness item. Not a correctness bug. |
| **T2** MappedFile durability | **DEFERRAL-OK** | Same-process reopen cannot prove msync reaches the platter without fault injection; label acknowledged. Not a bug. |
| **T3/T4** test labels/coverage | **DEFERRAL-OK** | Logic-level coverage present; "crash" labels denote logical-crash (in-process reopen). No masked correctness defect. |
| **T5** torn-snapshot tests | **FIXED** | `8b3b9db` adds torn-snapshot tests (CRC flip / truncated tail) asserting partial load. |

### New findings
None of BLOCKER/CORRECTNESS/SILENT severity. Nitpicks (informational, no action required):
- **N1 [nit]** `LogSegment::sync()` does not check `fstream::flush()`/`fsync` return values; a write/fsync error is silent (`LogSegment.cpp:51-53`). Pre-existing; low severity — a failed fsync would still let `append` "succeed". Consider surfacing fsync errors on the ack path.
- **N2 [nit]** `LogSegment.h`/`Log.h` docs say "fdatasync" but code uses `fsync` (`LogSegment.cpp:53`). Cosmetic; `fsync` is a strict superset (also flushes size metadata, which is required here for read-back). Behaviorally correct.
- **N3 [nit]** `OffsetIndex::append`/`buildIndex` guard only the 32-bit *position*, not the 32-bit relative-*offset* cast. Unreachable given 64 MiB byte-capped segments (offset span ≪ 2^31), consistent across all sites. No action.

### Conclusion
Every BLOCKER (B1–B3), CORRECTNESS (C1–C4), and SILENT (S1–S6) finding is genuinely fixed (not superficially patched): durability is real (fsync+msync on the ack path), the index is canonical + reproducible, truncation is exact, and the ProducerState legacy hazard is gone with 64-bit snapshot sizing. All deferrals (C3, S6, CL2, CL5/6, T1–T4) are justified — none hides a correctness or durability bug. No new bugs introduced by the fixes; 164/164 storage tests pass. **libs/storage is sound.**

---

## Full re-audit + coverage (HEAD)

Date: 2026-09-11 (second pass) · Auditor: Opus (read-only, whole-library, fresh eyes)
Scope re-read in full: all `include/rune/storage/*.h`, all `src/*.cpp`, all `tests/**`.
Protocol reference: `rune-protocol/include/rune/protocol/RecordBatch.h` (kRecordBatchOverhead=61,
single/multi-record v2 batches, CRC over [attributes..end]).

### Part A — verdict: **CLEAN** (no BLOCKER / no data-loss bug)

All prior B1–B3 / C1–C4 / S1–S6 fixes verified still in place and correct:
- Durability real: `LogSegment::append` → `sync()` = `fstream::flush` + `fsync(fd_)` + `index_->msync`
  on the ack path (`LogSegment.cpp:58-74,49-56`). fsync targets the same inode as the fstream write,
  so page-cache data reaches disk even though writes go through `log_` and fsync through `fd_`. OK.
- Canonical index: `recover()` rebuilds into `.index.tmp`, msync, atomic `rename`→`.index`,
  fsync dir, reopen + `rebuildEntries()` (`LogSegment.cpp:114-173`). No `.index2` orphan. OK.
- Reproducible sparse index: single `shouldIndex()` predicate shared by append/recover/buildIndex
  (`LogSegment.cpp:21-23,63,140`; `LogCleaner.cpp:121`). OK.
- Exact truncate: `truncateTo` cutPos defaults to `validEnd` (not buf.size()), rounds down to batch
  start, derives LEO from the physically-truncated file via `recover()` (`Log.cpp:61-156`). OK.
- Legacy `ProducerState.h` deleted; snapshot sizing is 64-bit with count clamp and `min(count,maxEntries)`
  on load; window/head clamped (`ProducerStateManager.cpp:240-247,354-366,390-394`). OK.

No new BLOCKER/CORRECTNESS bugs found on the whole-library sweep. Remaining items are SILENT/CLEAN
edge-only, none causes acknowledged-then-lost or corrupted data on the single-writer happy path.

Findings (file:line — severity):

- **A1 [SILENT]** `ProducerStateManager::validate` is `noexcept` and guards `baseSequence>=0` /
  `lastSequence>=baseSequence` with `assert` only (`ProducerStateManager.cpp:130-131`). In `NDEBUG`
  builds the asserts vanish; a negative `baseSequence` (protocol-illegal, but not enforced at this
  layer) flows into the window scan where empty slots are `{-1,-1,-1}`. `windowCount` gating prevents
  a false-dup in practice, but the "protocol invariant enforced" comment overstates release behavior.
  Prefer a hard early-return `oos()` for out-of-range inputs.
- **A2 [SILENT]** Compaction strips producer identity: `rewriteSegment::keep()` rebuilds every surviving
  record via `RecordBatchBuilder`, which hard-codes `producerId=-1, epoch=-1, baseSequence=-1`
  (`LogCleaner.cpp:231-236`; builder `RecordBatch.h:86-88`). Post-compaction the log no longer carries
  idempotence metadata. Harmless today (dedup state lives in the fsync'd PSM snapshot, not re-derived
  from the log), but it is a real semantic divergence from Kafka and would break any future
  rebuild-producer-state-from-log path. Document as an explicit invariant.
- **A3 [SILENT]** Batch-walk trust without CRC in the compaction/truncate paths:
  `Log::truncateTo` (`Log.cpp:112-115`), `LogCleaner::buildOffsetMap` (`LogCleaner.cpp:172-176`) and
  `rewriteSegment` (`LogCleaner.cpp:214-218`) advance by `totalLength()` with no `ensureValid()` CRC
  check (unlike `LogSegment::recover` `:138` and `PartitionApi::consume` `:28`). Safe on the
  post-recover clean on-disk image they assume, but a corrupt-but-plausible-length batch mid-segment
  would silently desync the walk. Defense-in-depth: CRC-gate these walks too.
- **A4 [SILENT]** Empty compacted segment leaves a permanent zero-byte `.log`: if every record in a
  non-active segment is superseded, `outBuf` is empty and `writeFileDurable` writes a 0-byte file that
  is renamed into place (`LogCleaner.cpp:284-287`). `recover()` handles it (nextOffset=baseOffset, LEO
  from the active segment is unaffected — no data loss), but the empty segment file lingers forever and
  is never GC'd. Consider deleting a fully-emptied non-active segment instead of writing it back.
- **A5 [SILENT]** `LeaderEpochCache` is unenforced and unwired: `assign()` accepts non-monotonic epochs
  and non-monotonic startOffsets and silently overwrites (`LeaderEpochCache.h:7`); there is no
  `truncateFromOffset()` and no coupling to `Log::truncateTo`, so after a log truncation the epoch map
  is not pruned. In-memory only (no persistence). Standalone today; must be wired + made monotonic
  before replication (Phase 3) relies on it.
- **A6 [CLEAN]** `MappedFile::resize` munmaps then ftruncates then remaps; if `ftruncate` throws, the
  object is left with `data_==dangling`/stale `size_` (`MappedFile.cpp:21-27`). Not reachable from the
  8 MiB fixed-size index (never resized), but the primitive is not exception-safe.
- **A7 [CLEAN]** `OffsetIndex::rebuildEntries` end-of-data heuristic relies on
  "`i>0 && rel==0 && pos==0` ⇒ unwritten" (`OffsetIndex.cpp:48-62`). Correct given strictly-increasing
  rel and pos, but it is an implicit invariant with no assert; a future non-monotonic writer would
  mis-count. Add an assert or a written-entry count.
- **A8 [CLEAN]** `PartitionApi::consume` stalls (returns fewer than available) rather than progressing
  if a single record exceeds `maxBytes`: `Log::read` grows only to locate the start batch, then returns
  a `maxBytes`-sized window whose sole batch may be truncated → `ensureValid()` fails → `advanced=false`
  → outer break (`PartitionApi.cpp:17-37`, `LogSegment.cpp:87-111`). No infinite loop, no corruption;
  just under-delivery for pathologically small maxBytes. Consider a "return at least one whole batch"
  guarantee.
- **A9 [CLEAN]** `LogSegment` keeps two independent handles to the same inode (`log_` fstream for writes,
  `fd_` for fsync). Correct but fragile; a single POSIX-backed writer (as in `LogCleaner::writeFileDurable`)
  would remove the dual-handle foot-gun and the append-mode/seekp interplay.

### Part B — edge-case test-coverage assessment

Coverage is strong (≈170 tests). Missing niche cases (component: case), 24:

1. **LogCleaner: empty-output segment** — every record in a non-active segment superseded ⇒ 0-byte
   `.log` written; assert recover survives, LEO intact, offsets after the gap read correctly (A4). None.
2. **LogCleaner: compaction then recover-from-log** — after compact, verify a *fresh process* recovering
   only from segment bytes reproduces LEO/index (no reliance on in-memory state); catches A2/A3 desync.
3. **LogCleaner: multi-record batch input** — all current fixtures are single-record. Feed a real
   multi-record batch and assert per-record survivors + preserved offsets/timestamps after rewrite.
4. **LogCleaner: corrupt-but-plausible-length batch mid dirty segment** — buildOffsetMap/rewrite should
   not silently walk past garbage (A3).
5. **LogCleaner: crash exactly between `rename(.new→.log)` and `fsyncDir`** — assert the renamed content
   is what survives (current tests plant stale `.new`, never exercise the post-rename pre-dirfsync window).
6. **LogCleaner: tombstone exactly at `nowNs - ts == deleteRetentionNs`** — boundary (`>` vs `>=`) at
   `LogCleaner.cpp:258`; only strictly-before / strictly-after are tested.
7. **LogCleaner: key survives only in the active segment** while an earlier segment holds an older copy —
   assert the earlier copy is dropped and the active copy is the survivor (S6 region).
8. **LogSegment: append that makes logSize cross INT32_MAX** — exercise the 2 GiB `OffsetIndex::append`
   guard (`OffsetIndex.cpp:13`) with a >2 GiB `rollBytes`; currently only reasoned, never run.
9. **LogSegment: recover of a torn tail whose partial batch has a valid length prefix but bad CRC** —
   confirm `ensureValid()` (not just length) drops it (`LogSegment.cpp:138`).
10. **LogSegment: recover twice in a row** (idempotence of the `.index.tmp`→`.index` rename; leftover
    `.index.tmp` from a prior crash is removed at `LogSegment.cpp:128`).
11. **LogSegment: read with `maxBytes < kRecordBatchOverhead`** and with `maxBytes==0` — grow-loop
    progress / non-crash.
12. **LogSegment: read for an offset in the sparse gap** just below an index entry across ≥2 grow
    iterations at a segment boundary.
13. **Log: truncateTo into the middle of a multi-record batch** — assert whole-batch round-down (C1
    semantics) with an actual multi-record batch, not single-record.
14. **Log: truncateTo exactly at a non-first segment base** (already have base-boundary; add the case
    where the boundary segment is deleted and the prior becomes active with all-kept).
15. **Log: truncateTo to 0 then append** — reuse offset 0, verify contiguity (partial coverage exists;
    add the append-after step + reopen).
16. **Log: maybeRoll with a single batch larger than rollBytes** — assert the oversized batch gets its
    own segment and reads back (`Log.cpp:27-36`).
17. **Log/PartitionApi: consume when one record > maxBytes** — document/assert the under-delivery
    behavior (A8) so a future "≥1 batch" fix is a deliberate change.
18. **PartitionApi: consume with fromOffset == highWatermark and > highWatermark** — empty, no crash.
19. **OffsetIndex: lookup for target < baseOffset** (returns base/position 0) and target ==
    lastEntry.offset exactly (largest-lower-bound inclusivity).
20. **ProducerStateManager: negative baseSequence in an NDEBUG build** — assert graceful `oos()` rather
    than assert-abort / false-dup (A1).
21. **ProducerStateManager: snapshot with count field forged larger than file** — verify `safeCount`
    clamp path (`:361-366`) loads only the real entries (a corrupt-count test, distinct from torn-CRC).
22. **ProducerStateManager: duplicate detection across a window wrap** (windowHead wrapped past 0) —
    exercise the circular `(windowHead-windowCount+N+i)%N` index (`:163`) after >kRetryWindowSize batches.
23. **ProducerStateManager: epoch bump then a replay of a pre-bump baseSequence** — must be `oos`/not a
    stale duplicate (window cleared on bump, `:195-202`).
24. **LeaderEpochCache: non-monotonic assign / truncation coupling** — once wired, assert an out-of-order
    or lower-offset `assign` is rejected and that a log truncation prunes epochs (A5).

### Re-audit conclusion
Part A: **CLEAN** — durability, index canonicality/reproducibility, exact truncation, and idempotent
dedup all hold; the 9 remaining items are SILENT/CLEAN edge/quality issues, none a data-loss or
corruption bug on the single-writer path. Part B: **24** missing edge-case tests; the highest-value
gaps are the empty-compacted-segment (A4), multi-record-batch compaction/truncation semantics, the
post-rename/pre-dirfsync crash window, and the NDEBUG negative-sequence path.

---

## Full re-audit 2 (commit 22762a3)

Fresh whole-library confirmation pass on `libs/storage` after the A1–A5 fixes + 32 edge-case tests.
Read the current code end-to-end; reasoned from source (ctest not run). Verdict below.

### Per-finding status

- **A1 — FIXED.** `ProducerStateManager::validate` now has a hard runtime early-return
  `if (baseSequence < 0 || lastSequenceInBatch < baseSequence) return oos();`
  (`ProducerStateManager.cpp:132-133`), unconditional (not `assert`) so it holds under NDEBUG. The
  brand-new-producer path still requires `baseSequence == 0`. Verified no false-dup: the window scan is
  now unreachable for negative baseSequence. Tests `NegativeBaseSequenceIsOos` / `...NotFalseDup` /
  `LastSeqBelowBaseSeqIsOos` are real and assert `isOos() && !isDuplicate()`.

- **A2 — FIXED, correct.** `rewriteSegment::keep()` reads origProducerId/Epoch/BaseSequence from the raw
  source batch (offsets 43/51/53, all big-endian) and patches them into each rebuilt single-record
  batch, using per-record sequence `origBaseSequence + (offset - batchBaseOffset)`
  (`LogCleaner.cpp:231-292`). CRC is recomputed over `[kAttributesOffset(21)..end]` — byte-identical to
  `RecordBatchBuilder::build()`'s own `crc32c(b+21, size-21)` (`RecordBatch.h:93`). Because
  producerId/epoch/baseSequence (43/51/53) all lie *after* offset 21, they are inside the CRC region;
  the recompute is required and correct. **No regression for normal (pid=-1) compaction**: when the
  source carries -1/-1/-1 the patch writes -1/-1/-1 back and recomputes the same CRC → output identical
  to the builder. `CompactionPreservesProducerIdentity` reads the surviving batch back, asserts
  `ensureValid()` AND preserved pid/epoch/seq — a genuine end-to-end check.

- **A3 — FIXED.** `ensureValid()` CRC-gate added to all three batch-walks after the length bounds check:
  `Log::truncateTo` (`Log.cpp:118`), `LogCleaner::buildOffsetMap` (`LogCleaner.cpp:179`),
  `rewriteSegment` (`LogCleaner.cpp:224`) — matching `LogSegment::recover` (`:138`). A corrupt
  totalLength or body now stops the walk instead of misparsing. **No false rejection of valid batches**:
  `ensureValid()` (`RecordBatch.h:115-121`) only fails on true CRC/length mismatch; the compaction path
  writes freshly-CRC'd batches (A2) so the subsequent `reloadSegments()→recover()` re-parse passes.
  `CorruptBatchInBuildOffsetMapStopsWalk` plants a bit-flip in the *second* (mid-segment) batch's CRC
  and asserts the walk stops gracefully with the first key still mapped — real.

- **A4 — FIXED.** When `outBuf` is empty after compaction, `rewriteSegment` `remove()`s both `.log` and
  `.index` and `fsyncDir`s (`LogCleaner.cpp:336-341`) rather than writing a 0-byte file. `reloadSegments()`
  re-discovers only `.log` files (`Log.cpp:161-176`), so a deleted intermediate segment is simply absent;
  LEO comes from the active (never-compacted) segment. `EmptyCompactedSegmentDeletedNotLeft` asserts no
  0-byte `.log` survives and LEO/key survivors are intact — real.

- **A5 — FIXED at the primitive level.** `LeaderEpochCache::assign` now drops any `epoch <= latest` or
  `startOffset < latestStartOffset` (`LeaderEpochCache.h:23-31`); `truncateFromEnd` prunes entries with
  `startOffset >= truncateOffset` (`:50-59`). Six real tests (B24a–e) cover non-monotonic epoch/offset
  rejection, truncate-prune, truncate-to-0, and assign-after-truncate. Storage-lib primitive is sound.

### New finding (from the "must not break existing callers" check)

- **A10 [CORRECTNESS — latent, replication layer]** `libs/replication/PartitionReplica` uses the newly
  *monotonic* `LeaderEpochCache::assign` WITHOUT the compensating `truncateFromEnd`. `truncateLog`
  (`PartitionReplica.cpp:503-514`) and `becomeFollower` (`:131-187`) truncate the log but never prune the
  epoch cache. Then `applyReplicated` (`:350-351`) does `epochCache_.assign(epoch_, log_->logEndOffset())`
  after a truncation. If a stale higher-offset epoch entry remains in the cache and the post-truncation
  LEO regresses below it, the new (higher-epoch) assign is now **silently dropped** by the A5
  `startOffset < latest` guard — pre-fix it would have overwritten. The cache then retains an entry whose
  startOffset is *above* the actual log end, and a later `truncationOffsetForEpochChange` can return a
  wrong (too-high) boundary. `truncateFromEnd` exists but is called nowhere in the tree
  (grep: only tests). Not reachable in the current suite (existing epoch-truncation tests use equal or
  higher offsets, so 1406/1406 still pass), hence *latent*; but it is a real coupling regression the A5
  change introduced in an existing caller. **Fix direction:** call
  `epochCache_.truncateFromEnd(truncateOffset)` inside `PartitionReplica::truncateLog` (and in the
  `becomeFollower` truncation branch) so cache and log truncate together — exactly the coupling A5's own
  doc-comment and test B24e anticipate. This is outside `libs/storage`; the storage primitive itself is
  correct.

### No new storage-lib findings

Fresh sweep of `LogSegment` (unchanged; `recover` already CRC-gated at `:138`), `OffsetIndex`,
`OffsetMap`, `MappedFile`, `PartitionApi`, `RetentionPolicy`, `ProducerStateManager` snapshot/recover
(64-bit sizing + `safeCount` clamp intact): no new material issue. The prior SILENT/CLEAN items A6–A9
are unchanged and remain non-data-loss quality notes.

### Coverage

32 new tests confirmed real and on-point (not tautological): B1 empty-segment recovery, B2 fresh-process
reopen LEO, B3 multi-record-batch compaction + B3-pid producer-identity round-trip, B4 mid-segment
corrupt-CRC stops walk, B5 post-rename/pre-dirfsync content survives, B6 tombstone exactly at retention
boundary retained, B7 key-only-in-active drops earlier copy, B8 >2GiB index guard, B9 torn tail good-len
bad-CRC dropped, B10 recover-twice idempotent, B11 tiny-maxBytes grow loop, B13 mid-batch truncate rounds
down, B14 at-segment-base truncate, B15 truncateTo(0)+append, B16 oversized batch > rollBytes, B18
consume at/past HWM, B19 index lookup below-base/at-last/above-last, B20 NDEBUG negative-seq OOS, B21
forged-count clamp, B22 window-wrap dup, B23 epoch-bump then pre-bump replay OOS, B24a–e epoch-cache
monotonicity + truncateFromEnd. Remaining Part-B gaps (from list of 24) still open, none blocking:
mostly the storage-side items are now covered; the untested residual is the **A10 replication coupling**
(a truncate-then-higher-epoch-with-regressed-offset failover test in `libs/replication`).

### Re-audit 2 conclusion
`libs/storage` in isolation: **CLEAN** — A1–A5 fixed correctly, no regression to normal compaction /
CRC walks / dedup, coverage adequate. One cross-cutting **latent CORRECTNESS** issue (**A10**) sits in
`libs/replication` where the A5 monotonic-`assign` change was not paired with `truncateFromEnd`; it is
out of the storage section's scope but was surfaced by the required "existing callers" check and should
be fixed before Phase-3 replication relies on the epoch cache.
