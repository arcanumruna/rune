# Hot-path B (read + role paths): report (2026-10-06)

- **Branch:** `perf/read-role-hotpath` (worktree `rune-broker-wt59`). Not pushed.
- **Base:** WIP `2b5524d`, merged with `integ/perf-defaults` `70217bd` (merge `9239f13`, no conflicts).
- **Commits:**
  - `be9f307` F7 role change
  - `f9ad01e` F7 truncation
  - `4c6fa49` F7 produce gate
  - `4c63945` gate scenario adaptation
  - `c2f8d9b` F5 read map
- **Checks:**
  - `ctest -j8` (build-fast): **3954/3954 passed** (round 0); after the review fixes **3966/3966 passed**, including the Debug latency gate.
  - `check_no_atomics`: OK. Allowlist categories are identical to `70217bd` (RING 66, CLEANER 63, CRASH-ORDER 1, CPU-HINT 3, SLICE-B 44, UNSCOPED 27).
  - `check_single_driver_pumper`: OK.
  - No new atomics or mutexes, and no virtual calls.

## What changed

### F5: zero-copy PushRead
The WIP part was finished and verified.

- `readPushChunk_` walks a `Log::readView` of the read mmap. It applies the bound, the marker strip and the read_committed clamp in a single pass, then gathers the kept runs straight into one claimed return-ring slot. No heap allocation and no intermediate vector.
- The gateway drains in view mode (`drainAllReturnsView`).
- The bytes a pass pushes are capped by a per-pass budget (`kPushPassBudgetBytes` = 128 KiB), shared with the in-worker egress. Reads over the budget are deferred to a fixed FIFO, served first on the next pass, with ownership re-validated.

**New in this round:** the active segment's read map is now sized for the whole segment when it is created (`LogSegment::readMapBytesFor`, built on the provisioner or at eager construction).
- Before, `readView` grew the map geometrically from 1 MiB: a munmap + mmap of up to 32 MiB on the worker.
- Probe: that remap cost 100-263 µs and was the dominant remaining read cost.
- It is address space only: pages are mapped on first touch, and `readView` never reads past the log size.

### F7: role changes no longer fsync on the worker
- Worker-driven replicas keep the leader-epoch fence and the epoch cache in a pre-allocated two-slot `leader-epoch-state` file. This is the same scheme as the recovery-point file: a CRC per slot, and the highest valid sequence wins.
- An update is one small page-cache `pwrite` of the alternate slot plus an io-backend `fdatasync`. The completion confirms it.
- **Ordering is unchanged and event-driven:** a controller-managed `becomeLeader` above the durable fence is deferred until the image carrying the fence and the new epoch's cache entry has completed. While deferred:
  - the replica keeps its role, applies nothing, fetches nothing and does not truncate;
  - a sitting leader that is being re-elected refuses produce (`acceptsProduce`, retriable NotLeader).
- A newer role supersedes a deferred leadership. A completion confirms a leadership only if it carried that leadership's generation.
- A failed write refuses the leadership and takes the log dir offline, as before.
- **Restore:** the state file is authoritative once it exists. If it exists but has no valid slot, the cache is rebuilt from the batch stamps and the stale legacy checkpoint is never used. The legacy files are read only for directories that never had a state file.
- Control-thread (internal) replicas keep the synchronous path.

### F7: truncation completion
- `Log::prepareTruncationTo` arms the cut. The recovery point is capped at the resulting log end, rounded down to a batch boundary, and the owner's io backend writes the lowered recovery point first. The truncation waits in TruncatePending, so E1d F6 still holds: the lower recovery point is durable before any file is touched.
- The rebuilt active-segment index is renamed without msync or a directory fsync. The recovery point stays gated on the batched async directory fsync (MEDIUM-2).
- Abandoning the truncation (a role change) releases the cap.

### F8
The TruncatePending worker part is now **16-48 µs** (op gate; budget 100 µs). The finding measured 76-162 µs. No extra change was needed beyond F7: the inline recovery-point write is gone from these passes.

## Regression tests (`HotPathBRegression.*`, all named `*Regression`)
Each test was shown RED with its fix disabled, then green.

| Test | Covers |
|---|---|
| `PushReadsBoundedPerPassAndFair` | F5 |
| `DeferredPushReadRevalidatesOwnership` | F5 |
| `ActiveSegmentReadsNeverRemapOnTheWorker` | F5 |
| `RoleChangeEpochDurabilityIsAsyncAndGatesLeadership` | F7 |
| `DeferredLeadershipSupersededByNewerRole` | F7 |
| `EpochStateRestoreAndCorruption` | F7 |
| `EpochBumpOfSittingLeaderRefusesProduceUntilDurable` | F7 |
| `FollowerTruncationNeverSyncsOnTheWorker` | F7 |
| `UnsyncedTruncationRebuildRecoversAfterCrash` | guard |

Test contracts updated:
- `ShardNonBlockingRegression.TruncateClampsSyncedLeo` now asserts that a worker-driven truncation does **no** role-change syncs.
- `EpochRestartRegression` reads through `readDurableEpochCache`.
- The latency gate's LeadershipChange scenario drives bounded passes until the async leadership has completed, and produces only to partitions that accept produce.

Gate budgets and `kKnownFailures` are unchanged.

## Release latency gate before / after (p99.9 / max µs, best attempt; COLD = worst allowed-cold pass)

- **Setup:** Linux arm64 Release, gcc-13, `rune-latgate-builder:a940b8b`, Docker Desktop VM (kernel 7.0.12-linuxkit). `docker ps` = 0 other containers and no ctest during every run (sampled every 5 s).
- **Before** = `70217bd` (merged main). **After** = `c2f8d9b`.

| Scenario | Before ext4 | After ext4 | Before tmpfs | After tmpfs |
|---|---|---|---|---|
| Reads+SegmentRoll/1p | 767 / 958 | 748 / 1,597 | 17 / 266 | 17 / 264 |
| Reads+Retention/2p | 1,783 / 4,328 | 977 / 2,267 | 19 / 187 | 20 / 190 |
| Reads+Compaction 16 MiB/1p | 62 / 125 | 22 / 183 | 63 / 91 | **17 / 34** ✅ |
| LeadershipChange/1p | 18 / 62, COLD 3,144 | **1 / 13, COLD 4** ✅ | 12 / 31, COLD 37 | 6 / 28, COLD 3 ✅ |
| LeadershipChange/8p | 67 / 199, COLD 7,030 | 13 / 269, COLD 50 | 35 / 88, COLD 114 | **32 / 64, COLD 16** ✅ |
| FollowerTruncation/2p | 102 / 865, COLD 6,412 | 486 / 2,086, COLD 830 | 13 / 269, COLD 107 | 16 / 252, COLD 92 |

**Solo read passes** (worker part of passes whose only background op is a read):
- ext4: before 125-2,771 µs; after 46-183 µs (one 2,206 µs pass whose worker part was 56 µs, i.e. intake).
- Compaction-read probe: `readView` max fell from 263 µs (remap) to under 30 µs; the copy into the ring is ≤ 21 µs.

**COLD role-op / truncate passes** went from 3-7 ms to 4-830 µs. The 830 µs case is the truncating pass's own active-segment rebuild (a file read and ftruncate), not an fsync.

**Other scenarios** (control): unchanged within run-to-run noise on tmpfs, for example SegmentRoll/1p 18/269 → 20/269 and TxnMarkers/1p 7/18 → 7/16.

## Residuals (said plainly)

### F6 dominates every remaining red maximum
Worst-pass splits are intake `pwritev` (worker part ≤ 7 µs):
- FollowerTruncation ext4 2,086 µs, `i/w/r=2620/3/0`
- Retention ext4 4,039 µs
- TxnMarkers/4p ext4 2,668 µs
- SteadyProduce/1p ext4 243 µs: fails before and after

**Regressions on ext4.** The ext4 p99.9 and max for FollowerTruncation went up (102 → 486, 865 → 2,086), and ext4 maxima for Retention and TxnMarkers/4p also went up. The cause is attributed to F6, not the worker:
- Their worker parts are 1-7 µs.
- tmpfs is flat.
- These scenarios now run about 40% more passes, because truncations no longer stall the loop for milliseconds. That gives more exposure to the VM's ext4 write stalls.
- This must be confirmed on bare metal.

### Two reads of about 100 µs per 20k on tmpfs
Two `readView` calls of about 100 µs per 20k reads remain in Reads+SegmentRoll on tmpfs:
- probed: no remap, no syscall, a 2 KiB view;
- this is page-fault latency in the VM;
- a standalone probe in the same container shows mmap stalls of up to 259 µs when a concurrent thread mmaps and munmaps.

### FollowerTruncation tmpfs precondition failure
FollowerTruncation on tmpfs fails its truncations ≥ 3 precondition **both before and after**. This is backlog D20 (retention throughput), not this change.

### Bare-metal confirmation
None of the F6 / VM residuals can be fixed by the worker. They need a bench-box run.

---

# Review fixes (review 2026-10-06 `2026-10-06-read-role-hotpath-review.md`: REJECT, 1 critical / 2 major / 6 medium / 4 low)

Commits on top of `2ab1bc2`:

| Commit | What |
|---|---|
| `80b61f1` | C1, M1, Md1, Md2, Md3, Md4, Md6 |
| `fb4c46b` | M2 (in-place truncation + fsync attribution in the gate), Md5 |
| `68cefb6` | Lows → backlog **D21** |
| `3e03c0b` … `8910ef8` | Gate scenarios made event-driven for async promotions / C1 re-fetch. Budgets and `kKnownFailures` untouched. |

Every behavioural fix has a `HotPathBRegression.*Regression` test. Each was shown RED with the fix disabled, then green. The suite is now 21 tests.

## Fixes

### C1 (Kafka model)
**(a) Ordering.** The epoch history now becomes durable before the recovery point can rely on it.
- The log's recovery point never passes epoch history that is not durable (`Log::setEpochHistoryCap`). The cap comes from the replica's pending and in-flight change offsets: the start of an assign, the truncation point, a deferral's LEO, or a cancelled deferral's LEO.
- After a divergence truncation, a follower neither re-fetches nor applies until the write carrying the truncated history completes. The re-fetch is sent from `epochStateWriteCompleted`, using production `epochStateDurable()`, so it is event-driven.

**(b) Restore.** On restart, the epoch cache is rebuilt from the batch stamps:
- Entries at or above the **startup** recovery point (`Log::startupRecoveryPoint`) are re-derived from the stamps on the surviving batches. Stale entries are dropped and missing ones assigned.
- History below that point is spot-checked against the stamps with O(entries) point reads. A mismatch triggers a full rebuild.
- In a directory that holds epoch history, `Log::recover` keeps the persisted recovery point at the startup value until the replica has made the reconciled history durable and lifts the guard.

**Tests:**
- `EpochHistoryNeverLagsRecoveryPointAcrossPowerLossRegression` (power-loss seam: the file is reverted to its last confirmed image).
- `DivergenceTruncationRefetchWaitsForDurableHistoryRegression`.
- `RefetchResumesOnDurableHistoryRegression`.
- RED before the fix: offset 20 is stamped 6 while the restored history says 3.

### M1 (ruling: no downgrade support)
- After the first durable write of `leader-epoch-state`, the legacy `leader-epoch-checkpoint` and `leader-epoch-fence` files (and their `.tmp`) are unlinked, followed by one full directory fsync. This is cold and never runs on a worker.
- If both formats are present, the state file wins. A legacy-only directory is migrated, validated against the stamps. A corrupt state file is rebuilt from the stamps, never from legacy files.
- Test: `LegacyEpochFilesMigrateAndAreRemovedRegression` covers three cases: a stale legacy-only directory, both formats present, and a corrupt state file with legacy files present.

### M2 (truncation and fsync attribution)
**In-place truncation.** A worker-driven truncation inside the active segment is now done **in place**:
- one `ftruncate` of the open fd;
- index entries past the cut are zeroed in its mapping;
- a header walk over the read map, with no file read, no new file and no index rename.

A directory fsync is therefore owed only when whole segments were unlinked.
- Tests: `InSegmentTruncationNeedsNoDirectoryFsyncRegression` (RED: a directory fsync per truncation) and `InPlaceTruncationRecoversAfterCrashRegression`.

**Fsync attribution.** The gate now prints per-scenario counts:
- io-backend seal, background, recovery-point, directory and epoch-state fdatasyncs;
- inline syncs on the worker;
- cleaner fsyncs and directory fsyncs;
- provisioned segments, directory mutations and in-place truncations.

### Mediums
- **Md1:** a cancelled deferral caps the recovery point at its entry, so a restart re-derives it. Test: `CancelledLeadershipLeavesNoPhantomEpochAfterCrashRegression`.
- **Md2:** a failed epoch-state write is re-armed, with exponential backoff measured in ns. Three failures in a row mark the log storage-failed and take its directory offline (fail-stop). Test: `FailedEpochStateWriteRetriesThenFailsStopRegression`.
- **Md3:** an unroutable completion re-queues the write (`epochStateWriteNotRouted`). Test: `UnroutableEpochStateCompletionNeverWedgesPromotionRegression`.
- **Md4:** the per-replica image is gone (one scratch buffer per worker; the bytes are copied by `pwrite`). The fd is open only while a write is due or in flight. The file is 8 KiB. Test: `IdlePartitionHoldsNoEpochStateFdRegression`.
- **Md5:** read maps start at 1 MiB. A provisioned log grows each segment's map to page-round(rollBytes + 1 MiB), never a power of two. The growth runs on the `SegmentProvisionerAgent`: a dup'd fd goes out, the mapping comes back over a per-log ControlRing, and the old mapping is munmapped there. `readView` never remaps on the owner. Failures are counted (Aeron counter `read-map-failures`) and fall back to owner growth.
  - FREE-tier sizing: 10k partitions with about 1% filling is roughly 9.9k × 1 MiB + 100 × 65 MiB ≈ 16 GiB of address space. It was 10k × 128 MiB ≈ 1.25 TiB.
  - The RC scan (`segmentBytesAt`) may still map on the owner. It is cold and counted.
  - Tests: `ReadMapAddressSpaceIsBoundedAndGrownOffOwnerRegression` and `ActiveSegmentReadsNeverRemapOnTheWorkerRegression` (now provisioned).
- **Md6:** the writer cannot be switched while a write is in flight (assert, refused in Release), and the inline path asserts Idle. Test: `EpochStateWriterToggleWhileInFlightRefusedRegression`.

### Lows
L1-L4 are in `docs/superpowers/backlog-2026-10-03.md` as **D21**.

## Release latency gate: before (`70217bd` + test-only counter backport) vs after (`8910ef8`)

**Setup:**
- Linux arm64 Release, `rune-latgate-builder:a940b8b`, Docker Desktop VM (kernel 7.0.12-linuxkit).
- 0 other containers and no ctest during every run, sampled every 5 s.
- The before tree carries a test-only backport (fsync counters plus the same event-driven LeadershipChange cycle). Its production code is untouched.

Values are p99.9 / max in µs, best attempt. COLD = the worst allowed-cold pass.

| Scenario | before ext4 | after ext4 | before tmpfs | after tmpfs |
|---|---|---|---|---|
| Reads+SegmentRoll/1p | 632 / 2,184 | 719 / 2,751 | 18 / 249 | 18 / 263 |
| Reads+Retention/2p | 799 / 1,266 | 1,622 / 2,787 | 20 / 194 | 21 / 168 |
| Reads+Compaction 16 MiB/1p | 62 / 138 | **22 / 95** | 62 / 97 | **17 / 29** ✅ |
| LeadershipChange/1p | 22 / 142, COLD 10,609 | **6 / 90, COLD 7** ✅ | 12 / 53, COLD 47 | 12 / 63, COLD 4 ✅ |
| LeadershipChange/8p | 67 / 222, COLD 15,690 | 47 / 248, COLD 58 | 34 / 112, COLD 105 | 38 / 67, COLD 28 ✅ |
| FollowerTruncation/2p | 153 / 1,706, COLD 6,095 | 198 / 1,723, COLD 274 | 14 / 224, COLD 110 | 16 / 264, COLD 85 |
| SegmentRoll/8p | 1,177 / 2,148 | 1,576 / 6,738 | 132 / 232 | 94 / 242 |
| Retention time+size/2p | 795 / 1,098 | 2,074 / 5,575 | 26 / 294 | 28 / 250 |
| TxnMarkers/4p | 42 / 260 | 42 / 403 | 15 / 28 | 16 / 28 |
| SteadyProduce/1p (control) | 8 / 35 | 8 / 117 | 4 / 35 | 5 / 22 |

**Fsync counts, ext4, same runs** (b → a). io = io-backend submissions from the worker; cleaner = cleaner-thread fsyncs / directory fsyncs.

| Scenario | seal | bg | rp | dir | epoch | worker-inline role syncs | cleaner fsync / dir |
|---|---|---|---|---|---|---|---|
| Retention/2p | 4651 → 4604 | 10 → 12 | 6 → 6 | 6 → 6 | 0 → 0 | 10 → 10 | 815/1630 → 816/1632 |
| Reads+Retention/2p | 4679 → 4607 | 12 → 12 | 8 → 6 | 8 → 6 | 0 → 0 | 10 → 10 | 789/1578 → 796/1592 |
| TxnMarkers/4p | 48 → 48 | 527 → 556 | 48 → 48 | 48 → 48 | 0 → 0 | 20 → 20 | 0 → 0 |
| SegmentRoll/8p | 8174 → 8883 | 65 → 75 | 27 → 24 | 26 → 23 | 0 → 0 | 40 → 40 | 0 → 0 |
| FollowerTruncation/2p | 3446 → 3947 | 10 → 54 | 9 → 45 | 8 → 34 | 0 → 24 | 34 → 10 | 12/28 → 15/36 |
| LeadershipChange/8p | 226 → 209 | 384 → 160 | 226 → 164 | 222 → 91 | 0 → 32 | 40 → 40 | 0 → 0 |

The legacy inline checkpoint syncs (fsync + rename + directory fsync) in the before tree are counted in its COLD role-op passes (7-36 ms). They do not appear in these io counts.

## Explaining each ext4 delta

### Retention, Reads+Retention, TxnMarkers/4p, SegmentRoll/8p, SteadyProduce/1p
These are best-of-run single numbers. Their fsync counts are identical within ±2% (the same seal, cleaner, retention and dir work).

To check them I ran an **interleaved ABAB, n = 15 per side**: 70217bd and HEAD alternating, the same ext4 volume recreated each run, with no load events logged. Medians (p25-p75) for before vs after:

| Scenario | Metric | Before | After |
|---|---|---|---|
| Retention/2p | p99.9 | 1182 (1044-1639) | 1456 (1166-1570) |
| Retention/2p | max | 37.3 ms (33-44) | 35.2 ms (31-45) |
| Reads+Retention/2p | p99.9 | 1483 (1214-1717) | 1207 (1106-1426) |
| Reads+Retention/2p | max | 13.8 ms (12-19) | 14.0 ms (12.6-22) |
| TxnMarkers/4p | p99.9 | 44 (42-45) | 42 (42-44) |
| TxnMarkers/4p | max | 243 (213-265) | 257 (218-272) |
| SegmentRoll/8p | p99.9 | 1872 (1585-1920) | 1788 (1573-1951) |
| SegmentRoll/8p | max | 7.2 ms (5.0-10.8) | 6.3 ms (4.4-18.7) |
| SteadyProduce/1p | max | 294 (256-349) | 364 (254-716) |
| SteadyProduce/8p | max | 354 (321-1626) | 325 (314-396) |
| Reads+SegmentRoll/1p | p99.9 | 739 (542-772) | 716 (501-769) |
| Reads+SegmentRoll/1p | max | 2.8 ms (2.0-4.9) | 3.5 ms (3.3-5.1) |
| FollowerTruncation/2p | p99.9 | 177 (160-189) | 189 (179-219) |
| FollowerTruncation/2p | max | 1.95 ms (1.6-2.2) | 1.71 ms (1.7-2.0) |

The IQRs overlap for every row. **These single-run deltas are not regressions.** The earlier report's attribution of them was unproven; this replaces it.

### LeadershipChange/8p: one real ext4 regression
The ABAB max is **864 µs (825-884) before vs 1,652 µs (1,634-1,706) after**, with no IQR overlap. p99.9 is 73 vs 78.

**Attribution** (per-frame intake probe, removed afterwards):
- The worst passes are single produce frames whose `pwrite` is the **first write into a freshly adopted, fallocated segment**: LEO = active base + 32. This is the F6 ext4 first-write class from the hot-path A report (0.5-3.7 ms).
- The worker part of those passes is ≤ 31 µs, and tmpfs shows no regression (67 vs 112).
- In HEAD, all 8 partitions hit it at their 58th segment, at the same segment index in every run. In the before tree, single partitions hit it at segments 96, 107 and 115 at about 780 µs.

**A/B experiments on the HEAD binary** (env switches, not committed) to isolate the cause:

| Variant | max |
|---|---|
| Epoch-state io kept but with **no fdatasync** | 1.5 ms spikes remain |
| **No slot pwrite** and no fdatasync | 1.5 ms spikes remain |
| **No recovery-point cap** (C1) | 1.7 ms remains |
| **Async epoch state off** (the legacy synchronous checkpoint on the worker) | **873-914 µs, identical to before** |
| Read-map growth off | 1.76-3.3 ms (unchanged) |

Neither the epoch-state io, the C1 cap nor the Md5 growth causes it. It appears exactly when the synchronous legacy checkpoint (fsync + rename + directory fsync on the worker at every role change, 7-36 ms COLD passes) is removed. The journal commits those COLD syncs used to force now land elsewhere, and one of them coincides with the 8 partitions' first writes into segment 58.

This is the reviewer's "cost moved from COLD into a HOT first write" mechanism, but through the extent conversion of the first write, not through the new fsyncs. The fsync counts confirm it: the after tree issues fewer io fsyncs in this scenario (656 vs 1058).

**Fix candidate** (F6 follow-up, not in this branch): have the provisioner pre-write or zero the first page(s) of each ready segment, so that the owner's first write is never an unwritten-extent conversion. It must be confirmed on bare metal. The gate budgets are untouched, and LeadershipChange/8p stays within its listed `kKnownFailures` ceiling.

---

# Re-review fixes (rereview of `bfc92af`, `2026-10-06-read-role-hotpath-rereview.md`)

Commit `ef1782d`. Each fix has a `*Regression` test, and all six were shown RED with their fix disabled.

| ID | Fix | Test |
|---|---|---|
| N1 (critical) | The in-place truncation takes its cut from the whole segment: it maps the whole file on the owner, on this cold path. It fails the log loudly unless the walk stopped at the batch holding the offset, or at the segment end. | `InPlaceTruncationOverPartialReadMapCutsAtTheOffsetRegression` |
| N2 | Each growth request carries a seq, packed with the dup'd fd and the size. A late answer to a superseded request is discarded (munmapped off the owner). | `LateReadMapForSupersededRequestIsDiscardedRegression` |
| N3 | Data already past the mapping is served by a counted owner remap. Readers never stall on the provisioner. | `ReadsPastTheMappingNeverStallOnTheProvisionerRegression` |
| N4 | One `onHistoryDurable_` hook releases the re-fetch wait. It is called from the async completion, the inline write, the pending-leadership clear, `becomeFollower` and the follower duty pass. | `RefetchWaitReleasedByInlineDurabilityRegression` |
| N5 | A missing recovery-point file is created at the epoch guard's held value. | `MissingRecoveryPointFileHonoursTheEpochGuardRegression` |
| N6 | A failed update of a valid `leader-epoch-state` fails the log. It never falls back to the legacy files. | `FailedEpochStateUpdateNeverFallsBackToLegacyRegression` |

The lows are appended to backlog D21.

## Tests
`ctest -j6` ran with the machine loaded by another agent's stress loop (load average 21-30).
- **3966/3972 passed.**
- Non-gate failures, both green in isolation:
  - `ConsensusCandidacyElectionRegression.StaleKnowledgeLearnedThroughExchange_SurvivorsElect`
  - `PerWorkerReplication.ExactEffectiveMaxBatchReplicatesPerWorkerRegression`
- **Debug latency-gate misses under this load** (re-run in isolation, still loaded; budgets not loosened):

| Scenario | Measured | Budget |
|---|---|---|
| Compaction/64MiB/1p | max 8.8 ms | 6 ms |
| FollowerTruncation/2p | p99.9 1.25 ms | 1 ms |
| Reads+Retention/2p | p99.9 2.1 ms | 1 ms |
| TxnMarkers/4p | max 6.5 ms | 6 ms |

The worst passes are produce-intake passes. The local latency re-run was skipped on instruction. The Reads* and SegmentRoll check moves to the bare-metal box run after merge.
