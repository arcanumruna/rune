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
  - `ctest -j8` (build-fast): **3954/3954 passed**, including the Debug latency gate.
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
