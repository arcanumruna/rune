# D20: segment open-file scale (2026-10-07)

Branch `fix/d20-segment-fd-scale`, based on main `375a539`.

## Problem
Every `LogSegment` kept a `std::fstream` open for its whole life, next to its raw `.log` fd. The fstream is a libc `FILE*` with its own fd. glibc keeps every `FILE` on one process-wide list, and `fclose` unlinks it with a linear walk (`_IO_un_link`). The list is newest-first, and retention drops the **oldest** segments, so each drop paid the worst case: a walk of the whole list. At about 40k segments the cleaner needed more than 1M owner passes to drain 256 drops, which stalled the TruncatePending follower in latency gate FollowerTruncation on tmpfs.

## Fix
- `LogSegment` no longer has a stream. `fd_` is now the only handle on the `.log` file:
  - The durable `append()` uses `pwrite` with a retry on EINTR and short writes, through the new `writeAll_`.
  - `appendNoSync` and `appendNoSyncRestamp` already used `pwrite` and `pwritev`.
  - `read()` uses `pread`, and the read mmap borrows `fd_`.
  - `recover()` reads with `fstat` and `pread`, then cuts the torn tail with `ftruncate(fd_)` instead of `resize_file(path)` followed by a stream reopen.
  - `sync()`, `syncForRecovery()` and `truncateInPlace()` lose only the stream flush. That flush is a no-op now because writes go straight to the page cache.
  - `flushBufferedToPageCache()` always returns 0.
- Durability is unchanged. `fsync` and `msync` run at the same points, recovery is the same, and fail-stop is the same: a write error goes to `failWrite_`, which records a sticky `failedErrno`.
- `.log` fds now open with `O_CLOEXEC`. `openPrebuilt` opens with `O_CREAT`, the same as the old stream open, which created the file.
- `FdBudget::kFdsPerSegment` drops from 4 to 2.

## fd audit, per open segment
| | before | after |
|---|---|---|
| `.log` fstream (`FILE*` + fd) | 1 + 1 | 0 |
| `.log` raw fd | 1 | 1 |
| `.index` writable mapping fd | 1 | 1 |
| `.log` read mapping | 0 (borrows `fd_`) | 0 |
| **total fds** | **3** | **2** |

The `.index` fd is kept on purpose, because grow-on-demand calls `ftruncate` on it. It cannot be released at seal time:
- `truncateTo` can turn a sealed segment back into the active one through `truncateInPlace`.
- Reopening the index by path is unsafe, because staging and retire renames change the name while the file is open.

Closing it is a plain `close(2)`, which is O(1). This is a possible follow-up (D20b) if fd limits become the binding constraint on the FREE tier.

Other code that uses `fstream` was checked:
- In `Log.cpp` and `LogCleaner.cpp`, `fstream` is only used in short-lived scopes on cold paths. Nothing stays open.
- `FileRecordingStore` holds an fstream for its lifetime, but no production code constructs it. It is only used by tests.

## Before and after cost

**Regression test** (`SegmentFdScaleRegression.*`, deterministic: it counts descriptors and does not time anything):

| test | before | after |
|---|---|---|
| live segments (durable append + no-sync append + sync + read) | 768 fds / 256 segments = 3 per segment (RED) | 512 = 2 per segment (GREEN) |
| recovered + staged segments | 1536 / 512 = 3 per segment (RED) | 1024 = 2 per segment (GREEN) |

Both tests also check that no descriptor leaks after destruction.

**Destruction cost on glibc 2.35** (Ubuntu 22.04, k3s-node-01, `/dev/shm`): standalone microbenchmark of the segment's open-file footprint. Open N segments, then drop the 256 oldest in retention order. Two runs:

| N open segments | before (fstream + fd) | after (fd only) |
|---|---|---|
| 1,000 | 12–17 µs / drop | 1.7–2.4 µs / drop |
| 10,000 | 495–581 µs / drop | 1.9–2.2 µs / drop |
| 40,000 | **4.6–5.4 ms / drop** | **1.6–1.8 µs / drop** |

Before the fix, the cost grows worse than linearly with N, because the walk over the list misses the cache. After the fix it is flat. At 40k segments, draining 256 drops goes from about 1.3 s of `fclose` walking to about 0.4 ms. macOS stdio does not walk a list in `fclose`, which is why the gate passed there and on ext4 and failed only on Linux tmpfs, where segment rolls are fast enough to build up 40k segments.

## Verification
- `rune_storage_tests`: 388/388 passed, including the 2 new tests.
- Full `ctest -j6` on macOS, with the machine at load ≈100–150 from three concurrent agents: 3960/3981 passed and 21 failed.
  - The 17 failures in `DutyPassLatencyGateRegression.*` are all scheduler stalls. The p50 values are normal (≈6 µs), and the worst passes show 20–120 ms of wall time inside intake or wait (`i/w/r`). Budgets were not loosened. Per the coordinator, the gates will be re-run on a quiet machine after merge.
  - The other 4 failures passed when re-run with `--rerun-failed -j1`: `GatewayReplicationWiring.ThreeNode_FollowersConverge_HWAdvances`, `GR4_Integration.ProduceViaGateway_FollowersConverge_HWAdvances`, `GR6_Integration.SubscriberBoundedByHW_FullyReplicated` and `CompactionRetentionSmoke.RestartResume_Checkpoint`.
  - `SteadyProduceSinglePartition` passes when run on its own (1.7 s).
- The tmpfs `FollowerTruncation` precondition (D20's symptom) can only be checked on Linux tmpfs. It is not checkable on this Mac.
- `scripts/check_no_atomics.py`: OK, with no category growing.
- No new allocations, I/O or virtual calls on the worker thread. The append paths do the same single `pwrite` / `pwritev` as before; the durable path swaps an fstream write for a `pwrite`.
