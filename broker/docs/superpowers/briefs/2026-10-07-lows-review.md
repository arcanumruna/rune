# Review: D19 / D20b / D21 lows (fix/d19-d21-lows @ 9a7088d)

Scope: cb483b9, 676df9d+e54b3ec, d366a57, 4f54161, a60d5e5, b5f590c, 5290ead, f4fa911, 0d8b339, b05b953.

**Verdict: APPROVE.** 0 critical, 0 major, 0 medium, 6 low.

Verification (build-fast, macOS, already up to date): `rune_hotpath_b_regression_tests --gtest_filter='HotPathBRegression.*'` 35/35 pass;
`rune_storage_tests --gtest_filter='SegmentFdScaleRegression.*:SegmentLifecycleRegression.*:LeaderEpochStateRegression.*'` 43/43 pass;
`scripts/check_no_atomics.py` OK (UNSCOPED 26). The Linux-only `st_blocks` sparse assertions were not run here.

## Per commit

| Commit | Correct | Notes |
|---|---|---|
| cb483b9 epoch-state route identity + dup fd | Yes | The route carries replica + registry incarnation. `sameReplica_` dereferences only live pointers: `gate->replica`, or `r.replica` after the `owned_` find short-circuits. Durability before acting holds because a stale completion is dropped and B leads only on its own completion. The fsync runs on an `F_DUPFD_CLOEXEC` dup. The dup is closed exactly once: at reap (both kinds), or immediately when submit back-pressure leaves it unsubmitted. The pwrite stays synchronous on `es_.fd` before the dup, and the replica destructor runs only after every worker has acked retirement, so closing `es_.fd` there cannot race an in-flight op. Both RED tests are meaningful: same Log with a restarted seq, and the same address via a kept block. See L1 and L2. |
| 676df9d + e54b3ec N6c | Yes | `reqTail - doneHead` bounds requests plus undrained completions (one completion per consumed request, both cursors owner-written, so the relaxed load is correct). The request ring stays bounded because `reqTail - reqHead <= reqTail - doneHead`. A held roll is released by the owner's normal drain. The test reproduces the strand (staging count == ready count) and the release. The allowlist update matches. |
| d366a57 L5 rebind borrowed fd | Yes | An installed map remaps only through the segment's own `fd_`. A retired map is unbound and its resize throws, which `ensureReadMap` catches as an empty view plus a lazy rebuild. The segment close path resets `readMap_` before closing `fd_`. The decoy-fd test is deterministic (the precondition asserts the fd number was reused). |
| 4f54161 N5a/N5b staging chain | Yes (cold) | Checks are now: a duplicate base, the second batch chaining to the first, `base >= prevEnd`, and its last offset `< nextBase`. Accepted files are inserted into `canonical`, so two staging files sharing a base cannot both be renamed. Quarantine only renames, never deletes. See L3. |
| a60d5e5 L2 corrupt state → refuse | Yes | `fenceUnknown_` puts -2 into every image (create, inline, worker), so the marker survives restart. On refusal the epoch becomes `fenceTarget` and is written before `fenceUnknown_` takes effect on disk. A crash in between keeps the -2 or the still-corrupt file, so it refuses again. If the state file could not be created (es_ disabled), the marker clears only in memory and the next restart still sees the corrupt file and refuses. This fails closed and cannot produce two leaders at one epoch. The 4-phase test covers corruption, a restart with no lead, refuse, and a durable bump. |
| b5f590c L1 pre-sized sparse | Yes | ftruncate to `2*stride`, then slot 0 only, then fsync, rename, and the existing directory fsync. Slot 1 reads as zeros, the same as the old zero-filled file. fdatasync covers hole-allocation metadata. See L4. |
| 5290ead L2 short-write resume | Yes | `finishShortWrite_` skips the bytes already written across the iovecs and retries EINTR. Zero progress returns ENOSPC. errno is read directly after the syscall. The fault seam is the same single predictable branch as before, and the `> 0` guards keep the -1 sentinel out of the RP paths. |
| f4fa911 L3 recover errors | Yes | It throws only before the valid prefix is known (fstat, pread) and marks the log sticky-failed in every case. An ftruncate failure leaves only a CRC-invalid torn tail behind a failed log, which restart recovery cuts again. No valid divergent batches can be resurrected, because truncateTo cuts before `recover()`. Test caveat: /dev/null has size 0, so only `validBytes=0` is covered. |
| 0d8b339 L4 drop flushBuffered | Yes | The function always returned 0 because writes are unbuffered pwrites. Removing it is a pure removal. |
| b05b953 sealed no async growth | Yes | `readView` sets `ownerRemap` whenever the read goes past the mapping, so a sealed segment is still served in the same call. A growth reply that arrives later for a once-active segment installs through `swapReadMap` (rebind), so it is safe. The test checks that the ring receives no post. |

No new data loss, two-leaders, fsync-ordering, double-close, fd reuse under in-flight io, or use-after-free issues. The only new worker-side syscalls are one `fcntl(F_DUPFD)` and one `close` per epoch-state write, both on the cold path (role or epoch changes). There are no new atomics, and N6c removes one acquire load.

## Lows

- **L1 (cb483b9)**: two cases leak the dup fd. (a) A route-insert failure leaves it open on purpose (documented as unreachable). (b) Worker teardown with an epoch-state fsync still in flight never closes it (shutdown only). Separately, rigs without a registry run at incarnation 0, so the same-address guard there relies on pointer + `owned_` only. Production always registers, so this does not affect production.
- **L2 (cb483b9 test)**: the test replaces global `operator new` / `operator delete` for the whole `rune_hotpath_b_regression_tests` binary. It works, but a comment at the definition should say the replacement is binary-wide.
- **L3 (4f54161)**: a single-batch staging file whose corrupt `baseOffset` lies above the chain (a gap) is still accepted. There is no second batch to cross-check, and `baseOffset` is not under the CRC. The window is narrowed, not closed. A possible check against the recovery point or the epoch cache goes to the backlog. Separately, `lastOffsetOf` reads whole neighbouring segments through ifstream with a vector allocated per batch. This is startup-only and only runs when staging files exist.
- **L4 (b5f590c)**: three effects of the sparse slot.
  - The worker's epoch-state pwrite and fdatasync can now hit block allocation, and ENOSPC on a full disk where the old pre-allocated overwrite could not. This fails closed (refuse or fail-stop) and is acknowledged in the Worker comment.
  - Existing 64-entry files are not migrated. They are re-created once when they outgrow, and the re-create can still land on a worker that one time.
  - `ES::read` now buffers about 104 KiB per partition at startup.
- **L5 (5290ead)**: `writeAll_` primes `errno = EINTR` as a sentinel meaning "nothing written yet". It works, but an explicit `first` sentinel would be clearer.
- **L6 (a60d5e5)**: a corrupt state file costs one extra controller bump by design: the first leadership is refused even at a far-higher epoch. That is the intended Kafka-like trade-off, noted here for operators.
