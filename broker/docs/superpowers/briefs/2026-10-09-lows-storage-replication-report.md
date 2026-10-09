# Lows batch B: storage, replication and transport — report

Branch `fix/lows-storage-replication` (from integ/perf-defaults 4b0b6a6), worktree rune-broker-wt71.
Build: `build-fast` (Ninja + ccache, Debug, macOS x86_64), cache settings copied from wt26.
12 commits, 1d44529..ad5b3de. Nothing merged or pushed.

RED evidence: each behaviour change was run against a build without the fix (the fix
reverted in place, or the test written before it) and failed as quoted. Afterwards the fix
was restored and the test passed.

## 1. D23 — lows from 2026-10-07-lows-review.md

| Item | Status | Commit | Test / RED evidence |
|---|---|---|---|
| L4: a sparse epoch-state file can fail the worker's epoch write with ENOSPC | **done** | 1d44529 | `LeaderEpochStateRegression.CreateAllocatesTheHeadOfBothSlotsRegression` RED: "slot 1 block at +0 is a hole". `LeaderEpochFenceRegression.SparseEpochStateFileIsReservedOnEnableRegression` RED: "slot 1's head is still a hole after enable". Both use a 64 MiB file, which is sparse on APFS as well as ext4/xfs. |
| L1: a dup fd is left open in two cases | **done** | 1e1d3e9 | `UnroutableEpochStateCompletionNeverWedgesPromotionRegression` (extended) RED: "an epoch-state fsync dup leaked" (1 open). `WorkerTeardownClosesAnInFlightEpochStateFsyncFdRegression` RED: the fd was still open after teardown. |
| L2: a test replaces global new/delete for its whole binary | **done** | bd5aa3c | The two tests that need the replacement moved to the new binary `rune_hotpath_b_alloc_regression_tests`. The shared rig moved to `HotPathBRig.h`. No behaviour change. |
| L5: errno is primed as a sentinel in writeAll_ | **done** | d2c0a4c | Refactor: `kNotWrittenYet` replaces the primed errno, and callers now pass in the errno of their first write. No behaviour change. The existing short-write / EINTR resume tests pass. |
| L3: a single-batch staging file with a corrupt base above the chain is still accepted | **ruled** | — | See the ruling below. |
| L6: one extra leadership round after a corrupt state file | **ruled** | — | Working as designed. See the ruling below. |

**L4 fix.**
- Each slot now has an allocated head. `LeaderEpochState::reserveFor` sizes it to twice the history, at least one block and at most the stride.
- `create()` writes both heads: slot 0 gets the image plus zeros, slot 1 gets zeros.
- Enabling the replica allocates the heads of a file that an earlier build left sparse. This runs once, on the construction thread. A head that is already allocated is detected with `lseek(SEEK_HOLE)`, so a restart costs no write and no sync.
- An in-place image must fit inside the reserved head. If it does not, the existing cold, counted re-create runs.
- The Linux test that caps st_blocks at 16 KiB still holds: the heads take 8 KiB.

**L1 fix.**
- (a) The free route slot is now checked before the slot write and the fsync. A full route table refuses the write: Prepared goes back to Idle, the write is still due, and the call returns 0 as back-pressure. Every submitted fsync is therefore routed and its dup is closed.
- (b) `~Worker` hands the dups of fsyncs still in flight to a member declared before `io_`. They are closed only after the io backend has been destroyed: the portable flusher is joined, or the uring ring is closed.
- Cost: cold path only (role and epoch changes).
- Separate note: rigs without a registry run at incarnation 0. The reviewer already noted this, and production always registers, so nothing changed.

**L3 ruling.**
- `baseOffset` is outside the batch CRC. That is true of every Kafka-format segment, not only staging files.
- A file with a single batch has no second source for its base. Each candidate check is unsound:
  - **Require `base == previous segment's end`.** The cleaner compacts the segment just below the active one (`produceCleanedFile` only requires `segBase < activeBase`) and can drop its tail. A legitimate active segment then starts above the previous segment's end, and this check would quarantine the active segment's data.
  - **Recovery point.** When the recovery point lies in the previous segment's range, it is at most the true base, so it cannot bound the base from above.
  - **Epoch cache.** `partitionLeaderEpoch` is also outside the CRC, and the latest epoch has no upper bound.
- No sound check exists without a CRC-covered base, which would be a record-format change.
- The remaining window is narrow: a corrupt base in the only batch of a staging file, at a crash, before the deferred rename.
- The ifstream reads and per-batch vectors in `lastOffsetOf` run only at startup and only when staging files exist, so they are left as they are.

**L6 ruling.**
- With a corrupt state file, the replica cannot know the highest epoch it led at.
- Refusing the first leadership and asking the controller for a new epoch is the only way to guarantee there are never two leaders at one epoch.
- The cost is one controller bump (one AlterIsr round trip) per corrupt-file incident.
- The error log names the file.
- This is the Kafka-like trade-off the review already accepted. No change.

## 2. D26 — follow-ups from 2026-10-08-fix65-review.md

| Item | Status | Commit | Test / RED evidence |
|---|---|---|---|
| LOW-1: provisioner read-map work is dead for push reads | **done** | d8aee26 | `HotPathBRegression.PushOnlyLogNeverProvisionsReadMapsRegression` RED: `readMapGrowRequests` was 1 instead of 0, and "segment 192 was provisioned with a read map nobody reads". |
| LOW-2: a pread EIO falls back to the mmap (SIGBUS) | **done** | d2ecf6e, af0d5d2 | `LogSegmentRegression.ReadIntoIoErrorIsNeverTooLargeRegression` RED: "an I/O error was reported as too large". The scenario is a file truncated behind the log's back under a batch larger than the buffer. |
| LOW-3: stale F5 comments in Worker.h / LogSegment.h | **done** | 043e48e (LogSegment.h also in d2ecf6e / d8aee26) | Comments only. |
| LOW-4: a pread copies about maxBytes when the HW is just past the cursor | **done** | d2ecf6e, af0d5d2 | `LogSegmentRegression.ReadIntoStopsNearTheBoundRegression` RED: "read far past the bound" at all 3 start offsets. |
| Observation: stuck cursor in a compacted tail gap | **real bug, fixed** | d2ecf6e (+ worker test in af0d5d2) | `LogSegmentRegression.ReadIntoInACompactedTailGapContinuesAtTheNextSegmentRegression` RED: "start 5 in the tail gap: empty (cursor stuck)". `HotPathBRegression.PushReadCursorInACompactedTailGapAdvancesRegression` RED: the push cursor was answered at 6 and never reached 12. |

**readInto status (d2ecf6e).**
- `LogSegment::readInto` and `Log::readInto` now report a `ReadIntoStatus`: Ok, Absent, FirstTooLarge, Unreadable or IoError. The `bool firstTooLarge` overloads remain.
- **Tail gap.** An Absent answer continues at the next segment's base, below the bound. It never continues past torn or unreadable bytes or an I/O error.
- **I/O errors.** Any pread failure, including a file shorter than its written size, is IoError. It is never reported as FirstTooLarge. ("Sticky" here meant within one `readInto` call only; nothing persisted across calls. Since the lows A/B/C mediums round (review M1), a read I/O error fails the log, sticky, like a write failure.)
- **Too-large batches.** A first batch that is genuinely too large is read through once (cold) before FirstTooLarge is reported, so a bad page there also answers IoError instead of SIGBUS in the mapped fallback.
- **Bound.** The read stops one index interval past the index entry for the bound. The batch holding the bound starts less than one interval after that entry, and the first batch is still always read whole.
- Counters added: `readIntoErrors`, `readIntoBytes`.

**Callers (af0d5d2).**
- Worker push reads (PushRead and owned subscriptions) pass their HW / LSO bound.
- The worker and the replica's fetch / owed / push reads fall back to the mapped view only on FirstTooLarge.
- An IoError is answered empty (retried next cycle), counted (`pushReadErrors`, replica `readErrors_`) and logged, rate-limited to powers of two.
- Per-read cost: one index lookup when the bound lies inside the segment, saving up to about 128 KiB of pread copy.

**Provisioner read maps (d8aee26).**
- `prepareStaging` no longer maps by default. Before, every staged segment got a 1 MiB mmap on the provisioner and an munmap at seal.
- The half-full growth request in `maybeRoll` is now only posted for a log that has a mapped reader. A log has one once `readView` has been called on it; the flag was sticky (since review M3 it decays at a roll with no `readView` since the previous one).
- The proof is a counter: `readMapGrowRequestsForTest`, together with `hasReadMapForTest` on every staged segment, in the test above.
- Owner cost: one bool test per append.
- Cold mapped readers (RC / PSM rebuild, truncation prep, the rare too-large fallback) now map a new active segment lazily on their first `readView`: an owner mmap sized to cover the active segment (1 MiB doubled until it covers the written bytes), not "one 1 MiB mmap". (Corrected by review L9 / M3; since M3 the provisioner maps it off the owner again, requested at the first append after the roll.)
- `LateReadMapForSupersededRequestIsDiscardedRegression` now issues a mapped read first, because growth is only provisioned for mapped readers.

## 3. A7-L — transport lows from 2026-10-04-a7-review.md

| Item | Status | Commit | Notes |
|---|---|---|---|
| LOW-1: the C1 tests do not assert that an ICMP error was pending | **done (broker)** | 5728bf4 | `PeerDownNeverLeavesWakeFdsReadyAfterAnIdlePassRegression` now asserts that a wake fd became readable between passes at least once. The test is Linux-only and was compiled but skipped on macOS. The client copy (`InvokerTransportTest.cpp:639`) is in rune-client-cpp, outside this worktree. |
| LOW-2: 3 coverage tests carry the Regression label | **done (broker 2 of 3)** | 5728bf4 | Renamed `EmbeddedDriverWake.ParkedRunnerProcessesDatagramPromptly` and `...ParkedRunnerTakesFdLessWorkWithinTheParkCap`. The third, `InvokerTransportEpollPollRegression.MultiTransport...`, is in rune-client-cpp. |
| L1: a non-ECONNREFUSED error ends the poll pass early | **ruled: outside this worktree** | — | The code is `rune-protocol/src/net/rune_driver_wake.c:95-116`, which the broker compiles from `RUNE_PROTOCOL_INCLUDE/../src` and this brief does not allow touching. The fix is to `continue` past a per-transport SO_ERROR / recvmmsg error instead of returning -1. Per the re-review this is bounded, not a spin: each pass consumes the failing socket and the ready list rotates. |
| L2: Aeron-internals self-check and pin | **done** | 3763230 (test in 5728bf4) | `EmbeddedDriver` installs epoll-first polling only when the runtime Aeron is the verified 1.41.4. Any other release gets stock polling, no wake fds and a logged reason. `EmbeddedDriverEpollPoll.AeronIsTheReleaseEpollFirstWasVerifiedAgainst` fails on an upgrade. The existing check that every handed-out fd is an epoll instance (`rune_driver_wake_fds`) stays. |
| L3: virtual idle() / doWork() in the runner | **done** | d8370fb | The runner is now `BasicAgentRunner<AgentT, IdleT, ClockT>`. Shard workers run `<ShardAgent, ShardIdle, MonotonicNanoClock>`: all final, with a static_assert. `ShardIdle` dispatches the strategy `makeShardIdle` picked by a branch on its kind. `AgentRunner` stays the virtual flavour for control-plane / cold threads. Microbench (x86_64, -O2, 20M empty passes × 5): virtual 24.5–29.1 ns/pass, direct 23.2–27.1 ns/pass, about 1.5–2 ns saved per pass. Test: `ShardIdle.EveryShardStrategyIsDispatchedWithoutAVirtualCall`. |
| L5: `Client::wakeFds` reaches the transport via `testTransport()` | **ruled: outside this worktree** | — | The code is in rune-client-cpp. |
| L6: driver error-log noise while a peer is down | **resolved by the C1 fix** | — | `rune_driver_wake.c` consumes SO_ERROR in the same pass, as the re-review confirmed, so the next send no longer fails with ECONNREFUSED. No broker change. |
| L7: `idleMaxParkUs` is unbounded | **done** | ad5b3de | `NodeConfigRegression.IdleMaxParkUsIsBoundedAndStrictRegression` RED: `0`, `-5`, `fast`, `10ms`, `1000001` and `5000000000` were all accepted. Now the value must be an integer in [1, 1000000] µs or startup refuses. A cap above 10 ms is named in the startup warnings (`HostDefaults.LongIdleMaxParkWarnsAtStartup`). No repo script uses another spelling. |

## Hot-path budget statement

- **Per-pass changes:**
  - L3 removes three indirect calls per runner pass (measured above).
  - The push / fetch read adds one status store and, when the HW lies inside the segment, one index lookup. It saves pread copy past the bound.
  - `maybeRoll` adds one bool test per append.
- **Everything else is cold:** epoch-state writes (role and epoch changes), teardown, startup and config.
- No new allocation on the pass, and no new atomics or mutexes: `scripts/check_no_atomics.py` reports OK.

## Tests run (affected targets only, macOS, -j4, latency gates skipped)

All green:

| Target | Tests |
|---|---|
| rune_storage_tests | 418 (2 Linux-only skipped) |
| rune_storage_integration_tests | 41 |
| rune_replication_tests | 428 |
| rune_replication_zero_alloc_tests | 3 |
| rune_hotpath_b_regression_tests | 41 |
| rune_hotpath_b_alloc_regression_tests | 2 |
| rune_node_tests | 343 |
| rune_embedded_driver_tests | 21 |
| rune_shard_agent_tests | 13 |
| rune_shard_mode_tests | 51 |
| rune_shard_nonblocking_tests | 117 |
| rune_worker_pool_tests | 16 |
| rune_worker_doorbell_tests | 5 |
| rune_worker_push_tests | 14 |
| rune_worker_tests | 9 |
| rune_worker_produce_path_tests | 10 |
| rune_f15_eos_regression_tests | 178 |
| rune_epoch_restart_tests | 4 |
| rune_consensus_tests | 446 |

Final sweep, all green:

| Target | Tests |
|---|---|
| rune_worker_egress_tests | 6 |
| rune_worker_subscribe_tests | 5 |
| rune_gateway_subscribe_hw_bound_tests | 6 |
| rune_psm_hw_bound_tests | 1 |
| rune_compaction_correctness_tests | 4 |
| rune_compaction_retention_smoke_tests | 12 |
| rune_per_worker_replication_integration_tests | 10 |
| rune_dr_integration_tests | 62 |
| rune_replication_integration_tests | 38 |
| rune_group_push_delivery_tests | 11 |
| rune_gateway_subscribe_tests | 20 |
| rune_async_durability_tests | 8 |

rune_uring_durability_tests runs 0 tests on macOS (Linux only).

The Linux-only assertions were compiled but not run here: the C1 positive control, the st_blocks / SEEK_DATA parts on ext4, and the io_uring paths.

## Concerns for the controller

- **A7 L1 and the client halves of LOW-1 / LOW-2 / L5** live in rune-protocol and rune-client-cpp, which this brief keeps out of scope. They need a follow-up there. The client also carries its own copy of the epoll-first install and needs the same Aeron pin as L2.
- **Legacy gateway path.** `readView` (used by the legacy inline gateway push path without a WorkerPool, `ClientGateway.cpp:3963`) does not continue past a compacted tail gap. Only `readInto` does. The RC and PSM rebuilds already skip gaps themselves.
- **D26 LOW-1 trade-off.** Cold mapped readers now map a new active segment on the owner at their first read (an mmap sized to cover the active segment — up to the whole segment, not one 1 MiB mmap). Previously the provisioner pre-mapped it. Fixed by review M3 (the provisioner maps it again for logs with a mapped reader).
