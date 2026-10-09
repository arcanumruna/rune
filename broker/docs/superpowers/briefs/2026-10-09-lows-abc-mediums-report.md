# Report: the MEDIUMs (and cheap LOWs) of the lows A/B/C review

Branch fix/lows-storage-replication (worktree rune-broker-wt71), on top of b045e77. Not merged, not pushed.
All builds and tests ran in the arm64 Linux dev container (`rune-dev/devrun.sh`). Latency-gate tests were skipped.

| Item | Status | Commit |
|---|---|---|
| M1 persistent read EIO | done | 17e176d |
| M2 epoch-state re-create on the worker pass | done | 6d8ae72 |
| M3 owner mmap after a roll | done | bf28f84 |
| L1 cap-deferral memo staleness | done | a12f5ad |
| L6 double read of a too-large first batch | done | 6ab908f |
| L7 stale scan-window comment | done | 2b086de |
| L9 report wordings (+ the M3 wording) | done | 2929f77 |
| (found) Throttler burst within one clock tick | done | 872630c |
| L2, L3, L4, L5, L8 | ruled | — |

## M1: a persistent read EIO fails the log (17e176d)

- **Fix.** `LogSegment::readInto` now sets the log's sticky `failedErrno` on a pread I/O error (first error wins),
  the same as a write or fsync failure.
  - The owner's per-pass `checkStorageFailure_` (both Worker loops) routes it to `failLog_`. That runs the
    log-dir-failure path: the D15 LogDirOffline mark and the broker halt. This matches Kafka, where a read
    IOException takes the log dir offline.
  - A failed log answers `IoError` at once, without a pread, so a bad sector is never re-read on the duty pass.
  - The Worker, PartitionReplica and header comments and the push-read log line were updated.
- **Test.** `LogSegmentRegression.ReadIntoIoErrorFailsTheLogStickyAndIsNotRetriedRegression`.
  - It truncates the segment behind the log's back, then asserts that the log is `storageFailed()`.
  - It then restores the bytes and asserts that the next read is still `IoError`, with `readIntoBytes` unchanged
    (no pread).
- **RED.** Before the fix the test failed: the log was not failed, and the second read issued a pread (114822
  bytes against 12349).

## M2: no epoch-state re-create on the worker pass short of outgrowing the slot; back off a failed one (6d8ae72)

- **Fix.** On the worker path, `prepareEpochStateWrite` re-creates the file only when the image outgrows the
  **slot** capacity. That is the pre-D23 threshold: 4096 or more entries, once per doubling.
  - An image past the reserved head (D23), or with `reserved = 0` after a failed reservation at enable, is written
    in place, async, through the io backend. This is the pre-D23 behaviour. That write may allocate a block, and a
    failure goes through the async failure path with its backoff.
  - The cold inline writer (control thread / construction) keeps the D23 reserved-head threshold.
- **Backoff.** A failed worker re-create, and the existing open failure, now go through `failEpochStateWriteNow_`,
  which uses the regular completion failure path:
  - `retryNotBeforeNs` backoff (1 ms << failures);
  - logged;
  - fail-closed for a pending leadership;
  - fail-stop after `kMaxEpochStateFailures`.
  - The errno is carried in `es_.lastErr`.
- **Test seam.** `PartitionReplica::failEpochStateReserveForTest(err)`.
- **Tests.**
  - `LeaderEpochFenceRegression.FailedReserveAtEnableNeverRecreatesOnTheWorkerPassRegression`: the reservation
    fails at enable, then a leadership's fence write on the worker path must be an async in-place write (no
    re-create, no inline write).
  - `LeaderEpochFenceRegression.FailedWorkerRecreateBacksOffRegression`: a crafted 4096-byte-stride file is driven
    to about 338 epochs until the slot is outgrown, then the dir is removed. One re-create is attempted, none
    within the backoff, the leadership fails closed, the change stays due, and it is retried after the backoff.
- **RED.**
  - Test 1: `prepareEpochStateWrite` returned false (an inline re-create ran).
  - Test 2: 6 re-creates and 6 errors in 6 same-time passes, where 1 is expected.
- **Hot-path budget.** The worker / owner duty pass must stay at 100 us or less. `prepareEpochStateWrite` with
  `reserved = 0`, measured in the dev container (5 runs each):

  | | Time |
  |---|---|
  | Before | 690 to 820 us (inline tmp + fsync + rename + dir fsync; on a real disk, ms) |
  | After | 0.7 to 1.9 us |

  The in-place path is otherwise unchanged (one capacity compare).
- **Accepted risk (documented in `LeaderEpochState::reserveFor`).** Past the reserved head, a worker write may
  allocate a block, which can mean ENOSPC on a full disk. That is the pre-D23 behaviour. Its failure is now backed
  off, and it fails closed and then fails stop.

## M3: a mapped-reader log's new active segment is mapped off the owner (bf28f84)

- **Fix.**
  - `maybeGrowReadMap_` now posts a provisioner map request from 0 for an unmapped **active** segment of a log with
    a mapped reader, instead of returning. `maybeRoll`'s growth trigger fires at the first append after the roll.
  - A roll clears `growOutstanding_`. The outstanding request was for the sealed segment, so its late answer is
    discarded as superseded (seq-matched, re-review N2). Without this, an unpolled answer kept the new request
    from ever being posted.
  - `mapReaders_` decays: a roll with no `readView` since the previous roll clears it, so a finished rebuild walk
    stops off-owner mmaps for later segments.
  - A `readView` that still beats the provisioner and maps the active segment on the owner is counted in
    `ownerFallbackRemaps`.
- **Test.** `HotPathBRegression.ActiveSegmentOfAMappedReaderLogIsMappedOffOwnerAfterARollRegression`.
  - The scenario: a mapped reader, a roll, the provisioner serves the posted requests, then `readView` on the new
    segment.
  - The test asserts `activeReadMapRemaps == 0` and `readMapGrowths >= 1`.
- **RED.** One owner remap and no growth.
- **Budget.** On the owner, the cost is one dup plus a ring post per roll, the same as the existing half-full
  growth. The whole-segment owner mmap is gone. No per-append cost was added (same `maybeRoll` condition).

## Cheap LOWs

- **L1 (a12f5ad).** After each controlled poll, the gateway passes the controlled subscription's image count to
  `noteControlledImages_`.
  - A drop (a lost client image) clears every cap-deferral memo. This is cold, and runs only when
    `capDeferredSessions_ > 0`.
  - A memo is only a fast-path hint. A frame that does return runs the full check and is re-memoized.
  - Test: `GatewayProduceCreditRegression.CapDeferralMemoIsClearedWhenAClientImageIsLostRegression`.
  - RED: `capDeferredSessions` stayed 1 after the image drop.
  - The optional re-validation of the routed worker on a memo hit was not done. It is covered by the LOW-R4
    head-of-line note.
- **L6 (6ab908f).** A too-large first batch that was read through cleanly is remembered per segment (file position
  and length).
  - A retry skips the second read-through. The memo is forgotten at truncate / recover; positions change nowhere
    else.
  - The read-through bytes are now counted in `readIntoBytes`. They were pread but uncounted.
  - Test: `LogSegmentRegression.TooLargeFirstBatchIsReadThroughOnceRegression`.
  - RED: the read-through was invisible (uncounted) and repeated.
  - Residual risk: a page that read cleanly once and later goes bad (evicted, then EIO) would be served through the
    mapping, which can SIGBUS. This is rare and accepted, in exchange for removing a full batch of I/O per retry.
- **L7 (2b086de).** The `onWorkerCapFull_` block and the `capEpisodeTags_` comments now say that tags wrap after
  65535 scan windows (about 4M deferred polls, about 80 s at 50k polls/s), not after 65536 episodes, and that a
  wrap is harmless.
- **L9 + M3 wording (2929f77).** The batch B report was corrected:
  - "IoError ... is sticky" meant within one `readInto` call only. Since M1 the log fails, sticky.
  - "one 1 MiB mmap" means "an owner mmap sized to cover the active segment". It is now mapped off the owner again
    (M3).
  - The `mapReaders_` stickiness note now mentions the decay.
- **Found (872630c).** `Throttler::maybeThrottle` returned without throttling when `elapsed == 0`, which happens
  with a burst inside one clock tick of the window start.
  - The existing `Throttler.BurstOverLimitSleeps` flaked at 0 ms in the full storage suite in the container.
  - The fix compares `bytes > rate * elapsed` without dividing.
  - No new test was added: the clock cannot be injected, and the existing test covers it.

## Ruled

- **L2 (MetadataService oversized unit).** Deferred to the backlog.
  - It is a control-plane fairness edge: it needs a busy bump / preferred-leader stream together with a unit
    larger than the budget.
  - The suggested fix (admit when `members.size() > maxRecords`) changes the D15b l2 budget semantics. It should
    be reviewed with that code, not in a storage / replication round.
- **L3 (follower drops a compacted tail-gap continuation).** Pre-existing. It is not a regression: before, the
  follower was stuck on empty answers. It needs follower-side Kafka semantics (accept base > LEO on a compacted
  topic). Backlog item.
- **L4 (dr-e2e sweep across pid namespaces).** This is dr-e2e harness infrastructure, out of the broker's scope.
  The sweep is fail-safe for non-harness containers. A host / boot-id label is a backlog item.
- **L5 (strict RUNE_IDLE_MAX_PARK_US).** Kept fail-loud, as accepted by the review. A CHANGELOG / SELF-HOSTING line
  is a docs follow-up.
- **L8 (wall-clock tests).** Bounded and polled, and documented. Injecting the session-timeout and lease values is
  a test-speed backlog item.

## Tests run (arm64 dev container, latency gates excluded)

| Target | Result |
|---|---|
| rune_storage_tests | 421 pass, 1 environmental fail |
| rune_storage_integration_tests | 41/41 |
| rune_replication_tests | 429 pass, 1 environmental fail |
| rune_dr_integration_tests | 62/62 |
| rune_hotpath_b_regression_tests | 42/42 |
| rune_hotpath_b_alloc_regression_tests | 2/2 |
| rune_gateway_produce_credit_tests | 15/15 |
| rune_gateway_read_committed_push_tests | 8/8 |
| rune_worker_push_tests | 14/14 |
| rune_worker_tests | 9/9 |
| rune_compaction_cleaner_agent_tests | 36/36 |
| rune_compaction_correctness_tests | 4/4 |
| rune_retention_cleaner_agent_tests | 11/11 |
| rune_shard_mode_tests | 53 pass, 1 flaky |

Environmental and flaky failures, all pre-existing:

- **Permission-based tests.** `SegmentLifecycleRegression.ProvisionerFailureThenInlineFailureFailsTheLogRegression`
  and `InternalIsrRegression.DirectoryFsyncFailureIsNotAcked` fail because the container runs as root, which
  bypasses `chmod`. The first was confirmed failing at b045e77 as well.
- **3-node Aeron test.** `ShardModeRegression.AdvertisedIngressRoutesAreLiveForEndpointUnawareAndAwareClients`
  failed 2 of 3 runs at baseline b045e77 too, in this container.
