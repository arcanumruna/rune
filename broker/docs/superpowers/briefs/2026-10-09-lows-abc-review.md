# Review: lows batches A, B and C (merged into integ/perf-defaults)

Diff: `git diff 4b0b6a6..HEAD` (merges 72cce32 / 66fd998 of fix/lows-consensus-metadata,
fix/lows-storage-replication, fix/lows-gateway-worker). This was a read-only review. No binaries ran,
because x86_64 test binaries hang on this host. All statements below come from reading the code, checked
against the three reports.

## Verdict

| Severity | Count |
|---|---|
| CRITICAL | 0 |
| MAJOR | 0 |
| MEDIUM | 3 |
| LOW | 9 |

The claimed behaviour changes are implemented as described, and the RED tests discriminate:

- **Encrypted-produce replay fix.** It does not weaken replay protection.
  - `openIngressFrameUncommitted` and `openProduceFrameUncommitted` still run the read-only window check
    before the AEAD open.
  - Seq 0 is always refused, so the `openSeq != 0` sentinel on the worker is sound.
  - The commit happens on the same thread, straight after the consuming handler, with no interleaving.
  - Both regression tests assert that a replay of a consumed frame is dropped (gateway: `produceInFlight`
    stays 1; worker: LEO stays leo+1).
  - The only path that commits a Produce on the worker is a consumed one (decode-fail drop, or past the
    roll hold). `kIngressHeld` is the only negative return of `handleWorkerIngressFrame_`.
  - On the gateway, `handleProduce` returns false only for the shard-mode cap deferral, which is
    controlled-poll ABORTed.
- **Other changes, all correct:**
  - The D23 fd-leak fix: `teardownSyncFds_` is declared before `io_`, so it is destroyed after it, and the
    room is checked before the pwrite and fsync.
  - The tombstone set: backward-shift delete is correct, load is at most 1/2, and sid 0 cannot collide in
    practice.
  - The A7 devirtualisation: all classes are final and checked by `static_assert`.
  - The D15b l1 / l2 changes.
  - The dr-e2e sweep only ever touches containers that carry the `rune.dr.owner` label. Compose clusters,
    including those kept by `DR_KEEP_CLUSTER`, carry no label and are never touched.
- **Shared-nothing:** no atomics, mutexes or new virtual calls were added on any hot path. The only new
  allocations are on cold paths: the control-thread scratch, `~Worker`, and the scan-window tags.

Three MEDIUM findings are about cold I/O paths that the changes made reachable, or left without
escalation or backoff, against the rule that owner-thread I/O never runs on the duty pass.

---

## MEDIUM

### M1. A persistent read I/O error becomes a per-pass failing pread loop with no escalation (D26)

**Where:**
- `libs/node/include/rune/node/Worker.h:3288` (`readNoFault_`, IoError answered empty);
- `libs/replication/src/PartitionReplica.cpp:3335` (`readLog_`, IoError answered empty);
- `libs/storage/src/LogSegment.cpp` `readInto` (`failed()`), where ioErr is sticky only within one call.

**Scenario:**
1. A bad sector (EIO) sits under batch B of partition P.
2. Before D26, the mapped fallback raised SIGBUS and the node crashed: loud and fail-stop.
3. Now every worker pass that reaches the cursor at B issues the same pread again. The push read retries
   next cycle, and a follower fetch at B is answered empty, so the follower re-fetches at once.
4. On real disks an EIO pread can block for tens of ms up to seconds while the drive and kernel retry. That
   time is spent on the worker's duty pass and stalls every partition that worker owns.
5. Nothing escalates:
   - the log dir is not marked offline;
   - the error is not made sticky (`storage_.failedErrno`);
   - no D15 LogDirOffline mark is set, so leadership never moves.
6. With acks=all, HW pins at B, because the follower never advances past it, and produces time out.

Kafka handles this case by turning a read IOException into log-dir-offline.

**Fix:** treat a readInto IoError like a write failure.
- Set the log's sticky storage failure (`failedErrno`), or count consecutive IoErrors per log and fail it
  after N.
- The existing log-dir-failure path, `logDirFailed` reports and D15 LogDirOffline marks, then moves
  leadership.
- At the least, add a per-cursor backoff (`retryNotBeforeNs`) so a bad page is not re-read on every pass.
- RED test: use the existing truncated-file seam, assert the log is failed and reported after the first
  (or Nth) IoError, and assert the read is not retried on every pass.

### M2. D23 makes the inline epoch-state re-create on the worker reachable, with no backoff on failure

**Where:**
- `libs/storage/include/rune/storage/LeaderEpochState.h:90` (`reserveFor`: twice the history at
  creation, at least one 4 KiB block);
- `libs/replication/src/PartitionReplica.cpp:1788` (a failed `reserve` leaves `es_.reserved = 0`);
- `:1929` (`prepareEpochStateWrite` falls to `writeEpochStateInline_` whenever the image does not fit
  `reserved`);
- `:1880` (a re-create failure returns false without setting `retryNotBeforeNs`).

**Scenario:**
1. Before D23, an in-place write fell to the re-create path (tmp write, fsync, rename, dir fsync) only
   after more than 4096 retained epochs. That was "practically never on a worker" (D21 L1).
2. Now the threshold is the reserved head:
   - about 338 retained epochs for a short history at creation, then once per doubling (about 4 more times
     before 4096);
   - **0 epochs** when `reserve()` failed at enable, for example ENOSPC or EIO at startup, which is exactly
     the disk-full case L4 targets.
3. `prepareEpochStateWrite` is called from `Worker::submitEpochStateWrite_` on the duty pass. The re-create
   therefore runs owner-thread file creation plus two fsyncs, which takes milliseconds.
4. If the re-create fails (disk full), nothing sets `retryNotBeforeNs`. The replica stays due, so the
   worker retries open(O_TRUNC), ftruncate, pwrite and fsync on every pass until space returns. That
   failure path does not log either; it only increments `es_.errors`.

**Fix (pick one):**
- (a) When the image exceeds `reserved` but still fits the slot capacity, do not re-create. Extend the
  reservation off the worker by posting a reserve request to the control or provisioner thread, as the
  read-map growth does. Until that completes, write in place, which is the pre-D23 behaviour and an
  accepted risk.
- (b) Reserve a larger fixed head, for example `imageBytes(1024)`, which is about 12 KiB per slot.
  Re-creates on the worker then need more than about 500 retained epochs.

In every case, set `retryNotBeforeNs` with backoff on a failed inline re-create, as the async failure path
does (review Md2). RED test: `reserve` fails at enable (a fault seam), then the first worker epoch write must
not run an inline re-create on every pass.

### M3. Logs with a steady mapped reader now mmap on the owner after every roll, and the report understates the cost (D26 LOW-1)

**Where:**
- `libs/storage/src/LogSegment.cpp:102` (`prepareStaging` no longer maps);
- `libs/storage/src/Log.cpp:1088` (`maybeGrowReadMap_` returns while `mapped == 0`);
- `libs/storage/src/Log.cpp:274` (the growth request is gated on `mapReaders_`).

**Scenario:**
1. On a log that is read through the mapping, such as the RC rebuild, a PSM rebuild after each leadership
   change, `prepareTruncationTo`, the too-large fallbacks, or the legacy inline gateway push
   (`ClientGateway.cpp:4016`), the first `readView` after each roll finds the new active segment unmapped.
2. `LogSegment::ensureReadMap` then maps on the reading thread, which is the owner or worker. The size is
   `kReadMapMinBytes` doubled until it covers `logSize_`, not the "one 1 MiB mmap" the report states. For
   a PSM rebuild of a nearly full 1 GiB segment that is a 1 GiB `mmap`.
3. That mmap takes the process `mmap_lock` for write, the same lock whose contention #65 measured at
   about 1 ms per worker pass.
4. Because `maybeGrowReadMap_` returns while `mapped == 0`, the provisioner is never asked to pre-map the
   next active segment, even when `mapReaders_` is set.
5. `mapReaders_` is also sticky: after one rebuild, every later half-full growth on the mapped segment
   posts an off-owner mmap that may never be read.

This is mostly cold, because the rebuild readers already page-fault through the mapping, which costs more.
It still adds owner-thread mmap work that #65/D30 had removed, and the report's cost statement is wrong.

**Fix:**
- In `maybeGrowReadMap_`, when `mapReaders_ && active && mapped == 0`, post a provisioner map request (the
  grow message can carry a target from 0) instead of returning.
- Optionally make `mapReaders_` decay, for example clear it at roll when no `readView` happened on the
  sealed segment.
- Correct the report and comment ("one 1 MiB mmap" means "an owner mmap sized to cover the active segment").

---

## LOW

### L1. The cap-deferral memo can keep every frame off the gateway fast path until the session closes (R3)

**Where:** `libs/gateway/src/ClientGateway.cpp:2643` and `:1227`.

- `capDeferredOn` is cleared only when a later Produce of that session runs `produceFramePause_`, or when
  the session closes.
- If the deferred frame never comes back (the client's image goes away and the session is reaped about
  10 s later, or up to `maxHeldIdleNs_` if it is Held), `capDeferredSessions_ > 0` stays set.
- Until then, `admitClientFrame_` leaves its `[[likely]]` early return for every frame of every session
  (a header decode plus a `sessions_.find` each).
- The memo also keys on worker w only. A redelivered head frame whose partition has since moved to another
  worker waits for w's share. The report's LOW-R4 head-of-line note covers this.

**Fix:** clear the memo in the controlled image watch when the session's image is lost. Optionally
re-validate the routed worker on the memo hit.

### L2. D15b l2: a unit larger than the budget is admitted only on a tick that decided nothing else

**Where:** `libs/node/include/rune/node/MetadataService.h:1001`.

- `decideBumps_` and the balancer run first.
- On a large cluster with continuous preferred-leader or bump records, for example during a rolling
  restart, `recs` is never empty when `decideLeaderless_` runs, so an oversized unit can still starve.

**Fix:** also admit when `members.size() > maxRecords` (the unit can never fit in any budget). The drain
stays bounded by `kMaxRecordsPerBatch`.

### L3. The compacted tail-gap continuation also serves follower fetches, but followers drop non-contiguous batches

**Where:** `Log.cpp:1054` and `PartitionReplica.cpp:1344`.

- `readLog_` (fetch, owed and push) now continues past a leader's compacted tail gap and serves the next
  segment's batches.
- The follower skips every batch whose base is above its LEO, so a follower whose LEO falls in a leader's
  compacted gap still never progresses. It now receives, and drops, the same bytes on every fetch instead
  of an empty answer.
- This is pre-existing (before, the follower was stuck on empty answers) and not a regression.

**Fix:** handle it on the follower side (accept base > LEO on a compacted topic, as Kafka does). It needs
a backlog item.

### L4. The dr-e2e sweep's pid liveness is wrong across pid namespaces and hosts

**Where:** `dr-e2e/harness/dockerutil.py:109`.

- The sweep only removes harness-labelled containers, so it can never kill non-harness containers.
- It can kill a *live concurrent harness run's* containers when that run's pid is not visible to this
  process: a harness inside a container or CI agent sharing the daemon, or a remote `DOCKER_HOST`. There
  `os.kill` raises ProcessLookupError, so the run is judged dead.
- PID reuse fails safe (the containers are kept).
- Running `dr-e2e/tests/test_harness_dockerutil.py` also triggers the real sweep through the autouse
  session fixture.

**Fix:**
- Add a `rune.dr.host=<hostname>:<boot-id>` label and sweep only containers whose host label matches.
- Skip the sweep for the unit-test directory.

### L5. RUNE_IDLE_MAX_PARK_US is now strict, which changes startup behaviour

**Where:** `libs/node/src/NodeConfig.cpp:1346`.

- A value of `0`, a value with surrounding whitespace (`"200 "`, common in env files and YAML), or a unit
  suffix used to be silently ignored, with the default of 200 applied. It now refuses startup.
- Every repo use (`=10`, `=50`, `=200`) still parses.
- This is acceptable as a fail-loud change, but the startup refusal deserves a CHANGELOG or SELF-HOSTING
  line.

**Fix:** optionally trim surrounding whitespace before parsing.

### L6. A too-large first batch is read twice on every retry

**Where:** `libs/storage/src/LogSegment.cpp:688`.

- `tooLarge` preads the whole batch through `buf` to surface EIO, then the caller re-reads it through the
  mapping.
- For a fetch or push that keeps hitting the same oversize batch (back-pressured consumer, retrying
  follower), that is about twice the batch size of I/O on the worker per attempt. It is cold and bounded by
  the maximum batch size.

**Fix:** none needed. Optionally remember a validated `(segment, pos)` so the read-through is skipped next
time.

### L7. The scan-window tags wrap faster now

**Where:** `ClientGateway.cpp:1345`.

- The D8c reader marks are now refreshed once per 64 deferred polls, so the uint16 `capEpisodeTags_` wraps
  after about 4M deferred polls (about 80 s at 50k polls/s).
- On a wrap, a stale `creditTagOn` can alias a fresh window. The effect is harmless (at worst one window's
  delay to an eviction), but the comment calling it "after 65536 episodes" is now wrong ("windows" is
  correct in SessionTable.h, but not in the `onWorkerCapFull_` block).
- Note: the window is pass-count based, so a slow-but-reading Held client gets about 64 gateway passes to
  credit before it is eligible again. That is aligned with D8d and time-independent by design.

### L8. Some tests depend on the wall clock

**Where:** `ShardModeTest.cpp` (`PausedInvokerClientResumingWithoutReadingIsFoundNotReapedRegression`,
`HeldDirectClientRecoveringAfterTheLeaseIsNotReapedOnReleaseRegression`).

- The tests take 13 to 20 s each because they wait for the real 5 s NOT_CONNECTED timeout and the 10 s
  lease. The client also idles with `sleep_for(100us)`.
- The waits are bounded and polled rather than fixed sleeps, and the report documents them. They are still
  slow and timing-coupled.

**Fix:** inject the session-timeout and lease values (test config) to bring them to about 1 s.

### L9. Report accuracy

The "one 1 MiB mmap" claim is covered in M3. Separately, "IoError ... is sticky" in the batch B report
means sticky within one `readInto` call only. Nothing persists across calls, as M1 shows. The report wording
should say so.

---

## Checked and found correct (no finding)

- **Replay window, gateway and worker:**
  - the read-only check runs before the open, the commit only after the AEAD tag verifies and the frame is
    consumed;
  - an ABORT stops the image, so no newer seq of the same image can commit and push the held seq out of
    the window;
  - non-Produce frames commit at once;
  - a Produce consumed by an error answer commits;
  - a Produce dropped by `checkFrame` after decrypt is not committed, which is harmless: its replay is
    dropped the same way.
- **D23 L1:**
  - `routeHasRoom_` runs before the pwrite;
  - `epochStateWriteNotSubmitted` restores `due` and `pendingMin`;
  - the dup is closed on every branch;
  - `~Worker` defers closing the in-flight dups until after `io_` is destroyed.
- **D23 L4:** `reserve` never touches the newest image (`keepSlot` / `keepLen`), the SEEK_HOLE skip makes a
  restart free, and it runs on the construction or config thread only (`enableEpochState_` callers:
  constructor, `setExternalFollowerFlush`, migration).
- **D23 L5:** `kNotWrittenYet` and the explicit `firstErr` are captured right after each syscall.
- **D26 bound:** the clamp applies only when `startOffset < bound < segment nextOffset`; the first batch is
  still read whole (the scan reads on); the HW and LSO are batch-aligned, so no deliverable bytes are cut.
- **Absent continuation:** it stops at the bound, never runs past Unreadable or IoError, and runs only on a
  cold branch.
- **A7:**
  - `ShardIdle` has direct dispatch for all three strategies that `makeShardIdle` can return;
  - `startRunners` and `startShardRunners` are mutually idempotent;
  - `stopAndJoinRunners` joins the shard runners;
  - under the Aeron pin, any other release gets stock polling with no wake fds, and `makeShardIdle` with
    `n == 0` falls to backoff or timed park.
- **D15b l1:** the queued-key filter is control-thread only, the scratch capacity persists, and it runs
  only when both the queue and the output are non-empty.
- **Tombstones:** the FIFO plus the open-addressing set agree, eviction erases the oldest from the set
  first, and a repeat close keeps the older tombstone.
- **dr-e2e:** `_with_labels` applies only to `docker run`, and every harness `docker run` uses the
  `["docker","run",...]` form. The timeout cleanup removes containers by the invocation label, so it never
  touches containers of other invocations.
