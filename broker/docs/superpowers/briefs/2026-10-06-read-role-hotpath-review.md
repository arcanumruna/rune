# Review: hot-path B (read + role paths), branch `perf/read-role-hotpath` @ 2ab1bc2

- **Diff:** `git diff 70217bd...2ab1bc2`
- **Verdict:** **REJECT.** Re-review after C1 and the majors are fixed.
- **Counts:** critical 1, major 2, medium 6, low 4.

## What I verified

- **Focused tests:** `ctest -R "HotPathB|TruncateClampsSyncedLeo|EpochRestart"` in build-fast: 21/21 passed.
- **Atomics:** `scripts/check_no_atomics.py` is OK. Its categories are identical to the implementer report: RING 66, CLEANER 63, CRASH-ORDER 1, CPU-HINT 3, SLICE-B 44, UNSCOPED 27.
- **Grep:** the diff adds no `std::atomic`, `std::mutex` or `virtual`.

## What is correct (checked, no finding)

### Leadership gating
- A controller-managed `becomeLeader` above `fencePersisted_` is deferred (PartitionReplica.cpp `deferLeadership_`).
- It resumes only from the io completion, and only if the confirmed image carried *this* deferral's generation (`completePendingLeadershipIfDurable_`: `confirmedLeadGen == pendingLead_.gen && fencePersisted_ >= epoch`).
- The gate is event-driven, with no timer.
- The fence and the new epoch's cache entry are in the same image, so a restarted node cannot lose a fence it has already led at, through this path.

### Produce refusal
A sitting leader being re-elected refuses produce through `acceptsProduce()`. Txn markers are refused too: `appendControlBatch` checks `acceptsProduce()`. So the LEO cannot move under the pending image.

### Deferred follower
A deferred follower:
- does not fetch, apply or truncate (`doWork` at :2088, `applyReplicated`);
- answers with its old epoch/role;
- does not advance HW as the new leader.

### Write failure
A failed write sets the log dir offline and refuses the leadership (fail closed). This matches the synchronous fence's semantics.

### Two-slot alternation
- The async path always targets `1 - activeSlot`, and `activeSlot` moves only on a confirmed completion.
- `resetEpochStateForNewOwner` rewrites the *unconfirmed* slot with a higher seq.
- Cross-worker moves halt the broker; the only sanctioned move is the test-only two-phase drain-before-grant.

### File creation
`LeaderEpochState::create` does tmp, write, fsync, rename, then a full directory fsync. Each slot has a CRC, and the highest valid seq wins.

### Truncation and the recovery point
`prepareTruncationTo` lowers the on-disk recovery point through the io backend before any file is touched (E1d F6 preserved). R is capped at the cut until the truncation runs or is abandoned.

### F5 zero-copy push
- The view is consumed in the same call: runs are gathered into a claimed `ret_` slot.
- Compaction swap, retention and truncation all run on the owner thread, so nothing can unmap a live view. There is no use-after-unmap.
- Whole-batch boundaries are respected. A single oversize batch is answered `MessageTooLarge`.
- The gateway's `chunkView` is consumed inside the drain callback and never stored. I checked `handleWorkerReturn_`.

### Test contract changes (legitimate, not weakened)
- `ShardNonBlockingRegression.TruncateClampsSyncedLeo` now asserts the new no-sync property. It still asserts the synced-LEO clamp and `syncCallsForTest()==0`.
- `EpochRestartRegression` reads through `readDurableEpochCache`.
- In the latency gate's LeadershipChange scenario, the completion pass is measured as a HOT pass. Produce is skipped only for partitions that refuse it.
- `kKnownFailures` and the budgets are unchanged in the diff.

---

## CRITICAL

### C1. Epoch-cache durability is no longer ordered before data durability or re-fetch → undetected KIP-101/279 divergence after power loss

**Where:**
- `PartitionReplica.cpp:1645` (`checkpointEpochs_`): with `es_.fd >= 0 && externalFollowerFlush_` it only sets `es_.due`.
- Callers that previously fsynced synchronously before anything else happened:
  - follower apply assign: `:1340`, `:1356`;
  - `truncateLog`: `:2873`, the KIP-279 divergence truncation;
  - `becomeFollower` phantom drop: `:489`;
  - the DUR-1 cut and rebuild in `initReplica`: `:208`, `:234`.
- **Nothing waits for it:**
  - `epochStateDurable()` (`PartitionReplica.h:972`) is used by no production code;
  - follower fetch after a truncation is gated only on `pendingLead_` (`:2088`);
  - the follower flush and recovery-point advance are independent io requests.
- **Restore never reconciles a valid image against the batch stamps:** `initReplica` `:193-235` only applies the DUR-1 cut of entries starting ≥ LEO.

**Failure scenario** (single-node power loss or kernel crash; `kill -9` is safe because the slot `pwrite` is already in page cache):
1. Follower F holds divergent epoch-5 records [100,150) from an old leader. The new leader L at epoch 6 makes F truncate to 100, giving in-memory cache {3:0}. F re-fetches L's epoch-6 records [100,200), giving cache {3:0, 6:100}. These records are committed (HW ≥ 200).
2. F's follower flush and recovery-point checkpoint complete. The epoch-state write ({3:0} or {3:0,6:100}) is still in flight, or queued behind it.
3. Power loss. On restart, F restores the old valid image {3:0, 5:100}. The DUR-1 cut keeps 5:100 because 100 < LEO 200.
4. F is elected leader at 7, giving cache {3:0, 5:100, 7:200}.
5. Replica G still holds the same divergent epoch-5 records [100,150). G fetches with `lastFetchedEpoch=5` at offset 150. F answers that epoch 5 ends at 200, so G sees no divergence and appends from 150.
6. G now holds different bytes than F for committed offsets [100,150). If G later leads, committed records are silently replaced.

A simpler variant needs no truncation. F appends epoch-6 data that becomes durable, but the image carrying `6:x` does not. After restart the cache lacks epoch 6, and epoch-5 end-offset answers extend over epoch-6 records.

**Before this change** the cache checkpoint was fsynced inline at the assign/truncation, before the flush and before any re-fetch, so this window did not exist.

Kafka's async epoch-checkpoint flush (KAFKA-15046) is safe only because of two rules:
- the checkpoint is flushed before the recovery point advances (in `LocalLog.flush`);
- recovery re-assigns epochs from batches above the recovery point (`LogSegment.recover(…, leaderEpochCache)`).

**Fix (both parts required):**
1. **Ordering:** a recovery-point checkpoint may cover offsets only if the epoch-state image describing them is confirmed. Track a "cache covered up to" offset per confirmed image, and cap `advanceRecoveryPoint_` by it. Also, after a divergence truncation, do not fetch until the truncated-cache image is confirmed. Gate on `epochStateDurable()`, event-driven.
2. **Restore:** reconcile the restored cache with the stamps of every batch above the recovery point:
   - drop entries whose epoch disagrees with the stamp at their start offset;
   - assign missing epochs.
   This is cold and runs once at recovery.

**RED test first:** add a power-loss seam that reverts `leader-epoch-state` to its last *confirmed* image (or skips the slot `pwrite` until completion). Run follower apply of a new epoch plus flush completion, then the seam, then restart, and assert `leaderEpochCacheForTest()` contains epoch 6. Do the same for the truncation path. Mark both `*Regression`.

---

## MAJOR

### M1. No downgrade or re-upgrade story; stale legacy files stay present

**Where:** `PartitionReplica.cpp:193-201` (restore), `enableEpochState_` (`:262-314`).

Once `leader-epoch-state` exists, the legacy `leader-epoch-checkpoint` and `leader-epoch-fence` stop being written but are left on disk. Nothing records a format generation.

**Downgrade:** the old binary trusts the stale legacy cache and fence. The cache is missing every epoch since the upgrade, which is C1's divergence class. The fence is stale too: the restart fence is lower than an epoch this node already led at.

**Re-upgrade after a downgrade:** the new binary sees `Valid` and ignores the newer legacy cache the old binary wrote. The fence is safe because the legacy fence file is still max-ed in through `setLeaderEpochFenceFile`. The cache is not.

**Fix:**
- After the state file is created and directory-fsynced, unlink the legacy files and fsync the directory again. A downgraded binary then sees *Missing* and rebuilds from stamps. That rebuild path already exists in the old binary (MEDIUM-5).
- Treat "legacy files present AND state file present" as "a downgrade ran". Restore from the legacy cache plus a stamp reconcile, take max(fences), then rewrite the state file.
- Document the downgrade procedure in the spec.

**Tests to add:**
- upgrade from a legacy-only directory with *stale* data (must-work-with-stale-data);
- legacy plus state present.

`EpochStateRestoreAndCorruption` covers neither. Its legacy assertion `!(exists(ckpt) && exists(fence))` is also too weak: it passes when one of the two is still written.

### M2. The ext4 regressions are attributed to F6 without proof; the change plausibly causes them

The report attributes FollowerTruncation p99.9 102→486 / max 865→2,086, Retention max 1,350→4,039 and TxnMarkers/4p 217→2,668 to VM `pwritev` stalls.

The mechanism the change introduces predicts exactly this pattern: ext4 regresses while tmpfs stays flat.

- **Before:** the truncation's recovery-point fdatasync, index msync and directory fsync ran *inline*, so no intake `pwritev` from that worker overlapped them. The cost landed in the COLD role-op pass.
- **Now:**
  - the same fsyncs run on the io backend, *concurrently* with intake appends;
  - every worker-driven truncation now bumps `dirMutGen_`, forcing an async directory fsync (`Log.cpp:1759` `deletesFiles || deferSealSync_`);
  - each role change adds an epoch-state fdatasync.
- A directory fsync (or the rename/ftruncate metadata) forces a jbd2 commit, and an O_APPEND `pwritev` that extends `i_size` waits on the running transaction. The cost has moved from a COLD pass into HOT intake passes, where the 100 µs budget applies.

"About 40% more passes" is an exposure argument, not a cause.

Retention and TxnMarkers/4p have no truncation or role change, so their maxima need their own explanation. Candidates: noise from single best-of runs, or the F5 128 MiB mapping and munmap churn on the cleaner and provisioner (mmap_lock / TLB shootdown). The report's own probe shows up to 259 µs worker stalls from concurrent mmap/munmap.

**Required before merge:**
- an interleaved ABAB (n ≥ 15) of 70217bd against c2f8d9b on the same VM, per scenario;
- per-scenario counts of io-backend fdatasync and directory-fsync submissions;
- a bench-box run.

If the overlap mechanism is confirmed, the truncation path needs a design that keeps journal commits off the window where intake appends run. For example, the cut, ftruncate and rename sequence could be batched with the seal's directory fsync instead of adding one per truncation.

---

## MEDIUM

### Md1. A cancelled deferred leadership can leave a durable phantom epoch entry
**Where:** `cancelPendingLeadership_` `:1914`; `prepareEpochStateWrite` encodes `pendingLead_.cache`.

1. An image carrying (newEpoch, leo) is in flight.
2. A newer role cancels the deferral. The real cache is only marked `due`.
3. The image lands, F follows another leader, and appends past `leo`.
4. A crash before the superseding write gives a restored cache with an epoch this node never led. The entry survives the DUR-1 cut once LEO > leo.

KIP-279 answers for that epoch are then wrong. The old synchronous path persisted only the fence before leading, never a cache entry.

**Fix:** on cancel with an image in flight, refuse fetch/apply until the superseding image is confirmed, or keep the pending entry out of the image (write the fence only). This is part of the C1 ordering fix.

### Md2. A failed non-leadership epoch-state write is never retried and never fails stop
**Where:** `epochStateWriteCompleted` `:1847`.

The comment says "retried with the next change", but `due` was cleared at prepare and is not re-armed. A failed fdatasync on the cache image therefore leaves the durable cache stale indefinitely (feeds C1), and the log dir stays online (fsyncgate).

**Fix:** re-arm `due`, and treat a fdatasync EIO as a storage failure, as the log's own fsyncs are.

### Md3. A full route table wedges a deferred leadership forever
**Where:** `Worker.h:4897`.

`routeInsert_` failure only logs `critical`. The replica stays `InFlight`, and the completion arrives unrouted and is dropped. As a result:
- `epochStateWriteDue()` stays false forever;
- the deferred leadership never completes;
- the deferred follower fetches nothing.

Only a newer role recovers the partition, which is not time-independent convergence.

**Fix:** on insert failure, fail closed (`epochStateWriteCompleted(seq, EIO)` semantics) or re-arm. Add a test.

### Md4. FREE-tier per-partition cost
**Where:** `PartitionReplica.h:1369`.

Each disk replica now holds:
- a 16 KiB `es_.image`, zero-filled and so resident;
- an extra open fd;
- a 32 KiB file;
- a `std::map` copy per deferral.

At 10k partitions that is 160 MiB RSS and 10k fds just for this.

**Fix:** use a per-worker image pool sized to the epoch writes in flight, open the fd lazily or keep it on the worker, and encode only `imageBytes(n)`.

### Md5. F5 whole-segment read maps: virtual-address cost
**Where:** `LogSegment.cpp:192`.

`readMapBytesFor` rounds rollBytes + 1 MiB up to a power of two:
- a 64 MiB roll gets a **128 MiB** mapping per active *and* per pre-provisioned staging segment;
- a 1 GiB roll gets 2 GiB.

There is no commit charge (MAP_SHARED, PROT_READ), but virtual address space is finite:
- arm64 kernels with a 39-bit VA have 512 GiB, enough for about 4k such maps;
- containers can set RLIMIT_AS.

On exhaustion the map fails silently and the lazy remap path, the thing this commit removes, returns on the worker.

**Fix:** use page-round(rollBytes + max batch) instead of a power of two, count map failures in metrics, and add a many-partitions sizing test or calculation for the FREE tier.

### Md6. The inline write path can clobber the confirmed slot and close an fd with I/O in flight
**Where:** `writeEpochStateInline_`, `:1749` and `:1735`.

While `InFlight`, the inline path targets `1 - inflightSlot`, which *is* `activeSlot`, the newest confirmed image. A torn inline write plus an unlanded async write falls back to an older image. The re-create branch also `close(es_.fd)`s while an io-backend fdatasync on that fd may be queued, so fd-number reuse can redirect it.

This is reachable only if `setExternalFollowerFlush` goes true→false with a write in flight. `EpochStateRestoreAndCorruption` performs that toggle.

**Fix:** forbid the toggle while not `Idle` (assert), or wait for `Idle`.

---

## LOW

- **L1.** `submitEpochStateWrite_` does the slot `pwrite` on the worker. It is cold (role changes only) and acceptable, but the `prepareEpochStateWrite` re-create branch runs `LeaderEpochState::create` (fsync + rename + dir fsync) **on the worker**. That is rare (more than ~512 epochs), but it is a worker fsync. Pre-size the file or post the re-create to the control thread.
- **L2.** If both slots are corrupt, the fence comes from rebuilt stamps plus a stale legacy fence. A fence for an epoch led with no appended batch is lost. `EpochStateRestoreAndCorruption` does not assert `restartFenceEpochForTest()` after corruption. Assert it, and consider refusing leadership at or below the "unknown" fence until the controller bumps.
- **L3.** `recover(syncIndex=false)` renames an un-msynced index. A directory fsync makes the rename durable, not the index data. Confirm that R can never cover a range that a post-crash restart would look up through that index without rebuilding it. `UnsyncedTruncationRebuildRecoversAfterCrash` covers the rebuild, but not R positioned inside the active segment.
- **L4.** `becomeLeader` with an epoch lower than the pending one cancels the deferral and proceeds against `epoch_`, the old epoch. This relies on the controller's epoch monotonicity. Refuse `newEpoch < pendingLead_.epoch` explicitly.

## Coverage gaps (RED-first tests missing)

- **C1:** the power-loss ordering of epoch-state against data flush and re-fetch, for both the assign and truncation variants.
- **M1:** upgrade from a stale legacy-only directory; legacy plus state present; downgrade.
- **Md2:** non-leadership write error.
- **Md3:** route-table-full.
- **Md1:** cancel with the pending image in flight, then a crash.
- **Md5:** many-partition virtual-address sizing.
- The nine `HotPathBRegression.*` tests exist, are marked `*Regression`, and pass. The report says each was shown RED, but I did not re-verify that.
