# Segment-lifecycle hot path (F1-F4): report (2026-10-06)

- **Branch:** `perf/segment-lifecycle-hotpath` (worktree `rune-broker-wt58`). Not pushed.
- **Base:** WIP checkpoint `c693c24` (on `e09b2f7` latency gate + `a940b8b`), merged with `integ/perf-defaults` (`aa93eec`, no conflicts).
- **Commits:** `e660223` (storage: finished F1-F4 + roll/rename correctness), `48a35c2` (node: worker hold test + start guard), plus this report.
- **Findings covered:** F1-F4 from `rune-broker-wt26/docs/superpowers/briefs/2026-10-05-latency-gate-report.md`.

## What changed

### Kept from the WIP, now finished and tested

**F1 + F2: `SegmentProvisionerAgent`.** This is a dedicated thread with BackoffIdleStrategy.
- It keeps `segmentProvisionDepth` (default 2) READY segments per worker-driven disk log. Each one is created, its index is mmapped and prefaulted, its read map is open, and its blocks are fallocated.
- It is event-driven. Every adoption posts a demand on an M2O `ControlRing`, and a 100 ms rescan tops up anything a full ring missed.
- It gets its replica view through its own generation-acked `CleanerViewChannel`, so a removed replica's Log is freed only after the provisioner acks.
- It also performs the staging→base renames. The cleaner's own provisioning is turned off (`setExternalProvisioner`), and provisioning is never stalled by a compaction pass.

**F2 + F3: a roll never blocks the owner.** A provisioned log with no ready segment, or a worker-driven log whose async seal queue is full (16), *defers* the roll (`Log::rollDeferred()`).
- The worker HOLDS the produce frame: `handleWorkerIngressFrame_` returns `kIngressHeld`, and the controlled poll ABORTs so the frame is re-delivered later.
- Appends that still arrive land in the active segment: follower apply, markers and op-ring appends.
- There is no inline create, no inline seal fsync and no owner fallocate any more (`ownerPreallocs_` stays at 0).

### New in this round

**F4 / D16: chunked persistent sealed snapshot.** This replaces the WIP's flat, doubling store, which still copied O(n) entries on every growth.
- Entries live in 256-entry chunks that every directory and snapshot covering them share. Slots at or after every published end are written in place.
- A roll appends one entry. A retention drop advances the front (`drainRetentionDeletes` and `deleteOldSegments`).
- Truncation, compaction install and reload call `publishSealedSnapshotFrom_(base)`. It rebuilds only the tail from the first changed entry, copy-on-writes that one chunk, and shares the prefix.
- A publish fills a header from a per-directory pool of 1024 and hands it out through an **aliasing `shared_ptr`**. That means **no allocation per publish**: one chunk is allocated per 256 rolls and one directory per 1024 publishes.
- The persisted recovery point now travels as one CLEANER-category atomic (`cleanerCompactionAllowedThrough`). This replaces the second O(n) republish per roll.

**Rename-loss bug, found by the gate.** It showed up as `FollowerTruncation`: EIO on the seal flush and a permanently failed log.
- A provisioned log could adopt segments faster than the provisioner finalized the rename ring (64 entries). `enqueueStagingRename` failed and the rename was silently dropped.
- The segment then stayed `pathPending` forever. Retention skips such segments, so 10k+ segments piled up, and `fopen` past fd 32767 failed on macOS.
- Fixes:
  - A full rename ring now defers the roll (`renameRoom_()` in `rollBlockedNow_`).
  - A forced roll never adopts without room in the ring.
  - `provisionReady` builds at most `depth` segments per call. The unbounded loop had starved the provisioner's own `finalizeStagingRenames` while the owner kept consuming.
  - `finalizeStagingRenames` consumes a request only when its completion fits in the done ring.

**Cold path vs in-flight rename.** `beginColdPath_` (truncateTo, reloadSegments, deleteOldSegments) now defers while a rename is queued, and applies completed renames first. It never re-opens a segment by its stale staging path.

**Bounded overshoot.** A deferred roll is forced (blocking, but correct) once the segment reaches `rollBytes + clamp(rollBytes, 1 MiB, 256 MiB)`. A stuck provisioner or io backend can therefore never grow a segment toward the 2 GiB position limit.

**Allocation-free owner roll.**
- `install_` reuses map nodes that retention, compaction or `erase_` released (`eraseKeepNode_` with a reserved spare vector of 64).
- The canonical path is composed in place (`setCanonicalLogPath`) into capacity that `prepareStaging` reserved off-thread.
- Measured: over 500+ steady-state rolls with retention, owner allocations stay within the rare chunk/directory bound.

**`BrokerNode::start`** now refuses to start if a provisioner is built but never wired to a runner. Otherwise every roll would wedge.

**Allowlist (`check_no_atomics` OK).**
- The WIP's ready-ring head/tail replace the old `preparedSegment_`/`preprovInFlight_` CAS hand-off, in the same UNSCOPED "rename ring / pre-provisioning hand-off → slice B SpscRing" bucket.
- `cleanerPersistedRp_` is CLEANER.
- `check_single_driver_pumper` OK.

## Tests (RED-first; every name contains `Regression`)

`libs/storage/tests/storage/SegmentLifecycleHotpathTest.cpp`, `SegmentLifecycleRegression.*` (11 tests):

| Test | What it pins |
|---|---|
| `RollNeverPreallocatesOnOwnerRegression` | F1 |
| `ProvisionedRollWithEmptyPoolDefersNeverCreatesInlineRegression` | F2 |
| `SealQueueFullDefersNeverFsyncsInlineRegression` | F3 |
| `RollAndRetentionPublishSnapshotInConstantTimeRegression` | F4 |
| `TruncationRebuildsOnlyTheSnapshotTailRegression` | F4 |
| `SnapshotMatchesSegmentsUnderRandomOpsRegression` | F4 |
| `DeferredRollOvershootIsBoundedRegression` | overshoot bound |
| `TruncateAfterProvisionerRenameKeepsLogHealthyRegression` | rename race |
| `TruncateDefersWhileStagingRenameQueuedRegression` | rename race |
| `FullRenameRingDefersRollNeverDropsRenameRegression` | rename loss |
| `SteadyStateRollAllocatesNothingOnOwnerRegression` | owner allocation |

How each was shown RED:
- **F1-F4:** red with the old behaviour restored (owner fallocate, inline create, inline seal fsync on overflow, O(n) publish).
- **Rename and overshoot tests:** red against the WIP.
- **Allocation test:** red at 1505 allocations over 500 rolls.

`WorkerProducePathTest.cpp`: `WorkerProduceRegression.DeferredRollHoldsIngressFrameRegression` was red with the hold disabled.

**ctest (build-fast, Debug, macOS, -j8):** 3920/3920 passed (735 s; 8 skipped/disabled as before). The Release latency gate passes 17/17 (within the known-failure ceilings).

## Before / after: Release duty-pass latency gate (µs, best attempt, p99.9 / max)

Linux arm64 Release, gcc-13, `rune-latgate-builder:a940b8b` image, Docker Desktop VM. Every run was taken with `docker ps` = 0 other containers (monitored every 5 s).

- **Before** = `e09b2f7` (the gate's base, WIP excluded).
- **After** = this branch.
- **ext4** = Docker volume (the gate report's setup). **tmpfs** isolates the code from the VM's ext4/virtio write stalls.

| Scenario | Before ext4 | After ext4 (best p99.9 / best max over 2 runs) | Before tmpfs | After tmpfs |
|---|---|---|---|---|
| SegmentRoll/1p | 798 / 13,926 | 524 / 1,362 | 249 / 16,386 | **23 / 258** |
| SegmentRoll/8p | 5,570 / 22,735 | 1,224 / 2,001 | 596 / 5,799 | 146 / 273 |
| Retention time+size/2p | 3,769 / 6,481 | 780 / 1,075 | 61 / 194 | **28 / 214** |
| Compaction 1 MiB/1p | 121 / 414 | **14 / 101** | 99 / 274 | **12 / 22** ✅ |
| Compaction 16 MiB/1p | 21 / 246 | 17 / 258 | 12 / 2,491 | **14 / 33** ✅ |
| Compaction 64 MiB/1p | 17 / 1,455 | 15 / 843 | 11 / 26,697 | **13 / 43** ✅ |
| Compaction 1 MiB/4p | 348 / 866 | 66 / 200 | 214 / 526 | **55 / 65** |
| FollowerTruncation/cleanerPass/2p | 3,358 / 8,216 | 205 / 876 | 462 / 691 | **17 / 197** |
| Reads+SegmentRoll/1p | 2,743 / 4,292 | 244 / 737 | 255 / 284 | **17 / 189** |
| Reads+Retention/2p | 3,575 / 5,305 | 745 / 1,213 | 53 / 212 | **23 / 200** |
| Reads+Compaction 16 MiB/1p | 64 / 211 | 60 / 97 | 64 / 4,520 | 63 / 94 |
| LeadershipChange/1p | 26 / 361 | **12 / 57** ✅ | 24 / 284 | **12 / 67** ✅ |
| LeadershipChange/8p | 366 / 1,553 | 68 / 193 | 204 / 870 | **33 / 58** ✅ |
| TxnMarkers/1p | 34 / 400 | 18 / 257 | 10 / 457 | **6 / 22** ✅ |
| TxnMarkers/4p | 58 / 948 | 50 / 255 | 16 / 791 | **16 / 39** ✅ |
| SteadyProduce/1p (control: no roll) | 16 / 247 | 10 / 21 | 4 / 22 | 3 / 11 |
| SteadyProduce/8p (control: no roll) | 133 / 1,934 | 98 / 230 | 15 / 46 | 16 / 38 |

✅ = meets the hot budget (max ≤ 100 µs, p99.9 ≤ 50 µs). On tmpfs, 9 of 17 scenarios meet it. The gate prints "now MEETS the budget" for them. The `kKnownFailures` table was left unchanged; nothing was loosened.

**Attribution after the change.**
- The `syncCreates` (F2) and `inlineFsyncs` (F3) counters are **0 in every scenario** (before: up to 4,366 sync creates and 2,289 inline fsyncs per run).
- With temporary timing probes (removed before commit), the roll itself, measured over ~5k rolls, splits as: seal ≤ 6 µs, adopt ≤ 10 µs, RP ≤ 0.3 µs, snapshot ≤ 6 µs, **total roll ≤ 10 µs**.
- The worker part of every remaining bad pass is 1-8 µs. The remaining max is **all intake `pwritev`**.

## Remaining red scenarios and what dominates them

**F6 (append `pwrite` stalls in the Docker VM, not the roll path) dominates every remaining red scenario.**
- **On ext4:** the first `pwritev` into a freshly adopted segment costs 0.5-3.7 ms. Its blocks are reserved with `FALLOC_FL_KEEP_SIZE`, so the first writes convert unwritten extents and extend `i_size` through the ext4 journal.
- **On tmpfs:** the residual 150-270 µs maxima are plain `pwritev` stalls. A standalone C++ probe in the same container shows plain `pwritev` reaching **max 238-727 µs on tmpfs with no background thread at all**, and up to 240 µs with no roll.

This is VM noise on the hypervisor. It must be confirmed on bare metal (bench box), as F6 already says.

Scenario by scenario:
- **SegmentRoll/1p, /8p, Retention, Reads+Retention, FollowerTruncation, Reads+SegmentRoll:** red on max because of the ext4 first-write-into-a-new-segment and VM `pwritev` stalls (F6).
  - Candidate follow-up for F6, other branch: have the provisioner pre-write or zero (not just fallocate) the first page(s), or use `fallocate` without KEEP_SIZE plus a logical-size header.
  - SegmentRoll/8p p99.9 on tmpfs (146 µs) is the same intake stall, with 8 partitions per pass.
- **Reads+Compaction:** F5 (PushRead copy), not in scope.
- **SteadyProduce/8p:** F6 by definition.
- **F7** (role-change fsyncs, COLD) is unchanged and is another branch's job.

## Risks / notes

**Back-pressure semantics.** While a roll is deferred, the produce frames of that partition stay in the Aeron image, and so do later frames from the same image.
- Other partitions on other images are unaffected.
- The provisioner's refill latency (≤ 100 µs park cap) bounds the hold.
- Follower apply and marker appends overshoot instead. They are bounded by the forced-roll cap, which blocks but is correct.

**Durability is unchanged.**
- The seal is still fsynced exactly once, before anything depends on it: async through the io backend, and Strict acks wait on pending seals.
- A forced roll with the queue full fsyncs inline (the old behaviour).
- Crash recovery of adopted-but-unrenamed staging files is unchanged (`recoverStagingFiles`). The new ordering only *prevents* lost renames.

**Allocation.** The owner roll path is allocation-free except:
- one 12 KiB chunk per 256 rolls;
- one directory (O(segments/256) pointer copies) per 1024 publishes;
- a map node when no released node is available. This only happens without retention, i.e. a growing log.

**Snapshot lifetime.** A `SealedSegmentSnapshot` is valid only through the `shared_ptr` it came from. This is the same contract as before; copying the object out by value is no longer a deep copy.

**Disk reservation.** Each provisioned partition reserves (depth + 1) × `segmentRollBytes` (KEEP_SIZE).

**Stale-data recovery.** Not changed beyond the rename ordering. A full DR run was not part of this task.

**Still to do.**
- Convert the ready ring to the slice-B `SpscRing` (UNSCOPED allowlist entries).
- Bare-metal confirmation of the residual intake stalls (F6).
- Tighten `kKnownFailures` once the bench-box numbers exist.
