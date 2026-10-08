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

---

## Review fixes (2026-10-06 review: APPROVE-WITH-CHANGES, 0 critical / 4 major / 6 medium / 8 low)

The review is at `docs/superpowers/briefs/2026-10-06-segment-lifecycle-hotpath-review.md`.
- **Commits:** `30f4385` (storage), `770091a` (replication), `32f2f2d` (node), `cf49200` (coverage test + backlog D20).
- **Lows:** filed as backlog D19 (one line each). L1 (cap clamp) and L5 (depth clamp) were fixed along the way. D20 is a new pre-existing finding (below).

### How each major / medium finding was fixed

**M1: a provisioner failure silently wedged produce.**
- A failed build (`prepareStaging` throws) or the node-wide cap now posts a FAILURE record on the log's ready ring. This is an event; there is no timer.
- The owner pulls it in `rollBlockedNow_` / `installNextSegment`. That ends the hold, and the roll happens INLINE once: blocking, correct, counted in `failureInlineRolls`, logged with a power-of-two rate-limited warn.
- If the inline create fails as well, the log fails exactly like the old inline-create failure (E1 fail-stop).
- A demand the provisioner's ring refused is re-posted by the owner. A demand that arrives before the log is in the provisioner's view is parked and served when the view arrives.
- Every Threshold demand therefore ends in a ready segment or a failure, so a hold is never unbounded or silent.
- Tests:
  - `ProvisionerFailureRollsInlineNeverHangsRegression`
  - `ProvisionerFailureThenInlineFailureFailsTheLogRegression`
  - `LostDemandIsRepostedRegression`
  - `SegmentProvisionerRegression.DemandBeforeItsViewIsServedWhenTheViewArrivesRegression`

**M2: the hand-rolled ready ring was removed.**
- The ready pool is now `rune::consensus::ControlRing<512>`, the agrona ManyToOneRingBuffer copy that is already RING-allowlisted. `rune_storage` adds the consensus include path and links `aeron::aeron` (header-only ring).
- `ControlRing` gained `pendingBytes()` and `watchInfo()`.
- No new atomics. UNSCOPED allowlist sites (checker count): 32 at `integ/perf-defaults`, 37 at `0ada704`, now **27**. Allowlist UNSCOPED lines: 35 at `0ada704`, now 25.
- `cleanerPersistedRp_` stays under CLEANER, as the review ruled.
- `check_no_atomics` OK.

**M3: provisioning is DEMAND-DRIVEN and bounded.**
- A provisioned log posts a Threshold demand when its active segment crosses `segmentProvisionThresholdPct` (default 50, `RUNE_SEGMENT_PROVISION_THRESHOLD_PCT`). The provisioner then builds up to `segmentProvisionDepth` ready segments: default **1**, clamp [1,8].
- The Adopted demand only finalizes the rename.
- **Idle partitions reserve nothing.**
- Node-wide cap: `segmentProvisionCapBytes`, default 16 GiB, `RUNE_SEGMENT_PROVISION_CAP_BYTES`, 0 = none. When it is exceeded the provisioner reports a failure, so the roll goes inline instead of being held.
- `FdBudget` counts `depth × kFdsPerSegment` per user partition.
- The 100 ms rescan of every log is gone.
- Tests:
  - `ProvisioningIsDemandDrivenByFillThresholdRegression`
  - `SegmentProvisionerRegression.NodeWideReservationCapReportsAFailureNotAHoldRegression`
  - `FdBudgetRegression.EstimateCountsProvisionerReadySegmentsRegression`

**M4: the provisioner protocol is now pinned by tests.** `SegmentProvisionerRegression.*` in `ShardNonBlockingTest.cpp`:
- `RetiredReplicaIsFreedOnlyAfterTheProvisionerAckedRegression`: the min over the cleaner view AND the provisioner view. It went RED when the provisioner was dropped from the min.
- `DemandForALogNotInTheViewIsNeverDereferencedRegression`: a bogus pointer is never touched.
- `DemandBeforeItsViewIsServedWhenTheViewArrivesRegression`.
- `NodeWideReservationCap…`.
- `ADemandWakesAParkedProvisionerRegression`.
- `BrokerNodeRefusesToStartWithAnUnwiredProvisionerRegression`: via `requireProvisionerRunner`, which `BrokerNode::start` now calls.
- These are deterministic interleavings. A TSan build (`build-tsan`) of `rune_storage_tests` and `rune_shard_nonblocking_tests` was also run: `SegmentLifecycleRegression.*` 18/18 and `SegmentProvisionerRegression.*` + `ReplicaLifetimeRegression.CleanerUsesItsOwnRingViewAndGatesTheFree` 7/7, with **0 ThreadSanitizer warnings**.

**m1: the hold is image-wide.**
- A frame that would be refused anyway (not the leader, oversize) is now answered at once and never held.
- The image-wide back-pressure contract is documented at the hold site.
- Tests:
  - `WorkerProduceFixture.HeldFrameAbortsOnlyItsImageAndIsRedeliveredRegression` drives a REAL controlled poll with two publishers: the held image stops, the other image's frame is acked, and after a refill the held frames are acked in order with no loss or duplicate.
  - `WorkerProduceRegression.RefusedFrameIsAnsweredNotHeldWhileRollDeferredRegression` (RED with the old ordering).

**m2: follower / marker / op-ring overshoot.**
- A follower whose log cannot roll asks for no data: the periodic fetch carries `maxBytes = 0`, a keepalive that keeps ISR liveness. Test: `FollowerRollBackPressureRegression.FollowerStopsFetchingDataWhileRollDeferredRegression` (RED: data fetches continued).
- The index is sized for the deferred-roll cap, so overshoot never grows the index mapping on the owner.
- Txn markers and op-ring produces can still overshoot, bounded by the cap. They are rare, and the cap forces the roll.

**m3: per-log snapshot memory.** Chunk is 64 entries and the header pool is 64: about 4 KiB per log, down from about 34 KiB.

**m4: stale data.**
- `CrashWithReadyAndQueuedRenamesRecoversAllDataRegression` covers ≥2 ready plus 3 unrenamed adopted segments, then a simulated crash.
  - `crashForTest` now leaves renames and ready files exactly as a real crash would.
  - It recovers every record and base, and leaves no staging file behind.
- Staging recovery now trusts a base only from a complete, CRC-valid first batch, and never overwrites an existing segment. Anything else is quarantined as `.stlog.corrupt`. Test: `TornStagingFileIsQuarantinedNeverOverwritesRegression` (RED: the existing segment was overwritten).

**m5: provisioner idle CPU.**
- The demand ring rings a `RingDoorbell`. BrokerNode parks the provisioner with `ParkUntilWorkIdle` on that doorbell, with a 10 ms cap, `segmentProvisionerMaxParkUs`.
- Idle wakeups go from about 10k/s to about 100/s.
- Test: `ADemandWakesAParkedProvisionerRegression`, with a 2 s park period: the demand is served in under 1 s.

**m6: cold paths deferred on any queued rename.**
- `beginColdPath_(touchFrom)` now defers only for queued renames of segments the cold path touches.
- `deleteOldSegments` stops at a `pathPending` segment instead of deferring.
- Test: `QueuedTailRenameDoesNotBlockRetentionRegression` (RED: retention waited for the tail's rename).

**RED-first.** Every behavioural fix was shown RED by temporarily reverting it: M1, M3, m1, m4, m6, the M4 view-ack gating, and the m2 follower test. The crash-recovery test is coverage: it was GREEN before as well.

### Release latency gate: before (`0ada704`) vs after (review fixes `32f2f2d`)

Same image and builder as above, run back-to-back with `docker ps` = 0 for every run. Values are p99.9 / max in µs, best attempt.

**holds** = frames held because a roll was deferred, i.e. a roll arrived before its ready segment. This counter is new in the rig, so it has no before value.

| Scenario | ext4 before | ext4 after | tmpfs before | tmpfs after | holds (after, tmpfs) |
|---|---|---|---|---|---|
| SteadyProduce/1p | 6.7 / 195 | 8.1 / 169 | 4.6 / 76 | 6.6 / 89 | 0 |
| SteadyProduce/8p | 192 / 1,360 | 125 / 236 | 14.2 / 38 | 11.7 / 39 | 0 |
| SegmentRoll/1p (4 KiB rolls) | 962 / 4,903 | 785 / 946 | 22.2 / 124 | 16.7 / 235 | 19,996 |
| SegmentRoll/8p (4 KiB) | 1,812 / 2,604 | 1,447 / 3,596 | 127 / 179 | 115 / 228 | 39,968 |
| Retention/2p (8 KiB) | 1,020 / 1,760 | 1,359 / 2,640 | 25.1 / 233 | 25.0 / 243 | 39,832 |
| Compaction 1 MiB/1p | 14.7 / 143 | 14.3 / 189 | 13.2 / 19 | 12.9 / 27 | 0 |
| Compaction 16 MiB/1p | 16.8 / 248 | 18.0 / 287 | 14.6 / 28 | 13.8 / 25 | 0 |
| Compaction 64 MiB/1p | 15.6 / 860 | 16.3 / 594 | 12.0 / 42 | 13.1 / 33 | 0 |
| Compaction 1 MiB/4p | 77.2 / 254 | 62.0 / 292 | 43.4 / 62 | 33.7 / 63 | 0 |
| FollowerTruncation/2p | 144 / 275 | 149 / 1,640 | 15.7 / 71* | 15.8 / 228* | 329,638 |
| Reads+SegmentRoll/1p | 438 / 2,026 | 811 / 5,046 | 16.5 / 181 | 18.2 / 262 | 19,996 |
| Reads+Retention/2p | 842 / 1,422 | 1,774 / 2,415 | 22.8 / 119 | 21.2 / 143 | 25,527 |
| Reads+Compaction 16 MiB/1p | 64.8 / 100 | 61.6 / 207 | 62.5 / 85 | 61.8 / 83 | 0 |
| LeadershipChange/1p | 13.2 / 49 | 17.2 / 50 | 10.6 / 33 | 11.7 / 42 | 0 |
| LeadershipChange/8p | 73.8 / 252 | 70.1 / 214 | 32.4 / 72 | 31.4 / 157 | 0 |
| TxnMarkers/1p | 25.2 / 68 | 28.2 / 323 | 5.7 / 25 | 6.7 / 20 | 0 |
| TxnMarkers/4p | 50.0 / 101 | 37.7 / 183 | 13.5 / 27 | 13.5 / 25 | 0 |

\* FollowerTruncation on tmpfs fails its non-vacuity precondition (truncations ≥ 3) **in both the before and the after run**. See D20 below.

**Reading the table.**
- On tmpfs the code path is unchanged within noise (±1 run).
- ext4 maxima in this Docker VM swing by 2-5× between runs (F6 `pwrite` stalls; compare the two `0ada704` ext4 runs in the first table above).
- No scenario regressed beyond that noise. The worker part of every bad pass is still 1-30 µs, and all of the excess is intake `pwritev` (F6).
- **Holds at the default threshold:** zero in every scenario with realistic roll sizes (≥ 1 MiB: Compaction, LeadershipChange, TxnMarkers).
- They occur only in the synthetic 4-8 KiB-roll scenarios. There a partition rolls every ~4 passes (~50k rolls/s), above the provisioner's build rate. The same scenarios already held at `0ada704` with depth 2 and continuous refill (acks < passes there too). Each roll waits about 4 passes, and no frame is lost.
- `deferrals` and `failureInlineRolls` are 0 everywhere: no forced roll and no provisioning failure.
- Gate verdicts: 16/17 in every run. SteadyProduce/1p's single 170-195 µs `pwrite` (F6) fails before and after. FollowerTruncation on tmpfs fails as noted. On ext4, FollowerTruncation passes (12 truncations).

### New finding D20 (pre-existing, exposed by faster rolls)

On tmpfs, hot-path A rolls about 6× faster than `e09b2f7` did, which was throttled by inline creates. Partition B of the FollowerTruncation scenario (8 KiB rolls, 16 KiB retention) then reaches about 40k segments, because retention cannot keep up.

Diagnosis, from instrumented msg-ring positions and gdb stacks of the cleaner thread:
- The TruncatePending follower waits for the cleaner to perform 256 posted retention drops.
- Each drop destroys a `LogSegment` whose `std::fstream` close (`fclose` → glibc `_IO_un_link`) walks the process-wide list of about 40k open `FILE`s.
- Draining takes more than 1M owner passes, and the scenario's pass budget runs out.

This is not a wedge and not caused by the review fixes: it is identical at `0ada704`. The fix (fd-only segment writes with no `FILE*`, and/or a retention-throughput floor) is filed as backlog **D20**. `CompactionCleanerAgentRegression.TruncatePendingFollowerPostsNoNewCleanerWorkRegression` pins the interleaving that was ruled out along the way.

**Final verification (HEAD of this section's commit):** build-fast `ctest -j8` 3938/3938 passed (734 s); `check_no_atomics` OK.
