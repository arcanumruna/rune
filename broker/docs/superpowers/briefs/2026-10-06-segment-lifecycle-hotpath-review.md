# Review: segment-lifecycle hot path (F1-F4), branch `perf/segment-lifecycle-hotpath` @ 0ada704

- **Reviewed:** `git diff integ/perf-defaults...HEAD` (c693c24 WIP + e660223 + 48a35c2 + report 0ada704).
- **Inputs:** `2026-10-06-segment-lifecycle-hotpath-report.md` (implementer), `rune-broker-wt26/.../2026-10-05-latency-gate-report.md` (F1-F8).
- **Verified locally (build-fast, macOS Debug):**
  - `rune_storage_tests --gtest_filter=SegmentLifecycleRegression.*`: 11/11 pass.
  - `ctest -R "WorkerProduceRegression|Staging|Recovery|SealedSnapshot|Retention|LogCleaner|NoAtomics"`: 254/254 pass.
  - `scripts/check_no_atomics.py`: OK. UNSCOPED sites went from 32 to 37.
- **Not re-run:** the Release latency gate (no Docker per brief) and the full 3920-test suite.

## Verdict: APPROVE-WITH-CHANGES

The change fixes what it set out to fix:
- F1: fallocate is now done by the provisioner.
- F2: a roll never creates a segment inline on a provisioned log.
- F3: no inline seal fsync below the cap.
- F4: snapshot publish is O(1) on roll and retention, and a tail-only rebuild elsewhere.

**Durability ordering is preserved.**
- Recovery-point advance is still per-segment (`advanceRecoveryPoint_` stops at the first segment that is not fully durable or whose dir entry is not synced). A forced inline seal with older seals still pending cannot move R past them.
- Strict acks still wait on pending seals.
- Retention (`drainRetentionDeletes`) still skips segments that are flush-pinned or `pathPending`.
- Compaction install re-checks `compactionAllowedThrough` and `pathPending`, so the new advisory `cleanerPersistedRp_` read cannot make the owner install unsafe output.

**The chunked snapshot is correct.**
- I walked through append, drop-front, `divergeAt` (partial copy-on-write of the chunk), the trailing-removal path and both rebase paths.
- Published slots are never rewritten. Every consumer holds the `shared_ptr` (LogCleanerAgent.h:290/331/743), so the aliasing lifetime is sound.
- A randomized-ops regression test covers it.

**No critical findings.** Four majors must be addressed or explicitly ruled on before merge.

| Severity | Count |
|---|---|
| Critical | 0 |
| Major | 4 |
| Medium | 6 |
| Low | 8 |

---

## MAJOR

### M1. A provisioner build failure is silent and wedges produce on the partition forever, plus head-of-line blocking on the image

**Where:**
- `Log::provisionReady`, Log.cpp:543-547: `catch (...)` → remove the staging files → `break`. No log, no counter, no signal to the owner.
- `Worker::handleWorkerIngressFrame_`, Worker.h:5392: the hold.

**Failure scenario:**
1. `LogSegment::prepareStaging` throws in the provisioner. Causes: EMFILE (see M3: FdBudget does not count the ready segments), ENOMEM / `vm.max_map_count` on the index mmap or read map, EACCES / EROFS after a remount, or a dead disk.
2. The provisioned log's ready pool stays empty.
3. Once the active segment reaches `rollBytes`, `rollDeferred()` is true for good.
4. Every worker-ingress produce for that partition is HELD before it is appended, so the active segment never grows toward `maxDeferredRollBytes_()`. The forced-roll backstop is reachable only by follower, marker or op-ring appends. A leader fed only by worker-ingress producers never reaches it.

**Result:**
- The partition stops accepting produces with no error. Clients time out instead of getting a storage error.
- The broker does not fail-stop. Before this change, the inline create threw, `markStorageFailed` fired, and the log dir went offline (Kafka semantics; the E1 review comment at Log.cpp `maybeRoll` still documents this).
- Because the hold is a controlled-poll ABORT, **every later frame on the same Aeron image is stuck behind it**: all partitions that publisher sends to this worker, not just the failed one.
- The 100 ms rescan retries forever, silently.

**Fix (time-independent, event-based, never a timeout):**
- The provisioner reports a failed build to the owner over a ring: a sticky per-log `provisionFailedErrno`, published through the existing rename-done ring or a new one-slot ring.
- On that event the owner either forces the roll inline (correct and blocking, as the cap does today) or applies the fail-stop `markStorageFailed`, whichever the E1 policy says.
- In both cases: rate-limited log, a counter, and an FR site.
- **Regression test:** a log whose provisioner build fails (inject a `prepareStaging` fault) must produce an error or a forced roll, never an indefinite hold.

### M2. A new hand-rolled atomic SPSC ("ready ring") is added under UNSCOPED, which breaks the shared-nothing law

**Where:**
- Log.h:1171-1175: `readyRing_`, `readyHead_`, `readyTail_`.
- Log.h `readyAvailable` / `readyCountForTest`.
- Log.cpp `provisionReady` / `takeReady_`.
- `scripts/atomics_allowlist.txt` +10 UNSCOPED lines (net UNSCOPED 32 → 37).

**What is fine:**
- The ring itself is correct: producer slot write → release tail; consumer acquire tail → release head; capacity 8 ≥ clamped depth.
- It removes a CAS (`preprovInFlight_`), which is an improvement over the WIP.

**What is not:**
- UNSCOPED is defined in the allowlist header as "TEMPORARY … must be converted". The law permits atomics only inside the project's ring implementations (RING) and the cleaner channel (CLEANER).
- Adding a *new* ad-hoc ring into the debt bucket grows the debt the slice-B work must retire, and sets a precedent: "a hand-rolled ring is fine if filed under UNSCOPED".
- Layering is a real obstacle: `libs/storage` cannot use `rune::node::detail::O2ORingBuffer`. It can use the Aeron `OneToOneRingBuffer` (already a broker dependency, and "Aeron is the bible"), or a templated SPSC moved into a lib below storage.

**Ruling:**
- Convert the ready ring in this branch to an existing ring type: the Aeron O2O carrying the 8-byte `LogSegment*` (the provisioner side is cold, so the scratch copy does not matter), or the shared SPSC the slice-B plan names.
- Alternatively, get an explicit user ruling to keep it as tracked UNSCOPED debt, with a backlog item ID in the allowlist reason.

**`cleanerPersistedRp_` (CLEANER) is ACCEPTABLE.**
- It is a single-writer owner→cleaner value used only for the cleaner's advisory compaction gate, so it is squarely inside the log-cleaner/compaction exception.
- The owner re-validates at install (Log.cpp:1890), and it deletes one `atomic_store` / `atomic_load` pair per roll.

### M3. Per-partition resource reservation does not scale, and the startup check under-reports it (FREE-tier multi-tenant box)

**Where:**
- NodeConfig.h:825 (`segmentProvisionDepth = 2`).
- NodeConfig.cpp:1010 (clamp **[2, 8]**: depth 1 cannot be configured even though the agent accepts 1).
- `Log::provisionReady` (`preallocate(rollBytes_)` on every ready segment).
- `LogSegment::prepareStaging` (index created with `prefault=true`, plus `openReadMap`).
- `FdBudget.cpp:13-28` (no term for ready segments).

**Per partition, idle or not, with the defaults (64 MiB roll):**

| Resource | Before | After |
|---|---|---|
| fallocated blocks | 64 MiB (active only; the single prepared staging segment was not fallocated) | **(depth+1) × rollBytes = 192 MiB**. 1,000 partitions = **192 GB** reserved with zero data; with Kafka-default 1 GiB rolls, 3 GiB per partition |
| prefaulted index of each ready segment | 1 prepared segment | 2 ready segments |
| fds | unchanged base | +2 × `kFdsPerSegment` (4) = **+8 fds**, which `FdBudget` does not count |
| mmaps | unchanged base | +4 (index + read map × 2) |

Note on the index prefault: `MappedFile` prefault does a *write*-touch (`data_[off] = data_[off]`). Each touched page becomes **dirty**: resident page cache plus writeback that allocates blocks. That is 256 KiB per ready segment at 64 MiB rolls, up to 8 MiB at ≥ 2 GiB rolls.

**FREE-tier impact** (multi-tenant 4-core box, "max clients per box at SLO"):
- Disk is consumed by empty reservations long before data, and that capacity cannot be sold.
- The startup NOFILE check passes while the provisioner later hits EMFILE, which leads straight into M1's silent wedge.
- At about 10k partitions, ready maps alone add about 40k to the default `vm.max_map_count` of 65,530.

**Fix (pick and benchmark):**
- fallocate only the *head* of the ready ring (the next segment to be adopted), or only a bounded prefix (for example the first 4-8 MiB, which also attacks F6's first-write cost).
- Allow depth 1 (fix the NodeConfig clamp), and make depth adaptive per partition: 1 for idle partitions, 2+ after back-to-back rolls.
- Add the ready segments to `FdBudget::fdsPerPartition` (and the map count to its diagnostics).
- Prefault the staging index with a read-touch or `MADV_WILLNEED` instead of dirtying it. This needs measuring, because it trades away part of the F2 win.
- Document the reservation formula in the hosting-tier capacity model.

### M4. The provisioner's memory-safety protocol has no regression test of its own

**Where:**
- `SegmentProvisionerAgent.h` (new; used only by `DutyPassLatencyGateTest`, which is a perf gate skipped under sanitizers).
- `NodeRuntimeAgent.h` `views_` / `publishView_` (the safe generation is now the min over the cleaner view *and* the provisioner view).

**Why it matters:**
- The demand ring carries raw `Log*` values.
- A retired replica's `Log` may be freed only after the provisioner acks a view without it. If the `min` over `views_` regresses, or the ack precedes the `logs_` swap, the result is a UAF on the provisioner thread (`serve_` → `finalizeStagingRenames` on a freed `Log`).
- Nothing pins:
  - "a demand for a removed log is ignored";
  - "the registry keeps a retired replica until `provisionerAckedGen >= removalGen`";
  - "the rescan recovers a demand lost to a full ring";
  - "a new partition gets its first ready segments from the view";
  - `BrokerNode::start` refusing to start without a runner (48a35c2 added the guard, but no test).

**Fix:** add `SegmentProvisionerRegression.*` tests (fast, in-process) for each of the above, ideally including a TSan run of the retire path. Mark each as Regression.

Severity: the protocol is a copy of the cleaner's tested one, so this could arguably be medium. It stays major because it is the only UAF guard for a new thread that dereferences raw `Log*` from a ring.

---

## MEDIUM

### m1. The hold is image-wide, not per-partition (Worker.h:5392, 5311-5330)

- A held frame ABORTs its image, so one partition with a deferred roll stalls **every** partition that the same publisher sends to this worker. Other images are unaffected.
- The hold check also runs *before* the leadership/ownership refusal in `produceCore_`. A stale-leader produce is held instead of being answered `NOT_LEADER` immediately.
- The duration is bounded by provisioner refill (≤ 100 µs park + build, which is ms on slow disks), and by seal-queue drain for F3.
- Document this as a back-pressure contract. Consider moving the hold after the cheap refusals (not leader, not owned, oversize).
- No test runs this through a real `controlledPoll` (ABORT, then re-delivery, then other images keep flowing). The unit test calls `handleIngressFrameForTest` directly.

### m2. F2/F3 are fixed only for worker-ingress produce; follower apply, txn markers and op-ring produces still block at the cap (Log.cpp `maybeRoll`, Log.h:1183)

- Those appends are never held. They overshoot to `rollBytes + clamp(rollBytes, 1 MiB, 256 MiB)` (2× for 64 MiB rolls) and then **force** an inline create or an inline seal fsync on the worker: the exact F2/F3 cost, now delayed rather than removed.
- A follower on a slow disk, with the seal queue full during catch-up, will hit it.
- Overshoot to 2× also consumes the entire 2× headroom of `indexMaxBytesFor`. Dense-index workloads then grow the index mapping on the owner. Read-map remaps past `kReadMapMinBytes` are also more frequent.
- Follow-up: back-pressure the follower fetch (do not apply or fetch while `rollDeferred()`), as the intake does, and size the index for the cap rather than for `rollBytes`.

### m3. Snapshot memory per log (Log.h:65-70, Log.cpp `snapRebaseDir_`)

- Every log with ≥ 1 sealed segment holds a directory with a **1024-header pool (24 KiB)** plus at least one 256-entry chunk (10 KiB). That is about 34 KiB per partition: 340 MB at 10k partitions, versus about 50 B per segment before.
- The header pool only amortizes allocation. 64 headers would still be one allocation per 64 rolls, at 1/16 of the footprint.
- Make `kSnapHeadersPerDir` small (or adaptive), and measure.

### m4. No stale-data crash-recovery test at depth ≥ 2 with queued renames

- `recoverStagingFiles` (Log.cpp:706-745) now routinely meets several never-written ready files plus up to 64 adopted-but-unrenamed files with data.
- No test crashes (or simulates a crash, by skipping `~Log`'s rename drain) with ≥ 2 ready segments plus ≥ 2 queued renames, then checks every segment, offsets and the recovery point.
- Required by must-work-with-stale-data.
- Pre-existing fragility worth pinning in the same test: a staging file whose first header is torn but non-empty has its base parsed from garbage. `std::filesystem::rename` then **overwrites** whatever `<base>.log` exists.

### m5. The provisioner idles at a 100 µs park cap (BrokerNode.cpp `BackoffIdleStrategy(10, 20, 1us, 100us)`)

- That is about 10k wakeups per second per broker, forever, plus a 100 ms walk of every log.
- This is not a busy spin, so the never-busy-spin rule is met. On a FREE-tier 4-core multi-tenant box it is per-broker idle CPU and context-switch cost.
- The demand ring could ring a doorbell, as the A9 `RingDoorbell` pattern does, so the thread parks long when idle and wakes on demand. That would also tighten the hold bound.

### m6. Cold paths defer while *any* rename is queued (Log.cpp:2108-2121)

- `truncateTo`, `reloadSegments` and `deleteOldSegments` return "retry" whenever `renameReqHead_ != renameReqTail_`.
- With tiny segments and a sustained high roll rate, a rename can be queued at nearly every check. Retention (`deleteOldSegments`) can then be starved for long stretches.
- Truncation is safe, because a TruncatePending follower stops appending.
- It is correct and progress resumes once the queue drains. Prefer deferring only when the cold path actually touches a `pathPending` segment.

---

## LOW

1. **Forced-roll cap can exceed the 32-bit position limit.** `maxDeferredRollBytes_()` (Log.h:1183) = `rollBytes + min(rollBytes, 256 MiB)`. `segmentRollBytes` has no upper bound (NodeConfig.cpp:911), so for `rollBytes` > about 1.75 GiB the cap passes 2 GiB. Clamp it to `kMaxSegmentPos - maxBatch`.
2. **Residual owner allocations.**
   - Logs without retention (compacted, or infinite retention) allocate one map node per roll (Log.cpp:1371 `emplace`), because nothing is ever released into `spareSegNodes_`.
   - Chunk (10 KiB per 256 rolls) and directory (per 1024 publishes) allocations are amortized but still on the owner.
   - Possible fix: the provisioner pre-builds a `SegMap::node_type` alongside each ready segment (node handles move between maps that share an allocator).
3. **Indirect call on the roll path.** `ProvisionHook` is a function-pointer call on the worker roll path, once per roll. That is acceptable at roll frequency, but it is an indirect call the no-virtual rule's spirit covers. A concrete `SegmentProvisionerAgent*` would de-virtualize it.
4. **Mutex-pool lock on the owner roll (pre-existing, CLEANER-allowlisted).** `std::atomic_store(&sealedSnapshot_, …)` (Log.cpp:379) is a libstdc++ mutex-pool lock on the owner, contended with the cleaner's `atomic_load`. The branch halves its frequency (one publish per roll). Track it for the cleaner-channel ring conversion.
5. **Depth clamp mismatch.** NodeConfig clamps to [2, 8]; `SegmentProvisionerAgent` clamps to [1, 8]. Pick one (see M3).
6. **Dead parameter.** `checkpointRecoveryPoint_(bool publish)` now ignores `publish` (`(void)publish;`). Remove the parameter.
7. **Test seams in the public header.** `SegmentLifecycleRegression` tests use `*ForTest` accessors (`ownerPreallocsForTest`, `forcedRollsForTest`, …) that are public in `Log.h`. Consider a friend test-peer.
8. **No end-to-end check of the start refusal.** `BrokerNode::start` throws only if `provisionerAgent_ && !provisionerRunner_`. The runner is built only inside `if (replicaRegistry_) … if (provisionerAgent_ && runtimeAgent_)`. That is fine today; a test (M4) should pin it.

---

## Checked and OK

- **Durability:**
  - Seal fsync happens exactly once (async, or inline when forced).
  - R and the checkpoint never pass a pending seal or an unsynced dir entry. The rename after a dir fsync is recoverable through `recoverStagingFiles`.
  - Retention skips pinned and `pathPending` segments.
  - Compaction rejects `RenamePending` and the RecoveryPoint.
- **Deadlock:**
  - Seals drain through the worker's io backend every pass (`submitBackgroundDiskWork_` / `reapDurableCompletions_`).
  - The rename-done ring is drained every pass (Worker.h:1444/1516).
  - `finalizeStagingRenames` consumes a request only when there is room for its completion, and `renameRoom_` gates adoption.
  - No cycle exists.
- **Lifecycle:**
  - The provisioner is pinned to `nonWorkerCpus` (ScopedThreadAffinity scope).
  - It is started after the cleaner and stopped after the workers are joined. `~Log` frees unconsumed ready segments once the provisioner has stopped.
  - A demand for a log not in the view is never dereferenced.
- **Snapshot:**
  - Copy-on-write (COW) invariants hold.
  - The drop-front fallback goes to a full rebuild.
  - `drainSwapSlots_` rebuilds from the lowest affected base.
  - Truncation rebuilds from the new active base.
  - Consumers hold the `shared_ptr`.
- **Hot path:**
  - `rollDeferred()` is O(1): map `rbegin` plus a size compare, and an acquire load only when the segment is full.
  - No file I/O, fallocate or fsync on the owner below the cap.
  - The canonical path is composed in place into reserved capacity.
- **Tests:**
  - All 11 `SegmentLifecycleRegression.*` tests and `WorkerProduceRegression.DeferredRollHoldsIngressFrameRegression` pass locally.
  - Names carry `Regression`.
  - I did not re-verify the RED-first claims.
