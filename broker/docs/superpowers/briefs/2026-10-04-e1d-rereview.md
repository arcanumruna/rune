# E1d fix round 1: scoped re-review

- **Reviewed:** `rune-broker-wt32`, branch `fix/e1d-recovery-point`. Fix round 1 is `85953b1`, on top of `53df10d` and `005b94a`. It needs rune-protocol `6d14dc8`.
- **Scope:** F1–F8, regressions introduced by the round, and the hard rules. This review did not cover DUR-1 itself.
- **Mode:** read-only. Nothing in wt32 was edited.
- **Repro tests:** written in a scratch dir and linked against wt32 `build-fast` libs. The scratch file includes the in-tree `RecoveryPointTest.cpp` to reuse its helpers. It lives at `/private/tmp/claude-501/.../scratchpad/RereviewTest.cpp`.
  - Baseline: all 19 in-tree `RecoveryPointRegression` tests pass.

## Verdict: CHANGES REQUIRED (no new review round needed after fixing)

| Severity | Count |
|---|---|
| CRITICAL | 0 |
| MAJOR | 1 (confirmed by a failing repro) |
| MEDIUM | 3 |
| LOW | 5 |

The MAJOR finding is a hole in the F7 replay. It brings back the exact offset gap that E1d exists to prevent.

## F1–F8 status

| # | Status | Notes |
|---|---|---|
| F1 | **Fixed** | `prepareRecoveryCheckpoint` only touches memory. The loop issues exactly one io-backend submit: on uring, the existing `io_uring_enter`; on portable, a ring copy. There is no open, pwrite, rename or dir-open on the loop. Tests: StrictPlacement (one request, nothing moves at submit) and `CheckpointFile_PreallocatedOnce_NeverRecreated`. |
| F2 | **Fixed** | `persistedRp_` and the gate move only in `recoveryCheckpointCompleted` (Log.cpp:971–991), after an inline fdatasync, or after creation. Both gate tests were mutation-verified. |
| F3 | **Fixed** | Creation runs tmp, `fsync`, rename, then a full `::fsync` of the dir (Log.cpp:831–867). The deletions at recovery use a full dir `::fsync` (Log.cpp:753–760). **But** the per-checkpoint dir sync that existed in round 0 is gone, and nothing replaced its side effect. See MEDIUM-2. |
| F4 | **Fixed** | NodeRuntimeAgent.h:759–778. The test is RED on the old code. The empty-segment deviation is sound: `rewriteSegmentImpl` returns false only for `buf.empty()`. |
| F5 | **Fixed** | Both slots invalid, or the file present but unreadable, now gives `Corrupt`. That path does a CRC-only recovery with `deleteOnGap=false` and leaves the log failed; `refuseStartOnFailedRecovery_` then refuses to start. A missing file behaves as before. A mutation test covers it. |
| F6 | **Fixed for the states the Log still remembers** | `persistAtMost_` runs before any file is touched, and a failed persist returns early. Hole: when the in-flight value has been *forgotten* (MAJOR-1). Also LOW-2. |
| F7 | **Partially fixed; introduces MAJOR-1** | The checkpoint replay drops the possibly-landed value (MAJOR-1). The seal replay is skipped whenever any other seal of the log is still routed (MEDIUM-1). |
| F8 | **Fixed** | `rpQueue_` reserves capacity of `slots_.size()`, and the `rpQueued` flag stops duplicates. A refused submit stays queued (`keep`). The queue is cleared and refilled on rebuild, so purged logs are dropped. Inline-path due-ness is not queued (LOW-3). |

## Findings

### MAJOR-1: F7 reset forgets a checkpoint write that may have landed; a later truncation then skips the lowering persist, so the on-disk R ends up above the LEO (offset gap after power loss)

**Where:**
- `Log::resetRecoveryCheckpointForNewOwner` (Log.cpp:994–1001)
- `Log::persistAtMost_` (Log.cpp:1021–1030)
- the caller, `Worker::rebuildFlushSlots_` (Worker.h:4214)

**The mechanism:**
- `prepareRecoveryCheckpoint` targets slot X = 1 − active with `{seq s+1, V}`, while `persistedRp_ = P`.
- The write+fdatasync completes **successfully** while the log is unowned (`gate == null`), so `recoveryCheckpointCompleted` is never called.
- The new owner's rebuild calls `resetRecoveryCheckpointForNewOwner()`, which sets `rp_ = Idle` and **throws away `rp_.value = V`**.
- On disk, slot X now holds `{s+1, V}`, the highest seq in the file.
- `truncateTo(B)` with `P ≤ B < V` then calls `persistAtMost_(B)`:
  - `staleInProgress` is false, because the stage is Idle.
  - `persistedRp_` (P) is not above B.
  - So **no inline persist happens**, and files above B are deleted. Restart recovery would still read R = V.

**Failure scenario:**
1. A follower partition's checkpoint (V = 21) completes while the log is briefly unowned.
2. The log is re-owned, and a divergence truncation to 1 follows.
3. New divergent data is appended. Async seals roll segments, and those seals have not completed.
4. Power loss.
5. Recovery trusts every segment ending at or below 21 without a chain check. It accepts the empty or short re-appended segments as "compaction gaps", and the LEO claims offsets the log does not hold.

**Repros** (scratch; each fails on 85953b1):
- `Rereview.ResetForNewOwnerThenTruncate_DiskRNeverAboveLeo` fails with "on-disk R 14 is ABOVE the truncated LEO 8 (P=0)".
- `Rereview.ResetForNewOwnerThenTruncate_PowerLoss_NoOffsetGap` fails with "hole at offset 1 (LEO 21)".
- Control: `Rereview.Control_CompletionObserved_NoOffsetGap` is the same sequence with the completion observed. It passes.

**Fix direction:**
- Track an upper bound of what *may* be on disk: `rpDiskMax_ = max(persistedRp_, the value of every write whose outcome is unknown or abandoned-but-possibly-landed and whose slot has not been overwritten since)`.
  - `resetRecoveryCheckpointForNewOwner` raises it to `rp_.value`. It must not drop that value.
  - `persistAtMost_` compares the bound against `rpDiskMax_`.
  - Any write that durably overwrites that slot clears it.
- Add both repros in-tree as `RecoveryPointRegression`.

### MEDIUM-1: the F7 seal replay is skipped when *another* seal of the same log is still routed, so R and the cleaner gate stay pinned until restart

**Where:** Worker.h:4205–4215 (`if (!sealRouted) requeueUndurableSeals()`).

**Scenario:**
1. The log has two seals in flight (`sealsInFlight` > 1).
2. Seal A completes while the log is unowned, so its durability is never recorded.
3. The log is re-owned while seal B is still routed on this worker. `sealRouted` is true, so there is no requeue.
4. Segment A never becomes `fullyDurable()`. `advanceRecoveryPoint_` advances in order, so it stops at A permanently.

**Effect:**
- No checkpoint ever passes A, and the cleaner gate (`compactionAllowedThrough`) freezes. Compaction for that log stops until restart, and a compacted topic grows without bound.
- Every recovery chain-validates everything above A.
- This is not a data-loss bug (a stale, lower R is safe), but it wedges the log permanently.

**Fix:** always call `requeueUndurableSeals`, skipping only segments that have a Seal route in flight (match `rt.seg`).

**Test:** two seals in flight; complete one while unowned; re-own; assert that R reaches the end of both.

### MEDIUM-2: no directory durability for segment files that were created, renamed or deleted before R moves past them (round-0 side effect lost; Kafka-parity gap)

**Where:**
- `renameStagingToBase` (Log.cpp:403–412): rename, no dir fsync.
- The staging pre-provision: create, no dir fsync.
- The deletions in `truncateTo` (Log.cpp:1137–1143): no dir fsync.
- The slot checkpoint itself (fdatasync of the rp file only).

**Why it matters:**
- Round 0's async checkpoint ended with a directory fdatasync. That was the stated mitigation for the round-0 report's concern 4: "the checkpoint's dir fsync runs after the segment file is created/renamed".
- Round 1 removed every per-roll dir sync (correct for F1), and nothing replaced that ordering.
- Recovery trusts segments at or below R **without a chain check**. A segment whose directory entry never became durable therefore looks like a legitimate compaction gap: a silent offset hole below R.
- After a truncation, deleted divergent segment files can also reappear. If their names are reused at or after the persisted lowered R, they chain-validate.
- Kafka flushes the parent dir before the recovery point covers newly rolled segments (`LocalLog.flush`, `needsFlushParentDir`).

**Practical risk:** low on ext4 (data=ordered) and xfs, because a later fdatasync commits the earlier journal transactions in order. It is not POSIX-guaranteed, and it is exactly the bug class E1d targets.

**Fix direction:**
- Keep a dir fd opened once at recovery.
- When segments were created, renamed or deleted since the last checkpoint (a flag set on roll, rename-done and truncation), prepend `IORING_OP_FSYNC(dirfd)` to the linked chain, or add a portable dir-sync step to `WriteSyncRequest`.
- Alternatively, have the bg thread fsync the dir after `finalizeStagingRenames`, and gate R from crossing a base until that has completed.
- No syscall on the loop either way.
- Update the stale concern-4 text in the report.

### MEDIUM-3: the default Linux path (io_uring) has not run in-tree

**Where:** `UringDurabilityTest.WriteSyncLinkedPairRoundTripRegression`, plus the StrictPlacement and ShardNonBlocking E1d cases on the uring backend.

**What has run:** only `-fsyntax-only`, and a standalone harness in Docker. The worker → `submitWriteSync` → reap → `recoveryCheckpointCompleted` path has never run end to end on uring with the default config. Under the never-hand-over-untested-code rule, that is a gate.

**Required before merge:** run on k3s-node-01 (after `vm-clean`):
- `rune_storage_tests` and `rune_storage_integration_tests`
- the node integration suites, with the uring backend
- the DR suite

### LOW-1: two inline writes during ONE in-flight checkpoint target the same slot; a torn second write falls back to the superseded, higher R. The report's concern 2 is wrong: the result is not fail-stop

**Where:** `checkpointRecoveryPoint_` (Log.cpp:880–882): `target = 1 - rp_.slot` on every inline write while the checkpoint is InFlight.

**Sequence:**
1. An async write `{s+1, V}` is in flight to slot X.
2. `truncateTo(B1)` writes the active slot inline: `{s+2, B1}`, durable. The files are cut.
3. The async write lands.
4. `truncateTo(B2)` runs while still InFlight and writes the **same** active slot `{s+3, B2}`.
5. A tear of that 32-byte write leaves slot X `{s+1, V}` as the only valid slot. Recovery trusts V, which is above the files already cut at B1.

**Repro:** `Rereview.SecondInlineWriteTornWhileInFlight_StaleHighSlotWins` fails with "stale R 21 vs LEO 8".

**Likelihood:** needs a non-atomic sector write, plus two truncations within one checkpoint's latency. Very unlikely.

**Fix:**
- Fold this into the MAJOR-1 `rpDiskMax_` bookkeeping. An abandoned slot counts toward the on-disk maximum until it is overwritten.
- On the abandoned completion, queue an immediate rewrite of that slot.
- Correct the report's concern 2.

### LOW-2: a mid-batch truncation persists `min(R, truncateOffset)`, not the batch-floored LEO

**Where:** Log.cpp:1126 and :1291.

**Effect:** a crash between `resize_file` and the second `persistAtMost_` leaves R = truncateOffset above the recovered LEO. Recovery then logs a spurious "durable data vanished" ERROR. The data is not affected, since the straddled segment is the last one and so is not chain-checked.

**Fix:** compute the cut floor before the first persist, or downgrade that specific case.

### LOW-3: R advanced on an inline path for a `deferSealSync_` log sets `rpDue_` but never queues it

**Where:**
- `Log::sync`, the durable `append`, and the legacy fallback seal once more than `kMaxPendingSeals` are pending (Log.cpp:208, 521, 1034).
- `refreshRpDue_` after an inline truncation write.

**Effect:** in each case `maybeCheckpointInline_` returns early (`deferSealSync_`) and the worker is never told. The checkpoint waits for the next flush completion on that log. The R is stale but lower, so this is safe; on an idle log the cleaner gate lags indefinitely.

**Fix:** expose "became due" from these paths, or have the worker check `recoveryCheckpointDue()` after the inline calls it makes (role change or truncation).

### LOW-4: io_uring linked-pair edge cases

**Where:** UringFlushBackend.cpp:265–271 and :319–343.

**Issues:**
- (a) There is no `probeOpcode_(IORING_OP_WRITE)` (5.6+). On a kernel without it, every checkpoint fails with ECANCELED: the log is failed and the broker halts. Probe it at construction and refuse uring (or fall back) up front.
- (b) A partial `io_uring_enter` (returns 1 of 2) counts as success. The chain is cut at the batch end, so the fsync SQE can run later, unlinked, and possibly before the write completes. Treat `ret < n` as an error, or loop until submitted.
- (c) The failed write's real errno is lost: the fsync surfaces as ECANCELED. Record it next to `shortWriteTokens_` for the fail-stop log line.

**Not an issue:** the short-write mapping to ENOSPC is correct. On kernels that fail links on a short read or write, the result is ECANCELED, which is fail-stop either way.

### LOW-5: `rpFd_` has no lifetime pin (no counterpart to segment M1)

**Where:** `Log::~Log` closes `rpFd_` (Log.cpp:113) even if a portable `WriteSyncRequest` is still queued on the flusher.

**Risk:** if the fd number is recycled, the flusher pwrites 32 bytes at offset 0 or 4096 into an unrelated file.

**Current exposure:** latent. No runtime Log destruction exists (no topic delete was found); destruction happens at shutdown after the workers stop. Add a pin, or assert "no RP route in flight" in the dtor, before any delete-topic work lands.

## Hard rules
- **No syscall or allocation on the loop in the checkpoint path:** OK. The only syscall is the backend submit, which the backend design allows. Allocations happen only in `rebuildFlushSlots_` (cold), and `freeWriteBlocks_` / `shortWriteTokens_` are reserved up front.
- **Atomics:** only the renamed `pushSqes_` SQ head/tail sites, and the allowlist was updated. `check_no_atomics.py` reports OK.
- **No virtual calls or `std::function` on hot paths:** none added (the reap handler is a template lambda).
- **Tests do not sleep:** the new node tests use bounded `doWork` + `std::this_thread::yield()` loops with caps. That is acceptable. One negative check ("unowned log never touched") runs a fixed 200 iterations; it is weak, but it cannot flake.

## DUR-1 interaction (flagged, not reviewed)
- **Textual overlap with the current `feat/dur-1` (dc9df66) is small:**
  - one line of `Log.cpp` (`appendNoSyncRestamp` stamp);
  - additions in `Log.h`;
  - separate hunks in `Worker.h` and `NodeRuntimeAgent.h`;
  - a `LogCleaner` epoch stamp.
  All of these should rebase cleanly.
- **Semantic points:**
  - Every `truncateTo` with `persistedRp_` above the truncation point now performs an inline pwrite + fdatasync. That is fine while DUR-1 truncation (divergence / OffsetsForLeaderEpoch) stays a cold role-change path. If DUR-1 truncates from the fetch-response path on the worker, this becomes a syscall on the loop.
  - Design item 5 (a follower's acked LEO can exceed its synced LEO / R) interacts directly with DUR-1's Kafka HW = min LEO over ISR. After E1d, a power-lost follower comes back lower than it reported, so DUR-1's incarnation / unclean-restart handling must treat that as a legitimate regression. The E1b / KIP-966 follow-up is still open.
  - MAJOR-1 is most reachable exactly on DUR-1's divergence-truncation paths, because a role change is what triggers re-ownership plus truncation.
- **rune-protocol `6d14dc8`** (`WriteSyncRequest`, template 7) is on `feat/self-contained-worker-shards`. It is an ancestor of that branch's HEAD and also present on `feat/unknown-producer-id`. It must be on whatever protocol revision DUR-1 builds against.
