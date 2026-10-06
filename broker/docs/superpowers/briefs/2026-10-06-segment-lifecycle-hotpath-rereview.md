# Re-review: segment-lifecycle hot path review fixes (`0ada704..4a5f846`)

- **Scope:** commits 30f4385, 770091a, 32f2f2d, cf49200 and 4a5f846, checked against the original review (M1-M4, m1-m6) and the report's "Review fixes" section.
- **Method:** code reading only. No build or test was run, because build-fast was in use by another process.

## Verdict: APPROVE-WITH-CHANGES

The four majors are fixed. One new major was introduced by the m6 fix: a reordering race in `beginColdPath_` that can let `truncateTo` work on a stale staging path. It must be fixed before merge. The fix is a swap of two statements plus a deterministic regression test.

---

## Per-finding status

| Finding | Fixed? | Evidence / note |
|---|---|---|
| M1 provisioning failure wedges produce | **Yes**, with residual medium N2 | A failed build reports a FAILURE record (Log.cpp `provisionReady` → `reportProvisionFailure`). The node-wide cap does the same (SegmentProvisionerAgent.h `serve_`). The owner pulls it (`pullReady_`); `rollBlockedNow_` returns false when `failFront_ != 0` (Log.h:1247); `installNextSegment` rolls inline and counts it (Log.cpp:641-643). A failed inline create is fail-stop through `maybeRoll`'s catch → `markStorageFailed` (Log.cpp:~306-313), pinned by `ProvisionerFailureThenInlineFailureFailsTheLogRegression`. Warnings are rate-limited by powers of two. |
| M1 re-sent demands storm? | **No storm** | `postProvisionDemands_` re-offers only while `!thresholdPosted_` or `adoptedPostPending_`, i.e. only after a refused offer. Once a demand is queued it is never re-sent. A hold re-tries at most one offer per hold check. |
| M2 hand-rolled ready ring | **Yes** | It is now `rune::consensus::ControlRing<512>` (Log.h:1203). The allowlist diff is **removals only** (−10 UNSCOPED lines, nothing added in any category). `ControlRing` gained only `pendingBytes()` / `watchInfo()`, which are non-atomic wrappers over the ring's own positions. The remaining `renameReq*` / `renameDone*` atomics are pre-existing UNSCOPED debt. |
| M3 reservation scaling | **Yes**, but the default cap is questionable (N3) | Provisioning is demand-driven at the fill threshold (Log.cpp `maybeRoll` ~262-270). Depth defaults to 1 with clamp [1,8] in both config and agent. The cap check is in `serve_`. `FdBudget` counts `depth × kFdsPerSegment` (FdBudget.cpp:515). The 100 ms rescan is removed. |
| M3 threshold miss (single batch > 50%) | **Bounded** | The threshold check runs *before* each append, so a batch that jumps past 50% posts the demand at the next append. A batch that jumps past 100% posts it from `rollBlockedNow_` → `postProvisionDemands_(rollBytes_)`. In both cases the hold lasts at most one provisioner build (plus a doorbell wake), and a failure turns it into an inline roll. No unbounded case. |
| M4 provisioner protocol untested | **Yes** | Six `SegmentProvisionerRegression.*` tests (ShardNonBlockingTest.cpp:3330-3442). The report claims a TSan run (not re-verified here). |
| m1 image-wide hold / refusal ordering | **Yes** | Worker.h:5397: frames that are not leader or are oversize are answered, not held. The contract is documented at the hold site. There is a real controlled-poll test. |
| m2 follower overshoot | **Yes** | PartitionReplica.cpp:1801-1806 sends `maxBytes = 0` while `rollDeferred()`. The index is sized for the overshoot cap (Log.cpp:230). Txn markers and op-ring produces can still overshoot up to the cap, which is accepted and documented. |
| m3 snapshot memory | **Yes** | 64/64 (Log.h:70-71). |
| m4 stale-data recovery | **Yes**, with a low note (N5) | The staging file is trusted only when its first batch is complete and CRC-valid. It never overwrites an existing segment and is otherwise quarantined as `.corrupt`, never deleted (Log.cpp ~805-830). There is a crash test with ready segments plus queued renames. See the durability analysis below. |
| m5 provisioner idle CPU | **Yes** | Doorbell plus `ParkUntilWorkIdle` with a 10 ms cap (BrokerNode.cpp:2493-2497). |
| m6 cold paths wait for any rename | **Fixed, but introduces N1 (major)** | `beginColdPath_(touchFrom)` (Log.cpp:2207-2226); `deleteOldSegments` breaks at `pathPending` (Log.cpp:1918). |

### m4 durability analysis (no regression on the normal path)

- **Normal case.** A staging file holding acked data is a segment that was adopted, appended to and fsynced. Its first batch is complete, so it passes the CRC and is renamed exactly as before. There is no new data-loss path.
- **Torn or short first batch.** The only acked-data case would be a fsynced first batch that is torn. Fsync makes that impossible, so any torn first batch was never acked. The file is kept as `.corrupt`, not deleted, which is strictly safer than before.
- **Target already exists.** See N5 (low).

---

## NEW issues

### N1 (MAJOR): `beginColdPath_` now drains the done ring *before* reading `renameReqHead_`, so a rename that completes in between is never applied, and `truncateTo` / `reloadSegments` touch a stale staging path

**Where:** Log.cpp:2215-2222.

**Order before 0ada704 (correct):**
1. Acquire `reqHead`.
2. If `head == tail`, drain the done ring.

The provisioner publishes `renameDoneTail_` *before* it releases `renameReqHead_` (Log.cpp:754-758). So acquiring `head == tail` guaranteed that every completion was visible to the drain.

**New order:**
1. `drainStagingRenamesDone()`.
2. Load `reqHead`.

**Interleaving:**
1. The owner drains the done ring and finds it empty.
2. The provisioner finishes the rename of segment B (base ≥ touchFrom): the file is renamed on disk, done is published, and head is released.
3. The owner acquires `reqHead`. B is no longer in [head, tail), so nothing defers.
4. The cold path proceeds. B's `logPath_` is still `staging-N.stlog` and its `pathPending_` is still true.

**Consequences in `truncateTo`:**
- **B is deleted (B ≥ truncateOffset).** `std::filesystem::remove(lpath)` on the staging name is a silent no-op (ENOENT returns false). The real `<B>.log` / `.index` **stay on disk** while the in-memory segment is erased. On restart, the truncated (divergent) records are rediscovered. If truncateOffset == B, they chain seamlessly onto the active segment and are **resurrected as valid log content**. This breaks the stale-data rule.
  - If B is later re-adopted at the same base and the broker crashes before the rename, N5's "target exists" check then quarantines the *new* acked staging file and keeps the stale one.
- **B is the straddled active segment.** `std::ifstream f(lpath)` fails and `tellg()` returns -1. `std::vector<uint8_t> buf(size_t(-1))` throws `length_error` after the later segments were already erased: a partially applied truncation, surfaced as an exception.

`reloadSegments` (touchFrom = INT64_MIN) has the same window. `deleteOldSegments` is safe, because it checks `pathPending()`.

**How likely:** the window is narrow, but it is a time-dependent race on the follower-truncation path (leader change right after an adoption), which is exactly the DR matrix's territory.

**Fix:**
1. Acquire `reqHead` first.
2. Then `drainStagingRenamesDone()`.
3. Then scan [reqHead, reqTail).

Because every completion before the acquired head is visible, step 2 applies it.

**Regression test (deterministic):**
1. Adopt, then call `finalizeStagingRenames()` without letting the owner drain.
2. Call `truncateTo(B)`.
3. Assert that `<B>.log` is gone on disk and that reopen shows no records ≥ B.

Mark it Regression.

### N2 (MEDIUM): demands parked for logs not in the view leak, and past `kMaxParked` a pre-view Threshold demand is dropped silently, which is an unbounded hold

**Where:** SegmentProvisionerAgent.h `park_` / `installView_`.

**What happens:**
- Entries are removed only when their address shows up in a view.
- A demand that a retiring replica posted after the view without it was installed is parked **forever**. It is a dangling address; serving it later is not a UAF, but it is an ABA serve of whatever new `Log` reuses the address.
- Over partition delete or reassign churn, these stale entries fill the 4096 slots.
- After that, `park_` drops new entries with no counter and no failure record. It cannot post one, because it may not dereference the log.
- The owner has `thresholdPosted_ = true`, so it never re-posts. That partition's next roll holds forever, which is the M1 failure class again.

**Fix (any of):**
- Tag each parked entry with the view generation and drop it if it is absent from the first view installed after it was parked. A log is registered before it is driven, so a genuine pre-view demand is always in the next view.
- Or, when a log *enters* the view, have the provisioner post a failure or a ready segment unconditionally whenever the log's demand was parked.
- In either case, count drops.

### N3 (MEDIUM): the 16 GiB static cap is not FREE-tier-safe, and reaching it brings back the F1/F2 inline cost in steady state

**Why the cap is reached in normal operation:**
- Every partition whose active segment is in [50%, 100%) holds one ready segment (fallocated with `rollBytes`). That is about half of all partitions with any traffic, and the reservation persists while a slow partition idles at 60%.
- 16 GiB / 64 MiB = **256 partitions**. A multi-tenant box with 1,000 slow partitions sits permanently above the cap.

**Effect above the cap:**
- Every further Threshold gets ENOSPC, so every roll of those partitions is an inline create plus fallocate on the worker. That is the 0.2-17 ms F2 cost, now counted but steady.
- On a small disk (about 40-80 GB), 16 GiB is 20-40% of capacity reserved for empty blocks, which brings ENOSPC forward.

**Fix:**
- Derive the default from `statvfs(dataDir)`, for example `min(16 GiB, 5-10% of fs size)`.
- Over the cap, build the ready segment **without** `preallocate` instead of failing. Files and maps are cheap, and the owner stays I/O-free; only the fallocate is shed.
- Add the formula to the hosting-tier capacity model.

### N4 (MEDIUM): M1 observability and the perf proof are incomplete

- `provisionFailures_`, `failureInlineRolls_`, `capped_` and `rollDeferrals_` are exposed only as `*ForTest`. There is no metric and no FR site; the review asked for a counter plus an FR site. A FREE-tier box running permanently above the cap (N3) is invisible except through a rate-limited warn.
- Several ext4 p99.9 values rose after the fixes, with no interleaved ABAB (n ≥ 15) to support the "noise" attribution in the report, as required:

  | Scenario | p99.9 before (µs) | p99.9 after (µs) |
  |---|---|---|
  | Reads+Retention | 842 | 1,774 |
  | Reads+SegmentRoll | 438 | 811 |
  | Retention/2p | 1,020 | 1,359 |

  It may well be F6 `pwrite` variance, but this needs either an ABAB or FR attribution of the excess before it can be called noise.

### N5 (LOW): recovery edge cases

- `RecordBatch::ensureValid` CRC-covers attributes onward. `baseOffset` (offset 0) is **not** under the CRC, so "trust the base from a CRC-valid batch" does not validate the base itself. Cross-check it against the recovered chain (for example, base ≥ the previous segment's end).
- "Target exists → quarantine the staging file" keeps the *older* file. The only reachable producer of a duplicate base found so far is N1 (a stale undeleted file after truncation), and there the staging file is the authoritative one. Once N1 is fixed this is defensive only, but log both files' last offsets so an operator can decide.
- `.stlog.corrupt` files are never reclaimed. That is fine, but document it.

### N6 (LOW): other provisioning edges

- **Cap overshoot.** The cap is checked once per `serve_`, and then `provisionReady(depth_)` builds up to `depth` segments, so the cap can be exceeded by `(depth−1) × rollBytes`.
- **Accounting counts failures.** `readyQueuedApprox()` counts FAILURE records as ready segments, both for the depth check and for `account_`. A ready segment queued behind a stale failure is also skipped by one inline roll. This is harmless but slightly pessimistic.
- **Safety net removed.** `finalizeStagingRenames` stops when the done ring is full, and the 100 ms rescan that used to re-drive it is gone. If a partial finalize ever happens, nothing re-serves the log until the next adoption, which itself needs rename room. That would be a hold with no wake. It is unreachable while the owner drains the done ring every pass, but add a `static_assert` / comment, or re-post an Adopted demand from `rollBlockedNow_` when `!renameRoom_()`.
- **Silent build drop.** A ready-ring `offer` failure after a successful build (`provisionReady`, `break`) returns 0 with no failure record. This is unreachable with 512 B against at most `depth + 1` records, but if it ever happened it would be silent.
- **Follower ISR.** A follower that sends `maxBytes = 0` keepalives while deferred still accrues lag, so a long deferral (slow disk with a full seal queue) leads to an ISR shrink. That is bounded and acceptable; note it in the follower contract.

---

## Summary

| Severity | New |
|---|---|
| Critical | 0 |
| Major | 1 (N1) |
| Medium | 3 (N2, N3, N4) |
| Low | 2 (N5, N6) |

**Required before merge:**
- N1: reorder `beginColdPath_` and add the regression test.
- N2: parked-demand pruning plus a drop counter.

N3 and N4 can follow as backlog items with IDs. N3 should at least get the `statvfs`-derived default before FREE-tier use. Once N1 is fixed with its RED → GREEN regression test, no re-review is needed.
