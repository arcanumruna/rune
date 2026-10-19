# Re-review: hot-path B review fixes, `perf/read-role-hotpath` 2ab1bc2..bfc92af

- **Verdict:** **REJECT.** The original findings are fixed, but the M2/Md5 fixes introduce one new critical and one new major. Both are small, local fixes. Re-review is scoped to N1 and N2 only.
- **Verified:**
  - `ninja` in build-fast is up to date.
  - `ctest -R "HotPathB|TruncateClampsSyncedLeo|EpochRestart"`: 33/33 passed.
  - Code reading of the full diff.

## Original findings

| ID | Fixed? | Where / note |
|---|---|---|
| C1 (a) ordering | **yes** | Recovery point capped by `min(pendingMin, inflightMin)`: `PartitionReplica::pushEpochCap_` → `Log::setEpochHistoryCap` (Log.cpp ~1374), applied in `advanceRecoveryPoint_` (Log.cpp:1350). Every epoch-changing site feeds the cap: assign `:1341/:1357`, truncation `:3081`, phantom drop `:488`, deferral `:2036`, cancel `:764`. Re-fetch/apply gated by `awaitEsFetch_` (`:1151`, `:1567`, `:2296`) and released only from the success completion (`:1967`), so it is event-driven with no timer. Inline-written replicas (internal / control-thread) clear the cap only after a successful inline fdatasync. Memory-backed replicas never enable it (no durability claim). Controlled shutdown: the persisted RP only moves through `advanceRecoveryPoint_`, so the cap holds; a pending write only costs re-derivation at restart. Cleaner/compaction do not touch epoch history (log-start pruning caps conservatively). See N4 for the one wedge. |
| C1 (b) restore | **yes** | `reconcileEpochCacheWithLog_` (`:2072`) re-derives from `startupRecoveryPoint`. `Log::recover` holds R at the startup value (Log.cpp:1058-1064) until the replica lifts the cap after the reconciled image is durable (enable path) or the legacy sync checkpoint is written. One bypass: N5. |
| Spot-check below RP sufficient? | **yes, given stamps are monotonic** | Each entry's first batch and the batch before the next entry are checked. Stamps are non-decreasing, so equal stamps at both ends imply every batch between carries that epoch. Any missing, extra or shifted entry below RP therefore changes a boundary stamp and triggers a full rebuild. A stale entry below RP cannot survive, so a follower diverging below this node's RP gets a correct answer. Caveats (low): batches stamped -1 are skipped; compacted gaps make `read(o)` return a later batch, which is a false "stale" and a full log scan at every restart. |
| C1 tests / power-loss realism | **adequate** | The seam rewrites `leader-epoch-state` to its last confirmed bytes and keeps all log data. That is the adversarial case for C1 (data durable, history not). A torn slot is covered by the CRC. The tests do not cover the missing-RP-file path (N5). |
| M1 | **yes** | `removeLegacy` (unlink + one full directory fsync) runs only after `create()` (fsync, rename, dir fsync) or after a Valid image is confirmed or updated inline. It is cold and runs on the control thread at build. A corrupt state file goes to a stamp rebuild (`:204`), never to legacy. No path yields an empty history over a recovered, non-empty, stamped log: the rebuild runs whenever `!restored && isRecovered && LEO > start`. See N6 for a fallback inconsistency. |
| M2 in-place truncation | **partially** | Crash consistency is fine. Every segment is `recover()`ed at startup (Log.cpp:1131/1140), which rebuilds the index, so a log shorter or longer than its unsynced, zeroed index is repaired. A lost `ftruncate` resurrects the tail above the lowered RP, the stamp reconcile re-derives its epoch, and KIP-279 re-truncates (Kafka-equivalent). The truncate-below-HW refusal is unchanged at the replica level. **But see N1: the in-place cut can land below the requested offset.** |
| M2 attribution / ABAB | **yes** | An n=15 ABAB was done and the fsync counts were added. On LeadershipChange/8p, see the bottom section. |
| Md1 | **yes** | Cancel caps R at `leo` (`:764`). The restart reconcile from `≤ leo` drops the phantom. |
| Md2 | **yes** | Re-arm with backoff (`:1937-1950`). The third failure in a row marks the log storage-failed and the directory offline (`:1958-1963`). |
| Md3 | **yes** | `epochStateWriteNotRouted` re-queues, and the slot `pwrite` is synchronous, so there is no slot-reorder hazard. It cannot loop forever: the route table drains on other completions. Low: no backoff while the table stays full, so each pass resubmits an fdatasync. |
| Md4 | **yes** | Per-worker scratch, lazy fd, 8 KiB file for new files (existing 32 KiB files are kept). |
| Md5 | **yes, with new defects** | Address-space sizing is fixed. The swap happens on the owner at the top of `readView` (Log.cpp:929) and every view is consumed in the same call (fetch `:878`, push `:3233`, RC `step_` on the owner). The munmap on the provisioner therefore never races a live view: no use-after-unmap found. Compaction or truncation reinstall changes `installGen` and the stale map is discarded. **But see N1, N2, N3.** |
| Md6 | **yes** | Assert plus refusal while not Idle; the inline path asserts Idle. |
| Lows | backlog D21 | — |

## New findings

### N1 CRITICAL: in-place truncation over a partial read map cuts below the requested offset, silently deleting committed records

**Where:**
- `LogSegment::truncateInPlace` (LogSegment.cpp:642-656) walks `size = min(logSize_, readMapSize_)` and `ftruncate`s at the last whole batch inside the *mapped prefix*.
- `ensureReadMap` (LogSegment.cpp:520) returns true with a prefix-only map whenever `ownerRemap_ == false`, which is the Md5 change.
- Nothing checks that the walk reached the batch holding `truncateOffset`.

**Reachable in production (provisioned logs):**
- `Log::readView` sets `ownerRemap_ = false` on every segment it serves (Log.cpp:933).
- The active segment starts with a 1 MiB map and grows only when the provisioner's mapping is installed, which happens **only from `readView`** (`pollReadMaps_`, Log.cpp:931).

Concretely:
1. A leader's active segment has been read (`ownerRemap_` false) with a map smaller than its file. This happens either while a growth is outstanding (512 KiB of headroom), or, more commonly, after demotion: as a follower it keeps appending into the same segment, `maybeRoll` posts a growth request, and no `readView` ever polls the answer, so the map stays at 1 MiB while the file grows to the roll size.
2. A new leader is elected and this follower is ahead of it. The KIP-279 truncation to the new leader's end offset lands beyond the mapped prefix.
3. `truncateInPlace` cuts at ≤ 1 MiB into the segment.

**Result:**
- The follower's LEO is far below `truncateOffset`. Records at or below the HW are gone locally, while the node is still in the ISR. If it is elected before catching up, committed data is lost.
- `truncateLog` prunes the epoch cache at `truncateOffset`, which is above the LEO, so the next `assign` at the lower LEO is dropped as non-monotonic (A5). The lineage goes wrong too.

**Fix:**
- `truncateInPlace` returns -1 (rebuild path) unless `readMapSize_ >= logSize_`, or it lets this cold path remap on the owner (`ownerRemap_ = true` around the walk, as `segmentBytesAt` does).
- In addition, it fails or falls back unless the walk stopped at the batch containing `truncateOffset` (`next <= truncateOffset < batch end`) or at the log end.
- RED test: a provisioned log, `readView` on the active segment, growth not polled, append past 1 MiB, then `truncateTo(x > 1 MiB)`. Assert `LEO == x` rounded down to the batch boundary.

### N2 MAJOR: a late grown mapping can be installed into the wrong segment

**Where:** `offerReadMap` carries only the pointer and size. `pollReadMaps_` installs into `segments_.find(growBase_)` if `installGen() == growGen_`, which are the *current* request's values (Log.cpp:276-283).

**How it happens:** the count-based re-request (Log.cpp:249, 4096 waits) clears `growOutstanding_` while the first request may still be in the provisioner's queue. A slow provisioner is enough: fallocate of 64 MiB segments on ext4 under load. The next request can be for a different segment, for example the new active segment after a roll. The first (late) mapping, of segment A's file, then passes the check and is installed as segment B's read map.

**Result:** `readView` serves A's bytes as B's, to followers and consumers. If A's file is shorter, readers fault with SIGBUS.

**Fix:** put `(base, installGen)` in the ring message (or in a small struct owned by the message) and match on it. Drop the map when the identity does not match. Add a test that delays the response across a re-request on another segment.

### N3 MEDIUM: replication can stall on the provisioner

`readView` serves only the mapped prefix while a growth is outstanding. If appends outrun the 512 KiB headroom before the provisioner answers, follower fetches and pushes beyond the prefix return nothing until the map lands. acks=all latency then becomes coupled to provisioner latency. The 4096-count fallback re-asks the same busy thread.

The owner-growth fallback only triggers on an explicit failure message.

**Fix (consider):** let the owner serve and remap the *active* segment when `logSize_ > readMapSize_`. This is a counted cold event and is better than a stall. Alternatively, request growth earlier and with a larger headroom. Measure it in the gate.

### N4 MEDIUM: `awaitEsFetch_` can stay set forever

It is cleared only in the async success completion (`:1967`). Two other paths make the history durable without passing there, and the flag is never reset on role transitions:
- the inline re-create in `prepareEpochStateWrite` (`:1879`, outgrown slot);
- `completePendingLeadershipIfDurable_` clearing `due` without a write.

So a later `becomeFollower` never fetches (`doWork :2296`) and the replica is wedged, which is not time-independent convergence. It is narrow: the capacity boundary is hit while the flag is set (for example truncation, then a deferral's extra entry, then promotion, then demotion).

**Fix:** have one "history became durable" hook, called from both the inline and async success paths, that clears the flag. Also clear or re-evaluate the flag in `becomeLeader` and `becomeFollower`.

### N5 MEDIUM: a missing recovery-point file bypasses the epoch guard

`Log::recover` with `RpFileState::Missing` calls `createRecoveryPointFile_(next)`, which persists RP = LEO, after setting the guard's held R. This happens on the upgrade path from a pre-E1d directory with legacy epoch files.

If the process crashes before the replica's legacy checkpoint is written, the next restart reconciles only from the LEO.

**Fix:** create the file with `recoveryPoint_`, the held value.

### N6 MEDIUM: an update failure in `enableEpochState_` leaves two authorities

If the Valid-branch inline write fails, `es_` is reset (`:1735-1738`). `initReplica` then writes the **legacy** checkpoint and lifts the cap (`:244-250`). The stale but Valid state file stays authoritative at the next restart, and RP has advanced past history that only exists in the legacy file. The spot-check (above) will usually catch it, but this is the M1 two-authority situation again.

**Fix:** fail-stop the log, or make the state file invalid before falling back.

### Lows
- `kMinEntries` went from 1024 to 64. The L1 inline re-create (fsync, rename and directory fsync **on the worker**) now first triggers at about 64 epochs per partition instead of about 1024. Pre-size the file, or move the re-create off the worker (D21).
- The in-place `ftruncate` frees the segment's `fallocate` preallocation past the cut. Later appends into that segment hit block allocation (the ext4 first-write class). Re-`fallocate(KEEP_SIZE)` after the cut, from the provisioner.
- `truncateInPlace` walks the map **before** `log_.flush()`. This only matters on the fstream path; flush first.
- `epochStateWriteNotRouted` has no backoff (Md3 above).
- When `openEpochStateFd_` fails, the code reports `errno` after other calls may have clobbered it (`:1888`).

## LeadershipChange/8p ext4 max 864 → 1,652 µs

**Nothing in this diff is plausibly the direct cause.** The worker issues no fsync or fdatasync in steady state. The diff's fsync sites are:
- io-backend submissions;
- the cleaner thread;
- recovery;
- `LeaderEpochState::create` / `removeLegacy` (control thread at build);
- the inline re-create (worker, cold, at more than 64 epochs, not reached at 24 flips).

The worker-thread syscalls this diff adds on role changes are all cold, role-path only:
- `open`/`close` of `leader-epoch-state`;
- the slot `pwrite`;
- the in-place `ftruncate` (truncation passes only, not in this scenario).

The env A/B in the report (epoch io, C1 cap and Md5 growth each removed, no change; legacy synchronous checkpoint restored, regression gone) points at F7's removal of the COLD inline syncs, which predates this diff. Accepting it as a known, narrowed F6/F7 item is reasonable, provided it stays inside the `kKnownFailures` ceiling and the provisioner pre-write follow-up is tracked.
