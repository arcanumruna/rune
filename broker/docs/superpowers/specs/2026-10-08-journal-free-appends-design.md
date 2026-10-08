# Journal-free worker appends on ext4 — design (DRAFT, awaiting approval)

Status: DRAFT — written 2026-10-08 for review; nothing implemented.
Origin: #66 / backlog D27 (box captures `/root/e3g/cap66/`).

## Problem

On ext4 the worker's produce append (`pwrite`/`pwritev` into the active segment) can block for 1–7 ms in the
journal (jbd2). Measured on bare metal (fresh ext4, interleaved, n=4): the FollowerTruncation gate's **max**
pass is 3–5× worse on current main than at 70217bd (0.9–1.6 ms → 3.0–6.4 ms) while **p99.9 is at parity**.
The slow passes are produce passes whose time is all intake (= the worker's append).

D-state stacks of the process show the append waiting in:
- `pwritev → ext4_buffered_write_iter → down_write` (inode lock),
- `generic_write_end → ext4_dirty_inode → start_this_handle → add_transaction_credits`,
- `__ext4_mark_inode_dirty → jbd2_journal_get_write_access`, and `jbd2_log_do_checkpoint`.

Why an append needs the journal at all:
1. Segments are preallocated with `fallocate(FALLOC_FL_KEEP_SIZE)` (`LogSegment.cpp:321`), so every append
   **grows `i_size`** → the inode is dirtied → a journal handle.
2. Preallocated blocks are **unwritten extents**; the first write into each converts it → extent-tree
   metadata in the journal.
3. mtime/ctime updates dirty the inode (the `lazytime` experiment showed this alone is NOT the cost — runs
   were no better — because 1 and 2 still dirty the inode).

Hot-path B moved role-change / truncation durability (C1 epoch-state, F7 fence, recovery point) onto
background threads, so their `fdatasync`s and the journal commits/checkpoints they force now overlap the
worker's appends instead of happening inside a few (slow, but rare) worker passes. Kafka has the same
exposure; Rune's ≤100 µs worker budget does not tolerate it.

## Goal

A worker append on ext4 issues no journal handle in steady state: it overwrites already-allocated,
already-initialised blocks inside the file's existing size, without touching timestamps.

## Design (revised 2026-10-08: NO extra I/O — the user rejected zero-filling)

1. **Full-size preallocation, no zero-fill.** The SegmentProvisionerAgent preallocates a ready segment with
   `fallocate(fd, 0, 0, rollBytes)` (no `FALLOC_FL_KEEP_SIZE`): blocks reserved as unwritten extents and
   `i_size = rollBytes` up front, so an append never grows `i_size` (no inode dirty from the size). Unwritten
   -> written extent conversion happens at writeback (flusher / the fsync threads), not in the worker's
   `pwrite`. No data is written twice; I/O is unchanged from today.
2. **`O_NOCMTIME`** on the segment fd (the broker owns its files; falls back silently on EPERM) so appends do
   not dirty the inode for mtime/ctime.
3. **Logical end != file size.** Unwritten extents read back as zeros, so:
   - recovery scans batches from the start (as today) and stops at the first batch header whose
     `totalLength` is 0 / CRC-invalid — a zero header is the defined end-of-data;
   - `sizeBytes()` / retention-by-size / snapshots use the logical size (`logSize_`), never `st_size`;
   - sealing a segment trims the unused tail (`ftruncate(logicalSize)`) on the provisioner/cleaner, never on
     the worker, so sealed files keep today's on-disk format;
   - in-place truncation (hot-path B M2) of the active segment keeps `i_size` and only moves the logical end
     (the stale bytes past it are overwritten by later appends; recovery never reads past a zero/invalid
     header — needs a "zero the first header past the cut" step so stale batches are not resurrected).
4. **Config switch** `RUNE_PREALLOC_MODE=full|keepsize` (default full on Linux) for A/B.

Probe evidence (box, fresh ext4 loop image, 3 runs x 131k 4 KiB appends under a concurrent fdatasync storm):
keepsize p99.9 41-46 us / p99.99 155-222 us / max 316-783 us; full+O_NOCMTIME p99.9 24-37 us / p99.99
59-85 us / max 238-278 us (one 4.5 ms outlier, unexplained). The synthetic pressure did not reproduce the
gate's multi-ms stalls — the gate itself (fresh scratch fs, fixed length, ABAB) is the acceptance test.

## Risks / open questions

- Recovery correctness with a zero-filled tail after a crash (torn batch followed by zeros, zeros followed by
  stale data from a reused file) — needs a fault-injection test matrix (DR-15 style) before shipping.
- Resurrection of stale batches past a logical truncation point (in-place truncate) — the zero-first-header step.
- Disk usage: active segments occupy `rollBytes` on disk immediately (today KEEP_SIZE already reserves the
  blocks, so `du` is unchanged; `ls -l` sizes change).
- `O_NOCMTIME` semantics on overlay/docker volumes.

## Validation plan

1. RED test: on a scratch ext4 (Linux CI / box), a produce loop concurrent with a background `fdatasync`
   storm — assert worker append never enters the journal (count `ext4_dirty_inode` via a tracepoint, or the
   gate's max on the FollowerTruncation scenario under the fixed-length method).
2. Recovery fault-injection tests (zero tail, torn batch + zeros, stale bytes after zeros).
3. Box: FollowerTruncation / SegmentRoll / TxnMarkers gates on a fresh ext4 loop image, interleaved ABAB vs
   current main, n ≥ 6; produce-throughput benchmark before/after.
