# Rune MC-C — Kafka-faithful Log Cleaner + Retention (at-least-Kafka, 10× where possible)

> **STATUS: DESIGN SPEC (for review).** Grounds the reshaped tail of Milestone C-C.
> Companion research: authoritative Kafka 3.x port spec (LogCleaner/LogCleanerManager/
> LogManager/UnifiedLog + storage-module configs), captured 2026-09-21.
> Discipline: mirror Kafka's *algorithm* faithfully (like Rune mirrors Aeron), then
> exploit Rune's structure (single-writer, no-GC, lock-free, mmap, core-pinned) to
> beat it on CPU/latency-jitter/memory — never perturbing the append hot path.

## 1. Goal / non-goals
- **Goal:** compaction + retention that are **at least as optimized as Kafka** for
  compacting/retaining **hundreds–thousands of logs**, and **~10× faster than Kafka**
  on the dimensions we control (CPU-per-byte, latency jitter, memory, append-p99
  during compaction). Faithful port of every Kafka mechanism in the PORT CHECKLIST.
- **Non-goals:** beating the disk's raw fsync/rewrite bandwidth (compaction is
  ultimately I/O-bound — we *match* that ceiling, not beat it 10×). Tiered storage.
- **Hard invariant (unchanged from CP-1..4):** compaction is leader-only, sealed-only,
  never touches the active segment, never blocks appends, HW/LEO never perturbed,
  latest-per-key correctness, producer-state/offset recovery survives compaction+retention.
- **Perf gate:** append p99 during a full-speed compaction pass over hundreds of
  partitions must stay <100µs (STOP-and-fix otherwise). This is what CP-5 proves.

## 2. Where Rune stands today (post CP-1..4)
**Already Kafka-idiomatic (keep):**
- Sparse mmap **offset index** (`OffsetIndex`, 4 KB interval, 8 B entries, binary search
  + warm-start) — the read-from-offset path. No gap.
- Segmented `Log`/`LogSegment`, active-segment-excluded, temp-write→rename swap.
- Primitives exist: `LogCleaner`, `OffsetMap`, `RetentionPolicy`, `ProducerStateManager`,
  `LeaderEpochCache`.
- `cleanupPolicy`/`retentionBytes`/`retentionMs`/`deleteRetentionMs`/`minCleanableDirtyRatio`
  threaded on `PartitionConfig`; in-memory `lastCleanOffset` watermark.
- CP-1..4 correctness: re-keyed internal topics, dirty-ratio gate, one-segment bound,
  global latest-per-key, cleanup.policy routing, tombstone delete-retention, and
  rebuildFromLog surviving retention + compaction gaps.

**Behind Kafka (this spec closes):**
1. Compaction+retention run **inline on the duty-cycle thread** (Kafka: dedicated
   background threads). — *biggest gap.*
2. **Iterate-all-per-tick**, no global prioritization or budget (Kafka: filthiest-first
   global `max`, one log per thread per cycle).
3. Dirty ratio is **offset-span**, not **bytes** (Kafka: `dirtyBytes/(dirty+clean)`).
4. Dedupe map is **unbounded** (Kafka: `SkimpyOffsetMap`, 128 MB cap, digest-only,
   bounded-range-per-pass).
5. No **segment grouping/coalescing** (Kafka: `groupSegmentsBySize`).
6. No **I/O throttle** (Kafka: shared `Throttler`, `io.max.bytes.per.second`).
7. `lastCleanOffset` **not persisted** (Kafka: `cleaner-offset-checkpoint`).
8. Retention is **size-only, synchronous, inline** (Kafka: separate scheduler, **time**
   via `largestTimestamp` + log-start-offset, **async** `.deleted`+delay).
9. No **min/max.compaction.lag.ms**; tombstone drop not **delete-horizon** stamped (KIP-534).

## 3. Architecture — Aeron-adapted, lock-free cleaner

### 3.1 Dedicated cleaner agent (off the hot path)
Introduce a **`LogCleanerAgent`** — a dedicated AgentRunner (core-pinnable) separate
from the node duty-cycle/append threads. It owns:
- the set of leader-owned logs eligible for compaction/retention,
- one (configurable, default 1) reusable **dedupe buffer** + I/O buffers,
- the shared **Throttler**,
- the **filthiest-first selector**.
Because sealed segments are **immutable** (only the active segment receives appends,
on the partition's single-writer thread), the cleaner rewrites+atomically-swaps sealed
segments **with zero reader/appender locking** — the Rune analogue of Kafka's
`replaceSegments`, but lock-free (single cleaner writer per log; readers use the mmap
+ atomic segment-list pointer swap). This is where "off-thread + no-GC + no-lock" buys
the flat append-p99 that Kafka can't guarantee.

**HARD REQUIREMENT — failover/truncation safety (load-bearing).** The lock-free swap
must be correct under the replication/failover path, not just steady state:
- A cleaner rewrite in flight when the partition's role changes (Leader→Follower) must
  be abandoned safely — a demoted node must stop cleaning immediately and must never
  publish a swapped segment list after it has lost leadership.
- Replication **truncation** (a new leader truncating a follower's uncommitted tail) and
  a concurrent cleaner swap must not race: compaction operates only on **sealed segments
  below the high-watermark**, and truncation acts on the tail above the committed point,
  so the two ranges are disjoint by construction — this disjointness must be enforced and
  tested, not assumed. The cleaner must re-read leadership + HW under the same publication
  that readers observe, so a reader never sees a segment list from a stale leader.
- The atomic segment-list pointer swap must be a single release-store observed by readers
  via acquire-load (no torn list), and the superseded segments must not be physically
  freed until no reader can hold a reference (defer to the async-delete grace in CP-9).
CP-6 must include an in-process test that interleaves a compaction swap with a
role-change + truncation and asserts no reader observes a torn/stale/ resurrected segment.

*Threading note:* faithful to Kafka's model (N `CleanerThread`s, default 1) but adapted
to Rune's thread-per-core discipline — the cleaner is its own agent, not a shared pool
with locks. Multi-cleaner (N>1) uses an `inProgress` claim set so two cleaners never
grab the same log (Kafka parity).

### 3.2 Filthiest-first global selection + budget
Each cleaner cycle: `grabFilthiest()` scans all eligible leader logs, computes the
**byte-based** dirty ratio, and picks the single highest above `minCleanableDirtyRatio`
(or forced by `maxCompactionLagMs`). One log per cycle (Kafka parity), plus a **global
per-cycle byte budget** so a burst of dirty logs can't monopolize a cycle. Low-throughput
logs are guaranteed progress by `maxCompactionLagMs` → `needCompactionNow`.

Dirty ratio (Kafka-exact):
```
cleanBytes     = Σ segment.size for bases < firstDirtyOffset
cleanableBytes = Σ segment.size for bases in [firstDirtyOffset, firstUncleanableOffset)
dirtyRatio     = cleanableBytes / (cleanBytes + cleanableBytes)
```
`firstUncleanableOffset` excludes the active segment and (with `minCompactionLagMs`)
segments whose `largestTimestamp > now - minCompactionLagMs`.

### 3.3 Bounded, fast dedupe map (the 10× hash win)
Replace the unbounded per-segment map with a `SkimpyOffsetMap` equivalent:
- flat, cache-friendly open-addressing table (Swiss/Robin-Hood layout); entry =
  `keyHash + int64 offset`.
- **Bounded by a memory budget** (`dedupeBufferBytes`, default 128 MiB, split per
  cleaner) × **load factor** (default 0.9) → `maxDesiredMapSize`.
- **Bounded-range-per-pass:** build the map over the dirty section only until full; then
  clean only up to `map.latestOffset + 1`; checkpoint; resume the remainder next cycle.
- **Hash: xxHash3/wyhash, NOT MD5.** Kafka's `SkimpyOffsetMap` uses cryptographic MD5
  (24 B/entry, slow; KAFKA-10650's Murmur3 never landed). A fast non-crypto hash is a
  near-free multiple-× on the CPU-bound dedupe phase. Collision safety: store a wider
  hash (e.g. 16 B) to keep false-collision probability ≤ Kafka's MD5, or store the full
  key for internal topics where keys are small — decide in CP-7 (open question §6).

### 3.4 Segment grouping/coalescing
Port `groupSegmentsBySize`: accumulate consecutive sealed segments into a group while
combined data ≤ `segment.bytes`, offset-index ≤ maxIndexSize, and offset span ≤
Int32 (4-byte relative offset in `OffsetIndex`). Rewrite one destination segment per
group → many small post-compaction segments coalesce into fewer near-full ones (keeps
file-handle/index counts bounded as logs churn). This *supersedes* CP-2's strict
one-segment-per-cycle rewrite; the per-cycle bound becomes the byte budget (§3.2).

### 3.5 I/O throttle
A single shared `Throttler` (token-bucket) across the cleaner(s), bounding aggregate
compaction+retention bytes/sec (`io.max.bytes.per.second`, default unlimited to match
Kafka, but tunable). Finer-grained than Kafka's (no GC pauses skewing the rate), so it
can safely run closer to real disk bandwidth without starving foreground I/O.

### 3.6 Checkpoint persistence
Persist per-log first-dirty-offset in a **`cleaner-offset-checkpoint`** (mmap-backed,
reusing `MappedFile`), truncation-aware, so passes resume across restarts and the
`lastCleanOffset` watermark is durable (today it's in-memory only → recomputed/lost on
restart).

### 3.7 Tombstone delete-horizon (KIP-534)
Stamp an explicit `deleteHorizonMs` into the record-batch header the first time a batch
containing a tombstone is compacted; drop the tombstone once `now >= deleteHorizonMs`.
Removes the fragile mtime dependence. Transaction/control markers follow the same horizon
(MC-B txns are in scope). Requires a batch-header field in **rune-protocol** — split into
its own prerequisite ticket **CP-8P** (see §5) so CP-8 doesn't stall on the schema
change: reuse a spare `RecordBatch` attribute/field if available, else a small schema
bump coordinated with the MC-B txn attributes.

### 3.8 Retention parity
- Move retention to a **separate scheduler cadence** (still on the cleaner agent, not the
  duty/append thread), `retentionCheckMs` (default 5 min).
- **Time retention** `retention.ms` (default 7 days) using segment **`largestTimestamp`**
  (not mtime), plus **size** `retention.bytes` (default -1) and **log-start-offset**
  deletion. Active segment excluded; bounded by HW.
- **Async delete:** rename `.deleted`, physically remove after `fileDeleteDelayMs`
  (default 1 min). Crash-safe.
- `compact,delete`: both run (compaction for latest-per-key + retention for a hard
  time/size cap), compaction first (already CP-3).

## 4. 10× deltas vs Kafka (explicit)
| Dimension | Kafka | Rune target | Lever |
|---|---|---|---|
| Dedupe hash | MD5 (crypto) | xxHash3/wyhash | non-crypto, SIMD-friendly |
| Map layout | ByteBuffer linear probe | Swiss/Robin-Hood, cache-line aware | fewer probes, better locality |
| Thread model | JVM pool + locks + GC | core-pinned, lock-free, no GC | flat p99, no pause jitter |
| Segment rewrite | heap ByteBuffer copy | mmap zero-copy | less CPU/alloc |
| Append p99 during compaction | GC/lock jitter | unperturbed (isolated core) | isolation |
| Checkpoint | file rewrite | mmap | cheaper |
Ceiling we only *match*: raw disk rewrite/fsync bandwidth.

## 5. Reshaped plan (tickets)
Keep **CP-1..CP-4** (foundation + correctness; CP-4 verified separately). Replace CP-2's
*inline scheduling* with the cleaner-agent architecture below; **CP-5 moves last** so its
benchmarks validate the final design.
- **CP-6 — Cleaner agent + filthiest-first + budget.** Dedicated core-pinnable
  `LogCleanerAgent` off the duty thread; byte-based dirty ratio; global filthiest-first
  selection; per-cycle byte budget; lock-free segment swap. Move existing compaction call
  out of `runBoundedCompaction`/doWork. Integration tests: off-thread, no append
  perturbation (in-process), filthiest picked first, budget bounds per-cycle work.
- **CP-7 — Bounded fast dedupe map + checkpoint.** `SkimpyOffsetMap` equiv (memory cap,
  load factor, bounded-range-per-pass, xxHash3 open-addressing); persisted
  `cleaner-offset-checkpoint` (resume across restart, truncation-aware). Tests: map never
  exceeds budget; huge log cleaned incrementally across passes; checkpoint resume;
  hash-collision correctness.
- **CP-8P (rune-protocol prerequisite) — `RecordBatch` deleteHorizonMs header.** Add the
  KIP-534 delete-horizon field/attribute to `RecordBatch` in rune-protocol (spare
  attribute bit if available, else a coordinated schema bump), with codec round-trip +
  conformance golden vectors. Unblocks CP-8's tombstone-horizon work. Can run in parallel
  with CP-6/CP-7.
- **CP-8 — Grouping + throttle + delete-horizon + lag gates.** `groupSegmentsBySize`
  coalescing; shared `Throttler`; KIP-534 delete-horizon (consumes CP-8P);
  `min/max.compaction.lag.ms`. Tests: small segments coalesce; throttle bounds bytes/sec;
  tombstone dropped exactly at horizon; low-throughput log forced by maxCompactionLag.
- **CP-9 — Retention parity.** Separate scheduler cadence; time (`retention.ms` via
  largestTimestamp) + log-start-offset; async `.deleted`+delay. Tests: time retention;
  async delete lifecycle; compact,delete combined.
- **CP-5 — DR-E2E + benchmarks + whole-branch review.** Compaction survives
  restart/failover; latest-per-key; tombstone; offsets preserved. Benchmarks at
  **hundreds of partitions**: compaction throughput, and **append p99 flat during
  compaction** (<100µs gate). Head-to-head framing vs Kafka's published numbers.
- **CP-10 (FINAL capstone) — Ultra-low-latency review + optimization over MC-B + MC-C.**
  A dedicated pass over BOTH exactly-once (MC-B) and compaction/retention (MC-C) code for
  ULL: **cache-friendly** data layouts (cache-line alignment/padding, false-sharing
  elimination, SoA-vs-AoS on hot structures, no pointer-chasing on hot paths), **compact
  memory** (bit-packing, small-buffer/inline storage, arena/pool allocation, replacing
  `std::string`/`std::map`/`std::unordered_map` on hot paths with intrusive/flat
  structures), **zero-allocation hot paths**, and branch/prefetch friendliness. RULE:
  **every proposed optimization is validated with a before/after micro/throughput
  benchmark** — measurable win, no correctness regression, no p99 regression (STOP-and-fix
  if >100µs on the append hot path). NO speculative optimization: measure first, keep only
  what the numbers justify. Adversarial verification per change; whole-branch review after.
  See standing standard [[rune-ull-engineering-standard]].

## 6. Open questions (resolve during CP-6..CP-9)
- Dedupe hash width vs false-collision probability (match/beat MD5 safety with a
  non-crypto hash): 128-bit xxh3 vs storing full small keys for internal topics.
- ~~`RecordBatch` header space for `deleteHorizonMs`~~ — RESOLVED into prerequisite ticket
  **CP-8P** (§5); design decision (spare attribute vs schema bump) made there.
- Multi-cleaner (N>1) on a thread-per-core box: how many cleaner cores by default, and
  the `inProgress` claim mechanism (lock-free vs a small mutex — acceptable off the hot
  path).
- ~~Interaction of async segment `.deleted` / lock-free swap with failover/replication
  truncation~~ — RESOLVED to a HARD REQUIREMENT in §3.1 (disjoint ranges enforced +
  role/HW re-read under the reader publication + release/acquire swap + deferred free +
  an interleave test). CP-9's async-delete grace period is the deferred-free mechanism.
- Exact segment `largestTimestamp` source (do sealed segments already track it, or add a
  time-index/max-timestamp footer?).
