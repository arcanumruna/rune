# Rune Transient (Non-Persisted) Topics — Design Spec

**Status:** DRAFT — for review
**Date:** 2026-10-09
**Program:** Rune materialized-views foundation (companion to the view-engine spec `2026-10-08-rune-view-engine-single-source-design.md` §9.7 transient views)
**Relationship:** topic/storage-layer feature. Transient *views* (view-engine spec) are independent of this but pair naturally with transient *topics* as their source.

---

## 1. Goal

A **transient topic** is a topic whose log is **not persisted to disk** — it is backed by an in-memory ring (`rune::storage::InMemoryLog`) instead of the on-disk `Log`. It trades durability-across-full-cluster-loss for **zero disk IO/footprint and bounded memory**, for streams where only recent/live data matters (hot caches, short-window feeds, derived/ephemeral streams, test/DR harnesses promoted to a product feature). **Persisted is the default; transient is opt-in.**

## 2. Non-goals

- Not a replacement for durable topics; transactional EOS durability guarantees (§6) are weakened for transient topics (see §5).
- Tiering/eviction-to-disk — a transient topic is memory-only; when the ring is full it evicts or back-pressures (§4), it does not spill to disk.

## 3. Mechanism — `InMemoryLog`-backed partition

`InMemoryLog` already exists (single-owner circular ring, O(1) evict, zero-disk, zero-alloc steady-state append; `DropOldest` default or `BackPressure` opt-in). A transient topic's partition uses it in place of the on-disk `Log`:

- **Storage:** fixed-capacity circular byte buffer (`maxBytes` / `maxRecords` caps), per partition. No segments, no fsync, no log cleaner, no recovery files.
- **Retention is the ring:** `DropOldest` (evict oldest batch when a cap is exceeded — the default bounded-memory behavior) or `BackPressure` (refuse the append until `truncateTo` frees space). `logStartOffset` advances on eviction, exactly as an on-disk log's retention floor advances.
- **Offsets/semantics identical:** append/read/truncate and offset semantics match the on-disk `Log`, so the worker, replication, and the view fold are agnostic to which backing a topic uses (the existing de-virtualized `LogT` template already compiles against both `Log` and `InMemoryLog`).

## 4. Durability & replication (the core semantic)

A transient topic **is still replicated through the normal plane** — followers hold the partition in their *own* `InMemoryLog`. Only the on-disk step is dropped:

- **`acks`**: `Leader` = the leader's in-memory append; `Majority` = a majority of replicas have it **in RAM** (no fsync). So a transient topic survives **minority** failures (surviving replicas still hold the range in RAM) but is **NOT durable across a majority / full-cluster restart** — by definition, nothing is on disk. This is the explicit trade.
- **Recovery:** a restarting node's transient partition starts **empty** and catches up from peers' rings via normal follower replication — **bounded by ring retention** (it can only obtain the range peers still hold). If *all* replicas restart, the topic is **empty** (ephemeral) — intended.
- **`recoveryPoint`/fsync bookkeeping** is inert for transient partitions (nothing to persist); the recovery-point machinery is simply not engaged.

## 5. Transactions on transient topics (OPEN / to confirm)

EOS/transaction markers (§ transaction libs) assume a durable log + `__transaction_state`. On a transient topic the LSO/commit-marker fold still works in RAM, but durability of a committed transaction is only "majority-in-RAM," not fsync'd. **Open question:** allow transactional produce to transient topics with the weakened (RAM-majority) durability, or reject transactions on transient topics? Proposed default: **allow**, with the documented weaker durability (consistent with §4). Confirm.

## 6. Control plane — DDL & metadata

- **Persistence is a per-topic property** set at create, stored in topic metadata (the `TopicRegistry`/`__topic_schemas`-adjacent metadata), default **persisted**.
- **Rune QL:** `CREATE TRANSIENT TOPIC name (…) ` (keyword) or `CREATE TOPIC name (…) WITH (persistence = transient [, maxBytes=…, maxRecords=…, onFull = dropOldest|backPressure])`. The ring caps + full-policy are transient-only knobs. (Rune QL standardizes on the SQL-style **`CREATE`** verb for both topics and views — `CREATE TOPIC` / `CREATE VIEW`, mirroring `CREATE TABLE`. The shipped 0c QL currently uses a verb-less `topic NAME { … } partitions N key k;` form, so adopting `CREATE TOPIC` is a small 0c grammar addition.)
- **AlterTopic:** flipping persisted↔transient is **not** a metadata-only change (it changes the storage backing); treat as create-new + migrate, or disallow in v1 (confirm). Schema evolution (`ALTER TOPIC ADD/DROP/…`) is orthogonal and unchanged.

## 7. Interaction with views

- A **persisted view over a transient topic** is coherent: the view keeps its own disk checkpoint, so it can recover to its last checkpoint `O` even though the transient source lost its post-`O` data in a crash (that data was transient — nothing durable is lost). The view resumes folding as the transient source refills from empty. (A persisted view thus can hold state beyond the transient source's ring retention.)
- A **transient view over a transient topic** is fully ephemeral and cheapest: no disk anywhere; restart = both empty, instantly ready (view-engine spec §9.7).

## 8. Testing & benchmarks (binding — #5/#6/#16)

- **No disk IO:** audit that a transient partition opens/creates **no files** and issues **no fsync** over its lifetime.
- **Ring retention:** `DropOldest` evicts oldest-first and advances `logStartOffset`; `BackPressure` refuses append at cap and resumes after `truncateTo` — both O(1), zero-alloc steady state (#4).
- **Replication:** a 3-node transient topic survives a **single** follower/leader loss (range still served from a peer's RAM within retention); a **full** restart yields an empty topic (ephemeral) — asserted, not accidental.
- **Parity:** offset/append/read/truncate semantics identical to the on-disk `Log` (the `LogT` template compiles + passes the shared suite against `InMemoryLog`).
- **Regression:** `*Regression*` per fix; regression suite first (#6/#7).

## 9. Open decisions

1. **Transactions on transient topics** (§5): allow with RAM-majority durability (proposed) vs reject.
2. **Alter persisted↔transient** (§6): disallow in v1 (proposed) vs support via migrate.
3. **Node-local (unreplicated) transient topics** as a further mode (single-node, no replication at all) — defer unless needed.
4. **`onFull` default**: `DropOldest` (bounded memory, lossy-oldest — proposed) vs `BackPressure` (lossless, stalls producer).
