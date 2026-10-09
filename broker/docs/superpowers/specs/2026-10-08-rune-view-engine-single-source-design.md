# Rune View Engine (single-source materialized views) — Design Spec

**Status:** DRAFT — open decisions resolved 2026-10-09 (§15 #1/#7/#9); prerequisite shipped; ready for spec-review → plan
**Date:** 2026-10-08 (decisions updated 2026-10-09)
**Program:** Rune materialized-views foundation (follows 0a rune-codec ✅, 0b schema registry + typed transport ✅, 0c Rune QL & CLI ✅)
**Depends on (prerequisite — SHIPPED 2026-10-09):** `2026-10-08-rune-schema-evolution-durable-tags-design.md` — durable per-topic field tags + versioned descriptors + compatibility modes. Implemented across rune-codec/protocol/broker/ql/client-cpp, merged locally into `feature/materialized-views`, e2e-green on real Aeron (k3s-node-01). The view engine's field identity `(topicId, tag)` and version-aware fold are now concrete: `FieldDescriptor::id` is a durable never-reused per-topic tag; the registry keeps versioned descriptors (`byIdVersion`/`byFingerprint`/`materializeAsOf`/`fingerprintAsOf`) and a per-record version-aware `readFieldByTag`.
**Prior art in this program:** `docs/superpowers/specs/2026-09-29-rune-materialized-views-typed-topics-design.md`, `…-0b-…`, `…-0c-…`

---

## 1. Goal

A broker-local **materialized view engine**: a view is a standing query over one or more source topics whose result is maintained **incrementally and continuously** as events are committed, persisted durably, and served by point-key lookup. This spec covers the engine end-to-end for the **single-source** case (one view folds one source topic) and lays the operator/dataflow and persistence foundation that the **join** (multi-source) case extends later without redesign.

A view, concretely, is a **keyed table** derived from a source log by a declarative plan: select/rename fields (projection), then merge records sharing a key under per-field rules (fold), delete on tombstone, and expose the current row per key.

## 2. Non-goals (this spec)

- **Joins / multi-source views.** The model is designed so a join is additive (§8, §12), but join execution, co-partitioning, and gather semantics are a separate follow-on spec.
- **Secondary indexes, range scans, filters, aggregations beyond per-field merge.** Point-key lookup only here; richer query surface is deferred.
- **Cross-node view placement / replication of the view table itself.** Views are rebuilt deterministically from the (already-replicated) source log on each node that hosts the partition (§11), so the view table is node-local derived state, not independently replicated. Multi-node placement/failover policy is a follow-on.
- **The query language surface** for defining views in Rune QL. This spec defines the engine and the `DefineView` control-plane API; the QL `create view …` syntax is a thin follow-on over `DefineView` (mirrors how 0c QL layered over `DefineTopic`).

## 3. Engineering-philosophy alignment (binding)

Every design choice below is checked against the Rune HARD RULES. The load-bearing ones for this engine:

- **Shared-nothing (#1).** The view table for a (view, partition) is owned outright by the single worker that owns that partition's log. No other thread reads or writes it. Persistence does **not** violate this: the worker publishes an **immutable** snapshot and a background thread reads only that (§9).
- **Ring buffers are the only cross-thread passing (#2).** Worker→snapshot-IO handoff and IO→worker completion go through SPSC rings, modeled on the existing `LogCleanerAgent` `swapRing_` + atomic-`shared_ptr` snapshot pattern. No shared mutable state, no locks.
- **Rich-state state machines (#3).** Per-key fold state and the snapshot lifecycle are explicit states (§7, §9), not scattered booleans.
- **Zero steady-state allocation (#4).** The view table is a preallocated open-addressing **index** (tag+key+row-id) over a **stable row-id slab** of fixed-width rows (`FixedStr`/bounded `VarChar(N)` from 0c), §7. The fold reuses buffers; deletes recycle row-ids via a free-list (no alloc); the next index buffer is background-pre-provisioned (no alloc on the triggering op); snapshot buffers are ping-pong-reused. No steady-state heap allocation.
- **No virtual calls on the hot path.** The fold dispatches per-field merge by a compiled `MergeRule` enum + `switch`, not virtual operator objects (§6). The dataflow "operators" are a *design* abstraction compiled to a flat plan, not a runtime object graph on the fold path.
- **Off the produce critical path.** The fold runs in the worker's duty cycle over its own read-committed log `[cursor, LSO)` (§11 — LSO, not raw HW, so aborted/in-flight transactions never materialize), bounded per cycle — never on the produce ack path. Serialize + fsync runs on a background thread, never the worker (§9). A produce may optionally hold its ack until dependent views have folded it (§11.1), but that is an opt-in per-produce acknowledgement, not the fold path moving onto the produce path.
- **Deterministic / time-independent convergence (#16).** The fold is a pure function of the committed log prefix; replica and leader converge to the identical view table at the same offset regardless of timing (§11).
- **Test + benchmark everything (#5), regression-test every fix (#6), fix-on-sight (#8).** §14.

> **Review note:** confirm this section is complete against the canonical 16-point philosophy; add any point you want explicitly gated here.

### 3.1 Typing & safety contract (binding — stronger than a typical RDBMS client)

The data-access surface (reads, view rows, projections, query results) is **strongly typed with no untyped escape hatch**. The defining property vs. a typical RDBMS client — whose dynamic path (`ResultSet.getInt(3)` / `getString("col")`) is untyped and silently coercing — is that **every access path here is late-bound-but-verified, never unchecked.** The following are binding invariants; §14 tests pin each one.

1. **Nothing decodes without a verified schema.** Every result — topic record, view row, projection, prepared-query row — passes a **fingerprint + length gate** at the accessor boundary (§12). On mismatch it **throws `SchemaMismatch`**; it never returns a value. There is no weakly-typed result object in the API.
2. **Identity is a durable never-reused tag, never name or position.** `ALTER` cannot silently remap a field under a client (an RDBMS column reorder/retype can). Old clients stay coherent against their retained schema version.
3. **No implicit coercion.** A column has exactly one type; the getter's type is checked **once** (at `TypedView` bind or `PreparedQuery::col<T>()` handle acquisition) and enforced structurally by the fingerprint. No int↔string, no truncation, no silent cast.
4. **Absence is explicit and typed.** A dropped / not-yet-live field surfaces as a typed absent at the boundary (decision §15 #9) — never a silent NULL flowing into arithmetic.
5. **Queries are values, not strings.** A query is a programmatic builder value → wire plan; structurally injection-free. SQL compatibility is a translator onto the *same* plan, not the primary surface.
6. **Drift is caught up front.** A client/broker schema-version mismatch fails at `openView` / `prepare`, not mid-scan (§10, §12).
7. **Deterministic read-committed + all-or-nothing visibility.** Reads observe the LSO-folded, read-committed view (§11); transactional writes are all-or-nothing in views (§11.1). No partial/aborted data leaks into a read.

**Two typing tiers, one mechanism, no hole:**
- **Compile-time** (`TypedView<RowT>`): a query known at build time — the C++ compiler itself rejects a missing/mistyped field; fully inlined, zero runtime type cost.
- **Runtime** (`PreparedQuery` + `col<T>()` handles): the result layout comes from the broker's result descriptor (§12.3); type is verified at handle acquisition; the per-row read is positional and safe-by-construction. Late-bound, still verified — and there is deliberately **no** `get(name)`-returns-any accessor that would bypass the check.

Compile-time where the query is static, fail-at-open/prepare otherwise, and **never a silent wrong value** anywhere.

## 4. Core model — a view is a dataflow topology

A view is defined as a small, **declarative dataflow plan**, conceptually a topology of operators:

```
Source(topicId, keyField) → Project(field select/rename) → Fold(keyed per-field merge) → Sink(viewTable)
```

- **Source** — binds the view to a source topic + names the key field.
- **Project** — selects and renames the fields the view keeps (a projection). Narrows each incoming record to exactly the fields the output needs.
- **Fold** — groups records by key and merges them under per-field rules into one current row per key.
- **Sink** — the materialized `viewTable`: the clustered, keyed result.

Crucially, the operator topology is a **design-time abstraction**. It is **compiled** into a flat `ViewPlan` (§6) that the fold executes with no per-record allocation and no virtual dispatch. "Operators" never exist as a runtime object graph on the fold path.

### 4.1 Push dispatch via a per-worker Reactor

A view is a **sink**, not a puller. When a batch commits on a topic-partition, the owning worker must discover *which views are interested in that topic* and feed them. This is a **per-worker Reactor** holding a subscription index:

```
subscriptionIndex : (topicId, partition) → [viewId, …]   // views interested in this source
```

On each advance of `[cursor, LSO)` for a partition (read-committed; §11), the worker's Reactor looks up the interested views and drives each view's fold over the newly committed records. The source topic carries the list of its subscriber views; the view does not scan topics. This keeps the dispatch O(interested views), co-located on the worker that already owns the log — no cross-thread hop to update a view.

> **Why this shape (recorded from design dialogue):** we do not make the *view* know its sources for pull; we make the *source* know its subscriber views for push. A reactor dealing in sinks collects the interested views on commit and applies the plan's transformations/merges/projections. For a join, the same index simply maps one worker's co-partitioned sources to the shared view (§8).

## 5. Operators (foundation set + extension points)

| Operator | Status | Role |
|---|---|---|
| **Source** | this spec | bind topic + key field; register in subscription index |
| **Project** | this spec | field select/rename = projection into output fields |
| **Fold** | this spec | keyed per-field merge into substate → reemit row |
| **Sink** | this spec | `viewTable` = compact index + stable row-id slab (§7) |
| **Join** | follow-on | additive gather over per-source substates sharing the key (§8) |
| **Filter** | follow-on | predicate drop before fold |
| **Aggregate** | follow-on | windowed/grouped aggregation beyond per-field merge |

## 6. Merge & projection — how a view definition compiles to functions

A view definition compiles to a flat, declarative plan. The per-field merge rule is the core primitive.

### 6.0 Field names come from the source topic's schema — resolved once, not on the fold path

Topics in Rune are **typed** (0b): every topic has a registered `Descriptor` in `__topic_schemas`, and the Descriptor is the authority for field names — `Descriptor::find(name)` maps a name to `{ id (uint16), type, offset, fixedLen }`. **The field names a view references are only meaningful against the source topic's Descriptor.** A view cannot name a field the source schema does not declare.

**Canonical field identity = `(topicId, fieldId)`.** The name a view writes is a *human alias* resolved to this pair at `DefineView`; it is not the identity. Reasoning:

- **`topicId`** is the broker-unique outer key (RULING-1: `TopicRegistry` is the sole sequential id allocator). A view sources from a specific topic/stream, so this is the right outer coordinate. It persists even if the topic's schema is replaced by a new lineage.
- **`fieldId`** is the field's id *within that topic's schema*. It must be a **durable logical tag** — assigned once, **never reused**, retired/reserved when a field is removed, a fresh tag for each new field (Protobuf/Thrift/Avro discipline). With durable tags, `(topicId, fieldId)` is a stable *logical* identity across **arbitrary** schema evolution, additive or breaking: tag *N* always denotes "the field that was tag *N*"; a version that dropped it simply reports it absent, and a view referencing it stays coherent with no version qualifier.
- **`schemaId` and `schemaVersion` are deliberately NOT part of the identity.** `schemaId` (fingerprint) is redundant with `topicId` for sourcing (the registry binds topic→schema). `schemaVersion` is a **per-record decode concern, not an identity component** — folding it into identity would give the same field a *new* identity every version, re-breaking compatibility and version-pinning every view. (The one world where you'd be forced to version-qualify identity is if tags were *reused* — which is exactly the discipline we reject.)

> **Prerequisite (SHIPPED 2026-10-09):** `FieldDescriptor::id` is now a **durable never-reused per-topic tag** (identity = `(topicId, tag)`, never name/position), delivered by the schema-evolution foundation. Field identity is therefore stable under **arbitrary** evolution (additive or breaking), not just additive-only. See resolved decisions #8 and #9.

**Version-aware decode (where `schemaVersion` actually lives).** For each committed record, the fold resolves `(topicId, tag) → physical accessor` using the Descriptor **for that record's schema version** (is the field live per `sinceVersion ≤ V < untilVersion`? what offset/type/width in *this* version?). This is now realized by the shipped schema-evolution foundation: the registry keeps **per-version materialized descriptors** (`materializeAsOf(evolving, V)` recomputes offsets for the fields live at `V`), looked up via `byIdVersion`/`byFingerprint`, and `readFieldByTag` decodes a record against its own stamped writer version. The fold reads each record against its writer version's materialized descriptor — the per-record decode cost that the <10µs fold budget (§14.0) must absorb.

**Uniqueness of the human alias at resolution time.** Since name is only an alias, the resolver must map it unambiguously to one `(topicId, fieldId)`: `DescriptorBuilder::add` does not reject duplicate names, `build()` has no dup check, and `find()` returns **first match**; duplicate names are rejected only at the Rune QL layer (`rune-ql Parser.cpp:241`). So at `DefineView` the resolver **asserts uniqueness** (a name resolving to >1 field is a define-time error) rather than trusting first-match.

**Across sources (a join), the alias is scoped per source.** Two source topics may each declare `id`/`name`; the identity is still `(topicId, fieldId)`, and the compiled plan's `sourceIdx` is just the local index into its source list (each entry = a `(topicId, Descriptor)`), so `(sourceIdx, srcFieldId)` is the compact local form of `(topicId, fieldId)`. Each source has its own key field resolved to its own `(topicId, keyFieldId)`, mapping to the shared join key.

**Output/alias side.** The view's **output** field names (aliases used in the projection) are a separate namespace, must be unique within the view's own result schema (the view is itself a typed topic), and live on `CompiledField.outSlot` — never on source identity.

Critically, name resolution happens **once, at `DefineView` time on the AdminWorker — never on the fold path**:

- The codec read primitive is `readField(descriptor, record, uint16_t fieldId)` — it reads **by field id**, not by name. Resolving a *name* costs a linear `find()` scan; doing that per record per field on the fold path would be a hot-path string scan, violating #4 / ultra-low-latency.
- So at `DefineView` the AdminWorker fetches the source topic's `Descriptor` from `__topic_schemas` and, for the `key` and every `srcField`, calls `find()` → resolves to `{ fieldId, type, width }`. This is also where it **validates**: the name exists, its type is numeric for `Sum/Min/Max`, the key field exists, and output types resolve.
- Because field identity is a **durable never-reused tag** (schema-evolution foundation, SHIPPED), a resolved `(topicId, tag)` stays valid across **arbitrary** later evolution — additive or breaking — with no re-resolution: a version that dropped the tag simply reports it absent at fold time (§11.1 / §15 #9), and per-record decode uses that record's version-materialized descriptor (§6).

This gives **two plan forms** — the authoring plan (string names, the Rune QL / `DefineView` wire form) and the compiled plan (resolved ids) installed into the worker's Reactor:

```cpp
enum class MergeRule : uint8_t { Replace, Sum, Min, Max, First };

// ---- Authoring plan: string names, produced by Rune QL / the DefineView wire op ----
struct AuthoredField {
    std::string name;      // output field name
    PrimType    type;      // output type (incl. FixedStr from 0c)
    uint16_t    sourceIdx; // which source this field is drawn from (0 single-source; >0 join)
    std::string srcField;  // field NAME in the source record — resolved at DefineView
    MergeRule   rule;      // how repeated keys merge for this field
};
struct SourceSpec   { int32_t topicId; std::string keyField; };
struct ViewPlan {              // the authored view, as defined
    std::string               viewName;
    std::string               key;      // key field NAME (clustered index key)
    std::vector<SourceSpec>   sources;  // size 1 here; >1 for join
    std::vector<AuthoredField> output;
};

// ---- Compiled plan: names resolved to ids against the source Descriptor(s), installed in the worker ----
struct CompiledField {
    uint16_t  outSlot;     // destination field slot in the view row
    PrimType  type;
    uint16_t  sourceIdx;
    uint16_t  srcFieldId;  // RESOLVED codec field id — fold reads by this, never by name
    MergeRule rule;
};
struct CompiledViewPlan {
    std::string                viewName;
    uint16_t                   keyFieldId;   // RESOLVED key field id, per source
    std::vector<const Descriptor*> srcDesc;  // source Descriptor(s) — already resident per 0b per-worker validation
    std::vector<CompiledField> output;
};
```

- **Projection** = the `output` list: which source fields survive, renamed, in what order and type. Fields not in `output` are dropped.
- **Merge** = per-field `MergeRule`, applied when a new record arrives for a key already present:

```cpp
// Pure, no-alloc, no-virtual. Called per output field during fold.
FieldValue mergeField(MergeRule rule, bool has_cur, FieldValue cur, FieldValue in, PrimType t) {
    switch (rule) {
        case MergeRule::Replace: return in;                       // upsert / last-write-wins
        case MergeRule::First:   return has_cur ? cur : in;       // keep earliest
        case MergeRule::Sum:     return has_cur ? numAdd(cur,in,t) : in;
        case MergeRule::Min:     return has_cur ? numMin(cur,in,t) : in;
        case MergeRule::Max:     return has_cur ? numMax(cur,in,t) : in;
    }
}
```

`Sum/Min/Max` require a numeric `PrimType` (validated at `DefineView` time against the source Descriptor, §6.0 — never on the fold path). `Replace` is the default and the single-source upsert semantics.

### 6.1 `foldRecord` (the fold path, per committed record)

Reads go **by resolved field id** against the source Descriptor the compiled plan carries (§6.0) — no name lookup, no allocation:

```
desc = plan.srcDesc[thisSource]                        // resident Descriptor
for each committed record r in [cursor, LSO) for an interested view:   // read-committed; §11
    k = readField(desc, r, plan.keyFieldId)            // codec zero-copy read BY ID (0a)
    if r is a tombstone (zero-length value):           // §6.2
        erase substate[k]; viewTable.erase(k); continue
    for each CompiledField f in plan.output where f.sourceIdx == thisSource:
        in  = readField(desc, r, f.srcFieldId)         // read BY RESOLVED ID
        cur = substate[f.sourceIdx][k].slot(f.outSlot) // current per-field value, if any
        substate[f.sourceIdx][k].setSlot(f.outSlot, mergeField(f.rule, has_cur, cur, in, f.type))
    reemit(k)                                           // gather substate → encode row → viewTable[k]
```

- **`substate[sourceIdx][k]`** holds the per-source merged fields for a key. For single-source there is one substate map; `reemit` is trivially the substate row. The substate layer exists now so **join** (§8) drops in as "more than one source substate gathered by `reemit`" with no fold-path redesign.

### 6.2 Delete = tombstone (zero-length record value)

A source record whose value is zero-length is a **tombstone** for its key: erase the substate and the view row. This matches Kafka compaction semantics and agrees with the broker's existing `Compact` cleaner (a tombstone both deletes the view row and is the signal the log cleaner eventually removes the key). Chosen over an explicit delete op for log/cleaner parity.

## 7. The sink: `viewTable` — compact index + stable row-id slab

The `viewTable` for a (view, partition) is the view's **primary storage** keyed by the view's key field — not a secondary structure over something else. It is split into a compact **index** and a stable **row slab**, a layout validated by a standalone benchmark (64/256/1024 B rows, 15 M rows, mixed create/update/delete churn — see §7.1 results) against a naive inline-row table.

- **Index = separated control + key + row-id (not `std::unordered_map`, not inline rows).** Flat, power-of-two, open-addressing arrays: a 1-byte **tag** per slot (top hash bits + empty marker, Swiss/F14-style — probed word-at-a-time), the 8-byte **key**, and a 4-byte dense **row-id**. The fat row payload is deliberately **kept out of the probe path** — the probe scans tiny tags/keys, touching a row only on a confirmed hit. (A naive inline-row table drags the whole 256 B/1 KB row through every probe step and through every resize; the benchmark showed that is pathological — see §7.1.)
- **Row slab = stable, row-id-addressed.** Rows live at `slab + rowid*rowLen` (fixed `rowLen` from §6.0's capped-width model — ints, `FixedStr(N)`, bounded `VarChar(N)`). Offsets are **stable**: a row never moves during normal operation, so the index's `rowid`s stay valid. Dense row-ids come from a free-list (recycled on delete) → no fragmentation holes to chase.
- **Deletes are O(1) and tombstone-free.** `erase` clears the index slot (backward-shift within the index — 13-byte entries, never payloads) and frees the row-id to the free-list. No payload move, no tombstone accumulation, no delete-driven compaction. (Benchmark: delete p99 flat at ~300 ns across all row sizes; the inline design degraded to ~1.2 µs at 1 KB because backshift memcpy'd kilobyte rows.)
- **Growth never blocks: incremental rehash + background-pre-provisioned buffer.** The index is pre-sized to the view's declared cardinality so it rarely grows. When load factor does cross the threshold, growth is **de-amortized**: a background thread pre-allocates+zeroes the next 2× index (the row slab is untouched — only tiny `(tag,key,rowid)` entries ever migrate), so `startGrow` is an **O(1) pointer swap** (~0.5 µs), and each subsequent op migrates a bounded `MIGK` entries old→new (reads/writes consult new-then-old during the window). Worst per-op rehash tax stays **< 50 µs, independent of row size** (§7.1). This is the only shape that meets the §14.0 budget — a stop-the-world rehash is 20 ms–4.2 s and is categorically forbidden on the hot path.
- **Point-lookup only.** `GetViewRow(viewId, key)` → index probe → `slab[rowid]`. No secondary indexes, no range scans in this spec (ordered access is a follow-on; the hash index is unordered).
- **Cheap persistence, no mmap.** Index + slab are flat and pointer-free and divided into chunks, so persistence is a **batch-boundary differential checkpoint** — copy only a batch's dirty chunks into an `{O, chunks, CRC}` delta (§9.2), never a full-view copy. The live view is **in-RAM, never an mmap'd file-of-record** — `msync` is non-atomic/not-offset-consistent (§9.6) and a mapped hot region risks page-fault latency tails, both disqualifying here.
- **Re-optimize pass (restart / on-request).** The only time rows move: a deliberate, batched pass reclaims free-list holes, **clusters hot rows into shared pages** (locality), and rebuilds the index dense. Off the hot path.

### 7.1 Storage-model benchmark results (validated 2026-10-09)

A throwaway spike compared the inline-row table vs. this index+slab design under the target workload (15 M rows @ 64/256 B, 8 M @ 1 KB; 60/25/15 create/update/delete):

| metric | inline rows | **index + slab (this design)** |
|---|---|---|
| worst rehash (256 B) | 935 ms | — |
| worst rehash (1 KB) | 4.2 s | — |
| **worst per-op rehash tax (incremental + pre-provisioned)** | n/a (blocking) | **< 50 µs, flat across 64/256/1024 B** (≈20–30 µs at MIGK=256; `startGrow` ≈0.5 µs; 0 fallbacks) |
| delete p99 @ 1 KB | ~1.2 µs | ~0.3 µs |
| point-find (steady) | ~25 ns | ~27 ns (the row-id indirection adds ~2–3 ns) |

The inline design's resize cost scales with `rows × rowSize` (unbounded, multi-second); this design's scales only with the tiny index, so it is pre-sizeable and incrementally migratable within budget. `MIGK` (entries migrated per op) is the tuning knob: 128–256 keeps the worst tax well under 50 µs while finishing a migration in ~10⁵ steps.

## 8. Join (follow-on — shape locked so it drops in)

Not built here, but the model is chosen so the join is **additive**:

- Each source folds into its own `substate[sourceIdx][k]` (§6.1 already keys substate by source).
- Sources for a join are **co-partitioned on the shared key** onto one worker (shared-nothing preserved: one worker owns the joined view's partition and all its source substates).
- A pure **gather** `Join` operator: on any upstream change to key `k`, `reemit(k)` gathers the latest field from each source substate and writes the row. **Outer-join by default** (missing source substate → null/absent fields), so a row appears as soon as any source has the key.
- No fold-path redesign: single-source is the `sources.size()==1` special case of the same `reemit`.

## 9. Persistence — periodic atomic snapshot + replay-from-offset

**Requirement (verbatim from design):** the view must persist **both current data and the offset representing the last applied event, atomically** — the persisted offset must *exactly* represent the view state in the artifact. Never persist "view-at-offset+1 with offset" or vice versa. Redis-style **periodic snapshotting** is sufficient because the **source log is the changelog**: on restart, load the snapshot and **replay the remainder from the persisted offset** to reach the latest view. The open problem: snapshot periodically **without serializing/persisting on the worker thread** that processes published events, under shared-nothing.

### 9.1 What already exists in rune-broker (reuse, don't invent)

Recon of the current tree (post-merge) confirms three reusable precedents:

1. **Atomic state-snapshot-with-offset pattern** — `ProducerStateManager::takeSnapshot` (full state snapshot + CRC32C + temp-file→rename) and `LeaderEpochCache::checkpointToFile` (write-`.tmp` → `fsync` → `rename` → `fsync` dir). This is the proven **atomic, all-or-nothing** on-disk write. `CleanerOffsetCheckpoint` is the precedent for persisting a cursor (int64 + magic).
2. **Background-IO-over-worker-owned-data pattern** — `LogCleanerAgent` (`rune-cleaner` thread) compacts off the worker/control threads by reading an **immutable `SealedSegmentSnapshot` the worker publishes via an atomic `shared_ptr`** (`std::atomic_store`), handing results back through an SPSC `swapRing_` the worker drains in its duty cycle. It never blocks the worker and never touches live state. **This is exactly the shape the view snapshotter needs.**
3. **SPSC byte handoff** — `OneToOneByteRing` (flat-bytes SPSC), for completion/signal handoff.

**Gaps to build new:** (a) a general "snapshot an arbitrary worker-owned keyed table at an offset" mechanism; (b) a rebuild path that **starts from a checkpoint offset** rather than a segment base (all current `rebuildFromLog` variants full-scan to HW).

### 9.2 Mechanism — batch-boundary differential checkpoint (validated via the spike series)

Persistence is **continuous and differential**, keyed off the worker's **batch boundaries** (§11 — the batch is the worker's unit of work), not a periodic stop-the-world cut. There is **no full-view copy, no COW machinery, and no memory doubling**; the worker's only cost is a small copy proportional to the chunks a batch actually dirtied.

**Durability is the replicated log's job, not the checkpoint's.** A row is therefore persisted **at most once per flush interval, regardless of how many times it was updated** — the checkpoint is allowed to *lag* the live view by up to the interval. This loses nothing: every update is already durable in the consensus log (the real WAL), and `replay (O, LSO]` on restart (§9.4) reapplies any post-`O` updates. The checkpoint is purely a **replay-bound optimization** so restart need not re-fold from the log's start; `O`, written atomically with the flushed chunks (§9.3), is the exact line between "already in the checkpoint" and "reconstructed from the log." So never persist per-update — persist a coalesced, offset-tagged delta on the cadence.

The slab is divided into **row-sized chunks** — the dirty/flush granularity is **one row (or a small multiple), NOT an OS page**. This is critical and spike-proven: a 4 KB chunk with 256 B rows is 4–16× **write amplification** (the checkpoint spike wrote 60 GB for a 3.8 GB view, saturated the SSD, and stalled the worker ~1.2 s); at chunk ≈ row size the amplification is ~1× and the problem vanishes. Per (view, partition), owned by the worker:

1. **Mark dirty on write (hot path, ~free).** The fold's row-write sets `dirty[C]` for the row-chunk `C` it touches — one bit-set per write (the only steady-state hot-path cost of persistence). A chunk touched many times within an interval stays a single dirty bit → it is written **once per flush**, not once per touch (coalescing — the reason flushes can be infrequent).
2. **Flush on a cadence, at a batch boundary (worker, small + bounded).** The flush trigger is `min(interval, dirtyCap)`, both configurable: a **time interval defaulting to ~1–2 s** (bounds staleness / replay length) **or** a **dirty-bytes cap** (bounds the per-flush copy, and thus the read tail, even under a churn burst) — whichever fires first. At the next batch boundary after the trigger — a consistent point `O = batchEnd` (an LSO/commit boundary, §11) — the worker **copies only the dirty chunks** into a **ping-pong handoff buffer** (size ∝ distinct chunks dirtied in the interval, bounded by `dirtyCap`, never the whole view), tags it `{O, chunkIds}`, rings it to the view-snapshot IO thread, and **clears the dirty bits**. The copy is mandatory (not a reference hand-off) so the worker proceeds while the IO thread drains the buffer, with no race on live state (shared-nothing preserved).
3. **Background write (off the worker).** The **dedicated view-snapshot IO thread** (§9.5) appends each delta `{O, dirty chunks, CRC}` to the view's on-disk checkpoint, with a **grouped `fsync`** (every ~T ms), so the durable recovery point is the last fsync'd delta's `O`. It touches only the handoff buffer, never live state.
4. **Base + compaction.** Periodically (or when deltas accumulate) a **base** is written/merged so restart replays a bounded delta chain; compaction of deltas→base runs on the IO thread / the §7 re-optimize pass. The live view keeps serving throughout.

**Validated (checkpoint spike, 15 M rows, 60/25/15 create/update/delete, row-sized chunks):** read latency with checkpointing **ON is identical to OFF through p99.9** — `p50 125 / p99 292 / p99.9 375 ns` (OFF: `125/291/375`); the per-flush copy is small (p99 ≈ 0.7 µs per batch at the finest cadence) and touches only the few reads in its window (p99.99+). IO volume per interval = **distinct dirty chunks × rowSize**, coalesced over the interval — modest for realistic skew; with row-sized chunks it never saturated (0.97 GB/s for 256 B). **Caveats the spike surfaced:** (a) chunk MUST be row-sized or amplification saturates IO (above); (b) the node's disk bandwidth must exceed the *aggregate* dirty rate across all a node's view-partitions sharing the one IO thread, else backpressure stalls the worker — the `dirtyCap`/interval knobs and coalescing keep that rate down.

Why this meets every constraint: **offset-atomic** (each delta is a consistent batch boundary tagged with its `O`; §9.3); **no memory doubling** (handoff buffers + the `dirty` bitmap only — never a second copy); **negligible worker impact** (a bit-set per write + one bounded copy per ~1–2 s, off the produce path; reads unmoved through p99.9); **IO fully off-worker**. A stop-the-world full-view copy, an inline double buffer, or an OS-page chunk granularity is a failing design (§14).

### 9.3 Atomicity guarantee (the core requirement — exact offset, never ±1)

Each durable unit — the base, and each delta — is **self-describing and all-or-nothing**: it carries its `appliedOffset O` and a CRC over `{O + bytes}`, and is committed atomically (a delta is appended with a length+CRC frame; the base is written temp→`fsync`→rename→`fsync(dir)`). The recovery point is **the offset of the last complete, CRC-valid delta** (or the base if no delta survives). A crash mid-append leaves a torn trailing frame the CRC rejects → the prior complete offset stands; a crash mid-base leaves the previous base intact.

The exactness follows from the batch boundary: a delta's chunks are copied at a **quiescent point** where the view reflects exactly `(…, O]` and nothing of `O+1` (the worker is between batches, single-threaded, not mutating — so no COW is even needed). `O` and the chunk bytes are captured in the same uninterrupted step and travel together in one CRC'd frame. Therefore **the saved offset always names exactly the state in the artifact — never `O-1`, never `O+1`** — and replay resumes at exactly `O+1` (no double-fold, no gap). ✔

### 9.4 Restart = load + replay-from-checkpoint, behind a readiness gate

1. Load the **base**, then apply the chain of **complete, CRC-valid deltas** in offset order → the view state at `O` = the last surviving delta's offset (fall back past any torn trailing delta; worst case, if no checkpoint survives, re-fold from the retained log floor — §9.1 gap b). Rebuild the index from the loaded slab (or the persisted `(key,rowid)` map), §7.
2. Set the fold cursor to `O`.
3. Replay the **source log `(O, committedTail]`** through the same `foldRecord` (§6.1) — the `rebuildFromLog` scan, **starting at `O`**, in the same batch unit (§11).
4. Result: the exact latest view. Deterministic (§11) — the fold is a pure function of the committed (read-committed, LSO) log prefix, so replay reproduces the identical view.

### 9.4.1 Readiness gate (a restarted node is **not ready** until views are rebuilt)

A node runs a lifecycle **`RECOVERING → REBUILDING_VIEWS → READY`** and gates participation on it:

- **RECOVERING** — recover the local log + consensus metadata (existing broker recovery). Not election-eligible, not read-serving.
- **REBUILDING_VIEWS** — run §9.4 steps 1–3 for every hosted (view, partition): load checkpoint, replay `(O, localCommittedTail]`. **Folding never touches an unseeded view** (snapshot load strictly precedes any fold — the node never applies event `@X` onto a view that has not loaded `{O}` and replayed `(O, X]`). Still not election-eligible, not read-serving.
- **READY** — only once **every** hosted view's fold cursor has reached the committed tail. Only then does the node become election-eligible and serve `GetViewRow` / acknowledge `OnViewApply`.

**This gate is guaranteeable and deadlock-free** because REBUILDING uses only local on-disk state (checkpoint + local committed log) — no dependency on the cluster — so it always makes progress to READY on its own, even on a cold all-node restart, and it is **bounded** (replay length = events since the last checkpoint, kept small by the ~T-second flush cadence). Candidacy and read-routing check the local READY flag.

The gate composes with consensus's existing rule that a leader must hold a current log: a node **leads a partition — and thus serves its view reads — only when both its log is current (consensus) and its views are folded up to that log (readiness)**. Log catch-up may proceed as a passive learner during REBUILDING (so a behind node can get the records it needs), but **view reads, view-apply acks, and view-read leadership are hard-gated on READY** — a half-built view is never observable. The only recovery floor: log retention must not delete below the oldest usable checkpoint's `O` (else fall back to a full re-fold from the retained log start — slower, still correct).

### 9.5 RESOLVED (2026-10-09) — a dedicated per-node view-snapshot IO thread

**Decision:** the background serialize+write runs on a **dedicated per-node "view-snapshot" IO thread** — its own `AgentRunner` (like `AdminWorker`), owning only view-snapshot serialization. Clean separation of concerns at the cost of one more thread per node. Rejected: extending `LogCleanerAgent` into a general "background storage maintenance" thread — the cleaner is compaction-focused and already busy pre-provisioning segments, so mixing view-snapshot IO would widen its remit.

The **mechanism** is unchanged by this choice: the worker copies the batch's dirty chunks into a ping-pong handoff buffer and rings it over (§9.2) → the view-snapshot thread appends the `{O, chunks, CRC}` delta and later merges the base → SPSC ring ack → replay-from-checkpoint behind the readiness gate on restart (§9.4).

### 9.6 Rejected alternatives (recorded)

- **Redis `fork()` + COW** — rejected. Redis forks safely because it is single-threaded; rune-broker is many threads + Aeron shared memory + mmaps + open fds + io_uring. `fork()` here is unsafe (whole-address-space COW, fd/lock hazards).
- **Persistent/immutable view table (HAMT/CHAMP) for O(1) snapshot** — rejected for steady state. Copy-free snapshot, but per-update indirection + allocation on every fold fights the flat index+slab layout (§7) and #4. The flat regions + batch-boundary differential checkpoint (§9.2) win for an ultra-low-latency fold.
- **mmap'd file-of-record (mutate the mapped view file in place; rely on `msync` for persistence)** — rejected. `msync` is page-granular and **not** atomic or offset-consistent (the OS may flush dirty pages at any time; a crash mid-flush leaves a torn file matching no clean `appliedOffset` — violates §9.3), and a mapped hot region risks **page-fault latency tails** incompatible with the §14.0 budget. The live view stays in RAM; persistence is the explicit batch-boundary differential checkpoint (§9.2).
- **Stop-the-world full-view copy at a periodic cut** — rejected (spike-measured): an O(view-size) synchronous copy on the worker is 0.9–4.2 s for large views and blows the read budget; even an incremental COW sweep adds worker bookkeeping. The batch-boundary differential flush (§9.2) copies only a batch's dirty chunks at a quiescent boundary — no COW, no full copy, no pause.
- **Log-structured / FASTER HybridLog for the view store** — considered (off-worker O(1) seal checkpoints) but not adopted: it trades in-place updates for append+RCU+compaction and multi-version garbage. The slab + batch-boundary differential checkpoint keeps in-place updates and still gets off-worker IO with no full copy. (Revisit only if snapshot offloading ever dominates.)
- **Separate changelog topic** — unnecessary; the source log already is the changelog. (The §9.2 delta log is a *physical chunk* changelog for fast restart, not a re-derivation from source events.)

### 9.7 Persisted (default) vs transient views

View persistence is a **per-view property**, set at `DefineView`, stored in the `__views` registry entry (§10). **Persisted is the default.**

- **Persisted** — checkpointed per §9.2 (dirty tracking + batch-boundary differential deltas + base). Restart loads the checkpoint `{O}` and replays `(O, LSO]` (§9.4) — fast, bounded. Costs steady-state checkpoint IO + disk.
- **Transient** (keyword; "not persisted" — mirrors Java's `transient` = not serialized, and avoids the `volatile` C++/Postgres collision) — **no checkpoint at all**: no `dirty` tracking (slightly cheaper hot path — no bit-set per write), no deltas, no base, no view-snapshot IO, **zero disk footprint**. It loses nothing, because durability is the replicated log's job (§9.2), not the view's. On restart a transient view is **rebuilt by replaying the source from its retained floor to `LSO`** — the cost is **bounded by the source's retention, not "all history"**: a short-retention or `InMemoryLog`-backed (transient-topic, see the transient-topics spec) source replays little or, after a full restart that emptied an in-memory source, **nothing (instantly ready/empty)**. Only a transient view over a long-retention *persisted* topic would replay a lot — a pairing to avoid.

The **readiness gate (§9.4.1) applies to both** — the node stays not-ready for a view until it is rebuilt to the committed tail; for a transient view that rebuild is a (usually cheap, sometimes empty) replay instead of checkpoint+tail. Typing is identical for both (§3.1/§12): a transient view still has a registered output `Descriptor` + fingerprint. Transient affects only the persistence path.

Transient suits views that are cheap to recompute, short-window, or whose source is itself transient — trading a disk checkpoint for zero persistence IO.

## 10. Control plane — `DefineView`, `__views` registry, AdminWorker

Mirrors the 0b `DefineTopic`/`__topic_schemas`/`AdminWorker` design (shared-nothing DDL on a dedicated thread, off the hot path):

- **`DefineView`** wire op: carries the authoring `ViewPlan` (string field names, §6). On the **AdminWorker** (never a produce worker) it fetches each source topic's `Descriptor` from `__topic_schemas` and **resolves + validates** (§6.0): every `srcField`/`key` name exists in the source schema, merge rules are numeric-only where required, output types resolve. The output is the `CompiledViewPlan` with resolved `(topicId, tag)` references, **plus a derived output `Descriptor`** — the view's row layout (the `output` columns, canonical ascending-tag order), each output column carrying a durable never-reused `(viewId, tag)`.
- **`__views` is a versioned schema registry** (reuses the `SchemaRegistry` shape verbatim, parallel to `__topic_schemas`). Each view version stores: the authoring `ViewPlan` (source of truth for rebuild), the **materialized output `Descriptor` blob**, its **fingerprint** (`fingerprintAsOf`), `nextTag`, and `compatMode`. Keyed `(viewId, version)` so compaction preserves history. Replay on boot reconstructs every view and every retained version.
- **`openView` handshake (identical to `openTopic`).** A typed client presents `RowT::kFingerprint`; the broker matches it against the view's registered schema versions and returns the matching version's materialized descriptor, which the client re-verifies before binding (§12.1). **No match → `SchemaMismatch` at open** — a drifted client is rejected up front, never served a wrongly-shaped row (contract §3.1). Prepared-query result fingerprints (§12.3) are likewise computed against the view's schema version, so the same guard covers ad-hoc queries.
- **Persistence property (persisted default | transient).** `DefineView` carries a `persistence` flag stored in the `__views` entry (§9.7). Rune QL: `DEFINE TRANSIENT VIEW …` (keyword) or `DEFINE VIEW … WITH (persistence = transient)`; programmatic builder `.transient()`. A transient view installs no checkpoint path on its worker (no dirty tracking / no view-snapshot IO).
- **`ALTER VIEW` (add/drop/rename output column, change a merge rule, add a source)** runs on the **same AdminWorker-serialized DDL path as `ALTER TOPIC`**: single in-flight, `expectedVersion` CAS, atomic multi-op, contiguous versions; output columns get fresh never-reused `(viewId, tag)`s; each alter bumps the view version + fingerprint. **The one difference from a topic alter:** a topic alter is pure metadata, but a view alter that changes **fold semantics** (new aggregation, changed merge, new source) changes the view's *contents* → it triggers a **background rebuild** — replay the source log through the new fold (§9.4 replay-from-checkpoint) into the new version while the **old version keeps serving reads** until the new one catches up to the live offset, then cut over. Output-only/additive changes that expose already-maintained substate skip the rebuild. **Phasing (decision §15 #13):** versioned view schemas + `openView` handshake land in the first cut; `ALTER VIEW` + live rebuild is the immediately-following increment (the rebuild/cut-over is the one genuinely new mechanism and earns its own tasks + tests).
- **Distribution.** On a successful `DefineView`/`ALTER VIEW`, the AdminWorker publishes the `CompiledViewPlan` + output descriptor + subscription edges to each worker's Reactor **via ring** (no shared registry read on the hot path). Each worker installs the plan into its per-worker subscription index (§4.1) for the partitions it owns.
- **Multi-node replication of `__views`** inherits the same deferral as 0b's `__topic_schemas` (RULING-4): single-node authoritative now; admin-owned transport replication is a follow-on.

## 11. Fold ownership & deterministic convergence

- The worker owns the partition's log on **both roles**: leader (produce → `appendNoSyncRestamp`) and follower (`ApplyReplicated` → `applyReplicated`), both on the worker thread.
- **The batch is the worker's unit of work.** Each duty-cycle step folds a **bounded batch** of committed records `(cursor, batchEnd]` (bounded per cycle; aligned to how the log/Aeron delivers records in fragments), and the batch boundary is the **single consistent point** everything keys off: the fold cursor / `appliedOffset` advances to `batchEnd` **once per batch** (not per event); the view-apply watermark (§11.1) advances per batch; and the differential checkpoint flushes that batch's dirty chunks (§9.2). One boundary drives cursor-advance, apply-ack, and persistence — and because the worker is single-threaded and quiescent at that boundary, the state there reflects exactly `(…, batchEnd]`, which is what makes the checkpoint copy COW-free (§9.3).
- The worker folds its **own committed, read-committed log `[cursor, LSO)`** in its duty cycle, **bounded per cycle**, off the produce critical path. **The fold cursor advances to the `LSO` (last stable offset), not raw `HW`** — derived state must be read-committed: an aborted transaction's records must never materialize, and an in-flight transaction must not leak into a view. Records inside an open transaction fold only once its **commit marker** lands and the LSO crosses them; an abort marker skips them entirely. (A non-transactional record's LSO == its commit offset, so the common path is unchanged.) A batch boundary is always placed at an LSO/commit boundary, never mid-transaction.
- Because the fold is a **pure function of the committed log prefix**, leader and every follower converge to the **identical view table at the same offset**, independent of timing (#16). Failover is ~free: the new leader already has the folded view (or rebuilds it deterministically from its replicated log via §9.4).

### 11.1 View-apply acknowledgement — durability × apply-point (per-produce)

A produce (or transaction commit) carries **two orthogonal acknowledgement axes**, selected **per-produce** (exactly like `acks` today):

- **Durability (existing):** `None` (fire-and-forget) / `Leader` (`acksAll=false`) / `Majority` (`acksAll=true`, the ISR quorum). *How many log copies hold the record before the ack.*
- **Apply-point (new):** `OnCommit` (**default** — ack as soon as durability is met, before views) / `OnViewApply` (ack only after **every** view that depends on this event has folded it). *Whether the ack waits for derived state.*

`OnViewApply` is **scoped to the durability level**, which is exactly the leader-vs-majority distinction:
- `Leader + OnViewApply` → the **leader's** dependent views have applied this offset.
- `Majority + OnViewApply` → the **same ISR/majority quorum** that satisfied durability has folded it into their view replicas.

`OnViewApply` is monotonic over durability (you cannot fold what is not committed), so it is always "durable **and then** applied," never weaker. `None + OnViewApply` is invalid (no ack to delay) and rejected at the API boundary.

**API shape (elegant: durability is the base, applying-after-views is a modifier).**
```cpp
Ack::none()                   // fire-and-forget
Ack::leader()                 // legacy: leader-durable, ack before views
Ack::leader().afterViews()    // leader-durable AND the leader's dependent views applied
Ack::majority()               // legacy: majority-durable, ack before views
Ack::majority().afterViews()  // majority-durable AND dependent views applied across the ISR quorum
```
One value `Ack{ Durability durability; ApplyPoint applyPoint = OnCommit; }`; `.afterViews()` flips `applyPoint`. A produce that fans out to several dependent views waits for **all** of them. Leaves room for future apply-points (`.afterIndexed()`, …) without a new top-level enum.

**Mechanism (shared-nothing, ring-only — #1/#2).** Each `(view, partition)` fold publishes an **apply-watermark** = "view *V* applied through source offset *O*" (monotone). The produce ack is held pending until the required watermarks reach the record's commit offset:
- **Leader scope:** the leader waits until every dependent view's apply-watermark ≥ `O`. Views may live on other workers of this node; their watermarks reach the ack site over a ring (no shared mutable state, no cross-thread callback).
- **Majority scope:** each follower piggybacks its view-apply watermark on the **existing replication ack feedback** — no new transport. The leader acks when the ISR quorum's view-apply watermarks ≥ `O`.

**Cost (honest, opt-in).** `OnViewApply` adds up to one fold duty-cycle of latency (bounded per cycle), and for `Majority` a follower fold + feedback RTT. It is a per-produce read-your-writes-on-views guarantee, never the default, so the `OnCommit` fast path carries zero new cost.

**Transactions.** The apply-point of a **transactional** produce is its **commit marker**, not the individual records. Because the fold only crosses a transaction at its commit marker (LSO, above), a view never shows a *partial* transaction — it jumps from "none of the txn" to "all of it." So `commitTransaction(Ack::majority().afterViews())` means: the commit marker is majority-durable **and** the dependent views across the ISR quorum reflect the **whole** committed transaction before the commit returns — EOS/all-or-nothing extended to derived views, composing with the existing D9 cross-node markers. The composed guarantee is: **durability(commit marker) × apply(dependent views folded past the commit marker, at the durability scope).**

## 12. Read path & query-result typing

### 12.0 Routing — `GetViewRow`

Route a read **exactly like a produce**, so no new routing plane is introduced:

1. `partition = hash(key) % N`.
2. Resolve the leader via `RoutingImage`.
3. On the node, resolve the owning worker via `ShardRouter`.
4. Enqueue the read op via `WorkerPool::tryEnqueueOp` (same mechanism as produce).
5. The worker does a **point lookup** in its `viewTable` (§7) and returns the row bytes.

No cross-thread read of the table, no lock — the read executes on the owning worker (#1). For a join view, the read routes on the shared key to the single co-partitioned worker (§8).

### 12.1 A view is a read-only typed topic

A view's result is a structured record in its **output `Descriptor`** layout (§10), encoded with the same SBE-style header every record carries — `[blockLength:u16][version:u16] …` — version stamped like `TypedTopic::send` already does. So a client consumes a view exactly like a read-only typed topic: `openView<RowT>("…")` performs the `openTopic` fingerprint handshake (§10), binding the version whose fingerprint equals `RowT::kFingerprint`.

### 12.2 Projection = a derived typed subset (identity = `SELECT *`)

Selecting a subset of columns yields a **projection**, itself a well-defined typed layout: the chosen `(viewId, tag)`s in canonical ascending-tag order. Its fingerprint is a deterministic function of `(view schema version, ordered tag set)`, **salted by the view's full-schema fingerprint** so two views' same-named projections never collide. The broker serves a projection by copying just the selected fixed-width fields from the stored flat row into a packed buffer (O(selected fields), no full deserialize) and stamping the projection fingerprint/version. `SELECT *` is the **identity projection** — its fingerprint equals the view's full-schema fingerprint — so full-row and projection are one mechanism, not a special case.

### 12.3 Query-result typing — two tiers, no untyped path (contract §3.1)

Queries on a view are formed **at runtime**, so a client cannot in general bind a codegen'd type. The result layout therefore comes from the **broker at prepare time**, the runtime analog of codegen:

- **Prepared (generic, runtime).** The client builds a query programmatically (a value, not SQL text) and `prepare`s it. The broker compiles it against the view's registered schema into a **result `Descriptor` + fingerprint `Fr`** — reusing the same `toDescriptor → serialize → fingerprint` machinery `DefineTopic` uses — and returns it in the prepare response. The client resolves typed **column handles once** (`col<i64>("customers")` → `{offset,type}`, type checked here, throwing at prepare on a type/column error), then iterates results: each row's accessor construction does the **one O(1) fingerprint+length gate** against `Fr`, after which `r.get(handle)` is a bare positional read at SBE speed. If the view is altered between prepare and execute, `Fr` changes, the stamped result no longer matches, and the gate **throws** (client re-prepares) — never a wrong value.
- **Typed (static optimization).** A query known at build time is a `TypedView<RowT>` whose `kFingerprint` the client computes at codegen. If it matches the broker's result fingerprint the client may skip the prepare round-trip and use fully-inlined getters. Same wire format, same stamp, same gate.

```cpp
auto q = query(view("rev_by_customer"))
           .select(col("customerId"), count().as("customers"))
           .where(col("totalAmount").gt(10))
           .groupBy("customerId");          // a VALUE; SQL compiles to the same plan
PreparedQuery pq = client.prepare(q);        // broker → result Descriptor + fingerprint Fr
auto cCid = pq.col<i64>("customerId");       // resolve offset+type ONCE (throws at prepare on mismatch)
auto cCnt = pq.col<i64>("customers");
for (Row r : pq.execute())                   // Row{} GATE: stamped==Fr && len ok → else throw SchemaMismatch
    use(r.get(cCid), r.get(cCnt));           // positional, SBE-speed, no per-field check
```

### 12.4 The decode gate — "checked construction, unchecked access"

SBE speed comes from bare positional reads; its danger is that the wrong buffer yields silent garbage. We keep the bare reads but make them provably safe by a single gate the type system makes **unskippable**: a result accessor (`RowView<RowT>` or `Row` over a `PreparedQuery`) **cannot be constructed** without the `[blockLength][version]`/fingerprint check passing. Match → SBE-speed positional reads; mismatch → `SchemaMismatch`, never a fall-through read. The flat `FixedStr` layout (§7) keeps even string fields fixed-width inline, so the single length check bounds the whole row — no offset-chasing that could run off the end.

*(The richer read-query surface — read-time `WHERE`/`GROUP BY`/aggregation beyond the view's maintained merge — is a deferred query engine (§2, §16); the result-typing + decode mechanism above is identical regardless of a query's complexity.)*

## 13. Data flow walkthrough (event → updated view)

End-to-end, single source (the join generalization is the §8 gather):

1. Producer appends a batch to topic `T` partition `p`. Leader worker `W` (owner of `T/p`) commits `[cursor, HW)` on the produce path.
2. In `W`'s **duty cycle** (not the produce ack path), `W`'s Reactor looks up `subscriptionIndex[(T, p)]` → interested views.
3. For each interested view `V`, `W` runs `foldRecord` (§6.1) over the newly committed records, bounded per cycle: project → per-field merge into `substate` → `reemit(k)` → write `viewTable[k]` (or erase on tombstone).
4. `W` advances `V`'s fold cursor to the batch boundary and, at that boundary (§9.2), copies the batch's dirty chunks into a handoff buffer `{O}` and rings it to the view-snapshot IO thread, which appends the delta atomically.
5. A `GetViewRow(V, k)` routes to `W` (§12) and point-looks-up the current row.
6. On restart/failover: load latest snapshot, replay `[O+1, HW)` through the same fold (§9.4) → identical view.

## 14. Testing & benchmarks (binding — #5, #6, #16)

### 14.0 Latency NFRs (HARD GATES — the reason this engine exists)

These are binding pass/fail budgets, measured firsthand under default config (ABAB, n ≥ 15, report p50 **and** p99; a gate fails if p99 exceeds the budget). They drive the design (flat index + row-id slab with fat rows out of the probe path, O(1) by-tag decode, zero steady-state allocation, shared-nothing fold):

| Path | Budget | Notes |
|---|---|---|
| **View update from an event batch** (fold: decode → project → per-field merge → viewTable upsert) | **< 10 µs / batch** | steady state, zero-alloc; off the produce ack path |
| **Point select spanning a single node** (`GetViewRow`: route → worker point lookup → reply) | **< 50 µs** | end-to-end incl. in-node hop |
| **Query spanning multiple nodes** | **< 200 µs** | end-to-end incl. cross-node fan-out/gather (query surface; join views) |

The schema-evolution prerequisite's decode path (`readField`-by-tag against a materialized per-version descriptor) is part of the <10µs fold budget — its own plan carries a derived sub-budget so it cannot erode this.

- **Unit:** `mergeField` per rule × type; projection select/rename; tombstone erase; open-addressing index probe/erase(backshift)/incremental-resize; row-id free-list alloc/recycle; re-optimize compaction.
- **Fold determinism:** same log prefix → identical table byte-for-byte, across orderings that preserve per-key commit order; leader vs replica table equality at equal offset.
- **Persistence round-trip:** snapshot → kill → load+replay `[O+1, HW)` → table equals the never-restarted table (time-independent, #16). Crash-mid-write leaves prior complete snapshot usable (CRC fallback). Offset in artifact exactly matches table.
- **Shared-nothing audit:** no other thread touches a live `viewTable`; worker→IO handoff is ring + immutable snapshot only (extend the existing no-atomics / single-driver checker scripts).
- **Apply-point acknowledgement (§11.1):** (a) `OnCommit` acks before the fold runs (ack offset ≤ any view apply-watermark) — and its latency is **unchanged** vs today's `acks` (ABAB regression, #12). (b) `Leader + OnViewApply` ack arrives only after every dependent view's apply-watermark ≥ the record offset (fan-out to multiple views waits for all). (c) `Majority + OnViewApply` ack arrives only after the ISR quorum's view-apply watermarks ≥ the offset; a non-quorum of applied followers does **not** ack. (d) **Transactional:** with `commitTransaction(... .afterViews())`, a `GetViewRow` observed by any acked reader shows **all-or-nothing** of the txn — never a partial transaction; an aborted txn never appears in the view (read-committed/LSO, §11). (e) `None + OnViewApply` is rejected at the API boundary.
- **E2E (real Aeron, default config):** define a view over a topic, produce, `GetViewRow`, restart broker, confirm view recovered to latest — firsthand green before "done" (#16), via a `docker/view-engine-e2e.sh` mirroring 0b/0c e2e scripts.
- **Benchmarks (gated against §14.0):** fold update latency per event batch **< 10 µs** p99 (zero-alloc verified); `GetViewRow` single-node **< 50 µs** p99; multi-node query **< 200 µs** p99; plus the per-batch dirty-chunk copy (∝ batch churn, not view size) + the hot-path `dirty` bit-set, and produce-path regression (fold off the critical path must not move produce latency — ABAB n≥15, #12). A benchmark that exceeds its §14.0 budget is a failing gate, not a note.
- **Typed result decoding & safety contract (§3.1, §12):** (a) **decode speed** — `RowView`/`Row` field access matches hand-written SBE positional access within noise (benchmark, zero-alloc); (b) **`SchemaMismatch` on wrong structure** — a result stamped with a non-matching fingerprint/version (view altered between prepare/execute; buffer from a different view/topic; truncated buffer failing the length check) **throws at accessor construction**, never returns a value (regression); (c) **`openView` handshake** — a client whose `kFingerprint` matches no registered version is rejected at open, and one matching a retained older version binds that version; (d) **type check at handle acquisition** — `col<i64>()` on a non-i64 column throws at `prepare`, and the per-row `get(handle)` path does no per-field check; (e) **projection** — `SELECT`-subset fingerprint is canonical-order + salted (two field orders dedupe to one `Fr`; `SELECT *` fingerprint == full-view fingerprint); (f) **no untyped escape hatch** — there is no accessor returning an unchecked/any value (audit).
- **`ALTER VIEW` rebuild/cut-over (when that increment lands):** an alter that changes fold semantics rebuilds the new version from the source log while the **old version keeps serving** reads correctly until the new version reaches the live offset, then cuts over atomically; a reader bound to the old version is never served a wrongly-shaped row across the cut (regression).
- **Storage model — rehash within budget (§7):** under a mixed create/update/delete workload that crosses several load-factor thresholds, the **worst per-op rehash tax stays < 50 µs, independent of row size** (64/256/1024 B) — `startGrow` is an O(1) swap of a background-pre-provisioned buffer (no inline allocation on the triggering op; **zero fallbacks** under the expected insert rate), and incremental migration moves ≤ `MIGK` index entries/op. A stop-the-world rehash on the hot path is a **failing gate**. Also: delete is O(1) tombstone-free (p99 flat across row sizes, no payload move); point-find is cache-dense (fat rows out of the probe path); snapshot is a memcpy of flat index+slab regions + atomic marker (no mmap-of-record).
- **Checkpoint — batch-boundary differential (§9.2, spike-validated):** with **row-sized chunks** and a **~1–2 s / `dirtyCap` flush cadence**, read latency with checkpointing ON must stay **on the OFF baseline through p99.9** (measured: `125/292/375 ns` ON vs `125/291/375 ns` OFF) — the per-flush copy is bounded and touches only p99.99+ reads. **No full-view copy, no COW, no memory doubling** (handoff buffers + `dirty` bitmap only). **Chunk = row-size is mandatory** — an OS-page chunk (4 KB) is 4–16× write amplification that saturates IO and stalls the worker (a failing config, spike-shown). **Aggregate dirty rate across a node's view-partitions must be ≤ disk bandwidth** or backpressure stalls the worker — gate the dirty rate (cadence/cap/coalescing) against the target disk. **Offset-atomicity:** every delta carries `O`+CRC at a batch boundary; a crash-injection/replay test shows the recovered offset is always exactly the last fsync'd delta's `O`, and replay `(O, LSO]` reproduces the byte-identical view (deterministic, #16).
- **Restart readiness gate (§9.4.1):** a node is **not election-eligible and does not serve `GetViewRow` / ack `OnViewApply`** until every hosted view is loaded + replayed to the committed tail (`RECOVERING → REBUILDING_VIEWS → READY`); a fold never touches an unseeded view; the gate reaches READY using only local state (no cluster dependency — test a cold all-node restart) within a bound set by the checkpoint cadence. A half-built view must never be observable (regression).
- **Regression suite:** every fix gets a `*Regression*` test; full regression suite runs first on every change (#6/#7).

## 15. Decisions

**Load-bearing decisions #1, #7, #9 resolved 2026-10-09** (bodies updated in §9.5, §6.0, §9 below). Items #2–#6 carry sound proposed defaults and are confirmed at plan time; #8 was already resolved into the prerequisite spec.

1. **§9.5 — snapshot-IO thread ownership (RESOLVED):** a **dedicated per-node view-snapshot IO thread** (own `AgentRunner`). Rejected: extending `LogCleanerAgent`. See §9.5.
2. **Snapshot trigger policy:** time *T* only, offset-delta *N* only, or both (whichever first)? Default proposal: both, whichever fires first, with *T* and *N* configurable per view.
3. **Snapshot granularity:** one artifact per (view, partition) (proposed) vs one per view across its partitions. Per-(view,partition) matches worker ownership and parallel IO; confirm.
4. **Very large views (RESOLVED into §9.2):** no stop-the-world bulk-copy at any size — persistence is the **batch-boundary differential checkpoint** (copy only a batch's dirty chunks), so cost scales with churn, not view size. The "accept the pause / defer chunked" proposal is retired.
5. **Slab compaction timing (folded into §7 re-optimize):** delete holes are recycled via the row-id free-list on the hot path (no inline compaction); physical hole reclaim + hot-row page clustering + dense index rebuild happen at the deliberate **re-optimize** pass (restart / on-request), never inline.
6. **`DefineView` field defaults:** default `MergeRule` = `Replace`; key field must appear in `output`; reject non-numeric `Sum/Min/Max` at define time — confirm.
7. **Field-name uniqueness enforcement point (RESOLVED — fix-on-sight, rule #8):** enforce duplicate-name rejection at schema **registration** — `DescriptorBuilder::build()` (codec) and broker `DefineTopic` — so every consumer (views, typed clients, QL), not just Rune QL's parser (`Parser.cpp:241`), is protected and `Descriptor::find()`'s first-match is always safe. The view resolver additionally asserts uniqueness defensively. This codec/broker enforcement lands as part of this work (a small, bounded scope addition), with its own `*Regression*` test.
8. **Durable field tags vs positional ids (RESOLVED — promoted to prerequisite spec):** `(topicId, fieldId)` identity requires `fieldId` to be a durable never-reused tag. This is now its own prerequisite foundation sub-project: **`docs/superpowers/specs/2026-10-08-rune-schema-evolution-durable-tags-design.md`** (durable numeric per-topic tags + versioned descriptor + compatibility modes + `ALTER TOPIC`). The view engine **depends on** it and lands after it (**prerequisite SHIPPED 2026-10-09** — see header). The view spec's field identity is concretely `(topicId, tag)` from that spec; this spec's §6.0 is realized there.
9. **View-vs-schema-evolution compatibility policy (RESOLVED):** **graceful-absent at runtime + validation at `DefineView` time.** The schema layer stays one-way independent — a DROP of a view-bound tag degrades the tag to **absent** rather than being blocked at the schema layer (prerequisite spec §11, decision #6). On top of that, the view side:
   - **At `DefineView`:** the AdminWorker validates every referenced `(topicId, name→tag)` against the source topic's current Descriptor and **rejects** obvious errors — a name that does not resolve, or a tag already dropped at the bound version. (A later-defined view over an already-evolved topic cannot silently reference a field that no longer exists.)
   - **At runtime:** a field dropped by a *later* `ALTER TOPIC DROP` degrades to **null/absent** in the view column for records written after the drop; the fold never wedges and the view stays coherent (`(topicId, tag)` identity + `isLiveAt` per writer version). No view invalidation, no coupling of `ALTER TOPIC` to view lifecycle.
   - Retype is treated as drop-old-tag + add-new-tag at the schema layer, so a view bound to the old tag sees it go absent (same runtime rule); a view wanting the new field is redefined to reference the new tag.

10. **View-apply acknowledgement (RESOLVED 2026-10-09, §11.1):** two orthogonal per-produce axes — Durability `{None, Leader, Majority}` × Apply-point `{OnCommit (default), OnViewApply}` — with `OnViewApply` scoped to the durability level (leader-local dependent views, or the same ISR/majority quorum), selected per-produce via `Ack::…().afterViews()`. `None + OnViewApply` invalid. Fold is read-committed (`[cursor, LSO)`); a transactional write's apply-point is its commit marker, giving all-or-nothing view visibility (EOS extended to derived state).

11. **Views are typed; results are typed (RESOLVED 2026-10-09, §12, §3.1):** a view has a derived, registered, **versioned output `Descriptor` + fingerprint**; a client consumes it as a read-only typed topic (`openView` fingerprint handshake). Query results are typed via two tiers — **prepared** (broker returns a result descriptor + fingerprint at `prepare`; generic `Row` + `col<T>()` handles) and **static** (`TypedView<RowT>` codegen as an opt-in fast path) — both passing one checked-construction fingerprint+length gate (SBE-speed reads, hard `SchemaMismatch` on mismatch). Projections are derived typed subsets (canonical tag order, salted fingerprint; `SELECT *` = identity). No untyped access path.
12. **Typing & safety contract (RESOLVED 2026-10-09, §3.1):** the seven binding invariants (verified-before-decode, durable-tag identity, no implicit coercion, explicit typed absence, queries-as-values, drift-caught-up-front, deterministic read-committed + all-or-nothing visibility) — strictly stronger than a typical RDBMS client. §14 pins each.
13. **`ALTER VIEW` phasing (RESOLVED 2026-10-09, §10):** versioned view schemas + the `openView` handshake land in the **first cut**; `ALTER VIEW` + **live background rebuild/cut-over** is the **immediately-following increment** (its own tasks + tests). Views alter on the same AdminWorker DDL path as topics; the rebuild-on-fold-semantics-change is the only genuinely new mechanism.

14. **View-table storage model (RESOLVED 2026-10-09, §7 — benchmark-validated):** compact open-addressing **index** (1-byte tag + key + dense row-id; Swiss/F14-style tag probe, fat rows kept out of the probe path) over a **stable row-id slab** (`slab + rowid*rowLen`, fixed `rowLen`). Deletes clear the index slot + free the row-id (O(1), tombstone-free, no payload move). Growth is **de-amortized**: incremental migration of ≤ `MIGK` index entries/op (slab shared, never copied) + a **background-pre-provisioned** next index buffer so `startGrow` is an O(1) swap → worst per-op rehash tax < 50 µs, flat across 64/256/1024 B. Snapshot = memcpy of flat index+slab + atomic `{offset,CRC}` marker (in-RAM, **not** mmap-of-record — page-fault tails + non-atomic `msync`). Rows move only at a deliberate **re-optimize** pass (hole reclaim + hot-row page clustering + index rebuild). Rejected: inline-row combined buffer (resize scales with `rows×rowSize` → 0.9–4.2 s stop-the-world spikes); tombstone deletes (rot/compaction); mmap-of-record. Validated by a standalone spike (§7.1).

15. **Checkpoint model (RESOLVED 2026-10-09, §9.2 — spike-validated):** batch-boundary **differential** checkpoint — `dirty[C]` bit (set in the fold write-path) over **row-sized chunks** (page-sized chunks rejected: 4–16× write amplification, spike-shown); flush on `min(~1–2 s interval, dirtyCap)` (both configurable) at a batch boundary — copy only the interval's dirty chunks (coalesced: a hot row written once per flush) into a ping-pong handoff buffer tagged `{O}`, ring to the dedicated view-snapshot IO thread, which appends an `{O, chunks, CRC}` delta with grouped fsync; base + periodic compaction. Offset-atomic (exact `O`, never ±1), no full-view copy, no COW, no memory doubling; reads unmoved through p99.9 (measured). Constraint: node disk bandwidth ≥ aggregate dirty rate across its view-partitions. Rejected: stop-the-world full copy, incremental-COW sweep, mmap-of-record, log-structured/FASTER store, OS-page chunk granularity (§9.6).
16. **Restart readiness (RESOLVED 2026-10-09, §9.4.1):** `RECOVERING → REBUILDING_VIEWS → READY`; a node is not election-eligible and serves no view reads / view-apply acks until every hosted view is loaded + replayed `(O, committedTail]`; folds never touch an unseeded view. Guaranteeable + deadlock-free (local-only rebuild) + bounded (checkpoint cadence). Composes with consensus's current-log rule so a partition is led/served only when log *and* views are current.

17. **View persistence property (RESOLVED 2026-10-09, §9.7):** per-view `persistence ∈ {persisted (default), transient}`; **transient** = no checkpoint / no persistence IO / zero disk (durability stays the log's job), rebuilt on restart by replaying the source from its retained floor (cheap/empty for bounded or `InMemoryLog`-backed sources — not inherently slower). Word is **`transient`** (not `volatile` — avoids the C++/Postgres collision; matches Java `transient`). QL `DEFINE TRANSIENT VIEW`. Typing + readiness gate unchanged. Pairs with **transient topics** (own spec: `2026-10-09-rune-transient-topics-design.md`).

## 16. Deferred follow-ons (program-level)
- **Read-time query engine** — `WHERE` / `GROUP BY` / aggregation *beyond* the view's maintained per-field merge: the Filter/Aggregate operators (§5) applied at read time (the §12.3 example's shape). The result-typing/decode mechanism is already defined; the query *operators* are the follow-on.
- **SQL → programmatic-plan translator** — SQL compatibility as a front-end that emits the same query-builder plan (Rune QL stays programmatic-first).
- **Formal BACKWARD/FORWARD compatibility modes for view output schemas** — beyond the versioning + fingerprint-handshake guard (which suffices for correctness now).
- **Join / multi-source views** (§8) — own spec: co-partitioning, gather semantics, outer/inner policy.
- **Secondary indexes & range scans** over the view table.
- **Rune QL `create view …`** surface over `DefineView` (thin, mirrors 0c).
- **Multi-node `__views` replication** + view placement/failover policy (inherits RULING-4).
- **Chunked/COW snapshot** for very large views (§9.6).
