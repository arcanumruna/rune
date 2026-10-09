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
- **Zero steady-state allocation (#4).** The view table is a preallocated open-addressing flat table with inline fixed-width rows (the `FixedStr` payoff from 0c) + a reused arena for variable-width payloads. The fold reuses buffers. Snapshot buffers are ping-pong-reused, not per-snapshot allocated.
- **No virtual calls on the hot path.** The fold dispatches per-field merge by a compiled `MergeRule` enum + `switch`, not virtual operator objects (§6). The dataflow "operators" are a *design* abstraction compiled to a flat plan, not a runtime object graph on the fold path.
- **Off the produce critical path.** The fold runs in the worker's duty cycle over its own committed log `[cursor, HW)`, bounded per cycle — never on the produce ack path. Serialize + fsync runs on a background thread, never the worker (§9).
- **Deterministic / time-independent convergence (#16).** The fold is a pure function of the committed log prefix; replica and leader converge to the identical view table at the same offset regardless of timing (§11).
- **Test + benchmark everything (#5), regression-test every fix (#6), fix-on-sight (#8).** §14.

> **Review note:** confirm this section is complete against the canonical 16-point philosophy; add any point you want explicitly gated here.

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

On commit of `[cursor, HW)` for a partition, the worker's Reactor looks up the interested views and drives each view's fold over the newly committed records. The source topic carries the list of its subscriber views; the view does not scan topics. This keeps the dispatch O(interested views), co-located on the worker that already owns the log — no cross-thread hop to update a view.

> **Why this shape (recorded from design dialogue):** we do not make the *view* know its sources for pull; we make the *source* know its subscriber views for push. A reactor dealing in sinks collects the interested views on commit and applies the plan's transformations/merges/projections. For a join, the same index simply maps one worker's co-partitioned sources to the shared view (§8).

## 5. Operators (foundation set + extension points)

| Operator | Status | Role |
|---|---|---|
| **Source** | this spec | bind topic + key field; register in subscription index |
| **Project** | this spec | field select/rename = projection into output fields |
| **Fold** | this spec | keyed per-field merge into substate → reemit row |
| **Sink** | this spec | clustered primary index `viewTable` (§7) |
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
- Because 0b schemas are **additive-only with stable field ids**, a resolved `fieldId` stays valid across later additive schema versions — no re-resolution on evolution.

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
for each committed record r in [cursor, HW) for an interested view:
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

## 7. The sink: `viewTable` as a clustered primary index

The `viewTable` for a (view, partition) is a **clustered primary index** keyed by the view's key field — effectively the view's primary storage, not a secondary structure over something else.

- **Open-addressing flat hash table** (not `std::unordered_map`): one contiguous allocation, linear-probe, power-of-two capacity, Robin-Hood or tombstone-aware deletion. Cache-friendly, no per-entry node allocation (#4, #16 compact structures).
- **Inline fixed-width rows.** Fixed-width fields (ints, `FixedStr(N)` from 0c) are stored inline in the slot — the whole point of adding `FixedStr` in 0c. A point lookup is one probe + one memcpy-able row, no pointer chase.
- **Arena for variable-width payloads.** Variable `string` fields spill into a per-table arena; the slot holds (offset, len). The arena is compacted/rebuilt at snapshot or on load, never grown on the hot path beyond amortized reserve.
- **Point-lookup only.** `GetViewRow(viewId, key)` → probe → row. No secondary indexes, no scans in this spec.
- **Memcpy-able bulk cut.** The flat table + arena are designed so a consistent snapshot is a bulk copy of two contiguous regions + the applied offset (§9), not a structure walk.

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

### 9.2 Recommended mechanism (mirrors the `LogCleanerAgent` pattern)

Per (view, partition), owned by the worker:

1. **Consistent cut (worker, cheap).** Periodically — every *T* ms or every *N* applied offsets — the worker captures an **immutable** snapshot object:
   ```cpp
   struct ViewSnapshot {               // immutable once published
       int64_t   appliedOffset;        // fold cursor: the view == source log applied through here
       /* bulk copy of the flat table region + arena region */
   };
   ```
   The worker **bulk-copies** the flat table + arena + current fold cursor into a `ViewSnapshot`, capturing offset and data **together** in one cut, and publishes `std::shared_ptr<const ViewSnapshot>` via `std::atomic_store` (exactly `publishSealedSnapshot`). The worker's only cost is the **memcpy** of two contiguous regions; it keeps folding immediately — the published snapshot is frozen and refcounted alive.
2. **Background serialize (off the worker).** A background snapshot-IO thread (ownership = **dedicated per-node view-snapshot IO thread**, resolved §9.5) takes the published `shared_ptr`, serializes it, and writes it **atomically**: `view-<id>-<partition>.snap.tmp` → `fsync` → `rename` → `fsync` dir, with `appliedOffset` + CRC32C in the header (the `ProducerStateManager`/`LeaderEpochCache` pattern). It touches only the immutable snapshot, never the live table.
3. **Ack / reclaim (ring).** On completion the IO thread signals back over an SPSC ring; the worker drops/reuses the snapshot buffer in its duty cycle. **Ping-pong two snapshot buffers** per (view, partition) so steady-state snapshotting is allocation-free (#4) — the worker refills buffer B next interval while the IO thread may still hold buffer A until its ack.

### 9.3 Atomicity guarantee (the core requirement)

The snapshot file is **one artifact** with `appliedOffset` in its header, written temp-then-rename, so it is all-or-nothing. A crash mid-write leaves the previous complete snapshot intact. Because offset and table bytes are copied together into the immutable `ViewSnapshot` (§9.2 step 1) and never separated, **the file's offset always names exactly the table in it** — never offset-ahead, never data-ahead. ✔

### 9.4 Restart = load + replay-from-checkpoint

1. Load the latest **complete** snapshot `{O, table}` (verify CRC; fall back to the prior complete snapshot if the newest fails verification).
2. Seed the `viewTable` + substate from the snapshot.
3. Replay the **source log `[O+1, HW)`** through the same `foldRecord` (§6.1) — the `rebuildFromLog` scan, but **starting at `O`** (the checkpoint-offset skip that is new, §9.1 gap b).
4. Result: the exact latest view. Deterministic (§11) — the fold is a pure function of the log prefix, so replay reproduces the identical table.

### 9.5 RESOLVED (2026-10-09) — a dedicated per-node view-snapshot IO thread

**Decision:** the background serialize+write runs on a **dedicated per-node "view-snapshot" IO thread** — its own `AgentRunner` (like `AdminWorker`), owning only view-snapshot serialization. Clean separation of concerns at the cost of one more thread per node. Rejected: extending `LogCleanerAgent` into a general "background storage maintenance" thread — the cleaner is compaction-focused and already busy pre-provisioning segments, so mixing view-snapshot IO would widen its remit.

The **mechanism** is unchanged by this choice: immutable `shared_ptr` publish (worker) → background serialize on the view-snapshot thread → atomic temp→`fsync`→rename → SPSC ring ack → replay-from-checkpoint on restart.

### 9.6 Rejected alternatives (recorded)

- **Redis `fork()` + COW** — rejected. Redis forks safely because it is single-threaded; rune-broker is many threads + Aeron shared memory + mmaps + open fds + io_uring. `fork()` here is unsafe (whole-address-space COW, fd/lock hazards).
- **Persistent/immutable view table (HAMT/CHAMP) for O(1) snapshot** — rejected for steady state. Copy-free snapshot, but per-update indirection + allocation on every fold fights the flat inline table (the `FixedStr` design) and #4. The flat table + periodic bulk-copy wins for an ultra-low-latency fold.
- **Separate changelog topic** — unnecessary; the source log already is the changelog.
- **Chunked/incremental COW snapshot** — a future optimization for very large views where even the bulk-copy pause matters; benchmark-gated (#5/#16), not now.

## 10. Control plane — `DefineView`, `__views` registry, AdminWorker

Mirrors the 0b `DefineTopic`/`__topic_schemas`/`AdminWorker` design (shared-nothing DDL on a dedicated thread, off the hot path):

- **`DefineView`** wire op: carries the authoring `ViewPlan` (string field names, §6). On the **AdminWorker** (never a produce worker) it fetches each source topic's `Descriptor` from `__topic_schemas` and **resolves + validates** (§6.0): every `srcField`/`key` name exists in the source schema, merge rules are numeric-only where required, output types resolve. The output is the `CompiledViewPlan` with resolved field ids.
- **`__views`** — a broker-local registry topic (parallels `__topic_schemas`), the durable record of defined views (stores the authoring `ViewPlan`; the compiled form is re-derived from the then-current source Descriptor on install, so additive schema evolution is picked up). Replay on boot reconstructs the set of views.
- **Distribution.** On a successful `DefineView`, the AdminWorker publishes the `CompiledViewPlan` + subscription edges to each worker's Reactor **via ring** (no shared registry read on the hot path). Each worker installs the plan into its per-worker subscription index (§4.1) for the partitions it owns. The source `Descriptor` the compiled plan references is already resident on the worker from 0b per-worker validation.
- **Multi-node replication of `__views`** inherits the same deferral as 0b's `__topic_schemas` (RULING-4): single-node authoritative now; admin-owned transport replication is a follow-on.

## 11. Fold ownership & deterministic convergence

- The worker owns the partition's log on **both roles**: leader (produce → `appendNoSyncRestamp`) and follower (`ApplyReplicated` → `applyReplicated`), both on the worker thread.
- The worker folds its **own committed log `[cursor, HW)`** in its duty cycle, **bounded per cycle**, off the produce critical path.
- Because the fold is a **pure function of the committed log prefix**, leader and every follower converge to the **identical view table at the same offset**, independent of timing (#16). Failover is ~free: the new leader already has the folded view (or rebuilds it deterministically from its replicated log via §9.4).

## 12. Read path — `GetViewRow`

Route a read **exactly like a produce**, so no new routing plane is introduced:

1. `partition = hash(key) % N`.
2. Resolve the leader via `RoutingImage`.
3. On the node, resolve the owning worker via `ShardRouter`.
4. Enqueue the read op via `WorkerPool::tryEnqueueOp` (same mechanism as produce).
5. The worker does a **point lookup** in its `viewTable` (§7) and returns the row.

No cross-thread read of the table, no lock — the read executes on the owning worker (#1). For a join view, the read routes on the shared key to the single co-partitioned worker (§8).

## 13. Data flow walkthrough (event → updated view)

End-to-end, single source (the join generalization is the §8 gather):

1. Producer appends a batch to topic `T` partition `p`. Leader worker `W` (owner of `T/p`) commits `[cursor, HW)` on the produce path.
2. In `W`'s **duty cycle** (not the produce ack path), `W`'s Reactor looks up `subscriptionIndex[(T, p)]` → interested views.
3. For each interested view `V`, `W` runs `foldRecord` (§6.1) over the newly committed records, bounded per cycle: project → per-field merge into `substate` → `reemit(k)` → write `viewTable[k]` (or erase on tombstone).
4. `W` advances `V`'s fold cursor. Periodically (§9.2) `W` takes a consistent cut and publishes a `ViewSnapshot`; the background IO thread persists it atomically.
5. A `GetViewRow(V, k)` routes to `W` (§12) and point-looks-up the current row.
6. On restart/failover: load latest snapshot, replay `[O+1, HW)` through the same fold (§9.4) → identical view.

## 14. Testing & benchmarks (binding — #5, #6, #16)

### 14.0 Latency NFRs (HARD GATES — the reason this engine exists)

These are binding pass/fail budgets, measured firsthand under default config (ABAB, n ≥ 15, report p50 **and** p99; a gate fails if p99 exceeds the budget). They drive the design (flat inline table, O(1) by-tag decode, zero steady-state allocation, shared-nothing fold):

| Path | Budget | Notes |
|---|---|---|
| **View update from an event batch** (fold: decode → project → per-field merge → viewTable upsert) | **< 10 µs / batch** | steady state, zero-alloc; off the produce ack path |
| **Point select spanning a single node** (`GetViewRow`: route → worker point lookup → reply) | **< 50 µs** | end-to-end incl. in-node hop |
| **Query spanning multiple nodes** | **< 200 µs** | end-to-end incl. cross-node fan-out/gather (query surface; join views) |

The schema-evolution prerequisite's decode path (`readField`-by-tag against a materialized per-version descriptor) is part of the <10µs fold budget — its own plan carries a derived sub-budget so it cannot erode this.

- **Unit:** `mergeField` per rule × type; projection select/rename; tombstone erase; open-addressing table probe/erase/resize; arena spill/compaction.
- **Fold determinism:** same log prefix → identical table byte-for-byte, across orderings that preserve per-key commit order; leader vs replica table equality at equal offset.
- **Persistence round-trip:** snapshot → kill → load+replay `[O+1, HW)` → table equals the never-restarted table (time-independent, #16). Crash-mid-write leaves prior complete snapshot usable (CRC fallback). Offset in artifact exactly matches table.
- **Shared-nothing audit:** no other thread touches a live `viewTable`; worker→IO handoff is ring + immutable snapshot only (extend the existing no-atomics / single-driver checker scripts).
- **E2E (real Aeron, default config):** define a view over a topic, produce, `GetViewRow`, restart broker, confirm view recovered to latest — firsthand green before "done" (#16), via a `docker/view-engine-e2e.sh` mirroring 0b/0c e2e scripts.
- **Benchmarks (gated against §14.0):** fold update latency per event batch **< 10 µs** p99 (zero-alloc verified); `GetViewRow` single-node **< 50 µs** p99; multi-node query **< 200 µs** p99; plus snapshot cut pause (worker memcpy time vs table size) and produce-path regression (fold off the critical path must not move produce latency — ABAB n≥15, #12). A benchmark that exceeds its §14.0 budget is a failing gate, not a note.
- **Regression suite:** every fix gets a `*Regression*` test; full regression suite runs first on every change (#6/#7).

## 15. Decisions

**Load-bearing decisions #1, #7, #9 resolved 2026-10-09** (bodies updated in §9.5, §6.0, §9 below). Items #2–#6 carry sound proposed defaults and are confirmed at plan time; #8 was already resolved into the prerequisite spec.

1. **§9.5 — snapshot-IO thread ownership (RESOLVED):** a **dedicated per-node view-snapshot IO thread** (own `AgentRunner`). Rejected: extending `LogCleanerAgent`. See §9.5.
2. **Snapshot trigger policy:** time *T* only, offset-delta *N* only, or both (whichever first)? Default proposal: both, whichever fires first, with *T* and *N* configurable per view.
3. **Snapshot granularity:** one artifact per (view, partition) (proposed) vs one per view across its partitions. Per-(view,partition) matches worker ownership and parallel IO; confirm.
4. **Very large views:** accept the bulk-copy pause now + benchmark, defer chunked/COW (proposed) — confirm the threshold at which we'd revisit.
5. **Arena compaction timing:** at snapshot cut vs on load vs incremental — proposed: rebuilt dense at load, amortized-reserve on the hot path, compacted during the snapshot copy.
6. **`DefineView` field defaults:** default `MergeRule` = `Replace`; key field must appear in `output`; reject non-numeric `Sum/Min/Max` at define time — confirm.
7. **Field-name uniqueness enforcement point (RESOLVED — fix-on-sight, rule #8):** enforce duplicate-name rejection at schema **registration** — `DescriptorBuilder::build()` (codec) and broker `DefineTopic` — so every consumer (views, typed clients, QL), not just Rune QL's parser (`Parser.cpp:241`), is protected and `Descriptor::find()`'s first-match is always safe. The view resolver additionally asserts uniqueness defensively. This codec/broker enforcement lands as part of this work (a small, bounded scope addition), with its own `*Regression*` test.
8. **Durable field tags vs positional ids (RESOLVED — promoted to prerequisite spec):** `(topicId, fieldId)` identity requires `fieldId` to be a durable never-reused tag. This is now its own prerequisite foundation sub-project: **`docs/superpowers/specs/2026-10-08-rune-schema-evolution-durable-tags-design.md`** (durable numeric per-topic tags + versioned descriptor + compatibility modes + `ALTER TOPIC`). The view engine **depends on** it and lands after it (**prerequisite SHIPPED 2026-10-09** — see header). The view spec's field identity is concretely `(topicId, tag)` from that spec; this spec's §6.0 is realized there.
9. **View-vs-schema-evolution compatibility policy (RESOLVED):** **graceful-absent at runtime + validation at `DefineView` time.** The schema layer stays one-way independent — a DROP of a view-bound tag degrades the tag to **absent** rather than being blocked at the schema layer (prerequisite spec §11, decision #6). On top of that, the view side:
   - **At `DefineView`:** the AdminWorker validates every referenced `(topicId, name→tag)` against the source topic's current Descriptor and **rejects** obvious errors — a name that does not resolve, or a tag already dropped at the bound version. (A later-defined view over an already-evolved topic cannot silently reference a field that no longer exists.)
   - **At runtime:** a field dropped by a *later* `ALTER TOPIC DROP` degrades to **null/absent** in the view column for records written after the drop; the fold never wedges and the view stays coherent (`(topicId, tag)` identity + `isLiveAt` per writer version). No view invalidation, no coupling of `ALTER TOPIC` to view lifecycle.
   - Retype is treated as drop-old-tag + add-new-tag at the schema layer, so a view bound to the old tag sees it go absent (same runtime rule); a view wanting the new field is redefined to reference the new tag.

## 16. Deferred follow-ons (program-level)

- **Join / multi-source views** (§8) — own spec: co-partitioning, gather semantics, outer/inner policy.
- **Filter / Aggregate operators** (§5).
- **Secondary indexes & range scans** over the view table.
- **Rune QL `create view …`** surface over `DefineView` (thin, mirrors 0c).
- **Multi-node `__views` replication** + view placement/failover policy (inherits RULING-4).
- **Chunked/COW snapshot** for very large views (§9.6).
