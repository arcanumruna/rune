# Rune Schema Evolution — Durable Field Tags + Versioned Descriptors + Compatibility Modes — Design Spec

**Status:** DRAFT — for review & enhancement
**Date:** 2026-10-08
**Program:** Rune materialized-views foundation. **Prerequisite** sub-project — lands **before** the view engine (`2026-10-08-rune-view-engine-single-source-design.md` depends on it).
**Builds on:** 0a rune-codec ✅, 0b schema registry + typed transport ✅, 0c Rune QL & CLI ✅.

---

## 1. Goal

Let a topic's schema **evolve non-additively** — drop, rename, and (later) retype fields, not only append — because no successful database is additive-only. Safety comes from **durable, numeric, per-topic field tags that are allocated once and never reused**, so a field has a stable identity `(topicId, tag)` across any evolution. Each record already carries its writer `version`, so the registry keeps a **versioned descriptor** and decodes every record against *its* version. Non-additive changes are governed by **compatibility modes** so an evolution cannot silently break readers. All schema DDL is **serialized** through the single AdminWorker thread.

## 2. Scope (v1) and non-goals

**In scope (v1):**
- Durable numeric tags (never reused), versioned descriptor, version-aware decode.
- `ALTER TOPIC … ADD COLUMN | DROP COLUMN | RENAME COLUMN | SET COMPATIBILITY` in Rune QL + `rune` CLI.
- Compatibility gate with modes **`{BACKWARD, FORWARD}`**, default **BACKWARD**.
- Per-version fingerprints; typed-transport gate accepts any known version.
- DDL serialization + optimistic CAS concurrency on the AdminWorker.

**Deferred (named follow-ons):**
- **`ALTER COLUMN TYPE` (retype)** — and with it the `FULL` and `NONE` compatibility modes (which only become distinct from `FORWARD`/`BACKWARD` once retype exists; see §7).
- **Multi-node `__topic_schemas` replication** — single-node registry now (inherits 0b **RULING-4**).
- Column reordering, default values, per-field docs.

## 3. Engineering-philosophy alignment (binding)

- **Shared-nothing (#1) + ring-only (#2):** all DDL runs on the single AdminWorker thread, fed by an SPSC command ring; no shared mutable schema state, no locks. Serialization is a property of the single-thread design, not a lock (§9).
- **Off the hot path:** schema evolution is control-plane only. The produce/consume/fold hot path sees at most an O(1), zero-alloc per-version **offset-table lookup** (§5); no DDL work, no allocation, no virtual dispatch on the hot path.
- **Rich-state state machines (#3):** a field's lifecycle is explicit — `live(sinceVersion)` → `retired(untilVersion)`; no scattered "isDeleted" flags.
- **Deterministic / time-independent (#16):** decode is a pure function of `(record bytes, record.version, descriptor)`. Registry recovery is a pure replay of the `__topic_schemas` log.
- **Test + benchmark everything (#5), regression every fix (#6), run regression first (#7), fix-on-sight (#8):** §13.

## 4. Data model — the evolving descriptor

`FieldDescriptor` gains durable identity and a lifecycle:

```cpp
struct FieldDescriptor {
    std::string name;          // human alias — NOT identity; mutable via RENAME
    uint16_t    tag;           // DURABLE numeric identity, per-topic, monotonic, NEVER reused
    PrimType    type;          // fixed for the field's life (no retype in v1)
    uint16_t    fixedLen = 0;  // FixedStr width
    uint16_t    sinceVersion;  // first schema version in which the field is live
    uint16_t    untilVersion;  // first version in which it is NO LONGER live; 0xFFFF = ∞ (live)
    bool        isKey = false;
    std::vector<int32_t> enumValues;
    // NOTE: block `offset` is no longer stored on the field — it is version-dependent
    //       and computed into per-version offset tables (§5).
};
```

Per-topic registry state (in `__topic_schemas`):

```cpp
struct TopicSchema {
    int32_t       topicId;
    uint16_t      schemaVersion;   // current (monotonic)
    uint16_t      nextTag;         // monotonic tag allocator; only ever increments
    CompatMode    compatMode;      // BACKWARD (default) | FORWARD
    std::vector<FieldDescriptor> fields;  // ALL fields ever defined (live + retired), in tag-allocation order
};
```

- **Tag** is numeric (`uint16`, matching the existing `id`), **registry-allocated** from `nextTag` on each `ADD`, **never reused** (a retired tag's number is burned forever). Tags never appear on the record wire (SBE is positional), so tag width is a metadata-only cost and can widen to `uint32` if ever needed.
- **`id` → `tag`:** the existing `FieldDescriptor::id` is repurposed as the durable tag; existing 0a/0b topics migrate by seeding `tag = id`, `untilVersion = ∞` (nothing has been dropped yet — lossless).
- **Live-at-V predicate:** a field is live at version `V` iff `sinceVersion ≤ V < untilVersion`. Retired = `untilVersion != 0xFFFF`.
- Fields are stored in **tag-allocation (append) order** — the stable order used to compute offsets.

## 5. Version-aware decode

**The record wire is unchanged** — `[blockLength u16][version u16]` + fixed block (from byte 4) + var-section. Every record self-identifies its writer `version`.

**Offset-as-of-version** is a pure function: iterate fields in tag-allocation order, keep those live at `V`, accumulate `fieldWidth`; each live field's block offset at `V` is the running sum; var-len fields get var-indices in the same filtered order. The registry precomputes this once per version into an **offset table** cached for the hot path:

```
offsetTable : version → { tag → (offset | varIndex) }   // precomputed, immutable per version
```

Decode:

```cpp
FieldValue readFieldByTag(const TopicSchema& s, std::span<const uint8_t> record, uint16_t tag) {
    uint16_t V = read_u16(record, 2);              // writer version from the record header
    auto slot = s.offsetTable[V].find(tag);        // O(1); absent ⇒ field not live at V ⇒ {}
    if (!slot) return {};
    return decodeAt(record, *slot, fieldOf(s, tag).type);  // type is stable for life (no retype v1)
}
```

- A **new reader reads any old record** (it has that record's version's offset table) ⇒ **BACKWARD is satisfiable by construction**.
- **`DROP` reclaims the dropped field's slot** in versions ≥ drop (records get smaller), so an *old* reader's offset table expects a slot the new record doesn't have ⇒ `DROP` is inherently **FORWARD-incompatible** (gated in §7).
- The hot path touches only `offsetTable[V]` — O(1), zero-alloc.

## 6. ALTER operations as tag actions

Each `ALTER TOPIC` statement is **atomic**, bumps `schemaVersion` exactly once (may carry multiple column ops), and runs on the AdminWorker.

- **`ADD COLUMN <name> <type>`** → `tag = nextTag++`; `sinceVersion = newVersion`; `untilVersion = ∞`; appended in tag order. `<name>` must not collide with a **live** field name. (It *may* reuse a **retired** field's name — the name is an alias; the new field gets a fresh tag.)
- **`DROP COLUMN <name>`** → resolve name→tag (must be live); set `untilVersion = newVersion`; tag retired forever. The key field cannot be dropped; the last live field cannot be dropped.
- **`RENAME COLUMN <old> TO <new>`** → resolve old→tag (must be live); change the `name` only. Tag, type, offsets untouched — invisible to decode. `<new>` must not collide with a live name.
- **`SET COMPATIBILITY (backward|forward)`** → change `compatMode`; applies to **future** ops only (never re-validates history).

**Within-statement validation** is against the cumulative intermediate state: contradictory/again-ops in one statement (e.g. `ADD x` then `DROP x`, two `ADD x`, `RENAME a→b` then `RENAME a→c`) are rejected. A rejected statement is a **no-op** (no version bump, nothing persisted, located error returned).

## 7. Compatibility gate

Runs on the AdminWorker before committing, against the topic's `compatMode`. With v1's op set, outcomes:

| Op | BACKWARD (new reader ↔ old data) | FORWARD (old reader ↔ new data) |
|----|------|------|
| **ADD**    | ✅ old data lacks field → absent via `sinceVersion` | ✅ old reader ignores trailing new field |
| **RENAME** | ✅ decode by tag | ✅ decode by tag |
| **DROP**   | ✅ new reader ignores it | ❌ old reader's offset table expects the reclaimed slot |

**Net rule in v1:** `DROP` is rejected under `FORWARD`, allowed under `BACKWARD` (default); `ADD`/`RENAME` pass under both. For these three ops BACKWARD holds against **all** prior versions (decode-by-tag works for any version), so transitive vs non-transitive is moot until retype.

**Why `FULL`/`NONE` are deferred:** with only ADD/DROP/RENAME, the mode decides exactly one bit — "is DROP allowed?" — so `{BACKWARD, NONE}` are indistinguishable and `{FORWARD, FULL}` are indistinguishable. `FULL` (= `BACKWARD ∧ FORWARD`) and `NONE` (escape hatch) only become distinct once `ALTER COLUMN TYPE` exists (a widening is backward-safe but forward-breaking, etc.). They ship with retype.

## 8. Registry integration & versioned fingerprints

- **Storage:** `__topic_schemas` stores `TopicSchema` (all fields + tags + lifecycle + version + nextTag + compatMode). Each DDL commit **appends** a record — the log *is* the history; recovery is replay.
- **Control flow:** `ALTER`/`CREATE`/`SET COMPATIBILITY` run on the AdminWorker (the 0b shared-nothing DDL thread): parse → resolve names→tags → within-statement validate → compat gate → bump version → update descriptor → append to `__topic_schemas` → recompute the affected per-version offset table → distribute the updated `TopicSchema` + offset tables to workers via ring (reusing 0b's per-worker schema-cache distribution). No hot-path involvement.
- **Per-version fingerprint:** `fp(V)` = FNV-1a-64 over the schema *as of version V* (fields live at V, each with `tag`, `name`, `type`, `fixedLen`, computed offset). Names are included so a generated typed client's field names match — therefore a `RENAME` produces a new `fp`/version even though decode is byte-identical. The registry keeps a `fp → version` map.
- **Typed-transport gate change (touches 0b):** 0b's `openTopic<T>` gated on an **exact** fingerprint match of the latest schema. It now accepts **any fingerprint in the topic's version history** and **binds the client to that version**: a producer on `fp(V)` stamps records `version = V`; a consumer on `fp(V)` gets exactly the shape it compiled against. Multiple versions are valid typed endpoints simultaneously. An unknown fingerprint is still rejected.
- **Blob format:** adding `untilVersion` and explicit `tag` bumps the descriptor-blob magic `RCD1 → RCD2`. The deserializer still reads `RCD1` (interpreting `tag = id`, `untilVersion = ∞`) so existing schemas migrate losslessly.

## 9. Concurrency & serialization

**All schema DDL is serialized through the single AdminWorker thread** — `CreateTopic`/`DefineTopic`, `AlterTopic`, `SetCompatibility` all funnel through the AdminWorker's SPSC command ring and are processed strictly **FIFO, one at a time**. Concurrent DDL is therefore impossible *by construction* (shared-nothing #1, ring-only #2) — there is no lock; the single consumer is the serialization point.

- **No concurrent ALTER / CREATE:** two in-flight DDL ops are enqueued and applied in arrival order, each against the then-current committed state. Deterministic.
- **Optimistic CAS (`expectedVersion`):** an `ALTER` may carry `expectedVersion`. When the AdminWorker dequeues it, if the topic's current `schemaVersion != expectedVersion` the op is **rejected as a conflict** (schema moved under the client — prevents lost updates). Omitted ⇒ applied against current (last-writer-wins). Tooling (CLI) sends it by default: fetch current → edit → submit with that version.
- **CREATE serialization:** two `CreateTopic` with the same name → serialized → the second sees the first committed → rejected (duplicate). TopicId allocation (RULING-1 sequential allocator) is naturally race-free under the single thread.
- **Atomic multi-op ALTER:** the whole statement validates (incl. within-statement checks §6 and the compat gate §7) before any mutation; then one version bump applies all ops. Any failure ⇒ entire statement rejected, zero partial state.
- **Durability ordering:** the new version is appended to `__topic_schemas` **before** the client is acked and **before** workers are notified. A crash leaves the ALTER either fully committed (in the log) or not acked at all — never partial. Recovery replays the log in order.

## 10. Rune QL `ALTER TOPIC` surface

Extends the 0c parser, backends, and `rune` CLI:

```
alter topic <name>
    add column <name> <type>            -- type incl. string / string(N)
  | drop column <name>
  | rename column <old> to <new>
  | set compatibility ( backward | forward )
  ( , <op> )* ;                         -- multiple ops in one atomic statement
```

Parser → `AlterTopicPlan { topicName, expectedVersion?, ops[] }` → `ALTER` wire op → AdminWorker. `CREATE TOPIC` defaults `compatMode = BACKWARD`. Applied via `rune schema apply <file>` (diff current → emit ops) or `rune schema alter …`, reusing 0c CLI plumbing. `rune schema get` surfaces tags + lifecycle (read-only) for inspection.

## 11. How the view engine depends on this (one-way)

- **Field identity in views = `(topicId, tag)`**, resolved once at `DefineView` by name→tag against the current descriptor (concretizes the view spec's §6.0).
- **Version-aware fold:** the view fold reads each source record by tag via that record's-version offset table (`readFieldByTag`, §5), so a view folds correctly across the source's whole evolution history.
- **Graceful degradation, not hard coupling:** if a tag a view binds is `DROP`ped, records after the drop decode that field as **absent** (like a nullable) — no crash, no dangling reference, because identity is the durable tag. Any stricter policy ("reject a DROP a live view depends on") is the view spec's decision #9; this spec only exposes the retired-tag state. Dependency is strictly schema-evolution ← view engine.

## 12. Components changed ("everything changed")

| Repo / module | Change |
|---|---|
| **rune-codec** | `FieldDescriptor`: `tag`, `sinceVersion`, `untilVersion`, drop stored `offset`. Per-version offset-table computation. `readFieldByTag` (version-aware). `Serialize` blob `RCD1→RCD2` (+ back-compat read). `Fingerprint` per-version (`fp(V)`), absorbs tag/name. `Validator` version-aware. `Generator` emits version-bound typed structs. |
| **rune-protocol** | New `ALTER` wire op (`AlterTopicRequest { topicName, expectedVersion?, ops[] }`, `AlterTopicResponse { version | error }`). Metadata/typed-open responses carry `version` + `fp(V)`. |
| **rune-broker** | `TopicSchema` registry state (version, nextTag, compatMode, retired tags) in `__topic_schemas`. AdminWorker: handle `ALTER`/`SET COMPATIBILITY`, within-statement validation, compat gate, CAS, atomic apply, durability ordering, offset-table recompute + ring distribution. Per-worker schema cache updated with versioned descriptors. Typed gate accepts any known version, binds + stamps producer version. |
| **rune-ql** | `ALTER TOPIC` grammar + `AlterTopicPlan` backend. |
| **rune-client-cpp** | `rune schema alter`/`apply` verbs; CAS `expectedVersion` default; typed client binds to its compiled version. |

## 13. Testing & benchmarks — exhaustive edge cases

Every case below is a required test. Each fix adds a `*Regression*` test; the full regression suite runs **first** on every change (#6/#7).

### 13.1 Unit — tag & descriptor model (rune-codec, no threads)

- ADD allocates `nextTag`; `nextTag` increments; the number is **not reused** after a later DROP.
- DROP sets `untilVersion`; tag retired; a retired tag is never returned by `nextTag`.
- RENAME changes `name` only; `tag`, `type`, offsets unchanged.
- Offset-as-of-version: correct offsets when fields were added across versions.
- Offset-as-of-version after DROP: dropped field's slot reclaimed at versions ≥ drop; **earlier versions retain it**.
- Live-at-V boundaries: `sinceVersion == V` (live), `untilVersion == V` (not live, exclusive), `sinceVersion > V` (absent), in-range (live).
- Mixed fixed + var fields: var-index reassignment as-of-version after dropping a var field.
- `FixedStr` widths preserved across versions; inline layout correct per version.
- Offset-table cache: cached result equals freshly computed; immutability per version.

### 13.2 Unit — operation validation (rune-codec / registry logic)

- ADD name collides with a **live** field name → reject.
- ADD name equals a **retired** field's name → allowed, fresh tag.
- RENAME to a name colliding with a live field → reject.
- RENAME of a non-existent or already-dropped field → reject.
- DROP of a non-existent or already-dropped field → reject.
- DROP of the **key** field → reject.
- DROP of the **last live** field → reject.
- Empty ALTER (no ops) → reject.
- Within-statement contradictions: `ADD x; DROP x`, `DROP x; ADD x`, two `ADD x`, `RENAME a→b; RENAME a→c` → reject (validated against cumulative state).
- Multi-op statement where op N fails → **entire** statement rejected, no partial apply, no version bump.

### 13.3 Unit — compatibility gate

- DROP under FORWARD → reject; DROP under BACKWARD → allow.
- ADD under BACKWARD and FORWARD → allow; RENAME under BACKWARD and FORWARD → allow.
- `SET COMPATIBILITY` backward↔forward changes gating of subsequent ops.
- Changing mode does **not** re-validate history (a topic with a past DROP may still be set to FORWARD; only future DROPs are blocked).

### 13.4 Unit — fingerprint & serialization

- `fp(V)` differs across versions; stable when recomputed for the same version.
- Two schemas with identical names but different tag history → different `fp`.
- RENAME → new `fp`/version but **byte-identical decode** of existing records.
- `fp → version` reverse lookup; unknown `fp` → rejected.
- Blob `RCD1` read → `tag = id`, `untilVersion = ∞`; re-serialize `RCD2`; round-trip equal.
- Blob `RCD2` round-trip; truncated/malformed `RCD2` → bounds-checked error (no OOB).

### 13.5 Integration — AdminWorker + registry + worker distribution (in-process, real threads, no network)

- Two ALTERs enqueued "concurrently" → processed FIFO → both applied, final version = base+2.
- Two ALTERs with the same `expectedVersion` → first applies (→W); second (expected=V) now stale → **conflict rejected** (no lost update).
- `expectedVersion` omitted → applied against current.
- Concurrent CREATE same name → one succeeds, other rejected duplicate.
- Concurrent CREATE different names → both succeed, distinct sequential topicIds, no collision.
- CREATE then immediate ALTER → serialized; ALTER sees the created topic.
- Rejected ALTER (validation or compat) → registry unchanged, no distribution, client gets located error.
- Version appended to `__topic_schemas` **before** worker distribution and client ack (ordering assertion).
- After ALTER, worker schema cache updated → workers decode the new version; in-flight old-version records still decode (per-version offset tables present).
- Producer bound to `fp(V)` keeps writing `version = V` after the topic evolves to W; records stamped V; broker accepts.
- Consumer bound to `fp(V)` reading a later version-W record: reads its known tags, sees **absent** for a field dropped between V and W, ignores tags added after V.
- AdminWorker never touches the produce hot path (thread-placement / no-shared-state assertion; extend the no-atomics / single-driver checker scripts).
- Registry replay on restart reconstructs descriptor + version + nextTag + retired tags + compatMode exactly.
- Crash mid-ALTER: the ALTER is either fully in `__topic_schemas` or absent (not acked) — replay yields a consistent state; no partial version.

### 13.6 E2E — real Aeron, docker, default config (`docker/schema-evolution-e2e.sh`)

- Baseline: define topic → produce typed records → consume → verify (0b/0c still green).
- ADD COLUMN → old records read with new field absent; new records carry it; one consumer reads both.
- DROP COLUMN under BACKWARD → old records still decode the dropped field (via their version); new records omit it; a new consumer reads both correctly.
- DROP COLUMN under FORWARD → broker rejects; client gets error; schema unchanged.
- RENAME COLUMN → decode unaffected; consumers using the new name work; `fp` changed.
- SET COMPATIBILITY backward→forward → a subsequent DROP is rejected end-to-end.
- Two clients issuing concurrent ALTERs over the wire → serialized; deterministic final schema; the CAS loser gets a conflict error.
- Producer generated against the old schema keeps producing after an evolution (version stamping) — full round trip.
- Full lifecycle: create → add → add → drop → rename → produce across each boundary → consume the entire log → **every record decodes per its own version**.
- Restart broker after several ALTERs → schema history recovered → full historical log still decodes.
- CLI: `rune schema alter` / `rune schema apply` round-trip against a live broker, incl. a CAS conflict surfaced to the CLI.

### 13.7 Benchmarks (#5, #12)

- `readFieldByTag` / offset-table lookup on the decode hot path — O(1), zero-alloc (verified).
- `ALTER` latency — confirmed on the AdminWorker, **off** the produce/consume hot path.
- Produce/consume steady-state regression: the evolution machinery adds **zero** cost to the hot path when no DDL is in flight (ABAB, n ≥ 15).

## 14. Decisions (locked to recommended defaults — override on the file if desired)

1. **Tag width:** **`uint16`** (matches existing `id`; 65 535 ever-defined fields). Documented path to widen to `uint32` (near-free since tags aren't on the wire).
2. **Retired name reuse:** **Allowed** — a dropped field's name may be reused by a later ADD (which gets a fresh tag). Name is an alias; identity is the tag.
3. **`expectedVersion`:** **Optional on the wire** (omitted ⇒ last-writer-wins against current); the CLI/tooling **sends it by default** (fetch → edit → submit).
4. **`rune schema apply` diffing:** `apply` emits only **ADD/DROP** from a schema diff (rename is ambiguous under a pure diff — it would look like drop+add); **RENAME must be an explicit `alter … rename column`**.
5. **Fingerprint includes field names:** **Yes** — typed struct field names must match, so a RENAME produces a new `fp`/version even though decode is byte-identical.
6. **View-drop protection:** **One-way dependency confirmed** — a DROP of a view-bound tag degrades to **absent** (nullable) rather than being blocked at the schema layer. Any stricter view-side policy is the view spec's decision #9.

## 15. Deferred follow-ons

- **`ALTER COLUMN TYPE` (retype)** + `FULL`/`NONE` compatibility modes + transitive modes (the point where mode distinctions become real).
- **Multi-node `__topic_schemas` replication** (inherits 0b RULING-4): admin-owned transport, versioned-schema replication, multi-node recovery matrix.
- **Column reorder, default values, per-field documentation.**
- **`rune schema diff` / migration tooling** beyond `apply`.
