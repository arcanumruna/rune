# Rune Materialized Views — Step 0: Runtime Typed Topics & SBE Schema Registry

> **STATUS: DESIGN / FOR REVIEW.** First sub-project of the broker-local
> materialized-views program. Supersedes the relevant parts of the idea-capture
> draft [`2026-09-15-rune-db-face-materialized-views-transactions-DRAFT.md`].
> Scope is deliberately narrow: this spec makes topics *typed at runtime* and
> stands up a *schema registry*. It does **not** build the view engine, the
> query system, the definition DSL, folds, or transactions — those are later
> specs that depend on this foundation.

## 1. Program context & decomposition

The end goal (from the draft) is **broker-local materialized views**: derived,
always-current tables the broker maintains by folding a topic's log, queryable
directly instead of by log replay. The canonical example is a `characters` view
folding three co-partitioned logs (`characters`, `attributes`, `position`, all
partitioned by `userId`) — a local join on `characterId`, with per-source merge
rules (character = upsert; attributes = sum of deltas; position = last-write).

The program breaks into independent sub-projects, each its own spec, built in
order:

0. **Typed topics + SBE schema registry (THIS SPEC).**
1. Single-source views (one log → keyed table; async apply; volatile; per-topic
   offset checkpoint).
2. Multi-source co-partitioned join + per-source merge rules (the `characters`
   view).
3. Query surface (filter by key/partition-key + projection; broker-local).
4. Sync apply (ack-after-view) + persistence (periodic snapshots).
5. Cross-partition transactions (trades; create-character).

Two architecture decisions from the brainstorming pass frame everything and are
recorded here so later specs inherit them:

- **Wire/type format = SBE, using its IR + on-the-fly (OTF) decoder** for
  runtime introspection and generic field access. No custom format. View/DB
  *semantics* (join keys, merge rules, projections) live in the view-definition
  layer, not the message schema. Rationale in §4.1.
- **View maintenance = replicated deterministic fold.** The partition leader
  folds an event into its local view shard after append; it replicates the
  event; each replica folds the *same* event into its own copy on
  `applyReplicated`. Views are therefore replicated derived state (query any
  replica, free HA), and follower folds run entirely off the write leader.
  **Folds must be deterministic** or replicas diverge. (Realized in specs 1–4;
  step 0 only provides the typed-record access the fold will use.)

## 2. Goals / non-goals

**Goals**
- A **runtime API** to declare a topic's message type (today topics are defined
  only via env property; there is no runtime definition path).
- Accept a **serialized SBE schema (IR)** + `schemaId`, deserialize it, link it
  to the topic, and **persist it durably** so it survives restart and reaches
  every node hosting the topic.
- **Optional per-topic validation**: when enabled, reject produces whose payload
  does not conform to the topic's schema, via a validator derived from the IR.
  Validation must be **extremely fast** and **zero-cost when disabled**.
- Expose a broker-internal capability to **read fields of a record generically
  by name** (via OTF decode over the IR) — the primitive specs 1–4 build on.

**Non-goals (explicitly deferred)**
- The view engine, folds, the query system, the definition DSL, transactions,
  cross-partition anything. Distribution/co-partitioning placement policy.
- Rich schema-evolution policy (this spec supports register + additive update;
  compatibility rules are sketched in §7 and finalized later).
- A general document/nested type system beyond what SBE expresses.

## 3. What exists today (build on, don't reinvent)

- **SBE** is already the wire format (`rune-protocol`, generated codecs,
  `schemaId`, `templateId`). Client and broker both link `rune-protocol`.
- **Reserved templateId range 64–95** ("view define/query/watch") is empty —
  room for the new messages here.
- **Topics** are currently declared via an env property at startup; there is no
  runtime create/define path.
- **Internal compacted topics** are already the idiom for replicated metadata
  (e.g. `__transaction_state`, offset/cluster metadata). We reuse this for the
  registry rather than inventing a store.
- **Metadata plumbing** (broker-side topic registry + client `MetadataCache`)
  already carries per-topic info to route produces/subscribes.

## 4. Design

### 4.1 Why SBE IR + OTF (and where DB semantics go)

SBE's *compiled* codecs bake field offsets in at build time, which looks
metadata-thin. But the SBE tool also emits an **IR** — the full structural
schema (field names, primitive/composite types, enums, groups, var-data,
offsets, null/min/max, `sinceVersion`) — which is **serializable**, and SBE
ships an **OTF decoder** that reads any message from `bytes + IR` with no
compiled codec. That is exactly the runtime capability the view/query engine
needs: "given this topic's schema, pull field `x`/`delta`/`type` from an
arbitrary record, and validate it."

The metadata SBE does **not** carry — join keys, that `attributes.delta` is
summed while `position.x` is last-write, which fields to project — is
**view-definition metadata, not message-schema metadata**, and belongs in the
view DSL (spec 2+). The few *intrinsic* topic-level semantics (primary-key
field, optional timestamp field) ride as a **thin sidecar descriptor** in the
define-topic request, not in the SBE schema.

Custom format is rejected: SBE is already in the stack, is zero-copy with
fixed-offset field access (fastest possible for the fold/validate hot path), and
its IR already expresses everything the `characters` example needs. Revisit only
if a concrete need appears that the IR cannot express (none found).

### 4.2 The schema artifact

A topic schema entry is:

```
TopicSchema {
  topicId        : int32          // resolved topic identity
  schemaId       : uint16         // SBE schema id (namespaces the type set)
  templateId     : uint16         // SBE message/template id within schemaId
  irBlob         : bytes          // serialized SBE IR for (schemaId, templateId)
  keyField       : string         // intrinsic primary key field name (sidecar)
  tsField        : string?        // optional event-timestamp field name (sidecar)
  validate       : bool           // per-topic: enforce schema on produce
  version        : int64          // monotonic registry version for this topic
}
```

`irBlob` is the serialized SBE IR (SBE already defines an IR encoding). The
broker never needs the topic's generated codecs — OTF decode over `irBlob` is
sufficient and generic.

### 4.3 Wire protocol (rune-protocol, templateIds 64–95)

New SBE messages, consumed by both broker and `rune-client-cpp`:

- `DefineTopic` (client→broker): `{ topic, partitionCount, schemaId, templateId,
  irBlob (var-data), keyField, tsField, validate }`. Idempotent by
  `(topic, schemaId, templateId, irBlob-hash)`.
- `DefineTopicResponse` (broker→client): `{ topicId, version, errorCode }`.
- `GetTopicSchema` / `TopicSchemaResponse` (optional, for tooling/clients that
  want the IR): `{ topicId } → { schemaId, templateId, irBlob, keyField, ... }`.

Client API addition (`rune-client-cpp`): `Client::defineTopic(TopicSchemaDef)`
returning the assigned `topicId`/`version`. The `MetadataCache` learns
`topicId → {schemaId, templateId, validate, keyField}` (not necessarily the full
IR — see §4.6) so the client can build correctly-typed records.

### 4.4 Broker handling & persistence

1. Gateway receives `DefineTopic`. It **parses `irBlob` into an SBE IR** to
   verify it is well-formed (reject `InvalidSchema` otherwise), and checks the
   named `keyField`/`tsField` exist and have sane types.
2. The entry is appended to an internal **compacted topic `__topic_schemas`**,
   keyed by `topicId` (value = the `TopicSchema` record). This is the durable,
   replicated store — reusing the existing compacted-topic + replication
   machinery; no new persistence subsystem. Compaction keeps only the latest
   schema per topic (plus history retained by the log until compacted).
2b. The topic itself is created/linked (partition assignment) exactly as env-
    declared topics are today; the only addition is the schema linkage.
3. On `__topic_schemas` apply (leader and every replica), each node updates its
   in-memory **SchemaRegistry**: `topicId → { IR (parsed), validator?, keyField,
   ... }`. Because it applies via the normal replicated log path, every node
   hosting the topic converges to the same schema — the same mechanism the view
   fold will later use.
4. On broker restart, the registry is rebuilt by replaying `__topic_schemas`
   from the last checkpoint (standard compacted-topic recovery).

### 4.5 Validation (opt-in, fast, off by default)

When `validate == true` for a topic, produced records are checked against the
IR before append:

- **What it checks:** total length/bounds; each fixed field's presence and
  (where cheap) enum/range/null constraints declared in the IR; var-data length
  consistency. SBE's fixed layout makes this a linear, branch-light pass — no
  allocation.
- **Implementation:** a **validator built once from the IR** at registration
  (cached on the registry entry). Two candidate implementations, decided in
  implementation: (a) drive the SBE OTF decoder and assert bounds/constraints,
  or (b) precompute a flat field-descriptor table (offset, type, min/max, enum
  set) and validate with a tight loop. (b) is the target for speed; (a) is the
  fallback. Benchmarked to confirm ~tens of ns for a small record.
- **Where it runs:** on the produce ingress path *before* the log append. Only
  topics with `validate` on pay anything; others take a single already-present
  branch. This preserves write throughput (the explicit constraint).
- **Failure:** reject the produce with `SchemaValidationFailed` and a field
  hint; nothing is appended (no partial state).

### 4.6 Generic field access for later stages

The registry exposes, per `topicId`: the parsed IR and a helper to **read a
named field from a record's bytes** via OTF (returning a typed value/view). This
is the single primitive specs 1–4 consume — the fold reads source fields
(`attributes.delta`, `position.x`), the query projects fields. Step 0 ships and
tests this accessor but wires no view to it.

The full IR lives broker-side. Clients receive only the lightweight
`{schemaId, templateId, validate, keyField}` in metadata (enough to encode
records); a client wanting the IR fetches it via `GetTopicSchema`.

## 5. Data flow

**Define:** client `defineTopic` → gateway parse/validate IR → append to
`__topic_schemas` (compacted) → replicated → every node applies → SchemaRegistry
updated → response with `topicId`/`version`.

**Produce (validate on):** record → registry lookup (cached, O(1)) → validator
→ pass ⇒ normal append path; fail ⇒ `SchemaValidationFailed`, no append.

**Restart/failover:** replay `__topic_schemas` from checkpoint → registry
rebuilt; a newly-promoted leader already has it (applied as a follower).

## 6. Components (isolation)

- **`SchemaRegistry`** (broker): `topicId → SchemaEntry{ IR, validator?,
  keyField, tsField, schemaId, templateId, version }`. Owns parse, validator
  build, generic field accessor. Pure in-memory, rebuilt from `__topic_schemas`.
- **`__topic_schemas` applier**: turns compacted-topic records into registry
  updates (leader + replicas).
- **`DefineTopic` handler** (gateway): validates request, writes the registry
  record, creates/links the topic.
- **`RecordValidator`**: built from an IR; `validate(bytes) → ok | field error`.
- **rune-protocol**: the new SBE messages (64–95) + the `TopicSchema` record
  encoding + IR-blob carriage.
- **rune-client-cpp**: `Client::defineTopic`, `getTopicSchema`, metadata
  extension.

Each is independently testable; the registry and validator have no I/O.

## 7. Schema evolution (minimal for step 0)

- A topic may be re-defined with a **new schema version** (new `irBlob`); the
  registry keeps the latest and bumps `version`. Records carry `schemaId` +
  `templateId` + SBE block version, so readers/validators pick the right layout.
- Step 0 permits **additive** evolution (new trailing optional fields / var-data,
  per SBE `sinceVersion`) and rejects incompatible redefinition with
  `IncompatibleSchema`. A full compatibility policy (and multi-version coexistence
  for in-flight records) is deferred to a later spec; noted in Open Questions.

## 8. Error handling

| Condition | Result |
|---|---|
| Malformed `irBlob` (won't parse to IR) | `InvalidSchema`, define rejected |
| `keyField`/`tsField` missing or wrong type in IR | `InvalidSchema` |
| Define on existing topic, identical schema | idempotent success (same version) |
| Define on existing topic, incompatible schema | `IncompatibleSchema` |
| Produce fails validation (validate on) | `SchemaValidationFailed`, no append |
| Produce to a validate-on topic with wrong `schemaId`/`templateId` | rejected |
| Registry miss on produce (schema not yet applied) | reject/retry (define must precede produce) |

## 9. Testing

- **Unit:** IR serialize→deserialize round-trip; validator accepts conformant
  records and rejects each violation class (bad length, out-of-range enum,
  null-in-required, truncated var-data); generic field accessor returns correct
  typed values for every field kind (fixed, enum, var-data, group).
- **Integration:** define a topic at runtime → produce valid + invalid records
  → assert accept/reject; assert `__topic_schemas` persists the entry and the
  registry **rebuilds after broker restart**; assert the schema **replicates to
  followers** (every node's registry converges).
- **Benchmark (throughput gate):** validation cost per small record — target
  ~tens of ns, and **zero measurable delta** on the produce path when
  `validate` is off (opt-in must not tax untyped/unvalidated topics).

## 10. Throughput considerations

- Validation is **opt-in per topic** and **off by default**; validate-off topics
  take one branch.
- Validator uses SBE's **fixed-offset layout** — no allocation, no parse tree.
- Registry lookups on the produce path are **O(1)** from an in-memory map keyed
  by `topicId`; the IR/OTF machinery is used **off the ack path** (view folds,
  queries) — never in the produce fast path except the cheap validator.
- The registry is maintained via the **existing replicated compacted-topic
  path** — no new hot-path work on produce.

## 11. Open questions (resolve during planning / later specs)

- Validator implementation (a) OTF-driven vs (b) precomputed descriptor table —
  decide by benchmark; spec targets (b).
- Multi-version coexistence: do in-flight records of an older block version need
  the older validator/accessor simultaneously, or is register-before-produce
  ordering sufficient for step 0?
- Where exactly validation runs (gateway ingress vs per-worker) given the
  per-worker ingress scale-out path — pick the point that keeps it off the
  shared ack path.
- Does `DefineTopic` also own **partition count / placement**, or only schema on
  top of the existing topic-creation path? (Leaning: schema-only in step 0;
  topic creation stays as-is.)
- Client ergonomics: do clients ship the IR (from their SBE-generated schema) or
  a higher-level type descriptor the broker compiles to IR? (Leaning: ship IR;
  it's what SBE already produces.)
