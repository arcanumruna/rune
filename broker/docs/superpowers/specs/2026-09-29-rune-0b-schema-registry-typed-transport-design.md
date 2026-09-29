# Rune 0b — Schema Registry & Typed Transport

**Sub-project:** 0b of the Materialized-Views foundation. Builds on **0a
(`rune-codec`, complete)** and precedes **0c (Rune QL & CLI)**.

**Parent spec:** `2026-09-29-rune-materialized-views-typed-topics-design.md`
(§4 sketches this sub-project; this document is its full design and supersedes
that sketch where they differ).

**Goal:** a runtime, replicated, versioned **schema registry** in the broker,
plus a **compile-time typed client** path — so a publisher can define a topic
with a rune-codec schema, have it persisted/replicated/optionally
validated on ingress, and open it type-safely with a generated struct.

---

## 1. Scope

**In scope (0b):**
- Wire API in `rune-protocol`: `DefineTopic` / `DefineTopicResponse` /
  `GetTopicSchema` / `TopicSchemaResponse` (templateIds 64–67).
- Broker: `__topic_schemas` compacted+replicated internal topic, in-memory
  `SchemaRegistry` rebuilt from it, a `DefineTopic` gateway handler that
  **creates topics at runtime**, and **opt-in per-worker validation** on the
  produce hot path.
- Client (`rune-client-cpp`): `openTopic<T>()` → `TypedTopic<T>` with a
  **client-side exact-fingerprint** gate, `MetadataCache` schema fields.
- A minimal `rune schema generate` tool (fetch descriptor → rune-codec
  codegen). No Rune QL.
- Build integration: consume `rune-codec` as a sibling source static library;
  make sibling-dependency include paths overridable for worktree builds.

**Out of scope (later sub-projects / specs):**
- Rune QL parsing and the full `rune` CLI (`apply`/`query`) → **0c**.
- Schema **evolution** (`ALTER`, multiple versions per topic), additive-compat
  open, and any `IncompatibleSchema` handling → a later foundation.
- The view engine, folds, joins, query execution, transactions.
- Retiring the env-var (`RUNE_TOPICS`) topic-creation path (0b **coexists**
  with it).

## 2. Program decisions inherited (locked)

- **rune-codec is the type system.** Descriptors are serializable +
  human-readable; `fingerprint()` (FNV-1a-64) is the deterministic type
  identity; `validate`/`readField` give schema-free runtime access; the codec
  generator emits typed C++ headers. All complete in 0a.
- **Additive-only evolution** (mechanism defined in 0a; **no evolution path
  ships in 0b** — every topic has exactly one version, v1).
- **Broker is the schema source of truth**, persisted + replicated + versioned.
- **Engineering philosophy (HARD RULES):** shared-nothing; ring buffers are the
  only cross-thread event passing; state-specific state machines; zero
  hot-path allocation; test + benchmark everything; regression test on every
  fix; regression suite runs first; fix-on-sight.

## 3. Decisions made during 0b brainstorming

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Coexist** with the env (`RUNE_TOPICS`) path; do not replace it. | Lowest blast radius; incremental; both paths tested. |
| D2 | **Split topicId space**: env `[0..N_env)`, internal negative sentinels (`__topic_schemas` = `-4`), runtime `DefineTopic` from `RUNTIME_TOPIC_ID_BASE = 0x40000000` up. | Env ids stay byte-compatible with existing deploys/tests; runtime ids collision-free. |
| D3 | **Exact fingerprint match** at open. | 0b has one version per topic, so additive-compat is behaviorally identical but far more code; deferred to the evolution spec where it is testable. |
| D4 | **Open folds into `GetTopicSchema`** with a **client-side** fingerprint check. | One fewer message; broker stays stateless about client types; client already needs `topicId`/routing from the response. |
| D5 | **Validation off by default**; opt-in per topic via `DefineTopic.validate`. | Matches "opt-in, per-worker, fast validation" goal; zero hot-path cost by default. |
| D6 | **Workers receive validators via their ingress ring** as immutable per-worker handles. | Shared-nothing + ring-buffer-only cross-thread passing (HARD RULES 1–2); no shared registry read on the hot path. |
| D7 | **`TypedTopic<T>` reuses the existing producer path**, keyed by `topicId`. | Inherits batching/routing/acks; typed layer only adds `encode()` + fingerprint gating. |
| D8 | **rune-codec consumed as sibling source static lib** (the `rune-fr` template), compiled under each consumer's toolchain via an overridable `RUNE_CODEC_ROOT`. | Dissolves the arm64/x86_64 question (rune-codec's native-arm64 was only for its own tests). |
| D9 | **3 worktrees + branches `feature/0b-schema-registry`** forked from **main** in rune-protocol, rune-broker, rune-client-cpp, created at execution time. rune-codec is **not** worktreed (read-only from its `main`). | Isolated multi-repo execution; design docs stay on the program's `feature/materialized-views` lineage. |

## 4. Architecture & module map

```
rune-protocol (header-only, sibling include)
  client/Protocol.h        + ErrorCodes, range relabel 64–95
  client/Dispatch.h        + isKnownTemplate 64–67
  client/SchemaMessages.h  NEW: DefineTopic/Response, GetTopicSchema/Response
  conformance/             + vectors for the 4 new messages

rune-broker (consumes rune-codec + rune-protocol as siblings)
  node/SchemaRegistry.h/.cpp   NEW: in-memory topicId→entry + name→id index
  node/BrokerNode.cpp          + __topic_schemas log/replica wiring (~like __transaction_state)
  node/NodeConfig.h            TopicSpec += {keyField, validate}
  gateway/ClientGateway.cpp    + handleDefineTopic / handleGetTopicSchema in onFragment
  gateway/TopicRegistry.h/.cpp + runtime createTopic(...) entry point
  node/Worker.h                + validation hook in executeProduceWire_ (pre-append)
  CMakeLists.txt               + RUNE_CODEC_ROOT (overridable), compile rune-codec src

rune-client-cpp (consumes rune-codec + rune-protocol as siblings)
  client/Client.h/.cpp         + openTopic<T>(), pendingSchemaOps_, GetTopicSchema round-trip
  client/TypedTopic.h          NEW: TypedTopic<T> handle (encode + reuse producer)
  client/metadata/MetadataCache.h + {schemaVersion, validate, keyField}
  tools/rune_schema_gen.cpp    NEW: minimal generate tool
  CMakeLists.txt               + RUNE_CODEC_ROOT, rune_schema_gen target, e2e test

rune-codec (0a, complete — consumed read-only)
  deserialize / fingerprint / validate / readField / generateHeader
```

### 4.1 Build integration (worktree-safe sibling consumption)

Today `rune-broker` and `rune-client-cpp` hardcode
`set(RUNE_PROTOCOL_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include)`
(and `rune-fr`). A worktree at a non-sibling path breaks these. **Change these
`set()`s to overridable cache vars** (default unchanged) and add `RUNE_CODEC_ROOT`:

```cmake
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include"
    CACHE PATH "rune-protocol include dir")
set(RUNE_CODEC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-codec"
    CACHE PATH "rune-codec repo root")
# compile rune-codec sources into the consumer (like rune-fr): add its
# include dir and ${RUNE_CODEC_ROOT}/src/*.cpp to the target.
```

Worktree builds pass `-DRUNE_PROTOCOL_INCLUDE=… -DRUNE_CODEC_ROOT=…` pointing at
the matching sibling worktrees. Existing (non-worktree) builds are unchanged.

## 5. Wire protocol (rune-protocol)

New file `client/SchemaMessages.h`, hand-coded like `MetadataMessages.h`
(plain struct + `size_t encode(uint8_t*) const` / `static std::optional<T>
decode(const uint8_t*, size_t avail)`; `putVarBytes32`/`getVarBytes32` for the
descriptor blob; `uint16` length prefix for strings).

| id | message | fields |
|----|---------|--------|
| 64 | `DefineTopic` (c→b) | `correlationId:u64, topicName:str, partitionCount:u32, keyField:str, validate:u8, descriptorBlob:varbytes32` |
| 65 | `DefineTopicResponse` (b→c) | `correlationId:u64, topicId:i32, version:u16, errorCode:u16` |
| 66 | `GetTopicSchema` (c→b) | `correlationId:u64, topicName:str` |
| 67 | `TopicSchemaResponse` (b→c) | `correlationId:u64, topicId:i32, version:u16, descriptorBlob:varbytes32, errorCode:u16` |

**Supporting changes:**
- `Protocol.h` `ErrorCode` — append after `MessageTooLarge=22`:
  `InvalidSchema`, `SchemaValidationFailed`, `UnknownTopic`, `TopicExists`.
  (`IncompatibleSchema` intentionally omitted — no evolution in 0b.)
- `Protocol.h:8` range comment: relabel `view 64-95` → `schema/view 64-95`.
- `Dispatch.h::isKnownTemplate()` — accept 64–67.

## 6. Broker

### 6.1 `__topic_schemas` persistence (mirror `__transaction_state`)

Model exactly on `BrokerNode.cpp` TX wiring (`~:975–1069`): a `storage::Log`
under `dataDir/__topic_schemas`, `PartitionConfig.topicId = -4`,
`cleanupPolicy = Compact`, majority durable; a `PartitionReplica` at
`Role::Follower`, registered in `replicaRegistry_` and on `replTransport_` at
`(-4, 0)`; entries replicate over the same `AeronPartitionTransport` as user
partitions. Records are keyed by `topicId` (compaction keeps latest per topic);
value = serialized `TopicSchema { topicId:i32, version:u16, keyField:str,
validate:u8, descriptorBlob:bytes }`.

### 6.2 `SchemaRegistry` (in-memory, per node)

`node/SchemaRegistry.{h,cpp}`:
- `topicId → RegistryEntry { Descriptor descriptor; uint64_t fingerprint;
  std::optional<Validator> validator; std::string keyField; uint16_t version;
  int32_t partitionCount; }`
- `name → topicId` index.
- `uint32_t runtimeIdHighWater` (next runtime id; starts at
  `RUNTIME_TOPIC_ID_BASE`).
- `applyRecord(const TopicSchema&)` — used by both leader-commit and
  replica-apply; deserializes the blob, builds the validator iff `validate`,
  updates both maps and `runtimeIdHighWater = max(hw, topicId+1)` for runtime
  ids.
- `rebuildFromLog()` at startup replays the compacted `__topic_schemas`
  (mirrors `ReplicatedTxnStateStore::rebuildFromLog`) so leader and every
  replica converge to identical state, and the id counter is recovered.

`NodeConfig.h` `TopicSpec` gains `keyField` and `validate` so env-declared
topics can also opt into a key field / validation (registered into the same
`SchemaRegistry` at boot when they carry a descriptor; env topics without a
descriptor remain untyped/unvalidated).

### 6.3 `DefineTopic` handler (`ClientGateway::onFragment`)

New `case` in the 64–67 range (structure mirrors `handleProduce`, `:1188`):
1. Decode; require a known/authenticated session.
2. `rune-codec::deserialize(descriptorBlob)` → on failure or a rejected
   `build()` invariant (e.g. var-index ordering) respond `InvalidSchema`.
3. **Name lookup.** If `topicName` already exists (env or runtime), respond
   `TopicExists` with the existing `topicId`/`version` (0b has no alter path;
   a differing blob on an existing name is still `TopicExists`).
4. **Runtime topic creation (the new capability).** Allocate `topicId` from
   `runtimeIdHighWater++`, set `partitionCount`, and create the partition
   log(s)/replica set via a new `TopicRegistry::createTopic(spec)` entry point
   — the runtime analogue of what `parseNodeConfig` + `TopicRegistry`
   construction do at boot (create dirs, `Log::recover()`, register with the
   balancer/placement). Reuses existing placement; only the *timing* (post-boot)
   is new.
5. Append the `TopicSchema` to `__topic_schemas`. On majority-durable commit,
   `SchemaRegistry::applyRecord` runs (locally and on replicas via
   `applyReplicated`), then respond `DefineTopicResponse { topicId, version:1,
   errorCode:Ok }`.

`GetTopicSchema` handler: look up by name in `SchemaRegistry`; respond with
`{ topicId, version, descriptorBlob }` or `UnknownTopic`.

### 6.4 Per-worker validation (`Worker::executeProduceWire_`)

**Handle distribution (D6, HARD RULES 1–2):** when `DefineTopic` commits with
`validate=1`, the gateway enqueues a `SchemaUpdate` op onto **each worker's
existing ingress ring**, carrying an **immutable per-worker `ValidatorHandle`**
(each worker builds/owns its own `Validator` from the descriptor bytes — no
shared object, no cross-thread pointer to shared state). Workers keep a local
`topicId → ValidatorHandle` map they mutate only from their own thread.

**Hot path:** in `executeProduceWire_`, immediately **before**
`appendNoSyncRestamp` (`Worker.h:~1176`): if the topic has a local validator,
run `rune-codec`'s precomputed `Validator` over each record in the batch;
on failure respond `SchemaValidationFailed` and **do not append**. No local
validator (validation off, or not-yet-pushed) ⇒ a single branch, no cost.
Validation reuses preallocated per-worker scratch; **zero hot-path allocation**
(HARD RULE 4).

## 7. Client

### 7.1 `rune schema generate` (tool, no QL)

`tools/rune_schema_gen.cpp` → executable `rune_schema_gen` (own CMake target,
reuses client connection code):
- `rune_schema_gen generate <topic> --out <dir> [--broker <endpoint>]`:
  connect → `GetTopicSchema` → `deserialize` → `generateHeader()` → write
  `<dir>/<topic>.gen.h`.
- `rune_schema_gen generate --from-file <descriptor.blob> --out <dir>`: offline
  generation from a local blob (no broker) for hermetic builds.

This is the only operator-facing surface in 0b. (The `rune` umbrella CLI and
`rune schema apply`/`query` are 0c; the verb spelling here is provisional and
0c may re-home it under `rune`.)

### 7.2 `openTopic<T>()` → `TypedTopic<T>`

`Client::openTopic<T>(std::string name)` mirrors the `ping()`
correlation→promise round-trip (`Client.cpp:376–450`):
1. Send `GetTopicSchema{ correlationId, name }`; register
   `pendingSchemaOps_[correlationId] = promise` (timeout-swept exactly like
   `pendingPings_`).
2. On `TopicSchemaResponse`: `errorCode != Ok` ⇒ fail the promise. Else compute
   `fingerprint(deserialize(descriptorBlob))` and compare to `T::kFingerprint`.
   **Exact match (D3, D4):** mismatch ⇒ fail with "generated type is stale —
   rerun `rune schema generate`".
3. On match: populate `MetadataCache` with `{ topicId, schemaVersion, validate,
   keyField }` and resolve the promise with a `TypedTopic<T>`.

`TypedTopic<T>` (`client/TypedTopic.h`) is a thin typed wrapper over the
existing producer path (D7): `send(const T& rec)` calls `T::encode(buf)` into a
**reusable per-handle buffer** (no per-send heap alloc — HARD RULE 4) and routes
to the current produce machinery by `topicId`. It does not re-validate
client-side; the broker validates iff `validate` is on.

### 7.3 `MetadataCache`

Per-topic `{ schemaVersion:u16, validate:bool, keyField:string }` alongside the
existing routing entry. The full descriptor is **not** cached (only `generate`
and `openTopic` need it, and both fetch fresh via `GetTopicSchema`).

## 8. Error handling

| Condition | Response |
|-----------|----------|
| Malformed / invariant-violating descriptor blob at `DefineTopic` | `InvalidSchema`, no creation |
| `DefineTopic` on an existing name | `TopicExists` (+ existing `topicId`/`version`) |
| Produce fails validation (topic has `validate=1`) | `SchemaValidationFailed`, no append |
| `GetTopicSchema`/`openTopic` on unknown topic | `UnknownTopic` |
| `openTopic<T>` fingerprint mismatch | client-side error ("regenerate type"); no server round-trip beyond the fetch |
| Registry not yet caught up on a replica | reads reflect last-applied `__topic_schemas`; a define is visible after its majority-durable commit applies |

## 9. Testing & benchmarks (HARD RULES 5–7)

**rune-protocol:** encode/decode round-trip unit tests for all 4 messages +
one conformance vector each (repo already ships conformance vectors).

**rune-broker:**
- `SchemaRegistry` unit tests: rebuild-from-log, runtime-id high-water
  recovery, name→id index, validator built iff `validate`.
- Integration: `DefineTopic` a topic; with `validate=on`, produce a valid and
  an invalid record; assert `SchemaValidationFailed` + no append for the bad
  one and a durable append for the good one.
- Replication integration: a follower rebuilds an **identical** registry from
  `__topic_schemas`.
- Restart: registry (and runtime-id counter) survives a restart.
- Benchmark: produce hot path validate-off vs validate-on delta.

**rune-client-cpp:**
- Unit: exact-fingerprint gate (match opens; mismatch errors).
- **Headline e2e** (build-time gen from a **checked-in descriptor blob**, like
  rune-codec's own round-trip test): checked-in blob → CMake custom command runs
  `rune_schema_gen --from-file` → generated header compiled into the test →
  against a live broker: `DefineTopic` → `openTopic<T>` → `send` → consume →
  `decode` → assert round-trip. Deterministic, hermetic, no broker needed at
  compile time.
- A separate runtime test covers the live `GetTopicSchema` → generate fetch.

**Regression suite** (`*Regression*`): seeded with any bug found during
execution; run first, before all other tests.

## 10. Module boundaries (isolation check)

- `SchemaRegistry` — owns registry state + rebuild; depends only on rune-codec
  (`Descriptor`, `fingerprint`, `Validator`) + the `TopicSchema` record.
  Testable with no networking.
- `SchemaMessages.h` — pure (de)serialization; testable in isolation.
- `TypedTopic<T>` — depends only on the producer interface + `T::encode`.
- `rune_schema_gen` — depends on the client fetch + rune-codec codegen;
  `--from-file` path has no broker dependency.
- Validation hook — a single call site in `executeProduceWire_` reading a
  worker-local map; no coupling to the registry's internals.

## 11. Open items deferred to plan/implementation

- Exact reuse points in `TopicRegistry`/placement for runtime `createTopic`
  (the plan pins the function signatures after reading the current code).
- The `SchemaUpdate` op encoding on the worker ingress ring (reuse the existing
  `WorkerOp` framing).
- Whether env-declared topics with a descriptor are registered eagerly at boot
  or lazily on first access (default: eagerly, alongside `TopicRegistry`
  construction).
