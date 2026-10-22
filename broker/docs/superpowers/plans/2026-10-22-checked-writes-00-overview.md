# Checked Writes — Program Overview (Plan 00) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> This file has no tasks. It is the **contract** that the eleven part plans (`2026-10-22-checked-writes-01…11-*.md`) implement. Every part plan copies the Global Constraints below, cites this file for every cross-part name, and must not rename, renumber or re-home anything fixed here. A part plan that needs a change to this contract stops and asks the controller. The contract changes in this file only, and every affected part plan is re-checked.

**Goal:** Typed topics that always validate, broker-enforced write intents and a key index, broker-assigned `autoincrement` ids, 128-byte composite and derived view keys with point and prefix reads, `unique` and `check` view constraints judged when the views fold each record (design B), persisted views over transient sources, and transactions that vote before commit. All defined in Rune QL and enforced by the broker for every producer.

**Architecture:** A per-partition write hook (a template policy inside `rune::storage::handleProduceBatch`, after idempotent dedupe) validates each batch, assigns ids and applies the per-write requirements. Its state is reached through one pointer stored next to the partition's `RcSlot`. Nothing is judged at write time. On the leader, the view engine judges every record of a **verdict source** after commit and in offset order. Verdict views compute prospective rows with the same code that applies them, evaluate the rules, then either every view applies the record or none does. Rejections become durable through `VerdictMarker` control records in the source log. Every replica indexes them in a `RejectedOffsetIndex`. Consumers, other views and the cleaner never see a rejected record. Constraint changes take effect at `ConstraintSetChange` control records, and transactions are judged at `TxnJudge` control records during a coordinator vote.

**Tech Stack:** C++23. Monorepo: `broker/`, `protocol/` (header-only wire), `codec/` (schemas, catalog, security), `client-cpp/`, `ql/`. Aeron rings and SBE (internal ring schema `protocol/schema/rune-internal-ring.xml`). GoogleTest and ctest. Google Benchmark for the benches. Conan 2 and CMake ≥ 3.31.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` (approved 2026-10-22). Executors read the spec section their part implements in full, together with this file.

---

## Execution context (all parts)

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: broker, protocol, client, codec, ql and crypto. Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp`: the client's own suites and examples.
  - `build/sub-codec`: `rune_codec_tests` and `rune_catalog_tests`. CatalogGolden runs only here.
  - `build/sub-ql`: the ql suites.
  - `build/rel`: Release with `BUILD_BENCHMARKS=ON`. The binary is `build/rel/broker/rune_broker_benchmark`.
  - `build/asan` (ASan+UBSan), `build/tsan`, and `build/client-tsan` (the client under TSan, `BUILD_TESTING=ON`).
- **Codec sources are listed three times.** A new `codec/src/*.cpp` goes into `codec/CMakeLists.txt`, into `broker/CMakeLists.txt` (its `rune_codec` copy, near line 144), and into `client-cpp/CMakeLists.txt` (its `if(NOT TARGET rune_codec)` copy).
- **SBE.** The internal-ring flyweights under `protocol/include/rune_protocol_internal_sbe/` are generated and committed. Regenerate them with a local `sbe-all-1.40.2.jar` given through `$SBE_JAR`. Never commit the jar.
- **Docker e2e, dr-e2e and VM benches** run only on the shared Linux VM (`$RUNE_VM_HOST`, under its lock file), never on local Mac docker. Prepare the clean box once for benches and restore it afterwards (`broker/bench/BENCHMARKING.md`).
- **Commits.** Use `git add <paths>`, never `.superpowers/` or build logs. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create/export/upload`.

## Global Constraints

Each line is verbatim from the spec or the repo rules. Every task of every part implicitly includes this section.

**Error codes** (`protocol/include/rune/protocol/client/Protocol.h:105`, `ErrorCode`, append-only):
- `ConstraintViolation = 51`: the record broke a rule when it was folded. Constraint id `viewObjectId << 16 | ordinal`, ordinal 0 = `unique`. Fold, per record, definite.
- `KeyExists = 52`: `insert` on a key that exists. Fold, per record, definite.
- `KeyNotFound = 53`: `update` or delete-existing on a key that's absent. Fold, per record, definite.
- `AutoIncrementExhausted = 54`: the counter has reached `autoincrement.max`. The batch isn't stored. Write, definite.
- `TxnTooLarge = 55`: a transaction wrote more than `txn.judged.records.max` records to one voting partition. The transaction aborts. Vote, definite.
- `ViewKeyExpressionInvalid = 56`, `ViewConstraintInvalid = 57`, `ViewUniqueInvalid = 58`: compile errors (ruling SD3).
- Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.
- "Definite" for 51–53 means *never visible*. 51–53 reach only `View` writers on client version 12.

**Versions.**
- `kClientVersionCheckedWrites = 12` and `kClientVersion = 12`.
- `kBrokerVersionCheckedWrites = 7` and `kBrokerVersion = 7`. Today `kClientVersion` is 11 and `kBrokerVersion` is 6 (`Protocol.h:327`, `Protocol.h:360`).
- Feature level `MetadataFeature::kCheckedWrites = 4` and `kLatest = kCheckedWrites` (`broker/libs/replication/include/rune/replication/ReplicationMessages.h:470-472`; today `kLatest = kBoundProducerIds = 3`).
- Plan v2, checkpoint v3 and catalog `formatVer` 2 follow the existing frozen-format rules: old formats still decode.

**Values.**
- `verdict.interval` = **100 ms**.
- `txn.judged.records.max` = **4096**.
- `autoincrement.reserve` = **4096**.
- Id cap **2^53 − 1** (`autoincrement.max` may be lower).
- View key width K ≤ **128 bytes**.
- Topic key field ≤ `Key::kMaxInline` = **16 bytes** (`client-cpp/include/rune/client/Key.h:35`).
- The `autoincrement` side window holds **32** batches = `kIdempotentMaxInFlightBatches` (`Protocol.h:16`).
- Partition `p` assigns `n·P + p` for n = 1, 2, …. Id 0 is never assigned.
- Index slot **13 bytes** (`tag u8 | word u64 | rid u32`). Plan key functions: 0 none, 1 `lower`, 2 reserved `casefold`.
- `lower(string(N))` width = the largest lowered width at the pinned Unicode version (⌈3N/2⌉ at Unicode 15/16).
- Wire `acks` byte: bit 0 = `All`, bit 1 = `View`. Other bits are refused with `InvalidRequest`.
- Intent bits: record `attributes` bits 0–1. 0 = upsert/delete, 1 = insert, 2 = update/delete-existing. 3 and the other bits are refused with `InvalidRequest`.
- `partitions` defaults to 1. Strings are UTF-8, and `lower()` is Unicode simple lowercasing.

**Budgets** (hard gates, spec §14):
- Write hook: autoincrement assignment, including the scratch copy, < **1 µs p50** per batch of up to 4 KiB (`ProduceValidateBench`).
- New typed-topic validation: ≤ **1 µs p50** for a one-record batch.
- Judging: ≤ **1 µs per record** for one unique view plus one count check (`ViewFoldBench`).
- `ViewFoldBench` with derived keys plus the owner map: ≤ **10 µs** per event batch.
- No ABAB n=3 regression on `ProducePathBenchmark` and `WorkerProduceLatencyBench` for untyped topics and for already-validated typed topics.
- `ViewTableBench`: no change for K ≤ 8.
- Repo latency gates stay: view update < 10 µs, point read < 50 µs, QL ≤ 2–5 µs.

**Repo rules** (root `CLAUDE.md` §1–§4):
- **Hot path.** The produce, consume, fold and query paths are zero-alloc, take no lock, use no atomics as coordination and no `thread_local`, make no virtual call and use no `std::function`. Hot paths use concrete types plus a branch, or templates. Control-plane work (installs, DDL, recovery scans) runs as cold worker ops and may allocate.
- **States.** Rich-state variants, never scattered booleans.
- **Clocks.** One injected clock. Tests advance it and call `doWork`, with a hard iteration cap and never a wall-clock wait.
- **Regression tests.** Every fix ships a `*Regression*` test. Each task runs the affected-area tests.
- **Checkpoints** run the full `-R Regression -LE latency` suite, plus ASan+UBSan and TSan for the broker and the client, plus CatalogGolden.
- **Interaction matrix.** Every feature is tested across its matrix (§ Shared test infrastructure). Each plan has an "Interaction matrix" section and a "Review Focus" section, with 1–3 phases of small tasks.
- **Real e2e.** Nothing is done until its real e2e path is green on the **default** config (shard worker mode).
- **No weakened tests.** A weakened assertion is recorded as a deviation. Urgent ones are fixed now and the rest are filed as GitHub issues with type, severity and area labels.

## Spec drift and rulings (binding for every part)

Each ruling is a decision that the part plans must follow. The owner can overturn any of them. Each part's plan restates the rulings it depends on.

- **SD1 — §4 vs §10.1, untyped topics.** §10.1 says "The field list is required (§4)". §4 and owner decision 7 say a topic without a field list is an untyped raw log. **§4 wins.** The grammar keeps `topic events;` as untyped. "Required" means only that every database feature needs a field list.
- **SD2 — Validation runs before dedupe today.** `schemaRejects_` runs in `produceCore_` before `handleProduceBatch` (`broker/libs/node/include/rune/node/Worker.h:8255`), so before `producerState.validate` (`broker/libs/storage/src/ProducePath.cpp:205`). Part A keeps validation at that call site, moved onto the per-partition pointer (C7). Part 3 moves it into the hook, after dedupe (§3).
- **SD3 — Compile-error codes.** §10.2 names `kViewKeyExpressionInvalid`, `kViewConstraintInvalid` and `kViewUniqueInvalid`. Compile errors are wire codes today (`AdminWorker.cpp:51`, `kViewKeyInvalid = 35`), but §11.1 lists no codes for them. They get **56, 57 and 58**. Only a v12 client can author a plan v2, so no mapping for older clients is needed. The message-search mapping at `AdminWorker.cpp:1239-1245` is removed by Part QL.
- **SD4 — The rejection list is unbounded.** §6.7 says the fixed ack scratch arrays "grow by 8 bytes", but a per-record rejection list is variable. **Ruling:** a batch written with `View` to a verdict source holds at most **4096 records** (`kMaxJudgedBatchRecords`).
  - The client splits such batches.
  - The hook refuses a bigger one with `InvalidRequest`.
  - So a `ProduceResponse` is at most 32,824 bytes (C5), well below the 68 KiB egress scratch.
  - The ack scratch is sized to `ProduceResponse::kMaxEncodedSize`.
- **SD5 — Constraint id width.** `constraintId` is u32 = `viewObjectId << 16 | ordinal`, but catalog object ids are u64 and dense (`codec/src/catalog/ApplyV1.cpp:92`). **Ruling:** creating a `unique` or `check` view whose `objectId ≥ 65536`, or with more than 65535 ordinals, is refused with `ViewConstraintInvalid` and the message "constraint ids exhausted". This is flagged for the owner.
- **SD6 — The feature level is needed by Part A.** §4 gates existing typed topics behind `kCheckedWrites`, but §16 lists the level under the write hook. **Part A defines `kCheckedWrites` and its propagation (C2)**, and Part 3 uses it.
- **SD7 — `lower()` is needed before Part E.** Derived keys (`key lower(name) as nameKey`, §7.1, Part D) and QL literals (§10.1) need the lowering table. **Part D owns the codec `lower` and the Unicode pin (§8.1 mechanics).** Part E owns §8.2, §8.3 and the §8.7 wiring.
- **SD8 — The key index needs wide keys.** The key index (§5.2, Part B) is keyed by wire keys up to 16 bytes, but `ViewTable` keys are `uint64_t` until Part D (`broker/libs/view/include/rune/view/ViewTable.h:95`). **Ruling:** Part B builds the key index for wire keys ≤ 8 bytes. `key.index` on a topic whose key field is wider than 8 bytes is refused by DDL with `InvalidRequest`, "key.index needs a key of at most 8 bytes", until **Part D lifts it**. That is a Part D deliverable. `autoincrement` keys are ≤ 8 bytes.
- **SD9 — Transient topics.** The spec makes constraints over transient sources a non-goal but is silent on `key.index`, intents and `autoincrement`. Each of them needs a verdict source, whose markers would live in a ring. **Ruling:** all three are refused at DDL on transient topics with `InvalidRequest`, "<feature> needs a persisted topic".
- **SD10 — Legacy worker mode** (`NodeConfig::WorkerMode::Legacy`, `NodeConfig.h:257`). The gateway encodes produce acks there (`ClientGateway.cpp:6930`), and it cannot carry v12 ack fields from a worker. Following Phase 2c FD1, a produce with the `View` bit, any non-zero intent, or a key-0 create is answered `InvalidRequest` in legacy mode. Plain writes are judged as usual. Shard mode is the default.
- **SD11 — The client cannot see the cluster feature level.**
  - The client gates `View`, intents, key-0 creates, `GetViewRowK`, `ScanViewRows` and `GetStats` on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`. Below that it fails locally with `OpStatus::BrokerTooOld`.
  - A v7 broker in a cluster whose committed level is below 4 answers those writes and DDL `InvalidRequest`. Nothing is appended.
- **SD12 — Inconsistent `acks` decode.** The gateway uses `msg.acks == 1` (`ClientGateway.cpp:3872`) and the worker uses `msg.acks != 0` (`Worker.h:8082`). Both move to the one decoder in C4.
- **SD13 — No `show topic` / `show view`, no stats transport and no metrics export exist.** `ql/include/rune/ql/Ast.h:92` has neither statement, and no view counter leaves the worker (`ViewFoldCounters`, `WorkerViewSet.h`). **Ruling:**
  - Part QL adds `show topic` and `show view`, and the `GetStats` / `StatsResponse` wire (84/85, C16).
  - "Metrics export" is `rune stats --json` over `GetStats`.
  - Aeron-counter export of §13 stats is not built. Part QL files it as an issue. This is flagged for the owner.
- **SD14 — Knob scope.** `verdict.interval` and `txn.judged.records.max` are **broker** settings, because before-images are pre-allocated per worker (§G.7). They are `NodeConfig::verdictIntervalMs` (env `RUNE_VERDICT_INTERVAL_MS`, default 100) and `NodeConfig::txnJudgedRecordsMax` (env `RUNE_TXN_JUDGED_RECORDS_MAX`, default 4096). `autoincrement.reserve` and `autoincrement.max` are topic settings (C13).
- **SD15 — #537 refold rule.** No checkpoint format change: Part D owns v3. At recovery, a persisted view whose source partition's `RcState` aborted index is non-empty discards its v1/v2 checkpoint and refolds from the log start.
- **SD16 — The `View` ack and open transactions.** A `View` waiter on a partition behind an open transaction waits for the LSO (§8.9). Waiters live in their own queue (C4), so `All` acks never queue behind them.

## Review Focus (program level; each part adds its own five)

1. **A dedupe retry of a stored batch on a verdict source.** This covers a retry before the fold judged the batch, one after, and one after failover plus compaction. It must get the original offsets, the original `assignedBase` and the same per-record verdicts. It must never get a second id or a second judgment (§6.6, §11.3).
2. **Every reader of a log meets control records of types 16–19.** These readers are `RcBuilder::step` (`ReadCommittedView.cpp:315`), the PSM rebuild (`RcState.h:660`), `LogCleaner::scanTxnState` (`LogCleaner.cpp:229`), the push and filter paths, `forEachRecordSpan`, the folds, `SubscriberHub` (`SubscriberHub.cpp:600`) and `DataCodec` (`DataCodec.h:341`). Each must skip the records and must never take one for a COMMIT or ABORT marker.
3. **Mixed versions.** A v11 broker receiving `acks` 2 or 3 answers `InvalidRequest` (`DataMessages.h:141`). A v11 client never receives a v12 `ProduceResponse` layout, codes 51–58 or `assignedBase`. A v12 client facing a v6 broker fails locally with `BrokerTooOld`.
4. **Consumers of a verdict source while a constraint view builds, or while the fold is stalled.** Delivery waits, never hangs past the build, never delivers an unjudged record, and the `View` ack deadline still answers `Internal`.
5. **A `View` batch of more than 4096 records on a verdict source.** The client splits it, a raw producer gets `InvalidRequest`, and no ack ever exceeds `kMaxEncodedSize` (SD4).

---

## Plan index

| NN | File | Spec | Depends on (merged first) | Delivers to later parts |
|---|---|---|---|---|
| 01 | `2026-10-22-checked-writes-01-aborted-fold.md` | #537, §8.8 last bullet | — | `rune::view::FoldSkip` (C9.2) threaded through `foldBatch` / `foldAggregateBatch`, rebuild and transient folds; SD15 refold |
| 02 | `2026-10-22-checked-writes-02-typed-validation.md` | §4 (Part A), §12 level | — | `kCheckedWrites` and its propagation (C2); `TopicWriteSpec`, `PartitionWriteState`, the RcSlot pointer and `WriteSpecUpdate` (C7); always-on validation, wire key = key field, UTF-8 (`codec/include/rune/codec/Utf8.h`); `Key` truncation turned into an error; the bind checks, the `raw` marker, `partitions` defaulting to 1, DDL refusals on untyped and keyless topics |
| 03 | `2026-10-22-checked-writes-03-write-hook.md` | §3 write path, §8.9, §11.2, §12 wire | 02 | The hook policy (C8); the `Acks` flags and `View` waiters (C4); the v12 wire skeleton (C1, C5); codes 51–58 with `errorCodeName`; intent constants and the `RecordSpan` extension (C3); `WriteNotAllowed`, `BrokerTooOld`; the per-write requirement table; SD10, SD11; the alloc-gate binary |
| 04 | `2026-10-22-checked-writes-04-verdict-pipeline.md` | §3 fold, §8.4–§8.8, §11.3 | 01, 03 | `SourceRecord` / `Prospect` and prepare/apply (C9); `VerdictSource`; control-record layout and types (C10); `RejectedOffsetIndex`, `VerdictCleanState` and the cleaner bound (C11); `ConstraintSetChange` timeline (C12); consumer bound and filter; ack verdict trailer; the shared matrix and stress harness (§ Shared test infrastructure) |
| 05 | `2026-10-22-checked-writes-05-intents-key-index.md` | §5 (Part B) | 04 | The key index as `VerdictViewKind::KeyIndex`; intents 52/53; catalog `formatVer` 2 with `TopicKeyFeatures` and the DefineTopic / TopicSchemaResponse fields (C13); `WriteIntent`, `RecordOp`, `WriteResult`, typed `insert`/`update`/`upsert`/`remove`/`removeExisting` (C15); the first docker e2e |
| 06 | `2026-10-22-checked-writes-06-autoincrement.md` | §6 (Part C) | 05 | The modulo partitioner; `AutoIncState`, reservations, side window and cleaner retention; `IMPORT` (C17); `assignedBase` filled; `insertAndWait(...).key()`; `OpStatus::StoredIdUnknown` |
| 07 | `2026-10-22-checked-writes-07-wide-view-keys.md` | §7, §9 (Part D), §8.1 mechanics (SD7) | 05 | `ViewKey` tables, seeded hash, owner map, prefix index, checkpoint v3; plan v2 format with every field (C14), and the authored plan moved to codec; codec `lower` and the key encoding; `GetViewRowK` 81 and `ScanViewRows` 82/83; client read API and key builder; key index widened to 16 bytes (SD8) |
| 08 | `2026-10-22-checked-writes-08-ql.md` | §10 | 06, 07 | Grammar (topics, `create view`, `select`), prepare to plan v2, CLI decode, `show topic` / `show view`; `GetStats` 84/85 and the stat ids (C16); removal of the message-search mapping (SD3) |
| 09 | `2026-10-22-checked-writes-09-view-constraints.md` | §8.2, §8.3, §8.7 (Part E) | 08 | `unique` and `check` (`VerdictViewKind::Unique` / `Check`), predicate evaluation on `Prospect`, unique conflicts and promotion, constraint create / replace / drop through C12; `Client::constraintName`; the RegressorsPath e2e scenario |
| 10 | `2026-10-22-checked-writes-10-persisted-over-transient.md` | §9a (Part F) | 01 | 37 lifted; the keyless-source refusal; the `reconcileTransient_` logging change (`WorkerViewSet.cpp:367`); docs |
| 11 | `2026-10-22-checked-writes-11-txn-vote.md` | §9b (Part G) | 06, 09 | `TxnVote` wire and ops (C18), `TxnJudge`, deferred judging, before-images and row locks, atomic batches, one-step visibility, `TxnTooLarge`; lifting the pre-G refusals of Part 3 |

**Docs (§16.12)** are not a separate plan. Each part updates the doc sections its file-map rows name. Part 11 ends with a cross-check of the whole §16.12 list. The site's views section lives in the separate `rune-site` repo and needs the owner's explicit go-ahead before any deploy.

### What each part must hand over (acceptance for the next part)

- **01:** `foldBatch` and `foldAggregateBatch` take `const FoldSkip&` (C9.2), and every caller passes it: `driveView_` (`WorkerViewSet.h:663`), rebuild and transient. A view over an aborted transaction equals a fresh refold.
- **02:** every typed topic's partition has a non-null `PartitionWriteState*` on its owning worker. Untyped topics have `nullptr`. Validation and the wire-key check go through it. `clusterFeatureLevel_` is on every worker and in the AdminWorker.
- **03:** `handleProduceBatch<LogT, HookT>` exists, and `CheckedWriteHook` runs for every non-null state. `Acks::View` works end to end on a partition with ordinary views: it is acked after `minFoldCursor > lastOffset`. Every §11.2 refusal is in place, with "constrained" and "has key index" both false until 04/05 set them. The v12 frames encode and decode with empty verdicts and `assignedBase = 0`.
- **04:** a verdict source works end to end with the test-only `TestReject` verdict view (C9.4): markers, index, consumer filter, other views skipping, the cleaner, refold below `W`, failover re-judging, `ConstraintSetChange`, and the ack trailer. The matrix, stress and model harnesses exist with the generic invariants.
- **05:** `key.index` and intents work on a real broker. Catalog v2 encodes every `TopicKeyFeatures` field. `WriteResult` exists.
- **06:** key-0 creates get ids. `assignedBase` and `WriteResult::key()` work. `IMPORT` is enforced.
- **07:** plan v2, every key form, both reads and the client key builder work. The codec `lower` exists with its pinned version. The key index takes 16-byte keys.
- **08:** every QL statement in the spec's target block (§1) parses, prepares and round-trips. `GetStats` serves every counter that exists.
- **09:** the §1 target block works end to end: `user_identity`, `character_name`, `account_stats` and `max_characters`.
- **10, 11:** as their spec sections say.

---

## Cross-part Interface Contract

Each item names the owning part. Other parts use the name exactly as given.

### C1. Protocol constants (`protocol/include/rune/protocol/client/Protocol.h`)

- **`ErrorCode`, owned by Part 03.** Append after `UnstableOffsetCommit = 50` (`Protocol.h:209`): `ConstraintViolation = 51`, `KeyExists = 52`, `KeyNotFound = 53`, `AutoIncrementExhausted = 54`, `TxnTooLarge = 55`, `ViewKeyExpressionInvalid = 56`, `ViewConstraintInvalid = 57`, `ViewUniqueInvalid = 58`.
  - Part 03 also extends `errorCodeName` (`client-cpp/src/cli/QlEngine.cpp:133`) with names 48–58 and moves its `static_assert` to `ViewUniqueInvalid`.
  - Part 03 adds `[[nodiscard]] constexpr bool clientUnderstandsCheckedWrites(int32_t clientVersion) noexcept` (≥ 12).
- **Versions, owned by Part 03.** `kClientVersionCheckedWrites = 12`, `kClientVersion = 12`, `kBrokerVersionCheckedWrites = 7`, `kBrokerVersion = 7`. All later v12 wire (07, 08, 11) is gated on the same constants.
- **`TemplateId`** (view range 64–95, `Protocol.h:90-102`): `GetViewRowK = 81` (07), `ScanViewRows = 82` (07), `ScanViewRowsResponse = 83` (07), `GetStats = 84` (08), `StatsResponse = 85` (08).
  - `GetViewRowK` is answered by the existing `ViewRowResponse` (73).
  - Each owner extends `Dispatch.h::isKnownTemplate()` and the conformance vectors.

### C2. Cluster feature level (Part 02)

- **The level.** `MetadataFeature::kCheckedWrites = 4` and `kLatest = kCheckedWrites` (`ReplicationMessages.h:470-472`).
- **Propagation.** BrokerNode posts every change of the committed level, read from `MetadataService`'s state machine, and posts the current level once at boot:
  - control → admin: `kMsgClusterFeatureLevel = 26`, `[level:i16]`, on `controlToAdminRing` (the enum at `broker/libs/node/include/rune/node/AdminChannels.h:48`; 26 is unused). The AdminWorker keeps `int16_t clusterFeatureLevel_`.
  - control → **every** worker on its op ring: `WorkerOp::Type::ClusterFeatureLevel = 26` (SBE `OpType` `CLUSTER_FEATURE_LEVEL = 26`, `rune-internal-ring.xml`), with the level in `nextOffset`. It is cold, never ownership-gated, and FIFO behind the role ops.
- **Worker state.** `int16_t clusterFeatureLevel_ = MetadataFeature::kBase` and `[[nodiscard]] bool checkedWritesEnabled_() const noexcept` (≥ 4).
- **Gates.** Below the level the worker skips every check §4 adds to existing typed topics (Part 02) and refuses everything §12 lists (Part 03). The AdminWorker refuses the §12 DDL with `InvalidRequest`.

### C3. Record attributes and the per-record visitor (Part 03)

- **Intent constants.** In `protocol/include/rune/protocol/RecordBatch.h`, namespace `rune::protocol::record`:
  - `inline constexpr uint8_t kAttrIntentMask = 0x03;`
  - `inline constexpr uint8_t kAttrReservedMask = 0xFC;`
  - `enum class Intent : uint8_t { Upsert = 0, Insert = 1, Existing = 2 };` (value 3 is reserved and refused).
  - `[[nodiscard]] constexpr uint8_t intentOf(uint8_t attributes) noexcept`.
- **Builder.** `RecordBatchBuilder::append(std::string_view key, std::string_view value, int64_t timestamp, uint8_t attributes = 0)`. Today it pushes a literal 0 at `RecordBatch.h:150`.
- **`RecordSpan`** (`protocol/include/rune/protocol/BatchRebuild.h:17`) gets appended members `uint8_t attributes = 0;`, `std::string_view key;` and `uint32_t index = 0;` (the record's ordinal in the batch). `forEachRecordSpan` fills them. The hook (validation, intents, id patch offsets = `key.data() - batch`) and the folds use only this visitor, never `RecordBatchReader::forEachRecord`, which skips the attributes (`RecordBatch.h:417`).
- **Tombstones** stay `value.empty()` (`LogCleaner.h:364`). A null value is a tombstone too.

### C4. Ack flags (Part 03; Part 04 adds the verdict condition)

- **Client enum** (`client-cpp/include/rune/client/producer/Producer.h:79`): `enum class Acks : uint8_t { None = 0, Leader = 1, All = 2, View = 4 };`
  - It gets `constexpr Acks operator|(Acks, Acks)`, `operator&`, and `[[nodiscard]] constexpr bool has(Acks set, Acks flag) noexcept`.
  - `Leader | All` is normalized to `All`.
  - `Client::producer` and the `ClientShard` constructor throw `std::invalid_argument` for `View` and `None | View`.
- **Wire** (`protocol/include/rune/protocol/client/DataMessages.h`, struct `Produce`):
  - `inline constexpr uint8_t kAcksWireAll = 0x01, kAcksWireView = 0x02, kAcksWireKnown = 0x03;`
  - Helpers `[[nodiscard]] constexpr bool acksAll(uint8_t)` and `acksView(uint8_t)`.
  - `Produce::decode` refuses `acks & ~kAcksWireKnown` (today `acks > 1`, `DataMessages.h:141`).
  - The client encodes `Leader` → 0, `All` → 1, `Leader|View` → 2, `All|View` → 3, and sends bit 1 only under SD11.
- **Broker.** `Worker::ProduceFields::acksAll` (`Worker.h:8130`) becomes `uint8_t acks` (the wire byte), with `acksAll()` and `acksView()`. The gateway path (`ClientGateway.cpp:3872`) and the ingress path (`Worker.h:8082`) use `rune::protocol::client::acksAll()`.
- **View waiters.**
  - `struct PendingAcksView` holds the same fields as `PendingAcksAll` (`Worker.h:5812`) plus `uint8_t acks`.
  - They are queued in `OrderedWaitQueue<PendingAcksView> pendingAcksView_`, reserved at `kMaxOwedReturns` and counted in `reservedAckSlots_()`.
  - `int flushOwedAcksView_(int64_t nowNs)` runs right after `serveHeldViewReads_()` in both duty loops (`Worker.h:1848` and `Worker.h:1979`).
  - A waiter is released when the HW is above `lastOffset` (both `All|View` and `Leader|View`: the fold never passes the HW) **and** `viewSet_->minFoldCursor(topicId, partition) > lastOffset` **and**, on a verdict source (04), `state->verdicts->judgedThrough() > lastOffset`.
  - `NotLeader` on a lost leadership and `Internal` on `ackTimeoutNs_`, as for `All`.
- **`WorkerViewSet::minFoldCursor`.** `[[nodiscard]] int64_t WorkerViewSet::minFoldCursor(int32_t srcTopicId, int32_t partition) const noexcept` returns the minimum `foldCursor` of every view interested in `srcTopicId` for that partition. An interested view with no instance yet counts as 0. It returns `INT64_MAX` when no view is interested (Part 03).

### C5. `ProduceResponse` v12, the ack sink and the return carrier (Part 03 layout; 04 verdicts; 06 `assignedBase`)

- **`ProduceResponse`** (`DataMessages.h:193`):
  - `BODY_FIXED_V12 = 30`, with `assignedBase u64` at body offset 22.
  - Then an optional trailer `[count u16][count × {recordIndex u16, code u16, constraintId u32}]`.
  - New members: `uint64_t assignedBase = 0;` (0 = none; id 0 is never assigned), `bool hasVerdicts = false;`, `uint16_t verdictCount = 0;` and `std::span<const uint8_t> verdictBytes;` (encoded entries; zero-copy on decode).
  - Helpers: `template <class Fn> void forEachVerdict(Fn&&) const noexcept`, `static constexpr uint16_t kMaxVerdictEntries = 4096;` and `static constexpr std::size_t kMaxEncodedSize = 24 + 30 + 2 + 8 * 4096;` (32,824).
  - `encodeFor(buf, clientVersion)` writes byte-identical v1/v0 frames below version 12.
  - The decoder reads `assignedBase` iff `blockLength ≥ 30`, and the trailer iff bytes remain after the block. An empty list (`count = 0`) means every record was accepted. The trailer is present only on a `View` ack of a verdict source.
- **`AckEntry`** (`broker/libs/node/include/rune/node/SessionAckSink.h:111`) appends `uint64_t assignedBase = 0; uint32_t verdictRun = kNoVerdictRun; uint16_t verdictCount = 0; uint8_t hasVerdicts = 0;`.
  - `encode_` (`SessionAckSink.h:1065`) writes them.
  - `ackPlainScratch_` and `ackSealedScratch_` (`SessionAckSink.h:1136-1138`) are sized `ProduceResponse::kMaxEncodedSize` (+ `kCryptoOverhead`).
  - `encodedSizeFor` is used at the claim (`SessionAckSink.h:360`/`382`) per entry.
- **Verdict ack arena (04).** The rejection entries of queued acks live in a sink-owned, fixed-capacity `VerdictAckArena`. Its default capacity is `kVerdictAckArenaEntries = 65536` (8 bytes each), reserved at construction.
  - `[[nodiscard]] bool reserveVerdicts(uint16_t n, uint32_t& run) noexcept` returns false when full. The worker then **defers the release** of that ack to a later pass and never drops it.
  - A run is freed when its ack is claimed.
- **`WorkerReturn`** (`WorkerOp.h:487`) appends in-process members `uint64_t assignedBase = 0; uint32_t verdictRun = 0; uint16_t verdictCount = 0; uint8_t hasVerdicts = 0;`. `WorkerReturnCodec` does **not** encode them (SD10), so the `ProtocolRegression.WorkerReturnLayoutUnchanged` pin stays as it is.
- **`ProduceOutcome`** (`broker/libs/storage/include/rune/storage/ProducePath.h:103`) appends `uint64_t assignedBase = 0;`.

### C6. Client results (Parts 03, 05, 06, 09, 11)

- **`SendResult`** (`Producer.h:302`), append-only: `KeyTooLong = 12` (02: a key wider than `Key::kMaxInline` or not the key field's width) and `WriteNotAllowed = 13` (03: a §11.2 requirement the client checks locally).
- **`OpStatus`** (`client-cpp/include/rune/client/Completion.h:54`), append-only: `BrokerTooOld = 12` (03, SD11) and `StoredIdUnknown = 13` (06: "stored; id unknown").
- **`Completion` for `OpKind::Send`** on a checked write:
  - `status = BrokerError` with `errorCode` 51–53 for a rejected record, otherwise `Ok`.
  - `reserved0` = `constraintId`.
  - `aux` = the assigned id (06; 0 = none).
  - Accessors in `Completion.h` (03): `[[nodiscard]] constexpr uint32_t constraintIdOf(const Completion&) noexcept` and `[[nodiscard]] constexpr uint64_t assignedKeyOf(const Completion&) noexcept`.
- **`WriteResult`** (05, new file `client-cpp/include/rune/client/WriteResult.h`), returned by every `…AndWait` write of `TypedTopic`:
  - `[[nodiscard]] bool ok() const noexcept;`
  - `[[nodiscard]] uint16_t code() const noexcept;`
  - `[[nodiscard]] uint32_t constraintId() const noexcept;`
  - `[[nodiscard]] uint64_t key() const noexcept;` (06 fills it; 0 = none)
  - `[[nodiscard]] OpStatus status() const noexcept;`

### C7. Per-partition write state, next to `RcSlot` (Part 02 creates; later parts append members)

- **New header `broker/libs/node/include/rune/node/WriteState.h`.**
  - **`struct TopicWriteSpec`**, one per typed topic per worker. Members by part:
    - **02:** `int32_t topicId; uint16_t keyFieldId = kNoKeyField /*0xFFFF*/; rune::codec::PrimType keyType; uint8_t keyWidth = 0;`, plus the per-version validators and materialized descriptors. Every version is materialized when it is installed, so the produce path never allocates. Lookup is `[[nodiscard]] const rune::codec::Validator* validatorFor(uint16_t version) const noexcept` and `const rune::codec::Descriptor* descriptorFor(uint16_t version) const noexcept`, with nullptr meaning an unknown version, which is rejected.
    - **05:** `bool keyIndex = false;`.
    - **06:** `bool autoIncrement = false; uint64_t autoIncrementMax; uint32_t autoIncrementReserve; int32_t partitionCount; rune::protocol::Partitioner partitioner;`.
  - **`struct PartitionWriteState`**, one per owned partition of a typed topic. Members by part:
    - **02:** `const TopicWriteSpec* spec;`.
    - **04:** `RejectedOffsetIndex rejected; ConstraintSetTimeline constraintSets; VerdictSource* verdicts = nullptr; VerdictSourceStats stats;`.
    - **06:** `AutoIncState autoInc; AssignedBaseWindow assignedBases; AutoIncStats autoIncStats;`.
    - **11:** `TxnJudgeState txn;`.
- **Ownership.**
  - The worker owns the specs: `std::unordered_map<int32_t, std::unique_ptr<TopicWriteSpec>> writeSpecs_` (stable addresses).
  - `RcSlot` (`Worker.h:6206`) appends `std::unique_ptr<PartitionWriteState> write;`. `rebuildRcSlots_` (`Worker.h:6364`) carries it like `rc`, creates it for a newly owned partition whose topic has a spec, and keeps it across role changes (followers need `rejected`).
  - A dense `std::vector<PartitionWriteState*> writeById_` sits beside `rcById_` (`Worker.h:6328`).
  - `[[nodiscard]] PartitionWriteState* writeOf_(const rune::replication::PartitionReplica*) const noexcept` mirrors `rcOf_` (`Worker.h:6350`).
  - nullptr means an untyped topic. `produceCore_` (`Worker.h:8136`) pays one `[[unlikely]]` branch.
- **Install op.** `WorkerOp::Type::WriteSpecUpdate = 27` (SBE `OpType` `WRITE_SPEC_UPDATE = 27`, tail `TailKind` `WRITE_SPEC = 12`) goes control → every worker on the op ring. It is cold and never ownership-gated.
  - Its tail is `encodeTopicWriteSpec()` / `decodeTopicWriteSpec()` in `WriteState.h`. That format is in-process: each part appends fields, and no compatibility is needed.
  - `SchemaUpdate` (`WorkerOp.h:99`, `executeSchemaUpdateWire_` `Worker.h:3128`) keeps carrying per-version descriptors. From Part 02 on, `BrokerNode::postCatalogValidator_` (`BrokerNode.h:1102`) posts them for **every** typed topic.
  - `validatedTopics_` and `validators_` (`Worker.h:5919-5926`) are removed by Part 02.
- **The `TopicWriteSpec` remove path.** Topic drop uses the existing `TopicDropped` op (20).

### C8. The write hook policy (Part 03)

- **`broker/libs/storage/include/rune/storage/WriteHookPolicy.h`** (namespace `rune::storage`):
  ```cpp
  struct HookStaged { rune::protocol::client::ErrorCode error{}; const uint8_t* batch = nullptr; std::size_t len = 0; };
  struct NoWriteHook { static constexpr bool kActive = false; };
  ```
  An active policy declares `static constexpr bool kActive = true` and these three members. Each is called only under `if constexpr (HookT::kActive)`:
  1. `rune::protocol::client::ErrorCode check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept`. It runs after the dedupe gate accepts (`ProducePath.cpp:205-255`) and before the min-ISR gate (`ProducePath.cpp:258`). It does validation, the wire-key check, the intent and requirement table, `kMaxJudgedBatchRecords` and id planning, with **no side effects**.
  2. `template <class LogT> HookStaged stage(LogT& log, const uint8_t* batch, std::size_t len) noexcept`. It runs after the min-ISR gate and before the append (`ProducePath.cpp:278`). It may append a reservation control record through `log`, copies into the scratch, patches ids and recomputes the CRC. The append then uses `staged.batch` / `staged.len`.
  3. `void onAppended(int64_t baseOffset, int64_t lastOffset) noexcept`. It runs after the PSM update (`ProducePath.cpp:311`) and fills the side windows and `ProduceOutcome::assignedBase`.
- **`handleProduceBatch`** becomes `template <class LogT, class HookT> ProduceOutcome handleProduceBatch(…existing params…, bool crcPreverified, HookT& hook) noexcept`.
  - The existing signature stays as a forwarder with a local `NoWriteHook`.
  - The body moves to a new `broker/libs/storage/include/rune/storage/ProducePathImpl.h`.
  - `ProducePath.cpp` explicitly instantiates `<Log, NoWriteHook>` and `<InMemoryLog, NoWriteHook>`.
  - A duplicate never calls the hook. Its `assignedBase` and verdicts are resolved by the worker (06, 04).
- **Node policy.** `rune::node::CheckedWriteHook` lives in `broker/libs/node/include/rune/node/CheckedWriteHook.h`. It is a per-call object built in `produceCore_` from `PartitionWriteState&`, `const WriteFacts&` and the worker's scratch.
  - It is instantiated explicitly in `broker/libs/node/src/CheckedWriteHook.cpp`, for `Log` and `InMemoryLog`, with `extern template` in the header.
  - `struct WriteFacts { uint8_t acks; bool idempotent; bool transactional; bool atomicBatch; bool importGranted; bool legacyMode; };`. `transactional` comes from the batch header attrs, `atomicBatch` from the header (05/11 define the bit use), and `importGranted` from C17.
- **Requirements (§11.2) are checked in `check()`, in this order.** Each failure is `InvalidRequest` unless noted:
  1. reserved attribute bits or intent 3;
  2. legacy mode (SD10);
  3. level < 4 for any non-zero intent or `View`;
  4. a non-zero intent on a topic without a key index;
  5. an explicit-key intent (`insert` with a non-zero key, `update`, `removeExisting`) without idempotence, or without the `View` bit;
  6. the same inside a transaction or atomic batch, until 11;
  7. a transactional or atomic batch on a constrained source, until 11;
  8. for key-0 creates, the C-rules (06);
  9. `View` and > 4096 records on a verdict source (SD4).
  Plain upserts and deletes pass with no requirement.

### C9. Verdict-view interface inside the view engine (Part 04 unless stated)

1. **`broker/libs/view/include/rune/view/ViewKey.h`:** `inline constexpr std::size_t kMaxViewKeyBytes = 128;` and `struct ViewKey { std::array<uint8_t, kMaxViewKeyBytes> bytes{}; uint8_t len = 0; static ViewKey fromU64(uint64_t) noexcept; std::span<const uint8_t> span() const noexcept; };`. Part 07 adds `ViewTable` overloads `put/get/erase(const ViewKey&)` and the `> 8` byte storage. Until then a `ViewKey` with `len ≤ 8` maps to today's `uint64_t` API.
2. **`broker/libs/view/include/rune/view/FoldSkip.h`** (01 creates it; 04 appends `rejected`):
   ```cpp
   struct FoldSkip {
       const rune::transaction::AbortedTxnIndex* aborted  = nullptr;   // 01: skip transactional batches isAborted(pid, base)
       const rune::storage::RejectedOffsetIndex* rejected = nullptr;   // 04: skip rejected offsets
   };
   inline constexpr FoldSkip kNoSkip{};
   ```
   `rune_view` gets `target_link_libraries(rune_view PUBLIC rune_transaction)` (01). There is no cycle: `rune_transaction` links storage and coordination (`broker/libs/transaction/CMakeLists.txt:16`). `AbortedTxnIndex` is `ReadCommittedView.h:117`, reached through `RcState::aborted()` (`RcState.h:412`).
3. **`broker/libs/view/include/rune/view/RecordFold.h`:**
   ```cpp
   struct SourceRecord {
       std::string_view key;                       // wire key
       std::string_view value;                     // empty = tombstone
       int64_t          offset = 0;
       uint8_t          intent = 0;                // rune::protocol::record::intentOf(attributes)
       const rune::codec::Descriptor* writer = nullptr;   // materialized writer version; nullptr for a tombstone
   };
   enum class PrepareStatus : uint8_t { Ok, Malformed, CapacityExhausted };
   struct Prospect;   // fixed-capacity, per-instance scratch reserved at install: the op (None/Upsert/Erase/Move),
                      // up to 2 prospective rows (new; old group for a Move) as row images, and the view keys touched.
   ```
   - In `ViewFold.h`: `PrepareStatus prepareProjection(ProjectionFold&, const SourceRecord&, Prospect&) noexcept;` and `void applyProjection(ProjectionFold&, const SourceRecord&, const Prospect&) noexcept;`.
   - In `AggFold.h`: `prepareAggregate(AggregateFold&, …)` and `applyAggregate(AggregateFold&, …)`.
   - `ProjectionFold` and `AggregateFold` are reference bundles that `ViewInstance` (`WorkerViewSet.h:191`) builds once.
   - `foldBatch` and `foldAggregateBatch` keep their signatures plus `const FoldSkip&`. They are re-implemented as the prepare-then-apply loop, so "the same code" computes and applies.
   - Invariant, tested by 04: on random logs, the loop produces state byte-identical to the pre-04 fold.
4. **`broker/libs/view/include/rune/view/Verdict.h`:**
   ```cpp
   struct Verdict { uint16_t code = 0; uint32_t constraintId = 0; [[nodiscard]] bool accepted() const noexcept { return code == 0; } };
   [[nodiscard]] constexpr uint32_t constraintIdOf(uint64_t viewObjectId, uint16_t ordinal) noexcept;   // (uint32(objectId) << 16) | ordinal
   enum class VerdictViewKind : uint8_t { KeyIndex = 1 /*05*/, Unique = 2 /*09*/, Check = 3 /*09*/, TestReject = 200 /*04, test seam only*/ };
   ```
5. **`broker/libs/node/include/rune/node/VerdictSource.h`.** `class VerdictSource` exists once per (source topic, partition). It is owned by `WorkerViewSet` in `std::unordered_map<uint64_t, VerdictSource> verdictSources_`, keyed by `instKey(srcTopicId, partition)` (`WorkerViewSet.h:554`).
   - **Public API:**
     - `[[nodiscard]] Verdict judge(const SourceRecord&) noexcept;`. It prepares every verdict view, evaluates the intents (05) and rules (09) on the prospects, applies every verdict view on accept or none on reject, and appends the rejection to the pending marker list and to `PartitionWriteState::rejected`.
     - `template <class ReadRangeFn> int driveJudge(ReadRangeFn&&, int64_t bound, int64_t nowNs, PartitionWriteState&)`.
     - `[[nodiscard]] int64_t judgedThrough() const noexcept;` and `[[nodiscard]] bool stalled() const noexcept;` (capacity: the fold stops with an alert, never a verdict).
     - `void addVerdictView(ViewInstance*, VerdictViewKind, uint64_t viewObjectId)`, `void removeVerdictView(...)` (cold).
     - Test seam `installTestVerdictViewForTest(…)`, which builds a `TestReject` view rejecting offsets chosen by a predicate.
   - **Order within one duty pass for a verdict source:** `driveJudge` first; then the other views of the partition with `FoldSkip{.rejected = &state.rejected}`, bounded by `judgedThrough()`; then marker emission (C10). Part 04 wires this into `WorkerViewSet::driveFolds` (`WorkerViewSet.h:417`).
   - `WorkerViewSet` gets `[[nodiscard]] VerdictSource* verdictSource(int32_t srcTopicId, int32_t partition) noexcept`. After every install, uninstall and ownership change, the worker re-resolves `PartitionWriteState::verdicts` (`Worker::rebindVerdictSources_()`). Hot paths only read `state->verdicts`.
6. **The consumer bound.** `[[nodiscard]] int64_t Worker::verdictBound_(const PartitionWriteState*, int64_t bound) const noexcept` returns `min(bound, verdicts->judgedThrough())`, or `bound` for a non-source. It is applied at `pushOwnedSubscriptions_` (`Worker.h:3736-3744`), `servePushRead_` (`Worker.h:4312`) and the filtered push. Rejected records are cut with `rune::protocol::rebuildBatch` (`BatchRebuild.h:82`), which keeps their offsets.

### C10. Control records (Part 04 creates the header and all type ids; 06 and 11 add builders and parsers)

- **New header** `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (namespace `rune::storage::control_record`, header-only, builders write into a caller buffer and never allocate).
- **Batch form.** A control batch with `attributes = kAttrControl` (0x0020) only, **`kAttrTransactional` clear**, `producerId = -1`, `producerEpoch = -1`, `baseSequence = -1`, and one record. The record key is an int16 LE **type**. The value is `[version u8 = 1][payload]`. `producerId -1` keeps it out of `TxnCleanState` (`LogCleaner.cpp:229`) and the PSM feed (`RcState.h:660`).
- **Types.** `inline constexpr int16_t kReservation = 16; kVerdictMarker = 17; kConstraintSetChange = 18; kTxnJudge = 19;`. Transaction markers use 0 and 1 (`ControlMarkerLayout.h:30-31`).
- **Parser.** `[[nodiscard]] int recordType(const uint8_t* batch, std::size_t len) noexcept` returns the type or `kUnparseable` (−1).
- **Hardening (04).** `ControlMarker::extract` (`broker/libs/transaction/src/ControlMarker.cpp:289-296`) and `RcBuilder::step` (`ReadCommittedView.cpp:315`) accept only types 0 and 1. A regression test feeds a batch of each new type to every reader listed in Review Focus 2.
- **Payloads:**
  - `kVerdictMarker` (04): `[fromOffset i64][finalThrough i64][constraintSetId u32][count u32] count × [offsetDelta u32 (offset − fromOffset)][code u16][constraintId u32]`. It covers the judged range `[fromOffset, finalThrough)`. `kMaxMarkerEntries = 4096`; more rejections in one step means several markers.
  - `kConstraintSetChange` (04): `[constraintSetId u32][count u16] count × [viewObjectId u64][kind u8 (VerdictViewKind)][planFingerprint u64]`. The key index appears with `viewObjectId = 0`.
  - `kReservation` (06): `[reservedThroughN u64]` (the last `n` reserved; ids are `n·P + p`).
  - `kTxnJudge` (11): `[producerId i64][producerEpoch i16]`.
- **Who appends.** The partition leader's worker appends these records through the replica's control append, the path `AppendControlRecord` already uses (`WorkerOp.h:86`). Reservations are the exception: they are appended inside `CheckedWriteHook::stage` through `log`.

### C11. Rejected-offset index, cleaner state and the cleaner bound (Part 04)

- **`broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h`** has one instance per partition replica, in `PartitionWriteState::rejected`. It is worker-owned. It is filled by the leader's judge, by follower apply of replicated markers, and by recovery scans of markers. It is reserved at creation and never allocates on the hot path.
  ```cpp
  class RejectedOffsetIndex {
  public:
      struct Entry { int64_t offset; uint16_t code; uint32_t constraintId; };
      void reserve(std::size_t entries);                                              // cold
      [[nodiscard]] bool add(int64_t offset, uint16_t code, uint32_t constraintId) noexcept;   // ascending; false = full → fold stall + alert
      void onMarker(int64_t fromOffset, int64_t finalThrough, std::span<const Entry> entries); // replica apply / recovery
      [[nodiscard]] bool isRejected(int64_t offset) const noexcept;
      [[nodiscard]] const Entry* find(int64_t offset) const noexcept;
      template <class Fn> void forEachIn(int64_t from, int64_t toExclusive, Fn&& fn) const noexcept;
      [[nodiscard]] int64_t nextRejectedAtOrAfter(int64_t offset) const noexcept;   // INT64_MAX = none
      void truncateFrom(int64_t offset) noexcept;
      void dropBelow(int64_t offset) noexcept;
      [[nodiscard]] int64_t watermark() const noexcept;   // W
      void setWatermark(int64_t w) noexcept;
      [[nodiscard]] std::size_t size() const noexcept;
  };
  ```
- **`broker/libs/storage/include/rune/storage/VerdictCleanState.h`** is cleaner-thread only, cold, and modeled on `TxnCleanState` (`TxnCleanState.h:40`).
  - API: `clear()`, `onVerdictMarker(int64_t markerOffset, int64_t fromOffset, int64_t finalThrough, std::span<const RejectedOffsetIndex::Entry>)`, `onConstraintSetChange(int64_t offset, bool nonEmpty)`, `onReservation(int64_t offset)` (06), `finish(int64_t toOffset)`, `isRejected(int64_t)`, `cleanLimit()`, `markerRemovable(int64_t)` and `keepsReservation(int64_t)` (06).
  - It is fed by the `scanTxnState` pre-pass (`LogCleaner.cpp:212`) for control batches with `pid == -1`.
- **Cleaner bound.**
  - `PartitionReplica::publishCleanerVerdictBound(Offset)` and `[[nodiscard]] Offset cleanerVerdictBound() const noexcept` are an atomic word beside `publishCleanerLso` (`PartitionReplica.h:866`). The default is `kNoCleanerLso`, meaning not a verdict source.
  - The worker publishes `W` from `PartitionWriteState::rejected.watermark()`.
  - `LogCleanerAgent.h:455` and `PartitionReplica::cleanerSwapBound` (`PartitionReplica.h:893`) take the min with it.

### C12. Constraint ids and constraint sets (Part 04 mechanism; 05 and 09 use it)

- **Constraint ids.** `constraintId = constraintIdOf(viewObjectId, ordinal)`. Ordinal 0 is `unique`. `check` constraints take 1..n in declaration order. Key-index codes 52 and 53 carry `constraintId = 0`. SD5 limits apply.
- **Constraint set ids.** `constraintSetId` is u32, starts at 1 per verdict source, and increments with each change.
- **`ConstraintSetTimeline`** (in `VerdictSource.h`) is per partition, inside `PartitionWriteState`. It maps change-record offsets to sets and is rebuilt from the log on recovery and failover.
  - `const ConstraintSet* setAt(int64_t offset) const noexcept`.
  - A missing view install makes the judge wait (§8.7).
- **Leader API.**
  - `int VerdictSource::requestChange(const ConstraintSet& next)` appends the `kConstraintSetChange` and returns the activation path.
  - `create`, `create or replace` (swap at `S` in one worker step) and `drop` all go through it.
  - Part 05 registers the key index as the first member. Part 09 adds `unique`/`check` views.
- **`CompiledViewPlan` v2 carries `uint64_t viewObjectId`** (07 format, set by AdminWorker from the catalog object).

### C13. Catalog `formatVer` 2 and topic key features (Part 05; 06 gives the autoincrement fields meaning)

- **`protocol/include/rune/protocol/CatalogTypes.h`:**
  ```cpp
  enum class Partitioner : uint8_t { Fnv1a = 0, Modulo = 1 };
  inline constexpr uint64_t kAutoIncrementMaxDefault     = (uint64_t{1} << 53) - 1;
  inline constexpr uint32_t kAutoIncrementReserveDefault = 4096;
  struct TopicKeyFeatures {
      bool        keyIndex             = false;
      bool        autoIncrement        = false;
      Partitioner partitioner          = Partitioner::Fnv1a;
      uint64_t    autoIncrementMax     = kAutoIncrementMaxDefault;
      uint32_t    autoIncrementReserve = kAutoIncrementReserveDefault;
      bool operator==(const TopicKeyFeatures&) const = default;
  };
  ```
- **`codec/include/rune/catalog/CatalogEvent.h`:** `kFormatV2 = 2` and `kCurrentFormatVer = kFormatV2`.
  - `SchemaDef` (`CatalogEvent.h:60`) appends `rune::protocol::TopicKeyFeatures keyFeatures{};`. It is encoded only in `formatVer ≥ 2` events and fingerprinted in v2.
  - New apply rules live in `codec/src/catalog/ApplyV2.cpp`, which carries the immutability rules of §6.2. `SchemaDef::validate` is ignored (02).
  - A v2 golden log is added (`codec/tests/catalog/golden`, `codec/tools/make_catalog_golden.cpp`). The v1 goldens stay unchanged.
- **Wire.** `DefineTopic` (`protocol/include/rune/protocol/client/SchemaMessages.h:181`) and `TopicSchemaResponse` take a v12 trailing block after their existing tail: `[flags u8: bit0 keyIndex, bit1 autoIncrement][partitioner u8][autoIncrementMax u64][autoIncrementReserve u32]`. The client exposes it on its `createTopic` options. `TopicHandle` (`client-cpp/include/rune/client/TopicHandle.h:34`) appends `rune::protocol::Partitioner partitioner = Partitioner::Fnv1a;` (06).
- **Routing.** `rune::protocol::partitionFor(std::span<const uint8_t> key, int32_t partitions, Partitioner)` is added to `protocol/include/rune/protocol/PartitionHash.h` (06). The client, `TopicRegistry::partitionForKey` and the view-read path call it.

### C14. Plan v2 (Part 07 owns the format of every field; 08 authors; 09 evaluates)

- **Authored plan.** The authored plan moves to `codec/include/rune/codec/ViewPlanAuthored.h` and `codec/src/ViewPlanAuthored.cpp` (listed three times), keeping namespace `rune::view`.
  - It holds `ViewPlan`, `AuthoredField`, `SourceSpec`, `ViewSizing`, `MergeRule`, `serializeAuthored` and `deserializeAuthored`.
  - `broker/libs/view/include/rune/view/ViewPlan.h` and `ViewTypes.h` include it.
  - `client-cpp/include/rune/client/view/ViewPlanAuthor.h` becomes aliases onto it.
- **New authored fields:**
  - `std::vector<KeyComponent> keyComponents;` with `struct KeyComponent { std::string column; uint8_t keyFunction = 0; std::string outName; };` (0 none, 1 lower, 2 reserved).
  - `bool unique = false;`
  - `std::vector<AuthoredConstraint> constraints;` with `struct AuthoredConstraint { std::string name; std::vector<PredicateNode> predicate; };`.
  - `uint32_t unicodeVersion = 0;` packed as `major << 16 | minor << 8 | update`.
- **Predicate.** `struct PredicateNode { PredicateOp op; uint16_t a = 0, b = 0; std::string column; rune::codec::PrimType constType{}; std::array<uint8_t, 16> constBytes{}; };`. The tree is a flat vector, root last. Its operator enum is `enum class PredicateOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };`.
- **Compiled plan v2** (`ViewPlan.h`) adds resolved key components (`srcFieldId`, `keyFunction`, `width`, `outSlot`), `keyWidth`, `unique`, compiled constraints (`ordinal`, predicate over out-slots), `unicodeVersion` and `viewObjectId`.
- **Versions.** `kAuthoredVersion` and `kCompiledVersion` become 2 (`broker/libs/view/src/ViewPlan.cpp:14-15`). v1 blobs still decode.
- **Codec key encoding** (07, `codec/include/rune/codec/ViewKeyEncoding.h`): `encodeKeyComponent(...)`. Unsigned integers are big-endian. Signed integers are big-endian with the sign bit flipped. `bool` is one byte. `string(N)` is zero-padded. `lower` goes through `codec/include/rune/codec/UnicodeLower.h` (`kUnicodeVersion`, `lowerUtf8`, `maxLoweredWidth(N)`). The fold, the client key builder and QL literals all use it.

### C15. Client write and read API names (owners in brackets)

- **Raw producer:** `send(topic, key, value, WriteIntent intent = WriteIntent::Upsert)` and the `trySend` counterparts. `enum class WriteIntent : uint8_t { Upsert = 0, Insert = 1, Update = 2 };` [05]. An empty value with `Update` means delete-existing.
- **`TypedTopic<T>`:** `insert`, `update`, `upsert`, `remove(const KeyT&)` and `removeExisting(const KeyT&)`, each with an `…AndWait` form returning `WriteResult` [05]. `insertAndWait(rec).key()` [06].
- **Consumer.** `Record` (`client-cpp/include/rune/client/subscriber/Subscription.h:117`) appends `uint8_t attributes`, and `[[nodiscard]] RecordOp op() const noexcept` with `enum class RecordOp : uint8_t { Upsert, Insert, Update, Delete };` [05].
- **Reads [07].** `getViewRowAndWait(view, key)` (any width) and `scanViewAndWait(view, prefix, limit)`. `ViewKeyBuilder` comes from the view's output descriptor. New `OpKind` values are appended after `Seek` (`Completion.h:51`): `GetViewRowK`, `ScanViewRows`. Part 08 appends `GetStats`.
- **Constraints.** `Client::constraintName(uint32_t constraintId) -> std::optional<ConstraintName>` with `struct ConstraintName { std::string view; std::string constraint; };` [09].
- **Transactions.** `commitTransaction()` and `commitTransactionAndWait()` report `code` and `constraintId` on a no vote, using the same completion fields as C6 [11].

### C16. Stats (Part 08 wire; earlier parts keep the counters)

- **New file `protocol/include/rune/protocol/client/StatsMessages.h`.**
  - `GetStats` (84) has the layout `[scope u8: 1 = topic partition, 2 = view partition][objectId i32][partition i32]`.
  - `StatsResponse` (85) has the layout `[status u16][count u16] count × [statId u16][value i64]` followed by `[nConstraints u16] × [constraintId u32][rejections u64]`.
  - `GetStats` is served by the owning worker on the `GetViewRow` route.
- **`enum class StatId : uint16_t`** (append-only). Each stat names the part that keeps the counter:

  | Id | Stat | Kept by |
  |---|---|---|
  | 1 | `rejectionsKeyExists` | 05 |
  | 2 | `rejectionsKeyNotFound` | 05 |
  | 3 | `rejectionsConstraint` (total) | 04 |
  | 4 | `verdictLag` (LEO − `judgedThrough`) | 04 |
  | 5 | `verdictWatermark` | 04 |
  | 6 | `verdictMarkersWritten` | 04 |
  | 7 | `rejectedNotCompacted` | 04 |
  | 8 | `autoIncrementCounter` | 06 |
  | 9 | `autoIncrementReservedThrough` | 06 |
  | 10 | `autoIncrementIdsBurned` | 06 |
  | 11 | `txnVotesYes` | 11 |
  | 12 | `txnVotesNo` | 11 |
  | 13 | `txnVotesNoConstraintViolation` | 11 |
  | 14 | `txnVotesNoKeyExists` | 11 |
  | 15 | `txnVotesNoKeyNotFound` | 11 |
  | 16 | `txnVotesNoTxnTooLarge` | 11 |
  | 17 | `txnVoteLatencyNsMax` | 11 |
  | 18 | `txnVoteLatencyNsTotal` | 11 |
  | 19 | `foldPausedOnTxnLockNs` | 11 |
  | 20 | `txnMarkerWithoutVote` | 11 |
  | 32 | `uniqueConflicts` | 09 |
  | 33 | `scanRows` | 07 |
  | 34 | `scanPages` | 07 |
  | 35 | `transientRecordsEvicted` | exists |
  | 36 | `transientSourceResets` | exists, 10 |

  Ids 1–31 are topic-partition scope and 32+ are view-partition scope. Per-constraint rejections ride the trailing list (04 keeps them in `VerdictSourceStats`).
- **Where the counters live.** `VerdictSourceStats` is in `VerdictSource.h` (04), `AutoIncStats` is in `WriteState.h` (06), and `ViewFoldCounters` (`WorkerViewSet.h:130`) appends `uniqueConflicts`, `scanRows` and `scanPages`.
- **Test access before 08.** `Worker::verdictStatsForTest(int32_t topicId, int32_t partition)`.

### C17. The `IMPORT` grant (Part 06)

- `SecurityOperation::Import = 7` (`protocol/include/rune/protocol/SecurityTypes.h:10`). `validSecurityOperation` is extended accordingly.
- The `opsOn` bit 7 (`codec/include/rune/security/Permissions.h:18`).
- The QL grammar is `grant import on topic <pattern> to …`.
- The worker side is `WritePermTable::importBits` (`broker/libs/node/include/rune/node/WorkerWriteGrants.h:28`), shipped by `SESSION_PERMISSIONS` with a new flag `kPermFlagImport`. It is read into `WriteFacts::importGranted`.

### C18. The transaction vote (Part 11)

- **Cluster plane** (`ReplicationMessages.h`, schema 112): `TxnVoteMsg` with `TEMPLATE_ID = 13` and `TxnVoteResponseMsg` with `TEMPLATE_ID = 14`. Ids 1–4 and 7–12 are used today. Both are routed like `WriteTxnMarkerMsg` (`ReplicationMessages.h:837`), through the gateway's `TxnMarkerChannel` (`ClientGateway.h:957`).
- **Worker.** `WorkerOp::Type::TxnVote = 28` (SBE `TXN_VOTE = 28`) and `WorkerReturn::Type::TxnVoteResult = 8` (SBE `ReturnType` `TXN_VOTE_RESULT = 8`).
- **`EndTxnResponse`** (client template 54) takes a v12 trailing `[constraintId u32][failingOffset i64]`.
- **Before-images and row locks** are pre-allocated per worker at `NodeConfig::txnJudgedRecordsMax` (SD14).

---

## File map

C = create, M = modify. Line numbers are where the change attaches today.

| File | C/M | Owner(s) |
|---|---|---|
| `broker/libs/view/include/rune/view/FoldSkip.h` | C | 01 (+04) |
| `broker/libs/view/{include/rune/view/ViewFold.h,src/ViewFold.cpp}`, `AggFold.{h,cpp}` | M | 01, 04, 07, 09 |
| `broker/libs/view/CMakeLists.txt` (link `rune_transaction`; new tests) | M | 01, 04, 07 |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | 01, 03 (`minFoldCursor`), 04, 07, 09, 10, 11 |
| `broker/libs/node/include/rune/node/RcState.h` (aborted index access) | M | 01 |
| `broker/libs/node/include/rune/node/WriteState.h` | C | 02 (+04, 05, 06, 11) |
| `broker/libs/node/include/rune/node/Worker.h` | M | 02, 03, 04, 05, 06, 07, 08, 11 |
| `broker/libs/node/include/rune/node/WorkerOp.h`, `WorkerReturnCodec.h`, `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*` | M | 02 (26/27, `WRITE_SPEC`), 03, 11 (28, return 8) |
| `broker/libs/node/include/rune/node/AdminChannels.h`, `AdminWorker.h`, `src/AdminWorker.cpp` | M | 02, 05, 06, 07, 08, 09, 10 |
| `broker/libs/node/include/rune/node/BrokerNode.h`, `src/BrokerNode.cpp`, `NodeConfig.h`, `src/NodeConfig.cpp` | M | 02, 04 (`verdictIntervalMs`), 11 (`txnJudgedRecordsMax`) |
| `broker/libs/replication/include/rune/replication/ReplicationMessages.h` | M | 02 (level), 11 (13/14) |
| `codec/include/rune/codec/Utf8.h` (+`codec/src/Utf8.cpp` if not header-only) | C | 02 |
| `codec/include/rune/codec/Validator.h`, `codec/src/Validator.cpp` (UTF-8, key check helpers) | M | 02 |
| `client-cpp/include/rune/client/{Key.h,TypedTopic.h,Client.h}`, `client-cpp/src/cli/CatalogCommands.cpp:250`, `Console.cpp:138` | M | 02 (+05, 06, 07) |
| `ql/src/Parser.cpp`, `ql/include/rune/ql/Ast.h`, `ql/src/Backends.cpp` | M | 02 (`partitions` optional, untyped marker), 06 (`grant import`), 08 (everything else) |
| `broker/libs/storage/include/rune/storage/WriteHookPolicy.h`, `ProducePathImpl.h` | C | 03 |
| `broker/libs/storage/{include/rune/storage/ProducePath.h,src/ProducePath.cpp}` | M | 03, 06 |
| `broker/libs/node/{include/rune/node/CheckedWriteHook.h,src/CheckedWriteHook.cpp}` | C | 03 (+05, 06, 11) |
| `protocol/include/rune/protocol/client/{Protocol.h,DataMessages.h,Dispatch.h}`, `protocol/conformance/*` | M | 03 (+07, 08, 11) |
| `protocol/include/rune/protocol/{RecordBatch.h,BatchRebuild.h}` | M | 03 |
| `broker/libs/node/include/rune/node/SessionAckSink.h` | M | 03, 04 (arena), 06 |
| `broker/libs/gateway/src/ClientGateway.cpp:3872` (acks decode, SD10 refusals) | M | 03 |
| `client-cpp/include/rune/client/producer/{Producer.h,ProducerImpl.h,BatchBuilder.h}`, `src/producer/*`, `client-cpp/include/rune/client/Completion.h` | M | 03, 04, 05, 06, 11 |
| `client-cpp/src/cli/QlEngine.cpp:133` (`errorCodeName`) | M | 03 |
| `broker/libs/view/include/rune/view/{ViewKey.h,RecordFold.h,Verdict.h}` | C | 04 |
| `broker/libs/node/include/rune/node/VerdictSource.h` (+`src/VerdictSource.cpp`) | C | 04 (+05, 09, 11) |
| `broker/libs/storage/include/rune/storage/{ControlRecordLayout.h,RejectedOffsetIndex.h,VerdictCleanState.h}` | C | 04 (+06, 11) |
| `broker/libs/storage/{include/rune/storage/LogCleaner.h,src/LogCleaner.cpp}`, `broker/libs/node/include/rune/node/LogCleanerAgent.h:455` | M | 04, 06 |
| `broker/libs/replication/include/rune/replication/PartitionReplica.h:866,893` | M | 04 |
| `broker/libs/transaction/src/{ControlMarker.cpp,ReadCommittedView.cpp}` (type hardening) | M | 04 |
| `protocol/include/rune/protocol/CatalogTypes.h` (`TopicKeyFeatures`, `Partitioner`) | M | 05 |
| `codec/include/rune/catalog/{CatalogEvent.h,CatalogState.h}`, `codec/src/catalog/{CatalogEvent.cpp,CatalogFingerprint.cpp,ApplyV2.cpp(C)}`, `codec/tests/catalog/golden/*`, `codec/tools/make_catalog_golden.cpp` | M/C | 05 |
| `protocol/include/rune/protocol/client/SchemaMessages.h` (DefineTopic / TopicSchemaResponse v12 trailer) | M | 05 |
| `client-cpp/include/rune/client/WriteResult.h` | C | 05 (+06, 09) |
| `client-cpp/include/rune/client/subscriber/Subscription.h:117`, `client-cpp/include/rune/client/codec/DataCodec.h`, `src/subscriber/SubscriberHub.cpp` | M | 05 |
| `protocol/include/rune/protocol/PartitionHash.h`, `client-cpp/include/rune/client/TopicHandle.h`, `client-cpp/include/rune/client/metadata/PartitionRouter.h`, `broker/libs/gateway/include/rune/gateway/TopicRegistry.h` | M | 06 |
| `protocol/include/rune/protocol/SecurityTypes.h`, `codec/src/security/*`, `broker/libs/node/include/rune/node/WorkerWriteGrants.h`, `PermissionShipper.h` | M | 06 |
| `broker/libs/view/{include/rune/view/ViewTable.h,src/ViewTable.cpp,include/rune/view/ViewCheckpoint.h,src/ViewCheckpoint.cpp,src/PlanCompiler.cpp,src/ViewPlan.cpp}` | M | 07 (+09) |
| `codec/include/rune/codec/{ViewPlanAuthored.h,ViewKeyEncoding.h,UnicodeLower.h}`, `codec/src/{ViewPlanAuthored.cpp,UnicodeLower.cpp}`, `codec/tools/gen_unicode_lower.cpp`, `codec/data/UnicodeData-<ver>.txt` | C | 07 |
| `client-cpp/include/rune/client/view/{ViewPlanAuthor.h,ViewKeyBuilder.h(C)}`, client read ops | M/C | 07 |
| `protocol/include/rune/protocol/client/ViewMessages.h` (or the file holding `GetViewRow` 72; 81–83 added there) | M | 07 |
| `protocol/include/rune/protocol/client/StatsMessages.h` | C | 08 |
| `client-cpp/src/cli/{QlEngine.cpp,CatalogCommands.cpp}` (`select`, `show`, `stats --json`) | M | 08 |
| `broker/libs/transaction/*`, `broker/libs/node/include/rune/node/TransactionCoordinationService.h`, `src/TransactionCoordinationService.cpp`, `broker/libs/gateway/include/rune/gateway/ClientGateway.h:957` | M | 11 |
| `docs/api/ql.md`, `docs/wire-protocol.md`, `docs/client-cpp.md`, `docs/admin-cli.md`, `CATALOG.md`, `SECURITY.md`, `SELF-HOSTING.md`, root and client `CLAUDE.md` | M | each part for what it ships; 11 cross-checks |
| Tests and support files | C/M | see below |

---

## Shared test infrastructure

**Binaries** (each part extends only the ones it names):

| Binary (target) | Location | Created by | Extended by |
|---|---|---|---|
| `rune_view_tests` | `broker/libs/view/tests/view/` | exists | 01, 04, 07, 09 |
| `rune_storage_tests` | `broker/libs/storage/tests/` | exists | 04 (index, layout, cleaner), 06 |
| `rune_protocol_tests` | `protocol/tests/` | exists | 03, 05, 07, 08, 11 |
| `rune_codec_tests`, `rune_catalog_tests` (`build/sub-codec`) | `codec/tests/` | exist | 02 (Utf8), 05 (catalog v2, golden), 06 (security op), 07 (lower, key encoding, plan v2) |
| `rune_produce_validation_tests` | `broker/libs/node/tests/integration/ProduceValidationTest.cpp` | exists | 02, 03 |
| `rune_checked_writes_tests` | `broker/libs/node/tests/node/CheckedWrites*Test.cpp` | 02 | 03–11: worker-level unit and integration |
| `rune_checked_writes_matrix_tests` | `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp` | 04 | 05, 06, 07, 09, 11 |
| `rune_checked_writes_stress_tests` | `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp` | 04 | 05, 06, 09, 11 |
| `rune_checked_writes_alloc_regression_tests` | `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` + `tests/support/AllocWindow.cpp` | 03 | 04, 05, 06, 07, 09, 11 |
| `rune_client_broker_tests` | add `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` to `broker/libs/node/CMakeLists.txt:1731` | 02 | all parts: real `OneBroker` (`tests/client/OneBroker.h`) / `ThreeNodes` (`tests/support/CatalogBrokerCluster.h:160`) |
| `rune_compaction_correctness_tests` | exists | — | 04, 06 |
| `rune_create_view_tests`, `rune_get_view_row_tests` | exist | — | 07, 09, 10 |
| `rune_transaction_tests`, `rune_txn_coordination_service_tests` | exist | — | 11 |
| client suites (`build/sub-client-cpp`) | `client-cpp/tests/` | exist | 02, 03, 05, 06, 07, 09, 11 |
| ql suites (`build/sub-ql`) | `ql/tests/` | exist | 02, 06, 08 |

**Support headers** (`broker/libs/node/tests/support/`), following the Phase 2c `FilterMatrixRig.h`, `FilterMatrixExpectations.h` and `FilterModel.h` pattern:
- **`CheckedWritesRig.h` (04).** One real `Worker`, a disk `Log` or an `InMemoryLog` behind a `PartitionReplica`, an injected clock, and helpers. Helpers built by 04:
  - `appendBatch(records, acks, intent…)`, which returns the ack;
  - `driveUntilJudged(offset)`, with a hard pass cap;
  - `subscribeAndCollect()`;
  - `failover()`, `restart()`, `truncateTo(offset)`, `compact()`;
  - `installTestRejectView(pred)`.
  Parts add `createKeyIndex()` (05), `createAutoIncTopic(P)` (06), `createView(planV2)` (07) and `createConstrainedView(…)` (09).
- **`CheckedWritesMatrixExpectations.h` (04).**
  - `enum class Op : uint16_t` with ranges reserved per part: 04 0–19, 05 20–39, 06 40–59, 07 60–79, 09 80–99, 11 100–119.
  - `enum class Entity : uint16_t` with the same ranges.
  - `enum class Timing : uint16_t`, fixed here as §15's timing list: `Single, BackToBack, SelfCollidingBatch, Racing, RetryBeforeJudge, RetryAfterJudge, FailoverAppendHw, FailoverHwFold, FailoverFoldMarker, TruncateTail, RestartDuringRebuild, CreateOnNonEmpty, ReplaceDuringWrites, DropDuringWrites`.
  - `struct Cell { Op op; Entity entity; Timing timing; };` and `Expected expect(const Cell&)` chain the per-part `expect04` … `expect11`, each with a top-down not-applicable table.
- **Cell counts** are pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned`. Each part updates its own row of the pin.
- **`CheckedWritesModel.h` (04).** The reference model: per-partition records with their verdicts, views as a fresh refold of the accepted records, and the consumer-visible set. It has extension points for key index (05), ids (06), unique and check (09) and transactions (11).
- **`CheckedWritesStressDriver.h` (04).** Seeded racing writers with injected failovers, restarts, truncation and compaction on `ThreeNodes`. It records the seed on failure. The invariants are named checks, one owner each:
  - 04: `AckedVisibleUnlessRejected`, `NoRejectedDeliveredOrFolded`, `SameVerdictEveryLeaderAndRefold`, `ViewEqualsFreshRefold`, `CompactionKeepsAcceptedValue`;
  - 06: `NoIdTwiceNoReuse`;
  - 09: `AtMostTwelveLiveCharacters`, `LowerNameUnique`;
  - 11: `TxnAllOrNothingAndAtomicPerPartition`.
- **Docker e2e.** `broker/docker/checked-writes-e2e.sh` and `docker-compose.checked-writes.yml` (VM only) are created by 05 and extended by 06, 09 (the RegressorsPath auth-server and character-server flows from spec §1) and 11.
- **Benches** (`build/rel`). ABAB n=3 against the part's base commit:
  - `ProduceValidateBench.cpp`: `BM_CheckedWriteHook/*` (02, 03, 06).
  - `ProducePathBenchmark.cpp` and `WorkerProduceLatencyBench.cpp`: non-regression (02, 03), plus a consumer attached to a verdict source (04).
  - `ViewFoldBench.cpp`: `BM_ViewFoldVerdict/*` (04, 09) and derived keys plus the owner map (07).
  - `ViewTableBench.cpp`: `BM_ViewTableWideKey/*` and `BM_ViewTablePrefixScan` (07).
- **Names.** Every new test suite name starts with `CheckedWrites`, so `ctest -R CheckedWrites` selects the program. Every fix test carries `Regression` in its suite or name.

### Interaction matrix: §15 cells by owner

- **02:** the whole "Validation (§4)" block except transient views and `lower()`; "Reads": `partitions` omitted, binding both ways, key fields wider than 16 refused and `Key` truncation.
- **03:**
  - "Clients": intents without `View` or from a non-idempotent producer refused; autoincrement creates without `All` refused (the rule only; the ids are 06);
  - every `View`-flag bullet (`Leader|View`, `All|View`, behind an open transaction, no views, `NotLeader`/`Internal`, `View`/`None|View` refused, unknown bits, `BrokerTooOld`, old values 0/1 unchanged);
  - before Part G, a transactional and an atomic batch on a constrained source are refused while unconstrained ones work. Constrained sources are exercised by 04 with `TestReject` and by 09 for real.
- **04 (with `TestReject`):**
  - every "Verdicts, markers and readers" bullet that is not Part-G-specific and not constraint-specific;
  - verdict lag during a build;
  - failover right after a change record;
  - a leader without the install waits;
  - plain writes from a non-idempotent, `acks=leader`, `acks=none` and v11 client;
  - a coalesced batch with one bad record.
- **05:** the Operations × Entities × Timing rows for intents (`insert`, `update`, `remove`, `removeExisting`, duplicate retry, plain upsert) over every key width; "typed `remove` routes as `send(rec)`"; "intents sent to an older cluster".
- **06:** the whole "Autoincrement" block; "a batch mixing key-0 creates with other records is refused"; "a retry after failover and compaction still returns its ids".
- **07:** "Reads" (point and prefix after an acked write, scan across pages with an erase, empty prefix, forced hash collision); every `lower()` bullet (SD7); rename with an owner-map move.
- **09:** create, rename, move, delete, delete-then-reuse across user × group (11 / 12 / over) × name; "a predicate failing on a row already over its limit"; "a delete always passes (`balance >= 0`)"; pre-existing duplicate names and promotion; several verdict views where one rejects; the login race.
- **10:** "persisted views over transient sources" (all six bullets).
- **11:** every Part G bullet.

---

## Sequencing and merge rules

- **This plan set merges first.** The spec and the eleven plans live on `feature/view-keys-and-constraints-spec`. They go to `main` through a PR before any implementation starts, after the owner reviews them.
- **One branch per part.** The branch is `feature/checked-writes-NN-<slug>` (slug = the plan file's slug). It is cut from the latest `main` (`git fetch` first) only after every part in its "Depends on" column has merged. `main` is merged in again before review (root `CLAUDE.md` §6).
- **Parallel lanes.** At most 3 agents at once. Lane 1: 01 → 04 → 05 → 06 → 07 → 08 → 09 → 11. Lane 2: 02 → 03, which joins lane 1 before 04. Lane 3: 10 after 01. 01, 02 and 10 may run concurrently.
- **Review and merge.** The PR of each part is reviewed by the most capable model, the implementer resolves each comment, and the reviewer approves, merges and closes linked issues. The adversarial pass applies only to issue-fix PRs.
- **Pushing.** Pushing needs the owner's explicit consent for that push, unless the owner has approved this program's merge-and-push flow up front. Nothing in a plan pushes on its own.
- **Checkpoints.**
  - Inside a part: at the end of each phase.
  - Before each PR: the full `*Regression*` suite (`-LE latency`), ASan+UBSan and TSan for the broker and the client, CatalogGolden (`build/sub-codec`), the part's benches (ABAB n=3), the part's VM e2e when it has one, and the latency gates on an idle host.
  - After merging, the next part's branch starts by re-running the checkpoint on its fresh base.
- **Releases.** No release tag is cut between parts. `kClientVersion = 12`, `kBrokerVersion = 7` and `kCheckedWrites = 4` are shared by every part, and a mid-program release would announce capabilities that are not there yet. Releasing is the owner's call after Part 11 (GitHub Actions run on release tags only).
- **Follow-ups and the docs pass.** Deferred findings go to GitHub issues, labelled, at each part's close. Part 11's finish runs the §16.12 docs cross-check.

## Self-review (done while writing)

- **Spec coverage:**
  - §1 → 09 e2e; §2 G1 → 02; G2 → 05; G3 → 06; G4/G5 → 07; G6 → 04/09; G7 → 04; G7a → 11; G7b → 10; G8 → the stress invariants; G9 → 08; G10 → 02/03 benches.
  - §3 → 02/03/04; §4 → 02; §5 → 05; §6 → 06; §7 and §9 → 07; §8.1 → 07 (SD7); §8.2, §8.3 and §8.7 wiring → 09; §8.4–§8.8 → 04; §8.9 → 03.
  - §9a → 10; §9b → 11; §10 → 08; §11.1 → 03; §11.2 → 03 (06 and 11 for their rows); §11.3 → 04; §12 → 02/03; §13 → C16; §14 → benches; §15 → the owner list above; §16 → the index; §18 → left to the part plans within C9–C11.
- **Type consistency:** each type is defined once, here, with its file. Part plans reference names (`PartitionWriteState`, `VerdictSource`, `RejectedOffsetIndex`, `FoldSkip`, `SourceRecord`, `Prospect`, `Verdict`, `ViewKey`, `TopicKeyFeatures`, `WriteResult`) and never redefine them.
- **Number collisions checked against the code:**
  - `ErrorCode` stops at 50 (`Protocol.h:209`);
  - `TemplateId` 81–85 are free (`Protocol.h:90-102`);
  - `WorkerOp::Type` stops at 25, plus 200 (`WorkerOp.h:151-158`);
  - `TailKind` stops at 11 (`rune-internal-ring.xml`);
  - `ReturnType` stops at 7;
  - `controlToAdminRing` 26 is unused (`AdminChannels.h:48-82`);
  - cluster-plane template ids 13 and 14 are free;
  - `SecurityOperation` stops at 6 (`SecurityTypes.h:10`);
  - `SendResult` stops at 11 and `OpStatus` at 11;
  - control record types 0 and 1 are used (`ControlMarkerLayout.h:30-31`).
