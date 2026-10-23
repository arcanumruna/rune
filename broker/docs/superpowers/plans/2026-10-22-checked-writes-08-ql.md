# Checked Writes Part 08 — Rune QL and the CLI (§10, the §13 stats surface) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every QL statement of the spec's target block (§1) parses, prepares into an authored plan v2 and round-trips: keyless typed topics, the `autoincrement` column modifier, `with (key.index = on)` and `autoincrement.reserve/.max`, `create [or replace] [transient] [unique] view … key <keyspec> as select … from … [group by …] [constraint … check (…)]*`, `select … where … [limit n]` decoded by the CLI, `show topic` / `show view`, and the `GetStats` (84) / `StatsResponse` (85) wire that serves every counter earlier parts keep, with `rune stats --json`. The AdminWorker's message-search mapping of compile errors is removed (SD3).

**Architecture:** `ql/` stays a pure library: the parser builds new AST nodes, and `ql::applyViewPlan` turns a `CreateViewDef` plus a resolved `ViewSource` into the codec's authored `rune::view::ViewPlan` (Part 07's `ViewPlanAuthored.h`). The CLI (`client-cpp/src/cli`) resolves sources from the topics earlier in the same script (for `--dry-run` and for refusing a bad script before anything is sent) and from the live catalog when a statement runs; `select` builds its key with Part 07's `ViewKeyBuilder` and decodes rows with the view's output descriptor. `GetStats` is a cold worker op served on the GetViewRow route (leader only, redirect otherwise) from fixed-size scratch, through one collector header that Parts 09 and 11 extend.

**Tech Stack:** C++23, GoogleTest (`TEST_P` tables with pinned cell counts), Google Benchmark (`broker/benchmark`), CMake/Ninja, SBE 1.40.2 for the internal ring, real Aeron, in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`).

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`: §10 in full, §13, the target block of §1, §4 (keyless typed topics, `partitions` default), §6.2 (the modifier), §7.1 (key forms), §8.1 (`lower()` literals), §12 (gates), and the QL half of the §15 `lower('Foo')` cell. Contract: `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C1 template ids 84/85, C13, C14, C15 `OpKind::GetStats`, C16, SD3, SD11, SD13, index row 08). Committed predecessors read through their Interfaces and Contract additions: plans 02 (P6, P10, Task 7, `featureRefusal` texts), 03 (codes 51–58 and `errorCodeName`), 04 (`VerdictSourceStats`, `verdictStatsForTest`, `CheckedWritesRig`), 05 (`TopicKeyFeatures`, `TopicOptions::keyFeatures`, P5-6), 06 (P6-1, P6-5, `AutoIncStats`, StatIds 8–10) and 07 (C14 format, P7-3, P7-8, P7-9, P7-14, `ViewKeyBuilder`, `lowerUtf8`). Controller rulings: `rulings.md` R02d, R06f, R07a, R07c, R07e, R07f. Executors read the spec sections, the overview, plan 07 and this file.

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-08-ql`, cut from the latest `main` once Parts 06 and 07 have merged (`git fetch && git switch -c feature/checked-writes-08-ql origin/main`). Merge `main` in again before review.
- **Preconditions** (each must hit before Task 1; a miss means a predecessor shipped differently — stop and tell the controller):
  - `grep -n "struct KeyComponent" codec/include/rune/codec/ViewPlanAuthored.h` (07, C14)
  - `grep -n "lowerUtf8" codec/include/rune/codec/UnicodeLower.h` (07)
  - `grep -n "class ViewKeyBuilder" client-cpp/include/rune/client/view/ViewKeyBuilder.h` (07)
  - `grep -n "enum class CompileErrorKind" broker/libs/view/include/rune/view/PlanCompiler.h` (07, P7-14)
  - `grep -n "unique views are not available yet" broker/libs/node/src/AdminWorker.cpp` (07, P7-9; one hit)
  - `grep -n "ScanViewRows" client-cpp/include/rune/client/Completion.h` (07, the last `OpKind`)
  - `grep -n "keyFeatures" client-cpp/include/rune/client/Client.h` (05, `TopicOptions::keyFeatures`; 06, `TopicSchemaResult`)
  - `grep -n "autoIncStatsForTest" broker/libs/node/include/rune/node/Worker.h` (06) and `grep -n "verdictStatsForTest" broker/libs/node/include/rune/node/Worker.h` (04)
  - `grep -n "PartitionsDefaultToOne" ql/tests/ParserTest.cpp` (02 Task 7) and `grep -n "kind raw" client-cpp/src/cli/CatalogCommands.cpp` (02 Task 7)
  - `grep -n '"ViewUniqueInvalid"' client-cpp/src/cli/QlEngine.cpp` (03, `errorCodeName` 48–58)
  - `grep -n 'GET_VIEW_ROW_K">29\|SCAN_VIEW_ROWS">30\|VIEW_SCAN_RESULT">9' protocol/schema/rune-internal-ring.xml` (07; three hits) and `grep -n 'TXN_VOTE' protocol/schema/rune-internal-ring.xml` (0 or 2 hits: 11 may not have merged; 28 and return 8 stay reserved for it either way)
- **Builds** (already configured; if one is missing, stop and tell the controller): `build/root` (Ninja, Debug, `BUILD_TESTING=ON`; `cmake --build build/root -j14 --target <t>`, `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`), `build/sub-ql` (`rune_ql_tests`), `build/sub-client-cpp` (`rune_cli_offline_tests`, `rune_cli_catalog_tests`, the client suites), `build/sub-codec` (CatalogGolden), `build/rel` (Release, `BUILD_BENCHMARKS=ON`, binary `build/rel/broker/rune_broker_benchmark`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Codec sources are listed three times** (overview). This part adds none to the codec; it adds the codec sources Part 07 created to `ql/CMakeLists.txt`'s standalone `rune_codec_dep` (Task 4), the fourth list that exists only for `build/sub-ql`.
- **SBE.** The internal-ring flyweights under `protocol/include/rune_protocol_internal_sbe/` are generated and committed. Regenerate them with a local `sbe-all-1.40.2.jar` given through `$SBE_JAR`. Never commit the jar.
- **VM.** This part has no docker e2e of its own; its real e2e is `rune_client_broker_tests` on the default shard runtime (Task 10). Stage (c) of the regression script runs on the shared VM (`ssh root@$RUNE_VM_HOST`, every command under `flock /root/vm.lock`, in your own directory), never on local Mac docker.
- **Commits.** `git add <paths>` only (never `.superpowers/`, build logs or `-A`). Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

## Global Constraints

Copied verbatim from the overview (Plan 00). Every task implicitly includes this section.

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

## Rulings this part depends on (restated, binding)

- **SD1.** A topic without a field list stays an untyped raw log (`topic events;`). §10.1's "the field list is required" means only that every database feature needs one.
- **SD3.** Compile errors carry 56, 57 and 58 (07 maps them by `CompileErrorKind`). **This part removes the message-search mapping** at `broker/libs/node/src/AdminWorker.cpp:1239-1245` (Task 5).
- **SD11.** The client gates `GetStats` on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`; below that it fails locally with `OpStatus::BrokerTooOld`.
- **SD13.** This part adds `show topic`, `show view`, and `GetStats` / `StatsResponse` (84/85, C16). "Metrics export" is `rune stats --json` over `GetStats`. Aeron-counter export is not built; this part files the issue (Finish).
- **R02d (02 P10).** `key` becomes optional on typed topics in QL: `topic t { … };` is a keyless typed topic.
- **R06f.** This part owns the QL `autoincrement` column modifier and the QL cell "an autoincrement topic with no `partitions` gives 1 partition". The parser change that made `partitions` optional is Part 02's Task 7 (precondition above); this part keeps it and adds the autoincrement cell.
- **R07a.** The width of a lowered `string(N)` key component is the table value ⌊3N/2⌋ at Unicode 16.0.0 (`rune::codec::maxLoweredWidth`); the QL never computes it itself.
- **R07c.** `group by <cols>` may name several columns, any key-eligible type and `lower(string(N))`; it must equal the view's key spec. The compiler is the authority for that rule; QL writes `groupBy` as the user wrote it.
- **R07e / P7-9.** The AdminWorker refuses `unique` with `ViewUniqueInvalid` (58), "unique views are not available yet", and any constraint with `ViewConstraintInvalid` (57), "check constraints are not available yet". Part 09 lifts both.
- **R07f.** Part 07 took op types 29 and 30, tail kind 13, return type 9, view-read status 8 and matrix ids 60–68. **This part's internal-ring numbers start at return type 10** (P8-9). This part owns the QL half of the `lower('Foo')` cell.
- **C14.** Part 07 owns the format of every plan v2 field; this part authors plans; Part 09 evaluates constraints.
- **C16.** The stat ids and their keepers are the overview's table, verbatim. "`GetStats` serves every counter that exists": ids whose keeper has not merged (09: 32; 11: 11–20) are added to the collector by that part (P8-10).

## Part rulings (P8; binding for executors, recorded so a reviewer can overturn any)

- **P8-1 — `unique` and `check` before Part 09.** QL parses `[unique]` and every `constraint <name> check (<predicate>)`, and `applyViewPlan` authors them into the plan (`unique = true`, `constraints`). QL adds **no** refusal of its own: the clear refusal is the AdminWorker's P7-9 pair (58 / 57, messages above), which `rune ql` prints as `ViewUniqueInvalid (58) unique views are not available yet`. Part 09 lifts the refusal by deleting those two AdminWorker lines; QL needs no change. This part's tests that pin the refusal through QL end to end are named `…BeforePart09…` so Part 09 finds and inverts them (Task 10).
- **P8-2 — Keyless typed topics (R02d).** `topic t { … };` parses with `keyField = ""`. `autoincrement` on a keyless topic is a parse error with 02's text, "autoincrement needs a key". Every other refusal of a keyless or untyped topic (views 35 "view source needs a key", `key.index` "key.index needs a key", …) stays the broker's (02's `featureRefusal`), so QL passes such statements through.
- **P8-3 — The modifier and the settings.** `<field> <type> autoincrement` is legal only on the key field (else "autoincrement is allowed only on the key field; '<f>' is not the key") and once per field ("duplicate modifier 'autoincrement'"). It sets `DefineTopicPlan::keyFeatures.autoIncrement`. The type rule (`uint64`, `int64`, `uint32`), the compact-only rule and every range stay the broker's (06 P6-5), like today's `topicConfigError`. QL never sets `keyIndex` or `partitioner` on its own: the broker normalizes (06 P6-1). New creation-only settings: `key.index = on | off`, `autoincrement.reserve = <int>`, `autoincrement.max = <int>`; `alter topic … set` refuses them as immutable.
- **P8-4 — Authoring rules (`applyViewPlan`).** The authored plan is the lowest shape that carries the statement, so a QL view and a code-authored view of the same shape have the same normalized bytes and catalog fingerprint:
  1. **One plain key column** (`key c` or `key (c)`): `plan.key = c`, `keyComponents` empty. Otherwise `plan.key = ""` and `keyComponents` = every component in order (`{column, keyFunction, outName}`; a plain component's `outName` is its column).
  2. **`sources[0]`** = `{topicId, keyField}`: `topicId` is the resolved topic id (−1 for a source defined earlier in a script that has not run, used only by `--dry-run`); `keyField` is the source topic's key field for a projection, and the first `group by` column's source column for an aggregate (07 P7-8: "`keyFieldId[0]` is the first group column, as today").
  3. **`groupBy`** is written only when the key is not one plain column, or when `group by` is not exactly that one column; then it is every `group by` item (`{column, keyFunction, outName}` where `outName` is the matching key component's `outName`, else the column).
  4. **Output columns**: every plain key column not named in the select list becomes an implicit `Replace` column (`name = srcField = column`, the source type); then the select items in authored order. A `lower()` key column adds nothing (the compiler adds the derived `FixedStr`, P7-15). `*` expands to every source field in source order (a projection only).
  5. **Types**: `Replace` takes the source field's type; aggregates take `rune::view::aggregateOutputType(rule, srcType)` (contract addition 1). No output type → a local error.
  6. **`unicodeVersion`** = `rune::codec::kUnicodeVersion` iff some component or `group by` item uses `lower`, else 0.
  7. **Function → rule**: `count(*)` `CountAll` (`srcField ""`), `count(c)` `Count`, `sum` `Sum`, `avg` `Avg`, `min` `Min`, `max` `Max`, `last` `Last`; `persistence` = 1 for `create transient view`; `sizing` default.
  8. **What QL refuses locally** (it cannot author a plan otherwise): an untyped source, a column that is not a source field, an aggregate with no output type, `*` in an aggregate view, aggregate items without `group by`, and `group by` without an aggregate item (exact messages in Task 4). Everything else — the identity rule, widths, `group by` = key, `unique` on an aggregate, partition rules, keyless sources — is the broker's, and Task 5's matrix proves QL and the broker agree.
- **P8-5 — Predicate constants** (contract addition 2). A `Const` node's `constType` is the compared column's output type and `constBytes` holds the constant in that column's **row-slot encoding** (`ViewRowCodec.h` `readSlot`): integers and `Enum` little-endian at `fixedSize(type)`, `Bool` one byte 0/1, `Float64` IEEE-754 little-endian, `FixedStr(N ≤ 16)` the UTF-8 bytes zero-padded to N; the rest of the 16 bytes zero. A literal that does not fit the column ("constraint 'n': -1 does not fit uint64 column 'c'"), or a column wider than 16 bytes or variable ("constraint 'n': check compares fixed-width columns of at most 16 bytes; 'c' is string(64)"), is a local error. Integer literals compare with `Float64` columns as their exact double. Part 09 evaluates by this rule.
- **P8-6 — `select` shape.** The parser accepts `where <col> = <lit> [and …]`. Which columns are key columns is known only with the view's plan, so the shape rule is applied when the statement is planned (Task 9): the set of named columns equals every key column → a point read; equals the leading n−1 (n ≥ 2) → a scan; anything else → "where must name every key column (<all>) for a point read, or the leading (<n−1>) for a scan". A key column may be named by its `outName` or, for a plain component, its column. `limit n` is legal only on a scan. A literal `lower('…')` on a `lower()` key column passes the literal's own text to `ViewKeyBuilder::setString` (the builder lowers with the codec table); on a plain `string(N)` column QL lowers it with `rune::codec::lowerUtf8` first. Both reach the same table (R07a, §8.1 "the same table runs everywhere").
- **P8-7 — Script scope.** `prepare` takes a `PrepareScope` that collects the `TopicDef`s already prepared in the script. A `create view` whose source is in the scope is authored at prepare time (so `--dry-run` prints the full plan and a bad view refuses the script before anything is sent); one whose source is not is printed as resolved at run time. At run time every view is re-authored from the live catalog after `awaitLastDdl()`, so a topic created earlier in the same script is visible.
- **P8-8 — `create or replace`.** At run time QL reads the catalog: an existing view → `alterViewAndWait(name, keyDisplayName(plan), persistence, expectedVersion = its catalog version, blob)`, so a concurrent change answers `SchemaConflict`; no view → `createViewAndWait`. `keyField` sent with either is `rune::view::keyDisplayName(plan)` (07 Task 15).
- **P8-9 — Internal-ring numbers.** `WorkerOp::Type::GetStats = 31` (SBE `OpType` `GET_STATS = 31`) and `WorkerReturn::Type::StatsResult = 10` (SBE `ReturnType` `STATS_RESULT = 10`). No tail kind: the request rides scalars (`topicId` = objectId, `partition`, `recordCount` = scope). Checked against the code and the other plans: op types 26/27 are 02's, 28 is 11's, 29/30 are 07's; return types 8 (11) and 9 (07).
- **P8-10 — Stats serving.** Served by the partition leader's owning worker only (an ex-leader's or follower's counters would be stale), routed exactly like `GetViewRow` (redirect / bare `NotLeader`). Scope 1 needs `DESCRIBE` on the topic, scope 2 `DESCRIBE` on the view (every grant implies `DESCRIBE`, D15). Ids are sent in ascending order; an id is present iff its counter exists on that partition (C16); a plain typed or untyped topic answers `OK` with zero entries. `StatsResponse` fits a fixed `kMaxEncodedSize` of 3,742 bytes, so the worker encodes into worker-owned scratch and allocates nothing.
- **P8-11 — Compile errors after SD3.** Every `CompileError` site that Part 07 left `Generic` gets an explicit kind equal to the code today's message search gives it **for a view whose name contains no "key"**: sites whose fixed text contains "key" (`PlanCompiler.cpp:254-281`, `:293-296`) → `KeyInvalid` (35), every other one → `FieldUnresolved` (34). The AdminWorker then maps `Generic` to 34 (it is unreachable; a test proves it) and passes the compiler's message as the detail for every compile refusal.

## Contract additions (this part adds them; the overview stays the authority for everything else)

1. **`codec/include/rune/codec/ViewPlanAuthored.h` / `codec/src/ViewPlanAuthored.cpp` (07's files):** `[[nodiscard]] std::optional<rune::codec::PrimType> aggregateOutputType(MergeRule rule, rune::codec::PrimType src) noexcept;`, moved unchanged from `broker/libs/view/src/PlanCompiler.cpp:78-98` (its anonymous copy is deleted; the compiler calls the codec one), so QL and the broker type aggregates with one function.
2. **Predicate constant bytes** (C14 leaves them open): P8-5's row-slot encoding. Part 09's evaluator decodes `constBytes` with the same rule.
3. **`unique` / `check` before Part 09:** P8-1. 08 parses and authors; the refusal stays the AdminWorker's P7-9 lines (58 / 57); Part 09 deletes them and inverts `CheckedWritesQlBrokerRegression.RegressorsPathOutcomesBeforePart09ThreeNodesRegression` and `CheckedWritesQlCompileMatrix`'s unique admin cell.
4. **Wire (`protocol/include/rune/protocol/client/StatsMessages.h`, new):** `struct GetStats` (84), `struct StatsResponse` (85), `enum class StatsScope : uint8_t { TopicPartition = 1, ViewPartition = 2 };`, C16's `enum class StatId : uint16_t` verbatim, `[[nodiscard]] constexpr std::string_view statName(StatId) noexcept` (the C16 names, `"unknown"` otherwise), layouts and constants in Task 6. `TemplateId::GetStats = 84`, `StatsResponse = 85` (`Protocol.h:89-102`); `isKnownTemplate` covers them.
5. **Internal ring:** P8-9.
6. **`broker/libs/node/include/rune/node/StatsCollect.h` (new, header-only, worker thread):**
   ```cpp
   class StatsSink {   // over caller-owned fixed storage; never allocates
   public:
       StatsSink(std::span<uint8_t> statBytes, std::span<uint8_t> constraintBytes) noexcept;
       bool add(rune::protocol::client::StatId id, int64_t value) noexcept;      // ids ascending; false = full
       bool addConstraint(uint32_t constraintId, uint64_t rejections) noexcept; // false = full
       [[nodiscard]] uint16_t count() const noexcept;  [[nodiscard]] uint16_t constraintCount() const noexcept;
   };
   void collectTopicPartitionStats(const PartitionWriteState* state, const rune::replication::PartitionReplica& replica,
                                   StatsSink& sink) noexcept;   // ids 1-10 today; 11 appends 11-20
   void collectViewPartitionStats(const ViewFoldCounters& counters, StatsSink& sink) noexcept;   // 33-36; 09 appends 32
   ```
7. **Gateway:** `[[nodiscard]] bool ClientGateway::partitionLedElsewhere_(int32_t topicId, int32_t partition, std::string_view& endpoint) const noexcept` (the body of `viewSourceLedElsewhere_` `ClientGateway.cpp:5639-5651` after its source lookup; `viewSourceLedElsewhere_` calls it), `void handleGetStats(const uint8_t*, std::size_t)`, test hook `applyGetStatsForTest`.
8. **Client:** `OpKind::GetStats` appended after `ScanViewRows` (C15); `struct StatsResult { uint16_t status = 0; std::vector<std::pair<uint16_t, int64_t>> stats; std::vector<std::pair<uint32_t, uint64_t>> constraints; [[nodiscard]] std::optional<int64_t> get(rune::protocol::client::StatId) const noexcept; }` with statuses `kOk = 0`, `kUnknownObject = 4`, `kInvalidRequest = 5`, `kBusy = 6`, `kNotAuthorized = 45` and the client-side `kNotLeader = 8`; `OpResult Client::getStats(rune::protocol::client::StatsScope, int32_t objectId, int32_t partition, CompletionTarget, Token)`; `StatsResult Client::getStatsAndWait(rune::protocol::client::StatsScope, int32_t objectId, int32_t partition, std::chrono::nanoseconds timeout = kDefaultWaitTimeout)`; `[[nodiscard]] bool rune::client::decodeStatsPayload(std::span<const uint8_t>, StatsResult&)`.
9. **`ql` (`ql/include/rune/ql/Ast.h`, `Backends.h`):** `FieldDef` appends `bool autoIncrement = false;`; `DefineTopicPlan` appends `rune::protocol::TopicKeyFeatures keyFeatures{};`; the AST types of Tasks 2–3; `struct ViewSource`, `applyViewPlan`, `predicateText` (Task 4). `isSecurityStatement` becomes an exact type test (Task 2).
10. **CLI (`client-cpp/src/cli/QlEngine.h`):** `PrepareScope`, the `prepare` overload taking it, `PCreateView`, `PSelect`, `PShowTopic`, `PShowView`, `SelectRead`, `planSelect`, `formatRows`, `formatStatsText`, `formatStatsJson`; `cmdStats` (`CatalogCommands.cpp`).
11. **Test support:** `CheckedWritesRig::getStats(StatsScope, int32_t objectId, int32_t partition) -> StatsReply` (`struct StatsReply { uint16_t status; std::vector<std::pair<uint16_t, int64_t>> stats; std::vector<std::pair<uint32_t, uint64_t>> constraints; bool notLeader; }`); `CatalogDdlRig::DefineArgs` appends `std::vector<uint8_t> descriptorBlob;` (non-empty replaces the rig's fixed descriptor; skip if 05 or 07 already added an equivalent) and `CatalogDdlRig::createViewWithPlan(const std::string& view, const std::string& keyField, uint8_t persistence, std::vector<uint8_t> planBlob) -> Answer` (skip if 07 added an equivalent; then use it).

## Review Focus

The five inputs the spec implies but no requirement line names, most likely to bite first. Each is pinned by a test in the owning task.

1. **The new statements land after the security statements in the `Statement` variant**, and `isSecurityStatement` is `s.index() >= 8` today (`ql/src/Redact.cpp:101`). Unfixed, every `create view`, `select` and `show` would be treated as a security statement (rendered through `redactedText`, sent as security DDL). Test: `CheckedWritesQlRegression.NewStatementsAreNotSecurityStatementsRegression` (Task 2).
2. **A view or column whose name contains "key"** (`monkey`, `apiKey`) refused for an unrelated reason: today's message search answers `ViewKeyInvalid` (35) because the message quotes the name. After SD3 it is the site's own code, with the compiler's message as the detail. Test: `CheckedWritesAdminViewDdlRegression.NamesContainingKeyKeepTheirCodeRegression` (Task 5).
3. **Literals at the edge of their column** in `select` and `check`: −1 into a `uint64`, 2^64 − 1 into a `uint64`, `INT64_MIN` into an `int64`, 70000 into a `uint16`, a 17-byte string into `string(16)`, `lower('ȺȺȺȺȺȺȺȺ')` that grows past a plain `string(16)` column, and a `true` into an integer column. Each is a clear local error, never a truncated key. Tests: `CheckedWritesQlSelectRegression.LiteralEdgesAreRefusedNeverTruncatedRegression` (Task 9), `CheckedWritesQlPlanRegression.CheckConstantEdgesRegression` (Task 4).
4. **A script that creates a topic and then a view over it, run live.** The view must be authored against the topic the script just created (after `awaitLastDdl()`), never against a stale catalog ("source not found"), and a bad view must refuse the whole script before the topic is sent. Tests: `CheckedWritesQlBrokerRegression.ScriptCreatesTopicThenViewShardRegression` (Task 10), `CheckedWritesQlCli.BadViewRefusesTheScriptBeforeAnythingIsSent` (Task 9).
5. **Keywords used as names.** Columns or aliases named `key`, `count`, `min`, `last`, `group`, `check`, `unique`, `from`, and an alias equal to a source column (`last(platform) as platform`). An aggregate is recognized only when the name is followed by `(`; everything else stays a name. Test: `CheckedWritesQlRegression.KeywordsAreNamesWhereTheGrammarAllowsRegression` (Task 3).

---

## Interaction matrix

The overview's §15 owner list gives Part 08 no cells of its own beyond the two rulings: the QL half of "`lower('Foo')` from QL and from the client key builder equals the broker's key" (R07f) and the QL cell of "`partitions` omitted gives 1 partition" for an autoincrement topic (R06f). The shared `CheckedWritesMatrixExpectations.h` reserves no id range for 08 (04 0–19, 05 20–39, 06 40–59, 07 60–79, 09 80–99, 11 100–119), so, like Parts 02 (P11) and 10 (R10e), this part's matrices are their own `TEST_P` tables with their own enums and count pins. The language is stateless, so its model check is a grammar round-trip (QM5); the stats surface reads counters that Parts 04–07 already model-check.

### QM1 — Topic grammar (`rune_ql_tests`, `CheckedWritesQlTopicGrammar/Cell.ParsesToThePlan/<n>`, Task 1)

Each row is one statement and its exact `DefineTopicPlan` (or the exact error). Pinned by `CheckedWritesQlTopicGrammarCount.CellCountIsPinned` (`EXPECT_EQ(kTopicCells.size(), 18u)`).

| # | Statement | Expected |
|---|---|---|
| 1 | `topic e;` | untyped, `partitions 1` (02's cell, re-pinned) |
| 2 | `topic t { id uint64 };` | typed, `keyField ""`, `partitions 1` (R02d) |
| 3 | `topic t { id uint64, v int64 } partitions 2;` | typed keyless, `partitions 2` |
| 4 | `topic t { id uint64 autoincrement, v int64 } key id with (cleanup.policy = compact);` | `partitions 1`, `keyFeatures == {false, true, Fnv1a, 2^53−1, 4096}`, `fields[0].autoIncrement` (**R06f cell**) |
| 5 | `topic t { id uint64 autoincrement } key id with (cleanup.policy = compact, autoincrement.reserve = 1024, autoincrement.max = 1000000);` | reserve 1024, max 1000000 |
| 6 | `topic t { id uint64, v int64 } key id with (key.index = on);` | `keyIndex == true` |
| 7 | `… with (key.index = off);` | `keyIndex == false` |
| 8 | `topic t { id uint64 autoincrement, v int64 } key v;` | error "autoincrement is allowed only on the key field; 'id' is not the key" |
| 9 | `topic t { id uint64 autoincrement };` | error "autoincrement needs a key" |
| 10 | `topic t { a uint64 autoincrement, b uint64 autoincrement } key a;` | error "autoincrement is allowed only on the key field; 'b' is not the key" |
| 11 | `topic e with (key.index = on);` | parses, untyped, `keyIndex true` (the broker answers "key.index needs a typed topic") |
| 12 | `topic t { id uint64 } key id with (key.index = yes);` | error "invalid value 'yes' for key.index" |
| 13 | `topic t { id uint64 } key id with (key.index = on, key.index = off);` | error "duplicate setting 'key.index'" |
| 14 | `alter topic t set key.index = on;` | error "unknown or immutable topic setting 'key.index'" |
| 15 | `topic t { id uint64 autoincrement autoincrement } key id;` | error "duplicate modifier 'autoincrement'" |
| 16 | `topic t { id uint64 } key id;` | unchanged: `keyFeatures == {}` |
| 17 | `transient topic t { id uint64 autoincrement } key id with (cleanup.policy = compact);` | parses (the broker answers "autoincrement needs a persisted topic") |
| 18 | `topic t { name string(16) autoincrement } key name;` | parses (the broker answers "autoincrement needs a uint64, int64 or uint32 key") |

### QM2 — QL plans against the broker (`rune_checked_writes_tests`, `CheckedWritesQlCompileMatrix/Cell.QlPlanMeetsTheBroker/<n>`, Task 5)

Source `src {id uint64, userId uint64, name string(16), tag string(80), gender uint16, n int64}`; each cell parses one `create view` statement, authors it with `ql::applyViewPlan`, then (a) compiles it with `rune::view::compileView` against the source descriptor and (b) submits it through a real `AdminWorker` (`CatalogDdlRig`, level 4) after defining the cell's source. It asserts the authoring succeeded, (a)'s result (OK with `keyModeOf`/`keyWidth`, or the `CompileErrorKind`), and (b)'s code.

| Axis | Values |
|---|---|
| `KeyForm` | K1 `key id`; K2 `key userId`; K3 `key (userId, id)`; K4 `key lower(name) as nameKey`; K5 `key (lower(name) as nameKey, id)`; K6 `key (lower(tag) as tagKey, id)` (K = 120 + 8 = 128); K7 `key (lower(tag) as tagKey, id, gender)` (K = 130) |
| `ViewKind` | Projection `as select n from src`; Aggregate `as select count(*) as c from src group by <the key's columns, lower() kept>`; Unique `create unique view … as select n from src` |
| `Source` | S1 `key id`, 1 partition; S2 `key id`, 3 partitions; S3 keyless, 1 partition |

Expected admin codes (b), Projection / Aggregate, S1 · S2 · S3:

| | Projection | Aggregate |
|---|---|---|
| K1 | 0 (LegacyU64) · 0 · 35 | 0 (LegacyU64) · 36 · 35 |
| K2 | 56 (identity) · 56 · 35 | 0 (LegacyU64) · 36 · 35 |
| K3 | 0 (Keyed, owner map) · 36 · 35 | 0 (Keyed) · 36 · 35 |
| K4 | 56 (identity) · 56 · 35 | 0 (Keyed, K 24) · 36 · 35 |
| K5 | 0 (Keyed, K 32) · 36 · 35 | 0 (Keyed, K 32) · 36 · 35 |
| K6 | 0 (Keyed, K 128) · 36 · 35 | 0 (Keyed, K 128) · 36 · 35 |
| K7 | 56 ("view key is 130 bytes; the limit is 128") · 56 · 35 | 56 · 56 · 35 |

S3's 35 is 02's keyless refusal, "view source needs a key", which runs before the compile. Unique cells run (a) only, on S1: K1–K6 compile OK (K2–K6 owner-mapped), K7 → `KeyExpressionInvalid`; plus one admin cell, K5 × S1 → 58 "unique views are not available yet" (**BeforePart09**). Count: 7 × 2 × 3 = 42 admin cells + 7 compile cells + 1 = **50**, pinned by `CheckedWritesQlCompileMatrixCount.CellCountIsPinned`.

### QM3 — `select` planning (`rune_cli_catalog_tests`, `CheckedWritesQlSelectMatrix/Cell.PlansTheRead/<n>`, Task 9)

Views (authored plans + the source descriptor, no broker): V1 `key characterId` (n = 1), V2 `key (userId, characterId)`, V3 `key (lower(name) as nameKey, characterId)`, V4 `key lower(name) as nameKey` (n = 1). `where` shapes: W1 every key column; W2 the leading n−1; W3 only the last key column; W4 a non-key column; W5 every key column plus a non-key column; W6 a key column twice. The composite views V2/V3 take all six; the single-column views V1/V4 take W1, W4, W5, W6 (W2 would be an empty `where`, W3 equals W1). Expected: W1 → Point; W2 → Scan; W3, W4, W5 → "where must name every key column (<all>) for a point read, or the leading (<n−1>) for a scan" (V1/V4 print "where must name the key column (<c>)"); W6 → "duplicate where column '<c>'". Each Point/Scan cell asserts the exact key or prefix bytes (07's encoding goldens). 2 × 6 + 2 × 4 = **20 cells**, pinned by `CheckedWritesQlSelectMatrixCount.CellCountIsPinned`.

### QM4 — `GetStats` on the worker (`rune_checked_writes_tests`, `CheckedWritesStats/Cell.ServesExactlyTheCountersThatExist/<n>`, Task 7)

Scope 1 objects: a key-index topic (05; ids 1–7), an autoincrement topic (06; ids 1–10, it implies the key index), a plain typed topic (no ids), an untyped topic (no ids), a topic id this worker does not own (the not-leader sentinel in both roles; the gateway's `kUnknownObject` for an id the registry lacks is `CheckedWritesStatsGateway.RefusalsInOrder`). Scope 2 objects: a projection view over the plain typed topic (ids 33–36), an unknown view id (`kUnknownObject`). Each × role {Leader, Follower (the internal not-leader sentinel → redirect)}. **14 cells**, pinned by `CheckedWritesStatsCount.CellCountIsPinned`. Every leader cell asserts the ids in ascending order, each value equal to the keeper's test seam (`verdictStatsForTest`, `autoIncStatsForTest`, `foldCounters`), and the constraint list equal to `VerdictSourceStats::perConstraint`.

### QM5 — Grammar model check (`rune_ql_tests`, Task 3)

`CheckedWritesQlFuzzRegression.GeneratedStatementsRoundTrip`: seeds 1–500; a generator draws a random `CreateViewDef` / `SelectDef` / `TopicDef` from the grammar (names from a pool that includes keywords, every key form, 0–3 constraints with nested `and`/`or`/`not`, every literal kind), renders it to text with random spacing, case and comments, parses it, and asserts the AST equals the generated one. `CheckedWritesQlFuzzRegression.RandomTokensNeverCrash`: seeds 1–2000; 0–60 random tokens from the full token set → `parse` returns a value or an error, never crashes (run under ASan at Checkpoint B).

### The §15 cells and where they are tested

| §15 cell | Test (task) |
|---|---|
| `lower('Foo')` from QL equals the broker's key (QL half, R07f) | `CheckedWritesQlSelect.LowerLiteralEqualsTheBrokerKey` (9), `CheckedWritesQlBrokerRegression.LowerLiteralReadsTheRowShardRegression` (10) |
| `partitions` omitted gives 1 partition, autoincrement topic (R06f) | QM1 row 4 (1), `CheckedWritesQlBrokerRegression.RegressorsPathOutcomesBeforePart09ThreeNodesRegression` (`show topic character` → `partitions 1`) (10) |
| an untyped and a keyless topic refuse views at DDL (02's cell, through QL) | QM2 S3 column (5), `CheckedWritesQlBrokerRegression.KeylessTypedTopicFromQlShardRegression` (10) |

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `ql/include/rune/ql/Ast.h`, `ql/src/Redact.cpp` | M | New AST nodes, `FieldDef::autoIncrement`, exact `isSecurityStatement` |
| `ql/src/Parser.cpp`, `ql/src/ConfigKeys.h` | M | Tokens, topics, `create view`, `select`, `show topic/view`; the three new settings |
| `ql/include/rune/ql/Backends.h`, `ql/src/Backends.cpp` | M | `keyFeatures`, `ViewSource`, `applyViewPlan`, `predicateText` |
| `ql/CMakeLists.txt` | M | 07's codec sources in `rune_codec_dep`; new tests; `RUNE_QL_GOLDEN_DIR` |
| `ql/tests/CheckedWritesQl{Topic,View,Select,Plan,Fuzz}Test.cpp`, `ql/tests/golden/regressorspath.rql` | C | ql suites and the acceptance script |
| `codec/include/rune/codec/ViewPlanAuthored.h`, `codec/src/ViewPlanAuthored.cpp`, `broker/libs/view/src/PlanCompiler.cpp` | M | `aggregateOutputType` moved (contract addition 1); every compile site gets a kind (P8-11) |
| `broker/libs/node/src/AdminWorker.cpp:1239-1245` | M | The message search removed; detail passed |
| `protocol/include/rune/protocol/client/StatsMessages.h` | C | 84/85, `StatId`, `statName` |
| `protocol/include/rune/protocol/client/{Protocol.h,Dispatch.h}`, `protocol/conformance/{gen_vectors.cpp,manifest.md,vectors/get_stats.bin,vectors/stats_response.bin}` | M/C | Template ids, known templates, vectors |
| `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*`, `broker/libs/node/include/rune/node/{WorkerOp.h,WorkerReturnCodec.h,Worker.h}` | M | Op 31, return 10, `executeGetStats_` |
| `broker/libs/node/include/rune/node/StatsCollect.h` | C | `StatsSink` and the two collectors |
| `broker/libs/gateway/{include/rune/gateway/ClientGateway.h,src/ClientGateway.cpp,include/rune/gateway/RequestRejection.h}` | M | `handleGetStats`, `partitionLedElsewhere_`, the return encoding |
| `client-cpp/include/rune/client/{Client.h,Completion.h,Completions.h}`, `client-cpp/src/Client.cpp` | M | `getStats`, `OpKind::GetStats`, `StatsResult` |
| `client-cpp/src/cli/{QlEngine.h,QlEngine.cpp,CatalogCommands.cpp,Console.cpp,Cli.cpp}` | M | Prepare scope, views, `select`, `show`, `rune stats` |
| `client-cpp/include/rune/cli/CatalogCommands.h` | M | `formatTopicDefinition` / `formatViewDefinition` extensions, `formatStatsText`, `formatStatsJson` |
| `client-cpp/tests/{RuneCliCatalogTest.cpp,RuneCliOfflineTest.cpp}`, `client-cpp/tests/CheckedWritesQlCliTest.cpp` (C), `client-cpp/tests/golden/regressorspath.dryrun.txt` (C), `client-cpp/CMakeLists.txt` | M/C | CLI offline tests and the dry-run golden |
| `broker/libs/node/tests/node/CheckedWrites{QlCompile,Stats,StatsGateway}Test.cpp` | C | `rune_checked_writes_tests` |
| `broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp` | C | Real e2e (`rune_client_broker_tests`, `broker/libs/node/CMakeLists.txt:1731`) |
| `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`, `broker/libs/node/tests/support/{CheckedWritesRig.h,CatalogDdlRig.h}`, `broker/libs/node/CMakeLists.txt` | M | Alloc gate, rig seams, `rune_ql` linked into `rune_checked_writes_tests` |
| `broker/benchmark/QlPlanBench.cpp` (C), `broker/benchmark/GetViewRowServeBench.cpp`, `broker/CMakeLists.txt:499-558` | M/C | QL ≤ 2–5 µs gate, `BM_GetStatsServe` |
| `docs/api/{ql.md,wire-protocol.md,client-cpp.md,admin-cli.md}` | M | The grammar, 84/85, `getStats`, `rune stats` |

---

## Phase 1 — The language (`ql/`, `build/sub-ql`)

Line numbers in `ql/` are today's; Parts 02 (Task 7) and 06 (`grant import`) edit `Parser.cpp` first, so locate each site by its function name.

### Task 1: Tokens and topics — keyless typed topics, `autoincrement`, `key.index`, `autoincrement.*`

**Files:**
- Modify: `ql/src/Parser.cpp` (header comment `:1-49`; `TK` `:65-81` and the lexer switch `:128-149`; `parseField` `:334-343`; `parseTopicStatement` `:572-671`, the key branch `:636-655`), `ql/src/ConfigKeys.h:12-38`, `ql/include/rune/ql/Ast.h:12` (`FieldDef`), `ql/include/rune/ql/Backends.h:20-28` (`DefineTopicPlan`), `ql/src/Backends.cpp:23-57` (`applySetting`, `applyPlan`), `ql/CMakeLists.txt` (the new test file)
- Test: `ql/tests/CheckedWritesQlTopicTest.cpp`

**Interfaces:**
- Consumes: `rune::protocol::TopicKeyFeatures`, `kAutoIncrementMaxDefault`, `kAutoIncrementReserveDefault` (C13, 05) from `rune/protocol/CatalogTypes.h` (ql already includes it).
- Produces:
  - Tokens `TK::Star` (`*`), `TK::Lt` (`<`), `TK::Le` (`<=`), `TK::Gt` (`>`), `TK::Ge` (`>=`), `TK::Ne` (`<>`), appended before `Eof`; `<` followed by `=` or `>` and `>` followed by `=` are one token (`adj` rules unchanged).
  - `struct FieldDef { std::string name; PrimType type; uint16_t fixedLen = 0; bool autoIncrement = false; };`
  - `DefineTopicPlan` appends `rune::protocol::TopicKeyFeatures keyFeatures{};`.
  - `ConfigKeys.h`: `Value::OnOff`; `Field::KeyIndex`, `Field::AutoIncrementReserve`, `Field::AutoIncrementMax`; `kSettings` grows to 17 with `{"key.index", OnOff, KeyIndex, 0}`, `{"autoincrement.reserve", Int, AutoIncrementReserve, 0}`, `{"autoincrement.max", Int, AutoIncrementMax, 0}` (alterKey 0 = creation-only).
  - Grammar: `topicStmt := "topic" IDENT [ "{" field ("," field)* ","? "}" ] [ "partitions" INT ] [ "replicas" INT ] [ "key" IDENT ] [ "with" "(" … ")" ] ";"`, `field := IDENT type [ "autoincrement" ]`. A typed topic no longer needs `key` (R02d); an untyped one still refuses it (`:637-639`).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesQlTopicTest.cpp`):
  - `CheckedWritesQlTopicGrammar/Cell.ParsesToThePlan/<n>`: the 18 QM1 rows, each `parse` → `applyPlan` and the exact fields (`partitions`, `keyField`, `fields[i].autoIncrement`, `keyFeatures`, `config.cleanupPolicy`) or the exact error message.
  - `CheckedWritesQlTopicGrammarCount.CellCountIsPinned`: `EXPECT_EQ(kTopicCells.size(), 18u)`.
  - `CheckedWritesQlTopic.NewTokensLex`: `classifyInput("a <= 3")` is `Incomplete`; `parse("topic t { a int32 } key a with (retention.ms = -1);")` still parses (the `Dash` rule is unchanged by the new tokens); `parse("topic t { a int32 } key a <")` fails with "expected ';', got '<'".
  - `CheckedWritesQlTopicRegression.KeyStillRefusedOnUntypedRegression`: `topic e key id;` → "'key' needs a field list: an untyped topic (no '{…}') has no key" (unchanged).
  - `CheckedWritesQlTopic.UserTopicOfTheTargetBlock`: the RegressorsPath `topic user { userId uint64 autoincrement, provider uint16, subject string(64), createdAt int64 } partitions 1 replicas 3 key userId with (cleanup.policy = compact);` → `partitions 1`, `replicas 3`, `keyField "userId"`, `fields[0] == {"userId", UInt64, 0, true}`, `fields[2] == {"subject", FixedStr, 64, false}`, `config.cleanupPolicy == 1`, `config.retentionMs == -1`, `keyFeatures.autoIncrement`, `!keyFeatures.keyIndex`, `keyFeatures.partitioner == Partitioner::Fnv1a`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-ql -j14 --target rune_ql_tests`. Expected: `no member named 'autoIncrement' in 'rune::ql::FieldDef'`.
- [ ] **Step 3: Implement** per the Interfaces and P8-2 / P8-3. `parseField` accepts a trailing `autoincrement` keyword (twice → the duplicate error); `parseTopicStatement` checks the modifiers after the key is known (keyless → "autoincrement needs a key"; a non-key field → the "only on the key field" error naming the first offending field in declaration order). `Value::OnOff` accepts `on` / `off` (case-insensitive, `parseWord`), else "invalid value '<w>' for key.index". `applySetting` routes the three fields into `plan.keyFeatures` (`keyIndex = value != 0`, `autoIncrementReserve = uint32_t(value)` when `0 ≤ value ≤ UINT32_MAX` else a `ParseError` "autoincrement.reserve must be between 0 and 4294967295" from the parser, `autoIncrementMax = uint64_t(value)` for `value ≥ 0` else "autoincrement.max must be >= 0"); `applyPlan` sets `keyFeatures.autoIncrement` from the fields. The header comment shows the new grammar lines.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-ql -R 'CheckedWritesQl|Parser|Backends|Script|CatalogStatements|SecurityStatements|AstSmoke' --output-on-failure`. Expected: PASS. An existing `ParserTest.cpp` case that asserted "expected 'key'" on a keyless typed topic is re-pinned to the new behaviour and recorded in the commit body (R02d lifts it by design).
- [ ] **Step 5: Commit.**
```bash
git add ql/src/Parser.cpp ql/src/ConfigKeys.h ql/include/rune/ql/Ast.h ql/include/rune/ql/Backends.h ql/src/Backends.cpp \
        ql/CMakeLists.txt ql/tests/CheckedWritesQlTopicTest.cpp ql/tests/ParserTest.cpp
git commit -m "feat(ql): keyless typed topics, the autoincrement modifier, key.index and autoincrement.* settings, comparison tokens" \
           -m "Deviation: ParserTest's 'typed topic needs key' case re-pinned (R02d lifts it by design)." \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `create [or replace] [transient] [unique] view`

**Files:**
- Modify: `ql/include/rune/ql/Ast.h` (new structs; `Statement` `:92-96` appends `CreateViewDef`), `ql/src/Redact.cpp:101` (`isSecurityStatement`), `ql/src/Parser.cpp` (`parseCreateStatement` `:854-878`; new `parseCreateView`, `parseKeySpec`, `parseSelectItems`, `parsePredicate`, `parseLiteral`; `parseStatement`'s expected-list text `:1214`)
- Test: `ql/tests/CheckedWritesQlViewTest.cpp` (add to `rune_ql_tests`)

**Interfaces:**
- Produces (`namespace rune::ql`, `Ast.h`):
  ```cpp
  enum class KeyFunction : uint8_t { None = 0, Lower = 1 };          // the plan's keyFunction byte
  struct KeyItem { std::string column; KeyFunction fn = KeyFunction::None; std::string outName; };   // outName = column when None
  enum class ItemKind : uint8_t { Star, Column, CountAll, Count, Sum, Avg, Min, Max, Last };
  struct SelectItem { ItemKind kind = ItemKind::Column; std::string column; std::string alias; };    // alias "" = none
  struct Literal { enum class Kind : uint8_t { Integer, String, Bool } kind = Kind::Integer;
                   bool negative = false; uint64_t magnitude = 0; std::string text; bool boolean = false;
                   bool lowered = false; };                                                        // lowered: lower('…'), select only
  enum class PredOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };   // = rune::view::PredicateOp values
  struct PredNode { PredOp op = PredOp::Column; uint16_t a = 0, b = 0; std::string column; Literal value; };   // flat, root last
  struct ConstraintDef { std::string name; std::vector<PredNode> predicate; };
  struct CreateViewDef { std::string name; bool orReplace = false; bool transient = false; bool unique = false;
                         std::vector<KeyItem> key; std::vector<SelectItem> items; std::string source;
                         std::vector<KeyItem> groupBy; std::vector<ConstraintDef> constraints; };
  ```
  `Statement` appends `CreateViewDef` (index 21). `isSecurityStatement(s)` is true exactly for `CreateUserDef … ShowSecurityHistoryDef` (an `std::visit` type test, no index arithmetic). `redactedText` returns `{}` for `CreateViewDef` (its catalog-statement `else`).
- Grammar (the header comment gets it verbatim):
  ```
  createView := "create" ["or" "replace"] ["transient"] ["unique"] "view" IDENT "key" keyspec "as" "select" items
                "from" IDENT ["group" "by" groupItem ("," groupItem)*] ("constraint" IDENT "check" "(" orExpr ")")* ";"
  keyspec    := keyItem | "(" keyItem ("," keyItem)* ")"
  keyItem    := IDENT | "lower" "(" IDENT ")" "as" IDENT
  items      := "*" | item ("," item)*
  item       := AGG "(" ("*" | IDENT) ")" "as" IDENT | IDENT ["as" IDENT]      AGG := count|sum|avg|min|max|last, only before "("
  groupItem  := IDENT | "lower" "(" IDENT ")"
  orExpr := andExpr ("or" andExpr)*   andExpr := notExpr ("and" notExpr)*   notExpr := "not" notExpr | "(" orExpr ")" | IDENT CMP literal
  CMP := "=" | "<>" | "<" | "<=" | ">" | ">="          literal := ["-"] INT | STRING | "true" | "false"
  ```
- Errors (exact): "'*' is only valid in count(*)"; "'*' cannot be combined with other select items"; "<fn>(…) needs 'as <name>'" (`count(*) needs 'as <name>'` for `count(*)`); "a check compares a column with a constant: <column> <op> <constant>"; "lower() takes a column name"; "expected 'view' after 'create [or replace] [transient] [unique]', got <x>"; `create` followed by anything else keeps "expected 'user', 'role' or 'view' after 'create', got <x>".

- [ ] **Step 1: Write the failing tests** (`CheckedWritesQlViewTest.cpp`):
  - `CheckedWritesQlView.TargetBlockViewsParse`: each of `user_identity`, `device_history`, `character_name`, `account_stats` (the RegressorsPath text) parses to the exact `CreateViewDef`: `user_identity` → `unique`, `key == {{provider,None,provider},{subject,None,subject}}`, `items == {{Column,userId,""}}`, `source "user"`; `device_history` → `key {userId, deviceFp}`, `items == {{Min,ts,firstSeen},{Max,ts,lastSeen},{CountAll,"",logins},{Last,platform,platform}}`, `groupBy {userId, deviceFp}`; `character_name` → `unique`, `key == {{name,Lower,nameKey}}`, `items {{Column,characterId,""}}`; `account_stats` → `key {userId}`, `items {{CountAll,"",characters}}`, `groupBy {userId}`, `constraints == {{"max_characters", [Column "characters", Const 12, Le(a=0,b=1)]}}`.
  - `CheckedWritesQlView.KeySpecForms`: `key c`, `key (c)`, `key (a, b, c)`, `key lower(n) as k`, `key (lower(n) as k, id)`; `key lower(n) as k as select …` (two `as`); `key lower(5)` → "lower() takes a column name".
  - `CheckedWritesQlView.Modifiers`: `create or replace transient unique view v …` → all three true; `create unique transient view` → "expected 'view' after 'create [or replace] [transient] [unique]', got 'transient'".
  - `CheckedWritesQlView.PredicateTree`: `check (a >= 1 and not (b <> 'x' or c < -5))` → nodes `[Col a, Const 1, Ge(0,1), Col b, Const 'x', Ne(3,4), Col c, Const -5, Lt(6,7), Or(5,8), Not(9), And(2,10)]`; `check (1 < a)` → the "column with a constant" error; `check (a = true)` → `Const{Bool, boolean = true}`; two constraints keep declaration order.
  - `CheckedWritesQlView.ItemErrors`: `select *, a` → combine error; `select sum(*)` → "'*' is only valid in count(*)"; `select count(*)` → "count(*) needs 'as <name>'"; `select min(ts)` → "min(…) needs 'as <name>'".
  - `CheckedWritesQlRegression.NewStatementsAreNotSecurityStatementsRegression` (Review Focus 1): `isSecurityStatement` is false and `redactedText` empty for a parsed `create view`; true for every security statement of `SecurityStatementsTest.cpp`'s table (unchanged).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-ql -j14 --target rune_ql_tests`. Expected: `no type named 'CreateViewDef' in namespace 'rune::ql'`.
- [ ] **Step 3: Implement** per the Interfaces. Predicates build the flat vector bottom-up, so children always sit below their parent and the root is last. Signed literals reuse `parseSignedInt`'s `adj` rule (magnitude and sign kept separately, so `-9223372036854775808` and `18446744073709551615` both parse; ranges are checked against the column in Task 4).
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-ql -R 'CheckedWritesQl|Parser|SecurityStatements|Script' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add ql/include/rune/ql/Ast.h ql/src/Redact.cpp ql/src/Parser.cpp ql/tests/CheckedWritesQlViewTest.cpp ql/CMakeLists.txt
git commit -m "feat(ql): create [or replace] [transient] [unique] view - key specs, aggregates, group by, check predicates; exact isSecurityStatement" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `select`, `show topic`, `show view`; the grammar model check

**Files:**
- Modify: `ql/include/rune/ql/Ast.h` (`Statement` appends `SelectDef`, `ShowTopicDef`, `ShowViewDef`), `ql/src/Parser.cpp` (`parseShowStatement` `:1110-1176`, new `parseSelect`, `parseStatement` `:1213-1241`)
- Test: `ql/tests/CheckedWritesQlSelectTest.cpp`, `ql/tests/CheckedWritesQlFuzzTest.cpp` (add both to `rune_ql_tests`; the fuzz file includes only `rune/ql/Parser.h`, and Task 5 also compiles it into `rune_checked_writes_tests` so `build/asan` runs it)

**Interfaces:**
- Produces:
  ```cpp
  struct WhereTerm { std::string column; Literal value; };
  struct SelectDef { std::vector<std::string> columns; /* empty = * */ std::string view; std::vector<WhereTerm> where;
                     std::optional<uint32_t> limit; };
  struct ShowTopicDef { std::string name; };
  struct ShowViewDef  { std::string name; };
  ```
  Indices 22, 23, 24. Grammar: `select := "select" ("*" | IDENT ("," IDENT)*) "from" IDENT "where" IDENT "=" selLit ("and" IDENT "=" selLit)* ["limit" INT] ";"`, `selLit := literal | "lower" "(" STRING ")"` (sets `lowered`); `show ("topic" | "view") IDENT ";"`; `show (…) IDENT "as" "of" …` is unchanged (one-token lookahead on `as`). `show schema IDENT;` keeps "expected 'as', got ';'". The statement dispatch accepts `select`, and its expected-list text names `'select'`.
- Errors: "limit must be >= 1"; "limit must be <= 4294967295"; "duplicate where column '<c>'" (detected here, by name); "lower() takes a string literal"; "a select needs 'where'".

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesQlSelect.SpecReadsParse`: `select * from account_characters where userId = 42;` → `columns {}`, `where {{userId, Integer 42}}`, no limit; `select characterId from character_name where nameKey = lower('Foo');` → `{{nameKey, String "Foo", lowered}}`; `select a, b from v where x = -3 and y = 'q' limit 10;` → two terms, `limit 10`.
  - `CheckedWritesQlSelect.ShowForms`: `show topic user;` → `ShowTopicDef{"user"}`; `show view device_history;` → `ShowViewDef`; `show topic user as of version 2;` → `ShowAsOfDef` (unchanged); `show schema user;` → "expected 'as', got ';'".
  - `CheckedWritesQlSelect.Errors`: `limit 0`, a duplicate `where` column, `lower(name)` in a `where` literal, a missing `where`.
  - `CheckedWritesQlRegression.KeywordsAreNamesWhereTheGrammarAllowsRegression` (Review Focus 5): `create view v key key as select count as min, last(platform) as platform, count(*) as group from src group by key;` parses with `key {{key}}`, `items {{Column,count,min},{Last,platform,platform},{CountAll,"",group}}`, `groupBy {key}`; `select from, check from unique where key = 1;` parses with columns `{from, check}`, view `unique`.
  - `CheckedWritesQlFuzzRegression.GeneratedStatementsRoundTrip` and `CheckedWritesQlFuzzRegression.RandomTokensNeverCrash` (QM5; the generator and renderer live in the test file).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-ql -j14 --target rune_ql_tests`. Expected: `no type named 'SelectDef'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-ql -R 'CheckedWritesQl|Parser|CatalogStatements|Script' --output-on-failure`. Expected: PASS (the fuzz tests included).
- [ ] **Step 5: Commit.**
```bash
git add ql/include/rune/ql/Ast.h ql/src/Parser.cpp ql/tests/CheckedWritesQlSelectTest.cpp ql/tests/CheckedWritesQlFuzzTest.cpp ql/CMakeLists.txt
git commit -m "feat(ql): select ... where ... [limit n], show topic, show view; grammar round-trip model check" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Prepare to plan v2 (`applyViewPlan`) and the RegressorsPath golden (QL leg)

**Files:**
- Modify: `ql/include/rune/ql/Backends.h`, `ql/src/Backends.cpp`, `ql/CMakeLists.txt` (`rune_codec_dep` gains `${RUNE_CODEC_ROOT}/src/ViewPlanAuthored.cpp` and `${RUNE_CODEC_ROOT}/src/UnicodeLower.cpp`, and `Utf8.cpp` if Part 02 made one; `target_compile_definitions(rune_ql_tests PRIVATE RUNE_QL_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/golden")`)
- Modify: `codec/include/rune/codec/ViewPlanAuthored.h`, `codec/src/ViewPlanAuthored.cpp` (contract addition 1), `broker/libs/view/src/PlanCompiler.cpp:75-98` (the anonymous copy deleted; the two call sites call `rune::view::aggregateOutputType`)
- Create: `ql/tests/golden/regressorspath.rql`, `ql/tests/CheckedWritesQlPlanTest.cpp`

**Interfaces:**
- Consumes: `rune::view::ViewPlan`, `KeyComponent`, `PredicateNode`, `PredicateOp`, `AuthoredConstraint`, `MergeRule`, `serializeAuthored`, `deserializeAuthored`, `needsV2`, `keyDisplayName` (07 Task 3); `rune::codec::kUnicodeVersion` (07 Task 1); `toDescriptor` (`Backends.cpp:8-19`).
- Produces (`Backends.h`):
  ```cpp
  struct ViewSource { std::string name; int32_t topicId = -1; std::string keyField; rune::codec::Descriptor descriptor;
                      int32_t partitions = 1; bool transient = false; };          // descriptor empty = untyped
  [[nodiscard]] std::expected<rune::view::ViewPlan, ParseError> applyViewPlan(const CreateViewDef&, const ViewSource&);
  [[nodiscard]] std::string predicateText(std::span<const rune::view::PredicateNode> nodes);   // "characters <= 12"
  [[nodiscard]] ViewSource viewSourceOf(const TopicDef&);                               // in-script source, topicId -1
  ```
  `applyViewPlan` follows P8-4 and P8-5. Its local errors (`ParseError{0, 0, msg}`, exact): "view source must be a typed topic" (02's text; empty descriptor); "column '<c>' is not a field of '<source>'"; "<fn>() cannot aggregate '<c>' of type <qltype>" (no output type); "'*' is not valid in an aggregate view"; "an aggregate view needs 'group by'"; "group by needs an aggregate item"; the two P8-5 constant errors and "constraint '<n>': column '<c>' is not a column of the view". `predicateText` renders comparisons as `<column> <op> <constant>` (constants decoded from `constBytes`: integers in decimal, strings quoted `'…'` up to the first NUL, `true`/`false`, `Float64` shortest round-trip), `And`/`Or` children that are `And`/`Or` in parentheses, `Not` as `not (<x>)`; QL type words are spelled as the grammar spells them (`uint16`, `string(64)`).
- The golden script `ql/tests/golden/regressorspath.rql` is the six statements of the brief, in its order, with spec §1's `character` topic inserted before `character_name` (the brief's block names `character` without defining it):
  ```sql
  topic user { userId uint64 autoincrement, provider uint16, subject string(64), createdAt int64 } partitions 1 replicas 3 key userId with (cleanup.policy = compact);
  create unique view user_identity key (provider, subject) as select userId from user;
  topic device_login { userId uint64, deviceFp string(64), platform uint16, ts int64 } key userId;
  create view device_history key (userId, deviceFp) as select min(ts) as firstSeen, max(ts) as lastSeen, count(*) as logins, last(platform) as platform from device_login group by userId, deviceFp;
  -- spec section 1's character topic (the appearance fields reduced to the three it names)
  topic character { characterId uint64 autoincrement, userId uint64, name string(16), gender uint16, hairSkin uint16, hairSheet uint16, headSkin uint16, createdAt int64, lastPlayed int64 } partitions 1 replicas 3 key characterId with (cleanup.policy = compact);
  -- unique and check: parsed and authored here; refused by the broker (58 / 57) until Part 09 lands
  create unique view character_name key lower(name) as nameKey as select characterId from character;
  create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);
  ```

- [ ] **Step 1: Write the failing tests** (`CheckedWritesQlPlanTest.cpp`; sources come from `viewSourceOf` of the script's own topics):
  - `CheckedWritesQlGolden.RegressorsPathParses`: `parseScript` of the golden file → 7 statements, kinds `TopicDef, CreateViewDef, TopicDef, CreateViewDef, TopicDef, CreateViewDef, CreateViewDef`.
  - `CheckedWritesQlGolden.RegressorsPathAuthorsPlanV2`: each view's plan, field by field:
    - `user_identity`: `key ""`, `sources {{-1, "userId"}}`, `output {{provider,UInt16,0,provider,Replace},{subject,FixedStr,0,subject,Replace},{userId,UInt64,0,userId,Replace}}`, `keyComponents {{provider,0,provider},{subject,0,subject}}`, `unique`, `groupBy {}`, `unicodeVersion 0`, `persistence 0`; `serializeAuthored(p)[0] == 0x02`; `keyDisplayName(p) == "provider,subject"`.
    - `device_history`: `key ""`, `sources {{-1, "userId"}}`, `output {{userId,UInt64,0,userId,Replace},{deviceFp,FixedStr,0,deviceFp,Replace},{firstSeen,Int64,0,ts,Min},{lastSeen,Int64,0,ts,Max},{logins,Int64,0,"",CountAll},{platform,UInt16,0,platform,Last}}`, `keyComponents` and `groupBy` both `{{userId,0,userId},{deviceFp,0,deviceFp}}`, `!unique`; byte 0 `0x02`.
    - `character_name`: `key ""`, `sources {{-1, "characterId"}}`, `output {{characterId,UInt64,0,characterId,Replace}}`, `keyComponents {{name,1,nameKey}}`, `unique`, `unicodeVersion == 0x00100000`; byte 0 `0x02`; `keyDisplayName == "nameKey"`.
    - `account_stats`: `key "userId"`, `sources {{-1, "userId"}}`, `output {{userId,UInt64,0,userId,Replace},{characters,Int64,0,"",CountAll}}`, `keyComponents {}`, `groupBy {}`, `constraints == {{"max_characters", [{Column,0,0,"characters"}, {Const,0,0,"",Int64, 0C 00 … 00}, {Le,0,1}]}}`; byte 0 `0x02` (the constraint forces v2); `predicateText(constraints[0].predicate) == "characters <= 12"`.
    - every plan: `deserializeAuthored(serializeAuthored(p)) == p` and re-serializing gives the same bytes.
  - `CheckedWritesQlPlan.SinglePlainSourceKeyIsTheV1Shape`: `create view character_by_id key characterId as select * from character;` → `key "characterId"`, `keyComponents {}`, `sources {{-1,"characterId"}}`, the nine source fields as `Replace` in source order, byte 0 `0x01`, and bytes equal to the same plan built field by field as `docs/api/client-cpp.md` ("Materialized views") authors one.
  - `CheckedWritesQlPlan.CompositeProjection`: `create view account_characters key (userId, characterId) as select name, gender, lastPlayed from character;` → output `userId, characterId` (implicit `Replace`), then `name, gender, lastPlayed`; `sources {{-1, "characterId"}}`.
  - `CheckedWritesQlPlan.AggregateTyping`: over `t {id uint64, i int32, u uint16, f float64, s string(8)}` key id: `sum(i)` → Int64, `sum(u)` → UInt64, `avg(i)` → Float64, `count(s)` → `Count` Int64, `min(f)` → Float64, `last(s)` → FixedStr; `sum(s)` → "sum() cannot aggregate 's' of type string(8)".
  - `CheckedWritesQlPlan.ShapeErrors`: an unknown column, `select *` with `group by`, aggregates without `group by`, `group by` without aggregates, an untyped source → the exact messages above.
  - `CheckedWritesQlPlan.GroupByIsWrittenAsTyped` (R07c): `key (userId, deviceFp) … group by deviceFp, userId` → `groupBy {{deviceFp,0,deviceFp},{userId,0,userId}}` (QL does not reorder; the compiler refuses, Task 5); `key lower(deviceFp) as fp … group by lower(deviceFp)` → `groupBy {{deviceFp,1,fp}}`, `unicodeVersion 0x00100000`.
  - `CheckedWritesQlPlanRegression.CheckConstantEdgesRegression` (Review Focus 3): on `account_stats`, `characters <= -1` passes (`Int64`), `characters <= 9223372036854775808` → "constraint 'max_characters': 9223372036854775808 does not fit int64 column 'characters'"; on a `uint64` column `-1` does not fit; on `string(16)` `'abc'` → `constBytes = 61 62 63` + zeros; on `string(64)` → the 16-byte rule; `true` on an integer column → does not fit.
  - `CheckedWritesQlPlan.TransientAndReplace`: `create or replace transient view …` → `persistence 1` (`orReplace` is the CLI's, P8-8).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-ql -j14 --target rune_ql_tests`. Expected: `no member named 'applyViewPlan' in namespace 'rune::ql'`.
- [ ] **Step 3: Implement** per the Interfaces; move `aggregateOutputType`.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/sub-ql -R 'CheckedWritesQl' --output-on-failure` → PASS.
  - `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'PlanCompiler|CheckedWritesPlan|ViewPlan' -j14 --output-on-failure` → PASS (the move changes no behaviour).
  - `ctest --test-dir build/sub-codec -R 'CheckedWritesViewPlanAuthored|CatalogGolden' --output-on-failure` → PASS.
- [ ] **Step 5: Commit.**
```bash
git add ql/include/rune/ql/Backends.h ql/src/Backends.cpp ql/CMakeLists.txt ql/tests/golden/regressorspath.rql \
        ql/tests/CheckedWritesQlPlanTest.cpp codec/include/rune/codec/ViewPlanAuthored.h codec/src/ViewPlanAuthored.cpp \
        broker/libs/view/src/PlanCompiler.cpp
git commit -m "feat(ql): prepare create view into the authored plan v2; aggregateOutputType shared from the codec; RegressorsPath golden" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (end of Phase 1).** `ctest --test-dir build/sub-ql --output-on-failure` (every ql suite); `ctest --test-dir build/sub-codec --output-on-failure`; `cmake --build build/root -j14 --target rune_view_tests rune_cli_lib && ctest --test-dir build/root -R 'View|Plan' -j14 --output-on-failure`. All PASS.

---

## Phase 2 — The broker: SD3 and the stats surface

### Task 5: Compile errors without the message search (SD3); QL plans meet the broker (QM2)

**Files:**
- Modify: `broker/libs/view/src/PlanCompiler.cpp` (every `CompileError{…}` that is still `Generic` after 07: `:115-369` today), `broker/libs/node/src/AdminWorker.cpp:1238-1245` (the compile branch of `viewDef_`)
- Modify: `broker/libs/node/tests/support/CatalogDdlRig.h` (contract addition 11), `broker/libs/node/CMakeLists.txt` (`rune_checked_writes_tests` links `rune_ql` and compiles the new test file and `ql/tests/CheckedWritesQlFuzzTest.cpp`)
- Test: `broker/libs/node/tests/node/CheckedWritesQlCompileTest.cpp` (create; `rune_checked_writes_tests`); extend 07's `broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp`

**Interfaces:**
- Consumes: `CompileErrorKind` and 07's mapping (P7-14); `ql::parse`, `ql::applyViewPlan`, `ql::viewSourceOf` (Task 4); `keyModeOf`, `ownerMappedOf`, `prefixWidthOf` (07 Task 4).
- Produces: P8-11. The AdminWorker's compile refusal becomes `reject(codeOf(compiled.error().kind), compiled.error().message)` with `Generic → kViewFieldUnresolved`; the `std::transform` / `find("key")` lines are deleted.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesAdminViewDdlRegression.NamesContainingKeyKeepTheirCodeRegression` (Review Focus 2): a view named `monkey` whose output field references an unknown source field → 34 (today 35), detail "view 'monkey': output field 'n' references unknown source field 'nope'"; a column `apiKey` that is var-len → 34 with the compiler's message; a source key field that is var-len → 35.
  - `CheckedWritesAdminViewDdlRegression.EveryCompileRefusalCarriesTheCompilerMessageRegression`: for one authored plan per pre-existing compile site (the site list of `PlanCompiler.cpp`, each provoked by its own bad plan), the admin answer's code equals the P8-11 kind's code and its detail equals the compiler's message; `CheckedWritesPlanCompile.NoSiteIsGeneric` (in `rune_view_tests`, `broker/libs/view/tests/view/CheckedWritesPlanV2Test.cpp`): the same plans compiled directly never return `CompileErrorKind::Generic`.
  - `CheckedWritesQlCompileMatrix/Cell.QlPlanMeetsTheBroker/<n>` (QM2, 50 cells) and `CheckedWritesQlCompileMatrixCount.CellCountIsPinned`.
  - `CheckedWritesQlCompile.RegressorsPathCompilesWithTheBrokerCompiler`: the four golden plans (Task 4) compiled with `compileView` against `toDescriptor` of their script topics → all OK: `user_identity` Keyed, `ownerMappedOf`, `keyWidth 66`, `prefixWidthOf 2`, `unique`; `device_history` Keyed, `!ownerMappedOf`, `keyWidth 72`, `prefixWidthOf 8`; `character_name` Keyed, `ownerMappedOf`, `keyWidth 24`, `prefixWidthOf 0`, `unicodeVersion 0x00100000`; `account_stats` LegacyU64, `keyWidth 8`, `constraints.size() == 1`, ordinal 1, the `Column` node's `outSlot` is the slot of `characters` (1).
  - `CheckedWritesQlCompile.GroupByOrderIsTheCompilersRule` (R07c): Task 4's reordered `group by` → admin code 35, detail "view 'v': group by (deviceFp, userId) must equal the key (userId, deviceFp)".
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_view_tests && ctest --test-dir build/root -R 'CheckedWritesAdminViewDdlRegression|CheckedWritesQlCompile|NoSiteIsGeneric' --output-on-failure`. Expected: FAIL (35 for `monkey`; empty details).
- [ ] **Step 3: Implement** P8-11 and the AdminWorker change.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAdminViewDdl|CheckedWritesQlCompile|CheckedWritesPlan|CreateView|PlanCompiler|AdminWorker|DdlOccupant' -j14 --output-on-failure`. Expected: PASS. An existing test that asserted 35 for a message that merely quoted a name with "key" is re-pinned and listed in the commit body as the SD3 fix.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/src/PlanCompiler.cpp broker/libs/node/src/AdminWorker.cpp broker/libs/node/tests/support/CatalogDdlRig.h \
        broker/libs/node/CMakeLists.txt broker/libs/node/tests/node/CheckedWritesQlCompileTest.cpp \
        broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp broker/libs/view/tests/view/CheckedWritesPlanV2Test.cpp
git commit -m "fix(admin): compile errors map by kind with the compiler's detail; the message search is gone (SD3); QL plans meet the broker" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The stats wire: `GetStats` (84), `StatsResponse` (85)

**Files:**
- Create: `protocol/include/rune/protocol/client/StatsMessages.h`
- Modify: `protocol/include/rune/protocol/client/Protocol.h:89-102` (`GetStats = 84`, `StatsResponse = 85`), `protocol/include/rune/protocol/client/Dispatch.h:53-71` (`isKnownTemplate` and the range comment)
- Modify: `protocol/conformance/gen_vectors.cpp`, `protocol/conformance/manifest.md`; create `protocol/conformance/vectors/get_stats.bin`, `protocol/conformance/vectors/stats_response.bin`
- Test: `protocol/tests/protocol/client/CheckedWritesStatsMessagesTest.cpp` (create; `rune_protocol_tests`, `protocol/CMakeLists.txt:22`)

**Interfaces:**
- Produces (`namespace rune::protocol::client`, little-endian, `MessageHeader` 24 bytes first):
  ```
  GetStats (84)  C→B   BODY_FIXED = 9        total 33
    [24] scope u8 (StatsScope: 1 topic partition, 2 view partition)   [25..28] objectId i32   [29..32] partition i32
  StatsResponse (85)  B→C   BODY_FIXED = 4
    [24..25] status u16   [26..27] count u16
    count × {statId u16, value i64}                     (10 B each, statId strictly ascending)
    [nConstraints u16]  nConstraints × {constraintId u32, rejections u64}   (12 B each)
  ```
  `struct GetStats { int64_t sessionId, correlationId; StatsScope scope; int32_t objectId; int32_t partition; … }` and `struct StatsResponse { int64_t sessionId, correlationId; uint16_t status; uint16_t count; std::span<const uint8_t> statBytes; uint16_t constraintCount; std::span<const uint8_t> constraintBytes; template <class Fn> void forEachStat(Fn&&) const noexcept; template <class Fn> void forEachConstraint(Fn&&) const noexcept; }`, each with `TEMPLATE_ID`, `BODY_FIXED`, `encodedSize()`, `encode(uint8_t*)`, `static std::optional<T> decode(const uint8_t*, std::size_t)` (zero-copy spans). Statuses `kOk = 0`, `kUnknownObject = 4`, `kInvalidRequest = 5`, `kBusy = 6`, `kNotAuthorized = 45`. Constants: `kMaxStats = 64`, `kMaxConstraintEntries = 256` (= `VerdictSourceStats::perConstraint` capacity, 04), `kMaxEncodedSize = 24 + 4 + 64 * 10 + 2 + 256 * 12` (3,742), `static_assert(kMaxEncodedSize <= 68 * 1024)`. Decoders refuse a scope other than 1 or 2, `count > kMaxStats`, ids not strictly ascending, `nConstraints > 256`, any truncation and trailing bytes.
  - `enum class StatId : uint16_t` with C16's 25 ids and names verbatim, and `statName`.
- Only a v12 client sends 84; the gateway answers a v11 session `kInvalidRequest` (Task 7).

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesStatsMessages.GetStatsRoundTrip`: both scopes, objectId 7, partition 3 → `encodedSize() == 33`, decode equals.
  - `CheckedWritesStatsMessages.ResponseRoundTrip`: 10 stats (ids 1–10) and 2 constraints → `encodedSize() == 24 + 4 + 100 + 2 + 24`; `forEachStat` yields them in order; an empty response (`count 0`, `nConstraints 0`) is 30 bytes.
  - `CheckedWritesStatsMessagesRegression.BoundsAreRefusedRegression`: scope 0 and 3; `count 65`; ids `5, 5` and `6, 5`; `nConstraints 257`; every truncation; one trailing byte → nullopt.
  - `CheckedWritesStatsMessages.IdsAndNamesArePinned`: `StatId::verdictLag == 4`, `autoIncrementIdsBurned == 10`, `txnMarkerWithoutVote == 20`, `uniqueConflicts == 32`, `transientSourceResets == 36`; `statName(StatId::scanPages) == "scanPages"`; `statName(StatId{99}) == "unknown"`.
  - `CheckedWritesStatsMessages.KnownTemplates`: `isKnownTemplate(84)`, `(85)` true; `(86)` false.
  - `CheckedWritesStatsMessages.ConformanceVectors`: the two `.bin` files decode to the manifest's values and re-encode byte for byte.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `StatsMessages.h: No such file or directory`.
- [ ] **Step 3: Implement** the header; regenerate the vectors with `gen_vectors` as the manifest's "Regenerate" note says and add their entries.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesStatsMessages|Conformance|Dispatch' --output-on-failure` and `ctest --test-dir build/sub-client-cpp -R Conformance --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add protocol/include/rune/protocol/client/StatsMessages.h protocol/include/rune/protocol/client/Protocol.h \
        protocol/include/rune/protocol/client/Dispatch.h protocol/conformance/gen_vectors.cpp protocol/conformance/manifest.md \
        protocol/conformance/vectors/get_stats.bin protocol/conformance/vectors/stats_response.bin \
        protocol/tests/protocol/client/CheckedWritesStatsMessagesTest.cpp protocol/CMakeLists.txt
git commit -m "feat(protocol): GetStats 84 and StatsResponse 85 with the C16 stat ids, vectors" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Serving `GetStats`: the worker op, the collectors, the gateway (QM4)

**Files:**
- Modify: `protocol/schema/rune-internal-ring.xml` (`OpType` `GET_STATS = 31` after 07's 30; `ReturnType` `STATS_RESULT = 10` after 07's 9) and the regenerated `protocol/include/rune_protocol_internal_sbe/*`
- Modify: `broker/libs/node/include/rune/node/WorkerOp.h` (`Type` `:66-158`: `GetStats = 31`; return `Type` `:528-537`: `StatsResult = 10`), `broker/libs/node/include/rune/node/WorkerReturnCodec.h` (the new return type; `ProtocolRegression.WorkerReturnLayoutUnchanged` stays green), `broker/libs/node/include/rune/node/Worker.h` (op dispatch `:2568-2569`; `executeGetStats_` beside `executeGetViewRowWire_` `:3986`; `std::array<uint8_t, StatsResponse::kMaxEncodedSize> statsScratch_`; the chunk gather of `enqueueViewRowResult_` `:4169-4189`; counter `getStats_` beside `getViewRows_` `:5857`)
- Create: `broker/libs/node/include/rune/node/StatsCollect.h`
- Modify: `broker/libs/gateway/include/rune/gateway/ClientGateway.h` (`:1858` declarations, `:1299` test hooks, `:2490` `partitionLedElsewhere_`), `broker/libs/gateway/src/ClientGateway.cpp` (dispatch `:1384-1386`; return encoding beside `ViewRowResult` `:6850-6889`; `handleGetStats` beside `handleGetViewRow` `:9259-9337`; `viewSourceLedElsewhere_` `:5639-5651` calls `partitionLedElsewhere_`), `broker/libs/gateway/include/rune/gateway/RequestRejection.h:165-169`
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (`getStats`, contract addition 11), `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`
- Test: `broker/libs/node/tests/node/CheckedWritesStatsTest.cpp`, `broker/libs/node/tests/node/CheckedWritesStatsGatewayTest.cpp` (create; `rune_checked_writes_tests`)

**Interfaces:**
- Consumes: `PartitionWriteState` (C7: `verdicts`, `rejected`, `autoInc`, `autoIncStats`, `spec`), `VerdictSourceStats` (04 + 05's fields), `VerdictSource::judgedThrough()`, `RejectedOffsetIndex::watermark()` / `countAtOrAfter` (04 P4-16), `AutoIncState::nextN/reservedThroughN/partitions/partition` (06), `WorkerViewSet::foldCounters` (`WorkerViewSet.h:503`; 07's `scanRows`, `scanPages`), `writeOf_` (C7), `findOwnedReplica_`, `viewSet_->viewSourceTopic`.
- Produces: contract additions 5–7, 11 and P8-10. Values:
  - 1 `rejectionsKeyExists`, 2 `rejectionsKeyNotFound`, 3 `rejectionsConstraint`, 6 `verdictMarkersWritten` (`markersWritten`): from `state->verdicts->stats()`, present iff `state->verdicts != nullptr`; 4 `verdictLag` = `replica.logEndOffset() − verdicts->judgedThrough()`; 5 `verdictWatermark` = `state->rejected.watermark()`; 7 `rejectedNotCompacted` = 04's P4-16 expression; the constraint list = `perConstraint[0 .. perConstraintCount)`.
  - 8 `autoIncrementCounter` = `nextN·P + p`, 9 `autoIncrementReservedThrough` = `R·P + p` (0 when R = 0), 10 `autoIncrementIdsBurned` = `autoIncStats.idsBurned`: present iff `state->spec->autoIncrement` and the counter is `Ready` (06 P6-7; a partition still scanning omits 8–9 and keeps 10).
  - 33 `scanRows`, 34 `scanPages`, 35 `transientRecordsEvicted`, 36 `transientSourceResets` from `ViewFoldCounters`.
- **Worker** `executeGetStats_(sbe::WorkerOp&)` (cold op; no allocation): scope 1 → `findOwnedReplica_(topicId, partition)`; none, not `Leader`, or the read lease expired (the `serveViewRead_` check 2c, `Worker.h:4092-4097`) → `errorCode = kViewRowNotLeaderInternal`; else collect. Scope 2 → `viewOutputFingerprint` fails (after the one `drainInstalls()` as `serveViewRead_` does) → `kUnknownObject`; the source replica not leading or the read lease expired (`serveViewRead_` checks 2b–2c, `Worker.h:4074-4097`) → `kViewRowNotLeaderInternal`; else collect. Return `StatsResult` with `errorCode = status`, `recordCount = count`, `aux = constraintCount` and the chunk `[count × 10 B][nConstraints u16][n × 12 B]`.
- **Gateway** `handleGetStats`, in this order, each answered with a status-only `StatsResponse` unless noted: decode fails → `kInvalidRequest`; unknown session → `noteUnanswerable_`; session client version < `kClientVersionCheckedWrites` → `kInvalidRequest`; `DESCRIBE` on the topic (scope 1, `ResourceType::Topic`) or the view (scope 2, `ResourceType::View`) via `authorizeTopicIdHere_` → `kNotAuthorized`; scope 2 `viewRowGate_` → its code; an unknown topic id (`topicRegistry_` has no entry) → `kUnknownObject`; `partition` outside `[0, P)` → `kInvalidRequest`; leader elsewhere (`partitionLedElsewhere_` / `viewSourceLedElsewhere_`) → `sendRouteRefusal_` (Redirect or bare `NotLeader`); route scope 1 by `(objectId, partition)`, scope 2 by `(viewSourceOf_(objectId), partition)`; ring full → `kBusy`. `StatsResult` → `StatsResponse` (the not-leader sentinel → `sendRouteRefusal_`, as `:6861-6866`).

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesStatsTest.cpp` (04's `CheckedWritesRig`, real `Worker`):
    - `CheckedWritesStats/Cell.ServesExactlyTheCountersThatExist/<n>` (QM4, 14 cells) and `CheckedWritesStatsCount.CellCountIsPinned`.
    - `CheckedWritesStats.KeyIndexCountersAfterRejections`: `createKeyIndex()`; insert id 5 twice, `update` id 9 (all `All | View`), `driveUntilJudged` → ids `{1: 1, 2: 1, 3: 0, 4: 0, 5: W, 6: markers, 7: 2}` with W and markers equal to `rig.worker().verdictStatsForTest(1, 0)` and `rejected.watermark()`.
    - `CheckedWritesStats.AutoIncrementCountersAfterCreates`: `createAutoIncTopic({.partitions = 1})`; three key-0 creates → ids 8 = 4, 9 = 4096, 10 = 0; after `failover()` and its scan → 8 = 4097, 10 = 4093 (06's burned gap).
    - `CheckedWritesStats.ViewCounters`: 07's `account_characters` through `createView(planV2)`; two full scans of 12 rows with pages of 5 (`rig.scanAll`) → 33 = 24 and 34 = 6, equal to `foldCounters(viewTopicId, 0)->scanRows` / `scanPages`; 35 and 36 present and 0.
    - `CheckedWritesStatsRegression.FollowerNeverAnswersStaleCountersRegression`: `demote()` the rig's replica → the sentinel, never an `OK` with the old values.
  - `CheckedWritesStatsGatewayTest.cpp` (a real `ClientGateway` + `WorkerPool`, `applyGetStatsForTest`, modeled on 07's `CheckedWritesViewReadGatewayTest.cpp`):
    - `CheckedWritesStatsGateway.RoutesToTheOwner`: scope 1 lands on `(topic, p)`'s worker; scope 2 on the source's.
    - `CheckedWritesStatsGateway.RefusalsInOrder`: v11 session → 5; no `DESCRIBE` → 45; unknown topic → 4; partition 4 of 4 → 5; another node leads → an `ErrorResponse` Redirect with that endpoint; a full ring → 6.
    - `CheckedWritesStatsGateway.EncodesTheResponse`: a `StatsResult` return → one `StatsResponse` whose `forEachStat` / `forEachConstraint` match.
  - `CheckedWritesAllocRegression.StatsServingAllocatesNothingRegression` (03's binary): warm up, then under `rune::testing::AllocWindow` 1,000 `GetStats` of each scope through the worker and the gateway → `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'GetStats' in 'rune::node::WorkerOp::Type'`.
- [ ] **Step 3: Implement.** Regenerate the SBE flyweights (`java -jar "$SBE_JAR" …` as the header of `rune-internal-ring.xml` documents), then the worker, collectors and gateway per the Interfaces.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R 'CheckedWritesStats|CheckedWritesAllocRegression|GetViewRow|ViewSink|ProtocolRegression|WorkerReturn|RequestRejection' -j14 --output-on-failure`. Expected: PASS (`WorkerReturnLayoutUnchanged` unchanged).
- [ ] **Step 5: Commit.**
```bash
git add protocol/schema/rune-internal-ring.xml protocol/include/rune_protocol_internal_sbe broker/libs/node/include/rune/node/WorkerOp.h \
        broker/libs/node/include/rune/node/WorkerReturnCodec.h broker/libs/node/include/rune/node/Worker.h \
        broker/libs/node/include/rune/node/StatsCollect.h broker/libs/gateway/include/rune/gateway/ClientGateway.h \
        broker/libs/gateway/src/ClientGateway.cpp broker/libs/gateway/include/rune/gateway/RequestRejection.h \
        broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesStatsTest.cpp \
        broker/libs/node/tests/node/CheckedWritesStatsGatewayTest.cpp broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp \
        broker/libs/node/CMakeLists.txt
git commit -m "feat(stats): GetStats served by the leader's worker on the GetViewRow route - every C16 counter that exists, no allocation" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (end of Phase 2).** `ctest --test-dir build/root -R 'CheckedWrites|View|Plan|AdminWorker|CreateView|Conformance' -j14 --output-on-failure`; `cmake --build build/asan -j14 --target rune_checked_writes_tests rune_protocol_tests && ctest --test-dir build/asan -R 'CheckedWritesStats|CheckedWritesQlCompile|CheckedWritesAdminViewDdl' --output-on-failure`; the ql fuzz tests under ASan: `cmake --build build/asan -j14 --target rune_checked_writes_tests && ctest --test-dir build/asan -R CheckedWritesQlFuzz --output-on-failure`. All PASS. Start the Task 11 baseline worktree build now (lean-bench rule).

---

## Phase 3 — The client, the CLI, the e2e and the docs

### Task 8: The client: `getStats`

**Files:**
- Modify: `client-cpp/include/rune/client/Completion.h` (`OpKind::GetStats` after 07's `ScanViewRows`), `client-cpp/include/rune/client/Completions.h:155-160` (`GetStats` reserves `StatsResponse::kMaxEncodedSize`), `client-cpp/include/rune/client/Client.h` (`StatsResult` beside `ViewRowResult` `:209`; the methods beside `getViewRow` `:820` and `getViewRowAndWait` `:941`), `client-cpp/src/Client.cpp` (beside `getViewRow` `:1579-1591`; `failDdl_` `:1898-1906` maps the routing codes for `GetStats` like `GetViewRow`; the response dispatch beside `ViewRowResponse` `:4334-4356`)
- Test: `client-cpp/tests/client/CheckedWritesClientStatsTest.cpp` (create; `rune_schema_codec_smoke_tests`, `client-cpp/CMakeLists.txt:1073-1081`)

**Interfaces:**
- Consumes: Task 6's messages; 03's `OpStatus::BrokerTooOld` and the seed `brokerVersion`.
- Produces: contract addition 8. The completion carries `errorCode = status` and `payload` = the response bytes after `count` (`[count × 10][nConstraints][n × 12]`) with `value = count`; `decodeStatsPayload` parses that payload. Redirect / `NotLeader` are followed exactly like `getViewRow` (within `min(pingTimeoutMs, ddlRetryBudget)`, then `kNotLeader`). SD11: `brokerVersion` known and below 7 → `OpStatus::BrokerTooOld`, nothing sent.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesClientStats.DecodePayload`: 3 stats and 1 constraint → `StatsResult` with `get(StatId::verdictLag) == 7`, `get(StatId::scanRows) == std::nullopt`; a truncated payload → false.
  - `CheckedWritesClientStats.BrokerTooOldIsLocal`: a client whose seed reported `brokerVersion 6` → `getStats` completes `OpStatus::BrokerTooOld` with no frame written (the client's test transport records zero sends).
  - `CheckedWritesClientStats.FrameIsTheWireMessage`: the frame `getStats(ViewPartition, 12, 0, …)` writes decodes as `GetStats{scope 2, objectId 12, partition 0}`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_schema_codec_smoke_tests`. Expected: `no member named 'GetStats' in 'rune::client::OpKind'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesClientStats|Conformance|ViewPlanAuthor' --output-on-failure` and `cmake --build build/root -j14 --target rune_client_broker_tests`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add client-cpp/include/rune/client/Completion.h client-cpp/include/rune/client/Completions.h client-cpp/include/rune/client/Client.h \
        client-cpp/src/Client.cpp client-cpp/tests/client/CheckedWritesClientStatsTest.cpp client-cpp/CMakeLists.txt
git commit -m "feat(client): getStats / getStatsAndWait over GetStats 84, BrokerTooOld below broker version 7" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: The CLI: views, `select`, `show`, `rune stats` (QM3)

**Files:**
- Modify: `client-cpp/src/cli/QlEngine.h` (`Prepared` `:59-90`, `PrepareOptions` `:98-101`, `prepare` `:105`, `QlRun` `:177-220`), `client-cpp/src/cli/QlEngine.cpp` (`prepare` `:227-331`, `printPlan` `:333-383`, `run(PDefine)` `:624-644` passes `topic.keyFeatures = d.keyFeatures`, new `run` overloads), `client-cpp/src/cli/CatalogCommands.cpp` (`formatTopicDefinition` `:152-176`, `formatViewDefinition` `:178-189`, `printCatalogUsage` `:203-220`, `cmdQl` `:240-367` passes a `PrepareScope`, new `cmdStats`), `client-cpp/include/rune/cli/CatalogCommands.h`, `client-cpp/src/cli/Console.cpp:185-215` (the batch prepare passes a `PrepareScope`), `client-cpp/src/cli/Cli.cpp:449-479` (`stats` command; `printUsage`)
- Create: `client-cpp/tests/CheckedWritesQlCliTest.cpp` (add to `rune_cli_catalog_tests`, `client-cpp/CMakeLists.txt:1759-1765`), `client-cpp/tests/golden/regressorspath.dryrun.txt`; modify `client-cpp/tests/RuneCliCatalogTest.cpp`, `client-cpp/tests/RuneCliOfflineTest.cpp`
- Modify: `client-cpp/CMakeLists.txt` (`rune_cli_catalog_tests` gets `RUNE_QL_GOLDEN_DIR` pointing at `ql/tests/golden` and `RUNE_CLI_GOLDEN_DIR` at `client-cpp/tests/golden`)

**Interfaces:**
- Consumes: Tasks 1–4 (`applyViewPlan`, `viewSourceOf`, `predicateText`), Task 8, 07's `ViewKeyBuilder::fromPlan` / `setInt` / `setUInt` / `setBool` / `setString` / `key()` / `prefix()` / `partition()`, `getViewRowAndWait(int32_t, std::span<const uint8_t>, ViewReadOptions, timeout)`, `scanViewAndWait`, `ViewScanResult`, `rune::client::view::buildRowLayout` / `readSlot` (`ViewRowCodec.h:69-150`), `rune::codec::lowerUtf8`, `maxLoweredWidth`.
- Produces (`QlEngine.h`, namespace `rune::cli::engine`):
  ```cpp
  struct PrepareScope { std::vector<ql::TopicDef> topics; };   // the topics prepared so far in this run (P8-7)
  struct PCreateView { ql::CreateViewDef def; std::optional<rune::view::ViewPlan> scriptPlan; std::string sourceText; };
  struct PSelect     { ql::SelectDef def; };
  struct PShowTopic  { std::string name; };
  struct PShowView   { std::string name; };
  // Prepared appends PCreateView, PSelect, PShowTopic, PShowView.
  [[nodiscard]] std::optional<Prepared> prepare(const ql::Statement&, const PrepareOptions&, PrepareScope&, std::string& why);
  // the old three-argument prepare forwards with a local scope.
  struct SelectRead { enum class Kind : uint8_t { Point, Scan } kind = Kind::Point; std::vector<uint8_t> keyOrPrefix;
                      int32_t partition = -1; std::vector<uint16_t> slots; /* output slots to print, in order */ uint32_t limit = 0; };
  [[nodiscard]] std::expected<SelectRead, std::string> planSelect(const ql::SelectDef&, const rune::view::ViewPlan& authored,
      const rune::codec::Descriptor& source, const rune::codec::Descriptor& output, int32_t sourcePartitions,
      rune::protocol::Partitioner partitioner);
  [[nodiscard]] std::string formatRows(const rune::codec::Descriptor& output, std::span<const uint16_t> slots,
                                       std::span<const std::vector<uint8_t>> rows);
  ```
  `CatalogCommands.h`: `std::string formatStatsText(std::string_view objectKind, std::string_view name, std::span<const PartitionStats>)`, `std::string formatStatsJson(…)` with `struct PartitionStats { int32_t partition; rune::client::StatsResult result; std::vector<std::string> constraintLabels; };`, and `int cmdStats(const std::vector<std::string>&, std::ostream&, std::ostream&)`.
- **`printPlan`** for `PCreateView` prints (exact, one plan line per view): `create [unique ][transient ]view <name> source <source> key <keyspec>[ group by (<cols>)][ unicode 16.0.0]`, then `  output <name> <qltype> <rule>(<srcField>)` joined by `, ` (`count(*)` without a field, `replace(<c>)` for projections), then one `  constraint <name> check (<predicateText>)` per constraint, then `  plan authored v<1|2>`; with no `scriptPlan` it prints `  (source resolved when the statement runs)` after the first line. `<keyspec>` is the statement's own (`(provider, subject)`, `lower(name) as nameKey`, `userId`). `PDefine`'s line appends ` autoincrement` / ` key.index` / ` autoincrement.reserve <n>` / ` autoincrement.max <n>` after the key for non-default features.
- **`QlRun::run(PCreateView)`**: `awaitLastDdl()`; refresh the catalog reader; the source must exist (`topicByName`), else "no topic '<s>'"; an untyped source → "view source must be a typed topic"; `getTopicSchemaAndWait(source)` → descriptor and `topicId`; `applyViewPlan`; then P8-8; the console line is `ok view <name>` and `rune ql` prints `created view '<name>' viewTopicId=<id>` (`altered view …` for replace). A broker code goes through `failBroker` (so 58 prints as `ViewUniqueInvalid (58) unique views are not available yet`).
- **`QlRun::run(PSelect)`**: `awaitLastDdl()`; the view from the catalog (`viewByName`), its `normalizedPlanBlob` → `deserializeAuthored`, `outputDescriptorBlob`, its source's descriptor, partition count and `keyFeatures.partitioner`; `planSelect`; a point read → `getViewRowAndWait(viewTopicId, key, {.partition = …})` → `formatRows` of one row, or `(no row)` on `kAbsentKey`; a scan → `scanViewAndWait` page by page (`limit` 0 = 256 per page) until `complete()` or `limit` rows, then `formatRows`; any other status → `fail("<status name>")` (`kCursorInvalidated` → "the scan was invalidated by a concurrent change; run it again").
- **`formatStatsText`**: per partition a line `  partition <p>`, then `    <statName> <value>` per stat in id order, then `    constraint <label> <rejections>` per constraint; a partition with none prints `    (no counters)`.
- **`formatRows`**: a header of the selected column names, then one line per row, columns separated by two spaces and left-aligned to the widest cell; integers decimal, `Bool` `true`/`false`, `Float64` shortest round-trip, `FixedStr` the bytes up to the first NUL, a NULL slot `NULL`; a final `(<n> rows)` line.
- **`run(PShowTopic)`**: `formatTopicDefinition(t)` (02's `kind` line, then — new, typed topics only — `  key.index on|off`, `  autoincrement on (reserve <r>, max <m>)` or `  autoincrement off`, `  partitioner fnv1a|modulo` from `schema->keyFeatures`), then for each partition `getStatsAndWait(TopicPartition, topicId, p)` and `formatStatsText`. **`run(PShowView)`**: `formatViewDefinition(v)` gains, after its existing lines, `  keyspec <spec>`, `  unique yes|no`, `  group by (<cols>)` for an aggregate (its `groupBy` outNames, or the key column for a v1-shaped aggregate), `  constraint <name> check (<pred>)` per constraint and `  unicode <M.m.u>` when set (all from `normalizedPlanBlob`); then per sink partition the scope 2 stats, plus the per-constraint rejections from the source partition's scope 1 response whose `constraintId >> 16` equals the view's `objectId`, labelled `<view>.<constraint>` (ordinal 0 = `unique`).
- **`rune stats (topic | view) <name> [--partition N] [--json] --broker host:port`**: text = `formatStatsText`, `--json` = one object `{"topic": "<name>", "partitions": [{"partition": 0, "stats": {"<statName>": <value>, …}, "constraints": [{"constraintId": <id>, "label": "<view>.<constraint>", "rejections": <n>}]}]}` (`"view"` for a view), keys in id order, no trailing whitespace. Exit 0, 1 on a failure, 2 on usage.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesQlCliTest.cpp`):
  - `CheckedWritesQlCli.RegressorsPathDryRunGolden`: `rune::cli::run({"ql", "--dry-run", "-f", <RUNE_QL_GOLDEN_DIR>/regressorspath.rql})` → exit 0 and stdout byte-equal to `client-cpp/tests/golden/regressorspath.dryrun.txt`. The golden is written by running the command once, reviewed line by line against this task's `printPlan` rules and Task 4's plan values (the four `create … view` lines are: `create unique view user_identity source user key (provider, subject)`, `create view device_history source device_login key (userId, deviceFp) group by (userId, deviceFp)`, `create unique view character_name source character key lower(name) as nameKey unicode 16.0.0`, `create view account_stats source character key userId group by (userId)`; `account_stats` prints `  constraint max_characters check (characters <= 12)`; `user_identity`'s output line is `  output provider uint16 replace(provider), subject string(64) replace(subject), userId uint64 replace(userId)`; every view prints `  plan authored v2`; the `user` topic line ends `key userId autoincrement`), then committed.
  - `CheckedWritesQlCli.BadViewRefusesTheScriptBeforeAnythingIsSent` (Review Focus 4): a script `topic s { id uint64, n int64 } key id; create view v key id as select nope from s;` with `--broker` set to an unreachable endpoint → exit 1 and stderr `statement 2: column 'nope' is not a field of 's'` with no connection attempt (the error precedes the connect line).
  - `CheckedWritesQlCli.UnresolvedSourcePrintsRunTime`: `rune ql --dry-run -e "create view v key id as select n from elsewhere;"` → `create view v source elsewhere key id\n  (source resolved when the statement runs)\n`.
  - `CheckedWritesQlSelectMatrix/Cell.PlansTheRead/<n>` (QM3, 20 cells) and `CheckedWritesQlSelectMatrixCount.CellCountIsPinned`.
  - `CheckedWritesQlSelect.LowerLiteralEqualsTheBrokerKey` (§15, the QL half, R07f): `character_name`'s plan and the `character` descriptor: `select characterId from character_name where nameKey = lower('Foo');` → Point, `keyOrPrefix` = `66 6F 6F` + 21 × `00` (24 bytes), equal to `rune::codec::encodeKeyComponent(PrimType::FixedStr, kKeyFunctionLower, FieldValue{"Foo"}, out24)` and to `ViewKeyBuilder::fromPlan(…).setString("name", "Foo").key()`; `where nameKey = 'FOO'` gives the same bytes. On `(lower(name) as nameKey, characterId)`, the same `where` is a Scan whose prefix is those 24 bytes. On a plain `string(16)` key column `tag`, `where tag = lower('ABC')` encodes `61 62 63` + 13 zeros.
  - `CheckedWritesQlSelectRegression.LiteralEdgesAreRefusedNeverTruncatedRegression` (Review Focus 3): on `(provider uint16, …)`: `-1` and `70000` → "70000 does not fit uint16 column 'provider'"; on `uint64`, `18446744073709551615` passes and `-1` fails; on `int64`, `-9223372036854775808` passes; a 17-byte `'…'` on `string(16)` → "'…' is 17 bytes; column 'name' is string(16)"; `lower('ȺȺȺȺȺȺȺȺ')` (16 bytes, lowers to 24) on a plain `string(16)` column → "lower('…') is 24 bytes; column 'tag' is string(16)"; `true` on an integer column → does not fit.
  - `CheckedWritesQlCli.FormatRows`: two rows of `(userId uint64, name string(16), score float64, alive bool)` with a NULL score → the exact text `userId  name  score  alive\n42      Foo   1.5    true\n43      Bar   NULL   false\n(2 rows)\n` (columns padded to the widest cell).
  - `CheckedWritesQlCli.ShowTopicFormatsFeatures` (`RuneCliCatalogTest.cpp`): a `TopicObject` with `keyFeatures {keyIndex, autoIncrement, Modulo, 2^53−1, 4096}` → contains `"  key.index on\n  autoincrement on (reserve 4096, max 9007199254740991)\n  partitioner modulo\n"`; a default typed topic → `key.index off`, `autoincrement off`, `partitioner fnv1a`; an untyped topic → none of the three.
  - `CheckedWritesQlCli.ShowViewFormatsConstraints`: the `account_stats` `ViewObject` (its normalized plan from Task 4) → ends with `"  keyspec userId\n  unique no\n  group by (userId)\n  constraint max_characters check (characters <= 12)\n"` (a v1-shaped aggregate prints its key column as the `group by`).
  - `CheckedWritesQlCli.StatsJson`: two partitions of a key-index topic → the exact JSON text (ids 1–7 by name, one constraint labelled `account_stats.max_characters`).
  - `RuneCliOfflineTest.cpp`: `CheckedWritesCli.StatsUsage`: `rune stats` → exit 2 and the usage; `rune stats topic t` without `--broker` → exit 2 "--broker host:port is required".
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_cli_catalog_tests rune_cli_offline_tests`. Expected: `no member named 'PCreateView'` / `no type named 'PrepareScope'`.
- [ ] **Step 3: Implement** per the Interfaces; generate, review and commit the dry-run golden as Step 1 describes.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesQl|CheckedWritesCli|RuneCli|RuneConsole' --output-on-failure`. Expected: PASS. Expected-text edits in existing `RuneCliCatalogTest` cases (the three new topic lines, the appended view lines) are the deliberate output change; list them in the commit body.
- [ ] **Step 5: Commit.**
```bash
git add client-cpp/src/cli client-cpp/include/rune/cli/CatalogCommands.h client-cpp/tests/CheckedWritesQlCliTest.cpp \
        client-cpp/tests/golden/regressorspath.dryrun.txt client-cpp/tests/RuneCliCatalogTest.cpp \
        client-cpp/tests/RuneCliOfflineTest.cpp client-cpp/CMakeLists.txt
git commit -m "feat(cli): create view from QL (plan v2), select decoded by the output descriptor, show topic/view with stats, rune stats --json" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: The real e2e

**Files:**
- Create: `broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp`; modify `broker/libs/node/CMakeLists.txt:1731-1751` (add the file to `rune_client_broker_tests`; `target_compile_definitions(rune_client_broker_tests PRIVATE RUNE_QL_GOLDEN_DIR="${CMAKE_SOURCE_DIR}/ql/tests/golden")`)

**Interfaces:**
- Consumes: everything above; `rune::cli::run` (as `RuneCliBrokerRegressionTest.cpp:49-53` calls it), `OneBroker(tag, Runtime::Shard)` (`tests/client/OneBroker.h:51-70`), `ThreeNodes` (`tests/support/CatalogBrokerCluster.h:160`), 05's typed `ki` fixture and `insertAndWait` / `TypedTopic` from `CheckedWritesBrokerRegressionTest.cpp`. No new production code. Every wait is bounded (`waitFor(…, 10s)`).

- [ ] **Step 1: Write the tests** (`rune_client_broker_tests`):
  - `CheckedWritesQlBrokerRegression.RegressorsPathOutcomesBeforePart09ThreeNodesRegression` (the acceptance case; `ThreeNodes::start("cw08")`, because `user` and `character` ask for `replicas 3`): run each statement of `regressorspath.rql` (split with `splitStatements`) with `rune ql -e … --broker <node 0>` in order; exit codes and stderr per statement: `user` 0, `user_identity` 1 with `ViewUniqueInvalid (58) unique views are not available yet`, `device_login` 0, `device_history` 0, `character` 0, `character_name` 1 with 58, `account_stats` 1 with `ViewConstraintInvalid (57) check constraints are not available yet`. **Part 09 flips the 58 / 57 rows to exit 0 and adds a whole-script `-f` run.** Then `rune ql -e "show topic user;"` contains `partitions 1`, `key.index on`, `autoincrement on (reserve 4096, max 9007199254740991)` and `partitioner modulo` (06 P6-1's normalization), and `autoIncrementCounter 1`; `show topic character;` contains `partitions 1` (R06f); `show view device_history;` contains `keyspec (userId, deviceFp)` and `group by (userId, deviceFp)`.
  - `CheckedWritesQlBrokerRegression.DeviceHistoryScanShardRegression` (`OneBroker`, Shard): the `device_login` and `device_history` statements with `replicas` omitted; write `(userId 7, deviceFp 'a', platform 1, ts 100)` then `(7, 'b', 2, 200)` and `(8, 'a', 1, 300)` with `All | View`; `rune ql -e "select * from device_history where userId = 7;"` → exactly one row with the values `7, b, 200, 200, 1, 2` and `(1 rows)` — the source key `userId` identifies a login, so a user's later login replaces the earlier one's contribution (spec §9a; flagged as a gap of the acceptance case, see Self-review).
  - `CheckedWritesQlBrokerRegression.LowerLiteralReadsTheRowShardRegression` (§15, R07f): `character` (no `replicas`) and `create view name_index key (lower(name) as nameKey, characterId) as select userId from character;`; insert `name 'Foo'` with `insertAndWait` (`All | View`) → `rune ql -e "select userId, nameKey from name_index where nameKey = lower('FOO');"` prints the row with `nameKey` `foo`.
  - `CheckedWritesQlBrokerRegression.PointAndPrefixSelectShardRegression`: `account_characters` and `character_by_id` from spec §1 created through QL; three characters of user 42 → `select * from account_characters where userId = 42;` prints 3 rows; `select name from character_by_id where characterId = <id>;` prints one; an absent id prints `(no row)`; `… limit 2` prints 2 rows.
  - `CheckedWritesQlBrokerRegression.ScriptCreatesTopicThenViewShardRegression` (Review Focus 4): one `rune ql -f` script `topic s { id uint64, n int64 } key id; create view v key id as select n from s;` → exit 0; the view's catalog `sources` names `s`.
  - `CheckedWritesQlBrokerRegression.KeylessTypedTopicFromQlShardRegression` (R02d, R10b): `topic k { a uint64, b int64 };` → exit 0, `show topic k;` has `kind typed` and `keyField -`; `create view kv key a as select b from k;` → exit 1 with `ViewKeyInvalid (35) view source needs a key`; `topic k2 { a uint64 } with (key.index = on);` → exit 1 with `InvalidRequest (39) key.index needs a key`.
  - `CheckedWritesQlBrokerRegression.CreateOrReplaceSwapsThePlanShardRegression`: `create or replace view v …` twice with a different select list → `created view` then `altered view`, and the catalog version goes 1 → 2.
  - `CheckedWritesQlBrokerRegression.StatsJsonOverAKeyIndexTopicShardRegression`: `topic ki { id uint64, name string(16), n int64 } key id with (key.index = on, cleanup.policy = compact);` through QL; 05's `TypedTopic<Ki>` binds; insert id 1 twice → the second `WriteResult::code() == 52`; `rune stats topic ki --json` has `"rejectionsKeyExists": 1` and `"verdictLag": 0` after the fold; `rune stats view nope` → exit 1 "no view 'nope'".
  - `CheckedWritesQlBrokerRegression.StatsFromAFollowerRedirectsThreeNodesRegression`: `ThreeNodes`; `rune stats topic ki --broker <a follower>` → exit 0 with the leader's values (the client followed the Redirect).
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesQlBrokerRegression' --output-on-failure`. Expected: PASS. Then `ctest --test-dir build/root -R 'RuneCliBroker|RuneConsoleBroker|ClientDdlBroker' -j6 --output-on-failure`. Expected: PASS.
- [ ] **Step 3: Commit.**
```bash
git add broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "test(ql): real-broker e2e - the RegressorsPath DDL (unique/check refused until Part 09), select, show, stats, lower() literals" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: The benches and the docs

**Files:**
- Create: `broker/benchmark/QlPlanBench.cpp`; modify `broker/CMakeLists.txt:499-558` (the file in `rune_broker_benchmark`; link `rune_ql`), `broker/benchmark/GetViewRowServeBench.cpp` (after `:220`)
- Modify: `docs/api/ql.md`, `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `docs/api/admin-cli.md`

**Interfaces:**
- Consumes: Tasks 1–9.

- [ ] **Step 1: Write the benches** (`benchmark::RegisterBenchmark`, `BenchGate.h` where the gate is absolute):
  - `BM_QlTextToPlan/{projection,composite,lower,aggregate_check}`: `ql::parse` + `applyViewPlan` (a pre-built `ViewSource`) + `serializeAuthored` of `character_by_id`, `account_characters`, `character_name`, `account_stats` — "QL text → wire", gate **≤ 5 µs p50**, reporting `ns_per_stmt`.
  - `BM_QlWireToPlan/{projection,composite,lower,aggregate_check}`: `deserializeAuthored` + `compileView` of the same blobs — "wire → plan", gate **≤ 5 µs p50**.
  - `BM_GetStatsServe/{topic,view}`: `BM_GetViewRowServe`'s worker loop with `GetStats` ops, reported.
- [ ] **Step 2: Run the benches** (`build/rel`; baseline = this part's base commit on `main`, ABAB n=3, one session, same box; one prepare/restore):
  - `git worktree add "$TMPDIR/rune-cw08-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel` (started at Checkpoint B).
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_GetViewRowServe|BM_QlTextToPlan|BM_QlWireToPlan|BM_GetStatsServe' --benchmark_repetitions=3` (the new cases exist only on the tip; the baseline runs `BM_GetViewRowServe`).
  - **Gates:** `BM_GetViewRowServe` no regression (the op dispatch gained one cold case); `BM_QlTextToPlan/*` and `BM_QlWireToPlan/*` ≤ 5 µs (the repo's QL gate, root `CLAUDE.md` §2); `BM_GetStatsServe/*` reported. Borderline numbers on the dev Mac are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under the VM lock on `$RUNE_VM_HOST`, then `broker/bench/vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 3: Write the docs:**
  - **`docs/api/ql.md`:** delete "What Rune QL does not have yet" (`:7-10`); the topic grammar gains `[ key <field> ]` optional on typed topics (keyless typed topics are append-only typed streams with no database features), `<field> <type> autoincrement`, and the three settings rows (`key.index`, `autoincrement.reserve`, `autoincrement.max`: creation-only); a **Views** section (the `create view` grammar of Task 2, the key forms with their widths — `lower(string(N))` is ⌊3N/2⌋ bytes at Unicode 16.0.0, keys up to 128 bytes —, select items, `group by` = the key, the identity rule message, `unique` and `check` with the predicate grammar and "the broker refuses `unique` (58) and `check` (57) until the view-constraint engine ships"); a **Reads** section (`select`, point vs scan, `lower('…')` literals, `limit`); `show topic` / `show view`; the compile codes 34/35/36/56/57/58 with what triggers each; the RegressorsPath block as the worked example.
  - **`docs/api/wire-protocol.md`:** the frame catalogue rows for 84/85 with the layouts and statuses; the `StatId` table (C16) with scopes.
  - **`docs/api/client-cpp.md`:** "Materialized views" opens with QL (`create view` now exists) and keeps the code path; `getStats` / `getStatsAndWait` and `StatsResult::get`.
  - **`docs/api/admin-cli.md`:** `rune stats` (flags, text and JSON forms with an example), `rune ql` `select` output and `show topic` / `show view`.
  - The site's views section lives in the separate `rune-site` repo and is not touched (overview § Docs; deploys need the owner's go-ahead).
- [ ] **Step 4: Commit.**
```bash
git add broker/benchmark/QlPlanBench.cpp broker/benchmark/GetViewRowServeBench.cpp broker/CMakeLists.txt \
        docs/api/ql.md docs/api/wire-protocol.md docs/api/client-cpp.md docs/api/admin-cli.md
git commit -m "bench(ql): text->plan and wire->plan gates, GetStats serve; docs for QL views, select, show and rune stats" \
           -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c); stage (c) runs on the VM under its lock.
  - `ctest --test-dir build/sub-ql --output-on-failure`; the client `Regression` suite on `build/sub-client-cpp`; the codec, catalog and security suites on `build/sub-codec`, CatalogGolden included.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) with `-R Regression -LE latency`, plus `-R 'CheckedWritesQl|CheckedWritesStats'` under ASan.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure --repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 11 benches (already recorded).
  - This part has no docker or VM e2e of its own; its real e2e is Task 10 on the default shard runtime.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model; the implementer resolves each comment.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. "Export the §13 stats as Aeron counters (SD13)" — `enhancement`, `sev:low`, `area:broker`, `area:ops`.
  2. "QL: `check` predicates over columns wider than 16 bytes or variable strings" — `enhancement`, `sev:low`, `area:ql` (P8-5).
  3. Every recorded deviation that is not urgent.
- [ ] Hand over to Part 09 per the overview's acceptance list for 08: every statement of §1 parses, prepares and round-trips (the golden, Tasks 4/5/9); `GetStats` serves every counter that exists. Part 09 deletes the two P7-9 AdminWorker lines, inverts the `…BeforePart09…` expectations (Task 10's outcome table and QM2's unique admin cell), adds id 32 to `collectViewPartitionStats` and switches `show view`'s constraint labels to `Client::constraintName`.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §10.1 tokens `*`, `<`, `<=`, `>`, `>=` (and `<>` for the predicate grammar of §8.2): Task 1. Topics: field list optional per SD1, keyless typed topics (R02d), `autoincrement`, `key.index`: Task 1. Views, every `<keyspec>` and `<items>` form, `group by`, `constraint … check`: Task 2. Reads with point/scan/parse-error semantics and `lower('…')` literals: Tasks 3 and 9 (P8-6).
  - §10.2 AST appended to `Statement`: Tasks 2–3. Prepare resolves the source descriptor and builds plan v2: Task 4 (offline) and Task 9 (live). The author type in a shared library: 07's codec move, linked into `ql` in Task 4. Catalog fields in `normalizedPlanBlob`: the AdminWorker already normalizes; the v1-shape rule (P8-4) keeps existing fingerprints. Compile-error codes and the removal of the message search: Task 5. CLI decode with the output descriptor: Task 9. `show view` prints the constraints and §13 stats: Task 9. `errorCodeName` 48–58: Part 03 (precondition).
  - §13 per verdict source (intent rejections, verdict lag, `W`, markers, rejected not compacted, autoincrement counter, `R`, burned) and per view (scan rows and pages; `uniqueConflicts` from 09): Tasks 6, 7; "appear in `show view` / `show topic` and the metrics export": Task 9 (`rune stats --json`, SD13). Part G stats: 11 appends.
  - §15 cells: the Interaction matrix section.
  - §12: `GetStats` gated on broker version 7 (Task 8) and client version 12 at the gateway (Task 7); every DDL gate stays the AdminWorker's.
  - Overview acceptance for 08: Tasks 4, 5, 9, 10 (every §1 statement) and 7 (every counter that exists).
- **Gaps found and recorded (returned to the controller):**
  - The acceptance block's `device_login` is keyed by `userId`. Aggregate contributors are keyed by the source key (spec §9a, `AggState.h`), so a user's later login replaces the earlier one's contribution and `device_history.logins` is at most 1 per user. Counting logins needs a per-login key (for example `loginId uint64 autoincrement`). Task 10 pins today's semantics; the owner decides the schema.
  - The block references `character` without defining it; the golden adds spec §1's topic.
  - `user` and `character` declare `replicas 3`, so the golden's e2e runs on `ThreeNodes`.
  - C14 leaves `PredicateNode::constBytes` unencoded: P8-5 (contract addition 2).
  - §10.1 "anything else is a parse error" for `select` needs the view's key, so it is decided when the statement is planned (P8-6).
  - `GetStats` serving role and authorization are unspecified: leader only, `DESCRIBE` (P8-10).
  - R06f and Part 02 Task 7 both claim `partitions` optional: 02 ships the parser change, 08 owns the autoincrement cell.
- **Step scan.** Every test step names its suite, file and assertions with the spec's values (codes 34/35/36/39/56/57/58, widths 24/66/72/128/130, Unicode `0x00100000`, ids 1–36, 33 and 3,742 bytes, ⌊3N/2⌋). Every code step names its file, signature and placement; layouts (84/85, the worker chunk), grammars, the authoring rules and the print formats are spelled out because the tests alone do not fix them. No function body is transcribed.
- **Type consistency.** `KeyItem`, `SelectItem`, `Literal`, `PredNode`, `ConstraintDef`, `CreateViewDef` (Task 2) → Tasks 3, 4, 9. `SelectDef`, `WhereTerm`, `ShowTopicDef`, `ShowViewDef` (Task 3) → Task 9. `ViewSource`, `applyViewPlan`, `viewSourceOf`, `predicateText` (Task 4) → Tasks 5, 9, 11. `GetStats`, `StatsResponse`, `StatsScope`, `StatId`, `statName` (Task 6) → Tasks 7, 8, 9. `StatsSink`, `collectTopicPartitionStats`, `collectViewPartitionStats`, `partitionLedElsewhere_`, `CheckedWritesRig::getStats` (Task 7) → Tasks 10, 11. `StatsResult`, `getStatsAndWait`, `decodeStatsPayload` (Task 8) → Tasks 9, 10. `PrepareScope`, `PCreateView`, `PSelect`, `planSelect`, `formatRows`, `formatStatsJson` (Task 9) → Task 10.
- **Review Focus.** Each of the five lines has its named test in its owning task (Tasks 2, 3, 4, 5, 9, 10).
- **Proportion.** The plan fixes the grammar, the authoring rules, the wire, the numbers and the assertions; parser bodies, the collectors, the gateway handler and the CLI formatting code are left to the implementer within the stated rules.
