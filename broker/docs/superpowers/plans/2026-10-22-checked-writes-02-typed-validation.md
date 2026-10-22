# Checked Writes Part 02 — Typed Topics Always Validate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every typed topic validates every write (decode against a known schema version, UTF-8 strings, wire key = key field) behind the new cluster feature level `kCheckedWrites`, through a per-partition `PartitionWriteState*` next to `RcSlot`; untyped topics stay raw logs with zero added cost; every database feature is refused on untyped and keyless topics; `Key` never truncates; typed and raw handles bind only to their own kind; `partitions` defaults to 1.

**Architecture:** The admin thread posts, for every typed topic and in this order, its per-version descriptors (`kA2cSchemaUpdate`, now for every typed topic), its write spec (new `kA2cWriteSpec`) and only then `kA2cCatalogTopicApplied`. The control thread fans the first two out through one FIFO (`SchemaFanout`, which now carries both `SchemaUpdate` and the new `WriteSpecUpdate` op) and builds the topic's replicas only once that FIFO holds nothing for the topic, so no worker can own a typed partition before it holds the partition's spec. On the worker, `TopicWriteSpec` holds the key facts and one materialized `Validator` + `Descriptor` per version; each owned partition of a typed topic gets a `PartitionWriteState` in its `RcSlot`, reached in O(1) through `writeById_`. `produceCore_` calls the header-only `checkTypedBatch()` at the existing validation call site (before dedupe, SD2). The committed metadata feature level reaches every worker and the AdminWorker through `ClusterFeatureLevelFanout`; below level 4 only topics that were created with `validate` keep validating exactly as before.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices), Google Benchmark with `broker/benchmark/BenchGate.h`, CMake/Ninja (`build/root`, `build/sub-codec`, `build/sub-ql`, `build/sub-client-cpp`, `build/rel`, `build/asan`, `build/tsan`, `build/client-tsan`), SBE 1.40.2 for the internal ring schema, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`) over real Aeron.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` §4 (Part A), §12 (feature level), §15 "Validation (§4)" and the "Reads" bullets owned by 02. Contract: `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C2, C7, SD1, SD2, SD6, SD9). Executors read §4, §12 and the overview's C2/C7 in full.

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root; every command runs from it.
- **Branch.** `feature/checked-writes-02-typed-validation`, cut from the latest `main` (`git fetch` first). Part 02 depends on nothing (overview lane 2: 02 → 03). It may run concurrently with 01 and 10; it touches none of their files except `Worker.h` (02 owns the write-state block; 01 does not edit `produceCore_`).
- **Preconditions** (each must hit exactly once before Task 1):
  - `grep -n "inline constexpr int16_t kLatest = kBoundProducerIds;" broker/libs/replication/include/rune/replication/ReplicationMessages.h`
  - `grep -n "bool schemaRejects_(int32_t topicId" broker/libs/node/include/rune/node/Worker.h`
  - `grep -n "kA2cFilterSchema        = 25," broker/libs/node/include/rune/node/AdminChannels.h`
  - `grep -n "FilterSchema              = 25," broker/libs/node/include/rune/node/WorkerOp.h`
  - `grep -n '<validValue name="FILTER">11</validValue>' protocol/schema/rune-internal-ring.xml`
- **Builds** (already configured; if one is missing, stop and tell the controller): `build/root` (`cmake --build build/root -j14 --target <t>`, `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`), `build/sub-codec`, `build/sub-ql`, `build/sub-client-cpp`, `build/rel` (`build/rel/broker/rune_broker_benchmark`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Codec sources are listed three times.** This part adds no `codec/src/*.cpp` (`Utf8.h` is header-only). New codec tests go only into `codec/CMakeLists.txt` (`rune_codec_tests`).
- **SBE.** Regenerate the internal-ring flyweights from `protocol/` with a local jar: `java -Dsbe.target.language=cpp -Dsbe.output.dir=include -jar "$SBE_JAR" schema/rune-internal-ring.xml`. Commit the regenerated `protocol/include/rune_protocol_internal_sbe/*` headers, never the jar.
- **No VM e2e in this part** (the docker e2e is created by 05). The real e2e path is the in-process real client against real brokers on the default shard config (Task 9). VM benches follow `broker/bench/BENCHMARKING.md` (prepare once with `broker/bench/vm-bench-prepare.sh`, restore with `vm-bench-restore.sh`, the whole run under the VM lock on `$RUNE_VM_HOST`).
- **Commits:** `git add <paths>` only; every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Nothing is pushed. Never run `conan create/export/upload`.

## Global Constraints

Copied from the overview (each line verbatim from the spec or the repo rules). Every task implicitly includes this section.

**Error codes** (`protocol/include/rune/protocol/client/Protocol.h:105`, `ErrorCode`, append-only):
- `ConstraintViolation = 51` … `ViewUniqueInvalid = 58` belong to later parts; Part 02 adds no code.
- Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.

**Versions.**
- `kClientVersionCheckedWrites = 12` and `kClientVersion = 12`; `kBrokerVersionCheckedWrites = 7` and `kBrokerVersion = 7` are **owned by Part 03**. Part 02 bumps neither (no v12 wire is added here).
- Feature level `MetadataFeature::kCheckedWrites = 4` and `kLatest = kCheckedWrites` (`broker/libs/replication/include/rune/replication/ReplicationMessages.h:470-472`; today `kLatest = kBoundProducerIds = 3`).
- Plan v2, checkpoint v3 and catalog `formatVer` 2 follow the existing frozen-format rules: old formats still decode. (Part 02 changes no frozen format.)

**Values.**
- Topic key field ≤ `Key::kMaxInline` = **16 bytes** (`client-cpp/include/rune/client/Key.h:35`).
- `partitions` defaults to 1. Strings are UTF-8, and `lower()` is Unicode simple lowercasing.

**Budgets** (hard gates, spec §14):
- New typed-topic validation: ≤ **1 µs p50** for a one-record batch.
- No ABAB n=3 regression on `ProducePathBenchmark` and `WorkerProduceLatencyBench` for untyped topics and for already-validated typed topics.
- Repo latency gates stay: view update < 10 µs, point read < 50 µs, QL ≤ 2–5 µs.

**Repo rules** (root `CLAUDE.md` §1–§4):
- **Hot path.** The produce, consume, fold and query paths are zero-alloc, take no lock, use no atomics as coordination and no `thread_local`, make no virtual call and use no `std::function`. Hot paths use concrete types plus a branch, or templates. Control-plane work (installs, DDL, recovery scans) runs as cold worker ops and may allocate.
- **States.** Rich-state variants, never scattered booleans.
- **Clocks.** One injected clock. Tests advance it and call `doWork`, with a hard iteration cap and never a wall-clock wait.
- **Regression tests.** Every fix ships a `*Regression*` test. Each task runs the affected-area tests.
- **Checkpoints** run the full `-R Regression -LE latency` suite, plus ASan+UBSan and TSan for the broker and the client, plus CatalogGolden.
- **Interaction matrix.** Every feature is tested across its matrix. Each plan has an "Interaction matrix" section and a "Review Focus" section, with 1–3 phases of small tasks.
- **Real e2e.** Nothing is done until its real e2e path is green on the **default** config (shard worker mode).
- **No weakened tests.** A weakened assertion is recorded as a deviation. Urgent ones are fixed now and the rest are filed as GitHub issues with type, severity and area labels.
- **Names.** Every new test suite name starts with `CheckedWrites`; every fix test carries `Regression` in its suite or name.

## Rulings this part depends on (restated from the overview; binding)

- **SD1.** A topic without a field list stays an untyped raw log (`topic events;`). "The field list is required" only means every database feature needs one.
- **SD2.** Validation stays at today's call site in `produceCore_` (`broker/libs/node/include/rune/node/Worker.h:8255`, before `producerState.validate`), moved onto the per-partition pointer (C7). Part 03 moves it into the hook after dedupe.
- **SD6.** Part 02 defines `kCheckedWrites` and its propagation (C2); Part 03 uses it.
- **SD9.** `key.index`, intents and `autoincrement` are refused at DDL on transient topics with `InvalidRequest`, "<feature> needs a persisted topic". Part 02 ships the one rule table that holds these messages (Task 6); Parts 05 and 06 call it from their DDL.

## Part 02 decisions (binding for executors; flagged to the owner in the return)

- **P1 — Tombstones validate as keys only.** A record with an empty or null value on a typed topic is never decoded. On a keyed topic with a checkable key its wire key must be exactly `keyWidth` bytes (level 4); on a keyless typed topic it is rejected (24). Today `schemaRejects_` rejects every tombstone on a `validate` topic (`val.size() < 4`, `Worker.h:3905`), which would break deletes on every typed topic once validation is always on — fixed at every level, with a regression test.
- **P2 — A version above the highest one the worker holds is retriable.** Such a record (an ALTER whose `SchemaUpdate` has not reached this worker or this broker yet) answers `NotLeader` (2): the client resends after its backoff and the batch is accepted once the version lands (time-independent convergence, root rule 16). Version 0 and any version at or below the highest installed one that has no entry stay a definite `SchemaValidationFailed` (24). Within one batch the first non-accepted record decides.
- **P3 — `string(N)` padding.** The content of a `string(N)` field is the bytes before its first NUL; every byte after that NUL must be NUL, else 24. The content must be valid UTF-8. `string` (variable) content is validated whole; `bytes` is never checked.
- **P4 — Topic key types.** A key field must be `uint16`, `int32`, `uint32`, `int64`, `uint64`, or `string(N)` with N ≤ 16 (`rune::codec::isTopicKeyType`). Any other key field (`string`, `bytes`, `float64`, `bool`, enum, `string(N > 16)`) is refused by `DefineTopic` with `InvalidSchema` (23) and a detail naming the field. Topics that already exist with such a key keep working: their spec has `keyWidth = 0` and the wire-key rule is skipped for them (logged once per worker at install).
- **P5 — Legacy below the level.** `TopicWriteSpec::legacyValidate` mirrors the catalog's `SchemaDef::validate`. Below level 4 a topic validates iff `legacyValidate`, with today's checks only (decode by exact version, block length, enums, var framing) plus P1/P2; no UTF-8 and no wire-key rule. At level 4 every typed topic validates fully. The client now always sends `validate = 1` for a typed `DefineTopic` (the byte stays on the wire), so topics created by new clients validate in a mixed cluster too.
- **P6 — View-source refusals keep their codes.** An untyped source answers `ViewSourceNotFound` (33) with detail "view source must be a typed topic" (the code existing tests pin, `DdlOccupantMatrixRegressionTest.cpp:444-445`); a keyless typed source answers `ViewKeyInvalid` (35) with detail "view source needs a key".
- **P7 — The create gate.** `BrokerNode::runCatalogCreate_` parks a topic while `SchemaFanout::pendingFor(topicId)` is true and builds it once the fanout drained it. Without it a full worker ring lets `WorkerPool::reconcile` deliver ownership of a typed partition before its spec (an unvalidated window).
- **P8 — Binding uses a kind probe.** `GetTopicSchema` with the new reserved fingerprint `kSchemaKindFingerprint` answers a typed topic like fingerprint 0, an untyped topic `OK` with an empty blob, version 0 and its partition count, and an unknown name `UnknownTopic`. Existing requests are answered exactly as today. An older broker answers the probe `UnknownTopic` for every name.
- **P9 — `RecordSpan::key` moves into Part 02.** `checkTypedBatch` must walk records with the bounded `forEachRecordSpan` (`protocol/include/rune/protocol/BatchRebuild.h:41`), because `RecordBatchReader::forEachRecord` (`RecordBatch.h:393`) never bounds `klen`/`vlen` and the bytes are untrusted (their CRC is checked only later, in `handleProduceBatch`). It needs the key, so Part 02 appends `std::string_view key;` to `RecordSpan`; Part 03 still appends `attributes` and `index` (C3).
- **P10 — QL keeps requiring `key` on a typed topic** (`ql/src/Parser.cpp:653`). Keyless typed topics are made through the client API; the QL grammar change is Part 08's.
- **P11 — No matrix id range.** The shared `CheckedWritesMatrixExpectations.h` (ids per part) is created by Part 04 and reserves no range for 02. Part 02's matrices are its own `TEST_P` tables with their own cell-count pin.

## Review Focus

1. **A typed-topic batch whose framing lies** (a key or value length varint past the batch end, `recordCount` larger than the records present, a record length of 0): bounded walk, `SchemaValidationFailed`, never an out-of-bounds read. Pinned by `CheckedWritesWriteStateRegression.LyingFramingIsRejectedInBoundsRegression` and the ASan run of the fuzz test (Task 3).
2. **Deletes on typed compacted topics** (empty and null values): a tombstone with the key field's width passes at every level and compaction removes the key; a wrong-width tombstone is refused at level 4. Pinned by the matrix rows V12/V13 (Task 3), `CheckedWritesWorkerRegression.TombstoneOnValidatedTopicIsAcceptedRegression` (Task 4) and `CheckedWritesBrokerRegression.TypedTombstoneDeletesAndCompactsRegression` (Task 9).
3. **A producer bound to a schema version this worker has not installed yet** (right after an ALTER, or on a broker whose catalog lags): retriable `NotLeader`, accepted once `SchemaUpdate` lands, never a definite rejection of a valid record. Pinned by `CheckedWritesWorkerRegression.VersionAheadIsRetriableUntilInstalledRegression` (Task 4) and `CheckedWritesBrokerRegression.AlterThenProduceNewVersionSucceedsRegression` (Task 9).
4. **A cluster upgrading under load** (level 3 → 4) with typed topics created by old clients (`validate = false`) that already hold invalid records: accepted below the level, refused from the level on, old records still readable. Pinned by `CheckedWritesWorkerRegression.LevelRaiseStartsFullValidationRegression` (Task 4) and `CheckedWritesBrokerRegression.PreUpgradeTopicValidatesAfterTheLevelRegression` (Task 9).
5. **Raw producers writing typed topics with a hand-built key** (`Key(uint64_t)` on a `uint32` key field, an unpadded `string(N)` key, a non-empty key on a keyless topic): the first two are refused (24) at level 4, the third is accepted (keyless topics have no key rule). Pinned by the wire-key matrix (Task 3) and `CheckedWritesBrokerRegression.RawKeyOfTheWrongWidthIsRefusedRegression` (Task 9).

---

## Interaction matrix

Cells owned by 02 (overview §"Interaction matrix: §15 cells by owner"): the "Validation (§4)" block except transient views and `lower()`, plus "Reads": `partitions` omitted, binding both ways, key fields wider than 16 refused and `Key` truncation.

### M1 — Validation (unit, `CheckedWritesValidationMatrix`, `broker/libs/node/tests/node/CheckedWritesValidationMatrixTest.cpp`, Task 3)

`TEST_P` over **Op × Entity × Level × Position**, each cell building one batch with `CheckedWritesBatches.h` and calling `checkTypedBatch` directly.

- **Ops (15):** V01 valid record; V02 version 0; V03 version = max + 1; V04 blockLength mismatch; V05 truncated var field; V06 enum outside its whitelist; V07 overlong UTF-8 (`C0 AF`) in a `string` field; V08 stray continuation byte (`80`) in a `string(16)` field; V09 dirty `string(16)` padding (`'a',0,'b'`); V10 wire key with the right width and other bytes; V11 wire key 4 bytes on a `uint64` key; V12 tombstone with an 8-byte key; V13 tombstone with a 4-byte key; V14 invalid UTF-8 in a `bytes` field; V15 a key-length varint past the batch end.
- **Entities (4)**, one descriptor `{id uint64, name string(16), bio string, blob bytes, kind enum{1,2,3}}`: TypedLegacy (`legacyValidate`, key `id`), TypedPreUpgrade (not `legacyValidate`, key `id`), TypedKeyless (`legacyValidate`, no key), TypedUncheckableKey (`legacyValidate`, key `bio`, `keyWidth = 0`). Untyped topics have no state and are covered at the worker layer (W1).
- **Levels (2):** L3 (`checkedLevel = false`), L4.
- **Positions (3):** only record; first of 3; last of 3 (the other records valid).
- **Expected, top-down (first matching rule):**
  1. L3 and TypedPreUpgrade → Accept.
  2. V15 → Reject.
  3. V12/V13: TypedKeyless → Reject; TypedUncheckableKey → Accept; L3 → Accept; L4 → V12 Accept, V13 Reject.
  4. V02 → Reject; V03 → VersionAhead.
  5. V04, V05, V06 → Reject.
  6. V07, V08, V09: L4 → Reject; L3 → Accept.
  7. V10, V11: L4 and (TypedLegacy or TypedPreUpgrade) → Reject; otherwise Accept.
  8. V01, V14 → Accept.
- **Count:** 15 × 4 × 2 × 3 = **360** cells.

### M2 — Wire key (unit, `CheckedWritesWireKeyMatrix`, same file, Task 3)

- **Key types (7):** `uint16`, `int32`, `uint32`, `int64`, `uint64`, `string(1)`, `string(16)`.
- **Variants (6):** Exact; OneShort; OneLong; SameWidthOtherBytes; ZeroPlaceholderBoth (key and field all zero: the §6.4 placeholder); ZeroKeyNonZeroField.
- **Levels (2):** L3, L4 (entity TypedLegacy).
- **Expected:** L3 → Accept. L4 → Exact and ZeroPlaceholderBoth Accept, every other variant Reject.
- **Count:** 7 × 6 × 2 = **84** cells.

`CheckedWritesValidationMatrixCountRegression.CellCountsArePinned` asserts 360 and 84 (a change to either table must change the pin).

### M3 — Feature rules (unit, `CheckedWritesDdl`, `broker/libs/node/tests/node/CheckedWritesDdlTest.cpp`, Task 6)

`featureRefusal(feature, shape)` over 5 features (`View`, `Filter`, `KeyIndex`, `AutoIncrement`, `Constraint`) × 6 shapes (untyped/keyless-typed/keyed-typed × disk/transient) = **30** cells, each asserting the exact message or `nullopt` (table in Task 6).

### W1 — Worker timing (integration, `CheckedWritesWorker*`, `broker/libs/node/tests/node/CheckedWritesWorkerTest.cpp`, Task 4)

| Cell | Test |
|---|---|
| untyped topic: no state, any bytes accepted | `CheckedWritesWorker.UntypedTopicHasNoWriteStateAndAcceptsAnyBytes` |
| spec installed before ownership | `CheckedWritesWorker.SpecBeforeOwnershipCreatesTheStateOnOwnership` |
| ownership before spec (schema adoption on a live topic) | `CheckedWritesWorker.SpecAfterOwnershipAttachesToOwnedPartitions` |
| ALTER adds v2 while producing | `CheckedWritesWorkerRegression.VersionAheadIsRetriableUntilInstalledRegression` |
| level raised mid-stream; a lower level never applies | `CheckedWritesWorkerRegression.LevelRaiseStartsFullValidationRegression`, `CheckedWritesWorker.FeatureLevelNeverLowers` |
| role change leader → follower → leader keeps the same state | `CheckedWritesWorker.RoleChangesKeepTheWriteState` |
| ownership away and back (new incarnation) | `CheckedWritesWorker.NewIncarnationGetsAFreshState` |
| topic dropped | `CheckedWritesWorker.TopicDropReleasesSpecAndStates` |
| mixed-version batch | `CheckedWritesWorker.MixedVersionBatchValidatesEachRecordByItsVersion` |
| `SchemaUpdate` after `WriteSpecUpdate` (new version) | `CheckedWritesWorker.LateVersionGetsItsKeyOffset` |
| tombstones on a validated topic | `CheckedWritesWorkerRegression.TombstoneOnValidatedTopicIsAcceptedRegression` |
| a broker's own control batch on a typed topic | `CheckedWritesWorker.ControlRecordsAreNeverValidated` |

### B1 — Real broker (e2e on the default shard runtime unless noted, `CheckedWritesBrokerRegression.*`, `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`, Task 9)

| §15 cell | Test |
|---|---|
| raw `send` works on untyped topics and is validated on typed ones | `RawSendIsRawOnUntypedAndValidatedOnTypedRegression` |
| a record matching no known schema version | `UnknownVersionIsRefusedRegression` |
| … including a typed topic that existed before the upgrade | `PreUpgradeTopicValidatesAfterTheLevelRegression` |
| a wire key that doesn't match the key field | `RawKeyOfTheWrongWidthIsRefusedRegression` |
| control batches pass | `TransactionOnTypedTopicCommitsRegression` |
| UTF-8 valid / invalid / padding / `bytes` (end to end) | `Utf8RulesEndToEndRegression` |
| tombstones | `TypedTombstoneDeletesAndCompactsRegression` |
| untyped and keyless topics refuse views (filters: untyped only) with the message | `ViewOnUntypedOrKeylessSourceIsRefusedRegression` (filters: the existing `FilterBrokerRegression.UntypedTopicRefusedRegression`) |
| `TypedTopic<T>` refuses an untyped topic or a fingerprint mismatch | `TypedOpenOfUntypedTopicIsRefusedRegression` (mismatch: the existing `OpenTopicGate.StaleFingerprintStillThrows`) |
| a raw handle refuses a typed topic | `RawOpenOfTypedTopicIsRefusedRegression` |
| `partitions` omitted gives 1 partition | `QlTopicWithoutPartitionsHasOneRegression` |
| key fields wider than 16 refused at DDL | `WideKeyFieldIsRefusedAtDefineRegression` |
| failover: the new leader validates | `ThreeNodesNewLeaderValidatesRegression` (ThreeNodes) |
| legacy runtime validates | `LegacyRuntimeValidatesRegression` (`OneBroker::Runtime::Legacy`) |
| both produce routes into `produceCore_` (gateway-routed op, per-worker ingress) validate | `BothIngressRoutesValidateRegression` (Shard, `perWorkerIngressEnabled` false and true) |
| restart: validation holds from the first produce | `RestartValidatesFromTheFirstProduceRegression` |
| schema adoption (untyped → typed) on a live topic | `AdoptionStartsValidationRegression` |
| ALTER then produce the new version | `AlterThenProduceNewVersionSucceedsRegression` |

Intents, `key.index` and `autoincrement` DDL do not exist before Parts 05/06: their refusal cells call `featureRefusal` from those parts' DDL and add their own broker cells there (M3 pins the messages now).

### S1 — Model-checked stress (Task 3)

- `CheckedWritesValidationFuzzRegression.RandomMutationsAgreeWithTheModel` (`rune_checked_writes_tests`): seeds 1..64, 2000 seeded byte mutations (flip, truncate, extend, varint rewrite, key rewrite) of valid 1–8-record batches per seed, each over the 4 entities × 2 levels; `checkTypedBatch` must equal the independent reference `referenceCheck` in `broker/libs/node/tests/support/CheckedWritesValidationModel.h`. On mismatch the test prints the seed and the batch hex. It runs under ASan at every checkpoint (Review Focus 1).
- `CheckedWritesUtf8Fuzz.AgreesWithTheCodePointDecoder` (`rune_codec_tests`): seeds 1..32, 10 000 random byte strings of length 0–64 each (biased toward 0x80–0xF4 lead bytes), `validateUtf8` equals a reference decoder written in the test as a straight code-point decode (Task 1).

---

## File Structure

| File | C/M | Responsibility |
|---|---|---|
| `codec/include/rune/codec/Utf8.h` | C | `validateUtf8` with an 8-byte ASCII fast path; error kinds |
| `codec/include/rune/codec/PrimType.h` | M | `kMaxTopicKeyBytes`, `isTopicKeyType` (P4) |
| `codec/include/rune/codec/Validator.h`, `codec/src/Validator.cpp` | M | `StringCheck` mode: UTF-8 for `string` / `string(N)`, P3 padding |
| `codec/tests/Utf8Test.cpp` (C), `codec/tests/ValidatorTest.cpp` (M), `codec/CMakeLists.txt` (M) | C/M | codec tests |
| `protocol/include/rune/protocol/BatchRebuild.h:17-23,66-73` | M | `RecordSpan::key` (P9) |
| `protocol/include/rune/protocol/client/SchemaMessages.h:309` | M | `kSchemaKindFingerprint` (P8) |
| `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*` | M | `CLUSTER_FEATURE_LEVEL = 26`, `WRITE_SPEC_UPDATE = 27`, `TailKind WRITE_SPEC = 12` |
| `broker/libs/replication/include/rune/replication/ReplicationMessages.h:465-472` | M | `kCheckedWrites = 4`, `kLatest` |
| `broker/libs/node/include/rune/node/WriteState.h` | C | `TopicWriteSpec`, `SpecVersion`, `PartitionWriteState`, `TopicWriteSpecWire` + codec, `writeSpecWireOf`, `checkTypedBatch` |
| `broker/libs/node/include/rune/node/ClusterFeatureLevelFanout.h` | C | control → every worker + admin, resumable, latest-wins |
| `broker/libs/node/include/rune/node/TopicFeatureRules.h` | C | `featureRefusal` (§4 + SD9 messages) |
| `broker/libs/node/include/rune/node/WorkerOp.h:65-158` | M | `ClusterFeatureLevel = 26`, `WriteSpecUpdate = 27` |
| `broker/libs/node/include/rune/node/Worker.h` | M | level, write specs, `RcSlot::write`, `writeById_`, `writeOf_`, `produceCore_` call site; removes `validators_`, `validatedTopics_`, `schemaRejects_` |
| `broker/libs/node/include/rune/node/SchemaFanout.h` | M | carries `WriteSpecUpdate`; `pendingFor` |
| `broker/libs/node/include/rune/node/AdminChannels.h` | M | `kMsgClusterFeatureLevel = 26`, `kA2cWriteSpec = 26` + codecs |
| `broker/libs/node/include/rune/node/AdminWorker.h`, `src/AdminWorker.cpp` | M | level setter; key-type rule; view-source details; kind probe |
| `broker/libs/node/include/rune/node/BrokerNode.h`, `src/BrokerNode.cpp`, `src/CatalogEffectFrames.cpp` | M | effect order, write-spec post, level service, create gate |
| `broker/libs/node/CMakeLists.txt` | M | `rune_checked_writes_tests`; `AllocWindow.cpp` into `rune_produce_validation_tests`; broker-client test file |
| `broker/libs/node/tests/node/CheckedWrites{FeatureLevel,WriteState,ValidationMatrix,Worker,SpecFanout,Ddl}Test.cpp` | C | unit / integration suites |
| `broker/libs/node/tests/support/CheckedWritesBatches.h`, `CheckedWritesValidationModel.h` | C | typed batch builders, reference model |
| `broker/libs/node/tests/integration/ProduceValidationTest.cpp`, `tests/node/WorkerOpWireRegressionTest.cpp`, `tests/node/SecurityRingCodecTest.cpp` | M | updated harness, new op and ring codec cases, alloc gate |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` | C | real-broker slice |
| `broker/benchmark/ProduceValidateBench.cpp` | M | `BM_CheckedWriteHook/*`, existing benches send the spec |
| `ql/src/Parser.cpp:604-620`, `ql/tests/ParserTest.cpp` | M | `partitions` optional |
| `client-cpp/include/rune/client/{Key.h,TypedTopic.h,Client.h}`, `client-cpp/src/Client.cpp`, `client-cpp/include/rune/client/producer/Producer.h`, `client-cpp/src/producer/{Producer.cpp,ProducerImpl.cpp}`, `client-cpp/src/shard/*` | M | no truncation, `KeyTooLong`, `validate` removed, `rawTopicAndWait`, bind messages |
| `client-cpp/src/cli/{CatalogCommands.cpp,Console.cpp,QlEngine.cpp,QlEngine.h,Cli.cpp}` | M | `--validate` gone, `raw` marker |
| every caller of the `defineTopic…` overloads (53 files today, found by the compiler) | M | `validate` argument removed |
| `docs/api/{ql.md,client-cpp.md,admin-cli.md,codec.md,wire-protocol.md}`, `broker/docs/CATALOG.md` | M | docs (Task 11) |

---

## Phase 1 — Foundations

### Task 1: UTF-8 validation and the validator's string check (codec)

**Files:**
- Create: `codec/include/rune/codec/Utf8.h`, `codec/tests/Utf8Test.cpp`
- Modify: `codec/include/rune/codec/PrimType.h:5-17`, `codec/include/rune/codec/Validator.h:16-35`, `codec/src/Validator.cpp:32-127`, `codec/tests/ValidatorTest.cpp`, `codec/CMakeLists.txt:197-224` (add `tests/Utf8Test.cpp` to `rune_codec_tests`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```cpp
  namespace rune::codec {
  enum class Utf8Error : uint8_t { None, InvalidLead, StrayContinuation, Truncated, Overlong, Surrogate, AboveMax };
  struct Utf8Result { Utf8Error error = Utf8Error::None; std::size_t offset = 0; [[nodiscard]] constexpr bool ok() const noexcept { return error == Utf8Error::None; } };
  [[nodiscard]] constexpr Utf8Result validateUtf8(std::span<const uint8_t> bytes) noexcept;      // Utf8.h
  inline constexpr std::size_t kMaxTopicKeyBytes = 16;                                              // PrimType.h
  [[nodiscard]] constexpr bool isTopicKeyType(PrimType t, uint32_t width) noexcept;                 // PrimType.h (P4)
  enum class StringCheck : uint8_t { None, Utf8 };                                                  // Validator.h
  ValResult Validator::validate(std::span<const uint8_t> record) const noexcept;                    // unchanged = StringCheck::None
  ValResult Validator::validate(std::span<const uint8_t> record, StringCheck check) const noexcept;
  }
  ```

- [ ] **Step 1: Write the failing tests.**
  - `codec/tests/Utf8Test.cpp`:
    - `CheckedWritesUtf8.AcceptsAsciiAndEveryWidth`: `""`, `"Foo"`, `C3 A9` (é), `E2 82 AC` (€), `F0 9F 98 80` (U+1F600), `EF BF BF` (U+FFFF), `F4 8F BF BF` (U+10FFFF), 1000 ASCII bytes → `ok()`.
    - `CheckedWritesUtf8.RejectsEachInvalidClass` (table `{bytes, error, offset}`): `C0 AF` → Overlong@0; `E0 80 AF` → Overlong@0; `F0 80 80 AF` → Overlong@0; `ED A0 80` (U+D800) → Surrogate@0; `ED BF BF` → Surrogate@0; `F4 90 80 80` → AboveMax@0; `F5 80 80 80` → InvalidLead@0; `FF` → InvalidLead@0; `61 80` → StrayContinuation@1; `E2 82` → Truncated@0; `61 F0 9F 98` → Truncated@1; `C3 41` → Truncated@0 (a non-continuation after a lead).
    - `CheckedWritesUtf8.FastPathBoundaryAtEveryOffset`: for k in 0..16, 'a'×k then `80` → StrayContinuation@k; 'a'×k then `C3 A9` → ok.
    - `CheckedWritesUtf8Fuzz.AgreesWithTheCodePointDecoder`: S1.
  - `codec/tests/ValidatorTest.cpp`, descriptor `{name string(8), bio string, blob bytes}`:
    - `CheckedWritesValidator.Utf8CheckRejectsBadString`: `bio = 61 80` → `ok == false`, `fieldId == bio.id`, `reason == "string is not valid UTF-8"`; the same record with `StringCheck::None` → `ok`.
    - `CheckedWritesValidator.Utf8CheckRejectsBadFixedStr`: `name = 80 00 … 00` → reason `"string is not valid UTF-8"`.
    - `CheckedWritesValidator.FixedStrPaddingMustBeZero` (P3): `name = 'a',00,'b',00…` → reason `"string(N) padding is not zero"`; `name = 'a','b',00…` → ok; `name` all zero → ok; `name` 8 non-NUL valid bytes → ok.
    - `CheckedWritesValidator.BytesFieldIsNeverChecked`: `blob = FF FE 80` with `StringCheck::Utf8` → ok.
    - `CheckedWritesValidator.OneArgValidateIsUnchanged`: every existing `Validator.*` fixture gives the same `ValResult` through `validate(r)` and `validate(r, StringCheck::None)`.
    - `CheckedWritesValidator.TopicKeyTypes`: `isTopicKeyType` true for `UInt16/2`, `Int32/4`, `UInt32/4`, `Int64/8`, `UInt64/8`, `FixedStr/1`, `FixedStr/16`; false for `FixedStr/17`, `String/0`, `Bytes/0`, `Float64/8`, `Bool/1`, `Enum/4`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-codec -j14 --target rune_codec_tests`. Expected: compile error, `rune/codec/Utf8.h` not found.
- [ ] **Step 3: Implement.**
  - `Utf8.h`: the standard well-formedness table (Unicode 15 Table 3-7): leads `C2..DF` (1 continuation), `E0` (`A0..BF`), `E1..EC`/`EE..EF`, `ED` (`80..9F`), `F0` (`90..BF`), `F1..F3`, `F4` (`80..8F`); `C0`, `C1` and `E0 80..9F` / `F0 80..8F` are Overlong; `ED A0..BF` is Surrogate; `F4 90..` is AboveMax; `F5..FF` InvalidLead; a bare `80..BF` StrayContinuation. ASCII fast path: while 8 bytes remain, `memcpy` a `uint64_t` and skip all 8 when `(w & 0x8080808080808080) == 0`.
  - `isTopicKeyType`: integers by their fixed size, `FixedStr` with `1 <= width <= kMaxTopicKeyBytes`.
  - `Validator`: `FieldEntry` keeps its `type`. With `StringCheck::Utf8`, a `FixedStr` field (in the fixed loop) checks P3 then `validateUtf8` on the content; a `String` var field runs `validateUtf8` on its `len` bytes; `Bytes` and `Enum` paths are unchanged. The one-argument overload forwards `StringCheck::None`. No allocation.
- [ ] **Step 4: Run.** `cmake --build build/sub-codec -j14 --target rune_codec_tests && ctest --test-dir build/sub-codec -R 'CheckedWritesUtf8|CheckedWritesValidator|Validator|FixedStr|DecodeBoundary' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add codec/include/rune/codec/Utf8.h codec/include/rune/codec/PrimType.h codec/include/rune/codec/Validator.h \
          codec/src/Validator.cpp codec/tests/Utf8Test.cpp codec/tests/ValidatorTest.cpp codec/CMakeLists.txt
  git commit -m "feat(codec): UTF-8 validation with an ASCII fast path, the validator's string check, topic key types" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 2: The `kCheckedWrites` feature level and its propagation (C2)

**Files:**
- Create: `broker/libs/node/include/rune/node/ClusterFeatureLevelFanout.h`, `broker/libs/node/tests/node/CheckedWritesFeatureLevelTest.cpp`
- Modify: `broker/libs/replication/include/rune/replication/ReplicationMessages.h:465-472`; `protocol/schema/rune-internal-ring.xml:109-120` (`CLUSTER_FEATURE_LEVEL` 26) + regenerated SBE headers; `broker/libs/node/include/rune/node/WorkerOp.h:151` (`ClusterFeatureLevel = 26`); `broker/libs/node/include/rune/node/Worker.h` (dispatch at `:2675`, members, accessors); `broker/libs/node/include/rune/node/AdminChannels.h:48-82` (`kMsgClusterFeatureLevel = 26`, `writeClusterFeatureLevel`, `decodeClusterFeatureLevel`); `broker/libs/node/include/rune/node/AdminWorker.h:298` (setter next to `onPlacementView`); `broker/libs/node/include/rune/node/BrokerNode.h:485-530` (admin-side drain) and `:2711` (members); `broker/libs/node/src/BrokerNode.cpp:4815` (`serviceAdminControl_`); `broker/libs/node/CMakeLists.txt` (create `rune_checked_writes_tests` next to `rune_produce_validation_tests` at `:759`); `broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp`, `broker/libs/node/tests/node/SecurityRingCodecTest.cpp`

**Interfaces:**
- Consumes: `MetadataService::featureLevel()` (`MetadataService.h:172`).
- Produces:
  ```cpp
  namespace rune::replication::MetadataFeature { inline constexpr int16_t kCheckedWrites = 4; inline constexpr int16_t kLatest = kCheckedWrites; }
  // WorkerOp::Type::ClusterFeatureLevel = 26: level in nextOffset; cold; never ownership-gated; op ring (not the lifecycle ring).
  int16_t Worker::clusterFeatureLevel_ = rune::replication::MetadataFeature::kBase;
  [[nodiscard]] bool Worker::checkedWritesEnabled_() const noexcept;           // clusterFeatureLevel_ >= kCheckedWrites
  [[nodiscard]] int16_t Worker::clusterFeatureLevelForTest() const noexcept;
  // AdminChannels.h (control → admin, controlToAdminRing): kMsgClusterFeatureLevel = 26, payload [level:i16]
  inline constexpr std::size_t kClusterFeatureLevelWireLen = 2;
  void writeClusterFeatureLevel(uint8_t* p, int16_t level) noexcept;
  [[nodiscard]] bool decodeClusterFeatureLevel(const uint8_t* p, std::size_t n, int16_t& level) noexcept;
  void AdminWorker::setClusterFeatureLevel(int16_t level) noexcept;            // keeps the maximum
  [[nodiscard]] int16_t AdminWorker::clusterFeatureLevel() const noexcept;     // kBase until the first post
  class ClusterFeatureLevelFanout {   // control thread only
  public:
      void post(int16_t level) noexcept;                                       // latest wins; resets delivery
      int pump(WorkerPool* pool, OneToOneByteRing* admin);                     // resumable per destination
      [[nodiscard]] bool pendingForTest() const noexcept;
  };
  int16_t BrokerNode::postedClusterLevel_ = -1;
  ```

- [ ] **Step 1: Write the failing tests** (`CheckedWritesFeatureLevelTest.cpp`, target `rune_checked_writes_tests`, linking `rune_node rune_view rune_replication rune_codec GTest::gtest_main`, include `${CMAKE_CURRENT_SOURCE_DIR}/tests`):
  - `CheckedWritesFeatureLevel.LatestIsCheckedWrites`: `static_assert(kCheckedWrites == 4 && kLatest == kCheckedWrites && kBoundProducerIds == 3)`.
  - `CheckedWritesFeatureLevel.WorkerStartsAtBaseAndAppliesTheOp`: a fresh `Worker(0)` → `clusterFeatureLevelForTest() == 0`; enqueue `ClusterFeatureLevel{nextOffset = 4}`, `doWork` once → 4.
  - `CheckedWritesFeatureLevel.FeatureLevelNeverLowers`: ops 4 then 3 → 4.
  - `CheckedWritesFeatureLevel.FanoutReachesEveryWorkerAndTheAdmin`: a 3-worker `WorkerPool` with `setRunnersStartedForTest(true)` and a `OneToOneByteRing(kControlToAdminRingBytes)`; `post(4)`, `pump` → each worker's level 4 after one `doWork`; the admin ring yields one `kMsgClusterFeatureLevel` frame decoding to 4.
  - `CheckedWritesFeatureLevelRegression.FullRingIsRetriedNotDroppedRegression`: worker 1's op ring filled with `ReadLease` ops until `Full`; `pump` returns with `pendingForTest() == true`; drain worker 1, `pump` again → every worker 4, `pendingForTest() == false`, exactly one op per worker.
  - `CheckedWritesFeatureLevel.NewerLevelSupersedesUndelivered`: `post(3)`, no pump, `post(4)` → after pump each worker received exactly one op, level 4.
  - `CheckedWritesFeatureLevel.AdminKeepsTheMaximum`: `AdminWorker::setClusterFeatureLevel(4)` then `(3)` → `clusterFeatureLevel() == 4` (use the `CatalogDdlRig` admin).
  - `WorkerOpWireRegressionTest.cpp`: `WorkerOpWireRegression.ClusterFeatureLevelRoundTripsRegression` (type 26, `nextOffset` 4, no tail).
  - `SecurityRingCodecTest.cpp`: `AdminChannelsRegression.ClusterFeatureLevelMessageRoundTripsRegression` (2 bytes; a 1-byte frame decodes false).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_node_tests`. Expected: compile errors (`kCheckedWrites`, `WorkerOp::Type::ClusterFeatureLevel`, `ClusterFeatureLevelFanout.h`).
- [ ] **Step 3: Implement.**
  - The level: add `kCheckedWrites` with a D15-style comment ("typed topics validate every write, wire key = key field, UTF-8; gates the §12 DDL and write kinds"), `kLatest = kCheckedWrites`.
  - SBE: `<validValue name="CLUSTER_FEATURE_LEVEL">26</validValue>` with a comment; regenerate (Execution context).
  - Worker: dispatch `case WorkerOp::Type::ClusterFeatureLevel: if (op.nextOffset() > clusterFeatureLevel_) clusterFeatureLevel_ = static_cast<int16_t>(op.nextOffset()); break;` next to `ReadLease`. No tail in `enqueueOp`.
  - Fanout: modeled on `PeerLinkFanout` (`PeerLinkFanout.h:23-100`): one pending level, `workersDone`, `adminDone`; `post` replaces a pending level and resets both.
  - BrokerNode, `serviceAdminControl_` right after `servicePeerLinks_` (`BrokerNode.cpp:4815`, before the `if (!agent) return work;`): read `metadataService_->featureLevel()` when `metadataService_`; when it differs from `postedClusterLevel_`, `clusterLevelFanout_.post(level)` and store it; then `work += clusterLevelFanout_.pump(workerPool_.get(), adminWorkerAgent_ ? &adminWorkerAgent_->controlToAdminRing() : nullptr)`. The first pass after start posts the boot level, so it is queued on every op ring before any client frame exists.
  - Admin side (`BrokerNode.h:485` drain): `if (tid == kMsgClusterFeatureLevel) { int16_t l; if (decodeClusterFeatureLevel(p, n, l)) worker_.setClusterFeatureLevel(l); else if (badControlFrames_++ % 1024 == 0) spdlog::error(...); return; }`.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_node_tests && ctest --test-dir build/root -R 'CheckedWritesFeatureLevel|WorkerOpWire|AdminChannels|MetadataService|FeatureLevel' -j14 --output-on-failure`. Expected: PASS (existing `FeatureLevel` tests that pin `kLatest == 3` are updated to `kCheckedWrites`: the level is a deliberate bump, not a weakened test; list each such edit in the task report).
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/replication/include/rune/replication/ReplicationMessages.h protocol/schema/rune-internal-ring.xml \
          protocol/include/rune_protocol_internal_sbe broker/libs/node/include/rune/node/WorkerOp.h \
          broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/AdminChannels.h \
          broker/libs/node/include/rune/node/AdminWorker.h broker/libs/node/include/rune/node/BrokerNode.h \
          broker/libs/node/src/BrokerNode.cpp broker/libs/node/include/rune/node/ClusterFeatureLevelFanout.h \
          broker/libs/node/CMakeLists.txt broker/libs/node/tests/node/CheckedWritesFeatureLevelTest.cpp \
          broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp broker/libs/node/tests/node/SecurityRingCodecTest.cpp
  git commit -m "feat(node): MetadataFeature::kCheckedWrites = 4 and its propagation to every worker and the admin thread" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 3: `WriteState.h`: the write spec, `checkTypedBatch`, the matrices and the model

**Files:**
- Create: `broker/libs/node/include/rune/node/WriteState.h`, `broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp`, `broker/libs/node/tests/node/CheckedWritesValidationMatrixTest.cpp`, `broker/libs/node/tests/support/CheckedWritesBatches.h`, `broker/libs/node/tests/support/CheckedWritesValidationModel.h`
- Modify: `protocol/include/rune/protocol/BatchRebuild.h:17-23` and `:66-73` (`RecordSpan::key`, P9), `broker/libs/node/CMakeLists.txt` (add the two test files to `rune_checked_writes_tests`; link `rune_catalog`)

**Interfaces:**
- Consumes: Task 1 (`validateUtf8`, `StringCheck`, `isTopicKeyType`).
- Produces (all in `namespace rune::node`, `WriteState.h`):
  ```cpp
  inline constexpr uint16_t kNoKeyField  = 0xFFFF;
  inline constexpr uint32_t kNoKeyOffset = 0xFFFFFFFF;
  struct TopicWriteSpecWire {            // the WriteSpecUpdate tail; in-process, later parts append fields
      int32_t topicId = -1; uint16_t keyFieldId = kNoKeyField; rune::codec::PrimType keyType = rune::codec::PrimType::Int32;
      uint8_t keyWidth = 0; bool legacyValidate = false;
      bool operator==(const TopicWriteSpecWire&) const = default;
  };
  inline constexpr std::size_t kTopicWriteSpecWireLen = 9;   // [topicId i32][keyFieldId u16][keyType u8][keyWidth u8][flags u8: bit0 legacyValidate]
  std::size_t encodeTopicWriteSpec(const TopicWriteSpecWire& w, std::span<uint8_t> out) noexcept;   // 0 if out is too small
  [[nodiscard]] std::optional<TopicWriteSpecWire> decodeTopicWriteSpec(std::span<const uint8_t> in) noexcept;
  [[nodiscard]] std::optional<TopicWriteSpecWire> writeSpecWireOf(const rune::catalog::TopicObject& t);   // nullopt = untyped; cold
  struct SpecVersion { rune::codec::Validator validator; rune::codec::Descriptor descriptor; uint32_t keyValueOffset = kNoKeyOffset; };
  struct TopicWriteSpec {
      int32_t topicId = -1; uint16_t keyFieldId = kNoKeyField; rune::codec::PrimType keyType = rune::codec::PrimType::Int32;
      uint8_t keyWidth = 0; bool legacyValidate = false;
      [[nodiscard]] const SpecVersion*              versionFor(uint16_t version) const noexcept;    // nullptr = not installed
      [[nodiscard]] const rune::codec::Validator*   validatorFor(uint16_t version) const noexcept;  // C7
      [[nodiscard]] const rune::codec::Descriptor*  descriptorFor(uint16_t version) const noexcept; // C7
      [[nodiscard]] uint16_t maxVersion() const noexcept;
      [[nodiscard]] bool hasFacts() const noexcept;
      void installVersion(uint16_t version, rune::codec::Descriptor materialized);   // cold; recomputes keyValueOffset
      void setFacts(const TopicWriteSpecWire& w);                                     // cold; recomputes every keyValueOffset
  private:
      std::vector<std::unique_ptr<SpecVersion>> versions_;   // index = version; grown only here (cold)
      uint16_t maxVersion_ = 0; bool hasFacts_ = false;
  };
  struct PartitionWriteState { const TopicWriteSpec* spec = nullptr; };   // 04/06/11 append members (C7)
  enum class TypedCheck : uint8_t { Accept, Reject, VersionAhead };
  [[nodiscard]] TypedCheck checkTypedBatch(const TopicWriteSpec& spec, std::span<const uint8_t> batch, bool checkedLevel) noexcept;
  ```
  `RecordSpan` gains `std::string_view key;` (filled by `forEachRecordSpan`; empty when `klen <= 0`).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesBatches.h`: `matrixDescriptor()` (the M1 descriptor), `specFor(Entity, bool level4)`, `TypedRecord{key bytes, value bytes}` builders `validRecord(id, version)`, `withVersion`, `withBlockLength`, `withTruncatedVar`, `withEnum`, `withBio(bytes)`, `withName(bytes16)`, `withBlob(bytes)`, `withKey(bytes)`, `tombstone(keyBytes)`, and `batchOf(records)` (a `RecordBatchBuilder` batch, CRC correct) plus `withLyingKeyLength(batch, recordIndex)`.
  - `CheckedWritesWriteStateTest.cpp`:
    - `CheckedWritesWriteState.WireRoundTripsAndRefusesShortInput`: encode → 9 bytes → decode equal; 8 bytes → nullopt; `encodeTopicWriteSpec` into 8 bytes → 0.
    - `CheckedWritesWriteState.SpecOfACatalogTopic`: `writeSpecWireOf` for an untyped `TopicObject` → nullopt; typed key `id uint64` validate=1 → `{keyFieldId = id.tag, UInt64, 8, legacyValidate true}`; key `name string(16)` → width 16; key `bio string` → `keyWidth 0` (P4); keyless → `kNoKeyField`; a renamed key field (ALTER RENAME) still resolves by the catalog's `keyField`.
    - `CheckedWritesWriteState.VersionsAreDenseAndLookupIsExact`: install v1 and v3 → `versionFor(2) == nullptr`, `maxVersion() == 3`, `validatorFor(1) != nullptr`, `descriptorFor(3)->version == 3`.
    - `CheckedWritesWriteState.KeyOffsetFollowsEachVersionsLayout`: v1 `{a int32, id uint64}` and v2 = v1 plus an appended `b int64` → `keyValueOffset == 4 + 4` in both; a version descriptor without the key field → `kNoKeyOffset`, and its records Reject.
    - `CheckedWritesWriteState.FactsAfterVersionsRecomputeOffsets`: installVersion then setFacts → offsets set; setFacts then installVersion → same offsets.
    - `CheckedWritesWriteStateRegression.LyingFramingIsRejectedInBoundsRegression`: a key length past the end, a value length past the end, `recordCount` one larger than the records present, a record length of 0 → `Reject` each (ASan clean).
    - `CheckedWritesWriteState.FirstNonAcceptedRecordDecides`: `[VersionAhead record, malformed record]` → VersionAhead; `[malformed, VersionAhead]` → Reject.
    - `CheckedWritesWriteState.BelowTheLevelWithoutLegacyAcceptsAnything`: TypedPreUpgrade, L3, garbage bytes → Accept without touching the records.
    - `CheckedWritesValidationFuzzRegression.RandomMutationsAgreeWithTheModel`: S1.
  - `CheckedWritesValidationMatrixTest.cpp`: `CheckedWritesValidationMatrix/Cell` (M1, 360 cells), `CheckedWritesWireKeyMatrix/Cell` (M2, 84 cells), `CheckedWritesValidationMatrixCountRegression.CellCountsArePinned`. Each cell asserts `checkTypedBatch(spec, batch, level4) == expect(cell)` and names itself `V07_TypedLegacy_L4_Last`.
  - `CheckedWritesValidationModel.h`: `TypedCheck referenceCheck(const TopicWriteSpec&, std::span<const uint8_t>, bool level4)` written independently of `WriteState.h`: its own bounded byte cursor over the batch, a code-point decoder for UTF-8, field reads through `descriptorFor(version)` offsets, rules P1–P5 in the M1 order.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `rune/node/WriteState.h` not found.
- [ ] **Step 3: Implement `WriteState.h`.** `writeSpecWireOf` deserializes `t.schema->evolvingBlob`, finds the live field named `t.schema->keyField`, and sets `keyWidth = isTopicKeyType(type, fieldWidth(f)) ? fieldWidth(f) : 0`. `installVersion` builds `Validator(materialized)` and sets `keyValueOffset = 4 + f.offset` for the field whose `id == keyFieldId` (when facts are known). `checkTypedBatch` (hot, `noexcept`, no allocation):

  ```cpp
  if (!checkedLevel && !spec.legacyValidate) return TypedCheck::Accept;                      // P5
  const auto check = checkedLevel ? rune::codec::StringCheck::Utf8 : rune::codec::StringCheck::None;
  const bool keyRule = checkedLevel && spec.keyFieldId != kNoKeyField && spec.keyWidth != 0;
  TypedCheck verdict = TypedCheck::Accept;
  const bool walked = rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan& r) {
      if (verdict != TypedCheck::Accept) return;                                              // first non-accept decides
      if (r.value.empty()) {                                                                  // P1: tombstone (empty or null)
          if (spec.keyFieldId == kNoKeyField) verdict = TypedCheck::Reject;
          else if (keyRule && r.key.size() != spec.keyWidth) verdict = TypedCheck::Reject;
          return;
      }
      if (r.value.size() < 4) { verdict = TypedCheck::Reject; return; }
      const uint16_t v = le16(r.value.data() + 2);
      const SpecVersion* sv = spec.versionFor(v);
      if (sv == nullptr) { verdict = (v > spec.maxVersion()) ? TypedCheck::VersionAhead : TypedCheck::Reject; return; }   // P2
      if (!sv->validator.validate(asBytes(r.value), check).ok) { verdict = TypedCheck::Reject; return; }
      if (keyRule && (sv->keyValueOffset == kNoKeyOffset || r.key.size() != spec.keyWidth ||
                      std::memcmp(r.key.data(), r.value.data() + sv->keyValueOffset, spec.keyWidth) != 0))
          verdict = TypedCheck::Reject;
  });
  return walked ? verdict : TypedCheck::Reject;
  ```
  `version 0` has no entry and is never above `maxVersion()`, so it is a Reject. `forEachRecordSpan` gains the key view (`s.key = std::string_view(batch + keyStart, klen)` before skipping it).
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_protocol_tests && ctest --test-dir build/root -R 'CheckedWrites|BatchRebuild|FilterRebuild' -j14 --output-on-failure`. Expected: PASS, 360 + 84 matrix cells.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/WriteState.h protocol/include/rune/protocol/BatchRebuild.h broker/libs/node/CMakeLists.txt \
          broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp broker/libs/node/tests/node/CheckedWritesValidationMatrixTest.cpp \
          broker/libs/node/tests/support/CheckedWritesBatches.h broker/libs/node/tests/support/CheckedWritesValidationModel.h
  git commit -m "feat(node): TopicWriteSpec / PartitionWriteState and checkTypedBatch (bounded walk, UTF-8, wire key = key field, tombstones, retriable version-ahead); validation matrices and model" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 4: The worker: `WriteSpecUpdate`, `RcSlot::write`, `writeOf_` and the produce call site

**Files:**
- Create: `broker/libs/node/tests/node/CheckedWritesWorkerTest.cpp`
- Modify: `protocol/schema/rune-internal-ring.xml` (`WRITE_SPEC_UPDATE` 27, `TailKind WRITE_SPEC` 12) + regenerated SBE; `broker/libs/node/include/rune/node/WorkerOp.h:151` (`WriteSpecUpdate = 27`; the tail reuses `schemaUpdateBlob`); `broker/libs/node/include/rune/node/Worker.h`:
  - ctor `:622-623` (drop the two `reserve`s, `writeSpecs_.reserve(kValidatorsReserve)`);
  - `hasValidatorForTest` `:1700-1707` (same name and meaning, over `writeSpecs_`);
  - `enqueueOp` tail switch `:1258-1265` (`WriteSpecUpdate` → `TailKind::WRITE_SPEC` from `schemaUpdateBlob`);
  - dispatch `:2643-2650` (`WriteSpecUpdate` → `executeWriteSpecUpdateWire_`), `TopicDropped` `:2658`;
  - `executeSchemaUpdateWire_` `:3128-3157` (installs into `writeSpecs_`);
  - delete `schemaRejects_` `:3873-3923` and `validators_` / `validatedTopics_` / `packValidatorKey_` `:5890-5926`;
  - `RcSlot` `:6206-6211`, `rcById_` `:6328` (+ `writeById_`), `rcOf_` `:6350` (+ `writeOf_`), `rebuildRcSlots_` `:6364-6400`;
  - `produceCore_` `:8252-8259`.
  
  `broker/libs/node/tests/integration/ProduceValidationTest.cpp` (harness `:236-330`; every test also sends the spec); `broker/libs/node/CMakeLists.txt:759-769` (add `tests/support/AllocWindow.cpp` to `rune_produce_validation_tests`); `broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp`

**Interfaces:**
- Consumes: Task 2 (`checkedWritesEnabled_()`), Task 3 (everything in `WriteState.h`).
- Produces:
  ```cpp
  // WorkerOp::Type::WriteSpecUpdate = 27: topicId = the typed topic; tail (TailKind WRITE_SPEC) = encodeTopicWriteSpec bytes, carried in schemaUpdateBlob. Cold; never ownership-gated.
  std::unordered_map<int32_t, std::unique_ptr<TopicWriteSpec>> Worker::writeSpecs_;   // stable addresses (C7)
  struct RcSlot { …; std::unique_ptr<PartitionWriteState> write; };                    // appended (C7)
  std::vector<PartitionWriteState*> Worker::writeById_;                                 // dense by registry id, beside rcById_
  [[nodiscard]] PartitionWriteState* Worker::writeOf_(const rune::replication::PartitionReplica*) const noexcept;
  [[nodiscard]] const PartitionWriteState* Worker::writeStateForTest(int32_t topicId, int32_t partition) const noexcept;
  [[nodiscard]] uint64_t Worker::typedRejectsForTest() const noexcept;
  [[nodiscard]] uint64_t Worker::typedVersionAheadForTest() const noexcept;
  ```

- [ ] **Step 1: Write the failing tests** (`CheckedWritesWorkerTest.cpp` in `rune_checked_writes_tests`; harness = `ProduceValidationTest.cpp`'s `WorkerHarness` pattern (disk `Log`, one leader `PartitionReplica`, `setOwnedDirect`), plus helpers `sendSpec(TopicWriteSpecWire)`, `sendVersion(topicId, version, Descriptor)`, `sendLevel(4)`, `produce(batch, via = Op | Ingress) -> int16_t errorCode` driving `doWork` with a hard cap of 10 000 passes). Every W1 row:
  - `CheckedWritesWorker.UntypedTopicHasNoWriteStateAndAcceptsAnyBytes`: no spec; `writeStateForTest(t, 0) == nullptr`; a garbage value → errorCode 0.
  - `CheckedWritesWorker.SpecBeforeOwnershipCreatesTheStateOnOwnership`: spec + v1 first, then `setOwnedDirect` → state non-null, `state->spec == hasValidatorForTest`'s spec; invalid record → 24.
  - `CheckedWritesWorker.SpecAfterOwnershipAttachesToOwnedPartitions`: owned first: garbage → 0; then spec + v1 → state non-null; garbage → 24.
  - `CheckedWritesWorkerRegression.VersionAheadIsRetriableUntilInstalledRegression`: v1 installed, a v2 record → 2 (`NotLeader`), `typedVersionAheadForTest() == 1`; install v2 → the same batch → 0; a version-0 record → 24.
  - `CheckedWritesWorkerRegression.LevelRaiseStartsFullValidationRegression`: spec `legacyValidate = false`; level 3: a record with invalid UTF-8 → 0 and a wrong-width key → 0; `sendLevel(4)` → both → 24; an earlier-accepted record still reads back from the log unchanged.
  - `CheckedWritesWorker.FeatureLevelNeverLowers` (worker side of Task 2's test, with a produce at the end → still 24).
  - `CheckedWritesWorker.RoleChangesKeepTheWriteState`: BecomeFollower then BecomeLeader ops on the same replica → `writeStateForTest` returns the same pointer.
  - `CheckedWritesWorker.NewIncarnationGetsAFreshState`: ownership removed (`setOwnedDirect({})`) → nullptr; re-added with a new replica of the same (topic, partition) → a non-null state whose `spec` is the same `TopicWriteSpec*`.
  - `CheckedWritesWorker.TopicDropReleasesSpecAndStates`: `TopicDropped` op → `hasValidatorForTest(t) == false`, `writeStateForTest == nullptr`; a produce → `UnknownTopic` (7) once ownership is gone, as today (`Worker.h:8167`).
  - `CheckedWritesWorker.MixedVersionBatchValidatesEachRecordByItsVersion`: v1+v2 installed; `[v1 valid, v2 valid]` → 0; `[v1 valid, v2 with a v1 blockLength]` → 24.
  - `CheckedWritesWorker.LateVersionGetsItsKeyOffset`: spec and v1 `{a int32, id uint64}` installed, then v2 (ALTER ADD `b int64`, appended) arrives after the spec → at level 4 a v2 record whose wire key equals its `id` bytes → 0, a differing key → 24.
  - `CheckedWritesWorkerRegression.TombstoneOnValidatedTopicIsAcceptedRegression`: `legacyValidate`, level 3 and level 4: `tombstone(8-byte key)` → 0 (fails today with 24, `Worker.h:3905`).
  - `CheckedWritesWorker.ControlRecordsAreNeverValidated`: an `AppendControlRecord` COMMIT marker on the typed partition → `ControlRecordAppended`.
  - `ProduceValidationTest.cpp`: the harness gains `enqueueWriteSpec(topicId, keyField, legacyValidate)` and every existing test calls it after `enqueueSchemaUpdate` with `legacyValidate = true` (the old `validate = true` meaning); `ProduceValidation.SchemaUpdate_Regression_NoValidator_FallsThrough` sends neither and still passes. New: `CheckedWritesValidationRegression.TypedProduceAllocatesNothingRegression`: level 4, 200 warm-up produces, then an `AllocWindow` around 1 000 produces alternating valid / invalid-UTF-8 / version-ahead records → `count() == 0`.
  - `WorkerOpWireRegression.WriteSpecUpdateRoundTripsRegression`: type 27, tail kind `WRITE_SPEC`, 9 tail bytes equal.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_produce_validation_tests rune_node_tests`. Expected: compile errors (`WorkerOp::Type::WriteSpecUpdate`, `writeStateForTest`).
- [ ] **Step 3: Implement.**
  - `executeSchemaUpdateWire_`: deserialize as today, `spec = getOrCreate_(topicId)`, `spec->installVersion(version, std::move(desc))`; same logging. `getOrCreate_` emplaces `std::make_unique<TopicWriteSpec>()` with `topicId` set.
  - `executeWriteSpecUpdateWire_` (`[[gnu::cold]]`): `decodeTopicWriteSpec(tail)`; malformed → `spdlog::error`, return. `spec->setFacts(w)`; if `w.keyFieldId != kNoKeyField && w.keyWidth == 0`, log once per topic at `warn` ("topic {} key field cannot be a topic key; wire-key check skipped (pre-existing topic)"). Then `attachWriteStates_(topicId)`: for every `RcSlot` whose replica's `topicId()` matches and whose `write` is null, `write = std::make_unique<PartitionWriteState>(PartitionWriteState{spec})`; then `rebuildWriteById_()`.
  - `rebuildRcSlots_`: move `old.write` with `old.rc` (same condition: same replica and incarnation); a new slot gets a state when `writeSpecs_` has the topic and `hasFacts()`; then `rebuildWriteById_()` (same shape as the `rcById_` loop at `:6396-6398`).
  - `writeOf_`: the exact shape of `rcOf_` (`:6350-6357`) over `writeById_` and `sl.write`.
  - `TopicDropped`: after `markTopicDropped_`, null every slot's `write` for that topic, `rebuildWriteById_()`, then erase `writeSpecs_[topicId]` (states first, so no dangling `spec`).
  - `produceCore_`, replacing `:8252-8259` (same position, before `acksAll`):
    ```cpp
    if (PartitionWriteState* ws = writeOf_(replica); ws != nullptr) [[unlikely]] {
        const TypedCheck c = checkTypedBatch(*ws->spec, rbSpan, checkedWritesEnabled_());
        if (c != TypedCheck::Accept) [[unlikely]] {
            if (c == TypedCheck::Reject) ++typedRejects_; else ++typedVersionAhead_;
            spdlog::debug("Worker {}: typed batch refused ({}) topicId={} partition={}", workerId_,
                          c == TypedCheck::Reject ? "SchemaValidationFailed" : "version ahead, NotLeader", f.topicId, f.partition);
            ret.errorCode = static_cast<int16_t>(c == TypedCheck::Reject ? pc::ErrorCode::SchemaValidationFailed : pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return 1;
        }
    }
    ```
    Untyped partitions pay `writeOf_` (one indexed load) and the predicted branch. `spdlog::debug` formats nothing at the default level, so the reject path stays allocation-free.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_produce_validation_tests rune_node_tests rune_shard_mode_tests && ctest --test-dir build/root -R 'CheckedWrites|ProduceValidation|WorkerValidator|WorkerOpWire|ShardMode|F15Eos|WorkerDroppedTopic' -j14 --output-on-failure`. Expected: PASS. `grep -n "validatedTopics_\|schemaRejects_\|packValidatorKey_" broker/libs/node/include/rune/node/Worker.h` → no hits.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/schema/rune-internal-ring.xml protocol/include/rune_protocol_internal_sbe broker/libs/node/include/rune/node/WorkerOp.h \
          broker/libs/node/include/rune/node/Worker.h broker/libs/node/CMakeLists.txt broker/libs/node/tests/node/CheckedWritesWorkerTest.cpp \
          broker/libs/node/tests/integration/ProduceValidationTest.cpp broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp
  git commit -m "feat(node): per-partition write state next to RcSlot; WriteSpecUpdate; every typed topic validates through writeOf_ (validators_/validatedTopics_ removed); typed tombstones pass" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Checkpoint A (after Task 4)

- [ ] `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure` → all pass.
- [ ] `cmake --build build/sub-codec -j14 && ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden|CheckedWrites' --output-on-failure` → all pass.
- [ ] `cmake --build build/asan -j14 --target rune_checked_writes_tests rune_produce_validation_tests && ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/asan -R 'CheckedWrites|ProduceValidation' --output-on-failure` → pass, no report (Review Focus 1).
- [ ] `cmake --build build/tsan -j14 --target rune_checked_writes_tests && ctest --test-dir build/tsan -R CheckedWrites --output-on-failure` → pass.
- [ ] A failure is bisected over the commits since the base; each fix gets a `Regression` test.

---

## Phase 2 — Control plane and DDL

### Task 5: Every typed topic's spec reaches every worker before the topic serves

**Files:**
- Create: `broker/libs/node/tests/node/CheckedWritesSpecFanoutTest.cpp`
- Modify: `broker/libs/node/include/rune/node/SchemaFanout.h:28-100`; `broker/libs/node/include/rune/node/NodeConfig.h:321` (`testHoldWriteSpecPasses`, next to `testSupportedFeatureLevel`); `broker/libs/node/tests/node/BrokerNodeTest.cpp`; `broker/libs/node/include/rune/node/AdminChannels.h:129-171` (`kA2cWriteSpec = 26`, `writeSpecWireLen`, `writeWriteSpec`, `decodeWriteSpec`); `broker/libs/node/include/rune/node/BrokerNode.h:1102-1118` (`postCatalogValidator_` for every typed topic), new `postCatalogWriteSpec_`, `:1128-1161` (`catalogSink_` order), members near `:2668` (`pendingCreatesAwaitingSpec_`); `broker/libs/node/src/BrokerNode.cpp:4980-5001` (dispatch `kA2cWriteSpec`, service the gate after the pump), `:5656` (`runCatalogCreate_` gate), `:5679` (`dropRuntimeTopic_` forgets a parked create); `broker/libs/node/src/CatalogEffectFrames.cpp:55-73`; `broker/libs/node/tests/node/SecurityRingCodecTest.cpp`

**Interfaces:**
- Consumes: Task 3 (`writeSpecWireOf`, `encodeTopicWriteSpec`), Task 4 (`WriteSpecUpdate`).
- Produces:
  ```cpp
  // AdminChannels.h: kA2cWriteSpec = 26, payload = encodeTopicWriteSpec bytes (kTopicWriteSpecWireLen).
  [[nodiscard]] constexpr std::size_t writeSpecWireLen() noexcept;   // kTopicWriteSpecWireLen
  void SchemaFanout::postWriteSpec(int32_t topicId, std::vector<uint8_t> specBytes);   // same FIFO as post()
  [[nodiscard]] bool SchemaFanout::pendingFor(int32_t topicId) const noexcept;          // any undelivered item of topicId
  void BrokerNode::postCatalogWriteSpec_(const rune::catalog::TopicObject& t);           // admin thread; no-op for untyped
  int BrokerNode::serviceCreatesAwaitingSpec_();                                         // control thread
  uint32_t NodeConfig::testHoldWriteSpecPasses = 0;                                      // TEST-ONLY; 0 = off
  void SchemaFanout::setHoldWriteSpecPassesForTest(uint32_t passes) noexcept;            // set once at BrokerNode start
  ```

- [ ] **Step 1: Write the failing tests.** (Broker-level tests read worker state only through `node().onOwnerThreadForTest`, as in Task 9.)
  - `CheckedWritesSpecFanoutTest.cpp` (`rune_checked_writes_tests`):
    - `CheckedWritesSpecFanout.WriteSpecRidesTheSameFifo`: `post(t, 1, blobV1)`, `postWriteSpec(t, spec)`, `post(t, 2, blobV2)` on a 2-worker pool → each worker's op ring yields `SchemaUpdate v1`, `WriteSpecUpdate`, `SchemaUpdate v2`, in that order.
    - `CheckedWritesSpecFanout.PendingForTracksUndeliveredItems`: worker 1 full → `pendingFor(t) == true`, `pendingFor(other) == false`; drain and pump → false.
    - `CheckedWritesSpecFanoutRegression.SpecFrameRoundTripsRegression` (in `SecurityRingCodecTest.cpp`: `kA2cWriteSpec` encode/decode, 8-byte frame refused).
  - `CheckedWritesBrokerRegressionTest.cpp` (created here, added to `rune_client_broker_tests` at `broker/libs/node/CMakeLists.txt:1731`; Task 9 extends it), on `OneBroker("cw-order", OneBroker::Runtime::Shard)`:
    - `CheckedWritesBrokerRegression.TopicCreateWaitsForItsSpecOnEveryWorkerRegression`: `OneBroker` with the tweak `cfg.testHoldWriteSpecPasses = 2000` (TEST-ONLY knob below: the fanout keeps every `WriteSpecUpdate` undelivered for that many pumps after it is posted); `defineTopicAndWait` of a typed topic (2 partitions) returns OK only after the hold; at that moment every worker's `writeStateForTest(topic, p)` is non-null for each partition it owns, and the first raw produce of garbage → `SchemaValidationFailed`. Mutation: without the P7 gate the topic serves during the hold and that produce is accepted.
    - `CheckedWritesBrokerRegression.DroppedWhileAwaitingSpecNeverBuildsRegression`: the same hold; `dropTopicAndWait` before it ends → after it ends `node().catalogTopicServesForTest(id)` stays false (`BrokerNode.h:1743`), every worker's `writeStateForTest(id, p)` is nullptr and no worker `hasValidatorForTest(id)`.
  - `broker/libs/node/tests/node/BrokerNodeTest.cpp` (`rune_broker_node_tests`, next to the validator fan-out test at `:1463-1571`): `CheckedWritesSpecFanoutRegression.EveryTypedTopicGetsValidatorsRegression`: its raw wire client defines a typed topic with `/*validate=*/false` → every worker `hasValidatorForTest(topicId)` and `writeStateForTest(topicId, p)` non-null for its partitions (today no validator is posted for such a topic).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_client_broker_tests rune_node_tests rune_broker_node_tests`. Expected: compile errors (`postWriteSpec`, `pendingFor`, `kA2cWriteSpec`, `testHoldWriteSpecPasses`).
- [ ] **Step 3: Implement.**
  - `SchemaFanout::Pending` gains `WorkerOp::Type type` (`SchemaUpdate` or `WriteSpecUpdate`) and `uint32_t holdPasses`; `pump` sets `op.type` from it, keeps `clientVersion = version` for `SchemaUpdate` only, and copies the blob into `schemaUpdateBlob` for both. A `WriteSpecUpdate` posted while the test hold is set starts with `holdPasses = hold` and is decremented per `pump` instead of delivered (FIFO: later items wait behind it). `pendingFor` scans `pending_` (cold, tiny). `BrokerNode` calls `setHoldWriteSpecPassesForTest(config_.testHoldWriteSpecPasses)` where it builds the worker pool.
  - `postCatalogValidator_`: drop `|| !t.schema->validate` from the guard at `BrokerNode.h:1103`.
  - `postCatalogWriteSpec_`: `if (auto w = writeSpecWireOf(t))` encode into a 9-byte stack buffer and `postCatalog_(kA2cWriteSpec, writeSpecWireLen(), …)`.
  - `catalogSink_`: `fx.topicApplied` posts `postCatalogFilterSchema_(t)`, `postCatalogValidator_(t, 1)`, `postCatalogWriteSpec_(t)`, then `postCatalogTopicApplied_(…)` (the failure ring is FIFO, so the control thread queues the spec before it sees the topic). `fx.schemaChanged` posts `postCatalogValidator_(t, fromVersion)`, `postCatalogWriteSpec_(t)`, `postCatalogFilterSchema_(t)`. The repeated validator posts at create are idempotent on the worker (`installVersion` replaces).
  - Control dispatch (`BrokerNode.cpp`, before the `dispatchAdminToControl` fallback): `if (tid == kA2cWriteSpec) { if (auto w = decodeTopicWriteSpec({p, n})) schemaFanout_.postWriteSpec(w->topicId, std::vector<uint8_t>(p, p + n)); else <the rate-limited malformed log>; return; }`.
  - The gate (P7): `runCatalogCreate_` starts with `if (workerPool_ && schemaFanout_.pendingFor(spec.topicId)) { park (idempotent per topicId) in pendingCreatesAwaitingSpec_; return; }`. `serviceCreatesAwaitingSpec_()` runs right after `schemaFanout_.pump` (`BrokerNode.cpp:5000`) and calls `runCatalogCreate_` for each parked spec whose fanout drained. `dropRuntimeTopic_` erases a parked entry for the id first and, when it erased one, behaves like the pending-behind-drop branch (`:5687-5690`).
  - `CatalogEffectFrames.cpp:55`: size the validator frames for every typed topic (`if (t.schema)`), and add `tooLarge("the WriteSpec effect", …, writeSpecWireLen(), maxFrame)`. No new halt risk: the filter-schema frame (the evolving blob, never smaller than a materialized version) is already required for every typed topic.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_client_broker_tests rune_node_tests rune_broker_node_tests && ctest --test-dir build/root -R 'CheckedWritesSpecFanout|CheckedWritesBrokerRegression|SchemaFanout|CatalogApplier|CatalogEffect|BrokerNode|ClientDdl' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/SchemaFanout.h broker/libs/node/include/rune/node/AdminChannels.h \
          broker/libs/node/include/rune/node/BrokerNode.h broker/libs/node/src/BrokerNode.cpp broker/libs/node/src/CatalogEffectFrames.cpp \
          broker/libs/node/include/rune/node/NodeConfig.h broker/libs/node/tests/node/BrokerNodeTest.cpp \
          broker/libs/node/CMakeLists.txt broker/libs/node/tests/node/CheckedWritesSpecFanoutTest.cpp \
          broker/libs/node/tests/node/SecurityRingCodecTest.cpp broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
  git commit -m "feat(node): every typed topic posts validators and its write spec before the topic is built; the create waits for the fanout (no unvalidated window)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 6: DDL rules: key types, view sources, the feature rule table and the kind probe

**Files:**
- Create: `broker/libs/node/include/rune/node/TopicFeatureRules.h`, `broker/libs/node/tests/node/CheckedWritesDdlTest.cpp`
- Modify: `broker/libs/node/src/AdminWorker.cpp` (`buildDefine_` `:1090-1172`, `viewDef_` source loop `:1224-1233`, `submitGetSchema` `:586-612`); `protocol/include/rune/protocol/client/SchemaMessages.h:309`; `broker/libs/node/CMakeLists.txt` (add the test file to `rune_checked_writes_tests`)

**Interfaces:**
- Consumes: Task 1 (`isTopicKeyType`), Task 2 (`AdminWorker::clusterFeatureLevel`, unused by the rules here).
- Produces:
  ```cpp
  namespace rune::protocol::client { inline constexpr uint64_t kSchemaKindFingerprint = ~uint64_t{0} - 1; }   // P8
  namespace rune::node {
  enum class DbFeature : uint8_t { View, Filter, KeyIndex, AutoIncrement, Constraint };
  struct TopicShape { bool typed = false; bool keyed = false; bool transient = false; };
  [[nodiscard]] std::optional<std::string_view> featureRefusal(DbFeature f, const TopicShape& s) noexcept;   // nullopt = allowed
  [[nodiscard]] TopicShape topicShapeOf(const rune::catalog::TopicObject& t) noexcept;
  }
  ```
  The rule order is: untyped, then keyless, then transient. Messages (exact text):

  | Feature | untyped | keyless (typed) | transient (typed, keyed) |
  |---|---|---|---|
  | `View` | "view source must be a typed topic" | "view source needs a key" | allowed |
  | `Filter` | "filter needs a typed topic" | allowed | allowed |
  | `KeyIndex` | "key.index needs a typed topic" | "key.index needs a key" | "key.index needs a persisted topic" |
  | `AutoIncrement` | "autoincrement needs a typed topic" | "autoincrement needs a key" | "autoincrement needs a persisted topic" |
  | `Constraint` | "constraints need a typed topic" | "constraints need a key" | "constraints need a persisted topic" |

- [ ] **Step 1: Write the failing tests** (`CheckedWritesDdlTest.cpp`; DDL cells drive `CatalogDdlRig` (`tests/support/CatalogDdlRig.h`) with `becomeServingLeader()` first):
  - `CheckedWritesDdl.FeatureRuleTable` (M3, 30 cells, `TEST_P`), asserting the table above; `CheckedWritesDdl.FilterMessageMatchesTheGateway`: `featureRefusal(Filter, untyped) == rune::codec::describe({rune::codec::FilterErrorKind::UntypedTopic})`.
  - `CheckedWritesDdl.KeyFieldTypesAtDefine` (`TEST_P`, one typed topic per row, `DefineArgs{.typed = true, .keyField = "k"}` with a descriptor whose field `k` has the row's type): `uint16`, `int32`, `uint32`, `int64`, `uint64`, `string(1)`, `string(16)` → code 0; `string(17)`, `string`, `bytes`, `float64`, `bool`, enum → code 23 and `detail` starting `"key field 'k' is "` and ending `": a topic key is an integer or string(N) with N <= 16"`.
  - `CheckedWritesDdlRegression.AdoptionAppliesTheKeyRuleRegression`: an untyped topic, then a schema adoption whose key is `string` → 23, the topic stays untyped.
  - `CheckedWritesDdl.ViewSourceKinds`: `createView` over an untyped topic → code 33, detail "view source must be a typed topic"; over a keyless typed topic → 35, detail "view source needs a key"; over a missing name → 33, empty detail; over a keyed typed topic → 0.
  - `CheckedWritesDdl.KindProbe`: `getSchema(name, kSchemaKindFingerprint)`: typed → code 0, version 1, the latest materialized blob (equal to `getSchema(name, 0).blob`); untyped (`partitions = 3`) → code 0, version 0, empty blob, `partitionCount == 3`, `topicId` its id; unknown → 7.
  - `CheckedWritesDdlRegression.ExistingRequestsAnswerAsBeforeRegression`: `getSchema(untyped, 0)` → 7 (unchanged); `getSchema(typed, kSchemaLineageFingerprint)` → the evolving blob (unchanged).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `TopicFeatureRules.h` not found.
- [ ] **Step 3: Implement.**
  - `featureRefusal`: a `switch` over the table; `topicShapeOf`: `typed = t.schema.has_value()`, `keyed = typed && !t.schema->keyField.empty()`, `transient = t.persistence == Persistence::Transient`.
  - `buildDefine_`: inside the `!m.blob.empty()` block, after deserialize, when `!m.keyField.empty()` find the field named `m.keyField` (a missing one stays the applier's Precondition, `ApplyV1.cpp:132`); `!isTopicKeyType(f.type, fieldWidth(f))` → `return Rejected(reject(kInvalidSchema, "key field " + quotedName(m.keyField) + " is " + keyTypeSpelling(f) + ": a topic key is an integer or string(N) with N <= 16"))`, where `keyTypeSpelling` (anonymous namespace) spells the QL type (`int32`, `int64`, `uint16`, `uint32`, `uint64`, `float64`, `bool`, `enum`, `string`, `bytes`, `string(N)`). It runs before both the adoption and the create branch. It is a leader build rule, never an apply rule, so existing catalogs replay unchanged.
  - `viewDef_`: for a source that exists with `!t->schema` → `reject(kViewSourceNotFound, std::string(*featureRefusal(View, topicShapeOf(*t))))`; for a keyless one → `reject(kViewKeyInvalid, …)`; a missing source keeps `reject(kViewSourceNotFound)`.
  - `submitGetSchema`: before the lineage branch, `if (msg.requestedFingerprint == kSchemaKindFingerprint)`: `reg_.byName` hit → answer exactly as fingerprint 0; else `applier_.state().topicByName(msg.name)` untyped hit → `respondGet(session, corr, t->topicId, 0, t->partitions, {}, kOK)`; else `kUnknownTopic`.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_ddl_conformance_tests rune_ddl_stress_tests rune_create_view_tests rune_node_tests && ctest --test-dir build/root -R 'CheckedWritesDdl|DdlOccupant|DdlStress|DdlHarness|CreateView|AdminWorker|SchemaRegistry' -j14 --output-on-failure`. Expected: PASS. An existing test that builds a view over a keyless typed source, or defines a typed topic keyed on a non-key type, now fails by design: give its source a key / change its key type, and list each edit in the task report as a semantics change (spec §4), not a weakened test.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/TopicFeatureRules.h broker/libs/node/src/AdminWorker.cpp \
          protocol/include/rune/protocol/client/SchemaMessages.h broker/libs/node/CMakeLists.txt \
          broker/libs/node/tests/node/CheckedWritesDdlTest.cpp
  git commit -m "feat(admin): topic key types refused at define, view sources must be typed and keyed, one feature rule table (SD9 messages), the GetTopicSchema kind probe" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 7: Rune QL and the CLI: `partitions` optional, the `raw` marker, `--validate` gone

**Files:**
- Modify: `ql/src/Parser.cpp:5-12` (header comment) and `:604-620`; `ql/tests/ParserTest.cpp`; `client-cpp/src/cli/QlEngine.h:59,98-101`; `client-cpp/src/cli/QlEngine.cpp:2,241,336-345,631`; `client-cpp/src/cli/CatalogCommands.cpp:137-176,205,242,250`; `client-cpp/src/cli/Console.cpp:4,92,104,138,148`; `client-cpp/src/cli/Cli.cpp:90`; `client-cpp/tests/RuneCliOfflineTest.cpp`, `client-cpp/tests/RuneConsoleOfflineTest.cpp`, `client-cpp/tests/RuneCliCatalogTest.cpp`

**Interfaces:**
- Consumes: nothing from earlier tasks. At `QlEngine.cpp:631` this task passes `!d.descriptorBlob.empty()` where `s.validate` was; Task 8's sweep then deletes that argument, so each task compiles on its own.
- Produces: `PDefine { ql::DefineTopicPlan plan; }` (no `validate`); `PrepareOptions { uint16_t expectedVersion = 0; }`; catalog text lines `  kind raw` / `  kind typed`.

- [ ] **Step 1: Write the failing tests.**
  - `ParserTest.cpp`: `CheckedWritesQl.PartitionsDefaultToOne`: `topic events;` → `partitions == 1`, fields empty; `topic p { id uint64 } key id;` → 1; `topic p { id uint64 } partitions 4 key id;` → 4; `topic p partitions 0;` → still "partitions must be >= 1"; `transient topic t;` → 1. `CheckedWritesQl.ReplicasAndWithWithoutPartitions`: `topic e replicas 3 with (tier = leader);` parses with `partitions == 1`, `replicas == 3`.
  - `RuneCliOfflineTest.cpp`: `CheckedWritesCli.ValidateFlagIsGoneRegression`: `rune ql --validate -e "topic e;"` → exit 2, stderr names `--validate` as an unknown option; `rune console --validate` → the same. `CheckedWritesCli.PlanMarksRawTopics`: `rune ql --dry-run -e "topic e;"` → stdout `topic e partitions 1 replicas 0 persistence disk raw\n`; a typed topic prints no `raw` and no `validate` token.
  - `RuneCliCatalogTest.cpp`: `CheckedWritesCli.CatalogShowsKind`: `formatTopicDefinition` of an untyped topic contains `"  kind raw\n"` and no `"(untyped"`; of a typed one `"  kind typed\n"`; `formatSchemaDefinition` contains no `"  validate "` line.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-ql -j14 --target rune_ql_tests && ctest --test-dir build/sub-ql -R CheckedWritesQl --output-on-failure` → FAIL ("expected 'partitions'"); `cmake --build build/sub-client-cpp -j14 --target rune_cli_offline_tests rune_cli_catalog_tests` then the `CheckedWritesCli` filter → FAIL.
- [ ] **Step 3: Implement.** Parser: `if (isKw("partitions")) { …today's parse… }` and otherwise keep `td.partitions = 1` (the `Ast.h:26` default). The header comment and the `ql.md` grammar line (Task 11) show `[ partitions <n> ]`. CLI: remove `--validate` from the `cmdQl` flag list and `Console.cpp` `kBool`, `PrepareOptions::validate`, `PDefine::validate`, the usage strings; `printPlan` appends ` raw` after the persistence word for an empty descriptor and drops the `(untyped)` line; `formatTopicDefinition` prints `  kind raw` / `  kind typed` after `persistence` and no longer prints `(untyped: no schema)`; `formatSchemaDefinition` drops its `validate` line.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-ql -R 'Parser|CheckedWritesQl|Script|Backends' --output-on-failure` and `ctest --test-dir build/sub-client-cpp -R 'RuneCli|RuneConsole|CheckedWritesCli' --output-on-failure`. Expected: PASS (expected-text edits in existing CLI tests that print `(untyped…)` or `validate` are the deliberate output change; list them).
- [ ] **Step 5: Commit.**
  ```bash
  git add ql/src/Parser.cpp ql/tests/ParserTest.cpp client-cpp/src/cli client-cpp/tests/RuneCliOfflineTest.cpp \
          client-cpp/tests/RuneConsoleOfflineTest.cpp client-cpp/tests/RuneCliCatalogTest.cpp
  git commit -m "feat(ql,cli): partitions defaults to 1; untyped topics show as raw; --validate removed (typed topics always validate)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Checkpoint B (after Task 7)

- [ ] The Checkpoint A commands, plus `ctest --test-dir build/sub-ql -R Regression --output-on-failure` and `ctest --test-dir build/sub-client-cpp -R Regression -LE latency -j14 --output-on-failure` → all pass.

---

## Phase 3 — Client, end to end, benches, docs

### Task 8: The client: no `validate`, `Key` never truncates, `KeyTooLong`, strict binding

**Files:**
- Modify: `client-cpp/include/rune/client/Key.h:52-71`; `client-cpp/include/rune/client/producer/Producer.h:302-324` (`KeyTooLong = 12`); `client-cpp/src/producer/ProducerImpl.cpp:413` (`trySubmit`), `:3562` / `:3623` (`claimRecord`, `claimBatch`), `throwForSendResult`; `client-cpp/include/rune/client/shard/ClientShard.h:99-120,305-307` (and its `.cpp`); `client-cpp/include/rune/client/Client.h:782-788,880-881,906-913,950-951` (drop `bool validate`), `:1058-1105` (`openTopicAndWait` messages), new `rawTopicAndWait`; `client-cpp/src/Client.cpp:1275-1305,2594-2635,4248-4252`; `client-cpp/include/rune/client/TypedTopic.h:221-240` (`fromSchema` message); every `defineTopic…` caller (compiler-guided: `client-cpp/{src,tests,examples}`, `broker/libs/*/tests`, `broker/benchmark`); new tests in `client-cpp/tests/client/KeyRoutingRegressionTest.cpp`, `client-cpp/tests/producer/ProducerSendTest.cpp`, `client-cpp/tests/client/OpenTopicGateTest.cpp`

**Interfaces:**
- Consumes: Task 6 (`kSchemaKindFingerprint`, the probe answers).
- Produces:
  ```cpp
  // Key.h
  explicit Key(std::span<const uint8_t> bytes) noexcept;   // > kMaxInline bytes → an over-long key: tooLong() true, bytes() empty
  template <class T> explicit Key(const T& pod) noexcept;   // static_assert(sizeof(T) <= kMaxInline, "…never truncated")
  [[nodiscard]] bool tooLong() const noexcept;
  static_assert(Key::kMaxInline == rune::codec::kMaxTopicKeyBytes);
  // Producer.h: SendResult::KeyTooLong = 12 — a key wider than Key::kMaxInline (05 adds "not the key field's width")
  // Client.h
  [[nodiscard]] TopicHandle rawTopicAndWait(std::string_view name, std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
  ```

- [ ] **Step 1: Write the failing tests.**
  - `KeyRoutingRegressionTest.cpp` (`rune_client_tests`): `CheckedWritesKeyRegression.SpanKeyIsNeverTruncatedRegression`: 16 bytes → `size() == 16`, `!tooLong()`; 17 bytes → `tooLong()`, `bytes().empty()`, `partition(8) == 0`; `CheckedWritesKey.PodKeyWiderThanSixteenDoesNotCompile` as a `static_assert(!std::is_constructible_v<Key, std::array<uint8_t, 17>>)` (the constructor is constrained with `requires (sizeof(T) <= kMaxInline)` so the trait is checkable); `std::array<uint8_t, 16>` constructible.
  - `ProducerSendTest.cpp` (`rune_invoker_tests`): `CheckedWritesKey.OverLongKeyIsRefusedEverywhere`: `trySend(topic, Key(span17), v)` → `SendResult::KeyTooLong`, nothing queued; `send(...)` throws `std::invalid_argument` whose `what()` contains "key is longer than 16 bytes"; `tryClaim(topic, Key(span17), 8)` → a refused claim with `result() == KeyTooLong`; `ClientShard::trySend` the same.
  - `OpenTopicGateTest.cpp` (`rune_open_topic_gate_tests`, the `GateGateway::FingerprintOverride` seam):
    - `CheckedWritesBind.TypedOpenOfAnUntypedTopicNamesTheRawTopic`: `T::kFingerprint` → UnknownTopic; `kSchemaKindFingerprint` → OK, empty blob → `openTopicAndWait<StubT_Match>` throws `std::runtime_error` containing "is an untyped (raw) topic".
    - `CheckedWritesBind.TypedOpenOfAMissingTopicSaysSo`: both → UnknownTopic → message contains "no topic 'characters'".
    - `CheckedWritesBind.TypedOpenFingerprintMismatchKeepsTheStaleMessage`: kind probe → OK + a blob → "generated type is stale".
    - `CheckedWritesBind.RawOpenOfATypedTopicIsRefused`: kind probe → OK + a blob → `rawTopicAndWait` throws containing "is a typed topic".
    - `CheckedWritesBind.RawOpenOfAnUntypedTopicReturnsItsHandle`: kind probe → OK, empty blob, topicId 42, partitionCount 3 → `TopicHandle{42, 3}`; the client's schema cache was not written (`version == 0` answers are not cached).
    - `CheckedWritesBind.DefineSendsValidateForTypedTopics`: the fake records the `DefineTopic` it receives: a typed define has `validate == true`, an untyped one `false`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_client_tests rune_invoker_tests rune_open_topic_gate_tests`. Expected: compile errors (`tooLong`, `KeyTooLong`, `rawTopicAndWait`).
- [ ] **Step 3: Implement.**
  - `Key(span)`: `> kMaxInline` sets `len_ = kTooLongLen` (0xFF) and leaves `buf_` zero; `bytes()`/`size()` return empty / 0 for it; `tooLong()` tests the sentinel. Update the header comment (no truncation anywhere).
  - The producer refuses an over-long key at one place per entry: `ProducerImpl::trySubmit`, `claimRecord`, `claimBatch` (the Direct variants are reached through them) and the `ClientShard` send path, each `if (key.tooLong()) [[unlikely]] return SendResult::KeyTooLong;` before any other work; `throwForSendResult(KeyTooLong)` throws `std::invalid_argument("Producer::send: key is longer than 16 bytes (Key::kMaxInline); a key is never truncated")`.
  - Remove `bool validate` from the six `defineTopic` / `defineTopicAndWait` overloads; `Client.cpp:1303` sets `msg.validate = !descriptorBlob.empty()` (P5). Then build every target of `build/root` and `build/sub-client-cpp`, fixing each call site the compiler names by deleting the argument (a `bool` never converts to the following `std::span` / `Persistence`, so no call silently changes meaning). `grep -rn "validate=" --include='*.cpp' client-cpp broker/libs ql` afterwards lists only raw-wire test clients (`defineTopicUntil`, `DefineTopicMsg.validate`), which keep the wire byte.
  - `rawTopicAndWait`: `getTopicSchemaAndWait(name, kSchemaKindFingerprint, timeout)`; `errorCode != 0` → throw "rawTopic: no topic '<name>'"; non-empty blob → throw "rawTopic: '<name>' is a typed topic; open it with openTopicAndWait<T> (the raw send path is for untyped topics only)"; else `TopicHandle{r.topicId, r.partitionCount > 0 ? r.partitionCount : 1}`.
  - `openTopicAndWait<T>` (both overloads): on `errorCode != 0`, probe the kind once; untyped → "openTopic: '<name>' is an untyped (raw) topic; a TypedTopic binds only to a typed topic (use rawTopicAndWait)"; unknown → "openTopic: no topic '<name>'"; typed → today's "stale" message.
  - `Client.cpp:4248`: write `topicSchemaCache_` only when `opt->version != 0`.
- [ ] **Step 4: Run.** `cmake --build build/sub-client-cpp -j14 && ctest --test-dir build/sub-client-cpp -R 'CheckedWrites|KeyRouting|TypedTopic|OpenTopic|ProducerSend|ProducerClaim|TypedClaim|ClientSchema' -j14 --output-on-failure`, then `cmake --build build/root -j14 && ctest --test-dir build/root -R 'ClientDdl|RuneCli|ClaimBroker|CheckedWrites' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git status --short   # expect only the files the sweep and this task touched (no build logs, no .superpowers/)
  git add client-cpp broker/libs broker/benchmark ql
  git commit -m "feat(client): Key never truncates (KeyTooLong), the validate parameter is gone (typed topics always validate), rawTopicAndWait, typed/raw bind refusals" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 9: The real-broker slice (e2e on the default shard runtime)

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` (created in Task 5)

**Interfaces:**
- Consumes: Tasks 1–8. Test seams: `OneBroker(tag, Runtime::Shard | Runtime::Legacy, tweak)` with `restart()`; `NodeConfig::testSupportedFeatureLevel` (`NodeConfig.h:321`) through a tweak that reads a captured `int16_t`; `ThreeNodes::startWith(tag, tweak)`, `stop(id)`, `startNode(id)`; `Worker::writeStateForTest`, `clusterFeatureLevelForTest`, `hasValidatorForTest` through `node().workerForTest(w)`, read only inside `node().onOwnerThreadForTest(topicId, p, …)` (`BrokerNode.h:1467`) exactly as `BrokerNodeTest.cpp:1565-1573` does (a direct read from the test thread is a TSan race); the `rune_cli_lib` `cmdQl` entry (as `RuneCliBrokerRegressionTest.cpp` uses it).
- Produces: nothing new.

- [ ] **Step 1: Write the tests** (each a B1 row; every wait is bounded; every produce uses an idempotent `Producer` with `Acks::All` and its completion's `errorCode`):
  - `RawSendIsRawOnUntypedAndValidatedOnTypedRegression`: `rawTopicAndWait("raw")`, `send(Key{}, 3 garbage bytes)` → Ok and a subscriber reads the bytes back; the same bytes on a typed topic `p` (`{id uint64, name string(16)} key id`) through a raw handle built from `defineTopicAndWait`'s id → `SchemaValidationFailed`.
  - `UnknownVersionIsRefusedRegression`: a valid `p` record re-stamped to version 0 → 24.
  - `PreUpgradeTopicValidatesAfterTheLevelRegression`: tweak level 3; `p` created; a record with `name = 61 80 …` → Ok; the worker level is 3; `level = -1`; `restart()`; await every worker `clusterFeatureLevelForTest() == 4` (bounded); the same record → 24; the first record still reads back.
  - `RawKeyOfTheWrongWidthIsRefusedRegression`: topic `q {id uint32, …} key id`: `Key(uint64_t{7})` with a value whose `id = 7` → 24; `Key(uint32_t{7})` → Ok; topic `s {name string(4)} key name`: key `"ab"` (2 bytes) → 24, key `"ab\0\0"` → Ok; a keyless typed topic: any key → Ok.
  - `TransactionOnTypedTopicCommitsRegression`: a transactional producer writes 3 valid records to `p` and commits; a `read_committed` subscriber sees 3 records (the COMMIT marker is not validated).
  - `Utf8RulesEndToEndRegression`: `string` with `F0 9F 98 80` → Ok; with `ED A0 80` → 24; `string(16)` with dirty padding → 24; `bytes` with `FF FE` → Ok.
  - `TypedTombstoneDeletesAndCompactsRegression`: topic `p` with `cleanup.policy = compact`, a small `segment.bytes` and `min.cleanable.dirty.ratio = 0`: write id 5, then `send(Key(uint64_t{5}), {})` → Ok; a 4-byte key tombstone → 24; roll and clean as `BrokerNodeTest.LogCleanerAgent_CompactsViaBrokerNode` does (`broker/libs/node/tests/node/BrokerNodeTest.cpp:523`, `onCleanerThreadForTest`) → a fresh subscriber from the start sees no live value for id 5.
  - `ViewOnUntypedOrKeylessSourceIsRefusedRegression`: `createViewAndWait` over `raw` → code 33, detail "view source must be a typed topic"; over a keyless typed topic → 35, "view source needs a key".
  - `TypedOpenOfUntypedTopicIsRefusedRegression` and `RawOpenOfTypedTopicIsRefusedRegression`: the Task 8 messages against the real broker.
  - `QlTopicWithoutPartitionsHasOneRegression`: `rune ql -e "topic solo;"` against `endpoint()` → exit 0; `rawTopicAndWait("solo").partitionCount == 1`.
  - `WideKeyFieldIsRefusedAtDefineRegression`: `topic w { k string(17) } key k;` through `rune ql` → nonzero exit, stderr contains "key field 'k' is string(17)".
  - `ThreeNodesNewLeaderValidatesRegression`: `ThreeNodes`, `p` with `replicas 3`; find and `stop` the leader of `(p, 0)`; after the new leader serves, an invalid record → 24 and a valid one → Ok.
  - `LegacyRuntimeValidatesRegression`: `OneBroker(…, Runtime::Legacy)`: invalid → 24, valid → Ok.
  - `BothIngressRoutesValidateRegression`: `TEST_P` over `NodeConfig::perWorkerIngressEnabled` ∈ {false (gateway → op ring → `executeProduceWire_`), true (client → worker ingress)} on the Shard runtime: invalid → 24, valid → Ok on both.
  - `RestartValidatesFromTheFirstProduceRegression`: `p` created, `restart()`, the first produce after reconnect is invalid → 24 (never Ok).
  - `AdoptionStartsValidationRegression`: untyped `a`, garbage → Ok; adopt a schema (`defineTopicAndWait("a", 1, "id", blob)`); await every owning worker's `writeStateForTest("a", 0) != nullptr`; garbage → 24.
  - `AlterThenProduceNewVersionSucceedsRegression`: `alterTopicAndWait` adds a column; immediately `openTopicAndWait<V2>` and send → Ok (a transient `NotLeader` for a version ahead is absorbed by the client retry; assert the completion is Ok and, through `typedVersionAheadForTest`, at most a few retries happened).
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R CheckedWritesBrokerRegression --output-on-failure`. Expected: PASS. A failure here is a defect in Tasks 1–8: fix it there with its own `Regression` test, never by relaxing the cell.
- [ ] **Step 3: Repeat for flakes.** `ctest --test-dir build/root -R CheckedWritesBrokerRegression --repeat until-fail:5 -j4 --output-on-failure`. Expected: PASS every round.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
  git commit -m "test(checked-writes): typed validation end to end on real brokers (shard default, legacy, three nodes, upgrade, restart, adoption)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 10: Benchmarks and the budget gate

**Files:**
- Modify: `broker/benchmark/ProduceValidateBench.cpp` (fixtures `SetupOn` / `SetupOff` and the two existing benches `:231-330`)

**Interfaces:**
- Consumes: Tasks 3, 4.
- Produces: `BM_CheckedWriteHook/CheckOnly1Record`, `/CheckOnly64Records`, `/UntypedPath`, `/TypedLegacyPath`, `/TypedCheckedPath`.

- [ ] **Step 1: Write the benches.**
  - `SetupOn` also sends a `WriteSpecUpdate` (`legacyValidate = true`) so `BM_ProduceValidate_On` keeps measuring a validated topic (without it the "On" arm silently measures an unvalidated topic).
  - `BM_CheckedWriteHook/CheckOnly1Record`: a `TopicWriteSpec` for the "characters" descriptor (all-ASCII strings, `uint64` key), level 4; the timed loop calls `checkTypedBatch` on a one-record batch 1 000 times per iteration, recording per-call ns with `std::chrono::steady_clock` into a preallocated vector; after the loop `counters["p50_ns"]`, `counters["p99_ns"]`; `rune::bench::gateCheck(state, p50 <= 1000.0, "p50_ns > 1000 (spec §14: new validation <= 1 us p50 per one-record batch)")`.
  - `/CheckOnly64Records`: the same over 64 records; counter `ns_per_record`; no gate (informational).
  - `/UntypedPath`, `/TypedLegacyPath` (level 3, `legacyValidate`), `/TypedCheckedPath` (level 4): the full `tryEnqueueOp → doWork → drainReturn` loop of `BM_ProduceValidate_Off`, with `Repetitions(15)`.
- [ ] **Step 2: Build and smoke-run locally.** `cmake --build build/rel -j14 --target rune_broker_benchmark && build/rel/broker/rune_broker_benchmark --benchmark_filter='BM_CheckedWriteHook/.*|BM_ProduceValidate_.*' --benchmark_repetitions=1`. Expected: every bench runs, `gate_failed = 0`.
- [ ] **Step 3: Gate on the prepared VM** (BENCHMARKING.md checklist, one prepare, one restore, under the VM lock): Release builds of the base SHA (`git merge-base HEAD main`) and the tip; interleaved A B A B with n = 3 of `ProducePathBenchmark` and `WorkerProduceLatencyBench` (untyped topic), and of `BM_ProduceValidate_On` (an already-validated typed topic, the base's own bench) against the tip's `BM_CheckedWriteHook/TypedLegacyPath`. Expected: no regression beyond the noise band for untyped and already-validated topics; `CheckOnly1Record` p50 ≤ 1000 ns; the `TypedCheckedPath − UntypedPath` delta is recorded as the accepted new cost (spec §14). On a confirmed regression, bisect the part's commits and name the code path.
- [ ] **Step 4: Record** the medians and noise bands in `broker/bench/BENCHMARKING.md`'s table (one row: "typed produce, level 4, one record").
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/benchmark/ProduceValidateBench.cpp broker/bench/BENCHMARKING.md
  git commit -m "bench: BM_CheckedWriteHook (typed validation budget gate) and the validated-topic bench sends its write spec" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

---

### Task 11: Docs

**Files:**
- Modify: `docs/api/ql.md:22-50`, `docs/api/client-cpp.md:125-145` (+ the producer and `Key` sections), `docs/api/admin-cli.md:50-65`, `docs/api/codec.md:68-80`, `docs/api/wire-protocol.md:94`, `broker/docs/CATALOG.md` (the `SchemaDef` section)

**Interfaces:** none.

- [ ] **Step 1: Write.**
  - `ql.md`: grammar `topic <name> [ { … } ] [ partitions <n> ] …` (default 1); "An **untyped** (raw) topic … has no database features (views, filters, key index, intents, `autoincrement`, constraints)"; "A **typed** topic validates every write: each record decodes against a schema version the topic knows, every `string` / `string(N)` holds valid UTF-8 (`string(N)` is N bytes, zero-padded), and the record key equals the key field's bytes. A batch with any invalid record is refused with `SchemaValidationFailed` (24)"; "A key field is an integer type or `string(N)` with N ≤ 16"; `topic events;` in the examples.
  - `client-cpp.md`: the `defineTopicAndWait` examples without `/*validate=*/`; a "Raw and typed handles" paragraph (`rawTopicAndWait`, `openTopicAndWait<T>`, the refusals); "`Key` never truncates: a key over 16 bytes is `SendResult::KeyTooLong` (`send` throws `std::invalid_argument`)"; "On a typed topic a raw `send` must use the key field's exact bytes (`Key(uint32_t)` for a `uint32` key)".
  - `admin-cli.md`: drop `--validate` from the synopsis and the options table.
  - `codec.md`: `validate(record, StringCheck::Utf8)`; "the broker runs the validator on every typed topic" (replacing "A topic defined with `validate` on…").
  - `wire-protocol.md`: `DefineTopic.validate` — "sent as 1 for a typed topic; a broker of this version ignores it (typed topics always validate)".
  - `CATALOG.md`: `SchemaDef::validate` is recorded and fingerprinted as before, decides validation only below feature level 4, and is otherwise ignored; untyped topics show as `kind raw`.
- [ ] **Step 2: Verify.** Every QL example added to `ql.md` parses: `cmake --build build/sub-ql -j14 --target rune_ql_tests && ctest --test-dir build/sub-ql -R 'Parser|Script' --output-on-failure` (add each new example to `CheckedWritesQl.DocExamplesParse` in `ParserTest.cpp`). `grep -n "validate" docs/api/admin-cli.md` → no hit.
- [ ] **Step 3: Commit.**
  ```bash
  git add docs/api/ql.md docs/api/client-cpp.md docs/api/admin-cli.md docs/api/codec.md docs/api/wire-protocol.md \
          broker/docs/CATALOG.md ql/tests/ParserTest.cpp
  git commit -m "docs: typed topics always validate, raw topics, partitions default, key rules, rawTopicAndWait, KeyTooLong" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Checkpoint C — before the PR (after Task 11)

- [ ] `ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure` → pass.
- [ ] `ctest --test-dir build/sub-client-cpp -R Regression -LE latency -j14 --output-on-failure`, `ctest --test-dir build/sub-ql -R Regression --output-on-failure`, `ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden' --output-on-failure` → pass (CatalogGolden: no catalog format changed in this part, so the goldens are byte-identical).
- [ ] `cmake --build build/asan -j14 && ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/asan -R Regression -LE latency -j8 --output-on-failure` → pass, no report.
- [ ] `cmake --build build/tsan -j14 && ctest --test-dir build/tsan -R Regression -LE latency -j8 --output-on-failure` → pass.
- [ ] `cmake --build build/client-tsan -j14 && ctest --test-dir build/client-tsan -R Regression -LE latency -j8 --output-on-failure` → pass.
- [ ] Task 10's VM bench verdict recorded; latency gates `ctest --test-dir build/root -L latency` on an idle host → pass.
- [ ] Hand-over check (overview "02"): every typed topic's partition has a non-null `PartitionWriteState*` on its owning worker, followers included (`CheckedWritesWorker.RoleChangesKeepTheWriteState`, `ThreeNodesNewLeaderValidatesRegression`); untyped topics have nullptr; `clusterFeatureLevel_` is on every worker and in the AdminWorker.
- [ ] File the parked follow-ups as labelled issues (`enhancement`/`tech-debt`, `sev:low`, `area:broker` / `area:ql`): QL keyless typed topics (P10, for Part 08); the gateway-inline produce path of `ClientGateway.cpp:4085` (no worker pool, unit rigs only) does not validate.

---

## Contract additions (listed for the controller; no existing contract type is redefined)

- `RecordSpan` gains `std::string_view key;` in **Part 02** (P9). C3 keeps `attributes` and `index` for Part 03.
- `TopicWriteSpec` (C7) gains, in Part 02: `bool legacyValidate`, `const SpecVersion* versionFor(uint16_t) const noexcept`, `uint16_t maxVersion() const noexcept`, `bool hasFacts() const noexcept`, `void installVersion(uint16_t, rune::codec::Descriptor)`, `void setFacts(const TopicWriteSpecWire&)`, and the struct `SpecVersion { Validator validator; Descriptor descriptor; uint32_t keyValueOffset; }`; constants `kNoKeyField = 0xFFFF`, `kNoKeyOffset = 0xFFFFFFFF`.
- `struct TopicWriteSpecWire` (the `WriteSpecUpdate` tail behind `encodeTopicWriteSpec` / `decodeTopicWriteSpec`, `kTopicWriteSpecWireLen = 9`) and `std::optional<TopicWriteSpecWire> writeSpecWireOf(const rune::catalog::TopicObject&)`.
- `enum class TypedCheck : uint8_t { Accept, Reject, VersionAhead }` and `TypedCheck checkTypedBatch(const TopicWriteSpec&, std::span<const uint8_t>, bool checkedLevel) noexcept` in `WriteState.h` (Part 03's hook calls it from `check()`).
- `kA2cWriteSpec = 26` (admin → control) and its codec; `SchemaFanout::postWriteSpec(int32_t, std::vector<uint8_t>)`, `SchemaFanout::pendingFor(int32_t) const noexcept`.
- `ClusterFeatureLevelFanout` (`broker/libs/node/include/rune/node/ClusterFeatureLevelFanout.h`), `AdminWorker::setClusterFeatureLevel(int16_t)` / `clusterFeatureLevel()`, `Worker::clusterFeatureLevelForTest()`, `kClusterFeatureLevelWireLen = 2` with `writeClusterFeatureLevel` / `decodeClusterFeatureLevel`.
- `broker/libs/node/include/rune/node/TopicFeatureRules.h`: `DbFeature`, `TopicShape`, `featureRefusal`, `topicShapeOf` (05 and 06 call it for `key.index` / `autoincrement`, 09 for constraints).
- `rune::codec::Utf8Error`, `Utf8Result`, `validateUtf8` (`codec/include/rune/codec/Utf8.h`); `rune::codec::kMaxTopicKeyBytes = 16`, `isTopicKeyType(PrimType, uint32_t)` (`PrimType.h`); `rune::codec::StringCheck` and `Validator::validate(span, StringCheck)`.
- `rune::protocol::client::kSchemaKindFingerprint = ~uint64_t{0} - 1` (`SchemaMessages.h`) and `Client::rawTopicAndWait(std::string_view, std::chrono::nanoseconds)`; `Key::tooLong()`.
- Test seams: `Worker::writeStateForTest`, `typedRejectsForTest`, `typedVersionAheadForTest`, `NodeConfig::testHoldWriteSpecPasses` with `SchemaFanout::setHoldWriteSpecPassesForTest`.

## Self-review (done while writing)

- **Spec coverage (§4, §12, §15 cells owned by 02):** untyped = no field list, `raw` marker → Task 7 (catalog text, plan text); typed always validates, `validate` flag / `--validate` / parameter removed → Tasks 4, 7, 8; every record decodes by exact version, whole batch rejected with 24 → Tasks 3, 4 (P2 refines "version above the highest" to retriable — flagged); control batches exempt → `ControlRecordsAreNeverValidated`, `TransactionOnTypedTopicCommitsRegression`; wire key = key field, `memcmp`, autoincrement placeholder → Task 3 (M2 `ZeroPlaceholderBoth`); key width ≤ 16 refused at DDL → Task 6; `Key` truncation an error → Task 8; the two encodings kept separate → nothing on the wire changes; database features only on typed (and keyed) topics → Task 6 (table + views now, 05/06/09 call it), filters already refused (`FilterBrokerRegression.UntypedTopicRefusedRegression`); binding strict both ways → Task 8, Task 9; `partitions` optional → Task 7; UTF-8 rules, ASCII fast path, padding skipped, `bytes` unchecked → Task 1; existing typed topics start checking behind the level → Tasks 2, 4, 9; keyless typed topics allowed → M1 TypedKeyless; hot path off `validatedTopics_` onto the per-partition pointer, untyped pays one branch → Task 4, gated in Task 10; §12 feature level → Task 2; §14 budgets → Task 10.
- **Step scan:** each test step names its suite, file and assertions with the spec's values; each code step names the signature, the file and the pinned values; the only code block is `checkTypedBatch`'s rule order, which the tests do not fully determine (first-non-accept, version-ahead vs reject).
- **Type consistency:** `TopicWriteSpec`, `PartitionWriteState`, `TopicWriteSpecWire`, `checkTypedBatch`, `TypedCheck`, `writeOf_`, `writeById_`, `WriteSpecUpdate = 27`, `ClusterFeatureLevel = 26`, `kA2cWriteSpec = 26`, `kMsgClusterFeatureLevel = 26`, `kSchemaKindFingerprint` are spelled the same in every task and in the contract list.
- **Review Focus:** each of the five lines has its test in the owning task (Tasks 3, 4, 9).
- **Proportion:** one algorithm block; the rest is signatures, tables and test assertions.
