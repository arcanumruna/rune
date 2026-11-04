# Checked Writes — Part 05: Write Intents, the Key Index and the Typed Delete — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Revision 2026-11-02 (re-cut against `main` 41a4de53).** Main moved after this plan was written on 2026-10-22:
> - **Namespaces took catalog `formatVer` 2** (`kFormatV2`, `kCurrentFormatVer = kFormatV2`, `ApplyV2`, `golden/v2/namespaces.rcat`, `kNamespacesV2Digests`). It shipped in `broker-v0.3.0` and is frozen. Key features are therefore **`formatVer` 3**: `kFormatV3`, `ApplyV3` wrapping `ApplyV2`, `EncodeError::NeedsFormatV3`, `goldenScriptV3()`, `golden/v3/basic.rcat`, `kBasicV3Digests`, `CheckedWritesCatalogV3*`. The AdminWorker writes v3 only at level ≥ `kCheckedWrites`, and `kCurrentFormatVer` (2) below it. The "no rules" tests move to `formatVer` 4 (overview RB4).
> - **Numbers follow the overview's RB2/RB5.** `ConstraintViolation` is 55, `KeyExists` 56 and `KeyNotFound` 57, and all three are already declared. The client version is 17, the broker version 10, and `kCheckedWrites` is 5. Conformance vectors are `_v17`, "v11 client" means 16, "v6 broker" means 9, and builds use `-j18` (RB6).
> - **Already on main, so this part no longer builds them:**
>   - the intent attribute bits and `intentOf` (`RecordBatch.h:41-45`);
>   - the builder's attributes (`:139`);
>   - the 5-argument `forEachRecord` visitor (`:403-449`), which replaces the planned `forEachRecordWithAttributes`;
>   - `SendResult::WriteNotAllowed`;
>   - `TopicHookFacts::keyIndex`.
> - **Renamed or moved on main:**
>   - The rig is `VerdictRig`, with `typed()`, `kSrc = 6` and a `tag` of type UInt16.
>   - `ConstraintSetMember` lives in `VerdictState.h`.
>   - The gateway fakes are split over four targets.
>   - TypedTopic tests live in `rune_open_topic_tests` and record-op tests in `rune_subscriber_tests`.
>   - broker/ and client-cpp/ glob the catalog sources.
>   - `rebindVerdictSources_`, `AbortedFoldLog.h` and `RigOptions` do not exist.
> - **Gaps closed:**
>   - Task 5: the `respondGet` test call sites.
>   - Task 6: the `judge()` restructure for per-member codes and tombstones.
>   - Task 7: `ensureVerdictSource`, `syncConstraintSet_` and its cache epoch, the `driveViews_` short-circuit, and `adoptNs_` / `chargeMem_`.
>   - Task 8: `expect05`, `cellsFor(Part::P05)` and `VerdictRig` drivers for the six timings 03 drove only on `AckRig`.
>   - Task 9: tombstone judging in the model.
>   - Line anchors are re-cut.

**Goal:** A typed topic declared with `key.index` judges every write that carries an intent (`insert`, `update`, delete-existing) against the set of keys that exist, when the fold reaches it. An `insert` on a live key gets `KeyExists` (56), and an `update` or `removeExisting` on an absent key gets `KeyNotFound` (57). The record is never visible, and the verdict reaches the writer through the `View` ack. The client gets `WriteIntent`, a typed `insert` / `update` / `upsert` / `remove` / `removeExisting` API with `…AndWait` forms that return `WriteResult`, and `record.op()` for consumers. The catalog gains `formatVer` 3: the namespaces' frozen `formatVer` 2 plus `TopicKeyFeatures`.

**Architecture:**
- **The key index is a verdict view.** It is a `VerdictViewKind::KeyIndex` member (object id 0, first in set order) of the partition's `VerdictSource` (Part 04, C9.5). Its state is a `ViewTable` of the live wire keys, judged at fold through 04's prepare / evaluate / apply loop. It is never checkpointed. Each new leadership rebuilds it from the local log, below the coverage K, without judging (P5-1).
- **The write path changes in one place.** The hook's facts set `keyIndex` (declared on main, always false today) and learn `keyIndexActive`, and an intent that arrives before the key index's `ConstraintSetChange` record is in the log gets a retriable `NotLeader`. Plain writes stay requirement-free, and the key index folds them like any record.
- **DDL and catalog.** `DefineTopic` carries a v17 `TopicKeyFeatures` block. The AdminWorker applies the feature rules: the level, Part 02's `featureRefusal`, and the 8-byte limit of SD8. The catalog writes `formatVer` 3 events once the cluster level is 5 (`kCheckedWrites`), and `kCurrentFormatVer` (2) below it. `writeSpecWireOf` passes `keyIndex` to every worker's `TopicWriteSpec`.
- **Client.** The intent rides the producer ring's flags byte into the record's `attributes` bits 0–1. Local refusals happen before anything is queued. The `…AndWait` forms wait on one `Send` completion through the producer's existing `andWait` hook.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices, seeded model checks), Google Benchmark (`build/rel/broker/rune_broker_benchmark`), CMake/Ninja, real Aeron (the embedded invoker driver over `aeron:ipc`) for the worker rig, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`), and Docker Compose on the shared VM for the first docker e2e of the program.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`: §5 (Part B) in full, the §3 fold terms, §11.1 codes `KeyExists` / `KeyNotFound` (56/57 on main, RB2), the §11.2 intent row, §12 (key.index DDL and catalog formats), and the §15 cells the overview gives Part 05. The binding contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md`: first its "Rebase to main 750c836b" section (RB1–RB6), which overrides every number below, then C3, C6, C7, C8, C9, C12, C13, C15, the C16 ids 1–2, SD8, SD9, SD11, and the shared test infrastructure. This part consumes plans `2026-10-22-checked-writes-01-aborted-fold.md`, `-02-typed-validation.md`, `-03-write-hook.md` and `-04-verdict-pipeline.md`, through their Interfaces and Contract additions. Executors read the spec sections, the overview, plans 02–04 and this file.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-05-intents-key-index`, cut from `main` 41a4de53, after Parts 02–04 merged (#780, #795, #810). Merge `main` in again before review.
- **Preconditions.** Run these before Task 1. Each must match; otherwise stop and tell the controller.
  - `grep -n "KeyIndex = 1" broker/libs/view/include/rune/view/Verdict.h` (1 hit, Part 04)
  - `grep -n "class VerdictSource" broker/libs/node/include/rune/node/VerdictSource.h` (1 hit, Part 04)
  - `grep -n "struct ConstraintSetMember" broker/libs/node/include/rune/node/VerdictState.h` (1 hit, Part 04)
  - `grep -n "struct TopicHookFacts" broker/libs/node/include/rune/node/CheckedWriteHook.h` (1 hit, Part 03)
  - `grep -n "featureRefusal" broker/libs/node/include/rune/node/TopicFeatureRules.h` (≥ 1 hit, Part 02)
  - `grep -n "void typed(int16_t level = 5)" broker/libs/node/tests/support/CheckedWritesRig.h` (1 hit, Part 04's `VerdictRig`)
  - `grep -n "OpStatusThirteenIsReservedAndVerdictUnknownIsFourteen" client-cpp/tests/producer/ProducerVerdictsTest.cpp` (1 hit, Part 04 R04e: `VerdictUnknown` is 14 by position)
  - `grep -n "WriteNotAllowed" client-cpp/include/rune/client/producer/Producer.h` (≥ 1 hit, Part 03)
  - `grep -n "KeyNotFound *= 57" protocol/include/rune/protocol/client/Protocol.h` (1 hit, Part 03)
  - `grep -n "kClientVersionCheckedWrites *= 17" protocol/include/rune/protocol/client/Protocol.h` and `grep -n "kBrokerVersionCheckedWrites *= 10" protocol/include/rune/protocol/client/Protocol.h` (1 hit each)
  - `grep -n "kCheckedWrites = 5" broker/libs/replication/include/rune/replication/ReplicationMessages.h` (1 hit, Part 02)
  - `grep -n "kFormatV2 *= 2" codec/include/rune/catalog/CatalogEvent.h` (1 hit, namespaces) and `grep -c "kFormatV3" codec/include/rune/catalog/CatalogEvent.h` (0)
  - `grep -rl "TopicKeyFeatures" protocol/include codec/include client-cpp/include broker/libs/node/include` (no hit)
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j18 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j18 --output-on-failure`.
  - `build/sub-client-cpp` (the client suites), `build/sub-codec` (`rune_codec_tests`, `rune_catalog_tests`, CatalogGolden), `build/rel` (Release, `BUILD_BENCHMARKS=ON`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Line numbers** cite `main` 41a4de53 (revision 2026-11-02). When a number has drifted, anchor on the named function or member.
- **Docker e2e, VM benches.** These run only on the shared Linux VM (`$RUNE_VM_HOST`). Every command runs under `flock -w 7200 /root/vm.lock`, never on local Mac docker. For benches, prepare the clean box once and restore it afterwards (`broker/bench/BENCHMARKING.md`). This plan writes the e2e script (Task 11). Running it on the VM is part of Checkpoint C.
- **Commits.** Use `git add <paths>` only, never `-A`, `.superpowers/` or build logs. Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

## Global Constraints

Copied verbatim from the overview (Plan 00), restricted to what this part touches. Every task implicitly includes this section.

- **Error codes** (`protocol/include/rune/protocol/client/Protocol.h:234-240`, `ErrorCode`, append-only; Part 03 already declared them, RB2):
  - `ConstraintViolation = 55`: the record broke a rule when it was folded. Constraint id `viewObjectId << 16 | ordinal`, ordinal 0 = `unique`. Fold, per record, definite.
  - `KeyExists = 56`: `insert` on a key that exists. Fold, per record, definite.
  - `KeyNotFound = 57`: `update` or delete-existing on a key that's absent. Fold, per record, definite.
  - Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.
  - "Definite" for 55–57 means *never visible*. 55–57 reach only `View` writers on client version 17; older clients get `InvalidRequest` (`Protocol.h:541`).
- **Versions** (all on main).
  - `kClientVersionCheckedWrites = 17` and `kClientVersion = 17` (`Protocol.h:397-398`).
  - `kBrokerVersionCheckedWrites = 10` and `kBrokerVersion = 10` (`Protocol.h:441-442`).
  - Feature level `MetadataFeature::kCheckedWrites = 5` and `kLatest = kCheckedWrites` (`ReplicationMessages.h:481-483`; 4 is `kTxnPartitionVerify`).
  - Plan v2, checkpoint v3 and catalog `formatVer` 3 follow the existing frozen-format rules: old formats still decode. Catalog `formatVer` 1 and 2 are frozen (`CatalogEvent.h:3-10`, `CatalogGoldenPins.h:2,37`).
- **Values.**
  - `verdict.interval` = **100 ms**.
  - Topic key field ≤ `Key::kMaxInline` = **16 bytes** (`client-cpp/include/rune/client/Key.h:35`).
  - Wire `acks` byte: bit 0 = `All`, bit 1 = `View`. Other bits are refused with `InvalidRequest`.
  - Intent bits: record `attributes` bits 0–1. 0 = upsert/delete, 1 = insert, 2 = update/delete-existing. 3 and the other bits are refused with `InvalidRequest`.
  - `partitions` defaults to 1. Strings are UTF-8, and `lower()` is Unicode simple lowercasing.
- **Budgets** (hard gates, spec §14):
  - Judging: ≤ **1 µs per record** for one unique view plus one count check (`ViewFoldBench`).
  - No ABAB n=3 regression on `ProducePathBenchmark` and `WorkerProduceLatencyBench` for untyped topics and for already-validated typed topics.
  - Repo latency gates stay: view update < 10 µs, point read < 50 µs, QL ≤ 2–5 µs.
- **Repo rules** (root `CLAUDE.md` §1–§4):
  - **Hot path.** The produce, consume, fold and query paths are zero-alloc, take no lock, use no atomics as coordination and no `thread_local`, make no virtual call and use no `std::function`. Hot paths use concrete types plus a branch, or templates. Control-plane work (installs, DDL, recovery scans) runs as cold worker ops and may allocate.
  - **States.** Rich-state variants, never scattered booleans.
  - **Clocks.** One injected clock. Tests advance it and call `doWork`, with a hard iteration cap and never a wall-clock wait.
  - **Regression tests.** Every fix ships a `*Regression*` test. Each task runs the affected-area tests.
  - **Checkpoints** run the full `-R Regression -LE latency` suite, plus ASan+UBSan and TSan for the broker and the client, plus CatalogGolden.
  - **Interaction matrix.** Every feature is tested across its matrix (§ Shared test infrastructure). Each plan has an "Interaction matrix" section and a "Review Focus" section, with 1–3 phases of small tasks.
  - **Real e2e.** Nothing is done until its real e2e path is green on the **default** config (shard worker mode).
  - **No weakened tests.** A weakened assertion is recorded as a deviation. Urgent ones are fixed now and the rest are filed as GitHub issues with type, severity and area labels.
- **Names.** Every new test suite name starts with `CheckedWrites`, so `ctest -R CheckedWrites` selects the program. Every fix test carries `Regression` in its suite or name. Part 07's run filter expects `CheckedWritesKeyIndex*` and `CheckedWritesIntent*`, so this part uses exactly those prefixes.

## Rulings this part depends on (restated; binding)

- **SD8.** The key index is built for wire keys of at most 8 bytes. `key.index` on a key field wider than 8 bytes is refused at DDL with `InvalidRequest`, "key.index needs a key of at most 8 bytes", until Part 07 lifts it (Part 07's precondition greps for this exact text in `broker/libs/node`, and its Task 16 removes it).
- **SD9 / R10a / R02e.** `key.index` on a transient topic is refused through Part 02's `featureRefusal(DbFeature::KeyIndex, shape)` (`broker/libs/node/include/rune/node/TopicFeatureRules.h`). Intents need a key index, so they are refused with it.
- **R03b / R05a.** On an untyped topic the broker never scans record attributes (G10: one branch). Intent bits there are stored as opaque bytes. The client refuses intents, key-0 creates and `View` locally on a topic it knows is untyped, with `SendResult::WriteNotAllowed`, synchronously and with no completion (R05a amends R03b's `OpStatus::InvalidRequest`, which doesn't exist). The docs say the bits are not interpreted on raw topics.
- **R05c (amended).** The key index is never checkpointed. Each new leadership rebuilds it from the local log. While it rebuilds, intents are **not** refused: they are accepted, judged at fold once the rebuild finishes, and their `View` acks wait meanwhile (P5-1). `HookRefusal::KeyIndexActivating` (`NotLeader`) covers only the window before the key index's `ConstraintSetChange` record is committed (P5-4).
- **R05b.** Part 11 defines the C8 atomic-batch header bit. This part does not.
- **R07d (reworded).** `ViewKey::fromU64` / `asU64` are Part 04's (`ViewKey.h`). This part takes them from 04; Part 07 later extends `ViewKey` without renaming them.
- **R02b / R03e.** A tombstone skips value validation; only its key width (exactly `keyWidth`) is checked.
- **R04e.** An in-window idempotent duplicate with `View` on a verdict source is answered with the original's verdicts once `W > lastOffset`. Outside the window it gets `DuplicateSequence` (21), which the client reports as one `OpStatus::VerdictUnknown` for the whole batch. So a retried `insert` gets its own original `Ok`, never `KeyExists` against itself.
- **R04d / R04f.** Part 04's contract additions bind this part: `ConstraintSet`, `ConstraintSetMember`, `ConstraintSetTimeline`, `kMaxVerdictViewsPerSource = 32`, `MarkerLedger` (all in `broker/libs/node/include/rune/node/VerdictState.h`), `RcVerdictHooks`, the `TestReject` seam. Matrix enums are append-only.
- **SD10.** In legacy worker mode a non-zero intent is answered `InvalidRequest` (03's rule 2).
- **SD11.** The client gates intents on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`. An intent needs `View` (§11.2), so 03's `View` gate covers it: `OpStatus::BrokerTooOld` locally. A v10 broker below level 5 answers an intent `InvalidRequest` (03's rule 3), and the AdminWorker refuses `key.index` DDL there.
- **SD4.** A `View` batch on a verdict source holds at most 4096 records. A key-index topic is a verdict source, so it inherits the cap.
- **P4-5 (04).** Set order is ascending `viewObjectId`. The key index has id 0 and comes first. A record's verdict is the first failing member's. Every member is prepared before any is evaluated.
- **P4-6 (04).** The desired set is compared with the logged set on a Ready leader, and `requestChange` appends `kConstraintSetChange`. A member named by the logged set that isn't installed makes the judge **wait**.
- **C12.** Key-index codes 56 and 57 carry `constraintId = 0`. In `kConstraintSetChange` the key index appears with `viewObjectId = 0`.

## Part rulings (P5; binding for executors, recorded so the reviewer can overturn any)

- **P5-1 — Key index storage and lifecycle.**
  - **Where it lives.** It is a `ViewInstance` owned by `WorkerViewSet::keyIndexes_`, keyed `instKey(srcTopicId, partition)`. It is registered with the partition's `VerdictSource` through `addVerdictView(&inst, VerdictViewKind::KeyIndex, /*viewObjectId=*/0, kKeyIndexPlanFingerprint)`. It isn't in `views_` or `templates_`, so `GetViewRow`, `minFoldCursor`, `allViewsReady` and recovery never see it. Like the judge's members, each instance is adopted into and charged to the source topic's namespace (`WorkerViewSet::adoptNs_` `WorkerViewSet.h:426`, `chargeMem_` `:410`; overview RB5, Part 04).
  - **Table.** A `ViewTable` with a synthesized one-byte row (`RowLayout{.rowLen = 1}`). A zero-length row would make `ViewTable` allocate a chunk per key (`ViewTable.cpp:45-49`). The chunk size is `kKeyIndexChunkBytes = 64 KiB` (65,536 rows per chunk), and the index starts at 4096 slots. That is about 14 bytes per key plus the load factor, close to §5.2's "about 16 bytes".
  - **Never checkpointed.** Spec §18 leaves this to the builder.
  - **Rebuilt on every activation.** That means each new leadership generation and each restart: the instance is replaced by a fresh one with `foldCursor = log->logStartOffset()`. 04's activation then replays it unjudged to K, skipping rejected offsets and aborted transactions, before judging resumes.
  - **Why it is never reused across leaderships.** While this node was a follower, its log may have taken compacted batches from another leader, whose cleaner removed a tombstone after `delete.retention.ms`. A key index carried over would keep that key forever. A re-judge above W after a failover would then reject a record the old leader had acked: an anti-goal. A replay over the local log, compacted or not, yields exactly the live keys of the accepted records.
  - **Replay pace.** The replay folds at most `kKeyIndexReplayRecordsPerPass = 65536` records per duty pass. Delivery and `View` acks wait meanwhile (04: the judge starts only when every member is at its cursor). Consumers still get every offset below K.
- **P5-2 — The key word.**
  - **Mapping.** The key index keys its table by `keyIndexWord(wireKey)`: the wire key's bytes packed little-endian into a `uint64_t`, after an exact check that `wireKey.size() == spec.keyWidth`. With `keyWidth ≤ 8` (SD8) and one width per topic, the mapping is injective for every topic key type, including `string(N ≤ 8)`. `decodeWireKey` (`ViewFold.cpp:73-110`) handles integers only, which is why the key index has its own fold (Task 6).
  - **Probes.** Every probe goes through `rune::view::ViewKey::fromU64(word).asU64()` (Part 04's `ViewKey.h`, R07d). That is the one place Part 07 replaces with `ViewKey::fromBytes` when it widens the index to 16 bytes.
- **P5-3 — Intent rules at fold** (§5.1). The key state is taken from the prospect, computed before any member applies.

  | Record | Intent | Key exists | Key absent |
  |---|---|---|---|
  | live (`!value.empty()`) | 0 upsert | accept, put | accept, put |
  | live | 1 insert | `{56, 0}` | accept, put |
  | live | 2 update | accept, put | `{57, 0}` |
  | tombstone | 0 remove | accept, erase | accept (no-op) |
  | tombstone | 2 removeExisting | accept, erase | `{57, 0}` |
  | tombstone | 1 | refused at write (P5-5), never folded | |

  - **Tombstones and other members.** Tombstones are never rejected by any other member (§8.2, P4-12). Only `removeExisting` on an absent key is rejected, and only by the key index.
  - **Order.** Records are judged in offset order, each against the accepted ones before it, including those earlier in the same batch.
  - **Rejected records apply nothing.** A record rejected by another member (55) leaves the key index unchanged, so a later `update` of that key gets 57.
  - **Malformed key.** A record whose wire key isn't `keyWidth` bytes long prepares `Malformed`. The key index then applies nothing and does not reject; the record is counted in `keyIndexSkippedMalformed`. At level 5 the hook makes this unreachable for new writes (P3-10). It can still happen for records written before an adoption.
- **P5-4 — No intent is ever applied unjudged.** An intent sent before the partition's logged constraint set contains the key index gets a retriable `NotLeader` (2), counted `HookRefusal::KeyIndexActivating`. That covers the first moments of a new topic, an adoption, and a new leader before its leadership scan. The client resends it after its backoff.
  - **The fact.** `TopicHookFacts::keyIndexActive` is `constraintSets.latest().count > 0 && constraintSets.latest().members[0].kind == VerdictViewKind::KeyIndex`. This is O(1) because the key index has id 0 and set order is ascending.
  - **Rule placement.** It sits after every definite intent rule (C8 rules 1–6), so a forbidden intent gets `InvalidRequest` at once, never a retry loop first.
- **P5-5 — An insert tombstone is a reserved shape.** A record with intent 1 and an empty or null value sets `BatchIntents::reservedBits` in `check()` (rule 1, `InvalidRequest`). The client refuses it locally with `SendResult::WriteNotAllowed` ("an insert needs a value").
- **P5-6 — `key.index` DDL rules** (`AdminWorker::buildDefine_`, leader-only build rules, in this order; each failure is `InvalidRequest` (39) with the exact detail):
  1. `clusterFeatureLevel_ < kCheckedWrites` → "key.index needs every broker at feature level 5".
  2. `featureRefusal(DbFeature::KeyIndex, shape)` → its message ("key.index needs a typed topic" / "key.index needs a key" / "key.index needs a persisted topic").
  3. The key field's encoded width > 8 → "key.index needs a key of at most 8 bytes" (SD8).
  4. `autoIncrement`, a `partitioner` other than `Fnv1a`, or a non-default `autoIncrementMax` / `autoIncrementReserve` → "autoincrement is not supported by this broker yet" (Part 06 removes this rule).
  - **Shapes.** For a create, `TopicShape{.typed = !m.blob.empty(), .keyed = !m.keyField.empty(), .transient = m.persistence == 1}`. For an adoption, `topicShapeOf(*t)` with `typed = true`.
  - **Defaults.** A request with default `TopicKeyFeatures` runs none of these rules.
  - **Immutability.** `key.index` is set at create or adoption only. No event changes the features of a typed topic (P5-7).
- **P5-7 — Catalog `formatVer` 3** (overview RB4).
  - **Why 3.** `formatVer` 2 is namespaces: a u32 `nsId` after the kind and kinds 32–36 (`CatalogEvent.h:45-57`, `ApplyV2`). It shipped in `broker-v0.3.0` and is frozen like v1 (`CatalogGoldenPins.h:2,37`; `golden/v2/namespaces.rcat`, `kNamespacesV2Digests`). `formatVer` 3 is v2 byte for byte plus the key block below. v3 keeps `nsId` and the namespace kinds, because `checkKind` and the header `nsId` test `formatVer >= kFormatV2` (`CatalogEvent.cpp:52,295,329`).
  - **Which format is written.** The AdminWorker writes `kFormatV3` when `clusterFeatureLevel() >= kCheckedWrites` (5), else `kCurrentFormatVer` (`kFormatV2`), and never v1 (v1 drops `nsId`). That happens at the one common build site (`AdminWorker.cpp:1354`), so namespace DDL follows the same rule. The level is committed only once every broker decodes v3. `kCurrentFormatVer` stays `kFormatV2` as the struct default, so `CurrentFormatVerIsTwoRegression` (`NamespaceCatalogV2Test.cpp:107-110`) stays green. The v1 and v2 fixtures are already pinned explicitly (`GoldenScript.h:76,84`, `CatalogEventTest.cpp:47,107`), and their bytes don't change.
  - **Layout.** The v3 layout is the v2 layout plus, inside **every** `SchemaDef` body (`CreateTopicOps` when it has a schema, `CreateSchemaOps`), a trailing 14-byte block `[flags u8: bit0 keyIndex, bit1 autoIncrement][partitioner u8][autoIncrementMax u64][autoIncrementReserve u32]`. Other flag bits and a partitioner > 1 are `BadEnum`. The encoder refuses a v1 or v2 event with non-default features with the new `EncodeError::NeedsFormatV3`; the decoder can never produce one.
  - **Apply.** `apply()` and `previewFingerprint()` (`CatalogState.cpp:119-149`) accept 1–3 and dispatch v3 to `ApplyV3`. `ApplyV3::run` calls `ApplyV2::run`, then sets `schema->keyFeatures = def.keyFeatures` on a staged `CreateTopic` / `CreateSchema`, then applies its own rules: `keyIndex` needs a schema with a non-empty `keyField` and `Persistence::Disk`, otherwise `Precondition`. `ApplyV3::commit` forwards to `ApplyV2::commit`, so no new `friend` is needed in `CatalogState.h:164-165` or `NamespaceRegistry.h:111`.
  - **Carry.** `ApplyV1.cpp:340` rebuilds `SchemaState` field by field on `AlterSchema`, which drops the features. So for **every** format, the dispatch calls `ApplyV3::carryKeyFeatures` between `run` and the fingerprint check. The carry copies the prior topic's `keyFeatures` onto every staged topic that already had a schema. It is a no-op when the prior features are default, which is true of every v1 and v2 log written so far. `ApplyV1.cpp` and `ApplyV2.cpp` are never edited.
  - **Fingerprint.** `CatalogFingerprint.h` is frozen (`:1-12`), and `fingerprintOf` (`CatalogFingerprint.cpp:69-89`) is untouched. `ApplyV3::keyFeaturesFingerprint(fp, features)` returns `fp` for default features. Otherwise it hashes the features after `fp`, layered over `objectFingerprintV2` as RB4 says. The dispatch applies it to every format's staged fingerprint, and `CatalogState::digest()` (`CatalogState.cpp:82-112`) applies it to each topic's fingerprint before its `nsId` layer. Every v1 and v2 digest and every default-feature v3 digest is therefore unchanged.
  - **When ApplyV3 freezes.** At the program's first release, not at this part. Part 06 appends its autoincrement rules and `golden/v3/autoinc.rcat` (RB4), and this part's `golden/v3/basic.rcat` must keep replaying unchanged.
- **P5-8 — Wire trailers (C13).**
  - **`DefineTopic`.** The 14-byte block (same layout as P5-7) comes after the v8 `DdlExtension`, iff `hasKeyFeatures`. `hasKeyFeatures` requires `hasExt`, so `fits()` is false otherwise. Decode reads it when bytes remain after the extension, exactly 14 of them, else `nullopt`. A broker older than version 10 fails such a frame as undecodable (`InvalidSchema`), but the client never sends it there (SD11, P5-9).
  - **`TopicSchemaResponse`.** The block comes after `partitionCount`, iff `hasKeyFeatures`. The broker sets it iff the topic's features are non-default. Older decoders stop after `partitionCount` and ignore it (`SchemaMessages.h:392-394`). Every existing golden vector is unchanged.
  - **Client exposure.** The client decodes the response block. Exposing it in `TopicSchemaResult` is Part 06's job, because the partitioner is its first consumer.
- **P5-9 — Client local refusals (§11.2 "the client refuses locally first", R03b).** Each is a synchronous `SendResult::WriteNotAllowed`: nothing is queued and no completion follows. The blocking forms throw `std::invalid_argument` naming the rule.
  - `intent != Upsert` on a `TopicHandle` whose `kind == TopicKind::Raw` ("intents need a typed topic").
  - `View` in the producer's acks on a `TopicHandle` whose `kind == TopicKind::Raw`, for any intent ("acks View needs a typed topic"; R05a). This check runs on every send of a `View` producer, behind the same handle-kind branch, so a non-`View` producer pays nothing new.
  - `intent != Upsert` without `View` in the producer's acks ("intents need acks including View").
  - `intent != Upsert` on a transactional producer ("intents inside a transaction need Part G").
  - `Insert` with an empty value (P5-5).
  - **Not checked locally.** A typed topic without a key index (the broker's definite `InvalidRequest` is the answer), the broker version (03's `View` gate gives `BrokerTooOld` at seal), and a handle of kind `Unknown` (a hand-built `TopicHandle`).
  - **What carries no intent.** Tier-2 batches (`Producer::batch`, `BatchBuilder`) and claims are upsert-only in this part. That is a follow-up issue. No intent can therefore appear inside an explicit atomic batch from a Rune client.
- **P5-10 — The typed API (C15).** For each op X ∈ {`insert`, `update`, `upsert`, `remove`, `removeExisting`}, `TypedTopic<T>` gets:
  - `void X(arg, Completions& cq, Token token)`, which follows `Producer::send(…, cq, token)`'s rules;
  - `[[nodiscard]] WriteResult XAndWait(arg, std::chrono::nanoseconds timeout = kDefaultWriteWait)`.
  - `upsert(const T&)` and `remove(const KeyT&)` also get fire-and-forget forms. `send(rec)` stays byte-identical, and `upsert` is its alias.
  - `KeyT` is `std::remove_cvref_t<decltype(std::declval<const T&>().key())>`. `remove` / `removeExisting` build the record key with the same `recordKey_` logic `send(rec)` uses (`TypedTopic.h:353`), so a delete routes exactly as the write did.
  - **`…AndWait` outcomes.** They throw `std::invalid_argument` for a local refusal (P5-9, `KeyTooLong`, `MessageTooLarge`). Every broker and transport outcome comes back as a `WriteResult`: `Ok`, `BrokerError` 56/57/39/24, `Timeout`, `BrokerTooOld`, `VerdictUnknown`, `DeliveryFailed`.
- **P5-11 — `record.op()`.** `value.empty()` → `Delete` (intent 0 or 2). Otherwise `attributes & 0x03`: 1 → `Insert`, 2 → `Update`, anything else → `Upsert`. On a raw topic the bits are whatever the producer sent, and the docs say so (R03b).
- **P5-12 — Requirements are per write.** A key-index topic accepts plain upserts and deletes from any producer, with any acks and any client version. They are folded into the key index and never rejected by it. Only records whose intent is not 0 are judged against it.

## Contract additions (none redefines a contract type)

1. **`protocol/include/rune/protocol/CatalogTypes.h`:** `inline constexpr std::size_t kTopicKeyFeaturesWireLen = 14;`, `void putTopicKeyFeatures(uint8_t* out, const TopicKeyFeatures&) noexcept;`, `[[nodiscard]] bool getTopicKeyFeatures(const uint8_t* in, std::size_t avail, TopicKeyFeatures& out) noexcept;` (false on unknown flag bits or a partitioner > 1). C13's `TopicKeyFeatures`, `Partitioner`, `kAutoIncrementMaxDefault` and `kAutoIncrementReserveDefault` are added verbatim.
2. **`SchemaMessages.h`:** `DefineTopic` and `TopicSchemaResponse` append `bool hasKeyFeatures = false; rune::protocol::TopicKeyFeatures keyFeatures{};` (P5-8).
3. **Catalog** (RB4):
   - `SchemaDef` (`CatalogEvent.h:73-79`) and `rune::catalog::SchemaState` (`CatalogState.h:23-31`) append `rune::protocol::TopicKeyFeatures keyFeatures{};`;
   - `inline constexpr uint16_t kFormatV3 = 3;` comes after `kFormatV2` (`CatalogEvent.h:52`), and `kCurrentFormatVer` stays `kFormatV2`;
   - `EncodeError::NeedsFormatV3` is appended (`CatalogEvent.h:250-258`), with its case in `encodeErrorName` (`CatalogEvent.cpp:375-379`; `-Werror=switch`, `codec/CMakeLists.txt:38`) and in `encodeErrorAnswer` (`AdminWorker.cpp:272`);
   - `struct ApplyV3` (`codec/src/catalog/ApplyV3.{h,cpp}`) has `run`, `fingerprintOfStaged` and `commit` with ApplyV2's signatures and `StagedV2` (`codec/src/catalog/ApplyV2.h`), plus `static void carryKeyFeatures(const CatalogState&, StagedV2&) noexcept` and `[[nodiscard]] static uint64_t keyFeaturesFingerprint(uint64_t fingerprint, const rune::protocol::TopicKeyFeatures&) noexcept` (identity on default features);
   - `rune::catalog::testing::goldenScriptV3()` (`GoldenScript.h`, after `goldenScriptV2()` at `:393`).
4. **`WriteState.h` (C7):** `TopicWriteSpecWire` appends `bool keyIndex = false;` (wire flags bit 1, `kTopicWriteSpecWireLen` stays 9). `TopicWriteSpec::keyIndex` (C7) is set by `setFacts`.
5. **`CheckedWriteHook.h` (03's seam):**
   - `TopicHookFacts` appends `bool keyIndexActive = false;`, and `topicHookFactsOf` (`CheckedWriteHook.h:42`) also sets the existing `keyIndex` (`:37`);
   - `HookRefusal` gets `KeyIndexActivating` before `Count_`;
   - `evaluateRequirements` adds rule 6b (P5-4), which returns `{NotLeader, KeyIndexActivating}`;
   - 03's sweep invariant (iv) becomes "every non-OK code is `InvalidRequest`, except `KeyIndexActivating` → `NotLeader`".
6. **`VerdictSource.h` / `.cpp` (04):**
   ```cpp
   inline constexpr uint64_t    kKeyIndexPlanFingerprint      = 0x4B45'5949'4E44'4558ull;   // "KEYINDEX"
   inline constexpr std::size_t kKeyIndexChunkBytes           = 64u << 10;
   inline constexpr int64_t     kKeyIndexReplayRecordsPerPass = 65536;
   struct KeyIndexFold { rune::view::ViewTable& table; uint8_t keyWidth = 0; };
   [[nodiscard]] uint64_t keyIndexWord(std::string_view wireKey) noexcept;   // LE pack; caller checked the width
   [[nodiscard]] rune::view::PrepareStatus prepareKeyIndex(const KeyIndexFold&, const rune::view::SourceRecord&,
                                                           rune::view::Prospect&) noexcept;
   [[nodiscard]] rune::view::Verdict evaluateIntent(const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
   void applyKeyIndex(KeyIndexFold&, const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
   rune::view::FoldStats foldKeyIndexBatch(KeyIndexFold&, const rune::storage::LogReadView&, int64_t bound,
                                           int64_t& cursor, const rune::view::FoldSkip&) noexcept;
   [[nodiscard]] rune::view::CompiledViewPlan keyIndexPlan(int32_t srcTopicId, uint16_t keyFieldId);   // cold
   [[nodiscard]] rune::view::RowLayout keyIndexRowLayout();                                           // rowLen 1
   ```
   - `[[nodiscard]] bool VerdictSource::activationPending() const noexcept;` (returns the private `needsActivation_`)
   - `VerdictSource::judge` / `rejects_` (`VerdictSource.cpp:214-262`) are restructured so each member kind answers its own verdict. `TestReject` keeps `{55, constraintIdOf(objectId, 1)}` and its tombstone skip, and `KeyIndex` answers `evaluateIntent` (`{56|57, 0}`), tombstones included. `prepare_` / `apply_` (`:186`, `:196`) and `replay_` (`:264`) get a `KeyIndex` arm (Task 6).
   - `VerdictSourceStats` appends `uint64_t rejectionsKeyExists = 0, rejectionsKeyNotFound = 0, keyIndexRebuilds = 0, keyIndexSkippedMalformed = 0;` (C16 ids 1 and 2).
7. **`WorkerViewSet`:**
   - `std::unordered_map<uint64_t, ViewInstance> keyIndexes_;`
   - `void dropSourceTopic(int32_t srcTopicId);` (cold)
   - test seams `[[nodiscard]] bool keyIndexContainsForTest(int32_t src, int32_t partition, std::string_view wireKey) const;`, `[[nodiscard]] std::size_t keyIndexSizeForTest(int32_t src, int32_t partition) const;` and `[[nodiscard]] int64_t keyIndexFoldCursorForTest(int32_t src, int32_t partition) const;` (−1 = none)
   - `ensureVerdictSource` (`WorkerViewSet.cpp:793-826`) and `syncConstraintSet_` (`:828-876`) read `write.spec->keyIndex`. A `WriteSpecUpdate` that changes `keyIndex` bumps `verdictEpoch_`, so the sync cache (`constraintSyncEpoch`, `constraintSyncChange`) re-runs. `driveFolds` / `driveRebuild` (`WorkerViewSet.h:241`, `:171`) keep their signatures.
   - the instances are adopted and charged through `adoptNs_` / `chargeMem_` (P5-1).
8. **Client:**
   - `enum class WriteIntent : uint8_t { Upsert = 0, Insert = 1, Update = 2 };` (C15) and `[[nodiscard]] constexpr uint8_t intentBits(WriteIntent) noexcept` in `Producer.h`;
   - a trailing `WriteIntent intent = WriteIntent::Upsert` on `trySend` (3 forms), `send` (3 forms), `trySendTo` and `sendTo`;
   - `[[nodiscard]] Completion Producer::sendAndWait(TopicHandle, const Key&, std::span<const uint8_t>, WriteIntent, std::chrono::nanoseconds);`;
   - `enum class TopicKind : uint8_t { Unknown = 0, Raw = 1, Typed = 2 };` and `TopicHandle` appends `TopicKind kind = TopicKind::Unknown;`, **before** Part 06's `partitioner`. `rawTopicAndWait` sets `Raw` and `TypedTopic::fromSchema` sets `Typed`;
   - `TopicOptions` (`Client.h:146-149`) appends `rune::protocol::TopicKeyFeatures keyFeatures{};`;
   - `WriteResult` (C6) also has `[[nodiscard]] int64_t offset() const noexcept;` and `static WriteResult fromCompletion(const Completion&) noexcept;`, plus `inline constexpr std::chrono::seconds kDefaultWriteWait{30};`;
   - `RecordOp` and `Record::attributes` / `op()` (C15);
   - `ProduceFrameBuilder::append(std::string_view, std::string_view, int64_t, uint8_t attributes = 0)`;
   - ring flags `kFlagIntentShift = 1`, `kFlagIntentMask = 0x06` (`ProducerImpl.h:180`, beside `kFlagWantAck = 0x01`);
   - no new reader function: the record walk uses `RecordBatchReader::forEachRecord`'s 5-argument visitor `(key, value, offset, timestamp, attributes)`, already on main (`RecordBatch.h:403-449`).
9. **Test support:**
   - `VerdictRig` (`broker/libs/node/tests/support/CheckedWritesRig.h:76`):
     - `struct KeyShape { rune::codec::PrimType type = rune::codec::PrimType::UInt64; uint16_t fixedLen = 0; };`;
     - `void createKeyIndex(KeyShape = {}, bool enabled = true);`, which is `typed()` (`:99`) with the shape's key and `keyIndex` in the `WriteSpecUpdate`;
     - `void enableKeyIndex();`;
     - `Rec` (`:63`) appends `uint8_t intent = 0;`, which `batch()` (`:253`) writes into the record's attributes;
     - `RigLogRec` (`:529`) appends `uint8_t intent = 0;`;
     - `std::set<std::string> keyIndexKeys();`;
     - `void holdJudge(bool)`, which forwards to the partition's `VerdictSource::holdJudgeForTest`;
     - `restart(bool reinstall = true, int64_t cutTo = -1, int specDelayPasses = 0)` (`:146`), which delivers the `WriteSpecUpdate` that many passes late. No `RigOptions` exists.
   - `CheckedWritesModel` (`CheckedWritesModel.h`): `void addKeyIndexRule();`, built on `addRule(0, …)`; `[[nodiscard]] bool isLiveAtJudge(uint64_t id) const;`; and tombstone judging for rules that opt in (Task 8; Task 9's model check exercises it).
   - matrix: `Op` 20–24, `Entity` 20–25, `expect05`, `cellsFor(Part::P05)`, pin row 05, and `VerdictRig` drivers for the six 03 timings (Task 8).

10. **`NodeConfig` test knob:** `int32_t testAdvertisedBrokerVersion = 0;` (`NodeConfig.h`, beside `testSupportedFeatureLevel` at `:363`; TEST-ONLY, 0 = `kBrokerVersion`). `BrokerNode` passes it to the gateway at start, and the OK `ConnectResponse` built at `ClientGateway.cpp:2691-2697` sets `resp.brokerVersion` from it when it is above 0. It lets a real-broker test present an older broker to the client's SD11 gate. Task 10 adds it.

## Contract gaps (flagged to the controller)

- **R03b's `OpStatus::InvalidRequest` doesn't exist** (`Completion.h:58-77`). Resolved by R05a: a `TypedTopic` can't bind to an untyped topic (Part 02), so the local refusal only ever meets a raw handle, and it is answered with the C6 value `SendResult::WriteNotAllowed`, synchronously and with no completion. No new `OpStatus` is added.
- **C8 says "05/11 define" the `atomicBatch` header bit.** This part needs none (P5-9: Tier-2 batches carry no intent). Part 11 defines it (R05b).
- **The spec's §5.2 mentions "the checkpoint" of the key index.** This part rebuilds it on every activation and never checkpoints it (P5-1, R05c). The reason is correctness under compaction. The cost is a replay of the local log per leadership. This is flagged for the owner, with the follow-up issue "checkpoint the key index, guarded by a cleaner 'tombstones removed through' watermark".
- **`TopicHandle` field order.** 05 appends `kind`, then Part 06 appends `partitioner`.
- **03's requirement sweep.** Invariant (iv) gains its one exception (contract addition 5). The sweep doubles from 147,456 to 294,912 cells, because the fact mask grows from 12 to 13 bits (`CheckedWritesHookTest.cpp:145-215`).
- **`ApplyV3` stays open until the release.** Part 06 appends autoincrement apply rules and `golden/v3/autoinc.rcat` (P5-7, RB4). `ApplyV2` is namespaces' and is frozen.

## Review Focus

These are the five inputs the spec implies but no requirement names, most likely to bite first. Each is pinned by a test in the owning task.

1. **An `insertAndWait` whose `View` ack is lost and is retried** (the same pid, epoch and sequence). It must get its original `Ok` from the index (R04e), never `KeyExists` against itself. A retry outside the window gets `VerdictUnknown`, never a fresh verdict.
   - Test: `CheckedWritesKeyIndexRegression.InsertRetryAfterALostAckReplaysOkNeverKeyExistsRegression` (Task 7).
2. **A new leadership on a node whose log changed under its old key index** (compacted since, or refilled from another leader's compacted batches, so a key's tombstone is gone). The key index must be rebuilt from the local log at every activation, never reused (P5-1). Re-inserting the deleted key is `Ok`, and every re-judged verdict equals the old leader's.
   - Test: `CheckedWritesKeyIndexRegression.EveryActivationRebuildsFromTheLocalLogRegression` (Task 7).
3. **An intent that arrives before the key index's change record is in the log.** This covers a fresh topic, an adoption of a non-empty untyped topic, and a new leader mid-scan. It must be a retriable `NotLeader`, never appended and later applied unjudged (P5-4).
   - Test: `CheckedWritesKeyIndexRegression.IntentBeforeActivationIsRetriableNeverUnjudgedRegression` (Task 7).
4. **One batch that mixes intents and plain writes on one key**, for example `[insert k, upsert k, insert k, remove k, removeExisting k, update k]`. Each record is judged against the accepted ones before it: `Ok, Ok, 56, Ok, 57, 57`. A plain write is never rejected.
   - Tests: `CheckedWritesKeyIndex.MixedIntentAndPlainRecordsInOneBatchJudgeInOrder` (Task 6) and the `SelfCollidingBatch` matrix cells (Task 8).
5. **A raw producer that sends intent bits a typed client never would.** This covers intent 1 on a tombstone, intent 3, an intent on a typed topic without a key index, and an intent on an untyped topic. The answers are `InvalidRequest` for the first three (definite, nothing appended), and opaque storage with `op()` reporting the bits for the fourth.
   - Tests: `CheckedWritesKeyIndexRegression.RawIntentShapesAreRefusedOrOpaqueRegression` (Task 10) and `CheckedWritesIntents.LocalRefusalsAreWriteNotAllowed` (Task 3).

---

## Interaction matrix

### A. The shared worker-level matrix (`rune_checked_writes_matrix_tests`, `CheckedWritesMatrix/Cell.*`, `cellsFor(Part::P05)`, Task 8)

Built on 04's `VerdictRig` (`CheckedWritesRig.h:76`) with `createKeyIndex()`: topic `kSrc = 6`, `{id uint64 key, v int64, tag uint16}` as `verdictSourceDescriptor()` declares it (`:58-61`), `key.index` on. A `TestReject{TagEquals{9}}` member (objectId 7) is installed only for the `KeyRejectedEarlier` entity. Every op is sent by an idempotent v17 producer with `All | View` (wire 3) unless the timing says otherwise.

**Ops (Part 05 range 20–39):**

| Id | Op | Record |
|---|---|---|
| 20 | `Insert` | live, intent 1 |
| 21 | `Update` | live, intent 2 |
| 22 | `Upsert` | live, intent 0 |
| 23 | `Remove` | tombstone, intent 0 |
| 24 | `RemoveExisting` | tombstone, intent 2 |

**Entities (Part 05 range 20–39), the state of key `k = 5` before the op:**

| Id | Entity | Setup |
|---|---|---|
| 20 | `KeyNew` | nothing |
| 21 | `KeyExisting` | an accepted upsert of `k`, acked |
| 22 | `KeyEarlierInBatch` | the op's batch is `[upsert k, op k]` |
| 23 | `KeyRemovedEarlier` | an upsert, then a plain remove of `k`, both acked |
| 24 | `KeyRejectedEarlier` | `insert k tag 9`, rejected 55 by `TestReject` (so `k` is absent) |
| 25 | `KeyBeforeIndex` | `createKeyIndex(…, enabled = false)`, an upsert of `k`, then `enableKeyIndex()`: the key predates the change record (the adoption path) |

**Timings:** the 21 `Timing` values through `RetryAfterWindow` (the 14 §15 values, 03's six appended values, and 04's `RetryAfterWindow`), listed explicitly in `cellsFor(Part::P05)`. Values appended later are not this part's (Part 11 is dropped, RB1). Their rig meaning is 04's and 03's. Today 03's six timings (`TxnCommitBehind`, `TxnAbortBehind`, `LeaderLossParked`, `AckDeadline`, `StrictTier`, `GatewayRouted`) are driven only on `AckRig` (`CheckedWritesViewAckMatrixTest.cpp:44-120`), and 04's `CellRun` drives the other 15 on `VerdictRig` (`CheckedWritesMatrixTest.cpp`). Task 8 ports the six drivers to `VerdictRig`. In this part:
- `Racing`: two producers send the op on `k` in the same pass.
- `CreateOnNonEmpty`: 500 upserts of other keys, then `enableKeyIndex()`, then the op while the key index replays.
- `TxnCommitBehind` / `TxnAbortBehind`: a third producer's open transaction upserts `k` before the op and later commits or aborts.

**Not applicable (top-down; the first rule that matches names the reason):**
1. `Timing ∈ {ReplaceDuringWrites, DropDuringWrites}`: "key.index is immutable (no replace or drop)" (5 × 6 × 2 = 60 cells).
2. `KeyRejectedEarlier × {TxnCommitBehind, TxnAbortBehind}`: "the TestReject member makes the source constrained, and transactions are refused there before Part G" (5 × 1 × 2 = 10 cells).

**Cell count:** 5 × 6 × 21 = 630. 560 are applicable and 70 skipped, pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 05).

**The reference verdict** comes from `CheckedWritesModel` with `addKeyIndexRule()` (Task 8). For the plain `Single` timing it equals this table (`expect05` asserts it directly, as a check on the model):

| Op \ Entity | New | Existing | EarlierInBatch | RemovedEarlier | RejectedEarlier | BeforeIndex |
|---|---|---|---|---|---|---|
| Insert | Ok | 56 | 56 | Ok | Ok | 56 |
| Update | 57 | Ok | Ok | 57 | 57 | Ok |
| Upsert | Ok | Ok | Ok | Ok | Ok | Ok |
| Remove | Ok | Ok | Ok | Ok | Ok | Ok |
| RemoveExisting | 57 | Ok | Ok | 57 | 57 | Ok |

**`expect05(cell)`, top-down:**
1. `FailoverAppendHw`, `TruncateTail` → the first ack is `NotLeader`, and the re-sent batch's ack carries the model's verdict.
2. `LeaderLossParked` → `NotLeader`. `AckDeadline` → `Internal`, and once the fold resumes the log holds the model's verdict.
3. `RetryAfterWindow` → `DuplicateSequence` (21) with no trailer (R04e).
4. `RetryBeforeJudge` / `RetryAfterJudge` → the duplicate's ack is byte-identical to the original's, and the record is stored once.
5. `SelfCollidingBatch` → the op twice in one batch: Insert/New `[Ok, 56]`, Update/Existing `[Ok, Ok]`, RemoveExisting/Existing `[Ok, 57]`, Remove `[Ok, Ok]`, Upsert `[Ok, Ok]`. Other entities follow the model.
6. `Racing` → for Insert/New exactly one `Ok` and one 56, and the `Ok` belongs to the lower offset.
7. Every other cell → one `Ok` ack whose trailer is exactly the model's rejections `(recordIndex, 56|57, 0)`.

**Every applicable cell asserts**, after `driveUntilJudged(leo())`, then `compact()`, then `restart()` and a second `driveUntilJudged`:
- (a) the `rejected` entries equal the model's verdicts, with `constraintId == 0` for 56/57;
- (b) `rig.keyIndexKeys()` equals the model's live keys of accepted records, both before and after the `restart()` rebuild;
- (c) a raw subscription from 0 receives exactly the accepted records, and each record's `op()` matches its intent (P5-11);
- (d) the plain projection view equals a fresh refold of the accepted records;
- (e) after `compact()` every key's latest record in the log is the latest accepted one, or absent once deleted;
- (f) `rejected.mismatches() == 0` and `keyIndexSkippedMalformed == 0`;
- (g) `rejectionsKeyExists` and `rejectionsKeyNotFound` equal the model's counts.

### B. The key-width sweep (`rune_checked_writes_verdict_tests`, `CheckedWritesKeyIndexWidths/Shape.*`, Task 8)

- **Shapes (7):** `uint16`, `int32`, `uint32`, `int64`, `uint64`, `string(1)`, `string(8)`. The rig encodes `Rec::id` at the shape: integers little-endian at their width, and `string(N)` as the decimal digits zero-padded to N.
- **Cases:**
  - every op × {`KeyNew`, `KeyExisting`} at the `Single` timing: 70 cells, each asserting the table above;
  - `KeysDifferingInTheLastByteAreDistinct`: two keys equal except the last byte → both inserts `Ok`;
  - `RemoveRoutesAndErasesLikeSend`: `remove(id)` erases the key that `send(rec)` put.
- **DDL refusals:** `string(9)` and `string(16)` are refused (SD8) by the Task 5 DDL test, not here.

### C. Named cells outside the matrix

| §15 / overview cell | Test (task) |
|---|---|
| typed `remove` routes as `send(rec)`, every key width | `CheckedWritesBrokerRegression.TypedRemoveRoutesAsSendForEveryKeyWidthShardRegression` (10) |
| intents sent to an older cluster (a v9 broker; a v10 broker at level 4) | `CheckedWritesBrokerRegression.IntentsToAnOlderClusterAreRefusedShardRegression` (10) |
| intents without `View` or from a non-idempotent producer | Part 03 (rules) + `CheckedWritesIntents.LocalRefusalsAreWriteNotAllowed` (3) |
| key.index refused on untyped, keyless, transient, wide and pre-level-5 topics | `CheckedWritesKeyIndexDdl.Refusals` (5), `CheckedWritesBrokerRegression.KeyIndexDdlRefusalsShardRegression` (10) |
| several verdict views, the key index rejects first | `CheckedWritesKeyIndex.KeyIndexIsFirstInSetOrder` (6) |
| restart: the key index rebuilds identically | matrix (b), `CheckedWritesKeyIndexRegression.RestartRebuildsTheSameKeySetRegression` (7) |
| a leader without the key index spec waits | `CheckedWritesKeyIndexRegression.LeaderWithoutTheKeyIndexSpecWaitsRegression` (7) |

### D. Model check and cluster stress (Task 9)

- **Model check.** `CheckedWritesKeyIndexModelRegression.SeededRigMatchesTheModel` runs seeds 1–100 × 250 steps on the rig. Actions (one per step, from the seed's `std::mt19937_64`):
  - the five ops on 8 keys, from 2 idempotent `All | View` producers, in batches of 1–8 records, so keys repeat within a batch;
  - plain upserts and removes from a non-idempotent `acks=leader` producer;
  - a `TestReject{TagEquals{9}}` member installed, replaced or dropped;
  - an idempotent re-send before and after the judge;
  - `failover()`, `demotePromote()`, `restart()`, `truncateTo(hw)`, `compact()`;
  - an open transaction of plain upserts, committed or aborted.

  After every step it checks 04's six invariants plus two of this part's:
  - **`IntentVerdictsExact`:** every 56/57 ever reported (an ack, the index or a marker) equals the model's, and no accepted record's verdict ever turns into 56/57.
  - **`KeyIndexEqualsLiveKeys`:** after each `driveUntilJudged`, the key index equals the model's live keys.

  `CheckedWritesKeyIndexModelRegression.SameSeedSameTrace` re-runs seeds 1–8 twice and compares the trace hash. A failure prints `seed=<n> step=<k> action=<name>`.
- **Cluster stress.** `CheckedWritesStressRegression.KeyIndexSeededClusterHoldsTheInvariants` runs seeds 1–8 × 300 steps on 04's `CheckedWritesStressDriver` (`ThreeNodes`, default shard mode). It uses a `key.index` topic `{id uint64 key, name string(16), n int64}` (P = 2, replicas 3, compact). Three typed clients send `insertAndWait` / `updateAndWait` / `removeExistingAndWait` / `removeAndWait` on 64 ids, and the faults are 04's. `IntentVerdictsExact` and `KeyIndexEqualsLiveKeys` are registered through `registerInvariant`, the key set read on each node's owner thread, together with 04's five.

### E. Docker e2e (VM, Task 11)

`broker/docker/checked-writes-e2e.sh`, three authenticated nodes; legs are listed in Task 11.

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `protocol/include/rune/protocol/CatalogTypes.h` | M | `Partitioner`, `TopicKeyFeatures`, defaults, the 14-byte block codec |
| `protocol/include/rune/protocol/client/SchemaMessages.h:181-270,359-397` | M | `DefineTopic` / `TopicSchemaResponse` v17 trailers |
| `protocol/conformance/{gen_vectors.cpp,manifest.md,vectors/define_topic_v17_key_index.bin,vectors/topic_schema_response_v17_key_index.bin}`, `protocol/tests/protocol/client/{SchemaMessagesTest.cpp,ConformanceTest.cpp}` | M/C | Golden frames and tests |
| `codec/include/rune/catalog/{CatalogEvent.h,CatalogState.h}`, `codec/src/catalog/{CatalogEvent.cpp,CatalogState.cpp}`, `codec/src/catalog/ApplyV3.{h,cpp}` (C) | M/C | `formatVer` 3 (`CatalogFingerprint.*`, `ApplyV1.*`, `ApplyV2.*` are frozen and not edited) |
| `codec/include/rune/catalog/testing/GoldenScript.h`, `codec/tools/make_catalog_golden.cpp`, `codec/tests/catalog/golden/v3/basic.rcat` (C), `codec/tests/catalog/{CatalogGoldenPins.h,CatalogEventTest.cpp,CatalogApplyV1Test.cpp}`, `codec/tests/catalog/CheckedWritesCatalogV3Test.cpp` (C), `codec/CMakeLists.txt` (`rune_catalog` sources, `rune_catalog_tests`) | M/C | v3 golden and tests. broker/ and client-cpp/ glob `src/catalog/*.cpp` and need no edit |
| `broker/libs/node/include/rune/node/AdminChannels.h:267-284,683-688,800`, `broker/libs/gateway/include/rune/gateway/IAdminRequestSink.h:92-99`, `broker/libs/gateway/src/ClientGateway.cpp:8425`, `broker/libs/node/include/rune/node/BrokerNode.h:262,766`, `broker/libs/node/src/BrokerNode.cpp:4410-4417` | M | Features from the wire to the admin thread and back |
| `broker/libs/node/{include/rune/node/AdminWorker.h:81-85,src/AdminWorker.cpp:272,642-677,1354,1387-1478}` | M | DDL rules (P5-6), `formatVer` by level, `respondGet` features, `NeedsFormatV3` answer |
| `broker/libs/node/tests/{integration/SchemaGatewayTest.cpp,integration/CreateViewRegressionTest.cpp,support/CatalogClusterHarness.h,support/CatalogTrio.h,support/CatalogDdlRig.h}`, `broker/libs/node/tests/node/CatalogWritePathTest.cpp` | M | The `respondGet` assignments that must take the new parameter; the `EncodeError` "every enumerator" test |
| `broker/libs/node/include/rune/node/WriteState.h` | M | `TopicWriteSpecWire::keyIndex`, `TopicWriteSpec::keyIndex`, `writeSpecWireOf` |
| `broker/libs/node/{include/rune/node/VerdictSource.h,include/rune/node/VerdictState.h,src/VerdictSource.cpp}` | M | The key index member, the per-kind `judge` (contract addition 6), the stats |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | `keyIndexes_`, the desired set, rebuild at activation, `dropSourceTopic` |
| `broker/libs/node/include/rune/node/Worker.h` | M | `driveViews_` short-circuit (`:7006-7009`), `TopicDropped` → `dropSourceTopic` (`:2790-2793`), the key index released with the judge in `dropWriteSpec_` (`:3385-3395`) |
| `broker/libs/node/{include/rune/node/CheckedWriteHook.h,src/CheckedWriteHook.cpp}` | M | `keyIndex` / `keyIndexActive`, rule 6b, the insert tombstone |
| `broker/libs/node/tests/node/CheckedWritesKeyIndexTest.cpp` (C), `CheckedWritesKeyIndexDdlTest.cpp` (C), `CheckedWritesKeyIndexWorkerTest.cpp` (C), `CheckedWritesHookTest.cpp` (M) | C/M | Unit, DDL and rig tests |
| `broker/libs/node/tests/support/{CheckedWritesRig.h,CheckedWritesModel.h,CheckedWritesMatrixExpectations.h,CheckedWritesStressDriver.h}` | M | 05's rig helpers, model rule, matrix rows |
| `broker/libs/node/tests/node/{CheckedWritesMatrixTest.cpp,CheckedWritesModelTest.cpp}`, `tests/integration/CheckedWritesStressTest.cpp` | M | Matrix (P05 instantiation, the six 03 timing drivers), model check, stress |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`, `broker/libs/node/tests/client/support/KeyIndexRecords.h` (C) | M/C | Real-broker slice |
| `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` | M | Zero-alloc gate |
| `broker/libs/node/CMakeLists.txt` | M | New sources in the existing binaries |
| `client-cpp/include/rune/client/producer/{Producer.h,ProducerImpl.h,ProduceFrameBuilder.h}`, `client-cpp/src/producer/{Producer.cpp,ProducerImpl.cpp}`, `client-cpp/include/rune/client/shard/ClientShard.h:99,107` and `client-cpp/src/shard/ClientShard.cpp` (`ClientShard` forwarding) | M | `WriteIntent`, intent plumbing, local refusals, `sendAndWait` |
| `client-cpp/include/rune/client/{TopicHandle.h,Client.h,TypedTopic.h,WriteResult.h (C)}`, `client-cpp/src/Client.cpp:1393-1418` | M/C | `TopicKind`, `TopicOptions::keyFeatures`, the typed API, `WriteResult` |
| `client-cpp/include/rune/client/subscriber/Subscription.h:117-123`, `client-cpp/src/subscriber/SubscriberHub.cpp:643-660` | M | `Record::attributes`, `op()` |
| `client-cpp/tests/producer/CheckedWritesIntentsTest.cpp` (C), `client-cpp/tests/client/CheckedWritesTypedTopicTest.cpp` (C), `client-cpp/tests/subscriber/CheckedWritesRecordOpTest.cpp` (C), `client-cpp/CMakeLists.txt` | C/M | Client suites |
| `client-cpp/examples/checked_writes_probe.cpp` (C), `client-cpp/tests/support/GenClaimSchemas.cpp` (M, `ki_account`), `client-cpp/docker/Dockerfile.checked-writes-probe{,.dockerignore}` (C), `broker/docker/checked-writes-e2e.sh` (C), `broker/docker/docker-compose.checked-writes.yml` (C) | C/M | The docker e2e |
| `broker/benchmark/ViewFoldBench.cpp` | M | `BM_ViewFoldVerdict/key_index` |
| `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `broker/docs/CATALOG.md`, `docs/operations.md`, `docs/testing.md`, `client-cpp/CLAUDE.md` | M | Docs this part ships |

---

## Phase 1 — Protocol, catalog and the client surface

### Task 1: `TopicKeyFeatures` and the v17 `DefineTopic` / `TopicSchemaResponse` trailers

**Files:**
- Modify: `protocol/include/rune/protocol/CatalogTypes.h` (append), `protocol/include/rune/protocol/client/SchemaMessages.h:181-270` (`DefineTopic`; decode ends `if (pos != avail) return nullopt` at `:265`), `:359-397` (`TopicSchemaResponse`)
- Modify: `protocol/conformance/gen_vectors.cpp`, `protocol/conformance/manifest.md`. Create the two vectors.
- Test: `protocol/tests/protocol/client/SchemaMessagesTest.cpp`, `protocol/tests/protocol/client/ConformanceTest.cpp`

**Interfaces:**
- Produces: C13's `Partitioner`, `kAutoIncrementMaxDefault`, `kAutoIncrementReserveDefault` and `TopicKeyFeatures`, verbatim; contract additions 1 and 2.
  - **Block layout:** `[flags u8: bit0 keyIndex, bit1 autoIncrement][partitioner u8][autoIncrementMax u64 LE][autoIncrementReserve u32 LE]`.
  - **`getTopicKeyFeatures`** returns false for `avail < 14`, for `flags & 0xFC`, or for a `partitioner > 1`.
  - **`DefineTopic`:**
    - `fits()` is also false for `hasKeyFeatures && !hasExt`;
    - `encodedSize()` adds 14 when `hasKeyFeatures`;
    - `encode` writes the block after `putExt`;
    - `decode`, after `getExt`, reads the block when `pos < avail`, then requires `pos == avail`.
  - **`TopicSchemaResponse`:** `encodedSize()` adds 14 when `hasKeyFeatures`; `encode` writes the block after `partitionCount`; `decode` reads it iff `avail >= pos + 14` after `partitionCount`, and a 1–13 byte remainder decodes as no block.

- [ ] **Step 1: Write the failing tests** (`SchemaMessagesTest.cpp`).
  - `CheckedWritesSchemaWire.KeyFeaturesBlockRoundTrips`: `{keyIndex true, autoIncrement false, Fnv1a, 2^53−1, 4096}` → 14 bytes `01 00 FF FF FF FF FF FF 1F 00 00 10 00 00`, which decode equal. Flags `0x04` → false. Partitioner 2 → false. 13 bytes → false.
  - `CheckedWritesSchemaWire.DefineTopicCarriesKeyFeaturesAfterTheExtension`: `hasExt`, `hasKeyFeatures`, `keyIndex` → `encodedSize()` is the v8 size + 14, the decode is equal, and `hasKeyFeatures` is true. The frame cut by 1 byte → `nullopt`. One stray byte after the block → `nullopt`.
  - `CheckedWritesSchemaWireRegression.DefineTopicWithoutFeaturesIsByteIdenticalRegression`: `hasKeyFeatures = false` → the bytes equal `define_topic_v8.bin` (the only DefineTopic vector, `manifest.md:940`, built at `gen_vectors.cpp:465-476`, today checked in `CatalogDdlMessagesTest.cpp:477`).
  - `CheckedWritesSchemaWire.KeyFeaturesNeedTheExtension`: `hasKeyFeatures && !hasExt` → `fits() == false` and `encode` returns 0.
  - `CheckedWritesSchemaWire.TopicSchemaResponseTrailer`: with the block → decodes `hasKeyFeatures`, features equal. Without it → the decode leaves `hasKeyFeatures == false`. A frame with 1–13 extra bytes → decodes with `hasKeyFeatures == false` and `partitionCount` correct.
  - `ConformanceTest.cpp`: `ConformanceGolden.DefineTopicV17KeyIndex` (the `define_topic_v8.bin` fields plus `keyIndex`, re-encoded byte-equal) and `ConformanceGolden.TopicSchemaResponseV17KeyIndex`. There is no manifest-table check test: document both frames in `manifest.md` (Step 3) instead.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j18 --target rune_protocol_tests`. Expected: `no member named 'hasKeyFeatures' in 'DefineTopic'`.
- [ ] **Step 3: Implement** as specified. Generate the vectors with `c++ -std=c++23 -I protocol/include protocol/conformance/gen_vectors.cpp -o "$TMPDIR/rune_gen_vectors" && "$TMPDIR/rune_gen_vectors" protocol/conformance/vectors`. Then `git status --porcelain protocol/conformance/vectors` must list only the two new files; anything else is a wire break, so stop. Document both frames in `manifest.md`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesSchemaWire|Conformance|SchemaMessages' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/include/rune/protocol/CatalogTypes.h protocol/include/rune/protocol/client/SchemaMessages.h \
          protocol/conformance/gen_vectors.cpp protocol/conformance/manifest.md \
          protocol/conformance/vectors/define_topic_v17_key_index.bin protocol/conformance/vectors/topic_schema_response_v17_key_index.bin \
          protocol/tests/protocol/client/SchemaMessagesTest.cpp protocol/tests/protocol/client/ConformanceTest.cpp
  git commit -m "feat(protocol): TopicKeyFeatures and the v17 DefineTopic / TopicSchemaResponse key-feature trailers (C13)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 2: Catalog `formatVer` 3

**Files:**
- Modify: `codec/include/rune/catalog/CatalogEvent.h`:
  - `:52-53`: `kFormatV3 = 3` after `kFormatV2`, with `kCurrentFormatVer` unchanged;
  - `:73-79`: `SchemaDef::keyFeatures`;
  - `:250-258`: `EncodeError::NeedsFormatV3`;
  - the header comment `:7-10` ("The broker writes only formatVer 2") and the `UnknownFormatVer` comment ("formatVer not 1 or 2"), both updated to the level rule.
- Modify: `codec/include/rune/catalog/CatalogState.h:23-31` (`SchemaState::keyFeatures`).
- Modify: `codec/src/catalog/CatalogEvent.cpp`:
  - `checkFormatVer` (`:46-49`, accept 1–3);
  - `validate` (`:89-96`): a v1 or v2 event with non-default features → `NeedsFormatV3`;
  - `putSchema` / `getSchema` (`:125-127`, `:205-209`): the block iff `formatVer >= kFormatV3`, and the writer and reader signatures that need the event's `formatVer`;
  - `encodeErrorName` (`:375-379`).
  - `checkKind` (`:52`) and the header `nsId` (`:295`, `:329`) already test `>= kFormatV2`, so v3 inherits them. Do not edit them.
- Modify: `codec/src/catalog/CatalogState.cpp`:
  - `apply` / `previewFingerprint` (`:119-149`): accept 3, a `kFormatV3` arm to `ApplyV3`, and for every format `ApplyV3::carryKeyFeatures` plus `ApplyV3::keyFeaturesFingerprint` around the format's staged fingerprint;
  - `digest()` (`:82-112`): `keyFeaturesFingerprint` on each topic before its `nsId` layer.
- Create: `codec/src/catalog/ApplyV3.h`, `codec/src/catalog/ApplyV3.cpp`. List the `.cpp` in the `rune_catalog` sources of `codec/CMakeLists.txt` (`:23-34`, beside `ApplyV2.cpp` at `:29`). `broker/CMakeLists.txt:168` and `client-cpp/CMakeLists.txt:216` glob `src/catalog/*.cpp`, so they need no edit.
- Do not modify: `CatalogFingerprint.{h,cpp}` (frozen, `CatalogFingerprint.h:1-12`), `CatalogFingerprintV2.*`, `ApplyV1.*`, `ApplyV2.*`, `golden/v1/*`, `golden/v2/*`, `kBasicV1Digests`, `kNamespacesV2Digests`.
- Modify: `codec/include/rune/catalog/testing/GoldenScript.h`, which gets a new `goldenScriptV3()` after `goldenScriptV2()` (`:393`), built with `ScriptBuilder(kFormatV3)` (`:76`). `codec/tools/make_catalog_golden.cpp` gets a positional `v3` mode beside `v1|v2` (`:13-21`) that writes the v3 script and prints `kBasicV3Digests`.
- Create: `codec/tests/catalog/golden/v3/basic.rcat`, `codec/tests/catalog/CheckedWritesCatalogV3Test.cpp` (in `rune_catalog_tests`, `codec/CMakeLists.txt:242`). Modify: `codec/tests/catalog/{CatalogGoldenPins.h,CatalogEventTest.cpp,CatalogApplyV1Test.cpp}`.
- Modify: `broker/libs/node/src/AdminWorker.cpp:272` (`encodeErrorAnswer` gains `NeedsFormatV3` → `InvalidRequest`) and `broker/libs/node/tests/node/CatalogWritePathTest.cpp:509-520` (its "every enumerator" list). Both are needed in this task because `-Werror=switch` breaks `build/root` otherwise.

**Interfaces:**
- Produces: contract addition 3, P5-7.
- `goldenScriptV3()` has 9 events at offsets 0–8, all with `formatVer = kFormatV3`:
  - 0: `CreateTopic` typed keyed `ki` with `keyIndex`;
  - 1: `CreateTopic` untyped `raw`;
  - 2: `CreateSchema` adopting `raw` with `keyIndex`;
  - 3: `AlterSchema` ADD on `ki`;
  - 4: `SetCompatibility` on `ki`;
  - 5: `CreateTopic` typed `plain` without features;
  - 6: `AlterTopicConfig` on `ki`;
  - 7: `DropTopic` `plain`;
  - 8: `NamespaceCreated` `ns1`, which proves v3 keeps the v2 kinds.
  - Events 0–7 are in the default namespace (`kDefaultNsId = 0`, `NsName.h:17`).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesCatalogV3Test.cpp`).
  - `CheckedWritesCatalogV3.SchemaDefBlockRoundTripsInV3`: a v3 `CreateTopic` with features → `encode` / `decode` are equal, and the body bytes end with the 14-byte block.
  - `CheckedWritesCatalogV3.DefaultFeaturesStillEncodeTheBlockInV3`: the block is present (v3 always writes it in a `SchemaDef`) and decodes to the defaults.
  - `CheckedWritesCatalogV3Regression.V1AndV2EventsWithFeaturesAreRefusedRegression`: `formatVer` 1 or 2 + `keyIndex` → `EncodeError::NeedsFormatV3`. With default features → the v1 and v2 bytes are unchanged.
  - `CheckedWritesCatalogV3.BadFlagBitsAreBadEnum`: flags `0x04` or partitioner 2 in a v3 body → `DecodeError::BadEnum`.
  - `CheckedWritesCatalogV3.V3KeepsNamespaces`: a v3 `CreateTopic` with `nsId = 7` round-trips its `nsId`, and a v3 `NamespaceCreated` decodes.
  - `CheckedWritesCatalogV3.ApplyCarriesFeaturesAcrossAlterSchema`: `CreateTopic(ki, keyIndex)`, then `AlterSchema` ADD (v3) → `topicByName(kDefaultNsId, "ki")->schema->keyFeatures.keyIndex`. The same `AlterSchema` as a **v2** event → still `keyIndex`.
  - `CheckedWritesCatalogV3.ApplyRefusesKeyIndexWithoutAKeyOrOnTransient`: `keyIndex` with an empty `keyField` → `Precondition`. On a `Persistence::Transient` topic → `Precondition`.
  - `CheckedWritesCatalogV3.FingerprintIgnoresDefaultFeatures`: a default-feature topic has the same fingerprint, and the state the same `digest()`, under v2 and v3. With `keyIndex` both differ.
  - `CheckedWritesCatalogV3.FormatFourHasNoRules`: `formatVer 4` → `UnsupportedFormat`.
  - The two existing "unknown format" cases move from 3 to 4, because 3 now has rules. Record both in the commit body:
    - `CatalogApplyV1Test.cpp:476` (`v2.formatVer = 3;   // 1 and 2 have rules; 3 does not`) becomes `= 4`, with the comment "1, 2 and 3 have rules; 4 does not";
    - `CatalogEventTest.cpp:398` (`uint16_t{3}` as the unknown `formatVer`, for both encode and decode) becomes `uint16_t{4}`.
  - `CatalogV3GoldenRegression.LogReplaysToPinnedFingerprints`, `…TheGoldenScriptStillProducesTheCheckedInBytes` and `…TheGoldenLogDecodesToTheScriptEventByEvent` (in `CheckedWritesCatalogV3Test.cpp`) mirror the v2 ones (`NamespaceCatalogV2Test.cpp:812`, `:838`) over `golden/v3/basic.rcat` and `kBasicV3Digests`. The v1 tests (`CatalogGoldenTest.cpp:12,26,39`) and the v2 ones stay unchanged and green. So does `CurrentFormatVerIsTwoRegression` (`NamespaceCatalogV2Test.cpp:107`).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-codec -j18 --target rune_catalog_tests`. Expected: `no member named 'keyFeatures' in 'SchemaDef'`.
- [ ] **Step 3: Implement** as specified (P5-7).
  - `ApplyV3::run`:
    1. call `ApplyV2::run`;
    2. for the `Staged` alternative of a `CreateTopic` / `CreateSchema`, set the staged topic's `schema->keyFeatures = def.keyFeatures`;
    3. check the two v3 rules.
  - `CatalogState.cpp` then calls `carryKeyFeatures` for every format.
  - `ApplyV3::fingerprintOfStaged` / `commit` forward to `ApplyV2`'s.
  - Generate the v3 golden once: `cmake --build build/sub-codec --target make_catalog_golden && build/sub-codec/make_catalog_golden codec/tests/catalog/golden/v3/basic.rcat v3`. Append the printed array at the end of `CatalogGoldenPins.h` as `kBasicV3Digests`, under the same "FROZEN" comment the v1 and v2 arrays carry (`:2`, `:37`).
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/sub-codec -R 'CheckedWritesCatalogV3|CatalogV3Golden|CatalogV2Golden|CatalogGolden|NamespaceCatalogV2|CatalogEvent|CatalogApply|CatalogReplayer|CatalogFingerprint' --output-on-failure`. Expected: PASS. The v1 and v2 golden tests pass unchanged, which proves no v1 or v2 rule moved.
  - Then `cmake --build build/root -j18 && ctest --test-dir build/root -R 'Catalog' -j18 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add codec/include/rune/catalog codec/src/catalog codec/tools/make_catalog_golden.cpp codec/tests/catalog codec/CMakeLists.txt \
          broker/libs/node/src/AdminWorker.cpp broker/libs/node/tests/node/CatalogWritePathTest.cpp
  git commit -m "feat(catalog): formatVer 3 - TopicKeyFeatures in SchemaDef, ApplyV3 over ApplyV2 (carry + key.index rules), v3 golden; v1 and v2 unchanged" \
             -m "The 'no rules' formatVer cases in CatalogApplyV1Test and CatalogEventTest move from 3 to 4 (3 has rules)." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 3: Client intents: `WriteIntent`, the ring and builder plumbing, local refusals, `sendAndWait`, `record.op()`, `TopicKind`, the define options

**Files:**
- Modify: `client-cpp/include/rune/client/producer/Producer.h`: beside `Acks` (`:88`) add `WriteIntent` and `intentBits`; give `:638`, `:708`, `:716-718` and `:730-744` a trailing `intent`; add a new `sendAndWait` beside `flushAndWait` (`:942`).
- Modify: `client-cpp/include/rune/client/producer/ProducerImpl.h`:
  - `:27` and `:180`: the flags byte doc, `kFlagIntentShift` and `kFlagIntentMask`, beside `kFlagWantAck = 0x01`;
  - `:665`: `trySubmit` gains `uint8_t intent`;
  - `:1548`: `trySubmitRing_`.
- Modify: `client-cpp/src/producer/ProducerImpl.cpp`:
  - `submitSendRecord` (`:375`, flags);
  - `trySubmit` / `trySubmitRing_` / `tryAppendDirect_` (`:439`, `:480`, `:499`; direct-mode flags at `:535`);
  - the deferred records (`:543-610`);
  - `appendRecord_` (`:1761`, the attributes to `builder->append` at `:1773` and `:1820`);
  - every `flags` read is audited (`grep -n "flags" client-cpp/src/producer/ProducerImpl.cpp`) so that the want-ack meaning is always `flags & kFlagWantAck`, never `flags != 0`. Today the `kFlagWantAck` tests are at `:915`, `:1585`, `:1723` and `:1751`, and main has no `flags != 0`.
- Modify: `client-cpp/include/rune/client/producer/ProduceFrameBuilder.h`: `append` at `:84` gains attributes, and in `writePrefix_` `pre[n++] = 0` at `:235` becomes `pre[n++] = attributes`. Also modify `client-cpp/src/producer/Producer.cpp` (the forwards, the local refusals, `sendAndWait`) and the `ClientShard` send forwards (`client-cpp/include/rune/client/shard/ClientShard.h:99,107` inline, `client-cpp/src/shard/ClientShard.cpp`).
- Modify: `client-cpp/include/rune/client/TopicHandle.h:34-50` (`TopicKind`, `kind`), `client-cpp/src/Client.cpp` (`rawTopicAndWait` `:2940-2947` sets `Raw`; `defineTopic` `:1393-1418` sends the features; the duty-side DDL send gates them, see below)
- Modify: `client-cpp/include/rune/client/Client.h:146-149` (`TopicOptions::keyFeatures`)
- Modify: `client-cpp/include/rune/client/subscriber/Subscription.h:117-123`, `client-cpp/src/subscriber/SubscriberHub.cpp:643-660` (the `forEachRecord` lambda).
- Already on main, not edited: the record intent bits `kAttrIntentMask`, `Intent` and `intentOf` (`protocol/include/rune/protocol/RecordBatch.h:41-45`), `RecordBatchBuilder::append(…, attributes)` (`:139`), and the 5-argument `forEachRecord` visitor (`:403-449`).
- Create: `client-cpp/tests/producer/CheckedWritesIntentsTest.cpp` (add to `rune_invoker_tests`, `client-cpp/CMakeLists.txt:1547`, beside `ProducerAcksFlagsTest.cpp`), `client-cpp/tests/subscriber/CheckedWritesRecordOpTest.cpp` (add to `rune_subscriber_tests`, `:1436`)

**Interfaces:**
- Consumes: 03's `Acks` flags (`has`, `normalized`), `SendResult::WriteNotAllowed` (`Producer.h:335-362`; nothing returns it yet), the SD11 seal gate (`ProducerImpl.cpp:2302-2305`), the record intent bits (`RecordBatch.h:41-45`); 02's `rawTopicAndWait`; 04's per-record completion mapping; C13 (Task 1).
- Produces: contract addition 8 (minus `WriteResult` and the typed API, which are Task 4):
  ```cpp
  enum class WriteIntent : uint8_t { Upsert = 0, Insert = 1, Update = 2 };   // an empty value with Update = delete-existing
  [[nodiscard]] constexpr uint8_t intentBits(WriteIntent i) noexcept { return static_cast<uint8_t>(i); }
  [[nodiscard]] Completion Producer::sendAndWait(TopicHandle topic, const Key& key, std::span<const uint8_t> value,
                                                 WriteIntent intent, std::chrono::nanoseconds timeout);
  enum class RecordOp : uint8_t { Upsert, Insert, Update, Delete };
  struct Record { /* existing */ uint8_t attributes = 0; [[nodiscard]] RecordOp op() const noexcept; };
  enum class TopicKind : uint8_t { Unknown = 0, Raw = 1, Typed = 2 };
  ```
  - **The local refusals (P5-9)** run in `Producer::trySend` / `trySendTo` (the handle is known there) before `trySubmit`. The rules live in one `[[nodiscard]] static const char* intentRefusal_(…) noexcept` that returns the P5-9 text, or nullptr when the send is allowed. `trySend` maps non-null to `WriteNotAllowed`. The blocking `send` calls it first and throws `std::invalid_argument(reason)`, because `throwForSendResult` (`ProducerImpl.cpp:1050-1085`) has one generic message for `WriteNotAllowed` (`:1083`). They sit behind one `[[unlikely]] if (intent != WriteIntent::Upsert || (viewAcks_ && topic.kind == TopicKind::Raw))`, where `viewAcks_` is the producer's constant `has(acks, View)`, so an upsert pays a single predicted branch.
  - **The intent rides the ring flags byte as bits 1–2** (`flags |= intentBits(intent) << kFlagIntentShift`). Direct mode passes the same flags to `appendRecord_`. Deferred records keep their flags. `appendRecord_` passes `(flags & kFlagIntentMask) >> kFlagIntentShift` as the record's `attributes`.
  - **`sendAndWait`** goes through `impl_->hooks().andWait` exactly as `txnAndWait_` does (`Producer.cpp:567-590`). The issue lambda calls `trySend(topic, key, value, *target.cq, token, intent)`, then `flush()`. It maps `SendResult::Ok` to an accepted issue, and every other result to a thrown `std::invalid_argument` (P5-9 refusals, `KeyTooLong`, `MessageTooLarge`) or `std::runtime_error` (back-pressure past the timeout). It returns the `Send` completion unchanged.
  - **`defineTopic`** sets `msg.hasKeyFeatures = topic.keyFeatures != TopicKeyFeatures{}` and `msg.keyFeatures`. When `hasKeyFeatures` and the seed session's broker version is in `[1, kBrokerVersionCheckedWrites)`, the op completes `OpStatus::BrokerTooOld` locally and nothing is sent.
    - `defineTopic` runs on the caller's thread, and the version the producers read lives duty-side in `ProducerHub::brokerVersion_` (`ProducerHub.h:331`, set at `Client.cpp:2439`).
    - So the gate runs on the duty thread where the queued DefineTopic control frame is sent (the `pendingCtl_` path, `Client.cpp:4783`). It reads `agent_.sessionBrokerVersion(0)`, as `onSeedConnected_` does (`:3986`). The frame's `hasKeyFeatures` comes from decoding it (`:1529`).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesIntentsTest.cpp` (rigs as in `ProducerAcksFlagsTest.cpp`, `support/FrameCapture.h`):
    - `CheckedWritesIntents.IntentBitsReachTheRecordAttributes`: an `All | View` producer with `brokerVersion 10` sends `(k1, v, Insert)`, `(k2, v, Update)`, `(k3, {}, Update)`, `(k4, v)` → one captured frame. `forEachRecordSpan` yields attributes `{1, 2, 2, 0}`, and the batch CRC is valid.
    - `CheckedWritesIntents.UpsertFramesAreByteIdenticalRegression`: the same sends with no intent argument and with an explicit `WriteIntent::Upsert` → byte-equal frames, which also equal a capture from before this task (the golden bytes of `ProducerAckPathTest.cpp`'s first frame).
    - `CheckedWritesIntents.LocalRefusalsAreWriteNotAllowed`:
      - `TopicHandle{.topicId = 3, .partitionCount = 1, .kind = TopicKind::Raw}` + `Insert` → `SendResult::WriteNotAllowed`;
      - an `All` producer (no `View`) + `Update` → `WriteNotAllowed`;
      - an `All | View` producer on the `Raw` handle + `Upsert` → `WriteNotAllowed` (R05a: `View` on an untyped topic);
      - a transactional `All | View` producer + `Insert` → `WriteNotAllowed`;
      - `Insert` with an empty value → `WriteNotAllowed`;
      - in every case nothing is queued (`cap.frames.empty()` after a flush) and no completion arrives in the cq.
      - The blocking `send` throws `std::invalid_argument` whose `what()` contains, respectively, "intents need a typed topic", "intents need acks including View", "acks View needs a typed topic", "intents inside a transaction need Part G", "an insert needs a value".
    - `CheckedWritesIntents.UpsertOnARawHandleIsUnchecked`: an `All` producer (no `View`), `kind = Raw` + `Upsert` → `Ok` and one frame.
    - `CheckedWritesIntents.IntentOnAnOldBrokerIsBrokerTooOld`: `setBrokerVersion(9)`, `All | View`, `Insert` → no frame, and one completion `OpStatus::BrokerTooOld` (03's gate, unchanged).
    - `CheckedWritesIntents.DirectModeAndDeferredRecordsKeepTheirIntent`: an Invoker `Direct` producer sends an `Insert` from inside its own ack callback (the deferred arena path, `ProducerImpl.cpp:543-610`). The applied record's attribute is 1.
    - `CheckedWritesIntents.SendAndWaitReturnsThePerRecordVerdict`: a fake session answers the v17 `ProduceResponse` with the trailer `{(0, 56, 0)}` → `sendAndWait(…, Insert, 1s)` returns `BrokerError`, `errorCode 56`, `constraintIdOf == 0`. An empty trailer → `Ok`, `value == baseOffset`.
    - `CheckedWritesIntents.DefineSendsKeyFeaturesOnlyWhenSet`: a fake gateway records the `DefineTopic`: `TopicOptions{}` → `!hasKeyFeatures`; `{.keyFeatures = {.keyIndex = true}}` → `hasKeyFeatures && keyFeatures.keyIndex`. With seed broker version 9 the second gives `BrokerTooOld` and no frame.
  - `CheckedWritesRecordOpTest.cpp`:
    - `CheckedWritesRecordOp.OpFollowsAttributesAndValue`: a batch with `(attributes 0, v)`, `(1, v)`, `(2, v)`, `(0, {})`, `(2, {})`, `(3, v)` delivered through `SubscriberHub` → `op()` gives `Upsert, Insert, Update, Delete, Delete, Upsert`, and `attributes` is `{0, 1, 2, 0, 2, 3}`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j18 --target rune_invoker_tests rune_subscriber_tests`. Expected: `no type named 'WriteIntent'`.
- [ ] **Step 3: Implement** as specified. `Record::op()` is the P5-11 rule. `SubscriberHub`'s `forEachRecord` lambda (`:643-660`) takes the 5-argument visitor form already on main (`RecordBatch.h:403-449`) and sets `rec.attributes`. `RecordBatch.h` is not edited.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesIntents|CheckedWritesRecordOp|ProducerAck|ProducerInFlight|ProducerTxn|ProducerClaim|ProducerAlloc|ProducerAcksFlags|Subscriber|Shard' -j18 --output-on-failure`. Expected: PASS.
  - The `ProducerAllocRegressionTest` suite stays at zero allocations: the intent adds no allocation.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/producer client-cpp/src/producer client-cpp/src/shard client-cpp/include/rune/client/shard/ClientShard.h \
          client-cpp/include/rune/client/TopicHandle.h client-cpp/include/rune/client/Client.h client-cpp/src/Client.cpp \
          client-cpp/include/rune/client/subscriber/Subscription.h client-cpp/src/subscriber/SubscriberHub.cpp \
          client-cpp/tests/producer/CheckedWritesIntentsTest.cpp \
          client-cpp/tests/subscriber/CheckedWritesRecordOpTest.cpp client-cpp/CMakeLists.txt
  git commit -m "feat(client): WriteIntent on the raw producer (ring flags -> record attributes), local intent refusals, sendAndWait, record.op(), TopicKind, key features on define" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 4: The typed API and `WriteResult`

**Files:**
- Create: `client-cpp/include/rune/client/WriteResult.h`
- Modify: `client-cpp/include/rune/client/TypedTopic.h:194-404` (the ops, `KeyT`, `keyOf_`, `fromSchema` `:229` sets `TopicKind::Typed`; the `ProducerT` contract `:34-38` gains the intent-taking `send` and `sendAndWait`)
- Create: `client-cpp/tests/client/CheckedWritesTypedTopicTest.cpp`.
  - Add it to `rune_open_topic_tests` (`client-cpp/CMakeLists.txt:1105`, where the TypedTopic unit tests live). That target also gets `add_dependencies(… generate_claim_schema_headers)` and the `${RUNE_CLAIM_GEN_DIR}` include (`:252`), guarded by `if(TARGET generate_claim_schema_headers)` as at `:1097`.
  - It uses a stub `ProducerT` that records `(handle, key, value, intent)` and serves `sendAndWait` from a scripted `Completion`.
  - It uses the generated `position` (`userId Int64 KEY`) and `short_key` (`code string(8) KEY`) types (`client-cpp/tests/support/GenClaimSchemas.cpp:4-9`). `long_key` is `string(20)`, a key `send()` cannot route.
  - It adds a hand-written `uint16`-keyed record that follows the `ClientAuthClusterRegressionTest.cpp:64-79` contract.

**Interfaces:**
- Consumes: Task 3 (`WriteIntent`, `Producer::send(…, cq, token, intent)`, `sendAndWait`), C6 accessors `constraintIdOf` / `assignedKeyOf` (03).
- Produces (C6 verbatim, plus contract addition 8):
  ```cpp
  inline constexpr std::chrono::seconds kDefaultWriteWait{30};
  class WriteResult {
  public:
      [[nodiscard]] static WriteResult fromCompletion(const Completion& c) noexcept;
      [[nodiscard]] bool     ok() const noexcept;            // status() == OpStatus::Ok
      [[nodiscard]] uint16_t code() const noexcept;          // errorCode when status() == BrokerError, else 0
      [[nodiscard]] uint32_t constraintId() const noexcept;  // constraintIdOf(c)
      [[nodiscard]] uint64_t key() const noexcept;           // assignedKeyOf(c); 0 until Part 06
      [[nodiscard]] OpStatus status() const noexcept;
      [[nodiscard]] int64_t  offset() const noexcept;        // c.value: the record's offset, -1 if none
  };
  // TypedTopic<T, ProducerT>, for KeyT = std::remove_cvref_t<decltype(std::declval<const T&>().key())>:
  void insert(const T&, Completions&, Token);         [[nodiscard]] WriteResult insertAndWait(const T&, std::chrono::nanoseconds = kDefaultWriteWait);
  void update(const T&, Completions&, Token);         [[nodiscard]] WriteResult updateAndWait(const T&, std::chrono::nanoseconds = kDefaultWriteWait);
  void upsert(const T&);  void upsert(const T&, Completions&, Token);   [[nodiscard]] WriteResult upsertAndWait(const T&, std::chrono::nanoseconds = kDefaultWriteWait);
  void remove(const KeyT&);  void remove(const KeyT&, Completions&, Token);   [[nodiscard]] WriteResult removeAndWait(const KeyT&, std::chrono::nanoseconds = kDefaultWriteWait);
  void removeExisting(const KeyT&, Completions&, Token);   [[nodiscard]] WriteResult removeExistingAndWait(const KeyT&, std::chrono::nanoseconds = kDefaultWriteWait);
  ```
  - **Keys.** `remove*` and the key-taking forms exist only `requires HasRecordKey<T>`. `keyOf_(const KeyT&)` is the body of today's `recordKey_` applied to the key value, and `recordKey_(rec)` becomes `keyOf_(rec.key())`. So `send(rec)` and `remove(rec.key())` build byte-identical `Key`s by construction.
  - **The `insert` / `update` / `upsert` value** is encoded and version-stamped by the existing `sendWithKey_` path (`TypedTopic.h:374`), which gains `WriteIntent` and a target. `remove*` sends an empty value.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesTypedTopicTest.cpp`).
  - `CheckedWritesTypedTopic.EachOpSendsItsIntentAndValue`: `insert` → intent 1 with the encoded bytes; `update` → 2; `upsert` → 0 (bytes equal to `send(rec)`); `remove` → 0 with an empty value; `removeExisting` → 2 with an empty value.
  - `CheckedWritesTypedTopicRegression.RemoveBuildsTheSameKeyAsSendRegression`: for `position` (`int64`), `short_key` (`string(8)`) and the hand-written `uint16` record, 50 random records → `remove(rec.key()).key == send(rec).key` (byte-equal) and `key.partition(8)` equal. For `long_key`, `remove` refuses exactly as `send(rec)` does (`KeyTooLong`).
  - `CheckedWritesTypedTopic.AndWaitMapsTheCompletion`:
    - scripted `{BrokerError, 56, reserved0 0}` → `!ok()`, `code() == 56`, `constraintId() == 0`, `key() == 0`;
    - `{Ok, value 41}` → `ok()`, `offset() == 41`;
    - `{VerdictUnknown, 21}` → `status() == OpStatus::VerdictUnknown`, `code() == 0`;
    - `{BrokerTooOld}` → `status() == BrokerTooOld`.
  - `CheckedWritesTypedTopic.AndWaitThrowsOnALocalRefusal`: the stub's `sendAndWait` throws `std::invalid_argument` → `insertAndWait` propagates it.
  - `CheckedWritesTypedTopic.SendIsUnchanged`: `send(rec)` records intent 0 and the same bytes as before this task; `boundVersion()` is stamped.
  - `CheckedWritesTypedTopic.FromSchemaMarksTheHandleTyped`: `fromSchema` → `handle().kind == TopicKind::Typed`.
  - `CheckedWritesTypedTopic.KeylessTopicHasNoRemove`: `static_assert(!requires(TypedTopic<Keyless, Stub>& t) { t.remove(0); })`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j18 --target rune_open_topic_tests`. Expected: `rune/client/WriteResult.h` not found.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesTypedTopic|TypedTopic|OpenTopic|TypedClaim|KeyRouting' -j18 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/WriteResult.h client-cpp/include/rune/client/TypedTopic.h \
          client-cpp/tests/client/CheckedWritesTypedTopicTest.cpp client-cpp/CMakeLists.txt
  git commit -m "feat(client): TypedTopic insert/update/upsert/remove/removeExisting with AndWait forms returning WriteResult (C6, C15)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint A (controller).**
  - `cmake --build build/root -j18 && ctest --test-dir build/root -R Regression -LE latency -j18 --output-on-failure`
  - `ctest --test-dir build/sub-client-cpp -R Regression -LE latency -j18 --output-on-failure`
  - `cmake --build build/sub-codec -j18 && ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden|CheckedWrites' --output-on-failure`. CatalogGolden must show the v1 and v2 golden tests unchanged and the three new `CatalogV3GoldenRegression` tests passing.
  - `build/asan` (`ASAN_OPTIONS=detect_leaks=0` on macOS x86_64 under Rosetta, then `leaks(1)` on `rune_catalog_tests`), `build/tsan` and `build/client-tsan` with `-R Regression -LE latency`.
  - Expected: 100 % pass and no sanitizer report. A failure is fixed with a `*Regression*` test, and then only the failed and affected tests are rerun.

---

## Phase 2 — The broker: DDL, the key index member, the worker wiring

### Task 5: `key.index` DDL, the format by level, and the spec's `keyIndex`

**Files:**
- Modify: `broker/libs/node/include/rune/node/AdminChannels.h:267-284` (`DefineTopicMsg` appends `rune::protocol::TopicKeyFeatures keyFeatures{};` after `nsId`), `:683-688` (`layout`: the 14-byte block after `w.u32(m.nsId)`, the last field today), `:800` (`decodeDefineTopic`)
- Modify: `broker/libs/gateway/include/rune/gateway/IAdminRequestSink.h:92-99` (`submitDefineTopic` gains a trailing `const rune::protocol::TopicKeyFeatures& keyFeatures`; cold, virtual control plane), `broker/libs/gateway/src/ClientGateway.cpp:8425` (passes `m.hasKeyFeatures ? m.keyFeatures : TopicKeyFeatures{}`), `broker/libs/node/include/rune/node/BrokerNode.h:262` (the sink copies it into `DefineTopicMsg`). Update the gateway test fakes that implement the interface (`broker/libs/gateway/tests/gateway/{AuthLoginGatewayTest,SecurityGatewayTest,AuthUnappliedTopicTest,CatalogGatewayTest}.cpp`).
- Modify: `broker/libs/node/src/AdminWorker.cpp`:
  - `:1354`: `b->formatVer = cat::kCurrentFormatVer;` (every DDL, namespace DDL included) becomes `b->formatVer = clusterFeatureLevel_ >= MetadataFeature::kCheckedWrites ? cat::kFormatV3 : cat::kCurrentFormatVer;` (P5-7);
  - `buildDefine_` (`:1387-1478`): P5-6 for both the adoption branch (`:1421-1444`) and the create branch (`:1475`), rule 3 after the P4 key-type rule (`:1404-1408`). Both `cat::SchemaDef{m.keyField, m.validate, m.blob, nextTag}` constructions gain `m.keyFeatures`;
  - `submitGetSchema` (`:642-677`): pass the topic's features to every OK answer, including the kind probe (`:645-651`) and the lineage answer (`:657-663`).
- Modify: `broker/libs/node/include/rune/node/AdminWorker.h:81-85` (`respondGet` gains a trailing `const rune::protocol::TopicKeyFeatures&`), `broker/libs/node/include/rune/node/BrokerNode.h:766` (`postTopicSchemaResponse` sets `hasKeyFeatures` iff non-default), `broker/libs/node/src/BrokerNode.cpp:4410-4417` (wiring).
- Modify every test that assigns `respondGet`, or the build breaks: `broker/libs/node/tests/integration/SchemaGatewayTest.cpp` (`rune_schema_gateway_tests`), `broker/libs/node/tests/integration/CreateViewRegressionTest.cpp` (`rune_create_view_tests`), `broker/libs/node/tests/support/{CatalogClusterHarness.h,CatalogTrio.h,CatalogDdlRig.h}`.
- Modify: `broker/libs/node/include/rune/node/WriteState.h`:
  - `TopicWriteSpecWire::keyIndex` (`:32-41`, flags bit 1; `kTopicWriteSpecWireLen` stays 9);
  - `encode` / `decode` (`:44`, `:54`): `decode` reads only `in[8] & 1` today, so it learns bit 1 and refuses the other bits;
  - `writeSpecWireOf` (`:66-82`, `keyIndex = t.schema->keyFeatures.keyIndex`);
  - `TopicWriteSpec::keyIndex` set in `setFacts` (`:121`).
- Create: `broker/libs/node/tests/node/CheckedWritesKeyIndexDdlTest.cpp` (in `rune_checked_writes_tests`, driving `CatalogDdlRig` as 02's `CheckedWritesDdlTest.cpp` does).
  - `CatalogDdlRig::DefineArgs` (`:83-97`) gains `keyFeatures`.
  - The level is set with `rig.admin().setClusterFeatureLevel(n)` (`:390`), because the rig has no setter of its own and starts at 0.
  - The setter is monotonic (`AdminWorker.h:332`), so a test that compares two levels uses two rigs. Modify: `broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp` (the spec wire).

**Interfaces:**
- Consumes: 02's `featureRefusal` / `topicShapeOf`, `AdminWorker::clusterFeatureLevel()`, `writeSpecWireOf`, `TopicWriteSpecWire`; Tasks 1 and 2.
- Produces: `TopicWriteSpec::keyIndex` true on every worker for a `key.index` topic, delivered by 02's `WriteSpecUpdate` before the topic serves (02 P7). The `TopicSchemaResponse` block is set for such a topic.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesKeyIndexDdlTest.cpp`; each case builds through `CatalogDdlRig` after `becomeServingLeader()`, at level 5 unless named).
  - `CheckedWritesKeyIndexDdl.TypedKeyedDiskTopicGetsAKeyIndex`:
    - `ki {id uint64 key, name string(16)}` with `keyIndex` → code 0;
    - the committed event has `formatVer == 3` and `keyFeatures.keyIndex`;
    - `catalog.topicByName("ki")->schema->keyFeatures.keyIndex`;
    - `writeSpecWireOf(*t)->keyIndex`.
  - `CheckedWritesKeyIndexDdl.Refusals` (`TEST_P`, each → code 39 and the exact detail):
    - level 4 (`setClusterFeatureLevel(4)`) → "key.index needs every broker at feature level 5";
    - untyped → "key.index needs a typed topic";
    - typed keyless → "key.index needs a key";
    - transient typed keyed → "key.index needs a persisted topic";
    - `string(9)` key → "key.index needs a key of at most 8 bytes";
    - `string(16)` key → the same;
    - `autoIncrement = true` → "autoincrement is not supported by this broker yet";
    - `partitioner = Modulo` → the same;
    - `autoIncrementReserve = 1` → the same.
  - `CheckedWritesKeyIndexDdl.EveryKeyWidthUpToEightIsAccepted` (`TEST_P` over `uint16`, `int32`, `uint32`, `int64`, `uint64`, `string(1)`, `string(8)`) → code 0.
  - `CheckedWritesKeyIndexDdl.AdoptionCanSetTheKeyIndex`: an untyped disk topic, then a `DefineTopic` with a blob, key `id` and `keyIndex` → a `CreateSchema` event with features. An untyped **transient** topic → 39 "key.index needs a persisted topic".
  - `CheckedWritesKeyIndexDdl.BelowCheckedWritesWritesV2AndKeepsTheTopicFingerprint`: at level 4 the event is `formatVer 2` (`kCurrentFormatVer`, no block). On a second rig at level 5 it is `formatVer 3` with a default block, and the topic's fingerprint equals the level-4 one. A namespace DDL follows the same rule.
  - `CheckedWritesKeyIndexDdl.SchemaResponseCarriesTheFeatures`: `getSchema("ki", 0)` → `hasKeyFeatures`, `keyIndex`. `getSchema(plain, 0)` → `!hasKeyFeatures`.
  - `CheckedWritesKeyIndexDdlRegression.AlterKeepsTheKeyIndexRegression`: ALTER ADD on `ki` → the topic still has `keyIndex`, and so does `writeSpecWireOf`.
  - `CheckedWritesWriteState.KeyIndexFlagRoundTrips` (`CheckedWritesWriteStateTest.cpp`): `{…, keyIndex true}` → 9 bytes with flags bit 1, decoding equal. Flags `0x04` → `nullopt`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j18 --target rune_checked_writes_tests`. Expected: `no member named 'keyFeatures' in 'DefineTopicMsg'`.
- [ ] **Step 3: Implement** as specified. The width in rule 3 is `fieldWidth(f)` of the live key field, the value 02's `writeSpecWireOf` already computes.
- [ ] **Step 4: Run.** `cmake --build build/root -j18 --target rune_checked_writes_tests rune_gateway_auth_login_tests rune_gateway_auth_unapplied_tests rune_gateway_catalog_tests rune_gateway_security_tests rune_ddl_conformance_tests rune_node_tests rune_schema_gateway_tests rune_create_view_tests && ctest --test-dir build/root -R 'CheckedWritesKeyIndexDdl|CheckedWritesDdl|CheckedWritesWriteState|DdlOccupant|DdlStress|CatalogGateway|CatalogWritePath|AdminWorker|SchemaRegistry|SchemaGateway|CreateView|AuthLogin|AuthUnapplied|SecurityGateway' -j18 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/AdminChannels.h broker/libs/gateway/include/rune/gateway/IAdminRequestSink.h \
          broker/libs/gateway/src/ClientGateway.cpp broker/libs/gateway/tests/gateway broker/libs/node/include/rune/node/BrokerNode.h \
          broker/libs/node/src/BrokerNode.cpp broker/libs/node/include/rune/node/AdminWorker.h broker/libs/node/src/AdminWorker.cpp \
          broker/libs/node/include/rune/node/WriteState.h broker/libs/node/tests/node/CheckedWritesKeyIndexDdlTest.cpp \
          broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp broker/libs/node/tests/integration/SchemaGatewayTest.cpp \
          broker/libs/node/tests/integration/CreateViewRegressionTest.cpp broker/libs/node/tests/support/CatalogClusterHarness.h \
          broker/libs/node/tests/support/CatalogTrio.h broker/libs/node/tests/support/CatalogDdlRig.h broker/libs/node/CMakeLists.txt
  git commit -m "feat(admin): key.index DDL (level, feature rules, 8-byte limit SD8), catalog formatVer by level, features in TopicSchemaResponse and the write spec" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 6: The key index member: prepare, evaluate, apply and its fold

**Files:**
- Modify: `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp`, `broker/libs/node/include/rune/node/VerdictState.h` (`VerdictSourceStats`):
  - contract addition 6;
  - the `KeyIndex` arms in `prepare_` (`VerdictSource.cpp:186`), `apply_` (`:196`) and the member replay `replay_` (`:264`);
  - the per-kind `judge` / `rejects_` restructure (`:214-262`, below);
  - the stats.
- Create: `broker/libs/node/tests/node/CheckedWritesKeyIndexTest.cpp` (in `rune_checked_writes_tests`)
- Modify: `broker/libs/node/CMakeLists.txt`: the new source goes in `rune_checked_writes_tests` (`:820-841`). Part 01's `AbortedFoldLog.h` / `TxnLog` were never built (RB3, RB5). The tests' aborted transaction is a transactional batch that a stub `AbortedTxnFilter{fn, ctx}` (`broker/libs/view/include/rune/view/FoldSkip.h`) reports as aborted, so no include path is added.

**Interfaces:**
- Consumes: 04's `FoldSkip`, `AbortedTxnFilter` and `isAbortedBatch` (`FoldSkip.h`, RB3); 04's `SourceRecord`, `Prospect`, `ProspectOp`, `PrepareStatus`, `Verdict`, `isRejectedRecord`, `ViewKey::fromU64` / `asU64` (R07d), `VerdictSource::judge` / `driveJudge` / `addVerdictView`, and the `installTestVerdictViewForTest` unit harness (04 Task 5's fake `readRange` over `InMemoryLog` batches).
- Produces (contract addition 6):
  - **`prepareKeyIndex`** sets `p.op = rec.value.empty() ? Erase : Upsert`, `p.rowCount = 1`, `rows[0].key = ViewKey::fromU64(keyIndexWord(rec.key))`, `rows[0].existed = table.get(word) != nullptr` and `rows[0].removed = rec.value.empty()`. A key whose size isn't `keyWidth` → `Malformed`.
  - **`evaluateIntent`** is the P5-3 table. A `Malformed` prospect → accepted.
  - **`applyKeyIndex`:** `Upsert` → `*table.put(word) = 1`; `Erase` → `table.erase(word)`; `Malformed` → nothing.
  - **`foldKeyIndexBatch`** has `foldBatch`'s batch, bound, cursor and corrupt-tail contract (`ViewFold.cpp:171`):
    - control batches, `isAbortedBatch` batches and `isRejectedRecord` records are skipped (the latter counted `skippedRejected`);
    - each remaining record is prepared and applied, without judging;
    - wrong-width keys are counted `skippedMalformed`.
  - **Judging (the restructure).**
    - Today `judge` (`VerdictSource.cpp:234-262`) prepares every active member and asks `rejects_` for a bool. It answers a hardcoded `{ConstraintViolation, constraintIdOf(objectId, 1)}` and bumps `rejectionsConstraint`. `rejects_` (`:214`) returns false for every non-`TestReject` member and for every tombstone.
    - `rejects_` becomes `[[nodiscard]] Verdict verdictOf_(const Member&, const SourceRecord&) const noexcept`, where code 0 means accept:
      - `TestReject` keeps its rule, its tombstone skip and `{55, constraintIdOf(objectId, 1)}`;
      - `KeyIndex` returns `evaluateIntent(rec, m.prospect)`, tombstones included (`{56|57, 0}`, P5-3).
    - The key index is prepared with every member and evaluated first (it has id 0, set order, P4-5), and the first non-zero verdict wins.
    - On a reject `judge` adds `{offset, code, constraintId}` to `state.rejected` as today. It bumps `rejectionsConstraint` for 55, `rejectionsKeyExists` for 56 and `rejectionsKeyNotFound` for 57, plus 04's per-constraint stats.
    - `judgeBatches_` (`:284-313`) resolves writer descriptors through the first active member's `vdc`, which is now the key index. So its `ViewInstance` is built over the evolving source descriptor (Task 7).
  - **Dirty tracking.** After each judge pass the key index's dirty tracking is drained with a no-op visitor (`discardDirty_`'s rule, `WorkerViewSet.h:716`), so `dirtyList_` never grows.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesKeyIndexTest.cpp`; a key index member over an 8-byte `uint64` key, built with `keyIndexPlan` / `keyIndexRowLayout` and `kKeyIndexChunkBytes`, registered with a `VerdictSource` as 04 Task 5's tests build members).
  - `CheckedWritesKeyIndex.IntentTable` (`TEST_P` over the 5 × 2 cells of P5-3 at exists / absent): `judge(rec)` gives the table's verdict; `constraintId == 0`; the table size changes only for accepted records.
  - `CheckedWritesKeyIndex.MixedIntentAndPlainRecordsInOneBatchJudgeInOrder` (Review Focus 4): one batch `[insert 5, upsert 5, insert 5, remove 5, removeExisting 5, update 5]` → `Ok, Ok, 56, Ok, 57, 57`; afterwards `!contains(5)`.
  - `CheckedWritesKeyIndex.KeyIndexIsFirstInSetOrder`: members key index (0) + `TestReject{TagEquals{9}}` (7). `insert 5 tag 9` on a live 5 → `{56, 0}`, not 55. `insert 6 tag 9` on an absent 6 → `{55, 0x00070001}`, and 6 is **not** in the key index. Then `update 6` → 57.
  - `CheckedWritesKeyIndex.TombstonesAreNeverRejectedExceptRemoveExistingOnAbsent`: `remove` of an absent key → `Ok` with the size unchanged; `removeExisting` of an absent key → 57; `TestReject{CapLiveKeys{0}}` never rejects either tombstone.
  - `CheckedWritesKeyIndex.FoldReplaysUnjudgedAndSkips`: a log of 12 records with an aborted transaction (a transactional batch that the stub `AbortedTxnFilter` reports as aborted), a control batch and an index rejecting offsets 3 and 7 → `foldKeyIndexBatch` holds exactly the live keys of the others; `skippedRejected == 2`; the cursor reaches the log end. Pass bounds 1..12 one at a time give the same set, with the cursor never past the bound.
  - `CheckedWritesKeyIndex.WordPacksEveryWidthInjectively`: for widths 2, 4 and 8 and `string(1..8)`, 10,000 random distinct keys → distinct words; `keyIndexWord("\x01\x02") == 0x0201`.
  - `CheckedWritesKeyIndex.MalformedKeyAppliesNothingAndCounts`: a 7-byte key on the 8-byte index → accepted, size unchanged, `keyIndexSkippedMalformed == 1`.
  - `CheckedWritesKeyIndex.DirtyTrackingNeverGrows`: 10,000 judged records over 100 keys → `table.dirtyRids().size() == 0` after each pass.
  - `CheckedWritesKeyIndexRegression.TestRejectVerdictsAreUnchangedByTheRestructureRegression`: a `TestReject{TagEquals{9}}` member alone → `{55, constraintIdOf(7, 1)}` for a live tag-9 record, a tombstone never rejected, and `rejectionsConstraint` counted as before.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j18 --target rune_checked_writes_tests`. Expected: `no member named 'prepareKeyIndex'`.
- [ ] **Step 3: Implement** as specified. No allocation in `judge` or the fold: the prospect is reserved at `addVerdictView` with `reserve(/*rowLen=*/1, nullptr)`, and the table grows only by `ViewTable`'s own amortized chunk and index growth.
- [ ] **Step 4: Run.** `cmake --build build/root -j18 --target rune_checked_writes_tests rune_checked_writes_verdict_tests rune_checked_writes_matrix_tests && ctest --test-dir build/root -R 'CheckedWritesKeyIndex|CheckedWritesVerdictSource|CheckedWritesTimeline|CheckedWritesVerdictWorker|CheckedWritesVerdictAck|CheckedWritesConsumer|CheckedWritesMatrix' --output-on-failure`. Expected: PASS, with 04's verdict and matrix suites unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/include/rune/node/VerdictState.h \
          broker/libs/node/src/VerdictSource.cpp broker/libs/node/tests/node/CheckedWritesKeyIndexTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): the key index as VerdictViewKind::KeyIndex - intents judged at fold (56/57), its unjudged replay fold" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 7: Worker wiring: the desired set, the rebuild on activation, the hook facts, and the rig

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h`, `broker/libs/node/src/WorkerViewSet.cpp`:
  - `keyIndexes_`, adopted into the source's namespace and charged with `adoptNs_` (`WorkerViewSet.h:426`) / `chargeMem_` (`:410`) when an instance is created, and released when it is erased (RB5);
  - `ensureVerdictSource` (`WorkerViewSet.cpp:793-826`): create (and keep) the `VerdictSource` when `write.spec->keyIndex`, beside 04's two conditions (a verdict template, a non-empty logged set). `driveFolds` (`WorkerViewSet.h:241`) already skips its early return when `judge != nullptr`;
  - the desired set in `syncConstraintSet_` (`WorkerViewSet.cpp:828-876`):
    - push `{0, KeyIndex, kKeyIndexPlanFingerprint}` when `write.spec->keyIndex` and `want` doesn't hold it yet;
    - a logged id-0 member has no template, so the loop over the logged set keeps it, and the existing sort keeps it first;
    - a `WriteSpecUpdate` whose `keyIndex` differs from the partition's spec bumps `verdictEpoch_`, so the `(constraintSyncEpoch, constraintSyncChange)` cache at `:829` doesn't hide the change;
  - before `driveJudge`, when `activationPending()`, move-assign a fresh key index `ViewInstance` (foldCursor = `logStart`, `++keyIndexRebuilds`) and re-register it;
  - 04's install check: the key index member counts as installed iff `write->spec->keyIndex`;
  - the replay of a `KeyIndex` member runs `foldKeyIndexBatch` capped at `kKeyIndexReplayRecordsPerPass`;
  - `dropSourceTopic`, and the two test seams.
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `driveViews_` (`:6999-7047`): the short-circuit at `:7006-7009` lets through a partition whose `writeOf_(replica)` has a non-empty logged set. It now also lets through one whose `spec->keyIndex` is set, so a fresh key-index topic with no views and no logged set yet is driven;
  - the `TopicDropped` case (`:2790-2793`, beside `markTopicDropped_` / `dropWriteSpec_`): `viewSet_->dropSourceTopic(op.topicId())`;
  - no `rebindVerdictSources_` exists on main. A partition's key index instance is erased where its judge is released: in `WorkerViewSet::dropVerdictSource` (`WorkerViewSet.h:287`, called per partition by `Worker::dropWriteSpec_`, `Worker.h:3385-3395`) and in `ensureVerdictSource`'s erase branch.
- Modify: `broker/libs/node/include/rune/node/CheckedWriteHook.h`, `broker/libs/node/src/CheckedWriteHook.cpp`:
  - `topicHookFactsOf` (`CheckedWriteHook.h:42-47`): set the existing `keyIndex` (`:37`) from `state.spec->keyIndex`, and the new `keyIndexActive` per P5-4;
  - `evaluateRequirements` rule 6b;
  - `check()` (`CheckedWriteHook.cpp:9-50`): an insert tombstone sets `reservedBits` (P5-5).
- Modify: `broker/libs/node/tests/node/CheckedWritesHookTest.cpp`:
  - the new pinned rows;
  - in the sweep (`SweepInvariants`, `:145-215`), the `build`, `flip` and `feeds` lambdas gain bit 4096 = `keyIndexActive`, which feeds `KeyIndexActivating`;
  - the mask loop runs to 8192, the flipped facts number 12, and the pinned count becomes `6 * 2 * 3 * 8192`;
  - invariant (iv) is amended.
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (`VerdictRig`, `:76`): `KeyShape`, `createKeyIndex`, `enableKeyIndex`, `Rec::intent`, `RigLogRec::intent`, `keyIndexKeys()`, `holdJudge(bool)` and `restart`'s `specDelayPasses` (contract addition 9). `batch()` (`:253`) writes each `Rec`'s intent into its attributes, and `readLog` fills `RigLogRec::intent`.
- Create: `broker/libs/node/tests/node/CheckedWritesKeyIndexWorkerTest.cpp` (in `rune_checked_writes_verdict_tests`, `broker/libs/node/CMakeLists.txt:1353`)

**Interfaces:**
- Consumes: Tasks 5 and 6; 04's `VerdictRig` with `typed`, `driveUntilJudged`, `failover`, `demotePromote`, `restart`, `truncateTo`, `compact`, `enableAcks`, `produceFrame`, `ack(corr)`, `subscribe` / `collect`; `Worker::verdictStatsForTest` (`Worker.h:710`); 03's `hookRefusalsForTest` (`:1807`).
- Produces:
  - **`createKeyIndex(KeyShape s = {}, bool enabled = true)`** runs `typed()`'s ops for the topic `kSrc` `{id <s> key, v int64, tag uint16}` and sends the `WriteSpecUpdate` with `keyIndex = enabled`. `enableKeyIndex()` re-sends it with `keyIndex = true`, simulating an adoption.
  - **`keyIndexKeys()`** reads `keyIndexContainsForTest` / a walk of the instance on the rig thread.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesHookTest.cpp`:
    - pinned rows `{insert explicit, acks 3, idempotent, level 1, keyIndex, !keyIndexActive}` → `{NotLeader, KeyIndexActivating}`;
    - the same not idempotent → `ExplicitKeyNeedsIdempotentAndView` (definite first);
    - the same with `keyIndexActive` → `OK`;
    - plain, `acks 1`, `keyIndex`, `!keyIndexActive` → `OK` (plain writes never wait);
    - `CheckedWritesHook.InsertTombstoneIsReservedBits` → `InvalidRequest` / `ReservedBits`.
    - `CheckedWritesRequirementTable.SweepInvariants` now runs over 294,912 cells (6 × 2 × 3 × 8192) with invariant (iv) amended (a deviation of 03's text, recorded in the commit body).
  - `CheckedWritesKeyIndexWorkerTest.cpp` (`VerdictRig`, `createKeyIndex()`, `enableAcks()`, an `All | View` idempotent v17 producer unless named):
    - `CheckedWritesKeyIndexWorker.InsertThenInsertIsKeyExistsOnTheWire`:
      - `insert 5` → the ack is OK with an empty trailer (56 B);
      - `insert 5` → OK, frame 64 B, trailer `{(0, 56, 0)}`;
      - a raw subscription from 0 receives only the first; `op() == Insert`;
      - `verdictStatsForTest(VerdictRig::kSrc, 0)->rejectionsKeyExists == 1`.
    - `CheckedWritesKeyIndexWorker.UpdateAndRemoveExistingOnAbsentAreKeyNotFound`: `update 9` → 57; `removeExisting 9` → 57; `upsert 9` then `removeExisting 9` → OK, OK; `rejectionsKeyNotFound == 2`.
    - `CheckedWritesKeyIndexWorker.PlainWritesFromAnyProducerAreFoldedNeverRejected` (P5-12): a non-idempotent `acks=leader` producer and a v16 session upsert 7 and remove 8 → accepted. Then an `insert 7` from the `View` producer → 56, and `insert 8` → OK.
    - `CheckedWritesKeyIndexWorker.ChangeRecordPutsTheKeyIndexFirst`: after `createKeyIndex()` and one pass, the log holds one type-18 record whose first member is `(0, KeyIndex, kKeyIndexPlanFingerprint)`; `state().constraintSets.latest().members[0].kind == KeyIndex`.
    - `CheckedWritesKeyIndexRegression.IntentBeforeActivationIsRetriableNeverUnjudgedRegression` (Review Focus 3):
      - `createKeyIndex()` with `holdJudge(true)` and the change record not yet appended (no pass driven); `insert 5` → `NotLeader`, LEO unchanged, `hookRefusalsForTest(KeyIndexActivating) == 1`;
      - after one pass the same batch re-sent → OK;
      - the adoption form: `createKeyIndex(…, enabled = false)`, 300 upserts (including id 5), `enableKeyIndex()`, an `insert 5` before the pass → `NotLeader`, and after activation → 56.
    - `CheckedWritesKeyIndexRegression.InsertRetryAfterALostAckReplaysOkNeverKeyExistsRegression` (Review Focus 1, R04e): `insert 5` with its ack dropped by the rig; the retry (same pid, epoch, seq) → the same base offset, an empty trailer (OK), `verdictReplays == 1`, LEO unchanged. Then 33 more batches and the same retry → `DuplicateSequence` 21.
    - `CheckedWritesKeyIndexRegression.EveryActivationRebuildsFromTheLocalLogRegression` (Review Focus 2):
      1. insert 1..50, then `remove 1..10`, all judged;
      2. `demotePromote()` (memory kept) → `keyIndexRebuilds` rose by 1, and right after activation `keyIndexFoldCursorForTest(VerdictRig::kSrc, 0) == log->logStartOffset()` (the old instance was not reused);
      3. after `driveUntilJudged`, `keyIndexKeys() == {11..50}`;
      4. `compact()` with the topic's `deleteRetentionMs = 0` until the tombstones of 1..10 are gone from the log, then `demotePromote()` again → rebuilt from the compacted log, `keyIndexKeys() == {11..50}`, `insert 3` → OK, `insert 20` → 56;
      5. the mutation check (do it, then revert): keep the old instance at activation → step 2's cursor check is red.
    - `CheckedWritesKeyIndexRegression.RestartRebuildsTheSameKeySetRegression`: 1,000 mixed intents on 64 keys, `restart()` → `keyIndexKeys()` equals before; the next intent verdicts equal those of a rig that didn't restart.
    - `CheckedWritesKeyIndexRegression.LeaderWithoutTheKeyIndexSpecWaitsRegression`: `restart(true, -1, /*specDelayPasses=*/100)` (the `WriteSpecUpdate` is delivered 100 passes late) → `waitingForInstall()` holds, `judgedThrough()` stays at the change offset + 1, a `View` ack stays parked; once delivered → it proceeds with the same verdicts.
    - `CheckedWritesKeyIndexWorker.FailoverTimingsGiveTheSameVerdicts`: `FailoverHwFold` and `FailoverFoldMarker` over `[insert 5, insert 5, update 6]` → the same entries `{(1, 56), (2, 57)}` as an undisturbed rig; `mismatches() == 0`.
    - `CheckedWritesKeyIndexWorker.ReplayIsBoundedPerPass`: 200,000 upserts, then `restart()` → the first pass after activation folds at most 65,536 records into the key index; `judgedThrough()` keeps K meanwhile; consumers receive offsets below K.
    - `CheckedWritesKeyIndexWorker.KeyIndexOnlyTopicIsDrivenWithoutViews`: no view installed → the judge runs, and `View` acks and consumers are released (the `driveViews_` short-circuit).
    - `CheckedWritesKeyIndexWorker.TopicDropReleasesTheKeyIndex`: `TopicDropped` → `keyIndexSizeForTest == 0` and no `VerdictSource` for the topic (`verdictSourceKeysForTest()` empty).
    - `CheckedWritesKeyIndexWorker.IntentOnATopicWithoutKeyIndexIsDefinite`: `typed()` (no key index) + `insert` → `InvalidRequest`, `NoKeyIndex` (03's rule, now on a real verdict-free topic).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j18 --target rune_checked_writes_tests rune_checked_writes_verdict_tests && ctest --test-dir build/root -R 'CheckedWritesKeyIndexWorker|CheckedWritesKeyIndexRegression|CheckedWritesHook|CheckedWritesRequirementTable' --output-on-failure`. Expected: FAIL (no key index member; `keyIndexActive` unknown).
- [ ] **Step 3: Implement** as specified. The key index `ViewInstance` is built cold:
  - plan `keyIndexPlan(srcTopicId, spec.keyFieldId)`;
  - layout `keyIndexRowLayout()`;
  - source: the evolving source descriptor, as 04's verdict members get it (`ViewInstance.h:215-239` builds `vdc` from it). `judgeBatches_` decodes every writer through the first member's `vdc` (`VerdictSource.cpp:284-313`), and the key index is first;
  - persisted `false`;
  - index slots 4096 and `kKeyIndexChunkBytes`.
  `std::unordered_map` keeps its address across the move-assign at activation, so the `VerdictSource`'s pointer stays valid.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesKeyIndex|CheckedWritesVerdict|CheckedWritesHook|CheckedWritesRequirementTable|CheckedWritesViewAck|WorkerViewSet|ViewRecovery|ShardWorkerAck' -j18 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
          broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/CheckedWriteHook.h \
          broker/libs/node/src/CheckedWriteHook.cpp broker/libs/node/tests/node/CheckedWritesHookTest.cpp \
          broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesKeyIndexWorkerTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): key-index topics are verdict sources - desired set, rebuild per leadership, activation gate (NotLeader), insert tombstone refused" \
             -m "Deviation (03 Task 6): sweep invariant (iv) now allows NotLeader for KeyIndexActivating; the sweep covers the keyIndexActive fact." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint B (controller).** The Checkpoint A commands, plus `ctest --test-dir build/root -R CheckedWrites -j18 --output-on-failure` under `--repeat until-fail:3`.

---

## Phase 3 — Matrix, stress, the real broker, the docker e2e, benches and docs

### Task 8: The shared matrix rows for 05 and the key-width sweep

**Files:**
- Modify: `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h`:
  - `Op` gains `Insert = 20, Update = 21, Upsert = 22, Remove = 23, RemoveExisting = 24`;
  - `Entity` gains `KeyNew = 20, KeyExisting = 21, KeyEarlierInBatch = 22, KeyRemovedEarlier = 23, KeyRejectedEarlier = 24, KeyBeforeIndex = 25`;
  - `expect05`, and in `expect()` (`:74`) an `isP05(c)` arm (`op` in 20–39) before today's P03 / P04 choice;
  - `cellsFor(Part::P05)` (`:76-93`), listing the 21 timings explicitly;
  - `nameOf` for the new ops and entities.
- Modify: `broker/libs/node/tests/support/CheckedWritesModel.h`:
  - `addKeyIndexRule` (through `addRule(0, …)`) and `isLiveAtJudge`;
  - tombstone judging: `reset()` asks rules only `if (!rec.del)` today, and it now asks every rule. `judge_`'s `TestReject` rules return no verdict for a tombstone, so 04's verdicts don't move;
  - `LogRec` is `VerdictRig::RigLogRec`, whose `intent` comes from Task 7.
- Modify: `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp`:
  - the P05 instantiation and the pin row (`:332-336`);
  - in `CellRun`, the 05 entities and ops;
  - the six 03 timings, which today are driven only on `AckRig` (`CheckedWritesViewAckMatrixTest.cpp:44-120`):
    - `TxnCommitBehind` / `TxnAbortBehind`: a third producer's transactional upsert of `k`, then an `AppendControlRecord` commit or abort marker, as `AckRig::marker` (`CheckedWritesAckRig.h:134`);
    - `LeaderLossParked`: `demote()` while the `View` ack is parked;
    - `AckDeadline`: `minIsr = 2` with the follower not fetching;
    - `StrictTier`: `durabilityTier = 2` on the produce op;
    - `GatewayRouted`: the frame through the routed path, as `AckRig::send_routed` (`:234`).
  - `VerdictRig` gains the small helpers these need (`marker(pid, commit)`, a `minIsr` / `durabilityTier` choice, a routed send).
- Modify: `broker/libs/node/tests/node/CheckedWritesKeyIndexWorkerTest.cpp` (the width sweep)

**Interfaces:**
- Consumes: Task 7's rig helpers; 04's model and matrix harness.
- Produces:
  - **The model's key index rule.** `addKeyIndexRule()` registers a rule for object id 0. `isLiveAtJudge(id)` reads the model's `live_`, which `reset()` already updates in offset order for every accepted record, below a change record too. The rule returns the P5-3 verdict (`{56|57, 0}`) from `isLiveAtJudge(rec.id)`, `rec.intent` and `rec.del`.
  - **Set order.** The model evaluates rules in ascending object id, the first failure wins, and nothing applies on a reject. This independent copy shares no code with `VerdictSource`.

- [ ] **Step 1: Write the matrix.** `INSTANTIATE_TEST_SUITE_P(Cell05, CheckedWritesMatrix, ValuesIn(cellsFor(Part::P05)))` runs the same `TEST_P(CheckedWritesMatrix, Holds)`, which dispatches on the op's range to 05's driver: it sets up the entity, runs the op under the timing, and asserts (a)–(g) of Interaction matrix A. The pin row is `EXPECT_EQ(applicable(cellsFor(Part::P05)), 560u); EXPECT_EQ(skipped(cellsFor(Part::P05)), 70u);`. `CheckedWritesModel.KeyIndexRuleMatchesTheTable` checks the model against the Single-timing table for all 30 op × entity pairs.
- [ ] **Step 2: Write the width sweep.** `CheckedWritesKeyIndexWidths/Shape.OpsJudgeByExactKeyBytes` (70 cells), `CheckedWritesKeyIndexWidths/Shape.KeysDifferingInTheLastByteAreDistinct`, and `CheckedWritesKeyIndexWidths/Shape.RemoveErasesWhatSendPut`, per Interaction matrix B.
- [ ] **Step 3: Run.** `cmake --build build/root -j18 --target rune_checked_writes_matrix_tests rune_checked_writes_verdict_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix|CheckedWritesModel\.|CheckedWritesKeyIndexWidths' -j18 --output-on-failure`. Expected: the P05 cells give 560 pass and 70 skip, the 04 and 03 rows are unchanged (585/15, 78/2), and the sweep passes. Mutation checks (do each, then revert):
  - make `evaluateIntent` ignore `existed` for `Update` → every `Update × KeyNew` and `× KeyRemovedEarlier` cell is red;
  - apply the key index for rejected records → every `KeyRejectedEarlier × Update` cell is red;
  - judge `KeyBeforeIndex` records below the change record → every `Insert × KeyBeforeIndex` cell is red (the pre-index upsert would itself have been judged, and the replay below A must not judge).
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h broker/libs/node/tests/support/CheckedWritesModel.h \
          broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp broker/libs/node/tests/node/CheckedWritesKeyIndexWorkerTest.cpp
  git commit -m "test(node): checked-writes matrix rows for intents (05: 560 cells) and the key-width sweep" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 9: The key-index model check and the cluster stress

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesModelTest.cpp` (the 05 seeds; in `rune_checked_writes_verdict_tests`), `broker/libs/node/tests/support/CheckedWritesStressDriver.h` (a key-index topic set-up option and the typed clients' actions), `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp`
- Create: `broker/libs/node/tests/client/support/KeyIndexRecords.h` (a hand-written `KiAccount {id uint64 key, name string(16), n int64}` following the `ClientAuthClusterRegressionTest.cpp:64` contract; shared with Task 10)

**Interfaces:**
- Consumes: Tasks 7 and 8; 04's `CheckedWritesStressDriver::registerInvariant`, `onOwnerThreadForTest`.

- [ ] **Step 1: Write the model check.** `CheckedWritesKeyIndexModelRegression.SeededRigMatchesTheModel` and `…SameSeedSameTrace`, with the actions and the eight invariants of Interaction matrix D. The actions include tombstones with intent 0 and 2. The model judges them through Task 8's tombstone path, so `IntentVerdictsExact` covers every `removeExisting` 57. A `CheckedWritesModel.TombstonesAreJudgedOnlyByTheKeyIndex` case pins that path: a `TestReject` rule never rejects a tombstone, and the key index rule answers 57 for `removeExisting` of an absent key. A failure prints `SCOPED_TRACE("seed=" … " step=" … " action=" …)`.
- [ ] **Step 2: Write the cluster stress.** `CheckedWritesStressRegression.KeyIndexSeededClusterHoldsTheInvariants`, per Interaction matrix D. Every wait is a bounded poll (10 s), and the deadline is the failure. On every node it also asserts `verdictUnknownDuplicates == 0`, `mismatches() == 0` and `keyIndexSkippedMalformed == 0`.
- [ ] **Step 3: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesKeyIndexModelRegression' -j18 --output-on-failure`. Expected: PASS.
  - `ctest --test-dir build/root -R 'CheckedWritesStressRegression' --output-on-failure`. Expected: PASS, including 04's run.
  - Mutation check (do it, then revert): judge intents against the key index **after** applying the record's own batch-mates in reverse order → `IntentVerdictsExact` is red.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesModelTest.cpp broker/libs/node/tests/support/CheckedWritesStressDriver.h \
          broker/libs/node/tests/integration/CheckedWritesStressTest.cpp broker/libs/node/tests/client/support/KeyIndexRecords.h
  git commit -m "test(node): key-index model check on the rig and the ThreeNodes key-index stress (IntentVerdictsExact, KeyIndexEqualsLiveKeys)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 10: The real-broker slice (default shard runtime)

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` (in `rune_client_broker_tests`)
- Modify: `broker/libs/node/include/rune/node/NodeConfig.h:363` (`testAdvertisedBrokerVersion`, beside `testSupportedFeatureLevel`), `broker/libs/node/src/BrokerNode.cpp` (passes it to the gateway at start), `broker/libs/gateway/include/rune/gateway/ClientGateway.h` and `broker/libs/gateway/src/ClientGateway.cpp:2691-2697` (the OK `ConnectResponse` takes it) (contract addition 10)

**Interfaces:**
- Consumes: everything above; `KeyIndexRecords.h` (Task 9); `OneBroker(tag, OneBroker::Runtime::Shard, tweak)`: the constructor defaults to `Runtime::Legacy` (`OneBroker.h:57`), so every shard cell passes `Shard` explicitly. Also `ThreeNodes` (`CatalogBrokerCluster.h:183`, `startWith` `:191`); `defineTopicAndWait(…, TopicOptions{.keyFeatures = {.keyIndex = true}})`; `openTopicAndWait<KiAccount>`; `rawTopicAndWait`.
- **Test setup.** The topic is `ki {id uint64 key, name string(16), n int64}` with `partitions 4`, `replicas` as the cluster has, `cleanup.policy = compact` and `key.index` on. Producers are `All | View` unless named. Every wait is `waitFor(…, 10s)`.

- [ ] **Step 1: Write the tests.**
  - `CheckedWritesBrokerRegression.TypedIntentsEndToEndShardRegression`:
    - `insertAndWait({1, "alice"})` → `ok()`, `offset() >= 0`;
    - again → `code() == 56`;
    - `updateAndWait({2, …})` → 57;
    - `removeExistingAndWait(1)` → ok; again → 57; `removeAndWait(1)` → ok;
    - `insertAndWait({1, "alice2"})` → ok;
    - a raw subscription from 0 sees exactly the accepted records, with `op()` `Insert, Delete, Delete, Insert`.
  - `CheckedWritesBrokerRegression.KeyIndexDdlRefusalsShardRegression`: through the real client, the P5-6 refusals with their exact `DefineTopicResult::detail` (untyped, keyless, transient, `string(9)`, autoincrement). A raw `DefineTopic` with flags `0x04` → `InvalidSchema` "DefineTopic body undecodable".
  - `CheckedWritesBrokerRegression.TypedRemoveRoutesAsSendForEveryKeyWidthShardRegression` (§15): one `P = 4` topic per key type `uint16`, `int32`, `uint32`, `int64`, `uint64`, `string(4)` and `string(16)`, the latter two **without** a key index (plain `remove` needs none):
    - 50 records per topic through `send(rec)`, then `remove(rec.key())`;
    - a subscription of every partition sees each tombstone on the partition of its record, with the identical key bytes;
    - after the cleaner pass (`BrokerNode::onCleanerThreadForTest`, `BrokerNode.h:1937`, as `CatalogEffectsTest.cpp:222` uses it) no key is live.
  - `CheckedWritesBrokerRegression.IntentsToAnOlderClusterAreRefusedShardRegression` (§15):
    - a broker with `testSupportedFeatureLevel = 4`: `insertAndWait` → `status() == BrokerError`, `code() == 39`, and the LEO is unchanged. The `key.index` DDL → 39 "key.index needs every broker at feature level 5".
    - a broker started with `NodeConfig::testAdvertisedBrokerVersion = 9` (contract addition 10), so its `ConnectResponse.brokerVersion` is 9 → `status() == BrokerTooOld`, and nothing is sent.
  - `CheckedWritesKeyIndexRegression.RawIntentShapesAreRefusedOrOpaqueRegression` (Review Focus 5). A raw `Producer` with `All | View`:
    - on the `ki` topic handle (`kind = Unknown`, so no local check), hand-built records `attributes = 3` → 39; an insert with an empty value → 39; LEO unchanged both times;
    - on a typed topic without a key index, intent 1 → 39 (`NoKeyIndex`);
    - on `rawTopicAndWait("raw")` with `kind = Raw`, `Insert` → `SendResult::WriteNotAllowed`, and a plain `Upsert` from the `All | View` producer → `WriteNotAllowed` too (R05a);
    - a handle `TopicHandle{rawId, 1}` (Unknown) with intent 2 → accepted and stored; a subscriber sees `attributes == 2` and `op() == Update` (R03b: opaque).
  - `CheckedWritesBrokerRegression.LoginRaceOnAKeyIndexThreeNodesRegression`:
    - `ThreeNodes::start("cw05")`, `replicas 3`;
    - two clients race `insertAndWait` on the same 200 ids → for each id exactly one `ok()` and one `code() == 56`;
    - after `crash(leader)` and a re-election, the remaining 200 ids race the same way;
    - every node, once it leads (leadership moved to each in turn), answers re-inserts of all 400 with 56.
  - `CheckedWritesBrokerRegression.RestartRebuildsTheKeyIndexThreeNodesRegression`: 300 inserts, 100 `removeExisting`, then `stop(id)` and `startNode(id)` for every node (`CatalogBrokerCluster.h:202,210`; there is no `restart`, RB5) → re-inserting the 200 live ids → 56, re-inserting the 100 removed ids → ok.
  - `CheckedWritesBrokerRegression.LegacyRuntimeRefusesIntentsRegression` (SD10): `OneBroker(…, OneBroker::Runtime::Legacy)` → `insertAndWait` → `code() == 39`; a plain `upsertAndWait` with `All` is accepted.
- [ ] **Step 2: Run.** `cmake --build build/root -j18 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesBrokerRegression|CheckedWritesKeyIndexRegression' --output-on-failure`, then `--repeat until-fail:3 -j4`. Expected: PASS every round. A failure here is a defect in Tasks 1–9: fix it there with its own `Regression` test, never by relaxing the cell.
- [ ] **Step 3: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp broker/libs/node/include/rune/node/NodeConfig.h \
          broker/libs/node/src/BrokerNode.cpp broker/libs/gateway/include/rune/gateway/ClientGateway.h \
          broker/libs/gateway/src/ClientGateway.cpp
  git commit -m "test(e2e): intents and the key index on real brokers - typed API, DDL refusals, remove routing, older clusters, login race, restart" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 11: The first docker e2e of the program (VM only; written here, run at Checkpoint C)

**Files:**
- Create: `broker/docker/checked-writes-e2e.sh`, `broker/docker/docker-compose.checked-writes.yml` (an overlay on `docker-compose.yml` + `docker-compose.auth.yml`, like `docker-compose.filter.yml`; it sets `RUNE_VERDICT_INTERVAL_MS=20`)
- Create: `client-cpp/examples/checked_writes_probe.cpp`, `client-cpp/docker/Dockerfile.checked-writes-probe`, `client-cpp/docker/Dockerfile.checked-writes-probe.dockerignore` (modeled on `Dockerfile.filter-probe` and its ignore file)
- Modify: `client-cpp/tests/support/GenClaimSchemas.cpp` (a `ki_account` schema `{id uint64 key, name string(16), n int64}`), `client-cpp/CMakeLists.txt` (`ki_account.gen.h` in `RUNE_CLAIM_GEN_HEADERS`; the `checked_writes_probe` target beside `filter_probe`, `:751-770`)
- Modify: `docs/testing.md` (a row in the docker e2e table, `:180-190`)

**Interfaces:**
- The probe has modes `setup`, `race --worker <i> --ids <a>-<b>`, `lifecycle`, `verify`, `refusals`.
  - It writes one result line per op (`id op code`) to the shared directory.
  - It uses `TypedTopic<rune::gen::KiAccount>` and the `…AndWait` forms.
  - It logs in with `broker/docker/lib/auth.sh`'s e2e user.
  - It takes no host, IP or credential from the source: everything comes from the environment the script sets.

- [ ] **Step 1: Write the probe and the script.** House style: `set -euo pipefail`, bounded waits, trap teardown (`docker compose down -v` on every exit path), and every failure names its step. The script header documents the legs:
  1. Build the broker and probe images, generate the cluster key, `up` 3 nodes, bootstrap the admin and the e2e user. Bounded at leader 180 s.
  2. **SETUP:** `ki_account` with `partitions 4`, `replicas 3`, `cleanup.policy = compact`, `key.index` on.
  3. **RACE:** 4 probe containers insert the same 20,000 ids concurrently. About 30 % of the way through, `docker kill` the leader of partition 0 and restart it once progress resumes. **Check:** for every id exactly one `Ok` across all workers, and every other result for it is 56. No `VerdictUnknown` and no `Timeout`. Bounded at 600 s.
  4. **LIFECYCLE:** for 2,000 ids, `update` → ok; `update` of an absent id → 57; `removeExisting` → ok, then again → 57; `remove` → ok; re-`insert` → ok.
  5. **VERIFY:** a subscription from 0 on every partition sees exactly the accepted records, with `op()` equal to the op that produced each, and none of the 56/57-rejected ones.
  6. **RESTART:** `docker restart` all three nodes; re-insert every live id → 56, and every removed id → ok.
  7. **REFUSALS:** `key.index` on a transient topic, an untyped topic, a keyless topic and a `string(9)` key → the four exact P5-6 messages.
  8. Print `CHECKED-WRITES-E2E: OK`.

  The usage line is `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'`. It honours `E2E_LOG_DIR` and `FLIGHT_RECORDER_DIR` like `filter-e2e.sh`.
- [ ] **Step 2: Build check locally (no docker).** `cmake --build build/sub-client-cpp -j18 --target checked_writes_probe`. Expected: it builds. Then `bash -n broker/docker/checked-writes-e2e.sh` and `shellcheck broker/docker/checked-writes-e2e.sh` (if installed) report nothing.
- [ ] **Step 3: Docs.** The `docs/testing.md` row: `| docker/checked-writes-e2e.sh | 3 nodes | Write intents and the key index: racing inserts give exactly one Ok per id through a partition-leader kill, the update / removeExisting / remove lifecycle, consumers see only accepted records with their op(), a full restart rebuilds the key index, and the key.index DDL refusals. | leader 180 s, race 600 s |`
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/docker/checked-writes-e2e.sh broker/docker/docker-compose.checked-writes.yml client-cpp/examples/checked_writes_probe.cpp \
          client-cpp/docker/Dockerfile.checked-writes-probe client-cpp/docker/Dockerfile.checked-writes-probe.dockerignore \
          client-cpp/tests/support/GenClaimSchemas.cpp client-cpp/CMakeLists.txt docs/testing.md
  git commit -m "test(docker): checked-writes e2e - racing inserts, intent lifecycle, op(), restart rebuild, key.index refusals (VM only)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 12: The alloc gate, the benches and the docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`, `broker/benchmark/ViewFoldBench.cpp` (beside 04's `BM_ViewFoldVerdict/*`)
- Modify: `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `broker/docs/CATALOG.md`, `docs/operations.md`, `client-cpp/CLAUDE.md` (§2.1 Producer)

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateIntentJudgingAllocatesNothingRegression`, on the rig with `createKeyIndex()`, the plain projection and a raw subscription:
  - warm up with 2,000 batches over a fixed set of 10,000 ids: 1 and 16 records, `acks` 3, intents in the mix 40 % insert, 20 % update, 20 % removeExisting, 20 % plain. That grows the table past its size, and markers come every 20 ms.
  - then, under `rune::testing::AllocWindow w;`, 5,000 more batches of the same mix over the same ids, including 56/57 rejections;
  - `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Write the bench.** `BM_ViewFoldVerdict/key_index`: 04's `BM_ViewFoldVerdict` workload driven through a `VerdictSource` whose only member is the key index, every record an intent and none rejected. It reports `verdict_record_p50_ns`, `verdict_record_p99_ns` and `steady_alloc`. The gate is `verdict_record_p50_ns − BM_ViewFoldReplace's record_p50_ns ≤ 1000` (the §14 judging budget) and `steady_alloc == 0`. The stderr line is `[ViewFoldVerdict key_index gate] … => PASS|FAIL`.
- [ ] **Step 3: Run the gate.** `cmake --build build/root -j18 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches.** The baseline is this part's base commit on `main`: ABAB n=3, one session, the same box, one prepare/restore.
  - `git worktree add "$TMPDIR/rune-cw05-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel`.
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ProducePath_|BM_WorkerProduce_Majority|BM_WorkerProduceToPushRead_Disk|BM_CheckedWriteHook/|BM_ViewFoldReplace$' --benchmark_repetitions=3`, then the tip alone for `BM_ViewFoldVerdict/key_index`.
  - **Gates:** no regression on the ABAB set for untyped and already-validated typed topics. 05 adds one fact load to the hook and nothing to the produce path of topics without a key index. The key-index gate above must also pass.
  - Borderline numbers are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under `flock -w 7200 /root/vm.lock` on `$RUNE_VM_HOST`, then `vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Write the docs.**
  - **`wire-protocol.md`:**
    - record `attributes` bits 0–1 (the §5.1 table, "3 and the other bits: `InvalidRequest`");
    - codes 56/57 in the `View` ack trailer with `constraintId 0`;
    - the `DefineTopic` and `TopicSchemaResponse` key-feature blocks (byte table);
    - "on a raw topic the bits are stored as sent and not interpreted".
  - **`client-cpp.md`:**
    - `WriteIntent` on `send`;
    - the typed `insert` / `update` / `upsert` / `remove` / `removeExisting` and `…AndWait` with `WriteResult`;
    - the local refusals (P5-9);
    - `record.op()`;
    - `TopicOptions::keyFeatures`;
    - "intents need `All | View` (or `Leader | View`) and an idempotent producer";
    - "a lost ack is retried and replays the original verdict".
  - **`CATALOG.md`:** `formatVer` 3 (v2 plus the `SchemaDef` block, written from feature level 5 on and v2 below it, the fingerprint rule, `ApplyV3`'s carry), and the `key.index` DDL rules with their messages. Also fix the stale lines: `:14` ("Only 1 exists") becomes v1 / v2 / v3, and `:40` (kinds 32–63 reserved) names the namespace kinds 32–36.
  - **`operations.md`**, section "Key index":
    - what it is;
    - that it rebuilds from the log on each leadership and restart, with delivery and `View` acks waiting meanwhile;
    - `keyIndexRebuilds` and the rejection stats (C16 ids 1 and 2);
    - the 8-byte key limit until wide keys ship.
  - **`client-cpp/CLAUDE.md` §2.1:** "intents ride record attributes bits 0–1; the client refuses them locally without `View`, inside a transaction, or on a raw handle, and refuses any `View` send on a raw handle (R05a)".
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ViewFoldBench.cpp \
          docs/api/wire-protocol.md docs/api/client-cpp.md broker/docs/CATALOG.md docs/operations.md client-cpp/CLAUDE.md
  git commit -m "bench/docs: key-index judging gate, zero-alloc intent judging, intents/key index/catalog v3 docs" \
             -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c). Stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`, and CatalogGolden on `build/sub-codec` (v1, v2 and v3).
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) with `-R Regression -LE latency`, including `rune_checked_writes_matrix_tests` and `rune_checked_writes_stress_tests`.
  - `ctest --test-dir build/root -R CheckedWrites -j18 --output-on-failure` under `--repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 12 benches, recorded.
  - **The docker e2e on the VM:** `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'`, run from the branch's checkout in your own VM directory. Expected: `CHECKED-WRITES-E2E: OK`. Keep the logs with `E2E_LOG_DIR` on a failure.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model. The reviewer leaves PR comments, and the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune`, in one batch with one summary at the end:
  1. "Checkpoint the key index, guarded by a cleaner 'tombstones removed through' watermark (today rebuilt from the log on every leadership)", `enhancement`, `sev:low`, `area:broker` (P5-1, R05c).
  2. "Intents on Tier-2 batches and claims (`BatchBuilder`, `tryClaim`)", `enhancement`, `sev:low`, `area:client` (P5-9).
  3. "Expose `TopicKeyFeatures` from `TopicSchemaResult`", `enhancement`, `sev:low`, `area:client`, for Part 06 (P5-8).
  4. Every recorded deviation that is not urgent.
- [ ] Hand over to Part 06 per the overview's acceptance list for 05:
  - `key.index` and intents work on a real broker;
  - catalog v3 encodes every `TopicKeyFeatures` field;
  - `WriteResult` exists;
  - Part 06 lifts P5-6 rule 4 and gives `autoIncrement` / `partitioner` meaning.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage (§5).**
  - §5.1, the intent bits and table, codes 56/57 definite: P5-3, Task 6 (the table test) and Task 7 (on the wire). Bit pattern 3 and the other bits are 03's rule 1; the insert tombstone is P5-5.
  - §5.2:
    - the key index "same machinery as a projection keyed by the source key": `ViewTable`, the fold contract, the 1-byte row (P5-1, Task 6);
    - "enabled by `with (key.index = on)`": the DDL in Task 5 (the QL grammar is Part 08's);
    - "implied by `autoincrement`": Part 06, with the field encoded here;
    - "the check is a verdict view": Tasks 6–7;
    - "not enabled → definite `InvalidRequest`": 03's rule 4, exercised in Tasks 7 and 10;
    - "about 16 bytes per key, plus the checkpoint and the leader-only rebuild": P5-1 (no checkpoint, gap flagged).
  - §5.3, the log says what happened: the consumer `op()` and assertion (c) of the matrix (Tasks 3 and 8); the compaction caveat is in the docs (Task 12).
  - §5.4:
    - the raw `send(…, WriteIntent)`: Task 3;
    - the typed ops and `…AndWait`: Task 4;
    - "key type encoded exactly as `send(rec)` routes": Task 4's regression test plus Task 10's routing cell;
    - consumer `op()`: Task 3;
    - older clusters fail locally: SD11 through 03's `View` gate, Task 3 and Task 10.
  - §11.2, the intent row (idempotent + `View`): 03's rules plus the local refusals (Task 3). §12, the key.index DDL behind the level and the catalog format bump: Tasks 2 and 5. §13, the per-intent-code rejections: C16 ids 1–2 (Task 7). §15 cells owned by 05: Interaction matrix A–C.
- **Step scan.**
  - Every test step names its suite and test and its assertions with the spec's values: 56, 57, `constraintId 0`, 14-byte blocks, 56/64 B acks, 8-byte limit, 65,536 records per pass, 560/70 cells.
  - Code steps name files, signatures and placement.
  - No function body is transcribed. The algorithms the signatures leave open are given as tables or rules: the P5-3 intent table, the P5-7 apply and carry order, the P5-4 fact, and the P5-1 lifecycle.
- **Type consistency.**
  - `TopicKeyFeatures`, `Partitioner`, `WriteIntent`, `RecordOp`, `WriteResult` and `VerdictViewKind::KeyIndex` are spelled as in C9, C13 and C15.
  - `kKeyIndexPlanFingerprint`, `KeyIndexFold`, `prepareKeyIndex`, `evaluateIntent`, `applyKeyIndex`, `foldKeyIndexBatch`, `keyIndexWord`, `keyIndexes_`, `keyIndexActive`, `KeyIndexActivating`, `createKeyIndex(KeyShape, bool)`, `enableKeyIndex()`, `keyIndexKeys()` and `addKeyIndexRule()` are defined once (contract additions 5–9) and used with the same names in Tasks 6–12.
  - Part 07's preconditions (the SD8 message, `VerdictViewKind::KeyIndex` in `VerdictSource.cpp`, the rig's `createKeyIndex()`, the probes through Part 04's `ViewKey::fromU64`) match.
  - `testAdvertisedBrokerVersion` (contract addition 10) is defined once and used only in Task 10.
- **Review Focus.** Each of the five has its named test in its owning task: 7, 7, 7, 6/8, 10/3.
- **Proportion.** The plan fixes decisions, placements, signatures and assertions. The fold body, the DDL checks, the client plumbing, the probe and the rig bodies are left to the implementer within the stated rules.
