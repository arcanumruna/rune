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

## Rebase to `main` 750c836b (binding; overrides every number, name and line below)

The plans were written against `3c1ad01d` (the merge-base of the plan branch; client 11, broker 6), so the drift to check is `3c1ad01d..origin/main`: namespaces (#642, #741), #112, #116, #450, #589 and #770. Main has since taken many of the numbers they claim, and #589 shipped Part 01 in a different form. This section is the single source of truth for both. Where any plan text, contract (C1–C18), the registry or the file map disagrees with it, this section wins. Each part's implementer brief restates the rows it uses; line numbers in every plan are stale and are found again by the quoted text, symbol or sentinel.

### RB1 — Scope

- **Part 11 is dropped.** No part builds the transaction vote. Everything owned by 11 (C18, `TxnConflict`, `TxnTooLarge`, the cluster templates, `TxnVote` ops and return types, `kAttrAtomic`, stats 11–20, R11a–R11d, the §16.12 cross-check) is out. Part 03's refusal of a transactional batch to a constrained source (03 rule 7) is permanent until a follow-up issue reopens §9b. R03c stays: the client never sets `View` on a transactional send.
- **Part 01 is done by #589 (merge `f71596cc`).** It has no branch and no PR. RB3 says what later parts use instead.
- **R-ORD becomes:** 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09, with 10 after 03 (beside 04–08), and 09 after 10. Autoincrement is usable after 06; `unique` and `check` after 09.

### RB2 — Numbers (old → new)

| Space | Plan value | Value on main | Owner |
|---|---|---|---|
| `ErrorCode` (51–54 are `NamespaceSuspended`, `NamespaceLimitExceeded`, `Throttled`, `ViewPaused`) | 51 `ConstraintViolation`, 52 `KeyExists`, 53 `KeyNotFound`, 54 `AutoIncrementExhausted`, 55 `TxnTooLarge`, 56 `ViewKeyExpressionInvalid`, 57 `ViewConstraintInvalid`, 58 `ViewUniqueInvalid`, 59 `TxnConflict` | **55** `ConstraintViolation`, **56** `KeyExists`, **57** `KeyNotFound`, **58** `AutoIncrementExhausted`, **59** `ViewKeyExpressionInvalid`, **60** `ViewConstraintInvalid`, **61** `ViewUniqueInvalid`; `TxnTooLarge` and `TxnConflict` are not added. Next free 62 | 03 |
| `errorCodeName` (`QlEngine.cpp`), `produceErrorForClient` | names 48–58, map 51–58 below v12 | names through 61, `static_assert` on `ViewUniqueInvalid`; map 55–61 to `InvalidRequest` below `kClientVersionCheckedWrites` | 03 |
| Client version | `kClientVersionCheckedWrites = kClientVersion = 12`; "v11 client" | **17**; "v11" means 16 | 03 |
| Broker version | `kBrokerVersionCheckedWrites = kBrokerVersion = 7`; "v6 broker" | **10**; "v6" means 9 | 03 |
| Cluster feature level | `MetadataFeature::kCheckedWrites = 4` (4 is `kTxnPartitionVerify`); "below level 4" | **5**, `kLatest = kCheckedWrites`; "below level 4" means below 5 | 02 |
| `WorkerOp::Type` / SBE `OpType` (26–30 are taken) | 26 `ClusterFeatureLevel`, 27 `WriteSpecUpdate`, 29 `GetViewRowK`, 30 `ScanViewRows`, 31 `GetStats` | **31** `ClusterFeatureLevel`, **32** `WriteSpecUpdate` (02); **33** `GetViewRowK`, **34** `ScanViewRows` (07); **35** `GetStats` (08). Next free 36 | 02, 07, 08 |
| `WorkerReturn::Type` / SBE `ReturnType` (8 is `TXN_VERIFY_REQUEST`) | 9 `ViewScanResult`, 10 `StatsResult` | unchanged (9, 10). Next free 11 | 07, 08 |
| SBE `TailKind` | 12 `WRITE_SPEC`, 13 `VIEW_READ` | unchanged | 02, 07 |
| Client `TemplateId` (81–87 are namespaces and `WorkerBind`; the view range ends at 95) | 81 `GetViewRowK`, 82 `ScanViewRows`, 83 `ScanViewRowsResponse`, 84 `GetStats`, 85 `StatsResponse` | **88**, **89**, **90** (07); **91**, **92** (08). Next free 93 | 07, 08 |
| Control → admin message (`AdminChannels.h`; the `kMsg` ids are one space shared by several rings, 36 is the highest) | `kMsgClusterFeatureLevel = 26` | **37** (26 is in fact free; 37 is kept). Next free 38 | 02 |
| Admin → control message (26–28 are namespace messages) | `kA2cWriteSpec = 26` | **29**. Next free 30 | 02 |
| `SecurityOperation` (7 is `Truncate`) | `Import = 7`, `opBit(Import) == 0x80` | **`Import = 8`**; `validSecurityOperation` checks `<= Import`. **No mask widening:** `opsOn`, `clusterOps`, `PermissionTable` and every other op mask stay `uint8_t` over ops 0–7 (0x80 is `opBit(Truncate)`), and root stays `0xFF`. IMPORT lives only in Part 06's separate `importBits_` table, compiled from grants whose `op == Import`, so root and ADMIN never imply it (P6-14, `NothingImpliesImport`). Part 06's `0x80` literals for IMPORT (`opsOn needs no change…`, `…BitSeven…`, `& 0x80`, `allows(topicId, 0x80)`, `allows(3, 0x80)`) test `importBits_` instead; "op 7 round-trips, op 8 → BadEnum" becomes op 8 / op 9, and `NamespaceMessagesTest.cpp`'s out-of-range op moves to 9. Also touched: `opBit` (`SecurityEvent.h`), `validGrant` (`SecurityState.cpp`), `SecurityEvent.cpp`'s op range, `ql/src/Redact.cpp` op names, `ql/src/Parser.cpp` `parseOperation`. The op value is persisted in `__security` events, so a grant of IMPORT is refused below level 5 | 06 |
| SESSION_PERMISSIONS import marker (all eight `kPermFlag*` bits of the `isolation` byte are used; 0x80 is `kPermFlagSecurityBehind`) | `kPermFlagImport = 0x80` | **no new flag.** An import Table or Patch item sets the op's `topicId` (unused by SESSION_PERMISSIONS) to `kPermBitmapImport = 1` (`WorkerWriteGrants.h`; 0 = the write bitmap). The stager and the patch path route on it | 06 |
| Produce op flag (`deferAck` byte; 0x10 is `kOpFlagKeyPending`) | `kOpFlagImportGranted = 0x10` | **0x80** (the last free bit) | 06 |
| Control record types, batch attribute bits, record attribute bits, client ring flags, `SendResult` 12–13, `OpStatus` 12–14, view-read status 8, `StatId`, key-feature flags, `TopicWriteSpecWire` flags, checkpoint v3 magic `0x564B4333` and table ids 0–7, `view_probe` 180–189 | as planned | unchanged | — |

Every plan sentence that names a code, version or level by its old number ("52/53", "answers 56", "a v12 session", "v7 broker", "level 4", "op 31") means the new value from this table. Test names that embed an old number (for example `…BeforePart09…` tests asserting 58 and 57) assert the new values.

### RB3 — Part 01 as shipped, and what later parts use

#589 built the aborted-transaction skip without the plan's marker design:

- `rune::view::AbortedTxnFilter {fn, ctx}` with `skips(reader, attrs)` and `kNoAbortedTxns` (`ViewFold.h`); `foldBatch` / `foldAggregateBatch` and `WorkerViewSet::driveFolds` / `driveRebuild` / `driveView_` take `const AbortedTxnFilter&`; `Worker::rcAbortedFilter_(const RcState&)` builds it.
- `FoldStats::skippedAborted` and `ViewFoldCounters::skippedAborted`.
- Checkpoints live in `view-<id>-p<n>.committed.delta` (`viewCheckpointFilePath`, `ViewSnapshotChannel.h`). A pre-fix `view-<id>-p<n>.delta` (`legacyViewCheckpointFilePath`) is never loaded: recovery removes it and the view refolds cold from the log.
- The docker leg over a committed and an aborted transaction is in `view-engine-e2e.sh`.

So **R01 and SD15 are replaced by R01r**: there are no marked magics (`0x534B433x`), no `CheckpointMark` / `checkpointMarkOf` / `AppliedDelta::mark`, no `sourceLogStart` parameter, no `checkpointsDiscarded` / `checkpointsKeptUnverifiable` counters and no `view_probe` codes 170–179. Every checkpoint in a `.committed.delta` file was written by a fold that skips aborted batches.

What each later part does instead:

- **Part 04 creates `FoldSkip`.** `broker/libs/view/include/rune/view/FoldSkip.h` (file map: `C`, not `M`) is header-only, because `rune_view` links only `rune_codec`. It takes over `AbortedTxnFilter` and `kNoAbortedTxns` from `ViewFold.h`, which with `AggFold.h` now includes it, and adds `struct FoldSkip { AbortedTxnFilter aborted{}; const rune::storage::RejectedOffsetIndex* rejected = nullptr; };`, `inline constexpr FoldSkip kNoSkip{};`, `isAbortedBatch(const FoldSkip&, const RecordBatchReader&, int16_t attrs)` (= `skip.aborted.skips(r, attrs)`) and `isRejectedRecord`. `RejectedOffsetIndex` is header-only too. In the same task `const AbortedTxnFilter&` becomes `const FoldSkip&` (same position) on `foldBatch`, both `foldAggregateBatch` overloads, `WorkerViewSet::driveFolds` / `driveRebuild` / `driveView_` and `Worker::driveViewsOf_`; `Worker::driveViews_` builds `FoldSkip{.aborted = rcAbortedFilter_(*rcState), .rejected = …}` and passes it to `driveViewsOf_`; every test and bench caller (`grep -rl kNoAbortedTxns broker`, about 15 files, e.g. `WorkerViewSetTest`, `ViewFoldTest`, `ViewRecoveryRegressionTest`, `AggFoldTest`, `ViewFoldBench`, `NamespaceHotPathBench`) switches to `kNoSkip`. 04's two `FoldSkip.h` preconditions are deleted; its preconditions grep `struct AbortedTxnFilter` in `ViewFold.h` and `kInstallMsgType   = 3` with no `kResetMsgType` in `ViewSnapshotChannel.h`.
- **Part 04 also owns the checkpoint reset** it needs for P4-4 (a checkpoint ahead of the local markers is discarded): 01's AD6 and AD7 move into 04 unchanged — `ViewSnapshotChannel::kResetMsgType = 4` (1–3 are taken; 4 is free), `enum class CheckpointStream : uint8_t { Live, ResetOwed }` on `ViewInstance`, and a reset branch in the IO agent (`ViewSnapshotIoAgent.cpp` drops every non-delta type today) that truncates the `.committed.delta` file (`viewCheckpointFilePath`). `verdictCheckpointsDiscarded` is 04's own counter.
- **Parts 05, 07 and 09** use `FoldSkip` / `kNoSkip` as written, since they run after 04.
- **Part 07** keeps v3 magic `0x564B4333` and the v1/v2 decoders. It drops every `CheckpointMark` use, and the test `V3IsNeverDiscardedByTheAbortedRuleRegression` is not built (nothing discards on aborted ranges any more). Its precondition on `kViewCheckpointMagicV2Marked` becomes `grep -n "kViewCheckpointMagicV2 = 0x564B4332u" broker/libs/view/include/rune/view/ViewCheckpoint.h`.
- **Part 10** may merge before 04. It adds `[[nodiscard]] int64_t WorkerViewSet::foldCursorForTest(int32_t viewTopicId, int32_t partition) const noexcept` (−1 when absent) itself, so its precondition on it expects 0 hits. Main's signatures are positional: `driveRebuild` / `driveFolds(src, p, readRange, bound, aborted, nowNs, transientSrc, leaderGen, hwConfirmed, NsTag ns = {})`; there is no `sourceLogStart`, and `installViewForRecovery` takes no filter. The rig's `restart(...)` passes the filter positionally to `driveRebuild` / `driveFolds` only. Before 04 the filter is `AbortedTxnFilter{.fn = <thunk>, .ctx = &idx}`, the thunk copied from `Worker::rcAbortedFilter_`; after 04 it is `FoldSkip{.aborted = AbortedTxnFilter{…}}`. The `checkpointsDiscarded == 0` asserts become `skippedAborted == <aborted batches>` plus rows equal to the pre-restart table. Its precondition on `enum class CheckpointMark` becomes `grep -n "committed.delta" broker/libs/node/include/rune/node/ViewSnapshotChannel.h`. Its AD5a reasoning reduces to: a persisted-over-transient checkpoint always replays, and Task 2 pins that.
- **Part 01's test cells** that #589 did not cover (the plan's capped-pass `skippedAborted` counting, AD2, and the fold model check) go to one follow-up issue, not a PR.

### RB4 — Catalog format 3

Catalog `formatVer 2` is namespaces (`kFormatV2`, `kCurrentFormatVer`, `ApplyV2`, `golden/v2/namespaces.rcat`, the frozen `kNamespacesV2Digests`). Every plan mention of checked-writes "formatVer 2" means **3**:

- `kFormatV3 = 3` = v2 plus `TopicKeyFeatures` (and Part 06's autoincrement state). `checkFormatVer` / `apply` accept 3.
- `codec/src/catalog/ApplyV3.{h,cpp}`: `ApplyV3` runs `ApplyV2::run`, then the checked-writes rules.
- A v3 fingerprint layered over `objectFingerprintV2`; the v2 goldens do not change.
- `EncodeError::NeedsFormatV3`, `goldenScriptV3()`, a `v3` mode in `make_catalog_golden`, `golden/v3/{basic,autoinc}.rcat`, `kBasicV3Digests` / `kAutoIncV3Digests`, `CheckedWritesCatalogV3Test.cpp`.
- The AdminWorker writes v3 at level ≥ 5 and **v2 below it, never v1** (v1 drops `nsId`).
- The "unsupported format" test (`CatalogApplyV1Test` uses 3 today) moves to `formatVer = 4`.
- Part 05's precondition greps `kFormatV2 = 2` and no `kFormatV3`; Part 06's greps `kFormatV3`.

### RB5 — Updates from main, by part

**All parts.** Levels: "level 3" / L3 means 4 (`kTxnPartitionVerify`), "level 4" / L4 means 5, and "3 → 4" means 4 → 5. Keeping 3 in a test would also turn off #112's partition verification. Version bands: broker "1–6" means 1–9, client "1 to 6" means 1–9, "versions 1, 2 and 11" means 1, 2 and 16 plus one of 12–15. Rig defaults: client 17, broker 10. Conformance vectors are named by client version: `_v12` / `V12` become `_v17` / `V17` (`produce_response_v17*.bin`, `define_topic_v17_key_index.bin`, `topic_schema_response_v17_key_index.bin`); the existing DefineTopic vector is `define_topic_v8.bin`. Literal codes in test data move with RB2: 51→55, 52→56, 53→57. Plans 08, 09 and 10 say catalog formatVer 3 (RB4).

**Shared tables.**
- `isKnownTemplate` (`Dispatch.h`): a separate clause for 88–92; 87 (`WorkerBindAck`) stays unknown at ingress. Tests: 07's "(84) false" becomes `(91)`; 08's becomes `(91)`/`(92)` true and `(93)`/`(87)` false.
- `NamespaceSuspendTable.h` `kDispatchedTemplates`: Parts 07 and 08 add 88, 89 and 91 (size 39, default read row `{Allow, Refuse}`); 90 and 92 go in the test's `kResponses`. On main the test already fails on 86; Part 07 adds 86 to `kResponses` too.
- `lineageErrorName` (`client-cpp/src/Client.cpp`) and `authErrorName` (`Completion.h`) extend through 61 with `errorCodeName`.

**Part 02.** Preconditions grep `kLatest = kTxnPartitionVerify;`. The static_assert is `kCheckedWrites == 5 && kLatest == kCheckedWrites && kTxnPartitionVerify == 4`. The kind probe passes the namespace: `reg_.byName(msg.nsId, msg.name)` and `topicByName(msg.nsId, msg.name)`. When `kNsFeatureViews` is off in the namespace, `NamespaceLimitCheck`'s view refusal wins over P6's `featureRefusal`; add one non-default-namespace cell.

**Part 03.** Preconditions grep `kCheckedWrites = 5` and `kClientVersion = kClientVersionResponseEgress;`. SD12 adds `nsAcksAllowed`: `l.lowestAcks < 2 || acksAll(wireAcks)` (plus its comments), so wire 2 (Leader|View) cannot pass a namespace that requires All. The rule-7 refusal and the P3-2 View clear run as a frame check before `txnAdmissionRefuses_` (#112), so a refused transactional batch never starts a verify round trip.

**Part 04.**
- Its new `PartitionWriteState*` parameter goes after `NsTag ns = {}` and passes through `driveViewsOf_`.
- P4-16 uses `replica->lastCleanOffset()`.
- Type 19 (`kTxnJudge`) stays reserved: recognised, skipped and always kept by the cleaner. `Part::P11` and `TxnAllOrNothingAndAtomicPerPartition` go, and `TxnTooLarge` leaves the copied constraints.
- Namespace memory: the judge's members and Part 05's `keyIndexes_` are adopted and charged to their namespace (`adoptNs_` / `chargeMem_`). A member paused by `nsViewGate_` stalls the judge for that source, so delivery and `View` acks wait until it resumes.
- The control term floor rises with verdict sources: a 32,824 B `View` ack needs `RUNE_AERON_CONTROL_TERM_BYTES` ≥ 512 KiB, so the broker raises the floor to 512 KiB when any verdict source exists.

**Part 05.** RB4. The test target is `rune_gateway_catalog_tests`. `AbortedFoldLog.h` / `AbortedFoldRig.h` do not exist: use `ViewAbortedTxnRegression`'s rig.

**Part 06.** RB2's IMPORT row. `CatalogTopicAppliedMsg`: the two feature bytes go after `nsId` (length `… + 4 + 2`), and `postCatalogTopicApplied_` keeps its `nsId` parameter.

**Part 07.**
- `RowStatus { Ok, NotFound, NotReady, Paused, BadKey }`.
- `OpKind` values append after `NamespaceUsage`.
- The precondition greps `ViewUniqueInvalid = 61`.
- `getRowK` and `scanPage` take the same gates as `GetViewRow`: the namespace query throttle (53) and the view-paused check (54) on the worker, and the client-version re-encode on the gateway.
- Line 753: `FoldSkip` and `CheckpointStream` come from Part 04; `sourceLogStart` is gone.

**Ctest filters** (04, 07, 10): `CheckedWritesAbortedFold` means `ViewAbortedTxnRegression` (target `rune_view_aborted_txn_tests`).

**Part 08.**
- Its preconditions grep `GET_VIEW_ROW_K">33`, `SCAN_VIEW_ROWS">34` and `VIEW_SCAN_RESULT">9`. The TXN_VOTE clause is deleted.
- P8-9 reads: 26–30 are main's (TxnVerifyResult, NsSlotAssign, NsSlotRelease, NsThrottle, NsViewMemOver), 31/32 are 02's, 33/34 are 07's and 35 is GetStats; return 8 is main's, 9 is 07's and 10 is StatsResult.
- GetStats is routed and gated like `GetViewRow`. It counts against the namespace query throttle, and `StatsResponse` gains status 53 (`Throttled`), version-gated like `ViewRowResponse`.
- QM2 adds a cell where the source is in another namespace; it answers 33 (`ViewSourceNotFound`).
- `partitionLedElsewhere_` is created by extracting it from `viewSourceLedElsewhere_`.
- The follower-redirect test (08:707) does not pin the endpoint text, because v16+ redirects carry the response-control port.

**Part 09.** `ThreeNodes::startWith(tag, tweak)` replaces `start(tag, tweak)`. A restart is `stop(id)` or `crash(id)` followed by `startNode(id)`; there is no `restart` / `restartAll`.

**Part 10.** Views over a transient source get the source topic's `NsTag` once 37 is lifted, so they are charged and paused (`NsViewMemOver`) and read-throttled like any view. Add one namespace cell.

**Aeron 1.51.** Aeron no longer ships `concurrent/ringbuffer/RecordDescriptor.h`; any plan code needing it includes `rune/protocol/ring/RecordDescriptor.h`.

### RB6 — Releases and builds

- The release constants in "Sequencing and merge rules" and owner decision 13 read `kClientVersion = 17`, `kBrokerVersion = 10`, `kCheckedWrites = 5`, after Part 09.
- Builds and unit or integration tests run on the developer Mac (`build/root`, `-j18`). Docker e2e, dr-e2e, the Linux-only stage and benches stay on the shared VM under its lock.

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
- `lower(string(N))` width = the largest lowered width at the pinned Unicode version (⌊3N/2⌋ at Unicode 16.0.0, packed 0x00100000; R07a).
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
- **SD15 — #537 refold rule. Replaced by ruling R01 (plan 01 AD5a) for all parts; the text below is not binding.** ~~No checkpoint format change: Part D owns v3. At recovery, a persisted view whose source partition's `RcState` aborted index is non-empty discards its v1/v2 checkpoint and refolds from the log start.~~ R01: checkpoints written by the fixed code use the marked magics 0x534B4331 / 0x534B4332; an unmarked checkpoint is refolded once, only when an aborted range starts below its unmarked coverage and the log still starts at 0; every v3 record counts as marked.
- **SD16 — The `View` ack and open transactions.** A `View` waiter on a partition behind an open transaction waits for the LSO (§8.9). Waiters live in their own queue (C4), so `All` acks never queue behind them.

## Rulings register (binding; they override this file where they differ)

Each ruling below was decided by the controller while the part plans were written. Every part plan restates the ones it depends on. The gist here is complete enough to act on.

- **R01:** Checkpoints written by the fixed code use the marked magics 0x534B4331 / 0x534B4332 (replaces SD15 for all parts). An unmarked checkpoint is refolded once, only when an aborted range starts below its coverage and the log still starts at 0. Part 07's v3 never uses 0x534B433x and counts as marked.
- **R02a:** `RecordSpan::key` and the bounds-checked `forEachRecordSpan` belong to Part 02. Part 03 consumes them.
- **R02b:** Tombstones skip value validation at every level. Only the key width is checked.
- **R02c:** A record whose schema version is above the worker's max gets a retriable `NotLeader`, not 24.
- **R02d:** Keyless typed topics in QL (`key` optional) belong to Part 08.
- **R02e:** Part 02's `TopicFeatureRules.h` (`featureRefusal`, `topicShapeOf`) is the DDL refusal helper that 05, 06 and 09 call.
- **R03a:** An ack longer than an Aeron publication's `maxPayloadLength` (a v12 ack reaches 32,824 B) goes out with `offer` (fragmented); a shorter one uses `tryClaim`. The 4096-record batch tests it.
- **R03b:** On untyped topics the broker stores intent bits as opaque bytes, and the client refuses intents, key-0 creates and `View` there locally (amended by R05a).
- **R03c:** The client never sets `View` on a transactional send, and the broker ignores it there. Transactions get their verdicts through Part 11's vote (`commitTransactionAndWait`).
- **R03d:** Part 03's matrix ids are 120–139, folded into Part 04's shared matrix.
- **R03e:** Tombstones need a key of exactly `keyWidth` bytes. Part 02's rule wins if they differ.
- **R04a:** The two-phase drop belongs to Part 09. A dropping verdict view's plan stays installed, and keeps judging, on every replica until W passes the drop's change record; only then is it uninstalled. A failover during the drop finishes it.
- **R04b:** C11's "follower apply" is the `RcState` leadership scan rebuilding the rejected-offset index from markers. Followers hold no verdict state between leaderships.
- **R04c:** A fourth marker trigger: judged records reaching a persisted verdict view's checkpoint cap force a marker, so a checkpoint never outruns the markers.
- **R04d (amended):** Part 04 appends 7 `Timing` values (03's six plus `RetryAfterWindow`). Later parts only append.
- **R04e:** A known duplicate with `View` on a verdict source replays the original verdicts once W passes the original batch (06 and 09 add the assigned keys and constraint names). Outside the dedupe window it is `DuplicateSequence` (21), which the client reports as one `OpStatus::VerdictUnknown` for the batch, never per record.
- **R04f:** Part 04's contract additions are binding: `RejectedOffsetIndex`, `ConstraintSet` / `ConstraintSetTimeline`, `kMaxVerdictViewsPerSource = 32`, `MarkerLedger`, the `TestReject` seam, `RcVerdictHooks` and the cleaner bounds.
- **R04g:** `OpStatus::VerdictUnknown = 14` (13 = `StoredIdUnknown`). The 32-slot dedupe window keeps the original base offset, so a `View` retry inside it replays the original verdicts.
- **R05a:** Local refusals on an untyped topic (intents, key-0 creates, `View`) are a synchronous `SendResult::WriteNotAllowed` with no completion.
- **R05b:** Part 11 defines the C8 atomic-batch header bit. Parts 05, 06 and 09 do not.
- **R05c (amended):** The key index is not checkpointed; each new leadership rebuilds it from the local log. Intents during the rebuild are accepted, judged once it finishes, and their `View` acks wait. `KeyIndexActivating` (retriable `NotLeader`) applies only before the key index's change record is committed. Follow-up issue: checkpoint the key index, guarded by a cleaner "tombstones removed through" watermark.
- **R05d:** `ApplyV2` (catalog `formatVer` 2) stays open until the first release. Part 06 appends its rules to it rather than adding a v3.
- **R05e:** Part 03's sweep invariant (iv) has one exception: `KeyIndexActivating` answers `NotLeader`.
- **R05f:** Part 05's contract additions are binding: the 14-byte `TopicKeyFeatures` block, `TopicWriteSpecWire::keyIndex` (flags bit 1), the key-index API of `VerdictSource`, client `WriteIntent` / `WriteResult` / `TopicKind` / `kDefaultWriteWait`, and matrix ids 20–24 / 20–25. Part 06 places `partitioner` after `TopicHandle::kind`.
- **R06a:** Autoincrement on an untyped topic is refused with "autoincrement needs a typed topic"; "autoincrement needs a key" is for keyless typed topics.
- **R06b (owner-flagged):** The cleaner keeps a tombstone while the producer batch that created its id is still kept (the last 32 per live producer), so a dedupe replay never brings a deleted id back.
- **R06c (owner-flagged):** The key index still checks broker-assigned ids as a reused-id detector. A hit rejects the record with 52 and counts a determinism alarm.
- **R06d:** No delete-records API exists, so that §15 cell is not applicable; a follow-up issue is filed.
- **R06e:** An `IMPORT` reservation extends to a full block.
- **R06f:** Part 08 owns the QL `autoincrement` column modifier (the `partitions` default is refined by R08f).
- **R06g:** A timed-out `insertAndWait` is an unknown outcome. `StoredIdUnknown = 13` is for a stored batch whose ids can't be returned.
- **R06h:** Autoincrement is allowed only on compacted topics with no time or size retention. Part 06's contract additions are binding: `partitionFor`, the 26-B `TopicWriteSpecWire` (flags bit 2), `AutoInc.h`, `Import = 7`, `kPermFlagImport = 0x80`, `kFlagKeyZero = 0x08`, `ProduceAck::assignedBase`, matrix ids 40–45 / 40–46, stat ids 8–10.
- **R07a:** A lowered `string(N)` key component is ⌊3N/2⌋ bytes, the table-computed maximum at Unicode 16.0.0, packed 0x00100000.
- **R07b:** The `lower()` table is generated by a tool and committed, and a test cross-checks it against UnicodeData.txt.
- **R07c:** Composite `group by` is in Part 07. Group-by columns form a composite `ViewKey` with the same order-preserving encoding as `key (a, b)` and must equal the view's key spec. Single 8-byte integer aggregates keep today's path.
- **R07d (reworded):** Part 04 defines `ViewKey.h` with `fromU64` / `asU64` (`fromU64(v).asU64() == v`, `asU64` needs width ≤ 8); 05 and 06 use them. Part 07 owns and extends `ViewKey` (`fromBytes`, `operator==`, wide keys) and never renames them or changes their meaning.
- **R07e:** `GetViewRowK` carries a client-computed partition. The old `GetViewRow` (72) on a new-style view answers `InvalidRequest`. The AdminWorker refuses `unique` / `check` until Part 09. Encoders write v1 bytes when no v2 field is used.
- **R07f:** Part 07 takes op types 29 and 30, tail kind 13, return type 9, view-read status 8 and matrix ids 60–68. Part 08 starts return types at 10 and owns the QL half of the `lower('Foo')` cell.
- **R07g:** A keyed aggregate needs an integer source key of at most 8 bytes, otherwise it is refused with 56; a follow-up issue is filed.
- **R08a:** Aggregates see only the latest record per source key, so RegressorsPath's `device_login` gets a per-login key (`loginId uint64 autoincrement`). The golden, the e2e and the migration doc match.
- **R08b:** The golden adds spec §1's `character` topic, and the golden e2e runs on `ThreeNodes`.
- **R08c:** Part 08 defines `PredicateNode::constBytes` (the row-slot encoding, padded to 16 B), Part 09 decodes it the same way, and `aggregateOutputType` moves to the codec's `ViewPlanAuthored.h`.
- **R08d:** The `select` key check runs at plan time. `GetStats` is served by the leader only and needs `DESCRIBE`.
- **R08e:** Part 08 parses and plans `unique` / `check`; the AdminWorker refuses them (58, 57) until Part 09 deletes those lines and inverts the `…BeforePart09…` tests.
- **R08f:** Part 02 owns the parser change that makes `partitions` optional; 08 owns the autoincrement no-partitions cell. `isSecurityStatement` becomes exact. `GetStats` = op 31, `StatsResult` = return 10. Part 09 adds stat 32 and Part 11 stats 11–20 through `StatsCollect.h`.
- **R09a:** Control record type 20 (`kVerdictPlan`) holds the installed plan of a dropping or replaced verdict view, so the two-phase drop survives a restart or a lagging catalog. The cleaner always keeps it. Part 11 would start new types at 21.
- **R09b:** A dropped or replaced plan is uninstalled once W passes its change record (amends 04 P4-6). Unique claims use their own tables (checkpoint v3 tables 6 and 7).
- **R09c (owner-flagged):** The literal rule is kept. An existing duplicate that only updates a non-key column is accepted, a move out of a group already over its limit is rejected, and a NULL or non-finite column makes a check unknown, so the record passes.
- **R09d:** Caps: 32 verdict views per topic (the key index counts as one), a 64 KiB constrained plan, 64 predicate nodes, and checks over fixed-width columns of at most 16 B. Matrix ids 80–86 / 80–90. `Client::constraintName` has a timeout overload. A rejected record's completion carries its offset in `value`.
- **R10a:** Part 02's `featureRefusal` table holds KeyIndex, AutoIncrement and Constraint × transient; 05, 06 and 09 call it.
- **R10b (owner-flagged wording):** Every view over a keyless topic is refused with 35 ("view source needs a key"); spec §4 wins over §9a. Part 10 has no keyless refusal of its own.
- **R10c:** Part 10 runs after 01 and 03. Its `View`-ack cell stays in 10.
- **R10d:** After a restart a gap counts as one `transientSourceResets` instead of `transientRecordsEvicted`; a deferred issue is filed.
- **R10e:** Part 10's matrices live in their own binary with their own enums; no id range is reserved.
- **R11a (owner-flagged; recommended accept):** New code `TxnConflict = 59`. A transaction votes no when a newer plain write to one of its keys lands before its `TxnJudge` (optimistic concurrency). Plain writers never wait behind a transaction.
- **R11b:** Every fold (live, refold, rebuild, a replica's leadership scan) applies a transaction's records at its commit marker, in the same order, so every view and verdict is identical however the view was built.
- **R11c:** A coordinator failover never loses vote candidates: on recovery every partition of the transaction's persisted partition set is a candidate, and a partition with nothing constrained answers yes at once.
- **R11d:** Distributed lock waits resolve through the transaction timeout (a deadlock-detection issue is filed). The `TxnJudge` payload carries `firstOffset`. The overlay holds after-images. One-step visibility comes from a visibility bound (the commit marker, R11b) plus a read fence. Clients below v12 get 39 on a no vote. `commitTransactionAndWait` returns `TxnCommitResult`. `read_uncommitted` on a verdict source is served as `read_committed`. The cleaner keeps a `TxnJudge` while it lies at or above the `txnJudgeBound`. Part 11 edits matrix rows 05 and 06 for the pre-G refusals. Part 11's rune-site change is a local commit only, follows the site's copy rules, and is never pushed or deployed without the owner's go-ahead.
- **R-ORD:** Dependency order 01, 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 11. 07 depends on 06 (`partitionFor`). 10 runs after 01 and 03, and 09 depends on 10 (its transient-source tests need 37 lifted).
- **R-X1:** Part 02's `checkTypedRecord(const TopicWriteSpec&, const RecordSpan&, int16_t level)` in `WriteState.h` carries all of Part 02's rules and counters. Part 03's single walk calls it and adds `HookRefusal::VersionAhead` → `NotLeader`.
- **R-X2:** `OpStatus`: 04 adds `StoredIdUnknown = 13` as a reserved placeholder (06 documents it) and `VerdictUnknown = 14`.
- **R-X3:** Checkpoint v3 accepts any table id below its table count; a test pins 8 tables.
- **R-X4:** The first marker starts at W, extended past spans whose constraint set is empty. K includes this node's own unmarked verdicts.
- **R-X5:** `checkTypedBatch` and `checkTypedRecord` take `int16_t level`. Below level 4 a topic created without `validate` stays unchecked. `HookRefusal::KeyMismatch` is replaced by `VersionAhead`, and key mismatches count as `ValidationFailed`.
- **R-X6:** This overview's contract, number registry, test table and file map are brought in line with all part plans in one pass after Part 11's plan (done).
- **R-X7:** Import batches answer `assignedBase = 0`. The reuse alarm and `idsBurned` count only batches judged in the leadership that appended them. Test knobs: `NodeConfig::testAdvertisedBrokerVersion`, `ClientConfig::producePartitionUnknownForTest`, `ClientConfig::dropProduceAcksForTest`. A local `View` refusal on a raw handle says "acks View needs a typed topic".

## Owner decisions pending

Each item is implemented as the plans say until the owner overturns it.

1. **`TxnConflict` (59)**, a new error code beyond §11.1 that aborts a transaction on a write-write conflict (R11a, 11 P11-3).
2. **A partition also votes under its latest constraint set** (11 P11-1 (b)), beyond §G.2's "set active at the transaction's first record".
3. **The key index is not checkpointed** (R05c): a failover pauses intents' verdicts for a rebuild proportional to the log, against the spec's mention of a checkpoint.
4. **Tombstone retention for autoincrement ids** (R06b), a reading of §6.6.
5. **Broker-assigned ids still go through the key index** as a reuse detector (R06c), against §6.4's "skip".
6. **Plain upserts are refused on autoincrement topics** (06 rule 8a, §6.6), against §11.2's "a plain upsert passes on any topic".
7. **The literal constraint rule** for duplicates, over-limit groups and NULL checks (R09c).
8. **Spec wording:** views over keyless topics are refused with 35 (R10b); the lowered width is ⌊3N/2⌋, not ⌈3N/2⌉ (R07a).
9. **Part 02's stricter validation:** a version above the worker's max answers retriable `NotLeader` instead of 24 (R02c), and dirty `string(N)` padding is rejected.
10. **Constraint ids exhausted** at `objectId ≥ 65536` (SD5) and **no Aeron-counter export of stats** (SD13).
11. **`TxnVoteMsg` carries no transactional id** (11 contract addition 4), against §G.2's message sketch.
12. **Tier-2 batches carry no intent** (05 P5-9, follow-up issue): a Rune client cannot send an atomic batch of intents or key-0 creates.
13. **Merging this plan set to `main`, every push, the release** (`kClientVersion = 12`, `kBrokerVersion = 7`, `kCheckedWrites = 4`) after Part 11, and **any rune-site deploy**.

## Review Focus (program level; each part adds its own five)

1. **A dedupe retry of a stored batch on a verdict source.** This covers a retry before the fold judged the batch, one after, and one after failover plus compaction. It must get the original offsets, the original `assignedBase` and the same per-record verdicts. It must never get a second id or a second judgment (§6.6, §11.3).
2. **Every reader of a log meets control records of types 16–20.** These readers are `RcBuilder::step` (`ReadCommittedView.cpp:315`), the PSM rebuild (`RcState.h:660`), `LogCleaner::scanTxnState` (`LogCleaner.cpp:229`), the push and filter paths, `forEachRecordSpan`, the folds, `SubscriberHub` (`SubscriberHub.cpp:600`) and `DataCodec` (`DataCodec.h:341`). Each must skip the records and must never take one for a COMMIT or ABORT marker.
3. **Mixed versions.** A v11 broker receiving `acks` 2 or 3 answers `InvalidRequest` (`DataMessages.h:141`). A v11 client never receives a v12 `ProduceResponse` or `EndTxnResponse` layout, codes 51–59 or `assignedBase`. A v12 client facing a v6 broker fails locally with `BrokerTooOld`.
4. **Consumers of a verdict source while a constraint view builds, or while the fold is stalled.** Delivery waits, never hangs past the build, never delivers an unjudged record, and the `View` ack deadline still answers `Internal`.
5. **A `View` batch of more than 4096 records on a verdict source.** The client splits it, a raw producer gets `InvalidRequest`, and no ack ever exceeds `kMaxEncodedSize` (SD4).

---

## Plan index

| NN | File | Spec | Depends on (merged first) | Delivers to later parts |
|---|---|---|---|---|
| 01 | `2026-10-22-checked-writes-01-aborted-fold.md` | #537, §8.8 last bullet | — | `rune::view::FoldSkip` (C9.2) threaded through `foldBatch` / `foldAggregateBatch`, rebuild and transient folds; the R01 refold rule (plan 01 AD5a; replaces SD15) |
| 02 | `2026-10-22-checked-writes-02-typed-validation.md` | §4 (Part A), §12 level | — | `kCheckedWrites` and its propagation (C2); `TopicWriteSpec`, `PartitionWriteState`, the RcSlot pointer and `WriteSpecUpdate` (C7); `checkTypedRecord` / `checkTypedBatch` (R-X1, R-X5); `RecordSpan::key` and the bounds-checked `forEachRecordSpan` (R02a); `TopicFeatureRules.h` `featureRefusal` (R02e, R10a); always-on validation, wire key = key field, UTF-8 (`codec/include/rune/codec/Utf8.h`); `Key` truncation turned into an error; the bind checks, the `raw` marker, `partitions` defaulting to 1, DDL refusals on untyped and keyless topics |
| 03 | `2026-10-22-checked-writes-03-write-hook.md` | §3 write path, §8.9, §11.2, §12 wire | 02 | The hook policy (C8); the `Acks` flags and `View` waiters (C4); the v12 wire skeleton (C1, C5); codes 51–58 with `errorCodeName`; intent constants and the `RecordSpan` `attributes` / `index` members (C3; `key` is 02's); the requirement seam `evaluateRequirements` / `HookRefusal`; `WriteNotAllowed`, `BrokerTooOld`; the per-write requirement table; SD10, SD11; the alloc-gate binary |
| 04 | `2026-10-22-checked-writes-04-verdict-pipeline.md` | §3 fold, §8.4–§8.8, §11.3 | 01, 03 | `ViewKey` with `fromU64` / `asU64` (C9.1, R07d); `SourceRecord` / `Prospect` and prepare/apply (C9); `VerdictSource`; control-record layout and types (C10); `RejectedOffsetIndex`, `VerdictCleanState` and the cleaner bound (C11); `ConstraintSetChange` timeline (C12); consumer bound and filter; ack verdict trailer; the duplicate replay and `OpStatus` 13 (reserved) / 14 `VerdictUnknown` (R04e, R-X2); the shared matrix, model and stress harness (§ Shared test infrastructure) |
| 05 | `2026-10-22-checked-writes-05-intents-key-index.md` | §5 (Part B) | 04 | The key index as `VerdictViewKind::KeyIndex`; intents 52/53; catalog `formatVer` 2 with `TopicKeyFeatures` and the DefineTopic / TopicSchemaResponse fields (C13); `WriteIntent`, `RecordOp`, `WriteResult`, typed `insert`/`update`/`upsert`/`remove`/`removeExisting` (C15); the first docker e2e |
| 06 | `2026-10-22-checked-writes-06-autoincrement.md` | §6 (Part C) | 05 | The modulo partitioner; `AutoIncState`, reservations, side window and cleaner retention; `IMPORT` (C17); `assignedBase` filled; `insertAndWait(...).key()`; the meaning of `OpStatus::StoredIdUnknown` (13, reserved by 04) |
| 07 | `2026-10-22-checked-writes-07-wide-view-keys.md` | §7, §9 (Part D), §8.1 mechanics (SD7) | 06 (it uses `partitionFor`; R-ORD) | `ViewKey` tables, seeded hash, owner map, prefix index, checkpoint v3; plan v2 format with every field (C14), and the authored plan moved to codec; codec `lower` and the key encoding; `GetViewRowK` 81 and `ScanViewRows` 82/83; client read API and key builder; key index widened to 16 bytes (SD8) |
| 08 | `2026-10-22-checked-writes-08-ql.md` | §10 | 06, 07 | Grammar (topics, `create view`, `select`), prepare to plan v2, CLI decode, `show topic` / `show view`; `GetStats` 84/85, the stat ids (C16) and `StatsCollect.h`; removal of the message-search mapping (SD3) |
| 09 | `2026-10-22-checked-writes-09-view-constraints.md` | §8.2, §8.3, §8.7 (Part E) | 08, 10 (its transient-source tests need 37 lifted; R-ORD) | `unique` and `check` (`VerdictViewKind::Unique` / `Check`), predicate evaluation on `Prospect`, unique conflicts and promotion, constraint create / replace / drop through C12, with the two-phase drop and control type 20 `kVerdictPlan` (R04a, R09a); stat 32; `Client::constraintName`; the RegressorsPath e2e scenario |
| 10 | `2026-10-22-checked-writes-10-persisted-over-transient.md` | §9a (Part F) | 01, 03 (R10c) | 37 lifted; keyless sources stay refused by Part 02's §4 rule (35, R10b), no refusal of its own; the `reconcileTransient_` logging change (`WorkerViewSet.cpp:367`); docs |
| 11 | `2026-10-22-checked-writes-11-txn-vote.md` | §9b (Part G), §16.12 | 09, 10 (every part 01–10 merged; R-ORD) | `TxnVote` wire and ops (C18), the `TxnJudge` record with `firstOffset`, deferred judging, after-image overlays and row locks, `TxnConflict` 59 (R11a), apply-at-marker on every fold path (R11b), atomic batches (`kAttrAtomic`), one-step visibility, `TxnTooLarge`, `TxnCommitResult`, stats 11–20; lifting the pre-G refusals of Part 03; the §16.12 docs cross-check |

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
  - Part 03 adds `[[nodiscard]] constexpr bool clientUnderstandsCheckedWrites(int32_t clientVersion) noexcept` (≥ 12), and `produceErrorForClient` maps 51–58 to `InvalidRequest` below 12 (P3-5).
  - **Part 11 appends `TxnConflict = 59`** (R11a): a vote-time write-write conflict. `errorCodeName` and its `static_assert` move to 59, `produceErrorForClient` covers 51–59, and `[[nodiscard]] constexpr bool isTxnVoteCode(uint16_t) noexcept` is true for 51, 52, 53, 55 and 59.
- **Versions, owned by Part 03.** `kClientVersionCheckedWrites = 12`, `kClientVersion = 12`, `kBrokerVersionCheckedWrites = 7`, `kBrokerVersion = 7`. All later v12 wire (07, 08, 11) is gated on the same constants.
- **`TemplateId`** (view range 64–95, `Protocol.h:90-102`): `GetViewRowK = 81` (07), `ScanViewRows = 82` (07), `ScanViewRowsResponse = 83` (07), `GetStats = 84` (08), `StatsResponse = 85` (08).
  - `GetViewRowK` is answered by the existing `ViewRowResponse` (73).
  - Each owner extends `Dispatch.h::isKnownTemplate()` and the conformance vectors.
  - `EndTxnResponse` (54) gains a v12 body (11, C18).

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

- **`SendResult`** (`Producer.h:302`), append-only: `KeyTooLong = 12` (02: a key wider than `Key::kMaxInline`, 16 B; `TypedTopic` always sends the key field's width, so it has no other meaning) and `WriteNotAllowed = 13` (03: a §11.2 requirement the client checks locally; 05's local refusals, R05a).
- **`OpStatus`** (`client-cpp/include/rune/client/Completion.h:54`), append-only (R-X2):
  - `BrokerTooOld = 12` (03, SD11);
  - `StoredIdUnknown = 13`: added by 04 as a reserved placeholder so the enum has no hole; 06 documents it ("stored; id unknown") and first returns it;
  - `VerdictUnknown = 14` (04, R04e/R04g): a `View` duplicate outside the dedupe window; one status for the whole batch.
- **`Completion` for `OpKind::Send`** on a checked write:
  - `status = BrokerError` with `errorCode` 51–53 for a rejected record, otherwise `Ok`.
  - `reserved0` = `constraintId`.
  - `value` = the record's offset, also for a rejected record (05 `WriteResult::offset()`, 09 P9-11).
  - `aux` = the assigned id (06; 0 = none).
  - A rejected atomic batch (11) completes once: `BrokerError` with the first failing record's code, `reserved0` and `value`.
- **`Completion` of a commit (11)** on a no vote: `BrokerError`, `errorCode` = the vote code (51, 52, 53, 55, 59), `reserved0` = `constraintId`, `value` = the failing offset; the producer is `Ready`. `commitTransactionAndWait` returns `struct TxnCommitResult { bool ok; uint16_t code; uint32_t constraintId; int64_t failingOffset; }` (`Producer.h`).
  - Accessors in `Completion.h` (03): `[[nodiscard]] constexpr uint32_t constraintIdOf(const Completion&) noexcept` and `[[nodiscard]] constexpr uint64_t assignedKeyOf(const Completion&) noexcept`.
- **`WriteResult`** (05, new file `client-cpp/include/rune/client/WriteResult.h`), returned by every `…AndWait` write of `TypedTopic`:
  - `[[nodiscard]] bool ok() const noexcept;`
  - `[[nodiscard]] uint16_t code() const noexcept;`
  - `[[nodiscard]] uint32_t constraintId() const noexcept;`
  - `[[nodiscard]] uint64_t key() const noexcept;` (06 fills it; 0 = none)
  - `[[nodiscard]] OpStatus status() const noexcept;`
  - `[[nodiscard]] int64_t offset() const noexcept;` and `static WriteResult fromCompletion(const Completion&) noexcept;` (05).

### C7. Per-partition write state, next to `RcSlot` (Part 02 creates; later parts append members)

- **New header `broker/libs/node/include/rune/node/WriteState.h`.**
  - **`struct TopicWriteSpec`**, one per typed topic per worker. Members by part:
    - **02:** `int32_t topicId; uint16_t keyFieldId = kNoKeyField /*0xFFFF*/; rune::codec::PrimType keyType; uint8_t keyWidth = 0; bool legacyValidate;`, plus the per-version `SpecVersion { Validator; Descriptor; uint32_t keyValueOffset; }` (`versionFor`, `maxVersion`, `installVersion`, `setFacts`). Every version is materialized when it is installed, so the produce path never allocates. Lookup is `[[nodiscard]] const rune::codec::Validator* validatorFor(uint16_t version) const noexcept` and `const rune::codec::Descriptor* descriptorFor(uint16_t version) const noexcept`, with nullptr meaning an unknown version, which is rejected.
    - **05:** `bool keyIndex = false;`.
    - **06:** `bool autoIncrement = false; uint64_t autoIncrementMax; uint32_t autoIncrementReserve; int32_t partitionCount; rune::protocol::Partitioner partitioner;`.
  - **`struct PartitionWriteState`**, one per owned partition of a typed topic. Members by part:
    - **02:** `const TopicWriteSpec* spec;`.
    - **04:** `RejectedOffsetIndex rejected; ConstraintSetTimeline constraintSets; VerdictSource* verdicts = nullptr; VerdictSourceStats stats; MarkerLedger markers;`, with `isVerdictSource()`, `isConstrainedSource()` and `verdictKnownThrough()`.
    - **06:** `AutoIncState autoInc; AssignedBaseWindow assignedBases; AutoIncStats autoIncStats;` (`AutoInc.h`).
    - **09:** `RetainedVerdictPlans retainedPlans;`.
    - **11:** `TxnJudgeState txn;`.
  - **Checks (02, R-X1, R-X5):** `TypedCheck checkTypedRecord(const TopicWriteSpec&, const RecordSpan&, int16_t level) noexcept` and `checkTypedBatch(…, int16_t level)` live in `WriteState.h`. 03's hook calls `checkTypedRecord` per record.
  - **`TopicWriteSpecWire`** (the `WriteSpecUpdate` tail): 9 B (02; flags bit 1 `keyIndex`, 05), then 26 B (06; flags bit 2 `autoIncrement`, partitioner, max, reserve, partition count).
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
  - `struct WriteFacts { uint8_t acks = 0; bool idempotent = false; bool transactional = false; bool atomicBatch = false; bool importGranted = false; bool legacyMode = false; bool levelCheckedWrites = false /*03*/; int64_t producerId = -1 /*06*/; };`. `transactional` comes from the batch header attrs. `atomicBatch` comes from the header bit `rune::protocol::batch::kAttrAtomic = 0x0080` (bit 7) on a non-transactional batch; **Part 11 defines the bit** (R05b) and sets the fact, and it is false until then. `importGranted` comes from C17.
  - The seam (03): `TopicHookFacts`, `topicHookFactsOf`, `BatchIntents`, `RequirementVerdict { ErrorCode code; HookRefusal why; }`, `constexpr evaluateRequirements(…)` and `enum class HookRefusal` (parts append values before `Count_`).
- **Requirements (§11.2) are checked in `check()`, in this order.** Each failure is `InvalidRequest` unless noted:
  1. reserved attribute bits or intent 3;
  2. legacy mode (SD10);
  3. level < 4 for any non-zero intent or `View`;
  4. a non-zero intent on a topic without a key index;
  5. an explicit-key intent (`insert` with a non-zero key, `update`, `removeExisting`) without idempotence, or without the `View` bit (from 11: the `View` bit is not needed inside a transaction);
  6. the same inside a transaction or atomic batch, until 11 (11 deletes the rule);
     - 6b (05): an intent before the key index's `ConstraintSetChange` is committed → `NotLeader`, `KeyIndexActivating` (R05c, R05e);
  7. a transactional or atomic batch on a constrained source, until 11 (11 deletes the rule);
  8. for key-0 creates: mixed batches, idempotent + `All`, and `View` on a constrained source (03; from 11 not inside a transaction); then 06's 8a–8f (plain upsert on an autoincrement topic, `IMPORT`, import range, activation `NotLeader`, `AutoIncrementExhausted` 54, side window full `NotLeader`);
  9. `View` and > 4096 records on a verdict source (SD4);
  10. (11) an atomic batch of more than `txnJudgedRecordsMax` records on a verdict source → `AtomicBatchTooLarge`.
  Plain upserts and deletes pass with no requirement (06's 8a refuses plain upserts on autoincrement topics, §6.6).

### C9. Verdict-view interface inside the view engine (Part 04 unless stated)

1. **`broker/libs/view/include/rune/view/ViewKey.h`** (04 defines it; 07 owns and extends it, R07d): `inline constexpr std::size_t kMaxViewKeyBytes = 128;` and `struct ViewKey { std::array<uint8_t, kMaxViewKeyBytes> bytes{}; uint8_t len = 0; static ViewKey fromU64(uint64_t) noexcept; [[nodiscard]] uint64_t asU64() const noexcept; std::span<const uint8_t> span() const noexcept; };`.
   - `fromU64(v)` gives `len = 8`, the little-endian bytes of `v`. `asU64()` requires `len ≤ 8` and returns the zero-extended little-endian load of the first `len` bytes, so `fromU64(v).asU64() == v`. 05 and 06 use both from 04. No other conversion name exists (no `packed()`).
   - Part 07 appends `static ViewKey fromBytes(std::span<const uint8_t>) noexcept` and `bool operator==(const ViewKey&) const noexcept`, makes `rune::view::kMaxViewKeyBytes` an alias of `rune::codec::kMaxViewKeyBytes` (one constant, 128), and adds `ViewTable` overloads `put/get/erase(const ViewKey&)` and the `> 8` byte storage. Until then a `ViewKey` with `len ≤ 8` maps to today's `uint64_t` API.
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
   enum class PrepareStatus : uint8_t { Ok, Malformed, CapacityExhausted, Locked /*11*/ };
   struct Prospect;   // fixed-capacity, per-instance scratch reserved at install: the op (None/Upsert/Erase/Move),
                      // up to 2 prospective rows (new; old group for a Move) as row images, and the view keys touched.
   ```
   - In `ViewFold.h`: `PrepareStatus prepareProjection(ProjectionFold&, const SourceRecord&, Prospect&) noexcept;` and `void applyProjection(ProjectionFold&, const SourceRecord&, const Prospect&) noexcept;`.
   - In `AggFold.h`: `prepareAggregate(AggregateFold&, …)` and `applyAggregate(AggregateFold&, …)`.
   - `ProjectionFold` and `AggregateFold` are reference bundles that `ViewInstance` (`WorkerViewSet.h:191`) builds once.
   - `foldBatch` and `foldAggregateBatch` keep their signatures plus `const FoldSkip&`. They are re-implemented as the prepare-then-apply loop, so "the same code" computes and applies.
   - Invariant, tested by 04: on random logs, the loop produces state byte-identical to the pre-04 fold (04's `LegacyFoldReference`). The folds run 04's `projectionStep<DirectStore>` / `AggStep<DirectStore>`; prepare runs the same template body over `OverlayStore`.
   - Part 11 appends a trailing `TxnOverlay* chain = nullptr` to `prepareProjection`, `prepareAggregate`, 05's `prepareKeyIndex` and 09's claims prepare, and `uint8_t member; const TxnLockSet* locks` to the fold bundles (`TxnOverlay.h`, chained after-images and row locks). With both null, 04's path is unchanged.
   - Part 11 (R11b): every fold path (live, refold, rebuild, checkpoint replay, leadership scan) applies a committed transaction's records at its commit marker, in offset order; Part 01's `FoldSkip.aborted` stays as a defensive skip.
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
     - `[[nodiscard]] int64_t judgedThrough() const noexcept;` and `[[nodiscard]] bool stalled() const noexcept;` (capacity: the fold stops with an alert, never a verdict). From Part 11, `judgedThrough() = min(judgeCursor(), txn.earliestUndecidedFirst())`, with `[[nodiscard]] int64_t judgeCursor() const noexcept` and the cold `bindTxn(…)`; the judge bound is the HW instead of the LSO.
     - `void addVerdictView(ViewInstance*, VerdictViewKind, uint64_t viewObjectId)`, `void removeVerdictView(...)` (cold).
     - Test seam `installTestVerdictViewForTest(…)`, which builds a `TestReject` view rejecting offsets chosen by a predicate.
   - **Order within one duty pass for a verdict source:** (11) finalize locked transactions whose marker is below the HW; then `driveJudge`; then the other views of the partition with `FoldSkip{.rejected = &state.rejected}`, bounded by `judgedThrough()`; then marker emission (C10). Part 04 wires this into `WorkerViewSet::driveFolds` (`WorkerViewSet.h:417`).
   - `WorkerViewSet` gets `[[nodiscard]] VerdictSource* verdictSource(int32_t srcTopicId, int32_t partition) noexcept`. After every install, uninstall and ownership change, the worker re-resolves `PartitionWriteState::verdicts` (`Worker::rebindVerdictSources_()`). Hot paths only read `state->verdicts`.
6. **The consumer bound.** `[[nodiscard]] int64_t Worker::verdictBound_(const PartitionWriteState*, int64_t bound) const noexcept` returns `min(bound, verdicts->judgedThrough())`, or `bound` for a non-source. It is applied at `pushOwnedSubscriptions_` (`Worker.h:3736-3744`), `servePushRead_` (`Worker.h:4312`) and the filtered push. Rejected records are cut with `rune::protocol::rebuildBatch` (`BatchRebuild.h:82`), which keeps their offsets.

### C10. Control records (Part 04 creates the header and types 16–19; 09 adds type 20; 06, 09 and 11 add builders and parsers)

- **New header** `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (namespace `rune::storage::control_record`, header-only, builders write into a caller buffer and never allocate).
- **Batch form.** A control batch with `attributes = kAttrControl` (0x0020) only, **`kAttrTransactional` clear**, `producerId = -1`, `producerEpoch = -1`, `baseSequence = -1`, and one record. The record key is an int16 LE **type**. The value is `[version u8 = 1][payload]`. `producerId -1` keeps it out of `TxnCleanState` (`LogCleaner.cpp:229`) and the PSM feed (`RcState.h:660`).
- **Types.** `inline constexpr int16_t kReservation = 16; kVerdictMarker = 17; kConstraintSetChange = 18; kTxnJudge = 19;` (04) and `kVerdictPlan = 20` (09, R09a). Transaction markers use 0 and 1 (`ControlMarkerLayout.h:30-31`). Types 16–20 are taken; a new type starts at 21. Part 11 adds none: it fills 04's reserved 19.
- **Parser.** `[[nodiscard]] int recordType(const uint8_t* batch, std::size_t len) noexcept` returns the type or `kUnparseable` (−1).
- **Hardening (04).** `ControlMarker::extract` (`broker/libs/transaction/src/ControlMarker.cpp:289-296`) and `RcBuilder::step` (`ReadCommittedView.cpp:315`) accept only types 0 and 1. A regression test feeds a batch of each new type to every reader listed in Review Focus 2.
- **Payloads:**
  - `kVerdictMarker` (04): `[fromOffset i64][finalThrough i64][constraintSetId u32][count u32] count × [offsetDelta u32 (offset − fromOffset)][code u16][constraintId u32]`. It covers the judged range `[fromOffset, finalThrough)`. `kMaxMarkerEntries = 4096`; more rejections in one step means several markers.
  - `kConstraintSetChange` (04): `[constraintSetId u32][count u16] count × [viewObjectId u64][kind u8 (VerdictViewKind)][planFingerprint u64]`. The key index appears with `viewObjectId = 0`.
  - `kReservation` (06): `[reservedThroughN u64]` (the last `n` reserved; ids are `n·P + p`).
  - `kVerdictPlan` (09): a `VerdictPlanHeader { viewObjectId u64, planFingerprint u64, kind u8 }` plus the plan, output and source descriptor blobs (byte layout in 09 P9-8), at most `kMaxVerdictPlanBytes = 65536` (`buildVerdictPlan` / `parseVerdictPlan`). It holds the installed plan of a dropping or replaced verdict view, so the two-phase drop (R04a) survives a restart.
  - `kTxnJudge` (11): `[producerId i64][producerEpoch i16][firstOffset i64]` (`TxnJudgeRecord`, `buildTxnJudge`, `parseTxnJudge`; R11d). Nothing writes type 19 before Part 11, so `firstOffset` needs no compatibility.
- **Who appends.** The partition leader's worker appends these records through the replica's control append, the path `AppendControlRecord` already uses (`WorkerOp.h:86`). Reservations are the exception: they are appended inside `CheckedWriteHook::stage` through `log`. A `TxnJudge` is appended by the leader's `TxnVote` op (11) once the HW is above the transaction's last record.

### C11. Rejected-offset index, cleaner state and the cleaner bound (Part 04)

- **`broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h`** has one instance per partition replica, in `PartitionWriteState::rejected`. It is worker-owned. It is filled by the leader's judge and, on every replica, by the `RcState` leadership scan, which rebuilds it from the markers in the log (R04b; followers hold no verdict state between leaderships). It is reserved at creation and never allocates on the hot path.
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
  04 appends `coveredThrough()` / `noteCovered()` (K), `mismatches()`, `countAtOrAfter()`, `capacity()` and `grow()` (cold). 11 appends `[[nodiscard]] bool insertRun(std::span<const int64_t> offsets, uint16_t code, uint32_t constraintId);` (cold, out of order; the marker-without-vote path).
- **`broker/libs/storage/include/rune/storage/VerdictCleanState.h`** is cleaner-thread only, cold, and modeled on `TxnCleanState` (`TxnCleanState.h:40`).
  - API: `clear()`, `onVerdictMarker(int64_t markerOffset, int64_t fromOffset, int64_t finalThrough, std::span<const RejectedOffsetIndex::Entry>)`, `onConstraintSetChange(int64_t offset, bool nonEmpty)`, `onReservation(int64_t offset)` (06), **`finish(int64_t toOffset, int64_t publishedBound, int64_t markerRemovalBound)`** (04's signature; it replaces the one-argument form), `void markRemovalsBelow(int64_t cleanBoundary) noexcept` (04), `isRejected(int64_t)`, `cleanLimit()`, `markerRemovable(int64_t)`, `keepsReservation(int64_t)` (06) and `[[nodiscard]] bool keepsControlRecord(int64_t offset, int16_t type) const noexcept` (04).
  - `keepsControlRecord`: type 18 always kept; 16 iff `keepsReservation` (06); 20 always kept (09, R09a); 19 kept while `offset >= txnJudgeBound`, the minimum `lastDurableO` over the source's persisted verdict members (11; 04 keeps 19 always until then).
  - It is fed by the `scanTxnState` pre-pass (`LogCleaner.cpp:212`) for control batches with `pid == -1`.
- **Cleaner bound.**
  - `PartitionReplica::publishCleanerVerdictBound(Offset)` and `[[nodiscard]] Offset cleanerVerdictBound() const noexcept` are an atomic word beside `publishCleanerLso` (`PartitionReplica.h:866`). The default is `kNoCleanerLso`, meaning not a verdict source.
  - The worker publishes `W` from `PartitionWriteState::rejected.watermark()`.
  - 04 adds `publishCleanerVerdictMarkerEnd(Offset)` / `cleanerVerdictMarkerEnd()`, published before the bound with release order and read after it with acquire order, and `LogCleaner::setVerdictBounds(Offset bound, Offset markerEnd)`.
  - 11 adds `publishCleanerTxnJudgeBound(Offset)` / `cleanerTxnJudgeBound()` (default `kNoCleanerLso`) and `LogCleaner::setTxnJudgeBound(Offset)`.
  - `LogCleanerAgent.h:455` and `PartitionReplica::cleanerSwapBound` (`PartitionReplica.h:893`) take the min with it: on a leader `cleanerSwapBound()` is `min(HW, cleanerLastStableOffset(), cleanerVerdictBound())` (04 Task 8).

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
- **Wire.** `DefineTopic` (`protocol/include/rune/protocol/client/SchemaMessages.h:181`) and `TopicSchemaResponse` take a v12 trailing block after their existing tail: `[flags u8: bit0 keyIndex, bit1 autoIncrement][partitioner u8][autoIncrementMax u64][autoIncrementReserve u32]`, `kTopicKeyFeaturesWireLen = 14` (05, `putTopicKeyFeatures` / `getTopicKeyFeatures`). The client exposes it on its `createTopic` options. `TopicHandle` (`client-cpp/include/rune/client/TopicHandle.h:34`) appends `TopicKind kind = TopicKind::Unknown;` (05) and then `rune::protocol::Partitioner partitioner = Partitioner::Fnv1a;` (06, R05f).
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
  - `uint32_t unicodeVersion = 0;` packed as `major << 16 | minor << 8 | update`; Unicode is pinned to 16.0.0 = `0x00100000` (R07a). A lowered `string(N)` component is ⌊3N/2⌋ bytes wide, computed from the table.
- **Predicate.** `struct PredicateNode { PredicateOp op; uint16_t a = 0, b = 0; std::string column; rune::codec::PrimType constType{}; std::array<uint8_t, 16> constBytes{}; };`. `constBytes` holds the row-slot encoding of the constant, padded to 16 B (08 P8-5, R08c); 09 decodes it the same way. `aggregateOutputType` lives in `ViewPlanAuthored.h` (08). The tree is a flat vector, root last. Its operator enum is `enum class PredicateOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };`.
- **Compiled plan v2** (`ViewPlan.h`) adds resolved key components (`srcFieldId`, `keyFunction`, `width`, `outSlot`), `keyWidth`, `unique`, compiled constraints (`ordinal`, predicate over out-slots), `unicodeVersion` and `viewObjectId`.
- **Versions.** `kAuthoredVersion` and `kCompiledVersion` become 2 (`broker/libs/view/src/ViewPlan.cpp:14-15`). v1 blobs still decode.
- **Codec key encoding** (07, `codec/include/rune/codec/ViewKeyEncoding.h`): `encodeKeyComponent(...)`. Unsigned integers are big-endian. Signed integers are big-endian with the sign bit flipped. `bool` is one byte. `string(N)` is zero-padded. `lower` goes through `codec/include/rune/codec/UnicodeLower.h` (`kUnicodeVersion`, `lowerUtf8`, `maxLoweredWidth(N)`). The fold, the client key builder and QL literals all use it.

### C15. Client write and read API names (owners in brackets)

- **Raw producer:** `send(topic, key, value, WriteIntent intent = WriteIntent::Upsert)` and the `trySend` counterparts. `enum class WriteIntent : uint8_t { Upsert = 0, Insert = 1, Update = 2 };` [05]. An empty value with `Update` means delete-existing. Tier-2 batches (`Producer::batch`, `BatchBuilder`) carry no intent [05, follow-up issue] and set `kAttrAtomic` [11].
- **`TypedTopic<T>`:** `insert`, `update`, `upsert`, `remove(const KeyT&)` and `removeExisting(const KeyT&)`, each with an `…AndWait` form returning `WriteResult` [05]. `insertAndWait(rec).key()` [06].
- **Consumer.** `Record` (`client-cpp/include/rune/client/subscriber/Subscription.h:117`) appends `uint8_t attributes`, and `[[nodiscard]] RecordOp op() const noexcept` with `enum class RecordOp : uint8_t { Upsert, Insert, Update, Delete };` [05].
- **Reads [07].** `getViewRowAndWait(view, key)` (any width) and `scanViewAndWait(view, prefix, limit)`. `ViewKeyBuilder` comes from the view's output descriptor. New `OpKind` values are appended after `Seek` (`Completion.h:51`): `GetViewRowK`, `ScanViewRows`. Part 08 appends `GetStats`.
- **Constraints.** `Client::constraintName(uint32_t constraintId) -> std::optional<ConstraintName>` with `struct ConstraintName { std::string view; std::string constraint; };` [09].
- **Transactions.** `commitTransaction()` completes with the no vote's `code`, `constraintId` and failing offset, using the C6 completion fields, and `commitTransactionAndWait()` returns `TxnCommitResult` (it still throws for every other failure) [11]. The client never sets `View` on a transactional send (R03c); `Verdict::VotedNo` classifies a no.

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

  Ids 1–31 are topic-partition scope and 32+ are view-partition scope. Per-constraint rejections ride the trailing list (04 keeps them in `VerdictSourceStats`). Id 36 also counts each reconcile of a checkpoint recovered over a transient source (10). Part 11's `votesNoConflict` (59), `txnOverlayWaits`, `lockPauses`, `checkpointBackfills` and `vetoedCheckpointPushes` have no stat id; they are logged and counted in id 12.
- **Extension point (08).** `broker/libs/node/include/rune/node/StatsCollect.h`: `StatsSink`, `collectTopicPartitionStats` (11 appends 11–20) and `collectViewPartitionStats` (09 appends 32 through a trailing `bool uniqueView`). `GetStats` is served by the leader only and needs `DESCRIBE` (R08d).
- **Where the counters live.** `VerdictSourceStats` is in `VerdictSource.h` (04), `AutoIncStats` is in `WriteState.h` (06), and `ViewFoldCounters` (`WorkerViewSet.h:130`) appends `uniqueConflicts`, `scanRows` and `scanPages`.
- **Test access before 08.** `Worker::verdictStatsForTest(int32_t topicId, int32_t partition)`.

### C17. The `IMPORT` grant (Part 06)

- `SecurityOperation::Import = 7` (`protocol/include/rune/protocol/SecurityTypes.h:10`). `validSecurityOperation` is extended accordingly.
- The `opsOn` bit 7 (`codec/include/rune/security/Permissions.h:18`).
- The QL grammar is `grant import on topic <pattern> to …`.
- The worker side is `WritePermTable::importBits` (`broker/libs/node/include/rune/node/WorkerWriteGrants.h:28`), shipped by `SESSION_PERMISSIONS` with a new flag `kPermFlagImport = 0x80`. The produce op carries `kOpFlagImportGranted = 0x10` (`WorkerOp.h:657`). It is read into `WriteFacts::importGranted`.

### C18. The transaction vote (Part 11)

- **Cluster plane** (`ReplicationMessages.h`, schema 112): `TxnVoteMsg` with `TEMPLATE_ID = 13` and `TxnVoteResponseMsg` with `TEMPLATE_ID = 14`. Ids 1–4 and 7–12 are used today. Both are routed like `WriteTxnMarkerMsg` (`ReplicationMessages.h:837`), through the gateway's `TxnMarkerChannel` (`ClientGateway.h:957`).
- **Worker.** `WorkerOp::Type::TxnVote = 28` (SBE `TXN_VOTE = 28`) and `WorkerReturn::Type::TxnVoteResult = 8` (SBE `ReturnType` `TXN_VOTE_RESULT = 8`).
- **Layouts.** `TxnVoteMsg` (`BLOCK_LENGTH = 40`) and `TxnVoteResponseMsg` (48) as in 11's contract addition 4. The transactional id is not carried.
- **`EndTxnResponse`** (client template 54) takes a v12 trailing `[constraintId u32][failingOffset i64]`: `ENCODED_SIZE_V12 = 38`, written only for a v12 session with a vote. A session below 12 gets `InvalidRequest` (39) for every no-vote code, with no trailer (R11d).
- **The record and the vote.** The leader appends `TxnJudge` (type 19, with `firstOffset`, C10) and answers once it is committed and judged. Candidates are the partitions that might vote at the first `AddPartitions` or at commit; after a coordinator recovery every partition of the persisted set is a candidate (R11c).
- **Write-write conflicts.** A newer accepted write by another writer to one of the transaction's keys before its `TxnJudge` makes the vote no with `TxnConflict` (59, R11a).
- **After-images and row locks** (R11d: the overlay holds after-images; tables keep the before-images) are pre-allocated per worker at `NodeConfig::txnJudgedRecordsMax` (env `RUNE_TXN_JUDGED_RECORDS_MAX`, default 4096, range 1–1,048,576; SD14). Distributed lock waits resolve through the transaction timeout (R11d).
- **Atomic batches.** `rune::protocol::batch::kAttrAtomic = 0x0080` (bit 7). On a verdict source an atomic batch is judged as a unit at its own offset; its `View` ack carries at most one trailer entry.


### Code and number registry (every number the program takes)

> Superseded where it differs by **RB2** (Rebase to `main` 750c836b). Part 11 rows are dropped (RB1).

Each row gives the number, its name and its owning part. A part takes a new number only from the next free value of its row and adds it here first.

| Space | Taken (owner) | Next free |
|---|---|---|
| `ErrorCode` (`Protocol.h`) | 51 `ConstraintViolation`, 52 `KeyExists`, 53 `KeyNotFound`, 54 `AutoIncrementExhausted`, 55 `TxnTooLarge`, 56–58 compile errors (03); 59 `TxnConflict` (11, R11a) | 60 |
| Client `TemplateId` (view range 64–95) | 81 `GetViewRowK`, 82 `ScanViewRows`, 83 `ScanViewRowsResponse` (07); 84 `GetStats`, 85 `StatsResponse` (08); 54 `EndTxnResponse` gains a v12 body (11) | 86 |
| Cluster-plane templates (schema 112) | 13 `TxnVoteMsg`, 14 `TxnVoteResponseMsg` (11) | 15 |
| `WorkerOp::Type` / SBE `OpType` | 26 `ClusterFeatureLevel`, 27 `WriteSpecUpdate` (02); 28 `TxnVote` (11); 29 `GetViewRowK`, 30 `ScanViewRows` (07); 31 `GetStats` (08) | 32 |
| `WorkerReturn::Type` / SBE `ReturnType` | 8 `TxnVoteResult` (11); 9 `ViewScanResult` (07); 10 `StatsResult` (08) | 11 |
| SBE `TailKind` | 12 `WRITE_SPEC` (02); 13 `VIEW_READ` (07) | 14 |
| Admin channels | `kMsgClusterFeatureLevel = 26` (control → admin) and `kA2cWriteSpec = 26` (admin → control), separate rings (02) | 27 on each ring |
| Control record types | 16 `kReservation`, 17 `kVerdictMarker`, 18 `kConstraintSetChange`, 19 `kTxnJudge` (04; 19's payload is 11's); 20 `kVerdictPlan` (09) | 21 |
| Batch attribute bits | bit 7 `kAttrAtomic = 0x0080` (11) | bit 8 |
| Record attribute bits | bits 0–1 intent (03); bits 2–7 reserved and refused | — |
| Client ring flags (`ProducerImpl.h`) | `kFlagIntentMask = 0x06`, `kFlagIntentShift = 1` (05); `kFlagKeyZero = 0x08` (06) | 0x10 |
| `SendResult` | 12 `KeyTooLong` (02); 13 `WriteNotAllowed` (03) | 14 |
| `OpStatus` | 12 `BrokerTooOld` (03); 13 `StoredIdUnknown` (reserved by 04, meaning and use 06); 14 `VerdictUnknown` (04) | 15 |
| View-read status | 8 `kCursorInvalidated` (07); `StatsResult::kNotLeader = 8` (08) is a separate struct's status, not a wire clash | 9 |
| `StatId` | 1–2 (05); 3–7 (04); 8–10 (06); 11–20 (11); 32 (09); 33–34 (07); 35–36 (existing; 36 extended by 10) | 21 (topic scope), 37 (view scope) |
| Security | `SecurityOperation::Import = 7`, `kPermFlagImport = 0x80`, `kOpFlagImportGranted = 0x10` (06) | operation 8 |
| `DefineTopic` / `TopicSchemaResponse` key-feature flags | bit 0 `keyIndex`, bit 1 `autoIncrement` (05, 06); block of 14 B | bit 2 |
| `TopicWriteSpecWire` flags | bit 1 `keyIndex` (05); bit 2 `autoIncrement` (06); 9 B, then 26 B (06) | bit 3 |
| Checkpoint magics | `0x534B4331`, `0x534B4332` marked v1/v2 (01, R01); `0x564B4333` v3 (07) | — |
| Checkpoint v3 table ids | 0 view, 1 owners, 2 contributors, 3 groups, 4 blocks, 5 group ids (07); 6 unique index, 7 unique extras (09) | 8 |
| `ViewSnapshotChannel` message | `kResetMsgType = 4` (01) | 5 |
| Matrix `Op` / `Entity` | 04 0–19; 05 20–24 / 20–25; 06 40–45 / 40–46; 07 60–68; 09 80–86 / 80–90; 11 100–106 / 100–107; 03 120–139 | per-part range |
| Matrix `Timing` | 14 §15 values; 03's six and `RetryAfterWindow` (04); ten Part G values (11) | append only |
| `view_probe` exit codes | 170–179 (01); 180–189 (10) | 190 |
| Caps | `kMaxJudgedBatchRecords = 4096` (03, SD4); `kMaxVerdictViewsPerSource = 32` (04; the key index counts as one, R09d); `kMaxVerdictPlanBytes = 65536`, `kMaxPredicateNodes = 64`, checks over fixed-width columns ≤ 16 B (09); `kTxnOverlaysPerWorker = 8`, `TxnJudgeState::kMaxTxns = 1024` (11) | — |

---

## File map

C = create, M = modify. Line numbers are where the change attaches today.

| File | C/M | Owner(s) |
|---|---|---|
| `broker/libs/view/include/rune/view/FoldSkip.h` | C | 01 (+04) |
| `broker/libs/view/{include/rune/view/ViewFold.h,src/ViewFold.cpp}`, `AggFold.{h,cpp}` | M | 01, 04, 07, 09, 11 |
| `broker/libs/view/{include/rune/view/AggState.h,src/AggState.cpp}` (composite group keys) | M | 07 |
| `broker/libs/view/CMakeLists.txt` (link `rune_transaction`; new sources and tests) | M | 01, 04, 07, 09, 11 |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | 01, 03 (`minFoldCursor`), 04, 05, 07, 09, 10, 11 |
| `broker/libs/node/include/rune/node/RcState.h` (`RcVerdictHooks`, the leadership scan) | M | 04, 06, 11 |
| `broker/libs/node/include/rune/node/WriteState.h` | C | 02 (+04, 05, 06, 09, 11) |
| `broker/libs/node/include/rune/node/AutoInc.h` | C | 06 |
| `broker/libs/node/include/rune/node/TopicFeatureRules.h`, `ClusterFeatureLevelFanout.h` | C | 02 |
| `broker/libs/node/include/rune/node/Worker.h` | M | 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11 |
| `broker/libs/node/include/rune/node/WorkerOp.h`, `WorkerReturnCodec.h`, `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*` | M | 02 (26/27, `WRITE_SPEC`), 03, 06 (`kOpFlagImportGranted`), 07 (29/30, tail 13, return 9), 08 (31, return 10), 11 (28, return 8) |
| `broker/libs/node/include/rune/node/AdminChannels.h`, `AdminWorker.h`, `src/AdminWorker.cpp` | M | 02, 05, 06, 07, 08, 09, 10 (`AdminChannels.h`: 02, 05, 06) |
| `broker/libs/node/include/rune/node/BrokerNode.h`, `src/BrokerNode.cpp`, `NodeConfig.h`, `src/NodeConfig.cpp` | M | 02, 04 (`verdictIntervalMs`), 05 (`testAdvertisedBrokerVersion`), 06, 07 (`viewHashSeedForTest`), 11 (`txnJudgedRecordsMax`) |
| `broker/libs/node/include/rune/node/StatsCollect.h` | C | 08 (+09, 11) |
| `broker/libs/node/include/rune/node/ConstraintDdlRules.h` | C | 09 |
| `broker/libs/replication/include/rune/replication/ReplicationMessages.h` | M | 02 (level), 11 (13/14) |
| `broker/libs/replication/{include/rune/replication/AeronPartitionTransport.h,src/AeronPartitionTransport.cpp}` | M | 11 |
| `codec/include/rune/codec/Utf8.h` (+`codec/src/Utf8.cpp` if not header-only) | C | 02 |
| `codec/include/rune/codec/{Validator.h,PrimType.h}`, `codec/src/Validator.cpp` (UTF-8, key types, `StringCheck`) | M | 02 |
| `client-cpp/include/rune/client/{Key.h,TypedTopic.h,Client.h}`, `client-cpp/src/cli/CatalogCommands.cpp:250`, `Console.cpp:138` | M | 02 (+04, 05, 06, 07, 08, 09, 10) |
| `ql/src/Parser.cpp` | M | 02 (`partitions` optional, untyped marker), 06 (`grant import`), 08 (everything else) |
| `ql/include/rune/ql/Ast.h`, `ql/src/Backends.cpp` | M | 08 |
| `broker/libs/storage/include/rune/storage/WriteHookPolicy.h`, `ProducePathImpl.h` | C | 03 |
| `broker/libs/storage/{include/rune/storage/ProducePath.h,src/ProducePath.cpp}` | M | 03, 06 |
| `broker/libs/node/{include/rune/node/CheckedWriteHook.h,src/CheckedWriteHook.cpp}` | C | 03 (+04, 05, 06, 11) |
| `protocol/include/rune/protocol/client/{Protocol.h,DataMessages.h,Dispatch.h}`, `protocol/conformance/*` | M | 03 (+07, 08, 10, 11) |
| `protocol/include/rune/protocol/client/TxnMessages.h` (`EndTxnResponse` v12) | M | 11 |
| `protocol/include/rune/protocol/{RecordBatch.h,BatchRebuild.h}` | M | 02 (`RecordSpan::key`, bounded walk), 03, 05 (`forEachRecordWithAttributes`), 11 (`kAttrAtomic`) |
| `broker/libs/node/include/rune/node/SessionAckSink.h` | M | 03, 04 (arena), 06 |
| `broker/libs/gateway/{include/rune/gateway/ClientGateway.h,src/ClientGateway.cpp}` | M | 03 (acks decode, SD10 refusals), 05, 06, 07 (81–83), 08 (84/85), 11 (`TxnMarkerChannel`, `sendTxnVote`) |
| `client-cpp/include/rune/client/producer/{Producer.h,ProducerImpl.h,BatchBuilder.h,TxnTypes.h,ProduceFrameBuilder.h}`, `src/producer/*` | M | 02, 03, 04, 05, 06, 09, 11 |
| `client-cpp/include/rune/client/Completion.h` | M | 03, 04 (`OpStatus` 13/14), 06, 07, 08 |
| `client-cpp/src/cli/QlEngine.cpp:133` (`errorCodeName`) | M | 02, 03, 08, 11 (59) |
| `broker/libs/view/include/rune/view/{ViewKey.h,RecordFold.h,Verdict.h}` | C | 04 (+07 `ViewKey`, 09 `RecordFold`/`Verdict`, 11 `PrepareStatus::Locked`) |
| `broker/libs/view/include/rune/view/{KeyedFold.h,CheckEval.h,UniqueClaims.h,TxnOverlay.h}` (+ their `src/*.cpp`) | C | 07 (`KeyedFold`), 09 (`CheckEval`, `UniqueClaims`), 11 (`TxnOverlay`) |
| `broker/libs/node/include/rune/node/VerdictSource.h` (+`src/VerdictSource.cpp`) | C | 04 (+05, 09, 11) |
| `broker/libs/storage/include/rune/storage/{ControlRecordLayout.h,RejectedOffsetIndex.h,VerdictCleanState.h}` | C | 04 (+06, 09, 11) |
| `broker/libs/storage/include/rune/storage/ProducerBatchWindow.h` | C | 06 |
| `broker/libs/storage/{include/rune/storage/LogCleaner.h,src/LogCleaner.cpp}`, `broker/libs/node/include/rune/node/LogCleanerAgent.h:455` | M | 04, 06, 11 |
| `broker/libs/replication/include/rune/replication/PartitionReplica.h:866,893` | M | 04, 06 (`producerTailDepth`), 11 (`cleanerTxnJudgeBound`) |
| `broker/libs/transaction/src/{ControlMarker.cpp,ReadCommittedView.cpp}` (type hardening) | M | 04 |
| `protocol/include/rune/protocol/CatalogTypes.h` (`TopicKeyFeatures`, `Partitioner`) | M | 05 |
| `codec/include/rune/catalog/{CatalogEvent.h,CatalogState.h}`, `codec/src/catalog/{CatalogEvent.cpp,CatalogFingerprint.cpp,ApplyV2.cpp(C)}`, `codec/tests/catalog/golden/*`, `codec/tools/make_catalog_golden.cpp` | M/C | 05 (+06 `ApplyV2`) |
| `protocol/include/rune/protocol/client/SchemaMessages.h` (DefineTopic / TopicSchemaResponse v12 trailer) | M | 02 (fingerprint kind), 05 |
| `client-cpp/include/rune/client/WriteResult.h` | C | 05 (+06, 09) |
| `client-cpp/include/rune/client/ConstraintNames.h`, `client-cpp/src/ConstraintNames.cpp` | C | 09 |
| `client-cpp/include/rune/client/subscriber/Subscription.h:117`, `src/subscriber/SubscriberHub.cpp` | M | 05 |
| `client-cpp/include/rune/client/codec/DataCodec.h` | M | 03 |
| `protocol/include/rune/protocol/PartitionHash.h`, `client-cpp/include/rune/client/TopicHandle.h`, `client-cpp/include/rune/client/metadata/PartitionRouter.h`, `broker/libs/gateway/include/rune/gateway/TopicRegistry.h` | M | 06 (`TopicHandle.h` also 05) |
| `protocol/include/rune/protocol/SecurityTypes.h`, `codec/src/security/*`, `broker/libs/node/include/rune/node/WorkerWriteGrants.h`, `PermissionShipper.h` | M | 06 |
| `broker/libs/view/{include/rune/view/ViewTable.h,src/ViewTable.cpp,include/rune/view/ViewCheckpoint.h,src/ViewCheckpoint.cpp,src/PlanCompiler.cpp,src/ViewPlan.cpp}` | M | 07 (+01 checkpoint marks, 08 `aggregateOutputType`, 09) |
| `codec/include/rune/codec/{ViewPlanAuthored.h,ViewKeyEncoding.h,UnicodeLower.h}`, `codec/src/{ViewPlanAuthored.cpp,UnicodeLower.cpp}`, `codec/tools/gen_unicode_lower.cpp`, `codec/data/UnicodeData-<ver>.txt` | C | 07 (+08 `ViewPlanAuthored`) |
| `client-cpp/include/rune/client/view/{ViewPlanAuthor.h,ViewKeyBuilder.h(C)}`, client read ops | M/C | 07 |
| `protocol/include/rune/protocol/client/DataMessages.h` (`GetViewRowK` 81, `ScanViewRows` 82/83) | M | 07 |
| `protocol/include/rune/protocol/client/StatsMessages.h` | C | 08 |
| `client-cpp/src/cli/{QlEngine.cpp,CatalogCommands.cpp}` (`select`, `show`, `stats --json`) | M | 08 (+09) |
| `broker/libs/node/{include/rune/node/TransactionCoordinationService.h,src/TransactionCoordinationService.cpp}` | M | 11 |
| `broker/docker/checked-writes-e2e.sh`, `docker-compose.checked-writes.yml`, `client-cpp/examples/checked_writes_probe.cpp` | C/M | 05 (+06, 09, 11) |
| `docs/api/{ql.md,wire-protocol.md,client-cpp.md,admin-cli.md}`, `docs/{operations.md,configuration.md,testing.md}`, `broker/docs/{CATALOG.md,SECURITY.md,SELF-HOSTING.md}`, root, `broker/` and `client-cpp/` `CLAUDE.md` | M | each part for what it ships; 11 cross-checks (§16.12) |
| The `rune-site` repo's views section (`rune/docs.html`, `rune/index.html`) | M | 11 Task 16: a local commit only; never pushed or deployed without the owner's go-ahead |
| Tests and support files | C/M | see below |

---

## Shared test infrastructure

**Binaries** (each part extends only the ones it names):

| Binary (target) | Location | Created by | Extended by |
|---|---|---|---|
| `rune_view_tests` | `broker/libs/view/tests/view/` | exists | 01, 04, 07, 08, 09, 11 |
| `rune_storage_tests` | `broker/libs/storage/tests/` | exists | 04 (index, layout, cleaner), 06, 09, 11 |
| `rune_protocol_tests` | `protocol/tests/` | exists | 02, 03, 05, 06, 07, 08, 10, 11 |
| `rune_replication_tests` | `broker/libs/replication/tests/` | exists | 11 (templates 13/14) |
| `rune_codec_tests`, `rune_catalog_tests` (`build/sub-codec`) | `codec/tests/` | exist | 02 (Utf8), 03, 05 (catalog v2, golden), 06 (security op), 07 (lower, key encoding, plan v2) |
| `rune_produce_validation_tests` | `broker/libs/node/tests/integration/ProduceValidationTest.cpp` | exists | 02, 03 |
| `rune_checked_writes_tests` | the `broker/libs/node/tests/node/CheckedWrites*Test.cpp` files each part lists (an explicit list, no glob) | 02 | 03–11: worker-level unit and integration, no Aeron |
| `rune_checked_writes_aborted_fold_tests` | `broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp` | 01 | — |
| `rune_checked_writes_ack_tests` | `broker/libs/node/tests/node/CheckedWrites{WorkerHook,ViewAck,ViewAckMatrix}Test.cpp` (Aeron block) | 03 | 04 |
| `rune_checked_writes_verdict_tests` | `broker/libs/node/tests/node/CheckedWrites{VerdictWorker,Consumer,VerdictAck}Test.cpp` and later rig tests (Aeron block, real shard `Worker`) | 04 | 05, 06, 09, 11 |
| `rune_checked_writes_matrix_tests` | `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp`, `CheckedWritesModelTest.cpp` | 04 | 03 (folded in by 04), 05, 06, 07, 09, 11 |
| `rune_checked_writes_stress_tests` | `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp` | 04 | 05, 06, 09, 11 |
| `rune_checked_writes_alloc_regression_tests` | `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` + `tests/support/AllocWindow.cpp` | 03 | 04, 05, 06, 07, 08, 09, 11 |
| `rune_checked_writes_transient_view_tests` | `broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp` (own enums, R10e) | 10 | — |
| `rune_client_broker_tests` | add `broker/libs/node/tests/client/CheckedWrites*RegressionTest.cpp` to `broker/libs/node/CMakeLists.txt:1731` | 02 | all parts: real `OneBroker` (`tests/client/OneBroker.h`) / `ThreeNodes` (`tests/support/CatalogBrokerCluster.h:160`) |
| `rune_create_view_tests`, `rune_ddl_conformance_tests`, `rune_ddl_stress_tests` | exist | — | 02, 05, 10 |
| `rune_txn_coordination_service_tests`, `rune_d9_cross_node_marker_tests` | exist (`broker/libs/node/CMakeLists.txt:475`, `:1272`) | — | 11 |
| client suites (`build/sub-client-cpp`), among them `rune_invoker_tests` and `rune_client_tests` | `client-cpp/tests/` | exist | 02, 03, 04, 05, 06, 07, 08, 09, 11 |
| ql suites (`build/sub-ql`) | `ql/tests/` | exist | 02, 06, 08 |

**Support headers** (`broker/libs/node/tests/support/`), following the Phase 2c `FilterMatrixRig.h`, `FilterMatrixExpectations.h` and `FilterModel.h` pattern:
- **`CheckedWritesRig.h` (04).** One real `Worker`, a disk `Log` or an `InMemoryLog` behind a `PartitionReplica`, an injected clock, and helpers. Helpers built by 04:
  - `appendBatch(records, acks, intent…)`, which returns the ack;
  - `driveUntilJudged(offset)`, with a hard pass cap;
  - `subscribeAndCollect()`;
  - `failover()`, `restart()`, `truncateTo(offset)`, `compact()`;
  - `installTestRejectView(pred)`.
  Parts add `createKeyIndex(KeyShape = {}, bool enabled = true)` (05), `createAutoIncTopic(AutoIncShape = {})` (06), `createView(planV2)` (07), `demote()` and `getStats(…)` (08), `createCharacterTopic()`, `createConstrainedView(…)`, `createLedgerTopic()` and `appendCharacters(…)` (09), and `sendVote`, `voteReply`, `driveUntilVoted`, `viewInt64` and a voting `commitTxn` (11).
  - Part 03's `CheckedWritesAckRig.h` (shard mode with the ack sink) serves 03's and some of 04's and 11's ack and visibility tests. Part 10 has its own `TransientViewRig.h`.
- **`CheckedWritesMatrixExpectations.h` (04).**
  - `enum class Op : uint16_t` with ranges reserved per part: 04 0–19, 05 20–39, 06 40–59, 07 60–79, 09 80–99, 11 100–119, 03 120–139 (R03d). Used today: 05 20–24, 06 40–45, 07 60–68, 09 80–86, 11 100–106.
  - `enum class Entity : uint16_t` with the same ranges. Used today: 05 20–25, 06 40–46, 07 60–68, 09 80–90, 11 100–107. Parts 01 and 10 keep their matrices in their own binaries (R10e).
  - `enum class Timing : uint16_t`, fixed here as §15's timing list: `Single, BackToBack, SelfCollidingBatch, Racing, RetryBeforeJudge, RetryAfterJudge, FailoverAppendHw, FailoverHwFold, FailoverFoldMarker, TruncateTail, RestartDuringRebuild, CreateOnNonEmpty, ReplaceDuringWrites, DropDuringWrites`. Append-only (R04d): 04 appends 03's six values and `RetryAfterWindow` (21 in all), and 11 appends ten more (`CoordinatorFailoverVotesOut` … `ConstraintReplacedWhileOpen`). Each part's `cellsFor` lists the timings it uses, so an appended value never changes an earlier part's pin.
  - `enum class Part : uint8_t { P03 = 3, P04 = 4, P05 = 5, P06 = 6, P07 = 7, P09 = 9, P11 = 11 };` and `std::vector<Cell> cellsFor(Part)` (04).
  - `struct Cell { Op op; Entity entity; Timing timing; };` and `Expected expect(const Cell&)` chain the per-part `expect04` … `expect11`, each with a top-down not-applicable table.
- **Cell counts** are pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned`. Each part updates its own row of the pin. The one exception: Part 11's Task 12 re-pins rows 05 (570 / 60) and 06 (810 / 72) when it lifts the pre-G refusals their not-applicable rules cite (R11d).
- **`CheckedWritesModel.h` (04).** The reference model: per-partition records with their verdicts, views as a fresh refold of the accepted records, and the consumer-visible set. It has extension points for key index (05), ids (06), unique and check (09) and transactions (11, judge order and the conflict rule). The model check runs 04's six named invariants plus each part's own (09 `AtMostCapLiveCharacters`, 11 `FoldPathsAgree`).
- **`CheckedWritesStressDriver.h` (04).** Seeded racing writers with injected failovers, restarts, truncation and compaction on `ThreeNodes`. It records the seed on failure. The invariants are named checks, one owner each:
  - 04: `AckedVisibleUnlessRejected`, `NoRejectedDeliveredOrFolded`, `SameVerdictEveryLeaderAndRefold`, `ViewEqualsFreshRefold`, `CompactionKeepsAcceptedValue`;
  - 06: `NoIdTwiceNoReuse`;
  - 09: `AtMostTwelveLiveCharacters`, `LowerNameUnique`;
  - 11: `TxnAllOrNothingAndAtomicPerPartition`.
- **Docker e2e.** `broker/docker/checked-writes-e2e.sh` and `docker-compose.checked-writes.yml` (VM only) are created by 05 and extended by 06, 09 (the RegressorsPath auth-server and character-server flows from spec §1) and 11 (the `TXN` leg). 01 and 10 extend the view-engine e2e (`view_probe` exit codes 170–179 and 180–189).
- **Benches** (`build/rel`). ABAB n=3 against the part's base commit:
  - `ProduceValidateBench.cpp`: `BM_CheckedWriteHook/*` (02, 03, 06).
  - `ProducePathBenchmark.cpp` and `WorkerProduceLatencyBench.cpp`: non-regression (02, 03), plus a consumer attached to a verdict source (04).
  - `ViewFoldBench.cpp`: `BM_ViewFoldVerdict/*` (04, 09, 11 `txn_judge_*`) and derived keys plus the owner map (07).
  - `WorkerProduceLatencyBench.cpp`: `BM_WorkerTxnCommit/voting` and `/non_voting` (11; `non_voting` is a no-regression gate).
  - `ViewTableBench.cpp`: `BM_ViewTableWideKey/*` and `BM_ViewTablePrefixScan` (07).
- **Names.** Every new test suite name starts with `CheckedWrites`, so `ctest -R CheckedWrites` selects the program. Every fix test carries `Regression` in its suite or name.

### Interaction matrix: §15 cells by owner

- **02:** the whole "Validation (§4)" block except transient views and `lower()`; "Reads": `partitions` omitted, binding both ways, key fields wider than 16 refused and `Key` truncation.
- **03:**
  - "Clients": intents without `View` or from a non-idempotent producer refused; autoincrement creates without `All` refused (the rule only; the ids are 06);
  - every `View`-flag bullet (`Leader|View`, `All|View`, behind an open transaction, no views, `NotLeader`/`Internal`, `View`/`None|View` refused, unknown bits, `BrokerTooOld`, old values 0/1 unchanged);
  - before Part G, a transactional and an atomic batch on a constrained source are refused while unconstrained ones work. Constrained sources are exercised by 04 with `TestReject` and by 09 for real (transactional half). The atomic half is a rule cell only (03's pinned row 15): the bit and its lift both ship in 11.
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
- **Parallel lanes** (R-ORD). At most 3 agents at once. The dependency order is 01, 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 11. Lane 1: 01 → 04 → 05 → 06 → 07 → 08 → 09 → 11. Lane 2: 02 → 03, which joins lane 1 before 04. Lane 3: 10, after 01 and 03 (R10c); 09 waits for 10. 01 and 02 may run concurrently; 10 may run beside 04–08.
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
- **Number collisions checked against the code** (the registry under C18 lists what the parts take):
  - `ErrorCode` stops at 50 (`Protocol.h:209`);
  - `TemplateId` 81–85 are free (`Protocol.h:90-102`);
  - `WorkerOp::Type` stops at 25, plus 200 (`WorkerOp.h:151-158`);
  - `TailKind` stops at 11 (`rune-internal-ring.xml`);
  - `ReturnType` stops at 7;
  - `controlToAdminRing` 26 is unused (`AdminChannels.h:48-82`);
  - cluster-plane template ids 13 and 14 are free;
  - `SecurityOperation` stops at 6 (`SecurityTypes.h:10`);
  - `SendResult` stops at 11 and `OpStatus` at 11;
  - control record types 0 and 1 are used (`ControlMarkerLayout.h:30-31`);
  - batch attribute bit 7 is unused (`RecordBatch.h:31-36`: bits 4–6 are taken).
- **R-X6 pass (after Part 11's plan):** C1, C6–C11, C13–C18, the registry, the binaries table, the support headers, the file map, the plan index and the rulings register were re-checked against plans 01–11.
