# Checked Writes Part 10 — Persisted Views over Transient Sources (Part F) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A persisted view may read a transient topic. `CreateView` and `AlterView` stop sending `ViewSourceTransient` (37). A view over a keyless topic stays refused by Part 02's rule (§4, P6: `ViewKeyInvalid` (35) "view source needs a key"), so this part adds no keyless refusal of its own. After a restart the view keeps its checkpointed rows and folds whatever the ring still holds. Every source reset and every eviction is counted, never silent (spec §9a, G7b).

**Architecture:** This is mostly a DDL change. The admin rule at `AdminWorker::viewDef_` (`broker/libs/node/src/AdminWorker.cpp:1230-1233`) changes from "transient source ⇒ transient view" to "transient source and persisted view ⇒ the source must be keyed". `viewDef_` is shared by create and alter, so one edit covers both. The worker side already works:
- A persisted view checkpoints to its own file, whatever its source's persistence.
- `WorkerViewSet::reconcileTransient_` (`broker/libs/node/src/WorkerViewSet.cpp:367-439`) re-folds the ring from its start when a recovered checkpoint cursor belongs to an earlier life of the source (`CursorRecoveredUnreconciled`).
- Re-folding is idempotent, because Replace rows and aggregate contributions are both keyed by the source's wire key.

The only worker change is in that function's two "invariant breach" warnings. Each becomes an INFO line and also increments `ViewFoldCounters::transientSourceResets`. A new view-set rig and a per-key model check every recovery and failover timing for a Replace view, a `count(*)` view and a `Min` view. Real brokers and the docker e2e then prove the full restart.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrices), CMake/Ninja, real Aeron with in-process `BrokerNode`s (`OneBroker` in shard mode, `ThreeNodes`), and docker compose on the shared VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`. This part implements §9a (Part F), the six §15 "persisted views over transient sources" bullets and G7b. The binding cross-part contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md`: index row 10, SD9, C16 stat 36, and the controller rulings R10a–R10e. The interfaces it consumes come from Part 01 (`broker/docs/superpowers/plans/2026-10-22-checked-writes-01-aborted-fold.md`): `FoldSkip`, the checkpoint marker (AD5), the AD5a recovery rule (which replaces the overview's SD15) and `foldCursorForTest`. Executors read all three.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-10-persisted-over-transient`. Cut it from the latest `main` (`git fetch && git switch -c feature/checked-writes-10-persisted-over-transient origin/main`) **after Parts 01 and 03 have merged** (ruling R10c; 03 implies 02). Merge `main` in again before review.
- **Preconditions.** Each must hit before Task 1; otherwise stop and tell the controller.
  - `grep -n "return Rejected(reject(kViewSourceTransient));" broker/libs/node/src/AdminWorker.cpp`: 1 hit, today line 1233.
  - `grep -c "invariant breach" broker/libs/node/src/WorkerViewSet.cpp`: 2.
  - `grep -n "foldCursorForTest" broker/libs/node/include/rune/node/WorkerViewSet.h`: 1 hit, from Part 01.
  - `grep -n "enum class CheckpointMark" broker/libs/view/include/rune/view/ViewCheckpoint.h`: 1 hit, from Part 01.
  - `grep -n "kAcksWireView" protocol/include/rune/protocol/client/DataMessages.h`: 1 hit, from Part 03.
  - `grep -n "featureRefusal" broker/libs/node/include/rune/node/TopicFeatureRules.h`: ≥ 1 hit, from Part 02.
  - `grep -n "kViewSourceTransient" broker/libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp`: 1 hit, line 442.
- **Line numbers** are cited against `main` before Parts 01–03. Those parts edit `WorkerViewSet.{h,cpp}`, `AdminWorker.cpp` and `Worker.h`, so locate each edit by the quoted text or by the function name.
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp`: examples on.
  - `build/sub-codec`.
  - `build/rel`: Release, `BUILD_BENCHMARKS=ON`.
  - `build/asan`, `build/tsan` and `build/client-tsan`.
- **Docker e2e** runs only on the shared VM (`ssh root@$RUNE_VM_HOST`), every command under `flock /root/vm.lock`, in your own directory. Never use local Mac docker.
- **Commits.** Use `git add <paths>` only: never `.superpowers/`, build logs or `-A`. Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

## Global Constraints

These are copied verbatim from the overview (Plan 00). Every task implicitly includes this section.

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

## Rulings this part depends on (restated from Plan 00 and Part 01, binding)

- **Index row 10.** "37 lifted; the keyless-source refusal; the `reconcileTransient_` logging change (`WorkerViewSet.cpp:367`); docs." Ruling R10c: the part runs after 01 **and** 03. R10b: "the keyless-source refusal" is Part 02's (below).
- **SD9 — Transient topics (ruling R10a).** `key.index`, intents and `autoincrement` are refused at DDL time on transient topics with `InvalidRequest`, "<feature> needs a persisted topic". Constraints over transient sources stay a non-goal (§2, §9a last bullet). The refusals are owned by Part 02's `featureRefusal` table (`broker/libs/node/include/rune/node/TopicFeatureRules.h`: `KeyIndex`, `AutoIncrement` and `Constraint` × transient). 05 calls it for `key.index` (intents need a key index, so they are refused with it), 06 for `autoincrement`, and 09 for `unique` / `check` over a transient source. This part lifts 37, adds none of those features, and only records the boundary in the docs (Task 7).
- **R10b — Keyless sources.** Spec §4 (Part 02, P6) refuses every view over a keyless typed topic with `ViewKeyInvalid` (35) "view source needs a key". That wins over §9a's "a transient view over a keyless transient source is unchanged" and over §9a's own keyless refusal (`InvalidRequest`), which can no longer be reached and is not built. This is spec wording the owner has to settle (Gaps).
- **SD13 / C16.** Stat 35 is `transientRecordsEvicted` and stat 36 is `transientSourceResets`. Both exist today (`broker/libs/node/include/rune/node/WorkerViewSet.h:137-138`), and Part 08 serves them over `GetStats`. Part 10 widens what 36 counts (PD3).
- **Part 01, AD5 and AD5a (replace SD15).**
  - Every checkpoint record written by a build with Part 01 carries the `…Marked` magic.
  - At recovery, a persisted instance replays its checkpoint unless it has an unmarked record (`U ≥ 0`) and an aborted range below `U`.
  - Every checkpoint of a persisted view over a transient source is written after Part 01, because 37 refused such views before. So `U = −1`, and AD5a always replays it. Task 2 pins that.
- **Part 01 signatures used here.**
  - `WorkerViewSet::driveFolds` and `driveRebuild` take `(…, const TransientBounds* transientSrc, uint64_t leaderGen, bool leaderHwConfirmed, const rune::view::FoldSkip& skip = rune::view::kNoSkip)`. `driveFolds` also takes a trailing `int64_t sourceLogStart = 0`.
  - `installViewForRecovery(plan, layout, evolvingSrc, persisted, partition, base, deltasInOrder, committedTail, const rune::view::FoldSkip& skip = rune::view::kNoSkip, int64_t sourceLogStart = 0)`.
  - `[[nodiscard]] int64_t foldCursorForTest(int32_t viewTopicId, int32_t partition) const noexcept;`

## Decisions (binding for executors; each is an obvious choice recorded so a reviewer can overturn it)

- **PD1 — Lifting 37; keyless sources left to Part 02.**
  - **What changes.** The 37 check in `viewDef_`'s per-source loop (`if (t->persistence == cat::Persistence::Transient && persistence == 0) return Rejected(reject(kViewSourceTransient));`) is deleted. Nothing replaces it. `buildCreateView_` and `buildAlterView_` both call `viewDef_`, so create and alter towards persisted are both lifted.
  - **Keyless sources (R10b).** Part 02's §4 refusal answers `ViewKeyInvalid` (35) "view source needs a key" for every view over a keyless typed topic, disk or transient, persisted or transient. This part adds no rule and must not change that order or message. Its tests pin that a keyless transient source still gets 35, never 37.
- **PD2 — Code 37 becomes reserved.**
  - **AdminWorker.** The file-local constant `kViewSourceTransient` (`AdminWorker.cpp:53`) is deleted. A one-line comment takes its place: `// 37 (ViewSourceTransient) is reserved and never sent: a persisted view may read a transient source (checked writes Part 10)`.
  - **Kept as is.** The protocol enum value `ErrorCode::ViewSourceTransient = 37` (`protocol/include/rune/protocol/client/Protocol.h:165`), its CLI name (`client-cpp/src/cli/QlEngine.cpp:142`) and its pin (`protocol/tests/protocol/client/ViewMessagesTest.cpp:198`) stay, because the enum is append-only. Only the comments change.
  - **Tests.** The test constant `kViewSourceTransient` (`broker/libs/node/tests/support/DdlExpectations.h:26`) stays. Tests use it to assert that 37 is never sent.
- **PD3 — Logging and counters (§9a "Recovery").** Two branches of `reconcileTransient_` change: `CursorRecoveredUnreconciled` (`WorkerViewSet.cpp:375-386`) and cursor above `logEnd` (`:426-433`).
  - Each WARN becomes `spdlog::info` and is preceded by `++inst.counters.transientSourceResets;`.
  - The cursor moves and the rows are kept exactly as today.
  - **Exact text, recovered branch:** `"WorkerViewSet: view {} partition {}: recovered checkpoint cursor {} is from an earlier life of its transient source (now [{}, {})); keeping the checkpoint rows, re-folding from offset {} (resets={})"`.
  - **Exact text, above-end branch:** `"WorkerViewSet: view {} partition {}: cursor {} is above its transient source end {} (the source restarted under it); keeping the rows, re-folding from offset {} (resets={})"`.
  - The words "invariant breach" leave the file, including the cursor-state comment at `WorkerViewSet.h:156-184`. That comment's `CursorRecoveredUnreconciled` line becomes "… the first reconcile logs once (INFO), counts a source reset, resets to logStart and keeps the rows", and its last line becomes "a source restart when above logEnd".
- **PD4 — Readiness is unchanged and needs no code change.** Reads must get `NotReady` while the ring is re-folded (§9a "Readiness").
  - **When the ring is longer than the old cursor.** The recovered cursor is below `committedTail`, so `installViewForRecovery` sets `RebuildingViews`.
  - **When the new ring is shorter.** The instance starts Ready, but the first `driveView_` pass sets `foldLeadership` (0 → the leader's generation) and moves the instance to `CatchingUp` (`WorkerViewSet.h:605-612`). That happens after the reconcile has moved the cursor back to `logStart` and before any read can be served, because reads run on the same worker thread between passes. The C1 serve-guard answers `NotReady` until the fold reaches the bound.
  - **The pin.** Task 2 pins this behaviour (`FullRestartCheckpointAheadOfTheNewRingServesNotReadyUntilRefoldedRegression`).
  - **Fallback, only if that test fails.** In the `CursorRecoveredUnreconciled` branch, set `inst.readiness = ViewReadiness::RebuildingViews` when the instance is Ready. Step 1b of `driveView_` raises it again once `foldCursor >= bound`.
- **PD5 — Lineage across a restart.** Checkpoints carry no source lineage, and plan and checkpoint formats stay unchanged (§9a "Changes"). So a recovered cursor is never compared with the new ring. Two cases follow:
  - **After a restart** (full cluster, or one node). Records evicted between the checkpoint and the ring's start are counted as one `transientSourceResets`, not as `transientRecordsEvicted`.
  - **On a failover where the node kept its instance in memory** (`CursorLive`, same incarnation). The gap is exact, and it is counted in `transientRecordsEvicted`.
  - Both are documented (Task 7). See Gaps for the §15 wording.
- **PD6 — A node that never hosted the instance.**
  - **Behaviour.** A new leader that never folded the view starts a fresh instance (`CursorFresh`) at its ring's start and counts nothing (`WorkerViewSet.h:158-161`). That is the existing rule.
  - **Consequence.** It applies equally to a persisted view, so after a failover or a full restart the rows come from *the new leader's* checkpoint plus its ring.
  - **Docs.** This is documented (Task 7).
- **PD7 — No hot-path change.**
  - The two changed branches run once per source reset.
  - The admin check is control plane.
  - No bench is rerun and no allocation gate is added (Task 9 records this).
- **PD8 — The `View` ack cell.** "The `View` ack on a transient source" uses `Acks::View` (C4, Part 03). Part 03 has merged (R10c), so Task 6 always runs.

## Review Focus

These five inputs are the ones most likely to bite a user, and the spec does not spell them out. Each is pinned by a named test in the task that owns the code.

1. **A full restart where the new ring already holds more records than the old checkpoint cursor.** The offsets are reused above `O`. The view must re-fold the ring from its start (never resume at `O`) and answer `NotReady` until the re-fold ends. Tests: `CheckedWritesTransientViewRegression.FullRestartCheckpointAheadOfTheNewRingServesNotReadyUntilRefoldedRegression` and `…FullRestartRingPastTheOldCursorFoldsEveryNewRecordRegression` (Task 2).
2. **A tombstone evicted while the node was behind.**
   - **Live instance.** The deleted row must stay visible (stale), `transientRecordsEvicted` must count exactly the gap, and the next write of the key must correct the row. Test: `…FailoverAfterAnEvictedTombstoneKeepsTheStaleRowAndCountsTheGapRegression` (Task 2).
   - **Restarted node.** On the same input the gap is one source reset. Test: `…NodeRestartAboveTheCheckpointCountsAResetNotAnEvictionRegression` (Task 2).
3. **Re-folding a ring whose records the checkpoint already holds** (a single-node restart in the same lineage), including a group move inside the re-folded range. `count(*)` must never double count, and `Min` must equal the minimum over the current contributors. Test: `…NodeRestartRefoldOfAGroupMoveNeverDoubleCountsRegression` (Task 2).
4. **A second restart.** The checkpoint file then holds records from two lives of the source, and the second life's O can be below the first's. Replay must take the last record's O, and the rows must equal the table before the restart. Test: `…SecondRestartReplaysBothLivesOfTheCheckpointRegression` (Task 2).
5. **Part 01's AD5a rule meeting a transient source.** The new ring holds an aborted transaction at offsets below the old cursor. The checkpoint, which is marked, must be kept, with `checkpointsDiscarded == 0`. Test: `…AbortedRangeInTheNewRingNeverDiscardsTheCheckpointRegression` (Task 2).

---

## Interaction matrix

Plan 00 assigns Part 10 the six §15 bullets "persisted views over transient sources". It reserves no `Op`/`Entity` id range for Part 10 in `CheckedWritesMatrixExpectations.h` (ruling R10e). No verdict view is involved, so this part's matrices live in their own binaries.

**§15 bullet → test:**

| §15 bullet | Tests (Task) |
|---|---|
| a full cluster restart keeps the checkpoint rows, and the cursor restarts at the new ring (Replace, `count(*)`, `Min`) | matrix timings `FullRestartEmptyRing`, `FullRestartRingBelowOldCursor`, `FullRestartRingPastOldCursor`, `SecondFullRestart` (Task 3); `CheckedWritesTransientViewBrokerRegression.FullRestartKeepsTheCheckpointRowsAndFoldsTheNewRingRegression` (Task 5); docker leg (Task 8) |
| a single-node restart with the ring re-replicated: a re-fold of already-folded records doesn't double count | matrix timing `NodeRestartReReplicatedRing` (Task 3); Review Focus 3 (Task 2) |
| failover to a node with an older checkpoint while the ring evicted part of the gap: stale rows stay, and `transientRecordsEvicted` counts the gap | matrix timings `FailoverLiveCursorEvicted`, `NodeRestartRingStartAboveCheckpoint` (Task 3); Review Focus 2 (Task 2); `CheckedWritesTransientViewFailoverBrokerRegression.LeaderCrashServesTheRowsFromTheNewLeadersRingRegression` (Task 5) |
| a keyless transient source is refused for a persisted view | refused for every view by Part 02 (R10b, code 35): DDL matrix (Task 1); `…BrokerRegression.ViewsOverAKeylessTransientTopicAreRefusedRegression` (Task 5) |
| the `View` ack on a transient source | `…BrokerRegression.ViewAckOnAPersistedViewOverATransientTopicMeansFoldedRegression` (Task 6) |
| code 37 is never sent | every DDL matrix cell (Task 1); the DDL and catalog stress models (Task 1); the docker leg (Task 8) |

**DDL matrix (`rune_ddl_conformance_tests`, Task 1):** source × op. There are 4 sources: `TransientKeyed`, `TransientKeyless`, `DiskKeyed` and `DiskKeyless`. There are 4 ops: `CreatePersisted`, `CreateTransient`, `AlterTransientToPersisted` and `AlterPersistedToTransient`. That gives 16 combinations. The four `Alter*` cells over a keyless source are not applicable, because no view over a keyless topic can be created (R10b). That leaves **12 cells**. The count is pinned by `CheckedWritesTransientViewDdlCountRegression.CellCountIsPinned`.
- **Expected answer.** `ViewKeyInvalid` (35) with detail "view source needs a key" for `{TransientKeyless, DiskKeyless} × {CreatePersisted, CreateTransient}` (Part 02's rule, R10b). `Ok` everywhere else.
- **Every cell asserts:**
  - the answer is never 37;
  - a refusal appends no event and leaves the catalog unchanged;
  - an accepted op appends exactly one event, and `views().byName(name)->persistence` is the requested one;
  - after `h.restart()` (boot replay) the same op on a fresh name gives the same answer.

**Fold and recovery matrix (`rune_checked_writes_transient_view_tests`, Task 3).** Entity × Op × Timing gives 3 × 4 × 9 = **108 cells**. The cell id is `CheckedWritesTransientViewMatrixRegression/Cell.ViewEqualsThePerKeyModel/<n>`, and the count is pinned by `CheckedWritesTransientViewMatrixCountRegression.CellCountIsPinned` (`EXPECT_EQ(cells().size(), 108u)`).

| Axis | Values |
|---|---|
| Entity (one persisted view, `ViewSetRig` Kind) | `Replace` (`{characterId, grp, hp}`); `CountByGrp` (key `grp`, `n = CountAll`); `MinByGrp` (key `grp`, `minHp = Min(hp)`, value-block table) |
| Op (the second history H2, 3 records, written after the timing event) | `Upserts` (new keys); `Overwrites` (H1's keys, new `hp`); `Tombstones` (deletes of H1's keys); `GroupMoves` (H1's keys to another `grp`) |
| Timing (between H1 and H2) | `FullRestartEmptyRing`; `FullRestartRingBelowOldCursor` (new ring `[0, 2)`, old O = 6); `FullRestartRingPastOldCursor` (new ring `[0, 9)`); `NodeRestartReReplicatedRing` (same lineage, ring `[0, 6)` retained); `NodeRestartRingStartAboveCheckpoint` (H1b at offsets 6–7 was never checkpointed here; ring now `[8, …)`); `FailoverLiveCursorRetained` (leaderGen 1 → 3, ring kept every record); `FailoverLiveCursorEvicted` (as before, with `evictTo(cursor + 2)`); `RingRecreatedLive` (incarnation 1 → 2, new content from offset 0); `SecondFullRestart` (`FullRestartEmptyRing`, then H2, then a second full restart, then H3 = one upsert) |

Each cell writes H1, which is 6 records over keys 1–4 and groups 10, 20 and 30, with one overwrite and one group move. It drives to caught up under leaderGen 1, applies the timing, writes H2, and drives to caught up under leaderGen 3. It then asserts:
- **(a) Rows.** Every view row equals the model. The model starts from the per-key map at the last checkpoint (restart timings) or the live map (failover timings). It then applies, in order, every record of the ring the instance read (`TransientViewModel::replay`).
- **(b) Counters.** `transientSourceResets` and `transientRecordsEvicted` equal the timing's column in the table below.
- **(c) No double count.** For `CountByGrp`, `count(g) == |{k in model : grp(k) == g}|` for every g. For `MinByGrp`, `minHp(g)` is the minimum over those keys. A group with no key is absent.
- **(d) Readiness.** With every read capped at one record, any key's `status` is `NotReady` after the first pass that follows the timing event. After caught up it is `Ok` or `NotFound`.
- **(e) Durability of what was folded after the event.** A further full restart with an empty ring recovers a table byte-equal to the final one.

| Timing | `transientSourceResets` | `transientRecordsEvicted` |
|---|---|---|
| `FullRestartEmptyRing`, `FullRestartRingBelowOldCursor`, `FullRestartRingPastOldCursor`, `NodeRestartReReplicatedRing`, `NodeRestartRingStartAboveCheckpoint`, `RingRecreatedLive` | 1 | 0 |
| `SecondFullRestart` | 1 (counters are per instance, and the instance is recreated) | 0 |
| `FailoverLiveCursorRetained` | 0 | 0 |
| `FailoverLiveCursorEvicted` | 0 | 2 |

**Model check (Task 3):** `CheckedWritesTransientViewModelRegression.RandomHistoriesMatchThePerKeyModel` runs seeds 1–200. Each seed is 40 random steps drawn from: write a batch of 1–3 records (upsert, overwrite, tombstone or group move over keys 1–8); full restart; node restart (same lineage, with 0–3 evicted); failover (0–3 evicted); ring recreated; ring cut below the cursor (generation + 1). All three views are installed together. Invariants (a), (c) and (e) are checked after every step, and the summed counters are checked against the model's count of resets and evictions. A failure prints the seed and the step list.

**Real brokers and e2e:**
- `rune_client_broker_tests` (Tasks 5 and 6): `OneBroker`, shard mode.
- `rune_client_failover_broker_tests` (Task 5): `ThreeNodes`.
- `broker/docker/view-engine-e2e.sh` (Task 8): two persisted views over `presence_t` across the broker restart.

## File Structure

| File | C/M | Responsibility |
|---|---|---|
| `broker/libs/node/src/AdminWorker.cpp:53`, `:1230-1233` | M | 37 lifted (PD1), PD2 constant (Task 1) |
| `broker/libs/node/include/rune/node/AdminWorker.h:65` | M | code list comment (Task 1) |
| `broker/libs/node/tests/integration/CreateViewRegressionTest.cpp:492-528` | M | the 37 test becomes the accepted and refused tests (Task 1) |
| `broker/libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp:436-484` | M | `kSourceCases` rows, detail check, the 12-cell DDL matrix (Task 1) |
| `broker/libs/node/tests/node/DdlStressRegressionTest.cpp:163`, `broker/libs/node/tests/support/CatalogModel.h:425-435` | M | the models drop the 37 rule (Task 1) |
| `broker/libs/node/src/WorkerViewSet.cpp:367-439`, `broker/libs/node/include/rune/node/WorkerViewSet.h:156-184` | M | PD3 (Task 2) |
| `broker/libs/node/tests/node/WorkerViewSetTest.cpp:1009-1043` | M | the above-end test renamed and its counter asserted (Task 2) |
| `broker/libs/node/tests/support/TransientViewRig.h` | C | `ViewSetRig`, `Kind`, `Rec`, `TransientViewModel` (Task 2) |
| `broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp` | C | named tests, the 108-cell matrix, the model check (Tasks 2, 3) |
| `broker/libs/node/CMakeLists.txt` | M | `rune_checked_writes_transient_view_tests` (Task 2); broker test sources (Task 5) |
| `broker/libs/node/tests/client/CheckedWritesTransientViewBrokerRegressionTest.cpp` | C | `OneBroker` tests (Tasks 5, 6) |
| `broker/libs/node/tests/client/CheckedWritesTransientViewFailoverBrokerRegressionTest.cpp` | C | `ThreeNodes` failover test (Task 5) |
| `client-cpp/examples/view_probe.cpp`, `broker/docker/view-engine-e2e.sh` | M | persisted-over-transient leg (Task 8) |
| `protocol/include/rune/protocol/client/Protocol.h:165-166`, `client-cpp/include/rune/client/Client.h:183-187`, `:927-930` | M | 37 comments (Task 7) |
| `broker/docs/SELF-HOSTING.md:335-338`, `:415-418`, `docs/api/ql.md:32`, `docs/api/client-cpp.md:493-494`, `docs/api/wire-protocol.md:250`, `broker/docs/CATALOG.md:123` | M | docs (Task 7) |

## Contract additions (none redefines a contract type)

- **Stat 36 (C16).** `transientSourceResets` also counts each reconcile of a checkpoint recovered over a transient source, and each cursor found above the ring's end (PD3).
- **Test binary** `rune_checked_writes_transient_view_tests` (`broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp`). It sits outside `tests/node/CheckedWrites*Test.cpp`, so it never joins Part 02's `rune_checked_writes_tests`.
- **Test support** `broker/libs/node/tests/support/TransientViewRig.h` (namespace `transient_view_rig`, test-only; Task 2 Interfaces).
- **`view_probe` exit codes 180–189** (setup 180–185, `verify_restart` 186–189) and the sentinels `PERSISTED-OVER-TRANSIENT: OK` and `PERSISTED-OVER-TRANSIENT-RESTART: OK` (Task 8). Part 01 holds 170–179.

---

## Phase 1 — The rule and the recovery semantics

### Task 1: Lift 37 (keyless sources stay Part 02's)

**Files:**
- Modify: `broker/libs/node/src/AdminWorker.cpp:53`, `:1230-1233`; `broker/libs/node/include/rune/node/AdminWorker.h:65`
- Modify (tests): `broker/libs/node/tests/integration/CreateViewRegressionTest.cpp:492-528`, `broker/libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp:436-484`, `broker/libs/node/tests/node/DdlStressRegressionTest.cpp:163`, `broker/libs/node/tests/support/CatalogModel.h:425-435`

**Interfaces:**
- Consumes: `AdminWorker::viewDef_` (`AdminWorker.cpp:1212`), Part 02's keyless refusal (35, "view source needs a key"), `DdlHarness::submit`, `submitRawDefine`, `submitRawView`, `submitRawAlterView`, `response(corr)->detail`, `views().byName`, `restart()` (`broker/libs/node/tests/support/DdlHarness.h`).
- Produces: the lifted rule (PD1). Every later task relies on "a persisted view over a keyed transient topic is accepted".

- [ ] **Step 1: Write the failing tests.**

  `CreateViewRegressionTest.cpp`. Replace `PersistedViewOverTransientSourceIsRejectedRegression` and its header comment (`:492-528`) with a fixture alias `class CheckedWritesTransientViewCreateRegression : public CreateViewRegression {};` and:

```cpp
TEST_F(CheckedWritesTransientViewCreateRegression, PersistedViewOverTransientSourceIsAcceptedRegression) {
    // defineCharacter() with persistence = 1 (transient, key "id"); CreateView persistence 0
    EXPECT_EQ(lastViewError_, 0u);
    EXPECT_GE(lastViewTopicId_, 0);
    ASSERT_NE(viewRegistry_->byName("char_hp"), nullptr);
    EXPECT_EQ(viewRegistry_->byName("char_hp")->persistence, 0u) << "persisted";
    EXPECT_EQ(lastSinkCleanupPolicy_, 1u) << "a compacted sink was built";
    EXPECT_EQ(events(), eventsBefore + 1);
    EXPECT_EQ(viewSet_.drainInstalls(), 1);
    restartAdmin_();
    ASSERT_NE(viewRegistry_->byName("char_hp"), nullptr) << "the view survives the boot replay";
    EXPECT_EQ(viewRegistry_->byName("char_hp")->persistence, 0u);
}
TEST_F(CheckedWritesTransientViewCreateRegression, PersistedViewOverKeylessTransientSourceIsStill35Regression) {
    // define "events_t": persistence 1, keyField "" (blob = characterBlob()); CreateView persisted over it
    EXPECT_EQ(lastViewError_, 35u) << "Part 02's keyless rule (R10b), never 37";
    EXPECT_EQ(lastViewTopicId_, -1);
    EXPECT_EQ(viewRegistry_->byName("ev_view"), nullptr);
    EXPECT_EQ(events(), eventsBefore) << "a refused view appends nothing";
    EXPECT_EQ(viewSet_.drainInstalls(), 0);
}
TEST_F(CheckedWritesTransientViewCreateRegression, TransientViewOverKeylessTransientSourceIsStill35Regression)
    // same source; CreateView persistence 1 -> lastViewError_ == 35, no event, no install (R10b)
```

  `DdlOccupantMatrixRegressionTest.cpp`:
  - `SourceCase` appends `const char* detail = nullptr;`.
  - The row `TransientSource_PersistedView` becomes `kOk`.
  - Rows are added for the sources `ktsrc` (transient, keyless) and `kdsrc` (disk, keyless), both created with `submitRawDefine` (keyField `""`, `charBlob()`, persistence 1 and 0) in the `ViewOverSource` setup:
    - `{"KeylessTransientSource_PersistedView", "ktsrc", Op::ViewPersisted, 35, "view source needs a key"}`
    - `{"KeylessTransientSource_TransientView", "ktsrc", Op::ViewTransient, 35, "view source needs a key"}`
    - `{"KeylessDiskSource_PersistedView", "kdsrc", Op::ViewPersisted, 35, "view source needs a key"}`
    - `{"KeylessDiskSource_TransientView", "kdsrc", Op::ViewTransient, 35, "view source needs a key"}`
    - If Part 02 already added keyless rows to `kSourceCases`, keep its rows and add only the missing ones.
  - `ViewOverSource` gains `if (c.detail) EXPECT_EQ(h.response(corr)->detail, c.detail);` and `EXPECT_NE(h.response(corr)->err, kViewSourceTransient) << "37 is never sent";`. Its after-restart re-check (`:474-480`) also runs for `c.want == 35`.

  Then add the 12-cell DDL matrix in the same file:

```cpp
enum class TvSource : uint8_t { TransientKeyed, TransientKeyless, DiskKeyed, DiskKeyless };
enum class TvOp : uint8_t { CreatePersisted, CreateTransient, AlterTransientToPersisted, AlterPersistedToTransient };
struct TvCell { TvSource source; TvOp op; };
std::vector<TvCell> tvCells();   // 4 x 4 minus the 4 Alter* cells over a keyless source
uint16_t tvExpected(const TvCell&);   // 35 (detail "view source needs a key") for a keyless source, else kOk
TEST(CheckedWritesTransientViewDdlCountRegression, CellCountIsPinned) { EXPECT_EQ(tvCells().size(), 12u); }
class CheckedWritesTransientViewDdlRegression : public ::testing::TestWithParam<TvCell> {};
TEST_P(CheckedWritesTransientViewDdlRegression, Cell) {
    // DdlHarness h; source by submitRawDefine (keyed: keyField "id"; keyless: ""); an Alter* op first creates the view
    // with the other persistence (submitView), then submitRawAlterView(AlterViewMsg{.persistence = target,
    // .expectedVersion = 1, planBlob = viewPlanBlob(name, {src}, target, /*extraColumn=*/true)});
    // assertions: err == tvExpected(cell) (and the detail for 35); err != 37; refused -> events unchanged and h.catalog() == before;
    // accepted -> one event and h.views().byName(name)->persistence == target;
    // h.restart(); the same op on name + "_2" -> the same err
}
```

  The models:
  - `DdlStressRegressionTest.cpp:163`: delete the line `if (o == Occupant::TypedTransient && s.persistedView) return kViewSourceTransient;`.
  - `CatalogModel.h:434`: delete the 37 line. Part 02's keyless rule (35) stays as Part 02 left it.
  - Update both doc comments (`DdlStressRegressionTest.cpp:152-155`, `CatalogModel.h:425-428`): drop the 37 clause and add "37 is never sent (checked writes Part 10)".

- [ ] **Step 2: Run them to verify they fail.**

  Run: `cmake --build build/root -j14 --target rune_create_view_tests rune_ddl_conformance_tests && ctest --test-dir build/root -R 'CheckedWritesTransientView|DdlViewSourceRegression' -j14 --output-on-failure`

  Expected: FAIL. `…IsAcceptedRegression` gets 37. `TransientSource_PersistedView` gets 37. The keyless cells already get 35 and pass (Part 02's rule runs first). The 12-cell matrix fails on every cell that expects `Ok` for a persisted view over a transient source.

- [ ] **Step 3: Implement PD1 and PD2 in `AdminWorker.cpp`.**
  - Delete the 37 check at `:1230-1233` per PD1.
  - Delete the constant at `:53` per PD2, leaving its comment.
  - Update the source-rule comment at `:1231` to "Checked writes Part 10 (spec §9a): any view may read a transient topic (a keyless source is refused above, §4)".
  - In `AdminWorker.h:65`, change `ViewSourceTransient 37` to `ViewSourceTransient 37 (reserved, never sent)`.

- [ ] **Step 4: Run the tests and the affected DDL suites.**

  Run: `cmake --build build/root -j14 --target rune_create_view_tests rune_ddl_conformance_tests rune_ddl_stress_tests rune_catalog_stress_tests && ctest --test-dir build/root -R 'CheckedWritesTransientView|CreateViewRegression|Ddl|CatalogStress' -j14 --output-on-failure`

  Expected: PASS. The stress seeds match the updated models.

- [ ] **Step 5: Commit.**

```bash
git add broker/libs/node/src/AdminWorker.cpp broker/libs/node/include/rune/node/AdminWorker.h \
        broker/libs/node/tests/integration/CreateViewRegressionTest.cpp \
        broker/libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp \
        broker/libs/node/tests/node/DdlStressRegressionTest.cpp broker/libs/node/tests/support/CatalogModel.h
git commit -m "admin: a persisted view may read a transient topic (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Recovery over a transient source: INFO plus a counted reset, and the named timings

**Files:**
- Modify: `broker/libs/node/src/WorkerViewSet.cpp:367-439`; `broker/libs/node/include/rune/node/WorkerViewSet.h:156-184` (comment)
- Modify: `broker/libs/node/tests/node/WorkerViewSetTest.cpp:1009-1043`
- Create: `broker/libs/node/tests/support/TransientViewRig.h`, `broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt` (new target, next to `rune_create_view_tests`, `:658-672`)

**Interfaces:**
- Consumes: the Part 01 signatures under Rulings; `TransientBounds` (`WorkerViewSet.h:148`); `splitViewCheckpointRecords` (`WorkerViewSet.h:108`); `rune::view::compileView`; `RecordBatchBuilder(baseOffset, baseTimestamp)`; `rune::transaction::AbortedTxnIndex::onAbortMarker(pid, firstDataOffset, abortMarkerOffset)` (`broker/libs/transaction/include/rune/transaction/ReadCommittedView.h:117-137`).
- Produces (`TransientViewRig.h`, namespace `transient_view_rig`, test-only):
  ```cpp
  enum class Kind : uint8_t { Replace, CountByGrp, MinByGrp };    // view ids 0x40000A00 + kind
  struct Rec { int64_t key; int64_t grp; int64_t hp; bool tombstone = false; };
  // source descriptor "character" v1: {characterId Int64 key (tag 0), grp Int64, hp Int64}; source topic id 5
  class ViewSetRig {
  public:
      explicit ViewSetRig(std::string tag);                       // own temp dir (per pid, as WorkerViewSetTest.cpp:57-75)
      void install(Kind kind, bool persisted = true);
      void append(std::span<const Rec> recs);                     // one single-record batch per Rec, at logEnd
      void evictTo(int64_t newLogStart);                          // the ring drops [logStart, newLogStart)
      void newRing(int64_t logStart, uint32_t incarnation);       // empty ring at logStart (a restart or a recreation)
      void cutAt(int64_t offset, std::span<const Rec> replacement);   // generation + 1, cutOffset = offset
      int  pass(uint64_t leaderGen, int64_t maxRecordsPerRead = INT64_MAX);   // driveRebuild + driveFolds, as Worker::driveViewsOf_
      [[nodiscard]] bool driveUntilCaughtUp(uint64_t leaderGen, int cap = 256);
      void restart(const rune::view::FoldSkip& skip = rune::view::kNoSkip, int64_t sourceLogStart = 0,
                   std::size_t dropLastDeltas = 0);               // new channel + WorkerViewSet; installViewForRecovery per persisted view
      [[nodiscard]] std::optional<int64_t> hp(int64_t key) const;
      [[nodiscard]] std::optional<int64_t> count(int64_t grp) const;
      [[nodiscard]] std::optional<int64_t> minHp(int64_t grp) const;
      [[nodiscard]] rune::node::WorkerViewSet::RowStatus status(Kind kind, int64_t key) const;
      [[nodiscard]] const rune::node::ViewFoldCounters& counters(Kind kind) const;
      [[nodiscard]] std::vector<std::vector<uint8_t>> tableImage(Kind kind) const;   // sorted rows, for invariant (e)
      [[nodiscard]] int64_t logStart() const; [[nodiscard]] int64_t logEnd() const;
      rune::node::WorkerViewSet& views();
  };
  struct TransientViewModel {                                     // the per-key map
      std::map<int64_t, Rec> rows;
      void apply(const Rec& r);                                   // upsert, or erase on a tombstone
      void replay(std::span<const Rec> ringReadInOrder);
      [[nodiscard]] std::optional<int64_t> hp(int64_t key) const;
      [[nodiscard]] std::optional<int64_t> count(int64_t grp) const;
      [[nodiscard]] std::optional<int64_t> minHp(int64_t grp) const;
  };
  ```
  - **Deltas.** After every pass the rig drains every pushed delta from the channel's `deltas` ring into a per-view file image (the IO agent's append). `restart` replays that image, minus its last `dropLastDeltas` records, against the ring as it stands at the call (tests set the post-restart ring with `newRing` / `append` first). It passes `committedTail` = that ring's end, and the given `skip` and `sourceLogStart`.
  - **The read range.** `readRange(from, to)` returns a `LogReadView` over the concatenated batches at offsets `[max(from, logStart), …)`, capped at `maxRecordsPerRead` batches.
  - **Bounds and identity.** `TransientBounds` carries `logStart`, `logEnd` and the ring's incarnation, generation and cut.
  - **Leadership.** `leaderHwConfirmed` is always true.

- [ ] **Step 1: Write the failing tests** in `PersistedOverTransientViewTest.cpp`, suite `CheckedWritesTransientViewRegression`. Each test installs persisted views, writes H1 = `{1,10,5} {2,10,7} {3,20,4} {1,20,6}` (key 1 moves 10 → 20), drives `driveUntilCaughtUp(1)`, and then:

```cpp
TEST(CheckedWritesTransientViewRegression, RecoveredCheckpointOverATransientSourceLogsInfoAndCountsAResetRegression) {
    // Replace; newRing(0, 2); restart(); append({{9,30,1}}); driveUntilCaughtUp(3)
    // ringbuffer sink at trace level (as WorkerViewSetTest.cpp:775): one INFO line containing
    // "is from an earlier life of its transient source"; no WARN line containing "invariant breach"
    EXPECT_EQ(rig.counters(Kind::Replace).transientSourceResets, 1u);
    EXPECT_EQ(rig.counters(Kind::Replace).transientRecordsEvicted, 0u);
    EXPECT_EQ(rig.hp(1), 6); EXPECT_EQ(rig.hp(2), 7); EXPECT_EQ(rig.hp(3), 4) << "checkpoint rows kept";
    EXPECT_EQ(rig.hp(9), 1) << "the new ring folds from its start";
}
TEST(CheckedWritesTransientViewRegression, FullRestartCheckpointAheadOfTheNewRingServesNotReadyUntilRefoldedRegression)
    // Replace; newRing(0, 2); append 3 records {7,..} {8,..} {9,..} (logEnd 3 < old O 4); restart();
    // pass(3, /*maxRecordsPerRead=*/1) -> status(Replace, 7) == NotReady and status(Replace, 1) == NotReady;
    // driveUntilCaughtUp(3) -> status(Replace, 9) == Ok, hp(1) == 6
TEST(CheckedWritesTransientViewRegression, FullRestartRingPastTheOldCursorFoldsEveryNewRecordRegression)
    // Replace; newRing(0, 2); append 9 records keys 11..19 (offsets 0..8, past old O 4); restart();
    // driveUntilCaughtUp(3) -> hp(11..19) all present (offsets 0..3 below O included); hp(1..3) kept
TEST(CheckedWritesTransientViewRegression, FailoverAfterAnEvictedTombstoneKeepsTheStaleRowAndCountsTheGapRegression)
    // Replace; no restart; while "another node leads" (no pass): append({{2,10,0,true}, {3,20,9}}) at offsets 4,5;
    // append({{4,30,1}}) at 6; evictTo(6); driveUntilCaughtUp(3)
    EXPECT_EQ(rig.hp(2), 7) << "the evicted tombstone never folded: the row stays (stale)";
    EXPECT_EQ(rig.hp(3), 4) << "the evicted overwrite never folded";
    EXPECT_EQ(rig.counters(Kind::Replace).transientRecordsEvicted, 2u);
    EXPECT_EQ(rig.counters(Kind::Replace).transientSourceResets, 0u);
    // append({{2,10,8}}); driveUntilCaughtUp(3) -> hp(2) == 8 (the next write corrects it)
TEST(CheckedWritesTransientViewRegression, NodeRestartAboveTheCheckpointCountsAResetNotAnEvictionRegression)
    // the same history, but a restart instead of the live failover: newRing(6, 1); restart(); append({{4,30,1}})
    // -> hp(2) == 7, hp(3) == 4, transientSourceResets == 1, transientRecordsEvicted == 0 (PD5)
TEST(CheckedWritesTransientViewRegression, NodeRestartRefoldOfAGroupMoveNeverDoubleCountsRegression)
    // CountByGrp + MinByGrp; the ring is the SAME lineage: newRing(0, 1) + append(H1) again (the re-replicated copy);
    // restart(); driveUntilCaughtUp(3)
    EXPECT_EQ(rig.count(10), 1); EXPECT_EQ(rig.count(20), 2);    // keys {2} and {1,3}; never 2 and 4
    EXPECT_EQ(rig.minHp(10), 7); EXPECT_EQ(rig.minHp(20), 4);
TEST(CheckedWritesTransientViewRegression, SecondRestartReplaysBothLivesOfTheCheckpointRegression)
    // all three kinds; newRing(0, 2); restart(); append({{5,30,2}}) (O of the second life = 1 < 4); caught up;
    // image = tableImage(each kind); newRing(0, 3); restart(); caught up -> tableImage(each kind) == image;
    // views().foldCursorForTest(view, 0) == 0 (reset to the empty ring's start)
TEST(CheckedWritesTransientViewRegression, AbortedRangeInTheNewRingNeverDiscardsTheCheckpointRegression)
    // Replace; AbortedTxnIndex idx; idx.onAbortMarker(/*pid*/ 7, 0, 1);
    // newRing(0, 2); restart(rune::view::FoldSkip{.aborted = &idx}, /*sourceLogStart*/ 0); caught up
    EXPECT_EQ(rig.counters(Kind::Replace).checkpointsDiscarded, 0u) << "a checkpoint over a transient source is marked";
    EXPECT_EQ(rig.hp(1), 6);
TEST(CheckedWritesTransientViewRegression, FullRestartLosesOnlyTheUncheckpointedTailRegression)
    // Replace; append({{6,30,3}}) in its own pass (one more delta); newRing(0, 2);
    // restart(kNoSkip, 0, /*dropLastDeltas*/ 1) -> hp(6) == nullopt (the tail is lost, §9a), hp(1) == 6 (the rest kept)
```

  In `WorkerViewSetTest.cpp`, rename `TransientSourceCursorAboveLogEndIsInvariantBreachRegression` to `TransientSourceCursorAboveLogEndRefoldsAndCountsAResetRegression`. Rewrite its header comment (`:1009-1013`): "the source restarted EMPTY and reuses offsets below a live cursor: rows kept, one INFO line, a counted reset". Add `EXPECT_EQ(vs.foldCounters(0x40000031, 0)->transientSourceResets, 1u);`.

  Add the CMake target:

```cmake
    # Checked writes Part 10 (spec §9a): persisted views over transient sources — view-set timings, matrix, model.
    add_executable(rune_checked_writes_transient_view_tests tests/integration/PersistedOverTransientViewTest.cpp)
    target_include_directories(rune_checked_writes_transient_view_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE}
                               ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_checked_writes_transient_view_tests PRIVATE rune_node rune_view rune_codec
                          rune_transaction GTest::gtest_main)
    gtest_discover_tests(rune_checked_writes_transient_view_tests PROPERTIES TIMEOUT 300 LABELS small)
```

- [ ] **Step 2: Run them to verify they fail.**

  Run: `cmake --build build/root -j14 --target rune_checked_writes_transient_view_tests rune_node_tests && ctest --test-dir build/root -R 'CheckedWritesTransientViewRegression|TransientSourceCursorAboveLogEnd' -j14 --output-on-failure`

  Expected: FAIL on every `transientSourceResets == 1` and on the INFO-line check. Every row, count and readiness assertion passes already (§9a: "Behaviour is unchanged"). If a row assertion fails, stop: that is a pre-existing bug, fixed in this task with its own `*Regression*` test.

- [ ] **Step 3: Implement PD3** in `reconcileTransient_` and the `WorkerViewSet.h` comment. If `…ServesNotReadyUntilRefoldedRegression` failed in Step 2, apply the PD4 fallback in the same branch.

- [ ] **Step 4: Run the tests and the view suites.**

  Run: `ctest --test-dir build/root -R 'CheckedWritesTransientView|WorkerViewSet|ViewRecovery|ViewReadiness|CheckedWritesAbortedFold' -j14 --output-on-failure`

  Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add broker/libs/node/src/WorkerViewSet.cpp broker/libs/node/include/rune/node/WorkerViewSet.h \
        broker/libs/node/tests/node/WorkerViewSetTest.cpp broker/libs/node/tests/support/TransientViewRig.h \
        broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "views: a reset of a transient source under a recovered checkpoint is INFO and counted (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: The 108-cell recovery matrix and the model check

**Files:**
- Modify: `broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp`, `broker/libs/node/tests/support/TransientViewRig.h` (cell table, H1 / H2 builders)

**Interfaces:**
- Consumes: `ViewSetRig`, `TransientViewModel`, `Kind`, `Rec` (Task 2).
- Produces (test-only, in the `.cpp`): `enum class TvEntity : uint8_t { Replace, CountByGrp, MinByGrp };`, `enum class TvHistoryOp : uint8_t { Upserts, Overwrites, Tombstones, GroupMoves };`, `enum class TvTiming : uint8_t { FullRestartEmptyRing, FullRestartRingBelowOldCursor, FullRestartRingPastOldCursor, NodeRestartReReplicatedRing, NodeRestartRingStartAboveCheckpoint, FailoverLiveCursorRetained, FailoverLiveCursorEvicted, RingRecreatedLive, SecondFullRestart };`, `struct TvFoldCell { TvEntity entity; TvHistoryOp op; TvTiming timing; };` and `std::vector<TvFoldCell> cells();`.

- [ ] **Step 1: Write the matrix and the model check.**
  - The matrix is `TEST_P(CheckedWritesTransientViewMatrixRegression, ViewEqualsThePerKeyModel)` over `cells()`, instantiated as `Cell` with the label `<Entity>_<Op>_<Timing>`. Each cell asserts invariants (a)–(e) and the counter table of the Interaction matrix section.
  - `TEST(CheckedWritesTransientViewMatrixCountRegression, CellCountIsPinned) { EXPECT_EQ(cells().size(), 108u); }`.
  - `TEST(CheckedWritesTransientViewModelRegression, RandomHistoriesMatchThePerKeyModel)` runs seeds 1–200 with `std::mt19937_64(seed)` and the step distribution from the Interaction matrix section, inside `SCOPED_TRACE("seed " + std::to_string(seed))`. It prints the step list on the first failure.
  - **How the model tracks what the instance read.** The model mirrors the rig per step:
    - On a restart or a recreation, it keeps the per-key map as of the last checkpointed pass, then replays the new ring from `logStart`.
    - On a live eviction, the evicted, unread records never reach the model.
    - On a cut, the model replays from `max(cut, logStart)` on top of the pre-cut map.

- [ ] **Step 2: Run them.**

  Run: `cmake --build build/root -j14 --target rune_checked_writes_transient_view_tests && ctest --test-dir build/root -R 'CheckedWritesTransientView(Matrix|Model)' -j14 --output-on-failure`

  Expected: PASS (108 cells + 1 pin + 1 model test).

  **Mutation check** (do it, then revert): make `reconcileTransient_`'s recovered branch keep `foldCursor` instead of moving it to `logStart`. The `FullRestartRingPastOldCursor` cells must fail on (a), and the model test must fail with a seed.

- [ ] **Step 3: Commit.**

```bash
git add broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp broker/libs/node/tests/support/TransientViewRig.h
git commit -m "test: persisted views over transient sources across every restart and failover timing (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Checkpoint A

The checkpoint commands below are referred to as **CP-full** from Task 9.

- [ ] **Step 1: Full regression suite.** Run `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure`. Expected: 100% pass.
- [ ] **Step 2: ASan+UBSan.** Run `cmake --build build/asan -j14 && ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/asan -R Regression -LE latency -j14 --output-on-failure`. On macOS x86_64 under Rosetta, use `detect_leaks=0` and then run `leaks(1)` on the `rune_checked_writes_transient_view_tests` and `rune_ddl_conformance_tests` binaries of `build/asan`. Expected: no report.
- [ ] **Step 3: TSan.** Run `cmake --build build/tsan -j14 && ctest --test-dir build/tsan -R Regression -LE latency -j14 --output-on-failure`, then the client: `cmake --build build/client-tsan -j14 && ctest --test-dir build/client-tsan -R Regression -LE latency -j14 --output-on-failure`. Expected: no report.
- [ ] **Step 4: CatalogGolden.** Run `cmake --build build/sub-codec -j14 && ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure`. Expected: pass, unchanged. No catalog format changes in this part.
- [ ] **Step 5: Failures.** Bisect a failure over the commits since the last green checkpoint and fix it with a `*Regression*` test. After the fix, rerun only the failed and affected tests.

---

## Phase 2 — Real brokers, e2e and docs

### Task 5: Real brokers: one broker in shard mode, and three nodes with a leader crash

**Files:**
- Create: `broker/libs/node/tests/client/CheckedWritesTransientViewBrokerRegressionTest.cpp` (add it to `rune_client_broker_tests`, `broker/libs/node/CMakeLists.txt:1731-1743`)
- Create: `broker/libs/node/tests/client/CheckedWritesTransientViewFailoverBrokerRegressionTest.cpp` (add it to `rune_client_failover_broker_tests`, `:1811-1813`; it kills brokers, so it stays in this `RUN_SERIAL` binary)

**Interfaces:**
- Consumes:
  - `OneBroker(tag, OneBroker::Runtime::Shard)`, `connectClient()`, `restart()` and `node().effectiveConfigForTest().dataDir` (`broker/libs/node/tests/client/OneBroker.h`, `BrokerNode.h:2005`);
  - `ThreeNodes::start`, `crash`, `awaitCatalogLeader`, `awaitController`, `leaderOn`, `connectClient` and `clientConfigFor` (`tests/support/CatalogBrokerCluster.h:160-400`, `tests/client/ClusterClient.h`);
  - `charBlob()` and `viewPlanBlob()` (`tests/support/DdlHarness.h:60-87`);
  - `Client::defineTopicAndWait`, `createViewAndWait`, `getViewRowAndWait` and `rune::client::view::readSlot`;
  - `splitViewCheckpointRecords`.
- Produces (file-local):
  - `std::vector<uint8_t> presenceTotalsBlob(const std::string& name, int32_t srcTopicId)`: an authored plan with key `"hp"`, source `{srcTopicId, "hp"}`, outputs `{hp Int64 Replace "hp"}`, `{n Int64 CountAll ""}` and `{minId Int64 Min "id"}`, persistence 0.
  - `std::vector<uint8_t> charValue(int64_t id, int64_t hp)`: `[16 u16][1 u16][id i64][hp i64]`.
  - `bool awaitCheckpointO(const std::filesystem::path& dataDir, int32_t viewTopicId, int64_t wantO, std::chrono::seconds bound)`: polls `<dataDir>/__view_snapshots/view-<uint32 id>-p0.delta` in 20 ms steps until the file's last record has O (bytes 4..11, LE) ≥ `wantO`.
- **Part 02 note.** Part 02 has merged (R10c), so `defineTopicAndWait` no longer takes `validate`. Use its signature as it stands on `main`.

- [ ] **Step 1: Write the tests.**

```cpp
TEST(CheckedWritesTransientViewBrokerRegression, PersistedViewsOverATransientTopicFoldAndServeRegression) {
    OneBroker b("tv_serve", OneBroker::Runtime::Shard);
    // define "presence" (1 partition, key "id", charBlob(), Persistence::Transient)
    // createViewAndWait("p_rows", "id", 0, viewPlanBlob("p_rows", src, 0)) -> errorCode 0 (never 37)
    // createViewAndWait("p_totals", "hp", 0, presenceTotalsBlob("p_totals", src)) -> errorCode 0
    // produce acks=All: (1,5) (2,5) (3,7)
    // getViewRowAndWait(p_rows, 3) -> kOk, hp 7 (bounded 30 s through kNotReady);
    // p_totals hp 5 -> n 2, minId 1; hp 7 -> n 1, minId 3
}
TEST(CheckedWritesTransientViewBrokerRegression, FullRestartKeepsTheCheckpointRowsAndFoldsTheNewRingRegression)
    // the setup and writes above; awaitCheckpointO(dataDir, p_rows, 3, 10s) and (p_totals, 3) -> true;
    // b.restart(); a fresh client; presence exists (define -> TopicExists 25, same id) and the first produce lands at 0
    // BEFORE that produce: p_rows 1 -> hp 5, 2 -> hp 5, 3 -> hp 7 (checkpoint rows kept, kOk after kNotReady);
    //   p_totals hp 5 -> n 2
    // produce (4,5) at offset 0 and (1,7) at offset 1 -> p_rows 4 -> hp 5, 1 -> hp 7;
    //   p_totals hp 5 -> n 2 (keys 2,4: key 1 moved out, nothing double counted), hp 7 -> n 2, minId 1
TEST(CheckedWritesTransientViewBrokerRegression, ViewsOverAKeylessTransientTopicAreRefusedRegression)
    // define "events_t" transient, keyField "", charBlob(); createViewAndWait(persisted) and (transient, another
    // name) -> errorCode 35, detail == "view source needs a key" (R10b; never 37);
    // getTopicSchemaAndWait(each name) -> UnknownTopic (7): no sink was created
```

```cpp
TEST(CheckedWritesTransientViewFailoverBrokerRegression, LeaderCrashServesTheRowsFromTheNewLeadersRingRegression)
    // ThreeNodes::start("tv_fo"); transient "presence" rf 3, key "id"; p_rows and p_totals persisted (codes 0)
    // through a client seeded on a non-leader: produce acks=All (1,5) (2,5) (3,7) (4,7) (5,9)
    // precondition: p_rows 5 -> hp 9 served; crash(source leader); await a new leader (leaderOn, bounded 60 s)
    // reads ride kNotLeader / kNotReady (bounded 60 s): p_rows 1..5 exact; p_totals hp 5 -> n 2, hp 7 -> n 2, hp 9 -> n 1
    //   (the new leader's fresh instance folds its own full ring: PD6)
    // produce (1,9) -> p_totals hp 5 -> n 1, hp 9 -> n 2 (a contribution moved, never added twice)
```

- [ ] **Step 2: Run them.**

  Run: `cmake --build build/root -j14 --target rune_client_broker_tests rune_client_failover_broker_tests && ctest --test-dir build/root -R 'CheckedWritesTransientView(Broker|FailoverBroker)' --output-on-failure`

  Expected: PASS. The failover binary runs serially.

  **Mutation check** (do it, then revert): restore the 37 check in `viewDef_`. All three `OneBroker` tests and the failover test must fail at `createViewAndWait`.

- [ ] **Step 3: Commit.**

```bash
git add broker/libs/node/tests/client/CheckedWritesTransientViewBrokerRegressionTest.cpp \
        broker/libs/node/tests/client/CheckedWritesTransientViewFailoverBrokerRegressionTest.cpp \
        broker/libs/node/CMakeLists.txt
git commit -m "test: persisted views over a transient topic on real brokers: serve, full restart, leader crash (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: The `View` ack on a persisted view over a transient source

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesTransientViewBrokerRegressionTest.cpp`

**Interfaces:**
- Consumes: `rune::client::Acks::All | Acks::View` and `has()` (C4); `Completion` (`OpStatus::Ok`).

- [ ] **Step 1: Write the test.**

```cpp
TEST(CheckedWritesTransientViewBrokerRegression, ViewAckOnAPersistedViewOverATransientTopicMeansFoldedRegression)
    // the "presence" setup (p_rows persisted); producer acks = All | View; send (8, 3) -> completion Ok;
    // getViewRowAndWait(p_rows, 8) ONCE, no retry loop -> kOk, hp 3 (the ack waited for the fold);
    // the same with acks = Leader | View on key 9 -> kOk at once
```

- [ ] **Step 2: Run it.**

  Run: `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesTransientViewBroker' --output-on-failure`

  Expected: PASS.

- [ ] **Step 3: Commit** with `git add broker/libs/node/tests/client/CheckedWritesTransientViewBrokerRegressionTest.cpp` and the message `test: the View ack on a persisted view over a transient topic means folded (Part F)`, ending with the trailer.

### Task 7: Docs

**Files:**
- Modify: `broker/docs/SELF-HOSTING.md:335-338` and `:415-418`; `docs/api/ql.md:32`; `docs/api/client-cpp.md:493-494`; `docs/api/wire-protocol.md:250`; `broker/docs/CATALOG.md:123`; `protocol/include/rune/protocol/client/Protocol.h:165-166`; `client-cpp/include/rune/client/Client.h:183-187` and `:927-930`

- [ ] **Step 1: `SELF-HOSTING.md`, "Transient topics".** Replace the bullet at `:335-338` with:

```markdown
- Any view may read a transient topic, persisted or transient. A persisted view over a transient topic needs a keyed
  topic, like every view: over a keyless one `Client::createView(...)` answers `ViewKeyInvalid` (35) "view source needs
  a key". Code 37 (`ViewSourceTransient`) is no longer sent.
- A persisted view over a transient topic is as durable as its checkpoint; the topic is not durable at all:
  - **After a full-cluster restart** the view keeps the rows of its last checkpoint and folds the empty ring from
    offset 0. Rows folded after the last checkpoint write are lost, with the records that made them.
  - **After one node restarts** it keeps its checkpoint rows and re-folds the ring it re-replicates. Re-folding a record
    it already holds changes nothing (rows and aggregate contributions are keyed by the record's key).
  - **After a failover** the rows come from the new leader's own checkpoint plus its ring. A node that never led the
    partition starts from its ring alone. A row whose later updates left the ring before that node folded them keeps
    its older value until its key is written again; that includes a deleted key, whose row can reappear.
  - None of this is silent: `transientSourceResets` counts every restart or reset of the ring under the view, and
    `transientRecordsEvicted` counts records a live view lost to eviction.
  - Reads answer `NotReady` while the ring is re-folded. The `View` ack means "folded", not "checkpointed".
  - Key indexes, `autoincrement` and view constraints need a persisted topic.
```

  At `:415`, the bullet "A transient view over a transient topic re-folds …" becomes "A view over a transient topic (persisted or transient) re-folds …". Its last sentence becomes: "Records evicted before a live view folded them are counted as `transientRecordsEvicted`; a restarted view counts one `transientSourceResets` instead."

- [ ] **Step 2: `docs/api/ql.md`.** After the bullet at `:32`, add: "- A persisted view may read a transient topic (a keyed one). It keeps its checkpointed rows across a restart and folds what the ring still holds; see `SELF-HOSTING.md`, Transient topics."

- [ ] **Step 3: `docs/api/client-cpp.md:493-494`.** Replace "a view over a transient topic must itself be transient (`ViewSourceTransient`)" with "a persisted view may read a transient topic; it keeps its checkpointed rows across a restart".

- [ ] **Step 4: The code table and the catalog.**
  - In `docs/api/wire-protocol.md:250`, the row becomes `| 37 | `ViewSourceTransient` | Reserved; no longer sent (a persisted view may read a transient source). |  |`.
  - In `broker/docs/CATALOG.md:123`, drop "a view over a transient topic, " from the first cell and "`ViewSourceTransient` 37, " from the second.

- [ ] **Step 5: The comments.**
  - `Protocol.h:165-166`: `// reserved, never sent (checked writes Part 10): a persisted view may read a transient source`.
  - `Client.h:183-187`: replace the 37 clause with "ViewSourceTransient=37 is no longer sent: a persisted view may read a transient topic".
  - `Client.h:927-930`: replace the example list's 37 clause the same way.

- [ ] **Step 6: Check.**

  Run: `git grep -n "must itself be transient\|must be transient" -- docs broker/docs/SELF-HOSTING.md broker/docs/CATALOG.md client-cpp/include protocol/include broker/libs/node/src`

  Expected: 0 hits. The release note `broker/docs/release-notes/phase2b-e-transient-topics.md` is history and stays.

  Then run `cmake --build build/root -j14 --target rune_protocol_tests && ctest --test-dir build/root -R ViewMessages --output-on-failure`. Expected: PASS (the 37 pin still holds).

- [ ] **Step 7: Commit.**

```bash
git add broker/docs/SELF-HOSTING.md docs/api/ql.md docs/api/client-cpp.md docs/api/wire-protocol.md broker/docs/CATALOG.md \
        protocol/include/rune/protocol/client/Protocol.h client-cpp/include/rune/client/Client.h
git commit -m "docs: persisted views over transient topics: durability, restarts, failover, counters (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Docker e2e leg

**Files:**
- Modify: `client-cpp/examples/view_probe.cpp`:
  - mode comment `:14-60`;
  - setup, after step 7's read_committed check (`:1345-1361`) and before the handoff (`:1363`);
  - `verify_restart`, after check (e) (`:1486-1510`) and before `VIEW-ENGINE-E2E: OK`.
- Modify: `broker/docker/view-engine-e2e.sh`:
  - header `:14-59`;
  - sentinel and handoff checks next to `:320-340`;
  - the pre-restart wait before `:482`;
  - `CATALOG_NAMES` at `:450`;
  - the `verify_restart` env and the sentinel check.

**Interfaces:**
- Consumes: the probe's `charHpPlan`, `waitForHp`, `waitForAbsent`, `waitResolved`, `produceOne` and the `presence_t` handle `tt` (`view_probe.cpp:210-416`, `:620-661`).
- Produces (probe, file-local):
  - `static rcv::ViewPlan presenceTotalsPlan(int32_t srcTopicId)`: name `presence_n`, key `"hp"`, outputs `{hp Replace "hp"}`, `{n CountAll}` and `{minId Min "id"}`, persistence 0;
  - `static rcv::RowLayout presenceTotalsRowLayout()`: Int64 × 3;
  - `static bool waitForPresenceTotals(Client&, int32_t viewId, uint64_t fp, int64_t hp, int64_t wantN, int64_t wantMinId, int boundMs, int timeoutMs, const char* what)`;
  - the handoff lines `VIEW_TOPIC_ID_PRESENCE_PV`, `VIEW_FP_PRESENCE_PV`, `VIEW_TOPIC_ID_PRESENCE_N` and `VIEW_FP_PRESENCE_N`.
- **Exit codes.** First confirm they are unused: `grep -n "return 18[0-9];" client-cpp/examples/view_probe.cpp` gives 0 hits.

- [ ] **Step 1: Setup leg "7b. Part F: persisted views over presence_t".**
  - `createViewAndWait("presence_pv", "id", 0, charHpPlan("presence_pv", tt.topicId, 0))` must return `errorCode == 0` (180; the message names 37 if it comes back).
  - `createViewAndWait("presence_n", "hp", 0, presenceTotalsPlan(tt.topicId))` must return `errorCode == 0` (181).
  - `waitForHp(presence_pv, 30, 30, 1)` (182).
  - `waitForAbsent(presence_pv, 20)`: the aborted transaction never folds (183).
  - `waitForPresenceTotals(presence_n, hp 1, n 7, minId 1)`: the 7 committed keys 1, 2, 3, 4, 10, 11 and 30 (184).
  - Print the four handoff lines and `PERSISTED-OVER-TRANSIENT: OK` before `VIEW-PROBE-SETUP: OK`.

- [ ] **Step 2: `verify_restart` checks, after (e).** Check (e) has already produced key 9 at offset 0 of the empty ring.
  - `waitForHp(presence_pv, 30, 30, 1)`: a checkpoint row kept, with no source record left (186).
  - `waitForHp(presence_pv, 9, 9, 1)`: the new ring is folded (187).
  - `waitForAbsent(presence_pv, 20)` (188).
  - `waitForPresenceTotals(presence_n, hp 1, n 8, minId 1)`: the 7 kept keys plus key 9; never more (189).
  - Print `PERSISTED-OVER-TRANSIENT-RESTART: OK` before `VIEW-ENGINE-E2E: OK`.

- [ ] **Step 3: The script.**
  - Extract the four handoff values after the `VIEW_TT_TOPIC_ID` extract.
  - Pass them to `verify_restart` as `RUNE_VIEW_TOPIC_ID_PRESENCE_PV`, `RUNE_VIEW_FP_PRESENCE_PV`, `RUNE_VIEW_TOPIC_ID_PRESENCE_N` and `RUNE_VIEW_FP_PRESENCE_N`.
  - `grep -q "PERSISTED-OVER-TRANSIENT: OK"` on `setup_out`. On failure: `fail "setup client did not print the 'PERSISTED-OVER-TRANSIENT: OK' sentinel"`.
  - `grep -q "PERSISTED-OVER-TRANSIENT-RESTART: OK"` on the `verify_restart` output.
  - Append `presence_pv,presence_n` to `CATALOG_NAMES`.
  - Before Step 5's restart, wait for both checkpoint files with a bounded poll (40 × 250 ms): `docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" exec -T rune-1 test -s "/var/lib/rune/__view_snapshots/view-${id}-p0.delta"`. On failure: `fail "presence_pv/presence_n checkpoint file never written"`.
  - Add both legs to the header comment.

- [ ] **Step 4: Build the probe locally.**

  Run: `cmake --build build/sub-client-cpp -j14 --target view_probe`

  Expected: builds clean.

- [ ] **Step 5: Commit.**

```bash
git add client-cpp/examples/view_probe.cpp broker/docker/view-engine-e2e.sh
git commit -m "e2e: persisted views over a transient topic keep their rows across the broker restart (Part F)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Checkpoint B (before the PR) and Finish

- [ ] **Step 1: CP-full.** Merge the latest `main` (`git fetch && git merge origin/main`), rebuild, and run **CP-full** (Task 4, Steps 1–5). Expected: all green.
- [ ] **Step 2: Real broker slice.** Run `ctest --test-dir build/root -R 'CheckedWritesTransientView' --output-on-failure`. This covers every layer, and the failover binary runs serially. Expected: PASS.
- [ ] **Step 3: Benches.** None (PD7). The part changes no produce, consume, fold or query hot-path code. The two changed `reconcileTransient_` branches run once per source reset, and the admin rule is control plane. Record "no bench: no hot-path change" in the PR body.
- [ ] **Step 4: Latency gates** on an idle host. Run `ctest --test-dir build/rel -L latency --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Docker e2e on the VM.** In your own directory on `$RUNE_VM_HOST`, run `flock /root/vm.lock bash broker/docker/view-engine-e2e.sh`. Check `df` before and after, and remove only your own containers and images. Expected: `PERSISTED-OVER-TRANSIENT: OK`, `PERSISTED-OVER-TRANSIENT-RESTART: OK` and `VIEW-ENGINE-E2E: OK`, exit 0.
- [ ] **Step 6: File the deferred items** as labelled issues on `arcanumruna/rune`. Dedupe with `gh issue list --search` first, and give one summary at the end.
  1. `enhancement`, `sev:low`, `area:views`. A restarted node cannot tell whether its checkpoint cursor is in the current lineage of its transient source. So records evicted between its checkpoint and its ring's start are counted as one `transientSourceResets`, not as `transientRecordsEvicted` (PD5). A fix needs the source lineage (the committed `TransientLineageRecord`) stored with the checkpoint: a format change for Part 07's v3 or later.
     - Test reference: `CheckedWritesTransientViewRegression.NodeRestartAboveTheCheckpointCountsAResetNotAnEvictionRegression` (`broker/libs/node/tests/integration/PersistedOverTransientViewTest.cpp`).
     - Origin: Part 10 plan, PD5.
- [ ] **Step 7: Open the PR.** Open it from `feature/checked-writes-10-persisted-over-transient` to `main`, only once the owner has agreed to pushes for this program; never push otherwise.
  - **The body:** a summary, the §15 bullet → test table, the e2e output lines, "no bench: no hot-path change", and the deviations list (empty if none).
  - **Not a deviation.** The changed expectation in `DdlViewSourceRegression/TransientSource_PersistedView` (37 → Ok) is mandated by spec §9a. It is listed under "changed by spec", not as a weakened test.
  - **The trailer:** `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
  - **Review.** The reviewer (the most capable model) reviews the views recovery area and the DDL rule, then approves and merges.

---

## Gaps (spec and contract; for the controller)

- **SD9** is owned by Part 02's `featureRefusal` table, called by 05, 06 and 09 (ruling R10a). No gap remains.
- **§4 vs §9a on keyless topics (owner to settle).** Ruling R10b: §4's refusal (35, "view source needs a key") wins over §9a's "a transient view over a keyless transient source is unchanged" and §9a's `InvalidRequest` keyless refusal. Part 10 builds neither §9a rule. The owner has to settle the spec wording. If the owner keeps §9a, the `Keyless*` DDL rows, `…IsStill35Regression` and `ViewsOverAKeylessTransientTopicAreRefusedRegression` change with it.
- **§15 "failover to a node with an older checkpoint … `transientRecordsEvicted` counts the gap".** This holds when the node kept its instance in memory. After a restart the gap is one `transientSourceResets` (PD5), because checkpoints carry no lineage and §9a freezes the formats. The deferred issue in Task 9 tracks it.
  Accepted by ruling R10d; filed as the deferred issue in Task 9.
- **No matrix id range.** `CheckedWritesMatrixExpectations.h` reserves no `Op`/`Entity` id range for Part 10. Part 10's matrices use their own enums in their own binary, with no verdict views. Accepted by ruling R10e.

## Self-review (done while writing)

- **Spec coverage.**
  - §9a "Rule" (lift, every combination allowed, 37 reserved): Task 1 and the DDL matrix.
  - "Meaning" and the "Cases" table: Tasks 2, 3 and 5.
  - "What already works" (checkpoint file, cursor machine, idempotent re-fold): pinned by Tasks 2 and 3.
  - "Consequences to document": Task 7.
  - "Changes": Admin → Task 1; Recovery → Task 2 (PD3); keyed sources only → Part 02's §4 rule (R10b), pinned by Task 1; Readiness → Task 2 (PD4); `View` ack → Task 6; constraints stay a non-goal → Part 02's `featureRefusal` (R10a) and the doc line in Task 7.
  - G7b → Tasks 2, 3, 5 and 8.
  - The six §15 bullets → the Interaction matrix table.
  - Index row 10 deliverables → Tasks 1, 2 and 7.
- **Step scan.**
  - Every code step names the file, the anchor text and the exact strings (PD2, PD3); no function body is given.
  - Every test step names its suite and test and carries the values.
  - The only conditional step is PD4's fallback, decided by a named test.
- **Type consistency.** These names are spelled the same in every task:
  - `ViewSetRig`, `TransientViewModel`, `Kind::{Replace, CountByGrp, MinByGrp}`, `Rec`;
  - `TvFoldCell`, `TvTiming`, `TvCell`, `TvSource`, `TvOp`;
  - `rune_checked_writes_transient_view_tests`;
  - `presenceTotalsBlob` (broker tests) and `presenceTotalsPlan` (probe), which are different files on purpose;
  - `awaitCheckpointO`.
- **Review Focus.** Five lines, each with its test in Task 2.
- **Proportion.** About 800 lines (the spec section is about 35 lines, the whole spec about 1000), for a part whose code change is two branches and one admin check. Most of the length is the recovery matrix, the docs copy and the real-broker and e2e legs that §9a and §15 require. The code blocks are test assertions, one CMake block and the docs copy that the spec fixes.
