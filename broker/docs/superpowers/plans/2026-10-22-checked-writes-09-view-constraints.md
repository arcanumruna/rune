# Checked Writes — Part 09: View Constraints (`unique` and `check`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `unique` views and `check` predicates are enforced when the views fold each record, through the verdict pipeline of Part 04: the RegressorsPath target block of spec §1 (`user_identity`, `character_name`, `account_stats` / `max_characters`) works end to end, including pre-existing duplicates, constraint create / replace / drop under writes and failover, and `Client::constraintName`.

**Architecture:**
- **View engine.** A predicate evaluator runs a compiled `check` tree on a prospective row image, with SQL three-valued logic over the row-slot encoding of Part 08's constants. A unique keyed projection carries `UniqueClaims`: a per-key claim index and the pre-existing extra claimants, so the smallest source key owns a key and the next one is promoted when the owner leaves (§8.3).
- **`VerdictSource` members.** `VerdictViewKind::Unique` and `::Check` plug into Part 04's `switch (kind)`: prepare with the view's own fold, evaluate unique (ordinal 0) then the checks (ordinals 1..n), apply all or none. Tombstones are never rejected.
- **Constraint changes (C12).** A create joins at once (Part 04's joining member). A replace pre-builds the new plan as a candidate and swaps at `S` once it has caught up. A drop leaves at `D`, and its plan stays installed until `W > D` (two-phase drop, R04a). The plan of every Unique/Check member is also written into the source log as a `kVerdictPlan` control record, so a replica that never installed it (restart, lagging catalog) still judges with it and nothing waits forever.
- **Surface.** The AdminWorker accepts `unique` / `check` (R08e), refuses them over transient sources (R10a) and past the SD5 limits. Stat 32 `uniqueConflicts` is served (R08f). `Client::constraintName` resolves ids from the catalog, including dropped and replaced views.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices, seeded model checks), Google Benchmark (`build/rel/broker/rune_broker_benchmark`), CMake/Ninja, real Aeron for the worker rig, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`), docker on the shared VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` — §8.2, §8.3, §8.7 (the wiring; Part 04 owns the mechanism), the §1 target block, §11.2 (requirements per write), §13 (`uniqueConflicts`) and the §15 cells the overview gives Part 09. The binding cross-part contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C6, C9, C10, C12, C14, C15, C16, SD5, SD9). This part consumes the Interfaces and "Contract additions" of plans 01–08 (`2026-10-22-checked-writes-0{1..8}-*.md`) and the controller rulings restated below. Executors read the spec sections, the overview, plans 04, 05, 07 and 08, and this file.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-09-view-constraints`, cut from the latest `main` (`git fetch` first) **after Part 08 has merged** (and with it 01–07). Merge `main` in again before review.
- **Preconditions** (run before Task 1; each must match, else stop and tell the controller):
  - `grep -n "Unique = 2" broker/libs/view/include/rune/view/Verdict.h` (1 hit, Part 04)
  - `grep -n "kKeyIndexPlanFingerprint" broker/libs/node/include/rune/node/VerdictSource.h` (1 hit, Part 05)
  - `grep -n "autoIncReady" broker/libs/node/include/rune/node/CheckedWriteHook.h` (≥ 1 hit, Part 06)
  - `grep -n "struct CompiledConstraint" broker/libs/view/include/rune/view/ViewPlan.h` (1 hit, Part 07)
  - `grep -n "kKeyedTableGroupIds = 5" broker/libs/view/include/rune/view/ViewCheckpoint.h` (1 hit, Part 07)
  - `grep -n "unique views are not available yet\|check constraints are not available yet" broker/libs/node/src/AdminWorker.cpp` (2 hits, Part 07 P7-9)
  - `grep -n "collectViewPartitionStats" broker/libs/node/include/rune/node/StatsCollect.h` (1 hit, Part 08)
  - `grep -rln "BeforePart09" broker/libs/node/tests` (≥ 2 files, Part 08 P8-1)
  - `test -f ql/tests/golden/regressorspath.rql` (Part 08)
  - `grep -n "Constraint" broker/libs/node/include/rune/node/TopicFeatureRules.h` (≥ 1 hit, Part 02)
- **Builds.** Already configured; a missing one means stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp` (client suites), `build/sub-codec` (CatalogGolden), `build/sub-ql`, `build/rel` (Release, `BUILD_BENCHMARKS=ON`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Line numbers** cite `main` before Parts 01–08. Those parts edit `AdminWorker.cpp`, `WorkerViewSet.*`, `ViewFold.cpp`, `AggFold.cpp`, `PlanCompiler.cpp` and `Worker.h` heavily, so anchor on the named function, member or test, not on the number.
- **Benches and VM.** Benches run in `build/rel`; borderline numbers are confirmed on the shared VM (`$RUNE_VM_HOST`, every command under `flock /root/vm.lock`, prepare the clean box once and restore it, `broker/bench/BENCHMARKING.md`). Docker e2e runs only there. Never local Mac docker.
- **Commits.** `git add <paths>` only (never `-A`, `.superpowers/` or build logs). Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

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

## Rulings this part depends on (restated; binding)

- **R04a — Two-phase drop.** A dropping verdict view's plan stays installed, and keeps judging, on every replica until W passes the drop's `ConstraintSetChange` record. Only then is it uninstalled. This part owns the test: a failover during the drop finishes the drop and nothing waits forever (P9-7, P9-8, Task 6).
- **R04e / R04g — Duplicate verdict replay.** An in-window `View` retry on a verdict source replays the original verdicts once `W > lastOffset` (Part 04). This part adds the constraint names to that replay: the replayed `constraintId`s resolve through `Client::constraintName` to the original `(view, constraint)`, also after the view was replaced or dropped (P9-11). Outside the window: `DuplicateSequence` (21) → `OpStatus::VerdictUnknown` (14), or `StoredIdUnknown` (13) for a key-0 batch (06 P6-10).
- **R08e.** Part 08 parses and authors `unique` / `check`. The refusal lines in the AdminWorker (58 "unique views are not available yet", 57 "check constraints are not available yet", 07 P7-9) are deleted here, and every `…BeforePart09…` test is inverted and renamed (Tasks 3, 10).
- **R08c / P8-5.** `PredicateNode::constBytes` holds the constant in the compared column's row-slot encoding, zero-padded to 16 B. The evaluator decodes it with `rune::view::decodeSlotValue` (`broker/libs/view/include/rune/view/ViewRow.h:41`) at the column's slot width.
- **R08f.** Stat id 32 `uniqueConflicts` is added through `StatsCollect.h` (`collectViewPartitionStats`).
- **R10a / SD9 / R02e.** `unique` / `check` over a transient source are refused through Part 02's `featureRefusal(DbFeature::Constraint, shape)` (`broker/libs/node/include/rune/node/TopicFeatureRules.h`): `InvalidRequest` (39), "constraints need a persisted topic".
- **SD5.** `constraintId = viewObjectId << 16 | ordinal`. A constrained view whose `objectId ≥ 65536`, or with more than 65535 ordinals, is refused with `ViewConstraintInvalid` (57), "constraint ids exhausted".
- **R07c.** A unique view over `lower(name)` and a check on a `count(*) group by` view must both work, and so must a check over a composite `group by` (07 Task 11's keyed aggregates).
- **P4-5 / P4-6 (04).** Set order is ascending `viewObjectId`; the key index (id 0) is first; a record's verdict is the first failing member's; every member is prepared before any is evaluated. A member named by the logged set without an installed template makes the judge **wait**. A joining member builds unjudged from `logStart` up to `A + 1`.
- **P5-12 / §11.2.** Requirements are per write. Plain upserts and deletes from any producer, with any acks and any client version, are judged on a constrained source; a rejected one is simply never visible.
- **P7-9 (07).** At the engine level a unique view's conflicting record was skipped and counted in `FoldStats::keyConflicts`. This part replaces that with the §8.3 rule (Task 2).
- **P8-1 / C14.** QL authors `unique = true` and `constraints`; Part 07 compiles them structurally (`CompiledConstraint { ordinal, name, predicate }`, ordinals 1..n in declaration order); this part evaluates them.

## Part rulings (P9; binding for executors, recorded so the reviewer can overturn any)

- **P9-1 — Member kinds and fingerprints.**
  - `verdictKindOf(plan)`: `plan.unique` → `VerdictViewKind::Unique` (it judges ordinal 0, then its checks 1..n); else a non-empty `plan.constraints` → `VerdictViewKind::Check`; else none (an ordinary view).
  - A member's `planFingerprint` is `verdictPlanFingerprint(compiledPlanBlob)`: FNV-1a 64 (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`) over the catalog's `compiledPlanBlob` bytes. The blob includes `viewTopicId`, so a `create or replace` (a fresh sink, `AdminWorker.cpp:1292`) always gets a new fingerprint under the same `viewObjectId`, and every node computes the same value.
- **P9-2 — The verdict of a member.**
  - A tombstone is never rejected by a Unique or Check member (§8.2). It is still prepared and applied. (`removeExisting` of an absent key stays the key index's 53.)
  - Otherwise: unique (ordinal 0) first, then each check in ordinal order; the first failure is `{ConstraintViolation, constraintIdOf(viewObjectId, ordinal)}`.
- **P9-3 — The unique rule (§8.2, §8.3).** On a unique, owner-mapped view (07 `ownerMappedOf`), every live source record that derives a view key N is a **claimant** of N. The **owner** of N is the claimant with the smallest source key; the others are **extras** (`uniqueConflicts`).
  - **Smallest** compares the source wire keys in the order of their values: each is encoded with `rune::codec::encodeKeyComponent` at the source key's type (big-endian, sign bit flipped for signed types; `string(N)` as is) and the results are `memcmp`ed. So `int64 −5 < 3`, and `uint64 0xFF < 0x100` even though their little-endian wire bytes compare the other way.
  - **Judged records** (at or above the member's activation offset, above K): a live record whose N is owned by a different source key is rejected with ordinal 0. An **extra** claimant that writes a record keeping its own N (an update of a non-key column) is **accepted**: it claims nothing new, its image is updated in place and the view row is unchanged. *Flagged:* the spec's sentence read literally would reject it, which would freeze every non-key update of a pre-existing duplicate; accepting keeps "the conflict set can only shrink".
  - **Unjudged records** (below the activation offset, refold below W, a joining member or a replace candidate building): never rejected. A new claimant of an owned N becomes the owner when its source key is smaller (the old owner's current row image moves to the extras), else an extra.
  - **Promotion.** When the owner leaves N (tombstone, rename, a writer version without a key component), the smallest extra is promoted: its stored image becomes N's row, in the same apply. A unique view keyed by its source key alone (no owner map) can never conflict and has no claims.
- **P9-4 — The check rule (§8.2).**
  - Every prospect row that is not `removed` (the last contributor left) and not `promoted` (another record's pre-existing data) is evaluated. The record is rejected iff the predicate is **False** on the row **and** at least one column the predicate reads differs between the current row (absent for a new row) and the prospect image.
  - **Three-valued logic.** A comparison with a NULL column (`isNull`), or with a non-finite `Float64` column, is **Unknown**. `and` / `or` / `not` follow Kleene logic. Unknown passes, as SQL `CHECK` does. *Flagged:* the spec says nothing about NULL; Sum overflow (`aggOverflow`) emits NULL.
  - **A move out of a group already over its limit is rejected** (the old group's row still fails and its count changed), exactly as the owner-confirmed rule reads; a delete out of it passes. *Flagged* for the owner.
  - **Comparison by type:** signed integers by value; `UInt16` / `UInt32` / `UInt64` / `Enum` unsigned by value; `Bool` as 0/1; `Float64` IEEE; `FixedStr` by `memcmp` over the slot width. A comparison node may have its `Column` child on either side (a `Const` on the left mirrors the operator).
  - **Compile rules** (`PlanCompiler.cpp`, 07's step 9, extended): a constraint reads only fixed-width columns of at most 16 bytes ("constraint '<n>': check compares fixed-width columns of at most 16 bytes; '<c>' is <Type>"), and has at most `kMaxPredicateNodes = 64` nodes ("constraint '<n>': at most 64 predicate nodes"). Both are `ConstraintInvalid` (57).
- **P9-5 — Changing constraints through C12** (the desired set of 04's P4-6, extended):
  - **Create.** A new verdict template joins the desired set at once: `requestChange` appends the change at A, the member builds unjudged below `A + 1`, and delivery above A waits until it has caught up (§8.7 create; 04's mechanism).
  - **Replace** (`create or replace` = `AlterView`: the old sink is uninstalled, a new template with the same `viewObjectId` and a new fingerprint is installed). The new template is a **candidate**: `VerdictSource::addCandidate` replays it unjudged, skipping rejected offsets (`FoldSkip.rejected`), bounded by `judgedThrough()`. The desired set keeps the logged member of that `viewObjectId` (the old fingerprint, kept installed as retiring) until `candidateCaughtUp` holds; then, in the same worker step, `requestChange` names the candidate, which appends the change at S. The old member judges every offset below S, the new one every offset above it, and the candidate follows the judge from `judgedThrough()` to `S + 1` as a joining member. There is never a gap and no long pause (§8.7 replace).
  - **Drop.** The desired set loses the member at once; the change lands at D; judging by it stops above D (§8.7 drop). Drop followed by create leaves the gap `(D, A')` (documented).
- **P9-6 — When a retiring plan is uninstalled (R04a, phase 2).** A template marked retiring (04: by `uninstallView` of a verdict template) or installed from a retained plan (P9-8) is erased, with its instances, only when for every partition of its source that this worker owns as Ready leader, `state.rejected.watermark()` (W) is above the member's leave offset `L = constraintSets.leaveOffsetOf(member)`, **and** this worker owns no partition of the source as follower. A follower keeps it (it may lead next). This replaces 04's "dropped once the judge has passed its leave offset" (deviation recorded in Task 6's commit body).
- **P9-7 — Why phase 2 is durable.** A replica that restarts after the `DropView` (or the `AlterView`) applied has no catalog plan for the leaving member, and one whose catalog lags never had it. If such a replica leads while `W < L`, it must still judge `[K, L)` with that plan. P9-8 gives it the plan from its own log: every leader that judges a record under a member has, below that record, the member's join change record, and the plan record just below that.
- **P9-8 — The `kVerdictPlan` control record (type 20).**
  - **Payload** (after C10's `[version u8 = 1]`): `[viewObjectId u64][planFingerprint u64][kind u8][planLen u32][plan bytes][outLen u32][output descriptor bytes][srcLen u32][source descriptor bytes]`, all LE. The plan bytes are the catalog's `compiledPlanBlob`, the output descriptor the `outputDescriptorBlob`, the source descriptor the view's evolving source descriptor (`srcDescBlob` of the install message). Batch form, CRC and `producerId −1` exactly as C10.
  - **Who appends it.** `VerdictSource::requestChange`, immediately before the change batch, one per **joining** member of kind `Unique` or `Check` (in `next`, not in the latest logged set). Key index and `TestReject` members never get one. A failover between the two appends is harmless: the change record is re-requested by the next leader, which re-appends the plan record.
  - **Readers.** `control_record::recordType` accepts 20. Every reader skips it like 16–19 (Part 04's sweep is extended to it). The cleaner always keeps type 20, like type 18 (`keepsControlRecord(offset, 20) == true`). Garbage collection is a follow-up issue (Finish).
  - **The feed.** `verdictFeedBatch` (04) stores each type-20 record into `PartitionWriteState::retainedPlans` (cold, may allocate; the leadership scan only). `verdictFeedBegin` truncates it from `target` like the other state. After activation, entries whose member left at `L < W` are pruned.
  - **Install from the log.** When the judge is `waitingForInstall()` for `(viewObjectId, planFingerprint)` and `retainedPlans.find(…)` has it, `WorkerViewSet` installs a retiring, non-persisted template from it (cold, counted `retainedPlanInstalls`), and 04's activation replays it unjudged to K. It serves no reads (its sink is gone or another plan's).
  - **Size.** `kMaxVerdictPlanBytes = 65536` for the whole payload; the AdminWorker refuses a bigger constrained view (P9-9), so `requestChange` never meets one.
- **P9-9 — DDL rules** (`AdminWorker::viewDef_`, after 07's compile and partition rule, only when the compiled plan has `unique` or constraints; in this order, each a `Rejected(reject(code, detail))`):
  1. `featureRefusal(DbFeature::Constraint, topicShapeOf(*source))` → `InvalidRequest` (39) with its message (only the transient branch is reachable: untyped and keyless sources are refused earlier with 33 / 35, 02 P6).
  2. `constrainedViewRefusal(viewObjectId, constraints.size(), membersAfter, planRecordBytes)` → `ViewConstraintInvalid` (57) with its message:
     - `viewObjectId >= 65536` or `constraints.size() > 65535` → "constraint ids exhausted" (SD5);
     - `membersAfter > kMaxVerdictViewsPerSource` (32), where `membersAfter` = 1 (this view) + 1 if the source has `keyFeatures.keyIndex` + every other live view in `s.views()` over the same source whose compiled plan is unique or constrained → "a topic feeds at most 32 verdict views (the key index counts as one)";
     - `planRecordBytes = verdictPlanPayloadBytes(compiled, output, source) > kMaxVerdictPlanBytes` → "a constrained view's plan must encode in at most 65536 bytes".
  - The two P7-9 lines are deleted. A transient **view** with constraints over a persisted source is allowed (it is rebuilt from the log on restart; Part 04's checkpoint gate concerns persisted members only).
- **P9-10 — `uniqueConflicts`** is a gauge: the instance's current number of extra claimants, set after every fold pass and after a checkpoint replay. Stat 32 is present iff the view is unique and owner-mapped. `uniquePromotions` (cumulative) is logged and kept in `ViewFoldCounters`, not a stat.
- **P9-11 — The replay's constraint names.** The broker side is Part 04's (the index stores each rejection's `constraintId`). A rejected record's completion carries its offset in `value` (= base offset + `recordIndex`), so `WriteResult::offset()` of a 51 is the rejected record's own offset: the loser of a race passes it as `minOffset` and its read finds the winner (§9 "Reading the record that caused a conflict"). If Part 04 already fills `value` there, this is a test only.
- **P9-12 — `Client::constraintName`.** `constraintId 0` (the key index) → `nullopt`. Else `objectId = id >> 16`, `ordinal = id & 0xFFFF`. The resolver looks in the catalog state first (`viewByObjectId`, the `normalizedPlanBlob` decoded with `deserializeAuthored`): ordinal 0 needs `unique` and names the constraint `"unique"`; ordinal k ≤ `constraints.size()` names `constraints[k − 1].name`. If that fails, it walks the cached events newest first for the latest `CreateView` / `AlterView` of that object id whose plan has the ordinal (the view name is the event's `objectName`): a dropped or replaced view still resolves. A miss refreshes the Client's `CatalogReader` once (bounded by `kConstraintNameRefreshTimeout`, 5 s) and retries; then `nullopt`. Owner thread only, like every catalog read (`Client.h:968-997`).
- **P9-13 — Nothing new on the write path.** No hook rule is added. A constrained source is already "constrained" for 03's rule 7 and 06's `KeyZeroCreateNeedsView` through 04's `isConstrainedSource()`, which counts `Unique` and `Check` members.

## Contract additions (none redefines a contract type)

1. **`RecordFold.h` (04's `ProspectRow`)** appends `bool ownedByOther = false; bool promoted = false;`.
2. **`broker/libs/view/include/rune/view/CheckEval.h`** (new):
   ```cpp
   enum class PredicateResult : uint8_t { False = 0, True = 1, Unknown = 2 };
   inline constexpr std::size_t kMaxPredicateNodes = 64;
   [[nodiscard]] PredicateResult evaluatePredicate(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout,
                                                   const uint8_t* row) noexcept;
   [[nodiscard]] bool predicateReadsChanged(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout,
                                            const uint8_t* before /*nullptr = a new row*/, const uint8_t* after) noexcept;
   [[nodiscard]] std::optional<std::string> constraintShapeError(const CompiledConstraint& c, const RowLayout& layout);   // cold
   ```
3. **`broker/libs/view/include/rune/view/UniqueClaims.h`** (new):
   ```cpp
   [[nodiscard]] int compareSourceKeys(rune::codec::PrimType keyType, std::string_view a, std::string_view b) noexcept;
   class UniqueClaims {
   public:
       UniqueClaims(uint16_t keyWidth, uint8_t wireWidth, uint32_t rowLen, rune::codec::PrimType sourceKeyType,
                    uint64_t hashSeed);                                                       // cold
       [[nodiscard]] const uint8_t* ownerOf(const ViewKey& n) const noexcept;               // owner wire bytes, nullptr = no row
       [[nodiscard]] const uint8_t* extraKeyOf(std::string_view wire) const noexcept;       // N this wire is an extra of
       [[nodiscard]] bool hasExtras(const ViewKey& n) const noexcept;
       [[nodiscard]] std::size_t extras() const noexcept;                                   // the uniqueConflicts gauge
       void setOwner(const ViewKey& n, std::string_view wire) noexcept;
       void eraseOwner(const ViewKey& n) noexcept;
       void addExtra(const ViewKey& n, std::string_view wire, const uint8_t* image) noexcept;
       void updateExtraImage(std::string_view wire, const uint8_t* image) noexcept;
       void removeExtra(std::string_view wire) noexcept;
       [[nodiscard]] bool smallestExtra(const ViewKey& n, std::string_view& wire, const uint8_t*& image) const noexcept;
       ViewTable& indexTable() noexcept;  ViewTable& extrasTable() noexcept;                // checkpoint v3 tables 6, 7
       void recountAfterReplay() noexcept;                                                  // cold
   };
   ```
   - `index`: a `ViewTable` keyed by N (K bytes) with payload `[ownerWire W][extraCount u32][headExtra W][flags u8]`.
   - `extras`: a `ViewTable` keyed by the wire key (W bytes) with payload `[N K][prev W][next W][links u8 (bit0 prev, bit1 next)][image rowLen]`, a doubly linked list per N (O(1) insert and erase; `smallestExtra` walks the list: the O(extras of N) rescan of §8.3).
   - `ProjectionFold` (04/07) appends `UniqueClaims* claims = nullptr;`. `FoldStats` appends `uint64_t uniquePromotions = 0;`. `ViewFoldCounters` appends `uint64_t uniqueConflicts = 0; uint64_t uniquePromotions = 0;`. `ViewInstance` appends `std::optional<rune::view::UniqueClaims> claims;`.
4. **`ViewCheckpoint.h` (07's v3)** appends table ids `kKeyedTableUniqueIndex = 6`, `kKeyedTableUniqueExtras = 7`.
5. **`Verdict.h` (04)** appends `[[nodiscard]] std::optional<VerdictViewKind> verdictKindOf(const CompiledViewPlan&) noexcept;` and `[[nodiscard]] uint64_t verdictPlanFingerprint(std::span<const uint8_t> compiledPlanBlob) noexcept;`. `VerdictViewKind::Unique` means "a unique view, with or without checks"; `::Check` "a non-unique view with checks".
6. **`VerdictSource.h` (04)** appends:
   ```cpp
   struct ConstraintFold { const rune::view::CompiledViewPlan& plan; const rune::view::RowLayout& layout;
                           const rune::view::ViewTable& table; };
   [[nodiscard]] rune::view::Verdict evaluateConstraints(const ConstraintFold&, const rune::view::SourceRecord&,
                                                         const rune::view::Prospect&) noexcept;
   struct RetainedVerdictPlan { uint64_t viewObjectId = 0; uint64_t planFingerprint = 0; rune::view::VerdictViewKind kind{};
                                int64_t offset = -1; std::vector<uint8_t> plan, outputDescriptor, sourceDescriptor; };
   class RetainedVerdictPlans {                                   // cold: the leadership scan and activation only
   public:
       void onRecord(int64_t offset, uint64_t viewObjectId, uint64_t planFingerprint, rune::view::VerdictViewKind kind,
                     std::span<const uint8_t> plan, std::span<const uint8_t> out, std::span<const uint8_t> src);
       [[nodiscard]] const RetainedVerdictPlan* find(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;
       void truncateFrom(int64_t offset) noexcept;
       void pruneLeftBelow(int64_t watermark, const ConstraintSetTimeline&) noexcept;
       [[nodiscard]] std::size_t size() const noexcept;
   };
   ```
   - `VerdictSource`: `void addCandidate(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint);`, `void removeCandidate(uint64_t viewObjectId, uint64_t planFingerprint);`, `[[nodiscard]] bool candidateCaughtUp(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;`. `addVerdictView` gains a trailing `std::span<const uint8_t> planRecordPayload = {}` (the P9-8 payload, built cold at install and owned by the template), which `requestChange` appends for a joining member.
   - `ConstraintSetTimeline`: `[[nodiscard]] int64_t leaveOffsetOf(const ConstraintSetMember&) const noexcept;` (the offset of the first change record after the member's latest join whose set lacks it; `INT64_MAX` while it is still in `latest()` or never was).
   - `VerdictSourceStats` appends `uint64_t retainedPlanInstalls = 0, candidateSwaps = 0;`.
7. **`WriteState.h` (C7)**: `PartitionWriteState` appends `RetainedVerdictPlans retainedPlans;` (09).
8. **`ControlRecordLayout.h` (C10)**: `inline constexpr int16_t kVerdictPlan = 20;` (types 16–19 unchanged), `inline constexpr std::size_t kMaxVerdictPlanBytes = 65536;`, `struct VerdictPlanHeader { uint64_t viewObjectId; uint64_t planFingerprint; uint8_t kind; };`, `[[nodiscard]] std::size_t verdictPlanPayloadBytes(std::size_t planLen, std::size_t outLen, std::size_t srcLen) noexcept;`, `[[nodiscard]] std::size_t verdictPlanBatchBytes(std::size_t payloadBytes) noexcept;`, `std::size_t buildVerdictPlan(std::span<uint8_t> out, const VerdictPlanHeader&, std::span<const uint8_t> plan, std::span<const uint8_t> outDesc, std::span<const uint8_t> srcDesc) noexcept;` (0 = too small or over the cap), `[[nodiscard]] bool parseVerdictPlan(const uint8_t* batch, std::size_t len, VerdictPlanHeader& h, std::span<const uint8_t>& plan, std::span<const uint8_t>& outDesc, std::span<const uint8_t>& srcDesc) noexcept;`. `recordType` returns 20 for it.
9. **`WorkerViewSet`**: `ViewTemplate` appends `std::vector<uint8_t> planRecordPayload; bool fromRetainedPlan = false;`; `void installRetainedPlan_(const RetainedVerdictPlan&)` (private, cold); `void retireVerdictTemplates_(...)` (private, cold, P9-6); test seams `[[nodiscard]] std::size_t retiringVerdictTemplatesForTest() const noexcept;` and `[[nodiscard]] bool candidateForTest(uint64_t viewObjectId) const noexcept;`.
10. **`broker/libs/node/include/rune/node/ConstraintDdlRules.h`** (new, header-only): `[[nodiscard]] std::optional<std::string> constrainedViewRefusal(uint64_t viewObjectId, std::size_t constraintCount, std::size_t membersAfter, std::size_t planRecordBytes);`.
11. **`StatsCollect.h` (08)**: `collectViewPartitionStats(const ViewFoldCounters&, StatsSink&, bool uniqueView = false) noexcept` (a trailing defaulted parameter; id 32 first, then 33–36).
12. **Client**:
    - `client-cpp/include/rune/client/WriteResult.h` (05) appends `struct ConstraintName { std::string view; std::string constraint; bool operator==(const ConstraintName&) const = default; };` and `inline constexpr std::chrono::milliseconds kConstraintNameRefreshTimeout{5000};`;
    - `client-cpp/include/rune/client/ConstraintNames.h` + `client-cpp/src/ConstraintNames.cpp` (new): `[[nodiscard]] std::optional<ConstraintName> resolveConstraintName(const rune::catalog::CatalogState&, std::span<const rune::catalog::RecordedEvent>, uint32_t constraintId);`;
    - `std::optional<ConstraintName> Client::constraintName(uint32_t constraintId);` (C15) and `std::optional<ConstraintName> Client::constraintName(uint32_t constraintId, std::chrono::milliseconds refreshTimeout);`;
    - a rejected record's `Completion::value` = its offset (P9-11).
13. **Test support**:
    - `CheckedWritesRig`: `struct Char { uint64_t id = 0; uint64_t userId = 0; std::string name; uint16_t gender = 0; int64_t lastPlayed = 0; uint8_t intent = 0; bool tombstone = false; };`, `void createCharacterTopic(bool autoIncrement = true);` (topic 1 `{characterId uint64 key [autoincrement], userId uint64, name string(16), gender uint16, lastPlayed int64}`, compact, P 1), `int64_t appendCharacters(std::span<const Char>, uint8_t wireAcks, int64_t pid = -1, int32_t seq = -1);`, `enum class ConstrainedView : uint8_t { CharacterName, AccountStats, AccountCharacters, PerGender, Ledger };`, `int32_t createConstrainedView(ConstrainedView, int64_t cap = 12);`, `void replaceConstrainedView(ConstrainedView, int64_t cap);`, `void dropConstrainedView(ConstrainedView);`, `[[nodiscard]] uint64_t objectIdOf(ConstrainedView) const;`, `void createLedgerTopic();` (topic 2 `{entryId uint64 key, account uint64, amount int64, memo string(16)}`), `int64_t appendEntries(...)`, `[[nodiscard]] std::size_t uniqueConflicts(ConstrainedView);`. The views are created through 07's `createView(planV2)` (the real AdminWorker path) from plans authored by 08's `ql::applyViewPlan` on these statements:
      - `CharacterName`: `create unique view character_name key lower(name) as nameKey as select characterId from character;`
      - `AccountStats`: `create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= <cap>);`
      - `AccountCharacters`: `create view account_characters key (userId, characterId) as select name, gender, lastPlayed from character;` (no constraint)
      - `PerGender`: `create view per_gender key (userId, gender) as select count(*) as n from character group by userId, gender constraint two_per_gender check (n <= 2);`
      - `Ledger`: `create view balances key account as select sum(amount) as balance from entry group by account constraint non_negative check (balance >= 0);`
    - `CheckedWritesModel`: `void addLowerNameUniqueRule(uint64_t viewObjectId);`, `void addCountCheckRule(uint64_t viewObjectId, int64_t cap);`, `[[nodiscard]] std::map<std::string, uint64_t> uniqueOwners() const;`, `[[nodiscard]] std::size_t uniqueExtras() const;`, `[[nodiscard]] std::map<uint64_t, int64_t> liveCountByUser() const;`.
    - Matrix `Op` 80–86, `Entity` 80–90, `expect09`, `cellsFor(Part::P09)`, pin row 09; stress invariants `AtMostTwelveLiveCharacters`, `LowerNameUnique`.

## Contract gaps (flagged to the controller)

- **§8.3 "using the retractable Min machinery that aggregates already use".** `AggFold.cpp`'s value blocks key contributors by an 8-byte source key and hold 8-byte aggregate inputs, not row images, so promotion could not rebuild a projected row from them. `UniqueClaims` follows the same pattern (a per-key contributor list plus a rescan when the extreme leaves) with its own two tables and works for any source key width (P9-3).
- **An extra claimant updating a non-key column is accepted** (P9-3). The literal reading of §8.2 would reject it.
- **A move out of an over-limit group is rejected** (P9-4, the literal owner-confirmed rule). A reasonable reader might expect it to pass, since it shrinks the group.
- **NULL in a check is Unknown and passes** (P9-4). The spec is silent.
- **R04a needs a durable plan.** The catalog forgets a dropped or replaced plan at once, so a restarted replica would wait forever. This part adds control record type 20 (`kVerdictPlan`, P9-8), always kept by the cleaner. GC of type-20 records is a follow-up issue.
- **`Client::constraintName` may block** for one catalog refresh (≤ 5 s) on a miss (P9-12). C15 gives it no timeout parameter, so a second overload takes one.
- **`VerdictSourceStats::perConstraint`** (04) holds 256 entries per source. A source with more distinct constraint ids keeps the first 256 and counts the rest only in `rejectionsConstraint`. That is 04's bound, noted here, unchanged.

## Review Focus

These are the five inputs or conditions the spec implies but no requirement names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A chain of pre-existing duplicates** (three claimants of one lowered name, then owners leaving one after another, a rename of the owner and an extra's update in the same batch). Each promotion must pick the next smallest source key, keep the extras' latest images, and end with one row per name and a zero gauge.
   - Tests: `CheckedWritesUniqueFoldRegression.PromotionChainOfThreeClaimantsRegression` (Task 2), `CheckedWritesUniqueFold.ExtraKeepsItsKeyAndItsLatestImageIsPromoted` (Task 2).
2. **A failover or a restart while a drop or a replace is in flight, on a node whose catalog no longer has the old plan.** The new leader must judge `[K, L)` with the old plan taken from its log and then finish the change. It must never wait forever, and never judge with a guessed set.
   - Tests: `CheckedWritesConstraintChangeRegression.FailoverDuringDropFinishesTheDropRegression`, `…NothingWaitsForeverAfterAnyDropTimingRegression`, `…RestartDuringReplaceUsesTheRetainedOldPlanRegression` (Task 6).
3. **A NULL or non-finite column under a check** (a `sum` that overflowed and emitted NULL, a `Float64` NaN), and a `Const` written on the left side of a comparison. The answer is Unknown, the record passes, and nothing crashes.
   - Tests: `CheckedWritesCheckEvalRegression.NullAndNonFiniteAreUnknownAndPassRegression`, `CheckedWritesCheckEval.ConstOnTheLeftMirrorsTheOperator` (Task 1), `CheckedWritesConstraintVerdictRegression.SumOverflowNullNeverRejectsRegression` (Task 4).
4. **Group moves at the limit**: out of a group already over its limit (rejected, P9-4), into a group at 11 (passes) or at 12 (rejected), and a delete plus a create for the same user in one batch (the delete makes room in the same fold).
   - Tests: `CheckedWritesConstraintVerdictRegression.MoveOutOfAnOverLimitGroupIsRejectedRegression`, `CheckedWritesConstraintVerdict.DeleteMakesRoomInTheSameBatch` (Task 4).
5. **A `View` retry of a rejected create after the constrained view was replaced or dropped.** The replay carries the original constraint ids, and `constraintName` still resolves them to the original view and constraint names.
   - Tests: `CheckedWritesConstraintWorkerRegression.ViewRetryReplaysTheOriginalConstraintIdsAfterAReplaceRegression` (Task 5), `CheckedWritesConstraintNameRegression.ReplayedIdResolvesAfterReplaceAndDropRegression` (Task 7).

---

## Interaction matrix

### A. The shared worker-level matrix (`rune_checked_writes_matrix_tests`, `CheckedWritesMatrix/Cell09.*`, `cellsFor(Part::P09)`, Task 8)

Built on 04's `CheckedWritesRig` with `createCharacterTopic()` (autoincrement, key index implied), then `createConstrainedView(CharacterName)` (lower object id) and `createConstrainedView(AccountStats)`, so a record failing both is answered by `character_name` (P4-5). Every op is sent by an idempotent v12 producer with `All | View` (wire 3) unless the timing says otherwise. U is the acting user, W another user (3 characters), V a source user (5 characters). Setups that exceed the limit or duplicate a name are written **before** the views are created (unjudged below A).

**Ops (Part 09 range 80–99):**

| Id | Op | Records |
|---|---|---|
| 80 | `CCreate` | a key-0 create for U named X |
| 81 | `CUpdateNonKey` | an `update` of C (one of U's) changing `lastPlayed` only |
| 82 | `CRename` | an `update` of C to name X |
| 83 | `CMove` | an `update` of C (one of V's) to `userId = U` |
| 84 | `CRemove` | `remove(C)` |
| 85 | `CRemoveExisting` | `removeExisting(C)` |
| 86 | `CDeleteThenReuse` | `remove(C)`, then a key-0 create for U named C's old name |

**Entities (Part 09 range 80–99), the state before the op:**

| Id | Entity | U's characters | Name X / C |
|---|---|---|---|
| 80 | `G11Free` | 11 | X free |
| 81 | `G11Taken` | 11 | X owned by one of W's characters (another case: `"Foo"` vs `"foo"`) |
| 82 | `G12Free` | 12 | X free |
| 83 | `G12Taken` | 12 | X owned by W's |
| 84 | `OverFree` | 14 (pre-existing) | X free |
| 85 | `OverTaken` | 14 (pre-existing) | X owned by W's |
| 86 | `G11Self` | 11 | C keeps its own name (for `CRename`, X = C's name in another case) |
| 87 | `G12Self` | 12 | as 86 |
| 88 | `OverSelf` | 14 (pre-existing) | as 86 |
| 89 | `DupOwner` | 5 | pre-existing `"Dup"` (character 5, U) and `"dup"` (character 9, W); the op acts on character 5, the owner; X = `"DUP"` |
| 90 | `DupExtra` | 5 | as 89; the op acts on character 9, the extra |

**Timings (15 of the shared enum):** `Single, BackToBack, SelfCollidingBatch, Racing, RetryBeforeJudge, RetryAfterJudge, FailoverAppendHw, FailoverHwFold, FailoverFoldMarker, TruncateTail, RestartDuringRebuild, CreateOnNonEmpty, ReplaceDuringWrites, DropDuringWrites, RetryAfterWindow`. 03's six appended timings and the two `Txn*Behind` values are not instantiated: a constrained source refuses transactions before Part G (03 rule 7, tested by name in Task 5). Their rig meaning is 04's, plus:
- `Racing`: a second producer sends the same op for U in the same pass (two creates for the last slot, two renames to the same name).
- `SelfCollidingBatch`: the op's records twice in one batch (`[create X, create x]`, `[removeExisting C, removeExisting C]`).
- `CreateOnNonEmpty`: the setup is written first, then both views are created, then the op runs while they build.
- `ReplaceDuringWrites`: `account_stats` is replaced by `check (characters <= 13)` right before the op's (last) batch, while a background producer writes for W.
- `DropDuringWrites`: `character_name` is dropped right before the op's (last) batch.

**Not applicable (top-down; the first matching rule names the reason):**
1. `CCreate × {G11Self, G12Self, OverSelf, DupExtra}`: "a new record owns no name yet" / "the duplicate's name is the DupOwner cell".
2. `{CUpdateNonKey, CRemove, CRemoveExisting, CDeleteThenReuse} × {G11Free … OverTaken}`: "the op keeps C's own name".
3. `CDeleteThenReuse × DupExtra`: "covered by CRemove × DupExtra".
4. `CMove × everything but {G11Self, G12Self, OverSelf}`: "a move keeps the name; the entity names the target group".

Applicable op × entity pairs: `CCreate` 7, `CUpdateNonKey` 5, `CRename` 11, `CMove` 3, `CRemove` 5, `CRemoveExisting` 5, `CDeleteThenReuse` 4 = **40**. **Cell count:** 7 × 11 × 15 = 1155: **600 applicable**, 555 skipped, pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 09).

**`expect09(cell)`, top-down.** The verdicts always come from `CheckedWritesModel` with `addKeyIndexRule()`, `addLowerNameUniqueRule(objectIdOf(CharacterName))` and `addCountCheckRule(objectIdOf(AccountStats), cap)`, which reads each offset's set from the log's change records. `expect09` names what the cell checks:
0. `RetryAfterWindow` → the retry's answer is `StoredIdUnknown` (13) for a key-0 batch (`CCreate`, the create of `CDeleteThenReuse`) and `VerdictUnknown` (14, `errorCode 21`) otherwise.
1. `Single`, as a check on the model (`CheckedWritesModel.ConstraintRulesMatchTheTable` asserts the model gives exactly this). `N` = 51 with `character_name`'s ordinal 0, `M` = 51 with `account_stats`'s ordinal 1:

   | Op \ Entity | G11Free | G11Taken | G12Free | G12Taken | OverFree | OverTaken | G11Self | G12Self | OverSelf | DupOwner | DupExtra |
   |---|---|---|---|---|---|---|---|---|---|---|---|
   | `CCreate` | Ok | N | M | N | M | N | — | — | — | N | — |
   | `CUpdateNonKey` | — | — | — | — | — | — | Ok | Ok | Ok | Ok | Ok (P9-3) |
   | `CRename` | Ok | N | Ok | N | Ok | N | Ok | Ok | Ok | Ok, 9 promoted | Ok, gauge 0 |
   | `CMove` | — | — | — | — | — | — | Ok | M | M | — | — |
   | `CRemove` / `CRemoveExisting` | — | — | — | — | — | — | Ok | Ok | Ok | Ok, 9 promoted | Ok, gauge 0 |
   | `CDeleteThenReuse` | — | — | — | — | — | — | Ok, Ok | Ok, Ok | Ok, M | Ok, N | — |

2. Every other timing → the model's verdicts for the committed log (they differ from row 1 only where the timing changes the log: a racing second writer, a replace to 13, a drop of `character_name`).

**Every applicable cell asserts** (after `driveUntilJudged(LEO)` and `compact()`): 04's (a)–(g), plus
- (h) `character_name`'s rows and claims equal the model's `uniqueOwners()` (owner = smallest live claimant) and `uniqueExtras()`;
- (i) `account_stats`'s rows equal `liveCountByUser()` of the accepted live records;
- (j) `ViewFoldCounters::uniqueConflicts` equals `uniqueExtras()`;
- (k) every rejected completion carries the model's constraint id, and `rejected.mismatches() == 0`.

### B. Named §15 cells (the overview's list for 09)

| §15 cell | Test (task) |
|---|---|
| create, rename, move, delete, delete-then-reuse × user × group (11 / 12 / over) × name | matrix A (8) |
| a predicate failing on a row already over its limit: a rename passes, a create fails, a delete passes | `CheckedWritesConstraintVerdict.OverLimitGroupRenamePassesCreateFailsDeletePasses` (4), matrix `Over*` |
| a delete always passes, including one that leaves `check (balance >= 0)` failing; then a debit is rejected, a repairing credit passes, a rename passes | `CheckedWritesConstraintVerdict.DeleteAlwaysPassesBalanceLedger` (4), `CheckedWritesConstraintWorker.LedgerDeleteLeavesItFailingThenOnlyRepairsPass` (5) |
| pre-existing duplicate names: the smallest source key owns the row, promoted on delete and on rename, a compaction then a refold gives the same owner | `CheckedWritesUniqueFold.*` (2), `CheckedWritesConstraintWorkerRegression.PreExistingDuplicatesSurviveCompactionAndRefoldRegression` (5), matrix `Dup*` |
| several verdict views, where one rejects: no view changes | `CheckedWritesConstraintVerdict.SeveralVerdictViewsOneRejectsNoViewChanges` (4) |
| the login race: the loser gets `ConstraintViolation` and its read finds the winner's row | `CheckedWritesConstraintWorker.LoginRaceLoserReadsTheWinner` (5), `CheckedWritesRegressorsPathRegression.TheWholeScenarioThreeNodesRegression` (10), docker `RP-LOGIN-RACE` (11) |
| a view created on a non-empty topic, with writes running during its build | `CheckedWritesConstraintWorker.CreateOnANonEmptyTopicJudgesFromTheChangeRecord` (5), matrix `CreateOnNonEmpty` |
| `create or replace` while writes are running | `CheckedWritesConstraintWorker.ReplaceSwapsAtSWithoutAGap` (5), matrix `ReplaceDuringWrites` |
| drop while writes are running | `CheckedWritesConstraintWorker.DropStopsJudgingAboveD` (5), `CheckedWritesConstraintChangeRegression.FailoverDuringDropFinishesTheDropRegression` (6), matrix `DropDuringWrites` |
| two writers racing for the last slot or the same name | matrix `Racing`; `TheWholeScenarioThreeNodesRegression` step 6 (10) |
| before Part G, a transactional and an atomic batch on a constrained source are refused | `CheckedWritesConstraintWorker.ConstrainedSourceRefusesTransactions` (5) |
| a create rejected at fold: its id is burned | `TheWholeScenarioThreeNodesRegression` step 6 (10) |

### C. The model check (`CheckedWritesModelRegression.SeededConstraintsMatchTheModel`, `rune_checked_writes_matrix_tests`, Task 9)

Seeds 1–200 × 250 steps on `CheckedWritesRig` with `character` + `character_name` + `account_stats` at `cap = 3` (dense boundary hits), plus seeds 1–20 at `cap = 12`. Actions, one per step from the seed's `std::mt19937_64`: creates for 3 users with names from `{"Foo","foo","FOO","Bar","bar","Baz","Ⱥx","ⱥx"}`, renames, moves, `remove`, `removeExisting`, plain-acks writers (`acks` 1, a v11 session); `failover()`, `demotePromote()`, `restart()` (also mid-rebuild), `truncateTo(HW)`, `compact()`; constraint changes: replace `account_stats` (cap ↔ cap + 1), drop and re-create `character_name`. After every step: 04's six invariants, plus `AtMostCapLiveCharacters` (every user's accepted live count ≤ the cap active at its last judged create, unless it was over before the view existed), and `LowerNameUnique` (no two live characters judged under `character_name` share a lowered name; every extra was written while no unique member was active). `CheckedWritesModelRegression.SameSeedSameTrace` re-runs seeds 1–8. A failure prints `seed=<n> step=<k> action=<name>`.

### D. The cluster stress (`rune_checked_writes_stress_tests`, `CheckedWritesStressRegression.SeededRegressorsPathHoldsTheInvariants`, Task 9)

`CheckedWritesStressDriver` on `ThreeNodes` (default shard mode, 64 KiB segments, cleaner every 50 ms, `RUNE_VERDICT_INTERVAL_MS=20`), set up with the golden DDL's `user`, `user_identity`, `character`, `character_by_id`, `character_name`, `account_stats`. Seeds 1–12 × 300 steps. Actions: racing first logins (two clients insert the same `(provider, subject)`), racing creates for 4 users near 12, renames to colliding names, moves, `removeExisting`, leader crash and restart, follower restart, cleaner passes, replace of `account_stats` (same predicate, a new plan). Invariants (by name): 04's five, 06's `NoIdTwiceNoReuse`, and this part's `AtMostTwelveLiveCharacters` and `LowerNameUnique`, checked from the final leader's views (`account_stats`, `character_by_id`, `character_name` read through `getViewRowAndWait`) **and** from the model of the committed log.

### E. Docker e2e (VM, Task 11)

The `REGRESSORSPATH` leg of `broker/docker/checked-writes-e2e.sh`: the golden script through the CLI, a login race and a character race through a partition-leader kill, a full restart, the delete and reuse lifecycle, and the consumer and compaction checks.

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `broker/libs/view/include/rune/view/CheckEval.h`, `broker/libs/view/src/CheckEval.cpp` | C | Predicate evaluation, changed-column test, compile shape rules |
| `broker/libs/view/include/rune/view/UniqueClaims.h`, `broker/libs/view/src/UniqueClaims.cpp` | C | Claim index, extras list, `compareSourceKeys`, the gauge |
| `broker/libs/view/{include/rune/view/RecordFold.h,include/rune/view/ViewFold.h,src/ViewFold.cpp}` | M | `ProspectRow` flags; the unique rules in the keyed projection step |
| `broker/libs/view/{include/rune/view/ViewCheckpoint.h,src/ViewCheckpoint.cpp}` | M | v3 tables 6 and 7 |
| `broker/libs/view/{include/rune/view/Verdict.h,src/PlanCompiler.cpp}` | M | `verdictKindOf`, `verdictPlanFingerprint`; the constraint shape rules |
| `broker/libs/view/CMakeLists.txt` | M | New sources and tests |
| `broker/libs/view/tests/view/CheckedWrites{CheckEval,UniqueFold}Test.cpp` | C | `rune_view_tests` |
| `broker/libs/storage/include/rune/storage/{ControlRecordLayout.h,VerdictCleanState.h}` | M | Type 20; the cleaner keeps it |
| `broker/libs/node/include/rune/node/{VerdictSource.h,WriteState.h,ConstraintDdlRules.h(C),StatsCollect.h}`, `broker/libs/node/src/VerdictSource.cpp` | M/C | Members, candidates, retained plans, the feed, DDL rule function, stat 32 |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | Kinds at install, claims instances, desired-set rules, candidates, retirement, install from a retained plan |
| `broker/libs/node/include/rune/node/Worker.h` | M | Retirement sweep in `rebindVerdictSources_`, stats collector call |
| `broker/libs/node/src/AdminWorker.cpp` | M | P7-9 lines deleted; P9-9 rules |
| `broker/libs/node/tests/node/CheckedWrites{ConstraintDdl,ConstraintVerdict}Test.cpp` | C | `rune_checked_writes_tests` |
| `broker/libs/node/tests/node/CheckedWrites{ConstraintWorker,ConstraintChange}Test.cpp` | C | `rune_checked_writes_verdict_tests` |
| `broker/libs/node/tests/node/{CheckedWritesAdminViewDdlTest.cpp,CheckedWritesQlCompileTest.cpp,CheckedWritesKeyedRecoveryTest.cpp,CheckedWritesVerdictSourceTest.cpp,CheckedWritesViewSetTest.cpp}` | M | Inversions (07, 08), recovery of claims, the type-20 reader sweep, template kinds |
| `broker/libs/node/tests/support/{CheckedWritesRig.h,CheckedWritesMatrixExpectations.h,CheckedWritesModel.h,CheckedWritesStressDriver.h}`, `tests/node/{CheckedWritesMatrixTest.cpp,CheckedWritesModelTest.cpp}`, `tests/integration/CheckedWritesStressTest.cpp` | M | Rig helpers, matrix rows, model rules, stress invariants |
| `broker/libs/node/tests/client/CheckedWritesRegressorsPathRegressionTest.cpp` (C, Task 10), `tests/client/support/RegressorsPathRecords.h` (C, Task 9), `tests/client/CheckedWritesQlBrokerRegressionTest.cpp` (M), `broker/libs/node/CMakeLists.txt:1731` (M) | M/C | Real e2e |
| `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` | M | Zero-alloc judging gate |
| `broker/libs/storage/tests/storage/{CheckedWritesControlRecordTest.cpp,CheckedWritesVerdictCleanerTest.cpp}` | M | Type 20 |
| `client-cpp/include/rune/client/{WriteResult.h,ConstraintNames.h(C),Client.h}`, `client-cpp/src/{ConstraintNames.cpp(C),Client.cpp}`, `client-cpp/src/producer/ProducerImpl.cpp`, `client-cpp/src/cli/CatalogCommands.cpp`, `client-cpp/CMakeLists.txt` | M/C | `constraintName`, the rejected offset, CLI names |
| `client-cpp/tests/client/CheckedWritesConstraintNameTest.cpp` (C), `client-cpp/tests/producer/ProducerVerdictsTest.cpp`, `client-cpp/tests/CheckedWritesQlCliTest.cpp` | M/C | Client tests |
| `broker/docker/checked-writes-e2e.sh`, `client-cpp/examples/checked_writes_probe.cpp`, `client-cpp/tests/support/GenClaimSchemas.cpp`, `docs/testing.md` | M | The `REGRESSORSPATH` leg |
| `broker/benchmark/ViewFoldBench.cpp` | M | `BM_ViewFoldVerdict/unique_lower_plus_count_check` and its baseline |
| `docs/api/{ql.md,client-cpp.md,wire-protocol.md,admin-cli.md}`, `docs/operations.md`, `client-cpp/CLAUDE.md` | M | Docs this part ships |

---

## Phase 1 — The view engine

### Task 1: Predicate evaluation (`CheckEval`) and the constraint shape rules

**Files:**
- Create: `broker/libs/view/include/rune/view/CheckEval.h`, `broker/libs/view/src/CheckEval.cpp` (add to `rune_view`, `broker/libs/view/CMakeLists.txt:1-10`), `broker/libs/view/tests/view/CheckedWritesCheckEvalTest.cpp` (add to `rune_view_tests`, `:21-27`)
- Modify: `broker/libs/view/src/PlanCompiler.cpp` (07's step 9, the constraint loop: call `constraintShapeError` per constraint after the structural checks)

**Interfaces:**
- Consumes: `CompiledPredicateNode`, `CompiledConstraint`, `PredicateOp` (07 Task 4 / C14); `RowLayout`, `readSlot`, `isNull`, `decodeSlotValue` (`ViewRow.h:13-55`).
- Produces: contract addition 2.
  - `evaluatePredicate` evaluates the flat vector in index order into a local `std::array<PredicateResult, kMaxPredicateNodes>`; the root is the last node. A `Column` or `Const` node is a leaf (no truth value). A comparison reads the `Column` child's slot from `row` with `readSlot` and decodes the `Const` child with `decodeSlotValue(node.constBytes.data(), node.constType, layout.slotWidth[col.outSlot])` (R08c), then compares by P9-4's type rules. A `Const` on the left mirrors the operator (`5 < x` ≡ `x > 5`).
  - `predicateReadsChanged(nodes, layout, before, after)`: true when `before == nullptr`, else iff some `Column` node's slot differs (null bit or bytes over `slotWidth`).
  - `constraintShapeError(c, layout)`: P9-4's two compile rules, with exact messages; nullopt = fine.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesCheckEvalTest.cpp`; a `RowLayout` built with `buildRowLayout` from a descriptor `{characters Int64, balance Int64, total UInt64, ratio Float64, ok Bool, plat Enum, tag FixedStr(8), small UInt16}`; constants encoded as Part 08's P8-5 does).
  ```cpp
  TEST(CheckedWritesCheckEval, CountLimitOnTwelveAndThirteen) {
      // nodes [Column characters, Const Int64 12, Le(a=0,b=1)]
      // row characters = 12 -> True; 13 -> False
  }
  ```
  - `CheckedWritesCheckEval.EveryComparisonOnEveryType` (`TEST_P` over the 6 operators × the 8 column types, each at below / equal / above the constant): the expected truth per C++ comparison of the decoded values; `FixedStr` compares `"abc\0\0\0\0\0"` < `"abd…"`, `UInt64` `0xFFFF'FFFF'FFFF'FFFF > 1`, `Int64 −1 < 0`.
  - `CheckedWritesCheckEval.ConstOnTheLeftMirrorsTheOperator`: `[Const 5, Column small, Lt(a=0,b=1)]` on `small = 6` → True, on 5 → False.
  - `CheckedWritesCheckEval.KleeneAndOrNot`: `and`/`or`/`not` over every pair of {True, False, Unknown} gives the Kleene table (`False and Unknown = False`, `True or Unknown = True`, `not Unknown = Unknown`).
  - `CheckedWritesCheckEvalRegression.NullAndNonFiniteAreUnknownAndPassRegression` (Review Focus 3): `balance` NULL (`setNull`) under `balance >= 0` → Unknown; `ratio` NaN and `+Inf` under `ratio <= 1.5` → Unknown; `not (balance >= 0)` with NULL → Unknown.
  - `CheckedWritesCheckEval.ConstantsDecodeFromThePaddedSlotEncoding` (R08c): `constBytes` `0C 00 … 00` as `Int64` → 12; `FF FF 00 …` as `UInt16` → 65535; `01` as `Bool` → true; `61 62 63 00…` as `FixedStr(8)` → `"abc"`; the bytes past the slot width are ignored.
  - `CheckedWritesCheckEval.ReadsChangedComparesOnlyTheReadColumns`: before/after differ only in `tag` under `characters <= 12` → false; differ in `characters` → true; `before == nullptr` → true; a null bit flip of `characters` → true.
  - `CheckedWritesCheckEval.DeepTreeEvaluatesWithoutAllocation`: a 63-node balanced `and` tree under `rune::testing::AllocWindow` (`broker/libs/node/tests/support/AllocWindow.h`, linked into this test only, as 04 Task 4 does) → 0 allocations and the right truth value.
  - Compile rules, through `compileView` (07) on a source `{id UInt64 key, subject FixedStr(64), n Int64}`:
    - `CheckedWritesPlanCompile.CheckOnAWideColumnIsConstraintInvalid`: `check (subject = 'a')` → `CompileErrorKind::ConstraintInvalid`, message ends "constraint 'c1': check compares fixed-width columns of at most 16 bytes; 'subject' is string(64)";
    - `CheckedWritesPlanCompile.SixtyFivePredicateNodesAreRefused`: 65 nodes → "constraint 'c1': at most 64 predicate nodes"; 64 compile.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `rune/view/CheckEval.h: No such file or directory`.
- [ ] **Step 3: Implement** per the Interfaces. Integer types read `FieldValue::i` / `::u`, `Float64` `::f`, `Bool` `::b`, `FixedStr` `::bytes` (`Record.h:10-18`).
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesCheckEval|CheckedWritesPlanCompile|PlanCompiler' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/CheckEval.h broker/libs/view/src/CheckEval.cpp broker/libs/view/src/PlanCompiler.cpp \
          broker/libs/view/CMakeLists.txt broker/libs/view/tests/view/CheckedWritesCheckEvalTest.cpp
  git commit -m "feat(view): check predicates - three-valued evaluation on row images, padded slot constants, changed-column test, shape rules" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 2: `UniqueClaims` and the §8.3 rules in the keyed projection fold

**Files:**
- Create: `broker/libs/view/include/rune/view/UniqueClaims.h`, `broker/libs/view/src/UniqueClaims.cpp`, `broker/libs/view/tests/view/CheckedWritesUniqueFoldTest.cpp`
- Modify: `broker/libs/view/include/rune/view/RecordFold.h` (contract addition 1), `broker/libs/view/include/rune/view/ViewFold.h` (`ProjectionFold::claims`, `FoldStats::uniquePromotions`), `broker/libs/view/src/ViewFold.cpp` (07's keyed projection step: the unique branch replaces 07's P7-9 `keyConflicts` skip), `broker/libs/view/{include/rune/view/ViewCheckpoint.h,src/ViewCheckpoint.cpp}` (tables 6 and 7 in `encodeKeyedDelta` / `replayKeyed`), `broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp` (07's `UniqueConflictIsSkippedAndCounted` re-pinned, see Step 4), `broker/libs/view/CMakeLists.txt`

**Interfaces:**
- Consumes: 07's `KeyedFoldSpec`, `deriveViewKey`, `ViewTable` with `ViewTableKeys`, the owner map, `drainDirtyKeyed`; `rune::codec::encodeKeyComponent` (07 Task 2); 04's `prepareProjection` / `applyProjection`, `Prospect`, the one-step-two-stores pattern (P4-13).
- Produces: contract additions 1, 3 and 4. The fold rules on a view with `claims != nullptr` (w = the record's wire key, O = `owners.get(w)`, E = `claims.extraKeyOf(w)`); prepare computes them against an overlay, apply writes them, both without allocation:

  | Record | Case | Prospect | Apply |
  |---|---|---|---|
  | tombstone, or a live record with an `Absent` key | O set, N = O has extras | `Upsert` rows[0] {O, existed, promoted} | promote: s = `smallestExtra(O)`; row O := s's image; `setOwner(O, s)`; `owners[s] = O`; `removeExtra(s)`; erase `owners[w]`; `uniquePromotions` + 1 |
  | | O set, no extras | `Erase` rows[0] {O, removed} | erase row O, `eraseOwner(O)`, erase `owners[w]` |
  | | E set | `None` | `removeExtra(w)` |
  | | neither | `None` | — |
  | live, key N | O == N | `Upsert` rows[0] {N, existed} | as 07 |
  | | E == N | `None` (P9-3) | `updateExtraImage(w, image)` |
  | | N row absent | `Upsert` or `Move` (O's release as above in rows[1]) | release O or E as above, then put N, `setOwner(N, w)`, `owners[w] = N` |
  | | N owned by o ≠ w | rows[0] {N, existed, **ownedByOther**} | unjudged only (a judged record was rejected first): release O or E; then `compareSourceKeys(w, o) < 0` → `addExtra(N, o, row N bytes)`, row N := w's image, `setOwner(N, w)`, erase `owners[o]`, `owners[w] = N`; else `addExtra(N, w, image)` |

  - `smallestExtra` walks N's list and keeps the claimant with the smallest `compareSourceKeys`; ties cannot occur (distinct wire keys).
  - A unique view whose key is its source key (07 `ownerMappedOf` false) gets no claims and folds as 07 does.
  - **Checkpoint v3:** a unique instance writes tables 6 (`index`, key K) and 7 (`extras`, key W) in the same record as tables 0 and 1, so every table is restored at the same O. `recountAfterReplay` recomputes the gauge.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesUniqueFoldTest.cpp`; the `character` source of 07 Task 4, `character_name` = `unique (lower(name) as nameKey) select characterId, gender`; records folded with `foldBatch` (unjudged) unless named):
  - `CheckedWritesUniqueFold.SmallestSourceKeyOwnsAPreExistingDuplicate`: c9 `"foo"`, then c7 `"Foo"` → one row `"foo"` whose `characterId` slot is 7; `claims.extras() == 1`; `ownerOf("foo")` is c7's wire key.
  - `CheckedWritesUniqueFold.OwnerIsIndependentOfTheOrder`: the same three claimants in all 6 orders → byte-identical view, owner and claims tables.
  - `CheckedWritesUniqueFold.PromotionOnDelete`: then a tombstone of c7 → the row holds c9's image; `extras() == 0`; `uniquePromotions == 1`.
  - `CheckedWritesUniqueFold.PromotionOnRename`: c7 renamed `"Bar"` → rows `"bar"` (c7) and `"foo"` (c9); `owners[c7] == "bar"`.
  - `CheckedWritesUniqueFoldRegression.PromotionChainOfThreeClaimantsRegression` (Review Focus 1): c3, c7, c9 all `"foo"`; delete c3 → owner c7; delete c7 → owner c9; delete c9 → no row, `extras() == 0`, index empty.
  - `CheckedWritesUniqueFold.ExtraKeepsItsKeyAndItsLatestImageIsPromoted` (Review Focus 1): c9 (extra) updates `gender` keeping `"foo"` → the row is unchanged (c7's gender); then delete c7 → the row has c9's **new** gender. One batch `[c7 rename "Bar", c9 gender 2]` gives the same final tables as two batches.
  - `CheckedWritesUniqueFold.ExtraLeavesQuietly`: delete c9 (extra) → the row is unchanged, `extras() == 0`, `uniquePromotions == 0`.
  - `CheckedWritesUniqueFold.SourceKeysOrderByValue`: an `Int64` source key: −5 and 3 → −5 owns; `UInt64` 0x100 and 0xFF → 0xFF owns; a `string(16)` key `"ab"` and `"b"` → `"ab"` owns.
  - `CheckedWritesUniqueFold.PrepareFlagsOwnedByOtherAndWritesNothing`: c7 owns `"foo"`; `prepareProjection(c11 "FOO")` → rows[0] `ownedByOther`; every table byte-identical to before (snapshot).
  - `CheckedWritesUniqueFold.PrepareOfTheOwnKeyIsNotOwnedByOther`: `prepareProjection(c7 "FOO")` → `!ownedByOther`, `op == Upsert`.
  - `CheckedWritesUniqueFold.PrepareMarksAPromotionWithoutComputingIt`: c7 owner with extra c9; `prepareProjection(tombstone c7)` → rows[0] `{"foo", promoted}`; tables unchanged.
  - `CheckedWritesUniqueFold.CheckpointV3CarriesTheClaimsRegression`: fold 300 records with duplicates, drain the keyed dirty stream into one v3 record, `replayKeyed` into fresh tables → tables 0, 1, 6 and 7 byte-equal; the gauge equals `extras()` after `recountAfterReplay`.
  - `CheckedWritesUniqueFoldModelRegression.RandomLogsFoldToTheFreshRefold`: seeds 1–300; 40 source keys, names from 6 strings in mixed case; random creates, renames, deletes and absent-key versions across 1–20 batches with random pass bounds → the tables equal a fresh refold of the whole log, and equal a `std::map` model where each lowered name's owner is the smallest live claimant and the row holds that claimant's latest image.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `rune/view/UniqueClaims.h: No such file or directory`.
- [ ] **Step 3: Implement** per the Interfaces. The two tables are created in `UniqueClaims`'s constructor with `ViewTableKeys{K, 0, hashSeed}` and `ViewTableKeys{W, 0, hashSeed}`; payload lengths `W + 4 + W + 1` and `K + 2W + 1 + rowLen`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesUniqueFold|CheckedWritesKeyedFold|CheckedWritesViewCheckpointV3|ViewFold|ViewCheckpoint' -j14 --output-on-failure`. Expected: PASS. 07's `CheckedWritesKeyedFold.UniqueConflictIsSkippedAndCounted` becomes `CheckedWritesKeyedFold.UniqueConflictBecomesAnExtraClaimant` (c9 `"foo"` after c7 → `uniqueConflicts` 1, `keyConflicts` 0, the row still c7's). That is a re-pin by design (P7-9 says Part 09 replaces the skip), recorded in the commit body.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/UniqueClaims.h broker/libs/view/src/UniqueClaims.cpp \
          broker/libs/view/include/rune/view/RecordFold.h broker/libs/view/include/rune/view/ViewFold.h broker/libs/view/src/ViewFold.cpp \
          broker/libs/view/include/rune/view/ViewCheckpoint.h broker/libs/view/src/ViewCheckpoint.cpp broker/libs/view/CMakeLists.txt \
          broker/libs/view/tests/view/CheckedWritesUniqueFoldTest.cpp broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp
  git commit -m "feat(view): unique views - claims index, smallest source key owns, promotion of extras, v3 tables 6/7 (8.3)" \
             -m "Re-pin by design (07 P7-9): UniqueConflictIsSkippedAndCounted -> UniqueConflictBecomesAnExtraClaimant." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint A (end of Phase 1).**
  - `ctest --test-dir build/root -R 'View|Plan|Agg|CheckedWritesCheckEval|CheckedWritesUniqueFold|CheckedWritesKeyed|CheckedWritesPrepareApply' -j14 --output-on-failure`
  - `cmake --build build/asan -j14 --target rune_view_tests && ctest --test-dir build/asan -R 'CheckedWritesCheckEval|CheckedWritesUniqueFold|CheckedWritesKeyedFold' --output-on-failure`
  - Expected: PASS, no sanitizer report. Start the Task 12 baseline worktree build now (lean-bench rule).

---

## Phase 2 — The broker

### Task 3: The AdminWorker accepts `unique` and `check` (R08e, R10a, SD5)

**Files:**
- Create: `broker/libs/node/include/rune/node/ConstraintDdlRules.h`, `broker/libs/node/tests/node/CheckedWritesConstraintDdlTest.cpp` (in `rune_checked_writes_tests`, driving `CatalogDdlRig` with `becomeServingLeader()` as 02's `CheckedWritesDdlTest.cpp` does)
- Modify: `broker/libs/node/src/AdminWorker.cpp` (`viewDef_`, `:1211-1265` before 07: delete 07's step-3 pair; add P9-9 after 07's partition rule), `broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp` (07), `broker/libs/node/tests/node/CheckedWritesQlCompileTest.cpp` (08's QM2), `broker/libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: `featureRefusal`, `topicShapeOf`, `DbFeature::Constraint` (02); `kMaxVerdictViewsPerSource` (04); `verdictPlanPayloadBytes`, `kMaxVerdictPlanBytes` (Task 6 defines them in `ControlRecordLayout.h`; this task adds the two declarations and their trivial bodies there first); `s.views()` (`codec/include/rune/catalog/CatalogState.h:122`).
- Produces: contract addition 10, P9-9, and every constrained view's compiled plan carrying its `viewObjectId` (07 already sets it).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesConstraintDdl.ConstrainedViewRefusalBoundaries` (pure, `constrainedViewRefusal`): `(65535, 1, 2, 100)` → nullopt; `(65536, 1, 2, 100)` → "constraint ids exhausted"; `(7, 65535, 2, 100)` → nullopt; `(7, 65536, 2, 100)` → "constraint ids exhausted"; `(7, 1, 32, 100)` → nullopt; `(7, 1, 33, 100)` → "a topic feeds at most 32 verdict views (the key index counts as one)"; `(7, 1, 2, 65536)` → nullopt; `(7, 1, 2, 65537)` → "a constrained view's plan must encode in at most 65536 bytes".
  - `CheckedWritesConstraintDdl.RegressorsPathViewsAreCreated`: the golden script's topics and views, authored with `ql::applyViewPlan` (08) and submitted in order → every code 0; `character_name`'s compiled plan has `unique` and `viewObjectId ==` its event's `objectId`; `account_stats` has `constraints[0] == {ordinal 1, "max_characters"}`.
  - `CheckedWritesConstraintDdl.TransientSourceIsRefused` (R10a): a transient typed keyed topic `t` → `create unique view u key lower(name) as k as select id from t` → 39 "constraints need a persisted topic"; a check view over `t` → the same; a plain view over `t` → 0 (Part 10).
  - `CheckedWritesConstraintDdl.TransientConstrainedViewOverAPersistedSourceIsAllowed`: `create transient unique view …` over `character` → 0.
  - `CheckedWritesConstraintDdl.ThirtyThirdMemberIsRefused`: a `key.index` topic and 31 check views over it → each 0; the 32nd constrained view → 57 with the members message; a plain view → 0.
  - `CheckedWritesConstraintDdl.AlterKeepsTheObjectIdAndCountsItselfOnce`: `alter view account_stats` with two constraints → 0, `viewObjectId` unchanged, ordinals 1 and 2; at 32 members an alter of an existing member is still 0.
  - `CheckedWritesConstraintDdl.CheckOnAWideColumnIsViewConstraintInvalid`: `check (subject = 'a')` on `user_identity`'s source → 57 with Task 1's message as the detail.
  - `CheckedWritesConstraintDdl.BelowTheLevelIsInvalidRequest`: level 3 → a unique view is 39 "plan v2 needs the checked-writes feature level" (07's gate, unchanged).
  - Inversions (R08e):
    - `CheckedWritesAdminViewDdl.UniqueAndCheckAreNotAvailableYet` (07) becomes `CheckedWritesAdminViewDdl.UniqueAndCheckAreAccepted`: the same two submits → 0.
    - 08's `CheckedWritesQlCompileMatrix` unique admin cell (K5 × S1) now expects 0; `CheckedWritesQlCompileMatrixCount.CellCountIsPinned` stays 50.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `rune/node/ConstraintDdlRules.h: No such file`, then 58/57 where 0 is expected.
- [ ] **Step 3: Implement** per P9-9. `membersAfter` decodes each other view's `compiledPlanBlob` with `rune::view::deserializeCompiledViewPlan` (a cold leader build) and counts it when `verdictKindOf` is set and its `sourceNames.front()` equals this view's source. For an alter, the view's own `objectId` is skipped.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesConstraintDdl|CheckedWritesAdminViewDdl|CheckedWritesQlCompile|CreateView|AdminWorker|DdlOccupant' -j14 --output-on-failure` → PASS.
  - `grep -rn "not available yet" broker/libs/node/src` → no output.
  - `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure` → PASS (no catalog format change).
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/ConstraintDdlRules.h broker/libs/node/src/AdminWorker.cpp \
          broker/libs/storage/include/rune/storage/ControlRecordLayout.h \
          broker/libs/node/tests/node/CheckedWritesConstraintDdlTest.cpp broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp \
          broker/libs/node/tests/node/CheckedWritesQlCompileTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(admin): unique and check views accepted - transient sources refused (R10a), constraint id and member limits (SD5)" \
             -m "R08e: the P7-9 refusal lines are deleted; UniqueAndCheckAreNotAvailableYet and the QM2 unique cell are inverted." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 4: `Unique` and `Check` members in the `VerdictSource`

**Files:**
- Modify: `broker/libs/view/include/rune/view/Verdict.h` (contract addition 5), `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp` (the `switch (kind)` cases for `Unique` and `Check` in prepare, evaluate, apply and the member replay of `driveJudge`; `evaluateConstraints`; per-constraint stats)
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h`, `broker/libs/node/src/WorkerViewSet.cpp`:
  - `drainInstalls` (`:278-336` before 07): after `deserializeCompiledViewPlan`, set the template's `verdictKind` / `verdictObjectId` / `verdictFingerprint` (04's members) from `verdictKindOf`, `plan.viewObjectId` and `verdictPlanFingerprint(m.planBlob)`, and build `planRecordPayload` (P9-8) from the message's three blobs;
  - `getOrCreate_` (`:338-422`): a unique owner-mapped instance gets `claims` (seeded like its tables, 07 P7-4) and `ProjectionFold::claims`;
  - `buildAndPushCheckpoint_`, `replayInstance`, `maxRecordsPerPassFor`: tables 6 and 7 (per record add `2 × (5 + K + indexLen) + 2 × (5 + W + extraLen)`);
  - after each pass of a unique instance, `counters.uniqueConflicts = claims->extras()`; `uniquePromotions` summed from `FoldStats`.
- Create: `broker/libs/node/tests/node/CheckedWritesConstraintVerdictTest.cpp` (in `rune_checked_writes_tests`)
- Modify: `broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp`, `broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp` (07)

**Interfaces:**
- Consumes: Tasks 1–2; 04's `judge`, `addVerdictView`, `Prospect`, the unit harness of 04 Task 5 (members as `ViewInstance`s built like `WorkerViewSetTest.cpp:100-170`, a fake `readRange` over `InMemoryLog` batches); 05's key index member; 07's keyed and aggregate prepare/apply.
- Produces: contract additions 5 and 6 (`ConstraintFold`, `evaluateConstraints`). In `judge`, a `Unique` or `Check` member is prepared with its view's own prepare (`prepareProjection` or `prepareAggregate`, with `claims` for unique views), evaluated with `evaluateConstraints(fold, rec, prospect)` (P9-2, P9-3, P9-4: `before` = `fold.table.get(rows[i].key)` when `rows[i].existed`, else nullptr), and applied with its own apply. Its replay below K is its own fold with `FoldSkip{.aborted, .rejected}` (unjudged, claims taken as P9-3's unjudged column). A rejection bumps `rejectionsConstraint` and `perConstraint[constraintId]` (04).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesConstraintVerdictTest.cpp`; compiled plans from Task 3's statements, `character_name` objectId 7 and `account_stats` objectId 8 unless named; the key index (05) is member 0 wherever the topic is the autoincrement `character`):
  - `CheckedWritesConstraintVerdict.FooThenLowerFooIsRejectedByTheUniqueView`: c1 `"Foo"` (user 1) → OK; c2 `"foo"` (user 2) → `{51, 0x00070000}`; `character_name` has 1 row; `account_stats` user 2 absent; `perConstraint` holds `(0x00070000, 1)`.
  - `CheckedWritesConstraintVerdict.RenameToTheOwnNameInAnotherCasePasses`: c1 `"FOO"` → OK, no move.
  - `CheckedWritesConstraintVerdict.TwelfthPassesThirteenthIsRejected`: 13 creates for user 7 → 12 OK, the 13th `{51, 0x00080001}`; `characters == 12`.
  - `CheckedWritesConstraintVerdict.OverLimitGroupRenamePassesCreateFailsDeletePasses` (§15): user 7 at 14, folded before the members joined (unjudged); a rename of one of 7's characters → OK; a create → `{51, 0x00080001}`; a delete → OK (13); a create → 51 (13 → 14 still fails); two deletes (11) and a create → OK (12).
  - `CheckedWritesConstraintVerdict.DeleteAlwaysPassesBalanceLedger` (§15; `Ledger` view objectId 9): entries `+10`, `−4` (balance 6); delete `+10` → OK (balance −4, failing); a `−1` debit → `{51, 0x00090001}`; a `+2` credit (−2, still failing, changed) → 51; a `+5` credit (1) → OK; a `memo` rename of the `−4` entry → OK.
  - `CheckedWritesConstraintVerdictRegression.MoveOutOfAnOverLimitGroupIsRejectedRegression` (Review Focus 4): user 7 at 14 (pre-existing), move one of 7's to user 8 (at 3) → 51 `0x00080001`; a move from user 8 to user 9 at 11 → OK; then from user 8 to user 9 at 12 → 51.
  - `CheckedWritesConstraintVerdict.DeleteMakesRoomInTheSameBatch` (Review Focus 4): one batch `[remove c (user 7 at 12), create for 7]` → OK, OK; one batch `[remove c1 "Foo", create "foo" for user 2]` → OK, OK.
  - `CheckedWritesConstraintVerdict.SelfCollidingBatchJudgesInOrder`: `[create "Bar", create "bar"]` → OK, `{51, 0x00070000}`.
  - `CheckedWritesConstraintVerdict.SeveralVerdictViewsOneRejectsNoViewChanges` (§15): key index + 7 + 8, user 7 at 11; a create named `"Foo"` (taken) → `{51, 0x00070000}`; `account_stats` user 7 still 11, the key index lacks the id, `character_name` byte-identical (snapshot); an explicit insert of an existing id → `{52, 0}` (the key index is first).
  - `CheckedWritesConstraintVerdict.UniqueBeforeItsChecksInsideOneMember`: a unique view with `check (gender <= 3)`: a record that breaks both → `{51, objectId << 16 | 0}`; one that breaks only the check → ordinal 1.
  - `CheckedWritesConstraintVerdict.TombstonesAreNeverRejected`: tombstones under every view above → OK; `removeExisting` of an absent id → `{53, 0}` (the key index, never 51).
  - `CheckedWritesConstraintVerdict.CheckOverACompositeGroupBy` (R07c; `PerGender` objectId 10): user 7 with two characters of gender 1 → a third of gender 1 → `{51, 0x000A0001}`; one of gender 2 → OK.
  - `CheckedWritesConstraintVerdict.CheckOnAProjectionColumn`: `create view lp key characterId as select lastPlayed from character constraint nonneg check (lastPlayed >= 0)` → an update with `lastPlayed −1` → 51; the delete of that character → OK.
  - `CheckedWritesConstraintVerdictRegression.SumOverflowNullNeverRejectsRegression` (Review Focus 3): `sum(amount)` overflowing `Int64` emits NULL (`aggOverflow`) under `check (balance <= 5)` → OK (Unknown passes).
  - `CheckedWritesConstraintVerdict.UnjudgedReplayBelowKNeverRejects`: 30 records with duplicates and user 7 at 14 below the members' change record → the replay applies them all, `claims.extras()` equals the duplicates, no index entry is added.
  - `CheckedWritesViewSet.ConstrainedTemplatesCarryKindObjectIdAndFingerprint` (`CheckedWritesViewSetTest.cpp`): installing `character_name`, `account_stats` and a plain view → kinds `Unique`, `Check`, none; object ids from the plans; fingerprints equal `verdictPlanFingerprint` of the blobs; a replaced plan (new `viewTopicId`) has a different fingerprint.
  - `CheckedWritesKeyedRecoveryRegression.UniqueClaimsSurviveRestartRegression` (`CheckedWritesKeyedRecoveryTest.cpp`): 1,000 records with duplicates folded and checkpointed → `installViewForRecovery` on a fresh set gives byte-equal tables 0, 1, 6 and 7 and the same `uniqueConflicts`; a promotion after the restart picks the same claimant as a fresh refold.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'verdictKindOf'`.
- [ ] **Step 3: Implement** per the Interfaces. Member prospects are reserved at `addVerdictView` (cold) with the view's `rowLen` and aggregate layout, as 04 reserves them; `evaluateConstraints` allocates nothing.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesConstraintVerdict|CheckedWritesVerdictSource|CheckedWritesKeyIndex\.|CheckedWritesViewSet|CheckedWritesKeyedRecovery|WorkerViewSet|ViewRecovery' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/Verdict.h broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp \
          broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
          broker/libs/node/tests/node/CheckedWritesConstraintVerdictTest.cpp broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp \
          broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): unique and check verdict members - evaluated on prospects, deletes never rejected, claims checkpointed (8.2)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 5: Worker wiring: create, replace, drop under writes; requirements per write; stat 32

**Files:**
- Modify: `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp` (`addCandidate`, `removeCandidate`, `candidateCaughtUp`; candidates replayed in `driveJudge`'s replay step, bounded by `judgedThrough()`, with `FoldSkip{.aborted, .rejected}`; `candidateSwaps`)
- Modify: `broker/libs/node/src/WorkerViewSet.cpp` (the desired set, P9-5: for each `viewObjectId`, the non-retiring template if its fingerprint is logged or it is a create, else, while it is a candidate that has not caught up, the logged member; a non-retiring template whose `viewObjectId` has another fingerprint in `latest()` is registered with `addCandidate` instead of `addVerdictView`), `broker/libs/node/include/rune/node/WorkerViewSet.h` (`candidateForTest`)
- Modify: `broker/libs/node/include/rune/node/StatsCollect.h` (contract addition 11), `broker/libs/node/include/rune/node/Worker.h` (`executeGetStats_` passes `uniqueView = claims.has_value()`)
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (contract addition 13's rig helpers)
- Create: `broker/libs/node/tests/node/CheckedWritesConstraintWorkerTest.cpp` (in `rune_checked_writes_verdict_tests`)

**Interfaces:**
- Consumes: Tasks 3 and 4; 04's rig (`driveUntilJudged`, `failover`, `restart`, `compact`, `subscribe`/`collect`, `ack(corr)`, `holdJudge`, `followerFetch`), 06's `createAutoIncTopic` plumbing (reused by `createCharacterTopic`), 07's `createView(planV2)` and `getViewRowK(view, key, minOffset)`, 08's `getStats`.
- Produces: P9-5 on the worker; stat 32; the rig helpers every later task uses.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesConstraintWorkerTest.cpp`; `createCharacterTopic()`, `createConstrainedView(CharacterName)`, `createConstrainedView(AccountStats)`, an idempotent v12 `All | View` producer unless named):
  - `CheckedWritesConstraintWorker.LoginRaceLoserReadsTheWinner` (§15; topic `user {userId uint64 key autoincrement, provider uint16, subject string(64), createdAt int64}` and `user_identity`): two producers each create `(1, "steam:42")` in the same pass → exactly one OK with an assigned id k, the other `{51, objectId << 16}`; the loser's `getViewRowK(user_identity, key(1, "steam:42"), minOffset = its offset)` returns `userId == k` on its first serve.
  - `CheckedWritesConstraintWorker.CreateOnANonEmptyTopicJudgesFromTheChangeRecord` (§8.7 create, §15): 500 characters first, among them `"Dup"` (c5) / `"dup"` (c9) and user 7 at 14; then both views are created (change record at A); 3 more writes during the build are acked at the HW (`acks 1`) and never refused; a raw subscription from 0 receives records below A as soon as they commit, and records above A only after both members reached `A + 1`; afterwards `"dup"` is owned by c5, `uniqueConflicts(CharacterName) == 1`, user 7 shows 14; a create for 7 → 51.
  - `CheckedWritesConstraintWorker.ReplaceSwapsAtSWithoutAGap` (§8.7 replace, §15): a background producer writes creates for 20 users; `replaceConstrainedView(AccountStats, 13)` → `candidateForTest(objectId)` holds while the candidate builds; `judgedThrough()` keeps advancing during the build (it never stalls for more than 3 passes); exactly one change record names the new fingerprint, at S; every record below S is judged with 12 and every one above with 13; `candidateSwaps == 1`; the old template is still installed as retiring right after the swap.
  - `CheckedWritesConstraintWorker.ReplaceCandidateNeverHoldsARejectedRecord`: the 13th create rejected under 12 before the replace → the candidate's `account_stats` row for that user is 12, and stays 12 after the swap.
  - `CheckedWritesConstraintWorker.DropStopsJudgingAboveD` (§8.7 drop, §15): `"Foo"` taken; `dropConstrainedView(CharacterName)` while a producer writes → records below D stay rejected (index unchanged), and a `"foo"` create above D → OK.
  - `CheckedWritesConstraintWorker.DropThenCreateLeavesAGap`: drop, a `"foo"` duplicate written, then create `character_name` again → the duplicate is accepted (unjudged below A'), `"foo"` is owned by the smaller id, `uniqueConflicts == 1`.
  - `CheckedWritesConstraintWorker.ConstrainedSourceRefusesTransactions` (§15, 03 rule 7 for real): a transactional batch to `character` → `InvalidRequest`; the same to an unconstrained typed topic → OK as today.
  - `CheckedWritesConstraintWorker.KeyZeroCreateNeedsViewOnAConstrainedSource` (06 `KeyZeroCreateNeedsView` for real): `acks 1` → 39; `All | View` → OK with its id.
  - `CheckedWritesConstraintWorker.PlainWritesFromAnyProducerAreJudged` (§11.2, P5-12): a non-autoincrement topic `pc {id uint64 key, userId uint64, name string(16)}` with a unique `lower(name)` view; a non-idempotent `acks=leader` producer and a v11 session each upsert `"Foo"` then `"foo"` under other ids → their acks are today's (no trailer, no 51); only the first `"Foo"` is in the view and reaches a consumer; `rejectionsConstraint == 2`.
  - `CheckedWritesConstraintWorker.ViewAckCarriesPerRecordVerdicts`: one 8-record batch where record 2 takes a taken name and record 5 is user 7's 13th → one OK ack whose trailer is exactly `{(2, 51, 0x…0000), (5, 51, 0x…0001)}` with the two views' ids; the other 6 are visible.
  - `CheckedWritesConstraintWorkerRegression.ViewRetryReplaysTheOriginalConstraintIdsAfterAReplaceRegression` (R04e / R04g, Review Focus 5): the batch above with its ack dropped; `replaceConstrainedView(AccountStats, 13)`; the retry (same pid, epoch, sequence) → the same base offset and a byte-identical trailer (the original ids), released only once `W > lastOffset`; `verdictReplays == 1`; LEO unchanged.
  - `CheckedWritesConstraintWorkerRegression.PreExistingDuplicatesSurviveCompactionAndRefoldRegression` (§15): duplicates c5 / c9 / c12 before the view; `compact()` with `deleteRetentionMs = 0`; delete c5; `restart()` with `character_name`'s checkpoint file removed (a full refold) → owner c9, then `failover()` → owner c9; `uniqueConflicts == 1` throughout.
  - `CheckedWritesConstraintWorker.LedgerDeleteLeavesItFailingThenOnlyRepairsPass` (§15 through the worker): Task 4's ledger sequence through `createLedgerTopic()` and acks → the same verdicts, and a consumer never sees the two rejected entries.
  - `CheckedWritesConstraintWorker.UniqueConflictsStatIsServed` (R08f): with c5 / c9 duplicates, `getStats(ViewPartition, character_name, 0)` → ids `32, 33, 34, 35, 36` in that order with `32 == 1`; after deleting c5 → `32 == 0`; `getStats` of `account_stats` has no id 32.
  - `CheckedWritesConstraintWorker.TopicStatsListEveryConstraint`: after one rejection by each view → `getStats(TopicPartition, character, 0)` has `3 == 2` and the constraint list `{(cidName, 1), (cidMax, 1)}`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests && ctest --test-dir build/root -R CheckedWritesConstraintWorker --output-on-failure`. Expected: FAIL (`createCharacterTopic` missing, then a replace that judges with an empty candidate).
- [ ] **Step 3: Implement** per the Interfaces. `candidateCaughtUp` is `instance.foldCursor >= judgedThrough()`. At the swap step, `requestChange(desired)` runs and, if it returns `kChangeDeferred`, the candidate stays a candidate and the swap is retried next pass.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests rune_checked_writes_tests && ctest --test-dir build/root -R 'CheckedWritesConstraint|CheckedWritesVerdict|CheckedWritesKeyIndex|CheckedWritesAutoInc|CheckedWritesStats|CheckedWritesViewAck' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp \
          broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
          broker/libs/node/include/rune/node/StatsCollect.h broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesConstraintWorkerTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): constraint create/replace/drop under writes - candidates swap at S, plain writes judged, stat 32 (8.7)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 6: Two-phase drop and the retained plan record (R04a)

**Files:**
- Modify: `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (contract addition 8; `recordType` accepts 20), `broker/libs/storage/include/rune/storage/VerdictCleanState.h` (`keepsControlRecord(offset, kVerdictPlan)` → true)
- Modify: `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp` (`RetainedVerdictPlans`, `leaveOffsetOf`; `verdictFeedBatch` stores type 20; `verdictFeedBegin` truncates it; `requestChange` appends a type-20 record per joining Unique/Check member before the change batch, from the member's `planRecordPayload`, into a scratch reserved cold at `addVerdictView` to `verdictPlanBatchBytes(payload.size())`)
- Modify: `broker/libs/node/include/rune/node/WriteState.h` (`retainedPlans`)
- Modify: `broker/libs/node/src/WorkerViewSet.cpp` (`installRetainedPlan_`; `retireVerdictTemplates_` per P9-6), `broker/libs/node/include/rune/node/WorkerViewSet.h` (`retiringVerdictTemplatesForTest`), `broker/libs/node/include/rune/node/Worker.h` (`rebindVerdictSources_` (04) calls `installRetainedPlan_` for a waiting source and `retireVerdictTemplates_` once per pass while any retiring template exists; activation calls `retainedPlans.pruneLeftBelow(W, timeline)`)
- Modify: `broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp`, `broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp`, `broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp` (04's reader sweep)
- Create: `broker/libs/node/tests/node/CheckedWritesConstraintChangeTest.cpp` (in `rune_checked_writes_verdict_tests`)

**Interfaces:**
- Consumes: Tasks 4 and 5; 04's `RcVerdictHooks` feed, `waitingForInstall()`, the activation (K, discard rule, replay to K), `MarkerLedger`, `appendControlBatch(span, false)`; 04's rig `failover()`, `restart()` with `RigOptions::skipReinstall` (a node whose catalog no longer installs the dropped view).
- Produces: contract additions 6 (`RetainedVerdictPlans`, `leaveOffsetOf`), 7, 8, 9; P9-6, P9-7, P9-8.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesControlRecordTest.cpp`:
    - `CheckedWritesControlRecord.VerdictPlanRoundTrips`: header `(7, 0xF00D, Unique)` and blobs of 300, 120 and 90 bytes → `recordType == 20`, `parseVerdictPlan` gives the same header and spans.
    - `CheckedWritesControlRecord.VerdictPlanOverTheCapIsRefused`: a payload of 65,537 bytes → `buildVerdictPlan` returns 0; 65,536 builds.
  - `CheckedWritesControlRecordRegression.EveryLogReaderSkipsTypes16To19Regression` (04) is parameterized over types 16–20 and renamed `…SkipsTypes16To20Regression`: each of the nine readers skips a type-20 batch exactly as 16–19.
  - `CheckedWritesVerdictCleaner.VerdictPlanRecordsAreAlwaysKept`: a type-20 record whose member left long below W → present after any number of passes.
  - `CheckedWritesConstraintChangeTest.cpp` (rig of Task 5):
    - `CheckedWritesConstraintChange.CreateAppendsThePlanBeforeTheChange`: after `createConstrainedView(CharacterName)` → the log holds a type-20 record at `A − 1` and the change at A; its blobs equal the template's; `createConstrainedView(AccountStats)` adds one more; the key index member has none.
    - `CheckedWritesConstraintChange.RetiringTemplateStaysUntilWPassesTheDrop` (R04a phase 2): drop `character_name`; drive until the judge passed D with the follower held (W ≤ D) → `retiringVerdictTemplatesForTest() == 1` and its instance is alive; `followerFetch()` and drive → 0.
    - `CheckedWritesConstraintChangeRegression.FailoverDuringDropFinishesTheDropRegression` (**R04a's test**, Review Focus 2): records judged by `character_name` up to the drop D; the marker covering them above the HW (W < D); `failover()` with `skipReinstall` (the new incarnation's catalog has no `character_name`) → `retainedPlanInstalls == 1`; the judge re-judges `[K, D)` with the retained plan and its verdicts equal an undisturbed rig's (entry by entry); it passes D; after `followerFetch()` W > D and the template is gone; `driveUntilJudged(LEO)` is true within its cap; `waitingForInstall()` is false at the end; a consumer from 0 receives every accepted record and no rejected one.
    - `CheckedWritesConstraintChangeRegression.FailoverAfterTheDropRecordWasCutRegression`: the drop change D appended but above the HW, then `failover()` with `skipReinstall` (D is truncated) → the new leader judges with the retained plan, finds the desired set without it and appends its own drop D′, passes it, and uninstalls once W > D′.
    - `CheckedWritesConstraintChangeRegression.RestartDuringReplaceUsesTheRetainedOldPlanRegression`: a replace whose swap S committed while W < S, then `restart()` installing only the new plan → the old fingerprint is installed from the log, judges `[K, S)`, the new one above S; verdicts equal an undisturbed rig's.
    - `CheckedWritesConstraintChangeRegression.NothingWaitsForeverAfterAnyDropTimingRegression` (`TEST_P` over {before D committed, D committed and the judge below it, the judge past D and W below it, W past D} × {`failover()`, `restart()`}, all with `skipReinstall`) → `driveUntilJudged(LEO)` within the cap, `waitingForInstall()` false, no retiring template once W > D, `mismatches() == 0`.
    - `CheckedWritesConstraintChange.RetainedPlansArePrunedAfterW`: after a drop and W > D, a new leadership's activation leaves `retainedPlans.size()` without the dropped member's entry; the log still holds the record.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_storage_tests rune_checked_writes_tests rune_checked_writes_verdict_tests`. Expected: `kVerdictPlan` undeclared, then `FailoverDuringDropFinishesTheDropRegression` fails its cap with `waitingForInstall()` true.
- [ ] **Step 3: Implement** per the Interfaces. `installRetainedPlan_` deserializes the plan and the two descriptors, builds the layout, and installs the template `persisted = false`, `retiring = true`, `fromRetainedPlan = true` (a deserialize failure logs `spdlog::critical("VERDICT-PLAN-UNREADABLE view={} fp={:#x}")` once and leaves the judge waiting, which is 04's safe behaviour).
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesControlRecord|CheckedWritesVerdictCleaner|CheckedWritesConstraintChange|CheckedWritesConstraintWorker|CheckedWritesVerdict|LogCleaner|RcState' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/storage/include/rune/storage/ControlRecordLayout.h broker/libs/storage/include/rune/storage/VerdictCleanState.h \
          broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp broker/libs/node/include/rune/node/WriteState.h \
          broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp broker/libs/node/include/rune/node/Worker.h \
          broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp \
          broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp broker/libs/node/tests/node/CheckedWritesConstraintChangeTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): two-phase drop - plans stay until W passes the change; kVerdictPlan (type 20) makes them durable across restart (R04a)" \
             -m "Deviation (04 P4-6): a leaving member is uninstalled once W, not the judge, passes its leave offset; followers keep it." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 7: `Client::constraintName`, the rejected record's offset, and names in the CLI

**Files:**
- Create: `client-cpp/include/rune/client/ConstraintNames.h`, `client-cpp/src/ConstraintNames.cpp` (in the client library sources of `client-cpp/CMakeLists.txt`), `client-cpp/tests/client/CheckedWritesConstraintNameTest.cpp` (in `rune_client_tests`, `client-cpp/CMakeLists.txt:1059`)
- Modify: `client-cpp/include/rune/client/WriteResult.h` (contract addition 12), `client-cpp/include/rune/client/Client.h` (beside `catalogReader()`, `:997`), `client-cpp/src/Client.cpp` (beside `catalogReader()`, `:2479`), `client-cpp/src/producer/ProducerImpl.cpp` (04's per-record override in `completeSlice_`: a rejected record keeps `value = base + recordIndex`), `client-cpp/src/cli/CatalogCommands.cpp` (08's `formatStatsText` / `formatStatsJson` take an optional name resolver), `client-cpp/tests/producer/ProducerVerdictsTest.cpp` (04), `client-cpp/tests/CheckedWritesQlCliTest.cpp` (08)

**Interfaces:**
- Consumes: `CatalogReader::state()`, `events()`, `refresh(timeout)` (`client-cpp/include/rune/client/detail/EventLogStream.h:143-151`); `rune::catalog::CatalogState::viewByObjectId`, `RecordedEvent` (`codec/include/rune/catalog/CatalogReplayer.h:26`), `CreateViewOps` / `AlterViewOps`; `rune::view::deserializeAuthored` (07, codec); 05's `WriteResult::offset()`, `fromCompletion`.
- Produces: contract addition 12, P9-11, P9-12. The CLI prints a constraint as `<view>.<constraint>` (`character_name.unique`, `account_stats.max_characters`), and an unresolved one as its hex id.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesConstraintNameTest.cpp` (catalog states built by replaying hand-made events through `CatalogReplayer`, as `codec/tests/catalog` builds them; `character_name` gets object id X, `account_stats` Y, with constraints `max_characters` and `max_level`):
    - `CheckedWritesConstraintName.ResolvesUniqueAndCheckOrdinals`: `X << 16` → `{"character_name", "unique"}`; `Y << 16 | 1` → `{"account_stats", "max_characters"}`; `Y << 16 | 2` → `{"account_stats", "max_level"}`.
    - `CheckedWritesConstraintName.UnknownIdsAreNone`: `0` (the key index) → nullopt; an object id that is a topic → nullopt; `Y << 16 | 3` → nullopt; `Y << 16` (ordinal 0 on a non-unique view) → nullopt.
    - `CheckedWritesConstraintNameRegression.ReplayedIdResolvesAfterReplaceAndDropRegression` (Review Focus 5): `alter view account_stats` that keeps only `max_level` (now ordinal 1), then `drop view character_name` → `Y << 16 | 1` → `{"account_stats", "max_level"}` (the live definition); `Y << 16 | 2` → `{"account_stats", "max_level"}` from the earlier definition, found in the events; `X << 16` → `{"character_name", "unique"}` from the dropped view's `CreateView` event.
  - `ProducerVerdictsTest.cpp`: `ProducerVerdicts.RejectedRecordCarriesItsOffset`: base 100, trailer `{(3, 51, 0x00070000)}` → record 3's completion has `value == 103`, and `WriteResult::fromCompletion(c).offset() == 103`, `code() == 51`, `constraintId() == 0x00070000`.
  - `CheckedWritesQlCliTest.cpp`: `CheckedWritesQlCli.StatsTextNamesConstraints`: `formatStatsText` of a `StatsResult` with constraints `{(X << 16, 2), (0x00990001, 1)}` and a resolver knowing X → lines `rejections character_name.unique 2` and `rejections 0x00990001 1`; `formatStatsJson` has `"constraints": [{"id": …, "name": "character_name.unique", "rejections": 2}, …]`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_client_tests rune_invoker_tests rune_cli_catalog_tests`. Expected: `rune/client/ConstraintNames.h: No such file or directory`.
- [ ] **Step 3: Implement** per P9-12. `Client::constraintName(id)` calls the two-argument form with `kConstraintNameRefreshTimeout`. A call from another thread throws `std::logic_error` (the catalog reader's rule). `cmdStats` passes a resolver that calls `client.constraintName`.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesConstraintName|ProducerVerdicts|CheckedWritesQlCli|CatalogReader' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/ConstraintNames.h client-cpp/src/ConstraintNames.cpp client-cpp/include/rune/client/WriteResult.h \
          client-cpp/include/rune/client/Client.h client-cpp/src/Client.cpp client-cpp/src/producer/ProducerImpl.cpp \
          client-cpp/src/cli/CatalogCommands.cpp client-cpp/CMakeLists.txt client-cpp/tests/client/CheckedWritesConstraintNameTest.cpp \
          client-cpp/tests/producer/ProducerVerdictsTest.cpp client-cpp/tests/CheckedWritesQlCliTest.cpp
  git commit -m "feat(client): Client::constraintName from the catalog (dropped and replaced views too); rejected records carry their offset" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint B (end of Phase 2).**
  - `cmake --build build/root -j14 && ctest --test-dir build/root -R 'CheckedWrites|View|Plan|AdminWorker|CreateView|LogCleaner|RcState' -j14 --output-on-failure`, then `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure --repeat until-fail:3`.
  - `ctest --test-dir build/sub-client-cpp -R 'CheckedWrites|Producer' -j14 --output-on-failure`.
  - `cmake --build build/asan -j14 --target rune_checked_writes_tests rune_checked_writes_verdict_tests rune_storage_tests && ctest --test-dir build/asan -R 'CheckedWritesConstraint|CheckedWritesControlRecord|CheckedWritesVerdictCleaner' --output-on-failure`; the same on `build/tsan`.
  - `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure`.
  - Expected: 100 % pass, no sanitizer report.

---

## Phase 3 — Matrix, stress, the real broker, the docker e2e, benches and docs

### Task 8: The shared matrix rows for 09

**Files:**
- Modify: `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h` (`Op` 80–86, `Entity` 80–90, `expect09`, `cellsFor(Part::P09)`), `broker/libs/node/tests/support/CheckedWritesModel.h` (contract addition 13's model rules), `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp` (the `Cell09` instantiation, the pin row), `broker/libs/node/tests/support/CheckedWritesRig.h` (the entity setups)

**Interfaces:**
- Consumes: Task 5's rig helpers; 04's model, `TEST_P(CheckedWritesMatrix, Holds)` and the pin; 05's `addKeyIndexRule`; `rune::codec::lowerUtf8` (07) in the model.
- Produces: Interaction matrix A.
  - **The model's rules** are written independently of `VerdictSource` and `UniqueClaims`: `std::map<std::string, std::set<uint64_t>>` claimants per lowered name and `std::map<uint64_t, Live>` live characters, updated in offset order as the model accepts records (unjudged below a member's change record, as P9-3 says). The unique rule rejects a live record whose lowered name has an owner that is not the record's id and of which the record is not already an extra. The count rule computes the before and after count of every group the record touches and applies P9-4. Rules run in ascending object id, after the key index; the first failure wins and nothing applies on a reject.

- [ ] **Step 1: Write the matrix.** `INSTANTIATE_TEST_SUITE_P(Cell09, CheckedWritesMatrix, ValuesIn(cellsFor(Part::P09)))` runs the shared `TEST_P(CheckedWritesMatrix, Holds)`, which dispatches on the op's range to 09's driver: it sets up the entity, runs the op under the timing, and asserts 04's (a)–(g) plus (h)–(k) of Interaction matrix A. N/A cells `GTEST_SKIP()` with their reason. The pin row is `EXPECT_EQ(applicable(cellsFor(Part::P09)), 600u); EXPECT_EQ(skipped(cellsFor(Part::P09)), 555u);`. `CheckedWritesModel.ConstraintRulesMatchTheTable` asserts the model gives the `Single` table of `expect09` for all 40 op × entity pairs.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_checked_writes_matrix_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix|CheckedWritesModel\.' -j14 --output-on-failure`. Expected: the 09 cells give 600 pass and 555 skip; rows 03–07 unchanged. Mutation checks (do each, then revert):
  - make `evaluateConstraints` ignore `predicateReadsChanged` → every `CRename × Over*` and `CUpdateNonKey × Over*` cell is red;
  - make `smallestExtra` return the largest claimant → every `DupOwner × {CRename, CRemove, CRemoveExisting, CDeleteThenReuse}` cell is red;
  - reject an extra that keeps its key → every `CUpdateNonKey × DupExtra` cell is red;
  - evaluate checks on `promoted` rows → no cell may turn red (the promoted row is another record's data); if one does, the rig's setup is wrong.
- [ ] **Step 3: Commit.**
  ```bash
  git add broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h broker/libs/node/tests/support/CheckedWritesModel.h \
          broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp broker/libs/node/tests/support/CheckedWritesRig.h
  git commit -m "test(node): checked-writes matrix rows for view constraints (09: 600 cells) and the model's unique and count rules" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 9: The constraint model check and the RegressorsPath cluster stress

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesModelTest.cpp`, `broker/libs/node/tests/support/CheckedWritesStressDriver.h` (`setUpRegressorsPath()`, the actions of Interaction matrix D, the two invariants), `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp`
- Create: `broker/libs/node/tests/client/support/RegressorsPathRecords.h` (shared with Tasks 10 and 11)

**Interfaces:**
- Consumes: Tasks 5–8; 04's `CheckedWritesStressDriver::registerInvariant`, 06's `NoIdTwiceNoReuse`; `ThreeNodes` (`broker/libs/node/tests/support/CatalogBrokerCluster.h:160`).
- Produces: Interaction matrix C and D; the invariants `AtMostTwelveLiveCharacters` and `LowerNameUnique` registered by name.
  - `RegressorsPathRecords.h`: hand-written `RpUser {userId uint64 key, provider uint16, subject string(64), createdAt int64}` and `RpCharacter {characterId uint64 key, userId uint64, name string(16), gender uint16, hairSkin uint16, hairSheet uint16, headSkin uint16, createdAt int64, lastPlayed int64}` following the typed-record contract of `ClientAuthClusterRegressionTest.cpp:64` (as 05's `KeyIndexRecords.h` does).

- [ ] **Step 1: Write the model check.** `CheckedWritesModelRegression.SeededConstraintsMatchTheModel` (seeds 1–200 × 250 steps at cap 3, and seeds 1–20 × 250 at cap 12) with the actions and invariants of Interaction matrix C, checked after every step; `CheckedWritesModelRegression.SameSeedSameTrace` for seeds 1–8. On failure `SCOPED_TRACE("seed=" … " step=" … " action=" …)`.
- [ ] **Step 2: Write the cluster stress.** `CheckedWritesStressRegression.SeededRegressorsPathHoldsTheInvariants` (seeds 1–12 × 300 steps) per Interaction matrix D. The DDL comes from `regressorspath.rql` run through `rune::cli::run` (as 08's Task 10 does), with `replicas 3`. Every wait is a bounded poll (10 s) and the deadline is the failure; the seed is printed on failure.
- [ ] **Step 3: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesModelRegression' -j14 --output-on-failure`. Expected: PASS.
  - `ctest --test-dir build/root -R 'CheckedWritesStressRegression' --output-on-failure`. Expected: PASS (04's, 05's, 06's and this part's seeds).
  - Mutation checks (each, then revert):
    - apply a member on a reject → `ViewEqualsFreshRefold` or `LowerNameUnique` red;
    - judge with the check's `before` taken after apply → `AtMostTwelveLiveCharacters` red within the seeds.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesModelTest.cpp broker/libs/node/tests/support/CheckedWritesStressDriver.h \
          broker/libs/node/tests/integration/CheckedWritesStressTest.cpp broker/libs/node/tests/client/support/RegressorsPathRecords.h
  git commit -m "test(node): view-constraint model check and the RegressorsPath ThreeNodes stress (AtMostTwelveLiveCharacters, LowerNameUnique)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 10: The real e2e: the RegressorsPath scenario on real brokers

**Files:**
- Create: `broker/libs/node/tests/client/CheckedWritesRegressorsPathRegressionTest.cpp`; modify `broker/libs/node/CMakeLists.txt:1731-1751` (add it to `rune_client_broker_tests`)
- Modify: `broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp` (08's `RegressorsPathOutcomesBeforePart09ThreeNodesRegression`, inverted and renamed)

**Interfaces:**
- Consumes: everything above; `ThreeNodes::start`, `crash`, `restart`, `restartAll`, `onOwnerThreadForTest`, `onWorkerThreadForTest` (`BrokerNode.h:1467`, `:1553-1559`); 05's `TypedTopic` intents and `WriteResult`; 06's `insertAndWait(...).key()`; 07's `ViewKeyBuilder`, `getViewRowAndWait(view, key, {.minOffset})`, `scanViewAndWait`; 08's `rune::cli::run` and `RUNE_QL_GOLDEN_DIR`. No new production code. Every wait is `waitFor(…, 10s)`.

- [ ] **Step 1: Write the tests** (`ThreeNodes::start("cw09", configTweak)` with 64 KiB segments, cleaner every 50 ms, `RUNE_VERDICT_INTERVAL_MS=20`, `deleteRetentionMs = 0` on the test topics; producers idempotent `All | View`).
  - `CheckedWritesRegressorsPathRegression.TheWholeScenarioThreeNodesRegression` (the acceptance case; every step asserts before the next):
    1. **DDL.** `rune ql -f regressorspath.rql --broker <node 0>` → exit 0. `show view character_name` contains `unique` and `keyspec lower(name) as nameKey`; `show view account_stats` contains `constraint max_characters check (characters <= 12)`.
    2. **Login race.** Clients A and B: `getViewRowAndWait(user_identity, key(1, "steam:42"))` → absent for both; both `users.insert(...)` issued before either is awaited → exactly one `ok()` with `key()` = k ≥ 1; the other `code() == 51` and `client.constraintName(r.constraintId()) == ConstraintName{"user_identity", "unique"}`; the loser's `getViewRowAndWait(user_identity, key(1, "steam:42"), {.minOffset = loser.offset()})` → OK on the first attempt with `userId == k`.
    3. **Failover mid-scenario.** `crash` the node leading `character`'s partition 0, wait for a new leader.
    4. **Create returns its id.** `characters.insertAndWait({.userId = k, .name = "Foo", …}).key()` = c1 ≥ 1; `character_by_id` has c1.
    5. **Case-insensitive name.** For user k2 (a second login), a create named `"foo"` → 51, `constraintName == {"character_name", "unique"}`.
    6. **The 13th slot.** 13 creates for user k3 issued concurrently from 4 clients → exactly 12 OK and 1 with `constraintName == {"account_stats", "max_characters"}`; `account_stats` for k3 is 12; `scanViewAndWait(account_characters, prefix(k3), 0)` returns 12 rows; the rejected create's id is burned: the next create's `key()` is above every id returned so far (06).
    7. **Restart mid-scenario.** `restartAll()`; once ready: k3 still 12, a 13th create still 51 (`max_characters`), `"foo"` still 51, `character_name` row `"foo"` → c1, the login race's `user_identity` row → k.
    8. **Delete, reuse, count.** `removeExistingAndWait(cx)` for one of k3's → OK; `account_stats` k3 = 11; a create for k2 with cx's name → OK (the name is free again); a create for k3 → OK (12); `removeExistingAndWait(cx)` again → `code() == 53` (KeyNotFound).
    9. **Never delivered.** A raw subscription from 0 on `user` and on `character` collects every record: no offset of a rejected completion appears, and every OK one does.
    10. **Gone after compaction.** Poll until every node's cleaner has passed the segments that held the rejected offsets (W past them, read on the owner thread); then on every node no record at a rejected offset exists in the log of `character` or `user`, and every key's latest record is its latest accepted one; `rune stats topic character --json` has `"rejectedNotCompacted": 0`.
  - `CheckedWritesRegressorsPathRegression.FailoverDuringDropFinishesTheDropThreeNodesRegression` (R04a on real brokers): `drop view character_name` acked; `crash` the leader at once and `restart` it; every node's `VerdictSource` (owner thread) shows `waitingForInstall() == false` within 10 s; a `"foo"` duplicate create → OK on the new leader; every `View` ack in flight completes within 10 s.
  - `CheckedWritesRegressorsPathRegression.OldWritersOnAConstrainedSourceShardRegression` (`OneBroker`, Shard; §11.2): a non-autoincrement `pc` topic with a unique `lower(name)` view; a non-idempotent producer, an `acks=leader` producer, an `acks=none` producer and a v11 session each write `"Foo"` / `"foo"` under new ids → their completions are today's; only one `"foo"` row exists, and a consumer from 0 sees only the accepted records.
  - `CheckedWritesRegressorsPathRegression.CompositeGroupByCheckShardRegression` (R07c): `per_gender` over `character` → a third character of the same user and gender → 51 `{"per_gender", "two_per_gender"}`; another gender → OK.
  - `CheckedWritesRegressorsPathRegression.ReplaceWhileWritingShardRegression`: a producer keeps creating for 20 users while `create or replace view account_stats … check (characters <= 13)` runs through QL → no completion is `Timeout`, every user ends at ≤ 13, and creates acked before the replace's change record stopped at 12.
  - Inverted (R08e): `CheckedWritesQlBrokerRegression.RegressorsPathOutcomesBeforePart09ThreeNodesRegression` becomes `CheckedWritesQlBrokerRegression.RegressorsPathOutcomesThreeNodesRegression`: every statement exits 0, and a whole-script `rune ql -f regressorspath.rql` on a fresh cluster exits 0.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesRegressorsPathRegression|CheckedWritesQlBrokerRegression' --output-on-failure`. Expected: PASS. Then `ctest --test-dir build/root -R 'CheckedWritesBrokerRegression|RuneCliBroker|ClientDdlBroker' -j6 --output-on-failure` and `grep -rn "BeforePart09" broker ql client-cpp` → no output.
- [ ] **Step 3: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesRegressorsPathRegressionTest.cpp \
          broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "test(e2e): the RegressorsPath scenario on real brokers - login race, 13th slot, Foo/foo, delete and reuse, restart and failover, compaction" \
             -m "R08e: RegressorsPathOutcomesBeforePart09ThreeNodesRegression inverted and renamed." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 11: The docker e2e `REGRESSORSPATH` leg (VM only; written here, run at Checkpoint C)

**Files:**
- Modify: `broker/docker/checked-writes-e2e.sh` (the leg after 06's `AUTOINC`), `client-cpp/examples/checked_writes_probe.cpp` (modes `rp-setup`, `rp-login --worker <i> --subjects <a>-<b>`, `rp-characters --worker <i> --users <a>-<b>`, `rp-lifecycle`, `rp-verify`), `client-cpp/tests/support/GenClaimSchemas.cpp` (`rp_user`, `rp_character` with the `RegressorsPathRecords.h` fields), `client-cpp/CMakeLists.txt` (`rp_user.gen.h`, `rp_character.gen.h` in `RUNE_CLAIM_GEN_HEADERS`), `docs/testing.md` (the row of `checked-writes-e2e.sh`, `:166-175`)

**Interfaces:**
- The probe writes one result line per op (`op key code constraintName`) to the shared directory, logs in with `docker/lib/auth.sh`'s e2e user, and takes no host, IP or credential from the source: everything comes from the environment the script sets.

- [ ] **Step 1: Write the leg.** House style: `set -euo pipefail`, bounded waits, the existing trap teardown, and every failure names its step.
  1. **RP-SETUP:** `rune ql -f ql/tests/golden/regressorspath.rql` from the CLI container → exit 0; `show view account_stats` lists `max_characters`.
  2. **RP-LOGIN-RACE:** 4 probes race first logins over the same 1,000 subjects. About 30 % through, `docker kill` the leader of `user` and restart it once progress resumes. **Check:** per subject exactly one `Ok`, every other result is 51 `user_identity.unique`, and each loser's `minOffset` read returned the winner's `userId`. No `VerdictUnknown`, `StoredIdUnknown` or `Timeout`. Bounded at 600 s.
  3. **RP-CHARACTERS:** 200 users; 4 probes each create 13 characters per user concurrently (names unique per probe), plus one `"Foo"`/`"foo"` pair per user pair. A `character` leader kill at 30 %. **Check:** per user exactly 12 `Ok`, the rest 51 `account_stats.max_characters`; each pair has exactly one `Ok` and one 51 `character_name.unique`. Bounded at 600 s.
  4. **RP-RESTART:** `docker restart` all three nodes; every user's `account_stats` row is 12, and a 13th create is 51.
  5. **RP-LIFECYCLE:** for 100 users: `removeExisting` one character → ok, count 11; a create reusing its name for another user → ok; `removeExisting` again → 53.
  6. **RP-VERIFY:** a subscription from 0 on `character` and `user` sees no offset any probe got a 51 for; after the cleaner's interval, `rune stats topic character --json` has `"rejectedNotCompacted": 0`.
  7. Print `REGRESSORSPATH: OK` before the script's final `CHECKED-WRITES-E2E: OK`.
  The usage line stays `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'`.
- [ ] **Step 2: Build check locally (no docker).** `cmake --build build/sub-client-cpp -j14 --target checked_writes_probe`. Expected: it builds. Then `bash -n broker/docker/checked-writes-e2e.sh` and `shellcheck broker/docker/checked-writes-e2e.sh` (if installed) report nothing.
- [ ] **Step 3: Docs.** Extend the `docs/testing.md` row with: "RegressorsPath: the golden DDL through the CLI, racing first logins and racing character creates through leader kills (one winner per subject, 12 per user, `Foo`/`foo`), a full restart, delete and name reuse, no rejected record delivered or left after compaction; `REGRESSORSPATH` races 600 s".
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/docker/checked-writes-e2e.sh client-cpp/examples/checked_writes_probe.cpp client-cpp/tests/support/GenClaimSchemas.cpp \
          client-cpp/CMakeLists.txt docs/testing.md
  git commit -m "test(docker): checked-writes e2e REGRESSORSPATH leg - login and character races through leader kills, restart, lifecycle, compaction (VM only)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 12: The alloc gate, the benches and the docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` (03's binary), `broker/benchmark/ViewFoldBench.cpp` (beside 04's `BM_ViewFoldVerdict/*`, after `BM_ViewFoldReplace`, `:542-581`)
- Modify: `docs/api/ql.md`, `docs/api/client-cpp.md`, `docs/api/wire-protocol.md`, `docs/api/admin-cli.md`, `docs/operations.md`, `client-cpp/CLAUDE.md`

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateUniqueAndCheckJudgingAllocatesNothingRegression`, on the rig with the key index, `character_name`, `account_stats` and `account_characters`, pre-existing duplicates of 8 names, a raw subscription:
  - warm up with 300 batches (1 and 16 records, creates, renames among a fixed pool of 64 names, deletes, 10 % rejected by each view, promotions);
  - then, under `rune::testing::AllocWindow w;`, 3,000 more batches of the same mix, the key set churning within the warmed tables;
  - `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Write the benches.**
  - `BM_ViewFoldVerdict/unique_lower_plus_count_check`: the `BM_ViewFoldReplace` churn workload (`generateChurnOps`, `kBatchSizes`) on the `character` shape, driven through a `VerdictSource` with `character_name` (unique `lower(name)`) and `account_stats` (`count(*)` with `characters <= 12`) as members, no rejections. It reports `verdict_record_p50_ns`, `verdict_record_p99_ns` and `steady_alloc`.
  - `BM_ViewFoldVerdict/unjudged_same_views`: the same two views folded through their plain folds (no `VerdictSource`), reporting `record_p50_ns`.
  - **Gate (§14 judging):** `verdict_record_p50_ns − unjudged_same_views.record_p50_ns ≤ 1000` and `steady_alloc == 0`; stderr line `[ViewFoldVerdict unique+count gate] … => PASS|FAIL`.
  - `BM_ViewFoldVerdict/unique_with_promotions`: 5 % of records leave an owned name with extras; reported only.
- [ ] **Step 3: Run the gate.** `cmake --build build/root -j14 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches** (baseline = this part's base commit on `main`, ABAB n=3, one session, same box, one prepare/restore):
  - `git worktree add "$TMPDIR/rune-cw09-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel`.
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ViewFoldReplace$|BM_ViewFoldAggregate$|BM_ViewFoldRetractable|BM_ViewFoldDerivedKey|BM_ViewFoldVerdict/test_reject_cap' --benchmark_repetitions=3`, then the tip alone for `BM_ViewFoldVerdict/unique_lower_plus_count_check|BM_ViewFoldVerdict/unjudged_same_views|BM_ViewFoldVerdict/unique_with_promotions`.
  - **Gates:** no regression on the ABAB set (ordinary and keyed non-unique views fold exactly as before), judged by same-session medians against `BENCHMARKING.md`'s noise; the judging gate above.
  - Borderline numbers are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under `flock /root/vm.lock` on `$RUNE_VM_HOST`, then `vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Write the docs.**
  - **`ql.md`**, a section "Constraints":
    - `create unique view` and `constraint <name> check (<predicate>)`, with the §1 examples;
    - what is checked (§8.2), the constraint id `viewObjectId << 16 | ordinal` with 0 = `unique`;
    - deletes are never rejected; "model ledgers as appended entries, not as deletable ones" (§8.2);
    - a group already over its limit can shrink by deletes, never grow; a move out of it is rejected;
    - NULL in a check passes;
    - pre-existing duplicates: the smallest source key owns the name, and the next is promoted (§8.3); remove duplicates before a back-fill;
    - creating a constraint on a large topic pauses delivery of new records while it builds, and writes are never refused (§8.7);
    - `create or replace` has no gap; drop followed by create leaves one (§8.7);
    - refused: transient sources, more than 32 verdict views per topic, ids past 65535 (SD5).
  - **`client-cpp.md`**, "Constraint violations": `WriteResult::code() == 51`, `constraintId()`, `Client::constraintName` (owner thread; dropped and replaced views resolve), and the login-race pattern (insert, on 51 read the unique view with `minOffset = r.offset()`), with the §1 auth-server and character-server snippets as they ship.
  - **`wire-protocol.md`**, "Control records 16–19": add type 20 `kVerdictPlan` with its byte table; "consumers never receive it".
  - **`admin-cli.md`**: `rune stats` prints `rejections <view>.<constraint> <n>`, and `uniqueConflicts` for a unique view.
  - **`operations.md`**, "Verdict sources" (04's section): constraint create, replace and drop; the two-phase drop (a dropped plan stays installed until W passes its change record); type-20 records are kept by the cleaner; `uniqueConflicts`.
  - **`client-cpp/CLAUDE.md`** §2.1: "`Client::constraintName(id)` resolves a 51's constraint id from the catalog; owner thread only".
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ViewFoldBench.cpp \
          docs/api/ql.md docs/api/client-cpp.md docs/api/wire-protocol.md docs/api/admin-cli.md docs/operations.md client-cpp/CLAUDE.md
  git commit -m "bench/docs: unique+count judging gate, zero-alloc judging gate, constraint docs" \
             -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c); stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`; CatalogGolden on `build/sub-codec`; the ql suites on `build/sub-ql`.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) `-R Regression -LE latency`, including `rune_checked_writes_matrix_tests` and `rune_checked_writes_stress_tests`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure --repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 12 benches (recorded).
  - The VM docker e2e: `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'` → `REGRESSORSPATH: OK` and `CHECKED-WRITES-E2E: OK`.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model. The reviewer leaves PR comments; the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. "GC of kVerdictPlan (type 20) control records once their member left below W" — `enhancement`, `sev:low`, `area:broker`.
  2. "Owner check: an extra claimant's same-key update is accepted (P9-3)", "a move out of an over-limit group is rejected (P9-4)", "NULL in a check passes (P9-4)" — `question`, `sev:low`, `area:broker`, one issue each, for the owner to confirm or overturn.
  3. Close 04's "Two-phase drop of a verdict view" issue with a link to Task 6.
  4. Every recorded deviation (re-pinned assertion) that is not urgent.
- [ ] Hand over to Part 11 per the overview's acceptance list for 09: the §1 target block works end to end (`user_identity`, `character_name`, `account_stats` and `max_characters`).
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §8.2 "a unique view": P9-3, Tasks 2 and 4. "`check` on any view, projection or aggregate": Tasks 1 and 4 (projection, count, sum, composite group by). The predicate grammar and constants: Part 08 authors, Task 1 evaluates (R08c). "Evaluated on each prospective row": Task 4. "Deletes are never rejected": P9-2, Task 4 `TombstonesAreNeverRejected`, `DeleteAlwaysPassesBalanceLedger`. "Any other record is rejected if … and changes a column the predicate reads": `predicateReadsChanged`, the over-limit cells. "A row the record removes entirely isn't checked": P9-4 (`removed` rows skipped). "A unique view never rejects a delete": P9-2. "Several views": `SeveralVerdictViewsOneRejectsNoViewChanges`. "Refused at compile time": 07's structural compile plus Task 1's shape rules.
  - §8.3 rule, promotion, accounting, writes, back-fills: Task 2 (claims, promotion, gauge), Task 4 (judged rule), Task 5 (stat 32, compaction and refold), docs in Task 12.
  - §8.7 wiring: create (Task 5 `CreateOnANonEmptyTopicJudgesFromTheChangeRecord`), replace (P9-5, `ReplaceSwapsAtSWithoutAGap`), drop (`DropStopsJudgingAboveD`, R04a in Task 6), "drop followed by create leaves a gap" (`DropThenCreateLeavesAGap`, docs), failover and missing installs (Task 6).
  - §1 target block and client snippets: Task 10's acceptance scenario and Task 11's leg; `removeExistingAndWait` → 53 in both.
  - §11.2 per-write requirements: P9-13, Task 5 `PlainWritesFromAnyProducerAreJudged`, Task 10 `OldWritersOnAConstrainedSourceShardRegression`. §11.3 per-record verdicts and `constraintId` resolution: Tasks 5 and 7.
  - §13 `uniqueConflicts`: P9-10, Task 5. §14 judging budget: Task 12's gate. §15 09 cells: Interaction matrix A and B.
  - Rulings: R04a (Task 6), R04e/R04g (Tasks 5, 7), R08e (Tasks 3, 10), R08c (Task 1), R08f (Task 5), R10a/SD9 (Task 3), SD5 (Task 3), R07c (Tasks 4, 10).
- **Step scan.** Every test step names its tests with the spec's values (51, `0x00070000`, `0x00080001`, 12, 13, 14, 65,536, 32, 64, 600 / 555). Code steps give files, signatures and placement. Bodies appear only for the unique rule table (Task 2) and the desired-set rule (P9-5), which the signatures and tests do not fix.
- **Type consistency.** `PredicateResult`, `evaluatePredicate`, `predicateReadsChanged`, `constraintShapeError`, `kMaxPredicateNodes` (Task 1, used in 3, 4); `UniqueClaims`, `compareSourceKeys`, `ProspectRow::ownedByOther` / `promoted`, tables 6 / 7 (Task 2, used in 4, 5, 8, 12); `verdictKindOf`, `verdictPlanFingerprint`, `ConstraintFold`, `evaluateConstraints` (Task 4); `addCandidate`, `candidateCaughtUp` (Task 5); `kVerdictPlan`, `verdictPlanPayloadBytes`, `kMaxVerdictPlanBytes` (declared in Task 3, completed in Task 6), `RetainedVerdictPlans`, `leaveOffsetOf`, `retainedPlanInstalls` (Task 6); `ConstraintName`, `resolveConstraintName`, `constraintName` (Task 7, used in 10, 11); the rig helpers (Task 5, used in 6, 8, 9, 12). Every contract name (`VerdictSource`, `ConstraintSet`, `Prospect`, `CompiledConstraint`, `featureRefusal`, `StatsSink`, `WriteResult`) is used as defined by its owner.
- **Review Focus.** Each of the five has its named test in its owning task (2, 6, 1/4, 4, 5/7).
- **Proportion.** The plan fixes decisions, placements, signatures and assertions. The evaluator, the claims tables, the candidate swap, the retained-plan install and the rig bodies are left to the implementer within the stated rules.
