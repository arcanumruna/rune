# Checked Writes Part 01 — Views Skip Aborted Transactions (#537) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialized views fold only committed data: every projection, aggregate, restart-rebuild and transient fold skips the data batches of aborted transactions, and a persisted view whose checkpoint may already hold aborted rows is refolded from the log start (#537, spec §8.8 last bullet, ruling SD15).

**Architecture:** A new `rune::view::FoldSkip` (contract C9.2) carries a pointer to the source partition's `rune::transaction::AbortedTxnIndex`. The worker reads it from the partition's `RcState` (`RcState::aborted()`, `broker/libs/node/include/rune/node/RcState.h:412`) at the same point it computes the fold bound `rcState->lso(liveHw)` (`Worker.h:6433-6434`), and threads it through `WorkerViewSet::driveFolds` / `driveRebuild` / `driveView_` into `foldBatch` and `foldAggregateBatch`, which skip a transactional batch whose `isAborted(producerId, baseOffset)` is true, exactly as the read_committed fetch does (`broker/libs/transaction/src/ReadCommittedView.cpp:442-450`). At recovery, a persisted instance whose source partition has a non-empty aborted index discards its v1/v2 checkpoint, refolds from offset 0, and first sends a new reset frame on its `ViewSnapshotChannel.deltas` ring so the IO agent truncates the stale checkpoint file before any new delta lands.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark (`broker/benchmark/ViewFoldBench.cpp`), CMake/Ninja, real Aeron, in-process `BrokerNode`s (`ThreeNodes`), docker compose on the shared VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` (§8.8 last bullet: "With #537 fixed, records of aborted transactions are skipped"; §16 item 1: "skip aborted-transaction records in the fold, and re-fold"). Contract: `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C9.2, SD15). Issue: arcanumruna/rune#537. Executors read all three.

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-01-aborted-fold`, cut from the latest `main` after the plan set has merged (`git fetch && git switch -c feature/checked-writes-01-aborted-fold origin/main`). Part 01 depends on no other part; 02 and 10 may run concurrently. Merge `main` in again before review.
- **Preconditions** (each must hit before Task 1):
  - `grep -n "if (r.isControl())" broker/libs/view/src/ViewFold.cpp broker/libs/view/src/AggFold.cpp` (2 hits, `ViewFold.cpp:141`, `AggFold.cpp:565`)
  - `grep -n "const int64_t bound  = rcState->lso(liveHw);" broker/libs/node/include/rune/node/Worker.h` (1 hit, line 6434)
  - `grep -n "aborted() const noexcept" broker/libs/node/include/rune/node/RcState.h` (1 hit, line 412)
  - `grep -n "kInstallMsgType   = 3" broker/libs/node/include/rune/node/ViewSnapshotChannel.h` (1 hit)
- **Builds** (already configured; if one is missing, stop and tell the controller): `build/root` (Ninja, Debug, `BUILD_TESTING=ON`; `cmake --build build/root -j14 --target <t>`, `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`), `build/sub-client-cpp` (examples on), `build/sub-codec`, `build/rel` (Release, `BUILD_BENCHMARKS=ON`, binary `build/rel/broker/rune_broker_benchmark`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Docker e2e and benches** run only on the shared VM (`ssh root@$RUNE_VM_HOST`, every command under `flock /root/vm.lock`, in your own directory), never on local Mac docker. Prepare the clean box once for the benches and restore it afterwards (`broker/bench/BENCHMARKING.md`, Checklist).
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

## Rulings this part depends on (restated from Plan 00, binding)

- **C9.2 — `FoldSkip`.** `broker/libs/view/include/rune/view/FoldSkip.h` is created by this part with the member `aborted` only; Part 04 appends `rejected`. `foldBatch` and `foldAggregateBatch` take `const FoldSkip&`, and every caller passes it: `driveView_` (`WorkerViewSet.h:663`), rebuild and transient. `rune_view` gets `target_link_libraries(rune_view PUBLIC rune_transaction)`.
- **SD15 — #537 refold rule.** No checkpoint format change: Part D owns v3. At recovery, a persisted view whose source partition's `RcState` aborted index is non-empty discards its v1/v2 checkpoint and refolds from the log start.
- **Acceptance for the next part (Plan 00):** "A view over an aborted transaction equals a fresh refold."

## Decisions (binding for executors; each is an obvious choice recorded so a reviewer can overturn it)

- **AD1 — Batch-level skip, same predicate as read_committed.** A batch is skipped iff `skip.aborted != nullptr`, the batch is not a control batch, its attributes carry `kAttrTransactional` (`protocol/include/rune/protocol/RecordBatch.h:32`), `producerId() >= 0`, and `skip.aborted->isAborted(producerId(), baseOffset())`. This is the exact test of `ReadCommittedView.cpp:442-450`. A non-transactional batch of the same producer inside an aborted range is folded.
- **AD2 — The bound and the cursor.** An aborted batch is passed like a control batch, except that it honours the pass bound: base ≥ bound → stop before it; last ≥ bound → count the offsets `[max(base, from), bound)` and stop (the next pass resumes inside it and skips the rest); otherwise count `[max(base, from), last + 1)` and move on. The cursor contract of `ViewFold.h:85-90` is unchanged (never past `bound`).
- **AD3 — `skippedAborted` counts offsets.** `FoldStats::skippedAborted` adds the offsets counted in AD2, so a capped pass sequence counts each aborted offset exactly once. The cleaner removes aborted batches whole (`LogCleaner.cpp:349`, `:419`), so an aborted batch has no internal holes.
- **AD4 — Where the predicate comes from.** `Worker::driveViews_` builds `const rune::view::FoldSkip skip{.aborted = &rcState->aborted()}` right after the bound. `rcServe_` returns non-null only when the RC state is Ready (`Worker.h:6431-6432`), so the index is complete for `[logStart, LEO)` whenever a fold runs: markers are applied to it in the same call as their append (`RcState.h:470-480`), before the LSO can move past their transaction.
- **AD5 — SD15 predicate.** `checkpointUntrusted(skip)` is `skip.aborted != nullptr && skip.aborted->size() != 0`, applied to persisted instances only (transient instances have no checkpoint). It runs on all three recovery entry points: `materializeRecoveryInstancesFor` → `installViewForRecovery`, and the lazy `getOrCreate_` → `loadRecoveryCheckpoint_` (the whole-branch "Bug A" path).
- **AD6 — Discarding a checkpoint resets its file.** The checkpoint file is append-only and replay keeps the last record's O (`WorkerViewSet.cpp:103-124`). New deltas appended after old contaminated records would replay the contaminated rows on a later restart (for example once retention has pruned the aborted range), and could restore a stale O. So a discard enqueues **`ViewSnapshotChannel::kResetMsgType = 4`** (frame `[viewTopicId i32][partition i32]`, no body) on the `deltas` ring before any delta of the instance. The IO agent truncates the file to 0 bytes, fdatasyncs it, and resets the key's `pendingO`, `lastDurableO` (both −1), `dirtySinceSync` and `errored`. This is an in-process IPC message, not a checkpoint format change, so SD15 holds.
- **AD7 — The reset is owed state, not a boolean.** `enum class CheckpointStream : uint8_t { Live, ResetOwed };` on `ViewInstance` (`checkpointStream = CheckpointStream::Live`). `driveView_` flushes an owed reset first, before the held delta and before folding, and holds on ring back-pressure exactly like a held delta (`WorkerViewSet.h:616-619`).
- **AD8 — No `RcState.h` change.** The file map lists `RcState.h` for "aborted index access". `RcState::aborted()` already exists (`RcState.h:412`), so the file is not modified.

## Review Focus

These five inputs are the ones a user is most likely to hit and that the spec does not spell out. Each is pinned by a named test in the task that owns the code:

1. **An aborted batch cut by a capped pass** (`maxRecordsPerPass` of a persisted view, or the LSO bound): it must be skipped whole, counted once, and the cursor must never pass the bound. Test: `CheckedWritesAbortedFoldRegression.AbortedBatchStraddlingThePassBoundIsSkippedOnceRegression` (Task 1) and its aggregate twin (Task 2).
2. **The same producer aborts and then commits** (its next transaction, same or bumped epoch): the aborted range must not hide the committed batches, and a non-transactional batch of that producer inside the range is folded. Tests: `CheckedWritesAbortedFoldRegression.SameProducerAbortThenCommitRegression`, `…NonTransactionalBatchInsideAnAbortedRangeIsFoldedRegression` (Task 1).
3. **Recovery of a second partition created lazily** after the view left `recoveryViews_` (the "Bug A" path, `WorkerViewSet.cpp:497-516`) must also discard its checkpoint. Test: `CheckedWritesAbortedFoldViewSetRegression.LazyRecoveryOfASecondPartitionAlsoDiscardsRegression` (Task 7).
4. **The reset frame meets a full `deltas` ring**: it is held, never dropped, and no delta of the instance is ever enqueued before it. Test: `CheckedWritesAbortedFoldViewSetRegression.ResetHeldUnderBackPressurePrecedesEveryDeltaRegression` (Task 7).
5. **A crash between the reset and the first durable delta**: the file is then empty, which recovery treats as a cold start (refold from 0), and `lastDurableOffset` never reports an O from before the reset. Test: `CheckedWritesAbortedFoldIoAgentRegression.ResetRestartsTheDurableOffsetRegression` (Task 6).

---

## Interaction matrix

Plan 00 assigns no §15 cells and no `Op`/`Entity` id range to Part 01: its matrix predates the shared harness of Part 04 and lives in its own binaries. It covers every axis of the #537 fix direction (commit and abort, interleaved producers, an abort spanning a capped pass, restart rebuild, leader failover with an open transaction that later aborts).

**Fold layer (`rune_view_tests`, Task 3), table-driven, 7 × 3 × 6 = 126 cells**, `CheckedWritesAbortedFoldMatrixRegression/Cell.FoldEqualsCommittedOnlyFold/<n>`; the count is pinned by `CheckedWritesAbortedFoldMatrixCountRegression.CellCountIsPinned` (`EXPECT_EQ(cells().size(), 126u)`).

| Axis | Values |
|---|---|
| Op | `PlainOnly`; `TxnCommit`; `TxnAbort`; `AbortThenCommitSamePid`; `TwoPidsInterleaved` (one commits, one aborts); `AbortedTombstone` (an aborted delete of a committed key); `AbortedOverwrite` (an aborted upsert of a committed key) |
| Entity | `Projection` (Replace, `foldBatch`); `Aggregate` (`Count(*)` + `Sum`, no value block); `AggregateMinMax` (`Max` with the value-block table) |
| Timing | `OnePass`; `CapOne` (every pass bounded at `cursor + 1`); `BoundAtAbortedBase` (first pass bounded at the aborted batch's base); `SplitAbortedBatch` (a 3-record aborted batch cut by a pass bound after its first record); `PerBatchViews` (each pass reads exactly one batch); `RefoldFromZero` (a second fold of the whole log into fresh tables) |

Every cell asserts: (a) every table (view, contributors, groups, blocks) equals, row for row by key and byte for byte, the `kNoSkip` fold of the same log with its aborted data batches and its control batches removed (offsets kept); (b) `Σ skippedAborted` equals the number of aborted offsets; (c) every pass ends with `cursor <= passBound`; (d) the final cursor equals the log end.

**Model check (`rune_view_tests`, Task 3):** `CheckedWritesAbortedFoldModelRegression.RandomTxnLogsFoldToTheCommittedOnlyFold`, seeds 1–300, invariants (a)–(d) above.

**Worker layer (`rune_checked_writes_aborted_fold_tests`, Tasks 5 and 7; `rune_node_tests`, Tasks 6 and 7):**

| Path × source | Disk source | Memory (transient) source |
|---|---|---|
| Steady fold, abort | `…WorkerRegression.AbortedTxnNeverReachesProjectionOrCountViewRegression` | `…WorkerRegression.TransientSourceAbortedTxnIsSkippedRegression` |
| Steady fold, two producers interleaved | `…WorkerRegression.InterleavedCommitAndAbortFromTwoProducersRegression` | covered by the fold matrix |
| Open transaction pins the fold, then aborts | `…WorkerRegression.OpenTxnThenAbortNeverFoldsItRegression` | `…WorkerRegression.TransientOpenTxnThenAbortNeverFoldsItRegression` |
| Restart rebuild with no checkpoint | `…WorkerRegression.RestartRebuildSkipsTheAbortedRangeRegression` | n/a (transient views refold the new ring) |
| Restart with a contaminated checkpoint (SD15) | `…WorkerRegression.ContaminatedCheckpointIsDiscardedAndRefoldedRegression`, `…ContaminatedAggregateCheckpointIsDiscardedAndRefoldedRegression` | n/a |
| Restart with no aborted range (checkpoint kept) | `…WorkerRegression.CheckpointKeptWhenTheLogHasNoAbortedTxnRegression` | n/a |
| View-set recovery entry points | `…ViewSetRegression.*` (Task 7) | n/a |
| IO agent reset | `…IoAgentRegression.*` (Task 6) | n/a |

**Real brokers (`rune_client_failover_broker_tests`, Task 9), ThreeNodes, shard mode:** abort and commit visible in a projection and a `count(*)` view; full cluster restart over the aborted range; leader failover with an open transaction that later aborts.

**Docker e2e (VM, Task 10):** `broker/docker/view-engine-e2e.sh` gains an aborted-transaction leg in `setup` and its check after the broker restart (`verify_restart`), which exercises the SD15 refold on a real broker.

## File Structure

| File | C/M | Responsibility |
|---|---|---|
| `broker/libs/view/include/rune/view/FoldSkip.h` | C | `FoldSkip`, `kNoSkip`, `isAbortedBatch` (Task 1) |
| `broker/libs/view/include/rune/view/ViewFold.h`, `src/ViewFold.cpp` | M | `FoldStats::skippedAborted`; `foldBatch(…, const FoldSkip&)` (Task 1) |
| `broker/libs/view/include/rune/view/AggFold.h`, `src/AggFold.cpp` | M | both `foldAggregateBatch` overloads take `const FoldSkip&` (Task 2) |
| `broker/libs/view/CMakeLists.txt` | M | link `rune_transaction`; new test sources (Tasks 1, 3) |
| `broker/libs/view/tests/view/AbortedFoldLog.h` | C | test log builder with an `AbortedTxnIndex` and the committed-only twin (Task 1) |
| `broker/libs/view/tests/view/CheckedWritesAbortedFoldTest.cpp` | C | fold unit tests, projection and aggregate (Tasks 1, 2) |
| `broker/libs/view/tests/view/CheckedWritesAbortedFoldMatrixTest.cpp` | C | the 126-cell matrix and the model check (Task 3) |
| `broker/libs/view/tests/view/{ViewFoldTest,AggFoldTest,AggRetractableTest}.cpp` | M | pass `rune::view::kNoSkip` (Tasks 1, 2) |
| `broker/benchmark/ViewFoldBench.cpp` | M | callers pass `kNoSkip`; new `BM_ViewFoldReplaceTxn` (Tasks 1–3) |
| `broker/libs/node/include/rune/node/WorkerViewSet.h`, `src/WorkerViewSet.cpp` | M | thread `FoldSkip`; counters; SD15 discard; owed reset (Tasks 5, 7) |
| `broker/libs/node/include/rune/node/Worker.h` | M | `driveViews_` / `driveViewsOf_` build and pass the skip (Task 5) |
| `broker/libs/node/include/rune/node/ViewSnapshotChannel.h`, `ViewSnapshotIoAgent.h`, `src/ViewSnapshotIoAgent.cpp` | M | `kResetMsgType = 4` and its handling (Task 6) |
| `broker/libs/node/tests/support/AbortedFoldRig.h` | C | one shard worker + one leader partition (disk or memory) + views + IO agent (Task 5) |
| `broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp` | C | worker-layer tests (Tasks 5, 7) |
| `broker/libs/node/tests/node/{WorkerViewSetTest,ViewSnapshotIoAgentTest}.cpp` | M | new view-set and IO-agent tests (Tasks 6, 7); every existing driver call keeps compiling through the defaulted `skip` |
| `broker/libs/node/CMakeLists.txt` | M | `rune_checked_writes_aborted_fold_tests` (Task 5); broker test source (Task 9) |
| `broker/libs/node/tests/client/CheckedWritesAbortedFoldBrokerRegressionTest.cpp` | C | ThreeNodes tests (Task 9) |
| `client-cpp/examples/view_probe.cpp`, `broker/docker/view-engine-e2e.sh` | M | aborted-transaction e2e leg (Task 10) |
| `docs/api/client-cpp.md`, `docs/operations.md` | M | views fold committed data only; the one-time refold (Task 10) |

## Contract additions (none redefines a contract type)

- `[[nodiscard]] inline bool rune::view::isAbortedBatch(const FoldSkip& skip, const rune::protocol::RecordBatchReader& r) noexcept;` in `FoldSkip.h` (AD1). Part 04 may add a per-record sibling for `rejected`.
- `FoldStats::skippedAborted` (`uint64_t`, appended), `ViewFoldCounters::skippedAborted` and `ViewFoldCounters::checkpointsDiscarded` (`uint64_t`, appended).
- `ViewSnapshotChannel::kResetMsgType = 4` (AD6). `enum class CheckpointStream : uint8_t { Live, ResetOwed };` and `ViewInstance::checkpointStream` (AD7).
- A trailing `const rune::view::FoldSkip& skip = rune::view::kNoSkip` on `WorkerViewSet::driveFolds`, `driveRebuild`, `materializeRecoveryInstancesFor` and `installViewForRecovery`. A required `const rune::view::FoldSkip& skip` on the private `driveView_`, `getOrCreate_` and `loadRecoveryCheckpoint_`. Part 04 passes `FoldSkip{.aborted = …, .rejected = …}` through the same parameters.
- `[[nodiscard]] int64_t WorkerViewSet::foldCursorForTest(int32_t viewTopicId, int32_t partition) const noexcept` (−1 when the instance is absent).
- Test binary `rune_checked_writes_aborted_fold_tests` (`broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp`), outside the `tests/node/CheckedWrites*Test.cpp` sources of Part 02's `rune_checked_writes_tests`, so the two never collide.

---

## Phase 1 — The fold skips aborted batches

### Task 1: `FoldSkip` and the projection fold

**Files:**
- Create: `broker/libs/view/include/rune/view/FoldSkip.h`, `broker/libs/view/tests/view/AbortedFoldLog.h`, `broker/libs/view/tests/view/CheckedWritesAbortedFoldTest.cpp`
- Modify: `broker/libs/view/include/rune/view/ViewFold.h:50-72` (append `skippedAborted`), `:91-94` (signature); `broker/libs/view/src/ViewFold.cpp:114-117`, `:141-146`
- Modify: `broker/libs/view/CMakeLists.txt:14-17` (link and comment), `:21-27` (add `tests/view/CheckedWritesAbortedFoldTest.cpp`)
- Modify (callers pass `rune::view::kNoSkip`): `broker/libs/view/tests/view/ViewFoldTest.cpp` (12 calls), `broker/benchmark/ViewFoldBench.cpp:567`, `broker/libs/node/include/rune/node/WorkerViewSet.h:667` (Task 5 replaces it with the real skip)

**Interfaces:**
- Produces (`FoldSkip.h`, namespace `rune::view`; includes `<rune/protocol/RecordBatch.h>` and `<rune/transaction/ReadCommittedView.h>`):
  ```cpp
  struct FoldSkip {
      const rune::transaction::AbortedTxnIndex* aborted = nullptr;   // 01: skip transactional batches isAborted(pid, base)
  };
  inline constexpr FoldSkip kNoSkip{};
  [[nodiscard]] inline bool isAbortedBatch(const FoldSkip& skip, const rune::protocol::RecordBatchReader& r) noexcept;
  ```
- Produces (`ViewFold.h`): `FoldStats::skippedAborted` and
  ```cpp
  FoldStats foldBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                      VersionDescriptorCache& vdc, ViewTable& table,
                      const rune::storage::LogReadView& view, int64_t bound,
                      int64_t& cursor, const FoldSkip& skip);
  ```
- Produces (`AbortedFoldLog.h`, namespace `aborted_fold_log`, test-only):
  ```cpp
  using Record = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;   // wire key, value (empty = tombstone)
  class TxnLog {
  public:
      int64_t plain(std::span<const Record> recs, int64_t producerId = -1);   // non-transactional batch; returns its base
      int64_t txn(int64_t producerId, std::span<const Record> recs);           // transactional data batch
      int64_t commit(int64_t producerId);                                      // one-record control batch
      int64_t abort(int64_t producerId);   // control batch + aborted().onAbortMarker(pid, firstOffsetOfOpenTxn, markerOffset)
      [[nodiscard]] rune::storage::LogReadView viewFrom(int64_t from) const;   // whole batches, starting at the one holding `from`
      [[nodiscard]] rune::storage::LogReadView oneBatchAt(int64_t from) const; // only the batch holding `from`
      [[nodiscard]] rune::storage::LogReadView committedOnly() const;          // aborted data + control batches removed, offsets kept
      [[nodiscard]] const rune::transaction::AbortedTxnIndex& aborted() const noexcept;
      [[nodiscard]] uint64_t abortedOffsets() const noexcept;                  // total offsets of aborted data batches
      [[nodiscard]] int64_t end() const noexcept;                              // next offset
  };
  ```
  Data batches are built with `RecordBatchBuilder` (`setTransactional`, `setProducerId`, `setProducerEpoch(0)`, `setBaseSequence`); control batches with `setControl(true)` and one record. The fold only needs `isControl()`, so the marker payload is not parsed.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesAbortedFoldTest.cpp`, projection half). Fixture: `charSourceV1()`, `hpView()`, `keyBytes`, `encodeCharacter` copied from `ViewFoldTest.cpp:15-89` into the file's anonymous namespace; a helper `foldAll(TxnLog&, ViewTable&, const FoldSkip&, int64_t cap, std::vector<int64_t>* passBounds)` that loops `passBound = min(log.end(), cursor + cap)`, `foldBatch(…, log.viewFrom(cursor), passBound, cursor, skip)` under a hard cap of 1000 passes (`ASSERT` on the cap), asserts `cursor <= passBound` after every pass, and returns the summed `FoldStats`. `hp(t, id)` returns `readSlot(t.get(id), layout, 1).i` or `std::nullopt`.

```cpp
TEST(CheckedWritesAbortedFoldRegression, AbortedTxnUpsertNeverReachesTheTableRegression) {
    TxnLog log;
    log.plain(one(1, 10));            // offset 0
    log.txn(7, one(2, 20));           // offset 1
    log.abort(7);                     // offset 2
    ViewTable t(comp.rowLayout);
    const auto s = foldAll(log, t, FoldSkip{.aborted = &log.aborted()}, /*cap*/ INT64_MAX);
    EXPECT_EQ(hp(t, 1), 10);
    EXPECT_EQ(t.get(2), nullptr) << "#537: an aborted record reached the view";
    EXPECT_EQ(s.skippedAborted, 1u);
    EXPECT_EQ(s.skippedControl, 1u);
    ViewTable pre(comp.rowLayout);    // the fixture reproduces the bug without the skip
    (void)foldAll(log, pre, kNoSkip, INT64_MAX);
    EXPECT_NE(pre.get(2), nullptr);
}
TEST(CheckedWritesAbortedFoldRegression, AbortedTombstoneNeverErasesRegression)     // plain(1,10); txn(7, tomb 1); abort(7) -> hp(1)==10, skippedAborted==1
TEST(CheckedWritesAbortedFoldRegression, AbortedOverwriteKeepsTheCommittedValueRegression)  // plain(1,10); txn(7,(1,99)); abort -> hp(1)==10
TEST(CheckedWritesAbortedFold, CommittedTxnFolds)                                   // txn(7,(2,20)); commit(7) -> hp(2)==20, skippedAborted==0
TEST(CheckedWritesAbortedFoldRegression, InterleavedCommitAndAbortFromTwoProducersRegression)
    // txn(7,(1,1))@0 txn(8,(2,2))@1 txn(7,(3,3))@2 abort(7)@3 txn(8,(4,4))@4 commit(8)@5
    // -> get(1)==nullptr, get(3)==nullptr, hp(2)==2, hp(4)==4, skippedAborted==2
TEST(CheckedWritesAbortedFoldRegression, SameProducerAbortThenCommitRegression)
    // txn(7,(1,1))@0 abort(7)@1 txn(7,(2,2))@2 commit(7)@3 -> get(1)==nullptr, hp(2)==2
TEST(CheckedWritesAbortedFoldRegression, NonTransactionalBatchInsideAnAbortedRangeIsFoldedRegression)
    // txn(7,(1,1))@0 plain((2,2), producerId 7)@1 abort(7)@2 -> get(1)==nullptr, hp(2)==2
TEST(CheckedWritesAbortedFoldRegression, AbortedBatchStraddlingThePassBoundIsSkippedOnceRegression)
    // txn(7, three records ids 1,2,3)@0..2 abort(7)@3 plain((4,4))@4; foldAll(cap 1)
    // -> passes end at cursors {1,2,3,4,5} exactly; Σ skippedAborted == 3; only id 4 present
TEST(CheckedWritesAbortedFoldRegression, ResumeInsideAnAbortedBatchSkipsTheRestRegression)
    // same log; cursor starts at 1, one pass bound 5 -> skippedAborted == 2, cursor == 5
TEST(CheckedWritesAbortedFoldRegression, BoundAtTheAbortedBatchBaseStopsBeforeItRegression)
    // plain((9,9))@0 txn(7,(1,1),(2,2))@1..2 abort(7)@3: pass bound 1 -> cursor 1, skippedAborted 0;
    // next pass bound 4 -> cursor 4, skippedAborted 2, get(1)==get(2)==nullptr
TEST(CheckedWritesAbortedFold, TransactionalBatchWithANegativeProducerIdIsFolded)
    // a kAttrTransactional batch with producerId -1, the index holding a range for pid -1 -> folded (AD1)
TEST(CheckedWritesAbortedFold, NoSkipIsTheFoldOfBefore)
    // the same interleaved log with kNoSkip: every data record applied, skippedAborted == 0
```

- [ ] **Step 2: Run to verify they fail to compile**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: FAIL: `rune/view/FoldSkip.h` not found (and `foldBatch` has no `FoldSkip` parameter).

- [ ] **Step 3: Create `FoldSkip.h` and implement `isAbortedBatch`** per AD1 (`r.isControl()` false, `(static_cast<uint16_t>(r.attributes()) & kAttrTransactional) != 0`, `r.producerId() >= 0`, `skip.aborted->isAborted(r.producerId(), r.baseOffset())`). In `broker/libs/view/CMakeLists.txt` add `target_link_libraries(rune_view PUBLIC rune_transaction)` and replace the "no link against rune_storage" comment at lines 14-15 with one naming C9.2 (the target is defined later in `broker/CMakeLists.txt:342`; CMake resolves it at generate time).

- [ ] **Step 4: Implement the skip in `foldBatch`** (`ViewFold.cpp`). Right after the control-batch branch (`:141-146`), an `isAbortedBatch(skip, r)` branch applies AD2 with the function's existing `from`, `bound`, `pos`, `walkedUpto` and `stopped` variables: base ≥ bound → `stopped = true; break`; last ≥ bound → `stats.skippedAborted += bound - max(base, from)` (if positive) then `stopped = true; break`; otherwise add `last + 1 - max(base, from)` (if positive), advance `pos` and `walkedUpto` as the control branch does, `continue`. The final cursor expression (`:207`) is unchanged. Update the doc comment at `ViewFold.h:74-90`: "data batches of aborted transactions (`skip.aborted`) are skipped like control batches; see FoldSkip.h". Append `uint64_t skippedAborted = 0;` to `FoldStats` with the comment "offsets of aborted-transaction batches passed (AD3)".

- [ ] **Step 5: Pass `rune::view::kNoSkip` at every other caller** listed under Files (so the build is green before Task 5 threads the real skip). Add `#include <rune/view/FoldSkip.h>` to `ViewFold.h`.

- [ ] **Step 6: Run the tests**

Run: `cmake --build build/root -j14 --target rune_view_tests rune_node_tests && cmake --build build/rel -j14 --target rune_broker_benchmark && ctest --test-dir build/root -R 'CheckedWritesAbortedFold|ViewFold|WorkerViewSet' -j14 --output-on-failure`
Expected: PASS, every pre-existing `ViewFold*` test unchanged, and the bench binary compiles.

- [ ] **Step 7: Commit**

```bash
git add broker/libs/view/include/rune/view/FoldSkip.h broker/libs/view/include/rune/view/ViewFold.h \
        broker/libs/view/src/ViewFold.cpp broker/libs/view/CMakeLists.txt \
        broker/libs/view/tests/view/AbortedFoldLog.h broker/libs/view/tests/view/CheckedWritesAbortedFoldTest.cpp \
        broker/libs/view/tests/view/ViewFoldTest.cpp broker/benchmark/ViewFoldBench.cpp \
        broker/libs/node/include/rune/node/WorkerViewSet.h
git commit -m "view: projection fold skips aborted-transaction batches (FoldSkip, #537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: The aggregate fold

**Files:**
- Modify: `broker/libs/view/include/rune/view/AggFold.h:22-33`, `broker/libs/view/src/AggFold.cpp:548-552`, `:565-570`, `:663-669`
- Modify (callers pass `kNoSkip`): `broker/libs/view/tests/view/AggFoldTest.cpp` (5 calls), `broker/libs/view/tests/view/AggRetractableTest.cpp` (4), `broker/benchmark/ViewFoldBench.cpp:520`, `:740`, `broker/libs/node/include/rune/node/WorkerViewSet.h:663`
- Test: `broker/libs/view/tests/view/CheckedWritesAbortedFoldTest.cpp` (aggregate half)

**Interfaces:**
- Consumes: `FoldSkip`, `isAbortedBatch`, `FoldStats::skippedAborted` (Task 1); `TxnLog` (Task 1).
- Produces (`AggFold.h`):
  ```cpp
  FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                               const AggLayout& agg, VersionDescriptorCache& vdc,
                               ViewTable& view, ViewTable& contributors, ViewTable& groups,
                               ViewTable* blocks, const rune::storage::LogReadView& lv,
                               int64_t bound, int64_t& cursor, const FoldSkip& skip);
  FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                               const AggLayout& agg, VersionDescriptorCache& vdc,
                               ViewTable& view, ViewTable& contributors, ViewTable& groups,
                               const rune::storage::LogReadView& lv, int64_t bound,
                               int64_t& cursor, const FoldSkip& skip);
  ```

- [ ] **Step 1: Write the failing tests.** Fixture: `orderSource()`, `userTotals()`, `orderV1()` and the `Slot` enum copied from `AggFoldTest.cpp:26-76`, plus `userMax()` = `{userId Replace, mx = Max(amount)}` built with a blocks table (`ViewTable(agg.blockRow)`, the `blocks` overload). `foldAllAgg(…, cap)` mirrors Task 1's `foldAll`.

```cpp
TEST(CheckedWritesAbortedAggFoldRegression, AbortedTxnIsNeverCountedRegression) {
    // plain o1(u100, 10)@0; txn(7, o2(u100, 30))@1; abort(7)@2
    EXPECT_EQ(col(100, kN).i, 1);
    EXPECT_EQ(col(100, kTotal).i, 10);
    EXPECT_EQ(contributors.size(), 1u);
    EXPECT_EQ(stats.skippedAborted, 1u);
}
TEST(CheckedWritesAbortedAggFoldRegression, AbortedMoveBetweenGroupsLeavesBothGroupsRegression)
    // plain o1(u100,10); txn(7, o1(u200,10)); abort -> u100 n==1 total==10, view.get(200)==nullptr
TEST(CheckedWritesAbortedAggFoldRegression, AbortedTombstoneNeverRetractsRegression)
    // plain o1(u100,10); txn(7, tomb o1); abort -> u100 n==1, contributors.size()==1
TEST(CheckedWritesAbortedAggFoldRegression, AbortedTxnNeverTouchesTheMinMaxBlockRegression)
    // userMax: plain o1(u100,10); txn(7, o2(u100,99)); abort -> mx==10, blocks->size() == 1 (one live block entry), aggRescans==0
TEST(CheckedWritesAbortedAggFoldRegression, InterleavedTwoProducersAggregateRegression)
    // txn(7,o1(u100,1)) txn(8,o2(u100,2)) abort(7) commit(8) -> u100 n==1 total==2
TEST(CheckedWritesAbortedAggFoldRegression, AbortedBatchStraddlingThePassBoundIsSkippedOnceRegression)
    // txn(7, o1,o2,o3 (u100))@0..2 abort@3 plain o4(u100,4)@4; cap 1 -> cursors {1..5}, Σ skippedAborted==3, u100 n==1 total==4
```

- [ ] **Step 2: Run to verify they fail to compile**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: FAIL: no `foldAggregateBatch` overload takes a `FoldSkip`.

- [ ] **Step 3: Implement** the AD2 branch in the batch loop of the `blocks` overload (after `:565-570`, same variables `from`, `bound`, `pos`, `walkedUpto`, `stopped`), and forward `skip` from the second overload (`:663-669`). Update the `AggFold.h:14-21` doc: "same … control-batch and aborted-batch skip …". Pass `kNoSkip` at every other caller listed under Files.

- [ ] **Step 4: Run the tests**

Run: `cmake --build build/root -j14 --target rune_view_tests rune_node && ctest --test-dir build/root -R 'CheckedWritesAborted|AggFold|AggRetractable|WorkerViewSet' -j14 --output-on-failure`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broker/libs/view/include/rune/view/AggFold.h broker/libs/view/src/AggFold.cpp \
        broker/libs/view/tests/view/CheckedWritesAbortedFoldTest.cpp broker/libs/view/tests/view/AggFoldTest.cpp \
        broker/libs/view/tests/view/AggRetractableTest.cpp broker/benchmark/ViewFoldBench.cpp \
        broker/libs/node/include/rune/node/WorkerViewSet.h
git commit -m "view: aggregate fold skips aborted-transaction batches (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Fold matrix, model check and the fold bench

**Files:**
- Create: `broker/libs/view/tests/view/CheckedWritesAbortedFoldMatrixTest.cpp`
- Modify: `broker/libs/view/CMakeLists.txt:21-27` (add the source), `broker/benchmark/ViewFoldBench.cpp` (add `BM_ViewFoldReplaceTxn` after `BM_ViewFoldReplace`, `:542-581`)

**Interfaces:**
- Consumes: `TxnLog` (Task 1), both fold signatures (Tasks 1, 2).
- Produces (test-only): `enum class Op : uint8_t`, `enum class Entity : uint8_t`, `enum class Timing : uint8_t` with the values of the Interaction matrix table; `struct Cell { Op op; Entity entity; Timing timing; }`; `std::vector<Cell> cells()` (the full product, in declaration order); `void expectTablesEqual(const ViewTable& got, const ViewTable& want, std::span<const uint64_t> keys, const char* which)` (equal `size()` and, for every key, both absent or `memcmp` of `rowLen` bytes equal).

- [ ] **Step 1: Write the matrix and the model test.** `INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesAbortedFoldMatrixRegression, ::testing::ValuesIn(cells()))`, one `TEST_P(…, FoldEqualsCommittedOnlyFold)` asserting (a)–(d) of the Interaction matrix: build the cell's log with `TxnLog` (keys from a fixed set of 8, so `expectTablesEqual` iterates them), fold it with `FoldSkip{.aborted = &log.aborted()}` under the cell's timing, fold `log.committedOnly()` with `kNoSkip` in one pass into fresh tables, compare every table (`view`, plus `contributors`, `groups` and `blocks` for aggregates), `Σ skippedAborted == log.abortedOffsets()`, final cursor `== log.end()`. `TEST(CheckedWritesAbortedFoldMatrixCountRegression, CellCountIsPinned) { EXPECT_EQ(cells().size(), 126u); }`.
  The model test `TEST(CheckedWritesAbortedFoldModelRegression, RandomTxnLogsFoldToTheCommittedOnlyFold)` runs seeds 1–300 with `std::mt19937_64(seed)`: 64 steps over {plain write, transactional write of 1–4 records by one of producers 7/8/9, commit, abort}, 16 keys, 10% tombstones, every still-open transaction aborted at the end; entity `seed % 3`; pass cap drawn from {1, 2, 3, 7, unbounded}; the same invariants. A failure prints `seed=<n>` with `SCOPED_TRACE`.

- [ ] **Step 2: Run them**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'CheckedWritesAbortedFold(Matrix|Model)' -j14 --output-on-failure`
Expected: PASS: 126 cells + 1 count pin + 1 model test. Mutation check (do it, then revert): make `isAbortedBatch` return `false` → the `TxnAbort`, `AbortThenCommitSamePid`, `TwoPidsInterleaved`, `AbortedTombstone` and `AbortedOverwrite` cells and the model test turn red.

- [ ] **Step 3: Add `BM_ViewFoldReplaceTxn`.** The `BM_ViewFoldReplace` workload (`generateChurnOps(kTotalOps)`, `kBatchSizes`) with every batch built transactional (`setTransactional(true)`, `setProducerId(batchIndex % 16)`), and an `AbortedTxnIndex` reserved and pre-filled before timing with `onAbortMarker(pid, base, base)` for every tenth batch (its own pid and base), so 10% of batches are skipped and 90% pay the index lookup. Fold with `FoldSkip{.aborted = &index}`. Report with `reportCommon(st, "txn_", r, table.size())`, and gate like `BM_ViewFoldAggregate` (`ViewFoldBench.cpp:649-657`): `txn_record_p99_us < 5`, `txn_batch_p99_us < 10`, `steady_alloc == 0`, counter `txn_gate_pass`, stderr line `[ViewFoldReplaceTxn gate] … => PASS|FAIL`.

- [ ] **Step 4: Build and smoke-run the bench**

Run: `cmake --build build/rel -j14 --target rune_broker_benchmark && build/rel/broker/rune_broker_benchmark --benchmark_filter='^BM_ViewFoldReplaceTxn$'`
Expected: one run, `txn_gate_pass=1` and `steady_alloc=0`. (Gate numbers are taken on the VM at Checkpoint C.)

- [ ] **Step 5: Commit**

```bash
git add broker/libs/view/tests/view/CheckedWritesAbortedFoldMatrixTest.cpp broker/libs/view/CMakeLists.txt \
        broker/benchmark/ViewFoldBench.cpp
git commit -m "view: aborted-fold interaction matrix, model check and BM_ViewFoldReplaceTxn (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Checkpoint A

The checkpoint commands below are referred to as **CP-full** from Checkpoints B and C.

- [ ] **Step 1: Full regression suite.** `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure`. Expected: 100% pass.
- [ ] **Step 2: ASan+UBSan.** `cmake --build build/asan -j14 && ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/asan -R Regression -LE latency -j14 --output-on-failure` (macOS x86_64 under Rosetta: `detect_leaks=0`, then `leaks(1)` on the `rune_view_tests`, `rune_node_tests` and `rune_checked_writes_aborted_fold_tests` binaries of `build/asan`). Expected: no report.
- [ ] **Step 3: TSan.** `cmake --build build/tsan -j14 && ctest --test-dir build/tsan -R Regression -LE latency -j14 --output-on-failure`, and the client: `cmake --build build/client-tsan -j14 && ctest --test-dir build/client-tsan -R Regression -LE latency -j14 --output-on-failure`. Expected: no report.
- [ ] **Step 4: CatalogGolden.** `cmake --build build/sub-codec -j14 && ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure`. Expected: 3 tests pass, unchanged.
- [ ] **Step 5:** A failure is bisected over the commits since the last green checkpoint and fixed with a `*Regression*` test; after the fix rerun only the failed and affected tests.

---

## Phase 2 — Wiring and the SD15 refold

### Task 5: Thread `FoldSkip` from the worker to every fold

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h:130-139` (counters), `:373-376` and `:386` (`driveRebuild`), `:417-420` and `:427-431` (`driveFolds`), `:575` (`getOrCreate_`), `:598-599` and `:660-677` (`driveView_`), after `:507` (`foldCursorForTest`)
- Modify: `broker/libs/node/src/WorkerViewSet.cpp:442` (`getOrCreate_` signature; the skip is consumed in Task 7)
- Modify: `broker/libs/node/include/rune/node/Worker.h:6422-6493` (`driveViews_`, `driveViewsOf_`)
- Create: `broker/libs/node/tests/support/AbortedFoldRig.h`, `broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt` (new target next to `rune_f15_eos_regression_tests`, `:1263-1268`, same guard)

**Interfaces:**
- Consumes: `FoldSkip`, `kNoSkip` (Task 1); both fold signatures (Tasks 1, 2).
- Produces (`WorkerViewSet.h`):
  ```cpp
  template <class ReadRangeFn>
  int driveRebuild(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                   int64_t committedTail, int64_t nowNs,
                   const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                   bool leaderHwConfirmed = true, const rune::view::FoldSkip& skip = rune::view::kNoSkip);
  template <class ReadRangeFn>
  int driveFolds(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                 int64_t bound, int64_t nowNs,
                 const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                 bool leaderHwConfirmed = true, const rune::view::FoldSkip& skip = rune::view::kNoSkip);
  [[nodiscard]] int64_t foldCursorForTest(int32_t viewTopicId, int32_t partition) const noexcept;   // -1 = absent
  // private:
  ViewInstance* getOrCreate_(int32_t viewTopicId, int32_t partition, int64_t committedTail,
                             const rune::view::FoldSkip& skip);
  template <class ReadRangeFn>
  int driveView_(ViewInstance& inst, ReadRangeFn&& readRange, int64_t bound,
                 const TransientBounds* transientSrc, uint64_t leaderGen, bool leaderHwConfirmed,
                 const rune::view::FoldSkip& skip);
  ```
  `ViewFoldCounters` appends `uint64_t skippedAborted = 0;   // offsets of aborted-transaction batches the fold passed (#537)`.
- Produces (`Worker.h`): `driveViewsOf_(…, uint64_t leaderGen, bool leaderHwConfirmed, const rune::view::FoldSkip& skip)`.
- Produces (`AbortedFoldRig.h`, namespace `aborted_fold_rig`, test-only), modeled on `broker/libs/node/tests/integration/TransientRig.h:63-176` (one shard-mode worker, worker ingress active, produce over `handleIngressFrameForTest`, markers as `WorkerOp::Type::AppendControlRecord` with `ControlMarker::build`):
  ```cpp
  enum class Backing : uint8_t { Disk, Memory };
  struct Rig {
      Rig(Backing backing, std::filesystem::path dir);   // Disk: a rune::storage::Log under dir/"log" (reopened if present); Memory: an InMemoryLog
      void installProjection(int32_t viewTopicId, bool persisted, bool recovery = false);  // {characterId Int64 key, hp Int32} Replace over topic 1
      void installCountByHp(int32_t viewTopicId, bool persisted, bool recovery = false);   // key hp, n = CountAll
      int16_t put(int64_t id, int32_t hp);                                                 // plain record, acks=1
      int16_t txnPut(int64_t pid, int32_t seq, std::span<const std::pair<int64_t, int32_t>> rows);  // one transactional batch
      void commit(int64_t pid);
      void abort(int64_t pid);
      [[nodiscard]] bool driveUntil(const std::function<bool()>& done, int cap = 512);  // test-only predicate; worker + IO agent passes
      [[nodiscard]] std::optional<int64_t> hp(int32_t view, int64_t id);
      [[nodiscard]] std::optional<int64_t> count(int32_t view, int64_t hp);
      [[nodiscard]] int64_t leo() const;
      rune::node::WorkerViewSet& views();
      rune::node::ViewSnapshotIoAgent& io();
      rune::node::Worker& worker();
  };
  ```
  Views are compiled with `rune::view::compileView` against the source descriptor and installed with `installView` (or, with `recovery`, a `ViewPlanUpdateMsg` with `recovery = true` on `channel.install`, as `WorkerTest.cpp:820-840` does). The rig owns one `ViewSnapshotChannel`, a `WorkerViewSet` and a `ViewSnapshotIoAgent` (fsync window 0) over `dir/"views"`; every pass runs `worker.doWork(now)` then `io.doWork(now)` with `now += 1 ms`. A restart is a new `Rig` over the same `dir`.

- [ ] **Step 1: Write the failing worker tests** (`AbortedFoldWorkerRegressionTest.cpp`, suite `CheckedWritesAbortedFoldWorkerRegression`). Each negative check first drives until the fold has demonstrably passed the aborted range: `driveUntil([&]{ return views().foldCursorForTest(kView, 0) >= rig.leo(); })`.

```cpp
TEST(CheckedWritesAbortedFoldWorkerRegression, AbortedTxnNeverReachesProjectionOrCountViewRegression) {
    Rig rig(Backing::Disk, tmp("abort-disk"));
    rig.installProjection(kProj, /*persisted*/ true);
    rig.installCountByHp(kCount, /*persisted*/ true);
    ASSERT_EQ(rig.put(1, 5), 0);
    const std::pair<int64_t, int32_t> aborted[] = {{2, 5}, {1, 9}};
    ASSERT_EQ(rig.txnPut(/*pid*/ 7, /*seq*/ 0, aborted), 0);
    rig.abort(7);
    ASSERT_TRUE(rig.driveUntil(caughtUp(rig, {kProj, kCount})));
    EXPECT_EQ(rig.hp(kProj, 1), 5) << "an aborted overwrite reached the view";
    EXPECT_EQ(rig.hp(kProj, 2), std::nullopt);
    EXPECT_EQ(rig.count(kCount, 5), 1);
    EXPECT_EQ(rig.count(kCount, 9), std::nullopt);
    EXPECT_EQ(rig.views().foldCounters(kProj, 0)->skippedAborted, 1u);
}
TEST(CheckedWritesAbortedFoldWorkerRegression, TransientSourceAbortedTxnIsSkippedRegression)
    // Backing::Memory, transient views (persisted=false); same writes and assertions
TEST(CheckedWritesAbortedFoldWorkerRegression, InterleavedCommitAndAbortFromTwoProducersRegression)
    // txnPut(7,{1,1}) txnPut(8,{2,2}) abort(7) txnPut(8,{3,3}) commit(8) -> hp(1)==nullopt, hp(2)==2, hp(3)==3
TEST(CheckedWritesAbortedFoldWorkerRegression, OpenTxnThenAbortNeverFoldsItRegression)
    // put(1,1); txnPut(7,{2,2}); put(3,3); 64 passes -> foldCursorForTest == offset of the txn batch (LSO pin),
    // hp(3)==nullopt; abort(7); caught up -> hp(2)==nullopt, hp(3)==3
TEST(CheckedWritesAbortedFoldWorkerRegression, TransientOpenTxnThenAbortNeverFoldsItRegression)   // same on Backing::Memory
TEST(CheckedWritesAbortedFoldWorkerRegression, RestartRebuildSkipsTheAbortedRangeRegression)
    // Disk; install both views persisted; put(1,5); txnPut(7,{2,5}); abort(7); put(3,5); caught up;
    // delete the view files (cold rebuild); new Rig over the same dir with recovery installs; caught up ->
    // hp(2)==nullopt, count(5)==2, readiness Ready
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build/root -j14 --target rune_checked_writes_aborted_fold_tests && ctest --test-dir build/root -R CheckedWritesAbortedFoldWorker -j14 --output-on-failure`
Expected: compile FAIL on `foldCursorForTest`; after adding only that accessor, FAIL on every `hp(…, 2) == nullopt` / count assertion (the worker still passes `kNoSkip`).

- [ ] **Step 3: Thread the skip.** Add the parameters above. `driveRebuild` and `driveFolds` pass `skip` to `driveView_` (and `driveFolds` to `getOrCreate_`); `driveView_` passes it to both fold calls (`:663-668`, replacing Task 1/2's `kNoSkip`), adds `st.skippedAborted` to `work` (`:670-671`) and to `inst.counters.skippedAborted`. In `Worker::driveViews_` build `const rune::view::FoldSkip skip{.aborted = &rcState->aborted()};` right after `bound` (`:6434`) and pass it through `driveViewsOf_` to `materializeRecoveryInstancesFor` (its parameter is added in Task 7; until then pass only to the two drivers), `driveRebuild` and `driveFolds` (`:6489-6492`). Add `#include <rune/view/FoldSkip.h>` to `WorkerViewSet.h`. Update the `driveViews_` doc (`:6409-6421`): "… and the RC state's aborted-transaction index (FoldSkip), so a view never folds an aborted batch either (#537)."

- [ ] **Step 4: Add the CMake target**

```cmake
            # Checked writes Part 01 (#537): views skip aborted transactions on a real shard worker.
            add_executable(rune_checked_writes_aborted_fold_tests tests/integration/AbortedFoldWorkerRegressionTest.cpp)
            target_include_directories(rune_checked_writes_aborted_fold_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_checked_writes_aborted_fold_tests PRIVATE rune_node rune_view rune_replication
                                  aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_checked_writes_aborted_fold_tests PROPERTIES TIMEOUT 120 LABELS small
                                 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

- [ ] **Step 5: Run the tests**

Run: `cmake --build build/root -j14 --target rune_checked_writes_aborted_fold_tests rune_node_tests rune_worker_tests && ctest --test-dir build/root -R 'CheckedWritesAbortedFoldWorker|WorkerViewSet|ViewRecovery|ViewReadiness|WorkerRegression|GetViewRow|CreateView' -j14 --output-on-failure`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
        broker/libs/node/include/rune/node/Worker.h broker/libs/node/tests/support/AbortedFoldRig.h \
        broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "node: views fold with the partition's aborted-transaction index (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: The checkpoint reset frame in the IO agent

**Files:**
- Modify: `broker/libs/node/include/rune/node/ViewSnapshotChannel.h:39-45` (constant and the wire-format comment at `:16-24`), `broker/libs/node/include/rune/node/ViewSnapshotIoAgent.h:145` (declare `handleResetFrame_`), `broker/libs/node/src/ViewSnapshotIoAgent.cpp:281-292` (drain), new `handleResetFrame_`
- Test: `broker/libs/node/tests/node/ViewSnapshotIoAgentTest.cpp`

**Interfaces:**
- Produces: `static constexpr int32_t kResetMsgType = 4;` in `ViewSnapshotChannel` (frame `[viewTopicId i32][partition i32]`, exactly `kDeltaHeaderBytes`). `void ViewSnapshotIoAgent::handleResetFrame_(const uint8_t* p, std::size_t len, int channelIdx);` (private).

- [ ] **Step 1: Write the failing tests** (suite `CheckedWritesAbortedFoldIoAgentRegression`, reusing the file's `TempDir`, `frameDelta`, `testLayout`, `readFile` and `ManualNanoClock` helpers, `ViewSnapshotIoAgentTest.cpp:20-96`):

```cpp
TEST(CheckedWritesAbortedFoldIoAgentRegression, ResetTruncatesTheFileRegression)
    // delta O=10 hp 42, durable; reset frame; doWork -> file size 0, lastDurableOffset == -1
TEST(CheckedWritesAbortedFoldIoAgentRegression, ResetThenDeltaReplaysOnlyTheNewDeltaRegression)
    // delta O=10 hp 42; reset; delta O=3 hp 7; drive to durable ->
    // replay(file): exactly one record, O == 3, row 1 hp == 7; lastDurableOffset == 3 (never 10)
TEST(CheckedWritesAbortedFoldIoAgentRegression, ResetRestartsTheDurableOffsetRegression)
    // delta O=10 durable; reset + delta O=3 written in one drain but the fsync window not yet due ->
    // lastDurableOffset == -1 (never 10) until the next grouped fsync, then 3
TEST(CheckedWritesAbortedFoldIoAgentRegression, ResetClearsAnErroredStreamRegression)
    // forceWriteFailureForTest; delta -> hasError true; reset; delta O=5 -> hasError false, durable 5
TEST(CheckedWritesAbortedFoldIoAgentRegression, ResetOfAnUnknownKeyCreatesAnEmptyFileRegression)
    // reset for a never-seen key -> file exists with size 0; hasError false
TEST(CheckedWritesAbortedFoldIoAgentRegression, ShortResetFrameIsDropped)
    // a 4-byte frame of type 4 -> no file, no crash
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build/root -j14 --target rune_node_tests && ctest --test-dir build/root -R CheckedWritesAbortedFoldIoAgent -j14 --output-on-failure`
Expected: compile FAIL (`kResetMsgType`), then assertion FAIL (the drain ignores type 4).

- [ ] **Step 3: Implement.** The drain lambda (`:285-290`) dispatches type 4 to `handleResetFrame_`. `handleResetFrame_`: drop a frame shorter than `kDeltaHeaderBytes`; take the key's `FileState` through `fileFor_` (opens with `O_CREAT | O_WRONLY | O_APPEND`, so a reset of a new key creates an empty file), set `channelIdx`, then `::ftruncate(fd, 0)` and `rune::node::detail::platformFdatasync(fd)`. On success set `pendingO = lastDurableO = -1`, `dirtySinceSync = false`, `errored = false`, and log `spdlog::info("ViewSnapshotIoAgent: checkpoint reset viewTopicId={} partition={} (#537 refold)", …)`. On failure mark `errored = true` and log at error level with `std::strerror(errno)`, as the write-failure path does (`:184-196`). Writes after the truncation land at offset 0 because the fd is `O_APPEND`. Document the frame in the header comment of `ViewSnapshotChannel.h`.

- [ ] **Step 4: Run the tests**

Run: `ctest --test-dir build/root -R 'CheckedWritesAbortedFoldIoAgent|ViewSnapshotIoAgent' -j14 --output-on-failure`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broker/libs/node/include/rune/node/ViewSnapshotChannel.h broker/libs/node/include/rune/node/ViewSnapshotIoAgent.h \
        broker/libs/node/src/ViewSnapshotIoAgent.cpp broker/libs/node/tests/node/ViewSnapshotIoAgentTest.cpp
git commit -m "node: view snapshot IO agent truncates a checkpoint on a reset frame (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: SD15 — discard and refold an untrusted checkpoint

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h:130-139` (`checkpointsDiscarded`), `:191-247` (`CheckpointStream`, `ViewInstance::checkpointStream`), `:312-313`, `:331-337` (public signatures), `:583-584` (`loadRecoveryCheckpoint_`), `:612-619` (flush an owed reset in `driveView_`), private `tryPushReset_`
- Modify: `broker/libs/node/src/WorkerViewSet.cpp:333-364`, `:442-517`, `:519-537`, `:539-591`, after `:741` (`tryPushReset_`)
- Modify: `broker/libs/node/include/rune/node/Worker.h:6489` (pass `skip` to `materializeRecoveryInstancesFor`)
- Test: `broker/libs/node/tests/node/WorkerViewSetTest.cpp` (suite `CheckedWritesAbortedFoldViewSetRegression`), `broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp`

**Interfaces:**
- Consumes: `kResetMsgType` (Task 6); the threaded skip (Task 5).
- Produces:
  ```cpp
  enum class CheckpointStream : uint8_t { Live, ResetOwed };   // WorkerViewSet.h, before ViewInstance
  // ViewInstance: CheckpointStream checkpointStream = CheckpointStream::Live;
  // ViewFoldCounters: uint64_t checkpointsDiscarded = 0;   // SD15: v1/v2 checkpoint discarded (aborted txns in the source log)
  void materializeRecoveryInstancesFor(int32_t srcTopicId, int32_t partition, int64_t committedTail,
                                       const rune::view::FoldSkip& skip = rune::view::kNoSkip);
  void installViewForRecovery(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
                              rune::codec::Descriptor evolvingSrc, bool persisted, int32_t partition,
                              std::span<const uint8_t> base,
                              std::span<const std::span<const uint8_t>> deltasInOrder, int64_t committedTail,
                              const rune::view::FoldSkip& skip = rune::view::kNoSkip);
  // private:
  void loadRecoveryCheckpoint_(ViewInstance& inst, int32_t viewTopicId, int32_t partition,
                               int64_t committedTail, const rune::view::FoldSkip& skip);
  [[nodiscard]] bool tryPushReset_(ViewInstance& inst);   // false = ring full: hold, never drop
  void applyRecovered_(ViewInstance& inst, std::span<const uint8_t> base,
                       std::span<const std::span<const uint8_t>> deltasInOrder, int64_t committedTail,
                       const rune::view::FoldSkip& skip);   // the one recovery body (Step 4)
  ```
  Test helpers (file-local in `WorkerViewSetTest.cpp`): `records(bytes)` splits a checkpoint with `splitViewCheckpointRecords`; `firstFrameType(OneToOneByteRing&)` drains exactly one frame (`limit = 1`) and returns its type, or −1 when the ring is empty; `drive(expr)` repeats a driver call until it returns 0, hard cap 64.

- [ ] **Step 1: Write the failing view-set tests** (`WorkerViewSetTest.cpp`, reusing its `tmpDir`, plan and `readRange` fixtures, `:150-200`). `idx` is a test-owned `rune::transaction::AbortedTxnIndex` with one range `onAbortMarker(7, 1, 2)`; `contaminated` is a v1 checkpoint record (`encodeDelta`) at O = 3 holding row key 2 hp 99, which the clean log (key 1 hp 5 @0, aborted txn key 2 @1, marker @2) never produces.

```cpp
TEST(CheckedWritesAbortedFoldViewSetRegression, UntrustedCheckpointIsDiscardedAndRefoldedRegression) {
    vs.installViewForRecovery(plan, layout, src, /*persisted*/ true, 0, {}, records(contaminated), /*tail*/ 3,
                              rune::view::FoldSkip{.aborted = &idx});
    EXPECT_EQ(vs.foldCursorForTest(kView, 0), 0) << "refold from the log start";
    EXPECT_EQ(vs.readiness(kView, 0), ViewReadiness::RebuildingViews);
    EXPECT_EQ(vs.foldCounters(kView, 0)->checkpointsDiscarded, 1u);
    drive(vs.driveRebuild(5, 0, readRange, 3, 0, nullptr, 0, true, rune::view::FoldSkip{.aborted = &idx}));
    EXPECT_EQ(slotOf(vs, kView, 0, 1, 1).i, 5);
    EXPECT_EQ(rowOf(vs, kView, 0, 2), nullptr) << "the contaminated row survived the refold";
    EXPECT_EQ(firstFrameType(ch.deltas), ViewSnapshotChannel::kResetMsgType) << "reset precedes every delta";
}
TEST(CheckedWritesAbortedFoldViewSetRegression, CheckpointKeptWhenTheAbortedIndexIsEmptyRegression)
    // empty index -> foldCursorForTest == 3, row 2 hp 99 kept (no discard), checkpointsDiscarded == 0, no reset frame
TEST(CheckedWritesAbortedFoldViewSetRegression, TransientInstanceNeverDiscardsRegression)
    // persisted=false, non-empty index -> no reset frame ever, checkpointsDiscarded == 0
TEST(CheckedWritesAbortedFoldViewSetRegression, MaterializeFromFileDiscardsRegression)
    // the contaminated record written to deltaFilePath(dir, kView, 0); recovery-flagged install on the install ring;
    // materializeRecoveryInstancesFor(5, 0, 3, skip) -> as UntrustedCheckpoint…
TEST(CheckedWritesAbortedFoldViewSetRegression, LazyRecoveryOfASecondPartitionAlsoDiscardsRegression)
    // the ViewRecoveryRegressionTest.cpp:345 Bug-A ordering over partitions {0,1}, both files contaminated:
    // partition 0 rebuilt to Ready (prunes recoveryViews_), partition 1 created by driveFolds(…, skip) only ->
    // partition 1 discarded (checkpointsDiscarded == 1), its contaminated row absent once caught up
TEST(CheckedWritesAbortedFoldViewSetRegression, AggregateUntrustedCheckpointIsDiscardedRegression)
    // a v2 (encodeMultiDelta) contaminated record for a Count(*) view -> all four tables refolded, group of key 2 absent
TEST(CheckedWritesAbortedFoldViewSetRegression, ResetHeldUnderBackPressurePrecedesEveryDeltaRegression)
    // fill ch.deltas with filler frames until tryClaim fails; drive 4 passes -> foldCursorForTest == 0 (nothing folded
    // while the reset is held); drain the ring; drive -> the first frame for (kView,0) is type 4, then deltas
TEST(CheckedWritesAbortedFoldViewSetRegression, RefoldedFileReplaysCleanWithoutTheIndexRegression)
    // after the refold, run a ViewSnapshotIoAgent over the channel to durable; then a fresh view set recovers the
    // file with kNoSkip (retention pruned the range) -> row 2 absent, row 1 hp 5, foldCursorForTest == 3
```

- [ ] **Step 2: Write the failing worker tests** (`AbortedFoldWorkerRegressionTest.cpp`):

```cpp
TEST(CheckedWritesAbortedFoldWorkerRegression, ContaminatedCheckpointIsDiscardedAndRefoldedRegression)
    // Disk: put(1,5); txnPut(7,{2,9}); abort(7); caught up; stop the rig; overwrite the projection's checkpoint file
    // (io().deltaFilePathForTest) with a v1 record at O = leo() holding key 2 hp 9 (what the pre-fix fold wrote);
    // new Rig over the dir with a recovery install; caught up ->
    // hp(2)==nullopt, hp(1)==5, foldCounters->checkpointsDiscarded==1, the file replays to the same table
TEST(CheckedWritesAbortedFoldWorkerRegression, ContaminatedAggregateCheckpointIsDiscardedAndRefoldedRegression)
    // the same for installCountByHp with a v2 record counting the aborted record -> count(9)==nullopt, count(5)==1
TEST(CheckedWritesAbortedFoldWorkerRegression, CheckpointKeptWhenTheLogHasNoAbortedTxnRegression)
    // Disk, only commits; restart -> checkpointsDiscarded==0 and the recovered foldCursorForTest equals the
    // pre-restart cursor before any new fold (checkpoint used, nothing refolded)
```

- [ ] **Step 3: Run to verify they fail**

Run: `cmake --build build/root -j14 --target rune_node_tests rune_checked_writes_aborted_fold_tests && ctest --test-dir build/root -R 'CheckedWritesAbortedFold(ViewSet|Worker)' -j14 --output-on-failure`
Expected: compile FAIL on the new parameters and members, then FAIL on `checkpointsDiscarded` and the contaminated-row assertions.

- [ ] **Step 4: Implement the discard.** One private helper `void applyRecovered_(ViewInstance& inst, std::span<const uint8_t> base, std::span<const std::span<const uint8_t>> deltas, int64_t committedTail, const rune::view::FoldSkip& skip)` replaces the two identical bodies (`WorkerViewSet.cpp:524-536` and `:580-590`). When `inst.persisted && skip.aborted != nullptr && skip.aborted->size() != 0` (AD5) it skips `replayInstance`, keeps the empty tables, sets `foldCursor = 0`, `lastEncodedO = -1`, `transientCursor` unchanged (`CursorFresh`), `checkpointStream = CheckpointStream::ResetOwed`, `++counters.checkpointsDiscarded`, and logs once `spdlog::info("VIEW-CHECKPOINT-DISCARDED view={} partition={} abortedRanges={} — the source log holds aborted transactions; refolding from the log start (#537)", …)`. Otherwise it runs today's replay path. Both paths end with today's readiness rule (`foldCursor >= committedTail` → Ready, else RebuildingViews). `materializeRecoveryInstancesFor` and `installViewForRecovery` forward `skip`; `getOrCreate_` passes its `skip` to `loadRecoveryCheckpoint_`; `Worker::driveViewsOf_` passes `skip` to `materializeRecoveryInstancesFor` (`Worker.h:6489`).

- [ ] **Step 5: Implement the owed reset.** In `driveView_`, before step 1 (`:616`): `if (inst.checkpointStream == CheckpointStream::ResetOwed) { ++work; if (!tryPushReset_(inst)) return work; inst.checkpointStream = CheckpointStream::Live; }`. `tryPushReset_` claims `kDeltaHeaderBytes` with `kResetMsgType` on `channel_.deltas`, writes the two ids as `tryPushDelta_` does (`WorkerViewSet.cpp:724-741`), commits, and returns false on a failed claim. No allocation.

- [ ] **Step 6: Run the tests**

Run: `ctest --test-dir build/root -R 'CheckedWritesAbortedFold|WorkerViewSet|ViewRecovery|ViewReadiness|ViewSnapshotIoAgent|WorkerRegression|GetViewRow|CreateView' -j14 --output-on-failure`
Expected: PASS. Mutation check (do it, then revert): make the AD5 predicate `false` → `UntrustedCheckpoint…`, `MaterializeFromFile…`, `LazyRecovery…`, `Aggregate…` and both `Contaminated…` tests turn red; drop the reset push → `ResetHeld…` and `RefoldedFileReplaysClean…` turn red.

- [ ] **Step 7: Commit**

```bash
git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
        broker/libs/node/include/rune/node/Worker.h broker/libs/node/tests/node/WorkerViewSetTest.cpp \
        broker/libs/node/tests/integration/AbortedFoldWorkerRegressionTest.cpp
git commit -m "node: SD15 - discard and refold a view checkpoint over aborted transactions (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Checkpoint B

- [ ] **Step 1:** Run **CP-full** (Task 4, Steps 1–5). Expected: all green.
- [ ] **Step 2: Allocation check of the hot path.** `build/rel/broker/rune_broker_benchmark --benchmark_filter='^BM_ViewFold(Replace|ReplaceTxn|Aggregate|Retractable)$'` (after `cmake --build build/rel -j14 --target rune_broker_benchmark`). Expected: `steady_alloc=0` in every row (the `driveView_` reset branch is cold and not in these benches; the fold skip allocates nothing).

---

## Phase 3 — Real brokers, e2e and docs

### Task 9: Real brokers (ThreeNodes, shard mode)

**Files:**
- Create: `broker/libs/node/tests/client/CheckedWritesAbortedFoldBrokerRegressionTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt:1811-1813` (add the source to `rune_client_failover_broker_tests`; it kills brokers, so it stays in this RUN_SERIAL binary)

**Interfaces:**
- Consumes: `ThreeNodes` (`broker/libs/node/tests/support/CatalogBrokerCluster.h:181`, default `Mode::Shard`; `crash`, `stop`, `startNode`), `connectClient`, `clientConfigFor`, `leaderOn`, `awaitController` (`tests/client/ClusterClient.h`), `charBlob()` and `viewPlanBlob()` (`tests/support/DdlHarness.h:60-87`).
- Produces (file-local): `countByHpBlob(const std::string& name, int32_t srcTopicId)` (authored plan: key `"hp"`, source `{srcTopicId, "hp"}`, outputs `{hp Int64 Replace "hp"}`, `{n Int64 CountAll ""}`, persistence 0); `charValue(int64_t id, int64_t hp)` (the 20-byte value `ClientFailoverBrokerRegressionTest.cpp:844-846` builds); `txnWrite(Client&, const TopicHandle&, const std::string& txnId, std::span<const std::pair<int64_t,int64_t>> rows, bool commit)` (a `transactionalId` producer with `acks = All`: `initTransactionsAndWait`, `beginTransaction`, `send` each row, `flushAndWait`, then `commitTransactionAndWait` or `abortTransactionAndWait`, 60 s bounds; returns false on any exception).

- [ ] **Step 1: Write the tests** (suite `CheckedWritesAbortedFoldBrokerRegression`). Setup common to all three: `ThreeNodes::start(tag)`, controller, typed topic `"src"` (1 partition, rf 3, `charBlob()`, disk), Replace view `"v"` over it and the aggregate view `"n_by_hp"` (`countByHpBlob`). A negative read is valid only after a later committed record of the same partition is visible (one partition, folded in offset order).

```cpp
TEST(CheckedWritesAbortedFoldBrokerRegression, AbortedTransactionNeverReachesProjectionOrCountViewRegression)
    // txnWrite("a1", {(1,5),(2,5)}, abort); txnWrite("c1", {(3,5)}, commit)
    // -> getViewRow(v,3).hp == 5 (waited, bounded 60 s); then getViewRow(v,1) and (v,2) == kAbsentKey;
    //    getViewRow(n_by_hp,5).n == 1
TEST(CheckedWritesAbortedFoldBrokerRegression, FullClusterRestartRebuildSkipsTheAbortedRangeRegression)
    // the writes above; stop(1..3); startNode(1..3); await the catalog leader and the partition leader ->
    // the same three reads, through a fresh client (rebuild + SD15 refold on the new leader)
TEST(CheckedWritesAbortedFoldBrokerRegression, LeaderFailoverWithAnOpenTransactionThatLaterAbortsRegression)
    // the client is seeded on a broker other than the source leader; producer "a2": begin, send (7,9),
    // flushAndWait (the record is on the log, the txn open); crash(source leader);
    // abortTransactionAndWait (bounded 150 s; the coordinator writes the abort marker on the new leader — if the
    // crashed broker was also the controller, the client re-finds the coordinator, as
    // EosBrokerRegression.CoordinatorCrashedUnderANonSeedClientCommitsAfterTheMoveRegression shows; same assertions);
    // txnWrite("c2", {(8,9)}, commit) -> getViewRow(v,8).hp == 9; getViewRow(v,7) == kAbsentKey;
    // getViewRow(n_by_hp,9).n == 1
```

- [ ] **Step 2: Run them**

Run: `cmake --build build/root -j14 --target rune_client_failover_broker_tests && ctest --test-dir build/root -R CheckedWritesAbortedFoldBroker --output-on-failure`
Expected: PASS (serial). Mutation check (do it, then revert): pass `rune::view::kNoSkip` instead of `skip` in `Worker::driveViewsOf_` → the first test fails on `getViewRow(v,1)` and the third on `getViewRow(v,7)`.

- [ ] **Step 3: Commit**

```bash
git add broker/libs/node/tests/client/CheckedWritesAbortedFoldBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "test: aborted transactions never reach views on three real brokers (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Docker e2e leg and docs

**Files:**
- Modify: `client-cpp/examples/view_probe.cpp` (setup flow after step 7, before `VIEW-PROBE-SETUP: OK`; `verify_restart` flow after its `user_totals` checks, `:1470-1486`; the mode comment at `:14-60`)
- Modify: `broker/docker/view-engine-e2e.sh` (header comment `:14-59`; sentinel checks next to `:320` and `:522`)
- Modify: `docs/api/client-cpp.md` (the bullet list under "Materialized views", after the "A row is served only by …" bullet), `docs/operations.md` ("Upgrades", after the "View reads during a rolling upgrade" bullet)

**Interfaces:**
- Consumes: the probe's `encodeCharacter`, `sendOrder`, `waitForRow`, `waitForHp`, `waitForTotals`, `waitForAbsent` (`view_probe.cpp:197-416`); its `transientTxn` pattern (`:635-661`).
- Produces (probe, file-local): `static bool txnCharacters(Client&, const TopicHandle&, const std::string& txnId, std::span<const std::pair<int64_t,int64_t>> idHp, bool commit, int timeoutMs)` and `static bool txnOrders(Client&, const TopicHandle&, const std::string& txnId, std::span<const std::array<int64_t,3>> orderUserAmount, bool commit, int timeoutMs)`. Exit codes **170–175** (setup) and **176–179** (`verify_restart`); confirm they are unused with `grep -n "return 17[0-9];" client-cpp/examples/view_probe.cpp` (0 hits today).

- [ ] **Step 1: Add the setup leg "8. #537: views skip aborted transactions".** On `character`: `txnCharacters("e2e-537-abort", {(5370, 1), (4242, 999)}, abort)` (170), `txnCharacters("e2e-537-commit", {(5371, 7)}, commit)` (171); `waitForHp(char_persisted, 5371, 7)` and `waitForHp(char_transient, 5371, 7)` (172); then `char_persisted` and `char_transient` keep `4242 → hp 100` and answer `kAbsentKey` for 5370 (173). On `order`: `txnOrders("e2e-537-orders", {(9, 100, 1000), (1, 100, 9999)}, abort)` (174), then the committed witness `sendOrder(c, ordTh, 10, 300, 1)` and `waitForTotals(u300, {1, 1, 1.0, 1, 1, 1})`, followed by `waitForTotals(u100, {3, 85, 85.0 / 3, 50, 5, 5})` (175). The witness goes to a new user on purpose: any committed write to u100 would change its `Last` column, which `verify_restart` check 41 pins at 5. Print `ABORTED-FOLD: OK` on stdout before `VIEW-PROBE-SETUP: OK`.
- [ ] **Step 2: Add the `verify_restart` checks** right after the `user_totals` recovery check (`:1471-1473`) and before the post-restart writes: `char_persisted` 5370 → `kAbsentKey` and 5371 → hp 7 (176), `char_persisted` 4242 → hp 100 is already check (a); `user_totals` u100 is already check 41 (the SD15 refold must reproduce it exactly). Print `ABORTED-FOLD-RESTART: OK` before `VIEW-ENGINE-E2E: OK`.
- [ ] **Step 3: Assert the sentinels in the script**: `grep -q "ABORTED-FOLD: OK"` on `setup_out` (fail message "setup client did not print the 'ABORTED-FOLD: OK' sentinel") and `grep -q "ABORTED-FOLD-RESTART: OK"` on `restart_out`. Add both legs to the header comment.
- [ ] **Step 4: Build the probe locally**

Run: `cmake --build build/sub-client-cpp -j14 --target view_probe`
Expected: builds clean.

- [ ] **Step 5: Docs.**
  - `docs/api/client-cpp.md`, new bullet: "- Views fold committed data only. The records of an aborted transaction never reach a view (a row, a `count(*)`, a `Min`/`Max`), on the live fold, after a restart and after a failover."
  - `docs/operations.md`, "Upgrades", new bullet: "- **Views over partitions with aborted transactions.** On the first start of a build with the #537 fix (and on every restart while the source log still holds an aborted transaction), a persisted view whose source partition holds an aborted transaction discards its checkpoint and rebuilds from the log start. Its reads answer `NotReady` until the rebuild ends. The rebuilt view equals a fresh fold of the retained log, so rows whose source records retention already deleted are gone afterwards."
- [ ] **Step 6: Commit**

```bash
git add client-cpp/examples/view_probe.cpp broker/docker/view-engine-e2e.sh docs/api/client-cpp.md docs/operations.md
git commit -m "e2e, docs: views skip aborted transactions across a restart (#537)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Checkpoint C (before the PR) and Finish

- [ ] **Step 1:** Merge the latest `main` (`git fetch && git merge origin/main`), rebuild, and run **CP-full** (Task 4, Steps 1–5). Expected: all green.
- [ ] **Step 2: Real broker slice.** `ctest --test-dir build/root -R 'CheckedWritesAbortedFold' --output-on-failure` (every layer, serial for the broker binary). Expected: PASS.
- [ ] **Step 3: Benches, ABAB n=3 on the VM** (prepared once with `broker/bench/vm-bench-prepare.sh`, restored with `vm-bench-restore.sh`, everything under `flock /root/vm.lock`). Build `build/rel` of the base commit (`git merge-base HEAD origin/main`) in a separate worktree and of the tip; alternate base, tip, base, tip, base, tip of `rune_broker_benchmark --benchmark_filter='^BM_ViewFold(Replace|Aggregate|Retractable)$'`, then the tip alone for `^BM_ViewFoldReplaceTxn$`. Expected: no percentile of `BM_ViewFoldReplace`, `BM_ViewFoldAggregate` or `BM_ViewFoldRetractable` worse than base beyond the run-to-run band; every `*_gate_pass=1`; `BM_ViewFoldReplaceTxn` `txn_record_p99_us < 5`, `txn_batch_p99_us < 10`, `steady_alloc=0`. Record the numbers in the PR body (not in `BENCHMARKING.md`; these are microbenches).
- [ ] **Step 4: Latency gates** on an idle host: `ctest --test-dir build/rel -L latency --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Docker e2e on the VM**: in your own directory, `flock /root/vm.lock bash broker/docker/view-engine-e2e.sh`, checking `df` before and after and removing only your own containers and images. Expected: `ABORTED-FOLD: OK`, `ABORTED-FOLD-RESTART: OK` and `VIEW-ENGINE-E2E: OK`, exit 0.
- [ ] **Step 6: File the deferred items** as labelled issues on `arcanumruna/rune` (dedupe with `gh issue list --search` first; one summary at the end):
  1. `bug`, `sev:medium`, `area:views`: a pre-fix v1/v2 checkpoint is still trusted when retention or compaction removed the whole aborted range (data and marker) before the first restart on a fixed build; the checkpoint's aborted rows survive. Closed by Part 07's checkpoint v3 if v3 lets recovery tell pre-fix checkpoints apart. Test reference: `CheckedWritesAbortedFoldViewSetRegression.RefoldedFileReplaysCleanWithoutTheIndexRegression` (the clean case); Origin: Part 01 plan, SD15.
  2. `enhancement`, `sev:low`, `area:views`: SD15 refolds a persisted view from the log start on **every** restart while its source log holds any aborted transaction; with checkpoint v3 provenance the refold is needed once. Origin: Part 01 plan, SD15.
- [ ] **Step 7: Open the PR** from `feature/checked-writes-01-aborted-fold` to `main` (only once the owner has agreed to pushes for this program; never push otherwise). Body: summary, "Fixes #537", the bench table, the e2e output lines, the deviations list (empty if none), and the trailer `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. The reviewer (most capable model) reviews the whole views fold area, approves, merges and closes #537 with `gh issue close 537 -c "Fixed in <sha>: views skip aborted transactions and refold untrusted checkpoints (regression: CheckedWritesAbortedFold*)"`.

---

## Self-review (done while writing)

- **Spec coverage.** §8.8 last bullet → Tasks 1, 2, 5. §16 item 1 "and re-fold" → Task 7 (SD15). #537 fix direction: the predicate from the source partition's `RcState` → Task 5 (AD4); projection, aggregate, rebuild and transient folds → Tasks 1, 2, 5 (one `driveView_` serves all four); tests for commit and abort, two interleaved producers, an abort spanning a capped pass, restart rebuild over an aborted range, leader failover with an open transaction that later aborts → Tasks 1–3, 5, 7, 9; "existing persisted views may need a re-fold" → Task 7 and the docs in Task 10. C9.2 (header, member, `kNoSkip`, link) → Task 1. Hand-over "a view over an aborted transaction equals a fresh refold" → the matrix and model invariants (Task 3) and `RefoldedFileReplaysClean…` (Task 7).
- **Step scan.** Every code step names the file, the signature and the values (AD1–AD7); bodies are left to the implementer except the AD2 branch rules and the CMake block. Test steps carry the assertions with values. Task 10 Step 1 records one real open question (the probe's MRU expectations) with both options and how to decide.
- **Type consistency.** `FoldSkip`, `kNoSkip`, `isAbortedBatch`, `skippedAborted`, `checkpointsDiscarded`, `CheckpointStream::{Live, ResetOwed}`, `kResetMsgType`, `foldCursorForTest`, `tryPushReset_`, `applyRecovered_` are spelled the same in every task. The skip parameter is last everywhere, defaulted only on the four public `WorkerViewSet` entry points.
- **Review Focus.** Five lines, each with its test added to the owning task (1, 2, 6, 7).
- **Proportion.** About 790 lines (the spec is about 1000) for a three-phase part touching two fold loops, the view-set recovery and one IO message; code blocks are signatures, test assertions and one CMake block.
