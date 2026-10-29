# Checked Writes — Part 03: the Write Hook, `View` Acks and the v12 Wire — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put one template write-hook policy inside `rune::storage::handleProduceBatch`, after idempotent dedupe, that applies the §11.2 per-write requirements for every typed topic. Make acks bit flags with a working `View` flag: a `View` ack is sent once every view of the partition has folded past the batch. Ship the client-version-12 / broker-version-7 wire skeleton: codes 51–58, the `acks` bit decode, and `ProduceResponse` v12 with `assignedBase` and an optional verdict trailer that stays empty until Part 04.

**Architecture:**
- **Storage.** `handleProduceBatch<LogT, HookT>` calls `check()` between the dedupe gate and the min-ISR gate, `stage()` before the append, and `onAppended()` after the PSM update. Each call sits under `if constexpr (HookT::kActive)`. The existing signature forwards with `NoWriteHook`, so untyped topics compile to today's code.
- **Node.** `rune::node::CheckedWriteHook` is a per-call object built in `Worker::produceCore_` from the partition's `PartitionWriteState*` (Part 02, C7).
  - It does validation and the wire-key check through Part 02's `checkTypedRecord` (moved in from Part 02's pre-append call site, SD2, R-X1), the record intent scan and one pure requirement table, `evaluateRequirements`.
  - Later parts fill the table's topic facts (key index, autoincrement, constrained, verdict source).
- **View acks.** `View` waiters park in their own `OrderedWaitQueue<PendingAcksView>` (SD16).
  - `flushOwedAcksView_` releases one when the HW is above `lastOffset`, the ISR still meets the admission minimum for `All`, and `WorkerViewSet::minFoldCursor` over the current leadership is above `lastOffset`.
- **Client.** `Acks` becomes a flag set. The client sends bit 1 only to a v7 broker (SD11).

**Tech Stack:** C++23. GoogleTest (`TEST_P` for the matrices), Google Benchmark through `rune_broker_benchmark`, and CMake/Ninja (`build/root`, `build/sub-client-cpp`, `build/sub-codec`, `build/rel`, `build/asan`, `build/tsan`, `build/client-tsan`). Real Aeron (embedded invoker driver over `aeron:ipc`) for worker-level ack tests, and real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`) for the real e2e slice.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`. This part covers §3 (write path), §8.9, §11.1, §11.2 and the wire half of §12. The binding cross-part contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md`: C1, C3, C4, C5, C6, C8, and C2/C7 as Part 02 hands them over. Executors read both, plus this file.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-03-write-hook`. Cut it from the latest `main` (`git fetch` first) **after Part 02 has merged**, then merge `main` in again before review.
- **Preconditions** (run before Task 1; each must match, else stop and tell the controller):
  - `grep -n "kCheckedWrites *= 4" broker/libs/replication/include/rune/replication/ReplicationMessages.h` (1 hit, from 02)
  - `grep -n "struct PartitionWriteState" broker/libs/node/include/rune/node/WriteState.h` (1 hit, from 02)
  - `grep -n "checkedWritesEnabled_\|PartitionWriteState\* writeOf_" broker/libs/node/include/rune/node/Worker.h` (≥ 2 hits, from 02)
  - `grep -n "kClientVersion  *= 11" protocol/include/rune/protocol/client/Protocol.h` (1 hit: 03 is the first part to bump it)
  - `grep -n "ConstraintViolation" protocol/include/rune/protocol/client/Protocol.h` (0 hits)
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp`: the client suites (`rune_invoker_tests`, `rune_client_tests`, `rune_cli_offline_tests`).
  - `build/rel`: Release, `BUILD_BENCHMARKS=ON`. The bench binary is `build/rel/broker/rune_broker_benchmark`.
  - Sanitizer builds: `build/asan`, `build/tsan`, `build/client-tsan`.
- **Line numbers.** They cite `main` before Part 02. Part 02 edits `Worker.h`, so anchor on the named function or member, not on the number.
- **Commits.** Use `git add <paths>` only. Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Nothing is pushed. Never run `conan create/export/upload`.

## Global Constraints

These lines are verbatim from the overview's Global Constraints, restricted to what this part touches. Every task includes them implicitly.

- **Error codes** (`protocol/include/rune/protocol/client/Protocol.h:105`, `ErrorCode`, append-only):
  - `ConstraintViolation = 51`: the record broke a rule when it was folded. Constraint id `viewObjectId << 16 | ordinal`, ordinal 0 = `unique`. Fold, per record, definite.
  - `KeyExists = 52`: `insert` on a key that exists. Fold, per record, definite.
  - `KeyNotFound = 53`: `update` or delete-existing on a key that's absent. Fold, per record, definite.
  - `AutoIncrementExhausted = 54`: the counter has reached `autoincrement.max`. The batch isn't stored. Write, definite.
  - `TxnTooLarge = 55`: a transaction wrote more than `txn.judged.records.max` records to one voting partition. The transaction aborts. Vote, definite.
  - `ViewKeyExpressionInvalid = 56`, `ViewConstraintInvalid = 57`, `ViewUniqueInvalid = 58`: compile errors (ruling SD3).
  - Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.
  - "Definite" for 51–53 means *never visible*. 51–53 reach only `View` writers on client version 12.
- **Versions.**
  - `kClientVersionCheckedWrites = 12` and `kClientVersion = 12`.
  - `kBrokerVersionCheckedWrites = 7` and `kBrokerVersion = 7`. Today `kClientVersion` is 11 and `kBrokerVersion` is 6 (`Protocol.h:327`, `Protocol.h:360`).
  - Feature level `MetadataFeature::kCheckedWrites = 4` and `kLatest = kCheckedWrites` (`broker/libs/replication/include/rune/replication/ReplicationMessages.h:470-472`; today `kLatest = kBoundProducerIds = 3`).
- **Values.**
  - The `autoincrement` side window holds **32** batches = `kIdempotentMaxInFlightBatches` (`Protocol.h:16`).
  - Wire `acks` byte: bit 0 = `All`, bit 1 = `View`. Other bits are refused with `InvalidRequest`.
  - Intent bits: record `attributes` bits 0–1. 0 = upsert/delete, 1 = insert, 2 = update/delete-existing. 3 and the other bits are refused with `InvalidRequest`.
- **Budgets** (hard gates, spec §14):
  - Write hook: autoincrement assignment, including the scratch copy, < **1 µs p50** per batch of up to 4 KiB (`ProduceValidateBench`).
  - New typed-topic validation: ≤ **1 µs p50** for a one-record batch.
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
- **Names.** Every new test suite name starts with `CheckedWrites`, so `ctest -R CheckedWrites` selects the program. Every fix test carries `Regression` in its suite or name.

## Rulings this part depends on (restated from the overview; binding)

- **SD2.** Validation runs before dedupe today (`schemaRejects_` in `produceCore_`, `Worker.h:8255`, then `producerState.validate`, `ProducePath.cpp:205`). Part 02 kept the call site and moved it onto the per-partition pointer. **This part moves it into the hook, after dedupe.**
- **SD4.** A batch written with `View` to a verdict source holds at most **4096 records** (`kMaxJudgedBatchRecords`). The client splits such batches. The hook refuses a bigger one with `InvalidRequest`. A `ProduceResponse` is therefore at most 32,824 bytes, and the ack scratch is sized to `ProduceResponse::kMaxEncodedSize`.
- **SD6.** Part 02 defines `kCheckedWrites` and its propagation (C2). This part uses `Worker::checkedWritesEnabled_()`.
- **SD10.** In legacy worker mode, a produce with the `View` bit, any non-zero intent, or a key-0 create is answered `InvalidRequest`. Plain writes are judged as usual.
- **SD11.** The client gates `View`, intents and key-0 creates on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`. Below that it fails locally with `OpStatus::BrokerTooOld`. A v7 broker whose committed level is below 4 answers those writes `InvalidRequest`, and nothing is appended.
- **SD12.** The gateway (`msg.acks == 1`, `ClientGateway.cpp:3872`) and the worker (`msg.acks != 0`, `Worker.h:8082`) both move to the one decoder in C4.
- **SD16.** A `View` waiter behind an open transaction waits for the LSO. `View` waiters live in their own queue, so `All` acks never queue behind them.
- **R-X1.** Part 02 factors `TypedCheck checkTypedRecord(const TopicWriteSpec&, const RecordSpan&, int16_t level)` into `WriteState.h`, carrying all of 02's rules (P1–P5). This part's single walk calls it per record and adds `HookRefusal::VersionAhead` → `NotLeader`. 02's `typedRejects_` / `typedVersionAhead_` counters and test seams stay, and no 02 rule or test is lost.
- **R02a.** `RecordSpan::key` and the bounds-checked `forEachRecordSpan` are Part 02's (its P9). This part consumes them and adds only `attributes` and `index`.
- **R02b / R03e.** Tombstones skip value validation at every level; on a keyed topic the key must be exactly `keyWidth` bytes. Part 02's P1 is the rule, applied through `checkTypedRecord`.
- **R03a–R03d.** Large acks are offered by Part 04 (R03a); untyped topics store intent bits opaquely and the client refuses intents, key-0 creates and `View` there locally with `SendResult::WriteNotAllowed` (R03b as amended by R05a); the client never sets `View` on a transactional send and the broker ignores it (R03c); this part's matrix ids are 120–139 in Part 04's shared matrix (R03d).

## Part rulings (P3; binding for executors, recorded so the reviewer can overturn any)

- **P3-1 — Frame checks vs record checks.**
  - **Frame checks, every topic.** `produceCore_` refuses a `View` produce with `InvalidRequest` before any append, for **every** topic, typed or not:
    - in legacy mode (`!workerSideAcks()`, SD10);
    - below the level (`!checkedWritesEnabled_()`, SD11);
    - with no replica wired (a rig-only path; nothing can fold).
  - **Record checks, typed topics only.** Intents, reserved bits and key-0 creates are checked by `CheckedWriteHook`, which runs only for typed topics.
  - **Untyped topics.** An untyped topic (`writeOf_ == nullptr`) never has its record attributes scanned: G10 allows "one branch that is never taken". Untyped topics have no key index, so a raw producer's intent bits there are stored as sent and mean nothing (R03b). The client refuses intents, key-0 creates and `View` on a raw handle locally with `SendResult::WriteNotAllowed` (R05a; Part 05 builds that client check).
- **P3-2 — `View` is ignored on transactional batches (R03c).** The client never sets bit 1 on a transactional producer's batches. Transactions get verdicts through Part 11's vote (`commitTransactionAndWait`). The broker clears it from a batch whose header has `kAttrTransactional` before any other check (spec G.7: "View on a single send is ignored").
  - Without this, the `View` ack of a transactional send waits for the LSO, the LSO waits for `EndTxn`, and `commitTransaction()` waits for the send's ack: a deadlock until the 30 s ack deadline.
- **P3-3 — Leadership-scoped fold cursor.** `minFoldCursor` takes a leadership generation. An instance folded under another leadership counts as 0. The waiter uses `replica->leadershipGeneration()`. This mirrors the read barrier (`Worker.h:4100-4103`), so an ex-leader's frozen table can never release a `View` ack.
- **P3-4 — Client gate source.** The client's SD11 gate reads the **seed** session's `brokerVersion` (`Client::onSeedConnected_`, `Client.cpp:2167`). That is the same source `GroupConsumerHub::setBrokerVersion` uses (`Client.cpp:2228`).
  - 0 (unknown) sends.
  - A known value from 1 to 6 fails the batch locally with `BrokerTooOld`.
  - In a mixed cluster, a v6 partition leader decodes `acks` 2 or 3 as undecodable and answers `InvalidRequest` (`DataMessages.h:141`): definite, never a silent downgrade.
- **P3-5 — Old sessions never see 51–58.** `produceErrorForClient` maps every code from 51 to 58 to `InvalidRequest` for a session below 12. They are unreachable for such a session by construction. This is the belt for Review Focus 3 of the overview.
- **P3-6 — The 4096-record `View` batch cap on the client** (SD4):
  - A producer whose acks include `View` clamps `batchRecords` to ≤ 4096 at creation, reusing the existing append-time and flush-time `batchRecords` enforcement (`ProducerImpl.cpp:1578`, `:1652`).
  - `claimBatch(n > 4096)` returns `SendResult::MessageTooLarge`.
  - `BatchBuilder::add` beyond 4096 records throws `std::length_error("BatchBuilder::add: MessageTooLarge: a View batch holds at most 4096 records")`.
- **P3-7 — `flushOwedAcksView_` placement.** It runs right after the `if (viewSet_ != nullptr) { … serveHeldViewReads_(); }` block in both duty loops (`Worker.h:1846-1849`, `:1975-1980`), not inside it. A worker without a view set must still release and time out `View` waiters: no views means the cursor is `INT64_MAX`.
- **P3-8 — Order of refusals inside `check()`.** `check()` walks the records once, in order.
  - A record with reserved attribute bits or intent 3 ends the walk with `InvalidRequest`.
  - A record that `checkTypedRecord` rejects (validation or the wire-key check) ends it with `SchemaValidationFailed`; one it reports `VersionAhead` ends it with a retriable `NotLeader` (02 P2, R-X1).
  - After a clean walk, `evaluateRequirements` applies C8 rules 2–9 in order.
  - A batch that is both invalid and forbidden is therefore answered by its first bad record. The answer is deterministic.
- **P3-9 — Strict durability with `View`.** The fsync, then the HW, then the fold: `DurablePending` hands a `View` entry to `pendingAcksView_`, not to `pendingAcksAll_`.
- **P3-10 — Tombstones (R02b, R03e).** Part 02's P1 is the rule, applied through `checkTypedRecord`: a record with an empty or null value (C3) skips the value validation; on a keyed typed topic with a checkable key its wire key must be exactly `keyWidth` bytes (level 4); on a keyless typed topic it is rejected (24).
- **P3-11 — Large acks (R03a).** At 4096 entries, a v12 ack (32,824 B) exceeds an Aeron publication's `maxPayloadLength`, so `tryClaim` throws. In 03 the trailer is always empty (≤ 56 B). Part 04's ack path `offer`s any frame above `maxPayloadLength` and claims the rest (04 P4-9).

## Contract additions (this part adds them; the overview stays the authority for everything else)

1. `protocol/include/rune/protocol/client/DataMessages.h`: `inline constexpr int32_t kMaxJudgedBatchRecords = 4096;` with `static_assert(kMaxJudgedBatchRecords == ProduceResponse::kMaxVerdictEntries)`. The client and the hook share it. `rune::node::kMaxJudgedBatchRecords` aliases it.
2. `ProduceResponse` additions:
   - `static constexpr std::size_t ENCODED_SIZE_V12 = 54;`
   - `[[nodiscard]] static constexpr std::size_t encodedSizeFor(int32_t clientVersion, bool hasVerdicts, uint16_t verdictCount) noexcept;`
   - `[[nodiscard]] std::size_t encodedSize(int32_t clientVersion) const noexcept;`
   - The existing one-argument `encodedSizeFor(clientVersion)` stays (no trailer).
3. `WriteFacts` (C8) appends `bool levelCheckedWrites = false;` (the rule 3 input). Its other members get default initializers: `uint8_t acks = 0; bool idempotent = false; …`.
4. An active hook policy has a fourth member, `[[nodiscard]] uint64_t assignedBase() const noexcept`. `handleProduceBatch` reads it right after `onAppended` into `ProduceOutcome::assignedBase`. `NoWriteHook` leaves 0.
5. `broker/libs/node/include/rune/node/CheckedWriteHook.h` adds the requirement seam that 04, 05, 06, 09 and 11 extend: `struct TopicHookFacts`, `[[nodiscard]] TopicHookFacts topicHookFactsOf(const PartitionWriteState&) noexcept`, `struct BatchIntents`, `struct RequirementVerdict { rune::protocol::client::ErrorCode code{}; HookRefusal why = HookRefusal::None; }`, `[[nodiscard]] constexpr RequirementVerdict evaluateRequirements(const BatchIntents&, const WriteFacts&, const TopicHookFacts&) noexcept`, and `enum class HookRefusal : uint8_t`. Later parts append `HookRefusal` values before `Count_` and may return a code other than `InvalidRequest` (05: `{NotLeader, KeyIndexActivating}`). Exact definitions are in Task 6.
6. `WorkerViewSet::minFoldCursor(int32_t srcTopicId, int32_t partition, uint64_t leadership = 0) const noexcept`. With `leadership != 0`, an instance whose `foldLeadership != leadership` counts as 0 (P3-3). The C4 two-argument call is unchanged.
7. `broker/libs/node/include/rune/node/SessionAckSink.h`: `inline constexpr uint32_t kNoVerdictRun = 0xFFFF'FFFFu;`.
8. Client:
   - `[[nodiscard]] constexpr Acks normalized(Acks) noexcept` and `[[nodiscard]] constexpr bool acksValid(Acks) noexcept` in `Producer.h`;
   - `void ProducerImpl::setBrokerVersion(int32_t) noexcept`;
   - `void ProducerHub::setBrokerVersion(int32_t) noexcept`, which forwards to every registered producer and to every later one.
9. Test seams on `Worker`: `[[nodiscard]] uint64_t hookChecksForTest() const noexcept` (how many times `check()` ran) and `[[nodiscard]] uint64_t hookRefusalsForTest(HookRefusal) const noexcept`.
10. Matrix ids (R03d): 03's matrix lives in its own file with local enums until Part 04 folds it into `CheckedWritesMatrixExpectations.h`, with **120–139** for both `Op` and `Entity`.

## Review Focus

These are the five inputs the spec implies but no requirement row names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A `View` ack while the partition's view tables were folded under an earlier leadership** (a quick demote/re-promote, or a duplicate retry whose original offset sits below a stale `foldCursor`). It must wait for a fold under the current leadership, never fire on the stale cursor.
   - Test: `CheckedWritesViewAckRegression.StaleLeadershipFoldCursorNeverReleasesAViewAckRegression` (Task 9).
2. **A transactional producer configured `All | View`.** `commitTransactionAndWait()` must complete normally, without a 30 s stall. The broker ignores the bit if a raw client sends it (P3-2).
   - Tests: `CheckedWritesAcksRegression.TransactionalProducerNeverSendsTheViewBitRegression` (Task 4) and `CheckedWritesViewAckRegression.ViewBitOnATransactionalBatchIsIgnoredRegression` (Task 7).
3. **A gateway-routed `View` produce when the return-ring reservation is exhausted.** It is refused `NotLeader` with nothing appended, never parked without a reserved slot. `reservedAckSlots_()` counts `View` waiters, so `All` intake throttles on them too.
   - Test: `CheckedWritesViewAckRegression.RoutedViewProduceWithoutAReservationIsRefusedNotParkedRegression` (Task 9).
4. **Strict durability tier + `View`.** The ack waits for the fsync, then the HW, then the fold, in that order, and never fires on fsync alone.
   - Test: `CheckedWritesViewAckRegression.StrictTierViewWaitsForFsyncHwAndFoldRegression` (Task 9).
5. **A session below version 12** on a v7 broker must keep getting the exact 46 B (or 38 B) `ProduceResponse` and never see codes 51–58. That holds even for an answer site that sets `assignedBase` or `hasVerdicts`.
   - Tests: `CheckedWritesWireRegression.EncodeForBelowTwelveIsByteIdenticalRegression` (Task 3) and `CheckedWritesAckSinkRegression.OldSessionGetsTheOldFrameWhateverTheEntryCarriesRegression` (Task 9).

---

## Interaction matrix

This part owns the overview's §15 cells listed for **03**:

- "Clients": intents without `View` or from a non-idempotent producer refused; autoincrement creates without `All` refused (the rule only).
- Every `View`-flag bullet.
- "Before Part G, a transactional and an atomic batch on a constrained source are refused while unconstrained ones work."

Constrained sources do not exist before Part 04, so that row is pinned as a rule (matrix B) and its positive half on a real unconstrained source (A, B, D). The atomic-batch half cannot be sent before Part 11 defines the atomic header bit (R05b): `WriteFacts::atomicBatch` stays false until then, so it stays a rule cell (row 15); Part 11 ships the bit and lifts the rule together, so no broker cell exists. The cells live in four matrices.

### A. The `View` ack matrix (`CheckedWritesViewAckMatrix`, Task 9; 2 × 4 × 10 = 80 cells)

- **Ops (2):** `LV` = `Leader | View` (wire 2), `AV` = `All | View` (wire 3).
- **Entities (4):**
  - `NoViews`: a typed disk partition, no view.
  - `OneView`: one projection view over it.
  - `TwoViewsLate`: two views. The second is installed after the batch's append and before the HW passes it, so its instance starts at 0.
  - `TransientSource`: a transient partition with a transient view.
- **Timings (10):**
  - `Single`.
  - `Pipelined8`: 8 batches back to back before any ack.
  - `DupBeforeFold`: the idempotent retry arrives while the original is parked.
  - `DupAfterFold`: the retry arrives after the original was acked.
  - `TxnCommit` / `TxnAbort`: a transactional batch of another producer, opened before ours, is committed or aborted later.
  - `LeaderLoss`: `becomeFollower` while parked.
  - `Deadline`: the HW is held and `ackTimeoutNs` passes.
  - `Strict`: `durabilityTier = 2`.
  - `Routed`: a gateway-routed `WorkerOp::Produce` instead of shard ingress.
- **Not applicable (top-down):**
  1. `TransientSource × Strict`: a transient ring has no fd, and Strict acks as Majority there (`Worker.h:8394-8399`), so the cell equals `Single`. 2 cells.
- **Cell count:** 78 applicable, 2 skipped. Pinned by `CheckedWritesViewAckMatrixCountRegression.CellCountsArePinned`.
- **`expect03(cell)`, top-down:**
  1. `Deadline` → exactly one answer, `Internal`, at the first step with `now ≥ park + ackTimeoutNs`, never earlier.
  2. `LeaderLoss` → `NotLeader` at the first pass after `becomeFollower`, before the deadline.
  3. `TxnCommit` / `TxnAbort` → `OK`. Never before the marker commits. A plain `All` batch produced after ours in the same cell is answered **before** ours (SD16).
  4. Every other cell → `OK`.
- **Every cell asserts:**
  - one answer per correlation id, with the expected code;
  - at the step the answer first appears:
    - `HW > lastOffset`;
    - for `AV`, `|ISR| ≥ minIsr`;
    - for every installed view of the source, `instanceServeState(view, 0).foldCursor > lastOffset` with `leadership == replica->leadershipGeneration()` (for `NoViews`, the same pass an `All` ack of the same batch would fire);
  - the frame is the v12 54-byte form with `assignedBase == 0` and no trailer (the rig's session announces client version 12).

### B. The requirement table (`CheckedWritesRequirementTable`, Task 6; pure, exhaustive)

- **Inputs:**
  - `BatchIntents`, one of 7 shapes: plain; plain + tombstone; explicit `insert`; `update`; `removeExisting`; key-0 creates only; key-0 creates mixed with a plain record.
  - Reserved bits on or off.
  - `acks` 0–3.
  - `idempotent`, `transactional`, `atomicBatch`, `legacyMode` and `levelCheckedWrites`, each 0/1.
  - `TopicHookFacts` `{keyIndex, autoIncrement, constrainedSource, verdictSource}`, each 0/1.
  - `records` ∈ {1, 4096, 4097}.
- **Size:** 7 × 2 × 4 × 2⁵ × 2⁴ × 3 = **86,016 cells**, checked two ways:
  - **(a)** 64 hand-pinned rows. Each names the §11.2 row and the C8 rule it proves (the table in Task 6, Step 1).
  - **(b)** Five sweep invariants over every cell (Task 6, Step 1).

### C. The wire matrices (Tasks 3, 4)

- `acks` byte 0–255 through `Produce::decode`: exactly {0, 1, 2, 3} decode (256 cells).
- `ProduceResponse::encodeFor` × client version {0, 1, 2, 5, 11, 12} × `{assignedBase 0 / 42}` × `{hasVerdicts 0 / 1}` × `verdictCount` {0, 2}: 48 cells.
- Client `Acks` × producer kind:
  - `Acks` ∈ {None, Leader, All, View, Leader|View, All|View, Leader|All, Leader|All|View, 0x08, 0x80};
  - producer kind ∈ {plain, transactional, shard};
  - 30 cells, each pinned to "throws `std::invalid_argument`" or to a wire byte.

### D. The real-broker slice (Task 10, `rune_client_broker_tests`, default shard runtime unless named)

- `All | View` and `Leader | View` behind an open transaction, then a commit.
- `View` on a partition with no views.
- Legacy runtime refusal.
- A leader crash mid-stream on `ThreeNodes`.

### Stress (Task 9)

`CheckedWritesViewAckStressRegression.SeededWaitersMatchTheModel` runs seeds 1–20 × 400 steps on the Task 9 rig. Each step picks one action:

- produce with acks ∈ {0, 1, 2, 3}, idempotent or not;
- a follower fetch, which moves the HW;
- open or commit a transaction, which holds or releases the LSO;
- install a second view;
- `becomeFollower`, then `becomeLeader` back;
- advance the clock by `ackTimeoutNs / 4`.

The reference model computes each correlation id's expected answer and its earliest legal step. The test asserts exactly one answer per id, never before its earliest step, with the model's code, and records the seed on failure.

The overview assigns no stress invariant to 03 (04 owns the shared driver). This one guards the shared waiter state that 03 adds.

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `protocol/include/rune/protocol/client/Protocol.h` | M | Codes 51–58, versions 12 / 7, `clientUnderstandsCheckedWrites`, the `produceErrorForClient` mapping (P3-5), version comments |
| `protocol/include/rune/protocol/client/DataMessages.h` | M | `Produce` acks bits (C4 wire), `ProduceResponse` v12 + trailer (C5), `kMaxJudgedBatchRecords` |
| `protocol/include/rune/protocol/RecordBatch.h` | M | `rune::protocol::record` intent constants, builder `attributes` parameter (C3) |
| `protocol/include/rune/protocol/BatchRebuild.h` | M | `RecordSpan::attributes / index`, filled by `forEachRecordSpan` (C3); `key` and the bounded walk are Part 02's (R02a) |
| `protocol/conformance/{gen_vectors.cpp,manifest.md,vectors/produce_response_v12.bin,vectors/produce_response_v12_verdicts.bin,vectors/produce_acks_all_view.bin}` | M/C | v12 golden frames |
| `protocol/tests/protocol/{RecordBatchTest.cpp,BatchRebuildTest.cpp,client/DataMessagesTest.cpp,client/ConformanceTest.cpp}` | M | Protocol tests |
| `broker/libs/storage/include/rune/storage/WriteHookPolicy.h` | C | `HookStaged`, `NoWriteHook` (C8) |
| `broker/libs/storage/include/rune/storage/ProducePathImpl.h` | C | The templated `handleProduceBatch<LogT, HookT>` body |
| `broker/libs/storage/{include/rune/storage/ProducePath.h,src/ProducePath.cpp}` | M | Forwarder, explicit instantiations, `ProduceOutcome::assignedBase` |
| `broker/libs/node/include/rune/node/CheckedWriteHook.h`, `broker/libs/node/src/CheckedWriteHook.cpp` | C | `WriteFacts`, `TopicHookFacts`, `BatchIntents`, `HookRefusal`, `evaluateRequirements`, `CheckedWriteHook`, explicit instantiations |
| `broker/libs/node/CMakeLists.txt` | M | `src/CheckedWriteHook.cpp` in `rune_node`; new test files and binaries |
| `broker/libs/node/include/rune/node/Worker.h` | M | `ProduceFields::acks`, hook wiring in `produceCore_`, P3-1 frame checks, `PendingAcksView` + `flushOwedAcksView_`, purge sites, counters |
| `broker/libs/node/include/rune/node/WorkerViewSet.h` | M | `minFoldCursor` (C4 + addition 6) |
| `broker/libs/node/include/rune/node/WorkerOp.h` | M | `WorkerReturn` in-process members (C5); `WorkerOp::acks` comment |
| `broker/libs/node/include/rune/node/SessionAckSink.h` | M | `AckEntry` v12 fields, `encode_`, scratch, per-entry size (C5), `kNoVerdictRun` |
| `broker/libs/gateway/src/ClientGateway.cpp` | M | `acksAll()` decode (SD12), legacy and inline `View` refusal (SD10), v12 scratch bound |
| `broker/libs/node/tests/support/CheckedWritesAckRig.h` | C | The worker-level rig (Task 7 creates it, Task 9 extends it; Part 04 may fold it into `CheckedWritesRig.h`) |
| `broker/libs/node/tests/node/CheckedWritesHookTest.cpp`, `CheckedWritesMinFoldCursorTest.cpp` | C | In `rune_checked_writes_tests` (Part 02's binary) |
| `broker/libs/node/tests/node/CheckedWritesProducePathHookTest.cpp` | C | In `rune_produce_path_tests` (storage-level hook placement) |
| `broker/libs/node/tests/node/CheckedWritesWorkerHookTest.cpp` (Task 7), `CheckedWritesViewAckTest.cpp`, `CheckedWritesViewAckMatrixTest.cpp` (Task 9) | C | New `rune_checked_writes_ack_tests` (Aeron; Task 7 creates it) |
| `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` | C | New `rune_checked_writes_alloc_regression_tests` (+ `tests/support/AllocWindow.cpp`) |
| `broker/libs/node/tests/node/SessionAckSinkTest.cpp`, `tests/integration/ProduceValidationTest.cpp` | M | Sink v12 tests; validation-after-dedupe |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` | M | The real-broker slice (Part 02 created it) |
| `broker/benchmark/ProduceValidateBench.cpp` | M | `BM_CheckedWriteHook/hook_check/*`, `BM_CheckedWriteHook/view_ack` |
| `client-cpp/include/rune/client/producer/{Producer.h,ProducerImpl.h,ProducerHub.h}`, `client-cpp/src/producer/{ProducerImpl.cpp,ProducerHub.cpp,BatchBuilder.cpp,Claim.cpp}`, `client-cpp/src/Client.cpp`, `client-cpp/src/shard/ShardConfig.cpp` (or wherever `shardProducerConfig` is defined) | M | `Acks` flags, validation, wire byte, SD11 gate, P3-2, P3-6 |
| `client-cpp/include/rune/client/Completion.h` | M | `OpStatus::BrokerTooOld`, `constraintIdOf`, `assignedKeyOf` |
| `client-cpp/include/rune/client/codec/DataCodec.h` | M | `ProduceResponseView` v12 fields |
| `client-cpp/src/cli/QlEngine.cpp` | M | `errorCodeName` 48–58 |
| `client-cpp/tests/producer/ProducerAcksFlagsTest.cpp` | C | In `rune_invoker_tests` |
| `client-cpp/tests/DataConformanceTest.cpp`, `client-cpp/tests/RuneCliOfflineTest.cpp` | M | Client conformance, CLI names |
| `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `client-cpp/CLAUDE.md` (§2.1 Producer) | M | Docs this part ships |

---

## Phase 1 — Protocol and client surface

### Task 1: Codes 51–58, client version 12 / broker version 7, the old-session mapping and CLI names

**Files:**
- Modify: `protocol/include/rune/protocol/client/Protocol.h:209` (append after `UnstableOffsetCommit = 50`), `:317-327` (client-version comment block and constants), `:341-360` (broker-version block), `:414` (add the helper after `clientUnderstandsFilters`), `:423-429` (`produceErrorForClient`)
- Modify: `client-cpp/src/cli/QlEngine.cpp:133-151` (`errorCodeName`)
- Modify: `docs/api/wire-protocol.md` ("Error codes" table: rows 51–58)
- Test: `protocol/tests/protocol/client/DataMessagesTest.cpp`, `client-cpp/tests/RuneCliOfflineTest.cpp`

**Interfaces:**
- Produces (exact):
  ```cpp
  // ErrorCode, appended after UnstableOffsetCommit = 50
  ConstraintViolation = 51, KeyExists = 52, KeyNotFound = 53, AutoIncrementExhausted = 54, TxnTooLarge = 55,
  ViewKeyExpressionInvalid = 56, ViewConstraintInvalid = 57, ViewUniqueInvalid = 58,
  inline constexpr int32_t kClientVersionCheckedWrites = 12;
  inline constexpr int32_t kClientVersion              = 12;
  inline constexpr int32_t kBrokerVersionCheckedWrites = 7;
  inline constexpr int32_t kBrokerVersion              = 7;
  [[nodiscard]] constexpr bool clientUnderstandsCheckedWrites(int32_t clientVersion) noexcept;   // >= 12
  ```
- `produceErrorForClient(ec, v)`: for `ec` in [51, 58] and `v < kClientVersionCheckedWrites` it returns `InvalidRequest` (P3-5). The 28 and 42 rules are unchanged.

- [ ] **Step 1: Write the failing tests.**
  - `DataMessagesTest.cpp`:
    - `CheckedWritesProtocol.ErrorCodesAreAppendedAfterUnstableOffsetCommit`: `static_assert` each of the eight values (51…58) and `static_cast<int>(ErrorCode::UnstableOffsetCommit) == 50`.
    - `CheckedWritesProtocol.VersionsAreTwelveAndSeven`:
      - `static_assert(kClientVersionCheckedWrites == kClientVersionFilter + 1 && kClientVersionCheckedWrites == 12 && kClientVersion == 12)`;
      - `static_assert(kBrokerVersionCheckedWrites == kBrokerVersionFilter + 1 && kBrokerVersion == 7)`;
      - `EXPECT_FALSE(clientUnderstandsCheckedWrites(11))`, `EXPECT_TRUE(clientUnderstandsCheckedWrites(12))`.
    - `CheckedWritesProtocolRegression.OldSessionsNeverSeeCheckedWriteCodesRegression`:
      - for `c` in 51..58 and `v` in {0, 2, 6, 11}: `produceErrorForClient(ErrorCode(c), v) == ErrorCode::InvalidRequest`;
      - for `v = 12`: `== ErrorCode(c)`;
      - `produceErrorForClient(UnknownProducerId, 1) == OutOfOrderSequence` and `produceErrorForClient(TransientLogFull, 5) == NotLeader` (pins);
      - `produceErrorForClient(SchemaValidationFailed, 11) == SchemaValidationFailed`.
  - `RuneCliOfflineTest.cpp`, `CheckedWritesCliRegression.ErrorCodeNamesCoverEveryCodeThroughViewUniqueInvalidRegression`:
    - `errorCodeName(47) == "PasswordChangeRequired"`, `48 == "AuthUnavailable"`, `49 == "RecordEgressUnavailable"`, `50 == "UnstableOffsetCommit"`;
    - `51 == "ConstraintViolation"`, `52 == "KeyExists"`, `53 == "KeyNotFound"`, `54 == "AutoIncrementExhausted"`, `55 == "TxnTooLarge"`, `56 == "ViewKeyExpressionInvalid"`, `57 == "ViewConstraintInvalid"`, `58 == "ViewUniqueInvalid"`;
    - `59 == "UnknownErrorCode"`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: compile error `no member named 'ConstraintViolation' in 'rune::protocol::client::ErrorCode'`.
- [ ] **Step 3: Implement.**
  - Give each new enumerator a one-line comment from the §11.1 table.
  - Add a "12 — checked writes" paragraph to the client-version comment:
    - the `acks` bit 1 (`View`);
    - the `ProduceResponse` v12 block (`assignedBase`) and the optional verdict trailer;
    - codes 51–58, which only sessions ≥ 12 see;
    - `GetViewRowK` / `ScanViewRows` / `GetStats` (07 and 08) gated on the same constants.
  - Add a "7 — checked writes" line to the broker-version comment.
  - `errorCodeName`: append the 11 names (48–58) and move the `static_assert` to `ErrorCode::ViewUniqueInvalid`.
  - `docs/api/wire-protocol.md`: add eight table rows. "Client handling":
    - 51–53: "per-record verdict of a `View` ack (Part 04)";
    - 54: "not retried";
    - 55: "returned by `commitTransaction()`";
    - 56–58: "DDL compile error".
- [ ] **Step 4: Run.**
  - `cmake --build build/root -j14 --target rune_protocol_tests && ctest --test-dir build/root -R 'CheckedWritesProtocol|UnknownProducerId|DataMessages|Conformance' --output-on-failure`
  - `cmake --build build/sub-client-cpp --target rune_cli_offline_tests && ctest --test-dir build/sub-client-cpp -R 'CheckedWritesCli|RuneCliOffline' --output-on-failure`
  - Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add protocol/include/rune/protocol/client/Protocol.h protocol/tests/protocol/client/DataMessagesTest.cpp \
        client-cpp/src/cli/QlEngine.cpp client-cpp/tests/RuneCliOfflineTest.cpp docs/api/wire-protocol.md
git commit -m "feat(protocol): checked writes - codes 51-58, client v12 / broker v7, old sessions never see 51-58, CLI names 48-58" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Record intent bits and the extended `RecordSpan` (C3; `key` is Part 02's, R02a)

**Files:**
- Modify: `protocol/include/rune/protocol/RecordBatch.h`:
  - a new `namespace rune::protocol::record` after `namespace rune::protocol::batch`, which ends at line 37;
  - `RecordBatchBuilder::append` (`:130`); it pushes the literal 0 at `:150` today.
- Modify: `protocol/include/rune/protocol/BatchRebuild.h:17-23` (`RecordSpan`) and `:51-73` (`forEachRecordSpan`)
- Test: `protocol/tests/protocol/RecordBatchTest.cpp`, `protocol/tests/protocol/BatchRebuildTest.cpp`

**Interfaces:**
- Produces (exact, C3):
  ```cpp
  namespace rune::protocol::record {
  inline constexpr uint8_t kAttrIntentMask   = 0x03;
  inline constexpr uint8_t kAttrReservedMask = 0xFC;
  enum class Intent : uint8_t { Upsert = 0, Insert = 1, Existing = 2 };   // 3 reserved, refused
  [[nodiscard]] constexpr uint8_t intentOf(uint8_t attributes) noexcept { return attributes & kAttrIntentMask; }
  }
  void RecordBatchBuilder::append(std::string_view key, std::string_view value, int64_t timestamp, uint8_t attributes = 0);
  struct RecordSpan { /* existing members, plus Part 02's std::string_view key (02 P9) */ uint8_t attributes = 0; uint32_t index = 0; };
  ```
- `forEachRecordSpan` (bounded since Part 02) fills the new members:
  - `attributes`: the byte it skips at `BatchRebuild.h:58`;
  - `index`: the loop's `i`.
- **Deviation note (R02a).** C3 lists `key` under this part. Part 02 already appended `key` and the bounded walk (its P9), so this task consumes them and adds only `attributes` and `index`. Record it as a deviation in the task report.
- `RecordBatchReader::forEachRecord` is unchanged. C3 says the hook and the folds use only `forEachRecordSpan`.

- [ ] **Step 1: Write the failing tests.**
  - `RecordBatchTest.cpp`:
    - `CheckedWritesRecordAttributes.IntentOfMasksBitsZeroAndOne`: `intentOf(0) == 0`, `intentOf(1) == 1`, `intentOf(2) == 2`, `intentOf(3) == 3`, `intentOf(0xFD) == 1`; `static_assert((kAttrIntentMask | kAttrReservedMask) == 0xFF && (kAttrIntentMask & kAttrReservedMask) == 0)`.
    - `CheckedWritesRecordAttributesRegression.DefaultAppendIsByteIdenticalRegression`: two builders, `append("k","v",1000)` against `append("k","v",1000,0)`. `build()` outputs are byte-equal and `ensureValid()` holds.
  - `BatchRebuildTest.cpp`:
    - `CheckedWritesRecordSpan.CarriesAttributesKeyAndIndex`:
      - build `append("a","1",0,0); append("bb","",0,1); append("ccc","3",0,2)`;
      - `forEachRecordSpan` yields `attributes` {0, 1, 2} and `index` {0, 1, 2}, beside Part 02's `key` {"a", "bb", "ccc"};
      - the second record has `value.empty()` and `nullValue == false`;
      - each `key.data()` lies inside `[batch, batch + len)`;
      - `RecordBatchReader::forEachRecord` still yields the same keys and values;
      - `ensureValid()` holds (the CRC covers the attributes byte).
    - `CheckedWritesRecordSpanRegression.RebuildKeepsAttributesVerbatimRegression`: `rebuildBatch` keeping {0, 2} of the batch above yields spans with attributes {0, 2} and a valid CRC.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `no member named 'record' in namespace 'rune::protocol'`.
- [ ] **Step 3: Implement** as specified: `records_.push_back(attributes)` at `:150`, and in `forEachRecordSpan` read the attributes byte before `++pos` and set `index`. Part 02's `key` assignment and bounds checks stay as 02 left them.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_protocol_tests rune_worker_filter_tests rune_filter_engine_tests && ctest --test-dir build/root -R 'CheckedWritesRecord|BatchRebuild|RecordBatch|Filter' -j14 --output-on-failure`. Expected: PASS. The Phase 2c filter suites use `RecordSpan` and must stay green.
- [ ] **Step 5: Commit.**
```bash
git add protocol/include/rune/protocol/RecordBatch.h protocol/include/rune/protocol/BatchRebuild.h \
        protocol/tests/protocol/RecordBatchTest.cpp protocol/tests/protocol/BatchRebuildTest.cpp
git commit -m "feat(protocol): record intent bits (attributes 0-1), builder attributes, RecordSpan attributes/index" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The `acks` bits and `ProduceResponse` v12 (C4 wire, C5 layout), golden vectors, client decode

**Files:**
- Modify: `protocol/include/rune/protocol/client/DataMessages.h`:
  - the `Produce` layout comment (`:21`) and `acks` member comment (`:49`), and the decode check (`:141`);
  - `ProduceResponse` (`:160-290`).
- Modify: `protocol/conformance/gen_vectors.cpp` (after block 13b, `:280-296`), `protocol/conformance/manifest.md` (§13 plus the table at `:634`). Create `protocol/conformance/vectors/{produce_response_v12.bin,produce_response_v12_verdicts.bin,produce_acks_all_view.bin}`.
- Modify: `client-cpp/include/rune/client/codec/DataCodec.h:108-152` (`ProduceResponseView`, `decodeProduceResponse`)
- Test: `protocol/tests/protocol/client/DataMessagesTest.cpp`, `protocol/tests/protocol/client/ConformanceTest.cpp`, `client-cpp/tests/DataConformanceTest.cpp`

**Interfaces:**
- Produces (exact):
  ```cpp
  // DataMessages.h, struct Produce scope (namespace rune::protocol::client)
  inline constexpr uint8_t kAcksWireAll = 0x01, kAcksWireView = 0x02, kAcksWireKnown = 0x03;
  [[nodiscard]] constexpr bool acksAll(uint8_t acks) noexcept  { return (acks & kAcksWireAll) != 0; }
  [[nodiscard]] constexpr bool acksView(uint8_t acks) noexcept { return (acks & kAcksWireView) != 0; }
  inline constexpr int32_t kMaxJudgedBatchRecords = 4096;            // contract addition 1
  struct ProduceResponse {
      /* existing members */
      uint64_t assignedBase = 0;                                    // C5: 0 = none
      bool     hasVerdicts  = false;
      uint16_t verdictCount = 0;
      std::span<const uint8_t> verdictBytes;                        // verdictCount x 8 encoded entries
      static constexpr uint16_t    BODY_FIXED_V12     = 30;
      static constexpr std::size_t ENCODED_SIZE_V12   = 54;         // contract addition 2
      static constexpr uint16_t    kMaxVerdictEntries = 4096;
      static constexpr std::size_t kMaxEncodedSize    = 24 + 30 + 2 + 8 * 4096;   // 32,824
      [[nodiscard]] static constexpr std::size_t encodedSizeFor(int32_t clientVersion, bool hasVerdicts,
                                                                uint16_t verdictCount) noexcept;
      [[nodiscard]] std::size_t encodedSize(int32_t clientVersion) const noexcept;
      template <class Fn> void forEachVerdict(Fn&& fn) const noexcept;   // fn(uint16_t recordIndex, uint16_t code, uint32_t constraintId)
  };
  ```
- **Layout (v12, `encodeFor(buf, v ≥ 12)`):**
  - `blockLength = 30`. Body offsets: 0 `errorCode`, 2 `partition`, 6 `baseOffset`, 14 `logStartOffset`, 22 `assignedBase u64`.
  - If `hasVerdicts`: `[count u16 = verdictCount]`, then `verdictBytes`, copied verbatim, `8 × count` bytes, each entry `{recordIndex u16, code u16, constraintId u32}` LE.
  - Below 12, `encodeFor` is exactly today's code: 46 B from version 2 on, 38 B below. It never writes `assignedBase` or a trailer.
  - `encode()` (no version) still writes the 46 B frame, so `produce_response.bin` stays the golden.
  - The one-argument `encodedSizeFor(v)` returns 54 for `v ≥ 12`.
- **Decode:**
  - `assignedBase` is read iff `blockLength ≥ 30`.
  - A trailer is read iff `avail > 24 + blockLength`. It needs ≥ 2 bytes, `count ≤ kMaxVerdictEntries`, and exactly `2 + 8 × count` bytes after the block; anything else is `nullopt`.
  - `verdictBytes` is a zero-copy span.
- `Produce::decode`: replace `if (msg.acks > 1)` with `if ((msg.acks & ~kAcksWireKnown) != 0) return std::nullopt;`.
- Client `ProduceResponseView` appends `uint64_t assignedBase = 0; bool hasVerdicts = false; uint16_t verdictCount = 0; std::span<const uint8_t> verdictBytes;`, copied by `decodeProduceResponse`. Routing them is Parts 04 and 06; `ClientAgent::ProduceAck` is unchanged in 03.

- [ ] **Step 1: Write the failing tests** (`DataMessagesTest.cpp` unless noted).
  - `CheckedWritesWire.AcksByteHelpers`: `acksAll(1)`, `acksAll(3)`, `!acksAll(0)`, `!acksAll(2)`, `acksView(2)`, `acksView(3)`, `!acksView(1)`.
  - `CheckedWritesWireRegression.ProduceDecodeAcceptsOnlyKnownAcksBitsRegression`: for each `b` in 0..255, encode a `Produce` with `acks = b`. `decode` has a value iff `(b & ~0x03) == 0`, and then `decoded->acks == b`.
  - `CheckedWritesWire.ProduceResponseV12RoundTrip`:
    - `{sessionId 7, correlationId 9, errorCode 0, partition 7, baseOffset 123456, logStartOffset -1, assignedBase 42}`;
    - `encodeFor(buf, 12) == 54 == encodedSizeFor(12, false, 0) == resp.encodedSize(12)`;
    - LE16 at 0 is 30; LE64 at 46 is 42;
    - `decode` → `assignedBase == 42`, `!hasVerdicts`, `verdictCount == 0`.
  - `CheckedWritesWireRegression.EncodeForBelowTwelveIsByteIdenticalRegression`:
    - for `v` in {0, 1, 2, 5, 11}, with `assignedBase = 42`, `hasVerdicts = true`, `verdictCount = 2` (bytes set);
    - `encodeFor(buf, v)` equals the golden `produce_response.bin` for `v ≥ 2` (46 B) and `produce_response_v0.bin` for `v < 2` (38 B), message fields as in `ConformanceTest.cpp:575-579`.
  - `CheckedWritesWire.VerdictTrailerRoundTrip`:
    - entries `{(0, 51, 0x0007'0000), (3, 52, 0)}`, `verdictCount 2` → `encodeFor(buf, 12) == 72 == encodedSizeFor(12, true, 2)`;
    - `decode` → `hasVerdicts`, `verdictCount == 2`, and `forEachVerdict` visits exactly those two tuples in order;
    - with `verdictCount 0` and `hasVerdicts`: 56 B, decodes to `hasVerdicts && verdictCount == 0`.
  - `CheckedWritesWireRegression.TruncatedOrOversizedTrailerIsUndecodableRegression`: each of these decodes to `nullopt`:
    - a 72 B frame cut to 71;
    - `count = 2` with 8 entry bytes;
    - `count = 4097` with `8 × 4097` bytes;
    - one stray byte after a 54 B block.
  - `CheckedWritesWire.MaxEncodedSizeIs32824`: `static_assert(ProduceResponse::kMaxEncodedSize == 32824)`, `encodedSizeFor(12, true, 4096) == 32824`, `static_assert(kMaxJudgedBatchRecords == ProduceResponse::kMaxVerdictEntries)`.
  - `ConformanceTest.cpp`:
    - `ConformanceGolden.ProduceResponseV12`: `produce_response_v12.bin` is 54 B with `blockLength 30` and the fields of vector 13 plus `assignedBase = 1001`; re-encoding with `encodeFor(…, 12)` is byte-equal.
    - `ConformanceGolden.ProduceResponseV12Verdicts`: `produce_response_v12_verdicts.bin` is 64 B. It carries the same fields with `assignedBase 0`, `hasVerdicts`, and one entry `{recordIndex 1, code 51, constraintId 0x0005'0000}`.
    - `ConformanceGolden.ProduceAcksAllView`: `produce_acks_all_view.bin` is `produce_explicit_partition.bin` with `acks = 3`. It decodes with `acks == 3` and differs from that vector at exactly byte 33.
    - Add the three files to the manifest table check at `ConformanceTest.cpp:874`.
  - `client-cpp/tests/DataConformanceTest.cpp`, `CheckedWritesClientConformance.DecodesV12ProduceResponse`: `codec::decodeProduceResponse` over the two v12 vectors yields `assignedBase` 1001 and 0, and `verdictCount` 0 and 1.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `no member named 'assignedBase' in 'ProduceResponse'`.
- [ ] **Step 3: Implement** the layout above.
  - Generate the vectors: add blocks 13c, 13d and 13e to `gen_vectors.cpp`, then run `c++ -std=c++23 -I protocol/include protocol/conformance/gen_vectors.cpp -o "$TMPDIR/rune_gen_vectors" && "$TMPDIR/rune_gen_vectors" protocol/conformance/vectors`.
  - Then `git status --porcelain protocol/conformance/vectors` must list **only** the three new files. Any existing vector that changed is a wire break: stop.
  - Document the three frames in `manifest.md` (§13c–e, with byte layouts).
- [ ] **Step 4: Run.**
  - `cmake --build build/root -j14 --target rune_protocol_tests && ctest --test-dir build/root -R 'CheckedWritesWire|Conformance|DataMessages|UnknownProducerId' --output-on-failure`
  - `cmake --build build/sub-client-cpp --target rune_client_tests && ctest --test-dir build/sub-client-cpp -R 'Conformance|CheckedWritesClientConformance' --output-on-failure`
  - Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add protocol/include/rune/protocol/client/DataMessages.h protocol/conformance/gen_vectors.cpp protocol/conformance/manifest.md \
        protocol/conformance/vectors/produce_response_v12.bin protocol/conformance/vectors/produce_response_v12_verdicts.bin \
        protocol/conformance/vectors/produce_acks_all_view.bin protocol/tests/protocol/client/DataMessagesTest.cpp \
        protocol/tests/protocol/client/ConformanceTest.cpp client-cpp/include/rune/client/codec/DataCodec.h \
        client-cpp/tests/DataConformanceTest.cpp
git commit -m "feat(protocol): acks bit 1 (View), ProduceResponse v12 (assignedBase + verdict trailer), kMaxJudgedBatchRecords, golden vectors" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Client `Acks` flags, creation checks, the wire byte, the SD11 gate, the 4096 cap, result codes

**Files:**
- Modify: `client-cpp/include/rune/client/producer/Producer.h`: `:79-83` (`Acks`), `:137` (config doc), `:302-324` (`SendResult`)
- Modify: `client-cpp/include/rune/client/Completion.h`: `:54-69` (`OpStatus`) and after `:109` (accessors)
- Modify: `client-cpp/src/producer/ProducerImpl.cpp`:
  - the constructor clamp block (`:320-340`);
  - Tier-1 seal (`:1706-1740`, wire byte at `:1726`);
  - Tier-2 encode (`:1873-1905`, wire byte at `:1899`);
  - `claimBatch` (in `Claim.cpp` or `ProducerImpl.cpp`, wherever `n` is admitted).
- Modify: `client-cpp/include/rune/client/producer/ProducerImpl.h` (`brokerVersion_`, `setBrokerVersion`), `client-cpp/include/rune/client/producer/ProducerHub.h` + `client-cpp/src/producer/ProducerHub.cpp` (`setBrokerVersion`, applied at register)
- Modify: `client-cpp/src/producer/BatchBuilder.cpp` (`add`, near the existing `length_error`s at `:102`, `:119`)
- Modify: `client-cpp/src/Client.cpp`:
  - `:2228` (next to `groupConsumerHub_.setBrokerVersion`: `producerHub_.setBrokerVersion(brokerVersion)`);
  - `:4401-4410` (`producerWithRingCap_`: `acksValid` check before the transactional checks).
- Modify: the definition of `shardProducerConfig` (`client-cpp/include/rune/client/shard/ShardConfig.h:167`): throw for `!acksValid(g.producer.acks)`.
- Modify: `docs/api/client-cpp.md:166-205` (Producer, `Acks` bullet), `client-cpp/CLAUDE.md` §2.1 (the `Acks` bullet)
- Test: `client-cpp/tests/producer/ProducerAcksFlagsTest.cpp` (create; add to `rune_invoker_tests` in `client-cpp/CMakeLists.txt:1519-1540`)

**Interfaces:**
- Produces (exact):
  ```cpp
  enum class Acks : uint8_t { None = 0, Leader = 1, All = 2, View = 4 };
  [[nodiscard]] constexpr Acks operator|(Acks a, Acks b) noexcept;
  [[nodiscard]] constexpr Acks operator&(Acks a, Acks b) noexcept;
  [[nodiscard]] constexpr bool has(Acks set, Acks flag) noexcept;      // (set & flag) == flag && flag != None
  [[nodiscard]] constexpr Acks normalized(Acks a) noexcept;            // Leader|All -> All, keeps View
  [[nodiscard]] constexpr bool acksValid(Acks a) noexcept;             // no bit outside 0x07; View needs Leader or All
  // SendResult: KeyTooLong = 12 (Part 02), WriteNotAllowed = 13 (this part; first used by 05/06)
  // OpStatus:   BrokerTooOld = 12
  [[nodiscard]] constexpr uint32_t constraintIdOf(const Completion& c) noexcept { return c.reserved0; }
  [[nodiscard]] constexpr uint64_t assignedKeyOf(const Completion& c) noexcept { return static_cast<uint64_t>(c.aux); }
  void ProducerImpl::setBrokerVersion(int32_t v) noexcept;
  void ProducerHub::setBrokerVersion(int32_t v) noexcept;
  ```
- **The wire byte** (both seal sites):
  - `(has(acks, All) ? 1 : 0) | (has(acks, View) && !transactional ? 2 : 0)`;
  - so `Leader` → 0, `All` → 1, `Leader|View` → 2, `All|View` → 3 (C4), and a transactional producer never sends bit 1 (P3-2).
- **Idempotence** stays `cfg_.acks != Acks::None`. Every `cfg_.acks == Acks::All` comparison becomes `has(cfg_.acks, Acks::All)`.
- **The constructor** sets `cfg_.acks = normalized(cfg_.acks)`. If `has(View)`, it clamps `cfg_.batchRecords` to `cfg_.batchRecords > 0 ? std::min(cfg_.batchRecords, 4096) : 4096` (P3-6).
- **The SD11 gate** (P3-4) runs at both seal sites when `has(cfg_.acks, View) && !transactional && brokerVersion_ >= 1 && brokerVersion_ < kBrokerVersionCheckedWrites`. Nothing is offered. Tier-1: `failBuilder_(ps, OpStatus::BrokerTooOld)`. Tier-2: each queued batch completes `OpStatus::BrokerTooOld`, through the same path its `DeliveryFailed` takes at `:2910-2911`. `brokerVersion_ == 0` sends.

- [ ] **Step 1: Write the failing tests** (`ProducerAcksFlagsTest.cpp`; rigs as in `ProducerAckPathTest.cpp:58-90`, `support/AckLedger.h`, `support/FrameCapture.h`).
  - `CheckedWritesAcks.FlagOperatorsAndHas`:
    - `(Acks::All | Acks::View) == Acks{6}`, `has(Acks::All | Acks::View, Acks::View)`, `!has(Acks::Leader, Acks::All)`, `((Acks::Leader | Acks::View) & Acks::View) == Acks::View`, `!has(Acks::All, Acks::None)`;
    - `normalized(Acks::Leader | Acks::All) == Acks::All`, `normalized(Acks::Leader | Acks::All | Acks::View) == (Acks::All | Acks::View)`, `normalized(Acks::Leader | Acks::View) == (Acks::Leader | Acks::View)`.
  - `CheckedWritesAcksMatrix.CreationAndWireByte` (`TEST_P` over matrix C's 30 cells). Each `Acks` value is created as a plain producer through `Client::producer`, the Client built offline as `ProducerTxnApiTest.cpp:45-53` does; as a transactional producer; and through `shard::shardProducerConfig`.
    - **Expected throws** `std::invalid_argument`: `View`, `0x08`, `0x80` in every kind; `None` for transactional (existing).
    - **Otherwise:** a `ProducerImpl` with the same config, one send pumped through `FrameCapture`, has frame `acks` == {None: n/a (no ack), Leader: 0, All: 1, Leader|View: 2, All|View: 3, Leader|All: 1, Leader|All|View: 3}.
    - **Transactional cells:** bit 1 is never set (2 → 0, 3 → 1).
  - `CheckedWritesAcksRegression.TransactionalProducerNeverSendsTheViewBitRegression`: a transactional `ProducerImpl` with `All | View` (identity stub as in `ProducerTxnDataTest.cpp`). Every captured frame of 10 sends has `acks == 1`.
  - `CheckedWritesAcksRegression.ViewProducerOnAnOldBrokerFailsLocallyBrokerTooOldRegression`:
    - `impl.setBrokerVersion(6)`; `All | View`; two acked sends; `flushAsOneBatch`;
    - `cap.frames.empty()`; both completions `OpStatus::BrokerTooOld`, each exactly once;
    - then `setBrokerVersion(7)` and two more sends: one frame with `acks == 3`;
    - with `setBrokerVersion(0)`: the frame is sent;
    - an `All`-only producer at version 6 sends normally.
  - `CheckedWritesAcks.ViewProducerFlushesAt4096Records`:
    - `All | View`, `lingerUs` large, 4100 void sends, pumped;
    - frames have 4096 and 4 records;
    - with `batchRecords = 100`: frames of ≤ 100;
    - an `All` producer with the same 4100 sends: one frame;
    - `claimBatch(topic, key, 4097, 8)` on the `View` producer returns `SendResult::MessageTooLarge`;
    - `BatchBuilder` add #4097 throws `std::length_error`.
  - `CheckedWritesResults.AppendOnlyValues`: `static_cast<int>(SendResult::WriteNotAllowed) == 13`, `static_cast<int>(OpStatus::BrokerTooOld) == 12`, `constraintIdOf(Completion{.reserved0 = 0x0007'0001}) == 0x0007'0001`, `assignedKeyOf(Completion{.aux = 42}) == 42`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp --target rune_invoker_tests`. Expected: `no member named 'View' in 'rune::client::Acks'`.
- [ ] **Step 3: Implement** as specified.
  - `has`, `normalized` and `acksValid` are one-liners on `std::to_underlying`.
  - Apply `ProducerHub::setBrokerVersion` to producers registered later by storing the value in the hub.
  - Docs: in `docs/api/client-cpp.md` the `Acks` bullet becomes a flag table (the §8.9 table), with "`View` alone is refused", "`Leader | View` waits for commit today", "a transactional producer's `View` is ignored on sends", and `BrokerTooOld`. Mirror that in one line in `client-cpp/CLAUDE.md` §2.1.
- [ ] **Step 4: Run.**
  - `cmake --build build/sub-client-cpp --target rune_invoker_tests && ctest --test-dir build/sub-client-cpp -R 'CheckedWritesAcks|CheckedWritesResults|ProducerAcks|ProducerInFlight|ProducerTxn|ProducerClaim|BatchBuilder|Shard' -j14 --output-on-failure`
  - Expected: PASS. The existing producer suites are the regression check for the `has()` rewrite.
- [ ] **Step 5: Commit.**
```bash
git add client-cpp/include/rune/client/producer/Producer.h client-cpp/include/rune/client/producer/ProducerImpl.h \
        client-cpp/include/rune/client/producer/ProducerHub.h client-cpp/src/producer/ProducerImpl.cpp \
        client-cpp/src/producer/ProducerHub.cpp client-cpp/src/producer/BatchBuilder.cpp client-cpp/src/producer/Claim.cpp \
        client-cpp/src/Client.cpp client-cpp/include/rune/client/shard/ShardConfig.h client-cpp/src/shard \
        client-cpp/include/rune/client/Completion.h client-cpp/tests/producer/ProducerAcksFlagsTest.cpp \
        client-cpp/CMakeLists.txt docs/api/client-cpp.md client-cpp/CLAUDE.md
git commit -m "feat(client): Acks flags (View), creation checks, wire byte, broker-version gate (BrokerTooOld), 4096-record View batches, WriteNotAllowed" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller).**
  - `broker/scripts/run-regression.sh build/root 10`: all three stages. Stage (c) runs on `$RUNE_VM_HOST` under its lock file.
  - `ctest --test-dir build/sub-client-cpp -R Regression -j14 --output-on-failure`
  - `cmake --build build/sub-codec --target rune_codec_tests rune_catalog_tests && ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden' --output-on-failure` (CatalogGolden unchanged)
  - `cmake --build build/asan -j14 && ctest --test-dir build/asan -R Regression -LE latency -j8 --output-on-failure`, and the same for `build/tsan`
  - `cmake --build build/client-tsan -j14 && ctest --test-dir build/client-tsan -R Regression -j8 --output-on-failure`
  - Before running: grep the broker and client tests for a `ProduceResponse::ENCODED_SIZE` pin on a session announced with `kClientVersion` (it is 12 now, so the frame is 54 B). Change each such pin to `encodedSizeFor(kClientVersion)`. Each change is a fix with a reason, not a weakened assertion.

---

## Phase 2 — The broker hook

### Task 5: `WriteHookPolicy.h`, `ProducePathImpl.h` and `handleProduceBatch<LogT, HookT>` (C8 storage half)

**Files:**
- Create: `broker/libs/storage/include/rune/storage/WriteHookPolicy.h`, `broker/libs/storage/include/rune/storage/ProducePathImpl.h`
- Modify:
  - `broker/libs/storage/include/rune/storage/ProducePath.h`:
    - `ProduceOutcome` (`:85-111`): append `uint64_t assignedBase = 0;`;
    - the templates and `extern template`s (`:162-182`).
  - `broker/libs/storage/src/ProducePath.cpp`: the body (`:29-350`) moves to `ProducePathImpl.h`; the instantiations (`:352-357`) become `<Log, NoWriteHook>` and `<InMemoryLog, NoWriteHook>`.
- Test: `broker/libs/node/tests/node/CheckedWritesProducePathHookTest.cpp` (create; add to `rune_produce_path_tests`, `broker/libs/node/CMakeLists.txt:358-366`)

**Interfaces:**
- Produces (exact, C8):
  ```cpp
  namespace rune::storage {
  struct HookStaged { rune::protocol::client::ErrorCode error{}; const uint8_t* batch = nullptr; std::size_t len = 0; };
  struct NoWriteHook { static constexpr bool kActive = false; };
  template <class LogT, class HookT>
  ProduceOutcome handleProduceBatch(const uint8_t* batch, std::size_t batchLen, int32_t topicId, int32_t partition,
                                    int64_t producerId, int16_t producerEpoch, int32_t baseSequence, LogT& log,
                                    ProducerStateManager& producerState, bool acksAll, int isrSize, int minIsr,
                                    uint8_t durabilityTier, bool crcPreverified, HookT& hook) noexcept;
  template <class LogT>   // unchanged signature: an inline forwarder with a local NoWriteHook
  ProduceOutcome handleProduceBatch(/* … the 13 existing params … */, bool crcPreverified = false) noexcept;
  extern template ProduceOutcome handleProduceBatch<Log, NoWriteHook>(/*…*/, NoWriteHook&) noexcept;
  extern template ProduceOutcome handleProduceBatch<InMemoryLog, NoWriteHook>(/*…*/, NoWriteHook&) noexcept;
  }
  ```
- **Placement in the body:**
  1. `if constexpr (HookT::kActive) { if (auto e = hook.check(batch, batchLen, recordCount); e != OK) { out.error = e; return out; } }` sits right after the dedupe block ends (`ProducePath.cpp:255`) and before the min-ISR gate (`:262`). It runs for `producerId < 0` too.
  2. `const uint8_t* appendData = batch; std::size_t appendLen = batchLen;`, then under `kActive` `HookStaged s = hook.stage(log, batch, batchLen)`. An error returns it. Otherwise `appendData = s.batch; appendLen = s.len;`. This sits right before the append (`:276`). The append uses `appendData` / `appendLen`.
  3. `if constexpr (HookT::kActive) { hook.onAppended(base, lastOffset); out.assignedBase = hook.assignedBase(); }` sits after the PSM update (`:315`) and before the Strict sync.
- A duplicate returns before step 1, so it never calls the hook (C8).
- Today's callers are unchanged: `Worker.h:8310-8321`, `ClientGateway.cpp` inline path, and the `rune::node` shim.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesProducePathHookTest.cpp`, the fixture from `ProducePathTest.cpp`: a real `Log` + `ProducerStateManager`).
  - A test policy `struct RecordingHook { static constexpr bool kActive = true; … }` records the call order and arguments in a fixed `std::array<char, 8>` trace. Its `check` returns a configurable code, and its `stage` either passes or substitutes a prepared buffer.
  - `CheckedWritesProducePathHook.CallsCheckStageOnAppendedInOrder`: an accepted idempotent batch (pid 11, seq 0) gives trace `"CSA"`, `onAppended(base = 0, last = 0)`, `ProduceOutcome::assignedBase == hook.assignedBase()` (set to 77), and log LEO 1.
  - `CheckedWritesProducePathHookRegression.DuplicateNeverCallsTheHookRegression`: send the same batch twice. The second gives `isDuplicate`, an empty trace and LEO 1.
  - `CheckedWritesProducePathHookRegression.OutOfOrderSequenceIsAnsweredBeforeTheHookRegression`: seq 5 for a new producer gives `OutOfOrderSequence` with an empty trace (SD2: dedupe first).
  - `CheckedWritesProducePathHook.CheckRefusalAppendsNothingAndLeavesThePsm`:
    - `check` returns `InvalidRequest` → `out.error == InvalidRequest`, LEO 0, trace `"C"`;
    - the same batch then with a passing `check` is accepted at offset 0 (the PSM was not advanced).
  - `CheckedWritesProducePathHook.CheckRunsBeforeTheMinIsrGate`: `acksAll`, `isrSize 1`, `minIsr 2`, `check` refusing → `InvalidRequest` (not `MinIsrViolation`). With `check` passing → `MinIsrViolation` and trace `"C"` (no stage).
  - `CheckedWritesProducePathHook.StagedBytesAreWhatIsAppended`: `stage` substitutes a second valid batch of the same `lastOffsetDelta` (different value bytes). `log.read(0)` returns the substituted bytes, and the CRC is valid.
  - `CheckedWritesProducePathHook.NonIdempotentBatchStillRunsCheck`: pid −1 gives trace `"CSA"`.
  - `CheckedWritesProducePathHookRegression.ForwarderIsByteIdenticalRegression`: the existing `ProducePathTest` suite unchanged plus one run of each `ProducePathTest` scenario through the forwarder (the existing tests are the pin).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_produce_path_tests`. Expected: `no template named 'NoWriteHook'`.
- [ ] **Step 3: Implement.** Move the body verbatim into `ProducePathImpl.h` (`#pragma once`, includes `ProducePath.h`, `WriteHookPolicy.h`, `RecordBatch.h`, `spdlog`), then add the three blocks above. `ProducePath.cpp` includes `ProducePathImpl.h` and holds the two explicit instantiations.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_produce_path_tests rune_node && ctest --test-dir build/root -R 'ProducePath|CheckedWritesProducePathHook' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/storage/include/rune/storage/WriteHookPolicy.h broker/libs/storage/include/rune/storage/ProducePathImpl.h \
        broker/libs/storage/include/rune/storage/ProducePath.h broker/libs/storage/src/ProducePath.cpp \
        broker/libs/node/tests/node/CheckedWritesProducePathHookTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(storage): handleProduceBatch<LogT, HookT> - check after dedupe, stage before append, onAppended after PSM; NoWriteHook forwarder" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: `CheckedWriteHook`: facts, the intent scan, validation after dedupe and the requirement table (C8 node half)

**Files:**
- Create: `broker/libs/node/include/rune/node/CheckedWriteHook.h`, `broker/libs/node/src/CheckedWriteHook.cpp` (add to the `rune_node` sources in `broker/libs/node/CMakeLists.txt`)
- Test: `broker/libs/node/tests/node/CheckedWritesHookTest.cpp` (create; add to `rune_checked_writes_tests`, the binary Part 02 created)

**Interfaces:**
- Consumes:
  - `PartitionWriteState` and `TopicWriteSpec` (C7), and Part 02's `TypedCheck checkTypedRecord(const TopicWriteSpec&, const RecordSpan&, int16_t level)` in `broker/libs/node/include/rune/node/WriteState.h` (R-X1; it holds the version lookup, the validator call and the `keyValueOffset` + `memcmp` wire-key rule);
  - `rune::protocol::forEachRecordSpan` and `RecordSpan::key` (Part 02, R02a), `RecordSpan::{attributes, index}` (Task 2);
  - `kMaxJudgedBatchRecords` (Task 3).
- Produces (exact; contract additions 3 and 5):
  ```cpp
  namespace rune::node {
  inline constexpr int32_t kMaxJudgedBatchRecords = rune::protocol::client::kMaxJudgedBatchRecords;
  struct WriteFacts {
      uint8_t acks = 0; bool idempotent = false; bool transactional = false; bool atomicBatch = false;
      bool importGranted = false; bool legacyMode = false; bool levelCheckedWrites = false;
  };
  struct TopicHookFacts { bool keyIndex = false; bool autoIncrement = false; bool constrainedSource = false; bool verdictSource = false; };
  /// 03: every member false.  04 sets verdictSource (state.verdicts != nullptr), 05 keyIndex (spec->keyIndex),
  /// 06 autoIncrement (spec->autoIncrement), 04 (TestReject) / 09 constrainedSource.
  [[nodiscard]] TopicHookFacts topicHookFactsOf(const PartitionWriteState& state) noexcept;
  struct BatchIntents {
      bool    reservedBits      = false;   // a record with attributes & kAttrReservedMask, or intent 3
      bool    explicitKeyIntent = false;   // insert with a non-key-0 key, update, removeExisting
      bool    keyZeroCreate     = false;   // insert with an all-zero key at keyWidth on an autoIncrement topic
      bool    mixedKeyZero      = false;   // keyZeroCreate and at least one record that is not one
      bool    anyIntent         = false;   // any record with intent != 0
      int32_t records           = 0;
  };
  enum class HookRefusal : uint8_t {
      None, ReservedBits, ValidationFailed, VersionAhead, LegacyMode, LevelTooLow, NoKeyIndex,
      ExplicitKeyNeedsIdempotentAndView, IntentInTxnOrAtomicBatch, TxnOnConstrainedSource,
      KeyZeroCreateNeedsIdempotentAll, KeyZeroCreateNeedsView, MixedKeyZeroBatch, ViewBatchTooLarge,
      ViewWithoutReplica, Count_
  };
  struct RequirementVerdict { rune::protocol::client::ErrorCode code{}; HookRefusal why = HookRefusal::None; };
  [[nodiscard]] constexpr RequirementVerdict evaluateRequirements(const BatchIntents&, const WriteFacts&,
                                                                  const TopicHookFacts&) noexcept;
  class CheckedWriteHook {
  public:
      static constexpr bool kActive = true;
      CheckedWriteHook(PartitionWriteState& state, const WriteFacts& facts, std::span<uint8_t> scratch) noexcept;
      rune::protocol::client::ErrorCode check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept;
      template <class LogT> rune::storage::HookStaged stage(LogT& log, const uint8_t* batch, std::size_t len) noexcept;
      void onAppended(int64_t baseOffset, int64_t lastOffset) noexcept;
      [[nodiscard]] uint64_t assignedBase() const noexcept;      // 03: 0
      [[nodiscard]] HookRefusal refusal() const noexcept;        // why check() refused (None when it passed)
      [[nodiscard]] bool checked() const noexcept;               // check() ran on this object
  };
  extern template rune::storage::ProduceOutcome rune::storage::handleProduceBatch<rune::storage::Log, CheckedWriteHook>(/*…*/) noexcept;
  extern template rune::storage::ProduceOutcome rune::storage::handleProduceBatch<rune::storage::InMemoryLog, CheckedWriteHook>(/*…*/) noexcept;
  }
  ```
- **`evaluateRequirements`**, the C8 order. It returns the first rule that fails, with `InvalidRequest`:
  1. `reservedBits` → `ReservedBits`.
  2. `facts.legacyMode && (anyIntent || acksView(facts.acks))` → `LegacyMode`.
  3. `!facts.levelCheckedWrites && (anyIntent || acksView(facts.acks))` → `LevelTooLow`.
  4. `anyIntent && !topic.keyIndex` → `NoKeyIndex`.
  5. `explicitKeyIntent && (!facts.idempotent || !acksView(facts.acks))` → `ExplicitKeyNeedsIdempotentAndView`.
  6. `explicitKeyIntent && (facts.transactional || facts.atomicBatch)` → `IntentInTxnOrAtomicBatch` (until 11).
  7. `(facts.transactional || facts.atomicBatch) && topic.constrainedSource` → `TxnOnConstrainedSource` (until 11).
  8. `keyZeroCreate` (06's C-rules):
     - `mixedKeyZero` → `MixedKeyZeroBatch`;
     - `!facts.idempotent || !acksAll(facts.acks)` → `KeyZeroCreateNeedsIdempotentAll`;
     - `topic.constrainedSource && !acksView(facts.acks)` → `KeyZeroCreateNeedsView`.
  9. `acksView(facts.acks) && records > kMaxJudgedBatchRecords && topic.verdictSource` → `ViewBatchTooLarge` (SD4).
  - Otherwise `{OK, None}`.
- **`check()`**: one `forEachRecordSpan` walk.
  - Per record, in this order (P3-8):
    - the attribute rules (`reservedBits` → stop, `InvalidRequest`);
    - `checkTypedRecord(*state_.spec, r, level)` with `level = facts_.levelCheckedWrites ? MetadataFeature::kCheckedWrites : MetadataFeature::kBase` (R-X1): `Reject` → stop, `SchemaValidationFailed`, `ValidationFailed`; `VersionAhead` → stop, `NotLeader`, `VersionAhead` (02 P2). The worker adds these two to 02's `typedRejects_` / `typedVersionAhead_` (Task 7). It covers the version lookup, the validator, the wire key and the tombstone rule (P3-10);
    - intent bookkeeping into `BatchIntents`.
  - A walk that `forEachRecordSpan` reports as structurally bad → `SchemaValidationFailed` (`ValidationFailed`), as Part 02's `checkTypedBatch` answers it. One case keeps 02's P5: below the level on a topic without `legacyValidate`, 02 never walks the batch, so a bad walk there passes unchecked (no intent or `View` means anything below the level, rules 2–3).
  - After the walk: `evaluateRequirements(intents, facts_, topicHookFactsOf(state_))`.
  - The walk never allocates. The validators and descriptors are materialized at install (C7).
- `stage()` returns `{OK, batch, len}` unchanged (06 adds the copy and patch). `onAppended` is empty in 03.
- `CheckedWriteHook.cpp` includes `rune/storage/ProducePathImpl.h` and explicitly instantiates `handleProduceBatch<Log, CheckedWriteHook>`, `handleProduceBatch<InMemoryLog, CheckedWriteHook>`, `CheckedWriteHook::stage<Log>` and `CheckedWriteHook::stage<InMemoryLog>`.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesHookTest.cpp`).
  - **`CheckedWritesRequirementTable.PinnedRows`**: 64 rows of `{BatchIntents, WriteFacts, TopicHookFacts} → {code, why}`. Each row's comment names the §11.2 row or C8 rule. The rows **must include**:

    | # | Shape | Facts | Topic | Expected |
    |---|---|---|---|---|
    | 1 | plain upsert, acks 0, not idempotent | — | all false | `OK` (§11.2 row 1) |
    | 2 | plain tombstone, acks 1, legacy | legacy, level 0 | all false | `OK` (plain writes never refused, SD10 last sentence) |
    | 3 | plain, acks 3 | idempotent, level 1 | verdictSource, records 4096 | `OK` |
    | 4 | plain, acks 3 | idempotent, level 1 | verdictSource, records 4097 | `InvalidRequest`/`ViewBatchTooLarge` (SD4) |
    | 5 | plain, acks 3 | idempotent, level 1 | verdictSource **false**, records 4097 | `OK` |
    | 6 | plain, acks 2 | legacy | — | `LegacyMode` |
    | 7 | plain, acks 3 | level 0 | — | `LevelTooLow` |
    | 8 | reserved bits, acks 0 | — | — | `ReservedBits` (rule 1 before every other) |
    | 9 | insert explicit, acks 3 | idempotent, level 1 | keyIndex false | `NoKeyIndex` |
    | 10 | insert explicit, acks 1 | idempotent, level 1 | keyIndex | `ExplicitKeyNeedsIdempotentAndView` (§11.2 row 2) |
    | 11 | update, acks 3 | **not** idempotent, level 1 | keyIndex | `ExplicitKeyNeedsIdempotentAndView` |
    | 12 | removeExisting, acks 2 | idempotent, level 1 | keyIndex | `OK` (`Leader \| View` qualifies) |
    | 13 | update, acks 3 | idempotent, txn, level 1 | keyIndex | `IntentInTxnOrAtomicBatch` (§11.2 row 6) |
    | 14 | plain, acks 1 | txn, level 1 | constrainedSource | `TxnOnConstrainedSource` (§11.2 row 5) |
    | 15 | plain, acks 1 | atomic, level 1 | constrainedSource | `TxnOnConstrainedSource` |
    | 16 | plain, acks 1 | txn, level 1 | keyIndex only | `OK` (unconstrained transactions work) |
    | 17 | key-0 creates, acks 1 | idempotent, level 1 | keyIndex, autoInc | `OK` (§11.2 row 3) |
    | 18 | key-0 creates, acks 0 | idempotent, level 1 | keyIndex, autoInc | `KeyZeroCreateNeedsIdempotentAll` |
    | 19 | key-0 creates, acks 1 | **not** idempotent | keyIndex, autoInc | `KeyZeroCreateNeedsIdempotentAll` |
    | 20 | key-0 creates, acks 1 | idempotent, level 1 | keyIndex, autoInc, constrained | `KeyZeroCreateNeedsView` (§11.2 row 4) |
    | 21 | key-0 creates, acks 3 | idempotent, level 1 | keyIndex, autoInc, constrained | `OK` |
    | 22 | key-0 mixed with plain, acks 1 | idempotent, level 1 | keyIndex, autoInc | `MixedKeyZeroBatch` (§6.3) |
    | 23 | key-0 creates, acks 1 | idempotent, txn, level 1 | keyIndex, autoInc | `OK` ("allowed in transactions" on a key-index-only topic) |
    | 24 | key-0 creates, acks 1 | legacy | keyIndex, autoInc | `LegacyMode` |

    The remaining 40 rows cover each rule's boundary from both sides (each condition flipped alone).
  - **`CheckedWritesRequirementTable.SweepInvariants`**: over all 86,016 cells (matrix B):
    - **(i)** `!anyIntent && !reservedBits && !acksView(acks)` → `OK`, whatever the facts. The only exception is rule 7 (`transactional`/`atomic` on a constrained source).
    - **(ii)** `reservedBits` → `ReservedBits` always.
    - **(iii)** `legacyMode && (anyIntent || acksView)` → `LegacyMode` unless `reservedBits`.
    - **(iv)** every non-`OK` code is `InvalidRequest` (05 adds the R05e exception: `KeyIndexActivating` answers `NotLeader`).
    - **(v)** flipping any single fact can change the result only to the rule that fact feeds (a monotonicity check per rule).
  - **Hook `check()` on real bytes**, a `PartitionWriteState` built with Part 02's spec builder for the `characters` descriptor (`id i64 key`, `type enum`, `name string`):
    - `CheckedWritesHook.ValidUpsertBatchPasses` (3 records): `OK`, `refusal() == None`.
    - `CheckedWritesHook.UnknownVersionIsSchemaValidationFailed`: version 0, and a version below the highest installed one with no entry → `SchemaValidationFailed` / `ValidationFailed`.
    - `CheckedWritesHookRegression.VersionAboveTheInstalledMaxIsRetriableNotLeaderRegression` (02 P2, R-X1): a record at `maxVersion() + 1` → `NotLeader` / `VersionAhead`; once that version is installed in the spec, the same batch on a fresh hook → `OK`. (The worker half, counters and LEO, stays pinned by 02's `CheckedWritesWorkerRegression.VersionAheadIsRetriableUntilInstalledRegression`.)
    - `CheckedWritesHook.WireKeyMismatchIsSchemaValidationFailed`: key 8 B ≠ `id` → `SchemaValidationFailed` / `ValidationFailed`.
    - `CheckedWritesHook.TombstoneSkipsValueValidation` (02 P1): empty value with an 8 B key → `OK`; a 7 B key → `ValidationFailed`; a tombstone on a keyless typed topic → `ValidationFailed`.
    - `CheckedWritesHookRegression.BelowTheLevelKeepsPartTwosLegacyRuleRegression` (02 P5): level 3, a non-`legacyValidate` spec, invalid UTF-8 → `OK`; the same with `legacyValidate` and a valid record → `OK`, with an unknown version → `SchemaValidationFailed`.
    - `CheckedWritesHook.ReservedAttributeBitIsInvalidRequest`: record 2 with attributes 0x04 → `InvalidRequest` / `ReservedBits`; with attributes 0x03 → same.
    - `CheckedWritesHookRegression.InvalidRecordBeforeAReservedBitAnswersSchemaValidationFailedRegression` (P3-8 order): record 0 invalid, record 1 reserved → `SchemaValidationFailed`. Records swapped → `InvalidRequest`.
    - `CheckedWritesHook.AnyIntentIsRefusedUntilAKeyIndexExists`: insert intent (1) on a valid record, `acks 3`, idempotent, level on → `InvalidRequest` / `NoKeyIndex` (the 03 handover: "has key index" false).
    - `CheckedWritesHook.StageIsIdentityAndAssignedBaseIsZero`: `stage(log, b, n)` returns `{OK, b, n}`; `assignedBase() == 0`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `'rune/node/CheckedWriteHook.h' file not found`.
- [ ] **Step 3: Implement** as specified. `evaluateRequirements` is the rule list above in that order. Keep it `constexpr`, so the pinned table can also be checked with `static_assert` for rows 1, 4, 8, 17 and 20.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_node && ctest --test-dir build/root -R 'CheckedWritesRequirementTable|CheckedWritesHook' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/include/rune/node/CheckedWriteHook.h broker/libs/node/src/CheckedWriteHook.cpp \
        broker/libs/node/tests/node/CheckedWritesHookTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(node): CheckedWriteHook - one record walk (attributes, validation, wire key, intents) and the section 11.2 requirement table" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Worker and gateway wiring: the acks byte, the hook in `produceCore_`, the frame checks, counters

**Files:**
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `ProduceFields` (`:8119-8134`): `bool acksAll` becomes `uint8_t acks` plus `acksAll()` / `acksView()`;
  - `handleWorkerIngressFrame_` (`:8082`): `f.acks = msg.acks`;
  - `executeProduceWire_` (`:3944-3961`): `f.acks` from `op.acks()` and the deferAck flag (below);
  - `produceCore_` (`:8136-8439`): the P3-2 strip, the P3-1 frame checks, the hook dispatch at `:8310-8321`; Part 02's pre-append `checkTypedBatch` call (which replaced `schemaRejects_` at `:8255`) is deleted;
  - the reservation predicate (`:8286`, `acksAll || asyncStrict` → `acksAll || acksView || asyncStrict`);
  - counters and test seams next to `identityMismatchRejects_`.
- Modify: `broker/libs/node/include/rune/node/WorkerOp.h:202` (the `acks` comment: "the wire byte: bit 0 All, bit 1 View")
- Modify: `broker/libs/gateway/src/ClientGateway.cpp`:
  - `:3872`: `const bool acksAll = pc::acksAll(msg.acks);`;
  - legacy refusal right after it: `if (pc::acksView(msg.acks) && !shardMode_)` → `sendResp(InvalidRequest, partition, 0)`, counted `viewAcksRefusedLegacy_`;
  - the inline path (`:4100` on): refuse `acksView` the same way;
  - `ProduceResponse::ENCODED_SIZE <= kScratchSize` asserts (`:596`, `:6934`) → `ENCODED_SIZE_V12`.
- Create: `broker/libs/node/tests/support/CheckedWritesAckRig.h` (the rig; spec under Task 9 "Rig", and this task builds the parts its tests use: shard mode with the ack sink or `Options{.ackSink = false}`, typed spec install, `installView`, `produce`, `routedProduce`, `followerFetch`, `marker`, `setLevel`, `awaitAck`), `broker/libs/node/tests/node/CheckedWritesWorkerHookTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt`. Create `rune_checked_writes_ack_tests` (`tests/node/CheckedWritesWorkerHookTest.cpp`) inside the `if(aeron_FOUND)` block next to `rune_shard_worker_ack_tests` (`:1227`), linking `rune_node rune_replication rune_codec rune_view aeron::aeron GTest::gtest_main`, with `PROPERTIES TIMEOUT 300 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}"`.
- Modify: `broker/libs/node/tests/integration/ProduceValidationTest.cpp`, `broker/libs/node/tests/node/ShardWorkerAckTest.cpp`
- Test: `broker/libs/node/tests/node/CheckedWritesWorkerHookTest.cpp`, `broker/libs/node/tests/integration/ProduceValidationTest.cpp`

**Interfaces:**
- Consumes: `CheckedWriteHook`, `WriteFacts`, `HookRefusal` (Task 6); `writeOf_`, `checkedWritesEnabled_()` (Part 02); `pc::acksAll` / `pc::acksView` (Task 3).
- Produces:
  ```cpp
  struct ProduceFields { /* … */ uint8_t acks = 0;   // the wire byte (C4)
      [[nodiscard]] bool acksAll()  const noexcept { return rune::protocol::client::acksAll(acks); }
      [[nodiscard]] bool acksView() const noexcept { return rune::protocol::client::acksView(acks); } };
  [[nodiscard]] uint64_t Worker::hookChecksForTest() const noexcept;
  [[nodiscard]] uint64_t Worker::hookRefusalsForTest(HookRefusal) const noexcept;
  ```
- **`executeProduceWire_`:** `f.acks = (static_cast<uint8_t>(op.acks()) & pc::kAcksWireView) | (((op.deferAck() & kOpFlagDeferAck) != 0 && rep != nullptr) ? pc::kAcksWireAll : 0)`. The `All` bit keeps today's deferAck semantics; `View` rides the existing SBE `acks` field (`rune-internal-ring.xml` id 8, encoded at `Worker.h:1372`). No ring change.
- **`produceCore_` order**, after the empty and oversize guards:
  - **(a) P3-2.** `txn = rbSpan.size() >= kAttributesOffset + 2 && (LE16 attrs & kAttrTransactional)`. If `txn`, `acks &= ~kAcksWireView`; `f` is const, so use a local `acks`.
  - **(b) P3-1.** If `acksView(acks)`:
    - `!workerSideAcks()` → `LegacyMode`;
    - else `!checkedWritesEnabled_()` → `LevelTooLow`;
    - else `replica == nullptr` → `ViewWithoutReplica`.
    - Each answers `InvalidRequest`, counts `hookRefusals_[why]`, and appends nothing.
  - **(c)** `PartitionWriteState* ws = writeOf_(replica);`. If `ws != nullptr` [[unlikely]], build the facts and dispatch with the hook, else today's call:
    - `WriteFacts{.acks = acks, .idempotent = f.producerId >= 0, .transactional = txn, .atomicBatch = false, .importGranted = false, .legacyMode = !workerSideAcks(), .levelCheckedWrites = checkedWritesEnabled_()}`;
    - `CheckedWriteHook hook(*ws, facts, {})`;
    - dispatch `rune::storage::handleProduceBatch(…, crcPreverified, hook)` on `*log` or `*mem`, else today's call.
    - `if (hook.checked()) ++hookChecks_;`.
    - A refusal increments `hookRefusals_[hook.refusal()]` and is logged rate-limited (first, then every 1024th, like `identityMismatchRejects_`). `ValidationFailed` also increments 02's `typedRejects_` and `VersionAhead` 02's `typedVersionAhead_`, so `typedRejectsForTest()` / `typedVersionAheadForTest()` keep their meaning (R-X1).
- `hookRefusals_` is a `std::array<uint64_t, std::to_underlying(HookRefusal::Count_)>`, a worker-owned plain member.

- [ ] **Step 1: Write the failing tests.**
  - `ProduceValidationTest.cpp` (Part 02's rig there installs the typed spec):
    - `CheckedWritesValidationRegression.ValidationRunsAfterDedupeRegression` (SD2):
      - **(1)** An idempotent producer (pid 11) sends a schema-invalid batch at seq 5 → `OutOfOrderSequence`, not `SchemaValidationFailed`.
      - **(2)** The same invalid batch at seq 0 → `SchemaValidationFailed`, LEO 0.
      - **(3)** A valid batch at seq 0 is then accepted at offset 0. The refusal did not consume the sequence.
    - `CheckedWritesValidationRegression.DuplicateRetryNeverRunsTheHookRegression`: a valid batch (pid 11, seq 0) twice → the second answers the original offset, `hookChecksForTest() == 1`, LEO 1.
    - `CheckedWritesValidation.UntypedTopicNeverBuildsAHook`: 100 produces on an untyped topic → `hookChecksForTest() == 0`.
  - `CheckedWritesWorkerHookTest.cpp` (the rig, shard mode with the ack sink unless named):
    - `CheckedWritesWorkerHook.IntentOnATypedTopicWithoutKeyIndexIsInvalidRequest`: insert intent, `acks 3`, idempotent → `InvalidRequest`, `hookRefusalsForTest(NoKeyIndex) == 1`, LEO 0.
    - `CheckedWritesWorkerHook.ViewBelowTheLevelIsInvalidRequestOnEveryTopic`: post `WorkerOp::Type::ClusterFeatureLevel` with level 3 (C2), then `acks 3` on a typed topic and on an untyped topic → both `InvalidRequest` (`LevelTooLow` count 2), LEO 0. After level 4 → both accepted.
    - `CheckedWritesWorkerHook.ViewWithoutAWorkerSideAckSinkIsInvalidRequest`: `Options{.ackSink = false}` (legacy), `acks 3` → the drained `WorkerReturn` carries `errorCode 39`, `LegacyMode` count 1.
    - `CheckedWritesWorkerHook.PlainWritesNeedNothingInLegacyMode`: `Options{.ackSink = false}`, typed topic, `acks 0` and `1`, non-idempotent → both accepted.
    - `CheckedWritesWorkerHook.TransactionalUnconstrainedBatchWorksAsToday`: a transactional batch (pid 77, `acks 1`) on a typed topic → accepted.
    - `CheckedWritesViewAckRegression.ViewBitOnATransactionalBatchIsIgnoredRegression` (P3-2, Review Focus 2): one view installed, `produce(1, 3, pid 77, seq 0, txn = true)` → acked OK at the HW (after `followerFetch()`), while that open transaction still holds the LSO and so the fold (`foldCursor(view) == 0` at the ack step).
  - `ShardWorkerAckTest.cpp`: update `ShardWorkerAckRegression.ShardIngressEnforcesTopicSchema` (`:656`) to install the spec the way Part 02 installs it, if 02 did not. It must stay green.
  - Gateway, `broker/libs/node/tests/integration/GatewayAcksAllDeferredAckTest.cpp`, `CheckedWritesGatewayRegression.LegacyGatewayRefusesTheViewBitRegression`:
    - a legacy-mode gateway receives `Produce{acks = 3}` → `ProduceResponse{InvalidRequest}` and nothing routed;
    - `acks = 1` behaves as before;
    - `acks = 2` with `!committedMetadataReady_` → `InvalidRequest` (legacy). In shard mode it is routed with `op.acks == 2`, `op.deferAck == false`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_ack_tests rune_produce_validation_tests`. Expected: `no member named 'hookChecksForTest'`.
- [ ] **Step 3: Implement** as specified. Delete Part 02's pre-append `checkTypedBatch` call in `produceCore_`; `CheckedWriteHook::check` now calls 02's `checkTypedRecord` per record (Task 6, R-X1). Keep 02's helpers where 02 put them (`checkTypedBatch` stays for 02's matrices, fuzz test and bench). Part 02's worker tests (`CheckedWritesWorkerRegression.VersionAheadIsRetriableUntilInstalledRegression`, `…LevelRaiseStartsFullValidationRegression`, `…TombstoneOnValidatedTopicIsAcceptedRegression`) must pass unchanged.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_checked_writes_ack_tests rune_produce_validation_tests rune_shard_worker_ack_tests rune_produce_path_tests rune_worker_tests && ctest --test-dir build/root -R 'CheckedWrites|ProduceValidation|ShardWorkerAck|ProducePath|GatewayAcksAll|WorkerRegression' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/WorkerOp.h \
        broker/libs/gateway/src/ClientGateway.cpp broker/libs/node/tests/support/CheckedWritesAckRig.h \
        broker/libs/node/tests/node/CheckedWritesWorkerHookTest.cpp broker/libs/node/CMakeLists.txt \
        broker/libs/node/tests/integration/ProduceValidationTest.cpp broker/libs/node/tests/node/ShardWorkerAckTest.cpp \
        broker/libs/node/tests/integration/GatewayAcksAllDeferredAckTest.cpp
git commit -m "feat(node): the write hook in produceCore_ (validation after dedupe), one acks decoder (SD12), View frame checks (SD10/SD11), hook counters" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller).** The same commands as Checkpoint A, plus `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure`.

---

## Phase 3 — `View` acks end to end

### Task 8: `WorkerViewSet::minFoldCursor`

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h`. Add it next to `interestedIn` (`:405-407`). It reads `subscriptionIndex_` (`:736`), `views_` (`:735`), `instKey` (`:554`) and `ViewInstance::foldLeadership` (`:226`).
- Test: `broker/libs/node/tests/node/CheckedWritesMinFoldCursorTest.cpp` (create; add to `rune_checked_writes_tests`). Helpers as in `WorkerViewSetTest.cpp:100-170`: `charSourceV1`, `hpView`, `oneUpsert`.

**Interfaces:**
- Produces (C4 + contract addition 6):
  ```cpp
  [[nodiscard]] int64_t minFoldCursor(int32_t srcTopicId, int32_t partition, uint64_t leadership = 0) const noexcept;
  ```
  - `INT64_MAX` when no view is interested in `srcTopicId`.
  - Otherwise the minimum over the interested views of their instance's `foldCursor`, where a missing instance counts 0 and so does an instance with `leadership != 0 && foldLeadership != leadership`.
  - No allocation, one hash lookup per interested view.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesMinFoldCursor.NoInterestedViewIsInt64Max`: an empty set → `minFoldCursor(5, 0) == INT64_MAX`; with a view over topic 5, `minFoldCursor(6, 0) == INT64_MAX`.
  - `CheckedWritesMinFoldCursor.InstalledButNeverFoldedCountsZero`: `installView` over 5, no `driveFolds` → `minFoldCursor(5, 0) == 0`.
  - `CheckedWritesMinFoldCursor.TracksTheSlowestView`: two views over 5. Drive view A to bound 2 and view B to bound 1 (separate `driveFolds` calls with different bounds and readRange spans) → `1`. Drive B to 2 → `2`.
  - `CheckedWritesMinFoldCursor.PartitionsAreIndependent`: fold partition 0 to 2 and leave partition 1 unfolded → `(5, 0) == 2`, `(5, 1) == 0`.
  - `CheckedWritesMinFoldCursorRegression.OtherLeadershipCountsZeroRegression`: `driveFolds(…, bound 2, now 0, nullptr, leaderGen 3)` → `minFoldCursor(5, 0, 3) == 2`, `minFoldCursor(5, 0, 4) == 0`, `minFoldCursor(5, 0) == 2`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'minFoldCursor'`.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesMinFoldCursor|WorkerViewSet' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/tests/node/CheckedWritesMinFoldCursorTest.cpp \
        broker/libs/node/CMakeLists.txt
git commit -m "feat(views): WorkerViewSet::minFoldCursor, leadership-scoped (an instance folded under another leadership counts 0)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `View` waiters, the v12 ack sink, the matrix and the stress test

**Files:**
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - **(a)** `struct PendingAcksView` and `OrderedWaitQueue<PendingAcksView> pendingAcksView_`, next to `PendingAcksAll` (`:5812-5825`).
  - **(b)** Reserve it beside `pendingAcksAll_` at `:415` (`setMaxOwedReturns`) and `:620` (constructor).
  - **(c)** Count it in `reservedAckSlots_()` (`:7760-7766`).
  - **(d)** Park sites in `produceCore_`:
    - the non-Strict tail (`:8413-8438`): `acksView()` parks in `pendingAcksView_` before the `!acksAll` early return;
    - Strict (`:8397-8411`): `dp.thenAwaitHw = (acksAll || acksView) ? replica : nullptr; dp.thenAwaitAcks = acks;`.
  - **(e)** `DurablePending` (`:5418-5435`) appends `uint8_t thenAwaitAcks = 0;`. `fireDurable_` (`:7415-7427`) moves a `View` entry into `pendingAcksView_` (P3-9).
  - **(f)** `int flushOwedAcksView_(int64_t nowNs)`, placed per P3-7 in both duty loops (after `Worker.h:1846-1849` and `:1975-1980`).
  - **(g)** Purges:
    - the ownership sweep (`:6811-6820`) also sweeps `pendingAcksView_` (NotLeader);
    - `failOwnPending_` (`:7459-7474`) also fails `pendingAcksView_` entries on the failed log when `includeAcksAll`.
  - **(h)** `emitProduceAck_` (`:2132-2149`) copies the C5 fields into `AckEntry`.
- Modify: `broker/libs/node/include/rune/node/WorkerOp.h`. `WorkerReturn` appends `uint64_t assignedBase = 0; uint32_t verdictRun = 0; uint16_t verdictCount = 0; uint8_t hasVerdicts = 0;` after `directAck` (`:578`). These are in-process only: `WorkerReturnCodec` is untouched, and `ProtocolRegression.WorkerReturnLayoutUnchanged` (`F15EosRegressionTest.cpp:1091`) must pass unchanged.
- Modify: `broker/libs/node/include/rune/node/SessionAckSink.h`:
  - `AckEntry` (`:111-131`) appends `uint64_t assignedBase = 0; uint32_t verdictRun = kNoVerdictRun; uint16_t verdictCount = 0; uint8_t hasVerdicts = 0;`;
  - the claim sizes (`:360`, `:382`) become `ProduceResponse::encodedSizeFor(ver, a.hasVerdicts, a.verdictCount)`;
  - `encode_` (`:1065-1076`) sets `assignedBase`, `hasVerdicts` and `verdictCount` (an empty `verdictBytes` span in 03: the arena is Part 04's, and `assert(a.verdictCount == 0)`);
  - `ackPlainScratch_` / `ackSealedScratch_` (`:1136-1138`) are sized `ProduceResponse::kMaxEncodedSize` (+ `kCryptoOverhead`).
- Create: `broker/libs/node/tests/node/CheckedWritesViewAckTest.cpp`, `broker/libs/node/tests/node/CheckedWritesViewAckMatrixTest.cpp`
- Modify: `broker/libs/node/tests/support/CheckedWritesAckRig.h` (the rest of the rig below: transient replica, `demote()`/`promote()`, Strict tier, frame lengths and first-seen steps)
- Modify: `broker/libs/node/CMakeLists.txt`. Add both files to `rune_checked_writes_ack_tests` (created by Task 7).
- Modify: `broker/libs/node/tests/node/SessionAckSinkTest.cpp`

**Interfaces:**
- Consumes: `minFoldCursor` (Task 8), `ProduceFields::acks` (Task 7), `ProduceResponse` v12 (Task 3).
- Produces:
  ```cpp
  struct PendingAcksView {   // = PendingAcksAll's fields + the wire acks byte (C4)
      rune::replication::PartitionReplica* replica = nullptr; int64_t lastOffset = 0; int64_t deadlineNs = 0;
      uint64_t logGen = 0; int32_t minIsr = 1; WorkerReturn ret; uint8_t acks = 0; };
  int flushOwedAcksView_(int64_t nowNs);
  ```
- **The fire rule, per entry, in park order (`sweep`):**
  - `rep = waiterReplica_(…)`; `nullptr` → `NotLeader`;
  - else, if `rep->highWatermark() > lastOffset && (!acksAll(acks) || |ISR| ≥ minIsr) && cursor > lastOffset`, with `cursor = viewSet_ ? viewSet_->minFoldCursor(rep->topicId(), rep->partitionId(), rep->leadershipGeneration()) : INT64_MAX` → `OK`;
  - else `nowNs ≥ deadlineNs` → `Internal` (one `spdlog::warn` like `flushOwedAcksAll_:7600`);
  - else keep waiting.
  - Part 04 adds `&& state->verdicts->judgedThrough() > lastOffset`.
- **Rig** `CheckedWritesAckRig.h` (namespace `rune::node::test`; Task 7 builds part of it, this task completes it), modeled on `ShardWorkerAckTest.cpp:56-224`:
  - **Setup:** a real `Worker` in shard mode with the ack sink; a disk `Log` + `PartitionReplica` (`replicas`, `minIsr`) or a transient replica; an embedded invoker driver; session 5 announced with `clientVersion = 12` (default) or a given version; `setAckTimeoutNs(50'000'000)`.
  - **Helpers:**
    - `produce(corr, wireAcks, pid = -1, seq = -1, txn = false, records = 1)`;
    - `routedProduce(corr, wireAcks, pid, seq)`, with `op.acks = wireAcks` and `op.deferAck = acksAll(wireAcks)`;
    - `installView(viewTopicId)`, via `WorkerViewSet` + `compileView` as in `WorkerTest.cpp:751-787`;
    - `followerFetch()`, via `replica->onFetchRequest` as in `ShardWorkerAckTest.cpp:340`;
    - `marker(pid, commit)`, via `AppendControlRecord` as in `TransientRig.h:164-171`;
    - `setLevel(int16_t)`;
    - `demote()` / `promote()`;
    - `step()`, `stepN(n)`, `awaitAck(corr)` (hard cap `kCap = 200'000` steps);
    - `foldCursor(view)`.
  - **Recorded per ack:** the raw frame length and the step it first appeared at.

- [ ] **Step 1: Write the failing tests.**
  - `SessionAckSinkTest.cpp`:
    - `CheckedWritesAckSink.V12SessionGets54ByteFrameWithAssignedBase`: announce with version 12; enqueue `AckEntry{… assignedBase = 42}` → the sent frame is 54 B, decodes to `assignedBase == 42`, `!hasVerdicts`.
    - `CheckedWritesAckSink.EmptyVerdictTrailerIs56Bytes`: `hasVerdicts = 1, verdictCount = 0` → 56 B, decodes to `hasVerdicts`, count 0.
    - `CheckedWritesAckSinkRegression.OldSessionGetsTheOldFrameWhateverTheEntryCarriesRegression`: versions 1, 2 and 11 with `assignedBase = 42, hasVerdicts = 1` → 38, 46 and 46 B, byte-equal to `ProduceResponse::encodeFor(…, v)` of the same fields with neither set.
    - `CheckedWritesAckSinkRegression.SealedV12AckFitsTheScratchRegression`: the sealed path with a v12 session (crypto ctx as the existing sealed tests) → one frame of `56 + kCryptoOverhead` B.
  - `CheckedWritesViewAckTest.cpp`:
    - `CheckedWritesViewAck.AllViewWaitsForTheFoldThenAcks`: replicas {1, 2}, `minIsr 1`, one view, `produce(1, 3)`.
      - 2,000 steps → no ack.
      - `followerFetch()` → acked OK. At the ack step, `foldCursor(view) > ack.lastOffset` and the frame is 54 B.
    - `CheckedWritesViewAck.LeaderViewWaitsForCommitLikeAllView`: the same with `produce(1, 2)` → no ack before the fetch, then OK.
    - `CheckedWritesViewAck.NoViewsAddsNothing`: no view; `produce(1, 3)` and `produce(2, 1)` → both acked in the same step after the fetch.
    - `CheckedWritesViewAckRegression.AllAcksNeverQueueBehindViewWaitersRegression` (SD16):
      - another producer's transaction is open (pid 77, `txn = true`, `acks 1`); `produce(1, 3)`; `produce(2, 1)`; `followerFetch()`;
      - ack 2 OK while ack 1 is still absent;
      - `marker(77, true)`, `followerFetch()` → ack 1 OK.
    - `CheckedWritesViewAck.BehindAnAbortedTransactionAcksAfterTheAbort`: the same with `marker(77, false)` → OK.
    - `CheckedWritesViewAck.LeaderLossWhileParkedIsNotLeader`: parked `View`, `demote()` → `NotLeader` before `ackTimeoutNs`.
    - `CheckedWritesViewAck.DeadlineIsInternal`: replicas {1, 2}, `minIsr 2`, no fetch → `Internal` at the first step with `now ≥ park + 50 ms`, never earlier.
    - `CheckedWritesViewAckRegression.StaleLeadershipFoldCursorNeverReleasesAViewAckRegression` (Review Focus 1):
      - fold offset 0 under leadership L, then `demote()` and `promote()` (leadership L + 2);
      - hold the fold with an open transaction opened after the promote;
      - an idempotent duplicate retry of the offset-0 batch with `acks 3` must not be acked while `instanceServeState(view, 0).leadership != L + 2`;
      - it is acked OK once the view has folded under L + 2 (after the commit).
    - `CheckedWritesViewAckRegression.StrictTierViewWaitsForFsyncHwAndFoldRegression` (Review Focus 4):
      - `durabilityTier 2`, `acks 3`;
      - the ack is absent after the fetch until the fsync completion is reaped, and absent after the fsync until the fold;
      - the order is asserted with the rig's step stamps.
    - `CheckedWritesViewAckRegression.RoutedViewProduceWithoutAReservationIsRefusedNotParkedRegression` (Review Focus 3):
      - fill the owed cap with parked `All` waiters (`setMaxOwedReturns(kMinOwedReturns)`, `minIsr 2`, no fetch);
      - then `routedProduce(…, 3)` → `NotLeader`, LEO unchanged;
      - `reservedAckSlotsForTest()` includes a parked `View` waiter (one parked `View` adds exactly 1).
    - `CheckedWritesViewAck.DuplicateBeforeAndAfterTheFoldIsAnsweredOnceEach`: an idempotent `acks 3` batch and its retry before the fold → both answered OK with the same `baseOffset` after the fold. A retry after the ack → answered OK at once (the condition holds). LEO 1.
  - `CheckedWritesViewAckMatrixTest.cpp`:
    - `CheckedWritesViewAckMatrix.Cell` (`TEST_P` over matrix A's 80 cells; the N/A cells `GTEST_SKIP()` with the rule's reason) and `expect03`, as the Interaction matrix section states.
    - `CheckedWritesViewAckMatrixCountRegression.CellCountsArePinned`: `applicable == 78`, `skipped == 2`. Part 04 Task 10 deletes this pin when it folds the matrix in; its row moves to the shared `CheckedWritesMatrixCountRegression.CellCountsArePinned` (R03d).
    - `CheckedWritesViewAckStressRegression.SeededWaitersMatchTheModel`: as in the Interaction matrix section. Failure messages print `seed=<n> step=<k>`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_ack_tests rune_session_ack_sink_tests`. Expected: `no member named 'assignedBase' in 'rune::node::AckEntry'`.
- [ ] **Step 3: Implement** as specified. Keep `flushOwedAcksView_` a sibling of `flushOwedAcksAll_` (`Worker.h:7576-7623`), with the same flight-recorder marks and the `ShWaiterFire` code value.
- [ ] **Step 4: Run.**
  - `cmake --build build/root -j14 --target rune_checked_writes_ack_tests rune_session_ack_sink_tests rune_shard_worker_ack_tests rune_worker_tests`
  - `ctest --test-dir build/root -R 'CheckedWritesViewAck|CheckedWritesAckSink|SessionAckSink|ShardWorkerAck|WorkerReturnLayoutUnchanged|AsyncDurability' -j14 --output-on-failure`
  - Then the matrix and stress under load: `ctest --test-dir build/root -R 'CheckedWritesViewAckMatrix|CheckedWritesViewAckStress' --repeat until-fail:5 -j14 --output-on-failure`.
  - Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/WorkerOp.h \
        broker/libs/node/include/rune/node/SessionAckSink.h broker/libs/node/tests/support/CheckedWritesAckRig.h \
        broker/libs/node/tests/node/CheckedWritesViewAckTest.cpp broker/libs/node/tests/node/CheckedWritesViewAckMatrixTest.cpp \
        broker/libs/node/tests/node/SessionAckSinkTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(node): View acks - own waiter queue (HW + ISR + leadership-scoped fold), Strict -> HW -> fold, v12 ack sink (assignedBase, trailer), 80-cell matrix, seeded stress" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Real e2e: a real client against real brokers on the default runtime

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`. Part 02 created it in `rune_client_broker_tests` (`broker/libs/node/CMakeLists.txt:1731`).
- Uses:
  - `tests/client/OneBroker.h`: `OneBroker(tag, Runtime::Shard)`, `connectClient()`, `viewPlanOver(source)`, `idDescriptor`;
  - `tests/support/CatalogBrokerCluster.h`: `ThreeNodes::start`, `crash(id)`, `node(id)`;
  - the crash-and-reroute pattern of `ClientFailoverBrokerRegression.SeedLeaderCrashedMidProduceReRoutesWithinTheBudgetRegression` (`ClientFailoverBrokerRegressionTest.cpp:244`).

**Interfaces:**
- Consumes: everything above. No new production code.
- Every wait is bounded: completions are polled with `waitFor(…, 10s)` and the failure is the deadline.

- [ ] **Step 1: Write the tests.** Each starts its own broker(s). The typed topic `acct {id uint64, balance int64} key id` is created with Part 02's typed `defineTopicAndWait` form, and the view `acct_v` with `createViewAndWait("acct_v", "id", 0, viewPlanOver("acct"), …)`.
  - `CheckedWritesBrokerRegression.AllViewAckWaitsBehindAnOpenTransactionShardRegression`:
    - `OneBroker(…, Runtime::Shard)`;
    - a transactional producer (`All`, `transactionalId "tx-cw"`) begins and sends id 1;
    - producer P (`All`) sends id 2 → Ok within 2 s;
    - producer V (`All | View`) sends id 3 → **no** completion within 1 s;
    - `commitTransactionAndWait()` → Ok;
    - V's completion → Ok within 5 s;
    - `getViewRowAndWait("acct_v", 3)` → present with `balance` as sent.
  - `CheckedWritesBrokerRegression.LeaderViewBehavesLikeAllViewShardRegression`: the same with V on `Leader | View`.
  - `CheckedWritesBrokerRegression.ViewOnAPartitionWithoutViewsAcksAtCommitShardRegression`: a topic without views; 100 `All | View` sends → all Ok. The p50 completion latency is ≤ 2 × that of 100 `All` sends in the same test (no extra wait), measured with the client clock and reported.
  - `CheckedWritesBrokerRegression.LegacyRuntimeRefusesTheViewBitRegression`: `OneBroker(…, Runtime::Legacy)`; `All | View` send → `OpStatus::BrokerError`, `errorCode == 39`; `All` send → Ok.
  - `CheckedWritesBrokerRegression.AllViewProduceAcrossALeaderCrashThreeNodesRegression`:
    - `ThreeNodes::start("cw03")` (default `Mode::Shard`), `acct` with `replicas 3`;
    - V (`All | View`, `deliveryTimeoutMs 30'000`) sends ids 1..200;
    - after 100 completions, `crash(leader)`;
    - every completion is Ok (`NotLeader` is retried by the client), and every id's row is readable from `acct_v` on the new leader.
  - `CheckedWritesBrokerRegression.V12ClientAgainstV7BrokerKeepsEveryOldAckBehaviourShardRegression`: on `OneBroker` Shard, `Acks::None` (100 sends complete locally), `Acks::Leader` and `Acks::All` (each 100 sends Ok) — the "old values 0 and 1 unchanged" cell, end to end.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesBrokerRegression' --output-on-failure`. Expected: PASS. Then the whole binary: `ctest --test-dir build/root -R 'rune_client_broker|ClientDdlBroker|ClientFailoverBroker|ClaimBroker|ProducerIdBroker' -j6 --output-on-failure`.
- [ ] **Step 3: Commit.**
```bash
git add broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
git commit -m "test(e2e): View acks on real brokers - behind an open transaction, no views, legacy refusal, leader crash on three nodes" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: The allocation gate, the benches, the wire docs

**Files:**
- Create: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`. Add the new binary `rune_checked_writes_alloc_regression_tests` (`tests/node/CheckedWritesAllocRegressionTest.cpp tests/support/AllocWindow.cpp`) in the `if(aeron_FOUND)` block of `broker/libs/node/CMakeLists.txt`, modeled on `rune_auth_alloc_regression_tests` (`:1237-1241`). Parts 04–11 extend it.
- Modify: `broker/benchmark/ProduceValidateBench.cpp` (after the existing registrations, `:318-330`)
- Modify: `docs/api/wire-protocol.md`:
  - frame-catalogue rows 16 and 17: `acks` "(bit 0 All, bit 1 View, v12)", and `assignedBase (v12)`, `verdicts (v12, optional trailer)`;
  - a new section "Checked writes (client version 12)" with the `acks` bits, the v12 `ProduceResponse` layout table, the trailer, the 4096 cap and the legacy/level refusals;
  - one line in that section (R03b): "On an untyped (raw) topic the broker stores the record intent bits as sent and does not interpret them."
- Modify: `docs/api/client-cpp.md` (the producer section): the same line, plus "a raw handle refuses intents, key-0 creates and `View` locally with `SendResult::WriteNotAllowed`" (R05a; the check itself ships with Part 05).

**Interfaces:**
- Consumes: `CheckedWritesAckRig.h` (Task 9).

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateHookAndViewAcksAllocateNothingRegression`:
  - The Task 9 rig with a typed topic + spec, one view, replicas {1, 2}.
  - Warm up: 200 batches mixing 1- and 16-record typed batches, `acks` 1 and 3, idempotent, each released by `followerFetch()`.
  - Then, under `rune::testing::AllocWindow w;`, 2,000 more batches of the same mix plus 100 hook refusals (insert intent → `NoKeyIndex`) and 50 `View` deadline expiries (minIsr 2 stretch).
  - `EXPECT_EQ(w.count(), 0u)`. The window counts the test thread, which drives `doWork`.
- [ ] **Step 2: Write the benches** (`ProduceValidateBench.cpp`, the existing `g_fix` worker fixture). Each is registered with `benchmark::RegisterBenchmark`:
  - `BM_CheckedWriteHook/hook_check/1` and `/hook_check/64`: `CheckedWriteHook::check` alone on a 1- and a 64-record valid `characters` batch, reporting `ns_per_batch`.
  - `BM_CheckedWriteHook/typed_produce/1`: the full worker produce of a 1-record typed batch (`acks 1`), the same loop as `BM_ProduceValidate_On`.
  - `BM_CheckedWriteHook/view_ack/1`: append → `View` ack with one view on a single-replica partition, timed to `drainReturn`.
- [ ] **Step 3: Run the gate.** `cmake --build build/root -j14 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches** (baseline = this part's base commit on `main`, ABAB n=3, one session, same box; lean-bench rule: one prepare/restore, these benches only):
  - **Baseline worktree:** `git worktree add "$TMPDIR/rune-cw03-base" <base>`. Configure it Release `BUILD_BENCHMARKS=ON` like `build/rel`.
  - **Run** in ABAB order: `rune_broker_benchmark --benchmark_filter='BM_ProducePath_|BM_WorkerProduce_Majority|BM_WorkerProduceToPushRead|BM_SessionAckSinkFlush|BM_ProduceValidate_On|BM_CheckedWriteHook' --benchmark_repetitions=3`.
  - **Gates:**
    - `BM_ProducePath_*`, `BM_WorkerProduce_Majority_*`, `BM_WorkerProduceToPushRead_*` and `BM_SessionAckSinkFlush/*`: no regression against baseline. Same-session ABAB medians; a shift beyond the noise of BENCHMARKING.md's counts is investigated, never waived.
    - `BM_CheckedWriteHook/typed_produce/1` minus the baseline `BM_ProduceValidate_On`: ≤ 1 µs p50 (the new-validation budget). The move of validation into the hook may not add to it.
    - `BM_CheckedWriteHook/hook_check/1` p50 < 1 µs.
    - Numbers borderline on the dev Mac are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh`, run under the VM lock on `$RUNE_VM_HOST`, then `broker/bench/vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/libs/node/CMakeLists.txt \
        broker/benchmark/ProduceValidateBench.cpp docs/api/wire-protocol.md docs/api/client-cpp.md
git commit -m "bench(node): BM_CheckedWriteHook (hook check, typed produce, View ack); zero-allocation gate; wire docs for client v12" \
           -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c). Stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`.
  - CatalogGolden on `build/sub-codec`.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) `-R Regression -LE latency`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure` under `--repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 11 benches (already recorded).
  - This part has no docker or VM e2e of its own (Part 05 creates `broker/docker/checked-writes-e2e.sh`). Its real e2e is Task 10 on the default shard runtime.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model. The reviewer leaves PR comments; the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. "Untyped topics do not inspect record attribute bits (P3-1)" — `enhancement`, `sev:low`, `area:broker`.
  2. "A 4096-entry v12 ack exceeds Aeron tryClaim's max payload (P3-11)" — `bug`, `sev:medium`, `area:broker`; assigned to Part 04, which fixes it before it can occur.
  3. Every recorded deviation (weakened or re-pinned assertion) that is not urgent.
- [ ] Hand over to Part 04 per the overview's acceptance list for 03:
  - `handleProduceBatch<LogT, HookT>` exists, and `CheckedWriteHook` runs for every non-null state;
  - `Acks::View` works end to end on a partition with ordinary views;
  - every §11.2 refusal is in place, with "constrained" and "has key index" false;
  - the v12 frames encode and decode with empty verdicts and `assignedBase = 0`.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §3 write path, steps 1, 2 and 4: Tasks 5–7. Step 3 (ids) is Part 06; the `stage`/`onAppended` seams exist in Task 5.
  - "Why after dedupe": Task 5 (`DuplicateNeverCallsTheHookRegression`) and Task 7 (`ValidationRunsAfterDedupeRegression`).
  - "Template policy, no virtual, pointer next to RcSlot, one unlikely branch": Tasks 5 and 7, plus the benches in Task 11.
  - §8.9 every rule: Tasks 4 (client flags, refusals, normalization, wire), 8 and 9 (release rule, `Leader | View` ≡ `All | View`, open transactions, failures), 10 (end to end). "On a verdict source the ack carries the verdicts" is Part 04 (C4/C5): the trailer exists in Task 3, empty.
  - §11.1: Task 1. 54 has no path until Part 06, 55 until Part 11; they are defined and named.
  - §11.2, every row: Task 6's table (rows 1–24 cite them). Old clients: P3-5 and Tasks 1, 3, 9.
  - §12 wire: Tasks 1 and 3. The level gate is Tasks 7 and 9. The DDL half of §12 belongs to each feature's owner (05, 06, 07, 09).
  - §6.7 "scratch arrays grow": Task 9 (`SessionAckSink.h`), Task 7 (`ClientGateway.cpp:596`), Task 9 (`WorkerReturn`).
- **Step scan.** Every test step names its test and its assertions with the spec values (4096, 32,824, 54, 30, 12, 7, 51–58). Every code step names its file, its signature and its placement. No function body is transcribed except the order of rules in `evaluateRequirements` and the fire rule, which the tests alone do not determine.
- **Type consistency.**
  - `WriteFacts`, `TopicHookFacts`, `BatchIntents`, `HookRefusal` and `RequirementVerdict`: defined in Task 6 (contract addition 5 lists `RequirementVerdict` as the return of `evaluateRequirements`), used in Tasks 7 and 11.
  - Part 02's `checkTypedRecord` / `TypedCheck` (R-X1): used in Task 6 only; `HookRefusal::VersionAhead` maps its `VersionAhead`.
  - `ProduceFields::acks`: Task 7, used in Task 9.
  - `PendingAcksView`, `flushOwedAcksView_`, `DurablePending::thenAwaitAcks`: Task 9 only.
  - `minFoldCursor(…, leadership)`: Task 8, used in Task 9.
  - `kMaxJudgedBatchRecords`: Task 3, used in Tasks 4 and 6.
  - `ProduceResponse::{assignedBase, hasVerdicts, verdictCount, verdictBytes, encodedSizeFor(v, h, n)}`: Task 3, used in Task 9.
  - `AckEntry` fields: Task 9.
  - Client `normalized` / `acksValid` / `has`: Task 4.
- **Review Focus.** The five lines each have a named test in their owning task (Tasks 3, 4, 9).
- **Proportion.** The plan sets decisions, signatures, placements and test assertions. The bodies of `check()`, `flushOwedAcksView_`, the encoders and the rig are left to the implementer, within the stated rules.
