# Checked Writes — Part 04: the Verdict Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On the partition leader, judge every record of a verdict source after commit and in offset order, so that a rejected record changes no view, reaches no consumer, never survives compaction as a key's latest value, and gets the same verdict on every leader, every restart and every refold. Writers that ask with `View` get the per-record verdicts in the ack. The judging is proven with the test-only `TestReject` verdict view; Parts 05 and 09 plug real rules into the same seams.

**Architecture:**
- **View engine.** Folds become *prepare, then apply* (C9.3). `prepareProjection` / `prepareAggregate` compute a record's prospective rows into a fixed per-instance `Prospect` without writing; `applyProjection` / `applyAggregate` commit them. The aggregate arithmetic is one template over a table *store*: the direct store keeps today's cost for ordinary views, the overlay store computes prospects.
- **`VerdictSource`** (one per source partition, in `WorkerViewSet`) owns the partition's verdict views. Each duty pass it judges `[judgedThrough, LSO)` record by record (prepare every member, evaluate, apply all or none), then folds the other views bounded by `judgedThrough` with `FoldSkip{.aborted, .rejected}`, then appends a `VerdictMarker` control record when due.
- **Durable verdicts.** Markers replicate like data. Every replica builds its `RejectedOffsetIndex` and `ConstraintSetTimeline` from them during the leadership scan that `RcState` already runs, and keeps both across role changes. The leader adds its own verdicts at judge time. `W` (the committed marker coverage) bounds the cleaner; a verdict view's checkpoint never covers more than a marker in the local log.
- **Readers.** Every push path bounds delivery by `judgedThrough` and cuts rejected records out of a batch with `rebuildBatch` (offsets kept). `View` acks wait for `judgedThrough > lastOffset` and carry the rejection trailer from a sink-owned arena.
- **Constraint changes** take effect at `ConstraintSetChange` records; a joining member builds unjudged up to the change offset while the old set judges, and the judge waits rather than guess when a named member is not installed.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices, seeded model checks), Google Benchmark (`build/rel/broker/rune_broker_benchmark`), CMake/Ninja, real Aeron (embedded invoker driver over `aeron:ipc`) for the worker rig, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`) for the e2e slice and the cluster stress.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` — §3 (the fold half), §8.4, §8.5, §8.6, §8.7 (mechanism), §8.8, §11.3, and the §15 cells the overview gives Part 04. The binding cross-part contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C9–C12, the C16 counters, SD4, SD16, the "Shared test infrastructure" section, which this part owns). It consumes `2026-10-22-checked-writes-01-aborted-fold.md` and `2026-10-22-checked-writes-03-write-hook.md` (their Interfaces and "Contract additions"), and Part 02's C2/C7 hand-over. Executors read the spec sections, the overview, plans 01 and 03, and this file.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-04-verdict-pipeline`, cut from the latest `main` (`git fetch` first) **after Parts 01, 02 and 03 have merged**. Merge `main` in again before review.
- **Preconditions** (run before Task 1; each must match, else stop and tell the controller):
  - `grep -n "const rune::transaction::AbortedTxnIndex\* aborted" broker/libs/view/include/rune/view/FoldSkip.h` (1 hit, Part 01)
  - `grep -n "isAbortedBatch" broker/libs/view/include/rune/view/FoldSkip.h` (≥ 1 hit, Part 01)
  - `grep -n "struct PartitionWriteState" broker/libs/node/include/rune/node/WriteState.h` (1 hit, Part 02)
  - `grep -n "struct TopicHookFacts" broker/libs/node/include/rune/node/CheckedWriteHook.h` (1 hit, Part 03)
  - `grep -n "int flushOwedAcksView_" broker/libs/node/include/rune/node/Worker.h` (1 hit, Part 03)
  - `grep -n "minFoldCursor" broker/libs/node/include/rune/node/WorkerViewSet.h` (≥ 1 hit, Part 03)
  - `grep -n "kMaxJudgedBatchRecords = 4096" protocol/include/rune/protocol/client/DataMessages.h` (1 hit, Part 03)
  - `grep -n "kNoVerdictRun" broker/libs/node/include/rune/node/SessionAckSink.h` (1 hit, Part 03)
  - `grep -n "kResetMsgType *= 4" broker/libs/node/include/rune/node/ViewSnapshotChannel.h` (1 hit, Part 01)
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp` (client suites), `build/sub-codec` (CatalogGolden), `build/rel` (Release, `BUILD_BENCHMARKS=ON`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Line numbers** cite `main` before Parts 01–03. Those parts edit `Worker.h`, `WorkerViewSet.h`, `ViewFold.cpp`, `AggFold.cpp` and `SessionAckSink.h`, so anchor on the named function or member, not on the number.
- **Benches and VM.** Benches run in `build/rel`; borderline numbers are confirmed on the shared VM (`$RUNE_VM_HOST`, every command under `flock /root/vm.lock`, prepare the clean box once and restore it, `broker/bench/BENCHMARKING.md`). Never local Mac docker.
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

## Rulings this part depends on (restated; binding)

- **SD4.** A batch written with `View` to a verdict source holds at most **4096 records** (`kMaxJudgedBatchRecords`). So a `ProduceResponse` is at most 32,824 bytes (C5), and the ack scratch is sized to `ProduceResponse::kMaxEncodedSize`. Part 03 refuses bigger batches (`HookRefusal::ViewBatchTooLarge`) once `TopicHookFacts::verdictSource` is true; this part sets it.
- **SD16.** A `View` waiter behind an open transaction waits for the LSO, in its own queue (Part 03's `pendingAcksView_`).
- **SD9.** Constraints, `key.index`, intents and `autoincrement` are refused on transient topics. So a verdict source is always disk-backed; the `TestReject` seam asserts it.
- **R03a.** A v12 ack can reach 32,824 B, above an Aeron publication's `maxPayloadLength`, so `tryClaim` can't carry it. This part's ack path uses `offer` (fragmented, up to `maxMessageLength`) for any frame above `maxPayloadLength` and `tryClaim` otherwise, tested with a 4096-record batch.
- **R03d.** Part 03's matrix ids are **120–139**, folded into this part's shared matrix (Task 10).
- **R03b / R03c** (context): untyped topics never have a `PartitionWriteState`, so they are never verdict sources; the broker ignores `View` on a transactional batch, so a transactional send never parks a `View` waiter.
- **Part 01 AD5 / AD6 / AD7.** Checkpoints written by this code are marked (`CheckpointMark::FoldSkipped`); a discarded checkpoint is truncated through `CheckpointStream::ResetOwed` and `ViewSnapshotChannel::kResetMsgType = 4`. This part reuses both for its own discard rule (P4-4).
- **Part 03 P3-3.** `minFoldCursor(src, partition, leadership)` counts an instance folded under another leadership as 0. Verdict-view instances keep `foldLeadership` current like any view.
- **R04a / R09b.** Two-phase drop is Part 09's. A dropping or replaced verdict view's plan stays installed on every replica, and keeps its place in the judge, until `W` passes its `ConstraintSetChange` record; only then is it uninstalled (P4-6). Part 09 owns the failover-during-drop test.
- **R04b.** C11's "follower apply" is the `RcState` leadership scan rebuilding the index from markers (P4-1). Followers hold no verdict state between leaderships. Accepted.
- **R04c / R04d.** Marker trigger 4 (P4-3) is accepted. The shared `Timing` enum gets the appended values of P4-15; later parts only append.
- **R04e / R04g / R-X2.** The duplicate replay of P4-8. `OpStatus::StoredIdUnknown = 13` is added here as a reserved placeholder (Part 06 documents and uses it) and `OpStatus::VerdictUnknown = 14`, so the enum has no hole.
- **R04f.** This part's Contract additions are binding for later parts.
- **R07d.** This part defines `ViewKey.h` with `fromU64(uint64_t)` and `asU64()`: `ViewKey::fromU64(v).asU64() == v`, and `asU64()` requires `len ≤ 8`. Parts 05 and 06 take them from here. Part 07 owns and extends `ViewKey` (`fromBytes`, `operator==`, wide keys) and never renames them or changes their meaning.

## Part rulings (P4; binding for executors, recorded so the reviewer can overturn any)

- **P4-1 — Where replicas learn verdicts.** C11 says the index is filled "by the leader's judge, by follower apply of replicated markers, and by recovery scans". In the code, compaction installs only on the leader (`PartitionReplica::cleanerSwapBound`, `PartitionReplica.h:888-893`, returns the minimum for a follower) and subscriptions are served only by the leader. A follower needs no verdict state until it leads. So "follower apply" is realised as follows:
  - **Scan.** The leadership scan `RcState` already runs on every new leadership (`RcState.h:376-383`, `Unbuilt → Scanning` from `logStartOffset()` to the LEO) hands every control batch with `producerId < 0` to a verdict feed. The feed merges `kVerdictMarker` records into `PartitionWriteState::rejected` and `markers`, and `kConstraintSetChange` records into `constraintSets`.
  - **Kept state.** `PartitionWriteState` survives role changes (C7). The index therefore keeps this node's own earlier verdicts, which are deterministic and so still true.
  - **Merge.** It is idempotent. A disagreement between a marker and an existing entry counts `RejectedOffsetIndex::mismatches()` and logs an error. That is a determinism alarm: the matrix, the model check and the stress run assert it stays 0.
  - **Cost.** No change to the follower append path, and no second log walk.
- **P4-2 — Three positions, one rule each.**
  - **`coveredThrough` (K, local).** Every offset below it has a known verdict on this node. It is the maximum of the `finalThrough` of every marker merged (committed or not: by determinism a marker's verdicts are true, and the records it covers were committed when judged) and the judge's own cursor. It is extended over spans whose constraint set is empty.
  - **`W` (committed).** The highest `finalThrough` among markers below the HW (the `MarkerLedger`). It bounds the cleaner (§8.5).
  - **`judgedThrough()`.** `max(judgeCursor, K at activation)`. It is the consumer bound and the `View` ack condition.
- **P4-3 — Marker emission (§8.5, extended).**
  - **Range.** A marker covers `[emitFrom, judgedThrough)`. At the start of a leadership `emitFrom` is `W` extended over the spans whose constraint set is empty (the same extension K gets in P4-2), so the first marker of a source starts at its first change offset + 1, never at 0. Afterwards it is the `finalThrough` of this leadership's last appended marker. Markers therefore tile the log contiguously, overlapping only where a new leader re-marks a range an uncommitted old marker covered; the content is identical by determinism.
  - **Entries.** They come from the index (`forEachIn(emitFrom, judgedThrough)`), so verdicts of an earlier leadership of this node are re-marked and never lost.
  - **Triggers**, checked at the end of each pass:
    1. the pass rejected anything;
    2. `judgedThrough > emitFrom` and `verdict.interval` has passed since the last marker;
    3. the judge reached a change record whose set is empty (the final marker of a drop);
    4. **(added)** `judgedThrough − emitFrom ≥ checkpointCap`, the smallest `maxRecordsPerPass` of the source's persisted verdict views (P4-4). Partitions without persisted verdict views never meet trigger 4.
  - **Split.** At most `kMaxMarkerEntries = 4096` entries per marker; more make several, split at an entry's offset.
  - **Append.** `PartitionReplica::appendControlBatch(span, /*acksAll=*/false)` (`PartitionReplica.h:600`): no min-ISR gate, since a marker carries no data. `kAppendNotLeader` drops the attempt (the next leader re-marks from W). `kAppendBackPressure` and `kAppendMinIsr` retry next pass, and the index keeps every entry, so nothing is lost. Each attempt is counted in `VerdictSourceStats`.
- **P4-4 — Verdict-view checkpoints never outrun the markers.**
  - **Gate.** A persisted verdict view pushes its checkpoint delta only in a pass that appended (or found in the local log) a marker whose `finalThrough ≥ inst.foldCursor`. The judge pass is capped at `emitFrom + checkpointCap`, so the delta fits one ring message, like today's `maxRecordsPerPass` (`WorkerViewSet.h:592-598`).
  - **Discard at activation.** If a member's `foldCursor > K` (only possible when a crash lost the log tail holding that marker), the instance is discarded: tables emptied, `foldCursor = logStart`, `checkpointStream = ResetOwed` (Part 01 AD6/AD7). It is refolded, applying unjudged below K and judging from K. Counted `verdictCheckpointsDiscarded` (named apart from Part 01's `ViewFoldCounters::checkpointsDiscarded`, which counts unmarked checkpoints). Rows are rebuilt, never dropped.
  - **Other views** on a verdict source checkpoint as today. After a restart they wait at their checkpoint `O` until `judgedThrough > O`.
- **P4-5 — Set order is judging order.** A `ConstraintSet` lists members in ascending `viewObjectId`; the key index (05) has id 0 and comes first. A record's verdict is the **first failing member's**, in that order. Every member is prepared before any is evaluated, and evaluation stops at the first failure. Nothing applies unless all pass.
- **P4-6 — Changing the set (C12).**
  - **Desired vs logged.** The desired set is the worker's verdict templates for the source. The logged set is `constraintSets.latest()`. When they differ on a Ready leader, `requestChange(desired)` appends `kConstraintSetChange` at the LEO (activation offset A). The leader does this on install, on uninstall, and once per activation.
  - **Joining member.** It builds from `logStart` unjudged: it applies non-rejected records, skipping index entries, bounded by `min(A + 1, judgedThrough)`. The old set judges up to A. The judge waits at A + 1 until every member of the new set has `foldCursor == A + 1`.
  - **Leaving member** (R09b amends this rule). It judges up to its leave offset D (the drop's change record) and no record after D. It stays registered (its template is `Retiring`, not erased) until `W` passes D, that is until a committed marker covers D. Then it is dropped.
  - **Missing member.** If a member named by the logged set has no installed template with the same `(viewObjectId, planFingerprint)`, the judge **waits** (`judgeWaitsForInstall`, logged once per wait). It never judges with a guessed set.
  - **Part 09 (R04a).** A view dropped from the catalog before a lagging node installed it would make that node wait forever after a failover. Part 09's two-phase drop keeps a dropping verdict view's plan installed on every replica until `W` passes the drop's change record. It is Part 09's plan work, not an issue.
- **P4-7 — Consumers (§8.6).**
  - **Bound.** `Worker::verdictBound_(state, bound)` returns `min(bound, state->verdictKnownThrough())`. It fails closed: a partition whose timeline needs verdicts above K but has no `VerdictSource` yet is bounded at K.
  - **Partly rejected batch.** A batch with a rejected record is rebuilt without it (`rune::protocol::rebuildBatch`, `BatchRebuild.h:82`), keeping its offsets.
  - **Fully rejected batch.** It is skipped like a control batch, with no empty frame, and the cursor moves past it.
  - **Fast path.** One `[[unlikely]]` branch on `rejected.size() != 0` per batch, then one `nextRejectedAtOrAfter(base)` binary search.
- **P4-8 — Ack trailer (§11.3, C5).**
  - **`recordIndex`** is the record's `offsetDelta` (`offset − baseOffset`). Rune's `BatchBuilder` writes `offsetDelta = ordinal`, so for every Rune client it is the record's ordinal. The wire docs say so.
  - **Duplicate replay (R04e).** The dedupe window already keeps every in-window batch's base offset. `ProducerStateEntry::baseOffsets` (`broker/libs/storage/include/rune/storage/ProducerStateManager.h:104-151`, `push` at `:144-150`) holds 32 slots = `kRetryWindowSize` (`:81`). An exact match returns `ValidationResult::dup(off, off + (last − base))` (`broker/libs/storage/src/ProducerStateManager.cpp:196-204`), and `handleProduceBatch` copies it into `ProduceOutcome::baseOffset` / `lastOffset` (`broker/libs/storage/src/ProducePath.cpp:209-225`). No new state is needed.
    - **In-window duplicate with `View` on a verdict source.** It parks in `pendingAcksView_` with the original `baseOffset` / `lastOffset`. It is released when the usual C4 condition holds **and** `state->rejected.watermark() > lastOffset` (W, the committed coverage). The trailer is then built from `rejected.forEachIn(baseOffset, lastOffset + 1)` exactly as for the original. So a retry after a lost ack (`insertAndWait`) gets the original's per-record verdicts and the same rejected indexes. Counted `verdictReplays`.
    - **Later parts extend the replay.** Part 06 adds the original `assignedBase` (its side window). Part 09 adds the constraint names (`Client::constraintName` on the replayed `constraintId`s). Neither changes this rule.
    - **Known-old duplicate.** A duplicate outside the window (`baseOffset = −1`, `ProducerStateManager.cpp:206-216`, `Worker.h:8323-8329`) with `View` on a verdict source cannot have its verdicts looked up. It is answered `DuplicateSequence` (21) with no trailer, after the same HW and fold wait. The client reports **one** `OpStatus::VerdictUnknown` for the whole batch (every record's completion carries it, `errorCode 21`), never per-record errors. Without `View`, or on a partition that is not a verdict source, today's answer (`OK −1`, `ProducerImpl.cpp:2363-2370`) is unchanged. Counted `verdictUnknownDuplicates`; the stress run asserts it stays 0, because the client never has more than 32 batches in flight.
- **P4-9 — Large acks (R03a).** `AeronSessionPubPolicy::claim` (`SessionAckSink.h:1189-1201`) encodes into a policy-owned `std::array<uint8_t, kMaxEncodedSize + kCryptoOverhead>` and calls `pub->offer(…)` when `len > pub->maxPayloadLength()`. Otherwise it keeps `tryClaim`. The return codes are mapped as today.
- **P4-10 — Arena back-pressure keeps order.** When `VerdictAckArena::reserveVerdicts` fails, `flushOwedAcksView_` keeps the entry. It also keeps every later entry of the **same partition** for this sweep (a fixed 16-slot list of deferred partition keys; overflow defers the whole sweep). Pipelined acks of one partition therefore never leave out of order. Nothing is dropped.
- **P4-11 — Index capacity.** `RejectedOffsetIndex` reserves **65,536** entries at creation. A cold maintenance step (`Worker::maintainVerdictIndexes_()`, once per 1,024 passes and whenever an `add` crosses 75 %) doubles it. The cap is **2²⁴** entries. When the index is full, `add` returns false, the judge stalls with an alert (`VerdictSource::stalled()`), and delivery and `View` acks pause. It is never a verdict (§8.4 "no resource-dependent verdicts"). `dropBelow(logStart)` runs where `RcState::advance` already prunes the aborted index (`RcState.h:352-356`).
- **P4-12 — `TestReject` (C9.4).**
  - **Modes.** `TagEquals{tag}` (reject a live record whose `tag` field equals `tag`), `OffsetSet{offsets}` (reject live records at listed offsets) and `CapLiveKeys{cap}` (reject a live record that would add a key beyond `cap` live keys in the member's projection table).
  - **Deletes and verdicts.** Tombstones are never rejected (§8.2). The verdict is `{ConstraintViolation, constraintIdOf(viewObjectId, 1)}`. The member's `planFingerprint` is `TestRejectRule::fingerprint()`.
  - **Constrained.** A `TestReject` member makes the source **constrained** (`TopicHookFacts::constrainedSource`), so Part 03's rule 7 refuses transactional and atomic batches there, as Part 09's real constraints will.
- **P4-13 — One template, two stores (C9.3).** The aggregate step becomes `template <class Store> struct AggStep` over `put/get/erase`.
  - **`DirectStore`** wraps the `ViewTable&`s. `foldAggregateBatch` uses it, so ordinary views fold exactly as today (§14).
  - **`OverlayStore`** reads overlay-first and writes into fixed slots: 16 rows per table kind, enough for a move with linked lists and block swaps. `prepareAggregate` runs `AggStep<OverlayStore>`, and `applyAggregate` commits the overlay.
  - **Projections** do the same with a row image.
  - **Overflow.** An overlay overflow returns `PrepareStatus::CapacityExhausted`, and the judge stalls.
  - **Proof.** A three-way identity test (legacy frozen copy, direct fold, prepare+apply loop) pins "the same code".
- **P4-14 — The cleaner (§8.5).**
  - **State.** `VerdictCleanState` is fed by `scanTxnState` (`LogCleaner.cpp:216-251`) from control batches with `pid < 0`. The scan now runs to `max(toOffset, verdictMarkerEnd)` for those batches only. `verdictMarkerEnd` is the published end of the committed marker that carries W.
  - **Limit.** `cleanLimit = min(published W bound, the coverage it saw itself)`, so a stale publish can only make it clean less.
  - **Rules.**
    - Rejected records never enter the offset map and are dropped by both rewrite engines (`rewriteSegmentImpl`, `LogCleaner.cpp:435`, `mergeGroupWithMap`, `:1386`).
    - `kConstraintSetChange` records are always kept.
    - A marker is removable only when every offset it lists is below the pass's clean boundary, a later marker below the scan end has a higher `finalThrough`, and the marker is below `markerRemovalBound` (every assigned replica fetched it). It is then horizon-stamped and dropped like a txn marker (`markerAction`, `:261-268`).
- **P4-15 — Matrix fold-in (R03d).** Part 03's local enums move into `CheckedWritesMatrixExpectations.h`, with `Op` and `Entity` values 120–139. Part 03's six timings that §15 does not list are appended to `Timing` after the §15 list: `TxnCommitBehind, TxnAbortBehind, LeaderLossParked, AckDeadline, StrictTier, GatewayRouted`, then Part 04's own `RetryAfterWindow` (R04e). Nothing is renumbered; later parts only append (R04d).
- **P4-16 — `rejectedNotCompacted`** (StatId 7) is `rejected.countAtOrAfter(log->firstDirtyOffset())`: the cleaner drops every rejected record below its clean watermark. On a non-compacted topic it is the count at or after `logStart`.

## Contract additions (none redefines a contract type)

1. **`RejectedOffsetIndex`** (C11) appends:
   - `[[nodiscard]] int64_t coveredThrough() const noexcept;` and `void noteCovered(int64_t offset) noexcept;` (K, P4-2);
   - `[[nodiscard]] uint64_t mismatches() const noexcept;` (P4-1);
   - `[[nodiscard]] std::size_t countAtOrAfter(int64_t offset) const noexcept;` (P4-16);
   - `[[nodiscard]] std::size_t capacity() const noexcept;` and `void grow(std::size_t entries);` (cold, P4-11).
2. **`PartitionWriteState`** (C7) appends `MarkerLedger markers;` and three methods:
   - `[[nodiscard]] bool isVerdictSource() const noexcept;`: `verdicts != nullptr` or the latest logged set is non-empty;
   - `[[nodiscard]] bool isConstrainedSource() const noexcept;`: the latest set has a member of kind `Unique`, `Check` or `TestReject`;
   - `[[nodiscard]] int64_t verdictKnownThrough() const noexcept;`: P4-7. `INT64_MAX` when no offset needs a verdict.
3. **C12 types in `VerdictSource.h`:**
   - `struct ConstraintSetMember { uint64_t viewObjectId = 0; rune::view::VerdictViewKind kind{}; uint64_t planFingerprint = 0; bool operator==(const ConstraintSetMember&) const = default; };`
   - `inline constexpr std::size_t kMaxVerdictViewsPerSource = 32;`
   - `struct ConstraintSet { uint32_t id = 0; uint8_t count = 0; std::array<ConstraintSetMember, kMaxVerdictViewsPerSource> members{}; [[nodiscard]] bool empty() const noexcept { return count == 0; } [[nodiscard]] bool sameMembers(const ConstraintSet&) const noexcept; };`
   - **`ConstraintSetTimeline`:**
     - `void onChange(int64_t offset, const ConstraintSet&)` (cold, idempotent per offset);
     - `const ConstraintSet* setAt(int64_t offset) const noexcept` (the set of the latest change below `offset`; the empty set id 0 before any change);
     - `[[nodiscard]] const ConstraintSet& latest() const noexcept`;
     - `[[nodiscard]] int64_t nextChangeAtOrAfter(int64_t offset) const noexcept` (`INT64_MAX` = none);
     - `void truncateFrom(int64_t offset) noexcept`;
     - `[[nodiscard]] std::size_t size() const noexcept`.
4. **`MarkerLedger`** (`VerdictSource.h`), a fixed ring of 1,024 uncommitted markers:
   - `[[nodiscard]] bool onMarker(int64_t markerOffset, int64_t finalThrough) noexcept;` (false = full: emission waits);
   - `void onHw(int64_t hw) noexcept;`
   - `[[nodiscard]] int64_t committedThrough() const noexcept;` (W);
   - `[[nodiscard]] int64_t committedMarkerEnd() const noexcept;`
   - `[[nodiscard]] int64_t appendedThrough() const noexcept;` (the highest `finalThrough` in the local log, committed or not);
   - `void truncateFrom(int64_t offset) noexcept`.
5. **`VerdictSource` (C9.5) adjustments:**
   - `void addVerdictView(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint = 0)` (a trailing defaulted parameter);
   - `int requestChange(const ConstraintSet& next)` returns the activation offset A (≥ 0), `kChangeDeferred = -1` (not a Ready leader or back-pressured: retried from the desired set) or `kChangeUnchanged = -2`;
   - `[[nodiscard]] bool waitingForInstall() const noexcept;`.
6. **`TestRejectRule`** (`VerdictSource.h`, P4-12): `struct TestRejectRule { enum class Mode : uint8_t { TagEquals, OffsetSet, CapLiveKeys }; Mode mode = Mode::TagEquals; uint8_t tag = 0; uint32_t cap = 0; std::array<int64_t, 16> offsets{}; uint8_t offsetCount = 0; [[nodiscard]] uint64_t fingerprint() const noexcept; };` and `void WorkerViewSet::installTestVerdictViewForTest(rune::view::CompiledViewPlan, rune::view::RowLayout, rune::codec::Descriptor src, bool persisted, uint64_t viewObjectId, TestRejectRule rule);`.
7. **`WorkerViewSet`:**
   - `driveFolds` and `driveRebuild` gain a trailing `PartitionWriteState* write = nullptr`, after Part 01's `skip` (and, on `driveFolds`, `sourceLogStart`);
   - `uninstallView` of a verdict template marks it `Retiring` (P4-6) and returns true;
   - `[[nodiscard]] std::vector<uint64_t> verdictSourceKeysForTest() const;`.
8. **`RcState` verdict hooks:** `struct RcVerdictHooks { void (*begin)(void* ctx, int64_t logStart, int64_t target) noexcept = nullptr; void (*batch)(void* ctx, const uint8_t* batch, std::size_t len, int64_t baseOffset) noexcept = nullptr; void* ctx = nullptr; };` and `void RcState::setVerdictHooks(const RcVerdictHooks&) noexcept;` (P4-1).
9. **`PartitionReplica`:** `void publishCleanerVerdictMarkerEnd(Offset) noexcept;` and `[[nodiscard]] Offset cleanerVerdictMarkerEnd() const noexcept;`. This is an atomic word beside the C11 `cleanerVerdictBound`, published **before** it with release order and read **after** it with acquire order (P4-14). Task 6 adds both atomics (with C11's `publishCleanerVerdictBound` / `cleanerVerdictBound`); Task 8 adds the readers: `cleanerSwapBound` and `LogCleanerAgent` take the min with `cleanerVerdictBound()` (C11).
10. **Cleaner:**
    - `void LogCleaner::setVerdictBounds(Offset bound, Offset markerEnd) noexcept;`;
    - `VerdictCleanState& verdictState() noexcept` (and `const`) on `OffsetMap` (`OffsetMap.h:49`) and `SkimpyOffsetMap` (`SkimpyOffsetMap.h:250`);
    - `scanTxnState(…)` gains `Offset verdictMarkerEnd, VerdictCleanState& verdicts` after `markerRemovalBound`;
    - `VerdictCleanState` appends `[[nodiscard]] bool keepsControlRecord(int64_t offset, int16_t type) const noexcept;` and `void markRemovalsBelow(int64_t cleanBoundary) noexcept;`, and C11's `finish(int64_t toOffset)` becomes `finish(int64_t toOffset, int64_t publishedBound, int64_t markerRemovalBound)`.
11. **`FilterSource`** concept (`filter/FilterGroup.h:95-101`) gains `{ s.rejected() } -> std::same_as<const rune::storage::RejectedOffsetIndex*>` (nullptr = none).
12. **`FoldSkip.h`:** `[[nodiscard]] inline bool isRejectedRecord(const FoldSkip& skip, int64_t offset) noexcept;` (the per-record sibling Part 01 anticipated).
13. **Client:**
    - `ClientAgent::ProduceAck` (`ClientAgent.h:571-576`) appends `uint32_t verdictOff = 0; uint16_t verdictCount = 0; bool hasVerdicts = false;`, with the entries copied into an agent-owned `std::vector<uint8_t> verdictBytes_`. It is reserved to 65,536 × 8 B, grow-once, cleared with the acks, and its growth is counted;
    - `ProducerHub::onProduceResponse` / `ProducerImpl::onProduceResponse` gain a trailing `std::span<const uint8_t> verdicts = {}, bool hasVerdicts = false`.
14. **Matrix** (P4-15, R04d): the `Timing` values appended for Part 03, then `RetryAfterWindow` (R04e); `enum class Part : uint8_t { P03 = 3, P04 = 4, P05 = 5, P06 = 6, P07 = 7, P09 = 9, P11 = 11 };`; `std::vector<Cell> cellsFor(Part)`.
15. **Knob:** `NodeConfig::verdictIntervalMs` (env `RUNE_VERDICT_INTERVAL_MS`, default 100, SD14) and `void Worker::setVerdictIntervalNs(int64_t) noexcept`.
16. **`ViewKey::asU64()`** (C9.1 appended, R07d): `fromU64(v).asU64() == v` for every `v`; `asU64()` requires `len ≤ 8` and returns the little-endian value of `bytes[0..len)`. Part 07 keeps both names and their meaning. Also the `FoldStats::skippedRejected` counter (appended).
17. **Duplicate replay (R04e, P4-8):**
    - `OpStatus::StoredIdUnknown = 13` and `OpStatus::VerdictUnknown = 14` are appended to `client-cpp/include/rune/client/Completion.h:54` (R-X2). 13 is a reserved placeholder that Part 06 documents and first returns; adding it here keeps the enum without a hole, so exhaustive switches stay valid. 14: "Stored; its verdicts are unknown (the retry fell outside the dedupe window)."
    - `VerdictSourceStats` appends `uint64_t verdictReplays = 0;`.
    - `PendingAcksView` appends `uint8_t replay = 0;`: a duplicate whose release also waits for `W > lastOffset`.
    - Parts 06 and 09 add the assigned keys and constraint names to the replayed answer; the release rule is fixed here.

## Contract gaps (settled by the rulings; recorded for the reviewer)

- **Two-phase drop** (P4-6): Part 09's plan work (R04a, R09b). Part 04's judge waits for a missing install, which is safe; Part 09 makes it live.
- **`Timing` is extended** for Part 03's fold-in (P4-15). Accepted (R04d).
- **C11 "follower apply"** is realised through the leadership scan (P4-1). Accepted (R04b).
- **Marker trigger 4** (P4-3) adds markers beyond the spec's two triggers, only on sources with persisted verdict views. It is needed for restart determinism (P4-4). Accepted (R04c).
- **C9.3 deviation.** C9.3 says `foldBatch` and `foldAggregateBatch` are "re-implemented as the prepare-then-apply loop". Here the ordinary folds run the shared step over the direct store (`projectionStep<DirectStore>`, `AggStep<DirectStore>`), and only the judge runs prepare then apply over the overlay (P4-13). The per-record code is the same template, so "the same code computes and applies" still holds, and the three-way identity tests (Tasks 1, 2) pin it; the direct store keeps today's fold cost (§14).

## Review Focus

These are the five inputs or conditions the spec implies but no requirement names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A restart whose verdict-view checkpoint is ahead of the markers the log kept** (a crash lost an unflushed tail holding the marker). The view must be refolded, never judged from a state that already contains verdicts the index has lost.
   - Test: `CheckedWritesVerdictRestartRegression.CheckpointAheadOfLocalMarkersIsDiscardedAndRefoldedRegression` (Task 6).
2. **A batch whose every record is rejected**, read by a raw, group, filtered or gateway-push consumer. No empty frame, the cursor moves past it, and the next batch is delivered.
   - Test: `CheckedWritesConsumerRegression.FullyRejectedBatchIsSkippedOnEveryPushPathRegression` (Task 7).
3. **A lagging replica that holds a rejected record but not yet its marker, while the leader compacts.** The marker must stay until `markerRemovalBound` passes it. Otherwise that replica, once leader, would deliver the record.
   - Test: `CheckedWritesVerdictCleanerRegression.MarkerKeptUntilEveryReplicaFetchedItRegression` (Task 8).
4. **A marker removed after its records were compacted, then a new leader's scan.** Coverage comes from the later marker, with no stall and no re-judging of gone records.
   - Test: `CheckedWritesVerdictCleanerRegression.RemovedMarkerNeverStallsCoverageRegression` (Task 8).
5. **A 4096-record `View` batch that is all rejected, pipelined behind a smaller one on the same partition.** It is acked through `offer`, in order, never dropped, even when the arena is full.
   - Tests: `CheckedWritesVerdictAckRegression.FourThousandRejectionsAckIsOfferedRegression` and `…ArenaFullDefersInPartitionOrderRegression` (Task 9).

---

## Interaction matrix

### A. The shared worker-level matrix (`rune_checked_writes_matrix_tests`, `CheckedWritesMatrix/Cell.*`, Task 10)

Built on `CheckedWritesRig.h` (one real shard-mode `Worker`, a disk `Log` behind a two-replica `PartitionReplica`, the ack sink, an injected clock). The topic is `verdictTopicDescriptor()` = `{id uint64 key, v int64, tag uint8}`.

**Ops (Part 04 range 0–19):**

| Id | Op | What it writes |
|---|---|---|
| 0 | `AcceptedWrite` | 3 live records with tags 1, 2, 1 |
| 1 | `RejectedWrite` | 3 live records with tags 1, 9, 1 under `TagEquals{9}` |
| 2 | `RejectedOverAccepted` | `id 5 tag 1`, then `id 5 tag 9` (rejected): the rejected record is the key's latest |
| 3 | `CapRace` | 4 new ids under `CapLiveKeys{2}`: ids 3 and 4 are rejected |
| 4 | `DeleteFreesCap` | `CapLiveKeys{2}`: ids 1 and 2, a tombstone of 1, then id 3 (accepted) |

**Entities (Part 04 range 0–19):**

| Id | Entity | Observer |
|---|---|---|
| 0 | `RawSubscription` | a read_uncommitted subscription from the cursor at write time |
| 1 | `GroupConsumer` | a group subscription (read_committed carrier) |
| 2 | `FilteredSubscription` | a filtered subscription on `tag = 1` (Phase 2c) |
| 3 | `ReplayFromStart` | a subscription from offset 0 created after the writes |
| 4 | `OtherViews` | a projection `{id → v, tag}` and a `count(*)` group by `tag`, both non-verdict |
| 5 | `TwoVerdictViews` | a second `TestReject{TagEquals{8}}`; one record rejected by each |
| 6 | `ViewAckWriter` | an idempotent v12 producer with `All \| View` |
| 7 | `OldWriters` | the same records from a v11 session, `acks=leader`, `acks=none` and a non-idempotent `acks=all` producer |

**Timings:** the 14 §15 values (`Single … DropDuringWrites`) plus the appended `RetryAfterWindow` (R04e), in which the idempotent producer sends 33 further batches and then re-sends the op's batch. In the rig:
- `RetryBeforeJudge` / `RetryAfterJudge`: the duplicate's ack (for `ViewAckWriter`) equals the original's: the same base offset, trailer and rejected indexes, released only once `W > lastOffset`.
- `FailoverAppendHw`: append, then `failover()` before `followerFetch()`. The tail is cut, the `View` ack is `NotLeader`, the writer re-sends.
- `FailoverHwFold`: HW passed, `failover()` before any driven fold.
- `FailoverFoldMarker`: judged, marker appended above the HW, then `failover()` (the marker is cut).
- `RestartDuringRebuild`: `restart()` after 2 of the rebuild's passes, then again.
- `CreateOnNonEmpty` / `ReplaceDuringWrites` / `DropDuringWrites`: the `TestReject` member is installed, replaced (`TagEquals{9}` → `TagEquals{1}`) or dropped between the op's second and third record.

**Not applicable (top-down; the first rule matching names the reason):**
1. `OldWriters × {RetryBeforeJudge, RetryAfterJudge, RetryAfterWindow}`: these writers make no idempotent retry (15 cells).

**Cell count:** 5 × 8 × 15 = 600. 585 are applicable and 15 skipped, pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 04).

**`expect04(cell)`, top-down.** The verdicts themselves always come from `CheckedWritesModel` (the reference judge, Task 10). `expect04` names what the cell checks:
0. `RetryAfterWindow × ViewAckWriter` → the retry's answer is `DuplicateSequence` (21) with no trailer (54 B), `verdictUnknownDuplicates == 1`. Under the other entities the retry is stored once and changes nothing.
1. `FailoverAppendHw` → the first `View` ack is `NotLeader`; the re-sent batch's ack (for `ViewAckWriter`) carries the model's verdicts.
2. `ViewAckWriter` → one OK ack, 54 + 2 + 8n bytes, whose trailer is exactly the model's rejections `(offsetDelta, 51, constraintIdOf(objectId, 1))` in ascending order.
3. `OldWriters` → the v11 session's frame is 46 B with no trailer; `acks=none` gets nothing; the others get their 03 frames.
4. Reader entities → the delivered records equal the model's accepted records, in offset order, with gaps where rejected.
5. `OtherViews`, `TwoVerdictViews` → each table equals a fresh refold of the model's accepted records.

**Every applicable cell asserts** (after `driveUntilJudged(LEO)` and `compact()`):
- (a) `rejected` entries equal the model's verdicts;
- (b) markers in the log tile `[firstChange + 1, judgedThrough)` with no gap, and each entry agrees with the index;
- (c) no rejected offset was delivered or is present in any view;
- (d) every view equals a fresh refold of the accepted records;
- (e) after `compact()` every key's latest record in the log is the model's latest **accepted** record (or absent when it was deleted);
- (f) `rejected.mismatches() == 0`;
- (g) the cleaner never cleaned an offset ≥ the published W bound.

### B. Part 03's `View` ack matrix, folded in (R03d, Task 10)

Ops 120 `ViewAckLV` (03's `LV`), 121 `ViewAckAV` (`AV`). Entities 120 `AckNoViews` (`NoViews`), 121 `AckOneView`, 122 `AckTwoViewsLate`, 123 `AckTransientSource`. Timings `Single`, `BackToBack` (03's `Pipelined8`), `RetryBeforeJudge` (`DupBeforeFold`), `RetryAfterJudge` (`DupAfterFold`), and the six appended timings. `expect03` moves unchanged into the shared header. The count pin row is 03's: 78 applicable and 2 skipped, now in the shared pin; 03's own `CheckedWritesViewAckMatrixCountRegression.CellCountsArePinned` is deleted. `CheckedWritesViewAckMatrixTest.cpp` now takes its cells from `cellsFor(Part::P03)`.

### C. The model check (`CheckedWritesModelRegression`, `rune_checked_writes_matrix_tests`, Task 11)

`CheckedWritesModelRegression.SeededRigMatchesTheModel` runs seeds 1–200 × 250 steps on `CheckedWritesRig`. `CheckedWritesModelRegression.SameSeedSameTrace` re-runs seeds 1–8 and compares the step trace hash. One action per step, drawn from the seed's `std::mt19937_64`:

| G8 condition (spec §2) | Actions |
|---|---|
| concurrent writers | 3 producer ids (2 idempotent, 1 not) interleaving batches; `acks` ∈ {0, 1, 3, 2} |
| collisions inside one batch | batches of 1–12 records over 6 ids, so ids repeat inside a batch, under `CapLiveKeys{3}` + `TagEquals{9}` |
| transactions that commit or abort | an open transaction of producer 77 committed or aborted later (votes are Part 11) |
| ack timeouts and retries | `advanceClock(ackTimeoutNs)`; an idempotent re-send of an earlier batch before and after `driveUntilJudged` |
| leader failover | `failover()` (cut above HW, fresh memory) and `demotePromote()` (memory kept) |
| restart | `restart()`, including mid-rebuild |
| log truncation | `truncateTo(HW)` while the tail holds data and markers |
| compaction | `compact()` |
| constraint changes | install, replace or drop a `TestReject` member |

After every step:
- **`AckedVisibleUnlessRejected`:** every OK `View` ack's non-rejected records are in every view once the fold passed them; every rejected one is absent.
- **`NoRejectedDeliveredOrFolded`:** nothing the model rejects was delivered to the step's subscription or folded.
- **`SameVerdictEveryLeaderAndRefold`:** every verdict ever reported (acks, index, markers, before and after each fault) equals the model's.
- **`ViewEqualsFreshRefold`:** each view equals the model's refold of accepted records.
- **`CompactionKeepsAcceptedValue`:** after each `compact()`, every key's latest record is the latest accepted one.
- **`MarkersTile`:** markers in the log tile their range, with no gap below `judgedThrough`.

A failure prints `seed=<n> step=<k> action=<name>`.

### D. The cluster stress (`rune_checked_writes_stress_tests`, `CheckedWritesStressRegression`, Task 11)

`CheckedWritesStressDriver` runs on `ThreeNodes` (default shard mode, `configTweak`: 64 KiB segments, cleaner every 50 ms, `RUNE_VERDICT_INTERVAL_MS=20`). It uses 3 real `Client`s writing `All | View` to one `TestReject` source (`CapLiveKeys{8}` + `TagEquals{9}`), a raw and a group consumer from offset 0, and one ordinary projection view.
- **Faults:** leader crash and restart, a follower restart, a restart mid-rebuild, cleaner passes, and `TestReject` replace and drop. `TestReject` is installed on every node's workers through `BrokerNode::onWorkerThreadForTest`, and re-installed after each restart.
- **Run:** `CheckedWritesStressRegression.SeededClusterHoldsTheInvariants` runs seeds 1–12 × 300 steps.
- **Invariants:** the five Part 04 invariants of the overview (`AckedVisibleUnlessRejected`, `NoRejectedDeliveredOrFolded`, `SameVerdictEveryLeaderAndRefold`, `ViewEqualsFreshRefold`, `CompactionKeepsAcceptedValue`).
- **How they are checked:** markers are read from **every** node's log (`onOwnerThreadForTest`), and the model judges the committed log of the final leader. It also asserts `verdictUnknownDuplicates == 0` and `mismatches() == 0` on every node.
- **Failure output:** the seed is printed.
- **Extension points:** Parts 05, 06, 09 and 11 add their invariants (`NoIdTwiceNoReuse`, `AtMostTwelveLiveCharacters`, `LowerNameUnique`, `TxnAllOrNothingAndAtomicPerPartition`) through the driver's `registerInvariant(name, fn)`.

### E. Named cells outside the matrix

| §15 cell | Test (task) |
|---|---|
| verdict lag while a constraint view builds: delivery waits, writes don't | `CheckedWritesVerdictWorker.DeliveryWaitsDuringABuildWritesDoNot` (6, the judge and writes), `CheckedWritesConsumer.DeliveryWaitsDuringABuildWritesDoNot` (7, delivery) |
| failover right after a change record: same set on every offset | `CheckedWritesVerdictWorkerRegression.FailoverRightAfterAChangeRecordJudgesWithTheSameSetRegression` (6) |
| a new leader without the install waits | `CheckedWritesVerdictWorkerRegression.LeaderWithoutTheInstallWaitsRegression` (6, the judge), `CheckedWritesConsumerRegression.LeaderWithoutTheInstallDeliversNothingAboveTheJudgeRegression` (7), `CheckedWritesVerdictAckRegression.LeaderWithoutTheInstallParksTheViewAckRegression` (9) |
| refold below W doesn't judge; a constraint changed since gives the same view | `CheckedWritesVerdictWorkerRegression.RefoldBelowWIgnoresALaterRuleRegression` (6) |
| a new leader judging past W reaches the same verdicts and writes the markers | `CheckedWritesVerdictWorkerRegression.NewLeaderRejudgesAboveWAndRemarksRegression` (6) |
| the cleaner never compacts above W | `CheckedWritesVerdictCleanerRegression.NeverCleansAtOrAboveWRegression` (8) |
| several verdict views, one rejects: no view changes | `CheckedWritesVerdictSource.OneOfTwoRejectsNoViewChanges` (5) |
| plain writes from non-idempotent / `acks=leader` / `acks=none` / v11 clients | `CheckedWritesBrokerRegression.OldWritersOnAVerdictSourceNeverSeeRejectedRecordsShardRegression` (12) |
| a coalesced batch with one bad record | `CheckedWritesBrokerRegression.CoalescedBatchWithOneBadRecordShardRegression` (12) |

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `broker/libs/view/include/rune/view/ViewKey.h` | C | `kMaxViewKeyBytes`, `ViewKey` with `fromU64` / `asU64` (C9.1, R07d; Part 07 extends it) |
| `broker/libs/view/include/rune/view/RecordFold.h` | C | `SourceRecord`, `PrepareStatus`, `ProspectOp`, `Prospect`, `RowOverlay` (C9.3) |
| `broker/libs/view/include/rune/view/Verdict.h` | C | `Verdict`, `constraintIdOf`, `VerdictViewKind` (C9.4) |
| `broker/libs/view/include/rune/view/FoldSkip.h` | M | `rejected` member, `isRejectedRecord` |
| `broker/libs/view/{include/rune/view/ViewFold.h,src/ViewFold.cpp}` | M | `ProjectionFold`, `prepareProjection` / `applyProjection`; `foldBatch` as the prepare-apply loop with the rejected skip |
| `broker/libs/view/{include/rune/view/AggFold.h,src/AggFold.cpp}` | M | `AggStep<Store>`, `DirectStore`, `OverlayStore`, `AggregateFold`, `prepareAggregate` / `applyAggregate` |
| `broker/libs/view/tests/view/LegacyFoldReference.{h,cpp}` | C | a frozen copy of the post-01 `foldBatch` / `foldAggregateBatch` (namespace `legacy_fold`) |
| `broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp` | C | prospect and identity tests |
| `broker/libs/view/CMakeLists.txt` | M | new test sources; `rune_view` links `rune_storage` through `rune_transaction` (already) |
| `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` | C | C10 header |
| `broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h` | C | C11 index |
| `broker/libs/storage/include/rune/storage/VerdictCleanState.h` | C | the cleaner state |
| `broker/libs/storage/{include/rune/storage/LogCleaner.h,src/LogCleaner.cpp}`, `OffsetMap.h`, `SkimpyOffsetMap.h` | M | the pre-pass feed, map skip, rewrite drop, marker removal, `setVerdictBounds` |
| `broker/libs/storage/tests/storage/CheckedWrites{ControlRecord,RejectedIndex,VerdictCleaner}Test.cpp`, `broker/libs/storage/CMakeLists.txt` | C/M | storage tests in `rune_storage_tests` |
| `broker/libs/transaction/src/{ControlMarker.cpp,ReadCommittedView.cpp}` | M | type hardening (C10) |
| `broker/libs/replication/include/rune/replication/PartitionReplica.h:866-893` | M | `cleanerVerdictBound` and `cleanerVerdictMarkerEnd` |
| `broker/libs/node/include/rune/node/LogCleanerAgent.h:455` | M | min with the verdict bound; pass the bounds to `LogCleaner` |
| `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp` | C | `VerdictSource`, `ConstraintSet*`, `MarkerLedger`, `VerdictSourceStats`, `TestRejectRule`, the verdict feed |
| `broker/libs/node/include/rune/node/WriteState.h` | M | the C7 members of 04 and the three methods |
| `broker/libs/node/include/rune/node/RcState.h` | M | `RcVerdictHooks` |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | `verdictSources_`, the driveFolds order, retiring templates, the `TestReject` seam |
| `broker/libs/node/include/rune/node/Worker.h` | M | hooks install, `rebindVerdictSources_`, `verdictBound_`, push cuts, the `View` release condition and trailer, cleaner publish, maintenance, test seams |
| `broker/libs/node/include/rune/node/CheckedWriteHook.h`, `src/CheckedWriteHook.cpp` | M | `topicHookFactsOf` sets `verdictSource` / `constrainedSource` |
| `broker/libs/node/include/rune/node/filter/FilterGroup.h` | M | `rejected()` in the source concept; `collect_` and `send_` |
| `broker/libs/node/include/rune/node/SessionAckSink.h` | M | `VerdictAckArena`, trailer encode, `offer` above `maxPayloadLength` |
| `broker/libs/node/{include/rune/node/NodeConfig.h,src/NodeConfig.cpp,src/BrokerNode.cpp}` | M | `verdictIntervalMs` |
| `broker/libs/node/tests/support/CheckedWritesRig.h`, `CheckedWritesMatrixExpectations.h`, `CheckedWritesModel.h`, `CheckedWritesStressDriver.h` | C | shared harness |
| `broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp`, `CheckedWritesTimelineTest.cpp` | C | unit tests in `rune_checked_writes_tests` |
| `broker/libs/node/tests/node/CheckedWritesVerdictWorkerTest.cpp`, `CheckedWritesConsumerTest.cpp`, `CheckedWritesVerdictAckTest.cpp` | C | rig tests in the new `rune_checked_writes_verdict_tests` |
| `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp`, `CheckedWritesModelTest.cpp` | C | `rune_checked_writes_matrix_tests` |
| `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp` | C | `rune_checked_writes_stress_tests` |
| `broker/libs/node/tests/node/CheckedWritesViewAckMatrixTest.cpp`, `tests/support/CheckedWritesAckRig.h` | M | 03's matrix on the shared cells |
| `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` | M | the judging / marker / cut / trailer alloc gate |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` | M | real-broker slice |
| `broker/libs/node/CMakeLists.txt` | M | new binaries and sources |
| `client-cpp/include/rune/client/agent/ClientAgent.h`, `client-cpp/src/agent/ClientAgent.cpp`, `client-cpp/src/Client.cpp`, `client-cpp/include/rune/client/producer/{ProducerHub.h,ProducerImpl.h}`, `client-cpp/src/producer/{ProducerHub.cpp,ProducerImpl.cpp}` | M | per-record verdicts |
| `client-cpp/tests/producer/ProducerVerdictsTest.cpp`, `client-cpp/CMakeLists.txt` | C/M | client tests in `rune_invoker_tests` |
| `broker/benchmark/{ViewFoldBench.cpp,WorkerProduceLatencyBench.cpp}` | M | `BM_ViewFoldVerdict/*`, the verdict-source consumer bench |
| `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `docs/operations.md`, `docs/configuration.md`, `client-cpp/CLAUDE.md` | M | docs this part ships |

---

## Phase 1 — The view engine and the storage building blocks

### Task 1: `ViewKey`, `Verdict`, `RecordFold` and the projection prepare/apply

**Files:**
- Create: `broker/libs/view/include/rune/view/ViewKey.h`, `Verdict.h`, `RecordFold.h`, `broker/libs/view/tests/view/LegacyFoldReference.h`, `LegacyFoldReference.cpp`, `CheckedWritesPrepareApplyTest.cpp`
- Modify: `broker/libs/view/include/rune/view/ViewFold.h:91-94`, `broker/libs/view/src/ViewFold.cpp:114-209` (the record loop), `broker/libs/view/CMakeLists.txt:21-27`

**Interfaces:**
- Consumes: `FoldSkip`, `isAbortedBatch`, `foldBatch(…, const FoldSkip&)` (Part 01); `rune::protocol::forEachRecordSpan`, `RecordSpan::key` (Part 02), `RecordSpan::{attributes, index}` (Part 03); `rune::protocol::record::intentOf` (Part 03).
- Produces (exact):
  ```cpp
  // ViewKey.h (C9.1)
  inline constexpr std::size_t kMaxViewKeyBytes = 128;
  struct ViewKey { std::array<uint8_t, kMaxViewKeyBytes> bytes{}; uint8_t len = 0;
      static ViewKey fromU64(uint64_t) noexcept;          // len = 8, the little-endian bytes of v
      std::span<const uint8_t> span() const noexcept;
      [[nodiscard]] uint64_t asU64() const noexcept; };   // R07d: requires len <= 8; little-endian of bytes[0..len); fromU64(v).asU64() == v
  // Verdict.h (C9.4, verbatim)
  struct Verdict { uint16_t code = 0; uint32_t constraintId = 0; [[nodiscard]] bool accepted() const noexcept { return code == 0; } };
  [[nodiscard]] constexpr uint32_t constraintIdOf(uint64_t viewObjectId, uint16_t ordinal) noexcept;
  enum class VerdictViewKind : uint8_t { KeyIndex = 1, Unique = 2, Check = 3, TestReject = 200 };
  // RecordFold.h (C9.3)
  struct SourceRecord { std::string_view key; std::string_view value; int64_t offset = 0; uint8_t intent = 0;
                        const rune::codec::Descriptor* writer = nullptr; };
  enum class PrepareStatus : uint8_t { Ok, Malformed, CapacityExhausted };
  enum class ProspectOp : uint8_t { None, Upsert, Erase, Move };
  struct ProspectRow { ViewKey key; uint8_t* image = nullptr; bool removed = false; bool existed = false; };
  struct Prospect {                       // reserved once per ViewInstance at install; never grows
      ProspectOp op = ProspectOp::None; uint8_t rowCount = 0; std::array<ProspectRow, 2> rows{};
      std::vector<uint8_t> scratch;       // 2 * rowLen, sized at install (cold)
      RowOverlay overlay;                 // aggregate state images: an empty stub here (reserve/clear no-ops), filled by Task 2
      void reserve(uint32_t rowLen, const AggLayout* agg);   // cold
      void clear() noexcept;
  };
  // ViewFold.h
  struct ProjectionFold { const CompiledViewPlan& plan; const RowLayout& layout; VersionDescriptorCache& vdc;
                          ViewTable& table; PrimType keyType; };
  [[nodiscard]] PrepareStatus prepareProjection(ProjectionFold&, const SourceRecord&, Prospect&) noexcept;
  void applyProjection(ProjectionFold&, const SourceRecord&, const Prospect&) noexcept;
  ```
  `constraintIdOf` returns `(static_cast<uint32_t>(objectId) << 16) | ordinal`.

- [ ] **Step 1: Freeze the legacy fold.** Copy the post-01 bodies of `foldBatch` (`ViewFold.cpp`) and both `foldAggregateBatch` overloads (`AggFold.cpp`, together with the anonymous-namespace helpers they need) verbatim into `LegacyFoldReference.cpp`, namespace `legacy_fold`, with the same signatures in `LegacyFoldReference.h`. Add both to `rune_view_tests`. The file's header comment says: "frozen copy of the pre-Part-04 fold (base commit <sha>); never edit; the identity tests compare against it".

- [ ] **Step 2: Write the failing tests** (`CheckedWritesPrepareApplyTest.cpp`). The fixture copies `charSourceV1()`, `hpView()`, `encodeCharacter` from `ViewFoldTest.cpp:15-89`.
  ```cpp
  TEST(CheckedWritesPrepareApply, ProjectionPrepareWritesNothing) {
      // table holds id 1 hp 10; prepare(id 1 hp 20) -> Ok, op Upsert, rows[0].existed, image hp == 20;
      // table.get(1) hp still 10, table.size() == 1, dirtyRids() unchanged
  }
  TEST(CheckedWritesPrepareApply, ProjectionApplyEqualsTheImage)        // apply -> table row bytes == image bytes (memcmp rowLen)
  TEST(CheckedWritesPrepareApply, TombstonePreparesErase)               // prepare(tomb 1) -> op Erase, rows[0].removed; apply -> get(1)==nullptr
  TEST(CheckedWritesPrepareApply, MalformedWireKeyPreparesMalformed)    // 7-byte key -> Malformed, apply is a no-op, table unchanged
  TEST(CheckedWritesPrepareApply, ConstraintIdPacksObjectIdAndOrdinal)  // constraintIdOf(5, 1) == 0x00050001u; (0xABCD, 0) == 0xABCD0000u
  TEST(CheckedWritesPrepareApplyRegression, ProjectionFoldIsByteIdenticalToLegacyRegression)
      // seeds 1-200: 64 batches of 1-8 records over 16 keys, 15 % tombstones, control batches and
      // aborted transactions (Part 01's TxnLog), pass caps {1, 3, unbounded}: foldBatch == legacy_fold::foldBatch
      // (table rows byte-equal by key, FoldStats equal, cursor equal)
  TEST(CheckedWritesPrepareApply, ViewKeyFromU64RoundTrips)             // R07d: fromU64(v).len == 8 and asU64() == v for 0, 1, 0x0102, 2^63, UINT64_MAX
  ```
- [ ] **Step 3: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `rune/view/RecordFold.h` not found.
- [ ] **Step 4: Implement.**
  - `prepareProjection` decodes the wire key (`decodeWireKey`, `ViewFold.cpp:60-110`, via `ProjectionFold::keyType`). On failure it returns `Malformed`.
  - A tombstone prepares `Erase`.
  - Otherwise the row image goes into `scratch[0, rowLen)`: start from `table.get(k)` bytes when the row exists, else zero, then `writeSlot` every `plan.output` field from `rec.writer` (or `vdc.materializedFor(version)` when `writer == nullptr`). The op is `Upsert`.
  - `applyProjection` does `memcpy(table.put(k), image, rowLen)` or `table.erase(k)`.
  - **One step, two stores (P4-13).** The per-record body becomes `template <class Store> PrepareStatus projectionStep(…)`. `DirectStore` writes the slots straight into `table.put(k)`, exactly as today. `prepareProjection` runs the same step against a row-image store over `Prospect::scratch`.
  - `foldBatch` keeps its signature. Its record loop becomes `forEachRecordSpan` + `projectionStep<DirectStore>` (Task 4 adds the rejected skip). The batch, bound, cursor and corrupt-tail handling stay as they are, so the ordinary fold never copies a row.
  - `RecordFold.h` declares `struct RowOverlay` as an empty stub with no-op `reserve` / `clear` and `full() == false`, so `Prospect` compiles; Task 2 gives it its slots.
- [ ] **Step 5: Run.** `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'CheckedWritesPrepareApply|ViewFold|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS. Every pre-existing `ViewFold*` test and Part 01's fold matrix are unchanged.
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/ViewKey.h broker/libs/view/include/rune/view/Verdict.h \
          broker/libs/view/include/rune/view/RecordFold.h broker/libs/view/include/rune/view/ViewFold.h \
          broker/libs/view/src/ViewFold.cpp broker/libs/view/CMakeLists.txt \
          broker/libs/view/tests/view/LegacyFoldReference.h broker/libs/view/tests/view/LegacyFoldReference.cpp \
          broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp
  git commit -m "view: ViewKey, Verdict, SourceRecord/Prospect; projection fold as prepare-then-apply (C9)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 2: The aggregate prepare/apply (one template, two stores)

**Files:**
- Modify: `broker/libs/view/include/rune/view/AggFold.h:22-33`, `broker/libs/view/src/AggFold.cpp:79-547` (`Ctx` → `AggStep<Store>`), `:550-660`
- Modify: `broker/libs/view/include/rune/view/RecordFold.h` (`RowOverlay`: the Task 1 stub gets its slots)
- Test: `broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp`

**Interfaces:**
- Consumes: Task 1.
- Produces:
  ```cpp
  // RecordFold.h
  struct RowOverlay {                            // fixed: never allocates after reserve
      static constexpr std::size_t kSlotsPerTable = 16;
      enum class Table : uint8_t { View, Contributors, Groups, Blocks };
      void reserve(const RowLayout& view, const AggLayout& agg);   // cold
      void clear() noexcept;
      [[nodiscard]] bool full() const noexcept;
  };
  // AggFold.h
  struct AggregateFold { const CompiledViewPlan& plan; const RowLayout& layout; const AggLayout& agg;
                         VersionDescriptorCache& vdc; ViewTable& view; ViewTable& contributors;
                         ViewTable& groups; ViewTable* blocks; };
  [[nodiscard]] PrepareStatus prepareAggregate(AggregateFold&, const SourceRecord&, Prospect&) noexcept;
  void applyAggregate(AggregateFold&, const SourceRecord&, const Prospect&) noexcept;
  ```
  `Prospect::rows` holds the new group's view-row image (rows[0]) and, for a `Move`, the old group's (rows[1]; `removed` when its count reaches 0). The overlay holds every other touched row.

- [ ] **Step 1: Write the failing tests.** The fixture is `orderSource()`, `userTotals()`, `orderV1()` and `userMax()` (blocks) from `AggFoldTest.cpp:26-76`.
  ```cpp
  TEST(CheckedWritesPrepareApply, AggregatePrepareWritesNothing)
      // u100 n=1 total=10; prepare(o2 u100 30) -> op Upsert, rows[0] image n=2 total=40;
      // view/contributors/groups/blocks byte-identical to before (snapshot all four tables, compare)
  TEST(CheckedWritesPrepareApply, AggregateMoveProspectsBothGroups)
      // o1 u100 -> u200: op Move, rows[0] = u200 n=1, rows[1] = u100 removed (its count reaches 0)
  TEST(CheckedWritesPrepareApply, MaxRetractionRescansReadOnly)
      // userMax: o1 10, o2 30 under u100; prepare(tomb o2) -> rows[0] mx == 10, aggRescans not applied to the
      // instance until apply; blocks->size() unchanged after prepare
  TEST(CheckedWritesPrepareApplyRegression, AggregateFoldIsByteIdenticalToLegacyRegression)
      // seeds 1-200 x {userTotals, userMax, Last-only}: three-way: legacy_fold::foldAggregateBatch ==
      // foldAggregateBatch (DirectStore) == a loop of prepareAggregate+applyAggregate; all four tables byte-equal,
      // FoldStats equal
  TEST(CheckedWritesPrepareApply, OverlayOverflowIsCapacityExhaustedNeverAVerdict)
      // a test-only RowOverlay with 1 slot per table: a Move prepares CapacityExhausted; tables unchanged
  ```
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `no member named 'prepareAggregate'`.
- [ ] **Step 3: Implement.**
  - Turn `Ctx` (`AggFold.cpp:79-547`) into `template <class Store> struct AggStep`. Every `view.` / `contributors.` / `groups.` / `blocks->` access goes through `store.put(T, k)`, `store.get(T, k)` and `store.erase(T, k)`, plus `store.size(T)` where it is read.
  - Move the per-record body of `foldAggregateBatch` (`:575-655`) into `AggStep::step(const SourceRecord&)`.
  - **`DirectStore`** forwards to the tables.
  - **`OverlayStore`:**
    - `get` returns the overlay copy when present, `nullptr` when it is erased in the overlay, else the table row.
    - `put` copies the table row (or zero) into a free slot once, then returns the slot.
    - `erase` marks the slot erased.
    - A full table kind sets `full()`, and `prepareAggregate` returns `CapacityExhausted`.
  - `applyAggregate` commits the overlay in the order the direct path writes: contributors, groups, blocks, view. Each slot is `put` with `memcpy` or `erase`, and the step's `FoldStats` deltas are added.
  - `foldAggregateBatch` keeps both signatures and runs `AggStep<DirectStore>` per record (Task 4 adds the rejected skip).
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesPrepareApply|AggFold|AggRetractable|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS. Mutation check (do it, then revert): make `OverlayStore::get` ignore the overlay. `AggregateMoveProspectsBothGroups` and the three-way test must turn red.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/AggFold.h broker/libs/view/src/AggFold.cpp \
          broker/libs/view/include/rune/view/RecordFold.h broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp
  git commit -m "view: aggregate step templated over a direct or overlay store; prepareAggregate/applyAggregate (C9.3)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 3: Control records (C10) and every reader's hardening

**Files:**
- Create: `broker/libs/storage/include/rune/storage/ControlRecordLayout.h`, `broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp`
- Modify: `broker/libs/transaction/src/ControlMarker.cpp:287-296` (accept only control types 0 and 1, and only `producerId >= 0`), `broker/libs/transaction/src/ReadCommittedView.cpp:315-324` (`RcBuilder::step`: a control batch with `producerId() < 0` is `Outcome::Ignored` before `extract`), `broker/libs/storage/CMakeLists.txt:22` (test source)
- Test: also `broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp` (the readers sweep; created here, extended in Task 5)

**Interfaces:**
- Produces (`namespace rune::storage::control_record`, header-only, no allocation):
  ```cpp
  inline constexpr int16_t kReservation = 16, kVerdictMarker = 17, kConstraintSetChange = 18, kTxnJudge = 19;
  inline constexpr int kUnparseable = -1;
  inline constexpr uint8_t kPayloadVersion = 1;
  inline constexpr std::size_t kMaxMarkerEntries = 4096;
  struct MarkerEntry { uint32_t offsetDelta; uint16_t code; uint32_t constraintId; };
  struct MarkerHeader { int64_t fromOffset; int64_t finalThrough; uint32_t constraintSetId; uint32_t count; };
  struct ChangeMember { uint64_t viewObjectId; uint8_t kind; uint64_t planFingerprint; };
  [[nodiscard]] std::size_t markerBatchBytes(uint32_t count) noexcept;
  [[nodiscard]] std::size_t changeBatchBytes(uint16_t count) noexcept;
  std::size_t buildVerdictMarker(std::span<uint8_t> out, const MarkerHeader&, std::span<const MarkerEntry>) noexcept;   // 0 = out too small / count mismatch
  std::size_t buildConstraintSetChange(std::span<uint8_t> out, uint32_t constraintSetId, std::span<const ChangeMember>) noexcept;
  [[nodiscard]] int recordType(const uint8_t* batch, std::size_t len) noexcept;
  [[nodiscard]] std::optional<MarkerHeader> parseMarkerHeader(const uint8_t* batch, std::size_t len) noexcept;
  template <class Fn> bool forEachMarkerEntry(const uint8_t* batch, std::size_t len, Fn&& fn) noexcept;   // fn(int64_t offset, uint16_t code, uint32_t cid)
  [[nodiscard]] bool parseConstraintSetChange(const uint8_t* batch, std::size_t len, uint32_t& setId,
                                              std::span<ChangeMember> out, uint16_t& count) noexcept;
  ```
  - **Batch form (C10):** `attributes = kAttrControl` only (0x0020), `producerId = −1`, `producerEpoch = −1`, `baseSequence = −1`, one record in standard Kafka framing (length varint, `offsetDelta 0`), key = int16 LE type, value = `[version u8 = 1][payload]` exactly as C10 lists, all LE, CRC-32C sealed.
  - **`recordType`** requires every one of those properties and a known type (16–19), else `kUnparseable`.
  - **Builders** write `baseOffset = 0`, which `appendControlBatch` restamps (`PartitionReplica.h:592`, CRC-safe).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesControlRecordTest.cpp`:
    - `CheckedWritesControlRecord.MarkerRoundTrips`: from 100, through 140, set 3, entries `{(5, 51, 0x00070001), (12, 52, 0)}` → `recordType == 17`, the header equal, the entries at offsets 105 and 112.
    - `CheckedWritesControlRecord.ChangeRoundTrips`: set 2, members `{(0, KeyIndex, 0), (7, TestReject, 0xF00D)}` → type 18, both members equal.
    - `CheckedWritesControlRecord.MaxMarkerFitsAndOneMoreIsRefused`: 4096 entries build; `buildVerdictMarker` with `count = 4097` returns 0.
    - `CheckedWritesControlRecord.RecordTypeRefusesLookalikes`: a txn COMMIT marker (`ControlMarker::build`) → `kUnparseable`; transactional bit set → `kUnparseable`; `producerId 0` → `kUnparseable`; value version 2 → `kUnparseable`; type 20 → `kUnparseable`; a flipped CRC byte → `kUnparseable`.
  - `CheckedWritesVerdictSourceTest.cpp`, **`CheckedWritesControlRecordRegression.EveryLogReaderSkipsTypes16To19Regression`** (Review Focus 2 of the overview). For a batch of each of the 4 types (types 16 and 19 built with a two-line test builder over the same layout), assert each reader in turn:
    - **(1)** `RcBuilder::step` returns `Outcome::Ignored` and leaves the tracker and `AbortedTxnIndex` empty.
    - **(2)** `ControlMarker::extract` returns false.
    - **(3)** `LogCleaner` `scanTxnState` over a one-segment log holding the batch plus a committed txn: `TxnCleanState` sees no extra marker and `cleanLimit` is unchanged.
    - **(4)** `RcState` advance over a log holding it builds Ready with no open transaction.
    - **(5)** `foldBatch` and `foldAggregateBatch` count `skippedControl == 1` and touch no table.
    - **(6)** The unfiltered push needs an Aeron shard `Worker`, which `rune_checked_writes_tests` does not link, so it is Task 7's `CheckedWritesConsumer.ControlRecordsSixteenToNineteenAreNeverPushed` in `rune_checked_writes_verdict_tests`.
    - **(7)** `rune::protocol::forEachRecordSpan` parses it.
    - **(8)** The client `DataCodec` record walk (`client-cpp/include/rune/client/codec/DataCodec.h:341`) skips it as control.
    - **(9)** The PSM rebuild (`feedProducerState_`, `RcState.h:657-662`) records nothing for `producerId −1`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_storage_tests rune_checked_writes_tests`. Expected: `ControlRecordLayout.h` not found.
- [ ] **Step 3: Implement** the header and the two hardening edits. `extract` now returns false for a key type outside {0, 1} or a `producerId < 0`. `RcBuilder::step` returns `Ignored` for a `producerId() < 0` control batch before calling `extract`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesControlRecord|ControlMarker|ReadCommitted|RcState|LogCleaner' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/storage/include/rune/storage/ControlRecordLayout.h broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp \
          broker/libs/storage/CMakeLists.txt broker/libs/transaction/src/ControlMarker.cpp broker/libs/transaction/src/ReadCommittedView.cpp \
          broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "storage: control record types 16-19 (C10); txn readers accept only types 0/1 with producerId >= 0" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 4: `RejectedOffsetIndex` and the fold's rejected skip

**Files:**
- Create: `broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h`, `broker/libs/storage/tests/storage/CheckedWritesRejectedIndexTest.cpp`
- Modify: `broker/libs/view/include/rune/view/FoldSkip.h` (`rejected`, `isRejectedRecord`), `ViewFold.h` (`FoldStats::skippedRejected`), `ViewFold.cpp`, `AggFold.cpp` (the per-record skip from Tasks 1 and 2)
- Test: `broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp`

**Interfaces:**
- Produces: the C11 class verbatim, plus contract addition 1. The storage is a sorted `std::vector<Entry>` reserved at `reserve()`.
  - `add` appends when `offset > back().offset`. A non-ascending offset returns false and counts it. A full vector (`size() == capacity()`) returns false.
  - `onMarker(from, ft, entries)`:
    - inserts each absent entry in order (cold path: a scan merge);
    - counts `mismatches` for an entry present with another code or constraintId, and for an existing entry in `[from, ft)` that the marker lacks;
    - raises `coveredThrough` to `ft`.
  - `isRejected` / `find` / `nextRejectedAtOrAfter` are `std::lower_bound` lookups.
  - `forEachIn` iterates `[from, to)`. `truncateFrom` and `dropBelow` erase a suffix or prefix (cold).
  - `watermark` / `setWatermark` hold W.
- Produces (`FoldSkip.h`): `const rune::storage::RejectedOffsetIndex* rejected = nullptr;` appended (C9.2), and `isRejectedRecord(skip, offset)` = `skip.rejected != nullptr && skip.rejected->size() != 0 && skip.rejected->isRejected(offset)`.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesRejectedIndex.AddIsAscendingOnly`: add 5, 9 → true; add 7 → false; `size() == 2`.
  - `CheckedWritesRejectedIndex.LookupsAndRanges`: entries 5, 9, 20 → `isRejected(9)`, `!isRejected(10)`, `nextRejectedAtOrAfter(10) == 20`, `nextRejectedAtOrAfter(21) == INT64_MAX`, and `forEachIn(6, 20)` visits exactly {9}.
  - `CheckedWritesRejectedIndex.MarkerMergeIsIdempotent`: `onMarker(0, 30, {5, 9})` twice → size 2, `mismatches() == 0`, `coveredThrough() == 30`.
  - `CheckedWritesRejectedIndexRegression.MarkerDisagreementCountsAMismatchRegression`:
    - own `add(9, 51, 1)` then `onMarker(0, 30, {(9, 52, 0)})` → `mismatches() == 1`, the entry is unchanged;
    - `onMarker(0, 30, {})` over an own entry → `mismatches() == 2`.
  - `CheckedWritesRejectedIndex.FullIsFalseNeverOverwrites`: reserve 2; the third add is false and `size() == 2`.
  - `CheckedWritesRejectedIndex.GrowKeepsEntries`: reserve 2, add 2, `grow(8)` → capacity 8, both entries kept, `add` true.
  - `CheckedWritesRejectedIndex.TruncateAndDropBelow`: entries {5, 9, 20}; `truncateFrom(9)` → {5}; `dropBelow(6)` → {}.
  - `CheckedWritesRejectedIndex.CountAtOrAfter`: {5, 9, 20} → `countAtOrAfter(9) == 2`.
  - `CheckedWritesRejectedIndex.SteadyAddAllocatesNothing`: under `rune::testing::AllocWindow` (`broker/libs/node/tests/support/AllocWindow.h`, linked into the storage test only), 1,000 ascending adds into a reserved index → 0 allocations.
  - `CheckedWritesPrepareApplyRegression.FoldSkipsRejectedRecordsInBothFoldsRegression`: a log of 6 records with the index rejecting offsets 2 and 4 → a projection has no row from 2 or 4, and a count aggregate counts 4. `skippedRejected == 2` in each, and the cursor is the log end.
  - `CheckedWritesPrepareApplyRegression.RejectedSkipHonoursThePassBoundRegression`: pass bounds 1..6 one at a time → `Σ skippedRejected == 2`, never past the bound.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_storage_tests rune_view_tests`. Expected: `RejectedOffsetIndex.h` not found.
- [ ] **Step 3: Implement** as specified. In both fold loops, a record with `isRejectedRecord(skip, offset)` is passed like a malformed record: counted in `FoldStats::skippedRejected` (appended), never prepared, and still bounded by the pass bound.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesRejectedIndex|CheckedWritesPrepareApply|ViewFold|AggFold' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h broker/libs/storage/tests/storage/CheckedWritesRejectedIndexTest.cpp \
          broker/libs/storage/CMakeLists.txt broker/libs/view/include/rune/view/FoldSkip.h broker/libs/view/include/rune/view/ViewFold.h \
          broker/libs/view/src/ViewFold.cpp broker/libs/view/src/AggFold.cpp broker/libs/view/tests/view/CheckedWritesPrepareApplyTest.cpp
  git commit -m "storage/view: RejectedOffsetIndex (C11) and FoldSkip.rejected in both folds" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint A (controller).**
  - `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure`
  - The same on `build/asan` (`ASAN_OPTIONS=detect_leaks=0` on macOS x86_64 under Rosetta, then `leaks(1)` on `rune_view_tests`, `rune_storage_tests`), `build/tsan` and `build/client-tsan`
  - `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure` (3 pass, unchanged)
  - Expected: 100 % pass and no sanitizer report. A failure is bisected and fixed with a `*Regression*` test; then only the failed and affected tests are rerun.

---

## Phase 2 — The broker pipeline

### Task 5: `VerdictSource`, the timeline and `TestReject` (unit level)

**Files:**
- Create: `broker/libs/node/include/rune/node/VerdictSource.h`, `broker/libs/node/src/VerdictSource.cpp` (add to `rune_node` in `broker/libs/node/CMakeLists.txt`), `broker/libs/node/tests/node/CheckedWritesTimelineTest.cpp`
- Modify: `broker/libs/node/include/rune/node/WriteState.h` (04's C7 members, contract addition 2), `broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp`
- Test: `rune_checked_writes_tests`

**Interfaces:**
- Consumes: `Prospect`, `prepareProjection` / `applyProjection`, `prepareAggregate` / `applyAggregate` (Tasks 1, 2); `RejectedOffsetIndex` (Task 4); `ControlRecordLayout.h` (Task 3); `ViewInstance` (`WorkerViewSet.h:191`); `FoldSkip` (Part 01).
- Produces (`namespace rune::node`; C9.5 verbatim plus contract additions 3–6):
  ```cpp
  struct VerdictSourceStats { uint64_t rejectionsConstraint = 0, markersWritten = 0, markerAppendRetries = 0,
      judgeWaitsForInstall = 0, verdictStalls = 0, verdictCheckpointsDiscarded = 0, verdictUnknownDuplicates = 0;
      struct PerConstraint { uint32_t constraintId = 0; uint64_t rejections = 0; };
      std::array<PerConstraint, 256> perConstraint{}; uint16_t perConstraintCount = 0; };
  class VerdictSource {
  public:
      static constexpr int kChangeDeferred = -1, kChangeUnchanged = -2;
      VerdictSource(int32_t srcTopicId, int32_t partition);
      [[nodiscard]] Verdict judge(const rune::view::SourceRecord&) noexcept;
      template <class ReadRangeFn>
      int driveJudge(ReadRangeFn&& readRange, int64_t bound, int64_t nowNs, PartitionWriteState& state);
      [[nodiscard]] int64_t judgedThrough() const noexcept;
      [[nodiscard]] bool stalled() const noexcept;
      [[nodiscard]] bool waitingForInstall() const noexcept;
      void addVerdictView(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint = 0);
      void removeVerdictView(uint64_t viewObjectId, uint64_t planFingerprint);
      int requestChange(const ConstraintSet& next);
      void installTestVerdictViewForTest(ViewInstance*, uint64_t viewObjectId, TestRejectRule);
      // Worker plumbing (cold): the replica to append through, the interval, activation on a new leadership
      void bindLeader(rune::replication::PartitionReplica* replica, uint64_t leadershipGeneration, int64_t logStart) noexcept;
      void setVerdictIntervalNs(int64_t) noexcept;
      [[nodiscard]] const VerdictSourceStats& stats() const noexcept;
      // test seams: with bindLeader(nullptr, …) appends are captured here instead of reaching a replica
      [[nodiscard]] std::span<const std::vector<uint8_t>> appendedForTest() const noexcept;
      [[nodiscard]] uint64_t judgeCallsForTest() const noexcept;
      void holdJudgeForTest(bool) noexcept;
  };
  void verdictFeedBegin(void* state, int64_t logStart, int64_t target) noexcept;           // RcVerdictHooks::begin
  void verdictFeedBatch(void* state, const uint8_t* b, std::size_t len, int64_t base) noexcept;   // RcVerdictHooks::batch
  ```
  - **`verdictFeedBegin`** truncates `constraintSets`, `markers` and `rejected` from `target`. It keeps everything below.
  - **`verdictFeedBatch`** applies a type-17 batch (`rejected.onMarker`, `markers.onMarker(base, finalThrough)`) or a type-18 batch (`constraintSets.onChange(base, set)`). Other types are ignored. It never allocates except `ConstraintSetTimeline::onChange` (cold).

- **`driveJudge`, per pass, in this order** (the algorithm the tests alone do not fix):
  ```
  if stalled_: return 0                                     // capacity: alert once, never a verdict
  if needsActivation_: activate_(state)                     // once per bindLeader with a new generation (cold)
  //   K = extendOverEmptySets(state.rejected.coveredThrough(), state.constraintSets)
  //   for every member of setAt(K) and every joining member: if inst.foldCursor > K -> discard (P4-4)
  //   judgeCursor_ = K ; startKnown_ = K                      // verdicts below K are known: never re-judged
  //   emitFrom_ = extendOverEmptySets(state.markers.committedThrough(), state.constraintSets)   // P4-3: W, past empty-set spans
  replay every member with foldCursor < judgeCursor_ up to judgeCursor_ via its own fold
  //   (foldBatch / foldAggregateBatch with FoldSkip{.aborted, .rejected = &state.rejected},
  //    pass cap maxRecordsPerPass): unjudged, rejected skipped
  if any active member's foldCursor < judgeCursor_: return work   // judging starts only with every member at the cursor
  passBound = min(bound, emitFrom_ + checkpointCap_)        // P4-4 (checkpointCap_ = INT64_MAX without persisted members)
  while judgeCursor_ < passBound:
      A = state.constraintSets.nextChangeAtOrAfter(activeSetFrom_)   // the next change record
      if judgeCursor_ > A: switch to setAt(A + 1):
          every member installed?            else waitingForInstall_ = true; break
          every joining member's foldCursor == A + 1?   else replay them (bounded by A + 1); break
          leaving members stop judging (Retiring); activeSetFrom_ = A + 1
      read whole batches from judgeCursor_; for each:
          control batch or Part-01 aborted batch -> judgeCursor_ = last + 1; continue
          for each record with offset in [judgeCursor_, passBound):
              set empty -> accept (members of no set apply nothing)
              else v = judge(rec); a CapacityExhausted prepare or a full index -> stalled_ = true; stop
          judgeCursor_ advances to the batch end (or the bound mid-batch, like the fold cursor contract)
  emit markers when a P4-3 trigger holds; push checkpoints of persisted members when a marker covers their cursor
  drop each Retiring member once state.markers.committedThrough() > its leave offset   // R09b: W passed its change record
  ```
  - **`judge(rec)`:** prepare every member of the active set in set order. Then evaluate in order, stopping at the first failure. Accepted → apply every member. Rejected → `state.rejected.add(offset, code, cid)` and the stats.
  - **`TestReject` evaluation** (P4-12):
    - **`TagEquals`** reads the `tag` field from `rec.writer`.
    - **`OffsetSet`** matches `rec.offset`.
    - **`CapLiveKeys`** rejects when the prospect is `Upsert` with `!rows[0].existed` and `table.size() >= cap`.
    - A tombstone is never rejected.
  - **`requestChange`**:
    1. builds the change batch (Task 3) in a per-source scratch reserved at construction (`changeBatchBytes(kMaxVerdictViewsPerSource)`);
    2. appends it with `replica->appendControlBatch(span, false)`;
    3. calls `constraintSets.onChange(base, next)` at append time;
    4. returns `base`.

- [ ] **Step 1: Write the failing tests.** They use a fake `readRange` over `InMemoryLog`-built batches, one `ViewInstance` per member built as `WorkerViewSetTest.cpp:100-170` does, and a fake `PartitionReplica`-free append. `bindLeader(nullptr, …)` records the would-be appends into `appendedForTest()`.
  - `CheckedWritesTimeline.SetAtTakesTheLatestChangeBelow`: changes at 10 (set 1) and 20 (set 2) → `setAt(10)` = empty (id 0), `setAt(11)` = 1, `setAt(20)` = 1, `setAt(21)` = 2; `nextChangeAtOrAfter(11) == 20`.
  - `CheckedWritesTimeline.OnChangeIsIdempotentAndTruncates`: `onChange(10, s)` twice → size 1; `truncateFrom(10)` → size 0.
  - `CheckedWritesTimeline.MarkerLedgerPromotesOnHw`:
    - markers at 15 (ft 12) and 30 (ft 25); `onHw(16)` → `committedThrough() == 12`, `committedMarkerEnd() == 16`, `appendedThrough() == 25`;
    - `onHw(31)` → 25 / 31;
    - `truncateFrom(20)` before `onHw(31)` drops the second marker.
  - `CheckedWritesVerdictSource.TagEqualsRejectsAndRecordsTheVerdict`: one `TestReject{TagEquals{9}}` (objectId 7) over the projection. Records tags 1, 9, 1 at offsets 11–13 → `judge` gives OK, `{51, 0x00070001}`, OK; the table has 2 rows; `rejected` holds {12}.
  - `CheckedWritesVerdictSource.OneOfTwoRejectsNoViewChanges`: members (objectId 7, `TagEquals{9}`) and (objectId 8, `CapLiveKeys{1}`); the second new id → rejected by 8 → neither table changes (byte snapshot), `constraintId == 0x00080001`.
  - `CheckedWritesVerdictSource.FirstFailingMemberInSetOrderWins`: both reject the same record → `constraintId == 0x00070001`.
  - `CheckedWritesVerdictSource.TombstonesAreNeverRejected`: `CapLiveKeys{0}` → a tombstone is accepted.
  - `CheckedWritesVerdictSource.CapLiveKeysCountsAcceptedOnly`: cap 2; ids 1, 2, 3 (rejected), tombstone 1, id 3 → accepted.
  - `CheckedWritesVerdictSourceRegression.CapacityStallIsNeverAVerdictRegression`: index reserved 1, two rejections → the second judge stalls (`stalled()`), `judgedThrough()` stays at that record, no entry added, no table touched.
  - `CheckedWritesVerdictSource.MarkerTriggers`: cursor moved and 100 ms passed → 1 marker `[emitFrom, judgedThrough)`; a pass with a rejection → a marker at once; 50 ms and nothing rejected → none; 4097 rejections in one pass → 2 markers, the split at the 4097th entry's offset.
  - `CheckedWritesVerdictSource.JoiningMemberBuildsUnjudgedToTheChangeOffset`: member X active; a change at A = 20 adds Y. Y replays [0, 21) skipping the index; records 15–19 are judged by X alone; record 22 waits until `Y.foldCursor == 21` and is then judged by both.
  - `CheckedWritesVerdictSource.LeavingMemberJudgesUntilItsLeaveOffset` (R09b): a drop of Y at D = 30 → records ≤ 29 are judged by both, records > 30 by X only; Y stays registered (`Retiring`) after the judge passes 31 while `W ≤ 30`, and is dropped at the first pass after a committed marker brings `W` past 30.
  - `CheckedWritesVerdictSourceRegression.MissingMemberWaitsNeverGuessesRegression`: the logged set names (9, TestReject, fp) with no such member → `waitingForInstall()`, the judge cursor does not pass the change offset + 1, `judgeWaitsForInstall == 1`; after `addVerdictView` it proceeds.
  - `CheckedWritesVerdictSource.FeedMergesMarkersAndChanges`: `verdictFeedBatch` with a change (set 1 at 10) and a marker (10..20, {12}) → `setAt(11).id == 1`, `isRejected(12)`, `coveredThrough() == 20`.
  - `CheckedWritesVerdictSource.ActivationExtendsCoverageOverEmptySets`: changes {10: {X}}, {30: {}}, {50: {X}}, markers covering [10, 31) → K = 51 (offsets 31..50 are under the empty set); the judge starts at 51.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `rune/node/VerdictSource.h` not found.
- [ ] **Step 3: Implement** as specified. Keep `judge` and `driveJudge` free of allocation: prospects are reserved per member at `addVerdictView` (cold), and the marker scratch is reserved at construction to `markerBatchBytes(kMaxMarkerEntries)`. Member evaluation is a `switch (kind)` (05 and 09 add their cases), never a virtual call.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTimeline|CheckedWritesVerdictSource|CheckedWritesControlRecord' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp \
          broker/libs/node/include/rune/node/WriteState.h broker/libs/node/CMakeLists.txt \
          broker/libs/node/tests/node/CheckedWritesTimelineTest.cpp broker/libs/node/tests/node/CheckedWritesVerdictSourceTest.cpp
  git commit -m "node: VerdictSource - judge (prepare all, evaluate, apply all or none), TestReject, timeline, marker ledger (C9.5, C12)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 6: Worker wiring: the scan feed, the duty-pass order, markers, refold and failover

**Files:**
- Modify: `broker/libs/node/include/rune/node/RcState.h:177-260` (`RcVerdictHooks`, `setVerdictHooks`), `:376-383` (call `begin` on the `Unbuilt → Scanning` edge, disk only), `:633-650` (call `batch` after `builder.step` for a control batch with `producerId() < 0`), `:352-356` (beside `aborted_.pruneBelow`, a `logStart` hook for `rejected.dropBelow`; the worker does it in `rcAdvance_`)
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h`:
  - `verdictSources_` (C9.5) and `verdictSource()`;
  - the `ViewTemplate` (`:536-552`) appends `VerdictViewKind verdictKind{}; uint64_t verdictObjectId = 0; uint64_t verdictFingerprint = 0; std::optional<TestRejectRule> testRule; bool retiring = false;`;
  - `driveFolds` (`:417-435`): with a non-null `write` and a `VerdictSource` for the partition, run `driveJudge` first, then the non-verdict views with `FoldSkip{.aborted = skip.aborted, .rejected = &write->rejected}` and `bound' = min(bound, judgedThrough())`;
  - `driveView_` is unchanged for non-verdict views. Verdict members are driven only by their `VerdictSource`;
  - `buildAndPushCheckpoint_` (`WorkerViewSet.cpp:652`) is called for a verdict member only by the `VerdictSource` (P4-4).
- Modify: `broker/libs/node/src/WorkerViewSet.cpp:258` (`uninstallView`: a verdict template becomes `retiring`), `:442` (`getOrCreate_`: a verdict template's instance is registered with its source's `VerdictSource` and its prospect reserved)
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `rebuildRcSlots_` (`:6364-6400`): `rc->setVerdictHooks({&verdictFeedBegin, &verdictFeedBatch, slot.write.get()})` for a disk slot with a write state;
  - `rcAdvance_` (`:6501-6524`): `write->rejected.dropBelow(log->logStartOffset())` when it moved, and `write->markers.onHw(r->highWatermark())`;
  - `driveViews_` (`:6422-6493`): pass `writeOf_(replica)` through `driveViewsOf_` to `driveFolds` / `driveRebuild`, and call `viewSet_->verdictSource(…)->bindLeader(replica, gen, logStart)` (cheap when the generation is unchanged);
  - new `rebindVerdictSources_()` after `drainInstalls`, uninstalls and `rebuildRcSlots_`;
  - new `publishVerdictCleanerBound_(replica, state)`;
  - new `maintainVerdictIndexes_()` (P4-11);
  - test seams `verdictStatsForTest(int32_t, int32_t)` (C16) and `setVerdictIntervalNs`.
- Modify: `broker/libs/node/src/CheckedWriteHook.cpp` (`topicHookFactsOf`: `verdictSource = state.isVerdictSource()`, `constrainedSource = state.isConstrainedSource()`)
- Modify: `broker/libs/replication/include/rune/replication/PartitionReplica.h:866-893`: the two atomic words beside `publishCleanerLso` (`:866`), default `kNoCleanerLso`: C11's `publishCleanerVerdictBound` / `cleanerVerdictBound` and contract addition 9's `publishCleanerVerdictMarkerEnd` / `cleanerVerdictMarkerEnd` (the marker end published before the bound with release order, read after it with acquire order). `publishVerdictCleanerBound_` (below) writes them; Task 8 adds their readers.
- Modify: `broker/libs/node/include/rune/node/NodeConfig.h` (beside the worker knobs), `broker/libs/node/src/NodeConfig.cpp` (beside `RUNE_ELECTION_TIMEOUT_MS`, `:825`, the same `parseOptionalInt64Ms`), `broker/libs/node/src/BrokerNode.cpp` (pass it to every worker)
- Create: `broker/libs/node/tests/support/CheckedWritesRig.h` (the parts below), `broker/libs/node/tests/node/CheckedWritesVerdictWorkerTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt`: a new `rune_checked_writes_verdict_tests` in the `if(aeron_FOUND)` block next to 03's `rune_checked_writes_ack_tests`, with the same links, `PROPERTIES TIMEOUT 300 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}"`

**Interfaces:**
- Consumes: Tasks 3–5; Part 01's `CheckpointStream::ResetOwed`; Part 02's `writeOf_`, `WriteSpecUpdate`; Part 03's `CheckedWritesAckRig.h` (the rig builds on its shard-mode setup).
- Produces:
  - **The duty-pass order** for an owned leader partition of a verdict source: `rcAdvanceAll_` (scan feed, `markers.onHw`) → `driveViews_` (`driveJudge`; other views ≤ `judgedThrough`; markers; verdict checkpoints) → `serveHeldViewReads_` → `flushOwedAcksView_` → `publishVerdictCleanerBound_` → push.
  - **`publishVerdictCleanerBound_`:** on a Ready leader of a verdict source, `replica->publishCleanerVerdictMarkerEnd(state.markers.committedMarkerEnd())` and then `replica->publishCleanerVerdictBound(boundFor(W, timeline))`. `boundFor` is `W` while the latest set ≤ HW is non-empty, `kNoCleanerLso` after a change to an empty set that W has passed, and `kNoCleanerLso` for a partition that was never a verdict source. It publishes only on change, like `publishCleanerLso_` (`Worker.h:6585-6592`).
  - **`CheckedWritesRig`** (namespace `rune::node::test`), first half (Task 7 adds readers, Task 8 `compact()`, Task 9 acks):
    ```cpp
    struct RigOptions { int replicas = 2; int minIsr = 1; int32_t clientVersion = 12; int64_t verdictIntervalMs = 100;
                        int64_t segmentBytes = 64 * 1024; };
    struct Rec { uint64_t id = 0; int64_t v = 0; uint8_t tag = 0; bool tombstone = false; };
    class CheckedWritesRig {
    public:
        CheckedWritesRig(std::filesystem::path dir, RigOptions = {});
        void createTypedTopic(int32_t topicId = 1);        // verdictTopicDescriptor(): {id uint64 key, v int64, tag uint8}
        void installTestRejectView(int32_t viewTopicId, uint64_t objectId, TestRejectRule, bool persisted = true);
        void replaceTestRejectView(int32_t oldView, int32_t newView, uint64_t objectId, TestRejectRule);
        void dropTestRejectView(int32_t viewTopicId);
        void installPlainProjection(int32_t viewTopicId, bool persisted = true);   // {id -> v, tag}
        void installCountByTag(int32_t viewTopicId, bool persisted = true);       // key tag, n = count(*)
        int64_t appendBatch(std::span<const Rec>, uint8_t wireAcks, int64_t pid = -1, int32_t seq = -1, bool txn = false);   // -> corr
        void commitTxn(int64_t pid); void abortTxn(int64_t pid);  // Part 11 changes commitTxn to return TxnEnd and vote first
        void followerFetch();                                 // the follower acknowledges LEO: HW := LEO
        [[nodiscard]] bool driveUntilJudged(int64_t offset, int cap = 2000);
        void demotePromote();                                 // leadership + 2, memory kept
        void failover();                                      // demote; truncate above HW; restart(); promote
        void restart();                                       // new Worker over the same log and checkpoint dirs; re-installs
        void truncateTo(int64_t offset);                      // offset >= HW (an uncommitted tail only)
        void advanceClock(int64_t ns);
        [[nodiscard]] const PartitionWriteState& state() const;
        [[nodiscard]] std::vector<rune::storage::control_record::MarkerHeader> markersInLog() const;
        [[nodiscard]] std::optional<std::vector<uint8_t>> viewRow(int32_t viewTopicId, uint64_t key);
        [[nodiscard]] std::size_t viewSize(int32_t viewTopicId);
        [[nodiscard]] int64_t leo() const; [[nodiscard]] int64_t hw() const;
        [[nodiscard]] int64_t firstDataOffset() const;        // the base of the first data batch after the change record
        struct RigLogRec { int64_t offset; bool control; int16_t controlType; uint64_t id; int64_t v; uint8_t tag; bool tombstone; };
        [[nodiscard]] std::vector<RigLogRec> logRecords() const;   // every record below the HW, data and control
        void holdJudge(bool);                                 // VerdictSource::holdJudgeForTest
        rune::node::Worker& worker();
    };
    // RigOptions also carries: bool skipReinstall = false (restart() leaves the TestReject installs out);
    // uint32_t verdictArenaEntries = 65536 (Task 9's kVerdictAckArenaEntries).
    ```
  - **Worker test seams:**
    - `[[nodiscard]] const VerdictSourceStats* Worker::verdictStatsForTest(int32_t topicId, int32_t partition) const noexcept` (C16; nullptr = no verdict source);
    - `verdictCutsForTest()` and `verdictScratchGrowthsForTest()` (Task 7);
    - the sink's `offerFramesForTest()` and `claimFramesForTest()` (Task 9).
    It uses one shard-mode `Worker` with the ack sink, a disk `Log` + leader `PartitionReplica` (the follower's fetches are driven by `followerFetch()` through `replica->onFetchRequest`, `ShardWorkerAckTest.cpp:340`), a `WorkerViewSet` with a `ViewSnapshotChannel` and a `ViewSnapshotIoAgent` (the pattern of Part 01's `AbortedFoldRig.h`), level 4, and `now += 1 ms` per pass.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesVerdictWorkerTest.cpp`). The rig sets up typed topic 1, `TestReject{TagEquals{9}}` objectId 7 on view 101 (persisted) and `installPlainProjection(102)`.
  - `CheckedWritesVerdictWorker.RejectedRecordNeverReachesAnyView`: records `{1, 10, 1}`, `{2, 20, 9}`, `{3, 30, 1}` in one `acks 1` batch, `followerFetch()`, `driveUntilJudged(leo())` → `rejected.isRejected(firstDataOffset() + 1)`; `viewRow(102, 2) == nullopt`; `viewSize(102) == 2` and `viewSize(101) == 2`; `verdictStatsForTest(1, 0)->rejectionsConstraint == 1`.
  - `CheckedWritesVerdictWorker.MarkerIsWrittenAndCommitted`: after the judge and `advanceClock(100 ms)` → one marker in the log `[A + 1, judgedThrough)` with the entry; after `followerFetch()` `state().rejected.watermark() == judgedThrough` (W); the replica's `cleanerVerdictBound()` equals W, and `cleanerVerdictMarkerEnd()` equals the marker offset + 1.
  - `CheckedWritesVerdictWorker.NothingRejectedNoMarkerBeforeTheInterval`: accepted records only, 50 ms → no marker; at 100 ms → one marker, entries empty.
  - `CheckedWritesVerdictWorker.DeliveryWaitsDuringABuildWritesDoNot` (§15):
    - 500 records first, then `installTestRejectView`; the change record lands at A;
    - `appendBatch` of 3 more records → acked OK at the HW (`acks 1`);
    - `state().verdicts->judgedThrough() <= A + 1` until the new member reaches `A + 1`;
    - pre-A records with tag 9 are accepted (never judged).
    - The delivery half ("delivery waits") needs Task 7's readers: `CheckedWritesConsumer.DeliveryWaitsDuringABuildWritesDoNot`.
  - `CheckedWritesVerdictWorkerRegression.RefoldBelowWIgnoresALaterRuleRegression` (§15):
    - `TagEquals{9}` rejects a record; the marker commits (W past it);
    - `replaceTestRejectView(101 → 103, objectId 7, TagEquals{1})`;
    - delete view 103's checkpoint file, `restart()`, drive;
    - view 103 never contains the tag-9 record (below W, the index decides);
    - tag-1 records written after the replace are rejected;
    - view 102 equals its state before the restart.
  - `CheckedWritesVerdictWorkerRegression.NewLeaderRejudgesAboveWAndRemarksRegression` (§15, timing `FailoverFoldMarker`):
    - judge a rejection; the marker is appended but the HW stays below it;
    - capture the marker bytes' payload; `failover()` (the marker is cut, memory fresh);
    - drive → a new marker covering the same range with identical entries;
    - `mismatches() == 0`, `rejectionsConstraint` per the model.
  - `CheckedWritesVerdictWorkerRegression.FailoverBetweenHwAndFoldJudgesTheSameRegression` (timing `FailoverHwFold`): HW passed, `failover()` before any fold → the same verdicts as a run without the failover (a second rig, compare entries).
  - `CheckedWritesVerdictWorkerRegression.FailoverRightAfterAChangeRecordJudgesWithTheSameSetRegression` (§15):
    - a change to `TagEquals{1}` at S, committed, then `failover()` before the judge reaches S;
    - records below S are judged with tag 9 and above S with tag 1, exactly as an undisturbed rig.
  - `CheckedWritesVerdictWorkerRegression.LeaderWithoutTheInstallWaitsRegression` (§15):
    - `restart()` without re-installing view 101 (rig option `skipReinstall`);
    - `verdicts->waitingForInstall()` holds and `judgedThrough()` stays at the first post-change offset;
    - after `installTestRejectView(101, …)` the judge proceeds, with the same verdicts.
    - The reader and ack halves need Tasks 7 and 9: `CheckedWritesConsumerRegression.LeaderWithoutTheInstallDeliversNothingAboveTheJudgeRegression` and `CheckedWritesVerdictAckRegression.LeaderWithoutTheInstallParksTheViewAckRegression`.
  - `CheckedWritesVerdictRestartRegression.CheckpointAheadOfLocalMarkersIsDiscardedAndRefoldedRegression` (Review Focus 1):
    - judge and checkpoint view 101 (marker in the log, `O` = its `finalThrough`);
    - truncate the log's local copy of the marker by stopping the rig, cutting the segment file at the marker's position (a crash that lost the unflushed tail; the records below stay), then `restart()`;
    - `verdictCheckpointsDiscarded == 1`; the view refolds from 0; verdicts equal the model; `ViewSnapshotIoAgent` saw a `kResetMsgType` frame before the first new delta.
  - `CheckedWritesVerdictWorker.RestartWithMarkersKeepsTheCheckpoint`: a normal restart → `verdictCheckpointsDiscarded == 0`; view 101 resumes at its checkpoint `O`.
  - `CheckedWritesVerdictWorker.DemotePromoteKeepsOwnVerdicts`: verdicts judged and not yet marked, `demotePromote()` → the first marker of the new leadership covers them from W (entries equal), with no re-judging (`judgeCalls` unchanged for those offsets).
  - `CheckedWritesVerdictWorker.TruncationOfAnUncommittedTailChangesNoVerdict` (timing `TruncateTail`): data above HW, `truncateTo(hw)` → the index and the markers are unchanged.
  - `CheckedWritesVerdictWorker.HookFactsFollowTheTimeline`: before the install, `topicHookFactsOf(state)` has both facts false; after it, `verdictSource` and `constrainedSource` are true; a transactional batch is then refused `InvalidRequest` (03 rule 7), and a 4097-record `View` batch `InvalidRequest` (SD4).
  - `CheckedWritesVerdictWorker.IntervalKnobIsHonoured`: `RigOptions.verdictIntervalMs = 20` → a marker after 20 ms.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests`. Expected: compile FAIL on `verdictStatsForTest`, then assertion FAIL.
- [ ] **Step 3: Implement** as specified. `VerdictSource` is created in `driveFolds` when `write != nullptr` and either a verdict template for the source exists or `write->constraintSets.latest()` is non-empty. It is erased when neither holds and `judgedThrough()` is past the last non-empty span. Each create or erase calls `rebindVerdictSources_()`. The desired set is the verdict templates of the source in ascending `viewObjectId` (P4-5); a non-retiring template is a member. When the desired set and `latest()` differ on a Ready leader, `requestChange(desired)` is called, and a `kChangeDeferred` result retries next pass. `NodeConfig::verdictIntervalMs` defaults to 100.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests rune_checked_writes_tests rune_checked_writes_ack_tests && ctest --test-dir build/root -R 'CheckedWritesVerdict|CheckedWritesViewAck|CheckedWritesAbortedFold|WorkerViewSet|ViewRecovery|RcState|ShardWorkerAck' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/RcState.h broker/libs/node/include/rune/node/WorkerViewSet.h \
          broker/libs/node/src/WorkerViewSet.cpp broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/src/CheckedWriteHook.cpp broker/libs/node/include/rune/node/NodeConfig.h \
          broker/libs/node/src/NodeConfig.cpp broker/libs/node/src/BrokerNode.cpp \
          broker/libs/replication/include/rune/replication/PartitionReplica.h \
          broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesVerdictWorkerTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "node: verdict sources on the worker - scan feed, judge-first duty order, markers, W, refold below W, failover re-judging, set changes" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 7: Consumers: the bound and the cut, on every push path

**Files:**
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `verdictBound_` (C9.6) next to `rcServe_` (`:6544-6551`);
  - `pushOwnedSubscriptions_` (`:3736-3744`: bound; `:3802-3830`: before building `pc::Records`, the cut);
  - `servePushRead_` (`:4294-4308`: bound) and `readPushChunk_` (`:4386-4420`: the cut into `verdictRebuildScratch_` as its own run);
  - `FilterSourceImpl` (`:3378-3420`: `bound()` with the verdict bound, `rejected()`).
- Modify: `broker/libs/node/include/rune/node/filter/FilterGroup.h:95-101` (concept), `:465-475` (`collect_` skips rejected records and counts `sc.droppedRejected`), `:481-505` (`send_` takes the zero-copy path only when `keep.size() == sc.records.size() && sc.droppedRejected == 0`); `FilterScratch` (`:113-130`) appends `uint32_t droppedRejected = 0;`
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (readers)
- Create: `broker/libs/node/tests/node/CheckedWritesConsumerTest.cpp` (in `rune_checked_writes_verdict_tests`)

**Interfaces:**
- Consumes: `PartitionWriteState::verdictKnownThrough()`, `rejected` (Tasks 4–6).
- Produces:
  ```cpp
  [[nodiscard]] int64_t Worker::verdictBound_(const PartitionWriteState* ws, int64_t bound) const noexcept;   // C9.6 + P4-7
  std::vector<uint8_t>                  Worker::verdictRebuildScratch_;   // reserved kPushMaxChunkBytes at construction; grow-once (counted)
  std::vector<rune::protocol::RecordSpan> Worker::verdictSpans_;          // reserved 4096; grow-once (counted)
  std::vector<uint32_t>                 Worker::verdictKeep_;             // reserved 4096; grow-once
  ```
  - **The cut**, per data batch with `ws != nullptr && ws->rejected.size() != 0` [[unlikely]] and `nextRejectedAtOrAfter(base) <= last`:
    1. `forEachRecordSpan` into `verdictSpans_`;
    2. collect the non-rejected indices into `verdictKeep_`;
    3. empty keep → the batch is skipped like a control batch (cursor `last + 1`, no frame);
    4. else `rebuildBatch` into the scratch and send that.
  - **Push-read chunks:** rebuilt batches are appended back to back in the scratch and each becomes its own `ChunkRun`. A chunk stops before a rebuild that would overflow the scratch; the rest goes in the next read.
- Rig readers:
  ```cpp
  enum class SubKind : uint8_t { Raw, Group, Filtered, PushRead };   // PushRead = a gateway-style PushRead op (servePushRead_)
  int64_t subscribe(SubKind, int64_t from = 0, uint8_t filterTag = 1);   // Filtered: tag == filterTag
  struct Delivered { int64_t offset; uint64_t id; uint8_t tag; bool tombstone; };
  std::vector<Delivered> collect(int64_t subId, int cap = 2000);
  ```

- [ ] **Step 1: Write the failing tests** (`CheckedWritesConsumerTest.cpp`; the rig of Task 6 with `TagEquals{9}`). For each `SubKind` (a `TEST_P` over the four kinds):
  - `CheckedWritesConsumer/Kind.RejectedRecordIsNeverDelivered`: tags 1, 9, 1 in one batch → delivered offsets are {o, o+2}; the delivered batch keeps its base offset and `lastOffsetDelta`.
  - `CheckedWritesConsumer/Kind.NothingBeyondJudgedThroughIsDelivered`: hold the judge (rig `holdJudge(true)`) → nothing above `judgedThrough()` is delivered even though the HW passed it; release → delivered.
  - `CheckedWritesConsumer/Kind.ReplayFromStartSkipsRejected`: 20 batches, 5 rejected records → a subscription from 0 after everything sees 15 records + the plain ones and no rejected one.
  - `CheckedWritesConsumerRegression/Kind.FullyRejectedBatchIsSkippedOnEveryPushPathRegression` (Review Focus 2): a batch whose 4 records are all tag 9, between two accepted batches → the consumer receives exactly the two accepted batches, no zero-record frame, and its cursor ends at the LEO.
  - `CheckedWritesConsumer/Kind.FailsClosedBeforeTheVerdictSourceExists`: after `restart()` and before the activation completes, nothing above K is delivered.
  - Not parameterized:
    - `CheckedWritesConsumer.FilteredGroupNeverZeroCopiesABatchWithARejectedRecord`: 3 filtered members on `tag = 1`, a batch tags 1, 9, 1 → `group_.rebuilt == 1`, `zeroCopy == 0` for that batch, and each member gets 2 records.
    - `CheckedWritesConsumer.ControlRecordsSixteenToNineteenAreNeverPushed` (Task 3's reader (6)): a control batch of each type 16–19 (Task 3's builders) between two data batches → an unfiltered subscription receives both data batches and never a control batch.
    - `CheckedWritesConsumer.DeliveryWaitsDuringABuildWritesDoNot` (§15, the delivery half of Task 6's test): 500 records, then `installTestRejectView` (change at A), then 3 more records → a raw subscription from 0 receives records < A as soon as they are committed, and records > A only after the new member reached `A + 1`.
    - `CheckedWritesConsumerRegression.LeaderWithoutTheInstallDeliversNothingAboveTheJudgeRegression` (§15): `restart()` with `skipReinstall` → a subscription receives nothing above `judgedThrough()` while `waitingForInstall()`; after the install it receives the rest, without the rejected records.
    - `CheckedWritesConsumer.UntypedTopicPaysOneBranch`: 1,000 push passes on an untyped topic → `verdictSpans_` never touched (`verdictCutsForTest() == 0`).
    - `CheckedWritesConsumer.OversizedRebuildGrowsOnceAndIsCounted`: a 600 KiB batch with one rejected record → delivered without it; `verdictScratchGrowthsForTest() == 1`; a second identical batch → still 1.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests && ctest --test-dir build/root -R CheckedWritesConsumer --output-on-failure`. Expected: FAIL (rejected records delivered).
- [ ] **Step 3: Implement** as specified. Each push path calls `verdictBound_` right after its existing HW/LSO bound. `FilterSourceImpl::rejected()` returns `&ws->rejected` for a typed partition, else nullptr.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesConsumer|Filter|ShardWorkerAck|PushRead|WorkerRegression' -j14 --output-on-failure`. Expected: PASS. The Phase 2c filter suites are unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/filter/FilterGroup.h \
          broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesConsumerTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "node: consumers of a verdict source - bound at judgedThrough, rejected records cut on every push path (8.6)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 8: The cleaner: `VerdictCleanState`, the bounds and the rewrite rules

**Files:**
- Create: `broker/libs/storage/include/rune/storage/VerdictCleanState.h`, `broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp`
- Modify:
  - `broker/libs/storage/src/LogCleaner.cpp`: `scanTxnState` (`:216-251`); both `buildOffsetMapFromBases` (`:315`, `:372`), the map skip at `:349` / `:419`, now per record; `markerAction` / `rewriteControlBatch` (`:261-299`); the rewrite drop in `rewriteSegmentImpl` (`:435`, next to the aborted drop at `:543`) and `mergeGroupWithMap` (`:1386`, next to `:1468`); every `cleanLimit()` site (`:443`, `:844`, `:995`, `:1141`, `:1398`, `:1640`) takes `min` with `map.verdictState().cleanLimit()`;
  - `broker/libs/storage/include/rune/storage/LogCleaner.h:79-110` (`setVerdictBounds`);
  - `OffsetMap.h:49`, `SkimpyOffsetMap.h:250` (`verdictState()`);
  - `broker/libs/replication/include/rune/replication/PartitionReplica.h:893`: `cleanerSwapBound()` returns `std::min({highWatermark(), cleanerLastStableOffset(), cleanerVerdictBound()})` on a leader (C11; Task 6 added the atomics);
  - `broker/libs/node/include/rune/node/LogCleanerAgent.h:455` (`firstUncleanable = min(firstUncleanable, replica->cleanerVerdictBound())`, and `cleaner.setVerdictBounds(bound, markerEnd)` before the pass).
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (`compact()`: seal the active segment, then one `LogCleanerAgent`-equivalent pass on the rig thread with the replica's published bounds and `markerRemovalBound`, then `drainCleanerSwapsOnOwner`)

**Interfaces:**
- Produces (cleaner-thread only, cold, modeled on `TxnCleanState.h:40-165`; C11 API plus contract addition 10):
  ```cpp
  class VerdictCleanState {
  public:
      void clear() noexcept;
      void onVerdictMarker(int64_t markerOffset, int64_t fromOffset, int64_t finalThrough,
                           std::span<const RejectedOffsetIndex::Entry> entries);
      void onConstraintSetChange(int64_t offset, bool nonEmpty);
      void onReservation(int64_t offset);                 // 06 gives it meaning; 04 only records it
      void finish(int64_t toOffset, int64_t publishedBound, int64_t markerRemovalBound);
      [[nodiscard]] bool isRejected(int64_t offset) const noexcept;
      [[nodiscard]] int64_t cleanLimit() const noexcept;
      [[nodiscard]] bool markerRemovable(int64_t markerOffset) const noexcept;   // given the pass boundary set by markRemovalsBelow
      void markRemovalsBelow(int64_t cleanBoundary) noexcept;
      [[nodiscard]] bool keepsReservation(int64_t offset) const noexcept;        // 06; 04 returns true
      [[nodiscard]] bool keepsControlRecord(int64_t offset, int16_t type) const noexcept;
  };
  ```
- **The coverage computed in `finish`** (not fixed by the signatures):
  ```
  // events in offset order: change(A, nonEmpty), marker(m, from, ft)
  everNonEmpty = false; active = false; lastEmptyAt = -1; cover = 0
  change(A, nonEmpty):
      if nonEmpty && !active: if (!everNonEmpty || cover > lastEmptyAt) cover = max(cover, A + 1); active = true; everNonEmpty = true
      if !nonEmpty && active: active = false; lastEmptyAt = A
  marker(m, from, ft): cover = max(cover, ft)
  own = !everNonEmpty ? kNone : (!active && cover > lastEmptyAt) ? kNone : cover
  cleanLimit = min(own, publishedBound)        // publishedBound kNoCleanerLso = no cap
  ```
- **Removal:** a marker at `m` is removable iff:
  - every listed offset < the pass boundary;
  - a later marker below the scan end has a higher `finalThrough`;
  - `m < markerRemovalBound`.
  It is then stamped and dropped through `markerAction`'s horizon path. Type 18 is always kept. Type 16 is kept iff `keepsReservation` (06). Type 19 is always kept until Part 11 replaces the rule with `offset >= txnJudgeBound` (11 P11-8 (c)).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesVerdictCleanerTest.cpp` (storage, on a real `Log` with small segments built through `appendNoSync` of data batches and `appendControlBatch`-equivalent control batches):
    - `CheckedWritesVerdictCleaner.RejectedLatestValueNeverWins`: key 5 accepted at 3 and rejected at 7, marker at 9 (from 0, ft 9, {7}) → after a full clean, offset 3 is present, 7 absent.
    - `CheckedWritesVerdictCleaner.RejectedRecordsAreDroppedInBothRewriteEngines`: the same through `produceCleanedFileBounded` and `produceCleanedGroupBounded` → no record at 7 in either output.
    - `CheckedWritesVerdictCleanerRegression.NeverCleansAtOrAboveWRegression` (§15): a published bound of 6, a log to 20 → no record at or above 6 is rewritten (segment bytes from 6 equal before and after).
    - `CheckedWritesVerdictCleaner.MarkerAboveToOffsetIsReadUpToMarkerEnd`: rejected at 7, its marker at 15 in the active segment, `toOffset = 10`, `markerEnd = 16` → 7 is dropped; with `markerEnd = 10` → `cleanLimit` stays below 7 (nothing at/after the uncovered rejected record is cleaned).
    - `CheckedWritesVerdictCleaner.CoverageRules`: never a change → `kNone`; a change {X} at 4 and no marker → `cleanLimit == 5`; markers to 30 → 30; a change {} at 40 with cover 41 → `kNone`; re-activation at 50 before the final marker → stays at the cover.
    - `CheckedWritesVerdictCleanerRegression.MarkerKeptUntilEveryReplicaFetchedItRegression` (Review Focus 3): the records gone and a later marker present, but `markerRemovalBound = m` → kept; at `m + 1` → stamped; at the horizon → dropped.
    - `CheckedWritesVerdictCleaner.ChangeRecordsAreAlwaysKept`: a superseded change → present after any number of passes.
    - `CheckedWritesVerdictCleaner.StaleBoundOnlyCleansLess`: published bound 50, own coverage 30 → `cleanLimit == 30`.
  - `CheckedWritesVerdictWorkerTest.cpp` (rig), C11 swap bound:
    - `CheckedWritesVerdictCleanerRegression.SwapAboveTheVerdictBoundIsNeverInstalledRegression`: a published verdict bound below a compaction output's member base (HW and LSO above it) → `drainCleanerSwapsOnOwner()` rejects the output and the segment is unchanged; once the bound passes the base the next pass installs it.
  - `CheckedWritesVerdictWorkerTest.cpp` (rig):
    - `CheckedWritesVerdictCleanerRegression.RemovedMarkerNeverStallsCoverageRegression` (Review Focus 4): compact until the first marker is removed, then `failover()` → K comes from the second marker, nothing is re-judged below it, and consumers from 0 receive no rejected record.
    - `CheckedWritesVerdictCleaner.RigCompactionKeepsTheAcceptedLatest`: cell-(e) assertion on 200 random writes.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_storage_tests rune_checked_writes_verdict_tests && ctest --test-dir build/root -R CheckedWritesVerdictCleaner --output-on-failure`. Expected: FAIL (a rejected latest value wins).
- [ ] **Step 3: Implement** as specified.
  - `scanTxnState` walks segments up to `max(toOffset, verdictMarkerEnd)`. Above `toOffset` it feeds only `pid < 0` control batches (data is not indexed there).
  - The map builds skip `verdicts.isRejected(offset)` per record.
  - Both rewrite engines drop such records.
  - `markerAction` consults `VerdictCleanState` for `pid < 0` control batches by `control_record::recordType`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesVerdictCleaner|LogCleaner|Compaction|CleanerOffset|SkimpyOffsetMap' -j14 --output-on-failure` and `ctest --test-dir build/root -R rune_compaction_correctness --output-on-failure`. Expected: PASS, every pre-existing cleaner test unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/storage/include/rune/storage/VerdictCleanState.h broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp \
          broker/libs/storage/CMakeLists.txt broker/libs/storage/src/LogCleaner.cpp broker/libs/storage/include/rune/storage/LogCleaner.h \
          broker/libs/storage/include/rune/storage/OffsetMap.h broker/libs/storage/include/rune/storage/SkimpyOffsetMap.h \
          broker/libs/replication/include/rune/replication/PartitionReplica.h broker/libs/node/include/rune/node/LogCleanerAgent.h \
          broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/node/CheckedWritesVerdictWorkerTest.cpp
  git commit -m "storage: the cleaner on a verdict source - bound at W, rejected records never latest and dropped, marker removal rules (8.5)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 9: `View` acks with verdicts: the arena, the release rule, retries, big frames and the client

**Files:**
- Modify: `broker/libs/node/include/rune/node/SessionAckSink.h`:
  - `VerdictAckArena` beside `AckEntry` (`:111-131`);
  - `encode_` (`:1065-1076`) becomes a member that writes the trailer from the arena;
  - every path that removes an `AckEntry` (claimed, `++dropped_`, `fail_`) frees its run;
  - `AeronSessionPubPolicy::claim` (`:1189-1201`): P4-9.
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `flushOwedAcksView_` (03): the condition `state->verdicts->judgedThrough() > lastOffset` on a verdict source (C4); the trailer reserve and fill (`rejected.forEachIn(baseOffset, lastOffset + 1)` → `(offset − baseOffset, code, cid)`); P4-10;
  - the duplicate answers (P4-8, R04e): an in-window duplicate with `View` on a verdict source parks with `replay = 1` and is released only once `W > lastOffset`, with the trailer rebuilt from the index; a known-old one is answered `DuplicateSequence` without a trailer;
  - `emitProduceAck_` (`:2132-2149`) copies `verdictRun` / `verdictCount` / `hasVerdicts`.
- Modify (client): `client-cpp/include/rune/client/Completion.h:54` (`OpStatus::StoredIdUnknown = 13`, a reserved placeholder for Part 06, and `OpStatus::VerdictUnknown = 14`; R-X2), `client-cpp/include/rune/client/agent/ClientAgent.h:571-580`, `client-cpp/src/agent/ClientAgent.cpp:1101-1113` (copy the trailer; `codec::decodeProduceResponse` v12 fields from 03), `client-cpp/src/Client.cpp:3152-3163`, `ProducerHub.{h,cpp}:81-88`, `ProducerImpl.{h,cpp}` (`onProduceResponse` `:2350-2390`, `completeSlice_` `:2761-2782`: a per-record override from the trailer; `completeHandle_` takes `reserved0 = constraintId`)
- Create: `broker/libs/node/tests/node/CheckedWritesVerdictAckTest.cpp` (in `rune_checked_writes_verdict_tests`), `client-cpp/tests/producer/ProducerVerdictsTest.cpp` (in `rune_invoker_tests`, `client-cpp/CMakeLists.txt`)
- Modify: `broker/libs/node/tests/node/SessionAckSinkTest.cpp`, `broker/libs/node/tests/support/CheckedWritesRig.h` (`ack(corr)`)

**Interfaces:**
- Consumes: C5 (`ProduceResponse` v12, `AckEntry` fields, `kNoVerdictRun`), Part 03's `PendingAcksView` and `flushOwedAcksView_`.
- Produces:
  ```cpp
  inline constexpr uint32_t kVerdictAckArenaEntries = 65536;   // C5
  class VerdictAckArena {                                       // fixed: the entries and the run table are reserved at construction
  public:
      [[nodiscard]] bool reserveVerdicts(uint16_t n, uint32_t& run) noexcept;   // false = full: defer the release
      [[nodiscard]] std::span<uint8_t> entries(uint32_t run) noexcept;          // n * 8 bytes, encoded {recordIndex u16, code u16, cid u32}
      void free(uint32_t run) noexcept;
      [[nodiscard]] std::size_t used() const noexcept;
  };
  struct Ack { int16_t code; int64_t baseOffset; bool hasVerdicts; std::vector<std::tuple<uint16_t, uint16_t, uint32_t>> verdicts;
               std::size_t frameLen; uint64_t step; };                          // rig
  [[nodiscard]] std::optional<Ack> CheckedWritesRig::ack(int64_t corr);
  ```
  - **The arena** is a ring of 8-byte entries plus a run table of `kMaxOwedReturns` `{start, count, freed}`. Runs never wrap: a run that does not fit before the end starts at 0 if the head allows. `free` marks the run freed and advances the head over freed runs.
  - **Client mapping:**
    - an accepted record → `Ok` with its offset;
    - a rejected record → `BrokerError`, `errorCode` 51–53, `reserved0 = constraintId` (C6);
    - a Tier-2 batch → one completion carrying the first rejection;
    - `DuplicateSequence` for a `View` producer → `OpStatus::VerdictUnknown` for the whole batch, never per-record errors (P4-8, R04e).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesVerdictAckTest.cpp` (rig, `TagEquals{9}`, v12 session):
    - `CheckedWritesVerdictAck.TrailerCarriesExactlyTheRejections`: tags 1, 9, 1, 9 with `acks 3` → one OK ack, frame 54 + 2 + 16 = 72 B, verdicts `{(1, 51, 0x00070001), (3, 51, 0x00070001)}`.
    - `CheckedWritesVerdictAck.AllAcceptedIsAnEmptyTrailer`: tags 1, 1 → 56 B, `hasVerdicts`, count 0.
    - `CheckedWritesVerdictAck.WaitsForTheJudgeNotTheMarker`: hold the follower so the marker cannot commit → the ack is sent at the step `judgedThrough() > lastOffset` with the HW past the batch but below the marker.
    - `CheckedWritesVerdictAckRegression.ViewRetryInTheWindowReplaysTheOriginalVerdictsRegression` (R04e). Tags 1, 9, 1, 9 with `acks 3`; the original ack is dropped by the rig (a lost ack). The idempotent retry of the same batch (same pid, epoch, sequence) then:
      - is answered with the same `baseOffset` and a byte-identical trailer `{(1, 51, 0x00070001), (3, 51, 0x00070001)}`, so the rejected indexes are the same;
      - is not answered before `state().rejected.watermark() > lastOffset` (hold the follower: no answer while W is below; `followerFetch()` → answered);
      - leaves `hookChecksForTest()` and the LEO unchanged, and gives `verdictReplays == 1`.
      This is run twice: the retry before the judge reaches the batch (parked) and after it (from the index).
    - `CheckedWritesVerdictAckRegression.ViewRetryOutsideTheWindowIsDuplicateSequenceRegression` (R04e). The original batch, then 33 more batches from the producer, then the original re-sent: `errorCode 21`, frame 54 B with no trailer, `verdictUnknownDuplicates == 1`, nothing appended.
    - `CheckedWritesVerdictAckRegression.FourThousandRejectionsAckIsOfferedRegression` (R03a, Review Focus 5): a 4096-record batch, all tag 9 → one ack of 32,824 B through the real Aeron sink; decodes to 4096 entries in order; `offerFramesForTest() == 1`, `claimFramesForTest()` unchanged for that ack.
    - `CheckedWritesVerdictAckRegression.ArenaFullDefersInPartitionOrderRegression` (P4-10): the arena sized 4096 (rig option); batch A (4096 rejections) then B (1 rejection) on one partition, A's session publication back-pressured → B is not acked before A; once A is claimed, B follows; nothing dropped.
    - `CheckedWritesVerdictAckRegression.LeaderWithoutTheInstallParksTheViewAckRegression` (§15, the ack half of Task 6's test): `restart()` with `skipReinstall`, an `acks 3` batch above the change → no ack while `waitingForInstall()` (and none before `ackTimeoutNs`); after the install the ack is OK with the model's trailer.
    - `CheckedWritesVerdictAck.PlainAllOnAVerdictSourceHasNoTrailer`: `acks 1` → 54 B, `!hasVerdicts`.
    - `CheckedWritesVerdictAck.OldSessionNeverSeesATrailer`: a v11 session, `acks 1` → 46 B; `acks 3` from v11 is refused by 03 (`InvalidRequest`).
    - `CheckedWritesVerdictAck.LeaderLossWhileParkedIsNotLeader` and `…DeadlineIsInternal` on a verdict source: as 03, with the judge held.
  - `SessionAckSinkTest.cpp`:
    - `CheckedWritesAckSink.ArenaRunsAreFreedOnClaimDropAndFail`: three entries, one each way → `used() == 0`.
    - `CheckedWritesAckSink.TrailerBytesComeFromTheArena`: a run of 2 entries → encoded exactly.
  - `ProducerVerdictsTest.cpp` (client, fake session feeding v12 `ProduceResponse` frames):
    - `ProducerVerdicts.CoalescedBatchWithOneBadRecord`: 8 records, the trailer `{(3, 51, 0x00070001)}` → 7 completions `Ok` with `value = base + i`, record 3 `BrokerError`, `errorCode 51`, `constraintIdOf(c) == 0x00070001`; the next batch is sent and acked normally; no epoch bump, no resend.
    - `ProducerVerdicts.Tier2BatchCarriesTheFirstRejection`.
    - `ProducerVerdicts.ReplayedVerdictsCompleteEachRecordAsTheOriginal` (R04e): the first ack is lost and the retry's ack carries the trailer `{(3, 51, 0x00070001)}` → the same per-record completions as `CoalescedBatchWithOneBadRecord`, each exactly once.
    - `ProducerVerdicts.DuplicateSequenceOnAViewProducerIsVerdictUnknown` (R04e): 8 records → 8 completions, each `OpStatus::VerdictUnknown` with `errorCode 21`; none is `BrokerError` or `Ok`. For an `All` producer → today's `Ok −1`.
    - `ProducerVerdicts.VerdictArenaGrowsOnceAndIsCounted`.
    - `ProducerVerdicts.OpStatusThirteenIsReservedAndVerdictUnknownIsFourteen` (R-X2): `static_cast<int>(OpStatus::StoredIdUnknown) == 13`, `static_cast<int>(OpStatus::VerdictUnknown) == 14`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests rune_session_ack_sink_tests` and `cmake --build build/sub-client-cpp -j14 --target rune_invoker_tests`. Expected: FAIL (no trailer).
- [ ] **Step 3: Implement** as specified. In `flushOwedAcksView_`, for an entry of a verdict source:
  - count the index entries in `[baseOffset, lastOffset]` with `forEachIn`;
  - `n == 0` → `hasVerdicts = 1, verdictCount = 0`, no run;
  - else `reserveVerdicts(n, run)`, fill the entries, and set the fields on the `WorkerReturn`.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesVerdictAck|CheckedWritesAckSink|SessionAckSink|CheckedWritesViewAck' -j14 --output-on-failure`
  - `ctest --test-dir build/sub-client-cpp -R 'ProducerVerdicts|ProducerAcksFlags|Producer' -j14 --output-on-failure`
  - Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/SessionAckSink.h broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/tests/node/CheckedWritesVerdictAckTest.cpp broker/libs/node/tests/node/SessionAckSinkTest.cpp \
          broker/libs/node/tests/support/CheckedWritesRig.h \
          client-cpp/include/rune/client/Completion.h \
          client-cpp/include/rune/client/agent/ClientAgent.h client-cpp/src/agent/ClientAgent.cpp client-cpp/src/Client.cpp \
          client-cpp/include/rune/client/producer/ProducerHub.h client-cpp/src/producer/ProducerHub.cpp \
          client-cpp/include/rune/client/producer/ProducerImpl.h client-cpp/src/producer/ProducerImpl.cpp \
          client-cpp/tests/producer/ProducerVerdictsTest.cpp client-cpp/CMakeLists.txt
  git commit -m "feat: View acks carry per-record verdicts - sink arena, judge-gated release, retries from the index, offer above maxPayloadLength; client per-record results (11.3)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint B (controller).** The commands of Checkpoint A, plus `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure` under `--repeat until-fail:3`, plus `ctest --test-dir build/sub-client-cpp -R Regression -j14 --output-on-failure`.

---

## Phase 3 — The shared harness, the stress, the real broker, benches and docs

### Task 10: The shared matrix (04 cells + Part 03 folded in)

**Files:**
- Create: `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h`, `broker/libs/node/tests/support/CheckedWritesModel.h`, `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp`
- Modify: `broker/libs/node/tests/node/CheckedWritesViewAckMatrixTest.cpp` (cells from `cellsFor(Part::P03)`), `broker/libs/node/tests/support/CheckedWritesAckRig.h` (its local enums removed), `broker/libs/node/CMakeLists.txt` (`rune_checked_writes_matrix_tests`, Aeron block, `TIMEOUT 900`)

**Interfaces:**
- Produces (shared; later parts extend their own ranges and `expectNN`):
  ```cpp
  enum class Op : uint16_t { AcceptedWrite = 0, RejectedWrite = 1, RejectedOverAccepted = 2, CapRace = 3, DeleteFreesCap = 4,
                             /* 05: 20-39, 06: 40-59, 07: 60-79, 09: 80-99, 11: 100-119 */ ViewAckLV = 120, ViewAckAV = 121 };
  enum class Entity : uint16_t { RawSubscription = 0, GroupConsumer = 1, FilteredSubscription = 2, ReplayFromStart = 3,
                                 OtherViews = 4, TwoVerdictViews = 5, ViewAckWriter = 6, OldWriters = 7,
                                 AckNoViews = 120, AckOneView = 121, AckTwoViewsLate = 122, AckTransientSource = 123 };
  enum class Timing : uint16_t { Single, BackToBack, SelfCollidingBatch, Racing, RetryBeforeJudge, RetryAfterJudge,
                                 FailoverAppendHw, FailoverHwFold, FailoverFoldMarker, TruncateTail, RestartDuringRebuild,
                                 CreateOnNonEmpty, ReplaceDuringWrites, DropDuringWrites,
                                 TxnCommitBehind, TxnAbortBehind, LeaderLossParked, AckDeadline, StrictTier, GatewayRouted,
                                 RetryAfterWindow /* 04, R04e */ };
  enum class Part : uint8_t { P03 = 3, P04 = 4, P05 = 5, P06 = 6, P07 = 7, P09 = 9, P11 = 11 };
  struct Cell { Op op; Entity entity; Timing timing; };
  struct Expected { bool applicable = true; const char* naReason = nullptr; /* per-part fields */ };
  [[nodiscard]] Expected expect(const Cell&);          // chains expect03, expect04, ... by the op's range
  [[nodiscard]] std::vector<Cell> cellsFor(Part);
  // CheckedWritesModel.h
  class CheckedWritesModel {
  public:
      using LogRec = CheckedWritesRig::RigLogRec;
      void reset(const std::vector<LogRec>& committedLog);                 // from rig.logRecords()
      [[nodiscard]] std::map<int64_t, rune::view::Verdict> verdicts() const;   // the reference judge, written independently of VerdictSource
      [[nodiscard]] std::vector<LogRec> acceptedRecords() const;
      [[nodiscard]] std::map<uint64_t, LogRec> latestAcceptedByKey() const;
      [[nodiscard]] std::map<uint64_t, std::pair<int64_t, uint8_t>> projection() const;
      [[nodiscard]] std::map<uint8_t, int64_t> countByTag() const;
      // extension points: 05 key index, 06 ids, 09 unique/check, 11 transactions
      using RuleFn = std::function<std::optional<rune::view::Verdict>(const LogRec&, const CheckedWritesModel&)>;
      void addRule(uint64_t viewObjectId, RuleFn);                          // test-only: std::function is allowed here
  };
  ```
  - **The model's judge** reads the change records (type 18) from the log to know each offset's set, and evaluates `TestRejectRule` from its own copy of the rule table. It shares no code with `VerdictSource`.

- [ ] **Step 1: Write the matrix.** `INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesMatrix, ValuesIn(cellsFor(Part::P04)))`. `TEST_P(CheckedWritesMatrix, Holds)` builds a `CheckedWritesRig` per cell, runs the op under the timing and the entity's observers, then asserts (a)–(g) of the Interaction matrix A against `CheckedWritesModel`. N/A cells `GTEST_SKIP()` with `naReason`.
  - The pin:
    ```cpp
    TEST(CheckedWritesMatrixCountRegression, CellCountsArePinned) {
        EXPECT_EQ(applicable(cellsFor(Part::P03)), 78u); EXPECT_EQ(skipped(cellsFor(Part::P03)), 2u);
        EXPECT_EQ(applicable(cellsFor(Part::P04)), 585u); EXPECT_EQ(skipped(cellsFor(Part::P04)), 15u);
    }
    ```
  - `CheckedWritesModel.ReferenceJudgeIsIndependent`: a hand-built log of 12 records with two change records → the expected verdict map is written out in the test.
- [ ] **Step 2: Rewire Part 03's matrix** onto `cellsFor(Part::P03)` and `expect03` (moved unchanged), and delete 03's own `CheckedWritesViewAckMatrixCountRegression.CellCountsArePinned`: its row is now in the shared pin above. Run: `ctest --test-dir build/root -R 'CheckedWritesViewAckMatrix' -j14 --output-on-failure`. Expected: the same 78 pass and 2 skip.
- [ ] **Step 3: Run.** `cmake --build build/root -j14 --target rune_checked_writes_matrix_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix|CheckedWritesModel\.' -j14 --output-on-failure`. Expected: 585 pass, 15 skip, the pin and the model test pass. Mutation checks (do each, then revert):
  - drop the cut in `pushOwnedSubscriptions_` → every `RejectedWrite × RawSubscription` cell is red;
  - make `isRejectedRecord` return false → every `OtherViews` cell with a rejection is red;
  - skip the map skip in the cleaner → every `RejectedOverAccepted` cell's (e) is red.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h broker/libs/node/tests/support/CheckedWritesModel.h \
          broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp broker/libs/node/tests/node/CheckedWritesViewAckMatrixTest.cpp \
          broker/libs/node/tests/support/CheckedWritesAckRig.h broker/libs/node/CMakeLists.txt
  git commit -m "test(node): shared checked-writes matrix (04: 585 cells; 03 folded in at 120-139) and the reference model" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 11: The model-checked stress (rig) and the cluster stress (ThreeNodes)

**Files:**
- Create: `broker/libs/node/tests/node/CheckedWritesModelTest.cpp` (in `rune_checked_writes_matrix_tests`), `broker/libs/node/tests/support/CheckedWritesStressDriver.h`, `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt` (`rune_checked_writes_stress_tests`: links `rune_node rune_replication rune_codec rune_view rune_client aeron::aeron GTest::gtest_main`, `TIMEOUT 1800`, modeled on the catalog stress target, `:268`)

**Interfaces:**
- Consumes: the rig (Tasks 6–9), the model (Task 10), `ThreeNodes` (`CatalogBrokerCluster.h:160-230`), `BrokerNode::onWorkerThreadForTest` (`BrokerNode.h:1553-1559`) and `onOwnerThreadForTest` (`:1467`).
- Produces:
  ```cpp
  class CheckedWritesStressDriver {
  public:
      CheckedWritesStressDriver(ThreeNodes& cluster, uint64_t seed);
      void setUp();                                     // typed topic replicas 3, TestReject on every node, a projection view, 3 clients
      void step();                                      // one seeded action (Interaction matrix D)
      void registerInvariant(std::string name, std::function<bool(CheckedWritesStressDriver&, std::string* why)>);
      [[nodiscard]] bool checkAll(std::string* failed);
      [[nodiscard]] uint64_t traceHash() const noexcept;
  };
  ```
- [ ] **Step 1: Write the model check** (`CheckedWritesModelTest.cpp`). `CheckedWritesModelRegression.SeededRigMatchesTheModel` covers seeds 1–200 × 250 steps, with the action table and the six named invariants of Interaction matrix C, checked after every step. `CheckedWritesModelRegression.SameSeedSameTrace` re-runs seeds 1–8 twice and requires equal `traceHash()`. On failure: `SCOPED_TRACE("seed=" … " step=" … " action=" …)`.
- [ ] **Step 2: Write the cluster stress** (`CheckedWritesStressTest.cpp`). `CheckedWritesStressRegression.SeededClusterHoldsTheInvariants` runs seeds 1–12 × 300 steps, with the faults of Interaction matrix D, the five Part 04 invariants registered by name, and `verdictUnknownDuplicates == 0` and `mismatches() == 0` on every node. Every wait is a bounded poll (10 s), and the deadline is the failure. On failure the seed is printed.
- [ ] **Step 3: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesModelRegression' -j14 --output-on-failure`. Expected: PASS.
  - `ctest --test-dir build/root -R 'CheckedWritesStressRegression' --output-on-failure`. Expected: PASS.
  - Mutation checks (each, then revert):
    - re-judge below K at activation (start the judge at the member cursor) → `SameVerdictEveryLeaderAndRefold` red;
    - emit markers from `appendedThrough()` instead of W → `MarkersTile` or `NoRejectedDeliveredOrFolded` red after a `failover()`.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesModelTest.cpp broker/libs/node/tests/support/CheckedWritesStressDriver.h \
          broker/libs/node/tests/integration/CheckedWritesStressTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "test(node): model-checked verdict stress on the worker rig (G8) and the seeded ThreeNodes stress driver" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 12: The real-broker slice (default shard runtime)

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` (in `rune_client_broker_tests`, `broker/libs/node/CMakeLists.txt:1731`)

**Interfaces:**
- Consumes: everything above. `TestReject` is installed on every worker of every node through `onWorkerThreadForTest` (a helper `installTestRejectEverywhere(node, topic, view, objectId, rule)` in the test file). There is no new production code.

- [ ] **Step 1: Write the tests.** Each starts its own broker(s) on `Runtime::Shard`. The topic is `vt {id uint64, v int64, tag uint8} key id`, created with Part 02's typed `defineTopicAndWait`, `TagEquals{9}` objectId 7, and the ordinary view `vt_v` (`createViewAndWait("vt_v", "id", 0, viewPlanOver("vt"), …)`). Every wait is `waitFor(…, 10s)`.
  - `CheckedWritesBrokerRegression.CoalescedBatchWithOneBadRecordShardRegression` (§15): an `All | View` producer sends 8 records in one batch, record 3 tag 9 → completions: 7 `Ok`, record 3 `BrokerError`/51 with `constraintIdOf == 0x00070001`; the next 8 all `Ok`; `getViewRowAndWait("vt_v", id3)` absent; a raw `subscribe` from 0 never sees id 3.
  - `CheckedWritesBrokerRegression.OldWritersOnAVerdictSourceNeverSeeRejectedRecordsShardRegression` (§15): a non-idempotent producer, an `acks=leader` producer, an `acks=none` producer and a session announcing version 11 each write tag 1 and tag 9 → every completion is today's (no 51); only the tag-1 records are visible in `vt_v` and to a consumer.
  - `CheckedWritesBrokerRegression.RejectedRecordAcrossALeaderCrashThreeNodesRegression`:
    - `ThreeNodes::start("cw04")`, `replicas 3`, `TestReject` everywhere;
    - 200 `All | View` sends (every 10th tag 9); `crash(leader)` after 100 completions;
    - every completion is `Ok` or 51, exactly the tag-9 ones are 51;
    - every node's index (read on its owner thread) holds the same offsets once it leads (move leadership to each in turn);
    - `vt_v` on the final leader equals the accepted set.
  - `CheckedWritesBrokerRegression.RestartKeepsVerdictsAndViewsThreeNodesRegression`: write, restart every node, re-install `TestReject` → no rejected record reappears for a consumer from 0, and `vt_v` is unchanged.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesBrokerRegression' --output-on-failure`. Expected: PASS. Then the whole binary: `ctest --test-dir build/root -R 'rune_client_broker|ClientFailoverBroker|ClaimBroker' -j6 --output-on-failure`.
- [ ] **Step 3: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
  git commit -m "test(e2e): verdict sources on real brokers - coalesced batch, old writers, leader crash, full restart" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 13: The alloc gate, the benches and the docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` (03's binary), `broker/benchmark/ViewFoldBench.cpp` (after `BM_ViewFoldReplace`, `:542-581`), `broker/benchmark/WorkerProduceLatencyBench.cpp` (beside `BM_WorkerProduceToPushRead_*`, `:321-336`)
- Modify: `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `docs/operations.md`, `docs/configuration.md`, `client-cpp/CLAUDE.md` (§2.1 Producer)

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateJudgingMarkersCutsAndTrailersAllocateNothingRegression`, on the rig with `TagEquals{9}` + `CapLiveKeys{1000}` + an ordinary projection, a raw and a filtered subscription:
  - warm up with 300 batches (1 and 16 records, `acks` 1 and 3, 10 % tag 9, markers every 20 ms);
  - then, under `rune::testing::AllocWindow w;`, 3,000 more batches of the same mix, including 50 fully rejected batches and 20 marker emissions;
  - `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Write the benches.**
  - `BM_ViewFoldVerdict/test_reject_cap`: the `BM_ViewFoldReplace` workload (`generateChurnOps`, `kBatchSizes`) driven through a `VerdictSource` with `TestReject{CapLiveKeys{1 << 20}}` over the projection plus a `count(*)` aggregate member, with no rejections (the judging path only). It reports `verdict_record_p50_ns`, `verdict_record_p99_ns` and `steady_alloc`.
    - Gate: `verdict_record_p50_ns − BM_ViewFoldReplace's record_p50_ns ≤ 1000` (§14 judging budget, with `TestReject` and a count member standing in for 09's unique + count) and `steady_alloc == 0`.
    - The stderr line is `[ViewFoldVerdict gate] … => PASS|FAIL`.
  - `BM_ViewFoldVerdict/rejecting_1pct`: the same with 1 % of records rejected, reported only.
  - `BM_WorkerProduceToPushRead_VerdictSource_256B`: `BM_WorkerProduceToPushRead_Disk_256B` with a `TestReject` member on the source. It reports the e2e p50/p99 and `verdict_lag_p50_ns`.
    - Gate: the e2e p50 minus the `Disk_256B` p50 of the same run ≤ `verdict_lag_p50_ns` + 2 µs (§14 "rises by no more than the verdict lag").
- [ ] **Step 3: Run the gate.** `cmake --build build/root -j14 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches** (baseline = this part's base commit on `main`, ABAB n=3, one session, same box; one prepare/restore):
  - `git worktree add "$TMPDIR/rune-cw04-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel`.
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ViewFoldReplace$|BM_ViewFoldAggregate$|BM_ViewFoldReplaceTxn|BM_ViewFoldRetractable|BM_ProducePath_|BM_WorkerProduce_Majority|BM_WorkerProduceToPushRead_Disk|BM_SessionAckSinkFlush' --benchmark_repetitions=3`, then the tip alone for `BM_ViewFoldVerdict|BM_WorkerProduceToPushRead_VerdictSource`.
  - **Gates:**
    - no regression on the ABAB set (the direct-store fold must keep today's cost, P4-13), judged by same-session medians against `BENCHMARKING.md`'s noise;
    - the two new gates above.
  - Borderline numbers are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under `flock /root/vm.lock` on `$RUNE_VM_HOST`, then `vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Write the docs.**
  - **`wire-protocol.md`**, a section "Control records 16–19":
    - the batch form and both payloads of C10, with byte tables;
    - "consumers never receive them";
    - the `View` ack trailer: `recordIndex` = the record's `offsetDelta` (= ordinal for Rune clients), the 4096 cap, an in-window `View` retry replays the original's verdicts once W passes it; `DuplicateSequence` for a retry outside the window on a verdict source.
  - **`client-cpp.md`:** per-record results on `View` acks (`errorCode` 51–53, `constraintIdOf`, Tier-2 first rejection, a `View` retry replays the original verdicts; `OpStatus::VerdictUnknown` when the retry fell outside the dedupe window).
  - **`operations.md`**, a section "Verdict sources":
    - markers and `W`;
    - the cleaner compacts only below `W`;
    - verdict lag;
    - delivery pausing while a constraint view builds;
    - a judge waiting for a missing view install;
    - a verdict view refolded after a crash that lost the marker tail (`verdictCheckpointsDiscarded`);
    - the index capacity and the stall alert.
  - **`configuration.md`:** `RUNE_VERDICT_INTERVAL_MS` (default 100).
  - **`client-cpp/CLAUDE.md` §2.1:** "a `View` ack on a verdict source carries per-record verdicts; no resend, no epoch bump".
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ViewFoldBench.cpp \
          broker/benchmark/WorkerProduceLatencyBench.cpp docs/api/wire-protocol.md docs/api/client-cpp.md \
          docs/operations.md docs/configuration.md client-cpp/CLAUDE.md
  git commit -m "bench/docs: verdict judging and consumer-latency gates, zero-alloc gate, control records and verdict docs" \
             -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c); stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`; CatalogGolden on `build/sub-codec`.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) `-R Regression -LE latency`, including `rune_checked_writes_matrix_tests` and `rune_checked_writes_stress_tests`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure` under `--repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 13 benches (recorded).
  - This part has no docker or VM e2e of its own (Part 05 creates `broker/docker/checked-writes-e2e.sh`). Its real e2e is Task 12 on the default shard runtime.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model. The reviewer leaves PR comments; the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. "Aeron-counter export of verdict-source stats" only if Part 08's SD13 issue does not already cover the C16 ids 3–7.
  2. Every recorded deviation (weakened or re-pinned assertion) that is not urgent.
  - The two-phase drop is not filed: it is Part 09's plan work (R04a).
- [ ] Hand over to Part 05 per the overview's acceptance list for 04:
  - a verdict source works end to end with `TestReject`: markers, the index, the consumer filter, other views skipping, the cleaner, refold below W, failover re-judging, `ConstraintSetChange` and the ack trailer;
  - the matrix, the model and the stress harnesses exist with the generic invariants.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §3 (the fold half):
    - prospective rows: Tasks 1–2;
    - "every verdict view computes … without writing; rules evaluated; all apply or none": Task 5;
    - "views that aren't verdict views fold after, skip rejected": Tasks 4 and 6;
    - "what a rejected record is" (no view, no consumer, the cleaner): Tasks 6, 7 and 8.
  - §8.4:
    - prospective rows and several verdict views: Task 5 (`OneOfTwoRejectsNoViewChanges`);
    - within a batch, one at a time: Task 5 and the matrix `SelfCollidingBatch`;
    - deletes never rejected: Task 5;
    - deterministic: P4-1, the mismatch alarm, `SameVerdictEveryLeaderAndRefold`;
    - no resource-dependent verdicts: P4-11, Task 5 `CapacityStallIsNeverAVerdictRegression`, Task 2 overlay overflow;
    - nothing pending: the judge runs below the LSO only (Task 6 duty order).
  - §8.5:
    - the marker and its triggers: Tasks 3 and 5 (P4-3);
    - replication and skip by readers: Task 3 (the readers sweep);
    - the bitmap: Tasks 4 and 6 (scan feed);
    - W: Task 5 `MarkerLedger`, Task 6 publish;
    - the cleaner (four rules): Task 8;
    - refold below / at W: Task 6;
    - failover: Task 6;
    - truncation: Task 6 `TruncationOfAnUncommittedTailChangesNoVerdict`.
  - §8.6: Task 7 (every push path, gaps, fail-closed), with other views in Task 6 and view reads unchanged (Part 01/03 barrier).
  - §8.7 mechanism:
    - change records and the set at each offset: Task 5;
    - failover taking the set from the log and waiting for installs: Task 6;
    - create (A, unjudged below, delivery waits), replace (old judges until caught up), drop: Tasks 5 and 6.
    - The catalog wiring is Part 09.
  - §8.8:
    - leader only, after commit, bounded by the LSO, verdict views first: Task 6;
    - `View` acked after the fold with verdicts: Task 9;
    - aborted transactions skipped: Part 01 through `FoldSkip.aborted`.
  - §11.3:
    - per-record verdicts and the trailer: Task 9;
    - "doesn't wait for the marker": Task 9 `WaitsForTheJudgeNotTheMarker`;
    - client results, no resend, pipelining: Task 9;
    - retries from the bitmap or parked: Task 9;
    - `constraintId` resolution helper: Part 09.
  - C16 counters 3–7: Tasks 5, 6 and 13 (P4-16).
  - SD4: Task 6 `HookFactsFollowTheTimeline` and Task 9 (4096-record ack).
  - SD16: unchanged queue (03); Task 9's waits keep it.
  - Shared infrastructure: Tasks 6–11.
- **Step scan.** Every test step names its tests and assertions with spec values (51, `constraintIdOf`, 4096, 32,824, 54/56/72 B, 100 ms, 65,536, 2²⁴). Code steps name files, signatures and placement. Bodies are given only for the judge's pass order and activation (Task 5) and the cleaner's coverage rule (Task 8), which the signatures and tests do not fix.
- **Type consistency.**
  - `VerdictSource`, `ConstraintSet`, `ConstraintSetMember`, `ConstraintSetTimeline`, `MarkerLedger`, `VerdictSourceStats` and `TestRejectRule`: Task 5, used in Tasks 6–13.
  - `RejectedOffsetIndex` (+ additions): Task 4, used in 5–9.
  - `Prospect`, `RowOverlay` (a stub in Task 1, filled in Task 2), `ProjectionFold`, `AggregateFold`: Tasks 1–2, used in 5.
  - `ViewKey::fromU64` / `asU64` (R07d): Task 1, used by Parts 05 and 06; Part 07 extends `ViewKey` under the same names.
  - `VerdictSourceStats::verdictCheckpointsDiscarded`: Task 5, asserted in Task 6, documented in Task 13 (never Part 01's `checkpointsDiscarded`).
  - The cleaner atomics on `PartitionReplica`: written from Task 6 (`publishVerdictCleanerBound_`), read from Task 8 (`cleanerSwapBound`, `LogCleanerAgent`).
  - `OpStatus::StoredIdUnknown = 13` (reserved) and `VerdictUnknown = 14`: Task 9 (R-X2).
  - `control_record::*`: Task 3, used in 5, 6 and 8.
  - `VerdictCleanState`: Task 8.
  - `VerdictAckArena`: Task 9.
  - `CheckedWritesRig` grows in Tasks 6–9.
  - `Op` / `Entity` / `Timing` / `Part` / `Cell` / `cellsFor`: Task 10.
  - `CheckedWritesModel`: Task 10, used in 11.
  - Names match C9–C12 verbatim. The additions are listed once.
- **Review Focus.** Each of the five has its named test in its owning task (6, 7, 8, 8, 9).
- **Proportion.** The plan fixes decisions, placements, signatures and assertions. The judge, the overlay store, the cleaner integration, the push cuts and the rig bodies are left to the implementer within the stated rules.
