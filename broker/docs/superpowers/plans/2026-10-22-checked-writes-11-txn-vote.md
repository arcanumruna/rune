# Checked Writes — Part 11: the Transaction Vote — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transactions and explicit atomic batches work on every verdict source. A transaction gets atomic verdicts across topics and views: every verdict partition it touches votes before the commit is decided, any rejection aborts the whole transaction, and `commitTransaction()` returns the code and the constraint id. No read or consumer ever sees part of a transaction on one partition (level 3a). This part also lifts every pre-G refusal and closes the program with the §16.12 docs cross-check.

**Architecture:**
- **The vote (C18).** On `EndTxn(commit)`, the coordinator keeps the transaction `Ongoing` and sends a `TxnVote` to the leader of every partition of the transaction that might vote. The leader decides from its log whether it votes. If it does, it appends a `TxnJudge` control record (type 19) at J and answers once its judge has judged the transaction at J.
- **Judging at J.** A voting transaction's records are *deferred*. The judge runs ahead of them to J, judging other records at their own offsets. At J it judges all of the transaction's records together, in a chained after-image overlay. A yes keeps the overlay as the row locks. A later record that touches a locked row pauses the judge.
- **Finality.** The commit marker re-applies the transaction's records through the normal apply path in one worker step. An abort drops the overlay. Tables never hold provisional rows, so reads, scans and checkpoints see before-images.
- **Atomic batches.** A batch with the new header bit is judged as a unit at its own offset with the same overlay.
- **Level 3a on every partition.** It comes from a visibility bound in `RcState` and a read fence on view instances.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices, seeded model checks), Google Benchmark (`build/rel/broker/rune_broker_benchmark`), CMake/Ninja, SBE 1.40.2 (internal ring), real Aeron (embedded driver over `aeron:ipc`) for the worker rig, in-process `OneBroker` / `ThreeNodes` for the e2e slice and the cluster stress, docker on the shared VM for the e2e leg.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`. This part covers §9b (Part G) in full, the §11.2 transaction and atomic-batch rows, the §13 Part G stats, the Part G bullets of §15, the §15 invariant "a transaction is either applied on every partition it touched or on none", and §16.12 (the docs cross-check). The binding cross-part contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C1, C6, C7, C8, C9, C10, C15, C16, C18, SD4, SD14, SD16, the shared test infrastructure). Executors read the spec, the overview, the controller rulings restated below, plans 01, 03, 04, 05, 06, 08 and 09 (their "Contract additions"), and this file.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it. The site lives in the separate `rune-site` repo (Task 16 only).
- **Branch.** Cut `feature/checked-writes-11-txn-vote` from the latest `main` (`git fetch` first), after Parts 01–10 have merged. Merge `main` in again before review.
- **Preconditions.** Run these before Task 1. Each must match; otherwise stop and tell the controller.
  - `grep -n "kTxnJudge = 19" broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (1 hit, 04)
  - `grep -n "kVerdictPlan = 20" broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (1 hit, 09)
  - `grep -n "IntentInTxnOrAtomicBatch\|TxnOnConstrainedSource" broker/libs/node/include/rune/node/CheckedWriteHook.h` (≥ 2 hits, 03)
  - `grep -n "struct RcVerdictHooks" broker/libs/node/include/rune/node/RcState.h` (1 hit, 04)
  - `grep -n "class RetainedVerdictPlans" broker/libs/node/include/rune/node/VerdictSource.h` (1 hit, 09)
  - `grep -n "class StatsSink" broker/libs/node/include/rune/node/StatsCollect.h` (1 hit, 08)
  - `grep -n "struct OverlayStore\|class OverlayStore" broker/libs/view/src/AggFold.cpp` (≥ 1 hit, 04)
  - `grep -n "createLedgerTopic" broker/libs/node/tests/support/CheckedWritesRig.h` (1 hit, 09)
  - `grep -n "kAttrAtomic" protocol/include/rune/protocol/RecordBatch.h` (0 hits)
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root` is Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - The others are `build/sub-client-cpp` (client suites), `build/sub-codec` (CatalogGolden), `build/sub-ql`, `build/rel` (Release, `BUILD_BENCHMARKS=ON`), `build/asan`, `build/tsan` and `build/client-tsan`.
- **Line numbers** cite `main` before Parts 01–10, which edit most of the files named here. Anchor on the named function or member, never on the number alone.
- **SBE.** Regenerate the internal-ring flyweights from `protocol/` with a local jar: `java -Dsbe.target.language=cpp -Dsbe.output.dir=include -jar "$SBE_JAR" schema/rune-internal-ring.xml`. Commit the regenerated `protocol/include/rune_protocol_internal_sbe/*`. Never commit the jar.
- **VM.** Docker e2e and borderline benches run only on the shared VM: `ssh root@$RUNE_VM_HOST`, every command under `flock /root/vm.lock`. Prepare the clean box once for benches and restore it afterwards (`broker/bench/BENCHMARKING.md`). Never use local Mac docker.
- **Commits.** Use `git add <paths>` only, never `-A`, `.superpowers/` or build logs. Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes, and nothing deploys the site.

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

- **R11a (owner-flagged).** `TxnConflict = 59`: a transaction votes no when a newer write to one of its keys lands before its `TxnJudge` (P11-3). Plain writers never wait behind a transaction.
- **R11b (binding).** Every fold, live, refold, rebuild or leadership scan, applies a transaction's records at its commit marker, in the same order (P11-0).
- **R11c (binding).** A coordinator failover never loses vote candidates: on recovery every partition of the transaction's persisted partition set is a candidate (P11-11).
- **R11d.** The recorded calls are accepted (`firstOffset` in `TxnJudge`, after-images, the read fence, 39 for old clients, `TxnCommitResult`, read_uncommitted as read_committed, the `TxnJudge` cleaner bound, the 05/06 matrix edits, the local-only site commit); distributed lock waits resolve by the transaction timeout, and a deadlock-detection follow-up issue is filed (Finish).
- **R05b.** Part 11 defines the C8 atomic-batch header bit. Parts 05, 06 and 09 do not (P11-6).
- **R03c / P3-2.** The client never sets `View` on a transactional send. The broker strips `View` from a transactional batch. Transactions get their verdicts through the vote and `commitTransactionAndWait`.
- **SD14 / SD4.** `txn.judged.records.max` is the broker setting `NodeConfig::txnJudgedRecordsMax` (env `RUNE_TXN_JUDGED_RECORDS_MAX`, default 4096). A `View` batch on a verdict source holds at most 4096 records (`kMaxJudgedBatchRecords`).
- **R08f.** Part 11 adds stat ids 11–20 through `StatsCollect.h` (`collectTopicPartitionStats`).
- **R09a.** Control record types 16–20 are taken. A new type would start at 21. This part needs none: it fills Part 04's reserved `kTxnJudge = 19` (P11-1).
- **R04a / P9-6 / P9-8.** A dropping verdict view's plan stays installed, and keeps judging, until W passes its drop record. A retained plan (type 20) lets a restarted replica judge with it.
- **R04c / P4-2 / P4-3 / P4-4.** These define K, W, `judgedThrough()`, the marker triggers and the checkpoint gate. This part redefines the judge's contribution to K and `judgedThrough()` (P11-2) and amends the checkpoint gate (P11-8).
- **R04d.** The shared `Timing` enum is append-only. This part appends 9 values (Interaction matrix).
- **R04e / R04g.** A `View` retry inside the dedupe window replays the original verdicts. An atomic batch's replay is its single entry (P11-6).
- **R05e / P5-4.** Intents meet `KeyIndexActivating` (retriable `NotLeader`) before the key index's change record. That holds inside transactions too.
- **R06h / P6-12 / P6-13.** Key-0 creates need `All`, are exempt from the `View` requirement inside a transaction, and are still judged by the key index. Ids of an aborted transaction, including one aborted by a no vote, are burned.
- **SD16.** A `View` waiter behind an open (or voting) transaction waits for it. It still does: `judgedThrough()` never passes an undecided transaction (P11-2).
- **Deletes are never rejected** (§8.2, P5-3, P9-2). This holds inside a transaction and an atomic batch: a tombstone's own verdict is always accepted, and a transaction can be rejected only by its non-delete records, by `removeExisting` of an absent key (53), by `TxnTooLarge` or by `TxnConflict` (P11-3).
- **Requirements apply per write** (§11.2, P5-12). The vote judges plain transactional writes from any producer and any client version. Only the record shapes that ask the broker for something carry requirements (P11-14).

## Part rulings (P11; binding for executors, recorded so the reviewer can overturn any)

- **P11-1 — Who votes, and the `TxnJudge` record.**
  - **The decision is the partition's** (spec G.2). The leader of partition p votes for transaction T (producer `pid`, epoch `e`) iff p is a verdict source and at least one of these holds:
    - (a) the set active at T's first record on p (`constraintSets.setAt(first)`) has a `Unique`, `Check` or `TestReject` member;
    - (b) the latest logged set, when the vote arrives, has one;
    - (c) T sent an explicit-key intent on p.
  - Rule (b) is added to the spec's (a) and (c). It covers a constraint created between T's first record and its commit, which the spec's coordinator-side check alone cannot catch on the partition. *Flagged.*
  - **The vote-time scan.** The leader scans T's batches on p, from `rc->tracker().firstOffsetOf(pid)` to the LEO, filtered by `pid` and the transactional bit. The scan reads at most `kVoteScanBytesPerPass = 1 MiB` per duty pass and records the record count, whether any record has an explicit-key intent, and T's last offset. A partition with no open transaction of `pid`, or one that does not vote, answers yes without writing anything.
  - **`TxnTooLarge`.** A voting partition whose count exceeds `txnJudgedRecordsMax` answers no with `TxnTooLarge` (55) at once, without a `TxnJudge`. The answer is reproducible from the committed log, so a retried vote and a new leader give the same answer.
  - **The record.** Otherwise the leader waits until the HW is above T's last offset, then appends `TxnJudge` at J through `PartitionReplica::appendControlBatch(span, /*acksAll=*/false)`.
    - Payload: `[producerId i64][producerEpoch i16][firstOffset i64]`, after C10's `[version u8 = 1]`. The `firstOffset` field is a contract addition.
    - The answer goes out only after J is committed and judged. A truncated J was therefore never answered, and a retried vote appends a new one.
  - **Idempotence.** A vote for `(pid, e)` is answered from the `TxnJudgeState` entry whose J is above T's current first offset. While that J is still being judged, the vote parks with it.
- **P11-2 — Judging order, deferral and `judgedThrough()`.**
  - **Classification.** On a verdict source, when the judge reaches the first batch of an undecided transaction T, it classifies T from the committed log:
    - T has a committed `TxnJudge` J: T is **deferred**. The judge records T in `TxnJudgeState`, skips T's batches, and keeps judging other records past them up to J.
    - T has a committed marker and no J: T is **ordinary**. The judge defers T to its marker M, runs ahead to M, and judges and applies T's records there, in offset order among themselves, in one step (an aborted T is dropped at its abort marker). Records superseded per P11-3a apply nothing.
    - Neither: the judge **waits** at T's first record, as it waits at the LSO today.
  - **The judge bound** is the HW, no longer the LSO. Waits come only from the classification above, the pause rule (P11-4), overlay exhaustion (P11-5) and Part 04's own waits.
  - **Two positions.**
    - `VerdictSource::judgeCursor()` is where the judge stands. It may run ahead of deferred transactions.
    - `judgedThrough()` becomes `min(judgeCursor, state.txn.earliestUndecidedFirst())`.
  - **What uses `judgedThrough()`.** Everything Part 04 bounds by it is unchanged: consumers, the `View` ack, markers (`[emitFrom, judgedThrough)`), the other views of the partition, and the judge's contribution to K. A deferred transaction therefore holds delivery, markers, W and `View` acks of later writes until it is decided, which is SD16's behaviour.
- **P11-0 — One fold order everywhere (R11b, binding).** Every fold path applies a committed transaction's records **at its commit marker**, in offset order among themselves, and drops an aborted one's at its abort marker:
  - the live judge and the live fold of every other view;
  - a refold (Part 01's discard-and-refold), a rebuild (`driveRebuild`, a joining member, a replace candidate), a checkpoint replay, and a replica's leadership scan and activation replay.
  - Part 01's `FoldSkip.aborted` stays as a defensive skip; a transactional batch is never applied at its own offset by any path.
  - A fold that meets a transactional batch records `{pid, first}` in the instance's `TxnSpanFence` (contract addition 13) and skips it; at the pid's commit marker it reads the pid's batches back from `[first, M)` and applies them.
  - *Consequence.* The view state is a function of the log alone, so `last` / `lastNonNull` and every verdict are identical however the view was built. Pinned by `CheckedWritesTxnDeterminismRegression.LiveEqualsRefoldEqualsRebuildUnderInterleavedTxnsRegression` (Task 8) and the model clause `FoldPathsAgree` (Task 13).
  - *Checkpoints.* A view or member never pushes a checkpoint at an O that lies inside an open span of its fence (`first < O ≤ M`), except P11-8 (a)'s run-ahead push of a verdict member, which backfills from J's `firstOffset`. Pushes refused this way count `vetoedCheckpointPushes`.
- **P11-3 — Write-write conflict: `TxnConflict` (59).** When T is judged at J, let `last_T(k)` be T's last record on source key k. T votes no with the new code `TxnConflict = 59` (constraintId 0, failing offset = that outside record) iff some record r of another writer exists such that all of these hold:
  - its source key is a key k that T writes;
  - `offset(r) > last_T(k)`;
  - r is accepted and applied before J in judge order. That means r is non-transactional and judged accepted, or belongs to an ordinary transaction committed below J, or belongs to a deferred transaction whose `TxnJudge` is below J and which committed (the pause rule guarantees its marker is known first).

  *Why.* Without the rule, the view applies T after r while the log, the cleaner and every consumer take r as the key's latest. With it, judge order and offset order agree on every key's latest accepted record, which is what consumers and compaction depend on. (Folds agree with each other by P11-0 regardless.)
- **P11-3a — Superseded records of a transaction without a vote.** When a transaction is applied at its marker without having voted (an ordinary transaction, on any partition), a record r of it is **superseded** iff an accepted record of another writer on the same source key, with an offset above `last_T(k)`, was applied before M (the same test as P11-3). A superseded record applies nothing. It cannot be rejected (an ordinary transaction has no constrained member and no explicit-key intent), so this only keeps the latest record per key equal to the log's. The marker-without-vote path (P11-7) uses P11-3's no instead.

  *Detection.* At J the judge builds T's key set (at most `txnJudgedRecordsMax` keys, a per-worker hash set reserved at `bindTxn`). It then scans `(T.first, J)` for records whose key hits the set, at most `kVoteScanBytesPerPass` per pass. The judge cursor stays at J until the scan and the judgement are complete.

  *Flagged:* §11.1 lists no code for this. It is an optimistic-concurrency abort, like Postgres SERIALIZABLE's "could not serialize".
- **P11-4 — Provisional rows are after-images. Locks and the pause.**
  - **Judging T at J.** The judge reads T's records from `[T.first, J)` in offset order. For each record it prepares every member of `setAt(J)` with a chained `TxnOverlay` (C9.3 prepare with a trailing `chain`): reads go overlay-first, and the record's effect is written into the overlay, so the next record of T sees it. It then evaluates the member rules exactly as `judge()` does. The first failing record ends the judgement with `(code, constraintId, offset)`.
  - **The tables never change at J.** They keep the before-images. On a yes, the overlay stays held. Its rows are T's **locks**. On a no, the overlay is released at once (spec G.2 3.v, "puts the before-images back": there is nothing to put back).
  - **The pause.** Every later record is prepared with the partition's `TxnLockSet`. A prepare that reads or writes a row held by a locked overlay returns `PrepareStatus::Locked`. The judge then pauses on that record, records nothing and counts `foldPausedOnTxnLockNs`.
  - **The marker.** A locked transaction is finalized in the first duty pass where its marker is below the HW, before any further judging, even when the judge cursor stands below the marker.
    - Commit: every one of T's records is re-applied through each member's normal apply path (prepare plus apply on the direct store), in offset order, in one worker step. The overlay is then released.
    - Abort: the overlay is released.
  - **Why finalizing out of cursor order is safe.** Every record between J and the cursor that touches T's rows paused before the cursor. So applying T at the marker or at the cursor gives the same state. *This is the builder's latitude under §18 ("the layout of before-images and row locks").*
- **P11-5 — The overlay pool.**
  - **Size.** `WorkerViewSet` owns `TxnOverlayPool txnOverlays_` with `kTxnOverlaysPerWorker = 8` overlays. Each is reserved for `txnJudgedRecordsMax × rowsPerRecord` rows of the largest source on the worker. The reservation is cold: it happens at verdict member install and at `setTxnJudgedRecordsMax`.
  - **Exhaustion.** A judgement at J that finds no free overlay waits at J and counts `txnOverlayWaits`. A full overlay is `PrepareStatus::CapacityExhausted`, which stalls the judge with an alert. Neither is ever a verdict (§8.4).
  - **Deadlocks.** A distributed lock wait is a cycle of two transactions holding rows on two partitions in opposite J order. It resolves only by the transaction timeout (the spec's design, §G.4). *Flagged.*
- **P11-6 — Atomic batches (spec G.6, R05b).**
  - **The bit.** `rune::protocol::batch::kAttrAtomic = 0x0080` (bit 7 of the batch attributes, unused today). Every Tier-2 batch (`Producer::batch`, `BatchBuilder`) sets it. A transactional batch ignores it (the transaction is the unit). An older broker ignores bit 7.
  - **Judging.** On a verdict source, the judge judges an atomic batch as a unit at its own offset. Its records are prepared through a chained per-worker `batchOverlay_` and evaluated in order.
    - All pass: they are applied through the normal path in the same step.
    - Any fails: every record of the batch goes into `RejectedOffsetIndex` with the first failing record's `(code, constraintId)`, and nothing applies.
  - **Locks.** Records that touch locked rows pause the judge.
  - **Cap.** An atomic batch on a verdict source holds at most `txnJudgedRecordsMax` records. A larger one is refused at the hook with `InvalidRequest` (`HookRefusal::AtomicBatchTooLarge`).
  - **Ack.** A `View` ack of an atomic batch carries exactly one trailer entry, the first failing record's `(recordIndex, code, constraintId)`, or none. Its client completion is `BrokerError(code)` with `reserved0 = constraintId` and `value` = that record's offset.
- **P11-7 — A commit marker without a `TxnJudge` (spec G.3).** The judge meets T as ordinary. If T "should have voted" (P11-1 (a), (b) or (c) on the log), the judge instead defers T to its marker M and judges it there, all or nothing, with a chained overlay, the conflict rule and the pause.
  - **Pass:** T applies.
  - **Fail:** every record of T on this partition is rejected through `RejectedOffsetIndex::insertRun` (a cold, out-of-order insert). Those offsets lie below the run-ahead entries but at or above `judgedThrough()`, so the next marker covers them.
  - Either way the judge counts `txnMarkerWithoutVote` and logs an error. The rules still hold on the partition; only cross-partition atomicity is lost.
- **P11-8 — Checkpoints of persisted verdict members.**
  - **(a) During run-ahead** (a deferred T below the cursor, its J ahead), a member may push its checkpoint at its cursor O. P4-4's gate "a marker covers `foldCursor`" is waived there, and the pass cap is `lastPushedO + checkpointCap`.
    - A crash before the covering marker leaves O above K, so P4-4's activation rule discards the checkpoint and refolds: correct, and rare.
    - After T decides, the markers cover O. A later restart replays from O and **backfills** T: when the member's unjudged replay meets a `TxnJudge` whose `firstOffset < O` and whose transaction committed (not in `RcState::aborted()`), it first applies that transaction's records in `[firstOffset, O)`. Order is safe by P11-3.
  - **(b) While a locked T has J below O, no member checkpoint is pushed.** The tables exclude T, and a replay from O would never revisit J. The pass cap may stall the judge meanwhile, but the marker finalizes T out of cursor order (P11-4), so nothing deadlocks.
  - **(c) The cleaner keeps a `TxnJudge`** while it lies at or above `txnJudgeBound`, the minimum `lastDurableO` over the source's persisted verdict members (published by the worker like W). `VerdictCleanState::keepsControlRecord(offset, 19)` returns `offset >= txnJudgeBound`.
- **P11-9 — Level 3a everywhere (spec G.5).** The spec's literal mechanism, "the fold never ends a step inside", cannot hold for a persisted view whose pass cap is smaller than the transaction. Two mechanisms give the same guarantee, that no read sees part of a transaction:
  - **Apply at the marker** (P11-0) makes a transaction visible in the step that applies its marker. A committed transaction whose marker lies past a later open transaction's first record simply waits whole below the LSO; reads are never held for the later transaction.
  - **The read fence.** When a marker's application exceeds the instance's pass cap (a persisted view, a large transaction), or an atomic batch is split by it, the application continues over several passes. Meanwhile `viewRowGate_` holds the instance's reads (`GetViewRow`, `GetViewRowK`, `ScanViewRows`) like the fold barrier does, and no checkpoint is pushed. `TxnSpanFence` has 64 span slots; an overflow makes the fold wait at the transactional batch that does not fit until a span closes (never a skip).
- **P11-10 — Consumers.** On a verdict source a `read_uncommitted` subscription is served as `read_committed`: the push paths filter aborted batches there too (spec G.5 "On these partitions, read_uncommitted behaves the same way"). `Worker::effectiveIsolation_(const PartitionWriteState*, uint8_t isolation)` sits next to `verdictBound_`.
- **P11-11 — The coordinator (`TransactionCoordinationService`).**
  - **Candidates.**
    - **Which partitions.** A partition of T is a vote candidate iff `voteCandidate_(topicId)` (BrokerNode's catalog predicate: the topic has `keyFeatures.keyIndex`, or a live view over it is unique or constrained) holds now, or held at T's first `AddPartitions` for that partition (`voteCandidatesAtAdd_`, per producer id).
    - **Recovery (R11c, binding).** A transaction loaded by `load_` (coordinator failover or restart) has no `voteCandidatesAtAdd_` entry: **every partition of its persisted partition set is a candidate**. A partition with nothing constrained answers yes at once without writing (P11-1), so the cost is one round trip per such partition, only for transactions that span a coordinator failover. No candidate is ever lost.
  - **Vote gate.** `EndTxn(commit)` on an `Ongoing` transaction with at least one candidate:
    - creates a `VoteGate`, sends one `TxnVote` per candidate through `ClientGateway::sendTxnVote`, persists nothing and defers the answer;
    - transactions with no candidate take today's path unchanged, with no extra round trip.
  - **Outcomes.**
    - **All yes:** today's commit path runs: `coordinator_.endTransaction(commit)`, PrepareCommit, the gate, the markers. The EndTxn answer goes out when the markers land, as today.
    - **The first no:** the gate aborts through `coordinator_.endTransaction(abort)`, PrepareAbort and the abort markers. The deferred EndTxn is answered, once the markers land, with the no's code and a v12 trailer `[constraintId u32][failingOffset i64]`. Votes still out are ignored.
    - **A transport error** (`NotLeader`, a link loss): that vote is re-routed and re-sent with the marker backoff (10 ms doubling to 1 s).
  - **Edge cases.**
    - **Timeout.** The transaction timeout is the voter timeout. `coordinator_.doWork`'s timeout abort moves the transaction to PrepareAbort, the gate goes stale, and the deferred EndTxn is answered with what `endTransaction(commit)` returns then (19 `InvalidTxnState`, today's behaviour for a timed-out transaction).
    - **A repeated `EndTxn(commit)`** while the gate is open becomes the gate's answer target. The earlier request is answered `ConcurrentTransactions` (27, retriable).
    - **An `EndTxn(abort)`** while the gate is open: the gate is cancelled, its request is answered 27, and the abort runs as today.
    - **Unload.** Coordinator failover drops every gate, and each deferred EndTxn is answered `NotCoordinator`, as `dropDeferred_` does today. The reloaded coordinator recovers the transaction as `Ongoing`, the client retries, and the partitions answer from their `TxnJudge` (spec G.2 "Votes are idempotent").
  - **Remembered no.** `lastVoteNo_[pid] = {epoch, code, constraintId, failingOffset}` answers a later `EndTxn(commit)` of the same producer and epoch while the transaction is PrepareAbort or CompleteAbort with the same no, instead of 19. It is cleared by the producer's next `AddPartitions`.
  - **Old sessions.** A session below client version 12 gets `InvalidRequest` (39) for every no-vote code (51–53, 55, 59), with no trailer. Its client then aborts as for any refused EndTxn (`Verdict::Reject`), and the abort succeeds (same direction, CompleteAbort).
- **P11-12 — Rows final before the answer (spec G.7, second bullet).** On a partition that voted yes, `executeAppendControlRecordWire_` parks the marker's answer until the HW passes it **and** the judge has finalized the transaction (P11-4). So a commit's OK always means every voting partition has made the rows final, for every producer, not only `View` ones. A judge stall delays the commit's answer and never drops it.
- **P11-13 — The client.**
  - **Classifying the answer.** `CoordinatorLink::classify(ReqKind::EndTxn, code)` returns the new `Verdict::VotedNo` for `isTxnVoteCode(code)` (51, 52, 53, 55, 59). The transaction is already aborted on the broker, so `onEndTxnAnswer_` returns to `Ready` (`endDone_`) and completes the commit with `OpStatus::BrokerError`, `errorCode = code`, `reserved0 = constraintId`, `value = failingOffset`. This holds for a re-sent commit too: the answer is definite.
  - **The blocking form.** `commitTransactionAndWait` now returns `TxnCommitResult`. It is `ok` on success, carries the no vote's fields, and still throws for every other failure, as today.
  - **Local refusals.** Part 05's local refusal "intents inside a transaction need Part G" is removed, and so is "intents need acks including View" for a transactional producer (R03c).
- **P11-14 — Lifting the pre-G refusals (Part 03's C8 rules, as amended by 05/06).** These ship last (Task 12), once the vote works end to end.
  - Rule 6 (`IntentInTxnOrAtomicBatch`) and rule 7 (`TxnOnConstrainedSource`) are deleted from `evaluateRequirements`. Their `HookRefusal` values stay (the counter array is indexed by them) and are documented "unused since Part 11".
  - Rule 5 becomes `explicitKeyIntent && (!facts.idempotent || (!acksView(facts.acks) && !facts.transactional))`. A transaction gets its verdict from the vote, and an atomic batch still needs `View` to learn it.
  - Rule 8's `KeyZeroCreateNeedsView` becomes `topic.constrainedSource && !acksView(facts.acks) && !facts.transactional`.
  - New rule 10: `facts.atomicBatch && topic.verdictSource && records > txnJudgedRecordsMax` → `AtomicBatchTooLarge`.
  - `WriteFacts::atomicBatch` comes from the header (`attrs & kAttrAtomic` and not transactional) in `produceCore_`.
- **P11-15 — Stats (C16 11–20, R08f).**
  - `TxnJudgeStats` lives in `TxnJudgeState`. `collectTopicPartitionStats` appends:
    - 11 `txnVotesYes`, 12 `txnVotesNo` (every no, including 59);
    - 13–16, the no votes for 51, 52, 53 and 55;
    - 17 `txnVoteLatencyNsMax` and 18 `txnVoteLatencyNsTotal`, measured from vote receipt to answer;
    - 19 `foldPausedOnTxnLockNs` and 20 `txnMarkerWithoutVote`.
  - `votesNoConflict`, `txnOverlayWaits`, `lockPauses`, `checkpointBackfills` and `vetoedCheckpointPushes` are counters without a stat id (C16 has no free id below 32). They are logged, readable through `Worker::txnStatsForTest`, and counted in `votesNo`.
- **P11-17 — Requirements per write, restated for transactions and atomic batches.**
  - Plain transactional upserts and deletes on any topic: no requirement beyond today's transactional producer (any client version).
  - Explicit-key intents in a transaction: an idempotent producer (every transactional producer is).
  - Key-0 creates in a transaction: `All` (a transaction always has it).
  - Explicit-key intents in an atomic batch: idempotent and `View`.
  - Plain records in an atomic batch: none.

## Contract additions (none redefines a contract type)

1. **C1:** `ErrorCode::TxnConflict = 59` (`Protocol.h`, after `ViewUniqueInvalid = 58`). `errorCodeName` gains "TxnConflict", its `static_assert` moves to 59, and Part 03's old-session mapping (P3-5) covers 51–59. `[[nodiscard]] constexpr bool isTxnVoteCode(uint16_t code) noexcept` is true for 51, 52, 53, 55 and 59.
2. **C8 header bit (R05b):** `inline constexpr int16_t kAttrAtomic = 0x0080;` in `rune::protocol::batch` (`RecordBatch.h:31-36`).
3. **`EndTxnResponse` v12** (`protocol/include/rune/protocol/client/TxnMessages.h:425`):
   - members `uint32_t constraintId = 0; int64_t failingOffset = -1; bool hasVote = false;`;
   - `static constexpr std::size_t ENCODED_SIZE_V12 = 38;` (header 24, body 14 = errorCode 2 + constraintId 4 + failingOffset 8);
   - `std::size_t encodeFor(uint8_t* buf, int32_t clientVersion) const noexcept;` writes the 26-byte frame below version 12 or when `!hasVote`;
   - `decode` reads the trailer iff `blockLength >= 14`.
4. **C18 cluster plane** (`ReplicationMessages.h`, after `WriteTxnMarkerResponseMsg`):
   - `TxnVoteMsg`, `TEMPLATE_ID = 13`, `BLOCK_LENGTH = 40`: `coordinatorNodeId i32 @0, coordinatorEpoch i32 @4, token i64 @8, topicId i32 @16, partition i32 @20, producerId i64 @24, producerEpoch i16 @32, pad @34, leaderEpoch i32 @36`.
   - `TxnVoteResponseMsg`, `TEMPLATE_ID = 14`, `BLOCK_LENGTH = 48`: `token i64 @0, topicId i32 @8, partition i32 @12, producerId i64 @16, coordinatorEpoch i32 @24, producerEpoch i16 @28, errorCode i16 @30 (transport: 0, NotLeader, InvalidTxnState), code u16 @32 (0 = yes), pad @34, constraintId u32 @36, failingOffset i64 @40`.
   - Both have `encode(uint8_t*, std::size_t) noexcept` and `static bool decode(const uint8_t*, std::size_t, T&) noexcept`, shaped like template 11's.
   - The transactional id is not carried: the partition never needs it. *Flagged* against G.2's message sketch.
5. **C10 `kTxnJudge` payload:** `[producerId i64][producerEpoch i16][firstOffset i64]` (P11-1). In `ControlRecordLayout.h`:
   - `struct TxnJudgeRecord { int64_t producerId; int16_t producerEpoch; int64_t firstOffset; };`
   - `inline constexpr std::size_t kTxnJudgeBatchBytes` (the exact batch size);
   - `std::size_t buildTxnJudge(std::span<uint8_t> out, const TxnJudgeRecord&) noexcept;` (0 = too small);
   - `[[nodiscard]] std::optional<TxnJudgeRecord> parseTxnJudge(const uint8_t* batch, std::size_t len) noexcept;`.
6. **Internal ring (C18):**
   - **The op.** `WorkerOp::Type::TxnVote = 28` (SBE `OpType TXN_VOTE = 28`) uses `correlationId` = token, `producerId`, `producerEpoch`, `roleEpoch` = target leader epoch + 1 (0 = unchecked) and `sessionId` = origin node id (−1 = local).
   - **The return.** `WorkerReturn::Type::TxnVoteResult = 8` (SBE `ReturnType TXN_VOTE_RESULT = 8`) reuses existing scalars, so `ProtocolRegression.WorkerReturnLayoutUnchanged` stays pinned:
     - `correlationId` = token;
     - `errorCode` = transport status;
     - `recordCount` = the vote code (0 = yes);
     - `newNextOffset` = constraintId;
     - `baseOffset` = failingOffset;
     - `lastOffset` = J (−1 = none);
     - `sessionId` = origin node.
7. **C7 `TxnJudgeState txn;`** (`broker/libs/node/include/rune/node/WriteState.h`):
   ```cpp
   enum class TxnPhase : uint8_t { Scanning, AwaitJudge, Deferred, Judging, Locked, Finalized };
   struct TxnVoteAnswer { uint16_t code = 0; uint32_t constraintId = 0; int64_t failingOffset = -1; };
   struct TxnJudgeStats { uint64_t votesYes = 0, votesNo = 0, votesNoConstraint = 0, votesNoKeyExists = 0,
       votesNoKeyNotFound = 0, votesNoTooLarge = 0, votesNoConflict = 0, voteLatencyNsMax = 0, voteLatencyNsTotal = 0,
       foldPausedOnTxnLockNs = 0, markerWithoutVote = 0, txnOverlayWaits = 0, lockPauses = 0, checkpointBackfills = 0,
       vetoedCheckpointPushes = 0; };
   struct DeferredTxn { int64_t producerId = -1; int16_t epoch = -1; int64_t firstOffset = -1; int64_t lastOffset = -1;
       int64_t judgeOffset = -1; int64_t markerOffset = -1; bool commit = false; int32_t records = 0; bool explicitIntent = false;
       TxnPhase phase = TxnPhase::Scanning; TxnVoteAnswer answer{}; int32_t overlay = -1; int64_t voteReceivedNs = 0; };
   struct ParkedVote { int64_t token = 0; int32_t originNode = -1; int64_t producerId = -1; int16_t epoch = -1; };
   class TxnJudgeState {
   public:
       static constexpr std::size_t kMaxTxns = 1024, kMaxParkedVotes = 256;
       void reserve();                                                                       // cold, at state creation
       [[nodiscard]] DeferredTxn* find(int64_t producerId) noexcept;
       DeferredTxn* onJudgeRecord(int64_t judgeOffset, const rune::storage::control_record::TxnJudgeRecord&) noexcept;
       void onMarker(int64_t producerId, int64_t offset, bool commit) noexcept;
       void truncateFrom(int64_t offset) noexcept;                                           // leadership scan begin
       [[nodiscard]] int64_t earliestUndecidedFirst() const noexcept;                        // INT64_MAX = none
       [[nodiscard]] bool parkVote(const ParkedVote&) noexcept;                              // false = full -> NotLeader
       template <class Fn> int drainAnswered(Fn&& fn) noexcept;                              // fn(const ParkedVote&, const DeferredTxn&)
       [[nodiscard]] std::size_t lockedCount() const noexcept;
       TxnJudgeStats stats;
   };
   ```
   A full table (`kMaxTxns`) makes the judge wait at the record that would add an entry. It is counted, never a verdict.
8. **View engine overlay** (`broker/libs/view/include/rune/view/TxnOverlay.h`, new; `src/TxnOverlay.cpp`):
   ```cpp
   enum class OverlayTable : uint8_t { View = 0, Contributors = 1, Groups = 2, Blocks = 3, Owners = 4, GroupIds = 5,
                                       ClaimsIndex = 6, ClaimsExtras = 7, KeyIndex = 8 };
   class TxnOverlay {                               // chained after-images of one transaction or atomic batch
   public:
       void reserve(std::size_t rows, uint32_t maxRowLen);                                   // cold
       void bind(uint64_t sourceKey) noexcept; void clear() noexcept;
       [[nodiscard]] bool full() const noexcept; [[nodiscard]] std::size_t rows() const noexcept;
       [[nodiscard]] bool contains(uint8_t member, OverlayTable, const ViewKey&) const noexcept;
       [[nodiscard]] const uint8_t* get(uint8_t member, OverlayTable, const ViewKey&, bool& erased) const noexcept;
       uint8_t* put(uint8_t member, OverlayTable, const ViewKey&, const uint8_t* base, uint32_t rowLen) noexcept;  // nullptr = full
       void erase(uint8_t member, OverlayTable, const ViewKey&) noexcept;
   };
   inline constexpr std::size_t kTxnOverlaysPerWorker = 8;
   struct TxnLockSet { std::array<const TxnOverlay*, kTxnOverlaysPerWorker> locked{}; uint8_t count = 0;
       [[nodiscard]] bool contains(uint8_t member, OverlayTable, const ViewKey&) const noexcept; };
   ```
   - `PrepareStatus` (C9.3) appends `Locked`.
   - `prepareProjection`, `prepareAggregate`, `prepareKeyIndex` (05) and the unique-claims prepare (09) gain a trailing `TxnOverlay* chain = nullptr`. When non-null, reads go overlay-first through it and the record's effect is written into it.
   - The fold bundles `ProjectionFold`, `AggregateFold` and `KeyIndexFold` append `uint8_t member = 0; const TxnLockSet* locks = nullptr;`.
   - Every store (Part 04's direct and overlay stores, 07's keyed stores, 09's claims stores) consults `locks` on every get, put and erase, and reports `Locked`.
9. **`VerdictSource` (C9.5):**
   - `void bindTxn(TxnOverlayPool*, std::span<TxnOverlay> batchOverlay, int32_t txnJudgedRecordsMax) noexcept;` (cold);
   - `[[nodiscard]] int64_t judgeCursor() const noexcept;`;
   - `judgedThrough()` follows P11-2;
   - `class TxnOverlayPool { public: void reserveFor(std::size_t rows, uint32_t maxRowLen); [[nodiscard]] int32_t acquire(uint64_t sourceKey) noexcept; /* -1 = none */ void release(int32_t) noexcept; TxnOverlay& at(int32_t) noexcept; [[nodiscard]] TxnLockSet lockSetFor(uint64_t sourceKey) const noexcept; };`.
10. **`RcState`:** `RcVerdictHooks` appends `void (*marker)(void* ctx, int64_t producerId, int64_t offset, bool commit) noexcept = nullptr;`, called from `RcState::onMarker` on the append path and in the leadership scan. (No visibility-bound accessor: P11-0 makes it unnecessary.)
11. **`RejectedOffsetIndex` (C11):** `[[nodiscard]] bool insertRun(std::span<const int64_t> offsets, uint16_t code, uint32_t constraintId);` (cold, out of order; P11-7).
12. **Cleaner:**
    - `PartitionReplica::publishCleanerTxnJudgeBound(Offset) noexcept` and `[[nodiscard]] Offset cleanerTxnJudgeBound() const noexcept` (an atomic word beside `cleanerVerdictBound`, default `kNoCleanerLso`);
    - `void LogCleaner::setTxnJudgeBound(Offset) noexcept`;
    - `VerdictCleanState::keepsControlRecord(offset, 19)` per P11-8 (c).
13. **`WorkerViewSet`:**
    - `ViewInstance` appends `TxnSpanFence spanFence;` with `struct TxnSpan { int64_t pid = -1; int64_t first = -1; }; struct TxnSpanFence { std::array<TxnSpan, 64> open{}; uint8_t count = 0; int64_t applyingThrough = -1; [[nodiscard]] bool add(int64_t pid, int64_t first) noexcept; /* false = full: wait */ [[nodiscard]] const TxnSpan* find(int64_t pid) const noexcept; void close(int64_t pid) noexcept; [[nodiscard]] bool fenced() const noexcept; /* a marker or atomic batch application in progress */ [[nodiscard]] bool straddles(int64_t o) const noexcept; /* some open first < o */ };` (P11-0, P11-9);
    - `TxnOverlayPool txnOverlays_;`;
    - `[[nodiscard]] bool readFenced(int32_t viewTopicId, int32_t partition) const noexcept;`.
14. **Hook (C8, 03's seam):** `HookRefusal::AtomicBatchTooLarge` is appended before `Count_`. `WriteFacts::atomicBatch` is set from the bit. Rules 5, 6, 7 and 8 change and rule 10 is added (P11-14).
15. **Gateway:**
    - `struct TxnVoteResult { int16_t errorCode = 0; uint16_t code = 0; uint32_t constraintId = 0; int64_t failingOffset = -1; };` in `broker/libs/gateway/include/rune/gateway/ClientGateway.h`;
    - `int64_t ClientGateway::sendTxnVote(int32_t topicId, int32_t partition, int64_t producerId, int16_t producerEpoch, int32_t coordinatorEpoch);` (a token ≥ 0, or −1 when refused synchronously);
    - `using TxnVoteSink = void (*)(void* ctx, int32_t topicId, int32_t partition, int64_t producerId, int16_t producerEpoch, int32_t coordinatorEpoch, const TxnVoteResult&) noexcept;` and `void setTxnVoteSink(TxnVoteSink, void*) noexcept;`;
    - `TxnMarkerChannel` appends `bool (*sendVote)(void*, int32_t toNode, const rune::replication::TxnVoteMsg&) = nullptr; bool (*sendVoteResponse)(void*, int32_t toNode, const rune::replication::TxnVoteResponseMsg&) = nullptr;`;
    - `void onRemoteTxnVote(const rune::replication::TxnVoteMsg&);`, `void onRemoteTxnVoteResponse(const rune::replication::TxnVoteResponseMsg&);` and their thunks;
    - `AeronPartitionTransport::sendTxnVote` and `sendTxnVoteResponse`, with handlers beside `sendWriteTxnMarker` (`AeronPartitionTransport.h:354-372`).
16. **`TransactionCoordinationService`:**
    - `using VoteCandidateFn = bool (*)(void* ctx, int32_t topicId) noexcept; void setVoteCandidateFn(VoteCandidateFn, void*) noexcept;`;
    - `using SessionVersionFn = int32_t (*)(void* ctx, int64_t sessionId) noexcept; void setSessionVersionFn(SessionVersionFn, void*) noexcept;`;
    - `void onTxnVoteResult(int32_t topicId, int32_t partition, int64_t producerId, int16_t producerEpoch, int32_t coordinatorEpoch, const rune::gateway::TxnVoteResult&);`;
    - the test seam `void setVoteFnOverride(std::function<bool(std::string_view topic, int32_t partition, int64_t pid, int16_t epoch)>);` (the control plane, beside `setAppendFnOverride`);
    - `[[nodiscard]] std::size_t voteGatesForTest() const noexcept;`.
17. **Knob (SD14):** `NodeConfig::txnJudgedRecordsMax` (`int32_t`, env `RUNE_TXN_JUDGED_RECORDS_MAX`, default 4096, range 1–1,048,576) and `void Worker::setTxnJudgedRecordsMax(int32_t)` (cold: it re-reserves the pool).
18. **Client:**
    - `struct TxnCommitResult { bool ok = true; uint16_t code = 0; uint32_t constraintId = 0; int64_t failingOffset = -1; };` in `Producer.h`;
    - `TxnCommitResult Producer::commitTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);` (was `void`);
    - `Verdict::VotedNo` (`TxnTypes.h:66`);
    - `void ProduceFrameBuilder::setAtomic(bool) noexcept;`;
    - the commit and Tier-2 completion fields of P11-6 and P11-13.
19. **Test support:**
    - **`CheckedWritesRig`:**
      - `struct VoteReply { int16_t errorCode = 0; uint16_t code = 0; uint32_t constraintId = 0; int64_t failingOffset = -1; int64_t judgeOffset = -1; };`;
      - `int64_t sendVote(int32_t topicId, int64_t pid, int16_t epoch);` and `std::optional<VoteReply> voteReply(int64_t token) const;`;
      - `[[nodiscard]] bool driveUntilVoted(int64_t token, int cap = 2000);`;
      - `struct TxnEnd { bool committed = false; std::vector<std::pair<int32_t, VoteReply>> votes; };` and `TxnEnd commitTxn(int64_t pid);` (Part 04's `void commitTxn` now votes on every verdict partition the pid wrote to first, and aborts on a no);
      - a trailing `bool atomic = false` on `appendBatch`, and `bool txn = false, bool atomic = false` on `appendCharacters`;
      - `void onEveryPass(std::function<void()>)` (test only);
      - `std::vector<int64_t> txnJudgeOffsetsInLog() const;`.
    - **`CheckedWritesModel`:** `void beginTxn(int64_t pid)`, `ModelVote vote(int32_t topicId, int64_t pid)`, `void endTxn(int64_t pid, bool commit)`, `void atomicBatch(int32_t topicId, std::span<const ModelRecord>)`, with judge-order semantics (P11-2 to P11-4) and the conflict rule.
    - **Matrix:** `Op` 100–106, `Entity` 100–107, the appended `Timing` values, `expect11`, `cellsFor(Part::P11)`, pin row 11. Rows 05 and 06 change (Task 12).
    - **Stress:** the invariant `TxnAllOrNothingAndAtomicPerPartition`.
20. **Worker seams:** `[[nodiscard]] const TxnJudgeStats* Worker::txnStatsForTest(int32_t topicId, int32_t partition) const noexcept;` and `[[nodiscard]] std::size_t Worker::parkedVotesForTest() const noexcept;`.

## Contract gaps (flagged to the controller)

- **A new error code** (`TxnConflict = 59`, P11-3): §11.1 lists 51–55 only.
- **The partition also votes under its latest set** (P11-1 (b)), beyond the spec's "set active at the transaction's first record".
- **`TxnJudge` carries `firstOffset`** (C10 lists two fields). It needs no compatibility, because nothing writes type 19 before this part.
- **Provisional rows are after-images in an overlay**, and the tables hold the before-images (P11-4). This is within §18's latitude, but the spec text describes undo.
- **Level 3a uses a visibility bound plus a read fence** (P11-9) rather than "the fold never ends a step inside".
- **R11b changes every fold path** (Part 01's refold, Part 04's replays and rebuilds, Part 07/09 candidate builds): transactional batches apply at their commit marker (P11-0). Their tests stay green because offset order and marker order differ only under interleaving; Task 8 re-pins any that encoded offset-order application, as a recorded deviation.
- **Distributed lock waits** resolve only by the transaction timeout (P11-5).
- **Old sessions get `InvalidRequest`** for a no vote (P11-11).
- **This part edits the not-applicable rules and pins of rows 05 and 06** (Task 12). "Each part updates its own row" is broken here on purpose, because those rules cite the pre-G refusal.

## Review Focus

These are the five inputs the spec implies but no requirement names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A plain write to a key between a transaction's write of that key and its `TxnJudge`.** If the outside write is newer than the transaction's last write of the key, the vote must be no with 59. If it is older (the transaction writes the key again later), the vote must be yes and the transaction's value must win, in the view, in compaction and for consumers.
   - Tests: `CheckedWritesTxnJudgeRegression.OutsideWriteNewerThanTheTxnsLastWriteVotesConflictRegression` and `…OutsideWriteOlderThanTheTxnsLastWriteVotesYesRegression` (Task 6).
2. **A transaction open across thousands of records on a busy partition with a persisted constrained view.** Then a crash after the commit, before the member's next checkpoint. The run-ahead must reach J, the checkpoint pushed during run-ahead must replay with the transaction backfilled from J, and the view must equal a fresh refold.
   - Test: `CheckedWritesTxnWorkerRegression.RunAheadCheckpointBackfillsTheTxnAfterARestartRegression` (Task 7).
3. **Two transactions locking rows on two partitions in opposite J order.** The waits must resolve by the transaction timeout, never hang past it. The surviving transaction's rows are correct, and the aborted one leaves nothing.
   - Test: `CheckedWritesTxnBrokerRegression.OppositeOrderLocksResolveByTimeoutShardRegression` (Task 14).
4. **A committed transaction whose marker lies past a later open transaction's first record**, on a partition with ordinary views only. Reads must not be held for the later transaction's lifetime, and the first transaction appears in one step, identically in the live fold, a refold and a rebuild (P11-0).
   - Test: `CheckedWritesTxnVisibilityRegression.CommittedSpanCrossingTheLsoNeverHoldsReadsRegression` (Task 8).
5. **A v11 transactional client whose commit gets a no vote.** It must get `InvalidRequest`, abort successfully, and run its next transaction. It must never loop on `EndTxn` or turn fatal.
   - Test: `CheckedWritesTxnCoordinatorRegression.OldSessionNoVoteIsInvalidRequestThenAbortSucceedsRegression` (Task 10).

---

## Interaction matrix

### A. The shared worker-level matrix (`rune_checked_writes_matrix_tests`, `CheckedWritesMatrix/Cell11.*`, `cellsFor(Part::P11)`, Task 13)

**Setup.** The matrix is built on `CheckedWritesRig`:
- Part 09's `createCharacterTopic()` (autoincrement, key index implied), `createConstrainedView(CharacterName)` and `createConstrainedView(AccountStats)` on topic 1;
- `createLedgerTopic()` with `createConstrainedView(Ledger)` (`balance >= 0`) on topic 2, so a transaction spans two voting partitions on one worker;
- one ordinary projection view (`account_characters`), the consumers of Part 04's entities, and `onEveryPass` observers for (d) below.

**Who writes.** The transaction is producer 77, idempotent and transactional, with `All`. It writes the op's records to topic 1 plus one ledger entry (a debit of 10) to topic 2, then `commitTxn(77)`. The second writer is producer 78: plain `All | View` or, for `RacingTxnSameName`, a second transaction.

**Ops (Part 11 range 100–119):**

| Id | Op | Records of the transaction on `character` |
|---|---|---|
| 100 | `TxnCreate` | a key-0 create for U named X |
| 101 | `TxnRename` | an `update` of C (one of U's) to name X |
| 102 | `TxnMove` | an `update` of C (one of V's) to `userId = U` |
| 103 | `TxnRemoveExisting` | `removeExisting(C)` |
| 104 | `TxnSelfCollidingCreates` | key-0 creates `X` then `x` for U |
| 105 | `TxnUpdateNonKey` | an `update` of C changing `lastPlayed` only |
| 106 | `AtomicBatch` | no transaction: one Tier-2 batch (`kAttrAtomic`, `All \| View`) of key-0 creates `X` and `Y` for U |

**Entities (Part 11 range 100–119), the state before the op:**

| Id | Entity | Setup |
|---|---|---|
| 100 | `AllPass` | U has 5 characters, X is free, ledger balance 100 |
| 101 | `FailsUnique` | X is owned by one of W's characters |
| 102 | `FailsCheck` | U has 12 characters |
| 103 | `SecondPartitionNo` | ledger balance 5, so the debit of 10 breaks `non_negative` |
| 104 | `KeyAbsent` | C was removed (acked) before the transaction |
| 105 | `OutsideWriteSameKey` | producer 78's `update` of C lands after the transaction's write of C and before J |
| 106 | `OutsideWriteLockedRow` | U has 11; after the yes and before the marker, producer 78 creates a character for U (its prospect touches the locked `account_stats` row of U) |
| 107 | `RacingTxnSameName` | transaction 79 does the same op on another key with the same target (name X, or U's group), votes after 77 |

**Timings.** There are 13. Three are from the shared enum: `Single`, `FailoverHwFold` (failover after the transaction's records are committed, before the judge reached them) and `RestartDuringRebuild` (restart while T is deferred). Ten are appended to `Timing` after `RetryAfterWindow` (R04d):
- `CoordinatorFailoverVotesOut`: the first vote's answer is dropped and the vote is re-sent, which is what a reloaded coordinator does. The reply equals the first.
- `VoterFailoverBeforeJudge`: J is appended, then `failover()` before J is committed. J is cut, and the re-sent vote appends a new J.
- `VoterFailoverAfterJudge`: J is committed, then `failover()` before it is judged. The new leader judges at the same J and gives the same answer.
- `CoordinatorFailoverBeforeCommit` (R11c): the coordinator is reloaded from its persisted state between the transaction's `AddPartitions` and its commit. The rig votes on every partition of the persisted set, as a reloaded coordinator does; every vote is collected and the outcome equals `Single`'s.
- `TimeoutDuringVote`: an abort marker (the timeout abort) is appended while J is not yet judged.
- `TimeoutWhileLocked`: an abort marker after a yes.
- `AbortAfterYes`: the rig aborts instead of committing after all votes are yes (the coordinator saw another partition's no).
- `ConstraintCreatedWhileOpen`: `account_stats` is created after the transaction's first record.
- `ConstraintDroppedWhileOpen`: `character_name` is dropped after the first record (R04a two-phase drop).
- `ConstraintReplacedWhileOpen`: `account_stats` is replaced by `check (characters <= 13)` after the first record.

**Not applicable** (top-down; the first matching rule names the reason):
1. `AtomicBatch × {CoordinatorFailoverVotesOut, CoordinatorFailoverBeforeCommit, VoterFailoverBeforeJudge, VoterFailoverAfterJudge, TimeoutDuringVote, TimeoutWhileLocked, AbortAfterYes}`: "an atomic batch has no vote and no marker". 1 × 8 × 7 = 56 cells.
2. `AtomicBatch × SecondPartitionNo`: "an atomic batch lives on one partition". 1 × 1 × 6 = 6 cells.
3. `{TxnCreate, TxnSelfCollidingCreates, AtomicBatch} × {KeyAbsent, OutsideWriteSameKey}`: "a key-0 create's id is new; no earlier key and no outside write can hit it". 2 × 2 × 13 + 1 × 2 × 6 = 64 cells.

**Cell count:** 7 × 8 × 13 = 728. 602 are applicable and 126 skipped, pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 11).

**`expect11(cell)`, top-down.** The verdicts always come from `CheckedWritesModel` with Part 09's rules plus the transaction semantics (P11-2 to P11-7). `expect11` names what the cell checks:

0. `TimeoutDuringVote`, `TimeoutWhileLocked`, `AbortAfterYes` → the transaction is aborted. Nothing of it is visible anywhere. Producer 78's write (if any) is judged against the state without it. For `TimeoutWhileLocked` the overlay is released at the abort marker.
1. `CoordinatorFailoverBeforeCommit` → one vote reply per partition of the persisted set, including the non-constrained ones (yes, no `TxnJudge`), and the outcome equals `Single`'s. `VoterFailoverBeforeJudge` → `txnJudgeOffsetsInLog()` holds one surviving J, and the answer equals `Single`'s. `VoterFailoverAfterJudge` and `CoordinatorFailoverVotesOut` → the same J and an identical reply.
2. `ConstraintCreatedWhileOpen` → the partition votes (P11-1 (b)) and the transaction is judged under `setAt(J)`. `ConstraintDroppedWhileOpen` → judged under `setAt(J)`, without `character_name`, since the drop record lies below J. `ConstraintReplacedWhileOpen` → judged by the new plan when the swap S lies below J, else by the old one; the model decides from the log.
3. `Single`, as a check on the model (`CheckedWritesModel.TxnRulesMatchTheTable` asserts the model gives exactly this). N = 51 `character_name` ordinal 0, M = 51 `account_stats` ordinal 1, L = 51 `balances` ordinal 1, K = 53, C = 59; each cell gives `character`'s vote, and in every no cell the transaction aborts on both partitions:

   | Op \ Entity | AllPass | FailsUnique | FailsCheck | SecondPartitionNo | KeyAbsent | OutsideWriteSameKey | OutsideWriteLockedRow | RacingTxnSameName |
   |---|---|---|---|---|---|---|---|---|
   | `TxnCreate` | Ok | N | M | Ok (ledger L) | — | — | Ok; 78's create then M | 77 Ok, 79 N |
   | `TxnRename` | Ok | N | Ok | Ok (ledger L) | K | C | Ok; 78's create Ok | 77 Ok, 79 N |
   | `TxnMove` | Ok | Ok | M | Ok (ledger L) | K | C | Ok; 78's create then M | 77 Ok, 79 M |
   | `TxnRemoveExisting` | Ok | Ok | Ok | Ok (ledger L) | K | C | Ok; 78's create Ok | 77 Ok, 79 K |
   | `TxnSelfCollidingCreates` | N | N | M | N (ledger L) | — | — | N; 78's create Ok | 77 N, 79 Ok |
   | `TxnUpdateNonKey` | Ok | Ok | Ok | Ok (ledger L) | K | C | Ok; 78's create Ok | 77 Ok, 79 Ok |
   | `AtomicBatch` | Ok | N | M | — | — | — | M (12 + 2); 78's create Ok | 1st Ok, 2nd N |

4. Every other timing → the model's verdicts for the committed log.

**Every applicable cell asserts** (after `driveUntilJudged(LEO)`, `compact()`, `restart()` and another `driveUntilJudged`):
- (a) every partition's vote reply, `(code, constraintId, failingOffset)`, equals the model's;
- (b) every view equals a fresh refold of the model's accepted records;
- (c) no consumer (raw, group, filtered, read_uncommitted) received a record of an aborted transaction, or of a rejected atomic batch, or any record of a transaction before its marker;
- (d) at every pass, the `onEveryPass` observer reads U's `account_stats` row and the transaction's `character_by_id` rows, and sees all of the transaction's changes on that partition or none (level 3a);
- (e) `rejected.mismatches() == 0`, `txnMarkerWithoutVote == 0`, and no overlay is held once the transaction is decided;
- (f) after `compact()`, every key's latest record is the model's latest accepted one in offset order;
- (f2) every view's tables are byte-identical across the live fold, a `restart()` refold from the log start, and a `driveRebuild` of a freshly installed copy (P11-0);
- (g) stats 11–20 equal the model's counts.

### B. Named §15 Part G cells

| §15 / prompt cell | Test (task) |
|---|---|
| two-partition transaction, one partition votes no: nothing commits, `commitTransaction()` returns the code and constraint id | matrix `SecondPartitionNo`, `CheckedWritesTxnBrokerRegression.TwoPartitionsOneNoNothingCommitsShardRegression` (14) |
| all votes yes: both partitions visible, each in one step, no read sees part | matrix (d), `CheckedWritesTxnBrokerRegression.AllYesEachPartitionVisibleInOneStepShardRegression` (14) |
| an abort after a yes restores the before-images | matrix `AbortAfterYes`, `CheckedWritesTxnJudge.AbortAfterYesReleasesTheOverlay` (6) |
| a record outside the transaction that touches a locked row pauses the fold, then is judged against the final state | matrix `OutsideWriteLockedRow`, `CheckedWritesTxnJudge.LockedRowPausesThenJudgesAgainstTheFinalState` (6) |
| two transactions racing for the same name or the last slot | matrix `RacingTxnSameName`, `CheckedWritesTxnBrokerRegression.RacingTransactionsForTheLastSlotShardRegression` (14) |
| failover of a voter before and after its `TxnJudge` commits | matrix `VoterFailover*`, `CheckedWritesTxnBrokerRegression.VoterLeaderCrashMidVoteThreeNodesRegression` (14) |
| coordinator failover with votes out, then a client retry: same answer | `CheckedWritesTxnCoordinatorRegression.UnloadWithVotesOutThenRetryGetsTheSameAnswerRegression` (10), `CheckedWritesTxnBrokerRegression.CoordinatorCrashMidVoteThreeNodesRegression` (14) |
| a constraint created or dropped while a transaction is open | matrix `Constraint*WhileOpen`, `CheckedWritesTxnWorkerRegression.DropWhileLockedKeepsThePlanUntilWRegression` (7) |
| a transaction over `txn.judged.records.max` gets `TxnTooLarge` | `CheckedWritesTxnWorker.OverTheCapVotesTooLargeWithoutAJudgeRecord` (7), `CheckedWritesTxnBrokerRegression.TxnTooLargeShardRegression` (14) |
| an atomic batch with one bad record: the whole batch is rejected and no view changes | matrix `AtomicBatch × {FailsUnique, FailsCheck}`, `CheckedWritesTxnJudge.AtomicBatchOneBadRecordRejectsTheWholeBatch` (6) |
| the transaction timeout while rows are locked | matrix `TimeoutWhileLocked`, `CheckedWritesTxnCoordinator.TimeoutDuringTheVoteAbortsAndAnswersInvalidTxnState` (10) |
| before Part G refusals (inverted): transactions and atomic batches on a constrained source now work | `CheckedWritesConstraintWorker.ConstrainedSourceAcceptsTransactions` (12, inverted from 09) |
| a vote racing with plain writes on the same key | matrix `OutsideWriteSameKey`, Review Focus 1 (6) |
| a vote racing with constraint replace or drop (R04a) | matrix `ConstraintReplacedWhileOpen` / `ConstraintDroppedWhileOpen`, `DropWhileLockedKeepsThePlanUntilWRegression` (7) |
| aborted folds (Part 01) | `CheckedWritesTxnWorkerRegression.VotedAbortedTxnNeverReachesAnyViewAndRefoldsEqualRegression` (7) |
| autoincrement inside a transaction (Part 06) | `CheckedWritesTxnWorker.NoVoteBurnsTheTransactionsIds` (7), matrix `TxnCreate` |
| unique and check (Part 09) | matrix entities 101–103, `CheckedWritesTxnJudge.TwoCreatesOfOneNameInOneTxnVoteNo` (6) |
| marker without a vote | `CheckedWritesTxnJudgeRegression.CommitMarkerWithoutJudgeJudgesAllOrNothingAtTheMarkerRegression` (6) |
| level 3a on non-verdict partitions | `CheckedWritesTxnVisibility.*` (8) |

### C. The model check (`CheckedWritesModelRegression.SeededTransactionsMatchTheModel`, `rune_checked_writes_matrix_tests`, Task 13)

- **Seeds and steps.** Seeds 1–200 × 250 steps on the matrix setup at `cap = 3`. `CheckedWritesModelRegression.SameSeedSameTraceTxn` re-runs seeds 1–8 and compares the step trace hash.
- **Actions,** one per step from the seed's `std::mt19937_64`:
  - transactions of 1–6 records from producers 77 and 79 over both topics, committed through `commitTxn` or aborted, with votes interleaved by `driveUntilJudged` in random amounts;
  - plain `All | View` and `acks=leader` writers on the same keys and groups;
  - atomic batches of 1–4 records;
  - `failover()`, `demotePromote()`, `restart()` (also while locked), `truncateTo(HW)`, `compact()`;
  - constraint replace and drop;
  - an abort marker for an open transaction (timeout).
- **Checked after every step:** Part 04's six invariants, Part 09's `AtMostCapLiveCharacters` and `LowerNameUnique`, and `TxnAllOrNothingAndAtomicPerPartition`, with the model as reference:
  - every transaction is fully applied or fully absent on every partition it touched;
  - no pass shows part of it;
  - no consumer received any of an aborted one;
  - votes match;
  - **`FoldPathsAgree`** (R11b): after every step that ends with no open transaction, the live tables, a refold from the log start, a rebuild of a fresh copy and the model's apply-at-marker refold are byte-identical, including the `last` / `lastNonNull` columns of a `count`, `last(v)`, `lastNonNull(v)` view with `check (lv >= 0)` installed for this model check.
- **Failure output:** `seed=<n> step=<k> action=<name>`.

### D. The cluster stress (`rune_checked_writes_stress_tests`, `CheckedWritesStressRegression.SeededTransactionsHoldTheInvariants`, Task 13)

- **Cluster.** `CheckedWritesStressDriver` on `ThreeNodes`: shard mode, 64 KiB segments, the cleaner every 50 ms, `RUNE_VERDICT_INTERVAL_MS=20`, `RUNE_TXN_JUDGED_RECORDS_MAX=64`.
- **Topics and run.** The golden `character` with `character_name`, `account_stats` and `character_by_id`, plus `ledger` with `balances`. Seeds 1–12 × 300 steps.
- **Writers.** Three transactional clients write two-topic transactions (creates, renames, debits), and two plain `All | View` clients race them.
- **Faults:**
  - a crash and restart of the coordinator node and of the partition leader, at random steps between `commitTransaction` and its answer;
  - a follower restart;
  - cleaner passes;
  - a replace of `account_stats`.
- **Invariants:** `TxnAllOrNothingAndAtomicPerPartition` (registered by this part), plus 04's five, 06's `NoIdTwiceNoReuse`, and 09's `AtMostTwelveLiveCharacters` and `LowerNameUnique`. They are checked from the final leader's views and consumers and from the model of the committed log. `TxnAllOrNothingAndAtomicPerPartition` asserts:
  - each `commitTransactionAndWait` that returned `ok` has every record visible on both topics;
  - each no or aborted one has none;
  - every no's code and constraint id equal the model's for the committed log;
  - `parkedVotesForTest() == 0` and no held overlay at the end.

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `protocol/include/rune/protocol/client/{Protocol.h,TxnMessages.h}`, `client-cpp/src/cli/QlEngine.cpp:133` | M | 59, `isTxnVoteCode`, `EndTxnResponse` v12, the name |
| `protocol/include/rune/protocol/RecordBatch.h` | M | `kAttrAtomic` |
| `broker/libs/replication/include/rune/replication/ReplicationMessages.h`, `AeronPartitionTransport.h` (+`src/AeronPartitionTransport.cpp`) | M | Templates 13 and 14, send and handlers |
| `broker/libs/storage/include/rune/storage/{ControlRecordLayout.h,RejectedOffsetIndex.h,VerdictCleanState.h}`, `LogCleaner.{h,cpp}`, `broker/libs/replication/include/rune/replication/PartitionReplica.h` | M | `TxnJudge` build and parse, `insertRun`, the `TxnJudge` cleaner bound |
| `broker/libs/node/include/rune/node/{WorkerOp.h,WorkerReturnCodec.h}`, `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*` | M | Op 28, return 8 |
| `broker/libs/view/include/rune/view/TxnOverlay.h`, `broker/libs/view/src/TxnOverlay.cpp` | C | Chained overlay, lock set |
| `broker/libs/view/{include/rune/view/RecordFold.h,ViewFold.h,AggFold.h,src/ViewFold.cpp,src/AggFold.cpp,src/UniqueClaims.cpp}`, `broker/libs/view/CMakeLists.txt` | M | `PrepareStatus::Locked`, `chain`, `locks` in every store |
| `broker/libs/node/include/rune/node/{WriteState.h,VerdictSource.h,RcState.h,StatsCollect.h,CheckedWriteHook.h}`, `broker/libs/node/src/{VerdictSource.cpp,CheckedWriteHook.cpp}` | M | `TxnJudgeState`, deferral, judging at J, locks, finalize, atomic batches, stats 11–20, hook rules |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` | M | Pool, span fence, read gate, backfill replay, checkpoint rules |
| `broker/libs/node/include/rune/node/Worker.h` | M | `executeTxnVoteWire_`, parked votes, marker answer parking, isolation, visible bound, cleaner publish, seams |
| `broker/libs/node/{include/rune/node/NodeConfig.h,src/NodeConfig.cpp,src/BrokerNode.cpp,include/rune/node/BrokerNode.h}` | M | Knob, vote candidate predicate, channel and sink wiring |
| `broker/libs/gateway/{include/rune/gateway/ClientGateway.h,src/ClientGateway.cpp}` | M | `sendTxnVote`, token table, remote routing, results |
| `broker/libs/node/{include/rune/node/TransactionCoordinationService.h,src/TransactionCoordinationService.cpp}` | M | Vote gate, no → abort, answers, remembered no |
| `client-cpp/include/rune/client/producer/{Producer.h,TxnTypes.h,ProduceFrameBuilder.h}`, `client-cpp/src/producer/{Producer.cpp,ProducerTxn.cpp,CoordinatorLink.cpp,ProducerHub.cpp,ProducerImpl.cpp,BatchBuilder.cpp}` | M | Client vote results, atomic bit |
| Tests: `protocol/tests/protocol/client/TxnProtocolTest.cpp`, `broker/libs/replication/tests/replication/ReplicationMessagesTest.cpp`, `broker/libs/storage/tests/storage/CheckedWrites{ControlRecord,RejectedIndex,VerdictCleaner}Test.cpp`, `broker/libs/view/tests/view/CheckedWritesTxnOverlayTest.cpp` (C), `broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp` (C), `CheckedWritesTxnWorkerTest.cpp` (C), `CheckedWritesTxnVisibilityTest.cpp` (C), `CheckedWritesTxnCoordinatorTest.cpp` (C), `broker/libs/node/tests/integration/CheckedWritesTxnVoteRoutingTest.cpp` (C), `broker/libs/node/tests/client/CheckedWritesTxnBrokerRegressionTest.cpp` (C), `client-cpp/tests/producer/CheckedWritesTxnVoteClientTest.cpp` (C), the shared support headers, `CheckedWritesMatrixTest.cpp`, `CheckedWritesModelTest.cpp`, `CheckedWritesStressTest.cpp`, `CheckedWritesAllocRegressionTest.cpp`, CMake lists | C/M | As listed per task |
| `broker/docker/checked-writes-e2e.sh`, `client-cpp/examples/checked_writes_probe.cpp`, `docs/testing.md` | M | The `TXN` leg |
| `broker/benchmark/{ViewFoldBench.cpp,WorkerProduceLatencyBench.cpp}` | M | Txn judging and commit benches |
| `docs/api/{ql.md,client-cpp.md,wire-protocol.md,admin-cli.md}`, `docs/{operations.md,configuration.md,testing.md}`, `broker/docs/{CATALOG.md,SECURITY.md,SELF-HOSTING.md}`, `CLAUDE.md`, `client-cpp/CLAUDE.md`, `broker/CLAUDE.md` | M | This part's docs and the §16.12 cross-check |
| `rune-site` repo: `rune/docs.html`, `rune/index.html` | M | The views section (Task 16, local commit only) |

---

## Phase 1 — Wire, records and the view-engine overlay

### Task 1: Protocol: code 59, the atomic bit, `EndTxnResponse` v12, templates 13/14, the `TxnJudge` record

**Files:**
- Modify: `protocol/include/rune/protocol/client/Protocol.h` (after `ViewUniqueInvalid = 58`), `client-cpp/src/cli/QlEngine.cpp:133` (`errorCodeName`), the old-session mapping `produceErrorForClient` (03, P3-5), `protocol/include/rune/protocol/RecordBatch.h:31-36`, `protocol/include/rune/protocol/client/TxnMessages.h:425-470`
- Modify: `broker/libs/replication/include/rune/replication/ReplicationMessages.h` (after `WriteTxnMarkerResponseMsg`, `:907-950`)
- Modify: `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (04's `kTxnJudge`; `recordType` already accepts 19)
- Test: `protocol/tests/protocol/client/TxnProtocolTest.cpp`, `broker/libs/replication/tests/replication/ReplicationMessagesTest.cpp`, `broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp`, `protocol/conformance/vectors` (the EndTxnResponse v12 vector beside the v1 one)

**Interfaces:**
- Consumes: C1 (03), C10 (04).
- Produces: contract additions 1–5.

- [ ] **Step 1: Write the failing tests.**
  ```cpp
  TEST(CheckedWritesTxnWire, EndTxnResponseV12CarriesTheVote) {
      EndTxnResponse r; r.sessionId = 7; r.correlationId = 9; r.errorCode = 51;
      r.hasVote = true; r.constraintId = 0x00070000u; r.failingOffset = 123;
      std::array<uint8_t, 64> b{};
      ASSERT_EQ(r.encodeFor(b.data(), 12), EndTxnResponse::ENCODED_SIZE_V12);   // 38
      auto d = EndTxnResponse::decode(b.data(), 38);
      ASSERT_TRUE(d); EXPECT_TRUE(d->hasVote); EXPECT_EQ(d->constraintId, 0x00070000u); EXPECT_EQ(d->failingOffset, 123);
  }
  ```
  - `CheckedWritesTxnWireRegression.EndTxnResponseBelowTwelveIsByteIdenticalRegression`: `encodeFor(b, 11)` of the same value writes the 26-byte frame and is byte-equal to `encode` of `{errorCode = 51}`. A v1 decoder (`decode` over 26 bytes) gives `hasVote == false`.
  - `CheckedWritesTxnWire.IsTxnVoteCode`: true for 51, 52, 53, 55 and 59, false for 0, 39, 54, 56 and 58.
  - `CheckedWritesTxnWire.ErrorCodeNameKnowsTxnConflict`: `errorCodeName(59) == "TxnConflict"`.
  - `CheckedWritesWireRegression.OldSessionsNeverSeeCode59Regression`: `produceErrorForClient(59, 11) == 39`.
  - `CheckedWritesTxnWire.AtomicBitIsBitSeven`: `kAttrAtomic == 0x0080`, disjoint from `kAttrControl | kAttrTransactional | kAttrDeleteHorizon`.
  - `ReplicationMessagesTest`:
    - `CheckedWritesTxnVoteMsg.RoundTrips`: every field with non-zero distinct values.
    - `CheckedWritesTxnVoteMsg.ResponseRoundTrips`.
    - `CheckedWritesTxnVoteMsg.DecodeRefusesShortAndForeign`: 1 byte short → false; template 11 → false.
  - `CheckedWritesControlRecord.TxnJudgeRoundTrips`: `{pid 77, epoch 3, first 1000}` → `recordType == 19`, parse equal; a buffer one byte short → `buildTxnJudge == 0`.
  - The EndTxnResponse v12 vector is added to the conformance manifest.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests rune_replication_tests rune_storage_tests`. Expected: compile errors on `hasVote`, `TxnVoteMsg` and `buildTxnJudge`.
- [ ] **Step 3: Implement** contract additions 1–5 exactly as listed. `encodeFor` writes the trailer only for `clientVersion >= kClientVersionCheckedWrites && hasVote`. Both new cluster messages follow `WriteTxnMarkerMsg`'s LE helpers, and their header comments carry the byte tables.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnWire|CheckedWritesTxnVoteMsg|CheckedWritesControlRecord|TxnProtocol|ReplicationMessages|CheckedWritesWire' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/include/rune/protocol/client/Protocol.h protocol/include/rune/protocol/client/TxnMessages.h \
          protocol/include/rune/protocol/RecordBatch.h client-cpp/src/cli/QlEngine.cpp \
          broker/libs/replication/include/rune/replication/ReplicationMessages.h \
          broker/libs/storage/include/rune/storage/ControlRecordLayout.h protocol/tests/protocol/client/TxnProtocolTest.cpp \
          broker/libs/replication/tests/replication/ReplicationMessagesTest.cpp \
          broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp protocol/conformance
  git commit -m "protocol: TxnConflict 59, kAttrAtomic, EndTxnResponse v12 vote trailer, TxnVote 13/14, TxnJudge record (C18)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```
  The produce-path mapping edit lives where Part 03 put `produceErrorForClient`. Add that file to the `git add`.

### Task 2: The internal ring: `TxnVote` (28) and `TxnVoteResult` (8)

**Files:**
- Modify: `protocol/schema/rune-internal-ring.xml` (`OpType` `TXN_VOTE = 28`, `ReturnType` `TXN_VOTE_RESULT = 8`), `protocol/include/rune_protocol_internal_sbe/*` (regenerated), `broker/libs/node/include/rune/node/WorkerOp.h` (`Type::TxnVote = 28` placed after `WriteSpecUpdate = 27`, before 07's 29/30 and 08's 31; `WorkerReturn::Type::TxnVoteResult = 8` after `ViewRowResult = 7`, before 07's 9 and 08's 10), `broker/libs/node/include/rune/node/WorkerReturnCodec.h`
- Test: `protocol/tests/protocol/` (the existing ring codec tests next to Part 02's `WRITE_SPEC_UPDATE` case)

**Interfaces:**
- Produces: contract addition 6. `TxnVote` is ownership-gated like `AppendControlRecord` (it targets one replica incarnation). A gated-out op returns `TxnVoteResult{errorCode = NotLeader}`, never `ControlRecordRefused`.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesTxnRing.TxnVoteOpRoundTrips`: an op with token 41, pid 77, epoch 3, roleEpoch 6 and sessionId 2 survives encode and decode.
  - `CheckedWritesTxnRing.TxnVoteResultMapsTheScalars`: `{errorCode 0, recordCount 51, newNextOffset 0x00070000, baseOffset 123, lastOffset 99}` round-trips through `WorkerReturnCodec`.
  - The existing `ProtocolRegression.WorkerReturnLayoutUnchanged` stays green.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `TXN_VOTE` undeclared.
- [ ] **Step 3: Implement.** Edit the XML, regenerate with the command in Execution context, then add the enum values and the codec mapping. A comment on `TxnVoteResult` lists the scalar reuse.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnRing|ProtocolRegression|WorkerReturn' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/schema/rune-internal-ring.xml protocol/include/rune_protocol_internal_sbe \
          broker/libs/node/include/rune/node/WorkerOp.h broker/libs/node/include/rune/node/WorkerReturnCodec.h protocol/tests
  git commit -m "ring: WorkerOp TxnVote 28 and WorkerReturn TxnVoteResult 8 (C18)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 3: `TxnOverlay`: chained prepares, the lock set and `PrepareStatus::Locked` in every store

**Files:**
- Create: `broker/libs/view/include/rune/view/TxnOverlay.h`, `broker/libs/view/src/TxnOverlay.cpp`, `broker/libs/view/tests/view/CheckedWritesTxnOverlayTest.cpp`
- Modify:
  - `broker/libs/view/include/rune/view/RecordFold.h` (`PrepareStatus::Locked`);
  - `ViewFold.h` / `src/ViewFold.cpp`: `ProjectionFold::{member, locks}`, the `chain` parameter on `prepareProjection`, both the plain and keyed (07) stores;
  - `AggFold.h` / `src/AggFold.cpp`: `AggregateFold::{member, locks}`, `prepareAggregate` with `chain`, and a third store `ChainStore` in `AggStep<Store>`;
  - `src/UniqueClaims.cpp` (09's claims store);
  - `broker/libs/node/include/rune/node/VerdictSource.h` and `src/VerdictSource.cpp`: `KeyIndexFold::{member, locks}` and `prepareKeyIndex` with `chain` (05);
  - `broker/libs/view/CMakeLists.txt`.
- Test: `rune_view_tests`, `rune_checked_writes_tests` (the key-index case)

**Interfaces:**
- Consumes:
  - 04's `AggStep<Store>` (`DirectStore`, `OverlayStore`) and its projection row-image store;
  - 07's keyed projection store (owner map, `groupIds`);
  - 09's claims store;
  - 05's `prepareKeyIndex`.
- Produces: contract addition 8.
  - `TxnOverlay` is a fixed slot arena with an open-addressed index over `(member, table, ViewKey)`. It is reserved once and never allocates afterwards.
  - With `chain != nullptr`, each store's `get` returns the chain's image (or "erased") before the table row, and its `put` and `erase` write into the chain. The prospect is still filled, so `judge()`'s evaluation is unchanged.
  - With `locks != nullptr`, any `get`, `put` or `erase` whose `(member, table, key)` is in `locks->contains(…)` sets the prepare's result to `Locked` and stops the step. The tables are untouched.
  - The chained store keeps the "same code" property: `AggStep<ChainStore>` is the same template body.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesTxnOverlayTest.cpp`; fixtures from 04's `CheckedWritesPrepareApplyTest.cpp`, 07's keyed fixtures and 09's `character_name` fixture):
  ```cpp
  TEST(CheckedWritesTxnOverlay, ChainedAggregatePrepareSeesTheEarlierRecord) {
      // count(*) by userId, u100 has n=11: chain-prepare create(c1,u100) then create(c2,u100):
      // rows[0] of the second prospect has n == 13; view/contributors/groups byte-identical to before.
  }
  ```
  - `CheckedWritesTxnOverlay.ChainedProjectionRenameThenReuse`: chain-prepare `rename c7 "foo" → "bar"` then `create c9 "foo"`. The second prospect is not `ownedByOther` (the chain released "foo"), and the tables are unchanged.
  - `CheckedWritesTxnOverlay.ChainedKeyIndexInsertThenUpdate`: chain-prepare `insert k` then `update k`. The second evaluates Ok (the key exists in the chain), and the key index table is unchanged.
  - `CheckedWritesTxnOverlayRegression.ChainedPreparesThenDirectApplyEqualSequentialFoldRegression`: seeds 1–200. Take 1–64 random records over 16 keys and 4 groups (renames, moves, deletes, Min/Max retractions). The chained prepare's final images equal, row by row, the tables produced by applying the same records one by one through `DirectStore`. After the direct re-apply the tables are byte-identical to the sequential fold.
  - `CheckedWritesTxnOverlay.LockedRowReportsLocked`: a held overlay with `account_stats` row u100. Preparing an outside create for u100 with `locks` → `PrepareStatus::Locked`, tables unchanged. Preparing one for u200 → `Ok`.
  - `CheckedWritesTxnOverlay.LockedNeverReportedWithoutALockSet`: the same with `locks == nullptr` → `Ok`. This is 04's path, unchanged.
  - `CheckedWritesTxnOverlay.FullOverlayIsCapacityExhausted`: reserve 2 rows, prepare a Move (needs more) → `CapacityExhausted`.
  - `CheckedWritesTxnOverlay.ReservedOverlayAllocatesNothing`: under `rune::testing::AllocWindow`, 4,096 chained prepares into a reserved overlay → 0 allocations.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `rune/view/TxnOverlay.h` not found.
- [ ] **Step 3: Implement** as specified. `OverlayTable` ids map each store's table kind. The keyed store's owner map is `Owners`, `groupIds` is `GroupIds`, and the claims tables are `ClaimsIndex` and `ClaimsExtras`. Each table keys its rows the way the table does (the `ViewKey` of the row). Group ids interned in a chained prepare come from the instance counter and are burned on release (07: ids are internal).
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnOverlay|CheckedWritesPrepareApply|CheckedWritesKeyed|CheckedWritesUniqueFold|CheckedWritesKeyIndex|ViewFold|AggFold' -j14 --output-on-failure`. Expected: PASS. Every identity test of 04, 07 and 09 is unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/TxnOverlay.h broker/libs/view/src/TxnOverlay.cpp \
          broker/libs/view/include/rune/view/RecordFold.h broker/libs/view/include/rune/view/ViewFold.h \
          broker/libs/view/include/rune/view/AggFold.h broker/libs/view/src/ViewFold.cpp broker/libs/view/src/AggFold.cpp \
          broker/libs/view/src/UniqueClaims.cpp broker/libs/view/CMakeLists.txt broker/libs/view/tests/view/CheckedWritesTxnOverlayTest.cpp \
          broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp
  git commit -m "view: TxnOverlay - chained after-image prepares and row locks in every fold store (P11-4)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint A (controller).** `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure`, the same on `build/asan`, `build/tsan` and `build/client-tsan`, and `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure`. Expected: 100 % pass and no sanitizer report.

---

## Phase 2 — The judge, the worker and level 3a

### Task 4: `TxnJudgeState`, the feed and the pool

**Files:**
- Modify: `broker/libs/node/include/rune/node/WriteState.h` (contract addition 7), `broker/libs/node/include/rune/node/VerdictSource.h` and `src/VerdictSource.cpp` (`TxnOverlayPool`, `verdictFeedBegin` / `verdictFeedBatch` for type 19, the new `verdictFeedMarker`), `broker/libs/node/include/rune/node/RcState.h` (`RcVerdictHooks::marker`, called from `onMarker` `:302` and from the scan path where `builder.step` applies a marker `:633-650`)
- Modify: `broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h` (`insertRun`)
- Test: `broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp` (create; add to `rune_checked_writes_tests`), `broker/libs/storage/tests/storage/CheckedWritesRejectedIndexTest.cpp`

**Interfaces:**
- Consumes: 04's `verdictFeedBegin` / `verdictFeedBatch`, `RcVerdictHooks`, `parseTxnJudge` (Task 1), `TxnOverlay` (Task 3).
- Produces:
  - contract additions 7, 9 (`TxnOverlayPool`), 10 (`RcVerdictHooks::marker`) and 11;
  - `void verdictFeedMarker(void* state, int64_t producerId, int64_t offset, bool commit) noexcept;`;
  - `verdictFeedBegin` truncates `txn` from `target`;
  - `verdictFeedBatch` handles type 19 through `txn.onJudgeRecord(base, rec)`;
  - `verdictFeedMarker` feeds `txn.onMarker`.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesTxnState.JudgeRecordThenMarkerDecides`: `onJudgeRecord(500, {77, 3, 400})` → `find(77)->phase == AwaitJudge`, `earliestUndecidedFirst() == 400`. Then `onMarker(77, 520, true)` → `markerOffset == 520`, `commit`. Once finalized, `earliestUndecidedFirst() == INT64_MAX`.
  - `CheckedWritesTxnState.TruncateDropsEntriesAtOrAbove`: J at 500 and 700 → `truncateFrom(600)` keeps the first only.
  - `CheckedWritesTxnState.SecondTxnOfOneProducerReplacesTheDecidedOne`: a decided entry for 77, then `onJudgeRecord(900, {77, 3, 850})` → a fresh entry with `first == 850`.
  - `CheckedWritesTxnState.ParkedVotesAreBounded`: 256 parks → true, the 257th → false.
  - `CheckedWritesTxnState.FeedHandlesType19AndMarkers`: `verdictFeedBatch` over a built `TxnJudge` batch, plus `verdictFeedMarker` → state as above. Other types are ignored.
  - `CheckedWritesTxnPool.AcquireReleaseAndLockSet`: reserve 8, acquire 8 → the ninth is −1. `lockSetFor(key)` lists only overlays bound to `key`. A release frees the slot.
  - `CheckedWritesRejectedIndex.InsertRunOutOfOrder`: entries {50, 60}; `insertRun({10, 11, 12}, 51, 7)` → size 5, ascending, `isRejected(11)`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_storage_tests`. Expected: `TxnJudgeState` undeclared.
- [ ] **Step 3: Implement** as specified. Every entry and park slot is reserved at `reserve()`. Lookups are linear over at most 1,024 entries, which is cold enough (once per transactional batch boundary and per marker); add an open-addressed pid index if the bench in Task 15 shows it.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnState|CheckedWritesTxnPool|CheckedWritesRejectedIndex|CheckedWritesVerdictSource|RcState' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/WriteState.h broker/libs/node/include/rune/node/VerdictSource.h \
          broker/libs/node/src/VerdictSource.cpp broker/libs/node/include/rune/node/RcState.h \
          broker/libs/storage/include/rune/storage/RejectedOffsetIndex.h broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp \
          broker/libs/storage/tests/storage/CheckedWritesRejectedIndexTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "node: TxnJudgeState, the TxnJudge and marker feed, the overlay pool (C7, C18)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 5: Deferral and run-ahead in `driveJudge`

**Files:**
- Modify: `broker/libs/node/src/VerdictSource.cpp` (`driveJudge`, `judgedThrough`, `judgeCursor`, activation K), `broker/libs/node/include/rune/node/VerdictSource.h`
- Test: `broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp`

**Interfaces:**
- Consumes: Task 4; 04's `driveJudge` order and P4-2/P4-3; Part 01's `isAbortedBatch`.
- Produces: P11-2. The caller (`WorkerViewSet::driveFolds`) passes the HW as `bound` for a verdict source; the change is in Task 7. Additions to 04's per-pass loop:
  ```
  for each batch at judgeCursor_ (bound = HW):
      control batch: type 19 -> nothing (already fed); txn COMMIT/ABORT -> nothing here (finalize is P11-4's step)
      transactional batch of pid p:
          t = state.txn.find(p) for p's current txn (t.firstOffset <= base)
          if t deferred (its J < HW):          t.records += n; judgeCursor_ = last + 1; continue     // skip, run ahead
          if p decided (marker < HW, no J):    ordinary: defer to the marker M (run ahead to M); at M judge and apply
                                               p's records in offset order in one step, superseded ones skipped (P11-3a);
                                               if p should have voted (P11-7): all or nothing at M with P11-3's no
          else:                                stop (wait at p's first record)
      atomic batch (kAttrAtomic, not transactional): judgeAtomic_(batch)                      // Task 6
      other: 04's per-record judge
      the judge reaches a TxnJudge offset J of an entry in AwaitJudge: judgeTxn_(t)             // Task 6
  judgedThrough() = min(judgeCursor_, state.txn.earliestUndecidedFirst())
  K contribution of the judge (P4-2) = judgedThrough()
  ```

- [ ] **Step 1: Write the failing tests** (04's unit harness: a fake `readRange` over `InMemoryLog` batches, members built as in `WorkerViewSetTest.cpp:100-170`, `TestReject{TagEquals{9}}` objectId 7):
  - `CheckedWritesTxnJudge.UndecidedTxnWithoutJudgeRecordWaitsAtItsFirstRecord`: plain records at 10–11, txn 77 at 12–13, plain at 14. Drive → `judgeCursor() == 12`, `judgedThrough() == 12`, and offset 14 is not judged.
  - `CheckedWritesTxnJudge.TxnWithACommittedJudgeIsDeferredAndTheJudgeRunsAhead`: the same log plus J (pid 77, first 12) at 15, HW 16 → plain 14 is judged (a tag-9 record there is rejected). `judgeCursor() == 16`, `judgedThrough() == 12`, and the records 12–13 have no verdict and no view row.
  - `CheckedWritesTxnJudge.OrdinaryCommittedTxnIsAppliedAtItsMarker`: txn 77 at 12–13, a plain record at 14, its commit marker at 15 and no J (the set is not constrained at 12 and 77 has no intents) → 14 is applied before 12–13, which apply in the step of 15; `txnMarkerWithoutVote == 0`.
  - `CheckedWritesTxnJudgeRegression.SupersededOrdinaryRecordAppliesNothingRegression` (P11-3a): 77 writes id 4 at 12, a plain upsert of id 4 at 14, marker at 15 → id 4's row is the record at 14, as after `compact()`.
  - `CheckedWritesTxnJudge.AbortedOrdinaryTxnIsSkipped`: an abort marker instead → nothing folded, nothing rejected.
  - `CheckedWritesTxnJudge.MarkersNeverPassAnUndecidedTxn`: deferred 77 at 12 → every marker emitted has `finalThrough <= 12` until 77 is decided.
  - `CheckedWritesTxnJudgeRegression.ActivationKNeverPassesAnUndecidedTxnRegression`: deferred 77, then `bindLeader` with a new generation → K ≤ 12, and the re-judge from K re-defers 77.
  - `CheckedWritesTxnJudge.FullTxnTableWaitsNeverJudges`: `kMaxTxns` deferred entries → the judge waits at the next transaction's first record. Counted.
- [ ] **Step 2: Run to verify they fail.** `ctest --test-dir build/root -R CheckedWritesTxnJudge --output-on-failure`. Expected: FAIL (the judge stops at the LSO-like bound or judges 12–13 at once).
- [ ] **Step 3: Implement** the loop above inside 04's `driveJudge`. Keep it allocation-free. The classification reads `state.txn` and the bound only.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnJudge|CheckedWritesVerdictSource|CheckedWritesTimeline' -j14 --output-on-failure`. Expected: PASS, with every Part 04, 05 and 09 unit test unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp \
          broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp
  git commit -m "node: the judge defers voted transactions and runs ahead to their TxnJudge; judgedThrough never passes one (P11-2)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 6: Judging at J, the conflict rule, locks, finalize, atomic batches, the marker-without-vote path

**Files:**
- Modify: `broker/libs/node/src/VerdictSource.cpp` (`judgeTxn_`, `finalizeLocked_`, `judgeAtomic_`, `judgeAtMarker_`), `broker/libs/node/include/rune/node/VerdictSource.h`
- Test: `broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp`

**Interfaces:**
- Consumes: Tasks 3–5.
- Produces:
  - **`judgeTxn_(DeferredTxn&)`:** acquire an overlay; read `[first, J)` for the pid; chain-prepare every member of `setAt(J)` per record with `locks = pool.lockSetFor(source)`. A `Locked` result pauses: the judge stays at J and adds the pass to `foldPausedOnTxnLockNs`. Then evaluate as `judge()` does, run the P11-3 scan, and write `t.answer`. A yes sets `Locked` with the overlay held; a no releases the overlay and sets `Judging → Finalized`.
  - **`finalizeLocked_()`:** for each `Locked` entry whose marker offset is below the HW, re-apply the transaction's records through each member's normal path in one step (commit), or release (abort). Every duty pass calls it first, before `driveJudge`'s loop.
  - **`judgeAtomic_`:** P11-6.
  - **`judgeAtMarker_`:** P11-7.
  - Reading and the conflict scan are each bounded by `kVoteScanBytesPerPass` per pass. The entry stays in `Judging`, keeping its overlay and the partial key set, across passes.

- [ ] **Step 1: Write the failing tests.** The members are `TestReject{CapLiveKeys{3}}` (objectId 7) and `TestReject{TagEquals{9}}` (objectId 8); Part 09's real members come in Task 7's rig tests.
  - `CheckedWritesTxnJudge.YesVoteLocksAndChangesNoTable`: deferred 77 writes ids 1 and 2 → yes, `phase == Locked`, both tables byte-identical to before (snapshot), `overlay >= 0`.
  - `CheckedWritesTxnJudge.NoVoteCarriesTheFirstFailingRecord`: 77 writes `{id 1 tag 1}, {id 2 tag 9}, {id 3 tag 9}` → no, `{51, 0x00080001, offset of id 2}`, the overlay released.
  - `CheckedWritesTxnJudge.TwoCreatesOfOneNameInOneTxnVoteNo`: `CapLiveKeys{1}`, 77 creates ids 5 and 6 → no on the second (the chain counted the first).
  - `CheckedWritesTxnJudge.TombstonesInATxnAreNeverRejected`: `CapLiveKeys{0}`, 77 deletes ids 1 and 2 → yes.
  - `CheckedWritesTxnJudge.LockedRowPausesThenJudgesAgainstTheFinalState`: 77 locked holding id 1. A plain upsert of id 1 at J + 2 → the judge stays at J + 2, `lockPauses == 1`, while id 9 at J + 1 was judged. After the commit marker below the HW, `finalizeLocked_` applies 77, and the paused record is judged against the state including 77.
  - `CheckedWritesTxnJudge.AbortAfterYesReleasesTheOverlay`: locked, then an abort marker → the overlay released, the tables unchanged, and the paused record judged against the old state.
  - `CheckedWritesTxnJudge.CommitFinalizesBeforeTheCursorReachesTheMarker`: the cursor is paused below the marker → `finalizeLocked_` still commits in the pass where the marker is below the HW.
  - `CheckedWritesTxnJudgeRegression.OutsideWriteNewerThanTheTxnsLastWriteVotesConflictRegression` (Review Focus 1): 77 writes id 4 at 12; a plain upsert of id 4 at 14 is accepted; J at 16 → no, `{59, 0, 14}`, `votesNoConflict == 1`.
  - `CheckedWritesTxnJudgeRegression.OutsideWriteOlderThanTheTxnsLastWriteVotesYesRegression`: 77 writes id 4 at 12 and 15; the plain upsert of id 4 at 14 → yes. After commit the view's id 4 is the record at 15, and so is the latest record of id 4 after `compact()`.
  - `CheckedWritesTxnJudge.RejectedOutsideWriteIsNoConflict`: the outside record at 14 is rejected (tag 9) → yes.
  - `CheckedWritesTxnJudge.AtomicBatchOneBadRecordRejectsTheWholeBatch`: an atomic batch of 3 records with the second tag 9 → all 3 offsets in `rejected` with `{51, 0x00080001}`, no table changed. The same batch without the bit → only the second is rejected.
  - `CheckedWritesTxnJudge.AtomicBatchPassesAppliesAllInOneStep`: all 3 rows appear in the same pass.
  - `CheckedWritesTxnJudgeRegression.CommitMarkerWithoutJudgeJudgesAllOrNothingAtTheMarkerRegression`: the set is constrained at 77's first record and there is no J. The commit marker at 20 → 77 judged at 20. One tag-9 record → every record of 77 rejected via `insertRun`, `txnMarkerWithoutVote == 1`, and the next marker lists them.
  - `CheckedWritesTxnJudge.NoFreeOverlayWaitsAtJ`: 8 overlays held → the ninth judgement waits at J and counts `txnOverlayWaits`. A release lets it proceed with the same answer.
  - `CheckedWritesTxnJudge.JudgingSpansPassesWithABoundedRead`: a 4,096-record transaction behind 8 MiB of other batches → the answer arrives after several passes, identical to one unbounded pass.
- [ ] **Step 2: Run to verify they fail.** `ctest --test-dir build/root -R CheckedWritesTxnJudge --output-on-failure`. Expected: FAIL.
- [ ] **Step 3: Implement** as specified. The P11-3 scan, the one algorithm the tests do not fully fix:
  ```
  keys := hash map from T's records: sourceKey -> last_T(k)          // reserved at bindTxn, <= txnJudgedRecordsMax
  for each batch b in (T.first, J), b not of T, b not control:
      accepted(r) := !rejected.isRejected(r.offset)
                     && (b non-transactional
                         || (ordinary txn of b.pid committed below J)
                         || (deferred txn q with q.J < J && q committed))      // q is Finalized-commit by now (pause rule)
      for each record r of b with keys.contains(r.key) && r.offset > keys[r.key] && accepted(r):
          return No{TxnConflict, 0, r.offset}
  ```
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnJudge|CheckedWritesTxnOverlay|CheckedWritesVerdictSource' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/VerdictSource.h broker/libs/node/src/VerdictSource.cpp \
          broker/libs/node/tests/node/CheckedWritesTxnJudgeTest.cpp
  git commit -m "node: judge transactions at TxnJudge in a chained overlay; locks, pause, finalize, conflict 59, atomic batches, marker-without-vote" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 7: Worker wiring: the vote op, parked answers, marker parking, checkpoints, the cleaner bound, isolation, stats, the knob

**Files:**
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `executeTxnVoteWire_` (a new case next to `AppendControlRecord` at `:2632` and in the role-gated switches `:1221`, `:6177`, `:6697`);
  - `answerTxnVotes_()`, called right after `driveViews_` in both duty loops (`:1846`, `:1979`);
  - in `executeAppendControlRecordWire_` (`:4933-5060`), after a successful marker append, `writeOf_(replica)->txn.onMarker(...)` through the RC hook, plus P11-12's parking condition;
  - `effectiveIsolation_` beside `verdictBound_`;
  - `publishVerdictCleanerBound_` publishes `txnJudgeBound`;
  - `setTxnJudgedRecordsMax`, `txnStatsForTest`, `parkedVotesForTest`.
- Modify: `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}`:
  - `driveFolds` passes the HW as the judge bound for a verdict source;
  - `txnOverlays_.reserveFor(...)` at verdict member install;
  - `bindTxn` on each `VerdictSource`;
  - the P11-8 checkpoint rules in the member push path and the backfill in member replay.
- Modify: `broker/libs/node/include/rune/node/StatsCollect.h` (ids 11–20), `broker/libs/node/{include/rune/node/NodeConfig.h,src/NodeConfig.cpp}` (beside `verdictIntervalMs`), `src/BrokerNode.cpp` (pass the knob to every worker)
- Modify: `broker/libs/replication/include/rune/replication/PartitionReplica.h` (beside `cleanerVerdictBound`), `broker/libs/node/include/rune/node/LogCleanerAgent.h:455`, `broker/libs/storage/{include/rune/storage/LogCleaner.h,src/LogCleaner.cpp,include/rune/storage/VerdictCleanState.h}`
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (contract addition 19, rig part)
- Create: `broker/libs/node/tests/node/CheckedWritesTxnWorkerTest.cpp` (add to `rune_checked_writes_verdict_tests`)
- Test: also `broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp`

**Interfaces:**
- Consumes: Tasks 1–6; 08's `StatsSink`; 04's rig and `publishVerdictCleanerBound_`.
- Produces: contract additions 12, 17, 19 and 20, and the vote handling of P11-1 on the leader:
  ```
  executeTxnVoteWire_(op):
      gate (ownership, roleEpoch like markers) else answer NotLeader
      ws = writeOf_(replica); no ws or !ws->isVerdictSource() -> answer yes
      first = rc->tracker().firstOffsetOf(pid); first < 0 -> answer yes
      t = ws->txn.find(pid) with t.firstOffset == first and a J -> park with t (answered when judged)
      else start (or join) the vote-time scan (Scanning), park; on scan end:
          !votes(t) -> answer yes;  t.records > txnJudgedRecordsMax -> answer No{55}
          wait HW > t.lastOffset -> append TxnJudge{pid, epoch, first} (scratch reserved) -> AwaitJudge
  answerTxnVotes_(): ws->txn.drainAnswered -> enqueueReturn_(TxnVoteResult{…}); latency into the stats
  ```
  A park reserves an owed return slot first (`ackReservationAvailable_`), like a parked marker. Without one the answer is `NotLeader` (retriable).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesTxnWorkerTest.cpp`). The rig is set up with Part 09's `createCharacterTopic()`, `character_name`, `account_stats` and `createLedgerTopic()` + `Ledger`.
  ```cpp
  TEST(CheckedWritesTxnWorker, VoteYesThenCommitAppliesInOneStep) {
      CheckedWritesRig rig(tmp("txn-yes"));
      // ...setup as above; U has 5 characters...
      rig.appendCharacters(createFor(U, "Xan"), /*acks*/1, /*pid*/77, /*seq*/0, /*txn*/true);
      rig.followerFetch();
      const int64_t tok = rig.sendVote(1, 77, 0);
      ASSERT_TRUE(rig.driveUntilVoted(tok));
      EXPECT_EQ(rig.voteReply(tok)->code, 0);
      EXPECT_EQ(rig.txnJudgeOffsetsInLog().size(), 1u);
      EXPECT_EQ(rig.viewRow(accountStats, U)->characters, 5) << "provisional rows must not be visible";
      (void)rig.commitTxn(77);
      ASSERT_TRUE(rig.driveUntilJudged(rig.leo()));
      EXPECT_EQ(rig.viewRow(accountStats, U)->characters, 6);
  }
  ```
  - `CheckedWritesTxnWorker.NonVotingPartitionAnswersYesWithoutWriting`: a transaction on a key-index-only topic with plain upserts → yes, no type-19 record, LEO unchanged.
  - `CheckedWritesTxnWorker.VoteForAnUnknownTxnAnswersYes`: pid 99 with no open transaction → yes.
  - `CheckedWritesTxnWorker.ExplicitIntentMakesAKeyIndexOnlyPartitionVote`: an `update` intent inside the transaction on a key-index-only topic → J appended, judged (53 on an absent key).
  - `CheckedWritesTxnWorker.OverTheCapVotesTooLargeWithoutAJudgeRecord`: `setTxnJudgedRecordsMax(4)` and 5 records → `{55}`, no type-19 record, `votesNoTooLarge == 1`.
  - `CheckedWritesTxnWorker.RepeatedVoteIsAnsweredFromTheSameJudge`: two votes for (77, 0) → one J, two identical replies.
  - `CheckedWritesTxnWorkerRegression.VoteWaitsForTheHwBeforeAppendingTheJudgeRegression`: HW below the transaction's last record → no J until `followerFetch()`.
  - `CheckedWritesTxnWorkerRegression.UncommittedJudgeIsNeverAnsweredRegression`: J appended, then `failover()` before the follower fetch → no reply. The re-sent vote appends a new J and gets the answer.
  - `CheckedWritesTxnWorkerRegression.MarkerAnswerWaitsForFinalizeRegression` (P11-12): yes, then the commit marker op → its `ControlRecordAppended` is not returned before the pass that finalized the transaction.
  - `CheckedWritesTxnWorker.NoVoteBurnsTheTransactionsIds`: a transaction creating 2 characters for a user at 11 → `{51, max_characters}`. After the rig aborts, the next plain create gets an id above both burned ids, and the key index has neither (P6-13).
  - `CheckedWritesTxnWorkerRegression.VotedAbortedTxnNeverReachesAnyViewAndRefoldsEqualRegression` (Part 01): yes then abort → no view, consumer or index entry has the records. A `restart()` refold gives byte-identical tables.
  - `CheckedWritesTxnWorkerRegression.DropWhileLockedKeepsThePlanUntilWRegression` (R04a): `character_name` is dropped while 77 is locked, then `failover()` → the new leader re-judges 77 at J with the retained plan (type 20) and gives the same answer. The plan is uninstalled only once W passes the drop record, which happens after 77's marker.
  - `CheckedWritesTxnWorkerRegression.RunAheadCheckpointBackfillsTheTxnAfterARestartRegression` (Review Focus 2):
    - 77 writes at 100, then 5,000 plain records, then the vote;
    - a member checkpoint is pushed during run-ahead at O between 100 and J;
    - commit; crash before the next push (stop, keep files), `restart()`;
    - `checkpointBackfills == 1`, and `account_stats` equals a fresh refold.
  - `CheckedWritesTxnWorkerRegression.NoCheckpointPushWhileLockedRegression`: while 77 is locked, `vetoedCheckpointPushes > 0` and no delta is pushed. After finalize a push happens.
  - `CheckedWritesTxnWorker.ReadUncommittedOnAVerdictSourceSkipsAbortedTxns` (P11-10): a read_uncommitted subscription never receives 77's records after a no vote.
  - `CheckedWritesTxnWorker.StatsElevenToTwentyAreServed`: `rig.getStats(TopicPartition, 1, 0)` lists ids 11–20 with the counts of the above.
  - `CheckedWritesVerdictCleanerRegression.TxnJudgeKeptUntilEveryMemberCheckpointPassedItRegression` (storage): `txnJudgeBound` at J → kept; at J + 1 → removable after the horizon.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests rune_storage_tests`. Expected: compile FAIL on `sendVote`, then assertion FAILs.
- [ ] **Step 3: Implement** as specified. The `TxnJudge` scratch is per worker, reserved to `kTxnJudgeBatchBytes`. `NodeConfig` parses `RUNE_TXN_JUDGED_RECORDS_MAX` with the existing integer helper beside `RUNE_VERDICT_INTERVAL_MS`, refuses a value outside 1–1,048,576 at startup with a message naming the variable, and defaults to 4096.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests rune_checked_writes_tests rune_storage_tests && ctest --test-dir build/root -R 'CheckedWritesTxn|CheckedWritesVerdict|CheckedWritesConstraint|CheckedWritesKeyIndex|CheckedWritesAutoInc|ShardWorkerAck|TxnMarker' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/Worker.h broker/libs/node/include/rune/node/WorkerViewSet.h \
          broker/libs/node/src/WorkerViewSet.cpp broker/libs/node/include/rune/node/StatsCollect.h \
          broker/libs/node/include/rune/node/NodeConfig.h broker/libs/node/src/NodeConfig.cpp broker/libs/node/src/BrokerNode.cpp \
          broker/libs/replication/include/rune/replication/PartitionReplica.h broker/libs/node/include/rune/node/LogCleanerAgent.h \
          broker/libs/storage/include/rune/storage/LogCleaner.h broker/libs/storage/src/LogCleaner.cpp \
          broker/libs/storage/include/rune/storage/VerdictCleanState.h broker/libs/node/tests/support/CheckedWritesRig.h \
          broker/libs/node/tests/node/CheckedWritesTxnWorkerTest.cpp broker/libs/storage/tests/storage/CheckedWritesVerdictCleanerTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "node: the TxnVote op on the leader - scan, TxnJudge, parked answers, final rows before the marker answer, checkpoints, stats 11-20" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 8: One fold order on every path, and level 3a (R11b)

**Files:**
- Modify: `broker/libs/view/{include/rune/view/ViewFold.h,src/ViewFold.cpp,include/rune/view/AggFold.h,src/AggFold.cpp}` (`foldBatch` / `foldAggregateBatch` take the instance's `TxnSpanFence*` and a `ReadRangeFn` for read-back: a transactional batch is recorded and skipped; a commit marker reads `[first, M)` for its pid and applies it; an abort marker closes the span)
- Modify: `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}` (every caller: `driveView_`, `driveRebuild`, recovery replay, joining members and candidates; the push veto; `readFenced`), `broker/libs/node/src/VerdictSource.cpp` (activation replay uses the same fold), `broker/libs/node/include/rune/node/Worker.h` (`viewRowGate_` holds a read while `readFenced`)
- Create: `broker/libs/node/tests/node/CheckedWritesTxnVisibilityTest.cpp` (in `rune_checked_writes_verdict_tests`)

**Interfaces:**
- Consumes: Part 01's `FoldSkip`, Part 07's read ops (`GetViewRowK`, `ScanViewRows`), 04's held-read machinery.
- Produces: P11-0, P11-9 and contract addition 13. The hot cost for non-transactional traffic is the existing per-batch transactional-bit test; 04's legacy identity test is extended to logs with committed transactions against an apply-at-marker reference (`LegacyFoldReference` keeps offset order and is used only for logs without interleaving).

- [ ] **Step 1: Write the failing tests** (Part 03's `CheckedWritesAckRig` on a partition with ordinary views; persisted views with a pass cap forced to 2 records):
  - `CheckedWritesTxnVisibility.CommittedTxnLargerThanThePassCapIsNeverReadHalfApplied`: a committed transaction of 6 records → at every pass, `GetViewRow` of its keys either holds (fenced) or returns all 6 new rows.
  - `CheckedWritesTxnVisibilityRegression.CommittedSpanCrossingTheLsoNeverHoldsReadsRegression` (Review Focus 4): A's records at 10–12, B opens at 13, A's marker at 20, B open for 10 s of the injected clock → reads are served (not held) the whole time with A's rows absent. After B's marker, A and B each appear in one pass.
  - `CheckedWritesTxnDeterminismRegression.LiveEqualsRefoldEqualsRebuildUnderInterleavedTxnsRegression` (R11b): seeds 1–100. Interleave transactions of 3 producers (commit and abort, 1–8 records, shared keys and groups) with plain writes on the same keys and groups, on a source with a `count(*)`, `last(v)`, `lastNonNull(v)` aggregate by group plus `check (lv >= 0)` over the last column (a verdict member) and the same aggregate without the check (an ordinary view). Then compare byte for byte: (1) the live fold, (2) `restart()` with the checkpoints deleted (refold from 0), (3) a second copy installed after the writes (`driveRebuild`), and (4) a `failover()` leadership scan with activation replay. Every table equal, every verdict in `rejected` equal, `mismatches() == 0`.
  - `CheckedWritesTxnVisibilityRegression.CheckpointNeverInsideAnOpenSpanRegression`: a persisted view with pass cap 2 and a 6-record transaction → no pushed delta's O satisfies `first < O ≤ M`; a crash at every pass boundary then `restart()` equals the undisturbed tables.
  - `CheckedWritesTxnVisibility.AtomicBatchSplitByThePassCapIsFenced`: a 6-record atomic batch on a plain partition → no read sees part.
  - `CheckedWritesTxnVisibility.SpanTableFullWaitsNeverSkips`: 65 concurrent open spans → the fold waits at the 65th transactional batch, and applies everything once a span closes.
  - `CheckedWritesTxnVisibility.NoTransactionsNoFence`: plain traffic → `readFenced` never true and folds byte-identical to before.
- [ ] **Step 2: Run to verify they fail.** `ctest --test-dir build/root -R CheckedWritesTxnVisibility --output-on-failure`. Expected: FAIL.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnVisibility|CheckedWritesTxnDeterminism|GetViewRow|ViewRecovery|WorkerViewSet|RcState|CheckedWritesViewAck|CheckedWritesAbortedFold|CheckedWritesPrepareApply|ViewFold|AggFold' -j14 --output-on-failure`. Expected: PASS. A pre-existing test that pinned offset-order application of an interleaved transaction is re-pinned to marker order and recorded as a deviation (R11b) in the commit body.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/view/include/rune/view/ViewFold.h broker/libs/view/src/ViewFold.cpp broker/libs/view/include/rune/view/AggFold.h \
          broker/libs/view/src/AggFold.cpp broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
          broker/libs/node/src/VerdictSource.cpp broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/tests/node/CheckedWritesTxnVisibilityTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "node/view: every fold path applies a transaction at its commit marker; read fence for split applications (R11b, P11-0, P11-9)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint B (controller).** The same commands as Checkpoint A, plus `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure`.

---

## Phase 3 — Coordinator, routing, client, lifting the refusals, matrix, e2e, docs

### Task 9: Routing votes: the gateway, the cluster plane, leadership changes

**Files:**
- Modify: `broker/libs/gateway/include/rune/gateway/ClientGateway.h` (`:957-1000`, `TxnMarkerChannel` and beside it), `broker/libs/gateway/src/ClientGateway.cpp`:
  - `sendTxnVote` beside `appendControlRecord` (`:9556`), sharing `markersInFlight_`'s token incarnation with a separate `votesInFlight_` table;
  - `onRemoteTxnVote` / `onRemoteTxnVoteResponse` beside `:6558-6600`;
  - re-send on reconnect in `pumpTxnMarkers_` (`:6728`);
  - the `TxnVoteResult` return drain next to `ControlRecordAppended`.
- Modify: `broker/libs/replication/include/rune/replication/AeronPartitionTransport.h` (`:354-372`), `src/AeronPartitionTransport.cpp`, `broker/libs/node/src/BrokerNode.cpp` (`:2561-2570`: the new channel members and the vote sink to the coordinator)
- Create: `broker/libs/node/tests/integration/CheckedWritesTxnVoteRoutingTest.cpp` (add to `rune_d9_cross_node_marker_tests`, `broker/libs/node/CMakeLists.txt:1272`)

**Interfaces:**
- Consumes: Tasks 1, 2, 7.
- Produces: contract addition 15.
  - A local leader gets a `TxnVote` op. A remote one gets a `TxnVoteMsg` with `leaderEpoch`.
  - A routing-image leader change fails every in-flight vote to the old leader with `TxnVoteResult{errorCode = NotLeader}`. A plane reconnect re-sends with the same token.
  - Answers that match no in-flight token are dropped.

- [ ] **Step 1: Write the failing tests** (the D9 two-node harness of `D9CrossNodeMarkerTest.cpp`):
  - `CheckedWritesTxnVoteRouting.LocalVoteReachesTheWorkerAndAnswers`.
  - `CheckedWritesTxnVoteRouting.RemoteVoteRoundTripsOverThePlane`: node 0 coordinates and node 1 leads → `TxnVoteResponseMsg` back on node 0 with the worker's fields.
  - `CheckedWritesTxnVoteRoutingRegression.LeaderChangeFailsInFlightVotesNotLeaderRegression`.
  - `CheckedWritesTxnVoteRoutingRegression.ReconnectResendsWithTheSameTokenRegression`: the link drops, then comes back up → one re-send. A late duplicate answer is ignored.
  - `CheckedWritesTxnVoteRouting.OlderLeaderEpochIsRefusedNotLeader`: `leaderEpoch` below the replica's → `NotLeader`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_d9_cross_node_marker_tests`. Expected: `sendTxnVote` undeclared.
- [ ] **Step 3: Implement** as specified, mirroring the marker path and its comments.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnVoteRouting|D9CrossNode|TxnMarker' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/gateway/include/rune/gateway/ClientGateway.h broker/libs/gateway/src/ClientGateway.cpp \
          broker/libs/replication/include/rune/replication/AeronPartitionTransport.h broker/libs/replication/src/AeronPartitionTransport.cpp \
          broker/libs/node/src/BrokerNode.cpp broker/libs/node/tests/integration/CheckedWritesTxnVoteRoutingTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "gateway: route TxnVote locally and over the cluster plane like markers (C18)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 10: The coordinator's vote gate

**Files:**
- Modify: `broker/libs/node/include/rune/node/TransactionCoordinationService.h` (`VoteGate`, `voteGates_`, `voteCandidatesAtAdd_` (in memory; on `load_` every partition of a recovered transaction is a candidate, R11c), `lastVoteNo_`, the setters of contract addition 16), `broker/libs/node/src/TransactionCoordinationService.cpp`:
  - `handleEndTxn_` (`:969-1047`) before `coordinator_.endTransaction`;
  - `handleAddPartitions_` (`:680`), which records candidates and clears `lastVoteNo_`;
  - `doWork` (`:306`), which re-sends due votes and detects stale gates;
  - `dropDeferred_` (`:585`), which drops gates;
  - `sendEndTxnAnswer_` (`:463`) and `completeAwaiting_` (`:1125`), which carry the vote fields and map them by session version.
- Modify: `broker/libs/node/src/BrokerNode.cpp` (`setVoteCandidateFn` from the node's catalog state beside `postCatalogValidator_`, `BrokerNode.h:1102`; `setSessionVersionFn` from the gateway's session table)
- Create: `broker/libs/node/tests/node/CheckedWritesTxnCoordinatorTest.cpp` (add to `rune_txn_coordination_service_tests`, `broker/libs/node/CMakeLists.txt:475`)

**Interfaces:**
- Consumes: Task 9's `sendTxnVote` and `TxnVoteResult`; the existing `PrepareGate` / `AwaitingMarkers` machinery.
- Produces: P11-11 and contract addition 16. `PrepareGate` and `AwaitingMarkers` append `TxnVoteAnswer vote{}; bool hasVote = false;`, so the deferred answer carries the no.

- [ ] **Step 1: Write the failing tests** (the fixture of `TransactionCoordinationServiceTest.cpp`, with `setVoteFnOverride` capturing votes, `setVoteCandidateFn` naming topic `cw` as a candidate, and `setAppendFnOverride` for markers):
  - `CheckedWritesTxnCoordinator.CommitWithACandidateSendsVotesAndPersistsNothing`: after `EndTxn(commit)`, one captured vote per candidate partition, the state still `Ongoing`, no new txn-state record, and the answer deferred.
  - `CheckedWritesTxnCoordinator.AllYesRunsTodaysCommitPath`: every `onTxnVoteResult` is yes → PrepareCommit is persisted and the markers are dispatched. The answer is 0 once they land.
  - `CheckedWritesTxnCoordinator.FirstNoAbortsAndAnswersTheVote`: one no `{51, 0x00070000, 123}` → PrepareAbort and abort markers. The EndTxn answer, once they land, is `{51, hasVote, 0x00070000, 123}` (a v12 session). A later yes for the other partition changes nothing.
  - `CheckedWritesTxnCoordinator.NoCandidateNoVoteNoRoundTrip`: a non-candidate topic → no vote captured, today's path.
  - `CheckedWritesTxnCoordinator.CandidateAtAddPartitionsIsAskedAfterItsConstraintDropped`.
  - `CheckedWritesTxnCoordinatorRegression.FailoverBetweenAddPartitionsAndCommitCollectsEveryVoteRegression` (R11c): `AddPartitions` for `cw-0` (constrained) and `plain-0` (not a candidate), then unload and reload (the persisted state only), then `EndTxn(commit)` → one vote captured per partition of the persisted set (both), the gate resolves only after both answers, and a no from `cw-0` aborts as in `FirstNoAbortsAndAnswersTheVote`. With `voteCandidate_` returning false for every topic after the reload (the constraint was dropped meanwhile) both votes are still sent.
  - `CheckedWritesTxnCoordinator.NotLeaderVoteIsResentWithBackoff`: re-sent at 10 ms, then 20 ms.
  - `CheckedWritesTxnCoordinator.TimeoutDuringTheVoteAbortsAndAnswersInvalidTxnState`: the clock advances past `txnTimeoutMs` → PrepareAbort, and the deferred answer is 19.
  - `CheckedWritesTxnCoordinator.RepeatedCommitTakesOverTheGate`: the first request is answered 27, the second gets the final answer.
  - `CheckedWritesTxnCoordinator.AbortDuringTheVoteCancelsTheGate`: the commit request is answered 27, and the abort proceeds.
  - `CheckedWritesTxnCoordinatorRegression.UnloadWithVotesOutThenRetryGetsTheSameAnswerRegression`:
    - unload while votes are out → `NotCoordinator`, and `voteGatesForTest() == 0`;
    - reload → the transaction is `Ongoing`;
    - the client's retry sends new votes;
    - answering them with the same no → the same answer.
  - `CheckedWritesTxnCoordinator.RememberedNoAnswersALateRetry`: after the no completed, an `EndTxn(commit)` with the same pid and epoch → the same no, not 19.
  - `CheckedWritesTxnCoordinatorRegression.OldSessionNoVoteIsInvalidRequestThenAbortSucceedsRegression` (Review Focus 5): session version 11 → 39 with no trailer. An `EndTxn(abort)` then → 0.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_txn_coordination_service_tests`. Expected: `setVoteFnOverride` undeclared.
- [ ] **Step 3: Implement** P11-11. Gates live in a `std::vector<VoteGate>` reserved at construction, on the control plane (cold), like `prepareGates_`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesTxnCoordinator|TransactionCoordinationService|TxnStateReplication|TxnMarker' -j14 --output-on-failure`. Expected: PASS; every existing coordinator test is unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/TransactionCoordinationService.h broker/libs/node/src/TransactionCoordinationService.cpp \
          broker/libs/node/src/BrokerNode.cpp broker/libs/node/tests/node/CheckedWritesTxnCoordinatorTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "txn: the coordinator votes before PrepareCommit; a no aborts and answers with the code, constraint id and offset (G.2)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 11: The client: vote results, `TxnCommitResult`, atomic batches

**Files:**
- Modify:
  - `client-cpp/include/rune/client/producer/TxnTypes.h:66` (`Verdict::VotedNo`) and `client-cpp/src/producer/CoordinatorLink.cpp:28-54`;
  - `client-cpp/src/producer/ProducerHub.cpp:158` (decode the trailer into `CoordAnswer`, which appends `uint32_t constraintId = 0; int64_t failingOffset = -1; bool hasVote = false;`);
  - `client-cpp/src/producer/ProducerTxn.cpp:752` (`onEndTxnAnswer_`);
  - `client-cpp/include/rune/client/producer/Producer.h:822-833` and `client-cpp/src/producer/Producer.cpp:513-573` (`TxnCommitResult`);
  - `client-cpp/include/rune/client/producer/ProduceFrameBuilder.h:72-77` (`setAtomic`) and `client-cpp/src/producer/ProducerImpl.cpp:295-300` (Tier-2 slot builders call it);
  - the Tier-2 ack path in `ProducerImpl.cpp`, where a trailer entry becomes the batch's completion;
  - Part 05's local refusal list (P5-9, in `ProducerImpl.cpp`).
- Create: `client-cpp/tests/producer/CheckedWritesTxnVoteClientTest.cpp` (add to `rune_invoker_tests`, `client-cpp/CMakeLists.txt:1531-1540`)

**Interfaces:**
- Consumes: Task 1.
- Produces: P11-13 and contract addition 18.

- [ ] **Step 1: Write the failing tests** (the identity stub of `ProducerTxnDataTest.cpp`, the coordinator answers injected):
  - `CheckedWritesTxnVoteClient.NoVoteCompletesTheCommitWithItsFields`: an EndTxn answer `{51, hasVote, 0x00070000, 123}` → the commit completion has `BrokerError`, `errorCode 51`, `reserved0 == 0x00070000`, `value == 123`. The producer state is `Ready`, and `beginTransaction()` succeeds.
  - `CheckedWritesTxnVoteClient.CommitAndWaitReturnsTheResult`: the same answer → `commitTransactionAndWait()` returns `{ok false, 51, 0x00070000, 123}` and does not throw. A 0 answer → `ok`.
  - `CheckedWritesTxnVoteClientRegression.NoVoteOnAResentCommitIsDefiniteNeverFatalRegression`.
  - `CheckedWritesTxnVoteClient.EveryVoteCodeClassifiesVotedNo`: for 51, 52, 53, 55 and 59 → `Verdict::VotedNo`. 19 is still `Fatal`, and 39 is still `Reject`.
  - `CheckedWritesTxnVoteClient.IntentsInATransactionAreSent`: an `update` intent from a transactional `All` producer → a frame with intent bits 2 and `acks == 1`, no local refusal.
  - `CheckedWritesTxnVoteClient.Tier2BatchCarriesTheAtomicBit` (also on a transactional producer's Tier-2 batch: both bits set).
  - `CheckedWritesTxnVoteClient.RejectedAtomicBatchCompletesOnceWithTheEntry`: a v12 ack with one trailer entry `(1, 51, 0x00070000)` → one completion `BrokerError 51`, `reserved0` equal, `value == base + 1`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_invoker_tests`. Expected: `VotedNo` undeclared.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesTxnVoteClient|ProducerTxn|CoordinatorLink|BatchBuilder|CheckedWritesIntents' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/producer/TxnTypes.h client-cpp/src/producer/CoordinatorLink.cpp \
          client-cpp/src/producer/ProducerHub.cpp client-cpp/src/producer/ProducerTxn.cpp \
          client-cpp/include/rune/client/producer/Producer.h client-cpp/src/producer/Producer.cpp \
          client-cpp/include/rune/client/producer/ProduceFrameBuilder.h client-cpp/src/producer/ProducerImpl.cpp \
          client-cpp/tests/producer/CheckedWritesTxnVoteClientTest.cpp client-cpp/CMakeLists.txt
  git commit -m "client: commitTransaction reports the no vote (code, constraint id, offset); Tier-2 batches carry kAttrAtomic" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 12: Lift the pre-G refusals

**Files:**
- Modify: `broker/libs/node/include/rune/node/CheckedWriteHook.h` and `src/CheckedWriteHook.cpp` (P11-14), `broker/libs/node/include/rune/node/Worker.h` (`produceCore_` (a): `atomicBatch` from the header)
- Modify the tests that pinned the refusals:
  - `broker/libs/node/tests/node/CheckedWritesHookTest.cpp`: 03's pinned rows 13, 14 and 15 → `OK`; sweep invariant (i) loses its rule-7 exception; new rows `AtomicBatchTooLarge` (4097 records, `txnJudgedRecordsMax 4096`) and rule 5's transactional exception;
  - `CheckedWritesVerdictWorkerTest.cpp`: 04's `HookFactsFollowTheTimeline` → "a transactional batch is then accepted";
  - `CheckedWritesConstraintWorkerTest.cpp`: 09's `ConstrainedSourceRefusesTransactions` → `ConstrainedSourceAcceptsTransactions`, with a transactional batch to `character` accepted, voted and committed;
  - `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h`: 05's not-applicable rule 2 and 06's rule 2 are deleted, and their pin rows change (05: 570 applicable and 60 skipped; 06: applicable +12, skipped −12);
  - `client-cpp/tests/` (05's `CheckedWritesIntents.LocalRefusalsAreWriteNotAllowed` loses its transaction case).
- Test: `rune_checked_writes_tests`, `rune_checked_writes_verdict_tests`, `rune_checked_writes_matrix_tests`, client `rune_invoker_tests`

**Interfaces:**
- Consumes: Tasks 5–11 (the vote works end to end, so lifting is safe).
- Produces: P11-14.

- [ ] **Step 1: Re-pin the tests first** (they fail against the old rules). Each commit body records the deviation per test: "pre-G refusal lifted by Part 11, spec §9b".
- [ ] **Step 2: Run to verify they fail.** `ctest --test-dir build/root -R 'CheckedWritesRequirementTable|CheckedWritesHook|CheckedWritesVerdictWorker|CheckedWritesConstraintWorker|CheckedWritesMatrixCount' --output-on-failure`. Expected: FAIL on the changed rows and pins.
- [ ] **Step 3: Implement** P11-14.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWrites' -j14 --output-on-failure` and `ctest --test-dir build/sub-client-cpp -R CheckedWrites -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/CheckedWriteHook.h broker/libs/node/src/CheckedWriteHook.cpp \
          broker/libs/node/include/rune/node/Worker.h broker/libs/node/tests/node/CheckedWritesHookTest.cpp \
          broker/libs/node/tests/node/CheckedWritesVerdictWorkerTest.cpp broker/libs/node/tests/node/CheckedWritesConstraintWorkerTest.cpp \
          broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h client-cpp/tests client-cpp/src/producer/ProducerImpl.cpp
  git commit -m "node: lift the pre-G refusals - transactions and atomic batches on constrained sources, intents inside them (P11-14)" \
             -m "Deviation: 03 rows 13-15, 04 HookFactsFollowTheTimeline, 09 ConstrainedSourceRefusesTransactions, 05/06 matrix NA rules re-pinned: the refusals they pinned were 'until Part 11'." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 13: The shared matrix rows for 11, the model check and the cluster stress

**Files:**
- Modify: `broker/libs/node/tests/support/{CheckedWritesMatrixExpectations.h,CheckedWritesModel.h,CheckedWritesStressDriver.h,CheckedWritesRig.h}`, `broker/libs/node/tests/node/{CheckedWritesMatrixTest.cpp,CheckedWritesModelTest.cpp}`, `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp`
- Test: `rune_checked_writes_matrix_tests`, `rune_checked_writes_stress_tests`

**Interfaces:**
- Consumes: Tasks 3–12; 04's harness; 09's character and ledger setup.
- Produces: Interaction matrix A, C and D, and contract addition 19 (model, matrix, stress).

- [ ] **Step 1: Write the matrix, model and stress** exactly as Interaction matrix A, C and D specify:
  - `CheckedWritesMatrix/Cell11.MatchesTheModel/<n>`;
  - `CheckedWritesModel.TxnRulesMatchTheTable`;
  - `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 11: 602 / 126);
  - `CheckedWritesModelRegression.SeededTransactionsMatchTheModel`, `…SameSeedSameTraceTxn`;
  - `CheckedWritesStressRegression.SeededTransactionsHoldTheInvariants`.
- [ ] **Step 2: Run them.** `cmake --build build/root -j14 --target rune_checked_writes_matrix_tests rune_checked_writes_stress_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix|CheckedWritesModel|CheckedWritesStress' -j14 --output-on-failure`. Expected: PASS. A failure prints its cell or `seed=<n> step=<k> action=<name>`; fix the cause with a `*Regression*` test in the owning task's file.
- [ ] **Step 3: Mutation check** (do it, then revert): disable P11-3's scan. The `OutsideWriteSameKey` cells and the seeded model check must turn red.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/support broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp \
          broker/libs/node/tests/node/CheckedWritesModelTest.cpp broker/libs/node/tests/integration/CheckedWritesStressTest.cpp
  git commit -m "test: Part 11 matrix rows (602 cells), the transaction model check and the TxnAllOrNothingAndAtomicPerPartition stress" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 14: The real e2e: real clients on real brokers, and the docker `TXN` leg

**Files:**
- Create: `broker/libs/node/tests/client/CheckedWritesTxnBrokerRegressionTest.cpp` (add to `rune_client_broker_tests`, `broker/libs/node/CMakeLists.txt:1731`)
- Modify: `broker/docker/checked-writes-e2e.sh` (the leg after 09's `REGRESSORSPATH`), `client-cpp/examples/checked_writes_probe.cpp` (modes `txn-setup`, `txn-run --worker <i> --users <a>-<b>`, `txn-verify`), `docs/testing.md` (the row of `checked-writes-e2e.sh`)

**Interfaces:**
- Consumes: everything. The schema is the golden `character` (09) plus `create topic ledger { entryId uint64 autoincrement, account uint64, amount int64, memo string(16) } key entryId with (cleanup.policy = compact);` and `create view balances key account as select sum(amount) as balance from ledger group by account constraint non_negative check (balance >= 0);`.

- [ ] **Step 1: Write the tests** (default shard runtime; `OneBroker` unless named `ThreeNodes`; real `Client`s, `TypedTopic`, and transactional producers with `All`):
  - `CheckedWritesTxnBrokerRegression.TwoPartitionsOneNoNothingCommitsShardRegression`: a character create for a free name plus a ledger debit over the balance → `commitTransactionAndWait()` returns `{ok false, 51, constraintId of balances.non_negative}`. `Client::constraintName` gives `("balances", "non_negative")`. Neither topic shows the records, through views and through a subscription from 0. The next transaction commits.
  - `CheckedWritesTxnBrokerRegression.AllYesEachPartitionVisibleInOneStepShardRegression`: 1,000 two-topic transactions. A concurrent reader of `account_stats` and `character_by_id` on `character`, and of `balances` on `ledger`, never sees part of one transaction on one partition.
  - `CheckedWritesTxnBrokerRegression.RacingTransactionsForTheLastSlotShardRegression`: 8 producers each create the 12th character of one user in a transaction → exactly one `ok`, and the other 7 return 51 `max_characters`.
  - `CheckedWritesTxnBrokerRegression.TxnTooLargeShardRegression`: `RUNE_TXN_JUDGED_RECORDS_MAX=16`, 17 creates → `{ok false, 55}`, and the ids are burned.
  - `CheckedWritesTxnBrokerRegression.AtomicBatchOnAConstrainedSourceShardRegression`: a `batch()` of 2 creates where one name is taken → one completion with 51, and no row.
  - `CheckedWritesTxnBrokerRegression.VoterLeaderCrashMidVoteThreeNodesRegression`: the `character` leader is crashed between the vote and its answer → `commitTransactionAndWait` returns the same result as an undisturbed run (yes or no, by setup), within the transaction timeout.
  - `CheckedWritesTxnBrokerRegression.CoordinatorCrashMidVoteThreeNodesRegression`: the coordinator node is crashed with votes out → the client rediscovers and retries, and gets the same answer.
  - `CheckedWritesTxnBrokerRegression.OppositeOrderLocksResolveByTimeoutShardRegression` (Review Focus 3): T1 and T2 each hold rows on both topics, with J order made opposite through the vote seam. With `transaction.timeout` 2 s, one aborts by timeout within 2 s plus one pass, and the other commits with correct rows. Nothing hangs.
  - `CheckedWritesTxnBrokerRegression.OldClientTransactionOnAConstrainedSourceShardRegression`: a v11 session's transaction (the session announces 11) whose vote is no → `InvalidRequest`, the abort succeeds, and the next transaction commits.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R CheckedWritesTxnBroker -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 3: Write the `TXN` leg.** House style: `set -euo pipefail`, bounded waits, the trap teardown, and every failure names its step.
  1. **TXN-SETUP:** the schema above through `rune ql`.
  2. **TXN-RACE:** 4 probes run 2,000 two-topic transactions over 100 users near 12 and over shared names. At 30 % the `character` leader is killed with `docker kill`, and the coordinator node at 60 %. **Check:** each user ends at most at 12; no name is live twice; every `ok` transaction is visible on both topics, and every no or aborted one on neither (checked from a subscription from 0 and from views). No `Timeout` past the transaction timeout. Bounded at 600 s.
  3. **TXN-RESTART:** `docker restart` all nodes → the views are unchanged.
  4. Print `TXN: OK` before the script's final `CHECKED-WRITES-E2E: OK`.
  - The usage line stays `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'`. The probe takes no host, IP or credential from source; everything comes from the environment the script sets.
- [ ] **Step 4: Build check locally (no docker).** `cmake --build build/sub-client-cpp -j14 --target checked_writes_probe`, then `bash -n broker/docker/checked-writes-e2e.sh`. Expected: no error.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesTxnBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt \
          broker/docker/checked-writes-e2e.sh client-cpp/examples/checked_writes_probe.cpp docs/testing.md
  git commit -m "test: transaction vote e2e on real brokers (shard, ThreeNodes) and the docker TXN leg (VM only)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 15: The alloc gate, the benches and this part's docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`, `broker/benchmark/ViewFoldBench.cpp` (beside 04's and 09's `BM_ViewFoldVerdict/*`), `broker/benchmark/WorkerProduceLatencyBench.cpp`
- Modify: `docs/api/ql.md`, `docs/api/client-cpp.md`, `docs/api/wire-protocol.md`, `docs/api/admin-cli.md`, `docs/operations.md`, `docs/configuration.md`, `client-cpp/CLAUDE.md`

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateTxnVotesAndAtomicBatchesAllocateNothingRegression` runs on the rig with both topics and all four constrained views:
  - warm up with 200 transactions (2–16 records, 10 % no, 10 % aborted after yes, outside writes on locked rows), 200 atomic batches, and votes through the op ring;
  - then 2,000 more of the same mix under `rune::testing::AllocWindow w;`;
  - `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Write the benches.**
  - `BM_ViewFoldVerdict/txn_judge_16` and `txn_judge_4096`: judging a deferred transaction at J (chained prepare plus the conflict scan) and finalizing its commit, on 09's `character` members. They report `judge_record_p50_ns`, `finalize_record_p50_ns` and `steady_alloc`. These are reported only; the spec sets no budget.
  - `BM_WorkerTxnCommit/voting` and `/non_voting`: an in-process vote round trip plus markers on one worker. They report p50 and p99. **Gate:** `non_voting` shows no ABAB regression against the base (spec G.2: "Transactions that touch none of these partitions run exactly as today").
- [ ] **Step 3: Run the gate.** `ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches** (baseline = this part's base commit on `main`, ABAB n=3, one session, same box, one prepare/restore):
  - `git worktree add "$TMPDIR/rune-cw11-base" <base>`, configured Release with `BUILD_BENCHMARKS=ON` like `build/rel`.
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ViewFoldReplace$|BM_ViewFoldAggregate$|BM_ViewFoldReplaceTxn|BM_ViewFoldVerdict/unique_lower_plus_count_check|BM_ProducePath_|BM_WorkerProduce_Majority|BM_WorkerProduceToPushRead_Disk|BM_WorkerTxnCommit/non_voting' --benchmark_repetitions=3`. Then run the tip alone for `BM_ViewFoldVerdict/txn_judge|BM_WorkerTxnCommit/voting`.
  - **Gates:** no regression on the ABAB set, judged by same-session medians against `BENCHMARKING.md`'s noise. The fence branch and the HW judge bound must not show. The 09 judging gate (≤ 1 µs per record) still holds.
  - Borderline numbers are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under `flock /root/vm.lock` on `$RUNE_VM_HOST`, then `vm-bench-restore.sh`). Record the medians in the commit body.
- [ ] **Step 5: Write the docs.**
  - **`ql.md`**, section "Transactions and atomic batches":
    - the three guarantee levels of §9b, with 3b a non-goal;
    - every constraint on every partition votes;
    - deletes are never rejected;
    - `TxnTooLarge` and the knob;
    - `TxnConflict` (a write to the same key by another writer between the transaction's write and its commit);
    - locked rows pause other writers' verdicts until the marker;
    - `p.batch(...)` on a verdict source is all or nothing;
    - every way a view is built gives the same rows (P11-0).
  - **`client-cpp.md`:**
    - `commitTransactionAndWait()` returns `TxnCommitResult` (code, `constraintId`, `failingOffset`), resolved with `Client::constraintName`;
    - after a no the transaction is already aborted: begin the next one;
    - `View` on a transactional producer is ignored on sends, and the commit's OK means the rows are final;
    - a rejected atomic batch has one completion.
  - **`wire-protocol.md`:**
    - the `EndTxnResponse` v12 trailer table;
    - code 59 in the error table;
    - batch attribute bit 7 `kAttrAtomic`;
    - control record 19's payload;
    - the cluster-plane templates 13 and 14 under the inter-broker section.
  - **`admin-cli.md`:** `rune stats topic <t>` prints the ids 11–20 names.
  - **`operations.md`**, under "Verdict sources":
    - votes, `TxnJudge`, the lock pause and `foldPausedOnTxnLockNs`;
    - `txnMarkerWithoutVote` as a coordinator-bug alarm;
    - distributed lock waits end at the transaction timeout;
    - a persisted constrained view checkpoints during run-ahead and backfills on restart.
  - **`configuration.md`:** `RUNE_TXN_JUDGED_RECORDS_MAX` (default 4096, range 1–1,048,576; before-images and locks are pre-allocated per worker).
  - **`client-cpp/CLAUDE.md`** §2.1: "a no vote completes the commit with the code in `errorCode`, the constraint id in `reserved0` and the failing offset in `value`; the producer is Ready".
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ViewFoldBench.cpp \
          broker/benchmark/WorkerProduceLatencyBench.cpp docs/api/ql.md docs/api/client-cpp.md docs/api/wire-protocol.md \
          docs/api/admin-cli.md docs/operations.md docs/configuration.md client-cpp/CLAUDE.md
  git commit -m "bench/docs: zero-alloc vote and atomic-batch gate, txn judge and commit benches, transaction docs" \
             -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 16: The §16.12 docs cross-check (the whole program)

**Files:**
- Modify (only where a check fails): `docs/api/ql.md`, `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `docs/api/admin-cli.md`, `broker/docs/CATALOG.md`, `broker/docs/SECURITY.md`, `broker/docs/SELF-HOSTING.md`, `docs/operations.md`, `docs/configuration.md`, `CLAUDE.md`, `client-cpp/CLAUDE.md`, `broker/CLAUDE.md`
- Modify, in the separate `rune-site` repo, on a local branch: `rune/docs.html` (the views section, `:390-455`; the error table, `:523`), `rune/index.html` (`:227-232`)

- [ ] **Step 1: Run the checklist.** Each line is a check with its command. A failing check is fixed in the named file, against the code as it ships (not the spec).

  | Doc | Must say (spec §) | Check |
  |---|---|---|
  | `ql.md` | the field list makes a topic typed; untyped topics are `raw` (§4); `partitions` defaults to 1 | `grep -n "raw" docs/api/ql.md`; no `--validate` anywhere: `grep -rn -- "--validate\|validate =" docs client-cpp/README.md` returns nothing |
  | `ql.md` | `autoincrement`, `key.index`, `create [or replace] [transient] [unique] view`, `key (a, b)`, `key lower(c) as n`, `constraint … check (…)`, `select … where … [limit n]`, `show topic`, `show view` (§10) | every statement in spec §1's target block appears and parses: `rune ql` dry-run of each code block (`build/sub-client-cpp` CLI, `--prepare-only`) |
  | `ql.md` | deletes are never rejected; ledgers as appended entries (§8.2); create pauses delivery; replace has no gap; drop then create has one (§8.7); persisted over transient consequences (§9a) | `grep -n -i "never rejected\|appended entries\|pauses delivery\|no gap" docs/api/ql.md` |
  | `wire-protocol.md` | the `acks` bits (§8.9); intent bits; `ProduceResponse` v12 with `assignedBase` and the trailer; codes 51–59 with kinds; `GetViewRowK` 81, `ScanViewRows` 82/83, `GetStats` 84/85; control records 16–20 and the atomic bit; `EndTxnResponse` v12 | `grep -c "| 5[1-9] " docs/api/wire-protocol.md` = 9; each template id appears once in the template table |
  | `client-cpp.md` | `Acks` flags; `WriteIntent`, `insert`/`update`/`upsert`/`remove`/`removeExisting` and `…AndWait` → `WriteResult`; `insertAndWait(...).key()`; `getViewRowAndWait`, `scanViewAndWait`, `ViewKeyBuilder`; `Client::constraintName`; `TxnCommitResult`; `BrokerTooOld`, `WriteNotAllowed`, `KeyTooLong`, `VerdictUnknown`, `StoredIdUnknown` | `grep -n` each name |
  | `admin-cli.md` | `rune ql "select …"` decoding; `show view` constraints and stats; `rune stats --json` with every C16 name | every `statName` value from `StatsMessages.h` appears: a loop over `grep -o '"[a-zA-Z]*"' protocol/include/rune/protocol/client/StatsMessages.h` |
  | `CATALOG.md` | catalog `formatVer` 2, `TopicKeyFeatures` (14-byte block), plan v2 fields in `normalizedPlanBlob`, the v2 golden | `grep -n "formatVer 2\|TopicKeyFeatures\|plan v2" broker/docs/CATALOG.md` |
  | `SECURITY.md` | the `IMPORT` grant (topic operation 7, implies DESCRIBE, nothing implies it, `grant import on topic …`) | `grep -n -i "import" broker/docs/SECURITY.md` |
  | `SELF-HOSTING.md` | persisted views over transient sources: tail lost on full restart, stale rows after failover, the two counters (§9a) | `grep -n "transientRecordsEvicted\|transientSourceResets" broker/docs/SELF-HOSTING.md` |
  | `operations.md`, `configuration.md` | verdict sources, W, markers, the cleaner rules, stats; `RUNE_VERDICT_INTERVAL_MS`, `RUNE_TXN_JUDGED_RECORDS_MAX` | `grep -n "RUNE_VERDICT_INTERVAL_MS\|RUNE_TXN_JUDGED_RECORDS_MAX" docs/configuration.md` |
  | root `CLAUDE.md`, `broker/CLAUDE.md` | typed topics always validate; database features need a typed (keyed) topic; the hot-path rule for verdict sources (one branch for topics without features); checked-writes test binaries and `ctest -R CheckedWrites` | `grep -n "CheckedWrites" CLAUDE.md broker/CLAUDE.md` |
  | `client-cpp/CLAUDE.md` | every §2.1 line Parts 03–11 added (acks flags, intents, ids, reads, constraint names, the vote) | read §2.1 against C4, C6, C15 |

- [ ] **Step 2: Prepare the site's views section** in the `rune-site` repo on a local branch `docs/checked-writes-views`, following that repo's `CLAUDE.md` copy rules (brief, infographic-like, no roadmap words). Correct it to the syntax that ships:
  - `create view` with `key (a, b)` / `lower(…) as …`, `unique`, `constraint … check (…)`;
  - point and prefix `select` only. Remove `where zone = 4` and `level > ?` (non-key filters and ranges are non-goals, §2), and the "Join view" label (joins are a non-goal);
  - "A view over a transient topic must be transient" becomes the Part 10 rule;
  - the Redis-style `get`/`hget`/`mget`/`scan` examples stay only if the CLI ships them; check `client-cpp/src/cli/QlEngine.cpp`, else remove;
  - the error table gains 51–59.
  - Commit locally with the trailer. **Do not push and do not deploy:** the owner gives the go-ahead (rune-site `CLAUDE.md`, "Publishing").
- [ ] **Step 3: Commit** the rune repo doc fixes.
  ```bash
  git add docs broker/docs/CATALOG.md broker/docs/SECURITY.md broker/docs/SELF-HOSTING.md CLAUDE.md client-cpp/CLAUDE.md broker/CLAUDE.md
  git commit -m "docs: section 16.12 cross-check of the checked-writes program" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```
  The `git add` lists only the files that changed; `docs` is narrowed to them.

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c); stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`, CatalogGolden on `build/sub-codec`, and the ql suites on `build/sub-ql`.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) with `-R Regression -LE latency`, including `rune_checked_writes_matrix_tests`, `rune_checked_writes_stress_tests`, `rune_txn_coordination_service_tests` and `rune_d9_cross_node_marker_tests`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure --repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 15 benches (recorded).
  - The VM docker e2e: `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'` → `TXN: OK` and `CHECKED-WRITES-E2E: OK`.

## Finish

- [ ] Checkpoint C is green.
- [ ] A whole-branch review by the most capable model. The reviewer leaves PR comments, and the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune`, in one batch with one summary at the end:
  1. "Owner check: `TxnConflict` (59) aborts a transaction on a write-write conflict (R11a, P11-3)" and "the partition votes under its latest set too (P11-1 (b))" — `question`, `sev:low`, `area:broker`, one issue each.
  2. "Deadlock detection for distributed transaction lock waits (today they resolve only by the transaction timeout; R11d)" — `enhancement`, `sev:medium`, `area:broker`.
  4. "GC of `TxnJudge` records whose bound passed and of type-20 plans" (joins 09's issue) — `enhancement`, `sev:low`, `area:broker`.
  5. Every recorded deviation (a re-pinned assertion) that is not urgent.
- [ ] Program close: hand the overview's acceptance ("10, 11: as their spec sections say") to the controller. Releasing (`kClientVersion = 12`, `kBrokerVersion = 7`, `kCheckedWrites = 4`) is the owner's call. Do not push, merge or deploy the site without the owner's go-ahead (overview § Sequencing; rune-site `CLAUDE.md`).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §9b levels 2 and 3a: Tasks 5–10 and 8; 3b is a non-goal, stated in the docs (Task 15).
  - G.1: P11-11.
  - G.2:
    - step 1 is unchanged;
    - step 2 (candidates at AddPartitions and at commit; the partition decides; non-voters answer yes without writing; no extra round trip without candidates): P11-1, P11-11, Tasks 7 and 10;
    - step 3 (i–v): Task 7 (HW wait, the `TxnJudge`) and Task 6 (judge at J, provisional apply, locks, answer);
    - step 4: Task 10;
    - step 5: P11-4, Task 6;
    - idempotent votes and coordinator failover: Tasks 7 and 10.
  - G.3: the order, Task 5; the marker without a `TxnJudge`, P11-7 and Task 6; failover, Task 7; "W never passes an undecided `TxnJudge`", P11-2.
  - G.4: P11-4 and P11-5, Task 6, `foldPausedOnTxnLockNs`.
  - G.5: view reads, P11-4; consumers, P11-10; non-verdict partitions, P11-9 and Task 8.
  - G.6: P11-6, Tasks 6, 11 and 12.
  - G.7: size, Task 7 (`TxnTooLarge`) and the knob; the client, Task 11; `View` producers, P11-12.
  - Cost: Task 15.
  - §11.1 code 55: Task 7. §11.2 rows 5–6 and "a key-0 create is not an explicit-key intent": P11-14 and P11-17, Task 12.
  - §13 Part G stats: P11-15, Task 7.
  - §15 Part G bullets and the invariant: Interaction matrix A–D.
  - §16.12: Task 16.
- **The prompt's interaction list.**
  - Coordinator failover: Tasks 10 and 14, and the matrix.
  - Leader failover: Tasks 7 and 14, and the matrix.
  - Timeout or abort: Tasks 6 and 10, and the matrix.
  - Plain writes on the same key: Task 6, Review Focus 1.
  - Replace or drop: Task 7 and the matrix.
  - Aborted folds: Task 7.
  - Autoincrement: Tasks 7 and 14.
  - Unique and check: the matrix entities and Task 14.
- **Rulings.** R05b: P11-6 and Task 1. R03c: P11-13 and P11-14. SD14 and SD4: Task 7 and P11-6. R08f: Task 7. R09a: no new type, P11-1. R04a: Task 7. R04d: the matrix timings. R04e: P11-6. P6-13: Task 7.
- **Step scan.** Each test step names its tests and their values (51, 55, 59, `0x00070000`, 4096, 4097, 8, 16, 602 / 126). Each code step names files, signatures and placement. Bodies appear only for the deferral loop (Task 5), the vote handling on the leader (Task 7) and the conflict scan (Task 6), which the signatures and tests do not fix.
- **Type consistency.**
  - `TxnJudgeState`, `DeferredTxn`, `TxnPhase`, `TxnVoteAnswer`, `TxnJudgeStats` and `ParkedVote` are defined in Task 4 and used in Tasks 5–7.
  - `TxnOverlay`, `TxnLockSet`, `OverlayTable` and `PrepareStatus::Locked` are defined in Task 3 and used in Task 6.
  - `TxnOverlayPool` and `bindTxn` (Task 4) are used in Task 7.
  - `TxnVoteMsg`, `TxnVoteResponseMsg`, `TxnJudgeRecord`, `buildTxnJudge`, `parseTxnJudge`, `kAttrAtomic`, `TxnConflict` and `isTxnVoteCode` (Task 1) are used in Tasks 6, 7, 9, 10 and 11.
  - `TxnVoteResult` and `sendTxnVote` (Task 9) are used in Task 10.
  - `TxnCommitResult` and `Verdict::VotedNo` (Task 11) are used in Task 14.
  - `TxnSpan` and `TxnSpanFence` (Task 8, used by every fold path and Task 7's push veto).
  - The rig's `sendVote`, `voteReply`, `driveUntilVoted` and `commitTxn` are defined in Task 7 and used in Tasks 12, 13 and 15.
  - Every contract name (`VerdictSource`, `PartitionWriteState`, `RejectedOffsetIndex`, `ConstraintSet`, `Prospect`, `FoldSkip`, `StatsSink`, `WriteFacts`, `HookRefusal`) is used as its owner defines it.
- **Review Focus.** Each of the five has its named test in its owning task (6, 7, 14, 8, 10).
- **Proportion.** The plan fixes the decisions (who votes, the order, the conflict rule, the overlay, the checkpoint rules, 3a, the coordinator states) together with their placements, signatures and assertions. The overlay index, the scans, the gate bookkeeping, the rig and the model are left to the implementer within those rules.
