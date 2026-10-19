# Rune Phase 2b — Part E2: Transient Topics Data Path (produce + subscribe) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A transient topic (`PersistenceMode::Transient`, `InMemoryLog`) is fully usable: produce in every ack mode (acks=all replicated to the ISR before the ack), push subscriptions from earliest / latest / an offset, consumer groups and read_committed, in BOTH worker modes (shard = default, legacy), with zero steady-state allocation and disk-class latency — plus a defined rule for a full ring, the DDL C1 rule (an uncommitted transient topic serves nothing) on every new path, and views over transient sources that survive a ring reset or recreation.

**Architecture:** The worker — the single owner of a partition's log — gets ONE extra read source and ONE extra append target: where a disk partition uses its `Log*`, a transient partition uses its replica's `InMemoryLog*` (zero-copy `readView` into the ring for reads; `handleProduceBatch` templated over the log type, with a new `InMemoryLog::appendNoSyncRestamp` that copies the batch once into the ring and patches base offset + leader epoch there). The gateway stops treating "no servable disk `Log`" as "no partition": a `servedPartition_` helper classifies a partition as `AwaitingCommit` (C1: retriable NotLeader, exactly as today), `Disk`, `Transient` (routed to the owning worker, never touching the ring cross-thread) or `Unknown`. The ring IS the topic's retention: it evicts the oldest batches only below the replica's high watermark (an "eviction floor"); a ring full of uncommitted data refuses the append (the new retriable `ErrorCode::TransientLogFull = 42`, nothing appended — controller ruling 2); a reader whose cursor was evicted resumes at the ring start (counted); a follower below the leader's ring start restarts its ring there (bumping the replica's `logGeneration`). A view over a transient source records the source ring's identity (replica incarnation + `logGeneration`) in its `Live` cursor state and re-folds when it changes.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja (`build-fast`, `build-bench-rel`), real Aeron UDP for the in-process BrokerNode integration tests, docker e2e (single node).

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` — section **E. Transient topics** ("replicated to a majority before ack", "produce + subscribe work", "restart → topic exists and is empty", "Ring eviction ahead of a view's cursor is counted (`transientRecordsEvicted`) and skipped, never a stall") and "Global constraints". The data path itself is the user decision recorded in Part E's plan (`docs/superpowers/plans/2026-10-11-rune-phase2b-e-transient-topics.md`, Spec question 1): *transient topics must fully work — produce (all ack modes), push subscribe from earliest/latest/offset, consumer groups, read-committed, in both shard and legacy worker modes — zero steady-state allocation, same latency class as disk.*

**Builds on / order:** E2 executes **after Part E, the no-hang wave and the DDL conformance suite** — all merged into `feature/materialized-views` at broker `6a3b9baa` (protocol `1890349`, client `c06348b`). What landed and what this plan now relies on (each verified in the code at those SHAs):

- **Part E:** `PersistenceMode {Disk = 0, Transient = 1}` (`libs/node/include/rune/node/PersistenceMode.h`); the `DefineTopic` persistence byte runs gateway → admin → control → `TopicRegistry` (transient = no data dir, `PartitionEntry.log == nullptr`) → `BrokerNode::createRuntimeUserReplicas` (an `InMemoryLog` of `config_.memoryLogRingBytes`, `DropOldest`, 1 000 000 batches); `RUNE_TRANSIENT_LOG_RING_BYTES` is canonical (`RUNE_MEMORY_LOG_RING_BYTES` a deprecated alias, power of two); views fold over transient sources with `CursorState{Fresh, RecoveredUnreconciled, Live}` reconciled inside `WorkerViewSet::driveView_` (`reconcileTransient_`), counting `ViewFoldCounters::transientRecordsEvicted`; a persisted view over a transient source is rejected `ViewSourceTransient = 37`.
- **No-hang:** every request with a decodable header is answered (`libs/gateway/include/rune/gateway/RequestRejection.h`), `ErrorCode::InvalidRequest = 39`, gateway egress with a preallocated per-session spill (`SpillPool`) and an explicit `Expiring` state for a session over quota, client version 5, and bare-NotLeader Subscribes retried with a budget then failed explicitly (client `SubscriberHub::chargeNotLeaderRetry`).
- **DDL conformance, incl. final-review C1:** an uncommitted runtime topic serves nothing — `TopicRegistry::servableLog()` returns nullptr and `awaitingCommit()` is true until the `kA2cTopicCommitted` admin→control ring message runs `TopicRegistry::commitTopic`, which is also where `createRuntimeUserReplicas` now builds the replicas and metadata assignment (`BrokerNode.cpp` admin-control drain). Produce/subscribe answer `NotLeader (2)`, metadata hides the topic, the DDL OK is held by `DdlAnswerGate` until the topic serves. Also: `TopicName.h` (codes 40/41), recovery placeholders `__orphan.<id>`, the two-phase topic-id marker, boot reconcile/quarantine.
- **The gap E2 closes (DDL final-fix concern):** a committed transient topic still has no servable disk `Log`, so today the gateway answers its produce/subscribe `UnknownPartition (8)` (legacy runtime) and the shard worker answers `Internal (6)`; push reads of it are left unanswered.

E's **Task 10 (e2e) was written BLOCKED on this part and was NOT executed** (checked: `examples/view_probe.cpp` and `docker/view-engine-e2e.sh` carry no `presence_t` leg) — it is executed here as **Task 14**. Every edit below is anchored by quoted code checked at `6a3b9baa`; line numbers are hints — use the anchor.

## Execution context

- **Repos / branch:** `feat/phase2b-e2-transient-data-path`, already cut from `feature/materialized-views` in
  - `~/Projects/se-worktrees/rune-broker` (base `6a3b9baa`)
  - `~/Projects/se-worktrees/rune-client-cpp` (base `c06348b`)
  - `~/Projects/se-worktrees/rune-protocol` (base `1890349`; Task 1 only)

  Verify with `git -C <repo> log --oneline -1` before Task 1.
- **Builds (all already configured — do not re-run the configure step unless a directory is missing):**
  - broker tests: `build-fast` (Ninja, Debug, `BUILD_TESTING=ON`, `BUILD_BENCHMARKS=OFF`; `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`, `RUNE_CODEC_ROOT` → `../rune-codec`). After a `CMakeLists.txt` edit: `cmake ~/Projects/se-worktrees/rune-broker/build-fast`.
  - broker benchmarks: `build-bench-rel` (Ninja, `BUILD_BENCHMARKS=ON`, `-O3 -DNDEBUG`). After a `CMakeLists.txt` edit: `cmake ~/Projects/se-worktrees/rune-broker/build-bench-rel`.
  - client: `build` (Unix Makefiles; `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`).
  - protocol: `build` (Unix Makefiles; target `rune_protocol_tests`).
  - If `build-fast` is missing: `cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DCMAKE_TOOLCHAIN_FILE=~/Projects/se-worktrees/rune-broker/build/Debug/generators/conan_toolchain.cmake -DRUNE_PROTOCOL_INCLUDE=~/Projects/se-worktrees/rune-protocol/include -DRUNE_CODEC_ROOT=~/Projects/se-worktrees/rune-codec`.
- **Test gate (CLAUDE.md rule 7, as amended in broker `03f2955f`):**
  - **Per task / fix round:** build, then run the test executables covering the code the task touches plus every new or changed test, at `-j14` — each task's "Run" step names the exact `ctest -R` filter (`ctest --test-dir <build> -R '<filter>' --output-on-failure -j14`).
  - **The FULL `*Regression*` suite runs only at controller checkpoints**, in every repo touched since the last checkpoint: `ctest --test-dir build-fast -R Regression --output-on-failure -j14` (broker), `ctest --test-dir build -R Regression --output-on-failure -j14` (client), `ctest --test-dir build -R Regression --output-on-failure` (protocol). A checkpoint failure is bisected over the commits since the last green checkpoint.
  - **Checkpoints:** A after Task 1 (protocol + client + broker build); B after Task 6 (worker data path); C after Task 8 (gateway + BrokerNode); D after Task 10 (views + fold bench); E after Task 14 — before the final whole-branch review.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- HARD RULES (CLAUDE.md, all eight): shared-nothing (the gateway NEVER reads an `InMemoryLog`; only the owning worker does); ring buffers are the only cross-thread passing (no new channel is needed: produce/read/marker ops already ride the op/return rings); state machines where each state holds only its variables (the gateway's partition kind and the view's transient cursor are explicit states); zero heap allocation on hot paths / in steady state; test + benchmark everything; a `*Regression*`-named test on every fix; affected-area tests per task and the full `*Regression*` suite at checkpoints; fix-on-sight.
- User decision (verbatim intent): transient topics **fully work** — produce in all ack modes incl. **acks=all replicated to a majority (the ISR) before the ack**; push subscribe from **earliest / latest / offset**; **consumer groups**; **read_committed**; in **both shard (default) and legacy** worker modes; **zero steady-state allocation**; **same latency class as disk topics**.
- Spec E: "replicated to a majority before ack, never written to disk"; "metadata survives, data does not … after a full-cluster restart a transient topic is empty".
- Spec E (user decision 2026-10-12, verbatim): "a view that reads from a transient topic must itself be transient. `create view` (persisted) with any transient source is rejected with a dedicated error ("<source> is transient; a view over it must be transient too"); `create transient view` is accepted and folds; after a full restart topic and view are both empty and refill together. Ring eviction ahead of a view's cursor is counted (`transientRecordsEvicted`) and skipped, never a stall."
- **C1 (DDL conformance, landed):** a runtime topic awaiting its catalog commit serves nothing — every E2 path keeps it: gateway produce/subscribe answer `NotLeader (2)` (subscribe: the bare `ErrorResponse{NotLeader}` the client retries with its budget), group subscriptions and markers are refused, and the worker has no replica to serve until `kA2cTopicCommitted` builds it (shard ingress answers NotLeader as "not owned"). A transient topic is gated exactly like a disk topic.
- **Full-ring rule (this plan's decision, Spec question 1, accepted — ruling 1):** the ring is the transient topic's retention (`RUNE_TRANSIENT_LOG_RING_BYTES` per partition, node-wide, default 8 MiB, power of two, ≤ 1 000 000 batches). It evicts oldest-first **only below the replica's high watermark**. An append that would have to evict a batch at/above the HW is **refused** — `ErrorCode::TransientLogFull = 42` (ruling 2), nothing appended. A batch larger than the whole ring is refused `MessageTooLarge` (non-retriable). A reader (subscription / PushRead / view) whose cursor is below the ring start resumes **at** the ring start (counted, rate-limited log) — never a stall. A follower whose fetch position is below the leader's ring start restarts its ring **empty at** the leader's ring start.
- **Wire (ruling 2):** ONE append-only addition — `ErrorCode::TransientLogFull = 42` (protocol), answered only to sessions with `clientVersion >= 6` (`kClientVersionTransientLogFull`); older sessions get `NotLeader`, their retriable code (`produceErrorForClient`, the one mapping every broker answer site already uses). Client v6 retries 42 after a backoff with the same identity and **without a metadata refresh**. `FetchResponse.logStartOffset` (an existing field, always 0 today) carries the leader's ring start for transient partitions only; disk partitions keep sending 0. No other message changes.
- Disk partitions: byte-for-byte the same behaviour and the same hot path (the transient branch is per call, one predicted pointer test; the disk path's calls are unchanged; the gateway's disk fast path is the same single `servableLog()` probe as today).
- A transient topic configured `tier=strict` acks like `majority` (there is no fsync to wait for) — documented (ruling 3).
- Out of E2 (recorded, not done here): disk followers stalling below the leader's retention start (ruling 4); the gateway's pre-existing cross-thread reads of disk log offsets (ruling 5); per-topic ring size (ruling 1); **the Part D console must map code 37 (`ViewSourceTransient`) and the new code 42 to readable names** — Part D's, noted for its executor.

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **A consumer slower than the ring must keep consuming** (resume at the ring start), never stall on an evicted offset → `TransientDataPathRegression.PushReadBelowRingStartResumesAtRingStartRegression` (Task 5) and the matrix's `EvictedCursor` column (Task 11).
2. **A full ring under a slow follower must not lose not-yet-replicated data** — it refuses (42, retriable) instead of evicting above the HW, and accepts again once the follower catches up → `TransientDataPathRegression.FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression` (Task 4), `TransientReplicaRegression.LeaderRingNeverEvictsAboveTheHighWatermarkRegression` (Task 3), and the seeded stress invariant "logStart ≤ HW after every step" (Task 12).
3. **A restarted (empty) node must catch up a transient partition whose leader already evicted offset 0** → `TransientReplicaRegression.FollowerBehindTheLeaderRingStartResetsAndCatchesUpRegression` (Task 3).
4. **A deferred/held read must never read a ring after its partition moved to another worker** (a cross-thread read of a single-owner ring) → `TransientDataPathRegression.DeferredPushReadAfterOwnershipMovedNeverReadsTheRingRegression` (Task 5) and the matrix's `LeaderMovesMidOp` column (Task 11).
5. **An uncommitted transient topic must serve nothing, and the committed one must serve through the legacy gateway** (today: UnknownPartition) → `TransientGatewayRegression.UncommittedTransientTopicServesNothingUntilCommitRegression`, `TransientGatewayRegression.CommittedRuntimeTransientTopicServesThroughTheWorkerRegression` (Task 7), `TransientTopicDataPathRegression.LegacyRuntimeTransientDefineThenProduceAndSubscribeRegression` (Task 8).
6. **A view over a transient source must not silently keep stale rows when its source ring is reset or recreated under a live cursor** → `WorkerViewSetRegression.TransientSourceRecreatedRefoldsFromRingStartRegression`, `WorkerRegression.ViewOverRecreatedTransientSourceRefoldsRegression` (Task 9).
7. **`From::latest()` on a transient topic must not replay old records; an explicit offset beyond the HW clamps to it** → `TransientGatewayRegression.SubscribeLatestAndExplicitOffsetClampToTheHighWatermarkRegression` (Task 7) and `TransientTopicDataPathRegression.ShardProduceAndSubscribeEarliestLatestOffsetRegression` (Task 8).

Also pinned: read_committed withholds an open transaction until its commit marker (`TransientDataPathRegression.ReadCommittedHoldsTheOpenTransactionUntilItsCommitRegression`, Task 5); steady-state produce + evict + read never allocates (`TransientDataPathRegression.SteadyStateProduceEvictAndPushReadNeverAllocateRegression`, Task 6); a replicated apply never allocates (`ZeroAllocReplicaRegression.TransientFollowerApplyAllocatesNothing`, Task 3 — fixes a hot-path `std::vector` found on sight); an old client never sees 42 (`TransientLogFullRegression.OldClientsAreAnsweredNotLeader`, Task 1).

---

## Interaction matrix

**Single source of truth:** `libs/node/tests/support/TransientMatrixExpectations.h` (Task 11) — `expect(Op, Entity, Timing) → Outcome`. The unit matrix (Task 11), the seeded stress (Task 12), the integration slice (Task 13) and the e2e leg (Task 14) read it; no test hard-codes an outcome.

### Operations (9)

| Op | What is sent |
|---|---|
| `ProduceAcks0` | client `Acks::None` — wire `acks = 0`, the answer is not awaited by the client (the broker still answers; the cell asserts the broker side) |
| `ProduceAcks1` | client `Acks::Leader` — wire `acks = 0` (Produce `acks`: 0 = Leader/None, 1 = All) |
| `ProduceAcksAll` | wire `acks = 1` — ack only after the HW passes the batch |
| `SubEarliest` | Subscribe `fromOffset = -2` |
| `SubLatest` | Subscribe `fromOffset = -1` |
| `SubOffset` | Subscribe `fromOffset = 3` (mid-batch: whole-batch delivery from the batch containing it) |
| `GroupConsume` | a group subscription from a committed offset (`ClientGateway::addGroupSubscription(…, fromOffset = 2, …)`) |
| `ReadCommitted` | Subscribe `isolation = 1` over a transactional batch + a plain batch |
| `TxnMarker` | a COMMIT control marker (`ClientGateway::appendControlRecord`) for an open transaction |

### Entities (2 × 2 × 2 × 2 = 16)

`Persist {Transient, Disk}` × `Mode {Shard, Legacy}` × `Role {Leader, Follower}` × `Commit {Committed, Uncommitted}`. Shard = produce on the worker's ingress (`handleIngressFrameForTest`); Legacy = produce/markers through the gateway's op ring. Reads at unit level go through the gateway's PushRead path in both modes — the worker read core (`servePushRead_` / `readPushChunk_`) both modes share; the shard owned-subscription push loop (`pushOwnedSubscriptions_`) needs a real egress publication and runs in the Task 13 slice.

### Timings (7)

| Timing | Precondition the harness builds |
|---|---|
| `Steady` | two batches of two records (offsets 0..3) produced and committed |
| `RingFullUncommitted` | ring 16 KiB, ISR {1,2}, follower 2 never fetches: produce until the ring refuses (HW stays 0) |
| `EvictedCursor` | ring 16 KiB, ISR {1}: 40 × ~1 KiB batches — the ring start is > 0; the op's cursor is below it |
| `LeaderMovesMidOp` | the op is enqueued, then the partition's ownership leaves the worker before it executes, then comes back |
| `Restart` | Steady, then the rig is destroyed and rebuilt on the same data dir (disk recovers; the transient ring is new and empty), then one post-restart batch |
| `FollowerBelowRingStart` | `EvictedCursor`'s ring, replicas {1,2}, ISR {1}: follower 2 fetches offset 0 (below the ring start) before the op |
| `CommitLandsMidOp` | the topic is created at runtime and NOT committed; the op is refused; `commitTopic` + replica build run (what `kA2cTopicCommitted` does); the op is retried |

### Expected outcome per cell (`expect()`; rules applied top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | `Uncommitted` × `Steady` | `RetriableNotLeader` (produce: ProduceResponse 2, nothing appended; reads: bare `ErrorResponse{NotLeader}` / group `-1`, nothing registered); `TxnMarker` → `NotApplicable` (no transaction can include a partition whose produce was refused) |
| R2 | `Uncommitted` × `CommitLandsMidOp` | `Leader`: `OkAfterCommit` (first answer `RetriableNotLeader`, the retry after the commit is the `Steady` `Ok`); `Follower` and `TxnMarker` → `NotApplicable` (after the commit a follower answers as R7 — the `Committed` rows) |
| R3 | `Uncommitted` × any other timing; `Committed` × `CommitLandsMidOp` | `NotApplicable` (those timings need a serving topic / an uncommitted one) |
| R4 | `Disk` × {`RingFullUncommitted`, `EvictedCursor`, `FollowerBelowRingStart`} | `NotApplicable` — a disk log has no ring; disk retention below a cursor is the existing CP-5.2 clamp (its own tests); a disk follower below the leader's log start is ruling 4's follow-up |
| R5 | `Follower` × {`RingFullUncommitted`, `EvictedCursor`, `FollowerBelowRingStart`} | `NotApplicable` — leader-side states; the lagging follower itself is Task 3 + Task 12 |
| R6 | `Follower` × `LeaderMovesMidOp` | as R9 |
| R7 | `Follower` × {`Steady`, `Restart`} | produce → `RetriableNotLeader`; `TxnMarker` → `MarkerRetried` (refused retriably — NotLeader — while a follower; the coordinator keeps retrying; landing is asserted by the leader cells); reads → `ParityWithDisk` (both topics' followers are seeded with identical replicated batches; the transient topic's observed answer and delivery equal the disk topic's) |
| R8 | `Leader` × `Steady` | `Ok` |
| R9 | `Leader` × `LeaderMovesMidOp` | produce → `RetriableNotLeader` (then the retry is `Ok`); `TxnMarker` → `MarkerRetried` (refused stale, re-routed, lands once); reads → `UnansweredThenReissued` (no Records while disowned, the worker reads no ring it does not own, delivery resumes after ownership returns) |
| R10 | `Leader` × `RingFullUncommitted` (transient) | produce (all acks) → `RetriableLogFull` (42, nothing appended; after the follower catches up the retry is `Ok`); `TxnMarker` → `MarkerRetried`; reads → `Ok` (nothing past the HW = 0 is delivered; after the catch-up delivery starts at offset 0 — nothing was evicted) |
| R11 | `Leader` × {`EvictedCursor`, `FollowerBelowRingStart`} (transient) | produce, `TxnMarker`, `SubLatest` → `Ok`; `SubEarliest`, `SubOffset`, `GroupConsume`, `ReadCommitted` → `OkFromRingStart` (first delivered base = ring start, skipped records counted by the worker); `FollowerBelowRingStart` additionally asserts the leader answered the below-start fetch with no records and `logStartOffset` = its ring start |
| R12 | `Leader` × `Restart` | transient → `OkEmptyAfterRestart` (produce lands at offset 0; reads deliver only post-restart records; a group offset above the empty ring clamps to its HW); disk → `Ok` (the data survived) |

### Cell counts (pinned by `TransientMatrixCountRegression.CellCountsArePinned`, Task 11)

9 ops × 16 entities × 7 timings = **1008 cells**: **366 applicable**, 642 `NotApplicable` (reported as skipped with the rule that excludes them). Applicable by outcome: `RetriableNotLeader` 112, `Ok` 84, `UnansweredThenReissued` 40, `ParityWithDisk` 40, `OkAfterCommit` 32, `OkEmptyAfterRestart` 18, `MarkerRetried` 18, `OkFromRingStart` 16, `RetriableLogFull` 6.

### Every cell asserts

1. The exact first answer (ProduceResponse / SubscribeResponse / ErrorResponse code, group `subId`, marker landed/refused + code), on a session that announced the current client version (6); `RetriableLogFull` cells also assert a v5 session gets `NotLeader` for the same refusal.
2. **Nothing half-done on a refusal:** a refused produce leaves the log end unchanged; a refused subscribe registers nothing; a refused marker appends nothing (and is never reported landed).
3. Delivery: exactly the expected batch base offsets for the subscription, in order, never a control batch, never past the HW (read_uncommitted) / LSO (read_committed), never an aborted or open transaction to read_committed.
4. **Ring invariants (transient):** `logStartOffset ≤ highWatermark` (nothing uncommitted evicted), the worker's skip counter grows by exactly `ringStart − cursor` for every `OkFromRingStart` read.
5. **No cross-thread ring access:** a disowned worker's push counters (`pushReadChunksForTest`) do not move while disowned.

### Coverage by layer

| Layer | Task | Cells |
|---|---|---|
| Unit, parameterized (`TEST_P` over all 1008; N/A skipped with its rule) | 11 | all 366 applicable |
| Seeded model-checked stress (shared ring: produce × eviction floor × readers × follower × markers) | 12 | the `Leader`/`Transient` rows of R8, R10, R11 under random interleavings, 64 seeds × 3000 steps |
| Integration slice — real BrokerNode over Aeron | 13 | both modes × {3 produce, 3 subscribe ops} × {`Steady`, `EvictedCursor`, `Restart`, `CommitLandsMidOp`} = 48 cells, plus `LeaderMovesMidOp` on 3 nodes (acks=all, then subscribe on the new leader) — shard owned-subscription pushes run here |
| e2e (docker, real client, shard default) | 14 | `Transient`/`Shard`/`Leader`/`Committed` × {all 3 produce acks, 3 subscribe starts, `ReadCommitted` (commit + abort)} × {`Steady`, `Restart`}, defined at runtime (`CommitLandsMidOp` through the DDL answer gate) |

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-protocol/include/rune/protocol/client/Protocol.h`, `tests/protocol/client/DataMessagesTest.cpp` | `ErrorCode::TransientLogFull = 42`, client version 6, `produceErrorForClient` gate + pins | 1 |
| `rune-client-cpp/src/producer/ProducerImpl.cpp`, `include/rune/client/producer/TransactionalProducerImpl.h`, `src/producer/TransactionalProducerImpl.cpp`, `tests/producer/ProducerRouteParkTest.cpp`, `tests/producer/TransactionalProducerTest.cpp`, `include/rune/client/producer/Producer.h` (doc) | 42 retried after a backoff, same identity, no metadata refresh | 1 |
| `libs/storage/include/rune/storage/InMemoryLog.h` | `appendNoSyncRestamp`, leader-epoch stamp, eviction floor, refusal codes, `resetTo`, counters | 2 |
| `libs/storage/include/rune/storage/ProducePath.h`, `libs/storage/src/ProducePath.cpp` | `handleProduceBatch` templated over the log type; `ProduceOutcome.logFull`; 42 | 2 |
| `libs/storage/tests/storage/InMemoryLogTransientTest.cpp` (new), `libs/storage/CMakeLists.txt` | storage tests | 2 |
| `libs/replication/include/rune/replication/PartitionReplica.h`, `libs/replication/src/PartitionReplica.cpp` | epoch stamp on the ring, HW eviction floor, refusals, follower ring reset (+ `logGeneration` bump), fetch answer below the ring start, apply zero-alloc fix | 3 |
| `libs/replication/tests/replication/TransientReplicaTest.cpp` (new), `ZeroAllocReplicaTest.cpp`, `libs/replication/CMakeLists.txt` | replication tests | 3 |
| `libs/node/include/rune/node/Worker.h` | produce core over the ring, marker back-pressure mapping, push reads / owned-subscription pushes from the ring, evicted-cursor skip, counters; view source identity (9) | 4, 5, 9 |
| `libs/node/tests/integration/TransientRig.h` (new) | one-worker transient rig (shared) | 4 |
| `libs/node/tests/integration/TransientDataPathTest.cpp` (new), `TransientDataPathAllocTest.cpp` (new), `libs/node/CMakeLists.txt` | worker regressions + allocation regression | 4, 5, 6 |
| `benchmark/WorkerProduceLatencyBench.cpp` | produce→PushRead latency, transient vs disk | 6 |
| `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` | `ServedPartition` kinds + `servedPartition_`; produce / subscribe / push / group / marker sites | 7 |
| `libs/gateway/tests/gateway/TransientGatewayTest.cpp` (new), `libs/gateway/CMakeLists.txt` | gateway regressions incl. C1 (legacy worker-pool mode) | 7 |
| `libs/node/tests/integration/TransientTopicDataPathTest.cpp` (new), `libs/node/CMakeLists.txt` | BrokerNode over real Aeron: shard, legacy, 3-node acks=all, restart, runtime define (C1) | 8, 13 |
| `docs/SELF-HOSTING.md`, `docs/release-notes/phase2b-e-transient-topics.md` | data-path + full-ring rule + code 42 | 8 |
| `libs/node/include/rune/node/WorkerViewSet.h`, `libs/node/src/WorkerViewSet.cpp`, `libs/node/tests/node/WorkerViewSetTest.cpp`, `libs/node/tests/node/WorkerTest.cpp` | transient cursor as a variant, `Live` carries the source identity; `transientSourceResets` | 9 |
| `benchmark/ViewFoldSourceBench.cpp` (new), `CMakeLists.txt` (benchmark list) | fold over a transient vs a disk source on the real worker pass | 10 |
| `libs/node/tests/support/TransientMatrixExpectations.h` (new) | the matrix table (`expect()`) | 11 |
| `libs/gateway/tests/gateway/TransientInteractionMatrixTest.cpp` (new), `libs/gateway/CMakeLists.txt` | the parameterized unit matrix (1008 cells) + count pin | 11 |
| `libs/node/tests/integration/TransientDataPathStressTest.cpp` (new), `libs/node/CMakeLists.txt` | seeded model-checked stress (own binary) | 12 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | e2e transient leg (Part E Task 10, executed here) + matrix e2e cells | 14 |

---
### Task 1: Protocol + client — `ErrorCode::TransientLogFull = 42`, client version 6 (controller ruling 2)

Ruling 2 (2026-10-11): "do NOT reuse `NotLeader` … Append a dedicated retriable error code `TransientLogFull` to the protocol `ErrorCode` enum (append-only), map it in the client as `BackPressured`-class retriable without a metadata refresh. Executors add this to the protocol and client in the task that introduces the refusal." The refusal is introduced by Tasks 2-4; the code lands first so those tasks compile against it. `39 = InvalidRequest`, `40 = InvalidTopicName` and `41 = ViewSourceIsView` are taken, so the next value is **42**.

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h` (`enum class ErrorCode` after `ViewSourceIsView = 41`; the "Client versions" comment block; `kClientVersion*` constants; `produceErrorForClient`)
- Test: `rune-protocol/tests/protocol/client/DataMessagesTest.cpp` (append)
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp` (`onProduceResponse`: the `MinIsrViolation` branch; the rule-R comment)
- Modify: `rune-client-cpp/include/rune/client/producer/TransactionalProducerImpl.h` (`InFlightBatch`), `rune-client-cpp/src/producer/TransactionalProducerImpl.cpp` (constants block, `onProduceResponse`, the `due` loop in `doWork`)
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h` (the `ProducerConfig::produceTimeoutMs` comment, ~`:96` — one line)
- Test: `rune-client-cpp/tests/producer/ProducerRouteParkTest.cpp`, `rune-client-cpp/tests/producer/TransactionalProducerTest.cpp` (append)

**Interfaces:**
- Produces (namespace `rune::protocol::client`):
  ```cpp
  enum class ErrorCode : uint16_t { /* … */ ViewSourceIsView = 41, TransientLogFull = 42 };
  inline constexpr int32_t kClientVersionTransientLogFull = 6;
  inline constexpr int32_t kClientVersion                 = 6;
  [[nodiscard]] constexpr bool clientUnderstandsTransientLogFull(int32_t clientVersion) noexcept;
  // produceErrorForClient: UnknownProducerId -> OutOfOrderSequence below v2 (unchanged);
  //                        TransientLogFull  -> NotLeader          below v6 (new)
  ```
- Consumed by Tasks 2 (`ProducePath` answers 42), 4 (marker refusal), 7, 11, 13, 14. Every broker ProduceResponse is encoded through `ProduceResponse::encodeFor(buf, session.clientVersion)` (gateway `ClientGateway.cpp` and the shard `SessionAckSink.h:987`), which already applies `produceErrorForClient` — so no broker answer site changes.

- [ ] **Step 1: Write the failing protocol tests** — append to `rune-protocol/tests/protocol/client/DataMessagesTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2 (controller ruling 2): a TRANSIENT partition's ring full of
// not-yet-replicated records answers the dedicated retriable TransientLogFull
// (42) — never NotLeader (which makes a client refresh metadata on every
// refusal).  Append-only wire constant; sessions below client version 6 are
// answered NotLeader, their retriable code.
// ---------------------------------------------------------------------------
TEST(TransientLogFullRegression, CodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(rune::protocol::client::ErrorCode::TransientLogFull), 42u);
    EXPECT_EQ(rune::protocol::client::kClientVersionTransientLogFull, 6);
    EXPECT_GE(rune::protocol::client::kClientVersion, rune::protocol::client::kClientVersionTransientLogFull);
}

TEST(TransientLogFullRegression, OldClientsAreAnsweredNotLeader) {
    namespace rpc = rune::protocol::client;
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 5), rpc::ErrorCode::NotLeader);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 1), rpc::ErrorCode::NotLeader);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 6), rpc::ErrorCode::TransientLogFull);
    // The pre-existing gate is unchanged.
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 1), rpc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 6), rpc::ErrorCode::UnknownProducerId);

    rpc::ProduceResponse r;
    r.sessionId = 1; r.correlationId = 2; r.partition = 0; r.baseOffset = -1;
    r.errorCode = static_cast<int16_t>(rpc::ErrorCode::TransientLogFull);
    std::array<uint8_t, 64> buf{};
    const std::size_t n5 = r.encodeFor(buf.data(), 5);
    auto d5 = rpc::ProduceResponse::decode(buf.data(), n5);
    ASSERT_TRUE(d5.has_value());
    EXPECT_EQ(d5->errorCode, static_cast<int16_t>(rpc::ErrorCode::NotLeader)) << "a v5 client must never see 42";
    const std::size_t n6 = r.encodeFor(buf.data(), 6);
    auto d6 = rpc::ProduceResponse::decode(buf.data(), n6);
    ASSERT_TRUE(d6.has_value());
    EXPECT_EQ(d6->errorCode, 42);
}
```

(Add `#include <array>` to the file's includes if it is not already there.)

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests`
Expected: compile error `no member named 'TransientLogFull' in 'rune::protocol::client::ErrorCode'`.

- [ ] **Step 3: Implement — `Protocol.h`**

In `enum class ErrorCode`, replace

```cpp
    ViewSourceIsView       = 41,  // CreateView: a source is another view's output (a view sink carries
                                  // no change events yet, so the view would stay empty; until Phase 3)
};
```
with
```cpp
    ViewSourceIsView       = 41,  // CreateView: a source is another view's output (a view sink carries
                                  // no change events yet, so the view would stay empty; until Phase 3)
    TransientLogFull       = 42,  // Produce / txn marker (Phase 2b E2): a TRANSIENT partition's in-memory
                                  // ring is full of records not yet replicated (at/above the high
                                  // watermark) — NOTHING was appended.  RETRIABLE on the SAME leader
                                  // after a backoff, without a metadata refresh.  Answered only to
                                  // clients >= kClientVersionTransientLogFull (6); older clients get
                                  // NotLeader (their retriable code) — see produceErrorForClient.
};
```

In the "Client versions" comment, after the `//   5 — no-hang wave: …` paragraph (ending `re-join on, forever).`) add:

```cpp
//   6 — Phase 2b E2: understands ErrorCode::TransientLogFull (42) on
//       ProduceResponse — retried after a backoff with the same identity and
//       no metadata refresh.  A broker answers a session below 6 NotLeader
//       instead (produceErrorForClient).
```

Replace

```cpp
inline constexpr int32_t kClientVersionInvalidRequest             = 5;
inline constexpr int32_t kClientVersion                           = 5;   // what this protocol's client announces
```
with
```cpp
inline constexpr int32_t kClientVersionInvalidRequest             = 5;
inline constexpr int32_t kClientVersionTransientLogFull           = 6;
inline constexpr int32_t kClientVersion                           = 6;   // what this protocol's client announces
```

After `clientUnderstandsUnknownProducerId` add:

```cpp
/// True when a session that connected with `clientVersion` may be answered
/// TransientLogFull (42) (else the broker answers NotLeader).
[[nodiscard]] constexpr bool clientUnderstandsTransientLogFull(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionTransientLogFull;
}
```

Replace the body of `produceErrorForClient`

```cpp
    return (ec == ErrorCode::UnknownProducerId && !clientUnderstandsUnknownProducerId(clientVersion))
               ? ErrorCode::OutOfOrderSequence
               : ec;
```
with
```cpp
    if (ec == ErrorCode::UnknownProducerId && !clientUnderstandsUnknownProducerId(clientVersion)) [[unlikely]]
        return ErrorCode::OutOfOrderSequence;
    if (ec == ErrorCode::TransientLogFull && !clientUnderstandsTransientLogFull(clientVersion)) [[unlikely]]
        return ErrorCode::NotLeader;   // Phase 2b E2: a pre-v6 client's retriable code
    return ec;
```
and extend its doc comment's first sentence: `… for clients below kClientVersionUnknownProducerId, and TransientLogFull becomes NotLeader for clients below kClientVersionTransientLogFull; every other code passes through.`

- [ ] **Step 4: Run the protocol tests**

Run: `cmake --build ~/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests && ctest --test-dir ~/Projects/se-worktrees/rune-protocol/build -R 'TransientLogFull|UnknownProducerId|DataMessages|Conformance' --output-on-failure -j14`
Expected: all PASS (the conformance vectors carry no client version field; if one pins `kClientVersion == 5`, update that pin in the same commit — it is the version this protocol announces).

- [ ] **Step 5: Write the failing client tests** — append to `rune-client-cpp/tests/producer/ProducerRouteParkTest.cpp`:

```cpp
// Phase 2b E2 (ruling 2): TransientLogFull (42) is a definite non-append on the
// RIGHT leader (its transient ring is full of not-yet-replicated records): the
// batch is re-sent with the SAME identity after the retriable backoff — never
// failed, never a route park (no metadata refresh, unlike NotLeader).
TEST(ProducerRoutePark, TransientLogFullIsRetriedAfterBackoffWithoutParkingRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    BatchTally t;
    impl.setOnBatchAck(&BatchTally::on, &t);
    const std::vector<uint8_t> v(8, 1);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 1u);
    impl.onProduceResponse(w.frames[0].corrId, -1,
                           static_cast<int16_t>(rune::protocol::client::ErrorCode::TransientLogFull));
    EXPECT_EQ(t.failed, 0) << "TransientLogFull (nothing appended) failed the batch";
    EXPECT_FALSE(impl.routeParked(1, 0)) << "42 must not park the route (no metadata refresh)";
    EXPECT_FALSE(impl.takeMetadataWanted()) << "42 must not request metadata";
    impl.retransmitStalled(kNow + 1'000'000LL, sink);   // inside the backoff
    EXPECT_EQ(w.frames.size(), 1u) << "re-sent without a backoff";
    impl.retransmitStalled(kNow + 200'000'000LL, sink); // past it
    ASSERT_EQ(w.frames.size(), 2u);
    EXPECT_EQ(batchcorr::base(w.frames[1].corrId), batchcorr::base(w.frames[0].corrId));
    EXPECT_EQ(w.frames[1].epoch, w.frames[0].epoch);
    EXPECT_EQ(w.frames[1].baseSequence, w.frames[0].baseSequence);
    impl.onProduceResponse(w.frames[1].corrId, 100, 0);
    EXPECT_EQ(t.okRecords, 1);
    EXPECT_EQ(t.failed, 0);
}
```

Append to `rune-client-cpp/tests/producer/TransactionalProducerTest.cpp`:

```cpp
// Phase 2b E2 (ruling 2): a transactional batch answered TransientLogFull (42)
// is re-offered (same identity, a new attempt) after a duty-cycle backoff, never
// spends the retry budget (a full ring can last longer than kMaxRetries cycles)
// and never aborts the transaction.  RED (pre-fix): the generic path re-sent it
// on the next cycle and poisoned the transaction after kMaxRetries answers.
TEST(TransactionalProducerTest, TransientLogFullBacksOffWithoutSpendingRetriesRegression) {
    TxnRig r;
    r.acquire(77, 0);
    auto cur = sendData(r, 1);
    ASSERT_EQ(cur.size(), 1u);
    const int32_t seq0 = decodeBaseSeqFromProduce(cur[0]);
    for (int round = 0; round < 3 * 5 /* > kMaxRetries */; ++round) {
        SCOPED_TRACE(round);
        answer(r, corrIdOf(cur[0]), ecOf(rpc::ErrorCode::TransientLogFull));
        r.sink.clear();
        r.doWork();
        EXPECT_TRUE(produceFrames(r.sink).empty()) << "re-sent on the very next cycle (no backoff)";
        int waited = 0;
        do { r.doWork(); ASSERT_LE(++waited, 2048); } while (produceFrames(r.sink).empty());
        cur = produceFrames(r.sink);
        ASSERT_EQ(cur.size(), 1u);
        EXPECT_EQ(decodeBaseSeqFromProduce(cur[0]), seq0) << "same identity";
        ASSERT_EQ(r.tp.state(), TxnState::InTransaction) << "42 poisoned the transaction";
    }
    answer(r, corrIdOf(cur[0]), 0, 7);
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction);
    EXPECT_EQ(r.impl->testInFlightBatches(), 0u);
}
```

- [ ] **Step 6: Run to verify they fail**

Run: `cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'TransientLogFull' --output-on-failure -j14`
Expected: `ProducerRoutePark.TransientLogFullIsRetriedAfterBackoffWithoutParkingRegression` FAILS (`t.failed == 1`: rule D fails it); `TransactionalProducerTest.TransientLogFullBacksOffWithoutSpendingRetriesRegression` FAILS ("re-sent on the very next cycle", then the transaction leaves `InTransaction` after 5 answers).

- [ ] **Step 7: Implement — client**

`src/producer/ProducerImpl.cpp`, `onProduceResponse`: replace

```cpp
    if (ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation)) {
```
with
```cpp
    // Phase 2b E2 (ruling 2): TransientLogFull (42) — the leader's transient
    // ring is full of not-yet-replicated records; definitely not appended, the
    // route is right (no park, no metadata) — exactly MinIsrViolation's retry.
    if (ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation) ||
        ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::TransientLogFull)) {
```
and in the rule comment block replace `//     (re-sent after kRetriableBackoffNs — Kafka NOT_ENOUGH_REPLICAS, N-e).` with
`//     (re-sent after kRetriableBackoffNs — Kafka NOT_ENOUGH_REPLICAS, N-e) and TransientLogFull (42, Phase 2b E2: same retry).`

`include/rune/client/producer/TransactionalProducerImpl.h`, in `struct InFlightBatch`, after `uint64_t             sendTick = 0;` add:

```cpp
        // Phase 2b E2 (ruling 2): answered TransientLogFull — not re-offered for
        // this many more duty cycles (exponential, capped at
        // kRetryBackoffCapCycles); logFullStreak = consecutive 42 answers.  Never
        // spends retryCount; bounded by deadlineNs (sweep_).
        uint32_t             backoffCycles = 0;
        uint32_t             logFullStreak = 0;
```

`src/producer/TransactionalProducerImpl.cpp`: after `constexpr int16_t kInvalidRequest         = static_cast<int16_t>(rpc::ErrorCode::InvalidRequest);` add

```cpp
constexpr int16_t kTransientLogFull       = static_cast<int16_t>(rpc::ErrorCode::TransientLogFull);
```

In `onProduceResponse`, immediately before `    if ((ec == kOutOfOrderSequence || unknownPid) && ps != nullptr) {` add:

```cpp
    if (ec == kTransientLogFull) {
        // Phase 2b E2 (ruling 2): the partition's transient ring is full of
        // not-yet-replicated records — definitely NOT appended, and the leader is
        // right (no re-discovery, no metadata).  Re-offered after a duty-cycle
        // backoff with the same identity; the retry budget is not spent (a slow
        // follower can keep the ring full longer than kMaxRetries cycles); the
        // batch deadline (sweep_) still bounds it.
        const uint32_t shift = std::min<uint32_t>(ib.logFullStreak, 30u);
        ib.backoffCycles = std::min<uint32_t>(1u << shift, kRetryBackoffCapCycles);
        ++ib.logFullStreak;
        ib.sentSessionId = 0;
        return;
    }
```

and in the OK branch (`if (ec == kOk) {`) the batch is erased, so no reset is needed there. In `doWork`, replace

```cpp
            if (b.sentSessionId == 0 && !b.parked && !b.awaitRoute) due.push_back(corr);
```
with
```cpp
            if (b.sentSessionId == 0 && !b.parked && !b.awaitRoute) {
                if (b.backoffCycles > 0) { --b.backoffCycles; continue; }   // E2: TransientLogFull backoff
                due.push_back(corr);
            }
```

`include/rune/client/producer/Producer.h`: replace `    int64_t produceTimeoutMs = 5'000;    // timeout for a pending ProduceResponse ack` with

```cpp
    // Timeout for a pending ProduceResponse ack.  It also bounds the retriable
    // answers: MinIsrViolation and (Phase 2b E2) TransientLogFull (42 — a
    // transient topic's ring is full of not-yet-replicated records) are re-sent
    // after a backoff with the same identity, never failed early.
    int64_t produceTimeoutMs = 5'000;
```

- [ ] **Step 8: Run to verify they pass**

Run: `cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'ProducerRoutePark|TransactionalProducerTest|UnknownProducerId|ProducerAckPath|Session' --output-on-failure -j14`
Expected: all PASS (`ProducerUnknownProducerIdTest` pins `cfg.clientVersion == rpc::kClientVersion`, which follows the constant).

Then the broker builds against the new header: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'UnknownProducerId|ClientGatewayEgress|SessionAckSink' --output-on-failure -j14`
Expected: PASS (no broker code changed; sessions now announce 6).

- [ ] **Step 9: Commit (two repos)**

```bash
git -C ~/Projects/se-worktrees/rune-protocol add include/rune/protocol/client/Protocol.h tests/protocol/client/DataMessagesTest.cpp
git -C ~/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): ErrorCode::TransientLogFull = 42, client version 6 (old clients answered NotLeader)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ~/Projects/se-worktrees/rune-client-cpp add src/producer/ProducerImpl.cpp include/rune/client/producer/TransactionalProducerImpl.h src/producer/TransactionalProducerImpl.cpp include/rune/client/producer/Producer.h tests/producer/ProducerRouteParkTest.cpp tests/producer/TransactionalProducerTest.cpp
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): retry TransientLogFull (42) after a backoff, same identity, no metadata refresh

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller):** full `*Regression*` suite in protocol, client and broker (commands in Execution context).

---

### Task 2: Storage — `InMemoryLog` data-path surface + `handleProduceBatch` over either log

**Files:**
- Modify: `libs/storage/include/rune/storage/InMemoryLog.h` (header comment, includes, public surface, `appendImpl`, `evictOldest`, fields)
- Modify: `libs/storage/include/rune/storage/ProducePath.h` (`struct ProduceOutcome` — `logStartOffset` at ~`:113`; the declaration ~`:160-174`)
- Modify: `libs/storage/src/ProducePath.cpp:29-43` (definition head), `:275-294` (step 7, the append), end of file (instantiations, before `} // namespace rune::storage` at ~`:345`)
- Create: `libs/storage/tests/storage/InMemoryLogTransientTest.cpp`
- Modify: `libs/storage/CMakeLists.txt:23-24` (`rune_in_memory_log_tests` sources)

**Interfaces:**
- Consumes (Task 1): `rune::protocol::client::ErrorCode::TransientLogFull`.
- Produces (namespace `rune::storage`):
  ```cpp
  class InMemoryLog {
    static constexpr int64_t kRefusedFull     = -1;   // would evict at/above the eviction floor (or BackPressure policy full)
    static constexpr int64_t kRefusedTooLarge = -2;   // batch larger than the whole ring
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len, int32_t lastOffsetDelta, int64_t& outBaseOffset);
    void    setLeaderEpochStamp(int32_t epoch) noexcept;   [[nodiscard]] int32_t leaderEpochStamp() const noexcept;
    void    setEvictionFloor(int64_t offset) noexcept;     [[nodiscard]] int64_t evictionFloor() const noexcept;
    void    resetTo(int64_t offset) noexcept;
    [[nodiscard]] constexpr bool storageFailed() const noexcept;  void markStorageFailed(int) noexcept;
    [[nodiscard]] uint64_t fullRefusals() const noexcept;  [[nodiscard]] uint64_t evictedRecords() const noexcept;
  };
  struct ProduceOutcome { /* … */ bool logFull = false; };
  template <class LogT> ProduceOutcome handleProduceBatch(/* same params */, LogT& log, /* … */) noexcept;  // Log, InMemoryLog
  ```
  `appendNoSyncRestamp` returns `nextOffset` (= base + delta + 1) or a negative refusal code; the default eviction floor is `INT64_MAX` (unbounded — today's `DropOldest` behaviour for every existing caller).

- [ ] **Step 1: Write the failing tests** — create `libs/storage/tests/storage/InMemoryLogTransientTest.cpp`:

```cpp
// InMemoryLogTransientTest.cpp — Phase 2b E2: the InMemoryLog surface a
// TRANSIENT topic's data path needs: the zero-copy restamp append (base offset
// + leader epoch patched in the ring copy), the eviction floor (never evict
// uncommitted data — refuse instead), the refusal codes, resetTo (follower
// catch-up below the leader's ring start), and handleProduceBatch over the ring.
#include <gtest/gtest.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <cstdint>
#include <string>
#include <vector>

using rune::storage::InMemoryLog;
namespace pc = rune::protocol::client;

namespace {
std::vector<uint8_t> batchOf(int records, std::size_t valueBytes = 100) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
int32_t deltaOf(const std::vector<uint8_t>& b) {
    return rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
}
} // namespace

TEST(InMemoryLogTransientRegression, RestampAppendPatchesBaseOffsetAndEpochInTheRingCopyOnly) {
    InMemoryLog log(1 << 20, 1024);
    log.setLeaderEpochStamp(3);
    const auto a = batchOf(2), b = batchOf(3);
    int64_t base = -1;
    ASSERT_EQ(log.appendNoSyncRestamp(a.data(), a.size(), deltaOf(a), base), 2);
    EXPECT_EQ(base, 0);
    const std::vector<uint8_t> bBefore = b;
    ASSERT_EQ(log.appendNoSyncRestamp(b.data(), b.size(), deltaOf(b), base), 5);
    EXPECT_EQ(base, 2);
    EXPECT_EQ(b, bBefore) << "the caller's batch is never mutated (it may alias a read-only fragment)";
    const auto v = log.readView(2, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 2);
    EXPECT_EQ(r.partitionLeaderEpoch(), 3) << "the leader stamps its epoch (Kafka parity; KIP-320 lineage)";
    EXPECT_TRUE(r.ensureValid()) << "base offset and leader epoch are outside the CRC";
}

TEST(InMemoryLogTransientRegression, EvictionFloorRefusesInsteadOfEvictingUncommittedData) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);   // room for exactly 4 batches
    log.setEvictionFloor(0);                // nothing committed yet
    int64_t base = -1;
    for (int i = 0; i < 4; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0) << "an uncommitted batch was evicted";
    EXPECT_EQ(log.logEndOffset(), 4);
    EXPECT_EQ(log.fullRefusals(), 1u);
    log.setEvictionFloor(2);                // offsets 0 and 1 are committed (HW = 2)
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 5);
    EXPECT_EQ(log.logStartOffset(), 1) << "exactly one committed batch made room";
    EXPECT_EQ(log.evictedRecords(), 1u);
}

TEST(InMemoryLogTransientRegression, RecordCapEvictionHonoursTheFloorToo) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 20, /*maxRecords=*/2);
    log.setEvictionFloor(0);
    int64_t base = -1;
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0);
}

TEST(InMemoryLogTransient, DefaultFloorKeepsDropOldestBehaviour) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);
    int64_t base = -1;
    for (int i = 0; i < 10; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.logEndOffset(), 10);
    EXPECT_EQ(log.logStartOffset(), 6) << "4 batches retained, oldest evicted (unbounded floor)";
    EXPECT_EQ(log.fullRefusals(), 0u);
    EXPECT_EQ(log.evictedRecords(), 6u);
}

TEST(InMemoryLogTransientRegression, BatchLargerThanTheRingIsRefusedTooLarge) {
    const auto big = batchOf(1, 4096);
    InMemoryLog log(1024, 16);
    int64_t base = -1;
    EXPECT_EQ(log.appendNoSyncRestamp(big.data(), big.size(), 0, base), InMemoryLog::kRefusedTooLarge);
    EXPECT_EQ(log.logEndOffset(), 0);
    EXPECT_EQ(log.appendNoSync(big.data(), big.size(), 0), InMemoryLog::kRefusedTooLarge)
        << "the plain append refuses too (was an assert)";
}

TEST(InMemoryLogTransient, ResetToRestartsTheRingEmptyAtAnOffset) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 16, 64);
    int64_t base = -1;
    for (int i = 0; i < 3; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    log.resetTo(42);
    EXPECT_EQ(log.logStartOffset(), 42);
    EXPECT_EQ(log.logEndOffset(), 42);
    EXPECT_TRUE(log.readView(42, 1 << 16).empty());
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 43);
    EXPECT_EQ(base, 42);
    const auto v = log.readView(42, 1 << 16);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(rune::protocol::RecordBatchReader(v.data, v.len).baseOffset(), 42);
}

TEST(InMemoryLogTransientRegression, ProducePathOverTheRingAppendsAndRefusesFullAsRetriable) {
    const auto x = batchOf(1);
    InMemoryLog log(2 * x.size(), 1024);
    log.setEvictionFloor(0);
    rune::storage::ProducerStateManager psm;
    auto produce = [&](InMemoryLog& l) {
        return rune::storage::handleProduceBatch(x.data(), x.size(), /*topicId=*/1, /*partition=*/0,
                                                 /*producerId=*/-1, /*epoch=*/0, /*baseSeq=*/-1, l, psm,
                                                 /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*tier=*/1);
    };
    const auto o1 = produce(log);
    ASSERT_TRUE(o1.isOK());
    EXPECT_EQ(o1.baseOffset, 0);
    ASSERT_TRUE(produce(log).isOK());
    const auto full = produce(log);
    EXPECT_EQ(full.error, pc::ErrorCode::TransientLogFull) << "retriable (42, ruling 2): nothing was appended";
    EXPECT_TRUE(full.logFull);
    EXPECT_EQ(log.logEndOffset(), 2);

    InMemoryLog tiny(64, 4);
    const auto tooLarge = produce(tiny);
    EXPECT_EQ(tooLarge.error, pc::ErrorCode::MessageTooLarge) << "a batch larger than the ring can never fit";
    EXPECT_TRUE(tooLarge.logFull);
}
```

In `libs/storage/CMakeLists.txt` change

```cmake
    add_executable(rune_in_memory_log_tests tests/storage/InMemoryLogTest.cpp
        tests/storage/InMemoryLogReadViewTest.cpp)
```
to
```cmake
    add_executable(rune_in_memory_log_tests tests/storage/InMemoryLogTest.cpp
        tests/storage/InMemoryLogReadViewTest.cpp
        tests/storage/InMemoryLogTransientTest.cpp)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j14 --target rune_in_memory_log_tests`
Expected: compile errors `no member named 'setLeaderEpochStamp' in 'rune::storage::InMemoryLog'`, `no member named 'appendNoSyncRestamp'`, `no member named 'logFull' in 'rune::storage::ProduceOutcome'`.

- [ ] **Step 3: Implement — `InMemoryLog.h`**

Header comment: after the paragraph that ends `// NOT thread-safe (same contract as rune::storage::Log — single writer).` insert:

```cpp
//
// Phase 2b E2 (TRANSIENT topics — the produce/subscribe data path):
//   appendNoSyncRestamp(): the Log-compatible zero-copy restamp append used by
//     the worker produce path (handleProduceBatch<InMemoryLog>): ONE memcpy into
//     the ring, then the assigned base offset and (when leading) the leader epoch
//     are patched in the ring copy — never in the caller's bytes.
//   Eviction floor: DropOldest never evicts a batch whose lastOffset >= the
//     floor (the owning replica keeps it at its high watermark: uncommitted
//     data).  An append that would need such an eviction is REFUSED
//     (kRefusedFull).  Default floor = INT64_MAX (unbounded: plain DropOldest).
//   kRefusedTooLarge: a batch larger than the whole ring (never storable).
//   resetTo(): restart EMPTY at an offset (follower below the leader's ring start).
```

Includes: after `#include <cstring>` add `#include <limits>`; after `#include <rune/storage/LogReadView.h>` add `#include <rune/protocol/RecordBatch.h>`.

In the public section, right after

```cpp
    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendImpl(data, len, lastOffset);
    }
```
insert:

```cpp
    // -----------------------------------------------------------------------
    // Phase 2b E2 — the transient produce path.
    // -----------------------------------------------------------------------
    /// The append would have to evict a batch at/above the eviction floor (or the
    /// BackPressure policy's ring is full): nothing appended, retry later.
    static constexpr int64_t kRefusedFull     = -1;
    /// The batch is larger than the whole ring: it can never be stored.
    static constexpr int64_t kRefusedTooLarge = -2;

    /// Log::appendNoSyncRestamp parity: assigns base = logEndOffset(), copies
    /// [batch, batch+len) into the ring ONCE and patches the base offset
    /// (bytes [0..7]) and — when leaderEpochStamp() >= 0 — the partition leader
    /// epoch (bytes [12..15], outside the CRC) in the RING COPY; `batch` is never
    /// mutated.  Returns nextOffset (base + lastOffsetDelta + 1) and sets
    /// outBaseOffset, or a negative refusal code (kRefusedFull / kRefusedTooLarge)
    /// with nothing appended.  Zero allocation.
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len,
                                int32_t lastOffsetDelta, int64_t& outBaseOffset) {
        const int64_t base = logEndOffset_;
        const int64_t r = appendImpl(batch, len, base + static_cast<int64_t>(lastOffsetDelta));
        if (r < 0) return r;
        uint8_t* dst = ring_.data() + backEntry().ringOffset;
        const auto ub = static_cast<uint64_t>(base);
        for (int i = 0; i < 8; ++i)
            dst[rune::protocol::batch::kBaseOffsetOffset + i] = static_cast<uint8_t>(ub >> (8 * i));
        if (leaderEpochStamp_ >= 0 &&
            len >= static_cast<std::size_t>(rune::protocol::batch::kPartitionLeaderEpochOffset + 4)) {
            const auto ue = static_cast<uint32_t>(leaderEpochStamp_);
            for (int i = 0; i < 4; ++i)
                dst[rune::protocol::batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(ue >> (8 * i));
        }
        outBaseOffset = base;
        return logEndOffset_;
    }

    /// Log::setLeaderEpochStamp parity: the epoch appendNoSyncRestamp writes into
    /// every batch it stores (-1 = leave the producer's bytes).  Set by the owning
    /// PartitionReplica on leadership changes.  Owner thread only.
    void setLeaderEpochStamp(int32_t epoch) noexcept { leaderEpochStamp_ = epoch; }
    [[nodiscard]] int32_t leaderEpochStamp() const noexcept { return leaderEpochStamp_; }

    /// Eviction floor: DropOldest never evicts a batch whose lastOffset >= floor.
    /// The owning replica sets it to its high watermark before each append.
    void setEvictionFloor(int64_t offset) noexcept { evictionFloor_ = offset; }
    [[nodiscard]] int64_t evictionFloor() const noexcept { return evictionFloor_; }
```

Right before `[[nodiscard]] bool isDiskBacked() const noexcept { return false; }` insert:

```cpp
    /// Phase 2b E2 (follower catch-up): drop every batch and restart the ring
    /// EMPTY at `offset` (logStart = logEnd = offset).  Keeps the eviction floor
    /// and the leader-epoch stamp.  Cold.
    void resetTo(int64_t offset) noexcept {
        entryHead_ = 0; entryCount_ = 0;
        head_ = 0; tail_ = 0; bytesUsed_ = 0;
        logStartOffset_ = offset;
        logEndOffset_   = offset;
    }

    /// Log parity for handleProduceBatch<LogT>: a ring never fails storage.
    [[nodiscard]] constexpr bool storageFailed() const noexcept { return false; }
    void markStorageFailed(int /*err*/) noexcept {}

    /// Appends refused because the ring would have had to evict at/above the floor.
    [[nodiscard]] uint64_t fullRefusals() const noexcept { return fullRefusals_; }
    /// Records evicted (retention), summed over evicted batches.
    [[nodiscard]] uint64_t evictedRecords() const noexcept { return evictedRecords_; }
```

In `appendImpl`, replace

```cpp
        assert(len <= maxBytes_ && "batch larger than ring capacity — cannot store");

        const int64_t baseOffset = logEndOffset_;
```
with
```cpp
        // Phase 2b E2: a batch larger than the whole ring can never be stored
        // (was an assert — a release build corrupted the ring).
        if (len > maxBytes_) [[unlikely]] return kRefusedTooLarge;

        const int64_t baseOffset = logEndOffset_;
```
and inside the `DropOldest` loop replace
```cpp
                if (bytesUsed_ + need <= maxBytes_ && entryCount_ < maxRecords_)
                    break; // fits — stop evicting
                evictOldest();
```
with
```cpp
                if (bytesUsed_ + need <= maxBytes_ && entryCount_ < maxRecords_)
                    break; // fits — stop evicting
                // Phase 2b E2: never evict at/above the floor (uncommitted data
                // of a transient partition) — refuse the append instead.
                if (frontEntry().lastOffset >= evictionFloor_) [[unlikely]] {
                    ++fullRefusals_;
                    return kRefusedFull;
                }
                evictOldest();
```
and in the `BackPressure` branch replace
```cpp
                return -1; // sentinel: ring full, caller must retry later
```
with
```cpp
                ++fullRefusals_;
                return kRefusedFull; // sentinel: ring full, caller must retry later
```

In `evictOldest()`, right after `const Entry& oldest = frontEntry();` add:
```cpp
        evictedRecords_ += static_cast<uint64_t>(oldest.lastOffset + 1 - oldest.baseOffset);
```

Fields: after `int64_t               logEndOffset_   = 0;` add:
```cpp
    int64_t               evictionFloor_  = std::numeric_limits<int64_t>::max();   ///< E2: see setEvictionFloor
    int32_t               leaderEpochStamp_ = -1;                                   ///< E2: see setLeaderEpochStamp
    uint64_t              fullRefusals_   = 0;
    uint64_t              evictedRecords_ = 0;
```

- [ ] **Step 4: Implement — `ProducePath.h` / `ProducePath.cpp`**

`ProducePath.h`: add `#include <rune/storage/InMemoryLog.h>` after `#include <rune/storage/Log.h>`. In `struct ProduceOutcome`, after `int64_t  logStartOffset = -1;` add:

```cpp
    /// Phase 2b E2: the append was refused by an InMemoryLog (a TRANSIENT
    /// partition's ring): error TransientLogFull (42, retriable — the ring is
    /// full of uncommitted data) or MessageTooLarge (larger than the ring).
    /// Nothing was appended.  Never set for a disk Log.
    bool     logFull = false;
```

Replace the declaration (from `ProduceOutcome handleProduceBatch(` through `    bool                              crcPreverified = false) noexcept;`) with:

```cpp
// Phase 2b E2: templated over the concrete log type — rune::storage::Log (disk)
// or rune::storage::InMemoryLog (a TRANSIENT partition's ring).  No virtual
// dispatch; the two instantiations live in ProducePath.cpp.
template <class LogT>
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
    std::size_t                       batchLen,
    int32_t                           topicId,
    int32_t                           partition,
    int64_t                           producerId,
    int16_t                           producerEpoch,
    int32_t                           baseSequence,
    LogT&                             log,
    rune::storage::ProducerStateManager& producerState,
    bool                              acksAll,
    int                               isrSize,
    int                               minIsr,
    uint8_t                           durabilityTier,
    bool                              crcPreverified = false) noexcept;

extern template ProduceOutcome handleProduceBatch<Log>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
extern template ProduceOutcome handleProduceBatch<InMemoryLog>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
```

`ProducePath.cpp`: replace the definition head

```cpp
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
```
…through…
```cpp
    rune::storage::Log&               log,
```
by
```cpp
template <class LogT>
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
    std::size_t                       batchLen,
    int32_t                           topicId,
    int32_t                           partition,
    int64_t                           producerId,
    int16_t                           producerEpoch,
    int32_t                           baseSequence,
    LogT&                             log,
```
(the remaining parameters and the body stay). In step 7 replace

```cpp
        if (appended < 0) {
            // Returns negative on back-pressure (e.g. InMemoryLog full).
            spdlog::warn("ProducePath: appendNoSyncRestamp refused (back-pressure) "
                         "topicId={} partition={}", topicId, partition);
            out.error = pc::ErrorCode::Internal;
            return out;
        }
```
with
```cpp
        if (appended < 0) [[unlikely]] {
            // Phase 2b E2: only an InMemoryLog refuses (a disk Log throws).
            // TRANSIENT_FULL: the ring cannot take the batch without evicting
            // uncommitted data (at/above the HW) — TransientLogFull (42,
            // controller ruling 2): retriable on the same leader, no metadata
            // refresh; a pre-v6 session is answered NotLeader by encodeFor.  A
            // batch larger than the whole ring can never fit: MessageTooLarge.
            // The caller counts + rate-limits the log line (hot path: no log here).
            out.logFull = true;
            out.error   = appended == InMemoryLog::kRefusedTooLarge ? pc::ErrorCode::MessageTooLarge
                                                                    : pc::ErrorCode::TransientLogFull;
            return out;
        }
```
At the end of the file, before `} // namespace rune::storage`, add:

```cpp
template ProduceOutcome handleProduceBatch<Log>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
template ProduceOutcome handleProduceBatch<InMemoryLog>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
```

(The `rune::node::handleProduceBatch` shim at the bottom of `ProducePath.h` takes `rune::storage::Log&` and deduces `Log` — unchanged. Existing callers in `Worker.h`, `ClientGateway.cpp`, `DrHarness.h`, `ProducePathTest.cpp`, `ProducePathBenchmark.cpp` pass a `Log` and deduce it.)

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'InMemoryLog|ProducePath|DrHarness|WorkerProducePath' --output-on-failure -j14`
Expected: all PASS (the existing `InMemoryLogRingTest` / `InMemoryLogTest` keep passing: the default floor is unbounded; every existing `handleProduceBatch` caller deduces `Log`).

- [ ] **Step 6: Commit**

```bash
git add libs/storage/include/rune/storage/InMemoryLog.h libs/storage/include/rune/storage/ProducePath.h libs/storage/src/ProducePath.cpp libs/storage/tests/storage/InMemoryLogTransientTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): InMemoryLog restamp append, HW eviction floor, refusal codes; produce path over either log

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---


### Task 3: Replication — transient replica: epoch stamp, HW floor, refusals, follower ring reset (+ `logGeneration`), zero-alloc apply

**Files:**
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h:1199-1202` (new public/private block after `appliesDroppedTruncatePending_`), `:1270-1285` (`makeFetchResponse`; `r.logStartOffset = 0;` at `:1279`)
- Modify: `libs/replication/src/PartitionReplica.cpp:276-279` (ctor), `:455-459` (`becomeLeader`), `:514` (`becomeFollower`), `:779` (self-demotion), `:566-577` (`append`), `:635-636` (`appendControlBatch`), `:679` (`appendKeyed`), `:1012-1013` (`onFetchRequest` empty-view skip), `:1289-1290` + `:1360-1397` (`applyReplicated`), new `resetTransientLogTo_` (after `syncIsrToLeaderState`)
- Create: `libs/replication/tests/replication/TransientReplicaTest.cpp`
- Modify: `libs/replication/tests/replication/ZeroAllocReplicaTest.cpp` (one test), `libs/replication/CMakeLists.txt:82-101` (source list)

**Interfaces:**
- Consumes (Task 2): `InMemoryLog::setLeaderEpochStamp`, `setEvictionFloor`, `resetTo`, `kRefusedFull`, `fullRefusals()`, `logStartOffset()`.
- Produces (namespace `rune::replication`, class `PartitionReplica`):
  ```cpp
  void prepareTransientAppend() noexcept;                         // floor := HW (leader) / followerHw_ (follower); no-op on disk
  [[nodiscard]] uint64_t transientLogResetsForTest() const noexcept;
  [[nodiscard]] uint64_t transientApplyRefusalsForTest() const noexcept;
  // append() / appendKeyed() / appendControlBatch() on a transient replica return kAppendBackPressure (-4) when the ring refuses.
  // FetchResponseMsg::logStartOffset = the leader ring start for a transient partition (0 for disk).
  // resetTransientLogTo_ (follower ring reset) bumps logGeneration() and sets lastTruncationOffset()
  //   to the OLD ring start: the ring's existing content changed, so parked waiters, the RC state
  //   (RcFacts.logGeneration) and a view over the source (Task 9) all re-validate.
  ```

- [ ] **Step 1: Write the failing tests** — create `libs/replication/tests/replication/TransientReplicaTest.cpp`:

```cpp
// TransientReplicaTest.cpp — Phase 2b E2: replication of a TRANSIENT
// (InMemoryLog) partition.  The leader's ring never evicts at/above its high
// watermark (refuses instead), and a follower whose fetch position is below the
// leader's ring start (a restarted node rejoining empty) restarts its ring there
// and catches up — before the fix it fetched an evicted offset forever.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <cstdint>
#include <span>
#include <vector>

using namespace rune::replication;

namespace {
PartitionConfig tcfg(NodeId self) {
    PartitionConfig c;
    c.topicId = 7; c.partitionId = 0; c.selfId = self; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
    c.replicaLagMaxMs = 600'000; c.replicaLagMaxOffset = 1'000'000'000;
    c.pushOnAppend = false; c.maxFetchBytes = 1 << 20;
    return c;
}
constexpr int kCap = 20'000;   // hard cap on every drive loop
} // namespace

TEST(TransientReplicaRegression, LeaderRingNeverEvictsAboveTheHighWatermarkRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    Offset last = 0;
    int appended = 0;
    for (int i = 0; i < 64; ++i) {   // follower 2 (in the ISR) never fetched: HW stays 0
        last = r1.append(std::span<const uint8_t>(payload), /*acksAll=*/false);
        if (last < 0) break;
        ++appended;
    }
    ASSERT_EQ(last, PartitionReplica::kAppendBackPressure) << "a full ring must refuse, not evict unreplicated data";
    EXPECT_GT(appended, 0);
    EXPECT_EQ(mem1.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(mem1.fullRefusals(), 1u);

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r1.highWatermark() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    ASSERT_EQ(r1.highWatermark(), r1.logEndOffset()) << "follower never caught up";
    EXPECT_GE(r1.append(std::span<const uint8_t>(payload), false), 0) << "committed records make room";
    EXPECT_GT(mem1.logStartOffset(), 0);
}

TEST(TransientReplicaRegression, FollowerBehindTheLeaderRingStartResetsAndCatchesUpRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    const std::vector<NodeId> leaderOnly = {1};
    // Follower 2 is OUT of the ISR (a restarted node rejoining empty): the HW
    // follows the leader alone, so the leader's ring evicts committed records.
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, leaderOnly, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all,        &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 40; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    const Offset leaderStart = mem1.logStartOffset();
    ASSERT_GT(leaderStart, 0) << "precondition: the leader's ring evicted its oldest records";
    const uint64_t genBefore = r2.logGeneration();

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    EXPECT_EQ(r2.logEndOffset(), r1.logEndOffset())
        << "stalled: a follower below the leader's ring start is never served from its LEO";
    EXPECT_EQ(r2.transientLogResetsForTest(), 1u);
    EXPECT_EQ(mem2.logStartOffset(), leaderStart);
    // The reset changed the ring's existing content: one generation bump, cut at
    // the OLD ring start (everything held was dropped) — RC state / waiters /
    // views over this source re-validate (Task 9 consumes this).
    EXPECT_EQ(r2.logGeneration(), genBefore + 1);
    EXPECT_EQ(r2.lastTruncationOffset(), 0);
}

TEST(TransientReplicaRegression, LeaderAppendStampsItsEpochIntoTheRing) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    InMemoryTransport transport;
    PartitionReplica r1(tcfg(1), Role::Leader, 4, {1}, {1}, &mem, &transport);
    EXPECT_EQ(mem.leaderEpochStamp(), 4) << "a transient leader stamps restamp-appends with its epoch";
    r1.becomeFollower(5, 2);
    EXPECT_EQ(mem.leaderEpochStamp(), -1) << "a follower keeps the producer's bytes";
}
```

Append to `libs/replication/tests/replication/ZeroAllocReplicaTest.cpp` (add `#include <rune/storage/InMemoryLog.h>` next to `#include <rune/storage/Log.h>`):

```cpp
// Phase 2b E2 (fix-on-sight): applyReplicated copied the new suffix of every
// replicated response into a fresh std::vector before appending — a heap
// allocation per apply on the follower's hot path (disk AND transient).
TEST(ZeroAllocReplicaRegression, TransientFollowerApplyAllocatesNothing) {
    rune::storage::InMemoryLog mem(1 << 20, 4096);
    SinkTransport t;
    PartitionConfig c = cfg(/*push=*/false);
    c.selfId = 2;
    PartitionReplica follower(c, Role::Follower, 1, {1, 2}, {1, 2}, &mem, &t);
    auto batchAt = [](int64_t base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(512, 'v'), 1000);
        auto x = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(x.data(), 1);
        return x;
    };
    const auto b0 = batchAt(0), b1 = batchAt(1);
    FetchResponseMsg r0{};
    r0.leaderEpoch = 1; r0.partition = 0; r0.highWatermark = 1; r0.logStartOffset = 0;
    r0.errorCode = 0; r0.topicId = 0; r0.records = std::span<const uint8_t>(b0);
    follower.applyReplicated(r0);   // warm-up: the epoch-cache entry for epoch 1 is created here
    ASSERT_EQ(follower.logEndOffset(), 1);
    FetchResponseMsg r1 = r0;
    r1.highWatermark = 2;
    r1.records = std::span<const uint8_t>(b1);
    uint64_t n = 0;
    {
        Armed a;
        follower.applyReplicated(r1);
        n = a.count();
    }
    EXPECT_EQ(follower.logEndOffset(), 2);
    EXPECT_EQ(n, 0u) << "the follower's replicated apply must not allocate";
}
```

In `libs/replication/CMakeLists.txt`, in the `add_executable(rune_replication_tests …)` list, change `        tests/replication/LeaderEpochFenceTest.cpp)` to:

```cmake
        tests/replication/LeaderEpochFenceTest.cpp
        tests/replication/TransientReplicaTest.cpp)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j14 --target rune_replication_tests rune_replication_zero_alloc_tests`
Expected: compile error `no member named 'transientLogResetsForTest'`. After adding only the accessors: `LeaderRingNeverEvictsAboveTheHighWatermark` FAILS (`last` is a non-negative offset: the ring evicted unreplicated data), `FollowerBehindTheLeaderRingStart…` FAILS (stalled), `LeaderAppendStampsItsEpoch…` FAILS (`-1`), `TransientFollowerApplyAllocatesNothing` FAILS (`n >= 1`).

- [ ] **Step 3: Implement — `PartitionReplica.h`**

In `makeFetchResponse`, replace `        r.logStartOffset = 0;` with:

```cpp
        // Phase 2b E2: a TRANSIENT leader advertises its ring start — a follower
        // below it restarts its ring there (applyReplicated).  Disk: 0, unchanged.
        r.logStartOffset = memoryBacked_ ? memLog_->logStartOffset() : 0;
```

After the block

```cpp
private:
    uint64_t truncatesDeferred_ = 0;
    uint64_t appliesDroppedTruncatePending_ = 0;
```
insert:

```cpp
public:
    /// Phase 2b E2 (TRANSIENT partitions): refresh the ring's eviction floor to
    /// the current high watermark before an append that bypasses this replica
    /// (the worker produce path appends to memoryLog() directly).  No-op on a
    /// disk partition.  Owner thread.
    void prepareTransientAppend() noexcept { refreshTransientEvictionFloor_(); }
    /// Follower ring restarts at the leader's ring start / replicated applies the ring refused.
    [[nodiscard]] uint64_t transientLogResetsForTest() const noexcept { return transientLogResets_; }
    [[nodiscard]] uint64_t transientApplyRefusalsForTest() const noexcept { return transientApplyRefusals_; }
private:
    uint64_t transientLogResets_     = 0;
    uint64_t transientApplyRefusals_ = 0;
    /// The ring never evicts at/above what is not committed: the leader's HW
    /// (min-ISR-gated served HW), a follower's followerHw_.
    void refreshTransientEvictionFloor_() noexcept {
        if (memoryBacked_) memLog_->setEvictionFloor(role_ == Role::Leader ? highWatermark() : followerHw_);
    }
    /// Kafka leader-epoch stamp of the ACTIVE log (disk Log or ring).
    void stampEpochOnLog_(int32_t epoch) noexcept {
        if (memoryBacked_) { if (memLog_ != nullptr) memLog_->setLeaderEpochStamp(epoch); }
        else if (diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(epoch);
    }
    /// A transient follower below the leader's ring start restarts its ring there.  Cold.
    void resetTransientLogTo_(Offset start);
```

- [ ] **Step 4: Implement — `PartitionReplica.cpp`**

Constructor (leader seed): replace

```cpp
            if (!memoryBacked_ && diskLog_ != nullptr)
                diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
```
with
```cpp
            stampEpochOnLog_(static_cast<int32_t>(epoch_));
```

`becomeLeader`: replace

```cpp
    if (!memoryBacked_ && diskLog_ != nullptr) {
        diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
        checkpointEpochs_();
    }
```
with
```cpp
    stampEpochOnLog_(static_cast<int32_t>(epoch_));
    if (!memoryBacked_ && diskLog_ != nullptr) checkpointEpochs_();
```

`becomeFollower` and the self-demotion path (two occurrences — use replace-all on the exact text): replace
`if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(-1);` with `stampEpochOnLog_(-1);`.

`append()`: replace

```cpp
    if (externalFollowerFlush_) {
        // Worker-driven replica (spec §3.1, every placement — F30): no inline
        // fsync on the worker (txn markers come through here).  Page-cache
        // append + a flush request the owning worker submits to its io backend
        // in the same pass.
        onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
        if (!memoryBacked_) followerFlushRequested_ = true;
    } else {
        onLog([&](auto& L){ L.append(batch, baseOffset); });
    }
```
with
```cpp
    if (memoryBacked_) {
        // Phase 2b E2: the ring never evicts at/above the HW — a full ring
        // refuses (retriable back-pressure), nothing appended.
        refreshTransientEvictionFloor_();
        if (memLog_->appendNoSync(batch.data(), batch.size(), baseOffset) < 0) [[unlikely]]
            return kAppendBackPressure;
    } else if (externalFollowerFlush_) {
        // Worker-driven replica (spec §3.1, every placement — F30): no inline
        // fsync on the worker (txn markers come through here).  Page-cache
        // append + a flush request the owning worker submits to its io backend
        // in the same pass.
        diskLog_->appendNoSync(batch.data(), batch.size(), baseOffset);
        followerFlushRequested_ = true;
    } else {
        diskLog_->append(batch, baseOffset);
    }
```

`appendControlBatch()`: replace

```cpp
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
```
with
```cpp
        refreshTransientEvictionFloor_();   // Phase 2b E2: never evict uncommitted data
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
```

`appendKeyed()`: replace `    onLog([&](auto& L){ L.append(batch, baseOffset); });` (the one right after `stampLeaderEpoch(batch.data(), …)` in `appendKeyed`) with:

```cpp
    refreshTransientEvictionFloor_();   // Phase 2b E2 (no-op on disk)
    if (onLog([&](auto& L){ return L.append(batch, baseOffset); }) < 0) [[unlikely]]
        return kAppendBackPressure;
```

`onFetchRequest()`: replace

```cpp
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_() && view.empty())
        [[unlikely]] return kErrNone;
```
with
```cpp
    // Phase 2b E2: a TRANSIENT follower fetching BELOW our ring start asks for
    // records the ring evicted (committed; retention) — answer it (no records;
    // makeFetchResponse carries our ring start) so it restarts its ring there,
    // instead of the read-map-failure skip below (which would never answer it).
    const bool belowRingStart = memoryBacked_ && req.fetchOffset < memLog_->logStartOffset();
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_() && view.empty() && !belowRingStart)
        [[unlikely]] return kErrNone;
```

`applyReplicated()`: right after

```cpp
    const Offset preLeo = logEndOffset_();
    const Offset preHw  = followerHw_;
```
insert:
```cpp
    // Phase 2b E2 — TRANSIENT follower behind the leader's ring start: the
    // leader no longer holds [our LEO, its ring start) (evicted committed
    // records), so a fetch from our LEO can never be served.  Restart our ring
    // EMPTY at the leader's ring start (Kafka: a follower out of range below the
    // leader's log start truncates fully and resumes there).  After preLeo is
    // captured, so the fast-ack below fetches from the new position at once.
    if (memoryBacked_ && role_ == Role::Follower && resp.logStartOffset > logEndOffset_()) [[unlikely]]
        resetTransientLogTo_(resp.logStartOffset);
```

In the record-batch branch replace

```cpp
            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
                // Append only the strictly-new suffix.
                std::vector<uint8_t> raw(resp.records.begin() + static_cast<std::ptrdiff_t>(newStart),
                                          resp.records.end());
```
…through…
```cpp
                if (assignEpochsFromBatches_(raw.data(), raw.size()) &&
                    !memoryBacked_ && diskLog_ != nullptr)
                    checkpointEpochs_();
            }
```
with
```cpp
            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
                // Append only the strictly-new suffix — straight from the
                // response view (Phase 2b E2 fix-on-sight: was copied into a
                // fresh std::vector per apply — a hot-path heap allocation).
                // THROUGHPUT FIX: appendNoSync — do NOT fsync per apply.  The syncing
                // append() cost ~693µs mean (disk fsync) PER batch on the follower,
                // capping acks=all at ~1.5k rec/s.  acks=all crash-durability comes
                // from ISR REPLICATION (multiple page-cache copies), NOT per-record
                // fsync on each follower (same as Kafka's default + rune's documented
                // appendNoSync contract).  A single coalesced sync() runs once per
                // duty cycle below (followerApplyDirty_), amortising the fsync.
                const uint8_t* const newData = resp.records.data() + newStart;
                const std::size_t    newLen  = resp.records.size() - newStart;
                refreshTransientEvictionFloor_();   // E2: a transient ring never evicts above followerHw_
                if (onLog([&](auto& L){ return L.appendNoSync(newData, newLen, lastOff); }) < 0) [[unlikely]] {
                    // E2: the ring is full of records above our HW — nothing applied;
                    // the leader re-serves them once our HW (from its answers) moves.
                    ++transientApplyRefusals_;
                } else {
                    followerApplyDirty_ = true;
                    // Kafka LeaderEpochFileCache.assign on append: the cache is built
                    // from the epochs the leader STAMPED on the batches.
                    if (assignEpochsFromBatches_(newData, newLen) &&
                        !memoryBacked_ && diskLog_ != nullptr)
                        checkpointEpochs_();
                }
            }
```

In the raw-bytes fallback branch of `applyReplicated` replace

```cpp
            onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
            followerApplyDirty_ = true;
            if (assignEpochsFromBatches_(batch.data(), batch.size()) &&
                !memoryBacked_ && diskLog_ != nullptr)
                checkpointEpochs_();
```
with
```cpp
            refreshTransientEvictionFloor_();
            if (onLog([&](auto& L){ return L.appendNoSync(batch.data(), batch.size(), baseOffset); }) < 0) [[unlikely]] {
                ++transientApplyRefusals_;
            } else {
                followerApplyDirty_ = true;
                if (assignEpochsFromBatches_(batch.data(), batch.size()) &&
                    !memoryBacked_ && diskLog_ != nullptr)
                    checkpointEpochs_();
            }
```

Add the definition right after `void PartitionReplica::syncIsrToLeaderState()`'s body:

```cpp
// Phase 2b E2: a TRANSIENT follower whose position is below the leader's ring
// start.  Cold (once per catch-up episode).
void PartitionReplica::resetTransientLogTo_(Offset start)
{
    const Offset was       = logEndOffset_();
    const Offset prevStart = memLog_->logStartOffset();
    memLog_->resetTo(start);
    // F15 §2.1 step 3 parity (truncateLog): the log's existing content changed —
    // every held record is gone.  Bump the generation, cut at the OLD ring start
    // (nothing below it was held), so parked waiters, the RC state and a view
    // over this source (Phase 2b E2 Task 9) re-validate instead of trusting a
    // cursor into the dropped ring.
    lastTruncationOffset_ = prevStart;
    ++logGeneration_;
    // Every epoch entry named offsets this ring no longer holds; the next
    // fetch carries lastFetchedEpoch -1 (an empty log: no lineage to validate)
    // and the cache is rebuilt from the stamped batches the leader sends.
    epochCache_.truncateFromEnd(std::numeric_limits<int64_t>::min());
    followerSync_.forceBehind();   // fetch on at once (the leader holds [start, LEO))
    ++transientLogResets_;
    spdlog::warn("PartitionReplica[node={} topic={} part={}]: transient follower at offset {} is below the "
                 "leader's ring start {} — ring restarted empty at {} (resets={})",
                 cfg_.selfId, cfg_.topicId, cfg_.partitionId, was, start, start, transientLogResets_);
}
```

(`<limits>` and `<spdlog/spdlog.h>` are already included in `PartitionReplica.cpp`.)

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'TransientReplica|ZeroAllocReplica|PartitionReplica|EpochFencing|HighWatermark|LeaderEpoch|TransientTopic_|RebalanceFailover|FollowerSync' --output-on-failure -j14`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/replication/include/rune/replication/PartitionReplica.h libs/replication/src/PartitionReplica.cpp libs/replication/tests/replication/TransientReplicaTest.cpp libs/replication/tests/replication/ZeroAllocReplicaTest.cpp libs/replication/CMakeLists.txt
git commit -m "feat(replication): transient ring never evicts above the HW; follower below the leader's ring start resets; zero-alloc apply

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---


### Task 4: Worker — produce to a transient partition (shard ingress + op ring), marker back-pressure

**Files:**
- Create: `libs/node/tests/integration/TransientRig.h`
- Create: `libs/node/tests/integration/TransientDataPathTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target after `gtest_discover_tests(rune_hotpath_b_alloc_regression_tests)`, ~`:916`)
- Modify: `libs/node/include/rune/node/Worker.h` — `produceCore_` (~`:6553`: the `if (!log) { … }` block ~`:6572-6601`, the `asyncStrict` line ~`:6704`, the `handleProduceBatch` call ~`:6726-6735`), `executeAppendControlRecordWire_` (the `kAppendMinIsr` branch ~`:3955`), `getLogAndReplicaForPartition_` (comment ~`:6222`), the push-read test accessors (`deferredPushReadsForTest`, ~`:4640`) and the private push fields (after `pushScratch_`, ~`:4648`), a new `noteTransientFull_` next to `notePushReadOversize_` (~`:3513`)

**Interfaces:**
- Consumes: `ErrorCode::TransientLogFull` (Task 1); `handleProduceBatch<InMemoryLog>`, `ProduceOutcome::logFull` (Task 2); `PartitionReplica::prepareTransientAppend()`, `kAppendBackPressure` from transient `appendControlBatch` (Task 3).
- Produces (class `rune::node::Worker`, test accessors used by Tasks 5, 6, 11, 12):
  ```cpp
  [[nodiscard]] uint64_t transientFullRefusalsForTest() const noexcept;
  [[nodiscard]] uint64_t transientCursorSkipsForTest() const noexcept;
  [[nodiscard]] uint64_t transientRecordsSkippedForTest() const noexcept;
  ```
  and the shared test rig `transient_rig::Rig` (`TransientRig.h`): `Rig(std::size_t ringBytes = 4u << 20, std::size_t maxBatches = 100'000, bool withFollower = false)`, members `mem`, `rep`, `pool`, `now`; `pass(frame*)`, `produce(batch, acks, int64_t* baseOut, pid, epoch, seq) → int16_t` (error code, `-1` = unanswered), `pushRead(subId, from, isolation)`, `readOnce(subId, from, isolation) → std::optional<WorkerReturn>`, `fetch(from)`, `marker(token, pid, commit)`; free functions `batchOf(recs, valueBytes, pid, epoch, seq, txn)`, `produceFrame(corr, batch, acks, pid, epoch, seq)`. The C1 gate needs nothing new here: an uncommitted topic has no replica on any worker (`kA2cTopicCommitted` builds it), so `getLogAndReplicaForPartition_` returns `{nullptr, nullptr}` and the produce is answered `NotLeader` ("not owned") — pinned by `TransientDataPathRegression.UnregisteredTransientPartitionIsNotLeaderRegression` below.

- [ ] **Step 1: Write the rig** — create `libs/node/tests/integration/TransientRig.h`:

```cpp
#pragma once
// TransientRig.h — Phase 2b E2 test rig: ONE worker (shard mode, registry-
// routed ops, worker ingress active) owning ONE TRANSIENT leader partition —
// topic 1, partition 0 — on an InMemoryLog.  Optional in-sync follower 2 that
// never fetches unless the test sends its FetchRequest (fetch()).  Test-only;
// shared by TransientDataPathTest.cpp and TransientDataPathAllocTest.cpp.
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace transient_rig {

namespace pc = rune::protocol::client;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerPool;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

constexpr int32_t kTopic = 1;

inline std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes, int64_t pid = -1, int16_t epoch = 0,
                                    int32_t seq = -1, bool txn = false) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < recs; ++i) b.append("k" + std::to_string(i), v, 1000 + i);
    return b.build();
}

/// One client Produce wire frame (what the worker's ingress delivers in shard mode).
inline std::vector<uint8_t> produceFrame(int64_t corr, const std::vector<uint8_t>& batch, uint8_t acks,
                                         int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = kTopic; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = acks; msg.producerId = pid; msg.producerEpoch = epoch;
    msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}

struct Rig {
    rune::storage::InMemoryLog                mem;          // outlives the replica (declared first)
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    WorkerPool                                pool{1, rune::metadata::ShardRouter(1)};
    PartitionReplica*                         rep  = nullptr;
    int64_t                                   now  = 1'000'000'000;
    int64_t                                   corr = 0;

    explicit Rig(std::size_t ringBytes = 4u << 20, std::size_t maxBatches = 100'000, bool withFollower = false)
        : mem(ringBytes, maxBatches) {
        PartitionConfig c;
        c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.replicaLagMaxMs = 600'000;
        c.followerFlushIntervalNs = 0;
        const std::vector<int32_t> set = withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, set, set, &mem, &transport);
        r->setExternalFollowerFlush(true);
        rep = r.get();
        reg.add(kTopic, 0, std::move(r));
        pool.setRegistry(&reg);
        Worker& w = pool.worker(0);
        w.setWorkerIngress(nullptr, /*active=*/true);
        w.setShardMode(true);
        pool.assignFromRegistry(reg);
        pass();
    }
    Worker& worker() { return pool.worker(0); }

    void pass(const std::vector<uint8_t>* frame = nullptr) {
        now += 1'000'000;
        if (frame) (void)worker().handleIngressFrameForTest(frame->data(), frame->size(), now);
        (void)worker().doWork(now);
    }

    /// One produce over the shard ingress; passes until ITS ProduceResult (bounded).
    /// Returns its error code (0 = OK, -1 = never answered); *baseOut = its base offset.
    /// Drops every other return drained meanwhile (tests keep phases separate).
    int16_t produce(const std::vector<uint8_t>& batch, uint8_t acks, int64_t* baseOut = nullptr,
                    int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
        const int64_t c = ++corr;
        const auto frame = produceFrame(c, batch, acks, pid, epoch, seq);
        pass(&frame);
        for (int i = 0; i < 64; ++i) {
            int16_t code = -1;
            pool.drainAllReturns([&](WorkerReturn r) {
                if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == c) {
                    code = r.errorCode;
                    if (baseOut != nullptr) *baseOut = r.baseOffset;
                }
            });
            if (code != -1) return code;
            pass();
        }
        return -1;
    }

    /// A gateway PushRead (legacy-mode subscription read) for the transient partition.
    void pushRead(int64_t subId, int64_t from, uint8_t isolation = 0) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead; op.topicId = kTopic; op.partition = 0;
        op.subscriptionId = subId; op.sessionId = 1; op.nextOffset = from;
        op.hwBound = INT64_MAX; op.isolation = isolation;
        op.logPtr = nullptr;   // the gateway holds no disk Log for a transient partition
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }
    /// pushRead + passes until ITS PushReadResult (bounded).
    std::optional<WorkerReturn> readOnce(int64_t subId, int64_t from, uint8_t isolation = 0) {
        pushRead(subId, from, isolation);
        for (int i = 0; i < 64; ++i) {
            pass();
            std::optional<WorkerReturn> got;
            pool.drainAllReturns([&](WorkerReturn r) {
                if (r.type == WorkerReturn::Type::PushReadResult && r.subscriptionId == subId) got = std::move(r);
            });
            if (got) return got;
        }
        return std::nullopt;
    }

    /// Follower 2's FetchRequest, as the replication transport hands it to the owning worker.
    void fetch(int64_t from) {
        WorkerOp op;
        op.type = WorkerOp::Type::FetchRequest; op.topicId = kTopic; op.partition = 0; op.replica = rep;
        op.fetchRequestPayload.leaderEpoch = 1;
        op.fetchRequestPayload.partition   = 0;
        op.fetchRequestPayload.followerId  = 2;
        op.fetchRequestPayload.fetchOffset = from;
        op.fetchRequestPayload.maxBytes    = 512 * 1024;
        op.fetchRequestPayload.topicId     = kTopic;
        op.fetchRequestPayload.nowNs       = now;
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }

    /// A transaction coordinator's control marker for the transient partition.
    void marker(int64_t token, int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kTopic; op.partition = 0;
        op.correlationId = token; op.producerId = pid;
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }
};

} // namespace transient_rig
```

- [ ] **Step 2: Write the failing tests** — create `libs/node/tests/integration/TransientDataPathTest.cpp`:

```cpp
// TransientDataPathTest.cpp — Phase 2b E2: the worker data path of a TRANSIENT
// (InMemoryLog) partition — produce over shard ingress and the op ring, push
// reads served from the ring, read_committed, the full-ring rule, the evicted
// cursor, ownership moves.  Deterministic: one worker driven by hand, no Aeron.
#include "TransientRig.h"

using namespace transient_rig;

namespace {
constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);   // 42 (Task 1)
} // namespace

TEST(TransientDataPathRegression, ShardIngressProduceAppendsToTheRingRegression) {
    Rig rig;
    const auto b = batchOf(3, 100);
    int64_t base = -1;
    ASSERT_EQ(rig.produce(b, /*acks=*/1, &base), kOk) << "a transient produce over shard ingress was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    ASSERT_EQ(rig.produce(b, 1, &base), kOk);
    EXPECT_EQ(base, 3);
    EXPECT_EQ(rig.mem.logEndOffset(), 6);
    EXPECT_EQ(rig.rep->highWatermark(), 6) << "single node: acks=all commits at once";
}

TEST(TransientDataPathRegression, ProducedBatchCarriesTheLeaderEpochRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(2, 50), /*acks=*/0), kOk);
    const auto v = rig.mem.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 0);
    EXPECT_EQ(r.partitionLeaderEpoch(), 1) << "followers build their epoch cache from the stamped epoch (KIP-320)";
    EXPECT_TRUE(r.ensureValid());
}

TEST(TransientDataPathRegression, GatewayRoutedProduceOpAppendsToTheRingRegression) {
    Rig rig;
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = kTopic; op.partition = 0;
    op.logPtr = nullptr;   // the gateway has no disk Log for a transient partition (legacy mode)
    op.sessionId = 1; op.correlationId = 77;
    op.lastOffsetDelta = 2; op.recordCount = 3; op.durabilityTier = 1;
    op.recordBatchBytes = batchOf(3, 100);
    ASSERT_TRUE(rig.pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == 77) { code = r.errorCode; base = r.baseOffset; }
        });
    }
    ASSERT_EQ(code, kOk) << "a gateway-routed (legacy mode) transient produce was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    EXPECT_EQ(rig.mem.logEndOffset(), 3);
}

TEST(TransientDataPathRegression, FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression) {
    Rig rig(/*ringBytes=*/16 * 1024, /*maxBatches=*/1000, /*withFollower=*/true);   // ISR {1,2}: HW waits for 2
    const auto b = batchOf(1, 1000);
    int ok = 0;
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        code = rig.produce(b, /*acks=*/0);
        if (code == kOk) ++ok;
    }
    ASSERT_EQ(code, kTransientLogFull) << "a full ring must refuse (42, retriable), not evict unreplicated data";
    EXPECT_GT(ok, 0);
    EXPECT_EQ(rig.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(rig.mem.fullRefusals(), 1u);
    EXPECT_GE(rig.worker().transientFullRefusalsForTest(), 1u);
    const int64_t leo = rig.mem.logEndOffset();
    EXPECT_EQ(leo, ok) << "a refused produce appended nothing";

    rig.fetch(leo);   // follower 2 now holds everything: the HW reaches the LEO
    for (int i = 0; i < 8; ++i) rig.pass();
    ASSERT_EQ(rig.rep->highWatermark(), leo);
    EXPECT_EQ(rig.produce(b, 0), kOk) << "committed records make room";
    EXPECT_GT(rig.mem.logStartOffset(), 0);
}

TEST(TransientDataPathRegression, FullRingMarkerIsRefusedRetriablyRegression) {
    Rig rig(16 * 1024, 1000, /*withFollower=*/true);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) code = rig.produce(batchOf(1, 1000), 0);
    ASSERT_EQ(code, kTransientLogFull) << "precondition: the ring is full of uncommitted data";
    rig.marker(/*token=*/5, /*pid=*/9, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 5) mcode = r.errorCode;
        });
    }
    EXPECT_EQ(mcode, kTransientLogFull) << "the coordinator must retry a refused marker (was: Internal — dropped)";
}

// C1 at the worker: a transient topic whose catalog record has not committed has
// NO replica on any worker (BrokerNode builds it on kA2cTopicCommitted), so a
// shard-ingress produce for it is "not owned" — retriable NotLeader, nothing
// appended anywhere.  Pins that the transient branch (replica->memoryLog())
// never turns "no replica" into an append.
TEST(TransientDataPathRegression, UnregisteredTransientPartitionIsNotLeaderRegression) {
    Rig rig;
    rig.worker().setOwnedDirect({});   // the partition is not (yet) a replica of this worker
    const auto b = batchOf(1, 50);
    EXPECT_EQ(rig.produce(b, /*acks=*/1), kNotLeader);
    EXPECT_EQ(rig.mem.logEndOffset(), 0) << "an uncommitted / unowned transient partition was appended to";
}
```

In `libs/node/CMakeLists.txt`, right after the `gtest_discover_tests(rune_hotpath_b_alloc_regression_tests)` line add:

```cmake
    # Phase 2b E2: the worker data path of TRANSIENT (InMemoryLog) partitions —
    # produce, push reads, read_committed, full ring, evicted cursors.  No Aeron.
    add_executable(rune_transient_data_path_tests
        tests/integration/TransientDataPathTest.cpp)
    target_link_libraries(rune_transient_data_path_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_data_path_tests)
```

- [ ] **Step 3: Run to verify they fail**

Run: `cmake --build build-fast -j14 --target rune_transient_data_path_tests`
Expected: compile error `no member named 'transientFullRefusalsForTest' in 'rune::node::Worker'`. With only the accessors added: `ShardIngressProduce…`, `ProducedBatchCarries…`, `GatewayRoutedProduceOp…` FAIL with errorCode 6 (`Internal`: "owned but transient (in-memory) log"), the full-ring tests FAIL the same way; `UnregisteredTransientPartitionIsNotLeader…` PASSES today (it pins the C1 rule the change must keep).

- [ ] **Step 4: Implement — `Worker.h`**

Accessors: right after `[[nodiscard]] std::size_t deferredPushReadsForTest() const noexcept { return deferredPushCount_; }` add:

```cpp
    /// Phase 2b E2 (TRANSIENT partitions): produces/markers the ring refused;
    /// reads whose cursor was below the ring start, and the records they skipped.
    [[nodiscard]] uint64_t transientFullRefusalsForTest() const noexcept { return transientFullRefusals_; }
    [[nodiscard]] uint64_t transientCursorSkipsForTest() const noexcept { return transientCursorSkips_; }
    [[nodiscard]] uint64_t transientRecordsSkippedForTest() const noexcept { return transientRecordsSkipped_; }
```

Fields: right after the `std::vector<uint8_t>                  pushScratch_ = …;` line add:

```cpp
    uint64_t transientFullRefusals_   = 0;   ///< E2: ring refused a produce / marker (full or too large)
    uint64_t transientCursorSkips_    = 0;   ///< E2: reads whose cursor was below the ring start
    uint64_t transientRecordsSkipped_ = 0;   ///< E2: records those reads skipped (evicted unread)
```

Right before `[[gnu::cold]] void notePushReadOversize_(` add:

```cpp
    // Phase 2b E2: a TRANSIENT ring refused an append (full of unreplicated
    // records, or a batch larger than the ring).  Counted; logged first + every 1024th.
    [[gnu::cold]] void noteTransientFull_(int32_t topicId, int32_t partition,
                                          rune::protocol::client::ErrorCode ec) noexcept {
        ++transientFullRefusals_;
        if (transientFullRefusals_ == 1 || (transientFullRefusals_ & 1023u) == 0)
            spdlog::warn("Worker {}: transient topicId={} partition={}: ring refused an append ({}) — {} "
                         "(total={}); raise RUNE_TRANSIENT_LOG_RING_BYTES or let the followers catch up",
                         workerId_, topicId, partition, static_cast<int>(ec),
                         ec == rune::protocol::client::ErrorCode::MessageTooLarge
                             ? "batch larger than the ring" : "ring full of unreplicated records",
                         transientFullRefusals_);
    }
```

`produceCore_`: replace the whole `if (!log) { … }` block — `if (!log) {` whose first line is the comment `// E5.1.5: distinguish "partition not owned by this worker" (retriable`, ending with `enqueueReturn_(ret);` / `return 1;` / `}` right before the comment `// F15 §3.1 (I1): the owning worker is the role authority` — with:

```cpp
        // Phase 2b E2: a TRANSIENT partition (owned replica, no disk Log) appends
        // to the replica's own InMemoryLog through this same produce core.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && replica != nullptr) ? replica->memoryLog() : nullptr;
        if (log == nullptr && mem == nullptr) {
            // Not owned by this worker (getLogAndReplicaForPartition_ → {nullptr,
            // nullptr}): retriable NotLeader so the client refreshes metadata and
            // re-routes.  Shared-nothing: no cross-worker forwarding per
            // [[rune-shared-nothing-law]].
            spdlog::debug("Worker {}: produce for a partition this worker does not own (topicId={} "
                          "partition={}); returning NotLeader (retriable)",
                          workerId_, f.topicId, f.partition);
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return 1;
        }
```

Replace `        const bool asyncStrict = durabilityTier == 2 && log->isDiskBacked();` with:

```cpp
        // Phase 2b E2: a transient ring has no fd — tier=strict acks as Majority.
        const bool asyncStrict = durabilityTier == 2 && log != nullptr && log->isDiskBacked();
```

Replace

```cpp
        // Call the shared produce helper.
        rune::storage::ProduceOutcome outcome = rune::storage::handleProduceBatch(
            rbSpan.data(), rbSpan.size(),
            f.topicId, f.partition,
            f.producerId, f.producerEpoch, f.baseSequence,
            *log, psm_,
            acksAll, isrSize, minIsr, helperTier,
            // M-3: skip the repeat CRC pass ONLY when the op's producer says it
            // validated these exact bytes (the gateway, right after ensureValid).
            /*crcPreverified=*/f.fromOp && f.crcValidated);
```
with
```cpp
        // Call the shared produce helper — over the disk Log, or (Phase 2b E2) a
        // TRANSIENT partition's ring: one memcpy into the ring, base offset +
        // leader epoch patched there; the ring never evicts at/above the HW
        // (prepareTransientAppend) — full → TransientLogFull (42, retriable),
        // nothing appended.
        // M-3: skip the repeat CRC pass ONLY when the op's producer says it
        // validated these exact bytes (the gateway, right after ensureValid).
        const bool crcPreverified = f.fromOp && f.crcValidated;
        rune::storage::ProduceOutcome outcome;
        if (log != nullptr) {
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *log, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
        } else {
            replica->prepareTransientAppend();
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *mem, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
            if (outcome.logFull) [[unlikely]] noteTransientFull_(f.topicId, f.partition, outcome.error);
        }
```

`executeAppendControlRecordWire_`: replace

```cpp
            } else if (base == RepT::kAppendMinIsr) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — min-ISR gate refused the marker "
                             "topicId={} partition={}", workerId_, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
            } else {
```
with
```cpp
            } else if (base == RepT::kAppendMinIsr) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — min-ISR gate refused the marker "
                             "topicId={} partition={}", workerId_, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
            } else if (base == RepT::kAppendBackPressure) {
                // Phase 2b E2: a TRANSIENT ring full of unreplicated records —
                // retriable: the coordinator re-sends the marker (never dropped;
                // TransactionCoordinationService::onControlMarkerRefused retries
                // every code but Unknown{Topic,Partition} and the fence).
                noteTransientFull_(topicId, partition, pc::ErrorCode::TransientLogFull);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);
            } else {
```

`getLogAndReplicaForPartition_`: replace the comment block

```cpp
                // Only disk-backed replicas expose a rune::storage::Log*.
                // Memory-backed (InMemoryLog) replicas are NOT supported on the
                // WorkerIngress produce path — the gateway fanout handles them
                // via the existing WorkerOp ring (which carries a logPtr).
                // Return nullptr for memory-backed so the caller sends an
                // Internal error rather than crash.
```
with
```cpp
                // Only disk-backed replicas expose a rune::storage::Log*.  A
                // TRANSIENT (InMemoryLog) replica returns {nullptr, replica}:
                // produceCore_ appends to replica->memoryLog() (Phase 2b E2).
```

- [ ] **Step 5: Run to verify they pass**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'TransientDataPath|Worker|ProducePath|F15|HotPathB|ShardNonBlocking|TxnCoordination|TransactionCoordination' --output-on-failure -j14`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/integration/TransientRig.h libs/node/tests/integration/TransientDataPathTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(worker): produce to transient partitions (shard ingress + op ring); full ring refuses retriably

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---


### Task 5: Worker — push reads and owned-subscription pushes from the ring; evicted cursor; ownership moves

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` — `pushOwnedSubscriptions_` (~`:2794-3030`; the `if (!s.logPtr || !s.active)` guard ~`:2809`, the bound ~`:2858-2861`, the read ~`:2876-2877`), `servePushRead_` (~`:3231-3345`), `readPushChunk_` (~`:3353-3366`), `beginPushPass_` (~`:3452-3475`), new `skipEvictedCursor_` before `readNoFault_` (~`:3492`), `serveHeldPushReads_` (~`:5080-5092`)
- Test: `libs/node/tests/integration/TransientDataPathTest.cpp` (append)

**Interfaces:**
- Consumes: Task 4's counters/accessors and rig.
- Produces: `int64_t Worker::skipEvictedCursor_(const rune::storage::InMemoryLog& mem, int64_t cursor) noexcept` (private; returns `max(cursor, mem.logStartOffset())`, counting a skip). `PushReadArgs` whose partition moved away now carries `replica == nullptr` (never only `logPtr == nullptr`).

- [ ] **Step 1: Write the failing tests** — append to `TransientDataPathTest.cpp`:

```cpp
TEST(TransientDataPathRegression, PushReadServesTheRingRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    const auto r = rig.readOnce(/*subId=*/7, /*from=*/0);
    ASSERT_TRUE(r.has_value()) << "no PushReadResult";
    ASSERT_FALSE(r->chunk.empty()) << "a transient PushRead was left unanswered (no disk Log)";
    EXPECT_EQ(r->pushEffectiveBound(), 6);
    EXPECT_EQ(r->newNextOffset, 6);
    std::size_t pos = 0;
    int64_t expect = 0;
    while (pos < r->chunk.size()) {
        rune::protocol::RecordBatchReader b(r->chunk.data() + pos, r->chunk.size() - pos);
        ASSERT_GT(b.totalLength(), 0);
        EXPECT_EQ(b.baseOffset(), expect);
        expect = b.baseOffset() + b.lastOffsetDelta() + 1;
        pos += static_cast<std::size_t>(b.totalLength());
    }
    EXPECT_EQ(expect, 6) << "whole contiguous batches from the cursor";
}

TEST(TransientDataPathRegression, PushReadBelowRingStartResumesAtRingStartRegression) {
    Rig rig(/*ringBytes=*/8 * 1024, /*maxBatches=*/1000);
    for (int i = 0; i < 20; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1000), 0), kOk);
    const int64_t start = rig.mem.logStartOffset();
    ASSERT_GT(start, 0) << "precondition: the ring evicted committed records (retention)";
    const auto r = rig.readOnce(9, /*from=*/0);   // a consumer that fell behind the ring
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->chunk.empty()) << "a cursor below the ring start stalled";
    EXPECT_EQ(rune::protocol::RecordBatchReader(r->chunk.data(), r->chunk.size()).baseOffset(), start);
    EXPECT_EQ(rig.worker().transientCursorSkipsForTest(), 1u);
    EXPECT_EQ(rig.worker().transientRecordsSkippedForTest(), static_cast<uint64_t>(start));
}

TEST(TransientDataPathRegression, ReadCommittedHoldsTheOpenTransactionUntilItsCommitRegression) {
    Rig rig;
    constexpr int64_t kPid = 7;
    const auto txn = batchOf(1, 50, kPid, /*epoch=*/0, /*seq=*/0, /*txn=*/true);
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code != kOk; ++i)   // NotLeader while the RC state builds (retriable)
        code = rig.produce(txn, /*acks=*/0, &base, kPid, 0, 0);
    ASSERT_EQ(code, kOk);
    ASSERT_EQ(base, 0);

    const auto open = rig.readOnce(21, 0, /*read_committed=*/1);
    ASSERT_TRUE(open.has_value());
    EXPECT_TRUE(open->chunk.empty()) << "an OPEN transaction leaked to read_committed";
    EXPECT_EQ(open->pushEffectiveBound(), 0) << "the LSO pins at the open transaction's first offset";

    rig.marker(/*token=*/3, kPid, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 3) mcode = r.errorCode;
        });
    }
    ASSERT_EQ(mcode, kOk);

    const auto committed = rig.readOnce(22, 0, 1);
    ASSERT_TRUE(committed.has_value());
    ASSERT_FALSE(committed->chunk.empty()) << "the committed transaction was never delivered";
    rune::protocol::RecordBatchReader b(committed->chunk.data(), committed->chunk.size());
    EXPECT_EQ(b.baseOffset(), 0);
    EXPECT_EQ(static_cast<std::size_t>(b.totalLength()), committed->chunk.size()) << "a marker was delivered";
    EXPECT_EQ(committed->newNextOffset, 2) << "the cursor passes the marker";
}

TEST(TransientDataPathRegression, DeferredPushReadAfterOwnershipMovedNeverReadsTheRingRegression) {
    Rig rig;   // 4 MiB ring
    const auto b = batchOf(4, 1000);
    for (int i = 0; i < 300; ++i) ASSERT_EQ(rig.produce(b, 0), kOk);   // ~1.2 MiB: several passes of push budget
    for (int s = 0; s < 4; ++s) rig.pushRead(200 + s, 0);
    rig.pass();
    ASSERT_GT(rig.worker().deferredPushReadsForTest(), 0u) << "precondition: reads deferred";
    int answeredWithData = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::PushReadResult && !r.chunk.empty()) ++answeredWithData;
    });
    rig.worker().setOwnedDirect({});   // the partition moved to another worker
    for (int k = 0; k < 4; ++k) rig.pass();
    int unanswered = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::PushReadResult) return;
        EXPECT_TRUE(r.chunk.empty()) << "read a ring this worker no longer owns (a cross-thread read)";
        EXPECT_EQ(r.pushEffectiveBound(), -1);
        ++unanswered;
    });
    EXPECT_EQ(answeredWithData + unanswered, 4);
    EXPECT_EQ(rig.worker().deferredPushReadsForTest(), 0u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j14 --target rune_transient_data_path_tests && ctest --test-dir build-fast -R 'TransientDataPathRegression\.(PushRead|ReadCommitted|DeferredPushRead)' --output-on-failure -j14`
Expected: `PushReadServesTheRing` FAILS ("left unanswered": no `Log*` → bound -1), `PushReadBelowRingStart…` FAILS (empty chunk), `ReadCommitted…` FAILS (committed read empty), `DeferredPushRead…` PASSES today (reads are unanswered for every transient partition) — it pins the ownership rule the next step must keep.

- [ ] **Step 3: Implement — `Worker.h`**

Add before `rune::storage::LogReadView readNoFault_(rune::storage::Log* log, int64_t offset, std::size_t maxBytes,`:

```cpp
    // Phase 2b E2 — TRANSIENT_EVICTED: the ring evicted committed records this
    // reader had not read yet (the in-memory retention).  The cursor resumes AT
    // the ring start — counted, logged first + every power of two — never a
    // stall on an offset the ring no longer holds.  Disk logs never take this.
    [[nodiscard]] int64_t skipEvictedCursor_(const rune::storage::InMemoryLog& mem, int64_t cursor) noexcept {
        const int64_t start = mem.logStartOffset();
        if (cursor >= start) [[likely]] return cursor;
        ++transientCursorSkips_;
        transientRecordsSkipped_ += static_cast<uint64_t>(start - cursor);
        if ((transientCursorSkips_ & (transientCursorSkips_ - 1)) == 0)
            spdlog::warn("Worker {}: a reader of a transient partition fell behind its ring: cursor {} < ring "
                         "start {} — resumed at {} (skips={}, records skipped={})", workerId_, cursor, start,
                         start, transientCursorSkips_, transientRecordsSkipped_);
        return start;
    }
```

`servePushRead_`: replace from `    void servePushRead_(const PushReadArgs& a, bool mayHold) {` through the end of the

```cpp
        rune::storage::Log* log = a.logPtr;
        if (!log) {
            // No log: an UNANSWERED read (review I-1) — the gateway re-issues it
            // and never parks the subscription on it.
            ret.setPushEffectiveBound(-1);
            enqueueReturn_(ret);
            return;
        }
```
block with:

```cpp
    void servePushRead_(const PushReadArgs& in, bool mayHold) {
        PushReadArgs a = in;   // Phase 2b E2: the cursor may move up (TRANSIENT_EVICTED) — a stack copy
        rune::storage::Log* log = a.logPtr;
        // Phase 2b E2: a TRANSIENT partition has no disk Log — its source is the
        // owning replica's InMemoryLog.  a.replica is THIS worker's replica here:
        // PushRead is ownership-gated, and a deferred / held read whose partition
        // moved away is re-served with replica == nullptr (beginPushPass_,
        // serveHeldPushReads_) — never a ring this worker does not own.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && a.replica != nullptr) ? a.replica->memoryLog() : nullptr;
        if (mem != nullptr) a.nextOffset = skipEvictedCursor_(*mem, a.nextOffset);
        const int32_t topicId    = a.topicId;
        const int32_t partition  = a.partition;
        const int64_t nextOffset = a.nextOffset;
        const int64_t hwBound    = a.hwBound;
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = a.subscriptionId;
        ret.sessionId            = a.sessionId; // gateway needs this to find the session
        ret.readTopicId          = topicId;
        ret.readPartition        = partition;
        ret.newNextOffset        = nextOffset; // will be advanced by gateway on success
        ret.hwBound              = hwBound;
        ret.isolation            = a.isolation;
        ret.isGroup              = a.isGroup;
        ret.groupCarrierReceived = a.carrier;
        ret.setPushEffectiveBound(-1);   // N5: unanswered unless a path below answers explicitly

        if (!log && !mem) {
            // No source: an UNANSWERED read (review I-1) — the gateway re-issues it
            // and never parks the subscription on it.
            ret.setPushEffectiveBound(-1);
            enqueueReturn_(ret);
            return;
        }
```

(The rest of `servePushRead_` keeps using `a`, `nextOffset`, `log` — now the clamped copy; a held / deferred read stores the clamped cursor, so a skip is counted once.) In it, replace `            switch (readPushChunk_(a, ret, log, bound, rc)) {` with `            switch (readPushChunk_(a, ret, log, mem, bound, rc)) {`.

`readPushChunk_`: replace its signature line

```cpp
    PushChunk readPushChunk_(const PushReadArgs& a, WorkerReturn& ret, rune::storage::Log* log, int64_t bound,
                             const RcState* rc) {
```
with
```cpp
    PushChunk readPushChunk_(const PushReadArgs& a, WorkerReturn& ret, rune::storage::Log* log,
                             rune::storage::InMemoryLog* mem, int64_t bound, const RcState* rc) {
```
and replace `        const rune::storage::LogReadView v = readNoFault_(log, a.nextOffset, cap, bound);` with:

```cpp
        // Phase 2b E2: a TRANSIENT partition reads a zero-copy view of its ring
        // (same thread, no syscall); the bound clamp below applies unchanged.
        const rune::storage::LogReadView v =
            log != nullptr ? readNoFault_(log, a.nextOffset, cap, bound)
                           : mem->readView(a.nextOffset, static_cast<int32_t>(cap));
```

`beginPushPass_`: replace

```cpp
            } else {
                a.logPtr = nullptr;   // never read a log this worker no longer owns → unanswered
            }
```
with
```cpp
            } else {
                // Never read a log — or (E2) a transient ring, found through the
                // replica — this worker no longer owns → unanswered.
                a.logPtr  = nullptr;
                a.replica = nullptr;
            }
```

`serveHeldPushReads_`: replace

```cpp
            } else {
                a.logPtr = nullptr;   // never read a log this worker no longer owns
                servePushRead_(a, false);   // → unanswered: the gateway re-issues to the new owner
            }
```
with
```cpp
            } else {
                a.logPtr  = nullptr;   // never read a log this worker no longer owns
                a.replica = nullptr;   // E2: nor its ring (a transient read finds it through the replica)
                servePushRead_(a, false);   // → unanswered: the gateway re-issues to the new owner
            }
```

`pushOwnedSubscriptions_` (shard mode — the worker pushes on its own egress): replace
`            if (!s.logPtr || !s.active) continue;   // parked: partition moved away` with
`            if (!s.active) continue;   // parked: partition moved away (E2: the source is resolved below)`.

Replace

```cpp
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            int64_t bound = rep ? rep->highWatermark()
                                : s.logPtr->logEndOffset();
```
with
```cpp
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            // Phase 2b E2: a TRANSIENT partition's source is the owned replica's
            // ring (the Subscribe op carried no disk Log); no source → nothing to push.
            rune::storage::InMemoryLog* const mem =
                (s.logPtr == nullptr && rep != nullptr) ? rep->memoryLog() : nullptr;
            if (s.logPtr == nullptr && mem == nullptr) continue;
            if (mem != nullptr) s.nextOffset = skipEvictedCursor_(*mem, s.nextOffset);   // TRANSIENT_EVICTED
            int64_t bound = rep ? rep->highWatermark()
                                : s.logPtr->logEndOffset();
```

Replace

```cpp
            const rune::storage::LogReadView view = readNoFault_(
                s.logPtr, s.nextOffset, std::min<std::size_t>(kPushMaxChunkBytes, pushBudgetLeft_), bound);
```
with
```cpp
            const std::size_t readCap = std::min<std::size_t>(kPushMaxChunkBytes, pushBudgetLeft_);
            const rune::storage::LogReadView view =
                s.logPtr != nullptr ? readNoFault_(s.logPtr, s.nextOffset, readCap, bound)
                                    : mem->readView(s.nextOffset, static_cast<int32_t>(readCap));   // E2: zero-copy ring view
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'TransientDataPath|HotPathB|Worker|SubscribeDedupe|GroupPush|ReadCommittedPush|GatewaySubscribe|ShardMode|ViewFold|WorkerViewSet' --output-on-failure -j14`
Expected: all PASS (`WorkerPoolTest`'s InMemoryLog PushReads with `hwBound = 0` are now answered at bound 0 instead of unanswered — that test only counts results).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/integration/TransientDataPathTest.cpp
git commit -m "feat(worker): push reads + owned-subscription pushes from transient rings; evicted cursors resume at the ring start

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---


### Task 6: Zero-allocation regression + produce→subscribe latency benchmark (transient vs disk)

**Files:**
- Create: `libs/node/tests/integration/TransientDataPathAllocTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target after `rune_transient_data_path_tests`)
- Modify: `benchmark/WorkerProduceLatencyBench.cpp` (fixture + runner inside the anonymous namespace, before `} // namespace`; registrations at the end)

**Interfaces:**
- Consumes: `transient_rig::Rig` (Task 4), the Task 4/5 worker paths.
- Produces: benchmarks `BM_WorkerProduceToPushRead_{Disk,Transient}_{256B,1KiB}` with counters `p50Us`, `p99Us`.

- [ ] **Step 1: Write the allocation regression** — create `libs/node/tests/integration/TransientDataPathAllocTest.cpp`:

```cpp
// TransientDataPathAllocTest.cpp — Phase 2b E2: a steady-state worker pass on a
// TRANSIENT partition — produce intake (the ring wraps and evicts), push reads
// served from the ring — never allocates.  Replaces the global operator new
// (binary-wide), so it lives in its own binary (rune_transient_alloc_regression_tests).
#include "TransientRig.h"

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <new>

// Heap allocations made by THIS thread while armed; the first few print a
// backtrace so the culprit is named.
thread_local bool     gAllocArmed  = false;
thread_local uint64_t gArmedAllocs = 0;
void* operator new(std::size_t n) {
    if (gAllocArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {
            gAllocArmed = false;   // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "E2 transient hot-path allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gAllocArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace transient_rig;

TEST(TransientDataPathRegression, SteadyStateProduceEvictAndPushReadNeverAllocateRegression) {
    Rig rig(/*ringBytes=*/256u << 10, /*maxBatches=*/100'000);
    const auto batch = batchOf(4, 200);
    const auto frame = produceFrame(1, batch, /*acks=*/0);
    int64_t next = 0;
    int64_t sub  = 1;
    auto drainReads = [&] {   // test side, disarmed (the copy-mode drain allocates the chunk)
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::PushReadResult && r.newNextOffset > next) next = r.newNextOffset;
        });
    };
    // Warm-up: every container reaches its steady size; the ring wraps and evicts many times.
    for (int i = 0; i < 2048; ++i) {
        rig.pass(&frame);
        if ((i & 3) == 3) { rig.pushRead(sub++, next); rig.pass(); }
        drainReads();
    }
    ASSERT_GT(rig.mem.logStartOffset(), 0) << "precondition: the ring evicts in steady state";
    ASSERT_GT(next, 0) << "precondition: the reads are answered from the ring";
    gArmedAllocs = 0;
    for (int i = 0; i < 512; ++i) {
        if ((i & 3) == 0) rig.pushRead(sub++, next);   // enqueue: test side, disarmed
        rig.now += 1'000'000;
        gAllocArmed = true;
        (void)rig.worker().handleIngressFrameForTest(frame.data(), frame.size(), rig.now);
        (void)rig.worker().doWork(rig.now);
        gAllocArmed = false;
        drainReads();
    }
    EXPECT_EQ(gArmedAllocs, 0u) << "a steady-state transient pass allocated (see the backtraces above)";
}
```

In `libs/node/CMakeLists.txt`, after `gtest_discover_tests(rune_transient_data_path_tests)` add:

```cmake
    # Phase 2b E2: replaces the global operator new (binary-wide) — own binary.
    add_executable(rune_transient_alloc_regression_tests
        tests/integration/TransientDataPathAllocTest.cpp)
    target_link_libraries(rune_transient_alloc_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_alloc_regression_tests)
```

- [ ] **Step 2: Run it**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 --target rune_transient_alloc_regression_tests && ctest --test-dir build-fast -R 'SteadyStateProduceEvictAndPushRead' --output-on-failure -j14`
Expected: PASS (Tasks 2-5 were written allocation-free: restamp append = one memcpy into the preallocated ring, `readView` = a pointer view, eviction = an index move). If it FAILS, the printed backtrace names the allocation — fix it in the named function (HARD RULE 4), never by loosening the test.

- [ ] **Step 3: Write the benchmark** — in `benchmark/WorkerProduceLatencyBench.cpp`, insert before the closing `} // namespace` of the anonymous namespace (right after `BM_WorkerRoleGate_Baseline`):

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2: produce → subscribe latency, TRANSIENT vs DISK, on the REAL
// worker path: a Produce op (append + ack) then a PushRead op at the produced
// offset (the read a subscription's next push cycle makes) → the
// PushReadResult carrying the batch.  One sample = "a produced record reaches
// a subscriber" on one worker.  Gate (E2): transient p50 and p99 <= disk's.
// ---------------------------------------------------------------------------
struct TransientWorkerFixture {
    rune::storage::InMemoryLog        mem{64u << 20, 1'000'000};
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker>           worker;

    TransientWorkerFixture() {
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(
            cfg, rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, &mem, &transport);
        worker = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }
};

template <class Fix>
static void runProduceToPushRead(benchmark::State& state, Fix& fix, Log* diskLog, std::size_t payload) {
    std::vector<int64_t> samples;
    samples.reserve(1 << 16);
    int64_t nowNs = 1, corr = 0, sub = 0;
    const auto proto = makeBatch(payload);
    for (auto _ : state) {
        WorkerOp prod;
        prod.type = WorkerOp::Type::Produce;
        prod.topicId = 1; prod.partition = 0;
        prod.replica = fix.replica.get(); prod.logPtr = diskLog;   // transient: nullptr
        prod.sessionId = 1; prod.correlationId = ++corr;
        prod.lastOffsetDelta = 0; prod.recordCount = 1; prod.durabilityTier = 1;
        prod.recordBatchBytes = proto;
        WorkerOp read;
        read.type = WorkerOp::Type::PushRead;
        read.topicId = 1; read.partition = 0;
        read.replica = fix.replica.get(); read.logPtr = diskLog;
        read.subscriptionId = ++sub; read.sessionId = 1;
        read.hwBound = INT64_MAX; read.isolation = 0;

        const auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(prod)) fix.worker->doWork(nowNs++);
        int64_t base = -1;
        for (int i = 0; i < 5'000'000 && base < 0; ++i) {
            fix.worker->doWork(nowNs++);
            fix.worker->drainReturn([&](WorkerReturn&& r) {
                if (r.type == WorkerReturn::Type::ProduceResult && r.errorCode == 0) base = r.baseOffset;
            });
        }
        read.nextOffset = base;
        while (!fix.worker->tryEnqueueOp(read)) fix.worker->doWork(nowNs++);
        bool delivered = false;
        for (int i = 0; i < 5'000'000 && !delivered; ++i) {
            fix.worker->doWork(nowNs++);
            fix.worker->drainReturn([&](WorkerReturn&& r) {
                if (r.type == WorkerReturn::Type::PushReadResult && !r.chunk.empty()) delivered = true;
            });
        }
        const auto t1 = std::chrono::steady_clock::now();
        if (!delivered) { state.SkipWithError("the produced record never reached the PushRead"); break; }
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
}

static void BM_WorkerProduceToPushRead_Disk_256B(benchmark::State& s) {
    RealWorkerFixture f; runProduceToPushRead(s, f, f.log.get(), 256);
}
static void BM_WorkerProduceToPushRead_Transient_256B(benchmark::State& s) {
    TransientWorkerFixture f; runProduceToPushRead(s, f, nullptr, 256);
}
static void BM_WorkerProduceToPushRead_Disk_1KiB(benchmark::State& s) {
    RealWorkerFixture f; runProduceToPushRead(s, f, f.log.get(), 1024);
}
static void BM_WorkerProduceToPushRead_Transient_1KiB(benchmark::State& s) {
    TransientWorkerFixture f; runProduceToPushRead(s, f, nullptr, 1024);
}
```

and at the end of the file (after the last `BENCHMARK(…)` line):

```cpp
BENCHMARK(BM_WorkerProduceToPushRead_Disk_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Transient_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Disk_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Transient_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
```

- [ ] **Step 4: Run the benchmark (gate)**

Run: `cmake --build build-bench-rel -j14 --target rune_broker_benchmark && $(find build-bench-rel -name rune_broker_benchmark -type f | head -1) --benchmark_filter='WorkerProduceToPushRead|WorkerProduce_Majority' --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`
Expected: all four `WorkerProduceToPushRead` benchmarks complete without `SkipWithError`; for each payload, **Transient `p50Us` and `p99Us` ≤ Disk's** (the transient path has no pwrite/pread syscall). `BM_WorkerProduce_Majority_*` within noise of the pre-E2 base (run it once on the base commit first). Record the four p50/p99 pairs and the Majority numbers in the commit body. If transient is slower than disk, stop and profile — that is a gate failure, not a tolerance to widen.

- [ ] **Step 5: Gate + commit**

Run: `ctest --test-dir build-fast -R 'TransientDataPath|SteadyStateProduceEvict' --output-on-failure -j14`
Expected: all PASS.

Commit (replace the text of the second `-m` with the measured Step 4 lines, e.g. `Transient_256B p50=… p99=… | Disk_256B p50=… p99=…`):

```bash
git add libs/node/tests/integration/TransientDataPathAllocTest.cpp libs/node/CMakeLists.txt benchmark/WorkerProduceLatencyBench.cpp
git commit -m "test(worker): transient steady state allocates nothing; bench produce->subscribe transient vs disk" \
  -m "Bench (Step 4, p50Us/p99Us): one line per BM_WorkerProduceToPushRead_* and BM_WorkerProduce_Majority_* (before/after)." \
  -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller):** full broker `*Regression*` suite (Tasks 2-6 landed the worker data path).

---

### Task 7: Gateway — serve transient partitions through their owning worker (C1-gated)

Today every gateway data-plane site resolves `topicRegistry_->servableLog(...)` (C1, `5ece3ee0`) and treats nullptr as "awaiting commit → NotLeader" or else "no partition → UnknownPartition / -1". A COMMITTED transient topic has no disk `Log`, so it lands in the second branch: produce/subscribe answer `UnknownPartition (8)` (the DDL final-fix concern for the legacy runtime), group subscriptions return -1, local markers are dropped (-1) and remote markers answered NotLeader forever. This task gives each site one explicit partition kind.

**Files:**
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h` (declaration right after `routesToWorker_`, ~`:2040`)
- Modify: `libs/gateway/src/ClientGateway.cpp` — new `servedPartition_` before `bool ClientGateway::handleProduce(` (~`:2970`); `handleProduce` (the C1 block ~`:3156-3171`, and before the INLINE path comment ~`:3518`); `handleSubscribe` (the C1 block ~`:4178-4192`, `leo` ~`:4215`, `logStart` ~`:4232`); `pushSubscriptions` (~`:4496-4512`); `appendRemoteControlRecord` (~`:5172-5176`, `:5230`); `addGroupSubscription` (~`:6527-6537`); `appendControlRecord` (~`:6703-6708`, last statement ~`:6783`)
- Create: `libs/gateway/tests/gateway/TransientGatewayTest.cpp`
- Modify: `libs/gateway/CMakeLists.txt` (new target after `gtest_discover_tests(rune_gateway_topic_servability_tests)`, ~`:218`)

**Interfaces:**
- Consumes: Part E's `rune::node::TopicSpec::persistence` / `rune::node::PersistenceMode::Transient`; C1's `TopicRegistry::servableLog / awaitingCommit / commitTopic`; `PartitionReplica::gatewayHighWatermark()` (existing acquire-load); `ErrorCode::TransientLogFull` + `produceErrorForClient` (Task 1); the worker paths of Tasks 4-5.
- Produces (private, `rune::gateway::ClientGateway`):
  ```cpp
  struct ServedPartition {
      enum class Kind : uint8_t { Unknown, AwaitingCommit, Disk, Transient };
      Kind                kind = Kind::Unknown;
      rune::storage::Log* log  = nullptr;   // Kind::Disk only
  };
  [[nodiscard]] ServedPartition servedPartition_(int32_t topicId, int32_t partition) const noexcept;
  ```
  One state per case (CLAUDE.md rule 3), C1 first: `AwaitingCommit` (the topic's catalog record has not committed — retriable NotLeader exactly as today), `Disk` (its servable `Log`), `Transient` (committed, `persistence=transient`, in range, replica registered — no disk `Log`; only its owning worker touches the ring; its published HW is the subscribe clamp), `Unknown`. The disk fast path is the same single `servableLog()` probe as today. The gateway never reads a ring: a transient Earliest resolves to 0 and the owning worker skips the cursor up to its ring start (Task 5).

- [ ] **Step 1: Write the failing tests** — create `libs/gateway/tests/gateway/TransientGatewayTest.cpp`:

```cpp
// TransientGatewayTest.cpp — Phase 2b E2: the gateway serves a TRANSIENT
// partition (no disk Log) through its owning worker — produce routed to the
// worker, Subscribe accepted and clamped to the HW snapshot, PushReads issued
// (legacy worker-pool mode), group subscriptions, txn markers routed — gates it
// by C1 exactly like a disk topic (uncommitted → NotLeader, nothing served),
// answers a full ring TransientLogFull (42) to v6 sessions and NotLeader to
// older ones, and refuses it (UnknownPartition) without a worker pool: no
// gateway-inline path may touch a worker-owned ring.  Deterministic: no Aeron.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-transient-gw-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes = 1) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < recs; ++i) b.append("k", v, 1000 + i);
    return b.build();
}

struct RigOpts {
    bool        workers      = true;      // a 1-worker pool (legacy mode: the gateway issues PushReads)
    bool        runtime      = false;     // create the topic at RUNTIME: awaiting its commit (C1) until commit()
    std::size_t ringBytes    = 1u << 20;
    bool        withFollower = false;     // ISR {1,2}: follower 2 never fetches → the HW stays 0
};

// Gateway + topic 0 "presence" (1 partition, TRANSIENT) + its leader replica on
// an InMemoryLog (built at construction, or — runtime — only on commit(), the
// way BrokerNode builds it on kA2cTopicCommitted).
struct Rig {
    TempDir                                   dir;
    RigOpts                                   o;
    rune::storage::InMemoryLog                mem;           // outlives the replica
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    std::unique_ptr<TopicRegistry>            registry;
    std::unique_ptr<rune::node::WorkerPool>   pool;
    ClientGateway                             gw;
    PartitionReplica*                         rep = nullptr;
    std::map<int64_t, pc::ProduceResponse>    produced;      // by correlationId
    std::map<int64_t, pc::SubscribeResponse>  subscribed;    // by correlationId
    std::map<int64_t, int16_t>                errors;        // ErrorResponse code by correlationId
    std::vector<pc::Records>                  records;       // baseOffset / subscriptionId only
    int64_t                                   now  = 1'000'000'000;
    int64_t                                   corr = 0;

    explicit Rig(RigOpts opts = {})
        : o(opts),
          mem(opts.ringBytes, 10'000),
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n, /*sessionEgress=*/false);
              return 1;
          }, 1)) {
        rune::node::TopicSpec ts;
        ts.name = "presence"; ts.partitionCount = 1; ts.topicId = 0;
        ts.persistence = rune::node::PersistenceMode::Transient;
        if (o.runtime) {
            registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
            EXPECT_EQ(registry->createTopic(ts), 0);   // C1: awaiting its catalog commit
            EXPECT_TRUE(registry->awaitingCommit(0));
        } else {
            registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        }
        if (o.workers) {
            pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1));
            pool->setRegistry(&reg);
        }
        if (!o.runtime) addReplica();
        gw.setTopicRegistry(registry.get());
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n, true); });
        if (pool) gw.setWorkerPool(pool.get());
    }
    void addReplica() {
        PartitionConfig c;
        c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
        const std::vector<int32_t> set = o.withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, set, set, &mem, &transport);
        r->setExternalFollowerFlush(true);
        rep = r.get();
        reg.add(0, 0, std::move(r));
        if (pool) pool->assignFromRegistry(reg);
    }
    /// What BrokerNode's kA2cTopicCommitted handler does: the topic becomes
    /// servable and its replicas are built (createRuntimeUserReplicas).
    void commit() {
        ASSERT_TRUE(registry->commitTopic(0));
        addReplica();
    }
    void onFrame(const uint8_t* b, std::size_t n, bool sessionEgress) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::ProduceResponse:
            if (auto r = pc::ProduceResponse::decode(b, n)) produced[r->correlationId] = *r;
            break;
        case pc::TemplateId::SubscribeResponse:
            if (auto r = pc::SubscribeResponse::decode(b, n)) subscribed[r->correlationId] = *r;
            break;
        case pc::TemplateId::ErrorResponse:
            if (auto r = pc::ErrorResponse::decode(b, n)) errors[r->correlationId] = r->code;
            break;
        case pc::TemplateId::Records:
            if (sessionEgress) if (auto r = pc::Records::decode(b, n)) records.push_back(*r);
            break;
        default: break;
        }
    }
    void pass() {
        now += 1'000'000;
        if (pool) (void)pool->worker(0).doWork(now);
        gw.drainWorkerReturnsForTest(now);
    }
    int64_t produce(int64_t sid, int recs, uint8_t acks = 1, std::size_t valueBytes = 1) {
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = ++corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = acks; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        const auto batch = batchOf(recs, valueBytes);
        msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        gw.applyProduceForTest(frame.data(), frame.size());
        return corr;
    }
    std::optional<pc::ProduceResponse> awaitProduced(int64_t c) {
        for (int i = 0; i < 64 && !produced.count(c); ++i) pass();
        const auto it = produced.find(c);
        return it == produced.end() ? std::nullopt : std::optional<pc::ProduceResponse>(it->second);
    }
    void subscribe(int64_t sid, int64_t fromOffset, int64_t subCorr) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = subCorr; s.topicId = 0; s.partition = 0;
        s.partitionKnown = 1; s.fromOffset = fromOffset; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
    }
    /// Push cycles (PushRead → worker → PushReadResult → Records) until n frames (bounded).
    bool pushUntil(std::size_t n) {
        for (int i = 0; i < 64 && records.size() < n; ++i) { gw.pushSubscriptionsForTest(); pass(); }
        return records.size() >= n;
    }
};

constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kUnknownPartition = static_cast<int16_t>(pc::ErrorCode::UnknownPartition);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);

} // namespace

TEST(TransientGatewayRegression, SubscribeToATransientPartitionIsAcceptedRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, /*Earliest=*/-2, 41);
    ASSERT_TRUE(r.subscribed.count(41));
    EXPECT_EQ(r.subscribed[41].errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[41].subscriptionId), 0);
}

TEST(TransientGatewayRegression, ProduceThroughTheWorkerAndPushToTheSubscriberRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    const auto p1 = r.awaitProduced(r.produce(sid, 3, /*acks=all*/1));
    ASSERT_TRUE(p1.has_value());
    ASSERT_EQ(p1->errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(p1->baseOffset, 0);
    const auto p2 = r.awaitProduced(r.produce(sid, 2, 1));
    ASSERT_TRUE(p2.has_value());
    ASSERT_EQ(p2->errorCode, kOk);
    EXPECT_EQ(p2->baseOffset, 3);
    EXPECT_EQ(r.mem.logEndOffset(), 5) << "the owning worker appended to the ring";

    r.subscribe(sid, -2, 42);
    ASSERT_TRUE(r.pushUntil(2)) << "no Records pushed for a transient subscription";
    EXPECT_EQ(r.records[0].baseOffset, 0);
    EXPECT_EQ(r.records[1].baseOffset, 3);
    EXPECT_EQ(r.records[0].subscriptionId, r.subscribed[42].subscriptionId);
}

TEST(TransientGatewayRegression, SubscribeLatestAndExplicitOffsetClampToTheHighWatermarkRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    for (int i = 0; i < 4; ++i) r.pass();   // the worker published its HW
    ASSERT_EQ(r.rep->gatewayHighWatermark(), 2);
    r.subscribe(sid, /*Latest=*/-1, 101);
    r.subscribe(sid, 1, 102);
    r.subscribe(sid, 99, 103);
    for (int64_t c : {101, 102, 103}) ASSERT_EQ(r.subscribed.at(c).errorCode, kOk);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[101].subscriptionId), 2) << "Latest replays old records";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[102].subscriptionId), 1);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[103].subscriptionId), 2) << "beyond the HW clamps to it";
}

TEST(TransientGatewayRegression, GroupSubscriptionOnATransientPartitionIsPushedRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 2)).value().errorCode, kOk);
    const int64_t subId = r.gw.addGroupSubscription(sid, 0, 0, /*fromOffset=*/0, /*isolation=*/0,
                                                    /*pendingCarrier=*/false);
    ASSERT_GE(subId, 0) << "a group member could not be assigned a transient partition (was: -1)";
    ASSERT_TRUE(r.pushUntil(1));
    EXPECT_EQ(r.records[0].subscriptionId, subId);
    EXPECT_EQ(r.records[0].baseOffset, 0);
}

TEST(TransientGatewayRegression, TxnMarkerForATransientPartitionIsRoutedToTheWorkerRegression) {
    Rig r;
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    EXPECT_EQ(r.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), /*producerId=*/7, /*commit=*/true,
                                       /*coordinatorEpoch=*/0),
              ClientGateway::kControlRecordAsync) << "was -1 (dropped): no disk Log";
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(r.mem.logEndOffset(), 1) << "the owning worker appended the marker to the ring";
}

// C1 (DDL conformance final review) × E2: a RUNTIME transient topic whose
// catalog record has not committed serves NOTHING — produce NotLeader (nothing
// appended), subscribe the bare ErrorResponse{NotLeader} (nothing registered),
// no group subscription, no marker — exactly like an uncommitted disk topic.
// Once committed (and its replica built, as kA2cTopicCommitted does) it serves.
TEST(TransientGatewayRegression, UncommittedTransientTopicServesNothingUntilCommitRegression) {
    Rig r({.workers = true, .runtime = true});
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t c = r.produce(sid, 1);
    ASSERT_TRUE(r.produced.count(c));
    EXPECT_EQ(r.produced[c].errorCode, kNotLeader) << "an uncommitted transient topic took a produce";
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    r.subscribe(sid, -2, 51);
    EXPECT_FALSE(r.subscribed.count(51)) << "an uncommitted transient topic registered a subscription";
    ASSERT_TRUE(r.errors.count(51));
    EXPECT_EQ(r.errors[51], kNotLeader);
    EXPECT_EQ(r.gw.addGroupSubscription(sid, 0, 0, 0, 0, false), -1);
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    EXPECT_EQ(r.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 7, true, 0), -1);

    r.commit();
    const auto ok = r.awaitProduced(r.produce(sid, 2));
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, kOk) << "the committed transient topic must serve";
    EXPECT_EQ(ok->baseOffset, 0);
    r.subscribe(sid, -2, 52);
    ASSERT_TRUE(r.subscribed.count(52));
    EXPECT_EQ(r.subscribed[52].errorCode, kOk);
    ASSERT_TRUE(r.pushUntil(1));
    EXPECT_EQ(r.records[0].baseOffset, 0);
}

// The DDL final-fix concern: a COMMITTED runtime transient topic produced
// through the gateway (legacy runtime) answered UnknownPartition (8) because it
// has no servable disk Log.  It is served through its owning worker.
TEST(TransientGatewayRegression, CommittedRuntimeTransientTopicServesThroughTheWorkerRegression) {
    Rig r({.workers = true, .runtime = true});
    r.commit();
    const int64_t sid = r.gw.injectTestSession(0);
    for (int64_t want : {0, 2}) {
        const auto p = r.awaitProduced(r.produce(sid, 2, /*acks=all*/1));
        ASSERT_TRUE(p.has_value());
        ASSERT_EQ(p->errorCode, kOk) << "was UnknownPartition (8)";
        EXPECT_EQ(p->baseOffset, want);
    }
    r.subscribe(sid, -2, 61);
    ASSERT_TRUE(r.pushUntil(2));
    EXPECT_EQ(r.records[0].baseOffset, 0);
    EXPECT_EQ(r.records[1].baseOffset, 2);
}

// Ruling 2 on the wire: a ring full of not-yet-replicated records answers 42 to
// a v6 session and NotLeader (its retriable code) to a v5 session; nothing is
// appended either way.
TEST(TransientGatewayRegression, FullRingAnswersTransientLogFullOnlyToV6SessionsRegression) {
    Rig r({.workers = true, .ringBytes = 16 * 1024, .withFollower = true});
    const int64_t v6 = r.gw.injectTestSession(0);
    const int64_t v5 = r.gw.injectTestSession(0, /*clientVersion=*/5);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        const auto p = r.awaitProduced(r.produce(v6, 1, /*acks=*/0, /*valueBytes=*/1000));
        ASSERT_TRUE(p.has_value());
        code = p->errorCode;
    }
    ASSERT_EQ(code, kTransientLogFull) << "a v6 session must see 42 for a full ring";
    const int64_t leo = r.mem.logEndOffset();
    const auto old = r.awaitProduced(r.produce(v5, 1, 0, 1000));
    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(old->errorCode, kNotLeader) << "a v5 session must never see 42";
    EXPECT_EQ(r.mem.logEndOffset(), leo) << "a refused produce appended";
    EXPECT_EQ(r.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
}

// Pins "never inline": without a worker pool nothing may touch a worker-owned
// ring — every path refuses the transient partition explicitly.
TEST(TransientGateway, WithoutAWorkerPoolTransientIsRefusedNeverInline) {
    Rig r({.workers = false});
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t c = r.produce(sid, 1);
    ASSERT_TRUE(r.produced.count(c));
    EXPECT_EQ(r.produced[c].errorCode, kUnknownPartition);
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    r.subscribe(sid, -2, 9);
    ASSERT_TRUE(r.subscribed.count(9));
    EXPECT_EQ(r.subscribed[9].errorCode, kUnknownPartition);
    EXPECT_EQ(r.gw.addGroupSubscription(sid, 0, 0, 0, 0, false), -1);
}
```

In `libs/gateway/CMakeLists.txt`, after `gtest_discover_tests(rune_gateway_topic_servability_tests)` add:

```cmake

    # Phase 2b E2: TRANSIENT partitions served through their owning worker (C1-gated) — no Aeron.
    add_executable(rune_gateway_transient_tests tests/gateway/TransientGatewayTest.cpp)
    target_link_libraries(rune_gateway_transient_tests PRIVATE rune_gateway rune_node rune_replication rune_storage
                          rune_transaction spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_transient_tests)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 --target rune_gateway_transient_tests && ctest --test-dir build-fast -R 'TransientGateway' --output-on-failure -j14`
Expected: `Subscribe…Accepted`, `ProduceThroughTheWorker…`, `SubscribeLatest…`, `CommittedRuntime…` and `FullRing…` FAIL (Subscribe/Produce answered `UnknownPartition` = 8); `GroupSubscription…` FAILS (`addGroupSubscription` returns -1); `TxnMarker…` FAILS (`appendControlRecord` returns -1); `UncommittedTransientTopicServesNothingUntilCommit…` FAILS only after `r.commit()` (the C1 half passes today — it pins the rule the change must keep); `TransientGateway.WithoutAWorkerPool…` PASSES (pins "never inline").

- [ ] **Step 3: Implement — `ClientGateway.h`**

Right after

```cpp
    [[nodiscard]] bool routesToWorker_(int32_t topicId) const noexcept {
        return workerPool_ != nullptr && (!perWorkerIngressActive_ || topicId >= 0);
    }
```
add:

```cpp
    // Phase 2b E2: what this gateway can serve for (topic, partition) — ONE
    // explicit kind each (CLAUDE.md rule 3), C1 first:
    //   AwaitingCommit — a runtime topic whose catalog record has not committed
    //                    (TopicRegistry::awaitingCommit): retriable NotLeader.
    //   Disk           — its servable disk Log (`log`).
    //   Transient      — committed, persistence=transient, in range, replica
    //                    registered: NO disk Log; its InMemoryLog is owned by the
    //                    replica's worker and is NEVER touched here (its
    //                    published HW is the subscribe clamp).
    //   Unknown        — nothing servable here.
    struct ServedPartition {
        enum class Kind : uint8_t { Unknown, AwaitingCommit, Disk, Transient };
        Kind                kind = Kind::Unknown;
        rune::storage::Log* log  = nullptr;   ///< Kind::Disk only
    };
    [[nodiscard]] ServedPartition servedPartition_(int32_t topicId, int32_t partition) const noexcept;
```

- [ ] **Step 4: Implement — `ClientGateway.cpp`**

Insert right before `bool ClientGateway::handleProduce(const uint8_t* buf, std::size_t len)`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2: servedPartition_ — see ServedPartition.  The disk fast path is the
// same single servableLog() probe every site made before; the C1 / topic /
// replica probes run only when there is no servable disk Log.  Control thread.
// ---------------------------------------------------------------------------
ClientGateway::ServedPartition ClientGateway::servedPartition_(int32_t topicId, int32_t partition) const noexcept
{
    using Kind = ServedPartition::Kind;
    ServedPartition s;
    if (topicRegistry_ == nullptr) return s;
    if (rune::storage::Log* log = topicRegistry_->servableLog(topicId, partition)) [[likely]] {
        s.kind = Kind::Disk;
        s.log  = log;
        return s;
    }
    if (topicRegistry_->awaitingCommit(topicId)) { s.kind = Kind::AwaitingCommit; return s; }   // C1
    const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
    if (spec == nullptr || spec->persistence != rune::node::PersistenceMode::Transient) return s;
    if (partition < 0 || partition >= spec->partitionCount) return s;
    if (replicaRegistry_ != nullptr && replicaRegistry_->lookup(topicId, partition) != nullptr)
        s.kind = Kind::Transient;
    return s;
}

```

(`#include <rune/node/PersistenceMode.h>` if `ClientGateway.cpp` does not already see it through `TopicRegistry.h` / `NodeConfig.h`.)

`handleProduce` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->servableLog(msg.topicId, partition);
    if (!log) [[unlikely]] {
        if (topicRegistry_->awaitingCommit(msg.topicId)) {
            spdlog::debug("ClientGateway: Produce to topicId={} awaiting its DDL commit — NotLeader "
                          "(retriable)", msg.topicId);
            sendResp(pc::ErrorCode::NotLeader, partition, 0);
            return true;
        }
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }
```
with
```cpp
    // Phase 2b E2: a TRANSIENT partition has no disk Log — routed to its owning
    // worker below (op.logPtr = nullptr: the worker appends to the replica's ring).
    const ServedPartition served = servedPartition_(msg.topicId, partition);
    if (served.kind == ServedPartition::Kind::AwaitingCommit) [[unlikely]] {
        spdlog::debug("ClientGateway: Produce to topicId={} awaiting its DDL commit — NotLeader "
                      "(retriable)", msg.topicId);
        sendResp(pc::ErrorCode::NotLeader, partition, 0);
        return true;
    }
    if (served.kind == ServedPartition::Kind::Unknown) [[unlikely]] {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }
    rune::storage::Log* log = served.log;   // nullptr ⟺ Kind::Transient
```
and right before the comment block

```cpp
    // ---------------------------------------------------------------------------
    // INLINE path (pre-E2 / no WorkerPool):
```
(just after the `if (routeToWorker) { … return true; }` block) insert:

```cpp
    // Phase 2b E2: a TRANSIENT partition is appended only by its owning worker —
    // never inline (the gateway never touches a worker-owned ring).  Reached
    // only without a worker pool (unit rigs; ruling 6).
    if (log == nullptr) [[unlikely]] {
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }

```

`handleSubscribe` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->servableLog(msg.topicId, partition);
    if (!log) {
        if (topicRegistry_->awaitingCommit(msg.topicId)) {
            spdlog::debug("ClientGateway: Subscribe to topicId={} awaiting its DDL commit — NotLeader "
                          "(retriable)", msg.topicId);
            sendSubRedirect(std::string{}, msg.correlationId);
            return;
        }
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }
```
with
```cpp
    const ServedPartition served = servedPartition_(msg.topicId, partition);
    if (served.kind == ServedPartition::Kind::AwaitingCommit) {
        spdlog::debug("ClientGateway: Subscribe to topicId={} awaiting its DDL commit — NotLeader "
                      "(retriable)", msg.topicId);
        sendSubRedirect(std::string{}, msg.correlationId);
        return;
    }
    // Phase 2b E2: a TRANSIENT partition (no disk Log) is pushed by its owning
    // worker only — refused without a worker pool (no inline push path).
    if (served.kind == ServedPartition::Kind::Unknown ||
        (served.kind == ServedPartition::Kind::Transient && !workerPool_)) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient
```
replace `    const int64_t leo = log->logEndOffset();` with
```cpp
    // E2: a transient partition's replica is registered (servedPartition_), so
    // its clamp is the published HW below — the ring itself is never read here.
    const int64_t leo = log != nullptr ? log->logEndOffset() : 0;
```
and replace `    const int64_t logStart = log->logStartOffset();` with
```cpp
    // E2: a transient ring's start is worker-owned state (never read cross-
    // thread): Earliest resolves to 0 and the owning worker moves a cursor below
    // its ring start up to it (TRANSIENT_EVICTED, Worker::skipEvictedCursor_).
    const int64_t logStart = log != nullptr ? log->logStartOffset() : 0;
```

`pushSubscriptions` — replace

```cpp
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        if (!log) continue; // registry removed or not set — skip
```
with
```cpp
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        // Phase 2b E2: a TRANSIENT partition (no disk Log) is read by its owning
        // worker only (the PushRead below) — never by the inline path.
        if (!log && (!workerPool_ ||
                     servedPartition_(sub.topicId, sub.partition).kind != ServedPartition::Kind::Transient))
            continue; // registry removed or not set — skip
```
then replace
```cpp
            hw = replica ? replica->gatewayHighWatermark() : log->logEndOffset();
```
with
```cpp
            hw = replica ? replica->gatewayHighWatermark() : (log ? log->logEndOffset() : 0);
```
and in the `else` branch below it replace `            hw = log->logEndOffset();` with `            hw = log ? log->logEndOffset() : 0;`.

`appendRemoteControlRecord` — replace

```cpp
    rune::storage::Log* log = topicRegistry_ ? topicRegistry_->servableLog(w.topicId, w.partition) : nullptr;   // C1
    if (log == nullptr || w.topicId < 0 || w.marker.empty()) {
```
with
```cpp
    // C1 + E2: a committed transient partition takes markers too (its owning
    // worker appends the control batch to the ring); uncommitted / unknown →
    // NotLeader (the coordinator re-resolves — never Unknown*, see above).
    const ServedPartition served = servedPartition_(w.topicId, w.partition);
    rune::storage::Log* const log = served.log;
    const bool servable = served.kind == ServedPartition::Kind::Disk ||
                          served.kind == ServedPartition::Kind::Transient;
    if (!servable || w.topicId < 0 || w.marker.empty()) {
```
and replace
```cpp
    const int64_t base = appendControlRecordInline_(w.topicId, w.partition, w.marker, log);
```
with
```cpp
    // E2: a transient partition has no inline append (worker-owned ring) — NotLeader, the coordinator retries.
    const int64_t base = log != nullptr ? appendControlRecordInline_(w.topicId, w.partition, w.marker, log) : -1;
```

`addGroupSubscription` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->servableLog(topicId, partition);   // C1
    if (!log) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown (or uncommitted) topicId={} partition={}",
                     topicId, partition);
        return -1;
    }

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start).
    const int64_t leo = log->logEndOffset();
```
with
```cpp
    // C1 + E2: uncommitted / unknown → refused; a TRANSIENT partition (no disk
    // Log) is pushed by its owning worker only — refused without a worker pool.
    const ServedPartition served = servedPartition_(topicId, partition);
    if (served.kind == ServedPartition::Kind::AwaitingCommit || served.kind == ServedPartition::Kind::Unknown ||
        (served.kind == ServedPartition::Kind::Transient && !workerPool_)) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown (or uncommitted) topicId={} partition={}",
                     topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start;
    // E2: a transient cursor below the ring start is moved up by the worker).
    const int64_t leo = log != nullptr ? log->logEndOffset() : 0;
```

`appendControlRecord` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->servableLog(topicId, partition);   // C1
    if (!log) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown (or uncommitted) topicId={} partition={} "
                      "— dropped", topicId, partition);
        return -1;
    }
```
with
```cpp
    // C1 + E2: a committed TRANSIENT partition's markers ride the same worker /
    // remote paths below (the worker appends the control batch to the ring).
    const ServedPartition served = servedPartition_(topicId, partition);
    if (served.kind != ServedPartition::Kind::Disk && served.kind != ServedPartition::Kind::Transient) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown (or uncommitted) topicId={} partition={} "
                      "— dropped", topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient
```
and replace the function's last statement `    return appendControlRecordInline_(topicId, partition, marker, log);` with:

```cpp
    if (log == nullptr) [[unlikely]] {   // E2: transient without a worker pool — no inline append (ruling 6)
        spdlog::debug("ClientGateway::appendControlRecord: transient topicId={} partition={} without a "
                      "worker pool — dropped", topicId, partition);
        return -1;
    }
    return appendControlRecordInline_(topicId, partition, marker, log);
```

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'TransientGateway|TopicServability|ClientGateway|SubscribeDedupe|GatewaySubscribe|GatewayProduce|ReadCommittedPush|GroupPush|F15|D9|CoordinatorRedirect|MetadataResponse|MetadataStale' --output-on-failure -j14`
Expected: all PASS (`TopicServabilityTest` — the C1 suite — is unchanged: a disk topic's kinds are `Disk` / `AwaitingCommit` / `Unknown`, answered exactly as before).

- [ ] **Step 6: Commit**

```bash
git add libs/gateway/include/rune/gateway/ClientGateway.h libs/gateway/src/ClientGateway.cpp libs/gateway/tests/gateway/TransientGatewayTest.cpp libs/gateway/CMakeLists.txt
git commit -m "feat(gateway): serve committed transient partitions through their owning worker (produce, subscribe, groups, markers); C1-gated

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: BrokerNode over real Aeron — shard, legacy, 3-node acks=all, restart, runtime define (C1); docs

**Files:**
- Create: `libs/node/tests/integration/TransientTopicDataPathTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target right after `gtest_discover_tests(rune_shard_mode_tests …)`, ~`:1404`, inside the same `if(aeron_FOUND)` block)
- Modify: `docs/SELF-HOSTING.md` (the "### Transient topics" section, ~`:139-169`), `docs/release-notes/phase2b-e-transient-topics.md` (the "Status:" paragraph, ~`:6-8`)

**Interfaces:**
- Consumes: everything above; `rune::node::test::ShardTestClient` (`connect`, `produceUntilOk`, `subscribe`, `pollUntil`, `records`, `metadata`, `sendDefineTopic`, `awaitDefine`), `freeUdpPortBlocks`, `freeUdpPort`, `makeBatch` (`libs/node/tests/support/ShardTestClient.h`), `leoOnOwner`, `hasReplica` (`support/OwnerThreadTestHelpers.h`); `TopicSpec.persistence = PersistenceMode::Transient`, `NodeConfig::memoryLogRingBytes` (Part E).
- Produces (consumed by Task 13, which appends to this file): the file-local helpers `TmpDir`, `Ports::discover`, `makeConfig(nodeId, dataDir, ports, nodes, shard, ringBytes = 0)`, `gatewayChannel`, `workerIngressChannel`, `pushedAt`, `ttBlob`, `defineTransient`.

- [ ] **Step 1: Write the tests** — create `libs/node/tests/integration/TransientTopicDataPathTest.cpp`:

```cpp
// TransientTopicDataPathTest.cpp — Phase 2b E2, in-process BrokerNodes over
// REAL Aeron UDP: a TRANSIENT topic (static RUNE_TOPICS persistence=transient,
// topic 0 "tt") produces and is consumed in shard mode (the default: produce on
// the worker's ingress, Records pushed by the worker) and in legacy mode
// (produce + PushRead through the gateway); acks=all is replicated to a
// majority before the ack (3 nodes); a restart keeps the topic and empties it;
// a transient topic DEFINED AT RUNTIME (DefineTopic persistence=1) is served
// once its DDL OK arrives (C1: the OK is held until the topic serves) — through
// the legacy gateway this answered UnknownPartition (8) before E2.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_tt_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;

struct Ports {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static Ports discover(int nodes) {
        Ports p;
        auto blocks = freeUdpPortBlocks(3 * nodes, 1);
        std::vector<std::pair<int, int>> taken;
        for (uint16_t b : blocks) taken.push_back({b, b + 1});
        auto single = [&taken] {
            for (int attempt = 0; attempt < 2000; ++attempt) {
                const int q = freeUdpPort();
                bool clash = q == 0;
                for (const auto& [lo, hi] : taken) clash = clash || (q >= lo && q < hi);
                if (!clash) { taken.push_back({q, q + 1}); return static_cast<uint16_t>(q); }
            }
            throw std::runtime_error("Ports: no collision-free UDP port");
        };
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(single()); p.client.push_back(single()); p.repl.push_back(single());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int nodes, bool shard,
                      int32_t ringBytes = 0 /* 0 = the default 8 MiB */) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = shard ? NodeConfig::WorkerMode::Shard : NodeConfig::WorkerMode::Legacy;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = shard;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    if (ringBytes > 0) cfg.memoryLogRingBytes = ringBytes;   // a power of two (RUNE_TRANSIENT_LOG_RING_BYTES rule)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t;
    t.name = "tt"; t.partitionCount = 1; t.topicId = 0;
    t.persistence = PersistenceMode::Transient;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gatewayChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[node]);
}
std::string workerIngressChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.ib[node]);
}
std::size_t pushedAt(const ShardTestClient& c, int64_t base) {
    std::size_t n = 0;
    for (const auto& r : c.records()) n += r.baseOffset == base ? 1u : 0u;
    return n;
}
/// A DefineTopic descriptor: {id Int64 (key), hp Int64} (validate=false: records are not checked).
std::vector<uint8_t> ttBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("presence", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}
/// DefineTopic(name, 1 partition, persistence=transient) over the wire; retries
/// Internal while the admin thread does not yet lead __topic_schemas (the
/// documented DDL settle — bounded, not a correctness gate).  Returns the
/// topic id, or -1.
int32_t defineTransient(ShardTestClient& c, const std::string& gw, int64_t sid, const std::string& name) {
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto r = c.awaitDefine(c.sendDefineTopic(gw, kClientStream, sid, name, 1, ttBlob(), /*persistence=*/1),
                                     std::chrono::steady_clock::now() + 10s);
        if (r && r->errorCode == 0) return r->topicId;
        if (r && r->errorCode != static_cast<uint16_t>(rune::protocol::client::ErrorCode::Internal)) return -1;
        std::this_thread::sleep_for(20ms);   // admin leadership settling
    }
    return -1;
}

} // namespace

TEST(TransientTopicDataPathRegression, ShardProduceAndSubscribeEarliestLatestOffsetRegression) {
    TmpDir dir{"shard"}, cdir{"shard_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 3, 6}) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0,
                                        /*acks=*/1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "a transient produce on the worker shard was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        // Earliest: the whole ring.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 1; })) << "an earliest subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u);
        EXPECT_EQ(pushedAt(c, 3), 1u);
        // An explicit offset.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*fromOffset=*/6));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 2; })) << "an offset subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u) << "the offset subscription replayed below its offset";
        // Latest: never replays — produce until its subscription delivers; its FIRST batch is new.
        std::set<int64_t> before;
        for (const auto& r : c.records()) before.insert(r.subscriptionId);
        ASSERT_EQ(before.size(), 2u);
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Latest=*/-1));
        int64_t latestFirstBase = -1;
        for (int i = 0; i < 20 && latestFirstBase < 0; ++i) {
            ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                         makeBatch(3)).has_value());
            (void)c.pollUntil([&] {
                for (const auto& r : c.records())
                    if (!before.count(r.subscriptionId)) { latestFirstBase = r.baseOffset; return true; }
                return false;
            });
        }
        ASSERT_GE(latestFirstBase, 0) << "the latest subscription never delivered";
        EXPECT_GE(latestFirstBase, 9) << "a Latest subscription replayed old records";
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, LegacyModeProduceAndSubscribeRegression) {
    TmpDir dir{"legacy"}, cdir{"legacy_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 2}) {
            auto ack = c.produceUntilOk(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*acks=*/1, makeBatch(2));
            ASSERT_TRUE(ack.has_value()) << "a transient produce through the gateway was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 2) >= 1; })) << "the gateway never pushed the transient topic";
        EXPECT_EQ(pushedAt(c, 0), 1u);
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, ThreeNodeAcksAllIsOnAMajorityBeforeTheAckRegression) {
    TmpDir d0{"m0"}, d1{"m1"}, d2{"m2"}, cdir{"mc"};
    const auto ports = Ports::discover(3);
    BrokerNode n0{makeConfig(0, d0.path, ports, 3, true)};
    BrokerNode n1{makeConfig(1, d1.path, ports, 3, true)};
    BrokerNode n2{makeConfig(2, d2.path, ports, 3, true)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms));
    int32_t leader = -1;
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        for (int i = 0; i < 200 && leader < 0; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            if (leader < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader), kClientStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " on a transient topic not committed";
        }
    }
    // While the nodes run (the data is in memory only): every acked record is on a majority.
    int holders = 0;
    for (BrokerNode* n : {&n0, &n1, &n2})
        holders += (hasReplica(*n, 0, 0) && leoOnOwner(*n, 0, 0) >= 20) ? 1 : 0;
    EXPECT_GE(holders, 2) << "an acks=all ack on a transient topic before a majority held the records";
    n0.stop(); n1.stop(); n2.stop();
}

TEST(TransientTopicDataPathRegression, RestartKeepsTheTopicAndEmptiesItRegression) {
    TmpDir dir{"restart"}, c1{"restart_c1"}, c2{"restart_c2"};
    const auto ports = Ports::discover(1);
    const auto cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/true);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c1.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            for (int i = 0; i < 2; ++i)
                ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                             makeBatch(3)).has_value());
        }
        node.stop();
    }
    EXPECT_FALSE(fs::exists(dir.path / "topics" / "tt")) << "a transient topic never writes a partition directory";
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c2.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "the transient topic must exist after a restart";
            EXPECT_EQ(ack->baseOffset, 0) << "a transient topic restarts EMPTY";
            ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
            ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 0) >= 1; }));
            EXPECT_EQ(c.records().size(), 1u) << "pre-restart records came back";
        }
        node.stop();
    }
}

// C1 × E2 (the DDL final-fix concern): a transient topic DEFINED AT RUNTIME on a
// legacy-runtime node.  Its DDL OK is held until it serves (DdlAnswerGate), so
// the first produce through the gateway is served by the owning worker — it
// answered UnknownPartition (8) before E2 — and a subscription from earliest
// gets both batches.
TEST(TransientTopicDataPathRegression, LegacyRuntimeTransientDefineThenProduceAndSubscribeRegression) {
    TmpDir dir{"rt_legacy"}, cdir{"rt_legacy_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        const int32_t tid = defineTransient(c, gatewayChannel(ports, 0), sid, "presence_rt");
        ASSERT_GE(tid, 0) << "DefineTopic(transient) failed";
        for (int64_t want : {0, 2}) {
            auto ack = c.produceUntilOk(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, /*acks=all*/1,
                                        makeBatch(2));
            ASSERT_TRUE(ack.has_value()) << "a runtime transient topic was not served (was UnknownPartition)";
            EXPECT_EQ(ack->baseOffset, want);
        }
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 2) >= 1; }));
        EXPECT_EQ(pushedAt(c, 0), 1u);
    }
    node.stop();
}
```

In `libs/node/CMakeLists.txt`, right after

```cmake
        gtest_discover_tests(rune_shard_mode_tests PROPERTIES TIMEOUT 360 RESOURCE_LOCK rune_udp_ports)
```
add:

```cmake

        # Phase 2b E2: TRANSIENT topics end to end in-process over real Aeron —
        # shard + legacy produce/subscribe, 3-node acks=all, restart (empty).
        add_executable(rune_transient_topic_data_path_tests tests/integration/TransientTopicDataPathTest.cpp)
        target_include_directories(rune_transient_topic_data_path_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_transient_topic_data_path_tests PRIVATE rune_node rune_view aeron::aeron
                              GTest::gtest_main)
        gtest_discover_tests(rune_transient_topic_data_path_tests PROPERTIES TIMEOUT 360 RESOURCE_LOCK rune_udp_ports)
```

- [ ] **Step 2: Run them**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 --target rune_transient_topic_data_path_tests && ctest --test-dir build-fast -R 'TransientTopicDataPath' --output-on-failure -j14`
Expected: all five PASS on top of Tasks 1-7. (At `6a3b9baa` every one FAILS: the shard produce answers Internal (6), the legacy produce/subscribe answer UnknownPartition (8).) A failure here is a real integration gap: fix it in the owning layer with its own `*Regression*` test before continuing.

- [ ] **Step 3: Docs** — in `docs/SELF-HOSTING.md`, in the "### Transient topics" section, replace the line

```markdown
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
```
with
```markdown
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
- Produce (every `acks` mode, idempotent and transactional producers), push subscriptions from
  earliest / latest / an offset, consumer groups and `read_committed` work exactly as on disk topics,
  in both worker modes (`RUNE_WORKER_MODE=shard`, the default, and `legacy`). `tier=strict` acks like
  `majority` (there is nothing to fsync).
- A transient topic defined at runtime serves only once its definition has committed, exactly like a
  disk topic: until then produce and subscribe answer `NotLeader` (retriable) and metadata does not list it.
- **Retention is the in-memory ring:** `RUNE_TRANSIENT_LOG_RING_BYTES` per partition (node-wide, default
  8 MiB, at most 1 000 000 batches). When it is full the oldest **committed** records (below the high
  watermark) are evicted; a consumer whose position was evicted resumes at the oldest record still held
  (counted and logged). Records not yet replicated are never evicted: while the ring is full of them a
  produce is refused with error 42 (`TransientLogFull`, retriable — the client retries after a backoff
  without refreshing metadata; clients older than this release see `NotLeader`) until the followers
  catch up. A batch larger than the whole ring is refused `MessageTooLarge`.
- A node that rejoins a transient partition whose leader already evicted the oldest records restarts
  its copy at the leader's oldest record and catches up from there.
- A transient view over a transient topic re-folds from the oldest held record when the topic's ring is
  reset or recreated under it (counted as `transientSourceResets`); records evicted before the view
  folded them are counted as `transientRecordsEvicted`.
```

and delete the bullet

```markdown
- Status in this release: a transient topic can be defined, survives restart as an empty topic,
  and replicates. Produce/subscribe on transient topics is completed by the transient data-path
  work (Part E2); end-to-end produce/consume is not yet claimed.
```

In `docs/release-notes/phase2b-e-transient-topics.md`, replace the paragraph

```markdown
Status: in this release a transient topic can be defined, survives restart as
an empty topic, and replicates. Produce/subscribe on transient topics is
completed by the transient data-path work (Part E2).
```
with
```markdown
Status: a transient topic can be defined, survives restart as an empty topic,
replicates, and is produced to and consumed like a disk topic (Part E2). A new
retriable error code 42 (`TransientLogFull`) answers a produce while the
topic's in-memory ring is full of records not yet replicated; it is sent only to
clients of this release (client version 6) — older clients get `NotLeader`.
Upgrade brokers before clients.
```

- [ ] **Step 4: Gate + commit**

Run: `ctest --test-dir build-fast -R 'TransientTopicDataPath|ShardMode|RuntimeTopicRecovery|TransientTopic_|DdlConformanceBroker' --output-on-failure -j14`
Expected: all PASS.

```bash
git add libs/node/tests/integration/TransientTopicDataPathTest.cpp libs/node/CMakeLists.txt docs/SELF-HOSTING.md docs/release-notes/phase2b-e-transient-topics.md
git commit -m "test(node): transient topics end to end over Aeron (shard, legacy, 3-node acks=all, restart, runtime define); docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller):** full broker `*Regression*` suite (gateway + BrokerNode).

---

### Task 9: Views over transient sources — the `Live` cursor records the source ring's identity (Part E Task 7 carry-over)

**What is missing (read from the code at `6a3b9baa`).** `WorkerViewSet::reconcileTransient_` (`WorkerViewSet.cpp:325-361`) judges a `Live` cursor only by the source's current `[logStart, logEnd)` (`TransientBounds`, filled in `Worker::driveViewsOf_` from the `InMemoryLog` alone). A view instance outlives the ring it folded: it lives in the worker's `WorkerViewSet` keyed by (view, partition), while the source ring can change identity under it in two ways the range check cannot see:

1. **The ring's content changes in place** — `PartitionReplica::truncateLog` (KIP-279 divergence) and, from Task 3, `resetTransientLogTo_` (a follower ring restarted at the leader's ring start). Both bump `PartitionReplica::logGeneration()` and set `lastTruncationOffset()`; the view never looks at either.
2. **The ring is recreated** — a NEW `PartitionReplica` + `InMemoryLog` for the same (topic, partition) on this node (the bridge's `applyDeleteForKey` removes a replica; `createRuntimeUserReplicas` / an assignment builds a new one with a new registry **incarnation**). The new ring restarts at 0 (or at the leader's ring start) and its offsets can name different records — exactly what `RecoveredUnreconciled` handles across a node restart, but within a running node nothing tells the instance.

In both cases a cursor that happens to lie inside the new `[logStart, logEnd)` is accepted as `Live`: the view silently skips the records below it and keeps rows folded from records that no longer exist. **The hardening:** the `Live` state carries the source identity it is live against — `(replica incarnation, logGeneration)` — and `reconcileTransient_` resolves a change explicitly: a new incarnation re-folds from the ring start (`SOURCE_RECREATED`); a new generation whose cut is below the cursor re-folds from the cut (`SOURCE_CUT`). Rows are kept (re-folding retained records is idempotent under the views' keyed table semantics, the same argument Part E used for `RecoveredUnreconciled`); each event is counted in the new `ViewFoldCounters::transientSourceResets` and logged once. The `CursorState` enum becomes a variant so `Live` holds exactly its variables (CLAUDE.md rule 3).

**Files:**
- Modify: `libs/node/include/rune/node/WorkerViewSet.h` (`#include <variant>`; `ViewFoldCounters` ~`:114-121`; `TransientBounds` ~`:124-128`; the `CursorState` enum + comment ~`:130-142`; `ViewInstance::cursorState` ~`:160`; the `reconcileTransient_` comment ~`:598`)
- Modify: `libs/node/src/WorkerViewSet.cpp` (`reconcileTransient_` ~`:325-361`; `loadRecoveryCheckpoint_` ~`:454`; `installViewForRecovery` ~`:507`)
- Modify: `libs/node/include/rune/node/Worker.h` (`driveViews_` ~`:5186-5220`, `driveViewsOf_` ~`:5228-5245`)
- Test: `libs/node/tests/node/WorkerViewSetTest.cpp`, `libs/node/tests/node/WorkerTest.cpp` (append)

**Interfaces:**
- Consumes: `PartitionReplica::incarnation()`, `logGeneration()`, `lastTruncationOffset()`, `memoryLog()`; Task 3's generation bump in `resetTransientLogTo_`.
- Produces (namespace `rune::node`):
  ```cpp
  struct TransientBounds { int64_t logStart = 0; int64_t logEnd = 0;
                           uint32_t incarnation = 0; uint64_t logGeneration = 0; int64_t cutOffset = 0; };
  struct CursorFresh {};  struct CursorRecoveredUnreconciled {};
  struct CursorLive { uint32_t incarnation = 0; uint64_t logGeneration = 0; };
  using TransientCursor = std::variant<CursorFresh, CursorRecoveredUnreconciled, CursorLive>;
  // ViewInstance::transientCursor replaces ViewInstance::cursorState
  // ViewFoldCounters::transientSourceResets (uint64_t)
  ```
  Existing `TransientBounds{logStart, logEnd}` aggregate initialisations (Part E tests) keep compiling: the new members default to an unregistered, generation-0 ring.

- [ ] **Step 1: Write the failing tests** — append to `libs/node/tests/node/WorkerViewSetTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2 (Part E Task 7 carry-over): a Live cursor is live against ONE
// source ring identity — (replica incarnation, logGeneration).
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> concatBatches(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> out;
    for (const auto& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}
} // namespace

// The source ring was RECREATED (a new replica for the same partition on this
// node) and its range [0,3) happens to cover the old cursor 2: before the fix
// the instance stayed Live at 2 — keys 7 and 8 kept their stale values.
TEST(WorkerViewSetRegression, TransientSourceRecreatedRefoldsFromRingStartRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000035).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-recreated"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto inc1 = concatBatches({upsertAt(0, 7, 10), upsertAt(1, 8, 20)});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{inc1.data(), inc1.size(), 0, 2}; };
    rune::node::TransientBounds tb1{0, 2};
    tb1.incarnation = 1;
    vs.driveFolds(5, 0, rr1, 2, 0, &tb1);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000035, 0, 7, L), nullptr);
    ASSERT_EQ(rune::view::readSlot(vs.getRow(0x40000035, 0, 7, L), *L, 1).i, 10);

    const auto inc2 = concatBatches({upsertAt(0, 7, 99), upsertAt(1, 8, 98), upsertAt(2, 9, 97)});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{inc2.data(), inc2.size(), 0, 3}; };
    rune::node::TransientBounds tb2{0, 3};
    tb2.incarnation = 2;
    vs.driveFolds(5, 0, rr2, 3, 0, &tb2);
    EXPECT_EQ(rune::view::readSlot(vs.getRow(0x40000035, 0, 7, L), *L, 1).i, 99) << "a stale row survived";
    EXPECT_EQ(rune::view::readSlot(vs.getRow(0x40000035, 0, 8, L), *L, 1).i, 98) << "a stale row survived";
    ASSERT_NE(vs.getRow(0x40000035, 0, 9, L), nullptr);
    EXPECT_EQ(vs.foldCounters(0x40000035, 0)->transientSourceResets, 1u);
    EXPECT_EQ(vs.foldCounters(0x40000035, 0)->transientRecordsEvicted, 0u) << "a recreation is not an eviction";
}

// The ring's content changed IN PLACE (logGeneration moved: a truncation or a
// follower ring reset) with the cut BELOW the cursor: re-fold from the cut.  A
// generation change whose cut is at/above the cursor leaves it alone.
TEST(WorkerViewSetRegression, TransientSourceCutBelowCursorRefoldsFromTheCutRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000036).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-cut"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto gen0 = concatBatches({upsertAt(0, 7, 1), upsertAt(1, 8, 2), upsertAt(2, 9, 3)});
    auto rr0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{gen0.data(), gen0.size(), 0, 3}; };
    rune::node::TransientBounds tb0{0, 3};
    vs.driveFolds(5, 0, rr0, 3, 0, &tb0);   // Live, cursor 3, generation 0

    // Generation 1, cut at 1: offsets >= 1 changed — 8 and 9 now carry new values.
    const auto gen1 = concatBatches({upsertAt(0, 7, 1), upsertAt(1, 8, 22), upsertAt(2, 9, 33)});
    auto rr1 = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 1) << "re-fold from the cut, not from the ring start or the old cursor";
        return rune::storage::LogReadView{gen1.data(), gen1.size(), 0, 3};
    };
    rune::node::TransientBounds tb1{0, 3};
    tb1.logGeneration = 1;
    tb1.cutOffset     = 1;
    vs.driveFolds(5, 0, rr1, 3, 0, &tb1);
    const rune::view::RowLayout* L = nullptr;
    EXPECT_EQ(rune::view::readSlot(vs.getRow(0x40000036, 0, 8, L), *L, 1).i, 22);
    EXPECT_EQ(rune::view::readSlot(vs.getRow(0x40000036, 0, 9, L), *L, 1).i, 33);
    EXPECT_EQ(vs.foldCounters(0x40000036, 0)->transientSourceResets, 1u);

    // Generation 2, cut at 5 (above the cursor 3): nothing the view folded changed.
    auto rrNone = [&](int64_t, int64_t) { return rune::storage::LogReadView{}; };
    rune::node::TransientBounds tb2{0, 3};
    tb2.logGeneration = 2;
    tb2.cutOffset     = 5;
    vs.driveFolds(5, 0, rrNone, 3, 0, &tb2);
    EXPECT_EQ(vs.foldCounters(0x40000036, 0)->transientSourceResets, 1u) << "a cut above the cursor is no reset";
}
```

Append to `libs/node/tests/node/WorkerTest.cpp`:

```cpp
// Phase 2b E2 (Part E Task 7 carry-over), on the real worker pass: the
// partition's replica is RECREATED (new incarnation, new ring) while the view
// instance survives on the worker; the new ring's range covers the old cursor.
// Before the fix the instance stayed Live and key 7 kept its stale hp 55.
TEST(WorkerRegression, ViewOverRecreatedTransientSourceRefoldsRegression) {
    constexpr int32_t kSrc = 22, kView = 0x40000042;
    ReplicaFixture a(kSrc, 0), b(kSrc, 0);
    a.replica->setRegistryHandle(/*id=*/1, /*incarnation=*/1);
    b.replica->setRegistryHandle(/*id=*/1, /*incarnation=*/2);

    const auto src = rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
    rune::view::ViewPlan v;
    v.viewName = "char_hp_recreated"; v.key = "characterId"; v.sources = {{kSrc, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, kView).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-worker-transient-recreated"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    auto put = [](PartitionReplica& r, uint8_t id, int32_t hp) {
        std::vector<uint8_t> value;
        auto le = [&](uint64_t x, int n) { for (int i = 0; i < n; ++i) value.push_back(uint8_t(x >> (8 * i))); };
        le(12, 2); le(1, 2); le(id, 8); le(static_cast<uint32_t>(hp), 4);
        std::vector<uint8_t> key(8, 0); key[0] = id;
        ASSERT_GE(r.appendKeyed(std::span<const uint8_t>(key), std::span<const uint8_t>(value), false), 0);
    };
    put(*a.replica, 7, 55);                                   // incarnation 1: offset 0
    Worker w(/*workerId=*/0);
    w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});
    w.setViewSet(&vs);
    const rune::view::RowLayout* L = nullptr;
    int64_t nowNs = 0;
    for (int i = 0; i < 200 && vs.getRow(kView, 0, 7, L) == nullptr; ++i) w.doWork(nowNs += 1'000'000);
    ASSERT_NE(vs.getRow(kView, 0, 7, L), nullptr);
    ASSERT_EQ(rune::view::readSlot(vs.getRow(kView, 0, 7, L), *L, 1).i, 55);

    put(*b.replica, 7, 66);                                   // incarnation 2: offset 0 — a different record
    put(*b.replica, 9, 1);                                    // offset 1: the ring now covers the old cursor 1
    w.setOwnedDirect(Worker::OwnedSet{b.replica.get()});      // the partition's replica was recreated
    for (int i = 0; i < 200 && rune::view::readSlot(vs.getRow(kView, 0, 7, L), *L, 1).i != 66; ++i)
        w.doWork(nowNs += 1'000'000);
    EXPECT_EQ(rune::view::readSlot(vs.getRow(kView, 0, 7, L), *L, 1).i, 66) << "the view kept a row of the old ring";
    ASSERT_NE(vs.getRow(kView, 0, 9, L), nullptr);
    EXPECT_EQ(vs.foldCounters(kView, 0)->transientSourceResets, 1u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j14 --target rune_node_tests rune_worker_tests 2>&1 | tail -5` (`WorkerViewSetTest.cpp` is in `rune_node_tests`, `WorkerTest.cpp` in `rune_worker_tests`)
Expected: compile errors `no member named 'incarnation' in 'rune::node::TransientBounds'` and `no member named 'transientSourceResets'`. With only the fields added (no reconcile change): `TransientSourceRecreatedRefolds…` FAILS (key 7 still 10), `TransientSourceCutBelowCursor…` FAILS (`from == 3`), `ViewOverRecreatedTransientSourceRefolds…` FAILS (hp 55).

- [ ] **Step 3: Implement — `WorkerViewSet.h`**

Add `#include <variant>` after `#include <unordered_set>`.

In `struct ViewFoldCounters`, after `uint64_t transientRecordsEvicted = 0;   // transient ring dropped records the view had not folded` add:

```cpp
    uint64_t transientSourceResets   = 0;   // E2: the source ring was recreated / cut under a live cursor (re-folded)
```

Replace `struct TransientBounds` with:

```cpp
// The live [logStart, logEnd) of a TRANSIENT (InMemoryLog) source partition,
// handed to the per-instance fold so it can reconcile its cursor (Phase 2b E),
// plus (Phase 2b E2) the ring's IDENTITY: the owning replica's registry
// incarnation (a new replica + ring for the same partition on this node gets a
// new one) and its logGeneration with the offset the last content change cut at
// (PartitionReplica::lastTruncationOffset — a tail truncation or a follower ring
// reset).  Defaults: an unregistered (0) generation-0 ring.
struct TransientBounds {
    int64_t  logStart      = 0;
    int64_t  logEnd        = 0;
    uint32_t incarnation   = 0;
    uint64_t logGeneration = 0;
    int64_t  cutOffset     = 0;
};
```

Replace the comment block starting `// Where an instance's foldCursor came from (only consulted for a transient` through `enum class CursorState : uint8_t { Fresh, RecoveredUnreconciled, Live };` with:

```cpp
// Where an instance's foldCursor came from (only consulted for a transient
// source; a disk source never reads it).  One explicit state, each holding only
// its variables (CLAUDE.md rule 3), one path each:
//   CursorFresh                 — created empty; the first transient reconcile
//                                 starts it at the source's logStart (records
//                                 evicted BEFORE the view existed are not
//                                 "lost" — never counted).
//   CursorRecoveredUnreconciled — cursor restored from a checkpoint taken against
//                                 an EARLIER incarnation of the source.  A
//                                 transient source restarts empty and reuses
//                                 offsets, so that cursor is meaningless: the
//                                 first reconcile warns once, resets to logStart
//                                 and keeps the rows.
//   CursorLive{incarnation, logGeneration}
//                               — cursor tracks THIS source ring: re-folded from
//                                 logStart when the ring was recreated (new
//                                 incarnation), from the cut when its content
//                                 changed below the cursor (new generation);
//                                 EVICTED when it falls below logStart,
//                                 invariant breach when above logEnd.
struct CursorFresh {};
struct CursorRecoveredUnreconciled {};
struct CursorLive {
    uint32_t incarnation   = 0;
    uint64_t logGeneration = 0;
};
using TransientCursor = std::variant<CursorFresh, CursorRecoveredUnreconciled, CursorLive>;
```

In `struct ViewInstance` replace `    CursorState                        cursorState     = CursorState::Fresh;` with
`    TransientCursor                    transientCursor{CursorFresh{}};`.

In the `reconcileTransient_` declaration comment replace `(see CursorState)` with `(see TransientCursor)`.

- [ ] **Step 4: Implement — `WorkerViewSet.cpp`**

In `loadRecoveryCheckpoint_` replace `    if (recoveredO >= 0) inst.cursorState = CursorState::RecoveredUnreconciled;` with
`    if (recoveredO >= 0) inst.transientCursor = CursorRecoveredUnreconciled{};`; in `installViewForRecovery` replace `    if (recoveredO >= 0) inst->cursorState = CursorState::RecoveredUnreconciled;` with
`    if (recoveredO >= 0) inst->transientCursor = CursorRecoveredUnreconciled{};`.

Replace the whole `int WorkerViewSet::reconcileTransient_(ViewInstance& inst, const TransientBounds& tb) { … }` with:

```cpp
int WorkerViewSet::reconcileTransient_(ViewInstance& inst, const TransientBounds& tb) {
    const int32_t viewTopicId = inst.plan.viewTopicId;
    if (std::holds_alternative<CursorFresh>(inst.transientCursor)) {             // new instance: start at logStart
        inst.transientCursor = CursorLive{tb.incarnation, tb.logGeneration};
        if (inst.foldCursor == tb.logStart) return 0;
        inst.foldCursor = tb.logStart;
        return 1;
    }
    if (std::holds_alternative<CursorRecoveredUnreconciled>(inst.transientCursor)) {   // older incarnation
        spdlog::warn("WorkerViewSet: view {} partition {}: recovered checkpoint cursor {} refers to "
                     "an earlier incarnation of its transient source (now [{}, {})) -- invariant "
                     "breach (a view over a transient source must be transient); keeping the rows, "
                     "re-folding from offset {}",
                     viewTopicId, inst.partition, inst.foldCursor, tb.logStart, tb.logEnd,
                     tb.logStart);
        inst.transientCursor = CursorLive{tb.incarnation, tb.logGeneration};
        inst.foldCursor      = tb.logStart;
        return 1;
    }
    CursorLive& live = std::get<CursorLive>(inst.transientCursor);
    if (live.incarnation != tb.incarnation) [[unlikely]] {                      // SOURCE_RECREATED
        // A NEW replica + ring for this partition on this node: its offsets may
        // name different records.  Keep the rows (re-folding retained records is
        // idempotent under the keyed table semantics), re-fold from logStart.
        ++inst.counters.transientSourceResets;
        spdlog::warn("WorkerViewSet: view {} partition {}: its transient source was recreated "
                     "(replica incarnation {} -> {}) under cursor {}; keeping the rows, re-folding "
                     "from offset {} (resets={})",
                     viewTopicId, inst.partition, live.incarnation, tb.incarnation, inst.foldCursor,
                     tb.logStart, inst.counters.transientSourceResets);
        live            = CursorLive{tb.incarnation, tb.logGeneration};
        inst.foldCursor = tb.logStart;
        return 1;
    }
    int moved = 0;
    if (live.logGeneration != tb.logGeneration) [[unlikely]] {                  // SOURCE_CUT
        // The ring's existing content changed at/above cutOffset (a truncation,
        // or a follower ring reset).  Records the view folded at/above the cut
        // may differ or be gone: re-fold from the cut (or the ring start).
        live.logGeneration = tb.logGeneration;
        if (inst.foldCursor > tb.cutOffset) {
            ++inst.counters.transientSourceResets;
            const int64_t to = std::max(tb.cutOffset, tb.logStart);
            spdlog::warn("WorkerViewSet: view {} partition {}: its transient source changed at offset {} "
                         "below cursor {}; keeping the rows, re-folding from offset {} (resets={})",
                         viewTopicId, inst.partition, tb.cutOffset, inst.foldCursor, to,
                         inst.counters.transientSourceResets);
            inst.foldCursor = to;
            moved = 1;
        }
    }
    if (inst.foldCursor > tb.logEnd) {                                          // INVARIANT BREACH
        spdlog::warn("WorkerViewSet: view {} partition {}: cursor {} is above its transient "
                     "source end {} -- invariant breach (a view over a transient source must "
                     "be transient); keeping the rows, re-folding from offset {}",
                     viewTopicId, inst.partition, inst.foldCursor, tb.logEnd, tb.logStart);
        inst.foldCursor = tb.logStart;
        return 1;
    }
    if (inst.foldCursor < tb.logStart) {                                        // EVICTED
        inst.counters.transientRecordsEvicted += static_cast<uint64_t>(tb.logStart - inst.foldCursor);
        inst.foldCursor = tb.logStart;
        return 1;
    }
    return moved;
}
```

- [ ] **Step 5: Implement — `Worker.h`**

In `driveViews_`, in the transient branch replace

```cpp
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, mem);
```
with
```cpp
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, replica);
```

In `driveViewsOf_` replace the parameter `rune::storage::InMemoryLog* transientSrc` with `const rune::replication::PartitionReplica* transientRep` (update its comment: `transientRep` is the owning replica of a TRANSIENT source, nullptr for disk), and replace

```cpp
        if (transientSrc != nullptr) {
            tb.logStart = transientSrc->logStartOffset();
            tb.logEnd   = transientSrc->logEndOffset();
            tbp         = &tb;
        }
```
with
```cpp
        if (transientRep != nullptr) {
            const rune::storage::InMemoryLog* mem = transientRep->memoryLog();
            tb.logStart      = mem->logStartOffset();
            tb.logEnd        = mem->logEndOffset();
            tb.incarnation   = transientRep->incarnation();     // E2: the ring's identity
            tb.logGeneration = transientRep->logGeneration();
            tb.cutOffset     = transientRep->lastTruncationOffset();
            tbp              = &tb;
        }
```

(Zero allocation: five scalar loads on the owning thread, per pass, per transient source.)

- [ ] **Step 6: Run to verify they pass**

Run: `cmake --build build-fast -j14 && ctest --test-dir build-fast -R 'WorkerViewSet|WorkerRegression|ViewReadiness|CreateViewRegression|GetViewRow|ViewFold|TransientDataPath' --output-on-failure -j14`
Expected: all PASS (Part E's transient-view tests keep passing: their `TransientBounds{a, b}` is an unregistered generation-0 ring every pass, so no reset fires).

- [ ] **Step 7: Commit**

```bash
git add libs/node/include/rune/node/WorkerViewSet.h libs/node/src/WorkerViewSet.cpp libs/node/include/rune/node/Worker.h libs/node/tests/node/WorkerViewSetTest.cpp libs/node/tests/node/WorkerTest.cpp
git commit -m "fix(view): a live transient cursor records its source ring identity; re-fold on recreation or a cut below it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 10: Benchmark — a view folding a transient source vs a disk source (carry-over a)

The spec's "same latency class as disk" holds for the produce→subscribe path (Task 6); views fold over transient sources since Part E, but nothing measures it. This benchmark runs the REAL worker pass (`Worker::driveViews_` → `WorkerViewSet::driveFolds`): the disk source reads through `readNoFault_` (a pread into the worker's scratch), the transient source through `InMemoryLog::readView` (a zero-copy view into the ring, Task 5 / Part E).

**Files:**
- Create: `benchmark/ViewFoldSourceBench.cpp`
- Modify: `CMakeLists.txt` (root; the `rune_broker_benchmark` source list — after `benchmark/ViewCheckpointBench.cpp`, ~`:359`)

**Interfaces:**
- Consumes: `Worker::{setOwnedDirect, setViewSet, tryEnqueueOp, doWork, drainReturn}`, `WorkerViewSet::{installView, getRow}`, `rune::view::compileView`, `rune::view::readSlot`; Tasks 2-5 (the transient produce/read paths) and Task 9 (the `Live` source identity — reconciled every pass, so its cost is in the numbers).
- Produces: benchmarks `BM_ViewFoldSource_{Disk,Transient}_Latency` (counters `p50Us`, `p99Us`) and `BM_ViewFoldSource_{Disk,Transient}_Catchup` (counter `records_per_s`).

- [ ] **Step 1: Write the benchmark** — create `benchmark/ViewFoldSourceBench.cpp`:

```cpp
// benchmark/ViewFoldSourceBench.cpp — Phase 2b E2 carry-over (a): a VIEW folding
// a TRANSIENT source vs a DISK source on the REAL worker pass.
//
//   BM_ViewFoldSource_{Disk,Transient}_Latency — produce ONE keyed record (op
//     ring, acks=leader) → worker passes until the view row carries its value.
//     One sample = "a produced record is visible in the view" on one worker.
//     Counters p50Us / p99Us.
//   BM_ViewFoldSource_{Disk,Transient}_Catchup — 20 000 records already in the
//     source, the view installed afterwards (a Fresh instance folds from the
//     ring/log start) → worker passes until it folded them all.  Counter
//     records_per_s (fold throughput through the real read path).
//
// Gate (E2): Transient p50Us and p99Us <= Disk's; Transient records_per_s >=
// Disk's (the ring read is a pointer view, the disk read a pread).
#include <benchmark/benchmark.h>
#include <rune/codec/Descriptor.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;

constexpr int32_t  kSrc            = 1;
constexpr int32_t  kView           = 0x40000077;
constexpr uint64_t kKeys           = 1024;
constexpr uint64_t kBatches        = 4096;     // pre-built; key = i % kKeys, hp = i
constexpr uint64_t kCatchupRecords = 20'000;

class NullXport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {
        return true;
    }
};

std::string tempDir(const char* tag) {
    static int ctr = 0;
    const fs::path p = fs::temp_directory_path() /
                       ("rune_vfsrc_" + std::string(tag) + "_" + std::to_string(++ctr) + "_" + std::to_string(::getpid()));
    fs::create_directories(p);
    return p.string();
}

rune::codec::Descriptor charSrc() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpPlan() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp_src_bench"; v.key = "characterId"; v.sources = {{kSrc, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;   // a transient view: no checkpoint IO in the measured pass (legal over both sources)
    return v;
}

// One keyed single-record batch: key = 8-byte LE id, value = [blockLength 12][version 1][id i64][hp i32].
std::vector<uint8_t> keyedBatch(uint64_t id, int32_t hp) {
    std::string key(8, '\0');
    for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<char>(id >> (8 * i));
    std::string value;
    auto le = [&](uint64_t x, int n) { for (int i = 0; i < n; ++i) value.push_back(static_cast<char>(x >> (8 * i))); };
    le(12, 2); le(1, 2); le(id, 8); le(static_cast<uint32_t>(hp), 4);
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(key, value, 0);
    return b.build();
}

struct FoldSourceFixture {
    std::string                                 dir;
    std::unique_ptr<rune::storage::Log>         disk;   // disk source
    std::unique_ptr<rune::storage::InMemoryLog> mem;    // transient source
    NullXport                                   transport;
    std::unique_ptr<PartitionReplica>           replica;
    rune::node::ViewSnapshotChannel             ch;
    std::unique_ptr<rune::node::WorkerViewSet>  vs;
    std::unique_ptr<Worker>                     worker;
    int64_t                                     nowNs = 1;
    int64_t                                     corr  = 0;

    explicit FoldSourceFixture(bool transient) : dir(tempDir(transient ? "t" : "d")) {
        PartitionConfig cfg{};
        cfg.topicId = kSrc; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        const std::vector<rune::replication::NodeId> one{1};
        if (transient) {
            mem     = std::make_unique<rune::storage::InMemoryLog>(64u << 20, 1'000'000);
            replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, one, one,
                                                         mem.get(), &transport);
        } else {
            disk = std::make_unique<rune::storage::Log>(dir + "/log", 64LL * 1024 * 1024);
            disk->recover();
            replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, one, one,
                                                         disk.get(), &transport);
        }
        vs     = std::make_unique<rune::node::WorkerViewSet>(ch, dir + "/views");
        worker = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }
    ~FoldSourceFixture() {
        worker.reset();
        vs.reset();
        replica.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    void installView() {
        const auto src = charSrc();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(hpPlan(), srcs, kView).value();
        vs->installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        worker->setViewSet(vs.get());
    }
    /// The gateway-routed produce op for `batch` (acks=leader); zero-copy view.
    WorkerOp produceOp(const std::vector<uint8_t>& batch) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
        op.replica = replica.get(); op.logPtr = disk.get();   // transient: nullptr (the ring is the replica's)
        op.sessionId = 1; op.correlationId = ++corr;
        op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        return op;
    }
    void pass() {
        worker->doWork(nowNs++);
        worker->drainReturn([](WorkerReturn&&) {});   // the acks (keep the return ring drained)
    }
    [[nodiscard]] bool rowIs(uint64_t key, int32_t hp) const {
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* row = vs->getRow(kView, 0, key, L);
        return row != nullptr && rune::view::readSlot(row, *L, 1).i == hp;
    }
};

void reportUs(benchmark::State& state, std::vector<int64_t>& ns) {
    if (ns.empty()) return;
    std::sort(ns.begin(), ns.end());
    state.counters["p50Us"] = static_cast<double>(ns[ns.size() / 2]) / 1000.0;
    state.counters["p99Us"] = static_cast<double>(ns[std::min(ns.size() - 1, ns.size() * 99 / 100)]) / 1000.0;
}

void runLatency(benchmark::State& state, bool transient) {
    FoldSourceFixture f(transient);
    f.installView();
    std::vector<std::vector<uint8_t>> batches;
    batches.reserve(kBatches);
    for (uint64_t i = 0; i < kBatches; ++i) batches.push_back(keyedBatch(i % kKeys, static_cast<int32_t>(i)));
    // Warm-up (not timed): every key folded once — table grown, descriptor cache warm.
    for (uint64_t i = 0; i < kKeys; ++i) {
        while (!f.worker->tryEnqueueOp(f.produceOp(batches[i]))) f.pass();
        for (int k = 0; k < 1'000'000 && !f.rowIs(i, static_cast<int32_t>(i)); ++k) f.pass();
    }
    std::vector<int64_t> samples;
    samples.reserve(1 << 16);
    uint64_t i = kKeys;
    for (auto _ : state) {
        const uint64_t idx = i % kBatches;
        const WorkerOp op  = f.produceOp(batches[idx]);
        const auto t0 = std::chrono::steady_clock::now();
        while (!f.worker->tryEnqueueOp(op)) f.pass();
        bool visible = false;
        for (int k = 0; k < 5'000'000 && !visible; ++k) {
            f.pass();
            visible = f.rowIs(idx % kKeys, static_cast<int32_t>(idx));
        }
        const auto t1 = std::chrono::steady_clock::now();
        if (!visible) { state.SkipWithError("the produced record never reached the view"); break; }
        if (samples.size() < samples.capacity())
            samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        ++i;
    }
    state.SetItemsProcessed(state.iterations());
    reportUs(state, samples);
}

void runCatchup(benchmark::State& state, bool transient) {
    for (auto _ : state) {
        state.PauseTiming();
        auto f = std::make_unique<FoldSourceFixture>(transient);
        std::vector<uint8_t> batch;
        for (uint64_t r = 0; r < kCatchupRecords; ++r) {
            batch = keyedBatch(r % kKeys, static_cast<int32_t>(r));
            while (!f->worker->tryEnqueueOp(f->produceOp(batch))) f->pass();
            f->pass();   // the op ring holds a view of `batch`: execute it before the next rebuild
        }
        for (int k = 0; k < 64; ++k) f->pass();   // HW / RC state settle
        f->installView();                         // Fresh: folds from the source start
        const uint64_t lastKey = (kCatchupRecords - 1) % kKeys;
        const auto     lastHp  = static_cast<int32_t>(kCatchupRecords - 1);
        state.ResumeTiming();
        const auto t0 = std::chrono::steady_clock::now();
        bool done = false;
        for (int k = 0; k < 50'000'000 && !done; ++k) {
            f->pass();
            done = f->rowIs(lastKey, lastHp);
        }
        const auto t1 = std::chrono::steady_clock::now();
        state.PauseTiming();
        if (!done) { state.SkipWithError("the view never folded the whole source"); break; }
        const double secs = std::chrono::duration<double>(t1 - t0).count();
        state.counters["records_per_s"] = static_cast<double>(kCatchupRecords) / secs;
        f.reset();
        state.ResumeTiming();
    }
}

} // namespace

static void BM_ViewFoldSource_Disk_Latency(benchmark::State& s)      { runLatency(s, /*transient=*/false); }
static void BM_ViewFoldSource_Transient_Latency(benchmark::State& s) { runLatency(s, /*transient=*/true); }
static void BM_ViewFoldSource_Disk_Catchup(benchmark::State& s)      { runCatchup(s, false); }
static void BM_ViewFoldSource_Transient_Catchup(benchmark::State& s) { runCatchup(s, true); }

BENCHMARK(BM_ViewFoldSource_Disk_Latency)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_ViewFoldSource_Transient_Latency)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_ViewFoldSource_Disk_Catchup)->Unit(benchmark::kMillisecond)->Iterations(3);
BENCHMARK(BM_ViewFoldSource_Transient_Catchup)->Unit(benchmark::kMillisecond)->Iterations(3);
```

(If `WorkerOp::recordBatchView` is not honoured for a `Produce` op in `Worker::tryEnqueueOp` — read its encode — use `op.recordBatchBytes = batch;` and build the op before `t0`, as Task 6 does.)

In the root `CMakeLists.txt`, in `add_executable(rune_broker_benchmark …)`, replace `        benchmark/ViewCheckpointBench.cpp)` with:

```cmake
        benchmark/ViewCheckpointBench.cpp
        benchmark/ViewFoldSourceBench.cpp)
```

- [ ] **Step 2: Run the benchmark (gate)**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-bench-rel && cmake --build build-bench-rel -j14 --target rune_broker_benchmark && $(find build-bench-rel -name rune_broker_benchmark -type f | head -1) --benchmark_filter='ViewFoldSource|ViewFoldReplace' --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`
Expected: the four `ViewFoldSource` benchmarks complete without `SkipWithError`; **Transient `p50Us` and `p99Us` ≤ Disk's** (medians of the 3 repetitions) and **Transient `records_per_s` ≥ Disk's**; `BM_ViewFoldReplace` (the §14.0 fold gate: `record_p99_us < 5`, `batch_p99_us < 10`, `steady_alloc == 0`) unchanged within noise from Part E's numbers in `3c1ce059` (record_p99 0.416-0.417 µs, batch_p99 0.875-0.959 µs). If transient is slower than disk, stop and profile — a gate failure, not a tolerance to widen.

- [ ] **Step 3: Commit** (replace the text of the second `-m` with the measured Step 2 lines)

```bash
git add benchmark/ViewFoldSourceBench.cpp CMakeLists.txt
git commit -m "bench(view): fold over a transient vs a disk source on the real worker pass" \
  -m "Bench (Step 2, medians of 3): BM_ViewFoldSource_{Disk,Transient}_Latency p50Us/p99Us, _Catchup records_per_s; BM_ViewFoldReplace record_p99/batch_p99/steady_alloc." \
  -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint D (controller):** full broker `*Regression*` suite (views + benchmarks).

---
### Task 11: Interaction matrix — the expectation table + the parameterized unit matrix (1008 cells)

Implements the plan's "Interaction matrix" section at unit level: every cell of operation × entity × timing runs as one `TEST_P` instance on a deterministic gateway + 1-worker-pool rig (no Aeron), and asserts the outcome `expect()` names. `NotApplicable` cells are reported as skipped with the rule that excludes them, so the ctest output shows the honest 366 / 1008.

**Files:**
- Create: `libs/node/tests/support/TransientMatrixExpectations.h`
- Create: `libs/gateway/tests/gateway/TransientInteractionMatrixTest.cpp`
- Modify: `libs/gateway/CMakeLists.txt` (new target after `gtest_discover_tests(rune_gateway_transient_tests)`, Task 7)

**Interfaces:**
- Consumes: Tasks 1-9 (every path the cells drive); `ClientGateway::{makeForTest, setTopicRegistry, setReplicaRegistry, setCommittedMetadataReady, setOnEgressFrame, setWorkerPool, setTxnService, injectTestSession, applyProduceForTest, applySubscribeForTest, pushSubscriptionsForTest, drainWorkerReturnsForTest, addGroupSubscription, appendControlRecord, controlRecordsRefusedForTest}`, `Worker::{handleIngressFrameForTest, setWorkerIngress, setShardMode, setOwnedDirect, transientRecordsSkippedForTest, pushReadChunksForTest}`, `TopicRegistry::{createTopic, commitTopic}`, `ITxnRequestHandler::{onControlMarkerRefused, onControlMarkerLanded}`.
- Produces (namespace `rune::node::test::tmatrix`, header-only, consumed by Tasks 12-14):
  ```cpp
  enum class Op : uint8_t      { ProduceAcks0, ProduceAcks1, ProduceAcksAll, SubEarliest, SubLatest, SubOffset,
                                 GroupConsume, ReadCommitted, TxnMarker };
  enum class Persist : uint8_t { Transient, Disk };   enum class Mode : uint8_t { Shard, Legacy };
  enum class Role : uint8_t    { Leader, Follower };  enum class Commit : uint8_t { Committed, Uncommitted };
  enum class Timing : uint8_t  { Steady, RingFullUncommitted, EvictedCursor, LeaderMovesMidOp, Restart,
                                 FollowerBelowRingStart, CommitLandsMidOp };
  struct Entity { Persist persist; Mode mode; Role role; Commit commit; };
  enum class Outcome : uint8_t { Ok, OkFromRingStart, OkEmptyAfterRestart, OkAfterCommit, RetriableNotLeader,
                                 RetriableLogFull, MarkerRetried, UnansweredThenReissued, ParityWithDisk,
                                 NotApplicable };
  constexpr Outcome expect(Op, Entity, Timing) noexcept;          // rules R1-R12
  constexpr std::string_view naRule(Op, Entity, Timing) noexcept;  // why a cell is NotApplicable
  constexpr uint8_t wireAcks(Op) noexcept;  constexpr bool isProduce(Op) noexcept;  constexpr bool isRead(Op) noexcept;
  constexpr std::string_view name(Op) / name(Timing) / name(Outcome) noexcept;
  ```

- [ ] **Step 1: Write the expectation table** — create `libs/node/tests/support/TransientMatrixExpectations.h`:

```cpp
#pragma once
// TransientMatrixExpectations.h — Phase 2b E2 interaction matrix: THE single
// source of truth (plan section "Interaction matrix").  expect(op, entity,
// timing) -> Outcome, rules R1-R12 applied top-down.  The unit matrix (Task
// 11), the seeded stress (Task 12), the BrokerNode slice (Task 13) and the e2e
// leg (Task 14) read it; no test hard-codes an outcome.  Header-only,
// constexpr, no allocation.  Test-only.
#include <array>
#include <cstdint>
#include <string_view>

namespace rune::node::test::tmatrix {

enum class Op : uint8_t {
    ProduceAcks0, ProduceAcks1, ProduceAcksAll,
    SubEarliest, SubLatest, SubOffset, GroupConsume, ReadCommitted,
    TxnMarker,
};
enum class Persist : uint8_t { Transient, Disk };
enum class Mode    : uint8_t { Shard, Legacy };
enum class Role    : uint8_t { Leader, Follower };
enum class Commit  : uint8_t { Committed, Uncommitted };
enum class Timing  : uint8_t {
    Steady, RingFullUncommitted, EvictedCursor, LeaderMovesMidOp, Restart, FollowerBelowRingStart,
    CommitLandsMidOp,
};

struct Entity {
    Persist persist;
    Mode    mode;
    Role    role;
    Commit  commit;
};

enum class Outcome : uint8_t {
    Ok,                      // acked / delivered exactly the expected batches
    OkFromRingStart,         // read resumed at the ring start; skipped records counted
    OkEmptyAfterRestart,     // transient after a restart: offsets from 0, only post-restart records
    OkAfterCommit,           // C1: refused NotLeader, then Ok once the topic committed
    RetriableNotLeader,      // 2 (subscribe: bare ErrorResponse; group: -1); nothing appended / registered
    RetriableLogFull,        // 42 to a v6 session (NotLeader to v5); nothing appended; Ok after catch-up
    MarkerRetried,           // marker refused retriably; lands once the condition clears (leader cells)
    UnansweredThenReissued,  // no Records while disowned; served again once ownership returns
    ParityWithDisk,          // the transient topic's answer + delivery == the disk topic's
    NotApplicable,
};

inline constexpr std::array<Op, 9> kOps{Op::ProduceAcks0, Op::ProduceAcks1, Op::ProduceAcksAll, Op::SubEarliest,
                                        Op::SubLatest, Op::SubOffset, Op::GroupConsume, Op::ReadCommitted,
                                        Op::TxnMarker};
inline constexpr std::array<Timing, 7> kTimings{Timing::Steady, Timing::RingFullUncommitted, Timing::EvictedCursor,
                                                Timing::LeaderMovesMidOp, Timing::Restart,
                                                Timing::FollowerBelowRingStart, Timing::CommitLandsMidOp};

[[nodiscard]] constexpr bool isProduce(Op o) noexcept {
    return o == Op::ProduceAcks0 || o == Op::ProduceAcks1 || o == Op::ProduceAcksAll;
}
[[nodiscard]] constexpr bool isRead(Op o) noexcept { return !isProduce(o) && o != Op::TxnMarker; }
/// The Produce `acks` byte: 0 = Leader/None (the client decides whether it waits), 1 = All.
[[nodiscard]] constexpr uint8_t wireAcks(Op o) noexcept { return o == Op::ProduceAcksAll ? 1 : 0; }

[[nodiscard]] constexpr Outcome leaderMoves_(Op op) noexcept {   // R9
    if (isProduce(op)) return Outcome::RetriableNotLeader;
    if (op == Op::TxnMarker) return Outcome::MarkerRetried;
    return Outcome::UnansweredThenReissued;
}

[[nodiscard]] constexpr Outcome expect(Op op, Entity e, Timing t) noexcept {
    const bool marker = op == Op::TxnMarker;
    if (e.commit == Commit::Uncommitted) {                                           // R1-R3
        if (t == Timing::Steady)           return marker ? Outcome::NotApplicable : Outcome::RetriableNotLeader;
        if (t == Timing::CommitLandsMidOp)                                           // a follower then answers as R7
            return marker || e.role == Role::Follower ? Outcome::NotApplicable : Outcome::OkAfterCommit;
        return Outcome::NotApplicable;
    }
    if (t == Timing::CommitLandsMidOp) return Outcome::NotApplicable;                // R3
    const bool ringOnly = t == Timing::RingFullUncommitted || t == Timing::EvictedCursor ||
                          t == Timing::FollowerBelowRingStart;
    if (e.persist == Persist::Disk && ringOnly) return Outcome::NotApplicable;      // R4
    if (e.role == Role::Follower) {
        if (ringOnly) return Outcome::NotApplicable;                                 // R5
        if (t == Timing::LeaderMovesMidOp) return leaderMoves_(op);                  // R6
        if (isProduce(op)) return Outcome::RetriableNotLeader;                       // R7
        return marker ? Outcome::MarkerRetried : Outcome::ParityWithDisk;
    }
    switch (t) {
    case Timing::Steady: return Outcome::Ok;                                         // R8
    case Timing::LeaderMovesMidOp: return leaderMoves_(op);                          // R9
    case Timing::RingFullUncommitted:                                                // R10
        if (isProduce(op)) return Outcome::RetriableLogFull;
        return marker ? Outcome::MarkerRetried : Outcome::Ok;
    case Timing::EvictedCursor:                                                      // R11
    case Timing::FollowerBelowRingStart:
        if (isProduce(op) || marker || op == Op::SubLatest) return Outcome::Ok;
        return Outcome::OkFromRingStart;
    case Timing::Restart:                                                            // R12
        return e.persist == Persist::Transient ? Outcome::OkEmptyAfterRestart : Outcome::Ok;
    case Timing::CommitLandsMidOp: break;
    }
    return Outcome::NotApplicable;
}

[[nodiscard]] constexpr std::string_view naRule(Op op, Entity e, Timing t) noexcept {
    if (e.commit == Commit::Uncommitted) {
        if (op == Op::TxnMarker && (t == Timing::Steady || t == Timing::CommitLandsMidOp))
            return "R1/R2: no transaction can include a partition whose produce was refused";
        if (t == Timing::CommitLandsMidOp) return "R2: after the commit a follower answers as R7 (Committed rows)";
        return "R3: this timing needs a serving topic";
    }
    if (t == Timing::CommitLandsMidOp) return "R3: the topic is already committed";
    if (e.persist == Persist::Disk) return "R4: a disk log has no ring (retention: CP-5.2; follower: ruling 4)";
    return "R5: a leader-side ring state (the lagging follower is Tasks 3 and 12)";
}

[[nodiscard]] constexpr std::string_view name(Op o) noexcept {
    constexpr std::array<std::string_view, 9> n{"ProduceAcks0", "ProduceAcks1", "ProduceAcksAll", "SubEarliest",
                                                "SubLatest", "SubOffset", "GroupConsume", "ReadCommitted",
                                                "TxnMarker"};
    return n[static_cast<std::size_t>(o)];
}
[[nodiscard]] constexpr std::string_view name(Timing t) noexcept {
    constexpr std::array<std::string_view, 7> n{"Steady", "RingFullUncommitted", "EvictedCursor",
                                                "LeaderMovesMidOp", "Restart", "FollowerBelowRingStart",
                                                "CommitLandsMidOp"};
    return n[static_cast<std::size_t>(t)];
}
[[nodiscard]] constexpr std::string_view name(Outcome o) noexcept {
    constexpr std::array<std::string_view, 10> n{"Ok", "OkFromRingStart", "OkEmptyAfterRestart", "OkAfterCommit",
                                                 "RetriableNotLeader", "RetriableLogFull", "MarkerRetried",
                                                 "UnansweredThenReissued", "ParityWithDisk", "NotApplicable"};
    return n[static_cast<std::size_t>(o)];
}

} // namespace rune::node::test::tmatrix
```

- [ ] **Step 2: Write the unit matrix** — create `libs/gateway/tests/gateway/TransientInteractionMatrixTest.cpp`:

```cpp
// TransientInteractionMatrixTest.cpp — Phase 2b E2 interaction matrix, unit
// level: EVERY cell of operation (9) × entity (persistence × worker mode × role
// × commit state = 16) × timing (7) = 1008 TEST_P instances on a deterministic
// gateway + 1-worker-pool rig (no Aeron).  Each cell asserts the outcome
// support/TransientMatrixExpectations.h names; NotApplicable cells are skipped
// with the rule that excludes them.  Topic 0 "tt" is TRANSIENT, topic 1 "dt" is
// DISK; a cell drives the one its entity names (ParityWithDisk drives both).
// Shard = produce on the worker's ingress; Legacy = produce / markers through
// the gateway's op ring.  Reads go through the gateway's PushRead path in both
// modes (the worker read core both modes share; the shard owned-subscription
// push loop is Task 13, over Aeron).
#include "support/TransientMatrixExpectations.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tm = rune::node::test::tmatrix;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::gateway::ClientGateway;
using rune::gateway::TopicRegistry;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using tm::Op;
using tm::Outcome;
using tm::Timing;

namespace {

constexpr int32_t kTT  = 0;    // "tt" — transient
constexpr int32_t kDT  = 1;    // "dt" — disk
constexpr int64_t kPid = 77;
constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);

struct Cell {
    Op         op;
    tm::Entity e;
    Timing     t;
};

std::vector<Cell> allCells() {
    std::vector<Cell> out;
    for (Op op : tm::kOps)
        for (tm::Persist p : {tm::Persist::Transient, tm::Persist::Disk})
            for (tm::Mode m : {tm::Mode::Shard, tm::Mode::Legacy})
                for (tm::Role r : {tm::Role::Leader, tm::Role::Follower})
                    for (tm::Commit c : {tm::Commit::Committed, tm::Commit::Uncommitted})
                        for (Timing t : tm::kTimings) out.push_back(Cell{op, tm::Entity{p, m, r, c}, t});
    return out;
}

std::string cellName(const testing::TestParamInfo<Cell>& i) {
    const Cell& c = i.param;
    return std::string(tm::name(c.op)) + "_" + (c.e.persist == tm::Persist::Transient ? "Transient" : "Disk") +
           "_" + (c.e.mode == tm::Mode::Shard ? "Shard" : "Legacy") + "_" +
           (c.e.role == tm::Role::Leader ? "Leader" : "Follower") + "_" +
           (c.e.commit == tm::Commit::Committed ? "Committed" : "Uncommitted") + "_" + std::string(tm::name(c.t));
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-tmatrix-" + std::to_string(rd()) + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

/// Captures the leader's FetchResponses (FollowerBelowRingStart asserts the answer).
class CaptureTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg& r) override {
        ++responses;
        lastLogStart     = r.logStartOffset;
        lastRecordsBytes = r.records.size();
        return true;
    }
    int         responses        = 0;
    int64_t     lastLogStart     = -1;
    std::size_t lastRecordsBytes = 0;
};

/// The transaction coordinator's view of markers (refused / landed).
class MarkerSpy final : public rune::gateway::ITxnRequestHandler {
public:
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    void onControlMarkerRefused(int32_t, int32_t, int64_t, int16_t ec, int32_t) override {
        ++refused;
        lastRefused = ec;
    }
    void onControlMarkerLanded(int32_t, int32_t, int64_t, int32_t) override { ++landed; }
    int     refused     = 0;
    int     landed      = 0;
    int16_t lastRefused = 0;
};

std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes, char fill, int64_t pid = -1, int32_t seq = -1,
                             bool txn = false, int64_t base = 0) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    const std::string v(valueBytes, fill);
    for (int i = 0; i < recs; ++i) b.append("k" + std::to_string(i), v, 1000 + i);
    return b.build();
}

/// One acked batch of the cell's topic (the delivery model).
struct Acked {
    int64_t base;
    int32_t count;
};

// ---------------------------------------------------------------------------
// The rig: gateway + topic registry (tt, dt) + replica registry + 1-worker pool.
// Built on `dataDir` so a Restart cell rebuilds it on the same disk data.
// ---------------------------------------------------------------------------
struct Rig {
    Cell                                        c;
    fs::path                                    dataDir;
    std::unique_ptr<rune::storage::InMemoryLog> mem;          // tt's ring — new on every (re)build
    CaptureTransport                            transport;
    rune::node::ReplicaRegistry                 reg;
    std::unique_ptr<TopicRegistry>              topics;
    std::unique_ptr<rune::node::WorkerPool>     pool;
    MarkerSpy                                   spy;
    ClientGateway                               gw;
    PartitionReplica*                           rep[2] = {nullptr, nullptr};
    int64_t                                     sid = 0, sidV5 = 0;
    int64_t                                     now = 1'000'000'000, corr = 0;
    std::map<int64_t, int16_t>                  codes;        // corr -> answer code (Produce / Subscribe / Error)
    std::map<int64_t, int64_t>                  bases;        // corr -> ProduceResponse.baseOffset
    std::map<int64_t, int64_t>                  subIds;       // subscribe corr -> subscriptionId
    std::vector<std::pair<int64_t, int64_t>>    pushed;       // (subscriptionId, baseOffset)

    Rig(const Cell& cell, const fs::path& dir)
        : c(cell), dataDir(dir),
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n);
              return 1;
          }, 1)) {
        const bool smallRing = c.t == Timing::RingFullUncommitted || c.t == Timing::EvictedCursor ||
                               c.t == Timing::FollowerBelowRingStart;
        mem = std::make_unique<rune::storage::InMemoryLog>(smallRing ? 16u * 1024 : 1u << 20, 100'000);
        rune::node::TopicSpec tt;
        tt.name = "tt"; tt.partitionCount = 1; tt.topicId = kTT;
        tt.persistence = rune::node::PersistenceMode::Transient;
        rune::node::TopicSpec dt;
        dt.name = "dt"; dt.partitionCount = 1; dt.topicId = kDT;
        const bool runtime = c.e.commit == tm::Commit::Uncommitted;
        if (runtime) {
            topics = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dataDir.string());
            EXPECT_EQ(topics->createTopic(tt), kTT);   // C1: awaiting the catalog commit
            EXPECT_EQ(topics->createTopic(dt), kDT);
        } else {
            topics = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{tt, dt}, dataDir.string());
        }
        pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1));
        pool->setRegistry(&reg);
        if (!runtime) { addReplica(kTT); addReplica(kDT); }   // C1: replicas only once committed
        pool->assignFromRegistry(reg);
        if (c.e.mode == tm::Mode::Shard) {
            pool->worker(0).setWorkerIngress(nullptr, /*active=*/true);
            pool->worker(0).setShardMode(true);
        }
        gw.setTopicRegistry(topics.get());
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setWorkerPool(pool.get());
        gw.setTxnService(&spy);
        sid   = gw.injectTestSession(0);                       // the current client (v6)
        sidV5 = gw.injectTestSession(0, /*clientVersion=*/5);
        pass(4);
    }

    [[nodiscard]] int32_t topic() const { return c.e.persist == tm::Persist::Transient ? kTT : kDT; }
    [[nodiscard]] PartitionReplica& replica() const { return *rep[topic()]; }

    void addReplica(int32_t topicId) {
        PartitionConfig pcfg;
        pcfg.topicId = topicId; pcfg.partitionId = 0; pcfg.selfId = 1; pcfg.minIsr = 1;
        pcfg.replicaLagMaxOffset = 1'000'000'000; pcfg.replicaLagMaxMs = 600'000; pcfg.followerFlushIntervalNs = 0;
        const bool follower = c.e.role == tm::Role::Follower;
        std::vector<int32_t> all{1}, isr{1};
        if (follower || c.t == Timing::RingFullUncommitted) { all = {1, 2}; isr = {1, 2}; }
        if (c.t == Timing::FollowerBelowRingStart) { all = {1, 2}; isr = {1}; }
        const auto role = follower ? rune::replication::Role::Follower : rune::replication::Role::Leader;
        std::unique_ptr<PartitionReplica> r =
            topicId == kTT
                ? std::make_unique<PartitionReplica>(pcfg, role, 1, all, isr, mem.get(), &transport)
                : std::make_unique<PartitionReplica>(pcfg, role, 1, all, isr, topics->log(kDT, 0), &transport);
        r->setExternalFollowerFlush(true);
        rep[topicId] = r.get();
        reg.add(topicId, 0, std::move(r));
    }
    /// What BrokerNode's kA2cTopicCommitted handler does (C1): servable + replicas built.
    void commitTopics() {
        ASSERT_TRUE(topics->commitTopic(kTT));
        ASSERT_TRUE(topics->commitTopic(kDT));
        addReplica(kTT);
        addReplica(kDT);
        pool->assignFromRegistry(reg);
        pass(4);
    }

    void onFrame(const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::ProduceResponse:
            if (auto r = pc::ProduceResponse::decode(b, n)) { codes.emplace(r->correlationId, r->errorCode);
                                                              bases.emplace(r->correlationId, r->baseOffset); }
            break;
        case pc::TemplateId::SubscribeResponse:
            if (auto r = pc::SubscribeResponse::decode(b, n)) { codes.emplace(r->correlationId, r->errorCode);
                                                                subIds.emplace(r->correlationId, r->subscriptionId); }
            break;
        case pc::TemplateId::ErrorResponse:
            if (auto r = pc::ErrorResponse::decode(b, n)) codes.emplace(r->correlationId, r->code);
            break;
        case pc::TemplateId::Records:
            if (auto r = pc::Records::decode(b, n)) pushed.emplace_back(r->subscriptionId, r->baseOffset);
            break;
        default: break;
        }
    }
    void pass(int n = 1) {
        for (int i = 0; i < n; ++i) {
            now += 1'000'000;
            (void)pool->worker(0).doWork(now);
            gw.drainWorkerReturnsForTest(now);
        }
    }

    // ---- operations ---------------------------------------------------------
    int64_t produceTo(int32_t topicId, uint8_t acks, int recs, std::size_t valueBytes, char fill, int64_t session,
                      int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        const auto batch = batchOf(recs, valueBytes, fill, pid, seq, txn);
        pc::Produce m;
        m.sessionId = session; m.correlationId = ++corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = acks; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        if (c.e.mode == tm::Mode::Shard) (void)pool->worker(0).handleIngressFrameForTest(f.data(), f.size(), now);
        else gw.applyProduceForTest(f.data(), f.size());
        return m.correlationId;
    }
    int64_t produce(uint8_t acks, int recs = 2, std::size_t valueBytes = 8, char fill = 'v', int64_t session = 0,
                    int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        return produceTo(topic(), acks, recs, valueBytes, fill, session != 0 ? session : sid, pid, seq, txn);
    }
    /// The answer to `corr` (bounded passes); -1 = unanswered.
    int16_t answer(int64_t c0, int passes = 64) {
        for (int i = 0; i < passes && !codes.count(c0); ++i) pass();
        const auto it = codes.find(c0);
        return it == codes.end() ? int16_t{-1} : it->second;
    }
    /// Produce until OK (retriable answers retried: the RC state builds for an idempotent batch).
    std::optional<int64_t> produceOk(uint8_t acks, int recs = 2, std::size_t valueBytes = 8, char fill = 'v',
                                     int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        for (int a = 0; a < 64; ++a) {
            const int64_t k = produce(acks, recs, valueBytes, fill, 0, pid, seq, txn);
            if (answer(k) == kOk) return bases.at(k);
        }
        return std::nullopt;
    }
    int64_t subscribeTo(int32_t topicId, int64_t from, uint8_t isolation) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = ++corr; s.topicId = topicId; s.partition = 0;
        s.partitionKnown = 1; s.fromOffset = from; s.isolation = isolation;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
        return s.correlationId;
    }
    int64_t subscribe(int64_t from, uint8_t isolation = 0) { return subscribeTo(topic(), from, isolation); }
    /// Push cycles; returns the bases delivered to `subId` so far (in order).
    std::vector<int64_t> push(int64_t subId, int cycles = 32) {
        for (int i = 0; i < cycles; ++i) { gw.pushSubscriptionsForTest(); pass(); }
        std::vector<int64_t> out;
        for (const auto& [s, b] : pushed) if (s == subId) out.push_back(b);
        return out;
    }
    int64_t marker(bool commit = true) {
        const auto m = rune::transaction::ControlMarker::build(
            kPid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        return gw.appendControlRecord(topic(), 0, std::span<const uint8_t>(m), kPid, commit, /*coordinatorEpoch=*/0);
    }
    /// Follower 2's FetchRequest for the cell's partition, as the transport hands it to the owning worker.
    void fetch(int64_t from) {
        rune::node::WorkerOp op;
        op.type = rune::node::WorkerOp::Type::FetchRequest; op.topicId = topic(); op.partition = 0;
        op.fetchRequestPayload.leaderEpoch = 1;
        op.fetchRequestPayload.partition   = 0;
        op.fetchRequestPayload.followerId  = 2;
        op.fetchRequestPayload.fetchOffset = from;
        op.fetchRequestPayload.maxBytes    = 512 * 1024;
        op.fetchRequestPayload.topicId     = topic();
        op.fetchRequestPayload.nowNs       = now;
        ASSERT_TRUE(pool->tryEnqueueOp(topic(), 0, std::move(op)));
        pass(4);
    }
    void disown() { pool->worker(0).setOwnedDirect({}); }
    void reown() {
        rune::node::Worker::OwnedSet s;
        for (PartitionReplica* r : rep) if (r != nullptr) s.push_back(r);
        pool->worker(0).setOwnedDirect(std::move(s));
    }
    [[nodiscard]] int64_t leo() const { return replica().logEndOffset(); }
    [[nodiscard]] int64_t hw() const { return replica().highWatermark(); }
    [[nodiscard]] int64_t ringStart() const { return mem->logStartOffset(); }
    [[nodiscard]] uint64_t skipped() { return pool->worker(0).transientRecordsSkippedForTest(); }
};

/// Two identical batches (offsets 0..3) replicated into a FOLLOWER replica.
void seedFollower(PartitionReplica& r, int32_t topicId) {
    auto b0 = batchOf(2, 8, 'v', -1, -1, false, 0);
    auto b1 = batchOf(2, 8, 'v', -1, -1, false, 2);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(b0.data(), 1);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(b1.data(), 1);
    std::vector<uint8_t> both = b0;
    both.insert(both.end(), b1.begin(), b1.end());
    rune::replication::FetchResponseMsg m{};
    m.leaderEpoch = 1; m.partition = 0; m.highWatermark = 4; m.logStartOffset = 0; m.errorCode = 0;
    m.topicId = topicId; m.records = std::span<const uint8_t>(both);
    r.applyReplicated(m);
}

/// Every batch of `hist` a read from `start` (whole-batch) delivers below `bound`, in order.
std::vector<int64_t> expectedBases(const std::vector<Acked>& hist, int64_t start, int64_t bound) {
    std::vector<int64_t> out;
    for (const Acked& a : hist)
        if (a.base + a.count > start && a.base + a.count <= bound) out.push_back(a.base);
    return out;
}

// ---------------------------------------------------------------------------
// The driver: owns the data dir (survives a Restart), seeds per timing, runs
// the op, asserts the expected outcome.
// ---------------------------------------------------------------------------
struct Driver {
    Cell                 c;
    TempDir              dir;
    std::unique_ptr<Rig> rig;
    std::vector<Acked>   hist;   // acked plain batches of the cell's topic (current ring / log)

    explicit Driver(const Cell& cell) : c(cell) { rig = std::make_unique<Rig>(c, dir.path); }

    void restart() {
        rig.reset();
        rig = std::make_unique<Rig>(c, dir.path);
        if (c.e.persist == tm::Persist::Transient) hist.clear();   // a new, empty ring
    }
    void ackPlain(uint8_t acks, int recs, std::size_t valueBytes = 8) {
        const auto b = rig->produceOk(acks, recs, valueBytes);
        ASSERT_TRUE(b.has_value()) << "seeding produce refused";
        hist.push_back(Acked{*b, recs});
    }
    /// The timing's preconditions (data + ring state).
    void seed() {
        Rig& r = *rig;
        if (c.e.commit == tm::Commit::Uncommitted) return;
        if (c.e.role == tm::Role::Follower) {
            seedFollower(*r.rep[kTT], kTT);
            seedFollower(*r.rep[kDT], kDT);
            r.pass(4);
            return;
        }
        switch (c.t) {
        case Timing::RingFullUncommitted: {
            if (c.op == Op::TxnMarker) ASSERT_TRUE(r.produceOk(0, 1, 8, 'v', kPid, 0, /*txn=*/true).has_value());
            int16_t code = kOk;
            for (int i = 0; i < 64 && code == kOk; ++i) code = r.answer(r.produce(0, 1, 1000));
            ASSERT_EQ(code, kTransientLogFull) << "precondition: the ring is full of uncommitted data";
            ASSERT_EQ(r.hw(), 0);
            break;
        }
        case Timing::EvictedCursor:
        case Timing::FollowerBelowRingStart:
            for (int i = 0; i < 40; ++i) ackPlain(0, 1, 1000);
            ASSERT_GT(r.ringStart(), 0) << "precondition: the ring evicted committed records";
            if (c.op == Op::TxnMarker) ASSERT_TRUE(r.produceOk(0, 1, 8, 'v', kPid, 0, true).has_value());
            break;
        default:   // Steady, LeaderMovesMidOp, Restart
            // An OPEN transaction only where the cell commits it; a Restart cell seeds plain
            // batches (a disk log would keep an open txn pinning the LSO across the restart).
            if ((c.op == Op::ReadCommitted || c.op == Op::TxnMarker) && c.t != Timing::Restart) {
                ASSERT_TRUE(r.produceOk(0, 2, 8, 'v', kPid, 0, /*txn=*/true).has_value());   // open txn at 0..1
                if (c.op == Op::ReadCommitted) ackPlain(0, 2);                               // plain at 2..3
            } else {
                ackPlain(0, 2);
                ackPlain(0, 2);
            }
            break;
        }
        r.pass(4);
    }

    /// The read op's subscribe (or group) → {answer code, subscriptionId}.
    std::pair<int16_t, int64_t> openRead(int32_t topicId) {
        Rig& r = *rig;
        if (c.op == Op::GroupConsume) {
            const int64_t s = r.gw.addGroupSubscription(r.sid, topicId, 0, /*fromOffset=*/2, 0, false);
            return {s >= 0 ? kOk : kNotLeader, s};
        }
        const int64_t from = c.op == Op::SubEarliest || c.op == Op::ReadCommitted ? -2
                           : c.op == Op::SubLatest ? -1 : 3;
        const int64_t k = r.subscribeTo(topicId, from, c.op == Op::ReadCommitted ? 1 : 0);
        const int16_t code = r.answer(k, 4);
        return {code, r.subIds.count(k) ? r.subIds.at(k) : -1};
    }
    /// The start offset the gateway resolves for the read op (its clamp: [0, HW]).
    [[nodiscard]] int64_t resolvedStart(int64_t hwAtOpen) const {
        switch (c.op) {
        case Op::SubLatest:    return hwAtOpen;
        case Op::SubOffset:    return std::min<int64_t>(3, hwAtOpen);
        case Op::GroupConsume: return std::min<int64_t>(2, hwAtOpen);
        default:               return 0;
        }
    }

    void run(Outcome want) {
        if (c.t == Timing::Restart && c.e.role == tm::Role::Follower) {   // R7 after a restart
            restart();
            seedFollower(*rig->rep[kTT], kTT);
            seedFollower(*rig->rep[kDT], kDT);
            rig->pass(4);
        }
        Rig* r = rig.get();
        switch (want) {
        case Outcome::NotApplicable: return;

        case Outcome::RetriableNotLeader: {
            if (c.t == Timing::LeaderMovesMidOp) r->disown();
            if (tm::isProduce(c.op)) {
                const int64_t before = c.e.commit == tm::Commit::Committed ? r->leo() : 0;
                EXPECT_EQ(r->answer(r->produce(tm::wireAcks(c.op))), kNotLeader);
                if (c.e.commit == tm::Commit::Committed) EXPECT_EQ(r->leo(), before) << "a refused produce appended";
                else EXPECT_EQ(r->mem->logEndOffset(), 0) << "an uncommitted topic was appended to";
                if (c.t == Timing::LeaderMovesMidOp) {
                    r->reown();
                    const int16_t retry = r->answer(r->produce(tm::wireAcks(c.op)));
                    EXPECT_EQ(retry, c.e.role == tm::Role::Leader ? kOk : kNotLeader) << "the retry on the owner";
                }
            } else {   // Uncommitted × Steady reads (R1)
                const auto [code, subId] = openRead(r->topic());
                EXPECT_EQ(code, kNotLeader) << "an uncommitted topic registered a subscription";
                EXPECT_LT(subId, 0);
            }
            return;
        }

        case Outcome::OkAfterCommit: {   // R2 (C1): refused, commit lands, the retry serves
            if (tm::isProduce(c.op)) {
                EXPECT_EQ(r->answer(r->produce(tm::wireAcks(c.op))), kNotLeader);
                r->commitTopics();
                const int64_t k = r->produce(tm::wireAcks(c.op));
                EXPECT_EQ(r->answer(k), kOk);
                EXPECT_EQ(r->bases.at(k), 0) << "nothing was appended while uncommitted";
            } else {
                EXPECT_EQ(openRead(r->topic()).first, kNotLeader);
                r->commitTopics();
                hist.clear();
                ackPlain(0, 2);
                ackPlain(0, 2);
                const auto [code, subId] = openRead(r->topic());
                ASSERT_EQ(code, kOk);
                const int64_t start = resolvedStart(r->hw());
                EXPECT_EQ(r->push(subId), expectedBases(hist, start, r->hw()));
            }
            return;
        }

        case Outcome::RetriableLogFull: {   // R10 produce: 42 (v6) / NotLeader (v5), nothing appended
            const int64_t leo = r->leo();
            EXPECT_EQ(r->answer(r->produce(tm::wireAcks(c.op), 1, 1000)), kTransientLogFull);
            EXPECT_EQ(r->answer(r->produce(tm::wireAcks(c.op), 1, 1000, 'v', r->sidV5)), kNotLeader)
                << "a v5 session must never see 42";
            EXPECT_EQ(r->leo(), leo) << "a refused produce appended";
            EXPECT_EQ(r->ringStart(), 0) << "an unreplicated record was evicted";
            r->fetch(r->leo());   // follower 2 catches up: HW = LEO
            ASSERT_EQ(r->hw(), r->leo());
            const int64_t k = r->produce(tm::wireAcks(c.op), 1, 1000);
            r->pass(2);
            r->fetch(r->leo());   // acks=all: follower 2 acknowledges the retry too
            EXPECT_EQ(r->answer(k, 256), kOk) << "the retry after catch-up";
            return;
        }

        case Outcome::MarkerRetried: {
            const int64_t leo = r->leo();
            if (c.t == Timing::LeaderMovesMidOp) {   // R9: refused stale, re-routed by the gateway, lands once
                const uint64_t refusedBefore = r->gw.controlRecordsRefusedForTest();
                r->disown();
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(2);
                EXPECT_GT(r->gw.controlRecordsRefusedForTest(), refusedBefore) << "the stale marker was not refused";
                r->reown();
                r->pass(16);
                if (c.e.role == tm::Role::Leader) {
                    EXPECT_EQ(r->spy.landed, 1) << "the re-routed marker did not land exactly once";
                    EXPECT_EQ(r->leo(), leo + 1);
                } else {
                    EXPECT_EQ(r->spy.landed, 0);
                    EXPECT_GE(r->spy.refused, 1) << "a follower must refuse the marker retriably";
                    EXPECT_EQ(r->spy.lastRefused, kNotLeader);
                }
                return;
            }
            EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
            r->pass(8);
            EXPECT_EQ(r->spy.landed, 0);
            ASSERT_GE(r->spy.refused, 1) << "the refused marker was not surfaced to the coordinator";
            EXPECT_EQ(r->leo(), leo) << "a refused marker appended";
            if (c.e.role == tm::Role::Follower) {     // R7: refused while a follower
                EXPECT_EQ(r->spy.lastRefused, kNotLeader);
                return;
            }
            EXPECT_EQ(r->spy.lastRefused, kTransientLogFull);   // R10: a full ring
            r->fetch(r->leo());                                 // the follower catches up
            EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);   // the coordinator's retry
            r->pass(16);
            r->fetch(r->leo());                                 // the marker itself commits (D9 MAJOR-3)
            r->pass(8);
            EXPECT_EQ(r->spy.landed, 1);
            EXPECT_EQ(r->leo(), leo + 1);
            return;
        }

        case Outcome::UnansweredThenReissued: {   // R9 reads
            const auto [code, subId] = openRead(r->topic());
            if (c.e.role == tm::Role::Leader) ASSERT_EQ(code, kOk);
            else if (subId < 0) return;   // a follower that refuses the subscribe has nothing to re-issue
                                          // (that answer is the ParityWithDisk cells' subject)
            const int64_t hwAtOpen = r->hw();
            const uint64_t chunks = r->pool->worker(0).pushReadChunksForTest();
            r->disown();
            EXPECT_TRUE(r->push(subId, 8).empty()) << "Records pushed for a partition this worker does not own";
            EXPECT_EQ(r->pool->worker(0).pushReadChunksForTest(), chunks) << "read a ring it does not own";
            const uint64_t readsWhileDisowned = r->pool->worker(0).pushReadsForTest();   // stale reads never execute
            r->reown();
            const auto got = r->push(subId);
            if (c.e.role == tm::Role::Leader && c.op != Op::ReadCommitted) {
                if (c.op == Op::SubLatest) EXPECT_TRUE(got.empty());
                else EXPECT_EQ(got, expectedBases(hist, resolvedStart(hwAtOpen), r->hw()));
            }
            EXPECT_GT(r->pool->worker(0).pushReadsForTest(), readsWhileDisowned)
                << "the read was never re-issued after ownership returned";
            return;
        }

        case Outcome::ParityWithDisk: {   // R7 follower reads: transient == disk
            std::vector<int64_t> got[2];
            int16_t code[2] = {-1, -1};
            for (int32_t t : {kTT, kDT}) {
                const auto [cd, subId] = openRead(t);
                code[t] = cd;
                if (subId >= 0) got[t] = r->push(subId);
            }
            EXPECT_EQ(code[kTT], code[kDT]) << "a follower's transient answer differs from its disk twin";
            EXPECT_EQ(got[kTT], got[kDT]) << "a follower's transient delivery differs from its disk twin";
            return;
        }

        case Outcome::OkFromRingStart: {   // R11 reads below the ring start
            if (c.t == Timing::FollowerBelowRingStart) {
                const int before = r->transport.responses;
                r->fetch(0);
                ASSERT_GT(r->transport.responses, before) << "a fetch below the ring start was never answered";
                EXPECT_EQ(r->transport.lastLogStart, r->ringStart());
                EXPECT_EQ(r->transport.lastRecordsBytes, 0u);
            }
            const uint64_t skippedBefore = r->skipped();
            const auto [code, subId] = openRead(r->topic());
            ASSERT_EQ(code, kOk);
            const int64_t start = r->ringStart();
            const auto got = r->push(subId, 64);
            ASSERT_FALSE(got.empty()) << "a cursor below the ring start stalled";
            EXPECT_EQ(got.front(), start) << "did not resume at the ring start";
            EXPECT_EQ(got, expectedBases(hist, start, r->hw()));
            EXPECT_EQ(r->skipped() - skippedBefore, static_cast<uint64_t>(start - resolvedStart(r->hw())))
                << "skipped records were not counted exactly";
            return;
        }

        case Outcome::OkEmptyAfterRestart:
        case Outcome::Ok: {
            if (c.t == Timing::FollowerBelowRingStart) {
                r->fetch(0);
                EXPECT_EQ(r->transport.lastLogStart, r->ringStart());
            }
            if (c.t == Timing::Restart) {
                restart();
                r = rig.get();
                if (want == Outcome::OkEmptyAfterRestart) EXPECT_EQ(r->leo(), 0) << "a transient topic kept data";
                if (!tm::isProduce(c.op) && c.op != Op::TxnMarker) ackPlain(0, 2);   // post-restart data
            }
            if (tm::isProduce(c.op)) {
                const int64_t expectBase = r->leo();
                const int64_t k = r->produce(tm::wireAcks(c.op));
                ASSERT_EQ(r->answer(k), kOk);
                EXPECT_EQ(r->bases.at(k), expectBase);
                if (c.op == Op::ProduceAcksAll) EXPECT_GE(r->hw(), expectBase + 2) << "acks=all acked above the HW";
                return;
            }
            if (c.op == Op::TxnMarker) {
                if (c.t == Timing::Restart)
                    ASSERT_TRUE(r->produceOk(0, 1, 8, 'v', kPid, 0, true).has_value());   // a post-restart txn
                const int64_t leo = r->leo();
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(16);
                EXPECT_EQ(r->spy.landed, 1);
                EXPECT_EQ(r->spy.refused, 0);
                EXPECT_EQ(r->leo(), leo + 1);
                return;
            }
            if (c.op == Op::ReadCommitted && (c.t == Timing::Steady || c.t == Timing::LeaderMovesMidOp)) {
                const auto [code, subId] = openRead(r->topic());
                ASSERT_EQ(code, kOk);
                EXPECT_TRUE(r->push(subId).empty()) << "an OPEN transaction leaked to read_committed";
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(16);
                const auto got = r->push(subId);
                EXPECT_EQ(got, (std::vector<int64_t>{0, 2})) << "committed txn + plain batch, never the marker";
                return;
            }
            const auto [code, subId] = openRead(r->topic());
            ASSERT_EQ(code, kOk);
            const int64_t hwAtOpen = r->hw();
            if (c.t == Timing::RingFullUncommitted) {
                EXPECT_TRUE(r->push(subId).empty()) << "delivered past the HW (0)";
                r->fetch(r->leo());   // the follower catches up
                const auto got = r->push(subId, 64);
                ASSERT_FALSE(got.empty());
                EXPECT_EQ(got.front(), resolvedStart(hwAtOpen)) << "a record was evicted before it committed";
                return;
            }
            if (c.op == Op::SubLatest) {
                EXPECT_TRUE(r->push(subId).empty()) << "Latest replayed old records";
                ackPlain(0, 2);
                EXPECT_EQ(r->push(subId), (std::vector<int64_t>{hist.back().base}));
                return;
            }
            EXPECT_EQ(r->push(subId), expectedBases(hist, resolvedStart(hwAtOpen), r->hw()));
            return;
        }
        }
    }
};

class TransientMatrixRegression : public testing::TestWithParam<Cell> {};

TEST_P(TransientMatrixRegression, Cell) {
    const Cell c = GetParam();
    const Outcome want = tm::expect(c.op, c.e, c.t);
    if (want == Outcome::NotApplicable) GTEST_SKIP() << tm::naRule(c.op, c.e, c.t);
    SCOPED_TRACE(std::string("expect ") + std::string(tm::name(want)));
    Driver d(c);
    d.seed();
    if (HasFatalFailure()) return;
    d.run(want);
    // Ring invariant (every transient cell): nothing at/above the HW was evicted.
    if (c.e.persist == tm::Persist::Transient && c.e.commit == tm::Commit::Committed &&
        c.e.role == tm::Role::Leader)
        EXPECT_LE(d.rig->ringStart(), d.rig->hw()) << "an uncommitted record was evicted";
}

INSTANTIATE_TEST_SUITE_P(AllCells, TransientMatrixRegression, testing::ValuesIn(allCells()), cellName);

// The table's shape is pinned: 1008 cells, 366 applicable, by outcome.
TEST(TransientMatrixCountRegression, CellCountsArePinned) {
    std::map<Outcome, int> by;
    int total = 0;
    for (const Cell& c : allCells()) { ++by[tm::expect(c.op, c.e, c.t)]; ++total; }
    EXPECT_EQ(total, 1008);
    EXPECT_EQ(total - by[Outcome::NotApplicable], 366);
    EXPECT_EQ(by[Outcome::RetriableNotLeader], 112);
    EXPECT_EQ(by[Outcome::Ok], 84);
    EXPECT_EQ(by[Outcome::OkAfterCommit], 32);
    EXPECT_EQ(by[Outcome::UnansweredThenReissued], 40);
    EXPECT_EQ(by[Outcome::ParityWithDisk], 40);
    EXPECT_EQ(by[Outcome::OkEmptyAfterRestart], 18);
    EXPECT_EQ(by[Outcome::MarkerRetried], 18);
    EXPECT_EQ(by[Outcome::OkFromRingStart], 16);
    EXPECT_EQ(by[Outcome::RetriableLogFull], 6);
}

} // namespace
```

In `libs/gateway/CMakeLists.txt`, after `gtest_discover_tests(rune_gateway_transient_tests)` add:

```cmake

    # Phase 2b E2: the transient interaction matrix (1008 TEST_P cells) — no Aeron.
    add_executable(rune_gateway_transient_matrix_tests tests/gateway/TransientInteractionMatrixTest.cpp)
    target_include_directories(rune_gateway_transient_matrix_tests PRIVATE ${CMAKE_SOURCE_DIR}/libs/node/tests)
    target_link_libraries(rune_gateway_transient_matrix_tests PRIVATE rune_gateway rune_node rune_replication
                          rune_storage rune_transaction spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_transient_matrix_tests DISCOVERY_TIMEOUT 120)
```

- [ ] **Step 3: Run the matrix**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 --target rune_gateway_transient_matrix_tests && ctest --test-dir build-fast -R 'TransientMatrix' --output-on-failure -j14`
Expected: `TransientMatrixCountRegression.CellCountsArePinned` PASSES; 366 `AllCells/TransientMatrixRegression.Cell/*` PASS and 642 are SKIPPED (each with its rule). On top of Tasks 1-9 every applicable cell is expected green — a failing cell is either a real gap (fix it in the owning layer with its own `*Regression*` test, keep the cell) or a rig defect (fix the rig, never the table). Do NOT edit `expect()` to make a cell pass; a table change needs the controller's ruling and a plan update.

- [ ] **Step 4: Commit**

```bash
git add libs/node/tests/support/TransientMatrixExpectations.h libs/gateway/tests/gateway/TransientInteractionMatrixTest.cpp libs/gateway/CMakeLists.txt
git commit -m "test(matrix): transient interaction matrix — op x entity x timing, 1008 cells (366 applicable), unit level

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 12: Interaction matrix — seeded model-checked stress over the shared ring

The ring of a transient partition is the one piece of state E2 shares between operations: the produce path appends to it, the follower's fetches move its eviction floor (the HW), push reads at arbitrary cursors read it (read_uncommitted and read_committed), and transactional batches and markers interleave. The unit matrix pins each cell under one timing; this stress drives random interleavings of all of them against a reference model and checks every invariant after EVERY step. It runs the `Leader` / `Transient` rows of R8, R10 and R11 (the cells where state is shared) at the worker level, where the ring lives.

**Files:**
- Create: `libs/node/tests/integration/TransientDataPathStressTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target after `gtest_discover_tests(rune_transient_alloc_regression_tests)`, Task 6)

**Interfaces:**
- Consumes: `transient_rig::Rig` + `batchOf` (Task 4), the Task 2-5 worker / replica paths, `Worker::transientRecordsSkippedForTest()` (Task 4).
- Produces: nothing consumed later.

- [ ] **Step 1: Write the stress** — create `libs/node/tests/integration/TransientDataPathStressTest.cpp`:

```cpp
// TransientDataPathStressTest.cpp — Phase 2b E2 interaction matrix, seeded
// model-checked stress over the SHARED state of one transient partition: the
// ring written by produce (plain + transactional), its eviction floor moved by
// follower 2's fetches (ISR {1,2}: the HW is the follower's position), read by
// push reads at random cursors in both isolations, with COMMIT / ABORT markers
// interleaved.  A reference model (every acked batch: base, count, kind, txn
// fate) is checked against the ring and against every answer after EVERY step.
// 64 seeds x 3000 steps; a failure prints its seed and step.
#include "TransientRig.h"

#include <algorithm>
#include <optional>
#include <random>
#include <vector>

using namespace transient_rig;

namespace {

constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);
constexpr int64_t kPid              = 7;
constexpr int     kSteps            = 3000;

enum class Fate : uint8_t { Plain, Open, Committed, Aborted, Control };
struct MBatch {
    int64_t base;
    int32_t count;
    Fate    fate;
};

struct Model {
    std::vector<MBatch> batches;        // every acked batch, in offset order
    int64_t             leo        = 0;
    int64_t             fetched    = 0; // follower 2's position = the HW
    int64_t             ringStart  = 0; // last observed (monotone)
    int32_t             nextSeq    = 0; // kPid's next base sequence
    bool                txnOpen    = false;

    [[nodiscard]] const MBatch* containing(int64_t off) const {
        for (const MBatch& b : batches) if (off >= b.base && off < b.base + b.count) return &b;
        return nullptr;
    }
    /// read_committed bound: the HW, pinned at the first offset of an OPEN transaction.
    [[nodiscard]] int64_t lso() const {
        for (const MBatch& b : batches) if (b.fate == Fate::Open && b.base < fetched) return b.base;
        return fetched;
    }
};

class Stress {
public:
    explicit Stress(uint32_t seed) : rng_(seed), rig_(/*ringBytes=*/32 * 1024, /*maxBatches=*/1000, /*withFollower=*/true) {}

    void run() {
        std::uniform_int_distribution<int> pick(0, 99);
        for (step_ = 0; step_ < kSteps; ++step_) {
            const int p = pick(rng_);
            if      (p < 40) producePlain();
            else if (p < 55) followerFetch();
            else if (p < 75) pushRead();
            else if (p < 87) produceTxn();
            else             marker();
            if (::testing::Test::HasFailure()) return;
            checkInvariants();
            if (::testing::Test::HasFailure()) return;
        }
    }

private:
    std::mt19937 rng_;
    Rig          rig_;
    Model        m_;
    int          step_ = 0;
    int64_t      subId_ = 1;
    int64_t      token_ = 1;

    void producePlain() {
        std::uniform_int_distribution<int> recs(1, 3), bytes(100, 1500);
        int64_t base = -1;
        const int n = recs(rng_);
        const int16_t code = rig_.produce(batchOf(n, static_cast<std::size_t>(bytes(rng_))), /*acks=*/0, &base);
        if (code == kOk) {
            ASSERT_EQ(base, m_.leo) << "step " << step_ << ": a produce landed off the log end";
            m_.batches.push_back(MBatch{base, n, Fate::Plain});
            m_.leo += n;
        } else {
            ASSERT_EQ(code, kTransientLogFull) << "step " << step_ << ": a plain produce answered " << code;
            ASSERT_EQ(rig_.mem.logEndOffset(), m_.leo) << "step " << step_ << ": a refused produce appended";
        }
    }

    void produceTxn() {
        int64_t base = -1;
        const int16_t code = rig_.produce(batchOf(1, 200, kPid, 0, m_.nextSeq, /*txn=*/true), 0, &base, kPid, 0,
                                          m_.nextSeq);
        if (code == kOk) {
            ASSERT_EQ(base, m_.leo) << "step " << step_;
            m_.batches.push_back(MBatch{base, 1, Fate::Open});
            m_.leo += 1;
            ++m_.nextSeq;
            m_.txnOpen = true;
        } else {
            ASSERT_TRUE(code == kTransientLogFull || code == kNotLeader)   // NotLeader: the RC state builds
                << "step " << step_ << ": a txn produce answered " << code;
            ASSERT_EQ(rig_.mem.logEndOffset(), m_.leo) << "step " << step_ << ": a refused txn produce appended";
        }
    }

    void marker() {
        if (!m_.txnOpen) return;
        const bool commit = std::bernoulli_distribution(0.5)(rng_);
        rig_.marker(token_++, kPid, commit);
        for (int i = 0; i < 8; ++i) rig_.pass();
        rig_.pool.drainAllReturns([](WorkerReturn) {});   // its answer may wait for the HW (D9): not needed here
        const int64_t leo = rig_.mem.logEndOffset();
        if (leo == m_.leo) return;                         // refused (ring full: 42) — the coordinator retries
        ASSERT_EQ(leo, m_.leo + 1) << "step " << step_ << ": a marker appended more than one record";
        for (MBatch& b : m_.batches) if (b.fate == Fate::Open) b.fate = commit ? Fate::Committed : Fate::Aborted;
        m_.batches.push_back(MBatch{m_.leo, 1, Fate::Control});
        m_.leo += 1;
        m_.txnOpen = false;
    }

    void followerFetch() {
        if (m_.fetched == m_.leo) return;
        std::uniform_int_distribution<int64_t> to(m_.fetched, m_.leo);
        const int64_t f = to(rng_);
        rig_.fetch(f);
        for (int i = 0; i < 4; ++i) rig_.pass();
        m_.fetched = f;
        ASSERT_EQ(rig_.rep->highWatermark(), f) << "step " << step_ << ": the HW is not the follower's position";
    }

    void pushRead() {
        if (m_.leo == 0) return;
        std::uniform_int_distribution<int64_t> at(0, m_.leo);
        const int64_t c   = at(rng_);
        const uint8_t iso = std::bernoulli_distribution(0.3)(rng_) ? 1 : 0;
        const uint64_t skippedBefore = rig_.worker().transientRecordsSkippedForTest();
        const auto r = rig_.readOnce(subId_++, c, iso);
        if (!r.has_value()) {
            ASSERT_EQ(iso, 1) << "step " << step_ << ": a read_uncommitted read was never answered";
            return;   // read_committed held while the RC state builds — answered (and dropped) later
        }
        const int64_t start = std::max(c, rig_.mem.logStartOffset());
        const uint64_t skipped = rig_.worker().transientRecordsSkippedForTest() - skippedBefore;
        ASSERT_EQ(skipped, static_cast<uint64_t>(start - c)) << "step " << step_ << ": skipped records miscounted";
        const int64_t bound = iso == 1 ? m_.lso() : m_.fetched;
        std::size_t pos = 0;
        bool first = true;
        while (pos < r->chunk.size()) {
            rune::protocol::RecordBatchReader b(r->chunk.data() + pos, r->chunk.size() - pos);
            ASSERT_GT(b.totalLength(), 0);
            const int64_t base = b.baseOffset();
            const int64_t last = base + b.lastOffsetDelta();
            const MBatch* mb = m_.containing(base);
            ASSERT_NE(mb, nullptr) << "step " << step_ << ": delivered a batch the model never acked at " << base;
            ASSERT_EQ(mb->base, base) << "step " << step_;
            ASSERT_EQ(mb->count, last - base + 1) << "step " << step_ << ": a delivered batch differs from the acked one";
            ASSERT_LT(last, bound) << "step " << step_ << ": delivered past the " << (iso ? "LSO" : "HW");
            if (first) {
                const MBatch* at0 = m_.containing(start);
                ASSERT_NE(at0, nullptr);
                ASSERT_GE(base, at0->base) << "step " << step_ << ": the read started below its cursor/ring start";
                first = false;
            }
            if (iso == 1) {
                ASSERT_NE(mb->fate, Fate::Control) << "step " << step_ << ": a marker reached read_committed";
                ASSERT_NE(mb->fate, Fate::Aborted) << "step " << step_ << ": an aborted batch reached read_committed";
                ASSERT_NE(mb->fate, Fate::Open) << "step " << step_ << ": an open transaction reached read_committed";
            }
            pos += static_cast<std::size_t>(b.totalLength());
        }
    }

    void checkInvariants() {
        const int64_t start = rig_.mem.logStartOffset();
        ASSERT_LE(start, rig_.rep->highWatermark()) << "step " << step_ << ": an uncommitted record was evicted";
        ASSERT_EQ(rig_.mem.logEndOffset(), m_.leo) << "step " << step_;
        ASSERT_GE(start, m_.ringStart) << "step " << step_ << ": the ring start moved backwards";
        m_.ringStart = start;
        if (start == m_.leo) return;
        const MBatch* first = m_.containing(start);
        ASSERT_NE(first, nullptr);
        ASSERT_EQ(first->base, start) << "step " << step_ << ": the ring start is not a batch boundary";
        for (const MBatch& b : m_.batches) {   // every held batch is exactly the acked one
            if (b.base < start) continue;
            const auto v = rig_.mem.readView(b.base, 1);
            ASSERT_FALSE(v.empty()) << "step " << step_ << ": held offset " << b.base << " unreadable";
            rune::protocol::RecordBatchReader r(v.data, v.len);
            ASSERT_EQ(r.baseOffset(), b.base) << "step " << step_;
            ASSERT_EQ(r.lastOffsetDelta() + 1, b.count) << "step " << step_;
        }
    }
};

class TransientDataPathStressRegression : public testing::TestWithParam<uint32_t> {};

TEST_P(TransientDataPathStressRegression, SeededModelChecked) {
    SCOPED_TRACE("seed " + std::to_string(GetParam()));
    Stress s(GetParam());
    s.run();
}

INSTANTIATE_TEST_SUITE_P(Seeds, TransientDataPathStressRegression, testing::Range(1u, 65u));

} // namespace
```

In `libs/node/CMakeLists.txt`, after `gtest_discover_tests(rune_transient_alloc_regression_tests)` add:

```cmake
    # Phase 2b E2: seeded model-checked stress over a transient partition's shared ring.
    add_executable(rune_transient_stress_tests tests/integration/TransientDataPathStressTest.cpp)
    target_link_libraries(rune_transient_stress_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_stress_tests PROPERTIES TIMEOUT 300)
```

- [ ] **Step 2: Run it**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build build-fast -j14 --target rune_transient_stress_tests && ctest --test-dir build-fast -R 'TransientDataPathStress' --output-on-failure -j14`
Expected: all 64 seeds PASS. A failing seed is a real interleaving bug (or a model error): reproduce it alone (`--gtest_filter='*SeededModelChecked/<n>'`), fix the owning layer with a `*Regression*` test that replays the minimal step sequence, keep the seed.

- [ ] **Step 3: Commit**

```bash
git add libs/node/tests/integration/TransientDataPathStressTest.cpp libs/node/CMakeLists.txt
git commit -m "test(matrix): seeded model-checked stress over a transient partition's shared ring (64 seeds)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 13: Interaction matrix — the BrokerNode integration slice over real Aeron

The slice runs matrix cells where only a real node can: the shard owned-subscription push loop (`pushOwnedSubscriptions_`, needs a real egress publication), produce on a real worker ingress, a real restart, a real DDL commit (C1) and a real leader failover. Cells: both worker modes × {`ProduceAcks0`, `ProduceAcks1`, `ProduceAcksAll`, `SubEarliest`, `SubLatest`, `SubOffset`} × {`Steady`, `EvictedCursor`, `Restart`, `CommitLandsMidOp`} = 48, plus `ProduceAcksAll` + `SubEarliest` × `LeaderMovesMidOp` on 3 nodes (shard). Outcomes come from `TransientMatrixExpectations.h`.

**Files:**
- Modify: `libs/node/tests/integration/TransientTopicDataPathTest.cpp` (append; uses Task 8's file-local helpers)

**Interfaces:**
- Consumes: Task 8's `TmpDir`, `Ports::discover`, `makeConfig(…, ringBytes)`, `gatewayChannel`, `workerIngressChannel`, `pushedAt`, `defineTransient`; `ShardTestClient`; `tm::expect`, `tm::wireAcks`, `tm::name` (Task 11).
- Produces: nothing consumed later.

- [ ] **Step 1: Write the slice** — add `#include "support/TransientMatrixExpectations.h"` and `#include <memory>` to the includes of `TransientTopicDataPathTest.cpp`, then append:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2 interaction matrix — the BrokerNode slice: both worker modes x
// {3 produce acks, 3 subscribe starts} x {Steady, EvictedCursor, Restart,
// CommitLandsMidOp} over REAL Aeron (48 cells; shard = produce on the worker's
// ingress, Records pushed by the worker's owned subscriptions — the loop the
// unit matrix cannot reach), plus LeaderMovesMidOp on 3 nodes.  Outcomes from
// support/TransientMatrixExpectations.h.
// ---------------------------------------------------------------------------
namespace {
namespace tm = rune::node::test::tmatrix;

std::vector<uint8_t> valueBatch(int records, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}

struct SliceParam {
    tm::Mode   mode;
    tm::Timing timing;
};
std::string sliceName(const testing::TestParamInfo<SliceParam>& i) {
    return std::string(i.param.mode == tm::Mode::Shard ? "Shard" : "Legacy") + "_" +
           std::string(tm::name(i.param.timing));
}

/// The first base a subscription created after `known` delivered (polls; -1 = none).
int64_t firstOfNewSubscription(ShardTestClient& c, const std::set<int64_t>& known, int64_t* subOut = nullptr) {
    int64_t first = -1;
    (void)c.pollUntil([&] {
        for (const auto& r : c.records())
            if (!known.count(r.subscriptionId)) { first = r.baseOffset; if (subOut) *subOut = r.subscriptionId; return true; }
        return false;
    });
    return first;
}
std::set<int64_t> knownSubscriptions(const ShardTestClient& c) {
    std::set<int64_t> s;
    for (const auto& r : c.records()) s.insert(r.subscriptionId);
    return s;
}
} // namespace

class TransientSliceRegression : public testing::TestWithParam<SliceParam> {};

TEST_P(TransientSliceRegression, ProduceAndSubscribeCells) {
    const auto [mode, timing] = GetParam();
    const bool shard = mode == tm::Mode::Shard;
    const bool evict = timing == tm::Timing::EvictedCursor;
    const tm::Commit commit = timing == tm::Timing::CommitLandsMidOp ? tm::Commit::Uncommitted : tm::Commit::Committed;
    const tm::Entity e{tm::Persist::Transient, mode, tm::Role::Leader, commit};
    TmpDir dir{"slice"}, c1{"slice_c1"}, c2{"slice_c2"};
    const auto ports = Ports::discover(1);
    const NodeConfig cfg = makeConfig(0, dir.path, ports, 1, shard, /*ringBytes=*/evict ? 16 * 1024 : 0);
    auto node = std::make_unique<BrokerNode>(cfg);
    node->start();
    ASSERT_TRUE(node->awaitLeader(10'000ms));
    auto client = std::make_unique<ShardTestClient>(c1.path);
    int64_t sid = client->connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    int32_t tid = 0;   // the static "tt" (makeConfig)
    if (commit == tm::Commit::Uncommitted) {
        // CommitLandsMidOp over a real node: the uncommitted window is internal (the DDL OK
        // is held until the topic serves — DdlAnswerGate, C1); the cell asserts the first op
        // after the OK is served, never refused for good (OkAfterCommit).
        tid = defineTransient(*client, gatewayChannel(ports, 0), sid, "tt_slice_rt");
        ASSERT_GE(tid, 0);
    }
    const std::string produceCh = shard ? workerIngressChannel(ports, 0) : gatewayChannel(ports, 0);
    std::vector<int64_t> acked;   // acked batch bases (2 records each)
    auto produce = [&](uint8_t acks, std::size_t valueBytes) -> std::optional<int64_t> {
        auto ack = client->produceUntilOk(produceCh, kClientStream, sid, tid, 0, acks, valueBatch(2, valueBytes));
        if (!ack) return std::nullopt;
        acked.push_back(ack->baseOffset);
        return ack->baseOffset;
    };
    // The timing's preconditions.
    for (int i = 0; i < (evict ? 40 : 2); ++i) ASSERT_TRUE(produce(0, evict ? 1000 : 8).has_value());
    if (timing == tm::Timing::Restart) {
        client.reset();
        node->stop();
        node.reset();
        node = std::make_unique<BrokerNode>(cfg);
        node->start();
        ASSERT_TRUE(node->awaitLeader(10'000ms));
        client = std::make_unique<ShardTestClient>(c2.path);
        sid = client->connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        acked.clear();   // the ring restarted empty
    }

    // ---- 3 produce cells ------------------------------------------------------
    for (tm::Op op : {tm::Op::ProduceAcks0, tm::Op::ProduceAcks1, tm::Op::ProduceAcksAll}) {
        const tm::Outcome want = tm::expect(op, e, timing);
        SCOPED_TRACE(std::string(tm::name(op)) + " expect " + std::string(tm::name(want)));
        const int64_t expectBase = acked.empty() ? 0 : acked.back() + 2;
        const auto base = produce(tm::wireAcks(op), 8);
        ASSERT_TRUE(base.has_value()) << "a transient produce was never acked";
        EXPECT_EQ(*base, expectBase) << "not contiguous with the acked history";
        if (want == tm::Outcome::OkEmptyAfterRestart && op == tm::Op::ProduceAcks0)
            EXPECT_EQ(*base, 0) << "a transient topic restarted non-empty";
    }

    // ---- 3 subscribe cells ----------------------------------------------------
    for (tm::Op op : {tm::Op::SubEarliest, tm::Op::SubLatest, tm::Op::SubOffset}) {
        const tm::Outcome want = tm::expect(op, e, timing);
        SCOPED_TRACE(std::string(tm::name(op)) + " expect " + std::string(tm::name(want)));
        const auto known = knownSubscriptions(*client);
        const int64_t hwBefore = acked.back() + 2;   // every acked record is committed (single node)
        const int64_t from = op == tm::Op::SubEarliest ? -2 : op == tm::Op::SubLatest ? -1 : 3;
        ASSERT_TRUE(client->subscribe(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, from));
        if (op == tm::Op::SubLatest) {
            // Latest never replays: produce until the new subscription delivers; its FIRST batch is new.
            int64_t first = -1;
            for (int i = 0; i < 20 && first < 0; ++i) {
                ASSERT_TRUE(produce(1, 8).has_value());
                first = firstOfNewSubscription(*client, known);
            }
            ASSERT_GE(first, 0) << "the latest subscription never delivered";
            EXPECT_GE(first, hwBefore) << "Latest replayed an old record";
            continue;
        }
        const int64_t first = firstOfNewSubscription(*client, known);
        ASSERT_GE(first, 0) << "the subscription delivered nothing (a stall)";
        switch (want) {
        case tm::Outcome::OkFromRingStart:   // resumed at the ring start (> the cursor), never a stall
            EXPECT_GT(first, op == tm::Op::SubEarliest ? 0 : 3) << "read an evicted offset?";
            EXPECT_LE(first, acked.back());
            break;
        default:                             // Ok / OkEmptyAfterRestart / OkAfterCommit
            EXPECT_EQ(first, op == tm::Op::SubEarliest ? acked.front() : int64_t{2})
                << "whole-batch delivery from the batch containing the cursor";
            break;
        }
    }
    client.reset();
    node->stop();
}

INSTANTIATE_TEST_SUITE_P(
    Cells, TransientSliceRegression,
    testing::Values(SliceParam{tm::Mode::Shard, tm::Timing::Steady}, SliceParam{tm::Mode::Legacy, tm::Timing::Steady},
                    SliceParam{tm::Mode::Shard, tm::Timing::EvictedCursor},
                    SliceParam{tm::Mode::Legacy, tm::Timing::EvictedCursor},
                    SliceParam{tm::Mode::Shard, tm::Timing::Restart}, SliceParam{tm::Mode::Legacy, tm::Timing::Restart},
                    SliceParam{tm::Mode::Shard, tm::Timing::CommitLandsMidOp},
                    SliceParam{tm::Mode::Legacy, tm::Timing::CommitLandsMidOp}),
    sliceName);

// LeaderMovesMidOp on 3 nodes (shard): acks=all records acked before the partition
// leader stops are all on the new leader — the next produce lands right after them
// (no gap, nothing lost) and a subscription from earliest on the new leader gets
// every one of them.  ProduceAcksAll: RetriableNotLeader then Ok (produceUntilOk
// retries); SubEarliest on the new leader: Ok.
TEST(TransientSliceRegression, ThreeNodeLeaderMovesMidOpLosesNoAckedRecordRegression) {
    TmpDir d0{"lm0"}, d1{"lm1"}, d2{"lm2"}, cdir{"lmc"};
    const auto ports = Ports::discover(3);
    std::unique_ptr<BrokerNode> nodes[3];
    const TmpDir* dirs[3] = {&d0, &d1, &d2};
    for (int i = 0; i < 3; ++i) nodes[i] = std::make_unique<BrokerNode>(makeConfig(i, dirs[i]->path, ports, 3, true));
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
    ShardTestClient c(cdir.path);
    auto leaderVia = [&](int gwNode, int notThis) {
        const int64_t s = c.connect(gatewayChannel(ports, gwNode), kClientStream);
        for (int i = 0; s > 0 && i < 400; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, gwNode), kClientStream, s))
                if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId >= 0 && pi.leaderId != notThis)
                    return static_cast<int>(pi.leaderId);
            std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        return -1;
    };
    const int leader = leaderVia(0, -1);
    ASSERT_GE(leader, 0);
    const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
    ASSERT_GT(sid, 0);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, leader), kClientStream, sid, 0, 0, /*acks=all*/1,
                                     makeBatch(2)).has_value());
    nodes[leader]->stop();        // the partition leader goes away mid-stream
    nodes[leader].reset();
    const int survivor = (leader + 1) % 3;
    const int next = leaderVia(survivor, leader);
    ASSERT_GE(next, 0) << "no new partition leader";
    const int64_t sidN = c.connect(gatewayChannel(ports, next), kClientStream);
    ASSERT_GT(sidN, 0);
    const auto ack = c.produceUntilOk(workerIngressChannel(ports, next), kClientStream, sidN, 0, 0, 1, makeBatch(2));
    ASSERT_TRUE(ack.has_value()) << "the new leader never took a produce";
    EXPECT_EQ(ack->baseOffset, 10) << "an acks=all record acked by the old leader is missing on the new one";
    ASSERT_TRUE(c.subscribe(gatewayChannel(ports, next), kClientStream, sidN, 0, 0, /*Earliest=*/-2));
    ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 10) >= 1; }));
    for (int64_t b : {0, 2, 4, 6, 8}) EXPECT_GE(pushedAt(c, b), 1u) << "acked batch " << b << " lost in the failover";
    for (auto& n : nodes) if (n) n->stop();
}
```

- [ ] **Step 2: Run it**

Run: `cmake --build build-fast -j14 --target rune_transient_topic_data_path_tests && ctest --test-dir build-fast -R 'TransientSlice|TransientTopicDataPath' --output-on-failure -j14`
Expected: all PASS (8 slice instances × 6 cells + the 3-node failover + Task 8's five). A failing cell is a real integration gap: fix the owning layer with its own `*Regression*` test, keep the cell.

- [ ] **Step 3: Commit**

```bash
git add libs/node/tests/integration/TransientTopicDataPathTest.cpp
git commit -m "test(matrix): transient matrix BrokerNode slice over Aeron (48 cells + 3-node leader move)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 14: e2e — define a transient topic, produce (3 acks) + subscribe (3 starts) + read_committed, restart → exists and empty (Part E Task 10, executed here; the matrix's e2e leg)

Part E wrote this leg and marked it BLOCKED on the data path (E's Spec question 1); Tasks 1-13 are that data path. It was NOT executed on `feature/materialized-views` (no `presence_t` in `examples/view_probe.cpp` or `docker/view-engine-e2e.sh` at `c06348b` / `6a3b9baa`), so Steps 1-3 apply in full. It is extended with the matrix's e2e cells: `Transient` / `Shard` (the docker broker's default `RUNE_WORKER_MODE=shard`) / `Leader` / defined at runtime (the DDL OK is held until the topic serves — C1) × {`ProduceAcks0`/`1`/`All` (client `Acks::None` / `Leader` / `All`), `SubEarliest`, `SubOffset`, `SubLatest`, `ReadCommitted` (a committed and an aborted transaction)} × {`Steady`, `Restart`}. The spec's e2e says "console defines a transient topic"; the console is Part D (after E/E2): this leg defines it through `Client::defineTopicAndWait(..., Persistence::Transient, …)` — exactly what D's console calls — and D re-runs it through `rune console`.

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp` (helpers above `main`; setup step 7 before `// ---- Handoff for the restart leg` ~`:687`; the handoff lines; verify_restart env reads ~`:716-726` and step (e) before `std::cout << "VIEW-ENGINE-E2E: OK\n";` ~`:796`; header comment ~`:24-49`)
- Modify: `docker/view-engine-e2e.sh` (extract/handoff `:294-312`, verify env `:445-456`, summary `:505-518`)

**Interfaces:**
- Consumes: Part E `rune::client::Persistence::Transient` + `Client::defineTopicAndWait(name, partitions, keyField, validate, blob, Persistence, timeout)`; existing probe helpers `sendAndWaitAck`, `encodeCharacter`, `characterDescriptor`, `getEnvI64`, locals `blob`, `wait`, `timeoutMs`, `boundMs`, `c` (all in `examples/view_probe.cpp`); client `Subscription::{position, subscriptionId, unsubscribe, finished}`, `SubscribeOptions{from, isolation}`, `From::{earliest, latest, offset}`, `Isolation::ReadCommitted`, `Client::transactionalProducer` + `TransactionalProducer::{initTransactionsAndWait, beginTransaction, send, flushAndWait, commitTransactionAndWait, abortTransactionAndWait}`; Task 1 (the client retries 42).
- Produces: handoff variable `VIEW_TT_TOPIC_ID` / env `RUNE_VIEW_TT_TOPIC_ID`.

- [ ] **Step 1: Probe — helpers + setup step 7** — in `examples/view_probe.cpp` (`<rune/client/subscriber/Subscription.h>` is already included), add above `main`:

```cpp
// Subscribe with `from` / `iso` and wait (bounded) until the subscription's
// published position reaches `atLeast`; returns the final position after
// unsubscribe + finished().  position() comes off the subscription's own status
// ring.  *seenOut = records the handler saw — read only after finished(): the
// handler never runs again (Subscription's documented resume pattern), so no
// state is shared with the duty thread while it can still run.
static int64_t subscribeUntil(Client& c, int32_t topicId, From from, Isolation iso, int64_t atLeast, int boundMs,
                              int64_t* seenOut = nullptr) {
    SubscribeOptions opts;
    opts.from      = from;
    opts.isolation = iso;
    int64_t seen = 0;
    auto sub = c.subscribe(TopicHandle{topicId, 1}, /*partition=*/0, opts, [&seen](const Record&) { ++seen; });
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (sub.position() < atLeast && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);   // poll interval, not correctness
    sub.unsubscribe();
    const auto fin = std::chrono::steady_clock::now() + 5s;
    while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    if (seenOut != nullptr) *seenOut = seen;
    return sub.position();
}

// One record with the given acks; offsetOut = its offset (-1 for Acks::None: the
// client reports Ok when the record is handed to the transport).
static bool produceOne(Client& c, int32_t topicId, int64_t key, Acks acks, int timeoutMs, int64_t& offsetOut) {
    ProducerConfig pcfg;
    pcfg.acks             = acks;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);
    Completions cq(c);
    Completion  done;
    const auto value = encodeCharacter(key, 1, /*version=*/1);
    const bool ok = sendAndWaitAck(cq, p, TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(key)),
                                   std::span<const uint8_t>(value), 1, timeoutMs, done);
    offsetOut = done.value;
    return ok && done.status == OpStatus::Ok;
}

// One transaction of `n` records on the transient topic, committed or aborted.
static bool transientTxn(Client& c, int32_t topicId, const std::string& txnId, int64_t firstKey, int n, bool commit,
                         int timeoutMs) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    try {
        auto tp = c.transactionalProducer(txnId, pcfg);
        tp.initTransactionsAndWait(std::chrono::milliseconds(timeoutMs * 2));
        tp.beginTransaction();
        for (int i = 0; i < n; ++i) {
            const auto value = encodeCharacter(firstKey + i, 1, 1);
            tp.send(TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(firstKey + i)), std::span<const uint8_t>(value));
        }
        if (commit) {
            tp.commitTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
        } else {
            tp.flushAndWait(std::chrono::milliseconds(timeoutMs * 2));   // the aborted records reach the log
            tp.abortTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
        }
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL transient txn (" << (commit ? "commit" : "abort") << "): " << ex.what() << "\n";
        return false;
    }
    return true;
}
```

In `mode == "setup"`, right before `            // ---- Handoff for the restart leg (scraped by the e2e script) ----` insert:

```cpp
            // ---- 7. Phase 2b E/E2: a TRANSIENT topic — define at runtime (the OK is
            //         held until it serves: C1), produce in all three ack modes,
            //         subscribe from earliest / an offset / latest, read_committed. ----
            DefineTopicResult tt = c.defineTopicAndWait(
                "presence_t", /*partitions=*/1, /*keyField=*/"id", /*validate=*/false,
                std::span<const uint8_t>(blob), Persistence::Transient, wait);
            if (tt.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(presence_t transient) errorCode=" << tt.errorCode << "\n"; return 50; }
            {
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 1, Acks::None, timeoutMs, off))   { std::cerr << "PROBE: FAIL produce(presence_t, acks=None)\n"; return 51; }
                if (!produceOne(c, tt.topicId, 2, Acks::Leader, timeoutMs, off) || off != 1) { std::cerr << "PROBE: FAIL produce(presence_t, acks=Leader) offset=" << off << "\n"; return 51; }
                if (!produceOne(c, tt.topicId, 3, Acks::All, timeoutMs, off) || off != 2)    { std::cerr << "PROBE: FAIL produce(presence_t, acks=All) offset=" << off << "\n"; return 51; }
            }
            if (const int64_t pos = subscribeUntil(c, tt.topicId, From::earliest(), Isolation::ReadUncommitted, 3, boundMs); pos < 3) {
                std::cerr << "PROBE: FAIL subscribe(presence_t, earliest) position=" << pos << " (want 3)\n"; return 52;
            }
            {
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, tt.topicId, From::offset(2), Isolation::ReadUncommitted, 3, boundMs, &seen);
                if (pos != 3 || seen != 1) { std::cerr << "PROBE: FAIL subscribe(presence_t, offset 2) position=" << pos << " seen=" << seen << " (want 3, 1)\n"; return 53; }
            }
            {
                // Latest never replays: the subscription is live (its id is known) before
                // the next record is produced; it sees exactly that one record.
                SubscribeOptions o;
                o.from = From::latest();
                int64_t seen = 0;
                auto sub = c.subscribe(TopicHandle{tt.topicId, 1}, 0, o, [&seen](const Record&) { ++seen; });
                const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
                while (sub.subscriptionId() <= 0 && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 4, Acks::All, timeoutMs, off) || off != 3) { std::cerr << "PROBE: FAIL produce(presence_t) #4 offset=" << off << "\n"; return 54; }
                while (sub.position() < 4 && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
                sub.unsubscribe();
                const auto fin = std::chrono::steady_clock::now() + 5s;
                while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
                if (sub.position() != 4 || seen != 1) { std::cerr << "PROBE: FAIL subscribe(presence_t, latest) position=" << sub.position() << " seen=" << seen << " (want 4, 1)\n"; return 54; }
            }
            {
                // read_committed: offsets 0-3 plain; a committed txn (2 records + marker),
                // an aborted txn (2 records + marker), then one plain record (acks=All).
                // read_committed must see exactly 4 + 2 + 1 = 7 records and reach its end.
                if (!transientTxn(c, tt.topicId, "e2-presence-commit", 10, 2, /*commit=*/true, timeoutMs))  return 55;
                if (!transientTxn(c, tt.topicId, "e2-presence-abort", 20, 2, /*commit=*/false, timeoutMs))  return 55;
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 30, Acks::All, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after txns\n"; return 55; }
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, tt.topicId, From::earliest(), Isolation::ReadCommitted, off + 1, boundMs, &seen);
                if (pos != off + 1 || seen != 7) {
                    std::cerr << "PROBE: FAIL read_committed(presence_t) position=" << pos << " seen=" << seen
                              << " (want " << off + 1 << ", 7: an aborted or open record leaked, or a committed one was lost)\n";
                    return 56;
                }
            }
            std::cerr << "PROBE: transient topic presence_t topicId=" << tt.topicId
                      << " produced (None/Leader/All) + subscribed (earliest/offset/latest) + read_committed OK\n";
```

and add to the handoff lines `std::cout << "VIEW_TT_TOPIC_ID=" << tt.topicId << "\n";`.

- [ ] **Step 2: Probe — verify_restart step (e)** — with the other env reads add
`const int32_t  ttTopic        = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));`, and right before `            std::cout << "VIEW-ENGINE-E2E: OK\n";` insert:

```cpp
            // ---- (e) Phase 2b E/E2: the transient topic EXISTS and is EMPTY ------
            if (ttTopic < 0) { std::cerr << "PROBE: FAIL missing RUNE_VIEW_TT_TOPIC_ID\n"; return 60; }
            {
                // Metadata survived: re-defining answers TopicExists with the SAME id.
                const auto ttBlob = rune::codec::serialize(characterDescriptor());
                DefineTopicResult again = c.defineTopicAndWait(
                    "presence_t", 1, "id", false, std::span<const uint8_t>(ttBlob),
                    Persistence::Transient, wait);
                if (again.errorCode != 25 || again.topicId != ttTopic) {
                    std::cerr << "PROBE: FAIL presence_t after restart errorCode=" << again.errorCode
                              << " topicId=" << again.topicId << " (want TopicExists, id " << ttTopic << ")\n";
                    return 61;
                }
                // Data did not: the first post-restart record lands at offset 0.
                int64_t off = -1;
                if (!produceOne(c, ttTopic, 9, Acks::All, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after restart\n"; return 62; }
                if (off != 0) { std::cerr << "PROBE: FAIL presence_t not empty after restart: first offset=" << off << "\n"; return 63; }
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, ttTopic, From::earliest(), Isolation::ReadUncommitted, 1, boundMs, &seen);
                if (pos != 1 || seen != 1) { std::cerr << "PROBE: FAIL presence_t subscribe after restart position=" << pos << " seen=" << seen << " (want 1, 1)\n"; return 64; }
                const int64_t rc = subscribeUntil(c, ttTopic, From::earliest(), Isolation::ReadCommitted, 1, boundMs, &seen);
                if (rc != 1 || seen != 1) { std::cerr << "PROBE: FAIL presence_t read_committed after restart position=" << rc << " seen=" << seen << "\n"; return 65; }
                std::cerr << "PROBE: presence_t exists after restart, empty, produce+subscribe+read_committed OK\n";
            }
```

Add to the file header's `RUNE_MODE=setup` description: `7. (Phase 2b E/E2) defineTopicAndWait("presence_t", Persistence::Transient); produce with acks None/Leader/All (offsets 0-2); subscribe from earliest (position 3), offset 2 (one record), latest (exactly the next record); a committed and an aborted transaction + one record — read_committed sees exactly 7.` and to `RUNE_MODE=verify_restart`: `(e) presence_t: TopicExists with the same id, first produce at offset 0, subscribe and read_committed from earliest → position 1.`

- [ ] **Step 3: Script** — in `docker/view-engine-e2e.sh`: add `VIEW_TT_TOPIC_ID="$(extract VIEW_TT_TOPIC_ID)"` after `VIEW_FP_AGG="$(extract VIEW_FP_AGG)"`; add `VIEW_TT_TOPIC_ID` to the `for v in …` required list; append ` transientTopicId=${VIEW_TT_TOPIC_ID}` to the `log "Setup OK: …"` line; add `-e RUNE_VIEW_TT_TOPIC_ID="${VIEW_TT_TOPIC_ID}" \` to the `run_client verify_restart` call (next to `-e RUNE_VIEW_FP_AGG=…`); add `log "  -> transient topic presence_t: produce (None/Leader/All) + subscribe (earliest/offset/latest) + read_committed (commit+abort); restart -> exists (TopicExists, same id) and empty (first offset 0)"` to the final summary, before `log "  VIEW-ENGINE-E2E: OK, exit 0."`. Add `presence_t` to `CATALOG_NAMES` (~`:399`) so the catalog-diff leg checks it survives the restart.

- [ ] **Step 4: Run**

Run (from the broker repo root, sibling layout `../rune-protocol ../rune-codec ../rune-crypto ../rune-fr ../rune-ql ../rune-client-cpp`, all on `feat/phase2b-e2-transient-data-path` where the repo has that branch): `bash docker/view-engine-e2e.sh`
Expected: `VIEW-ENGINE-E2E: OK, exit 0.` with both `presence_t` lines in the probe log (`produced (None/Leader/All) + subscribed (earliest/offset/latest) + read_committed OK`, `exists after restart, empty, produce+subscribe+read_committed OK`).

- [ ] **Step 5: Gate + commit (two repos)**

Run: `cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 --target view_probe && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'ViewProbe|View' --output-on-failure -j14` (the probe compiles; no unit test covers it — Step 4 is its test).

```bash
git -C ~/Projects/se-worktrees/rune-client-cpp add examples/view_probe.cpp
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "test(e2e): transient topic leg in view_probe (define, 3 acks, 3 subscribe starts, read_committed, restart empty)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add docker/view-engine-e2e.sh
git commit -m "test(e2e): view-engine e2e carries the transient-topic leg (produce + subscribe + read_committed + restart)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint E (controller):** full `*Regression*` suites in protocol, client and broker, then the final whole-branch review.

---

## Self-review

- **Coverage of the user decision / spec E:** produce in all ack modes — shard ingress (Task 4 `ShardIngressProduce…`, acks=all), op ring / legacy (Task 4 `GatewayRoutedProduceOp…`, Task 7 gateway produce, Task 8 legacy + runtime define), every ack mode × both worker modes × every timing (Task 11 matrix, Task 13 slice, Task 14 e2e with client `Acks::None/Leader/All`); **acks=all replicated to a majority before the ack** — Task 3 (ring never evicts above the HW), Task 8 `ThreeNodeAcksAll…`, Task 13 `ThreeNodeLeaderMovesMidOpLosesNoAckedRecord…` (every acked record on the new leader after the old one stops), Task 12 (`logStart ≤ HW` after every step); push subscribe from **earliest / latest / offset** — Task 7 (clamps), Task 8 shard, Task 11 (every timing), Task 13 (both modes over Aeron), Task 14 (real client); **consumer groups** — Task 7 `GroupSubscription…` (`addGroupSubscription`), the matrix's `GroupConsume` column (Task 11: Steady, evicted cursor, ring full, restart, leader move, C1); shard-mode group pushes ride the owned-subscription path Task 5 fixes and Task 13 exercises; **read_committed** — Task 5 (open txn withheld until the commit marker), the matrix's `ReadCommitted` column (Task 11), the stress's RC reads against the model (Task 12), the e2e committed + aborted transactions (Task 14); markers routed by the gateway (Task 7), refused retriably on a full ring (Task 4, matrix `TxnMarker` column); **both worker modes** — Tasks 4/5/8/11/13; **zero steady-state allocation** — Task 6 (`SteadyStateProduceEvictAndPushRead…`), Task 3 (`TransientFollowerApplyAllocatesNothing`), Task 9 (five scalar loads per pass); **same latency class** — Task 6 (produce→subscribe) and Task 10 (fold over the source) benchmark gates; **full-ring behaviour** — Tasks 1-5 (code 42, ruling 2), docs Task 8; **C1** — Task 4 (unregistered → NotLeader), Task 7 (`UncommittedTransientTopicServesNothingUntilCommit…`, `CommittedRuntimeTransientTopicServesThroughTheWorker…`), Task 8 (runtime define over Aeron), matrix R1/R2; **views over transient sources** (spec E user decision) — Part E's reconcile kept, hardened in Task 9 (source identity), benchmarked in Task 10; **e2e** — Task 14; **restart → exists and empty** — Task 8 (static topic, in-process), Task 13 (both modes), Task 14 (runtime topic via the client).
- **Interaction matrix:** section above; table in `TransientMatrixExpectations.h` (Task 11) with a count pin (1008 / 366); parameterized unit matrix (Task 11), seeded model-checked stress where state is shared (Task 12), integration slice (Task 13), e2e leg (Task 14).
- **Carry-overs:** (a) fold-over-transient benchmark → Task 10; (b) `logGeneration` hardening for views over transient sources across ring recreation → Task 9 (+ the generation bump on a ring reset in Task 3); (c) the Part D console must map code 37 (and now 42) → recorded in Global Constraints for Part D, not done here.
- **Placeholders:** none in code steps; every edit is an exact before/after anchored on code checked at `6a3b9baa`. The Task 6 and Task 10 commit bodies record numbers measured in their benchmark step (runtime data, not plan content). Two documented fallbacks name the exact alternative if an anchor's assumption fails: Task 10 (`recordBatchView` vs `recordBatchBytes`), Task 1 Step 4 (a conformance pin of `kClientVersion`).
- **Type consistency:** `ErrorCode::TransientLogFull` / `kClientVersionTransientLogFull` / `clientUnderstandsTransientLogFull` (Task 1) are what Tasks 2, 4, 7, 11, 12 use; `InMemoryLog::kRefusedFull` / `kRefusedTooLarge` (`int64_t`), `appendNoSyncRestamp(const uint8_t*, std::size_t, int32_t, int64_t&) → int64_t`, `ProduceOutcome::logFull` (Task 2) are what Tasks 3-4 use; `PartitionReplica::prepareTransientAppend()` / `kAppendBackPressure` / the `logGeneration` bump (Task 3) are what Tasks 4 and 9 use; `Worker::transient{FullRefusals,CursorSkips,RecordsSkipped}ForTest()` are added in Task 4 and asserted in Tasks 4, 5, 11, 12; `transient_rig::Rig` is defined once (Task 4) and used unchanged in Tasks 5, 6, 12; `ClientGateway::ServedPartition{Kind, log}` / `servedPartition_` are Task 7 only; `TransientBounds{logStart, logEnd, incarnation, logGeneration, cutOffset}`, `TransientCursor`, `ViewFoldCounters::transientSourceResets` are Task 9; `tm::{Op, Persist, Mode, Role, Commit, Timing, Entity, Outcome, expect, naRule, wireAcks, name}` are defined in Task 11 and used unchanged in Tasks 13 (and the plan's matrix section).
- **Review Focus:** each of the seven has its named test in the owning task (Tasks 5+11, 4+3+12, 3, 5+11, 7+8, 9, 7+8).
- **Philosophy:** shared-nothing — the gateway never reads an `InMemoryLog` (only its replica's published atomic HW), the worker reads only rings it owns (Task 5 clears `replica` on every not-owned re-serve; matrix column `LeaderMovesMidOp` asserts no chunk is read while disowned); rings only — no new channel; state machines — the gateway's partition kind (`ServedPartition::Kind`) and the view's transient cursor (`TransientCursor` variant, `CursorLive` holds exactly its identity) are explicit states, not flags; zero allocation — proven (Tasks 6, 3) and argued (Task 9); test + benchmark — every task has its regression tests, Tasks 6/10 benchmark; fix-on-sight — `applyReplicated`'s per-apply `std::vector` (Task 3), `InMemoryLog`'s release-mode over-capacity corruption (Task 2, was an `assert`), the dropped marker on a refused ring append (Task 4, was `Internal`), the stale `RUNE_MEMORY_LOG_RING_BYTES` name in the plan's docs/log text (now canonical `RUNE_TRANSIENT_LOG_RING_BYTES`).

## Refresh rulings (2026-10-13 — plan refreshed against `6a3b9baa`; the refresher ruled, no question left open)

1. **Ruling 2 was never implemented** (the plan body still answered a full ring `NotLeader`; protocol and client have no such code at `1890349` / `c06348b`). Ruled: implement it now — new Task 1 appends `TransientLogFull = 42` (39-41 are taken), gates it by client version 6 through the existing `produceErrorForClient` (old clients keep getting `NotLeader`, so no client is broken), and the client retries it like `MinIsrViolation` (plain producer) / with a duty-cycle backoff that spends no retry budget (transactional producer).
2. **C1 × transient:** a transient topic is gated exactly like a disk one. At the gateway the C1 state is checked before the transient kind (`servedPartition_`); at the worker nothing new is needed (no replica exists before the commit) — pinned by tests rather than code.
3. **Matrix `Uncommitted` × `Follower` × `CommitLandsMidOp`:** ruled `NotApplicable` (after the commit a follower answers as rule R7 — the `Committed` rows cover it), giving 366 applicable cells.
4. **`TxnMarker` on a follower** asserts the retriable refusal only; landing a marker needs leadership, which the leader cells (and Task 13's failover) cover.
5. **Group consume e2e:** not added — `GroupConsumer` resolves topics from the static `ClientConfig::topics` before `connect()`, and the probe's transient topic is defined at runtime. The `GroupConsume` column runs in the unit matrix (both group subscription paths through the gateway); its shard push path rides the owned-subscription loop Task 13 exercises for plain subscriptions.
6. **The FetchResponse ring-start carrier** stays the existing `logStartOffset` field (no wire change for replication).

## Spec questions (as asked at plan time, against `7a3fdf7`; the controller rulings below are binding — question 2's suggested value 37 is now `ViewSourceTransient`, so the code is 42, Task 1)

1. **Full-ring rule (decided here, please confirm).** `InMemoryLog` is a fixed ring (`RUNE_MEMORY_LOG_RING_BYTES`, default 8 MiB per partition, ≤ 1 000 000 batches) whose only policy today is `DropOldest` — which would evict records not yet replicated (losing acks=all data under a slow follower) and leave readers/followers stalled on evicted offsets. The plan's rule, Kafka-retention-consistent: the ring is the topic's **retention**; it evicts oldest-first **only below the high watermark**; an append needing an eviction at/above the HW is **refused (retriable)**; a reader behind the ring start **resumes at the ring start** (counted, logged); a follower behind the leader's ring start **restarts its ring there**. Alternatives considered: refuse-only (never evict — a transient topic would fill and block forever); evict-anything (loses unacked data, breaks "replicated before ack"); floor at the LSO instead of the HW (also never evict an open transaction's records — would let one long transaction block all produce; Kafka retention is HW-bounded, not LSO-bounded). Also: should transient topics get a per-topic ring size (e.g. `RUNE_TOPICS … ring=64m`, Rune QL option) instead of the node-wide env var?
2. **Refusal code.** A full ring answers `NotLeader` — the only retriable "nothing appended, retry" code the client protocol has (the same choice the worker already makes for a full return ring, review m11). It makes the client refresh metadata on every refusal. A dedicated retriable code (e.g. append-only `ErrorCode::TransientLogFull = 37`, client-retriable, version-gated like `UnknownProducerId`) is cleaner — protocol + client change; deferred unless requested.
3. **`tier=strict` on a transient topic** acks like `majority` (no fsync exists). Alternative: reject `tier=strict` together with `persistence=transient` at config / DefineTopic time. Plan documents the former.
4. **Disk followers behind the leader's retention start have the same stall** this plan fixes for transient rings (a disk leader advertises `logStartOffset = 0` and skips answering an empty read below its first segment). Out of E2's scope (disk path unchanged by design); it needs Kafka-style "truncate fully and resume at the leader's log start" for `Log`. Track separately?
5. **Gateway cross-thread reads of disk `Log::logEndOffset()` / `logStartOffset()`** (subscribe clamp, group subscribe, inline push) are pre-existing reads of worker-owned state on the control thread (a HARD RULE 1 violation, not introduced here; E2 adds none for transient partitions — it uses only the published atomic HW). Fixing it means publishing the disk log start/end like the HW (`publishCleanerState`). Out of E2's scope — confirm a follow-up.
6. **No worker pool (unit-rig gateway inline path)** refuses transient produce / subscribe / group subscribe with `UnknownPartition` and drops markers (no inline append to a worker-owned ring). Production always has a worker pool; confirm the refusal is acceptable rather than an inline transient path.

## Controller rulings on the spec questions (2026-10-11)
1. **Full ring:** accepted as written — the ring is the transient topic's retention; evict only records below the high watermark; refuse a produce that would evict unreplicated records (retriable); lagging consumers/followers resume at the oldest held record. Ring size stays node-wide (`RUNE_TRANSIENT_LOG_RING_BYTES`, `RUNE_MEMORY_LOG_RING_BYTES` alias per Part E); per-topic sizing is a follow-up.
2. **Refusal code:** do NOT reuse `NotLeader` (it triggers a client metadata refresh on every refusal). Append a dedicated retriable error code `TransientLogFull` to the protocol `ErrorCode` enum (append-only), map it in the client as `BackPressured`-class retriable without a metadata refresh. Executors add this to the protocol and client in the task that introduces the refusal.
3. **`tier=strict` on a transient topic:** acks like `majority`, documented (there is no fsync to wait for).
4. **Disk followers stalling below the leader's retention start:** out of scope for 2b; recorded as a follow-up.
5. **Gateway cross-thread reads of disk log offsets:** a real shared-nothing violation (CLAUDE.md rule 1), pre-existing and outside 2b; recorded as a follow-up plan to raise with the user. E2 adds no new cross-thread access.
6. **No-worker-pool unit-test path refusing transient topics with `UnknownPartition`:** accepted.
