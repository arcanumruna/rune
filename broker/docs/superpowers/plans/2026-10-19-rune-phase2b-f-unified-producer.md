# Rune Phase 2b — Part F: One Producer (transactions on every produce path), broker-assigned producer ids, consumer-group rewind — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Producer` with `ProducerConfig::transactionalId` is the only transactional producer: `initTransactions` / `beginTransaction` / `sendOffsetsToTransaction` / `commitTransaction` / `abortTransaction` / `flush` (+ `*AndWait`) work with **every** produce path — `send`, `sendTo`, `trySend` (all ack forms), `batch()`, `tryClaim`, `claimBatch`, `TypedTopic::send` and the typed `claim`. `TransactionalProducer` / `TransactionalProducerImpl` / `Client::transactionalProducer()` are deleted. A non-transactional acked producer gets its producer id from the broker (`InitProducerId` with no transactional id) instead of a client-drawn random id. `GroupConsumer::seekToCommitted()` / `seek()` rewind a consumer after an aborted transaction without a restart.

**Architecture:** All transactional state moves into the duty side of `ProducerImpl` as two explicit per-state-data machines: a producer **identity** machine (`IdUnassigned → IdRequested → IdAssigned | IdFailed`, used by every acked producer) and a **transaction** machine (`Uninitialized → Initializing → Ready → InTransaction → Committing | Aborting → Ready`, `AbortableError`, `FatalError`; each state holds only its own variables). Coordinator requests (InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn) go through a `CoordinatorLink` owned by the producer: a fixed pool of request slots with preallocated frames, per-slot state variants (`Free / Unsent / InFlight / Backoff / AwaitCoordinator`), coordinator-tagged correlation ids routed back to the producer by its ordinal. Transaction control ops travel on the **producer's own ring** (new `kMsgTxnControl = 5`) so they are ordered with the records sent before them; in Invoker mode they apply directly on the owner thread. The data path is the plain producer's: records accumulate in the partition's `ProduceFrameBuilder` / Tier-2 queue as today, a partition's batches are held (flush skips it) until its `AddPartitionsToTxn` is OK, and transactional batches carry the TRANSACTIONAL attribute bit. The app-side handle keeps a one-byte gate (`TxnGate`) written only by the producer's single issuing thread, so a produce outside a transaction is refused synchronously. The group consumer's rewind reuses its OffsetFetch-on-assign and subscription-restart paths.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark + the client's in-process bench harness, CMake (client `build` Unix Makefiles + `build_tsan`; broker `build-fast` Ninja, `build-bench-rel`), real Aeron IPC (`InvokerGateway`) for client tests, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`) in the broker's `rune_client_broker_tests`, docker compose (3 nodes) on the k3s VM for e2e.

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §F (F1, F2, F3, Gates) — binding.

**Builds on / order:** auth → D → B → **F** → A. F executes after Part B merged (it consumes `Producer::tryClaim`, `claimBatch`, `Claim`, `BatchClaim`, `TypedTopic::claim`, `SendResult::ClaimHeld`, `kMsgClaimBatch = 4`). What exists today (verified 2026-10-19 on `feat/catalog-history`; line numbers are hints, the quoted anchors bind):

- **`TransactionalProducer`** (`include/rune/client/producer/TransactionalProducer.h`, `src/producer/TransactionalProducer.cpp`) + **`TransactionalProducerImpl`** (`TransactionalProducerImpl.{h,cpp}`, 1638 lines): its own data path (`TxnPartitionState::waiting` = `std::deque<TxnSendRecord>` with a heap `std::vector<uint8_t>` value per record, `InFlightBatch::wireFrame` vectors), its own request machinery (`TxnRequest{kind, corrId, std::vector<uint8_t> frame, state variant}`, `classify_` → `Verdict`), `TxnSm` states (`SUninitialized … SFatalError`, several carrying booleans: `started`, `endSent`, `maybeStarted`, `bumpEpoch`, `ended`), commands on the agent command ring (`HubCommand::Op::TxnInit … TxnFlush`, 30–37; `kMsgRegisterTxnProducer = 7` in `ClientAgent.h`), responses broadcast to every txn producer (`Client.cpp` anchor `case codec::TemplateId::InitProducerIdResponse:`), data frames sent through `txnSendFn_` (a `std::function`, `Client.cpp` ~`:279`). Users: `examples/dr_client.cpp`, `examples/view_probe.cpp`, `tests/producer/TransactionalProducerTest.cpp` (70 tests), `tests/client/{HandleLifetimeTest,HubCommandClientTest,InvokerClientTest,GroupRoutingRegressionTest,TlsConnectEdgeRegressionTest}.cpp`, broker `libs/node/tests/client/ClientFailoverBrokerRegressionTest.cpp` (`SeedCoordinatorCrashedMidTransactionEndsCommittedOrRetriableRegression`), `dr-e2e/tests/test_tx_fencing.py` (through `dr_client`).
- **Plain producer identity:** `ProducerImpl::producerId_` drawn by `generateProducerId(this)` (`ProducerImpl.cpp` anchor `// producerId generation (F5 fix)`, `TODO(milestone-c)`); epoch overflow draws a fresh id; `PartitionState::{producerId, epoch, nextBaseSequence}` per partition; acks=None batches are `-1/-1/-1`.
- **Broker:** `TransactionCoordinationService::handleInitProducerId` already serves an **empty** transactional id (idempotent-only: a fresh id from the pid block, nothing persisted); every txn request on a non-coordinator broker is answered `ErrorResponse{NotCoordinator, message = the coordinator's client endpoint}` (`ClientGateway::redirectToCoordinator_`). **No broker change is needed** (spec F: "broker only if a gap is found"; Task 4's broker test proves it).
- **Group consumer:** `GroupConsumerImpl` (`include/rune/client/group/GroupConsumer.h`): `assignment` (`kUnseededSentinel = -1` until OffsetFetch), `fetch` (`FetchSub` per partition), `GroupConsumerHub::{sendOffsetFetchFor_, reconcileImpl_, dropFetch_}`; no seek API.
- **Correlation-id tags in use:** `batchcorr::kTag = 1 << 62`, `GroupConsumerHub::kFetchCorrTag = 1 << 61`, `kOffsetFetchCorrTag = 1 << 60`.
- **Auth (merged before D):** `TxnInit` needs `WRITE TRANSACTIONAL_ID`; `TxnProduce` needs that plus `WRITE TOPIC`; `AddOffsetsToTxn` / `TxnOffsetCommit` need `WRITE TRANSACTIONAL_ID` + `READ GROUP` (auth D18); a non-transactional `InitProducerId` needs nothing (D18); codes 44–47; `SessionRevoked` is terminal (D26). The ISR storage-identity redesign changes replication / metadata only — nothing here.

## Execution context

- **Repos / branch:** `feat/phase2b-f-unified-producer` in rune-client-cpp and rune-broker (tests + docker + docs only in the broker; no broker library change), cut from each repo's integration tip after Part B merged:
  ```bash
  git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/phase2b-f-unified-producer <B-tip>
  git -C /Users/vradu/Projects/se-worktrees/rune-broker      switch -c feat/phase2b-f-unified-producer <B-tip>
  ```
  Verify before Task 1: `grep -n "tryClaim" include/rune/client/producer/Producer.h`, `grep -n "kMsgClaimBatch" include/rune/client/producer/ProducerImpl.h`, `grep -n "ClaimHeld" include/rune/client/producer/Producer.h` (client) all hit. rune-protocol, rune-codec, rune-ql stay on their integration tips (no change).
- **Builds:** client `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14` (Debug, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`, `BUILD_BENCHMARKS=ON` for Task 14), client TSan `build_tsan`; broker `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_client_broker_tests rune_client_failover_broker_tests` (compiles the client from `RUNE_CLIENT_ROOT=../rune-client-cpp`).
- **Test gate (CLAUDE.md #7):** per task, the executables covering the touched code plus every new or changed test (each task's Run step, `-j14`). The FULL `*Regression*` suite runs at the checkpoints — **A** after Task 4, **B** after Task 9, **C** after Task 13, **D** after Task 15 (before the final whole-branch review) — in both repos (`ctest --test-dir <build> -R Regression --output-on-failure -j14`), plus client `build_tsan` and broker `build-asan` / `build-tsan`. A checkpoint failure is bisected over the commits since the last green checkpoint.
- **Docker e2e** (Task 15) only on the k3s VM (`ssh root@$RUNE_VM_HOST`, `flock /root/vm.lock`), never on the Mac.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- **HARD RULES (CLAUDE.md, all eight), as they bind here:**
  1. **Shared-nothing.** The identity machine, the transaction machine, the `CoordinatorLink`, per-partition txn membership and the status ring's producer side are owned by the producer's duty thread (the Thread-mode runner, or the Invoker owner inside `doWork()`). The handle's `TxnGate` is owned by the producer's single issuing thread (a transactional producer has exactly one issuing thread — today's `TransactionalProducer` rule, kept). No atomics as coordination, no locks, no `thread_local`.
  2. **Rings only.** Txn control ops: app → duty on the producer's own M2O ring (`kMsgTxnControl`); state / ids: duty → app on a `detail::StatusRing<ProducerStatus, N>`; completions on the existing completion rings / handlers; group seeks on the agent command ring (`HubCommand`). No cross-thread callback.
  3. **Per-state data.** `ProducerIdentity = std::variant<IdUnassigned, IdRequested, IdAssigned, IdFailed>`; `TxnMachine = std::variant<Uninitialized, Initializing, Ready, InTransaction, Committing, Aborting, AbortableError, FatalError>` with end phases `std::variant<AwaitData, EndSent, InDoubt, Bumping>`; per partition `TxnMembership = std::variant<NotInTxn, Adding, Added, AddFailed>`; per request slot `std::variant<Free, Unsent, InFlight, Backoff, AwaitCoordinator>`; the handle gate `enum class TxnGate : uint8_t { NotTransactional, Closed, Open }`. The booleans of today's `TxnSm` (`started`, `maybeStarted`, `endSent`, `bumpEpoch`, `ended`) become states / enums (`TxnStarted { No, Maybe, Yes }`, `AbortKind { Plain, BumpEpoch }`, the end-phase variant).
  4. **Zero allocation** on every produce path and in steady state, transactional or not: no per-record / per-batch / per-transaction heap. Request frames, AddPartitions / TxnOffsetCommit entry scratch and the offsets payload are preallocated at producer construction (capacity persists). Fix-on-sight: the Tier-2 heap `Tier2Entry` + `std::deque` (Task 3) and `TransactionalProducerImpl`'s per-record heap (deleted, Task 9).
  5. **Test and benchmark everything:** unit (fake coordinator), client integration (real Aeron IPC, `InvokerGateway`), real-broker integration (`OneBroker` / `ThreeNodes`), the parameterized interaction matrix (Task 12), seeded model-checked stress (Task 13), docker e2e (Task 15); benchmarks: transactional vs non-transactional per-record p50/p99 + allocations on every path, plain produce p50/p99 **non-regression** (Task 14).
  6. **A `*Regression*` test on every fix** (each named below).
  7. **Affected-area tests per task; the full `*Regression*` suite at checkpoints A–D** (+ sanitizers).
  8. **Fix-on-sight** (owned by the task named): Tier-2 per-batch heap (Task 3 — `ProducerTier2Regression.BatchSendAllocatesNothingInSteadyStateRegression`); the client-drawn producer id with no fencing (Task 4 — `ProducerIdRegression.AckedProducerUsesTheBrokerAssignedIdRegression`); `TransactionalProducerImpl`'s per-record heap and std::function data sink (deleted, Task 9 — `ProducerTxnAllocRegression.SteadyStateTransactionsAllocateNothingRegression`, Task 14); txn responses broadcast to every producer (Task 2: routed by ordinal — `CoordinatorLinkRegression.AResponseReachesOnlyItsProducerRegression`).
- **Spec F (verbatim rules):** `ProducerConfig::transactionalId` (empty = non-transactional, today's behaviour); the lifecycle names and semantics of today's `TransactionalProducer` (`initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction`, `flush`, `*AndWait`, `state()` (`TxnState`), `producerId()`, `producerEpoch()`, `transactionActive()`); every produce path participates while `InTransaction`; the first record to a partition in a transaction emits `AddPartitionsToTxn` and the partition's records are held until it is OK; misuse is explicit (lifecycle on a non-transactional producer → token forms `OpResult::InvalidArgument`, `*AndWait` `std::logic_error`; a produce on a transactional producer outside `InTransaction` is refused — `trySend` / `tryClaim` / `claimBatch` return it, `send` throws `std::logic_error` — never silently dropped); `acks = None` with a `transactionalId` is rejected at creation; `TransactionalProducer` and `Client::transactionalProducer()` are removed (no alias); a non-transactional producer with `acks = Leader|All` obtains its id from the broker on connect, retries stay deduplicated across reconnects; `acks = None` keeps `-1/-1/-1`; `GroupConsumer::seekToCommitted()` (+ `AndWait`, token form) and `GroupConsumer::seek(TopicPartition, int64_t)` (not assigned → `InvalidArgument`).
- **Append-only values:** `SendResult::NotInTransaction` and `RejectReason::NotInTransaction` appended after Part B's `ClaimHeld`; ring message `kMsgTxnControl = 5` after `kMsgClaimBatch = 4`; `OpKind::SeekToCommitted`, `OpKind::Seek` appended after `AlterTopicConfig`; `HubCommand::Op::GroupSeekToCommitted = 15`, `GroupSeek = 16`; `HubCommand::Op` 30–37 (`Txn*`) and `ClientAgent::kMsgRegisterTxnProducer = 7` are **retired, never reused** (comment left in place). No wire change.
- **No hangs:** every token op completes exactly once (Ok, BrokerError, Fenced, Timeout, Closed, InvalidArgument, DeliveryFailed, SessionLost, or the auth phase's SessionRevoked status); every coordinator request is bounded by its op deadline (`transactionOpTimeoutMs`) or `produceTimeoutMs` (AddPartitions); a void send refused by the gate throws, it is never dropped.
- **Latency gate:** plain-producer produce p50 / p99 must not regress (Task 14: `BM_ProduceRTT` and `rune_producer_acked_bench` within +5 % p50 / +10 % p99 of the parent-commit baseline on the same machine, median of 5 runs; the existing absolute gate `p99 < 100 µs` holds). The hot path gains one predictable branch on the handle (`txnGate_ != NotTransactional`) and, on the duty side, one variant-index compare per partition flush.

## Review Focus

Most likely to bite a user, each pinned by a named test:

1. **A record sent before `commitTransaction()` must be in that transaction, and one sent after it must not** (Thread mode: records and the commit travel different queues today) → `ProducerTxnRegression.RecordsSentBeforeCommitAreInItAfterAreRefusedRegression` (Task 8), matrix timing `AfterCommitIssued` (Task 12).
2. **A partition's records must never reach the log before its `AddPartitionsToTxn` is OK** (else the broker writes a transactional batch outside the txn) — on every path, including claims and Tier-2 batches → `ProducerTxnDataRegression.NoProduceBeforeAddPartitionsOkOnAnyPathRegression` (Task 6), matrix entity `TxnNewPartition`.
3. **Abort then `seekToCommitted` must redeliver exactly the records after the committed offset** — no gap, no duplicate in the output → `GroupSeekRegression.SeekToCommittedRedeliversExactlyTheUncommittedRecordsRegression` (Task 10), `EosBrokerRegression.AbortRewindRedoOutputEqualsInputRegression` (Task 11), the e2e count check (Task 15).
4. **A plain acked producer's retried batch after a reconnect is deduplicated under its broker-assigned id**, and records sent before the id arrives are held, not sent with a guessed id → `ProducerIdRegression.RecordsBeforeTheIdArrivesAreHeldNotSentRegression` (Task 4), `ProducerIdBrokerRegression.RetriedBatchAcrossAReconnectIsDeduplicatedRegression` (Task 11).
5. **A fenced producer (a second producer with the same `transactionalId`) never commits** and every pending op completes `Fenced` → `ProducerTxnRegression.FencedByASecondProducerFailsEverythingRegression` (Task 5), `EosBrokerRegression.ZombieIsFencedAndItsRecordsAreInvisibleRegression` (Task 11).
6. **Steady-state transactions allocate nothing** (the old txn producer allocated per record) → `ProducerTxnAllocRegression.SteadyStateTransactionsAllocateNothingRegression` (Task 14).

---

## Decisions (binding for executors)

- **FD1 — Control ops ride the producer ring.** `initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction`, `flush` are `kMsgTxnControl` messages in the producer's own M2O ring (Thread mode) — FIFO with the records before them, which is what makes Review Focus #1 hold. Invoker mode applies them directly on the owner thread (`WrongThread` off it). `close()` / `closeImmediately()` keep their existing path (`hooks_.issueClose`); a begin-close on a transactional producer first aborts an open transaction (today's `TransactionalProducer::close` semantics).
- **FD2 — The handle gate.** `TxnGate` lives in `ProducerImpl`'s issuing-thread section: `NotTransactional` (no `transactionalId`: the hot path checks only this, `[[likely]]`), `Closed`, `Open`. `beginTransaction()` sets `Open` (after its ring write succeeds); `commitTransaction` / `abortTransaction` / `close*` set `Closed` **at issue**. A produce while `Closed` returns `NotInTransaction` (`send` throws `std::logic_error("produce outside a transaction")`). The duty side re-checks: a record that reaches it while the machine is not `InTransaction` (e.g. the duty side went `AbortableError` / `FatalError` after the handle opened) is never produced; acked forms complete `BrokerError(19 InvalidTxnState)` (`AbortableError`: the transaction's status; `FatalError`: `Fenced`), void ones are counted (`txnRecordsRefusedDuty`) and fail the commit.
- **FD3 — One issuing thread per transactional producer** (documented on `ProducerConfig::transactionalId`; Thread mode). A plain producer keeps "any thread". `ClientShard` refuses a `transactionalId` (`std::invalid_argument`): shards own one producer per shard and a transaction spanning shards is out of scope.
- **FD4 — Identity for every acked producer.** `acks = Leader|All` (transactional or not) starts `IdUnassigned`; the first duty pass with a connected coordinator session sends `InitProducerId` (empty id for a plain producer; the transactional id for `initTransactions`). Batches of an acked producer are held (builders keep filling, back-pressure as today) until `IdAssigned`; `IdFailed` fails every pending and future record (`DeliveryFailed`, errorCode = the broker's). A bare `ProducerImpl` with no `CoordinatorLink` wired (unit rigs, benches) starts `IdAssigned{kRigProducerId = 0x7E57'0000'0000'0001, epoch 0}` so the existing producer tests and benches run unchanged. Epoch overflow of a plain producer → a new `InitProducerId` (cold), batches held meanwhile.
- **FD5 — Coordinator request slots.** `CoordinatorLink` holds `kMaxCoordRequests = 8` slots with `kCoordFrameCap = 4096`-byte frames. AddPartitions / TxnOffsetCommit entries that do not fit one frame are split across slots (FIFO); a pass that finds no free slot leaves the work for the next pass. Correlation ids `coordcorr::make(ordinal, slot, seq)` with tag `1 << 59` (static_asserted distinct from the three tags in use). Retries: `ConcurrentTransactions (27)` → `Backoff` `min(2^attempt ms, 100 ms)` on the duty clock (wall time, not cycles — the M3 fix); `NotCoordinator (14)` → `AwaitCoordinator` until the Client re-points the coordinator session (`onCoordinatorMoved()`); a session change re-sends `InFlight` requests on the new session (same correlation id; idempotent at the coordinator).
- **FD6 — Error classification** (Kafka parity, today's `classify_` plus auth): InitProducerId `17/18` → `FatalError(Fenced)`; `45 NotAuthorized` on InitProducerId or EndTxn → `FatalError(BrokerError 45)`; `45` on AddPartitions / AddOffsets / TxnOffsetCommit / a transactional Produce → `AbortableError(BrokerError 45)`; `19` on EndTxn → `FatalError`; `19` / unknown on AddPartitions / AddOffsets / TxnOffsetCommit → `AbortableError`; the auth phase's `SessionRevoked` → `FatalError` with that status (the session never reconnects, auth D26); `39 InvalidRequest` → never retried, completes `BrokerError(39)` (`AbortableError` inside a txn).
- **FD7 — Shared typed topics.** `TypedTopic<T, ProducerT>` owns its producer by value today. A typed topic that writes inside another producer's transaction uses the new borrowed handle `ProducerRef` (non-owning, trivially copyable, forwards `send` / `trySend` / `tryClaim` / `claimBatch` / `handle checks` to a `Producer&`): `TypedTopic<T, ProducerRef>`; `Client::openTopicAndWait<T>(name, Producer& shared, timeout)` returns one. The `ProducerRef` must not outlive its `Producer` (documented, debug-asserted through `ProducerImpl::handleDetached()`).
- **FD8 — `seekToCommitted` semantics.** For every partition assigned at the moment the command applies: drop its fetch subscription (`dropFetch_` — pushes of the old instance are ignored by instance id), set its position to `kUnseededSentinel` (auto-commit can never commit a stale position), mark `offsetKnown = false`, send one OffsetFetch for them; `reconcileImpl_` resubscribes from the answer. Completes `Ok` once every partition is re-subscribed from the fetched offset, `Rejoined` if the generation changes first, `Timeout` at `commitSyncTimeoutMs`. `seek(tp, offset)`: not assigned → the token form completes `OpStatus::InvalidArgument`, `seekAndWait` throws `std::invalid_argument` (the check needs duty-side state — G6); else drop + position = offset + `offsetKnown = true` + reconcile; completes `Ok` when re-subscribed. Records already handed to `onRecords` before the command applied stay delivered (documented).
- **FD9 — Visibility for tests:** a `ProducerStatus` (`producerId`, `producerEpoch`, `state`, `txnActive`) is published on a per-producer `StatusRing<ProducerStatus, 64>` on every identity / txn transition; `producerId()` / `producerEpoch()` / `state()` / `transactionActive()` drain it (one app thread, as today's txn handle).
- **FD10 — Latency budget** as in Global Constraints; numbers recorded in the Task 14 commit body.

---

## Interaction matrix

**Single source of truth:** `rune-client-cpp/tests/support/TxnMatrixExpectations.h` (Task 12) — `expect(Op, Entity, Timing) → Outcome`, `naRule(Op, Entity, Timing) → std::optional<NaRule>`. The unit matrix (Task 12), the stress model (Task 13) and the broker slice (Task 11) read it; no test hard-codes an outcome.

### Operations (10)

| Op | Issued as |
|---|---|
| `Send` | `p.send(t, key, value)` (void) |
| `SendTo` | `p.sendTo(t, partition, key, value)` |
| `TrySendCq` | `p.trySend(t, key, value, cq, token)` |
| `TrySendAck` | `p.trySend(t, key, value, AckToken{token})` |
| `Batch` | `p.batch(t, key).add(...)×3.send()` (Tier-2) |
| `TryClaim` | `p.tryClaim(t, key, len)->commit()` |
| `ClaimBatch` | `p.claimBatch(t, key, 3, len)->commit()` |
| `TypedSend` | `TypedTopic<Position, ProducerRef>::send(rec)` |
| `TypedClaim` | `TypedTopic<Position, ProducerRef>::claim(k).commit()` |
| `SendOffsets` | `p.sendOffsetsToTransaction(offsets, group, cq, token)` |

### Entities (4)

`PlainNoAck` (no `transactionalId`, `acks = None`) · `PlainAcked` (no `transactionalId`, `acks = All`, broker-assigned id) · `TxnNewPartition` (transactional; the op's partition is not yet in the current transaction) · `TxnAddedPartition` (transactional; already added this transaction).

### Timings (8)

| Timing | What the rig does before / around the op |
|---|---|
| `Steady` | initialized (txn: `initTransactions` OK, `beginTransaction`); op; (txn: `commitTransaction`) |
| `BeforeInit` | txn producer created, `initTransactions` not called; op |
| `OutsideTxn` | init OK, no `beginTransaction` (or after a completed commit); op |
| `AfterCommitIssued` | begin; one record; `commitTransaction` issued (not completed); op |
| `AbortableError` | begin; the fake answers the first AddPartitions `InvalidTxnState (19)`; op; commit; abort |
| `Fenced` | begin; a second init with the same `transactionalId` bumps the epoch; the next coordinator answer is `ProducerFenced (18)`; op; commit |
| `SessionLostMid` | op issued; the fake drops the session with the frame unanswered; the client reconnects; (txn: commit) |
| `AfterAbortRewind` | begin; op; abort; `seekToCommitted` on the rig's group consumer; begin; the same op again; commit |

### Not applicable (top-down; the first match names the reason)

| # | Rule | Cells |
|---|---|---|
| NA1 | `PlainNoAck` / `PlainAcked` × every timing except `Steady`, `SessionLostMid` (a plain producer has no transaction phases) | 120 |
| NA2 | `SendOffsets` × `PlainNoAck` / `PlainAcked` (an offsets leg exists only in a transaction) | 4 |
| NA3 | `SendOffsets` × `TxnNewPartition` (offsets carry no data partition; `TxnAddedPartition` stands for "the transactional producer") | 8 |

### Expected outcome per applicable cell (`expect()`, top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | Plain × `Steady` | `Delivered`: acked forms complete `Ok`; a `read_uncommitted` and a `read_committed` reader each see every record once; `PlainAcked` batches carry the broker-assigned id (FD4), `PlainNoAck` `-1/-1/-1` |
| R2 | `PlainAcked` × `SessionLostMid` | `DeliveredOnce`: the batch is re-sent with the same (pid, epoch, sequence) and deduplicated — exactly one copy in the log |
| R3 | `PlainNoAck` × `SessionLostMid` | `AtMostOnce`: zero or one copy, never two (no retransmit) |
| R4 | Txn × `BeforeInit` | `Refused`: `NotInTransaction` (void: `std::logic_error`); `SendOffsets`: `BrokerError(19)` — nothing reaches the fake |
| R5 | Txn × `OutsideTxn` | `Refused` (as R4) |
| R6 | Txn × `AfterCommitIssued` | `Refused` (as R4); the commit completes `Ok` with only the record sent before it |
| R7 | Txn × `Steady` | `CommittedVisible`: commit `Ok`; `read_committed` sees each record exactly once, `read_uncommitted` too; `TxnNewPartition`: exactly one `AddPartitionsToTxn` naming the partition precedes its first `Produce`; `TxnAddedPartition`: none; `SendOffsets`: the group's committed offset equals the sent one after commit |
| R8 | Txn × `AbortableError` | `FailsTxn`: the op is accepted or refused per FD2, never visible to `read_committed`; commit completes `BrokerError(19)`; abort `Ok`; state `Ready` after abort |
| R9 | Txn × `Fenced` | `Fenced`: nothing visible to `read_committed`; every token op and the commit complete `Fenced`; state `FatalError`; no further frame leaves the producer |
| R10 | Txn × `SessionLostMid` | `CommittedVisibleOnce`: records re-sent on the new session with their sequences, commit `Ok` (or in-doubt `Timeout` then a retried commit `Ok`); `read_committed` sees each exactly once |
| R11 | Txn × `AfterAbortRewind` | `RedoneOnce`: the aborted attempt invisible; the redo committed and visible exactly once; the group's committed offset advanced exactly by the redo |

### Cell counts (pinned by `TxnMatrixCountRegression.CellCountsArePinned`, Task 12)

10 ops × 4 entities × 8 timings = **320 cells**: **188 applicable**, **132 `NotApplicable`** (NA1 120, NA2 4, NA3 8). Applicable by rule: R1 18, R2 9, R3 9 (plain: 9 data ops × 2 entities × 2 timings = 36); R4–R11 19 each (txn: 10 ops × `TxnAddedPartition` + 9 data ops × `TxnNewPartition` = 19 per timing × 8 = 152). 36 + 152 = 188.

### Every cell asserts

1. The outcome's codes from `expect()` — no cell hard-codes one.
2. **Every accepted op completes exactly once** (acked forms; the rig counts completions per token) and every refused op had no side effect (the fake saw no frame for it).
3. **No transactional batch before its partition's AddPartitions OK** (the fake records the order of every frame per partition).
4. **Batch attributes:** every batch of a transactional producer has the TRANSACTIONAL bit; no plain batch has it.
5. **Zero allocation** inside the op (the rig's `AllocCounter` window around the issue call, after a warm-up pass of the same cell).

### Coverage by layer

| Layer | Task | Cells |
|---|---|---|
| Parameterized unit matrix (`TEST_P` over all 320; N/A skipped naming its rule) — `TxnMatrixRig`: a real `Client` in Invoker mode over Aeron IPC to `InvokerGateway` extended into a transaction-aware fake (`TxnFakeBroker`: AddPartitions / EndTxn / TxnOffsetCommit / Produce with a read-committed view, scripted 19 / 18 / session drops) | 12 | all 188 applicable |
| Seeded model-checked stress (independent `TxnModel`) | 13 | random interleavings of the 9 data ops, begin / commit / abort / flush / sendOffsets / seekToCommitted, scripted 27 / 14 / 19 / 18 / session drops; 64 seeds × 400 steps; invariants: committed data visible exactly once, aborted never, no produce before AddPartitions OK, every token completes once, sequences gap-free per (pid, epoch, partition) |
| Real-broker slice — `OneBroker` / `ThreeNodes`, real client | 11 | {`Send`, `Batch`, `TryClaim`, `ClaimBatch`, `TypedClaim`, `SendOffsets`} × {`TxnNewPartition`, `TxnAddedPartition`} × {`Steady`, `Fenced`, `SessionLostMid` (leader crash), `AfterAbortRewind`} (44 applicable cells) + `PlainAcked` × {`Steady`, `SessionLostMid`} × 9 ops (18) |
| Docker e2e (3 nodes, authenticated) | 15 | the exactly-once pipeline over every path with injected aborts and a coordinator kill |

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-client-cpp/include/rune/client/producer/Producer.h` | `ProducerConfig::{transactionalId, transactionTimeoutMs}`; `SendResult::NotInTransaction`; the lifecycle API; `TxnState` (moved from `TransactionalProducer.h`); `TopicPartitionKey` (moved) | 1, 8 |
| `rune-client-cpp/include/rune/client/producer/ProducerRef.h` (new) | the borrowed handle (FD7) | 8 |
| `rune-client-cpp/include/rune/client/producer/TxnTypes.h` (new) | identity / txn / membership / request-slot state variants, `ProducerStatus`, `coordcorr` | 2, 4, 5 |
| `rune-client-cpp/include/rune/client/producer/CoordinatorLink.h`, `src/producer/CoordinatorLink.cpp` (new) | request slots, frames, retries, routing | 2 |
| `rune-client-cpp/include/rune/client/producer/ProducerImpl.h`, `src/producer/ProducerImpl.cpp` | gate, identity machine, txn machine, membership gating, transactional attributes, `kMsgTxnControl`, Tier-2 pool | 1, 3, 4, 5, 6, 7, 8 |
| `rune-client-cpp/src/producer/ProducerTxn.cpp` (new) | the transaction machine's bodies (keeps `ProducerImpl.cpp` from growing past ~3.5k lines) | 5, 6, 7 |
| `rune-client-cpp/include/rune/client/producer/ProduceFrameBuilder.h` | `setTransactional(bool)` (attributes) | 6 |
| `rune-client-cpp/include/rune/client/producer/BatchBuilder.h`, `src/producer/BatchBuilder.cpp` | Tier-2 bytes into a pooled slot | 3 |
| `rune-client-cpp/include/rune/client/producer/ProducerHub.h`, `src/producer/ProducerHub.cpp` | `onCoordinatorResponse`, `onCoordinatorMoved`, coordinator sink | 2 |
| `rune-client-cpp/src/producer/Producer.cpp` | handle API, gate checks on every path, AndWait | 1, 8 |
| `rune-client-cpp/include/rune/client/Client.h`, `src/Client.cpp` | producer validation, coordinator sink wiring, response routing, `openTopicAndWait(name, Producer&)`; delete `transactionalProducer`, `txnProducers_`, `txnSendFn_`, `findTxn_` | 1, 2, 8, 9 |
| `rune-client-cpp/include/rune/client/TypedTopic.h` | works over `ProducerRef` | 8 |
| `rune-client-cpp/include/rune/client/group/GroupConsumer.h`, `src/group/GroupConsumer.cpp`, `include/rune/client/group/GroupConsumerHub.h`, `src/group/GroupConsumerHub.cpp`, `include/rune/client/detail/HubCommand.h`, `include/rune/client/Completion.h` | `seekToCommitted`, `seek`, ops 15 / 16, `OpKind::{SeekToCommitted, Seek}` | 10 |
| `rune-client-cpp/include/rune/client/producer/TransactionalProducer{,Impl}.h`, `src/producer/TransactionalProducer{,Impl}.cpp` | **deleted** | 9 |
| `rune-client-cpp/include/rune/client/agent/ClientAgent.h`, `src/agent/ClientAgent.cpp` | retire `kMsgRegisterTxnProducer` | 9 |
| `rune-client-cpp/tests/support/FakeCoordinator.h` (new), `tests/support/InvokerGateway.h` (txn-aware: `TxnFakeBroker` mode), `tests/support/TxnMatrixExpectations.h` (new) | fakes, expectations | 2, 12 |
| `rune-client-cpp/tests/producer/{CoordinatorLinkTest,ProducerTxnApiTest,ProducerTier2PoolTest,ProducerIdTest,ProducerTxnTest,ProducerTxnDataTest,ProducerTxnOffsetsTest}.cpp` (new), `tests/client/{ProducerTxnClientTest,GroupSeekTest,TxnMatrixTest,TxnStressTest}.cpp` (new) | tests | 1–13 |
| `rune-client-cpp/tests/producer/TransactionalProducerTest.cpp` | **migrated** into `ProducerTxnTest.cpp` / `ProducerTxnDataTest.cpp` / `ProducerTxnOffsetsTest.cpp`, then deleted | 9 |
| `rune-client-cpp/bench/ProducerTxnBench.cpp` (new) | txn vs plain per-record p50/p99 + allocations, every path | 14 |
| `rune-client-cpp/examples/{dr_client,view_probe}.cpp`, `examples/eos_probe.cpp` (new) | migrated; the e2e probe | 9, 15 |
| `rune-broker/libs/node/tests/client/{EosBrokerRegressionTest,ProducerIdBrokerRegressionTest}.cpp` (new), `ClientFailoverBrokerRegressionTest.cpp`, `libs/node/CMakeLists.txt` | real-broker tests; migration | 9, 11 |
| `rune-broker/docker/eos-e2e.sh`, `docker/docker-compose.eos.yml` (new), `docker/topics/eos-e2e.rql` (new), `dr-e2e/harness/driver.py` (if it names the txn producer) | e2e | 15 |
| `rune-client-cpp/README.md`, `rune-client-cpp/CLAUDE.md`, `rune-broker/docs/release-notes/phase2b-f-unified-producer.md` (new) | docs | 9, 15 |

---

### Task 1: API surface — `transactionalId`, creation validation, `NotInTransaction`, the handle gate, misuse on a plain producer

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h`, `src/producer/Producer.cpp`, `include/rune/client/producer/ProducerImpl.h` (gate + `RejectReason::NotInTransaction`), `src/producer/ProducerImpl.cpp` (`trySubmit` / claim entry / Tier-2 submit check the gate), `include/rune/client/Client.h`, `src/Client.cpp` (`Client::producer` validation), `include/rune/client/shard/ClientShard.h` (refuse a txn id)
- Test: `rune-client-cpp/tests/producer/ProducerTxnApiTest.cpp` (new; add to `rune_producer_tests` in `CMakeLists.txt`)

**Interfaces:**
- Produces:
  ```cpp
  namespace rune::client {
  struct ProducerConfig {
      // … existing fields …
      /// Phase 2b F: non-empty = a transactional producer (one issuing thread — FD3).  acks must be Leader / All.
      std::string transactionalId;
      int32_t     transactionTimeoutMs = 60'000;   // InitProducerId's transactionTimeoutMs (was a ctor arg)
  };
  enum class SendResult : uint8_t { /* … B's values …, ClaimHeld, */ NotInTransaction };   // appended last
  // TxnState and TopicPartitionKey move here from TransactionalProducer.h (unchanged values / layout).
  class Producer {
  public:
      // ---- Transactions (Phase 2b F) — token forms: exactly one completion after Accepted ----
      OpResult initTransactions(CompletionTarget target, Token token);
      void     beginTransaction();                    // throws std::logic_error on a plain producer
      OpResult sendOffsetsToTransaction(std::span<const OffsetEntry> offsets, std::string_view groupId,
                                        CompletionTarget target, Token token);
      OpResult commitTransaction(CompletionTarget target, Token token);
      OpResult abortTransaction(CompletionTarget target, Token token);
      OpResult flush(CompletionTarget target, Token token);   // the existing void flush() stays
      void initTransactionsAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
      void sendOffsetsToTransactionAndWait(std::span<const OffsetEntry>, std::string_view groupId,
                                           std::chrono::nanoseconds timeout = kDefaultTxnWait);
      void commitTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
      void abortTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
      void flushAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
      [[nodiscard]] TxnState state() const noexcept;
      [[nodiscard]] int64_t  producerId() const noexcept;      // -1 until assigned
      [[nodiscard]] int16_t  producerEpoch() const noexcept;
      [[nodiscard]] bool     transactionActive() const noexcept;
      [[nodiscard]] bool     transactional() const noexcept;
      static constexpr std::chrono::nanoseconds kDefaultTxnWait = std::chrono::seconds(60);
  };
  /// One offset of sendOffsetsToTransaction (a span, not a std::map: no allocation at the call).
  struct OffsetEntry { int32_t topicId = 0; int32_t partition = 0; int64_t offset = 0; };
  }
  ```
  `ProducerImpl` gains (issuing-thread section): `enum class TxnGate : uint8_t { NotTransactional, Closed, Open };`, `TxnGate txnGate_` (initialised `Closed` when `cfg.transactionalId` is non-empty), `[[nodiscard]] TxnGate txnGate() const noexcept`, `void setTxnGate(TxnGate) noexcept`.
- Rules (this task): `Client::producer(cfg)` with a non-empty `transactionalId` and `acks == None` → `std::invalid_argument("ProducerConfig: a transactional producer needs acks = Leader or All")`; `transactionalId` longer than 255 bytes → `std::invalid_argument` (the wire's u16-length field is fine, the coordinator's key limit is 255 — FD: keep it explicit); `ClientShard` with a `transactionalId` → `std::invalid_argument` (FD3). On a plain producer every lifecycle token form returns `OpResult::InvalidArgument` with no completion, every `*AndWait` and `beginTransaction()` throw `std::logic_error("<op>: producer has no transactionalId")`. On a transactional producer with the gate `Closed` every produce path returns `SendResult::NotInTransaction` (`send` / `sendTo` / blocking claim helpers throw `std::logic_error("produce outside a transaction")`), with **no** credit kept (reserved credit is returned) and nothing written to the ring. Lifecycle ops on a transactional producer are wired in Task 8 (until then they return `OpResult::InvalidArgument` too, which the Task 8 tests replace).

- [ ] **Step 1: Write the failing tests** — `ProducerTxnApiTest.cpp` (bare `ProducerImpl` + `Producer` handle where possible; the `Client::producer` cases use `InvokerGateway` as the existing `ProducerTest.cpp` does):

```cpp
// tests/producer/ProducerTxnApiTest.cpp — Phase 2b F Task 1: the transactional API surface and its refusals.
#include "../support/InvokerGateway.h"
#include "../support/AllocCounter.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

namespace rc = rune::client;
using rc::SendResult;

namespace {
std::span<const uint8_t> bytes(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
constexpr rc::TopicHandle kTopic{7, 4};
}

TEST(ProducerTxnApi, NotInTransactionIsAppendedLast) {
    static_assert(static_cast<int>(SendResult::NotInTransaction) == static_cast<int>(SendResult::ClaimHeld) + 1,
                  "append-only: NotInTransaction follows Part B's ClaimHeld");
    SUCCEED();
}

TEST(ProducerTxnApiRegression, AcksNoneWithATransactionalIdIsRejectedAtCreationRegression) {
    rc::test_support::InvokerGateway gw;                      // the existing loopback fake
    rc::Client client(gw.clientConfig());
    rc::ProducerConfig cfg; cfg.acks = rc::Acks::None; cfg.transactionalId = "tx-1";
    EXPECT_THROW((void)client.producer(cfg), std::invalid_argument);
    cfg.acks = rc::Acks::All;
    EXPECT_NO_THROW((void)client.producer(cfg));
    cfg.transactionalId = std::string(256, 'x');
    EXPECT_THROW((void)client.producer(cfg), std::invalid_argument);
}

TEST(ProducerTxnApiRegression, LifecycleOnAPlainProducerIsRefusedExplicitlyRegression) {
    rc::test_support::InvokerGateway gw;
    rc::Client client(gw.clientConfig());
    auto p = client.producer({.acks = rc::Acks::All});
    rc::Completions cq(client);
    EXPECT_EQ(p.initTransactions(cq, 1), rc::OpResult::InvalidArgument);
    EXPECT_EQ(p.commitTransaction(cq, 2), rc::OpResult::InvalidArgument);
    EXPECT_EQ(p.abortTransaction(cq, 3), rc::OpResult::InvalidArgument);
    EXPECT_THROW(p.beginTransaction(), std::logic_error);
    EXPECT_THROW(p.commitTransactionAndWait(std::chrono::seconds(1)), std::logic_error);
    EXPECT_EQ(cq.poll([](const rc::Completion&, std::span<const uint8_t>) {}), 0) << "no completion for a refusal";
    EXPECT_FALSE(p.transactional());
}

TEST(ProducerTxnApiRegression, ProduceOutsideATransactionIsRefusedOnEveryPathRegression) {
    rc::test_support::InvokerGateway gw;
    rc::Client client(gw.clientConfig());
    rc::ProducerConfig cfg; cfg.acks = rc::Acks::All; cfg.transactionalId = "tx-gate";
    auto p = client.producer(cfg);
    rc::Completions cq(client);
    EXPECT_EQ(p.trySend(kTopic, rc::Key(uint64_t{1}), bytes("v")), SendResult::NotInTransaction);
    EXPECT_EQ(p.trySend(kTopic, rc::Key(uint64_t{1}), bytes("v"), cq, 9), SendResult::NotInTransaction);
    EXPECT_EQ(cq.reservedBytesForTest(), 0u) << "the refused send keeps no credit";
    EXPECT_EQ(p.trySend(kTopic, rc::Key(uint64_t{1}), bytes("v"), rc::AckToken{9}), SendResult::NotInTransaction);
    EXPECT_EQ(p.tryClaim(kTopic, rc::Key(uint64_t{1}), 8).result(), SendResult::NotInTransaction);
    EXPECT_EQ(p.claimBatch(kTopic, rc::Key(uint64_t{1}), 2, 8).result(), SendResult::NotInTransaction);
    EXPECT_THROW(p.send(kTopic, rc::Key(uint64_t{1}), bytes("v")), std::logic_error);
    EXPECT_THROW(p.sendTo(kTopic, 0, rc::Key(uint64_t{1}), bytes("v")), std::logic_error);
    EXPECT_EQ(p.batch(kTopic, rc::Key(uint64_t{1})).add(bytes("a")).trySend(), SendResult::NotInTransaction);
    EXPECT_EQ(p.recordsSent(), 0u);
}

TEST(ProducerTxnApiRegression, PlainProducerHotPathIsUnchangedRegression) {
    // The gate is NotTransactional: trySend takes the same path (one predictable branch) and allocates nothing.
    rune::client::producer::ProducerImpl impl(rc::ProducerConfig{}, 1 << 20);
    rc::Producer p(&impl);
    for (int i = 0; i < 64; ++i) ASSERT_EQ(p.trySend(kTopic, rc::Key(uint64_t(i)), bytes("warm")), SendResult::Ok);
    rune::client::test_support::AllocCounter ac;
    for (int i = 0; i < 1000; ++i) ASSERT_EQ(p.trySend(kTopic, rc::Key(uint64_t(i)), bytes("x")), SendResult::Ok);
    EXPECT_EQ(ac.count(), 0u);
}
```
(`Completions::reservedBytesForTest()` — add a test accessor if it does not exist; `BatchBuilder::trySend()` — use B's / the existing non-throwing Tier-2 submit name; if the existing API only has `send()`, assert the throw instead. `rc::Producer p(&impl)` on a bare impl: the handle's destructor must not free a stack impl — use the existing bare-rig helper in `ProducerTest.cpp` instead of a raw pointer if one exists.)

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 --target rune_producer_tests` → `no member named 'transactionalId'` / `'NotInTransaction'`.
- [ ] **Step 3: Implement.** The gate check sits at the top of `Producer::trySendPart_`, the token `trySend` overloads, B's `tryClaim` / `claimBatch` entry and the Tier-2 submit, before any credit reservation:

```cpp
if (impl->txnGate() == producer::ProducerImpl::TxnGate::Closed) [[unlikely]] return SendResult::NotInTransaction;
```
(`NotTransactional` and `Open` both pass; the comparison is one byte load + compare on the hot path.) `ProducerImpl::throwForSendResult` maps `NotInTransaction` to `std::logic_error("produce outside a transaction")`. `RejectReason::NotInTransaction` is appended after B's `ClaimHeld`. Lifecycle stubs return `OpResult::InvalidArgument` / throw as specified.
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'ProducerTxnApi|Producer|BatchBuilder|Claim' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(producer): ProducerConfig::transactionalId, creation validation, SendResult::NotInTransaction, the handle gate on every produce path

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `CoordinatorLink` — preallocated request slots, coordinator-tagged ids, retries, routing by ordinal

**Files:**
- Create: `rune-client-cpp/include/rune/client/producer/TxnTypes.h`, `include/rune/client/producer/CoordinatorLink.h`, `src/producer/CoordinatorLink.cpp`
- Modify: `include/rune/client/producer/ProducerImpl.h` (owns a `std::optional<CoordinatorLink>` set by `enableCoordinator(...)`), `include/rune/client/producer/ProducerHub.h`, `src/producer/ProducerHub.cpp` (`onCoordinatorResponse`, `onCoordinatorMoved`, the sink pass-through), `src/Client.cpp` (wire the sink; route templates 46 / 48 / 50 / 52 / 54 and the coordinator's `ErrorResponse{NotCoordinator}` to `producerHub_` instead of `txnProducers_` — both paths coexist until Task 9)
- Create: `rune-client-cpp/tests/support/FakeCoordinator.h`; Test: `tests/producer/CoordinatorLinkTest.cpp`

**Interfaces:**
```cpp
namespace rune::client::producer {
namespace coordcorr {
inline constexpr int64_t kTag = int64_t{1} << 59;
static_assert((kTag & (batchcorr::kTag | (int64_t{1} << 61) | (int64_t{1} << 60))) == 0,
              "coordinator ids must not collide with batch / fetch / offset-fetch tags");
[[nodiscard]] constexpr int64_t make(uint32_t ordinal, uint8_t slot, uint32_t seq) noexcept {
    return kTag | (int64_t(ordinal & batchcorr::kMaxOrdinal) << 40) | (int64_t(slot) << 32) | int64_t(seq);
}
[[nodiscard]] constexpr bool     isCoord(int64_t c) noexcept { return (c & (int64_t{0xF} << 59)) == kTag; }
[[nodiscard]] constexpr uint32_t ordinal(int64_t c) noexcept { return uint32_t(c >> 40) & batchcorr::kMaxOrdinal; }
[[nodiscard]] constexpr uint8_t  slot(int64_t c) noexcept { return uint8_t(c >> 32); }
}
enum class ReqKind : uint8_t { InitProducerId, AddPartitions, AddOffsets, TxnOffsetCommit, EndTxn };
/// Coordinator answer, decoded (the link never interprets kind-specific bodies beyond errorCode).
struct CoordAnswer { ReqKind kind; int64_t corrId; int16_t errorCode; std::span<const uint8_t> frame; };

/// Function pointer + context (no std::function): offer one frame to the coordinator session.
struct CoordinatorSink {
    enum class Offer : uint8_t { Accepted, Retry, Fatal };
    Offer (*offer)(void* ctx, const uint8_t* buf, std::size_t len) noexcept = nullptr;
    int64_t (*sessionId)(void* ctx) noexcept = nullptr;   // 0 = not connected
    void* ctx = nullptr;
};

class CoordinatorLink {
public:
    static constexpr std::size_t kMaxCoordRequests = 8;
    static constexpr std::size_t kCoordFrameCap    = 4096;
    struct Free {};
    struct Unsent {};
    struct InFlight { int64_t sessionId = 0; int64_t answerByNs = 0; };
    struct Backoff  { int64_t untilNs = 0; };
    struct AwaitCoordinator { uint64_t gen = 0; };
    using SlotState = std::variant<Free, Unsent, InFlight, Backoff, AwaitCoordinator>;
    struct Slot {
        SlotState state = Free{};
        ReqKind   kind  = ReqKind::InitProducerId;
        int64_t   corrId = 0;
        uint32_t  attempt = 0;
        int64_t   deadlineNs = 0;
        uint16_t  frameLen = 0;
        std::array<uint8_t, kCoordFrameCap> frame{};
    };
    CoordinatorLink(uint32_t ordinal, CoordinatorSink sink) noexcept;
    /// A free slot, or nullptr (the caller retries next pass).  The caller encodes into slot->frame
    /// (frameLen), then submit() makes it Unsent.  ZERO allocation.
    [[nodiscard]] Slot* acquire(ReqKind kind, int64_t deadlineNs) noexcept;
    void submit(Slot& s) noexcept;
    /// One duty pass: offer Unsent / due Backoff / AwaitCoordinator-after-move slots; expire deadlines.
    /// `onExpired(slot)` reports a slot whose deadline passed (the caller completes its op Timeout).
    template <class OnExpired> int pump(int64_t nowNs, OnExpired&& onExpired);
    /// An answer for one of our ids: classify (FD5 / FD6), retry in place or hand it to the caller.
    /// Returns the slot to the caller (then release()) unless the answer was a retriable one.
    [[nodiscard]] Slot* onAnswer(const CoordAnswer& a, int64_t nowNs) noexcept;
    void release(Slot& s) noexcept;
    void onCoordinatorMoved() noexcept;     // ++gen_: AwaitCoordinator slots become due
    void onSessionChanged() noexcept;       // InFlight -> Unsent (same corrId)
    [[nodiscard]] std::size_t inUse() const noexcept;
private:
    std::array<Slot, kMaxCoordRequests> slots_{};
    uint32_t ordinal_; uint32_t seq_ = 0; uint64_t gen_ = 0; CoordinatorSink sink_;
};
}
```
`ProducerHub::onCoordinatorResponse(uint16_t templateId, const uint8_t* buf, std::size_t len, int64_t nowNs)` decodes the correlation id (common header), checks `coordcorr::isCoord`, and calls only `producers_[coordcorr::ordinal(id)]->onCoordinatorAnswer(...)`. A coordinator `ErrorResponse{NotCoordinator, message = endpoint}` for a coordinator id goes the same way (the Client also re-points its coordinator session from the message, as it does today).

- [ ] **Step 1: Write the failing tests** — `CoordinatorLinkTest.cpp` with `FakeCoordinator` (captures offered frames; `answer(kind, corrId, errorCode)` builds the matching response frame with rune-protocol's encoders):
  - `CoordinatorLink.AcquireSubmitPumpOffersOnceAndInFlightUntilAnswered`
  - `CoordinatorLink.ConcurrentTransactionsBacksOffOnTheDutyClockNotCycles` (answers 27: no re-offer before `untilNs`; re-offered after; attempt 1 → 1 ms, attempt 8 → 100 ms cap)
  - `CoordinatorLink.NotCoordinatorWaitsForTheMoveThenResends` (14 → `AwaitCoordinator`; `pump` offers nothing; `onCoordinatorMoved()` → offered with the same corrId)
  - `CoordinatorLink.SessionChangeResendsInFlightWithTheSameCorrId`
  - `CoordinatorLink.DeadlineExpiresIntoOnExpiredExactlyOnce`
  - `CoordinatorLink.NoFreeSlotReturnsNullNeverAllocates` (9 acquires: the 9th is nullptr; `AllocCounter` 0 across 10 000 acquire / submit / answer / release cycles)
  - `CoordinatorLinkRegression.AResponseReachesOnlyItsProducerRegression` (two producers with ordinals 1 and 2 in a `ProducerHub`; an answer for ordinal 2 is seen by producer 2 only — today's broadcast fixed)
  - `CoordinatorLinkRegression.CoordIdsNeverCollideWithBatchOrFetchIdsRegression` (`isCoord` false for `batchcorr::make(...)`, `GroupConsumerHub::kFetchCorrTag | x`, `kOffsetFetchCorrTag | x`; `batchcorr::isBatch` false for every `coordcorr::make`)
- [ ] **Step 2: Run to verify they fail** — build `rune_producer_tests` → missing `CoordinatorLink`.
- [ ] **Step 3: Implement** `CoordinatorLink.cpp` (the verdict table of FD5 / FD6 is a `switch` on `(kind, errorCode)`; port `TransactionalProducerImpl::classify_`'s cases verbatim and add 45 / 39 / the auth SessionRevoked code). `ProducerImpl::enableCoordinator(CoordinatorSink)` emplaces the link with the producer's ordinal; `Client::producer()` calls it with a sink whose `offer` is `agent_.offerToCoordinator(coordIdx_, …)` (seed when `coordIdx_ == 0`, as `txnSendFn_` does today) and whose `sessionId` returns the coordinator session's id.
- [ ] **Step 4: Run** — `ctest … -R 'CoordinatorLink|ProducerHub|Producer' --output-on-failure -j14` → PASS (every TransactionalProducer test still passes: its broadcast path is untouched until Task 9).
- [ ] **Step 5: Commit** — `feat(producer): CoordinatorLink — preallocated coordinator request slots, wall-clock backoff, coordinator-move resend, answers routed by producer ordinal`.

---

### Task 3: Fix-on-sight — Tier-2 batches in a preallocated slot pool (no per-batch heap, no `std::deque`)

**Files:**
- Modify: `include/rune/client/producer/ProducerImpl.h` (`Tier2Entry` → `Tier2Slot` pool; `PartitionState::pendingTier2Entries` → an intrusive FIFO of slot indices), `src/producer/ProducerImpl.cpp` (drain / flush / discard / destructor of Tier-2), `include/rune/client/producer/BatchBuilder.h`, `src/producer/BatchBuilder.cpp` (build into a pooled slot)
- Test: `tests/producer/ProducerTier2PoolTest.cpp` (new)

**Interfaces:**
- `ProducerConfig::tier2Slots = 64` (new knob; ≥ 2 × `maxInFlight` recommended), `tier2SlotBytes = batchBytes` (default).
- The pool: `std::vector<Tier2Slot>` sized once at construction (`tier2Slots` × (`tier2SlotBytes` + header)); slots move app → duty through the existing producer ring (`kMsgTier2Batch` now carries a **slot index** (u32), not a pointer) and duty → app through a new SPSC `OneToOneRingBuffer` of free slot indices (`tier2Free_`, capacity = pool size). `BatchBuilder` takes a free slot index at `batch()` (none free → its `send` / `trySend` reports `BackPressured`), builds the RecordBatch into the slot's preallocated bytes, and writes the index into the ring on send; the duty side appends the slot index to the partition's FIFO (`uint32_t head, tail` + per-slot `next`), and returns the index to `tier2Free_` once the batch completes (acked / failed / discarded). A batch larger than `tier2SlotBytes` → `MessageTooLarge` (non-retryable; today it was any size up to the frame cap — `tier2SlotBytes` defaults to `batchBytes`, which is ≤ `maxFrameBytes`, so no valid batch that fits a frame is refused).
- Fix-on-sight regression: `ProducerTier2Regression.BatchSendAllocatesNothingInSteadyStateRegression` (1000 `batch().add×8.send()` after warm-up: `AllocCounter` 0 on both threads).

- [ ] **Step 1: Write the failing tests:** the regression above; `ProducerTier2Pool.SlotsReturnAfterAckAndAfterFailure` (acked, broker error, delivery timeout, close: all return their slot — pool count restored); `ProducerTier2Pool.ExhaustedPoolIsBackPressuredNotBlockedForever` (`tier2Slots = 2`, three batches without acks: the third `BackPressured`; after one ack, `Ok`); `ProducerTier2Pool.OrderingWithTier1IsUnchanged` (the existing `ProducerBatchingTest` ordering assertions re-run over the pool — keep them passing unchanged); `ProducerTier2PoolRegression.OversizeBatchIsMessageTooLargeRegression`.
- [ ] **Step 2: Run to verify they fail** (the alloc regression reports > 0).
- [ ] **Step 3: Implement** as specified (the ring message type id `kMsgTier2Batch = 3` keeps its value; only its payload changes from a pointer to an index — in-process, not wire).
- [ ] **Step 4: Run** — `ctest … -R 'ProducerTier2|ProducerBatching|BatchBuilder|ProducerAlloc|Producer' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit** — `fix(producer): Tier-2 batches in a preallocated slot pool returned over an SPSC ring — no per-batch heap, no deque (fix-on-sight)`.

---

### Task 4: Broker-assigned producer ids for every acked producer (F2)

**Files:**
- Modify: `include/rune/client/producer/TxnTypes.h` (identity variants), `ProducerImpl.h` / `.cpp` (identity machine; delete `generateProducerId` and its TODO; flush gated on `IdAssigned`; epoch overflow → re-init), `src/Client.cpp` (nothing beyond Task 2's sink)
- Test: `tests/producer/ProducerIdTest.cpp` (new), `tests/client/ProducerTxnClientTest.cpp` (new, `InvokerGateway`), broker `libs/node/tests/client/ProducerIdBrokerRegressionTest.cpp` (new; add to `rune_client_broker_tests`)

**Interfaces:**
```cpp
namespace rune::client::producer {
struct IdUnassigned {};
struct IdRequested  { CoordinatorLink::Slot* slot = nullptr; };
struct IdAssigned   { int64_t producerId = -1; int16_t epoch = 0; };
struct IdFailed     { OpStatus status = OpStatus::BrokerError; uint16_t errorCode = 0; };
using ProducerIdentity = std::variant<IdUnassigned, IdRequested, IdAssigned, IdFailed>;
inline constexpr int64_t kRigProducerId = 0x7E57'0000'0000'0001LL;   // FD4: bare rigs
}
```
- Plain acked producer: `IdUnassigned` → (coordinator session connected) `IdRequested` (InitProducerId, empty id, `transactionTimeoutMs` 0) → `IdAssigned{pid, epoch}` → every partition's `producerId` / `epoch` taken from it at first use (the existing I-4 per-partition identity is kept); `flushBatches` skips a partition while the identity is not `IdAssigned` (one `index()` compare). Errors: retriable per FD5; non-retriable → `IdFailed` → `failAllPending(DeliveryFailed)` with the code and every later accepted record completes the same. Epoch overflow (`epoch == INT16_MAX` on a needed bump) → `IdRequested` again (new pid); partitions re-stamp when they next have nothing in flight (existing reset rule). `acks = None`: identity is never consulted (`-1/-1/-1`, as today). Transactional producers: the identity is set by `initTransactions` (Task 5) — the transactional InitProducerId answer feeds the same `IdAssigned`.
- `Producer::producerId()` / `producerEpoch()` (FD9) report it.

- [ ] **Step 1: Write the failing tests:**
  - `ProducerId.AckedProducerSendsInitProducerIdWithAnEmptyIdOnFirstConnectedPass` (fake coordinator: exactly one InitProducerId, `transactionalId` empty)
  - `ProducerIdRegression.RecordsBeforeTheIdArrivesAreHeldNotSentRegression` (100 records sent before the answer: zero Produce frames; after the answer every batch carries the assigned pid / epoch and sequences start at 0)
  - `ProducerIdRegression.AckedProducerUsesTheBrokerAssignedIdRegression` (`grep`-level: `generateProducerId` is gone; the pid on the wire equals the fake's answer, never a random one)
  - `ProducerId.AcksNoneNeverAsksForAnId` (no InitProducerId; batches `-1/-1/-1`)
  - `ProducerId.ConcurrentTransactionsAndNotCoordinatorRetryThenAssign`
  - `ProducerId.NonRetriableAnswerFailsEveryRecordExplicitly` (answer 39: pending acked records complete `DeliveryFailed` with errorCode 39; a later `trySend(cq)` completes the same; void sends counted)
  - `ProducerId.EpochOverflowRequestsANewId` (`testSetEpoch(INT16_MAX)`, force a bump: a second InitProducerId; the new pid used afterwards)
  - `ProducerId.BareRigStartsAssignedWithTheRigId` (FD4)
  - Client (`ProducerTxnClientTest.cpp`, real Aeron IPC to `InvokerGateway`): `ProducerTxnClient.PlainAckedProducerGetsItsIdFromTheGatewayBeforeItsFirstProduce`
  - Broker (`ProducerIdBrokerRegressionTest.cpp`, `OneBroker`): `ProducerIdBrokerRegression.TwoPlainProducersGetDistinctBrokerIdsRegression` (two producers, two clients: distinct pids from the broker's pid block; both topics' records all land)
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** the identity machine (`ProducerImpl::identityStep_(nowNs)` called from `drainAndFlush` before `flushBatches`; `onCoordinatorAnswer` for `ReqKind::InitProducerId` decodes `InitProducerIdResponse`).
- [ ] **Step 4: Run** — client `ctest … -R 'ProducerId|ProducerTxnClient|Producer|InvokerClient' --output-on-failure -j14`; broker `ctest … -R 'ProducerIdBroker' --output-on-failure -j14` → PASS. Every existing producer test passes unchanged (bare rigs are `IdAssigned`; `InvokerGateway` already answers InitProducerId — "Counted always").
- [ ] **Step 5: Commit** (client + broker) — `feat(producer): every acked producer gets its id from the broker (InitProducerId, empty transactional id); records held until assigned; client-drawn ids removed`.

- [ ] **Checkpoint A (controller):** full `*Regression*` in client (+ TSan) and broker (+ ASan / TSan).

---

### Task 5: The transaction machine in `ProducerImpl` — init, begin, commit, abort, flush, close, fencing (duty side)

**Files:**
- Modify: `include/rune/client/producer/TxnTypes.h` (txn states), `ProducerImpl.h` (the machine, `applyTxnControl`), create `src/producer/ProducerTxn.cpp` (bodies), `ProducerImpl.cpp` (`applyBeginClose` aborts an open transaction first)
- Test: `tests/producer/ProducerTxnTest.cpp` (new; bare `ProducerImpl` + `FakeCoordinator`, ops applied through `applyTxnControl` on the test thread = the duty thread)

**Interfaces:**
```cpp
namespace rune::client::producer {
enum class TxnStarted : uint8_t { No, Maybe, Yes };   // B-1: nothing / unknown / something added on the coordinator
enum class AbortKind  : uint8_t { Plain, BumpEpoch }; // B-3 (KIP-360): a sequence gap makes the abort bump the epoch
struct TxnOp { detail::OpTarget target{}; Token token = 0; int64_t deadlineNs = 0; };
struct AwaitData {};                                   // EndTxn waits for every record of the txn to be acked
struct EndSent  { CoordinatorLink::Slot* slot = nullptr; };
struct InDoubt  {};                                    // EndTxn timed out: retry the SAME direction (B-2)
struct Bumping  { CoordinatorLink::Slot* slot = nullptr; };   // InitProducerId after EndTxn(abort)
using EndPhase = std::variant<AwaitData, EndSent, InDoubt, Bumping>;

struct Uninitialized {};
struct Initializing   { TxnOp op; CoordinatorLink::Slot* slot = nullptr; };
struct Ready          {};
struct InTransaction  { TxnStarted started = TxnStarted::No; };
struct Committing     { TxnOp op; EndPhase phase = AwaitData{}; TxnStarted started = TxnStarted::No; };
struct Aborting       { TxnOp op; EndPhase phase = AwaitData{}; TxnStarted started = TxnStarted::No; AbortKind kind = AbortKind::Plain; };
struct AbortableError { OpStatus status = OpStatus::BrokerError; uint16_t errorCode = 0; TxnStarted started = TxnStarted::No; AbortKind kind = AbortKind::Plain; };
struct FatalError     { OpStatus status = OpStatus::Fenced; uint16_t errorCode = 0; };
using TxnMachine = std::variant<Uninitialized, Initializing, Ready, InTransaction, Committing, Aborting, AbortableError, FatalError>;
static_assert(std::variant_size_v<TxnMachine> == static_cast<std::size_t>(TxnState::FatalError) + 1,
              "TxnState is the machine's index");

/// One transaction control op (the kMsgTxnControl payload decoded, or an Invoker direct call).
struct TxnControl {
    enum class Kind : uint8_t { Init, Begin, SendOffsets, Commit, Abort, Flush };
    Kind kind = Kind::Begin;
    detail::OpTarget target{};
    Token token = 0;
    std::span<const OffsetEntry> offsets{};   // SendOffsets (Task 7)
    std::string_view groupId{};
};
// ProducerImpl (duty thread):
void applyTxnControl(const TxnControl& c, int64_t nowNs);
[[nodiscard]] TxnState txnStateDuty() const noexcept { return static_cast<TxnState>(txn_.index()); }
}
```
Semantics are today's `TransactionalProducerImpl` (port every rule of its header comment): state gating (begin only in `Ready`; sendOffsets / commit only `InTransaction`; abort in `InTransaction` / `AbortableError`; init only `Uninitialized`; `FatalError` → every op `Fenced` (or its status)); misuse completes `BrokerError(19)`; a transaction with nothing started ends locally (`TxnStarted::No` → no EndTxn, B-1); commit / abort timeout → `InDoubt` and the same call re-sends EndTxn (B-2; abort refused while committing); sequence gap → `AbortKind::BumpEpoch` (B-3); fencing 17 / 18 → `FatalError`; commit in `AbortableError` completes with its error; the local state resets only on EndTxn's final answer; `close()` on an open transaction aborts it first (`applyBeginClose`), `closeImmediately()` leaves it to the coordinator timeout. The data side (records, AddPartitions) arrives in Task 6; until then `TxnStarted` moves only through Task 7's offsets path and tests use an empty transaction plus `testMarkStarted()`.

- [ ] **Step 1: Write the failing tests** — port, by name, every `TransactionalProducerTest` case that exercises lifecycle only (no data) into `ProducerTxnTest.cpp` with the same assertions against `applyTxnControl` + `FakeCoordinator`; list them in the commit body. At least:
  - `ProducerTxn.InitAssignsIdAndEpochAndGoesReady`
  - `ProducerTxn.BeginCommitOfAnEmptyTransactionEndsLocallyWithoutEndTxn` (B-1)
  - `ProducerTxn.CommitWaitsForDataThenSendsEndTxnCommit`
  - `ProducerTxn.AbortSendsEndTxnAbort`
  - `ProducerTxn.CommitTimeoutIsInDoubtAndARetriedCommitResendsEndTxn` (B-2)
  - `ProducerTxn.AbortIsRefusedWhileCommitting`
  - `ProducerTxn.SequenceGapAbortBumpsTheEpoch` (B-3)
  - `ProducerTxn.ConcurrentTransactionsOnEndTxnRetriesUntilFinal`
  - `ProducerTxnRegression.FencedByASecondProducerFailsEverythingRegression` (answer 18 on EndTxn: commit `Fenced`, state `FatalError`, every later op `Fenced`, no frame offered afterwards)
  - `ProducerTxnRegression.MisuseCompletesInvalidTxnStateNeverHangsRegression` (begin twice, commit in `Ready`, init twice: each completes `BrokerError(19)` exactly once)
  - `ProducerTxnRegression.NotAuthorizedOnInitIsFatalOnAddOffsetsAbortableRegression` (FD6, auth)
  - `ProducerTxnRegression.SessionRevokedIsFatalAndNothingIsResentRegression` (auth D26)
  - `ProducerTxnRegression.CloseAbortsAnOpenTransactionFirstRegression`
  - `ProducerTxnRegression.TransientLogFullBackoffIsWallClockTimeRegression` (ported M3: a 42 on a txn batch waits `kTransientLogFullBackoffNs` on the duty clock)
  - `ProducerTxn.MachineStatesCarryNoBooleans` (a `static_assert` over the state structs' member types: no `bool` member in any `TxnMachine` alternative — the CLAUDE.md #3 pin)
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** in `ProducerTxn.cpp`: one `std::visit`-free dispatcher per op (`switch (txn_.index())`), each transition assigning the next state struct; EndTxn encoded into a `CoordinatorLink` slot; the status ring published on every transition (FD9).
- [ ] **Step 4: Run** — `ctest … -R 'ProducerTxn|CoordinatorLink|ProducerClose' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit** — `feat(producer): the transaction state machine moves into ProducerImpl (per-state data, no booleans): init / begin / commit / abort / flush / close / fencing`.

---

### Task 6: Transactional data on every produce path — AddPartitions gating, the TRANSACTIONAL bit, poisoning

**Files:**
- Modify: `ProducerImpl.h` (`PartitionState::txn` membership variant + `txnSeen_` dense list of partitions in the current transaction, preallocated to the partition table capacity), `src/producer/ProducerTxn.cpp`, `ProducerImpl.cpp` (`flushBatches`: skip a partition whose membership is not `Added` while transactional; Tier-2 restamp sets the attribute; Direct (Invoker) appends obey the same gate), `ProduceFrameBuilder.h` (`setTransactional(bool)`: `seal()` writes `kAttrTransactional` into the batch attributes), `restampBatchIdentity` → `restampBatchIdentityAndAttributes` (attributes are inside the CRC range: patch it with the same incremental `crc32cPatch`)
- Test: `tests/producer/ProducerTxnDataTest.cpp` (new)

**Interfaces:**
```cpp
struct NotInTxn {};
struct Adding   { CoordinatorLink::Slot* slot = nullptr; };
struct Added    {};
struct AddFailed { uint16_t errorCode = 0; };
using TxnMembership = std::variant<NotInTxn, Adding, Added, AddFailed>;   // PartitionState::txn
```
- First accepted record (any path) of a partition while `InTransaction` → membership `NotInTxn` → queued for the next AddPartitions request (one request per duty pass carries every newly dirty partition, split per FD5) → `Adding` → OK → `Added` (`TxnStarted::Yes`); a timeout with the frame offered → `TxnStarted::Maybe`; non-retriable → `AddFailed` + the machine `AbortableError` (the partition's records are discarded at abort, never produced; acked ones complete with the txn's status).
- While transactional, every batch is sealed / restamped with `kAttrTransactional`; plain producers never set it.
- Commit's `AwaitData` completes when every `Added` partition has nothing in flight and nothing buffered (flush forced for all transaction partitions at commit issue).
- On EndTxn's final answer, every partition in `txnSeen_` goes back to `NotInTxn` (O(partitions in the txn), not O(all partitions)).

- [ ] **Step 1: Write the failing tests:**
  - `ProducerTxnDataRegression.NoProduceBeforeAddPartitionsOkOnAnyPathRegression` (parameterized over `Send`, `TrySendCq`, `TrySendAck`, `Batch`, `TryClaim`, `ClaimBatch`: the fake delays AddPartitions; zero Produce frames for the partition until it answers OK; then the records in order)
  - `ProducerTxnData.OneAddPartitionsPerNewPartitionPerTransaction` (100 records over 4 partitions → AddPartitions names each partition once per txn; a second txn adds them again)
  - `ProducerTxnData.EveryTransactionalBatchHasTheTransactionalBitAndPlainNever` (decode every captured batch's attributes; Tier-2 restamped CRC verifies with a full recompute)
  - `ProducerTxnData.CommitWaitsForEveryAddedPartitionToDrain`
  - `ProducerTxnDataRegression.AddPartitionsFailurePoisonsAndAbortDiscardsWithoutProducingRegression`
  - `ProducerTxnDataRegression.RecordReachingDutyOutsideATransactionIsNeverProducedRegression` (FD2 duty re-check: gate `Open`, machine forced to `AbortableError` before the drain — the record never leaves; the acked one completes with the txn status; `txnRecordsRefusedDuty` 1)
  - `ProducerTxnData.InvokerModeClaimInATransactionWritesTheFrameAfterAdded` (Direct ingress: B's direct claim is held in the builder until `Added`)
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `ctest … -R 'ProducerTxnData|ProducerTxn|ProduceFrameBuilder|ProducerClaim|ProducerBatching' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit** — `feat(producer): every produce path participates in a transaction — AddPartitions gating per partition, TRANSACTIONAL bit, poisoning`.

---

### Task 7: `sendOffsetsToTransaction` — AddOffsetsToTxn then TxnOffsetCommit, zero allocation

**Files:**
- Modify: `src/producer/ProducerTxn.cpp`, `ProducerImpl.h` (offsets scratch: `std::array<OffsetEntry, kMaxTxnOffsets = 1024>` + a `std::array<char, 255>` group id buffer, preallocated)
- Test: `tests/producer/ProducerTxnOffsetsTest.cpp` (new)

**Interfaces:** `TxnControl{Kind::SendOffsets, target, token, offsets, groupId}` (offsets ≤ `kMaxTxnOffsets`, group id ≤ 255 bytes, else the op completes `InvalidArgument` with a text naming the limit); the op runs `AddOffsetsToTxn(groupId)` (`TxnStarted::Yes` on OK) then `TxnOffsetCommit` (entries split per FD5) and completes `Ok` once every entry is OK; a non-retriable error → `AbortableError` with that code (auth: 45 → abortable, FD6). In Thread mode the offsets travel in the `kMsgTxnControl` payload (Task 8) and are copied into the scratch on the duty side.

- [ ] **Step 1: Write the failing tests:** `ProducerTxnOffsets.AddOffsetsThenTxnOffsetCommitThenOk`, `ProducerTxnOffsets.ManyOffsetsSplitAcrossRequestsAllOrError`, `ProducerTxnOffsetsRegression.TooManyOffsetsIsInvalidArgumentNotTruncatedRegression`, `ProducerTxnOffsetsRegression.OffsetsOutsideATransactionCompleteInvalidTxnStateRegression`, `ProducerTxnOffsets.AllocatesNothingInSteadyState` (`AllocCounter` over 1000 sendOffsets of 16 entries).
- [ ] **Step 2–4:** fail, implement, run `ctest … -R 'ProducerTxnOffsets|ProducerTxn' --output-on-failure -j14`.
- [ ] **Step 5: Commit** — `feat(producer): sendOffsetsToTransaction on the unified producer (AddOffsets + TxnOffsetCommit, preallocated scratch)`.

---

### Task 8: The handle — `kMsgTxnControl` on the producer ring, Invoker direct ops, `*AndWait`, status, `ProducerRef`, typed topics in a transaction

**Files:**
- Modify: `include/rune/client/producer/ProducerImpl.h` (`kMsgTxnControl = 5` + layout; `submitTxnControl(...)`; drain branch), `src/producer/ProducerImpl.cpp`, `src/producer/Producer.cpp` (lifecycle bodies, AndWait through `hooks_.andWait`), `include/rune/client/producer/Producer.h` (status accessors), create `include/rune/client/producer/ProducerRef.h`, modify `include/rune/client/TypedTopic.h` (accepts `ProducerRef`), `include/rune/client/Client.h` / `src/Client.cpp` (`openTopicAndWait<T>(name, Producer& shared, timeout)`)
- Test: `tests/client/ProducerTxnClientTest.cpp` (append; real Aeron IPC, both runner modes, `InvokerGateway` in its txn-aware mode)

**Interfaces:**
- Ring message `kMsgTxnControl = 5`, payload: `[0] kind u8 | [1..3] pad | [4..7] groupIdLen u32 | [8..31] target (detail::encodeTarget words b, c + token a) | [32..35] offsetCount u32 | [36..] groupId bytes | 16-byte entries (topicId i32, partition i32, offset i64)`; credit for a CQ target is reserved on the app thread at issue (as `trySend(cq)` does) and returned on a refused write.
- Issue rules: `beginTransaction()` writes `Begin` then sets the gate `Open`; `commitTransaction` / `abortTransaction` set the gate `Closed` **before** writing (so a racing produce on the same issuing thread cannot slip after it); `flush` leaves the gate. A full ring → `OpResult::BackPressured` (token forms) / the AndWait forms wait like `sendBlocking`. Invoker mode: `applyTxnControl` directly on the owner thread (`OpResult::WrongThread` elsewhere).
- `ProducerRef`: `explicit ProducerRef(Producer& p) noexcept`; `trySend`, `send`, `sendTo`, `tryClaim`, `claimBatch` forward; copyable; `TypedTopic<T, ProducerRef>` compiles with B's typed `claim`.

- [ ] **Step 1: Write the failing tests** (`ProducerTxnClientTest.cpp`, `TEST_P` over `RunnerMode::{Thread, Invoker}`):
  - `ProducerTxnClient.InitBeginSendCommitIsVisibleToReadCommitted` (the gateway's read-committed view)
  - `ProducerTxnRegression.RecordsSentBeforeCommitAreInItAfterAreRefusedRegression` (Thread mode, one issuing thread: 1000 sends, commit, 1 send → the commit contains exactly 1000; the 1001st returns `NotInTransaction`)
  - `ProducerTxnClient.AbortMakesEveryPathInvisible` (send, batch, tryClaim, claimBatch, typed send / claim via `ProducerRef` in one transaction, then abort)
  - `ProducerTxnClient.MixedPathsInOneTransactionCommitTogether`
  - `ProducerTxnClient.AndWaitFormsThrowWithTheOutcome` (Fenced → `std::runtime_error` mentioning "ProducerFenced"; Timeout → "timed out")
  - `ProducerTxnClient.StatusAccessorsFollowTheMachine` (`state()`, `producerId()`, `producerEpoch()`, `transactionActive()` after init / begin / commit)
  - `ProducerTxnClientRegression.InvokerOpsOffTheOwnerAreWrongThreadRegression`
  - `ProducerTxnClient.TypedTopicOverProducerRefWritesInsideTheTransaction`
- [ ] **Step 2–4:** fail, implement, run `ctest … -R 'ProducerTxnClient|ProducerTxn|TypedTopic|TypedClaim|InvokerClient' --output-on-failure -j14`.
- [ ] **Step 5: Commit** — `feat(producer): transactions on Producer — ordered control ops on the producer ring, Invoker direct, AndWait, status, ProducerRef for typed topics`.

---

### Task 9: Delete `TransactionalProducer`; migrate every user and test

**Files:**
- Delete: `include/rune/client/producer/TransactionalProducer.h`, `TransactionalProducerImpl.h`, `src/producer/TransactionalProducer.cpp`, `TransactionalProducerImpl.cpp`, `tests/producer/TransactionalProducerTest.cpp` (after migration)
- Modify: `include/rune/client/Client.h`, `src/Client.cpp` (remove `transactionalProducer`, `txnProducers_`, `txnSendFn_`, `findTxn_`, the broadcast `case`s, the drain loop over txn producers, `setForwardProduceResponses`), `include/rune/client/detail/HubCommand.h` (ops 30–37 removed, a comment "30–37 retired (Phase 2b F) — never reuse"), `include/rune/client/agent/ClientAgent.h` / `src/agent/ClientAgent.cpp` (`kMsgRegisterTxnProducer = 7` retired the same way), `include/rune/client/Completion.h` (`OpKind::TxnClose` kept — append-only), `CMakeLists.txt`, `examples/dr_client.cpp`, `examples/view_probe.cpp`, `tests/client/{HandleLifetimeTest,HubCommandClientTest,InvokerClientTest,GroupRoutingRegressionTest,TlsConnectEdgeRegressionTest}.cpp`, broker `libs/node/tests/client/ClientFailoverBrokerRegressionTest.cpp`, `rune-client-cpp/README.md`, `rune-client-cpp/CLAUDE.md` (producer section)

**Migration rule:** every `TransactionalProducerTest` case not already ported in Tasks 5–7 moves to `ProducerTxnTest.cpp` (lifecycle), `ProducerTxnDataTest.cpp` (data / F14 gating / EOS-OOS sequencing) or `ProducerTxnOffsetsTest.cpp`, **same test body semantics, new API**, `*Regression*` names kept verbatim (suite renamed `TransactionalProducer*` → `ProducerTxn*`). Write the old → new name table into the commit body. `client.transactionalProducer(id, cfg)` becomes `cfg.transactionalId = id; client.producer(cfg)`; `tp.send(...)` stays `p.send(...)`; `std::map<TopicPartitionKey, int64_t>` offsets become a `std::vector<OffsetEntry>` built once (examples) or a stack array (tests).

- [ ] **Step 1:** Port the remaining tests (they fail to compile against the deleted header — that is the "failing" state). Add `ProducerTxnRegression.TransactionalProducerTypeIsGoneRegression` (`static_assert(!requires { typename rune::client::TransactionalProducer; })` and `!requires(rune::client::Client& c) { c.transactionalProducer(""); }`).
- [ ] **Step 2:** Delete the files; fix every compile error by migrating the call site (grep must be empty: `grep -rn "TransactionalProducer\|transactionalProducer" /Users/vradu/Projects/se-worktrees/rune-client-cpp/{include,src,tests,examples,bench} /Users/vradu/Projects/se-worktrees/rune-broker/libs` → only the regression test's `requires` expressions).
- [ ] **Step 3: Run** — client: everything (`ctest --test-dir … --output-on-failure -j14`); broker: `ctest … -R 'ClientFailoverBroker|ProducerIdBroker' --output-on-failure -j14` (the failover txn case — `SeedCoordinatorCrashedMidTransactionEndsCommittedOrRetriableRegression` — on the unified producer, assertions unchanged).
- [ ] **Step 4: Commit** (client + broker) — `refactor(client)!: TransactionalProducer removed — transactions are Producer + transactionalId; tests and examples migrated`.

- [ ] **Checkpoint B (controller):** full `*Regression*` in client (+ TSan) and broker (+ ASan / TSan).

---

### Task 10: `GroupConsumer::seekToCommitted()` / `seek()` (F3)

**Files:**
- Modify: `include/rune/client/group/GroupConsumer.h`, `src/group/GroupConsumer.cpp`, `include/rune/client/group/GroupConsumerHub.h`, `src/group/GroupConsumerHub.cpp`, `include/rune/client/detail/HubCommand.h` (`GroupSeekToCommitted = 15`, `GroupSeek = 16`), `include/rune/client/Completion.h` (`OpKind::SeekToCommitted`, `OpKind::Seek` appended)
- Test: `tests/client/GroupSeekTest.cpp` (new; real Aeron IPC to `InvokerGateway` in its group mode, as `tests/group/GroupConsumerTest.cpp`)

**Interfaces:**
```cpp
class GroupConsumer {
public:
    OpResult seekToCommitted(CompletionTarget target, Token token);
    void     seekToCommittedAndWait(std::chrono::nanoseconds timeout = std::chrono::seconds(60));
    OpResult seek(TopicPartition tp, int64_t offset, CompletionTarget target, Token token);
    void     seek(TopicPartition tp, int64_t offset);   // fire-and-forget (G6: an unassigned partition is reported only by the token / AndWait forms)
    void     seekAndWait(TopicPartition tp, int64_t offset, std::chrono::nanoseconds timeout = std::chrono::seconds(60));
};
// GroupConsumerImpl (duty thread): the pending rewind — at most one, per-state data:
struct NoRewind {};
struct RewindAwaitFetch  { detail::OpTarget target{}; Token token = 0; int64_t deadlineNs = 0; int64_t fetchCorr = 0; int32_t generation = 0; };
struct RewindResubscribe { detail::OpTarget target{}; Token token = 0; int64_t deadlineNs = 0; int32_t generation = 0; };
using Rewind = std::variant<NoRewind, RewindAwaitFetch, RewindResubscribe>;
```
Semantics per FD8. The assignment lives on the duty thread, so the check happens there: the token form completes `OpStatus::InvalidArgument` (payload "partition not assigned") for an unassigned partition, `seekAndWait` throws `std::invalid_argument`, and the void `seek` issues the token form with an internal handler and drops the result (G6). A second rewind while one is pending supersedes it (`OpStatus::Superseded`, like `commitSync`).

- [ ] **Step 1: Write the failing tests:**
  - `GroupSeekRegression.SeekToCommittedRedeliversExactlyTheUncommittedRecordsRegression` (commit offset 40 on a partition, consume to 100, `seekToCommittedAndWait` → the next records delivered start at 40 and run to 100 again; nothing before 40)
  - `GroupSeek.SeekRestartsOnePartitionAtTheOffset` (others untouched: no Unsubscribe for them)
  - `GroupSeekRegression.SeekOnAnUnassignedPartitionIsInvalidArgumentRegression`
  - `GroupSeekRegression.AutoCommitNeverCommitsAStalePositionDuringARewindRegression` (auto-commit tick between the command and the OffsetFetch answer: no OffsetCommit for the rewound partitions)
  - `GroupSeek.RebalanceDuringARewindCompletesRejoined`
  - `GroupSeek.PushesOfTheOldSubscriptionAreIgnoredAfterTheSeek` (the gateway pushes one more batch on the old instance id after the Unsubscribe: not delivered)
  - `GroupSeek.SecondRewindSupersedesTheFirst`
- [ ] **Step 2–4:** fail, implement (`applyTo_` handles ops 15 / 16; the OffsetFetch answer path seeds positions and moves `RewindAwaitFetch` → `RewindResubscribe`; `onFetchSubscribeResponse` completes the rewind once every rewound partition is bound), run `ctest … -R 'GroupSeek|GroupConsumer|GroupRouting' --output-on-failure -j14`.
- [ ] **Step 5: Commit** — `feat(group): GroupConsumer::seekToCommitted / seek — rewind without a restart (exactly-once loop after an abort)`.

---

### Task 11: Real-broker integration — exactly-once across every path, fencing, in-doubt commit, dedup across reconnect

**Files:**
- Create: `rune-broker/libs/node/tests/client/EosBrokerRegressionTest.cpp`; modify `ProducerIdBrokerRegressionTest.cpp` (append), `libs/node/CMakeLists.txt` (`EosBrokerRegressionTest.cpp` into `rune_client_failover_broker_tests` — it kills brokers, `RUN_SERIAL`)

**Interfaces:** consumes the real client (Tasks 1–10) and `OneBroker` / `ThreeNodes` + `ClusterClient.h` helpers (`clientConfigFor`, `connectClient`, `awaitController`, `awaitLeaders`, `ThreeNodes::crash`). Topics are created by DDL (`defineTopicAndWait`, catalog) — never `RUNE_TOPICS`.

- [ ] **Step 1: Write the tests** (each ends with a `read_committed` read-back counting every value):
  - `EosBrokerRegression.EveryPathCommittedIsVisibleOnceAbortedNeverRegression` (`ThreeNodes`, rf 3, 4 partitions: one transaction per path — send, batch, tryClaim, claimBatch, typed claim via `ProducerRef` — committed; the same set aborted; RC sees exactly the committed ones once)
  - `EosBrokerRegression.MixedPathsInOneTransactionRegression`
  - `EosBrokerRegression.ZombieIsFencedAndItsRecordsAreInvisibleRegression` (producer A begins + sends; producer B with the same `transactionalId` inits; A's commit completes `Fenced`; RC sees none of A's records)
  - `EosBrokerRegression.InDoubtCommitRetryCommitsOnceRegression` (crash the coordinator after the EndTxn was offered; the commit completes `Timeout` or `Ok`; a retried commit `Ok`; RC sees every record once — the unified-producer form of the failover test)
  - `EosBrokerRegression.LeaderCrashMidTransactionResendsAndCommitsOnceRegression` (crash a data partition leader with batches in flight)
  - `EosBrokerRegression.AbortRewindRedoOutputEqualsInputRegression` (consume → transform → produce loop over 10 000 input records with `sendOffsetsToTransaction` per transaction; every 5th transaction aborted, then `seekToCommittedAndWait`, then redone; output count == input count, each input id exactly once in the output)
  - `EosBrokerRegression.ProduceOutsideATransactionIsRefusedAgainstARealBrokerRegression`
  - `ProducerIdBrokerRegression.RetriedBatchAcrossAReconnectIsDeduplicatedRegression` (`ThreeNodes`: a plain acked producer; the seed crashes with a batch answered-but-unseen; the client reconnects and re-sends; the log holds one copy)
  - `ProducerIdBrokerRegression.PlainAckedEveryPathRegression` (9 ops × {`Steady`, `SessionLostMid`} — the matrix's real-broker plain slice, reading `TxnMatrixExpectations.h`)
- [ ] **Step 2: Run** — `cmake --build … --target rune_client_failover_broker_tests rune_client_broker_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'EosBroker|ProducerIdBroker' --output-on-failure` → PASS (fix forward in the client if a cell fails; every fix gets its own `*Regression*`).
- [ ] **Step 3: Commit** (broker) — `test(eos): the unified producer against real brokers — every path, fencing, in-doubt commit, leader crash, abort-rewind-redo, plain dedup across reconnect`.

---

### Task 12: Interaction matrix — expectations, the rig, 320 parameterized cells

**Files:**
- Create: `rune-client-cpp/tests/support/TxnMatrixExpectations.h`, `tests/client/TxnMatrixTest.cpp`; modify `tests/support/InvokerGateway.h` (`TxnFakeBroker` mode: per-partition frame order log, read-committed view, scripted 19 / 18 / session drop)

**Interfaces:**
```cpp
namespace rune::client::txnmatrix {
enum class Op : uint8_t { Send, SendTo, TrySendCq, TrySendAck, Batch, TryClaim, ClaimBatch, TypedSend, TypedClaim, SendOffsets };
enum class Entity : uint8_t { PlainNoAck, PlainAcked, TxnNewPartition, TxnAddedPartition };
enum class Timing : uint8_t { Steady, BeforeInit, OutsideTxn, AfterCommitIssued, AbortableError, Fenced, SessionLostMid, AfterAbortRewind };
enum class NaRule : uint8_t { NA1, NA2, NA3 };
enum class Outcome : uint8_t { Delivered, DeliveredOnce, AtMostOnce, Refused, CommittedVisible, FailsTxn, Fenced, CommittedVisibleOnce, RedoneOnce };
[[nodiscard]] constexpr std::optional<NaRule> naRule(Op, Entity, Timing) noexcept;   // the table above, top-down
[[nodiscard]] constexpr Outcome expect(Op, Entity, Timing) noexcept;                 // R1..R11, top-down
}
```

- [ ] **Step 1: Write the tests:** `TxnMatrixCountRegression.CellCountsArePinned` (320 / 188 / 132; NA1 120, NA2 4, NA3 8; R1 18, R2 9, R3 9, R4–R11 19 each — computed by iterating the enums); `TxnMatrix/TxnMatrixTest.Cell/*` (`INSTANTIATE_TEST_SUITE_P` over the cartesian product, `GTEST_SKIP() << "NA: " << rule` for N/A cells), each cell asserting the five "Every cell asserts" points through the `TxnMatrixRig` (one `Client` in Invoker mode per cell, a fresh `transactionalId` / group / topic name per cell).
- [ ] **Step 2: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'TxnMatrix' --output-on-failure -j14` → 188 pass, 132 skipped with their rule. A failing cell is a bug: fix it (with a `*Regression*` test) in the owning code, never by editing `expect()` without a spec reason recorded in the commit body.
- [ ] **Step 3: Commit** — `test(producer): transaction interaction matrix — 10 ops × 4 entities × 8 timings (188 applicable, 132 N/A by rule)`.

---

### Task 13: Seeded, model-checked stress against an independent transaction model

**Files:**
- Create: `rune-client-cpp/tests/client/TxnStressTest.cpp`, `tests/support/TxnModel.h` (independent: a plain map of committed / pending records per partition and a txn-state enum — it shares no code with `ProducerImpl`)

- [ ] **Step 1: Write** `TxnStressRegression.SeededInterleavingsMatchTheModelRegression` (`TEST_P` over seeds 1..64, 400 steps each, `RUNE_TXN_STRESS_SEEDS` env to widen locally): each step picks one of {a data op of the 9 paths on a random partition, begin, commit, abort, flush, sendOffsets, seekToCommitted, inject 27 / 14 / 19 / 18 on the next coordinator answer, drop the session, reconnect}; after every completed commit / abort the RC view must equal the model's committed set; at the end: every token completed exactly once, no produce before AddPartitions OK (frame log), per (pid, epoch, partition) sequences gap-free in the log, zero allocations in the measured steps after warm-up. A failure prints the seed and the step log.
- [ ] **Step 2: Run** — `ctest … -R 'TxnStress' --output-on-failure -j14` → PASS.
- [ ] **Step 3: Commit** — `test(producer): seeded model-checked transaction stress (64 seeds × 400 steps)`.

- [ ] **Checkpoint C (controller):** full `*Regression*` in client (+ TSan) and broker (+ ASan / TSan).

---

### Task 14: Benchmarks — transactional vs plain per path, zero allocation, plain produce p50/p99 non-regression

**Files:**
- Create: `rune-client-cpp/bench/ProducerTxnBench.cpp` (in-process like `ProducerAckedBench.cpp`: a duty thread with a fake coordinator / sink answering at once); modify `CMakeLists.txt` (`rune_producer_txn_bench`), `tests/producer/ProducerAllocRegressionTest.cpp` (append)

- [ ] **Step 1: Baseline first.** On the parent commit of this branch (`git -C … stash` not needed: build the parent in a scratch worktree under the scratchpad), run 5× each: `rune_client_produce_bench --benchmark_filter=BM_ProduceRTT`, `rune_producer_acked_bench`, `rune_producer_handle_bench`; record the medians (p50 / p99 / records·s⁻¹).
- [ ] **Step 2: Write** `ProducerTxnBench.cpp`: for each path in {send, trySend(cq), batch(8), tryClaim, claimBatch(8), typed claim}, plain acked vs transactional (one transaction per 1000 records, commit answered at once): per-record p50 / p99 (ns), records/s, allocations per record (must be 0) — printed as `PRODUCER_TXN path=<p> mode=<plain|txn> p50_ns=… p99_ns=… allocs=…`. Append to `ProducerAllocRegressionTest.cpp`: `ProducerTxnAllocRegression.SteadyStateTransactionsAllocateNothingRegression` (1000 transactions × 10 records mixing every path + one `sendOffsets` each, after a 100-transaction warm-up: `AllocCounter` 0 on the app thread and the duty thread).
- [ ] **Step 3: Run** (Release build: `build-bench-rel` of the client, `-O3 -DNDEBUG`): the alloc regression PASS; the benches 5× each; **gate:** plain-producer medians within +5 % p50 / +10 % p99 of the baseline and `BM_ProduceRTT` p99 < 100 µs; transactional per-record p50 within 2× plain (reported; the gate is the plain non-regression and the zero allocations).
- [ ] **Step 4: Commit** — `bench(producer): transactional vs plain on every path (p50/p99, 0 allocations); plain produce non-regression` with the baseline and new numbers in the body.

---

### Task 15: End to end — exactly-once pipeline on 3 brokers (k3s VM), docs, release notes

**Files:**
- Create: `rune-client-cpp/examples/eos_probe.cpp` (+ `CMakeLists.txt` example target, `docker/Dockerfile.eos-probe` modelled on `Dockerfile.view-probe`), `rune-broker/docker/eos-e2e.sh`, `docker/docker-compose.eos.yml` (3 nodes; authentication on, cluster key mounted as the auth phase's compose files do), `docker/topics/eos-e2e.rql`
- Modify: `rune-broker/dr-e2e/harness/*` only if it names the removed type (it drives `dr_client`, migrated in Task 9 — run `test_tx_fencing.py` unchanged), `rune-client-cpp/README.md` / site-facing docs in the repo (Transactions / Delivery guarantees use `Producer` + `transactionalId` + `seekToCommitted`), create `rune-broker/docs/release-notes/phase2b-f-unified-producer.md` (breaking: `TransactionalProducer` removed; acked producers ask the broker for an id on connect; new `SendResult::NotInTransaction`; Tier-2 slot pool knob)

`eos_probe` modes (env `RUNE_MODE`): `fill` (a plain acked producer writes N = 200 000 records `{id u64, v u64}` to `eos_in`, 8 partitions); `pipeline` (a group consumer `eos-1` on `eos_in` + a transactional producer `eos-worker-1`; per transaction: ≤ 500 input records, transformed `v' = v * 2`, written to `eos_out` rotating the path per transaction — send / batch / tryClaim / claimBatch / typed claim; `sendOffsetsToTransactionAndWait`; every 7th transaction aborted on purpose → `seekToCommittedAndWait` → redo; exits 0 when the committed input offsets reach the end); `verify` (a `read_committed` reader of `eos_out`: count == N, every id exactly once, every `v' == 2 * v`; prints `EOS-VERIFY: OK count=<N>`).

`eos-e2e.sh` (house style of `catalog-e2e.sh`: bounded waits, `trap` teardown, no fixed sleeps as gates): build images; generate the cluster key; `up` 3 nodes; bootstrap + create the e2e user (`docker/lib/auth.sh`: the `e2e` role holds `WRITE TRANSACTIONAL_ID PREFIX ''`, `READ GROUP PREFIX ''`, topic READ / WRITE / CREATE); `rune ql -f /topics/eos-e2e.rql` (topics `eos_in`, `eos_out`, rf 3); `fill`; start `pipeline`; after ~30 % progress `docker kill` the transaction coordinator (the consensus leader — read from `rune ql -e "show …"`/metadata the way `catalog-e2e.sh` finds the leader), restart it after the pipeline reports a new coordinator; after ~60 % kill an `eos_out` partition leader; `pipeline` must exit 0 within a bounded 600 s; `verify` must print `EOS-VERIFY: OK count=200000`; a zombie leg: start a second `pipeline` with the same `transactionalId` for 5 s, the first must report `Fenced` and the second continue — `verify` still exact; finally run `dr-e2e`'s `test_tx_fencing.py` against the same build. Print `EOS-E2E: OK`.

- [ ] **Step 1: Write the probe, script, compose, `.rql`, docs, release note.**
- [ ] **Step 2: Local static checks** — `bash -n docker/eos-e2e.sh`; the probe builds; `rune ql --dry-run -f docker/topics/eos-e2e.rql` parses.
- [ ] **Step 3: Run on the VM** (never the Mac):

```bash
rsync -a --delete /Users/vradu/Projects/se-worktrees/ root@$RUNE_VM_HOST:/root/se-worktrees/
ssh root@$RUNE_VM_HOST 'flock /root/vm.lock bash /root/se-worktrees/rune-broker/docker/eos-e2e.sh'
ssh root@$RUNE_VM_HOST 'flock /root/vm.lock bash -c "cd /root/se-worktrees/rune-broker/docker && ./view-engine-e2e.sh && ./catalog-e2e.sh"'
```
Expected: `EOS-E2E: OK` (count 200 000, each once, the zombie fenced); the view-engine e2e (its read_committed transaction leg now on the unified producer) and the catalog e2e still pass.
- [ ] **Step 4: Commit** (client + broker) — `test(eos): 3-node exactly-once e2e on the unified producer (every path, injected aborts + rewind, coordinator and leader kills, zombie fenced); docs and release notes`.

- [ ] **Checkpoint D (controller):** full `*Regression*` in both repos (+ sanitizers), the e2e set green on the VM; then the final whole-branch review.

---

## Self-review

- **Spec §F1:** `transactionalId` + validation → Task 1; lifecycle names / semantics → Tasks 5, 7, 8; every produce path in a transaction → Task 6 (+ matrix ops, Task 12); AddPartitions gating with a per-partition preallocated membership → Task 6; misuse explicit → Task 1 (plain), Tasks 1 / 6 / 8 (outside a txn), FD2; `acks = None` rejected → Task 1; `TransactionalProducer` removed, tests migrated, `TransactionalProducerImpl` deleted not wrapped → Task 9; state machine as explicit states → Task 5 (`MachineStatesCarryNoBooleans`).
- **Spec §F2:** broker-assigned ids, dedup across reconnects, `acks = None` unchanged → Task 4, Task 11.
- **Spec §F3:** `seekToCommitted` (+ AndWait, token), `seek` (+ `InvalidArgument`), the exactly-once loop without a restart → Task 10, Task 11 (`AbortRewindRedoOutputEqualsInputRegression`), Task 15.
- **Gates:** every path committed visible / aborted invisible, mixed paths, refused outside, lifecycle refused on plain, fencing, in-doubt retry, plain id + dedup across reconnect, seekToCommitted redelivery → Tasks 5–13; benchmarks (txn vs plain, p50 / p99, 0 allocations) → Task 14; docs → Task 15.
- **Hard rules:** shared-nothing / rings / per-state data / zero alloc are restated per task; the fix-on-sight items each own a regression test.
- **Task count:** 15; each sized for one Sonnet session (Tasks 5, 6 and 9 are the largest: their test lists are the port of named existing tests).

## Spec gaps (found while planning; resolved by the Decisions above — the spec text is not edited)

- **G1 — Ordering of control ops vs records** in Thread mode is unspecified; today's txn producer got it for free (one command ring). Resolved by FD1 (the producer's own ring).
- **G2 — Who may call a transactional producer** (Thread mode allows any thread for plain sends). Resolved by FD3 (one issuing thread; `ClientShard` refuses a `transactionalId`).
- **G3 — Records sent before a plain producer's broker id arrives.** Resolved by FD4 (held, never sent with a guessed id) — and FD4's rig default keeps the hundreds of bare-`ProducerImpl` tests valid.
- **G4 — `TypedTopic` owns its producer**, so `typed.claim(k)` inside `p`'s transaction (spec example) cannot work as is. Resolved by FD7 (`ProducerRef`).
- **G5 — Auth codes inside a transaction** (45 / 46) are not in spec F. Resolved by FD6 (Kafka parity: authorization on the transactional id is fatal, on topics / groups abortable; `SessionRevoked` fatal).
- **G6 — `seek` on an unassigned partition from a void call** cannot return `InvalidArgument` synchronously without reading duty state. Resolved in Task 10 (the check is reported by the token / AndWait forms).
- **G7 — Tier-2 batches allocate per batch** (hot path), and the spec gate says 0 allocations "on every path". Resolved by Task 3 (fix-on-sight, slot pool).
