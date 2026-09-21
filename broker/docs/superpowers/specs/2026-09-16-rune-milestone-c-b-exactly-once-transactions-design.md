# Milestone C-B — Exactly-Once / Transactions (design)

> **STATUS: proposed design, ready for review.** Second sub-project of the rest of
> Milestone C (A = consumer groups ✅ done; **B = exactly-once/transactions**;
> C = log compaction). Sequencing: B before C. Builds on MC-A (consumer groups).

## 1. Purpose & motivation

Turn Rune from at-least-once into **exactly-once (EOS)**. Three capabilities:

1. **Idempotent producer** — dedup by `(producerId, producerEpoch, sequence)` so a
   producer retry never double-appends; exactly-once *per-partition produce*.
2. **Transactions** — atomically produce across multiple partitions and atomically
   commit consumer offsets (`beginTransaction … commitTransaction`), so a
   consume-transform-produce pipeline is all-or-nothing.
3. **read_committed isolation** — consumers can be configured to read only committed
   records (≤ LSO, aborted records filtered), completing the EOS read side.

This wires the already-built + unit-tested Phase-5 engine (`ProducerStateManager`,
`TransactionCoordinator`, `TransactionLog`) into the gateway, protocol, and client —
the same "engine exists, wire it" pattern as MC-A.

## 2. Scope & non-goals

**In scope (v1):**
- Idempotent producer dedup on the produce path (pid/epoch/sequence).
- Full transactions: `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn` +
  `TxnOffsetCommit`, `EndTxn` (commit/abort); commit/abort **control markers** in
  partition logs; `__transaction_state` internal topic.
- Producer fencing (epoch bump fences zombies).
- **read_committed / LSO** isolation + aborted-txn filtering on BOTH plain
  `subscribe` and group `GroupConsumer` consume paths.
- Client Tier-3 transactional producer API + idempotent-by-default producer.
- DR-E2E (atomicity / fencing / read_committed) + benchmarks.

**Non-goals (deferred):**
- Hash-based multi-node coordinator placement — Milestone D (leader-only + seam here).
- Compaction of `__transaction_state` — MC-C.
- Cross-session producer-id reuse / producer-id expiry GC tuning — defaults only.

## 3. Locked decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| EOS scope | **Full EOS** (idempotent + txn + read_committed) | The point of the milestone; wires all of Phase-5 |
| Coordinator placement | **Cluster leader = sole txn coordinator**; `coordinatorFor(txnId)` seam; REDIRECT elsewhere | Matches MC-A; Milestone-D-ready |
| `__transaction_state` + control-marker durability | **Majority-durable (acks=All)** | Correctness crux — a lost commit marker breaks atomicity; txn commits are infrequent so latency is acceptable (differs from MC-A's minIsr=1 offsets log) |
| read_committed bound | **LSO (last-stable-offset) + aborted-txn index** | Kafka-faithful; reuses HW-bounded push (GR-6) with an LSO cap + abort filter |
| Client API | **Kafka-standard Tier-3** (`initTransactions`/`begin`/`send`/`sendOffsetsToTransaction`/`commit`/`abort`) | Familiar, complete |

## 4. Architecture: components & boundaries

### 4.1 Protocol (`rune-protocol`, templateIds in the 32–63 reserved range)
New client request/response pairs (LE SBE, schema 120, zero-copy `ArrayView` for
arrays — mirror MC-A codecs):
- `InitProducerId{transactionalId?, transactionTimeoutMs}` / `…Response{producerId, producerEpoch, errorCode}`
- `AddPartitionsToTxn{transactionalId, producerId, producerEpoch, [topicId,partition]}` / `…Response{[…,errorCode]}`
- `AddOffsetsToTxn{transactionalId, producerId, producerEpoch, groupId}` / `…Response{errorCode}`
- `TxnOffsetCommit{transactionalId, producerId, producerEpoch, groupId, [topicId,partition,offset,metadata]}` / `…Response{[…,errorCode]}`
- `EndTxn{transactionalId, producerId, producerEpoch, commit:bool}` / `…Response{errorCode}`
- `Produce` gains `{producerId, producerEpoch, baseSequence}` (idempotent/txn framing).
- `Subscribe`/`OffsetFetch` isolation field already exists (0=read_uncommitted, 1=read_committed).
- Control markers (commit/abort) are INTERNAL RecordBatch control records written to
  partition logs — NOT client frames. New error codes: `InvalidProducerEpoch`,
  `ProducerFenced`, `InvalidTxnState`, `OutOfOrderSequence`, `DuplicateSequence` (as needed).

### 4.2 Idempotent-producer dedup (produce path)
Wire `ProducerStateManager` into `ClientGateway::handleProduce`: before append, validate
the batch's `(producerId, producerEpoch, baseSequence..lastSequence)`:
- `Accept` → append (existing GR path).
- `Duplicate` → ACK with the previously-assigned base offset (no re-append) — the
  idempotent guarantee.
- `OutOfOrderSequence` → reject (`OutOfOrderSequence`).
- `InvalidProducerEpoch` → reject (zombie fenced).
Per-partition producer state is durable/replicated (it lives in the partition log's
producer-state, recovered on restart/failover — Phase-1 ProducerState).

### 4.3 `TransactionCoordinationService` (broker, mirrors MC-A's GroupCoordinationService)
- Owns the `TransactionCoordinator`, driven on the duty cycle; implements the §4.1
  handlers via an `ITxnRequestHandler` interface in the gateway lib (same seam that
  broke the gateway↔node cycle in MC-A — gateway depends on the interface only).
- `initProducerId` allocates/bumps producerId+epoch (epoch bump fences the prior
  session's zombie); `addPartitions`/`addOffsets` register txn scope; `endTxn` drives
  PREPARE → write commit/abort **control markers** to each participant partition (via
  the leader replica append, acks=All) → COMPLETE.
- `__transaction_state` = a **majority-durable** replicated internal topic (like
  `__consumer_offsets` but acks=All); coordinator replays it on leader-transition to
  rebuild in-flight txn metadata (reuse the MC-A ReplicatedOffsetStore rebuild pattern
  for a `ReplicatedTxnStateStore`, or a txn-log variant).
- Leader-only + `coordinatorFor(txnId)`; non-coordinator → REDIRECT.
- Txn timeout: `doWork` aborts expired open txns (writes abort markers).

### 4.4 read_committed / LSO on the consume path
- The partition tracks **LSO** = the base offset of the earliest still-open
  transaction (= HW when no open txn). Maintained as control markers land.
- An **aborted-txn index** per partition records `[producerId → aborted offset ranges]`.
- Consume (subscribe + GroupConsumer push): for `isolation=read_committed`, bound
  delivery to **LSO** (not HW) and skip records belonging to aborted producer ranges;
  skip control-marker records themselves. `read_uncommitted` stays ≤HW (unchanged).
- Reuses the HW-bounded push machinery (GR-6) — adds the LSO cap + abort filter.

### 4.5 Client (`rune-client-cpp`) Tier-3 producer
- `initTransactions()` (→ InitProducerId, stores pid/epoch); `beginTransaction()`;
  `send(...)` (auto AddPartitionsToTxn on first send to a partition; stamps
  pid/epoch/sequence); `sendOffsetsToTransaction(offsets, groupId)` (AddOffsetsToTxn +
  TxnOffsetCommit); `commitTransaction()`/`abortTransaction()` (EndTxn).
- Idempotent-by-default producer (sequence numbering + retry dedup) even without txn.
- GroupConsumer gains `isolation=read_committed` config → sends it on subscribe.
- Wait-free hot path (reuse the DR-BUG-1/MC-A COW snapshot discipline); TSan-clean.

### 4.6 DR-E2E + benchmarks
- Atomic multi-partition produce is **all-or-nothing** across leader-kill (commit
  marker survives ⇒ all visible; abort/none ⇒ none visible).
- `read_committed` never exposes aborted or open-txn records; `read_uncommitted` does
  (≤HW) — differential test.
- Zombie producer (old epoch) is **fenced** after a new `initProducerId`.
- Consume-transform-produce **exactly-once** across a failover (no dup, no loss).
- Benchmarks: idempotent dedup per-batch overhead (~ns hot path, zero-alloc), txn
  begin/commit p50/p99, read_committed filter cost. 100µs gate on hot paths.

## 5. Data flow (transactional produce, happy path)
1. Client → `InitProducerId(txnId)` → coordinator returns `{producerId, epoch}` (epoch
   bump fences any prior session).
2. `beginTransaction()` (client-local state).
3. First `send` to partition P → `AddPartitionsToTxn(P)` → coordinator records P in the
   txn (durable in `__transaction_state`, acks=All); then the Produce append (with
   pid/epoch/seq) goes to P's leader (idempotent-validated).
4. `sendOffsetsToTransaction` → `AddOffsetsToTxn(group)` + `TxnOffsetCommit` (offsets
   staged as part of the txn).
5. `commitTransaction()` → `EndTxn(commit)` → coordinator PREPARE_COMMIT (durable) →
   writes **commit markers** to every participant partition + the offsets partition
   (acks=All) → COMPLETE. LSO on each partition advances past the committed records,
   making them visible to read_committed consumers.
   (abort → abort markers; records remain ≤offset but are filtered by read_committed.)

## 6. Failover & recovery
- Coordinator (leader) failover: new leader replays `__transaction_state` (majority-
  durable) → rebuilds in-flight txn metadata; PENDING txns are resolved (a
  PREPARE_COMMIT re-drives markers; an unprepared txn is abortable). No committed txn is
  lost (markers + state are majority-durable).
- Producer state (idempotent dedup) recovers from the partition log's producer-state on
  restart/failover (Phase-1) → dedup survives.
- read_committed correctness across failover: LSO + aborted index rebuilt from the log's
  control markers on recovery.

## 7. Milestone-D seam
`coordinatorFor(txnId)` returns the cluster leader in v1; becomes `hash(txnId)` under
Milestone D, and `__transaction_state` becomes a distributed internal topic. No other
component encodes "leader owns all."

## 8. Error handling / edge cases
- `InvalidProducerEpoch`/`ProducerFenced` → client must re-`initTransactions`.
- `OutOfOrderSequence` → producer error (gap); `Duplicate` → silently ACK prior offset.
- Txn timeout → coordinator aborts (abort markers); a late commit from the fenced
  producer is rejected.
- `EndTxn` for an unknown/!ongoing txn → `InvalidTxnState`.
- Control markers are skipped by all consume paths (never delivered to apps).
- read_committed with an open txn at offset K → deliver only < K (LSO), even if HW > K.

## 9. Testing & benchmarks

**Coverage bar: FULL — every edge case, no matter how small.** Match the exhaustive
regime used for DR-E2E and MC-A: **hundreds of unit tests**, comprehensive integration
tests, and the **full docker-based E2E harness** (disaster recovery + latency gating +
smoke tests) plus a **complete benchmark matrix**. Nothing is "obvious enough to skip."

### 9.1 Unit tests (target: hundreds of cases across the suites below)
- **Idempotent dedup (`ProducerStateManager`):** accept in-order; exact duplicate (same
  pid/epoch/seq) → prior offset; partial-overlap batch; out-of-order (gap) → reject;
  seq wraparound at 2^31; epoch bump fences old epoch; unknown pid; pid with no prior
  seq; concurrent multi-partition seq spaces; first-sequence-not-zero; empty batch;
  max-batch-size boundary; state eviction/expiry boundaries.
- **Coordinator state machine (`TransactionCoordinator`):** every legal transition
  (Empty→Ongoing→PrepareCommit/PrepareAbort→CompleteCommit/CompleteAbort→Empty) AND
  every illegal transition → `InvalidTxnState`; EndTxn on Empty/unknown; double-commit;
  commit-after-abort; add-partitions after prepare; timeout-fires-mid-prepare;
  epoch-fence during each state.
- **`ReplicatedTxnStateStore`:** commit→replicate→replay round-trip; replay across
  multiple segments (multi-segment durability — the MC-A CG-2 bug class); acks=All
  majority wait; replay reconstructs in-flight scope exactly; empty log; corrupt/partial
  tail record tolerated.
- **LSO / aborted-index / read_committed filter:** LSO = HW when no open txn; LSO pinned
  by earliest open txn; LSO advance on commit marker; aborted-range add/remove; filter
  drops aborted records; filter drops control markers; open-txn record hidden even when
  HW past it; interleaved committed+aborted from different pids; boundary at exactly LSO.
- **Protocol codecs:** encode/decode round-trip + golden `.bin` conformance vectors for
  every new message (InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/TxnOffsetCommit/
  EndTxn + Produce pid/epoch/seq fields); empty arrays; max-cardinality arrays; every new
  error code; truncated-buffer rejection; `outCap` overrun guard (the MC-A CG-3 class).

### 9.2 Integration tests (broker in-process, real logs)
- Atomic multi-partition commit visible only after commit marker; abort hides all
  records from read_committed while read_uncommitted still sees them (differential).
- TxnOffsetCommit atomicity: staged offsets become visible on commit marker only, in
  lockstep with the data records (interaction with MC-A `ReplicatedOffsetStore`).
- Zombie fencing end-to-end (old-epoch produce/commit rejected after re-init).
- Coordinator-failover in-flight-txn resolution (PrepareCommit re-drives markers;
  unprepared txn abortable); producer-state recovery after restart (dedup survives).
- Txn timeout → auto-abort → late commit from fenced producer rejected.

### 9.3 Docker E2E (full harness — reuse `dr-e2e/`, matrix over 1/3/5-node)
- **Disaster recovery:** atomicity across leader-kill (commit marker survives ⇒ all
  visible; abort/none ⇒ none) and follower-kill/quorum-loss/recover; consume-transform-
  produce **exactly-once** across failover (no dup, no loss) validated by generation-
  aware single-owner + no-loss/no-dup invariants (the MC-A harness).
- **read_committed differential** under churn; zombie fencing under load.
- **Latency gating:** every E2E scenario asserts hot-path p99 under the **100µs gate**;
  a regression FAILS the run (same gate the DR/MC-A suites enforce).
- **Smoke tests:** single-node + 3-node bring-up, init→begin→produce→commit→consume
  happy path, clean shutdown — fast pre-flight before the full matrix.

### 9.4 Benchmarks (ALL of them — 100µs gate on hot paths, zero-alloc asserted)
- Idempotent dedup per-batch overhead (ns-class, zero-alloc) — the produce hot path.
- Txn `begin`/`addPartitions`/`commit`/`abort` p50/p99/p999 (control-marker acks=All).
- `read_committed` filter cost vs `read_uncommitted` baseline (per-record + per-batch).
- InitProducerId / epoch-bump throughput; coordinator replay time vs `__transaction_state`
  size. Contention/scaling sweeps (producers × partitions × nodes). Any hot-path p99
  >100µs = STOP-and-fix before the milestone closes.

## 10. Ticket breakdown (proposed)
- **TX-1** — protocol codecs (InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/
  TxnOffsetCommit/EndTxn + Produce pid/epoch/seq fields) + conformance.
- **TX-2** — idempotent-producer dedup wired into `handleProduce` (ProducerStateManager;
  Duplicate→ack-prior-offset; fencing).
- **TX-3** — `TransactionCoordinationService` + handlers + `ITxnRequestHandler` seam;
  `__transaction_state` majority-durable replicated store + replay-on-transition.
- **TX-4** — control markers (commit/abort) on participant partitions + LSO + aborted-
  txn index + read_committed filtering on subscribe & GroupConsumer.
- **TX-5** — client Tier-3 transactional producer API + idempotent-by-default + read_
  committed GroupConsumer.
  (Each TX-1..TX-5 lands its own §9.1 unit + §9.2 integration coverage — hundreds of
  cases total, every edge case — before its ticket is Done.)
- **TX-6** — full docker E2E (§9.3): DR/failover EOS scenarios + read_committed
  differential + fencing + latency gating + smoke, matrix over 1/3/5-node.
- **TX-7** — full benchmark matrix (§9.4) + address any hot-path p99 >100µs +
  full-area Opus review to CLEAN.

## 11. Open questions (resolve during planning)
- `__transaction_state` as its own store vs reusing `TransactionLog` (Phase-5) over a
  replicated partition Log — pick one in TX-3.
- Aborted-txn index: in-memory rebuilt-from-markers vs a persisted index — lean
  in-memory-rebuilt-from-log for v1 (bounded by open-txn horizon).
- `AddPartitionsToTxn` auto-sent by the client on first send vs explicit — lean auto
  (Kafka behavior).
- Interaction of TxnOffsetCommit with MC-A's `ReplicatedOffsetStore` (txn-staged offsets
  become visible only on commit marker) — the offsets partition also gets markers.
