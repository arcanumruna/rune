# Milestone C-B — Exactly-Once / Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Rune from at-least-once into exactly-once — wire the already-built Phase-5 engine (`ProducerStateManager`, `TransactionCoordinator`, `TransactionLog`, `ControlMarker`, `ReadCommittedView`) into the protocol, gateway, and client so producers dedup on retry, transactions are atomic across partitions + offsets, and `read_committed` consumers never see aborted/open-txn records.

**Architecture:** Idempotent dedup runs in `ClientGateway::handleProduce` (validate `(pid,epoch,seq)` → append or ack-prior-offset). A `TransactionCoordinationService` (mirrors MC-A's `GroupCoordinationService`) owns the `TransactionCoordinator`, is driven on the duty cycle, and is reached from the gateway through a new `ITxnRequestHandler` seam (same interface trick that broke the gateway↔node cycle in MC-A). Txn state persists on a **majority-durable** (`acks=All`) replicated `__transaction_state` topic; commit/abort **control markers** are appended to participant partitions via `PartitionReplica::append(acksAll=true)`. `read_committed` consumers are bounded by **LSO** + an aborted-txn filter on the existing HW-bounded push path (GR-6). The cluster leader is the sole coordinator behind a `coordinatorFor(txnId)` seam (Milestone D).

**Tech Stack:** C++23, GoogleTest, Google Benchmark, Aeron, SBE-style flat LE codecs (schema 120), pytest + Docker (dr-e2e). Repos: `rune-protocol`, `rune-broker`, `rune-client-cpp`.

**Spec:** `docs/superpowers/specs/2026-09-16-rune-milestone-c-b-exactly-once-transactions-design.md`

## Global Constraints

- Commit to **local `main` only — never push** (unless the user explicitly asks).
- **Never** `conan create` / `export` / `upload` — packages are CI-owned (a PreToolUse hook enforces this).
- Any benchmark **p99 > 100µs on a hot path = STOP and fix** before proceeding (or prove pure-harness-artifact + produce a corrected < 100µs measurement).
- **Single-writer duty-cycle discipline:** all coordinator / producer-state / marker / push state is mutated only on the duty-cycle thread; inject the clock (`nowNs`) sampled once per cycle; **no locks, no per-op heap allocation, no blocking** on the hot path (produce dedup + read_committed filter are hot paths).
- **`__transaction_state` durability = `acks=All` (majority-durable)** — this is the correctness crux and differs from MC-A's `__consumer_offsets` (minIsr=1). A lost commit marker breaks atomicity.
- Client protocol schema id = **120**; transaction templateIds occupy **45–54** (inside the reserved groups/txn range 32–63; groups already took 32–44). New error codes start at **17** (groups ended at 16).
- Every task ends green (unit/integration), is committed, and — where it maps to a TX ticket — updates Planka (move card, comment) and pushes its test run to live via `scripts/rune-live.sh test <suite> passed passed=.. failed=.. total=.. commit=..`.
- Build with CLion's bundled cmake + `conan_host_profile` (x86_64 / cppstd=23 / Debug): `export PATH="/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin:$PATH"` then build in `rune-broker/build/` (Unix Makefiles, `BUILD_TESTING=ON`).
- **Model policy:** Sonnet implements each task against this brief; Opus reviews (safety-critical — always Opus). Opus finds an issue → Sonnet fixes (first attempt). Sonnet fails / regresses / introduces a new issue → **Opus takes over that corrective fix**, then re-review.
- **Faithful-Aeron / Kafka-faithful** discipline: mirror the existing codec/handler/test/bench patterns exactly (`GroupMessages.h`, `IGroupRequestHandler`, `GroupCoordinationService`, `ExactlyOnceHarness`, `TransactionBenchmark.cpp`). Opus review gates on allocation + latency + correctness.

---

## File Structure

**rune-protocol** (`include/rune/protocol/client/`)
- `TxnMessages.h` — CREATE: InitProducerId/Response, AddPartitionsToTxn/Response, AddOffsetsToTxn/Response, TxnOffsetCommit/Response, EndTxn/Response codecs (mirror `GroupMessages.h`/`OffsetMessages.h` + `ArrayView`).
- `Protocol.h` — MODIFY: add `TemplateId` 45–54 + txn `ErrorCode`s 17–21.
- `DataMessages.h` — MODIFY: add `producerId`/`producerEpoch`/`baseSequence` to `Produce`.
- `tests/` — conformance vectors + round-trip tests per message.

**rune-broker**
- `libs/gateway/include/rune/gateway/ITxnRequestHandler.h` — CREATE: the gateway→coordinator seam (mirror `IGroupRequestHandler.h`).
- `libs/gateway/src/ClientGateway.cpp` (+ `.h`) — MODIFY: dispatch txn templateIds; idempotent dedup in `handleProduce`; LSO cap + abort filter in `pushSubscriptions`; `setTxnService()`.
- `libs/coordination/include/rune/coordination/ReplicatedTxnStateStore.h` (+ `src`) — CREATE: `__transaction_state` over a `PartitionReplica` (acks=All) with replay (mirror `ReplicatedOffsetStore`).
- `libs/node/include/rune/node/TransactionCoordinationService.h` (+ `src`) — CREATE: owns `TransactionCoordinator`, implements `ITxnRequestHandler`, drives `doWork`, writes markers, `coordinatorFor(txnId)` seam (mirror `GroupCoordinationService`).
- `libs/node/src/NodeRuntimeAgent.*` — MODIFY: construct + wire the service; drive `doWork`; register via `setTxnService`.
- `benchmark/TransactionBenchmark.cpp` — MODIFY/EXTEND: complete the hot-path matrix.
- `libs/*/tests/{unit,integration}/…` — tests per component (reuse `ExactlyOnceHarness.h`).

**rune-client-cpp** (`include/rune/client/`)
- `producer/Producer.h` (+ `src`) — MODIFY: idempotent-by-default sequence numbering + Tier-3 transactional API.
- `producer/TransactionalProducer.h` — CREATE (or extend `Producer`): `initTransactions/begin/send/sendOffsetsToTransaction/commit/abort`.
- `group/GroupConsumer.h` — MODIFY: `isolation` config → sent on subscribe.
- `tests/…` — unit + integration.

**dr-e2e**
- `harness/driver.py` / `dr_client` — MODIFY: transactional-produce + read_committed modes.
- `tests/test_tx_*.py` — CREATE: atomicity / read_committed / fencing / consume-transform-produce EOS.

---

## TX-1 — Protocol codecs (txn messages + Produce idempotent fields)

### Task 1: Txn templateIds + error codes

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h`
- Test: `rune-protocol/tests/TxnProtocolTest.cpp` (new target file)

**Interfaces:**
- Produces: `TemplateId::{InitProducerId=45, InitProducerIdResponse=46, AddPartitionsToTxn=47, AddPartitionsToTxnResponse=48, AddOffsetsToTxn=49, AddOffsetsToTxnResponse=50, TxnOffsetCommit=51, TxnOffsetCommitResponse=52, EndTxn=53, EndTxnResponse=54}` and `ErrorCode::{InvalidProducerEpoch=17, ProducerFenced=18, InvalidTxnState=19, OutOfOrderSequence=20, DuplicateSequence=21}`.

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/protocol/client/Protocol.h>
using namespace rune::protocol::client;
TEST(TxnProtocol, TemplateIdsInReservedRange) {
    EXPECT_EQ(static_cast<uint16_t>(TemplateId::InitProducerId), 45);
    EXPECT_EQ(static_cast<uint16_t>(TemplateId::EndTxnResponse), 54);
    EXPECT_LE(static_cast<uint16_t>(TemplateId::EndTxnResponse), 63); // groups/txn range
}
TEST(TxnProtocol, TxnErrorCodes) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidProducerEpoch), 17);
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidTxnState),      19);
}
```
- [ ] **Step 2: Run to verify it fails** — `ctest -R TxnProtocol` → FAIL (enumerators undefined).
- [ ] **Step 3: Add enumerators** to `TemplateId` (45–54) and `ErrorCode` (17–21) in `Protocol.h`, keeping the `// groups/txn 32-63` range comment.
- [ ] **Step 4: Run to verify pass** — `ctest -R TxnProtocol` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(protocol): [TX-1] transaction templateIds (45-54) + txn error codes"`.

### Task 2: Produce idempotent framing fields

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/DataMessages.h` (`Produce` struct, lines ~39–93)
- Test: `rune-protocol/tests/` (existing Produce codec test + a new case)

**Interfaces:**
- Produces: `Produce` gains `int64_t producerId = -1; int16_t producerEpoch = 0; int32_t baseSequence = -1;` (sentinel `producerId == -1` ⇒ non-idempotent legacy produce). Fields are appended **after** `acks` in the fixed body so existing decoders that stop earlier still parse the prefix; bump the body layout + `encodedSize()` accordingly.

- [ ] **Step 1: Write the failing round-trip test**
```cpp
TEST(DataMessages, ProduceCarriesIdempotentFraming) {
    Produce in;
    in.sessionId = 1; in.topicId = 7; in.partition = 0; in.acks = 1;
    in.producerId = 4242; in.producerEpoch = 3; in.baseSequence = 100;
    const uint8_t body[] = {0xAA, 0xBB};
    in.recordBatch = {body, sizeof(body)};
    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto out = Produce::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId, 4242);
    EXPECT_EQ(out->producerEpoch, 3);
    EXPECT_EQ(out->baseSequence, 100);
    EXPECT_EQ(out->acks, 1);
    ASSERT_EQ(out->recordBatch.size(), 2u);
}
TEST(DataMessages, ProduceDefaultsAreNonIdempotent) {
    Produce in; in.topicId = 1;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    auto out = Produce::decode(buf.data(), buf.size());
    ASSERT_TRUE(out); EXPECT_EQ(out->producerId, -1); // sentinel
}
```
- [ ] **Step 2: Run to verify it fails** — new fields not present → compile/assert FAIL.
- [ ] **Step 3: Implement** — add the three fields to `Produce`, extend the fixed-body write/read + `encodedSize()` (keep `key`/`recordBatch` var fields last, unchanged order).
- [ ] **Step 4: Run to verify pass** — `ctest -R DataMessages` → PASS.
- [ ] **Step 5: Commit** — `feat(protocol): [TX-1] Produce idempotent framing (producerId/epoch/baseSequence)`.

### Task 3: InitProducerId / EndTxn codecs

**Files:** Create `rune-protocol/include/rune/protocol/client/TxnMessages.h`; test `TxnProtocolTest.cpp`.

**Interfaces (mirror `GroupMessages.h` — `MessageHeader` prefix, fixed ints, `putVarBytes`/`getVarBytes` for the transactionalId, decode→`optional`):**
- Produces:
  - `struct InitProducerId { int64_t sessionId, correlationId; std::span<const uint8_t> transactionalId; int32_t transactionTimeoutMs; encode/encodedSize/decode; TEMPLATE_ID=45; }` (empty `transactionalId` ⇒ idempotent-only producer).
  - `struct InitProducerIdResponse { int64_t sessionId, correlationId; int64_t producerId; int16_t producerEpoch; int16_t errorCode; TEMPLATE_ID=46; }`.
  - `struct EndTxn { int64_t sessionId, correlationId; std::span<const uint8_t> transactionalId; int64_t producerId; int16_t producerEpoch; uint8_t commit; TEMPLATE_ID=53; }`.
  - `struct EndTxnResponse { int64_t sessionId, correlationId; int16_t errorCode; TEMPLATE_ID=54; }`.

- [ ] **Step 1: Write failing round-trip tests** for `InitProducerId` (var `transactionalId`, empty-id case) and `EndTxn` (fixed `commit` flag) + a truncation test (`decode(buf, len-1)` → `nullopt`).
- [ ] **Step 2: Run** → FAIL (header missing).
- [ ] **Step 3: Implement** `TxnMessages.h` following the `GroupMessages.h` pattern; `#include <rune/protocol/client/Codec.h>` for `putVarBytes`/`getVarBytes`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(protocol): [TX-1] InitProducerId/EndTxn codecs`.

### Task 4: AddPartitionsToTxn / AddOffsetsToTxn / TxnOffsetCommit codecs + Dispatch + conformance

**Files:** Modify `TxnMessages.h`, `Dispatch.h`; tests + conformance vectors.

**Interfaces (arrays use the zero-copy `ArrayView` fixed-stride pattern from `OffsetMessages.h`):**
- `AddPartitionsToTxn{transactionalId, producerId, producerEpoch, [ {topicId:int32, partition:int32} ]}` (47) / `…Response{ [ {topicId,partition,errorCode:int16} ] }` (48).
- `AddOffsetsToTxn{transactionalId, producerId, producerEpoch, groupId}` (49) / `…Response{errorCode}` (50).
- `TxnOffsetCommit{transactionalId, producerId, producerEpoch, groupId, [ {topicId,partition,offset:int64,metadata} ]}` (51) / `…Response{ [ {topicId,partition,errorCode} ] }` (52).

- [ ] **Step 1: Write failing tests** — multi-entry `AddPartitionsToTxn` round-trip (fixed-stride array via `ArrayView`), `TxnOffsetCommit` with per-entry var `metadata`, `AddOffsetsToTxn` with var `groupId`. Add a `Dispatch` test asserting `peekTemplateId` returns 45–54 and `classify` does not flag them `UnknownTemplate`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the three request/response pairs in `TxnMessages.h`; extend `Dispatch.h`'s known-template set to include 45–54.
- [ ] **Step 4: Run** → PASS. Add encoded-frame **conformance vectors** (fixed byte arrays + expected decode) for InitProducerId, AddPartitionsToTxn, TxnOffsetCommit, EndTxn — mirror the existing conformance harness.
- [ ] **Step 5: Commit** — `feat(protocol): [TX-1] AddPartitions/AddOffsets/TxnOffsetCommit codecs + dispatch + conformance`.
- [ ] **Step 6:** Full protocol suite green; **Planka** TX-1 → Done + comment; **live** push `rune-protocol/txn passed`.

---

## TX-2 — Idempotent-producer dedup in `handleProduce`

**Files:**
- Modify: `rune-broker/libs/gateway/src/ClientGateway.cpp` (`handleProduce`, ~line 737) + `.h` (hold a `ProducerStateManager` per (topic,partition) via the replica registry).
- Consumes: `rune::storage::ProducerStateManager::validate(int64_t producerId, int32_t partition, int16_t producerEpoch, int32_t baseSequence, int32_t lastSequenceInBatch) -> ValidationResult{Status::{Accept,Duplicate,OutOfOrderSequence,InvalidProducerEpoch}}` and `::update(producerId, partition, epoch, baseSeq, lastSeq, baseOffset)` (`libs/storage/include/rune/storage/ProducerStateManager.h`); `PartitionReplica::append(span, acksAll)` + `highWatermark()`.
- Test: `libs/gateway/tests/integration/ProduceIdempotentTest.cpp`.

**Behavior:** In `handleProduce`, after decoding `Produce`: if `producerId == -1` → legacy path (append as today). Else compute `lastSequence = baseSequence + recordCount - 1` (record count from the batch header), then:
- `Accept` → append via the existing GR path; on accepted base offset, call `update(...)` with that base offset.
- `Duplicate` → **do not append**; respond `ProduceResponse` with the previously-assigned base offset (the idempotent guarantee) — look it up from producer state.
- `OutOfOrderSequence` → `ProduceResponse{errorCode=OutOfOrderSequence}`.
- `InvalidProducerEpoch` → `ProduceResponse{errorCode=InvalidProducerEpoch}` (zombie fenced).
Producer state is per-partition and rides the partition log's producer state (recovered on restart/failover — Phase-1). No per-op heap alloc.

- [ ] **Step 1: Write the failing integration test**
```cpp
TEST(ProduceIdempotent, DuplicateBatchReturnsPriorOffsetNoReappend) {
    auto gw = makeGatewayWithReplica(/*topic=*/1, /*part=*/0); // existing helper
    auto r1 = produce(gw, /*pid=*/9, /*epoch=*/0, /*baseSeq=*/0, /*records=*/3);
    ASSERT_EQ(r1.errorCode, 0);
    auto leo1 = gw.logEndOffset(1,0);
    auto r2 = produce(gw, 9, 0, 0, 3);                 // exact retry
    EXPECT_EQ(r2.errorCode, 0);
    EXPECT_EQ(r2.baseOffset, r1.baseOffset);           // prior offset, not a new append
    EXPECT_EQ(gw.logEndOffset(1,0), leo1);             // nothing re-appended
}
TEST(ProduceIdempotent, OutOfOrderRejected) {
    auto gw = makeGatewayWithReplica(1,0);
    produce(gw, 9, 0, 0, 3);
    auto r = produce(gw, 9, 0, /*baseSeq=*/10, 3);     // gap
    EXPECT_EQ(r.errorCode, (int)ErrorCode::OutOfOrderSequence);
}
TEST(ProduceIdempotent, OldEpochFenced) {
    auto gw = makeGatewayWithReplica(1,0);
    produce(gw, 9, /*epoch=*/1, 0, 1);
    auto r = produce(gw, 9, /*epoch=*/0, 1, 1);        // zombie
    EXPECT_EQ(r.errorCode, (int)ErrorCode::InvalidProducerEpoch);
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the dedup branch in `handleProduce`; obtain/create the per-partition `ProducerStateManager` from the replica registry; wire `validate`/`update` and the Duplicate→prior-offset response.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(gateway): [TX-2] idempotent-producer dedup (validate/update, dup→prior-offset, fencing)`.
- [ ] **Step 6:** Unit-cover the sequence edge cases (partial overlap, first-seq-not-zero, seq at 2^31 boundary) in `libs/storage/tests/storage/ProducerStateManagerTest.cpp` if not already; green. **Planka** TX-2 → Done; **live** push.

---

## TX-3 — TransactionCoordinationService + ITxnRequestHandler seam + `__transaction_state`

### Task 5: `ReplicatedTxnStateStore` over `__transaction_state` (acks=All)

**Files:** Create `libs/coordination/include/rune/coordination/ReplicatedTxnStateStore.h` + `src`; test `libs/coordination/tests/coordination/ReplicatedTxnStateStoreTest.cpp` + `libs/node/tests/integration/TxnStateReplicationTest.cpp`.

**Interfaces:**
- Consumes: `PartitionReplica::append(span, /*acksAll=*/true)`, `highWatermark()`; the `TransactionLog` record codec (`libs/transaction/include/rune/transaction/TransactionLog.h` — magic `0x544B4E4C`, `kFixedHeaderSize=52`, CRC32C tail) reused as the **record body**.
- Produces:
  ```cpp
  class ReplicatedTxnStateStore {
  public:
    explicit ReplicatedTxnStateStore(rune::replication::PartitionReplica* replica);
    // Append one txn-state record via replica.append(acksAll=true); false if minIsr-gated.
    bool put(const rune::transaction::TransactionMetadata&, int32_t coordinatorEpoch);
    const rune::transaction::TransactionMetadata* get(const rune::transaction::TransactionalId&) const;
    // Replay [0, highWatermark()) folding last-write-wins per txnId.
    void rebuildFromLog();
  };
  ```

**Architecture note:** reuse the exact `TransactionLog` on-disk record layout as the record body appended to the replicated partition Log, so one decoder serves both. `put` builds one record → `replica->append(record, /*acksAll=*/true)`; in-memory map updated only after the append is accepted (offset ≥ 0). `rebuildFromLog` reads `[0, HW)` across **all segments** (the MC-A CG-2 multi-segment durability bug class — do not read only the first segment).

- [ ] **Step 1: Write the failing unit test** (single-node loopback replica so HW advances on append)
```cpp
TEST(ReplicatedTxnStateStore, PutThenGetRoundTrip) {
    auto replica = makeSingleNodeReplica(/*txnStateTopic*/0,0);
    rune::coordination::ReplicatedTxnStateStore store(replica.get());
    rune::transaction::TransactionMetadata m = makeMeta("tx1", /*pid=*/7, /*epoch=*/0,
                                                        rune::transaction::TxnState::Ongoing);
    ASSERT_TRUE(store.put(m, /*coordEpoch=*/1));
    driveHwAdvance(*replica);
    const auto* got = store.get("tx1");
    ASSERT_TRUE(got); EXPECT_EQ(got->state, rune::transaction::TxnState::Ongoing);
}
TEST(ReplicatedTxnStateStore, RebuildLastWriteWinsAcrossSegments) {
    auto replica = makeSingleNodeReplica(0,0);
    { rune::coordination::ReplicatedTxnStateStore s(replica.get());
      s.put(makeMeta("tx1",7,0,rune::transaction::TxnState::Ongoing),1);
      s.put(makeMeta("tx1",7,0,rune::transaction::TxnState::CompleteCommit),1);
      driveHwAdvance(*replica); }
    rune::coordination::ReplicatedTxnStateStore s2(replica.get());
    s2.rebuildFromLog();
    ASSERT_TRUE(s2.get("tx1"));
    EXPECT_EQ(s2.get("tx1")->state, rune::transaction::TxnState::CompleteCommit);
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `ReplicatedTxnStateStore` (record encode reused from `TransactionLog`; `put`→`replica->append(acksAll=true)`; `rebuildFromLog`→fold `[0,HW)` over all segments).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(coordination): [TX-3] ReplicatedTxnStateStore over __transaction_state (acks=All)`.
- [ ] **Step 6: Integration test** (`TxnStateReplicationTest.cpp`, 3-in-process-node harness): put on leader → kill leader → new leader `rebuildFromLog()` → `get` returns the txn state (no loss). Run → PASS. Commit.

### Task 6: `ITxnRequestHandler` seam + `TransactionCoordinationService` skeleton

**Files:** Create `libs/gateway/include/rune/gateway/ITxnRequestHandler.h`; create `libs/node/include/rune/node/TransactionCoordinationService.h` + `src`; modify `ClientGateway.*` (`setTxnService` + dispatch); modify `NodeRuntimeAgent` (construct + `doWork`-drive + register). Test `libs/node/tests/TransactionCoordinationServiceTest.cpp`.

**Interfaces:**
- Produces (mirror `IGroupRequestHandler.h`):
  ```cpp
  struct ITxnRequestHandler {
    virtual ~ITxnRequestHandler() = default;
    virtual size_t handleInitProducerId (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) = 0;
    virtual size_t handleAddPartitions  (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) = 0;
    virtual size_t handleAddOffsets     (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) = 0;
    virtual size_t handleTxnOffsetCommit(const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) = 0;
    virtual size_t handleEndTxn         (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) = 0;
  };
  ```
  ```cpp
  class TransactionCoordinationService : public rune::gateway::ITxnRequestHandler {
  public:
    TransactionCoordinationService(ReplicatedTxnStateStore* txnState,
                                   rune::gateway::TopicRegistry* topics,
                                   std::function<bool()> isLeader = []{return true;},
                                   std::function<NodeId()> leaderId = []{return NodeId{0};});
    int  doWork(int64_t nowNs);                 // drives TransactionCoordinator::doWork + resolvePendingMarkers
    NodeId coordinatorFor(const TransactionalId&) const;   // v1: cluster leader
    bool isLocalCoordinator() const;
    void setGateway(rune::gateway::ClientGateway*) noexcept; // for marker append (Task 8)
    // ITxnRequestHandler impls...
  };
  ```
- Consumes: `TransactionCoordinator` (`initProducerId/addPartitions/addOffsetsToTxn/endTransaction/completeTransaction/doWork/pendingMarkerTransactions`), `ReplicatedTxnStateStore`.
- ClientGateway: add `void setTxnService(ITxnRequestHandler*)` + dispatch templateIds 45–53 (requests) to a `handleTxnMessage(buf,len,nowNs,templateId)` (54 EndTxnResponse etc. are egress).

- [ ] **Step 1: Failing tests** — `doWork` on an empty service returns 0; when `isLocalCoordinator()==false`, every handler responds `NotCoordinator`; `coordinatorFor` returns the leader nodeId regardless.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the interface + service skeleton; wire `service.doWork(nowNs)` into `NodeRuntimeAgent::doWork` (same thread, after replication, alongside the group service); register via `gateway.setTxnService(&service)`; add the gateway dispatch cases (mirror `handleGroupMessage`).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(node+gateway): [TX-3] ITxnRequestHandler seam + TransactionCoordinationService skeleton + dispatch`.

### Task 7: Txn handlers (init / addPartitions / addOffsets / txnOffsetCommit / endTxn)

**Files:** Modify `TransactionCoordinationService.*`; tests.

**Interfaces:** each handler decodes (TX-1 codec) → calls the `TransactionCoordinator` → persists the resulting `TransactionMetadata` via `ReplicatedTxnStateStore::put(acks=All)` **before** responding → encodes the response. `endTxn(commit)` sets PREPARE state (durable) and leaves marker-writing to `doWork`/Task 8. Epoch mismatch → `InvalidProducerEpoch`/`ProducerFenced`; bad state → `InvalidTxnState`.

- [ ] **Step 1: Failing tests** — `handleInitProducerId` allocates pid+epoch (and a second init with the same txnId **bumps epoch**, fencing the prior); `handleAddPartitions` moves state to `Ongoing` and is durable (get returns it); `handleEndTxn(commit)` transitions to `PrepareCommit` and persists; a wrong-epoch `handleEndTxn` → `ProducerFenced`; `handleEndTxn` on unknown txn → `InvalidTxnState`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** all five handlers (persist-then-respond; member scratch buffers, `std::less<>` transparent comparators — no per-call `std::string` alloc, the MC-A CG-7 rule).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(node): [TX-3] transaction handlers (init/addPartitions/addOffsets/txnOffsetCommit/endTxn)`. **Planka** TX-3 → Done; **live** push.

---

## TX-4 — Control markers + LSO + read_committed on the consume path

### Task 8: Commit/abort markers on participant partitions

**Files:** Modify `TransactionCoordinationService.*` (`doWork` marker drive) + `ClientGateway.*` (an internal `appendControlRecord(topicId, partition, bytes)` that appends via the leader replica, acks=All); test `libs/node/tests/integration/TxnMarkerTest.cpp`.

**Interfaces:**
- Consumes: `rune::transaction::ControlMarker::{writeMarkers, resolvePendingMarkers}` (`libs/transaction/include/rune/transaction/ControlMarker.h`) — both take `const std::function<void(const TopicPartition&, const std::vector<uint8_t>&)>& appendFn`; and `TransactionCoordinator::pendingMarkerTransactions()`.
- Produces on `ClientGateway`: `int64_t appendControlRecord(int32_t topicId, int32_t partition, std::span<const uint8_t> marker)` → `replica->append(marker, /*acksAll=*/true)` then `onLeaderLogAppended()`.

**Behavior:** on a `PrepareCommit`/`PrepareAbort` txn, `doWork` calls `ControlMarker::writeMarkers(coordinator, txnId, pid, epoch, commit, coordEpoch, nowNs, appendFn)` where `appendFn` routes to `gateway->appendControlRecord(tp.topicId, tp.partition, bytes)` for each participant partition **and** the `__consumer_offsets` partition for txn-staged offsets; then `completeTransaction`. On leader-transition, `ControlMarker::resolvePendingMarkers` re-drives any prepared-but-unmarked txns.

- [ ] **Step 1: Failing integration test** — begin+addPartitions(2 partitions)+produce txn data → `endTxn(commit)` → drive `doWork` → assert a **control batch** (`ControlMarker::isControlBatch`) lands at the tail of **each** participant partition and the txn reaches `CompleteCommit`. Abort variant writes abort markers.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `appendControlRecord` + the `writeMarkers`/`resolvePendingMarkers` drive in `doWork`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(node+gateway): [TX-4] commit/abort control markers via acks=All append`.

### Task 9: LSO + aborted-txn index + read_committed on push

**Files:** Modify `ClientGateway::pushSubscriptions` (~line 1401; isolation bound at ~1415) to apply an LSO cap + abort filter for `isolation=1`; wire a per-partition `ReadCommittedView`/`LsoTracker`/`AbortedTxnIndex`. Test `libs/gateway/tests/integration/ReadCommittedPushTest.cpp`.

**Interfaces:**
- Consumes: `rune::transaction::{LsoTracker::{onProduceBatch,onControlMarker,lso}, AbortedTxnIndex::{onAbortMarker,isAborted}, ReadCommittedView::{rebuild,read}, IsolationLevel::{ReadUncommitted,ReadCommitted}}` (`libs/transaction/include/rune/transaction/ReadCommittedView.h`).
- Behavior: for `sub.isolation == 1`, `bound = min(highWatermark(), lsoTracker.lso(highWatermark()))` and skip records whose batch base offset `abortedIndex.isAborted(pid, off)`, and skip control-marker batches. `isolation == 0` stays HW-bounded but also skips control markers (never deliver markers to apps).

- [ ] **Step 1: Failing integration test**
```cpp
TEST(ReadCommittedPush, OpenTxnHiddenBelowLso) {
    // append committed 0..4, then open-txn batch at 5 (no marker yet), HW=6
    // read_committed subscriber receives 0..4 only (LSO pins at 5); read_uncommitted sees through HW
}
TEST(ReadCommittedPush, AbortedRecordsFilteredControlMarkersNeverDelivered) {
    // txn pid=9 writes 5..7 then ABORT marker at 8; commit unrelated 9
    // read_committed delivers 0..4 and 9, never 5..7 or the marker batch
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the LSO cap + abort filter + control-marker skip in `pushSubscriptions`; maintain the per-partition trackers as markers/batches land (single-writer).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(gateway): [TX-4] read_committed LSO bound + aborted-txn filter + control-marker skip`.
- [ ] **Step 6:** Mirror the same filter on the `GroupConsumer` push path (group subs carry `isolation`); add a group read_committed test. Green → commit. **Planka** TX-4 → Done; **live** push.

---

## TX-5 — Client Tier-3 transactional producer + read_committed consumer

**Files:** Modify `rune-client-cpp/include/rune/client/producer/Producer.h` (+ `src`) for idempotent-by-default; create `TransactionalProducer` API; modify `group/GroupConsumer.h` (`isolation` config). Tests `rune-client-cpp/tests/TransactionalProducerTest.cpp` + `GroupConsumerReadCommittedTest.cpp`.

**Interfaces:**
```cpp
// idempotent-by-default: Producer stamps monotonically increasing baseSequence per (topic,partition)
// once it has a producerId; retries reuse the same sequence (dedup on the broker).
class TransactionalProducer {              // or Producer with a txn mode flag
public:
  void initTransactions();                 // InitProducerId(transactionalId) → store pid/epoch
  void beginTransaction();
  void send(std::string topic, Key, Value);// auto AddPartitionsToTxn on first send to a partition; stamps pid/epoch/seq
  void sendOffsetsToTransaction(const std::map<TopicPartition,int64_t>& offsets, std::string groupId);
  void commitTransaction();                // EndTxn(commit)
  void abortTransaction();                 // EndTxn(abort)
};
// GroupConsumer config gains:
struct GroupConfig { /*...existing...*/ IsolationLevel isolation = IsolationLevel::ReadUncommitted; };
```
Reuse the DR-BUG-1 wait-free COW snapshot discipline (`ProducerHub.h`) on the hot path; TSan-clean.

- [ ] **Step 1: Failing unit tests** (mock/loopback gateway): `initTransactions` sends InitProducerId and stores pid/epoch; a retried `send` reuses the same baseSequence (idempotent); `beginTransaction`+`send` to a new partition emits `AddPartitionsToTxn` once; `sendOffsetsToTransaction` emits AddOffsetsToTxn+TxnOffsetCommit; `commitTransaction` emits EndTxn(commit); `GroupConfig{isolation=ReadCommitted}` sends isolation=1 on subscribe.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the Tier-3 API + idempotent sequence numbering + the GroupConsumer isolation config.
- [ ] **Step 4: Run** → PASS (add a TSan run over the send/commit hot path — clean).
- [ ] **Step 5: Commit** — `feat(client): [TX-5] Tier-3 transactional producer + idempotent-by-default + read_committed GroupConsumer`.
- [ ] **Step 6: Integration test** vs the in-process/dockerized cluster: consume-transform-produce with `sendOffsetsToTransaction` + `commit` → downstream read_committed sees the output exactly once. Green → commit. **Planka** TX-5 → Done; **live** push.

---

## TX-6 — DR-E2E exactly-once scenarios

**Files:** Modify `dr-e2e/harness/driver.py` + `dr_client` (transactional-produce mode: `RUNE_TXN_ID`, `RUNE_TXN_COMMIT=0|1`; read_committed consume mode: `RUNE_ISOLATION=read_committed`); create `dr-e2e/tests/test_tx_atomicity.py`, `test_tx_read_committed.py`, `test_tx_fencing.py`, `test_tx_eos.py`.

**Interfaces:** consumes the existing `cluster`/`fresh_cluster` fixtures + `wait_data_plane_ready` + `faults.py` (kill/pause/netem) + `invariants.py` (extend with generation-aware EOS invariants from MC-A CG-6).

- [ ] **Step 1: Add txn + isolation modes to `dr_client`** — transactional produce (init→begin→produce→commit/abort) and a read_committed consumer that prints `{partition, offset, count}` for the parser. Add `harness/driver.py::run_txn(...)`.
- [ ] **Step 2: `test_tx_atomicity.py`** (matrix: partitions ∈ {2,4}, nodes ∈ {1,3,5}): multi-partition txn commit is **all-or-nothing** across **leader-kill** (commit marker survives ⇒ all visible; abort/none ⇒ none). Run → PASS.
- [ ] **Step 3: `test_tx_read_committed.py`**: differential — read_committed never exposes aborted or open-txn records; read_uncommitted does (≤HW). Under churn + netem-loss variant. Run → PASS.
- [ ] **Step 4: `test_tx_fencing.py`**: a zombie producer (old epoch) is **fenced** after a new `initProducerId`; a late commit from the fenced producer is rejected. Run → PASS.
- [ ] **Step 5: `test_tx_eos.py`**: consume-transform-produce **exactly-once** across a coordinator (leader) failover — assert **no dup, no loss** via the generation-aware single-owner + no-loss/no-dup invariants. Run → PASS.
- [ ] **Step 6:** **Smoke** subset (single + 3-node bring-up, init→begin→produce→commit→read_committed happy path, clean shutdown) as a fast pre-flight; wire it into the suite. Commit each file as it goes green — `test(dr-e2e): [TX-6] <scenario>`.
- [ ] **Step 7:** Run the full TX DR-E2E set on a **fresh `--no-cache` build** (guard the stale-binary trap); all green; every scenario asserts hot-path p99 < 100µs (latency gate). **Planka** TX-6 → Done; **live** push `dr-e2e/txn passed`.

---

## TX-7 — Benchmarks + full-area review

**Files:** Extend `rune-broker/benchmark/TransactionBenchmark.cpp` (already has `BM_Dedup_ValidateUpdate`, `BM_TxnBeginCommit_p50p99`, `BM_ControlMarker_*`, `BM_ReadCommitted_vs_Uncommitted`, `BM_InitProducerId_EpochBump`, `BM_AddOffsetsToTxn`) to full coverage; no product code unless a gate fails.

- [ ] **Step 1: Complete the benchmark matrix** (Release): idempotent dedup per-batch overhead (ns-class, **zero-alloc** asserted via the global `operator new` counter); txn begin/addPartitions/commit/abort p50/p99/p999; read_committed filter cost vs read_uncommitted baseline (per-record + per-batch); InitProducerId/epoch-bump throughput; coordinator replay time vs `__transaction_state` size; contention sweeps (producers × partitions × nodes). Fill any gaps in the existing file.
- [ ] **Step 2: Gate check** — any hot-path p99 > 100µs → STOP, root-cause, fix, re-measure < 100µs (or prove harness artifact). Assert zero per-op heap alloc on dedup + read_committed filter. Record numbers.
- [ ] **Step 3: Full-area Opus review** of all TX changes (protocol + coordination + node + gateway + client) against: single-writer, zero hot-path alloc/locks/blocking, idempotent dedup correctness (accept/dup/oos/fence), atomicity (markers durable acks=All), `__transaction_state` majority-durability + failover replay, LSO/aborted-index correctness (no aborted/open-txn leak, markers never delivered), fencing, coordinator-move REDIRECT, consume-transform-produce EOS. Fix findings to **CLEAN** (Sonnet first; escalate any Sonnet-introduced regression to Opus).
- [ ] **Step 4: Commit** — `bench(transaction): [TX-7] full EOS benchmark matrix + full-area review CLEAN`.
- [ ] **Step 5:** **Planka** TX-7 → Done; **live** push bench numbers (`kind=benchmark`).

---

## Self-review (author checklist — completed)

- **Spec coverage:** every spec §4 component maps to a task — protocol §4.1 → TX-1; idempotent dedup §4.2 → TX-2; `TransactionCoordinationService` + `__transaction_state` §4.3 → TX-3; control markers + LSO/read_committed §4.4 → TX-4; client Tier-3 §4.5 → TX-5; DR-E2E §4.6 → TX-6; benchmarks §9 → TX-7. Failover/recovery (§6) covered by TX-3 Task 5 integration + TX-4 marker resolve + TX-6 EOS test. Milestone-D seam (§7) = `coordinatorFor(txnId)` in TX-3 Task 6. Error/edge cases (§8) covered across TX-2/TX-3/TX-4/TX-6.
- **Placeholder scan:** no TBD/TODO; each code step carries real test/impl content or an exact interface signature grounded in the explored file:line surfaces.
- **Type consistency:** `ProducerStateManager::{validate,update}` param order matches the header; `ReplicatedTxnStateStore::{put,get,rebuildFromLog}` consistent across TX-3/TX-4; `ITxnRequestHandler` five `handle*` signatures match the `IGroupRequestHandler` shape and the gateway dispatch; `ControlMarker::{writeMarkers,resolvePendingMarkers}` `appendFn` signature matches; `LsoTracker::{onProduceBatch,onControlMarker,lso}` + `AbortedTxnIndex::{onAbortMarker,isAborted}` + `IsolationLevel` used consistently in TX-4; `TransactionalProducer::{initTransactions,beginTransaction,send,sendOffsetsToTransaction,commitTransaction,abortTransaction}` consistent across TX-5/TX-6.

## Open items to confirm during execution (from spec §11)
- `__transaction_state` as its own `ReplicatedTxnStateStore` (chosen) vs reusing `TransactionLog` over a replicated partition Log — Task 5 uses the former, reusing the `TransactionLog` record codec as the body.
- Aborted-txn index: in-memory rebuilt-from-markers (chosen, via `ReadCommittedView::rebuild`) vs persisted.
- `AddPartitionsToTxn` auto-sent by the client on first send (chosen) vs explicit.
- TxnOffsetCommit ↔ MC-A `ReplicatedOffsetStore`: txn-staged offsets become visible only on the commit marker written to the `__consumer_offsets` partition (Task 8 appends markers there too).
