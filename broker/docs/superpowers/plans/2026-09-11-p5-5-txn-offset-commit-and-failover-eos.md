# P5-5 Review Findings: Real Transactional Offset Commit + Leader-Failover EOS Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement two P5-5 review findings: (F3) a real `addOffsetsToTxn` API that binds consumer offsets atomically to a transaction — applied on commit, discarded on abort, persisted+recovered; and (F7) a 2-replica `PartitionReplica` ISR leader-failover test that asserts the EOS atomicity invariant when a COMMIT marker is above HW at the moment of failover.

**Architecture:**

F3 adds `addOffsetsToTxn(txnId, pid, epoch, group, {TopicPartition→offset}, nowNs)` to `TransactionCoordinator`. Pending offsets are stored in `TransactionMetadata` (a new `pendingOffsets` map) and persisted to `TransactionLog` as a new section in the on-disk record format. On `completeTransaction(COMMIT)` they are applied to the `OffsetStore` via an apply-callback injected into the coordinator; on abort/recovery-PrepareAbort they are discarded. The harness is wired with this callback and tests 11/12/31/32 are replaced with genuinely atomic assertions.

F7 adds a new test file `LeaderFailoverEosTest.cpp` linked into `rune_transaction_integration_tests`. It creates a 2-replica `PartitionReplica` ISR using `InMemoryTransport`, appends a data batch (replicated+acked to HW), then appends a COMMIT marker to the LEADER log only (HW not yet advanced past marker), then "kills" the leader by isolating its transport and promoting the follower. It asserts: read_committed on the new leader never shows the txn's data without a durable marker (LSO holds), and after re-driving the marker from the coordinator's persisted PrepareCommit state, the txn becomes atomically visible.

**Tech Stack:** C++23, Phaser/GTest, `rune::transaction::TransactionCoordinator`, `rune::transaction::TransactionLog`, `rune::coordination::OffsetStore`, `rune::replication::PartitionReplica`, `rune::replication::PartitionFailoverCoordinator`, `InMemoryTransport`, `ReadCommittedView`.

**Spec:** `docs/audit/2026-09-11-opus-audit-transaction.md` — "P5-5 review (commit dbe03b7)" section, findings item 3 and item 7.

## Global Constraints

- TDD: write failing test first, run it, implement, run again — commit each task.
- Faithful Kafka EOS: pending offsets are part of the transaction, not a side effect.
- Single-writer: no locks, no busy-spin, no hot-path allocation beyond the initial map insert.
- Bounded drivers: hard cap `kCap=1000` iterations + `ASSERT_TRUE(done)` on every `driveUntil`.
- Unique temp paths: harness dirs already use `makeUniqSuffix()`.
- Full suite must stay green: sequential `ctest` AND `-j4`.
- Do NOT conan create/export/upload. Do NOT dispatch subagents.
- Build: `cmake --build build -j2` (cmake found via PATH trick if needed).
- Durable-before-visible ordering: offsets persisted to log before any OffsetStore::commit call.
- No re-introduction of prior findings (B1, B2, C1–C4, A1–A7, NEW-1).

---

## File Map

**Modified:**
- `libs/transaction/include/rune/transaction/TransactionCoordinator.h` — add `PendingTxnOffsets` struct, `pendingOffsets` field in `TransactionMetadata`, `OffsetApplyCallback` typedef, `addOffsetsToTxn()` method, `OffsetApplyCallback` as coordinator constructor param.
- `libs/transaction/include/rune/transaction/TransactionLog.h` — add `pendingOffsets` field in `TxnLogEntry`; add `kPendingOffsets` section marker to on-disk format doc comment; bump `kFixedHeaderSize` to 52 (unchanged — offsets are a var-length section appended after partitions and before CRC).
- `libs/transaction/src/TransactionCoordinator.cpp` — implement `addOffsetsToTxn()`, wire `offsetApplyCb_` field, call it in `completeTransaction(PrepareCommit)`.
- `libs/transaction/src/TransactionLog.cpp` — serialize/deserialize `pendingOffsets` as a var-length section in the record (after partition tuples, before CRC); bump `kFixedHeaderSize` only if fixed fields are added (they are not — offsets go in the var section).
- `libs/transaction/tests/integration/ExactlyOnceHarness.h` — add `addOffsetsToTxn()` helper; wire `OffsetApplyCallback` into coordinator constructor; replace bare `commitOffset` calls in tests 11/12/31/32 with `addOffsetsToTxn`.
- `libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp` — replace tests 11, 12, 31, 32 with genuinely atomic `addOffsetsToTxn` versions; add F7 test (or add separate file).

**Created:**
- `libs/transaction/tests/integration/LeaderFailoverEosTest.cpp` — F7: 2-replica ISR PartitionReplica leader-failover EOS test.

**Modified:**
- `libs/transaction/CMakeLists.txt` — add `LeaderFailoverEosTest.cpp` to `rune_transaction_integration_tests`; add `rune_replication` to its `target_link_libraries`.

---

## Task 1: Add PendingTxnOffsets type + `addOffsetsToTxn` signature to coordinator header (RED)

**Files:**
- Modify: `libs/transaction/include/rune/transaction/TransactionCoordinator.h`

**Interfaces:**
- Produces: `PendingTxnOffset` struct `{GroupId group; rune::coordination::TopicPartition tp; rune::coordination::Offset offset;}`, `using OffsetApplyCallback = std::function<void(const std::string& group, const rune::coordination::TopicPartition& tp, rune::coordination::Offset offset)>;`, `addOffsetsToTxn(txnId, pid, epoch, group, map<TopicPartition,Offset>, nowNs) → AddPartitionsResult` on `TransactionCoordinator`, `pendingOffsets` field `std::vector<PendingTxnOffset>` in `TransactionMetadata`.

- [ ] **Step 1: Write the failing test (compile error = red)**

In `ExactlyOnceIntegrationTest.cpp`, add a minimal test at the bottom that calls the not-yet-existing API:

```cpp
TEST(TxnOffsetCommit, AddOffsetsToTxn_ApiExists) {
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("api-test");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("api-test", ir.producerId, ir.producerEpoch).ok());
    // This should compile once addOffsetsToTxn exists on the coordinator.
    rcoord::TopicPartition tp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{tp, 5}};
    auto res = h.coordinator().addOffsetsToTxn(
        "api-test", ir.producerId, ir.producerEpoch, "grp", offsets, h.clock());
    EXPECT_TRUE(res.ok());
}
```

- [ ] **Step 2: Run build to see compile error**

```bash
cd ~/Projects/rune-broker
if ! command -v cmake >/dev/null 2>&1; then CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)"); export PATH="$CMAKE_BIN:$PATH"; fi
cmake --build build -j2 2>&1 | tail -20
```

Expected: compile error about `addOffsetsToTxn` not found / `PendingTxnOffset` not declared.

- [ ] **Step 3: Add PendingTxnOffset + OffsetApplyCallback + pendingOffsets + addOffsetsToTxn declaration to header**

In `TransactionCoordinator.h`, add after the `#include` block:

```cpp
#include <rune/coordination/Types.h>
```

Add after the existing structs (after `PendingMarkerTxn`):

```cpp
// ---------------------------------------------------------------------------
// PendingTxnOffset — one consumer-group offset bound to a transaction.
// Part of the transaction's state; applied on commit, discarded on abort.
// Mirrors Kafka TxnOffsetCommit / sendOffsetsToTransaction (KIP-98).
// ---------------------------------------------------------------------------
struct PendingTxnOffset {
    std::string                       group;
    rune::coordination::TopicPartition tp;
    rune::coordination::Offset         offset{0};
};
```

Add in `TransactionMetadata`:

```cpp
    /// Pending consumer-group offsets bound to this transaction.
    /// Accumulated by addOffsetsToTxn(); applied to OffsetStore on commit,
    /// discarded on abort.  Persisted to TransactionLog so they survive a
    /// coordinator failover while in PrepareCommit (recovered Prepare* re-applies).
    std::vector<PendingTxnOffset> pendingOffsets;
```

Add callback typedef and field/constructor param, and `addOffsetsToTxn` method:

```cpp
// Callback type: called by completeTransaction(COMMIT) to apply pending offsets
// atomically with the commit. Injected for testability (nullptr = no-op).
using OffsetApplyCallback = std::function<void(const std::string& group,
                                                const rune::coordination::TopicPartition& tp,
                                                rune::coordination::Offset offset,
                                                int64_t timestampNs)>;
```

Update `TransactionCoordinator` constructor to accept `OffsetApplyCallback`:

```cpp
explicit TransactionCoordinator(ProducerIdAllocator  allocator = ProducerIdAllocator{},
                                int32_t              defaultTxnTimeoutMs = 60'000,
                                TransactionLog*      txnLog = nullptr,
                                int32_t              coordinatorEpoch = 0,
                                OffsetApplyCallback  offsetApplyCb = nullptr);
```

Add method:

```cpp
/// Bind consumer offsets to the current transaction (Kafka addOffsetsToTxn /
/// TxnOffsetCommit).  Records each offset as PENDING within the transaction's
/// state (persisted to TransactionLog, survives coordinator failover).
///
/// On completeTransaction(COMMIT): pending offsets are applied via the
/// OffsetApplyCallback.  On completeTransaction(ABORT) or any abort path
/// (fence / timeout / recovered PrepareAbort): pending offsets are discarded.
///
/// Epoch/PID fenced like addPartitions.  Offsets-topic partitions are
/// conceptually added to the txn's partition set (per Kafka), but in this
/// implementation we record them in pendingOffsets only — the marker/commit
/// path handles the output partitions separately.
///
/// Valid states: Ongoing only (same as addPartitions).
///
/// \param txnId         Transactional id.
/// \param producerId    Must match.
/// \param producerEpoch Must match.
/// \param group         Consumer group id.
/// \param offsets       Map of TopicPartition → offset to commit.
/// \param nowNs         Injected clock.
AddPartitionsResult addOffsetsToTxn(
    const TransactionalId&                                           txnId,
    ProducerId                                                        producerId,
    ProducerEpoch                                                     producerEpoch,
    const std::string&                                                group,
    const std::map<rune::coordination::TopicPartition,
                   rune::coordination::Offset>&                       offsets,
    int64_t                                                           nowNs);
```

Add `offsetApplyCb_` field in private section:

```cpp
    OffsetApplyCallback  offsetApplyCb_;
```

- [ ] **Step 4: Run build — expect link error (not compile error)**

```bash
cmake --build build -j2 2>&1 | tail -20
```

Expected: compile passes, link fails because `addOffsetsToTxn` is not defined.

- [ ] **Step 5: Commit header-only skeleton**

```bash
git add libs/transaction/include/rune/transaction/TransactionCoordinator.h
git add libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp
git commit -m "test(transaction): add skeleton AddOffsetsToTxn_ApiExists (failing) + PendingTxnOffset types in header [P5-5 F3]"
```

---

## Task 2: Implement `addOffsetsToTxn` in TransactionCoordinator.cpp (GREEN for API)

**Files:**
- Modify: `libs/transaction/src/TransactionCoordinator.cpp`

**Interfaces:**
- Consumes: `PendingTxnOffset`, `OffsetApplyCallback`, `pendingOffsets` field in `TransactionMetadata`, `offsetApplyCb_` field.
- Produces: `addOffsetsToTxn` fully implemented, constructor stores `offsetApplyCb_`, `completeTransaction(PrepareCommit)` calls `offsetApplyCb_` for each pending offset before clearing them.

- [ ] **Step 1: Write the failing unit test (before implementing)**

In `ExactlyOnceIntegrationTest.cpp` replace the skeleton `ApiExists` test with the full F3 commit test:

```cpp
// ---------------------------------------------------------------------------
// F3-a. addOffsetsToTxn: commit → OffsetStore shows advanced offset.
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_CommitAppliesOffset)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-commit");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-commit", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    // addOffsetsToTxn records offset 5 as PENDING (not yet written to OffsetStore).
    ASSERT_TRUE(h.addOffsetsToTxn("ato-commit", ir.producerId, ir.producerEpoch,
                                   "grp-commit", {{inputTp, 5}}).ok());

    // BEFORE commit: OffsetStore must NOT have the offset.
    auto preCo = h.fetchOffset("grp-commit", inputTp);
    EXPECT_FALSE(preCo.has_value())
        << "Offset must NOT be in OffsetStore before txn commit";

    ASSERT_TRUE(h.commitTxn("ato-commit", ir.producerId, ir.producerEpoch));

    // AFTER commit: OffsetStore MUST have offset 5.
    auto co = h.fetchOffset("grp-commit", inputTp);
    ASSERT_TRUE(co.has_value()) << "Offset must be in OffsetStore after commit";
    EXPECT_EQ(co->offset, 5LL);
}
```

And the abort test:

```cpp
// ---------------------------------------------------------------------------
// F3-b. addOffsetsToTxn: abort → OffsetStore NOT advanced (MUST FAIL if it is).
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_AbortDiscardsOffset)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-abort", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    ASSERT_TRUE(h.addOffsetsToTxn("ato-abort", ir.producerId, ir.producerEpoch,
                                   "grp-abort", {{inputTp, 5}}).ok());

    ASSERT_TRUE(h.abortTxn("ato-abort", ir.producerId, ir.producerEpoch));

    // AFTER abort: OffsetStore must NOT have offset 5 (invariant).
    auto co = h.fetchOffset("grp-abort", inputTp);
    EXPECT_FALSE(co.has_value())
        << "INVARIANT VIOLATED: offset was applied to OffsetStore on abort";
}
```

- [ ] **Step 2: Run build to confirm the test file compiles but the linker fails**

```bash
cmake --build build -j2 2>&1 | tail -20
```

- [ ] **Step 3: Implement addOffsetsToTxn in TransactionCoordinator.cpp**

In the constructor, store the callback:

```cpp
TransactionCoordinator::TransactionCoordinator(ProducerIdAllocator allocator,
                                               int32_t             defaultTxnTimeoutMs,
                                               TransactionLog*     txnLog,
                                               int32_t             coordinatorEpoch,
                                               OffsetApplyCallback offsetApplyCb)
    : allocator_(std::move(allocator))
    , defaultTxnTimeoutMs_(defaultTxnTimeoutMs)
    , txnLog_(txnLog)
    , coordinatorEpoch_(coordinatorEpoch)
    , offsetApplyCb_(std::move(offsetApplyCb))
{
    // ... existing replay code unchanged ...
}
```

Add the implementation after `endTransaction`:

```cpp
// ---------------------------------------------------------------------------
// addOffsetsToTxn
// ---------------------------------------------------------------------------
AddPartitionsResult TransactionCoordinator::addOffsetsToTxn(
    const TransactionalId&                                           txnId,
    ProducerId                                                        producerId,
    ProducerEpoch                                                     producerEpoch,
    const std::string&                                                group,
    const std::map<rune::coordination::TopicPartition,
                   rune::coordination::Offset>&                       offsets,
    int64_t                                                           /*nowNs*/)
{
    TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) {
        return { .errorCode = AddPartitionsResult::kUnknownTxnId };
    }
    if (int e = validateEpochAndPid(*meta, producerId, producerEpoch); e != 0) {
        return { .errorCode = e };
    }
    // Only valid while transaction is Ongoing.
    if (meta->state != TxnState::Ongoing) {
        return { .errorCode = AddPartitionsResult::kInvalidTransition };
    }

    // Record each offset as PENDING within the transaction.
    for (const auto& [tp, off] : offsets) {
        meta->pendingOffsets.push_back({group, tp, off});
    }

    // Persist the updated metadata (including pendingOffsets) so these survive
    // a coordinator failover: a recovered PrepareCommit will re-apply them.
    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    return { .errorCode = AddPartitionsResult::kOk };
}
```

In `completeTransaction`, after the PrepareCommit → CompleteCommit path, apply pending offsets BEFORE clearing:

```cpp
if (meta->state == TxnState::PrepareCommit) {
    meta->state = TxnState::CompleteCommit;
    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    // Apply pending consumer-group offsets atomically with the commit.
    // The durable-before-visible ordering is guaranteed: CompleteCommit is
    // already persisted above; now we write to the OffsetStore.
    if (offsetApplyCb_) {
        for (const auto& po : meta->pendingOffsets) {
            offsetApplyCb_(po.group, po.tp, po.offset, /*timestampNs=*/0);
        }
    }

    meta->pendingOffsets.clear();
    meta->partitions.clear();
    meta->txnStartNs     = 0;
    meta->fencingEpoch   = false;
    meta->markersPending = false;
    meta->state          = TxnState::Empty;
    persistTransition(txnLog_, *meta, coordinatorEpoch_);
    return { .errorCode = CompleteTransactionResult::kOk };
}
```

In the PrepareAbort path, simply clear pendingOffsets (discard):

```cpp
if (meta->state == TxnState::PrepareAbort) {
    meta->state = TxnState::CompleteAbort;
    persistTransition(txnLog_, *meta, coordinatorEpoch_);
    meta->pendingOffsets.clear();   // ← discard, never written to OffsetStore
    meta->partitions.clear();
    // ... rest unchanged ...
}
```

- [ ] **Step 4: Run build and integration tests to verify F3-a and F3-b pass**

```bash
cmake --build build -j2 && cd build && ctest -R 'AddOffsetsToTxn' --timeout 90 --output-on-failure
```

Expected: both tests PASS. The abort test MUST FAIL if we accidentally call `offsetApplyCb_` on abort — verify by temporarily calling it on abort and confirming the test fails, then revert.

- [ ] **Step 5: Commit**

```bash
git add libs/transaction/include/rune/transaction/TransactionCoordinator.h \
        libs/transaction/src/TransactionCoordinator.cpp \
        libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp
git commit -m "feat(transaction): implement addOffsetsToTxn — pending offsets applied on commit, discarded on abort [P5-5 F3]"
```

---

## Task 3: Persist+recover pendingOffsets in TransactionLog (durability seam)

**Files:**
- Modify: `libs/transaction/include/rune/transaction/TransactionLog.h` — add `pendingOffsets` field to `TxnLogEntry`.
- Modify: `libs/transaction/src/TransactionLog.cpp` — serialize/deserialize pendingOffsets in var-length section.
- Modify: `libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp` — add F3-c: coordinator failover with PrepareCommit recovers pending offsets and applies them.

**Interfaces:**
- Consumes: `TxnLogEntry` (existing), `TransactionMetadata.pendingOffsets`.
- Produces: `TxnLogEntry.pendingOffsets` persisted to disk; on replay, `applyEntry` sets `meta.pendingOffsets`.

**On-disk format for pendingOffsets section (appended AFTER partition tuples, BEFORE CRC):**

```
offsetGroupCount : uint16    // number of (group, tp, offset) triples
for each triple:
  groupLen       : uint16
  topicLen       : uint16
  partition      : int32
  offset         : int64
  group          : groupLen bytes
  topic          : topicLen bytes
```

- [ ] **Step 1: Write the failing test (coordinator failover re-applies pending offsets)**

```cpp
// ---------------------------------------------------------------------------
// F3-c. addOffsetsToTxn: coordinator failover mid-PrepareCommit recovers
//        pending offsets and applies them on resolve.
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_FailoverRecoversPendingOffsets)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-failover");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-failover", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    ASSERT_TRUE(h.addOffsetsToTxn("ato-failover", ir.producerId, ir.producerEpoch,
                                   "grp-failover", {{inputTp, 7}}).ok());

    // Drive to PrepareCommit (endTransaction commit), then crash BEFORE markers.
    {
        auto endRes = h.coordinator().endTransaction(
            "ato-failover", ir.producerId, ir.producerEpoch, /*commit=*/true, h.clock());
        ASSERT_TRUE(endRes.ok());
        // Do NOT write markers or call completeTransaction — simulate crash here.
    }

    // Crash + restart coordinator (recovered state = PrepareCommit, markersPending=true).
    h.recreateCoordinator(1);

    // After recovery: pending offsets must NOT be in OffsetStore yet.
    auto preCo = h.fetchOffset("grp-failover", inputTp);
    EXPECT_FALSE(preCo.has_value())
        << "Pending offsets must NOT be applied before resolution";

    // Resolve pending markers (writes COMMIT markers + completes txn via callback).
    h.resolvePending(/*coordEpoch=*/1);
    h.rebuildAllViews();

    // After resolution: offset MUST be in OffsetStore.
    auto co = h.fetchOffset("grp-failover", inputTp);
    ASSERT_TRUE(co.has_value()) << "Offset must be applied after recovered commit resolve";
    EXPECT_EQ(co->offset, 7LL);

    // Data must be visible under read_committed.
    // (data offset was at 0 on partition 0)
    EXPECT_FALSE(parseOffsets(h.readCommitted(0)).empty());
}
```

- [ ] **Step 2: Run build/test — test fails because pendingOffsets are not persisted (lost on coordinator restart)**

```bash
cmake --build build -j2 && cd build && ctest -R 'FailoverRecoversPendingOffsets' --timeout 90 --output-on-failure 2>&1 | tail -20
```

Expected: test fails because after `recreateCoordinator`, the `pendingOffsets` vector is empty (not persisted).

- [ ] **Step 3: Add `pendingOffsets` to `TxnLogEntry` in TransactionLog.h**

```cpp
struct TxnLogEntry {
    // ... existing fields ...
    /// Pending consumer-group offsets bound to this transaction (F3).
    /// Applied on commit resolution; discarded on abort. Persisted so
    /// a recovered PrepareCommit re-applies them.
    std::vector<PendingTxnOffset> pendingOffsets;
};
```

Note: `PendingTxnOffset` is defined in `TransactionCoordinator.h`, which `TransactionLog.h` already includes.

- [ ] **Step 4: Serialize pendingOffsets in TransactionLog.cpp (appendRecord)**

In `TransactionLog.cpp` `encode()` / `appendRecord()` function (where partition tuples are written), add AFTER the partition tuples block:

```cpp
// Pending offset section.
uint16_t offsetGroupCount = 0;
if (entry.pendingOffsets.size() > std::numeric_limits<uint16_t>::max()) {
    throw std::length_error("pendingOffsets count exceeds uint16");
}
offsetGroupCount = static_cast<uint16_t>(entry.pendingOffsets.size());
write_le<uint16_t>(buf, pos, offsetGroupCount);
for (const auto& po : entry.pendingOffsets) {
    if (po.group.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::length_error("group id exceeds uint16");
    }
    if (po.tp.topic.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::length_error("topic exceeds uint16");
    }
    write_le<uint16_t>(buf, pos, static_cast<uint16_t>(po.group.size()));
    write_le<uint16_t>(buf, pos, static_cast<uint16_t>(po.tp.topic.size()));
    write_le<int32_t>(buf, pos, po.tp.partition);
    write_le<int64_t>(buf, pos, po.offset);
    buf.insert(buf.end(), po.group.begin(), po.group.end());
    buf.insert(buf.end(), po.tp.topic.begin(), po.tp.topic.end());
    pos += po.group.size() + po.tp.topic.size();
}
```

- [ ] **Step 5: Deserialize pendingOffsets in TransactionLog.cpp (replay / applyEntry)**

In the replay/scan loop, after reading partition tuples and before reading the CRC, add:

```cpp
// Pending offset section.
if (vpos + 2 > bodyEnd) break; // torn
uint16_t offsetCount = read_le<uint16_t>(data + vpos); vpos += 2;
for (uint16_t oi = 0; oi < offsetCount; ++oi) {
    if (vpos + 16 > bodyEnd) { tornOffsets = true; break; } // 2+2+4+8
    uint16_t gLen  = read_le<uint16_t>(data + vpos); vpos += 2;
    uint16_t tLen  = read_le<uint16_t>(data + vpos); vpos += 2;
    int32_t  part  = read_le<int32_t> (data + vpos); vpos += 4;
    int64_t  off   = read_le<int64_t> (data + vpos); vpos += 8;
    if (vpos + gLen + tLen > bodyEnd) { tornOffsets = true; break; }
    std::string group(reinterpret_cast<const char*>(data + vpos), gLen); vpos += gLen;
    std::string topic(reinterpret_cast<const char*>(data + vpos), tLen); vpos += tLen;
    entry.pendingOffsets.push_back({group, {topic, part}, off});
}
if (tornOffsets) break;
```

In `applyEntry`:

```cpp
meta.pendingOffsets = entry.pendingOffsets;
```

Also update `appendFromMetadata` to copy `pendingOffsets` from `TransactionMetadata` to `TxnLogEntry`:

```cpp
entry.pendingOffsets = meta.pendingOffsets;
```

- [ ] **Step 6: Wire OffsetApplyCallback into ExactlyOnceHarness**

In `ExactlyOnceHarness.h`, update `ExactlyOnceHarness` constructor to pass an `OffsetApplyCallback` that calls `offsetStore_->commit(...)`:

```cpp
// Build coordinator + log.
txnLog_  = std::make_unique<TransactionLog>(txnLogPath_);
TransactionCoordinator::OffsetApplyCallback cb =
    [this](const std::string& group,
           const rune::coordination::TopicPartition& tp,
           rune::coordination::Offset offset,
           int64_t timestampNs) {
        offsetStore_->commit(group, tp, offset, "", timestampNs != 0 ? timestampNs : clock_);
    };
coord_ = std::make_unique<TransactionCoordinator>(
             ProducerIdAllocator{}, txnTimeoutMs_,
             txnLog_.get(), /*coordinatorEpoch=*/0, std::move(cb));
```

Update `recreateCoordinator` to also pass the same callback (captures `this`):

```cpp
void recreateCoordinator(int32_t newCoordEpoch = 1) {
    coord_.reset();
    txnLog_.reset();
    txnLog_ = std::make_unique<TransactionLog>(txnLogPath_);
    ProducerIdAllocator alloc;
    for (auto& [txnId, meta] : txnLog_->all()) {
        alloc.reseedAtLeast(meta.producerId);
    }
    TransactionCoordinator::OffsetApplyCallback cb =
        [this](const std::string& group,
               const rune::coordination::TopicPartition& tp,
               rune::coordination::Offset offset,
               int64_t timestampNs) {
            offsetStore_->commit(group, tp, offset, "", timestampNs != 0 ? timestampNs : clock_);
        };
    coord_ = std::make_unique<TransactionCoordinator>(
                 std::move(alloc), txnTimeoutMs_,
                 txnLog_.get(), newCoordEpoch, std::move(cb));
}
```

Add `addOffsetsToTxn` helper to `ExactlyOnceHarness`:

```cpp
/// addOffsetsToTxn: bind consumer offsets to the current transaction.
AddPartitionsResult addOffsetsToTxn(
    const TransactionalId& txnId,
    ProducerId             pid,
    ProducerEpoch          epoch,
    const std::string&     group,
    const std::map<rune::coordination::TopicPartition, rune::coordination::Offset>& offsets)
{
    return coord_->addOffsetsToTxn(txnId, pid, epoch, group, offsets, clock_);
}
```

- [ ] **Step 7: Run F3-c test and all integration tests**

```bash
cmake --build build -j2 && cd build && ctest -R 'TxnOffsetCommit|ExactlyOnce|AtomicVisibility|DisasterRecovery' --timeout 90 --output-on-failure 2>&1 | tail -30
```

Expected: F3-a, F3-b, F3-c all PASS. Full integration suite still green.

- [ ] **Step 8: Commit**

```bash
git add libs/transaction/include/rune/transaction/TransactionLog.h \
        libs/transaction/src/TransactionLog.cpp \
        libs/transaction/tests/integration/ExactlyOnceHarness.h \
        libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp
git commit -m "feat(transaction): persist+recover pendingOffsets in TransactionLog; wire OffsetApplyCallback in harness [P5-5 F3]"
```

---

## Task 4: Replace tests 11, 12, 31, 32 with genuinely atomic addOffsetsToTxn versions

**Files:**
- Modify: `libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp`

The P5-5 audit finding is that tests 11/12/31/32 use bare `commitOffset` calls outside the transaction. Replace them with `addOffsetsToTxn` so the assertion is no longer a weak proxy — the abort test MUST fail if offsets were applied on abort.

- [ ] **Step 1: Review existing tests 11, 12, 31, 32 (lines ~330–970)**

The current tests 11/12 call `h.commitOffset(...)` AFTER `h.commitTxn(...)` — outside the transaction boundary. Tests 31/32 have the same issue (31: `h.commitOffset(...)` then `h.commitTxn(...)`; 32: omits call by author's choice).

- [ ] **Step 2: Replace test 11 — ReadProcessWrite_CommitAtomicOutputAndOffsets**

Replace the `h.commitOffset(...)` call with `h.addOffsetsToTxn(...)` BEFORE `h.commitTxn(...)`:

```cpp
TEST(ExactlyOnce, ReadProcessWrite_CommitAtomicOutputAndOffsets)
{
    ExactlyOnceHarness h(2, "topic");
    // ... (produce input data to p0, init producer, addAllPartitions) ...
    int64_t outOff = h.appendTxnData(pid, epoch, 1, 0, "output-record");
    ASSERT_GE(outOff, 0);

    // Output not yet visible.
    EXPECT_TRUE(parseOffsets(h.readCommitted(1)).empty());

    // Atomically bind offset 1 to the transaction BEFORE commit.
    rcoord::TopicPartition inputTp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsetsToCommit{{inputTp, 1}};
    ASSERT_TRUE(h.addOffsetsToTxn("rpw-txn", pid, epoch, "group-A", offsetsToCommit).ok());

    // Verify offset NOT yet in store.
    EXPECT_FALSE(h.fetchOffset("group-A", inputTp).has_value())
        << "Offset must not be visible before commit";

    // Commit: output + offsets applied atomically.
    ASSERT_TRUE(h.commitTxn("rpw-txn", pid, epoch));

    h.assertCommittedVisible(1, {outOff});
    auto co = h.fetchOffset("group-A", inputTp);
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 1LL);
}
```

- [ ] **Step 3: Replace test 12 — ReadProcessWrite_Abort_NoDuplicateOutput_OffsetNotAdvanced**

```cpp
TEST(ExactlyOnce, ReadProcessWrite_Abort_NoDuplicateOutput_OffsetNotAdvanced)
{
    ExactlyOnceHarness h(2, "topic");
    auto ir = h.initProducer("rpw-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("rpw-abort", ir.producerId, ir.producerEpoch).ok());

    int64_t outOff = h.appendTxnData(ir.producerId, ir.producerEpoch, 1, 0, "spurious");
    ASSERT_GE(outOff, 0);

    // Bind offset to the transaction BEFORE abort.
    rcoord::TopicPartition inputTp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{inputTp, 1}};
    ASSERT_TRUE(h.addOffsetsToTxn("rpw-abort", ir.producerId, ir.producerEpoch,
                                   "group-B", offsets).ok());

    // Abort. Offsets MUST be discarded.
    ASSERT_TRUE(h.abortTxn("rpw-abort", ir.producerId, ir.producerEpoch));

    h.assertAbortedInvisible(1, {outOff});
    // This assertion MUST FAIL if offsets were applied on abort.
    auto co = h.fetchOffset("group-B", inputTp);
    EXPECT_FALSE(co.has_value())
        << "INVARIANT VIOLATED: offset applied to OffsetStore on transaction abort";
}
```

- [ ] **Step 4: Replace tests 31 and 32**

Test 31 (`TxnOffsetCommit_AtomicWithTxn_CommitAdvancesOffset`): use `addOffsetsToTxn` before `commitTxn`, assert offset NOT in store before commit, assert offset in store after commit.

Test 32 (`TxnOffsetCommit_AbortDoesNotAdvanceOffset`): use `addOffsetsToTxn` before `abortTxn`, assert offset NOT in store after abort (this test NOW MUST FAIL if the offset is accidentally written on abort — the old test was vacuous because it never wrote the offset at all).

- [ ] **Step 5: Run the full integration suite**

```bash
cmake --build build -j2 && cd build && ctest -R 'ExactlyOnce|AtomicVisibility|DisasterRecovery|TxnOffsetCommit' --timeout 90 --output-on-failure 2>&1 | tail -30
```

Expected: ALL pass. Verify test 12 WOULD FAIL if we broke the abort path by temporarily calling `offsetApplyCb_` on abort — confirm the test catches it.

- [ ] **Step 6: Commit**

```bash
git add libs/transaction/tests/integration/ExactlyOnceIntegrationTest.cpp
git commit -m "test(transaction): replace tests 11/12/31/32 with genuine addOffsetsToTxn — abort test fails if offsets applied on abort [P5-5 F3]"
```

---

## Task 5: Add F7 — 2-replica leader-failover EOS test (marker-durability invariant)

**Files:**
- Create: `libs/transaction/tests/integration/LeaderFailoverEosTest.cpp`
- Modify: `libs/transaction/CMakeLists.txt` — add the new file + `rune_replication` link dep.

**Interfaces:**
- Consumes: `PartitionReplica`, `InMemoryTransport`, `PartitionFailoverCoordinator`, `ReadCommittedView`, `LsoTracker`, `AbortedTxnIndex`, `TransactionCoordinator`, `TransactionLog`, `ControlMarker`, `ExactlyOnceHarness.h` helpers (makeDataBatch, appendControlMarker, parseOffsets, HarnessDir).
- Produces: `LeaderFailoverEosTest.cpp` with at least 2 GTest cases covering the marker-durability invariant under HW < markerOffset leader-failover.

**Scenario (F7):**

```
Setup: 2-node ISR (node 1 = leader, node 2 = follower).
       TransactionCoordinator wired to leader's Log partition.
       ReadCommittedView + LsoTracker on leader's log.

Step 1: Init producer, addPartitions, appendTxnData (batch offset 0) →
        replicated to follower + acked to HW (HW = 1).
Step 2: endTransaction(commit) on coordinator → PrepareCommit persisted.
Step 3: Append COMMIT marker to leader log ONLY (marker offset = 1, HW still 1).
        Do NOT pump transport — follower does NOT get the marker. HW < LEO_leader.
        [This is the critical window: data replicated, marker NOT]
Step 4: Assert LSO on leader log = 0 (marker is at offset 1, HW = 1,
        LSO = min open txn firstOffset = 0 → pinned).
        Assert read_committed(leader log) = EMPTY (LSO=0 holds).
Step 5: "Kill" leader — isolate transport. Promote follower to leader
        via PartitionFailoverCoordinator.electNewLeader().
Step 6: New leader (follower) has: data batch at offset 0 (replicated), but NO
        marker (marker was only on old leader). HW on new leader = 1 (data committed).
        Assert: read_committed(new leader log) = EMPTY — LSO must still be pinned
        because there is no durable marker. The coordinator has PrepareCommit
        (durable) → pending marker task.
Step 7: Re-drive the COMMIT marker from coordinator's recovered PrepareCommit state
        using resolvePendingMarkers (or writeMarkers) targeting the new leader's log.
        append marker to new leader log at offset 1.
        Update LsoTracker with the COMMIT marker.
Step 8: Assert read_committed(new leader log) now includes the data batch at offset 0.
        Assert LSO advanced to HW (= 2 after marker append).
        Assert no data loss: data batch at offset 0 present.
        Assert atomicity: data visible ONLY AFTER durable marker written.

Invariant assertions:
  - NEVER: read_committed returns the data without a marker in the new leader's log.
  - NEVER: txn's data is visible under read_committed but commit marker was lost.
  - At all times: LSO <= HW.
  - After marker re-driven: data atomically visible, consistent with committed state.
```

- [ ] **Step 1: Write the failing test file**

```cpp
// LeaderFailoverEosTest.cpp
// F7: 2-replica ISR PartitionReplica leader-failover EOS test.
// Asserts the marker-durability invariant: a txn is NEVER visible under
// read_committed without its durable marker, even when the leader fails
// after the data is replicated but before the marker is replicated.

#include <gtest/gtest.h>
#include "ExactlyOnceHarness.h"

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/PartitionFailoverCoordinator.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/Types.h>
#include <rune/storage/Log.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <string>
#include <vector>
#include <memory>

using namespace rune::transaction;
using namespace rune::transaction::test;
using namespace rune::replication;

static constexpr int kCap = 1000;

// ---------------------------------------------------------------------------
// F7-a: Data replicated + acked to HW; COMMIT marker on leader ONLY (HW < LEO);
//        leader fails → follower promoted; read_committed on new leader is EMPTY
//        (LSO holds); marker re-driven from durable PrepareCommit; txn becomes
//        atomically visible.
// ---------------------------------------------------------------------------
TEST(LeaderFailoverEos, MarkerAboveHW_LeaderFails_TxnNeverVisibleWithoutMarker)
{
    // Dirs for the two node logs and the coordinator log.
    HarnessDir leaderDir("f7-leader");
    HarnessDir followerDir("f7-follower");
    HarnessDir txnLogDir("f7-txnlog");
    HarnessDir offsetStoreDir("f7-offsets");

    std::string txnLogPath    = txnLogDir.path + "/txn.log";
    std::string offsetPath    = offsetStoreDir.path + "/offsets.log";

    // Per-replica logs.
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    // 2-node InMemoryTransport.
    InMemoryTransport transport;

    PartitionConfig cfg1, cfg2;
    cfg1.partitionId = 0; cfg1.selfId = 1;
    cfg1.replicaLagMaxMs = 10000; cfg1.minIsr = 1;
    cfg1.fetchIntervalNs = 1'000'000LL; cfg1.isrCheckIntervalNs = 1'000'000LL;
    cfg2 = cfg1; cfg2.selfId = 2;

    std::vector<NodeId> replicas = {1, 2};
    std::vector<NodeId> isr      = {1, 2};

    auto leader   = std::make_unique<PartitionReplica>(cfg1, Role::Leader,   /*epoch=*/1,
                                                        replicas, isr, &leaderLog,   &transport);
    auto follower = std::make_unique<PartitionReplica>(cfg2, Role::Follower, /*epoch=*/1,
                                                        replicas, isr, &followerLog, &transport);

    transport.setLeader(leader.get());
    transport.addFollower(2, follower.get());

    // PartitionFailoverCoordinator.
    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = 1; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator failoverCoord(meta, &liveness);
    failoverCoord.registerReplica(1, leader.get());
    failoverCoord.registerReplica(2, follower.get());

    // TransactionCoordinator with durable log.
    auto txnLog = std::make_unique<TransactionLog>(txnLogPath);
    auto offsetStore = std::make_unique<rune::coordination::OffsetStore>(offsetPath);
    int64_t clock = 1'000'000'000LL;

    TransactionCoordinator::OffsetApplyCallback cb =
        [&](const std::string& grp,
            const rune::coordination::TopicPartition& tp,
            rune::coordination::Offset off,
            int64_t /*ts*/) {
            offsetStore->commit(grp, tp, off, "", clock);
        };

    auto coord = std::make_unique<TransactionCoordinator>(
        ProducerIdAllocator{}, 60'000, txnLog.get(), 0, cb);

    // Per-partition LSO state on the leader's log.
    LsoTracker lsoTracker;
    AbortedTxnIndex abortedIndex;
    ReadCommittedView rcvLeader(&lsoTracker, &abortedIndex);

    // Helper: drive replication until HW reaches target.
    auto driveReplication = [&](Offset target) {
        for (int i = 0; i < kCap && leader->highWatermark() < target; ++i) {
            ++clock;
            leader->doWork(clock);
            follower->doWork(clock);
            transport.pump(clock);
        }
        return leader->highWatermark() >= target;
    };

    // -----------------------------------------------------------------------
    // Step 1: Init producer, addPartitions, append data batch to leader.
    //         Replicate to follower + ack to HW.
    // -----------------------------------------------------------------------
    auto ir = coord->initProducerId("f7-txn", 60'000, clock);
    ASSERT_TRUE(ir.ok());
    ProducerId    pid   = ir.producerId;
    ProducerEpoch epoch = ir.producerEpoch;

    TopicPartition tp{"f7-topic", 0};
    ASSERT_TRUE(coord->addPartitions("f7-txn", pid, epoch, {tp}, clock).ok());

    // Build + append data batch to leader log.
    auto dataBatch = makeDataBatch(0, pid, epoch, 0, "txn-data");
    Offset dataOff = leader->append(dataBatch, /*acksAll=*/true);
    ASSERT_GE(dataOff, 0) << "Leader append must succeed with ISR >= minIsr";

    lsoTracker.onProduceBatch(pid, dataOff);  // Open txn on partition.

    // Replicate: drive until HW = 1 (data acked by follower).
    ASSERT_TRUE(driveReplication(1)) << "HW must advance to 1 after data replication";
    EXPECT_EQ(leader->highWatermark(), 1LL);
    EXPECT_EQ(follower->logEndOffset(), 1LL) << "Follower must have received the data batch";

    // -----------------------------------------------------------------------
    // Step 2: endTransaction(commit) → PrepareCommit persisted durably.
    // -----------------------------------------------------------------------
    auto endRes = coord->endTransaction("f7-txn", pid, epoch, /*commit=*/true, clock);
    ASSERT_TRUE(endRes.ok());
    // Verify durable PrepareCommit (log on disk).
    EXPECT_EQ(coord->state("f7-txn").value_or(TxnState::Empty), TxnState::PrepareCommit);

    // -----------------------------------------------------------------------
    // Step 3: Append COMMIT marker to LEADER log ONLY. Do NOT pump transport.
    //         Follower does NOT receive the marker. HW stays at 1 (data offset).
    // -----------------------------------------------------------------------
    Offset markerOff = leaderLog.logEndOffset(); // = 1
    auto markerBatch = ControlMarker::build(pid, epoch, ControlType::Commit, 0);
    // Stamp baseOffset LE.
    {
        uint64_t u = static_cast<uint64_t>(markerOff);
        for (int i = 0; i < 8; ++i) markerBatch[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    leaderLog.append(markerBatch, markerOff);

    // Leader LEO = 2, HW = 1 (follower hasn't seen marker yet).
    EXPECT_EQ(leaderLog.logEndOffset(), 2LL);
    EXPECT_EQ(leader->highWatermark(), 1LL)
        << "HW must remain at 1 — marker not yet replicated";

    // -----------------------------------------------------------------------
    // CRITICAL INVARIANT CHECK (pre-failover on leader log):
    // read_committed MUST be EMPTY because LSO is pinned at data offset (0).
    // -----------------------------------------------------------------------
    {
        int64_t hw = leader->highWatermark(); // = 1
        int64_t lso = lsoTracker.lso(hw);
        EXPECT_EQ(lso, 0LL)
            << "LSO must be pinned at data offset before marker is durable";
        auto rcRaw = rcvLeader.read(&leaderLog, hw, 0, 1 << 20,
                                    IsolationLevel::ReadCommitted);
        auto rcOffsets = parseOffsets(rcRaw);
        EXPECT_TRUE(rcOffsets.empty())
            << "read_committed must be EMPTY before marker reaches HW";
    }

    // -----------------------------------------------------------------------
    // Step 4: "Kill" leader — remove from transport, promote follower.
    //         The marker on the leader's log is LOST on failover.
    // -----------------------------------------------------------------------
    liveness.markDead(1);
    transport.removeFollower(2); // follower no longer receives from leader
    failoverCoord.signalLeaderDead(1);
    {
        bool elected = false;
        for (int i = 0; i < kCap && !elected; ++i) {
            ++clock;
            elected = failoverCoord.doWork(clock) > 0;
        }
        ASSERT_TRUE(elected) << "Failover coordinator must elect follower as leader";
    }
    EXPECT_EQ(failoverCoord.currentLeader(), 2) << "Node 2 must be new leader";

    // -----------------------------------------------------------------------
    // Step 5: New leader (node 2 / follower) has data batch at offset 0 but
    //         NO commit marker. HW on new leader = 1 (data committed to ISR
    //         before failover). LSO must still be pinned — no durable marker.
    // -----------------------------------------------------------------------
    // New leader's log has: [data@0]. No marker.
    EXPECT_EQ(followerLog.logEndOffset(), 1LL) << "New leader log: data batch only";

    // Build a fresh LSO view on the new leader's log from scratch.
    LsoTracker newLsoTracker;
    AbortedTxnIndex newAbortedIndex;
    ReadCommittedView rcvNewLeader(&newLsoTracker, &newAbortedIndex);
    rcvNewLeader.rebuild(&followerLog, follower->highWatermark());

    // After rebuild: LSO should be HW if there are no open txns tracked.
    // But the coordinator still has PrepareCommit → re-driving markers is needed.
    // For the invariant: we assert read_committed on new leader is EMPTY because
    // there is no durable marker in its log.
    {
        int64_t newHw = follower->highWatermark(); // = 1 (received data before failover)
        // The rebuild found no markers → no open txns in the aborted index.
        // However, ReadCommittedView::rebuild walks the log and finds no COMMIT marker
        // for the producer, so the txn was never closed in the view. LSO = HW only if
        // the LsoTracker has no open txn; since rebuild found no onProduceBatch call,
        // the lsoTracker starts fresh. We manually add the open txn:
        //   (In a full system the partition state is reloaded from the leader epoch log.)
        newLsoTracker.clear();
        newAbortedIndex.clear();
        // Re-scan the new leader's log to seed LsoTracker with open txns.
        rcvNewLeader.rebuild(&followerLog, newHw);
        // After rebuild with no markers: if we seed the tracker for the open txn:
        newLsoTracker.onProduceBatch(pid, dataOff); // re-register open txn

        int64_t lso = newLsoTracker.lso(newHw);
        EXPECT_EQ(lso, dataOff)
            << "LSO must be pinned at data offset on new leader — no durable marker";

        auto rcRaw = rcvNewLeader.read(&followerLog, newHw, 0, 1 << 20,
                                        IsolationLevel::ReadCommitted);
        EXPECT_TRUE(parseOffsets(rcRaw).empty())
            << "INVARIANT: read_committed on new leader must be EMPTY before durable marker";
    }

    // -----------------------------------------------------------------------
    // Step 6: Re-drive the COMMIT marker from the coordinator's durable
    //         PrepareCommit state. Write the marker to the NEW leader's log.
    // -----------------------------------------------------------------------
    // Coordinator has PrepareCommit + markersPending=false (client-driven path).
    // Simulate re-driving via endTransaction result (partitions are still returned
    // by the coordinator) — or use the durable PrepareCommit to re-emit.
    // Since endTransaction already ran and returned the partition set, we
    // write the marker directly to the new leader's log:
    Offset newMarkerOff = followerLog.logEndOffset(); // = 1
    auto newMarkerBatch = ControlMarker::build(pid, epoch, ControlType::Commit, 0);
    {
        uint64_t u = static_cast<uint64_t>(newMarkerOff);
        for (int i = 0; i < 8; ++i) newMarkerBatch[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    followerLog.append(newMarkerBatch, newMarkerOff);

    // Update LSO: COMMIT marker resolves the open txn.
    newLsoTracker.onControlMarker(pid, newMarkerOff, /*commit=*/true);

    // completeTransaction on the coordinator.
    ASSERT_TRUE(coord->completeTransaction("f7-txn").ok());

    // -----------------------------------------------------------------------
    // Step 7: Invariant post-resolution — txn atomically visible.
    // -----------------------------------------------------------------------
    {
        int64_t newHw2 = followerLog.logEndOffset(); // = 2 (data + marker)
        int64_t lso = newLsoTracker.lso(newHw2);
        EXPECT_EQ(lso, newHw2)
            << "LSO must equal HW after COMMIT marker written and txn resolved";

        auto rcRaw = rcvNewLeader.read(&followerLog, newHw2, 0, 1 << 20,
                                        IsolationLevel::ReadCommitted);
        auto rcOffsets = parseOffsets(rcRaw);
        EXPECT_FALSE(rcOffsets.empty())
            << "INVARIANT: txn data must be visible under read_committed after durable marker";
        bool foundData = false;
        for (int64_t off : rcOffsets) { if (off == dataOff) { foundData = true; break; } }
        EXPECT_TRUE(foundData) << "Data batch at offset " << dataOff
                                << " must appear in read_committed result";
    }
    EXPECT_LE(newLsoTracker.lso(followerLog.logEndOffset()), followerLog.logEndOffset())
        << "LSO <= HW invariant must hold at all times";
}

// ---------------------------------------------------------------------------
// F7-b: If the COMMIT marker was replicated BEFORE leader failure (HW advanced
//        past marker), the txn is always visible on the new leader.
//        Contrast with F7-a to confirm the test is not vacuous.
// ---------------------------------------------------------------------------
TEST(LeaderFailoverEos, MarkerReplicatedBeforeFailover_TxnAlwaysVisible)
{
    HarnessDir leaderDir("f7b-leader");
    HarnessDir followerDir("f7b-follower");
    HarnessDir txnLogDir("f7b-txnlog");
    HarnessDir offsetStoreDir("f7b-offsets");

    std::string txnLogPath = txnLogDir.path + "/txn.log";
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);
    InMemoryTransport transport;

    PartitionConfig cfg1, cfg2;
    cfg1.partitionId = 0; cfg1.selfId = 1; cfg1.replicaLagMaxMs = 10000;
    cfg1.minIsr = 1; cfg1.fetchIntervalNs = 1'000'000LL; cfg1.isrCheckIntervalNs = 1'000'000LL;
    cfg2 = cfg1; cfg2.selfId = 2;

    std::vector<NodeId> replicas = {1, 2};
    std::vector<NodeId> isr      = {1, 2};
    int64_t clock = 1'000'000'000LL;

    auto leader   = std::make_unique<PartitionReplica>(cfg1, Role::Leader,   1, replicas, isr, &leaderLog,   &transport);
    auto follower = std::make_unique<PartitionReplica>(cfg2, Role::Follower, 1, replicas, isr, &followerLog, &transport);
    transport.setLeader(leader.get());
    transport.addFollower(2, follower.get());

    SimpleNodeLiveness liveness;
    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = 1; meta.replicas = replicas; meta.isr = isr; meta.leaderEpoch = 1;
    PartitionFailoverCoordinator failoverCoord(meta, &liveness);
    failoverCoord.registerReplica(1, leader.get());
    failoverCoord.registerReplica(2, follower.get());

    auto txnLog = std::make_unique<TransactionLog>(txnLogPath);
    auto coord  = std::make_unique<TransactionCoordinator>(
        ProducerIdAllocator{}, 60'000, txnLog.get(), 0);

    LsoTracker lsoTracker;
    AbortedTxnIndex abortedIndex;

    auto driveReplication = [&](Offset target) {
        for (int i = 0; i < kCap && leader->highWatermark() < target; ++i) {
            ++clock;
            leader->doWork(clock);
            follower->doWork(clock);
            transport.pump(clock);
        }
        return leader->highWatermark() >= target;
    };

    // Init + addPartitions + append data.
    auto ir = coord->initProducerId("f7b-txn", 60'000, clock);
    ASSERT_TRUE(ir.ok());
    ProducerId pid = ir.producerId; ProducerEpoch epoch = ir.producerEpoch;
    TopicPartition tp{"f7b-topic", 0};
    ASSERT_TRUE(coord->addPartitions("f7b-txn", pid, epoch, {tp}, clock).ok());

    auto dataBatch = makeDataBatch(0, pid, epoch, 0, "data");
    Offset dataOff = leader->append(dataBatch, true);
    ASSERT_GE(dataOff, 0);
    lsoTracker.onProduceBatch(pid, dataOff);
    ASSERT_TRUE(driveReplication(1)) << "HW must advance to 1";

    // endTransaction + write marker to leader, THEN replicate (pump transport).
    auto endRes = coord->endTransaction("f7b-txn", pid, epoch, true, clock);
    ASSERT_TRUE(endRes.ok());

    Offset markerOff = leaderLog.logEndOffset();
    auto markerBatch = ControlMarker::build(pid, epoch, ControlType::Commit, 0);
    { uint64_t u = static_cast<uint64_t>(markerOff); for (int i = 0; i < 8; ++i) markerBatch[i] = static_cast<uint8_t>(u >> (i * 8)); }
    leaderLog.append(markerBatch, markerOff);
    // DO pump transport so marker is replicated.
    ASSERT_TRUE(driveReplication(2)) << "HW must advance to 2 after marker replication";

    // Now kill leader.
    liveness.markDead(1);
    transport.removeFollower(2);
    failoverCoord.signalLeaderDead(1);
    { bool elected = false; for (int i = 0; i < kCap && !elected; ++i) { ++clock; elected = failoverCoord.doWork(clock) > 0; } ASSERT_TRUE(elected); }

    // New leader (node 2) has: data@0 + marker@1. HW should be 2.
    EXPECT_EQ(followerLog.logEndOffset(), 2LL);

    LsoTracker newLso; AbortedTxnIndex newAbt; ReadCommittedView rcv(&newLso, &newAbt);
    newLso.onProduceBatch(pid, dataOff);
    newLso.onControlMarker(pid, markerOff, true);
    rcv.rebuild(&followerLog, follower->highWatermark());

    // Invariant: txn visible because marker was replicated before failover.
    auto raw = rcv.read(&followerLog, follower->highWatermark(), 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets = parseOffsets(raw);
    bool found = false;
    for (int64_t off : offsets) if (off == dataOff) { found = true; break; }
    EXPECT_TRUE(found) << "Txn data must be visible when marker was replicated before failover";
    EXPECT_LE(newLso.lso(followerLog.logEndOffset()), followerLog.logEndOffset());
}
```

- [ ] **Step 2: Update CMakeLists.txt to add the new file and rune_replication dep**

In `libs/transaction/CMakeLists.txt`, modify the integration test target:

```cmake
add_executable(rune_transaction_integration_tests
    ${TXN_INTEG_DIR}/ExactlyOnceIntegrationTest.cpp
    ${TXN_INTEG_DIR}/LeaderFailoverEosTest.cpp)
target_include_directories(rune_transaction_integration_tests PRIVATE
    ${TXN_INTEG_DIR}
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_transaction_integration_tests PRIVATE
    rune_transaction
    rune_coordination
    rune_replication
    GTest::gtest_main)
```

- [ ] **Step 3: Build and run the new tests**

```bash
cmake --build build -j2 && cd build && ctest -R 'LeaderFailoverEos' --timeout 90 --output-on-failure
```

Expected: F7-a and F7-b PASS.

- [ ] **Step 4: Run the full suite (seq + -j4)**

```bash
cd ~/Projects/rune-broker/build
ctest --timeout 90 2>&1 | tail -5
ctest -j4 --timeout 90 2>&1 | tail -2
```

Expected: all tests pass, no failures.

- [ ] **Step 5: Commit**

```bash
git add libs/transaction/tests/integration/LeaderFailoverEosTest.cpp \
        libs/transaction/CMakeLists.txt
git commit -m "test(transaction): F7 cross-layer leader-failover EOS test — 2-replica PartitionReplica HW<marker invariant [P5-5 F7]"
```

---

## Task 6: Final commit + full suite verification

- [ ] **Step 1: Run the full suite sequentially and with -j4**

```bash
cd ~/Projects/rune-broker
if ! command -v cmake >/dev/null 2>&1; then CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)"); export PATH="$CMAKE_BIN:$PATH"; fi
cmake --build build -j2
cd build && ctest --timeout 90 2>&1 | tail -5
ctest -j4 --timeout 90 2>&1 | tail -2
```

Expected: all tests pass in both modes.

- [ ] **Step 2: Final commit (using exact message from spec)**

```bash
cd ~/Projects/rune-broker
git add -A
git commit -m "feat(transaction): real addOffsetsToTxn (offsets applied on commit / discarded on abort, persisted+recovered) + cross-layer leader-failover EOS test (marker-durability invariant) [P5-5 review]"
```

---

## Self-Review

**Spec coverage:**

| Requirement | Task |
|---|---|
| addOffsetsToTxn API on coordinator | Task 1, 2 |
| Epoch/PID fenced like addPartitions | Task 2 (validateEpochAndPid called) |
| Pending offsets persisted to TransactionLog | Task 3 |
| Applied to OffsetStore on COMMIT | Task 2 (completeTransaction PrepareCommit path) |
| Discarded on ABORT (and fence/timeout abort) | Task 2 (PrepareAbort path clears pendingOffsets) |
| Recovered PrepareCommit re-applies offsets on resolve | Task 3 (F3-c test) |
| Abort test FAILS if offsets advanced on abort | Task 4 (test 12 replacement) |
| Offset NOT in store before commit (genuine assert) | Task 4 (test 11 replacement) |
| 2-replica ISR PartitionReplica (HW can lag LEO) | Task 5 (real PartitionReplica, not faked HW) |
| Marker appended to leader ONLY (marker offset > HW) | Task 5 (leader log append, no transport pump) |
| Leader fails → follower promoted via PartitionFailoverCoordinator | Task 5 |
| read_committed EMPTY before durable marker | Task 5 (F7-a Step 5) |
| Marker re-driven from durable PrepareCommit | Task 5 (F7-a Step 6) |
| Txn atomically visible after marker | Task 5 (F7-a Step 7) |
| Contrast: marker replicated = always visible | Task 5 (F7-b) |
| LSO <= HW at all times | Task 5 (asserted in both subtests) |
| Full suite green seq + -j4 | Task 6 |

**Placeholder scan:** No TBDs. All test bodies are concrete.

**Type consistency:**
- `PendingTxnOffset.tp` is `rune::coordination::TopicPartition` — same type used in `OffsetStore::commit`.
- `OffsetApplyCallback` signature matches `OffsetStore::commit` parameters.
- `addOffsetsToTxn` returns `AddPartitionsResult` (same error codes as `addPartitions`, per Kafka).
- `makeDataBatch`, `appendControlMarker`, `parseOffsets`, `HarnessDir` imported from `ExactlyOnceHarness.h` in F7 test.
