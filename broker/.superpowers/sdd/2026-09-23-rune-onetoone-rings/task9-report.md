# Task 9 Report — E3: IoBackend Wiring, Control-Thread Strict-Path Assessment, Parity + TSan

**Date:** 2026-09-24  
**Branch:** main (local only, never pushed)

---

## 1. IoBackend Wiring Confirmation (Deliverable 1)

**Result: Complete — no changes needed.**

The `WorkerPool` constructor already accepts `ioSubmitDepth` and `ioCompletionDepth` parameters (Task 7 completed this):

```
WorkerPool(int32_t workerCount, ShardRouter, size_t ringCapacity,
           size_t ioSubmitDepth = Worker::kIoSubmitDepth,
           size_t ioCompletionDepth = Worker::kIoCompletionDepth)
```

`BrokerNode::buildWorkerPool()` (line 577) passes `config_.ioSubmitRingDepth` and `config_.ioCompletionRingDepth` directly:
```cpp
workerPool_ = std::make_unique<WorkerPool>(
    wc, std::move(router),
    static_cast<std::size_t>(config_.workerRingCapacity),
    static_cast<std::size_t>(config_.ioSubmitRingDepth),      // E3 Task 7
    static_cast<std::size_t>(config_.ioCompletionRingDepth)); // E3 Task 7
```

Each Worker's `io_` (an `IoBackend` = `PortableFlushBackend` on macOS) is constructed in the Worker ctor with those depths:
```cpp
Worker(int32_t id, size_t, size_t ioSubmitDepth, size_t ioComplDepth) noexcept
    : io_(ioSubmitDepth, ioComplDepth)
```

Chain is complete: `NodeConfig → WorkerPool ctor → Worker ctor → io_(submitDepth, completionDepth)`.

---

## 2. Control-Thread / Single-Node Strict-Tier Path Assessment (Deliverable 2)

### Analysis

**Can a Strict-tier (durabilityTier==2) produce reach a code path with no IoBackend?**

Three cases examined:

**Case A: Internal partitions (topicId < 0)**  
`ocfg`/`tcfg`/`mcfg` in `buildGateway()` do NOT set `durabilityTier`, so they inherit the default `1` (Majority) from `PartitionConfig`. Internal partitions (`__consumer_offsets`, `__transaction_state`, `__cluster_metadata`) are driven by the control thread and will NEVER have `durabilityTier==2`. No gap.

**Case B: User partitions in normal BrokerNode operation**  
`BrokerNode::buildWorkerPool()` unconditionally creates a `WorkerPool` (`wc = max(1, workerCount)`). `buildGateway()` calls `buildWorkerPool()` as its last step and then wires `workerPool_` into the gateway. In any started `BrokerNode`, `workerPool_` is ALWAYS non-null. User produce ops (`topicId >= 0`) take the worker path at `ClientGateway.cpp:1434` — they route through `workerPool_->tryEnqueueOp()` to a Worker that owns an `IoBackend`. **No gap.**

**Case C: Inline gateway path (workerPool_ == null)**  
This path (`ClientGateway.cpp:1486-1607`) is reached ONLY when `workerPool_` is null — which only occurs in:
- Test fixtures that construct `ClientGateway` directly without a `WorkerPool` (bypassing `BrokerNode`)
- Theoretically if `config_.workerCount == 0` AND the code path skips `buildWorkerPool()` (not possible in current code)

The inline path does NOT check `durabilityTier`. It calls `appendNoSync`, marks the log dirty, and calls `flushDirtyLogs()` (which calls `log->sync()` synchronously) at end of the doWork cycle. The TIMING issue: the ack is sent BEFORE the sync when `acks=Leader`. So a hypothetical Strict-tier produce on the inline path would be prematurely acked as durable before the sync completes.

### Decision: Synchronous sync() fallback added

**Choice made: (a) SYNCHRONOUS `log->sync()` before acking.** Per the plan's guidance ("prefer synchronous sync() fallback so Strict still means durable").

**Code added to `ClientGateway.cpp` in the inline path (after `appendNoSync`, before ack):**

```cpp
// E3 Task 9: Strict-tier sync() fallback on the inline (no-WorkerPool) path.
// This path is only reached when workerPool_ is null — i.e. in test fixtures
// that bypass BrokerNode/WorkerPool construction. In a fully started BrokerNode
// workerPool_ is ALWAYS non-null and user produces NEVER reach this branch.
// Internal partitions always have durabilityTier=1 (Majority), never Strict.
// Defensive: if a Strict-tier produce reaches this path, SYNCHRONOUS sync()
// before the ack so Strict still means durable. Blocking on the control thread —
// deliberately so: it is a fallback for a path that should never fire in production.
const uint8_t effectiveTier = replica ? replica->durabilityTier() : 1;
if (effectiveTier == 2) {
    try {
        log->sync();
        dirtyLogs_.erase(log);  // already synced, skip flushDirtyLogs()
    } catch (...) {
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }
} else {
    dirtyLogs_.insert(log);  // normal: sync in flushDirtyLogs() at end of cycle
}
```

### Parked for future work

An async IoBackend on the control thread for internal partitions is OUT OF SCOPE for E3. Reasoning: internal partitions are always Majority-tier (the default), so no async durability is required for them. If internal partitions ever need Strict durability, a proper async IoBackend on the control thread would be needed — this is an E4/future item. Similarly, the inline gateway path with Strict tier is a test-only construct; the sync() fallback is defensive rather than a new production path.

---

## 3. Parity Gate (Primary Gate)

**Build:** `ninja -C build-fast rune_node_tests rune_broker_node_tests rune_broker_node_gateway_tests rune_async_durability_tests` — clean build.

**Test run:**
```
ctest --test-dir build-fast -R "BrokerNode\.|GatewayProduce|GatewayAcksAllDeferredAck|WorkerPool|MultiLeaderProduce|OffsetReplication|MetadataReplication|AsyncDurability" -E "FlushFailureErrorsTheAckNeverOK" -j4
```

**Result: 43/43 PASSED.**

**Note on FlushFailureErrorsTheAckNeverOK:** This test (`AsyncDurability.FlushFailureErrorsTheAckNeverOK`) is a **pre-existing flaky test** with a timing race: it closes an fd immediately after submit, but the flusher thread may or may not fsync before the close depending on OS scheduling. Confirmed flaky on the original codebase (fails ~2/5 runs, passes ~3/5 runs) by stashing Task 9 changes and running the original build 5 times. My changes do NOT affect this test — it exercises `PortableFlushBackend` directly without involving `ClientGateway`. The flakiness is entirely in the test's close-before-fsync timing assumption, which is not OS-guaranteed.

---

## 4. TSan Gate (Primary Gate)

**Build:** `make -j4 rune_async_durability_tests rune_node_tests rune_broker_node_tests rune_broker_node_gateway_tests` in `build-tsan` (Make + TSan flags: `-fsanitize=thread -fno-omit-frame-pointer -g`).

**Test run:**
```
TSAN_OPTIONS="suppressions=$(pwd)/cmake/tsan-aeron.supp halt_on_error=1" \
ctest --test-dir build-tsan -R "AsyncDurability|WorkerPool\.|BrokerNodeTest" \
  -E "FlushFailureErrorsTheAckNeverOK" -j1
```

**Result: 32/32 PASSED — ZERO ThreadSanitizer reports.**

Breakdown:
- `AsyncDurability.*` (3 tests): StrictProduceAcksOnlyAfterFlushCompletion, GroupCommitCoalescesManyProducesIntoOneFlush, SubmitRingFullHoldsProduce — all PASSED
- `WorkerPool.*` (12 tests including RealThreadProduceConcurrentTSan) — all PASSED
- `BrokerNodeTest.*` (17 tests) — all PASSED

No ThreadSanitizer data races detected on the new worker↔flusher submit/completion ring boundaries, the memory ring, or the BrokerNode multi-threaded paths.

---

## 5. Summary of Changes

**Only one file modified:** `libs/gateway/src/ClientGateway.cpp`

- Added the Strict-tier sync() fallback on the inline (no-WorkerPool) path, with a detailed comment explaining why this path is unreachable in production and documenting the decision.

**No changes to:**
- `WorkerPool.h` — already complete from Task 7
- `BrokerNode.cpp` — already complete from Task 7
- `NodeRuntimeAgent.h` — no gap found (internal partitions always Majority tier)
- `Worker.h` — already complete from Tasks 4/5

---

## 6. Open Items / Parked

1. **`AsyncDurability.FlushFailureErrorsTheAckNeverOK` flakiness:** Pre-existing race in test design (close fd after submit, assumes close happens before flusher's fdatasync). Not introduced by Task 9. Recommend fixing the test to use a bad fd (-99) instead of close-after-submit for deterministic EBADF.

2. **Async IoBackend on the control thread for internal partitions:** Not needed (internal topics always Majority). If ever needed (E4+), the control thread would need its own IoBackend and a deferred-ack mechanism mirroring the Worker's durablePending_ path.

3. **Linux io_uring validation (E4):** UringFlushBackend is implemented (Task 8) but the authoritative `<100µs` p99 benchmark and SQPOLL zero-syscall validation require the Hetzner Linux box. Deferred to E4 per plan.
