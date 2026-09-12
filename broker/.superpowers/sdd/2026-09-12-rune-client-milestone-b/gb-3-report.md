# GB-3 Report — Gateway Subscribe Handler → Egress-Paced Push

**Date:** 2026-09-12  
**Commit:** `93e6b69`  
**Base:** `d4bd177` (GB-2)

---

## Status

DONE. All 1727 tests pass (0 failures, 100.22 s). Commit `93e6b69` (local, main branch).

---

## Deliverables

### 1. Subscription Registry (`ClientGateway.h` / `ClientGateway.cpp`)

```cpp
struct Subscription {
    int64_t subscriptionId = 0;
    int64_t sessionId      = 0;
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int64_t nextOffset     = 0;
    uint8_t isolation      = 0;   // 0=read_uncommitted, 1=read_committed
};
```

- `std::unordered_map<int64_t, Subscription> subscriptions_` — keyed by subscriptionId
- `int64_t nextSubscriptionId_ = 0` — monotonic, incremented to 1 on first use
- `std::vector<int64_t> deadSubScratch_` — pre-allocated; not used in iteration (map handles erasure in-place)
- `static constexpr int kMaxBatchesPerSubPerCycle = 8` — per-cycle fairness cap
- `static constexpr int32_t kMaxChunkBytes = 512 * 1024` — max bytes per Log::read call

### 2. Dispatch

**handleSubscribe (tid18):**
1. Decode Subscribe — drop on truncation
2. Require known session — drop on unknown (no egress)
3. Resolve partition: explicit (partitionKnown=1) or `partitionForKey()` (FNV-1a 64)
4. Validate topic + partition → `SubscribeResponse{UnknownTopic | UnknownPartition}`
5. Clamp fromOffset: `fromOffset < 0` → Latest (LEO); `fromOffset > LEO` → LEO; otherwise as-is
6. Allocate subscriptionId; register Subscription; reply `SubscribeResponse{OK, partition, subscriptionId}`

**handleUnsubscribe (tid21):**  
Remove subscription by ID. No response. Unknown ID silently ignored.

**Session lifecycle:**  
Both `reapExpired()` and `handleDisconnect()` now iterate the subscriptions map and erase entries matching the removed session. No subscription outlives its session.

### 3. Push Delivery (`pushSubscriptions`)

Called from `doWork()` after ingress poll, before returning.

```
for each subscription:
  bound = log.logEndOffset()   // both isolation=0 and isolation=1 use LEO for M-B
  // TODO(Milestone C): read_committed uses real LSO once transactions land
  if nextOffset >= bound → skip

  chunk = log.read(nextOffset, kMaxChunkBytes)
  batchesPushed = 0
  while batchesPushed < kMaxBatchesPerSubPerCycle && !backPressured:
    batchSize = 12 + LE32@(chunk + kLengthOffset)
    baseOffset = LE64@(chunk + kBaseOffsetOffset)
    recordCount = LE32@(chunk + kRecordCountOffset)
    encode Records{subscriptionId, topicId, partition, baseOffset, batchBytes}
    pos = pub.tryClaim(encoded)
    if pos > 0:
      commit claim
      nextOffset = baseOffset + recordCount  // advance ONLY on success
      ++batchesPushed
    else:
      backPressured = true  // STOP — retry from same nextOffset next cycle
```

Key invariant: **nextOffset is never advanced past a batch that was not successfully pushed** (tryClaim > 0). Back-pressure → stop, retry next cycle from exact offset. No loss, no duplication.

### 4. Isolation Bound

```cpp
// TODO(Milestone C): read_committed uses the real LSO once transactions land.
const int64_t bound = log->logEndOffset();  // both isolation=0 and isolation=1 use LEO for M-B
```

Both `isolation=0` (read_uncommitted) and `isolation=1` (read_committed) resolve to LEO for Milestone B (no transactions → LSO == LEO). The TODO marks where real LSO tracking feeds in.

---

## Tests (9 cases, all passing)

| # | Test | Result |
|---|------|--------|
| 1 | `Subscribe_From0_ReceiveAll_InOrder` | PASS |
| 2 | `Subscribe_FromOffset_MidStream` | PASS |
| 3 | `Subscribe_FromLatest` | PASS |
| 4 | `BackPressure_ExactlyOnce_Gate` | PASS |
| 5 | `Unsubscribe_StopsDelivery` | PASS |
| 6 | `TwoSubscriptions_NoXtalk_Fairness` | PASS |
| 7 | `ReSubscribe_FromTrackedOffset_Resumes` | PASS |
| 8 | `ReadCommitted_EqualsUncommitted_ForMilestoneB` | PASS |
| 9 | `MalformedSubscribe_ErrorCodes_NoGatewayCrash` | PASS |

**Full suite: 1727/1727 passed.**

### BackPressure_ExactlyOnce_Gate design

Subscribe first (get SubscribeResponse with combined assembler), then produce 20 batches × 3 records. Drive doWork in bursts, drain egress between bursts. Assert:
- Exactly 20 Records received
- baseOffsets contiguous: 0, 3, 6, …, 57
- No duplicates (sorted+unique check)

Test uses a combined fragment assembler that captures both SubscribeResponse and Records in a single poll to prevent Records from being dropped during the SubscribeResponse pump.

---

## Design Notes

### RecordBatch iteration
`batchSize = 12 + LE32@kLengthOffset` where `kLengthOffset=8`. The `kLengthOffset` field holds the count of bytes AFTER itself (i.e. from byte 12 onward). Total batch bytes = 12 + that field. `baseOffset` at `kBaseOffsetOffset=0` (LE64), `recordCount` at `kRecordCountOffset=57` (LE32). `nextOffset` advances to `baseOffset + recordCount` exactly.

### Zero-heap hot path
`scratch_` (pre-allocated 68 KiB member array) is used for Records encoding. `Log::read` returns a `std::vector<uint8_t>` (unavoidable OS read copy) but no per-batch allocation occurs.

### fromOffset sentinel
`fromOffset < 0` → "from Latest" (nextOffset = LEO). `fromOffset > LEO` → clamp to LEO. Documented in handleSubscribe.

### Test design note
Tests that produce data before subscribing were structured to call `directAppend` AFTER `recvSubscribeResponse` returns, to prevent Records pushed during the SubscribeResponse pump from being silently dropped by a single-purpose SubscribeResponse assembler. The BackPressure test uses a combined assembler from the start.

---

## Files Changed

- `libs/gateway/include/rune/gateway/ClientGateway.h` — Subscription struct, registry members, method declarations
- `libs/gateway/src/ClientGateway.cpp` — handleSubscribe, handleUnsubscribe, pushSubscriptions, reapExpired/handleDisconnect subscription cleanup, doWork push call
- `libs/gateway/tests/integration/GatewaySubscribeTest.cpp` — 9 integration tests (new file)
- `libs/gateway/CMakeLists.txt` — rune_gateway_subscribe_tests target

---

## Concerns

None. The implementation exactly matches the brief's correctness gate: nextOffset is never advanced past an un-pushed batch, ensuring exactly-once in-order delivery under back-pressure.
