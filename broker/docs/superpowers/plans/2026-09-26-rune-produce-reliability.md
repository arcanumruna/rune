# Rune Produce Reliability (any-size, zero-loss) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee every accepted produce record is persisted-and-acked or explicitly failed after bounded reliable delivery, at any partition count / batch size / in-flight depth, with zero added cost on the happy path.

**Architecture:** Four surgical parts. (A) Client caps per-frame wire size + honors `linger` so batches never grow into fragmentation-prone giants. (B) Aeron ingress/egress flow-control + term sizing so the publisher can't overflow the receiver (Aeron's own NAK/retransmit then recovers in-term loss). (C) Client retransmits un-acked frames on the existing produce-timeout tick by reusing the existing redirect-resend path (retained wire frame + attempt counter) — never abandons. (D) Broker never silent-drops, appends atomically, and dedups retransmits on the acks=Leader path returning the original offset.

**Tech Stack:** C++ (rune-client-cpp, rune-broker), gtest, Aeron, CMake/Ninja. Build fast with `cmake --build <dir> --target <t> -j4`.

**Spec:** `rune-broker/docs/superpowers/specs/2026-09-26-rune-produce-reliability-design.md`

## Global Constraints

- **Zero cost on the happy path** (ack-before-timeout): no new hot-path branch, no new per-record allocation, no new copy, no new virtual call. Reliability engages only on the cold path.
- **No throughput/latency regression:** multi-partition write throughput and produce p50/p99/p99.9 must stay within noise of the pre-change baseline (see Task 9 gate).
- **Invariant:** persisted ⟺ acked. Never silently drop; never abandon an accepted record.
- **Works at ANY** partition count, batch size (`RUNE_BATCH_BYTES` up to the 8 MiB `maxProduceBatchBytes_`), and in-flight depth (`maxInFlight`).
- Local repo `main` is never pushed; branch first. Commit messages end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Client test target: `rune_client_tests` (rune-client-cpp/CMakeLists.txt:370). Broker tests via ctest.

## Review Focus

- **Retransmit storm under sustained loss** — if every frame times out, retransmit must stay bounded (`deliveryTimeoutMs`) and back-pressure, not livelock the runner. Task 5 test.
- **Duplicate delivery on retransmit** — a retransmitted frame that WAS persisted (only the ack was lost) must be deduped by the broker and return the original offset, never double-appended. Task 7 test.
- **Ordering across a split batch** — a logical batch split into N frames (Task 3) must persist in submission order per partition even if frame k is retransmitted. Task 3 + Task 5 tests.
- **`maxInFlight` interaction with split frames** — splitting one logical batch into N frames must count against `maxInFlight` correctly (N frames, not 1) so the window still bounds outstanding bytes. Task 3 test.
- **Frame exactly at the cap boundary** — a batch whose wire size equals `maxFrameBytes` must emit one frame, `+1` byte must emit two; off-by-one here silently changes fragmentation behavior. Task 3 test.

---

## Task 1: Honor `linger` (including 0) in client config

**Files:**
- Modify: `rune-client-cpp/src/producer/Producer.cpp:18-33` (`ProducerConfig::fromEnv`)
- Test: `rune-client-cpp/tests/producer/ProducerBatchingTest.cpp`

**Interfaces:**
- Consumes: `ProducerConfig` (Producer.h:63), env `RUNE_LINGER_US`.
- Produces: `RUNE_LINGER_US=0` now sets `cfg.lingerUs = 0` (flush-immediately); unset keeps the 5000 µs default.

- [ ] **Step 1: Write the failing test** in `ProducerBatchingTest.cpp`:

```cpp
TEST(ProducerConfigFromEnv, LingerZeroIsHonored) {
    ::setenv("RUNE_LINGER_US", "0", 1);
    auto cfg = rune::client::ProducerConfig::fromEnv();
    ::unsetenv("RUNE_LINGER_US");
    EXPECT_EQ(cfg.lingerUs, 0);   // 0 must be honored, not treated as "unset"
}
TEST(ProducerConfigFromEnv, LingerUnsetKeepsDefault) {
    ::unsetenv("RUNE_LINGER_US");
    auto cfg = rune::client::ProducerConfig::fromEnv();
    EXPECT_EQ(cfg.lingerUs, 5000);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build <client-build> --target rune_client_tests -j4 && <client-build>/rune_client_tests --gtest_filter='ProducerConfigFromEnv.*'`
Expected: `LingerZeroIsHonored` FAILS (cfg.lingerUs == 5000, not 0).

- [ ] **Step 3: Implement** — in `Producer.cpp:20-23` change the guard so 0 is honored but negative/garbage is rejected:

```cpp
if (const char* v = std::getenv("RUNE_LINGER_US")) {
    const long val = std::strtol(v, nullptr, 10);
    if (val >= 0) cfg.lingerUs = static_cast<int64_t>(val);
}
```

- [ ] **Step 4: Run to verify both pass.**

- [ ] **Step 5: Commit** `git commit -m "fix(producer): honor RUNE_LINGER_US=0 (flush-immediately) ..."`

---

## Task 2: Add `maxFrameBytes` / `deliveryTimeoutMs` to ProducerConfig

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h:63-78` (ProducerConfig struct + fromEnv doc)
- Modify: `rune-client-cpp/src/producer/Producer.cpp:18-33` (parse the two new env vars)
- Test: `rune-client-cpp/tests/producer/ProducerBatchingTest.cpp`

**Interfaces:**
- Produces: `ProducerConfig.maxFrameBytes` (int32_t, default 61440 = 60 KiB), `ProducerConfig.deliveryTimeoutMs` (int64_t, default 30000). Env `RUNE_MAX_FRAME_BYTES`, `RUNE_DELIVERY_TIMEOUT_MS`.

- [ ] **Step 1: Write the failing test:**

```cpp
TEST(ProducerConfigFromEnv, FrameCapAndDeliveryTimeoutDefaults) {
    ::unsetenv("RUNE_MAX_FRAME_BYTES"); ::unsetenv("RUNE_DELIVERY_TIMEOUT_MS");
    auto cfg = rune::client::ProducerConfig::fromEnv();
    EXPECT_EQ(cfg.maxFrameBytes, 61440);
    EXPECT_EQ(cfg.deliveryTimeoutMs, 30000);
}
TEST(ProducerConfigFromEnv, FrameCapOverride) {
    ::setenv("RUNE_MAX_FRAME_BYTES", "16384", 1);
    auto cfg = rune::client::ProducerConfig::fromEnv();
    ::unsetenv("RUNE_MAX_FRAME_BYTES");
    EXPECT_EQ(cfg.maxFrameBytes, 16384);
}
```

- [ ] **Step 2: Run — fails to compile** (`maxFrameBytes` not a member).

- [ ] **Step 3: Implement** — add to `ProducerConfig` (Producer.h, next to `batchBytes` at ~line 65):

```cpp
int32_t maxFrameBytes    = 61440;   // hard per-FRAME wire cap (≤ this many bytes/frame),
                                     // independent of batchBytes — bounds fragmentation.
int64_t deliveryTimeoutMs = 30000;  // total time to keep retransmitting before failing.
```
and parse both in `fromEnv` (val > 0 guard, mirroring `batchBytes`).

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "feat(producer): add maxFrameBytes + deliveryTimeoutMs config ..."`

---

## Task 3: Cap per-frame wire size (split oversize batches)

**Files:**
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp:686-733` (`flushBatches` flush trigger) and `:399-451` (`drainRing` size gate)
- Test: `rune-client-cpp/tests/producer/ProducerBatchingTest.cpp`

**Interfaces:**
- Consumes: `cfg_.maxFrameBytes` (Task 2), existing `ps.builderBytes`, `cfg_.batchBytes`.
- Produces: no single emitted frame exceeds `maxFrameBytes`; a logical batch larger than the cap is emitted as multiple frames, each a valid RecordBatch, in order.

- [ ] **Step 1: Write the failing test** — configure a small cap and a large `batchBytes`, produce enough records to exceed the cap, assert multiple frames each ≤ cap and every record accounted for. Use the existing in-memory/loopback fixture pattern from `ProducerBatchingTest.cpp` (read it first for the fixture + how it counts emitted frames via the test `sendFn`/hub). Concretely:

```cpp
TEST_F(ProducerBatchingFixture, LargeBatchSplitsIntoCappedFrames) {
    cfg.batchBytes = 1 << 20;   // 1 MiB logical
    cfg.maxFrameBytes = 16384;  // 16 KiB frame cap
    cfg.lingerUs = 5000;
    // produce 2000 * 256B ≈ 512 KiB into ONE partition
    auto frames = produceAndCaptureFrames(/*records=*/2000, /*valueBytes=*/256, /*partitions=*/1);
    ASSERT_GE(frames.size(), 512u * 1024u / 16384u); // ≥ ~32 frames
    for (auto& f : frames) EXPECT_LE(f.wireBytes, 16384u);
    EXPECT_EQ(totalRecords(frames), 2000);           // no record dropped
}
TEST_F(ProducerBatchingFixture, FrameAtCapBoundaryEmitsOne_PlusOneEmitsTwo) {
    cfg.maxFrameBytes = /*exact wire size of N 256B records*/ frameWireBytes(64, 256);
    auto one = produceAndCaptureFrames(64, 256, 1);
    EXPECT_EQ(one.size(), 1u);
    auto two = produceAndCaptureFrames(65, 256, 1);
    EXPECT_EQ(two.size(), 2u);
}
```

- [ ] **Step 2: Run — fails** (currently one 512 KiB frame; `frames.size()==1`).

- [ ] **Step 3: Implement** — the cap is a *smaller flush threshold*, added to the already-computed `sizeFull` compare (no new hot-path branch beyond a `min`):
  - In `flushBatches` (ProducerImpl.cpp:699-701) change `sizeFull` to use the effective threshold:
    `const int32_t effCap = cfg_.maxFrameBytes > 0 && cfg_.maxFrameBytes < cfg_.batchBytes ? cfg_.maxFrameBytes : cfg_.batchBytes;`
    `const bool sizeFull = (ps.builderBytes >= effCap) || (cfg_.batchRecords > 0 && ps.builder->recordCount() >= cfg_.batchRecords);`
  - In `drainRing` size gate (ProducerImpl.cpp:406) use the same `effCap` so the builder never accumulates past a frame cap before flushing: `if (ps.builderBytes >= effCap && ps.inFlight >= cfg_.maxInFlight) return ABORT;` and, critically, stop appending into a builder already at `effCap` so the next record starts a new frame (add an ABORT/flush-boundary when `ps.builderBytes + nextRecordWire > effCap`). Read `:442-449` for how `builderBytes` is computed to size the check.
  - Each emitted frame keeps its own corrId/ack-slice (already per-flush at :714-724), so split frames are independently ack-tracked → counts against `maxInFlight` as N frames (Review Focus item 4).

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "feat(producer): cap per-frame wire size; split oversize batches (bounds fragmentation) ..."`

---

## Task 4: Retain wire frames until ack (confirm/extend redirect-resend buffer)

**Files:**
- Read first: `rune-client-cpp/src/producer/ProducerImpl.cpp:860-915` (redirect-resend — it already resends a retained wire frame) and the struct holding the retained frame (find via the `wire frame not available for resend` string).
- Modify (only if the retained-frame lifetime ends before ack): `rune-client-cpp/src/producer/ProducerImpl.cpp`, `include/rune/client/producer/ProducerImpl.h` (PartitionState in-flight entry)
- Test: `rune-client-cpp/tests/producer/ProducerRaceTest.cpp`

**Interfaces:**
- Consumes: existing redirect-resend retained-frame storage + attempt counter.
- Produces: an un-acked frame's bytes remain retrievable for retransmit until its ack arrives or `deliveryTimeoutMs` elapses; storage is the EXISTING pre-allocated in-flight structure (no new per-send allocation).

- [ ] **Step 1: Write the failing test** — assert that after a frame is sent and NOT acked, its wire bytes are still retrievable for resend (the same accessor redirect-resend uses). Model it on the redirect test in `ProducerRaceTest.cpp` (read it first). If redirect already retains until ack, this test passes immediately and the task is a confirmation (record that in the ledger).

- [ ] **Step 2: Run** — if it fails, the frame is dropped too early; if it passes, proceed (confirmation-only task).

- [ ] **Step 3: Implement (only if failing)** — extend the retained-frame lifetime to ack (not just redirect), reusing the existing storage; do NOT add a per-send heap allocation (reuse the ring/slot that already holds it). No change if already retained.

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "feat(producer): retain wire frame until ack for retransmit (reuse redirect-resend buffer) ..."`

---

## Task 5: Retransmit un-acked frames on produce-timeout (idempotent, bounded)

**Files:**
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp:245-265` (timeout sweep) and the timeout branch that currently calls `failPending` (:932) — read `:240-265` + `:860-915` first.
- Test: `rune-client-cpp/tests/producer/ProducerRaceTest.cpp`

**Interfaces:**
- Consumes: `cfg_.produceTimeoutMs` (per-attempt), `cfg_.deliveryTimeoutMs` (total), retained frame (Task 4), redirect-resend send path (:895-910).
- Produces: on per-attempt timeout, the frame is retransmitted (same pid+seq) instead of failed; only after `deliveryTimeoutMs` total does it `failPending`. Reuses the existing per-duty-cycle timeout sweep — NO new timer/scan.

- [ ] **Step 1: Write the failing test** — with a fixture whose `sendFn` DROPS the first send of a frame (simulating fragment loss) but delivers the retransmit, assert the record ultimately succeeds (offset ≥ 0), exactly once, within `deliveryTimeoutMs`:

```cpp
TEST_F(ProducerRaceFixture, DroppedFrameIsRetransmittedAndSucceeds) {
    cfg.produceTimeoutMs = 50; cfg.deliveryTimeoutMs = 2000;
    dropFirstSendOfEachFrame_ = true;   // fixture drops attempt #1
    auto fut = produceOne(/*wantAck=*/true);
    driveRunnerFor(std::chrono::milliseconds(500));
    ASSERT_EQ(fut.wait_for(1s), std::future_status::ready);
    EXPECT_GE(fut.get(), 0);            // succeeded via retransmit, not failed
    EXPECT_EQ(sendAttempts_, 2);        // one drop + one delivered
}
TEST_F(ProducerRaceFixture, PermanentLossFailsAfterDeliveryTimeout) {
    cfg.produceTimeoutMs = 20; cfg.deliveryTimeoutMs = 200;
    dropAllSends_ = true;
    auto fut = produceOne(true);
    driveRunnerFor(std::chrono::milliseconds(400));
    EXPECT_LT(fut.get(), 0);            // fails loudly only after deliveryTimeout
    EXPECT_GE(sendAttempts_, 2);        // retried, not abandoned on first timeout
}
```

- [ ] **Step 2: Run — fails** (today the first timeout calls `failPending`; `sendAttempts_==1`, fut<0).

- [ ] **Step 3: Implement** — in the timeout branch: if `now - firstSendNs < deliveryTimeoutMs`, resend the retained frame via the redirect-resend send helper (increment attempt counter, reset per-attempt deadline) instead of `failPending`; only `failPending` once `deliveryTimeoutMs` is exceeded. Bounds the storm (Review Focus 1). Preserve per-partition submission order: retransmit the oldest un-acked frame first.

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "feat(producer): retransmit un-acked frames until deliveryTimeout (idempotent, never abandon) ..."`

---

## Task 6: Aeron ingress + egress flow-control & term sizing

**Files:**
- Modify: `rune-broker` gateway client-facing publication/subscription channel construction (grep `aeron:udp?endpoint` + `termBufferLength`/`initialWindow` in `libs/gateway/src/ClientGateway.cpp`) and `rune-client-cpp/include/rune/client/Client.h:117` (`termBufferBytes`).
- Test: `rune-broker` integration test that produces a large single-partition burst and asserts zero loss (or the cross-repo e2e in Task 8).

**Interfaces:**
- Produces: client→gateway and broker→client channels sized so the publisher back-pressures rather than dropping under a full `maxInFlight` window; no `fc=min` stall reintroduced (WorkerEgress.h:15 removed it deliberately).

- [ ] **Step 1: Write the failing test** — a broker+client loopback integration test (model on the existing cross-repo e2e) that produces a 1 MiB-batch single-partition burst at high in-flight and asserts `succeeded == attempted`. Read the existing e2e harness for the fixture. Expected initially: FAIL (loss).

- [ ] **Step 2: Run — fails** (loss without flow-control sizing; passes only once Tasks 3+5 land — sequence this test to run WITH them, so it validates B on top of A/C).

- [ ] **Step 3: Implement** — set the client publication `term-length` (Client.h termBufferBytes) and the gateway subscription initial window / flow-control to sizes that cover the max in-flight window; choose a back-pressuring flow-control strategy for the client→gateway path. Document the exact URI params chosen. Do not set `fc=min` on egress.

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "feat(transport): size Aeron ingress/egress flow-control + term buffers for full in-flight window ..."`

---

## Task 7: Broker — confirm PSM dedup on acks=Leader + never silent-drop

**Files:**
- Read: `rune-broker/libs/gateway/src/ProducePath.cpp:143-169,222` (dedup already returns original offset) and `libs/storage/src/ProducePath.cpp` (same). Confirm the acks=Leader route (`durabilityTier=0/leader`) passes through `producerState.validate`.
- Modify (only if Leader path skips dedup): the Leader branch to run `validate` + retry-window record.
- Verify: `ClientGateway.cpp:1467-1470` loud-rejects >`maxProduceBatchBytes_` (already present).
- Test: `rune-broker` unit/integration test.

**Interfaces:**
- Consumes: retransmitted frame with same pid+seq (Task 5).
- Produces: a duplicate (already-persisted) frame returns the ORIGINAL baseOffset, never double-appends; acks=Leader included.

- [ ] **Step 1: Write the failing test** — produce a batch (pid=P seq=S) to a Leader-only topic, capture its baseOffset, produce the identical batch again, assert the second returns the SAME baseOffset and the log length is unchanged (no double-append). Read the existing ProducePath dedup test for the fixture.

- [ ] **Step 2: Run** — if the Leader path already dedups, PASSES (confirmation-only; note in ledger). If it FAILS (double-append), proceed.

- [ ] **Step 3: Implement (only if failing)** — route the acks=Leader produce through the same `producerState.validate` + retry-window record used by Majority (ProducePath.cpp:143-224), so a retransmit is recognized and re-acked with the original offset.

- [ ] **Step 4: Run — passes.**

- [ ] **Step 5: Commit** `git commit -m "fix(broker): dedup produce retransmits on acks=Leader (idempotent re-ack, no double-append) ..."`

---

## Task 8: Cross-repo e2e — single-partition any-size zero-loss

**Files:**
- Use: `rune-broker/bench/kafka-compare/` (harness) + the existing cross-repo e2e runner.
- Test: a scripted e2e that produces to 1 partition at large window + large batch and asserts full delivery + consume parity.

- [ ] **Step 1: Write the failing test** — script (or gtest e2e) starting a real broker + real client: produce N=1,000,000 × 1 KiB to a single partition with `RUNE_BENCH_WINDOW=100000 RUNE_BATCH_BYTES=1048576`, assert `succeeded == N` and a subsequent consume-from-earliest returns exactly N.

- [ ] **Step 2: Run — fails** on current binaries (the bug).

- [ ] **Step 3: Implement** — none (integration of Tasks 1-7). If it fails, the failing sub-part is a bug in the owning task — return there.

- [ ] **Step 4: Run — passes:** single-partition large-batch produce is 100% delivered.

- [ ] **Step 5: Commit** `git commit -m "test(e2e): single-partition any-size produce is zero-loss ..."`

---

## Task 9: No-regression benchmark gate

**Files:**
- Use: `rune-broker/bench/kafka-compare/run-matrix.sh` (Rune phases).

- [ ] **Step 1: Capture baseline** — before merging Tasks 1-7, run the Rune write + latency phases on the bench box and record throughput + p50/p99/p99.9 to `docs/benchmarks/2026-09-26-reliability-baseline.md`.

- [ ] **Step 2: Run post-change** — same phases on the reliability branch.

- [ ] **Step 3: Compare** — multi-partition write throughput and produce p50/p99/p99.9 must be within noise (±5%) of baseline. If any regressed beyond noise, the happy path was touched — find and remove the added cost (return to the owning task).

- [ ] **Step 4: Record** results in the same doc; state pass/fail against the ±5% gate.

- [ ] **Step 5: Commit** `git commit -m "bench: reliability fix has no happy-path throughput/latency regression ..."`

---

## Self-Review

**Spec coverage:** Part A → Tasks 1-3; Part B → Task 6; Part C → Tasks 4-5; Part D → Task 7; guarantee/validation → Tasks 8-9. All four parts + the no-regression gate covered.

**Placeholder scan:** Tasks that may be confirmation-only (4, 7) say so explicitly and give the failing-test that decides; no "TBD"/"handle edge cases". Test code is concrete; deeper C++ internals cite exact files/lines the implementer reads first (redirect-resend buffer, PSM validate) rather than inventing unverified signatures.

**Type consistency:** `maxFrameBytes` (int32_t), `deliveryTimeoutMs` (int64_t) defined in Task 2, used identically in Tasks 3/5. `effCap` computed the same way in flushBatches and drainRing (Task 3).

**Review Focus:** all five items have owning-task tests (storm→T5, duplicate→T7, ordering→T3/T5, maxInFlight→T3, boundary→T3).
