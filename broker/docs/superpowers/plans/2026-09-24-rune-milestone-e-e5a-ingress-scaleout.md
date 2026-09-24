# Milestone E — E5a: Gate 2 + Per-Worker Ingress (throughput / latency scale-out) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the two single-thread ingress funnels and the 2–3× per-produce batch copies so a produce goes wire→append with zero cross-thread hops, unlocking near-linear throughput scaling and sub-millisecond batched latency.

**Architecture:** Land the missing "Gate 2" correctness/measurability fixes first (reliable egress, MTU/rcv-window, configurable fragment limit, client batch instrumentation). Then give each partition-worker its own Aeron subscription/stream: the worker polls the wire directly on its own pinned thread, decodes the produce frame straight from the Aeron term buffer, rewrites the base offset in place, and appends — eliminating `restampScratch_`, the `WorkerOp` vector copy, and the SPSC ring hop for the produce path. The client learns the per-worker stream from an extended metadata response and publishes each partition straight to its owning worker. The gateway retains only the control plane (metadata/group/txn/offset) and non-produce traffic.

**Tech Stack:** C++23, Aeron C media driver + Aeron C++ client, agrona `OneToOneByteRing`, `rune-protocol` (sibling header package), spdlog, gtest/ctest, `rune-client-cpp` sibling repo.

**Spec:** `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md` (§ "Throughput / ingress scale-out (E5 — added 2026-09-24)", items 1 + prerequisites; phasing E2/E5).

## Global Constraints

- **Shared-nothing law (foundational):** no shared mutable state, no locks, no atomics, no `thread_local` on any hot path; all cross-thread communication via SPSC rings. A worker is the single owner of its shards. `[[rune-shared-nothing-law]]`
- **No virtual calls on any hot path:** de-virtualize with concrete types + branch or templates; never benchmark-gate a virtual call away. `[[rune-no-virtual-calls-hot-path]]`
- **Aeron is the reference:** mirror Aeron/agrona threading, encoders, fences, and idle strategies exactly; use Aeron's own ring buffers. `[[aeron-is-the-bible]]`
- **Endianness:** all wire encodings little-endian. `[[rune-endianness-all-le]]`
- **No silent failures:** every dropped/refused produce or ack surfaces an error to the client; back-pressure returns a defined error code, never a lost promise.
- **Default idle strategy stays `Backoff`:** busy-spin is opt-in per isolated core only (never a bare busy-spin default).
- **Parity gate:** every Milestone-D and prior-E test passes unchanged after each phase. `[[rune-review-until-clean]]`
- **Latency floor preserved:** the existing per-record latency win (p50 ~2 ms on the 4-core box; native `<100µs` p99 gate on a right-sized box) must not regress.
- **Build/test loop:** use `build-fast` (Ninja + ccache, no TSan) for edit-build-test; run TSan only for the final race proof. `[[rune-fast-build-loop]]` `[[sibling-cpp-build-profile]]`
- **Never push:** all work stays on local `main`; never push either repo.

## Acceptance Gates (this plan)

- **G-A1:** Batched produce completes end-to-end with **no silent ack-drops** under sustained load (reliable egress proven by a drop-injection test).
- **G-A2:** On the 4-core build agent, batched throughput scales measurably with worker count (the E5 regression-of-record: it does **not** today), and the per-produce batch is copied **at most once** on the produce hot path (the wire→append path).
- **G-A3:** All Milestone-D + prior-E tests green under per-worker ingress, including a shard-migration test proving a partition's ingress stream moves with ownership (no dropped/duplicated produces).
- **G-A4:** Final ≥500k/s + <1 ms batched validation is **deferred to a ≥16-core box** (per the "implement first, defer big box" decision) and is covered by plan E5b's benchmark phase, not here.
- **G-A5 (shared-nothing / TSan):** After per-worker ingress, each worker is the **sole owner** of its shard's `Log`/`OffsetIndex` — including flush/sync. The **pre-existing data race** TSan found (`Worker::executeProduceWire_ → append` on the worker thread racing `ClientGateway::flushDirtyLogs → Log::sync` on the gateway thread, both touching `OffsetIndex::entries_`) MUST be closed: no thread other than the owning worker touches a shard's Log. Proven by a TSan run of the produce + migration path with **zero race reports**. (Found during the G0 failover retro-review; owned here per you-find-it-you-fix-it, not deferred.)
- **G-A5b (single-owner replication state — 2nd pre-existing race, found in the seam investigation):** the leader replica's HW/ISR state (`leaderState_.followerLeo_`/`prevHw_`, `isrManager.followerState_`) must be touched ONLY by the owning worker. Today the node thread mutates it inline via `transport_->poll → onFetchRequest` while the worker mutates it via `replica->doWork` — an unguarded race. Fix (Task E5.1.2c): route leader-side `FetchRequest` (and other leader-state-touching handlers, for user partitions) to the owning worker via the ingress ring (mirroring the `applyReplicated` routing that already exists), response via the outbound ring — making `leaderState_` single-owner. Proven by the TSan gate. This is also the enabler for correct worker-local acks=all commit-wait.
- **G-A6 (end-to-end backpressure — NEVER lose an ack):** The produce path must have **no drop point**. When any downstream stage cannot accept work, backpressure propagates upstream all the way to the producing client's `offer()`, via this chain: client egress blocked → gateway **stops draining** the per-worker `ret_` return rings (it must NOT best-effort-drop) → `ret_` fills → the owning worker **stops polling its ingress subscription** (holding any owed ack; it does not append what it can't ack) → the Aeron ingress term buffer fills → the client's produce `offer()` returns `BACK_PRESSURED` and the producing client slows/retries. Every appended record yields **exactly one** ack once the pipeline unblocks — zero acks lost, zero duplicated. **This SUPERSEDES G0.1's ring-full best-effort-drop fallback on the per-worker path** (replace it with "stop and let backpressure propagate"). Proven by an end-to-end backpressure test: block the client egress, sustain produces, assert (a) the producer's `offer()` eventually back-pressures, (b) no record is appended without an eventual ack, (c) on unblock every ack is delivered exactly once. Worker rule: in `doWork`, flush owed acks to `ret_` FIRST; only poll ingress when no ack is owed AND `ret_` has space.

---

## File Structure

**rune-broker**
- `libs/node/include/rune/node/NodeConfig.h` + `src/NodeConfig.cpp` — new config fields + env parsing (MTU, rcv-window, fragment limit, per-worker ingress base stream).
- `libs/node/src/BrokerNode.cpp` — driver-context MTU/window; build per-worker ingress subscriptions; stop gateway produce fan-out.
- `libs/gateway/include/rune/gateway/ClientGateway.h` + `src/ClientGateway.cpp` — reliable egress retry queue; configurable fragment limit; extend metadata `PartitionEntry` with per-worker stream; remove produce fan-out; micro-eliminations (cache `leaderView_`/`minIsr_`).
- `libs/node/include/rune/node/WorkerIngress.h` (**new**) — per-worker Aeron subscription + `FragmentAssembler` + decode-restamp-append handler.
- `libs/node/include/rune/node/Worker.h` — own a `WorkerIngress`; poll it in `doWork`; in-place offset restamp on the term-buffer view.
- `libs/node/include/rune/node/WorkerPool.h` — construct per-worker ingress with base stream + workerId.
- `libs/node/tests/**` — unit + integration tests per task.

**rune-protocol** (sibling header package)
- `PartitionEntry` schema — add `ingressStreamId` (and keep `leaderId`/`leaderEpoch`); bump metadata response version.

**rune-client-cpp**
- `include/rune/client/producer/Producer.h` + `src/producer/ProducerImpl.{h,cpp}` — `framesSent()`/`recordsSent()` counters; `RUNE_LINGER_US`/`RUNE_BATCH_BYTES`/`RUNE_BATCH_RECORDS`; per-partition stream selection.
- `include/rune/client/transport/AeronTransport.{h,cpp}` — per-partition publication keyed by `(channel, ingressStreamId)`; retarget preserves per-worker stream.
- `examples/latency_bench.cpp` — expose linger/batch knobs + records-per-frame reporting.

---

## Phase G0 — Gate 2 prerequisites (correctness + measurability)

### Task G0.1: Reliable egress retry queue (defect #3 — silent ack-drop)

**Files:**
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp`
- Test: `libs/gateway/tests/ClientGatewayEgressTest.cpp` (new)

**Interfaces:**
- Produces: `bool sendRespReliable(pc::ErrorCode, int32_t partition, int64_t baseOffset, int64_t correlationId)` — enqueues on the pending-responses ring if the Aeron `offer` back-pressures instead of dropping; `int drainPendingResponses(int64_t nowNs)` — retries queued responses at the top of `doWork`, returns work count.
- Consumes: existing `sendResp` lambda in `handleProduce` (`ClientGateway.cpp:958-1002`), existing egress `Publication`.

- [ ] **Step 1: Write the failing test** — offer that back-pressures then recovers must deliver the ack, never drop it.

```cpp
// ClientGatewayEgressTest.cpp
TEST(ClientGatewayEgress, BackpressuredAckIsRetriedNotDropped) {
    FakePublication pub;                    // offer() returns BACK_PRESSURED first N times
    pub.backpressureNext(3);
    ClientGateway gw = makeGatewayWith(pub);
    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::None, /*partition*/0,
                                    /*baseOffset*/42, /*corr*/7));
    // Not delivered yet (all 3 offers back-pressured):
    EXPECT_EQ(pub.deliveredCount(), 0);
    for (int i = 0; i < 5; ++i) gw.drainPendingResponses(nowNs());
    ASSERT_EQ(pub.deliveredCount(), 1);
    EXPECT_EQ(pub.lastResponse().baseOffset, 42);
    EXPECT_EQ(pub.lastResponse().correlationId, 7);
}
```

- [ ] **Step 2: Run it to confirm it fails** — Run: `ctest --test-dir build-fast -R ClientGatewayEgress -V`. Expected: FAIL (`sendRespReliable` undefined).
- [ ] **Step 3: Implement** — add a fixed-capacity `pendingResponses_` ring (pre-allocated, no hot-path alloc). `sendRespReliable` tries `offer` once; on `BACK_PRESSURED`/`ADMIN_ACTION` it enqueues the encoded response bytes and returns `true` (accepted for delivery). `drainPendingResponses` re-offers from the head each `doWork` before new ingress; on success pop, on back-pressure stop (preserve order). Route all `handleProduce` ack/redirect sends through `sendRespReliable`.
- [ ] **Step 4: Run tests** — Run: `ctest --test-dir build-fast -R "ClientGatewayEgress|gateway" -V`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "fix(gateway): reliable egress retry queue — never silently drop a produce ack (Gate2 defect #3)"` + trailer.

### Task G0.2: MTU + flow-control window config knobs

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h`, `libs/node/src/NodeConfig.cpp`, `libs/node/src/BrokerNode.cpp:1468-1480`
- Test: `libs/node/tests/NodeConfigTest.cpp`

**Interfaces:**
- Produces: `int32_t aeronMtuBytes = 65504;` (env `RUNE_AERON_MTU`), `int32_t aeronRcvWindowBytes = 128*1024;` (env `RUNE_AERON_RCV_WINDOW`), parsed with the existing `parsePow2`/`parseInt32` helpers (mirror `aeronTermBufferBytes` at `NodeConfig.cpp:626`).

- [ ] **Step 1: Failing test** — env override sets the fields.

```cpp
TEST(NodeConfig, ParsesAeronMtuAndRcvWindow) {
    auto env = fakeEnv({{"RUNE_AERON_MTU","8192"},
                        {"RUNE_AERON_RCV_WINDOW","262144"}});
    NodeConfig cfg = NodeConfig::fromEnv(env);
    EXPECT_EQ(cfg.aeronMtuBytes, 8192);
    EXPECT_EQ(cfg.aeronRcvWindowBytes, 262144);
}
```

- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R NodeConfig -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — add fields + env parse; in `BrokerNode.cpp` after the threading-mode block call `aeron_driver_context_set_mtu_length(driverCtx_, cfg.aeronMtuBytes)` and `aeron_driver_context_set_initial_window_length(driverCtx_, cfg.aeronRcvWindowBytes)`. Keep defaults so small boxes are unaffected.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "NodeConfig|node_tests" -V`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(config): configurable Aeron MTU + rcv-window (RUNE_AERON_MTU/RUNE_AERON_RCV_WINDOW) (Gate2)"` + trailer.

### Task G0.3: Configurable fragment poll limit

**Files:**
- Modify: `libs/gateway/src/ClientGateway.cpp:144-145`, `libs/node/include/rune/node/NodeConfig.h`, `src/NodeConfig.cpp`
- Test: `libs/node/tests/NodeConfigTest.cpp`

**Interfaces:**
- Produces: `int32_t gatewayFragmentLimit = 256;` (env `RUNE_GATEWAY_FRAGMENT_LIMIT`), threaded into `ClientGateway` ctor; replaces the hard-coded `kFragmentLimit = 10`. This value is also carried into the per-worker ingress poll in Task E5.1.2.

- [ ] **Step 1: Failing test** — `NodeConfig` parses the limit; default is 256.
```cpp
TEST(NodeConfig, FragmentLimitDefaultAndOverride) {
    EXPECT_EQ(NodeConfig::fromEnv(emptyEnv()).gatewayFragmentLimit, 256);
    EXPECT_EQ(NodeConfig::fromEnv(fakeEnv({{"RUNE_GATEWAY_FRAGMENT_LIMIT","1000"}}))
                  .gatewayFragmentLimit, 1000);
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R NodeConfig -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — add field+env parse; pass into gateway; replace `kFragmentLimit`.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "NodeConfig|gateway" -V`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(gateway): configurable fragment poll limit (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256) (Gate2)"` + trailer.

### Task G0.4: Client batch instrumentation + linger/batch knobs

**Files (rune-client-cpp):**
- Modify: `include/rune/client/producer/Producer.h`, `src/producer/ProducerImpl.{h,cpp}`, `examples/latency_bench.cpp`
- Test: `tests/ProducerBatchingTest.cpp` (new)

**Interfaces:**
- Produces: `uint64_t Producer::framesSent() const`, `uint64_t Producer::recordsSent() const`; `ProducerConfig` reads `RUNE_LINGER_US` → `lingerUs`, `RUNE_BATCH_BYTES` → `batchBytes`, `RUNE_BATCH_RECORDS` → `batchRecords`. The bench prints `records_per_frame = recordsSent/framesSent`.

- [ ] **Step 1: Failing test** — sending N records with linger enabled coalesces to fewer frames; counters reflect it.
```cpp
TEST(ProducerBatching, CoalescesRecordsIntoFewerFrames) {
    auto cfg = ProducerConfig{}; cfg.lingerUs = 1000; cfg.batchRecords = 64;
    FakeTransport t; Producer p = makeProducer(t, cfg);
    for (int i = 0; i < 640; ++i) p.send(topic, key(i), payload1k());
    p.flush();
    EXPECT_EQ(p.recordsSent(), 640u);
    EXPECT_LT(p.framesSent(), 640u);            // coalesced
    EXPECT_GE(p.recordsSent() / p.framesSent(), 8u);
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R ProducerBatching -V` (in client repo). Expected: FAIL.
- [ ] **Step 3: Implement** — add the two counters (incremented in `flushBatches`/`encodeAndSend`), env parsing in `ProducerConfig::fromEnv`, and bench reporting.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "ProducerBatching|producer" -V`. Expected: PASS.
- [ ] **Step 5: Commit** (client repo) — `git commit -m "feat(client): batch instrumentation (frames/recordsSent) + RUNE_LINGER_US/BATCH_BYTES/BATCH_RECORDS (Gate2)"` + trailer.

---

## Phase G1 — Topology-independent hot-path eliminations

### Task G1.1: Cache `leaderView_()` and `minIsr_` per produce/cycle (batched micro-elim)

**Files:**
- Modify: `libs/gateway/src/ClientGateway.cpp` (`handleProduce` ~:983 + :1063; `doWork` top; :1417)
- Test: `libs/gateway/tests/ClientGatewayHotPathTest.cpp` (new)

**Interfaces:**
- Consumes: `leaderView_` (`std::function<LeaderView()>`), `minIsr_` (`std::atomic<int>`).
- Produces: internal only — one `LeaderView` capture per `handleProduce`; one `minIsr_` snapshot per `doWork` cycle (`cachedMinIsr_`). No API change.

- [ ] **Step 1: Failing test** — instrument a counting `leaderView_`; a single produce in the fallback path invokes it exactly once.
```cpp
TEST(ClientGatewayHotPath, LeaderViewCalledOncePerProduce) {
    int calls = 0;
    auto gw = makeGatewayNoRoutingImage([&]{ ++calls; return LeaderView{true, {}}; });
    gw.injectProduce(topic, part0, batch1k());
    gw.doWork(nowNs());
    EXPECT_EQ(calls, 1);        // was 2 before (983 + 1063)
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R ClientGatewayHotPath -V`. Expected: FAIL (calls == 2).
- [ ] **Step 3: Implement** — capture `const LeaderView lv = leaderView_ ? leaderView_() : LeaderView{true,{}}` once at the top of `handleProduce`, reuse for both checks; snapshot `cachedMinIsr_ = minIsr_.load(relaxed)` once at `doWork` start and read the snapshot in the acks=all path.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "ClientGatewayHotPath|gateway" -V`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "perf(gateway): cache leaderView + minIsr on produce path (remove per-record std::function + atomic)"` + trailer.

---

## Phase E5.1 — Per-worker ingress (the load-bearing change)

> This phase eliminates the produce funnel *and* the 2–3× batch copy. After it, a produce is decoded from the Aeron term buffer, offset-restamped in place, and appended by the owning worker on its own thread. The gateway keeps only the control plane.

> **Transport model (confirmed 2026-09-24): Aeron streamId-multiplexing over ONE UDP endpoint — NOT a UDP port per worker.** Each worker owns a distinct Aeron `streamId = workerIngressBaseStreamId + workerId` on the shared client channel and polls its own term buffer; the client publishes each partition straight to its owner's streamId. All streams share one media-driver `ReceiveChannelEndpoint`, so a single receiver thread reads the socket and demuxes frames into per-worker term buffers — this parallelizes the *application* ingress (poll/decode/restamp/append) across worker threads while the socket read stays one thread. That single receiver (multi-million frag/s) is not the bottleneck at the ≥500k/s target once it runs on its own thread via **`DEDICATED` mode (plan E5b, Task E5.2.1)**. **Escalation path (only if E5b's E5.5 scaling curve shows the receiver saturated):** switch to a UDP endpoint/port per worker for NIC-RSS / multi-receiver parallelism — deliberately deferred as a measured decision, not built now.

### Task E5.1.1: Protocol — add per-worker `ingressStreamId` to `PartitionEntry`

**Files:**
- Modify (rune-protocol): `PartitionEntry` schema + metadata-response version constant.
- Modify (broker): `libs/gateway/src/ClientGateway.cpp:3560-3622` (metadata builder).
- Test: `libs/gateway/tests/MetadataResponseTest.cpp` (new).

**Interfaces:**
- Produces: `PartitionEntry { int32_t topicId; int32_t partition; int32_t leaderId; int32_t leaderEpoch; int32_t ingressStreamId; }` (LE-encoded, appended field, version bumped). `ingressStreamId = config.workerIngressBaseStreamId + shardRouter.workerForPartition(topicId, partition)`.
- Consumes: `ShardRouter::workerForPartition` (`ShardRouter.h:67`), `workerIngressBaseStreamId` (new NodeConfig field, default `clientStreamId + 1`).

- [ ] **Step 1: Failing test** — metadata response round-trips `ingressStreamId` = base + workerId for a partition.
```cpp
TEST(MetadataResponse, CarriesPerWorkerIngressStreamId) {
    auto gw = makeGateway(/*workerCount*/4, /*baseStream*/1001);
    auto resp = decodeMetadata(gw.buildMetadata(topicId));
    int32_t wid = ShardRouter(4).workerForPartition(topicId, /*part*/3);
    EXPECT_EQ(entryFor(resp, topicId, 3).ingressStreamId, 1001 + wid);
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R MetadataResponse -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — add the field to `rune-protocol` (append-only, bump version), set it in the metadata builder from `ShardRouter` + base stream. Old clients ignoring the field still work (they publish to the gateway stream, which stays valid — see Task E5.1.4 fallback).
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "MetadataResponse|gateway|metadata" -V`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(protocol): PartitionEntry.ingressStreamId — per-worker ingress routing (E5.1)"` + trailer.

### Task E5.1.2: `WorkerIngress` — per-worker Aeron subscription + decode-restamp-append

**Files:**
- Create: `libs/node/include/rune/node/WorkerIngress.h`
- Modify: `libs/node/include/rune/node/Worker.h` (own + poll ingress; in-place restamp)
- Test: `libs/node/tests/WorkerIngressTest.cpp` (new)

**Interfaces:**
- Produces: `class WorkerIngress { WorkerIngress(std::shared_ptr<Aeron>, std::string channel, int32_t streamId); int poll(int fragmentLimit, Handler&& onFrame); };` where `onFrame` receives a **zero-copy** `(const uint8_t* frame, std::size_t len, aeron::header::Header&)` — decoded straight from the term buffer. `Handler` is a concrete templated functor, **not** `std::function` (no-virtual/no-type-erasure rule).
- Consumes: the same produce-frame layout `ClientGateway::onFragment` decodes (`ClientGateway.cpp:314-382`); `RecordBatchReader`; the worker's owned `Log`.
- Produces on `Worker`: `int Worker::pollIngress_(int fragmentLimit)` called in `doWork` before the SPSC drain; an in-place base-offset rewrite `restampBaseOffsetInPlace(uint8_t* batch, int64_t baseOffset)` operating on the term-buffer bytes (the wire frame is writable within the poll callback lifetime) — **no `restampScratch_`, no `WorkerOp` vector, no ring encode**.

- [ ] **Step 1: Failing test** — a produce frame published to worker *w*'s stream is decoded, offset-restamped, and appended by worker *w* with **zero** enqueues on the SPSC ingress ring.
```cpp
TEST(WorkerIngress, DecodesRestampsAppendsWithoutRingHop) {
    TestBroker b(/*workers*/2, /*baseStream*/2001);
    auto* w = b.worker(1);
    int64_t before = w->ingressRingWriteCount();   // instrumentation
    b.publishProduce(/*stream*/2001 + 1, topicOwnedByWorker1, part, batch1k(/*baseOffset*/0));
    b.pumpUntil([&]{ return w->log(topic, part)->highWatermark() >= 1; });
    EXPECT_EQ(w->log(topic, part)->recordCount(), rowsIn(batch1k()));
    EXPECT_EQ(w->log(topic, part)->baseOffsetOf(0), expectedRestampedOffset());
    EXPECT_EQ(w->ingressRingWriteCount(), before);  // NO SPSC hop for produce
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R WorkerIngress -V`. Expected: FAIL.
- [ ] **Step 3: Implement `WorkerIngress`** — wrap `aeron_->addSubscription(channel, streamId)` with bounded resolve (mirror `ClientGateway.cpp:85-99`); `poll` uses a `FragmentAssembler` bound to a template handler. No allocation in `poll`.
- [ ] **Step 4: Wire into `Worker`** — construct one `WorkerIngress` per worker (Task E5.1.3 passes channel+stream); in `doWork` (`Worker.h:297-348`) call `pollIngress_` before `drainIngress_`. The handler validates CRC (`RecordBatchReader::ensureValid`), restamps the base offset in place on the term-buffer view, and appends to the owned `Log` — reusing the existing append path. Keep the SPSC ingress ring for **control ops only** (SetOwnership etc.).
- [ ] **Step 5: Run tests** — `ctest --test-dir build-fast -R "WorkerIngress|worker" -V`. Expected: PASS.
- [ ] **Step 6: Commit** — `git commit -m "feat(node): WorkerIngress — per-worker Aeron subscription; decode-restamp-append zero-copy, no ring hop (E5.1)"` + trailer.

### Task E5.1.3: `WorkerPool`/`BrokerNode` — build per-worker ingress; retire produce fan-out

**Files:**
- Modify: `libs/node/include/rune/node/WorkerPool.h`, `libs/node/src/BrokerNode.cpp:557-603` (`buildWorkerPool`) + `:621-703` (`buildGateway`)
- Modify: `libs/gateway/src/ClientGateway.cpp:1430-1484` (produce fan-out removed / gated)
- Test: `libs/node/tests/WorkerPoolIngressTest.cpp` (new)

**Interfaces:**
- Consumes: `workerIngressBaseStreamId`, `config.clientEndpoint.channel`, worker count.
- Produces: each `Worker i` constructed with `channel` + `baseStreamId + i`; `ClientGateway` no longer calls `workerPool_->tryEnqueueOp` for produce (the produce branch becomes a legacy fallback used only when `routingImage_`/metadata indicates a client on the old single stream — see E5.1.4).

- [ ] **Step 1: Failing test** — after `buildWorkerPool`, worker *i* is subscribed to `baseStream + i`; a produce on `baseStream + i` lands on worker *i*'s log; a produce for a partition owned by worker *j* published to *i*'s stream is rejected/redirected (not misfiled).
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R WorkerPoolIngress -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — thread channel+base stream through `buildWorkerPool`; gate the gateway produce fan-out behind a `produceFanoutLegacy_` flag (default off once per-worker ingress is active). Preserve the ordering/return-drain path for control responses. **Close the shared-nothing race (G-A5):** move each shard's `Log` flush/sync (`ClientGateway::flushDirtyLogs → Log::sync`) OFF the gateway thread and ONTO the owning worker's `doWork` — the gateway must no longer reach into any worker-owned `Log`/`OffsetIndex`. After this, only the owning worker touches its shard's Log.
- [ ] **Step 4: Run tests + full parity** — `ctest --test-dir build-fast -R "WorkerPoolIngress|worker|node_tests|integration" -V`. Expected: PASS (all D tests green).
- [ ] **Step 5: Commit** — `git commit -m "feat(node): per-worker ingress subscriptions; gateway produce fan-out retired to legacy fallback (E5.1)"` + trailer.

### Task E5.1.4: Client — per-partition stream selection + REDIRECT keeps worker stream

**Files (rune-client-cpp):**
- Modify: `include/rune/client/transport/AeronTransport.{h,cpp}` (per-`(channel,streamId)` publication cache; `retarget` preserves stream), `src/producer/ProducerImpl.{h,cpp}` (select stream per partition from metadata).
- Test: `tests/ClientPerWorkerRoutingTest.cpp` (new).

**Interfaces:**
- Consumes: `PartitionEntry.ingressStreamId` from Task E5.1.1.
- Produces: `AeronTransport::offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len)` — resolves/caches an `ExclusivePublication` per `(currentChannel, ingressStreamId)`; `ProducerImpl::flushBatches` picks `ingressStreamId` from the partition's metadata entry (fallback: legacy single `ingressStreamId` when the field is absent).

- [ ] **Step 1: Failing test** — two partitions owned by different workers publish on different stream ids; a REDIRECT changes the channel but keeps each partition's `ingressStreamId`.
```cpp
TEST(ClientPerWorkerRouting, PublishesPerPartitionStreamAndPreservesOnRedirect) {
    FakeTransport t;
    auto p = makeProducerWithMetadata(t, /*p0->stream*/2001, /*p1->stream*/2002);
    p.send(topic, keyForPartition(0), payload1k()); p.flush();
    p.send(topic, keyForPartition(1), payload1k()); p.flush();
    EXPECT_EQ(t.lastStreamFor(partition0), 2001);
    EXPECT_EQ(t.lastStreamFor(partition1), 2002);
    p.onRedirect(partition0, "aeron:udp?endpoint=host2:42000");
    p.send(topic, keyForPartition(0), payload1k()); p.flush();
    EXPECT_EQ(t.lastChannelFor(partition0), "aeron:udp?endpoint=host2:42000");
    EXPECT_EQ(t.lastStreamFor(partition0), 2001);   // stream preserved
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R ClientPerWorkerRouting -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — publication cache keyed by `(channel, streamId)`; `flushBatches` selects the stream per partition; `retarget` rebuilds publications for the new channel preserving each stream id; legacy fallback when `ingressStreamId` absent.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "ClientPerWorkerRouting|producer|transport" -V`. Expected: PASS.
- [ ] **Step 5: Commit** (client repo) — `git commit -m "feat(client): per-partition ingress stream selection; REDIRECT preserves worker stream (E5.1)"` + trailer.

### Task E5.1.5: Shard-migration correctness under dynamic reassignment

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` (on `SetOwnership`, (un)subscribe the migrating shard's stream ownership as needed)
- Test: `libs/node/tests/integration/IngressMigrationTest.cpp` (new)

**Interfaces:**
- Consumes: existing `executeSetOwnershipWire_` (`Worker.h:544`), Milestone-D reassignment path.
- Produces: guarantee that when shard *s* moves worker *a*→*b*, in-flight produces are neither dropped nor duplicated (the client re-resolves metadata → new `ingressStreamId`; the old worker rejects/redirects late frames for un-owned partitions).

- [ ] **Step 1: Failing test** — migrate a partition mid-stream; assert exactly-once append of every produce (no gap, no dup) across the ownership change.
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R IngressMigration -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — on ownership loss, the ex-owner rejects produces for that partition with a retriable NotLeader/Redirect (client already refreshes metadata — `rune-client-cpp` commit `630b04a`); on ownership gain, the new owner's ingress stream already receives the client's re-routed frames. No cross-worker forwarding (shared-nothing).
- [ ] **Step 4: Run tests + full D parity + TSan once** — `ctest --test-dir build-fast -R "IngressMigration|integration|node_tests" -V`, then a TSan build of the migration + async-durability tests.
- [ ] **Step 5: Commit** — `git commit -m "test(node): shard-migration ingress correctness — no dropped/duplicated produce across reassignment (E5.1)"` + trailer.

### Task E5.1.6: E5 in-process integration + backpressure + shared-nothing suite

> Consolidated fast in-process coverage proving the E5 gates end-to-end BEFORE any Docker E2E (per the standing "integration green before E2E" rule). Real client + multi-worker broker in one process (reuse the `GatewayLoopbackFixture` embedded-IPC-driver pattern).

**Files:**
- Test: `libs/node/tests/integration/E5IngressIntegrationTest.cpp` (new), `libs/node/tests/integration/E5BackpressureTest.cpp` (new)

**Interfaces:**
- Consumes: the full per-worker-ingress stack (E5.1.1–E5.1.5) + client per-partition routing (E5.1.4).
- Produces: no product code — this is the E5a integration gate.

- [ ] **Step 1: Per-worker ingress end-to-end** — real client produces across many keys → each partition routed to its owning worker's stream → appended → ack returns; assert every record acked exactly once, correct offsets, correct partition→worker mapping.
- [ ] **Step 2: Throughput-scaling-vs-worker-count (G-A2 regression-of-record)** — run the same produce load at workerCount ∈ {1,2,4} on the 4-core box; assert measured throughput *increases* with worker count (not flat). Records the curve; a flat curve FAILS (means a residual funnel). (Absolute 500k is E5b/big-box; here we assert the scaling *shape*.)
- [ ] **Step 3: Batched-produce correctness** — with `RUNE_LINGER_US`/`RUNE_BATCH_RECORDS` set, produce N records; assert coalescing (records_per_frame > 1), all records durably appended in order, one ack per batch.
- [ ] **Step 4: End-to-end backpressure / no-ack-loss (G-A6)** — block the client egress (unresolvable/stalled egress publication), sustain produces; assert (a) the producer's `offer()` eventually returns `BACK_PRESSURED` (backpressure reached ingress), (b) no record is appended without an owed ack held, (c) on unblock EVERY ack is delivered exactly once (zero lost, zero duplicated). This is the G-A6 proof.
- [ ] **Step 5: Single-owner-Log TSan (G-A5)** — TSan build; run the ingress end-to-end + migration tests; assert ZERO data races (proves only the owning worker touches its shard's Log).
- [ ] **Step 6: Run + commit** — `ctest --test-dir build-fast -R "E5Ingress|E5Backpressure|integration"`, then the TSan subset; full suite green. Commit: `git commit -m "test(node): E5 in-process integration — per-worker ingress, scaling, batching, backpressure (G-A6), single-owner TSan (G-A5)"` + trailer.

**Exit:** all E5a acceptance gates (G-A1..G-A3, G-A5, G-A6) proven in-process on the 4-core box. G-A4 (≥500k/<1ms) remains E5b/big-box.

---

## Self-Review

- **Spec coverage:** E5 item 1 (per-worker ingress) → E5.1.1–E5.1.5. Prerequisites (reliable egress, MTU/window, fragment limit, client batching) → G0.1–G0.4. Copy-elimination (audit #1/#2) → realized by E5.1.2 (decode-restamp-append in place). Micro-eliminations (audit #3/#5) → G1.1. Items 2–4 (DEDICATED, affinity, consensus split) → deferred to plan **E5b** (they need the ≥16-core box to validate). ✅
- **Placeholder scan:** none — every step has a command, assertion, or concrete signature. Where full C++ bodies depend on code read at implementation time (E5.1.2/3/5), exact signatures, anchors, and test assertions are given. ✅
- **Type consistency:** `ingressStreamId` name is consistent across protocol (E5.1.1), broker (E5.1.3), client (E5.1.4); `sendRespReliable`/`drainPendingResponses` consistent (G0.1); `gatewayFragmentLimit`/`RUNE_GATEWAY_FRAGMENT_LIMIT` consistent (G0.3, reused in E5.1.2). ✅
- **Ordering:** G0 (correctness/measurability) → G1 (safe micro-wins) → E5.1 (structural). Each phase leaves the broker working and D-parity green.
