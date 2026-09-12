# Rune Client — Milestone B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development — execute task-by-task, fresh implementer per ticket, Opus task-review (spec + quality + coverage) to CLEAN after each, full-area Opus review at the end of each epic.

**Goal:** Produce → push-subscribe through the ClientGateway into the real engine, with a three-repo split (protocol codecs, broker gateway↔engine wiring, C++ client producer/subscribe), proven E2E against the dockerized cluster.

**Architecture:** The client sends `Produce{topicId, partition|key, RecordBatch}` and `Subscribe{topicId, partition|key, fromOffset, isolation}` over the existing Aeron session; the gateway resolves the partition and drives the engine's per-partition `Log`/`PartitionApi` (append for produce; tail-follow + egress-paced push for subscribe). Record *values* are opaque bytes (SBE/whatever the app chooses); the broker never parses them. Single-node engine path first; cross-node proxy-to-partition-leader + real REDIRECT added as a follow-on within this milestone.

**Tech Stack:** C++23, Aeron (embedded driver + `aeron:udp`), Conan2/CMake, gtest, google-benchmark. Repos: `rune-protocol` (codecs), `rune-broker` (gateway+engine+docker), `rune-client-cpp` (client). Spec: `docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md`.

## Global Constraints
- **Aeron/Agrona is the bible:** single-writer `doWork(nowNs)` duty cycle; NO busy-spin (IdleStrategy / bounded pumps + ASSERT); `tryClaim` zero-copy egress; FragmentAssembler ingress; reusable zero-alloc codecs; release/acquire fences; injected clocks in tests.
- **All little-endian** wire framing; record *values* are opaque bytes (broker never parses them).
- **Addressing:** `topicId`(few) + `partition`(hash(key)%N) + binary `Key`(unbounded); keys are opaque bytes (no strings); hot path carries `topicId·partition·keyBytes·value`.
- **Producer:** Tier-1 auto-batch (linger/batchBytes/maxInFlight) + Tier-2 atomic same-partition batch; **idempotent on** (producerId+epoch+sequence, P5-1); `acks` = `None`/`Leader`(default)/`All`; idempotent ⇒ `acks=All` implicitly. **Tier-3 transactions + consumer groups are Milestone C — do NOT build here.**
- **Subscribe = push**, broker-paced against client egress back-pressure (`offer` BACK_PRESSURED → pause + resume from tracked offset); `fromOffset` + isolation (`read_uncommitted`≤HW / `read_committed`≤LSO-or-HW).
- **templateId ranges:** control 1–15 (done) · **produce/consume 16–31 (this milestone)** · groups/txn 32–63 · views 64–95.
- Process: Opus reviewers + Sonnet implementers; Sonnet-introduced regression caught by Opus → Opus fixes; after fixing an area, FULL-area from-scratch re-review to CLEAN; every ticket ships exhaustive edge-case unit tests + a benchmark + integration/conformance. NEVER `conan create/export/upload`. `-j2`. Commit locally on `main`; do NOT push. Update the live dashboard + Planka per ticket.

---

## Sub-plan 1 — produce/subscribe protocol codecs (`rune-protocol`) (epic **PB**)

### PB-1 — Produce / ProduceResponse codecs
- **Files:** `include/rune/protocol/client/DataMessages.h` (+ conformance vectors). `Produce` (tid16): `topicId:i32`, `partition:i32`, `partitionKnown:u8` (0 ⇒ resolve from key), `key:var-bytes`, `recordBatch:var-bytes` (opaque RecordBatch blob). `ProduceResponse` (tid17): `errorCode:i16`, `partition:i32`, `baseOffset:i64`.
- **Acceptance:** encode/decode round-trip + bounds/truncation/empty-key/empty-batch/max edge tests; 2 golden conformance vectors (produce with explicit partition; produce with key-only). Benchmark encode/decode.

### PB-2 — Subscribe / SubscribeResponse / Records / Unsubscribe codecs
- **Files:** extend `DataMessages.h` + vectors. `Subscribe` (tid18): `topicId:i32`, `partition:i32`, `partitionKnown:u8`, `key:var-bytes`, `fromOffset:i64`, `isolation:u8`. `SubscribeResponse` (tid19): `errorCode:i16`, `subscriptionId:i64`, `partition:i32`. `Records` (tid20, push): `subscriptionId:i64`, `topicId:i32`, `partition:i32`, `baseOffset:i64`, `recordBatch:var-bytes`. `Unsubscribe` (tid21): `subscriptionId:i64`.
- **Acceptance:** round-trip + edge tests (fromOffset extrema, isolation values, empty batch push); 4 golden conformance vectors. Benchmark.

### PB-3 — Dispatch/versioning + conformance harness extension
- **Files:** update `Dispatch.h` (`isKnownTemplate` now 1–21; version guard unchanged) + `conformance/manifest.md` + `tests/protocol/client/ConformanceTest.cpp` to cover the 6 new vectors both-direction + a produce→records session-flow script.
- **Acceptance:** all client codec tests green incl. the new templates; unknown-template (e.g. 99) still → clean ErrorResponse; full rune-protocol suite green.

---

## Sub-plan 2 — broker gateway ↔ engine (`rune-broker`) (epic **GB**)

### GB-1 — TopicRegistry + static topic/partition bootstrap
- **Files:** `libs/gateway/include/rune/gateway/TopicRegistry.h` (+ wiring in `BrokerNode`). Env `RUNE_TOPICS` (e.g. `inventory:8,entities:4` = name:partitionCount), parsed in `NodeConfig`; `topicId` assigned deterministically (index order). `TopicRegistry` maps `topicId → {partitionCount, per-partition Log/PartitionApi handle}` built over the Phase-1 storage engine (per-partition `Log` under `dataDir/topics/<topic>/<partition>`). `partitionForKey(topicId, keyBytes) = xxhash(key) % partitionCount`.
- **Acceptance:** parse/validate config (bad specs rejected); registry creates the partition logs; `partitionForKey` stable + uniform; unit tests. Keep BR suite green.

### GB-2 — Gateway Produce handler → engine append (leader-local)
- **Files:** `ClientGateway` produce dispatch. On `Produce`: resolve `(topicId, partition)` (explicit, or `partitionForKey`); append the RecordBatch to that partition's `PartitionApi`/`Log` (leader-local for now); reply `ProduceResponse{baseOffset}` (or `errorCode` for unknown topic/partition). Respect single-writer (append on the duty-cycle thread) + zero-alloc scratch. `acks`: for M-B leader-local, `Leader`/`None` ack after append; `All` waits for HW (P3) — implement `Leader` + `None`, stub `All`→`Leader` with a tracked TODO if replication-ack wiring is deferred.
- **Acceptance:** IPC-loopback test — a produced RecordBatch lands in the log at the returned baseOffset; unknown topic/partition → errorCode; malformed produce dropped safe; multiple produces to a key are ordered in one partition. Bounded, unique temp dirs.

### GB-3 — Gateway Subscribe handler → egress-paced push
- **Files:** `ClientGateway` subscribe dispatch + a per-session subscription registry. On `Subscribe`: allocate `subscriptionId`; reply `SubscribeResponse`; register `{subscriptionId, topicId, partition, nextOffset=fromOffset, isolation}`. Each `doWork`: for each active subscription, read new records from the partition `Log` beyond `nextOffset` up to the isolation bound (`read_uncommitted`≤LEO/HW; `read_committed`≤LSO-or-HW), encode `Records`, push on the session egress via `tryClaim`; on BACK_PRESSURED → **pause** (do not advance `nextOffset`, retry next cycle). `Unsubscribe` removes it. Bounded per-cycle read (fairness across subscriptions); single-writer.
- **Acceptance:** IPC-loopback — subscribe from offset 0, produce N, receive N records in order via push; `fromOffset` mid-stream; back-pressure (tiny egress term) → delivery pauses + resumes from the exact offset (no loss/dup); unsubscribe stops delivery; two subscriptions no cross-talk. Injected-clock/bounded, no busy-spin.

### GB-4 — Cross-node proxy-to-partition-leader + real REDIRECT (multi-node follow-on)
- **Files:** per-peer client endpoints in `NodeConfig`/`RUNE_PEERS` (add client port); gateway routes a data op whose partition leader is a DIFFERENT node → either proxy internally to that node OR reply `ErrorResponse{Redirect, leaderClientEndpoint}` (choose proxy for produce/subscribe transparency; document). Partition→leader derives from consensus. Wire the client's `retarget` to follow a data-op REDIRECT (activates the CC-4/CC-6 redirect path for real).
- **Acceptance:** 3-node: produce to a partition whose leader ≠ the connected node succeeds (proxied or redirect-followed); after leader-kill the client's produce/subscribe recovers to the new leader. (Deterministic where possible; the full failover proof is the CB-5 dockerized E2E.)

### GB-5 — Gateway produce/subscribe benchmarks + full-area Opus review
- **Files:** `bench/GatewayDataBench.cpp` (produce latency; subscribe delivery latency; batch throughput). Then a **full-area Opus review** of the produce/subscribe gateway + TopicRegistry + engine wiring; fix loop to CLEAN.
- **Acceptance:** latencies captured; review CLEAN (single-writer, no busy-spin, back-pressure correct, no offset loss/dup, bounded).

---

## Sub-plan 3 — client producer + subscribe (`rune-client-cpp`) (epic **CB**)

### CB-1 — client produce/subscribe codec adapters + conformance
- **Files:** `include/rune/client/codec/DataCodec.h` — thin adapters over the PB codecs (Produce/ProduceResponse/Subscribe/SubscribeResponse/Records/Unsubscribe); run the PB conformance vectors in the client build.
- **Acceptance:** all 6 PB golden vectors pass both directions in the client; adapters thin (wrap rune-protocol, no wire re-impl).

### CB-2 — Producer Tier 1 (auto-batch, idempotent, acks) + topic handles + Key
- **Files:** `include/rune/client/producer/Producer.h` + `Topic`/`Key`. `client.topic(name)→TopicHandle` (name→topicId, resolved once via a metadata exchange OR static config — start static/config-driven, TODO dynamic). `Producer` with `{acks, linger, batchBytes, maxInFlight}`. `send(topic, Key, value)` accumulates into a per-`(topic,partition)` `RecordBatchBuilder` (partition = `hash(key)%N` client-side using the topic's partition count); background flush on linger/batchBytes; `flush()`; per-send `void` / `callback(ack)` / `want_future→future<int64_t baseOffset>`. Idempotent seq (producerId+epoch+sequence) — obtain producerId via InitProducerId or a stub for M-B (document). Submitted via the agent ring; ProduceResponse routed back to resolve the ack/future by correlationId.
- **Acceptance:** fake-gateway + IPC tests — Tier-1 send auto-batches (linger/batchBytes triggers), future resolves with baseOffset, callback fires, `acks=None` returns without waiting; per-key ordering preserved; maxInFlight bounds outstanding. Binary values (NUL+0xFF), empty value.

### CB-3 — Producer Tier 2 (explicit atomic batch)
- **Files:** `p.batch(topic, Key)` → a `BatchBuilder`; `add(value)*`; `send()→future<int64_t base>` = one RecordBatch to one partition (atomic single append). Shares the Tier-1 encode/submit path.
- **Acceptance:** a Tier-2 batch of K records lands as one RecordBatch at a contiguous baseOffset..base+K-1; empty-batch rejected; wrong-usage (add after send) guarded. Tests.

### CB-4 — Subscribe (push handler + offset tracking + back-pressure + unsubscribe)
- **Files:** `client.subscribe(topic, Key|partition, {from, isolation}, handler)→Subscription`. Sends `Subscribe`, stores the `subscriptionId`, and dispatches pushed `Records` to the zero-copy handler on the agent thread; tracks `lastOffset` per subscription (for resume); `request(n)`/demand default = broker-paced push; `unsubscribe()`. Resume on reconnect re-presents `subscriptionId`+lastOffset.
- **Acceptance:** fake-gateway/IPC — subscribe from Latest/Earliest/offset; handler receives pushed records in order; offset tracked; unsubscribe stops; reconnect resumes from lastOffset (scaffold vs GB engine). Bounded, zero-copy handler validity documented.

### CB-5 — E2E vs dockerized cluster + producer/subscribe benchmarks + full-area review
- **Files:** `examples/rune_produce_subscribe.cpp` + `docker/e2e-data.sh` (reuse the 3-node compose): client connects, produces N to a key, subscribes and receives them; then leader-kill → client recovers (via GB-4 proxy/redirect) → further produce/subscribe still works; no committed-record loss. `bench/ClientProduceBench.cpp` (produce latency; subscribe delivery latency; batch throughput). Then a **full-area Opus review** of the whole client producer+subscribe surface; fix loop to CLEAN.
- **Acceptance:** E2E green (produce→subscribe across the cluster + survives leader-kill); benchmarks captured; review CLEAN.

---

## MB-DONE — Milestone B acceptance
- **Acceptance:** `compose up` 3 nodes + the client example → produce→push-subscribe across the cluster, leader-failover from the client's POV (no committed-record loss, ordered per key). Write the Milestone B acceptance doc; confirm all epic full-area reviews CLEAN; broker + client + protocol suites green.

---

## Self-review (spec coverage)
- §4.3 Produce/Subscribe ops → PB-1/PB-2, GB-2/GB-3, CB-2/CB-3/CB-4. ✓
- §5 addressing (topic/key/partition, opaque binary keys) → GB-1 (`partitionForKey`), CB-2 (`Key`, client-side partitioning). ✓
- §6.4/§7 producer tiers 1+2, acks, idempotence, auto-batch → CB-2/CB-3. Tier-3 txn deferred to C (per scope decision). ✓
- §4.3 push subscribe + back-pressure + isolation → GB-3, CB-4. ✓
- §3.2 data-ops proxy to leader + REDIRECT → GB-4 (single-node-first, then proxy per scope decision). ✓
- §8 testing (unit, conformance, IPC integration, cluster E2E w/ leader-failover, benchmarks) → each ticket + CB-5/MB-DONE. ✓
- Deferred (consumer groups §Milestone C; transactions; views §9) → explicitly out of scope. ✓
