# Rune Broker — Back-Pressure Workflow Audit

**Author:** R5 audit (2026-09-24)  
**Milestone:** E (post-R1–R4 ring refactor + E3 async durability)  
**Status:** audit complete; findings listed in §Findings

---

## Purpose

This document audits the back-pressure behaviour of **every cross-thread ring** in the
Rune broker, traces the **client-observable contract** under each back-pressure condition,
and records **divergences** from the spec.

Spec references:
- `docs/superpowers/plans/2026-09-23-rune-cross-thread-onetoone-rings.md` §Task R5
- `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md` §"Back-pressure, not blocking"
- `[[rune-shared-nothing-law]]` — no shared state; all cross-thread via rings
- `libs/protocol/…/Protocol.h` — `pc::ErrorCode` (the client wire protocol)

---

## Ring Table — Back-Pressure Audit

### 1. Worker Ingress Ring (`ingress_` in `Worker.h`)

| Property | Value |
|---|---|
| Type | `OneToOneByteRing` (agrona SPSC) |
| Producer | Control thread (gateway `handleProduce`, transport `sendFetchResponse`, bridge `BecomeLeader/Follower`, `WorkerPool::setOwnership`) |
| Consumer | Worker thread (`drainIngress_` in `doWork`) |
| Capacity | `Worker::kIngressByteCapacity` = 8 MiB; `maxMsgLength` = 1 MiB |
| NodeConfig knob | `workerRingCapacity` (TODO R4: not yet threaded to Worker ctor — uses hardcoded constant) |
| Full policy | `write()` returns `false` → caller (`tryEnqueueOp`) returns `false` |
| Data class | **Commit-critical** for Produce; retriable for role/ownership changes |

**Full condition propagation (Produce path):**

```
handleProduce → tryEnqueueOp returns false
→ ClientGateway sends ProduceResponse{Internal(6), partition, 0}
```

Code: `ClientGateway.cpp:1477–1481`:
```cpp
if (!workerPool_->tryEnqueueOp(...)) {
    spdlog::warn("...worker ring full...");
    sendResp(pc::ErrorCode::Internal, partition, 0);
}
```

**Spec conformance:** The spec states "commit-critical MUST back-pressure the source; never block". The ring correctly returns false (non-blocking). However, the gateway converts full-ring to `Internal(6)` and sends it immediately — the produce is **not retried**. The spec says "hold and retry next cycle". This is **FINDING F1** (see §Findings).

---

### 2. Worker Return Ring (`ret_` in `Worker.h`)

| Property | Value |
|---|---|
| Type | `OneToOneByteRing` (agrona SPSC) |
| Producer | Worker thread (`enqueueReturn_` in `executeProduceWire_`, `executePushReadWire_`) |
| Consumer | Control thread (`drainReturn` / `drainWorkerReturns_`) |
| Capacity | `Worker::kReturnByteCapacity` = 8 MiB; `maxMsgLength` = 1 MiB |
| NodeConfig knob | Not yet exposed (TODO comment in Worker.h) |
| Full policy | `write()` returns `false` → **silent drop** with `spdlog::warn` |
| Data class | Best-effort acks (safe for acks=Leader); but also carries acks=All deferred acks — **ack may be lost** |

**Full condition propagation:**

When `ret_` is full, `enqueueReturn_` emits a `spdlog::warn` and the ack is silently
dropped. The gateway never receives the `WorkerReturn` and never sends a `ProduceResponse`
to the client. The client's request hangs until its own timeout fires.

Code: `Worker.h:374–378`:
```cpp
if (!ret_.write(returnScratch_.data(), n, workerReturnMsgTypeId(ret))) {
    spdlog::warn("Worker {}: ret_ ring full — WorkerReturn type={} dropped", ...);
}
```

**Spec conformance:** The thread-per-core spec §"Back-pressure, not blocking" does not
explicitly address the return ring. The comment in Worker.h acknowledges:
> "TODO(R5): add retry or back-pressure propagation."

This is **FINDING F2** (see §Findings). For acks=All the lost ack is particularly
problematic: the client never learns the produce was committed.

---

### 3. IoBackend Submit Ring (`submitRing_` in `PortableFlushBackend.h`)

| Property | Value |
|---|---|
| Type | `OneToOneByteRing` (agrona SPSC) |
| Producer | Worker thread (`io_.submit(...)` in `submitCoalescedFlushes_`) |
| Consumer | Flusher thread (`submitRing_.drain(…)` in `flusherLoop_`) |
| Capacity | Derived from `ioSubmitDepth` (default 1024 messages); `inFlight_` depth guard |
| NodeConfig knob | `ioSubmitRingDepth` (default 1024); passed to `Worker` ctor |
| Full policy | `submit()` returns `kNoToken` when `inFlight_ >= submitDepth_` |
| Data class | **Strict-tier durability** — commit-critical for Strict produces |

**Full condition propagation:**

```
submitCoalescedFlushes_: io_.submit returns kNoToken
→ for every durablePending_ entry (token==0) for this log:
    entry.ret.errorCode = Internal(6)
    enqueueReturn_(entry.ret)   // fires ack immediately with Internal
```

Code: `Worker.h:410–419`. The back-pressure ack is sent as `Internal(6)` immediately —
the produce is **not retried**.

**Spec conformance:** The spec says "Submission ring / mailbox full → back-pressure
(hold new durable produces / defer the cross-worker send), never block." The current
implementation **acks Internal immediately** instead of holding and retrying. This is
**FINDING F1b** — same fundamental issue as F1 (immediate error vs hold-and-retry),
but on the IoBackend path.

For the macOS test harness (`PortableFlushBackend`), this path is well-tested and
correctly fires Internal for the back-pressured produces (never false-OK). Spec
conformance for the "never false-OK" invariant: ✓

---

### 4. IoBackend Completion Ring (`completionRing_` in `PortableFlushBackend.h`)

| Property | Value |
|---|---|
| Type | `OneToOneByteRing` (agrona SPSC) |
| Producer | Flusher thread (`completionRing_.write(…)` in `flusherLoop_`) |
| Consumer | Worker thread (`io_.reap(…)` in `reapDurableCompletions_`) |
| Capacity | Derived from `ioCompletionDepth` (default 1024); flusher spin-waits if full |
| NodeConfig knob | `ioCompletionRingDepth` (default 1024) |
| Full policy | Flusher **spin-loops** on `completionRing_.write(…)` until space is available |
| Data class | Completion tokens — drives deferred acks |

Code: `PortableFlushBackend.cpp:116–119`:
```cpp
while (!completionRing_.write(compBuf, sizeof(FlushCompletion), 1)) {
    std::this_thread::yield();
}
```

**Spec conformance:** The spec says "flusher spins on full (off worker path)". ✓
The flusher is on its own dedicated thread; spin-waiting here does NOT block the worker.
This is correct behavior. No finding.

**Note for io_uring path (Linux):** `UringFlushBackend` uses the CQE ring directly;
the completion ring full case is managed by the kernel. On macOS
`PortableFlushBackend` is used; the test harness confirms correctness.

---

### 5. Transport Outbound Ring (`outboundRing_` in `AeronPartitionTransport.h`)

| Property | Value |
|---|---|
| Type | `IngressRing` (agrona MPSC `ManyToOneRingBuffer`) |
| Producer | Worker threads (`enqueueOutbound` from `sendFetchRequest`/`sendFetchResponse`) |
| Consumer | Control thread (`drainOutbound` in `poll()`) |
| Capacity | `NodeConfig::replicationOutboundRingBytes` (default 4 MiB) |
| NodeConfig knob | `replicationOutboundRingBytes` (env: `RUNE_REPL_OUTBOUND_RING_BYTES`) |
| Full policy | `enqueueOutbound` returns `false` → increments `outboundRingFullCount_` → send **skipped** |
| Data class | Replication (retriable) — follower re-requests, leader re-responds next cycle |

Code: `AeronPartitionTransport.cpp:220–222`:
```cpp
if (!outboundRing_.write(framed, kOutboundHeaderSize + len)) {
    ++outboundRingFullCount_;  // back-pressure: skipped (retriable)
}
```

**Spec conformance:** The spec says "full → skip+retry (observed), never silent".
The skip is logged only via a counter (`outboundRingFullCount_`) — no per-skip
spdlog::warn. The counter is readable for observability. ✓ for correctness (retriable,
no committed loss). This is **FINDING F3** for observability (counter-only, no rate-limited
warn on sustained pressure).

---

### 6. Consensus Ingress Ring (agrona `ManyToOneRingBuffer` via `IngressRing`)

| Property | Value |
|---|---|
| Type | `IngressRing` (agrona MPSC `ManyToOneRingBuffer`) |
| Producer | Network receiver threads (Aeron fragment handlers) |
| Consumer | Consensus agent thread |
| Capacity | `kDefaultIngressCapacity` = 64 KiB |
| Full policy | `write()` returns `false` → consensus frame dropped |
| Data class | Consensus frames — **faithful Aeron replication** |

The consensus ring follows the faithful-Aeron model (Milestone B). Dropped frames trigger
retransmit at the consensus level (the Aeron Cluster model). This matches the
[[aeron-is-the-bible]] constraint. No finding.

---

### 7. Log Cleaner SPSC Rings (`Log.h`)

Three SPSC rings: swap, delete, staging-rename-done.

| Ring | Producer | Consumer | Full policy | Data class |
|---|---|---|---|---|
| `cleanerSwapRing_` (`enqueueCleanedSegment`) | LogCleanerAgent thread | Append/duty-cycle thread (`drainCleanerSwaps`) | Returns `false` → cleaner retries next cycle (idempotent) | Compaction results |
| `retentionDeleteRing_` (`enqueueDeletedSegment`) | LogCleanerAgent thread | Append/duty-cycle thread (`drainRetentionDeletes`) | Returns `false` → cleaner retries next cycle (idempotent) | Retention deletes |
| `renameDoneRing_` (`drainStagingRenamesDone`) | Background provisioner | Append thread | Background yields | Staging renames |

**Spec conformance:** "full → don't advance watermark, retry idempotently" ✓

All three are correctly implemented: producers drop on full (idempotent), consumer retries on next cycle. No finding.

---

### 8. InMemoryLog Per-Partition Ring

| Property | Value |
|---|---|
| Type | Single-owner circular byte buffer (no atomics; owned by one worker thread) |
| Producer | Worker thread (via `appendNoSync`) |
| Consumer | (Same worker) — storage only, no cross-thread ring |
| Capacity | `NodeConfig::memoryLogRingBytes` (default 8 MiB), `maxRecords` (configurable per topic) |
| NodeConfig knob | `memoryLogRingBytes` |
| Full policy | `DropOldest` (default): evict oldest batch to make room. `BackPressure` (opt-in): `appendNoSync` returns `-1` |
| Data class | Memory-topic data |

**Spec conformance (DropOldest):** ✓ — the spec states memory topics back-pressure when
full (per the 2026-09-23 decision), but the NodeConfig default is `DropOldest` (via
`TopicSpec::persistence = Memory` with no explicit `backPressurePolicy` field). This is
**FINDING F5** — the spec mandates BackPressure for memory topics; the default is
DropOldest (drops oldest data without telling the producer).

**Spec conformance (BackPressure mode):** The InMemoryLog correctly returns `-1` on
full. However, the Worker's `executeProduceWire_` does NOT check the return value of
`appendNoSync()` for `-1` — it always enqueues a `WorkerReturn{errorCode=0}`. This is
**FINDING F6** — a false-OK ack when BackPressure InMemoryLog is full.

---

## Section B: Client's Perspective Under Back-Pressure

### B.1: Worker Ingress Ring Full

**Client experience today:**
`ProduceResponse{errorCode=Internal(6), partition, baseOffset=0}`

The client receives `Internal(6)` immediately. This is an **opaque error code** — the
client cannot distinguish "broker internal error" from "transient ring back-pressure".
The produce was **not committed** (the batch was never appended); the client should retry.

**What the spec/protocol says:**
The protocol has no `Throttled` or `TryAgain` error code (cf. Kafka's `ErrorCode.THROTTLING_QUOTA_EXCEEDED` / `ErrorCode.REQUEST_TIMED_OUT`). The Rune `ErrorCode` enum has 21 values; none are explicitly "retriable due to back-pressure". `Internal(6)` is the closest available code.

**Finding F1 (detailed):** The correct behavior per the spec is "hold and retry next
cycle" — the gateway should not ACK at all when the ring is full, but instead buffer
the in-flight produce and retry on the next doWork() cycle. `Internal(6)` today signals
"something went wrong", which client SDKs will typically retry, so correctness is
preserved; however, it is NOT the right semantic (it signals an error, not transient
back-pressure). Adding a `Throttled` / `TryAgain` error code to the protocol (separate
from the existing `ErrorCode::Internal`) would allow client SDKs to retry immediately
with backoff rather than escalating to a user-visible error.

This is a **protocol change** — deferred to a follow-up ticket (see §Findings F1).

---

### B.2: IoBackend Submit Ring Full (Strict Tier)

**Client experience today:**
`ProduceResponse{errorCode=Internal(6), partition, 0}` — same as B.1.

The produce was committed to the in-memory log (appended) but the async fsync could not
be submitted. The worker acks Internal immediately via `submitCoalescedFlushes_`. This
means the produce is on disk (page-cache write succeeded) but fsync was not submitted.

**Concern:** the produce IS on disk (page-cache) so it will typically survive unless
the OS crashes. But the contract for Strict-tier is "ack only after fsync completion" —
so `Internal(6)` is the correct response (the batch is in page-cache but not durable).
The client should retry; on retry the batch will re-produce (idempotent producer dedup
handles duplicates).

**What the spec says:** The spec says "hold new durable produces". The current impl fires
Internal immediately and the pending-ack is erased. This is FINDING F1b.

---

### B.3: InMemoryLog Full (BackPressure mode — when configured)

**Client experience today:**
`ProduceResponse{errorCode=0 (OK), partition, baseOffset}` — **FALSE OK** (see F6).

The InMemoryLog returned `-1` (ring full, BackPressure policy) but the Worker acks OK
because `executeProduceWire_` does not check the return value of `appendNoSync`.

**What the spec says:** "full → produce back-pressures" (thread-per-core spec). The
spec requires an error ack, not a false OK. This is **FINDING F6** — a correctness
violation.

---

### B.4: Worker Return Ring Full

**Client experience today:**
The `WorkerReturn` is silently dropped on the worker. The gateway never receives it.
The client's `ProduceResponse` is **never sent**. The client waits until its own
timeout (application-layer timeout). No explicit error code is received.

**What the spec says:** The spec does not explicitly cover this case (it is the ack
delivery path, not the produce path). The behavior (silent drop → client timeout) is
observable; the gateway has no way to detect it from the control thread.

This is **FINDING F2** — a silent ack loss that causes client timeout rather than
an explicit error.

---

### B.5: Transport Outbound Ring Full (Replication)

**Client experience:** No direct impact on the producer's ProduceResponse. The
replication send is skipped; the follower re-requests next cycle (ISR lag tolerates
a few skipped cycles). If the ring is consistently full, the follower falls out of
ISR and `acks=All` produces are rejected with `MinIsrViolation`.

This is the correct retriable behavior.

---

## Section C: Test Coverage Summary

| Ring / Behavior | Test Name | File | Status |
|---|---|---|---|
| Worker ingress full → `tryEnqueueOp` returns `false` | `BackPressure.WorkerIngressFullReturnsFalseNoFalseOK` | `AsyncDurabilityTest.cpp` | **Added (R5)** |
| IoBackend submit ring full → Internal never false-OK | `AsyncDurability.SubmitRingFullHoldsProduce` | `AsyncDurabilityTest.cpp` | Exists (E3) |
| IoBackend submit bad fd → non-OK errorCode (deterministic) | `AsyncDurability.FlushFailureErrorsTheAckNeverOK` | `AsyncDurabilityTest.cpp` | **Fixed (R5) — was flaky** |
| Worker return ring full → no crash, log correct | `BackPressure.WorkerReturnFullDoesNotCrashAndLogsWarn` | `AsyncDurabilityTest.cpp` | **Added (R5)** |
| InMemoryLog BackPressure — append returns -1 | `InMemoryLogRing.BackPressureModeRefusesAppendWhenFullReturnsSentinel` | `InMemoryLogRingTest.cpp` | Exists (unit) |
| InMemoryLog DropOldest — evicts without loss | `InMemoryLogRing.DropOldestEvictsWithoutMemmoveAndAdvancesLogStart` | `InMemoryLogRingTest.cpp` | Exists (unit) |
| InMemoryLog BackPressure integration | `BackPressure.InMemoryLogBackPressureModeRefusesWhenFull` | `AsyncDurabilityTest.cpp` | **Added (R5)** |
| Strict produce ack deferred until flush | `AsyncDurability.StrictProduceAcksOnlyAfterFlushCompletion` | `AsyncDurabilityTest.cpp` | Exists (E3) |
| Group-commit: N produces → 1 fsync | `AsyncDurability.GroupCommitCoalescesManyProducesIntoOneFlush` | `AsyncDurabilityTest.cpp` | Exists (E3) |
| Transport outbound ring full → replication skip+retry | *Not in-process testable (Aeron IPC) — see below* | — | Untestable in-process |
| End-to-end: client drives ingress to full, retries, no loss | *Not in-process testable without full BrokerNode + client session — see below* | — | Untestable in-process |

**Why some behaviours are untestable in-process:**

1. **Transport outbound ring full + retry convergence:** requires a real Aeron media
   driver and a follower `PartitionReplica` to verify replication convergence after
   ring-full skips. The `GatewayReplicationWiringTest` drives this indirectly but does
   not saturate the outbound ring. A dockerized E2E test is the appropriate harness
   (parked — see dr-e2e/).

2. **E2E client session through full worker ingress:** requires a running `BrokerNode`
   (Aeron media driver embedded) + a live client session + network produce loop. The
   `WorkerPoolParityTest.MultiWorkerBrokerReachesLeaderAndServes` is the closest but
   does not drive back-pressure. Deferred to a dockerized test.

---

## Section D: Findings / Divergences

### F1: Worker ingress ring full → gateway sends `Internal(6)` instead of holding and retrying

**Where:** `ClientGateway.cpp:1477–1481`  
**Spec requirement:** "hold and retry next cycle (produce is commit-critical)"  
**Current behavior:** `Internal(6)` sent immediately; produce dropped  
**Impact:** Client receives an opaque error code on transient ring pressure; retry is
client-driven (application timeout + SDK retry), not broker-driven.  
**Recommendation:** On ingress-ring-full, the gateway should buffer the produce in a
per-worker "hold queue" and retry `tryEnqueueOp` at the start of the next doWork cycle.
This is a moderate change (add hold-queue to `ClientGateway`; bounded by ring cap).  
**Protocol note:** A dedicated `Throttled` or `TryAgain` error code would signal
"safe to retry immediately" to client SDKs — a small protocol extension, non-breaking
(new enum value). Deferred; document as a follow-up ticket.

---

### F1b: IoBackend submit ring full → worker acks `Internal(6)` instead of holding and retrying

**Where:** `Worker.h:submitCoalescedFlushes_` (~line 410)  
**Spec requirement:** "Submission ring / mailbox full → back-pressure (hold new durable
produces), never block"  
**Current behavior:** `Internal(6)` acked immediately; produce entry erased from
`durablePending_`.  
**Impact:** A Strict-tier produce under sustained submit-ring pressure is repeatedly
acked Internal instead of completing after fsync. Idempotent producer dedup prevents
data duplication on retry, but the client sees spurious Internal errors.  
**Note:** The submit ring depth (1024) is very unlikely to fill in practice (one fsync
per dirty log per cycle; typically 1–10 logs). This fires only under extreme load or
very small configured depth. Low-priority fix.

---

### F2: Worker return ring full → ack silently dropped; client never receives ProduceResponse

**Where:** `Worker.h:enqueueReturn_` (~line 374)  
**Spec requirement:** Not explicitly specified; implied by "ack delivered eventually".  
**Current behavior:** `spdlog::warn` + silent drop; gateway never sends ProduceResponse.  
**Impact:** Client waits for its own timeout. For acks=All this could be a deferred-ack
that was committed but whose ack was lost — the client retries an already-committed
produce (idempotent producer dedup must handle this).  
**Recommendation:** Implement retry/hold in `enqueueReturn_`: if `ret_.write()` returns
false, push to a per-worker "hold list" and retry on the next doWork cycle. The list
is bounded by the ring capacity. Alternatively, spin briefly (bounded retries) on the
assumption the consumer drains the ring on the next doWork cycle.

---

### F3: Transport outbound ring full uses a counter only — no rate-limited log warning

**Where:** `AeronPartitionTransport.cpp:221`  
**Spec requirement:** "never silent"  
**Current behavior:** `outboundRingFullCount_` incremented; no `spdlog::warn`.  
**Impact:** Operators cannot observe sustained replication back-pressure without
external metrics. Low-priority (counter is accessible for metrics scraping).  
**Recommendation:** Add a rate-limited `spdlog::warn` when `outboundRingFullCount_`
crosses thresholds (e.g., warn when first hit, then every 1000 drops). Small localized
change.

---

### F4: NodeConfig knobs for worker ingress/return ring byte capacity not threaded to Worker constructor

**Where:** `Worker.h` TODO comments (lines 66, 108); `NodeConfig.h` `workerRingCapacity`  
**Spec requirement:** "Ring sizes from NodeConfig"  
**Current behavior:** Worker uses hardcoded `kIngressByteCapacity` (8 MiB) and
`kReturnByteCapacity` (8 MiB); `NodeConfig::workerRingCapacity` is unused.  
**Impact:** Cannot tune ring sizes per deployment without recompilation.  
**Recommendation:** Thread `NodeConfig.workerRingCapacity` (or add
`workerIngressByteCapacity` / `workerReturnByteCapacity`) to `Worker` constructor.

---

### F5: Default InMemoryLog policy is `DropOldest`; spec mandates `BackPressure` for memory topics

**Where:** `InMemoryLog.h` constructor default; `NodeConfig.h` TopicSpec (no `fullPolicy` field)  
**Spec requirement (2026-09-23 decision):** "full → produce back-pressures, never
overwrites un-reclaimed data"  
**Current behavior:** `DropOldest` silently evicts oldest batches; producers never know.  
**Impact:** Memory topics lose oldest data under pressure without any signal to producers.  
**Recommendation:** Add `TopicSpec::memoryLogFullPolicy` (default `BackPressure` per
spec). Requires propagating the policy from `NodeConfig` through `BrokerNode` /
`TopicRegistry` to `InMemoryLog` construction.

---

### F6: Worker's `executeProduceWire_` does not check `appendNoSync` return value for `-1` (BackPressure sentinel)

**Where:** `Worker.h:executeProduceWire_` (~line 617)  
**Spec requirement:** "full → produce back-pressures" (memory topic)  
**Current behavior:** If `appendNoSync` returns `-1` (BackPressure mode), the Worker
still enqueues `WorkerReturn{errorCode=0}` — a **false OK**.  
**Impact:** A BackPressure-mode InMemoryLog that is full sends a false-OK ack to the
client. This is a correctness violation (data was NOT appended).  
**Recommendation:** Check the return value of `appendNoSync`; if it is `-1`, set
`ret.errorCode = Internal(6)` (or a new `Throttled` code per F1) before enqueueing.
This is a small, localized fix in `executeProduceWire_`.

---

## Summary Table of Findings

| # | Ring/Path | Category | Impact | Fix size |
|---|---|---|---|---|
| F1 | Worker ingress ring full → `Internal(6)` instead of hold+retry | Spec divergence | Client retries via timeout (suboptimal) | Medium (hold-queue in gateway) |
| F1b | IoBackend submit full → `Internal(6)` instead of hold+retry | Spec divergence | Spurious Internal on extreme load | Low (rarely fires) |
| F2 | Worker return ring full → silent ack drop | Missing observable | Client timeout instead of error | Medium (retry list in Worker) |
| F3 | Transport outbound ring full — counter only, no warn | Observability | Ops can't see replication BP | Trivial (rate-limited warn) |
| F4 | Worker ring byte capacity not configurable via NodeConfig | Config gap | Cannot tune rings without recompile | Small (thread knob through ctor) |
| F5 | Default InMemoryLog policy is DropOldest; spec says BackPressure | Spec divergence | Memory topic data loss under pressure | Medium (add TopicSpec field) |
| F6 | `executeProduceWire_` ignores `appendNoSync` `-1` → false-OK | Correctness bug | Client acked OK for un-appended record | Tiny (check return value) |

---

## Protocol Gap (not a code bug — spec extension needed)

The `pc::ErrorCode` enum has no `Throttled` / `TryAgain` value. All back-pressure
conditions today map to `Internal(6)`, which client SDKs cannot distinguish from a
genuine broker error. A Kafka-style `THROTTLING_QUOTA_EXCEEDED` / `TRY_AGAIN_LATER`
code would allow client SDKs to retry quickly with backoff rather than escalating.

This is a non-breaking protocol extension (new enum value); it requires a change to
`rune-protocol` and client SDKs. Deferred to a dedicated ticket.
