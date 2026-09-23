# Cross-Thread Rings → agrona OneToOne (zero-copy read, flat byte messages)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Replace the hand-rolled cooked `SpscRing<T>` cross-thread rings with agrona `OneToOneRingBuffer` byte rings carrying flat `[header][payload]` messages — zero heap in the ring, zero cooked objects, zero-copy consumer reads, one copy on write (stock agrona has no `tryClaim`, decided).

**Architecture:** Every worker↔control and worker↔flusher hand-off is single-producer→single-consumer, so each uses one agrona `OneToOneRingBuffer` (wrapped by a thin `OneToOneByteRing`, mirroring `libs/consensus/include/rune/consensus/IngressRing.h` which wraps `ManyToOneRingBuffer`). Messages are flat: a trivially-copyable POD header (scalars; in-process `PartitionReplica*`/`Log*` pointers carried as `uintptr_t` — same address space) + an inline variable tail (the record-batch bytes / replicated records / ISR list / owned-set). The consumer's `read()` handler gets a **view into the ring** and parses in place — for produce it re-stamps the real base offset in place (CRC region is after the base-offset field, so no CRC recompute) and appends the batch view straight to the log.

**Tech Stack:** C++23, `aeron::concurrent::ringbuffer::OneToOneRingBuffer` (vendored aeron, conan), agrona `handler_t` (`void(int32_t msgTypeId, AtomicBuffer&, index_t, index_t)`), gtest.

**Spec:** thread-per-core runtime design `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md`; law [[rune-shared-nothing-law]] (agrona rings, OneToOne where single-producer, no cooked/heap rings).

## Global Constraints
- **agrona OneToOne only** for these cross-thread rings — no hand-rolled ring, no `tryClaim` (not in the C++ build); `write()` copies src→ring (1 copy), `read()` is zero-copy view.
- **No heap / no cooked objects in any ring slot.** No `std::vector` crossing a ring. Encode scalars + inline bytes; pointers as `uintptr_t` (in-process only).
- **No allocation on the hot path:** encode into a pre-allocated per-producer scratch `AtomicBuffer`, then `write()`. Decode is a view (no alloc).
- **Back-pressure:** `write()` returns false when full → the producer must NOT ack/drop; it holds and retries next cycle (produce is commit-critical). Mirror the existing `tryEnqueueOp` back-pressure contract.
- **Ring sizes from NodeConfig** (`workerRingCapacity` already exists; reuse/rename for byte capacity — power-of-two payload bytes + agrona TRAILER_LENGTH).
- **Parity:** every existing worker/gateway/broker/replication test passes unchanged. Semantics identical; only the transport of the message changes.
- **TSan-gated:** the worker↔control and worker↔flusher boundaries proven race-free under `build-tsan`.
- **Commit LOCAL main; NEVER push.** Co-Authored-By trailer.
- Build: cmake `/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake`, ninja `/opt/homebrew/bin/ninja`, ctest same dir; `build-fast` for iteration.

## File Structure
- `libs/node/include/rune/node/OneToOneByteRing.h` (new) — thin wrapper over `OneToOneRingBuffer` (owned byte storage + AtomicBuffer + ring); `bool write(const uint8_t*, size_t, int32_t msgTypeId)`, `int drain(const handler_t&, int limit)`. Mirror `IngressRing.h` exactly but with `OneToOneRingBuffer`.
- `libs/node/include/rune/node/WorkerWire.h` (new) — flat POD headers + encode/decode for WorkerOp and WorkerReturn. `WorkerOpWire` (all WorkerOp scalar fields; pointers as `uintptr_t`; `uint32_t tailLen`) + inline tail; msgTypeId = `int32_t(op.type)+1`. Same for `WorkerReturnWire`. `encodeWorkerOp(const WorkerOp&, AtomicBuffer& scratch) -> length` and `decodeWorkerOpHeader(const AtomicBuffer& view, index_t off) -> const WorkerOpWire*` + a tail-view accessor. Symmetric for WorkerReturn.
- `libs/node/include/rune/node/Worker.h` (modify) — `ingress_`/`ret_` become `OneToOneByteRing`; a per-worker encode scratch for returns; `tryEnqueueOp` encodes→write; `drainIngress_` drains→decode-view→execute (produce re-stamps in place); `drainReturn` decodes views.
- Producers (modify): `libs/gateway/src/ClientGateway.cpp` (produce + pushread enqueue; return drain), `libs/replication/.../AeronPartitionTransport.*` (ApplyReplicated enqueue), `libs/node/.../ReplicationMetadataBridge.h` (BecomeLeader/Follower), `libs/node/.../WorkerPool.h` (SetOwnership).
- `libs/node/include/rune/node/PortableFlushBackend.h`/`.cpp` (modify) — submit/completion → `OneToOneByteRing` (POD messages).
- Delete `libs/node/include/rune/node/SpscRing.h` after all uses are gone.
- Tests (new): `libs/node/tests/node/OneToOneByteRingTest.cpp`, `libs/node/tests/node/WorkerWireTest.cpp` (round-trip every op/return type incl. tails).

## Task R1: `OneToOneByteRing` wrapper + `WorkerWire` codec + unit tests
**Interfaces produced:** `OneToOneByteRing` (write/drain), `WorkerOpWire`/`WorkerReturnWire` POD headers + `encodeWorkerOp`/`decodeWorkerOp*`/`encodeWorkerReturn`/`decodeWorkerReturn*`.
- [ ] Mirror `IngressRing.h` → `OneToOneByteRing.h` using `concurrent/ringbuffer/OneToOneRingBuffer.h`. Ctor takes payload-bytes capacity (pow2) + TRAILER_LENGTH. `write(data,len,msgTypeId)` wraps a src AtomicBuffer + `ring_.write(...)`; `drain(handler,limit)` = `ring_.read(handler,limit)`.
- [ ] `WorkerWire.h`: define `WorkerOpWire` (a `#pragma pack`/trivially-copyable struct with EVERY scalar field of `WorkerOp` from `libs/node/include/rune/node/WorkerOp.h`, `replica`/`logPtr` as `uintptr_t`, and `uint32_t tailLen` + `uint8_t tailKind`). Tail encodes the ONE variable field per type: Produce→`recordBatchBytes`; ApplyReplicated→`replicatedPayload.records`; BecomeLeader→`isr` (as `NodeId[]`); SetOwnership→`ownedSet` (as `uintptr_t[]`); others→empty. `encodeWorkerOp` writes header then tail into the scratch buffer; decode returns a header pointer + a tail span (views into the ring). Symmetric `WorkerReturnWire` (tail = PushRead `chunk`).
- [ ] Write `OneToOneByteRingTest` (write N frames → drain → byte-identical) and `WorkerWireTest` (encode→decode round-trip for EACH op type + each return type, asserting all scalar fields + tail bytes match). Build `rune_node_tests`, run, all pass.
- [ ] Commit: `feat(rings): OneToOneByteRing (agrona OneToOne wrapper) + WorkerWire flat codec`.

## Task R2: worker INGRESS → agrona OneToOne (WorkerOp)
**Consumes:** R1. **Depends on nothing else.**
- [ ] `Worker.h`: change `ingress_` to `OneToOneByteRing`; add a per-worker `ingressScratch_` (pre-sized AtomicBuffer-backed vector). `tryEnqueueOp(const WorkerOp&)` → `encodeWorkerOp(op, ingressScratch_)` → `ingress_.write(...)` (return false on full = back-pressure, SAME contract). `drainIngress_` → `ingress_.drain([](msgTypeId,buf,off,len){ decode header+tail views; executeOp on the views })`. For Produce, re-stamp the base offset in place in the ring view (mutable) then append the batch tail view to the log — no `std::vector` copy.
- [ ] Update ALL ingress producers to pass a `WorkerOp` to the (unchanged-signature) `tryEnqueueOp`, which now encodes internally — so producer call sites (gateway produce/pushread, transport ApplyReplicated, bridge BecomeLeader/Follower, WorkerPool SetOwnership) need NO change IF `tryEnqueueOp(WorkerOp)` keeps its signature. Verify each still compiles + behaves.
- [ ] Parity: `ninja -C build-fast rune_node_tests rune_worker_tests rune_worker_pool_tests rune_broker_node_tests`; run Worker/WorkerPool unit + ctest `-R "BrokerNode\.|WorkerPoolParity|GatewayProduce|MultiLeaderProduce|GatewayAcksAllDeferredAck|AsyncDurability"`. All pass.
- [ ] Commit: `refactor(rings): worker ingress SpscRing→agrona OneToOne byte ring`.

## Task R3: worker RETURN → agrona OneToOne (WorkerReturn)
**Consumes:** R1, R2.
- [ ] `Worker.h`: `ret_` → `OneToOneByteRing` + a return encode scratch. Worker produce/pushread paths encode `WorkerReturn`→`ret_.write()`. `drainReturn(fn)` decodes views and calls `fn(const WorkerReturn&)` (reconstruct a WorkerReturn from the view for the gateway callback, OR change the gateway drain to consume the view directly — prefer view-direct to avoid a rebuild copy; if the gateway callback needs an object, rebuild the scalar struct + a chunk view).
- [ ] Update the gateway return drain (`ClientGateway.cpp` `drainWorkerReturns_`).
- [ ] Parity (same suites as R2) + AsyncDurability (returns are the ack path). All pass.
- [ ] Commit: `refactor(rings): worker return SpscRing→agrona OneToOne byte ring`.

## Task R4: flush rings → agrona OneToOne + delete SpscRing + parity/TSan
**Consumes:** R1–R3.
- [ ] `PortableFlushBackend`: submit/completion rings → `OneToOneByteRing` carrying the POD `FlushRequest`/`FlushCompletion` bytes (fixed-size). `submit()` encodes+write (kNoToken on full); `reap()` drains views→handler. Update `IoBackend.h` types if needed. Keep the sentinel-stop.
- [ ] Grep-confirm no remaining `SpscRing` users; delete `libs/node/include/rune/node/SpscRing.h` and its CMake refs/tests (or repoint).
- [ ] Full parity: build all node/gateway/replication test targets; run the broad ctest set green.
- [ ] **TSan**: `build-tsan` + `ctest -j1` with `TSAN_OPTIONS="suppressions=$(pwd)/cmake/tsan-aeron.supp halt_on_error=1"` on Worker/WorkerPool/BrokerNode/AsyncDurability/GatewayReplicationWiring — ZERO races.
- [ ] Commit: `refactor(rings): flush rings→agrona OneToOne; remove hand-rolled SpscRing; TSan clean`.

## Self-Review
- Every cross-thread ring (worker ingress, worker return, flush submit, flush completion) → agrona OneToOne. ✅
- No cooked objects / no heap in any ring slot (flat header + inline tail). ✅
- Zero-copy read (view), 1-copy write (stock agrona, decided). ✅
- Back-pressure contract preserved (`write()` false → hold+retry, no ack/drop). ✅
- Parity + TSan gates. ✅
- InMemoryLog (per-partition byte buffer, Task 6 `2c932b8`) is UNCHANGED — it is single-owner storage, not a comms ring. ✅
- Type consistency: `OneToOneByteRing` write/drain + `WorkerWire` encode/decode signatures are defined in R1 and consumed unchanged in R2–R4.
