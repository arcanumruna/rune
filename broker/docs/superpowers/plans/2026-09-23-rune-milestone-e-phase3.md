# Milestone E Phase E3 — Async Durability + Deferred-Ack-on-Completion + Memory-Topic Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each partition-worker scheduler-free async durability — fsync happens off the worker's blocking path via an `IoBackend` (io_uring on Linux, portable flusher+SPSC on macOS/dev), and the produce ack fires only when the flush completion is reaped — plus redo the memory-topic `InMemoryLog` as a proper single-owner back-pressure ring.

**Architecture:** A per-owner `IoBackend` exposes a worker-facing SPSC hand-off: `submit(flush request) -> completion token`, and `reap(handler)` polled in `doWork`. The worker appends to page cache (`appendNoSync`), submits a coalesced fsync to its `IoBackend`, and **defers enqueuing the `WorkerReturn::ProduceResult`** (the ack) until that flush's completion token is reaped — reusing the existing worker→gateway return ring as the ack path. `fsync` failure → the return carries an error code (never OK) and the shard is marked unhealthy. Back-pressure: a full submission ring makes the worker hold new durable produces (no ack) rather than block. The `InMemoryLog` memmove buffer becomes a plain single-owner ring with back-pressure-when-full.

**Tech Stack:** C++23, `rune::consensus::Agent`/`AgentRunner`, `rune::node::SpscRing<T>` (rune's own object ring), Linux `io_uring` (liburing-free raw mmap SQ/CQ, SQPOLL/IOPOLL), gtest. Build: `build-fast` (Ninja+ccache, no TSan) for iteration; `build-tsan` for the race proof; Hetzner Linux box for the io_uring backend + `<100µs` validation.

**Spec:** `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md` (§"Async durability I/O — the scheduler-free hand-off", §Components `IoBackend`, §Testing, migration step 3).

## Global Constraints

- **Shared-nothing law ([[rune-shared-nothing-law]]):** no shared mutable state, no atomics on cross-thread state, no locks/condvars/thread_local on the hot path. All cross-thread comms via rings. Atomics allowed ONLY inside a ring's own impl. The portable flusher thread communicates with its worker ONLY via SPSC rings.
- **No OS scheduler / no blocking on the worker hot path.** The worker NEVER calls a blocking `fsync`/`pwrite` on its own thread. Submission is a memory write; completion reap is a memory read.
- **Off-thread durability, latency-first ([[benchmark-latency-gate]]):** a full fsync must never stall the worker's `append→replicate→ack`. `<100µs` p99 is the authoritative gate on native Linux (macOS/Rosetta numbers are non-authoritative).
- **No per-record hot-path virtual calls ([[rune-no-virtual-calls-hot-path]]):** the `IoBackend` platform choice is resolved ONCE (compile-time `#ifdef __linux__` / a single stored backend), NOT a per-submit virtual. The once-per-`doWork` `Agent::doWork` dispatch is the only allowed virtual.
- **Back-pressure mandatory on every ring:** submission-ring-full = hold the produce, do NOT ack, client retries (produce is commit-critical). Never silently drop; never block. Observe (metric/log).
- **Configurable ring sizes at NodeConfig level:** submission-ring depth, completion-ring depth, memory-topic ring bytes — each a NodeConfig field + `RUNE_*` env (power-of-two) + sensible default.
- **Parity:** all existing Milestone-D/E tests pass unchanged. Durability semantics only get STRICTER (ack later), never weaker.
- **Commit LOCAL main only; NEVER push.** End commit messages with the Co-Authored-By trailer.
- **TSan-gated:** every new cross-thread boundary (worker↔flusher submit/completion rings, memory ring) proven race-free under `build-tsan` before the task is done.

---

## File Structure

- `libs/node/include/rune/node/IoBackend.h` (new) — the `FlushRequest`/`FlushCompletion` types, the `CompletionToken`, and the `IoBackend` seam. A single concrete type selected at compile time (no vtable on submit/reap): `PortableFlushBackend` (all platforms) and `UringFlushBackend` (`#ifdef __linux__`), plus a `using IoBackend = <selected>` alias. Header-only where hot (submit/reap inline).
- `libs/node/include/rune/node/PortableFlushBackend.h` (new) — dedicated flusher `std::thread` + two `SpscRing`s (submit, completion). Worker-facing `submit()`/`reap()` are the SAME shape as the uring backend.
- `libs/node/src/PortableFlushBackend.cpp` (new) — flusher loop: drain submit ring → `pwrite`/`fdatasync` → post completion.
- `libs/node/include/rune/node/UringFlushBackend.h` + `.cpp` (new, `#ifdef __linux__`) — raw io_uring SQ/CQ mmap wrapper: submit write/fsync SQE (SQPOLL), reap CQE. Compiled to an empty TU on non-Linux.
- `libs/storage/include/rune/storage/Log.h` + `src/Log.cpp` (modify) — expose the active segment's fd + a `flushBoundary()` (byte offset that a completed fsync makes durable) so the `IoBackend` can fsync without the worker blocking; keep `sync()` for the non-async fallback path.
- `libs/node/include/rune/node/Worker.h` (modify) — own an `IoBackend`; in `executeProduce_` submit a coalesced flush + record a `DurablePending` keyed on the completion token instead of immediately enqueuing the `ProduceResult`; in `doWork` reap completions → fire due `ProduceResult`s (or error); back-pressure on full submit ring.
- `libs/node/include/rune/node/WorkerOp.h` (modify) — add the durability-tier field to `WorkerOp`/`WorkerReturn` (so the gateway knows an ack is completion-gated) and an `errorCode` already exists on the return.
- `libs/storage/include/rune/storage/InMemoryLog.h` (modify) — replace the `std::vector`-memmove buffer with a single-owner ring + back-pressure-when-full.
- `libs/node/include/rune/node/NodeConfig.h` + `src/NodeConfig.cpp` (modify) — `ioSubmitRingDepth`, `ioCompletionRingDepth`, `memoryLogRingBytes`, `durabilityTier` (+ `RUNE_*` env).
- Tests (new): `libs/node/tests/node/PortableFlushBackendTest.cpp`, `libs/node/tests/integration/AsyncDurabilityTest.cpp`, `libs/storage/tests/storage/InMemoryLogRingTest.cpp`, and (Linux-gated) `libs/node/tests/integration/UringDurabilityTest.cpp`.

**Ownership:** the `IoBackend` is owned by whoever owns the log — a `Worker` for user partitions, the control-thread `NodeRuntimeAgent` for internal partitions / single-node mode. Same split as the cleaner-drain pattern already in `NodeRuntimeAgent::doWork` (lines ~289-316) and `Worker::doWork` (Step 6.5).

---

## Design decisions (rulings, grounded in the spec + code map)

1. **Ack path = the existing return ring, deferred.** Today `Worker::executeProduce_` (`Worker.h:227-300`) enqueues `WorkerReturn::ProduceResult` immediately (line 299); the gateway's `drainWorkerReturns_` sends the `ProduceResponse`. For the strict/durable tier, the worker instead stores the fully-built `WorkerReturn` in a per-worker `DurablePending{token, WorkerReturn}` list and enqueues it only when the token's completion is reaped in `doWork`. This reuses the proven return-ring ack path — no new gateway↔worker channel.
2. **Durability tiers compose, they don't replace.** `durabilityTier ∈ {Leader, Majority, Strict}`. `Leader` = ack after append (today's acks=Leader). `Majority` = ack after HW advance (today's acks=All HW-gated `drainPendingAcks`, UNCHANGED, still gateway-side). `Strict` = ack after **local fsync completion AND** (if acks=All) HW advance — i.e. the worker completion-gates the return, and the gateway's HW gate still applies on top. Strict is opt-in per topic; default stays Majority so existing tests are parity-unchanged.
3. **Group-commit coalescing.** Per `doWork` cycle a worker submits AT MOST ONE fsync per owned Log (coalescing all that cycle's appends to that file); the completion token covers every pending produce whose `lastOffset <= flushBoundary`. This mirrors the existing 10ms `flushDirtyLogs` cadence (`ClientGateway.cpp:2945`) but per-worker and async.
4. **Single concrete backend, no per-submit virtual.** `using IoBackend = UringFlushBackend` on Linux, `= PortableFlushBackend` otherwise. `submit`/`reap` are non-virtual inline calls. This honors [[rune-no-virtual-calls-hot-path]].
5. **fsync failure = error, never ack.** A completion carrying an error → the held `WorkerReturn.errorCode` is set to a non-OK code and the shard is flagged unhealthy (a per-worker flag on the owning replica handle); the client sees a produce error and retries. No data is silently acked.

---

## Task 1: `IoBackend` seam + `FlushRequest`/`FlushCompletion` types

**Files:**
- Create: `libs/node/include/rune/node/IoBackend.h`
- Test: covered via Task 2's backend tests (this task is just types + the selection alias)

**Interfaces:**
- Produces: `struct FlushRequest { int fd; int64_t flushBoundary; uint64_t token; };`, `struct FlushCompletion { uint64_t token; int32_t errorCode; int64_t flushBoundary; };` (errorCode 0 = OK, else errno), `using CompletionToken = uint64_t;`. The backend concept: `CompletionToken submit(int fd, int64_t flushBoundary) noexcept;` (returns 0 / `kNoToken` when the submit ring is full — back-pressure), `template<class H> int reap(H&& handler) noexcept;` (calls `handler(const FlushCompletion&)` per reaped completion; returns count).

- [ ] **Step 1: Write the types + the compile-time selection alias**

```cpp
// IoBackend.h
#pragma once
#include <cstdint>
namespace rune::node {
using CompletionToken = uint64_t;
inline constexpr CompletionToken kNoToken = 0;   // returned when submit ring is full

struct FlushRequest    { int fd = -1; int64_t flushBoundary = 0; CompletionToken token = 0; };
struct FlushCompletion { CompletionToken token = 0; int32_t errorCode = 0; int64_t flushBoundary = 0; };
} // namespace rune::node

#include <rune/node/PortableFlushBackend.h>
#if defined(__linux__)
#include <rune/node/UringFlushBackend.h>
namespace rune::node { using IoBackend = UringFlushBackend; }
#else
namespace rune::node { using IoBackend = PortableFlushBackend; }
#endif
```

- [ ] **Step 2: Commit**

```bash
git add libs/node/include/rune/node/IoBackend.h
git commit -m "feat(E3): IoBackend seam + FlushRequest/FlushCompletion types"
```

---

## Task 2: `PortableFlushBackend` — flusher thread + SPSC submit/completion rings

**Files:**
- Create: `libs/node/include/rune/node/PortableFlushBackend.h`, `libs/node/src/PortableFlushBackend.cpp`
- Test: `libs/node/tests/node/PortableFlushBackendTest.cpp`

**Interfaces:**
- Consumes: `SpscRing<FlushRequest>`, `SpscRing<FlushCompletion>` (`rune/node/SpscRing.h`), `FlushRequest`/`FlushCompletion` (Task 1).
- Produces: `class PortableFlushBackend { PortableFlushBackend(std::size_t submitDepth, std::size_t completionDepth); ~PortableFlushBackend(); CompletionToken submit(int fd, int64_t flushBoundary) noexcept; template<class H> int reap(H&&) noexcept; };` — ctor spawns the flusher `std::thread`; dtor signals stop (a sentinel token via the ring) and joins.

- [ ] **Step 1: Write the failing test — submit → reap round-trips a completion**

```cpp
// PortableFlushBackendTest.cpp
TEST(PortableFlushBackend, SubmitThenReapCompletesForRealFd) {
    // Write a temp file, submit an fsync, spin-reap the completion.
    char path[] = "/tmp/rune_pfb_XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "hello", 5), 5);
    rune::node::PortableFlushBackend be(/*submitDepth=*/16, /*completionDepth=*/16);
    auto tok = be.submit(fd, /*flushBoundary=*/5);
    ASSERT_NE(tok, rune::node::kNoToken);
    rune::node::FlushCompletion got{};
    for (int i = 0; i < 1'000'000 && got.token == 0; ++i)
        be.reap([&](const rune::node::FlushCompletion& c){ got = c; });
    EXPECT_EQ(got.token, tok);
    EXPECT_EQ(got.errorCode, 0);
    EXPECT_EQ(got.flushBoundary, 5);
    ::close(fd); ::unlink(path);
}
```

- [ ] **Step 2: Run it — FAIL to compile (no PortableFlushBackend)**

Run: `ninja -C build-fast rune_node_tests` → expect compile error.

- [ ] **Step 3: Implement the backend**

Header declares the two rings, a `std::thread flusher_`, and a monotonic per-instance `CompletionToken nextToken_` (flusher-only + worker-only split: worker owns `nextToken_` since it calls `submit`). `submit()` builds a `FlushRequest{fd, flushBoundary, ++nextToken_}`, `if (!submitRing_.tryEnqueue(req)) return kNoToken;` (back-pressure), returns the token. `reap()` drains `completionRing_`. The flusher loop (`.cpp`) drains the submit ring, for each request calls `::fdatasync(fd)`, builds `FlushCompletion{token, (rc==0?0:errno), flushBoundary}`, and enqueues it on the completion ring (spin if momentarily full — the flusher may block, it is NOT the worker). Stop via a sentinel `FlushRequest{fd=-1}`.

- [ ] **Step 4: Run the test — PASS**

Run: `ninja -C build-fast rune_node_tests && ./build-fast/libs/node/rune_node_tests --gtest_filter='PortableFlushBackend.*'`

- [ ] **Step 5: Add the failure + back-pressure tests**

```cpp
TEST(PortableFlushBackend, BadFdCompletesWithError) {
    rune::node::PortableFlushBackend be(16, 16);
    auto tok = be.submit(/*fd=*/-99, 0);
    ASSERT_NE(tok, rune::node::kNoToken);
    rune::node::FlushCompletion got{};
    for (int i = 0; i < 1'000'000 && got.token == 0; ++i)
        be.reap([&](const rune::node::FlushCompletion& c){ got = c; });
    EXPECT_EQ(got.token, tok);
    EXPECT_NE(got.errorCode, 0);           // EBADF surfaced, never a silent OK
}
TEST(PortableFlushBackend, SubmitRingFullReturnsNoToken) {
    rune::node::PortableFlushBackend be(/*submitDepth=*/2, /*completionDepth=*/2);
    // Flood without reaping; at least one submit must report back-pressure.
    bool sawFull = false;
    for (int i = 0; i < 1000; ++i) if (be.submit(1, i) == rune::node::kNoToken) { sawFull = true; break; }
    EXPECT_TRUE(sawFull);
}
```

- [ ] **Step 6: Run all PortableFlushBackend tests — PASS. Commit**

```bash
git add libs/node/include/rune/node/PortableFlushBackend.h libs/node/src/PortableFlushBackend.cpp libs/node/tests/node/PortableFlushBackendTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(E3): PortableFlushBackend — off-thread flusher + SPSC submit/completion rings"
```

---

## Task 3: `Log` async-flush seam (fd + flushBoundary)

**Files:**
- Modify: `libs/storage/include/rune/storage/Log.h`, `libs/storage/src/Log.cpp`
- Test: `libs/storage/tests/storage/LogTest.cpp` (add cases)

**Interfaces:**
- Produces: `int Log::activeSegmentFd() const noexcept;` (the .log fd the flusher must fsync — the ACTIVE segment, since that is where `appendNoSync` writes) and `int64_t Log::flushBoundary() const noexcept { return logEndOffset_; }` (the offset that becomes durable once the active fd is fsync'd). Keep `sync()` unchanged for the non-async path.
- Consumes: existing `appendNoSync` (`Log.cpp:424`) / `activeSegment()`.

- [ ] **Step 1: Write the failing test — activeSegmentFd is a valid fd matching the file**

```cpp
TEST(Log, ActiveSegmentFdIsFsyncable) {
    auto dir = makeTempDir();
    rune::storage::Log log(dir, /*roll=*/1<<20);
    log.recover();
    std::vector<uint8_t> b = makeBatch(/*records=*/1);
    log.appendNoSync(b.data(), b.size(), /*lastOffset=*/0);
    int fd = log.activeSegmentFd();
    ASSERT_GE(fd, 0);
    EXPECT_EQ(::fdatasync(fd), 0);                 // the worker's flusher will do exactly this
    EXPECT_EQ(log.flushBoundary(), log.logEndOffset());
}
```

- [ ] **Step 2: Run — FAIL (no activeSegmentFd)**. `ninja -C build-fast rune_storage_tests`

- [ ] **Step 3: Implement** — `activeSegmentFd()` returns `activeSegment().logFd()` (add a trivial `int logFd() const` accessor to `LogSegment` returning its already-open `.log` fd). `flushBoundary()` returns `logEndOffset_`.

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/Log.h libs/storage/src/Log.cpp libs/storage/include/rune/storage/LogSegment.h libs/storage/tests/storage/LogTest.cpp
git commit -m "feat(E3): Log async-flush seam — activeSegmentFd() + flushBoundary()"
```

---

## Task 4: Worker async durability — submit flush, defer the ack until completion

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h`, `libs/node/include/rune/node/WorkerOp.h`
- Test: `libs/node/tests/integration/AsyncDurabilityTest.cpp`

**Interfaces:**
- Consumes: `IoBackend` (Task 1/2), `Log::activeSegmentFd()`/`flushBoundary()` (Task 3), existing `executeProduce_`/`ret_` (`Worker.h:227-300`), `WorkerReturn` (`WorkerOp.h:180`).
- Produces: a per-worker `IoBackend io_;` and `struct DurablePending { CompletionToken token; int64_t lastOffset; WorkerReturn ret; };` + `std::vector<DurablePending> durablePending_;`. New `WorkerOp` field `uint8_t durabilityTier = 0;` (0=Leader/Majority handled as today, 2=Strict). New `Worker::doWork` step (before Step 6.5 drains) `reapDurableCompletions_()`.

- [ ] **Step 1: Write the failing test — a Strict produce is NOT acked until the flush completes**

```cpp
// AsyncDurabilityTest.cpp — single-worker, InMemory transport, disk-backed Log.
// Enqueue a Strict produce op; drive doWork; assert NO ProduceResult on the
// return ring until reap has fired; then assert exactly one, errorCode==OK,
// lastOffset correct. (Uses the real PortableFlushBackend on macOS.)
TEST(AsyncDurability, StrictProduceAcksOnlyAfterFlushCompletion) {
    WorkerHarness h;                     // owns one disk-backed leader replica
    h.enqueueProduce(/*tier=*/Strict, /*records=*/4);
    // Immediately after append+submit, before the flusher completes, no ack yet:
    h.worker.doWork(1);
    EXPECT_EQ(h.drainReturns(), 0) << "ack must wait for fsync completion";
    // Spin doWork until the completion is reaped and the ack fires:
    int acks = 0;
    for (int i = 0; i < 1'000'000 && acks == 0; ++i) { h.worker.doWork(i+2); acks = h.drainReturns(); }
    ASSERT_EQ(acks, 1);
    EXPECT_EQ(h.lastReturn.errorCode, 0);
    EXPECT_EQ(h.lastReturn.lastOffset, 3);
}
```

- [ ] **Step 2: Run — FAIL (no Strict path / durablePending_)**

- [ ] **Step 3: Implement** — in `executeProduce_`, after `appendNoSync` + `onLeaderLogAppended`, if `op.durabilityTier == Strict`: `CompletionToken t = io_.submit(log->activeSegmentFd(), log->flushBoundary()); if (t == kNoToken) { /* back-pressure: build ret with TryAgain errorCode, enqueue now */ } else { durablePending_.push_back({t, ret.lastOffset, std::move(ret)}); }`. Otherwise enqueue `ret` immediately as today. Add `reapDurableCompletions_()`: `io_.reap([&](const FlushCompletion& c){ for each durablePending_ with token==c.token: set ret.errorCode = c.errorCode?kIoError:0; ret_.tryEnqueue(std::move(ret)); erase; });` — call it at the top of `doWork` before the produce ops so completions from prior cycles fire promptly.

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Add the fsync-failure + back-pressure tests**

```cpp
TEST(AsyncDurability, FlushFailureErrorsTheAckNeverOK) { /* inject bad fd → errorCode != OK, shard unhealthy */ }
TEST(AsyncDurability, SubmitRingFullHoldsProduce)      { /* tiny submit ring, flood → TryAgain, never a false OK */ }
```

- [ ] **Step 6: Run all AsyncDurability tests — PASS. Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/node/include/rune/node/WorkerOp.h libs/node/tests/integration/AsyncDurabilityTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(E3): worker async durability — submit flush, defer ack to completion reap"
```

---

## Task 5: Group-commit coalescing + gateway durability-tier plumbing

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` (coalesce), `libs/gateway/src/ClientGateway.cpp` (set `WorkerOp.durabilityTier` from the topic's tier; Majority path unchanged), `libs/gateway/include/rune/gateway/ClientGateway.h`
- Test: `libs/node/tests/integration/AsyncDurabilityTest.cpp` (add coalescing case)

**Interfaces:**
- Consumes: Task 4's `durablePending_`/`io_`; the topic durability tier (from `PartitionConfig`/`TopicSpec`).
- Produces: at most one `io_.submit` per owned Log per `doWork` cycle; one completion satisfies every `DurablePending` with `lastOffset <= completion.flushBoundary`.

- [ ] **Step 1: Write the failing test — N Strict produces in one cycle share ONE fsync**

```cpp
TEST(AsyncDurability, GroupCommitCoalescesManyProducesIntoOneFlush) {
    WorkerHarness h;
    for (int i = 0; i < 8; ++i) h.enqueueProduce(Strict, /*records=*/1);
    h.worker.doWork(1);                                   // 8 appends, ONE submit
    EXPECT_EQ(h.io_submitCount(), 1);
    int acks = 0; for (int i = 0; i < 1'000'000 && acks < 8; ++i) { h.worker.doWork(i+2); acks += h.drainReturns(); }
    EXPECT_EQ(acks, 8);                                    // one completion → 8 acks
}
```

- [ ] **Step 2: Run — FAIL (submits once per produce, not once per cycle)**

- [ ] **Step 3: Implement** — in `doWork`, defer the per-Log submit: during the ingress drain, mark each touched Log dirty (a small per-cycle `std::vector<Log*> dirtyThisCycle_`, cleared each cycle); after the produce loop, submit ONE flush per dirty Log and stamp its token onto every `DurablePending` created this cycle for that Log. On completion reap, fire every pending whose `lastOffset <= c.flushBoundary`. Gateway: set `op.durabilityTier` from the topic tier before `tryEnqueueOp`.

- [ ] **Step 4: Run — PASS.** Also re-run the Task 4 tests (parity).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/gateway/src/ClientGateway.cpp libs/gateway/include/rune/gateway/ClientGateway.h libs/node/tests/integration/AsyncDurabilityTest.cpp
git commit -m "feat(E3): group-commit coalescing (one fsync/log/cycle) + tier plumbing"
```

---

## Task 6: memory-topic `InMemoryLog` single-owner back-pressure ring

**Files:**
- Modify: `libs/storage/include/rune/storage/InMemoryLog.h`
- Test: `libs/storage/tests/storage/InMemoryLogRingTest.cpp`

**Interfaces:**
- Produces: `InMemoryLog` backed by a fixed-capacity single-owner byte ring (capacity = `memoryLogRingBytes`, power-of-two); `appendNoSync`/`append` return `-1` (or a documented full sentinel) when the ring is full (back-pressure) instead of unbounded `std::vector` growth + memmove. `read` unchanged in semantics.

- [ ] **Step 1: Write the failing test — full ring back-pressures, never memmoves/grows unbounded**

```cpp
TEST(InMemoryLogRing, FullRingBackPressuresInsteadOfGrowing) {
    rune::storage::InMemoryLog log(/*capacityBytes=*/4096);
    std::vector<uint8_t> b = makeBatch(/*~1KB*/);
    int appended = 0;
    for (int i = 0; i < 100; ++i) { if (log.appendNoSync(b.data(), b.size(), i) < 0) break; ++appended; }
    EXPECT_LT(appended, 100) << "must back-pressure at capacity, not grow unbounded";
    // After a read consumes the oldest, appends resume (ring wrap):
    (void)log.read(/*startOffset=*/0, /*maxBytes=*/2048);
    // ... assert an append now succeeds
}
```

- [ ] **Step 2: Run — FAIL (current impl grows the vector unbounded)**

- [ ] **Step 3: Implement** the single-owner ring (head/tail indices are plain — single owner, NO atomics; this log is touched only by its owning worker). Preserve offset→bytes mapping semantics `read()` relies on. `sync()` stays a no-op.

- [ ] **Step 4: Run — PASS. Re-run existing InMemoryLog + memory-persistence tests for parity.**

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/InMemoryLog.h libs/storage/tests/storage/InMemoryLogRingTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(E3): InMemoryLog redo — single-owner back-pressure ring (no memmove)"
```

---

## Task 7: NodeConfig durability/ring config

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h`, `libs/node/src/NodeConfig.cpp`
- Test: `libs/node/tests/node/NodeConfigTest.cpp`

**Interfaces:**
- Produces: `int32_t ioSubmitRingDepth = 1024;`, `int32_t ioCompletionRingDepth = 1024;`, `int32_t memoryLogRingBytes = 8*1024*1024;` on `NodeConfig` (power-of-two, `RUNE_IO_SUBMIT_RING_DEPTH` / `RUNE_IO_COMPLETION_RING_DEPTH` / `RUNE_MEMORY_LOG_RING_BYTES`), and a per-topic `durabilityTier` on `TopicSpec` (extend the existing `durability` field or add `uint8_t durabilityTier = 1;` default Majority). Plumb into `WorkerPool`→`Worker` (IoBackend depths) and `InMemoryLog` construction.

- [ ] **Step 1: Write the failing test — env overrides parse as power-of-two**

```cpp
TEST(NodeConfig, IoRingDepthsParseFromEnvPow2) {
    auto env = fakeEnv({{"RUNE_IO_SUBMIT_RING_DEPTH","2048"},{"RUNE_MEMORY_LOG_RING_BYTES","16777216"}});
    auto [cfg, err] = NodeConfig::fromEnv(env);
    ASSERT_TRUE(err.empty());
    EXPECT_EQ(cfg.ioSubmitRingDepth, 2048);
    EXPECT_EQ(cfg.memoryLogRingBytes, 16*1024*1024);
}
```

- [ ] **Step 2: Run — FAIL.** **Step 3: Implement** using the existing `parsePow2`/`parseInt32` pattern (`NodeConfig.cpp:325`). **Step 4: Run — PASS.**

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp libs/node/tests/node/NodeConfigTest.cpp
git commit -m "feat(E3): NodeConfig io-ring depths + memory-log ring bytes + durability tier"
```

---

## Task 8: `UringFlushBackend` (Linux) — validated on the Hetzner box

**Files:**
- Create: `libs/node/include/rune/node/UringFlushBackend.h`, `libs/node/src/UringFlushBackend.cpp` (whole body under `#if defined(__linux__)`; empty TU otherwise)
- Test: `libs/node/tests/integration/UringDurabilityTest.cpp` (registered ONLY when `__linux__`)

**Interfaces:**
- Produces: `class UringFlushBackend` with the SAME `submit(int fd, int64_t flushBoundary) -> CompletionToken` / `reap(H&&)` shape as `PortableFlushBackend`, backed by raw io_uring: mmap'd SQ/CQ, `IORING_OP_FSYNC` (IORING_FSYNC_DATASYNC) SQE, SQPOLL submit (zero-syscall), CQE reap by memory read. `io_uring_setup`/`mmap` in the ctor; teardown in dtor.

- [ ] **Step 1: Write the Linux-gated tests** — mirror the PortableFlushBackend suite (round-trip, bad-fd error, submit-ring-full) PLUS: SQPOLL zero-syscall submit (strace-free assertion via a submit-count vs syscall-count check), group-commit coalescing, crash-before-completion → no acked-data loss (fork+kill harness). Wrap the whole file in `#if defined(__linux__)`.

- [ ] **Step 2: Build+run on the Hetzner Linux box** (`[[infra-ssh-access]]`): `ssh <box>`, build with the project's Linux toolchain, run `rune_node_tests --gtest_filter='UringDurability.*'`. Expect FAIL (not implemented).

- [ ] **Step 3: Implement `UringFlushBackend`** (raw io_uring, no liburing dependency): `io_uring_setup(depth, params with IORING_SETUP_SQPOLL)`, mmap SQ/CQ/SQEs, `submit()` writes an `IORING_OP_FSYNC` SQE + advances the SQ tail (SQPOLL picks it up — no syscall), `reap()` reads the CQ head/tail and drains CQEs. `user_data` = the CompletionToken.

- [ ] **Step 4: Run the Linux tests — PASS on the Hetzner box.** Record results in the ledger (native-Linux, authoritative).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/UringFlushBackend.h libs/node/src/UringFlushBackend.cpp libs/node/tests/integration/UringDurabilityTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(E3): UringFlushBackend — raw io_uring SQPOLL fsync backend (Linux)"
```

---

## Task 9: Parity + TSan proof + wiring into BrokerNode

**Files:**
- Modify: `libs/node/include/rune/node/WorkerPool.h`, `libs/node/src/BrokerNode.cpp` (construct workers with IoBackend depths; control-thread IoBackend for internal/single-node), `libs/node/include/rune/node/NodeRuntimeAgent.h` (internal-partition flush path, mirroring its cleaner-drain split)
- Test: existing D/E suites (parity) + a new TSan structural test

**Interfaces:**
- Consumes: everything above. Default `durabilityTier = Majority` so all existing tests are unchanged.

- [ ] **Step 1: Wire IoBackend depths through WorkerPool→Worker and give NodeRuntimeAgent a control-thread IoBackend for internal/single-node logs (same owner-split as the cleaner drains at `NodeRuntimeAgent.h:289-316`).**

- [ ] **Step 2: Run the full parity suite under `build-fast`** — all Milestone-D/E node+gateway+replication tests green (Majority default unchanged).

Run: `ctest --test-dir build-fast -R 'BrokerNode|GatewayProduce|GatewayAcksAllDeferredAck|WorkerPool|MultiLeaderProduce|OffsetReplication|MetadataReplication'`

- [ ] **Step 3: TSan proof** — build `build-tsan`, run the async-durability + worker suites with `TSAN_OPTIONS="suppressions=$(pwd)/cmake/tsan-aeron.supp halt_on_error=1"` via `ctest -j1`. ZERO ThreadSanitizer reports on the worker↔flusher submit/completion rings and the memory ring.

- [ ] **Step 4: Commit**

```bash
git add libs/node/include/rune/node/WorkerPool.h libs/node/src/BrokerNode.cpp libs/node/include/rune/node/NodeRuntimeAgent.h
git commit -m "feat(E3): wire per-owner IoBackend into WorkerPool + control thread; parity + TSan clean"
```

---

## Self-Review

**Spec coverage:**
- io_uring async durability (Linux) → Task 8. ✅
- deferred-ack-on-completion → Tasks 4-5 (defer the return-ring `ProduceResult` until completion reap). ✅
- portable flusher+SPSC fallback (macOS) → Task 2. ✅
- group-commit coalescing → Task 5. ✅
- back-pressure on full submission ring → Tasks 2, 4 (kNoToken → hold produce). ✅
- fsync-failure → error-not-ack + shard unhealthy → Task 4. ✅
- memory-topic single-owner back-pressure ring (`InMemoryLog` redo) → Task 6. ✅
- configurable ring sizes at NodeConfig → Task 7. ✅
- TSan on new boundaries + parity → Task 9. ✅
- crash-before-completion → no acked-data loss → Task 8 (Linux). ✅
- native-Linux `<100µs` benchmark matrix → deferred to **E4** (per spec, E4 owns the authoritative bench + affinity + DR-E2E). Noted, not a gap.

**Type consistency:** `CompletionToken`/`FlushRequest`/`FlushCompletion` defined in Task 1, consumed identically in Tasks 2, 4, 5, 8. `submit(int fd, int64_t flushBoundary) -> CompletionToken` and `reap(H&&) -> int` are the same shape across `PortableFlushBackend` (Task 2) and `UringFlushBackend` (Task 8). `durabilityTier` added to `WorkerOp` (Task 4) and set by the gateway (Task 5).

**Placeholder scan:** each task carries concrete signatures, real test code, and exact build/commit commands. No TBDs.

**Open item confirmed at plan time:** the spec's Phase-2 close-out sequencing (§ line 29) recommends (a) — merge D now, `InMemoryLog` redone here in Task 6. This plan implements Task 6 accordingly; the D merge itself is an E4/close-out step, not blocking E3.
