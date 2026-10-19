# Sharded Invoker Client (rune-client-cpp) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give rune-client-cpp an opt-in sharded client whose shards each own an embedded Aeron driver in invoker mode plus an invoker-driven Aeron client (the owning thread runs the whole duty cycle), remove the per-record / per-batch / per-duty-cycle waste on the produce and ack paths in both modes, consume the broker's per-partition `ingressEndpoint`, and rebuild `rune_produce_throughput` as an open-loop, coordinated-omission-safe benchmark that records every record in a histogram.

**Architecture:** The existing `Client` (agent + sessions + metadata + producer hub) gains a construction-time runner mode: `Thread` (today's background runner, default) or `Invoker` (no threads at all: `AeronTransport` runs `AERON_THREADING_MODE_INVOKER` and `useConductorAgentInvoker(true)`, and the app thread calls `Client::doWork()`). A `ClientShard` wraps one `Client` and its producer; a `ShardGroup` owns N shards with a deterministic partition-to-shard map (agent mode routes any app thread's send to the owning shard; invoker mode gives each app thread its own shard). Inside every shard the produce path becomes flat: per-partition route slots resolved once into publication handles, a templated send sink instead of `std::function`, a fixed per-partition ring of in-flight batch slots addressed directly by a tagged correlation id, and ack ownership that travels through the producer ring instead of a mutex-guarded map.

**Tech Stack:** C++23, CMake ≥ 3.31 + Conan 2 (x86_64 Debug/Release toolchains already generated under `build/build/Debug/generators` and `build_release/build/Release/generators`), Aeron 1.41.4 (C media driver `aeronmd` + C++ client), GoogleTest, rune-protocol (sibling headers), rune-fr (sibling static lib).

**Spec:** /Users/vradu/Projects/rune-broker/docs/superpowers/specs/2026-09-30-self-contained-worker-shards-design.md

## Global Constraints

- **Where things run (user, 2026-09-30):** every benchmark and every real end-to-end / DR run happens on **k3s-node-01** (`root@$RUNE_VM_HOST`, repos under `/root/rune-e5`), never on the Mac; local runs are unit/integration only.
- **Mandatory VM cleanup before EVERY benchmark and e2e/DR run:** `bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh` (created by the broker plan, Task 16 Step 3b — if it is not on the VM yet, run that step first). It kills all Rune/Kafka/bench/client processes and Rune DR containers, waits for load1 < 1, and exits 1 on strays, a busy box, < 3 GiB root disk or < 1 GiB `/dev/shm` free. A run that did not start from a passing `vm-clean.sh` is not a valid measurement. The bench's `--idle-check-only` gate runs after it, not instead of it.
- Pass criterion (spec §1): "on the 4-core k3s-node-01 box, 3 brokers + 1 client co-located, 1 KB records, 3-node acks=all with the replicated, no-fsync-before-ack guarantee (Rune `tier=majority`, `durability=all-in-sync` — the same guarantee as Kafka acks=all with default flushing), an open-loop producer offering 100,000 rec/s for 60 s achieves ≥ 100,000 rec/s and p99 end-to-end ≤ 100 µs (every record measured, coordinated-omission-safe)."
- §1a.1 Shared-nothing. Every piece of mutable state is owned by exactly one thread. No shared mutable state, no locks/mutex/condvar, no atomics as cross-thread coordination (atomics only inside ring-buffer primitives), no `thread_local` on hot paths.
- §1a.2 Ring buffers are the only cross-thread channel. Aeron/Agrona rings (OneToOne where single-producer, ManyToOne only where genuinely multi-producer) — never shared reads or cross-thread callbacks.
- §1a.3 Aeron is the bible. Mirror Aeron's patterns exactly: single-writer agents with `doWork(nowNs)` under an `IdleStrategy`, invoker modes, `tryClaim`/`BufferClaim` zero-copy, flat SBE messages with reused encoders, correct release/acquire ordering, one injected clock sampled once per cycle. Busy-spin only on a dedicated pinned core; an agent with no work never burns a shared core.
- §1a.4 Rich-state state machines. Each state holds exactly the variables it needs (e.g. `Resolving(pubRegId, deadlineNs)`, `Ready(pub)`); every edge case is one explicit state with one code path — no scattered booleans. Reference: `FollowerSync`.
- §1a.5 Zero allocation, no virtual calls on hot paths. No heap allocation, `std::function`, node-based containers (`std::map`/`unordered_map`/`std::string`) or vtable dispatch on the per-record/per-batch/per-duty-cycle path. Polymorphism via concrete types + a construction-time branch, templates, or tagged unions.
- §1a.6 Cache-friendly, compact data structures; every optimisation proven by a before/after benchmark with no correctness or p99 regression — no speculative optimisation.
- §1a.7 Test everything; regression test on every fix (failing repro test first, marked `*Regression*`, regression suite run first). "Done" means the real end-to-end path is green (real client ↔ real broker over real transport) in the default configuration as well as flag-on; shortcut/in-process tests alone never count.
- §1a.8 Tests never spin to pass time: advance an injected clock to the next deadline; every drive loop has a hard iteration cap and fails instead of hanging.
- §1a.9 Time-independent liveness: control-plane convergence never depends on a tuned timeout out-racing jitter.
- §1a.10 Benchmark gate: any p99 > 100 µs is a stop-and-fix, not a result. Perf claims are proven with interleaved A/B on a verified-idle box, citing the exact code path; measurements are validated (same binary, complete captures, run succeeded, feature actually active) before any explanation is drawn from them.
- §1a.11 Never hardware to mask software: a bad number on this box is a software bug to root-cause and fix.
- §1a.12 Fix on sight: code found violating these while doing the work is fixed as part of the work, not noted.
- Shared interface with the broker plan (FIXED): rune-protocol `MetadataResponse` gains per-partition string field `ingressEndpoint` = "host:port" of the owning worker's ingress; EMPTY string means "use the node endpoint" (old brokers / legacy mode). Broker worker ProduceResponses in shard mode arrive on the client's session egress channel/stream from a different publication (same correlation ids) — client matching must not assume a single publisher.
- Rollout (spec §7): client invoker/shard options are default off until the DR matrix is green on the shard path; defaults unchanged (`ClientConfig::runnerMode = RunnerMode::Thread`, `ShardGroupConfig::shardCount = 1`, `ShardGroupConfig::mode = ShardMode::Agent`). The only deliberate default change is the explicit MTU (spec §4.1: "explicit MTU, default 8 KB").
- The default mode must pass every pre-existing test after every task. Test names that pin a fix contain `Regression` so `ctest -R Regression` / `--gtest_filter='*Regression*'` runs them as a suite; run that suite first, then the full suite.
- Build/test dirs used by this plan (created in Task 1, both gitignored by `/cmake-build-*/`): tests `cmake-build-sic` (Debug, x86_64, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`; baseline 292 tests, 100% pass); examples/bench `cmake-build-sic-rel` (Release, `BUILD_EXAMPLES=ON`, `BUILD_BENCHMARKS=ON`, `BUILD_TESTING=OFF`). The old `build/` dir's `all` target is broken by the codegen bug fixed in Task 1 and holds stale binaries — do not use it.
- Never `conan create/export/upload`. Never push. Commit only on branch `feat/sharded-invoker-client`. Never commit the stashed `RUNE_RETARGET_DBG` diff (Task 1 stashes it).
- Session protocol (repo CLAUDE.md §5): after each task's full-suite run push the counts ctest printed: `/Users/vradu/Projects/rune-broker/scripts/rune-live.sh test "rune-client/sharded-invoker" passed passed=$PASSED failed=$FAILED total=$TOTAL commit=$(git rev-parse --short HEAD)` (status `failed` when `$FAILED` > 0).

## Review Focus

1. The invoker-mode owner thread stops calling `doWork()` for longer than the Aeron client-liveness timeout (debugger pause, GC-like stall, blocking app code): Aeron's `ClientConductor` inter-service timeout closes the client and the default Aeron error handler calls `::exit(-1)`; a user expects the shard to report `SendResult::Closed` and stay alive, never exit or hang. Test: Task 6 Step 1 (`InvokerTransport.StalledOwnerSurfacesClosedInsteadOfExitingRegression`) and Task 15 Step 1 (`ClientShard.StalledOwnerTurnsSendsIntoClosed`).
2. A user ack callback running inside an invoker-mode duty cycle calls the void `Producer::send` while its partition is back-pressured: the void overload would re-enter the duty cycle (corrupting single-writer state) or wait forever for a drain that can only happen on the same thread; a user expects an immediate `std::logic_error`, never a hang. Test: Task 14 Step 1 (`DirectIngress.VoidSendInsideDutyCycleCallbackThrowsRegression`).
3. One batch mixes void sends and callback sends (the common "only some records need confirmation" pattern): today the ack ring stores only ack-wanting records, so record i of the slice is not record i of the batch and callbacks get the wrong offsets; a user expects each callback to receive exactly its own record's offset. Test: Task 10 Step 1 (`ProducerAcks.MixedVoidAndCallbackRecordsGetTheirOwnOffsetsRegression`).
4. A ProduceResponse arrives twice (retransmit dedup, a second publisher in shard mode) or after its batch timed out and the slot was reused: a user expects exactly one callback per record, no callback for a stale id, and no in-flight accounting drift. Test: Task 9 Step 1 (`InFlightRing.DuplicateAndStaleAcksAreIgnoredRegression`) and Task 10 Step 1 (`ProducerAcks.DuplicateBatchAckFiresCallbacksOnceRegression`).
5. A metadata refresh moves a partition's `ingressEndpoint` (worker ownership move / leader change) while batches for it are in flight: a user expects the in-flight batches to be re-offered to the new worker immediately and acked exactly once, not stranded until the retransmit timer or lost. Test: Task 16 Step 2 (`IngressEndpointRouting.EndpointMoveReoffersInFlightBatchesRegression`).

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `examples/rune_produce_throughput.cpp` | modify | Task 1 commits the window-wait backoff; Task 16 dispatches `argv` runs to the invoker bench, env-only runs keep the legacy path byte-for-byte. |
| `CMakeLists.txt` | modify | Task 1 fixes the codegen `COMMAND`; new test targets `rune_pure_tests`, `rune_invoker_tests`, `rune_bench_util_tests`; new sources `src/detail/CpuAffinity.cpp`, `src/shard/ClientShard.cpp`, `examples/bench/InvokerBench.cpp`. |
| `include/rune/client/transport/TransportConfig.h` | create | `TransportConfig`, `DriverMode`, `validate()`, `applyEnvOverrides()` — Aeron-free so pure tests can include it. |
| `include/rune/client/transport/AeronTransport.h`, `src/transport/AeronTransport.cpp` | modify | single-thread driver pumping; explicit MTU / socket buffers / idle / liveness; invoker mode + `dutyCycle()`; `Resolution`-driven async pubs/subs with `PubHandle`; non-exiting Aeron error handler. |
| `include/rune/client/transport/Resolution.h` | create | Rich-state resolution machine `Unstarted → Resolving(regId, deadlineNs) → Ready(handle) / Failed(reason, retryAtNs)`. |
| `include/rune/client/detail/FlatIndexMap.h` | create | Open-addressing `int64 → uint32` map (no node allocation; `find()` never allocates). |
| `include/rune/client/detail/M2ORingBuffer.h` | modify | Aeron-style consumer heartbeat accessors. |
| `include/rune/client/detail/CpuAffinity.h`, `src/detail/CpuAffinity.cpp` | create | `pinCurrentThreadToCore(int)`. |
| `include/rune/client/agent/ClientAgent.h`, `src/agent/ClientAgent.cpp` | modify | route slots (`FlatIndexMap` → `PartitionRoute{session, PubHandle}`), `setRoute` with separate session/data channel, getenv read once, `transport_.dutyCycle()` as step 0, ProduceResponse ack buffer. |
| `include/rune/client/agent/AgentProduceSink.h` | create | Concrete produce sink (plain member call into the agent). |
| `include/rune/client/Client.h`, `src/Client.cpp` | modify | `RunnerMode`, `Client::doWork()`, `dutyCycle()`, injected clock, runner pinning, producer/ack path as plain member calls, txn registration via ring (no `txnHubMtx_`), ingressEndpoint routes, route-change re-offer. |
| `include/rune/client/producer/InFlightRing.h` | create | Tagged batch correlation ids + fixed per-partition in-flight batch ring. |
| `include/rune/client/producer/ProducerImpl.h`, `src/producer/ProducerImpl.cpp` | modify | flat partition table, templated sink, in-flight ring, ack ownership through the ring, `BatchAck`, `trySubmit`/`SendResult`, direct (invoker) ingress. |
| `include/rune/client/producer/ProducerHub.h`, `src/producer/ProducerHub.cpp` | modify | templated drain, ordinal routing of acks, per-partition re-offer. |
| `include/rune/client/producer/Producer.h`, `src/producer/Producer.cpp`, `src/producer/BatchBuilder.cpp` | modify | `SendResult trySend`, never-dropping void `send`, `onBatchAck`, ack handles. |
| `include/rune/client/codec/Codec.h`, `include/rune/client/metadata/MetadataCache.h` | modify | carry `ingressEndpoint`. |
| `include/rune/client/shard/ShardMap.h` | create | `shardFor`, `channelWithPortOffset` (pure). |
| `include/rune/client/shard/ClientShard.h`, `src/shard/ClientShard.cpp` | create | `ShardMode`, `ShardGroupConfig`, `shardClientConfig`, `ClientShard`, `ShardGroup`. |
| `examples/bench/LogLinearHistogram.h`, `examples/bench/OpenLoopSchedule.h`, `examples/bench/SeqTimestampRing.h`, `examples/bench/ProcStat.h`, `examples/bench/InvokerBench.h`, `examples/bench/InvokerBench.cpp` | create | Bench building blocks + the invoker bench main. |
| `include/rune/client/producer/CountingSink.h` | create | Concrete counting sink for in-process benches/tests (one of the three explicitly instantiated sink types). |
| `tests/support/InvokerGateway.h` | create | Single-threaded fake broker on an invoker Aeron client (Connect / Metadata / Produce, extra worker endpoints with their own ack publications). |
| `tests/support/AllocCounter.h`, `tests/support/AllocCounter.cpp` | create | Replacement global new/delete counting allocations inside a window (`rune_invoker_tests` only). |
| `tests/detail/FlatIndexMapTest.cpp`, `tests/transport/ResolutionTest.cpp`, `tests/transport/TransportConfigTest.cpp`, `tests/producer/InFlightRingTest.cpp`, `tests/shard/ShardMapTest.cpp` | create | Pure tests (`rune_pure_tests`). |
| `tests/transport/InvokerTransportTest.cpp`, `tests/agent/InvokerAgentTest.cpp`, `tests/client/InvokerClientTest.cpp`, `tests/producer/ProducerSinkTest.cpp`, `tests/producer/ProducerAllocRegressionTest.cpp`, `tests/producer/ProducerAckPathTest.cpp`, `tests/producer/ProducerSendTest.cpp`, `tests/producer/DirectIngressTest.cpp`, `tests/shard/ClientShardTest.cpp`, `tests/client/IngressEndpointRoutingTest.cpp` | create | Aeron-backed / producer-level tests (`rune_invoker_tests`). |
| `tests/bench/BenchUtilTest.cpp` | create | Bench utility tests (`rune_bench_util_tests`). |
| `tests/transport/AeronTransportTest.cpp`, `tests/producer/ProducerTest.cpp`, `tests/agent/MultiNodeRoutingTest.cpp`, `bench/ProducerAccumBench.cpp`, `include/rune/client/TypedTopic.h`, `include/rune/client/producer/TransactionalProducerImpl.h`, `src/producer/TransactionalProducerImpl.cpp` | modify | new regression tests; two tests whose asserted behaviour the spec changes (`CloseWithPendingFuture`, `OversizedValueRejected`); accum bench sink; `TypedTopic::send` loses `noexcept`; txn `doWork` takes its send function by reference. |

---

### Task 1: Feature branch, build dirs, bench backoff commit, codegen fix

**Files:**
- Modify: `examples/rune_produce_throughput.cpp:205-216,220,246` (already modified in the working tree — the `waitUntil` spin→yield→sleep(20µs) backoff; commit as-is)
- Modify: `CMakeLists.txt:358` (`COMMAND rune_codec_gen` → `COMMAND $<TARGET_FILE:rune_codec_gen>`)
- Stash (do not commit): `src/transport/AeronTransport.cpp` (`RUNE_RETARGET_DBG` debug logging, not ours)

**Interfaces:**
- Consumes: nothing.
- Produces: branch `feat/sharded-invoker-client`; build dirs `cmake-build-sic` (tests) and `cmake-build-sic-rel` (examples + benchmarks); a recorded baseline test count.

- [ ] **Step 1: Park the unrelated debug diff**

```bash
cd /Users/vradu/Projects/rune-client-cpp
git status --short
# Expected:
#  M examples/rune_produce_throughput.cpp
#  M src/transport/AeronTransport.cpp
# ?? conan_provider.cmake
git stash push -m "RUNE_RETARGET_DBG debug logging (not part of the sharded-invoker plan; git stash pop when done)" -- src/transport/AeronTransport.cpp
git diff --stat
# Expected: only examples/rune_produce_throughput.cpp | 18 ++++++++++++++----
git switch -c feat/sharded-invoker-client
```

- [ ] **Step 2: Reproduce the pre-existing `all`-target build failure**

`BUILD_EXAMPLES=ON` breaks `cmake --build` of the `all` target today: the custom command runs a bare `rune_codec_gen` that is not on `PATH`.

```bash
cd /Users/vradu/Projects/rune-client-cpp
G=$PWD/build/build/Debug/generators
cmake -S . -B cmake-build-sic-rel -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON -DBUILD_BENCHMARKS=ON -DBUILD_TESTING=OFF \
  -DCMAKE_TOOLCHAIN_FILE=$PWD/build_release/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH=$PWD/build_release/build/Release/generators
cmake --build cmake-build-sic-rel -j8 --target generate_characters_header 2>&1 | tail -3
```
Expected: FAIL with `make[2]: rune_codec_gen: No such file or directory`.

- [ ] **Step 3: Fix the custom command**

In `CMakeLists.txt:358` replace

```cmake
        COMMAND rune_codec_gen "${CHARACTERS_BLOB}" "${CHARACTERS_GEN_H}"
```
with
```cmake
        COMMAND $<TARGET_FILE:rune_codec_gen> "${CHARACTERS_BLOB}" "${CHARACTERS_GEN_H}"
```

- [ ] **Step 4: Build examples and the tests dir; record the baseline**

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic-rel -j8 --target generate_characters_header rune_produce_throughput rune_producer_accum_bench 2>&1 | tail -3
G=$PWD/build/build/Debug/generators
cmake -S . -B cmake-build-sic -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DBUILD_EXAMPLES=ON \
  -DCMAKE_TOOLCHAIN_FILE=$G/conan_toolchain.cmake -DCMAKE_PREFIX_PATH=$G
cmake --build cmake-build-sic -j8 2>&1 | tail -3
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -4 | tee /tmp/sic-baseline.txt
```
Expected: `generate_characters_header` and `rune_produce_throughput` build (`[100%] Built target ...`); `rune_producer_accum_bench` FAILS to compile (`error: no viable conversion from '(lambda ...)' to 'std::function<bool (int32_t, int32_t, int32_t, const uint8_t *, std::size_t)>'` — its 3-argument sink predates the 5-argument `SendFn`; Task 8 fixes it, so until then build `cmake-build-sic-rel` only by explicit `--target`); the tests dir builds its `all` target and ctest prints `100% tests passed, 0 tests failed out of 292` (verified while writing this plan with the Step 3 fix applied; no `_NOT_BUILT` entries). If ctest reports failures, write the failing test names into `/tmp/sic-baseline.txt` — they are pre-existing and every later "all existing tests pass" check compares against this file; any of them that is a genuine bug is owned by this plan (fix on sight) in the task that touches its code.

- [ ] **Step 5: Commit**

```bash
cd /Users/vradu/Projects/rune-client-cpp
git add examples/rune_produce_throughput.cpp CMakeLists.txt
git commit -m "bench(client): backoff window wait + fix characters.gen.h codegen command

The acked-mode window wait spins -> yields -> sleeps 20us instead of a bare
yield loop (a yield-spinning load generator held a scheduler slot and stole
CPU from the brokers it measures: ~14k sched_yield/s).  The codegen custom
command now runs \$<TARGET_FILE:rune_codec_gen> so BUILD_EXAMPLES=ON builds
the all target.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
git log --oneline -1
```

---

### Task 2: Drive the embedded driver from exactly one thread (Regression)

Today `startEmbeddedDriver()` sets `driverRunning_` and launches `driverThread_` (`AeronTransport.cpp:533-540`) and then calls `aeron_driver_main_do_work(driver_)` from the caller thread in the CnC readiness loop (`:546-554`) while the driver thread runs the same duty cycle — two threads drive one driver (spec §3.6). The CnC file is created and signalled ready inside `aeron_driver_init` (`aeron_driver.c:864` `aeron_driver_create_cnc_file`, `:912` `aeron_cnc_version_signal_cnc_ready`), so the caller can prime the conductor before any driver thread exists.

**Files:**
- Modify: `include/rune/client/transport/AeronTransport.h:176-251` (private section; add accessor in public "Readiness" block at `:144-155`)
- Modify: `src/transport/AeronTransport.cpp:533-559` (`startEmbeddedDriver` tail), `:573-589` (`stopEmbeddedDriver` drain)
- Test: `tests/transport/AeronTransportTest.cpp` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces: `[[nodiscard]] int AeronTransport::callerPumpViolations() const noexcept;` and private `int AeronTransport::pumpDriverFromCaller_() noexcept;` (every caller-thread `aeron_driver_main_do_work` goes through it; later tasks keep using it).

- [ ] **Step 1: Write the failing test**

Append to `tests/transport/AeronTransportTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Regression (spec §3.6): a driver is driven by exactly one thread.  start()
// used to pump aeron_driver_main_do_work() from the caller thread while the
// driver thread was already running the same duty cycle.
// ---------------------------------------------------------------------------
TEST(AeronTransportRegression, StartupPumpsDriverFromOneThreadOnlyRegression) {
    Transport t{makeLoopbackConfig(1010)};
    t.start();
    EXPECT_EQ(t.callerPumpViolations(), 0)
        << "start() pumped the driver from the caller thread while driverThread_ ran";
    awaitConnected(t);
    t.stop();
    EXPECT_EQ(t.callerPumpViolations(), 0)
        << "stop() may drain the driver only after joining driverThread_";
}
```

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `cmake --build cmake-build-sic --target rune_transport_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'callerPumpViolations' in 'rune::client::transport::AeronTransport'`.

- [ ] **Step 3: Add the instrumentation only (still buggy ordering)**

In `AeronTransport.h`, public "Readiness" block after `egressConnected()` (`:154`):

```cpp
    /// Regression hook (spec §3.6): number of times the CALLER thread pumped
    /// aeron_driver_main_do_work() while driverThread_ was running.  A driver
    /// must be driven by exactly one thread, so this must always be 0.
    [[nodiscard]] int callerPumpViolations() const noexcept { return callerPumpViolations_; }
```

In the private driver section after `void stopEmbeddedDriver() noexcept;` (`:187`):

```cpp
    /// Every caller-thread duty cycle of the driver goes through here.  Counts a
    /// violation when driverThread_ is concurrently driving the same driver.
    int pumpDriverFromCaller_() noexcept;
    int callerPumpViolations_ = 0;  // written/read by the owning (caller) thread only
```

In `AeronTransport.cpp` add before `stopEmbeddedDriver`:

```cpp
int AeronTransport::pumpDriverFromCaller_() noexcept {
    if (driverRunning_.load(std::memory_order_acquire)) {
        ++callerPumpViolations_;
    }
    return aeron_driver_main_do_work(driver_);
}
```

and replace the five caller-thread `aeron_driver_main_do_work(driver_)` calls at `:547`, `:549`, `:550`, `:582`, `:584` with `pumpDriverFromCaller_()`.

- [ ] **Step 4: Run the test to verify it fails for the real reason**

Run: `cmake --build cmake-build-sic --target rune_transport_tests -j8 && ./cmake-build-sic/rune_transport_tests --gtest_filter='*Regression*'`
Expected: FAIL — `Expected equality of these values: t.callerPumpViolations() Which is: 3 (or ≥1) 0 ... start() pumped the driver from the caller thread while driverThread_ ran`.

- [ ] **Step 5: Fix the ordering**

Replace `AeronTransport.cpp:533-558` (from `driverRunning_.store(true, ...)` through the `if (!fs::exists(cncPath)) { throw ...; }` block) with:

```cpp
    // aeron_driver_init already created AND signalled the CnC file
    // (aeron_driver.c:864 create_cnc_file, :912 signal_cnc_ready), so prime the
    // conductor from THIS thread before any driver thread exists.  From here on
    // exactly one thread drives the driver (spec §3.6).
    const std::string cncPath = aeronDir + "/cnc.dat";
    if (!fs::exists(cncPath)) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(driverCtx_);
        driver_    = nullptr;
        driverCtx_ = nullptr;
        throw std::runtime_error(
            "AeronTransport: Aeron CnC file missing after aeron_driver_init: " + cncPath);
    }
    pumpDriverFromCaller_();
    pumpDriverFromCaller_();
    pumpDriverFromCaller_();

    driverRunning_.store(true, std::memory_order_release);
    driverThread_ = std::thread([this]() {
        while (driverRunning_.load(std::memory_order_acquire)) {
            aeron_driver_main_idle_strategy(
                driver_,
                aeron_driver_main_do_work(driver_));
        }
    });
```

`stopEmbeddedDriver` already joins before draining; with the counter in place it proves it.

- [ ] **Step 6: Run the regression suite, then the transport suite**

Run: `cmake --build cmake-build-sic --target rune_transport_tests -j8 && ./cmake-build-sic/rune_transport_tests --gtest_filter='*Regression*' && ./cmake-build-sic/rune_transport_tests`
Expected: PASS (`[  PASSED  ] 1 test.` then all transport tests pass).

- [ ] **Step 7: Full suite, then commit**

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic -j8 && ctest --test-dir cmake-build-sic -R Regression --output-on-failure && ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
git add include/rune/client/transport/AeronTransport.h src/transport/AeronTransport.cpp tests/transport/AeronTransportTest.cpp
git commit -m "fix(transport): drive the embedded driver from exactly one thread

start() primed the driver conductor from the caller thread while
driverThread_ was already running aeron_driver_main_do_work on the same
driver (spec 3.6; plausible cause of the 2026-09-30 shared-driver stall).
The CnC is ready after aeron_driver_init, so prime first, then start the
thread.  Every caller-thread pump is counted; regression test pins 0.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Explicit transport configuration (MTU, socket buffers, idle strategy, liveness)

**Files:**
- Create: `include/rune/client/transport/TransportConfig.h`
- Modify: `include/rune/client/transport/AeronTransport.h:43-63` (struct moves out; add include + accessor)
- Modify: `src/transport/AeronTransport.cpp:47-60` (`start()` validation), `:492-513` (driver context knobs)
- Modify: `include/rune/client/Client.h:119-152` (`ClientConfig` fields + `toAgentConfig` decl), `src/Client.cpp:42-76` (`makeAgentConfig` → public `toAgentConfig`)
- Create: `tests/transport/TransportConfigTest.cpp`
- Modify: `tests/transport/AeronTransportTest.cpp` (append real-driver MTU test)
- Modify: `CMakeLists.txt` (new target `rune_pure_tests`, appended after `gtest_discover_tests(rune_schema_gen_tests)` at `:891`)

**Interfaces:**
- Consumes: Task 2 `pumpDriverFromCaller_()`.
- Produces:
  - `enum class rune::client::transport::DriverMode : uint8_t { Thread, Invoker };`
  - `struct TransportConfig { std::string ingressChannel; int32_t ingressStreamId = 0; std::string egressChannel; int32_t egressStreamId = 0; std::string dataDir; std::size_t termBufferLength = 4u*1024*1024; std::size_t mtuLength = 8192; std::size_t socketSndBufBytes = 2u*1024*1024; std::size_t socketRcvBufBytes = 2u*1024*1024; std::string driverIdleStrategy = "sleep-ns"; DriverMode driverMode = DriverMode::Thread; int64_t clientLivenessTimeoutNs = 10'000'000'000LL; };`
  - `[[nodiscard]] std::optional<std::string> validate(const TransportConfig&);`
  - `void applyEnvOverrides(TransportConfig&);` (reads `RUNE_CLIENT_AERON_MTU`, `RUNE_AERON_SOCKET_BUFFER_BYTES`, `RUNE_CLIENT_AERON_DRIVER_IDLE`)
  - `[[nodiscard]] int32_t AeronTransport::ingressMaxPayloadLength() const noexcept;`
  - `ClientConfig::mtuBytes = 8192`, `ClientConfig::socketBufferBytes = 2 MiB`; `[[nodiscard]] agent::AgentConfig rune::client::toAgentConfig(const ClientConfig&);`

- [ ] **Step 1: Write the failing pure tests**

Create `tests/transport/TransportConfigTest.cpp`:

```cpp
// tests/transport/TransportConfigTest.cpp — explicit Aeron knob validation (no driver).
#include <rune/client/transport/TransportConfig.h>

#include <gtest/gtest.h>

#include <cstdlib>

using rune::client::transport::TransportConfig;
using rune::client::transport::validate;
using rune::client::transport::applyEnvOverrides;

TEST(TransportConfig, DefaultsAreValidAndMtuIsEightKiB) {
    TransportConfig c;
    EXPECT_EQ(c.mtuLength, 8192u);
    EXPECT_EQ(c.socketSndBufBytes, 2u * 1024 * 1024);
    EXPECT_EQ(c.socketRcvBufBytes, 2u * 1024 * 1024);
    EXPECT_EQ(c.driverIdleStrategy, "sleep-ns");
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsMtuNotMultipleOf32) {
    TransportConfig c; c.mtuLength = 8190;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("multiple of 32"), std::string::npos);
}

TEST(TransportConfig, RejectsMtuOutsideAeronBounds) {
    TransportConfig c; c.mtuLength = 32;
    EXPECT_TRUE(validate(c).has_value());
    c.mtuLength = 65536;
    EXPECT_TRUE(validate(c).has_value());
    c.mtuLength = 65504;
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsTermLengthNotPowerOfTwoOrOutOfRange) {
    TransportConfig c; c.termBufferLength = 3u * 1024 * 1024;
    EXPECT_TRUE(validate(c).has_value());
    c.termBufferLength = 32u * 1024;
    EXPECT_TRUE(validate(c).has_value());
    c.termBufferLength = 64u * 1024;
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsSocketBufferSmallerThanMtu) {
    TransportConfig c; c.socketSndBufBytes = 4096;
    EXPECT_TRUE(validate(c).has_value());
    c.socketSndBufBytes = 0;  // 0 = OS default, allowed
    c.socketRcvBufBytes = 4096;
    EXPECT_TRUE(validate(c).has_value());
}

TEST(TransportConfig, EnvOverridesMtuSocketBuffersAndIdle) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "16384", 1);
    ::setenv("RUNE_AERON_SOCKET_BUFFER_BYTES", "4194304", 1);
    ::setenv("RUNE_CLIENT_AERON_DRIVER_IDLE", "backoff", 1);
    TransportConfig c;
    applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    ::unsetenv("RUNE_CLIENT_AERON_DRIVER_IDLE");
    EXPECT_EQ(c.mtuLength, 16384u);
    EXPECT_EQ(c.socketSndBufBytes, 4194304u);
    EXPECT_EQ(c.socketRcvBufBytes, 4194304u);
    EXPECT_EQ(c.driverIdleStrategy, "backoff");
}

TEST(TransportConfig, MalformedEnvIsIgnored) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "8k", 1);
    TransportConfig c;
    applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    EXPECT_EQ(c.mtuLength, 8192u);
}
```

Add the target to `CMakeLists.txt` right after `gtest_discover_tests(rune_schema_gen_tests)` (`:891`, still inside `if(BUILD_TESTING)`):

```cmake
    # ── Sharded invoker client: pure (Aeron-free) unit tests ─────────────────
    add_executable(rune_pure_tests
        tests/transport/TransportConfigTest.cpp
    )
    target_include_directories(rune_pure_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_pure_tests PRIVATE GTest::gtest_main)
    gtest_discover_tests(rune_pure_tests)
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_pure_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/transport/TransportConfig.h' file not found`.

- [ ] **Step 3: Create `TransportConfig.h`**

```cpp
// include/rune/client/transport/TransportConfig.h
//
// Aeron-free transport configuration.  Every Aeron knob the client relies on is
// explicit here (spec §4.1/§4.3): MTU, socket buffers, term length, driver idle
// strategy, driver mode and client liveness.  validate() mirrors the driver's
// own rules (aeron_driver_context.c:1372-1390 for the MTU) so a bad value fails
// at start() with a clear message instead of an opaque aeron_driver_init error.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

namespace rune::client::transport {

/// Who drives the embedded media driver and the Aeron client conductor.
enum class DriverMode : uint8_t {
    Thread,   ///< default: SHARED driver thread + Aeron client conductor thread (unchanged)
    Invoker,  ///< AERON_THREADING_MODE_INVOKER + useConductorAgentInvoker(true): the
              ///< owning thread pumps both through AeronTransport::dutyCycle()
};

inline constexpr std::size_t kAeronDataHeaderLength    = 32;     // AERON_DATA_HEADER_LENGTH
inline constexpr std::size_t kAeronMaxUdpPayloadLength = 65504;  // AERON_MAX_UDP_PAYLOAD_LENGTH
inline constexpr std::size_t kAeronFrameAlignment      = 32;     // AERON_LOGBUFFER_FRAME_ALIGNMENT
inline constexpr std::size_t kMinTermLength            = 64u * 1024;
inline constexpr std::size_t kMaxTermLength            = 1024u * 1024 * 1024;

struct TransportConfig {
    std::string ingressChannel;        ///< Channel the client publishes to (→ gateway)
    int32_t     ingressStreamId = 0;   ///< Stream id for the default ingress publication
    std::string egressChannel;         ///< Channel the client subscribes on (← gateway)
    int32_t     egressStreamId  = 0;   ///< Stream id for the egress subscription
    std::string dataDir;               ///< Root dir; the driver lives at dataDir/aeron
    /// Term buffer length of every log buffer on this driver: power of two in
    /// [64 KiB, 1 GiB].  Default 4 MiB.
    std::size_t termBufferLength = 4u * 1024 * 1024;
    /// UDP MTU of every publication on this driver (Aeron default 1408).  8 KiB:
    /// measured 2026-09-30, 70% of all datagrams on the bench box were 1.4 KB
    /// client produce fragments; 8 KiB halved datagrams and cut p50 2-3x.
    /// Multiple of 32 in (32, 65504].  Env: RUNE_CLIENT_AERON_MTU.
    std::size_t mtuLength = 8192;
    /// SO_SNDBUF / SO_RCVBUF of the driver's UDP sockets (0 = OS default).
    /// Env: RUNE_AERON_SOCKET_BUFFER_BYTES (sets both, as before).
    std::size_t socketSndBufBytes = 2u * 1024 * 1024;
    std::size_t socketRcvBufBytes = 2u * 1024 * 1024;
    /// Thread-mode driver idle strategy (Aeron name: sleep-ns, backoff, noop, ...).
    /// Env: RUNE_CLIENT_AERON_DRIVER_IDLE.  Unused in Invoker mode.
    std::string driverIdleStrategy = "sleep-ns";
    DriverMode  driverMode = DriverMode::Thread;
    /// Driver client-liveness timeout (Aeron default 10 s).  In Invoker mode the
    /// owning thread must call dutyCycle() at least this often, otherwise the
    /// Aeron ClientConductor inter-service timeout closes the client.
    int64_t clientLivenessTimeoutNs = 10'000'000'000LL;
};

/// Returns an error message if any explicit knob violates Aeron's rules.
[[nodiscard]] inline std::optional<std::string> validate(const TransportConfig& c) {
    if (c.mtuLength <= kAeronDataHeaderLength || c.mtuLength > kAeronMaxUdpPayloadLength) {
        return "mtuLength must be > 32 and <= 65504: " + std::to_string(c.mtuLength);
    }
    if ((c.mtuLength & (kAeronFrameAlignment - 1)) != 0) {
        return "mtuLength must be a multiple of 32: " + std::to_string(c.mtuLength);
    }
    const std::size_t t = c.termBufferLength;
    if (t < kMinTermLength || t > kMaxTermLength || (t & (t - 1)) != 0) {
        return "termBufferLength must be a power of two in [65536, 1073741824]: " +
               std::to_string(t);
    }
    if (c.socketSndBufBytes != 0 && c.socketSndBufBytes < c.mtuLength) {
        return "socketSndBufBytes must be 0 (OS default) or >= mtuLength";
    }
    if (c.socketRcvBufBytes != 0 && c.socketRcvBufBytes < c.mtuLength) {
        return "socketRcvBufBytes must be 0 (OS default) or >= mtuLength";
    }
    if (c.clientLivenessTimeoutNs <= 0) {
        return "clientLivenessTimeoutNs must be > 0";
    }
    return std::nullopt;
}

/// Environment overrides win over code configuration (the historical semantics
/// of RUNE_AERON_SOCKET_BUFFER_BYTES).  Malformed values are ignored.
inline void applyEnvOverrides(TransportConfig& c) {
    auto readSize = [](const char* name, std::size_t& out) {
        const char* v = std::getenv(name);
        if (v == nullptr || *v == '\0') return;
        char* end = nullptr;
        const unsigned long long x = std::strtoull(v, &end, 10);
        if (end != nullptr && *end == '\0') out = static_cast<std::size_t>(x);
    };
    readSize("RUNE_CLIENT_AERON_MTU", c.mtuLength);
    std::size_t sock = static_cast<std::size_t>(-1);
    readSize("RUNE_AERON_SOCKET_BUFFER_BYTES", sock);
    if (sock != static_cast<std::size_t>(-1)) {
        c.socketSndBufBytes = sock;
        c.socketRcvBufBytes = sock;
    }
    if (const char* v = std::getenv("RUNE_CLIENT_AERON_DRIVER_IDLE"); v != nullptr && *v != '\0') {
        c.driverIdleStrategy = v;
    }
}

} // namespace rune::client::transport
```

- [ ] **Step 4: Run the pure tests**

Run: `cmake --build cmake-build-sic --target rune_pure_tests -j8 && ./cmake-build-sic/rune_pure_tests`
Expected: PASS (7 tests).

- [ ] **Step 5: Write the failing real-driver test**

Append to `tests/transport/AeronTransportTest.cpp`:

```cpp
// The configured MTU reaches the driver: a UDP publication's max payload is
// mtuLength - DATA_HEADER_LENGTH (32).  Aeron's default would give 1408 - 32.
TEST(AeronTransport, UdpPublicationHonoursConfiguredMtu) {
    Config cfg = makeLoopbackConfig(1020);
    cfg.ingressChannel = "aeron:udp?endpoint=127.0.0.1:24711";  // no subscriber needed
    cfg.mtuLength      = 8192;
    Transport t{cfg};
    t.start();
    EXPECT_EQ(t.ingressMaxPayloadLength(), 8192 - 32);
    t.stop();
}

TEST(AeronTransport, InvalidMtuFailsStartWithClearError) {
    Config cfg = makeLoopbackConfig(1021);
    cfg.mtuLength = 1000;  // not a multiple of 32
    Transport t{cfg};
    try {
        t.start();
        FAIL() << "start() must reject an invalid MTU";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("multiple of 32"), std::string::npos);
    }
}
```

Run: `cmake --build cmake-build-sic --target rune_transport_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'ingressMaxPayloadLength'` / `no member named 'mtuLength'`.

- [ ] **Step 6: Wire the config into `AeronTransport`**

`AeronTransport.h`: replace the `TransportConfig` struct at `:43-63` with `#include <rune/client/transport/TransportConfig.h>` (placed with the other includes at `:23-39`), and add to the public "Readiness" block:

```cpp
    /// Max single-frame payload of the default ingress publication
    /// (= mtuLength - 32 for UDP).  0 before start().
    [[nodiscard]] int32_t ingressMaxPayloadLength() const noexcept {
        return ingressPub_ ? ingressPub_->maxPayloadLength() : 0;
    }
```

`AeronTransport.cpp` `start()` — insert right after the `started_` CAS block (`:52`):

```cpp
    if (auto err = validate(cfg_)) {
        throw std::invalid_argument("AeronTransport: invalid TransportConfig: " + *err);
    }
```

`startEmbeddedDriver()` — replace `:493-513` (threading mode through the socket-buffer block) with:

```cpp
    aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
    aeron_driver_context_set_shared_idle_strategy(driverCtx_, cfg_.driverIdleStrategy.c_str());
    aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
    aeron_driver_context_set_term_buffer_length(
        driverCtx_, static_cast<uint64_t>(cfg_.termBufferLength));
    aeron_driver_context_set_mtu_length(driverCtx_, cfg_.mtuLength);
    aeron_driver_context_set_client_liveness_timeout_ns(
        driverCtx_, static_cast<uint64_t>(cfg_.clientLivenessTimeoutNs));
    if (cfg_.socketSndBufBytes > 0) {
        aeron_driver_context_set_socket_so_sndbuf(driverCtx_, cfg_.socketSndBufBytes);
    }
    if (cfg_.socketRcvBufBytes > 0) {
        aeron_driver_context_set_socket_so_rcvbuf(driverCtx_, cfg_.socketRcvBufBytes);
    }
```

(The `std::getenv("RUNE_AERON_SOCKET_BUFFER_BYTES")` read moves to `applyEnvOverrides`, applied by `toAgentConfig` below.)

- [ ] **Step 7: Surface the knobs on `ClientConfig`**

`Client.h`, inside `struct ClientConfig` after `termBufferBytes` (`:134`):

```cpp
    // Explicit Aeron knobs (spec §4.1/§4.3).  Env overrides (RUNE_CLIENT_AERON_MTU,
    // RUNE_AERON_SOCKET_BUFFER_BYTES, RUNE_CLIENT_AERON_DRIVER_IDLE) win.
    std::size_t mtuBytes          = 8192;
    std::size_t socketBufferBytes = 2u * 1024 * 1024;  // SO_SNDBUF and SO_RCVBUF
```

and after the `ClientConfig` struct:

```cpp
/// Maps a ClientConfig to the agent/transport configuration (applies env overrides).
[[nodiscard]] agent::AgentConfig toAgentConfig(const ClientConfig& c);
```

`Client.cpp`: rename `makeAgentConfig` to `toAgentConfig`, move it out of the anonymous namespace (define it in `namespace rune::client`), and add before `return acfg;`:

```cpp
    acfg.transport.mtuLength         = c.mtuBytes;
    acfg.transport.socketSndBufBytes = c.socketBufferBytes;
    acfg.transport.socketRcvBufBytes = c.socketBufferBytes;
    transport::applyEnvOverrides(acfg.transport);
```

Update the constructor initializer `agent_(makeAgentConfig(cfg_))` (`:84`) to `agent_(toAgentConfig(cfg_))`.

- [ ] **Step 8: Run the transport tests, the regression suite and the FULL real-driver suite**

A driver-default change must be proven on every real-driver test, not only the config tests.

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_transport_tests
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: transport tests PASS including `UdpPublicationHonoursConfiguredMtu` and `InvalidMtuFailsStartWithClearError`; full suite matches `/tmp/sic-baseline.txt`.

- [ ] **Step 9: Commit**

```bash
git add include/rune/client/transport/TransportConfig.h include/rune/client/transport/AeronTransport.h \
        src/transport/AeronTransport.cpp include/rune/client/Client.h src/Client.cpp \
        tests/transport/TransportConfigTest.cpp tests/transport/AeronTransportTest.cpp CMakeLists.txt
git commit -m "feat(transport): explicit MTU (8 KiB), socket buffers, idle strategy, liveness

Every Aeron knob the client relies on is now explicit TransportConfig with
validation mirroring the driver's own rules and env overrides
(RUNE_CLIENT_AERON_MTU, RUNE_AERON_SOCKET_BUFFER_BYTES,
RUNE_CLIENT_AERON_DRIVER_IDLE).  MTU defaults to 8 KiB per spec 4.1
(1.4 KB fragments were 70% of all datagrams on the bench box).

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `FlatIndexMap` — node-free hot-path index

Every hot-path lookup this plan introduces (route slots, per-stream publications, partition slots) needs an `int64 → uint32` map without node allocation (spec §1a.5). One primitive, tested once.

**Files:**
- Create: `include/rune/client/detail/FlatIndexMap.h`
- Create: `tests/detail/FlatIndexMapTest.cpp`
- Modify: `CMakeLists.txt` (`rune_pure_tests` sources)

**Interfaces:**
- Consumes: nothing.
- Produces: `class rune::client::detail::FlatIndexMap { static constexpr uint32_t kNone; explicit FlatIndexMap(uint32_t initialCapacityPow2 = 16); uint32_t find(int64_t) const noexcept; bool insert(int64_t key, uint32_t value); bool erase(int64_t) noexcept; uint32_t size() const noexcept; uint32_t capacity() const noexcept; template<class F> void forEach(F&&) const; };`

- [ ] **Step 1: Write the failing tests**

Create `tests/detail/FlatIndexMapTest.cpp`:

```cpp
// tests/detail/FlatIndexMapTest.cpp
#include <rune/client/detail/FlatIndexMap.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <unordered_map>

using rune::client::detail::FlatIndexMap;

namespace {
int64_t partKey(int32_t topic, int32_t partition) {
    return (static_cast<int64_t>(topic) << 32) | static_cast<uint32_t>(partition);
}
} // namespace

TEST(FlatIndexMap, FindOnEmptyReturnsNone) {
    FlatIndexMap m;
    EXPECT_EQ(m.find(42), FlatIndexMap::kNone);
    EXPECT_EQ(m.size(), 0u);
}

TEST(FlatIndexMap, InsertFindOverwrite) {
    FlatIndexMap m;
    EXPECT_TRUE(m.insert(partKey(1, 0), 7));
    EXPECT_FALSE(m.insert(partKey(1, 0), 9));   // overwrite, not new
    EXPECT_EQ(m.find(partKey(1, 0)), 9u);
    EXPECT_EQ(m.size(), 1u);
}

TEST(FlatIndexMap, ExtremeKeys) {
    FlatIndexMap m;
    const int64_t keys[] = {0, -1, std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<int64_t>::max(), partKey(-5, 3)};
    uint32_t v = 0;
    for (int64_t k : keys) m.insert(k, v++);
    v = 0;
    for (int64_t k : keys) EXPECT_EQ(m.find(k), v++);
}

TEST(FlatIndexMap, GrowsAndKeepsEveryKey) {
    FlatIndexMap m(4);
    for (int32_t t = 0; t < 10; ++t)
        for (int32_t p = 0; p < 1000; ++p)
            m.insert(partKey(t, p), static_cast<uint32_t>(t * 1000 + p));
    EXPECT_EQ(m.size(), 10'000u);
    EXPECT_LE(m.size() * 2, m.capacity());   // load factor <= 1/2
    for (int32_t t = 0; t < 10; ++t)
        for (int32_t p = 0; p < 1000; ++p)
            ASSERT_EQ(m.find(partKey(t, p)), static_cast<uint32_t>(t * 1000 + p));
}

// Backward-shift deletion must keep every probe run reachable.  Compare against a
// reference map over 200k random operations on a small key space (many collisions).
TEST(FlatIndexMap, RandomisedAgainstReference) {
    FlatIndexMap m(8);
    std::unordered_map<int64_t, uint32_t> ref;
    std::mt19937_64 rng(42);
    for (int op = 0; op < 200'000; ++op) {
        const int64_t k = static_cast<int64_t>(rng() % 4096) - 2048;
        switch (rng() % 3) {
        case 0: {
            const uint32_t v = static_cast<uint32_t>(rng() % 1'000'000);
            EXPECT_EQ(m.insert(k, v), ref.find(k) == ref.end());
            ref[k] = v;
            break;
        }
        case 1:
            EXPECT_EQ(m.erase(k), ref.erase(k) == 1);
            break;
        default: {
            const auto it = ref.find(k);
            ASSERT_EQ(m.find(k), it == ref.end() ? FlatIndexMap::kNone : it->second);
        }
        }
    }
    EXPECT_EQ(m.size(), ref.size());
    for (const auto& [k, v] : ref) ASSERT_EQ(m.find(k), v);
}

TEST(FlatIndexMap, ForEachVisitsEveryLiveEntryOnce) {
    FlatIndexMap m;
    for (uint32_t i = 0; i < 50; ++i) m.insert(i * 7919, i);
    for (uint32_t i = 0; i < 50; i += 2) m.erase(i * 7919);
    uint32_t seen = 0, sum = 0;
    m.forEach([&](int64_t, uint32_t v) { ++seen; sum += v; });
    EXPECT_EQ(seen, 25u);
    EXPECT_EQ(sum, 25u * 25u);   // 1 + 3 + ... + 49
}
```

In `CMakeLists.txt` add `tests/detail/FlatIndexMapTest.cpp` to the `add_executable(rune_pure_tests ...)` source list.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_pure_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/detail/FlatIndexMap.h' file not found`.

- [ ] **Step 3: Implement**

Create `include/rune/client/detail/FlatIndexMap.h`:

```cpp
// include/rune/client/detail/FlatIndexMap.h
//
// Open-addressing int64 -> uint32 map for hot-path lookups (spec §1a.5: no
// node-based containers on per-record / per-batch / per-duty-cycle paths).
// Linear probing over a power-of-two slot array kept at load factor <= 1/2.
// find() never allocates; insert() allocates only when it grows (first sight of
// a key = cold path).  erase() uses backward-shift deletion (no tombstones, so
// probe runs never degrade).  Single-threaded: owned by one agent/shard.
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::client::detail {

class FlatIndexMap {
public:
    static constexpr uint32_t kNone = 0xFFFFFFFFu;

    explicit FlatIndexMap(uint32_t initialCapacityPow2 = 16)
        : slots_(roundUpPow2(initialCapacityPow2 < 4 ? 4 : initialCapacityPow2)),
          mask_(static_cast<uint32_t>(slots_.size() - 1)) {}

    [[nodiscard]] uint32_t find(int64_t key) const noexcept {
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            const Slot& s = slots_[i];
            if (!s.used) return kNone;
            if (s.key == key) return s.value;
            i = (i + 1) & mask_;
        }
    }

    /// Inserts or overwrites.  Returns true when the key was not present.
    bool insert(int64_t key, uint32_t value) {
        if ((static_cast<std::size_t>(size_) + 1) * 2 > slots_.size()) grow();
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            Slot& s = slots_[i];
            if (!s.used) {
                s = Slot{key, value, true};
                ++size_;
                return true;
            }
            if (s.key == key) {
                s.value = value;
                return false;
            }
            i = (i + 1) & mask_;
        }
    }

    bool erase(int64_t key) noexcept {
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            if (!slots_[i].used) return false;
            if (slots_[i].key == key) break;
            i = (i + 1) & mask_;
        }
        uint32_t hole = i;
        uint32_t j    = i;
        for (;;) {
            j = (j + 1) & mask_;
            if (!slots_[j].used) break;
            const uint32_t home = static_cast<uint32_t>(mix(slots_[j].key)) & mask_;
            // Slot j stays iff its home lies cyclically in (hole, j].
            const bool stays = (hole <= j) ? (home > hole && home <= j)
                                           : (home > hole || home <= j);
            if (stays) continue;
            slots_[hole] = slots_[j];
            hole = j;
        }
        slots_[hole].used = false;
        --size_;
        return true;
    }

    [[nodiscard]] uint32_t size() const noexcept { return size_; }
    [[nodiscard]] uint32_t capacity() const noexcept {
        return static_cast<uint32_t>(slots_.size());
    }

    template <class F>
    void forEach(F&& f) const {
        for (const Slot& s : slots_) {
            if (s.used) f(s.key, s.value);
        }
    }

private:
    struct Slot {
        int64_t  key   = 0;
        uint32_t value = 0;
        bool     used  = false;
    };

    static uint64_t mix(int64_t k) noexcept {  // murmur3 fmix64
        uint64_t x = static_cast<uint64_t>(k);
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }

    static std::size_t roundUpPow2(uint32_t v) noexcept {
        std::size_t p = 1;
        while (p < v) p <<= 1;
        return p;
    }

    void grow() {
        std::vector<Slot> old;
        old.swap(slots_);
        slots_.assign(old.size() * 2, Slot{});
        mask_ = static_cast<uint32_t>(slots_.size() - 1);
        size_ = 0;
        for (const Slot& s : old) {
            if (s.used) insert(s.key, s.value);
        }
    }

    std::vector<Slot> slots_;
    uint32_t          mask_ = 0;
    uint32_t          size_ = 0;
};

} // namespace rune::client::detail
```

- [ ] **Step 4: Run to verify pass**

Run: `cmake --build cmake-build-sic --target rune_pure_tests -j8 && ./cmake-build-sic/rune_pure_tests --gtest_filter='FlatIndexMap.*'`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add include/rune/client/detail/FlatIndexMap.h tests/detail/FlatIndexMapTest.cpp CMakeLists.txt
git commit -m "feat(detail): FlatIndexMap — open-addressing int64->uint32 index map

Node-free lookup for the hot paths the sharded client flattens (route slots,
per-stream publications, partition slots).  find() never allocates; erase()
uses backward-shift deletion; verified against a reference map.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `Resolution<Handle>` — rich-state registration state machine

Replaces every "add, then sleep 1 ms until found, up to 5 s" loop (`AeronTransport.cpp:84-93, 229-237, 268-276, 379-386, 434-442`) with explicit states (spec §1a.4, §3.5a.3, §3.6). This task builds and proves the machine in isolation with a fake registry and an injected clock.

**Files:**
- Create: `include/rune/client/transport/Resolution.h`
- Create: `tests/transport/ResolutionTest.cpp`
- Modify: `CMakeLists.txt` (`rune_pure_tests` sources)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `enum class ResolveStatus : uint8_t { Pending, Ready, Failed };`
  - `template <class Handle> class Resolution` with states `Unstarted`, `Resolving{int64_t regId; int64_t deadlineNs;}`, `Ready{std::shared_ptr<Handle> handle;}`, `TimedOut{int64_t retryAtNs;}`, `Rejected{}`; `Resolution(int64_t timeoutNs, int64_t retryBackoffNs) noexcept`; `template<class Registry> ResolveStatus poll(Registry&, int64_t nowNs) noexcept`; `Handle* ready() const noexcept`; `const std::shared_ptr<Handle>* readyShared() const noexcept`; `bool rejected() const noexcept`; `const State& state() const noexcept`; `void reset() noexcept`.
  - Registry concept: `int64_t add()`; `std::shared_ptr<Handle> find(int64_t regId)` (nullptr while pending, throws on driver rejection).

- [ ] **Step 1: Write the failing tests**

Create `tests/transport/ResolutionTest.cpp`:

```cpp
// tests/transport/ResolutionTest.cpp — Resolution state machine, fake registry,
// injected clock (no sleeps, no Aeron).
#include <rune/client/transport/Resolution.h>

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <variant>

using rune::client::transport::Resolution;
using rune::client::transport::ResolveStatus;

namespace {

struct FakeHandle { int64_t regId; };

struct FakeRegistry {
    int     adds = 0;
    int     finds = 0;
    int64_t nextRegId = 100;
    int     readyOnFind = -1;      // find() #N returns a handle; -1 = never
    bool    rejectOnFind = false;
    bool    throwOnAdd = false;

    int64_t add() {
        if (throwOnAdd) throw std::runtime_error("invalid channel");
        ++adds;
        return nextRegId++;
    }
    std::shared_ptr<FakeHandle> find(int64_t regId) {
        ++finds;
        if (rejectOnFind) throw std::runtime_error("registration rejected");
        if (readyOnFind >= 0 && finds >= readyOnFind) {
            return std::make_shared<FakeHandle>(FakeHandle{regId});
        }
        return nullptr;
    }
};

using Res = Resolution<FakeHandle>;
constexpr int64_t kTimeout = 1'000;
constexpr int64_t kBackoff = 50;

} // namespace

TEST(Resolution, UnstartedIssuesExactlyOneRegistration) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Pending);
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(reg.finds, 0);
    ASSERT_TRUE(std::holds_alternative<Res::Resolving>(r.state()));
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 100);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).deadlineNs, kTimeout);
}

TEST(Resolution, BecomesReadyWhenRegistryHasHandleAndIsSticky) {
    FakeRegistry reg;
    reg.readyOnFind = 2;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Pending);    // add
    EXPECT_EQ(r.poll(reg, 10), ResolveStatus::Pending);   // find #1
    EXPECT_EQ(r.poll(reg, 20), ResolveStatus::Ready);     // find #2
    ASSERT_NE(r.ready(), nullptr);
    EXPECT_EQ(r.ready()->regId, 100);
    const int findsAtReady = reg.finds;
    EXPECT_EQ(r.poll(reg, 5'000'000), ResolveStatus::Ready);
    EXPECT_EQ(reg.finds, findsAtReady) << "Ready never touches the registry again";
}

TEST(Resolution, TimesOutExactlyAtDeadline) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    EXPECT_EQ(r.poll(reg, kTimeout - 1), ResolveStatus::Pending);
    EXPECT_EQ(r.poll(reg, kTimeout), ResolveStatus::Failed);
    ASSERT_TRUE(std::holds_alternative<Res::TimedOut>(r.state()));
    EXPECT_EQ(std::get<Res::TimedOut>(r.state()).retryAtNs, kTimeout + kBackoff);
    EXPECT_FALSE(r.rejected());
}

TEST(Resolution, RetriesWithFreshRegistrationAfterBackoff) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    r.poll(reg, kTimeout);                                           // TimedOut
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff - 1), ResolveStatus::Failed);
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff), ResolveStatus::Pending);
    EXPECT_EQ(reg.adds, 2);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 101);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).deadlineNs,
              kTimeout + kBackoff + kTimeout);
}

TEST(Resolution, DriverRejectionIsTerminal) {
    FakeRegistry reg;
    reg.rejectOnFind = true;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    EXPECT_EQ(r.poll(reg, 1), ResolveStatus::Failed);
    EXPECT_TRUE(r.rejected());
    EXPECT_EQ(r.poll(reg, 1'000'000'000), ResolveStatus::Failed);
    EXPECT_EQ(reg.adds, 1) << "a rejected channel is never re-registered";
}

TEST(Resolution, AddThrowIsTerminal) {
    FakeRegistry reg;
    reg.throwOnAdd = true;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Failed);
    EXPECT_TRUE(r.rejected());
}

TEST(Resolution, ResetDropsTheHandle) {
    FakeRegistry reg;
    reg.readyOnFind = 1;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    r.poll(reg, 1);
    ASSERT_NE(r.ready(), nullptr);
    std::weak_ptr<FakeHandle> weak = *r.readyShared();
    r.reset();
    EXPECT_EQ(r.ready(), nullptr);
    EXPECT_TRUE(weak.expired());
    EXPECT_TRUE(std::holds_alternative<Res::Unstarted>(r.state()));
}
```

Add `tests/transport/ResolutionTest.cpp` to `rune_pure_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_pure_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/transport/Resolution.h' file not found`.

- [ ] **Step 3: Implement**

Create `include/rune/client/transport/Resolution.h`:

```cpp
// include/rune/client/transport/Resolution.h
//
// Rich-state resolution of one Aeron registration (spec §1a.4, §3.5a.3, §3.6):
//
//   Unstarted ──poll──▶ Resolving(regId, deadlineNs) ──find()!=null──▶ Ready(handle)
//                             │  now >= deadline                         (sticky)
//                             ▼
//                       TimedOut(retryAtNs) ──now >= retryAt──▶ Resolving(fresh regId, ...)
//   add()/find() throws ─────────────────────────────────────▶ Rejected   (terminal)
//
// The owner pumps ITS OWN driver + conductor between polls (invoker mode) or
// lets the background agents run (thread mode).  This class never sleeps, never
// blocks and never reads a clock: nowNs is injected.
#pragma once

#include <cstdint>
#include <memory>
#include <variant>

namespace rune::client::transport {

enum class ResolveStatus : uint8_t { Pending, Ready, Failed };

template <class Handle>
class Resolution {
public:
    struct Unstarted {};
    struct Resolving { int64_t regId; int64_t deadlineNs; };
    struct Ready     { std::shared_ptr<Handle> handle; };
    struct TimedOut  { int64_t retryAtNs; };
    struct Rejected  {};
    using State = std::variant<Unstarted, Resolving, Ready, TimedOut, Rejected>;

    Resolution(int64_t timeoutNs, int64_t retryBackoffNs) noexcept
        : timeoutNs_(timeoutNs), retryBackoffNs_(retryBackoffNs) {}

    template <class Registry>
    ResolveStatus poll(Registry& registry, int64_t nowNs) noexcept {
        if (std::holds_alternative<Ready>(state_)) return ResolveStatus::Ready;
        if (std::holds_alternative<Unstarted>(state_)) return start(registry, nowNs);
        if (const auto* t = std::get_if<TimedOut>(&state_)) {
            return (nowNs >= t->retryAtNs) ? start(registry, nowNs) : ResolveStatus::Failed;
        }
        if (std::holds_alternative<Rejected>(state_)) return ResolveStatus::Failed;

        const Resolving r = std::get<Resolving>(state_);
        std::shared_ptr<Handle> h;
        try {
            h = registry.find(r.regId);
        } catch (...) {
            state_ = Rejected{};
            return ResolveStatus::Failed;
        }
        if (h) {
            state_ = Ready{std::move(h)};
            return ResolveStatus::Ready;
        }
        if (nowNs >= r.deadlineNs) {
            state_ = TimedOut{nowNs + retryBackoffNs_};
            return ResolveStatus::Failed;
        }
        return ResolveStatus::Pending;
    }

    /// Raw pointer to the resolved handle (hot path), nullptr unless Ready.
    [[nodiscard]] Handle* ready() const noexcept {
        const Ready* r = std::get_if<Ready>(&state_);
        return r ? r->handle.get() : nullptr;
    }
    [[nodiscard]] const std::shared_ptr<Handle>* readyShared() const noexcept {
        const Ready* r = std::get_if<Ready>(&state_);
        return r ? &r->handle : nullptr;
    }
    [[nodiscard]] bool rejected() const noexcept {
        return std::holds_alternative<Rejected>(state_);
    }
    [[nodiscard]] const State& state() const noexcept { return state_; }

    /// Drops the handle on the owning thread and returns to Unstarted.
    void reset() noexcept { state_ = Unstarted{}; }

private:
    template <class Registry>
    ResolveStatus start(Registry& registry, int64_t nowNs) noexcept {
        try {
            state_ = Resolving{registry.add(), nowNs + timeoutNs_};
        } catch (...) {
            state_ = Rejected{};
            return ResolveStatus::Failed;
        }
        return ResolveStatus::Pending;
    }

    int64_t timeoutNs_;
    int64_t retryBackoffNs_;
    State   state_{Unstarted{}};
};

} // namespace rune::client::transport
```

- [ ] **Step 4: Run to verify pass**

Run: `cmake --build cmake-build-sic --target rune_pure_tests -j8 && ./cmake-build-sic/rune_pure_tests --gtest_filter='Resolution.*'`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add include/rune/client/transport/Resolution.h tests/transport/ResolutionTest.cpp CMakeLists.txt
git commit -m "feat(transport): Resolution state machine for Aeron registrations

Unstarted -> Resolving(regId, deadlineNs) -> Ready(handle) | TimedOut(retryAtNs)
| Rejected, with an injected clock and no sleeping.  Replaces the add-then-sleep
resolve loops in the next tasks (spec 3.5a.3, 3.6).

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Invoker driver mode — the owner thread runs driver + conductor

**Driver threading-mode decision (Aeron C 1.41.4, `~/.conan2/p/aeron587c0be5e0d59/s/src/aeron-driver/src/main/c/`; the four `~/.conan2/p/b/aeron*/b/` build trees have byte-identical `aeron_driver.c`):** use `AERON_THREADING_MODE_INVOKER` with `aeron_driver_start(driver, /*manual_main_loop=*/true)`.
- `aeron_driver.c:939-957`: `INVOKER` and `SHARED` both init ONLY runner `AERON_AGENT_RUNNER_SHARED` (= index 0, `aeron_driver.h:29`) with `aeron_driver_shared_do_work`, which runs conductor + sender + receiver (`:712-722`).
- `aeron_driver.c:1066-1087`: with `manual_main_loop` runner 0 is marked `AERON_AGENT_STATE_MANUAL` and only runners `1..MAX-1` in state `INITED` get threads — none exist for INVOKER/SHARED. `DEDICATED` (`:988-1030`) and `SHARED_NETWORK` (`:958-986`) init runners 1/2 (sender/receiver or shared-network) and therefore start threads even with a manual main loop — rejected.
- `aeron_driver.c:1091-1100`: `aeron_driver_main_do_work` = `aeron_agent_do_work(&runners[0])` = the whole duty cycle.
- `aeron_driver.c:1054-1059`: INVOKER additionally refuses a non-manual start (`"INVOKER threading mode requires manual_main_loop"`), guarding against accidentally spawning the runner thread — the reason to prefer it over SHARED-with-manual-loop.

Aeron's C++ `defaultErrorHandler` calls `::exit(-1)` (`aeron-client/src/main/cpp/Context.h:145-161`), and in invoker mode an owner stall longer than the client liveness timeout triggers `ClientConductor::onHeartbeatCheckTimeouts` → `closeAllResources` + error handler (`ClientConductor.h:431-442`, `ClientConductor.cpp:988-990` sets `m_isClosed`). A library must never exit its host: install a logging handler and expose `closed()`.

**Files:**
- Modify: `include/rune/client/transport/AeronTransport.h` (public: `dutyCycle`, `closed`, `usesDriverThread`; private: `pumpUntil_`, `monoNs_`)
- Modify: `src/transport/AeronTransport.cpp:47-121` (`start()`), `:467-541` (`startEmbeddedDriver()`)
- Create: `tests/transport/InvokerTransportTest.cpp`
- Modify: `CMakeLists.txt` (new target `rune_invoker_tests` after the `rune_pure_tests` block)

**Interfaces:**
- Consumes: `TransportConfig::driverMode`, `clientLivenessTimeoutNs` (Task 3); `pumpDriverFromCaller_()` (Task 2).
- Produces: `int AeronTransport::dutyCycle() noexcept;` (Invoker: driver duty cycle then client conductor invoke; Thread: 0), `[[nodiscard]] bool AeronTransport::closed() const noexcept;`, `[[nodiscard]] bool AeronTransport::usesDriverThread() const noexcept;`, private `template<class Ready> bool pumpUntil_(Ready&&, int64_t timeoutNs);`, `static int64_t monoNs_() noexcept;`. Test target `rune_invoker_tests` linking `rune_client_api` + `GTest::gtest_main`.

- [ ] **Step 1: Write the failing tests**

Create `tests/transport/InvokerTransportTest.cpp`:

```cpp
// tests/transport/InvokerTransportTest.cpp — AeronTransport in DriverMode::Invoker.
// Every drive loop is bounded by kPumpCap duty cycles and needs no sleep: the
// test thread IS the driver.
#include <rune/client/transport/AeronTransport.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using Transport  = rune::client::transport::AeronTransport;
using Config     = rune::client::transport::TransportConfig;
using DriverMode = rune::client::transport::DriverMode;

namespace {

std::atomic<int> g_ctr{0};

std::string uniqueDir(const char* prefix) {
    return std::string("/tmp/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) +
           "_" + std::to_string(g_ctr.fetch_add(1));
}

Config invokerConfig(const std::string& ingress, const std::string& egress, int32_t stream) {
    Config c;
    c.ingressChannel  = ingress;
    c.ingressStreamId = stream;
    c.egressChannel   = egress;
    c.egressStreamId  = stream;
    c.dataDir         = uniqueDir("inv_tr");
    c.driverMode      = DriverMode::Invoker;
    return c;
}

constexpr int kPumpCap = 200'000;

template <class Pred>
bool pumpUntil(Transport& t, Pred&& pred) {
    for (int i = 0; i < kPumpCap; ++i) {
        t.dutyCycle();
        if (pred()) return true;
    }
    return false;
}

} // namespace

// Regression: invoker mode has no background thread, so start() must pump its
// OWN driver + conductor while resolving; a sleep-until-found loop would wait
// for a thread that does not exist (spec §3.6).
TEST(InvokerTransport, StartResolvesWithoutBackgroundThreadsRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3100)};
    t.start();
    EXPECT_FALSE(t.usesDriverThread());
    EXPECT_EQ(t.callerPumpViolations(), 0);
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    t.stop();
}

// Proof that nothing but the owner's dutyCycle() moves data: UDP needs the
// driver's sender and receiver agents.
TEST(InvokerTransport, UdpLoopbackMovesOnlyWhenOwnerRunsDutyCycle) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24721";
    Transport t{invokerConfig(ch, ch, 3101)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    const uint8_t payload[] = {1, 2, 3, 4, 5, 6, 7, 8};
    ASSERT_TRUE(pumpUntil(t, [&] { return t.offer(payload, sizeof payload); }));

    int stolen = 0;
    for (int i = 0; i < 10'000; ++i) {
        stolen += t.poll([](const uint8_t*, std::size_t) {}, 10);
    }
    EXPECT_EQ(stolen, 0) << "a background thread moved UDP data in invoker mode";

    std::vector<uint8_t> echo;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.poll([&](const uint8_t* b, std::size_t n) { echo.assign(b, b + n); }, 10);
        return !echo.empty();
    }));
    EXPECT_EQ(echo, std::vector<uint8_t>(std::begin(payload), std::end(payload)));
    t.stop();
}

// Regression (Review Focus 1): an owner stall past the client-liveness timeout
// must close the Aeron client and report closed() — never ::exit(-1) the process
// (Aeron's default error handler) and never hang.  This is the one test that
// needs a real pause: Aeron's ClientConductor reads its own epoch clock
// (ClientConductor.h:436) and cannot be injected.
TEST(InvokerTransport, StalledOwnerSurfacesClosedInsteadOfExitingRegression) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3102);
    c.clientLivenessTimeoutNs = 200'000'000;  // 200 ms
    Transport t{c};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    // Stall exactly past the condition Aeron checks — no tuned duration.  The
    // conductor's clock (aeron::currentTimeMillis, hard-wired in Aeron.cpp:43)
    // cannot be injected, so wait on THAT clock with THAT comparison
    // (ClientConductor.h:436: nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs).
    // t0 is taken after the last conductor doWork, so t0 >= timeOfLastDoWorkMs.
    const long long livenessMs = c.clientLivenessTimeoutNs / 1'000'000;
    const long long t0 = aeron::currentTimeMillis();
    while (aeron::currentTimeMillis() <= t0 + livenessMs) std::this_thread::yield();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.closed(); }))
        << "inter-service timeout must close the client and be observable";
    const uint8_t b[] = {1};
    EXPECT_FALSE(t.offer(b, 1));
    t.stop();
}

TEST(InvokerTransport, ThreadModeIsUnchanged) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3103);
    c.driverMode = DriverMode::Thread;
    Transport t{c};
    t.start();
    EXPECT_TRUE(t.usesDriverThread());
    EXPECT_EQ(t.dutyCycle(), 0) << "thread mode: the driver thread owns the duty cycle";
    t.stop();
}
```

Add to `CMakeLists.txt` right after the `rune_pure_tests` block:

```cmake
    # ── Sharded invoker client: Aeron-backed tests (single-threaded where possible) ──
    if(aeron_FOUND)
        add_executable(rune_invoker_tests
            tests/transport/InvokerTransportTest.cpp
        )
        target_include_directories(rune_invoker_tests PRIVATE ${RUNE_PROTOCOL_INCLUDE})
        target_link_libraries(rune_invoker_tests PRIVATE rune_client_api GTest::gtest_main)
        if(UNIX AND NOT APPLE)
            find_library(UUID_LIB_INVOKER uuid)
            if(UUID_LIB_INVOKER)
                target_link_libraries(rune_invoker_tests PRIVATE ${UUID_LIB_INVOKER})
            endif()
        endif()
        gtest_discover_tests(rune_invoker_tests)
    endif()
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'usesDriverThread'` (and `dutyCycle`, `closed`).

- [ ] **Step 3: Implement invoker mode**

`AeronTransport.h` — public, after `stop()`:

```cpp
    /// Invoker mode: one pass of this transport's OWN embedded driver duty cycle
    /// (conductor + sender + receiver, aeron_driver.c:712-722) followed by the Aeron
    /// client conductor (useConductorAgentInvoker).  Thread mode: returns 0 — the
    /// driver thread and conductor thread own those duty cycles.
    int dutyCycle() noexcept;

    /// True once the Aeron client closed itself (inter-service timeout, driver
    /// timeout).  Offers then fail; the owner decides whether to rebuild.
    [[nodiscard]] bool closed() const noexcept { return aeron_ && aeron_->isClosed(); }

    /// True when a background driver thread exists (Thread mode after start()).
    [[nodiscard]] bool usesDriverThread() const noexcept { return driverThread_.joinable(); }
```

private, after `pumpDriverFromCaller_`:

```cpp
    /// Pumps THIS transport's driver + conductor (Invoker) or backs off while the
    /// background agents work (Thread) until ready() or timeoutNs elapses.  Never
    /// waits on another component of the calling thread (spec §3.6).
    template <class Ready>
    bool pumpUntil_(Ready&& ready, int64_t timeoutNs);
    static int64_t monoNs_() noexcept;
```

`AeronTransport.cpp` — add after the includes (`#include <cstdio>` is new):

```cpp
#include <cstdio>

namespace rune::client::transport {

int64_t AeronTransport::monoNs_() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

template <class Ready>
bool AeronTransport::pumpUntil_(Ready&& ready, int64_t timeoutNs) {
    const int64_t deadline = monoNs_() + timeoutNs;
    for (;;) {
        dutyCycle();
        if (ready()) return true;
        if (monoNs_() >= deadline) return false;
        if (cfg_.driverMode == DriverMode::Thread) {
            // Background driver/conductor threads do the work; back off briefly.
            std::this_thread::sleep_for(std::chrono::microseconds{50});
        }
    }
}

int AeronTransport::dutyCycle() noexcept {
    if (cfg_.driverMode != DriverMode::Invoker || driver_ == nullptr) return 0;
    int work = pumpDriverFromCaller_();  // spec §3.1 order: driver, then client conductor
    if (aeron_) {
        try {
            work += aeron_->conductorAgentInvoker().invoke();
        } catch (...) {
            // AgentInvoker routes SourcedException to the error handler; anything
            // else is contained here — dutyCycle() is noexcept.
        }
    }
    return work;
}
```

(the existing `namespace rune::client::transport {` opening line moves up accordingly — one namespace block.)

In `start()`, replace step 3 (`:65-75`) with:

```cpp
    const std::string aeronDir = (fs::path{cfg_.dataDir} / "aeron").string();
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        ctx.useConductorAgentInvoker(cfg_.driverMode == DriverMode::Invoker);
        // Aeron's default handler calls ::exit(-1) (Context.h:145-161).  A client
        // library must never exit its host: log, and let closed() report it.
        ctx.errorHandler([](const std::exception& e) {
            std::fprintf(stderr, "rune-client: aeron client error: %s\n", e.what());
        });
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            stopEmbeddedDriver();
            throw std::runtime_error("AeronTransport: Aeron::connect failed");
        }
    }
```

and replace the resolve loop (`:83-92`) with:

```cpp
    const bool resolved = pumpUntil_([&] {
        if (!ingressPub_) ingressPub_ = aeron_->findExclusivePublication(pubRegId);
        if (!egressSub_)  egressSub_  = aeron_->findSubscription(subRegId);
        return ingressPub_ && egressSub_;
    }, static_cast<int64_t>(kResolveMaxMs) * 1'000'000LL);
    (void)resolved;  // the !ingressPub_ || !egressSub_ check below reports failure
```

In `startEmbeddedDriver()`:
- replace `aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);` with

```cpp
    // INVOKER: one runner (conductor+sender+receiver), no threads, refuses a
    // non-manual start (aeron_driver.c:939-957, :1054-1087).
    aeron_driver_context_set_threading_mode(
        driverCtx_, cfg_.driverMode == DriverMode::Invoker ? AERON_THREADING_MODE_INVOKER
                                                           : AERON_THREADING_MODE_SHARED);
```

- guard the thread launch added in Task 2:

```cpp
    if (cfg_.driverMode == DriverMode::Thread) {
        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
            }
        });
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cmake --build cmake-build-sic --target rune_invoker_tests rune_transport_tests -j8 && ./cmake-build-sic/rune_invoker_tests --gtest_filter='InvokerTransport.*' && ./cmake-build-sic/rune_transport_tests`
Expected: PASS (4 invoker tests; the stall test takes ~0.6 s; all transport tests pass).

- [ ] **Step 5: Regression suite, full suite, commit**

```bash
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
git add include/rune/client/transport/AeronTransport.h src/transport/AeronTransport.cpp \
        tests/transport/InvokerTransportTest.cpp CMakeLists.txt
git commit -m "feat(transport): invoker driver mode — owner thread runs driver + conductor

DriverMode::Invoker uses AERON_THREADING_MODE_INVOKER (single runner:
conductor+sender+receiver, no threads, refuses a non-manual start;
aeron_driver.c:939-957, 1054-1100) plus useConductorAgentInvoker(true).
dutyCycle() pumps both; start() pumps its own driver while resolving.
Replaces Aeron's ::exit(-1) default error handler with logging and exposes
closed() so an owner stall surfaces instead of killing the process.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Every resolve loop becomes a `Resolution` — publication handles, async egress subscriptions

**Files:**
- Modify: `include/rune/client/transport/AeronTransport.h:195-251` (private caches replaced; public `PubHandle` API added)
- Modify: `src/transport/AeronTransport.cpp` — `start()` resolve (`:77-103` after Task 6), `stop()` (`:134-152`), `resolveWorkerPub` (`:213-246`, deleted), `resolveEndpointPub` (`:252-285`, deleted), `offerTo` (`:291-313`), `offer(streamId)` (`:319-335`), `poll` (`:341-359`), `addEgressSubscription` (`:365-400`), `retarget` (`:418-461`)
- Modify: `tests/transport/InvokerTransportTest.cpp` (append)

**Interfaces:**
- Consumes: `Resolution<Handle>`, `ResolveStatus` (Task 5); `detail::FlatIndexMap` (Task 4); `pumpUntil_`, `monoNs_`, `dutyCycle` (Task 6).
- Produces:
  - `using AeronTransport::PubHandle = int32_t; static constexpr PubHandle kNoPub = -1;`
  - `PubHandle AeronTransport::publicationHandle(const std::string& channel, int32_t streamId);` (cold; never blocks; may throw `std::bad_alloc`)
  - `bool AeronTransport::offerOn(PubHandle h, const uint8_t* buf, std::size_t len) noexcept;` (hot; false while resolving or back-pressured)
  - `[[nodiscard]] bool AeronTransport::publicationReady(PubHandle h) const noexcept;`
  - `offer(streamId, ...)`, `offerTo(channel, streamId, ...)`, `addEgressSubscription(...)`, `retarget(...)` keep their signatures; none sleeps any more.

- [ ] **Step 1: Write the failing tests**

Append to `tests/transport/InvokerTransportTest.cpp`:

```cpp
// Regression: offer(streamId) used to add a per-worker publication and SLEEP up to
// 5 s for the conductor.  In invoker mode nobody else runs the conductor, so the
// caller blocked for the full timeout.  It must return at once (falling back to
// the default publication while the per-worker one resolves) and the handle must
// resolve through the owner's duty cycle.
TEST(InvokerTransport, PerStreamPublicationResolvesWithoutBlockingRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3200)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected(); }));
    const uint8_t b[] = {9};
    EXPECT_TRUE(t.offer(3201, b, 1)) << "unresolved per-worker stream falls back to default";
    const Transport::PubHandle h = t.publicationHandle("aeron:ipc", 3201);
    EXPECT_FALSE(t.publicationReady(h)) << "offer() must not have blocked until resolution";
    ASSERT_TRUE(pumpUntil(t, [&] { t.offerOn(h, b, 1); return t.publicationReady(h); }));
    EXPECT_EQ(t.publicationHandle("aeron:ipc", 3201), h) << "handles are stable and deduplicated";
    t.stop();
}

TEST(InvokerTransport, EgressSubscriptionResolvesAsynchronouslyAndDelivers) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3300)};
    t.start();
    t.addEgressSubscription("aeron:ipc", 3301);  // returns immediately
    const Transport::PubHandle h = t.publicationHandle("aeron:ipc", 3301);
    const uint8_t payload[] = {0xAB, 0xCD};
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.offerOn(h, payload, sizeof payload);
        t.poll([&](const uint8_t* p, std::size_t n) {
            if (n == sizeof payload) got.assign(p, p + n);
        }, 10);
        return !got.empty();
    }));
    EXPECT_EQ(got, std::vector<uint8_t>(std::begin(payload), std::end(payload)));
    t.stop();
}

TEST(InvokerTransport, OfferToUnknownEndpointNeverBlocks) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3400)};
    t.start();
    const uint8_t b[] = {1};
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(t.offerTo("aeron:udp?endpoint=127.0.0.1:24741", 3401, b, 1));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(500))
        << "offerTo must return while the publication resolves (was a 5 s sleep loop)";
    t.stop();
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no type named 'PubHandle' in 'rune::client::transport::AeronTransport'`.

- [ ] **Step 3: Replace the caches and loops**

`AeronTransport.h`: add includes `#include <rune/client/detail/FlatIndexMap.h>`, `#include <rune/client/transport/Resolution.h>`, `#include <vector>`; drop `<unordered_map>`. Public API (after `offerTo`):

```cpp
    /// Stable handle of the publication for (channel, streamId).  Cold path: the
    /// first call creates the slot; resolution then advances inside offerOn() /
    /// dutyCycle() without ever blocking.
    using PubHandle = int32_t;
    static constexpr PubHandle kNoPub = -1;
    PubHandle publicationHandle(const std::string& channel, int32_t streamId);

    /// Hot path: one bounds check + one variant check + tryClaim.  Returns false
    /// while the publication resolves or is back-pressured (retry next cycle).
    bool offerOn(PubHandle h, const uint8_t* buf, std::size_t len) noexcept;

    [[nodiscard]] bool publicationReady(PubHandle h) const noexcept {
        return h >= 0 && static_cast<std::size_t>(h) < pubSlots_.size() &&
               pubSlots_[static_cast<std::size_t>(h)].res.ready() != nullptr;
    }
```

Private: delete `pubCache_`, `endpointPubCache_`, `EgressSubscription`, `egressSubs_`, `resolveWorkerPub`, `resolveEndpointPub`, `kResolveSliceUs`; add the following at the END of the class, after `kResolveMaxMs` (`:249`) — a static constexpr initializer only sees members declared before it:

```cpp
    struct PubSlot {
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::ExclusivePublication> res;
    };
    struct SubSlot {
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::Subscription> res;
        std::unique_ptr<aeron::FragmentAssembler> assembler;  // built on Ready
    };
    std::vector<PubSlot>  pubSlots_;   // index == PubHandle (stable, never erased)
    detail::FlatIndexMap  streamPub_;  // default-channel streamId -> PubHandle
    std::vector<SubSlot>  subSlots_;   // per-worker dynamic-MDC egress subscriptions

    static constexpr int64_t kResolveTimeoutNs = static_cast<int64_t>(kResolveMaxMs) * 1'000'000LL;
    static constexpr int64_t kResolveRetryNs   = 100'000'000LL;  // 100 ms before re-registering
```

`AeronTransport.cpp` — registries (anonymous namespace inside `rune::client::transport`):

```cpp
namespace {
struct PubRegistry {
    aeron::Aeron&      aeron;
    const std::string& channel;
    int32_t            streamId;
    int64_t add() { return aeron.addExclusivePublication(channel, streamId); }
    std::shared_ptr<aeron::ExclusivePublication> find(int64_t id) {
        return aeron.findExclusivePublication(id);
    }
};
struct SubRegistry {
    aeron::Aeron&      aeron;
    const std::string& channel;
    int32_t            streamId;
    int64_t add() { return aeron.addSubscription(channel, streamId); }
    std::shared_ptr<aeron::Subscription> find(int64_t id) { return aeron.findSubscription(id); }
};
} // namespace
```

`start()` — replace the registration + `pumpUntil_` block from Task 6 and the failure check (`if (!ingressPub_ || !egressSub_) {...}`) with:

```cpp
    Resolution<aeron::ExclusivePublication> pubRes{kResolveTimeoutNs, kResolveRetryNs};
    Resolution<aeron::Subscription>         subRes{kResolveTimeoutNs, kResolveRetryNs};
    PubRegistry preg{*aeron_, cfg_.ingressChannel, cfg_.ingressStreamId};
    SubRegistry sreg{*aeron_, cfg_.egressChannel, cfg_.egressStreamId};
    pumpUntil_([&] {
        const int64_t now = monoNs_();
        const bool pubOk = pubRes.poll(preg, now) == ResolveStatus::Ready;
        const bool subOk = subRes.poll(sreg, now) == ResolveStatus::Ready;
        return (pubOk && subOk) || pubRes.rejected() || subRes.rejected();
    }, kResolveTimeoutNs);
    if (pubRes.ready() == nullptr || subRes.ready() == nullptr) {
        const std::string why = pubRes.rejected() || subRes.rejected() ? "rejected by the driver"
                                                                        : "timed out";
        aeron_.reset();
        stopEmbeddedDriver();
        throw std::runtime_error("AeronTransport: resolving ExclusivePublication/Subscription " + why +
                                 " (ingressChannel=" + cfg_.ingressChannel +
                                 " egressChannel=" + cfg_.egressChannel + ")");
    }
    ingressPub_ = *pubRes.readyShared();
    egressSub_  = *subRes.readyShared();
```

(`pubRes`/`subRes` go out of scope afterwards; `ingressPub_`/`egressSub_` keep the shared owners.)

`stop()` step 1–2 — replace `egressSubs_.clear();` and `pubCache_.clear(); endpointPubCache_.clear();` with:

```cpp
    subSlots_.clear();                  // assemblers + subscriptions (conductor dereg)
    pubSlots_.clear();                  // every per-stream / per-endpoint publication
    streamPub_ = detail::FlatIndexMap{};
```

Delete `resolveWorkerPub` and `resolveEndpointPub`. New definitions:

```cpp
AeronTransport::PubHandle AeronTransport::publicationHandle(const std::string& channel,
                                                            int32_t streamId) {
    for (std::size_t i = 0; i < pubSlots_.size(); ++i) {
        if (pubSlots_[i].streamId == streamId && pubSlots_[i].channel == channel) {
            return static_cast<PubHandle>(i);
        }
    }
    pubSlots_.push_back(PubSlot{channel, streamId,
                                Resolution<aeron::ExclusivePublication>{kResolveTimeoutNs,
                                                                        kResolveRetryNs}});
    return static_cast<PubHandle>(pubSlots_.size() - 1);
}

bool AeronTransport::offerOn(PubHandle h, const uint8_t* buf, std::size_t len) noexcept {
    if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return false;
    PubSlot& s = pubSlots_[static_cast<std::size_t>(h)];
    if (aeron::ExclusivePublication* p = s.res.ready()) return offerToPub(*p, buf, len);
    if (!aeron_) return false;
    PubRegistry reg{*aeron_, s.channel, s.streamId};
    if (s.res.poll(reg, monoNs_()) != ResolveStatus::Ready) return false;
    return offerToPub(*s.res.ready(), buf, len);
}

bool AeronTransport::offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len) noexcept {
    if (ingressStreamId == 0 || ingressStreamId == cfg_.ingressStreamId) return offer(buf, len);
    uint32_t h = streamPub_.find(ingressStreamId);
    if (h == detail::FlatIndexMap::kNone) {
        try {
            h = static_cast<uint32_t>(publicationHandle(cfg_.ingressChannel, ingressStreamId));
            streamPub_.insert(ingressStreamId, h);
        } catch (...) {
            return offer(buf, len);
        }
    }
    const auto ph = static_cast<PubHandle>(h);
    if (offerOn(ph, buf, len)) return true;
    // Unresolved: keep the historical fallback to the default publication (the
    // runtime routes it).  Resolved but back-pressured: report it (retry).
    return publicationReady(ph) ? false : offer(buf, len);
}

bool AeronTransport::offerTo(const std::string& channel, int32_t ingressStreamId,
                             const uint8_t* buf, std::size_t len) noexcept {
    if (channel.empty()) return false;
    if (channel == cfg_.ingressChannel) return offer(ingressStreamId, buf, len);
    PubHandle h = kNoPub;
    try {
        h = publicationHandle(channel,
                              ingressStreamId != 0 ? ingressStreamId : cfg_.ingressStreamId);
    } catch (...) {
        return false;
    }
    return offerOn(h, buf, len);
}
```

`poll()` — replace the per-worker loop:

```cpp
    for (SubSlot& s : subSlots_) {
        aeron::Subscription* sub = s.res.ready();
        if (sub == nullptr) {
            if (!aeron_) continue;
            SubRegistry reg{*aeron_, s.channel, s.streamId};
            if (s.res.poll(reg, monoNs_()) != ResolveStatus::Ready) continue;
            sub = s.res.ready();
            s.assembler = std::make_unique<aeron::FragmentAssembler>(
                [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                       aeron::util::index_t len, aeron::Header&) {
                    if (pollHandler_) {
                        const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                        (*pollHandler_)(p, static_cast<std::size_t>(len));
                    }
                });
        }
        frags += sub->poll(s.assembler->handler(), fragmentLimit);
    }
```

`addEgressSubscription()`:

```cpp
void AeronTransport::addEgressSubscription(const std::string& controlChannel,
                                           int32_t streamId) noexcept {
    if (!aeron_ || streamId == 0 || controlChannel.empty()) return;
    for (const SubSlot& s : subSlots_) {
        if (s.streamId == streamId && s.channel == controlChannel) return;  // idempotent
    }
    try {
        subSlots_.push_back(SubSlot{controlChannel, streamId,
                                    Resolution<aeron::Subscription>{kResolveTimeoutNs,
                                                                    kResolveRetryNs},
                                    nullptr});
    } catch (...) {
    }
}
```

`retarget()` — replace the body inside `try`:

```cpp
        if (!aeron_) return false;
        Resolution<aeron::ExclusivePublication> res{kResolveTimeoutNs, kResolveRetryNs};
        PubRegistry reg{*aeron_, newIngressChannel, cfg_.ingressStreamId};
        pumpUntil_([&] {
            return res.poll(reg, monoNs_()) == ResolveStatus::Ready || res.rejected();
        }, kResolveTimeoutNs);
        if (res.ready() == nullptr) return false;  // old publication stays intact
        // Per-stream publications on the OLD default channel are released and
        // lazily re-resolved on the new channel (stream ids preserved).
        for (PubSlot& s : pubSlots_) {
            if (s.channel == cfg_.ingressChannel) s.res.reset();
        }
        streamPub_ = detail::FlatIndexMap{};
        cfg_.ingressChannel = newIngressChannel;
        ingressPub_ = *res.readyShared();
        return true;
```

- [ ] **Step 4: Run to verify pass, then the real-driver suites**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='InvokerTransport.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: 7 `InvokerTransport` tests PASS; full suite matches the baseline (redirect/retarget, per-worker routing, metadata routing and read-rework egress tests exercise every rewritten path in thread mode).

- [ ] **Step 5: Commit**

```bash
git add include/rune/client/transport/AeronTransport.h src/transport/AeronTransport.cpp \
        tests/transport/InvokerTransportTest.cpp
git commit -m "refactor(transport): resolve every pub/sub through Resolution, never sleep

The five add-then-sleep(1ms)-until-found loops are gone.  Publications are
stable PubHandles resolved asynchronously (offerOn is the hot path: index +
variant check + tryClaim); egress subscriptions resolve inside poll();
start()/retarget() pump the transport's own driver+conductor.  String-keyed
publication caches are replaced by a slot vector + FlatIndexMap.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Plain-call produce sink + flat partition table

Spec §4.3: "`std::function` SendFn rebuilt each `doWork`, copied into every `flushBatches` call → plain member call". Today `ClientAgent::doWork` builds a lambda into a `std::function` every cycle (`ClientAgent.cpp:209-219`), `ProducerHub::drainAndFlush` takes it by value and `ProducerImpl::flushBatches` copies it again (`ProducerImpl.cpp:819-822`); every batch then calls through it. `ProducerImpl::partitions_` is a `std::unordered_map` probed per record (`ProducerImpl.cpp:373-386`, `.h:482`) — fixed on sight (§1a.5). The accumulator micro-bench (`bench/ProducerAccumBench.cpp:70-98`) no longer compiles (3-argument sink vs the 5-argument `SendFn`); it is repaired first so the change can be A/B-measured.

**Files:**
- Modify: `bench/ProducerAccumBench.cpp:66-98`
- Create: `include/rune/client/agent/AgentProduceSink.h`
- Create: `include/rune/client/producer/CountingSink.h`
- Modify: `include/rune/client/producer/ProducerImpl.h:133-199` (`PartitionState::slotIndex`), `:291-308` (templated `flushBatches` / `retransmitStalled`), `:371-392` (`firstPendingPartition`), `:437-451` (templated encoders), `:481-482` (`partitions_`)
- Modify: `src/producer/ProducerImpl.cpp:373-386` (`getOrCreatePartition`), `:427` (flush token loop), `:599-604`, `:716-720`, `:819-823`, `:842` (loop), `:937-943` (`onProduceResponse` lookup), `:1020-1022`; explicit instantiations appended
- Modify: `include/rune/client/producer/ProducerHub.h:74-76` (template body), `src/producer/ProducerHub.cpp:30-53` (definition removed)
- Create: `tests/producer/ProducerSinkTest.cpp`
- Modify: `CMakeLists.txt` (`rune_invoker_tests` sources)

**Interfaces:**
- Consumes: `detail::FlatIndexMap` (Task 4).
- Produces:
  - `using ProducerImpl::SendFn = std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)>;`
  - `template <class Sink> int ProducerImpl::flushBatches(int64_t nowNs, int64_t sessionId, const Sink& sink);`
  - `template <class Sink> int ProducerImpl::retransmitStalled(int64_t nowNs, const Sink& sink);`
  - Explicit instantiations for `Sink ∈ {ProducerImpl::SendFn, agent::AgentProduceSink, producer::CountingSink}` — any other sink type is a link error by design.
  - `template <class Sink> int ProducerHub::drainAndFlush(int64_t nowNs, int64_t sessionId, const Sink& sink);` (header-defined)
  - `struct rune::client::agent::AgentProduceSink { ClientAgent* agent; bool operator()(int32_t topicId, int32_t partition, int32_t ingressStreamId, const uint8_t* buf, std::size_t len) const noexcept; };`
  - `struct rune::client::producer::CountingSink { uint64_t* frames; uint64_t* bytes; bool operator()(...) const noexcept; };`
  - `PartitionState::slotIndex` (dense index of the partition inside its producer; stable for the producer's lifetime).

- [ ] **Step 1: Repair the accumulator bench and capture the "before" binary**

In `bench/ProducerAccumBench.cpp` replace the atomic counters and the 3-argument sink (`:69-74`) with a `SendFn`:

```cpp
    uint64_t frames = 0, bytes = 0;   // agent-thread only; read after join()
    const ProducerImpl::SendFn sink =
        [&frames, &bytes](int32_t, int32_t, int32_t, const uint8_t*, std::size_t len) -> bool {
            ++frames;
            bytes += len;
            return true;  // always accept (counting sink)
        };
```

(`ProducerImpl::SendFn` does not exist yet — for this step spell the type out as `std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)>` and add `#include <functional>`; Step 5 switches it to the alias.)

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic-rel --target rune_producer_accum_bench -j8
cp cmake-build-sic-rel/rune_producer_accum_bench /tmp/accum_A
for i in 1 2 3; do ACCUM_RECORDS=5000000 ACCUM_PARTITIONS=64 RUNE_LINGER_US=0 RUNE_BATCH_BYTES=16384 /tmp/accum_A | grep RESULT; done
git add bench/ProducerAccumBench.cpp
git commit -m "fix(bench): ProducerAccumBench sink matches the 5-argument SendFn

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```
Expected: three `RESULT records=5000000 ... records_per_sec=...` lines (record them as A).

- [ ] **Step 2: Write the failing test**

Create `tests/producer/ProducerSinkTest.cpp`:

```cpp
// tests/producer/ProducerSinkTest.cpp — concrete (non-std::function) produce sinks
// and the flat partition table.  No Aeron: ProducerImpl is driven directly.
#include <rune/client/Key.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <utility>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;

namespace {
constexpr int64_t kNow = 1'000'000'000;

ProducerConfig immediateCfg() {
    ProducerConfig c;
    c.acks = Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = 8;
    return c;
}

template <class Sink>
int pump(ProducerImpl& impl, const Sink& sink) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        flushed += impl.flushBatches(kNow, /*sessionId=*/42, sink);
        if (d == 0) break;
    }
    return flushed;
}
} // namespace

TEST(ProducerSink, ConcreteSinkSeesEveryFrame) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(immediateCfg(), corr);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    const std::vector<uint8_t> v(100, 0x5A);
    for (int32_t p = 0; p < 4; ++p) {
        ASSERT_TRUE(impl.submitSendRecord(1, p, Key(static_cast<uint64_t>(p)), v, false, 0));
    }
    pump(impl, sink);
    EXPECT_EQ(frames, 4u);
    EXPECT_GT(bytes, 4u * 100u);
}

TEST(ProducerSink, ThousandPartitionsKeepTheirOwnStreams) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(immediateCfg(), corr);
    for (int32_t p = 0; p < 1000; ++p) impl.updateIngressRoute(7, p, 5000 + p);
    std::map<int32_t, int32_t> streamByPartition;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t p, int32_t stream, const uint8_t*,
                                          std::size_t) {
        streamByPartition[p] = stream;
        return true;
    };
    const std::vector<uint8_t> v(8, 1);
    for (int32_t p = 0; p < 1000; ++p) {
        ASSERT_TRUE(impl.submitSendRecord(7, p, Key(static_cast<uint64_t>(p)), v, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(streamByPartition.size(), 1000u);
    for (const auto& [p, stream] : streamByPartition) ASSERT_EQ(stream, 5000 + p);
}
```

Add `tests/producer/ProducerSinkTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 3: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/producer/CountingSink.h' file not found`.

- [ ] **Step 4: Implement**

Create `include/rune/client/producer/CountingSink.h`:

```cpp
// include/rune/client/producer/CountingSink.h — concrete sink for in-process
// benches and tests: counts frames/bytes, always accepts.  Single-threaded.
#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::client::producer {
struct CountingSink {
    uint64_t* frames = nullptr;
    uint64_t* bytes  = nullptr;
    bool operator()(int32_t, int32_t, int32_t, const uint8_t*, std::size_t len) const noexcept {
        ++*frames;
        *bytes += len;
        return true;
    }
};
} // namespace rune::client::producer
```

Create `include/rune/client/agent/AgentProduceSink.h`:

```cpp
// include/rune/client/agent/AgentProduceSink.h — the production produce sink: a
// plain (inlinable) member call into the agent's partition router.  Replaces the
// std::function SendFn rebuilt every doWork (spec §4.3).
#pragma once
#include <rune/client/agent/ClientAgent.h>

namespace rune::client::agent {
struct AgentProduceSink {
    ClientAgent* agent = nullptr;
    bool operator()(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                    const uint8_t* buf, std::size_t len) const noexcept {
        return agent->offerToPartition(topicId, partition, ingressStreamId, buf, len);
    }
};
} // namespace rune::client::agent
```

`ProducerImpl.h`:
- add `#include <rune/client/detail/FlatIndexMap.h>`;
- in `PartitionState` after `partition`: `uint32_t slotIndex = 0;  ///< dense index inside the owning producer (stable)`;
- in `class ProducerImpl` public section, first line: `using SendFn = std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)>;`
- replace the `flushBatches` and `retransmitStalled` declarations with:

```cpp
    /// Sink concept: `bool operator()(int32_t topicId, int32_t partition,
    /// int32_t ingressStreamId, const uint8_t* buf, std::size_t len) const`.
    /// Explicitly instantiated in ProducerImpl.cpp for SendFn (tests, cold
    /// paths), agent::AgentProduceSink (production) and CountingSink (benches);
    /// no std::function is built or called on the production flush path.
    template <class Sink>
    int flushBatches(int64_t nowNs, int64_t sessionId, const Sink& sink);

    template <class Sink>
    int retransmitStalled(int64_t nowNs, const Sink& sink);
```

- replace the `encodeAndSend` / `encodeAndSendTier2` declarations' last parameter with `const Sink& sink` and prefix each with `template <class Sink>`;
- replace `std::unordered_map<int64_t, PartitionState> partitions_;` with:

```cpp
    // Flat partition table (spec §1a.5): FlatIndexMap key -> dense slot, slots own
    // their PartitionState (stable address; slotIndex == position).
    detail::FlatIndexMap                          partitionIndex_;
    std::vector<std::unique_ptr<PartitionState>>  partitions_;
```

- `firstPendingPartition` body loop becomes `for (const auto& up : partitions_) { const PartitionState& ps = *up; ... }`.

`ProducerImpl.cpp`:
- add `#include <rune/client/agent/AgentProduceSink.h>` and `#include <rune/client/producer/CountingSink.h>`;
- constructor: replace `partitions_.reserve(16);` with `partitions_.reserve(16);` (vector — unchanged text, now a vector);
- `getOrCreatePartition`:

```cpp
PartitionState& ProducerImpl::getOrCreatePartition(int32_t topicId, int32_t partition) {
    const int64_t k = partKey(topicId, partition);
    const uint32_t idx = partitionIndex_.find(k);
    if (idx != detail::FlatIndexMap::kNone) return *partitions_[idx];
    auto ps = std::make_unique<PartitionState>(topicId, partition);
    ps->slotIndex = static_cast<uint32_t>(partitions_.size());
    ps->ackRing.assign(kAckRingCapacity, 0);  // preallocated ONCE (zero per-batch alloc)
    ps->ackMask = kAckRingCapacity - 1;
    partitions_.push_back(std::move(ps));
    partitionIndex_.insert(k, partitions_.back()->slotIndex);
    return *partitions_.back();
}
```

- every `for (auto& [k, ps] : partitions_)` (flush token at `:427`, `flushBatches` at `:842`) becomes `for (auto& up : partitions_) { PartitionState& ps = *up; ... }`;
- `onProduceResponse` lookup (`:937-943`): `const uint32_t pidx = partitionIndex_.find(partKey(bit->second.topicId, bit->second.partition)); if (pidx != detail::FlatIndexMap::kNone) { ps = partitions_[pidx].get(); ... }`;
- `encodeAndSend`, `encodeAndSendTier2`, `flushBatches`, `retransmitStalled` definitions get `template <class Sink>` and `const Sink& sink` (bodies call `sink(...)` exactly where they called `sendFn(...)`);
- append at the end of the file (inside the namespace):

```cpp
// ---------------------------------------------------------------------------
// Explicit instantiations — the only sink types the producer is built for.
// ---------------------------------------------------------------------------
template int ProducerImpl::flushBatches<ProducerImpl::SendFn>(int64_t, int64_t,
                                                             const ProducerImpl::SendFn&);
template int ProducerImpl::flushBatches<agent::AgentProduceSink>(
    int64_t, int64_t, const agent::AgentProduceSink&);
template int ProducerImpl::flushBatches<CountingSink>(int64_t, int64_t, const CountingSink&);
template int ProducerImpl::retransmitStalled<ProducerImpl::SendFn>(int64_t,
                                                                  const ProducerImpl::SendFn&);
template int ProducerImpl::retransmitStalled<agent::AgentProduceSink>(
    int64_t, const agent::AgentProduceSink&);
template int ProducerImpl::retransmitStalled<CountingSink>(int64_t, const CountingSink&);
```

`ProducerHub.h` — replace the `drainAndFlush` declaration with the definition (and delete it from `ProducerHub.cpp:30-53`):

```cpp
    /// Drain all producer rings and flush ready batches through `sink` (a plain
    /// call; see ProducerImpl::flushBatches for the Sink concept).
    template <class Sink>
    int drainAndFlush(int64_t nowNs, int64_t sessionId, const Sink& sink) {
        int work = 0;
        for (auto& impl : producers_) {
            // Drain ONE message then flush, so flush() boundaries between sends are
            // honoured; bounded at 256 records per producer per cycle.
            static constexpr int kMaxInnerIters = 256;
            for (int iter = 0; iter < kMaxInnerIters; ++iter) {
                const int drained = impl->drainRing(nowNs);
                const int flushed = impl->flushBatches(nowNs, sessionId, sink);
                work += drained + flushed;
                if (drained == 0) break;
            }
            work += impl->retransmitStalled(nowNs, sink);
        }
        return work;
    }
```

- [ ] **Step 5: Switch the bench to the concrete sink with an A/B switch**

In `bench/ProducerAccumBench.cpp` add `#include <rune/client/producer/CountingSink.h>` and replace the sink + the two `flushBatches` calls with:

```cpp
    const bool concrete = std::getenv("ACCUM_SINK") == nullptr ||
                          std::string(std::getenv("ACCUM_SINK")) != "function";
    uint64_t frames = 0, bytes = 0;   // agent-thread only; read after join()
    const rune::client::producer::CountingSink csink{&frames, &bytes};
    const ProducerImpl::SendFn fsink = csink;  // ACCUM_SINK=function: same work via std::function
    auto flush = [&](int64_t nowNs) {
        return concrete ? impl.flushBatches(nowNs, /*sessionId=*/1, csink)
                        : impl.flushBatches(nowNs, /*sessionId=*/1, fsink);
    };
```

and use `flush(nowNs)` / `flush(wallNs())` at `:93` / `:98`.

- [ ] **Step 6: Run the tests**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='ProducerSink.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: 2 `ProducerSink` tests PASS; full suite matches the baseline (every `drainAndFlush(... std::move(sendFn))` harness still compiles: `Sink` deduces to `std::function`).

- [ ] **Step 7: A/B benchmark (interleaved, same box, validated)**

```bash
cmake --build cmake-build-sic-rel --target rune_producer_accum_bench -j8
cp cmake-build-sic-rel/rune_producer_accum_bench /tmp/accum_B
for i in $(seq 1 5); do
  for bin in /tmp/accum_A /tmp/accum_B; do
    echo -n "$bin "; ACCUM_RECORDS=5000000 ACCUM_PARTITIONS=64 RUNE_LINGER_US=0 RUNE_BATCH_BYTES=16384 $bin | grep RESULT
  done
done | tee /tmp/accum_ab.txt
median() { grep "^$1 " /tmp/accum_ab.txt | sed -E 's/.*records_per_sec=([0-9]+).*/\1/' | sort -n | sed -n 3p; }
A_MEDIAN=$(median /tmp/accum_A); B_MEDIAN=$(median /tmp/accum_B); echo "A=$A_MEDIAN B=$B_MEDIAN"
```
Expected: 10 lines each with `RESULT records=5000000`; `B_MEDIAN` ≥ `A_MEDIAN` (flat partition probe + direct sink call). If B is slower, stop and root-cause before committing (§1a.6, §1a.10). (Authoritative end-to-end A/B happens on the VM in Task 19.)

- [ ] **Step 8: Commit**

```bash
git add include/rune/client/agent/AgentProduceSink.h include/rune/client/producer/CountingSink.h \
        include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp \
        include/rune/client/producer/ProducerHub.h src/producer/ProducerHub.cpp \
        bench/ProducerAccumBench.cpp tests/producer/ProducerSinkTest.cpp CMakeLists.txt
git commit -m "perf(producer): plain-call produce sink + flat partition table

flushBatches/retransmitStalled/drainAndFlush are templated on a concrete sink
(AgentProduceSink in production, SendFn only for tests and cold paths) —
no std::function built or called per duty cycle/batch.  partitions_ is a
FlatIndexMap + dense slot vector instead of a per-record unordered_map probe.
Accum bench (64 partitions, 5M x 256B, interleaved x5) medians: A=${A_MEDIAN} B=${B_MEDIAN} rec/s.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Fixed per-partition in-flight batch ring with directly addressed correlation ids

Spec §4.3: "heap copy of each batch frame (`BatchInfo.wireFrame`) + `std::map` batch table → fixed-size per-partition ring of in-flight batch slots, reused, no allocation"; §4.4: "matched to its in-flight batch slot by correlation id via direct index (O(1))". Today every acked batch allocates a `std::map` node plus a `std::vector` frame copy (`ProducerImpl.cpp:681-693`, `.h:503-524`) and every ProduceResponse is broadcast to every producer (`ProducerHub.cpp:55-59`). Out-of-order acks also drift `ackTail` (`ProducerImpl.cpp:942` assumes FIFO) — fixed on sight.

**Files:**
- Create: `include/rune/client/producer/InFlightRing.h`
- Create: `tests/producer/InFlightRingTest.cpp`, `tests/support/AllocCounter.h`, `tests/support/AllocCounter.cpp`, `tests/producer/ProducerAllocRegressionTest.cpp`, `tests/producer/ProducerAckPathTest.cpp`
- Modify: `include/rune/client/producer/ProducerImpl.h` — `PartitionState` (`:133-199`: delete `inFlight`, `pendingCorrId`; add `inflight`), `hasInFlight`/`partitionForCorrId`/`firstPendingPartition` (`:348-392`), private members (`:502-524`: delete `BatchInfo`, `batchMap_`; add `ordinal_`, `earliestAttemptDeadlineNs_`, helpers)
- Modify: `src/producer/ProducerImpl.cpp` — `drainRing` gating (`:522`, `:551-556`), `encodeAndSend` (`:668-707`), `encodeAndSendTier2` (`:780-812`), `flushBatches` (`:864-904`), `onProduceResponse` (`:923-997`), `retransmitStalled` (`:1020-1067`), `retryRedirectedProduce` (`:1069-1094`), `resendAllRedirected` (`:1112-1147`), `resendAllViaRoute` (`:1149-1170`), `failPending` (`:1172-1186`)
- Modify: `include/rune/client/producer/ProducerHub.h:67`, `src/producer/ProducerHub.cpp:21-24, 55-59`
- Modify: `CMakeLists.txt` (`rune_pure_tests` + `rune_invoker_tests` sources)

**Interfaces:**
- Consumes: `PartitionState::slotIndex`, templated sinks (Task 8).
- Produces:
  - `namespace rune::client::producer::batchcorr { int64_t make(uint32_t ordinal, uint32_t slot, uint32_t seq); bool isBatch(int64_t); uint32_t ordinal(int64_t); uint32_t slot(int64_t); uint32_t seq(int64_t); kMaxOrdinal = 4095; kMaxSlot = 262143; }`
  - `struct InFlightBatch { int64_t corrId; int64_t firstSendNs; int64_t attemptDeadlineNs; uint64_t ackStart; int32_t ackCount; int32_t recordCount; int32_t baseSequence; int32_t ingressStreamId; int32_t attempts; int32_t redirectCount; std::vector<uint8_t> wireFrame; };`
  - `class InFlightRing { explicit InFlightRing(uint32_t minCapacity = 1); uint32_t capacity() const; uint32_t live() const; bool empty() const; bool full() const; uint32_t nextSeq() const; InFlightBatch& claim(uint32_t ordinal, uint32_t partitionSlot); InFlightBatch* find(int64_t corrId); void release(InFlightBatch&); InFlightBatch* oldestLive(); template<class F> void forEachLive(F&&); };`
  - `PartitionState::inflight` (replaces `inFlight`), `void ProducerImpl::setOrdinal(uint32_t) noexcept`, `uint32_t ProducerImpl::ordinal() const noexcept`, private `bool canFlight_(const PartitionState&) const noexcept`, `void reclaimAckRing_(PartitionState&) noexcept`, `void resolveSlice_(PartitionState&, uint64_t ackStart, int32_t ackCount, int64_t baseOffset)`, `void failBatch_(PartitionState&, InFlightBatch&, const std::string& reason)`.
  - `ProducerHub::add` assigns ordinals (throws `std::length_error` past 4095); `ProducerHub::onProduceResponse` routes by ordinal in O(1).
  - Test support: `namespace alloc_counter { void start() noexcept; long stop() noexcept; }`.

- [ ] **Step 1: Write the failing pure ring tests**

Create `tests/producer/InFlightRingTest.cpp`:

```cpp
// tests/producer/InFlightRingTest.cpp — batch correlation ids + in-flight ring (pure).
#include <rune/client/producer/InFlightRing.h>

#include <gtest/gtest.h>

using namespace rune::client::producer;

TEST(BatchCorrId, RoundTripsAndIsDistinctFromClientCounterIds) {
    const int64_t c = batchcorr::make(4095, 262143, 0xFFFFFFFFu);
    EXPECT_TRUE(batchcorr::isBatch(c));
    EXPECT_EQ(batchcorr::ordinal(c), 4095u);
    EXPECT_EQ(batchcorr::slot(c), 262143u);
    EXPECT_EQ(batchcorr::seq(c), 0xFFFFFFFFu);
    EXPECT_GT(c, 0);
    EXPECT_FALSE(batchcorr::isBatch(1));                   // Client::nextCorrId_ range
    EXPECT_FALSE(batchcorr::isBatch(3'000'000'000'000LL)); // SessionMachine corr bases
    EXPECT_FALSE(batchcorr::isBatch(0));
    EXPECT_FALSE(batchcorr::isBatch(-5));
}

TEST(InFlightRing, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(InFlightRing(1).capacity(), 1u);
    EXPECT_EQ(InFlightRing(3).capacity(), 4u);
    EXPECT_EQ(InFlightRing(32).capacity(), 32u);
}

TEST(InFlightRing, ClaimFindReleaseFifo) {
    InFlightRing r(4);
    InFlightBatch& a = r.claim(1, 7);
    InFlightBatch& b = r.claim(1, 7);
    EXPECT_EQ(batchcorr::seq(a.corrId), 0u);
    EXPECT_EQ(batchcorr::seq(b.corrId), 1u);
    EXPECT_EQ(r.live(), 2u);
    EXPECT_EQ(r.find(a.corrId), &a);
    r.release(a);
    EXPECT_EQ(r.find(a.corrId), nullptr);
    EXPECT_EQ(r.oldestLive(), &b);
    r.release(b);
    EXPECT_TRUE(r.empty());
    EXPECT_EQ(r.oldestLive(), nullptr);
}

TEST(InFlightRing, OutOfOrderReleaseKeepsWindowFullUntilOldestAcked) {
    InFlightRing r(2);
    InFlightBatch& a = r.claim(0, 0);
    InFlightBatch& b = r.claim(0, 0);
    r.release(b);
    EXPECT_EQ(r.live(), 1u);
    EXPECT_TRUE(r.full()) << "a claim now would overwrite live batch a";
    r.release(a);
    EXPECT_FALSE(r.full());
    EXPECT_TRUE(r.empty());
}

// Regression (Review Focus 4): a duplicate ack, or an ack for an id whose slot was
// reused, must never match a live batch or double-release.
TEST(InFlightRing, DuplicateAndStaleAcksAreIgnoredRegression) {
    InFlightRing r(2);
    const int64_t first = r.claim(0, 3).corrId;
    InFlightBatch* f = r.find(first);
    ASSERT_NE(f, nullptr);
    r.release(*f);
    r.release(*f);                        // duplicate release: no effect
    EXPECT_EQ(r.live(), 0u);
    r.claim(0, 3);                        // seq 1
    const int64_t reused = r.claim(0, 3).corrId;  // seq 2 -> same slot as seq 0
    EXPECT_EQ(batchcorr::seq(reused) & 1u, batchcorr::seq(first) & 1u);
    EXPECT_EQ(r.find(first), nullptr) << "stale id must not match the reused slot";
    EXPECT_NE(r.find(reused), nullptr);
    EXPECT_EQ(r.live(), 2u);
}
```

Add `tests/producer/InFlightRingTest.cpp` to `rune_pure_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_pure_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/producer/InFlightRing.h' file not found`.

- [ ] **Step 3: Implement `InFlightRing.h`**

```cpp
// include/rune/client/producer/InFlightRing.h
//
// Batch correlation ids + the fixed per-partition ring of in-flight batch slots
// (spec §4.3, §4.4).  Slots and their frame buffers are reused: after warm-up no
// batch allocates.  A ProduceResponse is matched to its slot by direct index.
#pragma once

#include <cstdint>
#include <vector>

namespace rune::client::producer {

/// Layout of a positive int64 batch correlation id:
///   bit 62       tag — a producer batch id (the Client counter starts at 1 and
///                session correlation bases are < 2^50; neither reaches 2^62)
///   bits 50..61  producer ordinal inside its ProducerHub (<= 4095)
///   bits 32..49  partition slot inside the producer      (<= 262143)
///   bits  0..31  per-partition batch sequence; slot = seq & mask, and an exact
///                corrId comparison rejects stale ids after the slot is reused
namespace batchcorr {
inline constexpr int64_t  kTag          = int64_t{1} << 62;
inline constexpr int      kOrdinalShift = 50;
inline constexpr int      kSlotShift    = 32;
inline constexpr uint32_t kMaxOrdinal   = (1u << 12) - 1;
inline constexpr uint32_t kMaxSlot      = (1u << 18) - 1;

[[nodiscard]] constexpr int64_t make(uint32_t ordinal, uint32_t slot, uint32_t seq) noexcept {
    return kTag | (static_cast<int64_t>(ordinal & kMaxOrdinal) << kOrdinalShift) |
           (static_cast<int64_t>(slot & kMaxSlot) << kSlotShift) | static_cast<int64_t>(seq);
}
[[nodiscard]] constexpr bool isBatch(int64_t c) noexcept { return c > 0 && (c & kTag) != 0; }
[[nodiscard]] constexpr uint32_t ordinal(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kOrdinalShift) & kMaxOrdinal;
}
[[nodiscard]] constexpr uint32_t slot(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kSlotShift) & kMaxSlot;
}
[[nodiscard]] constexpr uint32_t seq(int64_t c) noexcept { return static_cast<uint32_t>(c); }
} // namespace batchcorr

struct InFlightBatch {
    int64_t  corrId            = 0;  ///< 0 = free slot
    int64_t  firstSendNs       = 0;
    int64_t  attemptDeadlineNs = 0;
    uint64_t ackStart          = 0;  ///< this batch's slice of the partition ack ring
    int32_t  ackCount          = 0;
    int32_t  recordCount       = 0;
    int32_t  baseSequence      = 0;  ///< idempotent sequence of the batch's first record
    int32_t  ingressStreamId   = 0;
    int32_t  attempts          = 0;
    int32_t  redirectCount     = 0;
    std::vector<uint8_t> wireFrame;  ///< encoded Produce frame; capacity reused
};

class InFlightRing {
public:
    explicit InFlightRing(uint32_t minCapacity = 1) {
        uint32_t cap = 1;
        while (cap < minCapacity) cap <<= 1;
        slots_.resize(cap);
        mask_ = cap - 1;
    }

    [[nodiscard]] uint32_t capacity() const noexcept { return mask_ + 1; }
    [[nodiscard]] uint32_t live() const noexcept { return live_; }
    [[nodiscard]] bool     empty() const noexcept { return live_ == 0; }
    /// True when a claim would overwrite the oldest live slot — holes left by
    /// out-of-order acks still occupy the window until the oldest batch acks.
    [[nodiscard]] bool full() const noexcept {
        return head_ - tail_ >= static_cast<uint64_t>(mask_) + 1;
    }
    /// Sequence the next claim() will use (encode the frame before offering it).
    [[nodiscard]] uint32_t nextSeq() const noexcept { return static_cast<uint32_t>(head_); }

    /// Claims the next slot (caller checked !full()) and stamps its corrId.
    InFlightBatch& claim(uint32_t ordinal, uint32_t partitionSlot) noexcept {
        InFlightBatch& b = slots_[head_ & mask_];
        b.corrId = batchcorr::make(ordinal, partitionSlot, static_cast<uint32_t>(head_));
        ++head_;
        ++live_;
        return b;
    }

    /// The live batch with exactly this corrId, else nullptr (duplicate / stale).
    [[nodiscard]] InFlightBatch* find(int64_t corrId) noexcept {
        InFlightBatch& b = slots_[batchcorr::seq(corrId) & mask_];
        return (corrId != 0 && b.corrId == corrId) ? &b : nullptr;
    }

    /// Frees a live batch (idempotent) and advances the tail past freed slots.
    void release(InFlightBatch& b) noexcept {
        if (b.corrId == 0) return;
        b.corrId = 0;
        --live_;
        while (tail_ != head_ && slots_[tail_ & mask_].corrId == 0) ++tail_;
    }

    [[nodiscard]] InFlightBatch* oldestLive() noexcept {
        return tail_ == head_ ? nullptr : &slots_[tail_ & mask_];
    }

    /// Visits live batches oldest-first.  f may release() the visited batch.
    template <class F>
    void forEachLive(F&& f) {
        const uint64_t end = head_;
        for (uint64_t s = tail_; s != end; ++s) {
            InFlightBatch& b = slots_[s & mask_];
            if (b.corrId != 0) f(b);
        }
    }

private:
    std::vector<InFlightBatch> slots_;
    uint32_t mask_ = 0;
    uint64_t head_ = 0;
    uint64_t tail_ = 0;
    uint32_t live_ = 0;
};

} // namespace rune::client::producer
```

Run: `cmake --build cmake-build-sic --target rune_pure_tests -j8 && ./cmake-build-sic/rune_pure_tests --gtest_filter='BatchCorrId.*:InFlightRing.*'`
Expected: PASS (5 tests).

- [ ] **Step 4: Measure the "before" with a regression test that compiles against today's API**

Create `tests/producer/ProducerAllocRegressionTest.cpp` (uses only APIs that already exist, so it runs against the unchanged producer):

```cpp
// tests/producer/ProducerAllocRegressionTest.cpp
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::ProducerImpl;
namespace rcc = rune::client::codec;

namespace {
constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

ProducerConfig ackCfg(int32_t maxInFlight) {
    ProducerConfig c;
    c.acks = Acks::Leader;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = maxInFlight;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

int pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        flushed += impl.flushBatches(kNow, 42, sink);
        if (d == 0) break;
    }
    return flushed;
}
} // namespace

// Regression: every acked batch allocated a std::map node + a std::vector frame
// copy (BatchInfo.wireFrame).  Steady state must allocate nothing.
TEST(ProducerAllocRegression, SteadyStateAckedFlushAllocatesNothingRegression) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(4), corr);
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&lastCorr](int32_t, int32_t, int32_t, const uint8_t* b,
                                                  std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        lastCorr = v ? v->correlationId : 0;
        return true;
    };
    auto cycle = [&](uint64_t k) {
        (void)impl.submitSendRecord(1, 0, Key(k), kVal, false, 0);
        pump(impl, sink);
        impl.onProduceResponse(lastCorr, static_cast<int64_t>(k));
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);   // warm-up: builder, ring, frame capacity
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "acked produce steady state must not allocate";
    EXPECT_FALSE(impl.hasInFlight());
}
```

Create `tests/support/AllocCounter.h` and `tests/support/AllocCounter.cpp` exactly as listed in Step 5 (they are needed first here), add both `.cpp` files to `rune_invoker_tests`, then:

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 && ./cmake-build-sic/rune_invoker_tests --gtest_filter='*SteadyStateAcked*'`
Expected: FAIL — `allocs Which is: 2000` (or more; one `std::map` node + one frame `std::vector` per acked batch, `ProducerImpl.cpp:681-693`). Record the number: it is this task's "before".

- [ ] **Step 5: Write the failing producer-level tests**

Create `tests/support/AllocCounter.h`:

```cpp
// tests/support/AllocCounter.h — counts global operator new calls in a window.
#pragma once
namespace alloc_counter {
void start() noexcept;
long stop() noexcept;
} // namespace alloc_counter
```

Create `tests/support/AllocCounter.cpp`:

```cpp
// tests/support/AllocCounter.cpp — replacement global new/delete for the
// rune_invoker_tests binary.  Counting is off except between start()/stop().
#include "AllocCounter.h"

#include <atomic>
#include <cstdlib>
#include <new>

namespace {
std::atomic<bool> g_on{false};
std::atomic<long> g_count{0};
} // namespace

void* operator new(std::size_t n) {
    if (g_on.load(std::memory_order_relaxed)) g_count.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n != 0 ? n : 1)) return p;
    throw std::bad_alloc{};
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace alloc_counter {
void start() noexcept {
    g_count.store(0, std::memory_order_relaxed);
    g_on.store(true, std::memory_order_seq_cst);
}
long stop() noexcept {
    g_on.store(false, std::memory_order_seq_cst);
    return g_count.load(std::memory_order_relaxed);
}
} // namespace alloc_counter
```

Create `tests/producer/ProducerAckPathTest.cpp`:

```cpp
// tests/producer/ProducerAckPathTest.cpp — in-flight ring + ack path on a bare
// ProducerImpl (no Aeron, injected clock, bounded pumps).
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using namespace rune::client::producer;
namespace rcc = rune::client::codec;

namespace {

constexpr int64_t kNow = 1'000'000'000;

struct Frame {
    int32_t topicId;
    int32_t partition;
    int64_t corrId;
    int32_t records;
    int32_t baseSequence;
};

struct Capture {
    std::vector<Frame> frames;
    bool accept = true;
    ProducerImpl::SendFn fn() {
        return [this](int32_t t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            if (!accept) return false;
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({t, p, v->correlationId, r.recordCount(), r.baseSequence()});
            return true;
        };
    }
};

ProducerConfig ackCfg(int32_t maxInFlight, int64_t lingerUs = 0) {
    ProducerConfig c;
    c.acks = Acks::Leader;
    c.lingerUs = lingerUs;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = maxInFlight;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

int pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int64_t now = kNow) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(now);
        flushed += impl.flushBatches(now, 42, sink);
        if (d == 0) break;
    }
    return flushed;
}

const std::vector<uint8_t> kVal(16, 0x11);

} // namespace

TEST(ProducerInFlight, BatchCorrIdsAreTaggedAndAddressDirectly) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8), corr);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(impl.submitSendRecord(3, 0, Key(uint64_t{1}), kVal, false, 0));
    ASSERT_TRUE(impl.submitSendRecord(3, 1, Key(uint64_t{2}), kVal, false, 0));
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    for (const Frame& f : cap.frames) {
        ASSERT_TRUE(batchcorr::isBatch(f.corrId));
        EXPECT_EQ(batchcorr::ordinal(f.corrId), 0u);
        EXPECT_EQ(batchcorr::seq(f.corrId), 0u);
    }
    EXPECT_NE(batchcorr::slot(cap.frames[0].corrId), batchcorr::slot(cap.frames[1].corrId));
    EXPECT_TRUE(impl.hasInFlight());
    impl.onProduceResponse(cap.frames[0].corrId, 10);
    impl.onProduceResponse(cap.frames[1].corrId, 20);
    EXPECT_FALSE(impl.hasInFlight());
}

TEST(ProducerInFlight, OutOfOrderAckNeverOverwritesALiveSlot) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(2), corr);
    Capture cap;
    const auto sink = cap.fn();
    for (uint64_t k = 0; k < 3; ++k) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(k), kVal, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(cap.frames.size(), 2u) << "third batch held by maxInFlight=2";
    impl.onProduceResponse(cap.frames[1].corrId, 11);   // B acks first
    pump(impl, sink);
    EXPECT_EQ(cap.frames.size(), 2u) << "window stays full until the oldest (A) acks";
    impl.onProduceResponse(cap.frames[0].corrId, 10);   // A acks
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(batchcorr::seq(cap.frames[2].corrId), 2u);
}

TEST(ProducerInFlight, HubRoutesAcksByOrdinal) {
    std::atomic<int64_t> corr{1};
    ProducerHub hub;
    auto a = std::make_shared<ProducerImpl>(ackCfg(4), corr);
    auto b = std::make_shared<ProducerImpl>(ackCfg(4), corr);
    hub.add(a);
    hub.add(b);
    EXPECT_EQ(a->ordinal(), 0u);
    EXPECT_EQ(b->ordinal(), 1u);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(b->submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    hub.drainAndFlush(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(batchcorr::ordinal(cap.frames[0].corrId), 1u);
    hub.onProduceResponse(cap.frames[0].corrId, 5);
    EXPECT_FALSE(b->hasInFlight());
}

```

Add `tests/producer/ProducerAckPathTest.cpp` to `rune_invoker_tests` (`AllocCounter.cpp` was added in Step 4).

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL to compile — `no member named 'ordinal' in 'rune::client::producer::ProducerImpl'`.

- [ ] **Step 6: Integrate the ring into `ProducerImpl`**

`ProducerImpl.h`: `#include <rune/client/producer/InFlightRing.h>`. In `PartitionState` delete `int32_t inFlight = 0;` and `int64_t pendingCorrId = 0;` and add after `nextBaseSequence`:

```cpp
    // Fixed ring of in-flight (sent, un-acked) batches; capacity = maxInFlight
    // rounded up to a power of two (spec §4.3).
    InFlightRing inflight;
```

Public additions (next to `config()`):

```cpp
    /// Ordinal inside the owning ProducerHub; encoded into batch correlation ids.
    void setOrdinal(uint32_t o) noexcept { ordinal_ = o; }
    [[nodiscard]] uint32_t ordinal() const noexcept { return ordinal_; }
```

Replace the inline `hasInFlight`, `partitionForCorrId` and the in-flight fallback of `firstPendingPartition`:

```cpp
    [[nodiscard]] bool hasInFlight() const noexcept {
        for (const auto& up : partitions_) if (!up->inflight.empty()) return true;
        return false;
    }

    [[nodiscard]] bool partitionForCorrId(int64_t corrId, int32_t& topicIdOut,
                                          int32_t& partitionOut) noexcept {
        if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return false;
        const uint32_t s = batchcorr::slot(corrId);
        if (s >= partitions_.size() || partitions_[s]->inflight.find(corrId) == nullptr) return false;
        topicIdOut   = partitions_[s]->topicId;
        partitionOut = partitions_[s]->partition;
        return true;
    }
```

(`firstPendingPartition`'s second loop becomes `for (const auto& up : partitions_) if (!up->inflight.empty()) { topicIdOut = up->topicId; partitionOut = up->partition; return true; }`; `ProducerHub::partitionForCorrId` loses its `const` qualifier accordingly.)

Private: delete `struct BatchInfo` and `batchMap_`; add:

```cpp
    uint32_t ordinal_ = 0;
    // Lower bound of every live batch's attemptDeadlineNs: retransmitStalled()
    // returns in O(1) until it passes (spec §1a.5; replaces the ordered map front).
    int64_t earliestAttemptDeadlineNs_ = INT64_MAX;

    [[nodiscard]] bool canFlight_(const PartitionState& ps) const noexcept {
        return static_cast<int32_t>(ps.inflight.live()) < cfg_.maxInFlight && !ps.inflight.full();
    }
    /// ackTail = start of the oldest un-acked slice (in-flight or current builder).
    void reclaimAckRing_(PartitionState& ps) noexcept;
    /// Resolves the per-record acks of a batch slice (record i -> baseOffset + i;
    /// baseOffset < 0 = error sentinel -errorCode).
    void resolveSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount, int64_t baseOffset);
    /// Fails a live batch's records and releases its slot.
    void failBatch_(PartitionState& ps, InFlightBatch& b, const std::string& reason);
```

`ProducerImpl.cpp`:

`getOrCreatePartition` — after the `ackMask` line add `ps->inflight = InFlightRing(static_cast<uint32_t>(cfg_.maxInFlight > 0 ? cfg_.maxInFlight : 1));` and, before `push_back`, `if (partitions_.size() > batchcorr::kMaxSlot) throw std::length_error("ProducerImpl: more than 262144 partitions");`.

`drainRing` — the normal-path gate (`:522`) becomes `if (!frameCapped && ps.builderBytes >= effCap && !canFlight_(ps)) {`; delete `ps.pendingCorrId = corrId;` (`:555`).

`encodeAndSend` — replace from `ps.nextBaseSequence += ps.builder->recordCount();` (`:674`) through the end of the `if (correlationId != 0) {...}` block (`:694`) with:

```cpp
    const int32_t batchSeq     = ps.nextBaseSequence;
    const int32_t batchRecords = ps.builder->recordCount();
    ps.nextBaseSequence += batchRecords;                 // advance ONLY on success
    if (correlationId != 0) {
        // Only ack-expecting batches occupy the in-flight window (acks=None: corrId 0).
        InFlightBatch& b = ps.inflight.claim(ordinal_, ps.slotIndex);
        assert(b.corrId == correlationId && "frame corrId must equal the claimed slot id");
        b.firstSendNs       = nowNs;
        b.attemptDeadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
        b.attempts          = 1;
        b.redirectCount     = 0;
        b.ackStart          = (ps.batchAckCount > 0) ? ps.batchAckStart : ps.ackHead;
        b.ackCount          = ps.batchAckCount;
        b.recordCount       = batchRecords;
        b.baseSequence      = batchSeq;
        b.ingressStreamId   = ps.ingressStreamId;
        b.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);  // reuses capacity
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) {
            earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
        }
    }
```

(the FR mark above it uses `ps.inflight.live() + 1` instead of `ps.inFlight + 1`; delete `ps.pendingCorrId = 0;` from the reset block.)

`encodeAndSendTier2` — gains an `int64_t nowNs` parameter after `entry` (declaration: `template <class Sink> bool encodeAndSendTier2(PartitionState& ps, Tier2Entry& entry, int64_t nowNs, int64_t sessionId, const Sink& sink);`; the `flushBatches` call site becomes `encodeAndSendTier2(ps, entry, nowNs, sessionId, sink)`); compute the wire id from the ring and put the Tier-2 future's own id into the ack slice. Replace `msg.correlationId = entry.corrId;` with `msg.correlationId = (entry.corrId != 0) ? batchcorr::make(ordinal_, ps.slotIndex, ps.inflight.nextSeq()) : 0;` and the success block (`:800-810`) with:

```cpp
    const int32_t batchSeq = ps.nextBaseSequence;
    ps.nextBaseSequence += entry.recordCount;  // advance ONLY on success
    if (entry.corrId != 0) {
        if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
        const uint64_t slice = ps.ackHead;
        ps.ackRing[ps.ackHead & ps.ackMask] = entry.corrId;  // record 0 -> the batch future
        ++ps.ackHead;
        InFlightBatch& b = ps.inflight.claim(ordinal_, ps.slotIndex);
        // Tier-2 gets the same retransmit/delivery clock as Tier-1 (fixed on sight:
        // the old BatchInfo left firstSendNs = 0, so the first retransmit scan gave
        // it up immediately).
        b.firstSendNs       = nowNs;
        b.attemptDeadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) {
            earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
        }
        b.attempts          = 1;
        b.redirectCount     = 0;
        b.ackStart          = slice;
        b.ackCount          = 1;
        b.recordCount       = entry.recordCount;
        b.baseSequence      = batchSeq;
        b.ingressStreamId   = ps.ingressStreamId;
        b.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
    }
```

`flushBatches` — `canFlight` becomes `canFlight_(ps)`; the corrId selection block (`:874-879`) becomes:

```cpp
                const int64_t corrId = (cfg_.acks != Acks::None)
                    ? batchcorr::make(ordinal_, ps.slotIndex, ps.inflight.nextSeq())
                    : 0;
```

delete `ps.pendingCorrId = 0;` (`:882`); the Tier-2 guard (`:904`) becomes `if (!canFlight_(ps)) break;`.

Replace `onProduceResponse` entirely:

```cpp
void ProducerImpl::onProduceResponse(int64_t correlationId, int64_t baseOffset)
{
    if (!batchcorr::isBatch(correlationId) || batchcorr::ordinal(correlationId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(correlationId);
    if (s >= partitions_.size()) return;
    PartitionState& ps = *partitions_[s];
    InFlightBatch* b = ps.inflight.find(correlationId);
    if (b == nullptr) return;  // duplicate / stale (slot reused) / already expired: ignore
    resolveSlice_(ps, b->ackStart, b->ackCount, baseOffset);
    ps.inflight.release(*b);
    reclaimAckRing_(ps);
}

void ProducerImpl::reclaimAckRing_(PartitionState& ps) noexcept {
    if (const InFlightBatch* o = ps.inflight.oldestLive()) {
        ps.ackTail = o->ackStart;
    } else {
        ps.ackTail = (ps.batchAckCount > 0) ? ps.batchAckStart : ps.ackHead;
    }
}

void ProducerImpl::resolveSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount,
                                 int64_t baseOffset) {
    if (ackCount <= 0) return;
    // DR-BUG-5: a negative baseOffset is the -errorCode sentinel for the whole batch.
    const bool err = (baseOffset < 0);
    std::lock_guard<std::mutex> lock(ackMtx_);
    for (int32_t i = 0; i < ackCount; ++i) {
        const int64_t corrId = ps.ackRing[(ackStart + static_cast<uint64_t>(i)) & ps.ackMask];
        auto ait = pendingAcks_.find(corrId);
        if (ait == pendingAcks_.end()) continue;
        PendingAck& pa = ait->second;
        const int64_t off = baseOffset + i;
        RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientAckComplete,
                                 static_cast<uint32_t>(corrId), static_cast<double>(off));
        if (pa.kind == PendingAck::Kind::Callback) {
            if (pa.callback) pa.callback(err ? -1 : off);
        } else if (err) {
            try {
                pa.promise.set_exception(std::make_exception_ptr(std::runtime_error(
                    "produce rejected by broker (errorCode=" + std::to_string(-baseOffset) + ")")));
            } catch (...) {}
        } else {
            try { pa.promise.set_value(off); } catch (...) {}
        }
        pendingAcks_.erase(ait);
    }
}

void ProducerImpl::failBatch_(PartitionState& ps, InFlightBatch& b, const std::string& reason) {
    {
        std::lock_guard<std::mutex> lock(ackMtx_);
        for (int32_t i = 0; i < b.ackCount; ++i) {
            const int64_t corrId = ps.ackRing[(b.ackStart + static_cast<uint64_t>(i)) & ps.ackMask];
            auto it = pendingAcks_.find(corrId);
            if (it == pendingAcks_.end()) continue;
            if (it->second.kind == PendingAck::Kind::Future) {
                try { it->second.promise.set_exception(std::make_exception_ptr(std::runtime_error(reason))); } catch (...) {}
            } else if (it->second.callback) {
                it->second.callback(-1);
            }
            pendingAcks_.erase(it);
        }
    }
    ps.inflight.release(b);
    reclaimAckRing_(ps);
}
```

Replace `retransmitStalled`:

```cpp
template <class Sink>
int ProducerImpl::retransmitStalled(int64_t nowNs, const Sink& sink)
{
    if (nowNs < earliestAttemptDeadlineNs_) return 0;  // O(1) on the happy path
    const int64_t deliveryNs = cfg_.deliveryTimeoutMs * 1'000'000LL;
    const int64_t attemptNs  = cfg_.produceTimeoutMs * 1'000'000LL;
    int64_t nextEarliest = INT64_MAX;
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (nowNs - b.firstSendNs >= deliveryNs) {
                std::fprintf(stderr,
                    "ProducerImpl: batch corrId=%lld undelivered after %d attempts / %lldms "
                    "(topic=%d part=%d); giving up\n",
                    static_cast<long long>(b.corrId), b.attempts,
                    static_cast<long long>(cfg_.deliveryTimeoutMs), ps.topicId, ps.partition);
                failBatch_(ps, b, "produce delivery timed out (no ProduceResponse)");
                return;
            }
            if (nowNs >= b.attemptDeadlineNs &&
                sink(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                     b.wireFrame.size())) {
                ++b.attempts;
                b.attemptDeadlineNs = nowNs + attemptNs;  // same frame, same corrId: broker dedups
                ++resent;
            }
            if (b.attemptDeadlineNs < nextEarliest) nextEarliest = b.attemptDeadlineNs;
        });
    }
    earliestAttemptDeadlineNs_ = nextEarliest;
    return resent;
}
```

Replace `retryRedirectedProduce`, `resendAllRedirected`, `resendAllViaRoute`, `failPending`:

```cpp
bool ProducerImpl::retryRedirectedProduce(int64_t corrId, SendFn& sendFn)
{
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return false;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return false;
    PartitionState& ps = *partitions_[s];
    InFlightBatch* b = ps.inflight.find(corrId);
    if (b == nullptr) return false;
    if (b->redirectCount >= kMaxRedirects) {
        failBatch_(ps, *b, "Redirect exhausted: max redirect attempts reached");
        return false;
    }
    ++b->redirectCount;
    return sendFn(ps.topicId, ps.partition, b->ingressStreamId, b->wireFrame.data(),
                  b->wireFrame.size());
}

int ProducerImpl::resendAllRedirected(int64_t newSessionId, SendFn& sendFn)
{
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (b.redirectCount >= kMaxRedirects) {
                failBatch_(ps, b, "Redirect exhausted: max redirect attempts reached");
                return;
            }
            restampSessionId(b.wireFrame, newSessionId);
            ++b.redirectCount;
            if (sendFn(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                       b.wireFrame.size())) {
                ++resent;
            }
        });
    }
    return resent;
}

int ProducerImpl::resendAllViaRoute(SendFn& sendFn)
{
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (sendFn(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                       b.wireFrame.size())) {
                ++resent;
            }
        });
    }
    return resent;
}

void ProducerImpl::failPending(int64_t corrId, const std::string& reason) {
    if (batchcorr::isBatch(corrId)) {  // a wire (batch) id: fail every record of that batch
        if (batchcorr::ordinal(corrId) != ordinal_) return;
        const uint32_t s = batchcorr::slot(corrId);
        if (s >= partitions_.size()) return;
        if (InFlightBatch* b = partitions_[s]->inflight.find(corrId)) failBatch_(*partitions_[s], *b, reason);
        return;
    }
    std::lock_guard<std::mutex> lock(ackMtx_);  // an app-registered per-record id
    auto it = pendingAcks_.find(corrId);
    if (it == pendingAcks_.end()) return;
    PendingAck& pa = it->second;
    if (pa.kind == PendingAck::Kind::Future) {
        try { pa.promise.set_exception(std::make_exception_ptr(std::runtime_error(reason))); } catch (...) {}
    } else if (pa.callback) {
        pa.callback(-1);
    }
    pendingAcks_.erase(it);
}
```

(the three redirect helpers' parameter type is `SendFn&` — the same `std::function` type as before, spelled with the new alias; move the `restampSessionId` helper's anonymous namespace above `retryRedirectedProduce`.)

`ProducerHub.cpp`:

```cpp
void ProducerHub::add(std::shared_ptr<ProducerImpl> impl)
{
    if (producers_.size() > batchcorr::kMaxOrdinal) {
        throw std::length_error("ProducerHub: at most 4096 producers per client");
    }
    impl->setOrdinal(static_cast<uint32_t>(producers_.size()));
    producers_.push_back(std::move(impl));
}

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset) {
    if (!batchcorr::isBatch(correlationId)) return;       // txn / non-producer id
    const uint32_t o = batchcorr::ordinal(correlationId);
    if (o < producers_.size()) producers_[o]->onProduceResponse(correlationId, baseOffset);
}
```

(`#include <stdexcept>`; `ProducerHub::add`'s doc comment gains "assigns the producer's ordinal".)

- [ ] **Step 7: Run the new tests, then everything**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_pure_tests --gtest_filter='BatchCorrId.*:InFlightRing.*'
./cmake-build-sic/rune_invoker_tests --gtest_filter='ProducerInFlight.*:ProducerAllocRegression.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: all PASS; `SteadyStateAckedFlushAllocatesNothingRegression` now reports 0 allocations (Step 4 recorded the "before"); the full suite matches the baseline (Producer, BatchBuilder, Redirect, NotLeader, MetadataRouting and race suites exercise ack, redirect, resend and timeout paths).

- [ ] **Step 8: Commit (before/after = Step 4 vs Step 7 allocation counts; the end-to-end latency/throughput A/B of the produce path runs on the VM in Task 19)**

```bash
git add include/rune/client/producer/InFlightRing.h include/rune/client/producer/ProducerImpl.h \
        src/producer/ProducerImpl.cpp include/rune/client/producer/ProducerHub.h src/producer/ProducerHub.cpp \
        tests/producer/InFlightRingTest.cpp tests/producer/ProducerAckPathTest.cpp \
        tests/support/AllocCounter.h tests/support/AllocCounter.cpp tests/producer/ProducerAllocRegressionTest.cpp CMakeLists.txt
git commit -m "perf(producer): fixed in-flight batch ring, directly addressed batch ids

Each partition owns a power-of-two ring of in-flight batch slots whose frame
buffers are reused; batch correlation ids encode (producer ordinal, partition
slot, sequence) so a ProduceResponse finds its slot in O(1) and the hub routes
it to one producer instead of broadcasting.  Removes the per-batch std::map node
+ frame heap copy (0 allocations in steady state, was >= 2 per batch), the
FIFO-only ackTail drift on out-of-order acks, and ignores duplicate/stale acks.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Ack ownership travels through the ring — no ack map, no ack mutex; per-batch acks

Spec §4.3: "per-record `std::map<int64_t,PendingAck>` + `ackMtx_` fan-out on ack → per-partition flat ring indexed by batch; new per-batch callback `(baseOffset, count)`; per-record callback API kept, implemented on top". The app thread allocates a `PendingAck` only on the opt-in callback/future paths and hands the *pointer* to the duty-cycle thread inside the SEND_RECORD it already writes (ownership transfer through the ring, §1a.2); the partition ack ring stores that handle at the record's own index, so completing a batch is a walk of its slice with no lookup and no lock. Fixed on sight: the ack ring stored only ack-wanting records, so in a batch mixing void and callback sends record *i* of the slice was not record *i* of the batch (Review Focus 3); Tier-2 batches crossed threads through a mutex-guarded deque (`ProducerImpl.cpp:256-295`, `:432-445`).

**Files:**
- Modify: `include/rune/client/producer/Producer.h:54-87` (`BatchAck`), `:145-196` (`onBatchAck`)
- Modify: `src/producer/Producer.cpp:67-105` (ack handles), add `onBatchAck`
- Modify: `src/producer/BatchBuilder.cpp:78-104`
- Modify: `include/rune/client/producer/ProducerImpl.h:82-128` (Tier-2 token), `:169-183` (`ackRing` element type), `:201-216` (`PendingAck`), `:241-345` (API), `:526-534` (members)
- Modify: `src/producer/ProducerImpl.cpp` — `submitTier2Batch` (`:256-295`), `registerCallback`/`registerFuture` (`:301-327`), `drainRing` (`:432-445`, `:547-560`), `encodeAndSendTier2`, `onProduceResponse`/`resolveSlice_`/`failBatch_` (Task 9), `sweepTimedOut` (`:999-1018`), `failPending` → `failBatch`/`failUnsubmitted`, `failAllPending` (`:1188-1203`)
- Modify: `include/rune/client/producer/ProducerHub.h:89`, `src/producer/ProducerHub.cpp:67-71`
- Modify: `tests/producer/ProducerTest.cpp:1026-1063` (`CloseWithPendingFuture` — the "registered but never submitted" state no longer exists through any public API)
- Modify: `tests/producer/ProducerAckPathTest.cpp` (append)

**Interfaces:**
- Consumes: `InFlightRing`, `InFlightBatch::baseSequence/recordCount`, `resolveSlice_`/`failBatch_`/`reclaimAckRing_` (Task 9).
- Produces:
  - `struct rune::client::BatchAck { int32_t topicId; int32_t partition; int64_t baseOffset; int32_t baseSequence; int32_t recordCount; int16_t errorCode; };` (`errorCode` 0 = OK, >0 broker `ErrorCode`, `BatchAck::kClientFailure = -1` = delivery timeout / redirect exhausted / closed; `baseOffset = -1` unless OK)
  - `void Producer::onBatchAck(std::function<void(const BatchAck&)> cb);` (setup-phase: before the first send; fires on the duty-cycle thread)
  - `int64_t ProducerImpl::registerCallback(std::function<void(int64_t)>)` and `std::pair<int64_t, std::future<int64_t>> ProducerImpl::registerFuture()` now return an owning ack *handle* (not a correlation id).
  - `void ProducerImpl::failUnsubmitted(int64_t ackHandle, const std::string& reason) noexcept;` (app thread, handle never reached the ring)
  - `void ProducerImpl::failBatch(int64_t batchCorrId, const std::string& reason);` (duty-cycle thread; ignores non-batch ids) — replaces `failPending`.
  - `void ProducerImpl::onBatchAck(std::function<void(const BatchAck&)>);` `void ProducerImpl::collectBatchAcks(bool) noexcept;` `template <class H> int ProducerImpl::drainBatchAcks(H&& h);`
  - `static void PendingAck::complete(uint64_t handle, int64_t offset, std::string_view error) noexcept;` (fires exactly once, deletes)
  - `ProducerHub::failPending(int64_t corrId, reason)` keeps its name and forwards to `failBatch`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/producer/ProducerAckPathTest.cpp`:

```cpp
namespace {
void drainAll(ProducerImpl& impl) {
    for (int i = 0; i < 100'000 && impl.drainRing(kNow) > 0; ++i) {}
}
/// Drains every queued record WITHOUT flushing, then flushes them as one batch.
void flushAsOneBatch(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    drainAll(impl);
    impl.submitFlush();
    drainAll(impl);
    impl.flushBatches(kNow, 42, sink);
}
} // namespace

// Regression (Review Focus 3): the ack ring stored only ack-wanting records, so a
// batch [void, cb, void, cb] resolved the callbacks with base+0 and base+1.
TEST(ProducerAcks, MixedVoidAndCallbackRecordsGetTheirOwnOffsetsRegression) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/1'000'000'000), corr);
    Capture cap;
    const auto sink = cap.fn();
    std::vector<int64_t> got;
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));           // record 0
    const int64_t h1 = impl.registerCallback([&](int64_t off) { got.push_back(off); });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h1));           // record 1
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));           // record 2
    const int64_t h3 = impl.registerCallback([&](int64_t off) { got.push_back(off); });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h3));           // record 3
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    ASSERT_EQ(cap.frames[0].records, 4);
    impl.onProduceResponse(cap.frames[0].corrId, 100);
    EXPECT_EQ(got, (std::vector<int64_t>{101, 103}));
}

// Review Focus 4: a duplicate ack (retransmit dedup / second publisher) fires once.
TEST(ProducerAcks, DuplicateBatchAckFiresCallbacksOnceRegression) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8), corr);
    Capture cap;
    const auto sink = cap.fn();
    int fired = 0;
    const int64_t h = impl.registerCallback([&](int64_t) { ++fired; });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.frames[0].corrId, 7);
    impl.onProduceResponse(cap.frames[0].corrId, 7);
    EXPECT_EQ(fired, 1);
}

TEST(ProducerAcks, BatchAckReportsPartitionSequenceAndCount) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8, 1'000'000'000), corr);
    std::vector<rune::client::BatchAck> acks;
    impl.onBatchAck([&](const rune::client::BatchAck& a) { acks.push_back(a); });
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    for (int i = 0; i < 2; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    impl.onProduceResponse(cap.frames[0].corrId, 500);
    impl.onProduceResponse(cap.frames[1].corrId, 503);
    ASSERT_EQ(acks.size(), 2u);
    EXPECT_EQ(acks[0].topicId, 4);
    EXPECT_EQ(acks[0].partition, 2);
    EXPECT_EQ(acks[0].baseOffset, 500);
    EXPECT_EQ(acks[0].baseSequence, 0);
    EXPECT_EQ(acks[0].recordCount, 3);
    EXPECT_EQ(acks[0].errorCode, 0);
    EXPECT_EQ(acks[1].baseSequence, 3);
    EXPECT_EQ(acks[1].recordCount, 2);
}

TEST(ProducerAcks, BrokerErrorFailsEveryRecordAndReportsTheCode) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8, 1'000'000'000), corr);
    std::vector<rune::client::BatchAck> acks;
    impl.onBatchAck([&](const rune::client::BatchAck& a) { acks.push_back(a); });
    Capture cap;
    const auto sink = cap.fn();
    std::vector<int64_t> got;
    for (int i = 0; i < 2; ++i) {
        const int64_t h = impl.registerCallback([&](int64_t off) { got.push_back(off); });
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    }
    flushAsOneBatch(impl, sink);
    impl.onProduceResponse(cap.frames[0].corrId, /*-MinIsrViolation*/ -10);
    EXPECT_EQ(got, (std::vector<int64_t>{-1, -1}));
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].errorCode, 10);
    EXPECT_EQ(acks[0].baseOffset, -1);
}

TEST(ProducerAcks, CloseFailsRecordsStillInTheRing) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8), corr);
    auto [h, fut] = impl.registerFuture();
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));  // never drained
    impl.failAllPending("Client closed");
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_THROW(fut.get(), std::runtime_error);
}

TEST(ProducerAcks, DeliveryTimeoutFailsNeverSentRecordsAndDropsThem) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8), corr);
    Capture cap;
    const auto sink = cap.fn();
    int64_t got = 0;
    const int64_t h = impl.registerCallback([&](int64_t off) { got = off; });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    drainAll(impl);
    impl.flushBatches(kNow, /*sessionId=*/0, sink);  // no session: nothing is sent
    ASSERT_TRUE(cap.frames.empty());
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{kNow + 10'000'000'000LL}});  // deliveryTimeoutMs = 10 s
    EXPECT_EQ(got, -1);
    impl.flushBatches(kNow + 10'000'000'001LL, 42, sink);
    EXPECT_TRUE(cap.frames.empty()) << "expired records are dropped, never sent after failing";
}
```

Change `ProducerTest.CloseWithPendingFuture` (`tests/producer/ProducerTest.cpp:1026-1063`) body from "`registerFuture()` then `failAll`" to the public path:

```cpp
TEST(Producer, CloseWithPendingFuture) {
    static constexpr int32_t kInStr = 2025, kEgStr = 2026;
    AgentHarness h(kInStr, kEgStr);
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 30'000;
    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(15, 1);
    const std::vector<uint8_t> val = {0x01};
    // Submitted but never drained (no doWork): the record sits in the producer ring.
    auto fut = p.send(topic, Key(uint64_t{0}), std::span<const uint8_t>(val), want_future);
    ASSERT_EQ(fut.wait_for(0s), std::future_status::timeout);
    h.hub.failAll("Client closed");
    ASSERT_EQ(fut.wait_for(0s), std::future_status::ready)
        << "close must fail futures whose records are still in the ring";
    EXPECT_THROW({ fut.get(); }, std::runtime_error);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'onBatchAck' in 'rune::client::producer::ProducerImpl'`. (After adding only the declarations, `MixedVoidAndCallbackRecordsGetTheirOwnOffsetsRegression` fails with `got == {100, 101}` — the real bug.)

- [ ] **Step 3: Implement**

`Producer.h` — after `want_future_t` add:

```cpp
// ---------------------------------------------------------------------------
// BatchAck — one acknowledged (or failed) produce batch (spec §4.3).  Records of
// the batch are baseSequence .. baseSequence+recordCount-1 of this producer's
// per-partition idempotent sequence and landed at baseOffset .. +recordCount-1.
// ---------------------------------------------------------------------------
struct BatchAck {
    static constexpr int16_t kClientFailure = -1;  ///< delivery timeout / redirect exhausted / closed
    int32_t topicId      = 0;
    int32_t partition    = 0;
    int64_t baseOffset   = -1;  ///< -1 unless errorCode == 0
    int32_t baseSequence = 0;
    int32_t recordCount  = 0;
    int16_t errorCode    = 0;   ///< 0 OK; >0 broker ErrorCode; kClientFailure
};
```

and in `class Producer` (public, "Opt-in ack paths"):

```cpp
    /// Per-batch acknowledgement callback (one call per batch, not per record).
    /// Setup-phase: call before the first send(); fires on the duty-cycle thread
    /// (the runner in thread mode, the app thread inside doWork() in invoker mode).
    void onBatchAck(std::function<void(const BatchAck&)> cb);
```

`Producer.cpp`: `void Producer::onBatchAck(std::function<void(const BatchAck&)> cb) { if (impl_) impl_->onBatchAck(std::move(cb)); }`; in both opt-in `send` overloads rename the local `corrId` to `ackHandle` and replace `impl_->failPending(corrId, ...)` with `impl_->failUnsubmitted(ackHandle, ...)`. `BatchBuilder.cpp:104` likewise uses `failUnsubmitted`.

`ProducerImpl.h`:
- Tier-2 token — replace the `kT2*` offset constants (`:82-94`) with:

```cpp
// kMsgTier2Batch token: 8 bytes = a heap Tier2Entry* (int64 LE).  The entry's
// ownership travels through the ring (spec §1a.2) — no side queue, no mutex.
inline constexpr int kT2TokenSize = 8;
```

- `Tier2Entry::corrId` → `uint64_t ackHandle = 0;  // PendingAck* of the batch future (0 = none)`
- `PartitionState::ackRing` → `std::vector<uint64_t> ackRing;  // PendingAck* handles at record index (0 = void record)`
- replace `struct PendingAck` with:

```cpp
// ---------------------------------------------------------------------------
// PendingAck — heap object created on the opt-in ack paths.  Owned by the app
// thread until the SEND_RECORD carrying its address is committed to the ring,
// by the duty-cycle thread afterwards.  complete() fires it exactly once and
// deletes it.
// ---------------------------------------------------------------------------
struct PendingAck {
    enum class Kind : uint8_t { Callback, Future };
    Kind                         kind = Kind::Callback;
    std::function<void(int64_t)> callback;
    std::promise<int64_t>        promise;

    /// error empty = success (callback gets offset, future gets value);
    /// otherwise callback(-1) / future exception(error).
    static void complete(uint64_t handle, int64_t offset, std::string_view error) noexcept;
};
```

- public API: replace `void failPending(int64_t corrId, const std::string& reason);` with

```cpp
    /// App thread: fail an ack handle that never reached the ring (ring full).
    void failUnsubmitted(int64_t ackHandle, const std::string& reason) noexcept;
    /// Duty-cycle thread: fail every record of an in-flight batch (wire id).
    /// Non-batch ids are ignored (they can come from any ErrorResponse).
    void failBatch(int64_t batchCorrId, const std::string& reason);

    /// Per-batch acknowledgements (setup-phase; see Producer::onBatchAck).
    void onBatchAck(std::function<void(const BatchAck&)> cb) { batchAckCb_ = std::move(cb); }
    /// Invoker mode: buffer BatchAcks for drainBatchAcks() instead of (or as well
    /// as) calling the callback.
    void collectBatchAcks(bool on) noexcept {
        collectBatchAcks_ = on;
        if (on && completedAcks_.capacity() < 4096) completedAcks_.reserve(4096);
    }
    template <class H>
    int drainBatchAcks(H&& h) {
        const int n = static_cast<int>(completedAcks_.size());
        for (const BatchAck& a : completedAcks_) h(a);
        completedAcks_.clear();  // keeps capacity
        return n;
    }
```

- private: delete `tier2Mtx_`, `tier2Queue_`, `ackMtx_`, `pendingAcks_`; the Task 9 helper `resolveSlice_` becomes `completeSlice_`; add:

```cpp
    std::function<void(const BatchAck&)> batchAckCb_;   // set before the first send
    bool                                 collectBatchAcks_ = false;
    std::vector<BatchAck>                completedAcks_;

    void pushAck_(PartitionState& ps, uint64_t handle);
    /// Completes the handles of slice [ackStart, ackStart+ackCount): record i ->
    /// baseOffset + i, or error.  Zeroes each slot so nothing completes twice.
    void completeSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount,
                        int64_t baseOffset, std::string_view error) noexcept;
    void emitBatchAck_(const PartitionState& ps, const InFlightBatch& b, int64_t baseOffset,
                       int16_t errorCode);
```

`ProducerImpl.cpp`:

```cpp
void PendingAck::complete(uint64_t handle, int64_t offset, std::string_view error) noexcept {
    std::unique_ptr<PendingAck> pa(reinterpret_cast<PendingAck*>(static_cast<uintptr_t>(handle)));
    if (!pa) return;
    try {
        if (pa->kind == Kind::Callback) {
            if (pa->callback) pa->callback(error.empty() ? offset : -1);
        } else if (error.empty()) {
            pa->promise.set_value(offset);
        } else {
            pa->promise.set_exception(
                std::make_exception_ptr(std::runtime_error(std::string(error))));
        }
    } catch (...) {
        // A throwing user callback must not escape the duty cycle.
    }
}

int64_t ProducerImpl::registerCallback(std::function<void(int64_t)> cb) {
    auto* pa = new PendingAck{};
    pa->kind = PendingAck::Kind::Callback;
    pa->callback = std::move(cb);
    return static_cast<int64_t>(reinterpret_cast<uintptr_t>(pa));
}

std::pair<int64_t, std::future<int64_t>> ProducerImpl::registerFuture() {
    auto* pa = new PendingAck{};
    pa->kind = PendingAck::Kind::Future;
    auto fut = pa->promise.get_future();
    return {static_cast<int64_t>(reinterpret_cast<uintptr_t>(pa)), std::move(fut)};
}

void ProducerImpl::failUnsubmitted(int64_t ackHandle, const std::string& reason) noexcept {
    PendingAck::complete(static_cast<uint64_t>(ackHandle), -1, reason);
}

void ProducerImpl::pushAck_(PartitionState& ps, uint64_t handle) {
    if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
    ps.ackRing[ps.ackHead & ps.ackMask] = handle;
    ++ps.ackHead;
    ++ps.batchAckCount;
}

void ProducerImpl::completeSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount,
                                  int64_t baseOffset, std::string_view error) noexcept {
    for (int32_t i = 0; i < ackCount; ++i) {
        uint64_t& slot = ps.ackRing[(ackStart + static_cast<uint64_t>(i)) & ps.ackMask];
        const uint64_t h = slot;
        slot = 0;
        if (h == 0) continue;
        RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientAckComplete, static_cast<uint32_t>(h),
                                 static_cast<double>(baseOffset + i));
        PendingAck::complete(h, baseOffset + i, error);
    }
}

void ProducerImpl::emitBatchAck_(const PartitionState& ps, const InFlightBatch& b,
                                 int64_t baseOffset, int16_t errorCode) {
    const BatchAck a{ps.topicId, ps.partition, errorCode == 0 ? baseOffset : -1,
                     b.baseSequence, b.recordCount, errorCode};
    if (batchAckCb_) batchAckCb_(a);
    if (collectBatchAcks_) completedAcks_.push_back(a);
}
```

`submitTier2Batch`:

```cpp
bool ProducerImpl::submitTier2Batch(int32_t topicId, int32_t partition, int64_t ackHandle,
                                    int32_t recordCount, std::vector<uint8_t> batchBytes)
{
    auto entry = std::make_unique<Tier2Entry>();
    entry->topicId     = topicId;
    entry->partition   = partition;
    entry->ackHandle   = static_cast<uint64_t>(ackHandle);
    entry->recordCount = recordCount;
    entry->batchBytes  = std::move(batchBytes);
    uint8_t tok[kT2TokenSize];
    writeLE64(tok, 0, static_cast<int64_t>(reinterpret_cast<uintptr_t>(entry.get())));
    if (!writeToRing(kMsgTier2Batch, tok, kT2TokenSize)) return false;  // caller fails the handle
    entry.release();  // ownership now travels through the ring
    return true;
}
```

`drainRing` — move `const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);` to the top of the handler, replace the Tier-2 branch with:

```cpp
            if (msgTypeId == kMsgTier2Batch) {
                if (len < kT2TokenSize) return ControlledAction::CONTINUE;
                std::unique_ptr<Tier2Entry> entry(
                    reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
                PartitionState& ps = getOrCreatePartition(entry->topicId, entry->partition);
                if (ps.builder && ps.builder->recordCount() > 0) ps.flushRequested = true;
                recordsSent_ += static_cast<uint64_t>(entry->recordCount);
                ps.pendingTier2Entries.push_back(std::move(*entry));
                return ControlledAction::CONTINUE;
            }
```

rename `corrId` in the SEND_RECORD decode to `ackHandle` (`const uint64_t ackHandle = static_cast<uint64_t>(readLE64(p, kRecCorrIdOff));`) and replace the ack-ring block (`:551-560`) with:

```cpp
            // Per-record acks at the record's OWN index (spec §4.3): the slice of a
            // batch is aligned with its records, void records hold 0.  Zeros are
            // back-filled lazily so batches without callbacks cost nothing.
            if ((flags & kFlagWantAck) && ackHandle != 0) {
                const int32_t index = ps.builder->recordCount();  // this record's offsetDelta
                if (ps.batchAckCount == 0) ps.batchAckStart = ps.ackHead;
                while (ps.batchAckCount < index) pushAck_(ps, 0);
                pushAck_(ps, ackHandle);
            }
```

`encodeAndSendTier2` — Tier-2 always wants its ack: `msg.correlationId = batchcorr::make(ordinal_, ps.slotIndex, ps.inflight.nextSeq());`; the success block pushes `entry.ackHandle` (replace `ps.ackRing[ps.ackHead & ps.ackMask] = entry.corrId; ++ps.ackHead;` with `const uint64_t slice = ps.ackHead; ps.ackRing[ps.ackHead & ps.ackMask] = entry.ackHandle; ++ps.ackHead;`), and the `if (entry.corrId != 0)` guard is removed (always claim).

`onProduceResponse` (Task 9 body) — replace the `resolveSlice_` call with:

```cpp
    const int16_t ec = baseOffset < 0 ? static_cast<int16_t>(-baseOffset) : int16_t{0};
    emitBatchAck_(ps, *b, baseOffset, ec);
    if (ec == 0) {
        completeSlice_(ps, b->ackStart, b->ackCount, baseOffset, {});
    } else {
        const std::string err = "produce rejected by broker (errorCode=" + std::to_string(ec) + ")";
        completeSlice_(ps, b->ackStart, b->ackCount, -1, err);
    }
```

`failBatch_`:

```cpp
void ProducerImpl::failBatch_(PartitionState& ps, InFlightBatch& b, const std::string& reason) {
    emitBatchAck_(ps, b, -1, BatchAck::kClientFailure);
    completeSlice_(ps, b.ackStart, b.ackCount, -1, reason);
    ps.inflight.release(b);
    reclaimAckRing_(ps);
}

void ProducerImpl::failBatch(int64_t corrId, const std::string& reason) {
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return;
    if (InFlightBatch* b = partitions_[s]->inflight.find(corrId)) failBatch_(*partitions_[s], *b, reason);
}
```

(delete `failPending`, and in `retryRedirectedProduce` nothing else changes.)

`sweepTimedOut`:

```cpp
void ProducerImpl::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    const int64_t nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    const int64_t deliveryNs = cfg_.deliveryTimeoutMs * 1'000'000LL;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (nowNs - b.firstSendNs >= deliveryNs) {
                failBatch_(ps, b, "produce delivery timed out (no ProduceResponse)");
            }
        });
        // Records never sent (no session / no route) past the delivery deadline are
        // failed AND dropped — an ack is never reported failed for a record that
        // could still be written later.
        if (ps.builder && ps.builder->recordCount() > 0 && ps.batchStartNs != 0 &&
            nowNs - ps.batchStartNs >= deliveryNs) {
            if (ps.batchAckCount > 0) {
                completeSlice_(ps, ps.batchAckStart, ps.batchAckCount, -1,
                               "produce delivery timed out (never sent)");
                ps.ackHead = ps.batchAckStart;  // reclaim the unsent slice
            }
            ps.batchAckCount = 0;
            const int64_t resetWall = wallNs();
            ps.builder->reset(0, resetWall);
            ps.builderBaseWallNs = resetWall;
            ps.builderBytes      = 0;
            ps.batchStartNs      = 0;
            ps.flushRequested    = false;
            reclaimAckRing_(ps);
        }
    }
}
```

`failAllPending`:

```cpp
void ProducerImpl::failAllPending(const std::string& reason) {
    // Records still in the app->agent ring (single consumer: we are it).
    ring_.read(
        [&](int32_t type, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
            aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
            if (type == kMsgSendRecord && len >= kRecHeaderSize && (p[kRecFlagsOff] & kFlagWantAck)) {
                PendingAck::complete(static_cast<uint64_t>(readLE64(p, kRecCorrIdOff)), -1, reason);
            } else if (type == kMsgTier2Batch && len >= kT2TokenSize) {
                std::unique_ptr<Tier2Entry> e(
                    reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
                PendingAck::complete(e->ackHandle, -1, reason);
            }
        },
        INT_MAX);
    // Everything the producer accepted: in-flight slices + the current builder slice.
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            emitBatchAck_(ps, b, -1, BatchAck::kClientFailure);
            ps.inflight.release(b);
        });
        for (uint64_t i = ps.ackTail; i < ps.ackHead; ++i) {
            uint64_t& h = ps.ackRing[i & ps.ackMask];
            if (h != 0) {
                PendingAck::complete(h, -1, reason);
                h = 0;
            }
        }
        ps.ackTail = ps.ackHead;
        ps.batchAckCount = 0;
        for (Tier2Entry& e : ps.pendingTier2Entries) PendingAck::complete(e.ackHandle, -1, reason);
        ps.pendingTier2Entries.clear();
        if (ps.builder) ps.builder->reset(0, wallNs());
        ps.builderBytes = 0;
        ps.batchStartNs = 0;
    }
}
```

(`#include <climits>` for `INT_MAX`; `growAckRing` now copies `std::vector<uint64_t>`.)

`ProducerHub::failPending` body becomes `for (auto& impl : producers_) impl->failBatch(corrId, reason);`.

- [ ] **Step 4: Run to verify pass**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='ProducerAcks.*:ProducerInFlight.*:ProducerAllocRegression.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: PASS (`got == {101, 103}`); full suite matches the baseline (ProducerTest ack/timeout/close tests, BatchBuilderTest Tier-2 ordering + futures, TransactionalProducerTest, RedirectTest `failPending` paths).

- [ ] **Step 5: TSan proof of the ownership transfer**

```bash
G=$PWD/build/build/Debug/generators
cmake -S . -B cmake-build-sic-tsan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DBUILD_EXAMPLES=OFF \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -g -O1" -DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread \
  -DCMAKE_TOOLCHAIN_FILE=$G/conan_toolchain.cmake -DCMAKE_PREFIX_PATH=$G
cmake --build cmake-build-sic-tsan --target rune_producer_tests rune_producer_race_tests rune_batch_builder_tests -j8
./cmake-build-sic-tsan/rune_producer_race_tests && ./cmake-build-sic-tsan/rune_producer_tests && ./cmake-build-sic-tsan/rune_batch_builder_tests
```
Expected: PASS with no `WARNING: ThreadSanitizer` lines (the handle is written before the ring commit's release and read after the drain's acquire).

- [ ] **Step 6: Commit**

```bash
git add include/rune/client/producer/Producer.h src/producer/Producer.cpp src/producer/BatchBuilder.cpp \
        include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp \
        include/rune/client/producer/ProducerHub.h src/producer/ProducerHub.cpp \
        tests/producer/ProducerAckPathTest.cpp tests/producer/ProducerTest.cpp
git commit -m "perf(producer): ack ownership through the ring, per-batch acks, no ack mutex

Opt-in PendingAcks are handed to the duty-cycle thread inside the SEND_RECORD
(pointer ownership transfer) and stored at their record's own index in the
partition ack ring: completing a batch walks its slice — no map, no mutex.
Tier-2 entries travel the same way (no side queue + mutex).  New BatchAck
(baseOffset, baseSequence, count, errorCode) via Producer::onBatchAck /
drainBatchAcks.  Fixes mixed void+callback batches resolving callbacks with
the wrong offsets; never-sent records past the delivery deadline are failed
and dropped instead of failed-then-written.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Agent hot path — route slots, getenv once, template poll, ack buffer, owner duty cycle

Spec §4.3: "`getenv("RUNE_MN_DEBUG")`, string concat, string-keyed pub lookup on every routed offer (`ClientAgent::offerProduceToLeader_`, `AeronTransport` endpoint cache) → resolved once per route into a per-partition publication pointer". Today: `ClientAgent.cpp:617` calls `getenv` per routed offer, `:613` probes an `unordered_map<int64_t, std::string>`, `:615` string-compares the endpoint to find the session, then `AeronTransport::offerTo` string-compares the channel (`AeronTransport.cpp:302`). Fixed on sight on the same path: `AeronTransport::poll` takes a `std::function` built every `doWork` and calls it per fragment (`ClientAgent.cpp:186-190`, `AeronTransport.h:72,131`); every ProduceResponse is also pushed through the `std::function` egress observer only to serve transactional producers (`ClientAgent.cpp:489-492`); ProduceResponses reach the hub through a `std::function` hook (`:479-487`). The spec's route model also needs a data channel (worker ingress) distinct from the session endpoint (node) for `ingressEndpoint` (Task 16).

**Files:**
- Modify: `include/rune/client/transport/AeronTransport.h:71-72,127-131,228-230` (template `poll`, thunk members), `src/transport/AeronTransport.cpp` (`start()` assembler lambda, delete `poll` definition, add `resolveSubSlot_`)
- Modify: `include/rune/client/agent/ClientAgent.h:278-304` (routes), `:357-363` (ack buffer), `:508-522` (members)
- Modify: `src/agent/ClientAgent.cpp:65-81` (ctor), `:120-192` (`doWork` step 0 + template poll), `:394`, `:431`, `:563` (`getenv`), `:415` (`routeByPartition_`), `:470-494` (ProduceResponse), `:584-643` (routes + hot path)
- Create: `tests/support/InvokerGateway.h`, `tests/agent/InvokerAgentTest.cpp`
- Modify: `tests/agent/MultiNodeRoutingTest.cpp` (append), `tests/transport/InvokerTransportTest.cpp` (append), `CMakeLists.txt` (`rune_invoker_tests` sources)

**Interfaces:**
- Consumes: `PubHandle`, `publicationHandle`, `offerOn`, `dutyCycle`, `closed` (Tasks 6–7); `FlatIndexMap` (Task 4).
- Produces:
  - `template <class H> int AeronTransport::poll(H&& handler, int fragmentLimit);` (handler: `void(const uint8_t*, std::size_t)`)
  - `bool ClientAgent::setRoute(int32_t topicId, int32_t partition, const std::string& sessionEndpoint, const std::string& dataChannel) noexcept;` (returns changed); 3-arg `setRoute` kept.
  - `[[nodiscard]] std::string ClientAgent::dataChannelFor(int32_t, int32_t) const noexcept;`
  - `struct ClientAgent::ProduceAck { int64_t correlationId; int64_t baseOffsetOrError; };` `std::span<const ProduceAck> ClientAgent::produceAcks() const noexcept;` `void ClientAgent::clearProduceAcks() noexcept;` (buffered only when no `produceResponseFn_` hook is set)
  - `void ClientAgent::setForwardProduceResponses(bool) noexcept;` (default `true` = today's behaviour; the Client turns it off in Task 12)
  - `[[nodiscard]] bool ClientAgent::transportClosed() const noexcept;`
  - `ClientAgent::doWork` runs `transport_.dutyCycle()` first (no-op in thread mode).
  - Test support `test_support::InvokerGateway` (below), used by Tasks 12–16.

- [ ] **Step 1: Write the failing tests**

Create `tests/support/InvokerGateway.h`:

```cpp
// tests/support/InvokerGateway.h
//
// Single-threaded fake broker for invoker-mode tests.  Its Aeron client runs in
// invoker mode and attaches to the CLIENT's embedded driver (same aeron dir), so
// the test thread drives everything by alternating client/agent doWork() and
// gw.pump().  No sleeps, no threads.  Endpoint 0 is the node (control) endpoint;
// addEndpoint() adds worker endpoints, each with its OWN ack publication on the
// client's egress channel (shard-mode brokers ack from a different publication).
#pragma once

#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/MetadataMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

#include <cstdint>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace test_support {

struct InvokerGateway {
    struct PartitionAd {
        int32_t     topicId         = 0;
        int32_t     partition       = 0;
        int32_t     leaderId        = 0;
        int32_t     leaderEpoch     = 1;
        int32_t     ingressStreamId = 0;
        std::string ingressEndpoint;  // "host:port" of the owning worker; "" = node (Task 16)
    };
    struct Produced {
        int32_t topicId;
        int32_t partition;
        int64_t sessionId;
        int64_t corrId;
        int32_t records;
        int32_t baseSequence;
        int     endpoint;  // index of the endpoint that received it (0 = node)
    };
    struct Endpoint {
        std::string channel;
        int32_t     stream = 0;
        int64_t     subReg = -1;
        int64_t     pubReg = -1;
        std::shared_ptr<aeron::Subscription>         sub;
        std::shared_ptr<aeron::ExclusivePublication> pub;  // acks/replies to the client egress
        std::unique_ptr<aeron::FragmentAssembler>    assembler;
    };

    std::string                            egressChannel;
    int32_t                                egressStreamId = 0;
    std::shared_ptr<aeron::Aeron>          aeron;
    std::vector<std::unique_ptr<Endpoint>> endpoints;
    std::vector<std::pair<int, std::vector<uint8_t>>> outbox;  // (endpoint, frame) awaiting a connected pub
    std::vector<PartitionAd> partitions;
    std::string brokerHost;           // "" = advertise no broker entry (routes fall back to the seed)
    int32_t     brokerPort = 0;
    int64_t     assignedSessionId = 42;
    int64_t     nextOffset = 100;
    bool        replyAcks = true;
    std::set<int32_t>     holdPartitions;  // partitions whose acks are held
    std::vector<Produced> produced;
    std::vector<Produced> held;
    int connects = 0;
    int metadataRequests = 0;

    InvokerGateway(const std::string& aeronDir, const std::string& controlChannel,
                   int32_t controlStream, std::string egressCh, int32_t egressStream)
        : egressChannel(std::move(egressCh)), egressStreamId(egressStream) {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        ctx.useConductorAgentInvoker(true);
        ctx.errorHandler([](const std::exception&) {});
        aeron = aeron::Aeron::connect(ctx);
        addEndpoint(controlChannel, controlStream);
    }

    ~InvokerGateway() {
        endpoints.clear();
        aeron.reset();
    }

    int addEndpoint(const std::string& channel, int32_t stream) {
        auto ep = std::make_unique<Endpoint>();
        ep->channel = channel;
        ep->stream  = stream;
        ep->subReg  = aeron->addSubscription(channel, stream);
        ep->pubReg  = aeron->addExclusivePublication(egressChannel, egressStreamId);
        const int index = static_cast<int>(endpoints.size());
        ep->assembler = std::make_unique<aeron::FragmentAssembler>(
            [this, index](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                          aeron::util::index_t len, aeron::Header&) {
                onFrame(index, reinterpret_cast<const uint8_t*>(b.buffer() + off),
                        static_cast<std::size_t>(len));
            });
        endpoints.push_back(std::move(ep));
        return index;
    }

    [[nodiscard]] bool ready() const {
        for (const auto& ep : endpoints) {
            if (!ep->sub || !ep->pub) return false;
        }
        return true;
    }

    /// One gateway duty cycle: conductor, registrations, outbox, polls.
    int pump() {
        int work = aeron->conductorAgentInvoker().invoke();
        for (auto& ep : endpoints) {
            if (!ep->sub) ep->sub = aeron->findSubscription(ep->subReg);
            if (!ep->pub) ep->pub = aeron->findExclusivePublication(ep->pubReg);
        }
        for (std::size_t i = 0; i < outbox.size();) {
            if (offerNow(outbox[i].first, outbox[i].second)) {
                outbox.erase(outbox.begin() + static_cast<std::ptrdiff_t>(i));
                ++work;
            } else {
                ++i;
            }
        }
        for (auto& ep : endpoints) {
            if (ep->sub) work += ep->sub->poll(ep->assembler->handler(), 16);
        }
        return work;
    }

    void send(int endpoint, const uint8_t* b, std::size_t n) {
        std::vector<uint8_t> frame(b, b + n);
        if (!offerNow(endpoint, frame)) outbox.emplace_back(endpoint, std::move(frame));
    }

    /// Unsolicited or solicited MetadataResponse from the node endpoint.
    void sendMetadata(int64_t sessionId, int64_t correlationId) {
        namespace rp = rune::protocol::client;
        std::vector<rp::BrokerEntry> brokers;
        if (!brokerHost.empty()) {
            rp::BrokerEntry b;
            b.nodeId = 0;
            b.host   = brokerHost;  // view into a member: valid through encode()
            b.port   = brokerPort;
            brokers.push_back(b);
        }
        std::vector<rp::PartitionEntry> parts;
        for (const PartitionAd& ad : partitions) {
            rp::PartitionEntry p;
            p.topicId         = ad.topicId;
            p.partition       = ad.partition;
            p.leaderId        = ad.leaderId;
            p.leaderEpoch     = ad.leaderEpoch;
            p.ingressStreamId = ad.ingressStreamId;
            parts.push_back(p);
        }
        rp::MetadataResponse msg;
        msg.sessionId     = sessionId;
        msg.correlationId = correlationId;
        msg.brokers       = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions    = std::span<const rp::PartitionEntry>(parts);
        std::vector<uint8_t> buf(msg.encodedSize());
        const std::size_t n = msg.encode(buf.data());
        send(0, buf.data(), n);
    }

    /// ProduceResponse from the endpoint that received the batch.
    void ack(const Produced& pr, int16_t errorCode = 0) {
        namespace rcc = rune::client::codec;
        uint8_t buf[64];
        const int64_t base = nextOffset;
        nextOffset += pr.records;
        const std::size_t n = rcc::encodeProduceResponse(buf, sizeof buf, errorCode, pr.partition,
                                                         base, pr.sessionId, pr.corrId);
        send(pr.endpoint, buf, n);
    }

    void releaseHeld() {
        std::vector<Produced> h;
        h.swap(held);
        for (const Produced& pr : h) ack(pr);
    }

private:
    bool offerNow(int endpoint, const std::vector<uint8_t>& f) {
        auto& ep = endpoints[static_cast<std::size_t>(endpoint)];
        if (!ep->pub) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()),
                                           static_cast<aeron::util::index_t>(f.size()));
        return ep->pub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }

    void onFrame(int endpoint, const uint8_t* buf, std::size_t len) {
        namespace rcc = rune::client::codec;
        namespace rp  = rune::protocol::client;
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid) return;
        switch (static_cast<rcc::TemplateId>(*tid)) {
        case rcc::TemplateId::Connect: {
            const auto c = rcc::decodeConnectRequest(buf, len);
            if (!c) return;
            ++connects;
            uint8_t out[64];
            const std::size_t n = rcc::encodeConnectResponse(
                out, sizeof out, static_cast<int16_t>(rcc::ErrorCode::OK), 0, assignedSessionId,
                c->sessionId, c->correlationId);
            send(0, out, n);
            return;
        }
        case rcc::TemplateId::MetadataRequest: {
            const auto m = rp::MetadataRequest::decode(buf, len);
            if (!m) return;
            ++metadataRequests;
            sendMetadata(m->sessionId, m->correlationId);
            return;
        }
        case rcc::TemplateId::Produce: {
            const auto pv = rcc::decodeProduce(buf, len);
            if (!pv) return;
            rune::protocol::RecordBatchReader r(pv->recordBatch.data(), pv->recordBatch.size());
            const Produced pr{pv->topicId, pv->partition, pv->sessionId, pv->correlationId,
                              r.recordCount(), r.baseSequence(), endpoint};
            produced.push_back(pr);
            if (pv->correlationId == 0) return;  // acks=None
            if (!replyAcks || holdPartitions.count(pv->partition) != 0) {
                held.push_back(pr);
                return;
            }
            ack(pr);
            return;
        }
        default:
            return;
        }
    }
};

} // namespace test_support
```

Create `tests/agent/InvokerAgentTest.cpp`:

```cpp
// tests/agent/InvokerAgentTest.cpp — ClientAgent in invoker mode against the
// single-threaded InvokerGateway: no sleeps, iteration-capped drive loops.
#include "../support/InvokerGateway.h"

#include <rune/client/agent/ClientAgent.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <unistd.h>

namespace rca = rune::client::agent;
namespace rcs = rune::client::session;

namespace {
std::atomic<int> g_ctr{0};
int64_t monoNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
constexpr int kCap = 200'000;
} // namespace

TEST(InvokerAgent, ProduceResponsesAreBufferedForTheOwnerWhenNoHookIsSet) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3500;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3501;
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3501;
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3500,
                                    "aeron:ipc", 3501);
    auto drive = [&](auto&& pred) {
        for (int i = 0; i < kCap; ++i) {
            agent.doWork(monoNs());
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    ASSERT_TRUE(drive([&] { return agent.sessionState() == rcs::SessionState::Connected; }));

    gw.ack(test_support::InvokerGateway::Produced{1, 0, 42, 777, 3, 0, 0});
    ASSERT_TRUE(drive([&] { return !agent.produceAcks().empty(); }));
    ASSERT_EQ(agent.produceAcks().size(), 1u);
    EXPECT_EQ(agent.produceAcks()[0].correlationId, 777);
    EXPECT_EQ(agent.produceAcks()[0].baseOffsetOrError, 100);
    agent.clearProduceAcks();
    EXPECT_TRUE(agent.produceAcks().empty());
    // Destruction order: gw (declared last) closes its Aeron client before the
    // agent stops the driver it is attached to.
}
```

Append to `tests/agent/MultiNodeRoutingTest.cpp`:

```cpp
// A worker data channel distinct from the session (node) endpoint keeps the route
// even when the session endpoint IS the seed (single-node shard broker).
TEST(MultiNodeRouting, SeparateDataChannelKeepsSessionOnSeed) {
    const std::string seed = "aeron:udp?endpoint=seed:1";
    rca::ClientAgent agent(makeAgentConfig(seed));
    EXPECT_TRUE(agent.setRoute(5, 0, seed, "aeron:udp?endpoint=seed:40500"));
    EXPECT_EQ(agent.routeEndpointFor(5, 0), seed);
    EXPECT_EQ(agent.dataChannelFor(5, 0), "aeron:udp?endpoint=seed:40500");
    EXPECT_TRUE(agent.multiNodeRouting());
    EXPECT_EQ(agent.sessionCount(), 1u);
}

TEST(MultiNodeRouting, SetRouteReportsChanges) {
    const std::string seed = "aeron:udp?endpoint=seed:1";
    rca::ClientAgent agent(makeAgentConfig(seed));
    EXPECT_TRUE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40500"));
    EXPECT_FALSE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40500"));
    EXPECT_TRUE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40501"));
    EXPECT_TRUE(agent.setRoute(5, 1, seed, seed));   // back to the default path: route removed
    EXPECT_TRUE(agent.routeEndpointFor(5, 1).empty());
    EXPECT_FALSE(agent.setRoute(5, 1, seed, seed));
    EXPECT_FALSE(agent.multiNodeRouting());
}

// Regression: getenv("RUNE_MN_DEBUG") ran on every routed offer / session open.
// It is read once, at construction.
TEST(MultiNodeRouting, MnDebugIsReadOnceAtConstructionRegression) {
    ::unsetenv("RUNE_MN_DEBUG");
    rca::ClientAgent agent(makeAgentConfig("aeron:udp?endpoint=seed:1"));
    ::setenv("RUNE_MN_DEBUG", "1", 1);
    testing::internal::CaptureStderr();
    agent.sessionForEndpoint("aeron:udp?endpoint=hostZ:9");
    const std::string err = testing::internal::GetCapturedStderr();
    ::unsetenv("RUNE_MN_DEBUG");
    EXPECT_EQ(err.find("MNDBG"), std::string::npos);
}
```

Append to `tests/transport/InvokerTransportTest.cpp`:

```cpp
// The template poll keeps multi-fragment reassembly (IPC MTU 1408 → 20 KiB = 15 fragments).
TEST(InvokerTransport, FragmentedMessageStillReassemblesThroughTemplatePoll) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3600)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    std::vector<uint8_t> big(20 * 1024);
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<uint8_t>(i * 31);
    ASSERT_TRUE(pumpUntil(t, [&] { return t.offer(big.data(), big.size()); }));
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.poll([&](const uint8_t* b, std::size_t n) { got.assign(b, b + n); }, 32);
        return !got.empty();
    }));
    EXPECT_EQ(got, big);
    t.stop();
}
```

Add `tests/agent/InvokerAgentTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests rune_multinode_routing_tests -j8 2>&1 | grep -m2 error`
Expected: FAIL with `no member named 'produceAcks' in 'rune::client::agent::ClientAgent'` and `no matching member function for call to 'setRoute'` (4 arguments).

- [ ] **Step 3: Template poll in `AeronTransport`**

`AeronTransport.h`: `#include <type_traits>`. Replace the `poll` declaration (`:131`) with:

```cpp
    /// Polls the default egress subscription and every resolved per-worker egress
    /// subscription.  Unfragmented messages (every control frame / ProduceResponse)
    /// go straight to `handler` — an inlined template call, no std::function; only
    /// multi-fragment messages pass through Aeron's FragmentAssembler (the same
    /// UNFRAGMENTED test Aeron uses, FragmentAssembler.h:92).  handler:
    /// void(const uint8_t* buf, std::size_t len); buf valid only during the call.
    template <class H>
    int poll(H&& handler, int fragmentLimit) {
        using HT = std::remove_reference_t<H>;
        pollCtx_   = const_cast<void*>(static_cast<const void*>(&handler));
        pollThunk_ = [](void* ctx, const uint8_t* b, std::size_t n) { (*static_cast<HT*>(ctx))(b, n); };
        auto onFragment = [&handler](aeron::FragmentAssembler& assembler) {
            return [&handler, &assembler](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset, aeron::util::index_t length,
                                          aeron::Header& header) {
                using aeron::concurrent::logbuffer::FrameDescriptor;
                if ((header.flags() & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED) {
                    handler(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                            static_cast<std::size_t>(length));
                } else {
                    assembler.handler()(buf, offset, length, header);
                }
            };
        };
        int frags = 0;
        if (egressSub_ && assembler_) frags += egressSub_->poll(onFragment(*assembler_), fragmentLimit);
        for (SubSlot& s : subSlots_) {
            aeron::Subscription* sub = s.res.ready();
            if (sub == nullptr) sub = resolveSubSlot_(s);
            if (sub != nullptr) frags += sub->poll(onFragment(*s.assembler), fragmentLimit);
        }
        pollCtx_   = nullptr;
        pollThunk_ = nullptr;
        return frags;
    }
```

Private: replace `const Handler* pollHandler_ = nullptr;` with

```cpp
    // Reassembled (multi-fragment) messages reach the current poll() handler via
    // this thunk; set only for the duration of poll().
    void* pollCtx_ = nullptr;
    void (*pollThunk_)(void*, const uint8_t*, std::size_t) = nullptr;
    /// Advances a pending egress SubSlot; on Ready builds its assembler.
    aeron::Subscription* resolveSubSlot_(SubSlot& s) noexcept;
```

`AeronTransport.cpp`: delete the `poll` definition; in `start()` the assembler lambda body becomes `if (pollThunk_) pollThunk_(pollCtx_, reinterpret_cast<const uint8_t*>(buf.buffer() + offset), static_cast<std::size_t>(len));`; add:

```cpp
aeron::Subscription* AeronTransport::resolveSubSlot_(SubSlot& s) noexcept {
    if (!aeron_) return nullptr;
    SubRegistry reg{*aeron_, s.channel, s.streamId};
    if (s.res.poll(reg, monoNs_()) != ResolveStatus::Ready) return nullptr;
    try {
        s.assembler = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                   aeron::util::index_t len, aeron::Header&) {
                if (pollThunk_) {
                    pollThunk_(pollCtx_, reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                               static_cast<std::size_t>(len));
                }
            });
    } catch (...) {
        s.res.reset();
        return nullptr;
    }
    return s.res.ready();
}
```

- [ ] **Step 4: Route slots, getenv once, ack buffer, owner duty cycle in `ClientAgent`**

`ClientAgent.h`: `#include <rune/client/detail/FlatIndexMap.h>` and `#include <span>`. Replace the `setRoute` / `routeEndpointFor` / `multiNodeRouting` declarations (`:278-292`) with:

```cpp
    /// Task-1.3 form: session and produce data share `endpoint`.
    void setRoute(int32_t topicId, int32_t partition, const std::string& endpoint) noexcept {
        (void)setRoute(topicId, partition, endpoint, endpoint);
    }
    /// Routes (topicId, partition): Connect / session id on `sessionEndpoint` (the
    /// node), Produce frames on `dataChannel` (the owning worker's ingress, or the
    /// node).  Both equal to the seed endpoint (or empty) removes the route so the
    /// partition takes the byte-identical default path.  Cold path (metadata).
    /// Returns true when the route changed.
    bool setRoute(int32_t topicId, int32_t partition, const std::string& sessionEndpoint,
                  const std::string& dataChannel) noexcept;

    [[nodiscard]] std::string routeEndpointFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] std::string dataChannelFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] bool multiNodeRouting() const noexcept { return routeIndex_.size() != 0; }

    /// ProduceResponses decoded this duty cycle, for the owner to route with plain
    /// calls (buffered only when no produceResponseFn_ test hook is installed).
    struct ProduceAck {
        int64_t correlationId;
        int64_t baseOffsetOrError;  ///< < 0 = -ErrorCode
    };
    [[nodiscard]] std::span<const ProduceAck> produceAcks() const noexcept {
        return {produceAcks_.data(), produceAcks_.size()};
    }
    void clearProduceAcks() noexcept { produceAcks_.clear(); }

    /// Forward ProduceResponse frames to the egress observer (transactional
    /// producers need them).  Default true (historical behaviour).
    void setForwardProduceResponses(bool on) noexcept { forwardProduceResponses_ = on; }

    [[nodiscard]] bool transportClosed() const noexcept { return transport_.closed(); }
```

Private: replace `std::unordered_map<int64_t, std::string> routeByPartition_;` with

```cpp
    static constexpr uint32_t kNoSession = 0xFFFFFFFFu;
    // One slot per routed partition: strings are cold (setRoute / first offer);
    // the hot path touches only sessionIndex / streamId / pub.
    struct PartitionRoute {
        std::string sessionEndpoint;
        std::string dataChannel;
        uint32_t    sessionIndex = kNoSession;  // resolved on first offer
        int32_t     streamId     = -1;          // stream `pub` was built for
        transport::AeronTransport::PubHandle pub = transport::AeronTransport::kNoPub;
    };
    detail::FlatIndexMap        routeIndex_;   // routeKey(t,p) -> routes_ index
    std::vector<PartitionRoute> routes_;
    std::vector<uint32_t>       freeRoutes_;
    std::vector<ProduceAck>     produceAcks_;  // reserved in the ctor; owner clears
    bool                        forwardProduceResponses_ = true;
    bool                        mnDebug_ = false;  // RUNE_MN_DEBUG, read ONCE
    uint32_t sessionIndexFor_(const std::string& endpoint) noexcept;
```

`ClientAgent.cpp`:
- constructor body, first lines:

```cpp
    mnDebug_ = std::getenv("RUNE_MN_DEBUG") != nullptr;
    produceAcks_.reserve(static_cast<std::size_t>(cfg_.egressPollLimit) * 4);
```

- `doWork`: first statement `int totalWork = transport_.dutyCycle();  // invoker: driver + conductor; thread mode: 0` (replacing `int totalWork = 0;`);
- the three `if (std::getenv("RUNE_MN_DEBUG"))` at `:394`, `:431`, `:563` become `if (mnDebug_)`;
- `:415` `!routeByPartition_.empty()` becomes `routeIndex_.size() != 0`;
- ProduceResponse case (`:470-494`):

```cpp
    case codec::TemplateId::ProduceResponse: {
        // DR-BUG-5: a non-zero errorCode is passed as the negative sentinel -errorCode.
        if (auto opt = codec::decodeProduceResponse(buf, len)) {
            const int64_t sentinel =
                (opt->errorCode != 0)
                    ? -static_cast<int64_t>(static_cast<uint16_t>(opt->errorCode))
                    : opt->baseOffset;
            if (produceResponseFn_) {
                produceResponseFn_(opt->correlationId, sentinel);  // test-harness hook
            } else {
                produceAcks_.push_back(ProduceAck{opt->correlationId, sentinel});
            }
        }
        if (forwardProduceResponses_ && egressObserver_) egressObserver_(tid, buf, len);
        break;
    }
```

- replace `setRoute`, `routeEndpointFor`, `offerProduceToLeader_` (`:584-643`) with:

```cpp
uint32_t ClientAgent::sessionIndexFor_(const std::string& endpoint) noexcept {
    Session& s = sessionForEndpoint(endpoint);
    for (uint32_t i = 0; i < sessions_.size(); ++i) {
        if (sessions_[i].get() == &s) return i;
    }
    return 0;  // unreachable: sessionForEndpoint always returns a member
}

bool ClientAgent::setRoute(int32_t topicId, int32_t partition, const std::string& sessionEndpoint,
                           const std::string& dataChannel) noexcept {
    const int64_t  key  = routeKey(topicId, partition);
    const uint32_t idx  = routeIndex_.find(key);
    const std::string& seed = seedSession().endpoint;
    const bool isDefault = (sessionEndpoint.empty() || sessionEndpoint == seed) &&
                           (dataChannel.empty() || dataChannel == seed);
    if (isDefault) {
        if (idx == detail::FlatIndexMap::kNone) return false;
        routeIndex_.erase(key);
        routes_[idx] = PartitionRoute{};
        freeRoutes_.push_back(idx);
        return true;
    }
    try {
        const std::string& data = dataChannel.empty() ? sessionEndpoint : dataChannel;
        if (idx != detail::FlatIndexMap::kNone) {
            PartitionRoute& r = routes_[idx];
            if (r.sessionEndpoint == sessionEndpoint && r.dataChannel == data) return false;
            r = PartitionRoute{sessionEndpoint, data};
            return true;
        }
        uint32_t slot;
        if (!freeRoutes_.empty()) {
            slot = freeRoutes_.back();
            freeRoutes_.pop_back();
            routes_[slot] = PartitionRoute{sessionEndpoint, data};
        } else {
            slot = static_cast<uint32_t>(routes_.size());
            routes_.push_back(PartitionRoute{sessionEndpoint, data});
        }
        routeIndex_.insert(key, slot);
        return true;
    } catch (...) {
        return false;
    }
}

std::string ClientAgent::routeEndpointFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].sessionEndpoint;
}

std::string ClientAgent::dataChannelFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].dataChannel;
}

bool ClientAgent::offerProduceToLeader_(int32_t topicId, int32_t partition,
                                        int32_t ingressStreamId,
                                        const uint8_t* buf, std::size_t len) noexcept {
    // Single-node / no routes: one size check, then the default offer.
    if (routeIndex_.size() != 0) {
        const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
        if (idx != detail::FlatIndexMap::kNone) {
            PartitionRoute& r = routes_[idx];
            if (r.sessionIndex == kNoSession) r.sessionIndex = sessionIndexFor_(r.sessionEndpoint);  // once
            Session& leader = *sessions_[r.sessionIndex];
            const int64_t sid = leader.machine.sessionId();
            // Leader session still connecting: hold the batch (retried next cycle;
            // produceResendFn_ re-drives it on connect).
            if (sid == 0 || leader.machine.state() != session::SessionState::Connected) return false;
            if (r.streamId != ingressStreamId) {  // once per route / stream change
                const int32_t stream = ingressStreamId != 0 ? ingressStreamId
                                                            : cfg_.transport.ingressStreamId;
                try {
                    r.pub = transport_.publicationHandle(r.dataChannel, stream);
                } catch (...) {
                    return false;
                }
                r.streamId = ingressStreamId;
            }
            if (len >= 16) {
                ::rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, sid);
            }
            return transport_.offerOn(r.pub, buf, len);
        }
    }
    return transport_.offer(ingressStreamId, buf, len);
}
```

- [ ] **Step 5: Run to verify pass, then the full suite**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='InvokerAgent.*:InvokerTransport.*'
./cmake-build-sic/rune_multinode_routing_tests
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: all PASS; full suite matches the baseline (Client still installs the `produceResponseFn_` hook until Task 12, so acks keep flowing through it; MetadataRouting / Redirect / NotLeader / MultiNode suites cover the route rewrite).

- [ ] **Step 6: Commit**

```bash
git add include/rune/client/transport/AeronTransport.h src/transport/AeronTransport.cpp \
        include/rune/client/agent/ClientAgent.h src/agent/ClientAgent.cpp \
        tests/support/InvokerGateway.h tests/agent/InvokerAgentTest.cpp tests/agent/MultiNodeRoutingTest.cpp \
        tests/transport/InvokerTransportTest.cpp CMakeLists.txt
git commit -m "perf(agent): route slots resolved once, getenv once, template poll, ack buffer

Routed produce offers probe a FlatIndexMap slot holding the session index and
a PubHandle resolved on first use (no getenv, string compare/concat or
string-keyed lookup per offer).  setRoute separates the session endpoint
(node) from the data channel (worker ingress).  AeronTransport::poll is a
template with an UNFRAGMENTED fast path (no std::function per duty cycle or
fragment); ProduceResponses are buffered for the owner and forwarded to the
observer only when asked.  doWork runs the transport duty cycle first.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: `Client` invoker runner mode, one duty cycle, plain-call producer/ack path

Spec §4.1: "Invoker mode (new, opt-in): the app thread is the shard — `send()` then the shard duty cycle in the app's own loop." The `Client` keeps its thread mode as the default and gains `RunnerMode::Invoker`: no runner thread, `AeronTransport` in `DriverMode::Invoker`, the app calls `Client::doWork()`. Both modes share one `dutyCycle(nowNs)`: agent → ProduceResponses routed to the hub with plain calls → producer drain/flush through `AgentProduceSink` → periodic sweep. Fixed on sight on the same path: `txnHubMtx_` locked and the transactional-producer `shared_ptr` vector copied on every duty cycle (`Client.cpp:133-137`) and on every ProduceResponse (`:1368-1383`); the txn `SendFn` `std::function` rebuilt every cycle (`:139-142`); the no-progress metadata backstop probing an `unordered_map` every cycle (`:118-125`, moved to the 100 ms sweep — the refresh it arms is itself 100 ms backoff-gated); the setup-phase registration retry loops would spin forever in invoker mode (nobody else drains the ring).

**Files:**
- Modify: `include/rune/client/Client.h:114-152` (`RunnerMode`, `NanoClock`, config fields), `:157-190` (public `doWork`, `transportClosed`, `testTxnProducerCount`), `:451-457` (`sweepTimeouts(int64_t)`), `:528-545` (members)
- Modify: `src/Client.cpp:42-76` (`toAgentConfig`), `:82-199` (ctor), `:213-275` (`close`), `:611-697` (`runLoop`), `:703-760` (`sweepTimeouts`), `:955-963`, `:1138-1146`, `:1368-1453` (txn snapshots), `:1595-1628` (`producer()`), `:1765-1776` (`transactionalProducer()`)
- Modify: `include/rune/client/agent/ClientAgent.h:233-248, 320-323, 428-433, 553-556` (txn registration message + hook), `src/agent/ClientAgent.cpp:160-175, 293-302`
- Modify: `include/rune/client/producer/TransactionalProducerImpl.h:150-151`, `src/producer/TransactionalProducerImpl.cpp:177-179` (`doWork` takes `const std::function&`)
- Create: `include/rune/client/detail/CpuAffinity.h`, `src/detail/CpuAffinity.cpp`
- Create: `tests/client/InvokerClientTest.cpp`
- Modify: `CMakeLists.txt:217-219` (`rune_client_api` sources), `rune_invoker_tests` sources

**Interfaces:**
- Consumes: `AgentProduceSink` (Task 8); `ProducerHub::onProduceResponse` ordinal routing (Task 9); `ClientAgent::produceAcks/clearProduceAcks/setForwardProduceResponses/transportClosed`, `InvokerGateway` (Task 11); `DriverMode`, `clientLivenessTimeoutNs` (Tasks 3, 6).
- Produces:
  - `enum class rune::client::RunnerMode : uint8_t { Thread, Invoker };` `using rune::client::NanoClock = int64_t (*)() noexcept;`
  - `ClientConfig::runnerMode = RunnerMode::Thread`, `ClientConfig::runnerCore = -1`, `ClientConfig::nanoClock = nullptr`, `ClientConfig::aeronClientLivenessTimeoutNs = 10'000'000'000`.
  - `int Client::doWork();` (Invoker only; `std::logic_error` in Thread mode; returns 0 when re-entered from inside a duty-cycle callback)
  - `[[nodiscard]] bool Client::transportClosed() const noexcept;` `[[nodiscard]] std::size_t Client::testTxnProducerCount() const noexcept;`
  - private `int Client::dutyCycle(int64_t nowNs);` `int Client::producerDuty_(int64_t nowNs);` `int64_t Client::now() const noexcept;` `bool inDutyCycle_;` `std::thread::id ownerThread_;` (Task 14 uses the last two)
  - `bool ClientAgent::submitRegisterTxnProducer(void* holder) noexcept;` `void ClientAgent::setTxnProducerRegisterFn(ProducerRegisterFn) noexcept;`
  - `bool rune::client::detail::pinCurrentThreadToCore(int core) noexcept;` `int rune::client::detail::currentCore() noexcept;`

- [ ] **Step 1: Write the failing tests**

Create `tests/client/InvokerClientTest.cpp`:

```cpp
// tests/client/InvokerClientTest.cpp — Client in RunnerMode::Invoker against the
// single-threaded InvokerGateway.  The test thread is the only thread: every drive
// loop is iteration-capped and advances an injected clock instead of sleeping.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/detail/CpuAffinity.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

std::atomic<int> g_ctr{0};
int64_t          g_fakeNow = 0;  // test thread only
int64_t fakeNow() noexcept { return g_fakeNow; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/inv_client_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

template <class F>
bool ready(F& f) { return f.wait_for(0s) == std::future_status::ready; }

struct Rig {
    rc::Client                   client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId,
                        c.egressChannel, c.egressStreamId) {}

    template <class P>
    bool drive(P&& pred, int cap = 200'000, int64_t stepNs = 0) {
        for (int i = 0; i < cap; ++i) {
            g_fakeNow += stepNs;
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }

    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = client.connect();
        ASSERT_TRUE(drive([&] { return ready(f); }));
        f.get();
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
};

const std::vector<uint8_t> kVal{1, 2, 3};

} // namespace

TEST(InvokerClient, ConnectsAndProducesWithNoThreads) {
    Rig r(invokerCfg(3700, 3701));
    r.connect();
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p = r.client.producer(pcfg);
    auto fut = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                      rc::want_future);
    ASSERT_TRUE(r.drive([&] { return ready(fut); }));
    EXPECT_EQ(fut.get(), 100);
}

TEST(InvokerClient, NothingProgressesUnlessTheOwnerCallsDoWork) {
    Rig r(invokerCfg(3702, 3703));
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto f = r.client.connect();
    for (int i = 0; i < 10'000; ++i) r.gw.pump();
    EXPECT_EQ(r.gw.connects, 0) << "no background runner may drain the connect command";
    ASSERT_TRUE(r.drive([&] { return ready(f); }));
}

TEST(InvokerClient, InjectedClockDrivesRetransmitAndDeliveryTimeout) {
    rc::ClientConfig c = invokerCfg(3710, 3711);
    g_fakeNow   = realNow();
    c.nanoClock = &fakeNow;
    Rig r(c);
    r.connect();
    r.gw.replyAcks = false;
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    pcfg.produceTimeoutMs = 50;
    pcfg.deliveryTimeoutMs = 120;
    auto p = r.client.producer(pcfg);
    auto fut = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                      rc::want_future);
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; }));
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 2; }, 1'000, 1'000'000))
        << "retransmit after produceTimeoutMs of INJECTED time";
    ASSERT_TRUE(r.drive([&] { return ready(fut); }, 1'000, 1'000'000));
    EXPECT_THROW(fut.get(), std::runtime_error);
}

// Regression: transactionalProducer() pushed into a mutex-guarded vector that the
// runner locked and copied every duty cycle.  Registration now crosses through the
// agent ring like every other app->runner event.
TEST(InvokerClient, TxnProducerRegistersThroughTheRingRegression) {
    Rig r(invokerCfg(3720, 3721));
    auto tp = r.client.transactionalProducer("tx-1");
    EXPECT_EQ(r.client.testTxnProducerCount(), 0u);
    r.client.doWork();
    EXPECT_EQ(r.client.testTxnProducerCount(), 1u);
}

TEST(InvokerClient, DoWorkIsRejectedInThreadMode) {
    rc::ClientConfig c = invokerCfg(3730, 3731);
    c.runnerMode = rc::RunnerMode::Thread;
    rc::Client client(c);
    EXPECT_THROW(client.doWork(), std::logic_error);
}

TEST(CpuAffinity, PinsTheCallingThreadOnLinux) {
#if defined(__linux__)
    bool pinned = false;
    int  core   = -1;
    std::thread t([&] {
        pinned = rune::client::detail::pinCurrentThreadToCore(0);
        core   = rune::client::detail::currentCore();
    });
    t.join();
    EXPECT_TRUE(pinned);
    EXPECT_EQ(core, 0);
#else
    EXPECT_FALSE(rune::client::detail::pinCurrentThreadToCore(0));
    GTEST_SKIP() << "thread affinity is unsupported on this OS (macOS)";
#endif
}
```

Add `tests/client/InvokerClientTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'RunnerMode' in namespace 'rune::client'`.

- [ ] **Step 3: CPU affinity helper**

`include/rune/client/detail/CpuAffinity.h`:

```cpp
// include/rune/client/detail/CpuAffinity.h — thread pinning (cold path: thread start).
#pragma once

namespace rune::client::detail {
/// Pins the calling thread to one CPU (Linux pthread_setaffinity_np).  Returns
/// false where unsupported (macOS) or on failure.
bool pinCurrentThreadToCore(int core) noexcept;
/// CPU the calling thread runs on (Linux sched_getcpu), -1 where unsupported.
int currentCore() noexcept;
} // namespace rune::client::detail
```

`src/detail/CpuAffinity.cpp`:

```cpp
// src/detail/CpuAffinity.cpp
#include <rune/client/detail/CpuAffinity.h>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace rune::client::detail {

bool pinCurrentThreadToCore(int core) noexcept {
#if defined(__linux__)
    if (core < 0 || core >= CPU_SETSIZE) return false;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)core;
    return false;
#endif
}

int currentCore() noexcept {
#if defined(__linux__)
    return sched_getcpu();
#else
    return -1;
#endif
}

} // namespace rune::client::detail
```

In `CMakeLists.txt:217-219` add `src/detail/CpuAffinity.cpp` to `add_library(rune_client_api STATIC ...)`.

- [ ] **Step 4: Transactional-producer registration through the agent ring**

`ClientAgent.h`: after `submitRegisterProducer`:

```cpp
    /// Same shared-nothing hand-off for a transactional producer: `holder` is a heap
    /// std::shared_ptr<TransactionalProducerImpl>* adopted on the duty-cycle thread.
    bool submitRegisterTxnProducer(void* holder) noexcept;
    void setTxnProducerRegisterFn(ProducerRegisterFn fn) noexcept {
        txnProducerRegisterFn_ = std::move(fn);
    }
```

private: `static constexpr int32_t kMsgRegisterTxnProducer = 7;` next to `kMsgRegisterProducer`; member `ProducerRegisterFn txnProducerRegisterFn_;` next to `producerRegisterFn_`.

`ClientAgent.cpp`: `doWork` switch — add after the `kMsgRegisterProducer` case:

```cpp
            case kMsgRegisterTxnProducer:
                if (txnProducerRegisterFn_) {
                    const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                    auto msg = ring_msg::RegisterProducer::decode(p, static_cast<std::size_t>(len));
                    if (msg) txnProducerRegisterFn_(reinterpret_cast<void*>(msg->handle));
                }
                break;
```

and

```cpp
bool ClientAgent::submitRegisterTxnProducer(void* holder) noexcept {
    ring_msg::RegisterProducer msg;
    msg.handle = reinterpret_cast<uint64_t>(holder);
    uint8_t buf[ring_msg::RegisterProducer::ENCODED_SIZE];
    const std::size_t n = msg.encode(buf);
    return writeCommand(kMsgRegisterTxnProducer, buf, n);
}
```

`TransactionalProducerImpl.h:150-151` and `.cpp:177-179`: the `sendFn` parameter becomes `const std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn`.

- [ ] **Step 5: `ClientConfig`, `toAgentConfig`, members**

`Client.h`, before `struct ClientConfig`:

```cpp
// RunnerMode — who runs the client duty cycle (spec §4.1).
//   Thread  (default): a background runner thread under the IdleMode strategy.
//   Invoker (opt-in):  no threads at all — the embedded driver and the Aeron
//                      client conductor are invoker-driven and the constructing
//                      thread calls Client::doWork() in its own loop.
enum class RunnerMode : uint8_t { Thread, Invoker };

// Injected monotonic clock (ns), sampled once per duty cycle.  nullptr = steady clock.
using NanoClock = int64_t (*)() noexcept;
```

in `struct ClientConfig` after `idleMode`:

```cpp
    RunnerMode runnerMode = RunnerMode::Thread;
    // Thread mode: pin the runner thread to this CPU (Linux; -1 = do not pin).
    int32_t    runnerCore = -1;
    // Duty-cycle clock (tests inject; nullptr = std::chrono::steady_clock).
    NanoClock  nanoClock  = nullptr;
    // Aeron client liveness: in Invoker mode doWork() must run at least this often.
    int64_t    aeronClientLivenessTimeoutNs = 10'000'000'000LL;
```

public, after `close()`:

```cpp
    /// Invoker mode: one duty cycle (driver + conductor, commands, egress, sessions,
    /// ProduceResponses, producers, periodic sweep).  Returns the work count
    /// (0 = idle; the caller applies its own idle strategy).  Call only from the
    /// constructing thread.  Re-entry from a callback fired inside the duty cycle
    /// returns 0 without running a nested cycle.  Thread mode: throws logic_error.
    int doWork();

    /// True once the Aeron client closed itself (invoker owner stalled past
    /// aeronClientLivenessTimeoutNs, or the driver timed out).
    [[nodiscard]] bool transportClosed() const noexcept { return agent_.transportClosed(); }

    /// Test hook: transactional producers adopted by the duty-cycle thread.
    [[nodiscard]] std::size_t testTxnProducerCount() const noexcept { return txnProducers_.size(); }
```

private: replace `void sweepTimeouts();` with `void sweepTimeouts(int64_t nowNs);`; replace `std::chrono::steady_clock::time_point nextSweep_;` with:

```cpp
    int  dutyCycle(int64_t nowNs);
    int  producerDuty_(int64_t nowNs);
    [[nodiscard]] int64_t now() const noexcept {
        return cfg_.nanoClock != nullptr ? cfg_.nanoClock() : nowNs();
    }
    int64_t         nextSweepNs_ = 0;       // duty-cycle thread only
    bool            inDutyCycle_ = false;   // duty-cycle thread only
    std::thread::id ownerThread_;           // constructing thread (Invoker owner)
    // Built ONCE: the txn producers' send paths (no per-cycle std::function build).
    std::function<bool(int32_t, const uint8_t*, std::size_t)> txnSendFn_;
    std::function<bool(int32_t, const uint8_t*, std::size_t)> txnControlSendFn_;
```

delete `std::mutex txnHubMtx_;` and change the `txnProducers_` comment to "duty-cycle-thread owned (registered through the agent ring)".

`Client.cpp`: `#include <rune/client/agent/AgentProduceSink.h>` and `#include <rune/client/detail/CpuAffinity.h>`; in `toAgentConfig` before `applyEnvOverrides`:

```cpp
    acfg.transport.driverMode = (c.runnerMode == RunnerMode::Invoker)
                                    ? transport::DriverMode::Invoker
                                    : transport::DriverMode::Thread;
    acfg.transport.clientLivenessTimeoutNs = c.aeronClientLivenessTimeoutNs;
```

- [ ] **Step 6: Constructor, duty cycle, runner, sweep**

Constructor: delete the initializer `, nextSweep_(std::chrono::steady_clock::now() + std::chrono::milliseconds(kSweepIntervalMs))`; replace the body between the egress-observer wiring and the end (`:99-198`) with:

```cpp
    // Produce + ack path: plain member calls from dutyCycle() (spec §4.3).  No
    // producer-drain / ProduceResponse std::function hooks are installed on the
    // agent (they remain only as test-harness seams).
    agent_.setForwardProduceResponses(false);  // re-enabled when a txn producer registers

    // Task 1.4 (multi-node redirect convergence) — cold path, unchanged.
    agent_.setProduceResendFn([this]() {
        producer::ProducerImpl::SendFn sendFn =
            [this](int32_t t, int32_t p, int32_t sid, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.offerToPartition(t, p, sid, fb, fl);
            };
        producerHub_.resendAllViaRoute(sendFn);
    });

    agent_.setProducerRegisterFn([this](void* holder) {
        auto* sp = static_cast<std::shared_ptr<producer::ProducerImpl>*>(holder);
        producerHub_.add(std::move(*sp));
        delete sp;
    });
    agent_.setTxnProducerRegisterFn([this](void* holder) {
        auto* sp = static_cast<std::shared_ptr<producer::TransactionalProducerImpl>*>(holder);
        txnProducers_.push_back(std::move(*sp));
        delete sp;
        agent_.setForwardProduceResponses(true);  // txn producers consume raw ProduceResponses
    });
    agent_.setMetadataResponseFn([this](const codec::MetadataResponseDecoded& resp) {
        onMetadataResponse(resp);
    });

    txnSendFn_ = [this](int32_t streamId, const uint8_t* buf, std::size_t len) -> bool {
        return agent_.offerToPartition(/*topicId=*/0, /*partition=*/0, streamId, buf, len);
    };
    txnControlSendFn_ = [this](int32_t, const uint8_t* buf, std::size_t len) -> bool {
        return agent_.submitSend(buf, len);
    };

    agent_.start();  // Invoker: pumps its own driver while resolving (no threads)
    ownerThread_ = std::this_thread::get_id();
    nextSweepNs_ = now() + kSweepIntervalMs * 1'000'000LL;
    if (cfg_.runnerMode == RunnerMode::Thread) {
        runnerThread_ = std::thread([this]() { runLoop(); });
    }
```

Add:

```cpp
int Client::doWork() {
    if (cfg_.runnerMode != RunnerMode::Invoker) {
        throw std::logic_error("Client::doWork(): only valid in RunnerMode::Invoker");
    }
    assert(std::this_thread::get_id() == ownerThread_ && "doWork() from a non-owner thread");
    if (inDutyCycle_) return 0;  // re-entered from a callback: never nest the duty cycle
    return dutyCycle(now());
}

int Client::dutyCycle(int64_t nowNs) {
    struct InCycle {
        bool& flag;
        explicit InCycle(bool& f) : flag(f) { flag = true; }
        ~InCycle() { flag = false; }
    } inCycle(inDutyCycle_);

    int work = agent_.doWork(nowNs);
    const auto acks = agent_.produceAcks();
    const int ackCount = static_cast<int>(acks.size());
    for (const auto& a : acks) producerHub_.onProduceResponse(a.correlationId, a.baseOffsetOrError);
    agent_.clearProduceAcks();
    work += ackCount + producerDuty_(nowNs);
    if (nowNs >= nextSweepNs_) {
        sweepTimeouts(nowNs);
        nextSweepNs_ = nowNs + kSweepIntervalMs * 1'000'000LL;
    }
    return work;
}

int Client::producerDuty_(int64_t nowNs) {
    const int64_t sessionId = agent_.seedSession().machine.sessionId();
    int work = producerHub_.drainAndFlush(nowNs, sessionId, agent::AgentProduceSink{&agent_});
    for (auto& tp : txnProducers_) work += tp->doWork(sessionId, txnSendFn_);
    return work;
}
```

`runLoop()` body becomes:

```cpp
void Client::runLoop() {
    if (cfg_.runnerCore >= 0 && !detail::pinCurrentThreadToCore(cfg_.runnerCore)) {
        std::fprintf(stderr, "rune-client: could not pin the runner to core %d\n", cfg_.runnerCore);
    }
    if (cfg_.idleMode == IdleMode::LowLatency) {
        // spin 5, yield 3, park 1 µs .. 100 µs: the runner drives the producer drain
        // AND the ack poll, so its park ceiling is the client floor on ack latency.
        aeron::concurrent::BackoffIdleStrategy idle(
            5, 3, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        while (!stopped_.load(std::memory_order_acquire)) idle.idle(dutyCycle(now()));
    } else if (cfg_.idleMode == IdleMode::BusySpin) {
        // Never park or yield: pin this runner to a dedicated core.
        while (!stopped_.load(std::memory_order_acquire)) {
            if (dutyCycle(now()) == 0) {
#if defined(__x86_64__) || defined(__i386__)
                __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
                __asm__ volatile("yield" ::: "memory");
#else
                __asm__ volatile("" ::: "memory");
#endif
            }
        }
    } else {
        // Balanced: spins -> yields -> parks (max 1 ms).
        aeron::concurrent::BackoffIdleStrategy idle(
            10, 20, std::chrono::duration<long, std::nano>(1'000), std::chrono::milliseconds(1));
        while (!stopped_.load(std::memory_order_acquire)) idle.idle(dutyCycle(now()));
    }
}
```

`sweepTimeouts(int64_t nowNs)`: signature change; replace `producerHub_.sweepTimedOut(now);` with

```cpp
    // Producer deadlines are on the duty-cycle clock (injected in tests).
    producerHub_.sweepTimedOut(
        std::chrono::steady_clock::time_point{std::chrono::nanoseconds{nowNs}});
    // No-progress backstop (moved from the per-cycle drain hook; the refresh it arms
    // is itself backoff-gated at >= 100 ms, so the 100 ms sweep cadence loses nothing).
    {
        int32_t topicId = -1, partition = -1;
        if (producerHub_.firstPendingPartition(topicId, partition) &&
            metadataCache_.leaderFor(topicId, partition) == metadata::MetadataCache::kNoLeader) {
            scheduleMetadataRefresh();
        }
    }
```

(the rest of `sweepTimeouts` keeps its own `steady_clock::now()` for the connect/ping/subscribe promise deadlines, which are created on app threads with that clock.)

- [ ] **Step 7: Transactional producers without the mutex**

Every `std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap; { std::lock_guard<std::mutex> lk(txnHubMtx_); txnSnap = txnProducers_; }` block in `onEgressFrame` (`:1368-1453`) becomes a direct loop over `txnProducers_` (duty-cycle thread); the per-frame `sendFn` lambdas become `txnControlSendFn_`. The two `txnPending` blocks (`:958-963`, `:1141-1146`) become `const bool txnPending = !txnProducers_.empty();`. In `close()` the txn loop loses its lock (runs after the runner join / on the owner thread). `transactionalProducer()`:

```cpp
TransactionalProducer Client::transactionalProducer(const std::string& transactionalId,
                                                    ProducerConfig cfg)
{
    auto impl = std::make_shared<producer::TransactionalProducerImpl>(transactionalId, cfg, nextCorrId_);
    auto* holder = new std::shared_ptr<producer::TransactionalProducerImpl>(impl);
    submitRegistration_([&] { return agent_.submitRegisterTxnProducer(holder); },
                        [&] { delete holder; });
    return TransactionalProducer(std::move(impl));
}
```

and `producer()` uses the same helper (replacing its inline retry loop):

```cpp
    auto* holder = new std::shared_ptr<producer::ProducerImpl>(impl);
    submitRegistration_([&] { return agent_.submitRegisterProducer(holder); },
                        [&] { delete holder; });
    return Producer(std::move(impl));
```

with the private helper (declare in `Client.h` next to `dutyCycle`):

```cpp
    /// Setup-phase ring submit, bounded (5 s).  Invoker mode runs the duty cycle
    /// between attempts — this thread IS the ring's consumer; thread mode parks
    /// 100 µs.  On timeout runs `onFail` and throws.
    template <class Submit, class OnFail>
    void submitRegistration_(Submit&& submit, OnFail&& onFail) {
        if (submit()) return;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!submit()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                onFail();
                throw std::runtime_error("Client: command ring full — registration timed out");
            }
            if (cfg_.runnerMode == RunnerMode::Invoker) {
                (void)dutyCycle(now());
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
        }
    }
```

- [ ] **Step 8: Run to verify pass, then everything (thread mode must be unchanged)**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='InvokerClient.*:CpuAffinity.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
cmake --build cmake-build-sic-tsan --target rune_client_api_tests rune_producer_race_tests rune_txn_producer_tests -j8
./cmake-build-sic-tsan/rune_client_api_tests && ./cmake-build-sic-tsan/rune_producer_race_tests && ./cmake-build-sic-tsan/rune_txn_producer_tests
```
Expected: 6 new tests PASS (`CpuAffinity` SKIPPED on macOS); full suite matches the baseline; TSan clean (no `WARNING: ThreadSanitizer`).

- [ ] **Step 9: Commit**

```bash
git add include/rune/client/Client.h src/Client.cpp include/rune/client/agent/ClientAgent.h src/agent/ClientAgent.cpp \
        include/rune/client/producer/TransactionalProducerImpl.h src/producer/TransactionalProducerImpl.cpp \
        include/rune/client/detail/CpuAffinity.h src/detail/CpuAffinity.cpp \
        tests/client/InvokerClientTest.cpp CMakeLists.txt
git commit -m "feat(client): RunnerMode::Invoker + one duty cycle with plain-call produce/ack

The Client runs agent -> ProduceResponses -> producer drain/flush -> sweep as
one dutyCycle(nowNs), from the runner thread (default) or from the app via
Client::doWork() with no threads at all (invoker driver + conductor).  The
producer sink and ack routing are plain calls; the clock is injectable.
Fixed on sight: txnHubMtx_ lock + shared_ptr vector copy every cycle and
every ProduceResponse (txn producers now register through the agent ring),
txn SendFn rebuilt per cycle, metadata backstop probing per cycle, and the
registration retry loops that could not progress in invoker mode.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: `send()` never drops — `SendResult`, back-pressure propagated, void overload backs off

Spec §4.3: "`void Producer::send` silently drops on a full ring → returns/propagates back-pressure; the void overload backs off and retries, never drops". Today `Producer::send` (`Producer.cpp:51-61`) ignores `submitSendRecord`'s `false`. The void overload must also not wait forever for a runner that is gone: it watches the ring's consumer heartbeat, exactly like Aeron's `ManyToOneRingBuffer::consumerHeartbeatTime` (the M2O ring already reserves `CONSUMER_HEARTBEAT_OFFSET`, `M2ORingBuffer.h:56`, with no accessor).

**Files:**
- Modify: `include/rune/client/detail/M2ORingBuffer.h:62-71` (heartbeat accessors)
- Modify: `include/rune/client/producer/Producer.h:63-87` (`agentLivenessTimeoutMs`), `:114-143` (`SendResult`, `trySend`, void `send` contract)
- Modify: `src/producer/Producer.cpp:51-105`
- Modify: `include/rune/client/producer/ProducerImpl.h:237-249` (`trySubmit`, `sendBlocking`, clock), `src/producer/ProducerImpl.cpp:407-420` (`drainRing` heartbeat)
- Modify: `include/rune/client/TypedTopic.h:22,103` (drop `noexcept` — the void send can now throw on invalid input)
- Modify: `tests/producer/ProducerTest.cpp:1339-1380` (`OversizedValueRejected`: the oversize value is now reported, not silently dropped)
- Create: `tests/producer/ProducerSendTest.cpp`; `CMakeLists.txt` (`rune_invoker_tests`)

**Interfaces:**
- Consumes: `failUnsubmitted` (Task 10), `CountingSink` (Task 8).
- Produces:
  - `enum class rune::client::SendResult : uint8_t { Ok, BackPressured, ValueTooLarge, InvalidTopic, WrongThread, WrongShard, Closed };`
  - `[[nodiscard]] SendResult Producer::trySend(TopicHandle, const Key&, std::span<const uint8_t>) noexcept;`
  - `void Producer::send(TopicHandle, const Key&, std::span<const uint8_t>);` — never drops; throws `std::length_error` / `std::invalid_argument` / `std::logic_error` / `std::runtime_error` (see doc).
  - `ProducerConfig::agentLivenessTimeoutMs = 1000`.
  - `[[nodiscard]] SendResult ProducerImpl::trySubmit(int32_t topicId, int32_t partition, const Key& key, std::span<const uint8_t> value, int64_t ackHandle) noexcept;`
  - `void ProducerImpl::sendBlocking(SendResult first, int32_t topicId, int32_t partition, const Key& key, std::span<const uint8_t> value);`
  - `using ProducerImpl::ClockFn = int64_t (*)() noexcept; void ProducerImpl::setClockForTesting(ClockFn) noexcept;`
  - `void M2ORingBuffer::consumerHeartbeatTime(int64_t) noexcept; int64_t M2ORingBuffer::consumerHeartbeatTime() const noexcept;`

- [ ] **Step 1: Write the failing tests**

Create `tests/producer/ProducerSendTest.cpp`:

```cpp
// tests/producer/ProducerSendTest.cpp — back-pressure is propagated, never dropped.
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::Producer;
using rune::client::ProducerConfig;
using rune::client::SendResult;
using rune::client::TopicHandle;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;

namespace {
ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks = Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}
int64_t steadyNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
const TopicHandle kTopic{1, 4};
const std::vector<uint8_t> kVal(64, 0x42);
} // namespace

// Regression: Producer::send(void) returned silently when the producer ring was
// full — the record was dropped.  It must back off until the runner drains.
TEST(ProducerSend, VoidSendNeverDropsOnFullRingRegression) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, /*ringCap=*/4 * 1024);
    Producer p(impl);
    constexpr uint64_t kRecords = 2000;
    std::atomic<bool> sendsDone{false};
    std::thread drainer([&] {  // plays the runner thread
        uint64_t frames = 0, bytes = 0;
        const CountingSink sink{&frames, &bytes};
        for (int i = 0; i < 20'000'000; ++i) {
            const int64_t now = steadyNs();
            const int d = impl->drainRing(now);
            impl->flushBatches(now, 42, sink);
            if (impl->recordsSent() >= kRecords) break;
            if (d == 0 && sendsDone.load(std::memory_order_acquire)) break;
        }
    });
    for (uint64_t i = 0; i < kRecords; ++i) p.send(kTopic, Key(i), kVal);
    sendsDone.store(true, std::memory_order_release);
    drainer.join();
    EXPECT_EQ(impl->recordsSent(), kRecords);
}

TEST(ProducerSend, VoidSendThrowsWhenTheRunnerStopsDraining) {
    ProducerConfig cfg = voidCfg();
    cfg.agentLivenessTimeoutMs = 5;
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(cfg, corr, 4 * 1024);
    // Injected clock: +1 ms per read, so liveness expires after a few back-offs.
    impl->setClockForTesting(+[]() noexcept -> int64_t {
        static int64_t t = 0;
        return t += 1'000'000;
    });
    Producer p(impl);
    for (int i = 0; i < 1000 && p.trySend(kTopic, Key(uint64_t{1}), kVal) == SendResult::Ok; ++i) {}
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), kVal), std::runtime_error);
}

TEST(ProducerSend, TrySendReportsBackPressureAndLosesNothing) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, 4 * 1024);
    Producer p(impl);
    uint64_t ok = 0;
    SendResult last = SendResult::Ok;
    for (int i = 0; i < 1000 && last == SendResult::Ok; ++i) {
        last = p.trySend(kTopic, Key(static_cast<uint64_t>(i)), kVal);
        if (last == SendResult::Ok) ++ok;
    }
    ASSERT_EQ(last, SendResult::BackPressured);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int i = 0; i < 100'000 && impl->drainRing(1) > 0; ++i) impl->flushBatches(1, 42, sink);
    EXPECT_EQ(impl->recordsSent(), ok);
}

TEST(ProducerSend, InvalidInputIsReportedNeverSilentlyDropped) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr);
    Producer p(impl);
    const std::vector<uint8_t> tooBig(Producer::kMaxSendValueBytes + 1, 0);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), tooBig), SendResult::ValueTooLarge);
    EXPECT_EQ(p.trySend(TopicHandle{}, Key(uint64_t{1}), kVal), SendResult::InvalidTopic);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), tooBig), std::length_error);
    EXPECT_THROW(p.send(TopicHandle{}, Key(uint64_t{1}), kVal), std::invalid_argument);
}
```

In `tests/producer/ProducerTest.cpp` `OversizedValueRejected`, replace

```cpp
    // Value one byte over — must be silently dropped.
    const std::vector<uint8_t> overVal(rc::Producer::kMaxSendValueBytes + 1, 0xBB);
    p.send(topic, key, std::span<const uint8_t>(overVal)); // must not crash
```
with
```cpp
    // Value one byte over — reported to the caller, never silently dropped.
    const std::vector<uint8_t> overVal(rc::Producer::kMaxSendValueBytes + 1, 0xBB);
    EXPECT_EQ(p.trySend(topic, key, std::span<const uint8_t>(overVal)), rc::SendResult::ValueTooLarge);
    EXPECT_THROW(p.send(topic, key, std::span<const uint8_t>(overVal)), std::length_error);
```
(and the header comment "the send is silently dropped" at `:1339` becomes "the send is rejected with ValueTooLarge / std::length_error"; the final `EXPECT_EQ(... batchCount ..., 1)` stays.)

Add `tests/producer/ProducerSendTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no type named 'SendResult' in namespace 'rune::client'`. (With only the declarations added, `VoidSendNeverDropsOnFullRingRegression` fails with `recordsSent() < 2000`.)

- [ ] **Step 3: Implement**

`M2ORingBuffer.h`, public, after `trailerLength()`:

```cpp
    /// Aeron ManyToOneRingBuffer::consumerHeartbeatTime: the single consumer
    /// publishes its liveness (ordered store) so producers can tell a slow
    /// consumer from a dead one — the ring primitive's own atomic, no other
    /// shared state (spec §1a.1).
    void consumerHeartbeatTime(std::int64_t timeNs) noexcept {
        m_buffer.putInt64Ordered(m_consumerHeartbeatIndex, timeNs);
    }
    [[nodiscard]] std::int64_t consumerHeartbeatTime() const noexcept {
        return m_buffer.getInt64Volatile(m_consumerHeartbeatIndex);
    }
```

`Producer.h`: in `ProducerConfig` after `deliveryTimeoutMs`:

```cpp
    int64_t agentLivenessTimeoutMs = 1000; // void send(): back-pressured with no ring-consumer
                                           // heartbeat for this long => throw (runner gone)
```

before `class Producer`:

```cpp
// ---------------------------------------------------------------------------
// SendResult — outcome of a non-blocking send (spec §4.3: back-pressure is
// propagated, never a silent drop).
// ---------------------------------------------------------------------------
enum class SendResult : uint8_t {
    Ok = 0,         ///< accepted: will be batched and sent
    BackPressured,  ///< ring full / partition window full: retry after the duty cycle drains
    ValueTooLarge,  ///< value > kMaxSendValueBytes (never retryable)
    InvalidTopic,   ///< TopicHandle not valid
    WrongThread,    ///< invoker-mode producer used off its owner thread
    WrongShard,     ///< ClientShard: partition owned by another shard
    Closed,         ///< the shard's transport closed
};
```

replace the void `send` declaration + its "Drop policy" doc with:

```cpp
    /// Non-blocking send (ZERO per-send heap allocation, lock-free).  Never drops
    /// silently: anything but Ok means the record was NOT accepted.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept;

    /// Fire-and-forget send that NEVER drops.  BackPressured → backs off (thread
    /// mode: spin/yield/park until the runner drains; invoker mode: runs the duty
    /// cycle itself) and retries.  Throws std::length_error (value too large),
    /// std::invalid_argument (invalid topic), std::logic_error (off the invoker
    /// owner thread, or back-pressured inside an invoker duty-cycle callback —
    /// use trySend there), std::runtime_error (the runner stopped draining for
    /// agentLivenessTimeoutMs — client closed).  Zero allocation when accepted.
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);
```

`Producer.cpp` — replace the void send and the two opt-in overloads' submit/fail lines:

```cpp
SendResult Producer::trySend(TopicHandle topic, const Key& key,
                             std::span<const uint8_t> value) noexcept
{
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    return impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value, 0);
}

void Producer::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value)
{
    if (!impl_ || !topic.valid()) {
        throw std::invalid_argument("Producer::send: invalid topic handle");
    }
    const int32_t part = key.partition(topic.partitionCount);
    const SendResult r = impl_->trySubmit(topic.topicId, part, key, value, 0);
    if (r == SendResult::Ok) [[likely]] return;
    impl_->sendBlocking(r, topic.topicId, part, key, value);
}
```

in both opt-in overloads: `if (impl_->trySubmit(topic.topicId, part, key, value, ackHandle) != SendResult::Ok) { impl_->failUnsubmitted(ackHandle, "producer send not accepted (back-pressure or invalid value) — retry"); }`.

`ProducerImpl.h`, public (app-thread API):

```cpp
    [[nodiscard]] SendResult trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value, int64_t ackHandle) noexcept;
    /// Slow path of the void send: retries until accepted; throws on non-retryable results.
    void sendBlocking(SendResult first, int32_t topicId, int32_t partition, const Key& key,
                      std::span<const uint8_t> value);
    using ClockFn = int64_t (*)() noexcept;
    void setClockForTesting(ClockFn fn) noexcept { clock_ = fn; }
```

private: `ClockFn clock_ = &steadyNowNs_; static int64_t steadyNowNs_() noexcept;`

`ProducerImpl.cpp` (`#include <concurrent/BackOffIdleStrategy.h>`, `<algorithm>`):

```cpp
int64_t ProducerImpl::steadyNowNs_() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

SendResult ProducerImpl::trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                   std::span<const uint8_t> value, int64_t ackHandle) noexcept {
    if (value.size() > rune::client::Producer::kMaxSendValueBytes) return SendResult::ValueTooLarge;
    return submitSendRecord(topicId, partition, key, value, ackHandle != 0, ackHandle)
               ? SendResult::Ok
               : SendResult::BackPressured;
}

void ProducerImpl::sendBlocking(SendResult r, int32_t topicId, int32_t partition, const Key& key,
                                std::span<const uint8_t> value) {
    aeron::concurrent::BackoffIdleStrategy idle(
        100, 10, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    const int64_t startNs    = clock_();
    const int64_t livenessNs = cfg_.agentLivenessTimeoutMs * 1'000'000LL;
    for (;;) {
        switch (r) {
        case SendResult::Ok:            return;
        case SendResult::BackPressured: break;
        case SendResult::ValueTooLarge:
            throw std::length_error("Producer::send: value exceeds Producer::kMaxSendValueBytes");
        case SendResult::InvalidTopic:
            throw std::invalid_argument("Producer::send: invalid topic handle");
        case SendResult::WrongThread:
            throw std::logic_error("Producer::send: invoker-mode producer used off its owner thread");
        case SendResult::WrongShard:
            throw std::logic_error("Producer::send: partition owned by another shard");
        case SendResult::Closed:
            throw std::runtime_error("Producer::send: producer closed");
        }
        idle.idle(0);
        const int64_t nowNs = clock_();
        if (nowNs - std::max(ring_.consumerHeartbeatTime(), startNs) > livenessNs) {
            throw std::runtime_error(
                "Producer::send: back-pressured and the runner stopped draining for "
                "> agentLivenessTimeoutMs (client closed?)");
        }
        r = trySubmit(topicId, partition, key, value, 0);
    }
}
```

`drainRing` — first statement: `ring_.consumerHeartbeatTime(nowNs);`.

`TypedTopic.h:103` `void send(const T& rec) noexcept {` → `void send(const T& rec) {` (and the doc line at `:22` drops `noexcept`).

- [ ] **Step 4: Run to verify pass, then everything**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='ProducerSend.*'
./cmake-build-sic/rune_producer_tests --gtest_filter='Producer.OversizedValueRejected:Producer.ZeroAllocVoidSend'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
cmake --build cmake-build-sic-tsan --target rune_invoker_tests -j8 && ./cmake-build-sic-tsan/rune_invoker_tests --gtest_filter='ProducerSend.*'
```
Expected: PASS; `ZeroAllocVoidSend` still 0 allocations; full suite matches the baseline; TSan clean.

- [ ] **Step 5: Commit**

```bash
git add include/rune/client/detail/M2ORingBuffer.h include/rune/client/producer/Producer.h src/producer/Producer.cpp \
        include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp include/rune/client/TypedTopic.h \
        tests/producer/ProducerSendTest.cpp tests/producer/ProducerTest.cpp CMakeLists.txt
git commit -m "fix(producer): send never drops — SendResult + back-off on the void path

Producer::trySend returns SendResult (Ok / BackPressured / ValueTooLarge /
InvalidTopic / ...); the void send backs off and retries until accepted, and
throws instead of hanging when the ring consumer's heartbeat (Aeron
ManyToOneRingBuffer::consumerHeartbeatTime) stops for agentLivenessTimeoutMs.
Oversized values and invalid topics are reported, not silently dropped.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 14: Direct (invoker) ingress — `send()` appends on the owner thread, batches end at the pass

Spec §4.1: "Invoker mode: the app thread is the shard — `send()` then the shard duty cycle in the app's own loop"; §4.2: "records sent during one pass accumulate; at the end of the pass each partition's pending records go out as one frame, subject to its in-flight limit". In invoker mode the producer ring is a pointless hop (CAS + copy to a ring the same thread drains): the producer appends straight into the partition builder. The `drainRing` handler body becomes the shared `appendRecord_`. Review Focus 2 lives here: a void send back-pressured inside the duty cycle (ack callback) must throw, not re-enter or hang.

**Files:**
- Modify: `include/rune/client/producer/ProducerImpl.h` (`IngressMode`, `DirectIngress`, `appendRecord_`, `lastNowNs_`)
- Modify: `src/producer/ProducerImpl.cpp` — `drainRing` (`:407-593`, body → `appendRecord_`), `submitFlush` (`:246-250`), `submitTier2Batch`, `trySubmit`, `sendBlocking`, `flushBatches` (records `lastNowNs_`)
- Modify: `include/rune/client/Client.h` (private `pumpForProducer_`), `src/Client.cpp` `producer()`
- Create: `tests/producer/DirectIngressTest.cpp`; `CMakeLists.txt` (`rune_invoker_tests`)

**Interfaces:**
- Consumes: `Client::dutyCycle`, `inDutyCycle_`, `ownerThread_`, `now()` (Task 12); `SendResult`, `sendBlocking` (Task 13); `InvokerGateway` (Task 11); `AllocCounter` (Task 9).
- Produces:
  - `enum class ProducerImpl::IngressMode : uint8_t { Ring, Direct };`
  - `struct ProducerImpl::DirectIngress { std::thread::id owner; bool (*pump)(void* ctx) noexcept = nullptr; void* pumpCtx = nullptr; };`
  - `void ProducerImpl::enableDirectIngress(const DirectIngress&) noexcept;` `[[nodiscard]] IngressMode ProducerImpl::ingressMode() const noexcept;`
  - private `detail::ControlledAction ProducerImpl::appendRecord_(int32_t topicId, int32_t partition, const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* valBytes, uint32_t valueLen, uint8_t flags, uint64_t ackHandle, int64_t nowNs);`
  - `Client::producer()` in `RunnerMode::Invoker` returns a Direct-ingress producer bound to the owner thread.

- [ ] **Step 1: Write the failing tests**

Create `tests/producer/DirectIngressTest.cpp`:

```cpp
// tests/producer/DirectIngressTest.cpp — invoker-mode (Direct) producer ingress.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using rc::producer::CountingSink;
using rc::producer::ProducerImpl;
using namespace std::chrono_literals;

namespace {
std::atomic<int> g_ctr{0};
const std::vector<uint8_t> kVal(16, 0x33);

rc::ProducerConfig voidCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}
} // namespace

TEST(DirectIngress, SendAppendsWithoutTheRing) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr, /*ringCap=*/4 * 1024);  // the ring could hold ~60 records
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (uint64_t i = 0; i < 10'000; ++i) {
        ASSERT_EQ(impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0), rc::SendResult::Ok);
        if (i % 64 == 63) impl.flushBatches(1, 42, sink);  // end of an app "pass"
    }
    impl.flushBatches(1, 42, sink);
    EXPECT_EQ(impl.recordsSent(), 10'000u);
    EXPECT_GT(frames, 0u);
}

TEST(DirectIngress, OffOwnerThreadIsRejected) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    rc::SendResult r = rc::SendResult::Ok;
    std::thread other([&] { r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0); });
    other.join();
    EXPECT_EQ(r, rc::SendResult::WrongThread);
}

TEST(DirectIngress, TrySendAllocatesNothing) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (uint64_t i = 0; i < 256; ++i) {  // warm-up: partitions, builders, scratch
        (void)impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0);
        impl.flushBatches(1, 42, sink);
    }
    alloc_counter::start();
    for (uint64_t i = 0; i < 1000; ++i) {
        (void)impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0);
        impl.flushBatches(1, 42, sink);
    }
    EXPECT_EQ(alloc_counter::stop(), 0L);
}

// Review Focus 2: an ack callback runs INSIDE the invoker duty cycle.  A void
// send there that hits back-pressure cannot wait for a drain that only this
// thread can perform, and must not re-enter the duty cycle: it throws.
TEST(DirectIngress, VoidSendInsideDutyCycleCallbackThrowsRegression) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc";
    c.gatewayStreamId = 3740;
    c.egressChannel = "aeron:ipc";
    c.egressStreamId = 3741;
    c.dataDir = "/tmp/direct_ingress_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 2;
    c.topics.push_back(ts);
    rc::Client client(c);
    test_support::InvokerGateway gw(c.dataDir + "/aeron", "aeron:ipc", 3740, "aeron:ipc", 3741);
    auto drive = [&](auto&& pred) {
        for (int i = 0; i < 200'000; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    auto cf = client.connect();
    ASSERT_TRUE(drive([&] { return cf.wait_for(0s) == std::future_status::ready; }));

    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    pcfg.maxInFlight = 1;
    pcfg.batchBytes = 256;
    auto p = client.producer(pcfg);
    const auto topic = client.topic("t");
    const rc::Key k0 = keyFor(0, 2), k1 = keyFor(1, 2);

    gw.holdPartitions = {1};  // partition 1's single in-flight batch is never acked
    ASSERT_EQ(p.trySend(topic, k1, kVal), rc::SendResult::Ok);
    ASSERT_TRUE(drive([&] { return gw.held.size() == 1; }));
    rc::SendResult r = rc::SendResult::Ok;
    for (int i = 0; i < 1000 && r == rc::SendResult::Ok; ++i) r = p.trySend(topic, k1, kVal);
    ASSERT_EQ(r, rc::SendResult::BackPressured) << "partition 1: window full + builder at batchBytes";

    bool threw = false;
    p.send(topic, k0, kVal, [&](int64_t) {
        try {
            p.send(topic, k1, kVal);  // void, back-pressured, inside the duty cycle
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    ASSERT_TRUE(drive([&] { return threw; })) << "must throw, not hang or re-enter doWork()";
}
```

Add `tests/producer/DirectIngressTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'enableDirectIngress' in 'rune::client::producer::ProducerImpl'`.

- [ ] **Step 3: Implement Direct ingress**

`ProducerImpl.h` (`#include <thread>`), public:

```cpp
    /// Ring (default): app threads hand records to the duty-cycle thread through
    /// the MPSC ring.  Direct (invoker mode, spec §4.1): the owner thread IS the
    /// duty-cycle thread, so send() appends straight into the partition builder —
    /// no ring, no CAS, no second copy.
    enum class IngressMode : uint8_t { Ring, Direct };
    struct DirectIngress {
        std::thread::id owner;                       ///< the only thread allowed to send
        bool (*pump)(void* ctx) noexcept = nullptr;  ///< one owner duty cycle; false if re-entered
        void* pumpCtx = nullptr;
    };
    void enableDirectIngress(const DirectIngress& d) noexcept {
        mode_   = IngressMode::Direct;
        direct_ = d;
    }
    [[nodiscard]] IngressMode ingressMode() const noexcept { return mode_; }
```

private:

```cpp
    IngressMode   mode_ = IngressMode::Ring;
    DirectIngress direct_{};
    int64_t       lastNowNs_ = 0;  // duty-cycle clock of the last drain/flush (Direct appends use it)
    /// One record into its partition builder (shared by the ring drain and Direct
    /// sends).  ABORT = back-pressure (builder full and window full, or frame cap).
    rune::client::detail::ControlledAction appendRecord_(int32_t topicId, int32_t partition,
        const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* valBytes, uint32_t valueLen,
        uint8_t flags, uint64_t ackHandle, int64_t nowNs);
```

`ProducerImpl.cpp`:
- `drainRing`: after `ring_.consumerHeartbeatTime(nowNs);` add `lastNowNs_ = nowNs;`. In the handler, the SEND_RECORD branch keeps its decode (`topicId`, `partition`, `keyLen`, `flags`, `ackHandle`, `valueLen`, FR mark, size check) and then `return appendRecord_(topicId, partition, p + kRecKeyBytesOff, keyLen, p + kRecValBytesOff, valueLen, flags, ackHandle, nowNs);`. Everything from `PartitionState& ps = getOrCreatePartition(topicId, partition);` to the handler's final `return ControlledAction::CONTINUE;` moves verbatim into:

```cpp
rune::client::detail::ControlledAction ProducerImpl::appendRecord_(
    int32_t topicId, int32_t partition, const uint8_t* keyBytes, uint8_t keyLen,
    const uint8_t* valBytes, uint32_t valueLen, uint8_t flags, uint64_t ackHandle, int64_t nowNs)
{
    using rune::client::detail::ControlledAction;
    PartitionState& ps = getOrCreatePartition(topicId, partition);
    // ... the moved body, unchanged, using keyBytes / valBytes parameters ...
    return ControlledAction::CONTINUE;
}
```

(delete the two local `keyBytes`/`valBytes` declarations that the body had at `:526-527` — they are now parameters.)
- `flushBatches`: first statement `lastNowNs_ = nowNs;`.
- `trySubmit`, before the ring path:

```cpp
    if (mode_ == IngressMode::Direct) {
        if (std::this_thread::get_id() != direct_.owner) return SendResult::WrongThread;
        const auto kb = key.bytes();
        const auto a = appendRecord_(topicId, partition, kb.data(), static_cast<uint8_t>(kb.size()),
                                     value.data(), static_cast<uint32_t>(value.size()),
                                     ackHandle != 0 ? kFlagWantAck : uint8_t{0},
                                     static_cast<uint64_t>(ackHandle), lastNowNs_);
        return a == rune::client::detail::ControlledAction::ABORT ? SendResult::BackPressured
                                                                  : SendResult::Ok;
    }
```

(the `ValueTooLarge` check stays first.)
- `submitFlush`: `if (mode_ == IngressMode::Direct) { for (auto& up : partitions_) up->flushRequested = true; return; }` at the top.
- `submitTier2Batch`: at the top

```cpp
    if (mode_ == IngressMode::Direct) {
        if (std::this_thread::get_id() != direct_.owner) return false;
        PartitionState& ps = getOrCreatePartition(topicId, partition);
        if (ps.builder && ps.builder->recordCount() > 0) ps.flushRequested = true;
        recordsSent_ += static_cast<uint64_t>(recordCount);
        Tier2Entry e;
        e.topicId = topicId;
        e.partition = partition;
        e.ackHandle = static_cast<uint64_t>(ackHandle);
        e.recordCount = recordCount;
        e.batchBytes = std::move(batchBytes);
        ps.pendingTier2Entries.push_back(std::move(e));
        return true;
    }
```

- `sendBlocking`, just before `idle.idle(0);`:

```cpp
        if (mode_ == IngressMode::Direct) {
            // Only this thread can drain: run the owner's duty cycle.  Inside a
            // duty cycle (e.g. an ack callback) that is impossible — throw.
            if (direct_.pump == nullptr || !direct_.pump(direct_.pumpCtx)) {
                throw std::logic_error(
                    "Producer::send: back-pressured inside the invoker duty cycle "
                    "(e.g. from an ack callback); use trySend() there");
            }
            r = trySubmit(topicId, partition, key, value, 0);
            continue;
        }
```

`Client.h` private: `static bool pumpForProducer_(void* self) noexcept;`. `Client.cpp`:

```cpp
bool Client::pumpForProducer_(void* self) noexcept {
    auto* c = static_cast<Client*>(self);
    if (c->inDutyCycle_) return false;
    try {
        (void)c->dutyCycle(c->now());
    } catch (...) {
        return false;
    }
    return true;
}
```

and in `producer()` right after `auto impl = std::make_shared<producer::ProducerImpl>(cfg, nextCorrId_);`:

```cpp
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        // Invoker: this thread is the duty-cycle thread — append directly.
        impl->enableDirectIngress({ownerThread_, &Client::pumpForProducer_, this});
    }
```

- [ ] **Step 4: Run to verify pass, then everything**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='DirectIngress.*:InvokerClient.*:ProducerSend.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: PASS; full suite matches the baseline (Ring mode — every existing test — runs the moved `appendRecord_` body through `drainRing`).

- [ ] **Step 5: Commit**

```bash
git add include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp include/rune/client/Client.h \
        src/Client.cpp tests/producer/DirectIngressTest.cpp CMakeLists.txt
git commit -m "feat(producer): direct ingress in invoker mode — send appends on the owner thread

In RunnerMode::Invoker the producer appends straight into the partition
builder (no ring hop); records of one pass leave as one frame per partition
at the end of the duty cycle.  Off-owner-thread sends report WrongThread; a
back-pressured void send pumps the owner's duty cycle, and throws when it is
already inside one (ack callback) instead of re-entering or hanging.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 15: `ClientShard` / `ShardGroup` — deterministic partition → shard map, per-shard driver

Spec §4.1: "`ClientShard`: one thread owning a subset of partitions, with its own embedded driver in invoker mode (tmpfs dir, explicit MTU, default 8 KB, configurable), its own Aeron client with conductor invoker, its partitions' batch builders, publications to whichever worker endpoints they map to, its ack subscription, and its pending-ack table … Shard count = client cores given (config, default 1) … Deterministic partition → shard mapping … Agent mode (existing default, kept): a background thread per shard … any app thread may `send()` to any partition (routed to the owning shard)." A shard is one `Client` (its own dataDir → own driver; own egress port → own ack subscription; own sessions) plus its producer. Ownership is enforced: two shards writing one partition would collide on the shared idempotent producer id (`ProducerImpl.h:492-499`).

**F8c API (2026-10-02, spec `2026-10-01-f8c-no-futures-design.md` step C9 — this text supersedes the pre-F8 draft):** the client has no futures, promises, `std::async`, `shared_ptr` handles or atomics. `ClientShard::connect(CompletionTarget, Token)` returns an `OpResult` and completes exactly once into a `Completions` (polled on the issuing thread) or a `CompletionHandler`; `connectAndWait(timeout)` is the blocking sugar (Invoker owner: pumps `doWork()`; Thread mode: idle-strategy poll). Invoker batch acks arrive as `OpKind::BatchAck` completions in a shard-owned `Completions` (`Producer::batchAcksTo`, decoded with `batchAckOf`) — `collectBatchAcks`/`drainBatchAcks` and `Producer::implForInternalUse` no longer exist. Handles are move-only. Tests use no atomics and never sleep. The only real-time waits in tests are the two stalled-owner tests (Task 6 `StalledOwnerSurfacesClosedInsteadOfExitingRegression`, Task 15 `StalledOwnerTurnsSendsIntoClosed`): Aeron's conductor clock cannot be injected (`Aeron.cpp:43` hard-wires `currentTimeMillis`), so each yields on `aeron::currentTimeMillis()` until `ClientConductor.h:436`'s own condition (`nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs`) holds — the exact minimal stall (~200 ms), no tuned multiple. (Task 18's 500 ms `IDLE_CHECK` sampling window is a measurement interval, not synchronization.)

Resolved ambiguity: spec §4.1 says agent mode hands records "through a per-shard single-producer ring" but also "any app thread may send() to any partition" — several app threads can target one shard, so the per-shard ring stays the existing ManyToOne ring (§1a.2: "ManyToOne only where genuinely multi-producer"). Invoker mode has no ring at all (Task 14).

**Files:**
- Create: `include/rune/client/shard/ShardMap.h`, `include/rune/client/shard/ClientShard.h`, `src/shard/ClientShard.cpp`
- Create: `tests/shard/ShardMapTest.cpp` (pure), `tests/shard/ClientShardTest.cpp`
- Modify: `CMakeLists.txt` (`rune_client_api` sources; `rune_pure_tests`, `rune_invoker_tests`)

**Interfaces:**
- Consumes: `RunnerMode`, `runnerCore`, `aeronClientLivenessTimeoutNs`, `Client::doWork`, `Client::transportClosed` (Task 12); `SendResult` (Task 13); Direct ingress (Task 14); `BatchAck` (Task 10); `InvokerGateway` (Task 11); F8c: `Token`, `OpResult`, `OpStatus`, `Completion`, `Completions`, `CompletionTarget`, `Client::connect(target, token)`, `Client::connectAndWait`, `Producer::batchAcksTo(Completions&)`, `batchAckOf(const Completion&)`.
- Produces:
  - `[[nodiscard]] constexpr uint32_t rune::client::shard::shardFor(int32_t topicId, int32_t partition, uint32_t shardCount) noexcept;` = `(uint32(topicId) + uint32(partition)) % shardCount` (0 when `shardCount <= 1`)
  - `[[nodiscard]] std::string rune::client::shard::channelWithPortOffset(const std::string& channel, int32_t offset);`
  - `enum class ShardMode : uint8_t { Agent, Invoker };`
  - `struct ShardGroupConfig { ClientConfig base; uint32_t shardCount = 1; ShardMode mode = ShardMode::Agent; int32_t firstCore = -1; ProducerConfig producer = {}; int64_t invokerLingerUs = 0; };`
  - `[[nodiscard]] ClientConfig shardClientConfig(const ShardGroupConfig&, uint32_t index);` `[[nodiscard]] ProducerConfig shardProducerConfig(const ShardGroupConfig&);`
  - `class ClientShard { ClientShard(const ShardGroupConfig&, uint32_t index); [[nodiscard]] OpResult connect(CompletionTarget target, Token token); void connectAndWait(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout); SendResult trySend(TopicHandle, const Key&, std::span<const uint8_t>) noexcept; void send(...); int doWork(); template<class H> int pollBatchAcks(H&&) /* Invoker: h(const BatchAck&) per polled batch ack */; bool owns(int32_t, int32_t) const noexcept; bool closed() const noexcept; uint32_t index() const noexcept; TopicHandle topic(const std::string&) const noexcept; Client& client() noexcept; Producer& producer() noexcept; };`
  - `class ShardGroup { explicit ShardGroup(ShardGroupConfig); void connectAll(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout); SendResult trySend(...) noexcept; void send(...); ClientShard& shard(uint32_t); ClientShard& owner(int32_t, int32_t) noexcept; uint32_t size() const noexcept; };` (Agent mode only)

- [ ] **Step 1: Write the failing tests**

Create `tests/shard/ShardMapTest.cpp`:

```cpp
// tests/shard/ShardMapTest.cpp — deterministic partition->shard map + egress port offsets.
#include <rune/client/shard/ShardMap.h>

#include <gtest/gtest.h>

#include <set>
#include <stdexcept>

using rune::client::shard::channelWithPortOffset;
using rune::client::shard::shardFor;

TEST(ShardMap, SingleShardOwnsEverything) {
    for (int32_t t = 0; t < 4; ++t)
        for (int32_t p = 0; p < 64; ++p) EXPECT_EQ(shardFor(t, p, 1), 0u);
    EXPECT_EQ(shardFor(3, 5, 0), 0u);
}

TEST(ShardMap, DeterministicAndInRange) {
    for (uint32_t n = 1; n <= 8; ++n)
        for (int32_t t = 0; t < 4; ++t)
            for (int32_t p = 0; p < 64; ++p) {
                const uint32_t s = shardFor(t, p, n);
                EXPECT_LT(s, n);
                EXPECT_EQ(s, shardFor(t, p, n));
            }
}

TEST(ShardMap, EveryShardOwnsAPartitionOfAnEightPartitionTopic) {
    for (uint32_t n = 1; n <= 8; ++n) {
        std::set<uint32_t> owners;
        for (int32_t p = 0; p < 8; ++p) owners.insert(shardFor(0, p, n));
        EXPECT_EQ(owners.size(), n);
    }
}

TEST(ShardMap, PortOffsetRewritesOnlyTheEndpointPort) {
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=127.0.0.1:40400", 2),
              "aeron:udp?endpoint=127.0.0.1:40402");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=host:40400|mtu=8192", 1),
              "aeron:udp?endpoint=host:40401|mtu=8192");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?mtu=8192|endpoint=h:9", 3),
              "aeron:udp?mtu=8192|endpoint=h:12");
    EXPECT_EQ(channelWithPortOffset("aeron:ipc", 5), "aeron:ipc");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=h:40400", 0), "aeron:udp?endpoint=h:40400");
}

TEST(ShardMap, PortOffsetRejectsMalformedOrOverflowingPorts) {
    EXPECT_THROW(channelWithPortOffset("aeron:udp?endpoint=host", 1), std::invalid_argument);
    EXPECT_THROW(channelWithPortOffset("aeron:udp?endpoint=host:abc", 1), std::invalid_argument);
    EXPECT_THROW(channelWithPortOffset("aeron:udp?endpoint=host:65535", 1), std::invalid_argument);
}
```

Create `tests/shard/ClientShardTest.cpp`:

```cpp
// tests/shard/ClientShardTest.cpp — ClientShard (invoker: single-threaded against
// the InvokerGateway) and ShardGroup (agent mode: routing to the owning shard).
#include "../support/InvokerGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
const std::vector<uint8_t> kVal(32, 0x21);

rs::ShardGroupConfig group(int32_t in, int32_t eg, uint32_t count, rs::ShardMode mode) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = in;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = eg;
    g.base.dataDir = "/tmp/shard_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 8;
    g.base.topics.push_back(ts);
    g.shardCount = count;
    g.mode = mode;
    g.producer.acks = rc::Acks::Leader;
    g.producer.lingerUs = 0;
    return g;
}

rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}

struct ShardRig {
    rs::ClientShard              shard;  // owns the driver: constructed first, destroyed last
    test_support::InvokerGateway gw;
    ShardRig(const rs::ShardGroupConfig& g, uint32_t i)
        : shard(g, i),
          gw(rs::shardClientConfig(g, i).dataDir + "/aeron", g.base.gatewayChannel,
             g.base.gatewayStreamId, rs::shardClientConfig(g, i).egressChannel, g.base.egressStreamId) {}
    template <class P>
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            shard.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    // F8c: connect(target, Token) into a Completions owned by this (owner)
    // thread; the completion is polled while driving shard + gateway.
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(shard.client());
        ASSERT_EQ(shard.connect(cq, 1), rc::OpResult::Accepted);
        bool done = false;
        rc::OpStatus st = rc::OpStatus::Closed;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 1) { done = true; st = c.status; }
            });
            return done;
        }));
        ASSERT_EQ(st, rc::OpStatus::Ok);
    }
};
} // namespace

TEST(ClientShardConfig, DerivesDirEgressPortRunnerModeCoreAndLinger) {
    rs::ShardGroupConfig g;
    g.base.dataDir = "/tmp/x";
    g.base.egressChannel = "aeron:udp?endpoint=127.0.0.1:40400";
    g.shardCount = 4;
    g.firstCore = 2;
    const rc::ClientConfig c1 = rs::shardClientConfig(g, 1);
    EXPECT_EQ(c1.dataDir, "/tmp/x/shard-1");
    EXPECT_EQ(c1.egressChannel, "aeron:udp?endpoint=127.0.0.1:40401");
    EXPECT_EQ(c1.runnerMode, rc::RunnerMode::Thread);
    EXPECT_EQ(c1.runnerCore, 3);
    g.mode = rs::ShardMode::Invoker;
    const rc::ClientConfig c3 = rs::shardClientConfig(g, 3);
    EXPECT_EQ(c3.runnerMode, rc::RunnerMode::Invoker);
    EXPECT_EQ(c3.runnerCore, -1);
    EXPECT_THROW((void)rs::shardClientConfig(g, 4), std::invalid_argument);
    g.producer.lingerUs = 5000;
    EXPECT_EQ(rs::shardProducerConfig(g).lingerUs, 0) << "spec §4.2: invoker batches at end of pass";
    g.mode = rs::ShardMode::Agent;
    EXPECT_EQ(rs::shardProducerConfig(g).lingerUs, 5000);
}

TEST(ClientShard, InvokerShardProducesAndAcksWithNoThreads) {
    ShardRig r(group(3800, 3801, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto topic = r.shard.topic("t");
    for (uint64_t i = 0; i < 100; ++i) ASSERT_EQ(r.shard.trySend(topic, rc::Key(i), kVal), rc::SendResult::Ok);
    int64_t acked = 0;
    int errors = 0;
    bool contiguous = true;
    std::map<int32_t, int32_t> nextSeq;
    ASSERT_TRUE(r.drive([&] {
        r.shard.pollBatchAcks([&](const rc::BatchAck& a) {
            acked += a.recordCount;
            if (a.errorCode != 0) ++errors;
            if (a.baseSequence != nextSeq[a.partition]) contiguous = false;
            nextSeq[a.partition] = a.baseSequence + a.recordCount;
        });
        return acked == 100;
    }));
    EXPECT_EQ(errors, 0);
    EXPECT_TRUE(contiguous);
}

TEST(ClientShard, RecordsOfOnePassLeaveAsOneFramePerPartition) {
    ShardRig r(group(3802, 3803, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto topic = r.shard.topic("t");
    const rc::Key k = keyFor(3, 8);
    for (int i = 0; i < 10; ++i) ASSERT_EQ(r.shard.trySend(topic, k, kVal), rc::SendResult::Ok);
    const std::size_t before = r.gw.produced.size();
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() > before; }));
    EXPECT_EQ(r.gw.produced.back().records, 10) << "one pass = one frame per partition (no linger timer)";
}

TEST(ClientShard, RejectsPartitionsOwnedByAnotherShard) {
    ShardRig r(group(3804, 3805, 2, rs::ShardMode::Invoker), 0);
    const auto topic = r.shard.topic("t");
    int32_t foreign = -1;
    for (int32_t p = 0; p < 8 && foreign < 0; ++p) {
        if (!r.shard.owns(1, p)) foreign = p;
    }
    ASSERT_GE(foreign, 0);
    EXPECT_EQ(r.shard.trySend(topic, keyFor(foreign, 8), kVal), rc::SendResult::WrongShard);
    EXPECT_THROW(r.shard.send(topic, keyFor(foreign, 8), kVal), std::logic_error);
}

// Review Focus 1: an owner stall past the Aeron client liveness timeout turns
// sends into Closed — no ::exit, no hang.  Aeron's conductor clock cannot be
// injected (Aeron.cpp:43 hard-wires currentTimeMillis), so the test yields on
// that clock until ClientConductor.h:436's condition holds — no tuned sleep.
TEST(ClientShard, StalledOwnerTurnsSendsIntoClosed) {
    auto g = group(3806, 3807, 1, rs::ShardMode::Invoker);
    g.base.aeronClientLivenessTimeoutNs = 200'000'000;
    ShardRig r(g, 0);
    r.connect();
    // Stall exactly past Aeron's own condition (same clock, same comparison as
    // ClientConductor.h:436: nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs);
    // t0 is taken after connect()'s last doWork.  No tuned sleep.
    const long long livenessMs = g.base.aeronClientLivenessTimeoutNs / 1'000'000;
    const long long t0 = aeron::currentTimeMillis();
    while (aeron::currentTimeMillis() <= t0 + livenessMs) std::this_thread::yield();
    ASSERT_TRUE(r.drive([&] { return r.shard.closed(); }, 10'000));
    EXPECT_EQ(r.shard.trySend(r.shard.topic("t"), rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
}

TEST(ShardGroup, AgentModeRoutesEachSendToTheOwningShard) {
    auto g = group(3810, 3811, 2, rs::ShardMode::Agent);
    g.producer.acks = rc::Acks::None;
    rs::ShardGroup grp(g);
    test_support::InvokerGateway gw0(rs::shardClientConfig(g, 0).dataDir + "/aeron", "aeron:ipc", 3810,
                                     "aeron:ipc", 3811);
    test_support::InvokerGateway gw1(rs::shardClientConfig(g, 1).dataDir + "/aeron", "aeron:ipc", 3810,
                                     "aeron:ipc", 3811);
    // Agent mode: each shard's runner + driver threads work on their own; the
    // test thread pumps only the gateways, bounded by iterations (no sleep:
    // yield between rounds).
    auto pumpUntil = [&](auto&& pred) {
        for (int i = 0; i < 2'000'000; ++i) {
            gw0.pump();
            gw1.pump();
            if (pred()) return true;
            std::this_thread::yield();
        }
        return false;
    };
    ASSERT_TRUE(pumpUntil([&] { return gw0.ready() && gw1.ready(); }));
    // F8c: one Completions per shard, owned by this thread; each shard's runner
    // completes its connect token into it.
    rc::Completions cq0(grp.shard(0).client());
    rc::Completions cq1(grp.shard(1).client());
    ASSERT_EQ(grp.shard(0).connect(cq0, 10), rc::OpResult::Accepted);
    ASSERT_EQ(grp.shard(1).connect(cq1, 11), rc::OpResult::Accepted);
    int connected = 0, failed = 0;
    auto onConnect = [&](const rc::Completion& c, std::span<const uint8_t>) {
        (c.status == rc::OpStatus::Ok ? connected : failed) += 1;
    };
    ASSERT_TRUE(pumpUntil([&] {
        cq0.poll(onConnect);
        cq1.poll(onConnect);
        return connected + failed == 2;
    }));
    ASSERT_EQ(failed, 0);
    const auto topic = grp.shard(0).topic("t");
    for (int32_t p = 0; p < 8; ++p) grp.send(topic, keyFor(p, 8), kVal);
    ASSERT_TRUE(pumpUntil([&] { return gw0.produced.size() + gw1.produced.size() == 8; }));
    for (const auto& pr : gw0.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 0u);
    for (const auto& pr : gw1.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 1u);
}
```

Add `tests/shard/ShardMapTest.cpp` to `rune_pure_tests` and `tests/shard/ClientShardTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_pure_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'rune/client/shard/ShardMap.h' file not found`.

- [ ] **Step 3: Implement `ShardMap.h`**

```cpp
// include/rune/client/shard/ShardMap.h — pure helpers for client shards (spec §4.1).
#pragma once

#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace rune::client::shard {

/// Deterministic partition -> shard map: each topic's partitions round-robin
/// over the shards, offset by topicId so single-partition topics spread too.
[[nodiscard]] constexpr uint32_t shardFor(int32_t topicId, int32_t partition,
                                          uint32_t shardCount) noexcept {
    return shardCount <= 1 ? 0u
                           : (static_cast<uint32_t>(topicId) + static_cast<uint32_t>(partition)) %
                                 shardCount;
}

/// "aeron:udp?endpoint=h:40400|x=y" + 2 -> "aeron:udp?endpoint=h:40402|x=y".  A
/// channel without an endpoint (aeron:ipc) is returned unchanged; a missing,
/// non-numeric or overflowing port throws std::invalid_argument.
[[nodiscard]] inline std::string channelWithPortOffset(const std::string& channel, int32_t offset) {
    if (offset == 0) return channel;
    const std::size_t ep = channel.find("endpoint=");
    if (ep == std::string::npos) return channel;
    const std::size_t hostStart = ep + 9;
    std::size_t end = channel.find('|', hostStart);
    if (end == std::string::npos) end = channel.size();
    const std::size_t colon = (end > hostStart) ? channel.rfind(':', end - 1) : std::string::npos;
    if (colon == std::string::npos || colon < hostStart) {
        throw std::invalid_argument("channelWithPortOffset: no endpoint port in " + channel);
    }
    const std::string portStr = channel.substr(colon + 1, end - colon - 1);
    char* parsedEnd = nullptr;
    const long port = std::strtol(portStr.c_str(), &parsedEnd, 10);
    const long shifted = port + offset;
    if (portStr.empty() || parsedEnd == nullptr || *parsedEnd != '\0' || port <= 0 ||
        shifted <= 0 || shifted > 65535) {
        throw std::invalid_argument("channelWithPortOffset: bad port in " + channel);
    }
    return channel.substr(0, colon + 1) + std::to_string(shifted) + channel.substr(end);
}

} // namespace rune::client::shard
```

- [ ] **Step 4: Implement `ClientShard`**

No `Producer.h` change (F8c): the shard does not reach into `ProducerImpl`; Invoker batch acks go through `Producer::batchAcksTo(Completions&)` into a shard-owned `Completions`.

`include/rune/client/shard/ClientShard.h`:

```cpp
// include/rune/client/shard/ClientShard.h
//
// Client shards (spec §4.1).  A shard is one Client — its own embedded driver
// (own dataDir), own Aeron client, own sessions and ack (egress) subscription on
// its own egress port — plus its producer.  Partitions map deterministically to
// shards (shardFor); a shard only accepts its own partitions.
//
//   ShardMode::Agent   (default): background runner per shard (optionally pinned);
//                      ShardGroup::send from any app thread goes to the owner.
//   ShardMode::Invoker (opt-in):  no threads; each app thread constructs its own
//                      ClientShard(cfg, i) and calls trySend()...doWork() in its
//                      own loop.  Records of one pass leave as one frame per
//                      partition (no linger timer, spec §4.2).
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/shard/ShardMap.h>

#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace rune::client::shard {

enum class ShardMode : uint8_t { Agent, Invoker };

struct ShardGroupConfig {
    ClientConfig   base;                 ///< shard i: dataDir/shard-i, egress port + i
    uint32_t       shardCount = 1;       ///< "shard count = client cores given"
    ShardMode      mode       = ShardMode::Agent;
    int32_t        firstCore  = -1;      ///< Agent: runner of shard i pinned to firstCore + i
    ProducerConfig producer   = {};
    int64_t        invokerLingerUs = 0;  ///< Invoker: replaces producer.lingerUs (end-of-pass batching)
};

[[nodiscard]] ClientConfig   shardClientConfig(const ShardGroupConfig& g, uint32_t index);
[[nodiscard]] ProducerConfig shardProducerConfig(const ShardGroupConfig& g);

class ClientShard {
public:
    ClientShard(const ShardGroupConfig& g, uint32_t index);
    ClientShard(const ClientShard&)            = delete;
    ClientShard& operator=(const ClientShard&) = delete;

    /// F8c: exactly one Connect completion into `target` when Accepted (a
    /// Completions registered with client() and owned by the calling thread, or
    /// a CompletionHandler on the duty thread).  Invoker: owner thread only,
    /// then drive doWork() and poll.
    [[nodiscard]] OpResult connect(CompletionTarget target, Token token) {
        return client_->connect(target, token);
    }
    /// Blocking sugar: Invoker owner pumps doWork(); Agent polls under the idle
    /// strategy.  Throws std::runtime_error on any non-Ok outcome or timeout.
    void connectAndWait(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout) {
        client_->connectAndWait(timeout);
    }

    /// Invoker batch-ack queue: 8 MiB = ~174k accepted-but-unpolled records.
    static constexpr uint32_t kBatchAckQueueBytes = 8u << 20;

    /// Non-blocking; zero allocation when accepted.  Invoker: QueueFull when
    /// the batch-ack queue has no credit — pollBatchAcks(), then retry.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept {
        if (closed_) return SendResult::Closed;
        if (!topic.valid()) return SendResult::InvalidTopic;
        if (!owns(topic.topicId, key.partition(topic.partitionCount))) return SendResult::WrongShard;
        return producer_.trySend(topic, key, value);
    }

    /// Never drops (Producer::send semantics); WrongShard -> logic_error, Closed -> runtime_error,
    /// Invoker QueueFull -> runtime_error (only this thread can pollBatchAcks()).
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    /// Invoker mode: one shard duty cycle.  Returns the work count.
    int doWork();

    /// Invoker mode: h(const BatchAck&) for every batch ack completed since the
    /// last call (F8c: OpKind::BatchAck completions in the shard-owned queue).
    /// Agent mode: 0 (no queue).
    template <class H>
    int pollBatchAcks(H&& h) {
        if (!acks_) return 0;
        return acks_->poll([&](const Completion& c, std::span<const uint8_t>) { h(batchAckOf(c)); });
    }

    [[nodiscard]] bool owns(int32_t topicId, int32_t partition) const noexcept {
        return shardFor(topicId, partition, count_) == index_;
    }
    [[nodiscard]] bool        closed() const noexcept { return closed_; }
    [[nodiscard]] uint32_t    index() const noexcept { return index_; }
    [[nodiscard]] TopicHandle topic(const std::string& name) const noexcept { return client_->topic(name); }
    [[nodiscard]] Client&     client() noexcept { return *client_; }
    [[nodiscard]] Producer&   producer() noexcept { return producer_; }

private:
    uint32_t                   index_;
    uint32_t                   count_;
    ShardMode                  mode_;
    std::unique_ptr<Client>    client_;
    std::optional<Completions> acks_;      // Invoker: batch acks; owner thread.  Never
                                           // moved (the producer holds its address);
                                           // declared before producer_: outlives it
    Producer                   producer_;  // move-only handle (F8c §3)
    bool                       closed_ = false;  // owner thread (Invoker) only
};

class ShardGroup {
public:
    /// Agent mode only: in invoker mode each app thread builds its own ClientShard.
    explicit ShardGroup(ShardGroupConfig cfg);
    /// Issues every shard's connect (they proceed in parallel on the runners),
    /// then waits for each token; the broker side of each is bounded by
    /// base.connectTimeoutMs.  Throws std::runtime_error naming the first shard
    /// whose connect did not complete Ok within `timeout`.
    void connectAll(std::chrono::nanoseconds timeout = Client::kDefaultWaitTimeout);
    /// Any app thread: routed to the owning shard.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept;
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);
    [[nodiscard]] ClientShard& shard(uint32_t i) { return *shards_.at(i); }
    [[nodiscard]] ClientShard& owner(int32_t topicId, int32_t partition) noexcept {
        return *shards_[shardFor(topicId, partition, size())];
    }
    [[nodiscard]] uint32_t size() const noexcept { return static_cast<uint32_t>(shards_.size()); }

private:
    ShardGroupConfig                          cfg_;
    std::vector<std::unique_ptr<ClientShard>> shards_;
};

} // namespace rune::client::shard
```

`src/shard/ClientShard.cpp`:

```cpp
// src/shard/ClientShard.cpp
#include <rune/client/shard/ClientShard.h>

#include <stdexcept>

namespace rune::client::shard {

ClientConfig shardClientConfig(const ShardGroupConfig& g, uint32_t index) {
    if (g.shardCount == 0 || index >= g.shardCount) {
        throw std::invalid_argument("shardClientConfig: shard index " + std::to_string(index) +
                                    " out of range for shardCount " + std::to_string(g.shardCount));
    }
    ClientConfig c  = g.base;
    c.dataDir       = g.base.dataDir + "/shard-" + std::to_string(index);
    c.egressChannel = channelWithPortOffset(g.base.egressChannel, static_cast<int32_t>(index));
    c.runnerMode    = (g.mode == ShardMode::Invoker) ? RunnerMode::Invoker : RunnerMode::Thread;
    c.runnerCore    = (g.mode == ShardMode::Agent && g.firstCore >= 0)
                          ? g.firstCore + static_cast<int32_t>(index)
                          : -1;
    return c;
}

ProducerConfig shardProducerConfig(const ShardGroupConfig& g) {
    ProducerConfig p = g.producer;
    if (g.mode == ShardMode::Invoker) p.lingerUs = g.invokerLingerUs;
    return p;
}

ClientShard::ClientShard(const ShardGroupConfig& g, uint32_t index)
    : index_(index),
      count_(g.shardCount),
      mode_(g.mode),
      client_(std::make_unique<Client>(shardClientConfig(g, index))),
      producer_(client_->producer(shardProducerConfig(g))) {
    if (mode_ == ShardMode::Invoker) {
        // The constructing thread is the Invoker owner: it owns the queue too.
        // Credit (Producer::batchAcksTo): every send reserves 48 B until its
        // batch ack is polled; BatchAck records carry no payload.
        CompletionsConfig qc;
        qc.capacityBytes  = kBatchAckQueueBytes;
        qc.maxResultBytes = 0;
        qc.maxErrorText   = 0;
        acks_.emplace(*client_, qc);
        producer_.batchAcksTo(*acks_);  // setup phase: before the first send
    }
}

void ClientShard::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    switch (trySend(topic, key, value)) {
    case SendResult::Ok:
        return;
    case SendResult::WrongShard:
        throw std::logic_error("ClientShard::send: partition owned by another shard");
    case SendResult::Closed:
        throw std::runtime_error("ClientShard::send: shard transport closed");
    default:
        producer_.send(topic, key, value);  // backs off / pumps; throws on invalid input
    }
}

int ClientShard::doWork() {
    if (mode_ != ShardMode::Invoker) {
        throw std::logic_error("ClientShard::doWork(): invoker mode only");
    }
    const int work = client_->doWork();
    if (!closed_ && client_->transportClosed()) closed_ = true;
    return work;
}

ShardGroup::ShardGroup(ShardGroupConfig cfg) : cfg_(std::move(cfg)) {
    if (cfg_.mode != ShardMode::Agent) {
        throw std::invalid_argument(
            "ShardGroup: agent mode only; in invoker mode each app thread constructs its own "
            "ClientShard(cfg, i)");
    }
    if (cfg_.shardCount == 0) throw std::invalid_argument("ShardGroup: shardCount must be >= 1");
    shards_.reserve(cfg_.shardCount);
    for (uint32_t i = 0; i < cfg_.shardCount; ++i) {
        shards_.push_back(std::make_unique<ClientShard>(cfg_, i));
    }
}

void ShardGroup::connectAll(std::chrono::nanoseconds timeout) {
    // Agent mode: the runners make progress by themselves; this thread owns the
    // queues and only polls them (Completions::waitFor, no pump).
    std::vector<Completions> cqs;
    cqs.reserve(shards_.size());
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        cqs.emplace_back(shards_[i]->client());
        if (shards_[i]->connect(cqs.back(), i) != OpResult::Accepted) {
            throw std::runtime_error("ShardGroup::connectAll: shard " + std::to_string(i) +
                                     " rejected connect");
        }
    }
    for (uint32_t i = 0; i < shards_.size(); ++i) {
        OpStatus st = OpStatus::Closed;
        const auto w = cqs[i].waitFor(i, timeout,
                                      [&](const Completion& c, std::span<const uint8_t>) { st = c.status; });
        if (w != Completions::Wait::Done || st != OpStatus::Ok) {
            throw std::runtime_error("ShardGroup::connectAll: shard " + std::to_string(i) +
                                     " did not connect (status " + std::to_string(static_cast<int>(st)) + ")");
        }
    }
}

SendResult ShardGroup::trySend(TopicHandle topic, const Key& key,
                               std::span<const uint8_t> value) noexcept {
    if (!topic.valid()) return SendResult::InvalidTopic;
    return owner(topic.topicId, key.partition(topic.partitionCount)).trySend(topic, key, value);
}

void ShardGroup::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    if (!topic.valid()) throw std::invalid_argument("ShardGroup::send: invalid topic handle");
    owner(topic.topicId, key.partition(topic.partitionCount)).send(topic, key, value);
}

} // namespace rune::client::shard
```

`CMakeLists.txt`: add `src/shard/ClientShard.cpp` to `rune_client_api`.

- [ ] **Step 5: Run to verify pass, then everything**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_pure_tests --gtest_filter='ShardMap.*'
./cmake-build-sic/rune_invoker_tests --gtest_filter='ClientShard*:ShardGroup.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: PASS (the stall test takes ~0.6 s); full suite matches the baseline.

- [ ] **Step 6: Commit**

```bash
git add include/rune/client/shard/ShardMap.h include/rune/client/shard/ClientShard.h src/shard/ClientShard.cpp \
        include/rune/client/producer/Producer.h tests/shard/ShardMapTest.cpp tests/shard/ClientShardTest.cpp CMakeLists.txt
git commit -m "feat(client): ClientShard / ShardGroup with a deterministic partition->shard map

A shard is one Client (own embedded driver, Aeron client, sessions and ack
subscription on its own egress port) plus its producer, and accepts only the
partitions shardFor() assigns it (two shards on one partition would collide on
the shared idempotent producer id).  Agent mode: ShardGroup routes any app
thread's send to the owning shard (pinnable runners).  Invoker mode: each app
thread drives its own shard; linger defaults to 0 (end-of-pass batching).

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: Consume `MetadataResponse.ingressEndpoint` — produce straight to the owning worker

**F8c API (2026-10-02, C9):** the tests below issue `Client::connect(cq, token)` and acked sends `Producer::trySend(..., cq, token)` / `trySend(..., AckToken{t})` + `onAck(fn, ctx)`, and poll the `Completions` while driving — no futures, no `std::function` acks, no atomics.

Shared interface with the broker plan (FIXED): per-partition `ingressEndpoint` = "host:port" of the owning worker's ingress; EMPTY means "use the node endpoint". The session (Connect / session id / control) stays on the node endpoint; produce frames go to the worker on its `ingressStreamId`; worker ProduceResponses come from a different publication on the session egress stream (Tasks 9–11 match by batch id, never by publisher). When the endpoint moves (ownership move / leader change) the partition's in-flight batches are re-offered to the new worker at once — idempotent (same producer id + sequence), so a late ack from the old worker is ignored (Review Focus 5).

**Files:**
- Modify: `include/rune/client/codec/Codec.h:372-382` (`PartitionEntryView::ingressEndpoint`), `:437-448` (decode copy)
- Modify: `include/rune/client/metadata/MetadataCache.h:35-41` (`PartitionEntry::ingressEndpoint`)
- Modify: `src/Client.cpp:1805-1867` (`onMetadataResponse` routes + re-offer); `include/rune/client/Client.h:641-653` (test hook `testDataChannelFor`)
- Modify: `include/rune/client/producer/ProducerImpl.h`, `src/producer/ProducerImpl.cpp` (`resendPartitionViaRoute`); `include/rune/client/producer/ProducerHub.h`, `src/producer/ProducerHub.cpp`
- Modify: `tests/support/InvokerGateway.h` (`sendMetadata` encodes `ingressEndpoint`)
- Create: `tests/client/IngressEndpointRoutingTest.cpp`; `CMakeLists.txt` (`rune_invoker_tests`)

**Interfaces:**
- Consumes: rune-protocol `rune::protocol::client::PartitionEntry::ingressEndpoint` (from the broker plan); `ClientAgent::setRoute(t, p, session, data)` returning "changed", `dataChannelFor` (Task 11); `InFlightRing::forEachLive` (Task 9).
- Produces:
  - `std::string codec::PartitionEntryView::ingressEndpoint;` `std::string metadata::PartitionEntry::ingressEndpoint;`
  - `int ProducerImpl::resendPartitionViaRoute(int32_t topicId, int32_t partition, SendFn& sendFn);` `int ProducerHub::resendPartitionViaRoute(int32_t topicId, int32_t partition, std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn);`
  - `[[nodiscard]] std::string Client::testDataChannelFor(int32_t topicId, int32_t partition) const;`

- [ ] **Step 1: Verify the protocol field has landed (dependency gate)**

Run: `grep -n "ingressEndpoint" /Users/vradu/Projects/rune-protocol/include/rune/protocol/client/MetadataMessages.h`
Expected: lines showing `ingressEndpoint` as a member of `struct PartitionEntry` and in `MetadataResponse::encode`/`decode`/`encodedSize`. **If there is no output: STOP this task** — record "Task 16 blocked: rune-protocol MetadataResponse.ingressEndpoint (broker plan) not landed" in the task tracker, leave the checkboxes open, and continue with Task 17; resume here when the field exists. Do not add the field from this plan (the broker plan owns the schema change).

- [ ] **Step 2: Write the failing tests**

In `tests/support/InvokerGateway.h` `sendMetadata`, add after `p.ingressStreamId = ad.ingressStreamId;`:

```cpp
            p.ingressEndpoint = ad.ingressEndpoint;  // view/copy of a member: valid through encode()
```

Create `tests/client/IngressEndpointRoutingTest.cpp`:

```cpp
// tests/client/IngressEndpointRoutingTest.cpp — per-partition worker ingress
// endpoints (shard-mode brokers).  Invoker client + InvokerGateway on one thread;
// the duty-cycle clock is frozen so no retransmit timer can mask a routing bug.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>

#include <gtest/gtest.h>

#include <chrono>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
int64_t frozenNow() noexcept { return 1'000'000'000'000LL; }
const std::vector<uint8_t> kVal{7, 7, 7};
// The clock is frozen: linger must be 0 or a batch would wait forever.
rc::ProducerConfig noLinger() {
    rc::ProducerConfig p;
    p.lingerUs = 0;
    return p;
}

struct Rig {
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    Rig(int32_t in, int32_t eg)
        : c(makeCfg(in, eg)),
          client(c),
          gw(c.dataDir + "/aeron", "aeron:ipc", in, "aeron:ipc", eg) {}
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg) {
        rc::ClientConfig c;
        c.gatewayChannel = "aeron:ipc";
        c.gatewayStreamId = in;
        c.egressChannel = "aeron:ipc";
        c.egressStreamId = eg;
        c.dataDir = "/tmp/ingress_ep_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        c.nanoClock = &frozenNow;
        rc::TopicSpec ts;
        ts.name = "t";
        ts.topicId = 1;
        ts.partitionCount = 1;
        c.topics.push_back(ts);
        return c;
    }
    template <class P>
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    // F8c: completion tokens polled while driving (no future).  The frozen
    // clock never expires an op, so the drive cap is the only bound.
    rc::Completions cq{client};  // this (owner) thread's queue
    rc::Token       nextToken = 1;
    // Drive until `t` completes; returns its Completion (status Closed if the cap hit).
    rc::Completion awaitToken(rc::Token t) {
        rc::Completion got{};
        got.status = rc::OpStatus::Closed;
        bool done = false;
        (void)drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == t) { done = true; got = c; }
            });
            return done;
        });
        return got;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        const rc::Token t = nextToken++;
        ASSERT_EQ(client.connect(cq, t), rc::OpResult::Accepted);
        ASSERT_EQ(awaitToken(t).status, rc::OpStatus::Ok);
    }
    // One acked send into `cq`; returns its Completion.
    rc::Completion sendAndAwait(rc::Producer& p) {
        const rc::Token t = nextToken++;
        EXPECT_EQ(p.trySend(client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), cq, t),
                  rc::SendResult::Ok);
        return awaitToken(t);
    }
};

// onAck handler target (F8c: plain function pointer + context, duty thread).
struct AckSeen {
    int     fired  = 0;
    int64_t offset = -2;
    static void on(void* ctx, rc::Token, int64_t off, rc::OpStatus, uint16_t) noexcept {
        auto* s = static_cast<AckSeen*>(ctx);
        ++s->fired;
        s->offset = off;
    }
};
} // namespace

TEST(IngressEndpointRouting, ProduceGoesToTheAdvertisedWorkerEndpoint) {
    Rig r(3900, 3901);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24751", 5001);
    r.gw.partitions = {{1, 0, 0, 1, 5001, "127.0.0.1:24751"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    EXPECT_EQ(r.client.testDataChannelFor(1, 0), "aeron:udp?endpoint=127.0.0.1:24751");
    auto p = r.client.producer(noLinger());
    const rc::Completion c = r.sendAndAwait(p);
    ASSERT_EQ(c.status, rc::OpStatus::Ok);
    EXPECT_EQ(c.value, 100) << "acked by the worker's own publication";
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, w) << "produce must reach the worker, not the node";
    EXPECT_EQ(r.gw.produced[0].sessionId, r.gw.assignedSessionId) << "session id from the node Connect";
}

TEST(IngressEndpointRouting, EmptyEndpointKeepsNodeRouting) {
    Rig r(3902, 3903);
    r.gw.partitions = {{1, 0, 0, 1, 0, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, 0);
    EXPECT_TRUE(r.client.testDataChannelFor(1, 0).empty());
}

// Regression (Review Focus 5): a metadata refresh moves the partition to another
// worker while a batch is in flight at the old one.  The batch must be re-offered
// to the new worker immediately (the clock is frozen: no retransmit timer), and a
// late ack from the old worker must not complete the record twice.
TEST(IngressEndpointRouting, EndpointMoveReoffersInFlightBatchesRegression) {
    Rig r(3904, 3905);
    const int wA = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24761", 5002);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24762", 5003);
    r.gw.partitions = {{1, 0, 0, 1, 5002, "127.0.0.1:24761"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24761"; }));
    r.gw.holdPartitions = {0};
    auto p = r.client.producer(noLinger());
    AckSeen seen;
    p.onAck(&AckSeen::on, &seen);  // setup phase: before the first AckToken send
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal),
                        rc::AckToken{1}),
              rc::SendResult::Ok);
    const int& fired = seen.fired;
    const int64_t& offset = seen.offset;
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, wA);

    r.gw.partitions = {{1, 0, 0, 2, 5003, "127.0.0.1:24762"}};  // epoch 2: moved to B
    r.gw.holdPartitions.clear();
    r.gw.sendMetadata(r.gw.assignedSessionId, /*correlationId=*/0);  // unsolicited refresh
    ASSERT_TRUE(r.drive([&] { return fired == 1; })) << "in-flight batch not re-offered to the new worker";
    EXPECT_EQ(r.gw.produced.back().endpoint, wB);
    EXPECT_EQ(r.gw.produced.back().corrId, r.gw.held[0].corrId) << "same batch (idempotent re-offer)";

    r.gw.releaseHeld();  // the old worker acks late
    for (int i = 0; i < 20'000; ++i) {
        r.client.doWork();
        r.gw.pump();
    }
    EXPECT_EQ(fired, 1) << "exactly one completion per record";
    EXPECT_EQ(offset, 100);
}
```


Add `tests/client/IngressEndpointRoutingTest.cpp` to `rune_invoker_tests`.

- [ ] **Step 3: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_invoker_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `no member named 'testDataChannelFor' in 'rune::client::Client'`. (With only the hook added: `ProduceGoesToTheAdvertisedWorkerEndpoint` fails with `produced[0].endpoint == 0`, and the move test times out at `fired == 1`.)

- [ ] **Step 4: Implement**

`Codec.h` `PartitionEntryView` — add `std::string ingressEndpoint;  ///< "host:port" of the owning worker's ingress ("" = node)` (owned: the protocol's decoded struct is local to `decodeMetadataResponse`); in the decode loop add `pv.ingressEndpoint = std::string(p.ingressEndpoint);`.

`MetadataCache.h` `struct PartitionEntry` — add `std::string ingressEndpoint;  ///< "host:port" of the owning worker's ingress ("" = node)` as the last member.

`Client.h` public test hooks: `[[nodiscard]] std::string testDataChannelFor(int32_t topicId, int32_t partition) const { return agent_.dataChannelFor(topicId, partition); }`.

`ProducerImpl.h`/`.cpp`:

```cpp
    /// Re-offer every in-flight batch of one partition through its (new) route —
    /// used when metadata moves the partition's ingress endpoint.  Same frame, same
    /// batch id: the broker dedups by (producerId, sequence).  Returns # offered.
    int resendPartitionViaRoute(int32_t topicId, int32_t partition, SendFn& sendFn);
```

```cpp
int ProducerImpl::resendPartitionViaRoute(int32_t topicId, int32_t partition, SendFn& sendFn)
{
    const uint32_t idx = partitionIndex_.find(partKey(topicId, partition));
    if (idx == detail::FlatIndexMap::kNone) return 0;
    PartitionState& ps = *partitions_[idx];
    int resent = 0;
    ps.inflight.forEachLive([&](InFlightBatch& b) {
        b.ingressStreamId = ps.ingressStreamId;  // the new worker's stream
        if (sendFn(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(), b.wireFrame.size())) {
            ++resent;
        } else {
            // New worker publication still resolving / not yet connected: make the
            // batch due now so retransmitStalled() retries it every duty cycle
            // until it goes out (its delivery deadline still runs from firstSendNs).
            b.attemptDeadlineNs         = 0;
            earliestAttemptDeadlineNs_  = 0;
        }
    });
    return resent;
}
```

`ProducerHub.h` declaration + `.cpp`:

```cpp
int ProducerHub::resendPartitionViaRoute(
    int32_t topicId, int32_t partition,
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int resent = 0;
    for (auto& impl : producers_) resent += impl->resendPartitionViaRoute(topicId, partition, sendFn);
    return resent;
}
```

`Client.cpp` — anonymous-namespace helper:

```cpp
std::string toChannel(const std::string& endpoint) {
    return endpoint.rfind("aeron:", 0) == 0 ? endpoint : ("aeron:udp?endpoint=" + endpoint);
}
```

`onMetadataResponse`: in the conversion loop add `e.ingressEndpoint = p.ingressEndpoint;`; replace the route loop (`:1848-1867`) with:

```cpp
    const bool mnDbg = (std::getenv("RUNE_MN_DEBUG") != nullptr);  // cold: once per metadata response
    int mnRoutes = 0;
    std::vector<std::pair<int32_t, int32_t>> moved;  // shard-mode partitions whose route changed
    for (const auto& p : partitions) {
        const int32_t leaderNode = metadataCache_.leaderFor(p.topicId, p.partition);
        const std::string ep = (leaderNode == metadata::MetadataCache::kNoLeader)
                                   ? std::string() : metadataCache_.endpointFor(leaderNode);
        if (mnDbg) {
            std::fprintf(stderr, "[MNDBG route] topic=%d part=%d ingressStream=%d leaderNode=%d ep='%s' worker='%s'\n",
                         p.topicId, p.partition, p.ingressStreamId, leaderNode, ep.c_str(),
                         p.ingressEndpoint.c_str());
        }
        if (!p.ingressEndpoint.empty()) {
            // Shard-mode broker (spec §3.3): produce straight to the owning worker;
            // Connect / session id stay on the node endpoint.  Only entries the
            // cache accepted (not stale by epoch) move routes.
            if (metadataCache_.epochFor(p.topicId, p.partition) != p.leaderEpoch) continue;
            const std::string session = ep.empty() ? currentLeaderChannel() : toChannel(ep);
            if (agent_.setRoute(p.topicId, p.partition, session, toChannel(p.ingressEndpoint))) {
                moved.emplace_back(p.topicId, p.partition);
            }
            ++mnRoutes;
            continue;
        }
        // Legacy broker (empty ingressEndpoint): unchanged Task-1.3 routing.
        if (p.ingressStreamId == 0) continue;
        if (leaderNode == metadata::MetadataCache::kNoLeader) continue;
        if (ep.empty()) continue;
        agent_.setRoute(p.topicId, p.partition, toChannel(ep));
        ++mnRoutes;
    }
    if (mnDbg) std::fprintf(stderr, "[MNDBG route] built %d routes from %zu partitions\n",
                            mnRoutes, partitions.size());
    // A moved worker endpoint strands in-flight batches at the old worker: re-offer
    // them to the new one now (idempotent), instead of waiting for produceTimeoutMs.
    if (!moved.empty()) {
        std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn =
            [this](int32_t t, int32_t pp, int32_t sid, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.offerToPartition(t, pp, sid, fb, fl);
            };
        for (const auto& [t, pp] : moved) producerHub_.resendPartitionViaRoute(t, pp, sendFn);
    }
```

- [ ] **Step 5: Run to verify pass, then everything**

```bash
cmake --build cmake-build-sic -j8
./cmake-build-sic/rune_invoker_tests --gtest_filter='IngressEndpointRouting.*'
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 2>&1 | tail -3
```
Expected: PASS; full suite matches the baseline (`MetadataRouting.*`, `MultiNodeRouting.*`, `ClientPerWorkerRouting.*`, `DataConformance`/`Conformance` vectors for the new wire field come from rune-protocol).

- [ ] **Step 6: Commit**

```bash
git add include/rune/client/codec/Codec.h include/rune/client/metadata/MetadataCache.h include/rune/client/Client.h \
        src/Client.cpp include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp \
        include/rune/client/producer/ProducerHub.h src/producer/ProducerHub.cpp \
        tests/support/InvokerGateway.h tests/client/IngressEndpointRoutingTest.cpp CMakeLists.txt
git commit -m "feat(client): route produce to MetadataResponse.ingressEndpoint (shard brokers)

A non-empty per-partition ingressEndpoint routes Produce frames straight to
the owning worker on its ingressStreamId while Connect/session stay on the
node; empty keeps the legacy node routing.  Acks from the worker's own
publication match by batch id.  When the endpoint moves, the partition's
in-flight batches are re-offered to the new worker immediately; a late ack
from the old worker is ignored (exactly-once completion).

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 17: Benchmark building blocks — histogram, open-loop schedule, sequence-indexed timestamps, /proc readers

Spec §5: "Open-loop fixed rate (`--rate N`); each record's latency is measured from its scheduled send time (coordinated-omission-safe). Every record recorded in an HdrHistogram: p50/p99/p99.9/max; no sampling … Every run verifies the box is idle first and reports core layout + per-thread CPU split." Today the bench samples 1 ack in 256 into a `std::vector` (`rune_produce_throughput.cpp:184-186, 235-238`) and measures from the (closed-loop) send time. These header-only pieces are Aeron-free and unit-tested; Task 18 wires them.

**Files:**
- Create: `examples/bench/LogLinearHistogram.h`, `examples/bench/OpenLoopSchedule.h`, `examples/bench/SeqTimestampRing.h`, `examples/bench/ProcStat.h`
- Create: `tests/bench/BenchUtilTest.cpp`
- Modify: `CMakeLists.txt` (new `rune_bench_util_tests` after `rune_invoker_tests`)

**Interfaces:**
- Consumes: nothing.
- Produces (namespace `rune::bench`):
  - `class LogLinearHistogram { static constexpr int kBuckets = 4736; void record(int64_t valueNs) noexcept; uint64_t count() const noexcept; uint64_t max() const noexcept; uint64_t min() const noexcept; uint64_t valueAtQuantile(double q) const noexcept; void merge(const LogLinearHistogram&) noexcept; void reset() noexcept; static constexpr int index(uint64_t) noexcept; static constexpr uint64_t upperBound(int) noexcept; };`
  - `class OpenLoopSchedule { OpenLoopSchedule(int64_t startNs, uint64_t ratePerSec); int64_t scheduledNs(uint64_t i) const noexcept; uint64_t dueCount(int64_t nowNs) const noexcept; };`
  - `class SeqTimestampRing { explicit SeqTimestampRing(uint32_t minCapacity); bool full() const noexcept; uint64_t nextSeq() const noexcept; void push(int64_t ts) noexcept; int64_t take(uint64_t seq) noexcept; uint64_t outstanding() const noexcept; };`
  - `struct CpuTimes { uint64_t busy; uint64_t total; };` `std::optional<std::pair<int, CpuTimes>> parseProcStatCpuLine(std::string_view);` `std::optional<std::pair<std::string, uint64_t>> parseTaskStat(std::string_view);` `std::vector<CpuTimes> readPerCpuTimes();` `std::vector<double> busyFractions(const std::vector<CpuTimes>&, const std::vector<CpuTimes>&);` `double readThreadSelfCpuSeconds();` `std::string cpuAffinityList();`

- [ ] **Step 1: Write the failing tests**

Create `tests/bench/BenchUtilTest.cpp`:

```cpp
// tests/bench/BenchUtilTest.cpp — benchmark building blocks (pure, no Aeron).
#include "LogLinearHistogram.h"
#include "OpenLoopSchedule.h"
#include "ProcStat.h"
#include "SeqTimestampRing.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <type_traits>

using namespace rune::bench;

TEST(LogLinearHistogram, EmptyReportsZero) {
    LogLinearHistogram h;
    EXPECT_EQ(h.count(), 0u);
    EXPECT_EQ(h.valueAtQuantile(0.99), 0u);
    EXPECT_EQ(h.max(), 0u);
}

TEST(LogLinearHistogram, ExactBelow256) {
    LogLinearHistogram h;
    for (int64_t v = 0; v < 256; ++v) h.record(v);
    EXPECT_EQ(h.count(), 256u);
    EXPECT_EQ(h.valueAtQuantile(0.5), 127u);
    EXPECT_EQ(h.valueAtQuantile(1.0), 255u);
    EXPECT_EQ(h.min(), 0u);
}

TEST(LogLinearHistogram, RelativeErrorBelowOnePercent) {
    for (uint64_t v : {256ULL, 1000ULL, 12'345ULL, 1'000'000ULL, 987'654'321ULL, 3'700'000'000'000ULL}) {
        const int i = LogLinearHistogram::index(v);
        const uint64_t ub = LogLinearHistogram::upperBound(i);
        EXPECT_GE(ub, v);
        EXPECT_LE(ub - v, v / 128) << v;
        if (i > 0) EXPECT_LT(LogLinearHistogram::upperBound(i - 1), v) << v;
    }
}

TEST(LogLinearHistogram, QuantilesOfAUniformRun) {
    LogLinearHistogram h;
    for (int64_t v = 1; v <= 100'000; ++v) h.record(v);
    auto near = [](uint64_t got, double want) { return std::abs(static_cast<double>(got) - want) <= want / 128 + 1; };
    EXPECT_TRUE(near(h.valueAtQuantile(0.50), 50'000)) << h.valueAtQuantile(0.50);
    EXPECT_TRUE(near(h.valueAtQuantile(0.99), 99'000)) << h.valueAtQuantile(0.99);
    EXPECT_TRUE(near(h.valueAtQuantile(0.999), 99'900)) << h.valueAtQuantile(0.999);
    EXPECT_EQ(h.valueAtQuantile(1.0), 100'000u);
    EXPECT_EQ(h.max(), 100'000u);
}

TEST(LogLinearHistogram, HugeValuesClampButMaxStaysExact) {
    LogLinearHistogram h;
    h.record(int64_t{1} << 50);
    h.record(-5);  // negative clamps to 0
    EXPECT_EQ(h.count(), 2u);
    EXPECT_EQ(h.max(), uint64_t{1} << 50);
    EXPECT_EQ(h.valueAtQuantile(1.0), uint64_t{1} << 50);
    EXPECT_EQ(h.valueAtQuantile(0.5), 0u);
}

TEST(LogLinearHistogram, MergeAddsAndRecordNeverAllocates) {
    static_assert(std::is_trivially_copyable_v<LogLinearHistogram>,
                  "fixed array storage: record() cannot allocate");
    LogLinearHistogram a, b;
    for (int i = 0; i < 10; ++i) a.record(1000);
    for (int i = 0; i < 30; ++i) b.record(2'000'000);
    a.merge(b);
    EXPECT_EQ(a.count(), 40u);
    EXPECT_EQ(a.max(), 2'000'000u);
    EXPECT_EQ(a.min(), 1000u);
}

TEST(OpenLoopSchedule, DueCountMatchesScheduleExactly) {
    for (uint64_t rate : {1ULL, 3ULL, 100'000ULL, 1'000'000ULL, 7'777'777ULL}) {
        const OpenLoopSchedule s(5'000, rate);
        EXPECT_EQ(s.dueCount(4'999), 0u);
        EXPECT_EQ(s.dueCount(5'000), 1u) << "record 0 is due at startNs";
        for (int64_t e = 0; e <= 20'000; e += 7) {
            uint64_t brute = 0;
            while (s.scheduledNs(brute) <= 5'000 + e) ++brute;
            ASSERT_EQ(s.dueCount(5'000 + e), brute) << "rate=" << rate << " e=" << e;
        }
    }
}

TEST(OpenLoopSchedule, NoDriftOverAMinuteAtOneHundredThousandPerSecond) {
    const OpenLoopSchedule s(0, 100'000);
    EXPECT_EQ(s.scheduledNs(6'000'000), 60'000'000'000);
    EXPECT_EQ(s.dueCount(60'000'000'000 - 1), 6'000'000u);
}

TEST(SeqTimestampRing, OutOfOrderTakesKeepTheWindowUntilTheOldestIsTaken) {
    SeqTimestampRing r(4);
    for (int64_t t = 10; t < 14; ++t) r.push(t);
    EXPECT_TRUE(r.full());
    EXPECT_EQ(r.take(2), 12);
    EXPECT_TRUE(r.full()) << "seq 0 still outstanding";
    EXPECT_EQ(r.take(0), 10);
    EXPECT_EQ(r.take(1), 11);
    EXPECT_FALSE(r.full());
    EXPECT_EQ(r.outstanding(), 1u);
    r.push(14);
    EXPECT_EQ(r.nextSeq(), 5u);
    EXPECT_EQ(r.take(4), 14);
}

TEST(ProcStat, ParsesCpuLines) {
    const auto agg = parseProcStatCpuLine("cpu  100 5 50 1000 20 1 2 3 0 0");
    ASSERT_TRUE(agg.has_value());
    EXPECT_EQ(agg->first, -1);
    EXPECT_EQ(agg->second.busy, 100u + 5 + 50 + 1 + 2 + 3);
    EXPECT_EQ(agg->second.total, agg->second.busy + 1000 + 20);
    const auto c3 = parseProcStatCpuLine("cpu3 10 0 10 80 0 0 0 0 0 0");
    ASSERT_TRUE(c3.has_value());
    EXPECT_EQ(c3->first, 3);
    EXPECT_FALSE(parseProcStatCpuLine("intr 12345 0 0").has_value());
}

TEST(ProcStat, ParsesTaskStatWithSpacesInComm) {
    const auto t = parseTaskStat(
        "4242 (rune shard 0) R 1 2 3 4 5 6 7 8 9 10 250 50 0 0 20 0 1 0 100 0 0");
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->first, "rune shard 0");
    EXPECT_EQ(t->second, 300u);  // utime 250 + stime 50
}

TEST(ProcStat, BusyFractionsBetweenSnapshots) {
    const std::vector<CpuTimes> a{{100, 1000}, {0, 1000}};
    const std::vector<CpuTimes> b{{600, 2000}, {10, 2000}};
    const auto f = busyFractions(a, b);
    ASSERT_EQ(f.size(), 2u);
    EXPECT_DOUBLE_EQ(f[0], 0.5);
    EXPECT_DOUBLE_EQ(f[1], 0.01);
}
```

Add to `CMakeLists.txt` after the `rune_invoker_tests` block:

```cmake
    # ── Benchmark building blocks (header-only, examples/bench) ──────────────
    add_executable(rune_bench_util_tests tests/bench/BenchUtilTest.cpp)
    target_include_directories(rune_bench_util_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples/bench)
    target_link_libraries(rune_bench_util_tests PRIVATE GTest::gtest_main)
    gtest_discover_tests(rune_bench_util_tests)
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake -S . -B cmake-build-sic >/dev/null && cmake --build cmake-build-sic --target rune_bench_util_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'LogLinearHistogram.h' file not found`.

- [ ] **Step 3: Implement the headers**

`examples/bench/LogLinearHistogram.h`:

```cpp
// examples/bench/LogLinearHistogram.h
//
// Compact HdrHistogram-style log-linear latency histogram (ns).  Values < 256 are
// exact; above, each power of two is split into 128 linear sub-buckets (relative
// error <= 1/128 = 0.78%) up to 2^43 ns (~2.4 h).  record() is O(1) and never
// allocates (fixed std::array); larger values clamp into the last bucket while
// max() stays exact.  Spec §5: every record is recorded, no sampling.
#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>

namespace rune::bench {

class LogLinearHistogram {
public:
    static constexpr int kSubBits = 8;   // first 256 values exact
    static constexpr int kMaxExp  = 42;  // last octave [2^42, 2^43)
    static constexpr int kHalf    = 1 << (kSubBits - 1);  // 128 sub-buckets per octave
    static constexpr int kBuckets = (1 << kSubBits) + (kMaxExp - kSubBits + 1) * kHalf;  // 4736

    void record(int64_t valueNs) noexcept {
        const uint64_t v = valueNs < 0 ? 0 : static_cast<uint64_t>(valueNs);
        ++counts_[static_cast<std::size_t>(index(v))];
        ++total_;
        if (v > max_) max_ = v;
        if (v < min_) min_ = v;
    }

    [[nodiscard]] uint64_t count() const noexcept { return total_; }
    [[nodiscard]] uint64_t max() const noexcept { return total_ != 0 ? max_ : 0; }
    [[nodiscard]] uint64_t min() const noexcept { return total_ != 0 ? min_ : 0; }

    /// Upper bound of the bucket holding the ceil(q * count)-th smallest value
    /// (HdrHistogram "highest equivalent value": conservative for tails), capped at max().
    [[nodiscard]] uint64_t valueAtQuantile(double q) const noexcept {
        if (total_ == 0) return 0;
        const double c = std::clamp(q, 0.0, 1.0);
        uint64_t rank = static_cast<uint64_t>(std::ceil(c * static_cast<double>(total_)));
        if (rank == 0) rank = 1;
        uint64_t seen = 0;
        for (int i = 0; i < kBuckets; ++i) {
            seen += counts_[static_cast<std::size_t>(i)];
            if (seen >= rank) {
                return i == kBuckets - 1 ? max_ : std::min<uint64_t>(upperBound(i), max_);
            }
        }
        return max_;
    }

    void merge(const LogLinearHistogram& o) noexcept {
        for (int i = 0; i < kBuckets; ++i) {
            counts_[static_cast<std::size_t>(i)] += o.counts_[static_cast<std::size_t>(i)];
        }
        total_ += o.total_;
        if (o.total_ != 0) {
            max_ = std::max(max_, o.max_);
            min_ = std::min(min_, o.min_);
        }
    }

    void reset() noexcept {
        counts_.fill(0);
        total_ = 0;
        max_   = 0;
        min_   = UINT64_MAX;
    }

    static constexpr int index(uint64_t v) noexcept {
        if (v < (uint64_t{1} << kSubBits)) return static_cast<int>(v);
        const int e = 63 - std::countl_zero(v);  // floor(log2 v) >= kSubBits
        if (e > kMaxExp) return kBuckets - 1;
        const int shift = e - (kSubBits - 1);     // keep kSubBits significant bits
        const int sub   = static_cast<int>(v >> shift);  // [128, 256)
        return (1 << kSubBits) + (e - kSubBits) * kHalf + (sub - kHalf);
    }

    static constexpr uint64_t upperBound(int i) noexcept {
        if (i < (1 << kSubBits)) return static_cast<uint64_t>(i);
        const int j     = i - (1 << kSubBits);
        const int e     = kSubBits + j / kHalf;
        const int sub   = kHalf + j % kHalf;
        const int shift = e - (kSubBits - 1);
        return (static_cast<uint64_t>(sub + 1) << shift) - 1;
    }

private:
    std::array<uint64_t, kBuckets> counts_{};
    uint64_t total_ = 0;
    uint64_t max_   = 0;
    uint64_t min_   = UINT64_MAX;
};

} // namespace rune::bench
```

`examples/bench/OpenLoopSchedule.h`:

```cpp
// examples/bench/OpenLoopSchedule.h — fixed-rate open-loop send schedule.  Record i
// is due at startNs + floor(i * 1e9 / rate): exact integer math, no drift.
// Latency is measured from scheduledNs(i) (coordinated-omission-safe, spec §5).
#pragma once

#include <cstdint>

namespace rune::bench {

class OpenLoopSchedule {
public:
    OpenLoopSchedule(int64_t startNs, uint64_t ratePerSec) noexcept
        : startNs_(startNs), rate_(ratePerSec == 0 ? 1 : ratePerSec) {}

    [[nodiscard]] int64_t scheduledNs(uint64_t i) const noexcept {
        return startNs_ + static_cast<int64_t>((static_cast<unsigned __int128>(i) * kNsPerSec) / rate_);
    }

    /// Number of records with scheduledNs(i) <= nowNs, i.e. ceil((elapsed + 1) * rate / 1e9).
    [[nodiscard]] uint64_t dueCount(int64_t nowNs) const noexcept {
        if (nowNs < startNs_) return 0;
        const unsigned __int128 elapsed = static_cast<uint64_t>(nowNs - startNs_);
        return static_cast<uint64_t>(((elapsed + 1) * rate_ + (kNsPerSec - 1)) / kNsPerSec);
    }

private:
    static constexpr uint64_t kNsPerSec = 1'000'000'000ULL;
    int64_t  startNs_;
    uint64_t rate_;
};

} // namespace rune::bench
```

`examples/bench/SeqTimestampRing.h`:

```cpp
// examples/bench/SeqTimestampRing.h — per-partition timestamps indexed by the
// producer's idempotent sequence (BatchAck::baseSequence + i), so batches may be
// acknowledged in any order.  Storage is allocated once in the constructor.
#pragma once

#include <cstdint>
#include <vector>

namespace rune::bench {

class SeqTimestampRing {
public:
    explicit SeqTimestampRing(uint32_t minCapacity) {
        uint32_t cap = 1;
        while (cap < minCapacity) cap <<= 1;
        slots_.resize(cap);
        mask_ = cap - 1;
    }
    /// True when the next push would overwrite the oldest not-yet-taken slot.
    [[nodiscard]] bool     full() const noexcept { return next_ - oldest_ >= slots_.size(); }
    [[nodiscard]] uint64_t nextSeq() const noexcept { return next_; }
    [[nodiscard]] uint64_t outstanding() const noexcept { return next_ - oldest_; }

    void push(int64_t ts) noexcept {  // caller checked !full()
        slots_[next_ & mask_] = Slot{ts, false};
        ++next_;
    }

    int64_t take(uint64_t seq) noexcept {
        Slot& s = slots_[seq & mask_];
        s.taken = true;
        while (oldest_ < next_ && slots_[oldest_ & mask_].taken) ++oldest_;
        return s.ts;
    }

private:
    struct Slot {
        int64_t ts    = 0;
        bool    taken = true;
    };
    std::vector<Slot> slots_;
    uint64_t          mask_   = 0;
    uint64_t          next_   = 0;
    uint64_t          oldest_ = 0;
};

} // namespace rune::bench
```

`examples/bench/ProcStat.h`:

```cpp
// examples/bench/ProcStat.h — idle check + per-thread CPU split for the bench
// (spec §5).  Linux /proc readers; empty results elsewhere (macOS).
#pragma once

#include <cstdint>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
#endif

namespace rune::bench {

struct CpuTimes {
    uint64_t busy  = 0;
    uint64_t total = 0;
};

/// "cpuN user nice system idle iowait irq softirq steal ..." -> (N, times); "cpu" -> (-1, times).
[[nodiscard]] inline std::optional<std::pair<int, CpuTimes>> parseProcStatCpuLine(std::string_view line) {
    if (line.substr(0, 3) != "cpu") return std::nullopt;
    std::istringstream in{std::string(line)};
    std::string label;
    in >> label;
    int cpu = -1;
    if (label.size() > 3) {
        try {
            cpu = std::stoi(label.substr(3));
        } catch (...) {
            return std::nullopt;
        }
    }
    uint64_t f[8] = {};
    for (uint64_t& x : f) {
        if (!(in >> x)) return std::nullopt;
    }
    CpuTimes t;
    t.busy  = f[0] + f[1] + f[2] + f[5] + f[6] + f[7];  // user nice system irq softirq steal
    t.total = t.busy + f[3] + f[4];                     // + idle iowait
    return std::make_pair(cpu, t);
}

/// "/proc/<pid>/task/<tid>/stat" -> (comm, utime + stime ticks).
[[nodiscard]] inline std::optional<std::pair<std::string, uint64_t>> parseTaskStat(std::string_view s) {
    const auto open = s.find('(');
    const auto close = s.rfind(')');
    if (open == std::string_view::npos || close == std::string_view::npos || close < open) return std::nullopt;
    std::istringstream in{std::string(s.substr(close + 1))};
    std::string field;
    uint64_t utime = 0, stime = 0;
    for (int i = 3; i <= 15; ++i) {  // field 3 = state ... field 14 = utime, 15 = stime
        if (!(in >> field)) return std::nullopt;
        if (i == 14) utime = std::stoull(field);
        if (i == 15) stime = std::stoull(field);
    }
    return std::make_pair(std::string(s.substr(open + 1, close - open - 1)), utime + stime);
}

[[nodiscard]] inline std::vector<CpuTimes> readPerCpuTimes() {
    std::vector<CpuTimes> out;
#if defined(__linux__)
    std::ifstream f("/proc/stat");
    std::string line;
    while (std::getline(f, line)) {
        const auto p = parseProcStatCpuLine(line);
        if (!p || p->first < 0) continue;
        if (static_cast<std::size_t>(p->first) >= out.size()) out.resize(static_cast<std::size_t>(p->first) + 1);
        out[static_cast<std::size_t>(p->first)] = p->second;
    }
#endif
    return out;
}

[[nodiscard]] inline std::vector<double> busyFractions(const std::vector<CpuTimes>& a,
                                                       const std::vector<CpuTimes>& b) {
    std::vector<double> out;
    for (std::size_t i = 0; i < a.size() && i < b.size(); ++i) {
        const double dt = static_cast<double>(b[i].total - a[i].total);
        out.push_back(dt > 0 ? static_cast<double>(b[i].busy - a[i].busy) / dt : 0.0);
    }
    return out;
}

/// CPU seconds consumed by the calling thread (Linux), -1 elsewhere.
[[nodiscard]] inline double readThreadSelfCpuSeconds() {
#if defined(__linux__)
    std::ifstream f("/proc/thread-self/stat");
    std::string s((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const auto t = parseTaskStat(s);
    return t ? static_cast<double>(t->second) / static_cast<double>(::sysconf(_SC_CLK_TCK)) : -1.0;
#else
    return -1.0;
#endif
}

/// "0-3"-style list of CPUs this process may run on (Linux), "n/a" elsewhere.
[[nodiscard]] inline std::string cpuAffinityList() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) != 0) return "n/a";
    std::string out;
    for (int c = 0; c < CPU_SETSIZE; ++c) {
        if (CPU_ISSET(c, &set)) out += (out.empty() ? "" : ",") + std::to_string(c);
    }
    return out;
#else
    return "n/a";
#endif
}

} // namespace rune::bench
```

- [ ] **Step 4: Run to verify pass**

Run: `cmake --build cmake-build-sic --target rune_bench_util_tests -j8 && ./cmake-build-sic/rune_bench_util_tests`
Expected: PASS (12 tests).

- [ ] **Step 5: Commit**

```bash
git add examples/bench/LogLinearHistogram.h examples/bench/OpenLoopSchedule.h examples/bench/SeqTimestampRing.h \
        examples/bench/ProcStat.h tests/bench/BenchUtilTest.cpp CMakeLists.txt
git commit -m "bench(client): log-linear histogram, open-loop schedule, seq-indexed timestamps, /proc readers

Header-only building blocks for the invoker bench (spec 5): a fixed-array
HdrHistogram-style histogram (<=0.78% error, no allocation on record), an
exact integer open-loop schedule (latency from the scheduled time), a
per-partition timestamp ring indexed by idempotent sequence (acks in any
order), and /proc parsers for the idle check and per-thread CPU split.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 18: `rune_produce_throughput` on invoker shards — open-loop `--rate`, full histogram, saturation mode, pinning

**F8c API (2026-10-02, C9):** each shard thread connects with `ClientShard::connectAndWait` (Invoker owner pumps its own `doWork()`); the shard/main start barrier is a pair of one-record `OneToOneRingBuffer`s per shard (no atomics, no sleeps); batch acks come from `ClientShard::pollBatchAcks` (Task 15, `Completions`-backed).

Spec §5: "`rune_produce_throughput` rewritten on client invoker mode: open-loop fixed rate (`--rate N`) …; every record recorded … p50/p99/p99.9/max; secondary saturation mode (max throughput, large window); every run verifies the box is idle first and reports core layout + per-thread CPU split; VM layout for the pass run: client shard pinned to core 0." Any command-line argument selects the new invoker bench; an env-only invocation keeps the legacy agent-mode path byte-for-byte (the VM scripts `rune_vm_bench.sh`, `bench/kafka-compare/run-rune.sh`, `e2e.sh` depend on it), so the default configuration stays measurable for A/B.

**Files:**
- Create: `examples/bench/InvokerBench.h` (args + parse, Aeron-free), `examples/bench/InvokerBench.cpp` (runtime)
- Modify: `examples/rune_produce_throughput.cpp:104` (`main(int argc, char** argv)` dispatch), header comment `:1-30`
- Modify: `CMakeLists.txt:282-297` (`rune_produce_throughput` sources + include dir)
- Modify: `tests/bench/BenchUtilTest.cpp` (append arg-parse tests)

**Interfaces:**
- Consumes: `ClientShard`, `ShardGroupConfig`, `shardFor` (Task 15); `BatchAck`, `pollBatchAcks` (Tasks 10, 15); `SendResult` (Task 13); `pinCurrentThreadToCore`, `currentCore` (Task 12); Task 17 building blocks.
- Produces:
  - `struct rune::bench::BenchArgs { Mode mode; uint64_t rate; int64_t durationS; int64_t warmupS; int32_t valueBytes; uint32_t shards; int32_t core; int64_t window; uint64_t records; std::string acks; int32_t maxInFlight; int64_t gateP99Us; uint64_t gateRps; std::vector<int> idleCheckCores; bool idleCheckOnly; };`
  - `std::optional<BenchArgs> rune::bench::parseBenchArgs(int argc, char** argv, std::string& error);`
  - `int rune::bench::invokerBenchMain(int argc, char** argv);` — exit 0 ok, 1 run error, 2 usage, 3 box not idle, 4 gate failed.
  - Output lines: `IDLE_CHECK ...`, `CORES ...`, `THREAD shard=<i> core=<c> cpu_s=<s>`, `RESULT mode=... achieved_rps=... p50_us=... p99_us=... p999_us=... max_us=... errors=...`, `GATE ... -> PASS|FAIL`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/bench/BenchUtilTest.cpp` (add `#include "InvokerBench.h"` at the top):

```cpp
namespace {
std::optional<BenchArgs> parse(std::vector<std::string> args, std::string& err) {
    std::vector<char*> argv{const_cast<char*>("rune_produce_throughput")};
    for (auto& a : args) argv.push_back(a.data());
    return parseBenchArgs(static_cast<int>(argv.size()), argv.data(), err);
}
} // namespace

TEST(BenchArgs, DefaultsMatchThePassCriterion) {
    std::string err;
    const auto a = parse({"--mode", "open"}, err);
    ASSERT_TRUE(a.has_value()) << err;
    EXPECT_EQ(a->mode, BenchArgs::Mode::Open);
    EXPECT_EQ(a->rate, 100'000u);
    EXPECT_EQ(a->durationS, 60);
    EXPECT_EQ(a->valueBytes, 1024);
    EXPECT_EQ(a->acks, "all");
    EXPECT_EQ(a->shards, 1u);
    EXPECT_EQ(a->core, -1);
}

TEST(BenchArgs, ParsesEveryOption) {
    std::string err;
    const auto a = parse({"--mode", "saturate", "--rate", "5000", "--duration-s", "10", "--warmup-s", "1",
                          "--value-bytes", "256", "--shards", "2", "--core", "0", "--window", "512",
                          "--records", "1000000", "--acks", "leader", "--max-in-flight", "64",
                          "--gate-p99-us", "100", "--gate-rps", "100000", "--idle-check-cores", "0,1,2,3",
                          "--idle-check-only"},
                         err);
    ASSERT_TRUE(a.has_value()) << err;
    EXPECT_EQ(a->mode, BenchArgs::Mode::Saturate);
    EXPECT_EQ(a->rate, 5000u);
    EXPECT_EQ(a->shards, 2u);
    EXPECT_EQ(a->core, 0);
    EXPECT_EQ(a->window, 512);
    EXPECT_EQ(a->records, 1'000'000u);
    EXPECT_EQ(a->acks, "leader");
    EXPECT_EQ(a->maxInFlight, 64);
    EXPECT_EQ(a->gateP99Us, 100);
    EXPECT_EQ(a->gateRps, 100'000u);
    EXPECT_EQ(a->idleCheckCores, (std::vector<int>{0, 1, 2, 3}));
    EXPECT_TRUE(a->idleCheckOnly);
}

TEST(BenchArgs, RejectsBadInput) {
    std::string err;
    EXPECT_FALSE(parse({"--rate"}, err).has_value());
    EXPECT_FALSE(parse({"--rate", "fast"}, err).has_value());
    EXPECT_FALSE(parse({"--acks", "maybe"}, err).has_value());
    EXPECT_FALSE(parse({"--value-bytes", "9000"}, err).has_value());
    EXPECT_FALSE(parse({"--shards", "0"}, err).has_value());
    EXPECT_FALSE(parse({"--bogus"}, err).has_value());
    EXPECT_NE(err.find("--bogus"), std::string::npos);
}
```

(also add `#include <optional>`, `#include <string>`, `#include <vector>` to the test file.)

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build cmake-build-sic --target rune_bench_util_tests -j8 2>&1 | grep -m1 error`
Expected: FAIL with `'InvokerBench.h' file not found`.

- [ ] **Step 3: Implement `InvokerBench.h`**

```cpp
// examples/bench/InvokerBench.h — invoker-shard produce bench: arguments (Aeron-free).
#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

namespace rune::bench {

struct BenchArgs {
    enum class Mode { Open, Saturate };
    Mode        mode        = Mode::Open;
    uint64_t    rate        = 100'000;    // --rate: offered records/s (all shards together)
    int64_t     durationS   = 60;         // --duration-s: measurement window
    int64_t     warmupS     = 5;          // --warmup-s: offered but not measured
    int32_t     valueBytes  = 1024;       // --value-bytes
    uint32_t    shards      = 1;          // --shards: invoker shards (one app thread each)
    int32_t     core        = -1;         // --core: shard i pinned to core + i (-1 = no pin)
    int64_t     window      = 100'000;    // --window: saturate mode max outstanding
    uint64_t    records     = 3'000'000;  // --records: saturate mode total
    std::string acks        = "all";      // --acks none|leader|all
    int32_t     maxInFlight = 32;         // --max-in-flight: per-partition in-flight batches
    int64_t     gateP99Us   = 0;          // --gate-p99-us: fail (exit 4) if p99 above (0 = off)
    uint64_t    gateRps     = 0;          // --gate-rps: fail (exit 4) if achieved below (0 = off)
    std::vector<int> idleCheckCores;      // --idle-check-cores 0,1,2,3: each must be < 5% busy
    bool        idleCheckOnly = false;    // --idle-check-only: run the idle check and exit
};

inline constexpr const char* kBenchUsage =
    "usage: rune_produce_throughput [--mode open|saturate] [--rate N] [--duration-s S]\n"
    "         [--warmup-s S] [--value-bytes B] [--shards N] [--core C] [--window W]\n"
    "         [--records N] [--acks none|leader|all] [--max-in-flight N]\n"
    "         [--gate-p99-us U] [--gate-rps R] [--idle-check-cores 0,1,..] [--idle-check-only]\n"
    "  env: RUNE_TARGET RUNE_TARGET_STREAM RUNE_EGRESS RUNE_EGRESS_STREAM RUNE_DATA_DIR RUNE_TOPICS\n"
    "  (no arguments = legacy env-driven agent-mode bench)\n";

[[nodiscard]] inline std::optional<BenchArgs> parseBenchArgs(int argc, char** argv, std::string& error) {
    BenchArgs a;
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        auto value = [&](std::string& out) -> bool {
            if (i + 1 >= argc) {
                error = "missing value for " + k;
                return false;
            }
            out = argv[++i];
            return true;
        };
        auto number = [&](long long& out) -> bool {
            std::string v;
            if (!value(v)) return false;
            char* end = nullptr;
            out = std::strtoll(v.c_str(), &end, 10);
            if (v.empty() || end == nullptr || *end != '\0' || out < 0) {
                error = "bad number for " + k + ": " + v;
                return false;
            }
            return true;
        };
        long long n = 0;
        std::string s;
        if (k == "--mode") {
            if (!value(s)) return std::nullopt;
            if (s == "open") a.mode = BenchArgs::Mode::Open;
            else if (s == "saturate") a.mode = BenchArgs::Mode::Saturate;
            else { error = "--mode must be open|saturate"; return std::nullopt; }
        } else if (k == "--rate") { if (!number(n)) return std::nullopt; a.rate = static_cast<uint64_t>(n); }
        else if (k == "--duration-s") { if (!number(n)) return std::nullopt; a.durationS = n; }
        else if (k == "--warmup-s") { if (!number(n)) return std::nullopt; a.warmupS = n; }
        else if (k == "--value-bytes") { if (!number(n)) return std::nullopt; a.valueBytes = static_cast<int32_t>(n); }
        else if (k == "--shards") { if (!number(n)) return std::nullopt; a.shards = static_cast<uint32_t>(n); }
        else if (k == "--core") { if (!number(n)) return std::nullopt; a.core = static_cast<int32_t>(n); }
        else if (k == "--window") { if (!number(n)) return std::nullopt; a.window = n; }
        else if (k == "--records") { if (!number(n)) return std::nullopt; a.records = static_cast<uint64_t>(n); }
        else if (k == "--max-in-flight") { if (!number(n)) return std::nullopt; a.maxInFlight = static_cast<int32_t>(n); }
        else if (k == "--gate-p99-us") { if (!number(n)) return std::nullopt; a.gateP99Us = n; }
        else if (k == "--gate-rps") { if (!number(n)) return std::nullopt; a.gateRps = static_cast<uint64_t>(n); }
        else if (k == "--acks") {
            if (!value(s)) return std::nullopt;
            if (s != "none" && s != "leader" && s != "all") { error = "--acks must be none|leader|all"; return std::nullopt; }
            a.acks = s;
        } else if (k == "--idle-check-cores") {
            if (!value(s)) return std::nullopt;
            std::size_t pos = 0;
            while (pos <= s.size()) {
                const std::size_t comma = s.find(',', pos);
                const std::string tok = s.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                char* end = nullptr;
                const long c = std::strtol(tok.c_str(), &end, 10);
                if (tok.empty() || *end != '\0' || c < 0) { error = "bad --idle-check-cores: " + s; return std::nullopt; }
                a.idleCheckCores.push_back(static_cast<int>(c));
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        } else if (k == "--idle-check-only") {
            a.idleCheckOnly = true;
        } else {
            error = "unknown option " + k;
            return std::nullopt;
        }
    }
    if (a.mode == BenchArgs::Mode::Open && a.rate == 0) { error = "--rate must be > 0"; return std::nullopt; }
    if (a.shards == 0) { error = "--shards must be >= 1"; return std::nullopt; }
    if (a.valueBytes <= 0 || a.valueBytes > 8154) { error = "--value-bytes must be in [1, 8154]"; return std::nullopt; }
    if (a.maxInFlight <= 0) { error = "--max-in-flight must be > 0"; return std::nullopt; }
    return a;
}

/// Runs the invoker-shard bench (see kBenchUsage).  Exit codes: 0 ok, 1 run error,
/// 2 usage, 3 box not idle, 4 gate failed.
int invokerBenchMain(int argc, char** argv);

} // namespace rune::bench
```

Run: `cmake --build cmake-build-sic --target rune_bench_util_tests -j8 && ./cmake-build-sic/rune_bench_util_tests --gtest_filter='BenchArgs.*'`
Expected: PASS (3 tests).

- [ ] **Step 4: Implement the runtime (`InvokerBench.cpp`)**

```cpp
// examples/bench/InvokerBench.cpp — open-loop, coordinated-omission-safe produce
// bench on invoker-mode client shards (spec §5).  Each shard thread IS its shard:
// it schedules records, trySend()s them, runs doWork(), and records every ack into
// a LogLinearHistogram, latency measured from the record's SCHEDULED send time.
#include "InvokerBench.h"
#include "LogLinearHistogram.h"
#include "OpenLoopSchedule.h"
#include "ProcStat.h"
#include "SeqTimestampRing.h"

#include <rune/client/detail/CpuAffinity.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/client/shard/ClientShard.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rune::bench {
namespace {

namespace rc = rune::client;
namespace rs = rune::client::shard;

int64_t monoNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
std::string envStr(const char* n, const std::string& d = {}) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::string(v) : d;
}
int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::atoll(v) : d;
}
std::vector<rc::TopicSpec> parseTopics(const std::string& s) {  // "name:id:partitions,..."
    std::vector<rc::TopicSpec> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = (comma == std::string::npos ? s.size() + 1 : comma + 1);
        const auto c1 = tok.find(':');
        const auto c2 = c1 == std::string::npos ? std::string::npos : tok.find(':', c1 + 1);
        if (c2 == std::string::npos) continue;
        rc::TopicSpec ts;
        ts.name = tok.substr(0, c1);
        ts.topicId = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}
rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 10'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to partition " + std::to_string(partition));
}

struct ShardResult {
    LogLinearHistogram hist;
    uint64_t sent = 0, acked = 0, ackedInWindow = 0, errors = 0;
    int      core = -1;
    double   cpuSeconds = -1.0;
    bool     ok = true;
    std::string error;
};

// Start barrier with no atomics (F8 law: cross-thread state only through rings):
// per shard, two one-record OneToOneRingBuffers — shard -> main "ready" (connected,
// or failed), then main -> shard "start" carrying the common schedule origin.  The
// ring's release/acquire on the record header is the only ordering needed.
struct StartLink {
    static constexpr std::size_t kBytes = 64 + rc::detail::OneToOneRingBuffer::trailerLength();
    alignas(64) std::uint8_t readyBuf[kBytes] = {};
    alignas(64) std::uint8_t startBuf[kBytes] = {};
    rc::detail::OneToOneRingBuffer ready{readyBuf, kBytes};
    rc::detail::OneToOneRingBuffer start{startBuf, kBytes};
};
constexpr std::int32_t kMsgReady = 1;
constexpr std::int32_t kMsgStart = 2;

void signalReady(StartLink& l) {
    if (!l.ready.writeWith(kMsgReady, 0, [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {})) {
        std::abort();  // unreachable: one record into an empty 64-byte ring
    }
}
bool tryReadReady(StartLink& l) {
    return l.ready.read([](std::int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                           aeron::util::index_t) {}, 1) == 1;
}
void sendStart(StartLink& l, int64_t startNs) {
    if (!l.start.writeWith(kMsgStart, static_cast<aeron::util::index_t>(sizeof(int64_t)),
                           [&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off) {
                               b.putInt64(off, startNs);
                           })) {
        std::abort();  // unreachable: one record into an empty 64-byte ring
    }
}
int64_t tryReadStart(StartLink& l) {  // 0 = not yet
    int64_t v = 0;
    l.start.read([&](std::int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                     aeron::util::index_t) { v = b.getInt64(off); }, 1);
    return v;
}

void runShard(const BenchArgs& a, const rs::ShardGroupConfig& g, uint32_t index,
              const std::string& topicName, StartLink& link, ShardResult& out) {
    if (a.core >= 0 && !rc::detail::pinCurrentThreadToCore(a.core + static_cast<int>(index))) {
        std::fprintf(stderr, "WARN: could not pin shard %u to core %d\n", index, a.core + static_cast<int>(index));
    }
    out.core = rc::detail::currentCore();
    bool signaled = false;
    try {
        rs::ClientShard shard(g, index);
        // F8c: Invoker owner — connectAndWait pumps this shard's doWork() itself
        // and throws on any non-Ok outcome or after 10 s.
        shard.connectAndWait(std::chrono::seconds(10));
        const rc::TopicHandle topic = shard.topic(topicName);
        if (!topic.valid()) throw std::runtime_error("unknown topic " + topicName);

        std::vector<int32_t> owned;
        std::vector<rc::Key> keyOf(static_cast<std::size_t>(topic.partitionCount));
        std::vector<std::unique_ptr<SeqTimestampRing>> sched(static_cast<std::size_t>(topic.partitionCount));
        for (int32_t p = 0; p < topic.partitionCount; ++p) {
            if (!shard.owns(topic.topicId, p)) continue;
            owned.push_back(p);
            keyOf[static_cast<std::size_t>(p)] = keyFor(p, topic.partitionCount);
            sched[static_cast<std::size_t>(p)] = std::make_unique<SeqTimestampRing>(1u << 18);
        }
        if (owned.empty()) throw std::runtime_error("shard owns no partition of " + topicName);
        const std::vector<uint8_t> value(static_cast<std::size_t>(a.valueBytes), uint8_t{0xAB});

        signalReady(link);
        signaled = true;
        int64_t startNs = 0;
        while ((startNs = tryReadStart(link)) == 0) shard.doWork();

        const uint64_t shardRate = a.rate / a.shards + (index == 0 ? a.rate % a.shards : 0);
        const OpenLoopSchedule schedule(startNs, shardRate);
        const int64_t warmEnd  = startNs + a.warmupS * 1'000'000'000LL;
        const int64_t endNs    = warmEnd + a.durationS * 1'000'000'000LL;
        const bool    open     = a.mode == BenchArgs::Mode::Open;
        const uint64_t total   = open ? schedule.dueCount(endNs - 1)
                                      : a.records / a.shards + (index == 0 ? a.records % a.shards : 0);
        const int64_t drainDeadline = (open ? endNs : startNs + 600'000'000'000LL) + 30'000'000'000LL;

        std::size_t rr = 0;
        int64_t ackNow = 0;
        auto onAck = [&](const rc::BatchAck& ack) {
            SeqTimestampRing& ring = *sched[static_cast<std::size_t>(ack.partition)];
            for (int32_t k = 0; k < ack.recordCount; ++k) {
                const int64_t t = ring.take(static_cast<uint64_t>(ack.baseSequence) + static_cast<uint64_t>(k));
                ++out.acked;
                if (ack.errorCode != 0) {
                    ++out.errors;
                    continue;
                }
                if (t >= warmEnd && (!open || t < endNs)) out.hist.record(ackNow - t);
                if (ackNow >= warmEnd && ackNow < endNs) ++out.ackedInWindow;
            }
        };
        for (;;) {
            const int64_t now = monoNs();
            const uint64_t due = open ? std::min(total, schedule.dueCount(now)) : total;
            while (out.sent < due && (open || out.sent - out.acked < static_cast<uint64_t>(a.window))) {
                const int32_t p = owned[rr % owned.size()];
                SeqTimestampRing& ring = *sched[static_cast<std::size_t>(p)];
                if (ring.full()) break;
                const rc::SendResult r = shard.trySend(topic, keyOf[static_cast<std::size_t>(p)], value);
                // Stays due (its latency keeps accruing).  QueueFull: the batch-ack
                // queue is out of credit until pollBatchAcks() below drains it.
                if (r == rc::SendResult::BackPressured || r == rc::SendResult::QueueFull) break;
                if (r != rc::SendResult::Ok) {
                    throw std::runtime_error("trySend failed: SendResult=" + std::to_string(static_cast<int>(r)));
                }
                ring.push(open ? schedule.scheduledNs(out.sent) : now);
                ++out.sent;
                ++rr;
            }
            shard.doWork();
            ackNow = monoNs();
            shard.pollBatchAcks(onAck);
            if (out.sent >= total && out.acked >= out.sent) break;
            if (shard.closed()) throw std::runtime_error("shard transport closed");
            if (now > drainDeadline) {
                throw std::runtime_error("drain timeout: sent=" + std::to_string(out.sent) +
                                         " acked=" + std::to_string(out.acked));
            }
        }
    } catch (const std::exception& e) {
        out.ok = false;
        out.error = e.what();
        if (!signaled) signalReady(link);  // never leave main waiting
    }
    out.cpuSeconds = readThreadSelfCpuSeconds();
}

} // namespace

int invokerBenchMain(int argc, char** argv) {
    std::string err;
    const auto parsed = parseBenchArgs(argc, argv, err);
    if (!parsed) {
        std::cerr << "usage error: " << err << "\n" << kBenchUsage;
        return 2;
    }
    const BenchArgs a = *parsed;

    if (!a.idleCheckCores.empty()) {
        const auto before = readPerCpuTimes();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        const auto after = readPerCpuTimes();
        const auto busy = busyFractions(before, after);
        if (busy.empty()) {
            std::cout << "IDLE_CHECK unsupported (no /proc/stat)\n";
        } else {
            bool idle = true;
            std::cout << "IDLE_CHECK";
            for (int c : a.idleCheckCores) {
                const double b = static_cast<std::size_t>(c) < busy.size() ? busy[static_cast<std::size_t>(c)] : 1.0;
                std::printf(" cpu%d=%.1f%%", c, b * 100.0);
                if (b > 0.05) idle = false;
            }
            std::cout << (idle ? " -> PASS\n" : " -> FAIL\n");
            if (!idle) return 3;
        }
    }
    if (a.idleCheckOnly) return 0;

    const std::string target = envStr("RUNE_TARGET"), egress = envStr("RUNE_EGRESS"), dataDir = envStr("RUNE_DATA_DIR");
    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "RUNE_TARGET, RUNE_EGRESS, RUNE_DATA_DIR are required\n" << kBenchUsage;
        return 2;
    }
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = target;
    g.base.gatewayStreamId = static_cast<int32_t>(envI64("RUNE_TARGET_STREAM", 1));
    g.base.egressChannel   = egress;
    g.base.egressStreamId  = static_cast<int32_t>(envI64("RUNE_EGRESS_STREAM", 2));
    g.base.dataDir         = dataDir;
    g.base.topics          = parseTopics(envStr("RUNE_TOPICS", "bench:0:8"));
    if (g.base.topics.empty()) {
        std::cerr << "no topics parsed from RUNE_TOPICS\n";
        return 2;
    }
    g.shardCount           = a.shards;
    g.mode                 = rs::ShardMode::Invoker;
    g.producer.acks        = a.acks == "none" ? rc::Acks::None : a.acks == "leader" ? rc::Acks::Leader : rc::Acks::All;
    g.producer.maxInFlight = a.maxInFlight;

    std::cout << "CORES online=" << std::thread::hardware_concurrency() << " affinity=" << cpuAffinityList()
              << " shards=" << a.shards << " first_core=" << a.core << "\n";

    const auto links = std::make_unique<StartLink[]>(a.shards);  // rings point into themselves: never moved
    std::vector<ShardResult> results(a.shards);
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < a.shards; ++i) {
        threads.emplace_back(runShard, std::cref(a), std::cref(g), i, g.base.topics[0].name,
                             std::ref(links[i]), std::ref(results[i]));
    }
    // Every shard reports exactly once (connected, or failed — connectAndWait is
    // itself bounded), so this wait needs no deadline of its own.
    std::vector<bool> isReady(a.shards, false);
    for (uint32_t readyCount = 0; readyCount < a.shards;) {
        for (uint32_t i = 0; i < a.shards; ++i) {
            if (!isReady[i] && tryReadReady(links[i])) {
                isReady[i] = true;
                ++readyCount;
            }
        }
        std::this_thread::yield();
    }
    const int64_t origin = monoNs() + 100'000'000LL;  // common schedule origin
    for (uint32_t i = 0; i < a.shards; ++i) sendStart(links[i], origin);
    for (auto& t : threads) t.join();
    const double wallS =
        static_cast<double>(monoNs() - startAt.load(std::memory_order_acquire)) / 1e9;

    LogLinearHistogram all;
    uint64_t sent = 0, acked = 0, inWindow = 0, errors = 0;
    bool ok = true;
    for (uint32_t i = 0; i < a.shards; ++i) {
        const ShardResult& r = results[i];
        all.merge(r.hist);
        sent += r.sent;
        acked += r.acked;
        inWindow += r.ackedInWindow;
        errors += r.errors;
        std::printf("THREAD shard=%u core=%d cpu_s=%.3f%s%s\n", i, r.core, r.cpuSeconds,
                    r.ok ? "" : " error=", r.ok ? "" : r.error.c_str());
        ok = ok && r.ok;
    }
    // Open loop: records acked inside the measurement window per second (falling
    // behind shows up here).  Saturation: acked records per wall-clock second.
    const double achieved = a.mode == BenchArgs::Mode::Open
                                ? static_cast<double>(inWindow) / static_cast<double>(a.durationS)
                                : static_cast<double>(acked) / (wallS > 0 ? wallS : 1.0);
    auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    const double p99us = us(all.valueAtQuantile(0.99));
    std::printf("RESULT mode=%s shards=%u rate=%llu duration_s=%lld value=%d acks=%s sent=%llu acked=%llu "
                "errors=%llu measured=%llu achieved_rps=%.0f p50_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f\n",
                a.mode == BenchArgs::Mode::Open ? "open" : "saturate", a.shards,
                static_cast<unsigned long long>(a.rate), static_cast<long long>(a.durationS), a.valueBytes,
                a.acks.c_str(), static_cast<unsigned long long>(sent), static_cast<unsigned long long>(acked),
                static_cast<unsigned long long>(errors), static_cast<unsigned long long>(all.count()), achieved,
                us(all.valueAtQuantile(0.50)), p99us, us(all.valueAtQuantile(0.999)), us(all.max()));
    if (!ok || errors != 0) return 1;
    if (a.gateP99Us > 0 || a.gateRps > 0) {
        const bool pass = (a.gateP99Us == 0 || p99us <= static_cast<double>(a.gateP99Us)) &&
                          (a.gateRps == 0 || achieved >= static_cast<double>(a.gateRps));
        std::printf("GATE p99_us<=%lld achieved_rps>=%llu -> %s\n", static_cast<long long>(a.gateP99Us),
                    static_cast<unsigned long long>(a.gateRps), pass ? "PASS" : "FAIL");
        if (!pass) return 4;
    }
    return 0;
}

} // namespace rune::bench
```

`examples/rune_produce_throughput.cpp`: add `#include "InvokerBench.h"`, change `int main() {` (`:104`) to

```cpp
int main(int argc, char** argv) {
    // Any argument selects the invoker-shard bench (spec §5); env-only runs keep the
    // legacy agent-mode path unchanged (VM scripts and the default-config A/B use it).
    if (argc > 1) return rune::bench::invokerBenchMain(argc, argv);
```

and prepend to the header comment: `//   With arguments: invoker-shard open-loop bench — see examples/bench/InvokerBench.h (--rate, --mode, ...).`

`CMakeLists.txt` (`rune_produce_throughput`, `:282-297`):

```cmake
    add_executable(rune_produce_throughput
        examples/rune_produce_throughput.cpp
        examples/bench/InvokerBench.cpp
    )
    target_include_directories(rune_produce_throughput PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/examples/bench
    )
```

- [ ] **Step 5: Build, then a real end-to-end smoke against the local broker (default AND invoker)**

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic-rel --target rune_produce_throughput -j8
rm -rf /tmp/sic-broker /tmp/sic-client-a /tmp/sic-client-b && mkdir -p /tmp/sic-broker
RUNE_NODE_ID=0 RUNE_PEERS="0@127.0.0.1:40111:40200:40222" RUNE_CLIENT_ENDPOINT="127.0.0.1:40200" \
RUNE_DATA_DIR=/tmp/sic-broker RUNE_TOPICS="bench:8" RUNE_WORKERS=2 RUNE_PER_WORKER_INGRESS=true \
RUNE_LOG_LEVEL=warn /Users/vradu/Projects/rune-broker/build-release/rune_broker > /tmp/sic-broker.log 2>&1 &
BROKER=$!
for i in $(seq 1 80); do grep -q "is LEADER" /tmp/sic-broker.log && break; sleep 0.25; done
# (a) default configuration: legacy env path, thread-mode client
RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:40200" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40300" \
RUNE_DATA_DIR=/tmp/sic-client-a RUNE_TOPICS="bench:0:8" RUNE_BENCH_RECORDS=200000 RUNE_BENCH_WARMUP=10000 \
RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS=leader RUNE_BENCH_WINDOW=1024 RUNE_LINGER_US=0 \
  cmake-build-sic-rel/rune_produce_throughput | grep RESULT
# (b) invoker shard, open loop
RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:40200" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40310" \
RUNE_DATA_DIR=/tmp/sic-client-b RUNE_TOPICS="bench:0:8" \
  cmake-build-sic-rel/rune_produce_throughput --rate 20000 --duration-s 10 --warmup-s 2 --value-bytes 1024 --acks leader
kill $BROKER
```
Expected: (a) `RESULT mode=acked attempted=200000 succeeded=200000 failed=0 ...`; (b) `RESULT mode=open ... errors=0 ... achieved_rps=` ≈ 20000 (within 1%) with `p50_us/p99_us/p999_us/max_us` populated, `THREAD shard=0 ...` line, exit status 0. These are correctness smokes on the dev Mac (no latency conclusions from a shared, non-pinned box).

- [ ] **Step 6: Commit**

```bash
git add examples/bench/InvokerBench.h examples/bench/InvokerBench.cpp examples/rune_produce_throughput.cpp \
        tests/bench/BenchUtilTest.cpp CMakeLists.txt
git commit -m "bench(client): open-loop invoker-shard mode for rune_produce_throughput

With arguments the bench runs N invoker ClientShards (one pinned app thread
each): a fixed-rate open-loop schedule (--rate), every ack recorded into a
log-linear histogram with latency from the SCHEDULED send time
(coordinated-omission-safe), p50/p99/p99.9/max, a saturation mode, an idle
check, core layout + per-thread CPU split, and --gate-* exit codes.  Env-only
runs keep the legacy agent-mode path unchanged.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 19: Validation — full suites + TSan, real end-to-end (legacy and shard brokers, default and flag-on), interleaved A/B, §1 pass run

Spec §6: "Unit/regression … In-process integration … Dockerized DR matrix with shard mode on, plus TSan … VM runs, shard mode on and off (off must equal today's behaviour) … §1 pass criterion on a verified-idle box." §1a.7: "Done" means the real end-to-end path is green in the default configuration as well as flag-on. VM: `root@$RUNE_VM_HOST` (4 cores), repos under `/root/rune-e5`, client build dir `/root/rune-e5/rune-client-cpp/build/Release/build/Release` (Unix Makefiles, Release, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`, toolchain `/root/rune-e5/rune-client-cpp/build/Release/build/Release/generators/conan_toolchain.cmake`). The VM has no `rune-ql` sibling yet (the client's CMake globs it), so it is synced too.

**Files:**
- Create (outside the repo, not committed): `/tmp/sic-e2e.sh`, `/tmp/sic-ab.sh`
- No source changes. Any failure found here is fixed in the owning task's code with a failing `*Regression*` test first (§1a.7, §1a.12), then this task restarts from Step 1.

**Interfaces:**
- Consumes: everything above; the broker plan's shard mode (`RUNE_WORKER_MODE=shard`, `RUNE_RUNTIME_PLACEMENT=fused`, per-worker ports in `bench/kafka-compare/cluster.sh`) for Steps 5 and 7.
- Produces: pass/fail evidence pushed to rune-live; no API.

- [ ] **Step 1: Local — regression suite first, then everything, then TSan**

```bash
cd /Users/vradu/Projects/rune-client-cpp
cmake --build cmake-build-sic -j8
ctest --test-dir cmake-build-sic -R Regression --output-on-failure
ctest --test-dir cmake-build-sic -j4 --timeout 300 --output-on-failure 2>&1 | tail -5
cmake --build cmake-build-sic-tsan -j8
ctest --test-dir cmake-build-sic-tsan -R Regression --output-on-failure
ctest --test-dir cmake-build-sic-tsan -j2 --timeout 600 2>&1 | tail -5
grep -c "WARNING: ThreadSanitizer" cmake-build-sic-tsan/Testing/Temporary/LastTest.log
/Users/vradu/Projects/rune-broker/scripts/rune-live.sh test "rune-client/sharded-invoker" passed \
  passed=$(ctest --test-dir cmake-build-sic -N | awk '/Total Tests/{print $3}') failed=0 \
  total=$(ctest --test-dir cmake-build-sic -N | awk '/Total Tests/{print $3}') commit=$(git rev-parse --short HEAD)
```
Expected: `100% tests passed` in both dirs (the 292 baseline tests plus the new `rune_pure_tests`, `rune_invoker_tests`, `rune_bench_util_tests`); the TSan grep prints `0`.

- [ ] **Step 2: Sync to the VM (needs the user's approval: rsync to a public IP) and build + test on Linux**

```bash
cd /Users/vradu/Projects
RS="--exclude=.git --exclude=build --exclude=build-fast --exclude=build-release --exclude=build_release \
    --exclude=build_tsan --exclude=cmake-build-* --exclude=CMakeFiles --exclude=.sdd --exclude=docs"
for r in rune-protocol rune-fr rune-codec rune-ql rune-client-cpp; do
  rsync -az $RS $r/ root@$RUNE_VM_HOST:/root/rune-e5/$r/
done
ssh root@$RUNE_VM_HOST 'cd /root/rune-e5/rune-client-cpp && \
  cmake -S . -B build/Release/build/Release > /tmp/sic-cfg.log 2>&1 && \
  cmake --build build/Release/build/Release -j4 2>&1 | tail -2 && \
  ctest --test-dir build/Release/build/Release -R Regression --output-on-failure 2>&1 | tail -3 && \
  ctest --test-dir build/Release/build/Release -j2 --timeout 300 2>&1 | tail -3'
```
Expected: build `[100%]`; `100% tests passed` for the regression suite and the full suite — on Linux `CpuAffinity.PinsTheCallingThreadOnLinux` runs (not skipped) and passes.

- [ ] **Step 3: Write the VM end-to-end script**

`/tmp/sic-e2e.sh` (local file, copied to the VM):

```bash
#!/usr/bin/env bash
# sic-e2e.sh — sharded-invoker client end-to-end on the VM.
#   default = legacy env path (thread-mode Client, today's defaults)
#   invoker = open-loop invoker shard (--rate)
# Brokers: legacy 1-node + 3-node always; shard 1-node + 3-node when the broker
# plan's shard mode is in the harness.
set -uo pipefail
cd /root/rune-e5/rune-broker/bench/kafka-compare
set +u; source env.sh; source lib.sh; source cluster.sh; set -u
P=/root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_produce_throughput
fail=0

default_run() {  # label port acks
  rm -rf /dev/shm/sic-def
  local out
  out="$(RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:$2" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40900" \
    RUNE_DATA_DIR=/dev/shm/sic-def RUNE_TOPICS="bench:0:8" RUNE_BENCH_RECORDS=300000 RUNE_BENCH_WARMUP=10000 \
    RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS="$3" RUNE_BENCH_WINDOW=4096 RUNE_LINGER_US=0 \
    timeout 300 "$P" 2>/dev/null | grep '^RESULT')"
  echo "$1 default: $out"
  echo "$out" | grep -q "succeeded=300000 failed=0" || fail=1
}

invoker_run() {  # label port acks [extra env]
  rm -rf /dev/shm/sic-inv
  local out
  out="$(env ${4:-} RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:$2" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40910" \
    RUNE_DATA_DIR=/dev/shm/sic-inv RUNE_TOPICS="bench:0:8" \
    timeout 300 "$P" --rate 20000 --duration-s 20 --warmup-s 2 --value-bytes 1024 --acks "$3" 2>/tmp/sic-inv.err \
    | grep -E '^(RESULT|THREAD)')"
  echo "$1 invoker: $out"
  echo "$out" | grep -q "errors=0" || fail=1
}

rune_start_broker 2 "bench:8:tier=leader" uring nvme || exit 1
default_run "1node-legacy" "$RUNE_CLIENT_PORT" leader
invoker_run "1node-legacy" "$RUNE_CLIENT_PORT" leader
rune_stop_broker

LEADER="$(rune_start_cluster 1 "bench:8:tier=majority:durability=all-in-sync" uring nvme)" || exit 1
default_run "3node-legacy" "$((40200 + LEADER))" all
invoker_run "3node-legacy" "$((40200 + LEADER))" all
rune_stop_cluster

if grep -q RUNE_WORKER_INGRESS_BASE_PORT cluster.sh; then
  export RUNE_WORKER_MODE=shard RUNE_RUNTIME_PLACEMENT=fused
  LEADER="$(rune_start_cluster 1 "bench:8:tier=majority:durability=all-in-sync" uring nvme)" || exit 1
  default_run "3node-shard" "$((40200 + LEADER))" all
  invoker_run "3node-shard" "$((40200 + LEADER))" all "RUNE_MN_DEBUG=1"
  # Feature actually active (§1a.10): the client routed to worker ingress endpoints.
  grep -q "worker='127.0.0.1:" /tmp/sic-inv.err || { echo "ingressEndpoint NOT used"; fail=1; }
  rune_stop_cluster
  if grep -q RUNE_WORKER_INGRESS_BASE_PORT lib.sh; then
    rune_start_broker 2 "bench:8:tier=leader" uring nvme || exit 1
    default_run "1node-shard" "$RUNE_CLIENT_PORT" leader
    invoker_run "1node-shard" "$RUNE_CLIENT_PORT" leader
    rune_stop_broker
  else
    echo "SHARD_1NODE_UNAVAILABLE: broker plan has not added per-worker ports to lib.sh"; fail=1
  fi
  unset RUNE_WORKER_MODE RUNE_RUNTIME_PLACEMENT
else
  echo "SHARD_BROKER_UNAVAILABLE: broker plan shard mode not in cluster.sh yet"; fail=1
fi
echo "E2E_EXIT=$fail"
exit $fail
```

- [ ] **Step 4: Run it**

```bash
scp /tmp/sic-e2e.sh root@$RUNE_VM_HOST:/root/rune-e5/sic-e2e.sh
ssh root@$RUNE_VM_HOST 'bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh && bash /root/rune-e5/sic-e2e.sh 2>&1 | tail -20'
```
Expected: every `default:` line shows `succeeded=300000 failed=0`; every `invoker:` line shows `errors=0` and `achieved_rps≈20000`; `E2E_EXIT=0`. While the broker plan has not landed, the legacy half must pass and the script ends with `SHARD_BROKER_UNAVAILABLE ... E2E_EXIT=1` — this task is then NOT done (§1a.7): keep it open and re-run Step 4 when the broker plan lands.

- [ ] **Step 5: DR matrix with this client (spec §6.3)**

The dockerized DR matrix lives in `rune-broker/dr-e2e` and drives `dr_client`. With the broker plan's shard mode on, rebuild its client image from this branch (tag it with this commit's SHA, never `latest`), run the full matrix per the broker plan's DR step, and require it green in both `RUNE_WORKER_MODE=legacy` and `shard` before flipping any default (spec §7).

```bash
cd /Users/vradu/Projects/rune-client-cpp && git rev-parse --short HEAD   # image tag for dr_client
```
Expected: the broker plan's DR matrix reports all scenarios passed with the client image `rune-client:<sha>`.

- [ ] **Step 6: Interleaved A/B of the default path (main vs branch), n = 15**

```bash
cd /Users/vradu/Projects/rune-client-cpp
git worktree add /tmp/sic-main main
rsync -az --exclude=.git /tmp/sic-main/ root@$RUNE_VM_HOST:/root/rune-e5/rune-client-cpp-main/
git worktree remove /tmp/sic-main
ssh root@$RUNE_VM_HOST 'cd /root/rune-e5/rune-client-cpp-main && \
  cmake -S . -B build-rel -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON -DBUILD_TESTING=OFF \
    -DCMAKE_TOOLCHAIN_FILE=/root/rune-e5/rune-client-cpp/build/Release/build/Release/generators/conan_toolchain.cmake \
    > /tmp/sic-main-cfg.log 2>&1 && cmake --build build-rel -j4 --target rune_produce_throughput 2>&1 | tail -1'
```

`/tmp/sic-ab.sh`:

```bash
#!/usr/bin/env bash
# sic-ab.sh — interleaved ABAB (n=15) of the DEFAULT client path on one 3-node cluster.
set -uo pipefail
cd /root/rune-e5/rune-broker/bench/kafka-compare
set +u; source env.sh; source lib.sh; source cluster.sh; set -u
A=/root/rune-e5/rune-client-cpp-main/build-rel/rune_produce_throughput
B=/root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_produce_throughput
LEADER="$(rune_start_cluster 1 "bench:8:tier=majority:durability=all-in-sync" uring nvme)" || exit 1
for i in $(seq 1 15); do
  for label in A B; do
    bin=$A; [ "$label" = B ] && bin=$B
    rm -rf /dev/shm/sic-ab
    echo -n "$i $label "
    RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:$((40200 + LEADER))" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40920" \
    RUNE_DATA_DIR=/dev/shm/sic-ab RUNE_TOPICS="bench:0:8" RUNE_BENCH_RECORDS=300000 RUNE_BENCH_WARMUP=20000 \
    RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS=all RUNE_BENCH_WINDOW=4096 RUNE_LINGER_US=0 RUNE_IDLE_MODE=busyspin \
      taskset -c 0 timeout 300 "$bin" 2>/dev/null | grep '^RESULT' || echo "RUN FAILED"
  done
done
rune_stop_cluster
```

```bash
/Users/vradu/Projects/rune-client-cpp/cmake-build-sic-rel/rune_produce_throughput --idle-check-only --idle-check-cores 0,1,2,3 || echo "local check n/a"
scp /tmp/sic-ab.sh root@$RUNE_VM_HOST:/root/rune-e5/sic-ab.sh
ssh root@$RUNE_VM_HOST 'bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh && /root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_produce_throughput --idle-check-only --idle-check-cores 0,1,2,3 && bash /root/rune-e5/sic-ab.sh' | tee /tmp/sic-ab.out
grep -c "RUN FAILED" /tmp/sic-ab.out
```
Expected: `IDLE_CHECK ... -> PASS` first (else stop: the box is not idle — find and stop the foreign load; never blame "server noise"); 30 RESULT lines, `RUN FAILED` count 0, every run `failed=0`. Validate before interpreting (§1a.10): same two binaries throughout, all 30 captures complete. Compare medians of `records_per_sec`, `p50_us`, `p99_us` (A vs B): B must be ≥ A on throughput and ≤ A on p99. A worse B is a bug in this branch's default path: bisect by task commit on the same A/B harness, cite the exact code path, fix with a regression test, re-run. Push both medians:

```bash
AB_STATUS=$( [ "$(grep -c '^[0-9]* [AB] RESULT' /tmp/sic-ab.out)" = 30 ] && echo passed || echo failed )
/Users/vradu/Projects/rune-broker/scripts/rune-live.sh test "rune-client/sharded-invoker-ab" "$AB_STATUS" \
  runs=30 commit=$(git -C /Users/vradu/Projects/rune-client-cpp rev-parse --short HEAD)
```

- [ ] **Step 7: Spec §1 pass criterion (needs the broker plan's shard mode + fused runtime)**

Layout (spec §5): client shard pinned to core 0; broker i (shard worker 0, runtime fused) pinned to core i+1 — the broker plan's harness pins the brokers. Verified-idle box first.

```bash
ssh root@$RUNE_VM_HOST 'set -e
bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh
P=/root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_produce_throughput
$P --idle-check-only --idle-check-cores 0,1,2,3
cd /root/rune-e5/rune-broker/bench/kafka-compare
set +u; source env.sh; source lib.sh; source cluster.sh
export RUNE_WORKER_MODE=shard RUNE_RUNTIME_PLACEMENT=fused
LEADER="$(rune_start_cluster 1 "bench:8:tier=majority:durability=all-in-sync" uring nvme)"
rm -rf /dev/shm/sic-pass
RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:$((40200 + LEADER))" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40930" \
RUNE_DATA_DIR=/dev/shm/sic-pass RUNE_TOPICS="bench:0:8" \
  $P --mode open --rate 100000 --duration-s 60 --warmup-s 5 --value-bytes 1024 --acks all \
     --shards 1 --core 0 --gate-p99-us 100 --gate-rps 100000
echo "PASS_EXIT=$?"
rune_stop_cluster' | tee /tmp/sic-pass.out
```
Expected: `IDLE_CHECK ... -> PASS`, `THREAD shard=0 core=0 ...`, `RESULT mode=open ... errors=0 ... achieved_rps>=100000 ... p99_us<=100`, `GATE ... -> PASS`, `PASS_EXIT=0`. A `GATE ... -> FAIL` is a stop-and-fix (§1a.10, §1a.11): re-run with `RUNE_FR_ENABLE=1 RUNE_FR_OUT=/tmp/frsic` on client and brokers, localise the hop from the traces, fix with a regression test and repeat — never report the failing number as a result. Push the outcome:

```bash
PASS_STATUS=$(grep -q "GATE .* -> PASS" /tmp/sic-pass.out && echo passed || echo failed)
/Users/vradu/Projects/rune-broker/scripts/rune-live.sh test "rune-client/pass-criterion" "$PASS_STATUS" \
  commit=$(git -C /Users/vradu/Projects/rune-client-cpp rev-parse --short HEAD)
```

- [ ] **Step 8: Close out**

```bash
cd /Users/vradu/Projects/rune-client-cpp
git status --short             # clean except the untracked conan_provider.cmake
git log --oneline main..HEAD   # one commit per task
git stash list                 # the parked RUNE_RETARGET_DBG stash is still there
ssh root@$RUNE_VM_HOST 'systemctl is-active k3s || echo "k3s not running"'
```
Leave the `RUNE_RETARGET_DBG` stash untouched and report that it exists (it targets the pre-refactor `retarget()` loop and will not apply cleanly on this branch). The VM is the 4-core bench box of spec §1 with k3s stopped for benching: once the bench session is over, restore it with `ssh root@$RUNE_VM_HOST 'systemctl start k3s'` (a running k3s invalidates any later bench number, so this is the very last step). Do not push the branch.

---

## Self-Review

**1. Spec coverage (section → task):**

| Spec | Requirement | Task(s) |
|---|---|---|
| §1 | pass criterion: open-loop 100k rec/s × 60 s, p99 ≤ 100 µs, every record, CO-safe | 17, 18, 19 (Step 7) |
| §1a.1–2 | shared-nothing; rings only | 10 (ack/Tier-2 ownership through the ring, `ackMtx_`/`tier2Mtx_` removed), 12 (`txnHubMtx_` removed, txn registration through the ring), 13 (ring consumer heartbeat, no new shared flag), 15 (ManyToOne kept only where multi-producer) |
| §1a.3 | invoker modes, one injected clock per cycle | 6, 12 (`nanoClock`, `dutyCycle(nowNs)`), 14 |
| §1a.4 | rich-state machines | 5 (`Resolution`), 7 (every resolve path) |
| §1a.5 | no alloc / `std::function` / node containers / virtuals on hot paths | 4, 8, 9, 10, 11, 12, 14 (alloc-counter tests in 9 and 14) |
| §1a.6, §1a.10 | before/after proof, interleaved A/B, validated captures | 8 (accum A/B), 9 (alloc before/after), 19 (Steps 6–7) |
| §1a.7 | regression-first, real e2e default + flag-on | every task's run order; 18 Step 5 (local broker), 19 Steps 1–5 |
| §1a.8 | injected clock, capped loops | 5, 6, 9–16 (iteration-capped drive loops, frozen/advanced `nanoClock`) |
| §1a.9 | time-independent convergence | 5 (retry after timeout, never give up on a timeout), 16 (re-offer on route change, not on a timer) |
| §1a.12 | fix on sight | 1 (codegen), 2, 6 (`::exit(-1)` handler), 8 (accum bench, partition map), 9 (ackTail drift, Tier-2 clock), 10 (mixed-batch offsets, expired-then-written), 11 (template poll, per-ack observer), 12 (txn mutex, per-cycle backstop, invoker registration spin) |
| §3.3 | `ingressEndpoint` per partition; session on node | 11 (`setRoute(session, data)`), 16 |
| §3.4 | acks from a worker publication, same correlation ids | 9 (direct batch-id match), 11 (buffer), 16 (worker-publication test) |
| §3.6 | own-driver pumping instead of sleep loops; one driving thread; no same-thread waits | 2, 5, 6, 7, 12 (`submitRegistration_`), 14 (re-entrancy) |
| §4.1 | `ClientShard`, own invoker driver + invoker conductor, explicit MTU 8 KiB, shard count default 1, deterministic map, invoker + agent modes | 3, 6, 12, 14, 15 |
| §4.2 | batching without linger in invoker mode | 14 (end-of-pass flush), 15 (`invokerLingerUs = 0`, one-frame-per-pass test) |
| §4.3 row 1 | getenv / string concat / string-keyed lookup per routed offer | 11 |
| §4.3 row 2 | `std::function` SendFn per `doWork` / per `flushBatches` | 8, 12 |
| §4.3 row 3 | `BatchInfo.wireFrame` heap copy + `std::map` | 9 |
| §4.3 row 4 | per-record map + `ackMtx_`; per-batch callback; per-record API on top | 10 |
| §4.3 row 5 | `void send` never drops | 13 (thread mode), 14 (invoker mode) |
| §4.3 row 6 | driver sleep-ns idle / 1408 MTU / two-thread pumping | 3 (idle + MTU config), 6 (invoker), 2 |
| §4.4 | O(1) ack match by correlation id → batch callback | 9, 10 |
| §5 | open-loop `--rate`, scheduled-time latency, full histogram, saturation mode, idle check, core layout + per-thread CPU, client pinned to core 0 | 12 (`pinCurrentThreadToCore`), 17, 18, 19 |
| §6.1 | unit/regression incl. invoker no-deadlock, single-thread driver, flat ack ring, send never drops | 2, 6, 7, 9, 10, 13, 14 |
| §6.2–6.5 | in-process integration, DR matrix + TSan, VM on/off, pass run | 10/12/13 (TSan), 11–16 (in-process with `InvokerGateway`), 19 |
| §7 | defaults off; bench backoff committed in the first step | Global Constraints, 1 |

Gaps: none on the client side. Tasks 16 (Step 1 gate) and 19 (Steps 4–5, 7) depend on the broker plan's protocol field and shard mode; both stop with an explicit note instead of faking the dependency.

**2. Placeholder scan:** searched for "TBD", "TODO", "implement later", "fill in", "similar to Task", `<median>`, `<passed|failed>` — none remain; every code step carries the code, every run step the command and the expected output.

**3. Name / type consistency (defined → used):** `TransportConfig`/`DriverMode`/`validate`/`applyEnvOverrides` (3 → 6, 12); `FlatIndexMap` (4 → 7, 8, 11); `Resolution`/`ResolveStatus` (5 → 7, 11); `dutyCycle`/`closed`/`pumpUntil_`/`monoNs_` (6 → 7, 11, 12); `PubHandle`/`kNoPub`/`publicationHandle`/`offerOn`/`publicationReady`/`SubSlot` (7 → 11); `ProducerImpl::SendFn`/`AgentProduceSink`/`CountingSink`/templated `flushBatches`/`retransmitStalled`/`drainAndFlush` (8 → 9, 12, 13, 14, 16); `batchcorr::*`/`InFlightBatch`/`InFlightRing`/`setOrdinal`/`reclaimAckRing_`/`failBatch_` (9 → 10, 16); `PendingAck::complete`/`failUnsubmitted`/`failBatch`/`BatchAck`/`onBatchAck`/`collectBatchAcks`/`drainBatchAcks`/`completeSlice_`/`emitBatchAck_` (10 → 13, 15, 18); `ClientAgent::setRoute(4-arg)`/`dataChannelFor`/`ProduceAck`/`produceAcks`/`clearProduceAcks`/`setForwardProduceResponses`/`transportClosed`, `test_support::InvokerGateway` (11 → 12, 14, 15, 16); `RunnerMode`/`NanoClock`/`runnerCore`/`aeronClientLivenessTimeoutNs`/`Client::doWork`/`dutyCycle`/`inDutyCycle_`/`ownerThread_`/`now()`/`pinCurrentThreadToCore`/`currentCore` (12 → 14, 15, 18); `SendResult`/`trySend`/`trySubmit`/`sendBlocking` (13 → 14, 15, 18); `enableDirectIngress`/`DirectIngress`/`pumpForProducer_`/`appendRecord_` (14); `shardFor`/`channelWithPortOffset`/`ShardMode`/`ShardGroupConfig`/`shardClientConfig`/`shardProducerConfig`/`ClientShard`/`ShardGroup`/`implForInternalUse` (15 → 18); `resendPartitionViaRoute`/`testDataChannelFor` (16); `LogLinearHistogram`/`OpenLoopSchedule`/`SeqTimestampRing`/`ProcStat` helpers (17 → 18); `BenchArgs`/`parseBenchArgs`/`invokerBenchMain` (18). The Task 9 helper `resolveSlice_` is intentionally replaced by `completeSlice_` in Task 10, and `ProducerImpl::failPending` by `failBatch` / `failUnsubmitted` (the hub keeps `failPending` as its public name).

**4. Review Focus placement:** (1) owner stall → Task 6 Step 1 + Task 15 Step 1; (2) void send inside a duty-cycle callback → Task 14 Step 1; (3) mixed void + callback batch → Task 10 Step 1; (4) duplicate / stale acks → Task 9 Step 1 + Task 10 Step 1; (5) endpoint move with batches in flight → Task 16 Step 2. Each is a named test in its owning task.

**Deliberate decisions recorded for the executor:**
- Two tests (Task 6, Task 15) wait on real time: Aeron's `ClientConductor` inter-service timeout reads Aeron's own epoch clock (`ClientConductor.h:436`), which cannot be injected (`Aeron.cpp:43`). Neither sleeps a tuned duration: each yields on `aeron::currentTimeMillis()` until that exact condition holds (F8c C9 fix round 1). Every other new test is iteration-capped with no sleeping, including the agent-mode `ShardGroup` test, which yields between bounded gateway-pump rounds (F8c C9).
- The void `send` now throws on invalid input (value too large, invalid topic) instead of silently dropping; `TypedTopic::send` loses `noexcept` accordingly and `ProducerTest.OversizedValueRejected` asserts the new contract.
- Client-specific env names (`RUNE_CLIENT_AERON_MTU`, `RUNE_CLIENT_AERON_DRIVER_IDLE`) keep broker tuning exported by the VM scripts (`RUNE_AERON_MTU`, `RUNE_AERON_DRIVER_IDLE`) from silently changing the client; `RUNE_AERON_SOCKET_BUFFER_BYTES` keeps its historical client meaning.
