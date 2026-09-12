# BR-8 (part 1) — Gateway Ping Round-Trip Latency Benchmark

## Status
DONE — benchmark implemented, built, run, full test suite passes.

## Deliverables
- `benchmark/GatewayBench.cpp` — new file; `GatewayBenchFixture/PingRoundtrip`
- `CMakeLists.txt` — `GatewayBench.cpp` added to `rune_broker_benchmark` sources; `rune_gateway` + `aeron::aeron` added to its link libraries

## Measured Latency (aeron:ipc loopback, debug build)

| Metric   | Value (µs) |
|----------|------------|
| p50      | 1.000      |
| p99      | 1.916      |
| p99.9    | 3.708      |
| mean     | 1.069      |

Measured on: macOS 25.3.0 (x86_64), 18-core M-series at 2400 MHz, debug build, 1 299 325 iterations over ≥1 s.

> Note: debug build (`***WARNING*** Library was built as DEBUG. Timings may be affected.`). Numbers are indicative; a release build will show lower latency and tighter tails. The sub-2 µs p99 on IPC is consistent with expected Aeron shared-memory performance even in a debug binary.

## Implementation Notes

### Harness design
Follows `GatewayLoopbackTest.cpp` exactly:
- One embedded `aeron_driver_t` (SHARED threading mode, sleep-ns idle, sparse term buffers, unique temp dir via `getpid()` + atomic counter).
- One `aeron::Aeron` C++ client shared by gateway and harness.
- `ClientGateway` subscribes on `aeron:ipc` / stream 101 (ingress).
- Harness: one `Publication` to ingress + one `Subscription` on egress (stream 200) + `FragmentAssembler`.
- One-time Connect handshake in `SetUp()` yields `sessionId_`.
- Warm-up: 50 Pings discarded before the timed loop.
- Timed loop: `UseManualTime()` + `SetIterationTime()` per ping; p50/p99/p99.9/mean counters computed from raw sample vector and emitted via `state.counters[...]`.

### Key constraint adherence
| Constraint | Implementation |
|------------|----------------|
| Bounded pump | Hard cap `kPumpCap = 100 000` on offer retry + Pong wait; `state.SkipWithError` on exceed |
| No busy-spin | `sleep_for(1µs)` between pump ticks in hot loop; `sleep_for(100µs)` in setup loops |
| Unique temp path | `getpid()` + global atomic counter → `/tmp/rune_gw_bench_<pid>_<seq>` |
| Clean teardown | TearDown() resets `ingressPub_`, `egressSub_`, `egressAsm_` → `gw_` → `aeron_` → driver (in that order); then `remove_all()` on temp dir |
| No production code changes | Only `benchmark/GatewayBench.cpp` + `CMakeLists.txt` modified |

### Fixture choice: `benchmark::Fixture` (not plain function)
The initial implementation used a plain function-scoped RAII class. Google Benchmark's calibration phase calls the benchmark function multiple times to determine iteration count. On macOS, tearing down and immediately recreating an Aeron client+driver caused `recursive_mutex lock failed: EINVAL` in the new client-conductor — a known macOS `pthread` ordering hazard when shared-memory regions are rapidly recycled. Switching to `BENCHMARK_DEFINE_F` / `benchmark::Fixture` (SetUp/TearDown model) moves teardown out of the hot calibration loop. The fixture is created once per benchmark invocation, eliminating the crash.

### Root cause of initial crash (noted for review)
`ingressPub_` and `egressSub_` (`shared_ptr<Publication>` / `shared_ptr<Subscription>`) hold internal references into the Aeron `ClientConductor`. If those shared_ptrs are not explicitly reset BEFORE `aeron_.reset()`, the Aeron destructor (`~Aeron()`) doesn't join the conductor thread (the ref count is still > 1). The conductor thread remains alive into `stopDriver()`, which calls `aeron_driver_close()` on the C driver while the conductor is still polling. On the next fixture creation, the recycled memory contains a corrupted `pthread_mutex_t` (not re-initialized), causing the EINVAL. Fixed in TearDown() by explicitly resetting pub/sub/asm → gw → aeron → driver in that order. This is not a gateway production bug — it is a test-harness lifecycle ordering issue.

## Test Suite
All 1679 tests pass (`ctest --timeout 240`, 49 s wall time).

## No production code modified
`ClientGateway.cpp`, `ClientGateway.h`, all `libs/node/*`, all `libs/gateway/*` sources are untouched.

## Concerns / Observations (for BR-8 part 2 / full-area review)
1. **Debug-only build**: The conan profile builds debug; release numbers will differ. Recommend a release build for the Planka entry.
2. `ClientGateway::handleConnect` has a `// TODO(Milestone B)` synchronous `resolvePublication` on the redirect path that would block the consensus thread if a follower node ever returns a non-empty `leaderClientEndpoint`. This is documented in the source but would block the duty-cycle thread in a multi-node follower. Not a Milestone A concern per the brief (single-node is always leader), but flagged for Milestone B.
3. `sendOnPub` casts away `const` on the buf pointer (`const_cast<uint8_t*>(buf)`) to construct an `AtomicBuffer`. This is safe since Aeron never writes to the source buffer during `offer`, but it bypasses type safety. Minor.
