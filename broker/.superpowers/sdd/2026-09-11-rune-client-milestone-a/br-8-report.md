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

---

# BR-8 (part 2) — Full-area review fix round (gateway + node runtime)

## Status
DONE — all 7 fixes (A-G) implemented; 3 new tests added; 1682/1682 pass.

## Per-fix confirmation

**FIX A** (No orphaned session on ConnectResponse back-pressure): Implemented minimum-acceptable path in `pollPendingConnects()`. On successful publication resolve, the ConnectResponse is attempted via `sendOnPub(*pub, ...)` BEFORE `sessions_.add()`. If the send fails (publication not yet connected / term buffer full), the `pub` goes out of scope (releasing the registration) and the pending entry is erased — no session created. If the send succeeds, `sessions_.add()` is called. Invariant: there is never a session the client was not told about.
- Files: `libs/gateway/src/ClientGateway.cpp` (pollPendingConnects resolve block)
- New test: `GatewayAsyncDropFixture.BackPressure_SendFails_NoOrphanSession` (GatewayAsyncDropTest.cpp): sends Connect with no egress subscriber → pub resolves but `isConnected()==false` → `sendOnPub` fails → no session, pending clears. Deterministic, bounded.

**FIX B** (Explicit `newLeaderId = nodeId_` in Leader* arms): Split Leader* from Closed in `updateObservedLeader()` switch. LeaderLogReplication/LeaderReplay/LeaderInit/LeaderReady now set `newLeaderId = nodeId_` explicitly. Closed retains `leaderMemberId()` (reached from both leader and follower path). Updated the comment above the switch to match.
- File: `libs/node/include/rune/node/NodeConsensusAgent.h`

**FIX C** (Drain-growth warning, no hard cap): Added a rate-limited warning at the start of `drainPendingReleases()`. Fires once when `drainingRegistrations_.size() > 4*kMaxPendingConnects`, resets when the table shrinks below the threshold. No hard cap added (correct per brief).
- File: `libs/gateway/src/ClientGateway.cpp` (drainPendingReleases)

**FIX D** (pendingConnects_ → std::deque): Changed member from `std::vector<PendingConnect>` to `std::deque<PendingConnect>` in header; added `#include <deque>`; removed `reserve()` call in constructor (deque doesn't need it); changed `erase(begin())` in cap-eviction to `pop_front()` for O(1). Mid-sequence erase(it) for resolve/timeout paths unchanged (deque supports iterator erase). All existing tests continue to pass.
- Files: `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp`

**FIX E** (4 comment/consistency nits):
- Nit 1: `ClientGateway.h` scratch comment corrected "64 KiB" → "68 KiB".
- Nit 2: `ClientGateway.cpp` `sendOnPub` `const_cast<uint8_t*>` — added one-line comment explaining the cast is safe (Aeron copies out, never writes back).
- Nit 3: `handlePing` changed to accept `nowNs` as explicit parameter (matching `handleConnect`/`handleKeepalive` siblings). Updated declaration in header, definition in .cpp, and call site in `onFragment`. Removed use of `nowNs_` member in `handlePing` body.
- Nit 4: `kResolveSliceMs`/`kResolveMaxMs` comment updated: now says "Used for: (a) non-leader redirect path, (b) ingress subscription resolve in constructor. NOT used for leader-accept path (async)."

**FIX F** (Idempotent lifecycle tests): Added `BrokerNodeTest.DtorAfterExplicitStop_NoCrash` (Test 9) to `libs/node/tests/node/BrokerNodeTest.cpp`. Verifies start→awaitLeader→stop()→dtor() does not double-free or hang. Tests 2 (StopIdempotent) and 7 (DoubleStart_Idempotent) were already present and cover the other FIX F scenarios.

**FIX G** (Table-driven NodeConsensusAgent leader-id mapping test): Added `BrokerNodeTest.NodeConsensusAgentLeaderMap_InitAndLeader` (Test 10) to `libs/node/tests/node/BrokerNodeTest.cpp`. Asserts: (1) before start(), leaderId()==-1 (Init/no-leader arm); (2) after self-election, leaderId()==nodeId_ (FIX B: explicit nodeId_ in Leader* arm). Validates the FIX B observable directly via BrokerNode's public API (smallest seam without over-building).

## New / changed test count
- New tests: 3 (GW-DR-4 `BackPressure_SendFails_NoOrphanSession`, BN-9 `DtorAfterExplicitStop_NoCrash`, BN-10 `NodeConsensusAgentLeaderMap_InitAndLeader`)
- Full suite: **1682 tests, 1682 pass, 0 fail** (`ctest --timeout 260`, 92.77 s wall time)

## Deferred (DEFER list — do NOT implement in Milestone A)

- **Missing-coverage #3** (never-resolving drain bound): requires a fake Aeron that never resolves `findExclusivePublication` — needs stub-Aeron harness (Milestone B).
- **Missing-coverage #4/#5** (findExclusivePublication/eviction ERRORED-throw branches): requires a stub Aeron conductor that can be programmed to throw — Milestone B.
- **Minor 2 full async-redirect**: routing the redirect egress through the async pending machine. Comment + assertion added at the BrokerNode `leaderView` call site in `BrokerNode.cpp::buildGateway()` explaining why the redirect-with-endpoint branch must not fire on the node path in M-A (Milestone-A ruling documented in-source).

## Concerns
None. The minimum-acceptable FIX A path (release pub on send failure, no retry sub-state) is simpler and avoids adding a `resolved_pub` field to `PendingConnect`. The trade-off is one additional Connect round-trip if back-pressure occurs at the moment of pub resolution. In practice this only fires when the client's egress subscription isn't connected yet — which is an extremely narrow window and the client's normal retry behaviour handles it.
