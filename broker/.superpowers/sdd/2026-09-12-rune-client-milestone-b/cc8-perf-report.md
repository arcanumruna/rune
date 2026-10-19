# CC-8 Performance Report — Client Low-Latency Runner Idle Mode + True Hot-Path Ping Benchmark

**Date**: 2026-09-12  
**Branch**: `main` (local commits, no push)  
**Build**: Release (`build_release/`), Apple Clang / macOS x86_64, `-O2`

---

## Root Cause (confirmed)

The old p99 of 466µs had **two additive sources** — neither was the client's actual IPC hot path:

| Source | Contribution |
|--------|--------------|
| Gateway pump thread slept `kGwPollSleep = 200µs` between polls | Up to 200µs per RTT |
| Client runner used `BackoffIdleStrategy` (parks up to 1 ms) | Up to ~1 ms per RTT |

The broker's raw gateway RTT was already confirmed at p99 ~1.9µs (BR-8). The client overhead on top is ring hop + yield cycles + observer + future wake-up — not hundreds of microseconds.

---

## Fix 1 — `IdleMode` knob in `ClientConfig`

**Files changed**:
- `include/rune/client/Client.h` — added `IdleMode` enum and `idleMode` field to `ClientConfig`
- `src/Client.cpp` — `runLoop()` branches on `cfg_.idleMode`

```cpp
enum class IdleMode { Balanced, LowLatency };

struct ClientConfig {
    // ...existing fields...
    IdleMode idleMode = IdleMode::Balanced;
};
```

**Runner logic**:
- `IdleMode::Balanced` (default): existing `BackoffIdleStrategy` (spins → yields → parks ≤1 ms). CPU-friendly, unchanged behaviour.
- `IdleMode::LowLatency`: `YieldingIdleStrategy` — on each idle cycle calls `std::this_thread::yield()` instead of parking. No OS park → no wake-up latency. Runner reacts to submitted commands / inbound frames in ~µs. Cost: one CPU core pegged at ~100% while the runner is alive.

Both strategies are Aeron-native (`concurrent/YieldingIdleStrategy.h`, `concurrent/BackOffIdleStrategy.h`).

**Manual-poll path**: The `Client` public API is runner-thread-based. A manual-poll (`doWork()` inline) path is not exposed in CC-8; `IdleMode::LowLatency` achieves the required sub-100µs p99 in the runner-thread mode and is the deliverable.

---

## Fix 2 — Benchmark uses the TRUE hot path

**File changed**: `bench/ClientPingBench.cpp`

### Changes:
1. **Gateway pump thread**: removed `kGwPollSleep = 200µs`. Now uses `YieldingIdleStrategy` — tight-polls, yields between iterations, no sleep. Gateway reaction time: single-digit µs (negligible contribution to measured RTT).
2. **Client**: constructed with `cfg.idleMode = IdleMode::LowLatency`.
3. **Gateway send retries**: replaced `sleep_for(1ms)` with `std::this_thread::yield()` — no artificial delay on IPC back-pressure.
4. **Warm-up**: increased from 32 to 64 pings to allow branch predictor and IPC channels to reach steady state.
5. **Comments**: updated to document what the benchmark actually measures.

---

## Benchmark Results (Release build, LowLatency mode)

```
Platform : macOS 25.3, 18 × 2400 MHz (Intel x86_64)
Build    : Release (-O2), Apple Clang
Driver   : aeron:ipc loopback, embedded MediaDriver
Load avg : 9.08 (busy host — conservative numbers)

BM_PingRoundTrip  samples=447k  MinTime=3s
  p50   =  8.0 µs
  p99   = 34.3 µs   ← WELL UNDER 100µs  ✓
  p99.9 = 85.4 µs
  mean  =  9.6 µs
```

**p99 < 100µs: CONFIRMED** (34.3µs vs old 466µs — **13.6× improvement**).

Note: the host was under significant load (load avg 9.08 across 18 cores). On a quiet host, p99 would be expected in the 10–20µs range. Even under load, sub-100µs is met with margin.

### What the numbers represent

```
app-thread ping() → ring submit → runner (yield loop wakes in µs) →
transport tryClaim → aeron:ipc ring buffer (IPC, no syscall) →
gateway yield loop (wakes in µs) → pong encode → aeron:ipc →
runner yield loop → egress observer → promise.set_value() →
future.wait_until() → future.get() returns
```

All overhead is: ring hop (~1µs) + two yield-loop wake-up cycles (~1–5µs each) + IPC transfer (~1–2µs) + promise/future synchronisation (~1–3µs). Entirely consistent with 8–34µs at p50/p99.

---

## Test Results

All existing tests preserved; one new test added.

| Test binary | Tests | Result |
|---|---|---|
| `rune_client_tests` | 13 | PASSED |
| `rune_session_tests` | 61 | PASSED |
| `rune_transport_tests` | 5 | PASSED |
| `rune_agent_tests` | 12 | PASSED |
| `rune_client_api_tests` | **16** (was 15) | PASSED |
| **Total** | **107** | **ALL PASSED** |

**New test 16**: `Client.IdleModeBalancedAndLowLatencyBothConnectAndPing`
- Part A: `IdleMode::Balanced` — connect + ping resolves correctly.
- Part B: `IdleMode::LowLatency` — connect + ping resolves correctly.
- Both parts use the full in-process gateway harness.

---

## Constraints Satisfied

- [x] Single-writer preserved (ring submit is the only cross-thread path)
- [x] No busy-spin outside explicit `LowLatency` choice (`BusySpinIdleStrategy` not used; `YieldingIdleStrategy` yields, not spins)
- [x] Zero-alloc hot path (no heap allocations in `runLoop` inner loop)
- [x] Conan packages not built/published
- [x] `-j2` build flag used
- [x] Release build for benchmark numbers
- [x] All 107 tests green
- [x] p99 < 100µs confirmed (34.3µs)
