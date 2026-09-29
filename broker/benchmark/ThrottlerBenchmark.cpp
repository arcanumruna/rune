// Throttler benchmark.
//
// Throttler is the compaction cleaner's token-bucket I/O limiter.  The only
// meaningful thing to benchmark is the NON-throttling overhead — the fast-path
// no-op (unlimited) and the steady_clock + rate arithmetic on each call when
// the rate stays under the limit.  We deliberately do NOT benchmark the
// throttling path itself: it calls sleep_for, so timing it would just measure
// the sleep, not the code.

#include <benchmark/benchmark.h>
#include <rune/storage/Throttler.h>

#include <cstdint>

namespace {

using rune::storage::Throttler;

// BM_Throttler_Unlimited — maxBytesPerSec = -1 → the `<= 0` fast-path no-op.
static void BM_Throttler_Unlimited(benchmark::State& state) {
    Throttler t(-1);
    for (auto _ : state) {
        t.maybeThrottle(4096);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Throttler_Unlimited)->Unit(benchmark::kNanosecond);

// BM_Throttler_UnderLimit — a very high limit so the rate never exceeds it and
// no sleep is ever triggered; measures the clock read + rate-check arithmetic.
static void BM_Throttler_UnderLimit(benchmark::State& state) {
    Throttler t(static_cast<int64_t>(1) << 62); // effectively unbounded, but not the -1 fast path
    for (auto _ : state) {
        t.maybeThrottle(64); // tiny → rate stays far below the ceiling
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Throttler_UnderLimit)->Unit(benchmark::kNanosecond);

} // namespace
