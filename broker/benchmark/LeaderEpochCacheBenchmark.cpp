// LeaderEpochCache benchmark.
//
// BM_LeaderEpochCache_Assign — sequential epoch assignment; wraps under
//   PauseTiming when the counter would overflow to keep measurements stable.
//
// BM_LeaderEpochCache_TruncateLookup — pre-build a cache with state.range(0)
//   epochs outside the timed loop; time truncateOffsetForEpoch on a mid epoch.
//   Exercises the std::map::upper_bound path that followers use during leader
//   epoch negotiation.

#include <benchmark/benchmark.h>
#include <rune/storage/LeaderEpochCache.h>

#include <cstdint>

namespace {

// ---------------------------------------------------------------------------
// BM_LeaderEpochCache_Assign
//
// Measures the cost of assign(epoch, startOffset) — a std::map insert/update.
// Epoch numbers advance monotonically; when they reach kMaxEpoch the cache is
// rebuilt under PauseTiming to reset state without ever exceeding int32_t.
// ---------------------------------------------------------------------------
static void BM_LeaderEpochCache_Assign(benchmark::State& state) {
    static constexpr int32_t kMaxEpoch = 1 << 20; // ~1 M reassigns before wrap

    rune::storage::LeaderEpochCache cache;
    int32_t epoch = 0;

    for (auto _ : state) {
        if (epoch >= kMaxEpoch) {
            state.PauseTiming();
            cache = rune::storage::LeaderEpochCache{};
            epoch = 0;
            state.ResumeTiming();
        }

        cache.assign(epoch, static_cast<int64_t>(epoch) * 100LL);
        benchmark::DoNotOptimize(epoch);
        ++epoch;
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_LeaderEpochCache_Assign)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_LeaderEpochCache_TruncateLookup
//
// Pre-build a cache with state.range(0) epochs (each mapped to offset = epoch
// * 100) outside the timed loop.  Each iteration calls
// truncateOffsetForEpoch(midEpoch) where midEpoch is the median epoch number,
// exercising the upper_bound path with a realistic non-trivial cache size.
// ---------------------------------------------------------------------------
static void BM_LeaderEpochCache_TruncateLookup(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));

    // Build outside the timed loop.
    rune::storage::LeaderEpochCache cache;
    for (int32_t e = 0; e < static_cast<int32_t>(N); ++e) {
        cache.assign(e, static_cast<int64_t>(e) * 100LL);
    }

    const int32_t midEpoch = static_cast<int32_t>(N / 2);

    for (auto _ : state) {
        int64_t result = cache.truncateOffsetForEpoch(midEpoch);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["epochs"] = benchmark::Counter(static_cast<double>(N));
}

BENCHMARK(BM_LeaderEpochCache_TruncateLookup)
    ->Arg(64)
    ->Arg(1024)
    ->Unit(benchmark::kNanosecond);

} // namespace
