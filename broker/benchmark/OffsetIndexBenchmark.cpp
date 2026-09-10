// OffsetIndex benchmark.
//
// Measures append throughput and binary-search lookup latency for both a
// warm (tail) target and a cold (early) target.  Index construction is
// always outside the timed loop so that only the operation under test
// appears in the reported numbers.

#include <benchmark/benchmark.h>
#include <rune/storage/OffsetIndex.h>

#include <filesystem>
#include <cstdint>
#include <string>

namespace {

// Each OffsetIndex entry is 8 bytes (kEntrySize).  We size the file
// generously so we never hit the capacity limit during the build phase.
static constexpr int32_t kMaxIndexBytes = 8 * 1024 * 1024; // 8 MiB → 1 M entries cap

// Number of entries that fit in kMaxIndexBytes (kEntrySize = 8).
static constexpr int kMaxEntries = kMaxIndexBytes / 8;

// Build an OffsetIndex with `count` entries: offset = i, position = i * 100.
static rune::storage::OffsetIndex buildIndex(const std::string& path, int count) {
    rune::storage::OffsetIndex idx(path, /*baseOffset=*/0, kMaxIndexBytes);
    for (int i = 0; i < count; ++i) {
        idx.append(static_cast<int64_t>(i), static_cast<int32_t>(i) * 100);
    }
    return idx;
}

// ---------------------------------------------------------------------------
// BM_OffsetIndex_Append
//
// Sequential append into a pre-allocated index.  The counter wraps around
// when the mapping is full so the same physical capacity is exercised
// indefinitely (the file is truncated via resize-in-place each wrap).
// Only the append() store path is timed.
// ---------------------------------------------------------------------------
static void BM_OffsetIndex_Append(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "rune_bench_idx_append";
    fs::remove_all(dir);
    fs::create_directories(dir);
    auto path = (dir / "00000000000000000000.index").string();

    // Fresh index for each benchmark run; cap at kMaxEntries to avoid SIGSEGV.
    // We reconstruct it via PauseTiming on wrap so only append() is timed.
    rune::storage::OffsetIndex* idx =
        new rune::storage::OffsetIndex(path, /*baseOffset=*/0, kMaxIndexBytes);

    int64_t off = 0;
    int32_t pos = 0;
    for (auto _ : state) {
        if (off >= kMaxEntries) {
            // Wrap: recreate index on same path (reset entries_ to 0)
            state.PauseTiming();
            delete idx;
            fs::remove_all(dir);
            fs::create_directories(dir);
            idx = new rune::storage::OffsetIndex(path, /*baseOffset=*/0, kMaxIndexBytes);
            off = 0; pos = 0;
            state.ResumeTiming();
        }
        idx->append(off, pos);
        benchmark::DoNotOptimize(off);
        ++off;
        pos += 100;
    }

    delete idx;
    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}

BENCHMARK(BM_OffsetIndex_Append)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_OffsetIndex_LookupWarm
//
// Binary-search lookup of the TAIL (highest) offset in a pre-built index.
// This exercises the common hot path: the search low hint from the previous
// lookup is at/near the tail, so few comparisons are needed.
// Parameterised: 1 K and 100 K entries.
// ---------------------------------------------------------------------------
static void BM_OffsetIndex_LookupWarm(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int count = static_cast<int>(state.range(0));

    auto dir = fs::temp_directory_path() / ("rune_bench_idx_lwarm_" + std::to_string(count));
    fs::remove_all(dir);
    fs::create_directories(dir);
    auto path = (dir / "00000000000000000000.index").string();

    // Build index OUTSIDE the timed loop
    auto idx = buildIndex(path, count);
    const int64_t tailOffset = static_cast<int64_t>(count - 1);

    for (auto _ : state) {
        auto result = idx.lookup(tailOffset);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["entries"] = benchmark::Counter(static_cast<double>(count));
    fs::remove_all(dir);
}

BENCHMARK(BM_OffsetIndex_LookupWarm)
    ->Arg(1024)
    ->Arg(100000)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_OffsetIndex_LookupCold
//
// Binary-search lookup of an EARLY (near-zero) offset in a pre-built index.
// The search starts at a high hint (warm from a prior tail lookup) so it
// exercises the fall-back full-scan path — the slower, "cold" scenario.
// ---------------------------------------------------------------------------
static void BM_OffsetIndex_LookupCold(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int count = static_cast<int>(state.range(0));

    auto dir = fs::temp_directory_path() / ("rune_bench_idx_lcold_" + std::to_string(count));
    fs::remove_all(dir);
    fs::create_directories(dir);
    auto path = (dir / "00000000000000000000.index").string();

    // Build index OUTSIDE the timed loop; then do a tail lookup to warm the hint
    auto idx = buildIndex(path, count);
    // Warm the lastSearchLow to the tail by doing a tail lookup first
    (void)idx.lookup(static_cast<int64_t>(count - 1));

    const int64_t earlyOffset = 1; // very early: forces hint reset + full scan from left

    for (auto _ : state) {
        auto result = idx.lookup(earlyOffset);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["entries"] = benchmark::Counter(static_cast<double>(count));
    fs::remove_all(dir);
}

BENCHMARK(BM_OffsetIndex_LookupCold)
    ->Arg(1024)
    ->Arg(100000)
    ->Unit(benchmark::kNanosecond);

} // namespace
