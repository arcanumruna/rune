// PartitionApi consume benchmarks.
//
// BM_Produce already exists in StorageBenchmark.cpp; this file adds
// throughput-oriented consume benchmarks and a produce-consume round-trip.
//
// All pre-built state is set up OUTSIDE the timed loop.  Parameterised on N,
// the number of records that are pre-written to the partition before the timed
// iterations begin.
//
// Batch shape: ~48-byte single-record MMORPG entity-position event produced
// via PartitionApi::produce("pos", "x:1234,y:5678,z:9012", ts).

#include <benchmark/benchmark.h>
#include <rune/storage/PartitionApi.h>

#include <filesystem>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>

namespace {

// ---------------------------------------------------------------------------
// BM_PartitionApi_Consume
//
// Pre-build a partition with N = state.range(0) records outside the loop.
// Each iteration consumes from offset 0 (full scan) with maxBytes = 1 MiB.
// Exercises the decode path for a multi-record batch fetch starting at the
// head of the log.
// ---------------------------------------------------------------------------
static void BM_PartitionApi_Consume(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int N = static_cast<int>(state.range(0));

    auto dir = fs::temp_directory_path() /
               ("rune_bench_partition_consume_" + std::to_string(N));
    fs::remove_all(dir);
    fs::create_directories(dir);

    // Pre-build partition OUTSIDE the timed loop.
    auto api = std::make_unique<rune::storage::PartitionApi>(dir.string());
    for (int i = 0; i < N; ++i) {
        api->produce("pos", "x:1234,y:5678,z:9012",
                     1700000000000LL + static_cast<int64_t>(i));
    }

    for (auto _ : state) {
        auto records = api->consume(/*fromOffset=*/0, /*maxBytes=*/1 << 20);
        benchmark::DoNotOptimize(records);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(N));
    state.counters["records"] = benchmark::Counter(static_cast<double>(N));
    fs::remove_all(dir);
}

BENCHMARK(BM_PartitionApi_Consume)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_PartitionApi_ConsumeTail
//
// Pre-build a partition with N records.  Each iteration consumes near the
// high-watermark (last record only) — exercises the tail-fetch hot path
// typical of a follower catching up one batch at a time.
// ---------------------------------------------------------------------------
static void BM_PartitionApi_ConsumeTail(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int N = static_cast<int>(state.range(0));

    auto dir = fs::temp_directory_path() /
               ("rune_bench_partition_ctail_" + std::to_string(N));
    fs::remove_all(dir);
    fs::create_directories(dir);

    auto api = std::make_unique<rune::storage::PartitionApi>(dir.string());
    for (int i = 0; i < N; ++i) {
        api->produce("pos", "x:1234,y:5678,z:9012",
                     1700000000000LL + static_cast<int64_t>(i));
    }

    // Tail offset: last record offset = N-1 (zero-based).
    const int64_t tailOffset = static_cast<int64_t>(N - 1);

    for (auto _ : state) {
        auto records = api->consume(tailOffset, /*maxBytes=*/1 << 20);
        benchmark::DoNotOptimize(records);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["records"] = benchmark::Counter(static_cast<double>(N));
    fs::remove_all(dir);
}

BENCHMARK(BM_PartitionApi_ConsumeTail)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_PartitionApi_ProduceConsumeRoundTrip
//
// Each iteration: produce one record, then consume from the tail offset that
// was just written.  Models the single-record latency path (write-then-read).
// N is used only to size an initial pre-fill so the partition is non-empty;
// the timed loop always produces+consumes exactly one additional record.
// ---------------------------------------------------------------------------
static void BM_PartitionApi_ProduceConsumeRoundTrip(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int N = static_cast<int>(state.range(0));

    auto dir = fs::temp_directory_path() /
               ("rune_bench_partition_roundtrip_" + std::to_string(N));
    fs::remove_all(dir);
    fs::create_directories(dir);

    auto api = std::make_unique<rune::storage::PartitionApi>(dir.string());
    // Pre-fill so the log is non-trivial.
    for (int i = 0; i < N; ++i) {
        api->produce("pos", "x:1234,y:5678,z:9012",
                     1700000000000LL + static_cast<int64_t>(i));
    }

    int64_t ts = 1700000000000LL + N;

    for (auto _ : state) {
        int64_t off = api->produce("pos", "x:1234,y:5678,z:9012", ts++);
        benchmark::DoNotOptimize(off);

        auto records = api->consume(off, /*maxBytes=*/1 << 20);
        benchmark::DoNotOptimize(records);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["prefill"] = benchmark::Counter(static_cast<double>(N));
    fs::remove_all(dir);
}

BENCHMARK(BM_PartitionApi_ProduceConsumeRoundTrip)
    ->Arg(0)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond);

} // namespace
