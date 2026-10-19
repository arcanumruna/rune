// ProducerIdScan benchmark.
//
// scanMaxProducerId() replays the committed log ([logStart, highWatermark)) in
// 64 MiB chunks, parses every RecordBatch header, and tracks the max producerId
// — used at recovery to reseed the producer-id allocator.  Measures scan
// throughput over a pre-built in-memory log, parameterised by batch count.

#include <benchmark/benchmark.h>
#include <rune/storage/ProducerIdScan.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <vector>

namespace {

using rune::storage::InMemoryLog;
using rune::storage::scanMaxProducerId;

// BM_ProducerIdScan_Scan — scan a log of `n` single-record idempotent batches
// (producerId increases with offset) up to the high watermark.
static void BM_ProducerIdScan_Scan(benchmark::State& state) {
    const int n = static_cast<int>(state.range(0));

    // Ring large enough to hold all n batches without eviction.
    InMemoryLog log(64ull * 1024 * 1024, static_cast<std::size_t>(n) + 16);
    for (int i = 0; i < n; ++i) {
        rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), 1000LL + i);
        b.setProducerId(static_cast<int64_t>(i));   // strictly increasing PID
        b.append("k", "v", 1000LL + i);
        auto data = b.build();
        log.append(data.data(), data.size(), static_cast<int64_t>(i));
    }
    const int64_t hw = log.logEndOffset();

    for (auto _ : state) {
        int64_t maxPid = scanMaxProducerId(log, hw);
        benchmark::DoNotOptimize(maxPid);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * n);
    state.counters["batches"] = benchmark::Counter(static_cast<double>(n));
}
BENCHMARK(BM_ProducerIdScan_Scan)->Arg(1024)->Arg(16384)->Unit(benchmark::kMicrosecond);

} // namespace
