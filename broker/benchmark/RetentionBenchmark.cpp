// Retention benchmark.
//
// Measures the cost of Log::deleteOldSegments(), which scans all but the
// active segment and drops those whose cumulative size exceeds retainBytes.
// Because deletion is destructive (it removes segment files and the in-memory
// map entries) we rebuild the log under PauseTiming before each timed call so
// the measurement captures one realistic "compute + delete N" operation.
//
// Parameterised on the number of pre-built segments (state.range(0)).
// retainBytes is set to 0 so ALL sealed segments are deleted, giving the
// worst-case deletion sweep for that segment count.
//
// Batch shape: ~48-byte single-record MMORPG entity-position event.
// rollBytes = 4 KiB → roughly 80 batches per segment.

#include <benchmark/benchmark.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>

namespace {

static std::vector<uint8_t> makePosBatch(int64_t baseOffset, int64_t ts) {
    rune::protocol::RecordBatchBuilder b(baseOffset, ts);
    b.append("pos", "x:1234,y:5678,z:9012", ts);
    return b.build();
}

// ---------------------------------------------------------------------------
// BM_Retention_DeleteOldSegments
//
// Per-iteration protocol:
//   PauseTiming  → rebuild the log with `segCount` sealed segments
//   ResumeTiming → call deleteOldSegments(0) (retain nothing)
//   DoNotOptimize the returned count; SetItemsProcessed = segments deleted.
//
// rollBytes = 4 KiB; ~80 batches needed per segment.
// ---------------------------------------------------------------------------
static void BM_Retention_DeleteOldSegments(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int segCount = static_cast<int>(state.range(0));

    static constexpr int64_t kRollBytes      = 4 * 1024;   // 4 KiB
    static constexpr int     kBatchesPerSeg  = 82;          // ~82 × ~50 B > 4 KiB

    // One temp dir per parameterization.
    auto dir = fs::temp_directory_path() /
               ("rune_bench_retention_" + std::to_string(segCount));

    for (auto _ : state) {
        // ---- setup: rebuild log outside timed region ----
        state.PauseTiming();
        fs::remove_all(dir);
        fs::create_directories(dir);
        {
            rune::storage::Log log(dir.string(), kRollBytes);
            // Produce enough batches to create segCount sealed segments plus
            // one active segment (so there is something to delete).
            const int totalBatches = segCount * kBatchesPerSeg + kBatchesPerSeg;
            for (int i = 0; i < totalBatches; ++i) {
                auto batch = makePosBatch(static_cast<int64_t>(i),
                                         1700000000000LL + i);
                log.append(batch, static_cast<int64_t>(i));
            }
        }
        // Open a fresh Log over the populated directory.
        rune::storage::Log log(dir.string(), kRollBytes);
        state.ResumeTiming();

        // ---- timed region ----
        int deleted = log.deleteOldSegments(/*retainBytes=*/0);
        benchmark::DoNotOptimize(deleted);
        benchmark::ClobberMemory();
    }

    // Report throughput as segments-deleted-per-second.
    // Each iteration deletes exactly segCount sealed segments.
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(segCount));
    state.counters["segs_per_iter"] = benchmark::Counter(static_cast<double>(segCount));
    fs::remove_all(dir);
}

BENCHMARK(BM_Retention_DeleteOldSegments)
    ->Arg(4)
    ->Arg(16)
    ->Arg(64)
    ->Unit(benchmark::kMicrosecond);

} // namespace
