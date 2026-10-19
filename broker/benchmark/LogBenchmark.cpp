// Log benchmark.
//
// Measures multi-segment append throughput (with forced segment rolling),
// cross-segment read latency, and multi-segment recover() cost.  All
// pre-built state is set up outside the timed loop.
//
// Batch shape matches LogSegmentBenchmark: ~48-byte single-record MMORPG
// entity-position event (key="pos", value="x:1234,y:5678,z:9012").

#include <benchmark/benchmark.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>

namespace {

static std::vector<uint8_t> makePoseBatch(int64_t baseOffset, int64_t ts) {
    rune::protocol::RecordBatchBuilder b(baseOffset, ts);
    b.append("pos", "x:1234,y:5678,z:9012", ts);
    return b.build();
}

// Build a multi-segment log with segmentRollBytes = rollBytes and `count`
// batches.  Returns the directory path; caller cleans up.
static std::filesystem::path buildLog(const std::string& tag,
                                      int count,
                                      int64_t rollBytes) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / ("rune_bench_log_" + tag);
    fs::remove_all(dir);
    fs::create_directories(dir);

    rune::storage::Log log(dir.string(), rollBytes);
    for (int i = 0; i < count; ++i) {
        auto batch = makePoseBatch(static_cast<int64_t>(i), 1700000000000LL + i);
        log.append(batch, static_cast<int64_t>(i));
    }
    return dir;
}

// ---------------------------------------------------------------------------
// BM_Log_AppendWithRolling
//
// Small segmentRollBytes (4 KiB) forces frequent segment creation — the
// realistic path when a broker is under sustained write load.  Batch
// construction is PauseTiming'd so only append() (including any roll) is
// measured.
// ---------------------------------------------------------------------------
static void BM_Log_AppendWithRolling(benchmark::State& state) {
    namespace fs = std::filesystem;
    static constexpr int64_t kRollBytes = 4 * 1024; // 4 KiB → rolls ~every 80 batches

    auto dir = fs::temp_directory_path() / "rune_bench_log_rolling";
    fs::remove_all(dir);
    fs::create_directories(dir);

    rune::storage::Log log(dir.string(), kRollBytes);

    int64_t nextOff = 0;
    std::vector<uint8_t> batch;

    for (auto _ : state) {
        state.PauseTiming();
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        int64_t result = log.append(batch, nextOff);
        benchmark::DoNotOptimize(result);
        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(batch.size()));
    fs::remove_all(dir);
}

BENCHMARK(BM_Log_AppendWithRolling)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Log_AppendNoSyncThenRead
//
// Simulates the gateway group-commit hot path: appendNoSync() one batch,
// then immediately read() it back (as pushSubscriptions does in each cycle).
// Measures combined latency without fsync.
// ---------------------------------------------------------------------------
static void BM_Log_AppendNoSyncThenRead(benchmark::State& state) {
    namespace fs = std::filesystem;
    static constexpr int64_t kRollBytes = 64LL * 1024 * 1024;

    auto dir = fs::temp_directory_path() / "rune_bench_log_nosync_read";
    fs::remove_all(dir);
    fs::create_directories(dir);

    rune::storage::Log log(dir.string(), kRollBytes);
    log.recover();

    int64_t nextOff = 0;
    std::vector<uint8_t> batch;

    for (auto _ : state) {
        state.PauseTiming();
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        // appendNoSync (page-cache write only)
        int64_t result = log.appendNoSync(batch.data(), batch.size(), nextOff);
        benchmark::DoNotOptimize(result);

        // Immediately read it back (as pushSubscriptions does)
        auto data = log.read(nextOff, 512 * 1024);
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();

        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(batch.size()));
    fs::remove_all(dir);
}
BENCHMARK(BM_Log_AppendNoSyncThenRead)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Log_ReadAcrossSegments
//
// Pre-build a multi-segment log (4 KiB roll, 500 batches → ~6 segments)
// outside the loop, then read at a mid offset that falls in a non-first
// segment.  Exercises the segment lookup + OffsetIndex binary search.
// ---------------------------------------------------------------------------
static void BM_Log_ReadAcrossSegments(benchmark::State& state) {
    static constexpr int kBatches    = 500;
    static constexpr int64_t kRoll   = 4 * 1024;

    auto dir = buildLog("read", kBatches, kRoll);
    rune::storage::Log log(dir.string(), kRoll);

    // Read from the second half — guaranteed to span at least one segment boundary
    const int64_t midOffset = static_cast<int64_t>(kBatches * 3 / 4);

    for (auto _ : state) {
        auto data = log.read(midOffset, /*maxBytes=*/4096);
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_Log_ReadAcrossSegments)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Log_RecoverMultiSegment
//
// Pre-build a multi-segment log, then time recover() on a fresh Log object
// over the same directory.  Fresh construction is PauseTiming'd so only
// recover() is measured.  Parameterised: 200 and 2 000 batches.
// ---------------------------------------------------------------------------
static void BM_Log_RecoverMultiSegment(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int count = static_cast<int>(state.range(0));
    static constexpr int64_t kRoll = 4 * 1024;

    auto dir = buildLog("recover_" + std::to_string(count), count, kRoll);

    for (auto _ : state) {
        state.PauseTiming();
        rune::storage::Log fresh(dir.string(), kRoll);
        state.ResumeTiming();

        int64_t result = fresh.recover();
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["batches"] = benchmark::Counter(static_cast<double>(count));
    fs::remove_all(dir);
}

BENCHMARK(BM_Log_RecoverMultiSegment)
    ->Arg(200)
    ->Arg(2000)
    ->Unit(benchmark::kMicrosecond);

} // namespace
