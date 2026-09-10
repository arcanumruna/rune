// LogSegment benchmark.
//
// Measures single-segment append throughput, mid-segment read latency, and
// recover() cost for segments of varying sizes.  All disk setup happens
// outside the timed loop; only the operation under test is measured.
//
// Batch shape: 1 record, key="pos" (3 B), value="x:1234,y:5678,z:9012" (20 B)
// → ~48-byte wire batch — representative of an MMORPG entity-position event.

#include <benchmark/benchmark.h>
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>

#include <filesystem>
#include <vector>
#include <string>
#include <cstdint>

namespace {

// ---------------------------------------------------------------------------
// Build a minimal MMORPG-shaped ~48-byte single-record batch
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makePoseBatch(int64_t baseOffset, int64_t ts) {
    rune::protocol::RecordBatchBuilder b(baseOffset, ts);
    b.append("pos", "x:1234,y:5678,z:9012", ts);
    return b.build();
}

// Pre-populate a LogSegment directory with `count` batches and return the
// directory path.  The caller is responsible for cleanup.
static std::filesystem::path buildSegmentDir(const std::string& tag, int count) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / ("rune_bench_seg_" + tag);
    fs::remove_all(dir);
    fs::create_directories(dir);

    rune::storage::LogSegment seg(dir.string(), /*baseOffset=*/0);
    for (int i = 0; i < count; ++i) {
        auto batch = makePoseBatch(static_cast<int64_t>(i), 1700000000000LL + i);
        seg.append(batch, static_cast<int64_t>(i));
    }
    return dir;
}

// ---------------------------------------------------------------------------
// BM_LogSegment_AppendBatch
//
// Append 48 B single-record batches into a single LogSegment.  The batch is
// built OUTSIDE the timed loop; only append() is measured.
// ---------------------------------------------------------------------------
static void BM_LogSegment_AppendBatch(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "rune_bench_seg_append";
    fs::remove_all(dir);
    fs::create_directories(dir);

    rune::storage::LogSegment seg(dir.string(), /*baseOffset=*/0);

    // Pre-build batch template outside the loop
    auto batch = makePoseBatch(0, 1700000000000LL);

    int64_t nextOff = 0;
    for (auto _ : state) {
        state.PauseTiming();
        // Rebuild batch with correct baseOffset to keep log valid
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        int64_t result = seg.append(batch, nextOff);
        benchmark::DoNotOptimize(result);
        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(batch.size()));
    fs::remove_all(dir);
}

BENCHMARK(BM_LogSegment_AppendBatch)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_LogSegment_ReadAtOffset
//
// Pre-build a segment with 1 000 batches (outside loop), then repeatedly
// read from the middle.  Measures random-offset read + index lookup latency.
// ---------------------------------------------------------------------------
static void BM_LogSegment_ReadAtOffset(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int kBatches = 1000;
    auto dir = buildSegmentDir("read", kBatches);

    rune::storage::LogSegment seg(dir.string(), /*baseOffset=*/0);
    const int64_t midOffset = static_cast<int64_t>(kBatches / 2);

    for (auto _ : state) {
        auto data = seg.read(midOffset, /*maxBytes=*/4096);
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_LogSegment_ReadAtOffset)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_LogSegment_Recover
//
// Pre-build a segment with state.range(0) valid batches (outside loop), then
// time recover() on a freshly-constructed LogSegment over the same directory.
// A new LogSegment is constructed per iteration with PauseTiming so that only
// recover() itself is in the measured region.
// ---------------------------------------------------------------------------
static void BM_LogSegment_Recover(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int count = static_cast<int>(state.range(0));
    auto dir = buildSegmentDir("recover_" + std::to_string(count), count);

    for (auto _ : state) {
        state.PauseTiming();
        // Fresh LogSegment object so recover() has real work to do
        rune::storage::LogSegment fresh(dir.string(), /*baseOffset=*/0);
        state.ResumeTiming();

        int64_t result = fresh.recover();
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["batches"] = benchmark::Counter(static_cast<double>(count));
    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_LogSegment_Recover)
    ->Arg(100)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond);

} // namespace
