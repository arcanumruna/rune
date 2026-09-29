// ProducePath benchmark.
//
// handleProduceBatch() is the shared produce hot path: batch sanity + CRC
// validation, idempotent dedup (ProducerStateManager), min-ISR gate, and the
// zero-copy appendNoSyncRestamp into the Log.  Measures the two dominant
// shapes: a legacy (non-idempotent) append, and an idempotent duplicate
// fast-reject (no append).

#include <benchmark/benchmark.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;
using rune::storage::Log;
using rune::storage::ProducerStateManager;
using rune::storage::handleProduceBatch;

static fs::path freshDir(const std::string& tag) {
    static std::atomic<uint32_t> seq{0};
    auto n = seq.fetch_add(1, std::memory_order_relaxed);
    auto dir = fs::temp_directory_path() /
               ("rune_bench_pp_" + tag + "_" + std::to_string(n));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

// Build a single-record batch, optionally with idempotent producer fields.
static std::vector<uint8_t> makeBatch(int64_t producerId    = -1,
                                      int16_t producerEpoch = 0,
                                      int32_t baseSequence  = -1) {
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    if (producerId >= 0) {
        b.setProducerId(producerId);
        b.setProducerEpoch(producerEpoch);
        b.setBaseSequence(baseSequence);
    }
    b.append("k", "value-payload", 1000LL);
    return b.build();
}

// BM_ProducePath_LegacyAppend — non-idempotent (pid=-1) → skips PSM, validates
// CRC, appends (page-cache, tier=1).  The realistic produce hot path.
static void BM_ProducePath_LegacyAppend(benchmark::State& state) {
    auto dir = freshDir("legacy");
    Log log(dir.string());
    ProducerStateManager psm;
    auto batch = makeBatch(/*pid=*/-1);

    for (auto _ : state) {
        auto out = handleProduceBatch(
            batch.data(), batch.size(),
            /*topicId=*/1, /*partition=*/0,
            /*producerId=*/-1, /*epoch=*/0, /*baseSeq=*/-1,
            log, psm,
            /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*tier=*/1);
        benchmark::DoNotOptimize(out.baseOffset);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(batch.size()));
    fs::remove_all(dir);
}
BENCHMARK(BM_ProducePath_LegacyAppend)->Unit(benchmark::kMicrosecond);

// BM_ProducePath_DuplicateReject — idempotent (pid>=0) with a fixed sequence:
// the first call appends, every subsequent identical call is detected as a
// duplicate and returns the prior offset with NO append.  Measures the PSM
// dedup fast-reject path.
static void BM_ProducePath_DuplicateReject(benchmark::State& state) {
    auto dir = freshDir("dup");
    Log log(dir.string());
    ProducerStateManager psm;
    auto batch = makeBatch(/*pid=*/100, /*epoch=*/0, /*baseSeq=*/0);

    // Prime: first call accepts + appends so the rest are duplicates.
    auto primed = handleProduceBatch(
        batch.data(), batch.size(), 1, 0, 100, 0, 0, log, psm,
        false, 0, 1, 1);
    benchmark::DoNotOptimize(primed.baseOffset);

    for (auto _ : state) {
        auto out = handleProduceBatch(
            batch.data(), batch.size(),
            /*topicId=*/1, /*partition=*/0,
            /*producerId=*/100, /*epoch=*/0, /*baseSeq=*/0,
            log, psm,
            /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*tier=*/1);
        benchmark::DoNotOptimize(out.isDuplicate);
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_ProducePath_DuplicateReject)->Unit(benchmark::kMicrosecond);

} // namespace
