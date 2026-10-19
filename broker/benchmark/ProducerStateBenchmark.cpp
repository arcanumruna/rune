// ProducerStateManager benchmark (formerly ProducerState benchmark).
//
// ProducerState has been removed (S1 audit fix — legacy no-window dedup class,
// superseded by ProducerStateManager which provides correct retry-window dedup).
// Benchmarks are rewritten to exercise ProducerStateManager::validate + update.
//
// BM_ProducerState_ValidateContiguous — single producer, contiguous sequence
//   advances every iteration.  Reset under PauseTiming when sequence wraps so
//   the expected-next-sequence invariant is maintained indefinitely.
//
// BM_ProducerState_ManyProducers — round-robin across state.range(0) producer
//   IDs, each advancing one record per turn.  Models the multi-tenant path
//   where the broker tracks many independent idempotent producers.

#include <benchmark/benchmark.h>
#include <rune/storage/ProducerStateManager.h>

#include <cstdint>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// BM_ProducerState_ValidateContiguous
//
// Single producer (id=1, epoch=0, partition=0).  Each call advances baseSequence
// by 1.  When baseSequence reaches kWrapAt, reset under PauseTiming.
// ---------------------------------------------------------------------------
static void BM_ProducerState_ValidateContiguous(benchmark::State& state) {
    static constexpr int32_t kWrapAt      = 1 << 20; // ~1 M calls before wrap
    static constexpr int64_t kProducerId  = 1;
    static constexpr int16_t kEpoch       = 0;
    static constexpr int32_t kPartition   = 0;

    rune::storage::ProducerStateManager psm;
    int32_t seq = 0;
    int64_t logOffset = 0;

    for (auto _ : state) {
        if (seq >= kWrapAt) {
            state.PauseTiming();
            psm = rune::storage::ProducerStateManager{};
            seq = 0;
            logOffset = 0;
            state.ResumeTiming();
        }

        int32_t last = seq; // single-record batch: lastSeq == baseSeq
        auto result = psm.validate(/*topicId=*/0, kProducerId, kPartition, kEpoch, seq, last);
        benchmark::DoNotOptimize(result);
        if (result.isAccept()) {
            psm.update(/*topicId=*/0, kProducerId, kPartition, kEpoch, seq, last, logOffset);
            ++logOffset;
        }
        ++seq;
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_ProducerState_ValidateContiguous)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ProducerState_ManyProducers
//
// state.range(0) distinct producer IDs, each with epoch=0, partition=0.
// The benchmark round-robins through all producers; each producer's sequence
// advances by 1 per visit.  Reset when any producer's sequence hits kWrapAt.
// ---------------------------------------------------------------------------
static void BM_ProducerState_ManyProducers(benchmark::State& state) {
    static constexpr int32_t kWrapAt    = 1 << 16; // 64 K rounds before reset
    static constexpr int16_t kEpoch     = 0;
    static constexpr int32_t kPartition = 0;

    const int numProducers = static_cast<int>(state.range(0));

    rune::storage::ProducerStateManager psm;
    std::vector<int32_t> seqs(static_cast<std::size_t>(numProducers), 0);
    std::vector<int64_t> offsets(static_cast<std::size_t>(numProducers), 0);
    int idx = 0;

    for (auto _ : state) {
        if (seqs[static_cast<std::size_t>(idx)] >= kWrapAt) {
            state.PauseTiming();
            psm = rune::storage::ProducerStateManager{};
            std::fill(seqs.begin(),    seqs.end(),    0);
            std::fill(offsets.begin(), offsets.end(), 0);
            state.ResumeTiming();
        }

        const int64_t producerId = static_cast<int64_t>(idx) + 1; // ids 1..N
        int32_t seq  = seqs   [static_cast<std::size_t>(idx)];
        int64_t off  = offsets[static_cast<std::size_t>(idx)];

        auto result = psm.validate(/*topicId=*/0, producerId, kPartition, kEpoch, seq, seq);
        benchmark::DoNotOptimize(result);
        if (result.isAccept()) {
            psm.update(/*topicId=*/0, producerId, kPartition, kEpoch, seq, seq, off);
            ++offsets[static_cast<std::size_t>(idx)];
        }
        ++seqs[static_cast<std::size_t>(idx)];

        idx = (idx + 1) % numProducers;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["producers"] = benchmark::Counter(static_cast<double>(numProducers));
}

BENCHMARK(BM_ProducerState_ManyProducers)
    ->Arg(8)
    ->Arg(64)
    ->Arg(512)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ProducerState_Retransmit — EOS-OOS: the cost of answering a retransmit
// (baseSequence <= lastSequence).  range(0) = how many batches back the
// retransmit is: 1 = newest, kRetryWindowSize = oldest in the window (worst
// in-window scan), kRetryWindowSize + 1 = known-old (full scan, then OK -1).
// Window full (64 batches appended).  The in-order path never scans.
// ---------------------------------------------------------------------------
static void BM_ProducerState_Retransmit(benchmark::State& state) {
    static constexpr int32_t kAppended = 64;
    const int32_t back = static_cast<int32_t>(state.range(0));
    rune::storage::ProducerStateManager psm;
    for (int32_t s = 0; s < kAppended; ++s) {
        (void)psm.validate(0, 1, 0, 0, s, s);
        psm.update(0, 1, 0, 0, s, s, s);
    }
    const int32_t seq = kAppended - back;
    for (auto _ : state) {
        auto r = psm.validate(0, 1, 0, 0, seq, seq);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_ProducerState_Retransmit)
    ->Arg(1)->Arg(5)->Arg(rune::storage::kRetryWindowSize)->Arg(rune::storage::kRetryWindowSize + 1)
    ->Unit(benchmark::kNanosecond);

} // namespace
