// ProducerState benchmark.
//
// BM_ProducerState_ValidateContiguous — single producer, contiguous sequence
//   advances every iteration.  Reset under PauseTiming when sequence wraps so
//   the expected-next-sequence invariant is maintained indefinitely.
//
// BM_ProducerState_ManyProducers — round-robin across state.range(0) producer
//   IDs, each advancing one record per turn.  Models the multi-tenant path
//   where the broker tracks many independent idempotent producers.

#include <benchmark/benchmark.h>
#include <rune/storage/ProducerState.h>

#include <cstdint>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// BM_ProducerState_ValidateContiguous
//
// Single producer (id=1, epoch=0).  Each call advances baseSequence by 1.
// When baseSequence reaches kWrapAt, reset the ProducerState object under
// PauseTiming so we never overflow int32_t and the state is clean for the
// next block of iterations.
// ---------------------------------------------------------------------------
static void BM_ProducerState_ValidateContiguous(benchmark::State& state) {
    static constexpr int32_t kWrapAt     = 1 << 20; // ~1 M calls before wrap
    static constexpr int64_t kProducerId = 1;
    static constexpr int16_t kEpoch      = 0;
    static constexpr int32_t kRecordCount = 1;

    rune::storage::ProducerState ps;
    int32_t seq = 0;

    for (auto _ : state) {
        if (seq >= kWrapAt) {
            state.PauseTiming();
            ps = rune::storage::ProducerState{};
            seq = 0;
            state.ResumeTiming();
        }

        auto result = ps.validateAndUpdate(kProducerId, kEpoch, seq, kRecordCount);
        benchmark::DoNotOptimize(result);
        seq += kRecordCount;
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_ProducerState_ValidateContiguous)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ProducerState_ManyProducers
//
// state.range(0) distinct producer IDs, each with epoch=0.  The benchmark
// round-robins through all producers; each producer's sequence advances by 1
// per visit.  Reset when any producer's sequence would overflow kWrapAt.
// ---------------------------------------------------------------------------
static void BM_ProducerState_ManyProducers(benchmark::State& state) {
    static constexpr int32_t kWrapAt      = 1 << 16; // 64 K rounds before reset
    static constexpr int16_t kEpoch       = 0;
    static constexpr int32_t kRecordCount = 1;

    const int numProducers = static_cast<int>(state.range(0));

    rune::storage::ProducerState ps;
    std::vector<int32_t> seqs(static_cast<std::size_t>(numProducers), 0);
    int idx = 0;

    for (auto _ : state) {
        if (seqs[static_cast<std::size_t>(idx)] >= kWrapAt) {
            // At least one producer is near overflow — rebuild all state.
            state.PauseTiming();
            ps = rune::storage::ProducerState{};
            std::fill(seqs.begin(), seqs.end(), 0);
            state.ResumeTiming();
        }

        const int64_t producerId = static_cast<int64_t>(idx) + 1; // ids 1..N
        auto result = ps.validateAndUpdate(producerId, kEpoch,
                                           seqs[static_cast<std::size_t>(idx)],
                                           kRecordCount);
        benchmark::DoNotOptimize(result);
        seqs[static_cast<std::size_t>(idx)] += kRecordCount;

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

} // namespace
