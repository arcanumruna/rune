// WorkerReturnCodecBench.cpp — F15 S1 (design note §7/§9): the ProduceResult
// carrier (WorkerReturn, SBE template 2) must keep its encode cost when the
// transactional returns move to their own template (TxnReturn, id 6).
//
//   BM_ProduceResultEncode — encodeWorkerReturnSbe of a ProduceResult (empty
//                            chunk) into a reused slot: exactly what
//                            Worker::enqueueReturn_ does per produce ack.
//   BM_TxnReturnEncode     — encodeTxnReturn of a CONTROL_RECORD_COMMITTED.

#include <rune/node/WorkerReturnCodec.h>

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>

using rune::node::TxnReturn;
using rune::node::WorkerReturn;

static void BM_ProduceResultEncode(benchmark::State& state) {
    alignas(64) std::array<char, 512> slot{};
    WorkerReturn ret;
    ret.type          = WorkerReturn::Type::ProduceResult;
    ret.sessionId     = 42;
    ret.correlationId = 1;
    ret.partition     = 3;
    ret.topicId       = 7;
    ret.recordCount   = 1;
    int64_t base = 0;
    for (auto _ : state) {
        ret.baseOffset = base;
        ret.lastOffset = base;
        ++base;
        const std::size_t n = rune::node::workerReturnEncodedLength(ret);
        rune::node::encodeWorkerReturnSbe(slot.data(), n, ret);
        benchmark::DoNotOptimize(slot.data());
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_ProduceResultEncode)->Unit(benchmark::kNanosecond);

static void BM_TxnReturnEncode(benchmark::State& state) {
    alignas(64) std::array<char, 128> slot{};
    TxnReturn r;
    r.topicId = 7; r.partition = 3; r.incarnation = 1; r.leaderEpoch = 2;
    int64_t token = 0;
    for (auto _ : state) {
        r.token = ++token;
        benchmark::DoNotOptimize(rune::node::encodeTxnReturn(slot.data(), slot.size(), r));
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_TxnReturnEncode)->Unit(benchmark::kNanosecond);
