// WorkerReturnCodecBench.cpp — F15 S1 (design note §7/§9): the ProduceResult
// carrier (WorkerReturn, SBE template 2) must keep its encode cost when the
// transactional returns move to their own template (TxnReturn, id 6).
//
//   BM_ProduceResultEncode — encodeWorkerReturnSbe of a ProduceResult (empty
//                            chunk) into a reused slot: exactly what
//                            Worker::enqueueReturn_ does per produce ack.
//   BM_TxnReturnEncode     — encodeTxnReturn of a CONTROL_RECORD_COMMITTED.
//
// F15 S3 (design note §6/§9): the worker's return FIFO.
//   BM_ReturnFifo                    — push + pop-front of a ProduceResult at a
//                                      steady FIFO depth (Arg): O(1), no alloc.
//   BM_ReturnFifoVectorEraseBaseline — the replaced retOverflow_ shape
//                                      (vector push_back + erase(begin)): O(depth).

#include <rune/node/ReturnFifo.h>
#include <rune/node/WorkerReturnCodec.h>

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <memory>
#include <vector>

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

namespace {
WorkerReturn produceResult(int64_t corr) {
    WorkerReturn ret;
    ret.type          = WorkerReturn::Type::ProduceResult;
    ret.sessionId     = 42;
    ret.correlationId = corr;
    ret.partition     = 3;
    ret.topicId       = 7;
    ret.recordCount   = 1;
    return ret;
}
} // namespace

static void BM_ReturnFifo(benchmark::State& state) {
    // The worker's FIFO capacity (Worker::kRetFifoCapacity); heap-held, built once.
    auto fifo = std::make_unique<rune::node::ReturnFifo<16384>>();
    const auto depth = static_cast<int64_t>(state.range(0));
    int64_t corr = 0;
    for (int64_t i = 0; i < depth; ++i) (void)fifo->push(produceResult(++corr));
    WorkerReturn ret = produceResult(0);
    for (auto _ : state) {
        ret.correlationId = ++corr;
        benchmark::DoNotOptimize(fifo->push(ret));
        benchmark::DoNotOptimize(fifo->front().worker.correlationId);
        fifo->popFront();
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_ReturnFifo)->Arg(1)->Arg(64)->Arg(1024)->Arg(8192)->Unit(benchmark::kNanosecond);

static void BM_ReturnFifoVectorEraseBaseline(benchmark::State& state) {
    std::vector<WorkerReturn> v;
    v.reserve(16384);
    const auto depth = static_cast<int64_t>(state.range(0));
    int64_t corr = 0;
    for (int64_t i = 0; i < depth; ++i) v.push_back(produceResult(++corr));
    WorkerReturn ret = produceResult(0);
    for (auto _ : state) {
        ret.correlationId = ++corr;
        v.push_back(ret);
        benchmark::DoNotOptimize(v.front().correlationId);
        v.erase(v.begin());
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_ReturnFifoVectorEraseBaseline)->Arg(1)->Arg(64)->Arg(1024)->Arg(8192)->Unit(benchmark::kNanosecond);
