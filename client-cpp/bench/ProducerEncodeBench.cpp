// bench/ProducerEncodeBench.cpp — LAT-1 h/i/j (backlog B): the producer's
// per-batch encode + CRC cost (Google Benchmark, in-process, no Aeron).
//
//   BM_EncodeFlush/V/R   acks=Leader, one partition: a full batch of V-byte
//                        values is drained, then flushBatches() runs R times
//                        against a sink that REFUSES (transport back-pressure)
//                        and once more where it accepts; the batch is then
//                        acked.  Models the saturation profile (~6.6 encodes
//                        per sent frame).  CountingSink behaves like the Aeron
//                        transport (refuse = back-pressure).
//   BM_AppendEncodeFlush/V/0  the same with the ring drain (records appended into
//                        the batch) inside the timed region: the duty thread's
//                        whole per-batch produce cost (CRC share = BM_Crc32c/64K
//                        over this).
//   BM_Crc32c/N          raw rune::protocol::crc32c throughput over N bytes
//                        (backlog B3: is CRC still hot?).
//   BM_Crc32cFast/N      the client's 3-way interleaved CRC-32C (B3).
//
// Counters: bytes_per_second = frame bytes accepted per second.

#include <rune/client/Key.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/Crc32c.h>
#if __has_include(<rune/client/detail/Crc32cFast.h>)
#include <rune/client/detail/Crc32cFast.h>
#define RUNE_BENCH_HAS_CRC_FAST 1
#endif


#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

namespace rc = rune::client;
using rc::producer::ProducerImpl;

namespace {

constexpr int32_t kBatchBytes = 64 * 1024;

rc::ProducerConfig benchCfg() {
    rc::ProducerConfig c;
    c.acks              = rc::Acks::Leader;
    c.lingerUs          = 0;
    c.batchBytes        = kBatchBytes;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 600'000;
    c.deliveryTimeoutMs = 600'000;
    return c;
}

void submitBatch(ProducerImpl& impl, const std::vector<uint8_t>& val, int records, int64_t& key) {
    for (int i = 0; i < records; ++i) {
        rc::Key k(static_cast<uint64_t>(key++));
        if (!impl.submitSendRecord(0, 0, k, std::span<const uint8_t>(val), false, 0)) {
            throw std::runtime_error("ring full");
        }
    }
}

template <bool kTimeAppend>
void encodeFlushT(benchmark::State& st) {
    const auto valueBytes = static_cast<std::size_t>(st.range(0));
    const int  refusals   = static_cast<int>(st.range(1));
    auto       impl       = std::make_unique<ProducerImpl>(benchCfg(), 8 * 1024 * 1024);
    const std::vector<uint8_t> val(valueBytes, uint8_t{0xAB});
    // Records that fill one batch (wire ~= value + ~20 B framing).
    const int perBatch = std::max(1, static_cast<int>((kBatchBytes - 128) / (valueBytes + 24)));
    int      left = refusals;
    int64_t  corr = 0, key = 0, offset = 0, now = 1;
    uint64_t frames = 0, bytes = 0;
    const rc::producer::CountingSink sink{&frames, &bytes, refusals, &left, &corr};
    for (auto _ : st) {
        st.PauseTiming();
        submitBatch(*impl, val, perBatch, key);
        if constexpr (kTimeAppend) st.ResumeTiming();
        impl->drainRing(now, perBatch + 1);
        if constexpr (!kTimeAppend) st.ResumeTiming();
        for (int r = 0; r <= refusals; ++r) impl->flushBatches(++now, 1, sink);
        st.PauseTiming();
        impl->onProduceResponse(corr, offset, 0);
        offset += perBatch;
        st.ResumeTiming();
    }
    st.SetBytesProcessed(static_cast<int64_t>(bytes));
    st.counters["records_per_batch"] = perBatch;
}

void encodeFlush(benchmark::State& st) { encodeFlushT<false>(st); }
void appendEncodeFlush(benchmark::State& st) { encodeFlushT<true>(st); }

void crc32cRaw(benchmark::State& st) {
    const auto n = static_cast<std::size_t>(st.range(0));
    std::vector<uint8_t> buf(n);
    for (std::size_t i = 0; i < n; ++i) buf[i] = static_cast<uint8_t>(i * 131u + 7u);
    uint32_t acc = 0;
    for (auto _ : st) {
        acc ^= rune::protocol::crc32c(buf.data(), buf.size());
        benchmark::DoNotOptimize(acc);
    }
    st.SetBytesProcessed(static_cast<int64_t>(st.iterations()) * static_cast<int64_t>(n));
}

#ifdef RUNE_BENCH_HAS_CRC_FAST
void crc32cFastRaw(benchmark::State& st) {
    const auto n = static_cast<std::size_t>(st.range(0));
    std::vector<uint8_t> buf(n);
    for (std::size_t i = 0; i < n; ++i) buf[i] = static_cast<uint8_t>(i * 131u + 7u);
    uint32_t acc = 0;
    for (auto _ : st) {
        acc ^= rune::client::detail::crc32cFast(buf.data(), buf.size());
        benchmark::DoNotOptimize(acc);
    }
    st.SetBytesProcessed(static_cast<int64_t>(st.iterations()) * static_cast<int64_t>(n));
}
#endif

}  // namespace

#ifdef RUNE_BENCH_HAS_CRC_FAST
BENCHMARK(crc32cFastRaw)->Name("BM_Crc32cFast")->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024);
#endif
BENCHMARK(encodeFlush)->Name("BM_EncodeFlush")->ArgsProduct({{256, 1024, 4096}, {0, 6}});
BENCHMARK(appendEncodeFlush)->Name("BM_AppendEncodeFlush")->ArgsProduct({{256, 1024, 4096}, {0}});
BENCHMARK(crc32cRaw)->Name("BM_Crc32c")->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024);
BENCHMARK_MAIN();
