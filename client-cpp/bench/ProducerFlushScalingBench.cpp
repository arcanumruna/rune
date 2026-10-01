// bench/ProducerFlushScalingBench.cpp — F12: per-duty-cycle producer cost vs
// partition count (Google Benchmark, in-process, counting sink, no Aeron).
//
// Every partition is created up front (one record each, flushed), so the
// producer holds N PartitionStates but only a FEW of them have work in a cycle.
// A cycle's cost must scale with the partitions that have work, not with N.
//
//   BM_RingCycle/N/R    Ring ingress: R records (all to partition 0) written to
//                       the producer ring, then ONE ProducerHub::drainAndFlush.
//   BM_RingIdleCycle/N  Ring ingress: an empty duty cycle (nothing to do).
//   BM_DirectCycle/N/R  Direct (Invoker) ingress: R trySubmits to partition 0,
//                       then ONE drainAndFlush (the end-of-pass flush).
//
// acks=None, lingerUs=0, so every cycle sends exactly one frame.

#include <rune/client/Key.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

namespace rc = rune::client;
using rc::producer::CountingSink;
using rc::producer::ProducerHub;
using rc::producer::ProducerImpl;

namespace {

const std::vector<uint8_t> kVal(64, 0x5A);

rc::ProducerConfig benchCfg() {
    rc::ProducerConfig c;
    c.acks        = rc::Acks::None;
    c.lingerUs    = 0;
    c.batchBytes  = 64 * 1024;
    c.maxInFlight = 8;
    return c;
}

struct Rig {
    std::shared_ptr<ProducerImpl> impl;
    ProducerHub                   hub;
    uint64_t                      frames = 0, bytes = 0;
    CountingSink                  sink{&frames, &bytes};
    int64_t                       now = 1;

    Rig(int32_t partitions, bool direct) {
        impl = std::make_shared<ProducerImpl>(benchCfg(), 4 * 1024 * 1024);
        if (direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, now});
        hub.add(impl);
        for (int32_t p = 0; p < partitions; ++p) {
            send(p);
            if ((p & 63) == 63) cycle();
        }
        cycle();
        cycle();
        if (frames == 0) throw std::runtime_error("rig: no frames");
    }
    void send(int32_t partition) {
        const auto r = impl->trySubmit(1, partition, rc::Key(uint64_t{7}), kVal, 0);
        if (r != rc::SendResult::Ok) throw std::runtime_error("rig: send rejected");
    }
    int cycle() { return hub.drainAndFlush(++now, /*sessionId=*/42, sink); }
};

void ringCycle(benchmark::State& st) {
    Rig rig(static_cast<int32_t>(st.range(0)), /*direct=*/false);
    const int64_t records = st.range(1);
    for (auto _ : st) {
        for (int64_t i = 0; i < records; ++i) rig.send(0);
        benchmark::DoNotOptimize(rig.cycle());
    }
    st.counters["partitions"] = static_cast<double>(st.range(0));
}

void ringIdleCycle(benchmark::State& st) {
    Rig rig(static_cast<int32_t>(st.range(0)), /*direct=*/false);
    for (auto _ : st) benchmark::DoNotOptimize(rig.cycle());
}

void directCycle(benchmark::State& st) {
    Rig rig(static_cast<int32_t>(st.range(0)), /*direct=*/true);
    const int64_t records = st.range(1);
    for (auto _ : st) {
        for (int64_t i = 0; i < records; ++i) rig.send(0);
        benchmark::DoNotOptimize(rig.cycle());
    }
}

} // namespace

BENCHMARK(ringCycle)->Name("BM_RingCycle")
    ->ArgsProduct({{8, 64, 256, 1000, 4096}, {1, 64}});
BENCHMARK(ringIdleCycle)->Name("BM_RingIdleCycle")->Arg(8)->Arg(64)->Arg(256)->Arg(1000)->Arg(4096);
BENCHMARK(directCycle)->Name("BM_DirectCycle")
    ->ArgsProduct({{8, 64, 256, 1000, 4096}, {1, 64}});

BENCHMARK_MAIN();
