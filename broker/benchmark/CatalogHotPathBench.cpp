// CatalogHotPathBench.cpp — catalog history Task 23 (spec §10 "hot-path benchmarks unchanged";
// Task 8 ruling 1; Task 8 N4 / Task 22 N-M2).  PORTABLE: the arm-0 benchmarks also build at the
// parity base 7d86db94 (RUNE_T23_HAS_CATALOG == 0 there), so one source measures both sides.
//
//   BM_GatewayProducePlacementGate/<0 static | 1 catalog-placed>   the per-produce placement check
//   BM_GatewaySubscribePlacementGate/<0|1>                         the per-subscribe placement check
//   BM_GatewayRefusedUnplaced                                      the waiting side (NotLeader refusals)
//   BM_GatewayViewRowServed/fillers:<n>   (+ BM_ViewRowIdlePass)   serve cost = Served - Idle; scan = f(1023) - f(0)
//   BM_GatewayViewRowRedirect                                      the source-led-elsewhere Redirect
//
// allocs_per_op uses BenchmarkMain's shared operator-new counter (no second operator new here).
#include "GatewayProduceRig.h"
#if RUNE_T23_HAS_CATALOG
#include "ViewReadRig.h"
#endif

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include <array>
#include <atomic>
#include <string>
#include <vector>

extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

using rune::node::test::GatewayProduceRig;
using rune::node::test::ProduceTopicMode;
namespace pc = rune::protocol::client;

constexpr int kPool = 1 << 14;

void allocsOn() { g_allocCount.store(0); g_allocEnabled.store(true); }
void allocsOff(benchmark::State& state) {
    g_allocEnabled.store(false);
    const auto it = static_cast<double>(state.iterations());
    state.counters["allocs_per_op"] = it > 0 ? static_cast<double>(g_allocCount.load()) / it : 0.0;
}

// ---------------------------------------------------------------------------
void BM_GatewayProducePlacementGate(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    const auto mode = state.range(0) == 0 ? ProduceTopicMode::StaticRouted : ProduceTopicMode::CatalogPlaced;
    GatewayProduceRig rig(mode);
    std::vector<std::vector<uint8_t>> frames;
    int64_t corr = 0;
    int next = 0;
    auto refill = [&] {
        frames.clear();
        frames.reserve(kPool);
        for (int i = 0; i < kPool; ++i) frames.push_back(rig.produceFrame(++corr));
        next = 0;
    };
    refill();
    rig.produceOk = rig.produceErr = rig.produceNotLeader = 0;
    allocsOn();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] {
            g_allocEnabled.store(false);
            state.PauseTiming(); refill(); state.ResumeTiming();
            g_allocEnabled.store(true);
        }
        const auto& f = frames[static_cast<std::size_t>(next++)];
        const uint64_t before = rig.produceOk + rig.produceErr + rig.produceNotLeader;
        rig.produce(f);
        for (int k = 0; k < 1024 && rig.produceOk + rig.produceErr + rig.produceNotLeader == before; ++k) rig.pass();
    }
    allocsOff(state);
    if (rig.produceOk != static_cast<uint64_t>(state.iterations()) || rig.produceErr + rig.produceNotLeader != 0)
        state.SkipWithError("a produce was not answered OK");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayProducePlacementGate)
    ->Arg(0)
#if RUNE_T23_HAS_CATALOG
    ->Arg(1)
#endif
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
void BM_GatewaySubscribePlacementGate(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    const auto mode = state.range(0) == 0 ? ProduceTopicMode::StaticRouted : ProduceTopicMode::CatalogPlaced;
    GatewayProduceRig rig(mode);
    std::vector<std::vector<uint8_t>> frames;
    int64_t corr = 0;
    int next = 0;
    auto refill = [&] {
        frames.clear();
        frames.reserve(kPool);
        for (int i = 0; i < kPool; ++i) frames.push_back(rig.subscribeFrame(++corr));
        next = 0;
    };
    refill();
    rig.subscribeOk = rig.errNotLeader = rig.errOther = 0;
    allocsOn();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] {
            g_allocEnabled.store(false);
            state.PauseTiming(); refill(); state.ResumeTiming();
            g_allocEnabled.store(true);
        }
        rig.subscribe(frames[static_cast<std::size_t>(next++)]);
        std::array<uint8_t, 64> u{};
        pc::Unsubscribe m;
        m.sessionId = rig.sid; m.correlationId = ++corr; m.subscriptionId = rig.lastSubscriptionId;
        m.encode(u.data());
        rig.gw.applyUnsubscribeForTest(u.data(), pc::Unsubscribe::ENCODED_SIZE);
        rig.pass();
    }
    allocsOff(state);   // reported only: a subscription registration allocates by design
    if (rig.subscribeOk != static_cast<uint64_t>(state.iterations()))
        state.SkipWithError("a subscribe was not answered OK");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewaySubscribePlacementGate)
    ->Arg(0)
#if RUNE_T23_HAS_CATALOG
    ->Arg(1)
#endif
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

#if RUNE_T23_HAS_CATALOG
// ---------------------------------------------------------------------------
void BM_GatewayRefusedUnplaced(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    GatewayProduceRig rig(ProduceTopicMode::CatalogUnplaced);
    std::vector<std::vector<uint8_t>> pf, sf;
    int64_t corr = 0;
    int next = 0;
    auto refill = [&] {
        pf.clear(); sf.clear();
        pf.reserve(kPool); sf.reserve(kPool);
        for (int i = 0; i < kPool; ++i) { pf.push_back(rig.produceFrame(++corr)); sf.push_back(rig.subscribeFrame(++corr)); }
        next = 0;
    };
    refill();
    rig.produceOk = rig.produceErr = rig.produceNotLeader = 0;
    rig.errNotLeader = rig.errOther = rig.subscribeOk = 0;
    allocsOn();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] {
            g_allocEnabled.store(false);
            state.PauseTiming(); refill(); state.ResumeTiming();
            g_allocEnabled.store(true);
        }
        rig.produce(pf[static_cast<std::size_t>(next)]);
        rig.subscribe(sf[static_cast<std::size_t>(next)]);
        ++next;
    }
    allocsOff(state);
    const auto it = static_cast<uint64_t>(state.iterations());
    if (rig.produceNotLeader != it || rig.errNotLeader != it || rig.produceOk != 0 || rig.subscribeOk != 0)
        state.SkipWithError("a refusal was not answered NotLeader exactly once");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayRefusedUnplaced)->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
using rune::node::test::GatewayViewReadRig;

void BM_GatewayViewRowServed(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::ServedHere, static_cast<std::size_t>(state.range(0)));
    std::vector<std::vector<uint8_t>> frames;
    int64_t corr = 1'000'000;
    int next = 0;
    auto refill = [&] {
        frames.clear();
        frames.reserve(kPool);
        for (int i = 0; i < kPool; ++i) frames.push_back(rig.getViewRowFrame(++corr, 7));
        next = 0;
    };
    refill();
    rig.viewOk = rig.viewOther = rig.redirects = rig.errOther = 0;
    allocsOn();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] {
            g_allocEnabled.store(false);
            state.PauseTiming(); refill(); state.ResumeTiming();
            g_allocEnabled.store(true);
        }
        const auto& f = frames[static_cast<std::size_t>(next++)];
        const uint64_t before = rig.viewOk + rig.viewOther + rig.redirects + rig.errOther;
        rig.gw.applyGetViewRowForTest(f.data(), f.size());
        for (int k = 0; k < 1024 && rig.viewOk + rig.viewOther + rig.redirects + rig.errOther == before; ++k) rig.pass();
    }
    allocsOff(state);
    if (rig.viewOk != static_cast<uint64_t>(state.iterations()))
        state.SkipWithError("a GetViewRow was not served OK");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayViewRowServed)->ArgName("fillers")->Arg(0)->Arg(63)->Arg(1023)
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

// The baseline: the same rig, the timed pass only.  Serve cost = Served - Idle.
void BM_ViewRowIdlePass(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::ServedHere, static_cast<std::size_t>(state.range(0)));
    allocsOn();
    for (auto _ : state) rig.pass();
    allocsOff(state);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ViewRowIdlePass)->ArgName("fillers")->Arg(0)->Arg(63)->Arg(1023)
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
void BM_GatewayViewRowRedirect(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::SourceLedElsewhere, 0);
    std::vector<std::vector<uint8_t>> frames;
    int64_t corr = 1'000'000;
    int next = 0;
    auto refill = [&] {
        frames.clear();
        frames.reserve(kPool);
        for (int i = 0; i < kPool; ++i) frames.push_back(rig.getViewRowFrame(++corr, 7));
        next = 0;
    };
    refill();
    rig.viewOk = rig.viewOther = rig.redirects = rig.errOther = 0;
    allocsOn();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] {
            g_allocEnabled.store(false);
            state.PauseTiming(); refill(); state.ResumeTiming();
            g_allocEnabled.store(true);
        }
        const auto& f = frames[static_cast<std::size_t>(next++)];
        rig.gw.applyGetViewRowForTest(f.data(), f.size());
    }
    allocsOff(state);
    if (rig.redirects != static_cast<uint64_t>(state.iterations()) || rig.viewOk != 0)
        state.SkipWithError("a GetViewRow was not Redirected exactly once");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayViewRowRedirect)->Unit(benchmark::kNanosecond)->MinTime(0.5);
#endif

}  // namespace
