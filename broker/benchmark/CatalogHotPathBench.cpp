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
#include "BenchGate.h"
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
    rune::bench::gateCheck(state, rig.produceOk == static_cast<uint64_t>(state.iterations()) &&
                                      rig.produceErr + rig.produceNotLeader == 0,
                           "a produce was not answered OK");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayProducePlacementGate)
    ->Arg(0)
#if RUNE_T23_HAS_CATALOG
    ->Arg(1)
#endif
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
// Review C I3 (authentication): the Required-mode cost of a produce.  Arg 0 = InProcess (every session AuthAnonymous),
// Arg 1 = Required with an AuthAuthenticated allow-all session, Arg 2 = Required with a compiled table (Task 12).  Required adds exactly one variant-index compare inside
// handleProduce (on the session it already looked up) - within noise of arg 0.
void BM_GatewayProduceAuthGate(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    GatewayProduceRig rig(ProduceTopicMode::StaticRouted);
    rune::gateway::SecurityView view{rune::security::SecurityState{}};
    if (state.range(0) == 1) {
        rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
        rig.gw.setSessionAuthForTest(rig.sid, rune::gateway::AuthAuthenticated{1, "ws1", rune::gateway::PermissionTable::allowAll()});
    } else if (state.range(0) == 2) {
        // Task 12: Required with a REAL compiled table (one WRITE bit among 4096 topics) - the per-produce check is one
        // bounds check + one byte load on top of arg 1.
        rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
        rune::gateway::ObjectIndex idx;
        for (int32_t i = 0; i < 4096; ++i) idx.onTopic(i, "t" + std::to_string(i));
        idx.onTopic(rig.kTopic, "granted");
        const std::vector<rune::security::GrantSpec> grants = {
            {rune::security::Operation::Write, {rune::security::ResourceType::Topic, rune::security::PatternKind::Exact, "granted"}}};
        rig.gw.setSessionAuthForTest(rig.sid, rune::gateway::AuthAuthenticated{1, "ws1", rune::gateway::PermissionTable::compile(grants, idx)});
    }
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
    rune::bench::gateCheck(state, rig.produceOk == static_cast<uint64_t>(state.iterations()) &&
                                      rig.produceErr + rig.produceNotLeader == 0,
                           "a produce was not answered OK");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayProduceAuthGate)->Arg(0)->Arg(1)->Arg(2)->Unit(benchmark::kNanosecond)->MinTime(1.0)->Repetitions(5)
    ->ReportAggregatesOnly(false);

// Authentication Task 12: the per-request check itself - PermissionTable::allows (one bounds check + one byte load)
// over a 4096-topic table, ids walked in a scrambled order so the load is not a single cached byte.
void BM_PermissionTableAllows(benchmark::State& state) {
    rune::gateway::ObjectIndex idx;
    for (int32_t i = 0; i < 4096; ++i) idx.onTopic(i, "t" + std::to_string(i));
    const std::vector<rune::security::GrantSpec> grants = {
        {rune::security::Operation::Write, {rune::security::ResourceType::Topic, rune::security::PatternKind::Prefix, "t1"}}};
    const auto table = rune::gateway::PermissionTable::compile(grants, idx);
    const uint8_t write = rune::security::opBit(rune::security::Operation::Write);
    uint32_t id = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        id = (id * 2654435761u + 12345u) & 4095u;
        allowed += table.allows(static_cast<int32_t>(id), write) ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_PermissionTableAllows)->Unit(benchmark::kNanosecond)->MinTime(0.5);

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
    rune::bench::gateCheck(state, rig.subscribeOk == static_cast<uint64_t>(state.iterations()),
                           "a subscribe was not answered OK");
    // M7: every Unsubscribe was answered: no subscription leaked into the state being measured.
    rune::bench::gateCheck(state, rig.gw.subscriptionCount() == 0, "a subscription leaked (subscriptionCount != 0)");
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
    rune::bench::gateCheck(state, rig.produceNotLeader == it && rig.errNotLeader == it && rig.produceOk == 0 &&
                                      rig.subscribeOk == 0,
                           "a refusal was not answered NotLeader exactly once");
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
    rune::bench::gateCheck(state, rig.viewOk == static_cast<uint64_t>(state.iterations()),
                           "a GetViewRow was not served OK");
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
    rune::bench::gateCheck(state, rig.redirects == static_cast<uint64_t>(state.iterations()) && rig.viewOk == 0,
                           "a GetViewRow was not Redirected exactly once");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayViewRowRedirect)->Unit(benchmark::kNanosecond)->MinTime(0.5);
#endif

}  // namespace
