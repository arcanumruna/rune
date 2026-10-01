// bench/GroupConsumerBench.cpp — CG-7: GroupConsumer hot-path benchmarks.
//
// Covers the client-side consumer-group hot paths (in-process, no Aeron):
//
//   BM_GCHub_OnRecords         — GroupConsumerHub::onRecords (records-dispatch + position-advance).
//                                 Hot path: snapshot load + map lookup + callback + advance.
//                                 Gate: p99 < 100µs. Zero-alloc assertion.
//
//   BM_GCHub_DoWork_HeartbeatCadence  — doWork() with heartbeat timer fired (sends one HB
//                                       per call via the send-fn). Measures the per-heartbeat
//                                       duty-cycle cost including encode + send dispatch.
//                                       Gate: p99 < 100µs. Zero-alloc assertion.
//
//   BM_GCHub_DoWork_CommitCadence     — doWork() with auto-commit timer fired (N partitions).
//                                       Measures sendOffsetCommit encode + send dispatch.
//                                       Gate: p99 < 100µs. Zero-alloc assertion.
//
//   BM_GCHub_DoWork_Stable     — doWork() with timers NOT fired (pure idle cost).
//                                Zero-alloc assertion.
//
// Alloc shim: operator new intercept counting allocations; enabled only during
// the isolated alloc-check phase (NOT during the timing loop — the timing loop
// uses a guarded push_back to avoid harness-vector realloc false positives).
// Two-phase design:
//   Phase 1: timing loop — collects latency samples; alloc tracking disabled.
//   Phase 2: 10 000 isolated hot-path calls with alloc tracking; no sample storage.

#include <benchmark/benchmark.h>

#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rccg  = rune::client::group;
namespace rcc   = rune::client::codec;

// ---------------------------------------------------------------------------
// Allocation counter shim.
// ---------------------------------------------------------------------------
static std::atomic<int64_t> g_allocCount{0};
static std::atomic<bool>    g_allocEnabled{false};

void* operator new(std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed))
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}
void* operator new[](std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed))
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}
void operator delete(void* p) noexcept  { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept  { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

// ---------------------------------------------------------------------------
// Monotonic nanoseconds
// ---------------------------------------------------------------------------
static inline int64_t nowNsI() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Percentile helper
// ---------------------------------------------------------------------------
static double pct(const std::vector<double>& sorted, double p)
{
    if (sorted.empty()) return 0.0;
    if (sorted.size() == 1) return sorted[0];
    const double idx = p * static_cast<double>(sorted.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(idx);
    const std::size_t hi = std::min(lo + 1, sorted.size() - 1);
    return sorted[lo] + (idx - static_cast<double>(lo)) * (sorted[hi] - sorted[lo]);
}

static void reportPercentiles(benchmark::State& state,
                               std::vector<double>& samples,
                               const char* prefix,
                               double gateMicros = 100.0)
{
    if (samples.empty()) return;
    std::sort(samples.begin(), samples.end());
    double sum = 0.0;
    for (double v : samples) sum += v;
    const std::string sp = prefix;
    state.counters[sp + "_p50_us"]  = pct(samples, 0.50);
    state.counters[sp + "_p99_us"]  = pct(samples, 0.99);
    state.counters[sp + "_p999_us"] = pct(samples, 0.999);
    state.counters[sp + "_mean_us"] = sum / static_cast<double>(samples.size());
    if (gateMicros > 0.0) {
        state.counters[sp + "_p99_gate"] = (pct(samples, 0.99) < gateMicros) ? 1.0 : 0.0;
    }
}

// How many iterations to use for the isolated alloc-check phase.
static constexpr int kAllocCheckIter = 10'000;

// ---------------------------------------------------------------------------
// No-op SendFn (no transport — pure in-process measurement).
// ---------------------------------------------------------------------------
static rccg::SendFn makeSendFn()
{
    return [](int32_t, const uint8_t*, std::size_t) -> bool { return true; };
}

// ---------------------------------------------------------------------------
// Build a minimal RecordsView from fabricated field values.
// recordBatch is intentionally empty (zero-copy span) — onRecords uses only
// the metadata fields for routing + advances position via recordCount.
// ---------------------------------------------------------------------------
static rcc::RecordsView makeRecordsView(int32_t topicId, int32_t partition,
                                        int64_t baseOffset, int32_t recordCount)
{
    rcc::RecordsView rv;
    rv.sessionId      = 1;
    rv.correlationId  = 0;
    rv.subscriptionId = 1;
    rv.topicId        = topicId;
    rv.partition      = partition;
    rv.baseOffset     = baseOffset;
    rv.recordBatch    = {};  // empty (no real batch bytes needed for routing bench)
    rv.recordCount    = recordCount;
    return rv;
}

// ---------------------------------------------------------------------------
// Build a GroupConsumerImpl in Stable state with N partitions assigned.
// sendBuf pre-allocated to 4 KiB; commitEntriesScratch pre-reserved.
// ---------------------------------------------------------------------------
static std::shared_ptr<rune::client::GroupConsumerImpl>
makeStableImpl(int nPartitions,
               int32_t topicId    = 0,
               int32_t sessionTimeoutMs = 30000,
               std::chrono::steady_clock::time_point hbDeadline  =
                   std::chrono::steady_clock::now() + std::chrono::hours{1},
               std::chrono::steady_clock::time_point acDeadline  =
                   std::chrono::steady_clock::now() + std::chrono::hours{1})
{
    auto impl = std::make_shared<rune::client::GroupConsumerImpl>();
    impl->groupId       = "bench-group";
    impl->topicIds      = {topicId};
    impl->memberId      = "bench-member-1";
    impl->generationId  = 1;
    impl->state         = rune::client::GroupConsumerState::Stable;
    impl->cfg.sessionTimeoutMs = sessionTimeoutMs;
    impl->cfg.autoCommit = true;
    impl->cfg.autoCommitIntervalMs = 5000;
    impl->sendBuf.resize(4096);
    impl->nextHeartbeat  = hbDeadline;
    impl->nextAutoCommit = acDeadline;

    // Assign N partitions with position 0 (seeded — not sentinel).
    for (int p = 0; p < nPartitions; ++p) {
        rune::client::TopicPartition tp{topicId, p};
        impl->assignment[tp] = 0;
    }
    impl->commitEntriesScratch.reserve(static_cast<std::size_t>(nPartitions));

    // No-op callbacks (avoids closure allocation in the measured section).
    impl->cbRecords  = [](const rune::client::RecordSet&) {};

    return impl;
}

// ===========================================================================
// BM_GCHub_OnRecords — records dispatch + position advance (hot path).
//
// GroupConsumerHub::onRecords():
//   1. snapshot_.load(acquire)
//   2. Iterate consumers: check active, check state==Stable
//   3. assignment.find(tp)
//   4. cbRecords(rv)
//   5. advance position
//
// Gate: p99 < 100µs. Zero-alloc assertion (two-phase: see file header).
// ===========================================================================
static void BM_GCHub_OnRecords(benchmark::State& state)
{
    rccg::GroupConsumerHub hub;
    auto impl = makeStableImpl(1);
    hub.add(impl);

    // Pre-build the RecordsView for (topicId=0, partition=0).
    rcc::RecordsView rv = makeRecordsView(0, 0, 0, 1);

    static constexpr int kWarmup = 50;
    for (int i = 0; i < kWarmup; ++i) {
        hub.onRecords(rv);
        rv.baseOffset++;  // keep advancing so position tracking sees progress
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — no alloc tracking (avoids push_back false-positive).
    for (auto _ : state) {
        rv.baseOffset++;
        const int64_t t0 = nowNsI();
        hub.onRecords(rv);
        const int64_t elapsed = nowNsI() - t0;
        benchmark::ClobberMemory();
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc check — isolated calls, no sample storage.
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        rv.baseOffset++;
        hub.onRecords(rv);
        benchmark::ClobberMemory();
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "onrecords", 100.0);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    if (totalAllocs > 0) {
        state.SkipWithError("onRecords allocated heap (zero-alloc violated)");
    }
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("onRecords p99 >= 100µs (gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCHub_OnRecords)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCHub_DoWork_HeartbeatCadence — doWork() with heartbeat timer fired.
//
// We arm impl->nextHeartbeat = now() so the timer fires on the very first
// doWork call. After each doWork, we re-arm the timer to fire again on the
// next call (simulating the cadence path, not just one call).
//
// This exercises: snapshot load, active check, commit drain, heartbeat arm +
// encode + sendFn call, re-arm nextHeartbeat.
//
// Gate: p99 < 100µs. Zero-alloc assertion (two-phase).
// ===========================================================================
static void BM_GCHub_DoWork_HeartbeatCadence(benchmark::State& state)
{
    const int64_t sessionId = 1;
    auto sendFn = makeSendFn();

    rccg::GroupConsumerHub hub;
    // Arm heartbeat to fire immediately on every doWork call.
    auto impl = makeStableImpl(
        1,
        /*topicId=*/0,
        /*sessionTimeoutMs=*/30000,
        /*hbDeadline=*/std::chrono::steady_clock::now() - std::chrono::seconds{1},
        /*acDeadline=*/std::chrono::steady_clock::now() + std::chrono::hours{1});
    hub.add(impl);

    int64_t clk = 1'000'000'000LL;

    // Warm-up: fire and drain a few times so the sendBuf is at steady-state size.
    for (int i = 0; i < 50; ++i) {
        hub.doWork(clk, sessionId, sendFn);
        // Re-arm the heartbeat timer to fire on next call.
        impl->nextHeartbeat = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — no alloc tracking.
    for (auto _ : state) {
        // Re-arm so heartbeat fires on this iteration.
        impl->nextHeartbeat = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;

        const int64_t t0 = nowNsI();
        const int work = hub.doWork(clk, sessionId, sendFn);
        const int64_t elapsed = nowNsI() - t0;
        benchmark::DoNotOptimize(work);
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc check.
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        impl->nextHeartbeat = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;
        const int work = hub.doWork(clk, sessionId, sendFn);
        benchmark::DoNotOptimize(work);
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "hb_cadence", 100.0);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    if (totalAllocs > 0) {
        state.SkipWithError("doWork/heartbeat allocated heap (zero-alloc violated)");
    }
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("doWork/heartbeat p99 >= 100µs (gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCHub_DoWork_HeartbeatCadence)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCHub_DoWork_CommitCadence — doWork() with auto-commit timer fired.
//
// Arms auto-commit timer to fire immediately. Measures the commit encode
// path for N partitions (commitEntriesScratch reused — zero-alloc).
//
// Gate: p99 < 100µs. Zero-alloc assertion (two-phase).
// ===========================================================================
static void BM_GCHub_DoWork_CommitCadence(benchmark::State& state)
{
    const int nPartitions = static_cast<int>(state.range(0));
    const int64_t sessionId = 1;
    auto sendFn = makeSendFn();

    rccg::GroupConsumerHub hub;
    auto impl = makeStableImpl(
        nPartitions,
        /*topicId=*/0,
        /*sessionTimeoutMs=*/30000,
        /*hbDeadline=*/std::chrono::steady_clock::now() + std::chrono::hours{1},
        /*acDeadline=*/std::chrono::steady_clock::now() - std::chrono::seconds{1});
    hub.add(impl);

    int64_t clk = 1'000'000'000LL;

    // Warm-up.
    for (int i = 0; i < 50; ++i) {
        hub.doWork(clk, sessionId, sendFn);
        impl->nextAutoCommit = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — no alloc tracking.
    for (auto _ : state) {
        impl->nextAutoCommit = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;

        const int64_t t0 = nowNsI();
        const int work = hub.doWork(clk, sessionId, sendFn);
        const int64_t elapsed = nowNsI() - t0;
        benchmark::DoNotOptimize(work);
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc check.
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        impl->nextAutoCommit = std::chrono::steady_clock::now() - std::chrono::seconds{1};
        clk += 1'000LL;
        const int work = hub.doWork(clk, sessionId, sendFn);
        benchmark::DoNotOptimize(work);
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "commit_cadence", 100.0);
    state.counters["partitions"]      = static_cast<double>(nPartitions);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    if (totalAllocs > 0) {
        state.SkipWithError("doWork/commit allocated heap (zero-alloc violated)");
    }
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("doWork/commit p99 >= 100µs (gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCHub_DoWork_CommitCadence)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(1)
    ->Arg(4)
    ->Arg(16)
    ->Arg(64);

// ===========================================================================
// BM_GCHub_DoWork_Stable — doWork idle cost for a Stable consumer.
//
// Heartbeat and auto-commit timers armed far in the future so they don't fire.
// Measures snapshot-load + state-machine-iteration cost when no timers fire.
// Zero-alloc assertion (two-phase).
// ===========================================================================
static void BM_GCHub_DoWork_Stable(benchmark::State& state)
{
    const int64_t sessionId = 1;
    auto sendFn = makeSendFn();

    rccg::GroupConsumerHub hub;
    auto impl = makeStableImpl(4);
    hub.add(impl);

    int64_t clk = 1'000'000'000LL;
    for (int i = 0; i < 50; ++i) {
        hub.doWork(clk, sessionId, sendFn);
        clk += 1'000LL;
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — no alloc tracking.
    for (auto _ : state) {
        clk += 1'000LL;
        const int64_t t0 = nowNsI();
        int work = hub.doWork(clk, sessionId, sendFn);
        const int64_t elapsed = nowNsI() - t0;
        benchmark::DoNotOptimize(work);
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc check.
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        clk += 1'000LL;
        int work = hub.doWork(clk, sessionId, sendFn);
        benchmark::DoNotOptimize(work);
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "dowork_idle", -1.0);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    if (totalAllocs > 0) {
        state.SkipWithError("GroupConsumerHub::doWork idle allocated heap (zero-alloc violated)");
    }
}

BENCHMARK(BM_GCHub_DoWork_Stable)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

} // anonymous namespace

BENCHMARK_MAIN();
