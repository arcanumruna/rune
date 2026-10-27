// ControlDutyCycleBench — metrics plan Task 12: what SystemMetrics adds to one control-agent duty cycle.
//
// The control agent's doWork does `if (systemMetrics_) work += systemMetrics_->doWork(nowNs);` (D62). This bench
// isolates that increment on a real metrics file and the real /proc/self, in three cases:
//   * NoSystemMetrics  — the optional is empty (metrics file off): the baseline branch.
//   * OffInterval      — sampled once, then every call lands inside the interval: the steady-state cost.
//   * OnInterval       — intervalNs = 1 and nowNs advances every call, so every call samples (pread statm and
//                        status, getdents64 the fd dir): the worst case, which the default 1 s interval hits once
//                        a second.
// Gates (checked after the timed loop, BenchGate.h): zero allocations in every case; OffInterval < 50 ns per cycle;
// OnInterval < 50 µs per cycle (well under one control-loop budget; it runs at most once per interval).
// The full BrokerNode control doWork is not driven here: it needs a whole broker, and the produce path is covered
// by the ABAB single-node produce p99 run (BENCHMARKING.md).
#include "BenchGate.h"

#include "metrics/MetricsFile.h"
#include "metrics/MetricsWriter.h"
#include "metrics/SystemMetrics.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool> g_allocEnabled;

namespace {

constexpr int32_t kSlots = 64;
// Fixed iteration counts: Google Benchmark's iteration-estimation runs (1, 10, 100 ... iterations) would otherwise
// call runCase with so few cycles that the two clock reads dominate ns_per_cycle and trip the time gate.
constexpr benchmark::IterationCount kCycleIterations = 10'000'000;
constexpr benchmark::IterationCount kSampleIterations = 50'000;

// A metrics file in a private temp directory, removed on exit.
struct MetricsFixture {
    std::string dir;
    std::string path;
    metrics::MetricsFile file;
    std::vector<int32_t> stack = std::vector<int32_t>(kSlots);
    std::unique_ptr<metrics::MetricsWriter> writer = std::make_unique<metrics::MetricsWriter>();
    bool ok = false;

    MetricsFixture() {
        char tmpl[] = "/tmp/rune_duty_cycle_XXXXXX";
        const char* d = ::mkdtemp(tmpl);
        if (d == nullptr) return;
        dir = d;
        path = dir + "/m.dat";
        const metrics::CreateParams cp{path.c_str(), kSlots, kSlots, ::getpid(), 1, {}};
        if (metrics::MetricsFile::create(cp, file) != metrics::FileStatus::Ok) return;
        ok = metrics::MetricsWriter::create(file, std::span<int32_t>(stack), *writer) == metrics::WriterStatus::Ok;
    }
    ~MetricsFixture() {
        if (!path.empty()) ::unlink(path.c_str());
        if (!dir.empty()) ::rmdir(dir.c_str());
    }
};

// The control-agent increment under test, exactly as BrokerNode's control doWork writes it.
inline int controlIncrement(std::optional<metrics::SystemMetrics>& sm, int64_t nowNs) noexcept {
    int work = 0;
    if (sm) work += sm->doWork(nowNs);
    return work;
}

enum class Mode { None, OffInterval, OnInterval };

void runCase(benchmark::State& state, Mode mode, double gateNs, const char* gateMsg) {
    MetricsFixture f;
    if (!f.ok) {
        state.SkipWithError("metrics file setup failed");
        return;
    }
    std::optional<metrics::SystemMetrics> sm;
    const int64_t intervalNs = mode == Mode::OnInterval ? 1 : int64_t{3'600} * 1'000'000'000;  // 1 ns, or 1 h
    int64_t nowNs = 1'000'000'000;
    if (mode != Mode::None) {
        sm.emplace();
        const uint8_t key[4] = {1, 0, 0, 0};
        if (sm->init(*f.writer, {intervalNs, "/proc/self"}, std::as_bytes(std::span(key))) !=
            metrics::SystemMetrics::Status::Ok) {
            state.SkipWithError("SystemMetrics init failed");
            return;
        }
        (void)sm->doWork(nowNs);  // the first sample: Armed -> Sampled
    }

    int64_t work = 0;
    g_allocCount.store(0);
    g_allocEnabled.store(true, std::memory_order_release);
    const auto t0 = std::chrono::steady_clock::now();
    for (auto _ : state) {
        ++nowNs;  // OffInterval: stays inside the 1 h interval; OnInterval: past the 1 ns interval every call
        work += controlIncrement(sm, nowNs);
        benchmark::DoNotOptimize(work);
    }
    const auto t1 = std::chrono::steady_clock::now();
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t allocs = g_allocCount.load(std::memory_order_acquire);

    const double nsPerCycle =
        static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
        static_cast<double>(state.iterations() > 0 ? state.iterations() : 1);
    state.counters["ns_per_cycle"] = nsPerCycle;
    state.counters["allocs"] = static_cast<double>(allocs);
    state.counters["sampled_cycles"] = static_cast<double>(work);
    if (!rune::bench::gateCheck(state, allocs == 0, "allocation on the control duty cycle")) return;
    // An OnInterval run that did not sample every call measures the wrong thing.
    if (mode == Mode::OnInterval &&
        !rune::bench::gateCheck(state, work == static_cast<int64_t>(state.iterations()), "OnInterval did not sample every cycle")) {
        return;
    }
    if (mode == Mode::OffInterval && !rune::bench::gateCheck(state, work == 0, "OffInterval sampled inside the interval")) {
        return;
    }
    rune::bench::gateCheck(state, nsPerCycle < gateNs, gateMsg);
}

void BM_ControlDutyCycle_NoSystemMetrics(benchmark::State& state) {
    runCase(state, Mode::None, 50.0, "NoSystemMetrics cycle >= 50 ns");
}
BENCHMARK(BM_ControlDutyCycle_NoSystemMetrics)->Iterations(kCycleIterations);

void BM_ControlDutyCycle_SystemMetricsOffInterval(benchmark::State& state) {
    runCase(state, Mode::OffInterval, 50.0, "SystemMetrics off-interval cycle >= 50 ns");
}
BENCHMARK(BM_ControlDutyCycle_SystemMetricsOffInterval)->Iterations(kCycleIterations);

void BM_ControlDutyCycle_SystemMetricsOnInterval(benchmark::State& state) {
    runCase(state, Mode::OnInterval, 50'000.0, "SystemMetrics on-interval cycle >= 50 us");
}
BENCHMARK(BM_ControlDutyCycle_SystemMetricsOnInterval)->Iterations(kSampleIterations);

}  // namespace
