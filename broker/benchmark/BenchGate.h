#pragma once
// BenchGate.h — Task 23 fix round 1 (R-I2): benchmark gates that neither crash nor get recorded as passing.
//
// Google Benchmark 1.8.2 .. 1.9.5 dereferences a null `reports[0].statistics` in ComputeStats when
// repetition 0 called State::SkipWithError and at least two later repetitions did not (benchmark_runner.cc
// sets report.statistics only for non-skipped runs).  A GATE (latency / allocation / correctness check made
// after the timed loop) therefore must not use SkipWithError: it calls gateFail(), which
//   * sets the counter gate_failed = 1 on the repetition (gatePass/gateCheck set it to 0 on a passing one, so
//     every repetition carries the same counter set), and
//   * records {benchmark name, message} in a main-thread-only list that BenchmarkMain prints and turns into a
//     nonzero exit code after RunSpecifiedBenchmarks().
// SkipWithError stays ONLY for "cannot run at all" setup failures before the timed loop (they skip every
// repetition alike, so ComputeStats returns early) and for in-loop aborts that must leave the loop.
// Thread rule: call from thread 0, after the loop (the list is touched by that one thread at a time and read
// by main after all benchmark threads joined).  Header-only: also compiled at the parity base.
#include <benchmark/benchmark.h>

#include <string>
#include <vector>

namespace rune::bench {

struct GateFailure {
    std::string benchmark;
    std::string message;
};

inline std::vector<GateFailure>& gateFailures() {
    static std::vector<GateFailure> v;
    return v;
}

inline void gatePass(benchmark::State& state) { state.counters["gate_failed"] = 0; }

inline void gateFail(benchmark::State& state, const char* msg) {
    state.counters["gate_failed"] = 1;
    if (state.thread_index() == 0) gateFailures().push_back(GateFailure{state.name(), msg});
}

/// gateFail when !ok, else gatePass.  Returns ok.
inline bool gateCheck(benchmark::State& state, bool ok, const char* msg) {
    if (ok) gatePass(state); else gateFail(state, msg);
    return ok;
}

/// Round 2 (R-N1): a display-reporter wrapper that records every SKIPPED (errored) run - a SkipWithError that left the loop
/// (a setup failure, an in-loop abort such as an unanswered request) - so main() can fail the process.  Everything is forwarded
/// to the inner (default) display reporter, so --benchmark_format etc. keep working.  Main thread only.
class SkipTrackingReporter final : public benchmark::BenchmarkReporter {
public:
    explicit SkipTrackingReporter(benchmark::BenchmarkReporter* inner) : inner_(inner) {}
    bool ReportContext(const Context& c) override { return inner_->ReportContext(c); }
    void ReportRunsConfig(double minTime, bool hasExplicitIters, benchmark::IterationCount iters) override {
        inner_->ReportRunsConfig(minTime, hasExplicitIters, iters);
    }
    void ReportRuns(const std::vector<Run>& runs) override {
        for (const Run& r : runs)
            if (r.skipped != benchmark::internal::NotSkipped) skipped_.push_back(r.benchmark_name() + ": " + r.skip_message);
        inner_->ReportRuns(runs);
    }
    void Finalize() override { inner_->Finalize(); }
    [[nodiscard]] const std::vector<std::string>& skipped() const { return skipped_; }
private:
    benchmark::BenchmarkReporter* inner_;
    std::vector<std::string>      skipped_;
};

}  // namespace rune::bench
