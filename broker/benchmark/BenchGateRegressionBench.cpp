// BenchGateRegressionBench.cpp — Task 23 fix round 1 (R-I2): self-test benchmarks for BenchGate.h, built into
// rune_bench_gate_selftest only (never into rune_broker_benchmark).  Iterations(1): every repetition is exactly one
// call, so "repetition 0" is the first call.
//   BM_GatePassesRegression                  a passing gate           -> exit 0
//   BM_GateFirstRepetitionFailsRegression    gate fails on repetition 0 only, passes on 1..n
//                                            -> no crash, aggregates printed, exit 1
//   BM_GateAlwaysFailsRegression             -> exit 1 (ctest WILL_FAIL)
//   BM_GateSkippedRunRegression              every repetition skipped -> exit 2 + "BENCH SKIPPED"
//   BM_GateLibrarySkipFirstRepetition        documents the library bug the gates avoid: SkipWithError on repetition 0
//                                            with >= 2 later passing repetitions crashes benchmark 1.8.2 .. 1.9.5
//                                            in ComputeStats (exit 139).  NOT a ctest (a crash is the expected result).
#include "BenchGate.h"

#include <benchmark/benchmark.h>

namespace {

void BM_GatePassesRegression(benchmark::State& state) {
    for (auto _ : state) benchmark::DoNotOptimize(state.iterations());
    rune::bench::gateCheck(state, true, "unreachable");
}
BENCHMARK(BM_GatePassesRegression)->Iterations(1);

void BM_GateFirstRepetitionFailsRegression(benchmark::State& state) {
    static int calls = 0;   // main thread, one call per repetition (Iterations(1))
    const bool first = (calls++ == 0);
    for (auto _ : state) benchmark::DoNotOptimize(state.iterations());
    rune::bench::gateCheck(state, !first, "gate failed on repetition 0");
}
BENCHMARK(BM_GateFirstRepetitionFailsRegression)->Iterations(1);

void BM_GateAlwaysFailsRegression(benchmark::State& state) {
    for (auto _ : state) benchmark::DoNotOptimize(state.iterations());
    rune::bench::gateFail(state, "this gate always fails");
}
BENCHMARK(BM_GateAlwaysFailsRegression)->Iterations(1);

void BM_GateLibrarySkipFirstRepetition(benchmark::State& state) {
    static int calls = 0;
    const bool first = (calls++ == 0);
    for (auto _ : state) { benchmark::DoNotOptimize(state.iterations()); state.SetIterationTime(1e-6); }
    state.counters["c"] = 1;
    if (first) state.SkipWithError("repetition 0 skipped (library crash reproducer)");
}
BENCHMARK(BM_GateLibrarySkipFirstRepetition)->Iterations(1)->UseManualTime();

// R-N1: SkipWithError on EVERY repetition (a setup failure / an in-loop abort): no crash (ComputeStats returns early), exit 2.
void BM_GateSkippedRunRegression(benchmark::State& state) {
    state.SkipWithError("this benchmark cannot run");
    for (auto _ : state) {}
}
BENCHMARK(BM_GateSkippedRunRegression)->Iterations(1);

}  // namespace
