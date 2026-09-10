// Single translation unit that provides BENCHMARK_MAIN() for the entire
// rune_protocol_benchmark executable. All other benchmark TUs register their
// benchmarks via the BENCHMARK() macro without defining main().
#include <benchmark/benchmark.h>

BENCHMARK_MAIN();
