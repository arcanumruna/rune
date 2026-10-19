// benchmark/ValidatorBenchmark.cpp
#include <benchmark/benchmark.h>
#include <rune/codec/Validator.h>
using namespace rune::codec;

static void BM_Validate_Small(benchmark::State& s) {
    Validator v(DescriptorBuilder("c")
                    .add("id", PrimType::Int64)
                    .add("s",  PrimType::Int32)
                    .build());
    std::vector<uint8_t> rec(4 + 12, 0);
    rec[0] = 12;  // blockLength=12, ver=0
    for (auto _ : s) {
        auto r = v.validate(rec);
        benchmark::DoNotOptimize(r.ok);
    }
}
BENCHMARK(BM_Validate_Small)->Unit(benchmark::kNanosecond);
BENCHMARK_MAIN();
