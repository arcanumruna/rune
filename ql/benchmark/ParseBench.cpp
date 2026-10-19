// rune-ql parse benchmark — validates the microsecond-parse claim.
// Measures the cost of parse() on the 'characters' DDL end-to-end.
#include <benchmark/benchmark.h>
#include "rune/ql/Parser.h"

static constexpr std::string_view kCharactersDDL =
    "topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;";

static void BM_ParseCharacters(benchmark::State& state) {
    for (auto _ : state) {
        auto result = rune::ql::parse(kCharactersDDL);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ParseCharacters);

static constexpr std::string_view kTransientDDL =
    "transient topic presence { userId int64, online bool } partitions 8 key userId;";

static void BM_ParseTransientTopic(benchmark::State& state) {
    for (auto _ : state) {
        auto result = rune::ql::parse(kTransientDDL);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ParseTransientTopic);

// Phase 2b Part D: a three-statement console script, and the per-line classification.
static constexpr std::string_view kScript3 =
    "-- three statements\n"
    "topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;\n"
    "alter topic characters add column level int32, set compatibility backward;\n"
    "topic guilds { id int64, name string } partitions 4 key id;\n";

static void BM_ParseScript3(benchmark::State& state) {
    for (auto _ : state) {
        auto result = rune::ql::parseScript(kScript3);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * 3);
}
BENCHMARK(BM_ParseScript3);

static void BM_ClassifyInput(benchmark::State& state) {
    for (auto _ : state) {
        auto s = rune::ql::classifyInput(kScript3);
        benchmark::DoNotOptimize(s);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClassifyInput);
