#include <benchmark/benchmark.h>
#include <rune/storage/PartitionApi.h>
#include <filesystem>

static void BM_Produce(benchmark::State& state) {
    auto dir = std::filesystem::temp_directory_path() / "rune_bench";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    rune::storage::PartitionApi api(dir.string());
    int64_t ts = 0;
    for (auto _ : state) {
        api.produce("k", "value-payload", ts++);
    }
    std::filesystem::remove_all(dir);
}
BENCHMARK(BM_Produce);
