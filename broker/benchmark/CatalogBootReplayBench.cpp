// CatalogBootReplayBench.cpp — catalog history Task 23, spec §10 gate: BrokerNode's boot step
// (bootFromCatalog_, via the bootFromCatalogForBench seam) over a 10 000-event __catalog.  Gate:
// every iteration < 1 s (SkipWithError otherwise); median recorded in the Task 23 commit.
#include "CatalogBootLog.h"
#include <rune/node/BrokerNode.h>
#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <string>
#include <unistd.h>
namespace {
struct BootBenchDir {
    std::filesystem::path path;
    rune::node::test::CatalogBootLogShape shape;
    BootBenchDir() : path(std::filesystem::temp_directory_path() /
                          ("rune_bootbench_" + std::to_string(::getpid()))) {
        std::filesystem::remove_all(path);
        shape = rune::node::test::writeCatalogBootLog(path);
    }
    ~BootBenchDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};
void BM_CatalogBootReplay10k(benchmark::State& state) {
    spdlog::set_level(spdlog::level::warn);
    static const BootBenchDir dir;
    for (auto _ : state) {
        const auto r = rune::node::BrokerNode::bootFromCatalogForBench(dir.path.string());
        if (r.appliedEvents != 10'000 || r.halted || r.bootTopics != dir.shape.bootTopics ||
            r.droppedIds != dir.shape.droppedIds) {
            state.SkipWithError("the boot step did not apply the 10 000-event catalog exactly"); return;
        }
        if (r.bootNs >= 1'000'000'000) { state.SkipWithError("GATE FAILED: 10k-event boot replay >= 1 s"); return; }
        state.SetIterationTime(static_cast<double>(r.bootNs) / 1e9);
    }
    state.counters["events"] = 10'000;
}
}  // namespace
BENCHMARK(BM_CatalogBootReplay10k)->UseManualTime()->Iterations(3)->Unit(benchmark::kMillisecond);
