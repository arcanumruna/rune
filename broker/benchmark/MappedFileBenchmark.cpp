// MappedFile benchmark.
//
// Measures sequential-write throughput into a memory-mapped file and the
// cost of msync(MS_SYNC) on a dirty page.  Both benchmarks write 4 KiB
// pages so each iteration exercises a full OS page fault + store path.

#include <benchmark/benchmark.h>
#include <rune/storage/MappedFile.h>

#include <filesystem>
#include <cstring>
#include <cstdint>

namespace {

static constexpr size_t kPageSize   = 4096;
static constexpr size_t kFilePages  = 256;          // 1 MiB total mapping
static constexpr size_t kFileBytes  = kFilePages * kPageSize;

// ---------------------------------------------------------------------------
// BM_MappedFile_SequentialWrite
//
// Creates a 1 MiB mapping, then iterates through 4 KiB pages writing a
// sentinel byte at the start of each page.  Setup (file creation) is done
// outside the timed loop; only the stores are measured.
// ---------------------------------------------------------------------------
static void BM_MappedFile_SequentialWrite(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "rune_bench_mf_sw";
    fs::remove_all(dir);
    fs::create_directories(dir);
    auto path = (dir / "mapped.bin").string();

    rune::storage::MappedFile mf(path, kFileBytes, /*writable=*/true);
    uint8_t* base = mf.data();

    size_t page = 0;
    for (auto _ : state) {
        uint8_t* dst = base + (page % kFilePages) * kPageSize;
        // Write a recognisable pattern to the first 8 bytes of the page
        std::memset(dst, static_cast<int>(page & 0xFF), 8);
        benchmark::DoNotOptimize(dst);
        benchmark::ClobberMemory();
        ++page;
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 8);
    fs::remove_all(dir);
}

BENCHMARK(BM_MappedFile_SequentialWrite)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_MappedFile_Sync
//
// Writes one byte to the mapping then calls sync() (msync MS_SYNC) — the
// dominant cost in durable-log write paths.  PauseTiming wraps the write so
// only the sync itself is measured.
// ---------------------------------------------------------------------------
static void BM_MappedFile_Sync(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "rune_bench_mf_sync";
    fs::remove_all(dir);
    fs::create_directories(dir);
    auto path = (dir / "mapped.bin").string();

    rune::storage::MappedFile mf(path, kPageSize, /*writable=*/true);
    uint8_t* base = mf.data();

    for (auto _ : state) {
        // Dirty the page outside timed region so we measure pure msync cost
        state.PauseTiming();
        base[0] = static_cast<uint8_t>(state.iterations() & 0xFF);
        benchmark::ClobberMemory();
        state.ResumeTiming();

        mf.sync();
        benchmark::DoNotOptimize(base[0]);
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}

BENCHMARK(BM_MappedFile_Sync)->Unit(benchmark::kMicrosecond);

} // namespace
