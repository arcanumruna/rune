// NodeStateFile benchmark.
//
// Measures the two hot paths of the durable candidateTermId file:
//   ProposeMax — propose a monotonically increasing candidateTermId
//               (fileSyncLevel=0 so msync is out of the measured path;
//                the store order logPosition→timestamp→candidateTermId
//                with RELEASE is still in scope).
//   CandidateTermIdRead — ACQUIRE-ordered read of the volatile slot.
//
// A unique temp file per benchmark run avoids cross-benchmark interference.
// The file is unlinked in the state teardown.

#include <benchmark/benchmark.h>
#include <rune/consensus/NodeStateFile.h>

#include <filesystem>
#include <string>

namespace {

using rune::consensus::NodeStateFile;

// ---------------------------------------------------------------------------
// BM_NodeStateFile_ProposeMax
//
// Proposes monotonically increasing candidateTermIds so every call takes the
// write path (candidateTermId > current).  fileSyncLevel=0 keeps msync out
// of the measured window; the release-store ordering is still exercised.
// ---------------------------------------------------------------------------
static void BM_NodeStateFile_ProposeMax(benchmark::State& state) {
    namespace fs = std::filesystem;

    auto path = (fs::temp_directory_path() /
                 "rune_bench_nsf_propose.state").string();
    fs::remove(path);  // start fresh

    // fileSyncLevel=0: no msync, so only the atomic RELEASE store is timed.
    NodeStateFile nsf(path, /*fileSyncLevel=*/0);

    int64_t term     = 0;
    int64_t logPos   = 0;
    int64_t tsMs     = 1'700'000'000'000LL;

    for (auto _ : state) {
        ++term;
        logPos += 64;
        tsMs   += 1;
        auto result = nsf.proposeMaxCandidateTermId(term, logPos, tsMs);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove(path);
}
BENCHMARK(BM_NodeStateFile_ProposeMax)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_NodeStateFile_CandidateTermIdRead
//
// ACQUIRE-ordered read of the candidateTermId slot.  A single write is done
// outside the loop to ensure the slot holds a non-NULL_VALUE so the read
// actually touches the mapped page.
// ---------------------------------------------------------------------------
static void BM_NodeStateFile_CandidateTermIdRead(benchmark::State& state) {
    namespace fs = std::filesystem;

    auto path = (fs::temp_directory_path() /
                 "rune_bench_nsf_read.state").string();
    fs::remove(path);

    NodeStateFile nsf(path, /*fileSyncLevel=*/0);
    // Prime the slot so it is not NULL_VALUE.
    nsf.proposeMaxCandidateTermId(/*candidateTermId=*/1, /*logPosition=*/0, /*timestampMs=*/0);

    for (auto _ : state) {
        auto term = nsf.candidateTermId();
        benchmark::DoNotOptimize(term);
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove(path);
}
BENCHMARK(BM_NodeStateFile_CandidateTermIdRead)->Unit(benchmark::kNanosecond);

} // namespace
