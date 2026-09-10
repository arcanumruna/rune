// TermLog benchmark.
//
// Measures the three primary TermLog operations:
//   AppendTerm       — sequential append of new open term entries (disk write path).
//   GetTermEntry     — O(1) unordered_map lookup by leadershipTermId (pure memory).
//   CommitLogPosition — patch a single int64_t field in an existing record (disk
//                       pwrite path; no full-record rewrite).
//
// Disk benchmarks use a unique temp dir per run and clean up on exit.
// Pre-built state for GetTermEntry is constructed fully OUTSIDE the timed loop.

#include <benchmark/benchmark.h>
#include <rune/consensus/TermLog.h>

#include <filesystem>
#include <string>

namespace {

using rune::consensus::TermLog;

// ---------------------------------------------------------------------------
// BM_TermLog_AppendTerm
//
// Sequential append of OPEN term entries.  When the log grows too large
// (avoiding unbounded disk usage), the bench uses PauseTiming to recreate
// the temp dir and a fresh TermLog.
// ---------------------------------------------------------------------------
static void BM_TermLog_AppendTerm(benchmark::State& state) {
    namespace fs = std::filesystem;

    static constexpr int kWrapAfter = 512; // recreate after this many appends

    auto dir  = fs::temp_directory_path() / "rune_bench_termlog_append";
    auto path = (dir / "term.log").string();
    fs::remove_all(dir);
    fs::create_directories(dir);

    auto* log = new TermLog(path);
    rune::consensus::Term nextTerm = 0;

    for (auto _ : state) {
        if (nextTerm > 0 && nextTerm % kWrapAfter == 0) {
            // Wrap: recreate log on same path (bounded disk usage).
            state.PauseTiming();
            delete log;
            fs::remove_all(dir);
            fs::create_directories(dir);
            log      = new TermLog(path);
            nextTerm = 0;
            state.ResumeTiming();
        }

        log->appendTerm(
            /*recordingId=*/static_cast<int64_t>(nextTerm),
            /*leadershipTermId=*/nextTerm,
            /*termBaseLogPosition=*/nextTerm * 4096,
            /*timestamp=*/1'700'000'000'000LL + nextTerm);
        benchmark::DoNotOptimize(nextTerm);
        ++nextTerm;
    }

    delete log;
    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_TermLog_AppendTerm)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_TermLog_GetTermEntry
//
// Pre-build a TermLog with N terms OUTSIDE the loop.  The timed section is
// a single getTermEntry() call — an unordered_map lookup (indexByTerm_ path).
//
// Parameterised: 16 / 128 terms (typical cluster lifetimes).
// ---------------------------------------------------------------------------
static void BM_TermLog_GetTermEntry(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int N = static_cast<int>(state.range(0));

    auto dir  = fs::temp_directory_path() /
                ("rune_bench_termlog_get_" + std::to_string(N));
    auto path = (dir / "term.log").string();
    fs::remove_all(dir);
    fs::create_directories(dir);

    // Build N terms OUTSIDE the timed loop.
    {
        TermLog builder(path);
        for (int i = 0; i < N; ++i) {
            builder.appendTerm(
                static_cast<int64_t>(i),
                static_cast<rune::consensus::Term>(i),
                static_cast<int64_t>(i) * 4096,
                1'700'000'000'000LL + i);
        }
    }

    // Re-open so the in-memory index is populated from the file.
    TermLog log(path);
    // Look up a mid-range term to exercise the hash map (not just the first slot).
    const rune::consensus::Term kTarget = static_cast<rune::consensus::Term>(N / 2);

    for (auto _ : state) {
        auto entry = log.getTermEntry(kTarget);
        benchmark::DoNotOptimize(entry);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["terms"] = benchmark::Counter(static_cast<double>(N));
    fs::remove_all(dir);
}
BENCHMARK(BM_TermLog_GetTermEntry)
    ->Arg(16)->Arg(128)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_TermLog_CommitLogPosition
//
// A single open term entry is appended OUTSIDE the loop; the loop repeatedly
// re-commits the same term with an increasing logPosition.  Measures the
// pwrite patch path (patchInt64 on the logPosition field only — no full
// record rewrite).
//
// The term is re-appended via PauseTiming once the commit count wraps, to
// keep the term's in-memory entry in an open (uncommitted) state and exercise
// the normal update path rather than an out-of-contract re-commit.
// ---------------------------------------------------------------------------
static void BM_TermLog_CommitLogPosition(benchmark::State& state) {
    namespace fs = std::filesystem;

    static constexpr int kWrapAfter = 256;

    auto dir  = fs::temp_directory_path() / "rune_bench_termlog_commit";
    auto path = (dir / "term.log").string();
    fs::remove_all(dir);
    fs::create_directories(dir);

    auto* log = new TermLog(path);
    rune::consensus::Term currentTerm = 0;
    log->appendTerm(0, currentTerm, 0, 1'700'000'000'000LL);

    int64_t commitPos  = 64;
    int     iteration  = 0;

    for (auto _ : state) {
        if (iteration > 0 && iteration % kWrapAfter == 0) {
            // Advance to a new term so commitLogPosition finds a fresh OPEN entry.
            state.PauseTiming();
            ++currentTerm;
            log->appendTerm(
                static_cast<int64_t>(currentTerm),
                currentTerm,
                commitPos,
                1'700'000'000'000LL + currentTerm);
            commitPos = commitPos + 64; // ensure next commit > base
            state.ResumeTiming();
        }

        log->commitLogPosition(currentTerm, commitPos);
        benchmark::DoNotOptimize(commitPos);
        commitPos += 64;
        ++iteration;
    }

    delete log;
    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_TermLog_CommitLogPosition)->Unit(benchmark::kNanosecond);

} // namespace
