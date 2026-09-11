// ClusterMember benchmark.
//
// Measures the hot-path consensus compute functions:
//   compareLog  — 4-field comparison used in canvass/up-to-date checks.
//   quorumThreshold — floor(N/2)+1, called on every commit-path check.
//   quorumPosition  — insertion-sort of active member log positions (NO per-call
//                     allocation; ranked buffer is caller-owned, pre-allocated
//                     outside the loop).
//   hasQuorumAtPosition — per-member active+position scan used by the leader.
//
// All benchmarks produce real work and use DoNotOptimize to defeat DCE.

#include <benchmark/benchmark.h>
#include <rune/consensus/ClusterMember.h>

#include <array>
#include <cstdint>
#include <vector>

namespace {

using rune::consensus::ClusterMember;

// ---------------------------------------------------------------------------
// BM_ClusterMember_CompareLog
//
// Two members with different leadershipTermId/logPosition; the 4-arg static
// overload is the hot canvass-path call.  Result is DoNotOptimize'd.
// ---------------------------------------------------------------------------
static void BM_ClusterMember_CompareLog(benchmark::State& state) {
    // lhs is "more advanced" — different term, different position.
    constexpr rune::consensus::Term lhsTerm = 7;
    constexpr int64_t               lhsPos  = 4096;
    constexpr rune::consensus::Term rhsTerm = 6;
    constexpr int64_t               rhsPos  = 8192;

    for (auto _ : state) {
        int result = ClusterMember::compareLog(lhsTerm, lhsPos, rhsTerm, rhsPos);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClusterMember_CompareLog)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ClusterMember_QuorumThreshold
//
// Parameterised over cluster sizes 3/5/7.  The computation is a single
// right-shift+add; benchmarked to confirm it stays sub-nanosecond.
// ---------------------------------------------------------------------------
static void BM_ClusterMember_QuorumThreshold(benchmark::State& state) {
    const int memberCount = static_cast<int>(state.range(0));
    for (auto _ : state) {
        int threshold = ClusterMember::quorumThreshold(memberCount);
        benchmark::DoNotOptimize(threshold);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClusterMember_QuorumThreshold)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ClusterMember_QuorumPosition
//
// Hot commit-path compute: given N active members with varied logPositions,
// find the K-th highest (the quorum position) via insertion-sort.
//
// The ranked-positions buffer is pre-allocated OUTSIDE the loop to the exact
// quorumThreshold(N) size — this is the no-allocation contract: quorumPosition
// itself does no dynamic allocation; the caller owns and reuses the buffer.
//
// Parameterised: 3 / 5 / 7 members.
// ---------------------------------------------------------------------------
static void BM_ClusterMember_QuorumPosition(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));

    // Build N members with monotonically increasing logPositions + timestamps
    // that make all of them "active" (nowNs - timeoutNs < timeOfLastAppendPositionNs).
    constexpr int64_t kNowNs     = 1'000'000'000LL;
    constexpr int64_t kTimeoutNs = 500'000'000LL;

    std::vector<ClusterMember> members(static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i) {
        members[static_cast<std::size_t>(i)].id                         = i;
        members[static_cast<std::size_t>(i)].logPosition                = static_cast<int64_t>((i + 1) * 1024);
        // timeOfLastAppendPositionNs > nowNs - timeoutNs  → isActive == true
        members[static_cast<std::size_t>(i)].timeOfLastAppendPositionNs = kNowNs - kTimeoutNs + 1LL;
    }

    // Pre-allocate ranked buffer OUTSIDE the loop (no per-call allocation).
    const int K = ClusterMember::quorumThreshold(N);
    // ASSERT (comment): quorumPosition() does no per-call allocation —
    // the ranked buffer below is the sole scratch space and is caller-owned.
    std::vector<int64_t> ranked(static_cast<std::size_t>(K));

    for (auto _ : state) {
        int64_t pos = ClusterMember::quorumPosition(members, ranked, kNowNs, kTimeoutNs);
        benchmark::DoNotOptimize(pos);
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["members"] = benchmark::Counter(static_cast<double>(N));
}
BENCHMARK(BM_ClusterMember_QuorumPosition)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ClusterMember_HasQuorumAtPosition
//
// Counts members that have reached a given leadershipTermId + logPosition
// and compares against the quorum threshold.  Called by the leader on every
// incoming append-position acknowledgement.
//
// Parameterised over cluster sizes 3/5/7.  The members vector is built OUTSIDE
// the loop from state.range(0).  logPositions are varied so the compiler cannot
// constant-fold the result: half the members (rounded up) have the target
// position; the rest lag.  Both the bool result AND the size are DoNotOptimize'd
// to block DCE.  This ensures a realistic scan of N live ClusterMember objects.
// ---------------------------------------------------------------------------
static void BM_ClusterMember_HasQuorumAtPosition(benchmark::State& state) {
    const int     N          = static_cast<int>(state.range(0));
    const int64_t kNowNs     = 2'000'000'000LL;
    const int64_t kTimeoutNs = 500'000'000LL;
    const rune::consensus::Term kLeaderTerm = 3;
    const int64_t kTargetPos = 2048LL;

    // Build N members: quorum members have reached kTargetPos; remainder lag.
    // Use a vector so size is runtime-determined (prevents loop unrolling / folding).
    std::vector<ClusterMember> members(static_cast<std::size_t>(N));
    const int quorum = ClusterMember::quorumThreshold(N);
    for (int i = 0; i < N; ++i) {
        auto idx = static_cast<std::size_t>(i);
        members[idx].id                         = i;
        members[idx].leadershipTermId           = kLeaderTerm;
        members[idx].logLeadershipTermId        = kLeaderTerm; // same branch (C6 fix)
        members[idx].timeOfLastAppendPositionNs = kNowNs - kTimeoutNs + 1LL; // active
        // First `quorum` members have reached target; others lag behind by varying amounts
        // so positions aren't constant (compiler can't fold the individual comparisons).
        members[idx].logPosition = (i < quorum)
            ? kTargetPos + static_cast<int64_t>(i * 64)   // at or past target
            : kTargetPos / 2 - static_cast<int64_t>(i * 32); // behind
    }

    for (auto _ : state) {
        // Read size from state so the compiler sees it as non-constant.
        int sz = static_cast<int>(state.range(0));
        benchmark::DoNotOptimize(sz);

        bool result = ClusterMember::hasQuorumAtPosition(
            std::span<const ClusterMember>(members.data(), static_cast<std::size_t>(sz)),
            kLeaderTerm, kLeaderTerm, kTargetPos, kNowNs, kTimeoutNs);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["members"] = benchmark::Counter(static_cast<double>(N));
}
BENCHMARK(BM_ClusterMember_HasQuorumAtPosition)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

} // namespace
