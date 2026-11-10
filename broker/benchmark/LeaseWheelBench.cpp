// LeaseWheelBench.cpp - lease topics Task 9: the lease timer wheel over the entry store, at 1 k / 100 k / 1 M / 16 M rows.
//
//   BM_Arm           cancel + arm of one row through LeaseTimers (a fresh link: unlink of the old slot is included)
//   BM_Rearm         arm of an already linked row (O(1) unlink + link)
//   BM_IdlePoll      poll when nothing is due: the one `nowNs < nextCheckNs_` compare
//   BM_PollDrain256  poll that hands 256 due rows (the 256 arms are untimed)
//
// Gates, ASSERTED in Task 23 (not here): arm / re-arm p99 < 50 ns, idle poll < 100 ns at 1 M rows.
// Run per broker/bench/BENCHMARKING.md (prepared box, Release, ABAB).  Setup (arming every row once) is outside the timed region.
#include <rune/node/lease/LeaseWheel.h>

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <memory>
#include <random>

namespace {

using namespace rune::node::lease;

constexpr int64_t kNow = 1'000'000'000;

struct Rig {
    std::unique_ptr<LeaseTimers> t;
    std::array<uint32_t, 4096> rids{};
    std::array<int64_t, 4096> spans{};
    uint32_t rows;
    bool ok = false;

    explicit Rig(uint32_t n) : t(std::make_unique<LeaseTimers>(n, kNow)), rows(n) {
        ok = t->reserveFor(n - 1);
        std::mt19937_64 rng(0x1EA5E);
        for (uint32_t i = 0; i < n; ++i) {
            // U[10 s, 1 h] ahead of now: nothing is due or near during the idle-poll case
            t->arm(i, kNow + 10'000'000'000 + static_cast<int64_t>(rng() % 3'590'000'000'001ull), kLeaseNoCheckpoint);
        }
        for (std::size_t i = 0; i < rids.size(); ++i) {
            rids[i]  = static_cast<uint32_t>(rng() % n);
            spans[i] = 10'000'000'000 + static_cast<int64_t>(rng() % 3'590'000'000'001ull);
        }
    }
};

void BM_Arm(benchmark::State& st) {
    Rig r(static_cast<uint32_t>(st.range(0)));
    if (!r.ok) { st.SkipWithError("reserve"); return; }
    std::size_t i = 0;
    for (auto _ : st) {
        const uint32_t rid = r.rids[i & 4095];
        r.t->setState(rid, LeaseEntryState::Emitting, 1);   // the cancel half: unlinks both wheels
        r.t->rearm(rid, kNow + r.spans[i & 4095]);          // stores the deadline (not Armed: no link)
        r.t->setState(rid, LeaseEntryState::Armed);         // the arm half: links at the stored deadline
        ++i;
    }
}

void BM_Rearm(benchmark::State& st) {
    Rig r(static_cast<uint32_t>(st.range(0)));
    if (!r.ok) { st.SkipWithError("reserve"); return; }
    std::size_t i = 0;
    for (auto _ : st) {
        r.t->rearm(r.rids[i & 4095], kNow + r.spans[(i + 1) & 4095]);
        ++i;
    }
}

void BM_IdlePoll(benchmark::State& st) {
    Rig r(static_cast<uint32_t>(st.range(0)));
    if (!r.ok) { st.SkipWithError("reserve"); return; }
    int64_t now = kNow;
    uint64_t handed = 0;
    for (auto _ : st) {
        now += 1;   // monotonic; 1 ns per iteration stays far below the first deadline for any iteration count (nothing is due)
        handed += r.t->pollDeadline(now, [](uint32_t) { return true; }, UINT32_MAX);
    }
    benchmark::DoNotOptimize(handed);
}

void BM_PollDrain256(benchmark::State& st) {
    const uint32_t n = static_cast<uint32_t>(st.range(0));
    Rig r(n);
    if (!r.ok) { st.SkipWithError("reserve"); return; }
    // the background rows sit beyond the run's reach (10 years), so a poll hands exactly the 256 rows armed below
    constexpr int64_t kTenYears = int64_t{10} * 365 * 86'400 * 1'000'000'000;
    for (uint32_t i = 0; i < n; ++i) r.t->rearm(i, kNow + kTenYears);
    int64_t now = kNow;
    uint32_t base = 0;
    uint64_t handed = 0;
    for (auto _ : st) {
        st.PauseTiming();
        for (uint32_t k = 0; k < 256; ++k) r.t->rearm((base + k) % n, now + 1'000'000);
        base = (base + 256) % n;
        now += 2'000'000;
        st.ResumeTiming();
        handed += r.t->pollDeadline(now, [](uint32_t) { return true; }, UINT32_MAX);
        // the handed rows stay Armed but unlinked: the next rearm links them again
    }
    benchmark::DoNotOptimize(handed);
}

}  // namespace

BENCHMARK(BM_Arm)->Arg(1'000)->Arg(100'000)->Arg(1'000'000)->Arg(1 << 24);
BENCHMARK(BM_Rearm)->Arg(1'000)->Arg(100'000)->Arg(1'000'000)->Arg(1 << 24);
BENCHMARK(BM_IdlePoll)->Arg(1'000)->Arg(100'000)->Arg(1'000'000)->Arg(1 << 24);
BENCHMARK(BM_PollDrain256)->Arg(1'000)->Arg(100'000)->Arg(1'000'000)->Arg(1 << 24);
