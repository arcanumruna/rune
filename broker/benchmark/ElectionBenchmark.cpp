// ElectionBenchmark.cpp — P2P-10: Election state-machine benchmarks.
//
// Benchmarks:
//   BM_Election_SingleNodeToClosed  — full election: INIT → … → CLOSED (single-member cluster).
//     - construct + drive under PauseTiming each iteration
//     - jump nowNs to nextDeadlineNs instead of spinning 1ns (bounded loop ≤200)
//     - SetItemsProcessed(1) per iteration (one complete election)
//
//   BM_Election_OnRequestVote  — pre-built 3-member follower; time one onRequestVote(grant).
//     - per-iteration: call onRequestVote on a fresh Election in CANVASS state
//     - rebuild under PauseTiming every kWrap iterations to stay in the grant path
//
// Disk files (NodeStateFile + TermLog) are created under PauseTiming and
// removed in teardown.  All doWork() calls use a manually-advanced clock —
// no system clock, no busy-spin.

#include <benchmark/benchmark.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/Types.h>

#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>
#include <climits>
#include <memory>

namespace {

namespace fs = std::filesystem;
using namespace rune::consensus;

// ---------------------------------------------------------------------------
// NullPublisher — discards all outbound messages.
// ---------------------------------------------------------------------------
struct NullPublisher final : public ConsensusPublisher {
    void canvassPosition(NodeId, Term, int64_t, Term, NodeId) override {}
    void requestVote(NodeId, Term, int64_t, Term, NodeId) override {}
    void placeVote(NodeId, Term, Term, int64_t, NodeId, NodeId, bool) override {}
    void newLeadershipTerm(NodeId, Term, Term, int64_t, int64_t,
                           Term, int64_t, int64_t, int64_t, int64_t,
                           NodeId, int32_t, bool, int64_t) override {}
    void appendPosition(NodeId, Term, int64_t, NodeId, uint8_t) override {}
    void commitPosition(NodeId, Term, int64_t, NodeId) override {}
};

static constexpr int64_t kElectionTimeoutNs      = 150'000'000LL;
static constexpr int64_t kStatusIntervalNs        =  50'000'000LL;
static constexpr int64_t kStartupCanvassTimeoutNs = 300'000'000LL;

// Build a 1-member ClusterMember vector (self = 0).
static std::vector<ClusterMember> makeMembers1() {
    ClusterMember m;
    m.id                         = 0;
    m.logPosition                = 0;
    m.logLeadershipTermId        = 0;
    m.leadershipTermId           = 0;
    m.timeOfLastAppendPositionNs = 0; // not used for single-member quorum
    return {m};
}

// Build a 3-member ClusterMember vector.
static std::vector<ClusterMember> makeMembers3() {
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; ++i) {
        ms[i].id                  = i;
        ms[i].logPosition         = 0;
        ms[i].logLeadershipTermId = 0;
        ms[i].leadershipTermId    = 0;
    }
    return ms;
}

// Unique temp path generator.
static int s_pathCounter = 0;

static std::string makeTempPath(const char* prefix, const char* suffix) {
    return (fs::temp_directory_path() /
            (std::string(prefix) + "_" +
             std::to_string(++s_pathCounter) + suffix)).string();
}

// ---------------------------------------------------------------------------
// BM_Election_SingleNodeToClosed
//
// Each iteration:
//   PauseTiming  → create temp dirs, NSF, TermLog, Election
//   ResumeTiming → drive Election to CLOSED via bounded loop (≤200 advances)
//   (teardown)   → remove temp files
//
// Clock advancement: jump nowNs directly to election.nextDeadlineNs() to avoid
// busy-spin.  The iteration is capped at 200 doWork calls as a safety net.
// ---------------------------------------------------------------------------
static void BM_Election_SingleNodeToClosed(benchmark::State& state) {
    NullPublisher pub;

    std::string nsfPath, tlPath;
    std::unique_ptr<NodeStateFile> nsf;
    std::unique_ptr<TermLog>       tl;
    std::unique_ptr<Election>      election;

    auto rebuild = [&]() {
        // Clean up previous.
        if (!nsfPath.empty()) { std::error_code ec; fs::remove(nsfPath, ec); }
        if (!tlPath.empty())  { std::error_code ec; fs::remove(tlPath,  ec); }

        nsfPath = makeTempPath("rune_bench_elec_nsf", ".nsf");
        tlPath  = makeTempPath("rune_bench_elec_tl",  ".tl");

        nsf = std::make_unique<NodeStateFile>(nsfPath, /*fileSyncLevel=*/0);
        tl  = std::make_unique<TermLog>(tlPath);
        election = std::make_unique<Election>(
            /*isNodeStartup=*/true,
            /*gracefulClosedLeaderId=*/-1,
            /*leadershipTermId=*/0,
            /*logLeadershipTermId=*/0,
            /*logPosition=*/0,
            /*appendPosition=*/0,
            makeMembers1(),
            /*thisMemberId=*/0,
            pub, *nsf, *tl,
            kElectionTimeoutNs,
            kStatusIntervalNs,
            kStartupCanvassTimeoutNs,
            /*rngSeed=*/42);
    };

    for (auto _ : state) {
        state.PauseTiming();
        rebuild();
        state.ResumeTiming();

        // Drive to CLOSED. A single-member cluster closes in a small number of steps.
        constexpr int kMaxSteps = 200;
        int64_t nowNs = 1'000'000LL;
        int steps = 0;

        while (!election->isClosed() && steps < kMaxSteps) {
            int64_t deadline = election->nextDeadlineNs();
            if (deadline > nowNs) {
                nowNs = deadline;
            }
            int w = election->doWork(nowNs);
            benchmark::DoNotOptimize(w);
            ++steps;
            nowNs += 1; // ensure time moves forward even on no-deadline states
        }

        benchmark::DoNotOptimize(steps);
    }

    state.SetItemsProcessed(state.iterations());

    // Cleanup.
    if (!nsfPath.empty()) { std::error_code ec; fs::remove(nsfPath, ec); }
    if (!tlPath.empty())  { std::error_code ec; fs::remove(tlPath,  ec); }
}
BENCHMARK(BM_Election_SingleNodeToClosed)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Election_OnRequestVote
//
// Pre-build a 3-member Election in CANVASS state.  Time a single
// onRequestVote() call on the grant path (candidate has >= our log position).
//
// The Election is rebuilt under PauseTiming every iteration because a single
// onRequestVote(grant) transitions state (→ FollowerBallot), meaning the next
// call would not be on the grant path.  Rebuild cost is kept outside the timer.
// ---------------------------------------------------------------------------
static void BM_Election_OnRequestVote(benchmark::State& state) {
    NullPublisher pub;

    std::string nsfPath = makeTempPath("rune_bench_elec3_nsf", ".nsf");
    std::string tlPath  = makeTempPath("rune_bench_elec3_tl",  ".tl");

    std::unique_ptr<NodeStateFile> nsf;
    std::unique_ptr<TermLog>       tl;
    std::unique_ptr<Election>      election;

    auto rebuild3 = [&]() {
        // Reuse the same paths — delete and recreate.
        {
            std::error_code ec;
            fs::remove(nsfPath, ec);
            fs::remove(tlPath, ec);
        }
        nsf = std::make_unique<NodeStateFile>(nsfPath, /*fileSyncLevel=*/0);
        tl  = std::make_unique<TermLog>(tlPath);
        election = std::make_unique<Election>(
            /*isNodeStartup=*/false,
            /*gracefulClosedLeaderId=*/-1,
            /*leadershipTermId=*/0,
            /*logLeadershipTermId=*/0,
            /*logPosition=*/0,
            /*appendPosition=*/0,
            makeMembers3(),
            /*thisMemberId=*/0,   // self = node 0 (follower)
            pub, *nsf, *tl,
            kElectionTimeoutNs,
            kStatusIntervalNs,
            kStartupCanvassTimeoutNs,
            /*rngSeed=*/42);

        // Advance through INIT → CANVASS (immediate transition on non-startup 3-member).
        int64_t nowNs = 1'000'000LL;
        election->doWork(nowNs);
    };

    for (auto _ : state) {
        state.PauseTiming();
        rebuild3();
        state.ResumeTiming();

        // Grant path: candidate 1 has term=1, logLtId=0, logPos=0 (equal to ours → grant).
        constexpr int64_t kNow         = 2'000'000LL;
        constexpr Term    kCandTerm    = 1;
        constexpr Term    kLogLtId     = 0;
        constexpr int64_t kLogPos      = 0;
        constexpr NodeId  kCandidateId = 1;

        election->onRequestVote(kLogLtId, kLogPos, kCandTerm, kCandidateId, kNow);

        benchmark::DoNotOptimize(election->state());
    }

    state.SetItemsProcessed(state.iterations());

    std::error_code ec;
    fs::remove(nsfPath, ec);
    fs::remove(tlPath, ec);
}
BENCHMARK(BM_Election_OnRequestVote)->Unit(benchmark::kNanosecond);

} // namespace
