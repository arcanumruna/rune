// ConsensusAgentBenchmark.cpp — P2P-10: ConsensusAgent doWork drain benchmark.
//
// Benchmark:
//   BM_ConsensusAgent_DoWork_DrainN
//     - Parameterised by N (1, 5, 10) = number of encoded SBE frames pre-loaded
//       into the IngressRing UNDER PauseTiming each iteration.
//     - Timed section: one doWork(nowNs) that drains + dispatches up to N frames
//       and then calls election_.doWork() (which for a CLOSED election returns 0).
//     - SetItemsProcessed(N * iterations).
//
// Setup:
//   - Single-member cluster: Election runs to CLOSED before the benchmark loop
//     starts (driven outside PauseTiming once). Once CLOSED, election_.doWork()
//     is near-zero cost so the measured work is dominated by ring drain + dispatch.
//   - Messages are pre-encoded CanvassPosition frames (smallest fixed-size SBE
//     frame: 40 bytes) so the ring holds many of them and IngressRing capacity
//     is ample.
//   - IngressRing capacity = 64 KiB (default), more than enough for 10 × 40B frames.
//   - NullPublisher absorbs all outbound messages from Election.
//   - NullHandler is NOT needed here: ConsensusAgent implements ConsensusMessageHandler
//     itself and forwards to the Election (which is CLOSED and ignores the calls).

#include <benchmark/benchmark.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/ConsensusMessages.h>
#include <rune/consensus/Types.h>

#include <filesystem>
#include <string>
#include <vector>
#include <array>
#include <cstdint>
#include <climits>
#include <memory>

namespace {

namespace fs = std::filesystem;
using namespace rune::consensus;

// ---------------------------------------------------------------------------
// NullPublisher
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

static std::vector<ClusterMember> makeMembers1() {
    ClusterMember m;
    m.id = 0; m.logPosition = 0;
    m.logLeadershipTermId = 0; m.leadershipTermId = 0;
    return {m};
}

static int s_agentBenchCounter = 0;

static std::string makeTempPath(const char* tag, const char* ext) {
    return (fs::temp_directory_path() /
            (std::string(tag) + "_" +
             std::to_string(++s_agentBenchCounter) + ext)).string();
}

// Pre-encode a CanvassPosition frame into a fixed buffer.
// Returns the number of bytes written.
static constexpr std::size_t kCanvassFrameSize = CanvassPositionMsg::TOTAL_SIZE; // 40 bytes

static void encodeCanvassFrame(uint8_t* buf) {
    CanvassPositionMsg::encode(buf, 0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*leadershipTermId=*/0,
        /*followerMemberId=*/1,
        /*protocolVersion=*/1);
}

// ---------------------------------------------------------------------------
// BM_ConsensusAgent_DoWork_DrainN
//
// Parameterised by N (number of frames to pre-load per doWork call).
// ---------------------------------------------------------------------------
static void BM_ConsensusAgent_DoWork_DrainN(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));

    // --- Build persistent objects OUTSIDE the timed loop ---
    std::string nsfPath = makeTempPath("rune_bench_cagt_nsf", ".nsf");
    std::string tlPath  = makeTempPath("rune_bench_cagt_tl",  ".tl");

    NullPublisher pub;
    NodeStateFile nsf{nsfPath, /*fileSyncLevel=*/0};
    TermLog       tl{tlPath};
    IngressRing   ring; // 64 KiB default

    Election election{
        /*isNodeStartup=*/true,
        /*gracefulClosedLeaderId=*/-1,
        /*leadershipTermId=*/0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*appendPosition=*/0,
        makeMembers1(),
        /*thisMemberId=*/0,
        pub, nsf, tl,
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        /*rngSeed=*/42};

    ConsensusAgent agent{election, ring};

    // Drive election to CLOSED (single-member, few steps).
    {
        constexpr int kMaxSteps = 200;
        int64_t nowNs = 1'000'000LL;
        for (int i = 0; i < kMaxSteps && !election.isClosed(); ++i) {
            int64_t dl = election.nextDeadlineNs();
            if (dl > nowNs) nowNs = dl;
            agent.doWork(nowNs);
            nowNs += 1;
        }
    }

    // Pre-encode one CanvassPosition frame.
    std::array<uint8_t, kCanvassFrameSize> frameBuf{};
    encodeCanvassFrame(frameBuf.data());

    for (auto _ : state) {
        // Under PauseTiming: pre-load N frames into the ring.
        state.PauseTiming();
        for (int i = 0; i < N; ++i) {
            ring.write(frameBuf.data(), kCanvassFrameSize);
        }
        state.ResumeTiming();

        // Timed: one doWork that drains up to FRAGMENT_LIMIT frames.
        int64_t nowNs = 2'000'000'000LL;
        int w = agent.doWork(nowNs);
        benchmark::DoNotOptimize(w);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(N) * state.iterations());
    state.counters["frames"] = benchmark::Counter(static_cast<double>(N));

    // Cleanup.
    std::error_code ec;
    fs::remove(nsfPath, ec);
    fs::remove(tlPath, ec);
}
BENCHMARK(BM_ConsensusAgent_DoWork_DrainN)
    ->Arg(1)->Arg(5)->Arg(10)
    ->Unit(benchmark::kNanosecond);

} // namespace
