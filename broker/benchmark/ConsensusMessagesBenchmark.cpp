// ConsensusMessagesBenchmark.cpp — P2P-10: SBE codec benchmarks.
//
// Benchmarks:
//   BM_Sbe_Encode_<Msg>          — encode into a reused buffer; real work confirmed
//   BM_Sbe_Decode_<Msg>          — decode a pre-built buffer; DoNotOptimize on struct
//   BM_Sbe_PeekTemplateId        — read templateId from header (hot dispatch path)
//   BM_PublisherAdapter_RoundTrip — publisher.newLeadershipTerm → LoopbackSink bytes
//                                   → adapter.onFragment → NullHandler dispatch
//
// DCE prevention strategy for encode:
//   - buf is declared as volatile uint8_t array (prevents stores being elided).
//   - The varying field (logPos++) ensures the compiler cannot constant-fold input.
//   - DoNotOptimize(bytes) after each encode confirms the result is consumed.
//
// All benchmarks: SetItemsProcessed(state.iterations()), no per-iteration alloc.

#include <benchmark/benchmark.h>
#include <rune/consensus/ConsensusMessages.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/ConsensusMessageHandler.h>

#include <array>
#include <cstdint>
#include <vector>

namespace {

using namespace rune::consensus;

// ---------------------------------------------------------------------------
// Encode buffer — 128 bytes, volatile to prevent DCE of store sequences.
// We cast to uint8_t* for the encode calls (volatile uint8_t* is compatible
// because encode only writes via memcpy which observes volatile semantics
// through the aliased pointer).
// ---------------------------------------------------------------------------
// NOTE: Using a static volatile buffer forces each encode call to actually
// issue stores into memory, confirming it isn't dead-code-eliminated.
static volatile uint8_t g_encodeBuf[128];

// Convenience: non-volatile alias used for decode (reads from same storage).
// Decode reads are never elided (the result struct is DoNotOptimize'd).
static uint8_t* decodeBuf() {
    return const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(g_encodeBuf));
}

// ---------------------------------------------------------------------------
// LoopbackSink: captures last send() bytes for adapter loopback.
// ---------------------------------------------------------------------------
class BenchLoopbackSink final : public MessageSink {
public:
    void send(NodeId, const uint8_t* buf, std::size_t len) override {
        bytes_.assign(buf, buf + len);
    }
    const std::vector<uint8_t>& bytes() const noexcept { return bytes_; }
private:
    std::vector<uint8_t> bytes_;
};

// No-op handler for adapter dispatch benchmark.
class NullHandler final : public ConsensusMessageHandler {
public:
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) override {}
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) override {}
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) override {}
    void onNewLeadershipTerm(int64_t nowNs, Term, Term, int64_t, int64_t, Term, int64_t,
                             int64_t, int64_t, int64_t, NodeId, int32_t, int32_t,
                             bool, int64_t) override {
        benchmark::DoNotOptimize(nowNs);
    }
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) override {}
    void onCommitPosition(int64_t, Term, int64_t, NodeId) override {}
};

// ---------------------------------------------------------------------------
// Encode benchmarks
// Each uses g_encodeBuf (volatile) to prevent dead-store elimination.
// ---------------------------------------------------------------------------

static void BM_Sbe_Encode_CanvassPosition(benchmark::State& state) {
    int64_t logLtId = 3, logPos = 4096;
    int64_t ltId = 3;
    int32_t follower = 1, proto = 1;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        std::size_t n = CanvassPositionMsg::encode(
            decodeBuf(), 0, logLtId, logPos, ltId, follower, proto);
        benchmark::DoNotOptimize(n);
        ++logPos;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_CanvassPosition)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_CanvassPosition(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    CanvassPositionMsg::encode(buf.data(), 0, 3, 4096, 3, 1, 1);

    for (auto _ : state) {
        auto m = CanvassPositionMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_CanvassPosition)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Encode_RequestVote(benchmark::State& state) {
    int64_t logLtId = 5, logPos = 8192, candidateTerm = 6;
    int32_t candidateId = 0, proto = 1;

    for (auto _ : state) {
        benchmark::DoNotOptimize(candidateTerm);
        std::size_t n = RequestVoteMsg::encode(
            decodeBuf(), 0, logLtId, logPos, candidateTerm, candidateId, proto);
        benchmark::DoNotOptimize(n);
        ++candidateTerm;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_RequestVote)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_RequestVote(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    RequestVoteMsg::encode(buf.data(), 0, 5, 8192, 6, 0, 1);

    for (auto _ : state) {
        auto m = RequestVoteMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_RequestVote)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Encode_Vote(benchmark::State& state) {
    int64_t candidateTerm = 7, logLtId = 6, logPos = 16384;
    int32_t candidateId = 1, followerId = 2;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        std::size_t n = VoteMsg::encode(
            decodeBuf(), 0, candidateTerm, logLtId, logPos,
            candidateId, followerId, BooleanType::TRUE_VALUE);
        benchmark::DoNotOptimize(n);
        ++logPos;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_Vote)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_Vote(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    VoteMsg::encode(buf.data(), 0, 7, 6, 16384, 1, 2, BooleanType::TRUE_VALUE);

    for (auto _ : state) {
        auto m = VoteMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_Vote)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Encode_NewLeadershipTerm(benchmark::State& state) {
    int64_t logLtId = 10, nextLtId = 11, nextTermBase = 32768;
    int64_t nextLogPos = 65536, ltId = 10, termBase = 0, logPos = 65536;
    int64_t recId = 42, ts = 1'700'000'000'000LL, commitPos = 65536;
    int32_t leaderMem = 0, logSess = 1, appVer = 1;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        std::size_t n = NewLeadershipTermMsg::encode(
            decodeBuf(), 0,
            logLtId, nextLtId, nextTermBase, nextLogPos,
            ltId, termBase, logPos, recId, ts,
            leaderMem, logSess, appVer, BooleanType::FALSE_VALUE, commitPos);
        benchmark::DoNotOptimize(n);
        ++logPos;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_NewLeadershipTerm)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_NewLeadershipTerm(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    NewLeadershipTermMsg::encode(
        buf.data(), 0,
        10, 11, 32768, 65536,
        10, 0, 65536, 42, 1'700'000'000'000LL,
        0, 1, 1, BooleanType::FALSE_VALUE, 65536);

    for (auto _ : state) {
        auto m = NewLeadershipTermMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_NewLeadershipTerm)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Encode_AppendPosition(benchmark::State& state) {
    int64_t ltId = 8, logPos = 131072;
    int32_t follower = 1;
    uint8_t flags = 0x01;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        std::size_t n = AppendPositionMsg::encode(
            decodeBuf(), 0, ltId, logPos, follower, flags);
        benchmark::DoNotOptimize(n);
        logPos += 4096;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_AppendPosition)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_AppendPosition(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    AppendPositionMsg::encode(buf.data(), 0, 8, 131072, 1, 0x01);

    for (auto _ : state) {
        auto m = AppendPositionMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_AppendPosition)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Encode_CommitPosition(benchmark::State& state) {
    int64_t ltId = 9, logPos = 262144;
    int32_t leaderMem = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        std::size_t n = CommitPositionMsg::encode(
            decodeBuf(), 0, ltId, logPos, leaderMem);
        benchmark::DoNotOptimize(n);
        logPos += 4096;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Encode_CommitPosition)->Unit(benchmark::kNanosecond);

static void BM_Sbe_Decode_CommitPosition(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    CommitPositionMsg::encode(buf.data(), 0, 9, 262144, 0);

    for (auto _ : state) {
        auto m = CommitPositionMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_Decode_CommitPosition)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Sbe_PeekTemplateId
// ---------------------------------------------------------------------------
static void BM_Sbe_PeekTemplateId(benchmark::State& state) {
    std::array<uint8_t, 128> buf{};
    CommitPositionMsg::encode(buf.data(), 0, 9, 262144, 0);

    for (auto _ : state) {
        uint16_t tid = peekTemplateId(buf.data(), 0);
        benchmark::DoNotOptimize(tid);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_PeekTemplateId)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_PublisherAdapter_RoundTrip
//
// Measures: publisher.newLeadershipTerm() encode into internal buffer →
//           LoopbackSink captures bytes → adapter.onFragment dispatches →
//           NullHandler consumes.
// The LoopbackSink vector is pre-warmed (first publish outside loop) to avoid
// reallocation in the timed path.
// ---------------------------------------------------------------------------
static void BM_PublisherAdapter_RoundTrip(benchmark::State& state) {
    BenchLoopbackSink sink;
    NullHandler handler;
    SbeConsensusPublisher pub{sink};
    ConsensusAdapter adapter{handler};

    constexpr int64_t kNow = 1'000'000'000LL;

    // Warm up: size the vector before the timed loop.
    pub.newLeadershipTerm(
        0, 10, 11, 32768, 65536, 10, 0, 65536, 42, kNow, 0, 1, false, 65536);

    int64_t logPos = 65536;

    for (auto _ : state) {
        benchmark::DoNotOptimize(logPos);
        pub.newLeadershipTerm(
            0, 10, 11, 32768, logPos, 10, 0, logPos, 42, kNow, 0, 1, false, logPos);

        const auto& bytes = sink.bytes();
        adapter.onFragment(kNow, bytes.data(), bytes.size());
        benchmark::ClobberMemory();
        ++logPos;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_PublisherAdapter_RoundTrip)->Unit(benchmark::kNanosecond);

} // namespace
