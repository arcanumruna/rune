// PeerFrameBench.cpp — authentication plan Task 16: the cost of AEAD on every inter-broker frame.
//
//   BM_PeerFrameSeal16K / Open16K / SealInPlace16K (+ 1K, 128K)  - the PeerFrameCrypto primitives (AES-256-GCM);
//   BM_PeerReplFetchRoundTrip_{Plain,Sealed}     - a FetchRequest -> FetchResponse round trip between two REAL
//       per-worker replication transports over aeron:ipc (one thread ping-pongs; p50 / p99 / max in ns as counters);
//   BM_PeerReplFetchResponseThroughput_{Plain,Sealed} - FetchResponse frames of N record bytes streamed A -> B
//       (bytes/s), one MTU frames (claim + seal in the term buffer) and fragmented ones (sealed whole in the scratch).
//
// Gates (rune::bench::gateCheck): every frame the sealed run sends opens (no drops) and the plain run's transport is
// byte-identical to today's (no crypto: peerFrameDrops() == 0).  No latency gate: the numbers inform the 3-node
// replication latency recorded in Task 19.
// RUNE_BENCH_PEER_BASELINE: compiled at the pre-Task-16 parity base (no rune/peerlink frame crypto exists there) - only the
// PeerPlain replication benchmarks are built, to prove the plain path did not regress (+-5 %).
#ifndef RUNE_BENCH_PEER_BASELINE
#include <rune/peerlink/PeerFrameCrypto.h>
#endif
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/WorkerReplicationTransport.h>

#include "BenchGate.h"
#include <benchmark/benchmark.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>
#include <Context.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

#ifndef RUNE_BENCH_PEER_BASELINE
namespace pl = rune::peerlink;
#endif
namespace rr = rune::replication;
namespace fs = std::filesystem;

#ifndef RUNE_BENCH_PEER_BASELINE
pl::LinkKeys linkFor(bool aToB) {
    pl::LinkKeys k; k.epoch = 1;
    k.sendMaster.fill(aToB ? 1 : 2); k.recvMaster.fill(aToB ? 2 : 1);
    return k;
}

struct CryptoPair {
    pl::PeerFrameCrypto a{1, 3}, b{2, 3};
    CryptoPair() { (void)a.installLink(2, linkFor(true), 0); (void)b.installLink(1, linkFor(false), 0); }
};

void BM_PeerFrameSeal(benchmark::State& state) {
    const std::size_t n = static_cast<std::size_t>(state.range(0));
    CryptoPair p;
    std::vector<uint8_t> plain(n, 0x5A), wire(n + pl::kPeerFrameOverhead);
    uint64_t bad = 0;
    for (auto _ : state) {
        const std::size_t w = p.a.seal(2, plain.data(), plain.size(), wire.data(), wire.size());
        if (w != n + pl::kPeerFrameOverhead) ++bad;
        benchmark::DoNotOptimize(wire.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    rune::bench::gateCheck(state, bad == 0, "a seal failed");
}
BENCHMARK(BM_PeerFrameSeal)->Name("BM_PeerFrameSeal1K")->Arg(1024);
BENCHMARK(BM_PeerFrameSeal)->Name("BM_PeerFrameSeal16K")->Arg(16 * 1024);
BENCHMARK(BM_PeerFrameSeal)->Name("BM_PeerFrameSeal128K")->Arg(128 * 1024);

void BM_PeerFrameSealInPlace(benchmark::State& state) {
    const std::size_t n = static_cast<std::size_t>(state.range(0));
    CryptoPair p;
    std::vector<uint8_t> frame(n + pl::kPeerFrameOverhead, 0x5A);
    uint64_t bad = 0;
    for (auto _ : state) {
        const std::size_t w = p.a.sealInPlace(2, frame.data(), n, frame.size());
        if (w != n + pl::kPeerFrameOverhead) ++bad;
        benchmark::DoNotOptimize(frame.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    rune::bench::gateCheck(state, bad == 0, "a sealInPlace failed");
}
BENCHMARK(BM_PeerFrameSealInPlace)->Name("BM_PeerFrameSealInPlace16K")->Arg(16 * 1024);

void BM_PeerFrameOpen(benchmark::State& state) {
    const std::size_t n = static_cast<std::size_t>(state.range(0));
    CryptoPair p;
    std::vector<uint8_t> plain(n, 0x5A), out(n + pl::kPeerFrameOverhead);
    // A window of pre-sealed frames, opened in order (each seq opens exactly once).
    constexpr int kFrames = 4096;
    std::vector<std::vector<uint8_t>> frames(kFrames, std::vector<uint8_t>(n + pl::kPeerFrameOverhead));
    for (auto& f : frames) (void)p.a.seal(2, plain.data(), plain.size(), f.data(), f.size());
    uint64_t bad = 0, i = 0;
    std::size_t outLen = 0; int32_t from = 0; uint32_t epoch = 0;
    for (auto _ : state) {
        if (i == kFrames) {   // a fresh pair re-opens the same frames (replay-safe): untimed
            state.PauseTiming();
            p.b = pl::PeerFrameCrypto(2, 3);
            (void)p.b.installLink(1, linkFor(false), 0);
            i = 0;
            state.ResumeTiming();
        }
        const auto& f = frames[i++];
        if (p.b.open(f.data(), f.size(), out.data(), out.size(), outLen, from, epoch) != pl::PeerFrameCrypto::OpenResult::Ok) ++bad;
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    rune::bench::gateCheck(state, bad == 0, "an open failed");
}
BENCHMARK(BM_PeerFrameOpen)->Name("BM_PeerFrameOpen1K")->Arg(1024);
BENCHMARK(BM_PeerFrameOpen)->Name("BM_PeerFrameOpen16K")->Arg(16 * 1024);
BENCHMARK(BM_PeerFrameOpen)->Name("BM_PeerFrameOpen128K")->Arg(128 * 1024);
#endif   // RUNE_BENCH_PEER_BASELINE

// --------------------------------------------------------------------------------------------------------------
// Real replication transports over aeron:ipc.
// --------------------------------------------------------------------------------------------------------------
class MediaDriver {
public:
    MediaDriver() {
        static std::atomic<int> seq{0};
        dir_ = (fs::temp_directory_path() / ("rune_pfb_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
        if (aeron_driver_context_init(&ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        aeron_driver_context_set_dir(ctx_, dir_.c_str());
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_DEDICATED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 1 << 20);        // the production default term (max message 128 KiB)
        aeron_driver_context_set_ipc_term_buffer_length(ctx_, 1 << 20);
        if (aeron_driver_init(&driver_, ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        if (aeron_driver_start(driver_, false) < 0) throw std::runtime_error(aeron_errmsg());
    }
    ~MediaDriver() { aeron_driver_close(driver_); aeron_driver_context_close(ctx_); }
    [[nodiscard]] const std::string& dir() const { return dir_; }
private:
    aeron_driver_context_t* ctx_ = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::string             dir_;
};

struct ReplRig {
    MediaDriver                   driver;
    std::shared_ptr<aeron::Aeron> aeron;
    std::unique_ptr<rr::WorkerReplicationTransport> a, b;   // a = node 1, b = node 2

    explicit ReplRig(bool sealed) {
        static std::atomic<int32_t> streams{800};
        const int32_t sa = streams.fetch_add(2), sb = sa + 1;
        aeron::Context ctx; ctx.aeronDir(driver.dir());
        aeron = aeron::Aeron::connect(ctx);
#ifndef RUNE_BENCH_PEER_BASELINE
        pl::PeerCryptoMode ca = pl::PeerPlain{}, cb = pl::PeerPlain{};
        if (sealed) { ca = pl::PeerFrameCrypto(1, 3); cb = pl::PeerFrameCrypto(2, 3); }
        a = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", sa,
            std::vector<std::pair<rr::NodeId, std::string>>{{2, "aeron:ipc"}}, sb, rune::aeronutil::AeronPump{}, std::move(ca));
        b = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", sb,
            std::vector<std::pair<rr::NodeId, std::string>>{{1, "aeron:ipc"}}, sa, rune::aeronutil::AeronPump{}, std::move(cb));
        if (sealed) { (void)a->installPeerLink(2, linkFor(true)); (void)b->installPeerLink(1, linkFor(false)); }
#else
        (void)sealed;
        a = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", sa,
            std::vector<std::pair<rr::NodeId, std::string>>{{2, "aeron:ipc"}}, sb);
        b = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", sb,
            std::vector<std::pair<rr::NodeId, std::string>>{{1, "aeron:ipc"}}, sa);
#endif
        for (int i = 0; i < 5000 && !(a->allPeersResolved() && b->allPeersResolved()); ++i) {
            (void)a->drivePeerResolve(); (void)b->drivePeerResolve(); std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));   // connect
    }
};

rr::FetchRequestMsg fetchRequest() {
    rr::FetchRequestMsg r{};
    r.leaderEpoch = 3; r.partition = 7; r.followerId = 1; r.fetchOffset = 42; r.maxBytes = 1 << 20; r.topicId = 5;
    return r;
}

void roundTrip(benchmark::State& state, bool sealed) {
    const std::size_t records = static_cast<std::size_t>(state.range(0));
    ReplRig rig(sealed);
    const std::vector<uint8_t> recs(records, 0x33);
    rr::FetchResponseMsg resp{};
    resp.leaderEpoch = 3; resp.partition = 7; resp.highWatermark = 99; resp.topicId = 5;
    resp.records = std::span<const uint8_t>(recs);
    const auto req = fetchRequest();
    std::vector<int64_t> lat;
    lat.reserve(1 << 20);
    bool gotReq = false, gotResp = false;
    auto onB = [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) { if (tid == rr::FetchRequestMsg::TEMPLATE_ID) gotReq = true; };
    auto onA = [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) { if (tid == rr::FetchResponseMsg::TEMPLATE_ID) gotResp = true; };
    uint64_t timeouts = 0;
    auto once = [&]() -> int64_t {
        const auto t0 = std::chrono::steady_clock::now();
        gotReq = gotResp = false;
        while (!rig.a->sendFetchRequest(req)) {}
        while (!gotReq) (void)rig.b->poll(4, onB);
        while (!rig.b->sendFetchResponse(1, resp)) {}
        while (!gotResp) (void)rig.a->poll(4, onA);
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    };
    for (int i = 0; i < 2000; ++i) (void)once();   // warm-up
    for (auto _ : state) {
        const int64_t ns = once();
        if (lat.size() < lat.capacity()) lat.push_back(ns);
    }
    std::sort(lat.begin(), lat.end());
    if (!lat.empty()) {
        state.counters["p50_ns"] = static_cast<double>(lat[lat.size() / 2]);
        state.counters["p99_ns"] = static_cast<double>(lat[lat.size() * 99 / 100]);
        state.counters["p999_ns"] = static_cast<double>(lat[lat.size() * 999 / 1000]);
        state.counters["max_ns"] = static_cast<double>(lat.back());
    }
    (void)timeouts;
#ifndef RUNE_BENCH_PEER_BASELINE
    rune::bench::gateCheck(state, rig.a->peerFrameDrops() + rig.b->peerFrameDrops() == 0 && rig.a->peerSealed() == sealed,
                           "frames were dropped (or the transport is not in the expected crypto mode)");
#else
    rune::bench::gatePass(state);
#endif
}
void BM_PeerReplFetchRoundTrip_Plain(benchmark::State& s)  { roundTrip(s, false); }
BENCHMARK(BM_PeerReplFetchRoundTrip_Plain)->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024)->Unit(benchmark::kMicrosecond)->MinTime(3);
#ifndef RUNE_BENCH_PEER_BASELINE
void BM_PeerReplFetchRoundTrip_Sealed(benchmark::State& s) { roundTrip(s, true); }
BENCHMARK(BM_PeerReplFetchRoundTrip_Sealed)->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024)->Unit(benchmark::kMicrosecond)->MinTime(3);
#endif

void throughput(benchmark::State& state, bool sealed) {
    const std::size_t records = static_cast<std::size_t>(state.range(0));
    ReplRig rig(sealed);
    const std::vector<uint8_t> recs(records, 0x33);
    rr::FetchResponseMsg resp{};
    resp.leaderEpoch = 3; resp.partition = 7; resp.highWatermark = 99; resp.topicId = 5;
    resp.records = std::span<const uint8_t>(recs);
    uint64_t sent = 0, got = 0;
    auto onA = [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) { if (tid == rr::FetchResponseMsg::TEMPLATE_ID) ++got; };
    for (int i = 0; i < 1000; ++i) { while (!rig.b->sendFetchResponse(1, resp)) (void)rig.a->poll(16, onA); ++sent; }
    while (got < sent) (void)rig.a->poll(16, onA);
    sent = got = 0;
    for (auto _ : state) {
        while (!rig.b->sendFetchResponse(1, resp)) (void)rig.a->poll(16, onA);   // back-pressure: drain the receiver
        ++sent;
        (void)rig.a->poll(16, onA);
    }
    while (got < sent) (void)rig.a->poll(16, onA);
    state.SetBytesProcessed(static_cast<int64_t>(sent) * static_cast<int64_t>(records));
    state.counters["frames_per_s"] = benchmark::Counter(static_cast<double>(sent), benchmark::Counter::kIsRate);
#ifndef RUNE_BENCH_PEER_BASELINE
    rune::bench::gateCheck(state, got == sent && rig.a->peerFrameDrops() + rig.b->peerFrameDrops() == 0,
                           "a sent frame was lost or dropped");
#else
    rune::bench::gateCheck(state, got == sent, "a sent frame was lost");
#endif
}
void BM_PeerReplFetchResponseThroughput_Plain(benchmark::State& s)  { throughput(s, false); }
BENCHMARK(BM_PeerReplFetchResponseThroughput_Plain)->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024)->Unit(benchmark::kMicrosecond)->MinTime(3)->UseRealTime();
#ifndef RUNE_BENCH_PEER_BASELINE
void BM_PeerReplFetchResponseThroughput_Sealed(benchmark::State& s) { throughput(s, true); }
BENCHMARK(BM_PeerReplFetchResponseThroughput_Sealed)->Arg(1024)->Arg(16 * 1024)->Arg(64 * 1024)->Unit(benchmark::kMicrosecond)->MinTime(3)->UseRealTime();
#endif

}  // namespace
