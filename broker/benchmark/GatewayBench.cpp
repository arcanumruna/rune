// GatewayBench.cpp — BR-8: gateway ping round-trip latency (aeron:ipc loopback).
//
// Measures steady-state Ping → Pong latency through a live ClientGateway over
// aeron:ipc, reporting p50 / p99 / p99.9 (and mean) in microseconds.
//
// Setup (modelled on GatewayLoopbackTest.cpp):
//   - One embedded Aeron media driver per benchmark fixture (unique temp dir).
//   - One aeron::Aeron client shared by the gateway and harness.
//   - ClientGateway subscribed on the ingress channel (aeron:ipc).
//   - Harness client: Publication to ingress + Subscription on egress +
//     FragmentAssembler.
//   - One-time Connect handshake to obtain a sessionId, then steady-state Pings.
//
// Constraints:
//   - Bounded pump per iteration (hard cap + state.SkipWithError on timeout).
//   - NO busy-spin: short sleep_for(1µs) between pump ticks.
//   - Unique temp driver dir: getpid() + atomic counter.
//   - Warm-up phase: first kWarmupPings iterations discarded before sampling.
//   - Clean driver teardown in fixture TearDown().
//
// Benchmark:
//   BM_GatewayPingRoundtrip — manual timing via state.SetIterationTime().
//   Percentile counters (p50_us / p99_us / p999_us / mean_us) computed from
//   raw samples collected across all timed iterations.

#include <benchmark/benchmark.h>

#include <rune/gateway/ClientGateway.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t kIngressStream = 101;
static constexpr int32_t kEgressStream  = 200;
static constexpr int32_t kThisNodeId    = 7;
static constexpr int     kPumpCap       = 100'000; // hard cap per pump call
static constexpr int     kWarmupPings   = 50;      // discarded before sampling
static constexpr std::size_t kScratch   = 68 * 1024;

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDir() {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gw_bench_") +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// Monotonic nanoseconds
// ---------------------------------------------------------------------------
static int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ===========================================================================
// GatewayBenchFixture — Google Benchmark fixture class.
// SetUp creates the embedded driver + Aeron client + gateway + connect.
// TearDown tears everything down in safe order.
// All waits are bounded (hard cap + sleep — NO busy-spin).
// ===========================================================================
class GatewayBenchFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override {
        tmpPath_ = makeTempDir();
        startDriver(state);
        if (state.skipped()) return;
        connectAeron(state);
        if (state.skipped()) return;
        buildGateway(state);
        if (state.skipped()) return;
        makeClientEndpoints(state);
        if (state.skipped()) return;
        doConnect(state);
    }

    void TearDown(::benchmark::State& /*state*/) override {
        // Release publications and subscriptions BEFORE destroying the Aeron client.
        // Aeron pub/sub share_ptrs indirectly keep the ClientConductor alive;
        // resetting them here ensures the Aeron shared_ptr destructor joins the
        // conductor thread cleanly when we call aeron_.reset() below.
        ingressPub_.reset();
        egressSub_.reset();
        egressAsm_.reset();

        // Destroy gateway first (releases its ingress Subscription +
        // ExclusivePublications).
        gw_.reset();

        // Destroy Aeron C++ client. The Aeron destructor joins the conductor
        // thread. All publications/subscriptions must already be gone (above).
        aeron_.reset();

        // Stop the C media driver thread, drain, close.
        stopDriver();

        // Remove temp dir.
        std::error_code ec;
        fs::remove_all(tmpPath_, ec);
    }

    // -----------------------------------------------------------------------
    // measurePing — send one Ping and wait for the matching Pong.
    // Returns round-trip wall-clock in nanoseconds, or -1 on timeout/error.
    // -----------------------------------------------------------------------
    int64_t measurePing(int64_t correlationId) {
        // Encode Ping into the pre-allocated scratch buffer (zero-alloc steady-state).
        pc::Ping ping;
        ping.sessionId     = sessionId_;
        ping.correlationId = correlationId;
        ping.echo          = "bench";
        const std::size_t pingLen = ping.encode(pingBuf_.data());

        aeron::concurrent::AtomicBuffer ab(pingBuf_.data(),
            static_cast<aeron::util::index_t>(pingLen));

        // --- Offer (bounded retry, no busy-spin) ---
        bool sent = false;
        for (int i = 0; i < kPumpCap && !sent; ++i) {
            const int64_t pos = ingressPub_->offer(
                ab, 0, static_cast<aeron::util::index_t>(pingLen));
            if (pos > 0) {
                sent = true;
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{1});
            }
        }
        if (!sent) return -1;

        // --- Start timing (t0 after offer completes) ---
        const int64_t t0 = nowNs();

        // --- Pump until matching Pong arrives (bounded, no busy-spin) ---
        for (int i = 0; i < kPumpCap; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastCorrId_ == correlationId) {
                return nowNs() - t0;
            }
            std::this_thread::sleep_for(std::chrono::microseconds{1});
        }
        return -1; // timeout
    }

private:
    // -----------------------------------------------------------------------
    // Driver lifecycle
    // -----------------------------------------------------------------------
    void startDriver(benchmark::State& state) {
        const std::string aeronDir = (tmpPath_ / "aeron").string();
        fs::create_directories(tmpPath_);

        // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
        // readiness pumping — aeron_driver_init already wrote cnc.dat and stamped
        // the heartbeat synchronously before the driver thread even starts.
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir;
        cfg.termBufferBytes = 64 * 1024;
        try {
            driver_ = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
        } catch (const std::exception& e) {
            state.SkipWithError((std::string("EmbeddedDriver failed: ") + e.what()).c_str());
        }
    }

    void stopDriver() {
        driver_.reset();
    }

    // -----------------------------------------------------------------------
    // Aeron C++ client
    // -----------------------------------------------------------------------
    void connectAeron(benchmark::State& state) {
        const std::string aeronDir = (tmpPath_ / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            state.SkipWithError("aeron::Aeron::connect failed");
        }
    }

    // -----------------------------------------------------------------------
    // Build ClientGateway (subscribes on the ingress channel)
    // -----------------------------------------------------------------------
    void buildGateway(benchmark::State& state) {
        try {
            gw_ = std::make_unique<rune::gateway::ClientGateway>(
                aeron_, "aeron:ipc", kIngressStream, kThisNodeId);
        } catch (const std::exception& ex) {
            state.SkipWithError(ex.what());
        }
    }

    // -----------------------------------------------------------------------
    // Resolve ingress Publication + egress Subscription
    // -----------------------------------------------------------------------
    void makeClientEndpoints(benchmark::State& state) {
        const int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        const int64_t subRegId = aeron_->addSubscription("aeron:ipc", kEgressStream);

        // Bounded resolve (no busy-spin).
        bool pubOk = false, subOk = false;
        for (int i = 0; i < kPumpCap && !(pubOk && subOk); ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (!pubOk) {
                ingressPub_ = aeron_->findPublication(pubRegId);
                if (ingressPub_) pubOk = true;
            }
            if (!subOk) {
                egressSub_ = aeron_->findSubscription(subRegId);
                if (egressSub_) subOk = true;
            }
            if (!(pubOk && subOk))
                std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!pubOk || !subOk) {
            state.SkipWithError("Could not resolve pub/sub within timeout");
            return;
        }

        // Wait for ingress pub to connect (bounded).
        for (int i = 0; i < kPumpCap; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (ingressPub_->isConnected()) break;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!ingressPub_->isConnected()) {
            state.SkipWithError("Ingress publication did not connect within timeout");
            return;
        }

        // Set up egress FragmentAssembler — captures correlationId of received Pong.
        egressAsm_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t offset,
                   aeron::util::index_t len,
                   aeron::Header&) {
                auto dec = pc::Pong::decode(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                    static_cast<std::size_t>(len));
                if (dec) {
                    lastCorrId_ = dec->correlationId;
                }
            });
    }

    // -----------------------------------------------------------------------
    // One-time Connect handshake — assigns sessionId_
    // -----------------------------------------------------------------------
    void doConnect(benchmark::State& state) {
        uint8_t scratch[kScratch] = {};
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = 1;
        req.egressStreamId = kEgressStream;
        req.clientVersion  = 1;
        req.egressChannel  = "aeron:ipc";
        const std::size_t reqLen = req.encode(scratch);

        aeron::concurrent::AtomicBuffer ab(scratch,
            static_cast<aeron::util::index_t>(reqLen));

        // Offer ConnectRequest (bounded retry, no busy-spin).
        bool sent = false;
        for (int i = 0; i < kPumpCap && !sent; ++i) {
            const int64_t pos = ingressPub_->offer(
                ab, 0, static_cast<aeron::util::index_t>(reqLen));
            if (pos > 0) sent = true;
            else std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!sent) {
            state.SkipWithError("ConnectRequest offer timed out");
            return;
        }

        // Wait for ConnectResponse via egress subscription.
        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler connectAsm(
            [&](aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t offset,
                aeron::util::index_t len,
                aeron::Header&) {
                auto dec = pc::ConnectResponse::decode(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                    static_cast<std::size_t>(len));
                if (dec) resp = *dec;
            });

        for (int i = 0; i < kPumpCap && !resp.has_value(); ++i) {
            if (gw_) gw_->doWork(nowNs());
            egressSub_->poll(connectAsm.handler(), 16);
            if (!resp.has_value())
                std::this_thread::sleep_for(std::chrono::microseconds{100});
        }

        if (!resp.has_value()) {
            state.SkipWithError("ConnectResponse not received within timeout");
            return;
        }
        if (resp->code != static_cast<int16_t>(pc::ErrorCode::OK) ||
            resp->assignedSessionId < 1) {
            state.SkipWithError("ConnectResponse returned non-OK or invalid sessionId");
            return;
        }

        sessionId_ = resp->assignedSessionId;
    }

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    fs::path                                      tmpPath_;
    std::unique_ptr<rune::node::EmbeddedDriver>   driver_;

    std::shared_ptr<aeron::Aeron>                 aeron_;
    std::unique_ptr<rune::gateway::ClientGateway> gw_;
    std::shared_ptr<aeron::Publication>           ingressPub_;
    std::shared_ptr<aeron::Subscription>          egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>     egressAsm_;

    int64_t                                       sessionId_   = 0;
    int64_t                                       lastCorrId_  = -1;

    // Pre-allocated encode buffer (zero-alloc steady-state).
    std::array<uint8_t, kScratch>                 pingBuf_{};
};

// ===========================================================================
// BENCHMARK_DEFINE_F + BENCHMARK_REGISTER_F
//
// Measures steady-state Ping → Pong latency over aeron:ipc with a live
// ClientGateway.
//
// - First kWarmupPings: sent before the timed loop to prime the gateway and
//   the Aeron IPC buffers (driver-connect costs excluded from samples).
// - Timed iterations: SetIterationTime() records each round-trip; raw samples
//   are accumulated for p50/p99/p99.9 percentile counters emitted at the end.
// ===========================================================================
BENCHMARK_DEFINE_F(GatewayBenchFixture, PingRoundtrip)(benchmark::State& state) {
    if (state.skipped()) return;

    // --- Warm-up: fire kWarmupPings and discard ---
    for (int w = 0; w < kWarmupPings; ++w) {
        const int64_t ns = measurePing(static_cast<int64_t>(-(w + 1)));
        if (ns < 0) {
            state.SkipWithError("warm-up ping timed out");
            return;
        }
    }

    // --- Timed iterations ---
    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

    int64_t corrId = 1;
    for (auto _ : state) {
        state.PauseTiming();
        const int64_t corrIdLocal = corrId++;
        state.ResumeTiming();

        const int64_t ns = measurePing(corrIdLocal);

        state.PauseTiming();
        if (ns < 0) {
            state.SkipWithError("ping round-trip timed out during benchmark");
            return;
        }
        samples.push_back(static_cast<double>(ns) / 1'000.0);
        state.SetIterationTime(static_cast<double>(ns) / 1e9);
        state.ResumeTiming();
    }

    // --- Compute and report percentiles ---
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const std::size_t n = samples.size();

        const auto pct = [&](double p) -> double {
            if (n == 1) return samples[0];
            const double idx = p * static_cast<double>(n - 1);
            const std::size_t lo = static_cast<std::size_t>(idx);
            const std::size_t hi = lo + 1 < n ? lo + 1 : lo;
            const double frac = idx - static_cast<double>(lo);
            return samples[lo] + frac * (samples[hi] - samples[lo]);
        };

        double sum = 0.0;
        for (const double v : samples) sum += v;

        state.counters["p50_us"]  = pct(0.50);
        state.counters["p99_us"]  = pct(0.99);
        state.counters["p999_us"] = pct(0.999);
        state.counters["mean_us"] = sum / static_cast<double>(n);
    }
}

BENCHMARK_REGISTER_F(GatewayBenchFixture, PingRoundtrip)
    ->UseManualTime()
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);
