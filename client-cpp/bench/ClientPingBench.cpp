// bench/ClientPingBench.cpp
//
// CC-8 — Client ping round-trip latency benchmark (REVISED).
//
// Measures steady-state Client::ping() RTT over aeron:ipc loopback against an
// in-test gateway echoer run in LowLatency mode.
//
// Root-cause of old p99 466µs:
//   (a) In-test gateway thread slept 200µs between polls — charged to client RTT.
//   (b) Client runner used BackoffIdleStrategy, parking up to 1 ms on idle cycles.
//
// Fixes applied:
//   (a) Gateway pump thread now tight-polls (YieldingIdleStrategy / no sleep).
//   (b) Client constructed with IdleMode::LowLatency — YieldingIdleStrategy,
//       no park — runner reacts in ~µs.
//
// Benchmark: BM_PingRoundTrip
//   - One-time setup: embedded driver + connected Client + gateway echoer.
//   - Gateway runs on a dedicated helper thread, tight-polling (yielding loop).
//   - Client runs in IdleMode::LowLatency (YieldingIdleStrategy, no park).
//   - Per iteration: c.ping(payload).get() — wall-clock RTT measured.
//   - Warm-up: explicit pre-benchmark warm-up of kWarmupPings + Google
//     Benchmark's built-in warm-up iterations (both discard early data).
//   - Percentiles reported: p50/p99/p99.9/mean (µs) via state.counters.
//   - Bounded: per-iteration hard cap (kPingTimeoutMs); SkipWithError on timeout.
//   - Unique temp driver dir (pid + atomic counter).
//   - Clean teardown: gateway thread joined, Client::close() called.
//
// What this measures (the TRUE client hot path):
//   app-thread → ring → runner-thread (yield loop) → transport tryClaim →
//   aeron:ipc (IPC ring buffer) → gateway (yield loop) → aeron:ipc →
//   runner-thread → egress observer → promise.set_value → future.get()
//
// Expected: p99 well under 100µs (typically 10–40µs on a quiet macOS/Linux host).
// The broker's raw gateway RTT is ~1.9µs; the client adds ring-hop + yield cycles
// + observer overhead + future wake-up.  A p99 of 30–60µs is typical and correct.

#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/YieldingIdleStrategy.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Hard cap on each ping() call inside the benchmark.
static constexpr int64_t kPingTimeoutMs    = 5'000;
/// Timeout used for the initial connect().
static constexpr int64_t kConnectTimeoutMs = 10'000;
/// Number of warm-up pings before any sampling.
static constexpr int     kWarmupPings      = 64;
/// Gateway send retry limit (no sleep — tight retry on IPC back-pressure).
static constexpr int     kGwSendRetries    = 500;

// ---------------------------------------------------------------------------
// Unique temp directory
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCtr{0};

std::string uniqueDir() {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cc8_bench_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // namespace

// ---------------------------------------------------------------------------
// In-test gateway — tight-polling pump thread (NO sleep between polls).
//
// The gateway is the "server" peer in this benchmark.  Its poll latency must
// NOT be charged to the measured client RTT.  A 200µs sleep between polls
// would add up to 200µs to every single RTT measurement.  We use a
// YieldingIdleStrategy instead: idle() calls std::this_thread::yield() so the
// OS can schedule other threads but we re-poll in microseconds, not hundreds
// of microseconds.
// ---------------------------------------------------------------------------

struct BenchGateway {
    std::string aeronDir;

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    int32_t ingressStream = 0;
    int32_t egressStream  = 0;

    uint8_t replyBuf[512]{};

    std::thread       pumpThread_;
    std::atomic<bool> stopPump_{false};

    explicit BenchGateway(std::string dir) : aeronDir(std::move(dir)) {}

    ~BenchGateway() { stop(); }

    void start(int32_t inStream, int32_t egStream) {
        ingressStream = inStream;
        egressStream  = egStream;

        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) throw std::runtime_error("BenchGateway: Aeron::connect failed");

        int64_t subId = aeron_->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = aeron_->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub_) sub_ = aeron_->findSubscription(subId);
            if (!pub_) pub_ = aeron_->findExclusivePublication(pubId);
            if (sub_ && pub_) break;
            std::this_thread::sleep_for(2ms);
        }
        if (!sub_ || !pub_)
            throw std::runtime_error("BenchGateway: timed out resolving sub/pub");

        assembler_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t            offset,
                   aeron::util::index_t            len,
                   aeron::Header&)
            {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                        static_cast<std::size_t>(len));
            });

        // Tight-poll pump thread: yields between polls, no sleep.
        // This keeps gateway reaction time in the single-digit-µs range so its
        // contribution to the measured client RTT is negligible.
        stopPump_.store(false, std::memory_order_relaxed);
        pumpThread_ = std::thread([this] {
            aeron::concurrent::YieldingIdleStrategy idle;
            while (!stopPump_.load(std::memory_order_relaxed)) {
                int work = 0;
                if (sub_ && assembler_) {
                    work = sub_->poll(assembler_->handler(), 16);
                }
                idle.idle(work);
            }
        });
    }

    void stop() {
        stopPump_.store(true, std::memory_order_relaxed);
        if (pumpThread_.joinable()) pumpThread_.join();
        assembler_.reset();
        pub_.reset();
        sub_.reset();
        aeron_.reset();
    }

    bool send(const uint8_t* buf, std::size_t len) {
        if (!pub_) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
        // Tight retry — no sleep between attempts; IPC back-pressure on send
        // is extremely rare in a two-party benchmark but we guard against it.
        for (int i = 0; i < kGwSendRetries; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::yield();
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        switch (static_cast<rcc::TemplateId>(*tidOpt)) {
        case rcc::TemplateId::Connect: {
            auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            break;
        }

        case rcc::TemplateId::Ping: {
            auto opt = rcc::decodePing(buf, len);
            if (!opt) return;
            std::size_t n = rcc::encodePong(
                replyBuf, sizeof(replyBuf),
                opt->echo,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            break;
        }

        default:
            break;
        }
    }
};

// ---------------------------------------------------------------------------
// BenchFixture — sets up the Client (LowLatency) + gateway once for the
// whole benchmark.
// ---------------------------------------------------------------------------

struct BenchFixture {
    static constexpr int32_t kIngressStream = 700;
    static constexpr int32_t kEgressStream  = 701;

    std::string                   agentDir;
    std::string                   aeronDir;
    std::unique_ptr<Client>       client;
    std::unique_ptr<BenchGateway> gw;

    BenchFixture() {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";

        ClientConfig cfg;
        cfg.gatewayChannel   = "aeron:ipc";
        cfg.gatewayStreamId  = kIngressStream;
        cfg.egressChannel    = "aeron:ipc";
        cfg.egressStreamId   = kEgressStream;
        cfg.dataDir          = agentDir;
        cfg.connectTimeoutMs = kConnectTimeoutMs;
        cfg.pingTimeoutMs    = kPingTimeoutMs;
        // LowLatency: runner uses YieldingIdleStrategy → reacts in µs.
        cfg.idleMode         = IdleMode::LowLatency;

        client = std::make_unique<Client>(cfg);

        // Give the embedded driver a moment to initialise.
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<BenchGateway>(aeronDir);
        gw->start(kIngressStream, kEgressStream);

        // Connect.
        auto fut = client->connect();
        const auto connDl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(kConnectTimeoutMs);
        if (fut.wait_until(connDl) != std::future_status::ready)
            throw std::runtime_error("BenchFixture: connect() timed out");
        fut.get(); // throws on error

        // Explicit warm-up: discard first kWarmupPings before sampling.
        // This lets the IPC channels, Aeron internal caches, and CPU
        // branch-predictor reach steady state.
        Bytes wPayload = {'w', 'a', 'r', 'm'};
        for (int i = 0; i < kWarmupPings; ++i) {
            auto wf = client->ping(wPayload);
            const auto wdl = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(kPingTimeoutMs);
            if (wf.wait_until(wdl) != std::future_status::ready)
                throw std::runtime_error("BenchFixture: warm-up ping timed out");
            wf.get();
        }
    }

    ~BenchFixture() {
        if (client) {
            client->close();
            client.reset();
        }
        if (gw) {
            gw->stop();
            gw.reset();
        }
        // Best-effort cleanup of temp dir.
        try {
            std::filesystem::remove_all(agentDir);
        } catch (...) {}
    }
};

// ---------------------------------------------------------------------------
// Benchmark state — keeps the fixture alive for the duration of the process.
// ---------------------------------------------------------------------------

static BenchFixture* g_fixture = nullptr;

// ---------------------------------------------------------------------------
// BM_PingRoundTrip — main benchmark.
//
// Measures wall-clock of client.ping(payload).get() per iteration.
// Accumulates per-iteration samples, then reports p50/p99/p99.9/mean (µs).
//
// Configuration:
//   Client: IdleMode::LowLatency (YieldingIdleStrategy, no park).
//   Gateway: tight-poll (YieldingIdleStrategy, no sleep).
//   Expected: p99 < 100µs on a quiet host (typically 10–60µs).
// ---------------------------------------------------------------------------

static void BM_PingRoundTrip(benchmark::State& state) {
    // Payload for steady-state pings.
    static const Bytes kPayload = {'p', 'i', 'n', 'g', 0x00, 0xFF};

    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

    for (auto _ : state) {
        state.PauseTiming();
        auto& c = *g_fixture->client;
        state.ResumeTiming();

        const auto t0 = std::chrono::steady_clock::now();
        auto fut = c.ping(kPayload);

        const auto deadline = t0 + std::chrono::milliseconds(kPingTimeoutMs);
        if (fut.wait_until(deadline) != std::future_status::ready) {
            state.SkipWithError("ping() future did not resolve within timeout");
            break;
        }

        try {
            (void)fut.get();
        } catch (const std::exception& ex) {
            state.SkipWithError(ex.what());
            break;
        }

        const auto t1 = std::chrono::steady_clock::now();
        const double us = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / 1000.0;
        samples.push_back(us);
        state.SetIterationTime(us * 1e-6); // seconds for Google Benchmark's timing
    }

    if (samples.empty()) return;

    // Compute percentiles.
    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const std::size_t n = sorted.size();

    auto pct = [&](double p) -> double {
        if (n == 1) return sorted[0];
        const double idx = p * static_cast<double>(n - 1);
        const std::size_t lo = static_cast<std::size_t>(idx);
        const std::size_t hi = std::min(lo + 1, n - 1);
        const double frac = idx - static_cast<double>(lo);
        return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
    };

    const double mean = std::accumulate(sorted.begin(), sorted.end(), 0.0) /
                        static_cast<double>(n);

    state.counters["p50_us"]   = pct(0.50);
    state.counters["p99_us"]   = pct(0.99);
    state.counters["p999_us"]  = pct(0.999);
    state.counters["mean_us"]  = mean;
    state.counters["samples"]  = static_cast<double>(n);
}

BENCHMARK(BM_PingRoundTrip)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond)
    ->MinTime(3.0);   // run for at least 3 seconds to collect enough samples

// ---------------------------------------------------------------------------
// main — initialise the shared fixture before running benchmarks.
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

    // Create the fixture (starts driver, gateway [tight-poll], client [LowLatency],
    // connect, warm-up kWarmupPings).
    BenchFixture fixture;
    g_fixture = &fixture;

    ::benchmark::RunSpecifiedBenchmarks();

    g_fixture = nullptr;
    return 0;
}
