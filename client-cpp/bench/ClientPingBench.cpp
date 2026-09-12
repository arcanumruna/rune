// bench/ClientPingBench.cpp
//
// CC-8 — Client ping round-trip latency benchmark.
//
// Measures steady-state Client::ping() RTT over aeron:ipc loopback against an
// in-test gateway echoer (same pattern as CC-5/CC-6 tests).
//
// Benchmark: BM_PingRoundTrip
//   - One-time setup: embedded driver + connected Client + gateway echoer.
//   - Gateway runs on a dedicated helper thread pumping its subscription.
//   - Per iteration: c.ping(payload).get() — measured wall-clock RTT.
//   - Warm-up: Google Benchmark's built-in warm-up is used (first N iterations
//     are not sampled by default), supplemented by an explicit pre-benchmark
//     warm-up loop of kWarmupPings iterations.
//   - Percentiles reported via state.counters["p50_us"/"p99_us"/"p999_us"/"mean_us"].
//   - Bounded: per-iteration hard cap (kPingTimeoutMs); SkipWithError on timeout.
//   - NO busy-spin: gateway helper thread sleeps between polls.
//   - Unique temp driver dir (pid + atomic counter).
//   - Clean teardown: gateway thread joined, Client::close() called.
//
// Note: this measures the FULL client path:
//   app-thread → ring → runner-thread → transport → aeron:ipc →
//   gateway-thread → aeron:ipc → runner-thread → observer → promise → future.get()
// Including the runner-thread park + wake-up overhead.  Absolute numbers will
// therefore be higher than broker's raw gateway RTT (BR-8 ~1–2 µs) — expected.

#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

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
static constexpr int64_t kPingTimeoutMs  = 5'000;
/// Timeout used for the initial connect().
static constexpr int64_t kConnectTimeoutMs = 10'000;
/// Number of warm-up pings before any sampling.
static constexpr int     kWarmupPings    = 32;
/// Sleep inside the gateway poll thread between poll iterations.
static constexpr auto    kGwPollSleep    = std::chrono::microseconds{200};
/// Gateway send retry limit.
static constexpr int     kGwSendRetries  = 500;

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
// In-test gateway — mirrors the CC-6 TestGateway, adapted for the benchmark.
//
// Runs a dedicated pump thread so the measured CLIENT ping() call doesn't need
// to drive the gateway (keeps the measurement of the CLIENT RTT clean).
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

        // Start the pump thread.
        stopPump_.store(false, std::memory_order_relaxed);
        pumpThread_ = std::thread([this] {
            while (!stopPump_.load(std::memory_order_relaxed)) {
                if (sub_ && assembler_) {
                    sub_->poll(assembler_->handler(), 16);
                }
                std::this_thread::sleep_for(kGwPollSleep);
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
        for (int i = 0; i < kGwSendRetries; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
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
// BenchFixture — sets up the Client + gateway once for the whole benchmark.
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
// Constructed once; destructed at process exit (after benchmark::RunSpecifiedBenchmarks).
// ---------------------------------------------------------------------------

static BenchFixture* g_fixture = nullptr;

// ---------------------------------------------------------------------------
// BM_PingRoundTrip — main benchmark.
//
// Measures wall-clock of client.ping(payload).get() per iteration.
// Accumulates per-iteration samples, then reports p50/p99/p99.9/mean.
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

    // Create the fixture (starts driver, gateway, client, connect, warm-up).
    BenchFixture fixture;
    g_fixture = &fixture;

    ::benchmark::RunSpecifiedBenchmarks();

    g_fixture = nullptr;
    return 0;
}
