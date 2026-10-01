// bench/ClientProduceBench.cpp — CB-5 D
//
// Measures two latency dimensions in aeron:ipc loopback against an in-test
// gateway pump thread running in tight-yielding mode:
//
//   BM_ProduceRTT
//     Produces a single 64-byte record (Tier-1 auto-batch, Acks::Leader) and
//     waits for the ProduceResponse.  Measures wall-clock of
//     producer.send(…,want_future).get() per iteration.
//     Gate: p99 < 100µs.
//
//   BM_SubscribeDeliveryLatency
//     Produces one record, then measures the wall-clock from the final byte of
//     the produce future resolution (gateway has sent ProduceResponse) until the
//     SubscribeResponse + first delivered record hits the RecordHandler.
//     This is the end-to-end "write-to-read" delivery latency.
//     Gate: p99 < 100µs.
//
// Design:
//   - A single BenchFixture is created once: embedded driver, Client
//     (LowLatency), and ProducerBenchGateway (tight-poll thread).
//   - ProducerBenchGateway handles ConnectRequest, Produce, and Subscribe:
//       ConnectRequest → ConnectResponse{OK, sessionId=42}
//       Produce        → ProduceResponse{OK, baseOffset}
//       Subscribe      → SubscribeResponse{subscriptionId=1, startOffset}
//                        then pushes a RecordBatch back for each subscribe
//   - Produce RTT: the full app→ring→agent→transport→gateway→transport→agent→
//                  promise path.
//   - Subscribe delivery: gap from produce-ack to first RecordHandler call.

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/RecordBatch.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/YieldingIdleStrategy.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
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

static constexpr int64_t kOpTimeoutMs    = 5'000;
static constexpr int64_t kConnTimeoutMs  = 10'000;
static constexpr int     kWarmupIter     = 64;
static constexpr int     kGwRetries      = 1'000;
static constexpr int32_t kTopicId        = 1;
static constexpr int32_t kPartition      = 0;
static constexpr int32_t kSubscriptionId = 1;

// ---------------------------------------------------------------------------
// Unique temp directory
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCtr{0};

std::string uniqueDir() {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cb5d_bench_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // namespace

// ---------------------------------------------------------------------------
// ProducerBenchGateway — handles Connect, Produce, Subscribe in a tight
// pump thread.  For each Produce it sends back ProduceResponse{OK}.
// For each Subscribe it sends SubscribeResponse then one RecordBatch push.
// ---------------------------------------------------------------------------

struct ProducerBenchGateway {
    std::string aeronDir;

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    int32_t ingressStream = 0;
    int32_t egressStream  = 0;

    // Counters / state.
    std::atomic<int64_t> nextOffset_{0};
    std::atomic<int64_t> producesHandled_{0};
    std::atomic<int64_t> subscribesHandled_{0};

    // For subscribe-delivery bench: timestamp when ProduceResponse was sent.
    std::atomic<int64_t> produceAckNs_{0};

    // Session id captured from the last Subscribe request.  Used to push
    // RecordBatch frames to the active subscription on each Produce.
    std::atomic<int64_t> lastSessionId_{0};

    uint8_t replyBuf[2048]{};

    std::thread       pumpThread_;
    std::atomic<bool> stopPump_{false};

    explicit ProducerBenchGateway(std::string dir) : aeronDir(std::move(dir)) {}
    ~ProducerBenchGateway() { stop(); }

    void start(int32_t inStream, int32_t egStream) {
        ingressStream = inStream;
        egressStream  = egStream;

        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_)
            throw std::runtime_error("ProducerBenchGateway: connect failed");

        const int64_t subId =
            aeron_->addSubscription("aeron:ipc", ingressStream);
        const int64_t pubId =
            aeron_->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub_) sub_ = aeron_->findSubscription(subId);
            if (!pub_) pub_ = aeron_->findExclusivePublication(pubId);
            if (sub_ && pub_) break;
            std::this_thread::sleep_for(2ms);
        }
        if (!sub_ || !pub_)
            throw std::runtime_error("ProducerBenchGateway: sub/pub timed out");

        assembler_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                    static_cast<std::size_t>(len));
            });

        stopPump_.store(false, std::memory_order_relaxed);
        pumpThread_ = std::thread([this] {
            aeron::concurrent::YieldingIdleStrategy idle;
            while (!stopPump_.load(std::memory_order_relaxed)) {
                const int work = sub_ ? sub_->poll(assembler_->handler(), 16) : 0;
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
        for (int i = 0; i < kGwRetries; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::yield();
        }
        return false;
    }

    // Push a synthetic RecordBatch to the subscriber as a Records frame.
    bool sendRecordBatch(int64_t sessionId, int64_t subscriptionId,
                          int64_t baseOffset) {
        // Build a minimal RecordBatch with one record.
        rune::protocol::RecordBatchBuilder rb(baseOffset, /*baseTimestamp=*/0);
        rb.append(/*key=*/std::string_view{"k"}, /*value=*/std::string_view{"v"},
                  /*timestamp=*/0);
        const auto batch = rb.build();

        // Encode as a Records frame (the egress delivery message).
        const std::size_t n = rcc::encodeRecords(
            replyBuf, sizeof(replyBuf),
            subscriptionId,
            kTopicId,
            kPartition,
            baseOffset,
            std::span<const uint8_t>(batch.data(), batch.size()),
            sessionId,
            /*correlationId=*/0);
        if (n == 0) return false;
        return send(replyBuf, n);
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        switch (static_cast<rcc::TemplateId>(*tidOpt)) {
        case rcc::TemplateId::Connect: {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            break;
        }

        case rcc::TemplateId::Produce: {
            const auto opt = rcc::decodeProduce(buf, len);
            if (!opt) return;

            const int64_t base = nextOffset_.fetch_add(1, std::memory_order_relaxed);

            const std::size_t n = rcc::encodeProduceResponse(
                replyBuf, sizeof(replyBuf),
                /*errorCode=*/0,
                opt->partition,
                base,
                opt->sessionId,
                opt->correlationId);

            // Stamp ack time before sending (so delivery latency = now − ackNs).
            // This is the gateway-side instant the ProduceResponse goes on the wire.
            produceAckNs_.store(
                static_cast<int64_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count()),
                std::memory_order_release);

            if (n > 0) send(replyBuf, n);

            // If a subscription is active, push a RecordBatch for this produce
            // so BM_SubscribeDeliveryLatency can measure produce-ack → handler
            // delivery latency.  This models the broker pushing the newly produced
            // record to all active subscribers immediately after acking the produce.
            if (subscribesHandled_.load(std::memory_order_relaxed) > 0) {
                const int64_t sid = lastSessionId_.load(std::memory_order_acquire);
                if (sid != 0) {
                    sendRecordBatch(sid, kSubscriptionId, base);
                }
            }

            producesHandled_.fetch_add(1, std::memory_order_relaxed);
            break;
        }

        case rcc::TemplateId::Subscribe: {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;

            // Remember the session id so the Produce handler can push
            // RecordBatch frames back to this active subscription.
            lastSessionId_.store(opt->sessionId, std::memory_order_release);

            // Send SubscribeResponse{OK, partition, subscriptionId}.
            const std::size_t n = rcc::encodeSubscribeResponse(
                replyBuf, sizeof(replyBuf),
                /*errorCode=*/0,
                opt->partition,
                kSubscriptionId,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);

            // Immediately push one RecordBatch to prime the subscription
            // (offset 0 — delivers first record so the sub is confirmed active).
            sendRecordBatch(opt->sessionId, kSubscriptionId,
                            /*baseOffset=*/0);
            subscribesHandled_.fetch_add(1, std::memory_order_relaxed);
            break;
        }

        default:
            break;
        }
    }
};

// ---------------------------------------------------------------------------
// BenchFixture
// ---------------------------------------------------------------------------

struct BenchFixture {
    static constexpr int32_t kIngressStream = 800;
    static constexpr int32_t kEgressStream  = 801;

    std::string                          agentDir;
    std::string                          aeronDir;
    std::unique_ptr<Client>              client;
    std::unique_ptr<ProducerBenchGateway> gw;
    TopicHandle                          th;

    BenchFixture() {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";

        ClientConfig cfg;
        cfg.gatewayChannel   = "aeron:ipc";
        cfg.gatewayStreamId  = kIngressStream;
        cfg.egressChannel    = "aeron:ipc";
        cfg.egressStreamId   = kEgressStream;
        cfg.dataDir          = agentDir;
        cfg.connectTimeoutMs = kConnTimeoutMs;
        cfg.pingTimeoutMs    = kOpTimeoutMs;
        cfg.idleMode         = IdleMode::LowLatency;

        // Register a static topic.
        TopicSpec ts;
        ts.name           = "bench_topic";
        ts.topicId        = kTopicId;
        ts.partitionCount = 1;
        cfg.topics.push_back(ts);

        client = std::make_unique<Client>(cfg);

        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<ProducerBenchGateway>(aeronDir);
        gw->start(kIngressStream, kEgressStream);

        // Connect.
        client->connectAndWait(std::chrono::milliseconds(kConnTimeoutMs));

        th = client->topic("bench_topic");
        if (!th.valid())
            throw std::runtime_error("BenchFixture: bench_topic not found");

        // Warm up produce path (honor env-driven batch knobs for fair batched
        // throughput measurement: RUNE_LINGER_US, RUNE_BATCH_BYTES, RUNE_BATCH_RECORDS).
        ProducerConfig pcfg = ProducerConfig::fromEnv();
        pcfg.acks             = Acks::Leader;
        if (pcfg.lingerUs    == 5'000)    pcfg.lingerUs   = 1; // override default for bench
        if (pcfg.batchBytes  == 64*1024)  pcfg.batchBytes = 1; // override default for bench
        pcfg.maxInFlight      = 16;
        pcfg.produceTimeoutMs = kOpTimeoutMs;

        auto p = client->producer(pcfg);
        Key key(uint64_t{0});
        const std::vector<uint8_t> wval = {'w','a','r','m'};

        for (int i = 0; i < kWarmupIter; ++i) {
            auto wf = p.send(th, key, std::span<const uint8_t>(wval), want_future);
            const auto wdl = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(kOpTimeoutMs);
            if (wf.wait_until(wdl) != std::future_status::ready)
                throw std::runtime_error("BenchFixture: warm-up produce timed out");
            wf.get();
        }
    }

    ~BenchFixture() {
        if (client) { client->close(); client.reset(); }
        if (gw)     { gw->stop(); gw.reset(); }
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
};

static BenchFixture* g_fixture = nullptr;

// ---------------------------------------------------------------------------
// BM_ProduceRTT — produce one record, wait for ProduceResponse ack.
// Gate: p99 < 100µs.
// ---------------------------------------------------------------------------

static void BM_ProduceRTT(benchmark::State& state) {
    static const std::vector<uint8_t> kVal(64, 0xAB); // 64-byte payload
    static Key kKey(uint64_t{1});

    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = kOpTimeoutMs;

    auto p = g_fixture->client->producer(pcfg);

    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

    for (auto _ : state) {
        state.PauseTiming();
        auto& c = *g_fixture->client;
        (void)c;
        state.ResumeTiming();

        const auto t0 = std::chrono::steady_clock::now();
        auto fut = p.send(g_fixture->th, kKey,
                          std::span<const uint8_t>(kVal),
                          want_future);

        const auto dl = t0 + std::chrono::milliseconds(kOpTimeoutMs);
        if (fut.wait_until(dl) != std::future_status::ready) {
            state.SkipWithError("ProduceRTT: future timed out");
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
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count())
            / 1000.0;
        samples.push_back(us);
        state.SetIterationTime(us * 1e-6);
    }

    if (samples.empty()) return;

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

    state.counters["p50_us"]  = pct(0.50);
    state.counters["p99_us"]  = pct(0.99);
    state.counters["p999_us"] = pct(0.999);
    state.counters["mean_us"] = mean;
    state.counters["samples"] = static_cast<double>(n);

    // Report coalescing efficiency: records_per_frame (> 1 means batching occurred).
    const uint64_t frames  = p.framesSent();
    const uint64_t records = p.recordsSent();
    state.counters["records_per_frame"] =
        (frames > 0) ? static_cast<double>(records) / static_cast<double>(frames)
                     : 0.0;

    // Enforce gate: p99 < 100µs.
    if (pct(0.99) >= 100.0) {
        state.SkipWithError("GATE FAIL: ProduceRTT p99 >= 100µs");
    }
}

BENCHMARK(BM_ProduceRTT)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond)
    ->MinTime(3.0);

// ---------------------------------------------------------------------------
// BM_SubscribeDeliveryLatency
//
// Measures: from the moment the ProduceResponse is received (produce-ack) to
// the moment the RecordHandler fires for the delivered record.
//
// This exercises the subscribe delivery path: SubscribeRequest → gateway →
// SubscribeResponse → RecordBatch push → fragment assembler → handler.
//
// Implementation:
//   1. Subscribe from Earliest (sets up the subscription once).
//   2. For each iteration: produce one record, gateway sends ProduceResponse
//      (stamps produceAckNs_), then gateway pushes RecordBatch (delivery).
//   3. RecordHandler captures delivery timestamp.
//   4. Sample = deliveryNs − produceAckNs_.
//
// Gate: p99 < 100µs.
// ---------------------------------------------------------------------------

// Per-iteration delivery timestamp (set by RecordHandler on agent thread).
static std::atomic<int64_t> g_deliveryNs{0};
static std::atomic<bool>    g_deliveryReady{false};

static void BM_SubscribeDeliveryLatency(benchmark::State& state) {
    static const std::vector<uint8_t> kVal(32, 0xCD);
    static Key kKey(uint64_t{2});

    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = kOpTimeoutMs;

    auto p = g_fixture->client->producer(pcfg);

    // Subscribe once (stays active across iterations).
    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;

    auto sub = g_fixture->client->subscribe(
        g_fixture->th,
        kPartition,
        opts,
        [](const Record&) {
            // Stamp delivery time and signal ready.
            g_deliveryNs.store(
                static_cast<int64_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count()),
                std::memory_order_release);
            g_deliveryReady.store(true, std::memory_order_release);
        });

    // Wait until the client has processed the SubscribeResponse (subscriptionId
    // transitions from 0 to the broker-assigned id).  The gateway is
    // tight-polling so this should complete in microseconds; we yield rather
    // than sleep so we don't add tens of milliseconds of OS wake-up latency.
    const auto subDl = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(kOpTimeoutMs);
    while (std::chrono::steady_clock::now() < subDl) {
        if (sub.subscriptionId() != 0) break;
        std::this_thread::yield();
    }
    if (sub.subscriptionId() == 0) {
        state.SkipWithError("SubscribeDelivery: SubscribeResponse not received in time");
        return;
    }

    // Also drain any priming RecordBatch the gateway sent on Subscribe so that
    // the first iteration's reset is clean.  We reset g_deliveryReady here and
    // briefly wait for it — if the priming batch already fired, great; if not,
    // we don't block long (the first iteration will handle it).
    {
        const auto primeDl = std::chrono::steady_clock::now() + 50ms;
        while (!g_deliveryReady.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < primeDl) {
            std::this_thread::yield();
        }
        // Reset so iteration 1 waits for its own produce-triggered delivery.
        g_deliveryReady.store(false, std::memory_order_release);
        g_deliveryNs.store(0, std::memory_order_release);
    }

    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

    for (auto _ : state) {
        // Reset delivery flag.
        g_deliveryReady.store(false, std::memory_order_release);
        g_deliveryNs.store(0, std::memory_order_release);
        g_fixture->gw->produceAckNs_.store(0, std::memory_order_release);

        state.PauseTiming();
        // (Nothing to pause — timing is manual)
        state.ResumeTiming();

        // Produce: gateway will send ProduceResponse (stamps ackNs) then
        // push RecordBatch (triggers delivery).
        auto fut = p.send(g_fixture->th, kKey,
                          std::span<const uint8_t>(kVal),
                          want_future);

        // Wait for produce ack.
        const auto dl = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(kOpTimeoutMs);
        if (fut.wait_until(dl) != std::future_status::ready) {
            state.SkipWithError("SubscribeDelivery: produce timed out");
            break;
        }
        try { (void)fut.get(); } catch (const std::exception& ex) {
            state.SkipWithError(ex.what()); break;
        }

        // Wait for delivery.
        const auto delivDl = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(kOpTimeoutMs);
        while (!g_deliveryReady.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < delivDl) {
            std::this_thread::yield();
        }
        if (!g_deliveryReady.load(std::memory_order_acquire)) {
            state.SkipWithError("SubscribeDelivery: record not delivered in time");
            break;
        }

        const int64_t ackNs  = g_fixture->gw->produceAckNs_.load(
            std::memory_order_acquire);
        const int64_t delivNs = g_deliveryNs.load(std::memory_order_acquire);

        if (ackNs <= 0 || delivNs <= 0) {
            // Timestamps not captured properly; skip sample.
            continue;
        }

        const double us = static_cast<double>(delivNs - ackNs) / 1000.0;
        if (us < 0.0) continue; // clock skew guard (same thread → shouldn't happen)
        samples.push_back(us);
        state.SetIterationTime(us * 1e-6);
    }

    sub.unsubscribe();

    if (samples.empty()) return;

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

    state.counters["p50_us"]  = pct(0.50);
    state.counters["p99_us"]  = pct(0.99);
    state.counters["p999_us"] = pct(0.999);
    state.counters["mean_us"] = mean;
    state.counters["samples"] = static_cast<double>(n);

    // Enforce gate: p99 < 100µs.
    if (pct(0.99) >= 100.0) {
        state.SkipWithError("GATE FAIL: SubscribeDelivery p99 >= 100µs");
    }
}

BENCHMARK(BM_SubscribeDeliveryLatency)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond)
    ->MinTime(3.0);

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

    BenchFixture fixture;
    g_fixture = &fixture;

    ::benchmark::RunSpecifiedBenchmarks();

    g_fixture = nullptr;
    return 0;
}
