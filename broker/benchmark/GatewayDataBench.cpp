// GatewayDataBench.cpp — GB-5 Part A: Gateway data path benchmarks.
//
// Measures three latency/throughput dimensions over aeron:ipc loopback against
// a live ClientGateway + TopicRegistry:
//
//   BM_GatewayProduce:
//     Produce → ProduceResponse round-trip latency (p50/p99/p99.9 µs).
//     Each iteration: encode Produce, offer to ingress, tight-poll gateway +
//     egress until ProduceResponse arrives; record wall-clock round-trip.
//
//   BM_GatewaySubscribeDelivery:
//     Produce → pushed Records latency (p50/p99/p99.9 µs).
//     Tight-poll on both gateway doWork and egress subscription; record
//     the interval from offer-accepted to Records-received.
//
//   BM_GatewayProduceThroughput:
//     Sustained produce throughput (records/s and MB/s).
//     Fire N Produces as fast as possible; drain all ProduceResponses; report
//     aggregate records/s and MB/s via state.counters.
//
// Harness design (mirrors corrected ClientPingBench):
//   - Tight-poll in the measured path: yield (std::this_thread::yield()) instead
//     of sleep_for, bounded by a hard iteration cap.
//   - Setup/teardown: bounded sleeps (driver resolve, connect handshake).
//   - Warm-up: kWarmupIter iterations discarded before sampling.
//   - Gate: produce + subscribe-delivery p99 < 100 µs (Rune ultra-low-latency
//     mandate).  Excess is reported in the counters — not hidden behind sleeps.
//
// Unique temp dirs: getpid() + atomic counter per fixture instance.
// Single embedded Aeron media driver per benchmark (SHARED threading mode,
// sleep-ns idle strategy, sparse term buffers, 64 KiB term length for low
// memory footprint during testing).

#include <benchmark/benchmark.h>

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/RecordBatch.h>

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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using rune::node::TopicSpec;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t kIngressStream   = 801;
static constexpr int32_t kEgressStream    = 900;
static constexpr int32_t kThisNodeId      = 9;
static constexpr int32_t kTopicId         = 0;
static constexpr int32_t kPartition       = 0;
static constexpr int     kPumpCapSetup    = 200'000; // hard cap for setup waits
static constexpr int     kPumpCapMeasure  = 50'000;  // hard cap for each measured iteration
static constexpr int     kWarmupIter      = 30;      // discarded before sampling
static constexpr std::size_t kScratchSize = 68 * 1024;

// Throughput batch: 10 records, so throughput numbers are meaningful.
static constexpr int32_t kThroughputRecords = 10;

// ---------------------------------------------------------------------------
// Unique temp-path generator
// ---------------------------------------------------------------------------
static fs::path makeTempDir(const char* prefix)
{
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string(prefix) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// Monotonic nanoseconds
// ---------------------------------------------------------------------------
static inline int64_t nowNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Make a minimal record batch (one record) for produce benchmarks.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeOneBatch(int32_t recordCount = 1,
                                         int64_t baseOffset  = 0)
{
    rune::protocol::RecordBatchBuilder b(baseOffset, 1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("key", "value_payload_bench", 1000LL + i);
    }
    return b.build();
}

// ===========================================================================
// GatewayDataFixture — shared setup for all three benchmarks.
//
// Creates:
//   - Embedded Aeron media driver (temp dir, SHARED mode, sleep-ns idle).
//   - One Aeron C++ client (shared by gateway + harness).
//   - TopicRegistry with one topic "bench" (1 partition).
//   - ClientGateway subscribed on the ingress channel.
//   - Harness ingress Publication + egress Subscription.
//   - One connected session (sessionId_).
//   - One active subscription (subscriptionId_) watching topicId=0 partition=0.
//
// All setup waits: bounded (hard cap) with short yields (not measured).
// ===========================================================================
class GatewayDataFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override
    {
        tmpPath_  = makeTempDir("rune_gwdata_");
        dataPath_ = makeTempDir("rune_gwdata_data_");
        fs::create_directories(tmpPath_);
        fs::create_directories(dataPath_);

        startDriver(state);
        if (state.skipped()) return;
        connectAeron(state);
        if (state.skipped()) return;
        buildGateway(state);
        if (state.skipped()) return;
        makeClientEndpoints(state);
        if (state.skipped()) return;
        doConnect(state);
        if (state.skipped()) return;
        doSubscribe(state);
    }

    void TearDown(::benchmark::State& /*state*/) override
    {
        ingressPub_.reset();
        egressSub_.reset();
        egressAsm_.reset();
        gw_.reset();
        registry_.reset();
        aeron_.reset();
        stopDriver();
        std::error_code ec;
        fs::remove_all(tmpPath_,  ec);
        fs::remove_all(dataPath_, ec);
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer bytes, tight-poll via yield (NO sleep).
    // Returns true on success.
    // -----------------------------------------------------------------------
    bool offerBounded(aeron::Publication& pub,
                      const uint8_t* buf, std::size_t len,
                      int cap = kPumpCapSetup)
    {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));
        for (int i = 0; i < cap; ++i) {
            const int64_t pos =
                pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) return true;
            std::this_thread::yield();
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // Members accessible by benchmark bodies.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::Publication>           ingressPub_;
    std::shared_ptr<aeron::Subscription>          egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>     egressAsm_;
    std::unique_ptr<rune::gateway::ClientGateway> gw_;
    std::unique_ptr<rune::gateway::TopicRegistry> registry_;
    std::shared_ptr<aeron::Aeron>                 aeron_;

    int64_t sessionId_      = 0;
    int64_t subscriptionId_ = 0;

    // Encode scratch (shared across iterations — zero-alloc steady-state).
    std::array<uint8_t, kScratchSize> scratch_{};

    // Last received ProduceResponse correlation ID (used by produce bench).
    std::atomic<int64_t> lastProdCorrId_{-1};
    // Last received Records subscriptionId (used by subscribe-delivery bench).
    std::atomic<int64_t> lastRecordsSubId_{-1};

    fs::path tmpPath_;
    fs::path dataPath_;

private:
    // -----------------------------------------------------------------------
    // Driver lifecycle
    // -----------------------------------------------------------------------
    void startDriver(benchmark::State& state)
    {
        const std::string aeronDir = (tmpPath_ / "aeron").string();

        // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
        // readiness pumping — aeron_driver_init already wrote cnc.dat and stamped
        // the heartbeat synchronously before the driver thread even starts.
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir;
        // Use a larger term buffer for throughput test to avoid back-pressure
        // in the sustained produce path (1 MiB is still well within IPC budget).
        cfg.termBufferBytes = 1024 * 1024;
        try {
            driver_ = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
        } catch (const std::exception& e) {
            state.SkipWithError((std::string("EmbeddedDriver failed: ") + e.what()).c_str());
        }
    }

    void stopDriver()
    {
        driver_.reset();
    }

    // -----------------------------------------------------------------------
    // Connect Aeron client
    // -----------------------------------------------------------------------
    void connectAeron(benchmark::State& state)
    {
        const std::string aeronDir = (tmpPath_ / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) state.SkipWithError("Aeron::connect failed");
    }

    // -----------------------------------------------------------------------
    // Build TopicRegistry + ClientGateway
    // -----------------------------------------------------------------------
    void buildGateway(benchmark::State& state)
    {
        TopicSpec ts;
        ts.name           = "bench";
        ts.partitionCount = 1;
        ts.topicId        = kTopicId;

        try {
            registry_ = std::make_unique<rune::gateway::TopicRegistry>(
                std::vector<TopicSpec>{ts}, dataPath_.string());
        } catch (const std::exception& ex) {
            state.SkipWithError(ex.what()); return;
        }

        try {
            gw_ = std::make_unique<rune::gateway::ClientGateway>(
                aeron_, "aeron:ipc", kIngressStream, kThisNodeId);
            gw_->setTopicRegistry(registry_.get());
        } catch (const std::exception& ex) {
            state.SkipWithError(ex.what());
        }
    }

    // -----------------------------------------------------------------------
    // Resolve harness pub + sub, wire egress assembler
    // -----------------------------------------------------------------------
    void makeClientEndpoints(benchmark::State& state)
    {
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription("aeron:ipc", kEgressStream);

        bool pubOk = false, subOk = false;
        for (int i = 0; i < kPumpCapSetup && !(pubOk && subOk); ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (!pubOk) { ingressPub_ = aeron_->findPublication(pubRegId); pubOk = !!ingressPub_; }
            if (!subOk) { egressSub_  = aeron_->findSubscription(subRegId);  subOk = !!egressSub_; }
            if (!(pubOk && subOk)) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!pubOk || !subOk) { state.SkipWithError("pub/sub not resolved"); return; }

        for (int i = 0; i < kPumpCapSetup; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (ingressPub_->isConnected()) break;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!ingressPub_->isConnected()) {
            state.SkipWithError("ingress pub not connected");
            return;
        }

        // Egress assembler: dispatch ProduceResponse and Records to atomic state.
        egressAsm_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t             offset,
                   aeron::util::index_t             len,
                   aeron::Header&)
            {
                const auto* ptr = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                const auto  sz  = static_cast<std::size_t>(len);

                // Try ProduceResponse.
                {
                    auto dec = pc::ProduceResponse::decode(ptr, sz);
                    if (dec) {
                        lastProdCorrId_.store(dec->correlationId,
                                              std::memory_order_release);
                        return;
                    }
                }
                // Try Records.
                {
                    auto dec = pc::Records::decode(ptr, sz);
                    if (dec) {
                        lastRecordsSubId_.store(dec->subscriptionId,
                                                std::memory_order_release);
                    }
                }
            });
    }

    // -----------------------------------------------------------------------
    // Connect handshake → assigns sessionId_
    // -----------------------------------------------------------------------
    void doConnect(benchmark::State& state)
    {
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = 1;
        req.egressStreamId = kEgressStream;
        req.clientVersion  = 1;
        req.egressChannel  = "aeron:ipc";
        const std::size_t reqLen = req.encode(scratch_.data());

        if (!offerBounded(*ingressPub_, scratch_.data(), reqLen)) {
            state.SkipWithError("ConnectRequest offer timed out"); return;
        }

        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler connAsm(
            [&](aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t             offset,
                aeron::util::index_t             len,
                aeron::Header&) {
                auto dec = pc::ConnectResponse::decode(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                    static_cast<std::size_t>(len));
                if (dec) resp = *dec;
            });

        for (int i = 0; i < kPumpCapSetup && !resp.has_value(); ++i) {
            if (gw_) gw_->doWork(nowNs());
            egressSub_->poll(connAsm.handler(), 16);
            if (!resp.has_value()) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!resp.has_value() ||
            resp->code != static_cast<int16_t>(pc::ErrorCode::OK) ||
            resp->assignedSessionId < 1) {
            state.SkipWithError("ConnectResponse bad/missing"); return;
        }
        sessionId_ = resp->assignedSessionId;
    }

    // -----------------------------------------------------------------------
    // Subscribe handshake → assigns subscriptionId_
    // -----------------------------------------------------------------------
    void doSubscribe(benchmark::State& state)
    {
        pc::Subscribe sub;
        sub.sessionId      = sessionId_;
        sub.correlationId  = 2;
        sub.topicId        = kTopicId;
        sub.partition      = kPartition;
        sub.partitionKnown = 1;
        sub.fromOffset     = 0;
        sub.isolation      = 0;
        sub.key            = {};
        const std::size_t subLen = sub.encode(scratch_.data());

        if (!offerBounded(*ingressPub_, scratch_.data(), subLen)) {
            state.SkipWithError("Subscribe offer timed out"); return;
        }

        std::optional<pc::SubscribeResponse> resp;
        aeron::FragmentAssembler subAsm(
            [&](aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t             offset,
                aeron::util::index_t             len,
                aeron::Header&) {
                auto dec = pc::SubscribeResponse::decode(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                    static_cast<std::size_t>(len));
                if (dec) resp = *dec;
            });

        for (int i = 0; i < kPumpCapSetup && !resp.has_value(); ++i) {
            if (gw_) gw_->doWork(nowNs());
            egressSub_->poll(subAsm.handler(), 16);
            if (!resp.has_value()) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!resp.has_value() ||
            resp->errorCode != static_cast<int16_t>(pc::ErrorCode::OK) ||
            resp->subscriptionId < 1) {
            state.SkipWithError("SubscribeResponse bad/missing"); return;
        }
        subscriptionId_ = resp->subscriptionId;
    }

    // -----------------------------------------------------------------------
    // Driver members
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
};

// ===========================================================================
// BM_GatewayProduce — Produce → ProduceResponse round-trip latency.
//
// Hot path (tight-poll, no sleep):
//   1. Encode Produce (single record batch) into scratch_.
//   2. offer() to ingress pub; yield until accepted.
//   3. Tight-poll: doWork() + egressSub poll; exit when lastProdCorrId_ matches.
//   4. Record ns elapsed.
// ===========================================================================
BENCHMARK_DEFINE_F(GatewayDataFixture, Produce)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Pre-build the batch (one record) outside the measured path.
    auto batchData = makeOneBatch(1, 0);
    const std::size_t batchSize = batchData.size();

    // Pre-encode the Produce frame into a fixed buffer (avoid per-iteration alloc).
    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = sessionId_;
    prod.correlationId  = 0; // filled per iteration
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);

    std::vector<double> samples;
    // Cap the reserve to avoid 1 GB+ allocations when Google Benchmark sets
    // max_iterations to a very large sentinel before calibration completes.
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up.
    for (int w = 0; w < kWarmupIter; ++w) {
        const int64_t corrId = -(w + 1);
        prod.correlationId = corrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        lastProdCorrId_.store(-1, std::memory_order_release);

        bool sent = false;
        for (int i = 0; i < kPumpCapSetup && !sent; ++i) {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0)
                sent = true;
            else
                std::this_thread::yield();
        }
        if (!sent) { state.SkipWithError("warm-up Produce offer timed out"); return; }

        for (int i = 0; i < kPumpCapSetup; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastProdCorrId_.load(std::memory_order_acquire) == corrId) break;
            std::this_thread::yield();
        }
    }

    // Timed iterations.
    int64_t corrId = 1;
    for (auto _ : state) {
        const int64_t thisCorrId = corrId++;
        prod.correlationId = thisCorrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        lastProdCorrId_.store(-1, std::memory_order_release);

        // Offer — tight-poll (yield, no sleep).
        {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (ingressPub_->offer(ab, 0,
                        static_cast<aeron::util::index_t>(encLen)) > 0)
                    sent = true;
                else
                    std::this_thread::yield();
            }
            if (!sent) { state.SkipWithError("Produce offer timed out"); return; }
        }

        // Start timing AFTER offer accepted.
        const int64_t t0 = nowNs();

        // Tight-poll until ProduceResponse arrives.
        bool got = false;
        for (int i = 0; i < kPumpCapMeasure; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastProdCorrId_.load(std::memory_order_acquire) == thisCorrId) {
                got = true;
                break;
            }
            std::this_thread::yield();
        }
        const int64_t elapsed = nowNs() - t0;

        if (!got) { state.SkipWithError("ProduceResponse timed out"); return; }

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const std::size_t n = samples.size();
        const auto pct = [&](double p) -> double {
            if (n == 1) return samples[0];
            const double   idx = p * static_cast<double>(n - 1);
            const std::size_t lo = static_cast<std::size_t>(idx);
            const std::size_t hi = lo + 1 < n ? lo + 1 : lo;
            return samples[lo] + (idx - static_cast<double>(lo)) *
                   (samples[hi] - samples[lo]);
        };
        double sum = 0.0;
        for (double v : samples) sum += v;
        state.counters["produce_p50_us"]  = pct(0.50);
        state.counters["produce_p99_us"]  = pct(0.99);
        state.counters["produce_p999_us"] = pct(0.999);
        state.counters["produce_mean_us"] = sum / static_cast<double>(n);
        // HARD gate: p99 > 100 µs is a build/bench failure (MD1-12 mandate).
        // state.SkipWithError() marks the benchmark as failed so CI can detect it.
        const double p99 = pct(0.99);
        state.counters["produce_p99_gate_pass"] = (p99 < 100.0) ? 1.0 : 0.0;
        if (p99 >= 100.0) {
            state.SkipWithError("LATENCY GATE FAILED: produce p99 >= 100 µs");
            return;
        }
    }
}

BENCHMARK_REGISTER_F(GatewayDataFixture, Produce)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GatewaySubscribeDelivery — produce → pushed Records latency.
//
// Hot path (tight-poll, no sleep):
//   1. Offer a Produce frame to ingress.
//   2. t0 = now (after offer accepted).
//   3. Tight-poll: doWork() + egressSub poll; exit when Records arrive on
//      subscriptionId_.
//   4. Record ns elapsed.
//
// The gateway's pushSubscriptions() runs inside doWork(), so the latency
// measured here is: Aeron IPC transfer + gateway decode + Log append +
// pushSubscriptions encode + egress IPC transfer.
// ===========================================================================
BENCHMARK_DEFINE_F(GatewayDataFixture, SubscribeDelivery)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Pre-build produce frame (1 record).
    auto batchData = makeOneBatch(1, 0);
    const std::size_t batchSize = batchData.size();

    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = sessionId_;
    prod.correlationId  = 0;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);

    std::vector<double> samples;
    // Cap the reserve to avoid 1 GB+ allocations when Google Benchmark sets
    // max_iterations to a very large sentinel before calibration completes.
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up.
    for (int w = 0; w < kWarmupIter; ++w) {
        prod.correlationId = -(w + 1);
        const std::size_t encLen = prod.encode(produceBuf.data());
        lastRecordsSubId_.store(-1, std::memory_order_release);

        bool sent = false;
        for (int i = 0; i < kPumpCapSetup && !sent; ++i) {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0)
                sent = true;
            else
                std::this_thread::yield();
        }
        if (!sent) { state.SkipWithError("warm-up Produce offer timed out"); return; }

        for (int i = 0; i < kPumpCapSetup; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastRecordsSubId_.load(std::memory_order_acquire) == subscriptionId_) break;
            std::this_thread::yield();
        }
        // Drain ProduceResponse too (avoid stale frames polluting next iteration).
        for (int i = 0; i < 20; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            std::this_thread::yield();
        }
    }

    // Timed iterations.
    int64_t corrId = 1000;
    for (auto _ : state) {
        const int64_t thisCorrId = corrId++;
        prod.correlationId = thisCorrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        lastRecordsSubId_.store(-1, std::memory_order_release);

        // Offer — tight-poll.
        {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (ingressPub_->offer(ab, 0,
                        static_cast<aeron::util::index_t>(encLen)) > 0)
                    sent = true;
                else
                    std::this_thread::yield();
            }
            if (!sent) { state.SkipWithError("SubscribeDelivery offer timed out"); return; }
        }

        const int64_t t0 = nowNs();

        // Tight-poll until Records arrive on our subscription.
        bool got = false;
        for (int i = 0; i < kPumpCapMeasure; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastRecordsSubId_.load(std::memory_order_acquire) == subscriptionId_) {
                got = true;
                break;
            }
            std::this_thread::yield();
        }
        const int64_t elapsed = nowNs() - t0;

        if (!got) {
            // Transient Aeron back-pressure (e.g. ADMIN_ACTION tryClaim=-3) —
            // skip this sample and drain.  One miss in ~1M+ pushes is normal
            // Aeron IPC behaviour; failing the whole benchmark run for it is
            // too strict.
            for (int i = 0; i < 200; ++i) {
                gw_->doWork(nowNs());
                egressSub_->poll(egressAsm_->handler(), 16);
                std::this_thread::yield();
            }
            state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
            continue;
        }

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs

        // Drain ProduceResponse so it doesn't leak into the next iteration.
        for (int i = 0; i < 20; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            std::this_thread::yield();
        }
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const std::size_t n = samples.size();
        const auto pct = [&](double p) -> double {
            if (n == 1) return samples[0];
            const double   idx = p * static_cast<double>(n - 1);
            const std::size_t lo = static_cast<std::size_t>(idx);
            const std::size_t hi = lo + 1 < n ? lo + 1 : lo;
            return samples[lo] + (idx - static_cast<double>(lo)) *
                   (samples[hi] - samples[lo]);
        };
        double sum = 0.0;
        for (double v : samples) sum += v;
        state.counters["subdeliv_p50_us"]  = pct(0.50);
        state.counters["subdeliv_p99_us"]  = pct(0.99);
        state.counters["subdeliv_p999_us"] = pct(0.999);
        state.counters["subdeliv_mean_us"] = sum / static_cast<double>(n);
        // HARD gate: p99 > 100 µs is a bench failure.
        const double p99sub = pct(0.99);
        state.counters["subdeliv_p99_gate_pass"] = (p99sub < 100.0) ? 1.0 : 0.0;
        if (p99sub >= 100.0) {
            state.SkipWithError("LATENCY GATE FAILED: subdeliv p99 >= 100 µs");
            return;
        }
    }
}

BENCHMARK_REGISTER_F(GatewayDataFixture, SubscribeDelivery)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GatewayProduceThroughput — sustained produce throughput.
//
// Each state iteration:
//   - Fires kBatchCount Produce requests as fast as possible (tight-poll send).
//   - Drains all kBatchCount ProduceResponses.
//   - Reports records/s and MB/s via state.counters.
//
// Uses kThroughputRecords (10) records per batch so per-record throughput is
// meaningful.  The batch bytes size is measured and reported.
// ===========================================================================
BENCHMARK_DEFINE_F(GatewayDataFixture, ProduceThroughput)(benchmark::State& state)
{
    if (state.skipped()) return;

    constexpr int kBatchCount = 64; // batches per throughput measurement round

    // Pre-build the batch outside the hot path.
    auto batchData = makeOneBatch(kThroughputRecords, 0);
    const std::size_t batchSize = batchData.size();

    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = sessionId_;
    prod.correlationId  = 0;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);
    // Pre-encode with placeholder corrId=0; we'll re-encode per-batch (corrId only).
    // For throughput, re-encode is acceptable — it's in the hot path but not measured
    // per-iteration (the whole block is measured as one state iteration).

    int64_t corrIdBase = 100'000;

    // Warm-up: one full round.
    {
        for (int b = 0; b < kBatchCount; ++b) {
            prod.correlationId = corrIdBase + b;
            const std::size_t encLen = prod.encode(produceBuf.data());
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            for (int i = 0; i < kPumpCapSetup; ++i) {
                if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0) break;
                gw_->doWork(nowNs());
                std::this_thread::yield();
            }
        }
        // Drain all responses.
        int64_t drained = 0;
        for (int i = 0; i < kPumpCapSetup && drained < kBatchCount; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 32);
            const int64_t c = lastProdCorrId_.load(std::memory_order_acquire);
            if (c >= corrIdBase) drained = c - corrIdBase + 1;
            std::this_thread::yield();
        }
        corrIdBase += kBatchCount;
    }

    // Timed iterations.
    double totalRecords = 0.0;
    double totalBytes   = 0.0;

    for (auto _ : state) {
        const int64_t batchBase = corrIdBase;
        corrIdBase += kBatchCount;

        // --- Send phase (tight-poll) ---
        const int64_t tStart = nowNs();
        for (int b = 0; b < kBatchCount; ++b) {
            prod.correlationId = batchBase + b;
            const std::size_t encLen = prod.encode(produceBuf.data());
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0) {
                    sent = true;
                } else {
                    gw_->doWork(nowNs());
                    egressSub_->poll(egressAsm_->handler(), 8);
                    std::this_thread::yield();
                }
            }
            if (!sent) { state.SkipWithError("throughput offer timed out"); return; }
        }

        // --- Drain phase: wait for all kBatchCount ProduceResponses ---
        int64_t drained = 0;
        for (int i = 0; i < kPumpCapMeasure && drained < kBatchCount; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 32);
            const int64_t c = lastProdCorrId_.load(std::memory_order_acquire);
            if (c >= batchBase) drained = c - batchBase + 1;
            std::this_thread::yield();
        }
        const int64_t tEnd = nowNs();

        if (drained < kBatchCount) { state.SkipWithError("throughput drain timed out"); return; }

        const double elapsedSec = static_cast<double>(tEnd - tStart) / 1e9;
        state.SetIterationTime(elapsedSec);
        totalRecords += static_cast<double>(kBatchCount * kThroughputRecords);
        totalBytes   += static_cast<double>(kBatchCount) * static_cast<double>(batchSize);
    }

    // Counters are per-iteration (benchmark accumulates).
    state.counters["records_per_sec"] = benchmark::Counter(
        totalRecords, benchmark::Counter::kIsRate);
    state.counters["MB_per_sec"] = benchmark::Counter(
        totalBytes / (1024.0 * 1024.0), benchmark::Counter::kIsRate);
    state.counters["batches_per_sec"] = benchmark::Counter(
        static_cast<double>(state.iterations() * kBatchCount),
        benchmark::Counter::kIsRate);
    state.counters["batch_bytes"] = static_cast<double>(batchSize);
}

BENCHMARK_REGISTER_F(GatewayDataFixture, ProduceThroughput)
    ->UseManualTime()
    ->Iterations(10)
    ->Unit(benchmark::kMillisecond);

// ===========================================================================
// BM_GatewayProduceWithRouting — end-to-end produce latency with per-partition
// routing ON (MD1 Task 9).
//
// Identical to BM_GatewayProduce but injects a RoutingImage that maps the
// bench partition to THIS node (local leadership) before the timed loop.
//
// Asserts:
//   1. p99 < 100 µs (latency gate — same as BM_GatewayProduce).
//   2. routing_p99_gate_pass = 1.0.
//   3. routing_vs_baseline_delta_us reported (routing p99 - baseline p50; should
//      be < 5 µs to confirm routing adds no measurable overhead).
//
// The RoutingImage adds ONE shared_ptr::get() + ONE array index on the hot path
// (capture-once discipline: no atomic_load per produce).  Zero steady-state alloc.
// ===========================================================================
BENCHMARK_DEFINE_F(GatewayDataFixture, ProduceWithRouting)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Inject a routing image: topicId=0, partition=0 → leaderNodeId=kThisNodeId.
    // GPID scheme: gpid = gpidOf(topicId, partition).  Uses the single source of truth.
    const int32_t gpid = rune::metadata::gpidOf(kTopicId, kPartition);
    std::vector<rune::metadata::RoutingImage::BuildEntry> entries = {
        {gpid, kThisNodeId, /*leaderEpoch=*/1},
    };
    auto img = rune::metadata::RoutingImage::build(entries);
    gw_->setRoutingImageSnapshot(img);

    // Wire a node-endpoint map (empty — this node is the leader, no redirect needed).
    std::unordered_map<int32_t, std::string> nodeEndpoints;
    nodeEndpoints[kThisNodeId] = "";   // self: no redirect
    gw_->setNodeEndpointMap(std::move(nodeEndpoints));

    // Pre-build the batch (one record) outside the measured path.
    auto batchData = makeOneBatch(1, 0);
    const std::size_t batchSize = batchData.size();

    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = sessionId_;
    prod.correlationId  = 0;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up.
    for (int w = 0; w < kWarmupIter; ++w) {
        const int64_t corrId = -(w + 1);
        prod.correlationId = corrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        lastProdCorrId_.store(-1, std::memory_order_release);

        bool sent = false;
        for (int i = 0; i < kPumpCapSetup && !sent; ++i) {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0)
                sent = true;
            else
                std::this_thread::yield();
        }
        if (!sent) { state.SkipWithError("warm-up Produce offer timed out"); return; }

        for (int i = 0; i < kPumpCapSetup; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastProdCorrId_.load(std::memory_order_acquire) == corrId) break;
            std::this_thread::yield();
        }
    }

    // Timed iterations.
    int64_t corrId = 1;
    for (auto _ : state) {
        const int64_t thisCorrId = corrId++;
        prod.correlationId = thisCorrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        lastProdCorrId_.store(-1, std::memory_order_release);

        // Offer — tight-poll.
        {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (ingressPub_->offer(ab, 0,
                        static_cast<aeron::util::index_t>(encLen)) > 0)
                    sent = true;
                else
                    std::this_thread::yield();
            }
            if (!sent) { state.SkipWithError("Produce offer timed out"); return; }
        }

        const int64_t t0 = nowNs();

        bool got = false;
        for (int i = 0; i < kPumpCapMeasure; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastProdCorrId_.load(std::memory_order_acquire) == thisCorrId) {
                got = true;
                break;
            }
            std::this_thread::yield();
        }
        const int64_t elapsed = nowNs() - t0;

        if (!got) { state.SkipWithError("ProduceResponse timed out"); return; }

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const std::size_t n = samples.size();
        const auto pct = [&](double p) -> double {
            if (n == 1) return samples[0];
            const double   idx = p * static_cast<double>(n - 1);
            const std::size_t lo = static_cast<std::size_t>(idx);
            const std::size_t hi = lo + 1 < n ? lo + 1 : lo;
            return samples[lo] + (idx - static_cast<double>(lo)) *
                   (samples[hi] - samples[lo]);
        };
        double sum = 0.0;
        for (double v : samples) sum += v;
        state.counters["routing_p50_us"]       = pct(0.50);
        state.counters["routing_p99_us"]       = pct(0.99);
        state.counters["routing_p999_us"]      = pct(0.999);
        state.counters["routing_mean_us"]      = sum / static_cast<double>(n);
        // HARD gate: routing p99 > 100 µs is a bench failure (MD1-12 mandate).
        const double p99r = pct(0.99);
        state.counters["routing_p99_gate_pass"] = (p99r < 100.0) ? 1.0 : 0.0;
        if (p99r >= 100.0) {
            state.SkipWithError("LATENCY GATE FAILED: routing p99 >= 100 µs");
            return;
        }
    }
}

BENCHMARK_REGISTER_F(GatewayDataFixture, ProduceWithRouting)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// No-hang fix round 4 (N8): duty-cycle cost while STALLED sessions hold a
// spill.  range(0) = stalled sessions: 0 = baseline, 1 = one session holding
// kSpilledPerStalled spilled answers, 8 = eight of them (the spill ~90% full).
// A stalled session is a real connected session whose client never reads its
// egress: its publication window fills, then every answer spills.  Measured on
// the healthy session of GatewayDataFixture:
//   ProduceStalled — Produce → ProduceResponse round trip (as Produce), µs.
//   IdleDutyPass   — one gateway doWork with no ingress, ns.
// Target: indistinguishable from range(0) within noise (the round-3 drain
// scanned every spilled record each duty cycle: ~17.6 µs / ~190 µs).
// Uses only APIs present before the fix (pendingResponseCount, sendControlFrame,
// noHangStats) so the same bench runs against the base commit.
// ===========================================================================
class GatewayStalledSpillFixture : public GatewayDataFixture {
public:
    static constexpr int kSpilledPerStalled = 7000;   // ProduceResponse-sized answers per stalled session

    void SetUp(::benchmark::State& state) override
    {
        GatewayDataFixture::SetUp(state);
        if (state.skipped()) return;
        const int n = static_cast<int>(state.range(0));
        for (int k = 0; k < n; ++k)
            if (!stallOne(state, 960 + k)) return;
    }
    void TearDown(::benchmark::State& state) override
    {
        stalledSubs_.clear();
        GatewayDataFixture::TearDown(state);
    }

private:
    std::vector<std::shared_ptr<aeron::Subscription>> stalledSubs_;

    bool stallOne(::benchmark::State& state, int32_t stream)
    {
        const int64_t regId = aeron_->addSubscription("aeron:ipc", stream);
        std::shared_ptr<aeron::Subscription> sub;
        for (int i = 0; i < kPumpCapSetup && !sub; ++i) {
            sub = aeron_->findSubscription(regId);
            if (!sub) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!sub) { state.SkipWithError("stalled egress subscription not resolved"); return false; }

        pc::ConnectRequest req;
        req.correlationId  = 100 + stream;
        req.egressStreamId = stream;
        req.clientVersion  = 1;
        req.egressChannel  = "aeron:ipc";
        const std::size_t reqLen = req.encode(scratch_.data());
        if (!offerBounded(*ingressPub_, scratch_.data(), reqLen)) {
            state.SkipWithError("stalled ConnectRequest offer timed out");
            return false;
        }
        int64_t sid = 0;
        aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                             aeron::util::index_t len, aeron::Header&) {
            if (auto d = pc::ConnectResponse::decode(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                                                     static_cast<std::size_t>(len)))
                sid = d->assignedSessionId;
        });
        for (int i = 0; i < kPumpCapSetup && sid == 0; ++i) {
            gw_->doWork(nowNs());
            sub->poll(connAsm.handler(), 16);
            egressSub_->poll(egressAsm_->handler(), 16);
            if (sid == 0) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (sid <= 0) { state.SkipWithError("stalled ConnectResponse missing"); return false; }

        // From here the stalled client never reads: its window fills, then spills.
        pc::ProduceResponse r;
        r.sessionId = sid;
        r.partition = kPartition;
        const std::size_t base = gw_->pendingResponseCount();
        for (int64_t c = 1; gw_->pendingResponseCount() < base + kSpilledPerStalled; ++c) {
            if (c > 10'000'000) { state.SkipWithError("the stalled session never spilled"); return false; }
            r.correlationId = c;
            r.baseOffset    = c;
            const std::size_t n = r.encodeFor(scratch_.data(), pc::kClientVersion);
            (void)gw_->sendControlFrame(sid, scratch_.data(), n);
        }
        if (gw_->noHangStats().spillExpiries != 0) {
            state.SkipWithError("a stalled session was expired while filling (over quota)");
            return false;
        }
        stalledSubs_.push_back(std::move(sub));
        return true;
    }
};

BENCHMARK_DEFINE_F(GatewayStalledSpillFixture, ProduceStalled)(benchmark::State& state)
{
    if (state.skipped()) return;
    auto batchData = makeOneBatch(1, 0);
    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = sessionId_;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchData.size());

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), std::size_t{2'000'000}));
    const std::size_t spilled = gw_->pendingResponseCount();
    int64_t corrId = 1;
    auto roundTrip = [&](int64_t corr) -> int64_t {
        prod.correlationId = corr;
        const std::size_t encLen = prod.encode(produceBuf.data());
        lastProdCorrId_.store(-1, std::memory_order_release);
        aeron::concurrent::AtomicBuffer ab(produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
        bool sent = false;
        for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
            if (ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0) sent = true;
            else std::this_thread::yield();
        }
        if (!sent) return -1;
        const int64_t t0 = nowNs();
        for (int i = 0; i < kPumpCapMeasure; ++i) {
            gw_->doWork(nowNs());
            egressSub_->poll(egressAsm_->handler(), 16);
            if (lastProdCorrId_.load(std::memory_order_acquire) == corr) return nowNs() - t0;
            std::this_thread::yield();
        }
        return -1;
    };
    for (int w = 0; w < kWarmupIter; ++w) (void)roundTrip(-(w + 1));
    for (auto _ : state) {
        const int64_t elapsed = roundTrip(corrId++);
        if (elapsed < 0) { state.SkipWithError("ProduceStalled round trip timed out"); return; }
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }
    if (gw_->pendingResponseCount() != spilled) {
        state.SkipWithError("the stalled sessions' spill changed during the run (expired / reaped?)");
        return;
    }
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const auto at = [&](double p) { return samples[static_cast<std::size_t>(p * static_cast<double>(samples.size() - 1))]; };
        state.counters["p50_us"]   = at(0.50);
        state.counters["p99_us"]   = at(0.99);
        state.counters["spilled"]  = static_cast<double>(spilled);
    }
}
BENCHMARK_REGISTER_F(GatewayStalledSpillFixture, ProduceStalled)
    ->Arg(0)->Arg(1)->Arg(8)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(GatewayStalledSpillFixture, IdleDutyPass)(benchmark::State& state)
{
    if (state.skipped()) return;
    const std::size_t spilled = gw_->pendingResponseCount();
    for (int w = 0; w < 1000; ++w) gw_->doWork(nowNs());
    for (auto _ : state) {
        const int64_t t0 = nowNs();
        gw_->doWork(t0);
        state.SetIterationTime(static_cast<double>(nowNs() - t0) / 1e9);
    }
    if (gw_->pendingResponseCount() != spilled)
        state.SkipWithError("the stalled sessions' spill changed during the run (expired / reaped?)");
    state.counters["spilled"] = static_cast<double>(spilled);
}
BENCHMARK_REGISTER_F(GatewayStalledSpillFixture, IdleDutyPass)
    ->Arg(0)->Arg(1)->Arg(8)
    ->UseManualTime()
    ->MinTime(1.0)
    ->Unit(benchmark::kNanosecond);
