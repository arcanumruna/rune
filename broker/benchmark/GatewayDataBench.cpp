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

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        // Use a larger term buffer for throughput test to avoid back-pressure
        // in the sustained produce path (1 MiB is still well within IPC budget).
        aeron_driver_context_set_term_buffer_length(driverCtx_, 1024 * 1024);

        if (aeron_driver_init(&driver_, driverCtx_) != 0 ||
            aeron_driver_start(driver_, /*manual_main_loop=*/true) != 0) {
            state.SkipWithError("aeron_driver_init/start failed");
            return;
        }

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(
                    driver_, aeron_driver_main_do_work(driver_));
            }
        });

        const std::string cncPath = aeronDir + "/cnc.dat";
        for (int i = 0; i < 400; ++i) {
            aeron_driver_main_do_work(driver_);
            if (fs::exists(cncPath)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        if (!fs::exists(cncPath)) {
            state.SkipWithError("Aeron CnC not created in time");
        }
    }

    void stopDriver()
    {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds{50};
            while (std::chrono::steady_clock::now() < dl) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_);
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
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
    aeron_driver_context_t* driverCtx_   = nullptr;
    aeron_driver_t*         driver_      = nullptr;
    std::atomic<bool>       driverRunning_{false};
    std::thread             driverThread_;
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
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

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
        // Gate flag: 1.0 = pass (<100µs), 0.0 = fail.
        state.counters["produce_p99_gate_pass"] = (pct(0.99) < 100.0) ? 1.0 : 0.0;
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
    samples.reserve(static_cast<std::size_t>(state.max_iterations));

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

        if (!got) { state.SkipWithError("Records delivery timed out"); return; }

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
        state.counters["subdeliv_p99_gate_pass"] = (pct(0.99) < 100.0) ? 1.0 : 0.0;
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
