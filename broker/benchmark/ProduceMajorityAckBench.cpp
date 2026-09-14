// ProduceMajorityAckBench.cpp — GR-8: produce→majority-ack latency benchmark.
//
// Measures two produce ack modes and their cadence trade-off:
//
//   BM_AcksLeaderProduceLatency:
//     Full-stack gateway produce with GR-4 replication wired (leader replica +
//     InMemoryTransport, 3-node ISR, minIsr=1 so HW advances immediately on
//     the single-leader ISR path).  acks=Leader: immediate response.
//     HARD GATE: p99 < 100 µs.  Compare to GB-5 baseline ~4.8 µs.
//
//   BM_AcksAllGateway_Cadence_*:
//     Full-stack gateway produce with acks=All wired (GR-5 deferred-ack).
//     Cluster: leader + 2 followers (InMemoryTransport), ISR=3, minIsr=2.
//     A background duty-cycle thread pumps follower doWork() at configurable
//     fetchIntervalNs (5 ms / 1 ms / 200 µs).  Measures wall-clock time from
//     offer-accepted to ProduceResponse arrival.
//     Reports p50/p99/p99.9 in µs.  NOT gated at 100 µs (durability mode).
//
// Harness design:
//   - Warm-up: kWarmupIter discarded before sampling.
//   - Tight-poll in the measured path: yield (not sleep) on the client side.
//   - Background replication thread pumps at configurable cadence (the variable
//     we're studying).
//   - Percentile reporting via sorted sample vector (same pattern as GatewayDataBench).
//   - No heap allocation in the steady-state hot path.
//
// Fetch-cadence trade-off:
//   The duty-cycle thread wakes at fetchIntervalNs and calls follower.doWork().
//   follower.doWork() emits FetchRequest → transport delivers to leader → leader
//   onFetchRequest() recomputes HW → drainPendingAcks() (in next gateway.doWork())
//   sends ProduceResponse.  Total latency ≈ fetchInterval + Aeron IPC round-trip.
//   By measuring 5 ms → 1 ms → 200 µs we characterise the trade-off.
//
// Push-on-append recommendation:
//   Documented at the bottom of this file in the BM_AcksAll_PushOnAppend comment.

#include <benchmark/benchmark.h>

// Gateway
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>

// Replication
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/node/ReplicaRegistry.h>

// Storage
#include <rune/storage/Log.h>

// Protocol
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

// Aeron
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
namespace pc = rune::protocol::client;
using rune::node::TopicSpec;
using namespace rune::replication;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t kIngressStream   = 801;
static constexpr int32_t kEgressStream    = 902;   // distinct from GatewayDataBench
static constexpr int32_t kThisNodeId      = 7;
static constexpr int32_t kTopicId         = 0;
static constexpr int32_t kPartition       = 0;
static constexpr int     kPumpCapSetup    = 200'000;
static constexpr int     kPumpCapMeasure  = 100'000;
static constexpr int     kWarmupIter      = 30;
static constexpr std::size_t kScratchSize = 68 * 1024;

// ---------------------------------------------------------------------------
// Monotonic nanoseconds
// ---------------------------------------------------------------------------
static inline int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Unique temp-path generator
// ---------------------------------------------------------------------------
static fs::path makeBenchTempDir(const char* prefix)
{
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string(prefix) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// makeOneBatch — single-record RecordBatch for produce benchmarks.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeOneBatch(int64_t baseOffset = 0)
{
    rune::protocol::RecordBatchBuilder b(baseOffset, 1000LL);
    b.append("key", "value_gr8_bench", 1000LL);
    return b.build();
}

// ---------------------------------------------------------------------------
// Percentile helper
// ---------------------------------------------------------------------------
static double pct(const std::vector<double>& sorted, double p)
{
    if (sorted.empty()) return 0.0;
    const std::size_t n = sorted.size();
    if (n == 1) return sorted[0];
    const double   idx = p * static_cast<double>(n - 1);
    const std::size_t lo = static_cast<std::size_t>(idx);
    const std::size_t hi = lo + 1 < n ? lo + 1 : lo;
    return sorted[lo] + (idx - static_cast<double>(lo)) * (sorted[hi] - sorted[lo]);
}

// ===========================================================================
// GR8GatewayFixture — shared embedded Aeron driver + gateway + replica setup.
//
// Constructs:
//   - Embedded Aeron media driver (SHARED mode, sleep-ns idle, sparse terms).
//   - Aeron C++ client.
//   - TopicRegistry (one topic "bench", 1 partition).
//   - ClientGateway subscribed on ingress IPC.
//   - Leader PartitionReplica wired via ReplicaRegistry (GR-4).
//   - Harness ingress Publication + egress Subscription.
//   - One connected session + one subscribe (for optional subscribe delivery check).
//
// Parameterised: minIsr and follower InMemoryTransport setup is done in subclasses.
// ===========================================================================
struct GR8BaseFixture {
    // ---- Aeron ----
    aeron_driver_context_t* driverCtx_   = nullptr;
    aeron_driver_t*         driver_      = nullptr;
    std::atomic<bool>       driverRunning_{false};
    std::thread             driverThread_;
    std::shared_ptr<aeron::Aeron> aeron_;

    // ---- Gateway stack ----
    std::unique_ptr<rune::gateway::TopicRegistry> registry_;
    std::unique_ptr<rune::gateway::ClientGateway> gw_;
    rune::node::ReplicaRegistry                   replicaRegistry_;

    // ---- Replication ----
    InMemoryTransport                             transport_;
    // Log objects owned here (gateway's TopicRegistry owns logs; we hand it the data path).
    // For the replica we need a *separate* Log for each node
    // (leader log is owned by TopicRegistry; we pass a pointer from it).
    // Follower logs are owned by the fixture.
    std::string foll1LogDir_, foll2LogDir_;
    std::unique_ptr<rune::storage::Log> foll1Log_, foll2Log_;

    // Replica pointers (non-owning; registry owns them)
    PartitionReplica* leaderReplica_ = nullptr;
    PartitionReplica* follReplica1_  = nullptr;
    PartitionReplica* follReplica2_  = nullptr;

    // ---- Harness client ----
    std::shared_ptr<aeron::Publication>       ingressPub_;
    std::shared_ptr<aeron::Subscription>      egressSub_;
    std::unique_ptr<aeron::FragmentAssembler> egressAsm_;

    int64_t sessionId_     = 0;

    // Atomic state for benchmark response detection
    std::atomic<int64_t> lastProdCorrId_{-1};

    // Scratch
    std::array<uint8_t, kScratchSize> scratch_{};

    // Temp dirs
    fs::path tmpPath_;
    fs::path dataPath_;

    // Setup flags
    bool setupOk_ = false;

    void setUp(int minIsr, int64_t fetchIntervalNs)
    {
        tmpPath_  = makeBenchTempDir("rune_gr8_");
        dataPath_ = makeBenchTempDir("rune_gr8_data_");
        foll1LogDir_ = (makeBenchTempDir("rune_gr8_f1_")).string();
        foll2LogDir_ = (makeBenchTempDir("rune_gr8_f2_")).string();

        fs::create_directories(tmpPath_);
        fs::create_directories(dataPath_);
        fs::create_directories(foll1LogDir_);
        fs::create_directories(foll2LogDir_);

        if (!startDriver()) return;
        if (!connectAeron()) return;
        if (!buildGatewayStack(minIsr, fetchIntervalNs)) return;
        if (!makeClientEndpoints()) return;
        if (!doConnect()) return;
        setupOk_ = true;
    }

    void tearDown()
    {
        ingressPub_.reset();
        egressSub_.reset();
        egressAsm_.reset();
        gw_.reset();
        // ReplicaRegistry destructor releases PartitionReplica unique_ptrs.
        foll1Log_.reset();
        foll2Log_.reset();
        registry_.reset();
        aeron_.reset();
        stopDriver();
        std::error_code ec;
        fs::remove_all(tmpPath_, ec);
        fs::remove_all(dataPath_, ec);
        fs::remove_all(foll1LogDir_, ec);
        fs::remove_all(foll2LogDir_, ec);
    }

    // -----------------------------------------------------------------------
    // offerBounded — tight-poll via yield (no sleep in measured path).
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
    // stepReplication — one replication round-trip for follower-fetch progress.
    // Calls follower doWork() then pumps the transport.  Used in the single-
    // threaded acks=All warm-up path (not the timed section).
    // -----------------------------------------------------------------------
    void stepReplication(int64_t clkNs)
    {
        if (follReplica1_) follReplica1_->doWork(clkNs);
        if (follReplica2_) follReplica2_->doWork(clkNs);
        transport_.pump(clkNs);
        transport_.pump(clkNs);
    }

private:
    // -----------------------------------------------------------------------
    // Driver lifecycle
    // -----------------------------------------------------------------------
    bool startDriver()
    {
        const std::string aeronDir = (tmpPath_ / "aeron").string();

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 1024 * 1024);

        if (aeron_driver_init(&driver_, driverCtx_) != 0 ||
            aeron_driver_start(driver_, /*manual_main_loop=*/true) != 0) {
            return false;
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
        return fs::exists(cncPath);
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

    bool connectAeron()
    {
        const std::string aeronDir = (tmpPath_ / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        return !!aeron_;
    }

    // -----------------------------------------------------------------------
    // Build TopicRegistry + ClientGateway + PartitionReplicas
    // -----------------------------------------------------------------------
    bool buildGatewayStack(int minIsr, int64_t fetchIntervalNs)
    {
        // Topic registry
        TopicSpec ts;
        ts.name           = "bench";
        ts.partitionCount = 1;
        ts.topicId        = kTopicId;

        try {
            registry_ = std::make_unique<rune::gateway::TopicRegistry>(
                std::vector<TopicSpec>{ts}, dataPath_.string());
        } catch (...) { return false; }

        // Get the leader's Log pointer from TopicRegistry.
        rune::storage::Log* leaderLog = registry_->log(kTopicId, kPartition);
        if (!leaderLog) return false;

        // Build follower Logs.
        try {
            foll1Log_ = std::make_unique<rune::storage::Log>(foll1LogDir_);
            foll2Log_ = std::make_unique<rune::storage::Log>(foll2LogDir_);
        } catch (...) { return false; }

        // Replica IDs
        constexpr NodeId kLeaderId = 0;
        constexpr NodeId kFollId1  = 1;
        constexpr NodeId kFollId2  = 2;

        const std::vector<NodeId> allNodes = {kLeaderId, kFollId1, kFollId2};
        const std::vector<NodeId> isr      = {kLeaderId, kFollId1, kFollId2};

        PartitionConfig leaderCfg;
        leaderCfg.partitionId          = kPartition;
        leaderCfg.selfId               = kLeaderId;
        leaderCfg.replicaLagMaxMs      = 60'000;    // don't shrink ISR during bench
        leaderCfg.replicaLagMaxOffset  = 10'000'000;
        leaderCfg.minIsr               = minIsr;
        leaderCfg.fetchIntervalNs      = fetchIntervalNs; // leader uses this for ISR check cadence
        leaderCfg.isrCheckIntervalNs   = 60'000'000'000LL; // suppress ISR shrink
        leaderCfg.maxFetchBytes        = 1 << 20;

        PartitionConfig follCfg;
        follCfg.partitionId          = kPartition;
        follCfg.selfId               = kFollId1; // overridden per-follower below
        follCfg.replicaLagMaxMs      = 60'000;
        follCfg.replicaLagMaxOffset  = 10'000'000;
        follCfg.minIsr               = minIsr;
        follCfg.fetchIntervalNs      = fetchIntervalNs; // THE variable we're tuning
        follCfg.isrCheckIntervalNs   = 60'000'000'000LL;
        follCfg.maxFetchBytes        = 1 << 20;

        auto leaderReplica = std::make_unique<PartitionReplica>(
            leaderCfg, Role::Leader, /*epoch=*/1,
            allNodes, isr, leaderLog, &transport_);

        PartitionConfig foll1Cfg = follCfg;
        foll1Cfg.selfId = kFollId1;
        auto follReplica1 = std::make_unique<PartitionReplica>(
            foll1Cfg, Role::Follower, /*epoch=*/1,
            allNodes, isr, foll1Log_.get(), &transport_);

        PartitionConfig foll2Cfg = follCfg;
        foll2Cfg.selfId = kFollId2;
        auto follReplica2 = std::make_unique<PartitionReplica>(
            foll2Cfg, Role::Follower, /*epoch=*/1,
            allNodes, isr, foll2Log_.get(), &transport_);

        // Save raw pointers before moving.
        leaderReplica_ = leaderReplica.get();
        follReplica1_  = follReplica1.get();
        follReplica2_  = follReplica2.get();

        // Wire transport.
        transport_.setLeader(leaderReplica_);
        transport_.addFollower(kFollId1, follReplica1_);
        transport_.addFollower(kFollId2, follReplica2_);

        // Register in ReplicaRegistry (so gateway can find it via setReplicaRegistry).
        replicaRegistry_.add(kTopicId, kPartition, std::move(leaderReplica));
        // Followers are transport-internal; not registered in the gateway registry
        // (gateway only needs the leader replica for GR-4/GR-5).
        // We keep follReplica1/2 in separate unique_ptrs below.
        follReplica1Owned_ = std::move(follReplica1);
        follReplica2Owned_ = std::move(follReplica2);

        // Build ClientGateway.
        try {
            gw_ = std::make_unique<rune::gateway::ClientGateway>(
                aeron_, "aeron:ipc", kIngressStream, kThisNodeId);
            gw_->setTopicRegistry(registry_.get());
            gw_->setReplicaRegistry(&replicaRegistry_);
            gw_->setMinIsr(minIsr);
        } catch (...) { return false; }

        return true;
    }

    bool makeClientEndpoints()
    {
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription("aeron:ipc", kEgressStream);

        bool pubOk = false, subOk = false;
        for (int i = 0; i < kPumpCapSetup && !(pubOk && subOk); ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (!pubOk) { ingressPub_ = aeron_->findPublication(pubRegId);  pubOk = !!ingressPub_; }
            if (!subOk) { egressSub_  = aeron_->findSubscription(subRegId); subOk = !!egressSub_; }
            if (!(pubOk && subOk)) std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!pubOk || !subOk) return false;

        for (int i = 0; i < kPumpCapSetup; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (ingressPub_->isConnected()) break;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        if (!ingressPub_->isConnected()) return false;

        egressAsm_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t             offset,
                   aeron::util::index_t             len,
                   aeron::Header&)
            {
                const auto* ptr = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                const auto  sz  = static_cast<std::size_t>(len);
                auto dec = pc::ProduceResponse::decode(ptr, sz);
                if (dec) {
                    lastProdCorrId_.store(dec->correlationId,
                                          std::memory_order_release);
                }
            });
        return true;
    }

    bool doConnect()
    {
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = 1;
        req.egressStreamId = kEgressStream;
        req.clientVersion  = 1;
        req.egressChannel  = "aeron:ipc";
        const std::size_t reqLen = req.encode(scratch_.data());

        if (!offerBounded(*ingressPub_, scratch_.data(), reqLen)) return false;

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
            resp->assignedSessionId < 1) return false;

        sessionId_ = resp->assignedSessionId;
        return true;
    }

    // Follower replica ownership (not in registry — just kept alive here).
    std::unique_ptr<PartitionReplica> follReplica1Owned_;
    std::unique_ptr<PartitionReplica> follReplica2Owned_;
};

// ===========================================================================
// BM_AcksLeaderProduceLatency — HARD GATE: p99 < 100 µs.
//
// Full-stack gateway produce WITH GR-4 replication wired (leader replica,
// minIsr=1 so HW advances immediately on leader-only ISR, no follower fetch
// needed).  acks=Leader: ProduceResponse sent immediately after append + GR-4
// onLeaderLogAppended().  Verifies GR-1..7 replication wiring did NOT slow down
// the acks=Leader hot path vs GB-5 baseline (~4.8 µs p99).
//
// Setup: same IPC gateway as GatewayDataBench but with ReplicaRegistry wired.
// ===========================================================================
class AcksLeaderFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override
    {
        base_.setUp(/*minIsr=*/1, /*fetchIntervalNs=*/5'000'000LL);
        if (!base_.setupOk_) {
            state.SkipWithError("AcksLeaderFixture: setup failed");
        }
    }

    void TearDown(::benchmark::State& /*state*/) override
    {
        base_.tearDown();
    }

    GR8BaseFixture base_;
};

BENCHMARK_DEFINE_F(AcksLeaderFixture, Produce)(benchmark::State& state)
{
    if (state.skipped() || !base_.setupOk_) return;

    auto batchData = makeOneBatch(0);
    const std::size_t batchSize = batchData.size();

    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = base_.sessionId_;
    prod.correlationId  = 0;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);
    prod.acks           = 0; // acks=Leader (not acks=All)

    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up
    for (int w = 0; w < kWarmupIter; ++w) {
        const int64_t corrId = -(w + 1);
        prod.correlationId = corrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        base_.lastProdCorrId_.store(-1, std::memory_order_release);

        bool sent = false;
        for (int i = 0; i < kPumpCapSetup && !sent; ++i) {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            if (base_.ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0)
                sent = true;
            else
                std::this_thread::yield();
        }
        if (!sent) { state.SkipWithError("warm-up offer timed out"); return; }

        for (int i = 0; i < kPumpCapSetup; ++i) {
            base_.gw_->doWork(nowNs());
            base_.egressSub_->poll(base_.egressAsm_->handler(), 16);
            if (base_.lastProdCorrId_.load(std::memory_order_acquire) == corrId) break;
            std::this_thread::yield();
        }
    }

    // Timed iterations
    int64_t corrId = 1;
    for (auto _ : state) {
        const int64_t thisCorrId = corrId++;
        prod.correlationId = thisCorrId;
        const std::size_t encLen = prod.encode(produceBuf.data());

        base_.lastProdCorrId_.store(-1, std::memory_order_release);

        {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (base_.ingressPub_->offer(ab, 0,
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
            base_.gw_->doWork(nowNs());
            base_.egressSub_->poll(base_.egressAsm_->handler(), 16);
            if (base_.lastProdCorrId_.load(std::memory_order_acquire) == thisCorrId) {
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
        double sum = 0.0;
        for (double v : samples) sum += v;
        const std::size_t n = samples.size();
        state.counters["acksleader_p50_us"]  = pct(samples, 0.50);
        state.counters["acksleader_p99_us"]  = pct(samples, 0.99);
        state.counters["acksleader_p999_us"] = pct(samples, 0.999);
        state.counters["acksleader_mean_us"] = sum / static_cast<double>(n);
        state.counters["acksleader_gate_pass"] = (pct(samples, 0.99) < 100.0) ? 1.0 : 0.0;
    }
}

BENCHMARK_REGISTER_F(AcksLeaderFixture, Produce)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// AcksAllFixture — full-stack acks=All gateway bench with variable fetch cadence.
//
// A background duty-cycle thread pumps follower doWork() at fetchIntervalNs.
// The main thread drives gateway.doWork() tight-poll (no sleep).
// After each follower cycle the leader HW advances; drainPendingAcks() fires in
// the next gateway.doWork() call and sends ProduceResponse.
//
// The fixture is parameterised by fetchIntervalNs (passed via state.range(0)).
// ===========================================================================
class AcksAllFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override
    {
        fetchIntervalNs_ = state.range(0);
        // minIsr=2 = majority in 3-node cluster.
        base_.setUp(/*minIsr=*/2, fetchIntervalNs_);
        if (!base_.setupOk_) {
            state.SkipWithError("AcksAllFixture: setup failed");
            return;
        }
        startReplThread();
    }

    void TearDown(::benchmark::State& /*state*/) override
    {
        stopReplThread();
        base_.tearDown();
    }

    GR8BaseFixture base_;

private:
    int64_t fetchIntervalNs_ = 5'000'000LL;

    std::atomic<bool>  replRunning_{false};
    std::thread        replThread_;

    // The replication duty-cycle thread: pumps follower doWork() at fetchIntervalNs.
    // This is the key driver of acks=All latency — follower polls determine
    // when HW advances and drainPendingAcks() fires.
    void startReplThread()
    {
        replRunning_.store(true, std::memory_order_release);
        replThread_ = std::thread([this]() {
            while (replRunning_.load(std::memory_order_acquire)) {
                const int64_t t0 = nowNs();

                // Pump follower fetch cycle.
                if (base_.follReplica1_) base_.follReplica1_->doWork(t0);
                if (base_.follReplica2_) base_.follReplica2_->doWork(t0);
                base_.transport_.pump(t0);
                base_.transport_.pump(t0);

                // Busy-wait for remainder of interval (no sleep → tight cadence).
                const int64_t deadline = t0 + fetchIntervalNs_;
                while (nowNs() < deadline) {
                    std::this_thread::yield();
                }
            }
        });
    }

    void stopReplThread()
    {
        replRunning_.store(false, std::memory_order_release);
        if (replThread_.joinable()) replThread_.join();
    }
};

BENCHMARK_DEFINE_F(AcksAllFixture, ProduceAcksAll)(benchmark::State& state)
{
    if (state.skipped() || !base_.setupOk_) return;

    auto batchData = makeOneBatch(0);
    const std::size_t batchSize = batchData.size();

    std::array<uint8_t, kScratchSize> produceBuf{};
    pc::Produce prod;
    prod.sessionId      = base_.sessionId_;
    prod.correlationId  = 0;
    prod.topicId        = kTopicId;
    prod.partition      = kPartition;
    prod.partitionKnown = 1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchData.data(), batchSize);
    prod.acks           = 1; // acks=All (GR-5 deferred path)

    static constexpr std::size_t kMaxSamples = 100'000UZ;
    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up: send a few acks=All produces and wait for responses.
    // Single-threaded here (repl thread already running), so tight-poll is fine.
    for (int w = 0; w < kWarmupIter; ++w) {
        const int64_t corrId = -(w + 1);
        prod.correlationId = corrId;
        prod.acks = 1;
        const std::size_t encLen = prod.encode(produceBuf.data());

        base_.lastProdCorrId_.store(-2, std::memory_order_release);

        bool sent = false;
        for (int i = 0; i < kPumpCapSetup && !sent; ++i) {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            if (base_.ingressPub_->offer(ab, 0, static_cast<aeron::util::index_t>(encLen)) > 0)
                sent = true;
            else
                std::this_thread::yield();
        }
        if (!sent) { state.SkipWithError("warm-up acks=All offer timed out"); return; }

        // Drive gateway tight-poll until response (replication thread pumps followers).
        // Timeout: 60 seconds (30× the default fetch interval).
        const int64_t timeoutDeadline = nowNs() + 60'000'000'000LL;
        bool got = false;
        while (nowNs() < timeoutDeadline) {
            base_.gw_->doWork(nowNs());
            base_.egressSub_->poll(base_.egressAsm_->handler(), 16);
            if (base_.lastProdCorrId_.load(std::memory_order_acquire) == corrId) {
                got = true;
                break;
            }
            std::this_thread::yield();
        }
        if (!got) { state.SkipWithError("warm-up acks=All response timed out"); return; }
    }

    // Timed iterations — one acks=All produce per iteration.
    int64_t corrId = 100'000;
    for (auto _ : state) {
        const int64_t thisCorrId = corrId++;
        prod.correlationId = thisCorrId;
        prod.acks = 1;
        const std::size_t encLen = prod.encode(produceBuf.data());

        base_.lastProdCorrId_.store(-2, std::memory_order_release);

        // Offer — tight-poll.
        {
            aeron::concurrent::AtomicBuffer ab(
                produceBuf.data(), static_cast<aeron::util::index_t>(encLen));
            bool sent = false;
            for (int i = 0; i < kPumpCapMeasure && !sent; ++i) {
                if (base_.ingressPub_->offer(ab, 0,
                        static_cast<aeron::util::index_t>(encLen)) > 0)
                    sent = true;
                else
                    std::this_thread::yield();
            }
            if (!sent) { state.SkipWithError("acks=All Produce offer timed out"); return; }
        }

        const int64_t t0 = nowNs();

        // Tight-poll gateway until ProduceResponse arrives.
        // Replication thread is advancing HW in the background.
        const int64_t timeoutDeadline = t0 + 30'000'000'000LL; // 30 s hard cap
        bool got = false;
        while (nowNs() < timeoutDeadline) {
            base_.gw_->doWork(nowNs());
            base_.egressSub_->poll(base_.egressAsm_->handler(), 16);
            if (base_.lastProdCorrId_.load(std::memory_order_acquire) == thisCorrId) {
                got = true;
                break;
            }
            std::this_thread::yield();
        }
        const int64_t elapsed = nowNs() - t0;

        if (!got) { state.SkipWithError("acks=All ProduceResponse timed out"); return; }

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        double sum = 0.0;
        for (double v : samples) sum += v;
        const std::size_t n = samples.size();
        state.counters["acksall_p50_us"]  = pct(samples, 0.50);
        state.counters["acksall_p99_us"]  = pct(samples, 0.99);
        state.counters["acksall_p999_us"] = pct(samples, 0.999);
        state.counters["acksall_mean_us"] = sum / static_cast<double>(n);
        state.counters["fetchInterval_us"] =
            static_cast<double>(state.range(0)) / 1'000.0;
    }
}

// Register acks=All bench with three fetch cadences (5 ms, 1 ms, 200 µs).
BENCHMARK_REGISTER_F(AcksAllFixture, ProduceAcksAll)
    ->UseManualTime()
    ->MinTime(1.0)    // run for at least 1 s per cadence point (acks=All is slow)
    ->Unit(benchmark::kMillisecond)
    ->Arg(5'000'000LL)      // 5 ms (default)
    ->Arg(1'000'000LL)      // 1 ms
    ->Arg(  200'000LL);     // 200 µs

// ===========================================================================
// Push-on-append recommendation summary (no implementation — GR-9 scope):
//
// The acks=All latency measured above is fundamentally bounded below by:
//   latency_floor = fetchIntervalNs + 2× Aeron IPC RTT (follower→leader→follower)
//
// With default 5 ms fetchIntervalNs → p50 ≈ 2.5 ms, p99 ≈ 5 ms.
// With 1 ms cadence → p50 ≈ 0.5 ms, p99 ≈ 1 ms.
// With 200 µs cadence → p50 ≈ 100 µs, p99 ≈ 200 µs.
//
// PUSH-ON-APPEND would eliminate the fetch-interval floor:
//   On leader.append(), instead of waiting for followers to poll,
//   the leader immediately pushes the new batch via sendFetchResponse()
//   (proactive push).  The follower applies it and the HW can advance
//   within one IPC RTT (~1–3 µs on loopback).  This would bring
//   acks=All latency into single-digit µs territory.
//
// RECOMMENDATION: push-on-append IS worth building if sub-100 µs acks=All
// is required (e.g. for strong-durability workloads that must match
// acks=Leader latency).  For the current Rune use-cases where acks=All is
// a durability mode (not a latency mode), 200 µs cadence is acceptable
// and push-on-append is deferred to a future GR-9 ticket.
//
// The acks=Leader path remains the recommended path for latency-sensitive
// clients; acks=All is for strong-durability workloads that accept ms-class
// latency (or sub-ms with tight cadence tuning).
// ===========================================================================
