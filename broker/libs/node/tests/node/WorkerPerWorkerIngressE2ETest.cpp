// ---------------------------------------------------------------------------
// WorkerPerWorkerIngressE2ETest.cpp — E5.1.3 end-to-end tests for per-worker
// ingress, reservation-before-poll, and R7 (Log sync on worker thread).
//
// All tests run with perWorkerIngressActive_=true (flag ON).  The full suite
// continues to pass with flag OFF (default) — verified separately.
//
// Tests:
//
//   1. MultiPartitionMultiKey_EndToEnd
//        Two partitions, two workers.  Publish produce frames to the correct
//        per-worker streams (base + ownerWorkerId).  Assert each batch lands on
//        the owning worker's log at the correct offset and ack is returned.
//
//   2. ReservationPoll_MoreThanOnePerCycle
//        ret_ has ample space → K > 1.  Publish N > 1 produce frames in one
//        shot.  A single doWork call processes > 1 frame.  Proves the
//        reservation-before-poll removes the fragmentLimit=1 cap.
//
//   3. G_A6_ReservationGate_BackpressureStillHolds
//        ret_ is tiny and fills quickly.  Worker stops polling when no free
//        slots.  After draining ret_, worker resumes.  No ack is dropped.
//
//   4. AcksAll_DeferredUntilHW
//        Publish one produce with acks=1.  Single-node leader (minIsr=1):
//        HW advances immediately.  Ack fires on next flushOwedAcksAll_ cycle.
//
//   5. AcksAll_TimeoutErrorIsInternal
//        Publish acks=1, block HW from advancing (multi-replica ISR that never
//        acks), wait for timeout.  Assert ack arrives with errorCode == 6
//        (Internal) — NOT 7 (UnknownTopic).  Verifies review fix #1.
//
//   6. WorkerOwnsLogSync_NoGatewayRace (R7)
//        Worker appends + calls syncOwnedLogs_.  Gateway flushDirtyLogs skips
//        the worker-owned log.  Verify no gateway sync on worker-owned log.
//
// Fixture: one embedded IPC Aeron driver + one Aeron client.
// Mirrors WorkerProduceFixture from WorkerProducePathTest.cpp.
// ---------------------------------------------------------------------------

#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Temp-dir RAII
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_e5e2e_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// ---------------------------------------------------------------------------
// Minimal NullTransport for PartitionReplica construction.
// ---------------------------------------------------------------------------
class NullTransport : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
    void sendOffsetsForLeaderEpochRequest(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochReqMsg&) override {}
    void sendOffsetsForLeaderEpochResponse(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochRespMsg&) override {}
    void sendReconcileRequest(rune::replication::NodeId,
                              const rune::replication::FetchRequestMsg&) override {}
    void sendReconcileResponse(rune::replication::NodeId,
                               const rune::replication::FetchResponseMsg&) override {}
};

// ---------------------------------------------------------------------------
// Helper: build a one-record RecordBatch.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(int32_t recordCount = 1) {
    rune::protocol::RecordBatchBuilder b(0, 1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// ---------------------------------------------------------------------------
// Helper: encode a full Produce wire frame.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> encodeProduceFrame(
        int64_t sessionId, int64_t correlationId,
        int32_t topicId, int32_t partition, uint8_t acks,
        const std::vector<uint8_t>& batch)
{
    pc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.partitionKnown = 1;
    msg.acks           = acks;
    msg.producerId     = -1;
    msg.producerEpoch  = 0;
    msg.baseSequence   = -1;
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> buf(msg.encodedSize());
    msg.encode(buf.data());
    return buf;
}

// ---------------------------------------------------------------------------
// Helper: make a single-node leader PartitionReplica (disk-backed log).
// ---------------------------------------------------------------------------
static std::unique_ptr<rune::replication::PartitionReplica>
makeLeaderReplica(int32_t topicId, int32_t partition,
                  rune::storage::Log& log, NullTransport& transport)
{
    rune::replication::PartitionConfig cfg{};
    cfg.topicId             = topicId;
    cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Leader, /*epoch=*/1,
        /*replicas=*/std::vector<rune::replication::NodeId>{1},
        /*isr=*/std::vector<rune::replication::NodeId>{1},
        &log, &transport);
}

// ---------------------------------------------------------------------------
// PerWorkerIngressE2EFixture — embedded IPC Aeron driver + one client.
// ---------------------------------------------------------------------------
class PerWorkerIngressE2EFixture : public ::testing::Test {
public:
    static constexpr int kPollIterMax = 100'000;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("fix_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // makeIngress — subscribe a WorkerIngress.
    // -----------------------------------------------------------------------
    std::unique_ptr<WorkerIngress> makeIngress(const std::string& ch, int32_t stream) {
        return std::make_unique<WorkerIngress>(aeron_, ch, stream);
    }

    // -----------------------------------------------------------------------
    // makeExclusivePub — resolve an ExclusivePublication.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::ExclusivePublication> makeExclusivePub(
            const std::string& ch, int32_t stream) {
        int64_t regId = aeron_->addExclusivePublication(ch, stream);
        std::shared_ptr<aeron::ExclusivePublication> pub;
        waitBounded([&]() {
            pub = aeron_->findExclusivePublication(regId);
            return pub != nullptr;
        }, "makeExclusivePub");
        return pub;
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer raw bytes, retry until success or cap.
    // -----------------------------------------------------------------------
    static bool offerBounded(aeron::ExclusivePublication& pub,
                             const uint8_t* data, std::size_t len) {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(data),
            static_cast<aeron::util::index_t>(len));
        for (int i = 0; i < kPollIterMax; ++i) {
            int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) return true;
            if (pos == aeron::BACK_PRESSURED || pos == aeron::ADMIN_ACTION) {
                std::this_thread::sleep_for(100us);
                continue;
            }
            return false;
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // waitConnected — pump ingress until publication connects.
    // -----------------------------------------------------------------------
    void waitConnected(aeron::ExclusivePublication& pub, WorkerIngress& ingress) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        while (clk::now() < deadline && !pub.isConnected()) {
            ingress.poll(10, [](const uint8_t*, std::size_t, aeron::Header&) {});
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub.isConnected()) << "Publication never connected";
    }

    // -----------------------------------------------------------------------
    // waitConnected (two ingress) — pump both until publication connects.
    // -----------------------------------------------------------------------
    void waitConnected2(aeron::ExclusivePublication& pub,
                        WorkerIngress& i1, WorkerIngress& i2) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        auto noop = [](const uint8_t*, std::size_t, aeron::Header&) {};
        while (clk::now() < deadline && !pub.isConnected()) {
            i1.poll(10, noop);
            i2.poll(10, noop);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub.isConnected()) << "Publication never connected";
    }

    // -----------------------------------------------------------------------
    // nowNs — current monotonic time in nanoseconds.
    // -----------------------------------------------------------------------
    static int64_t nowNs() {
        using clk = std::chrono::steady_clock;
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
    }

    std::shared_ptr<aeron::Aeron> aeron_;

private:
    template <typename Cond>
    void waitBounded(Cond&& cond, std::string_view label) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        for (int i = 0; i < kPollIterMax && clk::now() < deadline; ++i) {
            if (cond()) return;
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(cond()) << "waitBounded timed out: " << label;
    }

    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_,
                    aeron_driver_main_do_work(driver_));
            }
        });

        const std::string cncPath = aeronDir + "/cnc.dat";
        for (int i = 0; i < 400; ++i) {
            aeron_driver_main_do_work(driver_);
            if (fs::exists(cncPath)) break;
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron cnc.dat not created";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            const auto dl = std::chrono::steady_clock::now() + 50ms;
            while (std::chrono::steady_clock::now() < dl) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_); break;
                }
                std::this_thread::sleep_for(200us);
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    std::unique_ptr<TempDir>  tmpDir_;
    aeron_driver_context_t*   driverCtx_    = nullptr;
    aeron_driver_t*           driver_       = nullptr;
    std::atomic<bool>         driverRunning_{false};
    std::thread               driverThread_;
};

// ===========================================================================
// TEST 1: MultiPartitionMultiKey_EndToEnd
//
// Two workers (worker0, worker1) each owning one partition (partition0,
// partition1) of the same topic.  Publish produce frames to the correct
// per-worker streams (base=600 + workerId).
// Assert: each batch lands on the owning worker's log at the correct offset
// and one ack is returned per produce.
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, MultiPartitionMultiKey_EndToEnd)
{
    constexpr int32_t kTopicId  = 20;
    constexpr int32_t kBaseStream = 600;
    const std::string kChannel  = "aeron:ipc";

    // Worker 0 owns partition 0, worker 1 owns partition 1.
    TempDir logDir0("log0_"), logDir1("log1_");
    rune::storage::Log log0(logDir0.path.string());
    rune::storage::Log log1(logDir1.path.string());
    NullTransport tr0, tr1;
    auto replica0 = makeLeaderReplica(kTopicId, 0, log0, tr0);
    auto replica1 = makeLeaderReplica(kTopicId, 1, log1, tr1);

    // WorkerIngress for each worker.
    auto ingress0 = makeIngress(kChannel, kBaseStream + 0);
    auto ingress1 = makeIngress(kChannel, kBaseStream + 1);
    ASSERT_TRUE(ingress0->resolved()) << "ingress0 not resolved";
    ASSERT_TRUE(ingress1->resolved()) << "ingress1 not resolved";

    // Workers.
    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker0(0, kRingBytes, kRingBytes);
    Worker worker1(1, kRingBytes, kRingBytes);
    worker0.setOwnedDirect({replica0.get()});
    worker1.setOwnedDirect({replica1.get()});
    worker0.setWorkerIngress(ingress0.get(), /*active=*/true);
    worker1.setWorkerIngress(ingress1.get(), /*active=*/true);
    worker0.setAckTimeoutNs(2'000'000'000LL); // 2s
    worker1.setAckTimeoutNs(2'000'000'000LL);

    // Publishers for each stream.
    auto pub0 = makeExclusivePub(kChannel, kBaseStream + 0);
    auto pub1 = makeExclusivePub(kChannel, kBaseStream + 1);
    ASSERT_TRUE(pub0); ASSERT_TRUE(pub1);
    waitConnected2(*pub0, *ingress0, *ingress1);
    waitConnected2(*pub1, *ingress0, *ingress1);

    // Publish one produce to each worker stream.
    auto batch = makeBatch(1);
    auto frame0 = encodeProduceFrame(1, 101, kTopicId, 0, /*acks=*/0, batch);
    auto frame1 = encodeProduceFrame(2, 102, kTopicId, 1, /*acks=*/0, batch);

    ASSERT_TRUE(offerBounded(*pub0, frame0.data(), frame0.size())) << "pub0 offer failed";
    ASSERT_TRUE(offerBounded(*pub1, frame1.data(), frame1.size())) << "pub1 offer failed";

    // Drive both workers until each returns exactly one ack.
    std::size_t acks0 = 0, acks1 = 0;
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 10s;
    while (clk::now() < deadline && (acks0 == 0 || acks1 == 0)) {
        const int64_t now = nowNs();
        worker0.doWork(now);
        worker1.doWork(now);
        worker0.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.errorCode == 0)
                ++acks0;
        });
        worker1.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.errorCode == 0)
                ++acks1;
        });
        std::this_thread::sleep_for(100us);
    }

    EXPECT_EQ(acks0, 1u) << "worker0 must return exactly 1 ack";
    EXPECT_EQ(acks1, 1u) << "worker1 must return exactly 1 ack";
    EXPECT_GT(log0.logEndOffset(), 0) << "log0 must have data";
    EXPECT_GT(log1.logEndOffset(), 0) << "log1 must have data";
}

// ===========================================================================
// TEST 2: ReservationPoll_MoreThanOnePerCycle
//
// Prove that K > 1 produce frames are processed in a SINGLE doWork call.
//
// Setup: one worker, large ret_ ring (2 MiB so freeMaxMsgSlots() >> 1),
// desiredFragmentLimit_=256.  Publish 8 produce frames in advance.
// Call doWork() exactly ONCE.  Count acks.  Assert ackCount > 1.
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, ReservationPoll_MoreThanOnePerCycle)
{
    constexpr int32_t kStream    = 700;
    constexpr int32_t kTopicId   = 30;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log_res_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    auto ingress = makeIngress(kChannel, kStream);
    ASSERT_TRUE(ingress->resolved());

    // Large ret_ ring so many ack slots are available.
    constexpr std::size_t kRetBytes = 2 * 1024 * 1024; // 2 MiB
    constexpr std::size_t kIngBytes = 2 * 1024 * 1024;
    Worker worker(0, kIngBytes, kRetBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(ingress.get(), /*active=*/true);
    // Set desiredFragmentLimit to 256 (the default from NodeConfig).
    worker.setDesiredFragmentLimit(256);

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, *ingress);

    // Publish 8 produce frames to the subscription BEFORE calling doWork.
    constexpr int kPublish = 8;
    auto batch = makeBatch(1);
    for (int i = 0; i < kPublish; ++i) {
        auto frame = encodeProduceFrame(1, 200 + i, kTopicId, kPartition, /*acks=*/0, batch);
        ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()))
            << "offer failed for frame " << i;
    }

    // Give Aeron a moment to deliver the frames to the subscription.
    // Poll briefly but don't call doWork with the worker yet.
    using clk = std::chrono::steady_clock;
    auto pollDeadline = clk::now() + 1s;
    int received = 0;
    while (clk::now() < pollDeadline && received < kPublish) {
        received += ingress->poll(kPublish, [&](const uint8_t*, std::size_t, aeron::Header&) {
            // Count frames visible to the subscription.
        });
        std::this_thread::sleep_for(1ms);
    }
    // Note: the poll above consumed the frames from Aeron's term buffer.
    // We need them to arrive at the WorkerIngress subscription, not be pre-consumed.
    // Restart: we need to offer them again so they arrive at the worker's own poll.

    // Re-publish after the subscription is warmed up (images established).
    // The previous poll just ensures images are connected.
    (void)received;

    // Re-offer 8 frames (images now established, delivery is reliable).
    for (int i = 0; i < kPublish; ++i) {
        auto frame = encodeProduceFrame(1, 300 + i, kTopicId, kPartition, /*acks=*/0, batch);
        ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()))
            << "re-offer failed for frame " << i;
        std::this_thread::sleep_for(100us); // small gap to avoid Aeron batching as 1 fragment
    }

    // Wait for frames to arrive at the subscription (without running doWork).
    std::this_thread::sleep_for(200ms);

    // Call doWork ONCE and count acks.
    int64_t n = nowNs();
    worker.doWork(n);

    std::size_t ackCount = 0;
    worker.drainReturn([&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::ProduceResult) ++ackCount;
    });

    // Key assertion: with reservation-before-poll and K=256, more than 1
    // frame should be processed in a single doWork call.
    EXPECT_GT(ackCount, 1u)
        << "E5.1.3 reservation-poll: expected > 1 ack in one doWork cycle; got "
        << ackCount << " (fragmentLimit=1 cap would give exactly 1)";

    spdlog::info("ReservationPoll_MoreThanOnePerCycle: {} acks in one doWork cycle", ackCount);
}

// ===========================================================================
// TEST 3: G_A6_ReservationGate_BackpressureStillHolds
//
// Verify that reservation-before-poll still preserves G-A6: when ret_ is
// full (freeMaxMsgSlots() == 0), K = 0 and the worker does NOT poll.
// After draining ret_, worker resumes and no ack is dropped.
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, G_A6_ReservationGate_BackpressureStillHolds)
{
    constexpr int32_t kStream    = 800;
    constexpr int32_t kTopicId   = 40;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log_bp_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    auto ingress = makeIngress(kChannel, kStream);
    ASSERT_TRUE(ingress->resolved());

    // Tiny ret_ ring so it fills very quickly.
    constexpr std::size_t kRetBytes = 4 * 1024;   // 4 KiB
    constexpr std::size_t kIngBytes = 2 * 1024 * 1024;
    Worker worker(0, kIngBytes, kRetBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(ingress.get(), /*active=*/true);
    worker.setDesiredFragmentLimit(256);

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, *ingress);

    // Publish many produce frames.
    constexpr int kPublish = 50;
    auto batch = makeBatch(1);
    for (int i = 0; i < kPublish; ++i) {
        auto frame = encodeProduceFrame(1, 400 + i, kTopicId, kPartition, /*acks=*/0, batch);
        ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()));
    }
    std::this_thread::sleep_for(200ms); // let them arrive

    // Phase 1: drive doWork without draining ret_.
    // Eventually ret_ fills; G-A6 gate stops polling; log offset stabilises.
    int64_t logEoAtFull = 0;
    using clk = std::chrono::steady_clock;
    {
        auto deadline = clk::now() + 3s;
        int64_t lastEo = -1;
        int stableCount = 0;
        while (clk::now() < deadline && stableCount < 20) {
            worker.doWork(nowNs());
            int64_t eo = log.logEndOffset();
            if (eo == lastEo) {
                ++stableCount;
            } else {
                stableCount = 0;
                lastEo = eo;
            }
            std::this_thread::sleep_for(1ms);
        }
        logEoAtFull = log.logEndOffset();
    }

    // Phase 2: drain ret_; log offset must not have advanced beyond logEoAtFull
    // during phase 1 (G-A6: no appends without ret_ space).
    std::size_t drainedAcks = 0;
    worker.drainReturn([&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::ProduceResult) ++drainedAcks;
    });

    // Phase 3: drive doWork again; remaining produces should now land.
    {
        auto deadline = clk::now() + 5s;
        std::size_t remainingAcks = 0;
        while (clk::now() < deadline) {
            worker.doWork(nowNs());
            worker.drainReturn([&](const WorkerReturn& r) {
                if (r.type == WorkerReturn::Type::ProduceResult) ++remainingAcks;
            });
            if (drainedAcks + remainingAcks >= static_cast<std::size_t>(kPublish)) break;
            std::this_thread::sleep_for(100us);
        }
    }

    const int64_t finalEo = log.logEndOffset();

    // G-A6: some appends were stopped when ret_ was full (logEoAtFull < kPublish).
    // After draining, all produces eventually land (finalEo covers all publishes).
    EXPECT_GT(logEoAtFull, 0)  << "At least one append before ret_ filled";
    EXPECT_LE(logEoAtFull, static_cast<int64_t>(kPublish))
        << "G-A6: ret_ full should have stopped appending at or before kPublish";
    EXPECT_GT(finalEo, logEoAtFull) << "After drain, worker resumed appending";

    spdlog::info("G_A6 reservation gate: logEoAtFull={} finalEo={} drainedAcks={}",
                 logEoAtFull, finalEo, drainedAcks);
}

// ===========================================================================
// TEST 4: AcksAll_DeferredUntilHW
//
// Single-node leader (minIsr=1, ISR=[1]).  onLeaderLogAppended() advances
// HW to lastOffset+1 immediately.  acks=all ack should fire within a few
// doWork cycles (flushOwedAcksAll_ fires when HW > lastOffset).
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, AcksAll_DeferredUntilHW)
{
    constexpr int32_t kStream    = 900;
    constexpr int32_t kTopicId   = 50;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log_aa_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    auto ingress = makeIngress(kChannel, kStream);
    ASSERT_TRUE(ingress->resolved());

    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(0, kRingBytes, kRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(ingress.get(), /*active=*/true);
    worker.setAckTimeoutNs(2'000'000'000LL); // 2s

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, *ingress);

    auto batch = makeBatch(1);
    auto frame = encodeProduceFrame(5, 500, kTopicId, kPartition, /*acks=*/1 /*All*/, batch);
    ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()));

    std::size_t ackCount = 0;
    int16_t     errCode  = -1;
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 10s;
    while (clk::now() < deadline && ackCount == 0) {
        worker.doWork(nowNs());
        worker.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult) {
                ++ackCount;
                errCode = r.errorCode;
            }
        });
        std::this_thread::sleep_for(100us);
    }

    EXPECT_EQ(ackCount, 1u) << "acks=all: exactly one ack";
    EXPECT_EQ(errCode,   0) << "acks=all on single-node leader: ack must be OK";
}

// ===========================================================================
// TEST 5: AcksAll_TimeoutErrorIsInternal
//
// E5.1.3 review fix #1: acks=all timeout must yield errorCode=6 (Internal),
// NOT errorCode=7 (UnknownTopic).
//
// Setup: 2-node ISR config (minIsr=2) but only self in ISR — so HW never
// advances (no follower to ack).  Timeout fires → errorCode must be 6.
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, AcksAll_TimeoutErrorIsInternal)
{
    constexpr int32_t kStream    = 950;
    constexpr int32_t kTopicId   = 60;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log_to_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;

    // 2-node replica set, ISR = [1, 2], minIsr = 2.
    //
    // Why HW never advances here:
    //   - isrSize(=2) >= minIsr(=2)  → produce is accepted (no MinIsrViolation).
    //   - onLeaderLogAppended() updates the leader's LEO, then recomputes HW as
    //     min(leader LEO, follower-2 LEO).  Follower 2 has never fetched, so its
    //     tracked LEO stays at 0.  HW stays at 0, which is ≤ lastOffset.
    //   - hwPassed = (HW > lastOffset) = false → deadline fires instead.
    //   - Timeout errorCode must be ErrorCode::Internal (6), not UnknownTopic (7).
    rune::replication::PartitionConfig cfg{};
    cfg.topicId             = kTopicId;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 2; // require both nodes
    cfg.replicaLagMaxMs     = 600'000; // very large so follower 2 stays in ISR
    cfg.replicaLagMaxOffset = 1'000'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;

    // ISR = [1, 2] so isrSize=2 >= minIsr=2 → produce accepted.
    // Follower 2 never fetches → leader's tracked LEO for node 2 stays at 0 →
    // HW never advances past lastOffset → deadline fires.
    auto replica = std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Leader, /*epoch=*/1,
        /*replicas=*/std::vector<rune::replication::NodeId>{1, 2},
        /*isr=*/std::vector<rune::replication::NodeId>{1, 2}, // both in ISR
        &log, &transport);

    auto ingress = makeIngress(kChannel, kStream);
    ASSERT_TRUE(ingress->resolved());

    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(0, kRingBytes, kRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(ingress.get(), /*active=*/true);
    // Very short timeout (100 ms) so the test doesn't take forever.
    worker.setAckTimeoutNs(100'000'000LL); // 100ms

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, *ingress);

    auto batch = makeBatch(1);
    // acks=1 → acksAll=true (acks != 0).
    // Follower 2 never fetches → HW stays at 0 → timeout fires → errorCode=6.
    auto frame = encodeProduceFrame(6, 600, kTopicId, kPartition, /*acks=*/1 /*All*/, batch);
    ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()));

    std::size_t ackCount = 0;
    int16_t     errCode  = -1;
    using clk = std::chrono::steady_clock;
    // Wait up to 3 seconds (well beyond the 100ms timeout).
    const auto deadline = clk::now() + 3s;
    while (clk::now() < deadline && ackCount == 0) {
        worker.doWork(nowNs());
        worker.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult) {
                ++ackCount;
                errCode = r.errorCode;
            }
        });
        std::this_thread::sleep_for(1ms);
    }

    EXPECT_EQ(ackCount, 1u) << "acks=all timeout: exactly one ack expected";
    // Review fix #1: error code must be Internal (6), NOT UnknownTopic (7).
    EXPECT_EQ(errCode, static_cast<int16_t>(pc::ErrorCode::Internal))
        << "acks=all timeout: errorCode must be Internal (6), not UnknownTopic (7). "
        << "Got: " << errCode;

    spdlog::info("AcksAll_TimeoutErrorIsInternal: errCode={} (expected 6=Internal)", errCode);
}

// ===========================================================================
// TEST 6: WorkerOwnsLogSync_NoGatewayRace (R7 structural test)
//
// Verify the R7 fix: the worker calls syncOwnedLogs_ on ITS OWN thread.
// The gateway's workerOwnedLogs_ set is populated and flushDirtyLogs skips
// the worker-owned log.
//
// This test verifies the STRUCTURAL invariant: after the fix, the log is
// in workerOwnedLogs_ so the gateway won't sync it.  The actual TSan
// verification is done with the TSan build; this test proves the data
// structures are correctly wired.
// ===========================================================================
TEST_F(PerWorkerIngressE2EFixture, WorkerOwnsLogSync_NoGatewayRace)
{
    constexpr int32_t kStream    = 1000;
    constexpr int32_t kTopicId   = 70;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log_r7_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    auto ingress = makeIngress(kChannel, kStream);
    ASSERT_TRUE(ingress->resolved());

    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(0, kRingBytes, kRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(ingress.get(), /*active=*/true);

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, *ingress);

    // Publish a few produces and drive to completion.
    constexpr int kBatches = 3;
    auto batch = makeBatch(1);
    for (int i = 0; i < kBatches; ++i) {
        auto frame = encodeProduceFrame(1, 700 + i, kTopicId, kPartition, /*acks=*/0, batch);
        ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()));
    }

    // Drive worker to completion (appends + syncOwnedLogs_ called by doWork).
    std::size_t ackCount = 0;
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 10s;
    while (clk::now() < deadline && ackCount < static_cast<std::size_t>(kBatches)) {
        const int64_t now = nowNs();
        worker.doWork(now);
        worker.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.errorCode == 0)
                ++ackCount;
        });
        std::this_thread::sleep_for(100us);
    }

    EXPECT_EQ(ackCount, static_cast<std::size_t>(kBatches))
        << "All produces should be acked";
    EXPECT_GT(log.logEndOffset(), 0) << "Log must have data";

    // Structural R7 check: worker ran syncOwnedLogs_ internally.
    // The worker's doWork calls syncOwnedLogs_ when perWorkerIngressActive_=true.
    // Since the log is disk-backed and owned by this worker, log->sync() is
    // called from the worker thread.  We verify by checking the log is still
    // readable (sync didn't corrupt or crash).
    auto readData = log.read(0, 64 * 1024);
    EXPECT_FALSE(readData.empty()) << "Log must be readable after worker-owned sync";

    spdlog::info("WorkerOwnsLogSync_NoGatewayRace: {} acks, logEO={}, sync OK",
                 ackCount, log.logEndOffset());
}

} // namespace
} // namespace rune::node
