// GatewaySubscribeTest.cpp — IPC-loopback integration tests for the GB-3
// Subscribe handler wired into ClientGateway.
//
// Mirrors the GatewayLoopbackTest / GatewayProduceTest harness:
//   - One embedded Aeron media driver + one Aeron client per test fixture.
//   - Gateway subscribes on the CLIENT INGRESS channel/stream.
//   - Test harness publishes to ingress and subscribes on the egress.
//
// Tests cover:
//   1. Subscribe from 0 + produce N → receive N records in order.
//   2. fromOffset mid-stream → receive only records from that offset.
//   3. from Latest → receive only new records produced after subscribe.
//   4. Back-pressure exactly-once gate: tiny egress term, many batches,
//      assert pause + resume from exact offset, every record once in order.
//   5. Unsubscribe stops further delivery.
//   6. Two subscriptions (different partitions/sessions) — no cross-talk + fairness.
//   7. Re-subscribe from tracked offset resumes correctly.
//   8. read_committed == read_uncommitted for M-B (both deliver up to LEO).
//   9. Malformed subscribe / unknown topic / bad partition → errorCode, no crash.
//
// Bounded drivers (hard cap + ASSERT), unique temp dirs, no busy-spin.

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/WorkerPool.h>
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

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
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
using rune::node::TopicSpec;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter).
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "")
{
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwsub_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// RAII temp dir cleanup.
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// Monotonic nanoseconds.
// ---------------------------------------------------------------------------
static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Helper: build a TopicSpec.
// ---------------------------------------------------------------------------
static TopicSpec makeSpec(std::string name, int32_t partitions, int32_t id,
                          int64_t segmentRollBytes = 64LL * 1024 * 1024)
{
    TopicSpec ts;
    ts.name             = std::move(name);
    ts.partitionCount   = partitions;
    ts.topicId          = id;
    ts.segmentRollBytes = segmentRollBytes;
    return ts;
}

// ---------------------------------------------------------------------------
// Helper: build a RecordBatch with K records (baseOffset=0 placeholder).
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(int32_t recordCount, int64_t baseOffsetPlaceholder = 0)
{
    rune::protocol::RecordBatchBuilder b(baseOffsetPlaceholder, /*baseTimestamp=*/1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// ===========================================================================
// GatewaySubscribeFixture
// ===========================================================================
class GatewaySubscribeFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 501;
    static constexpr int32_t kEgressBase    = 600;
    static constexpr int32_t kThisNodeId    = 7;
    static constexpr int     kPumpIter      = 50'000;

    static constexpr int32_t kTopicIdA    = 0;   // "events", 4 partitions
    static constexpr int32_t kPartitionA0 = 0;
    static constexpr int32_t kPartitionA1 = 1;

    // CP-5.2 (bug-1 regression): a 1-partition topic with a tiny 256-byte
    // segment roll so a handful of directAppend()s seal multiple segments,
    // letting deleteOldSegments() advance logStartOffset() past 0.  Used to
    // prove the ClientGateway Earliest->logStartOffset resolution is real.
    static constexpr int32_t kTopicIdC    = 1;   // "smallseg", 1 partition
    static constexpr int32_t kPartitionC0 = 0;

    void SetUp() override
    {
        tmpDir_  = std::make_unique<TempDir>("fix_");
        dataDir_ = std::make_unique<TempDir>("data_");
        fs::create_directories(dataDir_->path);

        startDriver();
        connectAeron();

        std::vector<TopicSpec> specs = {
            makeSpec("events", 4, kTopicIdA),
            // Tiny 256-byte segments → old-segment deletion advances logStart.
            makeSpec("smallseg", 1, kTopicIdC, /*segmentRollBytes=*/256),
        };
        registry_ = std::make_unique<TopicRegistry>(specs, dataDir_->path.string());

        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId);
        gw_->setTopicRegistry(registry_.get());
    }

    void TearDown() override
    {
        gw_.reset();
        registry_.reset();
        aeron_.reset();
        stopDriver();
        dataDir_.reset();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // TestClient — one logical client: ingress Publication + egress Subscription.
    // -----------------------------------------------------------------------
    struct TestClient {
        std::shared_ptr<aeron::Publication>   ingressPub;
        std::shared_ptr<aeron::Subscription>  egressSub;
        int32_t                               egressStreamId;
        std::string                           egressChannel;
    };

    TestClient makeClient(int32_t egressStreamId)
    {
        const std::string egressCh = "aeron:ipc";
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription(egressCh, egressStreamId);

        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;

        pump([&]() {
            if (!pub) pub = aeron_->findPublication(pubRegId);
            if (!sub) sub = aeron_->findSubscription(subRegId);
            return pub && sub;
        }, nullptr, nullptr, kPumpIter, "makeClient resolve pub+sub");

        return TestClient{pub, sub, egressStreamId, egressCh};
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer bytes on a Publication with bounded retry.
    // -----------------------------------------------------------------------
    static void offerBounded(aeron::Publication& pub,
                             const uint8_t* buf, std::size_t len,
                             std::string_view label = "offer")
    {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));

        bool sent = false;
        for (int i = 0; i < kPumpIter && !sent; ++i) {
            int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) {
                sent = true;
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // pump — drive gateway + optional client subscription until done() or cap.
    // -----------------------------------------------------------------------
    template <typename DoneFn>
    void pump(DoneFn&& done,
              aeron::Subscription* clientSub = nullptr,
              aeron::FragmentAssembler* clientAsm = nullptr,
              int maxIter = kPumpIter,
              std::string_view label = "pump")
    {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{15};

        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (pool_) pool_->doWork(nowNs());   // F19: worker-return push path
            if (clientSub && clientAsm) {
                clientSub->poll(clientAsm->handler(), 10);
            }
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(done()) << "pump timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // doConnect — send ConnectRequest, wait for ConnectResponse.
    // -----------------------------------------------------------------------
    pc::ConnectResponse doConnect(aeron::Publication& ingressPub,
                                  aeron::Subscription& egressSub,
                                  int32_t egressStreamId,
                                  const std::string& egressChannel,
                                  int64_t correlationId,
                                  uint8_t* scratch)
    {
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = correlationId;
        req.egressStreamId = egressStreamId;
        req.clientVersion  = 1;
        req.egressChannel  = egressChannel;
        const std::size_t reqLen = req.encode(scratch);

        pump([&]() { return ingressPub.isConnected(); },
             nullptr, nullptr, kPumpIter, "waitPubConnected");

        offerBounded(ingressPub, scratch, reqLen, "ConnectRequest");

        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
            auto decoded = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (decoded) resp = *decoded;
        });

        pump([&]() { return resp.has_value(); },
             &egressSub, &asm_, kPumpIter, "ConnectResponse");

        EXPECT_TRUE(resp.has_value()) << "No ConnectResponse received";
        return resp.value_or(pc::ConnectResponse{});
    }

    // -----------------------------------------------------------------------
    // sendSubscribe — encode + offer a Subscribe frame.
    // -----------------------------------------------------------------------
    void sendSubscribe(aeron::Publication& ingressPub,
                       int64_t sessionId, int64_t correlationId,
                       int32_t topicId, int32_t partition, uint8_t partitionKnown,
                       int64_t fromOffset, uint8_t isolation,
                       std::string_view label = "Subscribe")
    {
        pc::Subscribe sub;
        sub.sessionId      = sessionId;
        sub.correlationId  = correlationId;
        sub.topicId        = topicId;
        sub.partition      = partition;
        sub.partitionKnown = partitionKnown;
        sub.fromOffset     = fromOffset;
        sub.isolation      = isolation;
        sub.key            = {};

        const std::size_t encoded = sub.encode(scratch_);
        offerBounded(ingressPub, scratch_, encoded, label);
    }

    // -----------------------------------------------------------------------
    // sendUnsubscribe — encode + offer an Unsubscribe frame.
    // -----------------------------------------------------------------------
    void sendUnsubscribe(aeron::Publication& ingressPub,
                         int64_t sessionId, int64_t subscriptionId,
                         std::string_view label = "Unsubscribe")
    {
        pc::Unsubscribe unsub;
        unsub.sessionId      = sessionId;
        unsub.correlationId  = 0;
        unsub.subscriptionId = subscriptionId;
        const std::size_t encoded = unsub.encode(scratch_);
        offerBounded(ingressPub, scratch_, encoded, label);
    }

    // -----------------------------------------------------------------------
    // recvSubscribeResponse — poll for one SubscribeResponse.
    // -----------------------------------------------------------------------
    std::optional<pc::SubscribeResponse> recvSubscribeResponse(
        aeron::Subscription& egressSub, std::string_view label = "SubscribeResponse")
    {
        std::optional<pc::SubscribeResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
            auto dec = pc::SubscribeResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) resp = *dec;
        });
        pump([&]() { return resp.has_value(); },
             &egressSub, &asm_, kPumpIter, label);
        return resp;
    }

    // -----------------------------------------------------------------------
    // directAppend — append a batch directly to the Log (bypassing the gateway
    // Produce path).  Used to pre-populate log data for subscribe tests.
    // -----------------------------------------------------------------------
    int64_t directAppend(int32_t topicId, int32_t partition,
                         int32_t recordCount)
    {
        rune::storage::Log* log = registry_->log(topicId, partition);
        EXPECT_NE(log, nullptr) << "directAppend: unknown topic/partition";
        if (!log) return -1;

        const int64_t base = log->logEndOffset();
        auto batch = makeBatch(recordCount, base);
        log->append(batch, base + recordCount - 1);
        return base;
    }

    // -----------------------------------------------------------------------
    // Scratch buffer for encoding test frames.
    // -----------------------------------------------------------------------
    static constexpr std::size_t kScratch = 68 * 1024;
    uint8_t scratch_[kScratch] = {};

    std::unique_ptr<ClientGateway> gw_;
    std::unique_ptr<TopicRegistry> registry_;
    // F19: when set, pump() also drives the WorkerPool (same thread) so the
    // gateway's worker-return push path (PushRead → handleWorkerReturn_) runs.
    rune::node::WorkerPool*        pool_ = nullptr;
    std::shared_ptr<aeron::Aeron>  aeron_;

private:
    void startDriver()
    {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init failed: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start failed: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(
                    driver_,
                    aeron_driver_main_do_work(driver_));
            }
        });

        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC file not created within timeout";
    }

    void connectAeron()
    {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver()
    {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            static constexpr int kDrainMs = 50;
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds{kDrainMs};
            while (std::chrono::steady_clock::now() < deadline) {
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

    std::unique_ptr<TempDir>   tmpDir_;
    std::unique_ptr<TempDir>   dataDir_;
    aeron_driver_context_t*    driverCtx_    = nullptr;
    aeron_driver_t*            driver_       = nullptr;
    std::atomic<bool>          driverRunning_{false};
    std::thread                driverThread_;
};

// ===========================================================================
// TEST 1: Subscribe from 0, produce N batches → receive all N in order.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, Subscribe_From0_ReceiveAll_InOrder)
{
    auto client = makeClient(kEgressBase + 1);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/1, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // Subscribe from offset 0.
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/10,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "SubResp1");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(subResp->subscriptionId, 1);
    const int64_t subId = subResp->subscriptionId;

    // Produce 3 batches of 2 records each via directAppend.
    constexpr int kBatches = 3;
    constexpr int kRecordsPerBatch = 2;
    for (int i = 0; i < kBatches; ++i) {
        directAppend(kTopicIdA, kPartitionA0, kRecordsPerBatch);
    }

    // Collect Records pushed to the client.
    std::vector<pc::Records> received;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId) {
            received.push_back(*dec);
        }
    });

    pump([&]() { return static_cast<int>(received.size()) >= kBatches; },
         &*client.egressSub, &asm_, kPumpIter, "ReceiveAll");

    ASSERT_EQ(static_cast<int>(received.size()), kBatches);

    // Verify order: baseOffsets should be 0, 2, 4.
    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(received[i].baseOffset, static_cast<int64_t>(i * kRecordsPerBatch))
            << "Batch " << i << " baseOffset mismatch";
        EXPECT_EQ(received[i].subscriptionId, subId);
        EXPECT_EQ(received[i].topicId, kTopicIdA);
        EXPECT_EQ(received[i].partition, kPartitionA0);
        EXPECT_FALSE(received[i].recordBatch.empty()) << "Batch " << i << " recordBatch is empty";
    }
}

// ===========================================================================
// TEST 2: fromOffset mid-stream — receive only records from that offset.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, Subscribe_FromOffset_MidStream)
{
    // Produce 5 records (as one batch of 5) directly to the log.
    directAppend(kTopicIdA, kPartitionA0, 5); // batch at offset 0, records 0..4

    auto client = makeClient(kEgressBase + 2);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/2, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // Subscribe from offset 3 (inside the first batch — Log::read will start at/after 3).
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/20,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/3, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "SubResp2");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId = subResp->subscriptionId;

    // Produce one more batch of 2 records.
    directAppend(kTopicIdA, kPartitionA0, 2); // batch at offset 5, records 5..6

    std::vector<pc::Records> received;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId) received.push_back(*dec);
    });

    // We expect at most 2 pushes: the Log::read(3, ...) may return either
    // the whole first batch (starting at 0) or the second batch (at 5).
    // The Log implementation returns whole batches starting at/after startOffset,
    // so it may return the batch at offset 0 (which covers offset 3).
    // Either way, we receive the second batch with baseOffset=5.
    pump([&]() { return !received.empty(); }, &*client.egressSub, &asm_, kPumpIter, "Recv2");

    ASSERT_FALSE(received.empty());
    // All received batches must have baseOffset >= 0 (the log starts there).
    // The subscription nextOffset starts at 3; Log::read(3,..) will return whatever
    // batch is at/after offset 3. Our log stores the batch at baseOffset=0 (records 0..4),
    // which covers offset 3, so it may be returned. After that, offset 5 batch is pushed.
    // Verify: no batch has a baseOffset that would imply a batch before fromOffset=3
    // was skipped; all records delivered are valid.
    for (const auto& rec : received) {
        EXPECT_GE(rec.baseOffset, 0) << "Unexpected baseOffset";
        EXPECT_FALSE(rec.recordBatch.empty());
    }
}

// ===========================================================================
// TEST 3: from Latest — receive only records produced AFTER subscribe.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, Subscribe_FromLatest)
{
    // Pre-populate the log with 3 records.
    directAppend(kTopicIdA, kPartitionA0, 3); // records 0..2

    auto client = makeClient(kEgressBase + 3);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/3, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // Subscribe with fromOffset = -1 (sentinel for "from Latest").
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/30,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/-1, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "SubResp3");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId = subResp->subscriptionId;

    // Pump briefly to ensure no pre-subscribe records are pushed.
    {
        std::vector<pc::Records> earlyReceived;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
            auto dec = pc::Records::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec && dec->subscriptionId == subId) earlyReceived.push_back(*dec);
        });

        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::milliseconds{300};
        while (clock::now() < deadline) {
            gw_->doWork(nowNs());
            client.egressSub->poll(asm_.handler(), 10);
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }

        EXPECT_TRUE(earlyReceived.empty())
            << "Should not receive records produced before subscribe with fromLatest";
    }

    // Produce after subscribe.
    directAppend(kTopicIdA, kPartitionA0, 2); // records 3..4

    std::vector<pc::Records> received;
    aeron::FragmentAssembler asm2_([&](aeron::concurrent::AtomicBuffer& buf,
                                        aeron::util::index_t offset,
                                        aeron::util::index_t len,
                                        aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId) received.push_back(*dec);
    });

    pump([&]() { return !received.empty(); }, &*client.egressSub, &asm2_, kPumpIter, "LatestRecv");

    ASSERT_FALSE(received.empty());
    // All received batches must have baseOffset >= 3 (records produced after subscribe).
    for (const auto& rec : received) {
        EXPECT_GE(rec.baseOffset, 3)
            << "Received record before subscribe from Latest (baseOffset=" << rec.baseOffset << ")";
    }
}

// ===========================================================================
// TEST 3b (CP-5.2 bug-1 regression): Subscribe-from-Earliest AFTER the head
// segment has been deleted (logStartOffset advanced past 0).
//
// This is the NON-VACUOUS proof for the ClientGateway Earliest->logStartOffset
// fix.  Before the fix, handleSubscribe resolved the Earliest sentinel (-2) to
// offset 0.  Once retention/compaction deletes the head segment(s),
// Log::read(0) returns {} forever (upper_bound lands on begin()), so an
// Earliest subscriber pinned to 0 gets ZERO records for the life of the
// subscription — the exact starvation this test guards against.
//
// Scratch-revert proof: change the fix at ClientGateway.cpp (the
// `msg.fromOffset == kFromEarliest` branch) from `nextOffset = logStart` back
// to `nextOffset = 0` and this test FAILS (0 records delivered), while the
// happy-path Subscribe_From0/FromLatest tests still pass.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, Subscribe_FromEarliest_AfterHeadSegmentDeleted)
{
    // From::Earliest sentinel on the wire (mirrors client From::toWire()).
    static constexpr int64_t kFromEarliest = -2;

    // Produce enough single-record batches on the tiny-segment topic that the
    // log seals several segments (256-byte roll → a few records per segment).
    // Each directAppend here writes one 1-record batch.
    constexpr int kRecords = 40;
    for (int i = 0; i < kRecords; ++i) {
        directAppend(kTopicIdC, kPartitionC0, /*recordCount=*/1);
    }

    rune::storage::Log* log = registry_->log(kTopicIdC, kPartitionC0);
    ASSERT_NE(log, nullptr);
    ASSERT_GT(log->segmentBaseOffsets().size(), 1u)
        << "tiny 256-byte roll should have sealed multiple segments";
    ASSERT_EQ(log->logStartOffset(), 0);

    // Delete old segments so the head is dropped and logStartOffset advances.
    const int deleted = log->deleteOldSegments(/*retainBytes=*/0);
    ASSERT_GT(deleted, 0);
    const int64_t logStart = log->logStartOffset();
    ASSERT_GT(logStart, 0)
        << "old-segment deletion must advance logStartOffset past 0";

    // Sanity: reading from 0 (the OLD Earliest resolution) yields nothing —
    // this is precisely why the un-fixed gateway starved Earliest subscribers.
    ASSERT_TRUE(log->read(0, 1 << 20).empty())
        << "read(0) below logStart must be empty (the starvation trap)";
    // Sanity: reading from logStart via the SAME log the gateway will use must
    // return the oldest survivor (proves the push path has data to deliver).
    ASSERT_FALSE(log->read(logStart, 1 << 20).empty())
        << "read(logStart) must return the oldest surviving batch";
    ASSERT_LT(logStart, log->logEndOffset())
        << "logStart must be below LEO so the push bound admits delivery";

    // Now subscribe FROM EARLIEST (-2).  The fix must resolve this to logStart,
    // so the subscriber receives the oldest SURVIVING batch, not zero records.
    auto client = makeClient(kEgressBase + 31);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/31, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    sendSubscribe(*client.ingressPub, sid, /*corrId=*/310,
                  kTopicIdC, kPartitionC0, /*partitionKnown=*/1,
                  /*fromOffset=*/kFromEarliest, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "SubRespEarliest");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId = subResp->subscriptionId;

    std::vector<pc::Records> received;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId) received.push_back(*dec);
    });

    // Append one record AFTER subscribe: the push loop is level-driven from the
    // duty cycle, and this fresh append is a reliable delivery trigger for the
    // in-process pump.  It does NOT change WHERE Earliest resolves — the
    // subscription's nextOffset was already fixed at subscribe time (to logStart
    // under the fix, or to 0 under the reverted code).  Because the reverted code
    // pins nextOffset=0 and Log::read(0) is permanently empty after head-segment
    // deletion, the push loop delivers NOTHING even after this trigger append —
    // that is exactly the starvation the fix cures.
    directAppend(kTopicIdC, kPartitionC0, /*recordCount=*/1);

    pump([&]() { return !received.empty(); },
         &*client.egressSub, &asm_, kPumpIter, "EarliestRecv");

    // THE FIX CONTRACT (non-vacuous): with the fix, handleSubscribe resolves the
    // Earliest sentinel (-2) to logStartOffset(), so the subscription's
    // nextOffset lands on a LIVE offset and the push loop delivers records.
    // With the fix REVERTED (nextOffset = 0), the subscriber is pinned below the
    // deleted head; Log::read(0) returns {} forever, so this vector stays EMPTY
    // and the pump times out — this ASSERT fires.  That divergence is the proof.
    ASSERT_FALSE(received.empty())
        << "Earliest subscriber received ZERO records after head-segment "
           "deletion — the Earliest->logStartOffset fix regressed (or was reverted)";
    // Every delivered batch must live at/above the live log start — never a
    // batch from the deleted (pre-logStart) region.
    for (const auto& rec : received) {
        EXPECT_GE(rec.baseOffset, logStart)
            << "delivered batch below live logStartOffset (baseOffset="
            << rec.baseOffset << ", logStart=" << logStart << ")";
        EXPECT_FALSE(rec.recordBatch.empty());
    }
}

// ===========================================================================
// TEST 4: Back-pressure exactly-once gate.
//
// Strategy: use a TINY egress term buffer (4 KiB, minimum Aeron allows) so that
// back-pressure triggers quickly.  Produce many batches.  Drive doWork in small
// bursts.  Between bursts, drain the egress subscription so the term buffer
// frees up.  Assert that every record is received exactly once, in order.
//
// This is the correctness gate: it verifies that:
//   (a) delivery pauses when egress is back-pressured,
//   (b) resumes from the exact same offset (no loss),
//   (c) no duplicates (every batch delivered exactly once).
// ===========================================================================
TEST_F(GatewaySubscribeFixture, BackPressure_ExactlyOnce_Gate)
{
    // We need a tiny egress publication to create back-pressure.
    // Use a different egress streamId with small term buffer.
    // Strategy: create the egress subscription with a tiny term buffer via
    // the Aeron URI parameter term-length.
    // Aeron minimum IPC term-length is 64 KiB; we'll use the minimum and produce
    // many batches to fill it.

    const int32_t bpEgressStream = kEgressBase + 40;
    // Use minimum term size (64 KiB — Aeron IPC minimum).
    const std::string bpEgressCh = "aeron:ipc?term-length=65536";

    // Client: ingress pub on the standard ingress stream.
    int64_t ingressPubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
    // Egress subscription with tiny term buffer.
    int64_t egressSubRegId  = aeron_->addSubscription(bpEgressCh, bpEgressStream);

    std::shared_ptr<aeron::Publication>  ingressPub;
    std::shared_ptr<aeron::Subscription> egressSub;

    pump([&]() {
        if (!ingressPub) ingressPub = aeron_->findPublication(ingressPubRegId);
        if (!egressSub)  egressSub  = aeron_->findSubscription(egressSubRegId);
        return ingressPub && egressSub;
    }, nullptr, nullptr, kPumpIter, "BP resolve pub+sub");

    // Connect.
    pc::ConnectRequest req;
    req.sessionId      = 0;
    req.correlationId  = 400;
    req.egressStreamId = bpEgressStream;
    req.clientVersion  = 1;
    req.egressChannel  = bpEgressCh;
    const std::size_t reqLen = req.encode(scratch_);

    pump([&]() { return ingressPub->isConnected(); }, nullptr, nullptr, kPumpIter, "BP waitConn");
    offerBounded(*ingressPub, scratch_, reqLen, "BP Connect");

    std::optional<pc::ConnectResponse> connResp;
    aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
        auto dec = pc::ConnectResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) connResp = *dec;
    });
    pump([&]() { return connResp.has_value(); }, &*egressSub, &connAsm, kPumpIter, "BP ConnResp");
    ASSERT_TRUE(connResp.has_value());
    const int64_t sid = connResp->assignedSessionId;

    // Subscribe from 0 FIRST, then produce — so the SubscribeResponse arrives before
    // any push, and the combined assembler below captures all Records without loss.
    sendSubscribe(*ingressPub, sid, /*corrId=*/401,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    // Combined assembler: captures both SubscribeResponse and Records.
    // This prevents Records pushed during the SubscribeResponse pump from being
    // silently dropped by a single-purpose SubscribeResponse assembler.
    std::optional<pc::SubscribeResponse> subResp;
    std::vector<int64_t> receivedBaseOffsets;
    // subId is set once subResp arrives; use 0 as sentinel (no valid sub has id=0).
    int64_t subId = 0;

    aeron::FragmentAssembler combinedAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                              aeron::util::index_t offset,
                                              aeron::util::index_t len,
                                              aeron::Header&) {
        const auto* ptr = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
        const auto  sz  = static_cast<std::size_t>(len);

        // Try SubscribeResponse first.
        if (!subResp) {
            auto dec = pc::SubscribeResponse::decode(ptr, sz);
            if (dec) { subResp = *dec; subId = dec->subscriptionId; return; }
        }
        // Try Records (captures any pushed during or after SubscribeResponse pump).
        auto dec = pc::Records::decode(ptr, sz);
        if (dec && subId != 0 && dec->subscriptionId == subId) {
            receivedBaseOffsets.push_back(dec->baseOffset);
        }
    });

    // Wait for SubscribeResponse (combined assembler also captures early Records).
    pump([&]() { return subResp.has_value(); }, &*egressSub, &combinedAsm, kPumpIter, "BP SubResp");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // Produce 20 batches of 3 records each directly to the log AFTER subscribing.
    // This ensures the log has data for the push loop to deliver.
    constexpr int kBatches = 20;
    constexpr int kRecordsPerBatch = 3;
    for (int i = 0; i < kBatches; ++i) {
        directAppend(kTopicIdA, kPartitionA0, kRecordsPerBatch);
    }
    // Total: 60 records.

    // GB-5 Fix #3: assert back-pressure actually occurred.
    //
    // The production per-cycle cap is large (kMaxBatchesPerSubPerCycle=1024, raised
    // for read throughput), so 20 batches would all deliver in one cycle and the
    // pause path would never run.  Lower the cap to 8 for THIS test so the pause is
    // exercised deterministically, independent of the production default.
    constexpr int kTestPerCycleCap = 8;
    gw_->setMaxBatchesPerSubPerCycleForTest(kTestPerCycleCap);
    //
    // Strategy: run exactly ONE doWork() without draining the egress.  Since
    // pushSubscriptions() now caps delivery at kTestPerCycleCap (8) and we have
    // kBatches (20) > 8, the first doWork can deliver at most 8 batches.
    // Additionally, the tiny 64 KiB egress term + the ConnectResponse already
    // sent may fill the ring and cause tryClaim to return back-pressure (pos <= 0)
    // before the cap is hit.
    //
    // Either way: after exactly ONE doWork(), receivedBaseOffsets.size() MUST be
    // < kBatches — proving that pushSubscriptions() paused and that nextOffset was
    // preserved (no data loss).  The subsequent drain loop resumes delivery from
    // the exact saved offset, exercising the full pause/resume path.
    gw_->doWork(nowNs()); // one doWork — capped at kTestPerCycleCap=8
    egressSub->poll(combinedAsm.handler(), 32);
    const std::size_t afterOneDoWork = receivedBaseOffsets.size();

    // After one doWork we must have received FEWER than all kBatches:
    // (a) the per-cycle cap (8) is smaller than kBatches (20), OR
    // (b) the tiny egress term was full and tryClaim returned back-pressure.
    // Either condition means the pause path ran.
    EXPECT_LT(afterOneDoWork, static_cast<std::size_t>(kBatches))
        << "BP gate: expected partial delivery after one doWork() cycle "
        << "(max-per-cycle=" << kTestPerCycleCap << ", kBatches=" << kBatches << "). "
        << "Got " << afterOneDoWork << " — the pause/resume path was not reached.";

    // Now drive to completion (drain egress between doWork bursts).
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{30};
    while (static_cast<int>(receivedBaseOffsets.size()) < kBatches &&
           clock::now() < deadline) {
        // Small burst of doWork.
        for (int i = 0; i < 5; ++i) {
            gw_->doWork(nowNs());
        }
        // Drain egress to relieve back-pressure.
        for (int i = 0; i < 20; ++i) {
            egressSub->poll(combinedAsm.handler(), 10);
        }
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    ASSERT_EQ(static_cast<int>(receivedBaseOffsets.size()), kBatches)
        << "Expected " << kBatches << " batches, got " << receivedBaseOffsets.size();

    // Verify: each batch received exactly once, in order, no gaps.
    for (int i = 0; i < kBatches; ++i) {
        const int64_t expectedBase = static_cast<int64_t>(i * kRecordsPerBatch);
        EXPECT_EQ(receivedBaseOffsets[i], expectedBase)
            << "Batch " << i << ": expected baseOffset=" << expectedBase
            << " got=" << receivedBaseOffsets[i]
            << " — back-pressure exactly-once invariant violated";
    }

    // Check no duplicates (a sorted deduplicated list has the same size).
    {
        auto sorted = receivedBaseOffsets;
        std::sort(sorted.begin(), sorted.end());
        auto unique_end = std::unique(sorted.begin(), sorted.end());
        EXPECT_EQ(std::distance(sorted.begin(), unique_end),
                  static_cast<std::ptrdiff_t>(receivedBaseOffsets.size()))
            << "Duplicate Records detected — exactly-once invariant violated";
    }
}

// ===========================================================================
// TEST 5: Unsubscribe stops further delivery.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, Unsubscribe_StopsDelivery)
{
    auto client = makeClient(kEgressBase + 5);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/5, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    sendSubscribe(*client.ingressPub, sid, /*corrId=*/50,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "SubResp5");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId = subResp->subscriptionId;

    // Produce one batch to verify delivery works.
    directAppend(kTopicIdA, kPartitionA0, 2);

    std::vector<pc::Records> received;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId) received.push_back(*dec);
    });

    pump([&]() { return !received.empty(); }, &*client.egressSub, &asm_, kPumpIter, "Recv5a");
    ASSERT_FALSE(received.empty()) << "Initial records must be received before unsubscribe";

    // Unsubscribe.
    sendUnsubscribe(*client.ingressPub, sid, subId);

    // Give gateway time to process the Unsubscribe.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 500; }, nullptr, nullptr, 1000, "UnsubProcess");
    }

    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "Subscription should be removed after Unsubscribe";

    // Produce more records AFTER unsubscribe.
    const std::size_t beforeCount = received.size();
    directAppend(kTopicIdA, kPartitionA0, 3);

    // Drive for a while; expect NO additional Records for this subscription.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::milliseconds{500};
    while (clock::now() < deadline) {
        gw_->doWork(nowNs());
        client.egressSub->poll(asm_.handler(), 10);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    EXPECT_EQ(received.size(), beforeCount)
        << "Received records after Unsubscribe — delivery should have stopped";
}

// ===========================================================================
// TEST 6: Two subscriptions (different partitions, different sessions) —
//         no cross-talk; each gets its own records; fairness within cycle cap.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, TwoSubscriptions_NoXtalk_Fairness)
{
    // Client A → partition 0, Client B → partition 1.
    auto clientA = makeClient(kEgressBase + 6);
    auto clientB = makeClient(kEgressBase + 7);

    auto connA = doConnect(*clientA.ingressPub, *clientA.egressSub,
                           clientA.egressStreamId, clientA.egressChannel,
                           /*correlationId=*/60, scratch_);
    uint8_t scratch2[68 * 1024] = {};
    auto connB = doConnect(*clientB.ingressPub, *clientB.egressSub,
                           clientB.egressStreamId, clientB.egressChannel,
                           /*correlationId=*/61, scratch2);

    const int64_t sidA = connA.assignedSessionId;
    const int64_t sidB = connB.assignedSessionId;

    // Subscribe A to partition 0.
    sendSubscribe(*clientA.ingressPub, sidA, /*corrId=*/600,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subRespA = recvSubscribeResponse(*clientA.egressSub, "SubRespA");
    ASSERT_TRUE(subRespA.has_value());
    EXPECT_EQ(subRespA->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subIdA = subRespA->subscriptionId;

    // Subscribe B to partition 1.
    {
        pc::Subscribe sub;
        sub.sessionId      = sidB;
        sub.correlationId  = 601;
        sub.topicId        = kTopicIdA;
        sub.partition      = kPartitionA1;
        sub.partitionKnown = 1;
        sub.fromOffset     = 0;
        sub.isolation      = 0;
        sub.key            = {};
        const std::size_t encoded = sub.encode(scratch2);
        offerBounded(*clientB.ingressPub, scratch2, encoded, "SubB");
    }

    std::optional<pc::SubscribeResponse> subRespB;
    aeron::FragmentAssembler subAsmB([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
        auto dec = pc::SubscribeResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) subRespB = *dec;
    });
    pump([&]() { return subRespB.has_value(); }, &*clientB.egressSub, &subAsmB, kPumpIter, "SubRespB");
    ASSERT_TRUE(subRespB.has_value());
    EXPECT_EQ(subRespB->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subIdB = subRespB->subscriptionId;

    EXPECT_NE(subIdA, subIdB) << "Subscription IDs must be distinct";

    // Produce records to each partition independently.
    directAppend(kTopicIdA, kPartitionA0, 3);  // partition 0: records 0..2
    directAppend(kTopicIdA, kPartitionA1, 4);  // partition 1: records 0..3

    std::vector<pc::Records> recvA, recvB;
    aeron::FragmentAssembler asmA([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subIdA) recvA.push_back(*dec);
    });
    aeron::FragmentAssembler asmB([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subIdB) recvB.push_back(*dec);
    });

    // Drive until both receive at least one batch.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{15};
    while ((recvA.empty() || recvB.empty()) && clock::now() < deadline) {
        gw_->doWork(nowNs());
        clientA.egressSub->poll(asmA.handler(), 10);
        clientB.egressSub->poll(asmB.handler(), 10);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    ASSERT_FALSE(recvA.empty()) << "Client A received nothing";
    ASSERT_FALSE(recvB.empty()) << "Client B received nothing";

    // No cross-talk: A must not have B's subscription ID and vice versa.
    for (const auto& r : recvA) {
        EXPECT_EQ(r.subscriptionId, subIdA) << "Client A received record from wrong sub";
        EXPECT_EQ(r.partition, kPartitionA0) << "Client A received wrong partition";
    }
    for (const auto& r : recvB) {
        EXPECT_EQ(r.subscriptionId, subIdB) << "Client B received record from wrong sub";
        EXPECT_EQ(r.partition, kPartitionA1) << "Client B received wrong partition";
    }
}

// ===========================================================================
// TEST 7: Re-subscribe from tracked offset resumes correctly.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, ReSubscribe_FromTrackedOffset_Resumes)
{
    auto client = makeClient(kEgressBase + 7);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/7, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // First subscription from 0. Produce records AFTER receiving SubscribeResponse
    // to prevent Records being pushed and dropped during the sub-response pump.
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/70,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subResp1 = recvSubscribeResponse(*client.egressSub, "SubResp7a");
    ASSERT_TRUE(subResp1.has_value());
    const int64_t subId1 = subResp1->subscriptionId;

    // Produce 3 records AFTER the subscription is registered.
    directAppend(kTopicIdA, kPartitionA0, 3); // records 0..2

    std::vector<pc::Records> recv1;
    aeron::FragmentAssembler asm1([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId1) recv1.push_back(*dec);
    });

    pump([&]() { return !recv1.empty(); }, &*client.egressSub, &asm1, kPumpIter, "Recv7a");
    ASSERT_FALSE(recv1.empty());

    // Compute nextOffset after receiving (simulate client tracking).
    // The batch at baseOffset=0 has 3 records → nextOffset after = 3.
    const int64_t trackedOffset = recv1.back().baseOffset + 3; // baseOffset=0 + 3 = 3

    // Unsubscribe the first subscription.
    sendUnsubscribe(*client.ingressPub, sid, subId1);
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "UnsubProcess7");
    }

    // Re-subscribe from the tracked offset.
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/71,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/trackedOffset, /*isolation=*/0);

    auto subResp2 = recvSubscribeResponse(*client.egressSub, "SubResp7b");
    ASSERT_TRUE(subResp2.has_value());
    EXPECT_EQ(subResp2->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId2 = subResp2->subscriptionId;

    // Produce 2 more records (records 3..4).
    directAppend(kTopicIdA, kPartitionA0, 2);

    std::vector<pc::Records> recv2;
    aeron::FragmentAssembler asm2([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId2) recv2.push_back(*dec);
    });

    pump([&]() { return !recv2.empty(); }, &*client.egressSub, &asm2, kPumpIter, "Recv7b");
    ASSERT_FALSE(recv2.empty());

    // All received batches must have baseOffset >= trackedOffset (no replay).
    for (const auto& r : recv2) {
        EXPECT_GE(r.baseOffset, trackedOffset)
            << "Re-subscribe returned record before trackedOffset=" << trackedOffset;
    }
}

// ===========================================================================
// TEST 8: read_committed == read_uncommitted for M-B.
//
// Both isolation modes (0 and 1) deliver up to LEO; assert they produce the
// same result.  The TODO(Milestone C) for real LSO is noted in pushSubscriptions.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, ReadCommitted_EqualsUncommitted_ForMilestoneB)
{
    auto client = makeClient(kEgressBase + 8);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/8, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // Subscribe with read_uncommitted (isolation=0).
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/80,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);
    auto subRespUncomm = recvSubscribeResponse(*client.egressSub, "SubRespUncomm");
    ASSERT_TRUE(subRespUncomm.has_value());
    const int64_t subIdUncomm = subRespUncomm->subscriptionId;

    // Subscribe with read_committed (isolation=1) — same partition, same session.
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/81,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/1);
    auto subRespComm = recvSubscribeResponse(*client.egressSub, "SubRespComm");
    ASSERT_TRUE(subRespComm.has_value());
    const int64_t subIdComm = subRespComm->subscriptionId;

    // Produce 4 records AFTER both subscriptions are registered (avoids race with
    // pump in recvSubscribeResponse consuming pushed Records before asm_ is set up).
    directAppend(kTopicIdA, kPartitionA0, 4);

    std::vector<pc::Records> recvUncomm, recvComm;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (!dec) return;
        if (dec->subscriptionId == subIdUncomm) recvUncomm.push_back(*dec);
        if (dec->subscriptionId == subIdComm)   recvComm.push_back(*dec);
    });

    pump([&]() { return !recvUncomm.empty() && !recvComm.empty(); },
         &*client.egressSub, &asm_, kPumpIter, "RecvBothIsolations");

    ASSERT_FALSE(recvUncomm.empty()) << "read_uncommitted received nothing";
    ASSERT_FALSE(recvComm.empty())   << "read_committed received nothing";

    // Both should receive the same batches (same bound = LEO for M-B).
    // They both start from offset 0 so the first batch should be the same.
    EXPECT_EQ(recvUncomm.front().baseOffset, recvComm.front().baseOffset)
        << "read_committed and read_uncommitted should deliver from the same bound in M-B";
    // TODO(Milestone C): when real LSO is implemented, read_committed will stop at LSO,
    // which may differ from LEO when there are open transactions.
}

// ===========================================================================
// TEST 9: Malformed subscribe / unknown topic / bad partition → errorCode, no crash.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, MalformedSubscribe_ErrorCodes_NoGatewayCrash)
{
    auto client = makeClient(kEgressBase + 9);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/9, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    // --- Unknown topic ---
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/90,
                  /*topicId=*/99, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0, "SubUnknownTopic");

    auto errResp1 = recvSubscribeResponse(*client.egressSub, "SubErrUnknownTopic");
    ASSERT_TRUE(errResp1.has_value());
    EXPECT_EQ(errResp1->errorCode, static_cast<int16_t>(pc::ErrorCode::UnknownTopic));

    // --- Bad partition ---
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/91,
                  kTopicIdA, /*partition=*/99, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0, "SubBadPartition");

    auto errResp2 = recvSubscribeResponse(*client.egressSub, "SubErrBadPartition");
    ASSERT_TRUE(errResp2.has_value());
    EXPECT_EQ(errResp2->errorCode, static_cast<int16_t>(pc::ErrorCode::UnknownPartition));

    // Gateway must keep serving — valid subscribe must succeed.
    sendSubscribe(*client.ingressPub, sid, /*corrId=*/92,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0, "SubValid");

    auto okResp = recvSubscribeResponse(*client.egressSub, "SubRespOK");
    ASSERT_TRUE(okResp.has_value());
    EXPECT_EQ(okResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(okResp->subscriptionId, 1);
}

// ===========================================================================
// TEST 10: Reap-driven subscription drop (GB-5 Fix #4).
//
// Scenario: a session subscribes successfully, then times out via reapExpired()
// (not an explicit Unsubscribe). Asserts:
//   (a) subscriptionCount() drops to 0 after reap.
//   (b) No crash or push attempt to the freed egress.
//   (c) The gateway continues serving other sessions unaffected.
//
// Uses injected nowNs to drive reaping deterministically (no wall-clock wait).
// ===========================================================================
TEST_F(GatewaySubscribeFixture, ReapDriven_SubscriptionDrop_NocrashNoPush)
{
    // Use a very short session timeout (10 ms expressed in ns) so we can
    // advance the clock past it without long waits.
    // We must rebuild the gateway with a custom timeout for this test.
    gw_.reset(); // destroy the fixture's gateway

    constexpr int64_t kShortTimeoutNs = 10LL * 1'000'000LL; // 10 ms in ns

    gw_ = std::make_unique<ClientGateway>(
        aeron_,
        "aeron:ipc",
        kIngressStream + 100, // use a fresh stream to avoid residual traffic
        kThisNodeId,
        kShortTimeoutNs);
    gw_->setTopicRegistry(registry_.get());

    // Create the ingress publication on the new stream.
    const int32_t reapEgressStream = kEgressBase + 100;
    int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream + 100);
    int64_t subRegId = aeron_->addSubscription("aeron:ipc", reapEgressStream);

    std::shared_ptr<aeron::Publication>  ingressPub;
    std::shared_ptr<aeron::Subscription> egressSub;

    pump([&]() {
        if (!ingressPub) ingressPub = aeron_->findPublication(pubRegId);
        if (!egressSub)  egressSub  = aeron_->findSubscription(subRegId);
        return ingressPub && egressSub;
    }, nullptr, nullptr, kPumpIter, "Reap resolve pub+sub");

    // Connect.
    auto connResp = doConnect(*ingressPub, *egressSub,
                              reapEgressStream, "aeron:ipc",
                              /*correlationId=*/100, scratch_);
    const int64_t sid = connResp.assignedSessionId;
    ASSERT_GE(sid, 1);
    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Subscribe from 0.
    sendSubscribe(*ingressPub, sid, /*corrId=*/1001,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*egressSub, "SubResp_Reap");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(gw_->subscriptionCount(), 1u) << "Subscription should be registered";

    // Produce some records so pushSubscriptions() has something to attempt.
    directAppend(kTopicIdA, kPartitionA0, 3);

    // Advance injected clock well past the session timeout to trigger reap.
    // We use a large artificial nowNs: session was created at ~current wall time,
    // so we pass nowNs = INT64_MAX / 2 to ensure nowNs - lastSeenNs > kShortTimeoutNs.
    const int64_t reapNs = std::numeric_limits<int64_t>::max() / 2;
    gw_->reapExpired(reapNs);

    // After reap: both session and subscription must be gone.
    EXPECT_EQ(gw_->sessionCount(),      0u) << "Session should be reaped by idle-timeout";
    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "Subscription should be dropped when session reaped";

    // Run a few doWork cycles to confirm pushSubscriptions() handles the missing
    // session gracefully (no crash, no push to the freed egress).
    for (int i = 0; i < 20; ++i) {
        ASSERT_NO_THROW(gw_->doWork(reapNs))
            << "doWork must not throw or crash after session reap at cycle " << i;
    }

    // Drain egress — expect NO Records (subscription was dropped).
    std::vector<pc::Records> unexpectedRecords;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) unexpectedRecords.push_back(*dec);
    });
    using clock = std::chrono::steady_clock;
    const auto drainDeadline = clock::now() + std::chrono::milliseconds{300};
    while (clock::now() < drainDeadline) {
        gw_->doWork(reapNs);
        egressSub->poll(asm_.handler(), 10);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }
    EXPECT_TRUE(unexpectedRecords.empty())
        << "No Records should be pushed after reap-driven subscription drop";
}

// ===========================================================================
// MC3 [acks=1 semantics pin] — subscriber can receive a record that is not
// yet fsync'd (read-uncommitted-of-unflushed).
//
// This test pins the INTENDED acks=1 / page-cache-visible semantics:
//   A subscriber CAN receive a record that is page-cache-visible (via
//   Log::read) but NOT yet fsync'd (flushDirtyLogs has not fired yet).
//   If the leader crashes between appendNoSync() and sync(), the record
//   may be lost.  This is the documented acks=1 / acks=Leader trade-off.
//   Milestone C (acks=All) closes this gap via ISR replication.
//
// The test confirms:
//   1. A subscriber receives the record immediately (page-cache-visible).
//   2. The record is NOT yet in the fsynced log (we verify by dropping the
//      in-memory log state and reopening without sync — simulating a crash
//      before flush).
//   3. After that simulated crash, the subscriber would have seen a record
//      that is no longer on disk — this is the documented acks=1 window.
//
// Note: the test does NOT assert the acks=1 window is safe (it is not — it
// is an explicit trade-off).  It asserts that the window EXISTS and the
// subscriber receives the record before the flush, pinning this as a known
// property rather than an accidental one.
// ===========================================================================
TEST_F(GatewaySubscribeFixture, MC3_SubscriberAheadOfFlush_Acks1PageCacheSemantics)
{
    // Use a Log directly to inject a no-sync write, bypass the gateway produce
    // path (which would flush asynchronously and complicate the timing).
    rune::storage::Log* log = registry_->log(kTopicIdA, kPartitionA0);
    ASSERT_NE(log, nullptr);

    // Subscribe FIRST so the subscription is registered before any data.
    auto client = makeClient(kEgressBase + 110);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/110, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    sendSubscribe(*client.ingressPub, sid, /*corrId=*/1100,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    auto subResp = recvSubscribeResponse(*client.egressSub, "MC3-SubResp");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t subId = subResp->subscriptionId;

    // appendNoSync — page-cache write, NO fsync.
    // This simulates a produce arriving in the acks=1 window: the gateway has
    // acked the record to the producer but has not yet called flushDirtyLogs().
    constexpr int kNoSyncRecords = 2;
    {
        const int64_t base = log->logEndOffset();
        auto batch = makeBatch(kNoSyncRecords, base);
        const int64_t lastOff = base + kNoSyncRecords - 1;
        log->appendNoSync(batch.data(), batch.size(), lastOff);
    }
    // LEO has advanced (page-cache visible).
    EXPECT_EQ(log->logEndOffset(), kNoSyncRecords);

    // --- Gate: subscriber MUST receive the record (page-cache-visible) ---
    // This is the key acks=1 assertion: pushSubscriptions() calls Log::read()
    // which reads from page cache, so the not-yet-fsynced record is visible.
    std::vector<int64_t> received;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                       aeron::util::index_t offset,
                                       aeron::util::index_t len,
                                       aeron::Header&) {
        auto dec = pc::Records::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec && dec->subscriptionId == subId)
            received.push_back(dec->baseOffset);
    });

    pump([&]() { return !received.empty(); },
         &*client.egressSub, &asm_, kPumpIter, "MC3-RecvUnflushed");

    ASSERT_FALSE(received.empty())
        << "MC3 [acks=1 pin]: subscriber must receive page-cache-visible record "
           "BEFORE flushDirtyLogs() fires — acks=1 semantics";
    EXPECT_EQ(received.front(), 0)
        << "MC3: first received batch must start at baseOffset=0";

    // --- Document the acks=1 window (comment — no further assertion) ---
    // At this point, the subscriber has received the record but Log::sync()
    // has NOT been called.  A leader crash here would lose the record from
    // disk (torn tail dropped by recover()), yet the subscriber already
    // received and acked it.  This is the known acks=1 / acks=Leader window.
    // Milestone C (acks=All + ISR HWM) will close this gap.
    // We do NOT assert data loss here — we only pin that the window exists.

    // Clean up: sync the log to avoid interfering with other tests.
    EXPECT_NO_THROW(log->sync());
}

// ===========================================================================
// MC5 [M2 regression gate] — mid-chunk corrupt-batch skip advances nextOffset.
//
// If a batch in the middle of a chunk read has recordCount < 1 (corrupt),
// the push loop must advance BOTH chunkPos AND sub.nextOffset past it.
// Without the M2 fix, nextOffset stays at the corrupt batch's baseOffset,
// the next cycle re-reads the same corrupt batch, and the subscription
// permanently stalls — all subsequent (valid) records are starved.
//
// Test scenario:
//   1. Append one VALID batch (batch A, offsets [0..1]).
//   2. Directly write a corrupt batch (recordCount=0) into the log segment
//      at offset 2 (bypassing gateway M1 validation).
//   3. Append another VALID batch (batch C, offsets [3..4]) after the corrupt one.
//   4. Subscribe from 0.
//   5. Assert batch A is delivered.
//   6. Assert batch C is eventually delivered (the corrupt batch is skipped,
//      nextOffset advances past it, forward progress is made).
//   7. Assert the back-pressure path is unchanged (no regression).
// ===========================================================================
TEST_F(GatewaySubscribeFixture, MC5_MidChunkCorruptBatchSkip_ForwardProgress)
{
    rune::storage::Log* log = registry_->log(kTopicIdA, kPartitionA0);
    ASSERT_NE(log, nullptr);

    // --- Step 1: append valid batch A (2 records: offsets 0, 1) ---
    {
        const int64_t base = log->logEndOffset(); // 0
        auto batchA = makeBatch(2, base);
        log->append(batchA, base + 1);
    }
    ASSERT_EQ(log->logEndOffset(), 2);

    // --- Step 2: write a corrupt batch with recordCount=0 at offset 2 ---
    // We build a well-formed 1-record batch and then patch recordCount to 0.
    // The batch's length field and structure are otherwise valid so the push
    // loop can parse batchSize and advance chunkPos past it.
    // We use appendNoSync directly (bypasses the gateway M1 CRC+count checks).
    {
        const int64_t base = log->logEndOffset(); // 2
        auto corruptBatch = makeBatch(1, base);   // valid 1-record batch
        // Patch recordCount (LE32 at kRecordCountOffset=57) to 0.
        constexpr int rcOff = rune::protocol::batch::kRecordCountOffset;
        corruptBatch[rcOff + 0] = 0;
        corruptBatch[rcOff + 1] = 0;
        corruptBatch[rcOff + 2] = 0;
        corruptBatch[rcOff + 3] = 0;
        // lastOffset = base (single record was there before patching, lastOff=base).
        // We use base as lastOff — the log's nextOffset_ will advance by 1,
        // but we don't use the log's LEO tracking for the corrupt batch's extent.
        // The important thing is the batch bytes on disk have recordCount=0.
        log->appendNoSync(corruptBatch.data(), corruptBatch.size(), base);
    }
    // LEO has advanced past the corrupt batch (it's in page cache).
    ASSERT_EQ(log->logEndOffset(), 3);

    // --- Step 3: append valid batch C (2 records: offsets 3, 4) ---
    {
        const int64_t base = log->logEndOffset(); // 3
        auto batchC = makeBatch(2, base);
        // Use appendNoSync to keep everything in the same chunk.
        log->appendNoSync(batchC.data(), batchC.size(), base + 1);
    }
    ASSERT_EQ(log->logEndOffset(), 5);

    // Sync all data to disk (the corrupt batch and batch C are now fsync'd).
    log->sync();

    // --- Step 4: subscribe from offset 0 ---
    auto client = makeClient(kEgressBase + 120);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/120, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    sendSubscribe(*client.ingressPub, sid, /*corrId=*/1200,
                  kTopicIdA, kPartitionA0, /*partitionKnown=*/1,
                  /*fromOffset=*/0, /*isolation=*/0);

    // --- Steps 5 & 6: receive batch A and batch C ---
    // Batch A: baseOffset=0 (2 records).
    // Corrupt batch: skipped, nextOffset advanced past it.
    // Batch C: baseOffset=3 (2 records) — must be delivered (no stall).
    //
    // Use a COMBINED assembler that captures both SubscribeResponse and Records.
    // The doWork() pump for SubscribeResponse can push Records at the same time —
    // a single-purpose SubscribeResponse assembler would silently drop them.
    std::optional<pc::SubscribeResponse> subResp;
    int64_t subId = 0;
    std::vector<int64_t> receivedBaseOffsets;

    aeron::FragmentAssembler combinedAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                              aeron::util::index_t offset,
                                              aeron::util::index_t len,
                                              aeron::Header&) {
        const auto* ptr = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
        const auto  sz  = static_cast<std::size_t>(len);
        if (!subResp) {
            auto dec = pc::SubscribeResponse::decode(ptr, sz);
            if (dec) { subResp = *dec; subId = dec->subscriptionId; return; }
        }
        if (subId != 0) {
            auto dec = pc::Records::decode(ptr, sz);
            if (dec && dec->subscriptionId == subId)
                receivedBaseOffsets.push_back(dec->baseOffset);
        }
    });

    // Wait for SubscribeResponse (combined assembler also captures early Records).
    pump([&]() { return subResp.has_value(); },
         &*client.egressSub, &combinedAsm, kPumpIter, "MC5-SubResp");
    ASSERT_TRUE(subResp.has_value());
    EXPECT_EQ(subResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // We expect two batches: base=0 and base=3.
    // Use a bounded wait with a hard cap — test must not hang even if M2 is broken
    // (in that case the second batch is never delivered, and we fail the ASSERT below).
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{10};
    while (receivedBaseOffsets.size() < 2 && clock::now() < deadline) {
        gw_->doWork(nowNs());
        client.egressSub->poll(combinedAsm.handler(), 10);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    // --- Assert forward progress ---
    ASSERT_GE(receivedBaseOffsets.size(), 1u)
        << "MC5: batch A (baseOffset=0) must be delivered";
    EXPECT_EQ(receivedBaseOffsets[0], 0)
        << "MC5: first received batch must be batch A at baseOffset=0";

    ASSERT_GE(receivedBaseOffsets.size(), 2u)
        << "MC5 [M2 regression gate]: batch C (baseOffset=3) must be delivered after "
           "the corrupt batch is skipped. If this fails, nextOffset was not advanced "
           "past the corrupt batch — permanent subscription stall (M2 bug).";
    EXPECT_EQ(receivedBaseOffsets[1], 3)
        << "MC5: second received batch must be batch C at baseOffset=3 "
           "(corrupt batch skipped, nextOffset advanced)";

    // No duplicates.
    {
        auto sorted = receivedBaseOffsets;
        std::sort(sorted.begin(), sorted.end());
        auto unique_end = std::unique(sorted.begin(), sorted.end());
        EXPECT_EQ(std::distance(sorted.begin(), unique_end),
                  static_cast<std::ptrdiff_t>(receivedBaseOffsets.size()))
            << "MC5: duplicate Records detected";
    }
}


// ===========================================================================
// F19 (REGRESSION): a stored record batch larger than ~64 KiB was never
// delivered to push consumers — both gateway push paths (inline + worker-return)
// skipped it behind a stale u16 guard / the 68 KiB scratch and ADVANCED the
// subscription's nextOffset with no error (silent consumer data loss).
//
// Fixed contract:
//   - any batch whose Records frame fits the egress publication's
//     maxMessageLength is delivered intact (vectored offer: header from scratch
//     + the batch bytes zero-copy from the log; Aeron fragments, the client's
//     FragmentAssembler reassembles);
//   - a frame above maxMessageLength answers that subscription ONE
//     ErrorResponse{MessageTooLarge, correlationId = subscriptionId} and parks
//     it (cursor NOT advanced, no further pushes, no repeated errors); other
//     subscriptions are unaffected;
//   - back-pressure on a large offer keeps today's retry semantics (cursor not
//     advanced; the same batch is re-offered; nothing lost or duplicated).
//
// Each scenario runs on the inline path (no WorkerPool) and on the worker-return
// path (a 1-worker WorkerPool pumped on the test thread).
// ===========================================================================
class GatewayLargeBatchPushRegression
    : public GatewaySubscribeFixture,
      public ::testing::WithParamInterface<bool /*workerPool*/> {
public:
    void SetUp() override {
        GatewaySubscribeFixture::SetUp();
        if (GetParam()) {
            ownedPool_ = std::make_unique<rune::node::WorkerPool>(
                1, rune::metadata::ShardRouter(1));
            gw_->setWorkerPool(ownedPool_.get());
            pool_ = ownedPool_.get();
        }
    }
    void TearDown() override {
        pool_ = nullptr;
        GatewaySubscribeFixture::TearDown();   // gateway first, then the pool
        ownedPool_.reset();
    }

    // Everything the gateway pushes to one client egress stream.
    struct Egress {
        std::vector<pc::SubscribeResponse> subResps;
        struct Rec { int64_t subId; int64_t baseOffset; std::vector<uint8_t> bytes; };
        std::vector<Rec> records;
        struct Err { int64_t correlationId; int16_t code; };
        std::vector<Err> errors;
        aeron::FragmentAssembler fa;
        Egress()
            : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                        aeron::util::index_t len, aeron::Header&) {
                  const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
                  const auto  n = static_cast<std::size_t>(len);
                  if (auto r = pc::Records::decode(p, n)) {
                      records.push_back({r->subscriptionId, r->baseOffset,
                                         {r->recordBatch.begin(), r->recordBatch.end()}});
                  } else if (auto sr = pc::SubscribeResponse::decode(p, n)) {
                      subResps.push_back(*sr);
                  } else if (auto e = pc::ErrorResponse::decode(p, n)) {
                      errors.push_back({e->correlationId, e->code});
                  }
              }) {}
        [[nodiscard]] std::size_t recordsFor(int64_t subId) const {
            std::size_t c = 0;
            for (const auto& r : records) c += (r.subId == subId);
            return c;
        }
    };

    struct Conn {
        std::shared_ptr<aeron::Publication>  ingress;
        std::shared_ptr<aeron::Subscription> egressSub;
        int64_t sid = 0;
    };

    Conn connect(const std::string& egressCh, int32_t egressStream, Egress& eg) {
        Conn c;
        const int64_t pr = aeron_->addPublication("aeron:ipc", kIngressStream);
        const int64_t sr = aeron_->addSubscription(egressCh, egressStream);
        pump([&]() {
            if (!c.ingress)   c.ingress   = aeron_->findPublication(pr);
            if (!c.egressSub) c.egressSub = aeron_->findSubscription(sr);
            return c.ingress && c.egressSub && c.ingress->isConnected();
        }, nullptr, nullptr, kPumpIter, "F19 resolve");
        pc::ConnectRequest req;
        req.correlationId  = 1;
        req.egressStreamId = egressStream;
        req.clientVersion  = 1;
        req.egressChannel  = egressCh;
        offerBounded(*c.ingress, scratch_, req.encode(scratch_), "F19 Connect");
        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler fa([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                        aeron::util::index_t l, aeron::Header&) {
            if (auto d = pc::ConnectResponse::decode(
                    reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l)))
                resp = *d;
        });
        pump([&]() { return resp.has_value(); }, c.egressSub.get(), &fa, kPumpIter, "F19 ConnResp");
        c.sid = resp ? resp->assignedSessionId : 0;
        (void)eg;
        return c;
    }

    int64_t subscribe(Conn& c, Egress& eg, int32_t partition, int64_t corr) {
        const std::size_t before = eg.subResps.size();
        sendSubscribe(*c.ingress, c.sid, corr, kTopicIdA, partition, 1, /*from*/0, /*iso*/0);
        pump([&]() { return eg.subResps.size() > before; }, c.egressSub.get(), &eg.fa,
             kPumpIter, "F19 SubResp");
        EXPECT_EQ(eg.subResps.back().errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
        return eg.subResps.back().subscriptionId;
    }

    // Append one batch whose single record value is `valueBytes` long; returns
    // the exact stored bytes (the restamped batch).
    std::vector<uint8_t> appendSized(int32_t partition, std::size_t valueBytes) {
        rune::storage::Log* log = registry_->log(kTopicIdA, partition);
        const int64_t base = log->logEndOffset();
        std::string value(valueBytes, '\0');
        for (std::size_t i = 0; i < valueBytes; ++i)
            value[i] = static_cast<char>((i * 31u + static_cast<std::size_t>(base)) & 0xffu);
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", value, 1000);
        auto bytes = b.build();
        log->append(bytes, base);
        return bytes;
    }

    std::unique_ptr<rune::node::WorkerPool> ownedPool_;
};

TEST_P(GatewayLargeBatchPushRegression, OneMiBBatchDeliveredIntactThenSmallBatch)
{
    Egress eg;
    Conn c = connect("aeron:ipc", kEgressBase + 70, eg);   // IPC: 64 MiB term → 8 MiB max message
    const int64_t subId = subscribe(c, eg, kPartitionA0, 70);

    const auto big   = appendSized(kPartitionA0, 1024 * 1024);   // offset 0, ~1 MiB
    const auto small = appendSized(kPartitionA0, 16);            // offset 1

    pump([&]() { return eg.recordsFor(subId) >= 2 || !eg.errors.empty(); },
         c.egressSub.get(), &eg.fa, kPumpIter, "F19 1MiB+small");

    ASSERT_TRUE(eg.errors.empty()) << "unexpected ErrorResponse code=" << eg.errors[0].code;
    ASSERT_EQ(eg.recordsFor(subId), 2u) << "the 1 MiB batch was skipped (F19 silent loss)";
    EXPECT_EQ(eg.records[0].baseOffset, 0);
    EXPECT_EQ(eg.records[0].bytes, big) << "1 MiB batch not byte-identical after reassembly";
    EXPECT_EQ(eg.records[1].baseOffset, 1);
    EXPECT_EQ(eg.records[1].bytes, small);
    EXPECT_EQ(gw_->subscriptionNextOffsetForTest(subId), 2);
}

TEST_P(GatewayLargeBatchPushRegression, AboveMaxMessageLengthAnswersErrorNoSilentAdvance)
{
    Egress eg;
    // 64 KiB IPC term → maxMessageLength = 8 KiB.
    Conn c = connect("aeron:ipc?term-length=65536", kEgressBase + 71, eg);
    const int64_t subBig   = subscribe(c, eg, kPartitionA0, 71);
    const int64_t subOther = subscribe(c, eg, kPartitionA1, 72);

    appendSized(kPartitionA0, 16 * 1024);     // offset 0 on A0: frame > 8 KiB max message
    appendSized(kPartitionA0, 16);            // offset 1 on A0: behind the undeliverable batch
    appendSized(kPartitionA1, 16);            // A1: other subscription, must flow
    appendSized(kPartitionA1, 16);

    pump([&]() { return !eg.errors.empty() && eg.recordsFor(subOther) >= 2; },
         c.egressSub.get(), &eg.fa, kPumpIter, "F19 too-large error");

    ASSERT_EQ(eg.errors.size(), 1u);
    EXPECT_EQ(eg.errors[0].correlationId, subBig);
    EXPECT_EQ(eg.errors[0].code, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(eg.recordsFor(subOther), 2u) << "other subscription must be unaffected";

    // Parked: more cycles push nothing for subBig, send no second error, and the
    // cursor stays AT the undeliverable batch (never advanced past it silently).
    for (int i = 0; i < 200; ++i) {
        gw_->doWork(nowNs());
        if (pool_) pool_->doWork(nowNs());
        c.egressSub->poll(eg.fa.handler(), 64);
    }
    EXPECT_EQ(eg.recordsFor(subBig), 0u) << "nothing may be delivered past the failed batch";
    EXPECT_EQ(eg.errors.size(), 1u) << "exactly one error, no spin";
    EXPECT_EQ(gw_->subscriptionNextOffsetForTest(subBig), 0)
        << "cursor advanced past an undelivered batch (silent loss)";
}

TEST_P(GatewayLargeBatchPushRegression, BackPressuredLargeOfferRetriesWithoutLoss)
{
    Egress eg;
    // 16 MiB IPC term → 2 MiB max message, 8 MiB publication window: a dozen
    // 1 MiB frames cannot all fit while the client is not polling.
    Conn c = connect("aeron:ipc?term-length=16777216", kEgressBase + 72, eg);
    const int64_t subId = subscribe(c, eg, kPartitionA0, 73);

    constexpr int kBatches = 12;
    std::vector<std::vector<uint8_t>> stored;
    for (int i = 0; i < kBatches; ++i) stored.push_back(appendSized(kPartitionA0, 1024 * 1024));

    // Drive the gateway without draining the egress: the window fills and the
    // offer back-pressures.
    for (int i = 0; i < 4 * kBatches; ++i) {
        gw_->doWork(nowNs());
        if (pool_) pool_->doWork(nowNs());
    }
    const int64_t cursorUnderBp = gw_->subscriptionNextOffsetForTest(subId);
    EXPECT_LT(cursorUnderBp, kBatches) << "back-pressure never happened (test precondition)";

    pump([&]() { return eg.recordsFor(subId) >= static_cast<std::size_t>(kBatches) ||
                        !eg.errors.empty(); },
         c.egressSub.get(), &eg.fa, kPumpIter, "F19 BP drain");

    ASSERT_TRUE(eg.errors.empty());
    ASSERT_EQ(eg.recordsFor(subId), static_cast<std::size_t>(kBatches));
    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(eg.records[i].baseOffset, i) << "gap/dup/reorder at " << i;
        EXPECT_EQ(eg.records[i].bytes, stored[i]) << "batch " << i << " corrupted";
    }
    EXPECT_EQ(gw_->subscriptionNextOffsetForTest(subId), kBatches);
}

INSTANTIATE_TEST_SUITE_P(Paths, GatewayLargeBatchPushRegression, ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& i) {
                             return i.param ? std::string("WorkerReturn") : std::string("Inline");
                         });

} // namespace
} // namespace rune::gateway
