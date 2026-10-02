// ProduceIdempotentTest.cpp — Integration tests for TX-2: idempotent-producer
// dedup wired into ClientGateway::handleProduce.
//
// Tests:
//   1. DuplicateBatchReturnsPriorOffsetNoReappend — exact retry → prior offset,
//      LEO unchanged.
//   2. OutOfOrderRejected — sequence gap → OutOfOrderSequence error.
//   3. OldEpochFenced    — zombie (lower epoch) → InvalidProducerEpoch error.
//   4. LegacyProducerPassthrough — producerId==-1 → normal append (no dedup).
//
// Harness: embedded Aeron IPC loopback, mirrors GatewayProduceTest.cpp structure.
// Single-writer duty-cycle: gw_->doWork() called from the test thread.

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
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
// Unique temp-path generator.
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "")
{
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwidm_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static TopicSpec makeSpec(std::string name, int32_t pc, int32_t id)
{
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = pc;
    ts.topicId        = id;
    return ts;
}

// Build a RecordBatch with `recordCount` records, baseOffset placeholder = 0.
// An idempotent batch's header carries the same producer identity as the
// Produce frame (the broker rejects a mismatch — EOS-DUP defense).
static std::vector<uint8_t> makeBatch(int32_t recordCount, int64_t pid = -1,
                                      int16_t epoch = -1, int32_t seq = -1)
{
    rune::protocol::RecordBatchBuilder b(0, 1000LL);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq); }
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// ===========================================================================
// ProduceIdempotentFixture
// ===========================================================================
class ProduceIdempotentFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream  = 601;
    static constexpr int32_t kEgressBase     = 700;
    static constexpr int32_t kThisNodeId     = 3;
    static constexpr int     kPumpIter       = 50'000;

    static constexpr int32_t kTopicId    = 0;   // "orders", 2 partitions (topicId used as vector index)
    static constexpr int32_t kPartition0 = 0;

    void SetUp() override
    {
        tmpDir_  = std::make_unique<TempDir>("fix_");
        dataDir_ = std::make_unique<TempDir>("data_");
        fs::create_directories(dataDir_->path);

        startDriver();
        connectAeron();

        std::vector<TopicSpec> specs = { makeSpec("orders", 2, 0) };  // topicId=0 = vector index 0
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
    // TestClient
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

    template <typename DoneFn>
    void pump(DoneFn&& done,
              aeron::Subscription* clientSub = nullptr,
              aeron::FragmentAssembler* clientAsm = nullptr,
              int maxIter = kPumpIter,
              std::string_view label = "pump")
    {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (clientSub && clientAsm) {
                clientSub->poll(clientAsm->handler(), 10);
            }
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(done()) << "pump timed out: " << label;
    }

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

    // Send a Produce with explicit idempotent fields.
    void sendIdempotentProduce(aeron::Publication& ingressPub,
                               int64_t sessionId, int64_t correlationId,
                               int32_t topicId, int32_t partition,
                               int64_t producerId, int16_t producerEpoch,
                               int32_t baseSequence,
                               std::span<const uint8_t> recordBatch,
                               std::string_view label = "IdempotentProduce")
    {
        pc::Produce prod;
        prod.sessionId      = sessionId;
        prod.correlationId  = correlationId;
        prod.topicId        = topicId;
        prod.partition      = partition;
        prod.partitionKnown = 1;
        prod.acks           = 0;
        prod.producerId     = producerId;
        prod.producerEpoch  = producerEpoch;
        prod.baseSequence   = baseSequence;
        prod.key            = {};
        prod.recordBatch    = recordBatch;

        const std::size_t needed = prod.encodedSize();
        ASSERT_LE(needed, sizeof(largeScratch_)) << "Frame too large for largeScratch_";
        const std::size_t encoded = prod.encode(largeScratch_);
        offerBounded(ingressPub, largeScratch_, encoded, label);
    }

    std::optional<pc::ProduceResponse> recvProduceResponse(
        aeron::Subscription& egressSub, std::string_view label = "ProduceResponse")
    {
        std::optional<pc::ProduceResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
            auto dec = pc::ProduceResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) resp = *dec;
        });
        pump([&]() { return resp.has_value(); },
             &egressSub, &asm_, kPumpIter, label);
        return resp;
    }

    static constexpr std::size_t kScratch = 68 * 1024;
    uint8_t scratch_[kScratch]      = {};
    uint8_t largeScratch_[kScratch] = {};

    std::unique_ptr<ClientGateway> gw_;
    std::unique_ptr<TopicRegistry> registry_;
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
// TEST 1: DuplicateBatchReturnsPriorOffsetNoReappend
//
// First produce (pid=9, epoch=0, baseSeq=0, 3 records) → OK, baseOffset=0.
// Second produce (exact retry: same pid/epoch/baseSeq/records) → OK,
//   baseOffset == first.baseOffset (prior offset returned), LEO unchanged.
// ===========================================================================
TEST_F(ProduceIdempotentFixture, ProduceIdempotent_DuplicateBatchReturnsPriorOffsetNoReappend)
{
    auto client = makeClient(kEgressBase + 1);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/1, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    constexpr int32_t kRecords = 3;
    auto batch = makeBatch(kRecords, /*pid=*/9, /*epoch=*/0, /*seq=*/0);
    std::span<const uint8_t> batchSpan(batch.data(), batch.size());

    // First produce (idempotent: pid=9, epoch=0, baseSeq=0).
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/10,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/0, /*baseSeq=*/0,
                          batchSpan, "FirstProduce");

    auto r1 = recvProduceResponse(*client.egressSub, "FirstProduceResponse");
    ASSERT_TRUE(r1.has_value()) << "No response for first produce";
    EXPECT_EQ(r1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r1->baseOffset, 0);

    rune::storage::Log* log = registry_->log(kTopicId, kPartition0);
    ASSERT_NE(log, nullptr);
    const int64_t leo1 = log->logEndOffset();
    EXPECT_EQ(leo1, static_cast<int64_t>(kRecords));

    // Exact retry — same (pid, epoch, baseSeq, batch content).
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/11,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/0, /*baseSeq=*/0,
                          batchSpan, "RetryProduce");

    auto r2 = recvProduceResponse(*client.egressSub, "RetryProduceResponse");
    ASSERT_TRUE(r2.has_value()) << "No response for retry produce";
    EXPECT_EQ(r2->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r2->baseOffset, r1->baseOffset);            // prior offset
    EXPECT_EQ(log->logEndOffset(), leo1);                 // nothing re-appended
}

// ===========================================================================
// TEST 2: OutOfOrderRejected
//
// Produce (pid=9, epoch=0, baseSeq=0, 3 records) → OK.
// Produce (pid=9, epoch=0, baseSeq=10, 3 records) → gap → OutOfOrderSequence.
// ===========================================================================
TEST_F(ProduceIdempotentFixture, ProduceIdempotent_OutOfOrderRejected)
{
    auto client = makeClient(kEgressBase + 2);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/2, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    constexpr int32_t kRecords = 3;
    auto batch = makeBatch(kRecords, /*pid=*/9, /*epoch=*/0, /*seq=*/0);
    std::span<const uint8_t> batchSpan(batch.data(), batch.size());
    auto gapBatch = makeBatch(kRecords, /*pid=*/9, /*epoch=*/0, /*seq=*/10);
    std::span<const uint8_t> gapSpan(gapBatch.data(), gapBatch.size());

    // First produce OK.
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/20,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/0, /*baseSeq=*/0,
                          batchSpan, "FirstProduce");
    auto r1 = recvProduceResponse(*client.egressSub, "OOSFirstResponse");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // Gap: next expected is seq 3, but we send seq 10.
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/21,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/0, /*baseSeq=*/10,
                          gapSpan, "OOSProduce");
    auto r2 = recvProduceResponse(*client.egressSub, "OOSResponse");
    ASSERT_TRUE(r2.has_value()) << "No response for OOS produce";
    EXPECT_EQ(r2->errorCode,
              static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence));
}

// ===========================================================================
// TEST 3: OldEpochFenced
//
// Produce (pid=9, epoch=1, baseSeq=0, 1 record) → OK (epoch 1 established).
// Produce (pid=9, epoch=0, baseSeq=1, 1 record) → zombie → InvalidProducerEpoch.
// ===========================================================================
TEST_F(ProduceIdempotentFixture, ProduceIdempotent_OldEpochFenced)
{
    auto client = makeClient(kEgressBase + 3);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/3, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    auto batch1 = makeBatch(1, /*pid=*/9, /*epoch=*/1, /*seq=*/0);
    std::span<const uint8_t> batchSpan1(batch1.data(), batch1.size());

    // Produce with epoch=1 (higher epoch). Expected seq=0 for new session.
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/30,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/1, /*baseSeq=*/0,
                          batchSpan1, "EpochOneProduce");
    auto r1 = recvProduceResponse(*client.egressSub, "EpochOneResponse");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // Zombie: lower epoch=0 attempt.
    auto batch2 = makeBatch(1, /*pid=*/9, /*epoch=*/0, /*seq=*/1);
    std::span<const uint8_t> batchSpan2(batch2.data(), batch2.size());
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/31,
                          kTopicId, kPartition0,
                          /*pid=*/9, /*epoch=*/0, /*baseSeq=*/1,
                          batchSpan2, "ZombieProduce");
    auto r2 = recvProduceResponse(*client.egressSub, "FencedResponse");
    ASSERT_TRUE(r2.has_value()) << "No response for zombie produce";
    EXPECT_EQ(r2->errorCode,
              static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch));
}

// ===========================================================================
// TEST 4: LegacyProducerPassthrough
//
// producerId==-1 → legacy path: appends normally without any dedup check.
// ===========================================================================
TEST_F(ProduceIdempotentFixture, ProduceIdempotent_LegacyProducerPassthrough)
{
    auto client = makeClient(kEgressBase + 4);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/4, scratch_);
    const int64_t sid = connResp.assignedSessionId;

    auto batch = makeBatch(2);
    std::span<const uint8_t> batchSpan(batch.data(), batch.size());

    // Two produces with pid==-1: both should append (no dedup).
    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/40,
                          kTopicId, kPartition0,
                          /*pid=*/-1, /*epoch=*/0, /*baseSeq=*/-1,
                          batchSpan, "LegacyProduce1");
    auto r1 = recvProduceResponse(*client.egressSub, "LegacyResponse1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r1->baseOffset, 0);

    sendIdempotentProduce(*client.ingressPub, sid, /*corrId=*/41,
                          kTopicId, kPartition0,
                          /*pid=*/-1, /*epoch=*/0, /*baseSeq=*/-1,
                          batchSpan, "LegacyProduce2");
    auto r2 = recvProduceResponse(*client.egressSub, "LegacyResponse2");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r2->baseOffset, 2);  // advanced by 2 records from first batch

    rune::storage::Log* log = registry_->log(kTopicId, kPartition0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 4);  // 2 batches × 2 records
}

// Review I3: the no-worker-pool INLINE path's PSM is rebuilt only on a node-
// level leadership change, so it refuses a KNOWN-OLD duplicate (older than the
// window) with OutOfOrderSequence instead of answering OK with an unknown
// offset; an in-window retransmit is still answered with its original offset.
TEST_F(ProduceIdempotentFixture, IdempotenceWindowRegression_InlinePathKnownOldDuplicateIsOutcomeUnknown)
{
    auto client = makeClient(kEgressBase + 5);
    auto connResp = doConnect(*client.ingressPub, *client.egressSub,
                              client.egressStreamId, client.egressChannel,
                              /*correlationId=*/5, scratch_);
    const int64_t sid = connResp.assignedSessionId;
    const int n = rune::protocol::client::kIdempotentMaxInFlightBatches + 2;
    std::vector<std::vector<uint8_t>> batches;
    for (int i = 0; i < n; ++i) batches.push_back(makeBatch(1, /*pid=*/12, /*epoch=*/0, /*seq=*/i));
    for (int i = 0; i < n; ++i) {
        sendIdempotentProduce(*client.ingressPub, sid, 500 + i, kTopicId, kPartition0, 12, 0, i,
                              std::span<const uint8_t>(batches[i]), "Fill");
        auto r = recvProduceResponse(*client.egressSub, "FillResponse");
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::OK)) << i;
    }
    sendIdempotentProduce(*client.ingressPub, sid, 600, kTopicId, kPartition0, 12, 0, 0,
                          std::span<const uint8_t>(batches[0]), "KnownOld");
    auto old = recvProduceResponse(*client.egressSub, "KnownOldResponse");
    ASSERT_TRUE(old.has_value());
    // The client's OOS contract (Kafka): OOS means "NOT appended" and makes the
    // client re-sequence (re-stamp under a new epoch → a stored batch written
    // twice).  This path cannot vouch for the sequence, so it answers an
    // outcome-UNKNOWN error (Internal), never OOS and never OK -1.
    EXPECT_NE(old->errorCode, static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence));
    EXPECT_EQ(old->errorCode, static_cast<int16_t>(pc::ErrorCode::Internal));
    sendIdempotentProduce(*client.ingressPub, sid, 601, kTopicId, kPartition0, 12, 0, n - 1,
                          std::span<const uint8_t>(batches[n - 1]), "InWindow");
    auto dup = recvProduceResponse(*client.egressSub, "InWindowResponse");
    ASSERT_TRUE(dup.has_value());
    EXPECT_EQ(dup->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(dup->baseOffset, n - 1);
    EXPECT_EQ(registry_->log(kTopicId, kPartition0)->logEndOffset(), n);
}

} // namespace
} // namespace rune::gateway
