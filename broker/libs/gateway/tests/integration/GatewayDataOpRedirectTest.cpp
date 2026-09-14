// GatewayDataOpRedirectTest.cpp — GB-4: Produce/Subscribe data-op REDIRECT to leader.
//
// Tests (injected leaderView — no real cluster needed):
//   DR-1: Follower + known endpoint → Produce → ErrorResponse{Redirect, endpoint}, engine untouched.
//   DR-2: Follower + known endpoint → Subscribe → ErrorResponse{Redirect, endpoint}, no subscription.
//   DR-3: Follower + unknown endpoint (empty) → Produce → ErrorResponse{NotLeader}.
//   DR-4: Follower + unknown endpoint (empty) → Subscribe → ErrorResponse{NotLeader}.
//   DR-5: Leader → Produce served (append OK, ProduceResponse{OK}).
//   DR-6: Leader → Subscribe served (SubscribeResponse{OK}).
//   DR-7: Connect accepted locally even when NOT leader (control degrades-to-accept).
//   DR-8: Ping accepted locally even when NOT leader (control degrades-to-accept).

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
    auto name = std::string("rune_gwdrop_") + std::string(tag) +
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

// ---------------------------------------------------------------------------
// Helper: build a minimal valid RecordBatch (1 record) for Produce messages.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch()
{
    // Build a proper 1-record batch via RecordBatchBuilder so the CRC +
    // lastOffsetDelta/recordCount are valid — the GB-5 M1 fix now validates both
    // on the produce path (ensureValid + recordCount==lastOffsetDelta+1), so a
    // hand-assembled batch with a zero CRC is (correctly) rejected as InvalidBatch.
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    b.append("k", "v", 1000LL);
    return b.build();
}

static TopicSpec makeSpec(std::string name, int32_t partCount, int32_t id)
{
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = id;
    return ts;
}

// ---------------------------------------------------------------------------
// GatewayDataOpRedirectFixture — one embedded driver + Aeron client per test.
// Gateway built with injected leaderView hook.
// ---------------------------------------------------------------------------
class GatewayDataOpRedirectFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 700;
    static constexpr int32_t kEgressBase    = 800;
    static constexpr int32_t kThisNodeId    = 42;
    static constexpr int     kPumpIter      = 50'000;

    const std::string kLeaderEndpoint = "aeron:udp?endpoint=leader:40200";

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("dr_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        gw_.reset();
        topicReg_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // Build the gateway with the given leaderView hook.
    void buildGateway(std::function<LeaderView()> leaderView,
                      bool withTopics = false)
    {
        if (withTopics) {
            std::vector<TopicSpec> specs = {makeSpec("test", 1, 0)};
            topicReg_ = std::make_unique<TopicRegistry>(specs, tmpDir_->path.string());
        }

        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId,
            ClientGateway::kDefaultSessionTimeoutNs,
            std::move(leaderView));

        if (topicReg_) {
            gw_->setTopicRegistry(topicReg_.get());
        }
    }

    // -----------------------------------------------------------------------
    // TestClient
    // -----------------------------------------------------------------------
    struct TestClient {
        std::shared_ptr<aeron::Publication>   ingressPub;
        std::shared_ptr<aeron::Subscription>  egressSub;
        int32_t                               egressStreamId;
        std::string                           egressChannel{"aeron:ipc"};
    };

    TestClient makeClient(int32_t egressStreamId) {
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription("aeron:ipc", egressStreamId);

        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;

        pump([&]() {
            if (!pub) pub = aeron_->findPublication(pubRegId);
            if (!sub) sub = aeron_->findSubscription(subRegId);
            return pub && sub;
        }, nullptr, nullptr, kPumpIter, "makeClient");

        return TestClient{pub, sub, egressStreamId};
    }

    // Connect the client and return the assigned sessionId.
    int64_t connectClient(TestClient& client, int64_t correlationId = 1) {
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = correlationId;
        req.egressStreamId = client.egressStreamId;
        req.clientVersion  = 1;
        req.egressChannel  = client.egressChannel;
        uint8_t sc[512] = {};
        const std::size_t len = req.encode(sc);
        offerBounded(*client.ingressPub, sc, len, "Connect");

        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t flen,
                                          aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(flen));
            if (dec) resp = *dec;
        });

        pump([&]() { return resp.has_value(); },
             &*client.egressSub, &asm_, kPumpIter, "ConnectResponse");

        EXPECT_TRUE(resp.has_value()) << "Expected ConnectResponse";
        if (!resp.has_value()) return -1;
        EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
        return resp->assignedSessionId;
    }

    static void offerBounded(aeron::Publication& pub,
                             const uint8_t* buf, std::size_t len,
                             std::string_view label = "offer") {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));
        bool sent = false;
        for (int i = 0; i < kPumpIter && !sent; ++i) {
            if (pub.offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0)
                sent = true;
            else
                std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    template <typename DoneFn>
    void pump(DoneFn&& done,
              aeron::Subscription*       clientSub = nullptr,
              aeron::FragmentAssembler*  clientAsm = nullptr,
              int                        maxIter   = kPumpIter,
              std::string_view           label     = "pump") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};

        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (clientSub && clientAsm)
                clientSub->poll(clientAsm->handler(), 10);
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(done()) << "pump timed out: " << label;
    }

    std::unique_ptr<ClientGateway>  gw_;
    std::unique_ptr<TopicRegistry>  topicReg_;
    std::shared_ptr<aeron::Aeron>   aeron_;

private:
    void startDriver() {
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
                aeron_driver_main_idle_strategy(driver_,
                                                aeron_driver_main_do_work(driver_));
            }
        });

        const std::string cncPath = aeronDir + "/cnc.dat";
        for (int i = 0; i < 400; ++i) {
            aeron_driver_main_do_work(driver_);
            if (fs::exists(cncPath)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC not created within timeout";
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            static constexpr int kDrainMs = 50;
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds{kDrainMs};
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

    std::unique_ptr<TempDir>   tmpDir_;
    aeron_driver_context_t*    driverCtx_   = nullptr;
    aeron_driver_t*            driver_      = nullptr;
    std::atomic<bool>          driverRunning_{false};
    std::thread                driverThread_;
};

// ===========================================================================
// DR-1: Follower + known endpoint → Produce → ErrorResponse{Redirect, endpoint}, engine untouched.
//
// Strategy: Connect as leader (so session is established), then flip to follower
// before sending Produce. This tests the data-op redirect path correctly.
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Produce_Redirect)
{
    // Start as leader so Connect succeeds, then flip to follower for data ops.
    std::atomic<bool> isLeader{true};
    buildGateway([&]() -> LeaderView {
        if (isLeader.load(std::memory_order_acquire)) {
            return LeaderView{/*isLeader=*/true, ""};
        }
        return LeaderView{/*isLeader=*/false, kLeaderEndpoint};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 1);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    // Connect as leader.
    const int64_t sid = connectClient(client, /*correlationId=*/10);
    ASSERT_GT(sid, 0) << "Connect must succeed when leader";
    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Flip to follower before the data op.
    isLeader.store(false, std::memory_order_release);

    // Send Produce.
    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 20;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    auto batch = makeBatch();
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    uint8_t prodBuf[4096] = {};
    const std::size_t prodLen = prod.encode(prodBuf);
    offerBounded(*client.ingressPub, prodBuf, prodLen, "Produce");

    // Expect ErrorResponse{Redirect}.
    std::optional<pc::ErrorResponse> errResp;
    std::string                      errMsg;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ErrorResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) {
            errMsg = std::string(dec->message);
            dec->message = errMsg;
            errResp = *dec;
        }
    });

    pump([&]() { return errResp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ErrorResponse(Redirect,Produce)");

    ASSERT_TRUE(errResp.has_value()) << "Follower must redirect Produce";
    EXPECT_EQ(errResp->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(errMsg, kLeaderEndpoint) << "Redirect message must carry leader endpoint";
    EXPECT_EQ(errResp->correlationId, prod.correlationId) << "correlationId echoed";
    EXPECT_EQ(errResp->sessionId, sid) << "sessionId echoed";
    // Engine was not touched — log end offset should still be 0.
    auto* log = topicReg_->log(0, 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 0) << "Engine must not be touched on redirect";
}

// ===========================================================================
// DR-2: Follower + known endpoint → Subscribe → ErrorResponse{Redirect, endpoint}, no subscription.
//
// Strategy: Connect as leader (session established), then flip to follower.
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Subscribe_Redirect)
{
    std::atomic<bool> isLeader{true};
    buildGateway([&]() -> LeaderView {
        if (isLeader.load(std::memory_order_acquire)) {
            return LeaderView{/*isLeader=*/true, ""};
        }
        return LeaderView{/*isLeader=*/false, kLeaderEndpoint};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 2);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    const int64_t sid = connectClient(client, /*correlationId=*/11);
    ASSERT_GT(sid, 0);

    // Flip to follower before the data op.
    isLeader.store(false, std::memory_order_release);

    // Send Subscribe.
    pc::Subscribe sub;
    sub.sessionId      = sid;
    sub.correlationId  = 21;
    sub.topicId        = 0;
    sub.partition      = 0;
    sub.partitionKnown = 1;
    sub.fromOffset     = 0;
    sub.isolation      = 0;

    uint8_t subBuf[512] = {};
    const std::size_t subLen = sub.encode(subBuf);
    offerBounded(*client.ingressPub, subBuf, subLen, "Subscribe");

    // Expect ErrorResponse{Redirect}.
    std::optional<pc::ErrorResponse> errResp;
    std::string                      errMsg;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ErrorResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) {
            errMsg = std::string(dec->message);
            dec->message = errMsg;
            errResp = *dec;
        }
    });

    pump([&]() { return errResp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ErrorResponse(Redirect,Subscribe)");

    ASSERT_TRUE(errResp.has_value()) << "Follower must redirect Subscribe";
    EXPECT_EQ(errResp->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(errMsg, kLeaderEndpoint);
    EXPECT_EQ(errResp->correlationId, sub.correlationId);
    // No subscription must be registered.
    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "No subscription on redirect";
}

// ===========================================================================
// DR-3: Follower + unknown endpoint (empty) → Produce → ErrorResponse{NotLeader}.
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Produce_NotLeader_UnknownEndpoint)
{
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, /*leaderClientEndpoint=*/""};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 3);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    const int64_t sid = connectClient(client, /*correlationId=*/12);
    ASSERT_GT(sid, 0);

    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 22;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    auto batch = makeBatch();
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    uint8_t prodBuf[4096] = {};
    offerBounded(*client.ingressPub, prodBuf, prod.encode(prodBuf), "Produce(NotLeader)");

    std::optional<pc::ErrorResponse> errResp;
    std::string errMsg;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ErrorResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) {
            errMsg = std::string(dec->message);
            dec->message = errMsg;
            errResp = *dec;
        }
    });

    pump([&]() { return errResp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ErrorResponse(NotLeader,Produce)");

    ASSERT_TRUE(errResp.has_value());
    EXPECT_EQ(errResp->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_TRUE(errMsg.empty()) << "NotLeader response must have empty message";

    // Engine must not be touched.
    auto* log = topicReg_->log(0, 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 0) << "Engine must not be touched on NotLeader";
}

// ===========================================================================
// DR-4: Follower + unknown endpoint (empty) → Subscribe → ErrorResponse{NotLeader}.
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Subscribe_NotLeader_UnknownEndpoint)
{
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, /*leaderClientEndpoint=*/""};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 4);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    const int64_t sid = connectClient(client, /*correlationId=*/13);
    ASSERT_GT(sid, 0);

    pc::Subscribe sub;
    sub.sessionId      = sid;
    sub.correlationId  = 23;
    sub.topicId        = 0;
    sub.partition      = 0;
    sub.partitionKnown = 1;
    sub.fromOffset     = 0;
    sub.isolation      = 0;

    uint8_t subBuf[512] = {};
    offerBounded(*client.ingressPub, subBuf, sub.encode(subBuf), "Subscribe(NotLeader)");

    std::optional<pc::ErrorResponse> errResp;
    std::string errMsg;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ErrorResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) {
            errMsg = std::string(dec->message);
            dec->message = errMsg;
            errResp = *dec;
        }
    });

    pump([&]() { return errResp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ErrorResponse(NotLeader,Subscribe)");

    ASSERT_TRUE(errResp.has_value());
    EXPECT_EQ(errResp->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "No subscription registered on NotLeader";
}

// ===========================================================================
// DR-5: Leader → Produce served (ProduceResponse{OK}).
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Leader_Produce_Served)
{
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/true, ""};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 5);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    const int64_t sid = connectClient(client, /*correlationId=*/14);
    ASSERT_GT(sid, 0);

    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 24;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    auto batch = makeBatch();
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    uint8_t prodBuf[4096] = {};
    offerBounded(*client.ingressPub, prodBuf, prod.encode(prodBuf), "Produce(Leader)");

    std::optional<pc::ProduceResponse> resp;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ProduceResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) resp = *dec;
    });

    pump([&]() { return resp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ProduceResponse(OK)");

    ASSERT_TRUE(resp.has_value()) << "Leader must serve Produce";
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(resp->baseOffset, 0);

    // Engine WAS touched.
    auto* log = topicReg_->log(0, 0);
    ASSERT_NE(log, nullptr);
    EXPECT_GT(log->logEndOffset(), 0) << "Engine must have appended data";
}

// ===========================================================================
// DR-6: Leader → Subscribe served (SubscribeResponse{OK}).
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Leader_Subscribe_Served)
{
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/true, ""};
    }, /*withTopics=*/true);

    auto client = makeClient(kEgressBase + 6);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    const int64_t sid = connectClient(client, /*correlationId=*/15);
    ASSERT_GT(sid, 0);

    pc::Subscribe sub;
    sub.sessionId      = sid;
    sub.correlationId  = 25;
    sub.topicId        = 0;
    sub.partition      = 0;
    sub.partitionKnown = 1;
    sub.fromOffset     = 0;
    sub.isolation      = 0;

    uint8_t subBuf[512] = {};
    offerBounded(*client.ingressPub, subBuf, sub.encode(subBuf), "Subscribe(Leader)");

    std::optional<pc::SubscribeResponse> resp;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::SubscribeResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) resp = *dec;
    });

    pump([&]() { return resp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "SubscribeResponse(OK)");

    ASSERT_TRUE(resp.has_value()) << "Leader must serve Subscribe";
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GT(resp->subscriptionId, 0);
    EXPECT_EQ(gw_->subscriptionCount(), 1u);
}

// ===========================================================================
// DR-7: Connect accepted locally even when NOT leader (control degrades-to-accept).
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Connect_DegradesToAccept)
{
    // isLeader=false, leaderClientEndpoint="" → Connect degrades to local accept.
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, ""};
    });

    auto client = makeClient(kEgressBase + 7);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    pc::ConnectRequest req;
    req.sessionId      = 0;
    req.correlationId  = 77;
    req.egressStreamId = client.egressStreamId;
    req.clientVersion  = 1;
    req.egressChannel  = client.egressChannel;
    uint8_t sc[512] = {};
    offerBounded(*client.ingressPub, sc, req.encode(sc), "Connect(Follower)");

    std::optional<pc::ConnectResponse> resp;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t flen,
                                      aeron::Header&) {
        auto dec = pc::ConnectResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) resp = *dec;
    });

    pump([&]() { return resp.has_value(); },
         &*client.egressSub, &asm_, kPumpIter, "ConnectResponse(DegradedAccept)");

    ASSERT_TRUE(resp.has_value()) << "Connect must be accepted even on follower (degrade-to-accept)";
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GT(resp->assignedSessionId, 0);
    EXPECT_EQ(gw_->sessionCount(), 1u);
}

// ===========================================================================
// DR-8: Ping accepted locally even when NOT leader (control degrades-to-accept).
// ===========================================================================
TEST_F(GatewayDataOpRedirectFixture, Follower_Ping_DegradesToAccept)
{
    // Start as NOT leader.
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, ""};
    });

    auto client = makeClient(kEgressBase + 8);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    // Connect (must succeed — degrade-to-accept).
    const int64_t sid = connectClient(client, /*correlationId=*/88);
    ASSERT_GT(sid, 0);

    // Send Ping.
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 99;
    ping.echo          = "ctrl-still-works";
    uint8_t pingBuf[256] = {};
    offerBounded(*client.ingressPub, pingBuf, ping.encode(pingBuf), "Ping(Follower)");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler pingAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                         aeron::util::index_t offset,
                                         aeron::util::index_t flen,
                                         aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(flen));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); },
         &*client.egressSub, &pingAsm, kPumpIter, "Pong(Follower)");

    ASSERT_TRUE(pong.has_value()) << "Ping must still get Pong on a follower";
    EXPECT_EQ(pong->correlationId, 99);
    EXPECT_EQ(pongEcho, "ctrl-still-works");
}

} // namespace
} // namespace rune::gateway
