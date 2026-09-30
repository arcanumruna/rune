// GatewayRedirectTest.cpp — BR-5 Part 2: leader-awareness REDIRECT tests.
//
// Scenarios:
//  BR5R-1: Follower redirects — Connect receives ErrorResponse{Redirect, leaderEndpoint},
//          correlationId echoed, no session created.
//  BR5R-2: Leader accepts — Connect receives ConnectResponse{OK}, session created.
//  BR5R-3: Leader-unknown degrades to accept — empty leaderEndpoint + isLeader=false
//          → Connect accepted locally → ConnectResponse{OK}.
//  BR5R-4: Existing session unaffected by leadership flip — connect as leader, flip to
//          follower; Ping on the existing session still gets Pong.

#include <rune/gateway/ClientGateway.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter, distinct prefix)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwrd_") + std::string(tag) +
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

static int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// GatewayRedirectFixture — one embedded driver + one Aeron client per test.
// The leaderView_ hook is injected per-test into a separate gateway instance.
// ---------------------------------------------------------------------------
class GatewayRedirectFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 501;
    static constexpr int32_t kEgressBase    = 600;
    static constexpr int32_t kThisNodeId    = 11;
    static constexpr int     kPumpIter      = 50'000;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("rd_");
        startDriver();
        connectAeron();
        // Gateway is built in each test with the appropriate leaderView hook.
    }

    void TearDown() override {
        gw_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // Build the gateway with the given leaderView hook.
    void buildGateway(std::function<LeaderView()> leaderView) {
        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId,
            ClientGateway::kDefaultSessionTimeoutNs,
            std::move(leaderView));
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

    std::unique_ptr<ClientGateway> gw_;
    std::shared_ptr<aeron::Aeron>  aeron_;

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

        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
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
// BR5R-1: Follower redirects.
//
// Gateway with isLeader=false, leaderEndpoint="aeron:ipc" (non-empty).
// Client sends Connect → expects ErrorResponse{code=Redirect, message==leaderEndpoint}.
// correlationId must be echoed; sessionCount==0 (no session created).
// ===========================================================================
TEST_F(GatewayRedirectFixture, Follower_Redirects_Connect) {
    const std::string leaderEndpoint = "aeron:ipc";

    buildGateway([&]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, leaderEndpoint};
    });

    auto client = makeClient(kEgressBase + 1);

    // Wait for ingress pub to connect.
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    // Send ConnectRequest.
    pc::ConnectRequest req;
    req.sessionId       = 0;
    req.correlationId   = 42;
    req.egressStreamId  = client.egressStreamId;
    req.clientVersion   = 1;
    req.egressChannel   = client.egressChannel;
    uint8_t sc[512] = {};
    const std::size_t len = req.encode(sc);
    offerBounded(*client.ingressPub, sc, len, "FollowerConnect");

    // Listen for ErrorResponse on the egress subscription.
    std::optional<pc::ErrorResponse> errResp;
    std::string                      errMsg;   // stable storage for message string_view
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
         &*client.egressSub, &asm_, kPumpIter, "ErrorResponse(Redirect)");

    ASSERT_TRUE(errResp.has_value()) << "Follower must return ErrorResponse";
    EXPECT_EQ(errResp->code,
              static_cast<int16_t>(pc::ErrorCode::Redirect))
        << "code must be Redirect";
    EXPECT_EQ(errMsg, leaderEndpoint)
        << "message must carry the leader client endpoint";
    EXPECT_EQ(errResp->correlationId, req.correlationId)
        << "correlationId must be echoed";
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "No session must be created on redirect";
}

// ===========================================================================
// BR5R-2: Leader accepts — Connect receives ConnectResponse{OK}, session created.
// ===========================================================================
TEST_F(GatewayRedirectFixture, Leader_Accepts_Connect) {
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/true, "aeron:ipc"};
    });

    auto client = makeClient(kEgressBase + 2);

    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    pc::ConnectRequest req;
    req.sessionId       = 0;
    req.correlationId   = 99;
    req.egressStreamId  = client.egressStreamId;
    req.clientVersion   = 1;
    req.egressChannel   = client.egressChannel;
    uint8_t sc[512] = {};
    const std::size_t len = req.encode(sc);
    offerBounded(*client.ingressPub, sc, len, "LeaderConnect");

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

    ASSERT_TRUE(resp.has_value()) << "Leader must return ConnectResponse";
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp->assignedSessionId, 1);
    EXPECT_EQ(resp->correlationId, req.correlationId);
    EXPECT_EQ(gw_->sessionCount(), 1u);
}

// ===========================================================================
// BR5R-3: Leader-unknown degrades to accept.
//
// isLeader=false but leaderClientEndpoint="" → no redirect → local accept.
// ===========================================================================
TEST_F(GatewayRedirectFixture, LeaderUnknown_DegradesToAccept) {
    buildGateway([]() -> LeaderView {
        return LeaderView{/*isLeader=*/false, /*leaderClientEndpoint=*/""};
    });

    auto client = makeClient(kEgressBase + 3);

    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    pc::ConnectRequest req;
    req.sessionId       = 0;
    req.correlationId   = 55;
    req.egressStreamId  = client.egressStreamId;
    req.clientVersion   = 1;
    req.egressChannel   = client.egressChannel;
    uint8_t sc[512] = {};
    const std::size_t len = req.encode(sc);
    offerBounded(*client.ingressPub, sc, len, "UnknownLeaderConnect");

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
         &*client.egressSub, &asm_, kPumpIter, "ConnectResponse(degraded)");

    ASSERT_TRUE(resp.has_value())
        << "Leader-unknown must degrade to local accept";
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp->assignedSessionId, 1);
    EXPECT_EQ(gw_->sessionCount(), 1u);
}

// ===========================================================================
// BR5R-4: Existing session unaffected by leadership flip.
//
// Connect as leader → session created.
// Flip leaderView to follower.
// Ping on existing session → Pong received (only NEW Connects are redirected;
// established sessions are always handled locally).
// ===========================================================================
TEST_F(GatewayRedirectFixture, ExistingSession_UnaffectedBy_LeadershipFlip) {
    // Start as leader.
    std::atomic<bool> isLeader{true};

    buildGateway([&]() -> LeaderView {
        return LeaderView{isLeader.load(), "aeron:ipc"};
    });

    auto client = makeClient(kEgressBase + 4);

    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitPubConnected");

    // Connect as leader.
    pc::ConnectRequest req;
    req.sessionId       = 0;
    req.correlationId   = 77;
    req.egressStreamId  = client.egressStreamId;
    req.clientVersion   = 1;
    req.egressChannel   = client.egressChannel;
    uint8_t sc[512] = {};
    offerBounded(*client.ingressPub, sc, req.encode(sc), "LeaderConnect");

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
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    const int64_t sid = resp->assignedSessionId;
    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Flip to follower.
    isLeader.store(false, std::memory_order_release);

    // Send Ping on the existing session — must still get Pong.
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 88;
    ping.echo          = "still-routed-after-flip";
    uint8_t pingBuf[256] = {};
    const std::size_t pingLen = ping.encode(pingBuf);
    offerBounded(*client.ingressPub, pingBuf, pingLen, "PingAfterFlip");

    std::optional<pc::Pong> pong;
    std::string             pongEcho;
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
         &*client.egressSub, &pingAsm, kPumpIter, "PongAfterFlip");

    ASSERT_TRUE(pong.has_value())
        << "Existing session must still receive Pong after leadership flip";
    EXPECT_EQ(pong->correlationId, 88);
    EXPECT_EQ(pongEcho, "still-routed-after-flip");
    EXPECT_EQ(gw_->sessionCount(), 1u);
}

} // namespace
} // namespace rune::gateway
