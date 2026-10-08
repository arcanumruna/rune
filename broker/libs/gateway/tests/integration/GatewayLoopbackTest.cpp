// GatewayLoopbackTest.cpp — aeron:ipc loopback integration tests for
// ClientGateway (BR-4).
//
// Setup:
//   - One embedded Aeron media driver (unique temp dir per test fixture).
//   - One Aeron client shared by the gateway and test harness.
//   - Gateway subscribes on the CLIENT INGRESS channel/stream.
//   - Test harness publishes to that ingress and subscribes on the egress
//     channel/stream (per-session).
//
// Bounded drivers only: all waits use hard caps + ASSERT (no busy-spin).
// Unique temp paths: getpid() + atomic counter.
//
// Scenarios:
//   1.  Connect round-trip: ConnectResponse with OK, assignedSessionId≥1, leaderMemberId echoed
//   2.  Ping → Pong: echo bytes match, correlationId echoed
//   3.  Binary echo: Ping with NUL + 0xFF in echo
//   4.  Empty echo: Ping with empty echo bytes
//   5.  Keepalive: sessionCount stays 1; subsequent Ping still works
//   6.  Disconnect: sessionCount → 0; subsequent Ping yields no Pong
//   7.  Two concurrent sessions: distinct sessionIds; Pong routed correctly (no cross-talk)
//   8.  Unknown templateId (99): gateway drops, still serves valid Ping
//   9.  Truncated/garbage frame: gateway drops, still serves valid Ping
//  10.  Ping for unknown session: no crash, no response
//  11.  Wrong schemaId frame: gateway drops cleanly
//  12.  Multiple Pings in sequence: all Pongs received, echo matches each time
//  13.  Unresolvable egress channel (error path): ConnectRequest with bad channel →
//       no session created, no crash, gateway keeps serving (BR-4 coverage top-up)
//  14.  UnsupportedVersion frame dropped: version > kProtocolVersion → dropped;
//       subsequent valid Ping still gets Pong (BR-4 coverage top-up)
//  15.  correlationId distinctness: two Pings with different correlationIds →
//       Pongs echo each id correctly (BR-4 coverage top-up)

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
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gw_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// RAII temp dir cleanup
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "")
        : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// Current timestamp (monotonic nanoseconds).
// ---------------------------------------------------------------------------
static int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// GatewayLoopbackFixture — one embedded driver + one Aeron client per suite.
// ALL helper methods are public so free functions can call them via ref.
// ---------------------------------------------------------------------------
class GatewayLoopbackFixture : public ::testing::Test {
public:
    // Stream IDs: ingress (clients → gateway) and egress base (gateway → client).
    // Each "test client" uses egressBase + N to get a unique egress stream.
    static constexpr int32_t kIngressStream  = 101;
    static constexpr int32_t kEgressBase     = 200;
    static constexpr int32_t kThisNodeId     = 7;
    static constexpr int     kPumpIter       = 50'000;  // hard cap

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("fix_");
        startDriver();
        connectAeron();

        // Build the gateway (subscribes on ingress).
        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId);
    }

    void TearDown() override {
        // Destroy gateway first (releases Subscription + ExclusivePublications).
        gw_.reset();
        // Destroy Aeron client (conductor joins).
        aeron_.reset();
        // Stop driver.
        stopDriver();
        // TempDir RAII deletes the dir.
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

    TestClient makeClient(int32_t egressStreamId) {
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
                             std::string_view label = "offer") {
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
              std::string_view label = "pump") {
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

    // -----------------------------------------------------------------------
    // doConnect — send ConnectRequest, wait for ConnectResponse.
    // -----------------------------------------------------------------------
    pc::ConnectResponse doConnect(aeron::Publication&   ingressPub,
                                  aeron::Subscription&  egressSub,
                                  int32_t               egressStreamId,
                                  const std::string&    egressChannel,
                                  int64_t               correlationId,
                                  uint8_t*              scratch) {
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = correlationId;
        req.egressStreamId  = egressStreamId;
        req.clientVersion   = 1;
        req.egressChannel   = egressChannel;
        const std::size_t reqLen = req.encode(scratch);

        // Wait for ingress publication to connect.
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
    // Scratch buffer for encoding test frames.
    // -----------------------------------------------------------------------
    static constexpr std::size_t kScratch = 68 * 1024;
    uint8_t scratch_[kScratch] = {};

    std::unique_ptr<ClientGateway> gw_;
    std::shared_ptr<aeron::Aeron>  aeron_;

private:
    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_,
                                                AERON_THREADING_MODE_SHARED);
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

        // No caller-side pumping: aeron_driver_init/start already wrote cnc.dat
        // and stamped the heartbeat synchronously before driverThread_ even
        // starts; pumping here would be a second concurrent pumper on the
        // single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath))
            << "Aeron CnC file not created within timeout";
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
            // Drain residual work.
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
    aeron_driver_context_t*    driverCtx_   = nullptr;
    aeron_driver_t*            driver_      = nullptr;
    std::atomic<bool>          driverRunning_{false};
    std::thread                driverThread_;
};

// ===========================================================================
// TEST 1: Connect round-trip
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Connect_RoundTrip) {
    auto client = makeClient(kEgressBase + 1);

    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel,
                          /*correlationId=*/42, scratch_);

    EXPECT_EQ(resp.correlationId, 42);
    EXPECT_EQ(resp.code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp.assignedSessionId, 1);
    EXPECT_EQ(resp.leaderMemberId, kThisNodeId);
    EXPECT_EQ(gw_->sessionCount(), 1u);
    // CG round 3/4: the broker advertises its version (the 42-byte form,
    // golden connect_response_v3.bin) and its session ids carry its node id.
    EXPECT_EQ(resp.brokerVersion, pc::kBrokerVersion);
    EXPECT_EQ(resp.assignedSessionId >> 40, static_cast<int64_t>(kThisNodeId) + 1);
}

// ===========================================================================
// TEST 2: Ping → Pong (basic string echo)
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Ping_Pong_Basic) {
    auto client = makeClient(kEgressBase + 2);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 1, scratch_);
    const int64_t sid = resp.assignedSessionId;

    const std::string echoStr = "hello-rune";
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 77;
    ping.echo          = echoStr;
    const std::size_t pingLen = ping.encode(scratch_);

    offerBounded(*client.ingressPub, scratch_, pingLen, "Ping");

    std::optional<pc::Pong> pong;
    std::string             pongEcho; // stable storage for echo
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            dec->echo = pongEcho;
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "Pong");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pong->correlationId, 77);
    EXPECT_EQ(pongEcho, echoStr);
}

// ===========================================================================
// TEST 3: Binary echo (NUL + 0xFF)
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Ping_Pong_BinaryEcho) {
    auto client = makeClient(kEgressBase + 3);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 2, scratch_);
    const int64_t sid = resp.assignedSessionId;

    const uint8_t echoBytes[] = {0x00, 0xFF, 0xAB, 0x00, 0x01};
    const std::string_view echoSv(reinterpret_cast<const char*>(echoBytes),
                                  sizeof(echoBytes));

    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 88;
    ping.echo          = echoSv;
    const std::size_t pingLen = ping.encode(scratch_);

    offerBounded(*client.ingressPub, scratch_, pingLen, "BinaryPing");

    std::optional<pc::Pong> pong;
    std::vector<uint8_t>    pongEchoBytes;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEchoBytes.assign(dec->echo.begin(), dec->echo.end());
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "BinaryPong");

    ASSERT_TRUE(pong.has_value());
    ASSERT_EQ(pongEchoBytes.size(), sizeof(echoBytes));
    EXPECT_EQ(std::memcmp(pongEchoBytes.data(), echoBytes, sizeof(echoBytes)), 0);
}

// ===========================================================================
// TEST 4: Empty echo
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Ping_Pong_EmptyEcho) {
    auto client = makeClient(kEgressBase + 4);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 3, scratch_);
    const int64_t sid = resp.assignedSessionId;

    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 99;
    ping.echo          = std::string_view{};   // empty
    const std::size_t pingLen = ping.encode(scratch_);

    offerBounded(*client.ingressPub, scratch_, pingLen, "EmptyPing");

    std::optional<pc::Pong> pong;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) pong = *dec;
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "EmptyPong");

    ASSERT_TRUE(pong.has_value());
    EXPECT_TRUE(pong->echo.empty());
}

// ===========================================================================
// TEST 5: Keepalive — session stays; subsequent Ping still works
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Keepalive_SessionPersists) {
    auto client = makeClient(kEgressBase + 5);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 4, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Send Keepalive.
    pc::Keepalive ka;
    ka.sessionId = sid;
    const std::size_t kaLen = ka.encode(scratch_);
    offerBounded(*client.ingressPub, scratch_, kaLen, "Keepalive");

    // Give gateway time to process it (bounded idle).
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "KeepaliveProcess");
    }

    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Subsequent Ping must still work.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 55;
    ping.echo          = "after-keepalive";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterKeepalive");

    std::optional<pc::Pong> pong;
    std::string             pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "PongAfterKeepalive");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pongEcho, "after-keepalive");
}

// ===========================================================================
// TEST 6: Disconnect — sessionCount → 0; subsequent Ping yields no Pong
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Disconnect_SessionRemoved_NoSubsequentPong) {
    auto client = makeClient(kEgressBase + 6);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 5, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Disconnect.
    pc::Disconnect disc;
    disc.sessionId = sid;
    disc.reason    = 0;
    const std::size_t discLen = disc.encode(scratch_);
    offerBounded(*client.ingressPub, scratch_, discLen, "Disconnect");

    // Wait for gateway to process disconnect.
    pump([&]() { return gw_->sessionCount() == 0u; },
         nullptr, nullptr, kPumpIter, "SessionCount==0");

    EXPECT_EQ(gw_->sessionCount(), 0u);

    // Send a Ping — should receive NO Pong.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 66;
    ping.echo          = "after-disconnect";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterDisconnect");

    // Bounded no-response wait.
    std::optional<pc::Pong> pong;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) pong = *dec;
    });

    for (int i = 0; i < 2000; ++i) {
        gw_->doWork(nowNs());
        client.egressSub->poll(asm_.handler(), 10);
        if (pong.has_value()) break;
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    EXPECT_FALSE(pong.has_value()) << "Received unexpected Pong after Disconnect";
}

// ===========================================================================
// TEST 7: Two concurrent sessions — distinct IDs; Pong routed correctly
// ===========================================================================
TEST_F(GatewayLoopbackFixture, TwoConcurrentSessions_NoXtalk) {
    auto clientA = makeClient(kEgressBase + 7);
    auto clientB = makeClient(kEgressBase + 8);

    auto respA = doConnect(*clientA.ingressPub, *clientA.egressSub,
                           clientA.egressStreamId, clientA.egressChannel, 10, scratch_);
    uint8_t scratchB[512] = {};
    auto respB = doConnect(*clientB.ingressPub, *clientB.egressSub,
                           clientB.egressStreamId, clientB.egressChannel, 20, scratchB);

    EXPECT_GE(respA.assignedSessionId, 1);
    EXPECT_GE(respB.assignedSessionId, 1);
    EXPECT_NE(respA.assignedSessionId, respB.assignedSessionId);
    EXPECT_EQ(gw_->sessionCount(), 2u);

    const int64_t sidA = respA.assignedSessionId;
    const int64_t sidB = respB.assignedSessionId;

    // Ping from A.
    uint8_t pingBufA[256] = {};
    pc::Ping pingA;
    pingA.sessionId     = sidA;
    pingA.correlationId = 101;
    pingA.echo          = "from-A";
    const std::size_t pingALen = pingA.encode(pingBufA);
    offerBounded(*clientA.ingressPub, pingBufA, pingALen, "PingA");

    // Ping from B.
    uint8_t pingBufB[256] = {};
    pc::Ping pingB;
    pingB.sessionId     = sidB;
    pingB.correlationId = 102;
    pingB.echo          = "from-B";
    const std::size_t pingBLen = pingB.encode(pingBufB);
    offerBounded(*clientB.ingressPub, pingBufB, pingBLen, "PingB");

    std::optional<pc::Pong> pongA;
    std::string pongEchoA;
    aeron::FragmentAssembler asmA([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEchoA = std::string(dec->echo);
            pongA = *dec;
        }
    });

    std::optional<pc::Pong> pongB;
    std::string pongEchoB;
    aeron::FragmentAssembler asmB([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEchoB = std::string(dec->echo);
            pongB = *dec;
        }
    });

    // Pump until both Pongs arrive.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{10};
    for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
        gw_->doWork(nowNs());
        clientA.egressSub->poll(asmA.handler(), 10);
        clientB.egressSub->poll(asmB.handler(), 10);
        if (pongA.has_value() && pongB.has_value()) break;
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    ASSERT_TRUE(pongA.has_value()) << "PongA not received";
    ASSERT_TRUE(pongB.has_value()) << "PongB not received";

    EXPECT_EQ(pongA->correlationId, 101);
    EXPECT_EQ(pongEchoA, "from-A");

    EXPECT_EQ(pongB->correlationId, 102);
    EXPECT_EQ(pongEchoB, "from-B");
}

// ===========================================================================
// TEST 8: Unknown templateId — gateway drops; still serves valid Ping
// ===========================================================================
TEST_F(GatewayLoopbackFixture, UnknownTemplateId_DropsAndKeepsServing) {
    auto client = makeClient(kEgressBase + 9);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 6, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Craft a frame with templateId=99 (unknown), correct schemaId/version.
    {
        pc::MessageHeader hdr;
        hdr.blockLength   = 0;
        hdr.templateId    = 99;   // unknown — triggers UnknownTemplate in checkFrame
        hdr.schemaId      = pc::kSchemaId;
        hdr.version       = pc::kProtocolVersion;
        hdr.sessionId     = 0;
        hdr.correlationId = 0;
        hdr.encode(scratch_);
        offerBounded(*client.ingressPub, scratch_,
                     pc::MessageHeader::ENCODED_SIZE, "UnknownTid");
    }

    // Give gateway time to process the bad frame.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "BadFrameProcess");
    }

    // Send valid Ping — must still get Pong.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 33;
    ping.echo          = "still-alive";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterBadFrame");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "PongAfterBadFrame");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pongEcho, "still-alive");
}

// ===========================================================================
// TEST 9: Truncated/garbage frame — gateway drops; still serves valid Ping
// ===========================================================================
TEST_F(GatewayLoopbackFixture, GarbageFrame_DropsAndKeepsServing) {
    auto client = makeClient(kEgressBase + 10);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 7, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Offer garbage bytes (too short to parse as a header).
    const uint8_t garbage[] = {0xFF, 0x00, 0x01, 0xAB};
    offerBounded(*client.ingressPub, garbage, sizeof(garbage), "GarbageFrame");

    // Give gateway time to process/drop.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "GarbageProcess");
    }

    // Valid Ping must still get Pong.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 44;
    ping.echo          = "survived-garbage";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterGarbage");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "PongAfterGarbage");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pongEcho, "survived-garbage");
}

// ===========================================================================
// TEST 10: Ping for unknown session — no crash, no response
// ===========================================================================
TEST_F(GatewayLoopbackFixture, Ping_UnknownSession_NoCrashNoResponse) {
    // Resolve an ingress publication (no doConnect — no session registered).
    int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
    std::shared_ptr<aeron::Publication> ingressPub;
    pump([&]() {
            if (!ingressPub) ingressPub = aeron_->findPublication(pubRegId);
            return ingressPub != nullptr;
         }, nullptr, nullptr, kPumpIter, "unknownSession ingressPub");

    pump([&]() { return ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "unknownSession pubConnected");

    // Send Ping with a fabricated sessionId that was never assigned.
    const int64_t fakeSid = 99999;
    pc::Ping ping;
    ping.sessionId     = fakeSid;
    ping.correlationId = 11;
    ping.echo          = "ghost";
    const std::size_t pingLen = ping.encode(scratch_);
    offerBounded(*ingressPub, scratch_, pingLen, "UnknownSessPing");

    // Let gateway process; the session doesn't exist so no Pong can be sent.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 2000; }, nullptr, nullptr, 3000, "UnknownSessProcess");
    }

    EXPECT_EQ(gw_->sessionCount(), 0u);
    SUCCEED(); // no crash → test passes
}

// ===========================================================================
// TEST 11: Wrong schemaId — gateway drops cleanly, session survives
// ===========================================================================
TEST_F(GatewayLoopbackFixture, WrongSchemaId_DropsCleanly) {
    auto client = makeClient(kEgressBase + 11);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 8, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Build a frame with wrong schemaId.
    {
        pc::MessageHeader hdr;
        hdr.blockLength   = 0;
        hdr.templateId    = static_cast<uint16_t>(pc::TemplateId::Ping);
        hdr.schemaId      = 0xFFFF;   // wrong schemaId
        hdr.version       = pc::kProtocolVersion;
        hdr.sessionId     = sid;
        hdr.correlationId = 0;
        hdr.encode(scratch_);
        // Append minimal var-bytes (echo length = 0).
        scratch_[pc::MessageHeader::ENCODED_SIZE]     = 0;
        scratch_[pc::MessageHeader::ENCODED_SIZE + 1] = 0;
        offerBounded(*client.ingressPub, scratch_,
                     pc::MessageHeader::ENCODED_SIZE + 2, "WrongSchema");
    }

    // Give gateway time to drop.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "WrongSchemaProcess");
    }

    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Valid Ping still gets Pong.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 22;
    ping.echo          = "after-wrong-schema";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterWrongSchema");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "PongAfterWrongSchema");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pongEcho, "after-wrong-schema");
}

// ===========================================================================
// TEST 12: Multiple Pings in sequence — all Pongs received, echo matches
// ===========================================================================
TEST_F(GatewayLoopbackFixture, MultiplePings_AllPongsReceived) {
    auto client = makeClient(kEgressBase + 12);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 9, scratch_);
    const int64_t sid = resp.assignedSessionId;

    constexpr int kN = 5;

    // Storage for stable echo strings and received pongs.
    std::vector<std::string>        echoes;
    std::vector<int64_t>            corrIds;
    std::vector<std::string>        recvEchoes;
    std::vector<int64_t>            recvCorrIds;
    echoes.reserve(kN);
    corrIds.reserve(kN);
    recvEchoes.reserve(kN);
    recvCorrIds.reserve(kN);

    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            recvEchoes.push_back(std::string(dec->echo));
            recvCorrIds.push_back(dec->correlationId);
        }
    });

    for (int i = 0; i < kN; ++i) {
        echoes.push_back("ping-" + std::to_string(i));
        corrIds.push_back(static_cast<int64_t>(i + 100));

        uint8_t sc[512] = {};
        pc::Ping ping;
        ping.sessionId     = sid;
        ping.correlationId = corrIds.back();
        ping.echo          = echoes.back();
        const std::size_t pingLen = ping.encode(sc);
        offerBounded(*client.ingressPub, sc, pingLen, "SeqPing");

        // Wait for each Pong before sending the next.
        const std::size_t want = static_cast<std::size_t>(i + 1);
        pump([&]() { return recvEchoes.size() >= want; },
             &*client.egressSub, &asm_,
             kPumpIter, "SeqPong");
    }

    ASSERT_EQ(recvEchoes.size(), static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        EXPECT_EQ(recvCorrIds[i], corrIds[i]);
        EXPECT_EQ(recvEchoes[i],  echoes[i])
            << "Echo mismatch at index " << i;
    }
}

// ===========================================================================
// TEST 13: Unresolvable egress channel — no session created, no crash,
//          gateway keeps serving (BR-4 coverage top-up).
//
// Strategy: send a ConnectRequest whose egressChannel is "aeron:bogus" — a
// syntactically parseable-but-invalid channel that Aeron's media driver will
// reject.  The async pending-connect path sees the registration error and
// drops the connect (no session added).
//
// Note on timing: Aeron rejects the channel at the driver level, so the test
// completes quickly; it is bounded by the outer pump() deadline.
// ===========================================================================
TEST_F(GatewayLoopbackFixture, UnresolvableEgressChannel_NoSession_GatewayKeepsServing) {
    // We need a raw ingress publication to send the bad ConnectRequest.
    // No doConnect() helper here since we expect NO response.
    int64_t badPubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
    std::shared_ptr<aeron::Publication> ingressPub;
    pump([&]() {
            if (!ingressPub) ingressPub = aeron_->findPublication(badPubRegId);
            return ingressPub != nullptr;
         }, nullptr, nullptr, kPumpIter, "badEgress ingressPub resolve");

    pump([&]() { return ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "badEgress ingressPub connected");

    // Build a ConnectRequest pointing at an invalid egress channel.
    // "aeron:bogus" is parseable (valid URI prefix) but Aeron will reject it.
    pc::ConnectRequest req;
    req.sessionId      = 0;
    req.correlationId  = 999;
    req.egressStreamId = kEgressBase + 13;
    req.clientVersion  = 1;
    req.egressChannel  = "aeron:bogus";
    const std::size_t reqLen = req.encode(scratch_);
    offerBounded(*ingressPub, scratch_, reqLen, "BadEgressConnectRequest");

    // Give gateway ample time to process — it must not crash.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 2000; }, nullptr, nullptr, 3000, "BadEgressProcess");
    }

    // No session must have been created.
    EXPECT_EQ(gw_->sessionCount(), 0u) << "Gateway must not create a session for an unresolvable egress";

    // Gateway must keep serving: connect a valid second client and ping it.
    auto goodClient = makeClient(kEgressBase + 14);
    uint8_t scratch2[512] = {};
    auto respGood = doConnect(*goodClient.ingressPub, *goodClient.egressSub,
                              goodClient.egressStreamId, goodClient.egressChannel,
                              /*correlationId=*/888, scratch2);
    ASSERT_EQ(respGood.code, static_cast<int16_t>(pc::ErrorCode::OK));
    ASSERT_GE(respGood.assignedSessionId, 1);
    EXPECT_EQ(gw_->sessionCount(), 1u);

    uint8_t scratch3[512] = {};
    pc::Ping ping;
    ping.sessionId     = respGood.assignedSessionId;
    ping.correlationId = 777;
    ping.echo          = "after-bad-egress";
    const std::size_t pingLen = ping.encode(scratch3);
    offerBounded(*goodClient.ingressPub, scratch3, pingLen, "PingAfterBadEgress");

    std::optional<pc::Pong> pong;
    std::string             pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*goodClient.egressSub, &asm_,
         kPumpIter, "PongAfterBadEgress");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pongEcho, "after-bad-egress");
}

// ===========================================================================
// TEST 14: UnsupportedVersion frame dropped — gateway keeps serving.
//          (BR-4 coverage top-up)
//
// Sends a frame whose header version = kProtocolVersion + 1 (= 2).
// checkFrame() returns FrameStatus::UnsupportedVersion → gateway drops it.
// A subsequent valid Ping still produces a Pong.
// ===========================================================================
TEST_F(GatewayLoopbackFixture, UnsupportedVersionFrame_DroppedGatewayKeepsServing) {
    auto client = makeClient(kEgressBase + 15);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 11, scratch_);
    const int64_t sid = resp.assignedSessionId;

    // Craft a Ping-shaped frame with version = kProtocolVersion + 1.
    {
        pc::MessageHeader hdr;
        hdr.blockLength   = 0;
        hdr.templateId    = static_cast<uint16_t>(pc::TemplateId::Ping);
        hdr.schemaId      = pc::kSchemaId;
        hdr.version       = static_cast<uint16_t>(pc::kProtocolVersion + 1); // 2 — unsupported
        hdr.sessionId     = sid;
        hdr.correlationId = 0;
        hdr.encode(scratch_);
        // Append minimal var-bytes (echo length = 0) so the buffer is well-formed length-wise.
        scratch_[pc::MessageHeader::ENCODED_SIZE]     = 0;
        scratch_[pc::MessageHeader::ENCODED_SIZE + 1] = 0;
        offerBounded(*client.ingressPub, scratch_,
                     pc::MessageHeader::ENCODED_SIZE + 2, "UnsupportedVersionFrame");
    }

    // Give gateway time to drop the bad frame.
    {
        int idle = 0;
        pump([&]() { ++idle; return idle >= 300; }, nullptr, nullptr, 600, "UnsupportedVersionProcess");
    }

    // Session must still be alive.
    EXPECT_EQ(gw_->sessionCount(), 1u);

    // Valid Ping must still get a Pong.
    uint8_t scratch2[512] = {};
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 50;
    ping.echo          = "after-unsupported-version";
    const std::size_t pingLen = ping.encode(scratch2);
    offerBounded(*client.ingressPub, scratch2, pingLen, "PingAfterUnsupportedVersion");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            pong = *dec;
        }
    });

    pump([&]() { return pong.has_value(); }, &*client.egressSub, &asm_,
         kPumpIter, "PongAfterUnsupportedVersion");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pong->correlationId, 50);
    EXPECT_EQ(pongEcho, "after-unsupported-version");
}

// ===========================================================================
// TEST 15: correlationId distinctness — two Pings with different correlationIds
//          produce Pongs that each echo the correct id.  (BR-4 coverage top-up)
//
// Verifies that the gateway does not carry over a stale correlationId between
// consecutive Ping/Pong pairs on the same session.
// ===========================================================================
TEST_F(GatewayLoopbackFixture, CorrelationId_Distinctness_TwoPings) {
    auto client = makeClient(kEgressBase + 16);
    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 12, scratch_);
    const int64_t sid = resp.assignedSessionId;

    constexpr int64_t kCorrId1 = 1001;
    constexpr int64_t kCorrId2 = 2002;

    std::vector<int64_t> recvCorrIds;
    std::vector<std::string> recvEchoes;

    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t offset,
                                     aeron::util::index_t len,
                                     aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            recvCorrIds.push_back(dec->correlationId);
            recvEchoes.push_back(std::string(dec->echo));
        }
    });

    // --- First Ping (correlationId = C1) ---
    {
        uint8_t sc[512] = {};
        pc::Ping ping;
        ping.sessionId     = sid;
        ping.correlationId = kCorrId1;
        ping.echo          = "echo-C1";
        const std::size_t pingLen = ping.encode(sc);
        offerBounded(*client.ingressPub, sc, pingLen, "Ping-C1");
    }
    // Wait for first Pong.
    pump([&]() { return recvCorrIds.size() >= 1u; },
         &*client.egressSub, &asm_, kPumpIter, "Pong-C1");

    ASSERT_EQ(recvCorrIds.size(), 1u);
    EXPECT_EQ(recvCorrIds[0], kCorrId1) << "First Pong must carry C1";
    EXPECT_EQ(recvEchoes[0],  "echo-C1");

    // --- Second Ping (correlationId = C2, different from C1) ---
    {
        uint8_t sc[512] = {};
        pc::Ping ping;
        ping.sessionId     = sid;
        ping.correlationId = kCorrId2;
        ping.echo          = "echo-C2";
        const std::size_t pingLen = ping.encode(sc);
        offerBounded(*client.ingressPub, sc, pingLen, "Ping-C2");
    }
    // Wait for second Pong.
    pump([&]() { return recvCorrIds.size() >= 2u; },
         &*client.egressSub, &asm_, kPumpIter, "Pong-C2");

    ASSERT_EQ(recvCorrIds.size(), 2u);
    EXPECT_EQ(recvCorrIds[1], kCorrId2) << "Second Pong must carry C2, not C1";
    EXPECT_EQ(recvEchoes[1],  "echo-C2");

    // Extra distinctness guard: the two correlationIds must be different.
    EXPECT_NE(recvCorrIds[0], recvCorrIds[1]);
}

} // namespace
} // namespace rune::gateway
