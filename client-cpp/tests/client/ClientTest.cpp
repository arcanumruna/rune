// tests/client/ClientTest.cpp
//
// CC-6 Acceptance tests — Client public API (connect/ping futures + callbacks).
//
// Test matrix (all bounded, no busy-spin, unique temp dirs):
//
//   1. ConnectFutureResolvesOnConnected
//        client.connect().get() resolves; onConnected(S) fires; state()==Connected.
//
//   2. PingFutureEchoNonEmpty
//        ping({'h','i',0x00,0xFF}) resolves with the same bytes (binary-safe).
//
//   3. PingFutureEchoEmpty
//        ping({}) resolves with an empty Bytes vector.
//
//   4. MultipleConcurrentPings
//        3 concurrent pings with distinct payloads all resolve with their own
//        echoes — proves per-correlationId matching, no cross-resolution.
//
//   5. PingWithoutSessionRejected
//        ping() before connect() → future throws (not Connected).
//
//   6. ErrorOnConnectSurfacesOnFuture
//        Gateway replies ErrorResponse → connect().get() throws; onError fires.
//
//   7. PingTimeoutNoHang
//        Gateway does NOT reply to a Ping → future fails after pingTimeoutMs
//        (short 500 ms) — no hang.
//
//   8. CloseFailsPendingFutures
//        close() while a connect is in-flight → future fails (not hang); runner joined.
//
//   9. CloseIsIdempotent
//        close() called twice (and again via dtor) → no crash / hang.
//
//  10. DtorAfterConnectClean
//        Connect successfully then let the dtor run — no crash / hang.
//
// Rules:
//   - Hard deadline on every future::wait_for (kFutureTimeout).
//   - NO busy-spin anywhere — sleep_for between poll iterations.
//   - Unique temp driver dirs: getpid() + atomic counter.
//   - Shared-driver gateway pattern identical to CC-5 (gateway Aeron client
//     attaches to the agent's embedded driver via the same aeronDir).

#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Hard cap on any blocking wait in tests.
static constexpr auto kFutureTimeout = 10s;

/// Poll iterations / sleep between them when driving gateway.
static constexpr int  kPollMax   = 3'000;
static constexpr auto kPollSleep = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCtr{0};

std::string uniqueDir() {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cc6_test_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // namespace

// ---------------------------------------------------------------------------
// In-test gateway — mirrors the CC-5 harness but lives entirely in a helper
// struct.  The gateway Aeron client attaches to the agent's embedded driver
// (sharing the same aeronDir), so aeron:ipc works without two separate
// processes or two embedded drivers on the same IPC path.
//
// The gateway sub/pub pair is set up AFTER the agent starts (the agent's
// embedded driver must be running first so the gateway client can attach).
//
// Behaviour is configured via policy flags before calling start():
//   replyWithOK       — respond to ConnectRequest with ConnectResponse{OK}.
//   replyWithError    — respond to ConnectRequest with ErrorResponse.
//   echoPong          — respond to Ping with Pong{echo, same correlationId}.
//   (all default true / true / true for the happy-path gateway)
// ---------------------------------------------------------------------------

struct TestGateway {
    std::string aeronDir;

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;   // reads ingress
    std::shared_ptr<aeron::ExclusivePublication> pub_;   // writes egress
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    int32_t ingressStream = 0;
    int32_t egressStream  = 0;

    bool replyWithOK    = true;
    bool replyWithError = false;
    bool echoPong       = true;

    uint8_t replyBuf[512]{};

    explicit TestGateway(std::string dir) : aeronDir(std::move(dir)) {}

    ~TestGateway() { stop(); }

    /// Connect the gateway Aeron client to the SAME driver as the agent.
    /// Call after the agent (and its embedded driver) has started and had a
    /// moment to initialise.
    void start(int32_t inStream, int32_t egStream) {
        ingressStream = inStream;
        egressStream  = egStream;

        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) throw std::runtime_error("TestGateway: Aeron::connect failed");

        int64_t subId = aeron_->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = aeron_->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub_) sub_ = aeron_->findSubscription(subId);
            if (!pub_) pub_ = aeron_->findExclusivePublication(pubId);
            if (sub_ && pub_) break;
            std::this_thread::sleep_for(kPollSleep);
        }
        if (!sub_ || !pub_)
            throw std::runtime_error("TestGateway: timed out resolving sub/pub");

        assembler_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t            offset,
                   aeron::util::index_t            len,
                   aeron::Header&)
            {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                        static_cast<std::size_t>(len));
            });
    }

    void stop() {
        assembler_.reset();
        pub_.reset();
        sub_.reset();
        aeron_.reset();
    }

    int poll(int limit = 16) {
        if (!sub_ || !assembler_) return 0;
        return sub_->poll(assembler_->handler(), limit);
    }

    bool send(const uint8_t* buf, std::size_t len) {
        if (!pub_) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
        for (int i = 0; i < 500; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        switch (static_cast<rcc::TemplateId>(*tidOpt)) {
        case rcc::TemplateId::Connect: {
            auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;

            if (replyWithError) {
                // Reply ErrorResponse with a descriptive message.
                std::size_t n = rcc::encodeErrorResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::Internal),
                    "auth-failed",
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) send(replyBuf, n);
            } else if (replyWithOK) {
                std::size_t n = rcc::encodeConnectResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::OK),
                    /*leaderMemberId=*/0,
                    /*assignedSessionId=*/42,
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) send(replyBuf, n);
            }
            // else: no reply (simulates timeout)
            break;
        }

        case rcc::TemplateId::Ping: {
            if (!echoPong) break; // no reply → simulates ping timeout
            auto opt = rcc::decodePing(buf, len);
            if (!opt) return;
            std::size_t n = rcc::encodePong(
                replyBuf, sizeof(replyBuf),
                opt->echo,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            break;
        }

        default:
            break;
        }
    }
};

// ---------------------------------------------------------------------------
// GatewayHarness — RAII wrapper that wires a Client + TestGateway together.
//
// Creates a unique data directory, builds a ClientConfig with short timeouts
// for tests, constructs the Client (which starts the agent + runner), then
// attaches a TestGateway to the same embedded driver.
//
// After construction the gateway is polling-ready (call harness.pollGw() in
// a loop or rely on the gateway's own poll thread).
// ---------------------------------------------------------------------------

struct GatewayHarness {
    static constexpr int32_t kIngressStream = 800;
    static constexpr int32_t kEgressStream  = 801;

    std::string     agentDir;
    std::string     aeronDir;
    ClientConfig    clientCfg;

    std::unique_ptr<Client>      client;
    std::unique_ptr<TestGateway> gw;

    /// connectTimeoutMs / pingTimeoutMs for the client (caller can set before
    /// calling build()).
    int64_t connectTimeoutMs = 5000;
    int64_t pingTimeoutMs    = 5000;

    // Unique stream base so tests don't collide.
    int32_t ingressStream;
    int32_t egressStream;

    explicit GatewayHarness(int32_t inStream = kIngressStream,
                             int32_t egStream = kEgressStream)
        : ingressStream(inStream)
        , egressStream(egStream)
    {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";
    }

    /// Build and start everything.  Call this once the caller has set any
    /// custom connectTimeoutMs / pingTimeoutMs / gw flags.
    void build(bool gwReplyOK    = true,
               bool gwReplyErr   = false,
               bool gwEchoPong   = true)
    {
        clientCfg.gatewayChannel   = "aeron:ipc";
        clientCfg.gatewayStreamId  = ingressStream;
        clientCfg.egressChannel    = "aeron:ipc";
        clientCfg.egressStreamId   = egressStream;
        clientCfg.dataDir          = agentDir;
        clientCfg.connectTimeoutMs = connectTimeoutMs;
        clientCfg.pingTimeoutMs    = pingTimeoutMs;

        client = std::make_unique<Client>(clientCfg);

        // Give the embedded driver a moment to write cnc.dat.
        std::this_thread::sleep_for(150ms);

        // Attach the gateway to the agent's driver.
        gw = std::make_unique<TestGateway>(aeronDir);
        gw->replyWithOK    = gwReplyOK;
        gw->replyWithError = gwReplyErr;
        gw->echoPong       = gwEchoPong;
        gw->start(ingressStream, egressStream);
    }

    /// Poll the gateway once (from the test thread, to drive the reply logic).
    int pollGw(int limit = 16) {
        return gw ? gw->poll(limit) : 0;
    }

    /// Drive the gateway poll for up to kPollMax iterations or until pred is true.
    template<typename Pred>
    bool pumpGwUntil(Pred pred, int limit = kPollMax) {
        for (int i = 0; i < limit; ++i) {
            gw->poll(16);
            if (pred()) return true;
            std::this_thread::sleep_for(kPollSleep);
        }
        return false;
    }
};

// ===========================================================================
// Test 1: ConnectFutureResolvesOnConnected
// ===========================================================================
TEST(Client, ConnectFutureResolvesOnConnected) {
    GatewayHarness h(810, 811);
    h.build(/*gwReplyOK=*/true);

    std::atomic<bool> connectedFired{false};
    std::atomic<int64_t> connectedSid{0};

    h.client->onConnected([&](int64_t sid) {
        connectedSid.store(sid, std::memory_order_release);
        connectedFired.store(true, std::memory_order_release);
    });

    auto fut = h.client->connect();

    // Drive the gateway so it receives the ConnectRequest and replies.
    h.pumpGwUntil([&]() {
        return connectedFired.load(std::memory_order_acquire);
    });

    // The future must resolve within kFutureTimeout.
    ASSERT_EQ(fut.wait_for(kFutureTimeout), std::future_status::ready)
        << "connect() future did not resolve within timeout";

    ASSERT_NO_THROW(fut.get()) << "connect() future threw unexpectedly";

    EXPECT_TRUE(connectedFired.load())  << "onConnected was not fired";
    EXPECT_EQ(connectedSid.load(), 42LL) << "onConnected received wrong sessionId";
    EXPECT_EQ(h.client->state(), rcs::SessionState::Connected)
        << "state() != Connected after connect()";
}

// ===========================================================================
// Test 2: PingFutureEchoNonEmpty (binary-safe payload)
// ===========================================================================
TEST(Client, PingFutureEchoNonEmpty) {
    GatewayHarness h(820, 821);
    h.build();

    h.client->onConnected([](int64_t){});

    auto connFut = h.client->connect();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_EQ(connFut.wait_for(kFutureTimeout), std::future_status::ready);
    ASSERT_NO_THROW(connFut.get());

    // Binary-safe payload including NUL and 0xFF bytes.
    Bytes payload = {'h', 'i', 0x00, 0xFF};

    auto pingFut = h.client->ping(payload);

    bool pingFired = false;
    h.pumpGwUntil([&]() {
        // Check if the future is already ready.
        return pingFut.wait_for(0ms) == std::future_status::ready;
    });

    ASSERT_EQ(pingFut.wait_for(kFutureTimeout), std::future_status::ready)
        << "ping() future did not resolve within timeout";

    Bytes echo;
    ASSERT_NO_THROW(echo = pingFut.get()) << "ping() future threw";
    ASSERT_EQ(echo, payload) << "Pong echo does not match payload";
}

// ===========================================================================
// Test 3: PingFutureEchoEmpty
// ===========================================================================
TEST(Client, PingFutureEchoEmpty) {
    GatewayHarness h(830, 831);
    h.build();

    h.client->onConnected([](int64_t){});

    auto connFut = h.client->connect();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_EQ(connFut.wait_for(kFutureTimeout), std::future_status::ready);
    ASSERT_NO_THROW(connFut.get());

    Bytes emptyPayload{};
    auto pingFut = h.client->ping(emptyPayload);

    h.pumpGwUntil([&]() {
        return pingFut.wait_for(0ms) == std::future_status::ready;
    });

    ASSERT_EQ(pingFut.wait_for(kFutureTimeout), std::future_status::ready)
        << "empty ping() future did not resolve within timeout";

    Bytes echo;
    ASSERT_NO_THROW(echo = pingFut.get());
    ASSERT_TRUE(echo.empty()) << "Expected empty echo, got " << echo.size() << " bytes";
}

// ===========================================================================
// Test 4: MultipleConcurrentPings — correlationId matching
//
// Send 3 pings with distinct payloads concurrently; each future must resolve
// with its own echo, not another ping's echo.
// ===========================================================================
TEST(Client, MultipleConcurrentPings) {
    GatewayHarness h(840, 841);
    h.build();

    h.client->onConnected([](int64_t){});

    auto connFut = h.client->connect();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_EQ(connFut.wait_for(kFutureTimeout), std::future_status::ready);
    ASSERT_NO_THROW(connFut.get());

    Bytes p1 = {0x01, 0x02, 0x03};
    Bytes p2 = {0xAA, 0xBB};
    Bytes p3 = {'x', 'y', 'z', 0x00, 0xFF};

    // Submit all 3 pings from the app thread before any gateway polling.
    auto f1 = h.client->ping(p1);
    auto f2 = h.client->ping(p2);
    auto f3 = h.client->ping(p3);

    // Drive the gateway until all 3 futures are ready.
    h.pumpGwUntil([&]() {
        return (f1.wait_for(0ms) == std::future_status::ready) &&
               (f2.wait_for(0ms) == std::future_status::ready) &&
               (f3.wait_for(0ms) == std::future_status::ready);
    });

    ASSERT_EQ(f1.wait_for(kFutureTimeout), std::future_status::ready) << "f1 not ready";
    ASSERT_EQ(f2.wait_for(kFutureTimeout), std::future_status::ready) << "f2 not ready";
    ASSERT_EQ(f3.wait_for(kFutureTimeout), std::future_status::ready) << "f3 not ready";

    Bytes e1, e2, e3;
    ASSERT_NO_THROW(e1 = f1.get()) << "f1 threw";
    ASSERT_NO_THROW(e2 = f2.get()) << "f2 threw";
    ASSERT_NO_THROW(e3 = f3.get()) << "f3 threw";

    EXPECT_EQ(e1, p1) << "f1 echo mismatch";
    EXPECT_EQ(e2, p2) << "f2 echo mismatch";
    EXPECT_EQ(e3, p3) << "f3 echo mismatch";
}

// ===========================================================================
// Test 5: PingWithoutSessionRejected
//
// ping() before connect() → future is rejected with a clear error.
// ===========================================================================
TEST(Client, PingWithoutSessionRejected) {
    GatewayHarness h(850, 851);
    h.build();

    // Do NOT call connect() — state is Disconnected.
    ASSERT_NE(h.client->state(), rcs::SessionState::Connected)
        << "Expected non-Connected state before connect()";

    auto fut = h.client->ping({'a', 'b', 'c'});

    // The future must already be rejected immediately (no wait for gateway).
    ASSERT_EQ(fut.wait_for(1s), std::future_status::ready)
        << "ping-without-session future was not rejected promptly";

    EXPECT_THROW(fut.get(), std::exception)
        << "ping() without session must throw, not return echo";
}

// ===========================================================================
// Test 6: ErrorOnConnectSurfacesOnFuture
//
// Gateway replies ErrorResponse → connect() future throws; onError fires.
// ===========================================================================
TEST(Client, ErrorOnConnectSurfacesOnFuture) {
    GatewayHarness h(860, 861);
    h.build(/*gwReplyOK=*/false, /*gwReplyErr=*/true);

    std::atomic<bool> errFired{false};
    std::string       errMsg;

    h.client->onError([&](const std::string& msg) {
        errMsg = msg;
        errFired.store(true, std::memory_order_release);
    });

    auto fut = h.client->connect();

    // Drive the gateway to send the error reply.
    h.pumpGwUntil([&]() {
        return errFired.load(std::memory_order_acquire);
    });

    ASSERT_EQ(fut.wait_for(kFutureTimeout), std::future_status::ready)
        << "connect() future did not resolve after ErrorResponse";

    EXPECT_THROW(fut.get(), std::exception)
        << "connect() must throw on ErrorResponse";

    EXPECT_TRUE(errFired.load()) << "onError was not fired";
    EXPECT_FALSE(errMsg.empty()) << "onError message must not be empty";
}

// ===========================================================================
// Test 7: PingTimeoutNoHang
//
// Gateway does NOT echo pings.  Ping future must fail after pingTimeoutMs
// (500 ms in this test) without hanging.
// ===========================================================================
TEST(Client, PingTimeoutNoHang) {
    GatewayHarness h(870, 871);
    h.connectTimeoutMs = 5000;
    h.pingTimeoutMs    = 500; // short timeout for this test
    h.build(/*gwReplyOK=*/true, /*gwReplyErr=*/false, /*gwEchoPong=*/false);

    h.client->onConnected([](int64_t){});

    // Establish a connection first (gateway still replies OK to ConnectRequest).
    auto connFut = h.client->connect();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_EQ(connFut.wait_for(kFutureTimeout), std::future_status::ready);
    ASSERT_NO_THROW(connFut.get());

    // Now send a ping — gateway will NOT reply.
    auto pingFut = h.client->ping({'x'});

    // The future must fail within pingTimeoutMs + generous test margin (3 s).
    const auto status = pingFut.wait_for(3s);
    ASSERT_EQ(status, std::future_status::ready)
        << "ping() future did not fail within timeout — potential hang";

    EXPECT_THROW(pingFut.get(), std::exception)
        << "ping() must throw on timeout, not return a value";
}

// ===========================================================================
// Test 8: CloseFailsPendingFutures
//
// close() while a connect future is in-flight → future fails (not hang).
// Runner is joined cleanly.
// ===========================================================================
TEST(Client, CloseFailsPendingFutures) {
    GatewayHarness h(880, 881);
    h.connectTimeoutMs = 10000; // long — we will close() first
    // Gateway does NOT reply to ConnectRequest (no gwReplyOK, no error).
    h.build(/*gwReplyOK=*/false, /*gwReplyErr=*/false);

    // Submit a connect — gateway won't reply so the future hangs... until close().
    auto fut = h.client->connect();

    // Let it sit for a bit then close.
    std::this_thread::sleep_for(200ms);

    // close() must join the runner and fail the future.
    ASSERT_NO_THROW(h.client->close())
        << "close() threw while pending connect was in-flight";

    ASSERT_EQ(fut.wait_for(2s), std::future_status::ready)
        << "connect() future was not failed by close() within 2 s";

    EXPECT_THROW(fut.get(), std::exception)
        << "connect() future must throw after close()";
}

// ===========================================================================
// Test 9: CloseIsIdempotent
//
// close() called multiple times → no crash / hang.
// ===========================================================================
TEST(Client, CloseIsIdempotent) {
    GatewayHarness h(890, 891);
    h.build();

    ASSERT_NO_THROW(h.client->close()) << "first close() threw";
    ASSERT_NO_THROW(h.client->close()) << "second close() threw";
    ASSERT_NO_THROW(h.client->close()) << "third close() threw";
    // dtor will call close() a fourth time — no crash.
}

// ===========================================================================
// Test 10: DtorAfterConnectClean
//
// Connect successfully then let the Client dtor run — no crash / hang.
// ===========================================================================
TEST(Client, DtorAfterConnectClean) {
    ASSERT_NO_THROW({
        GatewayHarness h(900, 901);
        h.build();

        std::atomic<bool> connectedFired{false};
        h.client->onConnected([&](int64_t) {
            connectedFired.store(true, std::memory_order_release);
        });

        auto fut = h.client->connect();

        h.pumpGwUntil([&]() {
            return connectedFired.load(std::memory_order_acquire);
        });

        // Wait for the future to resolve so we know we're in Connected state.
        EXPECT_EQ(fut.wait_for(kFutureTimeout), std::future_status::ready);

        // Dtor fires here (GatewayHarness destructs client, then gw, then driver).
        // The client dtor calls close() → joins runner → stops agent → cleans up.
    });
}
