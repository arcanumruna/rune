// tests/client/ClientTest.cpp
//
// CC-6 Acceptance tests — Client public API (connect/ping + callbacks).
// F8c C3: ops complete into a Completions queue the test thread polls
// (test_support::TestCq / PendingOp) — no futures.
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

#include "../support/AeronTestErrors.h"
#include "../support/TestCompletions.h"
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
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
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

    // MC-2 [CC-8]: instead of a Pong, reply to a Ping with an
    // ErrorResponse{code} carrying the ping's correlationId (session-fatal).
    bool    pingReplyError = false;
    int16_t pingErrorCode  = static_cast<int16_t>(rcc::ErrorCode::SessionExpired);

    // MC-5 [CC-8]: reply to a ConnectRequest with ErrorResponse{Redirect,
    // message=<endpoint>} to drive the redirect/retarget path through the agent.
    bool        redirectOnConnect = false;
    std::string redirectEndpoint;

    // Observed correlationId of the last Ping the gateway saw (for assertions).
    std::atomic<int64_t> lastPingCorrId{0};
    std::atomic<int>     pingsSeen{0};
    std::atomic<int>     connectsSeen{0};

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
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
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

            connectsSeen.fetch_add(1, std::memory_order_relaxed);

            if (redirectOnConnect) {
                // MC-5: ErrorResponse{Redirect, message=endpoint} → agent
                // retargets to an unresolvable endpoint (noexcept path).
                std::size_t n = rcc::encodeErrorResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::Redirect),
                    redirectEndpoint,
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) send(replyBuf, n);
                break;
            }

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
            auto opt = rcc::decodePing(buf, len);
            if (!opt) return;
            lastPingCorrId.store(opt->correlationId, std::memory_order_release);
            pingsSeen.fetch_add(1, std::memory_order_relaxed);

            if (pingReplyError) {
                // MC-2: reply with ErrorResponse carrying the ping's
                // correlationId (e.g. SessionExpired) → client fails the ping.
                std::size_t n = rcc::encodeErrorResponse(
                    replyBuf, sizeof(replyBuf),
                    pingErrorCode,
                    "session-expired",
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) send(replyBuf, n);
                break;
            }

            if (!echoPong) break; // no reply → simulates ping timeout
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
    // F8c C3: completions of this test's control ops (owned by the test thread).
    std::unique_ptr<test_support::TestCq> cq;

    test_support::PendingOp connectOp() {
        return cq->issue([&](Completions& q, Token t) { return client->connect(q, t); });
    }
    test_support::PendingOp pingOp(const Bytes& payload) {
        return cq->issue([&](Completions& q, Token t) { return client->ping(payload, q, t); });
    }

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
        cq     = std::make_unique<test_support::TestCq>(*client);

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

    auto fut = h.connectOp();

    // Drive the gateway so it receives the ConnectRequest and replies.
    h.pumpGwUntil([&]() {
        return connectedFired.load(std::memory_order_acquire);
    });

    // The future must resolve within kFutureTimeout.
    ASSERT_TRUE(fut.waitReady())
        << "connect() future did not resolve within timeout";

    ASSERT_TRUE(fut.ok()) << fut.describe() << "connect() future threw unexpectedly";

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

    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    // Binary-safe payload including NUL and 0xFF bytes.
    Bytes payload = {'h', 'i', 0x00, 0xFF};

    auto pingFut = h.pingOp(payload);

    bool pingFired = false;
    h.pumpGwUntil([&]() {
        // Check if the future is already ready.
        return pingFut.ready();
    });

    ASSERT_TRUE(pingFut.waitReady())
        << "ping() future did not resolve within timeout";

    Bytes echo;
    ASSERT_TRUE(pingFut.ok()) << pingFut.describe() << "ping() future threw";
    echo = pingFut.bytes();
    ASSERT_EQ(echo, payload) << "Pong echo does not match payload";
}

// ===========================================================================
// Test 3: PingFutureEchoEmpty
// ===========================================================================
TEST(Client, PingFutureEchoEmpty) {
    GatewayHarness h(830, 831);
    h.build();

    h.client->onConnected([](int64_t){});

    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    Bytes emptyPayload{};
    auto pingFut = h.pingOp(emptyPayload);

    h.pumpGwUntil([&]() {
        return pingFut.ready();
    });

    ASSERT_TRUE(pingFut.waitReady())
        << "empty ping() future did not resolve within timeout";

    Bytes echo;
    ASSERT_TRUE(pingFut.ok()) << pingFut.describe();
    echo = pingFut.bytes();
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

    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    Bytes p1 = {0x01, 0x02, 0x03};
    Bytes p2 = {0xAA, 0xBB};
    Bytes p3 = {'x', 'y', 'z', 0x00, 0xFF};

    // Submit all 3 pings from the app thread before any gateway polling.
    auto f1 = h.pingOp(p1);
    auto f2 = h.pingOp(p2);
    auto f3 = h.pingOp(p3);

    // Drive the gateway until all 3 futures are ready.
    h.pumpGwUntil([&]() {
        return (f1.ready()) &&
               (f2.ready()) &&
               (f3.ready());
    });

    ASSERT_TRUE(f1.waitReady()) << "f1 not ready";
    ASSERT_TRUE(f2.waitReady()) << "f2 not ready";
    ASSERT_TRUE(f3.waitReady()) << "f3 not ready";

    Bytes e1, e2, e3;
    ASSERT_TRUE(f1.ok()) << f1.describe() << "f1 threw";
    e1 = f1.bytes();
    ASSERT_TRUE(f2.ok()) << f2.describe() << "f2 threw";
    e2 = f2.bytes();
    ASSERT_TRUE(f3.ok()) << f3.describe() << "f3 threw";
    e3 = f3.bytes();

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

    auto fut = h.pingOp(Bytes{'a', 'b', 'c'});

    // F8c C3: "not connected" is decided on the duty thread — a NotConnected
    // completion, no gateway round trip.
    ASSERT_TRUE(fut.waitReady())
        << "ping-without-session was not completed promptly";

    EXPECT_EQ(fut.status(), OpStatus::NotConnected) << fut.describe()
        << "ping() without session must complete NotConnected, not return echo";
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

    auto fut = h.connectOp();

    // Drive the gateway to send the error reply.
    h.pumpGwUntil([&]() {
        return errFired.load(std::memory_order_acquire);
    });

    ASSERT_TRUE(fut.waitReady())
        << "connect() future did not resolve after ErrorResponse";

    EXPECT_TRUE(fut.failed()) << fut.describe()
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
    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    // Now send a ping — gateway will NOT reply.
    auto pingFut = h.pingOp(Bytes{'x'});

    // The future must fail within pingTimeoutMs + generous test margin (3 s).
    ASSERT_TRUE(pingFut.waitReady())
        << "ping() future did not fail within timeout — potential hang";

    EXPECT_TRUE(pingFut.failed()) << pingFut.describe()
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
    auto fut = h.connectOp();

    // close() must join the runner and complete the connect (Closed) — the
    // op is either still in the command ring (close()'s final drain) or
    // pending on the duty side; either way it completes exactly once.
    ASSERT_NO_THROW(h.client->close())
        << "close() threw while pending connect was in-flight";

    ASSERT_TRUE(fut.ready()) << "connect was not completed by close()";
    EXPECT_EQ(fut.status(), OpStatus::Closed) << fut.describe();
    EXPECT_EQ(h.cq->timesCompleted(fut.token()), 1);
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

        auto fut = h.connectOp();

        h.pumpGwUntil([&]() {
            return connectedFired.load(std::memory_order_acquire);
        });

        // Wait for the future to resolve so we know we're in Connected state.
        EXPECT_TRUE(fut.waitReady());

        // Dtor fires here (GatewayHarness destructs client, then gw, then driver).
        // The client dtor calls close() → joins runner → stops agent → cleans up.
    });
}

// ===========================================================================
// Test 11 (MC-1) [CC-8]: ConnectThenImmediatePingNotRejected — FIX F1
//
// The MOMENT connect().get() returns, call ping() on the SAME app thread with
// NO intervening state poll.  Before F1 the state snapshot was published only
// at the END of doWork (after the observer had already woken this thread), so
// ping() could read a stale Connecting/Disconnected snapshot and reject with
// "not connected".  With F1 the snapshot is published BEFORE the observer, so
// the ping must NOT be rejected for "not connected".
// ===========================================================================
TEST(Client, ConnectThenImmediatePingNotRejected) {
    GatewayHarness h(910, 911);

    // Deterministic F1 gate:  sample the state snapshot INSIDE onConnected, which
    // fires on the runner thread from within the egress observer — i.e. at the
    // exact instant the connect promise is resolved and BEFORE the end-of-doWork
    // catch-all publish.  With F1 the pre-observer publishSnapshot() has already
    // run, so state() MUST already be Connected here.  Without F1 the snapshot is
    // still the stale (pre-doWork) value at this point — the race the CC-7 miss
    // observed.  This does not depend on runner-vs-app timing, so it is stable.
    std::atomic<bool>              connectedFired{false};
    std::atomic<rcs::SessionState> stateAtCallback{rcs::SessionState::Disconnected};

    // build() constructs the Client (and starts the runner).
    h.build(/*gwReplyOK=*/true);

    // Register the callback BEFORE connect() (FIX F4 callback contract).
    h.client->onConnected([&](int64_t) {
        stateAtCallback.store(h.client->state(), std::memory_order_release);
        connectedFired.store(true, std::memory_order_release);
    });

    auto connFut = h.connectOp();

    // Drive the gateway until onConnected has fired.
    h.pumpGwUntil([&]() { return connectedFired.load(std::memory_order_acquire); });

    ASSERT_TRUE(connFut.waitReady())
        << "connect() future did not resolve";
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    ASSERT_TRUE(connectedFired.load()) << "onConnected did not fire";
    // The core F1 assertion: at the instant the observer resolved the promise,
    // the published snapshot was ALREADY Connected (published before the observer).
    EXPECT_EQ(stateAtCallback.load(), rcs::SessionState::Connected)
        << "state snapshot was stale inside onConnected — F1 regression "
           "(snapshot must be published BEFORE the egress observer runs)";

    // Belt-and-braces: a ping issued right now must not be rejected "not connected".
    auto pingFut = h.pingOp(Bytes{'p', 'i', 'n', 'g'});
    h.pumpGwUntil([&]() {
        return pingFut.ready();
    });
    ASSERT_TRUE(pingFut.waitReady())
        << "immediate ping() future did not resolve";
    Bytes echo;
    ASSERT_TRUE(pingFut.ok()) << pingFut.describe()
        << "immediate ping after connect() was rejected — F1 regression";
    echo = pingFut.bytes();
    EXPECT_EQ(echo, (Bytes{'p', 'i', 'n', 'g'}));
}

// ===========================================================================
// Test 12 (MC-2) [CC-8]: ErrorResponseFailsInFlightPing — FIX F2
//
// A ping is in-flight; the gateway replies ErrorResponse{SessionExpired} with
// the ping's correlationId.  The ping future must fail PROMPTLY (well under
// pingTimeoutMs) with the server error — not sit until the timeout.
// ===========================================================================
TEST(Client, ErrorResponseFailsInFlightPing) {
    GatewayHarness h(920, 921);
    h.pingTimeoutMs = 10000;  // long: prove we fail via the error, not the timeout
    h.build();

    h.client->onConnected([](int64_t){});
    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    // Arm the gateway to answer the next Ping with ErrorResponse{SessionExpired}.
    h.gw->pingReplyError = true;
    h.gw->pingErrorCode  = static_cast<int16_t>(rcc::ErrorCode::SessionExpired);

    const auto t0 = std::chrono::steady_clock::now();
    auto pingFut = h.pingOp(Bytes{'x', 'y'});

    h.pumpGwUntil([&]() {
        return pingFut.ready();
    });

    ASSERT_TRUE(pingFut.waitReady())
        << "ping() future did not resolve after ErrorResponse{SessionExpired}";
    EXPECT_TRUE(pingFut.failed()) << pingFut.describe()
        << "ping() must FAIL on ErrorResponse{SessionExpired}";

    const auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(elapsed, std::chrono::milliseconds(h.pingTimeoutMs))
        << "ping failed by TIMEOUT, not promptly by the server error (F2 regression)";
}

// ===========================================================================
// Test 13 (MC-3) [CC-8]: ConnectResolveVsTimeoutExactlyOnce — FIX F5
//
// A short connectTimeoutMs races the gateway's OK reply.  Whatever wins, the
// connect future must be satisfied EXACTLY once (either resolved OR failed) and
// never double-satisfy / lose the resolution.  We assert the future becomes
// ready and get() has a single, well-defined outcome (no crash, no hang).
// ===========================================================================
TEST(Client, ConnectResolveVsTimeoutExactlyOnce) {
    GatewayHarness h(930, 931);
    h.connectTimeoutMs = 120;  // short — races the gateway reply
    h.build(/*gwReplyOK=*/true);

    // Pump the gateway on a background thread so the OK reply may land right
    // around the deadline.
    std::atomic<bool> pumpStop{false};
    std::thread pump([&] {
        while (!pumpStop.load(std::memory_order_acquire)) {
            h.pollGw(16);
            std::this_thread::sleep_for(1ms);
        }
    });

    auto fut = h.connectOp();

    // Exactly one outcome; must become ready (resolved or timed out), never hang.
    ASSERT_TRUE(fut.waitReady())
        << "connect() future neither resolved nor timed out — possible lost resolution";

    // Exactly one completion: either Ok or Timeout, never both.
    EXPECT_TRUE(fut.status() == OpStatus::Ok || fut.status() == OpStatus::Timeout) << fut.describe();
    EXPECT_EQ(h.cq->timesCompleted(fut.token()), 1) << "connect completed more than once";

    pumpStop.store(true, std::memory_order_release);
    pump.join();
}

// ===========================================================================
// Test 14 (MC-5) [CC-8]: RedirectToUnresolvableEndpointNoTerminate — FIX F8
//
// The gateway answers the ConnectRequest with ErrorResponse{Redirect,
// message=<unresolvable endpoint>}.  This drives the full agent → session →
// io_.retarget() path.  retarget is now noexcept and non-throwing, so the
// process must NOT std::terminate.  The connect eventually fails (redirect cap /
// timeout) but the client stays alive and closes cleanly.
// ===========================================================================
TEST(Client, RedirectToUnresolvableEndpointNoTerminate) {
    GatewayHarness h(940, 941);
    h.connectTimeoutMs = 400;  // keep the test short; redirect resends then times out
    h.build(/*gwReplyOK=*/false);

    // Arm the redirect BEFORE connecting.
    h.gw->redirectOnConnect = true;
    h.gw->redirectEndpoint  = "aeron:udp?endpoint=192.0.2.1:59999"; // TEST-NET-1, unroutable

    auto fut = h.connectOp();

    // Drive the gateway so it delivers the redirect(s); the agent will attempt
    // to retarget to the unresolvable endpoint on the runner thread.
    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    // The key assertion: we got here with NO std::terminate.  The future must be
    // resolved (as a failure) within a bounded time — never hang.
    ASSERT_TRUE(fut.waitReady())
        << "connect() future never completed after unresolvable redirect";
    EXPECT_TRUE(fut.failed()) << fut.describe()
        << "connect() to an unresolvable redirect endpoint must fail (not succeed)";

    // Client must still be usable / closeable — no crash.
    ASSERT_NO_THROW(h.client->close());
}

// ===========================================================================
// Test 15 (MC-4 unit) [CC-8]: CoherentStateSnapshotAfterConnect — FIX F3
//
// After a successful connect, the (state, sessionId) pair read via the coherent
// accessor must never be Connected with sessionId==0 (the packing invariant),
// and state()/sessionId() (the split accessors) must agree with it.
// ===========================================================================
TEST(Client, CoherentStateSnapshotAfterConnect) {
    GatewayHarness h(950, 951);
    h.build(/*gwReplyOK=*/true);

    auto connFut = h.connectOp();
    h.pumpGwUntil([&]() {
        return h.client->state() == rcs::SessionState::Connected;
    });
    ASSERT_TRUE(connFut.waitReady());
    ASSERT_TRUE(connFut.ok()) << connFut.describe();

    // The public state() must be Connected; the assigned sessionId (42) is set.
    EXPECT_EQ(h.client->state(), rcs::SessionState::Connected);

    // Repeatedly sample: Connected must NEVER pair with sessionId 0.
    // (We can only observe via the public state(); the coherent accessor is
    // exercised inside ping() — a successful ping proves a valid sessionId was
    // stamped, i.e. the pair was coherent.)
    auto pingFut = h.pingOp(Bytes{0x01});
    h.pumpGwUntil([&]() {
        return pingFut.ready();
    });
    ASSERT_TRUE(pingFut.waitReady());
    Bytes echo;
    ASSERT_TRUE(pingFut.ok()) << pingFut.describe()
        << "ping stamped with a coherent (Connected, sessionId!=0) snapshot must succeed";
    echo = pingFut.bytes();
    EXPECT_EQ(echo, (Bytes{0x01}));
}

// ===========================================================================
// Test 16 [CC-8]: IdleModeBalancedAndLowLatencyBothConnectAndPing
//
// Verify that both IdleMode::Balanced (default) and IdleMode::LowLatency
// produce a functional client that can connect and ping successfully.
// This is a correctness gate for the new idle-mode knob — it does NOT measure
// latency (that is the benchmark's job), only that neither mode breaks the
// connect/ping path.
// ===========================================================================
TEST(Client, IdleModeBalancedAndLowLatencyBothConnectAndPing) {
    // --- Part A: IdleMode::Balanced (default) ---
    {
        GatewayHarness h(960, 961);
        // Default ClientConfig has idleMode = IdleMode::Balanced.
        h.build(/*gwReplyOK=*/true);

        auto connFut = h.connectOp();
        h.pumpGwUntil([&]() {
            return h.client->state() == rcs::SessionState::Connected;
        });
        ASSERT_TRUE(connFut.waitReady())
            << "Balanced: connect() future did not resolve";
        ASSERT_TRUE(connFut.ok()) << connFut.describe() << "Balanced: connect() threw";

        auto pingFut = h.pingOp(Bytes{'b', 'a', 'l'});
        h.pumpGwUntil([&]() {
            return pingFut.ready();
        });
        ASSERT_TRUE(pingFut.waitReady())
            << "Balanced: ping() future did not resolve";
        Bytes echo;
        ASSERT_TRUE(pingFut.ok()) << pingFut.describe() << "Balanced: ping() threw";
    echo = pingFut.bytes();
        EXPECT_EQ(echo, (Bytes{'b', 'a', 'l'})) << "Balanced: echo mismatch";
    }

    // --- Part B: IdleMode::LowLatency ---
    {
        // Build the client manually with LowLatency mode set.
        std::string agentDir = uniqueDir();
        std::string aeronDir = agentDir + "/aeron";

        ClientConfig cfg;
        cfg.gatewayChannel   = "aeron:ipc";
        cfg.gatewayStreamId  = 970;
        cfg.egressChannel    = "aeron:ipc";
        cfg.egressStreamId   = 971;
        cfg.dataDir          = agentDir;
        cfg.connectTimeoutMs = 5000;
        cfg.pingTimeoutMs    = 5000;
        cfg.idleMode         = IdleMode::LowLatency;   // <-- the key flag

        auto client = std::make_unique<Client>(cfg);
        std::this_thread::sleep_for(150ms);

        // Attach a test gateway to the same driver.
        auto gw = std::make_unique<TestGateway>(aeronDir);
        gw->replyWithOK = true;
        gw->echoPong    = true;
        gw->start(/*inStream=*/970, /*egStream=*/971);

        auto pumpUntil = [&](auto pred) {
            for (int i = 0; i < kPollMax; ++i) {
                gw->poll(16);
                if (pred()) return true;
                std::this_thread::sleep_for(kPollSleep);
            }
            return false;
        };

        test_support::TestCq cq(*client);
        auto connFut = cq.issue([&](Completions& q, Token t) { return client->connect(q, t); });
        pumpUntil([&]() { return client->state() == rcs::SessionState::Connected; });

        ASSERT_TRUE(connFut.waitReady())
            << "LowLatency: connect() future did not resolve";
        ASSERT_TRUE(connFut.ok()) << connFut.describe() << "LowLatency: connect() threw";
        ASSERT_EQ(client->state(), rcs::SessionState::Connected)
            << "LowLatency: state() != Connected after connect()";

        auto pingFut = cq.issue([&](Completions& q, Token t) {
            return client->ping(Bytes{'l', 'l', 'p'}, q, t);
        });
        pumpUntil([&]() { return pingFut.ready(); });

        ASSERT_TRUE(pingFut.waitReady())
            << "LowLatency: ping() future did not resolve";
        Bytes echo;
        ASSERT_TRUE(pingFut.ok()) << pingFut.describe() << "LowLatency: ping() threw";
    echo = pingFut.bytes();
        EXPECT_EQ(echo, (Bytes{'l', 'l', 'p'})) << "LowLatency: echo mismatch";

        client->close();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
}

// ===========================================================================
// fix1 C1: a blocking send() from inside a duty-cycle callback must never
// terminate the process, in either runner mode. These two cover Thread mode
// (the real background runner thread); InvokerClientTest.cpp covers Invoker.
// ===========================================================================

// The onConnected handler deliberately does NOT catch: it runs on the runner
// thread, already inside dutyCycle() (inDutyCycle_ is set) — the pump hook
// declines with DeclinedInCycle and sendBlocking throws immediately.
// Client::invokeUserCallback_ must catch it so the runner thread (and the
// process) survives; the exception count must reflect the catch.
TEST(Client, BlockingSendFromCallbackDoesNotTerminateThreadRegression) {
    GatewayHarness h(880, 881);
    h.build(/*gwReplyOK=*/true);

    std::atomic<bool> ran{false};
    h.client->onConnected([&](int64_t) {
        // Set BEFORE the throwing call: p.send() below throws and propagates
        // OUT of this lambda (caught by Client::invokeUserCallback_, which
        // wraps the whole callback dispatch) — nothing after it would run.
        ran.store(true, std::memory_order_release);
        ProducerConfig pcfg;
        pcfg.acks     = Acks::None;
        pcfg.lingerUs = 0;
        auto p = h.client->testProducerWithRingCap(pcfg, 4 * 1024);
        const TopicHandle topic{1, 1};
        const std::vector<uint8_t> val(64, 0x7A);
        uint64_t ok = 0;
        for (; ok < 10'000; ++ok) {
            if (p.trySend(topic, Key(ok), std::span<const uint8_t>(val)) != SendResult::Ok) break;
        }
        p.send(topic, Key(ok), std::span<const uint8_t>(val)); // NOT wrapped in try/catch
    });

    auto fut = h.connectOp();
    h.pumpGwUntil([&]() { return ran.load(std::memory_order_acquire); });

    ASSERT_TRUE(fut.waitReady())
        << "connect() future did not resolve — the runner thread may have died";
    ASSERT_TRUE(fut.ok()) << fut.describe();
    EXPECT_TRUE(ran.load());

    // The runner thread survived: a subsequent ping still completes.
    auto pingFut = h.pingOp(Bytes{'o', 'k'});
    h.pumpGwUntil([&] { return pingFut.ready(); });
    ASSERT_TRUE(pingFut.waitReady())
        << "runner thread did not survive the uncaught callback exception";
    EXPECT_TRUE(pingFut.ok()) << pingFut.describe();

    // fix2 minor-5: callbackExceptions_ is a PLAIN counter (shared-nothing
    // rule — no atomics), written only on the runner thread. Reading it from
    // this (test) thread is only safe once close() has joined the runner —
    // we already waited on the test's own flag (`ran`, via pumpGwUntil above)
    // before getting here, but that only proves the callback STARTED; close()
    // is what actually guarantees no writer is left running.
    h.client->close();
    EXPECT_GT(h.client->testCallbackExceptionCount(), 0u)
        << "the uncaught throw inside onConnected must be caught by "
           "Client::invokeUserCallback_ around the callback dispatch, not escape it";
}

// ===========================================================================
// fix1 C2/I3: a ring "closed" word — send() after close() returns/throws
// Closed, consistent with producer() after close() (Task 12).
// ===========================================================================
TEST(Client, SendAfterClientCloseReturnsClosedRegression) {
    GatewayHarness h(884, 885);
    h.build(/*gwReplyOK=*/true);

    ProducerConfig pcfg;
    pcfg.acks = Acks::None;
    auto p = h.client->testProducerWithRingCap(pcfg, 4 * 1024);
    const TopicHandle topic{1, 1};
    const std::vector<uint8_t> val(64, 0x7C);
    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, Key(ok), std::span<const uint8_t>(val)) != SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u);

    h.client->close();

    EXPECT_EQ(p.trySend(topic, Key(ok), std::span<const uint8_t>(val)), SendResult::Closed);
    EXPECT_THROW(p.send(topic, Key(ok), std::span<const uint8_t>(val)), std::logic_error);
}

// Proves the DeclinedInCycle path is used (fail FAST) rather than falling back
// to the heartbeat-bounded wait: agentLivenessTimeoutMs is set absurdly high,
// so if sendBlocking ever waited it out the test would hang for minutes. It
// must return almost immediately regardless.
TEST(Client, ThreadModeRunnerBlockingSendDoesNotStallRegression) {
    GatewayHarness h(882, 883);
    h.build(/*gwReplyOK=*/true);

    std::atomic<bool> threw{false};
    const auto t0 = std::chrono::steady_clock::now();
    std::atomic<int64_t> elapsedMs{-1};

    h.client->onConnected([&](int64_t) {
        ProducerConfig pcfg;
        pcfg.acks                   = Acks::None;
        pcfg.lingerUs               = 0;
        pcfg.agentLivenessTimeoutMs = 60'000; // would hang minutes if this were ever waited out
        auto p = h.client->testProducerWithRingCap(pcfg, 4 * 1024);
        const TopicHandle topic{1, 1};
        const std::vector<uint8_t> val(64, 0x7B);
        uint64_t ok = 0;
        for (; ok < 10'000; ++ok) {
            if (p.trySend(topic, Key(ok), std::span<const uint8_t>(val)) != SendResult::Ok) break;
        }
        try {
            p.send(topic, Key(ok), std::span<const uint8_t>(val));
        } catch (const std::runtime_error&) {
            threw.store(true, std::memory_order_release);
        }
        elapsedMs.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count(),
            std::memory_order_release);
    });

    auto fut = h.connectOp();
    h.pumpGwUntil([&]() { return elapsedMs.load(std::memory_order_acquire) >= 0; });

    ASSERT_TRUE(fut.waitReady());
    ASSERT_TRUE(fut.ok()) << fut.describe();
    EXPECT_TRUE(threw.load());
    EXPECT_LT(elapsedMs.load(), 5000)
        << "sendBlocking waited instead of failing fast on DeclinedInCycle "
           "(agentLivenessTimeoutMs=60s would otherwise make this take minutes)";

    h.client->close();
}

// ---------------------------------------------------------------------------
// F12: ClientConfig -> transport mapping (toAgentConfig).  No driver needed.
// ---------------------------------------------------------------------------
TEST(ClientConfigMapping, MtuAndSocketBuffersMapToTransportRegression) {
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    rune::client::ClientConfig def;
    EXPECT_EQ(def.mtuBytes, 1408u) << "jumbo MTU must stay opt-in, never the default";
    const auto a = rune::client::toAgentConfig(def);
    EXPECT_EQ(a.transport.mtuLength, 1408u);
    EXPECT_EQ(a.transport.socketSndBufBytes, def.socketBufferBytes);
    EXPECT_EQ(a.transport.socketRcvBufBytes, def.socketBufferBytes);

    rune::client::ClientConfig jumbo;
    jumbo.mtuBytes          = 8192;
    jumbo.socketBufferBytes = 4u * 1024 * 1024;
    const auto b = rune::client::toAgentConfig(jumbo);
    EXPECT_EQ(b.transport.mtuLength, 8192u);
    EXPECT_EQ(b.transport.socketSndBufBytes, 4u * 1024 * 1024);
    EXPECT_EQ(b.transport.socketRcvBufBytes, 4u * 1024 * 1024);
    EXPECT_FALSE(rune::client::transport::validate(b.transport).has_value());
}

TEST(ClientConfigMapping, EnvMtuOverridesClientConfig) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "4096", 1);
    rune::client::ClientConfig c;
    c.mtuBytes = 8192;
    const auto a = rune::client::toAgentConfig(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    EXPECT_EQ(a.transport.mtuLength, 4096u);
}

// ---------------------------------------------------------------------------
// F8c C0: ClientAgent's lifecycle flags (started_/stopped_) are plain owner
// state — no atomics.  A stop requested from another thread goes through the
// runner shutdown path: Client::close() ends the runner loop, JOINS it, and only
// then calls ClientAgent::stop().  This connected Thread-mode client (the runner
// has run many duty cycles, each reading stopped_) is closed from the app
// thread; under TSan the join edge must order the runner's reads before the
// stop() write (RED by mutation: stop() before the join -> data race report).
// No sleeps: the test thread pumps the gateway until each step is observed.
// ---------------------------------------------------------------------------
TEST(ClientLifecycle, CloseStopsAgentOnlyAfterRunnerJoinRegression) {
    const std::string dir = uniqueDir();
    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = 990;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = 991;
    cfg.dataDir          = dir;
    cfg.connectTimeoutMs = 30'000;
    auto client = std::make_unique<Client>(cfg);   // Thread mode: runner is live
    auto gw = std::make_unique<TestGateway>(dir + "/aeron");
    gw->replyWithOK = true;
    gw->start(990, 991);
    test_support::TestCq cq(*client);
    auto fut = cq.issue([&](Completions& q, Token t) { return client->connect(q, t); });
    for (int i = 0; i < 50'000'000 && !fut.ready(); ++i) {
        gw->poll(16);
    }
    ASSERT_TRUE(fut.ready());
    ASSERT_TRUE(fut.ok()) << fut.describe();
    // A duty cycle AFTER the last runner->app synchronisation: the runner drains
    // this ping and offers it over Aeron IPC; the gateway (this thread) seeing it
    // proves that cycle ran — and its doWork() read of ClientAgent::stopped_ is
    // ordered before the close only by the join (no Pong: nothing else syncs).
    gw->echoPong = false;
    auto pingFut = cq.issue([&](Completions& q, Token t) { return client->ping(Bytes{'z'}, q, t); });
    for (int i = 0; i < 50'000'000 && gw->pingsSeen.load() == 0; ++i) gw->poll(16);
    ASSERT_EQ(gw->pingsSeen.load(), 1);
    client->close();                                // from the app thread
    EXPECT_THROW((void)client->producer(), std::logic_error);  // closed for good
    gw.reset();
    client.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
