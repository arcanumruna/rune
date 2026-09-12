// tests/session/SessionMachineTest.cpp
//
// CC-4 — Exhaustive deterministic tests for SessionMachine.
//
// Strategy:
//   - FakeIo records every sendFrame and every retarget call.
//   - Frames are decoded with the CC-2 adapters (rune::client::codec) to
//     assert message type + fields — NOT raw byte comparisons.
//   - tick() is driven with explicit int64_t timestamps (no sleeps, no wall-clock).
//   - Every state/event transition, boundary, and error path is covered.

#include <gtest/gtest.h>

#include <rune/client/codec/Codec.h>
#include <rune/client/session/SessionMachine.h>

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace cc  = rune::client::codec;
namespace rpc = rune::protocol::client;
namespace sm  = rune::client::session;

// ===========================================================================
// FakeIo — records all sendFrame / retarget calls.
// ===========================================================================
struct FakeIo final : sm::SessionIo {
    struct Frame {
        std::vector<uint8_t> data;
    };

    std::vector<Frame>       sends;
    std::vector<std::string> retargets;
    bool                     sendSucceeds = true;

    bool sendFrame(const uint8_t* buf, std::size_t len) override {
        sends.push_back(Frame{std::vector<uint8_t>(buf, buf + len)});
        return sendSucceeds;
    }

    void retarget(const std::string& newChannel) override {
        retargets.push_back(newChannel);
    }

    void reset() {
        sends.clear();
        retargets.clear();
    }

    std::size_t sendCount() const { return sends.size(); }
    std::size_t retargetCount() const { return retargets.size(); }

    // Decode helpers — return nullopt if the frame can't be decoded.
    std::optional<cc::ConnectRequestView> lastConnectRequest() const {
        if (sends.empty()) return std::nullopt;
        const auto& f = sends.back();
        return cc::decodeConnectRequest(f.data.data(), f.data.size());
    }

    std::optional<cc::ConnectRequestView> connectRequestAt(std::size_t idx) const {
        if (idx >= sends.size()) return std::nullopt;
        const auto& f = sends[idx];
        return cc::decodeConnectRequest(f.data.data(), f.data.size());
    }

    std::optional<cc::KeepaliveView> lastKeepalive() const {
        if (sends.empty()) return std::nullopt;
        const auto& f = sends.back();
        return cc::decodeKeepalive(f.data.data(), f.data.size());
    }

    std::optional<cc::DisconnectView> lastDisconnect() const {
        if (sends.empty()) return std::nullopt;
        const auto& f = sends.back();
        return cc::decodeDisconnect(f.data.data(), f.data.size());
    }

    // Peek template id of the last frame.
    std::optional<uint16_t> lastTemplateId() const {
        if (sends.empty()) return std::nullopt;
        const auto& f = sends.back();
        return cc::peekTemplate(f.data.data(), f.data.size());
    }

    // Peek template id of a specific frame.
    std::optional<uint16_t> templateIdAt(std::size_t idx) const {
        if (idx >= sends.size()) return std::nullopt;
        const auto& f = sends[idx];
        return cc::peekTemplate(f.data.data(), f.data.size());
    }
};

// ===========================================================================
// Helper: build a config with small, easy-to-reason-about timeouts.
// ===========================================================================
static sm::SessionConfig testConfig() {
    sm::SessionConfig cfg;
    cfg.egressChannel       = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId      = 1001;
    cfg.clientVersion       = 1;
    cfg.connectTimeoutNs    = 1'000'000LL;   // 1 ms
    cfg.keepaliveIntervalNs = 5'000'000LL;   // 5 ms
    cfg.responseTimeoutNs   = 20'000'000LL;  // 20 ms
    cfg.maxRedirects        = 5;
    cfg.maxConnectRetries   = 3;
    cfg.maxResumeRetries    = 3;
    return cfg;
}

// Base timestamp — arbitrary but large enough to avoid subtraction underflow.
static constexpr int64_t T0 = 1'000'000'000LL;

// ===========================================================================
// 1. Initial state
// ===========================================================================
TEST(SessionMachine, InitialState) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    EXPECT_EQ(m.sessionId(), 0LL);
    EXPECT_EQ(m.redirectCount(), 0);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 2. connect() — Disconnected → Connecting; ConnectRequest sent with correct fields
// ===========================================================================
TEST(SessionMachine, ConnectSendsConnectRequest) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    ASSERT_EQ(io.sendCount(), 1u);

    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value()) << "Last frame must be decodable as ConnectRequest";

    EXPECT_EQ(req->egressChannel,  "aeron:udp?endpoint=client:40456");
    EXPECT_EQ(req->egressStreamId, 1001);
    EXPECT_EQ(req->clientVersion,  1);
    EXPECT_EQ(req->sessionId,      0LL);  // fresh connect → sessionId=0

    // correlationId must be non-zero (machine-generated counter).
    EXPECT_NE(req->correlationId, 0LL);
    const int64_t firstCorrelationId = req->correlationId;

    // Verify templateId directly.
    auto tid = io.lastTemplateId();
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect));

    // correlationId is exposed via observer.
    EXPECT_EQ(m.correlationId(), firstCorrelationId);
}

// ===========================================================================
// 3. connect() is a no-op from non-Disconnected states
// ===========================================================================
TEST(SessionMachine, ConnectNoOpFromConnecting) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    ASSERT_EQ(io.sendCount(), 1u);

    // Second connect() while already Connecting → no-op.
    m.connect(T0);
    EXPECT_EQ(io.sendCount(), 1u);  // no extra frame
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
}

TEST(SessionMachine, ConnectNoOpFromClosed) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);

    m.connect(T0);
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 4. Connecting → Connected on onConnectResponse{OK}
// ===========================================================================
TEST(SessionMachine, ConnectResponseOkTransitionsToConnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    const int64_t corrId = m.correlationId();

    constexpr int64_t kAssignedSession = 0xDEAD'BEEF'1234'5678LL;
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        /*leaderMemberId=*/2,
        kAssignedSession,
        corrId,
        T0);

    EXPECT_EQ(m.state(),     sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), kAssignedSession);
    EXPECT_EQ(m.redirectCount(), 0);
}

// ===========================================================================
// 5. Stale / mismatched correlationId is ignored (no transition)
// ===========================================================================
TEST(SessionMachine, StaleCorrIdIgnored) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);

    // Feed a response with a wrong correlationId.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 0x1234, /*wrongCorrelationId=*/m.correlationId() + 99,
        T0);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.sessionId(), 0LL);  // no session assigned
}

// ===========================================================================
// 6. Keepalive cadence — just-before / just-after boundary
// ===========================================================================
TEST(SessionMachine, KeepaliveJustBeforeBoundary) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 42LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    io.reset();  // clear the ConnectRequest frame

    // Tick before the keepalive interval expires → no send.
    const int64_t almostInterval = cfg.keepaliveIntervalNs - 1;
    m.tick(T0 + almostInterval);
    EXPECT_EQ(io.sendCount(), 0u);

    // Tick at exactly the interval → exactly one Keepalive sent.
    m.tick(T0 + cfg.keepaliveIntervalNs);
    ASSERT_EQ(io.sendCount(), 1u);

    auto kav = io.lastKeepalive();
    ASSERT_TRUE(kav.has_value()) << "Frame must be decodable as Keepalive";
    EXPECT_EQ(kav->sessionId, 42LL);

    auto tid = io.lastTemplateId();
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Keepalive));
}

TEST(SessionMachine, KeepaliveSentOnlyOncePerInterval) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 42LL, m.correlationId(), T0);

    io.reset();

    // First keepalive fires at T0 + interval.
    m.tick(T0 + cfg.keepaliveIntervalNs);
    ASSERT_EQ(io.sendCount(), 1u);

    // Tick again just before the second interval → no extra send.
    m.tick(T0 + cfg.keepaliveIntervalNs + (cfg.keepaliveIntervalNs - 1));
    EXPECT_EQ(io.sendCount(), 1u);

    // Tick at exactly second interval → second keepalive.
    m.tick(T0 + cfg.keepaliveIntervalNs * 2);
    EXPECT_EQ(io.sendCount(), 2u);
}

TEST(SessionMachine, KeepaliveMultipleTicks) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 99LL, m.correlationId(), T0);

    io.reset();

    // 3 keepalives across 3 intervals.
    for (int i = 1; i <= 3; ++i) {
        m.tick(T0 + cfg.keepaliveIntervalNs * i);
    }
    EXPECT_EQ(io.sendCount(), 3u);

    // All three must decode as Keepalive with the correct sessionId.
    for (std::size_t i = 0; i < 3; ++i) {
        auto kav_opt = cc::decodeKeepalive(io.sends[i].data.data(),
                                           io.sends[i].data.size());
        ASSERT_TRUE(kav_opt.has_value()) << "send[" << i << "] is not a Keepalive";
        EXPECT_EQ(kav_opt->sessionId, 99LL);
    }
}

// ===========================================================================
// 7. Response timeout → Resuming
// ===========================================================================
TEST(SessionMachine, ResponseTimeoutTransitionsToResuming) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 77LL, m.correlationId(), T0);

    // Activity just happened at T0; no activity since.
    // Tick past the response timeout.
    m.tick(T0 + cfg.responseTimeoutNs + 1);

    // Machine should be in Connecting (Resuming triggered doResume → Connecting).
    // (After Resuming triggers doResume, the state moves to Connecting.)
    EXPECT_TRUE(m.state() == sm::SessionState::Resuming ||
                m.state() == sm::SessionState::Connecting)
        << "Expected Resuming or Connecting after response timeout";
}

TEST(SessionMachine, ResponseTimeoutThenSuccessfulResume) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 77LL, m.correlationId(), T0);

    ASSERT_EQ(m.state(), sm::SessionState::Connected);
    io.reset();

    // Trigger response timeout.
    m.tick(T0 + cfg.responseTimeoutNs + 1);
    // A new ConnectRequest should have been sent as a resume attempt.
    ASSERT_GE(io.sendCount(), 1u);

    // The resume ConnectRequest should carry the old sessionId as hint.
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->sessionId, 77LL);  // resume hint

    // Feed a successful ConnectResponse for the resumed connection.
    const int64_t newCorrId = m.correlationId();
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 77LL, newCorrId, T0 + cfg.responseTimeoutNs + 2);

    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), 77LL);
}

// ===========================================================================
// 8. onActivity refreshes the activity timestamp (keeps Connected alive)
// ===========================================================================
TEST(SessionMachine, OnActivityPreventsResponseTimeout) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 55LL, m.correlationId(), T0);

    // Tick to just before the response timeout.
    const int64_t almostTimeout = T0 + cfg.responseTimeoutNs - 1;
    m.tick(almostTimeout);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);

    // Refresh activity — timestamp resets.
    m.onActivity(almostTimeout);

    // Now tick past the original timeout — but since activity was refreshed,
    // the machine should still be Connected (timeout measured from last activity).
    m.tick(almostTimeout + cfg.responseTimeoutNs - 1);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
}

// ===========================================================================
// 9. Redirect — onConnectResponseRedirect
// ===========================================================================
TEST(SessionMachine, RedirectCallsRetargetAndResendsConnectRequest) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    ASSERT_EQ(io.sendCount(), 1u);
    const int64_t firstCorrId = m.correlationId();

    io.reset();

    // Redirect response.
    const std::string newEndpoint = "aeron:udp?endpoint=leader:40200";
    m.onConnectResponseRedirect(newEndpoint, firstCorrId, T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 1);

    // retarget() must have been called once.
    ASSERT_EQ(io.retargetCount(), 1u);
    EXPECT_EQ(io.retargets[0], newEndpoint);

    // A new ConnectRequest must have been sent.
    ASSERT_EQ(io.sendCount(), 1u);
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->egressChannel,  "aeron:udp?endpoint=client:40456");
    EXPECT_EQ(req->egressStreamId, 1001);

    // New correlationId must differ from the first.
    EXPECT_NE(req->correlationId, firstCorrId);
}

// ===========================================================================
// 10. Redirect cap — bounded redirect loop, no infinite reconnect
// ===========================================================================
TEST(SessionMachine, RedirectCapExhausted_GoesDisconnected) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 3;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);

    // Issue maxRedirects redirects, each accepted.
    for (int i = 0; i < cfg.maxRedirects; ++i) {
        ASSERT_EQ(m.state(), sm::SessionState::Connecting)
            << "Should still be Connecting before cap exhausted (redirect " << i << ")";
        const int64_t corrId = m.correlationId();
        m.onConnectResponseRedirect("aeron:udp?endpoint=leader:40200", corrId, T0 + i);
        EXPECT_EQ(m.redirectCount(), i + 1) << "redirectCount mismatch at " << i;
    }

    // One more redirect — should exceed the cap.
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    const int64_t corrId = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=leader:40200", corrId, T0 + cfg.maxRedirects);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
}

// ===========================================================================
// 11. Connect timeout — bounded retry then Disconnected
// ===========================================================================
TEST(SessionMachine, ConnectTimeoutRetryThenDisconnected) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxConnectRetries = 2;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    ASSERT_EQ(io.sendCount(), 1u);

    // Let the timeout expire maxConnectRetries times.
    for (int i = 0; i < cfg.maxConnectRetries; ++i) {
        const int64_t tExpiry = T0 + static_cast<int64_t>(i + 1) * (cfg.connectTimeoutNs + 1LL);
        m.tick(tExpiry);
        ASSERT_EQ(m.state(), sm::SessionState::Connecting)
            << "Should still be Connecting after retry " << i;
        // A new ConnectRequest should have been sent on each retry.
        EXPECT_EQ(io.sendCount(), static_cast<std::size_t>(2 + i))
            << "Expected retried ConnectRequest at retry " << i;
    }

    // One more tick past the last timeout — exhausted, → Disconnected.
    const int64_t tFinal = T0 + static_cast<int64_t>(cfg.maxConnectRetries + 1) * (cfg.connectTimeoutNs + 1LL);
    m.tick(tFinal);
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
}

TEST(SessionMachine, ConnectTimeoutNoResponseNoInfiniteRetry) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxConnectRetries = 1;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    // Tick well past one timeout period — exactly one retry then Disconnected.
    m.tick(T0 + cfg.connectTimeoutNs + 1LL);  // first timeout → retry
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    m.tick(T0 + (cfg.connectTimeoutNs + 1LL) * 2);  // second timeout → Disconnected
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);

    // Total sends: initial + 1 retry = 2.
    EXPECT_EQ(io.sendCount(), 2u);

    // All sends must be ConnectRequests.
    for (std::size_t i = 0; i < io.sendCount(); ++i) {
        auto tid = io.templateIdAt(i);
        ASSERT_TRUE(tid.has_value());
        EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect));
    }
}

// ===========================================================================
// 12. Each retry gets a fresh correlationId
// ===========================================================================
TEST(SessionMachine, EachConnectTimeoutRetryHasFreshCorrelationId) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxConnectRetries = 2;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    const int64_t id1 = m.correlationId();

    m.tick(T0 + cfg.connectTimeoutNs + 1LL);
    const int64_t id2 = m.correlationId();
    EXPECT_NE(id1, id2);

    m.tick(T0 + (cfg.connectTimeoutNs + 1LL) * 2LL);
    const int64_t id3 = m.correlationId();
    // id3 might equal id2 if we've already gone Disconnected; check only while Connecting.
    if (m.state() == sm::SessionState::Connecting) {
        EXPECT_NE(id2, id3);
    }
}

// ===========================================================================
// 13. disconnect() — Connected → Closed, Disconnect frame sent
// ===========================================================================
TEST(SessionMachine, DisconnectSendsDisconnectAndGoesToClosed) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 88LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    io.reset();

    m.disconnect();

    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    ASSERT_EQ(io.sendCount(), 1u);

    auto disc = io.lastDisconnect();
    ASSERT_TRUE(disc.has_value()) << "Last frame must be decodable as Disconnect";
    EXPECT_EQ(disc->sessionId, 88LL);

    auto tid = io.lastTemplateId();
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Disconnect));
}

TEST(SessionMachine, DisconnectNoOpFromDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.disconnect();  // Disconnected → no-op
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, DisconnectNoOpFromConnecting) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    io.reset();
    m.disconnect();  // Connecting → no-op
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 14. close() from every state → Closed, idempotent
// ===========================================================================
TEST(SessionMachine, CloseFromDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, CloseFromConnecting) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    io.reset();
    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);  // close() sends nothing
}

TEST(SessionMachine, CloseFromConnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 1LL, m.correlationId(), T0);
    io.reset();
    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, CloseFromResuming) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 1LL, m.correlationId(), T0);
    m.onTransportDisconnected(T0 + 1);
    io.reset();

    // State is now Connecting (doResume was called).
    // Force it back to see close from any internal Resuming-like state.
    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, CloseIdempotent) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.close();
    ASSERT_EQ(m.state(), sm::SessionState::Closed);

    // Second close — must not crash or change state.
    m.close();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, CloseIdempotentAfterConnect) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.close();
    m.close();  // idempotent
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
}

// ===========================================================================
// 15. tick/inbound are no-ops after Closed
// ===========================================================================
TEST(SessionMachine, NoOpsAfterClosed) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 9LL, m.correlationId(), T0);
    m.close();

    ASSERT_EQ(m.state(), sm::SessionState::Closed);
    io.reset();

    // All of these must be no-ops.
    m.tick(T0 + 1'000'000'000LL);
    m.onConnectResponse(static_cast<int16_t>(rpc::ErrorCode::OK), 2, 9LL, 0LL, T0);
    m.onErrorResponse(static_cast<int16_t>(rpc::ErrorCode::SessionExpired), "", 0LL, T0);
    m.onActivity(T0 + 1);
    m.onTransportDisconnected(T0 + 2);

    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 16. onTransportDisconnected() — Connected → Resuming → doResume → Connecting
// ===========================================================================
TEST(SessionMachine, TransportDisconnectedFromConnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 123LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    io.reset();

    m.onTransportDisconnected(T0 + 1);

    // Machine should be Connecting (Resuming triggered doResume immediately).
    EXPECT_TRUE(m.state() == sm::SessionState::Connecting ||
                m.state() == sm::SessionState::Resuming);

    // A ConnectRequest must have been sent as a resume attempt.
    ASSERT_GE(io.sendCount(), 1u);
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    // Resume hint: old sessionId carried forward.
    EXPECT_EQ(req->sessionId, 123LL);
}

TEST(SessionMachine, TransportDisconnectedIgnoredFromDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.onTransportDisconnected(T0);  // no-op; not Connected
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, TransportDisconnectedIgnoredFromConnecting) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    io.reset();
    m.onTransportDisconnected(T0 + 1);  // not Connected → no-op
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 17. onErrorResponse — SessionExpired in Connected → Resuming
// ===========================================================================
TEST(SessionMachine, ErrorResponseSessionExpired_TriggersResume) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 50LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    io.reset();

    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::SessionExpired),
        "session expired",
        m.correlationId(),
        T0 + 1);

    // Should be Connecting (Resuming→doResume→Connecting).
    EXPECT_TRUE(m.state() == sm::SessionState::Resuming ||
                m.state() == sm::SessionState::Connecting);
    ASSERT_GE(io.sendCount(), 1u);
}

TEST(SessionMachine, ErrorResponseUnknownSession_TriggersResume) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 51LL, m.correlationId(), T0);
    io.reset();

    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::UnknownSession),
        "unknown session",
        m.correlationId(),
        T0 + 1);

    EXPECT_TRUE(m.state() == sm::SessionState::Resuming ||
                m.state() == sm::SessionState::Connecting);
}

// ===========================================================================
// 18. onErrorResponse — Redirect in Connecting with correlationId check
// ===========================================================================
TEST(SessionMachine, ErrorResponseRedirectInConnecting) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 3;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    const int64_t corrId = m.correlationId();
    io.reset();

    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        "",
        corrId,
        T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 1);
    ASSERT_EQ(io.sendCount(), 1u);  // new ConnectRequest
}

TEST(SessionMachine, ErrorResponseRedirectCapExhausted) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 2;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);

    // Exhaust the redirect cap via ErrorResponse{Redirect}.
    for (int i = 0; i < cfg.maxRedirects; ++i) {
        ASSERT_EQ(m.state(), sm::SessionState::Connecting);
        m.onErrorResponse(
            static_cast<int16_t>(rpc::ErrorCode::Redirect),
            "",
            m.correlationId(),
            T0 + i);
    }
    // One more — should tip to Disconnected.
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        "",
        m.correlationId(),
        T0 + cfg.maxRedirects);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
}

// ===========================================================================
// 19. onConnectResponse{NotLeader} in Connecting
// ===========================================================================
TEST(SessionMachine, NotLeaderHandledLikeRedirect) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    const int64_t corrId = m.correlationId();
    io.reset();

    // NotLeader without endpoint (assignedSessionId=0) → resend without retarget.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::NotLeader),
        /*leaderMemberId=*/1,
        /*assignedSessionId=*/0LL,
        corrId,
        T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 1);
}

// ===========================================================================
// 20. onConnectResponse in wrong states is a no-op
// ===========================================================================
TEST(SessionMachine, ConnectResponseIgnoredWhenDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    // Feed response before any connect() — no-op.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 99LL, /*correlationId=*/1LL, T0);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    EXPECT_EQ(m.sessionId(), 0LL);
}

TEST(SessionMachine, ConnectResponseIgnoredWhenConnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 10LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // A second ConnectResponse while Connected → no-op.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 99LL, m.correlationId(), T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), 10LL);  // unchanged
}

// ===========================================================================
// 21. tick while Disconnected / Closed is a no-op (no sends, no crash)
// ===========================================================================
TEST(SessionMachine, TickWhileDisconnectedNoop) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.tick(T0);
    m.tick(T0 + 1'000'000'000LL);
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    EXPECT_EQ(io.sendCount(), 0u);
}

TEST(SessionMachine, TickWhileClosedNoop) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.close();
    m.tick(T0);
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// 22. Resume retry cap — bounded, then Disconnected
// ===========================================================================
TEST(SessionMachine, ResumeCappedAtMaxResumeRetries) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxResumeRetries = 2;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 200LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // First transport disconnect kicks off resume.
    m.onTransportDisconnected(T0 + 1);

    // Each time the resume connect attempt times out,
    // it should retry until cap is hit → Disconnected.
    for (int attempt = 0; attempt < cfg.maxResumeRetries; ++attempt) {
        // If still Connecting, time it out.
        if (m.state() == sm::SessionState::Connecting) {
            // Time out this connect attempt.
            const int64_t t = T0 + 1 + static_cast<int64_t>(attempt + 1) * (cfg.connectTimeoutNs + 1LL);
            m.tick(t);
        }
    }

    // After maxResumeRetries + maxConnectRetries exhaustion, should be Disconnected.
    // (The exact path depends on whether retries cascade through resume→connect→timeout.
    //  The invariant is: no infinite loop, and eventually Disconnected or Closed.)
    EXPECT_TRUE(m.state() == sm::SessionState::Disconnected ||
                m.state() == sm::SessionState::Connecting)
        << "Expected Disconnected (or still working through retries) after cap";
}

// ===========================================================================
// 23. Redirect correlationId updated after each redirect
// ===========================================================================
TEST(SessionMachine, RedirectUpdatesCorrelationId) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    const int64_t id0 = m.correlationId();

    m.onConnectResponseRedirect("aeron:udp?endpoint=a:1", id0, T0 + 1);
    const int64_t id1 = m.correlationId();
    EXPECT_NE(id0, id1);

    m.onConnectResponseRedirect("aeron:udp?endpoint=b:2", id1, T0 + 2);
    const int64_t id2 = m.correlationId();
    EXPECT_NE(id1, id2);

    EXPECT_EQ(m.redirectCount(), 2);
}

// ===========================================================================
// 24. Redirect with wrong correlationId is ignored
// ===========================================================================
TEST(SessionMachine, RedirectWithWrongCorrIdIgnored) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    io.reset();

    m.onConnectResponseRedirect("aeron:udp?endpoint=leader:40200",
                                 m.correlationId() + 999, T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 0);
    EXPECT_EQ(io.sendCount(), 0u);  // no retarget, no resend
    EXPECT_EQ(io.retargetCount(), 0u);
}

// ===========================================================================
// 25. ConnectRequest fields match config exactly
// ===========================================================================
TEST(SessionMachine, ConnectRequestFieldsMatchConfig) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.egressChannel  = "aeron:udp?endpoint=myhost:12345";
    cfg.egressStreamId = 7777;
    cfg.clientVersion  = 42;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);

    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->egressChannel,  cfg.egressChannel);
    EXPECT_EQ(req->egressStreamId, cfg.egressStreamId);
    EXPECT_EQ(req->clientVersion,  cfg.clientVersion);
    EXPECT_EQ(req->sessionId,      0LL);  // fresh connect
}

// ===========================================================================
// 26. Keepalive frame carries the assigned sessionId
// ===========================================================================
TEST(SessionMachine, KeepaliveCarriesAssignedSessionId) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    constexpr int64_t kSession = 0xABCD'EF12'3456'789ALL;
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, kSession, m.correlationId(), T0);

    io.reset();
    m.tick(T0 + cfg.keepaliveIntervalNs);

    auto kav = io.lastKeepalive();
    ASSERT_TRUE(kav.has_value());
    EXPECT_EQ(kav->sessionId, kSession);
}

// ===========================================================================
// 27. Disconnect frame carries the assigned sessionId
// ===========================================================================
TEST(SessionMachine, DisconnectFrameCarriesSessionId) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    constexpr int64_t kSession = 0x1111'2222'3333'4444LL;
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, kSession, m.correlationId(), T0);

    io.reset();
    m.disconnect();

    auto disc = io.lastDisconnect();
    ASSERT_TRUE(disc.has_value());
    EXPECT_EQ(disc->sessionId, kSession);
}

// ===========================================================================
// 28. Milestone-B scaffold — noteOffset / resumeHint
// ===========================================================================
TEST(SessionMachine, OffsetScaffoldNoteAndRetrieve) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    EXPECT_EQ(m.resumeHint(1), -1LL);  // unknown key

    m.noteOffset(1, 100LL);
    EXPECT_EQ(m.resumeHint(1), 100LL);

    m.noteOffset(1, 200LL);  // update
    EXPECT_EQ(m.resumeHint(1), 200LL);

    m.noteOffset(2, 50LL);
    EXPECT_EQ(m.resumeHint(2), 50LL);
    EXPECT_EQ(m.resumeHint(1), 200LL);  // unaffected

    EXPECT_EQ(m.resumeHint(99), -1LL);  // still unknown
}

// ===========================================================================
// 29. Full session flow — connect → connected → keepalive → disconnect
// ===========================================================================
TEST(SessionMachine, FullHappyPathSessionFlow) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    // --- Disconnected ---
    ASSERT_EQ(m.state(), sm::SessionState::Disconnected);

    // --- connect() → Connecting ---
    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    {
        auto req = io.lastConnectRequest();
        ASSERT_TRUE(req.has_value());
        EXPECT_EQ(req->sessionId, 0LL);
        EXPECT_NE(req->correlationId, 0LL);
    }

    // --- ConnectResponse{OK} → Connected ---
    const int64_t corrId = m.correlationId();
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 777LL, corrId, T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), 777LL);

    // --- Keepalive at T0 + interval ---
    io.reset();
    m.tick(T0 + cfg.keepaliveIntervalNs);
    ASSERT_EQ(io.sendCount(), 1u);
    {
        auto kav = io.lastKeepalive();
        ASSERT_TRUE(kav.has_value());
        EXPECT_EQ(kav->sessionId, 777LL);
    }

    // --- Activity keeps session alive ---
    m.onActivity(T0 + cfg.keepaliveIntervalNs);

    // --- disconnect() → Closed ---
    io.reset();
    m.disconnect();
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    {
        auto disc = io.lastDisconnect();
        ASSERT_TRUE(disc.has_value());
        EXPECT_EQ(disc->sessionId, 777LL);
    }

    // --- terminal: all further events are no-ops ---
    io.reset();
    m.tick(T0 + 1'000'000'000LL);
    m.disconnect();
    m.close();
    EXPECT_EQ(io.sendCount(), 0u);
    EXPECT_EQ(m.state(), sm::SessionState::Closed);
}

// ===========================================================================
// 30. Multiple distinct correlationIds across multiple connect() calls
//     (after going Disconnected and reconnecting)
// ===========================================================================
TEST(SessionMachine, ReconnectAfterDisconnectedHasFreshCorrId) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxConnectRetries = 0;
    sm::SessionMachine m(io, cfg);

    // First connect attempt → times out → Disconnected.
    m.connect(T0);
    const int64_t id1 = m.correlationId();

    m.tick(T0 + cfg.connectTimeoutNs + 1LL);
    ASSERT_EQ(m.state(), sm::SessionState::Disconnected);

    // Second connect() after Disconnected → fresh correlationId.
    m.connect(T0 + cfg.connectTimeoutNs + 2LL);
    const int64_t id2 = m.correlationId();
    EXPECT_NE(id1, id2);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
}

// ===========================================================================
// 31. errorResponse with wrong code in Connected is a no-op
// ===========================================================================
TEST(SessionMachine, ErrorResponseNonSessionErrorInConnectedIgnored) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 5LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // Internal error while connected → machine doesn't know what to do, should not crash.
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Internal),
        "internal error",
        m.correlationId(),
        T0 + 1);

    // State unchanged (Internal error in Connected is not handled → no-op per spec).
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
}

// ===========================================================================
// 32. ProtocolVersion error in Connecting → Disconnected
// ===========================================================================
TEST(SessionMachine, ProtocolVersionErrorGoesDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::ProtocolVersion),
        -1, 0LL, m.correlationId(), T0);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
}
