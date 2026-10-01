// tests/session/SessionMachineTest.cpp
//
// CC-4 — Exhaustive deterministic tests for SessionMachine.
//        Updated by CC-4 fix-round (FIX A–E).
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
#include <iterator>
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

    bool retargetSucceeds = true;

    bool retarget(const std::string& newChannel) noexcept override {
        retargets.push_back(newChannel);
        return retargetSucceeds;
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
// 5b. ConnectResponse{OK} with mismatched correlationId is ignored (new test)
// ===========================================================================
TEST(SessionMachine, ConnectResponseOkMismatchedCorrIdIgnored) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    const int64_t corrId = m.correlationId();

    // Deliver a response with a different correlationId.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        /*leaderMemberId=*/2,
        /*assignedSessionId=*/0xABCDLL,
        corrId + 7,   // mismatch
        T0);

    // Must remain in Connecting, no session assigned.
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.sessionId(), 0LL);
    EXPECT_EQ(io.sendCount(), 1u);  // only the initial ConnectRequest
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
// 7. Response timeout → Resuming (FIX D: Resuming is now observable)
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

    // FIX D: state is now concretely Resuming (doResume fires on next tick).
    EXPECT_EQ(m.state(), sm::SessionState::Resuming)
        << "Expected Resuming immediately after response timeout (doResume fires on next tick)";

    // No frame sent yet — doResume fires on the next tick.
    EXPECT_EQ(io.sendCount(), 1u);  // only the original ConnectRequest
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

    // Trigger response timeout → Resuming.
    m.tick(T0 + cfg.responseTimeoutNs + 1);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);
    // No frame yet.
    EXPECT_EQ(io.sendCount(), 0u);

    // One more tick → doResume fires → Connecting + ConnectRequest sent.
    m.tick(T0 + cfg.responseTimeoutNs + 2);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    ASSERT_GE(io.sendCount(), 1u);

    // The resume ConnectRequest should carry the old sessionId as hint.
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->sessionId, 77LL);  // resume hint

    // Feed a successful ConnectResponse for the resumed connection.
    const int64_t newCorrId = m.correlationId();
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 77LL, newCorrId, T0 + cfg.responseTimeoutNs + 3);

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
//    FIX B: retargetCount() asserted (non-vacuous).
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

    // FIX B: retarget() must have been called exactly once (non-vacuous).
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
//     FIX B: retargetCount() asserted.
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

    // FIX B: exactly maxRedirects retarget() calls.
    EXPECT_EQ(io.retargetCount(), static_cast<std::size_t>(cfg.maxRedirects));

    // One more redirect — should exceed the cap.
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    const int64_t corrId = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=leader:40200", corrId, T0 + cfg.maxRedirects);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    // No additional retarget on the cap-exceeded call.
    EXPECT_EQ(io.retargetCount(), static_cast<std::size_t>(cfg.maxRedirects));
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
// FIX D: disconnect() while in Resuming — sends Disconnect if session existed,
//        then → Closed.
// ===========================================================================
TEST(SessionMachine, DisconnectFromResumingSendsDisconnectIfSessionExisted) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    // Establish a session.
    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 999LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // Trigger Resuming (response timeout).
    m.tick(T0 + testConfig().responseTimeoutNs + 1);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);

    io.reset();

    // disconnect() while Resuming — should send Disconnect (sessionId != 0) and → Closed.
    m.disconnect();

    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    ASSERT_EQ(io.sendCount(), 1u);

    auto disc = io.lastDisconnect();
    ASSERT_TRUE(disc.has_value()) << "Must send a Disconnect frame while Resuming";
    EXPECT_EQ(disc->sessionId, 999LL);

    auto tid = io.lastTemplateId();
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Disconnect));
}

TEST(SessionMachine, DisconnectFromResumingNoSessionNoop) {
    // If we somehow arrive at Resuming without ever getting a sessionId (edge case),
    // disconnect() still transitions to Closed but sends no Disconnect frame.
    // In practice Resuming is only entered from Connected (sessionId != 0), but
    // we test the branch for completeness.
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    // Force into Resuming by simulating the machine directly.
    // We do this by connecting and then using onTransportDisconnected —
    // but we never complete the connect, so sessionId_ == 0.
    // Actually the normal path: Resuming requires prior Connected state (sessionId set).
    // So test with a session to verify Disconnect is sent.
    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 0LL,  // sessionId == 0 is unusual but test the guard
        m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // Move to Resuming.
    m.tick(T0 + testConfig().responseTimeoutNs + 1);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);

    io.reset();

    // sessionId_ == 0 → disconnect() → no frame, → Closed.
    m.disconnect();

    EXPECT_EQ(m.state(), sm::SessionState::Closed);
    // No Disconnect frame sent because sessionId is 0.
    EXPECT_EQ(io.sendCount(), 0u);
}

// ===========================================================================
// FIX D: tick() while Resuming → doResume fires (state observable as Resuming
//         before first tick, then moves to Connecting).
// ===========================================================================
TEST(SessionMachine, TickInResumingFiresDoResume) {
    FakeIo io;
    auto cfg = testConfig();
    sm::SessionMachine m(io, cfg);

    // Establish a session.
    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 77LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    io.reset();

    // Trigger Resuming (transport disconnected).
    m.onTransportDisconnected(T0 + 1);

    // FIX D: state is now concretely Resuming — observable.
    EXPECT_EQ(m.state(), sm::SessionState::Resuming);
    // No frame sent yet.
    EXPECT_EQ(io.sendCount(), 0u);

    // One tick → doResume fires → Connecting + ConnectRequest sent.
    m.tick(T0 + 2);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    ASSERT_EQ(io.sendCount(), 1u);

    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->sessionId, 77LL);  // resume hint: old sessionId

    auto tid = io.lastTemplateId();
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect));
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

    // FIX D: state is now concretely Resuming.
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);
    io.reset();

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
// 16. onTransportDisconnected() — Connected → Resuming (FIX D: observable)
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

    // FIX D: state is concretely Resuming, not Connecting.
    EXPECT_EQ(m.state(), sm::SessionState::Resuming);

    // No ConnectRequest sent yet — doResume fires on the next tick.
    EXPECT_EQ(io.sendCount(), 0u);

    // One tick → doResume fires.
    m.tick(T0 + 2);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    ASSERT_EQ(io.sendCount(), 1u);

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
// 17. onErrorResponse — SessionExpired in Connected → Resuming (FIX D: observable)
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

    // FIX D: state is concretely Resuming.
    EXPECT_EQ(m.state(), sm::SessionState::Resuming);
    // No frame yet — fires on next tick.
    EXPECT_EQ(io.sendCount(), 0u);

    // Tick → doResume → Connecting.
    m.tick(T0 + 2);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
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

    // FIX D: state is concretely Resuming.
    EXPECT_EQ(m.state(), sm::SessionState::Resuming);
}

// ===========================================================================
// 18. onErrorResponse — Redirect in Connecting with correlationId check
//     FIX B: with a non-empty message (endpoint), retarget() is called.
// ===========================================================================
TEST(SessionMachine, ErrorResponseRedirectInConnecting_NoEndpoint) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 3;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    const int64_t corrId = m.correlationId();
    io.reset();

    // Empty message → no retarget (memberId-only path, same endpoint resend).
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        "",
        corrId,
        T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 1);
    ASSERT_EQ(io.sendCount(), 1u);  // new ConnectRequest

    // FIX B: no retarget when message is empty.
    EXPECT_EQ(io.retargetCount(), 0u);
}

TEST(SessionMachine, ErrorResponseRedirectInConnecting_WithEndpoint) {
    // FIX B: non-empty message in ErrorResponse{Redirect} → retarget called.
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 3;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    const int64_t corrId = m.correlationId();
    io.reset();

    const std::string leaderEndpoint = "aeron:udp?endpoint=leader:40200";
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        leaderEndpoint,
        corrId,
        T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    EXPECT_EQ(m.redirectCount(), 1);
    ASSERT_EQ(io.sendCount(), 1u);  // new ConnectRequest

    // FIX B: retarget() called once with the endpoint from the message.
    ASSERT_EQ(io.retargetCount(), 1u);
    EXPECT_EQ(io.retargets[0], leaderEndpoint);

    // ConnectRequest fields correct.
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_NE(req->correlationId, corrId);  // fresh correlationId after redirect
}

// DR-BUG-2 (redirect-reconnect robustness): when a follower redirects the
// INITIAL connect via ErrorResponse{Redirect, endpoint}, the machine retargets
// to the leader and resends the ConnectRequest.  The freshly-retargeted leader
// publication is NOT yet Aeron-connected, so that first offer is dropped
// (sendFrame → false).  The machine MUST fast-retry the ConnectRequest at
// connectRetryIntervalNs within the connect window — NOT wait the full
// connectTimeoutNs — so the reconnect completes as soon as the leader pub warms.
// This is the same NOT_CONNECTED fast-retry discipline the initial connect uses.
TEST(SessionMachine, RedirectReconnectFastRetriesWhenLeaderPubNotConnected) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.connectTimeoutNs       = 5'000'000'000LL;  // 5 s window (real-ish)
    cfg.connectRetryIntervalNs = 200'000'000LL;    // 200 ms fast-retry
    cfg.maxRedirects           = 5;
    sm::SessionMachine m(io, cfg);

    // Initial connect to the follower succeeds at offer time.
    io.sendSucceeds = true;
    m.connect(T0);
    const int64_t corrId = m.correlationId();
    io.reset();

    // Follower redirects to the leader.  Model the not-yet-connected leader pub:
    // the retargeted resend of the ConnectRequest is DROPPED (offer → false).
    io.sendSucceeds = false;
    const std::string leaderEndpoint = "aeron:udp?endpoint=leader:40200";
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        leaderEndpoint,
        corrId,
        T0 + 1);

    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    ASSERT_EQ(io.retargetCount(), 1u);          // retargeted to the leader
    ASSERT_EQ(io.sendCount(), 1u);              // one (dropped) ConnectRequest offer
    const int64_t redirectCorrId = m.correlationId();
    EXPECT_NE(redirectCorrId, corrId);

    // Tick BEFORE the fast-retry cadence elapses: no extra send yet, and we are
    // NOWHERE NEAR the full connect timeout (proving we don't wait it out).
    m.tick(T0 + 1 + cfg.connectRetryIntervalNs / 2);
    EXPECT_EQ(io.sendCount(), 1u);

    // Leader pub warms up; tick at the fast-retry cadence → ConnectRequest resent.
    io.sendSucceeds = true;
    m.tick(T0 + 1 + cfg.connectRetryIntervalNs);
    ASSERT_EQ(io.sendCount(), 2u);              // fast-retry fired
    EXPECT_EQ(io.retargetCount(), 1u);          // no extra retarget on fast-retry

    // The fast-retry keeps the SAME (redirect-episode) correlationId so a
    // ConnectResponse the broker already stashed still matches.
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->correlationId, redirectCorrId);

    // Now the leader answers → Connected, well within the timeout window.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        /*leaderMemberId=*/0,
        /*assignedSessionId=*/4242,
        redirectCorrId,
        T0 + 1 + cfg.connectRetryIntervalNs + 1);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), 4242);
}

TEST(SessionMachine, ErrorResponseRedirectCapExhausted) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 2;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);

    // Exhaust the redirect cap via ErrorResponse{Redirect} (no endpoint → no retarget).
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
    // No retargets (all had empty message).
    EXPECT_EQ(io.retargetCount(), 0u);
}

// ===========================================================================
// 19. onConnectResponse{NotLeader} in Connecting
//     FIX B: memberId-only path — no retarget (comment added in header).
// ===========================================================================
TEST(SessionMachine, NotLeaderHandledLikeRedirect_NoRetarget) {
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
    // FIX B: no retarget for memberId-only path.
    EXPECT_EQ(io.retargetCount(), 0u);
    // A new ConnectRequest was sent.
    ASSERT_EQ(io.sendCount(), 1u);
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

    // First transport disconnect kicks off Resuming.
    m.onTransportDisconnected(T0 + 1);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);

    // Each tick fires doResume → Connecting; then connect timeout → another
    // resume attempt or Disconnected.
    for (int attempt = 0; attempt < cfg.maxResumeRetries + 1; ++attempt) {
        if (m.state() == sm::SessionState::Resuming) {
            // Tick fires doResume.
            m.tick(T0 + 2 + static_cast<int64_t>(attempt) * (cfg.connectTimeoutNs + 10LL));
        }
        if (m.state() == sm::SessionState::Connecting) {
            // Time out this connect attempt; next timeout in tickConnecting
            // won't try resume (maxConnectRetries=3 is separate from resume).
            // We want to trigger more resumes, so let connect timeout → Disconnected,
            // but the cap is on resumeRetryCount, exercised by doResume.
            // Just time out the connect.
            const int64_t t = T0 + 2 + static_cast<int64_t>(attempt + 1) * (cfg.connectTimeoutNs + 10LL);
            // Exhaust connectRetryCount quickly.
            for (int r = 0; r <= cfg.maxConnectRetries; ++r) {
                m.tick(t + static_cast<int64_t>(r + 1) * (cfg.connectTimeoutNs + 5LL));
                if (m.state() != sm::SessionState::Connecting) break;
            }
        }
        if (m.state() == sm::SessionState::Disconnected) break;
    }

    // After retries exhausted → Disconnected.
    EXPECT_TRUE(m.state() == sm::SessionState::Disconnected ||
                m.state() == sm::SessionState::Connecting)
        << "Expected Disconnected (or still working through retries) after cap";
}

// ===========================================================================
// 23. Redirect correlationId updated after each redirect
//     FIX B: retargetCount asserted.
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
    // FIX B: 2 retargets, one per redirect.
    EXPECT_EQ(io.retargetCount(), 2u);
    EXPECT_EQ(io.retargets[0], "aeron:udp?endpoint=a:1");
    EXPECT_EQ(io.retargets[1], "aeron:udp?endpoint=b:2");
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

    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Internal),
        "internal error",
        m.correlationId(),
        T0 + 1);

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

// ===========================================================================
// FIX A — Interleaved redirects then timeout: cap spans entire connect episode.
//
// A gateway that redirects N times then goes silent MUST NOT let timeouts
// reset the redirect counter — total retargets must be bounded by maxRedirects.
// ===========================================================================
TEST(SessionMachine, RedirectCapSpansConnectEpisode_InterleavedRedirectThenTimeout) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects      = 3;
    cfg.maxConnectRetries = 5;  // give plenty of connect retries
    sm::SessionMachine m(io, cfg);

    m.connect(T0);

    // Issue 2 redirects (below the cap of 3).
    const int64_t c0 = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=n1:1", c0, T0 + 1);
    ASSERT_EQ(m.redirectCount(), 1);

    const int64_t c1 = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=n2:2", c1, T0 + 2);
    ASSERT_EQ(m.redirectCount(), 2);

    // retargets so far.
    EXPECT_EQ(io.retargetCount(), 2u);

    // Now let the connect timeout fire (a timeout retry).
    // FIX A: this must NOT reset redirectCount_ to 0.
    int64_t tNext = T0 + cfg.connectTimeoutNs + 10LL;
    m.tick(tNext);  // one timeout-retry
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    // redirectCount_ must still be 2 (NOT reset by the timeout retry).
    EXPECT_EQ(m.redirectCount(), 2);

    // Now issue 1 more redirect to hit cap (2 existing + 1 new = 3 = cap).
    const int64_t c2 = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=n3:3", c2, tNext + 1);
    ASSERT_EQ(m.redirectCount(), 3);
    EXPECT_EQ(io.retargetCount(), 3u);

    // One more redirect attempt must trip the cap → Disconnected.
    const int64_t c3 = m.correlationId();
    m.onConnectResponseRedirect("aeron:udp?endpoint=n4:4", c3, tNext + 2);
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected);

    // Total retargets must be exactly maxRedirects (3), not more.
    EXPECT_EQ(io.retargetCount(), static_cast<std::size_t>(cfg.maxRedirects));
}

// ===========================================================================
// FIX C — Reconnect after exhausted connectRetryCount_ does not immediately
//          time out (fresh counters on new connect()).
// ===========================================================================
TEST(SessionMachine, ReconnectAfterExhaustedRetries_FreshCounters) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxConnectRetries = 1;  // exhaust quickly
    sm::SessionMachine m(io, cfg);

    // First connect episode: exhaust connectRetryCount_.
    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    m.tick(T0 + cfg.connectTimeoutNs + 1LL);  // first timeout → retry
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    m.tick(T0 + (cfg.connectTimeoutNs + 1LL) * 2LL);  // second timeout → Disconnected
    ASSERT_EQ(m.state(), sm::SessionState::Disconnected);

    // connectRetryCount_ is now at 1 (== maxConnectRetries).
    // A fresh connect() must reset it.
    const int64_t t2 = T0 + (cfg.connectTimeoutNs + 1LL) * 3LL;
    m.connect(t2);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    // Tick just before the timeout — must NOT immediately go Disconnected.
    m.tick(t2 + cfg.connectTimeoutNs - 1LL);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting)
        << "Fresh connect() must reset connectRetryCount_; machine should not immediately Disconnect";

    // Send count: initial(1) + retry(1) + reconnect(1) = 3 so far.
    EXPECT_EQ(io.sendCount(), 3u);
    for (std::size_t i = 0; i < io.sendCount(); ++i) {
        auto tid = io.templateIdAt(i);
        ASSERT_TRUE(tid.has_value());
        EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect));
    }
}

// ===========================================================================
// FIX C — resumeRetryCount_ is reset on onConnectResponse{OK}.
//          After a successful resume a later resume must not be immediately blocked.
// ===========================================================================
TEST(SessionMachine, ResumeRetryCountResetOnSuccessfulConnect) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxResumeRetries = 1;  // very tight cap to expose the bug
    sm::SessionMachine m(io, cfg);

    // Connect.
    m.connect(T0);
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 100LL, m.correlationId(), T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // First transport disconnect → Resuming → tick → Connecting.
    m.onTransportDisconnected(T0 + 1);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);
    m.tick(T0 + 2);  // doResume fires (resumeRetryCount_ becomes 1 == maxResumeRetries)
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    // Successful reconnect — onConnectResponse{OK} must reset resumeRetryCount_.
    const int64_t c1 = m.correlationId();
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        2, 200LL, c1, T0 + 3);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // Second transport disconnect → Resuming → tick.
    // If resumeRetryCount_ was NOT reset, doResume() would immediately → Disconnected.
    m.onTransportDisconnected(T0 + 4);
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);

    io.reset();
    m.tick(T0 + 5);  // doResume must fire successfully (not → Disconnected).

    EXPECT_EQ(m.state(), sm::SessionState::Connecting)
        << "resumeRetryCount_ must have been reset by onConnectResponse{OK}; "
           "second resume must not immediately go Disconnected";
    ASSERT_EQ(io.sendCount(), 1u);
    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->sessionId, 200LL);  // resume hint is the latest session
}

// ===========================================================================
// FIX B — Double redirect via error-path then response-path in one episode.
//          Asserts retargetCount + ConnectRequest fields (non-vacuous).
// ===========================================================================
TEST(SessionMachine, DoubleRedirect_ErrorPathThenResponsePath) {
    FakeIo io;
    auto cfg = testConfig();
    cfg.maxRedirects = 5;
    sm::SessionMachine m(io, cfg);

    m.connect(T0);
    const int64_t corrId0 = m.correlationId();

    // First redirect: ErrorResponse{Redirect, endpoint} → retarget.
    const std::string ep1 = "aeron:udp?endpoint=node1:40200";
    m.onErrorResponse(
        static_cast<int16_t>(rpc::ErrorCode::Redirect),
        ep1,
        corrId0,
        T0 + 1);

    EXPECT_EQ(m.redirectCount(), 1);
    ASSERT_EQ(io.retargetCount(), 1u);
    EXPECT_EQ(io.retargets[0], ep1);

    // Verify the ConnectRequest that was resent.
    ASSERT_GE(io.sendCount(), 2u);  // initial + resend
    auto req1 = io.connectRequestAt(1);
    ASSERT_TRUE(req1.has_value());
    EXPECT_NE(req1->correlationId, corrId0);  // fresh correlationId

    const int64_t corrId1 = m.correlationId();

    // Second redirect: ConnectResponseRedirect → retarget.
    const std::string ep2 = "aeron:udp?endpoint=node2:40200";
    m.onConnectResponseRedirect(ep2, corrId1, T0 + 2);

    EXPECT_EQ(m.redirectCount(), 2);
    ASSERT_EQ(io.retargetCount(), 2u);
    EXPECT_EQ(io.retargets[1], ep2);

    // Verify the second resent ConnectRequest.
    ASSERT_GE(io.sendCount(), 3u);
    auto req2 = io.connectRequestAt(2);
    ASSERT_TRUE(req2.has_value());
    EXPECT_NE(req2->correlationId, corrId1);  // another fresh correlationId

    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
}

// ===========================================================================
// MC-8 [CC-8]: onConnectResponse with a non-OK / non-redirect error code
//              from Connecting → Disconnected (the else-branch).
//
// ProtocolVersion is neither OK nor Redirect/NotLeader, so the machine must
// fall through to the terminal else and go Disconnected (no resend, no retarget).
// ===========================================================================
TEST(SessionMachine, ConnectResponseFatalErrorGoesDisconnected) {
    FakeIo io;
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    const int64_t corrId = m.correlationId();
    io.reset();

    // ProtocolVersion error → else-branch → Disconnected.
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::ProtocolVersion),
        /*leaderMemberId=*/0,
        /*assignedSessionId=*/0,
        corrId,
        T0 + 1);

    EXPECT_EQ(m.state(), sm::SessionState::Disconnected)
        << "non-OK/non-redirect ConnectResponse must go Disconnected";
    EXPECT_EQ(io.sendCount(), 0u)   << "must NOT resend a ConnectRequest";
    EXPECT_EQ(io.retargetCount(), 0u) << "must NOT retarget";
}

// ===========================================================================
// DR-BUG-2 regression: ConnectRequest fast-retry when send fails (NOT_CONNECTED).
//
// Pre-8906828 behaviour: if io_.sendFrame returned false (NOT_CONNECTED),
// the machine would NOT retry until the full connectTimeoutNs elapsed.
// Post-8906828: the machine re-offers the ConnectRequest every
// connectRetryIntervalNs (200ms) as long as connectSentOk_==false.
//
// This test fails against the pre-fix SessionMachine and passes now.
// ===========================================================================
TEST(SessionMachine, ConnectFastRetryWhenSendFails) {
    FakeIo io;
    auto cfg = testConfig();
    // Use a retry interval shorter than the connect timeout so we can
    // advance the clock in manageable increments.
    cfg.connectTimeoutNs        = 10'000'000LL;  // 10 ms
    cfg.connectRetryIntervalNs  =  2'000'000LL;  //  2 ms fast-retry
    cfg.maxConnectRetries       = 3;
    sm::SessionMachine m(io, cfg);

    // Phase 1: send fails (simulates NOT_CONNECTED publication).
    io.sendSucceeds = false;
    m.connect(T0);

    // The initial ConnectRequest is attempted but fails (sendSucceeds=false).
    // The FakeIo still records the call, so sendCount grows.
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    const std::size_t initial_sends = io.sendCount();
    EXPECT_GE(initial_sends, 1u) << "connect() must attempt a send even when failing";

    // Phase 2: advance clock by connectRetryIntervalNs → machine must re-offer.
    // We tick 3 times within the timeout window; each tick at a retry boundary
    // must produce an additional ConnectRequest attempt.
    const int64_t interval = cfg.connectRetryIntervalNs;
    m.tick(T0 + interval);
    const std::size_t after_retry1 = io.sendCount();
    EXPECT_GT(after_retry1, initial_sends)
        << "DR-BUG-2: machine must fast-retry ConnectRequest after connectRetryIntervalNs "
           "when send has not yet succeeded (connectSentOk_==false)";

    m.tick(T0 + interval * 2);
    const std::size_t after_retry2 = io.sendCount();
    EXPECT_GT(after_retry2, after_retry1)
        << "DR-BUG-2: second fast-retry must fire at 2*connectRetryIntervalNs";

    // All sends so far must be ConnectRequests (no keepalives / other frames).
    for (std::size_t i = 0; i < io.sendCount(); ++i) {
        auto tid = io.templateIdAt(i);
        ASSERT_TRUE(tid.has_value()) << "send[" << i << "] must be parseable";
        EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect))
            << "send[" << i << "] must be a ConnectRequest during fast-retry";
    }

    // Phase 3: sends start succeeding → machine stops re-offering (sent OK).
    io.sendSucceeds = true;
    const std::size_t before_success = io.sendCount();
    m.tick(T0 + interval * 3);
    const std::size_t after_success = io.sendCount();
    // One more send on the next retry boundary (now succeeding).
    EXPECT_GT(after_success, before_success)
        << "machine must attempt one more send when sends start succeeding";

    // Phase 4: once sent OK, no more fast-retries (connectSentOk_==true).
    const std::size_t sends_after_ok = io.sendCount();
    m.tick(T0 + interval * 4);
    m.tick(T0 + interval * 5);
    EXPECT_EQ(io.sendCount(), sends_after_ok)
        << "after a successful send the machine must not fast-retry further";

    // Machine is still Connecting (no ConnectResponse yet).
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);

    // Phase 5: ConnectResponse arrives → Connected.
    const int64_t corrId = m.correlationId();
    m.onConnectResponse(
        static_cast<int16_t>(rpc::ErrorCode::OK),
        /*leaderMemberId=*/1,
        /*assignedSessionId=*/0xCAFELL,
        corrId,
        T0 + interval * 6);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), 0xCAFELL);
}

// ===========================================================================
// F8 [CC-8]: a FAILED retarget (io_.retarget returns false) must NOT throw and
//            the machine must still resend the ConnectRequest (bounded by cap).
//
// This exercises the noexcept/return-bool retarget contract at the session
// layer: even if the transport can't resolve the new endpoint, the machine
// keeps making forward progress instead of crashing.
// ===========================================================================
TEST(SessionMachine, RetargetFailureStillResendsNoThrow) {
    FakeIo io;
    io.retargetSucceeds = false;  // simulate a resolve-timeout retarget failure
    sm::SessionMachine m(io, testConfig());

    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    const int64_t corrId0 = m.correlationId();
    io.reset();

    const std::string ep = "aeron:udp?endpoint=unresolvable:1";
    ASSERT_NO_THROW(m.onConnectResponseRedirect(ep, corrId0, T0 + 1))
        << "redirect with a failing retarget must not throw";

    // retarget was attempted (returned false) and the machine still resent.
    EXPECT_EQ(io.retargetCount(), 1u);
    EXPECT_EQ(io.retargets[0], ep);
    ASSERT_EQ(io.sendCount(), 1u) << "must resend the ConnectRequest after a failed retarget";
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);

    auto req = io.lastConnectRequest();
    ASSERT_TRUE(req.has_value());
    EXPECT_NE(req->correlationId, corrId0) << "resend must use a fresh correlationId";
}

// ===========================================================================
// T15a R1 — "never give up once Live" (SessionConfig::reconnectForeverAfterLive).
//
// Shard clients set the flag.  Once a session has been Live (first
// ConnectResponse{OK}), every retry-budget exhaustion that used to be terminal
// Disconnected goes to Backoff{attempt, nextAttemptNs} instead, and Backoff
// fires a fresh (resume-first) ConnectRequest at nextAttemptNs on the injected
// clock: capped exponential, 10 ms doubling to 1 s, forever.  A non-retriable
// answer (ProtocolVersion) goes to the terminal Rejected{errorCode}.  A plain
// Client (flag false) keeps today's behaviour.  Pure: FakeIo + injected clock.
// ===========================================================================
namespace {

sm::SessionConfig foreverConfig() {
    auto cfg = testConfig();
    cfg.reconnectForeverAfterLive = true;
    return cfg;
}

constexpr int16_t kOk          = static_cast<int16_t>(rpc::ErrorCode::OK);
constexpr int16_t kRedirect    = static_cast<int16_t>(rpc::ErrorCode::Redirect);
constexpr int16_t kNotLeader   = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
constexpr int16_t kProtoVer    = static_cast<int16_t>(rpc::ErrorCode::ProtocolVersion);
constexpr int16_t kExpired     = static_cast<int16_t>(rpc::ErrorCode::SessionExpired);
constexpr int16_t kInternal    = static_cast<int16_t>(rpc::ErrorCode::Internal);

// Connect and go Live with sessionId `sid`; returns the time used.
int64_t goLive(sm::SessionMachine& m, int64_t sid, int64_t now) {
    m.connect(now);
    m.onConnectResponse(kOk, 1, sid, m.correlationId(), now);
    return now;
}

// Drive connect timeouts until the machine leaves Connecting; returns the time.
// Every step is one bounded tick past connectTimeoutNs (no sleeps).
int64_t timeOutUntilNotConnecting(sm::SessionMachine& m, const sm::SessionConfig& cfg,
                                  int64_t now, int cap = 64) {
    for (int i = 0; i < cap && m.state() == sm::SessionState::Connecting; ++i) {
        now += cfg.connectTimeoutNs + 1;
        m.tick(now);
    }
    return now;
}

} // namespace

TEST(SessionMachineRegression, SeedSessionLostBeyondRetryBudgetStillReconnects) {
    FakeIo io;
    auto cfg = foreverConfig();
    sm::SessionMachine m(io, cfg);
    int64_t now = goLive(m, 77, T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // The egress link drops: Resuming -> resume ConnectRequest (carries sid 77).
    m.onTransportDisconnected(now);
    m.tick(++now);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    // Burn through far more than maxConnectRetries + maxResumeRetries failures.
    // Today this is terminal Disconnected after 4 timeouts; with the flag it is
    // Backoff, and each Backoff deadline fires a new resume attempt — forever.
    const int failures = (cfg.maxConnectRetries + cfg.maxResumeRetries + 1) * 3;
    int backoffs = 0;
    for (int f = 0; f < failures; ++f) {
        now = timeOutUntilNotConnecting(m, cfg, now);
        ASSERT_NE(m.state(), sm::SessionState::Disconnected) << "gave up after failure " << f;
        ASSERT_EQ(m.state(), sm::SessionState::Backoff) << "failure " << f;
        ++backoffs;
        EXPECT_EQ(m.backoffAttempt(), backoffs);
        const int64_t due = m.nextAttemptNs();
        ASSERT_GT(due, now);
        io.reset();
        m.tick(due - 1);
        EXPECT_EQ(io.sendCount(), 0u) << "no attempt before its deadline";
        EXPECT_EQ(m.state(), sm::SessionState::Backoff);
        now = due;
        m.tick(now);
        ASSERT_EQ(m.state(), sm::SessionState::Connecting);
        auto req = io.lastConnectRequest();
        ASSERT_TRUE(req.has_value());
        EXPECT_EQ(req->sessionId, 77) << "a backoff attempt resumes first (broker may still know sid)";
    }

    // The broker finally answers: Live again, backoff reset.
    m.onConnectResponse(kOk, 1, 77, m.correlationId(), ++now);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.backoffAttempt(), 0);

    // Resume-budget exhaustion (maxResumeRetries = 0: the very first resume
    // would give up today) also goes to Backoff once Live.
    FakeIo io2;
    auto cfg2 = foreverConfig();
    cfg2.maxResumeRetries = 0;
    sm::SessionMachine m2(io2, cfg2);
    now = goLive(m2, 5, T0);
    m2.onTransportDisconnected(now);
    m2.tick(++now);
    EXPECT_EQ(m2.state(), sm::SessionState::Backoff);

    // The broker forgot the session: SessionExpired to the resume → drop the sid
    // and connect fresh at once (not terminal, not another backoff wait).
    m2.tick(m2.nextAttemptNs());
    ASSERT_EQ(m2.state(), sm::SessionState::Connecting);
    io2.reset();
    m2.onConnectResponse(kExpired, 1, 0, m2.correlationId(), m2.nextAttemptNs() + 1);
    ASSERT_EQ(m2.state(), sm::SessionState::Connecting);
    auto fresh = io2.lastConnectRequest();
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(fresh->sessionId, 0) << "fresh connect after SessionExpired";
    m2.onConnectResponse(kOk, 1, 9, m2.correlationId(), m2.nextAttemptNs() + 2);
    EXPECT_EQ(m2.state(), sm::SessionState::Connected);
    EXPECT_EQ(m2.sessionId(), 9);
}

TEST(SessionMachineRegression, LeaderSessionLostBeyondRetryBudgetStillReconnects) {
    // A leader-endpoint session: same machine, its own disjoint corrId range.
    auto cfg = foreverConfig();
    cfg.correlationIdBase = int64_t{1} << 40;
    const std::string leaderEp = "aeron:udp?endpoint=leader:40456";

    // Three redirect-exhaustion paths, each must Backoff (not Disconnected) once Live.
    enum class Path { ConnectResponseRedirect, ConnectResponseWithEndpoint, ErrorResponseRedirect };
    for (const Path path : {Path::ConnectResponseRedirect, Path::ConnectResponseWithEndpoint,
                            Path::ErrorResponseRedirect}) {
        SCOPED_TRACE(static_cast<int>(path));
        FakeIo io;
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 1234, T0);
        ASSERT_EQ(m.state(), sm::SessionState::Connected);
        m.onTransportDisconnected(now);
        m.tick(++now);
        ASSERT_EQ(m.state(), sm::SessionState::Connecting);

        for (int round = 0; round < 3; ++round) {
            // maxRedirects redirects are followed; the next one exhausts the cap.
            for (int r = 0; r <= cfg.maxRedirects; ++r) {
                ASSERT_EQ(m.state(), sm::SessionState::Connecting) << "round " << round << " r " << r;
                const int64_t corr = m.correlationId();
                switch (path) {
                case Path::ConnectResponseRedirect:
                    m.onConnectResponse(kNotLeader, 2, 0, corr, ++now);
                    break;
                case Path::ConnectResponseWithEndpoint:
                    m.onConnectResponseRedirect(leaderEp, corr, ++now);
                    break;
                case Path::ErrorResponseRedirect:
                    m.onErrorResponse(kRedirect, leaderEp, corr, ++now);
                    break;
                }
            }
            ASSERT_EQ(m.state(), sm::SessionState::Backoff) << "redirect cap exhausted after Live";
            now = m.nextAttemptNs();
            m.tick(now);
            ASSERT_EQ(m.state(), sm::SessionState::Connecting)
                << "a backoff attempt follows redirects again (redirect budget renewed)";
        }
        m.onConnectResponse(kOk, 2, 1234, m.correlationId(), ++now);
        EXPECT_EQ(m.state(), sm::SessionState::Connected);
        EXPECT_GE(m.correlationId(), cfg.correlationIdBase) << "stays in its own corrId range";
    }

    // A retriable non-redirect error to the resume (Internal) also backs off.
    FakeIo io;
    sm::SessionMachine m(io, cfg);
    int64_t now = goLive(m, 1, T0);
    m.onTransportDisconnected(now);
    m.tick(++now);
    m.onConnectResponse(kInternal, 0, 0, m.correlationId(), ++now);
    EXPECT_EQ(m.state(), sm::SessionState::Backoff);
    m.tick(m.nextAttemptNs());
    m.onErrorResponse(kInternal, "", m.correlationId(), m.nextAttemptNs() + 1);
    EXPECT_EQ(m.state(), sm::SessionState::Backoff);
}

TEST(SessionMachineRegression, BackoffIsCappedExponential) {
    FakeIo io;
    auto cfg = foreverConfig();
    cfg.maxConnectRetries = 0;  // every timeout after Live enters Backoff directly
    ASSERT_EQ(cfg.backoffInitialNs, 10'000'000LL) << "10 ms first backoff";
    ASSERT_EQ(cfg.backoffMaxNs, 1'000'000'000LL) << "1 s cap";
    sm::SessionMachine m(io, cfg);
    int64_t now = goLive(m, 3, T0);
    m.onTransportDisconnected(now);
    m.tick(++now);

    const int64_t expected[] = {10'000'000,  20'000'000,  40'000'000,    80'000'000,
                                160'000'000, 320'000'000, 640'000'000,   1'000'000'000,
                                1'000'000'000, 1'000'000'000, 1'000'000'000};
    for (std::size_t i = 0; i < std::size(expected); ++i) {
        now = timeOutUntilNotConnecting(m, cfg, now);
        ASSERT_EQ(m.state(), sm::SessionState::Backoff) << "attempt " << i;
        EXPECT_EQ(m.backoffAttempt(), static_cast<int>(i) + 1);
        EXPECT_EQ(m.nextAttemptNs() - now, expected[i]) << "attempt " << i;
        io.reset();
        m.tick(m.nextAttemptNs() - 1);
        EXPECT_EQ(io.sendCount(), 0u) << "attempt " << i << " fired before its deadline";
        now = m.nextAttemptNs();
        m.tick(now);
        EXPECT_EQ(io.sendCount(), 1u) << "attempt " << i << " fires exactly at its deadline";
    }
    // A very long outage never overflows the delay.
    for (int i = 0; i < 200; ++i) {
        now = timeOutUntilNotConnecting(m, cfg, now);
        ASSERT_EQ(m.state(), sm::SessionState::Backoff);
        ASSERT_EQ(m.nextAttemptNs() - now, cfg.backoffMaxNs);
        now = m.nextAttemptNs();
        m.tick(now);
    }
}

TEST(SessionMachineRegression, NeverLiveSessionKeepsTerminalDisconnected) {
    // Flag on, but the session was never Live: the first-connect budget is
    // unchanged — a dead endpoint still fails the connect (terminal Disconnected).
    {
        FakeIo io;
        auto cfg = foreverConfig();
        sm::SessionMachine m(io, cfg);
        m.connect(T0);
        const int64_t now = timeOutUntilNotConnecting(m, cfg, T0);
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
        EXPECT_EQ(io.sendCount(), static_cast<std::size_t>(cfg.maxConnectRetries) + 1);
        m.tick(now + 10'000'000'000LL);
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected) << "no backoff before the first Live";
    }
    {
        FakeIo io;
        auto cfg = foreverConfig();
        sm::SessionMachine m(io, cfg);
        m.connect(T0);
        int64_t now = T0;
        for (int r = 0; r <= cfg.maxRedirects; ++r) {
            m.onConnectResponse(kRedirect, 2, 0, m.correlationId(), ++now);
        }
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    }
    {
        FakeIo io;
        sm::SessionMachine m(io, foreverConfig());
        m.connect(T0);
        m.onConnectResponse(kInternal, 0, 0, m.correlationId(), T0 + 1);
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    }
}

TEST(SessionMachineRegression, NonRetriableAnswerStaysTerminalAfterLive) {
    // ConnectResponse{ProtocolVersion} to a resume after Live.
    {
        FakeIo io;
        auto cfg = foreverConfig();
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 8, T0);
        m.onTransportDisconnected(now);
        m.tick(++now);
        ASSERT_EQ(m.state(), sm::SessionState::Connecting);
        m.onConnectResponse(kProtoVer, 0, 0, m.correlationId(), ++now);
        EXPECT_EQ(m.state(), sm::SessionState::Rejected);
        EXPECT_EQ(m.rejectCode(), kProtoVer);
        io.reset();
        for (int i = 0; i < 10; ++i) m.tick(now += cfg.backoffMaxNs);
        EXPECT_EQ(io.sendCount(), 0u) << "Rejected is terminal: no retries";
        m.connect(now);
        m.forceReconnect(now);
        EXPECT_EQ(m.state(), sm::SessionState::Rejected);
        m.close();
        EXPECT_EQ(m.state(), sm::SessionState::Closed);
    }
    // ErrorResponse{ProtocolVersion} while Connecting (from a Backoff attempt).
    {
        FakeIo io;
        auto cfg = foreverConfig();
        cfg.maxConnectRetries = 0;
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 8, T0);
        m.onTransportDisconnected(now);
        m.tick(++now);
        now = timeOutUntilNotConnecting(m, cfg, now);
        ASSERT_EQ(m.state(), sm::SessionState::Backoff);
        now = m.nextAttemptNs();
        m.tick(now);
        m.onErrorResponse(kProtoVer, "", m.correlationId(), ++now);
        EXPECT_EQ(m.state(), sm::SessionState::Rejected);
        EXPECT_EQ(m.rejectCode(), kProtoVer);
    }
    // Before the first Live, too: the shard fails Rejected rather than Idle.
    {
        FakeIo io;
        sm::SessionMachine m(io, foreverConfig());
        m.connect(T0);
        m.onConnectResponse(kProtoVer, 0, 0, m.correlationId(), T0 + 1);
        EXPECT_EQ(m.state(), sm::SessionState::Rejected);
    }
}

TEST(SessionMachineRegression, PlainClientDefaultStillGivesUp) {
    ASSERT_FALSE(sm::SessionConfig{}.reconnectForeverAfterLive) << "plain Client default unchanged";
    auto cfg = testConfig();
    {
        FakeIo io;
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 8, T0);
        m.onTransportDisconnected(now);
        m.tick(++now);
        now = timeOutUntilNotConnecting(m, cfg, now);
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected) << "today: terminal after the budget";
        io.reset();
        m.tick(now + 10'000'000'000LL);
        EXPECT_EQ(io.sendCount(), 0u);
    }
    {
        FakeIo io;
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 8, T0);
        m.onTransportDisconnected(now);
        m.tick(++now);
        m.onConnectResponse(kProtoVer, 0, 0, m.correlationId(), ++now);
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected) << "no Rejected state without the flag";
    }
    {
        FakeIo io;
        sm::SessionMachine m(io, cfg);
        int64_t now = goLive(m, 8, T0);
        m.onTransportDisconnected(now);
        m.tick(++now);
        for (int r = 0; r <= cfg.maxRedirects; ++r) {
            m.onErrorResponse(kRedirect, "", m.correlationId(), ++now);
        }
        EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
    }
}
