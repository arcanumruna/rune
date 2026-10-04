// A10 (2026-10-04) regression: a live, busy session must never be declared stale.
//
// Bug: the session's activity clock was refreshed only by ConnectResponse /
// ErrorResponse / Pong (Pong always credited to the seed) and the keepalive was
// one-way, so every producing session went "stale" after
// responseTimeoutNs (10 s) and resumed with a fresh Connect every 10 s.  Under
// load each resume could hit a broker-side ack-publication failure and turn into
// a reconnect storm (p99 = 10 s).
//
// Deterministic: the agent is never started (no Aeron), frames are injected with
// an explicit clock, no sleeps.
#include <rune/client/agent/ClientAgent.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

namespace rca = rune::client::agent;
namespace rcc = rune::client::codec;
namespace sm  = rune::client::session;
namespace rpc = rune::protocol::client;

namespace {

constexpr int64_t T0       = 1'000'000'000'000LL;
constexpr int64_t kMs      = 1'000'000LL;
constexpr int64_t kTimeout = 1'000 * kMs;   // responseTimeoutNs used by these tests

rca::AgentConfig cfg() {
    rca::AgentConfig c;
    c.transport.ingressChannel  = "aeron:udp?endpoint=127.0.0.1:1";
    c.transport.ingressStreamId = 1;
    c.transport.egressChannel   = "aeron:udp?endpoint=127.0.0.1:2";
    c.transport.egressStreamId  = 2;
    c.session.egressChannel     = c.transport.egressChannel;
    c.session.egressStreamId    = 2;
    c.session.keepaliveIntervalNs = 100 * kMs;
    c.session.responseTimeoutNs   = kTimeout;
    return c;
}

// Bring session `s` to Connected with `sid` via an injected ConnectResponse.
void connect(rca::ClientAgent& a, sm::SessionMachine& m, int64_t sid, int64_t now) {
    m.connect(now);
    std::array<uint8_t, 256> buf{};
    const std::size_t n = rcc::encodeConnectResponse(buf.data(), buf.size(), static_cast<int16_t>(rpc::ErrorCode::OK),
                                                     /*leaderMemberId=*/0, /*assignedSessionId=*/sid,
                                                     /*sessionId=*/0, m.correlationId());
    ASSERT_GT(n, 0u);
    a.dispatchEgressFrameForTest(now, buf.data(), n);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);
}

void produceResponse(rca::ClientAgent& a, int64_t sid, int64_t now) {
    std::array<uint8_t, 128> buf{};
    const std::size_t n = rcc::encodeProduceResponse(buf.data(), buf.size(), 0, /*partition=*/0, /*baseOffset=*/1,
                                                     sid, /*correlationId=*/7);
    ASSERT_GT(n, 0u);
    a.dispatchEgressFrameForTest(now, buf.data(), n);
}

void pong(rca::ClientAgent& a, int64_t sid, int64_t corr, int64_t now) {
    std::array<uint8_t, 128> buf{};
    const std::size_t n = rcc::encodePong(buf.data(), buf.size(), "", sid, corr);
    ASSERT_GT(n, 0u);
    a.dispatchEgressFrameForTest(now, buf.data(), n);
}

}  // namespace

// Broker session ids are a per-node counter: two nodes typically both assign id 1
// (review C1).  Each session's keepalive Pong is routed by its own correlationId,
// so a session answered by its node stays Connected while a session whose node is
// silent goes stale — even though the live node's acks carry the SAME session id.
TEST(SessionLivenessRegression, PongRoutesByKeepaliveCorrelationNotByCollidingSessionId) {
    rca::ClientAgent a(cfg());
    sm::SessionMachine& seed  = a.sessionMachineForTest(0);
    sm::SessionMachine& other = a.sessionForEndpointForTest("aeron:udp?endpoint=127.0.0.1:3");
    connect(a, seed, /*sid=*/1, T0);
    connect(a, other, /*sid=*/1, T0);   // the collision
    ASSERT_NE(seed.keepaliveCorrelationId(), other.keepaliveCorrelationId());
    ASSERT_LT(seed.keepaliveCorrelationId(), 0);
    ASSERT_LT(other.keepaliveCorrelationId(), 0);
    bool seedWentStale = false;
    for (int64_t t = T0; t <= T0 + 3 * kTimeout; t += 50 * kMs) {
        produceResponse(a, 1, t);                                // acks with the shared id
        pong(a, 1, other.keepaliveCorrelationId(), t);           // only `other`'s node answers
        a.tickSessionsForTest(t);
        ASSERT_EQ(other.state(), sm::SessionState::Connected) << "+" << (t - T0) / kMs << " ms";
        seedWentStale |= seed.state() != sm::SessionState::Connected;
    }
    EXPECT_TRUE(seedWentStale) << "a silent node's session must time out; acks with a colliding id must not mask it";
}

// A busy session whose node answers its keepalives never goes stale (the A10 false resume).
TEST(SessionLivenessRegression, AnsweredKeepalivesKeepABusySessionAlive) {
    rca::ClientAgent a(cfg());
    sm::SessionMachine& m = a.sessionMachineForTest(0);
    connect(a, m, /*sid=*/41, T0);
    for (int64_t t = T0; t <= T0 + 5 * kTimeout; t += 50 * kMs) {
        produceResponse(a, 41, t);
        if ((t - T0) % (500 * kMs) == 0) pong(a, 41, m.keepaliveCorrelationId(), t);
        a.tickSessionsForTest(t);
        ASSERT_EQ(m.state(), sm::SessionState::Connected) << "went stale at +" << (t - T0) / kMs << " ms";
    }
}

// Control: a session that hears nothing at all still times out (the detector is not disabled).
TEST(SessionLivenessRegression, SilentSessionStillTimesOut) {
    rca::ClientAgent a(cfg());
    sm::SessionMachine& m = a.sessionMachineForTest(0);
    connect(a, m, /*sid=*/41, T0);
    a.tickSessionsForTest(T0 + kTimeout + 1);
    EXPECT_NE(m.state(), sm::SessionState::Connected);
}
