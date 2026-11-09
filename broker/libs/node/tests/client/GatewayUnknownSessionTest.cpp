// GatewayUnknownSessionTest.cpp - L22: a restarted node answers a forgotten session's keepalive Ping with UnknownSession on the
// client's DECLARED egress channel (the keepalive Ping's echo is the egress stream id plus that channel), so the client reconnects at once
// instead of at its response timeout; and a restarted node never re-issues a session id of its previous life.  THREE real brokers, a real client
// whose responseTimeout is 60 s (recovery by timeout is impossible inside the deadlines).  Every wait is bounded.
#include "client/ClusterClient.h"

#include <rune/client/Client.h>

#include <rune/aeronutil/testing/FreePort.h>

#include <gtest/gtest.h>

#include <array>

using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
using namespace std::chrono_literals;
namespace rc = rune::client;
using Clk = std::chrono::steady_clock;

namespace {
constexpr int64_t kResponseTimeoutNs = 60'000'000'000LL;

/// The response ports of the trio, for the response-mode test only (DISABLED_ResponseModeRestartedNodeAnswersRegression).  Allocated once
/// per test; the tweak is re-applied when a node restarts, so the ports survive it.
struct ResponsePorts {
    std::array<uint16_t, 3> gw{}, worker{};
    ResponsePorts() {
        namespace t = rune::aeronutil::testing;
        for (auto& p : gw) p = t::allocFreeUdpPort();
        for (auto& p : worker) p = t::allocFreeUdpPortRange(1);
    }
    void apply(rune::node::NodeConfig& cfg) const {
        cfg.gatewayResponseControlPort = gw[static_cast<std::size_t>(cfg.nodeId - 1)];
        cfg.responseControlBasePort    = worker[static_cast<std::size_t>(cfg.nodeId - 1)];
        for (auto& peer : cfg.peers) {
            peer.gatewayResponseControlPort = gw[static_cast<std::size_t>(peer.id - 1)];
            peer.responseControlBasePort    = worker[static_cast<std::size_t>(peer.id - 1)];
        }
    }
};

rc::ClientConfig slowTimeoutConfig(ThreeNodes& c, const ResponsePorts* rp, int id, int64_t keepaliveNs = 0) {
    auto cfg = clientConfigFor(c, id);
    if (rp != nullptr) cfg.gatewayResponseControlPort = rp->gw[static_cast<std::size_t>(id - 1)];
    cfg.responseTimeoutNs = kResponseTimeoutNs;
    cfg.keepaliveIntervalNs = keepaliveNs;
    return cfg;
}

/// Polls (hard-capped by time AND iterations) until `pred` holds.
template <class Pred>
bool pollUntil(std::chrono::milliseconds within, Pred&& pred) {
    const auto end = Clk::now() + within;
    for (int i = 0; i < 100'000 && Clk::now() < end; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

/// The default config (declared egress) unless `responseMode`.
struct Restarted {
    ResponsePorts rp;
    ThreeNodes c;
    int node = -1;
    const ResponsePorts* ports = nullptr;
    explicit Restarted(const char* tag, bool responseMode = false)
        : c(ThreeNodes::startWith(tag, [this, responseMode](rune::node::NodeConfig& cfg) { if (responseMode) rp.apply(cfg); })) {
        if (responseMode) ports = &rp;
    }
};

uint64_t noticesSent(ThreeNodes& c, int id) { return c.node(id).testUnknownSessionNotices()[0]; }

/// The client's session on `node`, killed and restarted; returns the old session id.  The client then recovers by the notice alone.
void recoverByNotice(const char* tag, bool appTrafficDuringOutage, bool responseMode = false) {
    Restarted r(tag, responseMode);
    ASSERT_TRUE(r.c.awaitCatalogLeader(30s));
    r.node = aFollowerOfTheCatalog(r.c);
    ASSERT_GT(r.node, 0);
    auto client = connectClient(r.c, slowTimeoutConfig(r.c, r.ports, r.node));
    const int64_t old = client->testSeedSessionId();
    ASSERT_NE(old, 0);
    ASSERT_EQ(client->testSeedStaleSessionLosses(), 0u);
    r.c.crash(r.node);
    r.c.startNode(r.node);
    if (appTrafficDuringOutage) {   // an application ping under the forgotten id is unanswerable (no stream id in it): it must not wedge anything
        try { (void)client->pingAndWait(std::span<const uint8_t>{}, 300ms); } catch (const std::exception&) {}
    }
    const bool recovered = pollUntil(5s, [&] { const int64_t s = client->testSeedSessionId(); return s != 0 && s != old; });
    EXPECT_TRUE(recovered) << "the keepalive Ping alone brings the session back within 5 s (response timeout is 60 s)";
    EXPECT_EQ(client->testSeedStaleSessionLosses(), 0u) << "recovery came from the notice, not from the response timeout";
    { const auto n = r.c.node(r.node).testUnknownSessionNotices(); EXPECT_GE(n[0], 1u) << "sent/deduped/dropped/abandoned = " << n[0] << "/" << n[1] << "/" << n[2] << "/" << n[3]; }
    bool ok = false;
    for (int i = 0; i < 50 && !ok; ++i) {
        try { (void)client->pingAndWait(std::span<const uint8_t>{}, 1s); ok = true; } catch (const std::exception&) {}
    }
    EXPECT_TRUE(ok) << "a request routed to the node succeeds on the new session";
    client->close();
}
}  // namespace

TEST(GatewayUnknownSessionRegression, RestartedNodeAnswersUnknownSessionRegression) { recoverByNotice("unk-restart", true); }

TEST(GatewayUnknownSessionRegression, IdleSessionPingGetsUnknownSessionRegression) { recoverByNotice("unk-idle", false); }

// Response mode (gatewayResponseControlPort != 0 on gateway and client): the restarted gateway's notice publication on the response
// channel never connects to the client's surviving response subscription, so recovery is by the response timeout only.  DISABLED until
// that is solved (tracked in a GitHub issue).
TEST(GatewayUnknownSessionRegression, DISABLED_ResponseModeRestartedNodeAnswersRegression) { recoverByNotice("unk-resp", true, true); }

// Part A end to end: client 1's session id from the node's previous life is never handed to client 2 after the restart.
TEST(GatewayUnknownSessionRegression, StaleIdNeverLandsInAnotherSessionRegression) {
    Restarted r("unk-stale");
    ASSERT_TRUE(r.c.awaitCatalogLeader(30s));
    r.node = aFollowerOfTheCatalog(r.c);
    ASSERT_GT(r.node, 0);
    // Client 1 does not send a keepalive for a minute: it cannot recover before client 2 connects.
    auto c1 = connectClient(r.c, slowTimeoutConfig(r.c, r.ports, r.node, 60'000'000'000LL));
    const int64_t stale = c1->testSeedSessionId();
    ASSERT_NE(stale, 0);
    r.c.crash(r.node);
    r.c.startNode(r.node);
    auto c2 = connectClient(r.c, slowTimeoutConfig(r.c, r.ports, r.node));
    const int64_t fresh = c2->testSeedSessionId();
    ASSERT_NE(fresh, 0);
    EXPECT_NE(fresh, stale) << "the restarted node re-issued an id of its previous life";
    // Client 1's request under the stale id must not run inside client 2's session: it gets no answer (UnknownSession needs the keepalive's
    // stream id), and client 2 receives nothing meant for client 1.
    bool answered = false;
    try { (void)c1->pingAndWait(std::span<const uint8_t>{}, 1500ms); answered = true; } catch (const std::exception&) {}
    EXPECT_FALSE(answered) << "the stale id ran inside another session";
    EXPECT_EQ(c2->testStaleSessionNotices(), 0u) << "client 2 got a session-fatal notice naming another client's session";
    EXPECT_EQ(c2->testSeedSessionId(), fresh);
    c1->close();
    c2->close();
}
