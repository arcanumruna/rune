// ClientConnectRoleRegressionTest.cpp — the REAL client (rune::client::Client,
// Thread runner) Connecting to a broker of a 3-node in-process cluster in EVERY
// role it can be in: consensus / catalog leader, follower, catching up (just
// restarted from an empty data dir), halted (its catalog applier halted), the
// lone survivor below minIsr, and stopped.
//
// Seeding on ANY broker works: every running broker hosts the session itself
// (only DDL is redirected, to the catalog leader).  A broker that cannot answer
// at all (stopped) ends the connect explicitly at the client's connect timeout.
// No connect ever waits forever.
//
// Root cause pinned here (follower-connect-hang.md): a cluster with no user
// topic has no routing image, and ClientGateway::handleConnect used the absence
// of the image to fall back to the pre-MD1 "only the Raft leader hosts
// sessions" gate — every follower Redirected the Connect; the client's seed
// never followed a connect-time Redirect (a no-op retarget), so the connect
// burnt its redirect budget on the follower and only ended at its timeout.
#include "support/CatalogBrokerCluster.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using namespace rune::node::test::catalog_cluster;

namespace {

/// The connect bound of every test here: the client's own connect timeout plus
/// margin.  A connect that is neither complete nor explicitly failed by then is
/// the hang this suite exists for.
constexpr auto kConnectTimeoutMs = 5'000;
constexpr auto kBound            = 15s;

/// The node a session id was minted by: SessionTable::nextSessionId stamps
/// ((nodeId + 1) << 40) into every id it hands out.
int32_t hostOfSession(int64_t sessionId) { return static_cast<int32_t>(sessionId >> 40) - 1; }

int32_t consensusLeaderOf(ThreeNodes& c) {
    for (rune::node::BrokerNode* n : c.live())
        if (n->isLeader()) return n->nodeIdForTest();
    return -1;
}

struct Outcome {
    bool                      ok = false;
    std::string               failure;     // the explicit failure, when !ok
    int64_t                   sessionId = 0;
    std::chrono::milliseconds took{0};
};

/// One real client connecting to broker `id`; owned by the caller (`out`).
Outcome connectTo(ThreeNodes& c, int32_t id, const std::string& tag,
                  std::unique_ptr<rune::client::Client>* keep = nullptr) {
    rune::client::ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
    cfg.gatewayStreamId  = kClientStream;
    cfg.egressChannel    = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.egressStreamId   = 2;
    cfg.dataDir          = (c.dirs[static_cast<std::size_t>(id - 1)]->path.parent_path() /
                            ("rune_conn_client_" + tag + "_" + std::to_string(::getpid()))).string();
    cfg.connectTimeoutMs = kConnectTimeoutMs;
    auto client = std::make_unique<rune::client::Client>(cfg);
    auto sid = std::make_shared<std::atomic<int64_t>>(0);
    client->onConnected([sid](int64_t s) { sid->store(s); });
    Outcome o;
    const auto t0 = std::chrono::steady_clock::now();
    try {
        client->connectAndWait(kBound);
        o.ok = true;
    } catch (const std::exception& e) {
        o.failure = e.what();
    }
    o.took      = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
    // Isolation fix (2026-10-15, found once the suite ran in parallel): onConnected runs on the client's duty
    // thread and is not ordered before connectAndWait() returns — on a loaded host (ctest -j) the session id
    // was read as 0 (host -1) before the callback stored it.  Wait for it (bounded) after a successful connect.
    for (const auto dl = std::chrono::steady_clock::now() + 5s; o.ok && sid->load() == 0 &&
                                                                std::chrono::steady_clock::now() < dl;)
        std::this_thread::sleep_for(1ms);
    o.sessionId = sid->load();
    if (keep) *keep = std::move(client);
    return o;
}

/// Connect to `id` and require the session to be hosted BY `id` itself.
void expectHostedHere(ThreeNodes& c, int32_t id, const std::string& role, const std::string& tag,
                      std::unique_ptr<rune::client::Client>* keep = nullptr) {
    const Outcome o = connectTo(c, id, tag, keep);
    ASSERT_TRUE(o.ok) << role << " broker " << id << ": connect failed after " << o.took.count()
                      << " ms: " << o.failure;
    EXPECT_LT(o.took, kBound) << role;
    EXPECT_EQ(hostOfSession(o.sessionId), id)
        << role << " broker " << id << " must host the session itself (session " << o.sessionId << ")";
}

int32_t aFollower(ThreeNodes& c) {
    const int32_t ctl = consensusLeaderOf(c);
    const int32_t cat = c.settledLeader();
    for (int32_t id = 1; id <= 3; ++id)
        if (id != ctl && id != cat && c.running(id)) return id;
    return -1;
}

bool awaitConsensusLeader(ThreeNodes& c, std::chrono::seconds within) {
    for (const auto dl = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < dl;) {
        if (consensusLeaderOf(c) > 0) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

} // namespace

// Leader and BOTH followers of an empty cluster (no user topic: no routing
// image) host the session themselves.  RED before the fix: both followers
// Redirect the Connect and the client times out on them.
TEST(ClientConnectRoleRegression, EveryBrokerOfAnEmptyClusterHostsTheSessionRegression) {
    ThreeNodes c = ThreeNodes::start("conn_each");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    ASSERT_TRUE(awaitConsensusLeader(c, 30s));
    const int32_t ctl = consensusLeaderOf(c);
    for (int32_t id = 1; id <= 3; ++id)
        expectHostedHere(c, id, id == ctl ? "consensus leader" : "follower", "each" + std::to_string(id));
}

// A session hosted on a FOLLOWER serves DDL: only the DDL is redirected (to the
// catalog leader), the session stays on the follower.
TEST(ClientConnectRoleRegression, AFollowerHostedSessionRunsDdlThroughTheRedirectRegression) {
    ThreeNodes c = ThreeNodes::start("conn_ddl");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    ASSERT_TRUE(awaitConsensusLeader(c, 30s));
    const int32_t f = aFollower(c);
    ASSERT_GT(f, 0) << "a broker that leads neither consensus nor the catalog";
    std::unique_ptr<rune::client::Client> client;
    expectHostedHere(c, f, "follower", "ddl", &client);
    ASSERT_TRUE(client);
    const auto r = client->defineTopicAndWait("from_follower", 1, "", {}, rune::client::Persistence::Disk,
                                              {}, {}, 20s);
    EXPECT_EQ(r.errorCode, 0) << r.detail;
}

// Catching up: a follower restarted from an EMPTY data dir is connected to at
// once — before it has replicated the catalog or learnt the leader.
TEST(ClientConnectRoleRegression, ACatchingUpBrokerHostsTheSessionRegression) {
    ThreeNodes c = ThreeNodes::start("conn_catchup");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    ASSERT_TRUE(awaitConsensusLeader(c, 30s));
    const int32_t f = aFollower(c);
    ASSERT_GT(f, 0);
    c.stop(f);
    c.wipe(f);
    c.startNode(f);
    expectHostedHere(c, f, "catching-up", "catchup");
    // …and once it has caught up (back in the catalog ISR), still.
    ASSERT_TRUE(c.awaitInCatalogIsr(f, 60s));
    expectHostedHere(c, f, "caught-up follower", "caughtup");
}

// Halted: a follower whose catalog applier halted still hosts sessions (the
// halt refuses its DDL — Internal — never its connects).
TEST(ClientConnectRoleRegression, AHaltedBrokerHostsTheSessionRegression) {
    ThreeNodes c = ThreeNodes::start("conn_halted");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    ASSERT_TRUE(awaitConsensusLeader(c, 30s));
    const int32_t h = aFollower(c);
    ASSERT_GT(h, 0);
    rune::node::BrokerNode& n = c.node(h);
    n.onAdminThreadForTest([&n] { n.adminWorkerAgentForTest()->worker().applierForTest().tamperNextFingerprintForTest(); });
    ASSERT_GE(c.appendEventOnCatalogLeaderForTest(), 0) << "the event broker " << h << " halts on";
    bool halted = false;
    for (const auto dl = std::chrono::steady_clock::now() + 15s; !halted && std::chrono::steady_clock::now() < dl;) {
        halted = n.onAdminThreadForTest([&n] { return n.adminWorkerAgentForTest()->worker().roleName() == "Halted"; });
        if (!halted) std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(halted);
    expectHostedHere(c, h, "halted", "halted");
}

// Below minIsr: the two other brokers stopped — the lone survivor (whichever
// role it had: here a follower, so it still believes in a now-dead leader for
// a while) hosts the session itself.
TEST(ClientConnectRoleRegression, TheLoneSurvivorBelowMinIsrHostsTheSessionRegression) {
    ThreeNodes c = ThreeNodes::start("conn_minisr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    ASSERT_TRUE(awaitConsensusLeader(c, 30s));
    const int32_t s = aFollower(c);
    ASSERT_GT(s, 0);
    for (int32_t id = 1; id <= 3; ++id)
        if (id != s) c.stop(id);
    expectHostedHere(c, s, "below-minIsr survivor", "minisr");
}

// Stopped: nothing answers — the connect ends EXPLICITLY (timed out) at the
// client's connect timeout, never a wait past it.
TEST(ClientConnectRoleRegression, AStoppedBrokerEndsTheConnectExplicitlyAtTheTimeoutRegression) {
    ThreeNodes c = ThreeNodes::start("conn_stopped");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t f = c.settledLeader() == 3 ? 2 : 3;
    c.stop(f);
    const Outcome o = connectTo(c, f, "stopped");
    EXPECT_FALSE(o.ok);
    EXPECT_NE(o.failure.find("timed out"), std::string::npos) << o.failure;
    EXPECT_LT(o.took, std::chrono::milliseconds(kConnectTimeoutMs + 3'000)) << "ends at the connect timeout";
}
