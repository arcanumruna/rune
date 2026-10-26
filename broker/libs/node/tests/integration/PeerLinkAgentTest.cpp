// PeerLinkAgentTest.cpp — authentication plan Task 15: the inter-broker link handshakes over REAL Aeron UDP on the
// peer-auth stream.  Part 1 drives PeerLinkAgents directly (three agents over one embedded media driver, the test
// thread playing the control thread of each).  Part 2 runs three in-process BrokerNodes with authMode Enabled and
// asserts the keys reached the control thread, every worker and the admin thread.
#include "support/SecurityBrokerCluster.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/PeerLinkAgent.h>

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <Aeron.h>
#include <Context.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <rune/node/RuneMetrics.h>
#include <chrono>
#include <map>
#include <sstream>
#include <thread>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::security_cluster;
namespace pl = rune::peerlink;
using namespace std::chrono_literals;

namespace {

class MediaDriver {
public:
    MediaDriver() : driver_(makeCfg(dir_.path / "aeron"), DriverMode::Threaded) {}
    [[nodiscard]] const std::string& aeronDir() const { return driver_.dir(); }
private:
    static EmbeddedDriverConfig makeCfg(const std::filesystem::path& d) {
        EmbeddedDriverConfig c;
        c.dir               = d.string();
        c.termBufferBytes   = 64 * 1024;
        c.termBufferSparse  = true;
        c.udpGso            = false;
        c.epollPoll         = false;
        return c;
    }
    TmpDir         dir_{"plagent_drv"};
    EmbeddedDriver driver_;   // the one dedicated pump thread
};

pl::ClusterKeys keysOf(char c, std::optional<char> next = std::nullopt) {
    auto mk = [](char ch) {
        std::string k(48, ch);
        return *rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    };
    pl::ClusterKeys k{mk(c), std::nullopt};
    if (next) k.next = mk(*next);
    return k;
}

constexpr int32_t kPeerAuthStream = 40;

/// Three agents (ids 1..3) on one driver, each on its own consensus port; driven by the test thread.
struct AgentRig {
    MediaDriver                                  driver;
    std::shared_ptr<aeron::Aeron>                aeron;
    std::vector<uint16_t>                        ports;
    std::map<int32_t, std::unique_ptr<PeerLinkAgent>> agents;
    struct Got { int32_t peer; pl::LinkKeys keys; };
    std::map<int32_t, std::vector<Got>>          established;   // by agent id

    AgentRig() {
        aeron::Context ctx; ctx.aeronDir(driver.aeronDir());
        aeron = aeron::Aeron::connect(ctx);
        for (int i = 0; i < 3; ++i) ports.push_back(rune::aeronutil::testing::allocFreeUdpPort());
    }
    ~AgentRig() { agents.clear(); }

    static void onEstablished(void* ctx, int32_t peer, const pl::LinkKeys& k, int64_t) {
        auto* me = static_cast<std::pair<AgentRig*, int32_t>*>(ctx);
        me->first->established[me->second].push_back({peer, k});
    }
    std::map<int32_t, std::unique_ptr<std::pair<AgentRig*, int32_t>>> ctxs;

    void start(int32_t id, pl::ClusterKeys keys, uint64_t incarnation) {
        PeerLinkAgent::Params p;
        p.selfId = id; p.incarnation = incarnation; p.streamId = kPeerAuthStream; p.keys = std::move(keys);
        p.bindChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[static_cast<std::size_t>(id - 1)]);
        for (int32_t o = 1; o <= 3; ++o)
            if (o != id) p.peers.push_back({o, "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[static_cast<std::size_t>(o - 1)])});
        agents[id] = std::make_unique<PeerLinkAgent>(aeron, std::move(p));
        ctxs[id] = std::make_unique<std::pair<AgentRig*, int32_t>>(this, id);
        agents[id]->setEstablishedSink(&AgentRig::onEstablished, ctxs[id].get());
    }
    void stop(int32_t id) { agents.erase(id); }

    static int64_t nowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    template <class Pred> bool runUntil(std::chrono::milliseconds within, Pred&& done) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            for (auto& [id, a] : agents) a->doWork(nowNs());
            if (done()) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }
};

/// Captures everything spdlog's default logger writes while alive.
struct LogCapture {
    std::ostringstream      out;
    std::shared_ptr<spdlog::logger> saved = spdlog::default_logger();
    LogCapture() {
        auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(out);
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::trace);
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(saved); }
    [[nodiscard]] std::string text() { spdlog::default_logger()->flush(); return out.str(); }
};

fs::path writeKeyFile(const fs::path& dir, const std::string& name, char c) {
    const fs::path p = dir / name;
    { std::ofstream f(p, std::ios::binary); f << std::string(48, c) << "\n"; }
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);   // M13: a group/world-readable key refuses to start
    return p;
}

} // namespace

TEST(PeerLinkAgentRegression, EveryPairEstablishesWithinTwoSeconds) {
    AgentRig rig;
    for (int32_t id = 1; id <= 3; ++id) rig.start(id, keysOf('k'), 100 + static_cast<uint64_t>(id));
    const bool ok = rig.runUntil(2000ms, [&] {
        for (auto& [id, a] : rig.agents) if (a->establishedPeers() != 2) return false;
        return true;
    });
    ASSERT_TRUE(ok) << "every pair within 2 s";
    // Matching directional keys on both ends of every pair.
    std::map<std::pair<int32_t, int32_t>, pl::LinkKeys> byPair;   // (self, peer)
    for (auto& [self, v] : rig.established) for (auto& g : v) byPair[{self, g.peer}] = g.keys;
    for (int32_t a = 1; a <= 3; ++a)
        for (int32_t b = a + 1; b <= 3; ++b) {
            ASSERT_TRUE(byPair.count({a, b}) && byPair.count({b, a}));
            const pl::LinkKeys& ab = byPair[{a, b}];
            const pl::LinkKeys& ba = byPair[{b, a}];
            EXPECT_EQ(ab.sendMaster, ba.recvMaster);
            EXPECT_EQ(ab.recvMaster, ba.sendMaster);
            EXPECT_EQ(ab.epoch, ba.epoch);
            EXPECT_NE(ab.epoch, 0u);
        }
    for (auto& [id, a] : rig.agents) EXPECT_EQ(a->refusals(), 0u);
    // Issue #242: the handshakes ran over HelloV2 / ChallengeV2 (tags 7 / 8) - the initiators send nothing else, and an
    // agent that did not route those tags would have counted them malformed and never linked.
    for (auto& [id, a] : rig.agents) EXPECT_EQ(a->malformedFrames(), 0u) << "tags 7 / 8 are routed, not malformed";
}

TEST(PeerLinkAgentRegression, AWrongKeyNodeIsRefusedByEveryPeerAndLogsIt) {
    LogCapture log;
    AgentRig rig;
    rig.start(1, keysOf('k'), 101);
    rig.start(2, keysOf('k'), 102);
    rig.start(3, keysOf('x'), 103);
    // 1 <-> 2 establish; 3 is refused by both and never establishes.
    ASSERT_TRUE(rig.runUntil(3000ms, [&] {
        return rig.agents[1]->establishedPeers() == 1 && rig.agents[2]->establishedPeers() == 1 &&
               rig.agents[3]->refusals() >= 2 && rig.agents[1]->refusals() >= 1 && rig.agents[2]->refusals() >= 1;
    }));
    EXPECT_EQ(rig.agents[3]->establishedPeers(), 0);
    EXPECT_TRUE(rig.established[3].empty());
    // Counter 1'000'234's source: refusals counted on BOTH sides (the refuser and the refused).
    const std::string t = log.text();
    EXPECT_NE(t.find("peer link from node 1 REFUSED: no common cluster key (key id "), std::string::npos) << t;
    EXPECT_NE(t.find("peer link from node 2 REFUSED: no common cluster key (key id "), std::string::npos) << t;
    EXPECT_NE(t.find("peer link to node 3 REFUSED by 3: no common cluster key"), std::string::npos) << t;
    EXPECT_NE(t.find("peer link with node 2 established (epoch "), std::string::npos) << t;
    const auto* st = rig.agents[1]->stateOf(3);
    ASSERT_NE(st, nullptr);
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(*st));
    EXPECT_NE(std::get<pl::HelloSent>(*st).ephPub, (std::array<uint8_t, 32>{})) << "issue #242: a v2 Hello carries an ephemeral share";
}

TEST(PeerLinkAgentRegression, ARestartedResponderIsReestablishedWithNewKeys) {
    AgentRig rig;
    for (int32_t id = 1; id <= 3; ++id) rig.start(id, keysOf('k'), 100 + static_cast<uint64_t>(id));
    ASSERT_TRUE(rig.runUntil(3000ms, [&] {
        for (auto& [id, a] : rig.agents) if (a->establishedPeers() != 2) return false;
        return true;
    }));
    pl::LinkKeys before;
    for (auto& g : rig.established[1]) if (g.peer == 3) before = g.keys;
    rig.established.clear();
    rig.stop(3);
    rig.start(3, keysOf('k'), 999);        // restarted: a higher incarnation, no link
    ASSERT_TRUE(rig.runUntil(4000ms, [&] {
        return rig.agents[3]->establishedPeers() == 2 && !rig.established[1].empty() && !rig.established[2].empty();
    })) << "the poke makes the established initiators start over";
    pl::LinkKeys after;
    for (auto& g : rig.established[1]) if (g.peer == 3) after = g.keys;
    EXPECT_NE(after.sendMaster, before.sendMaster) << "new keys replace the old ones everywhere";
}

TEST(PeerLinkAgentRegression, AnAgentWithNoPeersTicksQuietly) {
    AgentRig rig;
    PeerLinkAgent::Params p;
    p.selfId = 1; p.incarnation = 1; p.streamId = kPeerAuthStream; p.keys = keysOf('k');
    p.bindChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rig.ports[0]);
    PeerLinkAgent a(rig.aeron, std::move(p));
    EXPECT_EQ(a.doWork(AgentRig::nowNs()), 0);
    EXPECT_EQ(a.establishedPeers(), 0);
}

// ---- Part 2: BrokerNodes, authMode Enabled ------------------------------------------------------------------

namespace {
void enable(NodeConfig& cfg, const fs::path& keyFile) {
    cfg.authMode = AuthMode::Enabled;
    cfg.clientTransport = ClientTransport::PlaintextForTest;   // issue #10: plaintext client sessions, test only
    cfg.clusterKeyFile = keyFile.string();
    cfg.peerAuthStreamId = kPeerAuthStream;
}
} // namespace

TEST(PeerLinkAgentRegression, KeysReachEveryWorkerAndTheAdminThread) {
    TmpDir keys{"plkeys"};
    const fs::path keyFile = writeKeyFile(keys.path, "cluster.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("pl_keys", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    auto allEstablished = [&] {
        for (BrokerNode* n : c.live()) if (n->peerLinksForTest().established != 2) return false;
        return true;
    };
    while (!allEstablished() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(20ms);
    ASSERT_TRUE(allEstablished());

    for (BrokerNode* n : c.live()) {
        const auto snap = n->peerLinksForTest();
        ASSERT_TRUE(snap.present);
        EXPECT_EQ(snap.refusals, 0u);
        for (int32_t peer : snap.peers) {
            const uint32_t epoch = snap.epochOf[peer];
            ASSERT_NE(epoch, 0u) << "control thread of node " << n->nodeIdForTest() << " peer " << peer;
            // The same epoch reaches every worker and the admin thread (over their rings).
            const auto until = std::chrono::steady_clock::now() + 10s;
            uint32_t admin = 0, w0 = 0;
            while (std::chrono::steady_clock::now() < until) {
                admin = n->adminPeerLinkEpochForTest(peer);
                w0 = n->workerPeerLinkEpochForTest(0, peer);
                if (admin == epoch && w0 == epoch) break;
                std::this_thread::sleep_for(20ms);
            }
            EXPECT_EQ(admin, epoch) << "admin thread of node " << n->nodeIdForTest() << " peer " << peer;
            EXPECT_EQ(w0, epoch) << "worker 0 of node " << n->nodeIdForTest() << " peer " << peer;
        }
    }
}

TEST(PeerLinkAgentRegression, TheCounterIdsAreAssignedOnce) {
    EXPECT_EQ(typeIdOf(RuneMetric::PeerLinksEstablished), 1'000'233);
    EXPECT_EQ(typeIdOf(RuneMetric::PeerLinkRefusals), 1'000'234);
}

TEST(PeerLinkAgentRegression, InProcessUnauthenticatedBuildsNoAgent) {
    ThreeNodes c = ThreeNodes::start("pl_none");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (BrokerNode* n : c.live()) EXPECT_FALSE(n->peerLinksForTest().present);
}
