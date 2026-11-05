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
#include <random>
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

    static uint64_t onEstablished(void* ctx, int32_t peer, const pl::LinkKeys& k, int64_t) {
        auto* me = static_cast<std::pair<AgentRig*, int32_t>*>(ctx);
        me->first->established[me->second].push_back({peer, k});
        return 0;   // no other plane: the go-ahead leaves at once
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

// Issue #554: a flood of forged HelloV2s (a real peer id and key id, random nonces and shares) onto a responder's
// peer-auth stream is answered at most kHelloAnswersPerPeerPerCycle per peer per duty cycle; the rest are shed before any crypto.  The
// real initiator still links afterwards (its Hello is retried).  Mutation: removing the budget in onFrame_ -> every
// Hello answered, helloShed() == 0 -> red.
TEST(PeerLinkAgentRegression, AHelloFloodIsAnsweredAtMostTheBudgetPerDutyCycleAdversarialRegression) {
    AgentRig rig;
    rig.start(2, keysOf('k'), 102);   // the responder of the pair (1, 2)
    PeerLinkAgent& b = *rig.agents[2];
    const std::string bChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rig.ports[1]);
    const int64_t regId = rig.aeron->addPublication(bChannel, kPeerAuthStream);
    std::shared_ptr<aeron::Publication> pub;
    const auto connectBy = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && !(pub && pub->isConnected()) && std::chrono::steady_clock::now() < connectBy; ++i) {
        if (!pub) pub = rig.aeron->findPublication(regId);
        (void)b.doWork(AgentRig::nowNs());
        std::this_thread::yield();
    }
    ASSERT_TRUE(pub && pub->isConnected());
    // Settle: the responder's own NoLink pokes etc. are not Hellos; nothing is answered yet.
    ASSERT_EQ(b.helloAnswered() + b.helloShed(), 0u);

    constexpr int kFlood = 64;
    std::mt19937_64 rng(554);
    const uint64_t keyId = keysOf('k').current.keyId;
    for (int i = 0; i < kFlood; ++i) {
        pl::PeerHelloV2 h; h.from = 1; h.to = 2; h.keyId = keyId; h.incarnation = rng();
        for (auto& x : h.nonceA) x = static_cast<uint8_t>(rng());
        for (auto& x : h.shareA) x = static_cast<uint8_t>(rng());
        std::array<uint8_t, pl::PeerHelloV2::kSize> wire{};
        h.encode(wire.data());
        aeron::concurrent::AtomicBuffer ab(wire.data(), wire.size());
        int64_t r = -1;
        for (int t = 0; t < 1'000'000 && (r = pub->offer(ab, 0, static_cast<aeron::util::index_t>(wire.size()))) < 0; ++t)
            std::this_thread::yield();
        ASSERT_GE(r, 0) << "offer " << i;
    }
    // Drive the responder until every forged Hello was consumed (answered or shed); at most the budget per cycle.
    int cycles = 0;
    uint64_t maxPerCycle = 0;
    const auto drainBy = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && b.helloAnswered() + b.helloShed() < static_cast<uint64_t>(kFlood) &&
                    std::chrono::steady_clock::now() < drainBy; ++i) {
        const uint64_t before = b.helloAnswered();
        (void)b.doWork(AgentRig::nowNs());
        ++cycles;
        maxPerCycle = std::max(maxPerCycle, b.helloAnswered() - before);
        std::this_thread::yield();
    }
    ASSERT_EQ(b.helloAnswered() + b.helloShed(), static_cast<uint64_t>(kFlood)) << "every forged Hello consumed";
    EXPECT_LE(maxPerCycle, static_cast<uint64_t>(PeerLinkAgent::kHelloAnswersPerPeerPerCycle));
    EXPECT_LE(b.helloAnswered(), static_cast<uint64_t>(PeerLinkAgent::kHelloAnswersPerPeerPerCycle) * static_cast<uint64_t>(cycles));
    EXPECT_GT(b.helloShed(), 0u) << "a burst larger than the budget is shed";
    EXPECT_EQ(b.establishedPeers(), 0);

    rig.start(1, keysOf('k'), 101);   // the real initiator still links within the usual bound
    ASSERT_TRUE(rig.runUntil(2000ms, [&] {
        return rig.agents[1]->establishedPeers() == 1 && rig.agents[2]->establishedPeers() == 1;
    })) << "shed " << b.helloShed();
}

namespace {
/// Connects a publication from the test onto `target`'s peer-auth stream, driving `target` until it is connected.
std::shared_ptr<aeron::Publication> connectForgePub(AgentRig& rig, int32_t target) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rig.ports[static_cast<std::size_t>(target - 1)]);
    const int64_t regId = rig.aeron->addPublication(ch, kPeerAuthStream);
    std::shared_ptr<aeron::Publication> pub;
    const auto connectBy = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && !(pub && pub->isConnected()) && std::chrono::steady_clock::now() < connectBy; ++i) {
        if (!pub) pub = rig.aeron->findPublication(regId);
        (void)rig.agents[target]->doWork(AgentRig::nowNs());
        std::this_thread::yield();
    }
    return pub;
}
bool offerWire(aeron::Publication& pub, const uint8_t* data, std::size_t n) {
    aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(data), n);
    int64_t r = -1;
    for (int t = 0; t < 1'000'000 && (r = pub.offer(ab, 0, static_cast<aeron::util::index_t>(n))) < 0; ++t) std::this_thread::yield();
    return r >= 0;
}
bool offerHelloV2(aeron::Publication& pub, std::mt19937_64& rng, int32_t from, int32_t to, uint64_t keyId) {
    pl::PeerHelloV2 h; h.from = from; h.to = to; h.keyId = keyId; h.incarnation = rng();
    for (auto& x : h.nonceA) x = static_cast<uint8_t>(rng());
    for (auto& x : h.shareA) x = static_cast<uint8_t>(rng());
    std::array<uint8_t, pl::PeerHelloV2::kSize> wire{};
    h.encode(wire.data());
    return offerWire(pub, wire.data(), wire.size());
}
}  // namespace

// Issue #683: the budget is PER PEER.  A flood of HelloV2s naming peer 1 (a valid key id) onto node 3 must not shed
// peer 2's Hello.  Every polled batch of 8 fragments is [peer 1, peer 1, peer 2, peer 1 x5]: under a global budget the
// two peer-1 frames in front spend it and peer 2's frame is shed; per peer, peer 2's frame is the only one of its peer
// in the cycle and is always answered.  Afterwards the real node 2 links with node 3.
// Mutation: a global budget (the first version of #554) sheds peer 2's frames -> helloShedOf(2) > 0 -> red.
TEST(PeerLinkAgentRegression, AHelloFloodNamingOnePeerNeverStarvesAnotherAdversarialRegression) {
    AgentRig rig;
    rig.start(3, keysOf('k'), 103);   // the responder of both (1,3) and (2,3)
    PeerLinkAgent& c = *rig.agents[3];
    auto pub = connectForgePub(rig, 3);
    ASSERT_TRUE(pub && pub->isConnected());
    ASSERT_EQ(c.helloAnswered() + c.helloShed(), 0u);
    std::mt19937_64 rng(683);
    const uint64_t keyId = keysOf('k').current.keyId;
    constexpr int kGroups = 8;
    for (int g = 0; g < kGroups; ++g)
        for (int i = 0; i < 8; ++i)
            ASSERT_TRUE(offerHelloV2(*pub, rng, i == 2 ? 2 : 1, 3, keyId)) << g << "/" << i;
    constexpr uint64_t kTotal = kGroups * 8;
    const auto drainBy = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && c.helloAnswered() + c.helloShed() < kTotal && std::chrono::steady_clock::now() < drainBy; ++i) {
        (void)c.doWork(AgentRig::nowNs());
        std::this_thread::yield();
    }
    ASSERT_EQ(c.helloAnswered() + c.helloShed(), kTotal) << "every forged Hello consumed";
    EXPECT_EQ(c.helloShedOf(2), 0u) << "peer 2 was starved by a flood naming peer 1";
    EXPECT_GT(c.helloShedOf(1), 0u);
    EXPECT_EQ(c.helloShed(), c.helloShedOf(1) + c.helloShedOf(2));

    rig.start(2, keysOf('k'), 102);   // a real handshake with node 3 still completes within the usual bound
    ASSERT_TRUE(rig.runUntil(3000ms, [&] { return rig.agents[2]->establishedPeers() >= 1 && c.establishedPeers() >= 1; }))
        << "shed(2)=" << c.helloShedOf(2);
}

// Issue #554: garbage wearing a Hello tag is never charged: a body that does not decode, and a well-formed HelloV2 with
// a key id we do not hold (refused cheaply).  None of it sheds, and a real handshake still links afterwards.
// Mutation: charging before decode / before the key check sheds most of the 64 frames -> helloShed() > 0 -> red.
TEST(PeerLinkAgentRegression, GarbageWithAHelloTagIsNeverChargedRegression) {
    AgentRig rig;
    rig.start(2, keysOf('k'), 102);
    PeerLinkAgent& b = *rig.agents[2];
    auto pub = connectForgePub(rig, 2);
    ASSERT_TRUE(pub && pub->isConnected());
    std::mt19937_64 rng(554);
    const uint64_t before = b.malformedFrames() + b.refusals();
    constexpr int kFlood = 64;
    for (int i = 0; i < kFlood; ++i) {
        if (i % 2 == 0) {   // a truncated body: tag + from, no decode
            std::array<uint8_t, 12> w{};
            w[0] = static_cast<uint8_t>(pl::PeerMsg::HelloV2); w[1] = 1;
            for (std::size_t k = 9; k < w.size(); ++k) w[k] = static_cast<uint8_t>(rng());
            ASSERT_TRUE(offerWire(*pub, w.data(), w.size()));
        } else {            // well-formed, but a key id nobody holds
            ASSERT_TRUE(offerHelloV2(*pub, rng, 1, 2, keysOf('k').current.keyId ^ 0xDEADBEEFull));
        }
    }
    const auto drainBy = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < 10'000'000 && b.malformedFrames() + b.refusals() - before < kFlood &&
                    std::chrono::steady_clock::now() < drainBy; ++i) {
        (void)b.doWork(AgentRig::nowNs());
        std::this_thread::yield();
    }
    EXPECT_GE(b.malformedFrames() + b.refusals() - before, static_cast<uint64_t>(kFlood)) << "every frame refused or malformed";
    EXPECT_EQ(b.helloShed(), 0u) << "garbage is never charged";
    EXPECT_EQ(b.helloAnswered(), 0u);

    rig.start(1, keysOf('k'), 101);
    ASSERT_TRUE(rig.runUntil(2000ms, [&] { return rig.agents[1]->establishedPeers() == 1 && b.establishedPeers() == 1; }));
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

// ---------------------------------------------------------------------------------------------------------------------
// Review of the go-ahead hold (8d726892), adversarial: the agents are driven on an INJECTED clock (the handshake's
// kRetryNs / kAwaitNs run on it), the frames travel over real Aeron UDP.  The sink models the other planes: while
// `hold` is set it answers a sequence number (the install is still running on a worker / the admin thread) and the
// test plays BrokerNode::servicePeerLinks_ by calling releaseHeldAt when it decides every plane acknowledged.
namespace {
struct GatedSink {
    struct Call { int32_t peer; pl::LinkKeys keys; uint64_t seq; };
    std::vector<Call> calls;
    bool     hold = false;
    uint64_t next = 0;
    static uint64_t fn(void* ctx, int32_t peer, const pl::LinkKeys& k, int64_t) {
        auto* s = static_cast<GatedSink*>(ctx);
        const uint64_t seq = s->hold ? ++s->next : 0;
        s->calls.push_back({peer, k, seq});
        return seq;
    }
    [[nodiscard]] bool sawInstall(int32_t peer, uint32_t epoch, pl::LinkInstall mode) const {
        for (const auto& c : calls) if (c.peer == peer && c.keys.epoch == epoch && c.keys.install == mode) return true;
        return false;
    }
};
std::size_t slotOf(const PeerLinkAgent& a, int32_t peer) {
    for (std::size_t i = 0; i < a.peerCount(); ++i) if (a.peerIdAt(i) == peer) return i;
    return SIZE_MAX;
}
const pl::Established* establishedOf(const PeerLinkAgent& a, int32_t peer) {
    const auto* st = a.stateOf(peer);
    return st != nullptr ? std::get_if<pl::Established>(st) : nullptr;
}
/// Agents 1 (initiator) and 2 (responder) only, each with a GatedSink, on an injected clock `t` advanced `stepNs` per
/// pass (one real ms per pass lets the UDP frames land).  Every drive is capped and fails rather than hangs.
struct GatedPair {
    AgentRig  rig;
    GatedSink s1, s2;
    int64_t   t = 5'000'000'000'000;
    GatedPair() {
        rig.start(1, keysOf('k'), 101);
        rig.start(2, keysOf('k'), 102);
        rig.agents[1]->setEstablishedSink(&GatedSink::fn, &s1);
        rig.agents[2]->setEstablishedSink(&GatedSink::fn, &s2);
    }
    PeerLinkAgent& a1() { return *rig.agents[1]; }
    PeerLinkAgent& a2() { return *rig.agents[2]; }
    void pass(int64_t stepNs) {
        t += stepNs;
        a1().doWork(t);
        a2().doWork(t);
        std::this_thread::sleep_for(1ms);
    }
    template <class Pred> bool driveUntil(int maxPasses, int64_t stepNs, Pred&& done) {
        for (int i = 0; i < maxPasses; ++i) { pass(stepNs); if (done()) return true; }
        return false;
    }
};
} // namespace

// The initiator's planes are slow: its Confirm (the responder's promotion trigger) is held although the responder
// re-sends its Accept every kRetryNs for kAwaitNs and each re-send is answered with the Confirm again (#584).  Every
// one of those answers must be held too; the responder stays receive-only until the initiator releases, then promotes.
// Mutation: PeerLinkAgent::absorb_ sending a re-sent Confirm at once while awaitSeq != 0 -> the responder promotes
// before the release -> red.
TEST(PeerLinkAgentRegression, AHeldConfirmStaysHeldThroughEveryAcceptResendAdversarialRegression) {
    GatedPair g;
    ASSERT_TRUE(g.driveUntil(5000, 1'000'000, [&] { return establishedOf(g.a1(), 2) && establishedOf(g.a2(), 1); }))
        << "the first link";
    const uint32_t e1 = establishedOf(g.a1(), 2)->keys.epoch;
    g.s1.hold = true;
    ASSERT_TRUE(g.a1().forceRekeyForTest(2, g.t));
    // The new epoch reaches the initiator (Accept); its planes have not acknowledged the receive-only install.
    ASSERT_TRUE(g.driveUntil(3000, 1'000'000, [&] {
        const auto* e = establishedOf(g.a1(), 2);
        return e != nullptr && e->keys.epoch != e1;
    })) << "the initiator took the Accept";
    const uint32_t e2 = establishedOf(g.a1(), 2)->keys.epoch;
    const std::size_t i1 = slotOf(g.a1(), 2);
    ASSERT_NE(g.a1().awaitedInstallAt(i1), 0u) << "the Confirm awaits the initiator's planes";
    EXPECT_TRUE(g.s1.sawInstall(2, e2, pl::LinkInstall::ReceiveOnly));
    // Past the responder's whole re-send window (kAwaitNs) plus slack: several Accept re-sends, each answered and held.
    for (int i = 0; i < 1500; ++i) {
        g.pass(1'000'000);
        const auto* r = establishedOf(g.a2(), 1);
        ASSERT_NE(r, nullptr);
        ASSERT_EQ(r->keys.epoch, e2);
        ASSERT_TRUE(r->unconfirmedAccept.has_value()) << "the responder promoted at pass " << i << " while the initiator's planes could not open " << e2;
    }
    EXPECT_FALSE(g.s1.sawInstall(2, e2, pl::LinkInstall::SendAndReceive)) << "the initiator switched send before its planes acked";
    EXPECT_FALSE(g.s2.sawInstall(1, e2, pl::LinkInstall::SendAndReceive));
    // Every plane acknowledged: the send side switches and the Confirm (its newest copy) leaves; the responder promotes.
    g.a1().releaseHeldAt(i1);
    EXPECT_EQ(g.a1().awaitedInstallAt(i1), 0u);
    EXPECT_TRUE(g.s1.sawInstall(2, e2, pl::LinkInstall::SendAndReceive));
    ASSERT_TRUE(g.driveUntil(2000, 1'000'000, [&] { return g.s2.sawInstall(1, e2, pl::LinkInstall::SendAndReceive); }))
        << "the released Confirm promotes the responder (even past kAwaitNs)";
    EXPECT_FALSE(establishedOf(g.a2(), 1)->unconfirmedAccept.has_value());
}

// The responder's planes are slow: its Accept is held (the initiator must not seal under the epoch), and so is its own
// promotion on the Confirm (a second receive-only install, then send-and-receive only on the release).
// Mutation: absorb_ sending the Accept at once -> the initiator reaches the new epoch before the release -> red.
TEST(PeerLinkAgentRegression, AHeldAcceptAndAHeldPromotionOnTheResponderAdversarialRegression) {
    GatedPair g;
    ASSERT_TRUE(g.driveUntil(5000, 1'000'000, [&] { return establishedOf(g.a1(), 2) && establishedOf(g.a2(), 1); }));
    const uint32_t e1 = establishedOf(g.a1(), 2)->keys.epoch;
    g.s2.hold = true;
    ASSERT_TRUE(g.a1().forceRekeyForTest(2, g.t));
    const std::size_t i2 = slotOf(g.a2(), 1);
    ASSERT_TRUE(g.driveUntil(2000, 1'000'000, [&] { return g.a2().awaitedInstallAt(i2) != 0; })) << "the responder established";
    const uint32_t e2 = establishedOf(g.a2(), 1)->keys.epoch;
    ASSERT_NE(e2, e1);
    for (int i = 0; i < 400; ++i) {   // less than kAwaitNs: the initiator still waits for the Accept
        g.pass(1'000'000);
        const auto* e = establishedOf(g.a1(), 2);
        ASSERT_TRUE(e == nullptr || e->keys.epoch == e1) << "the initiator reached " << e2 << " while the responder's planes could not open it";
    }
    g.a2().releaseHeldAt(i2);
    ASSERT_TRUE(g.driveUntil(2000, 1'000'000, [&] { return g.a2().awaitedInstallAt(i2) != 0; }))
        << "the Confirm arrives and the responder's promotion awaits its planes in turn";
    EXPECT_TRUE(establishedOf(g.a1(), 2) && establishedOf(g.a1(), 2)->keys.epoch == e2);
    EXPECT_FALSE(g.s2.sawInstall(1, e2, pl::LinkInstall::SendAndReceive)) << "the responder switched send before its planes acked";
    g.a2().releaseHeldAt(i2);
    EXPECT_TRUE(g.s2.sawInstall(1, e2, pl::LinkInstall::SendAndReceive));
    EXPECT_EQ(g.a2().awaitedInstallAt(i2), 0u);
}

// Liveness (rule 16): a FIRST link must establish whatever the responder's planes' acknowledgement latency.  The
// responder's planes ack every install kAwaitNs + 100 ms after it was posted (a worker / the admin thread busy for that
// long).  A held first-link Accept leaves after the initiator's ChallengeSent gave up (kAwaitNs), every retry is a new
// epoch held as long, and the link never establishes while the latency lasts; so a responder's send-and-receive Accept
// is never held.  Mutation: absorb_ holding that Accept too (as 8d726892 did) -> no link in 15 s -> red.
TEST(PeerLinkAgentRegression, AFirstLinkEstablishesWhateverTheResponderPlanesAckLatencyAdversarialRegression) {
    GatedPair g;
    g.s2.hold = true;
    constexpr int64_t kAckLatencyNs = pl::PeerHandshake::kAwaitNs + 100'000'000;
    const std::size_t i2 = slotOf(g.a2(), 1);
    uint64_t awaited = 0;
    int64_t  since = 0;
    const bool linked = g.driveUntil(3000, 5'000'000, [&] {   // 15 s of injected time
        const uint64_t s = g.a2().awaitedInstallAt(i2);
        if (s != awaited) { awaited = s; since = g.t; }
        if (s != 0 && g.t - since >= kAckLatencyNs) { g.a2().releaseHeldAt(i2); awaited = 0; }
        const auto* e1 = establishedOf(g.a1(), 2);
        return e1 != nullptr && establishedOf(g.a2(), 1) != nullptr && e1->keys.epoch == establishedOf(g.a2(), 1)->keys.epoch;
    });
    EXPECT_TRUE(linked) << "1 -> 2 never established; responder installs: " << g.s2.calls.size();
}
