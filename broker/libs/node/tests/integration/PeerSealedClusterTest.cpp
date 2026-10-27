// PeerSealedClusterTest.cpp — authentication plan Task 16 (D34), END TO END: three in-process BrokerNodes with
// authMode Enabled over real Aeron UDP.  Their consensus, controller / replication (legacy transport), every worker's
// replication plane and the admin plane (`__catalog`, `__security`) all run sealed, keyed by the links the PeerLinkAgent
// establishes - and the cluster works exactly as an unsealed one.
#include "support/SecurityBrokerCluster.h"

#include <rune/node/EmbeddedDriver.h>
#include <rune/node/RuneMetrics.h>
#include <rune/peerlink/ClusterKeys.h>
#include <rune/peerlink/PeerLinkMessages.h>

#include <Aeron.h>
#include <Context.h>

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <map>
#include <random>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::security_cluster;
using namespace std::chrono_literals;

namespace {

fs::path keyFileWith(const fs::path& dir, const std::string& name, char c) {
    const fs::path p = dir / name;
    std::ofstream(p, std::ios::binary) << std::string(48, c) << "\n";
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);   // M13: a group/world-readable key refuses to start
    return p;
}

void enable(NodeConfig& cfg, const fs::path& keyFile) {
    cfg.authMode = AuthMode::Enabled;
    cfg.clientTransport = ClientTransport::PlaintextForTest;   // issue #10: plaintext client sessions, test only
    cfg.clusterKeyFile = keyFile.string();
}

template <class Pred> bool until(std::chrono::milliseconds within, Pred&& p) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (p()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

} // namespace

TEST(PeerSealedClusterRegression, ASealedClusterFormsAndReplicatesCatalogAndSecurityExactlyLikeAnUnsealedOne) {
    TmpDir keys{"pscl"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_cluster", [&](NodeConfig& cfg) { enable(cfg, keyFile); });

    // The controller / catalog leadership rides the legacy transport and the admin plane, both sealed.
    ASSERT_TRUE(c.awaitCatalogLeader(60s)) << "no catalog leader over sealed transports";
    ASSERT_TRUE(awaitSecurityLeader(c, 60s)) << "no __security leader over the sealed admin plane";
    for (BrokerNode* n : c.live()) {
        const auto snap = n->peerLinksForTest();
        EXPECT_TRUE(snap.sealed) << "node " << n->nodeIdForTest();
        EXPECT_EQ(snap.established, 2) << "node " << n->nodeIdForTest();
    }

    // __security events replicate (acks=all, minIsr = majority) and every control-thread view mirrors them.
    GoldenAppender g;
    for (int i = 0; i < 4; ++i) ASSERT_GE(g.appendNext(c, 60s), 1) << "event " << i;
    ASSERT_TRUE(awaitViewsAt(c, 3, 30s));
    for (BrokerNode* n : c.live()) EXPECT_EQ(n->securityViewForTest().digest, g.digestOfFirst(4));

    // Every worker's replication plane verified both peers (PlaneHello rides the sealed plane): sealed frames flow
    // both ways on every plane.
    for (BrokerNode* n : c.live()) {
        for (int32_t peer = 1; peer <= 3; ++peer) {
            if (peer == n->nodeIdForTest()) continue;
            EXPECT_TRUE(until(30s, [&] {
                return n->onWorkerThreadForTest(0, [&](Worker& w) {
                    auto* t = w.replicationTransport();
                    return t != nullptr && t->peerPlaneState(peer) == rune::replication::WorkerReplicationTransport::PeerPlaneState::Verified;
                });
            })) << "node " << n->nodeIdForTest() << " worker 0 plane never verified node " << peer;
        }
    }
}

TEST(PeerSealedClusterRegression, ANodeWithTheWrongKeyNeverJoinsTheSealedCluster) {
    TmpDir keys{"pscl2"};
    const fs::path good = keyFileWith(keys.path, "k.key", 'k'), bad = keyFileWith(keys.path, "x.key", 'x');
    ThreeNodes c = ThreeNodes::startWith("psc_wrongkey", [&](NodeConfig& cfg) { enable(cfg, cfg.nodeId == 3 ? bad : good); });
    // Nodes 1 and 2 hold a majority: the catalog and __security form without node 3 ...
    ASSERT_TRUE(until(60s, [&] {
        for (int id : {1, 2}) {
            const auto pa = catalogAssignmentOf(c.node(id));
            if (!pa || pa->leaderNode == rune::metadata::NO_LEADER || pa->leaderNode == 3) return false;
        }
        return true;
    })) << "the two linked brokers must form the catalog without node 3";
    // ... node 3 is never in a committed ISR, its frames never opened anywhere, and it counted what it could not send.
    std::this_thread::sleep_for(1500ms);
    for (int id : {1, 2}) {
        const auto pa = catalogAssignmentOf(c.node(id));
        ASSERT_TRUE(pa.has_value());
        EXPECT_FALSE(contains(pa->isr, 3)) << "node " << id << ": node 3 must never be in the catalog ISR";
    }
    EXPECT_EQ(c.node(3).peerLinksForTest().established, 0);
    EXPECT_GT(c.node(3).peerLinksForTest().frameDrops, 0u) << "node 3's frames had no link: dropped and counted";
    EXPECT_EQ(c.node(1).peerLinksForTest().established, 1);
    EXPECT_EQ(c.node(2).peerLinksForTest().established, 1);
}

// Issue #241: a same-incarnation re-handshake of a live, loaded link drops no frame on ANY transport.  The responder
// installs the new epoch receive-only and keeps sealing under the old one until the initiator's Confirm; every
// transport keeps its older epochs in slots (no timer), so frames in flight under either epoch open.
TEST(PeerSealedClusterRegression, NoFrameIsDroppedAcrossARehandshakeUnderLoadRegression) {
    TmpDir keys{"pscl3"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_rekey", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(awaitSecurityLeader(c, 60s));
    ASSERT_TRUE(until(30s, [&] {
        for (BrokerNode* n : c.live()) if (n->peerLinksForTest().established != 2) return false;
        return true;
    }));

    // Every transport's UnknownEpoch drops: the control thread's consensus ingress + legacy replication, every
    // worker's plane and the admin plane.
    auto unknownEpochDrops = [&](BrokerNode& n) {
        uint64_t d = n.peerLinksForTest().unknownEpochDrops + n.adminPeerUnknownEpochDropsForTest();
        for (int32_t w = 0; w < n.workerCountForTest(); ++w) d += n.workerPeerUnknownEpochDropsForTest(w);
        return d;
    };
    std::map<int, uint64_t> before;
    for (int id : {1, 2, 3}) before[id] = unknownEpochDrops(c.node(id));

    const std::vector<std::pair<int, int>> pairs = {{1, 2}, {1, 3}, {2, 3}};   // (initiator = lower id, responder)
    GoldenAppender g;
    constexpr int kRounds = 3, kPerRound = 5;
    for (int round = 0; round < kRounds; ++round) {
        std::map<std::pair<int, int>, uint32_t> was;
        for (auto [a, b] : pairs) {
            was[std::make_pair(a, b)] = c.node(a).peerLinksForTest().epochOf[b];
            ASSERT_NE(was[std::make_pair(a, b)], 0u);
            ASSERT_TRUE(c.node(a).forcePeerRekeyForTest(b)) << "round " << round << " " << a << "->" << b;
        }
        // Replicated produce (acks=all __security events) while the three re-handshakes complete.
        for (int i = 0; i < kPerRound; ++i) ASSERT_GE(g.appendNext(c, 60s), 0) << "round " << round << " event " << i;
        // The new epoch became the SEND epoch on both ends of every pair (the responder promoted it).
        for (auto [a, b] : pairs) {
            uint32_t ea = 0, eb = 0;
            EXPECT_TRUE(until(30s, [&] {
                ea = c.node(a).peerLinksForTest().epochOf[b];
                eb = c.node(b).peerLinksForTest().epochOf[a];
                return ea == eb && ea > was[std::make_pair(a, b)];
            })) << "round " << round << " " << a << "<->" << b << ": " << ea << " / " << eb << " (was " << was[std::make_pair(a, b)] << ")";
        }
    }
    const auto n = static_cast<int64_t>(g.appended());
    ASSERT_EQ(n, kRounds * kPerRound);
    ASSERT_TRUE(awaitViewsAt(c, n - 1, 30s));
    for (BrokerNode* node : c.live()) {
        EXPECT_EQ(node->securityViewForTest().appliedOffset, n - 1) << "node " << node->nodeIdForTest() << ": no duplicate";
        EXPECT_EQ(node->securityViewForTest().digest, g.digestOfFirst(static_cast<std::size_t>(n)))
            << "node " << node->nodeIdForTest() << ": produced == consumed";
    }
    for (int id : {1, 2, 3})
        EXPECT_EQ(unknownEpochDrops(c.node(id)) - before[id], 0u) << "node " << id << " dropped frames as UnknownEpoch";
}

// Review of PR #538, adversarial (concurrency: the same operation twice / back to back).  A re-handshake requested while
// one is in flight is refused (no second exchange); then a storm: each initiator re-handshakes again as soon as it holds
// the previous epoch - before its responder promoted it - under acks=all load.  Every pair converges on one send
// epoch, produced == consumed with no duplicate, and the control-thread transports (installed synchronously with the
// handshake) drop nothing as UnknownEpoch.  Mutation: the responder installing SendAndReceive at the Proof drops frames.
TEST(PeerSealedClusterRegression, BackToBackRehandshakesUnderLoadConvergeWithNoLossAdversarialRegression) {
    TmpDir keys{"pscl4"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_storm", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(awaitSecurityLeader(c, 60s));
    ASSERT_TRUE(until(30s, [&] {
        for (BrokerNode* n : c.live()) if (n->peerLinksForTest().established != 2) return false;
        return true;
    }));
    auto controlDrops = [&](BrokerNode& n) { return n.peerLinksForTest().unknownEpochDrops; };
    auto allDrops = [&](BrokerNode& n) {
        uint64_t d = n.peerLinksForTest().unknownEpochDrops + n.adminPeerUnknownEpochDropsForTest();
        for (int32_t w = 0; w < n.workerCountForTest(); ++w) d += n.workerPeerUnknownEpochDropsForTest(w);
        return d;
    };
    std::map<int, uint64_t> ctlBefore, allBefore;
    for (int id : {1, 2, 3}) { ctlBefore[id] = controlDrops(c.node(id)); allBefore[id] = allDrops(c.node(id)); }

    const std::vector<std::pair<int, int>> pairs = {{1, 2}, {1, 3}, {2, 3}};
    GoldenAppender g;
    constexpr int kRounds = 6;
    for (int round = 0; round < kRounds; ++round) {
        std::map<std::pair<int, int>, uint32_t> was;
        for (auto [a, b] : pairs) {
            was[{a, b}] = c.node(a).peerLinksForTest().epochOf[b];
            ASSERT_TRUE(c.node(a).forcePeerRekeyForTest(b)) << "round " << round << " " << a << "->" << b;
            EXPECT_FALSE(c.node(a).forcePeerRekeyForTest(b)) << "a second re-handshake while one is in flight is refused";
            EXPECT_FALSE(c.node(b).forcePeerRekeyForTest(a)) << "the responder never starts one";
        }
        ASSERT_GE(g.appendNext(c, 60s), 0) << "round " << round;
        // Only the INITIATOR's new epoch is awaited: the next round starts before the responder necessarily promoted.
        for (auto [a, b] : pairs)
            ASSERT_TRUE(until(30s, [&] { return c.node(a).peerLinksForTest().epochOf[b] > was[{a, b}]; }))
                << "round " << round << " " << a << "->" << b;
    }
    for (auto [a, b] : pairs) {
        uint32_t ea = 0, eb = 0;
        EXPECT_TRUE(until(30s, [&] {
            ea = c.node(a).peerLinksForTest().epochOf[b];
            eb = c.node(b).peerLinksForTest().epochOf[a];
            return ea == eb && ea != 0;
        })) << a << "<->" << b << ": " << ea << " / " << eb;
    }
    for (int i = 0; i < 3; ++i) ASSERT_GE(g.appendNext(c, 60s), 0) << "after the storm, event " << i;
    const auto n = static_cast<int64_t>(g.appended());
    ASSERT_EQ(n, kRounds + 3);
    ASSERT_TRUE(awaitViewsAt(c, n - 1, 30s));
    for (BrokerNode* node : c.live()) {
        EXPECT_EQ(node->securityViewForTest().appliedOffset, n - 1) << "node " << node->nodeIdForTest() << ": no duplicate";
        EXPECT_EQ(node->securityViewForTest().digest, g.digestOfFirst(static_cast<std::size_t>(n)))
            << "node " << node->nodeIdForTest() << ": produced == consumed";
    }
    for (int id : {1, 2, 3}) {
        EXPECT_EQ(controlDrops(c.node(id)) - ctlBefore[id], 0u) << "node " << id << ": control-thread UnknownEpoch drops";
        // Workers / admin plane get their installs over rings (a known limit of #241): reported, not asserted.
        std::printf("[storm] node %d: UnknownEpoch drops on every plane during the storm: %llu\n", id,
                    static_cast<unsigned long long>(allDrops(c.node(id)) - allBefore[id]));
    }
}

// ---- issue #17: the sealed idle keepalive.  Consensus only carries traffic between the leader and each follower, so ----
// before #17 two followers exchanged no sealed control-plane frame and looked "silent" (a forged hint could rekey them,
// which is why a 30 s hint-rekey cap existed).  Every ordered pair now keeps opening sealed consensus frames while idle.
namespace {
bool allLinked(ThreeNodes& c) {
    for (BrokerNode* n : c.live()) if (n->peerLinksForTest().established != 2) return false;
    return true;
}
} // namespace

TEST(PeerSealedClusterRegression, AnIdleFollowerPairStaysAuthenticatedRegression) {
    TmpDir keys{"pscl5"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_idle", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(until(30s, [&] { return allLinked(c); }));
    std::this_thread::sleep_for(500ms);   // past the election: the cluster is idle from here
    std::map<int, BrokerNode::PeerLinkSnap> first;
    for (int id : {1, 2, 3}) first[id] = c.node(id).peerLinksForTest();
    std::map<int, BrokerNode::PeerLinkSnap> prev = first;
    // Sampled every 750 ms (< kSilenceNs) for 3 s: each node opened at least one sealed consensus frame from EACH peer
    // in every window - the follower <-> follower pair included.
    for (int window = 1; window <= 4; ++window) {
        std::this_thread::sleep_for(750ms);
        for (int id : {1, 2, 3}) {
            const auto now = c.node(id).peerLinksForTest();
            for (int peer : {1, 2, 3}) {
                if (peer == id) continue;
                EXPECT_GT(now.consensusAuthFramesOf[peer], prev[id].consensusAuthFramesOf[peer])
                    << "window " << window << ": node " << id << " opened no sealed consensus frame from node " << peer;
            }
            prev[id] = now;
        }
    }
    for (int id : {1, 2, 3})
        for (int peer : {1, 2, 3})
            if (peer != id) EXPECT_EQ(prev[id].epochOf[peer], first[id].epochOf[peer]) << id << "->" << peer << ": no re-handshake";
}

// A responder restarted right after a completed re-handshake relinks within seconds (before #17: the initiator ignored
// its NoLink poke for 30 s after that re-handshake).
TEST(PeerSealedClusterRegression, ARestartRightAfterAReHandshakeRelinksFastRegression) {
    TmpDir keys{"pscl6"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_restart", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(awaitSecurityLeader(c, 60s));
    ASSERT_TRUE(until(30s, [&] { return allLinked(c); }));
    // Node 1 initiates both its links; the responder restarted is a node that leads nothing.
    const int32_t leader = c.settledLeader();
    const int b = leader == 2 ? 3 : 2;
    const uint32_t was = c.node(1).peerLinksForTest().epochOf[b];
    ASSERT_TRUE(c.node(1).forcePeerRekeyForTest(b));
    ASSERT_TRUE(until(10s, [&] {
        return c.node(1).peerLinksForTest().epochOf[b] > was && c.node(b).peerLinksForTest().epochOf[1] == c.node(1).peerLinksForTest().epochOf[b];
    })) << "the re-handshake completes";
    c.stop(b);
    c.startNode(b);
    const auto restarted = std::chrono::steady_clock::now();
    EXPECT_TRUE(until(5s, [&] { return allLinked(c); })) << "every link is re-established within 5 s of the restart";
    const auto linkedAfter = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - restarted);
    std::printf("[restart] relinked %lld ms after the restart\n", static_cast<long long>(linkedAfter.count()));
    GoldenAppender g;
    EXPECT_GE(g.appendNext(c, 5s), 0) << "an acks=all __security append after the relink";
}

// ---- Review of PR #551 (issues #17 / #18), adversarial, END TO END: an off-path attacker on the network (its own Aeron ----
// driver, no cluster key) sprays the three idle brokers' handshake endpoints for 4 s: a forged NoLink poke naming each
// responder at each initiator every 20 ms (before #17 only the 30 s cap held these off; now only the sealed keepalives
// do - the follower pair carries no consensus traffic), and forged HelloV2s under the REAL key id with random nonces /
// shares at each responder (before #18 they evicted the real initiator's pending slot).  No link may re-handshake under
// the pokes, and a real re-handshake of every pair forced in the middle of the HelloV2 flood must complete.
// Mutation: sendIdleKeepalives returning 0 turns the first half red (the idle follower pair is re-keyed by a poke).
namespace {
class AttackerDriver {
public:
    AttackerDriver() : driver_(makeCfg(dir_.path / "aeron"), DriverMode::Threaded) {}
    [[nodiscard]] const std::string& aeronDir() const { return driver_.dir(); }
private:
    static EmbeddedDriverConfig makeCfg(const fs::path& d) {
        EmbeddedDriverConfig c;
        c.dir = d.string(); c.termBufferBytes = 64 * 1024; c.termBufferSparse = true; c.udpGso = false; c.epollPoll = false;
        return c;
    }
    TmpDir         dir_{"pscl_attacker"};
    EmbeddedDriver driver_;
};
} // namespace

TEST(PeerSealedClusterRegression, ForgedNoLinkPokesAndHelloV2FloodsAgainstAnIdleClusterAdversarialRegression) {
    TmpDir keys{"pscl7"};
    const fs::path keyFile = keyFileWith(keys.path, "k.key", 'k');
    ThreeNodes c = ThreeNodes::startWith("psc_attack", [&](NodeConfig& cfg) { enable(cfg, keyFile); });
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(awaitSecurityLeader(c, 60s));
    ASSERT_TRUE(until(30s, [&] {
        for (BrokerNode* n : c.live()) if (n->peerLinksForTest().established != 2) return false;
        return true;
    }));
    const auto clusterKeys = rune::peerlink::loadClusterKeys(keyFile.string(), "");
    ASSERT_TRUE(clusterKeys.has_value());
    const uint64_t keyId = clusterKeys->current.keyId;

    AttackerDriver drv;
    aeron::Context ctx; ctx.aeronDir(drv.aeronDir());
    auto aeron = aeron::Aeron::connect(ctx);
    const NodeConfig cfg1 = makeConfig(1, c.dirs[0]->path, c.ports, 3, Mode::Shard);
    std::map<int, std::shared_ptr<aeron::Publication>> to;   // the attacker's publication to each node's handshake stream
    for (const auto& peer : cfg1.peers) {
        const int64_t reg = aeron->addPublication(peer.consensusChannel(), cfg1.peerAuthStreamId);
        std::shared_ptr<aeron::Publication> pub;
        ASSERT_TRUE(until(10s, [&] { pub = aeron->findPublication(reg); return pub != nullptr; }));
        to[peer.id] = pub;
    }
    ASSERT_TRUE(until(10s, [&] { for (auto& [id, p] : to) if (!p->isConnected()) return false; return true; }));

    std::mt19937_64 rng(551);
    uint64_t offered = 0;
    auto offer = [&](int node, const uint8_t* b, std::size_t n) {
        aeron::concurrent::AtomicBuffer buf(const_cast<uint8_t*>(b), static_cast<aeron::util::index_t>(n));
        if (to[node]->offer(buf, 0, static_cast<aeron::util::index_t>(n)) > 0) ++offered;
    };
    const std::vector<std::pair<int, int>> pairs = {{1, 2}, {1, 3}, {2, 3}};   // (initiator = lower id, responder)
    auto pokeAll = [&] {
        for (auto [ini, resp] : pairs) {
            rune::peerlink::PeerRefuse r; r.from = resp; r.to = ini; r.reason = rune::peerlink::RefuseReason::NoLink;
            uint8_t b[rune::peerlink::PeerRefuse::kSize]; r.encode(b); offer(ini, b, sizeof b);
        }
    };
    auto floodHellos = [&](int perResponder) {
        for (auto [ini, resp] : pairs) {
            for (int i = 0; i < perResponder; ++i) {
                rune::peerlink::PeerHelloV2 h; h.from = ini; h.to = resp; h.keyId = keyId; h.incarnation = rng();
                for (auto& x : h.nonceA) x = static_cast<uint8_t>(rng());
                for (auto& x : h.shareA) x = static_cast<uint8_t>(rng());
                uint8_t b[rune::peerlink::PeerHelloV2::kSize]; h.encode(b); offer(resp, b, sizeof b);
            }
        }
    };
    std::map<std::pair<int, int>, uint32_t> epochBefore;
    for (auto [ini, resp] : pairs) epochBefore[{ini, resp}] = c.node(ini).peerLinksForTest().epochOf[resp];
    auto before = [&](int ini, int resp) { return epochBefore[std::make_pair(ini, resp)]; };

    // The attacker really reaches each broker's handshake agent: a HelloV2 under a key id nobody holds is refused (counted).
    std::map<int, uint64_t> refusals0;
    for (int id : {1, 2, 3}) refusals0[id] = c.node(id).peerLinksForTest().refusals;
    for (auto [ini, resp] : pairs) {
        rune::peerlink::PeerHelloV2 h; h.from = ini; h.to = resp; h.keyId = keyId ^ 0xFFFF; h.incarnation = 1;
        uint8_t b[rune::peerlink::PeerHelloV2::kSize]; h.encode(b); offer(resp, b, sizeof b);
    }
    EXPECT_TRUE(until(5s, [&] { return c.node(2).peerLinksForTest().refusals > refusals0[2] &&
                                       c.node(3).peerLinksForTest().refusals > refusals0[3]; }))
        << "the attacker's frames never reached the responders' handshake agents";

    // Phase 1: 4 s of forged pokes (every 20 ms) at an otherwise idle cluster.
    const auto pokeEnd = std::chrono::steady_clock::now() + 4s;
    while (std::chrono::steady_clock::now() < pokeEnd) { pokeAll(); std::this_thread::sleep_for(20ms); }
    for (auto [ini, resp] : pairs) {
        EXPECT_EQ(c.node(ini).peerLinksForTest().epochOf[resp], before(ini, resp))
            << ini << "->" << resp << ": a forged NoLink poke re-keyed a live idle link";
        EXPECT_EQ(c.node(resp).peerLinksForTest().epochOf[ini], before(ini, resp)) << resp << "->" << ini;
    }

    // Phase 2: a HelloV2 flood (8 per responder per 2 ms - the old pending table's size) while every pair re-handshakes.
    for (auto [ini, resp] : pairs) ASSERT_TRUE(c.node(ini).forcePeerRekeyForTest(resp)) << ini << "->" << resp;
    const auto floodEnd = std::chrono::steady_clock::now() + 3s;
    bool allDone = false;
    while (std::chrono::steady_clock::now() < floodEnd) {
        floodHellos(8);
        std::this_thread::sleep_for(2ms);
        if (!allDone) {
            allDone = true;
            for (auto [ini, resp] : pairs) {
                const uint32_t ei = c.node(ini).peerLinksForTest().epochOf[resp], er = c.node(resp).peerLinksForTest().epochOf[ini];
                if (ei <= before(ini, resp) || ei != er) allDone = false;
            }
        }
    }
    EXPECT_TRUE(allDone) << "a real re-handshake of every pair completes under the HelloV2 flood";
    for (auto [ini, resp] : pairs) {
        const uint32_t ei = c.node(ini).peerLinksForTest().epochOf[resp], er = c.node(resp).peerLinksForTest().epochOf[ini];
        EXPECT_EQ(ei, er) << ini << "<->" << resp;
        EXPECT_GT(ei, before(ini, resp)) << ini << "->" << resp;
    }
    EXPECT_GT(offered, 3'000u) << "the attack really went out";
    GoldenAppender g;
    EXPECT_GE(g.appendNext(c, 30s), 0) << "an acks=all __security append after the attack";
}
