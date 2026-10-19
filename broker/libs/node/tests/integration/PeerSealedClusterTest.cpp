// PeerSealedClusterTest.cpp — authentication plan Task 16 (D34), END TO END: three in-process BrokerNodes with
// authMode Enabled over real Aeron UDP.  Their consensus, controller / replication (legacy transport), every worker's
// replication plane and the admin plane (`__catalog`, `__security`) all run sealed, keyed by the links the PeerLinkAgent
// establishes - and the cluster works exactly as an unsealed one.
#include "support/SecurityBrokerCluster.h"

#include <rune/node/RoleCounter.h>

#include <gtest/gtest.h>

#include <fstream>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::security_cluster;
using namespace std::chrono_literals;

namespace {

fs::path keyFileWith(const fs::path& dir, const std::string& name, char c) {
    const fs::path p = dir / name;
    std::ofstream(p, std::ios::binary) << std::string(48, c) << "\n";
    return p;
}

void enable(NodeConfig& cfg, const fs::path& keyFile) {
    cfg.authMode = AuthMode::Enabled;
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
