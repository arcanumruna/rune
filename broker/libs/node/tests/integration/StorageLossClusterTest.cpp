// StorageLossClusterTest.cpp - ISR design v3 Step 10: a broker whose disk was wiped and which restarts at once must
// never lead its old leaderships (the bridge's storage gate holds them and asks for a failover) and must never make the
// others truncate the committed catalog.  Three in-process BrokerNodes over real Aeron (the catalog cluster rig).
#include "support/CatalogBrokerCluster.h"

#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <fstream>
#include <optional>
#include <string>
#include <thread>

using namespace rune::node;
using namespace rune::node::test::catalog_cluster;
using namespace std::chrono_literals;
namespace rr = rune::replication;

namespace {

/// storage.meta's `storageId=` of node `id` (hex -> int64); 0 when absent.
int64_t storageIdOnDisk(ThreeNodes& c, int id) {
    std::ifstream in(c.dirs[static_cast<std::size_t>(id - 1)]->path / "storage.meta");
    std::string line;
    while (std::getline(in, line))
        if (line.rfind("storageId=", 0) == 0) return static_cast<int64_t>(std::stoull(line.substr(10), nullptr, 16));
    return 0;
}

}  // namespace

// The wiped catalog leader restarts at once while the controller's decisions are held: nothing moves the leadership off
// it, so only the bridge's gate keeps it from leading the (empty) catalog and truncating the followers.  Then the hold
// is released: the controller strips the node, fails the leadership over, commits the new storage and re-admits it.
// RED: remove the bridge gate (step 4 fails: the wiped node leads, or the others' catalog shrinks).
TEST(StorageLossClusterRegression, WipedCatalogLeaderRestartedAtOnceNeverLeadsNorTruncatesRegression) {
    ThreeNodes c = ThreeNodes::startWith("lossA", [](NodeConfig& k) { k.testHoldAllControllerDecisionsMs = 120'000; });
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (int i = 0; i < 5; ++i) ASSERT_GE(c.appendEventOnCatalogLeaderForTest(), 0) << "DDL event " << i;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s));
    const int32_t L = c.settledLeader();
    ASSERT_GT(L, 0);
    int64_t hw0[4] = {0, 0, 0, 0};
    for (int id : c.runningNodes()) hw0[id] = catalogOf(c.node(id)).hw;

    for (int id : c.runningNodes()) c.node(id).setControllerDecisionHoldForTest(true);
    c.stop(L);
    c.wipe(L);
    c.startNode(L);
    c.node(L).setControllerDecisionHoldForTest(true);

    const auto until5s = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < until5s) {
        for (int id : c.runningNodes()) {
            const auto v = catalogOf(c.node(id));
            if (id == L) {
                EXPECT_NE(v.role, rr::Role::Leader) << "the wiped node " << L << " led the catalog";
            } else {
                EXPECT_GE(v.leo, hw0[id]) << "node " << id << " truncated the committed catalog (leo " << v.leo << " < " << hw0[id] << ")";
                EXPECT_GE(v.hw, hw0[id]) << "node " << id << "'s catalog HW went backwards";
            }
        }
        if (::testing::Test::HasFailure()) break;
        std::this_thread::sleep_for(20ms);
    }

    for (int id : c.runningNodes()) c.node(id).setControllerDecisionHoldForTest(false);
    int32_t leader = -1;
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(L, 60s)) << "the leadership never moved off the wiped node";
    leader = c.settledLeader();
    ASSERT_GT(leader, 0);
    const int64_t id = storageIdOnDisk(c, L);
    ASSERT_NE(id, 0);
    bool confirmed = false, inIsr = false;
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (std::chrono::steady_clock::now() < deadline && !(confirmed && inIsr)) {
        leader = c.settledLeader();
        if (leader > 0) {
            confirmed = c.node(leader).onControlThreadForTest([&] {
                return c.node(leader).metadataServiceForTest()->msmForTest().nodeStorage(L);
            }) == id;
            const auto pa = catalogAssignmentOf(c.node(leader));
            inIsr = pa && contains(pa->isr, L);
        }
        if (!(confirmed && inIsr)) std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(confirmed) << "the controller never committed the wiped node's storage";
    EXPECT_TRUE(inIsr) << "the wiped node never rejoined the catalog ISR";
    std::string why;
    EXPECT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
}

// At feature level 1 (no storage tracking in the controller) a held leader role still fails over BY BUMP: the replica asks
// for a new epoch (the restart fence) and the controller moves the leadership to a live ISR member.  The catalog
// partition follows the controller on its own (the controller takes a coordinator partition it is in the ISR of), so the
// bump is proven on a USER partition the wiped node led.
// RED: a plain becomeFollower (no bump) - the user partition stays led by the wiped node (alive, so no other failover).
TEST(StorageLossClusterRegression, WipedCatalogLeaderAtFeatureLevelOneFailsOverByBumpRegression) {
    ThreeNodes c = ThreeNodes::startWith("lossB", [](NodeConfig& k) { k.testSupportedFeatureLevel = 0; });
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (int i = 0; i < 3; ++i) ASSERT_GE(c.appendEventOnCatalogLeaderForTest(), 0) << "DDL event " << i;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s));
    const int32_t L = c.settledLeader();
    ASSERT_GT(L, 0);
    // A user partition the soon-wiped node leads (the golden script's topics are placed by the controller).
    const auto ledBy = [&](int node, int32_t who) -> std::optional<std::pair<int32_t, int32_t>> {
        return c.node(node).onControlThreadForTest([&]() -> std::optional<std::pair<int32_t, int32_t>> {
            std::optional<std::pair<int32_t, int32_t>> k;
            c.node(node).metadataServiceForTest()->msm().committedMap().forEach(
                [&](int32_t t, int32_t p, const rune::metadata::PartitionAssignment& pa) {
                    if (!k && t >= 0 && pa.leaderNode == who) k = std::make_pair(t, p);
                });
            return k;
        });
    };
    const int other = L == 1 ? 2 : 1;
    std::optional<std::pair<int32_t, int32_t>> key;
    const auto found = std::chrono::steady_clock::now() + 30s;
    while (!key && std::chrono::steady_clock::now() < found) {
        key = ledBy(other, L);
        if (!key) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(key.has_value()) << "no user partition led by node " << L << " (the placement never arrived)";
    c.stop(L);
    c.wipe(L);
    c.startNode(L);
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(L, 10s)) << "the catalog leadership never moved off the wiped node";
    bool moved = false;
    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (!moved && std::chrono::steady_clock::now() < deadline) {
        moved = c.node(other).onControlThreadForTest([&]() -> bool {
            const auto* pa = c.node(other).metadataServiceForTest()->msm().assignment(key->first, key->second);
            return pa != nullptr && pa->leaderNode != L;
        });
        if (!moved) std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(moved) << "the user partition (" << key->first << ", " << key->second << ") stayed led by the wiped node";
    EXPECT_GE(c.appendEventOnCatalogLeaderForTest(), 0) << "no DDL succeeded after the failover";
}
