// CatalogReplicationTest.cpp — catalog history Task 6: the `__catalog`
// partition (topic -6, partition 0) on every broker, replicated for real
// (acks=all, minIsr = majority) on its own admin-thread replication plane, its
// leader always elected from its ISR by the controller (unclean election
// disabled).  In-process BrokerNodes over REAL Aeron UDP (shard mode — the
// default — and legacy workers), plus the admin/control relay units that
// carry its role and ISR.
//
// E2 lessons pinned here: a leader is never unclean; a follower whose log is
// reset (new broker / wiped dir) refetches from offset 0 and rejoins the ISR; a
// controller answer reaches the admin-owned replica with its leaderId (shard
// mode: leaderId stayed -1 for worker-owned replicas, and their acks never
// matched); a relay ring that is full re-delivers, never drops.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/AdminIsrChannel.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace rr = rune::replication;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const std::string& tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_cat_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;

struct Ports {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static Ports discover(int nodes) {
        Ports p;
        auto blocks = freeUdpPortBlocks(3 * nodes, 1);
        if (blocks.size() != static_cast<std::size_t>(3 * nodes)) throw std::runtime_error("Ports: no free blocks");
        std::vector<std::pair<int, int>> taken;
        for (uint16_t b : blocks) taken.push_back({b, b + 1});
        auto single = [&taken] {
            for (int attempt = 0; attempt < 2000; ++attempt) {
                const int q = freeUdpPort();
                bool clash = q == 0;
                for (const auto& [lo, hi] : taken) clash = clash || (q >= lo && q < hi);
                if (!clash) { taken.push_back({q, q + 1}); return static_cast<uint16_t>(q); }
            }
            throw std::runtime_error("Ports: no collision-free UDP port");
        };
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(single()); p.client.push_back(single()); p.repl.push_back(single());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

/// The worker / runtime layouts the catalog plane must work under (it rides the
/// node client, which in fused placement is worker 0's invoker client).
enum class Mode { Shard, ShardFused, Legacy };

/// Node ids are 1..nodes (index = id - 1).
NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int nodes, Mode mode) {
    const bool shard = mode != Mode::Legacy;
    const int idx = nodeId - 1;
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    // catalogReplStreamId: the default (30) — distinct from both.
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = shard ? NodeConfig::WorkerMode::Shard : NodeConfig::WorkerMode::Legacy;
    cfg.runtimePlacement = mode == Mode::ShardFused ? NodeConfig::RuntimePlacement::Fused
                                                    : NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = shard;
    cfg.workerIngressBasePort = p.ib[idx];
    cfg.workerReplBasePort    = p.rb[idx];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[idx];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i + 1; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[idx]);
    cfg.clientEndpoint.streamId = kClientStream;
    return cfg;
}

std::optional<rune::metadata::PartitionAssignment> catalogAssignmentOf(BrokerNode& n) {
    return n.onControlThreadForTest([&n]() -> std::optional<rune::metadata::PartitionAssignment> {
        const MetadataService* ms = n.metadataServiceForTest();
        if (ms == nullptr) return std::nullopt;
        const auto* pa = ms->assignment(kCatalogTopicId, kCatalogPartition);   // F26: the real API
        return pa ? std::optional(*pa) : std::nullopt;
    });
}

struct CatalogView {
    bool         present = false;
    rr::Role     role    = rr::Role::Follower;
    rr::Offset   leo     = -1;
    rr::Offset   hw      = -1;
    rr::Offset   logStart = -1;
};

CatalogView catalogOf(BrokerNode& n) {
    return n.onAdminThreadForTest([&n]() -> CatalogView {
        CatalogView v;
        rr::PartitionReplica* r = n.catalogReplicaForTest();
        if (r == nullptr) return v;
        v.present  = true;
        v.role     = r->role();
        v.leo      = r->logEndOffset();
        v.hw       = r->highWatermark();
        v.logStart = r->log()->logStartOffset();
        return v;
    });
}

bool contains(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }

/// A test-local 3-broker cluster (ids 1..3).  Polls are bounded (the deadline
/// is the failure, never a hang); the nodes' own threads do the work.
struct ThreeNodes {
    std::string tag;
    Mode        mode = Mode::Shard;
    Ports       ports;
    std::vector<std::unique_ptr<TmpDir>>     dirs;
    std::vector<std::unique_ptr<BrokerNode>> nodes;

    ThreeNodes(std::string t, Mode m) : tag(std::move(t)), mode(m), ports(Ports::discover(3)) {
        for (int i = 0; i < 3; ++i) dirs.push_back(std::make_unique<TmpDir>(tag + "_n" + std::to_string(i + 1)));
        nodes.resize(3);
    }
    ThreeNodes(ThreeNodes&&) = default;
    ~ThreeNodes() {
        for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
            if (*it) (*it)->stop();
    }

    static ThreeNodes start(const std::string& tag, std::vector<int> startOnly = {1, 2, 3}, Mode mode = Mode::Shard) {
        ThreeNodes c(tag, mode);
        for (int id : startOnly) c.startNode(id);
        return c;
    }

    void startNode(int id) {
        nodes[static_cast<std::size_t>(id - 1)] =
            std::make_unique<BrokerNode>(makeConfig(id, dirs[static_cast<std::size_t>(id - 1)]->path, ports, 3, mode));
        nodes[static_cast<std::size_t>(id - 1)]->start();
    }
    void stop(int id) {
        auto& n = nodes[static_cast<std::size_t>(id - 1)];
        if (n) { n->stop(); n.reset(); }
    }
    void wipe(int id) {
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(dirs[static_cast<std::size_t>(id - 1)]->path, ec))
            fs::remove_all(e.path(), ec);
    }
    BrokerNode& node(int id) { return *nodes[static_cast<std::size_t>(id - 1)]; }
    [[nodiscard]] bool running(int id) const { return nodes[static_cast<std::size_t>(id - 1)] != nullptr; }
    std::vector<BrokerNode*> live() {
        std::vector<BrokerNode*> v;
        for (auto& n : nodes) if (n) v.push_back(n.get());
        return v;
    }

    /// The committed leader every running broker agrees on, iff it runs, is in
    /// the committed ISR, and its admin-thread replica is Leader; else -1.
    int32_t settledLeader() {
        int32_t leader = rune::metadata::NO_LEADER;
        for (BrokerNode* n : live()) {
            const auto pa = catalogAssignmentOf(*n);
            if (!pa || pa->leaderNode == rune::metadata::NO_LEADER) return -1;
            if (leader == rune::metadata::NO_LEADER) leader = pa->leaderNode;
            if (pa->leaderNode != leader || !contains(pa->isr, leader)) return -1;
        }
        if (leader < 1 || leader > 3 || !running(leader)) return -1;
        return catalogOf(node(leader)).role == rr::Role::Leader ? leader : -1;
    }
    bool awaitCatalogLeader(std::chrono::milliseconds within, int32_t notThis = -1) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            const int32_t l = settledLeader();
            if (l > 0 && l != notThis) return true;
            std::this_thread::sleep_for(20ms);
        }
        return false;
    }
    bool awaitCatalogLeaderOtherThan(int32_t old, std::chrono::milliseconds within) {
        return awaitCatalogLeader(within, old);
    }
    /// From the first running broker's committed map.
    int32_t catalogLeaderId() {
        for (BrokerNode* n : live())
            if (const auto pa = catalogAssignmentOf(*n)) return pa->leaderNode;
        return -1;
    }

    /// Append `bytes` as one record on the catalog leader's admin thread
    /// (acks=all), then wait for its HW to cover it.  Returns that HW (> the
    /// record's offset), or -1 if it never committed.
    int64_t appendRawOnCatalogLeaderForTest(std::vector<uint8_t> bytes,
                                            std::chrono::milliseconds within = 30s) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        int64_t leo = -1;
        int32_t leader = -1;
        while (leo < 0 && std::chrono::steady_clock::now() < deadline) {
            leader = settledLeader();
            if (leader > 0) {
                BrokerNode& n = node(leader);
                leo = n.onAdminThreadForTest([&n, &bytes]() -> int64_t {
                    rr::PartitionReplica* r = n.catalogReplicaForTest();
                    if (r == nullptr || r->role() != rr::Role::Leader) return -1;
                    const rr::Offset base = r->append(std::span<const uint8_t>(bytes), /*acksAll=*/true);
                    return base < 0 ? -1 : r->logEndOffset();   // < 0: min-ISR gate (ISR forming)
                });
            }
            if (leo < 0) std::this_thread::sleep_for(20ms);
        }
        if (leo < 0) return -1;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto v = catalogOf(node(leader));
            if (v.hw >= leo) return v.hw;
            std::this_thread::sleep_for(10ms);
        }
        return -1;
    }
    bool awaitCatalogEndOffset(BrokerNode& n, int64_t want, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            if (catalogOf(n).leo >= want) return true;
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }
    bool awaitCatalogHighWatermark(BrokerNode& n, int64_t want, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            if (catalogOf(n).hw >= want) return true;
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }
    int64_t catalogHighWatermark(BrokerNode& n) { return catalogOf(n).hw; }
    /// `id` is in the COMMITTED catalog ISR (the controller accepted the
    /// leader's AlterIsr) on every running broker.
    bool awaitInCatalogIsr(int32_t id, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            bool all = true;
            for (BrokerNode* n : live()) {
                const auto pa = catalogAssignmentOf(*n);
                all = all && pa && contains(pa->isr, id);
            }
            if (all) return true;
            std::this_thread::sleep_for(20ms);
        }
        return false;
    }
};

void expectFormation(ThreeNodes& c) {
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    for (BrokerNode* n : c.live()) {
        const auto pa = catalogAssignmentOf(*n);
        ASSERT_TRUE(pa.has_value()) << "node " << n->nodeIdForTest();
        EXPECT_EQ(pa->leaderNode, leader);
        EXPECT_EQ(pa->replicas.size(), 3u) << "replicated to every broker";
        EXPECT_TRUE(contains(pa->isr, leader));
        EXPECT_TRUE(pa->cleanElection);
        const auto v = catalogOf(*n);
        EXPECT_TRUE(v.present) << "every broker builds the catalog";
        EXPECT_EQ(v.role == rr::Role::Leader, n->nodeIdForTest() == leader)
            << "exactly the committed leader leads (node " << n->nodeIdForTest() << ")";
    }
}

void expectReplication(ThreeNodes& c) {
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({1, 2, 3});   // admin-thread task (onAdminThreadForTest)
    ASSERT_GE(hw, 1);
    for (BrokerNode* n : c.live())
        EXPECT_TRUE(c.awaitCatalogEndOffset(*n, hw, 10s)) << "node " << n->nodeIdForTest();
    // Every broker's catalog is a disk log under dataDir/__catalog.
    for (int id = 1; id <= 3; ++id)
        EXPECT_TRUE(fs::is_directory(c.dirs[static_cast<std::size_t>(id - 1)]->path / "__catalog")) << id;
}

} // namespace

// ===========================================================================
// 3 brokers over real Aeron
// ===========================================================================

// Catalog history §3/§5.6: __catalog exists on every broker from cluster formation,
// is replicated for real (acks=all, minIsr = majority), and its leader is always an
// ISR member (unclean election disabled).
TEST(CatalogReplicationRegression, FormationCreatesTheCatalogOnEveryBrokerWithACleanLeader) {
    ThreeNodes c = ThreeNodes::start("cat_form");
    expectFormation(c);
}

TEST(CatalogReplicationRegression, FormationCreatesTheCatalogOnEveryBrokerWithLegacyWorkers) {
    ThreeNodes c = ThreeNodes::start("cat_form_l", {1, 2, 3}, Mode::Legacy);
    expectFormation(c);
}

// The production default: shard workers + the runtime FUSED on worker 0 (the
// node client is worker 0's invoker client; the admin thread polls the catalog
// plane on it).
TEST(CatalogReplicationRegression, FormationAndReplicationInTheFusedDefaultPlacement) {
    ThreeNodes c = ThreeNodes::start("cat_fused", {1, 2, 3}, Mode::ShardFused);
    expectFormation(c);
    expectReplication(c);
}

// (Fix round 1 M7: named for what it shows — a committed append reaches every
// broker's log.  The HW-only-after-a-majority ordering is pinned at replica
// level by CatalogMinIsrRegression below.)
TEST(CatalogReplicationRegression, CommittedAppendsReachEveryBroker) {
    ThreeNodes c = ThreeNodes::start("cat_repl");
    expectReplication(c);
}

TEST(CatalogReplicationRegression, CommittedAppendsReachEveryBrokerWithLegacyWorkers) {
    ThreeNodes c = ThreeNodes::start("cat_repl_l", {1, 2, 3}, Mode::Legacy);
    expectReplication(c);
}

// With both followers unreachable the leader may still take an append into its
// log, but the HW never covers it: the followers stay in its (maximal) ISR and
// hold the HW (fix round 1 I1: renamed for what it shows — the min-ISR gate
// itself is pinned by CatalogMinIsrRegression).
TEST(CatalogReplicationRegression, NoHighWatermarkProgressWhileBothFollowersAreUnreachable) {
    ThreeNodes c = ThreeNodes::start("cat_major");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw0 = c.appendRawOnCatalogLeaderForTest({9});
    ASSERT_GE(hw0, 1);
    const int32_t leader = c.catalogLeaderId();
    for (int id = 1; id <= 3; ++id) if (id != leader) c.stop(id);
    BrokerNode& l = c.node(leader);
    const int64_t leo = l.onAdminThreadForTest([&l]() -> int64_t {
        rr::PartitionReplica* r = l.catalogReplicaForTest();
        const std::vector<uint8_t> rec{8};
        const rr::Offset base = r->append(std::span<const uint8_t>(rec), /*acksAll=*/true);
        return base < 0 ? base : r->logEndOffset();
    });
    const auto until = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < until) {
        EXPECT_LE(c.catalogHighWatermark(l), hw0) << "the HW moved without a majority (leo=" << leo << ")";
        std::this_thread::sleep_for(100ms);
    }
}

TEST(CatalogReplicationRegression, LeaderKillElectsAnIsrMemberAndKeepsEveryCommittedRecord) {
    ThreeNodes c = ThreeNodes::start("cat_kill");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({7});
    ASSERT_GE(hw, 1);
    const int32_t old = c.catalogLeaderId();
    c.stop(old);
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(old, 30s));
    auto& nl = c.node(c.catalogLeaderId());
    // Never unclean: the new leader's LOG already holds the committed prefix the
    // moment it leads (it was elected from the ISR) ...
    EXPECT_GE(catalogOf(nl).leo, hw) << "the new leader lacks committed records — an unclean election";
    // ... and its HW covers it once its ISR re-forms.
    ASSERT_TRUE(c.awaitCatalogHighWatermark(nl, hw, 20s));
    EXPECT_GE(c.catalogHighWatermark(nl), hw) << "never unclean: the new leader holds the committed prefix";
    // And it keeps taking catalog writes.
    EXPECT_GT(c.appendRawOnCatalogLeaderForTest({8}), hw);
}

TEST(CatalogReplicationRegression, CatalogIsNeverCompactedOrRetentionDeleted) {
    const auto cfg = rune::node::makeCatalogPartitionConfig(rune::node::NodeConfig{}, 3);
    EXPECT_EQ(cfg.cleanupPolicy, rune::replication::CleanupPolicy::Delete);
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.retentionBytes, -1);
    EXPECT_EQ(cfg.logStartOffset, 0);
    EXPECT_EQ(cfg.minIsr, 2);
    EXPECT_EQ(rune::node::catalogMinIsr(5), 3);
}

TEST(CatalogReplicationRegression, ANewBrokerWithAnEmptyDataDirFetchesTheCatalogFromOffsetZero) {
    ThreeNodes c = ThreeNodes::start("cat_join", /*startOnly=*/{1, 2});
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    // Fix round 1 (M6): the bootstrap assignment seeds the ISR with EVERY
    // configured broker (MetadataService::decideCoordinatorPartition), including
    // broker 3, which never started.  HW = min over the ISR, so the catalog
    // commits nothing until the controller fences it / the leader shrinks it
    // (≈ the fencing timeout, at most replicaLagMaxMs) — a bounded formation
    // stall, never a commit without a majority.
    const auto t0 = std::chrono::steady_clock::now();
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({4, 5});
    ASSERT_GE(hw, 1);
    const auto pa = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(pa.has_value());
    EXPECT_FALSE(contains(pa->isr, 3)) << "the first commit can only follow the never-started broker leaving the ISR";
    EXPECT_EQ(pa->replicas.size(), 3u) << "it stays a replica (it fetches from 0 when it starts)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 20s) << "the formation stall is bounded";
    c.startNode(3);                                       // empty data dir
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(3), hw, 20s));
    EXPECT_EQ(catalogOf(c.node(3)).logStart, 0) << "the catalog always starts at offset 0";
    EXPECT_TRUE(c.awaitInCatalogIsr(3, 20s)) << "AlterIsr admits it once caught up";
}

// E2 lesson: a follower's log reset (a wiped data dir, a replaced disk) must be
// handled — the broker refetches the whole catalog from offset 0 and rejoins
// the ISR; it is never counted in sync with a log it no longer has.
TEST(CatalogReplicationRegression, AFollowerWhoseDataDirIsWipedRefetchesFromZeroAndRejoinsTheIsr) {
    ThreeNodes c = ThreeNodes::start("cat_wipe");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw1 = c.appendRawOnCatalogLeaderForTest({1});
    ASSERT_GE(hw1, 1);
    const int32_t leader = c.catalogLeaderId();
    const int32_t victim = leader == 3 ? 2 : 3;
    ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw1, 10s));
    c.stop(victim);
    c.wipe(victim);
    const int64_t hw2 = c.appendRawOnCatalogLeaderForTest({2});
    ASSERT_GT(hw2, hw1) << "two of three keep committing";
    c.startNode(victim);
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw2, 20s)) << "refetched from offset 0";
    EXPECT_TRUE(c.awaitInCatalogIsr(victim, 20s));
}

// A full-cluster restart: every broker recovers its disk catalog, and the
// committed leader (which already led at the committed epoch in its previous
// incarnation — the restart fence) is re-elected only at a NEW epoch through
// the admin→control AlterIsr relay; the committed prefix survives and the
// catalog keeps taking writes.
TEST(CatalogReplicationRegression, FullClusterRestartRecoversTheCatalogAndReElectsAtANewEpoch) {
    ThreeNodes c = ThreeNodes::start("cat_restart");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({6, 6});
    ASSERT_GE(hw, 1);
    for (int id = 1; id <= 3; ++id) ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(id), hw, 10s)) << id;
    const auto before = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(before.has_value());
    for (int id = 3; id >= 1; --id) c.stop(id);
    for (int id = 1; id <= 3; ++id) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const auto after = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(after.has_value());
    EXPECT_GT(after->leaderEpoch, before->leaderEpoch) << "a new leadership always gets a new epoch";
    for (int id = 1; id <= 3; ++id) EXPECT_GE(catalogOf(c.node(id)).leo, hw) << "recovered from disk: " << id;
    EXPECT_GT(c.appendRawOnCatalogLeaderForTest({7, 7}), hw);
}

// ===========================================================================
// Single node: the consensus leader leads the catalog (no controller path).
// ===========================================================================
TEST(CatalogReplicationRegression, SingleNodeLeadsTheCatalogAndKeepsItAcrossARestart) {
    TmpDir dir{"cat_single"};
    const Ports ports = Ports::discover(1);
    int64_t hw = -1;
    {
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        n.start();
        ASSERT_TRUE(n.awaitLeader(10'000ms));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (hw < 0 && std::chrono::steady_clock::now() < deadline) {
            hw = n.onAdminThreadForTest([&n]() -> int64_t {
                rr::PartitionReplica* r = n.catalogReplicaForTest();
                if (r == nullptr || r->role() != rr::Role::Leader) return -1;
                const std::vector<uint8_t> rec{3, 1, 4};
                if (r->append(std::span<const uint8_t>(rec), /*acksAll=*/true) < 0) return -1;
                return r->highWatermark() >= r->logEndOffset() ? r->highWatermark() : -1;
            });
            if (hw < 0) std::this_thread::sleep_for(20ms);
        }
        ASSERT_GE(hw, 1) << "a single node commits catalog writes (minIsr 1)";
        n.stop();
        // Quiesced: the Log is readable from the test thread now.
        ASSERT_NE(n.catalogLogForTest(), nullptr);
        EXPECT_GE(n.catalogLogForTest()->logEndOffset(), hw);
        EXPECT_EQ(n.catalogLogForTest()->dir(), (dir.path / "__catalog").string());
    }
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    EXPECT_GE(catalogOf(n).leo, hw) << "the catalog is disk-backed: recovered on restart";
    n.stop();
}

// ===========================================================================
// The admin/control relay units (no cluster).
// ===========================================================================
namespace {
struct CatalogRig {
    TmpDir                          dir{"cat_rig"};
    rune::storage::Log              log{dir.path.string()};
    rr::NullPartitionTransport      transport;
    std::unique_ptr<rr::PartitionReplica> replica;
    SchemaRegistry                  reg;
    std::unique_ptr<AdminWorkerAgent> agent;
    CatalogRig() {
        log.recover();
        NodeConfig nc;
        nc.nodeId = 1;
        replica = std::make_unique<rr::PartitionReplica>(makeCatalogPartitionConfig(nc, 3), rr::Role::Follower, 0,
                                                         std::vector<rr::NodeId>{1, 2, 3},
                                                         std::vector<rr::NodeId>{1, 2, 3}, &log, &transport);
        agent = std::make_unique<AdminWorkerAgent>(reg, nullptr, 1, std::vector<int32_t>{1, 2, 3}, AdminDeps{},
                                                   nullptr, nullptr, replica.get(), nullptr);
        replica->setIsrChannel(&agent->catalogIsrChannel());
    }
    std::vector<rr::AlterIsrMsg> drainAlterIsr() {
        std::vector<rr::AlterIsrMsg> out;
        while (agent->failureRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                              aeron::util::index_t off, aeron::util::index_t len) {
            if (tid != kA2cCatalogAlterIsr) return;
            rr::AlterIsrMsg m{};
            if (decodeCatalogAlterIsr(reinterpret_cast<const uint8_t*>(b.buffer() + off),
                                      static_cast<std::size_t>(len), m))
                out.push_back(m);
        }, 64) > 0) {}
        return out;
    }
};
} // namespace

TEST(CatalogAlterIsrRelayRegression, AdminWorkerAppliesTheCatalogRoleOnItsOwnThread) {
    CatalogRig rig;
    AdminRoleMsg lead;
    lead.leader = true; lead.epoch = 3; lead.leaderId = 1; lead.partitionEpoch = 0;
    lead.isrCount = 2; lead.isr[0] = 1; lead.isr[1] = 2;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), lead, kMsgCatalogRole));
    (void)rig.agent->doWork(1'000'000);
    EXPECT_EQ(rig.replica->role(), rr::Role::Leader);
    EXPECT_EQ(rig.replica->epoch(), 3u);
    EXPECT_EQ(rig.replica->isr().size(), 2u) << "the committed ISR, not the replica set";
    EXPECT_EQ(rig.replica->committedPartitionEpochForTest(), 0);

    AdminRoleMsg follow;
    follow.leader = false; follow.epoch = 4; follow.leaderId = 2;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), follow, kMsgCatalogRole));
    (void)rig.agent->doWork(2'000'000);
    EXPECT_EQ(rig.replica->role(), rr::Role::Follower);
    EXPECT_EQ(rig.replica->epoch(), 4u);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 0u);

    // A malformed catalog role is refused and reported — never applied.
    const uint8_t junk[3] = {1, 2, 3};
    const auto idx = rig.agent->roleRing().tryClaim(kMsgCatalogRole, sizeof junk);
    ASSERT_GE(idx, 0);
    std::memcpy(rig.agent->roleRing().base() + idx, junk, sizeof junk);
    rig.agent->roleRing().commit(idx);
    (void)rig.agent->doWork(3'000'000);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 1u);
    EXPECT_EQ(rig.replica->role(), rr::Role::Follower);
    EXPECT_EQ(rig.replica->epoch(), 4u);
}

// E2 shard-mode lesson, admin edition: the controller's answer reaches the
// admin-owned replica WITH its leaderId, so the replica matches its own
// leaderless-partition report and stops re-sending it (left at -1 it never
// matched: re-sent forever).
TEST(CatalogAlterIsrRelayRegression, ControllerAnswerReachesTheAdminReplicaAndMatchesItsReportRegression) {
    CatalogRig rig;
    AdminRoleMsg leaderless;
    leaderless.leader = false; leaderless.epoch = 5; leaderless.leaderId = -1;   // no live ISR member
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), leaderless, kMsgCatalogRole));
    (void)rig.agent->doWork(1'000'000);
    ASSERT_TRUE(rig.replica->leaderlessReportPendingForTest());
    const auto sent = rig.drainAlterIsr();
    ASSERT_EQ(sent.size(), 1u) << "the report rides the admin→control ring (kA2cCatalogAlterIsr)";
    EXPECT_EQ(sent[0].topicId, kCatalogTopicId);
    EXPECT_EQ(sent[0].leaderId, 1) << "the reporting replica";
    EXPECT_EQ(sent[0].leaderEpoch, 5);
    EXPECT_EQ(sent[0].isrCount, rr::AlterIsrMsg::kLeaderEpochBump);
    EXPECT_NE(sent[0].isr[1] & rr::AlterIsrMsg::kBumpFollowerReport, 0);

    // An answer that lost its leaderId (the E2 bug) is NOT this replica's.
    rr::AlterIsrResponseMsg lost{};
    lost.topicId = kCatalogTopicId; lost.partition = 0; lost.leaderId = -1; lost.leaderEpoch = 5;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), lost));
    (void)rig.agent->doWork(2'000'000);
    EXPECT_TRUE(rig.replica->leaderlessReportPendingForTest());

    // The verbatim relay (leaderId = the asker) clears it.
    rr::AlterIsrResponseMsg ack = lost;
    ack.leaderId = 1;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), ack));
    (void)rig.agent->doWork(3'000'000);
    EXPECT_FALSE(rig.replica->leaderlessReportPendingForTest())
        << "the controller's answer must reach the admin-owned replica and match";
}

// A full relay ring re-delivers (ABORT), never drops; a local answer refused by
// it is reported refused (the outbox retries), not delivered.
TEST(CatalogAlterIsrRelayRegression, ForeignAnswerRingFullIsRedeliveredNotDroppedRegression) {
    TmpDir ad{"cat_drv"};
    EmbeddedDriverConfig dc;
    dc.dir = ad.path.string();
    dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rr::AeronPartitionTransport tr(client, "aeron:ipc", 7311, /*self*/1, 1 << 20);

    struct Sink { int calls = 0; bool accept = false; rr::AlterIsrResponseMsg last{}; } sink;
    tr.setForeignAlterIsrResponseHandler(
        [](void* ctx, const rr::AlterIsrResponseMsg& r) noexcept {
            auto* s = static_cast<Sink*>(ctx);
            ++s->calls;
            s->last = r;
            return s->accept;
        }, &sink);

    rr::AlterIsrResponseMsg r{};
    r.topicId = kCatalogTopicId; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 2; r.partitionEpoch = 1;
    uint8_t wire[rr::AlterIsrResponseMsg::TOTAL_SIZE];
    ASSERT_EQ(r.encode(wire, sizeof wire), sizeof wire);

    EXPECT_FALSE(tr.deliverFrameForTest(wire, sizeof wire, 1)) << "a full relay ring leaves the fragment unconsumed";
    EXPECT_FALSE(tr.sendAlterIsrResponse(1, r)) << "a refused local answer is not reported delivered";
    sink.accept = true;
    EXPECT_TRUE(tr.deliverFrameForTest(wire, sizeof wire, 1));
    EXPECT_TRUE(tr.sendAlterIsrResponse(1, r));
    EXPECT_EQ(sink.calls, 4);
    EXPECT_EQ(sink.last.leaderId, 1) << "verbatim";
}

// ===========================================================================
// Fix-on-sight (hard rule 1), found reading ReplicationMetadataBridge.h for
// this task: the bridge kept std::atomic leadership/epoch MIRRORS for other
// threads to read.  Observers now read the bridge ON its own (control) thread.
// ===========================================================================
template <class B>
concept HasAtomicLeadershipMirror = requires(const B& b) { b.isLeaderAtomic(); } ||
                                    requires(const B& b) { b.lastEpochAtomic(); };
static_assert(!HasAtomicLeadershipMirror<rune::node::ReplicationMetadataBridge>,
              "no cross-thread atomic mirror on the bridge");

TEST(BridgeObserverRegression, LeadershipIsObservedOnTheControlThreadNotThroughAnAtomicMirrorRegression) {
    TmpDir dir{"cat_obs"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    EXPECT_FALSE(n.replicationIsLeaderForTest()) << "not running: nothing observed";
    EXPECT_EQ(n.replicationEpochForTest(), 0);
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!n.replicationIsLeaderForTest() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(n.replicationIsLeaderForTest());
    EXPECT_GE(n.replicationEpochForTest(), 1);
    n.stop();
    EXPECT_FALSE(n.replicationIsLeaderForTest()) << "stopped: the bridge is gone";
}

// ===========================================================================
// Fix round 0 (Task 6 review rulings).
// ===========================================================================

// Ruling on concern 3: a multi-node broker without a replication transport
// REFUSES to start — never a NullPartitionTransport catalog that can never be
// replicated (nor reach its majority minIsr).
TEST(CatalogReplicationRegression, MultiNodeWithoutAReplicationTransportRefusesToStartRegression) {
    TmpDir dir{"cat_norepl"};
    const Ports ports = Ports::discover(3);
    NodeConfig cfg = makeConfig(1, dir.path, ports, 3, Mode::Legacy);
    cfg.perWorkerReplication = false;
    for (auto& p : cfg.peers) p.replicationPort = 0;   // no replication endpoint anywhere
    BrokerNode n{cfg};
    try {
        n.start();
        ADD_FAILURE() << "a multi-node broker without a replication transport started";
    } catch (const std::runtime_error& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("__catalog"), std::string::npos) << what;
        EXPECT_NE(what.find("replication transport"), std::string::npos) << what;
    }
    n.stop();
}

// Ruling on concern 2: the control-thread observers, called ON the control
// thread, fail loudly instead of awaiting themselves forever.
namespace {
void expectObserversRefuseOnTheControlThread(Mode mode, const char* tag) {
    TmpDir dir{tag};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, mode)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const std::thread::id onControl = n.onControlThreadForTest([] { return std::this_thread::get_id(); });
    EXPECT_EQ(n.controlThreadIdForTest(), onControl) << "start() records the real control thread";
    EXPECT_NE(n.controlThreadIdForTest(), std::this_thread::get_id());
    EXPECT_THROW((void)n.onControlThreadForTest([&n] { return n.replicationIsLeaderForTest(); }), std::logic_error);
    EXPECT_THROW((void)n.onControlThreadForTest([&n] { return n.replicationEpochForTest(); }), std::logic_error);
    EXPECT_NO_THROW((void)n.replicationIsLeaderForTest()) << "the test thread still reads it";
    n.stop();
    EXPECT_EQ(n.controlThreadIdForTest(), std::thread::id{});
}
} // namespace

TEST(BridgeObserverRegression, ObserverCalledOnTheControlThreadFailsLoudlyNotDeadlocksRegression) {
    expectObserversRefuseOnTheControlThread(Mode::Shard, "cat_obs_thr");
}

TEST(BridgeObserverRegression, ObserverCalledOnTheFusedControlThreadFailsLoudlyNotDeadlocksRegression) {
    expectObserversRefuseOnTheControlThread(Mode::ShardFused, "cat_obs_fus");
}


// ===========================================================================
// Fix round 1 (I1): the catalog's minIsr = majority + acks=all, fired at
// replica level with THE catalog config (no timing: a controller-managed
// leader, its committed ISR set explicitly).
// ===========================================================================
TEST(CatalogMinIsrRegression, AcksAllIsGatedBelowAMajorityAndCommitsOnceAFollowerAcksRegression) {
    TmpDir dir{"cat_minisr"};
    rune::storage::Log log{dir.path.string()};
    log.recover();
    rr::NullPartitionTransport transport;
    OneToOneByteRing a2c(1 << 16);
    AdminIsrChannel isrChannel(a2c);
    NodeConfig nc;
    nc.nodeId = 1;
    const auto cfg = makeCatalogPartitionConfig(nc, 3);
    EXPECT_EQ(cfg.minIsr, 2);   // EXPECT: the gate checks below must fire on their own
    rr::PartitionReplica r(cfg, rr::Role::Follower, 0, {1, 2, 3}, {1, 2, 3}, &log, &transport);
    r.setIsrChannel(&isrChannel);   // controller-managed, as in production
    const std::vector<uint8_t> rec{1, 2, 3};
    int64_t now = 1'000'000;

    // Committed ISR {1}: below the majority → acks=all is refused.
    r.becomeLeader(3, {1}, /*partitionEpoch=*/0);
    ASSERT_EQ(r.role(), rr::Role::Leader);
    (void)r.doWork(now += 1'000'000);
    EXPECT_EQ(r.append(std::span<const uint8_t>(rec), /*acksAll=*/true), rr::PartitionReplica::kAppendMinIsr)
        << "acks=all must be refused with a committed ISR below minIsr (majority)";
    EXPECT_EQ(r.logEndOffset(), 0) << "nothing entered the log";
    EXPECT_EQ(r.highWatermark(), 0);

    // The controller commits ISR {1, 2} (same epoch, partitionEpoch 1): the gate opens,
    // but the HW stays below the record until follower 2 holds it.
    r.becomeLeader(3, {1, 2}, /*partitionEpoch=*/1);
    const rr::Offset base = r.append(std::span<const uint8_t>(rec), /*acksAll=*/true);
    ASSERT_GE(base, 0) << "a majority ISR admits acks=all";
    const rr::Offset leo = r.logEndOffset();
    ASSERT_GT(leo, base);
    (void)r.doWork(now += 1'000'000);
    EXPECT_LE(r.highWatermark(), base) << "no follower holds it yet: not committed";

    // Follower 2 acks (fetches at the leader's LEO) → committed.
    rr::FetchRequestMsg ack{};
    ack.leaderEpoch = 3; ack.partition = 0; ack.followerId = 2; ack.fetchOffset = leo;
    ack.maxBytes = 1 << 20; ack.topicId = kCatalogTopicId; ack.lastFetchedEpoch = -1;
    r.onFetchRequest(ack, now += 1'000'000);
    (void)r.doWork(now += 1'000'000);
    EXPECT_EQ(r.highWatermark(), leo) << "the HW advances once a majority (leader + 1 follower) holds it";
}

// Fix round 1 (M1): -6's never-unclean wiring — no live ISR member → NO_LEADER
// with the ISR lineage kept (an out-of-sync live replica is never elected).
TEST(CatalogNeverUncleanRegression, NoLiveIsrMemberLeavesTheCatalogLeaderlessWithItsIsrKeptRegression) {
    rune::metadata::PartitionAssignment cur;
    cur.leaderNode = 1; cur.isr = {1}; cur.replicas = {1, 2, 3};
    cur.leaderEpoch = 4; cur.partitionEpoch = 7;
    const auto next = MetadataService::decideCoordinatorPartition(
        kCatalogTopicId, kCatalogPartition, /*self=*/2, &cur, /*eligible=*/{2, 3}, /*positions=*/nullptr,
        /*bootstrapEpoch=*/9, /*replicas=*/{1, 2, 3});
    ASSERT_TRUE(next.has_value()) << "the dead leader must be replaced";
    EXPECT_EQ(next->leaderNode, rune::metadata::NO_LEADER) << "never an unclean (out-of-ISR) leader";
    EXPECT_EQ(next->isr, std::vector<int32_t>{1}) << "the ISR lineage is kept";
    EXPECT_GT(next->leaderEpoch, cur.leaderEpoch);

    // With a live ISR member it is elected — and only it.
    cur.isr = {1, 3};
    const auto clean = MetadataService::decideCoordinatorPartition(
        kCatalogTopicId, kCatalogPartition, 2, &cur, {2, 3}, nullptr, 9, {1, 2, 3});
    ASSERT_TRUE(clean.has_value());
    EXPECT_EQ(clean->leaderNode, 3);
    EXPECT_TRUE(contains(clean->isr, 3));
    EXPECT_FALSE(contains(clean->isr, 2)) << "the out-of-sync controller is not admitted by an election";
}

// Fix round 1 (M3 + M5): the controller-answer relay decision.
TEST(CatalogAlterIsrRelayRegression, AnswerNeverOvertakesAHeldRoleAndIsDroppedOnceTheAdminThreadIsGoneRegression) {
    OneToOneByteRing ring(1 << 12);
    rr::AlterIsrResponseMsg r{};
    r.topicId = kCatalogTopicId; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 2;
    EXPECT_EQ(relayCatalogAlterIsrResponse(&ring, /*rolePending=*/true, r), CatalogAnswerRelay::Held)
        << "FIFO: a held catalog role goes first";
    EXPECT_EQ(relayCatalogAlterIsrResponse(nullptr, false, r), CatalogAnswerRelay::Dropped)
        << "admin thread gone: dropped (counted), never blocking the peer's image";
    EXPECT_EQ(relayCatalogAlterIsrResponse(nullptr, true, r), CatalogAnswerRelay::Dropped);
    int frames = 0;
    (void)ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {
        ++frames;
    }, 64);
    EXPECT_EQ(frames, 0) << "neither encoded anything";
    EXPECT_EQ(relayCatalogAlterIsrResponse(&ring, false, r), CatalogAnswerRelay::Delivered);
    OneToOneByteRing full(256);
    while (encodeCatalogAlterIsrResponse(full, r)) {}
    EXPECT_EQ(relayCatalogAlterIsrResponse(&full, false, r), CatalogAnswerRelay::Held) << "ring full: re-delivered";
}

// Fix round 1 (M4, fix-on-sight): a malformed __topic_schemas role frame is
// reported, never applied as a default Follower@0 role.
TEST(CatalogAlterIsrRelayRegression, MalformedSchemaRoleFrameIsReportedNotAppliedRegression) {
    CatalogRig rig;
    const uint8_t junk[3] = {1, 9, 9};
    const auto idx = rig.agent->roleRing().tryClaim(kMsgAdminRole, sizeof junk);
    ASSERT_GE(idx, 0);
    std::memcpy(rig.agent->roleRing().base() + idx, junk, sizeof junk);
    rig.agent->roleRing().commit(idx);
    (void)rig.agent->doWork(1'000'000);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 1u);
}
