#pragma once
// CatalogBrokerCluster.h — three in-process BrokerNodes over REAL Aeron UDP
// (ids 1..3) for the catalog suites: Task 6's CatalogReplicationTest and Task
// 11's CatalogLeadershipBrokerTest.  Polls are bounded (the deadline is the
// failure, never a hang); the nodes' own threads do the work.
#include "support/ShardTestClient.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

namespace rune::node::test::catalog_cluster {

namespace fs = std::filesystem;
namespace rr = rune::replication;
using namespace std::chrono_literals;

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

inline constexpr int32_t kClientStream  = 110;
inline constexpr int32_t kIngressStream = 111;

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
inline NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int nodes, Mode mode) {
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

inline std::optional<rune::metadata::PartitionAssignment> catalogAssignmentOf(BrokerNode& n) {
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

inline CatalogView catalogOf(BrokerNode& n) {
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

inline bool contains(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }

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

    /// Applied to every node's config before it starts (catalog Task 12 fix round 1: a small MTU).
    std::function<void(NodeConfig&)> configTweak;
    static ThreeNodes startWith(const std::string& tag, std::function<void(NodeConfig&)> tweak) {
        ThreeNodes c(tag, Mode::Shard);
        c.configTweak = std::move(tweak);
        for (int id : {1, 2, 3}) c.startNode(id);
        return c;
    }

    /// Catalog history Task 16: called with (id, the constructed, not yet started node) —
    /// the pre-start test seams (holdCatalogApplyAtBootForTest).
    std::function<void(int, BrokerNode&)> beforeStart;

    void startNode(int id) {
        NodeConfig cfg = makeConfig(id, dirs[static_cast<std::size_t>(id - 1)]->path, ports, 3, mode);
        if (configTweak) configTweak(cfg);
        nodes[static_cast<std::size_t>(id - 1)] = std::make_unique<BrokerNode>(cfg);
        if (beforeStart) beforeStart(id, *nodes[static_cast<std::size_t>(id - 1)]);
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

    /// Append the NEXT event of the v1 golden script (a valid catalog history:
    /// every broker's applier applies it — a garbage record would halt every
    /// applier, and a halted broker resigns the controllership, Task 11) as one
    /// record on the catalog leader's admin thread (acks=all), then wait for its
    /// HW to cover it.  Returns that HW (> the record's offset), or -1 if it never
    /// committed (the event is then retried by the next call).
    int64_t appendEventOnCatalogLeaderForTest(std::chrono::milliseconds within = 30s) {
        if (nextGolden_ >= golden_.size()) throw std::logic_error("ThreeNodes: the golden script is exhausted");
        auto encoded = rune::catalog::encode(golden_[nextGolden_]);
        if (!encoded) throw std::logic_error("ThreeNodes: a golden event does not encode");
        const int64_t hw = appendRaw_(std::move(*encoded), within);
        if (hw >= 0) ++nextGolden_;
        return hw;
    }

private:
    std::vector<rune::catalog::CatalogEvent> golden_ = rune::catalog::testing::goldenScriptV1();
    std::size_t                              nextGolden_ = 0;
    int64_t appendRaw_(std::vector<uint8_t> bytes, std::chrono::milliseconds within) {
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
public:
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


} // namespace rune::node::test::catalog_cluster
