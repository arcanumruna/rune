#pragma once
// CatalogBrokerCluster.h — three in-process BrokerNodes over REAL Aeron UDP
// (ids 1..3) for the catalog suites: Task 6's CatalogReplicationTest and Task
// 11's CatalogLeadershipBrokerTest.  Polls are bounded (the deadline is the
// failure, never a hang); the nodes' own threads do the work.
#include "support/ShardTestClient.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/StorageIdentity.h>

#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <array>
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
    /// Task 22: the real clients' data dirs live OUTSIDE the brokers' (wipe(id) must not take them along).
    std::unique_ptr<TmpDir>                  clientsDir;

    ThreeNodes(std::string t, Mode m) : tag(std::move(t)), mode(m), ports(Ports::discover(3)) {
        for (int i = 0; i < 3; ++i) dirs.push_back(std::make_unique<TmpDir>(tag + "_n" + std::to_string(i + 1)));
        nodes.resize(3);
        clientsDir = std::make_unique<TmpDir>(tag + "_clients");
    }
    ThreeNodes(ThreeNodes&&) = default;
    ~ThreeNodes() {
        joinKillWatcher_();
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
        if (const auto f = storageFormatFor_[static_cast<std::size_t>(id - 1)]) cfg.storageFormat = *f;   // wipeRaw
        nodes[static_cast<std::size_t>(id - 1)] = std::make_unique<BrokerNode>(cfg);
        if (beforeStart) beforeStart(id, *nodes[static_cast<std::size_t>(id - 1)]);
        nodes[static_cast<std::size_t>(id - 1)]->start();
    }
    void stop(int id) {
        auto& n = nodes[static_cast<std::size_t>(id - 1)];
        if (n) { n->stop(); n.reset(); }
    }
    /// A lost disk replaced the production way (ISR design v3 Step 5): the contents go, then the operator's
    /// `rune_broker storage format --join` — the node restarts as a Joiner (the in-process TestFounder default
    /// would otherwise re-found the empty dir).
    void wipe(int id) {
        removeContents_(id);
        storageFormatFor_[static_cast<std::size_t>(id - 1)].reset();   // CP1 M7: a prior wipeRaw override must not outlive this wipe
        formatJoin(id);
    }
    [[nodiscard]] bool hasStorageFormatOverrideForTest(int id) const {
        return storageFormatFor_[static_cast<std::size_t>(id - 1)].has_value();
    }
    void formatJoin(int id) {
        ASSERT_EQ(rune::node::StorageIdentity::format(dirs[static_cast<std::size_t>(id - 1)]->path.string(),
                                                      rune::node::StorageIdentity::Origin::Joiner,
                                                      rune::node::StorageIdentity::Mode::None, false).code, 0);
    }
    /// The contents only, no storage.meta (v3-check V3-I1): the node restarts with format `f` (Auto / New /
    /// Required).  TestFounder is refused: it would re-found a wiped node, a state production cannot reach.
    void wipeRaw(int id, rune::node::StorageFormat f) {
        if (f == rune::node::StorageFormat::TestFounder) {
            ADD_FAILURE() << "a raw-wiped node must restart Auto/New/Required: TestFounder would re-found it";
            return;
        }
        removeContents_(id);
        storageFormatFor_[static_cast<std::size_t>(id - 1)] = f;
    }
    BrokerNode& node(int id) { return *nodes[static_cast<std::size_t>(id - 1)]; }
private:
    void removeContents_(int id) {
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(dirs[static_cast<std::size_t>(id - 1)]->path, ec))
            fs::remove_all(e.path(), ec);
    }
    /// Per node: the storage format wipeRaw installed (applied after configTweak); nullopt = the config's own.
    std::array<std::optional<rune::node::StorageFormat>, 3> storageFormatFor_{};
public:
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
    std::thread killWatcher_;
    int32_t     killWatcherNode_   = -1;
    int32_t     killWatcherResult_ = -1;   // written by the watcher, read after its join
    void joinKillWatcher_() { if (killWatcher_.joinable()) killWatcher_.join(); }
    std::vector<rune::catalog::CatalogEvent> golden_ = rune::catalog::testing::goldenScriptV1();
    std::size_t                              nextGolden_ = 0;
    // Review B-I7 (DR-bugs fix round 1): the helper follows a leadership change.  Invariant: it returns an
    // HW >= the record's end offset on the leader of the CURRENT settled leadership, or -1 at the deadline; it
    // never waits on a node that no longer leads.  A record appended on a leadership that was deposed before it
    // committed is truncated there; when the new leader does not hold it, the same bytes are appended again.
    // Re-check M8: "holds it" = its log reaches our end offset AND the batch at end-1 carries our epoch.
    int64_t appendRaw_(std::vector<uint8_t> bytes, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            int64_t leo = -1, epoch = -1;
            int32_t leader = -1;
            while (leo < 0 && std::chrono::steady_clock::now() < deadline) {   // append phase
                leader = settledLeader();
                if (leader > 0) {
                    BrokerNode& n = node(leader);
                    const auto le = n.onAdminThreadForTest([&n, &bytes]() -> std::pair<int64_t, int64_t> {
                        rr::PartitionReplica* r = n.catalogReplicaForTest();
                        if (r == nullptr || r->role() != rr::Role::Leader) return {-1, -1};
                        const rr::Offset base = r->append(std::span<const uint8_t>(bytes), /*acksAll=*/true);
                        if (base < 0) return {-1, -1};   // min-ISR gate (ISR forming)
                        return {r->logEndOffset(), static_cast<int64_t>(r->epoch())};
                    });
                    leo = le.first;
                    epoch = le.second;
                }
                if (leo < 0) std::this_thread::sleep_for(20ms);
            }
            if (leo < 0) return -1;
            bool reappend = false;
            while (!reappend && std::chrono::steady_clock::now() < deadline) {   // HW phase
                const int32_t now = settledLeader();
                if (now > 0) {
                    const auto pa = catalogAssignmentOf(node(now));
                    const bool moved = now != leader || (pa && pa->leaderEpoch != epoch);
                    const auto v = catalogOf(node(now));
                    if (moved && v.leo < leo) { reappend = true; break; }   // truncated with the deposed leadership
                    if (moved) {
                        // Re-check M8: an end offset >= ours is not proof our record survived — the new leadership
                        // may have written ITS records over the truncated range.  The batch at leo-1 must still
                        // carry the epoch we appended under; read on the new leader's admin thread.
                        BrokerNode& nn = node(now);
                        const int32_t at = nn.onAdminThreadForTest([&nn, leo]() -> int32_t {
                            rr::PartitionReplica* r = nn.catalogReplicaForTest();
                            return r == nullptr ? -2 : r->lastFetchedEpochAtForTest(leo);
                        });
                        if (at == -2) { std::this_thread::sleep_for(10ms); continue; }   // replica not open yet
                        if (at != static_cast<int32_t>(epoch)) { reappend = true; break; }   // another leadership's record
                    }
                    if (v.hw >= leo) return v.hw;
                }
                std::this_thread::sleep_for(10ms);
            }
            if (!reappend) return -1;
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

    // ── Catalog history Task 22: the 3-node slice's cluster-level observations / actions ──────────────
    /// The client endpoint of broker `id` (the channel a wire client / the real client connects to).
    [[nodiscard]] std::string clientChannel(int id) const {
        return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[static_cast<std::size_t>(id - 1)]);
    }
    [[nodiscard]] std::vector<int> runningNodes() const {
        std::vector<int> v;
        for (int id = 1; id <= 3; ++id) if (running(id)) v.push_back(id);
        return v;
    }
    /// A REAL crash (BrokerNode::crashForTest: no controlled shutdown, no goodbye).  Test thread.
    void crash(int id) {
        joinKillWatcher_();
        auto& n = nodes[static_cast<std::size_t>(id - 1)];
        if (n) { n->crashForTest(); n.reset(); }
    }

    /// What broker `n`'s admin thread holds of the catalog (read ON the admin thread).
    struct CatalogSnap {
        bool        ok = false;
        int64_t     applied = -1;     ///< the applier's next offset (-1: halted)
        uint64_t    digest = 0;       ///< CatalogState::digest() (definitions + counters + request records)
        int64_t     leo = -1, hw = -1;
        std::string role;
    };
    static CatalogSnap snapOf(BrokerNode& n) {
        return n.onAdminThreadForTest([&n]() -> CatalogSnap {
            CatalogSnap v;
            rr::PartitionReplica* r = n.catalogReplicaForTest();
            if (r == nullptr) return v;
            auto& w = n.adminWorkerAgentForTest()->worker();
            v.ok = true;
            v.applied = w.applier().nextOffset();
            v.digest = w.applier().state().digest();
            v.leo = r->logEndOffset();
            v.hw = r->highWatermark();
            v.role = std::string(w.roleName());
            return v;
        });
    }
    /// Every running broker holds the SAME committed catalog and has applied all of it: equal digests,
    /// applied == LEO == HW everywhere (so an uncommitted tail has been truncated, none is pending).
    bool awaitCatalogDigestsEqual(std::chrono::milliseconds within, std::string* why = nullptr) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        std::string last = "no broker running";
        while (true) {
            bool ok = !runningNodes().empty();
            last.clear();
            std::optional<CatalogSnap> first;
            for (int id : runningNodes()) {
                const CatalogSnap v = snapOf(node(id));
                last += " n" + std::to_string(id) + "{" + v.role + " applied=" + std::to_string(v.applied) +
                        " leo=" + std::to_string(v.leo) + " hw=" + std::to_string(v.hw) + " digest=" +
                        std::to_string(v.digest) + "}";
                if (!v.ok || v.applied < 0 || v.applied != v.leo || v.leo != v.hw) ok = false;
                if (!first) first = v;
                else if (v.digest != first->digest || v.leo != first->leo) ok = false;
            }
            if (ok) return true;
            if (std::chrono::steady_clock::now() >= deadline) break;
            std::this_thread::sleep_for(20ms);
        }
        if (why != nullptr) *why = last;
        return false;
    }
    /// Broker `id`'s catalog replica ISR size (read ON its admin thread).
    std::size_t catalogIsrSizeOn(int id) {
        BrokerNode& n = node(id);
        return n.onAdminThreadForTest([&n]() -> std::size_t {
            rr::PartitionReplica* r = n.catalogReplicaForTest();
            return r == nullptr ? 0 : r->isr().size();
        });
    }
    bool awaitCatalogIsrSizeOn(int id, std::size_t want, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            if (catalogIsrSizeOn(id) == want) return true;
            std::this_thread::sleep_for(50ms);
        }
        return catalogIsrSizeOn(id) == want;
    }
    /// Slice Recipe 1: the CURRENT settled catalog leader's committed-ISR size reaches `want` (re-resolved every poll: a
    /// follower's isr() is always empty, so polling a node that lost the leadership could never succeed).  *leaderOut = it.
    bool awaitCatalogIsrSize(std::size_t want, std::chrono::milliseconds within, int* leaderOut = nullptr) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        do {
            const int l = settledLeader();
            if (l > 0 && catalogIsrSizeOn(l) == want) { if (leaderOut) *leaderOut = l; return true; }
            std::this_thread::sleep_for(50ms);
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }
    /// Broker `id`'s whole __catalog log (committed or not), decoded, in offset order (ON its admin thread).
    std::vector<rune::catalog::RecordedEvent> catalogEventsOn(int id) {
        BrokerNode& n = node(id);
        return n.onAdminThreadForTest([&n]() {
            std::vector<rune::catalog::RecordedEvent> out;
            rr::PartitionReplica* r = n.catalogReplicaForTest();
            if (r == nullptr) return out;
            int64_t next = 0;
            const int64_t leo = r->log()->logEndOffset();
            while (next < leo) {
                const std::vector<uint8_t> data = r->log()->read(next, 1 << 22);
                if (data.empty()) break;
                const int64_t before = next;
                std::size_t pos = 0;
                while (pos < data.size()) {
                    rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                    if (!reader.ensureValid()) break;
                    reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                        if (off < next) return;
                        auto ev = rune::catalog::decode(
                            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                        if (!ev) throw std::logic_error("ThreeNodes: a __catalog record does not decode");
                        out.push_back(rune::catalog::RecordedEvent{off, std::move(*ev)});
                        next = off + 1;
                    });
                    if (reader.totalLength() == 0) break;
                    pos += static_cast<std::size_t>(reader.totalLength());
                }
                if (next == before) break;
            }
            return out;
        });
    }
    /// Events carrying `rid` in broker `id`'s whole __catalog log (committed or not), read ON its admin thread.
    std::size_t eventsWithRequestIdOn(int id, const rune::protocol::RequestId& rid) {
        BrokerNode& n = node(id);
        return n.onAdminThreadForTest([&n, &rid]() -> std::size_t {
            rr::PartitionReplica* r = n.catalogReplicaForTest();
            if (r == nullptr) return 0;
            std::size_t count = 0;
            int64_t next = 0;
            const int64_t leo = r->log()->logEndOffset();
            while (next < leo) {
                const std::vector<uint8_t> data = r->log()->read(next, 1 << 22);
                if (data.empty()) break;
                const int64_t before = next;
                std::size_t pos = 0;
                while (pos < data.size()) {
                    rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                    if (!reader.ensureValid()) break;
                    reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                        if (off < next) return;
                        const auto ev = rune::catalog::decode(
                            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                        if (ev && ev->principal.requestId == rid) ++count;
                        next = off + 1;
                    });
                    if (reader.totalLength() == 0) break;
                    pos += static_cast<std::size_t>(reader.totalLength());
                }
                if (next == before) break;
            }
            return count;
        });
    }
    /// Events carrying `rid`, asserted equal on every running broker (the catalogs are identical once
    /// awaitCatalogDigestsEqual held); returns the count on the first one, SIZE_MAX if brokers disagree.
    std::size_t countEventsWithRequestId(const rune::protocol::RequestId& rid) {
        std::optional<std::size_t> n;
        for (int id : runningNodes()) {
            const std::size_t c = eventsWithRequestIdOn(id, rid);
            if (n && *n != c) return SIZE_MAX;
            n = c;
        }
        return n.value_or(0);
    }

    /// Hold broker `id`'s DDL answers, then (on THIS thread) wait until its admin thread appended an event
    /// beyond `baseline` [and, with `waitCommit`, until that event is committed on the leader's HW], and
    /// crash it.  false: the append (or commit) never showed within `within`.  Test thread.
    bool crashAfterNextAppend(int id, uint64_t baseline, bool waitCommit, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        BrokerNode& n = node(id);
        bool ready = false;
        while (std::chrono::steady_clock::now() < deadline && !ready) {
            ready = n.catalogEventsAppendedForTest() > baseline;
            if (ready && waitCommit) {
                const CatalogView v = catalogOf(n);
                ready = v.hw >= v.leo;
            }
            if (!ready) std::this_thread::sleep_for(1ms);
        }
        if (!ready) return false;
        crash(id);
        return true;
    }
    /// The Task 22 seam the client tests use while their test thread is blocked in a blocking client call:
    /// arm the catalog leader (its answers held), and crash it from a watcher thread right after its next
    /// APPENDED + COMMITTED catalog event.  reapKilledLeader() joins the watcher and returns the crashed id
    /// (-1: nothing happened within `within`).
    void killCatalogLeaderAfterNextAppend(std::chrono::milliseconds within = 60s) {
        joinKillWatcher_();
        const int32_t leader = settledLeader();
        if (leader < 1) throw std::logic_error("killCatalogLeaderAfterNextAppend: no settled catalog leader");
        BrokerNode& n = node(leader);
        n.holdCatalogAnswersForTest(true);
        const uint64_t baseline = n.catalogEventsAppendedForTest();
        killWatcherNode_ = leader;
        killWatcher_ = std::thread([this, leader, baseline, within] {
            BrokerNode& l = *nodes[static_cast<std::size_t>(leader - 1)];
            const auto deadline = std::chrono::steady_clock::now() + within;
            while (std::chrono::steady_clock::now() < deadline) {
                if (l.catalogEventsAppendedForTest() > baseline) {
                    const CatalogView v = catalogOf(l);
                    if (v.hw >= v.leo) { l.crashForTest(); killWatcherResult_ = leader; return; }
                }
                std::this_thread::sleep_for(1ms);
            }
        });
    }
    int32_t reapKilledLeader() {
        joinKillWatcher_();
        const int32_t id = killWatcherResult_;
        killWatcherResult_ = -1;
        if (id > 0) nodes[static_cast<std::size_t>(id - 1)].reset();   // crashed already; release the husk
        return id;
    }
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
