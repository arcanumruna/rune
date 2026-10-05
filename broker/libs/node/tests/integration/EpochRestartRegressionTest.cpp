// EpochRestartRegressionTest.cpp — leader-epoch monotonicity across restarts
// (Kafka parity: every establishment of partition leadership uses a leader
// epoch strictly greater than any epoch already in any replica's log).
//
// Ground truth (dockerized DR-7/12/15 cold restarts): after a restart on the
// existing data volumes every USER partition re-established leadership at the
// SAME epoch its previous incarnation used (the replayed committed assignment).
// A leader writing a new history under an epoch another replica already holds
// a DIFFERENT history of is undetectable by KIP-101 epoch truncation: the
// divergent tail survives and is served when that replica leads.
//
// In-process 3-node clusters over real UDP + real data dirs, DEFAULT runtime
// (shard mode).  Every wait is a bounded poll of a real condition.
//
//   1. FullClusterColdRestartBumpsEveryUserPartitionEpochRegression
//   2. StaleSameEpochTailTruncatedAfterRestartRegression
//   3. SingleNodeRestartBumpsLeaderEpochRegression
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/LeaderEpochCache.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using rune::replication::Epoch;
using rune::replication::Offset;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_epr_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;
constexpr const char* kTopic     = "ep";

struct Ports {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static Ports discover(int nodes) {
        Ports p;
        auto blocks = freeUdpPortBlocks(3 * nodes, 1);
        std::vector<uint16_t> taken(blocks.begin(), blocks.end());
        auto single = [&taken] {
            for (int attempt = 0; attempt < 2000; ++attempt) {
                const uint16_t q = freeUdpPort();
                bool clash = q == 0;
                for (uint16_t t : taken) clash = clash || q == t;
                if (!clash) { taken.push_back(q); return q; }
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

// Default runtime (shard mode, one worker, thread placement), 3 nodes.
NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int32_t partitions,
                      int32_t minIsr = 2, int64_t electionMs = 150) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = electionMs; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = minIsr;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;
    for (int i = 0; i < 3; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = kTopic; t.partitionCount = partitions; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}

template <typename F>
bool eventually(F&& cond, std::chrono::milliseconds within = 30'000ms) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return cond();
}

struct RoleEpoch { bool present = false; Role role = Role::Follower; Epoch epoch = -1; };

RoleEpoch roleEpoch(BrokerNode& n, int32_t part) {
    return n.onOwnerThreadForTest(0, part, [](PartitionReplica* r) {
        RoleEpoch re;
        if (r) { re.present = true; re.role = r->role(); re.epoch = r->epoch(); }
        return re;
    });
}

/// Index (into `ns`, null entries skipped) of the node whose replica of `part`
/// is Leader, with its epoch; {-1, -1} when none.
std::pair<int, Epoch> leaderOf(const std::vector<BrokerNode*>& ns, int32_t part) {
    for (std::size_t i = 0; i < ns.size(); ++i) {
        if (!ns[i]) continue;
        const RoleEpoch re = roleEpoch(*ns[i], part);
        if (re.present && re.role == Role::Leader) return {static_cast<int>(i), re.epoch};
    }
    return {-1, -1};
}

/// The partition is settled on all three nodes: one leader, every replica at
/// the leader's epoch, the leader's ISR is the full replica set.  Returns the
/// leader index (or -1).
int settled(const std::vector<BrokerNode*>& ns, int32_t part) {
    const auto l = leaderOf(ns, part);
    if (l.first < 0) return -1;
    for (auto* n : ns) {
        if (!n) return -1;
        if (roleEpoch(*n, part).epoch != l.second) return -1;
    }
    if (gatewayIsrSizeOnOwner(*ns[static_cast<std::size_t>(l.first)], 0, part) != 3) return -1;
    return l.first;
}

/// Highest epoch any replica of `part` holds (role epoch or log lineage).
Epoch maxEpoch(const std::vector<BrokerNode*>& ns, int32_t part) {
    Epoch m = -1;
    for (auto* n : ns) {
        if (!n) continue;
        const Epoch e = n->onOwnerThreadForTest(0, part, [](PartitionReplica* r) -> Epoch {
            if (!r) return -1;
            const Epoch lineage = r->leaderEpochCacheForTest().latestEpoch();
            return r->epoch() > lineage ? r->epoch() : lineage;
        });
        if (e > m) m = e;
    }
    return m;
}

/// Highest leader epoch recorded in any replica's durable leader-epoch
/// checkpoint (nodes stopped) — every epoch a leader ever led at.
Epoch diskMaxEpoch(const std::vector<fs::path>& dirs, int32_t part) {
    Epoch m = -1;
    for (const auto& d : dirs) {
        rune::storage::LeaderEpochCache c;
        if (c.restoreFromFile((d / "topics" / kTopic / std::to_string(part) / "leader-epoch-checkpoint").string()))
            m = std::max<Epoch>(m, c.latestEpoch());
    }
    return m;
}

/// Construct + start the listed nodes (start() binds the leadership observer
/// to the calling thread: always the test thread).
void startAll(std::array<std::unique_ptr<BrokerNode>, 3>& ns, const std::array<NodeConfig, 3>& cfg,
              std::initializer_list<int> which) {
    for (int i : which) {
        ns[static_cast<std::size_t>(i)] = std::make_unique<BrokerNode>(cfg[static_cast<std::size_t>(i)]);
        ns[static_cast<std::size_t>(i)]->start();
    }
}

std::vector<uint8_t> payload(uint8_t fill) { return std::vector<uint8_t>(16, fill); }

/// Append `count` records (fill byte `fill`) on the leader of `part`; each is
/// retried until accepted (an incomplete leader / min-ISR gate refuses).
bool produce(BrokerNode& leader, int32_t part, uint8_t fill, int count) {
    for (int i = 0; i < count; ++i) {
        const auto p = payload(fill);
        const bool ok = eventually([&] {
            return appendOnOwner(leader, std::span<const uint8_t>(p.data(), p.size()),
                                 /*acksAll=*/false, 0, part) >= 0;
        }, 15'000ms);
        if (!ok) return false;
    }
    return true;
}

/// Append `count` records on WHICHEVER node currently leads `part` (startup
/// leadership churn may move it between records).
bool produceAny(const std::vector<BrokerNode*>& ns, int32_t part, uint8_t fill, int count) {
    for (int i = 0; i < count; ++i) {
        const auto p = payload(fill);
        const bool ok = eventually([&] {
            const auto l = leaderOf(ns, part);
            return l.first >= 0 &&
                   appendOnOwner(*ns[static_cast<std::size_t>(l.first)],
                                 std::span<const uint8_t>(p.data(), p.size()), false, 0, part) >= 0;
        }, 15'000ms);
        if (!ok) return false;
    }
    return true;
}

/// Settled AND every replica holds the first `k` records AND they committed.
int settledWith(const std::vector<BrokerNode*>& ns, int32_t part, Offset k) {
    const int l = settled(ns, part);
    if (l < 0) return -1;
    for (auto* n : ns) if (leoOnOwner(*n, 0, part) < k) return -1;
    return hwOnOwner(*ns[static_cast<std::size_t>(l)], 0, part) >= k ? l : -1;
}

/// First value byte of every record in [from, to) of the replica's log.
std::vector<uint8_t> valueBytes(BrokerNode& n, int32_t part, Offset from, Offset to) {
    const auto raw = readLogOnOwner(n, from, 1 << 20, 0, part);
    std::vector<uint8_t> out;
    std::size_t pos = 0;
    while (pos < raw.size()) {
        const std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
        rune::protocol::RecordBatchReader rd(raw.data() + pos, rem);
        if (!rd.ensureValid()) break;
        rd.forEach([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            if (off >= from && off < to && !v.empty()) out.push_back(static_cast<uint8_t>(v[0]));
        });
        const int32_t tl = rd.totalLength();
        if (tl <= 0) break;
        pos += static_cast<std::size_t>(tl);
    }
    return out;
}

/// Offline (node stopped): append `count` single-record batches stamped with
/// leader epoch `epoch` (fill byte `fill`) at the end of the partition's disk
/// log — an unreplicated tail of an earlier incarnation.
Offset appendStaleTail(const fs::path& dataDir, int32_t part, int32_t epoch, uint8_t fill, int count) {
    rune::storage::Log log((dataDir / "topics" / kTopic / std::to_string(part)).string(), 64LL * 1024 * 1024);
    log.recover();
    for (int i = 0; i < count; ++i) {
        const Offset base = log.logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, 0);
        const auto v = payload(fill);
        b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        auto batch = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(batch.data(), epoch);
        log.append(batch, base);
    }
    return log.logEndOffset();
}

std::vector<BrokerNode*> raw(const std::array<std::unique_ptr<BrokerNode>, 3>& ns) {
    return {ns[0].get(), ns[1].get(), ns[2].get()};
}

} // namespace

// ===========================================================================
// 1. Full cold restart: produce, stop every node, restart all on the same data
//    dirs — every user partition's leader epoch must exceed the one its
//    previous incarnation led (and wrote) at; no replica may lead at an old
//    epoch at any time after the restart.
// ===========================================================================
TEST(EpochRestartRegression, FullClusterColdRestartBumpsEveryUserPartitionEpochRegression) {
    constexpr int32_t kParts = 3;
    TmpDir d0{"c0"}, d1{"c1"}, d2{"c2"};
    const auto ports = Ports::discover(3);
    std::array<NodeConfig, 3> cfg{makeConfig(0, d0.path, ports, kParts),
                                  makeConfig(1, d1.path, ports, kParts),
                                  makeConfig(2, d2.path, ports, kParts)};
    std::array<Epoch, kParts> oldEpoch{};
    {
        std::array<std::unique_ptr<BrokerNode>, 3> ns;
        startAll(ns, cfg, {0, 1, 2});
        for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
        for (int32_t p = 0; p < kParts; ++p) {
            ASSERT_TRUE(produceAny(raw(ns), p, static_cast<uint8_t>(0xA0 + p), 3));
            ASSERT_TRUE(eventually([&] { return settledWith(raw(ns), p, 3) >= 0; }))
                << "partition " << p << " never settled with its records committed";
        }
        for (int32_t p = 0; p < kParts; ++p) oldEpoch[p] = maxEpoch(raw(ns), p);
        for (auto& n : ns) n->stop();
        for (int32_t p = 0; p < kParts; ++p)
            oldEpoch[p] = std::max(oldEpoch[p], diskMaxEpoch({d0.path, d1.path, d2.path}, p));
    }

    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    startAll(ns, cfg, {0, 1, 2});
    bool staleLead = false;
    std::string staleWhat;
    for (int32_t p = 0; p < kParts; ++p) {
        const bool bumped = eventually([&] {
            const auto l = leaderOf(raw(ns), p);
            if (l.first >= 0 && l.second <= oldEpoch[p] && !staleLead) {
                staleLead = true;
                staleWhat = "partition " + std::to_string(p) + " led by node " + std::to_string(l.first) +
                            " at epoch " + std::to_string(l.second) + " (previous incarnation: " +
                            std::to_string(oldEpoch[p]) + ")";
            }
            return l.first >= 0 && l.second > oldEpoch[p];
        });
        EXPECT_TRUE(bumped) << "partition " << p << " never led at an epoch > " << oldEpoch[p];
    }
    EXPECT_FALSE(staleLead) << "a replica led at a pre-restart epoch: " << staleWhat;
    // Fix round 1 (M2 / MAJOR-1): restarted ex-leaders hand leadership to an
    // in-sync peer; once they are back in the ISR leadership RE-SPREADS (each
    // node leads one partition again) — never stays collapsed.
    EXPECT_TRUE(eventually([&] {
        std::array<int, 3> led{};
        for (int32_t p = 0; p < kParts; ++p) {
            const int l = settled(raw(ns), p);
            if (l < 0) return false;
            ++led[static_cast<std::size_t>(l)];
        }
        return led[0] == 1 && led[1] == 1 && led[2] == 1;
    }, 60'000ms)) << "leadership never re-spread after the cold restart";
    // The leaders are writable and the history survived.
    for (int32_t p = 0; p < kParts; ++p) {
        ASSERT_TRUE(produceAny(raw(ns), p, 0xB0, 1));
        int lead = -1;
        EXPECT_TRUE(eventually([&] { lead = settledWith(raw(ns), p, 4); return lead >= 0; }))
            << "partition " << p << ": post-restart record never committed";
        ASSERT_GE(lead, 0);
        const auto l = std::pair<int, Epoch>{lead, 0};
        const auto v = valueBytes(*ns[l.first], p, 0, 4);
        ASSERT_EQ(v.size(), 4u);
        for (int i = 0; i < 3; ++i) EXPECT_EQ(v[static_cast<std::size_t>(i)], 0xA0 + p);
        EXPECT_EQ(v[3], 0xB0);
    }
    for (auto& n : ns) n->stop();
}

// ===========================================================================
// 2. Divergence: a follower holds an unreplicated tail written under the SAME
//    epoch its leader leads at (fabricated on disk while the cluster is down).
//    The leader and the third node restart first and commit NEW records over
//    those offsets; then the stale follower restarts.  It must truncate the
//    stale tail (the leader leads at a NEW epoch, so its end offset for the old
//    epoch is where the tail starts) and hold exactly the acked records.
// ===========================================================================
TEST(EpochRestartRegression, StaleSameEpochTailTruncatedAfterRestartRegression) {
    TmpDir d0{"s0"}, d1{"s1"}, d2{"s2"};
    const auto ports = Ports::discover(3);
    std::array<NodeConfig, 3> cfg{makeConfig(0, d0.path, ports, 1), makeConfig(1, d1.path, ports, 1),
                                  makeConfig(2, d2.path, ports, 1)};
    const std::array<fs::path, 3> dirs{d0.path, d1.path, d2.path};
    constexpr int kK = 4;      // committed before the restart
    constexpr int kStale = 3;  // fabricated stale tail on the follower
    constexpr int kNew = 5;    // acked after the restart (> kStale: covers the tail)
    int leader = -1, stale = -1, third = -1;
    Epoch oldEpoch = -1;
    {
        std::array<std::unique_ptr<BrokerNode>, 3> ns;
        startAll(ns, cfg, {0, 1, 2});
        for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
        ASSERT_TRUE(produceAny(raw(ns), 0, 0xC0, kK));
        ASSERT_TRUE(eventually([&] { leader = settledWith(raw(ns), 0, kK); return leader >= 0; }))
            << "the K records never settled on every replica";
        oldEpoch = roleEpoch(*ns[leader], 0).epoch;
        ASSERT_EQ(maxEpoch(raw(ns), 0), oldEpoch);
        // Stop the followers first (the stale-to-be one first, a non-controller
        // when possible: no controller change while the leader is up), then the
        // leader — with the leader alive and then no quorum left, nothing fails
        // the partition over during the shutdown.
        const int32_t controller = ns[0]->leaderId();
        stale = (leader + 1) % 3;
        if (stale == controller) stale = (leader + 2) % 3;
        third = 3 - leader - stale;
        ns[stale]->stop();
        ns[third]->stop();
        ns[leader]->stop();
    }
    ASSERT_EQ(diskMaxEpoch({d0.path, d1.path, d2.path}, 0), oldEpoch)
        << "leadership moved before the stop (precondition: one leader, one epoch)";
    ASSERT_EQ(appendStaleTail(dirs[stale], 0, static_cast<int32_t>(oldEpoch), 0xEE, kStale), kK + kStale);

    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    startAll(ns, cfg, {leader, third});
    ASSERT_TRUE(ns[leader]->awaitLeader(30s));
    std::pair<int, Epoch> l{-1, -1};
    ASSERT_TRUE(eventually([&] { l = leaderOf(raw(ns), 0); return l.first >= 0; }))
        << "no leader with the stale node down";
    ASSERT_TRUE(produce(*ns[l.first], 0, 0xD0, kNew));
    ASSERT_TRUE(eventually([&] { return hwOnOwner(*ns[l.first], 0, 0) >= kK + kNew; }))
        << "the new records never committed on {leader, third}";

    ns[stale] = std::make_unique<BrokerNode>(cfg[stale]);
    ns[stale]->start();
    const bool caughtUp = eventually([&] {
        return leoOnOwner(*ns[stale], 0, 0) >= kK + kNew &&
               valueBytes(*ns[stale], 0, kK, kK + kNew) == std::vector<uint8_t>(kNew, 0xD0);
    });
    const auto tail = valueBytes(*ns[stale], 0, kK, kK + kNew);
    std::string got;
    for (uint8_t b : tail) got += std::to_string(b) + " ";
    EXPECT_TRUE(caughtUp) << "the restarted replica must hold exactly the acked records at ["
                          << kK << ", " << kK + kNew << "), got value bytes: " << got
                          << "(0xEE=238 is the stale tail)";
    for (uint8_t b : tail) EXPECT_NE(b, 0xEE) << "stale same-epoch tail survived on the follower";
    const auto head = valueBytes(*ns[stale], 0, 0, kK);
    EXPECT_EQ(head, std::vector<uint8_t>(kK, 0xC0)) << "committed prefix damaged";
    for (auto& n : ns) if (n) n->stop();
}

// ===========================================================================
// 3. Single-node restart: restart ONLY the node leading a partition (not the
//    consensus leader, so the controller survives).  Whoever leads that
//    partition afterwards leads at an epoch > the pre-restart one, and the
//    restarted node never leads it at the old epoch.
// ===========================================================================
TEST(EpochRestartRegression, SingleNodeRestartBumpsLeaderEpochRegression) {
    constexpr int32_t kParts = 3;
    TmpDir d0{"n0"}, d1{"n1"}, d2{"n2"};
    const auto ports = Ports::discover(3);
    // A liveness timeout far above a node restart (~1 s in-process): the
    // controller does NOT fail the partition over while its leader restarts —
    // the restarted node finds itself the committed leader at its old epoch
    // (the docker DR window: a fast container restart).
    constexpr int64_t kSlowLivenessMs = 3000;
    std::array<NodeConfig, 3> cfg{makeConfig(0, d0.path, ports, kParts, 2, kSlowLivenessMs),
                                  makeConfig(1, d1.path, ports, kParts, 2, kSlowLivenessMs),
                                  makeConfig(2, d2.path, ports, kParts, 2, kSlowLivenessMs)};
    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    startAll(ns, cfg, {0, 1, 2});
    for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
    // Produce on every partition, then pick a settled partition led by a node
    // other than the controller (the controller survives the restart).
    for (int32_t p = 0; p < kParts; ++p) {
        ASSERT_TRUE(produceAny(raw(ns), p, 0xE0, 3));
        ASSERT_TRUE(eventually([&] { return settledWith(raw(ns), p, 3) >= 0; }));
    }
    int32_t part = -1;
    int victim = -1;
    Epoch oldEpoch = -1;
    ASSERT_TRUE(eventually([&] {
        const int32_t controller = ns[0]->leaderId();
        for (int32_t p = 0; p < kParts; ++p) {
            const int l = settledWith(raw(ns), p, 3);
            if (l >= 0 && l != controller && ns[1]->leaderId() == controller && ns[2]->leaderId() == controller) {
                part = p; victim = l; return true;
            }
        }
        return false;
    })) << "no settled partition led by a non-controller node";
    oldEpoch = maxEpoch(raw(ns), part);
    ns[victim]->stop();
    ns[victim] = std::make_unique<BrokerNode>(cfg[victim]);
    ns[victim]->start();

    bool staleLead = false;
    Epoch staleEpoch = -1;
    const bool bumped = eventually([&] {
        const RoleEpoch re = roleEpoch(*ns[victim], part);
        if (re.present && re.role == Role::Leader && re.epoch <= oldEpoch) { staleLead = true; staleEpoch = re.epoch; }
        const auto l = leaderOf(raw(ns), part);
        // Converged: the restarted node is back in the game (leading, or
        // following a leader) and the partition's leadership is newer.
        return l.first >= 0 && l.second > oldEpoch && re.present && re.epoch > oldEpoch;
    });
    EXPECT_TRUE(bumped) << "partition " << part << " leadership never moved past epoch " << oldEpoch;
    EXPECT_FALSE(staleLead) << "the restarted node led partition " << part << " at pre-restart epoch "
                            << staleEpoch << " (<= " << oldEpoch << ")";
    // Fix round 1 (M2 / MAJOR-1): the restarted ex-leader handed leadership to
    // an in-sync peer; once it is back in the ISR it leads its (preferred)
    // partition again — leadership re-spreads, never stays collapsed.
    EXPECT_TRUE(eventually([&] { return settledWith(raw(ns), part, 3) == victim; }, 60'000ms))
        << "leadership of partition " << part << " never returned to node " << victim;
    ASSERT_TRUE(produceAny(raw(ns), part, 0xE1, 1));
    EXPECT_TRUE(eventually([&] { return settledWith(raw(ns), part, 4) >= 0; }));
    for (auto& n : ns) n->stop();
}
