// ShardModeTest.cpp — in-process shard mode over REAL Aeron UDP: raw wire client
// → gateway (Connect) → worker shard ingress (Produce) → worker replication plane
// → ProduceResponse.  Legacy default must build no shard runtime.
#include <algorithm>
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string_view>
#include <thread>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_shard_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
// Legacy per-worker ingress base (also the default egress-stream anchor).  F25:
// a SHARD worker's ingress subscribes kClientStream on its own port — the
// stream the metadata advertises — never base + w.
constexpr int32_t kIngressStream = 111;

struct ShardPorts {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static ShardPorts discover(int nodes, int workers) {
        ShardPorts p;
        auto blocks = freeUdpPortBlocks(3 * nodes, workers);
        // Single ports must avoid every block and each other: a block's ports are
        // probed then released, so the OS may hand one out again.
        std::vector<std::pair<int, int>> taken;   // [lo, hi)
        for (uint16_t b : blocks) taken.push_back({b, b + workers});
        auto single = [&taken] {
            for (int attempt = 0; attempt < 2000; ++attempt) {
                const int q = freeUdpPort();
                bool clash = q == 0;
                for (const auto& [lo, hi] : taken) clash = clash || (q >= lo && q < hi);
                if (!clash) { taken.push_back({q, q + 1}); return static_cast<uint16_t>(q); }
            }
            throw std::runtime_error("ShardPorts: no collision-free UDP port");
        };
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(single()); p.client.push_back(single()); p.repl.push_back(single());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

NodeConfig makeShardConfig(int nodeId, const fs::path& dataDir, const ShardPorts& p, int nodes,
                           int32_t workers, bool fused) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = workers;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = fused ? NodeConfig::RuntimePlacement::Fused : NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = workers;
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "st"; t.partitionCount = 1; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gatewayChannel(const ShardPorts& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[node]);
}
std::string workerIngressChannel(const ShardPorts& p, int node, int w) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.ib[node] + w);
}
int64_t logEndOffsetAfterStop(const fs::path& dataDir) {
    rune::storage::Log log((dataDir / "topics" / "st" / "0").string(), 64LL * 1024 * 1024);
    log.recover();
    return log.logEndOffset();
}

} // namespace

// REGRESSION (found in the F19 gate): ShardPorts::discover drew the per-node
// single ports (consensus/client/replication) AFTER the per-worker base blocks
// without excluding them — an ephemeral single could land inside a block, and
// BrokerNode refused to start ("port collision: clientPort and workerRepl[w1]").
// Every discovered port must be distinct.
TEST(ShardPortsRegression, DiscoverYieldsPairwiseDisjointPorts) {
    constexpr int kNodes = 3, kWorkers = 4;
    for (int round = 0; round < 300; ++round) {
        const auto p = ShardPorts::discover(kNodes, kWorkers);
        std::vector<int> all;
        for (int i = 0; i < kNodes; ++i) {
            all.push_back(p.cons[i]); all.push_back(p.client[i]); all.push_back(p.repl[i]);
            for (int w = 0; w < kWorkers; ++w) {
                all.push_back(p.ib[i] + w); all.push_back(p.rb[i] + w); all.push_back(p.eg[i] + w);
            }
        }
        auto sorted = all;
        std::sort(sorted.begin(), sorted.end());
        ASSERT_EQ(std::adjacent_find(sorted.begin(), sorted.end()), sorted.end())
            << "round " << round << ": two roles share a port";
    }
}

TEST(ShardMode, SingleNodeProduceRoundTripOnWorkerShard) {
    TmpDir dir{"n0"}, cdir{"client"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 1u);
    EXPECT_TRUE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0" / "cnc.dat"));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0,
                                    /*acks=*/1, makeBatch(3));
        ASSERT_TRUE(ack.has_value()) << "no OK ack from the worker shard";
        EXPECT_EQ(ack->partition, 0);
    }
    node.stop();
    EXPECT_GE(logEndOffsetAfterStop(dir.path), 3);
}

TEST(ShardMode, ThreeNodeAcksAllCommitsOverShardReplicationPlanes) {
    TmpDir d0{"t0"}, d1{"t1"}, d2{"t2"}, cdir{"tc"};
    const auto ports = ShardPorts::discover(3, 1);
    BrokerNode n0{makeShardConfig(0, d0.path, ports, 3, 1, false)};
    BrokerNode n1{makeShardConfig(1, d1.path, ports, 3, 1, false)};
    BrokerNode n2{makeShardConfig(2, d2.path, ports, 3, 1, false)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms));
    int32_t leader = -1;
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        for (int i = 0; i < 200 && leader < 0; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            if (leader < 0) std::this_thread::sleep_for(50ms);
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " not committed";
        }
    }
    n0.stop(); n1.stop(); n2.stop();
    int atLeast20 = 0;
    for (const auto* d : {&d0, &d1, &d2}) atLeast20 += logEndOffsetAfterStop(d->path) >= 20 ? 1 : 0;
    EXPECT_GE(atLeast20, 2) << "every acked record must be on a majority";
}

TEST(ShardModeRegression, LegacyDefaultBuildsNoShardRuntime) {
    TmpDir dir{"legacy"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, false);
    cfg.workerMode = NodeConfig::WorkerMode::Legacy;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = false;
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    EXPECT_FALSE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0"));
    node.stop();
}

// Task 7/9 carry: the invoker drivers are pumped by the worker threads; stop()
// must join them and hand every driver to the stopping thread before closing
// (debug builds abort on a leaked driver).  Stopping from a thread that is
// neither the starter nor a worker must still close every shard driver.
TEST(ShardModeRegression, StopFromAnotherThreadClosesEveryShardDriver) {
    TmpDir dir{"stopthr"};
    const auto ports = ShardPorts::discover(1, 2);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 2, /*fused=*/false);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(node.shardDriverCountForTest(), 2u);
    std::thread stopper([&] { node.stop(); });
    stopper.join();
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    EXPECT_FALSE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0" / "cnc.dat"));
    EXPECT_FALSE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w1" / "cnc.dat"));
}

// ---------------------------------------------------------------------------
// Runtime worker-count symmetry (Task 6 ruling I2; Task 9 fix round 1 I1/I3).
// A worker-count-mismatched peer is treated exactly like an unreachable peer on
// that plane.  All waits below are bounded polls of a real condition (hard cap)
// read on the OWNING worker thread; no negative-by-timeout assertions.
// ---------------------------------------------------------------------------
namespace {
using PS = WorkerReplicationTransport::PeerPlaneState;
constexpr int32_t kAsymPartitions = 8;

/// Bounded condition poll: cond() is re-evaluated until true (hard cap ~10 s).
template <class F>
bool eventually(F&& cond, int maxIters = 2'000) {
    for (int i = 0; i < maxIters; ++i) {
        if (cond()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return false;
}

/// A partition this node routes to worker w (-1 if none).
int32_t partitionOnWorker(BrokerNode& n, int32_t w) {
    for (int32_t p = 0; p < kAsymPartitions; ++p)
        if (n.workerForPartitionForTest(0, p) == w) return p;
    return -1;
}

/// Run f(plane) on worker w's thread (the plane's owner).
template <class F>
auto onPlane(BrokerNode& n, int32_t w, F f) {
    auto* plane = n.perWorkerPlaneForTest(w);
    const int32_t part = partitionOnWorker(n, w);
    if (!plane || part < 0) throw std::runtime_error("no plane / partition for worker " + std::to_string(w));
    return n.onOwnerThreadForTest(0, part, [plane, f](rune::replication::PartitionReplica*) { return f(*plane); });
}

PS peerState(BrokerNode& n, int32_t w, int32_t peer) {
    return onPlane(n, w, [peer](WorkerReplicationTransport& t) { return t.peerPlaneState(peer); });
}
uint64_t refusals(BrokerNode& n, int32_t w) {
    return onPlane(n, w, [](WorkerReplicationTransport& t) { return t.planeRefusals(); });
}

/// Static (preferred) leader of topic 0 partition p for an n-node cluster:
/// exactly what the metadata leader emits (BrokerNode: rf = cluster size).
int32_t staticLeader(int32_t p, int nodes) {
    rune::metadata::TopicSpec t; t.topicId = 0; t.partitionCount = kAsymPartitions;
    t.rf = static_cast<int16_t>(nodes);
    std::vector<rune::metadata::NodeId> live;
    for (int i = 0; i < nodes; ++i) live.push_back(i);
    for (const auto& r : rune::metadata::StaticBalancer::assign({t}, live))
        if (r.rec.partition == p) return r.rec.leaderNode;
    return -1;
}

/// Advertised leader of topic 0 partition p, polled until it equals `want`.
bool leaderBecomes(ShardTestClient& c, const ShardPorts& ports, int64_t sid, int32_t p, int32_t want) {
    return eventually([&] {
        for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid))
            if (pi.topicId == 0 && pi.partition == p && pi.leaderId == want) return true;
        return false;
    }, 400);
}

NodeConfig asymConfig(int32_t nodeId, const fs::path& dir, const ShardPorts& ports, int nodes,
                      int32_t workers) {
    auto c = makeShardConfig(nodeId, dir, ports, nodes, workers, false);
    c.topics[0].partitionCount = kAsymPartitions;
    for (auto& p : c.peers) p.workerCount = (p.id == nodeId) ? workers : 0;   // peers unknown at parse
    return c;
}
} // namespace

TEST(ShardModeRegression, AsymmetricWorkerCountRefusedAtRuntime) {
    TmpDir d0{"asym0"}, d1{"asym1"};
    const auto ports = ShardPorts::discover(2, 2);
    const auto c0 = asymConfig(0, d0.path, ports, 2, /*workers=*/1);
    const auto c1 = asymConfig(1, d1.path, ports, 2, /*workers=*/2);
    ASSERT_FALSE(c0.validateShardConfig().has_value()) << "parse-time check cannot see the mismatch";
    ASSERT_FALSE(c1.validateShardConfig().has_value());
    BrokerNode n0{c0}, n1{c1};
    n0.start(); n1.start();
    ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Refused; }))
        << "node 0 never refused the 2-worker peer on plane 0";
    ASSERT_TRUE(eventually([&] { return peerState(n1, 0, 0) == PS::Refused; }))
        << "node 1 never refused the 1-worker peer on plane 0";
    EXPECT_GE(refusals(n0, 0), 1u);
    n1.stop(); n0.stop();
}

TEST(ShardModeRegression, PeerRestartStillMismatchedStaysRefused) {
    TmpDir d0{"rst0"}, d1{"rst1"}, cdir{"rstc"};
    const auto ports = ShardPorts::discover(2, 2);
    const auto c0 = asymConfig(0, d0.path, ports, 2, 1);
    const auto c1 = asymConfig(1, d1.path, ports, 2, 2);
    BrokerNode n0{c0};
    n0.start();
    {
        BrokerNode n1{c1};
        n1.start();
        ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Refused; }));
        n1.stop();
    }
    const uint64_t before = refusals(n0, 0);                 // read AFTER the old peer is gone
    BrokerNode n1b{c1};                                       // restarted: new image session, same wrong W
    n1b.start();
    ASSERT_TRUE(eventually([&] { return refusals(n0, 0) > before; }))
        << "the restarted peer's new session was never re-evaluated";
    EXPECT_EQ(peerState(n0, 0, 1), PS::Refused) << "still mismatched: must stay refused (no split)";
    ASSERT_TRUE(eventually([&] { return peerState(n1b, 0, 0) == PS::Refused; }));
    // An affected partition on the one plane both nodes share (node 1's shard 0),
    // statically led by node 0 (node 0 never stops, so it leads it throughout).
    // Records acked by node 0 after both sides refused never reach node 1, and
    // node 0 never applies anything for partitions only node 1 could have led.
    int32_t part = -1, peerLed = -1;
    for (int32_t p = 0; p < kAsymPartitions; ++p) {
        if (n1b.workerForPartitionForTest(0, p) != 0) continue;
        if (staticLeader(p, 2) == 0 && part < 0) part = p;
        if (staticLeader(p, 2) == 1 && peerLed < 0) peerLed = p;
    }
    ASSERT_GE(part, 0) << "no node-0-led partition on node 1's shard 0";
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        ASSERT_TRUE(leaderBecomes(c, ports, sid, part, 0));
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, part,
                                    /*acks=leader*/0, makeBatch(3));
        ASSERT_TRUE(ack.has_value());
    }
    EXPECT_GE(rune::node::test::leoOnOwner(n0, 0, part), 3);
    EXPECT_EQ(rune::node::test::leoOnOwner(n1b, 0, part), 0) << "records crossed a refused plane (split)";
    if (peerLed >= 0)
        EXPECT_EQ(rune::node::test::leoOnOwner(n0, 0, peerLed), 0) << "node 0 applied a refused peer's records";
    n1b.stop(); n0.stop();
}

TEST(ShardModeRegression, PeerFixedAndRestartedHeals) {
    TmpDir d0{"heal0"}, d1{"heal1"}, d2{"heal2"}, cdir{"healc"};
    const auto ports = ShardPorts::discover(3, 2);
    const auto c0 = asymConfig(0, d0.path, ports, 3, 1);
    const auto c2 = asymConfig(2, d2.path, ports, 3, 1);
    BrokerNode n0{c0}, n2{c2};
    n0.start(); n2.start();
    {
        BrokerNode bad{asymConfig(1, d1.path, ports, 3, /*workers=*/2)};
        bad.start();
        ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Refused; }));
        bad.stop();
    }
    BrokerNode n1{asymConfig(1, d1.path, ports, 3, /*workers=*/1)};   // fixed RUNE_WORKERS, restarted
    n1.start();
    ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Verified; }))
        << "node 0 did not heal without being restarted";
    ASSERT_TRUE(eventually([&] { return peerState(n1, 0, 0) == PS::Verified; }));
    ASSERT_TRUE(eventually([&] { return peerState(n1, 0, 2) == PS::Verified; }));
    ASSERT_TRUE(n0.awaitLeader(15'000ms) || n2.awaitLeader(15'000ms) || n1.awaitLeader(15'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        int32_t leader = -1;
        ASSERT_TRUE(eventually([&] {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            return leader >= 0;
        }, 400)) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, 0, 0,
                                    /*acks=all*/1, makeBatch(4));
        ASSERT_TRUE(ack.has_value()) << "acks=all did not complete after the plane healed";
    }
    ASSERT_TRUE(eventually([&] { return rune::node::test::leoOrMinusOne(n1, 0, 0) >= 4; }))
        << "replication never reached the healed node";
    n1.stop(); n2.stop(); n0.stop();
}

TEST(ShardModeRegression, LargerWorkerCountExtraShardsDoNotSplit) {
    TmpDir d0{"xtra0"}, d1{"xtra1"}, cdir{"xtrac"};
    const auto ports = ShardPorts::discover(2, 2);
    auto c0 = asymConfig(0, d0.path, ports, 2, 1);
    auto c1 = asymConfig(1, d1.path, ports, 2, 2);
    c0.replicaLagMaxMs = c1.replicaLagMaxMs = 500;          // unreachable follower leaves the ISR fast
    BrokerNode n1{c1}, n0{c0};
    n1.start(); n0.start();
    // Pin the scenario: a partition whose static (preferred) leader is node 1 and
    // which node 1 routes to shard 1 — a shard index the 1-worker peer lacks.
    int32_t part = -1;
    for (int32_t p = 0; p < kAsymPartitions && part < 0; ++p)
        if (staticLeader(p, 2) == 1 && n1.workerForPartitionForTest(0, p) == 1) part = p;
    ASSERT_GE(part, 0) << "no partition statically led by node 1 on its shard 1";
    ShardTestClient c(cdir.path);
    const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid0, 0);
    ASSERT_TRUE(leaderBecomes(c, ports, sid0, part, 1)) << "node 1 never became the partition's leader";
    // The leader's ISR shrinks to itself: the follower's shard 0 can never reach
    // node 1's plane 1.  Condition, read on node 1's shard 1 (the owner).
    ASSERT_TRUE(eventually([&] { return rune::node::test::isrSizeOnOwner(n1, 0, part) == 1; }))
        << "the unreachable follower never left the ISR";
    const int64_t sid = c.connect(gatewayChannel(ports, 1), kClientStream);
    ASSERT_GT(sid, 0);
    constexpr int64_t kCorr = 50'000;
    ASSERT_TRUE(c.produce(workerIngressChannel(ports, 1, 1), kClientStream, sid, kCorr, 0, part,
                          /*acks=all*/1, makeBatch(1)));
    auto r = c.awaitProduceResponse(kCorr);
    ASSERT_TRUE(r.has_value()) << "acks=all got no answer (silent hang)";
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation))
        << "ISR {1} < minIsr 2 must be reported to the client";
    EXPECT_EQ(rune::node::test::leoOnOwner(n0, 0, part), 0) << "no split: node 0 never applied it";
    n0.stop(); n1.stop();
}

// Fix round 1 (I2): no long startup phase may run between shard-runtime creation
// and the shard runners (nothing pumps an invoker driver/client in between; a
// gap > the client liveness timeout kills the client — Aeron's default handler
// even exit()s the process).  Stall the offset/txn/metadata/admin rebuild phase
// for 2x a small liveness timeout; every shard must still serve.
TEST(ShardModeRegression, SlowStartupPhaseDoesNotKillShardClient) {
    TmpDir dir{"slowstart"}, cdir{"slowstartc"};
    constexpr int32_t kWorkers = 2;
    const auto ports = ShardPorts::discover(1, kWorkers);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, kWorkers, /*fused=*/false);
    cfg.topics[0].partitionCount = 8;
    constexpr int64_t kLivenessNs = 2'000'000'000;           // > driver timer interval (1 s)
    cfg.shardClientLivenessTimeoutNs = kLivenessNs;
    BrokerNode node{cfg};
    node.setStartupPhaseHookForTest([](std::string_view phase) {
        if (phase == "build-gateway")                         // the injected fault: a slow rebuild
            std::this_thread::sleep_for(std::chrono::nanoseconds{2 * kLivenessNs});
    });
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    for (int32_t w = 0; w < kWorkers; ++w) {
        int32_t part = -1;
        for (int32_t p = 0; p < cfg.topics[0].partitionCount && part < 0; ++p)
            if (node.workerForPartitionForTest(0, p) == w) part = p;
        ASSERT_GE(part, 0) << "no partition routed to worker " << w;
        EXPECT_FALSE(node.onOwnerThreadForTest(0, part, [&node, w](rune::replication::PartitionReplica*) {
            return node.shardClientClosedForTest(w);
        })) << "shard " << w << " client died during startup";
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, w), kClientStream, sid, 0, part,
                                    /*acks=*/0, makeBatch(2), /*maxAttempts=*/20);
        EXPECT_TRUE(ack.has_value()) << "shard " << w << " does not serve after a slow startup phase";
    }
    node.stop();
}

// Fix round 2 (item 5): shards are attached one after another at startup.  While
// a LATER shard resolves (here: a stalled resolve of shard 2 for 2x the client
// liveness timeout), every other shard's driver + client must keep being pumped,
// so total liveness is independent of the shard count and of resolve time.
TEST(ShardModeRegression, SequentialShardResolveKeepsEarlierShardsAlive) {
    TmpDir dir{"seqres"}, cdir{"seqresc"};
    constexpr int32_t kWorkers = 3;
    const auto ports = ShardPorts::discover(1, kWorkers);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, kWorkers, /*fused=*/false);
    cfg.topics[0].partitionCount = 12;
    constexpr int64_t kLivenessNs = 2'000'000'000;           // > driver timer interval (1 s)
    cfg.shardClientLivenessTimeoutNs = kLivenessNs;
    BrokerNode node{cfg};
    node.setShardResolveStallForTest(kWorkers - 1, std::chrono::milliseconds{2 * kLivenessNs / 1'000'000});
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    for (int32_t w = 0; w < kWorkers; ++w) {
        int32_t part = -1;
        for (int32_t p = 0; p < cfg.topics[0].partitionCount && part < 0; ++p)
            if (node.workerForPartitionForTest(0, p) == w) part = p;
        ASSERT_GE(part, 0) << "no partition routed to worker " << w;
        EXPECT_FALSE(node.onOwnerThreadForTest(0, part, [&node, w](rune::replication::PartitionReplica*) {
            return node.shardClientClosedForTest(w);
        })) << "shard " << w << " client died while a later shard resolved";
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, w), kClientStream, sid, 0, part,
                                    /*acks=*/0, makeBatch(2), /*maxAttempts=*/20);
        EXPECT_TRUE(ack.has_value()) << "shard " << w << " does not serve";
    }
    node.stop();
}

// ---------------------------------------------------------------------------
// Task 10 fix 1 — fsyncgate fail-stop + io backend selection, end to end.
// ---------------------------------------------------------------------------
namespace {
struct HaltCapture {
    std::mutex m;
    std::condition_variable cv;
    int code = 0;
    static void handler(void* ctx, int exitCode) noexcept {
        auto* self = static_cast<HaltCapture*>(ctx);
        { std::lock_guard<std::mutex> g(self->m); self->code = exitCode; }
        self->cv.notify_all();
    }
    int await(std::chrono::milliseconds cap) {
        std::unique_lock<std::mutex> l(m);
        cv.wait_for(l, cap, [this] { return code != 0; });
        return code;
    }
};
} // namespace

TEST(ShardModeRegression, StorageFailureHaltsBrokerWithExitCode) {
    // A background fsync error on a shard's log: the log is permanently failed,
    // the worker signals the runtime over its return ring, the runtime halts the
    // broker with kExitStorageFailure (the test hook replaces the real exit), and
    // the failed partition refuses every later produce with StorageError.
    TmpDir dir{"halt"}, cdir{"client"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0, 1, makeBatch(2)));
    ASSERT_TRUE(node.onOwnerThreadForTest(0, 0, [&node](rune::replication::PartitionReplica*) {
        node.workerForTest(0)->forceFlushErrorForTest(EIO);
        return true;
    }));
    ASSERT_TRUE(c.produce(workerIngressChannel(ports, 0, 0), kClientStream, sid, 9001, 0, 0, 1, makeBatch(1)));
    ASSERT_TRUE(c.awaitProduceResponse(9001).has_value());
    EXPECT_EQ(halt.await(10'000ms), BrokerNode::kExitStorageFailure)
        << "the next background fsync fails → the broker is halted with EX_IOERR";
    ASSERT_TRUE(c.produce(workerIngressChannel(ports, 0, 0), kClientStream, sid, 9002, 0, 0, 1, makeBatch(1)));
    const auto refused = c.awaitProduceResponse(9002);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(pc::ErrorCode::StorageError));
    node.stop();
}

TEST(ShardModeRegression, RefusesIoBackendThatCannotSubmit) {
    TmpDir dir{"iobk"};
    const auto ports = ShardPorts::discover(1, 1);
    ::setenv("RUNE_IO_BACKEND", "bogus", 1);
    {
        BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
        try {
            node.start();
            ADD_FAILURE() << "shard mode must refuse a backend that cannot submit";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string_view(e.what()).find("RUNE_IO_BACKEND"), std::string_view::npos) << e.what();
        }
        node.stop();
    }
    ::unsetenv("RUNE_IO_BACKEND");
}

TEST(ShardModeRegression, NoHaltHandlerExitsWithStorageFailureCode) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    TmpDir dir{"nohalt"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    EXPECT_EXIT({
        BrokerNode node{cfg};
        node.raiseStorageFailureForTest("/data/x", EIO);
    }, ::testing::ExitedWithCode(BrokerNode::kExitStorageFailure), "");
}

TEST(ShardModeRegression, StartRefusesWhenALogFailedRecovery) {
    TmpDir dir{"recfail"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.setRecoveryFaultForTest(EIO);
    EXPECT_THROW(node.start(), std::runtime_error) << "a log that failed recovery never serves";
    EXPECT_EQ(halt.await(1'000ms), BrokerNode::kExitStorageFailure) << "through the halt path (exit 74)";
    node.stop();
}

TEST(ShardModeRegression, StartRefusesWhenTheSchemaLogFailedRecovery) {
    // Task 10 fix 3 / I1: the schema log lives outside the replica registry.
    TmpDir dir{"schemafail"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.setSchemaRecoveryFaultForTest(EIO);
    EXPECT_THROW(node.start(), std::runtime_error) << "a failed schema log never serves";
    EXPECT_EQ(halt.await(1'000ms), BrokerNode::kExitStorageFailure);
    node.stop();
}

TEST(ShardMode, MetadataAdvertisesTheLeaderWorkerEndpoint) {
    TmpDir dir{"md"}, cdir{"mdc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    std::string ep;
    // Condition wait: metadata is served once the committed routing image lands.
    ASSERT_TRUE(eventually([&] {
        for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid))
            if (pi.topicId == 0 && pi.partition == 0) ep = pi.ingressEndpoint;
        return !ep.empty();
    }, 400)) << "no ingressEndpoint advertised for topic 0 partition 0";
    EXPECT_EQ(ep, "127.0.0.1:" + std::to_string(ports.ib[0]));
    ASSERT_TRUE(c.produceUntilOk("aeron:udp?endpoint=" + ep, kClientStream, sid, 0, 0, 1, makeBatch(1)))
        << "the advertised endpoint is the live worker ingress";
    node.stop();
}

// ---------------------------------------------------------------------------
// Task 11 — RUNE_RUNTIME_PLACEMENT=fused (spec §3.2): the runtime (consensus,
// metadata, internal partitions, gateway control) has NO thread of its own; its
// duty cycle — own invoker driver + own client conductor + NodeRuntimeAgent —
// runs at the end of worker 0's pass.  `thread` placement is unchanged.
// ---------------------------------------------------------------------------
namespace {
bool runtimeDriverClosed(const NodeConfig& cfg) {
    return !fs::exists(fs::path(cfg.effectiveAeronDir()) / "cnc.dat");
}
} // namespace

TEST(ShardModeFusedRegression, ThreadPlacementStillStartsTheRuntimeThread) {
    TmpDir dir{"thrplc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_TRUE(node.runtimeThreadStartedForTest()) << "thread placement = today's behaviour";
    node.stop();
}

TEST(ShardModeFusedRegression, SingleNodeRunsRuntimeOnWorkerZero) {
    TmpDir dir{"fused"}, cdir{"fc"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms)) << "consensus must run inside worker 0's loop";
    EXPECT_FALSE(node.runtimeThreadStartedForTest());
    EXPECT_EQ(node.runtimeConsensusKindForTest(), NodeRuntimeAgent::ConsensusKind::Single)
        << "worker 0's pass must call the concrete consensus agent (no virtual dispatch)";
    EXPECT_TRUE(node.runtimePathIsConcreteForTest())
        << "consensus publisher/sink + internal-replica transports must be concrete (no virtual)";
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0) << "gateway Connect served by the fused runtime";
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0, 1, makeBatch(2));
        ASSERT_TRUE(ack.has_value());
    }
    node.stop();
    EXPECT_TRUE(runtimeDriverClosed(cfg)) << "the runtime's invoker driver must close on stop()";
    EXPECT_GE(logEndOffsetAfterStop(dir.path), 2);
}

TEST(ShardModeFusedRegression, ReturnRingPressureNeverDeadlocksTheFusedLoop) {
    TmpDir dir{"fusedring"}, cdir{"frc"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    // Integration wave 2 (T11 ⇄ F15 S3): the return ring must now hold one full
    // 512 KiB PushReadResult (startup refuses less) and be >= the ingress ring,
    // so the smallest legal ring is 8 MiB.  Return-ring pressure is forced by a
    // 64 KiB per-ack reservation (~128 reservable acks per ring) and the
    // minimum owed-return cap — intake still has to skip, on the one thread
    // that also drains ret_.
    cfg.workerReturnRingBytes  = 8 * 1024 * 1024;
    cfg.workerIngressRingBytes = 8 * 1024 * 1024;
    cfg.workerAckSlotBytes     = 64 * 1024;
    cfg.workerMaxOwedReturns   = static_cast<int32_t>(Worker::kMinOwedReturns);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0, 0, makeBatch(1)));
    constexpr int kN = 2000;
    for (int i = 0; i < kN; ++i)
        ASSERT_TRUE(c.produce(workerIngressChannel(ports, 0, 0), kClientStream, sid, 10'000 + i, 0, 0, 0, makeBatch(1)));
    int ok = 0;
    for (int i = 0; i < kN; ++i) {
        auto a = c.awaitProduceResponse(10'000 + i);
        ok += (a && a->errorCode == 0) ? 1 : 0;
    }
    EXPECT_EQ(ok, kN) << "producer and consumer of ret_ share one thread — no path may wait on the other";
    node.stop();
}

TEST(ShardModeFusedRegression, ThreeNodeAcksAllCommitsWithFusedRuntimes) {
    TmpDir d0{"ft0"}, d1{"ft1"}, d2{"ft2"}, cdir{"ftc"};
    const auto ports = ShardPorts::discover(3, 1);
    BrokerNode n0{makeShardConfig(0, d0.path, ports, 3, 1, /*fused=*/true)};
    BrokerNode n1{makeShardConfig(1, d1.path, ports, 3, 1, /*fused=*/true)};
    BrokerNode n2{makeShardConfig(2, d2.path, ports, 3, 1, /*fused=*/true)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms)) << "consensus over UDP between three fused runtimes";
    EXPECT_FALSE(n0.runtimeThreadStartedForTest());
    EXPECT_EQ(n0.runtimeConsensusKindForTest(), NodeRuntimeAgent::ConsensusKind::Multi)
        << "worker 0's pass must call the concrete consensus agent (no virtual dispatch)";
    EXPECT_TRUE(n0.runtimePathIsConcreteForTest())
        << "consensus publisher/sink + internal-replica transports must be concrete (no virtual)";
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        int32_t leader = -1;
        ASSERT_TRUE(eventually([&] {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            return leader >= 0;
        }, 400)) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " not committed";
        }
    }
    n0.stop(); n1.stop(); n2.stop();
    int atLeast20 = 0;
    for (const auto* d : {&d0, &d1, &d2}) atLeast20 += logEndOffsetAfterStop(d->path) >= 20 ? 1 : 0;
    EXPECT_GE(atLeast20, 2) << "every acked record must be on a majority";
}

// The runtime's invoker driver + client exist from step 3 of start() (consensus,
// replication and gateway are built on them), long before worker 0 runs.  A slow
// startup phase (offset/txn/metadata rebuilds) must not leave them unpumped for
// longer than the client liveness timeout — the driver would time the runtime
// client out (its subscriptions/publications vanish; nothing serves).
TEST(ShardModeFusedRegression, SlowStartupPhaseDoesNotKillTheRuntimeClient) {
    TmpDir dir{"fslow"}, cdir{"fslowc"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    constexpr int64_t kLivenessNs = 2'000'000'000;           // > driver timer interval (1 s)
    cfg.shardClientLivenessTimeoutNs = kLivenessNs;          // every invoker driver of the node
    BrokerNode node{cfg};
    node.setStartupPhaseHookForTest([](std::string_view phase) {
        if (phase == "build-gateway")                         // the injected fault: a slow rebuild
            std::this_thread::sleep_for(std::chrono::nanoseconds{2 * kLivenessNs});
    });
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0) << "the runtime client died during startup (gateway no longer serves)";
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0, 1, makeBatch(1)));
    node.stop();
}

TEST(ShardModeFusedRegression, StopFromAnotherThreadClosesTheRuntimeDriver) {
    TmpDir dir{"fstopthr"};
    const auto ports = ShardPorts::discover(1, 2);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 2, /*fused=*/true);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    std::thread stopper([&] { node.stop(); });
    stopper.join();
    EXPECT_TRUE(runtimeDriverClosed(cfg)) << "runtime driver leaked (debug builds abort)";
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
}

TEST(ShardModeFusedRegression, StartFailureStillClosesTheRuntimeDriver) {
    TmpDir dir{"fstartfail"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    BrokerNode node{cfg};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.setRecoveryFaultForTest(EIO);
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_EQ(halt.await(1'000ms), BrokerNode::kExitStorageFailure);
    std::thread stopper([&] { node.stop(); });
    stopper.join();
    EXPECT_TRUE(runtimeDriverClosed(cfg)) << "runtime driver leaked after a failed start";
}

// Bounded runtime duty cycle (Task 11: under fused placement every runtime cycle
// is part of worker 0's pass).  The routing image is rebuilt + republished only
// when its inputs change (committed assignment map / topic+node config) — never
// once per cycle (a heap allocation + an O(partitions) rebuild per cycle).
TEST(ShardModeFusedRegression, RoutingImageIsRepublishedOnlyWhenItsInputsChange) {
    for (const bool fused : {true, false}) {
        SCOPED_TRACE(fused ? "fused" : "thread");
        TmpDir dir{"rimg"};
        const auto ports = ShardPorts::discover(1, 1);
        BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, fused)};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        // Condition wait: the committed assignment map has landed and the steady-
        // state image is published (publish count stops moving across a cycle).
        uint64_t before = 0;
        ASSERT_TRUE(eventually([&] {
            const uint64_t a = node.onControlThreadForTest([&] { return node.routingImagePublishesForTest(); });
            const uint64_t b = node.onControlThreadForTest([&] { return node.routingImagePublishesForTest(); });
            before = b;
            return a > 0 && a == b;
        })) << "the routing image is republished on every runtime cycle";
        uint64_t cycles0 = node.onControlThreadForTest([&] { return node.runtimeCyclesForTest(); });
        uint64_t cycles = cycles0;
        for (int i = 0; i < 2'000 && cycles - cycles0 < 200; ++i)   // >= 200 more runtime cycles
            cycles = node.onControlThreadForTest([&] { return node.runtimeCyclesForTest(); });
        ASSERT_GE(cycles - cycles0, 200u);
        EXPECT_EQ(node.onControlThreadForTest([&] { return node.routingImagePublishesForTest(); }), before)
            << "no input changed — no rebuild";
        node.stop();
    }
}

// Fused placement runs the failover/rebalance scan as an Aeron slow tick (not
// every cycle): a dead partition leader must still fail over to a survivor, and
// acks=all must commit on the new leader's worker shard.
TEST(ShardModeFusedRegression, PartitionLeaderFailsOverBetweenFusedRuntimes) {
    TmpDir d0{"ff0"}, d1{"ff1"}, d2{"ff2"}, cdir{"ffc"};
    const auto ports = ShardPorts::discover(3, 1);
    auto nodes = std::vector<std::unique_ptr<BrokerNode>>{};
    for (int i = 0; i < 3; ++i)
        nodes.push_back(std::make_unique<BrokerNode>(
            makeShardConfig(i, (i == 0 ? d0 : i == 1 ? d1 : d2).path, ports, 3, 1, /*fused=*/true)));
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
    ShardTestClient c(cdir.path);
    auto leaderSeenBy = [&](int via, int64_t sid) {
        int32_t l = -1;
        for (const auto& pi : c.metadata(gatewayChannel(ports, via), kClientStream, sid))
            if (pi.topicId == 0 && pi.partition == 0) l = pi.leaderId;
        return l;
    };
    const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid0, 0);
    int32_t oldLeader = -1;
    ASSERT_TRUE(eventually([&] { return (oldLeader = leaderSeenBy(0, sid0)) >= 0; }, 400));
    {
        const int64_t sid = c.connect(gatewayChannel(ports, oldLeader), kClientStream);
        ASSERT_GT(sid, 0);
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, oldLeader, 0), kClientStream, sid, 0, 0,
                                     /*acks=all*/1, makeBatch(2)));
    }
    nodes[static_cast<std::size_t>(oldLeader)]->stop();                  // the partition leader dies
    const int via = oldLeader == 0 ? 1 : 0;
    const int64_t sidVia = c.connect(gatewayChannel(ports, via), kClientStream);
    ASSERT_GT(sidVia, 0);
    int32_t newLeader = -1;
    ASSERT_TRUE(eventually([&] {
        newLeader = leaderSeenBy(via, sidVia);
        return newLeader >= 0 && newLeader != oldLeader;
    })) << "partition 0 never failed over from dead node " << oldLeader;
    const int64_t sid = c.connect(gatewayChannel(ports, newLeader), kClientStream);
    ASSERT_GT(sid, 0);
    EXPECT_TRUE(c.produceUntilOk(workerIngressChannel(ports, newLeader, 0), kClientStream, sid, 0, 0,
                                 /*acks=all*/1, makeBatch(2)))
        << "acks=all must commit on the surviving pair";
    for (auto& n : nodes) n->stop();
}

// Harness fix (Task 11 TSan run: "refusing to start — port collision with
// co-located peer 2: our workerEgressControl[w0] and its consensusPort both use
// 53473"): single ports were drawn independently of the per-worker blocks, so a
// port freed after probing a block could be handed out again.  Every port and
// block ShardPorts::discover returns must be pairwise disjoint.
TEST(ShardTestHarnessRegression, DiscoveredPortsNeverCollide) {
    for (int round = 0; round < 300; ++round) {
        constexpr int kNodes = 3, kWorkers = 4;
        const auto p = ShardPorts::discover(kNodes, kWorkers);
        std::vector<std::pair<int, int>> ranges;   // [lo, hi)
        for (int i = 0; i < kNodes; ++i) {
            for (uint16_t s : {p.cons[i], p.client[i], p.repl[i]}) ranges.push_back({s, s + 1});
            for (uint16_t b : {p.ib[i], p.rb[i], p.eg[i]}) ranges.push_back({b, b + kWorkers});
        }
        for (std::size_t a = 0; a < ranges.size(); ++a)
            for (std::size_t b = a + 1; b < ranges.size(); ++b)
                ASSERT_FALSE(ranges[a].first < ranges[b].second && ranges[b].first < ranges[a].second)
                    << "round " << round << ": ports " << ranges[a].first << " and " << ranges[b].first << " overlap";
    }
}

// ===========================================================================
// Integration wave 2 (F18 ⇄ F15 S3 ⇄ F19) — REGRESSION: ONE batch limit, and a
// batch at EXACTLY that limit must be accepted, replicated AND delivered to a
// push consumer under the DEFAULT config (3 nodes, default rings / Aeron term,
// legacy worker mode).  Before the unification the limits disagreed: produce
// intake admitted a batch up to one replication frame while the default 4 MiB
// egress term (512 KiB frames) could not push it — the subscription was
// error-parked with MessageTooLarge at delivery.  limit + 1 is refused
// MessageTooLarge at intake (never appended).
// ===========================================================================
namespace {
NodeConfig makeDefaultConfig(int nodeId, const fs::path& dataDir, const ShardPorts& p, int nodes) {
    NodeConfig cfg;                                // DEFAULT ring / term / worker sizes
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;        // what parseNodeConfig derives (majority)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "st"; t.partitionCount = 1; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}
// A record batch of EXACTLY n wire bytes (one record; value length searched).
std::vector<uint8_t> batchOfExactly(std::size_t n) {
    auto build = [](std::size_t v) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", std::string(v, 'x'), 1000);
        return b.build();
    };
    const std::size_t over = build(0).size();
    if (n < over) return {};
    for (std::size_t v = n - over > 16 ? n - over - 16 : 0; v <= n; ++v) {
        auto b = build(v);
        if (b.size() == n) return b;
        if (b.size() > n) break;
    }
    return {};
}
} // namespace

namespace {
void runBatchAtExactLimitAcceptedReplicatedAndPushed(int nodes) {
    TmpDir d0{"bl0"}, d1{"bl1"}, d2{"bl2"}, cdir{"blc"};
    const auto ports = ShardPorts::discover(nodes, 1);
    const NodeConfig c0 = makeDefaultConfig(0, d0.path, ports, nodes);
    ASSERT_EQ(c0.aeronTermBufferBytes, NodeConfig{}.aeronTermBufferBytes) << "default term";
    ASSERT_EQ(c0.workerMode, NodeConfig::WorkerMode::Legacy) << "default worker mode";
    const std::size_t limit = static_cast<std::size_t>(c0.produceIntakeLimit());
    ASSERT_EQ(limit, Worker::maxAcceptableBatchBytes(
        static_cast<std::size_t>(c0.workerIngressRingBytes), static_cast<std::size_t>(c0.workerReturnRingBytes),
        static_cast<std::size_t>(c0.replicationOutboundRingBytes),
        static_cast<std::size_t>(c0.aeronTermBufferBytes), static_cast<std::size_t>(c0.aeronTermBufferBytes),
        static_cast<std::size_t>(c0.aeronTermBufferBytes)));
    // Default 4 MiB term: the egress Records frame (term / 8 = 512 KiB) is one
    // of the binding hops — the case the unification closes.
    ASSERT_LE(limit + rune::node::kRecordsFixedBytes, 512u * 1024u);

    std::vector<std::unique_ptr<BrokerNode>> ns;
    ns.push_back(std::make_unique<BrokerNode>(c0));
    if (nodes == 3) {
        ns.push_back(std::make_unique<BrokerNode>(makeDefaultConfig(1, d1.path, ports, 3)));
        ns.push_back(std::make_unique<BrokerNode>(makeDefaultConfig(2, d2.path, ports, 3)));
    }
    for (auto& n : ns) n->start();
    ASSERT_TRUE(ns[0]->awaitLeader(15'000ms));
    for (auto& n : ns)
        EXPECT_EQ(n->gatewayForTest()->maxProduceBatchBytes(), limit) << "one limit on every node";

    const auto atLimit = batchOfExactly(limit);
    ASSERT_EQ(atLimit.size(), limit);
    const auto over = batchOfExactly(limit + 1);
    ASSERT_EQ(over.size(), limit + 1);
    {
        // The client's own produce frame (limit + 1 + Produce framing) must fit
        // ITS publication (term / 8): size the client term from the limit.
        int32_t clientTerm = 8 * 1024 * 1024;
        while (static_cast<std::size_t>(clientTerm) / 8 < limit + 4096) clientTerm *= 2;
        ShardTestClient c(cdir.path, clientTerm);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        int32_t leader = -1;
        for (int i = 0; i < 200 && leader < 0; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            if (leader < 0) std::this_thread::sleep_for(50ms);
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const std::string gw = gatewayChannel(ports, leader);
        const int64_t sid = c.connect(gw, kClientStream);
        ASSERT_GT(sid, 0);

        // Accepted (+ replicated, 3 nodes: acks=all with minIsr 2 answers OK only
        // once a follower holds the batch).
        auto ack = c.produceUntilOk(gw, kClientStream, sid, 0, 0, /*acks=all*/1, atLimit);
        ASSERT_TRUE(ack.has_value()) << "a batch at exactly the limit must be accepted (acks=all)";
        EXPECT_EQ(ack->baseOffset, 0);

        // limit + 1: refused at intake, non-retriable, never appended.
        ASSERT_TRUE(c.produce(gw, kClientStream, sid, 900001, 0, 0, /*acks=all*/1, over));
        auto tooLarge = c.awaitProduceResponse(900001);
        ASSERT_TRUE(tooLarge.has_value());
        EXPECT_EQ(tooLarge->errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));

        // Delivered to a push consumer, byte-identical — never error-parked.
        ASSERT_TRUE(c.subscribe(gw, kClientStream, sid, 0, 0, /*fromOffset=*/0));
        const bool got = c.pollUntil([&] {
            for (const auto& r : c.records()) if (r.baseOffset == 0) return true;
            for (const auto& e : c.errors()) if (e.code == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) return true;
            return false;
        });
        ASSERT_TRUE(got) << "nothing pushed for offset 0";
        for (const auto& e : c.errors())
            EXPECT_NE(e.code, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge))
                << "the push subscription was error-parked at the limit batch: " << e.message;
        ASSERT_FALSE(c.records().empty());
        EXPECT_EQ(c.records().front().baseOffset, 0);
        EXPECT_EQ(c.records().front().bytes.size(), limit);
        auto expected = atLimit;
        for (int i = 0; i < 8; ++i) expected[i] = 0;   // baseOffset 0 restamped (bytes 0..7)
        EXPECT_EQ(c.records().front().bytes, expected) << "pushed batch not byte-identical";
    }
    for (auto& n : ns) n->stop();
    int replicas = 0;
    for (const auto* d : {&d0, &d1, &d2})
        if (d == &d0 || nodes == 3) replicas += logEndOffsetAfterStop(d->path) == 1 ? 1 : 0;
    EXPECT_GE(replicas, nodes == 3 ? 2 : 1)
        << "the acks=all limit batch must be on a majority (and limit+1 nowhere)";
}
} // namespace

TEST(ThreeNodeDefaultConfigBatchLimitRegression, BatchAtExactLimitIsAcceptedReplicatedAndPushed) {
    runBatchAtExactLimitAcceptedReplicatedAndPushed(3);
}

// Single-node default config — the case that actually diverged before the
// merge: F18 skipped the replication terms single-node, so intake admitted up
// to the op-ring frame (~4 MiB) while the default egress term pushes at most
// ~512 KiB → a batch in between was accepted and then error-parked at delivery.
// Replication terms now apply even single-node (the limit must not change when
// a node joins a cluster) and the egress frame is a term of its own.
TEST(SingleNodeDefaultConfigBatchLimitRegression, BatchAtExactLimitIsAcceptedAndPushed) {
    runBatchAtExactLimitAcceptedReplicatedAndPushed(1);
}

// Task 13 (spec §3.4): the produce ack comes from the WORKER's own publication to
// the client session (announced before ConnectResponse), not relayed by the
// runtime; a session closed by Disconnect is forgotten and a new one is
// announced and acked.  Real client ↔ real broker over UDP, fused placement.
TEST(ShardModeRegression, WorkerSideAcksEndToEndAndSessionReconnect) {
    TmpDir dir{"wsa"}, cdir{"wsac"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid1 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid1, 0);
        auto ack1 = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid1, 0, 0, 1, makeBatch(2));
        ASSERT_TRUE(ack1.has_value());
        EXPECT_NE(c.ackImageSessionId(ack1->correlationId), 0);
        EXPECT_NE(c.ackImageSessionId(ack1->correlationId), c.connectImageSessionId())
            << "the ack must arrive on the worker's own publication, not the runtime's session publication";
        ASSERT_TRUE(c.disconnect(gatewayChannel(ports, 0), kClientStream, sid1));
        const int64_t sid2 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid2, 0);
        ASSERT_NE(sid2, sid1);
        auto ack2 = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid2, 0, 0, 1, makeBatch(2));
        ASSERT_TRUE(ack2.has_value()) << "a new session is announced and acked by the worker after the old one closed";
        EXPECT_NE(c.ackImageSessionId(ack2->correlationId), c.connectImageSessionId());
    }
    node.stop();
}

// Thread placement + 2 workers: the runtime on its own thread announces to EVERY
// worker over the cross-thread rings before it accepts the session.
TEST(ShardModeRegression, WorkerSideAcksEndToEndThreadPlacement) {
    TmpDir dir{"wsat"}, cdir{"wsatc"};
    const auto ports = ShardPorts::discover(1, 2);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 2, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0, 1, makeBatch(4));
        ASSERT_TRUE(ack.has_value());
        EXPECT_NE(c.ackImageSessionId(ack->correlationId), c.connectImageSessionId());
    }
    node.stop();
}

// F25 (REGRESSION, k3s 3-node shard bench hang): shard mode advertised
// ingressStreamId = workerIngressBase + owner, subscribed ONLY on the worker's
// own port.  A client that ignores the additive ingressEndpoint trailer (spec
// §2: older clients must keep working) routes (leader NODE endpoint, advertised
// stream) — nothing subscribed it → NOT_CONNECTED forever.  Both routes a
// client can derive from the metadata must reach a live subscriber and every
// acks=all batch must be acked, on partitions led by more than one node.  The
// test routes ONLY from what the MetadataResponse advertises (no hard-coded
// stream — the harness used to pair the worker endpoint with a known stream,
// which masked this).
TEST(ShardModeRegression, AdvertisedIngressRoutesAreLiveForEndpointUnawareAndAwareClients) {
    constexpr int kNodes = 3, kBatches = 6;
    constexpr int32_t kWorkers = 2, kParts = 6;
    TmpDir d0{"f25n0"}, d1{"f25n1"}, d2{"f25n2"}, cdir{"f25cl"};
    const auto ports = ShardPorts::discover(kNodes, kWorkers);
    const fs::path* dirs[] = {&d0.path, &d1.path, &d2.path};
    std::vector<std::unique_ptr<BrokerNode>> nodes;
    for (int i = 0; i < kNodes; ++i) {
        auto cfg = makeShardConfig(i, *dirs[i], ports, kNodes, kWorkers, /*fused=*/true);
        cfg.topics[0].partitionCount = kParts;
        nodes.push_back(std::make_unique<BrokerNode>(cfg));
    }
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        std::vector<PartitionInfo> parts;
        std::set<int32_t> leaders;
        ASSERT_TRUE(eventually([&] {
            parts.clear(); leaders.clear();
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.leaderId >= 0) { parts.push_back(pi); leaders.insert(pi.leaderId); }
            return parts.size() == static_cast<std::size_t>(kParts) && leaders.size() > 1;
        }, 400)) << "partitions never led by more than one node";
        std::map<int32_t, int64_t> sidOn;   // sessions are per node: one on every leader
        for (int32_t l : leaders) {
            sidOn[l] = c.connect(gatewayChannel(ports, l), kClientStream);
            ASSERT_GT(sidOn[l], 0) << "connect to leader " << l;
        }
        for (const bool endpointAware : {false, true}) {
            for (const auto& pi : parts) {
                if (endpointAware) ASSERT_FALSE(pi.ingressEndpoint.empty()) << "partition " << pi.partition;
                const std::string ch = endpointAware ? "aeron:udp?endpoint=" + pi.ingressEndpoint
                                                     : gatewayChannel(ports, pi.leaderId);
                for (int b = 0; b < kBatches; ++b) {
                    auto ack = c.produceUntilOk(ch, pi.ingressStreamId, sidOn[pi.leaderId], 0, pi.partition,
                                                /*acks=all*/1, makeBatch(2), /*maxAttempts=*/3);
                    ASSERT_TRUE(ack.has_value())
                        << (endpointAware ? "endpoint-aware" : "endpoint-UNAWARE") << " client: partition "
                        << pi.partition << " (leader " << pi.leaderId << ", advertised stream "
                        << pi.ingressStreamId << ") batch " << b << " never acked";
                }
            }
        }
    }
    for (auto& n : nodes) n->stop();
}

// T13 fix round 2 (finding 1, critical) + fix round 3 (re-review 2, finding 3):
// C1 on the GATEWAY-ROUTED path, end to end.  A client that ignores
// ingressEndpoint (every Produce to the node endpoint → gateway → op ring) and
// stops reading its acks must be paused ALONE: the worker reports it Held and
// the gateway ABORTs its produce frames (asserted via the gateway's pause
// counter — the defining mechanism), while the worker's op drain never runs
// out of ack room.  Another client keeps producing; the slow one loses nothing
// once it reads.  No elapsed-time decisions: A stops offering when an offer is
// refused AND the gateway has paused it.
TEST(ShardModeRegression, SlowEndpointUnawareClientCannotStarveTheShard) {
    TmpDir dir{"slowcl"}, ad{"slowa"}, bd{"slowb"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    const std::string gw = gatewayChannel(ports, 0);
    auto gatewayPauses = [&] {
        return node.onControlThreadForTest([&] { return node.gatewayForTest()->heldProduceAbortsForTest(); });
    };
    auto ackRoomOpPauses = [&] {
        return node.onOwnerThreadForTest(0, 0, [&](rune::replication::PartitionReplica&) {
            return node.workerForTest(0)->ackRoomOpPausesForTest();
        });
    };
    {
        ShardTestClient a(ad.path), b(bd.path);
        const int64_t sidA = a.connect(gw, kClientStream);
        ASSERT_GT(sidA, 0);
        const auto batch = makeBatch(1);
        ASSERT_TRUE(a.produceUntilOk(gw, kClientStream, sidA, 0, 0, /*acks=*/1, batch));
        constexpr int64_t c0 = 50'000'000;
        const int64_t n = a.offerUntilPaused(gw, kClientStream, [&](int64_t i, std::vector<uint8_t>& out) {
            pc::Produce m;
            m.sessionId = sidA; m.correlationId = c0 + i; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
            m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
            m.recordBatch = std::span<const uint8_t>(batch);
            out.resize(m.encodedSize());
            m.encode(out.data());
        }, /*maxFrames=*/40'000, [&] { return gatewayPauses() > 0; });
        ASSERT_LT(n, 40'000) << "client A was never paused (gateway ABORT never engaged)";
        EXPECT_GT(gatewayPauses(), 0u);

        const int64_t sidB = b.connect(gw, kClientStream);
        ASSERT_GT(sidB, 0) << "a new session is accepted while A is paused";
        for (int k = 0; k < 5; ++k)
            ASSERT_TRUE(b.produceUntilOk(gw, kClientStream, sidB, 0, 0, /*acks=*/1, batch, /*maxAttempts=*/3))
                << "client B's produce " << k << " starved behind client A's unread acks";
        EXPECT_EQ(ackRoomOpPauses(), 0u) << "the worker's op drain never ran out of ack room";

        for (int64_t c = c0; c < c0 + n; ++c)                 // A reads again: nothing lost
            ASSERT_TRUE(a.awaitProduceResponse(c).has_value()) << "ack " << c - c0 << " of " << n;
    }
    node.stop();
}

// ===========================================================================
// F15 S5 (§2.5, I6) — END TO END over real Aeron UDP: the owning worker's psm_
// is the sole producer-state authority in the DEFAULT (Legacy) config and in
// shard mode under BOTH runtime placements.  An idempotent producer's retry of
// an appended batch is deduped whichever route it takes (gateway-routed op, or
// the shard's own ingress): one copy, answered with the original offset; the
// next sequence is accepted.  (Idempotent produces are refused NotLeader —
// retriable — until the new leader's producer state is built.)
// ===========================================================================
namespace {
std::vector<uint8_t> idempotentBatch(int64_t pid, int16_t epoch, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.append("k", "idem", 1000);
    return b.build();
}
enum class PsmE2eMode { Legacy, ShardThread, ShardFused };
void runWorkerPsmDedupEndToEnd(PsmE2eMode mode) {
    TmpDir dir{"psm"}, cdir{"psmc"};
    const auto ports = ShardPorts::discover(1, 1);
    const NodeConfig cfg = mode == PsmE2eMode::Legacy
        ? makeDefaultConfig(0, dir.path, ports, 1)
        : makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/mode == PsmE2eMode::ShardFused);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    constexpr int64_t kPid = 4242;
    {
        ShardTestClient c(cdir.path);
        const std::string gw = gatewayChannel(ports, 0);
        const int64_t sid = c.connect(gw, kClientStream);
        ASSERT_GT(sid, 0);
        // One idempotent produce; NotLeader (bootstrap / producer state still
        // building) is retried — bounded, progress-driven, no sleep.
        auto send = [&](const std::string& ch, int32_t seq) -> std::optional<pc::ProduceResponse> {
            const auto batch = idempotentBatch(kPid, 0, seq);
            for (int a = 0; a < 200; ++a) {
                const int64_t corr = c.nextCorrelationId();
                if (!c.produceIdempotent(ch, kClientStream, sid, corr, 0, 0, /*acks=all*/1, kPid, 0, seq, batch))
                    continue;
                auto ack = c.awaitProduceResponse(corr);
                if (ack && ack->errorCode == static_cast<int16_t>(pc::ErrorCode::NotLeader)) continue;
                return ack;
            }
            return std::nullopt;
        };
        auto first = send(gw, 0);
        ASSERT_TRUE(first.has_value());
        ASSERT_EQ(first->errorCode, 0);
        EXPECT_EQ(first->baseOffset, 0);
        auto retryGw = send(gw, 0);                            // gateway-routed retry
        ASSERT_TRUE(retryGw.has_value());
        EXPECT_EQ(retryGw->errorCode, 0);
        EXPECT_EQ(retryGw->baseOffset, 0) << "the gateway-routed retry was not deduped by the worker";
        std::string next = gw;
        if (mode != PsmE2eMode::Legacy) {
            const std::string ing = workerIngressChannel(ports, 0, 0);
            auto retryIng = send(ing, 0);                      // the shard's own ingress
            ASSERT_TRUE(retryIng.has_value());
            EXPECT_EQ(retryIng->errorCode, 0);
            EXPECT_EQ(retryIng->baseOffset, 0) << "the shard-ingress retry was not deduped";
            next = ing;
        }
        auto second = send(next, 1);
        ASSERT_TRUE(second.has_value());
        EXPECT_EQ(second->errorCode, 0) << "seq 1 refused (20 = OutOfOrderSequence)";
        EXPECT_EQ(second->baseOffset, 1);
    }
    node.stop();
    EXPECT_EQ(logEndOffsetAfterStop(dir.path), 2) << "a retried idempotent batch was appended twice";
}
} // namespace

TEST(WorkerPsmEndToEndRegression, DefaultLegacyConfigDedupsOnTheOwningWorker) {
    runWorkerPsmDedupEndToEnd(PsmE2eMode::Legacy);
}
TEST(WorkerPsmEndToEndRegression, ShardThreadPlacementDedupsOnTheOwningWorker) {
    runWorkerPsmDedupEndToEnd(PsmE2eMode::ShardThread);
}
TEST(WorkerPsmEndToEndRegression, ShardFusedPlacementDedupsOnTheOwningWorker) {
    runWorkerPsmDedupEndToEnd(PsmE2eMode::ShardFused);
}
