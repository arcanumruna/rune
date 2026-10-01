// ShardModeTest.cpp — in-process shard mode over REAL Aeron UDP: raw wire client
// → gateway (Connect) → worker shard ingress (Produce) → worker replication plane
// → ProduceResponse.  Legacy default must build no shard runtime.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
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
constexpr int32_t kIngressStream = 111;

struct ShardPorts {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static ShardPorts discover(int nodes, int workers) {
        ShardPorts p;
        auto blocks = freeUdpPortBlocks(3 * nodes, workers);
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(freeUdpPort()); p.client.push_back(freeUdpPort()); p.repl.push_back(freeUdpPort());
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
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid, 0, 0,
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
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kIngressStream, sid, 0, 0,
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
    TmpDir d0{"rst0"}, d1{"rst1"};
    const auto ports = ShardPorts::discover(2, 2);
    const auto c0 = asymConfig(0, d0.path, ports, 2, 1);
    const auto c1 = asymConfig(1, d1.path, ports, 2, 2);
    BrokerNode n0{c0};
    n0.start();
    uint64_t before = 0;
    {
        BrokerNode n1{c1};
        n1.start();
        ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Refused; }));
        before = refusals(n0, 0);
        n1.stop();
    }
    BrokerNode n1b{c1};                                   // restarted: new image session, same wrong W
    n1b.start();
    ASSERT_TRUE(eventually([&] { return refusals(n0, 0) > before; }))
        << "the restarted peer's new session was never re-evaluated";
    EXPECT_EQ(peerState(n0, 0, 1), PS::Refused) << "still mismatched: must stay refused (no split)";
    ASSERT_TRUE(eventually([&] { return peerState(n1b, 0, 0) == PS::Refused; }));
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
        auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kIngressStream, sid, 0, 0,
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
    BrokerNode n0{c0}, n1{c1};
    n0.start(); n1.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms) || n1.awaitLeader(15'000ms));
    const int32_t part = partitionOnWorker(n1, 1);          // a shard index the 1-worker peer lacks
    ASSERT_GE(part, 0);
    ShardTestClient c(cdir.path);
    const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid0, 0);
    int32_t leader = -1;
    ASSERT_TRUE(eventually([&] {
        for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
            if (pi.topicId == 0 && pi.partition == part) leader = pi.leaderId;
        return leader >= 0;
    }, 400));
    const int32_t w = (leader == 1) ? 1 : 0;                // the leader's owning shard
    const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
    ASSERT_GT(sid, 0);
    // Every produce gets an answer (never a silent hang); once the unreachable
    // follower has left the ISR the answer is MinIsrViolation (minIsr=2).
    bool sawMinIsr = false;
    for (int attempt = 0; attempt < 40 && !sawMinIsr; ++attempt) {
        const int64_t corr = 50'000 + attempt;
        ASSERT_TRUE(c.produce(workerIngressChannel(ports, leader, w), kIngressStream + w, sid, corr, 0, part,
                              /*acks=all*/1, makeBatch(1)));
        auto r = c.awaitProduceResponse(corr);
        ASSERT_TRUE(r.has_value()) << "produce " << attempt << " got no answer (silent hang)";
        sawMinIsr = r->errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation);
        if (!sawMinIsr) std::this_thread::sleep_for(50ms);   // pacing only; outcome asserted below
    }
    EXPECT_TRUE(sawMinIsr) << "acks=all never reported the shrunken ISR";
    // No split: the other node never applied this partition's records.
    BrokerNode& follower = (leader == 1) ? n0 : n1;
    EXPECT_LE(follower.onOwnerThreadForTest(0, part, [](rune::replication::PartitionReplica& r) {
                  return r.logEndOffset(); }), 0);
    n1.stop(); n0.stop();
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
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, w), kIngressStream + w, sid, 0, part,
                                    /*acks=*/0, makeBatch(2), /*maxAttempts=*/20);
        EXPECT_TRUE(ack.has_value()) << "shard " << w << " does not serve after a slow startup phase";
    }
    node.stop();
}
