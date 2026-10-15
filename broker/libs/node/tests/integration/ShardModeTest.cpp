// ShardModeTest.cpp — in-process shard mode over REAL Aeron UDP: raw wire client
// → gateway (Connect) → worker shard ingress (Produce) → worker replication plane
// → ProduceResponse.  Legacy default must build no shard runtime.
#include <algorithm>
#include "support/TestCluster.h"
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/consensus/IdleStrategy.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/ThreadAffinity.h>
#include <rune/metadata/ShardRouter.h>
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
#include <fstream>
#include <sstream>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string_view>
#include <thread>
#include <pthread.h>
#include <sys/syscall.h>
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
    cfg.workerCoreBase = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = workers;
        pc.egressControlBasePort = p.eg[i];   // advertised (clients dial the leader's)
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    return cfg;
}

/// The test topic "st" (topics exist only through DDL: defined through the catalog once the nodes are up).
TopicSpec shardTopic(int32_t partitions = 1) {
    TopicSpec t; t.name = "st"; t.partitionCount = partitions;
    return t;
}
int32_t defineShardTopic(std::vector<BrokerNode*> nodes, int32_t partitions = 1) {
    return rune::node::test::TestCluster(std::move(nodes)).defineTopic(shardTopic(partitions));
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&n0, &n1, &n2}), 0);
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

// Catalog history Task 18 (R2): topics defined by DDL are RECOVERED from __catalog on a full restart and are
// assigned + routable without any DDL being re-sent.  3 brokers, 1 worker each; every broker is destroyed and
// rebuilt on the SAME data dirs; nothing is defined the second time.
TEST(CatalogBootstrapRoutingImageRegression, CatalogRecoveredTopicsAreAssignedAndRoutableAfterFullRestartRegression) {
    struct Def { const char* name; int32_t parts; const char* group; };
    const Def defs[] = {{"orders", 3, "shop"}, {"payments", 3, "shop"}, {"chat", 2, ""}};
    constexpr int kNodes = 3;
    TmpDir d0{"cb0"}, d1{"cb1"}, d2{"cb2"}, cdir{"cbc"};
    const fs::path dirs[kNodes] = {d0.path, d1.path, d2.path};
    const auto ports = ShardPorts::discover(kNodes, 1);

    std::map<std::string, int32_t> ids;
    {   // incarnation 1: define by DDL
        std::vector<std::unique_ptr<BrokerNode>> nodes;
        for (int i = 0; i < kNodes; ++i) nodes.push_back(std::make_unique<BrokerNode>(makeShardConfig(i, dirs[i], ports, kNodes, 1, false)));
        for (auto& n : nodes) n->start();
        ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
        TestCluster cluster({nodes[0].get(), nodes[1].get(), nodes[2].get()});
        for (const Def& d : defs) {
            TopicSpec t; t.name = d.name; t.partitionCount = d.parts; t.coPartitionGroup = d.group;
            const int32_t id = cluster.defineTopic(t);
            ASSERT_GE(id, 0) << "define " << d.name;
            ids[d.name] = id;
        }
        for (auto& n : nodes) n->stop();
    }   // every broker destroyed
    // Forget the replicated metadata log (committed assignments): the ONLY thing left to place these topics
    // is the catalog recovered at boot.  Without this the persisted assignments would mask a boot that
    // forgot to configure the catalog's topics.
    for (const auto& d : dirs) {
        std::error_code ec;
        fs::remove_all(d / "__cluster_metadata", ec);
        EXPECT_FALSE(ec) << "removing the metadata log of " << d << ": " << ec.message();
    }

    // incarnation 2: same data dirs, same ports, NO define
    std::vector<std::unique_ptr<BrokerNode>> nodes;
    for (int i = 0; i < kNodes; ++i) nodes.push_back(std::make_unique<BrokerNode>(makeShardConfig(i, dirs[i], ports, kNodes, 1, false)));
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));

    // (a) every node holds an assignment for every (topic, partition), within a bounded wait
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    for (int i = 0; i < kNodes; ++i) {
        auto* ms = nodes[i]->metadataServiceForTest();
        if (ms == nullptr) { ADD_FAILURE() << "node " << i << " has no MetadataService"; continue; }
        for (const Def& d : defs)
            for (int32_t p = 0; p < d.parts; ++p) {
                while (ms->assignment(ids[d.name], p) == nullptr && std::chrono::steady_clock::now() < deadline)
                    std::this_thread::sleep_for(20ms);
                if (ms->assignment(ids[d.name], p) == nullptr)
                    ADD_FAILURE() << "node " << i << " has no assignment for " << d.name << "/" << p << " after restart";
            }
    }
    if (::testing::Test::HasFailure()) { for (auto& n : nodes) n->stop(); return; }

    // (b) co-partition: orders p and payments p share a leader; (c) identical assignment on all nodes
    for (const Def& d : defs)
        for (int32_t p = 0; p < d.parts; ++p) {
            const auto* ref = nodes[0]->metadataServiceForTest()->assignment(ids[d.name], p);
            for (int i = 1; i < kNodes; ++i) {
                const auto* o = nodes[i]->metadataServiceForTest()->assignment(ids[d.name], p);
                EXPECT_TRUE(o->leaderNode == ref->leaderNode && o->replicas == ref->replicas)
                    << "node " << i << " disagrees on " << d.name << "/" << p;
            }
        }
    for (int32_t p = 0; p < 3; ++p)
        EXPECT_EQ(nodes[0]->metadataServiceForTest()->assignment(ids["orders"], p)->leaderNode,
                  nodes[0]->metadataServiceForTest()->assignment(ids["payments"], p)->leaderNode)
            << "co-partition group 'shop' split at partition " << p;

    // (d) a produce to every (topic, partition) through its leader succeeds.  The restart fence bumps the
    // epoch (leadership moves) after boot, so the leader is re-read on every attempt; bounded by one deadline.
    {
        ShardTestClient c(cdir.path);
        const auto produceDeadline = std::chrono::steady_clock::now() + 20s;
        for (const Def& d : defs)
            for (int32_t p = 0; p < d.parts; ++p) {
                bool acked = false;
                int32_t leader = -1;
                while (!acked && std::chrono::steady_clock::now() < produceDeadline) {
                    leader = nodes[0]->metadataServiceForTest()->assignment(ids[d.name], p)->leaderNode;
                    if (leader < 0 || leader >= kNodes) { std::this_thread::sleep_for(50ms); continue; }
                    const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
                    if (sid <= 0) continue;
                    acked = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, ids[d.name], p,
                                             /*acks=all*/1, makeBatch(1), /*maxAttempts=*/20).has_value();
                }
                EXPECT_TRUE(acked) << "produce to " << d.name << "/" << p << " (last leader " << leader
                                   << ") never acked after the restart";
            }
    }
    for (auto& n : nodes) n->stop();
}

// A4: in fused placement the runtime (control plane) runs on worker 0's thread
// and rides worker 0's OWN driver + client — exactly one Aeron driver per worker,
// no extra runtime driver (the extra one cost ~15 us of polling every pass).
TEST(ShardModeRegression, FusedWorkerZeroSharesTheRuntimeDriverRegression) {
    TmpDir dir{"fz"}, cdir{"fzc"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    EXPECT_EQ(node.shardDriverCountForTest(), 0u)
        << "worker 0 must not own a second driver: it shares the runtime's";
    EXPECT_FALSE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0" / "cnc.dat"));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0,
                                    /*acks=*/1, makeBatch(3));
        ASSERT_TRUE(ack.has_value()) << "no OK ack from the fused worker shard";
    }
    node.stop();
    EXPECT_GE(logEndOffsetAfterStop(dir.path), 3);
}

TEST(ShardModeRegression, FusedTwoWorkersOwnOneExtraDriverRegression) {
    TmpDir dir{"f2"};
    const auto ports = ShardPorts::discover(1, 2);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 2, /*fused=*/true);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    EXPECT_EQ(node.shardDriverCountForTest(), 1u) << "worker 1 owns its driver; worker 0 shares the runtime's";
    node.stop();
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
}

TEST(ShardModeRegression, FusedThreeNodeAcksAllCommitsRegression) {
    TmpDir d0{"ft0"}, d1{"ft1"}, d2{"ft2"}, cdir{"ftc"};
    const auto ports = ShardPorts::discover(3, 1);
    BrokerNode n0{makeShardConfig(0, d0.path, ports, 3, 1, true)};
    BrokerNode n1{makeShardConfig(1, d1.path, ports, 3, 1, true)};
    BrokerNode n2{makeShardConfig(2, d2.path, ports, 3, 1, true)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms));
    ASSERT_EQ(defineShardTopic({&n0, &n1, &n2}), 0);
    EXPECT_EQ(n0.shardDriverCountForTest() + n1.shardDriverCountForTest() + n2.shardDriverCountForTest(), 0u);
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
            ASSERT_TRUE(ack.has_value()) << "fused acks=all batch " << b << " not committed";
        }
    }
    n0.stop(); n1.stop(); n2.stop();
    int atLeast20 = 0;
    for (const auto* d : {&d0, &d1, &d2}) atLeast20 += logEndOffsetAfterStop(d->path) >= 20 ? 1 : 0;
    EXPECT_GE(atLeast20, 2) << "every acked record must be on a majority";
}

#if defined(__linux__)
namespace {
/// "0-2,5" -> {0,1,2,5} (a /proc Cpus_allowed_list).
std::vector<int32_t> parseCpuList(const std::string& s) {
    std::vector<int32_t> out;
    std::stringstream ss(s);
    std::string part;
    while (std::getline(ss, part, ',')) {
        if (part.empty()) continue;
        const auto dash = part.find('-');
        const int lo = std::stoi(part.substr(0, dash));
        const int hi = dash == std::string::npos ? lo : std::stoi(part.substr(dash + 1));
        for (int c = lo; c <= hi; ++c) out.push_back(c);
    }
    return out;
}
struct TaskAffinity { std::string comm; std::vector<int32_t> cpus; };
/// Every thread of this process: tid -> (comm, allowed CPUs).  Threads that exit mid-scan are skipped.
std::map<std::string, TaskAffinity> scanTasks() {
    std::map<std::string, TaskAffinity> out;
    for (const auto& e : fs::directory_iterator("/proc/self/task")) {
        std::ifstream comm(e.path() / "comm"), status(e.path() / "status");
        TaskAffinity t;
        if (!std::getline(comm, t.comm)) continue;
        std::string line;
        bool found = false;
        while (std::getline(status, line))
            if (line.rfind("Cpus_allowed_list:", 0) == 0) {
                t.cpus = parseCpuList(line.substr(line.find_first_not_of(" \t", 18)));
                found = true;
            }
        if (found) out[e.path().filename().string()] = std::move(t);
    }
    return out;
}
} // namespace

// D2 review Md-3: the wiring, end to end on a real broker — BrokerNode::start applies the derived /
// explicit non-worker set to EVERY thread it creates (node driver, Aeron conductor, runtime, admin,
// cleaner, ...), each worker ends up on exactly its own core, and the caller's mask is restored.
// Before this test only a manual /proc reading on the bench box proved it.
TEST(ShardModeRegression, NonWorkerThreadsPinnedOffTheWorkerCoresRegression) {
    const std::vector<int32_t> allowed = currentThreadCpus();
    if (allowed.size() < 2) GTEST_SKIP() << "needs >= 2 allowed CPUs (one worker, one non-worker)";
    TmpDir dir{"affin"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    cfg.workerCoreBase = allowed.back();
    cfg.nonWorkerCpus  = {allowed.front()};
    std::set<std::string> preexisting;
    for (const auto& [tid, t] : scanTasks()) preexisting.insert(tid);

    BrokerNode node{cfg};
    node.start();
    EXPECT_EQ(currentThreadCpus(), allowed) << "start() must restore the caller's mask";
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    // Runs on worker 0 (owner of st/0) after its pass prologue re-pinned it.
    // D34: the box saw the full mask here once (1 of 4 -j3 runs, not reproduced locally) — name the
    // executing thread so the next failure says WHERE the task ran.
    std::string ranOn;
    const auto workerCpus = node.onOwnerThreadForTest(0, 0, [&ranOn](rune::replication::PartitionReplica&) {
        char name[32] = {0};
        (void)pthread_getname_np(pthread_self(), name, sizeof name);
        ranOn = std::string(name) + " tid " + std::to_string(static_cast<long>(::syscall(SYS_gettid)));
        return currentThreadCpus();
    });
    EXPECT_EQ(workerCpus, std::vector<int32_t>{allowed.back()}) << "the worker runs on exactly its own core (ran on "
                                                               << ranOn << ")";

    std::set<std::string> named;
    int brokerThreads = 0;
    for (const auto& [tid, t] : scanTasks()) {
        if (preexisting.count(tid) != 0) continue;
        if (t.comm.rfind("iou-", 0) == 0) continue;   // io_uring kernel workers: A4's ioWq placement
        ++brokerThreads;
        named.insert(t.comm);
        if (t.comm.rfind("rune-wrk-", 0) == 0)
            EXPECT_EQ(t.cpus, std::vector<int32_t>{allowed.back()}) << "worker tid " << tid;
        else
            EXPECT_EQ(t.cpus, std::vector<int32_t>{allowed.front()}) << t.comm << " tid " << tid
                << " must run on the non-worker set";
    }
    EXPECT_GE(brokerThreads, 4) << "driver + runtime + admin + worker at least";
    for (const char* n : {"rune-runtime", "rune-admin", "rune-wrk-0"})
        EXPECT_EQ(named.count(n), 1u) << n << " not found among the broker's threads";
    node.stop();
}
#endif

// D2 round 2: the control-plane threads of a thread-placement shard broker idle with the shard-mode park shape
// (no yields; NodeConfig::controlIdleShape) — runtime and admin AgentRunners, and the threaded node driver's Aeron
// backoff args.  Before: Aeron/Agrona's 10 spins + 20 yields + 1 us doubling park on all three (~1 core idle for
// three co-located nodes on the 4-core box).
TEST(ShardModeRegression, ControlPlaneThreadsUseTheParkShapeRegression) {
    TmpDir dir{"ctlidle"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const auto shapeOf = [](const rune::consensus::IdleStrategy* i) {
        const auto* b = dynamic_cast<const rune::consensus::BackoffIdleStrategy*>(i);
        EXPECT_NE(b, nullptr) << "a backoff idle (never busy-spin by default)";
        return b == nullptr ? NodeConfig::BackoffShape{}
                            : NodeConfig::BackoffShape{b->maxSpins(), b->maxYields(), b->minParkNs(), b->maxParkNs()};
    };
    const NodeConfig::BackoffShape want{10, 0, NodeConfig::kControlIdleMinParkUs * 1000,
                                        NodeConfig::kControlIdleMaxParkUsDefault * 1000};
    EXPECT_EQ(shapeOf(node.runtimeIdleForTest()), want) << "runtime thread";
    EXPECT_EQ(shapeOf(node.adminIdleForTest()), want) << "admin thread";
    ASSERT_NE(node.driverForTest(), nullptr);
    EXPECT_EQ(node.driverForTest()->idleStrategyArgsForTest(),
              "1-0-" + std::to_string(want.minParkNs) + "-" + std::to_string(want.maxParkNs)) << "node driver";
    node.stop();
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&n0, &n1}, kAsymPartitions), 0);
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
        ASSERT_EQ(defineShardTopic({&n0, &n1}, kAsymPartitions), 0);
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
        // DUR-1 B1: node 0 is an INCOMPLETE leader until node 1 (whose plane is
        // refused, so it never reports in) leaves its ISR by the lag check
        // (replicaLagMaxMs) — every produce, acks=leader included, is refused
        // NotLeader (retriable) until then.  Retry past that window.
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, part,
                                    /*acks=leader*/0, makeBatch(3), /*maxAttempts=*/2000);
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
    ASSERT_EQ(defineShardTopic({&n0, &n1, &n2}, kAsymPartitions), 0);
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
    ASSERT_EQ(defineShardTopic({&n0, &n1}, kAsymPartitions), 0);
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
    constexpr int32_t kTopicParts = 8;
    constexpr int64_t kLivenessNs = 2'000'000'000;           // > driver timer interval (1 s)
    cfg.shardClientLivenessTimeoutNs = kLivenessNs;
    BrokerNode node{cfg};
    node.setStartupPhaseHookForTest([](std::string_view phase) {
        if (phase == "build-gateway")                         // the injected fault: a slow rebuild
            std::this_thread::sleep_for(std::chrono::nanoseconds{2 * kLivenessNs});
    });
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}, kTopicParts), 0);
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    for (int32_t w = 0; w < kWorkers; ++w) {
        int32_t part = -1;
        for (int32_t p = 0; p < kTopicParts && part < 0; ++p)
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
    constexpr int32_t kTopicParts = 12;
    constexpr int64_t kLivenessNs = 2'000'000'000;           // > driver timer interval (1 s)
    cfg.shardClientLivenessTimeoutNs = kLivenessNs;
    BrokerNode node{cfg};
    node.setShardResolveStallForTest(kWorkers - 1, std::chrono::milliseconds{2 * kLivenessNs / 1'000'000});
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}, kTopicParts), 0);
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    for (int32_t w = 0; w < kWorkers; ++w) {
        int32_t part = -1;
        for (int32_t p = 0; p < kTopicParts && part < 0; ++p)
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
    auto haltCfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    haltCfg.workerSyncIntervalMs = 10;   // the periodic owned-log fsync whose failure is injected (default: off)
    BrokerNode node{haltCfg};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    {   // Topics exist only through DDL: a first boot creates the topic whose log then fails recovery.
        BrokerNode creator{cfg};
        creator.start();
        ASSERT_TRUE(creator.awaitLeader(10'000ms));
        ASSERT_EQ(defineShardTopic({&creator}), 0);
        creator.stop();
    }
    BrokerNode node{cfg};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.setRecoveryFaultForTest(EIO);
    EXPECT_THROW(node.start(), std::runtime_error) << "a log that failed recovery never serves";
    EXPECT_EQ(halt.await(1'000ms), BrokerNode::kExitStorageFailure) << "through the halt path (exit 74)";
    node.stop();
}

// D19 review L8: the start refusal for a provisioner without its runner thread,
// end to end through BrokerNode::start (the unit test pins requireProvisionerRunner).
TEST(ShardModeRegression, StartRefusesWithAnUnwiredProvisionerRegression) {
    TmpDir dir{"provunwired"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.skipProvisionerRunnerForTest();
    EXPECT_THROW(node.start(), std::logic_error) << "provisioned rolls would wait forever on no thread";
    node.stop();
}

TEST(ShardModeRegression, StartRefusesWhenTheCatalogLogFailedRecovery) {
    // Task 10 fix 3 / I1: the __catalog log lives outside the replica registry (Task 16: it
    // replaced the retired schema log).
    TmpDir dir{"schemafail"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    HaltCapture halt;
    node.setHaltHandler(&HaltCapture::handler, &halt);
    node.setCatalogRecoveryFaultForTest(EIO);
    EXPECT_THROW(node.start(), std::runtime_error) << "a failed catalog log never serves";
    EXPECT_EQ(halt.await(1'000ms), BrokerNode::kExitStorageFailure);
    node.stop();
}

TEST(ShardMode, MetadataAdvertisesTheLeaderWorkerEndpoint) {
    TmpDir dir{"md"}, cdir{"mdc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&n0, &n1, &n2}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
    std::thread stopper([&] { node.stop(); });
    stopper.join();
    EXPECT_TRUE(runtimeDriverClosed(cfg)) << "runtime driver leaked (debug builds abort)";
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
}

TEST(ShardModeFusedRegression, StartFailureStillClosesTheRuntimeDriver) {
    TmpDir dir{"fstartfail"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    {   // Topics exist only through DDL: a first boot creates the topic whose log then fails recovery.
        BrokerNode creator{cfg};
        creator.start();
        ASSERT_TRUE(creator.awaitLeader(10'000ms));
        ASSERT_EQ(defineShardTopic({&creator}), 0);
        creator.stop();
    }
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({nodes[0].get(), nodes[1].get(), nodes[2].get()}), 0);
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
        // Deployment LAYOUT only (not tuning): the derived per-node-id shard
        // ports are fixed, so concurrent test processes would collide on them.
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i];
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.workerIngressBasePort       = p.ib[nodeId];
    cfg.workerReplBasePort          = p.rb[nodeId];
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId   = kIngressStream;
    cfg.workerCoreBase              = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
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
    ASSERT_EQ(c0.workerMode, NodeConfig::WorkerMode::Shard) << "default worker mode (2026-10-02)";
    ASSERT_EQ(c0.runtimePlacement, NodeConfig::RuntimePlacement::Fused) << "default placement";
    ASSERT_EQ(c0.aeronMtuBytes, NodeConfig::kAeronAuto) << "derived transport";
    const std::size_t limit = static_cast<std::size_t>(c0.produceIntakeLimit());
    ASSERT_EQ(limit, Worker::maxAcceptableBatchBytes(
        static_cast<std::size_t>(c0.workerIngressRingBytes), static_cast<std::size_t>(c0.workerReturnRingBytes),
        static_cast<std::size_t>(c0.replicationOutboundRingBytes),
        static_cast<std::size_t>(c0.aeronTermBufferBytes), static_cast<std::size_t>(c0.aeronTermBufferBytes),
        static_cast<std::size_t>(c0.aeronTermBufferBytes)));
    // Default term / 8 bounds the egress Records frame — one of the binding hops
    // (the case the unification closes): 16 MiB term → 2 MiB.
    ASSERT_LE(limit + rune::node::kRecordsFixedBytes, static_cast<std::size_t>(c0.aeronTermBufferBytes) / 8);

    std::vector<std::unique_ptr<BrokerNode>> ns;
    ns.push_back(std::make_unique<BrokerNode>(c0));
    if (nodes == 3) {
        ns.push_back(std::make_unique<BrokerNode>(makeDefaultConfig(1, d1.path, ports, 3)));
        ns.push_back(std::make_unique<BrokerNode>(makeDefaultConfig(2, d2.path, ports, 3)));
    }
    for (auto& n : ns) n->start();
    ASSERT_TRUE(ns[0]->awaitLeader(15'000ms));
    {
        std::vector<BrokerNode*> raw;
        for (auto& n : ns) raw.push_back(n.get());
        ASSERT_EQ(defineShardTopic(std::move(raw)), 0);
    }
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
        // Review B1 (regression): metadata answered by ANY node advertises the
        // LEADER's per-worker egress-control port (per-node bases differ).
        for (int asker = 0; asker < nodes; ++asker) {
            const int64_t s = c.connect(gatewayChannel(ports, asker), kClientStream);
            ASSERT_GT(s, 0);
            bool seen = false;
            for (int i = 0; i < 200 && !seen; ++i) {
                for (const auto& pi : c.metadata(gatewayChannel(ports, asker), kClientStream, s))
                    if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId == leader) {
                        EXPECT_EQ(pi.egressControlPort, ports.eg[leader]) << "asked node " << asker;
                        seen = true;
                    }
                if (!seen) std::this_thread::sleep_for(50ms);
            }
            EXPECT_TRUE(seen) << "node " << asker << " never advertised the leader";
        }
        const std::string gw = gatewayChannel(ports, leader);
        const int64_t sid = c.connect(gw, kClientStream);
        ASSERT_GT(sid, 0);

        // Accepted (+ replicated, 3 nodes: acks=all with minIsr 2 answers OK only
        // once a follower holds the batch).
        // Shard default: produce goes straight to the leader's worker ingress.
        const std::string ing = workerIngressChannel(ports, leader, 0);
        auto ack = c.produceUntilOk(ing, kClientStream, sid, 0, 0, /*acks=all*/1, atLimit);
        ASSERT_TRUE(ack.has_value()) << "a batch at exactly the limit must be accepted (acks=all)";
        EXPECT_EQ(ack->baseOffset, 0);

        // limit + 1: refused at intake, non-retriable, never appended.
        ASSERT_TRUE(c.produce(ing, kClientStream, sid, 900001, 0, 0, /*acks=all*/1, over));
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
        // Kafka parity: the leader stamps its partitionLeaderEpoch (bytes 12..15,
        // CRC-excluded) on append — a real epoch, never the producer's -1.
        const auto& got0 = c.records().front().bytes;
        int32_t stampedEpoch = 0;
        std::memcpy(&stampedEpoch, got0.data() + 12, 4);
        EXPECT_GE(stampedEpoch, 0) << "the leader did not stamp its epoch";
        std::memcpy(expected.data() + 12, &stampedEpoch, 4);
        EXPECT_EQ(got0, expected) << "pushed batch not byte-identical (beyond the leader stamps)";
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
        nodes.push_back(std::make_unique<BrokerNode>(cfg));
    }
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(defineShardTopic({nodes[0].get(), nodes[1].get(), nodes[2].get()}, kParts), 0);
    // A9 review M-2 / L-1 (wiring, fused, 2 workers): where worker 0 parks on its doorbell (Linux wake fds),
    // the runtime on worker 0 is woken by worker 1 freeing op / session ring space and by worker 1's returns;
    // worker 0's own ret_ (same thread) gets no space doorbell.  Elsewhere nothing is wired (review M-1).
    for (auto& n : nodes) {
        const bool on = n->workerForTest(0)->doorbellEnabled();
        EXPECT_EQ(n->workerForTest(1)->doorbellEnabled(), on);
        EXPECT_EQ(n->workerForTest(1)->opRingProducerDoorbellForTest(), on);
        EXPECT_EQ(n->workerForTest(1)->sessionRingProducerDoorbellForTest(), on);
        EXPECT_EQ(n->workerForTest(1)->retConsumerDoorbellForTest(), on);
        EXPECT_EQ(n->workerForTest(1)->retProducerDoorbellForTest(), on);
        EXPECT_FALSE(n->workerForTest(0)->retProducerDoorbellForTest());
#if defined(__linux__)
        EXPECT_TRUE(on) << "Linux default: epoll-first wake fds + RUNE_IDLE_DOORBELL on";
#endif
    }
    {
        ShardTestClient c(cdir.path);
        // D31 box run: an ACTIVE client (not the silent-client model) — a re-route that waits out a stale
        // leader must not cross the 10 s session lease, or its metadata session is reaped and the route can
        // never be refreshed.
        c.keepSessionsAlive(true);
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
        // D31 (box -j3): leadership may still move after the first multi-leader snapshot (the spread rebalance
        // lands ~40 ms later), so a route is re-read from the metadata whenever a batch is not acked — as a real
        // client does on a Redirect.  Every route still comes ONLY from the MetadataResponse.
        const auto advertised = [&](int32_t partition) -> std::optional<PartitionInfo> {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == partition && pi.leaderId >= 0) return pi;
            return std::nullopt;
        };
        for (const bool endpointAware : {false, true}) {
            for (const auto& first : parts) {
                PartitionInfo pi = first;
                for (int b = 0; b < kBatches; ++b) {
                    std::optional<pc::ProduceResponse> ack;
                    for (int attempt = 0; attempt < 5 && !ack; ++attempt) {
                        if (attempt > 0) if (auto fresh = advertised(pi.partition)) pi = *fresh;
                        if (endpointAware) ASSERT_FALSE(pi.ingressEndpoint.empty()) << "partition " << pi.partition;
                        if (!sidOn.count(pi.leaderId)) {
                            sidOn[pi.leaderId] = c.connect(gatewayChannel(ports, pi.leaderId), kClientStream);
                            ASSERT_GT(sidOn[pi.leaderId], 0) << "connect to leader " << pi.leaderId;
                        }
                        const std::string ch = endpointAware ? "aeron:udp?endpoint=" + pi.ingressEndpoint
                                                             : gatewayChannel(ports, pi.leaderId);
                        ack = c.produceUntilOk(ch, pi.ingressStreamId, sidOn[pi.leaderId], 0, pi.partition,
                                               /*acks=all*/1, makeBatch(2), /*maxAttempts=*/3);
                    }
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
// refused AND the gateway has paused it.  D8c: the pause is credit based (at most
// ClientGateway::sessionProduceCredits() forwarded, unclaimed produces per
// session), so it is independent of pipeline depth — runs on the host-derived
// transport.
TEST(ShardModeRegression, SlowEndpointUnawareClientCannotStarveTheShard) {
    TmpDir dir{"slowcl"}, ad{"slowa"}, bd{"slowb"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
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
        // D8c: the HOST-DERIVED client transport (64 KiB loopback MTU, 16 MiB
        // socket buffers — one datagram carries hundreds of A's frames).  Was
        // pinned to the pre-2026-10-02 transport because the pause raced the
        // burst (Linux: engaged at 27-32k frames or never, ~28k worker ack-room
        // pauses); the gateway's per-session produce credits now pause A after
        // at most its budget of unclaimed forwards, whatever one poll carries.
        ShardTestClient a(ad.path, 1 << 20), b(bd.path, 1 << 20);
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

// D8b round 2 (re-review LOW-A / CRITICAL-1 / MAJOR-A), REAL driver, default broker config.  A gateway-routed
// client paused by back-pressure (acks held, produce intake ABORTed) ...
namespace {
template <class Pred>
bool awaitBrokerState(Pred&& pred, std::chrono::seconds budget) {   // bounded poll of a real broker condition
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(1ms);                              // poll interval only (client NOT pumped)
    }
    return pred();
}
int64_t pauseClient(ShardTestClient& a, const std::string& gw, int64_t sidA, int64_t c0,
                    const std::function<bool()>& paused) {
    const auto batch = makeBatch(1);
    return a.offerUntilPaused(gw, kClientStream, [&](int64_t i, std::vector<uint8_t>& out) {
        pc::Produce m;
        m.sessionId = sidA; m.correlationId = c0 + i; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
        m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch);
        out.resize(m.encodedSize());
        m.encode(out.data());
    }, /*maxFrames=*/40'000, paused);
}
} // namespace

// ... that is KILLED (its Aeron client + driver closed — no more status messages) has its session closed and its
// owed acks freed within ~ the publication-connection timeout (5 s) + the normal lease (10 s): the worker reports
// it Lost, the gateway reaps it 10 s later.  (982b50e kept it forever.)  Another session is served afterwards.
TEST(ShardModeRegression, KilledPausedClientsSessionAndAcksAreFreedRegression) {
    TmpDir dir{"killcl"}, ad{"killa"}, bd{"killb"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const std::string gw = gatewayChannel(ports, 0);
    // D11: aborts because a WORKER HOLDS this session's acks — not credit / worker-cap pauses (D8c), which also
    // count in heldProduceAborts but hold nothing: their acks fit the client's egress window.
    auto gatewayPauses = [&] {
        return node.onControlThreadForTest([&] {
            auto* g = node.gatewayForTest();
            return g->heldProduceAbortsForTest() - g->creditProduceAbortsForTest() - g->workerCapProduceAbortsForTest();
        });
    };
    auto sessionAlive = [&](int64_t sid) {
        return node.onControlThreadForTest([&] { return node.gatewayForTest()->sessionLastSeenNsForTest(sid) >= 0; });
    };
    auto acksPending = [&] {
        return node.onOwnerThreadForTest(0, 0, [&](rune::replication::PartitionReplica&) {
            return node.workerForTest(0)->sessionAcksPendingForTest();
        });
    };
    auto clientImages = [&] { return node.onControlThreadForTest([&] { return node.gatewayForTest()->clientImageCountForTest(); }); };
    const int images0 = clientImages();
    ShardTestClient b(bd.path, 1 << 20);   // D8c follow-up: the host-derived (default) transport
    int64_t sidA = 0, sidB = 0;
    {
        ShardTestClient a(ad.path, 1 << 20);
        sidA = a.connect(gw, kClientStream);
        ASSERT_GT(sidA, 0);
        ASSERT_TRUE(a.produceUntilOk(gw, kClientStream, sidA, 0, 0, /*acks=*/1, makeBatch(1)));
        // Precondition: the worker HOLDS A's acks (A reads nothing; 40k acks >> its egress window).  D11: wait for
        // a HOLD abort — a transient ack backlog (pending > 256) or a credit pause is not a hold: those acks still
        // fit A's window and drain moments later.
        (void)pauseClient(a, gw, sidA, 50'000'000, [&] { return gatewayPauses() > 0; });
        ASSERT_TRUE(awaitBrokerState([&] { return acksPending() > 0; }, 10s))
            << "the worker holds A's acks (stall offer=" << a.lastStallOffer_ << ")";
        // D8b-L (LOW-4): another session on the SAME worker keeps full intake WHILE A still pins its acks.
        sidB = b.connect(gw, kClientStream);
        ASSERT_GT(sidB, 0);
        for (int k = 0; k < 5; ++k)
            ASSERT_TRUE(b.produceUntilOk(gw, kClientStream, sidB, 0, 0, /*acks=*/1, makeBatch(1), /*maxAttempts=*/3))
                << "B's produce " << k << " starved while A pins acks";
        ASSERT_GT(acksPending(), 0) << "A still pins acks while B is served; A alive=" << sessionAlive(sidA);
    }                                                                  // A is killed (client + driver closed)
    const auto killed = std::chrono::steady_clock::now();
    EXPECT_TRUE(awaitBrokerState([&] { return !sessionAlive(sidA) && acksPending() == 0; }, 40s))
        << "a killed paused client's session and owed acks must be freed";
    const auto took = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - killed);
    EXPECT_LE(took.count(), 25) << "~5 s NOT_CONNECTED + 10 s lease, not the 10-min held bound";
    // D8b-L (LOW-4): A's image at the gateway is released (an aborted held frame never pins it); B's stays.
    EXPECT_TRUE(awaitBrokerState([&] { return clientImages() == images0 + 1; }, 40s))
        << "the killed client's gateway image was not released: images=" << clientImages() << " baseline=" << images0;
    // B sent nothing during that wait (no keepalives): its session may be reaped on the normal lease — a new one.
    const int64_t sidB2 = b.connect(gw, kClientStream);
    ASSERT_GT(sidB2, 0);
    for (int k = 0; k < 5; ++k)
        ASSERT_TRUE(b.produceUntilOk(gw, kClientStream, sidB2, 0, 0, /*acks=*/1, makeBatch(1), /*maxAttempts=*/3));
    node.stop();
}

// ... whose client is INVOKER-driven (rune-client-cpp RunnerMode::Invoker: its driver runs only inside its calls) and
// stops calling for longer than the publication-connection timeout: the worker's claim reports NOT_CONNECTED
// (Lost) — a suspicion, not a verdict.  The session and every queued ack survive; once the client pumps again the
// publication is Found and it reads every ack.  (7dc95c1 failed the session at NOT_CONNECTED and dropped its acks.)
namespace {
void runPausedInvokerClientSurvivesNotConnected(uint32_t gatewayProduceCredits) {
    TmpDir dir{"invcl"}, ad{"inva"};
    const auto ports = ShardPorts::discover(1, 1);
    NodeConfig cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    cfg.gatewayProduceCredits = gatewayProduceCredits;   // 0 = default (1024)
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const std::string gw = gatewayChannel(ports, 0);
    // D11: aborts because a WORKER HOLDS this session's acks — not credit / worker-cap pauses (D8c), which also
    // count in heldProduceAborts but hold nothing: their acks fit the client's egress window.
    auto gatewayPauses = [&] {
        return node.onControlThreadForTest([&] {
            auto* g = node.gatewayForTest();
            return g->heldProduceAbortsForTest() - g->creditProduceAbortsForTest() - g->workerCapProduceAbortsForTest();
        });
    };
    ShardTestClient a(ad.path, 1 << 20, /*legacyTransport=*/false, DriverMode::Invoker);   // default transport (D8c follow-up)
    const int64_t sidA = a.connect(gw, kClientStream);
    ASSERT_GT(sidA, 0);
    ASSERT_TRUE(a.produceUntilOk(gw, kClientStream, sidA, 0, 0, /*acks=*/1, makeBatch(1)));
    constexpr int64_t c0 = 50'000'000;
    auto acksPending = [&] {
        return node.onOwnerThreadForTest(0, 0, [&](rune::replication::PartitionReplica&) {
            return node.workerForTest(0)->sessionAcksPendingForTest();
        });
    };
    // Precondition: the worker HOLDS A's acks (D11: a HOLD abort — not a credit pause / transient backlog).
    const int64_t n = pauseClient(a, gw, sidA, c0, [&] { return gatewayPauses() > 0; });
    ASSERT_GT(n, 0) << "stall offer=" << a.lastStallOffer_;
    ASSERT_TRUE(awaitBrokerState([&] { return acksPending() > 0; }, 10s)) << "the worker holds A's acks; n=" << n
        << " aborts{all=" << gatewayPauses() << " credit="
        << node.onControlThreadForTest([&] { return node.gatewayForTest()->creditProduceAbortsForTest(); })
        << " workerCap=" << node.onControlThreadForTest([&] { return node.gatewayForTest()->workerCapProduceAbortsForTest(); })
        << "}";
    // The client stops pumping until the broker has seen its publication go NOT_CONNECTED (or, pre-fix, closed it).
    auto lostOrClosed = [&] {
        return node.onControlThreadForTest([&] {
            auto* g = node.gatewayForTest();
            return g->sessionLostForTest(sidA) || g->sessionLastSeenNsForTest(sidA) < 0;
        });
    };
    ASSERT_TRUE(awaitBrokerState(lostOrClosed, 20s)) << "no status messages for > 5 s -> NOT_CONNECTED";
    EXPECT_TRUE(node.onControlThreadForTest([&] { return node.gatewayForTest()->sessionLastSeenNsForTest(sidA) >= 0; }))
        << "NOT_CONNECTED alone must not close a live client's session";
    for (int64_t c = c0; c < c0 + n; ++c)                          // pumps again: Found, every ack arrives
        ASSERT_TRUE(a.awaitProduceResponse(c).has_value()) << "ack " << c - c0 << " of " << n;
    EXPECT_TRUE(node.onControlThreadForTest([&] {
        auto* g = node.gatewayForTest();
        return !g->sessionLostForTest(sidA) && g->sessionLastSeenNsForTest(sidA) >= 0;
    })) << "Found clears Lost; the session lives on";
    node.stop();
}
} // namespace

TEST(ShardModeRegression, PausedInvokerClientSurvivesNotConnectedRegression) {
    runPausedInvokerClientSurvivesNotConnected(0);
}

// D11 (ShardModeRegression.PausedInvokerClientSurvivesNotConnectedRegression flaked under load: "the worker holds
// A's acks" never held).  The pause predicate accepted ANY gateway produce abort — but a CREDIT pause (D8c: the
// session's forwarded produces not yet acked) is not a hold: its <= credits acks all fit the client's egress window
// once the worker catches up, so the worker never holds any.  Under load the worker lags the client and the credit
// pause comes first.  A 1-credit gateway makes that ordering deterministic.
TEST(ShardModeRegression, PausedInvokerCreditPauseIsNotAHoldRegression) {
    runPausedInvokerClientSurvivesNotConnected(1);
}

// D8b-L (re-review LOW-4, lows batch C), REAL driver, default config + transport: the BACK_PRESSURED -> Found path.  An
// INVOKER client paused (acks held) stops pumping until the broker sees its ack publication NOT_CONNECTED (Lost), then
// pumps its Aeron runtime again WITHOUT reading: status messages flow, the worker's claim now returns BACK_PRESSURED —
// proof the publication is connected — so the session is Found and stays Held (the 10-min bound), NOT reaped on the
// Lost lease 10 s later.  Then it reads every ack.  (A Lost that only a successful claim could clear reaped it.)
TEST(ShardModeRegression, PausedInvokerClientResumingWithoutReadingIsFoundNotReapedRegression) {
    TmpDir dir{"invbp"}, ad{"invbpa"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const std::string gw = gatewayChannel(ports, 0);
    auto onGw = [&](auto&& f) { return node.onControlThreadForTest([&] { return f(*node.gatewayForTest()); }); };
    auto gatewayPauses = [&] {   // D11: HOLD aborts only (not credit / worker-cap pauses)
        return onGw([](auto& g) {
            return g.heldProduceAbortsForTest() - g.creditProduceAbortsForTest() - g.workerCapProduceAbortsForTest();
        });
    };
    ShardTestClient a(ad.path, 1 << 20, /*legacyTransport=*/false, DriverMode::Invoker);
    const int64_t sidA = a.connect(gw, kClientStream);
    ASSERT_GT(sidA, 0);
    ASSERT_TRUE(a.produceUntilOk(gw, kClientStream, sidA, 0, 0, /*acks=*/1, makeBatch(1)));
    constexpr int64_t c0 = 50'000'000;
    const int64_t n = pauseClient(a, gw, sidA, c0, [&] { return gatewayPauses() > 0; });
    ASSERT_GT(n, 0) << "stall offer=" << a.lastStallOffer_;
    ASSERT_TRUE(awaitBrokerState([&] { return onGw([&](auto& g) { return g.sessionHeldForTest(sidA); }); }, 10s))
        << "the worker holds A's acks";
    // Stop pumping until the broker sees NOT_CONNECTED (~ the 5 s publication-connection timeout).
    ASSERT_TRUE(awaitBrokerState([&] { return onGw([&](auto& g) { return g.sessionLostForTest(sidA); }); }, 20s))
        << "no status messages -> Lost";
    // Pump again, never reading: Found (BACK_PRESSURED proves the publication connected) ...
    auto pumpFor = [&](auto&& until, std::chrono::seconds budget) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (std::chrono::steady_clock::now() < deadline && !until()) {
            a.pumpWithoutReading();
            std::this_thread::sleep_for(100us);                        // the client's idle (it pumps, never reads)
        }
        return until();
    };
    ASSERT_TRUE(pumpFor([&] { return !onGw([&](auto& g) { return g.sessionLostForTest(sidA); }); }, 20s))
        << "a pumping (but not reading) client must be Found";
    const auto found = std::chrono::steady_clock::now();
    // ... and held past the Lost lease (sessionTimeout 10 s, measured from the Lost edge) without being reaped.
    constexpr auto kLease = std::chrono::nanoseconds{rune::gateway::ClientGateway::kDefaultSessionTimeoutNs};
    const auto pastLease = found + kLease + 2s;
    (void)pumpFor([&] { return std::chrono::steady_clock::now() >= pastLease ||
                               onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA) < 0; }); }, 30s);
    EXPECT_TRUE(onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA) >= 0; }))
        << "a Found (back-pressured, connected) held session was reaped on the Lost lease";
    EXPECT_TRUE(onGw([&](auto& g) { return g.sessionHeldForTest(sidA) && !g.sessionLostForTest(sidA); }))
        << "still Held (it has not read), not Lost";
    for (int64_t c = c0; c < c0 + n; ++c)                              // it reads: every ack arrives, in order
        ASSERT_TRUE(a.awaitProduceResponse(c).has_value()) << "ack " << c - c0 << " of " << n;
    EXPECT_TRUE(onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA) >= 0; }));
    node.stop();
}

// D8b-L (re-review LOW-4 / MEDIUM-1, lows batch C), REAL driver, default config + transport: a DIRECT (worker-ingress)
// client whose acks are held and that sends nothing to the gateway for longer than the session lease (its app thread
// stalls; its driver keeps sending status messages, so it is never Lost) is kept by the held bound.  When it reads
// again its small backlog drains (typically in ONE worker pass: no Progress signal) and the Released edge itself
// refreshes the lease — it must not be reaped in the same gateway cycle as it recovers.
TEST(ShardModeRegression, HeldDirectClientRecoveringAfterTheLeaseIsNotReapedOnReleaseRegression) {
    TmpDir dir{"relrp"}, ad{"relrpa"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const std::string gw = gatewayChannel(ports, 0), direct = workerIngressChannel(ports, 0, 0);
    auto onGw = [&](auto&& f) { return node.onControlThreadForTest([&] { return f(*node.gatewayForTest()); }); };
    auto acksPending = [&] {
        return node.onOwnerThreadForTest(0, 0, [&](rune::replication::PartitionReplica&) {
            return node.workerForTest(0)->sessionAcksPendingForTest();
        });
    };
    ShardTestClient a(ad.path, 1 << 20);
    const int64_t sidA = a.connect(gw, kClientStream);
    ASSERT_GT(sidA, 0);
    const auto batch = makeBatch(1);
    ASSERT_TRUE(a.produceUntilOk(direct, kClientStream, sidA, 0, 0, /*acks=*/1, batch));
    // Produce directly, never reading, ONE frame at a time — each appended before the next is sent — until the
    // worker holds acks (the client's egress window is full).  So no frame waits refused in A's image: nothing is
    // admitted after the drain (an admitted produce's ack notification is liveness too, F23), and the held backlog
    // is a single ack — one window update takes it, in one pass, with no Progress signal: only the Released edge
    // can refresh the lease.
    auto leo = [&] {
        return node.onOwnerThreadForTest(0, 0, [](rune::replication::PartitionReplica& r) {
            return r.log()->logEndOffset();
        });
    };
    constexpr int64_t c0 = 60'000'000;
    int64_t sent = 0;
    const int64_t leo0 = leo();
    while (sent < 200'000 && acksPending() == 0) {
        ASSERT_TRUE(a.produce(direct, kClientStream, sidA, c0 + sent, 0, 0, /*acks=*/0, batch));
        ++sent;
        int spins = 0;
        while (leo() < leo0 + sent) ASSERT_LT(++spins, 100'000) << "frame " << sent << " never appended";
    }
    ASSERT_LT(sent, 200'000);
    ASSERT_TRUE(awaitBrokerState([&] { return onGw([&](auto& g) { return g.sessionHeldForTest(sidA); }); }, 10s))
        << "the worker holds A's acks (sent=" << sent << ", pending=" << acksPending() << ")";
    const auto lastSent = std::chrono::steady_clock::now();
    // A's app is silent past the whole lease (no keepalive, no read); its driver thread keeps the publication up.
    constexpr auto kLease = std::chrono::nanoseconds{rune::gateway::ClientGateway::kDefaultSessionTimeoutNs};
    ASSERT_FALSE(awaitBrokerState([&] { return std::chrono::steady_clock::now() >= lastSent + kLease + 1s &&
                                               onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA) < 0; }); },
                                  std::chrono::duration_cast<std::chrono::seconds>(kLease + 2s)))
        << "the held bound must keep A while it is silent";
    ASSERT_GE(onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA); }), 0);
    ASSERT_FALSE(onGw([&](auto& g) { return g.sessionLostForTest(sidA); })) << "its driver is alive: never Lost";
    for (int64_t c = c0; c < c0 + sent; ++c)                           // A reads everything
        ASSERT_TRUE(a.awaitProduceResponse(c).has_value()) << "ack " << c - c0 << " of " << sent;
    ASSERT_TRUE(awaitBrokerState([&] { return onGw([&](auto& g) {
        return !g.sessionHeldForTest(sidA) || g.sessionLastSeenNsForTest(sidA) < 0; }); }, 10s));
    EXPECT_GE(onGw([&](auto& g) { return g.sessionLastSeenNsForTest(sidA); }), 0)
        << "reaped in the cycle it was Released (the release did not refresh its lease)";
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
    ASSERT_EQ(defineShardTopic({&node}), 0);
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

// F15 S5 fix round 1 (review M-5): the DEFAULT config across a PROCESS RESTART.
// Before S5 the gateway mirror seeded Legacy dedup state at startup; now the
// owning worker's RC build rebuilds it from the log.  Produce seq 0 and 1,
// stop, start a NEW broker on the same data dir, retry seq 1: answered with
// the original offset (1), one copy; seq 2 lands at 2.
TEST(WorkerPsmEndToEndRegression, DefaultLegacyConfigDedupsARetryAfterRestart) {
    TmpDir dir{"psmr"}, cdir1{"psmrc1"}, cdir2{"psmrc2"};
    const auto ports = ShardPorts::discover(1, 1);
    const NodeConfig cfg = makeDefaultConfig(0, dir.path, ports, 1);
    constexpr int64_t kPid = 4343;
    const std::string gw = gatewayChannel(ports, 0);
    auto sendIdem = [&](ShardTestClient& c, int64_t sid, int32_t seq) -> std::optional<pc::ProduceResponse> {
        const auto batch = idempotentBatch(kPid, 0, seq);
        for (int a = 0; a < 200; ++a) {
            const int64_t corr = c.nextCorrelationId();
            if (!c.produceIdempotent(gw, kClientStream, sid, corr, 0, 0, /*acks=all*/1, kPid, 0, seq, batch)) continue;
            auto ack = c.awaitProduceResponse(corr);
            if (ack && ack->errorCode == static_cast<int16_t>(pc::ErrorCode::NotLeader)) continue;
            return ack;
        }
        return std::nullopt;
    };
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        ASSERT_EQ(defineShardTopic({&node}), 0);
        {
            ShardTestClient c(cdir1.path);
            const int64_t sid = c.connect(gw, kClientStream);
            ASSERT_GT(sid, 0);
            for (int32_t seq = 0; seq < 2; ++seq) {
                auto a = sendIdem(c, sid, seq);
                ASSERT_TRUE(a.has_value());
                ASSERT_EQ(a->errorCode, 0);
                ASSERT_EQ(a->baseOffset, seq);
            }
        }
        node.stop();
    }
    ASSERT_EQ(logEndOffsetAfterStop(dir.path), 2);
    {
        BrokerNode node{cfg};                                  // a new process image, same data dir
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));   // the topic comes back from __catalog: nothing is defined again
        {
            ShardTestClient c(cdir2.path);
            const int64_t sid = c.connect(gw, kClientStream);
            ASSERT_GT(sid, 0);
            auto retry = sendIdem(c, sid, 1);                  // the producer's retry of seq 1
            ASSERT_TRUE(retry.has_value());
            EXPECT_EQ(retry->errorCode, 0);
            EXPECT_EQ(retry->baseOffset, 1) << "the retry after restart was not deduped (original offset 1)";
            auto next = sendIdem(c, sid, 2);
            ASSERT_TRUE(next.has_value());
            EXPECT_EQ(next->errorCode, 0) << "seq 2 refused after restart (20 = OutOfOrderSequence)";
            EXPECT_EQ(next->baseOffset, 2);
        }
        node.stop();
    }
    EXPECT_EQ(logEndOffsetAfterStop(dir.path), 3) << "the retried batch was appended twice across the restart";
}

// D8d (D8c review LOW-R2), REAL driver + real broker: DIRECT (worker-ingress)
// clients that stop reading their acks — each its own client, driver and image,
// at the fragment-limit ceiling the worker's reserve is sized for (one poll may
// take 4096 frames of one image) — must not stop another direct client's intake
// on the same worker, nor the worker's op drain.  (Was: 48 stalled sessions held
// 30-32k of the 32768-slot ack queue — the gateway-routed half included; at
// 32768 ackRoom_ is 0: no op drain, no poll.)
TEST(ShardModeRegression, StalledDirectClientsCannotStopAnotherDirectClientsIntakeRegression) {
    constexpr int kStalled = 48;
    TmpDir dir{"d8dn"}, ad{"d8da"}, bd{"d8db"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    cfg.gatewayFragmentLimit = rune::node::Worker::kMaxFragmentLimit;
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    const std::string gw = gatewayChannel(ports, 0), direct = workerIngressChannel(ports, 0, 0);
    auto onWorker = [&](auto&& f) {
        return node.onOwnerThreadForTest(0, 0, [&](rune::replication::PartitionReplica&) { return f(*node.workerForTest(0)); });
    };
    {
        // One stalled client process with kStalled sessions, each producing on its own exclusive publication (its
        // own image at the worker) and never reading its acks again.
        ShardTestClient a(ad.path, 1 << 22);
        const auto batch = makeBatch(1);
        for (int k = 0; k < kStalled; ++k) {
            const int64_t sid = a.connect(gw, kClientStream);
            ASSERT_GT(sid, 0);
            // Offer until refused AND the worker has paused (ABORTed) intake.
            const uint64_t abortsBefore = onWorker([](rune::node::Worker& w) { return w.intakePausedFramesForTest(); });
            const int64_t n = a.offerUntilPausedExclusive(k, direct, kClientStream, [&](int64_t i, std::vector<uint8_t>& out) {
                pc::Produce m;
                m.sessionId = sid; m.correlationId = 70'000'000 + i; m.topicId = 0; m.partition = 0;
                m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
                m.recordBatch = std::span<const uint8_t>(batch);
                out.resize(m.encodedSize());
                m.encode(out.data());
            }, /*maxFrames=*/200'000, [&] {
                return onWorker([](rune::node::Worker& w) { return w.intakePausedFramesForTest(); }) > abortsBefore;
            });
            ASSERT_LT(n, 200'000) << "stalled session " << k << " was never paused";
        }
        // Pre-fix: 30-32k of the 32768-slot queue held — the gateway-routed half eaten too, one more burst from a
        // full queue (ackRoom_ 0: no op drain, no poll).
        EXPECT_LE(onWorker([](rune::node::Worker& w) { return w.sessionAcksPendingForTest(); }),
                  rune::node::Worker::kDirectAckBudget) << "stalled direct sessions hold at most the direct share";
        EXPECT_LE(onWorker([](rune::node::Worker& w) { return w.directAcksInFlightForTest(); }),
                  rune::node::Worker::kDirectAckBudget);

        ShardTestClient b(bd.path, 1 << 22);
        const int64_t sidB = b.connect(gw, kClientStream);
        ASSERT_GT(sidB, 0) << "a new session is accepted while the stalled ones hold acks";
        for (int k = 0; k < 5; ++k)
            ASSERT_TRUE(b.produceUntilOk(direct, kClientStream, sidB, 0, 0, /*acks=*/1, batch, /*maxAttempts=*/3))
                << "client B's direct produce " << k << " starved behind the stalled sessions' unread acks";
        EXPECT_EQ(onWorker([](rune::node::Worker& w) { return w.ackRoomOpPausesForTest(); }), 0u)
            << "the worker's op drain never ran out of ack room";
        EXPECT_EQ(onWorker([](rune::node::Worker& w) { return w.directUnderflowsForTest(); }), 0u);
    }
    node.stop();
}

// E3 / DR-10 (2026-10-04): a broker FROZEN past Aeron's default client liveness (10 s) — SIGSTOP, a VM stall,
// docker pause — died on resume with "client timeout from driver" (exit 255): its in-process driver timed out its
// own in-process client.  Both live in this process, so that check can only detect the process's own stall; the
// default must survive it (the broker then rejoins as a follower and is fenced by term).  Child process: the
// freeze is the stimulus (12 s), every other wait is progress-driven.
#include <csignal>
#include <sys/wait.h>
TEST(ShardModeRegression, BrokerSurvivesAProcessFreezePastAeronLivenessDefault) {
    TmpDir dir{"freeze"};
    const auto ports = ShardPorts::discover(1, 1);
    NodeConfig cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);   // the default placement
    int beat[2];
    ASSERT_EQ(::pipe(beat), 0);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::close(beat[0]);
        BrokerNode node{cfg};
        node.start();
        if (!node.awaitLeader(std::chrono::milliseconds{30'000})) ::_exit(3);
        for (;;) {                                         // heartbeat while the node runs
            if (::write(beat[1], "H", 1) != 1) ::_exit(4);
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
    }
    ::close(beat[1]);
    auto waitBeat = [&](std::chrono::seconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        char b;
        while (std::chrono::steady_clock::now() < deadline) {
            fd_set fds; FD_ZERO(&fds); FD_SET(beat[0], &fds);
            timeval tv{0, 200'000};
            if (::select(beat[0] + 1, &fds, nullptr, nullptr, &tv) > 0) return ::read(beat[0], &b, 1) == 1;
        }
        return false;
    };
    ASSERT_TRUE(waitBeat(std::chrono::seconds{60})) << "child broker never started";
    ASSERT_EQ(::kill(child, SIGSTOP), 0);
    std::this_thread::sleep_for(std::chrono::seconds{12});   // the stimulus: frozen past the 10 s Aeron default
    ASSERT_EQ(::kill(child, SIGCONT), 0);
    // Drain beats buffered before the stop, then require the process to stay alive for 5 s of fresh beats.
    int status = 0;
    bool alive = true;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < until) {
        if (::waitpid(child, &status, WNOHANG) == child) { alive = false; break; }
        (void)waitBeat(std::chrono::seconds{1});
    }
    EXPECT_TRUE(alive) << "the broker died after the freeze (exit status " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1)
                       << ") — its in-process Aeron driver timed out its own client";
    if (alive) { ::kill(child, SIGKILL); ::waitpid(child, &status, 0); }
    ::close(beat[0]);
}

// ===========================================================================
// DR-15 (2026-10-05): two brokers restarted at once both EXITED — each node's
// shard replication plane registers a publication to every peer at startup; the
// co-restarting peer had no DNS record yet ("Unresolved - endpoint=rune-2:41256"),
// the plane attach failed and BrokerNode threw "shard 0 replication plane did not
// resolve — refusing to start".  A node MUST start and serve with any peer
// unresolvable (RFC 6761 ".invalid" never resolves).  RED on bc153c3: start() threw.
// ===========================================================================
TEST(UnresolvedPeerStartupRegression, NodesStartAndServeWhileAPeerHostnameIsUnresolvableRegression) {
    TmpDir d0{"unres0"}, d1{"unres1"}, cdir{"unresc"};
    const auto ports = ShardPorts::discover(3, 1);
    auto cfgFor = [&](int32_t id, const fs::path& dir) {
        auto c = asymConfig(id, dir, ports, 3, /*workers=*/1);
        c.replicaLagMaxMs = 500;                              // the absent peer leaves the ISR fast
        for (auto& p : c.peers)
            if (p.id == 2) p.host = "rune-unresolvable.invalid";   // node 2's pod is restarting: no DNS yet
        return c;
    };
    BrokerNode n0{cfgFor(0, d0.path)}, n1{cfgFor(1, d1.path)};
    ASSERT_NO_THROW(n0.start()) << "a node must start while a peer hostname is unresolvable";
    ASSERT_NO_THROW(n1.start()) << "a node must start while a peer hostname is unresolvable";
    ASSERT_TRUE(n0.awaitLeader(15'000ms) || n1.awaitLeader(15'000ms)) << "no consensus with the resolvable peer";
    ASSERT_EQ(defineShardTopic({&n0, &n1}, kAsymPartitions), 0);
    // The shard planes of the two live nodes verify each other; node 2 stays
    // unverified (its publication is still Resolving, re-added on each node's passes).
    ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 1) == PS::Verified; }));
    ASSERT_TRUE(eventually([&] { return peerState(n1, 0, 0) == PS::Verified; }));
    EXPECT_EQ(peerState(n0, 0, 2), PS::Unverified);
    EXPECT_FALSE(onPlane(n0, 0, [](WorkerReplicationTransport& t) { return t.allPeersResolved(); }));
    // ... and the cluster serves: an acks=all produce commits on the two live nodes.
    int32_t part = -1;
    for (int32_t p = 0; p < kAsymPartitions && part < 0; ++p)
        if (staticLeader(p, 3) != 2) part = p;
    ASSERT_GE(part, 0);
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        int32_t leader = -1;
        ASSERT_TRUE(eventually([&] {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == part && (pi.leaderId == 0 || pi.leaderId == 1))
                    leader = pi.leaderId;
            return leader >= 0;
        }, 400)) << "no live partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, 0, part,
                                    /*acks=all*/1, makeBatch(4), /*maxAttempts=*/2000);
        ASSERT_TRUE(ack.has_value()) << "acks=all never committed with an unresolvable third peer";
    }
    n1.stop(); n0.stop();
}

// ===========================================================================
// DR-15 fix round 1 (MEDIUM-3): node 2 starts LATE and its hostname only
// becomes resolvable then (the SAME name — no channel swap): nodes 0 and 1
// learn it from their cold resolver thread, re-add every peer publication to
// it (consensus sink, internal plane, shard plane) on that event, and node 2
// catches up and rejoins the ISR.  Node 2's OWN name never resolves for itself
// (k8s headless record before Ready — MEDIUM-1): it binds its peer-facing
// inbound on the wildcard and still starts.
// ===========================================================================
TEST(UnresolvedPeerStartupRegression, LatePeerJoinsAfterItsHostnameResolvesRegression) {
    TmpDir d0{"late0"}, d1{"late1"}, d2{"late2"}, cdir{"latec"};
    const auto ports = ShardPorts::discover(3, 1);
    const std::string kLate = "rune-late.test";
    std::atomic<bool> lateResolvable{false};
    auto cfgFor = [&](int32_t id, const fs::path& dir) {
        auto c = asymConfig(id, dir, ports, 3, /*workers=*/1);
        c.replicaLagMaxMs = 500;                              // the absent peer leaves the ISR fast
        for (auto& p : c.peers)
            if (p.id == 2) p.host = kLate;
        return c;
    };
    auto peerView = [&](const std::string& host, std::uint32_t& ip) {
        if (host != kLate || !lateResolvable.load()) return false;
        ip = htonl(INADDR_LOOPBACK);
        return true;
    };
    BrokerNode n0{cfgFor(0, d0.path)}, n1{cfgFor(1, d1.path)};
    n0.setPeerResolveFnForTest(peerView);
    n1.setPeerResolveFnForTest(peerView);
    ASSERT_NO_THROW(n0.start());
    ASSERT_NO_THROW(n1.start());
    ASSERT_TRUE(n0.awaitLeader(15'000ms) || n1.awaitLeader(15'000ms));
    ASSERT_EQ(defineShardTopic({&n0, &n1}, kAsymPartitions), 0);

    int32_t part = -1;
    for (int32_t p = 0; p < kAsymPartitions && part < 0; ++p)
        if (staticLeader(p, 3) == 0) part = p;
    ASSERT_GE(part, 0);
    ShardTestClient c(cdir.path);
    const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid0, 0);
    auto produceAll = [&](int records) {
        int32_t leader = -1;
        if (!eventually([&] {
                for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                    if (pi.topicId == 0 && pi.partition == part && (pi.leaderId == 0 || pi.leaderId == 1))
                        leader = pi.leaderId;
                return leader >= 0;
            }, 400))
            return false;
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        if (sid <= 0) return false;
        return c.produceUntilOk(workerIngressChannel(ports, leader, 0), kClientStream, sid, 0, part,
                                /*acks=all*/1, makeBatch(records), /*maxAttempts=*/2000)
            .has_value();
    };
    ASSERT_TRUE(produceAll(4)) << "acks=all never committed on the two live nodes";

    // Node 2's pod is up now: its name resolves for the others; it never resolves for itself.
    lateResolvable = true;
    BrokerNode n2{cfgFor(2, d2.path)};
    n2.setPeerResolveFnForTest([](const std::string&, std::uint32_t&) { return false; });
    ASSERT_NO_THROW(n2.start()) << "a node must start while its OWN hostname does not resolve";

    ASSERT_TRUE(eventually([&] { return peerState(n0, 0, 2) == PS::Verified; }, 4000))
        << "node 0's shard plane never attached the late-resolving node 2";
    ASSERT_TRUE(eventually([&] { return peerState(n2, 0, 0) == PS::Verified; }, 4000));
    ASSERT_TRUE(eventually([&] { return rune::node::test::leoOrMinusOne(n2, 0, part) >= 4; }, 4000))
        << "node 2 never caught up after joining late";
    ASSERT_TRUE(produceAll(4));
    ASSERT_TRUE(eventually([&] { return rune::node::test::leoOrMinusOne(n2, 0, part) >= 8; }, 4000))
        << "node 2 does not receive new records (not back in the replication set)";
    n2.stop(); n1.stop(); n0.stop();
}

// ===========================================================================
// DR-15 fix round 2 (LOW-4): a client's reply-channel HOSTNAME (dr-e2e clients
// send `endpoint=<container>:<port>`) is resolved by the cold resolver thread
// and the egress publication is added on the numeric endpoint — never resolved
// by a driver conductor (worker 0's thread under fused placement).  The name
// below exists only in the node's resolver (Aeron's own getaddrinfo cannot
// resolve it): RED before — the Connect never completed.
// ===========================================================================
TEST(UnresolvedPeerStartupRegression, ClientReplyHostnameResolvesOffThreadRegression) {
    TmpDir dir{"crh"}, cdir{"crhc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true)};
    node.setPeerResolveFnForTest([](const std::string& host, std::uint32_t& ip) {
        if (host != "rune-client.test") return false;
        ip = htonl(INADDR_LOOPBACK);
        return true;
    });
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineShardTopic({&node}), 0);
    {
        ShardTestClient c(cdir.path);
        c.advertiseEgressHost("rune-client.test");
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0) << "a Connect with a reply HOSTNAME never completed";
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kClientStream, sid, 0, 0,
                                    /*acks=*/1, makeBatch(3));
        ASSERT_TRUE(ack.has_value()) << "no worker ack on the resolved reply channel";
    }
    {
        ShardTestClient bad(cdir.path);
        bad.advertiseEgressHost("rune-client-unknown.invalid");
        EXPECT_LT(bad.connect(gatewayChannel(ports, 0), kClientStream), 0)
            << "an unresolvable reply host must be dropped (the client retries)";
    }
    node.stop();
}

// DR-15 fix round 2 (MEDIUM-B): a LEGACY node's control-thread add brokers (one
// may hold an undelivered publication — a peer resolved during shutdown) are
// released in stop(), while the node client is still alive.
TEST(UnresolvedPeerStartupRegression, LegacyAddBrokersAreReleasedInStopRegression) {
    TmpDir d0{"lgb0"};
    const auto ports = ShardPorts::discover(2, 1);
    auto cfg = makeShardConfig(0, d0.path, ports, 2, 1, /*fused=*/false);
    cfg.workerMode       = NodeConfig::WorkerMode::Legacy;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    for (auto& p : cfg.peers)
        if (p.id == 1) p.host = "rune-late.test";
    std::atomic<bool> resolvable{false};
    BrokerNode n0{cfg};
    n0.setPeerResolveFnForTest([&](const std::string& host, std::uint32_t& ip) {
        if (host != "rune-late.test" || !resolvable.load()) return false;
        ip = htonl(INADDR_LOOPBACK);
        return true;
    });
    ASSERT_NO_THROW(n0.start());
    ASSERT_GE(n0.legacyPlaneBrokerCountForTest(), 1u) << "legacy planes must re-add through the control thread";
    resolvable = true;   // the peer resolves while the node shuts down: a re-add may be in flight
    n0.stop();
    EXPECT_EQ(n0.legacyPlaneBrokerCountForTest(), 0u)
        << "add brokers (and any undelivered publication) must be released before the client closes";
}

// Catalog history Task 17 fix round 1 (C1): co-partition group keys are a pure function of the group NAME, so brokers
// whose boot sets differ (node 0 booted with no topic, node 2 with {b1}, node 1 with {b1, a1, b2}) still place every
// partition on the same worker -- the per-worker replication planes (worker w <-> peer worker w) then carry it.
// Every step fails fast and names the (node, t, p) it fails on:
//   (f)  the worker maps agree across brokers  -- RED: sorted boot numbering (node 2 numbers "b" as -1), or the
//        mapCoPartitionGroup call deleted from runCatalogCreate_;
//   (f2) on every broker each replica is driven on its owner worker's replication plane -- RED: the boot or the
//        runtime replica build derives the plane from a different key than the WorkerPool router;
//   (g)/(h) end to end: acks=all commits and every broker reaches LEO 1, under one shared deadline per step.
TEST(ShardModeRegression, CoPartitionedTopicsReplicateAcrossBrokersWithDifferentBootHistoriesRegression) {
    using rune::metadata::coPartitionGroupKey;
    using rune::metadata::workerIdForShard;
    // Non-vacuity: grouped "b" differs from the ungrouped topic 0 for some p.
    {
        bool differs = false;
        for (int32_t p = 0; p < 4; ++p)
            differs = differs || workerIdForShard({0, p}, 4) != workerIdForShard({coPartitionGroupKey("b"), p}, 4);
        ASSERT_TRUE(differs) << "pick other group names: the placement assertion would be vacuous";
        // Node 2's boot set {b1} would number "b" as -1 under sorted numbering; that must be detectable.
        bool bDiffersFromSortedMinus1 = false;
        for (int32_t p = 0; p < 4; ++p)
            bDiffersFromSortedMinus1 = bDiffersFromSortedMinus1 ||
                workerIdForShard({coPartitionGroupKey("b"), p}, 4) != workerIdForShard({-1, p}, 4);
        ASSERT_TRUE(bDiffersFromSortedMinus1) << "pick other group names: a boot-numbering regression would be invisible";
    }
    TmpDir d0{"cph0"}, d1{"cph1"}, d2{"cph2"}, cdir{"cphc"};
    const fs::path dirs[3] = {d0.path, d1.path, d2.path};
    const auto ports = ShardPorts::discover(3, 4);
    std::unique_ptr<BrokerNode> n[3];
    auto boot = [&](int i) {
        n[i] = std::make_unique<BrokerNode>(makeShardConfig(i, dirs[i], ports, 3, 4, /*fused=*/false));
        n[i]->start();
    };
    auto all = [&] { return std::vector<BrokerNode*>{n[0].get(), n[1].get(), n[2].get()}; };
    auto awaitAnyLeader = [&] {
        return eventually([&] { return n[0]->awaitLeader(1ms) || n[1]->awaitLeader(1ms) || n[2]->awaitLeader(1ms); }, 3000);
    };
    auto grouped = [](const char* name, const char* group) {
        TopicSpec t; t.name = name; t.partitionCount = 4; t.coPartitionGroup = group; t.partitionKey = "k";
        return t;
    };
    auto restart = [&](int i) {
        n[i]->stop();
        n[i].reset();
        boot(i);
        ASSERT_TRUE(awaitAnyLeader());
    };

    for (int i = 0; i < 3; ++i) boot(i);
    ASSERT_TRUE(awaitAnyLeader());
    ASSERT_EQ(rune::node::test::TestCluster(all()).defineTopic(grouped("b1", "b")), 0);
    ASSERT_NO_FATAL_FAILURE(restart(2));
    {
        rune::node::test::TestCluster c(all());
        ASSERT_EQ(c.defineTopic(grouped("a1", "a")), 1);
        ASSERT_EQ(c.defineTopic(grouped("b2", "b")), 2);
    }
    ASSERT_NO_FATAL_FAILURE(restart(1));
    ASSERT_TRUE(eventually([&] {
        for (int i = 0; i < 3; ++i)
            for (int32_t t = 0; t < 3; ++t)
                if (!n[i]->catalogTopicServesForTest(t)) return false;
        return true;
    }, 4000)) << "the restarted brokers never served every topic";

    // (f) every node places every partition on the same worker.
    for (int32_t t = 0; t < 3; ++t)
        for (int32_t p = 0; p < 4; ++p) {
            const int32_t w0 = n[0]->workerForPartitionForTest(t, p);
            ASSERT_EQ(w0, n[1]->workerForPartitionForTest(t, p)) << "t " << t << " p " << p << " node 1";
            ASSERT_EQ(w0, n[2]->workerForPartitionForTest(t, p)) << "t " << t << " p " << p << " node 2";
        }

    // (f2) on every node, every replica is driven on its owner worker's replication plane (owner worker == plane
    // index).  Node 0: every topic is runtime-built.  Nodes 1/2: boot topics come from buildReplication, the rest are
    // runtime-built.
    for (int i = 0; i < 3; ++i)
        for (int32_t t = 0; t < 3; ++t)
            for (int32_t p = 0; p < 4; ++p) {
                const int32_t w = n[i]->workerForPartitionForTest(t, p);
                const auto* want =
                    static_cast<const rune::replication::PartitionTransport*>(n[i]->perWorkerPlaneForTest(w));
                ASSERT_NE(want, nullptr) << "node " << i << " has no plane " << w;
                const auto* got = n[i]->onOwnerThreadForTest(
                    t, p, [](rune::replication::PartitionReplica& r) { return r.transportForTest(); });
                ASSERT_EQ(got, want) << "node " << i << " t " << t << " p " << p
                                     << ": replica driven on a plane other than its owner worker " << w;
            }

    // (g) one acks=all record per (t, p), produced to its leader.
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        const auto gDeadline = std::chrono::steady_clock::now() + 30s;
        for (int32_t t = 0; t < 3; ++t)
            for (int32_t p = 0; p < 4; ++p) {
                int32_t leader = -1;
                ASSERT_TRUE(eventually([&] {
                    for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                        if (pi.topicId == t && pi.partition == p) leader = pi.leaderId;
                    return leader >= 0;
                }, 400)) << "no leader for t " << t << " p " << p;
                const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
                ASSERT_GT(sid, 0);
                const int32_t w = n[leader]->workerForPartitionForTest(t, p);
                auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, w), kClientStream, sid, t, p,
                                            /*acks=all*/1, makeBatch(1), /*maxAttempts=*/10);
                ASSERT_TRUE(ack.has_value()) << "acks=all not committed for t " << t << " p " << p;
                ASSERT_LT(std::chrono::steady_clock::now(), gDeadline) << "producing 12 records took > 30 s";
            }
    }

    // (h) rf 0 = every broker holds every partition: each must reach LEO 1 everywhere.  One shared deadline; every
    // laggard is reported at once.
    {
        const auto hDeadline = std::chrono::steady_clock::now() + 20s;
        std::string lagging;
        for (;;) {
            lagging.clear();
            for (int i = 0; i < 3; ++i)
                for (int32_t t = 0; t < 3; ++t)
                    for (int32_t p = 0; p < 4; ++p) {
                        const auto leo = rune::node::test::leoOrMinusOne(*n[i], t, p);
                        if (leo != 1)
                            lagging += "node " + std::to_string(i) + " t " + std::to_string(t) + " p " +
                                       std::to_string(p) + " LEO " + std::to_string(leo) + "; ";
                    }
            if (lagging.empty() || std::chrono::steady_clock::now() >= hDeadline) break;
            std::this_thread::sleep_for(20ms);
        }
        EXPECT_TRUE(lagging.empty()) << "not replicated everywhere within 20 s: " << lagging;
    }
    for (int i = 2; i >= 0; --i) n[i]->stop();
}
