// TransientTopicDataPathTest.cpp — Phase 2b E2, in-process BrokerNodes over
// REAL Aeron UDP: a TRANSIENT topic (static RUNE_TOPICS persistence=transient,
// topic 0 "tt") produces and is consumed in shard mode (the default: produce on
// the worker's ingress, Records pushed by the worker) and in legacy mode
// (produce + PushRead through the gateway); acks=all is replicated to a
// majority before the ack (3 nodes); a restart keeps the topic and empties it;
// a transient topic DEFINED AT RUNTIME (DefineTopic persistence=1) is served
// once its DDL OK arrives (C1: the OK is held until the topic serves) — through
// the legacy gateway this answered UnknownPartition (8) before E2.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
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
               (std::string("rune_tt_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
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

NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int nodes, bool shard,
                      int32_t ringBytes = 0 /* 0 = the default 8 MiB */) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = shard ? NodeConfig::WorkerMode::Shard : NodeConfig::WorkerMode::Legacy;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = shard;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    if (ringBytes > 0) cfg.memoryLogRingBytes = ringBytes;   // a power of two (RUNE_TRANSIENT_LOG_RING_BYTES rule)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t;
    t.name = "tt"; t.partitionCount = 1; t.topicId = 0;
    t.persistence = PersistenceMode::Transient;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gatewayChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[node]);
}
std::string workerIngressChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.ib[node]);
}
std::size_t pushedAt(const ShardTestClient& c, int64_t base) {
    std::size_t n = 0;
    for (const auto& r : c.records()) n += r.baseOffset == base ? 1u : 0u;
    return n;
}
/// A DefineTopic descriptor: {id Int64 (key), hp Int64} (validate=false: records are not checked).
std::vector<uint8_t> ttBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("presence", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}
/// DefineTopic(name, 1 partition, persistence=transient) over the wire; retries
/// Internal while the admin thread does not yet lead __topic_schemas (the
/// documented DDL settle — bounded, not a correctness gate).  Returns the
/// topic id, or -1.
int32_t defineTransient(ShardTestClient& c, const std::string& gw, int64_t sid, const std::string& name) {
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto r = c.awaitDefine(c.sendDefineTopic(gw, kClientStream, sid, name, 1, ttBlob(), /*persistence=*/1),
                                     std::chrono::steady_clock::now() + 10s);
        if (r && r->errorCode == 0) return r->topicId;
        if (r && r->errorCode != static_cast<uint16_t>(rune::protocol::client::ErrorCode::Internal)) return -1;
        std::this_thread::sleep_for(20ms);   // admin leadership settling
    }
    return -1;
}

} // namespace

TEST(TransientTopicDataPathRegression, ShardProduceAndSubscribeEarliestLatestOffsetRegression) {
    TmpDir dir{"shard"}, cdir{"shard_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 3, 6}) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0,
                                        /*acks=*/1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "a transient produce on the worker shard was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        // Earliest: the whole ring.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 1; })) << "an earliest subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u);
        EXPECT_EQ(pushedAt(c, 3), 1u);
        // An explicit offset.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*fromOffset=*/6));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 2; })) << "an offset subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u) << "the offset subscription replayed below its offset";
        // Latest: never replays — produce until its subscription delivers; its FIRST batch is new.
        std::set<int64_t> before;
        for (const auto& r : c.records()) before.insert(r.subscriptionId);
        ASSERT_EQ(before.size(), 2u);
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Latest=*/-1));
        int64_t latestFirstBase = -1;
        for (int i = 0; i < 20 && latestFirstBase < 0; ++i) {
            ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                         makeBatch(3)).has_value());
            (void)c.pollUntil([&] {
                for (const auto& r : c.records())
                    if (!before.count(r.subscriptionId)) { latestFirstBase = r.baseOffset; return true; }
                return false;
            });
        }
        ASSERT_GE(latestFirstBase, 0) << "the latest subscription never delivered";
        EXPECT_GE(latestFirstBase, 9) << "a Latest subscription replayed old records";
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, LegacyModeProduceAndSubscribeRegression) {
    TmpDir dir{"legacy"}, cdir{"legacy_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 2}) {
            auto ack = c.produceUntilOk(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*acks=*/1, makeBatch(2));
            ASSERT_TRUE(ack.has_value()) << "a transient produce through the gateway was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 2) >= 1; })) << "the gateway never pushed the transient topic";
        EXPECT_EQ(pushedAt(c, 0), 1u);
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, ThreeNodeAcksAllIsOnAMajorityBeforeTheAckRegression) {
    TmpDir d0{"m0"}, d1{"m1"}, d2{"m2"}, cdir{"mc"};
    const auto ports = Ports::discover(3);
    BrokerNode n0{makeConfig(0, d0.path, ports, 3, true)};
    BrokerNode n1{makeConfig(1, d1.path, ports, 3, true)};
    BrokerNode n2{makeConfig(2, d2.path, ports, 3, true)};
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
            if (leader < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader), kClientStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " on a transient topic not committed";
        }
    }
    // While the nodes run (the data is in memory only): every acked record is on a majority.
    int holders = 0;
    for (BrokerNode* n : {&n0, &n1, &n2})
        holders += (hasReplica(*n, 0, 0) && leoOnOwner(*n, 0, 0) >= 20) ? 1 : 0;
    EXPECT_GE(holders, 2) << "an acks=all ack on a transient topic before a majority held the records";
    n0.stop(); n1.stop(); n2.stop();
}

TEST(TransientTopicDataPathRegression, RestartKeepsTheTopicAndEmptiesItRegression) {
    TmpDir dir{"restart"}, c1{"restart_c1"}, c2{"restart_c2"};
    const auto ports = Ports::discover(1);
    const auto cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/true);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c1.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            for (int i = 0; i < 2; ++i)
                ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                             makeBatch(3)).has_value());
        }
        node.stop();
    }
    EXPECT_FALSE(fs::exists(dir.path / "topics" / "tt")) << "a transient topic never writes a partition directory";
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c2.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "the transient topic must exist after a restart";
            EXPECT_EQ(ack->baseOffset, 0) << "a transient topic restarts EMPTY";
            ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
            ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 0) >= 1; }));
            EXPECT_EQ(c.records().size(), 1u) << "pre-restart records came back";
        }
        node.stop();
    }
}

// C1 × E2 (the DDL final-fix concern): a transient topic DEFINED AT RUNTIME on a
// legacy-runtime node.  Its DDL OK is held until it serves (DdlAnswerGate), so
// the first produce through the gateway is served by the owning worker — it
// answered UnknownPartition (8) before E2 — and a subscription from earliest
// gets both batches.
TEST(TransientTopicDataPathRegression, LegacyRuntimeTransientDefineThenProduceAndSubscribeRegression) {
    TmpDir dir{"rt_legacy"}, cdir{"rt_legacy_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        const int32_t tid = defineTransient(c, gatewayChannel(ports, 0), sid, "presence_rt");
        ASSERT_GE(tid, 0) << "DefineTopic(transient) failed";
        for (int64_t want : {0, 2}) {
            auto ack = c.produceUntilOk(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, /*acks=all*/1,
                                        makeBatch(2));
            ASSERT_TRUE(ack.has_value()) << "a runtime transient topic was not served (was UnknownPartition)";
            EXPECT_EQ(ack->baseOffset, want);
        }
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 2) >= 1; }));
        EXPECT_EQ(pushedAt(c, 0), 1u);
    }
    node.stop();
}
