// TransientTopicDataPathTest.cpp — Phase 2b E2, in-process BrokerNodes over
// REAL Aeron UDP: a TRANSIENT topic (created by DDL with persistence=transient,
// topic 0 "tt") produces and is consumed in shard mode (the default: produce on
// the worker's ingress, Records pushed by the worker) and in legacy mode
// (produce + PushRead through the gateway); acks=all is replicated to a
// majority before the ack (3 nodes); a restart keeps the topic and empties it;
// a transient topic DEFINED AT RUNTIME (DefineTopic persistence=1) is served
// once its DDL OK arrives (C1: the OK is held until the topic serves) — through
// the legacy gateway this answered UnknownPartition (8) before E2.
#include "support/TestCluster.h"
#include <rune/node/NamespacePaths.h>
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"
#include "support/TransientMatrixExpectations.h"

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <random>
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
        // pid + a per-dir random nonce (no shared counter: no static / atomic state).
        const uint64_t nonce = (static_cast<uint64_t>(std::random_device{}()) << 32) ^ std::random_device{}();
        path = fs::temp_directory_path() /
               (std::string("rune_tt_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(nonce));
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
    return cfg;
}

/// Topic 0 "tt": transient unless a cell asks for its disk twin — created through the catalog (topics exist
/// only through DDL) once the nodes are up.
int32_t defineTt(std::vector<BrokerNode*> nodes, PersistenceMode persistence = PersistenceMode::Transient,
                 int16_t rf = 0, int32_t partitions = 1) {
    TopicSpec t;
    t.name = "tt"; t.partitionCount = partitions; t.replicationFactor = rf;
    t.persistence = persistence;
    return rune::node::test::TestCluster(std::move(nodes)).defineTopic(t);
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
    ASSERT_EQ(defineTt({&node}), 0);
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
        // Until the Subscribe registers, a produce is not delivered to it: re-produce with a short per-attempt
        // window (the overall budget stays the old 20 x 5 s), never a whole 5 s wait on a lost attempt.
        const auto latestDeadline = std::chrono::steady_clock::now() + 100s;
        while (latestFirstBase < 0 && std::chrono::steady_clock::now() < latestDeadline) {
            ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                         makeBatch(3)).has_value());
            (void)c.pollFor(250ms, [&] {
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
    ASSERT_EQ(defineTt({&node}), 0);
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
    ASSERT_EQ(defineTt({&n0, &n1, &n2}), 0);
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
        ASSERT_EQ(defineTt({&node}), 0);
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
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(dir.path, 0) / "tt")) << "a transient topic never writes a partition directory";
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
    NodeConfig cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/false);
    BrokerNode node{cfg};
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

// ---------------------------------------------------------------------------
// Phase 2b E2 interaction matrix — the BrokerNode slice: both worker modes x
// {3 produce acks, 3 subscribe starts} x {Steady, EvictedCursor, Restart,
// CommitLandsMidOp} over REAL Aeron (48 cells; shard = produce on the worker's
// ingress, Records pushed by the worker's owned subscriptions — the loop the
// unit matrix cannot reach), plus LeaderMovesMidOp on 3 nodes.  Outcomes from
// support/TransientMatrixExpectations.h (alias `mx`: `tm` collides with struct tm).
// ---------------------------------------------------------------------------
namespace {
namespace mx = rune::node::test::tmatrix;

std::vector<uint8_t> valueBatch(int records, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}

struct SliceParam {
    mx::Mode   mode;
    mx::Timing timing;
};
std::string sliceName(const testing::TestParamInfo<SliceParam>& i) {
    return std::string(i.param.mode == mx::Mode::Shard ? "Shard" : "Legacy") + "_" +
           std::string(mx::name(i.param.timing));
}

/// The first base a subscription created after `known` delivered (polls; -1 = none).
int64_t firstOfNewSubscription(ShardTestClient& c, const std::set<int64_t>& known, int64_t* subOut = nullptr,
                               std::optional<std::chrono::milliseconds> budget = std::nullopt) {
    int64_t first = -1;
    const auto pred = [&] {
        for (const auto& r : c.records())
            if (!known.count(r.subscriptionId)) { first = r.baseOffset; if (subOut) *subOut = r.subscriptionId; return true; }
        return false;
    };
    if (budget) (void)c.pollFor(*budget, pred);
    else (void)c.pollUntil(pred);
    return first;
}
std::set<int64_t> knownSubscriptions(const ShardTestClient& c) {
    std::set<int64_t> s;
    for (const auto& r : c.records()) s.insert(r.subscriptionId);
    return s;
}
} // namespace

class TransientSliceRegression : public testing::TestWithParam<SliceParam> {};

TEST_P(TransientSliceRegression, ProduceAndSubscribeCells) {
    const auto [mode, timing] = GetParam();
    const bool shard = mode == mx::Mode::Shard;
    const bool evict = timing == mx::Timing::EvictedCursor;
    const mx::Commit commit = timing == mx::Timing::CommitLandsMidOp ? mx::Commit::Uncommitted : mx::Commit::Committed;
    const mx::Entity e{mx::Persist::Transient, mode, mx::Role::Leader, commit};
    TmpDir dir{"slice"}, c1{"slice_c1"}, c2{"slice_c2"};
    const auto ports = Ports::discover(1);
    NodeConfig cfg = makeConfig(0, dir.path, ports, 1, shard, /*ringBytes=*/evict ? 16 * 1024 : 0);
    auto node = std::make_unique<BrokerNode>(cfg);
    node->start();
    ASSERT_TRUE(node->awaitLeader(10'000ms));
    // The committed cells run on topic 0 "tt" (created here through the catalog); the CommitLandsMidOp cell
    // defines its own topic below, over the wire.
    if (commit != mx::Commit::Uncommitted) ASSERT_EQ(defineTt({node.get()}), 0);
    auto client = std::make_unique<ShardTestClient>(c1.path);
    int64_t sid = client->connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    int32_t tid = 0;   // "tt" (defineTt)
    if (commit == mx::Commit::Uncommitted) {
        // CommitLandsMidOp over a real node: the uncommitted window is internal (the DDL OK
        // is held until the topic serves — DdlAnswerGate, C1); the cell asserts the first op
        // after the OK is served, never refused for good (OkAfterCommit).
        tid = defineTransient(*client, gatewayChannel(ports, 0), sid, "tt_slice_rt");
        ASSERT_GE(tid, 0);
    }
    const std::string produceCh = shard ? workerIngressChannel(ports, 0) : gatewayChannel(ports, 0);
    std::vector<int64_t> acked;   // acked batch bases (2 records each)
    auto produce = [&](uint8_t acks, std::size_t valueBytes) -> std::optional<int64_t> {
        auto ack = client->produceUntilOk(produceCh, kClientStream, sid, tid, 0, acks, valueBatch(2, valueBytes));
        if (!ack) return std::nullopt;
        acked.push_back(ack->baseOffset);
        return ack->baseOffset;
    };
    // The timing's preconditions.
    for (int i = 0; i < (evict ? 40 : 2); ++i) ASSERT_TRUE(produce(0, evict ? 1000 : 8).has_value());
    if (timing == mx::Timing::Restart) {
        client.reset();
        node->stop();
        node.reset();
        node = std::make_unique<BrokerNode>(cfg);
        node->start();
        ASSERT_TRUE(node->awaitLeader(10'000ms));
        client = std::make_unique<ShardTestClient>(c2.path);
        sid = client->connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        acked.clear();   // the ring restarted empty
    }

    // ---- 3 produce cells ------------------------------------------------------
    for (mx::Op op : {mx::Op::ProduceAcks0, mx::Op::ProduceAcks1, mx::Op::ProduceAcksAll}) {
        const mx::Outcome want = mx::expect(op, e, timing);
        SCOPED_TRACE(std::string(mx::name(op)) + " expect " + std::string(mx::name(want)));
        const int64_t expectBase = acked.empty() ? 0 : acked.back() + 2;
        const auto base = produce(mx::wireAcks(op), 8);
        ASSERT_TRUE(base.has_value()) << "a transient produce was never acked";
        EXPECT_EQ(*base, expectBase) << "not contiguous with the acked history";
        if (want == mx::Outcome::OkEmptyAfterRestart && op == mx::Op::ProduceAcks0)
            EXPECT_EQ(*base, 0) << "a transient topic restarted non-empty";
    }

    // ---- 3 subscribe cells ----------------------------------------------------
    for (mx::Op op : {mx::Op::SubEarliest, mx::Op::SubLatest, mx::Op::SubOffset}) {
        const mx::Outcome want = mx::expect(op, e, timing);
        SCOPED_TRACE(std::string(mx::name(op)) + " expect " + std::string(mx::name(want)));
        const auto known = knownSubscriptions(*client);
        const int64_t hwBefore = acked.back() + 2;   // every acked record is committed (single node)
        const int64_t from = op == mx::Op::SubEarliest ? -2 : op == mx::Op::SubLatest ? -1 : 3;
        ASSERT_TRUE(client->subscribe(gatewayChannel(ports, 0), kClientStream, sid, tid, 0, from));
        if (op == mx::Op::SubLatest) {
            // Latest never replays: produce until the new subscription delivers; its FIRST batch is new.
            int64_t first = -1;
            // Until the Subscribe registers, a produce is not delivered to it: re-produce with a short
            // per-attempt window (overall budget = the old 20 x 5 s), never a whole 5 s wait per lost attempt.
            const auto latestDeadline = std::chrono::steady_clock::now() + 100s;
            while (first < 0 && std::chrono::steady_clock::now() < latestDeadline) {
                ASSERT_TRUE(produce(1, 8).has_value());
                first = firstOfNewSubscription(*client, known, nullptr, 250ms);
            }
            ASSERT_GE(first, 0) << "the latest subscription never delivered";
            EXPECT_GE(first, hwBefore) << "Latest replayed an old record";
            continue;
        }
        const int64_t first = firstOfNewSubscription(*client, known);
        ASSERT_GE(first, 0) << "the subscription delivered nothing (a stall)";
        switch (want) {
        case mx::Outcome::OkFromRingStart:   // resumed at the ring start (> the cursor), never a stall
            EXPECT_GT(first, op == mx::Op::SubEarliest ? 0 : 3) << "read an evicted offset?";
            EXPECT_LE(first, acked.back());
            break;
        default:                             // Ok / OkEmptyAfterRestart / OkAfterCommit
            EXPECT_EQ(first, op == mx::Op::SubEarliest ? acked.front() : int64_t{2})
                << "whole-batch delivery from the batch containing the cursor";
            break;
        }
    }
    client.reset();
    node->stop();
}

INSTANTIATE_TEST_SUITE_P(
    Cells, TransientSliceRegression,
    testing::Values(SliceParam{mx::Mode::Shard, mx::Timing::Steady}, SliceParam{mx::Mode::Legacy, mx::Timing::Steady},
                    SliceParam{mx::Mode::Shard, mx::Timing::EvictedCursor},
                    SliceParam{mx::Mode::Legacy, mx::Timing::EvictedCursor},
                    SliceParam{mx::Mode::Shard, mx::Timing::Restart}, SliceParam{mx::Mode::Legacy, mx::Timing::Restart},
                    SliceParam{mx::Mode::Shard, mx::Timing::CommitLandsMidOp},
                    SliceParam{mx::Mode::Legacy, mx::Timing::CommitLandsMidOp}),
    sliceName);

// LeaderMovesMidOp on 3 nodes (shard): acks=all records acked before the partition
// leader stops are all on the new leader — the next produce lands right after them
// (no gap, nothing lost) and a subscription from earliest on the new leader gets
// every one of them.  ProduceAcksAll: RetriableNotLeader then Ok (produceUntilOk
// retries); SubEarliest on the new leader: Ok.
TEST(TransientSliceRegression, ThreeNodeLeaderMovesMidOpLosesNoAckedRecordRegression) {
    TmpDir d0{"lm0"}, d1{"lm1"}, d2{"lm2"}, cdir{"lmc"};
    const auto ports = Ports::discover(3);
    std::unique_ptr<BrokerNode> nodes[3];
    const TmpDir* dirs[3] = {&d0, &d1, &d2};
    for (int i = 0; i < 3; ++i) nodes[i] = std::make_unique<BrokerNode>(makeConfig(i, dirs[i]->path, ports, 3, true));
    for (auto& n : nodes) n->start();
    ASSERT_TRUE(nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(defineTt({nodes[0].get(), nodes[1].get(), nodes[2].get()}), 0);
    ShardTestClient c(cdir.path);
    auto leaderVia = [&](int gwNode, int notThis) {
        const int64_t s = c.connect(gatewayChannel(ports, gwNode), kClientStream);
        for (int i = 0; s > 0 && i < 400; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, gwNode), kClientStream, s))
                if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId >= 0 && pi.leaderId != notThis)
                    return static_cast<int>(pi.leaderId);
            std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        return -1;
    };
    const int leader = leaderVia(0, -1);
    ASSERT_GE(leader, 0);
    const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
    ASSERT_GT(sid, 0);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, leader), kClientStream, sid, 0, 0, /*acks=all*/1,
                                     makeBatch(2)).has_value());
    nodes[leader]->stop();        // the partition leader goes away mid-stream
    nodes[leader].reset();
    const int survivor = (leader + 1) % 3;
    const int next = leaderVia(survivor, leader);
    ASSERT_GE(next, 0) << "no new partition leader";
    const int64_t sidN = c.connect(gatewayChannel(ports, next), kClientStream);
    ASSERT_GT(sidN, 0);
    const auto ack = c.produceUntilOk(workerIngressChannel(ports, next), kClientStream, sidN, 0, 0, 1, makeBatch(2));
    ASSERT_TRUE(ack.has_value()) << "the new leader never took a produce";
    EXPECT_EQ(ack->baseOffset, 10) << "an acks=all record acked by the old leader is missing on the new one";
    ASSERT_TRUE(c.subscribe(gatewayChannel(ports, next), kClientStream, sidN, 0, 0, /*Earliest=*/-2));
    ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 10) >= 1; }));
    for (int64_t b : {0, 2, 4, 6, 8}) EXPECT_GE(pushedAt(c, b), 1u) << "acked batch " << b << " lost in the failover";
    for (auto& n : nodes) if (n) n->stop();
}

// ---------------------------------------------------------------------------
// Task 11 carry (ledger): a partition leader DEMOTED while it stays alive (a
// controlled shutdown hands its partitions off before the process exits) AFTER
// it accepted a read_committed subscription.  read_committed is served from a
// LEADER's RC state only (RcState I2), so the demoted node can no longer serve
// it — and must not go silent: it answers the subscription's Subscribe
// correlation id with ErrorResponse{NotLeader} (the retriable re-route every
// unservable-here subscribe gets).  The client then does what rune-client-cpp
// does with it: re-subscribes read_committed from its position on its session
// (the demoted node redirects it to the new leader) and follows the Redirect;
// committed data keeps flowing, nothing below its position is re-delivered.
// Transient and disk, shard and legacy.  Every wait is bounded and fails with
// a message naming the stall.
// ---------------------------------------------------------------------------
namespace {
struct DemoteParam {
    mx::Mode    mode;
    mx::Persist persist;
};
std::string demoteName(const testing::TestParamInfo<DemoteParam>& i) {
    return std::string(i.param.mode == mx::Mode::Shard ? "Shard" : "Legacy") + "_" +
           (i.param.persist == mx::Persist::Transient ? "Transient" : "Disk");
}
constexpr uint8_t kReadCommitted = 1;
constexpr int16_t kCodeNotLeader = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
constexpr int16_t kCodeRedirect  = static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect);

/// The error answered on `corr` (nullopt = none within ~4 x 5 s of polling).
std::optional<ShardTestClient::ErrorSeen> awaitErrorOn(ShardTestClient& c, int64_t corr) {
    std::optional<ShardTestClient::ErrorSeen> seen;
    for (int round = 0; round < 4 && !seen; ++round)
        (void)c.pollUntil([&] {
            for (const auto& e : c.errors())
                if (e.correlationId == corr) { seen = e; return true; }
            return false;
        });
    return seen;
}
std::size_t pushedOn(const ShardTestClient& c, int64_t subId, int64_t base) {
    std::size_t n = 0;
    for (const auto& r : c.records()) n += (r.subscriptionId == subId && r.baseOffset == base) ? 1u : 0u;
    return n;
}
} // namespace

class DemotedLeaderReadCommittedRegression : public testing::TestWithParam<DemoteParam> {};

TEST_P(DemotedLeaderReadCommittedRegression, SubscriptionMovesToTheNewLeaderRegression) {
    const auto [mode, persist] = GetParam();
    const bool shard = mode == mx::Mode::Shard;
    const PersistenceMode pm = persist == mx::Persist::Transient ? PersistenceMode::Transient : PersistenceMode::Disk;
    TmpDir d0{"dm0"}, d1{"dm1"}, d2{"dm2"}, cdir{"dmc"};
    const auto ports = Ports::discover(3);
    std::unique_ptr<BrokerNode> nodes[3];
    const TmpDir* dirs[3] = {&d0, &d1, &d2};
    for (int i = 0; i < 3; ++i)
        nodes[i] = std::make_unique<BrokerNode>(makeConfig(i, dirs[i]->path, ports, 3, shard));
    for (auto& n : nodes) n->start();
    for (auto& n : nodes) ASSERT_TRUE(n->awaitLeader(30'000ms));
    ASSERT_EQ(defineTt({nodes[0].get(), nodes[1].get(), nodes[2].get()}, pm), 0);
    ShardTestClient c(cdir.path);
    c.keepSessionsAlive(true);   // a real client keeps its sessions alive (1 s keepalive)
    auto leaderVia = [&](int gwNode, int notThis) {
        const int64_t s = c.connect(gatewayChannel(ports, gwNode), kClientStream);
        for (int i = 0; s > 0 && i < 400; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, gwNode), kClientStream, s))
                if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId >= 0 && pi.leaderId != notThis)
                    return static_cast<int>(pi.leaderId);
            std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        return -1;
    };
    const int leader = leaderVia(0, -1);
    ASSERT_GE(leader, 0) << "no partition leader advertised";
    const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
    ASSERT_GT(sid, 0);
    auto produceCh = [&](int node) { return shard ? workerIngressChannel(ports, node) : gatewayChannel(ports, node); };
    for (int64_t want : {0, 2, 4}) {
        const auto ack = c.produceUntilOk(produceCh(leader), kClientStream, sid, 0, 0, /*acks=all*/1, makeBatch(2));
        ASSERT_TRUE(ack.has_value()) << "acks=all produce on the leader failed";
        ASSERT_EQ(ack->baseOffset, want);
    }

    // 1. A read_committed subscription accepted by the leader, delivering.
    const int64_t subCorr = c.subscribeIsolated(gatewayChannel(ports, leader), kClientStream, sid, 0, 0,
                                                /*Earliest=*/-2, kReadCommitted);
    ASSERT_GT(subCorr, 0);
    ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 4) >= 1; }))
        << "the leader accepted read_committed but delivered nothing";
    const int64_t oldSub = c.records().back().subscriptionId;
    const int64_t position = 6;   // the next offset the client wants (records 0..5 delivered)

    const std::size_t beforeMove = c.records().size();   // everything pushed from here on is post-demotion

    // 2. The leader is DEMOTED and stays alive.
    ASSERT_TRUE(c.whilePumping([&] { return nodes[leader]->controlledShutdown(20'000ms); }))
        << "the controlled shutdown never handed the partition off";

    // 3. The demoted node tells the client to move — never a silent stall.
    const auto told = awaitErrorOn(c, subCorr);
    ASSERT_TRUE(told.has_value())
        << "SILENT STALL: the demoted leader neither serves the read_committed subscription nor answers it "
           "NotLeader — the client has no signal to move it";
    EXPECT_EQ(told->code, kCodeNotLeader) << "the move signal is the retriable bare NotLeader";

    // 4. The client re-subscribes from its position on its session: the demoted
    //    node redirects it to the new leader, and it follows.
    const int next = leaderVia((leader + 1) % 3, leader);
    ASSERT_GE(next, 0) << "no new partition leader";
    const int64_t reCorr = c.subscribeIsolated(gatewayChannel(ports, leader), kClientStream, sid, 0, 0, position,
                                               kReadCommitted);
    ASSERT_GT(reCorr, 0);
    const auto redirect = awaitErrorOn(c, reCorr);
    ASSERT_TRUE(redirect.has_value()) << "the demoted node never answered the re-subscribe";
    EXPECT_EQ(redirect->code, kCodeRedirect);
    EXPECT_NE(redirect->message.find(std::to_string(ports.client[next])), std::string::npos)
        << "redirected to " << redirect->message << ", not the new leader";
    const int64_t sidN = c.connect(gatewayChannel(ports, next), kClientStream);
    ASSERT_GT(sidN, 0);
    const auto known = knownSubscriptions(c);
    const int64_t movedCorr = c.subscribeIsolated(gatewayChannel(ports, next), kClientStream, sidN, 0, 0, position,
                                                  kReadCommitted);
    ASSERT_GT(movedCorr, 0);

    // 5. Committed data keeps flowing on the moved subscription.
    for (int64_t want : {6, 8}) {
        const auto ack = c.produceUntilOk(produceCh(next), kClientStream, sidN, 0, 0, 1, makeBatch(2));
        ASSERT_TRUE(ack.has_value()) << "the new leader never took a produce";
        ASSERT_EQ(ack->baseOffset, want) << "an acks=all record acked by the old leader is missing";
    }
    int64_t newSub = -1;
    const int64_t first = firstOfNewSubscription(c, known, &newSub);
    ASSERT_GE(first, 0) << "SILENT STALL: the moved read_committed subscription never delivered on the new leader";
    EXPECT_EQ(first, position) << "the moved subscription did not resume at its position";
    ASSERT_TRUE(c.pollUntil([&] { return pushedOn(c, newSub, 8) >= 1; }))
        << "SILENT STALL: committed data stopped flowing after the move";
    // Nothing pushed after the demotion — on the old subscription or the moved
    // one — is below the position, and the demoted node pushed no data at all.
    for (std::size_t i = beforeMove; i < c.records().size(); ++i) {
        const auto& rec = c.records()[i];
        if (rec.bytes.empty()) continue;   // a position note (client version >= 6 only)
        EXPECT_GE(rec.baseOffset, position) << "re-delivered below the position (subscription " << rec.subscriptionId
                                            << ", base " << rec.baseOffset << ")";
        EXPECT_NE(rec.subscriptionId, oldSub) << "the demoted node kept pushing (base " << rec.baseOffset << ")";
    }
    c.whilePumping([&] { nodes[leader]->stop(); });
    for (auto& n : nodes) n->stop();
}

INSTANTIATE_TEST_SUITE_P(Cells, DemotedLeaderReadCommittedRegression,
                         testing::Values(DemoteParam{mx::Mode::Shard, mx::Persist::Transient},
                                         DemoteParam{mx::Mode::Legacy, mx::Persist::Transient},
                                         DemoteParam{mx::Mode::Shard, mx::Persist::Disk},
                                         DemoteParam{mx::Mode::Legacy, mx::Persist::Disk}),
                         demoteName);

// ===========================================================================
// E2 final fix wave (C1 multi-node, USER DECISION option b): committed group
// offsets of a TRANSIENT partition across restarts of a 3-node cluster.  The
// partition's data lineage is committed metadata, confirmed per leadership:
//   - FULL-cluster restart: every ring is unborn, the new leader MINTS a new
//     lineage — the group's committed offset is answered "none" (-1) after the
//     (retriable NotLeader) settling window, never the old offset; the member
//     applies its reset policy (the log start) and gets every post-restart
//     record.  RED (no lineage): 20 was answered after the restart.
//   - ROLLING restart (one node at a time, each catching up before the next):
//     the data survives every step, the lineage is KEPT — the offset stays
//     valid (no false -1).
// ===========================================================================
namespace {
constexpr int16_t kOffsetOk        = 0;
constexpr int16_t kOffsetNotLeader = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);

struct GroupCluster {
    TmpDir d0{"lg0"}, d1{"lg1"}, d2{"lg2"}, cdir{"lgc"};
    Ports  ports = Ports::discover(3);
    std::unique_ptr<BrokerNode> nodes[3];
    bool    crashStops = false;   // controlledShutdownMs = 0: a stop hands nothing off (like a crash)
    int64_t holdMs     = 0;       // NodeConfig::testUserDecisionHoldMs (the N1 window)
    int16_t rf         = 0;       // topic "tt" replication factor (0 = every node)
    int32_t partitions = 1;       // topic "tt" partition count
    int32_t electionTimeoutMs = 0;   // > 0: overrides (the failure detector runs at 3x the heartbeat timeout)
    NodeConfig cfg(int i) {
        NodeConfig c = makeConfig(i, (i == 0 ? d0 : i == 1 ? d1 : d2).path, ports, 3, true);
        if (rf == 1) c.gatewayMinIsr = 1;   // an RF=1 partition can never satisfy minIsr 2
        if (electionTimeoutMs > 0) c.electionTimeoutMs = electionTimeoutMs;
        if (crashStops) c.controlledShutdownMs = 0;
        c.testUserDecisionHoldMs = holdMs;
        return c;
    }
    void start(int i) { nodes[i] = std::make_unique<BrokerNode>(cfg(i)); nodes[i]->start(); }
    /// Topic "tt" (rf / partitions as configured), defined through the catalog on the nodes that are up.
    int32_t defineTopic() {
        std::vector<BrokerNode*> up;
        for (auto& n : nodes) if (n) up.push_back(n.get());
        return defineTt(std::move(up), PersistenceMode::Transient, rf, partitions);
    }
    void stop(int i) { if (nodes[i]) { nodes[i]->stop(); nodes[i].reset(); } }
    int liveNode() const { for (int i = 0; i < 3; ++i) if (nodes[i]) return i; return -1; }
    /// The consensus leader (the group coordinator) as a live node sees it.
    int coordinator() {
        for (int k = 0; k < 400; ++k) {
            for (int i = 0; i < 3; ++i)
                if (nodes[i] && nodes[i]->isLeader()) return i;
            std::this_thread::sleep_for(25ms);   // election settling (poll interval)
        }
        return -1;
    }
    /// topic 0 / partition 0's advertised leader (not `notThis`).
    int partitionLeader(ShardTestClient& c, int notThis = -1) {
        const int gw = liveNode();
        const int64_t s = c.connect(gatewayChannel(ports, gw), kClientStream);
        for (int i = 0; s > 0 && i < 400; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, gw), kClientStream, s))
                if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId >= 0 && pi.leaderId != notThis &&
                    nodes[pi.leaderId] != nullptr)
                    return static_cast<int>(pi.leaderId);
            std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        return -1;
    }
    ~GroupCluster() { for (int i = 0; i < 3; ++i) stop(i); }
};

/// OffsetFetch to a settled answer: retries the retriable NotLeader (the
/// lineage settling window) and a NotCoordinator (the coordinator moving);
/// returns the offset, or -99 on no settled answer.  *pendingSeen counts the
/// NotLeader answers; *answers records every non-pending offset answered.
int64_t settledFetch(GroupCluster& g, ShardTestClient& c, const std::string& group, int* pendingSeen) {
    for (int k = 0; k < 600; ++k) {
        const int coord = g.coordinator();
        if (coord < 0) continue;
        const int64_t sid = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        if (sid <= 0) { std::this_thread::sleep_for(25ms); continue; }
        const auto r = c.offsetFetch(gatewayChannel(g.ports, coord), kClientStream, sid, group, 0, 0);
        if (r && r->second == kOffsetOk) return r->first;
        if (r && r->second == kOffsetNotLeader && pendingSeen != nullptr) ++*pendingSeen;
        std::this_thread::sleep_for(50ms);   // the client's retry pacing (poll interval)
    }
    return -99;
}
/// Join + commit `offset` on the coordinator, retrying the settling window.
bool commitOffset(GroupCluster& g, ShardTestClient& c, const std::string& group, int64_t offset) {
    for (int k = 0; k < 600; ++k) {
        const int coord = g.coordinator();
        if (coord < 0) continue;
        const std::string gw = gatewayChannel(g.ports, coord);
        const int64_t sid = c.connect(gw, kClientStream);
        if (sid <= 0) { std::this_thread::sleep_for(25ms); continue; }
        const auto j = c.joinGroup(gw, kClientStream, sid, group);
        if (!j || j->errorCode != kOffsetOk) { std::this_thread::sleep_for(50ms); continue; }
        const auto code = c.offsetCommit(gw, kClientStream, sid, group, j->memberId, j->generation, 0, 0, offset);
        if (code && *code == kOffsetOk) return true;
        std::this_thread::sleep_for(50ms);   // pending lineage / rebalance: retried (poll interval)
    }
    return false;
}
}  // namespace

TEST(TransientLineageClusterRegression, FullClusterRestartAnswersAGroupsOffsetAsNoneRegression) {
    GroupCluster g;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int leader = g.partitionLeader(c);
    ASSERT_GE(leader, 0);
    const int64_t sidL = c.connect(gatewayChannel(g.ports, leader), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, leader), kClientStream, sidL, 0, 0, 1, makeBatch(4)));
    ASSERT_TRUE(commitOffset(g, c, "lg-full", 20)) << "the group never committed its offset";
    ASSERT_EQ(settledFetch(g, c, "lg-full", nullptr), 20) << "precondition: the committed offset is served";

    // Full-cluster restart: every process (and ring) goes; metadata survives.
    for (int i = 0; i < 3; ++i) g.stop(i);
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    int pending = 0;
    const int64_t after = settledFetch(g, c, "lg-full", &pending);
    EXPECT_EQ(after, -1) << "SILENT LOSS: the offset of the ring's previous life was answered after a full-cluster "
                            "restart (a member would skip the first " << after << " refilled records)";
    std::printf("[lineage full restart] settled after %d retriable NotLeader answer(s)\n", pending);

    // The member's reset policy (the log start) sees every post-restart record.
    // Isolation fix (2026-10-15, found once the suite ran in parallel): right after a full restart the
    // advertised leader may still be settling (retriable answers); produceUntilOk's 100 quick retries were a
    // ~2 s budget that a loaded host (ctest -j) outlasts.  Re-resolve the leader and retry until a deadline.
    int leader2 = -1;
    int64_t sid2 = 0;
    std::optional<rune::protocol::client::ProduceResponse> first;
    const auto produceDeadline = std::chrono::steady_clock::now() + 30s;
    while (!first && std::chrono::steady_clock::now() < produceDeadline) {
        leader2 = g.partitionLeader(c);
        ASSERT_GE(leader2, 0);
        sid2 = c.connect(gatewayChannel(g.ports, leader2), kClientStream);
        if (sid2 <= 0) continue;
        first = c.produceUntilOk(workerIngressChannel(g.ports, leader2), kClientStream, sid2, 0, 0, 1, makeBatch(2),
                                 /*maxAttempts=*/25);
    }
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->baseOffset, 0) << "precondition: the ring restarted empty";
    ASSERT_TRUE(c.subscribeUntilBound(gatewayChannel(g.ports, leader2), kClientStream, sid2, 0, 0, /*from (reset)=*/0,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe was never bound (re-check I5: a gated Subscribe is answered NotLeader, re-sent)";
    ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 0) >= 1; })) << "the post-restart record was not delivered";
}

TEST(TransientLineageClusterRegression, RollingRestartKeepsAGroupsOffsetValidRegression) {
    GroupCluster g;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int leader = g.partitionLeader(c);
    ASSERT_GE(leader, 0);
    const int64_t sidL = c.connect(gatewayChannel(g.ports, leader), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, leader), kClientStream, sidL, 0, 0, 1, makeBatch(4)));
    ASSERT_TRUE(commitOffset(g, c, "lg-rolling", 20));
    ASSERT_EQ(settledFetch(g, c, "lg-rolling", nullptr), 20);

    // One node at a time; each catches up (holds the 20 records again) before the next.
    for (int i = 0; i < 3; ++i) {
        g.stop(i);
        ASSERT_GE(g.coordinator(), 0) << "no coordinator while node " << i << " is down";
        ASSERT_GE(g.partitionLeader(c, i), 0) << "no partition leader while node " << i << " is down";
        g.start(i);
        bool caughtUp = false;
        for (int k = 0; k < 600 && !caughtUp; ++k) {
            caughtUp = hasReplica(*g.nodes[i], 0, 0) && leoOnOwner(*g.nodes[i], 0, 0) >= 20;
            if (!caughtUp) std::this_thread::sleep_for(50ms);   // replication catch-up (poll interval)
        }
        ASSERT_TRUE(caughtUp) << "restarted node " << i << " never caught up";
        std::this_thread::sleep_for(500ms);   // ISR re-admission settles (not a correctness gate: asserted below)
    }
    EXPECT_EQ(settledFetch(g, c, "lg-rolling", nullptr), 20)
        << "a rolling restart (the data survived every step) answered the committed offset as 'none' (false -1)";
}

// C1 (single node — the same lineage mechanism; the controller is the lineage
// authority, every ring living exactly as long as the process): the offset is
// served while the process lives and answered "none" after a restart.  RED
// (no single-node authority): the lineage never confirms — every answer stays
// the retriable NotLeader (no settled answer).
// Re-check I5 (DR-bugs fix round 2): the "restarted leader: subscribe answered OK, then 0/6 records" stall was the
// TEST ignoring the gateway's answer.  Diagnosed (2 of 45 runs under 3-process load): the Subscribe on the restarted
// RF=1 leader was answered ErrorResponse NotLeader — the FU1 gate: that broker had not yet learned the partition's
// NEW lineage (its metadata replica applies the committed record a moment after the coordinator's does) — nothing
// was registered, and the fire-and-forget subscribe waited 30 s for records that can never come.  The product
// contract (an explicit, retriable answer; bound once the lineage is known) is pinned here deterministically: the
// lineage is held unknown, the answer must be NotLeader (never silence), and the re-sent Subscribe — released on
// the first NotLeader — binds and delivers every record within the original cap.
// RED: (a) the gateway drops the gated Subscribe silently (no answer); (b) subscribeUntilBound treats NotLeader as
// final (the old fire-and-forget behaviour: never bound, 0 records).
TEST(TransientLineageClusterRegression, SubscribeWhileTheLineageIsUnknownIsAnsweredRetriableAndBindsOnceKnownRegression) {
    TmpDir dir{"lgsub"}, cdir{"lgsubc"};
    const auto ports = Ports::discover(1);
    const auto cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/true);
    ShardTestClient c(cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const std::string gw = gatewayChannel(ports, 0);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ASSERT_EQ(defineTt({&node}), 0);
    const int64_t sid = c.connect(gw, kClientStream);
    for (int b = 0; b < 3; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1, makeBatch(4)));
    node.holdTransientLineageForTest(true);
    const uint64_t awaiting0 =
        node.onControlThreadForTest([&] { return node.gatewayForTest()->subscribesAwaitingLineageForTest(); });
    const int64_t corr = c.subscribeIsolated(gw, kClientStream, sid, 0, 0, /*from=*/0, /*isolation=*/0);
    const auto code = c.awaitSubscribeCode(corr, std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(code.has_value()) << "SILENT: a Subscribe the gateway cannot bind yet was not answered";
    EXPECT_EQ(*code, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader))
        << "a lineage-unknown Subscribe must be answered the retriable NotLeader";
    EXPECT_EQ(node.onControlThreadForTest([&] { return node.gatewayForTest()->subscribesAwaitingLineageForTest(); }),
              awaiting0 + 1);
    const std::size_t before = c.records().size();
    bool released = false;
    ASSERT_TRUE(c.subscribeUntilBound(gw, kClientStream, sid, 0, 0, /*from=*/0, std::chrono::steady_clock::now() + 5s,
                                      [&] { if (!released) { node.holdTransientLineageForTest(false); released = true; } }))
        << "the re-sent Subscribe was never bound once the lineage was known";
    EXPECT_TRUE(released) << "the retry path (NotLeader -> re-send) did not run";
    std::set<int64_t> seen;
    ASSERT_TRUE(c.pollUntil([&] {
        for (std::size_t i = before; i < c.records().size(); ++i)
            if (!c.records()[i].bytes.empty()) seen.insert(c.records()[i].baseOffset);
        return seen.size() >= 3;
    })) << "the bound Subscribe delivered " << seen.size() << "/3 batches";
    node.stop();
}

TEST(TransientLineageClusterRegression, SingleNodeRestartAnswersAGroupsOffsetAsNoneRegression) {
    TmpDir dir{"lg1n"}, cdir{"lg1nc"};
    const auto ports = Ports::discover(1);
    const auto cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/true);
    ShardTestClient c(cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const std::string gw = gatewayChannel(ports, 0);
    auto settled = [&](int64_t& out) {
        for (int k = 0; k < 400; ++k) {
            const int64_t sid = c.connect(gw, kClientStream);
            const auto r = sid > 0 ? c.offsetFetch(gw, kClientStream, sid, "lg-one", 0, 0) : std::nullopt;
            if (r && r->second == kOffsetOk) { out = r->first; return true; }
            std::this_thread::sleep_for(50ms);   // retry pacing (poll interval)
        }
        return false;
    };
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        ASSERT_EQ(defineTt({&node}), 0);
        const int64_t sid = c.connect(gw, kClientStream);
        for (int b = 0; b < 3; ++b)
            ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1, makeBatch(4)));
        bool committed = false;
        for (int k = 0; k < 400 && !committed; ++k) {
            const int64_t s = c.connect(gw, kClientStream);
            const auto j = c.joinGroup(gw, kClientStream, s, "lg-one");
            if (j && j->errorCode == kOffsetOk) {
                const auto code = c.offsetCommit(gw, kClientStream, s, "lg-one", j->memberId, j->generation, 0, 0, 12);
                committed = code && *code == kOffsetOk;
            }
            if (!committed) std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(committed) << "the single-node lineage never confirmed (commits refused NotLeader forever)";
        int64_t off = -99;
        ASSERT_TRUE(settled(off));
        EXPECT_EQ(off, 12);
        node.stop();
    }
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    int64_t off = -99;
    ASSERT_TRUE(settled(off)) << "no settled OffsetFetch answer after the restart";
    EXPECT_EQ(off, -1) << "SILENT LOSS: a single-node restart answered the previous process's offset";
    node.stop();
}

// E2 final fix wave, residual round 3 — N1 (Critical): after a full-cluster
// restart the committed assignment still names the dead pre-restart leadership
// (L, E) until its bump / failover commits, and the committed lineage is
// "confirmed" for exactly (L, E).  The coordinator must not trust it: the
// lineage is trusted only while the controller's currently known process
// identity for L matches the one committed with it.  Staggered restart: the
// two other nodes come back first (consensus, the coordinator, the offsets),
// the old leader L LAST; the group's OffsetFetch arrives before P's leadership
// moved.  Every answer must be NotLeader (pending) until a new leadership
// confirms a new lineage, then -1 — never the old offset.  RED (identity not
// checked): 20 answered in the window.
TEST(TransientLineageClusterRegression, StaggeredFullRestartWithTheOldLeaderLastNeverAnswersTheOldOffsetRegression) {
    GroupCluster g;
    g.crashStops = true;   // ISRs stay full: nothing is handed off on the stops
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int leader = g.partitionLeader(c);
    ASSERT_GE(leader, 0);
    const int64_t sidL = c.connect(gatewayChannel(g.ports, leader), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, leader), kClientStream, sidL, 0, 0, 1, makeBatch(4)));
    ASSERT_TRUE(commitOffset(g, c, "lg-stagger", 20));
    ASSERT_EQ(settledFetch(g, c, "lg-stagger", nullptr), 20);

    // Stop the others first and L LAST (a controlled shutdown with no live ISR
    // peer cannot hand P off): the committed assignment keeps naming (L, E).
    for (int i = 0; i < 3; ++i) if (i != leader) g.stop(i);
    g.stop(leader);
    // The restarted controller decides no USER partition for a while (test
    // hold): P's committed leadership stays the dead (L, E), exactly as when
    // its failover / bump is decided on a later tick.
    // Isolation fix (2026-10-15, found once the suite ran in parallel): the hold starts when each restarted
    // controller boots, so it lasts until at least holdStart + holdMs.  The sampling below must stay inside
    // that window — before, a loaded host (ctest -j) took longer than the 6 s hold to collect 150 answers and
    // the last few, served after the hold legitimately decided P, read as "an answer other than NotLeader".
    // The window is now explicit (sampling ends 500 ms before the earliest possible hold end) and 10 s wide
    // so >= 100 answers fit even under load.
    // TSan (#622): restarting the two nodes alone took ~9.6 s, so a 10 s hold left no window at all (0 answers).
    // 30 s covers the restart plus >= 100 answers at sanitizer speed; the final settledFetch (>= 30 s of retries)
    // absorbs whatever hold is left once the old leader returns.
    g.holdMs = 30'000;
    const auto holdStart = std::chrono::steady_clock::now();
    const auto sampleUntil = holdStart + std::chrono::milliseconds{g.holdMs} - 500ms;
    for (int i = 0; i < 3; ++i) if (i != leader) g.start(i);   // the old partition leader stays down
    // The window: P's committed leadership is still the dead (L, E) — no clean
    // leader exists while L is down.  Ask repeatedly (as a rejoining member does).
    std::vector<std::pair<int64_t, int16_t>> answers;   // the coordinator's served answers (OK / NotLeader)
    for (int k = 0; k < 4000 && answers.size() < 150 && std::chrono::steady_clock::now() < sampleUntil; ++k) {
        const int coord = g.coordinator();
        if (coord < 0) continue;
        const int64_t sid = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        if (sid <= 0) { std::this_thread::sleep_for(25ms); continue; }
        const auto r = c.offsetFetch(gatewayChannel(g.ports, coord), kClientStream, sid, "lg-stagger", 0, 0);
        if (std::chrono::steady_clock::now() >= sampleUntil) break;   // answered past the guaranteed window
        // NotCoordinator: the coordinator is not ready yet (it does not answer offsets).
        if (r && (r->second == kOffsetOk || r->second == kOffsetNotLeader)) answers.push_back(*r);
        std::this_thread::sleep_for(25ms);   // the client's retry pacing (poll interval)
    }
    ASSERT_GE(answers.size(), 100u) << "the coordinator answered too rarely to cover the window";
    int pendingN = 0;
    for (const auto& [off, code] : answers) {
        ASSERT_FALSE(code == kOffsetOk && off == 20)
            << "SILENT LOSS: the old offset was answered from the dead leadership's lineage after a full restart";
        pendingN += code == kOffsetNotLeader ? 1 : 0;
    }
    EXPECT_EQ(pendingN, static_cast<int>(answers.size()))
        << "an answer other than the retriable pending NotLeader while the old leader's process is gone";
    // The old leader comes back LAST (no hold for it — its process registers a
    // NEW identity): its restart fence takes a new epoch with an unborn ring ->
    // a NEW lineage -> -1 (never the old offset).
    g.holdMs = 0;
    g.start(leader);
    EXPECT_EQ(settledFetch(g, c, "lg-stagger", nullptr), -1)
        << "after the old leader returned, the offset of the ring's previous life was not answered as none";
    std::printf("[lineage staggered restart] %zu answer(s) while the old leader was down, %d pending\n",
                answers.size(), pendingN);
}

// E2 final fix wave, residual round 5 — R1 (silent loss, closed END TO END).
// An RF=1 transient partition P: its only replica's node L crashes.  Until the
// controller's failure detector finds L dead, the committed assignment still
// names (L, E) and L's registered process identity still vouches for the
// lineage — a member that re-joins in that window is answered the OLD offset
// (20).  That answer cannot be taken back, so the rest of the chain must hold:
//   - L restarts with an empty ring and a NEW lineage is minted for P;
//   - the coordinator forces the group to rebalance (generation bump);
//   - the member's commit at its pre-mint generation (a position based on the
//     old data: 25) is FENCED (IllegalGeneration) — never stamped with the new
//     lineage;
//   - after re-joining it is answered "none" (-1: the log start) — never 20 or
//     25 — and consumes every post-restart record exactly once.
// RED (no forced rebalance / commit fence): the stale commit was accepted and
// the next fetch answered 25 (the member would skip the refilled records 0..24).
TEST(TransientLineageClusterRegression, Rf1CrashedLeaderMemberRejoiningBeforeDetectionNeverConsumesTheStaleOffsetRegression) {
    GroupCluster g;
    g.crashStops = true;
    g.rf         = 1;
    g.partitions = 3;   // RF=1 x 3: some partition lives on a node that is not the coordinator
    g.electionTimeoutMs = 1'500;   // the failure detector finds a crashed node dead after ~4.5 s: the window
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.keepSessionsAlive(true);   // a real client keeps its sessions alive (1 s keepalive)
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int coord = g.coordinator();
    ASSERT_GE(coord, 0);
    // P: a partition whose only replica is NOT on the coordinator.
    int P = -1, L = -1;
    {
        const int64_t s = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        for (int k = 0; k < 400 && P < 0; ++k) {
            for (const auto& pi : c.metadata(gatewayChannel(g.ports, coord), kClientStream, s))
                if (pi.topicId == 0 && pi.leaderId >= 0 && pi.leaderId != coord) { P = pi.partition; L = pi.leaderId; break; }
            if (P < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
    }
    ASSERT_GE(P, 0) << "no RF=1 partition away from the coordinator";
    const std::string gwC = gatewayChannel(g.ports, coord);

    // 20 records on P, a member commits 20.
    const int64_t sidL = c.connect(gatewayChannel(g.ports, L), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sidL, 0, P, 1, makeBatch(4)));
    const int64_t sidC = c.connect(gwC, kClientStream);
    std::string member;
    bool committed = false;
    for (int k = 0; k < 400 && !committed; ++k) {
        const auto j = c.joinGroup(gwC, kClientStream, sidC, "lg-rf1", member);
        if (j && j->errorCode == kOffsetOk) {
            member = j->memberId;
            const auto code = c.offsetCommit(gwC, kClientStream, sidC, "lg-rf1", member, j->generation, 0, P, 20);
            committed = code && *code == kOffsetOk;
        }
        if (!committed) std::this_thread::sleep_for(50ms);   // lineage settling (poll interval)
    }
    ASSERT_TRUE(committed) << "the group never committed";

    // L crashes; the member re-joins and fetches BEFORE the failure detector ran.
    c.whilePumping([&] { g.stop(L); });
    const auto rj = c.joinGroup(gwC, kClientStream, sidC, "lg-rf1", member);
    ASSERT_TRUE(rj.has_value() && rj->errorCode == kOffsetOk) << "the re-join was not answered";
    const int32_t preMintGen = rj->generation;
    const auto window = c.offsetFetch(gwC, kClientStream, sidC, "lg-rf1", 0, P);
    ASSERT_TRUE(window.has_value());
    const bool windowHit = window->second == kOffsetOk && window->first == 20;
    std::printf("[R1] pre-detection fetch answered %lld (code %d)%s\n", static_cast<long long>(window->first),
                window->second, windowHit ? " — the window was hit" : "");
    ASSERT_TRUE(windowHit) << "precondition: the re-join landed before the failure detector found L dead";

    // L restarts: empty ring, a NEW lineage for P.  (Until the mint commits,
    // the old offset may still be answered — the window this test is about;
    // what must hold is everything AFTER the mint.)
    // the client keeps pumping (keepalives) while the harness restarts L
    c.whilePumping([&] { g.start(L); });
    bool minted = false;
    for (int k = 0; k < 600 && !minted; ++k) {
        const auto r = c.offsetFetch(gwC, kClientStream, sidC, "lg-rf1", 0, P);
        minted = r && r->second == kOffsetOk && r->first == -1;
        if (!minted) std::this_thread::sleep_for(50ms);   // lineage settling (poll interval)
    }
    ASSERT_TRUE(minted) << "P's new lineage never superseded the old offset";

    // The member, still at its pre-mint generation, commits a position based on
    // the old data: fenced, explicit.
    const auto stale = c.offsetCommit(gwC, kClientStream, sidC, "lg-rf1", member, preMintGen, 0, P, 25);
    ASSERT_TRUE(stale.has_value()) << "the stale commit was not answered";
    EXPECT_NE(*stale, kOffsetOk)
        << "SILENT LOSS: a commit based on the superseded lineage was accepted (stamped with the new lineage)";
    const auto after = c.offsetFetch(gwC, kClientStream, sidC, "lg-rf1", 0, P);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->first, -1) << "SILENT LOSS: the member would resume at " << after->first
                                << " and skip the refilled records";

    // Re-join (a new generation), re-fetch: none -> the log start; every
    // post-restart record exactly once.
    std::optional<ShardTestClient::JoinAnswer> j2;
    for (int k = 0; k < 400; ++k) {
        j2 = c.joinGroup(gwC, kClientStream, sidC, "lg-rf1", member);
        if (j2 && j2->errorCode == kOffsetOk) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(j2 && j2->errorCode == kOffsetOk);
    EXPECT_GT(j2->generation, preMintGen);
    const auto start = c.offsetFetch(gwC, kClientStream, sidC, "lg-rf1", 0, P);
    ASSERT_TRUE(start.has_value());
    ASSERT_EQ(start->first, -1) << "the re-joined member was not answered 'none'";
    const int64_t sidL2 = c.connect(gatewayChannel(g.ports, L), kClientStream);
    ASSERT_GT(sidL2, 0);
    for (int64_t want = 0; want < 6; ++want) {
        const auto ack = c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sidL2, 0, P, 1, makeBatch(1));
        ASSERT_TRUE(ack.has_value());
        ASSERT_EQ(ack->baseOffset, want) << "precondition: the ring restarted empty";
    }
    const std::size_t before = c.records().size();
    ASSERT_TRUE(c.subscribeUntilBound(gatewayChannel(g.ports, L), kClientStream, sidL2, 0, P, /*log start=*/0,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe on the restarted leader was never bound";
    std::map<int64_t, int> seen;
    ASSERT_TRUE(c.pollUntil([&] {
        seen.clear();
        for (std::size_t i = before; i < c.records().size(); ++i)
            if (!c.records()[i].bytes.empty()) ++seen[c.records()[i].baseOffset];
        return seen.size() >= 6;
    })) << "the post-restart records were not all delivered (" << seen.size() << "/6)";
    for (int64_t o = 0; o < 6; ++o) EXPECT_EQ(seen[o], 1) << "post-restart record " << o << " not delivered exactly once";
    std::printf("[R1] window hit: %s; stale commit answered %d\n", windowHit ? "yes" : "no", *stale);
}

// E2 final fix wave, residual round 5 — R2: the partition leader's process
// identity ROTATES (a controller answered it kNodeSuperseded — a late frame of
// an unseen older process).  The controller now knows the NEW identity while
// the committed lineage carries the old one: the coordinator would answer
// NotLeader for the rest of the process's life.  The node tells its user
// replicas (owner rings) and the leader re-reports under the CURRENT identity;
// the controller re-confirms (same leadership, same data: the lineage is KEPT)
// and the group's committed offset is answered again — within a bound, never
// a re-mint (-1).  RED (replicas never re-report): NotLeader forever.
TEST(TransientLineageClusterRegression, IdentityRotationReconfirmsTheLineageWithinABoundRegression) {
    GroupCluster g;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int leader = g.partitionLeader(c);
    ASSERT_GE(leader, 0);
    const int64_t sidL = c.connect(gatewayChannel(g.ports, leader), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, leader), kClientStream, sidL, 0, 0, 1, makeBatch(4)));
    ASSERT_TRUE(commitOffset(g, c, "lg-rotate", 20));
    ASSERT_EQ(settledFetch(g, c, "lg-rotate", nullptr), 20);

    const int64_t before = g.nodes[leader]->supersedeProcessIdentityForTest();
    ASSERT_NE(before, 0);
    bool rotated = false;
    for (int k = 0; k < 200 && !rotated; ++k) {
        rotated = g.nodes[leader]->processIdentityForTest() != before;
        if (!rotated) std::this_thread::sleep_for(25ms);   // the control thread rotates (poll interval)
    }
    ASSERT_TRUE(rotated) << "precondition: the leader's identity rotated";

    // Bounded: well within the client's OffsetFetch retry budget.
    const auto t0 = std::chrono::steady_clock::now();
    int64_t off = -99;
    int pending = 0;
    for (int k = 0; k < 200; ++k) {
        const int coord = g.coordinator();
        if (coord < 0) continue;
        const int64_t sid = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        const auto r = sid > 0 ? c.offsetFetch(gatewayChannel(g.ports, coord), kClientStream, sid, "lg-rotate", 0, 0)
                               : std::nullopt;
        if (r && r->second == kOffsetOk) { off = r->first; break; }
        if (r && r->second == kOffsetNotLeader) ++pending;
        std::this_thread::sleep_for(50ms);   // the client's retry pacing (poll interval)
    }
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_EQ(off, 20) << "after the identity rotation the lineage was never re-confirmed (" << pending
                       << " NotLeader answers) — or it was re-minted (-1: a false 'none')";
    EXPECT_LT(ms, 10'000) << "re-confirmation took " << ms << " ms";
    std::printf("[R2] re-confirmed after %lld ms, %d NotLeader answer(s)\n", static_cast<long long>(ms), pending);
}

// E2 final fix wave, residual round 6 (fix-on-sight): in SHARD mode the
// controller's answer to a user replica's lineage report reaches it through
// the worker's AlterIsrResponse op, which carries no leaderId — the replica's
// "is this answer mine" check never matched, so its report stayed pending
// (re-sent forever) and a later identity rotation re-sent an UNBORN report
// (a false re-mint).  Once the coordinator serves the committed offset (the
// lineage is confirmed), the leader replica's report is no longer pending.
// RED (leaderId left -1): still pending after the confirmation.
TEST(TransientLineageClusterRegression, ShardModeLeaderSeesItsLineageConfirmationRegression) {
    GroupCluster g;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int leader = g.partitionLeader(c);
    ASSERT_GE(leader, 0);
    const int64_t sidL = c.connect(gatewayChannel(g.ports, leader), kClientStream);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, leader), kClientStream, sidL, 0, 0, 1, makeBatch(4)));
    ASSERT_TRUE(commitOffset(g, c, "lg-ack", 4));   // committable: the lineage is confirmed
    bool pending = true;
    for (int k = 0; k < 100 && pending; ++k) {
        pending = g.nodes[leader]->onOwnerThreadForTest(
            0, 0, [](rune::replication::PartitionReplica& r) { return r.lineageReportPendingForTest(); });
        if (pending) std::this_thread::sleep_for(50ms);   // the answer's round trip (poll interval)
    }
    EXPECT_FALSE(pending) << "the leader never matched the controller's lineage confirmation (shard mode)";
}

// E2 final fix wave, residual round 6 — F1 (silent loss): a group consumes an
// RF=1 transient partition P but has NO committed offset on it (the first
// auto-commit interval has not passed).  Its position (20) lives only in the
// member.  P's node crashes and restarts empty: a new lineage.  Nothing stored
// is superseded, yet the member's in-memory position is — the group must be
// rebalanced all the same, and the member's commit at the pre-mint generation
// (20: a position in the old data) refused; after re-joining it starts at the
// log start and gets every post-restart record exactly once.  RED (only
// committed offsets swept / fenced): the stale commit was accepted (stamped
// with the new lineage) — the next fetch answered 20.
TEST(TransientLineageClusterRegression, Rf1GroupWithoutACommitIsRebalancedOnANewLineageRegression) {
    GroupCluster g;
    g.crashStops = true;
    g.rf         = 1;
    g.partitions = 3;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.keepSessionsAlive(true);   // a real client keeps its sessions alive (1 s keepalive)
    c.setClientVersion(rune::protocol::client::kClientVersion);
    const int coord = g.coordinator();
    ASSERT_GE(coord, 0);
    int P = -1, L = -1;
    {
        const int64_t s = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        for (int k = 0; k < 400 && P < 0; ++k) {
            for (const auto& pi : c.metadata(gatewayChannel(g.ports, coord), kClientStream, s))
                if (pi.topicId == 0 && pi.leaderId >= 0 && pi.leaderId != coord) { P = pi.partition; L = pi.leaderId; break; }
            if (P < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
    }
    ASSERT_GE(P, 0);
    const std::string gwC = gatewayChannel(g.ports, coord);
    const int64_t sidC = c.connect(gwC, kClientStream);

    // The member joins (assigned every partition of topic 0, P among them) and
    // consumes P from the log start — 20 records — committing nothing.
    std::optional<ShardTestClient::JoinAnswer> j;
    for (int k = 0; k < 400; ++k) {
        j = c.joinGroup(gwC, kClientStream, sidC, "lg-nocommit", {}, {0});
        if (j && j->errorCode == kOffsetOk) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(j && j->errorCode == kOffsetOk);
    const std::string member = j->memberId;
    int32_t gen = j->generation;
    const int64_t sidL = c.connect(gatewayChannel(g.ports, L), kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sidL, 0, P, 1, makeBatch(4)));
    ASSERT_TRUE(c.subscribeUntilBound(gatewayChannel(g.ports, L), kClientStream, sidL, 0, P, 0,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe was never bound (re-check I5: a gated Subscribe is answered NotLeader, re-sent)";
    ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 16) >= 1; })) << "precondition: the member consumed 0..19";
    // The group is Stable at `gen` (assigned): its generation consumes P.  As a
    // member does: RebalanceInProgress -> re-join (same member id).
    bool stable = false;
    for (int k = 0; k < 600 && !stable; ++k) {
        const auto hb = c.heartbeat(gwC, kClientStream, sidC, "lg-nocommit", member, gen);
        stable = hb && *hb == kOffsetOk;
        if (!stable && hb && *hb == static_cast<int16_t>(rune::protocol::client::ErrorCode::RebalanceInProgress)) {
            const auto rj = c.joinGroup(gwC, kClientStream, sidC, "lg-nocommit", member, {0});
            if (rj && rj->errorCode == kOffsetOk) gen = rj->generation;
        }
        if (!stable) std::this_thread::sleep_for(50ms);   // the coordinator's rebalance (poll interval)
    }
    ASSERT_TRUE(stable) << "precondition: the group reached Stable";
    // A separate PROBE group commits on P: its fetch turning -1 shows the mint.
    std::string probeMember;
    int32_t probeGen = -1;
    bool probed = false;
    for (int k = 0; k < 400 && !probed; ++k) {
        const auto pj = c.joinGroup(gwC, kClientStream, sidC, "lg-probe", probeMember);
        if (pj && pj->errorCode == kOffsetOk) {
            probeMember = pj->memberId;
            probeGen    = pj->generation;
            const auto code = c.offsetCommit(gwC, kClientStream, sidC, "lg-probe", probeMember, probeGen, 0, P, 20);
            probed = code && *code == kOffsetOk;
        }
        if (!probed) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(probed);

    // L crashes and restarts: a new lineage.
    // the client keeps pumping (keepalives) while the harness restarts L: sidC must outlive the ~9 s restart
    c.whilePumping([&] { g.stop(L); });
    c.whilePumping([&] { g.start(L); });
    bool minted = false;
    for (int k = 0; k < 600 && !minted; ++k) {
        const auto r = c.offsetFetch(gwC, kClientStream, sidC, "lg-probe", 0, P);
        minted = r && r->second == kOffsetOk && r->first == -1;
        if (!minted) std::this_thread::sleep_for(50ms);   // lineage settling (poll interval)
    }
    ASSERT_TRUE(minted) << "P's new lineage never confirmed";
    // The member, at its pre-mint generation, commits its in-memory position.
    const std::optional<int16_t> stale = c.offsetCommit(gwC, kClientStream, sidC, "lg-nocommit", member, gen, 0, P, 20);
    ASSERT_TRUE(stale.has_value());
    EXPECT_NE(*stale, kOffsetOk)
        << "SILENT LOSS: a position from the superseded data was committed (no stored offset to compare with)";
    const auto after = c.offsetFetch(gwC, kClientStream, sidC, "lg-nocommit", 0, P);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->first, -1) << "SILENT LOSS: the member would resume at " << after->first;

    // The member's pre-mint (member, generation) no longer holds: it must re-join.  Normally the lineage sweep
    // bumps the generation; if the coordinator role flapped meanwhile, the group was unloaded (#565 / #570) and
    // the member is unknown.  Either way a heartbeat at the old pair is refused — the generation number itself
    // is not compared, since a reloaded group starts its count afresh.
    const auto oldHb = c.heartbeat(gwC, kClientStream, sidC, "lg-nocommit", member, gen);
    ASSERT_TRUE(oldHb.has_value());
    EXPECT_NE(*oldHb, kOffsetOk) << "the group was not rebalanced for the new lineage";

    // Re-join: every post-restart record exactly once from the log start.
    std::optional<ShardTestClient::JoinAnswer> j2;
    for (int k = 0; k < 400; ++k) {
        j2 = c.joinGroup(gwC, kClientStream, sidC, "lg-nocommit", member, {0});
        if (j2 && j2->errorCode == kOffsetOk) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(j2 && j2->errorCode == kOffsetOk);
    const int64_t sidL2 = c.connect(gatewayChannel(g.ports, L), kClientStream);
    for (int64_t want = 0; want < 6; ++want) {
        const auto ack = c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sidL2, 0, P, 1, makeBatch(1));
        ASSERT_TRUE(ack.has_value());
        ASSERT_EQ(ack->baseOffset, want) << "precondition: the ring restarted empty";
    }
    const std::size_t before = c.records().size();
    ASSERT_TRUE(c.subscribeUntilBound(gatewayChannel(g.ports, L), kClientStream, sidL2, 0, P, 0,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe was never bound (re-check I5: a gated Subscribe is answered NotLeader, re-sent)";
    std::map<int64_t, int> seen;
    ASSERT_TRUE(c.pollUntil([&] {
        seen.clear();
        for (std::size_t i = before; i < c.records().size(); ++i)
            if (!c.records()[i].bytes.empty()) ++seen[c.records()[i].baseOffset];
        return seen.size() >= 6;
    })) << [&] { std::string d = "DIAG recs:"; for (std::size_t i = before; i < c.records().size(); ++i) d += " (" + std::to_string(c.records()[i].subscriptionId) + "," + std::to_string(c.records()[i].baseOffset) + "," + std::to_string(c.records()[i].bytes.size()) + ")"; d += " errs:"; for (const auto& e : c.errors()) d += " (" + std::to_string(e.correlationId) + "," + std::to_string(e.code) + "," + e.message + ")"; return d; }();
    for (int64_t o = 0; o < 6; ++o) EXPECT_EQ(seen[o], 1) << "post-restart record " << o;
    std::printf("[F1] stale commit answered %d\n", *stale);
}

// E2 final fix wave, residual round 6 — F1 (plain subscriptions, broker half):
// an RF=1 transient partition P; a v7 plain subscriber bound on P's node L is
// sent the partition's data lineage (the lineage note: an 8-byte batch) and
// consumes 0..19.  L crashes and restarts EMPTY (a new lineage).  The client's
// resume from its position, claiming the noted lineage, must be answered
// PartitionDataReset — never served from a clamped position (its floor would
// drop the refilled records); a fresh subscription from the log start then
// gets every post-restart record exactly once.  RED (no claim check): the
// resume was served (SubscribeResponse OK).
TEST(TransientLineageClusterRegression, Rf1PlainSubscriptionResumeAcrossANewLineageIsAnsweredDataResetRegression) {
    GroupCluster g;
    g.crashStops = true;
    g.rf         = 1;
    g.partitions = 3;
    for (int i = 0; i < 3; ++i) g.start(i);
    ASSERT_TRUE(g.nodes[0]->awaitLeader(15'000ms));
    ASSERT_EQ(g.defineTopic(), 0);
    ShardTestClient c(g.cdir.path);
    c.setClientVersion(rune::protocol::client::kClientVersion);
    ASSERT_GE(rune::protocol::client::kClientVersion, rune::protocol::client::kClientVersionLineageNote);
    const int coord = g.coordinator();
    ASSERT_GE(coord, 0);
    int P = -1, L = -1;
    {
        const int64_t s = c.connect(gatewayChannel(g.ports, coord), kClientStream);
        for (int k = 0; k < 400 && P < 0; ++k) {
            for (const auto& pi : c.metadata(gatewayChannel(g.ports, coord), kClientStream, s))
                if (pi.topicId == 0 && pi.leaderId >= 0 && pi.leaderId != coord) { P = pi.partition; L = pi.leaderId; break; }
            if (P < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
    }
    ASSERT_GE(P, 0);
    const std::string gwL = gatewayChannel(g.ports, L);
    const int64_t sidL = c.connect(gwL, kClientStream);
    for (int b = 0; b < 5; ++b)
        ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sidL, 0, P, 1, makeBatch(4)));
    ASSERT_TRUE(c.subscribeUntilBound(gwL, kClientStream, sidL, 0, P, 0,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe was never bound (re-check I5: a gated Subscribe is answered NotLeader, re-sent)";
    uint64_t token = 0;
    ASSERT_TRUE(c.pollUntil([&] {
        for (const auto& r : c.records())
            if (r.bytes.size() == 8) {
                token = 0;
                for (int i = 0; i < 8; ++i) token |= static_cast<uint64_t>(r.bytes[static_cast<std::size_t>(i)]) << (8 * i);
            }
        return token != 0 && pushedAt(c, 16) >= 1;
    })) << "no lineage note for the transient subscription (or the records never came)";

    // L crashes and restarts empty.
    g.stop(L);
    g.start(L);
    const std::string gwL2 = gatewayChannel(g.ports, L);
    std::optional<int16_t> answer;
    for (int k = 0; k < 600; ++k) {
        const int64_t sid = c.connect(gwL2, kClientStream);
        if (sid <= 0) { std::this_thread::sleep_for(50ms); continue; }
        const int64_t corr = c.subscribeClaiming(gwL2, kClientStream, sid, 0, P, /*position=*/20, token);
        answer = c.awaitSubscribeCode(corr, std::chrono::steady_clock::now() + 2s);
        if (answer && *answer != static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader) &&
            *answer != static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect))
            break;   // NotLeader: the lineage is not known yet (retriable)
        std::this_thread::sleep_for(50ms);   // the client's retry pacing (poll interval)
    }
    ASSERT_TRUE(answer.has_value());
    EXPECT_EQ(*answer, static_cast<int16_t>(rune::protocol::client::ErrorCode::PartitionDataReset))
        << "SILENT LOSS: a resume claiming the superseded lineage was answered " << *answer
        << " (served from a clamped position: the client's floor drops the refilled records)";

    // From the log start: every post-restart record exactly once.
    const int64_t sid2 = c.connect(gwL2, kClientStream);
    for (int64_t want = 0; want < 6; ++want) {
        const auto ack = c.produceUntilOk(workerIngressChannel(g.ports, L), kClientStream, sid2, 0, P, 1, makeBatch(1));
        ASSERT_TRUE(ack.has_value());
        ASSERT_EQ(ack->baseOffset, want);
    }
    const std::size_t before = c.records().size();
    ASSERT_TRUE(c.subscribeUntilBound(gwL2, kClientStream, sid2, 0, P, /*Earliest=*/-2,
                                      std::chrono::steady_clock::now() + 5s))
        << "the Subscribe was never bound (re-check I5: a gated Subscribe is answered NotLeader, re-sent)";
    std::map<int64_t, int> seen;
    ASSERT_TRUE(c.pollUntil([&] {
        seen.clear();
        for (std::size_t i = before; i < c.records().size(); ++i)
            if (c.records()[i].bytes.size() > 8) ++seen[c.records()[i].baseOffset];
        return seen.size() >= 6;
    }));
    for (int64_t o = 0; o < 6; ++o) EXPECT_EQ(seen[o], 1) << "post-restart record " << o;
}
