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
                      int32_t ringBytes = 0 /* 0 = the default 8 MiB */,
                      PersistenceMode persistence = PersistenceMode::Transient) {
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
    t.persistence = persistence;   // topic 0 "tt": transient unless a cell asks for its disk twin
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
int64_t firstOfNewSubscription(ShardTestClient& c, const std::set<int64_t>& known, int64_t* subOut = nullptr) {
    int64_t first = -1;
    (void)c.pollUntil([&] {
        for (const auto& r : c.records())
            if (!known.count(r.subscriptionId)) { first = r.baseOffset; if (subOut) *subOut = r.subscriptionId; return true; }
        return false;
    });
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
    const NodeConfig cfg = makeConfig(0, dir.path, ports, 1, shard, /*ringBytes=*/evict ? 16 * 1024 : 0);
    auto node = std::make_unique<BrokerNode>(cfg);
    node->start();
    ASSERT_TRUE(node->awaitLeader(10'000ms));
    auto client = std::make_unique<ShardTestClient>(c1.path);
    int64_t sid = client->connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    int32_t tid = 0;   // the static "tt" (makeConfig)
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
            for (int i = 0; i < 20 && first < 0; ++i) {
                ASSERT_TRUE(produce(1, 8).has_value());
                first = firstOfNewSubscription(*client, known);
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
        nodes[i] = std::make_unique<BrokerNode>(makeConfig(i, dirs[i]->path, ports, 3, shard, 0, pm));
    for (auto& n : nodes) n->start();
    for (auto& n : nodes) ASSERT_TRUE(n->awaitLeader(30'000ms));
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
    for (int64_t b : {0, 2, 4}) EXPECT_EQ(pushedOn(c, newSub, b), 0u) << "re-delivered below the position: " << b;
    for (int64_t b : {6, 8}) EXPECT_EQ(pushedOn(c, oldSub, b), 0u) << "the demoted node kept pushing " << b;
    c.whilePumping([&] { nodes[leader]->stop(); });
    for (auto& n : nodes) n->stop();
}

INSTANTIATE_TEST_SUITE_P(Cells, DemotedLeaderReadCommittedRegression,
                         testing::Values(DemoteParam{mx::Mode::Shard, mx::Persist::Transient},
                                         DemoteParam{mx::Mode::Legacy, mx::Persist::Transient},
                                         DemoteParam{mx::Mode::Shard, mx::Persist::Disk},
                                         DemoteParam{mx::Mode::Legacy, mx::Persist::Disk}),
                         demoteName);
