// ControlledShutdownRegressionTest.cpp — Kafka-parity controlled shutdown.
//
// Ground truth (dockerized DR-15 SIGTERM, 2026-10-05): on SIGTERM a node exited
// still advertised as the leader of its partitions.  Failover then took ~2 s
// (consensus re-election + partition failover) and meanwhile every broker
// redirected clients to the stopped node.
//
// Controlled shutdown (BrokerNode::controlledShutdown, called by main() on a
// graceful stop signal before stop()): the node asks the controller to move
// every partition it leads to another in-sync replica (new epoch), waits —
// event-driven — until its committed map shows them led elsewhere, hands the
// consensus (controller) leadership off when it holds it, and only then stops.
//
// In-process 3-node clusters over real UDP + real data dirs, DEFAULT runtime
// (shard mode).  Every wait is a bounded poll of a real condition.
//
//   1. NonControllerGracefulStopHandsEveryPartitionOffBeforeExitRegression
//   2. ControllerGracefulStopHandsConsensusAndPartitionsOffRegression
//   3. GracefullyStoppedNodeRestartsAndLeadsAgainRegression
//
// The sole-ISR rule (a partition with no other in-sync replica stays with the
// stopping leader — no data loss, it goes offline as before) is proven at the
// controller level (StaticAssignmentEmitTest.cpp, ControlledShutdownController-
// Regression): with RF = cluster size, an in-process sole-ISR partition needs
// every follower down, which in a 3-node cluster also loses the consensus
// quorum the hand-off itself needs.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
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
               (std::string("rune_cs_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;
constexpr int32_t kParts         = 3;
constexpr uint8_t kAcksAll       = 1;

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

std::string gatewayChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[static_cast<std::size_t>(node)]);
}

// Default runtime (shard mode, one worker, thread placement), 3 nodes.
NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = 2;
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
    cfg.clientEndpoint.channel  = gatewayChannel(p, nodeId);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "cs"; t.partitionCount = kParts; t.topicId = 0;
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

RoleEpoch roleEpoch(BrokerNode& n, int32_t topicId, int32_t part) {
    return n.onOwnerThreadForTest(topicId, part, [](PartitionReplica* r) {
        RoleEpoch re;
        if (r) { re.present = true; re.role = r->role(); re.epoch = r->epoch(); }
        return re;
    });
}

/// The node's COMMITTED assignment of (topicId, part): {leader, leaderEpoch}
/// ({-2, -1} when none) — what its gateway routes / redirects by.
std::pair<int32_t, int32_t> committedLeader(BrokerNode& n, int32_t topicId, int32_t part) {
    return n.onControlThreadForTest([&n, topicId, part] {
        const auto* a = n.metadataServiceForTest()->assignment(topicId, part);
        return a ? std::pair<int32_t, int32_t>{a->leaderNode, a->leaderEpoch}
                 : std::pair<int32_t, int32_t>{-2, -1};
    });
}

/// The node's own consensus view (control thread): the leader it follows / is.
int32_t consensusLeaderOnControl(BrokerNode& n) {
    return n.onControlThreadForTest([&n] { return static_cast<int32_t>(n.electionForTest()->leaderMemberId()); });
}

/// Every node: one leader per partition, every replica at the leader's epoch,
/// the leader's ISR is all three, and leadership is spread (one each).
bool spreadAndSettled(const std::array<std::unique_ptr<BrokerNode>, 3>& ns) {
    std::array<int, 3> led{};
    for (int32_t p = 0; p < kParts; ++p) {
        int leader = -1; Epoch e = -1;
        for (int i = 0; i < 3; ++i) {
            const auto re = roleEpoch(*ns[static_cast<std::size_t>(i)], 0, p);
            if (re.present && re.role == Role::Leader) { leader = i; e = re.epoch; }
        }
        if (leader < 0) return false;
        for (auto& n : ns) if (roleEpoch(*n, 0, p).epoch != e) return false;
        if (gatewayIsrSizeOnOwner(*ns[static_cast<std::size_t>(leader)], 0, p) != 3) return false;
        ++led[static_cast<std::size_t>(leader)];
    }
    return led[0] == 1 && led[1] == 1 && led[2] == 1;
}

/// Records in the committed prefix [0, HW) of the node's replica of `part`.
int committedRecords(BrokerNode& n, int32_t part) {
    const Offset hw = hwOnOwner(n, 0, part);
    const auto raw = readLogOnOwner(n, 0, 1 << 22, 0, part);
    int count = 0;
    std::size_t pos = 0;
    while (pos < raw.size()) {
        const std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
        rune::protocol::RecordBatchReader rd(raw.data() + pos, rem);
        if (!rd.ensureValid()) break;
        rd.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) { if (off < hw) ++count; });
        const int32_t tl = rd.totalLength();
        if (tl <= 0) break;
        pos += static_cast<std::size_t>(tl);
    }
    return count;
}

/// One Produce through `channel`: {acked OK, error code (0 = ok), Redirect target}.
struct Outcome { bool ok = false; int16_t code = -1; std::string redirect; };
Outcome produceOnce(ShardTestClient& c, const std::string& channel, int32_t stream, int64_t sid,
                    int32_t part, int records) {
    Outcome o;
    const int64_t corr = c.nextCorrelationId();
    if (!c.produce(channel, stream, sid, corr, 0, part, kAcksAll, makeBatch(records))) return o;
    std::optional<rune::protocol::client::ProduceResponse> ack;
    c.pollUntil([&] {
        for (const auto& e : c.errors())
            if (e.correlationId == corr) { o.code = e.code; o.redirect = e.message; return true; }
        ack = c.ackFor(corr);
        return ack.has_value();
    });
    if (ack) { o.code = ack->errorCode; o.ok = ack->errorCode == 0; }
    return o;
}

/// Produce `batches` acks=all batches of 2 records to every partition through
/// its advertised leader (metadata from `via`); retried while leadership settles.
bool produceAll(ShardTestClient& c, const Ports& ports, int via, std::map<int, int64_t>& sid,
                int batches, std::array<int, kParts>& produced) {
    auto session = [&](int node) {
        if (!sid.count(node)) sid[node] = c.connect(gatewayChannel(ports, node), kClientStream);
        return sid[node];
    };
    for (int32_t p = 0; p < kParts; ++p) {
        for (int b = 0; b < batches; ++b) {
            const bool ok = eventually([&] {
                int32_t leader = -1;
                for (const auto& pi : c.metadata(gatewayChannel(ports, via), kClientStream, session(via)))
                    if (pi.topicId == 0 && pi.partition == p) leader = pi.leaderId;
                if (leader < 0) return false;
                return produceOnce(c, gatewayChannel(ports, leader), kClientStream, session(leader), p, 2).ok;
            }, 15'000ms);
            if (!ok) return false;
            produced[static_cast<std::size_t>(p)] += 2;
        }
    }
    return true;
}

/// A client talking to every SURVIVOR (metadata, a produce per partition, a
/// subscribe per partition through each survivor's gateway) is never sent to
/// `victim`: no survivor advertises it as a partition leader and no Redirect
/// names its endpoint.  Acked produces are counted into `produced`.
void expectNeverSentToVictim(ShardTestClient& c, const Ports& ports, std::map<int, int64_t>& sid,
                             int victim, std::array<int, kParts>& produced, const char* when) {
    SCOPED_TRACE(when);
    const std::string victimEp = "127.0.0.1:" + std::to_string(ports.client[static_cast<std::size_t>(victim)]);
    const std::size_t errorsBefore = c.errors().size();
    for (int i = 0; i < 3; ++i) {
        if (i == victim) continue;
        if (!sid.count(i)) sid[i] = c.connect(gatewayChannel(ports, i), kClientStream);
        ASSERT_GT(sid[i], 0);
        const auto md = c.metadata(gatewayChannel(ports, i), kClientStream, sid[i]);
        EXPECT_FALSE(md.empty()) << "node " << i << " answered no metadata";
        for (const auto& pi : md)
            if (pi.topicId == 0)
                EXPECT_NE(pi.leaderId, victim) << "node " << i << " advertises the stopping node for partition "
                                               << pi.partition;
        for (int32_t p = 0; p < kParts; ++p) {
            const Outcome o = produceOnce(c, gatewayChannel(ports, i), kClientStream, sid[i], p, 2);
            if (o.ok) produced[static_cast<std::size_t>(p)] += 2;
            else
                EXPECT_EQ(o.code, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect))
                    << "node " << i << " partition " << p << ": produce failed with code " << o.code;
            ASSERT_TRUE(c.subscribe(gatewayChannel(ports, i), kClientStream, sid[i], 0, p, 0));
        }
    }
    (void)c.pollUntil([] { return false; });   // drain the subscribe answers (bounded)
    for (std::size_t k = errorsBefore; k < c.errors().size(); ++k)
        EXPECT_EQ(c.errors()[k].message.find(victimEp), std::string::npos)
            << "a survivor redirected the client to the stopping node (code " << c.errors()[k].code << ")";
}

} // namespace

// ===========================================================================
// 1. A NON-controller node leading partitions stops gracefully: every
//    partition it led is led by another in-sync replica at a HIGHER epoch in
//    every survivor's committed map BEFORE the process exits; a client right
//    after the stop is never redirected to the stopped node and its acks=all
//    produce succeeds on the first attempt; every acks=all record produced
//    before and after is committed and readable.
// ===========================================================================
TEST(ControlledShutdownRegression, NonControllerGracefulStopHandsEveryPartitionOffBeforeExitRegression) {
    TmpDir d0{"n0"}, d1{"n1"}, d2{"n2"}, cd{"cl"};
    const auto ports = Ports::discover(3);
    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    for (int i = 0; i < 3; ++i) {
        const fs::path& d = i == 0 ? d0.path : i == 1 ? d1.path : d2.path;
        ns[static_cast<std::size_t>(i)] = std::make_unique<BrokerNode>(makeConfig(i, d, ports));
        ns[static_cast<std::size_t>(i)]->start();
    }
    for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
    ASSERT_TRUE(eventually([&] { return spreadAndSettled(ns); }, 60'000ms)) << "leadership never spread";

    const int controller = consensusLeaderOnControl(*ns[0]);
    ASSERT_GE(controller, 0);
    const int victim = (controller + 1) % 3;
    std::vector<int32_t> victimParts;
    std::map<int32_t, int32_t> oldEpoch;
    for (int32_t p = 0; p < kParts; ++p) {
        const auto re = roleEpoch(*ns[static_cast<std::size_t>(victim)], 0, p);
        if (re.role == Role::Leader) { victimParts.push_back(p); oldEpoch[p] = re.epoch; }
    }
    ASSERT_FALSE(victimParts.empty());

    ShardTestClient c(cd.path);
    std::map<int, int64_t> sid;
    std::array<int, kParts> produced{};
    ASSERT_TRUE(produceAll(c, ports, controller, sid, 3, produced)) << "pre-stop produce failed";

    EXPECT_TRUE(ns[static_cast<std::size_t>(victim)]->controlledShutdown(20'000ms))
        << "controlled shutdown did not complete within its budget";

    // BEFORE the process exits: every survivor routes the victim's partitions
    // to another node at a higher epoch, and that node's replica leads.
    for (int32_t p : victimParts) {
        for (int i = 0; i < 3; ++i) {
            if (i == victim) continue;
            const auto cl = committedLeader(*ns[static_cast<std::size_t>(i)], 0, p);
            EXPECT_NE(cl.first, victim) << "node " << i << " still routes partition " << p << " to the stopping node";
            EXPECT_GE(cl.first, 0) << "node " << i << ": partition " << p << " leaderless";
            EXPECT_GT(cl.second, oldEpoch[p]) << "partition " << p << " not re-led at a higher epoch";
            if (cl.first >= 0 && cl.first != victim) {
                const auto re = roleEpoch(*ns[static_cast<std::size_t>(cl.first)], 0, p);
                EXPECT_EQ(re.role, Role::Leader) << "the new leader's replica does not lead";
                EXPECT_GT(re.epoch, oldEpoch[p]);
            }
        }
        EXPECT_NE(roleEpoch(*ns[static_cast<std::size_t>(victim)], 0, p).role, Role::Leader)
            << "the stopping node still leads partition " << p;
    }
    // The stop signal's moment: a client is never sent to the stopping node.
    expectNeverSentToVictim(c, ports, sid, victim, produced, "after the controlled shutdown, before exit");
    ns[static_cast<std::size_t>(victim)]->stop();
    expectNeverSentToVictim(c, ports, sid, victim, produced, "right after the stop");
    std::map<int, int64_t> sid2;
    for (auto& [k, v] : sid) if (k != victim) sid2[k] = v;
    ASSERT_TRUE(produceAll(c, ports, (victim + 1) % 3, sid2, 2, produced)) << "post-stop produce failed";

    // Every acks=all record produced before and after is committed + readable.
    for (int32_t p = 0; p < kParts; ++p) {
        const auto cl = committedLeader(*ns[static_cast<std::size_t>((victim + 1) % 3)], 0, p);
        ASSERT_GE(cl.first, 0);
        ASSERT_NE(cl.first, victim);
        EXPECT_TRUE(eventually([&] {
            return committedRecords(*ns[static_cast<std::size_t>(cl.first)], p) == produced[static_cast<std::size_t>(p)];
        }, 10'000ms)) << "partition " << p << ": " << committedRecords(*ns[static_cast<std::size_t>(cl.first)], p)
                      << " committed records, expected " << produced[static_cast<std::size_t>(p)];
    }
    for (int i = 0; i < 3; ++i) if (i != victim) ns[static_cast<std::size_t>(i)]->stop();
}

// ===========================================================================
// 2. The CONTROLLER (consensus leader) stops gracefully: its partitions move
//    first, then it hands the consensus leadership to a survivor before it
//    exits (no heartbeat-timeout re-election); the coordinator partitions
//    follow the new controller; clients are never sent to the stopped node.
// ===========================================================================
TEST(ControlledShutdownRegression, ControllerGracefulStopHandsConsensusAndPartitionsOffRegression) {
    TmpDir d0{"c0"}, d1{"c1"}, d2{"c2"}, cd{"ccl"};
    const auto ports = Ports::discover(3);
    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    for (int i = 0; i < 3; ++i) {
        const fs::path& d = i == 0 ? d0.path : i == 1 ? d1.path : d2.path;
        ns[static_cast<std::size_t>(i)] = std::make_unique<BrokerNode>(makeConfig(i, d, ports));
        ns[static_cast<std::size_t>(i)]->start();
    }
    for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
    ASSERT_TRUE(eventually([&] { return spreadAndSettled(ns); }, 60'000ms)) << "leadership never spread";

    const int victim = consensusLeaderOnControl(*ns[0]);
    ASSERT_GE(victim, 0);
    std::map<int32_t, int32_t> oldEpoch;
    for (int32_t p = 0; p < kParts; ++p) {
        const auto re = roleEpoch(*ns[static_cast<std::size_t>(victim)], 0, p);
        if (re.role == Role::Leader) oldEpoch[p] = re.epoch;
    }
    ASSERT_FALSE(oldEpoch.empty());

    ShardTestClient c(cd.path);
    std::map<int, int64_t> sid;
    std::array<int, kParts> produced{};
    ASSERT_TRUE(produceAll(c, ports, victim, sid, 3, produced)) << "pre-stop produce failed";

    EXPECT_TRUE(ns[static_cast<std::size_t>(victim)]->controlledShutdown(20'000ms))
        << "controlled shutdown did not complete within its budget";

    // BEFORE exit: a survivor is the consensus leader (it says so itself), and
    // the stopping node follows it.
    const int newController = consensusLeaderOnControl(*ns[static_cast<std::size_t>(victim)]);
    ASSERT_GE(newController, 0) << "the stopping controller saw no successor";
    ASSERT_NE(newController, victim) << "the stopping node still leads the consensus";
    EXPECT_EQ(consensusLeaderOnControl(*ns[static_cast<std::size_t>(newController)]), newController);
    for (int i = 0; i < 3; ++i) {
        if (i == victim) continue;
        for (const auto& [p, e] : oldEpoch) {
            const auto cl = committedLeader(*ns[static_cast<std::size_t>(i)], 0, p);
            EXPECT_NE(cl.first, victim) << "node " << i << " still routes partition " << p << " to the stopping node";
            EXPECT_GT(cl.second, e) << "partition " << p << " not re-led at a higher epoch";
        }
        for (int32_t t : {-1, -2})
            EXPECT_NE(committedLeader(*ns[static_cast<std::size_t>(i)], t, 0).first, victim)
                << "node " << i << ": coordinator partition " << t << " still led by the stopping node";
    }
    expectNeverSentToVictim(c, ports, sid, victim, produced, "after the controlled shutdown, before exit");
    const auto stopAt = std::chrono::steady_clock::now();
    ns[static_cast<std::size_t>(victim)]->stop();
    expectNeverSentToVictim(c, ports, sid, victim, produced, "right after the stop");
    std::map<int, int64_t> sid2;
    for (auto& [k, v] : sid) if (k != victim) sid2[k] = v;
    ASSERT_TRUE(produceAll(c, ports, newController, sid2, 2, produced)) << "post-stop produce failed";
    // The whole post-stop round trip ran without a ~2 s failover window.
    EXPECT_LT(std::chrono::steady_clock::now() - stopAt, 10s);
    // The consensus never left the new controller (no timeout re-election).
    for (int i = 0; i < 3; ++i)
        if (i != victim) EXPECT_EQ(consensusLeaderOnControl(*ns[static_cast<std::size_t>(i)]), newController);
    for (int32_t p = 0; p < kParts; ++p) {
        const auto cl = committedLeader(*ns[static_cast<std::size_t>(newController)], 0, p);
        ASSERT_GE(cl.first, 0);
        EXPECT_TRUE(eventually([&] {
            return committedRecords(*ns[static_cast<std::size_t>(cl.first)], p) == produced[static_cast<std::size_t>(p)];
        }, 10'000ms)) << "partition " << p << ": committed records != produced " << produced[static_cast<std::size_t>(p)];
    }
    for (int i = 0; i < 3; ++i) if (i != victim) ns[static_cast<std::size_t>(i)]->stop();
}

// ===========================================================================
// 3. A gracefully stopped node that restarts at once (DR-15: SIGTERM, then
//    the container restarts) is a normal member again: the controller's
//    shutdown mark is cleared by the new incarnation, the node re-enters the
//    ISRs and leadership re-spreads to it.
// ===========================================================================
TEST(ControlledShutdownRegression, GracefullyStoppedNodeRestartsAndLeadsAgainRegression) {
    TmpDir d0{"r0"}, d1{"r1"}, d2{"r2"};
    const auto ports = Ports::discover(3);
    std::array<NodeConfig, 3> cfg{makeConfig(0, d0.path, ports), makeConfig(1, d1.path, ports),
                                  makeConfig(2, d2.path, ports)};
    std::array<std::unique_ptr<BrokerNode>, 3> ns;
    for (int i = 0; i < 3; ++i) {
        ns[static_cast<std::size_t>(i)] = std::make_unique<BrokerNode>(cfg[static_cast<std::size_t>(i)]);
        ns[static_cast<std::size_t>(i)]->start();
    }
    for (auto& n : ns) ASSERT_TRUE(n->awaitLeader(30s));
    ASSERT_TRUE(eventually([&] { return spreadAndSettled(ns); }, 60'000ms)) << "leadership never spread";
    const int controller = consensusLeaderOnControl(*ns[0]);
    const int victim = (controller + 1) % 3;
    ASSERT_TRUE(ns[static_cast<std::size_t>(victim)]->controlledShutdown(20'000ms));
    ns[static_cast<std::size_t>(victim)]->stop();
    ns[static_cast<std::size_t>(victim)].reset();
    ns[static_cast<std::size_t>(victim)] = std::make_unique<BrokerNode>(cfg[static_cast<std::size_t>(victim)]);
    ns[static_cast<std::size_t>(victim)]->start();
    EXPECT_TRUE(eventually([&] { return spreadAndSettled(ns); }, 60'000ms))
        << "the restarted node never re-entered the ISRs / regained its preferred leadership";
    for (auto& n : ns) n->stop();
}
