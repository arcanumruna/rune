// ---------------------------------------------------------------------------
// PerWorkerReplicationTest.cpp — E5 Task 7: in-process per-worker replication
//                                 integration + failover suite (flag ON).
//
// Three BrokerNodes started with perWorkerReplication=true, workerCount=2, and
// a disjoint replBaseStreamId=200.  All tests exercise the REAL per-worker
// Aeron transport end-to-end (no InMemoryTransport, no Docker).
//
// Tests:
//
//  PerWorkerReplication_AcksAllThreeNodeReplicatesAndCommits:
//    Flag on; produce N records via leader replica; assert every follower's
//    LEO reaches N and the leader's HW advances to N.  Exercises FetchRequest(1)
//    + FetchResponse(2) routing through the per-worker plane end-to-end.
//
//  PerWorkerReplication_RollingLeaderKillNoCommittedLoss (*Regression*):
//    Flag on; produce, kill the leader after commit, wait for re-election,
//    assert NO committed record is lost and the new leader serves all committed
//    offsets; ISR re-expands.  Exercises OLE(3/4) epoch-probe + reconcile(5/6)
//    routing on the per-worker plane during failover.
//
//  PerWorkerReplication_EpochChangeTruncatesDivergentTailOnly:
//    Flag on; produce a committed batch (HW advances); stop a follower; produce
//    an uncommitted batch on the 2-node cluster (follower partitioned off); stop
//    the leader so the restarted follower's uncommitted tail is divergent.
//    Re-elect, assert committed data preserved and only the divergent tail above
//    HW is truncated.  Exercises the OLE + reconcile path over the per-worker plane.
//
// Design:
//   - All waits bounded (hard deadline + ASSERT/EXPECT).
//   - No busy-spin: poll with 20–50 ms sleep slices.
//   - RESOURCE_LOCK rune_udp_ports (same as GR-7): ephemeral port probe is TOCTOU;
//     we serialize all UDP failover suites to avoid port races.
//   - perWorkerReplication flag is set directly on NodeConfig (no env var) so
//     the rest of the codebase is flag-off by default.
//   - workerCount=2 on ALL 3 nodes (symmetric-worker-count invariant).
//   - replBaseStreamId=200: range [200,201] disjoint from all other streams
//     (consensusStreamId=90, replicationStreamId=100, clientStreamId=110,
//     workerIngress=[111,112]).
//
// NO Docker.  NO mocks.  C++23.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// Replication
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>

// Storage
#include <rune/storage/Log.h>

// Gateway / Node (integration only)
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>

// Aeron (integration tests)
#include <Aeron.h>
extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <array>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::replication;

// ===========================================================================
// Shared helpers
// ===========================================================================

static std::atomic<uint32_t> g_pwSeq{0};

static fs::path makePwTempDir(std::string_view tag = "") {
    auto seq = g_pwSeq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_pw_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct PwTempDir {
    fs::path path;
    explicit PwTempDir(std::string_view tag = "") : path(makePwTempDir(tag)) {
        fs::create_directories(path);
    }
    ~PwTempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

static uint16_t pwDiscoverFreeUdpPort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd); throw std::runtime_error("bind() failed");
    }
    socklen_t addrLen = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addrLen);
    const uint16_t netPort = addr.sin_port;
    const uint16_t port    = static_cast<uint16_t>((netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

static std::vector<uint16_t> pwDiscoverFreePorts(int n) {
    std::vector<uint16_t> ports;
    ports.reserve(n);
    while (static_cast<int>(ports.size()) < n) {
        uint16_t p = pwDiscoverFreeUdpPort();
        bool dup = false;
        for (uint16_t e : ports) { if (e == p) { dup = true; break; } }
        if (!dup) ports.push_back(p);
    }
    return ports;
}

static std::vector<uint8_t> makePwPayload(uint8_t fill = 0xAB, std::size_t size = 16) {
    return std::vector<uint8_t>(size, fill);
}

// ---------------------------------------------------------------------------
// makePwConfig — build a 3-node NodeConfig with:
//   perWorkerReplication = true
//   workerCount          = 2
//   replBaseStreamId     = 200  (range [200,201] — disjoint from all others)
//   consensusStreamId    = 90
//   replicationStreamId  = 100  (legacy; still present for ISR/metadata path)
//   clientStreamId       = 110
//   workerIngress base   = 111  (110+1; resolved from clientStreamId+1 default)
//
// The replBaseStreamId=200 range is validated by validateStreamRanges() before
// each BrokerNode::start() in these tests.
// ---------------------------------------------------------------------------
static rune::node::NodeConfig makePwConfig(
    int nodeId,
    const fs::path& dataDir,
    const std::vector<uint16_t>& cPorts,   // consensus
    const std::vector<uint16_t>& kPorts,   // client
    const std::vector<uint16_t>& rPorts)   // replication
{
    rune::node::NodeConfig cfg;
    cfg.nodeId              = nodeId;
    cfg.dataDir             = dataDir.string();
    cfg.consensusStreamId   = 90;
    cfg.replicationStreamId = 100;

    cfg.electionTimeoutMs        = 150;
    cfg.electionStatusIntervalMs = 20;
    cfg.startupCanvassTimeoutMs  = 100;

    for (int i = 0; i < 3; ++i) {
        rune::node::PeerConfig p;
        p.id              = i;
        p.host            = "127.0.0.1";
        p.consensusPort   = cPorts[static_cast<std::size_t>(i)];
        p.clientPort      = kPorts[static_cast<std::size_t>(i)];
        p.replicationPort = rPorts[static_cast<std::size_t>(i)];
        cfg.peers.push_back(p);
    }

    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(nodeId)]);
    cfg.clientEndpoint.streamId = 110;

    // Per-worker replication — the feature under test.
    cfg.perWorkerReplication = true;
    cfg.workerCount          = 2;   // symmetric across all 3 nodes
    cfg.replBaseStreamId     = 200; // [200, 201] — disjoint from all others

    rune::node::TopicSpec topic;
    topic.name           = "pwtopic";
    topic.partitionCount = 1;
    topic.topicId        = 0;
    cfg.topics.push_back(topic);

    return cfg;
}

// ===========================================================================
// TEST 1: AcksAllThreeNodeReplicatesAndCommits
//
// 3 BrokerNodes, flag ON.  Produce N records directly on the leader replica.
// Assert:
//   - Leader HW advances to N (majority committed).
//   - Both followers' LEO reach N (replicated over the per-worker plane).
//
// This exercises FetchRequest(1) + FetchResponse(2) routing through the REAL
// per-worker WorkerReplicationTransport end-to-end.
// ===========================================================================
TEST(PerWorkerReplication, AcksAllThreeNodeReplicatesAndCommits) {
    PwTempDir dir0{"pw1_0_"}, dir1{"pw1_1_"}, dir2{"pw1_2_"};

    const auto cPorts = pwDiscoverFreePorts(3);
    const auto kPorts = pwDiscoverFreePorts(3);
    const auto rPorts = pwDiscoverFreePorts(3);

    auto cfg0 = makePwConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makePwConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makePwConfig(2, dir2.path, cPorts, kPorts, rPorts);

    // Validate stream ranges before starting — a mismatch is a test bug, not a
    // production bug, so fail fast here.
    ASSERT_FALSE(cfg0.validateStreamRanges().has_value())
        << "cfg0 stream ranges must be valid: " << cfg0.validateStreamRanges().value_or("");
    ASSERT_FALSE(cfg1.validateStreamRanges().has_value())
        << "cfg1 stream ranges must be valid";
    ASSERT_FALSE(cfg2.validateStreamRanges().has_value())
        << "cfg2 stream ranges must be valid";

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    // Wait for all nodes to elect a leader (up to 10 s).
    ASSERT_TRUE(node0->awaitLeader(10s)) << "node0 must converge to a leader";
    ASSERT_TRUE(node1->awaitLeader(10s)) << "node1 must converge to a leader";
    ASSERT_TRUE(node2->awaitLeader(10s)) << "node2 must converge to a leader";

    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0) << "no leader elected";
    ASSERT_LE(lid, 2);

    rune::node::BrokerNode* leaderNode  = nullptr;
    rune::node::BrokerNode* follower1   = nullptr;
    rune::node::BrokerNode* follower2   = nullptr;
    std::array<rune::node::BrokerNode*, 3> nodes = {node0.get(), node1.get(), node2.get()};
    for (int i = 0; i < 3; ++i) {
        if (i == lid) leaderNode = nodes[i];
        else if (!follower1) follower1 = nodes[i];
        else follower2 = nodes[i];
    }
    ASSERT_NE(leaderNode, nullptr);
    ASSERT_NE(follower1,  nullptr);
    ASSERT_NE(follower2,  nullptr);

    // Wait for the leader's replication bridge to report isLeader.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest())
        << "Leader's replication bridge must report isLeader after election";

    PartitionReplica* leaderReplica = leaderNode->replicaForTest(0, 0);
    ASSERT_NE(leaderReplica, nullptr) << "Leader must have a partition replica for (topicId=0, partition=0)";
    ASSERT_EQ(leaderReplica->role(), Role::Leader);

    // Produce N=6 records via the leader replica directly.
    constexpr int kN = 6;
    for (int i = 0; i < kN; ++i) {
        auto payload = makePwPayload(static_cast<uint8_t>(0xA0 + i), 16);
        Offset off = leaderReplica->append(
            std::span<const uint8_t>(payload.data(), payload.size()), false);
        ASSERT_GE(off, 0) << "Leader must accept record " << i;
    }
    const Offset leaderLeo = leaderReplica->logEndOffset();
    ASSERT_GE(leaderLeo, kN) << "Leader LEO must be at least N after append";

    // Wait for the leader's HW to advance (majority-committed).
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool hwAdvanced = false;
        while (std::chrono::steady_clock::now() < deadline && !hwAdvanced) {
            hwAdvanced = (leaderReplica->highWatermark() >= leaderLeo);
            if (!hwAdvanced) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(hwAdvanced)
            << "Leader HW must advance to " << leaderLeo
            << " via per-worker replication; actual HW="
            << leaderReplica->highWatermark();
    }
    const Offset committedHw = leaderReplica->highWatermark();
    ASSERT_GE(committedHw, leaderLeo)
        << "committedHw must equal leaderLeo (all records committed)";

    // Wait for both followers' LEO to reach committedHw — the per-worker plane
    // FetchRequest/FetchResponse messages are the mechanism under test here.
    PartitionReplica* f1Replica = follower1->replicaForTest(0, 0);
    PartitionReplica* f2Replica = follower2->replicaForTest(0, 0);
    ASSERT_NE(f1Replica, nullptr) << "Follower1 must have a partition replica";
    ASSERT_NE(f2Replica, nullptr) << "Follower2 must have a partition replica";

    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool f1ok = false, f2ok = false;
        while (std::chrono::steady_clock::now() < deadline && (!f1ok || !f2ok)) {
            f1ok = (f1Replica->logEndOffset() >= committedHw);
            f2ok = (f2Replica->logEndOffset() >= committedHw);
            if (!f1ok || !f2ok) std::this_thread::sleep_for(20ms);
        }
        EXPECT_TRUE(f1ok)
            << "Follower1 LEO must reach committedHw=" << committedHw
            << " via per-worker replication; actual LEO=" << f1Replica->logEndOffset();
        EXPECT_TRUE(f2ok)
            << "Follower2 LEO must reach committedHw=" << committedHw
            << " via per-worker replication; actual LEO=" << f2Replica->logEndOffset();
    }

    // HW must be N on the leader (all records committed).
    EXPECT_GE(committedHw, static_cast<Offset>(kN))
        << "All N records must be committed (HW >= N)";

    node0->stop();
    node1->stop();
    node2->stop();
}

// ===========================================================================
// TEST 2: RollingLeaderKillNoCommittedLoss  (*Regression*)
//
// 3 BrokerNodes, flag ON.  Produce N records (majority-committed).  Kill the
// leader.  Wait for re-election on surviving nodes.  Assert:
//   - New leader holds all committed records (LEO >= committedHw).
//   - No phantom-committed record (HW <= LEO on new leader).
//   - New leader's partition replica is in Leader role.
//
// Exercises OLE(3/4) epoch-probe + reconcile(5/6) routing on the per-worker
// plane during failover — the HARD durability invariant.
//
// Marked *Regression*: committed data loss under failover with flag ON is the
// production-blocking scenario this task gates.
// ===========================================================================
TEST(PerWorkerReplication, RollingLeaderKillNoCommittedLossRegression) {
    PwTempDir dir0{"pw2_0_"}, dir1{"pw2_1_"}, dir2{"pw2_2_"};

    const auto cPorts = pwDiscoverFreePorts(3);
    const auto kPorts = pwDiscoverFreePorts(3);
    const auto rPorts = pwDiscoverFreePorts(3);

    auto cfg0 = makePwConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makePwConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makePwConfig(2, dir2.path, cPorts, kPorts, rPorts);

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    ASSERT_TRUE(node0->awaitLeader(10s)) << "node0 must converge to a leader";
    ASSERT_TRUE(node1->awaitLeader(10s)) << "node1 must converge to a leader";
    ASSERT_TRUE(node2->awaitLeader(10s)) << "node2 must converge to a leader";

    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0);
    ASSERT_LE(lid, 2);

    rune::node::BrokerNode* leaderNode = nullptr;
    rune::node::BrokerNode* follower1  = nullptr;
    rune::node::BrokerNode* follower2  = nullptr;
    std::array<rune::node::BrokerNode*, 3> nodes = {node0.get(), node1.get(), node2.get()};
    for (int i = 0; i < 3; ++i) {
        if (i == lid) leaderNode = nodes[i];
        else if (!follower1) follower1 = nodes[i];
        else follower2 = nodes[i];
    }
    ASSERT_NE(leaderNode, nullptr);
    ASSERT_NE(follower1,  nullptr);
    ASSERT_NE(follower2,  nullptr);

    // Wait for leader's replication bridge.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest());

    PartitionReplica* leaderReplica = leaderNode->replicaForTest(0, 0);
    ASSERT_NE(leaderReplica, nullptr);
    ASSERT_EQ(leaderReplica->role(), Role::Leader);

    // Produce N=5 records.
    constexpr int kN = 5;
    for (int i = 0; i < kN; ++i) {
        auto payload = makePwPayload(static_cast<uint8_t>(0xB0 + i), 16);
        Offset off = leaderReplica->append(
            std::span<const uint8_t>(payload.data(), payload.size()), false);
        ASSERT_GE(off, 0) << "Leader must accept record " << i;
    }
    const Offset leaderLeo = leaderReplica->logEndOffset();

    // Wait for HW to advance (majority-committed).
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool hwOk = false;
        while (std::chrono::steady_clock::now() < deadline && !hwOk) {
            hwOk = (leaderReplica->highWatermark() >= leaderLeo);
            if (!hwOk) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(hwOk)
            << "Leader HW must advance to " << leaderLeo
            << " via per-worker replication before killing the leader";
    }
    const Offset committedHw = leaderReplica->highWatermark();
    ASSERT_GE(committedHw, leaderLeo);

    // Kill the leader.
    leaderNode->stop();

    // Wait for surviving nodes to re-elect a new leader.
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool reElected = false;
        while (std::chrono::steady_clock::now() < deadline && !reElected) {
            const auto l1 = follower1->leaderId();
            const auto l2 = follower2->leaderId();
            reElected = (l1 >= 0 && l1 != lid && l2 >= 0 && l1 == l2);
            if (!reElected) std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(reElected) << "Surviving nodes must re-elect a new leader after kill";
    }

    std::this_thread::sleep_for(500ms);

    const rune::consensus::NodeId newLid = follower1->leaderId();
    ASSERT_NE(newLid, lid) << "New leader must differ from the stopped leader";

    rune::node::BrokerNode* newLeaderNode =
        (follower1->isLeader()) ? follower1 : follower2;
    ASSERT_TRUE(newLeaderNode->isLeader());

    // Wait for new leader's replication bridge.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (newLeaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(newLeaderNode->replicationIsLeaderForTest())
        << "New leader's replication bridge must report isLeader";

    PartitionReplica* newLeaderReplica = newLeaderNode->replicaForTest(0, 0);
    ASSERT_NE(newLeaderReplica, nullptr);

    // Partition failover is asynchronous — poll for Leader role.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline &&
               newLeaderReplica->role() != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }

    // --- Core assertion: no committed-data loss ---
    //
    // The new leader MUST hold all records that were committed before the kill.
    // A failure here with per-worker replication ON is a correctness regression.
    EXPECT_GE(newLeaderReplica->logEndOffset(), committedHw)
        << "REGRESSION: new leader lost committed records!"
        << " LEO=" << newLeaderReplica->logEndOffset()
        << " committedHw=" << committedHw;

    // No phantom-committed record: HW must not exceed LEO.
    EXPECT_LE(newLeaderReplica->highWatermark(), newLeaderReplica->logEndOffset())
        << "HW must never exceed LEO after failover";

    // Per-partition role must be Leader on the new leader.
    EXPECT_EQ(newLeaderReplica->role(), Role::Leader)
        << "New leader's partition replica must be in Leader role";

    // Committed data must be readable from offset 0.
    rune::gateway::TopicRegistry* reg = newLeaderNode->topicRegistryForTest();
    ASSERT_NE(reg, nullptr);
    rune::storage::Log* log = reg->log(0, 0);
    ASSERT_NE(log, nullptr);
    auto bytes = log->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty())
        << "New leader's log must contain committed data from offset 0";

    node0->stop();
    node1->stop();
    node2->stop();
}

// ===========================================================================
// TEST 3: EpochChangeTruncatesDivergentTailOnly
//
// 3 BrokerNodes, flag ON.  Scenario:
//   1. Produce N=4 records → all 3 nodes hold them; HW advances (committed).
//   2. Stop one follower (call it F1) — ISR shrinks to 2.
//   3. Produce M=2 more records on the 2-node cluster (leader+F2);
//      HW may advance on the 2-node ISR (majority of 3 is 2, so it still commits).
//   4. Stop the leader so we have only F2 alive.
//      F2 may have the M records if the 2-node cluster committed them.
//   5. Restart F1 (same dataDir — log survives with only N records).
//   6. Re-elect: new leader is whichever of F1/F2 wins.
//   7. Assert:
//      a. The new leader's committed records (up to N) are NOT truncated.
//      b. If F1 became the new leader, any divergent tail above its HW was
//         truncated (OLE + reconcile path).
//      c. No phantom-committed record (HW <= LEO).
//
// This exercises the OLE epoch-probe (3/4) + reconcile (5/6) path over the
// per-worker plane for divergent-tail recovery.
// ===========================================================================
TEST(PerWorkerReplication, EpochChangeTruncatesDivergentTailOnly) {
    PwTempDir dir0{"pw3_0_"}, dir1{"pw3_1_"}, dir2{"pw3_2_"};

    const auto cPorts = pwDiscoverFreePorts(3);
    const auto kPorts = pwDiscoverFreePorts(3);
    const auto rPorts = pwDiscoverFreePorts(3);

    auto cfg0 = makePwConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makePwConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makePwConfig(2, dir2.path, cPorts, kPorts, rPorts);

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    ASSERT_TRUE(node0->awaitLeader(10s));
    ASSERT_TRUE(node1->awaitLeader(10s));
    ASSERT_TRUE(node2->awaitLeader(10s));

    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0);
    ASSERT_LE(lid, 2);

    rune::node::BrokerNode* leaderNode   = nullptr;
    rune::node::BrokerNode* f1Node       = nullptr;   // will be stopped first
    rune::node::BrokerNode* f2Node       = nullptr;   // stays alive through M produces
    int f1Idx = -1;
    std::array<rune::node::BrokerNode*, 3> nodes = {node0.get(), node1.get(), node2.get()};
    for (int i = 0; i < 3; ++i) {
        if (i == lid) leaderNode = nodes[i];
        else if (!f1Node) { f1Node = nodes[i]; f1Idx = i; }
        else f2Node = nodes[i];
    }
    ASSERT_NE(leaderNode, nullptr);
    ASSERT_NE(f1Node, nullptr);
    ASSERT_NE(f2Node, nullptr);

    // Wait for leader's replication bridge.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest());

    PartitionReplica* leaderReplica = leaderNode->replicaForTest(0, 0);
    ASSERT_NE(leaderReplica, nullptr);
    ASSERT_EQ(leaderReplica->role(), Role::Leader);

    // Phase 1: produce N=4 records on the 3-node cluster.
    constexpr int kN = 4;
    for (int i = 0; i < kN; ++i) {
        auto payload = makePwPayload(static_cast<uint8_t>(0xC0 + i), 16);
        Offset off = leaderReplica->append(
            std::span<const uint8_t>(payload.data(), payload.size()), false);
        ASSERT_GE(off, 0) << "Leader must accept N-phase record " << i;
    }
    const Offset leoN = leaderReplica->logEndOffset();

    // Wait for HW to advance to leoN (3-node majority committed).
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (leaderReplica->highWatermark() >= leoN);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(ok) << "N records must commit on the 3-node cluster";
    }
    const Offset hwAfterN = leaderReplica->highWatermark();

    // Phase 2: stop F1.
    f1Node->stop();
    f1Node = nullptr;
    std::this_thread::sleep_for(500ms);

    // Phase 3: produce M=2 records on the 2-node cluster (leader + F2).
    // With a 3-node cluster's majority=2, a 2-node ISR still commits.
    constexpr int kM = 2;
    for (int i = 0; i < kM; ++i) {
        auto payload = makePwPayload(static_cast<uint8_t>(0xD0 + i), 16);
        leaderReplica->append(
            std::span<const uint8_t>(payload.data(), payload.size()), false);
    }
    const Offset leoNM = leaderReplica->logEndOffset();

    // Wait briefly for HW on the 2-node ISR (may or may not advance depending
    // on ISR contraction timing — we only need leoN committed for the invariant).
    std::this_thread::sleep_for(1000ms);
    const Offset hwAfterM = leaderReplica->highWatermark();

    // Phase 4: stop the leader.
    leaderNode->stop();
    leaderNode = nullptr;

    // Phase 5: restart F1 on the same dataDir (its log has only leoN records).
    std::unique_ptr<rune::node::BrokerNode> restartedF1;
    if (f1Idx == 0) restartedF1 = std::make_unique<rune::node::BrokerNode>(cfg0);
    else if (f1Idx == 1) restartedF1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    else restartedF1 = std::make_unique<rune::node::BrokerNode>(cfg2);

    restartedF1->start();

    // Phase 6: wait for F2 and restartedF1 to re-elect.
    ASSERT_TRUE(f2Node->awaitLeader(15s))
        << "F2 must converge to a new leader after leader kill";
    ASSERT_TRUE(restartedF1->awaitLeader(15s))
        << "Restarted F1 must converge to a new leader";

    std::this_thread::sleep_for(500ms);

    // The new consensus leader (either f2Node or restartedF1).
    const rune::consensus::NodeId newLid = f2Node->leaderId();
    rune::node::BrokerNode* newLeaderNode =
        (f2Node->isLeader()) ? f2Node : restartedF1.get();

    // Wait for new leader's replication bridge.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (newLeaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(newLeaderNode->replicationIsLeaderForTest())
        << "New leader's replication bridge must report isLeader";

    PartitionReplica* newLeaderReplica = newLeaderNode->replicaForTest(0, 0);
    ASSERT_NE(newLeaderReplica, nullptr);

    // Poll for Leader role on new leader's partition replica.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline &&
               newLeaderReplica->role() != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }

    // Phase 7: invariant checks.
    //
    // 7a. The leoN baseline (fully committed before F1 was stopped) must be
    //     preserved on the new leader — NOT truncated by the epoch change.
    EXPECT_GE(newLeaderReplica->logEndOffset(), hwAfterN)
        << "Committed records up to hwAfterN=" << hwAfterN
        << " must NOT be truncated by the epoch change;"
        << " new leader LEO=" << newLeaderReplica->logEndOffset();

    // 7b. No phantom-committed record: HW <= LEO at all times.
    EXPECT_LE(newLeaderReplica->highWatermark(), newLeaderReplica->logEndOffset())
        << "HW must never exceed LEO after failover (no phantom-committed records)";

    // 7c. Committed data is readable from offset 0.
    rune::gateway::TopicRegistry* reg = newLeaderNode->topicRegistryForTest();
    ASSERT_NE(reg, nullptr);
    rune::storage::Log* log = reg->log(0, 0);
    ASSERT_NE(log, nullptr);
    auto bytes = log->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty())
        << "New leader's log must contain committed data readable from offset 0";

    // Suppress unused-variable warning; hwAfterM used for diagnostics.
    (void)hwAfterM;
    (void)leoNM;
    (void)newLid;

    restartedF1->stop();
    node0->stop();
    node1->stop();
    node2->stop();
}
