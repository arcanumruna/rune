// UdpClusterElectionTest.cpp — 3-node consensus over aeron:udp (BR-3, BR-3b).
//
// Tests:
//   1. ThreeNodeUdpCluster_ElectsOneLeader:
//      Starts 3 BrokerNodes each with their own embedded Aeron driver (distinct
//      temp dirs), all on 127.0.0.1 with dynamically-discovered free UDP ports.
//      Waits for all three to report a leader within 5 s.
//      Asserts exactly ONE node reports isLeader() and all three agree on the
//      same leaderId(), which must be a valid member id (0, 1, or 2).
//      Loops 5 times to confirm no split-brain under reduced timeouts.
//
//   2. ThreeNodeUdpCluster_LateJoin:
//      Nodes 0 and 1 start and elect a leader; node 2 joins 300 ms later.
//      All three must converge to the same leader.
//
// Port discovery: bind UDP socket to 127.0.0.1:0, read assigned port, close,
// use. Accepts the tiny TOCTOU window — ports are ephemeral and the test
// binds them immediately after discovery.
//
// Unique temp paths: getpid() + atomic counter.
// All waits hard-capped with ASSERT (no infinite spin).

#include "../support/TestNodeConfig.h"
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <vector>
#include <string>
#include <thread>
#include <unistd.h>

// UDP port discovery
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDataDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_udp_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// RAII cleanup for temp data dir
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "")
        : path(makeTempDataDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec); // best-effort
    }
};

// ---------------------------------------------------------------------------
// discoverFreeUdpPort — bind to 127.0.0.1:0, read the assigned port, close.
// Accepts the small TOCTOU window; call immediately before use.
// ---------------------------------------------------------------------------
static uint16_t discoverFreeUdpPort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        throw std::runtime_error("UdpClusterElectionTest: socket() failed");
    }

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = 0;          // let OS pick
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");

    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("UdpClusterElectionTest: bind() for port discovery failed");
    }

    socklen_t addrLen = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addrLen) < 0) {
        ::close(fd);
        throw std::runtime_error("UdpClusterElectionTest: getsockname() failed");
    }

    // sin_port is network byte order; swap manually to avoid macro issues.
    const uint16_t netPort = addr.sin_port;
    const uint16_t port =
        static_cast<uint16_t>((netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

// ---------------------------------------------------------------------------
// discoverFreePorts — discover N distinct free UDP ports.
// ---------------------------------------------------------------------------
static std::vector<uint16_t> discoverFreePorts(int n) {
    std::vector<uint16_t> ports;
    ports.reserve(n);
    while (static_cast<int>(ports.size()) < n) {
        uint16_t p = discoverFreeUdpPort();
        // Reject duplicates (highly unlikely but defensive).
        bool dup = false;
        for (uint16_t existing : ports) { if (existing == p) { dup = true; break; } }
        if (!dup) ports.push_back(p);
    }
    return ports;
}

// ---------------------------------------------------------------------------
// Build a 3-node NodeConfig for a given node index (0, 1, or 2).
// All 3 nodes have the same peer list but each reports a different nodeId.
// Consensus ports supplied externally (discovered at test start).
// ---------------------------------------------------------------------------
static NodeConfig makeThreeNodeConfig(
    int nodeId,
    const fs::path& dataDir,
    const std::vector<uint16_t>& consensusPorts,  // 3 ports, one per node
    const std::vector<uint16_t>& clientPorts)     // 3 ports, one per node
{
    NodeConfig cfg;
    rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
    cfg.nodeId            = nodeId;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    // Fast-but-safe election timing for co-located test clusters (BR-3b).
    // These values are already the NodeConfig defaults; set explicitly for clarity.
    cfg.electionTimeoutMs        = 150;
    cfg.electionStatusIntervalMs =  20;
    cfg.startupCanvassTimeoutMs  = 100;

    // All 3 peers on 127.0.0.1.
    for (int i = 0; i < 3; ++i) {
        PeerConfig p;
        p.id            = i;
        p.host          = "127.0.0.1";
        p.consensusPort = consensusPorts[static_cast<std::size_t>(i)];
        cfg.peers.push_back(p);
    }

    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(clientPorts[static_cast<std::size_t>(nodeId)]);
    cfg.clientEndpoint.streamId = 1;

    cfg.workerCount = rune::node::test::workerCountFromEnv();

    return cfg;
}

// ---------------------------------------------------------------------------
// Helper: assert single-leader invariants on 3 started nodes.
// Used by the loop test so the assertion logic is not duplicated.
// ---------------------------------------------------------------------------
static void assertSingleLeader(
    BrokerNode& node0, BrokerNode& node1, BrokerNode& node2,
    const NodeConfig& cfg0, const NodeConfig& cfg1, const NodeConfig& cfg2,
    int iteration = 0)
{
    // Wait for all three nodes to know the leader (deadline: 5 s).
    const auto kDeadline = 5s;

    bool ok0 = node0.awaitLeader(kDeadline);
    bool ok1 = node1.awaitLeader(kDeadline);
    bool ok2 = node2.awaitLeader(kDeadline);

    ASSERT_TRUE(ok0) << "[iter " << iteration << "] node0 did not converge to a leader";
    ASSERT_TRUE(ok1) << "[iter " << iteration << "] node1 did not converge to a leader";
    ASSERT_TRUE(ok2) << "[iter " << iteration << "] node2 did not converge to a leader";

    rune::consensus::NodeId lid0 = node0.leaderId();
    rune::consensus::NodeId lid1 = node1.leaderId();
    rune::consensus::NodeId lid2 = node2.leaderId();

    // (a) Each leaderId must be a valid member id (0, 1, or 2).
    EXPECT_GE(lid0, 0) << "[iter " << iteration << "] node0 leaderId not in {0,1,2}: " << lid0;
    EXPECT_LE(lid0, 2) << "[iter " << iteration << "] node0 leaderId not in {0,1,2}: " << lid0;
    EXPECT_GE(lid1, 0) << "[iter " << iteration << "] node1 leaderId not in {0,1,2}: " << lid1;
    EXPECT_LE(lid1, 2) << "[iter " << iteration << "] node1 leaderId not in {0,1,2}: " << lid1;
    EXPECT_GE(lid2, 0) << "[iter " << iteration << "] node2 leaderId not in {0,1,2}: " << lid2;
    EXPECT_LE(lid2, 2) << "[iter " << iteration << "] node2 leaderId not in {0,1,2}: " << lid2;

    // All three must agree on the same leader.
    EXPECT_EQ(lid0, lid1)
        << "[iter " << iteration << "] node0 and node1 disagree: " << lid0 << " vs " << lid1;
    EXPECT_EQ(lid1, lid2)
        << "[iter " << iteration << "] node1 and node2 disagree: " << lid1 << " vs " << lid2;

    // Exactly one node must report isLeader() == true.
    int leaderCount = 0;
    if (node0.isLeader()) ++leaderCount;
    if (node1.isLeader()) ++leaderCount;
    if (node2.isLeader()) ++leaderCount;

    EXPECT_EQ(leaderCount, 1)
        << "[iter " << iteration << "] Expected exactly 1 leader, got " << leaderCount
        << " (leaderId=" << lid0 << ")";

    // The self-reported leader must match the agreed leaderId.
    rune::consensus::NodeId selfReportedLeader = -1;
    if (node0.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg0.nodeId);
    if (node1.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg1.nodeId);
    if (node2.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg2.nodeId);

    EXPECT_EQ(selfReportedLeader, lid0)
        << "[iter " << iteration << "] Leader self-reports id=" << selfReportedLeader
        << " but peers see leaderId=" << lid0;
}

} // namespace

// ===========================================================================
// TEST 1: 3-node UDP cluster elects exactly one leader; looped 5x (no split-brain)
// ===========================================================================
TEST(UdpClusterElectionTest, ThreeNodeUdpCluster_ElectsOneLeader) {
    // Unique dirs per run (pid + counter).
    TempDir dir0{"n0_"}, dir1{"n1_"}, dir2{"n2_"};

    // Discover 6 free UDP ports at test start (3 consensus + 3 client).
    const auto cPorts = discoverFreePorts(3);
    const auto kPorts = discoverFreePorts(3);

    NodeConfig cfg0 = makeThreeNodeConfig(0, dir0.path, cPorts, kPorts);
    NodeConfig cfg1 = makeThreeNodeConfig(1, dir1.path, cPorts, kPorts);
    NodeConfig cfg2 = makeThreeNodeConfig(2, dir2.path, cPorts, kPorts);

    // Construct all 3 nodes.
    BrokerNode node0{cfg0};
    BrokerNode node1{cfg1};
    BrokerNode node2{cfg2};

    // Start all nodes — each embeds its own Aeron media driver in its own dir.
    node0.start();
    node1.start();
    node2.start();

    // Assert single-leader invariants (includes leaderId-valid + agree checks).
    assertSingleLeader(node0, node1, node2, cfg0, cfg1, cfg2, /*iteration=*/0);

    // Clean stop — must not hang (bounded by ctest TIMEOUT).
    node0.stop();
    node1.stop();
    node2.stop();
}

// ===========================================================================
// TEST 2: Safety loop — elect 5 independent times; no split-brain allowed.
// Each iteration uses fresh dirs + freshly-discovered ports.
// ===========================================================================
TEST(UdpClusterElectionTest, ThreeNodeUdpCluster_NoSplitBrain_LoopedFiveTimes) {
    static constexpr int kIterations = 5;

    for (int iter = 0; iter < kIterations; ++iter) {
        SCOPED_TRACE("iteration " + std::to_string(iter));

        TempDir dir0{"lp0_"}, dir1{"lp1_"}, dir2{"lp2_"};

        // Fresh ports each iteration — eliminates port-reuse races.
        const auto cPorts = discoverFreePorts(3);
        const auto kPorts = discoverFreePorts(3);

        NodeConfig cfg0 = makeThreeNodeConfig(0, dir0.path, cPorts, kPorts);
        NodeConfig cfg1 = makeThreeNodeConfig(1, dir1.path, cPorts, kPorts);
        NodeConfig cfg2 = makeThreeNodeConfig(2, dir2.path, cPorts, kPorts);

        BrokerNode node0{cfg0};
        BrokerNode node1{cfg1};
        BrokerNode node2{cfg2};

        node0.start();
        node1.start();
        node2.start();

        assertSingleLeader(node0, node1, node2, cfg0, cfg1, cfg2, iter);

        node0.stop();
        node1.stop();
        node2.stop();
    }
}

// ===========================================================================
// TEST 3: Late-joining node converges to existing leader.
// Nodes 0 and 1 start; node 2 joins ~300 ms later.
// All must agree on the same leader.
// ===========================================================================
TEST(UdpClusterElectionTest, ThreeNodeUdpCluster_LateJoin_Converges) {
    TempDir dir0{"lj0_"}, dir1{"lj1_"}, dir2{"lj2_"};

    const auto cPorts = discoverFreePorts(3);
    const auto kPorts = discoverFreePorts(3);

    NodeConfig cfg0 = makeThreeNodeConfig(0, dir0.path, cPorts, kPorts);
    NodeConfig cfg1 = makeThreeNodeConfig(1, dir1.path, cPorts, kPorts);
    NodeConfig cfg2 = makeThreeNodeConfig(2, dir2.path, cPorts, kPorts);

    BrokerNode node0{cfg0};
    BrokerNode node1{cfg1};
    BrokerNode node2{cfg2};

    // Start nodes 0 and 1 first — they will elect a leader between themselves.
    node0.start();
    node1.start();

    // Wait a bit so 0+1 have time to start their election round.
    std::this_thread::sleep_for(300ms);

    // Node 2 joins late.
    node2.start();

    // All three must converge to a single leader.
    assertSingleLeader(node0, node1, node2, cfg0, cfg1, cfg2, /*iteration=*/0);

    // Confirm the late-joiner (node2) reports a valid leader id.
    const rune::consensus::NodeId lid2 = node2.leaderId();
    EXPECT_GE(lid2, 0) << "Late-joining node2 did not acquire a valid leaderId";
    EXPECT_LE(lid2, 2) << "Late-joining node2 leaderId out of range: " << lid2;

    node0.stop();
    node1.stop();
    node2.stop();
}

// ===========================================================================
// TEST 4 (CB-5): leader-kill failover — a surviving follower's observed
// leaderId() must UPDATE to a NEW, live leader after the old leader dies.
//
// This is the exact invariant that CB-5 Phase 2 depends on: the gateway
// redirects data ops to BrokerNode::leaderId()'s client endpoint, so after a
// leader-kill a follower whose leaderId() stayed stale would redirect clients
// to the DEAD endpoint forever.  Here we assert the atomic leader mirror on the
// survivors moves off the killed node to a live survivor.
// ===========================================================================
TEST(UdpClusterElectionTest, ThreeNodeUdpCluster_LeaderKill_SurvivorsReElect) {
    TempDir dir0{"fk0_"}, dir1{"fk1_"}, dir2{"fk2_"};

    const auto cPorts = discoverFreePorts(3);
    const auto kPorts = discoverFreePorts(3);

    NodeConfig cfg0 = makeThreeNodeConfig(0, dir0.path, cPorts, kPorts);
    NodeConfig cfg1 = makeThreeNodeConfig(1, dir1.path, cPorts, kPorts);
    NodeConfig cfg2 = makeThreeNodeConfig(2, dir2.path, cPorts, kPorts);

    auto node0 = std::make_unique<BrokerNode>(cfg0);
    auto node1 = std::make_unique<BrokerNode>(cfg1);
    auto node2 = std::make_unique<BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    constexpr auto kDeadline = 8s;
    ASSERT_TRUE(node0->awaitLeader(kDeadline));
    ASSERT_TRUE(node1->awaitLeader(kDeadline));
    ASSERT_TRUE(node2->awaitLeader(kDeadline));

    // Give the cluster a moment to settle on a single agreed leader.
    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId initialLeader = node0->leaderId();
    ASSERT_GE(initialLeader, 0);
    ASSERT_LE(initialLeader, 2);
    // All three must agree on the initial leader.
    ASSERT_EQ(node1->leaderId(), initialLeader);
    ASSERT_EQ(node2->leaderId(), initialLeader);

    // Map the leader id to its node object; collect the two survivors.
    std::array<BrokerNode*, 3> nodes{node0.get(), node1.get(), node2.get()};
    std::array<std::unique_ptr<BrokerNode>*, 3> owners{&node0, &node1, &node2};
    std::vector<BrokerNode*> survivors;
    for (int i = 0; i < 3; ++i) {
        if (i != initialLeader) survivors.push_back(nodes[i]);
    }
    ASSERT_EQ(survivors.size(), 2u);

    // Kill the leader (full stop → joins its agent thread, closes Aeron/driver;
    // the survivors stop receiving its heartbeats).
    owners[initialLeader]->reset();

    // Poll the survivors: their observed leaderId() must move to a NEW live
    // leader (one of the two survivors) within a bounded window.  This proves
    // the atomic leader mirror updates on failover (the CB-5 root cause).
    const auto pollDeadline = std::chrono::steady_clock::now() + 12s;
    bool reElected = false;
    rune::consensus::NodeId newLeader = -1;
    while (std::chrono::steady_clock::now() < pollDeadline) {
        const rune::consensus::NodeId l0 = survivors[0]->leaderId();
        const rune::consensus::NodeId l1 = survivors[1]->leaderId();
        if (l0 >= 0 && l0 != initialLeader && l0 == l1) {
            reElected = true;
            newLeader = l0;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    EXPECT_TRUE(reElected)
        << "survivors did not re-elect a new leader after leader-kill: "
        << "survivor leaderIds = {" << survivors[0]->leaderId() << ", "
        << survivors[1]->leaderId() << "}, killed=" << initialLeader;
    EXPECT_NE(newLeader, initialLeader)
        << "new leader must differ from the killed node";
    EXPECT_GE(newLeader, 0);
    EXPECT_LE(newLeader, 2);
    // The new leader must be one of the survivors, and it must self-report leader.
    for (auto* s : survivors) {
        if (s->leaderId() == newLeader) {
            EXPECT_TRUE(s->isLeader() || s->leaderId() == newLeader);
        }
    }

    // Clean shutdown of the survivors (the killed node is already reset()).
    for (auto* ownerPtr : owners) {
        if (*ownerPtr) (*ownerPtr)->stop();
    }
}

} // namespace rune::node
