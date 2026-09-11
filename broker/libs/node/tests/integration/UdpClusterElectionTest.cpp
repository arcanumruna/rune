// UdpClusterElectionTest.cpp — 3-node consensus over aeron:udp (BR-3).
//
// Tests:
//   1. ThreeNodeUdpCluster_ElectsOneLeader:
//      Starts 3 BrokerNodes each with their own embedded Aeron driver (distinct
//      temp dirs), all on 127.0.0.1 with distinct consensus UDP ports.
//      Waits for all three to report a leader within 10 s.
//      Asserts exactly ONE node reports isLeader() and all three agree on the
//      same leaderId().
//
// Unique temp paths: getpid() + atomic counter.
// All waits hard-capped with ASSERT (no infinite spin).

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>

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
// Build a 3-node NodeConfig for a given node index (0, 1, or 2).
// All 3 nodes have the same peer list but each reports a different nodeId.
// Ports: consensus 40111+nodeId (distinct), client 50101+nodeId (distinct).
// ---------------------------------------------------------------------------
static NodeConfig makeThreeNodeConfig(
    int nodeId,
    const fs::path& dataDir,
    int baseConsensusPort = 40211,  // Use high ports to avoid conflicts with BrokerNodeTest
    int baseClientPort    = 50201)
{
    NodeConfig cfg;
    cfg.nodeId            = nodeId;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    // All 3 peers on 127.0.0.1.
    for (int i = 0; i < 3; ++i) {
        PeerConfig p;
        p.id            = i;
        p.host          = "127.0.0.1";
        p.consensusPort = static_cast<uint16_t>(baseConsensusPort + i);
        cfg.peers.push_back(p);
    }

    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(baseClientPort + nodeId);
    cfg.clientEndpoint.streamId = 1;

    return cfg;
}

} // namespace

// ===========================================================================
// TEST: 3-node UDP cluster elects exactly one leader; all agree on leaderId
// ===========================================================================
TEST(UdpClusterElectionTest, ThreeNodeUdpCluster_ElectsOneLeader) {
    // Unique dirs per run (pid + counter).
    TempDir dir0{"n0_"}, dir1{"n1_"}, dir2{"n2_"};

    // Build configs — each node has all 3 peers in its peer list.
    // Use unique port range so this test doesn't clash with BrokerNodeTest's ports.
    const int BASE_CONSENSUS = 40311 + (static_cast<int>(::getpid()) % 100) * 3;
    const int BASE_CLIENT    = 50311 + (static_cast<int>(::getpid()) % 100) * 3;

    NodeConfig cfg0 = makeThreeNodeConfig(0, dir0.path, BASE_CONSENSUS, BASE_CLIENT);
    NodeConfig cfg1 = makeThreeNodeConfig(1, dir1.path, BASE_CONSENSUS, BASE_CLIENT);
    NodeConfig cfg2 = makeThreeNodeConfig(2, dir2.path, BASE_CONSENSUS, BASE_CLIENT);

    // Construct all 3 nodes.
    BrokerNode node0{cfg0};
    BrokerNode node1{cfg1};
    BrokerNode node2{cfg2};

    // Start all nodes — each embeds its own Aeron media driver in its own dir.
    node0.start();
    node1.start();
    node2.start();

    // --- Wait for ALL nodes to report a leader (hard deadline: 10 s) ---
    // awaitLeader() returns true as soon as leaderId() >= 0 (any node, leader OR follower).
    const auto kDeadline = 10s;

    bool ok0 = node0.awaitLeader(kDeadline);
    bool ok1 = node1.awaitLeader(kDeadline);
    bool ok2 = node2.awaitLeader(kDeadline);

    // Hard assertions — do NOT proceed if any node timed out (no fake pass).
    ASSERT_TRUE(ok0) << "node0 did not converge to a leader within " << kDeadline.count() << " s";
    ASSERT_TRUE(ok1) << "node1 did not converge to a leader within " << kDeadline.count() << " s";
    ASSERT_TRUE(ok2) << "node2 did not converge to a leader within " << kDeadline.count() << " s";

    // Query each node's view of the leader.
    rune::consensus::NodeId lid0 = node0.leaderId();
    rune::consensus::NodeId lid1 = node1.leaderId();
    rune::consensus::NodeId lid2 = node2.leaderId();

    // All three must agree on the same leader.
    EXPECT_EQ(lid0, lid1)
        << "node0 and node1 disagree on leader: " << lid0 << " vs " << lid1;
    EXPECT_EQ(lid1, lid2)
        << "node1 and node2 disagree on leader: " << lid1 << " vs " << lid2;

    // Exactly one node must report isLeader() == true.
    int leaderCount = 0;
    if (node0.isLeader()) ++leaderCount;
    if (node1.isLeader()) ++leaderCount;
    if (node2.isLeader()) ++leaderCount;

    EXPECT_EQ(leaderCount, 1)
        << "Expected exactly 1 leader, got " << leaderCount
        << " (leaderId=" << lid0 << ")";

    // The self-reported leader must match the agreed leaderId.
    rune::consensus::NodeId selfReportedLeader = -1;
    if (node0.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg0.nodeId);
    if (node1.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg1.nodeId);
    if (node2.isLeader()) selfReportedLeader = static_cast<rune::consensus::NodeId>(cfg2.nodeId);

    EXPECT_EQ(selfReportedLeader, lid0)
        << "Leader self-reports id=" << selfReportedLeader
        << " but peers see leaderId=" << lid0;

    // Clean stop — must not hang (bounded by ctest TIMEOUT).
    node0.stop();
    node1.stop();
    node2.stop();
}

} // namespace rune::node
