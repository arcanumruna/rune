// BrokerNodeTest.cpp — Integration tests for BrokerNode (BR-2).
//
// Tests:
//   1. Single-node self-elects within 2 s.
//   2. Clean shutdown: stop() twice is safe (idempotent).
//   3. Dtor-after-start: no hang or crash.
//   4. Data dir created on start(); node-state.dat and term.log exist.
//   5. Agent idles (no busy-spin): after leadership, awaitLeader() is immediate
//      and stop() returns promptly (bounded 3 s).
//
// All waits are bounded with hard deadlines + ASSERT.
// Unique temp paths use getpid() + atomic counter.

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator  (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDataDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_bn_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

// ---------------------------------------------------------------------------
// Build a 1-node NodeConfig pointing at a fresh temp dir
// ---------------------------------------------------------------------------
static NodeConfig makeSingleNodeConfig(const fs::path& dataDir) {
    NodeConfig cfg;
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = 40111;
    cfg.peers          = {self};

    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:50001";
    cfg.clientEndpoint.streamId = 1;

    return cfg;
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

} // namespace

// ===========================================================================
// TEST 1: Single-node self-elects within 2 s
// ===========================================================================
TEST(BrokerNodeTest, SingleNode_SelfElects) {
    TempDir tmp{"t1_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(2s))
        << "1-node cluster should self-elect within 2 s";

    EXPECT_TRUE(node.isLeader());
    EXPECT_EQ(node.leaderId(), static_cast<rune::consensus::NodeId>(cfg.nodeId));

    node.stop();
}

// ===========================================================================
// TEST 2: Clean shutdown — stop() twice is safe (idempotent)
// ===========================================================================
TEST(BrokerNodeTest, StopIdempotent) {
    TempDir tmp{"t2_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();

    // Wait for leadership so the runner is active before first stop().
    ASSERT_TRUE(node.awaitLeader(2s))
        << "1-node cluster should self-elect within 2 s";

    // First stop — must not hang (hard deadline: test timeout 60 s from ctest).
    node.stop();

    // Second stop — idempotent, must not crash or hang.
    node.stop();
}

// ===========================================================================
// TEST 3: Dtor after start — no hang or crash
// ===========================================================================
TEST(BrokerNodeTest, DtorAfterStart_NoCrash) {
    TempDir tmp{"t3_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    {
        BrokerNode node{cfg};
        node.start();
        // Allow a short window so the runner thread actually starts.
        std::this_thread::sleep_for(200ms);
        // Let the dtor call stop() implicitly — must not hang.
    }
    // If we reached here: OK.
    SUCCEED();
}

// ===========================================================================
// TEST 4: Data dir created; node-state.dat and term.log exist after start
// ===========================================================================
TEST(BrokerNodeTest, DataDirCreated_FilesExist) {
    TempDir tmp{"t4_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    ASSERT_FALSE(fs::exists(tmp.path))
        << "Precondition: dataDir must not exist before start()";

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(fs::exists(tmp.path))
        << "start() must create dataDir";
    EXPECT_TRUE(fs::exists(tmp.path / "node-state.dat"))
        << "node-state.dat must exist after start()";
    EXPECT_TRUE(fs::exists(tmp.path / "term.log"))
        << "term.log must exist after start()";

    node.stop();
}

// ===========================================================================
// TEST 5: Agent idles — no busy-spin; stop() is prompt (≤ 3 s)
// ===========================================================================
TEST(BrokerNodeTest, Stop_IsPrompt_AfterLeadership) {
    TempDir tmp{"t5_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(2s))
        << "1-node cluster should self-elect within 2 s";

    // A second awaitLeader must return immediately (already leader).
    EXPECT_TRUE(node.awaitLeader(100ms))
        << "awaitLeader() should be immediate when already leader";

    // stop() must complete within 3 s (BackoffIdleStrategy parks ≤ 1 ms per cycle).
    using sclock = std::chrono::steady_clock;
    const auto before = sclock::now();
    node.stop();
    const auto elapsed = sclock::now() - before;

    EXPECT_LT(elapsed, 3s)
        << "stop() should return within 3 s after leadership is established";
}

// ===========================================================================
// TEST 6: awaitLeader timeout → false (never started → no leader)
// ===========================================================================
TEST(BrokerNodeTest, AwaitLeader_Timeout_ReturnsFalse) {
    TempDir tmp{"t6_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    // Do NOT call start() — no agent thread, no election → leadership never reached.
    BrokerNode node{cfg};

    using sclock = std::chrono::steady_clock;
    const auto before = sclock::now();
    const bool result = node.awaitLeader(50ms);
    const auto elapsed = sclock::now() - before;

    EXPECT_FALSE(result) << "awaitLeader() must return false when node is not started";
    // The wait must have actually elapsed (bounded check — should be ≥ 50 ms and < 5 s).
    EXPECT_GE(elapsed, 50ms) << "awaitLeader() must not return before timeout";
    EXPECT_LT(elapsed, 5s)   << "awaitLeader() must not block beyond its timeout";
}

// ===========================================================================
// TEST 7: double-start is idempotent — second start() is a no-op
// ===========================================================================
TEST(BrokerNodeTest, DoubleStart_Idempotent) {
    TempDir tmp{"t7_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();  // first start — launches driver + agent
    node.start();  // second start — must be a no-op (no second driver, no crash)

    // Node must still reach leadership normally — only one election running.
    ASSERT_TRUE(node.awaitLeader(2s))
        << "After double-start, node should still self-elect within 2 s";

    EXPECT_TRUE(node.isLeader());
    EXPECT_EQ(node.leaderId(), static_cast<rune::consensus::NodeId>(cfg.nodeId));

    // Clean stop must succeed exactly once.
    node.stop();
}

// ===========================================================================
// TEST 8: start() failure — bad dataDir → error surfaces; stop()/dtor clean
// ===========================================================================
TEST(BrokerNodeTest, Start_BadDataDir_ErrorAndCleanShutdown) {
    // Use a path where a regular FILE exists where the directory should be,
    // making create_directories fail deterministically.
    TempDir tmp{"t8_parent_"};
    // Create the parent so we can plant a file.
    std::error_code ec;
    fs::create_directories(tmp.path, ec);
    ASSERT_FALSE(ec) << "Could not create parent for test: " << ec.message();

    // Plant a regular file where BrokerNode would need to create a directory.
    const fs::path badDataDir = tmp.path / "blocker";
    {
        std::ofstream f(badDataDir);
        ASSERT_TRUE(f.is_open()) << "Could not create blocker file";
        f << "blocker";
    }
    ASSERT_TRUE(fs::is_regular_file(badDataDir))
        << "Precondition: blocker must be a regular file";

    NodeConfig cfg = makeSingleNodeConfig(badDataDir);

    // start() must throw (cannot create dataDir because it exists as a file).
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error)
        << "start() must throw when dataDir cannot be created";

    // stop() and dtor must not hang or crash after a failed start().
    using sclock = std::chrono::steady_clock;
    const auto before = sclock::now();
    node.stop(); // explicit stop after failed start — must be clean
    const auto elapsed = sclock::now() - before;

    EXPECT_LT(elapsed, 5s)
        << "stop() after failed start() must return promptly (no hang)";
    // Dtor runs here — also must not hang.
}

// ===========================================================================
// TEST 9: Dtor after explicit stop — no double-free / no hang (FIX F coverage).
//
// start() → awaitLeader() → stop() → dtor
// The dtor calls stop() again internally (via ~BrokerNode).  That second call
// must be a clean no-op (idempotent stop() guard), not a double-free or hang.
// ===========================================================================
TEST(BrokerNodeTest, DtorAfterExplicitStop_NoCrash) {
    TempDir tmp{"t9_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    {
        BrokerNode node{cfg};
        node.start();

        ASSERT_TRUE(node.awaitLeader(2s))
            << "1-node cluster should self-elect within 2 s";

        // Explicit stop: agent thread joins, Aeron and driver shut down.
        node.stop();

        // Dtor runs here — calls stop() again internally.
        // Must not double-free, hang, or crash.
    }
    SUCCEED(); // reaching here means dtor completed cleanly
}

// ===========================================================================
// TEST 11: leaderClientEndpoint() — single-node (leader=self → empty).
//
// On a single-node cluster, the node self-elects: isLeader()==true.
// leaderClientEndpoint() must return empty (no self-redirect).
// ===========================================================================
TEST(BrokerNodeTest, LeaderClientEndpoint_SingleNode_LeaderSelf_ReturnsEmpty) {
    TempDir tmp{"t11_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    // Give the self-peer a clientPort so we can verify the empty-on-leader rule.
    cfg.peers[0].clientPort = 50001;  // self has known client endpoint

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(2s))
        << "1-node cluster should self-elect within 2 s";
    ASSERT_TRUE(node.isLeader());

    // Leader → no self-redirect.
    EXPECT_TRUE(node.leaderClientEndpoint().empty())
        << "leaderClientEndpoint() must be empty when this node IS the leader";

    node.stop();
}

// ===========================================================================
// TEST 12: leaderClientEndpoint() — before start (no leader known → empty).
// ===========================================================================
TEST(BrokerNodeTest, LeaderClientEndpoint_BeforeStart_ReturnsEmpty) {
    TempDir tmp{"t12_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    // Not started → no election → leaderId() == -1 → empty endpoint.
    EXPECT_TRUE(node.leaderClientEndpoint().empty())
        << "leaderClientEndpoint() must be empty before start()";
}

// ===========================================================================
// TEST 10: NodeConsensusAgent leader-id state-machine mapping (FIX G).
//
// Table-driven coverage of the updateObservedLeader() switch in
// NodeConsensusAgent for the states reachable in a single-node run:
//
//   Before election starts (observedLeaderId_ seeded to -1):
//     - leaderId() == -1   (Init state → default/no-leader arm)
//
//   After self-election (LeaderReady / Closed):
//     - leaderId() == nodeId_   (Leader* arm sets nodeId_ explicitly — FIX B)
//
// FIX B correctness: before FIX B the Leader* arm used leaderMemberId() which
// happened to equal nodeId_ for single-node.  After FIX B the arm explicitly
// uses nodeId_, making it safe if a future Election refactor leaves leaderMember_
// stale.  This test asserts the post-FIX-B observable: leaderId() == cfg.nodeId.
// ===========================================================================
TEST(BrokerNodeTest, NodeConsensusAgentLeaderMap_InitAndLeader) {
    TempDir tmp{"t10_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};

    // Before start: no election running, leaderId() must be -1.
    // (BrokerNode.leaderId() reads the atomic mirror which was seeded to -1.)
    EXPECT_EQ(node.leaderId(), -1)
        << "Before start(): leaderId() must be -1 (Init state / no leader)";

    // Start the node and wait for self-election.
    node.start();
    ASSERT_TRUE(node.awaitLeader(2s))
        << "1-node cluster should self-elect within 2 s";

    // After LeaderReady is reached: leaderId() must equal this node's id
    // (FIX B: Leader* arm uses nodeId_, not leaderMemberId()).
    EXPECT_EQ(node.leaderId(), static_cast<rune::consensus::NodeId>(cfg.nodeId))
        << "After leadership: leaderId() must equal nodeId (FIX B — explicit nodeId_ in Leader* arm)";

    // Sanity: isLeader() agrees.
    EXPECT_TRUE(node.isLeader())
        << "isLeader() must be true when leaderId() == nodeId";

    node.stop();
}

} // namespace rune::node
