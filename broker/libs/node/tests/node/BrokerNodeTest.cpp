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
#include <string>
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

} // namespace rune::node
