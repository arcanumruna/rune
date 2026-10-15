// BrokerNodeTest.cpp — Integration tests for BrokerNode (BR-2).
//
// Tests:
//   1. Single-node self-elects within 2 s.
//   2. Clean shutdown: stop() twice is safe (idempotent).
//   3. Dtor-after-start: no hang or crash.
//   4. Data dir created on start(); node-state.dat and term.log exist.
//   5. Agent idles (no busy-spin): after leadership, awaitLeader() is immediate
//      and stop() returns promptly (bounded 3 s).
//   (CP-6 smoke) LogCleanerAgent_CompactsViaBrokerNode: a real BrokerNode with
//      a compact topic compacts records via the LogCleanerAgent thread.
//
// All waits are bounded with hard deadlines + ASSERT.
// Unique temp paths use getpid() + atomic counter.

#include <rune/aeronutil/testing/FreePort.h>
#include "../support/TestNodeConfig.h"
#include <rune/node/BrokerNode.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/HostDefaults.h>
#include <rune/node/Worker.h>
#include "../support/OwnerThreadTestHelpers.h"  // F1: owner-thread replica probes
#include <utility>
#include <rune/node/NodeConfig.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include "support/TestWorkerCount.h"
#include "support/ShardTestClient.h"
#include "support/TestCluster.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <stdexcept>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator  (getpid + atomic counter)
// ---------------------------------------------------------------------------
// F3 round 6, item 7: WorkerOp ring framing computed from the GENERATED codec
// constants (never a literal) — it moves whenever rune-protocol grows WorkerOp.
static constexpr std::size_t kWorkerOpFramingFromCodec =
    rune::protocol::internal::sbe::MessageHeader::encodedLength() +
    rune::protocol::internal::sbe::WorkerOp::sbeBlockLength() +
    rune::protocol::internal::sbe::WorkerOp::tailHeaderLength();

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
    rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();   // D8: never a fixed port
    cfg.peers          = {self};

    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;

    cfg.workerCount = rune::node::test::workerCountFromEnv();

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

// ---------------------------------------------------------------------------
// Build the on-disk partition log directory used by BrokerNode.
// Path: dataDir/topics/<name>/<partition>/  (mirrors TopicRegistry logic)
// Must be called BEFORE node.start() so BrokerNode recovers pre-written data.
// ---------------------------------------------------------------------------
static fs::path makePartitionDir(const fs::path& dataDir,
                                  const std::string& topicName,
                                  int32_t partition = 0) {
    fs::path d = dataDir / "topics" / topicName / std::to_string(partition);
    std::error_code ec;
    fs::create_directories(d, ec);
    return d;
}

// ---------------------------------------------------------------------------
// Count live records across all segments of a Log (post-stop, single reader).
// ---------------------------------------------------------------------------
static int countLiveRecords(rune::storage::Log& log) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size())
                         ? bases[si + 1]
                         : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
                ++count; cursor = off + 1; advanced = true;
            });
            if (!advanced || cursor >= segEnd) break;
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Poll pred() until it returns true or the deadline is reached.
// ---------------------------------------------------------------------------
template <typename Pred>
static bool waitFor(Pred&& pred, std::chrono::steady_clock::duration timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return true;
}

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

// Boot-level proof that every driver/client site honours aeronDir (the parse-level
// NodeConfigRegression tests can't see BrokerNode's call sites).
TEST(BrokerNodeRegression, AeronDirOverrideIsWhereTheDriverRuns) {
    TempDir data{"aerondir_data_"};
    TempDir aeron{"aerondir_aeron_"};
    NodeConfig cfg = makeSingleNodeConfig(data.path);
    cfg.aeronDir = (aeron.path / "a").string();

    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(2s));
    EXPECT_TRUE(fs::exists(fs::path{cfg.aeronDir} / "cnc.dat"))
        << "driver must run in the configured RUNE_AERON_DIR";
    EXPECT_FALSE(fs::exists(data.path / "aeron" / "cnc.dat"))
        << "no driver may fall back to dataDir/aeron when aeronDir is set";
    node.stop();
}

// An Aeron dir equal to the data dir would be recursively deleted by the driver
// (dir_delete_on_start) — start() must refuse before any side effect.
TEST(BrokerNodeRegression, AeronDirEqualToDataDirRefusedAtStartLogSurvives) {
    TempDir data{"aerondir_same_"};
    fs::create_directories(data.path);
    const auto sentinel = data.path / "00000000000000000000.log";
    { std::ofstream(sentinel) << "segment"; }

    NodeConfig cfg = makeSingleNodeConfig(data.path);
    cfg.aeronDir = data.path.string();
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_TRUE(fs::exists(sentinel)) << "refusal must happen before the driver deletes anything";
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

// ===========================================================================
// CP-6 Smoke: LogCleanerAgent compacts via a real BrokerNode.
//
// Verifies that a real BrokerNode with a compact topic actually compacts data
// via the dedicated LogCleanerAgent thread.  This is the production-wiring
// smoke check for CP-6/Issue 4: LogCleanerAgent constructed in BrokerNode
// and running on its own AgentRunner.
//
// TSan-clean single-writer design:
//   Phase A (pre-start): all Log::append() calls happen via a STANDALONE Log
//     before node.start().  No duty thread is running yet — single writer.
//   Phase B (post-start): test thread does NOT touch Log or PartitionReplica.
//     becomeLeader() on the duty thread advances HW; the cleaner runs.
//   Phase C (post-stop): race-free verification via a fresh Log after stop().
// ===========================================================================
TEST(BrokerNodeTest, LogCleanerAgent_CompactsViaBrokerNode) {
    TempDir tmp{"cp6_smoke_"};

    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    // Add one compact topic with a SMALL segment roll so a modest number of
    // appends forms multiple sealed segments.
    TopicSpec ts;
    ts.name                  = "smoke-compact";
    ts.partitionCount        = 1;
    ts.topicId               = 0;
    ts.cleanupPolicy         = TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;    // compact immediately (no threshold)
    ts.deleteRetentionMs     = 86'400'000LL;
    ts.segmentRollBytes      = 128;     // tiny roll → many sealed segments fast
    ASSERT_NO_FATAL_FAILURE(rune::node::test::defineTopicsThenStop(cfg, {ts}));

    // -----------------------------------------------------------------------
    // Phase A (pre-start): write all data via a STANDALONE Log.
    // No BrokerNode / duty-cycle thread running → single-writer, TSan-clean.
    // -----------------------------------------------------------------------
    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    constexpr int kRecords = 300;
    int before = 0;
    {
        rune::storage::Log rawLog(logDir, 128);
        for (int v = 0; v < kRecords; ++v) {
            rune::protocol::RecordBatchBuilder b(v, /*ts=*/1000);
            std::string val = "v" + std::to_string(v);
            b.append(std::string_view("hot"), std::string_view(val), 1000);
            auto batch = b.build();
            rawLog.append(batch, v);
        }
        auto bases = rawLog.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u)
            << "Small roll must form multiple sealed segments; got " << bases.size();
        before = countLiveRecords(rawLog);
        ASSERT_GT(before, 0);
    } // rawLog destroyed (closed); BrokerNode recovers these files.

    // -----------------------------------------------------------------------
    // Phase B (post-start): start BrokerNode.  The duty thread calls
    // becomeLeader() which advances HW to LEO — no test-thread replica calls
    // needed.  The LogCleanerAgent runs on its own thread.
    // -----------------------------------------------------------------------
    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(3s))
        << "CP-6 smoke: node must self-elect within 3 s";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "CP-6 smoke: replication bridge must propagate leadership within 2 s";
    // HW is now at LEO (set by becomeLeader() on the duty thread).
    // Test thread does NOT touch Log or PartitionReplica while node is live
    // (duty-thread-only invariant → TSan-clean).

    // Give the cleaner agent time to compact + duty cycle to drain swaps.
    std::this_thread::sleep_for(2s);

    // Stop the node — quiesces the cleaner + duty-cycle threads.
    using sclock = std::chrono::steady_clock;
    const auto beforeStop = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - beforeStop, 5s)
        << "BrokerNode stop() must complete within 5 s with cleaner agent running";

    // -----------------------------------------------------------------------
    // Phase C (post-stop): race-free on-disk verification; single reader.
    // -----------------------------------------------------------------------
    rune::storage::Log finalLog(logDir, 128);
    finalLog.recover();
    const int after = countLiveRecords(finalLog);
    EXPECT_LT(after, before)
        << "CP-6 smoke: LogCleanerAgent (running in the node) must compact the "
           "hot-key topic — compacted live count (" << after
        << ") must be < pre-compaction count (" << before << ")";
}

// ===========================================================================
// CP-8: CoalescingEndToEndViaBrokerNode
//
// Tests that segment coalescing, when enabled via NodeConfig, actually reduces
// the on-disk segment count through a real BrokerNode.
//
// Setup (coalescing ON):
//   - Single-node BrokerNode with enableCompactionCoalescing=true.
//   - One compact topic with segmentRollBytes=128 (very small) so many sealed
//     segments form from a modest number of appends.
//   - Produce records with many repeated keys to ensure high dirty ratio.
//   - Wait for the cleaner to run; stop the node; verify on disk:
//       * Segment count is STRICTLY LESS than before compaction (coalescing reduced it).
//       * Latest-per-key is preserved (each key's last value survives).
//       * Offsets are preserved (no renumbering).
//
// Non-vacuous (coalescing OFF):
//   - Same setup but enableCompactionCoalescing=false.
//   - After the same wait, segment count does NOT drop (individual-segment
//     compaction does not reduce count; it rewrites segments in place).
//   The coalescing-off arm confirms the assertion is driven by coalescing, not
//   by compaction removing all superseded records (which also happens in OFF
//   mode but does not merge segments).
// ===========================================================================
TEST(BrokerNodeTest, CoalescingEndToEndViaBrokerNode) {
    // -------------------------------------------------------------------------
    // Helper: count on-disk segment files in a log directory.
    // Each sealed segment is a <base>.log file (no ".new" suffix).
    // -------------------------------------------------------------------------
    auto countSegmentFiles = [](const std::string& dir) -> int {
        int count = 0;
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) continue;
            auto name = entry.path().filename().string();
            if (name.size() > 4 &&
                name.substr(name.size() - 4) == ".log" &&
                name.substr(name.size() - 8) != ".log.new") {
                ++count;
            }
        }
        return count;
    };

    // -------------------------------------------------------------------------
    // Helper: build a NodeConfig for the test with a compact topic and a
    // configurable coalescing flag.
    // -------------------------------------------------------------------------
    TopicSpec coalesceTopic;   // set by makeCoalescingConfig; defined through the catalog by each arm
    auto makeCoalescingConfig = [&coalesceTopic](const fs::path& dataDir, bool coalesce) -> NodeConfig {
        NodeConfig cfg;
        rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
        cfg.nodeId            = 0;
        cfg.dataDir           = dataDir.string();
        cfg.consensusStreamId = 10;

        PeerConfig self;
        self.id            = 0;
        self.host          = "127.0.0.1";
        self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();   // D8: never a fixed port
        cfg.peers          = {self};

        cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        cfg.clientEndpoint.streamId = 1;

        TopicSpec ts;
        ts.name                   = "coalesce-test";
        ts.partitionCount         = 1;
        ts.topicId                = 0;
        ts.cleanupPolicy          = TopicSpec::CleanupPolicy::Compact;
        ts.minCleanableDirtyRatio = 0.0;
        ts.deleteRetentionMs      = 86'400'000LL;
        ts.segmentRollBytes       = 128;

        coalesceTopic = ts;
        cfg.enableCompactionCoalescing = coalesce;
        cfg.workerCount = rune::node::test::workerCountFromEnv();
        return cfg;
    };

    // Helper: scan latest-per-key from a (post-stop) Log.
    auto scanLatestPerKey = [](rune::storage::Log& l) {
        std::unordered_map<std::string, std::string> latest;
        auto bases = l.segmentBaseOffsets();
        if (bases.empty()) return latest;
        int64_t cursor = 0;
        for (size_t si = 0; si < bases.size(); ++si) {
            int64_t segEnd = (si + 1 < bases.size())
                ? bases[si + 1] : std::numeric_limits<int64_t>::max();
            if (cursor < bases[si]) cursor = bases[si];
            for (;;) {
                auto bytes = l.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                    latest[std::string(k)] = std::string(v);
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }
        return latest;
    };

    // =========================================================================
    // Arm A: coalescing ENABLED — segment count must DROP.
    //
    // TSan-clean single-writer design:
    //   Phase A (pre-start): all Log::append() via standalone Log; no duty thread.
    //   Phase B (post-start): test thread does NOT touch Log or PartitionReplica;
    //     becomeLeader() on duty thread advances HW; cleaner coalesces.
    //   Phase C (post-stop): race-free verification with a fresh Log.
    // =========================================================================
    {
        TempDir tmp{"cp8_coal_on_"};
        NodeConfig cfg = makeCoalescingConfig(tmp.path, /*coalesce=*/true);
        ASSERT_NO_FATAL_FAILURE(rune::node::test::defineTopicsThenStop(cfg, {coalesceTopic}));

        // --- Phase A (pre-start) ---
        const fs::path partDir = makePartitionDir(tmp.path, "coalesce-test");
        const std::string logDir = partDir.string();

        constexpr int kRecords = 400;
        int segCountBefore = 0;
        std::unordered_map<std::string, std::string> expectedLatest;
        {
            rune::storage::Log rawLog(logDir, 128);
            for (int v = 0; v < kRecords; ++v) {
                std::string key = "key" + std::to_string(v % 4);
                rune::protocol::RecordBatchBuilder b(v, /*ts=*/1000);
                b.append(std::string_view(key),
                         std::string_view("v" + std::to_string(v)), 1000);
                auto batch = b.build();
                rawLog.append(batch, v);
            }
            const auto bases = rawLog.segmentBaseOffsets();
            ASSERT_GE(bases.size(), 3u)
                << "Need >= 3 sealed segments to test coalescing; got "
                << bases.size();
            segCountBefore = static_cast<int>(bases.size());
        } // rawLog closed; BrokerNode will recover.

        // Expected latest: last write for each key.
        for (int v = kRecords - 4; v < kRecords; ++v) {
            expectedLatest["key" + std::to_string(v % 4)] = "v" + std::to_string(v);
        }

        // --- Phase B (post-start) ---
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(3s))
            << "CP-8 coalescing (on): node must self-elect within 3 s";
        ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
            << "CP-8 (coalescing on): replication bridge must propagate within 2 s";
        // HW advanced to LEO by becomeLeader() on the duty thread.
        // Test thread does NOT touch Log or PartitionReplica (TSan-clean).

        std::this_thread::sleep_for(2s);

        using sclock = std::chrono::steady_clock;
        const auto t0 = sclock::now();
        node.stop();
        EXPECT_LT(sclock::now() - t0, 5s)
            << "CP-8 (coalescing on): stop() must complete within 5 s";

        // --- Phase C (post-stop) ---
        const int onDiskAfter = countSegmentFiles(logDir);

        rune::storage::Log finalLog(logDir, 128);
        finalLog.recover();
        const int segCountAfter = static_cast<int>(finalLog.segmentBaseOffsets().size());

        EXPECT_LT(segCountAfter, segCountBefore)
            << "CP-8 (coalescing ON): segment count must DROP after coalescing; "
               "before=" << segCountBefore << " after=" << segCountAfter;

        auto actualLatest = scanLatestPerKey(finalLog);
        for (const auto& [k, v] : expectedLatest) {
            EXPECT_EQ(actualLatest[k], v)
                << "CP-8 (coalescing ON): latest value for key='" << k
                << "' must be preserved; expected='" << v
                << "' got='" << actualLatest[k] << "'";
        }

        EXPECT_EQ(onDiskAfter, segCountAfter)
            << "CP-8 (coalescing ON): on-disk .log file count must match "
               "in-memory segment count";
    }

    // =========================================================================
    // Arm B: coalescing DISABLED — individual-segment compaction still CONVERGES.
    //
    // Individual-segment compaction rewrites one sealed segment per pass; once a
    // segment's records are all superseded it is emptied and dropped.  Over
    // enough passes the log CONVERGES to latest-per-key (one live record per key)
    // and the segment count collapses — same end state as coalescing ON, reached
    // one-segment-at-a-time instead of via merged groups.
    //
    // (The prior version asserted >= 90% of segments REMAIN with coalescing OFF.
    // That was only ever true because of a compaction-convergence watermark bug
    // that advanced the clean watermark past un-rewritten segments after a single
    // rewrite — so individual compaction never actually converged.  With the fix
    // the watermark advances only past the rewritten segment, so coalescing-OFF
    // converges too; the ON-vs-OFF distinction is the merge mechanism during
    // processing, not the final residual count.)
    // TSan-clean: same pre-start write pattern as arm A.
    // =========================================================================
    {
        TempDir tmp{"cp8_coal_off_"};
        NodeConfig cfg = makeCoalescingConfig(tmp.path, /*coalesce=*/false);
        ASSERT_NO_FATAL_FAILURE(rune::node::test::defineTopicsThenStop(cfg, {coalesceTopic}));

        // --- Phase A (pre-start) ---
        const fs::path partDir = makePartitionDir(tmp.path, "coalesce-test");
        const std::string logDir = partDir.string();

        constexpr int kRecords = 400;
        int segCountBefore = 0;
        std::unordered_map<std::string, std::string> expectedLatest;
        {
            rune::storage::Log rawLog(logDir, 128);
            for (int v = 0; v < kRecords; ++v) {
                std::string key = "key" + std::to_string(v % 4);
                rune::protocol::RecordBatchBuilder b(v, /*ts=*/1000);
                b.append(std::string_view(key),
                         std::string_view("v" + std::to_string(v)), 1000);
                auto batch = b.build();
                rawLog.append(batch, v);
                expectedLatest[key] = "v" + std::to_string(v);
            }
            const auto bases = rawLog.segmentBaseOffsets();
            ASSERT_GE(bases.size(), 3u)
                << "Need >= 3 sealed segments; got " << bases.size();
            segCountBefore = static_cast<int>(bases.size());
        } // rawLog closed; BrokerNode will recover.

        // --- Phase B (post-start) ---
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(3s))
            << "CP-8 coalescing (off): node must self-elect within 3 s";
        ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
            << "CP-8 (coalescing off): replication bridge must propagate within 2 s";

        std::this_thread::sleep_for(2s);

        using sclock = std::chrono::steady_clock;
        const auto t0 = sclock::now();
        node.stop();
        EXPECT_LT(sclock::now() - t0, 5s)
            << "CP-8 (coalescing off): stop() must complete within 5 s";

        // --- Phase C (post-stop) ---
        rune::storage::Log finalLog(logDir, 128);
        finalLog.recover();
        const int segCountAfter = static_cast<int>(finalLog.segmentBaseOffsets().size());

        // CONVERGENCE: coalescing-OFF individual compaction must ALSO converge —
        // segment count drops (superseded-empty segments dropped) and latest-per-
        // key is preserved.  Non-vacuous: this asserts real convergence, not the
        // bug-dependent "segments must remain" behavior.
        EXPECT_LT(segCountAfter, segCountBefore)
            << "CP-8 (coalescing OFF): individual compaction must still CONVERGE "
               "(segment count drops as superseded segments are emptied+dropped); "
               "before=" << segCountBefore << " after=" << segCountAfter;

        auto actualLatest = scanLatestPerKey(finalLog);
        for (const auto& [k, v] : expectedLatest) {
            EXPECT_EQ(actualLatest[k], v)
                << "CP-8 (coalescing OFF): latest value for key='" << k
                << "' must be preserved; expected='" << v
                << "' got='" << actualLatest[k] << "'";
        }
    }
}

// ===========================================================================
// CP-10: the cleaner's I/O throttle is a BROKER setting (NodeConfig::cleanerIoMaxBytesPerSecond,
// RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND) -- Task 17 fix round 1 (R2).  Arm A: the default is unlimited and a cleaner
// exists.  Arm B: a set value reaches the Throttler.  The cleaner is built in start().
// ===========================================================================
TEST(BrokerNodeTest, CP10_IoMaxBytesPerSecond_ReachesThrottler) {
    // Arm A: default config -> a cleaner exists and its throttle is unlimited.
    {
        TempDir tmp{"cp10_thr_a_"};
        fs::create_directories(tmp.path);
        NodeConfig cfg = makeSingleNodeConfig(tmp.path);
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10s));
        ASSERT_NE(node.cleanerThrottleRateForTest(), -2) << "a started broker must have a cleaner";
        EXPECT_EQ(node.cleanerThrottleRateForTest(), -1);
        node.stop();
    }
    // Arm B: a configured value reaches the Throttler.
    {
        TempDir tmp{"cp10_thr_b_"};
        fs::create_directories(tmp.path);
        NodeConfig cfg = makeSingleNodeConfig(tmp.path);
        cfg.cleanerIoMaxBytesPerSecond = 1 << 20;
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10s));
        EXPECT_EQ(node.cleanerThrottleRateForTest(), 1 << 20);
        node.stop();
    }
}

// ===========================================================================
// Task M1 precondition #1: durability=all-in-sync wires end-to-end.
//
// A topic declared with durability=1 (AllInSync) must yield a PartitionReplica
// whose durability() == DurabilityMode::AllInSync.  On a single-node cluster
// Majority and AllInSync are equivalent (only one node → ISR={self}), but the
// config field must be forwarded correctly from TopicSpec → PartitionConfig →
// PartitionReplica.
// ===========================================================================
TEST(BrokerNodeTest, M1_AllInSyncDurabilityWiresEndToEnd) {
    TempDir tmp{"m1_dur_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    TopicSpec ts;
    ts.name           = "wallet";
    ts.partitionCount = 1;
    ts.topicId        = 0;
    ts.durability     = 1; // AllInSync

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(2s)) << "single-node must self-elect within 2 s";
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);

    ASSERT_TRUE(rune::node::test::hasReplica(node)) << "replica must exist for topic 0 partition 0";

    // F1: read on the replica's OWNING thread (no raw cross-thread replica access).
    EXPECT_EQ(node.onOwnerThreadForTest(0, 0,
                  [](rune::replication::PartitionReplica& r) { return r.durability(); }),
              rune::replication::DurabilityMode::AllInSync)
        << "Task M1 precondition #1: durability=1 in TopicSpec must produce AllInSync "
           "PartitionReplica — durability field was not threaded through to PartitionConfig";

    node.stop();
}

// ===========================================================================
// Task M1 precondition #2: memory-backed partition skips cleaner / log().
//
// A topic declared with persistence=Transient uses an InMemoryLog.
// The LogCleanerAgent must not call replica->log() (which asserts isDiskBacked())
// on such a replica.  The test verifies this by:
//   1. Creating a compact memory-backed topic (normally the cleaner would run).
//   2. Starting the node and waiting for leadership + cleaner time.
//   3. No crash (no assert failure) — proves the isDiskBacked() guard works.
//
// Also verifies ilog()->isDiskBacked() == false for the memory replica.
// ===========================================================================
TEST(BrokerNodeTest, M1_TransientPartitionSkipsCleaner) {
    TempDir tmp{"m1_mem_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    TopicSpec ts;
    ts.name                  = "presence";
    ts.partitionCount        = 1;
    ts.topicId               = 0;
    ts.cleanupPolicy         = TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;  // would fire immediately on disk topics
    ts.persistence           = PersistenceMode::Transient;

    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(2s)) << "single-node must self-elect within 2 s";
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "replication bridge must propagate leadership";

    // Give the cleaner time to run (it must NOT crash on the memory-backed replica).
    std::this_thread::sleep_for(std::chrono::milliseconds{500});

    ASSERT_TRUE(rune::node::test::hasReplica(node)) << "replica must exist for topic 0 partition 0";

    // Verify the log is memory-backed (de-virtualized seam: isMemoryBacked()).
    // F1: read on the replica's OWNING thread.
    const auto [memBacked, diskBacked] = node.onOwnerThreadForTest(0, 0,
        [](rune::replication::PartitionReplica& r) {
            return std::pair{r.isMemoryBacked(), r.isDiskBacked()};
        });
    EXPECT_TRUE(memBacked)
        << "Task M1: persistence=Transient must produce a memory-backed replica (InMemoryLog)";
    EXPECT_FALSE(diskBacked)
        << "Task M1: persistence=Transient replica must not be disk-backed";

    // If we reach here without a crash/assertion, the cleaner correctly skipped
    // the memory-backed replica's log() call.
    node.stop();
}

// ===========================================================================
// E5.2: DEDICATED threading-mode smoke — driver must boot + node must elect
// ===========================================================================
// Prove that AERON_THREADING_MODE_DEDICATED (3 driver threads) actually boots
// the embedded media driver and that the broker self-elects.
// Does NOT require a round-trip produce/consume — election implies Aeron
// subscriptions and publications are live (consensus channel is up).
TEST(BrokerNodeTest, DedicatedAeronThreadingMode_BootsAndElects) {
    TempDir tmp{"ded_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.aeronThreadingMode = NodeConfig::AeronThreadingMode::Dedicated;
    // Keep default driverIdleStrategy ("sleep-ns") — lightweight even in DEDICATED mode.

    BrokerNode node{cfg};
    // Must not throw — aeron_driver_init must accept AERON_THREADING_MODE_DEDICATED.
    ASSERT_NO_THROW(node.start())
        << "BrokerNode::start() must not throw with AeronThreadingMode::Dedicated";

    // Driver is up and the consensus channel is live iff the node can elect.
    ASSERT_TRUE(node.awaitLeader(3s))
        << "DEDICATED-mode node must self-elect within 3 s (Aeron driver + "
           "consensus channel must both be live)";

    EXPECT_TRUE(node.isLeader());

    node.stop();
}

// ---------------------------------------------------------------------------
// read-rework T3/T4 I1b: BrokerNode wires one dynamic-MDC WorkerEgress per worker
// when per-worker ingress is enabled.  Asserts count == workerCount, each resolved,
// and stream ids are the contiguous range [egressBase, egressBase+W-1] with
// egressBase = workerIngressBaseStreamId + workerCount (non-overlapping).
// ---------------------------------------------------------------------------
TEST(BrokerNodeTest, PerWorkerEgressWiredWithContiguousStreams) {
    TempDir tmp{"egress_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.workerCount             = 2;
    cfg.perWorkerEgressEnabled  = true;   // read-rework: gates egress creation
    // Distinct control ports/streams to avoid collision with other tests' endpoints.
    cfg.workerEgressControlHost     = "127.0.0.1";
    cfg.workerEgressControlBasePort = 40360;
    // workerIngressBaseStreamId resolves to clientStreamId+1 = 2; egressBase auto =
    // ingressBase + workerCount = 2 + 2 = 4.
    cfg.workerIngressBaseStreamId   = 2;

    TopicSpec ts;
    ts.name           = "egress-topic";
    ts.partitionCount = 4;      // spread across the 2 workers
    ts.topicId        = 0;

    BrokerNode node{cfg};
    ASSERT_NO_THROW(node.start());
    ASSERT_TRUE(node.awaitLeader(3s));
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);

    const auto& egresses = node.workerEgressesForTest();
    ASSERT_EQ(egresses.size(), 2u) << "one WorkerEgress per worker";

    const int32_t egressBase = cfg.workerIngressBaseStreamId + cfg.workerCount; // 4
    for (int32_t i = 0; i < cfg.workerCount; ++i) {
        ASSERT_NE(egresses[i], nullptr);
        EXPECT_TRUE(egresses[i]->resolved())
            << "worker " << i << " egress must resolve on the embedded UDP driver";
        EXPECT_EQ(egresses[i]->streamId(), egressBase + i)
            << "worker " << i << " egress stream id must be contiguous from egressBase";
        EXPECT_EQ(egresses[i]->controlEndpoint(),
                  std::string("127.0.0.1:") + std::to_string(40360 + i))
            << "worker " << i << " control endpoint must be host:(basePort+workerId)";
    }

    node.stop();
}

// F15 S3 (design note §6): a tryClaim exception now HALTS, so the return ring
// size is a correctness setting — its max message must hold one 512 KiB
// PushReadResult chunk plus its header.  A smaller ring refuses to start,
// before any side effect.
TEST(ReturnOverflowRegression, StartupRefusesReturnRingBelowMaxPushChunk) {
    EXPECT_TRUE(Worker::returnRingCoversMaxPushChunk(32u << 20)) << "the default must pass";
    EXPECT_TRUE(Worker::returnRingCoversMaxPushChunk(8u << 20));
    EXPECT_FALSE(Worker::returnRingCoversMaxPushChunk(4u << 20)) << "4 MiB ring: max message 512 KiB < chunk + header";
    const fs::path dir = makeTempDataDir("retring_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    cfg.workerReturnRingBytes = 1 << 20;
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
    std::error_code ec; fs::remove_all(dir, ec);
}

// F15 S3 fix round 1 (review I-1): the return ring must also hold the largest
// batch any intake path can store (a PushRead of one whole batch that does not
// fit would halt on every restart: a crash loop).  Legacy: min(gateway max
// produce batch, ingress max message); shard: min(max produce batch, 16 MiB,
// term / 8).  Plus ret ring >= ingress ring, and the owed-return cap in range.
TEST(ReturnOverflowRegression, StartupRefusesReturnRingBelowMaxStorableBatch) {
    constexpr std::size_t MiB = 1u << 20;
    EXPECT_FALSE(Worker::validateRingConfig(32 * MiB, 32 * MiB, 4096).has_value())
        << "the defaults must pass";
    EXPECT_TRUE(Worker::validateRingConfig(8 * MiB, 32 * MiB, 4096).has_value())
        << "Legacy: an 8 MiB return ring cannot hold a 2-4 MiB batch the 32 MiB ingress ring accepts";
    EXPECT_TRUE(Worker::validateRingConfig(16 * MiB, 32 * MiB, 4096).has_value())
        << "return ring smaller than the ingress ring";
    // Re-review 1 I-3 / re-review 2 m-h: the Aeron term no longer enters the
    // startup check (every intake path refuses an oversize batch with
    // MessageTooLarge — ReturnOverflowRegression.OneBatchLimitOnEveryPath), so
    // the shipped configs' ring sizes start: cluster.sh / CLAUDE.md (32/32)
    // above, replbench.sh (64/64) here.
    EXPECT_FALSE(Worker::validateRingConfig(64 * MiB, 64 * MiB, 4096).has_value())
        << "replbench.sh (rings 64/64)";
    EXPECT_FALSE(Worker::validateRingConfig(128 * MiB, 32 * MiB, 4096).has_value());
    EXPECT_TRUE(Worker::validateRingConfig(32 * MiB, 32 * MiB, 9000).has_value())
        << "owed-return cap above the ceiling";
    EXPECT_TRUE(Worker::validateRingConfig(32 * MiB, 32 * MiB, 100).has_value())
        << "owed-return cap below the floor";
    EXPECT_FALSE(Worker::validateRingConfig(32 * MiB, 32 * MiB, 8192).has_value());
    {
        const fs::path dir = makeTempDataDir("retring8_");
        NodeConfig cfg = makeSingleNodeConfig(dir);
        cfg.workerReturnRingBytes = 8 << 20;   // ingress stays 32 MiB
        BrokerNode node{cfg};
        EXPECT_THROW(node.start(), std::runtime_error);
        EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
        std::error_code ec; fs::remove_all(dir, ec);
    }
    {
        const fs::path dir = makeTempDataDir("owedcap_");
        NodeConfig cfg = makeSingleNodeConfig(dir);
        cfg.workerMaxOwedReturns = 9000;
        BrokerNode node{cfg};
        EXPECT_THROW(node.start(), std::runtime_error);
        EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
        std::error_code ec; fs::remove_all(dir, ec);
    }
}

TEST(BrokerNodeRegression, ShardInvalidConfigRefusesStartBeforeAnySideEffect) {
    const fs::path dir = makeTempDataDir("shardbad_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    // Since 2026-10-02 unset per-worker ports are DERIVED (shard is the default),
    // so "no ports" is no longer invalid; an explicit out-of-range base still is
    // (65535 + worker 1 overflows the port space).
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.workerCount = 2;
    cfg.workerCoreBase = -1;
    cfg.workerIngressBasePort = 65535;
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// F3 round 4 (supersedes round 3's validateReplicationFetchCeiling tests,
// which tested a startup-refusal design the controller replaced): instead of
// refusing to start whenever the gateway's configured max batch exceeds a
// transport's hard maximum, derive the gateway's EFFECTIVE max produce-batch
// size from the actual transport limits and use THAT to reject oversize
// produce batches at the gateway (see ClientGatewayHotPathTest.cpp's
// OversizeProduceBatchRejectedNotAcceptedRegression for the gateway-side
// enforcement).  Startup now refuses ONLY when the operator EXPLICITLY
// configures a max batch that the transport can never carry.
// ---------------------------------------------------------------------------

// Unit test of the derivation itself (effectiveMaxProduceBatchBytes()):
//   1. Multi-node: effective = min(configured default, ingress hard max,
//      replication hard max) — whichever of the three is tightest wins.
//   2. An EXPLICIT override smaller than every hard max is honored exactly
//      (the min() picks the configured value itself).
//   3. Single-node EXCLUDES the replication term (round 3's exemption,
//      narrowed in round 4 to just the replication half of the min) — a tiny
//      replicationOutboundRingBytes must NOT affect a single-node result,
//      but a tiny ingress ring/term still must.
TEST(BrokerNodeRegression, EffectiveMaxBatchDerivedFromTransportRegression) {
    const fs::path dir = makeTempDataDir("effmaxderive_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    PeerConfig peer1;
    peer1.id = 1; peer1.host = "127.0.0.1"; peer1.consensusPort = 40114;
    cfg.peers.push_back(peer1);
    ASSERT_FALSE(cfg.isSingleNode());

    cfg.aeronTermBufferBytes         = 1 * 1024 * 1024; // term bound = 131072
    cfg.workerIngressRingBytes       = 8 * 1024 * 1024;  // ring bound = 1048576 -> ingress hard max = 131072
    cfg.replicationOutboundRingBytes = 2 * 1024 * 1024;  // ring bound = 262144 -> repl hard max = min(131072,262144) - framing

    const int32_t ingressMax = ingressHardTransportMaxBytes(cfg);
    const int32_t replMax    = replicationHardTransportMaxBytes(cfg);
    // F3 round 5: exact numbers (real framing constants, no hand /8 shortcut):
    //   worker ring tail  = 8 MiB/8 − WorkerOp framing (SBE header + block + tail
    //                       prefix, from the generated codec — 247 with rune-protocol fb82500)
    //   aeron ingress     = term max msg 131072 − Produce framing (24 hdr? + 24 + 2 + 4)
    //   replication       = min(ring 262144 − 16 − 46, term 131072 − 46) = 131026
    EXPECT_EQ(workerIngressMaxBatchBytes(cfg), 1048576u - kWorkerOpFramingFromCodec);
    EXPECT_EQ(ingressMax, aeronIngressMaxBatchBytes(cfg));
    EXPECT_LT(ingressMax, 131072);
    EXPECT_EQ(replMax, 131072 - static_cast<int32_t>(
                           rune::replication::FetchResponseMsg::encodedSize(0)));

    // (1) Default config (no explicit override): effective = min(8 MiB, ingressMax, replMax).
    // Here the Aeron-ingress Produce framing (54) exceeds the FetchResponse
    // framing (46), so ingress is the tightest by 8 bytes.
    EXPECT_EQ(effectiveMaxProduceBatchBytes(cfg),
              static_cast<std::size_t>(std::min(ingressMax, replMax)));
    EXPECT_EQ(ingressMax, 131072 - 54);

    // (2) Explicit override smaller than every hard max wins exactly.
    cfg.maxProduceBatchBytes = 1000;
    EXPECT_EQ(effectiveMaxProduceBatchBytes(cfg), 1000u);
    cfg.maxProduceBatchBytes.reset();

    // (3) Single-node — integration wave 2 (F18 ⇄ S3 merge plan, binding):
    // the replication terms apply ALWAYS, so the limit does not change when a
    // node later joins a cluster (F3 round 4's single-node exemption is
    // overruled).  The same sizes give the same limit single- and multi-node.
    NodeConfig singleCfg = makeSingleNodeConfig(dir);
    ASSERT_TRUE(singleCfg.isSingleNode());
    singleCfg.aeronTermBufferBytes         = cfg.aeronTermBufferBytes;
    singleCfg.workerIngressRingBytes       = cfg.workerIngressRingBytes;
    singleCfg.workerReturnRingBytes        = cfg.workerReturnRingBytes;
    singleCfg.replicationOutboundRingBytes = cfg.replicationOutboundRingBytes;
    EXPECT_EQ(effectiveMaxProduceBatchBytes(singleCfg), effectiveMaxProduceBatchBytes(cfg))
        << "single-node applies the replication terms too";
    singleCfg.replicationOutboundRingBytes = 64 * 1024;   // tiny ring: it binds single-node too
    EXPECT_LT(effectiveMaxProduceBatchBytes(singleCfg), static_cast<std::size_t>(ingressMax));

    std::error_code ec; fs::remove_all(dir, ec);
}

// End-to-end: BrokerNode::start() refuses, before any side effect, ONLY when
// an EXPLICIT max-produce-batch override exceeds the hard transport maximum.
// Uses a SINGLE-node config deliberately (round 3's exemption no longer
// applies wholesale — see point 3 of the round-4 ruling): the ingress limit
// alone is enough to trigger the refusal, proving single-node is NOT exempt
// from this check in general, only from the replication half of it.
TEST(BrokerNodeRegression, ExplicitOversizeMaxBatchConfigRefusedRegression) {
    const fs::path dir = makeTempDataDir("explicitoversize_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    ASSERT_TRUE(cfg.isSingleNode());
    cfg.aeronTermBufferBytes   = 64 * 1024; // term bound = 8192
    cfg.workerIngressRingBytes = 64 * 1024; // ring bound = 8192 -> ingress hard max = 8192
    cfg.maxProduceBatchBytes   = 10 * 1024 * 1024;  // EXPLICIT 10 MiB >> hard max

    // Pure-function check first (naming the exact numbers).
    auto err = validateExplicitMaxProduceBatch(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find(std::to_string(10 * 1024 * 1024)), std::string::npos) << *err;
    EXPECT_NE(err->find(std::to_string(ingressHardTransportMaxBytes(cfg))), std::string::npos) << *err;

    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
    std::error_code ec; fs::remove_all(dir, ec);
}

// The default (unset) config must NEVER be refused, however small the hard
// transport maximum ends up — this is the behavior that makes all 19
// previously-failing multi-node integration tests pass again without any
// per-test config patch (F3 round 4, point 4 of the ruling).
TEST(BrokerNodeRegression, DefaultMaxBatchConfigNeverRefusedRegression) {
    NodeConfig cfg = makeSingleNodeConfig(makeTempDataDir("defaultnotrefused_"));
    PeerConfig peer1;
    peer1.id = 1; peer1.host = "127.0.0.1"; peer1.consensusPort = 40115;
    cfg.peers.push_back(peer1);
    cfg.aeronTermBufferBytes = 64 * 1024;            // tiny — would have refused under round 3
    cfg.replicationOutboundRingBytes = 64 * 1024;
    cfg.workerIngressRingBytes = 64 * 1024;
    ASSERT_FALSE(cfg.maxProduceBatchBytes.has_value()) << "test setup: must be the unset default";
    EXPECT_FALSE(validateExplicitMaxProduceBatch(cfg).has_value());
}


// ---------------------------------------------------------------------------
// F3 round 5, items C1 + M4: the derivation is the SAME function the
// transports enforce.  Pins the exact default numbers and checks them against
// REAL transport instances (one EmbeddedDriver, default 4 MiB term):
//   legacy (AeronPartitionTransport, 4 MiB outbound ring):
//     min(524288 − 16 envelope − 46 framing, 524288 − 46) = 524226
//   per-worker (WorkerReplicationTransport, gather send): 524288 − 46 = 524242
//   worker ingress ring (32 MiB): 4194304 − WorkerOp framing from the codec
//     (8 SBE header + sbeBlockLength + 4 tail prefix: 4194073 with rune-protocol
//     66bf9e6, 4194057 with fb82500's targetReplicaId/incarnation fields)
//   aeron ingress (4 MiB term):   524288 − Produce framing
// → effective (multi-node) = 524226 = the ENFORCED legacy ceiling (round 4
//   derived 524242, 16 bytes above what the legacy ring path can carry).
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, DerivedMaxMatchesRealTransportsRegression) {
    const fs::path dir = makeTempDataDir("derivedreal_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    PeerConfig peer1;
    peer1.id = 1; peer1.host = "127.0.0.1"; peer1.consensusPort = 40116;
    cfg.peers.push_back(peer1);
    ASSERT_FALSE(cfg.isSingleNode());
    // Pinned (the numbers below are for a 4 MiB term + 4 MiB outbound ring — the
    // pre-2026-10-02 defaults; the derivation-vs-transport property is size-free).
    cfg.aeronTermBufferBytes = 4 * 1024 * 1024;
    cfg.replicationOutboundRingBytes = 4 * 1024 * 1024;
    ASSERT_EQ(cfg.workerIngressRingBytes, 32 * 1024 * 1024);

    EXPECT_EQ(replicationHardTransportMaxBytes(cfg), 524226);
    EXPECT_EQ(workerIngressMaxBatchBytes(cfg), 4194304u - kWorkerOpFramingFromCodec);
    EXPECT_EQ(effectiveMaxProduceBatchBytes(cfg), 524226u);
    // F3 round 6, item 1: the multi-node fetch/push chunk is bounded by the
    // replication hard max too (a 1 MiB coalesced push over the 524226 B
    // ceiling was dropped → 20 ms re-serve stall).  Single-node keeps 1 MiB.
    EXPECT_EQ(userTopicMaxFetchBytes(cfg), 524226);
    {
        NodeConfig single = makeSingleNodeConfig(dir);
        EXPECT_EQ(userTopicMaxFetchBytes(single), 1 << 20);
    }

    fs::create_directories(dir);
    EmbeddedDriverConfig dcfg = EmbeddedDriverConfig::fromNode(cfg, (dir / "aeron").string());
    ASSERT_EQ(dcfg.termBufferBytes, cfg.aeronTermBufferBytes);
    EmbeddedDriver driver(dcfg, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(dcfg.dir);
    auto aeron = aeron::Aeron::connect(ctx);

    const std::string chanA = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(40117);
    const std::string chanB = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(40118);
    rune::replication::AeronPartitionTransport legacy(
        aeron, chanA, 900, /*selfId=*/0, cfg.replicationOutboundRingBytes);
    // As BrokerNode (F18): the publication bound, set before any worker runs.
    legacy.setPublicationMaxMessageBytes(rune::replication::aeronMaxMessageLength(cfg.aeronTermBufferBytes));
    WorkerReplicationTransport perWorker(aeron, 0, chanB, 901, {{1, chanA}}, 902);
    ASSERT_TRUE(perWorker.resolved());

    const std::size_t eff = effectiveMaxProduceBatchBytes(cfg);
    EXPECT_EQ(legacy.maxFetchResponseBytes(), 524226) << "derived == enforced (legacy ring path)";
    EXPECT_EQ(perWorker.maxFetchResponseBytes(), 524242) << "derived == enforced (gather path)";
    EXPECT_GE(static_cast<std::size_t>(legacy.maxFetchResponseBytes()), eff);
    EXPECT_GE(static_cast<std::size_t>(perWorker.maxFetchResponseBytes()), eff);
    EXPECT_EQ(perWorker.maxFetchResponseBytes(),
              WorkerReplicationTransport::maxFetchResponseBytesFor(cfg.aeronTermBufferBytes));

    Worker w(0, static_cast<std::size_t>(cfg.workerIngressRingBytes));
    EXPECT_EQ(w.maxOpTailBytes(), workerIngressMaxBatchBytes(cfg));
    EXPECT_GE(w.maxOpTailBytes(), eff);

    std::error_code ec; fs::remove_all(dir, ec);
}

// F3 round 5, item M3: an effective max too small to carry a single record
// (here: a 1 KiB worker ingress ring → 128 − WorkerOp framing → 0) must REFUSE to
// start with a clear error naming the numbers — never silently fall back to
// the 8 MiB default (round 4's gateway setter did exactly that for 0).
TEST(BrokerNodeRegression, EffectiveMaxBelowMinimumRefusesStartRegression) {
    const fs::path dir = makeTempDataDir("effmaxzero_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    cfg.workerIngressRingBytes = 1024;
    EXPECT_EQ(effectiveMaxProduceBatchBytes(cfg), 0u);
    auto err = validateEffectiveMaxProduceBatch(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("workerIngress=0"), std::string::npos) << *err;

    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";

    // Exactly one record-batch header + 1 byte is the minimum accepted.
    cfg.workerIngressRingBytes = 32 * 1024 * 1024;
    cfg.maxProduceBatchBytes   = static_cast<int64_t>(kMinSaneMaxProduceBatchBytes) - 1;
    EXPECT_TRUE(validateEffectiveMaxProduceBatch(cfg).has_value());
    cfg.maxProduceBatchBytes   = static_cast<int64_t>(kMinSaneMaxProduceBatchBytes);
    EXPECT_FALSE(validateEffectiveMaxProduceBatch(cfg).has_value());
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// F3 round 6, item 6: heterogeneous LEGACY config — the leader's transport can
// carry a batch the follower's worker ingress ring can never hold.  The
// follower's AeronPartitionTransport drop path (FetchResponse → fitsIngressTail)
// must log (rate-limited) and stay BOUNDED: one drop per leader serve (push,
// then one re-serve per 20 ms resend window), never a per-cycle spin and never
// an ABORT-retry loop that would wedge the subscription.  Simulated replica
// clock; the yield only waits for real Aeron IO.
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, HeterogeneousFollowerDropIsBoundedRegression) {
    const fs::path dir = makeTempDataDir("hetero_");
    fs::create_directories(dir);
    NodeConfig ncfg = makeSingleNodeConfig(dir);
    ncfg.aeronTermBufferBytes = 1 << 20;                 // leader ceiling ~131 KB
    EmbeddedDriverConfig dcfg = EmbeddedDriverConfig::fromNode(ncfg, (dir / "aeron").string());
    EmbeddedDriver driver(dcfg, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(dcfg.dir);
    auto aeron = aeron::Aeron::connect(ctx);

    const std::string chanL = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());   // D11: allocator, not fixed
    const std::string chanF = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    rune::replication::AeronPartitionTransport ldrT(aeron, chanL, 910, 1, 4 << 20);
    rune::replication::AeronPartitionTransport flwT(aeron, chanF, 910, 2, 4 << 20);
    ldrT.setPublicationMaxMessageBytes(rune::replication::aeronMaxMessageLength(ncfg.aeronTermBufferBytes));
    flwT.setPublicationMaxMessageBytes(rune::replication::aeronMaxMessageLength(ncfg.aeronTermBufferBytes));
    ldrT.addPeer(2, chanF, 910);
    flwT.addPeer(1, chanL, 910);

    rune::replication::PartitionConfig pc;
    pc.topicId = 0; pc.partitionId = 0; pc.minIsr = 1;
    pc.replicaLagMaxMs = 1'000'000; pc.replicaLagMaxOffset = 1'000'000;
    pc.fetchIntervalNs = 5'000'000LL; pc.isrCheckIntervalNs = 10'000'000LL;
    pc.maxFetchBytes = 1 << 20; pc.pushOnAppend = true;
    auto lc = pc; lc.selfId = 1;
    auto fc = pc; fc.selfId = 2;
    fs::create_directories(dir / "l"); fs::create_directories(dir / "f");
    rune::storage::Log logL((dir / "l").string());
    rune::storage::Log logF((dir / "f").string());
    rune::replication::PartitionReplica leader(lc, rune::replication::Role::Leader, 1, {1, 2}, {1, 2}, &logL, &ldrT);
    rune::replication::PartitionReplica follower(fc, rune::replication::Role::Follower, 1, {1, 2}, {1, 2}, &logF, &flwT);
    follower.becomeFollower(1, 1);
    ldrT.registerReplica(0, 0, &leader);
    flwT.registerReplica(0, 0, &follower);

    // The follower's worker ring (64 KiB → ~7.9 KB max tail) is SMALLER than
    // what the leader's transport carries — the heterogeneous config.
    rune::metadata::ShardRouter router(1);
    WorkerPool pool(1, router, /*ingressRingBytes=*/64 * 1024);
    pool.worker(0).setOwnedDirect({&follower});
    flwT.setWorkerPool(&pool);

    {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", std::string(20000, 'z'), 1000);   // fits the leader, never the follower ring
        logL.append(b.build(), 0);
    }
    leader.onLeaderLogAppended();

    int64_t sim = 1'000'000'000;
    auto cycle = [&](int64_t step) {
        sim += step;
        (void)ldrT.poll(sim);
        (void)flwT.poll(sim);
        (void)leader.doWork(sim);
        (void)pool.worker(0).doWork(sim);
        pool.worker(0).drainReturn([](const WorkerReturn&) {});
    };
    // Phase 1: wait (real IO) until the first oversize frame reached the follower.
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (flwT.unfittableApplyDropsForTest() == 0 && std::chrono::steady_clock::now() < deadline) {
        cycle(100'000);                                  // 100 µs of replica time
        std::this_thread::yield();
    }
    ASSERT_GE(flwT.unfittableApplyDropsForTest(), 1u) << "the oversize frame must be dropped, not applied";
    const uint64_t d1 = flwT.unfittableApplyDropsForTest();

    // Phase 2: 2000 cycles = 200 ms of replica time.  Drops are bounded by the
    // leader's serve cadence (20 ms resend window → ≤ ~10 re-serves), NOT by
    // the number of cycles (a spin would be ~2000).
    for (int i = 0; i < 2000; ++i) cycle(100'000);
    const uint64_t extra = flwT.unfittableApplyDropsForTest() - d1;
    EXPECT_LE(extra, 12u) << "drops must be bounded by the re-serve cadence, not the cycle count";
    EXPECT_EQ(follower.logEndOffset(), 0) << "the unfittable batch is never applied";

    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// F3 round 7, item 1: the REAL production SchemaUpdate path end to end on a
// live single-node BrokerNode (2 workers): a client DefineTopic(validate=true)
// → gateway → admin thread commits it to __catalog → the applier's schemaChanged
// effect (admin→control ring, kA2cSchemaUpdate) →
// control-thread hook type dispatch → schemaFanout_.pump → EVERY worker's
// ingress ring → each worker installs the Validator on ITS OWN thread.
// Then a non-conforming produce is rejected (SchemaValidationFailed) and a
// conforming one is accepted.  Every wait is condition-driven and bounded.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> charactersValue(bool conforming) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); };
    auto le64 = [&](uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); };
    le16(12); le16(1);                         // blockLength (int64 + int32), version
    le64(1);                                   // id
    le32(conforming ? 1u : 99u);               // enum type: 99 is NOT in {0,1,2}
    const std::string_view name = "Alice";
    le32(static_cast<uint32_t>(name.size()));
    b.insert(b.end(), name.begin(), name.end());
    return b;
}
std::vector<uint8_t> charactersBatch(bool conforming) {
    const auto v = charactersValue(conforming);
    rune::protocol::RecordBatchBuilder bb(0, 1000);
    bb.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 1000);
    return bb.build();
}
} // namespace

TEST(BrokerNodeRegression, DefineTopicValidatorReachesEveryWorkerEndToEndRegression) {
    const fs::path dir = makeTempDataDir("schema_e2e_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    cfg.workerCount = 2;
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));

    rune::node::test::ShardTestClient client(dir / "client-aeron");
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const int64_t sid = client.connect(gw, stream);
    ASSERT_GT(sid, 0);

    const auto desc = rune::codec::DescriptorBuilder("characters")
        .add("id",   rune::codec::PrimType::Int64)
        .add("type", rune::codec::PrimType::Enum, 1, false, {0, 1, 2})
        .add("name", rune::codec::PrimType::String)
        .build();
    const std::vector<uint8_t> blob = rune::codec::serialize(desc);
    constexpr uint32_t kParts = 8;              // spread partitions over both workers

    // F3 round 8: ONE overall deadline (well under ctest's 300 s), checked by
    // every wait; only the documented "settling" codes are retried (short
    // backoff); a missing response ends the test immediately.  The gateway
    // reaps a session idle for 10 s (kDefaultSessionTimeoutNs), so no single
    // client-silent phase may approach that — the validator wait is capped at 5 s.
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    const auto testDeadline = Clock::now() + 20s;
    auto remaining = [&] { return std::chrono::duration_cast<std::chrono::milliseconds>(testDeadline - Clock::now()); };
    constexpr uint16_t kDdlInternal = static_cast<uint16_t>(pc::ErrorCode::Internal);      // admin not leader yet
    constexpr uint16_t kDdlExists   = static_cast<uint16_t>(pc::ErrorCode::TopicExists);

    int32_t topicId = -1;
    bool    sawInternal = false;
    while (topicId < 0) {
        ASSERT_GT(remaining().count(), 0) << "DefineTopic did not commit within the test deadline";
        auto def = client.defineTopicUntil(gw, stream, sid, "characters", kParts, /*validate=*/true,
                                           blob, std::min(testDeadline, Clock::now() + 5s));
        ASSERT_TRUE(def.has_value()) << "no DefineTopicResponse — gateway/admin path did not answer";
        if (def->errorCode == 0) { topicId = def->topicId; break; }
        if (def->errorCode == kDdlExists && sawInternal) {
            // A retry after a reply we treated as failed: the DDL had actually
            // committed.  Treat as success ONLY if the topic really exists.
            ASSERT_GE(def->topicId, 0) << "TopicExists without a topic id";
            ASSERT_TRUE(rune::node::test::hasReplica(node, def->topicId, 0))
                << "TopicExists but the topic has no replica";
            topicId = def->topicId;
            break;
        }
        ASSERT_EQ(def->errorCode, kDdlInternal)
            << "DefineTopic failed with a non-retryable code " << def->errorCode;
        sawInternal = true;
        std::this_thread::sleep_for(20ms);        // admin leadership settling (short backoff)
    }
    ASSERT_GE(topicId, 0);

    // Condition-driven: EVERY worker has the validator.  Each probe runs ON the
    // owning worker's thread (owner-thread test task) — worker state is never
    // read cross-thread.  Partition p of the topic is owned by
    // ShardRouter(2).workerForPartition(topicId, p); cover every worker.
    rune::metadata::ShardRouter router(2);
    std::map<int32_t, int32_t> probePartitionForWorker;
    for (int32_t p = 0; p < static_cast<int32_t>(kParts); ++p)
        probePartitionForWorker.emplace(router.workerForPartition(topicId, p), p);
    ASSERT_EQ(probePartitionForWorker.size(), 2u) << "test setup: partitions must span both workers";
    {
        const auto validatorDeadline = std::min(testDeadline, Clock::now() + 5s);   // < session idle reap
        std::map<int32_t, bool> installed;
        auto allInstalled = [&] {
            for (auto [w, p] : probePartitionForWorker) {
                if (installed[w]) continue;
                if (!rune::node::test::hasReplica(node, topicId, p)) return false;
                installed[w] = node.onOwnerThreadForTest(topicId, p,
                    [&node, w = w, topicId](rune::replication::PartitionReplica&) {
                        return node.workerForTest(w)->hasValidatorForTest(topicId);
                    });
                if (!installed[w]) return false;
            }
            return true;
        };
        while (!allInstalled() && Clock::now() < validatorDeadline)
            std::this_thread::sleep_for(5ms);    // poll interval, not correctness
        // Fatal: the produce phase is meaningless without validators.
        for (auto [w, p] : probePartitionForWorker)
            ASSERT_TRUE(installed[w]) << "worker " << w << " never installed the validator";
    }

    // F30 fix round 1 (F3): a RUNTIME-created user replica (thread placement,
    // the default WorkerMode) gets the same worker-driven durability as a boot
    // one — external follower flush + deferred roll seals, never inline fsync.
    for (int32_t p = 0; p < static_cast<int32_t>(kParts); ++p) {
        const bool workerDriven = node.onOwnerThreadForTest(topicId, p,
            [](rune::replication::PartitionReplica& r) {
                return r.externalFollowerFlush() && r.isDiskBacked() && r.log()->deferSealSync();
            });
        EXPECT_TRUE(workerDriven) << "runtime topic partition " << p
                                  << ": must use the worker's external flush + deferred seals";
    }

    // Produce: non-conforming rejected, conforming accepted.  Retry ONLY
    // NotLeader (runtime-topic leadership settling) with a short backoff; a
    // missing response or the overall deadline fails at once.
    auto produceSettled = [&](const std::vector<uint8_t>& batch, int32_t partition) -> std::optional<int16_t> {
        while (remaining().count() > 0) {
            const int64_t corr = client.nextCorrelationId();   // never reuse a correlation id
            if (!client.produce(gw, stream, sid, corr, topicId, partition, 0, batch)) return std::nullopt;
            auto ack = client.awaitProduceResponseUntil(corr, std::min(testDeadline, Clock::now() + 2s));
            if (!ack) return std::nullopt;               // no response: do not retry
            if (ack->errorCode != static_cast<int16_t>(pc::ErrorCode::NotLeader)) return ack->errorCode;
            std::this_thread::sleep_for(20ms);
        }
        return std::nullopt;
    };
    for (auto [w, p] : probePartitionForWorker) {
        const auto bad = produceSettled(charactersBatch(false), p);
        ASSERT_TRUE(bad.has_value()) << "worker " << w << ": no ProduceResponse (or test deadline)";
        EXPECT_EQ(*bad, static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed))
            << "worker " << w << ": a non-conforming record must be rejected";
        const auto good = produceSettled(charactersBatch(true), p);
        ASSERT_TRUE(good.has_value()) << "worker " << w << ": no ProduceResponse (or test deadline)";
        EXPECT_EQ(*good, 0) << "worker " << w << ": a conforming record must be accepted";
    }

    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(BrokerNodeRegression, StartNeverPumpsNodeDriverFromCallerThread) {
    const fs::path dir = makeTempDataDir("drvrace_");
    BrokerNode node{makeSingleNodeConfig(dir)};
    node.start();
    ASSERT_NE(node.driverForTest(), nullptr);
    EXPECT_EQ(node.driverForTest()->callerPumpsWhileThreadRunning(), 0u);
    EXPECT_EQ(node.driverForTest()->foreignPumpAttempts(), 0u);
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

// ===========================================================================
// F1 REGRESSION: owner-thread test hook runs on the replica's TRUE owner.
//
// The old replicaForTest() raw accessor let tests append()/read LEO/HW from the
// TEST thread while a WORKER owned the replica (append pushes inline on the
// worker's single-writer ExclusivePublication → corrupted/lost replication
// frames under load).  onOwnerThreadForTest must execute:
//   - user partitions (WorkerPool present) → on the OWNING worker thread
//     (never the test thread, never the control thread; stable per partition;
//     distinct threads for partitions owned by distinct workers);
//   - internal partitions (topicId < 0)    → on the CONTROL thread;
// and fail loudly on misuse (not running, admin-owned __topic_schemas, missing
// replica for the reference form) while propagating the callable's exceptions.
// ===========================================================================
TEST(BrokerNodeTest, OwnerThreadHookRunsOnTrueOwnerThreadRegression) {
    using rune::replication::PartitionReplica;
    TempDir tmp{"f1_owner_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.workerCount = 2;

    TopicSpec ts;
    ts.name           = "f1";
    ts.partitionCount = 8;
    ts.topicId        = 0;

    BrokerNode node{cfg};
    auto threadOf = [](PartitionReplica&) { return std::this_thread::get_id(); };

    // Not started → no owner thread → loud failure (never a silent test-thread run).
    EXPECT_THROW(node.onOwnerThreadForTest(0, 0, threadOf), std::logic_error);

    node.start();
    ASSERT_TRUE(node.awaitLeader(2s));
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s));

    const auto testTid    = std::this_thread::get_id();
    const auto controlTid = node.onControlThreadForTest([] { return std::this_thread::get_id(); });
    EXPECT_NE(controlTid, testTid) << "control-thread task ran on the test thread";

    // User partitions → the owning WORKER thread.
    std::map<int32_t, std::thread::id> tidByWorker;
    for (int32_t p = 0; p < static_cast<int32_t>(ts.partitionCount); ++p) {
        const auto tid = node.onOwnerThreadForTest(0, p, threadOf);
        EXPECT_NE(tid, testTid)    << "partition " << p << " ran on the test thread";
        EXPECT_NE(tid, controlTid) << "partition " << p << " (worker-owned) ran on the control thread";
        EXPECT_EQ(tid, node.onOwnerThreadForTest(0, p, threadOf))
            << "partition " << p << " owner thread must be stable";
        const int32_t wid = node.workerForPartitionForTest(0, p);
        ASSERT_GE(wid, 0);
        auto [it, inserted] = tidByWorker.emplace(wid, tid);
        if (!inserted) {
            EXPECT_EQ(it->second, tid) << "partitions of worker " << wid << " ran on different threads";
        }
    }
    for (auto a = tidByWorker.begin(); a != tidByWorker.end(); ++a) {
        for (auto b = std::next(a); b != tidByWorker.end(); ++b) {
            EXPECT_NE(a->second, b->second)
                << "workers " << a->first << " and " << b->first << " share a thread";
        }
    }

    // Internal partitions (topicId < 0) → the CONTROL thread.
    int internalSeen = 0;
    for (int32_t t : {-1, -2, -3}) {
        if (!rune::node::test::hasReplica(node, t, 0)) continue;
        ++internalSeen;
        EXPECT_EQ(node.onOwnerThreadForTest(t, 0, threadOf), controlTid)
            << "internal partition topicId=" << t << " must run on the control thread";
    }
    EXPECT_GE(internalSeen, 1) << "expected at least one internal partition replica";

    // Mutation on the owner: append advances LEO by exactly one record.
    const auto leo0 = rune::node::test::leoOnOwner(node);
    const std::vector<uint8_t> payload(16, 0x5A);
    EXPECT_GE(rune::node::test::appendOnOwner(node, payload, /*acksAll=*/false), 0);
    EXPECT_EQ(rune::node::test::leoOnOwner(node), leo0 + 1);

    // Misuse fails loudly; the callable's own exceptions propagate.
    EXPECT_FALSE(rune::node::test::hasReplica(node, 0, 99));            // pointer form: nullptr
    EXPECT_THROW(rune::node::test::leoOnOwner(node, 0, 99), std::runtime_error);  // ref form
    EXPECT_THROW(node.onOwnerThreadForTest(kCatalogTopicId, 0, threadOf), std::invalid_argument);
    EXPECT_THROW(node.onOwnerThreadForTest(0, 0,
                     [](PartitionReplica&) -> int { throw std::domain_error("boom"); }),
                 std::domain_error);

    node.stop();
    EXPECT_THROW(node.onOwnerThreadForTest(0, 0, threadOf), std::logic_error);
}

// ===========================================================================
// F1 REGRESSION: a task the owner has not STARTED by the deadline is cancelled
// and NEVER runs afterwards (its callable captures test locals by reference —
// running it after the test frame unwinds would be a use-after-free).
// ===========================================================================
TEST(BrokerNodeTest, OwnerThreadHookTimeoutCancelsTaskRegression) {
    TempDir tmp{"f1_cancel_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    TopicSpec ts;
    ts.name           = "f1c";
    ts.partitionCount = 1;
    ts.topicId        = 0;

    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(2s));
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);

    // Occupy the control thread with a task that blocks until released (hard cap).
    std::promise<void> started, release;
    auto releaseFut = release.get_future().share();
    auto blocker = std::async(std::launch::async, [&] {
        return node.onControlThreadForTest([&started, releaseFut] {
            started.set_value();
            return releaseFut.wait_for(10s) == std::future_status::ready;
        }, 15s);
    });
    ASSERT_EQ(started.get_future().wait_for(5s), std::future_status::ready);

    // Queued behind the blocker → cannot start within 100ms → the owner skips it
    // + the caller throws.  F9a fix round 1: the caller waits for the owner's
    // decision with no bound, so the blocker is released from another thread
    // AFTER the 100 ms deadline (the owner then reaches + skips the task).
    std::thread releaser([&release] {
        std::this_thread::sleep_for(std::chrono::milliseconds{400});
        release.set_value();
    });
    bool ran = false;
    EXPECT_THROW(node.onControlThreadForTest([&ran] { ran = true; return 0; },
                                             std::chrono::milliseconds{100}),
                 std::runtime_error);
    releaser.join();
    EXPECT_TRUE(blocker.get()) << "blocker task must have been released, not timed out";

    // FIFO round-trip AFTER the cancelled task: it has been drained by now.
    EXPECT_EQ(node.onControlThreadForTest([] { return 7; }), 7);
    EXPECT_FALSE(ran) << "a cancelled (timed-out) task must never run";

    node.stop();
}

// F18: the started node enforces the derived intake limit on the gateway (not the
// old hardcoded 8 MiB, which a default worker-ingress op frame cannot carry).
TEST(BrokerNodeRegression, GatewayEnforcesProduceIntakeLimit) {
    TempDir tmp{"f18_wire_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    // A free port from the shared allocator (the fixed 50001 collides with parallel suites).
    cfg.clientEndpoint.channel =   // D11: never an ephemeral (bind-0) port
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    BrokerNode node{cfg};
    node.start();
    ASSERT_NE(node.gatewayForTest(), nullptr);
    EXPECT_EQ(node.gatewayForTest()->maxProduceBatchBytes(),
              static_cast<std::size_t>(cfg.produceIntakeLimit()));
    node.stop();
}

// Integration wave 2 (F18 ⇄ F15 S3 ⇄ F19, S3 re-review 3 minor 1): ONE batch
// limit.  The started node's gateway limit == EVERY worker's intake limit ==
// the config formula (Worker::maxAcceptableBatchBytes over the raw ring / term
// sizes, replication and egress terms included even single-node).
TEST(BrokerNodeRegression, GatewayAndEveryWorkerShareTheOneBatchLimit) {
    TempDir tmp{"wave2_limit_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.clientEndpoint.channel =   // D11: never an ephemeral (bind-0) port
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.workerCount = 2;
    TopicSpec ts;
    ts.name           = "wave2-limit";
    ts.partitionCount = 4;
    ts.topicId        = 0;

    const std::size_t formula = Worker::maxAcceptableBatchBytes(
        static_cast<std::size_t>(cfg.workerIngressRingBytes),
        static_cast<std::size_t>(cfg.workerReturnRingBytes),
        static_cast<std::size_t>(cfg.replicationOutboundRingBytes),
        static_cast<std::size_t>(cfg.aeronTermBufferBytes),
        static_cast<std::size_t>(cfg.aeronTermBufferBytes),
        static_cast<std::size_t>(cfg.aeronTermBufferBytes));
    ASSERT_EQ(static_cast<std::size_t>(cfg.produceIntakeLimit()), formula) << "a thin wrapper";

    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10s));
    ASSERT_EQ(rune::node::test::TestCluster({&node}).defineTopic(ts), 0);
    ASSERT_NE(node.gatewayForTest(), nullptr);
    EXPECT_EQ(node.gatewayForTest()->maxProduceBatchBytes(), formula);
    int workers = 0;
    // Set once at startup, before any worker thread runs; immutable after.
    for (int32_t w = 0; Worker* wk = node.workerForTest(w); ++w, ++workers)
        EXPECT_EQ(wk->maxIntakeBatchBytes(), formula) << "worker " << w;
    EXPECT_EQ(workers, 2);
    node.stop();
}

} // namespace rune::node

namespace rune::node {
// REGRESSION (k3s, broker 7ad0b2a): the bench's cluster.sh binds the client
// endpoint on 0.0.0.0 with every peer on 127.0.0.1 — start() derived mtu=1472
// (the NIC) instead of 65504 (14-18k vs 136,234 rec/s).  A real BrokerNode with
// exactly those endpoints must resolve the 64 KiB loopback MTU (clamped only by
// the platform's socket limit).
TEST(BrokerNodeRegression, ClusterShStyleWildcardBindDerivesLoopbackMtuRegression) {
    const fs::path dir = makeTempDataDir("wildmtu_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:" + std::to_string(cfg.peers[0].clientPort ? cfg.peers[0].clientPort : 50001);
    ASSERT_EQ(cfg.aeronMtuBytes, NodeConfig::kAeronAuto) << "default = derived";
    BrokerNode node{cfg};
    node.start();
    const auto& eff = node.effectiveConfigForTest();
    const auto facts = probeHostNetFacts({"127.0.0.1"});
    if (std::min(facts.rmemMax, facts.wmemMax) >= 1024 * 1024)
        EXPECT_EQ(eff.aeronMtuBytes, 65504) << "wildcard bind + loopback peers is loopback";
    else
        EXPECT_GT(eff.aeronMtuBytes, 1472) << "loopback, clamped by small socket limits";
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}
} // namespace rune::node

namespace rune::node {
// D8 root cause (PerWorkerReplication segfault, 2026-10-03): a ClientGateway that failed to construct (here: its
// client port is already bound) was logged and SKIPPED — the node ran HEADLESS: in consensus (it could lead)
// but unreachable by any client, and gatewayForTest() was null.  Now start() refuses, like the per-worker planes.
TEST(BrokerNodeRegression, GatewayConstructionFailureRefusesToStartNotHeadless) {
    const fs::path dir = makeTempDataDir("headless_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    const int squat = ::socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(squat, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(clientPort);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(squat, reinterpret_cast<sockaddr*>(&a), sizeof(a)), 0) << "test setup: occupy the client port";
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::exception) << "a node without its client gateway must not run";
    EXPECT_EQ(node.gatewayForTest(), nullptr);
    node.stop();
    ::close(squat);
}

// ---------------------------------------------------------------------------
// Phase 2b E (spec gate): a runtime DefineTopic(persistence=transient) creates
// the topic on an InMemoryLog (no disk partition dir), and after a node restart
// the topic EXISTS again as a transient topic and is EMPTY (data never on disk).
// ---------------------------------------------------------------------------
// Catalog history Task 9: a DISK topic created by DDL comes back after a restart
// from the __catalog boot replay — at its catalog id, WITH its data: boot's topic
// dir reconcile claims the catalog's live topics (topic dirs are claimed by the
// catalog), so the dir is never quarantined and the topic never restarts empty.
TEST(BrokerNodeRegression, CatalogDiskTopicKeepsItsDataAcrossARestartRegression) {
    using Clock = std::chrono::steady_clock;
    using rune::replication::PartitionReplica;
    const fs::path dir = makeTempDataDir("catalog_restart_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const std::vector<uint8_t> blob = rune::codec::serialize(
        rune::codec::DescriptorBuilder("kept_disk").add("id", rune::codec::PrimType::Int64, 1, true).build());
    auto awaitLeaderReplica = [](BrokerNode& n, int32_t topicId, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            if (rune::node::test::hasReplica(n, topicId, 0) &&
                rune::node::test::roleOnOwner(n, topicId, 0) == rune::replication::Role::Leader)
                return true;
            std::this_thread::sleep_for(5ms);   // poll interval, not correctness
        }
        return false;
    };
    int32_t topicId = -1;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        rune::node::test::ShardTestClient client(dir / "client-aeron");
        const int64_t sid = client.connect(gw, stream);
        ASSERT_GT(sid, 0);
        const auto def = client.defineTopicUntil(gw, stream, sid, "kept_disk", 1, /*validate=*/false, blob,
                                                 Clock::now() + 20s);
        ASSERT_TRUE(def.has_value());
        ASSERT_EQ(def->errorCode, 0u);
        topicId = def->topicId;
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 10s));
        const std::vector<uint8_t> rec(24, 0x5A);
        ASSERT_GE(rune::node::test::appendOnOwner(node, rec, /*acksAll=*/false, topicId, 0), 0);
        ASSERT_EQ(rune::node::test::leoOnOwner(node, topicId, 0), 1);
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 20s)) << "rebuilt by the boot replay";
        EXPECT_EQ(rune::node::test::leoOnOwner(node, topicId, 0), 1) << "its data survived the restart";
        EXPECT_FALSE(fs::exists(dir / "quarantine")) << "the catalog topic's dir was claimed, never quarantined";
        node.stop();
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(BrokerNodeRegression, RuntimeTransientTopicIsInMemoryAndRestartsEmptyRegression) {
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    using rune::replication::PartitionReplica;
    const fs::path dir = makeTempDataDir("transient_rt_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;

    const auto desc = rune::codec::DescriptorBuilder("presence")
        .add("userId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("online", rune::codec::PrimType::Bool)
        .build();
    const std::vector<uint8_t> blob = rune::codec::serialize(desc);

    auto awaitLeaderReplica = [](BrokerNode& n, int32_t topicId, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            if (rune::node::test::hasReplica(n, topicId, 0) &&
                rune::node::test::roleOnOwner(n, topicId, 0) == rune::replication::Role::Leader)
                return true;
            std::this_thread::sleep_for(5ms);   // poll interval, not correctness
        }
        return false;
    };

    int32_t topicId = -1;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        rune::node::test::ShardTestClient client(dir / "client-aeron");
        const int64_t sid = client.connect(gw, stream);
        ASSERT_GT(sid, 0);

        const auto deadline = Clock::now() + 20s;
        while (topicId < 0) {
            ASSERT_LT(Clock::now(), deadline) << "DefineTopic did not commit within the deadline";
            auto def = client.defineTopicUntil(gw, stream, sid, "presence", 1, /*validate=*/false,
                                               blob, std::min(deadline, Clock::now() + 5s),
                                               pc::DefineTopic::kPersistenceTransient);
            ASSERT_TRUE(def.has_value()) << "no DefineTopicResponse";
            if (def->errorCode == 0 ||
                (def->errorCode == static_cast<uint16_t>(pc::ErrorCode::TopicExists) && def->topicId >= 0)) {
                topicId = def->topicId;
                break;
            }
            ASSERT_EQ(def->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal))
                << "non-retryable DefineTopic error " << def->errorCode;
            std::this_thread::sleep_for(20ms);   // admin leadership settling (short backoff)
        }
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 10s));

        const auto [mem, disk] = node.onOwnerThreadForTest(topicId, 0,
            [](PartitionReplica& r) { return std::pair{r.isMemoryBacked(), r.isDiskBacked()}; });
        EXPECT_TRUE(mem) << "a transient runtime topic must be InMemoryLog-backed";
        EXPECT_FALSE(disk);
        EXPECT_FALSE(fs::exists(dir / "topics" / "presence"))
            << "a transient topic must never create a disk partition directory";

        const std::vector<uint8_t> rec(24, 0x5A);
        ASSERT_GE(rune::node::test::appendOnOwner(node, rec, /*acksAll=*/false, topicId, 0), 0);
        ASSERT_GT(rune::node::test::leoOnOwner(node, topicId, 0), 0) << "data present before restart";
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 20s))
            << "the transient topic's metadata must survive the restart (the __catalog boot replay)";
        const auto [mem, leo] = node.onOwnerThreadForTest(topicId, 0,
            [](PartitionReplica& r) { return std::pair{r.isMemoryBacked(), r.logEndOffset()}; });
        EXPECT_TRUE(mem) << "recovered as TRANSIENT, not as a disk topic";
        EXPECT_EQ(leo, 0) << "a transient topic restarts EMPTY — its data was never on disk";
        EXPECT_FALSE(fs::exists(dir / "topics" / "presence"));
        node.stop();
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// Catalog history Task 9 (the catalog equivalent of B10 / D4's marker test): on a
// live single-node BrokerNode a client DefineTopic is ONE __catalog event; once it
// is committed and applied the control thread builds the topic at its catalog id
// (kA2cCatalogTopicApplied) — replicas included — and writes NO topic-id marker
// (the catalog is the record of truth).  Condition-driven, bounded.
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, CatalogDefineTopicBuildsTheTopicAtItsCatalogIdWithItsMarkerRegression) {
    const fs::path dir = makeTempDataDir("marker_e2e_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));

    rune::node::test::ShardTestClient client(dir / "client-aeron");
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const int64_t sid = client.connect(gw, stream);
    ASSERT_GT(sid, 0);

    const std::vector<uint8_t> blob = rune::codec::serialize(
        rune::codec::DescriptorBuilder("marked").add("id", rune::codec::PrimType::Int64).build());
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + 20s;
    constexpr uint16_t kDdlInternal = static_cast<uint16_t>(pc::ErrorCode::Internal);
    constexpr uint16_t kDdlExists   = static_cast<uint16_t>(pc::ErrorCode::TopicExists);

    int32_t topicId = -1;
    bool sawInternal = false;
    while (topicId < 0) {
        ASSERT_LT(Clock::now(), deadline) << "DefineTopic did not commit within the test deadline";
        auto def = client.defineTopicUntil(gw, stream, sid, "marked", 1, /*validate=*/false,
                                           blob, std::min(deadline, Clock::now() + 5s));
        ASSERT_TRUE(def.has_value()) << "no DefineTopicResponse";
        if (def->errorCode == 0) { topicId = def->topicId; break; }
        if (def->errorCode == kDdlExists && sawInternal) { topicId = def->topicId; break; }
        ASSERT_EQ(def->errorCode, kDdlInternal) << "non-retryable code " << def->errorCode;
        sawInternal = true;
        std::this_thread::sleep_for(20ms);   // admin leadership settling
    }
    ASSERT_GE(topicId, 0);

    EXPECT_EQ(topicId, 0) << "the first catalog id";
    EXPECT_TRUE(rune::node::test::hasReplica(node, topicId, 0)) << "built from the applied event";
    EXPECT_TRUE(fs::exists(dir / "topics" / "marked" / "0"));
    {   // Task 16 fix round 1 (R1): the dir carries the topic-id marker, written before any partition dir
        std::ifstream in(dir / "topics" / "marked" / "topic-id");
        std::string line((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_EQ(line, std::to_string(topicId) + "\n") << "a catalog topic's dir is marked with its catalog id";
    }

    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// Catalog history Task 9 (fix-on-sight, Task 4 M3): a test helper whose encoder
// refuses a frame (encode() returns 0 for a field over its wire width) never
// sends a zero-length frame — the DDL helpers report the refusal explicitly.
// ---------------------------------------------------------------------------
TEST(ShardTestClientRegression, AnEncoderRefusalIsNeverSentRegression) {
    const fs::path dir = makeTempDataDir("stc_encode0_");
    fs::create_directories(dir);
    rune::node::test::ShardTestClient client(dir / "client-aeron");
    const std::string tooLong(0x10000, 'x');   // one past a varstr's u16 length
    const std::string channel = "aeron:udp?endpoint=127.0.0.1:9";   // never reached
    EXPECT_EQ(client.sendDefineTopic(channel, 1, 1, tooLong, 1, {}), 0) << "DefineTopic";
    EXPECT_EQ(client.sendCreateView(channel, 1, 1, tooLong, "id", 0, {}), 0) << "CreateView";
    EXPECT_EQ(client.sendAlterTopic(channel, 1, 1, tooLong, 0, {}), 0) << "AlterTopic";
    EXPECT_EQ(client.sendGetTopicSchema(channel, 1, 1, tooLong), 0) << "GetTopicSchema";
    EXPECT_FALSE(client.defineTopicUntil(channel, 1, 1, tooLong, 1, false, {},
                                         std::chrono::steady_clock::now() + 1s).has_value());
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// Catalog history Task 9 (found while wiring the catalog OK): the admin thread
// posts the applied topic (failure ring) and the client's OK (egress ring) with
// no order between them, so the DDL answer gate must hold an OK until the
// control thread has BUILT the topic at its catalog id and placed it — the old
// predicate treated "not built yet" as "serves" and released the OK at once.
// Deterministic: building the topic fails (setCommitReplicaFaultForTest), so it
// never serves — the client must get Internal (in doubt) after the hold, never OK.
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, CatalogDefineTopicOkIsNeverSentForATopicNotBuiltRegression) {
    const fs::path dir = makeTempDataDir("catalog_ok_gate_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    BrokerNode node{cfg};
    node.setCommitReplicaFaultForTest("unbuilt");
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));

    rune::node::test::ShardTestClient client(dir / "client-aeron");
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const int64_t sid = client.connect(gw, stream);
    ASSERT_GT(sid, 0);
    const std::vector<uint8_t> blob = rune::codec::serialize(
        rune::codec::DescriptorBuilder("unbuilt").add("id", rune::codec::PrimType::Int64).build());
    namespace pc = rune::protocol::client;
    const auto def = client.defineTopicUntil(gw, stream, sid, "unbuilt", 1, /*validate=*/false, blob,
                                             std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(def.has_value()) << "the DDL must be answered (never a wait-out)";
    EXPECT_EQ(def->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal))
        << "an OK for a topic that was never built is a silent fallback";
    EXPECT_FALSE(rune::node::test::hasReplica(node, 0, 0));
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------
// DDL conformance B3 end to end on a live single-node BrokerNode: a DefineTopic
// whose name differs only in letter case from an existing topic is TopicExists
// carrying the existing topic's id, and NOTHING is created for it (no catalog id
// consumed, no replica) — catalog history Task 9: decided by the catalog leader
// from its applied state (rune/catalog/TopicName.h fold), no event appended.
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, CaseVariantDefineTopicIsTopicExistsEndToEndRegression) {
    const fs::path dir = makeTempDataDir("case_fold_e2e_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    // The case rule is the catalog's own.
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));

    rune::node::test::ShardTestClient client(dir / "client-aeron");
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const int64_t sid = client.connect(gw, stream);
    ASSERT_GT(sid, 0);

    const std::vector<uint8_t> blob = rune::codec::serialize(
        rune::codec::DescriptorBuilder("orders").add("id", rune::codec::PrimType::Int64).build());
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    constexpr uint16_t kInternal = static_cast<uint16_t>(pc::ErrorCode::Internal);
    constexpr uint16_t kExists   = static_cast<uint16_t>(pc::ErrorCode::TopicExists);
    const auto testDeadline = Clock::now() + 30s;
    // One DefineTopic; Internal (admin leadership still settling) is retried.
    auto define = [&](std::string_view name, uint8_t persistence) -> std::optional<pc::DefineTopicResponse> {
        while (Clock::now() < testDeadline) {
            auto r = client.defineTopicUntil(gw, stream, sid, name, 1, /*validate=*/false, blob,
                                             std::min(testDeadline, Clock::now() + 5s), persistence);
            if (!r || r->errorCode != kInternal) return r;
            std::this_thread::sleep_for(20ms);
        }
        return std::nullopt;
    };

    const auto orders = define("Orders", 0);
    ASSERT_TRUE(orders.has_value());
    ASSERT_EQ(orders->errorCode, 0);
    const int32_t ordersId = orders->topicId;
    ASSERT_GE(ordersId, 0);

    for (const auto& [name, persistence] : std::vector<std::pair<std::string, uint8_t>>{
             {"orders", 0}, {"ORDERS", 1}, {"oRdErS", 0}}) {
        SCOPED_TRACE(name);
        const auto r = define(name, persistence);
        ASSERT_TRUE(r.has_value()) << "no DefineTopicResponse";
        EXPECT_EQ(r->errorCode, kExists);
        EXPECT_EQ(r->topicId, ordersId) << "answers the colliding topic's id";
    }

    // Nothing was created for any refused variant: the next topic takes the very
    // next id, and no replica exists past it.
    const auto next = define("next", 0);
    ASSERT_TRUE(next.has_value());
    ASSERT_EQ(next->errorCode, 0);
    EXPECT_EQ(next->topicId, ordersId + 1) << "a refused variant consumed a topic id";
    EXPECT_TRUE(rune::node::test::hasReplica(node, ordersId, 0));
    EXPECT_FALSE(rune::node::test::hasReplica(node, next->topicId + 1, 0));

    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

// DDL conformance Task 12: schemaRegistryForTest() / viewRegistryForTest() are
// documented as readable "when the node is stopped", but stop() reset both, so
// every post-stop catalog check dereferenced null (SEGFAULT).  The registries own
// no replica pointers and no thread touches them after stop(): they now live until
// the node is destroyed.
TEST(BrokerNodeStopRegression, CatalogRegistriesReadableAfterStop) {
    const fs::path dir = makeTempDataDir("regs_after_stop_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));

    rune::node::test::ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;
    const int64_t sid = client.connect(gw, stream);
    ASSERT_GT(sid, 0);
    const std::vector<uint8_t> blob = rune::codec::serialize(
        rune::codec::DescriptorBuilder("kept").add("id", rune::codec::PrimType::Int64).build());
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + 30s;
    std::optional<pc::DefineTopicResponse> r;
    while (Clock::now() < deadline) {   // Internal while admin leadership settles
        r = client.defineTopicUntil(gw, stream, sid, "kept", 1, /*validate=*/false, blob,
                                    std::min(deadline, Clock::now() + 5s), 0);
        if (!r || r->errorCode != static_cast<uint16_t>(pc::ErrorCode::Internal)) break;
        std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->errorCode, 0);

    node.stop();
    ASSERT_NE(node.schemaRegistryForTest(), nullptr) << "readable after stop (documented contract)";
    ASSERT_NE(node.schemaRegistryForTest()->byName("kept"), nullptr);
    EXPECT_EQ(node.schemaRegistryForTest()->byName("kept")->version, 1u);
    EXPECT_NE(node.viewRegistryForTest(), nullptr) << "readable after stop (documented contract)";
    std::error_code ec; fs::remove_all(dir, ec);
}

} // namespace rune::node
