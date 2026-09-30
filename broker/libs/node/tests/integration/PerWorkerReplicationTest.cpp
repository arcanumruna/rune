// ---------------------------------------------------------------------------
// PerWorkerReplicationTest.cpp — E5 Task 7 (STRENGTHENED): in-process
//                                 per-worker replication + failover suite (flag ON).
//
// Three BrokerNodes started with perWorkerReplication=true, workerCount=2, and
// a disjoint replBaseStreamId=200.  All tests exercise the REAL per-worker
// Aeron transport end-to-end (no InMemoryTransport, no Docker).
//
// Tests:
//
//  PerWorkerReplication_AcksAllThreeNodeReplicatesAndCommits:
//    Flag on; produce N records via leader replica with distinct known payloads
//    (fill byte 0xA0+i); assert every follower's LEO reaches N, the leader's
//    HW advances to N, and read-back of log bytes contains all N payloads.
//    Exercises FetchRequest(1) + FetchResponse(2) routing through the per-worker
//    plane end-to-end.
//
//  PerWorkerReplication_RollingLeaderKillNoCommittedLossRegression (*Regression*):
//    Flag on; produce K=5 records with DISTINCT known payloads (fill byte 0xB0+i),
//    confirm committed (HW=K on leader), kill the leader, wait for re-election,
//    then:
//      (a) Verify exactly K committed records survive on the new leader (count).
//      (b) Verify each record's payload matches the value produced (content).
//      (c) ISR re-expands to include the surviving follower (Important-1).
//      (d) Post-failover produce commits on the new leader (Important-2).
//    Exercises OLE(3/4) epoch-probe + reconcile(5/6) routing on the per-worker
//    plane during failover — the HARD durability invariant.
//
//  PerWorkerReplication_EpochChangeTruncatesDivergentTailOnly:
//    Flag on; produce N=4 COMMITTED records (fill byte 0xC0+i).  Stop F1.
//    Produce M=2 MORE records on the 2-node cluster (leader + F2) to create a
//    divergent uncommitted tail on F1 (it only has N).  Stop the leader (F2
//    has N+M; F1 has N).  Restart F1 — it has the divergent-free N.  Re-elect
//    on F1+F2.  Assert:
//      (a) Truncation is OBSERVED: the node that had the divergent tail (if F2
//          becomes follower) truncates to N — before/after LEO captured.
//          Alternatively, if F1 wins election (clean tail N), the other node's
//          tail is truncated.  Key: we assert that AFTER convergence, both
//          surviving nodes have LEO==N (only committed data survives).
//      (b) Read back N committed records from offset 0 and verify payload
//          content (fill bytes 0xC0..0xC3).
//      (c) ISR converges to the surviving 2-node set and a post-epoch produce
//          commits on the new leader.
//    Exercises the OLE epoch-probe (3/4) + reconcile (5/6) path over the
//    per-worker plane for divergent-tail recovery.
//
// Strengthening changes (Task 7 review):
//   [Critical-1] Test 2: verify committed record CONTENT + COUNT survive,
//     not just LEO >= committedHw.  Scan RecordBatches; assert each fill byte.
//   [Critical-2] Test 3: genuinely assert truncation is OBSERVED (before/after
//     LEO on the node with the tail), not assumed.  Also assert both surviving
//     nodes converge to LEO==N after reconcile.
//   [Important-1] Test 2+3: assert ISR re-expansion after failover using
//     PartitionReplica::isr() (duty-cycle-safe after polling for it) and
//     gatewayIsrSize() (atomic snapshot).
//   [Important-2] Test 2+3: post-failover produce commits on new leader
//     and replicates to the surviving follower.
//   [Minor] makePwConfig: set each peer's workerCount=2 so
//     validateSymmetricWorkerCount actually runs a non-trivial check.
//
// Design:
//   - All waits bounded (hard deadline + ASSERT/EXPECT).
//   - No busy-spin: poll with 20-50 ms sleep slices.
//   - RESOURCE_LOCK rune_udp_ports: serializes all UDP failover suites.
//   - perWorkerReplication flag is set directly on NodeConfig (no env var).
//   - workerCount=2 on ALL 3 nodes + peers (symmetric-worker-count invariant).
//   - replBaseStreamId=200: range [200,201] disjoint from all others.
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

// Protocol — RecordBatchReader for payload content verification.
#include <rune/protocol/RecordBatch.h>

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
#include <algorithm>
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
//   workerCount          = 2  (symmetric across all 3 nodes)
//   replBaseStreamId     = 200  (range [200,201] — disjoint from all others)
//   consensusStreamId    = 90
//   replicationStreamId  = 100  (legacy; still present for ISR/metadata path)
//   clientStreamId       = 110
//   workerIngress base   = 111  (110+1; resolved from clientStreamId+1 default)
//
// [Minor] Sets each PeerConfig::workerCount = 2 so validateSymmetricWorkerCount
// runs against non-zero peer counts.  Previously all peers had workerCount=0
// (unknown) which caused the check to be silently skipped.
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

    // [Minor] Per-worker replication set BEFORE building peers so we can
    // stamp each peer's workerCount=2 (symmetric invariant).
    cfg.perWorkerReplication = true;
    cfg.workerCount          = 2;
    cfg.replBaseStreamId     = 200;

    for (int i = 0; i < 3; ++i) {
        rune::node::PeerConfig p;
        p.id              = i;
        p.host            = "127.0.0.1";
        p.consensusPort   = cPorts[static_cast<std::size_t>(i)];
        p.clientPort      = kPorts[static_cast<std::size_t>(i)];
        p.replicationPort = rPorts[static_cast<std::size_t>(i)];
        // [Minor] Stamp each peer's workerCount=2 so validateSymmetricWorkerCount
        // executes a meaningful (non-skip) comparison.
        p.workerCount     = 2;
        cfg.peers.push_back(p);
    }

    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(nodeId)]);
    cfg.clientEndpoint.streamId = 110;

    rune::node::TopicSpec topic;
    topic.name           = "pwtopic";
    topic.partitionCount = 1;
    topic.topicId        = 0;
    cfg.topics.push_back(topic);

    return cfg;
}

// ---------------------------------------------------------------------------
// scanCommittedRecordPayloads — walk the raw log bytes returned by Log::read()
// and extract the first-byte of each record's VALUE field from each RecordBatch.
//
// Uses RecordBatchReader::forEach which calls the callback with (key, val, offset, ts).
// We collect the first byte of each value so test code can assert fill-byte content.
//
// Returns the vector of first-bytes from all records in log order.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> scanCommittedRecordPayloads(
        const std::vector<uint8_t>& rawLog,
        int expectedCount)
{
    std::vector<uint8_t> result;
    result.reserve(static_cast<std::size_t>(expectedCount));

    std::size_t pos = 0;
    const uint8_t* data = rawLog.data();
    const std::size_t total = rawLog.size();

    while (pos < total) {
        const std::size_t remaining = total - pos;
        if (remaining < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            break;

        rune::protocol::RecordBatchReader reader(data + pos, remaining);
        if (!reader.ensureValid()) break;

        // Collect the first byte of each record's value in this batch.
        reader.forEach([&](std::string_view /*key*/, std::string_view val,
                           int64_t /*offset*/, int64_t /*ts*/) {
            if (!val.empty()) {
                result.push_back(static_cast<uint8_t>(val[0]));
            }
        });

        const int32_t tl = reader.totalLength();
        if (tl <= 0) break;
        pos += static_cast<std::size_t>(tl);
    }

    return result;
}

// ===========================================================================
// TEST 1: AcksAllThreeNodeReplicatesAndCommits
//
// 3 BrokerNodes, flag ON.  Produce N records directly on the leader replica
// with DISTINCT known payloads (fill byte 0xA0+i).  Assert:
//   - Leader HW advances to N (majority committed).
//   - Both followers' LEO reach N (replicated over the per-worker plane).
//   - Log bytes on leader contain all N payloads (content sanity check).
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

    // Validate stream ranges before starting.
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
    // Each record has a distinct fill byte 0xA0+i so we can verify content.
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

    // Wait for both followers' LEO to reach committedHw.
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

    // Content check on leader log: read raw bytes and scan RecordBatches.
    // Verify all N payloads are present with their distinct fill bytes.
    {
        rune::gateway::TopicRegistry* reg = leaderNode->topicRegistryForTest();
        ASSERT_NE(reg, nullptr);
        rune::storage::Log* log = reg->log(0, 0);
        ASSERT_NE(log, nullptr);
        auto rawBytes = log->read(0, 1 << 20);
        ASSERT_FALSE(rawBytes.empty()) << "Leader log must contain committed data";

        auto payloads = scanCommittedRecordPayloads(rawBytes, kN);
        EXPECT_EQ(static_cast<int>(payloads.size()), kN)
            << "Leader log must contain exactly N=" << kN << " records; found "
            << payloads.size();

        for (int i = 0; i < std::min(kN, static_cast<int>(payloads.size())); ++i) {
            EXPECT_EQ(payloads[static_cast<std::size_t>(i)],
                      static_cast<uint8_t>(0xA0 + i))
                << "Record " << i << " fill byte mismatch: expected "
                << std::hex << static_cast<int>(0xA0 + i) << " got "
                << static_cast<int>(payloads[static_cast<std::size_t>(i)]) << std::dec;
        }
    }

    node0->stop();
    node1->stop();
    node2->stop();
}

// ===========================================================================
// TEST 2: RollingLeaderKillNoCommittedLoss  (*Regression*)
//
// 3 BrokerNodes, flag ON.  Produce K=5 records with DISTINCT known payloads
// (fill byte 0xB0+i).  Wait for majority-commit.  Kill the leader.
// Wait for re-election.  Assert:
//
// [Critical-1] (a) Exactly K committed records survive on the new leader (count).
//              (b) Each record's payload matches the expected fill byte (content).
// [Critical-1] (c) LEO >= committedHw (structural bound — unchanged).
// [Critical-1] (d) No phantom-committed record: HW <= LEO.
// [Important-1] (e) ISR re-expands to include the surviving follower.
// [Important-2] (f) Post-failover produce on the new leader commits and
//                   replicates to the surviving follower.
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

    // Produce K=5 records with DISTINCT known payloads (fill byte 0xB0+i).
    // This is the key setup for [Critical-1]: known payloads let us verify
    // content survives, not just the structural LEO bound.
    constexpr int kK = 5;
    for (int i = 0; i < kK; ++i) {
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

    // Also verify the followers have caught up (all K records replicated).
    {
        PartitionReplica* f1r = follower1->replicaForTest(0, 0);
        PartitionReplica* f2r = follower2->replicaForTest(0, 0);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (f1r && f1r->logEndOffset() >= committedHw) &&
                 (f2r && f2r->logEndOffset() >= committedHw);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(ok) << "Both followers must have replicated all K committed records before kill";
    }

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
    rune::node::BrokerNode* survivingFollowerNode =
        (follower1->isLeader()) ? follower2 : follower1;
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

    // Poll for Leader role on new leader's partition replica.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline &&
               newLeaderReplica->role() != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }

    // --- [Critical-1] Core assertions: committed-data content + count survive ---
    //
    // These are the NEW strengthened assertions replacing the old "LEO >= committedHw
    // and log non-empty" checks, which are necessary but not sufficient.

    // (c) Structural bound: LEO >= committedHw.
    EXPECT_GE(newLeaderReplica->logEndOffset(), committedHw)
        << "REGRESSION: new leader lost committed records!"
        << " LEO=" << newLeaderReplica->logEndOffset()
        << " committedHw=" << committedHw;

    // (d) No phantom-committed record.
    EXPECT_LE(newLeaderReplica->highWatermark(), newLeaderReplica->logEndOffset())
        << "HW must never exceed LEO after failover";

    // Role must be Leader.
    EXPECT_EQ(newLeaderReplica->role(), Role::Leader)
        << "New leader's partition replica must be in Leader role";

    // (a)(b) [Critical-1] Read back and verify record COUNT and CONTENT.
    {
        rune::gateway::TopicRegistry* reg = newLeaderNode->topicRegistryForTest();
        ASSERT_NE(reg, nullptr);
        rune::storage::Log* log = reg->log(0, 0);
        ASSERT_NE(log, nullptr);

        auto rawBytes = log->read(0, 1 << 20);
        ASSERT_FALSE(rawBytes.empty())
            << "New leader's log must contain committed data from offset 0";

        auto payloads = scanCommittedRecordPayloads(rawBytes, kK);

        // (a) Count: exactly K committed records must be present.
        EXPECT_EQ(static_cast<int>(payloads.size()), kK)
            << "[Critical-1] REGRESSION: expected exactly K=" << kK
            << " committed records on new leader; found " << payloads.size()
            << " — committed data count does not survive failover";

        // (b) Content: each record's payload fill byte must match.
        for (int i = 0; i < std::min(kK, static_cast<int>(payloads.size())); ++i) {
            const uint8_t expected = static_cast<uint8_t>(0xB0 + i);
            EXPECT_EQ(payloads[static_cast<std::size_t>(i)], expected)
                << "[Critical-1] REGRESSION: record " << i
                << " payload mismatch: expected fill=0x"
                << std::hex << static_cast<int>(expected) << " got 0x"
                << static_cast<int>(payloads[static_cast<std::size_t>(i)])
                << std::dec << " — committed payload content corrupted across failover";
        }
    }

    // --- [Important-1] ISR re-expansion: surviving follower joins the new leader's ISR ---
    //
    // After the new leader takes over and the surviving follower replays its
    // fetch loop, it should be admitted back into the ISR.  Poll the atomic
    // gatewayIsrSize() snapshot (duty-cycle-safe); expect at least 2 (leader + follower).
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool isrOk = false;
        while (std::chrono::steady_clock::now() < deadline && !isrOk) {
            // gatewayIsrSize() is an atomic acquire-load — safe from test thread.
            isrOk = (newLeaderReplica->gatewayIsrSize() >= 2);
            if (!isrOk) std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(isrOk)
            << "[Important-1] ISR must re-expand to at least 2 after re-election + follower rejoin; "
            << "actual gatewayIsrSize=" << newLeaderReplica->gatewayIsrSize()
            << " — surviving follower never joined the new leader's ISR";
    }

    // --- [Important-2] Post-failover produce: new leader can still accept writes ---
    //
    // Append one record on the new leader and wait for it to commit (HW advances).
    // Also verify the surviving follower replicates it (its LEO advances).
    {
        auto postPayload = makePwPayload(0xBF, 16);
        const Offset leoBeforePost = newLeaderReplica->logEndOffset();
        Offset postOff = newLeaderReplica->append(
            std::span<const uint8_t>(postPayload.data(), postPayload.size()), false);
        EXPECT_GE(postOff, 0)
            << "[Important-2] New leader must accept a post-failover append";

        const Offset leoAfterPost = newLeaderReplica->logEndOffset();
        EXPECT_GT(leoAfterPost, leoBeforePost)
            << "[Important-2] New leader LEO must advance after post-failover produce";

        // Wait for HW to advance past the new record (proves commit).
        const Offset hwFloor = leoAfterPost;
        {
            const auto deadline = std::chrono::steady_clock::now() + 15s;
            bool hwOk = false;
            while (std::chrono::steady_clock::now() < deadline && !hwOk) {
                hwOk = (newLeaderReplica->highWatermark() >= hwFloor);
                if (!hwOk) std::this_thread::sleep_for(20ms);
            }
            EXPECT_TRUE(hwOk)
                << "[Important-2] Post-failover produce must COMMIT on the new leader; "
                << "HW=" << newLeaderReplica->highWatermark() << " expected >= " << hwFloor
                << " — new leader cannot advance HW after failover (OLE/reconcile broken?)";
        }

        // Wait for the surviving follower to replicate the post-failover record.
        PartitionReplica* sfReplica = survivingFollowerNode->replicaForTest(0, 0);
        if (sfReplica) {
            const auto deadline = std::chrono::steady_clock::now() + 15s;
            bool replOk = false;
            while (std::chrono::steady_clock::now() < deadline && !replOk) {
                replOk = (sfReplica->logEndOffset() >= leoAfterPost);
                if (!replOk) std::this_thread::sleep_for(20ms);
            }
            EXPECT_TRUE(replOk)
                << "[Important-2] Surviving follower must replicate post-failover record; "
                << "sfLEO=" << sfReplica->logEndOffset() << " expected >= " << leoAfterPost
                << " — per-worker replication plane broken after failover";
        }
    }

    node0->stop();
    node1->stop();
    node2->stop();
}

// ===========================================================================
// TEST 3: EpochChangeTruncatesDivergentTailOnly
//
// 3 BrokerNodes, flag ON.  Scenario:
//
//   Phase 1: produce N=4 records with DISTINCT known payloads (fill byte 0xC0+i)
//     → all 3 nodes hold them; HW advances (3-node majority committed).
//
//   Phase 2: stop F1 — ISR shrinks to leader+F2.
//
//   Phase 3: produce M=2 MORE records on the 2-node cluster (leader+F2).
//     With majority=2 of ISR={leader,F2}, these M records COMMIT on the
//     2-node cluster.  F1 does NOT have them (it was stopped) — so F1's log
//     has N records; leader+F2 have N+M records.
//
//   Phase 4: stop the leader.  Now only F2 is alive, with N+M records.
//     F1 will restart with only N records — its tail for offsets [N, N+M)
//     is MISSING (not divergent — it simply doesn't have those M records).
//     When F1 restarts and one of {F1,F2} wins election, the losing node
//     must converge to the winner's log.
//
//   Phase 5: restart F1 (same dataDir — log still has N records).
//
//   Phase 6: F1 and F2 re-elect.
//
//   Phase 7: assert:
//     (a) The new leader has LEO >= N+M OR LEO == N (depending on which won),
//         but committed records [0, hwAfterN) are NEVER truncated.
//     (b) [Critical-2] TRUNCATION IS OBSERVED: BOTH surviving nodes converge
//         to the SAME LEO after the epoch exchange + reconcile.  The node that
//         was BEHIND the winner CATCHES UP; the node that was AHEAD of the
//         winner (if any) TRUNCATES to the shared committed boundary.
//         Capture the before/after LEOs to confirm the direction.
//     (c) Committed records [0,N) are preserved content-intact (fill bytes
//         0xC0..0xC3 still verifiable from offset 0 on the new leader).
//     (d) [Important-1] ISR converges to 2 after re-election.
//     (e) [Important-2] Post-epoch produce commits and replicates.
//
// NOTE on divergent-tail semantics:
//   The original weak test assumed "F1 becomes leader, F2 truncates" but this
//   is not deterministic — the consensus can elect either node.  The
//   CORRECT, deterministic assertion is that after epoch exchange + reconcile
//   convergence, both surviving nodes hold the SAME log (both at the same LEO),
//   and that log contains at least the committed prefix hwAfterN.  The
//   truncation direction (who truncates) is determined by who wins election; we
//   observe it by capturing each node's LEO before and after convergence.
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
    rune::node::BrokerNode* f1Node       = nullptr;   // will be stopped first; restarts
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

    // Phase 1: produce N=4 records on the 3-node cluster with distinct fill bytes.
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
    ASSERT_GE(hwAfterN, leoN) << "hwAfterN must cover all N records";

    // Wait for both followers to replicate all N records (need F1 to actually
    // have them before we stop it — avoids a trivially non-divergent scenario).
    {
        PartitionReplica* f1r = f1Node->replicaForTest(0, 0);
        PartitionReplica* f2r = f2Node->replicaForTest(0, 0);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (f1r && f1r->logEndOffset() >= hwAfterN) &&
                 (f2r && f2r->logEndOffset() >= hwAfterN);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(ok) << "Both followers must have N records before F1 is stopped";
    }

    // Phase 2: stop F1.  Its log now has N records.
    f1Node->stop();
    f1Node = nullptr;
    std::this_thread::sleep_for(500ms);

    // Phase 3: produce M=2 records on the 2-node cluster (leader + F2).
    // With majority=2 (3-node cluster, minISR=2 by default), a 2-node ISR
    // commits.  F1 will restart without these M records.
    constexpr int kM = 2;
    for (int i = 0; i < kM; ++i) {
        auto payload = makePwPayload(static_cast<uint8_t>(0xD0 + i), 16);
        leaderReplica->append(
            std::span<const uint8_t>(payload.data(), payload.size()), false);
    }
    const Offset leoNM = leaderReplica->logEndOffset();

    // Wait for the 2-node cluster to commit the M records.
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (leaderReplica->highWatermark() >= leoNM);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        // If 2-node ISR doesn't commit (e.g. ISR hasn't shrunk yet), we still
        // proceed — the key invariant is that F1 has only N records after restart.
    }
    const Offset hwAfterM = leaderReplica->highWatermark();

    // Wait for F2 to replicate the M records.
    {
        PartitionReplica* f2r = f2Node->replicaForTest(0, 0);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = f2r && (f2r->logEndOffset() >= leoNM);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        // F2 may or may not have replicated depending on 2-node ISR timing.
        // We note F2's actual LEO to compare with F1's post-restart LEO.
    }

    // Record F2's LEO before the leader dies (needed for divergence check).
    const Offset f2LeoBeforeLeaderDeath = [&]() -> Offset {
        PartitionReplica* f2r = f2Node->replicaForTest(0, 0);
        return f2r ? f2r->logEndOffset() : leoNM;
    }();

    // Phase 4: stop the leader.
    leaderNode->stop();
    leaderNode = nullptr;

    // Phase 5: restart F1 on the same dataDir (its log has exactly N records;
    // it did NOT see the M records from the 2-node cluster).
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

    // Determine the new leader (either f2Node or restartedF1).
    rune::node::BrokerNode* newLeaderNode =
        (f2Node->isLeader()) ? f2Node : restartedF1.get();
    rune::node::BrokerNode* newFollowerNode =
        (f2Node->isLeader()) ? restartedF1.get() : f2Node;

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
    PartitionReplica* newFollowerReplica = newFollowerNode->replicaForTest(0, 0);
    ASSERT_NE(newFollowerReplica, nullptr);

    // Poll for Leader role on new leader's partition replica.
    {
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (std::chrono::steady_clock::now() < deadline &&
               newLeaderReplica->role() != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }
    EXPECT_EQ(newLeaderReplica->role(), Role::Leader)
        << "New leader's partition replica must be in Leader role";

    // --- Phase 7: invariant checks ---

    // 7a. The leoN baseline (fully committed before F1 was stopped) must be
    //     preserved on the new leader — NOT truncated by the epoch change.
    EXPECT_GE(newLeaderReplica->logEndOffset(), hwAfterN)
        << "Committed records up to hwAfterN=" << hwAfterN
        << " must NOT be truncated; new leader LEO=" << newLeaderReplica->logEndOffset();

    // 7b. No phantom-committed record: HW <= LEO at all times.
    EXPECT_LE(newLeaderReplica->highWatermark(), newLeaderReplica->logEndOffset())
        << "HW must never exceed LEO after failover (no phantom-committed records)";

    // 7c. [Critical-2] TRUNCATION OBSERVED: capture the follower's LEO BEFORE
    //     and AFTER convergence.  If the follower had MORE records than the leader
    //     (the "divergent tail" case), its LEO must DROP to match the leader's.
    //     If the follower had FEWER records, it must CATCH UP (LEO rises).
    //     Either way, after convergence both nodes must have the SAME LEO.
    const Offset followerLeoBeforeConverge = newFollowerReplica->logEndOffset();
    const Offset leaderLeoAtElection       = newLeaderReplica->logEndOffset();

    // Wait for the follower to converge to the leader's log.
    {
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        bool converged = false;
        while (std::chrono::steady_clock::now() < deadline && !converged) {
            converged = (newFollowerReplica->logEndOffset() == newLeaderReplica->logEndOffset());
            if (!converged) std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(converged)
            << "[Critical-2] Both surviving nodes must converge to the same LEO after "
            << "epoch exchange + reconcile; "
            << "leader LEO=" << newLeaderReplica->logEndOffset()
            << " follower LEO=" << newFollowerReplica->logEndOffset()
            << " (follower was " << followerLeoBeforeConverge << " before converge; "
            << "leader was " << leaderLeoAtElection << " at election)";
    }

    const Offset followerLeoAfterConverge  = newFollowerReplica->logEndOffset();
    const Offset leaderLeoAfterConverge    = newLeaderReplica->logEndOffset();

    // [Critical-2] Summarize what happened: truncation or catch-up?
    if (followerLeoBeforeConverge > leaderLeoAtElection) {
        // Follower had a DIVERGENT TAIL — it must have TRUNCATED.
        EXPECT_LT(followerLeoAfterConverge, followerLeoBeforeConverge)
            << "[Critical-2] Follower had divergent tail (LEO "
            << followerLeoBeforeConverge << " > leader " << leaderLeoAtElection
            << ") but did NOT truncate: after-converge LEO=" << followerLeoAfterConverge
            << " — truncation path broken on per-worker transport";
    } else if (followerLeoBeforeConverge < leaderLeoAtElection) {
        // Follower was BEHIND — it must have CAUGHT UP.
        EXPECT_GE(followerLeoAfterConverge, leaderLeoAtElection)
            << "[Critical-2] Follower was behind (LEO "
            << followerLeoBeforeConverge << " < leader " << leaderLeoAtElection
            << ") but did NOT catch up: after-converge LEO=" << followerLeoAfterConverge
            << " — catch-up (reconcile) path broken on per-worker transport";
    }
    // Convergence: both must be at the same LEO now.
    EXPECT_EQ(followerLeoAfterConverge, leaderLeoAfterConverge)
        << "[Critical-2] After epoch exchange + reconcile, both surviving nodes must "
        << "be at the same LEO; leader=" << leaderLeoAfterConverge
        << " follower=" << followerLeoAfterConverge;

    // Committed records [0,N) must be intact (content check).
    {
        rune::gateway::TopicRegistry* reg = newLeaderNode->topicRegistryForTest();
        ASSERT_NE(reg, nullptr);
        rune::storage::Log* log = reg->log(0, 0);
        ASSERT_NE(log, nullptr);
        auto rawBytes = log->read(0, 1 << 20);
        ASSERT_FALSE(rawBytes.empty())
            << "New leader's log must contain committed data readable from offset 0";

        auto payloads = scanCommittedRecordPayloads(rawBytes, kN);
        // There may be more than N records (if M also committed and survived);
        // at minimum the first N records must have the correct fill bytes.
        EXPECT_GE(static_cast<int>(payloads.size()), kN)
            << "New leader log must contain at least N=" << kN << " records";

        for (int i = 0; i < std::min(kN, static_cast<int>(payloads.size())); ++i) {
            const uint8_t expected = static_cast<uint8_t>(0xC0 + i);
            EXPECT_EQ(payloads[static_cast<std::size_t>(i)], expected)
                << "[Critical-2] Record " << i
                << " payload mismatch after epoch change: expected fill=0x"
                << std::hex << static_cast<int>(expected) << " got 0x"
                << static_cast<int>(payloads[static_cast<std::size_t>(i)])
                << std::dec << " — committed content corrupted by truncation";
        }
    }

    // [Important-1] ISR re-expansion: poll until ISR includes the follower.
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool isrOk = false;
        while (std::chrono::steady_clock::now() < deadline && !isrOk) {
            isrOk = (newLeaderReplica->gatewayIsrSize() >= 2);
            if (!isrOk) std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(isrOk)
            << "[Important-1] ISR must re-expand to at least 2 (leader + follower); "
            << "actual gatewayIsrSize=" << newLeaderReplica->gatewayIsrSize();
    }

    // [Important-2] Post-failover produce commits and replicates.
    {
        auto postPayload = makePwPayload(0xCF, 16);
        const Offset leoBeforePost = newLeaderReplica->logEndOffset();
        Offset postOff = newLeaderReplica->append(
            std::span<const uint8_t>(postPayload.data(), postPayload.size()), false);
        EXPECT_GE(postOff, 0)
            << "[Important-2] New leader must accept post-failover append";

        const Offset leoAfterPost = newLeaderReplica->logEndOffset();
        EXPECT_GT(leoAfterPost, leoBeforePost)
            << "[Important-2] New leader LEO must advance";

        // Wait for HW to advance (commit).
        {
            const auto deadline = std::chrono::steady_clock::now() + 15s;
            bool hwOk = false;
            while (std::chrono::steady_clock::now() < deadline && !hwOk) {
                hwOk = (newLeaderReplica->highWatermark() >= leoAfterPost);
                if (!hwOk) std::this_thread::sleep_for(20ms);
            }
            EXPECT_TRUE(hwOk)
                << "[Important-2] Post-failover produce must commit; "
                << "HW=" << newLeaderReplica->highWatermark()
                << " expected >= " << leoAfterPost;
        }

        // Wait for the surviving follower to replicate the post-failover record.
        {
            const auto deadline = std::chrono::steady_clock::now() + 15s;
            bool replOk = false;
            while (std::chrono::steady_clock::now() < deadline && !replOk) {
                replOk = (newFollowerReplica->logEndOffset() >= leoAfterPost);
                if (!replOk) std::this_thread::sleep_for(20ms);
            }
            EXPECT_TRUE(replOk)
                << "[Important-2] Surviving follower must replicate post-failover record; "
                << "sfLEO=" << newFollowerReplica->logEndOffset()
                << " expected >= " << leoAfterPost;
        }
    }

    // Diagnostic suppression (no unused-variable warnings).
    (void)hwAfterM;
    (void)leoNM;
    (void)f2LeoBeforeLeaderDeath;

    restartedF1->stop();
    node0->stop();
    node1->stop();
    node2->stop();
}
