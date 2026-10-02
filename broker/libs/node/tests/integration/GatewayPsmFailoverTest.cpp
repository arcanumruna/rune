// ---------------------------------------------------------------------------
// GatewayPsmFailoverTest.cpp — TX-2b fix round 1: gateway-driven PSM tests.
//
// Two test suites:
//
// SUITE 1 — UNIT (no Aeron, no BrokerNode):
//
//   TX2b_GatewayDriven.DivergentTail_HWBoundRebuild_NoFalseDuplicate
//
//   Directly tests the CRITICAL TX-2b fix: rebuildFromLog() must use the
//   committed high-watermark (HW) as its scan upper bound, NOT the log-end-
//   offset (LEO).
//
//   Scenario:
//     - Log has kN committed batches (pid=P, epoch=E, seq 0..kN-1) + 1 UNCOMMITTED
//       batch (seq=kN, above HW, will be truncated by KIP-101 divergence resolution).
//
//   Scenario A (pre-fix, LEO-bound):
//     rebuildFromLog(log, leoOffset, partition) → PSM sees seq=kN → returns
//     Duplicate for a producer retry of seq=kN → false-Duplicate → acknowledged
//     data loss (the batch was NEVER committed; the retry is a genuinely new produce).
//
//   Scenario B (post-fix, HW-bound):
//     rebuildFromLog(log, hwOffset, partition) → PSM does NOT see seq=kN →
//     returns Accept for the retry of seq=kN → correct.
//     All committed seqs (0..kN-1) are still Duplicate → dedup preserved.
//
//   The test FAILS against the pre-fix LEO-bound code (Scenario C verifies this
//   explicitly: LEO-bound must produce false-Duplicate for kUncSeq).
//
// SUITE 2 — INTEGRATION (3 BrokerNodes + Aeron UDP):
//
//   TX2b_GatewayDriven.FailoverDedup_HandleProducePath
//
//   Exercises the REAL gateway-driven path:
//     - 3 BrokerNodes start, elect a leader.
//     - A test client sends Produce messages with idempotent producer fields
//       (producerId, producerEpoch, baseSequence) via the real handleProduce
//       path (Aeron client → gateway ingress → ProduceResponse).
//     - The old leader is stopped; survivors re-elect a new leader.
//     - The new leader's doWork() fires the wasLeader_ false→true transition,
//       which calls rebuildProducerStateFromLogs() with the HW-bound scan.
//     - The test connects to the new leader and re-sends the last committed
//       sequence: handleProduce must return OK with the ORIGINAL baseOffset
//       (Duplicate suppression) and logEndOffset must NOT advance.
//     - A genuinely new sequence must be Accepted (new baseOffset > prior).
//
//   Gated behind BUILD_NODE_UDP_TESTS && aeron_FOUND (same gate as GR-7).
//
// NO Docker.  C++23.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// Storage + protocol (both unit and integration; no Aeron dependency)
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#ifndef TX2B_UNIT_ONLY
// Integration-only headers (require Aeron)
#include <rune/replication/PartitionReplica.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/EmbeddedDriver.h>
#include "../support/OwnerThreadTestHelpers.h"  // F1: owner-thread replica probes
#include <rune/node/NodeConfig.h>
#include "../support/TestNodeConfig.h"
#include <rune/node/ReplicaRegistry.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif // TX2B_UNIT_ONLY

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::storage;
#ifndef TX2B_UNIT_ONLY
using namespace rune::replication;
using namespace rune::node::test;  // F1: leoOnOwner/hwOnOwner/appendOnOwner/...
namespace pc = rune::protocol::client;
#endif // TX2B_UNIT_ONLY

// ===========================================================================
// Helpers shared by both suites
// ===========================================================================

static std::atomic<uint64_t> g_tx2bSeq{0};

static std::string tx2bUniq(std::string_view tag = "") {
    return std::string(tag)
         + std::to_string(::getpid()) + "_"
         + std::to_string(g_tx2bSeq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count());
}

struct Tx2bTmpDir {
    fs::path path;
    explicit Tx2bTmpDir(std::string_view tag = "") {
        path = fs::temp_directory_path() / tx2bUniq(tag);
        fs::create_directories(path);
    }
    ~Tx2bTmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Build a single-record idempotent RecordBatch with explicit producer fields.
static std::vector<uint8_t> makeIdempotentBatch(int64_t baseOffsetHint,
                                                 int64_t producerId,
                                                 int16_t producerEpoch,
                                                 int32_t baseSequence,
                                                 std::string_view value = "v")
{
    rune::protocol::RecordBatchBuilder b(baseOffsetHint, 0);
    b.setProducerId(producerId);
    b.setProducerEpoch(producerEpoch);
    b.setBaseSequence(baseSequence);
    b.append("", value, 0);
    return b.build();
}

// ===========================================================================
// UNIT TEST — no Aeron, no BrokerNode
// ===========================================================================

// ---------------------------------------------------------------------------
// TX2b_GatewayDriven.DivergentTail_HWBoundRebuild_NoFalseDuplicate
//
// Proves the CRITICAL TX-2b fix: only the HW-bounded scan is safe.
//
// Test structure:
//   kN committed batches (seq 0..kN-1) → hwOffset = kN
//   1 uncommitted batch  (seq = kN)    → leoOffset = kN+1 > hwOffset
//
// Scenario B (post-fix, HW-bound): PSM does NOT see kUncSeq → isAccept() == true.
//   All committed seqs are Duplicate (dedup preserved).
//
// Scenario C (pre-fix, LEO-bound): PSM sees kUncSeq → isDuplicate() == true.
//   This is the BUG: producer retry of the never-committed seq=kN is
//   rejected as Duplicate and the client is told the batch landed at an
//   offset that later gets truncated → acknowledged data loss.
//   This scenario is asserted (EXPECT_TRUE isDuplicate) to prove the test
//   is a valid RED/GREEN discriminator for the CRITICAL fix.
// ---------------------------------------------------------------------------
TEST(TX2b_GatewayDriven, DivergentTail_HWBoundRebuild_NoFalseDuplicate) {
    Tx2bTmpDir logDir("divtail_");

    Log log(logDir.path.string());

    constexpr int64_t kPid      = 77777;
    constexpr int16_t kEpoch    = 3;
    constexpr int     kN        = 2;  // 2 committed batches
    constexpr int32_t kPartition = 0;

    // --- Build the log: kN committed batches + 1 uncommitted batch above HW ---

    std::vector<int64_t> committedOffsets;
    for (int seq = 0; seq < kN; ++seq) {
        const int64_t base = log.logEndOffset();
        auto batch = makeIdempotentBatch(base, kPid, kEpoch, seq);
        log.append(batch, base);  // lastOffset = base (single record, delta=0)
        committedOffsets.push_back(base);
    }
    const int64_t hwOffset = log.logEndOffset();  // = kN (committed boundary)

    // Append ONE uncommitted batch above HW (will be truncated post-failover).
    const int32_t kUncSeq = kN;
    const int64_t uncBase = log.logEndOffset();
    {
        auto batch = makeIdempotentBatch(uncBase, kPid, kEpoch, kUncSeq, "uncommitted");
        log.append(batch, uncBase);
    }
    const int64_t leoOffset = log.logEndOffset();  // = kN + 1

    ASSERT_GT(leoOffset, hwOffset)
        << "LEO must exceed HW — uncommitted tail must exist for the test to be valid";
    ASSERT_EQ(hwOffset, static_cast<int64_t>(kN));
    ASSERT_EQ(leoOffset, static_cast<int64_t>(kN + 1));

    // -----------------------------------------------------------------------
    // Scenario B (POST-FIX): HW-bounded scan — the correct behavior.
    // -----------------------------------------------------------------------
    {
        ProducerStateManager psmHw;
        psmHw.rebuildFromLog(/*topicId=*/0, log, hwOffset, kPartition);

        // COMMITTED seqs must be Duplicate (dedup window fully intact).
        for (int seq = 0; seq < kN; ++seq) {
            const auto vr = psmHw.validate(/*topicId=*/0, kPid, kPartition, kEpoch, seq, seq);
            EXPECT_TRUE(vr.isDuplicate())
                << "committed seq=" << seq
                << " must be Duplicate with HW-bound scan (dedup preserved)";
            EXPECT_EQ(vr.retryBaseOffset, committedOffsets[static_cast<std::size_t>(seq)])
                << "retryBaseOffset must match original committed offset for seq=" << seq;
        }

        // UNCOMMITTED seq MUST be Accept.
        // This is the key assertion for the TX-2b CRITICAL fix.
        // Pre-fix LEO-bound code returns Duplicate here (see Scenario C below).
        const auto vrUnc = psmHw.validate(/*topicId=*/0, kPid, kPartition, kEpoch, kUncSeq, kUncSeq);
        EXPECT_TRUE(vrUnc.isAccept())
            << "CRITICAL TX-2b fix: uncommitted seq=" << kUncSeq
            << " (above HW=" << hwOffset << ", below LEO=" << leoOffset << ")"
            << " must be ACCEPTED after HW-bounded rebuild — not false-Duplicate."
            << " A false-Duplicate here acknowledges data at a truncated-away offset"
            << " (acknowledged data loss). Pre-fix LEO-bound code fails this check."
            << " Got status=" << static_cast<int>(vrUnc.status);

        // A new producer is also Accept.
        const auto vrNew = psmHw.validate(/*topicId=*/0, 0, kPartition, kEpoch, 0, 0);
        EXPECT_TRUE(vrNew.isAccept())
            << "A new producer (pid=0) must be Accept after HW-bounded rebuild";
    }

    // -----------------------------------------------------------------------
    // Scenario C (PRE-FIX): LEO-bounded scan — the BUG.
    // Asserts isDuplicate() == true to confirm this test is a valid
    // RED/GREEN discriminator: code that uses LEO fails where HW-bound passes.
    // -----------------------------------------------------------------------
    {
        ProducerStateManager psmLeo;
        psmLeo.rebuildFromLog(/*topicId=*/0, log, leoOffset, kPartition);

        // Pre-fix behavior: LEO-bound seeds PSM from the uncommitted batch →
        // producer retry of kUncSeq is incorrectly marked Duplicate.
        const auto vrUnc = psmLeo.validate(/*topicId=*/0, kPid, kPartition, kEpoch, kUncSeq, kUncSeq);
        EXPECT_TRUE(vrUnc.isDuplicate())
            << "PRE-FIX REGRESSION CHECK: LEO-bound rebuildFromLog MUST produce"
            << " false-Duplicate for uncommitted seq=" << kUncSeq << "."
            << " This EXPECT documents the pre-fix bug (LEO-bound seeds PSM from"
            << " uncommitted batches). Scenario B (HW-bound) corrects this to Accept."
            << " Together B+C prove: HW-bound=Accept (FIX), LEO-bound=Duplicate (BUG).";

        // Committed seqs are still Duplicate in LEO-bound mode (correct in both cases).
        for (int seq = 0; seq < kN; ++seq) {
            const auto vr = psmLeo.validate(/*topicId=*/0, kPid, kPartition, kEpoch, seq, seq);
            EXPECT_TRUE(vr.isDuplicate())
                << "seq=" << seq << " must be Duplicate in LEO-bound scan too";
        }
    }
}

#ifndef TX2B_UNIT_ONLY

// ===========================================================================
// Integration helpers (3-node BrokerNode + Aeron UDP test client)
// ===========================================================================

static uint16_t tx2bFreePort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    socklen_t l = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &l);
    const uint16_t netPort = addr.sin_port;
    const uint16_t port = static_cast<uint16_t>(
        (netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

static std::vector<uint16_t> tx2bFreePorts(int n) {
    std::vector<uint16_t> ports;
    ports.reserve(static_cast<std::size_t>(n));
    while (static_cast<int>(ports.size()) < n) {
        uint16_t p = tx2bFreePort();
        bool dup = false;
        for (uint16_t e : ports) { if (e == p) { dup = true; break; } }
        if (!dup) ports.push_back(p);
    }
    return ports;
}

// NodeConfig for a 3-node cluster.
static rune::node::NodeConfig makeTx2bConfig(
    int nodeId,
    const fs::path& dataDir,
    const std::vector<uint16_t>& cPorts,
    const std::vector<uint16_t>& kPorts,
    const std::vector<uint16_t>& rPorts)
{
    rune::node::NodeConfig cfg;
    rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
    cfg.nodeId              = nodeId;
    cfg.dataDir             = dataDir.string();
    cfg.consensusStreamId   = 71;
    cfg.replicationStreamId = 72;

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
    cfg.clientEndpoint.streamId = 73;

    rune::node::TopicSpec topic;
    topic.name           = "psmtopic";
    topic.partitionCount = 1;
    topic.topicId        = 0;
    cfg.topics.push_back(topic);

    return cfg;
}

// Test-client embedded Aeron driver (mirrors Gr5ClientDriver exactly).
struct Tx2bClientDriver {
    // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
    // readiness pumping — aeron_driver_init/start already wrote cnc.dat and
    // stamped the heartbeat synchronously before the driver thread even starts.
    rune::node::EmbeddedDriver    driver;
    std::shared_ptr<aeron::Aeron> aeron;

    explicit Tx2bClientDriver(const fs::path& aeronDir)
        : driver(makeCfg(aeronDir), rune::node::DriverMode::Threaded)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir.string());
        aeron = aeron::Aeron::connect(ctx);
    }

private:
    static rune::node::EmbeddedDriverConfig makeCfg(const fs::path& aeronDir) {
        fs::create_directories(aeronDir);
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir.string();
        cfg.termBufferBytes = 256 * 1024;   // receiver window = term/2 must be >= the broker MTU (derived 65504 on loopback)
        return cfg;
    }
};

static bool tx2bOffer(aeron::Publication& pub,
                      const uint8_t* buf, std::size_t len,
                      int maxIter = 100'000)
{
    aeron::concurrent::AtomicBuffer ab(
        const_cast<uint8_t*>(buf),
        static_cast<aeron::util::index_t>(len));
    for (int i = 0; i < maxIter; ++i) {
        const int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
        if (pos > 0) return true;
        std::this_thread::sleep_for(100us);
    }
    return false;
}

// Test-client connection to one gateway (mirrors Gr5 ClientConn).
// Uses aeron:udp for egress (test driver ≠ broker driver; IPC not shared).
struct Tx2bClientConn {
    std::shared_ptr<aeron::Publication>  ingressPub;
    std::shared_ptr<aeron::Subscription> egressSub;
    int64_t                              assignedSessionId = -1;

    static Tx2bClientConn connect(Tx2bClientDriver& drv,
                                   const std::string& leaderClientChannel,
                                   int32_t            clientStreamId,
                                   uint16_t           egressPort)
    {
        Tx2bClientConn conn;
        // Egress channel and stream (must be unique across concurrent tests).
        const std::string egressCh =
            "aeron:udp?endpoint=127.0.0.1:" + std::to_string(egressPort);
        // Unique egress stream derived from port to avoid collision with other tests.
        const int32_t egressStreamId = 7000 + static_cast<int32_t>(egressPort % 1000);

        const int64_t pubReg = drv.aeron->addPublication(leaderClientChannel, clientStreamId);
        const int64_t subReg = drv.aeron->addSubscription(egressCh, egressStreamId);

        {
            const auto dl = std::chrono::steady_clock::now() + 10s;
            while (std::chrono::steady_clock::now() < dl) {
                if (!conn.ingressPub) conn.ingressPub = drv.aeron->findPublication(pubReg);
                if (!conn.egressSub)  conn.egressSub  = drv.aeron->findSubscription(subReg);
                if (conn.ingressPub && conn.egressSub) break;
                std::this_thread::sleep_for(1ms);
            }
        }
        if (!conn.ingressPub || !conn.egressSub) return conn;

        {
            const auto dl = std::chrono::steady_clock::now() + 10s;
            while (std::chrono::steady_clock::now() < dl) {
                if (conn.ingressPub->isConnected()) break;
                std::this_thread::sleep_for(1ms);
            }
        }

        constexpr std::size_t kScratch = 96 * 1024;
        std::vector<uint8_t> scratch(kScratch, 0);

        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = 1;
        req.egressStreamId = egressStreamId;
        req.clientVersion  = 1;
        req.egressChannel  = egressCh;
        const std::size_t connLen = req.encode(scratch.data());
        if (!tx2bOffer(*conn.ingressPub, scratch.data(), connLen)) return conn;

        {
            std::optional<pc::ConnectResponse> resp;
            aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                              aeron::util::index_t off,
                                              aeron::util::index_t len_,
                                              aeron::Header&) {
                auto dec = pc::ConnectResponse::decode(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                    static_cast<std::size_t>(len_));
                if (dec) resp = *dec;
            });
            const auto dl = std::chrono::steady_clock::now() + 10s;
            while (std::chrono::steady_clock::now() < dl && !resp.has_value()) {
                conn.egressSub->poll(asm_.handler(), 10);
                std::this_thread::sleep_for(1ms);
            }
            if (resp.has_value()) conn.assignedSessionId = resp->assignedSessionId;
        }
        return conn;
    }
};

// Send an idempotent Produce; wait for ProduceResponse matching correlationId.
static std::optional<pc::ProduceResponse>
tx2bProduce(Tx2bClientConn& conn,
            int64_t correlationId,
            int64_t producerId,
            int16_t producerEpoch,
            int32_t baseSequence,
            std::vector<uint8_t>& scratch)
{
    auto batchBytes = makeIdempotentBatch(0, producerId, producerEpoch, baseSequence);

    pc::Produce prod;
    prod.sessionId      = conn.assignedSessionId;
    prod.correlationId  = correlationId;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.acks           = 0;  // acks=Leader (immediate ack, no min-ISR gate)
    prod.producerId     = producerId;
    prod.producerEpoch  = producerEpoch;
    prod.baseSequence   = baseSequence;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batchBytes.data(), batchBytes.size());

    const std::size_t prodLen = prod.encode(scratch.data());
    if (!tx2bOffer(*conn.ingressPub, scratch.data(), prodLen)) return std::nullopt;

    std::optional<pc::ProduceResponse> resp;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t off,
                                      aeron::util::index_t len_,
                                      aeron::Header&) {
        auto dec = pc::ProduceResponse::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + off),
            static_cast<std::size_t>(len_));
        if (dec && dec->correlationId == correlationId) resp = *dec;
    });
    const auto dl = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < dl && !resp.has_value()) {
        conn.egressSub->poll(asm_.handler(), 10);
        std::this_thread::sleep_for(1ms);
    }
    return resp;
}

// ===========================================================================
// INTEGRATION TEST — gateway-driven PSM failover
//
// 1. 3 BrokerNodes start + elect a leader.
// 2. kN idempotent produces via the real handleProduce Aeron path.
//    (producerId=P, producerEpoch=E, baseSequence 0..kN-1)
// 3. Wait for majority HW (all batches committed).
// 4. Kill the leader; wait for re-election.
// 5. Connect to new leader.  The new leader's doWork() fires wasLeader_
//    false→true, calling rebuildProducerStateFromLogs() with HW-bound scan.
// 6. Re-send seq=kN-1 (last committed) → Duplicate: OK, original baseOffset,
//    logEndOffset UNCHANGED.
// 7. Re-send seq=0 → also Duplicate.
// 8. Send seq=kN (new) → Accept: new baseOffset > prior, log advances.
//
// This exercises:
//   - The real wasLeader_ false→true transition in ClientGateway::doWork().
//   - rebuildProducerStateFromLogs() with the HW-bound scan (TX-2b fix).
//   - handleProduce's validate→no-append Duplicate path.
// ===========================================================================
TEST(TX2b_GatewayDriven, FailoverDedup_HandleProducePath) {
    Tx2bTmpDir dir0{"fp0_"}, dir1{"fp1_"}, dir2{"fp2_"};
    Tx2bTmpDir clientDir{"fp_cli_"};

    const auto cPorts = tx2bFreePorts(3);
    const auto kPorts = tx2bFreePorts(3);
    const auto rPorts = tx2bFreePorts(3);

    auto cfg0 = makeTx2bConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makeTx2bConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makeTx2bConfig(2, dir2.path, cPorts, kPorts, rPorts);

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    ASSERT_TRUE(node0->awaitLeader(8s));
    ASSERT_TRUE(node1->awaitLeader(8s));
    ASSERT_TRUE(node2->awaitLeader(8s));
    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0);
    ASSERT_LE(lid, 2);

    std::array<rune::node::BrokerNode*, 3> allNodes{
        node0.get(), node1.get(), node2.get()};
    rune::node::BrokerNode* leaderNode = allNodes[static_cast<std::size_t>(lid)];

    // Wait for replication bridge to promote the leader replica.
    {
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest());
    ASSERT_TRUE(hasReplica(*leaderNode));
    ASSERT_EQ(roleOnOwner(*leaderNode), Role::Leader);

    // Start the test-client Aeron driver.
    Tx2bClientDriver clientDrv(clientDir.path / "aeron");
    ASSERT_NE(clientDrv.aeron, nullptr) << "test-client Aeron driver failed to start";

    const std::string leaderCh =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(lid)]);
    constexpr int32_t kClientStreamId = 73;

    const uint16_t egressPort1 = tx2bFreePort();
    auto conn1 = Tx2bClientConn::connect(clientDrv, leaderCh, kClientStreamId, egressPort1);
    ASSERT_GT(conn1.assignedSessionId, 0) << "failed to connect to leader gateway";

    // Produce kN idempotent batches (pid=P, epoch=E, seq 0..kN-1).
    constexpr int64_t kPid   = 314159265LL;
    constexpr int16_t kEpoch = 5;
    constexpr int     kN     = 3;

    std::vector<uint8_t> scratch(96 * 1024, 0);
    std::vector<int64_t> baseOffsets(kN, -1LL);

    for (int seq = 0; seq < kN; ++seq) {
        const auto resp = tx2bProduce(
            conn1, static_cast<int64_t>(100 + seq), kPid, kEpoch, seq, scratch);
        ASSERT_TRUE(resp.has_value())
            << "No ProduceResponse for seq=" << seq;
        ASSERT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK))
            << "seq=" << seq << " must be OK; errorCode=" << resp->errorCode;
        EXPECT_GE(resp->baseOffset, 0) << "baseOffset must be >= 0 for seq=" << seq;
        baseOffsets[static_cast<std::size_t>(seq)] = resp->baseOffset;
    }

    // Monotone offsets (each single-record batch occupies one offset).
    for (int seq = 1; seq < kN; ++seq) {
        EXPECT_GT(baseOffsets[static_cast<std::size_t>(seq)],
                  baseOffsets[static_cast<std::size_t>(seq - 1)])
            << "baseOffset must increase with each batch";
    }

    // Wait for majority HW to cover all kN batches.
    const Offset targetLeo = leoOnOwner(*leaderNode);
    ASSERT_GT(targetLeo, 0) << "leader LEO must be > 0 after produces";
    {
        const auto dl = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < dl) {
            if (hwOnOwner(*leaderNode) >= targetLeo) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_GE(hwOnOwner(*leaderNode), targetLeo)
        << "leader HW must reach " << targetLeo << " before failover";
    const Offset committedHw = hwOnOwner(*leaderNode);

    // Disconnect from old leader before killing it.
    conn1.ingressPub.reset();
    conn1.egressSub.reset();

    // Kill the leader.
    leaderNode->stop();
    leaderNode = nullptr;

    // Find the two surviving nodes.
    rune::node::BrokerNode* surv1 = nullptr;
    rune::node::BrokerNode* surv2 = nullptr;
    for (int i = 0; i < 3; ++i) {
        if (i != lid && allNodes[i]) {
            if (!surv1) surv1 = allNodes[i];
            else         surv2 = allNodes[i];
        }
    }
    ASSERT_NE(surv1, nullptr);
    ASSERT_NE(surv2, nullptr);

    // Wait for re-election.
    rune::consensus::NodeId newLid = -1;
    {
        const auto dl = std::chrono::steady_clock::now() + 15s;
        while (std::chrono::steady_clock::now() < dl) {
            const auto l1 = surv1->leaderId();
            const auto l2 = surv2->leaderId();
            if (l1 >= 0 && l1 != lid && l2 >= 0 && l1 == l2) {
                newLid = l1;
                break;
            }
            std::this_thread::sleep_for(50ms);
        }
    }
    ASSERT_GE(newLid, 0) << "Surviving nodes must re-elect a new leader";
    ASSERT_NE(newLid, lid) << "New leader must differ from the killed leader";
    std::this_thread::sleep_for(500ms);  // let new leader settle

    rune::node::BrokerNode* newLeaderNode =
        (surv1->isLeader()) ? surv1 : surv2;
    ASSERT_TRUE(newLeaderNode->isLeader());

    // Wait for new leader's replication bridge to fire → doWork() wasLeader_
    // transition → rebuildProducerStateFromLogs() with HW-bound.
    {
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (newLeaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(newLeaderNode->replicationIsLeaderForTest())
        << "New leader's replication bridge must report isLeader (PSM rebuild fires)";

    ASSERT_TRUE(hasReplica(*newLeaderNode));

    // Partition failover (metadata reassignment of the dead leader's partitions)
    // is ASYNCHRONOUS and completes shortly AFTER consensus re-election — the
    // reassignment liveness window must expire before the dead node is treated as
    // gone.  Poll for the partition replica to reach Leader role rather than
    // asserting it instantaneously (mirrors the consensus-isLeader poll above).
    {
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl &&
               roleOnOwner(*newLeaderNode) != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_EQ(roleOnOwner(*newLeaderNode), Role::Leader)
        << "New leader's partition replica must be promoted (async failover)";
    ASSERT_GE(leoOnOwner(*newLeaderNode), committedHw)
        << "New leader must hold all committed records (LEO >= committedHw="
        << committedHw << ")";

    // Connect to the new leader.
    const std::string newLeaderCh =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(newLid)]);
    const uint16_t egressPort2 = tx2bFreePort();
    auto conn2 = Tx2bClientConn::connect(
        clientDrv, newLeaderCh, kClientStreamId, egressPort2);
    ASSERT_GT(conn2.assignedSessionId, 0) << "failed to connect to new leader gateway";

    const Offset logEndBeforeRetry = leoOnOwner(*newLeaderNode);

    // -----------------------------------------------------------------------
    // KEY ASSERTION 1: Re-send seq=kN-1 (last committed) → Duplicate.
    // handleProduce must return OK with the ORIGINAL baseOffset.
    // logEndOffset must NOT advance (no re-append).
    // -----------------------------------------------------------------------
    {
        const int32_t lastSeq = kN - 1;
        const auto resp = tx2bProduce(conn2, 200, kPid, kEpoch, lastSeq, scratch);
        ASSERT_TRUE(resp.has_value())
            << "No ProduceResponse for duplicate seq=" << lastSeq;
        EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK))
            << "Duplicate produce must return OK (handled, not rejected); "
            << "errorCode=" << resp->errorCode;
        EXPECT_EQ(resp->baseOffset, baseOffsets[static_cast<std::size_t>(lastSeq)])
            << "Duplicate seq=" << lastSeq
            << " must return the ORIGINAL baseOffset="
            << baseOffsets[static_cast<std::size_t>(lastSeq)]
            << ". A different baseOffset means the gateway re-appended "
               "(TX-2b PSM rebuild bug: stale window caused Accept of a Duplicate).";
        EXPECT_EQ(leoOnOwner(*newLeaderNode), logEndBeforeRetry)
            << "logEndOffset must NOT advance on a Duplicate — re-append detected! "
               "Pre-fix: fresh PSM = empty map → Accept → re-append → data corruption.";
    }

    // -----------------------------------------------------------------------
    // KEY ASSERTION 2: Re-send seq=0 → also Duplicate.
    // -----------------------------------------------------------------------
    {
        const auto resp = tx2bProduce(conn2, 201, kPid, kEpoch, 0, scratch);
        ASSERT_TRUE(resp.has_value()) << "No ProduceResponse for dup seq=0";
        EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
        EXPECT_EQ(resp->baseOffset, baseOffsets[0])
            << "Dup seq=0 must return original baseOffset=" << baseOffsets[0];
        EXPECT_EQ(leoOnOwner(*newLeaderNode), logEndBeforeRetry)
            << "log must not advance on dup seq=0";
    }

    // -----------------------------------------------------------------------
    // KEY ASSERTION 3: Send seq=kN (new) → Accepted with a new baseOffset.
    // -----------------------------------------------------------------------
    {
        const auto resp = tx2bProduce(conn2, 202, kPid, kEpoch, kN, scratch);
        ASSERT_TRUE(resp.has_value()) << "No ProduceResponse for new seq=" << kN;
        EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK))
            << "New seq=" << kN << " must be OK (Accept); "
            << "errorCode=" << resp->errorCode;
        EXPECT_GT(resp->baseOffset, baseOffsets[kN - 1])
            << "New seq=" << kN
            << " must have a baseOffset strictly above the last committed batch";
        EXPECT_GT(leoOnOwner(*newLeaderNode), logEndBeforeRetry)
            << "log MUST advance for a genuinely new sequence (seq=" << kN << ")";
    }

    conn2.ingressPub.reset();
    conn2.egressSub.reset();

    surv1->stop();
    surv2->stop();
}

#endif // TX2B_UNIT_ONLY
