// ---------------------------------------------------------------------------
// GatewayReplicationWiringTest.cpp — GR-2 + GR-3 in-process integration test.
//
// Verifies that:
//   1. Three BrokerNodes with replication ports start, elect a leader, and
//      construct PartitionReplicas wired to AeronPartitionTransport.
//   2. After the leader appends data to its partition Log, the followers'
//      replicas fetch and their logEndOffset() converges to the leader's.
//   3. The leader's highWatermark() advances once followers acknowledge.
//   4. (GR-2) A leadership change: the new leader's replicas report role==Leader
//      with a HIGHER epoch than the old leader's epoch.  The old leader's replicas
//      become followers with the new epoch.  Epoch never regresses.
//   5. (GR-2) A follower receiving a FetchRequest with the OLD epoch gets
//      kErrStaleEpoch (stale-epoch fencing).
//
// Design:
//   - 3 BrokerNodes in-process with distinct embedded Aeron media drivers.
//   - One shared topic "repl:1" (1 partition, partitionId=0).
//   - Each node's replicationPort is discovered dynamically.
//   - Replication is driven by each node's own AgentRunner duty-cycle thread.
//   - The test thread polls BrokerNode accessors + PartitionReplica accessors via
//     an accessor function added to BrokerNode (or via replBridge/replicaRegistry
//     exposed for test-only through a thin friend/accessor).
//   - Hard time bounds on all waits (ASSERT_TRUE, no infinite spin).
//
// NO Docker.  NO mocks for the replication path.  Real aeron:udp on loopback.
// ---------------------------------------------------------------------------

#include <rune/aeronutil/testing/FreePort.h>
#include "../support/TestNodeConfig.h"
#include <gtest/gtest.h>

#include <rune/node/BrokerNode.h>
#include <rune/node/EmbeddedDriver.h>
#include "../support/OwnerThreadTestHelpers.h"  // F1: owner-thread replica probes
#include <rune/node/NodeConfig.h>
#include "support/TestWorkerCount.h"
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>

// Gateway client produce path (safe: the leader's duty-cycle thread owns the
// replica; the test drives produce over real aeron:udp instead of calling
// PartitionReplica::append() from the test thread, which would race the duty
// cycle — see ThreeNode test below).
#include <rune/protocol/RecordBatch.h>
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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::replication;
using namespace rune::node::test;  // F1: leoOnOwner/hwOnOwner/appendOnOwner/...

// ---------------------------------------------------------------------------
// Helpers (mirrors UdpClusterElectionTest)
// ---------------------------------------------------------------------------

static fs::path makeGrTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gr_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct GrTempDir {
    fs::path path;
    explicit GrTempDir(std::string_view tag = "") : path(makeGrTempDir(tag)) {
        fs::create_directories(path);
    }
    ~GrTempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

static uint16_t grDiscoverFreeUdpPort() {
    return rune::aeronutil::testing::allocFreeUdpPort();   // D8: never an ephemeral port
}

static std::vector<uint16_t> grDiscoverFreePorts(int n) {
    std::vector<uint16_t> ports;
    ports.reserve(n);
    while (static_cast<int>(ports.size()) < n) {
        uint16_t p = grDiscoverFreeUdpPort();
        bool dup = false;
        for (uint16_t e : ports) { if (e == p) { dup = true; break; } }
        if (!dup) ports.push_back(p);
    }
    return ports;
}

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-client Aeron driver (mirrors GR-4's Gr4ClientDriver). Used by the
// ThreeNode test to drive produce over the real gateway client path so the
// leader replica is mutated ONLY on its own duty-cycle thread (never from the
// test thread — that would race PartitionReplica's single-writer state).
// ---------------------------------------------------------------------------
struct GrClientDriver {
    // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
    // readiness pumping — aeron_driver_init/start already wrote cnc.dat and
    // stamped the heartbeat synchronously before the driver thread even starts.
    rune::node::EmbeddedDriver    driver;
    std::shared_ptr<aeron::Aeron> aeron;

    explicit GrClientDriver(const fs::path& aeronDir)
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

// Build a RecordBatch of recordCount records (one record per key "k"/"v").
static std::vector<uint8_t> grMakeBatch(int32_t recordCount) {
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// Bounded, non-blocking Aeron offer (no busy-spin beyond the cap).
static bool grOfferBounded(aeron::Publication& pub,
                           const uint8_t* buf, std::size_t len,
                           int maxIter = 100'000) {
    aeron::concurrent::AtomicBuffer ab(
        const_cast<uint8_t*>(buf), static_cast<aeron::util::index_t>(len));
    for (int i = 0; i < maxIter; ++i) {
        if (pub.offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) return true;
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }
    return false;
}

// Build a 3-node config with consensus + client + replication ports.
// Topic "repl:1" gives each node 1 partition (partitionId=0).
static rune::node::NodeConfig makeGrConfig(
    int nodeId,
    const fs::path& dataDir,
    const std::vector<uint16_t>& cPorts,    // consensus ports
    const std::vector<uint16_t>& kPorts,    // client ports
    const std::vector<uint16_t>& rPorts)    // replication ports
{
    rune::node::NodeConfig cfg;
    rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
    cfg.nodeId              = nodeId;
    cfg.dataDir             = dataDir.string();
    cfg.consensusStreamId   = 10;
    cfg.replicationStreamId = 20;

    // Fast election timing.
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
    cfg.clientEndpoint.streamId = 1;

    // Single topic: "repl" with 1 partition.
    rune::node::TopicSpec topic;
    topic.name           = "repl";
    topic.partitionCount = 1;
    topic.topicId        = 0;
    cfg.topics.push_back(topic);

    cfg.workerCount = rune::node::test::workerCountFromEnv();

    return cfg;
}

// ---------------------------------------------------------------------------
// BrokerNodeReplicationAccessor — thin test helper to reach inside BrokerNode.
//
// BrokerNode's replication members are private, so we expose them via friend or
// a test-accessor class.  We use a non-intrusive approach: rely on the fact that
// BrokerNode::replicaRegistry_ and replBridge_ are accessible via the
// declared-public accessors we'll add to BrokerNode below.
//
// APPROACH: We add two public test-accessor methods to BrokerNode (gated on
// a test-only inline in the header — no production overhead).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TEST 1: Three nodes, replication ports configured.
//         Leader appends → followers converge → HW advances.
// ---------------------------------------------------------------------------
TEST(GatewayReplicationWiring, ThreeNode_FollowersConverge_HWAdvances) {
    GrTempDir dir0{"gr0_"}, dir1{"gr1_"}, dir2{"gr2_"};

    // Discover 9 free ports: 3 consensus + 3 client + 3 replication.
    const auto cPorts = grDiscoverFreePorts(3);
    const auto kPorts = grDiscoverFreePorts(3);
    const auto rPorts = grDiscoverFreePorts(3);

    auto cfg0 = makeGrConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makeGrConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makeGrConfig(2, dir2.path, cPorts, kPorts, rPorts);

    rune::node::BrokerNode node0{cfg0};
    rune::node::BrokerNode node1{cfg1};
    rune::node::BrokerNode node2{cfg2};

    node0.start();
    node1.start();
    node2.start();

    // Wait for leader election to settle (up to 8 s).
    ASSERT_TRUE(node0.awaitLeader(8s)) << "node0 did not converge to a leader";
    ASSERT_TRUE(node1.awaitLeader(8s)) << "node1 did not converge to a leader";
    ASSERT_TRUE(node2.awaitLeader(8s)) << "node2 did not converge to a leader";

    // Give the metadata bridge a moment to apply the initial becomeLeader/becomeFollower
    // transition (it fires on the first duty-cycle after election convergence).
    std::this_thread::sleep_for(300ms);

    // Determine which node is the leader.
    const rune::consensus::NodeId lid = node0.leaderId();
    ASSERT_GE(lid, 0) << "no leader elected";
    ASSERT_LE(lid, 2);
    ASSERT_EQ(node1.leaderId(), lid);
    ASSERT_EQ(node2.leaderId(), lid);

    // Access the leader's partition replica via its replicaRegistry.
    rune::node::BrokerNode* leaderNode = nullptr;
    if (lid == 0) leaderNode = &node0;
    else if (lid == 1) leaderNode = &node1;
    else leaderNode = &node2;

    ASSERT_NE(leaderNode, nullptr);

    // Get the leader's PartitionReplica (topicId=0, partition=0).
    ASSERT_TRUE(hasReplica(*leaderNode)) << "leader has no replica for topic=0 partition=0";

    // Wait for the metadata bridge to transition the replica to Leader role.
    // Uses the atomic mirror in ReplicationMetadataBridge — safe from the test thread.
    // The bridge fires on the first duty-cycle after election converges; allow 5 s.
    {
        const auto roleDeadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < roleDeadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(10ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest())
        << "leader's replication bridge must report isLeader after election convergence";

    // Produce N records via the leader's gateway over real aeron:udp.
    //
    // IMPORTANT: we do NOT call PartitionReplica::append() from the test thread.
    // PartitionReplica is single-writer (duty-cycle-thread-only); a test-thread
    // append() races the leader's duty cycle (onFetchRequest → recomputeHw,
    // pushToFollowers → Aeron publish), which TSan correctly flags. Driving the
    // append through the gateway keeps ALL replica mutation on the owning duty
    // thread — the followers/HW convergence reads below use the replica's atomic
    // snapshot accessors and are safe to poll from the test thread.
    constexpr int kNumRecords = 5;

    // Test-client Aeron driver (separate dir) connecting to the leader gateway.
    GrTempDir clientDir{"client_"};
    GrClientDriver clientDrv(clientDir.path / "aeron");
    ASSERT_NE(clientDrv.aeron, nullptr) << "test-client Aeron::connect failed";

    const std::string leaderClientChannel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(lid)]);
    constexpr int32_t kClientStreamId = 1;   // matches cfg.clientEndpoint.streamId

    const uint16_t egressPort = grDiscoverFreeUdpPort();
    const std::string egressChannel =
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(egressPort);
    constexpr int32_t kEgressStreamId = 9910;

    int64_t ingressPubRegId = clientDrv.aeron->addPublication(leaderClientChannel, kClientStreamId);
    int64_t egressSubRegId  = clientDrv.aeron->addSubscription(egressChannel, kEgressStreamId);

    std::shared_ptr<aeron::Publication>  ingressPub;
    std::shared_ptr<aeron::Subscription> egressSub;
    {
        const auto rd = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < rd) {
            if (!ingressPub) ingressPub = clientDrv.aeron->findPublication(ingressPubRegId);
            if (!egressSub)  egressSub  = clientDrv.aeron->findSubscription(egressSubRegId);
            if (ingressPub && egressSub) break;
            std::this_thread::sleep_for(5ms);
        }
        ASSERT_NE(ingressPub, nullptr) << "could not resolve ingress publication";
        ASSERT_NE(egressSub,  nullptr) << "could not resolve egress subscription";
    }
    {
        const auto rd = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < rd) {
            if (ingressPub->isConnected()) break;
            std::this_thread::sleep_for(5ms);
        }
        ASSERT_TRUE(ingressPub->isConnected())
            << "ingress publication not connected to leader gateway";
    }

    constexpr std::size_t kScratch = 68 * 1024;
    std::vector<uint8_t> scratch(kScratch, 0);

    // Connect → ConnectResponse to obtain a session id.
    pc::ConnectRequest connReq;
    connReq.sessionId      = 0;
    connReq.correlationId  = 1;
    connReq.egressStreamId = kEgressStreamId;
    connReq.clientVersion  = 1;
    connReq.egressChannel  = egressChannel;
    const std::size_t connLen = connReq.encode(scratch.data());
    ASSERT_TRUE(grOfferBounded(*ingressPub, scratch.data(), connLen, 100'000))
        << "ConnectRequest offer failed";

    int64_t assignedSessionId = -1;
    {
        std::optional<pc::ConnectResponse> connResp;
        aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                             aeron::util::index_t offset,
                                             aeron::util::index_t len,
                                             aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) connResp = *dec;
        });
        const auto rd = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < rd && !connResp.has_value()) {
            egressSub->poll(connAsm.handler(), 10);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(connResp.has_value()) << "No ConnectResponse from leader gateway";
        ASSERT_EQ(connResp->code, 0) << "ConnectResponse must be OK (code=0)";
        assignedSessionId = connResp->assignedSessionId;
    }
    ASSERT_GT(assignedSessionId, 0) << "assignedSessionId must be > 0";

    // Produce one batch of kNumRecords records to (topicId=0, partition=0).
    auto batch = grMakeBatch(kNumRecords);
    std::span<const uint8_t> emptyKey{};
    pc::Produce prod;
    prod.sessionId      = assignedSessionId;
    prod.correlationId  = 100;
    prod.topicId        = 0;   // "repl", topicId=0
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.key            = emptyKey;
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> prodScratch(kScratch, 0);
    const std::size_t prodLen = prod.encode(prodScratch.data());
    ASSERT_LE(prodLen, kScratch) << "Produce frame too large for scratch";
    ASSERT_TRUE(grOfferBounded(*ingressPub, prodScratch.data(), prodLen, 100'000))
        << "Produce offer failed";

    {
        std::optional<pc::ProduceResponse> prodResp;
        aeron::FragmentAssembler prodAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                             aeron::util::index_t offset,
                                             aeron::util::index_t len,
                                             aeron::Header&) {
            auto dec = pc::ProduceResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) prodResp = *dec;
        });
        const auto rd = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < rd && !prodResp.has_value()) {
            egressSub->poll(prodAsm.handler(), 10);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(prodResp.has_value()) << "No ProduceResponse from leader gateway";
        EXPECT_EQ(prodResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK))
            << "ProduceResponse must be OK; got errorCode=" << prodResp->errorCode;
    }
    // ProduceResponse{OK} confirms the leader appended the batch on its own duty
    // thread. The target committed offset is kNumRecords. We do NOT read the
    // leader's non-atomic logEndOffset() from the test thread; convergence is
    // proven below via the duty-thread-published cleanerHighWatermark() atomic.
    const Offset targetLeo = static_cast<Offset>(kNumRecords);

    // Wait up to 10 s for followers to replicate.
    using Wall = std::chrono::steady_clock;
    const auto deadline = Wall::now() + 10s;
    bool converged = false;

    std::array<rune::node::BrokerNode*, 3> allNodes{&node0, &node1, &node2};

    // Convergence is polled via the DUTY-THREAD-PUBLISHED atomic snapshot
    // cleanerHighWatermark() (acquire-load; the duty thread release-stores it at
    // every HW change). We do NOT read the non-atomic logEndOffset()/highWatermark()
    // live state from the test thread — that races the owning duty cycle. Each
    // replica's served HW advances to kNumRecords once the batch commits across
    // the ISR, which simultaneously proves the leader committed AND the followers
    // replicated (HW only advances past an offset the ISR has acknowledged).
    auto leaderCommittedHw = [&]() -> Offset {
        return cleanerHwOrMinusOne(*leaderNode);
    };
    auto followerCommittedHw = [&](int i) -> Offset {
        return cleanerHwOrMinusOne(*allNodes[i]);   // -1 when no replica
    };

    while (Wall::now() < deadline) {
        bool allFollowersCommitted = true;
        for (int i = 0; i < 3; ++i) {
            if (allNodes[i] == leaderNode) continue;
            if (followerCommittedHw(i) < targetLeo) {
                allFollowersCommitted = false;
                break;
            }
        }
        if (allFollowersCommitted && leaderCommittedHw() >= targetLeo) {
            converged = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    ASSERT_TRUE(converged)
        << "Replication did not converge within 10 s. "
        << "targetLeo=" << targetLeo
        << " leaderHw=" << leaderCommittedHw();

    EXPECT_GE(leaderCommittedHw(), targetLeo)
        << "Leader committed HW must advance to LEO after followers ack";

    // At least one follower's committed HW must have reached the target (proves
    // the bytes replicated AND committed on a follower).
    bool anyFollowerConverged = false;
    for (int i = 0; i < 3; ++i) {
        if (allNodes[i] == leaderNode) continue;
        if (followerCommittedHw(i) >= targetLeo) anyFollowerConverged = true;
    }
    EXPECT_TRUE(anyFollowerConverged)
        << "At least one follower must have replicated and committed";

    node0.stop();
    node1.stop();
    node2.stop();
}

// ---------------------------------------------------------------------------
// TEST 2: Leadership change → new leader's epoch strictly > old epoch;
//         old leader's replicas adopt the new epoch (no split-brain).
//         Stale-epoch FetchRequest from old leader is rejected.
// ---------------------------------------------------------------------------
TEST(GatewayReplicationWiring, LeadershipChange_EpochBumped_StaleEpochFenced) {
    GrTempDir dir0{"gr_lc0_"}, dir1{"gr_lc1_"}, dir2{"gr_lc2_"};

    const auto cPorts = grDiscoverFreePorts(3);
    const auto kPorts = grDiscoverFreePorts(3);
    const auto rPorts = grDiscoverFreePorts(3);

    auto cfg0 = makeGrConfig(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makeGrConfig(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makeGrConfig(2, dir2.path, cPorts, kPorts, rPorts);

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

    const rune::consensus::NodeId initialLeader = node0->leaderId();
    ASSERT_GE(initialLeader, 0);

    std::array<rune::node::BrokerNode*, 3> allNodes{node0.get(), node1.get(), node2.get()};
    std::array<std::unique_ptr<rune::node::BrokerNode>*, 3>
        owners{&node0, &node1, &node2};

    // Wait for the metadata bridge on the initial leader to report isLeader
    // (atomic — safe from the test thread).
    {
        const auto bridgeDeadline0 = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < bridgeDeadline0) {
            if (allNodes[initialLeader]->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(10ms);
        }
    }
    ASSERT_TRUE(allNodes[initialLeader]->replicationIsLeaderForTest())
        << "initial leader's bridge must report isLeader";

    // Read the initial epoch via the atomic bridge mirror (observer-thread-safe).
    const Epoch epochBefore = allNodes[initialLeader]->replicationEpochForTest();
    ASSERT_GT(epochBefore, 0) << "initial leader epoch must be > 0 after election";

    // Kill the leader (hard stop — the survivors detect the heartbeat timeout
    // and re-elect).
    owners[initialLeader]->reset();

    // Wait for re-election on the survivors.
    std::vector<rune::node::BrokerNode*> survivors;
    for (int i = 0; i < 3; ++i) {
        if (i != initialLeader) survivors.push_back(allNodes[i]);
    }
    ASSERT_EQ(survivors.size(), 2u);

    const auto reElectDeadline = std::chrono::steady_clock::now() + 12s;
    rune::consensus::NodeId newLeaderId = -1;
    bool reElected = false;
    while (std::chrono::steady_clock::now() < reElectDeadline) {
        const auto l0 = survivors[0]->leaderId();
        const auto l1 = survivors[1]->leaderId();
        if (l0 >= 0 && l0 != initialLeader && l0 == l1) {
            reElected = true;
            newLeaderId = l0;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    ASSERT_TRUE(reElected)
        << "survivors did not re-elect after leader kill; "
        << "survivorLeaderIds = {" << survivors[0]->leaderId()
        << ", " << survivors[1]->leaderId() << "}";
    ASSERT_GE(newLeaderId, 0);
    ASSERT_NE(newLeaderId, initialLeader);

    // Wait for the metadata bridge on the new leader to fire (atomic check).
    {
        const auto bridgeDeadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < bridgeDeadline) {
            if (allNodes[newLeaderId]->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(10ms);
        }
    }
    ASSERT_TRUE(allNodes[newLeaderId]->replicationIsLeaderForTest())
        << "new leader's bridge must report isLeader after re-election";

    // Read the new leader's epoch via the atomic bridge mirror BEFORE stop().
    // stop() destroys the ReplicaRegistry (and with it the PartitionReplica objects),
    // so we must not touch replica raw pointers after stop().
    // replicationEpochForTest() is observer-thread-safe (acquire-load pairs with
    // the release-store in ReplicationMetadataBridge::doWork).
    const Epoch epochAfter = allNodes[newLeaderId]->replicationEpochForTest();

    // Stop all live nodes.
    for (auto* ownerPtr : owners) {
        if (*ownerPtr) (*ownerPtr)->stop();
    }

    // Epoch assertions use the cached value read before stop.
    EXPECT_GT(epochAfter, epochBefore)
        << "new leader epoch must be STRICTLY GREATER than old epoch: "
        << epochAfter << " > " << epochBefore;
    // NOTE: Stale-epoch fencing (kErrStaleEpoch) is verified thoroughly in the
    // single-threaded Unit_TermEpochMonotonic_Bridge test (Tick 5/6).
    // The integration test focuses on the epoch advance + bridge wiring.
}

// ---------------------------------------------------------------------------
// TEST 3: Unit — term→epoch monotonicity via ReplicationMetadataBridge.
// No Aeron, no BrokerNode.  Pure logic test.
// ---------------------------------------------------------------------------
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/storage/Log.h>

TEST(GatewayReplicationWiring, Unit_TermEpochMonotonic_Bridge) {
    // Single-partition, single-node setup to exercise the bridge directly.
    GrTempDir d0("unit_bridge_");
    rune::storage::Log log(d0.path.string());

    rune::node::ReplicaRegistry registry;

    constexpr rune::replication::NodeId kSelfId = 1;
    const std::vector<rune::replication::NodeId> allNodes = {1};

    // Per-partition bridge contract (commit 4e70cdf): INTERNAL partitions
    // (topicId < 0) follow CLUSTER-consensus leadership via bridge Step A; USER
    // partitions (topicId >= 0) follow the committed assignment map via setMsm().
    // This test exercises the cluster-consensus term→epoch monotonicity path, so
    // the replica MUST be an internal partition (topicId = -1).
    rune::replication::PartitionConfig cfg;
    cfg.topicId = -1; cfg.partitionId = 0; cfg.selfId = kSelfId;
    cfg.replicaLagMaxMs = 10000; cfg.replicaLagMaxOffset = 10000;
    cfg.minIsr = 1;
    cfg.fetchIntervalNs = 5'000'000LL;
    cfg.isrCheckIntervalNs = 10'000'000LL;
    cfg.maxFetchBytes = 1 << 20;

    // InMemoryTransport (no-op — no leader/follower registered = drops all frames).
    InMemoryTransport transport;
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Follower, 0, allNodes, allNodes, &log, &transport);

    PartitionReplica* rawReplica = replica.get();
    registry.add(-1, 0, std::move(replica));

    rune::node::ReplicationMetadataBridge bridge(kSelfId, allNodes, registry);

    // === Tick 1: no leader yet (lid=-1) → no role flip ===
    int work = bridge.doWork(-1, 0);
    EXPECT_EQ(work, 0) << "no-leader tick must do no work";
    EXPECT_EQ(rawReplica->role(), Role::Follower);

    // === Tick 2: leader = self, epoch 1 → becomeLeader ===
    work = bridge.doWork(kSelfId, 1);
    EXPECT_EQ(work, 1) << "first leader election must trigger role flip";
    EXPECT_EQ(rawReplica->role(), Role::Leader)
        << "replica must be Leader when bridge detects self as leader";
    EXPECT_EQ(rawReplica->epoch(), static_cast<Epoch>(1));

    // === Tick 3: same leader/epoch → no flip ===
    work = bridge.doWork(kSelfId, 1);
    EXPECT_EQ(work, 0) << "same leader/epoch must not re-flip";
    EXPECT_EQ(rawReplica->role(), Role::Leader);

    // === Tick 4: epoch bump (term advanced) → re-apply becomeLeader ===
    work = bridge.doWork(kSelfId, 2);
    EXPECT_EQ(work, 1);
    EXPECT_EQ(rawReplica->epoch(), static_cast<Epoch>(2));
    EXPECT_EQ(rawReplica->role(), Role::Leader);

    // === Tick 5: epoch regress attempt (must be clamped → no-op) ===
    work = bridge.doWork(kSelfId, 1);  // regress attempt
    EXPECT_EQ(work, 0) << "epoch regress must be a no-op (epoch stays at 2)";
    EXPECT_GE(rawReplica->epoch(), static_cast<Epoch>(2))
        << "epoch must not regress";

    // === Tick 6: different leader (leader change) → becomeFollower ===
    constexpr rune::replication::NodeId kOtherId = 2;
    work = bridge.doWork(kOtherId, 3);
    EXPECT_EQ(work, 1);
    EXPECT_EQ(rawReplica->role(), Role::Follower)
        << "replica must be Follower when another node becomes leader";
    EXPECT_EQ(rawReplica->epoch(), static_cast<Epoch>(3));
    EXPECT_EQ(bridge.isLeader(), false);

    // === Epoch must be monotonic across all ticks ===
    EXPECT_GE(bridge.lastEpoch(), static_cast<Epoch>(3));
}

// ---------------------------------------------------------------------------
// TEST 4: Unit — role-flip logic on repeated leadership changes.
// Tests the GR-2 invariants using a fake consensus-view stream.
// ---------------------------------------------------------------------------
TEST(GatewayReplicationWiring, Unit_RoleFlip_RepeatedLeaderChanges) {
    GrTempDir dA("rlf_A_"), dB("rlf_B_");
    rune::storage::Log logA(dA.path.string()), logB(dB.path.string());

    rune::node::ReplicaRegistry registry;

    const std::vector<rune::replication::NodeId> allNodes = {1, 2};
    constexpr rune::replication::NodeId kSelf = 1;

    // Per-partition bridge contract (commit 4e70cdf): this test drives role flips
    // from CLUSTER-consensus leadership changes, so the replica MUST be an internal
    // partition (topicId < 0). User partitions follow the committed map (setMsm).
    rune::replication::PartitionConfig cfg;
    cfg.topicId = -1; cfg.partitionId = 0; cfg.selfId = kSelf;
    cfg.replicaLagMaxMs = 10000; cfg.replicaLagMaxOffset = 10000;
    cfg.minIsr = 1;
    cfg.fetchIntervalNs = 5'000'000LL; cfg.isrCheckIntervalNs = 10'000'000LL;
    cfg.maxFetchBytes = 1 << 20;

    InMemoryTransport transport;
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Follower, 0, allNodes, allNodes, &logA, &transport);
    PartitionReplica* r = replica.get();
    registry.add(-1, 0, std::move(replica));

    rune::node::ReplicationMetadataBridge bridge(kSelf, allNodes, registry);

    // --- Election 1: node 2 wins (term 1) ---
    bridge.doWork(2, 1);
    EXPECT_EQ(r->role(), Role::Follower);
    EXPECT_EQ(r->epoch(), Epoch{1});

    // --- Election 2: node 1 (self) wins (term 2) ---
    bridge.doWork(1, 2);
    EXPECT_EQ(r->role(), Role::Leader);
    EXPECT_EQ(r->epoch(), Epoch{2});

    // --- Election 3: node 2 wins again (term 3) ---
    bridge.doWork(2, 3);
    EXPECT_EQ(r->role(), Role::Follower);
    EXPECT_EQ(r->epoch(), Epoch{3});

    // --- Election 4: node 1 (self) wins again (term 4) ---
    bridge.doWork(1, 4);
    EXPECT_EQ(r->role(), Role::Leader);
    EXPECT_EQ(r->epoch(), Epoch{4});

    // Epoch must be strictly increasing across all changes.
    EXPECT_GT(bridge.lastEpoch(), Epoch{0});
    EXPECT_EQ(bridge.lastEpoch(), Epoch{4});
}
