// ---------------------------------------------------------------------------
// BootstrapRoutingImageTest.cpp — MD1-13 Round-2: bootstrap routing image
// correctness + acks=All bootstrap rejection.
//
// PRIMARY FIX COVERAGE (NodeRuntimeAgent-level, mirrors MetadataReplication
// integration harness):
//
//   When metadataService_->committedMap() is EMPTY (metadata log not yet
//   replicated + applied), the NodeRuntimeAgent duty-cycle path:
//     a) calls buildStaticRoutingImage() — deterministic StaticBalancer image;
//     b) publishes that image to the gateway via setRoutingImageSnapshot();
//     c) leaves committedMetadataReady_ = false.
//
//   This test proves:
//
//     Test 1: BootstrapImage_NonNull
//       buildStaticRoutingImage() returns non-null when topics are
//       configured, even while committedMap() is empty.
//
//     Test 2: BootstrapImage_DeterministicPartitionAssignment
//       The bootstrap image assigns each partition to a deterministic node
//       (StaticBalancer round-robin): NO partition maps to NO_LEADER_NODE_ID.
//       Partition 0 of the first topic maps to the first (lowest) nodeId,
//       so a produce to that partition on that node routes locally — NOT
//       into a redirect loop.
//
//     Test 3: BootstrapImage_EmptyWhenNoTopicsConfigured
//       If no topics are configured, buildStaticRoutingImage() returns null
//       (gateway falls back to Path B, cluster-leader routing).
//
//     Test 4: AcksAll_Bootstrap_NotLeader
//       When committedMetadataReady_=false, an acks=All produce via the
//       gateway returns ErrorCode::NotLeader (retriable, code=2) — NOT OK.
//       The log is untouched (baseOffset stays 0 — NEVER falsely ack).
//
//     Test 5: AcksLeader_Bootstrap_ServesLocally
//       When committedMetadataReady_=false, an acks=Leader produce via the
//       gateway returns ProduceResponse{OK} immediately (local append — the
//       acks=Leader contract permits local-only ack).
//
// Tests 1-3 use InMemoryTransport + MetadataService only (no Aeron).
// Tests 4-5 use applyProduceForTest + injectTestSession + setOnEgressFrame
// (no live Aeron ingress needed; a minimal embedded Aeron driver is started
// only to satisfy the ClientGateway constructor, but the ingress sub is never
// polled).
//
// Gated behind aeron_FOUND (Tests 4-5 require the Aeron driver header and
// library).  When BUILD_NODE_UDP_TESTS is ON, this file compiles in full;
// otherwise only the no-Aeron tests (1-3) are compiled.
//
// NO Docker. All waits are bounded. No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// MetadataService / RoutingImage (no Aeron)
#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::node;
using namespace rune::metadata;
namespace fs = std::filesystem;

using rune::metadata::kMaxGpidPartitions;

// ===========================================================================
// Shared helpers (no Aeron)
// ===========================================================================

static std::string makeBrtSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct BrtTempDir {
    std::string path;
    explicit BrtTempDir(std::string_view tag) {
        path = fs::temp_directory_path().string()
             + "/rune_brt_" + std::string(tag) + "_" + makeBrtSuffix();
        fs::create_directories(path);
    }
    ~BrtTempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Build a PartitionConfig for the metadata replica (topicId=-3).
static PartitionConfig makeMetaCfg(NodeId selfId, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.topicId             = -3;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

// ===========================================================================
// Test 1: buildStaticRoutingImage returns non-null when topics are configured
//         and committedMap() is empty (bootstrap window).
// ===========================================================================
TEST(BootstrapRoutingImage, BootstrapImage_NonNull) {
    BrtTempDir metaDir("t1m");
    rune::storage::Log metaLog(metaDir.path);
    metaLog.recover();

    constexpr NodeId kSelf = 1;
    const std::vector<NodeId> allNodes = {kSelf};

    InMemoryTransport transport;
    auto metaReplica = std::make_unique<PartitionReplica>(
        makeMetaCfg(kSelf, 1), Role::Leader, 1, allNodes, allNodes,
        &metaLog, &transport);
    transport.setLeader(metaReplica.get());

    // Configure one topic (rune::metadata::TopicSpec).
    std::vector<rune::metadata::TopicSpec> topics = {
        rune::metadata::TopicSpec{/*topicId=*/0, /*partitionCount=*/1, /*rf=*/1,
                                  /*coGroup=*/"", /*key=*/"events"},
    };

    MetadataService svc(metaReplica.get(), kSelf);
    svc.configure(topics, allNodes);

    // Before any duty-cycle drives: committedMap must be empty.
    ASSERT_TRUE(svc.committedMap().empty())
        << "committedMap must be empty before metadata log is driven";

    // buildStaticRoutingImage must return non-null (topics are configured).
    auto img = svc.buildStaticRoutingImage();
    ASSERT_NE(img, nullptr)
        << "buildStaticRoutingImage must return non-null when topics are configured "
           "and committedMap is empty — this is the bootstrap image used to prevent "
           "redirect loops during the bootstrap window";
}

// ===========================================================================
// Test 2: bootstrap image has deterministic partition→node assignment.
//
// With 3 nodes (nodeIds 1, 2, 3) and a 3-partition topic (RF=1), StaticBalancer
// assigns partition p → allNodes[p % 3] (sorted ascending):
//   p=0 → node 1, p=1 → node 2, p=2 → node 3.
//
// This test asserts:
//   a) No partition maps to NO_LEADER_NODE_ID (no redirect loop during bootstrap).
//   b) The assignment is balanced (1 partition per node).
//   c) Partition 0 routes to node 1 (the smallest nodeId) — a produce from
//      node 1 to partition 0 is served locally, not redirect-looped.
// ===========================================================================
TEST(BootstrapRoutingImage, BootstrapImage_DeterministicPartitionAssignment) {
    BrtTempDir metaDir("t2m");
    rune::storage::Log metaLog(metaDir.path);
    metaLog.recover();

    constexpr NodeId kNode1 = 1, kNode2 = 2, kNode3 = 3;
    const std::vector<NodeId> allNodes = {kNode1, kNode2, kNode3};

    InMemoryTransport transport;
    auto metaReplica = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode1, 2), Role::Leader, 1, allNodes, allNodes,
        &metaLog, &transport);
    transport.setLeader(metaReplica.get());

    constexpr int32_t kTopicId = 0;
    constexpr int32_t kPartitions = 3;

    std::vector<rune::metadata::TopicSpec> topics = {
        rune::metadata::TopicSpec{kTopicId, kPartitions, /*rf=*/1, /*coGroup=*/"", "multi"},
    };

    MetadataService svc(metaReplica.get(), kNode1);
    svc.configure(topics, allNodes);

    // committedMap must be empty (no duty-cycle drives yet).
    ASSERT_TRUE(svc.committedMap().empty())
        << "committedMap must be empty before any metadata replication";

    // Build the bootstrap image.
    auto img = svc.buildStaticRoutingImage();
    ASSERT_NE(img, nullptr) << "bootstrap image must be non-null with topics configured";

    // (a) No partition maps to NO_LEADER_NODE_ID.
    for (int32_t p = 0; p < kPartitions; ++p) {
        const int32_t gpid = gpidOf(kTopicId, p);
        const auto& entry = img->lookup(gpid);
        EXPECT_NE(entry.leaderNodeId, rune::metadata::RoutingImage::NO_LEADER_NODE_ID)
            << "bootstrap image must assign a valid leader for partition " << p
            << " — NO_LEADER_NODE_ID causes a redirect loop";
    }

    // (b) Balanced assignment: each of the 3 nodes leads exactly 1 partition.
    std::map<int32_t, int32_t> leaderCount;
    for (int32_t p = 0; p < kPartitions; ++p) {
        const int32_t gpid = gpidOf(kTopicId, p);
        leaderCount[img->lookup(gpid).leaderNodeId]++;
    }
    EXPECT_EQ(static_cast<int>(leaderCount.size()), 3)
        << "all 3 nodes must be represented as partition leaders in the bootstrap image";
    for (auto& [node, cnt] : leaderCount) {
        EXPECT_EQ(cnt, 1)
            << "each node must lead exactly 1 partition (balanced, RF=1, 3-node, 3-partition)";
    }

    // (c) Partition 0 routes to node 1 (smallest nodeId = StaticBalancer stripe 0).
    //     A produce from node 1 to partition 0 is served locally — no redirect.
    {
        const int32_t gpid = gpidOf(kTopicId, 0);
        const auto& e = img->lookup(gpid);
        EXPECT_EQ(e.leaderNodeId, kNode1)
            << "partition 0 must route to node 1 in the bootstrap image "
               "(StaticBalancer assigns stripe 0 to the smallest nodeId); "
               "a produce from node 1 to partition 0 must be served locally";
    }
}

// ===========================================================================
// Test 3: buildStaticRoutingImage returns null when no topics are configured.
//         (Gateway falls back to Path B — cluster-leader routing.)
// ===========================================================================
TEST(BootstrapRoutingImage, BootstrapImage_NullWhenNoTopicsConfigured) {
    BrtTempDir metaDir("t3m");
    rune::storage::Log metaLog(metaDir.path);
    metaLog.recover();

    constexpr NodeId kSelf = 1;
    const std::vector<NodeId> allNodes = {kSelf};

    InMemoryTransport transport;
    auto metaReplica = std::make_unique<PartitionReplica>(
        makeMetaCfg(kSelf, 1), Role::Leader, 1, allNodes, allNodes,
        &metaLog, &transport);
    transport.setLeader(metaReplica.get());

    // No configure() call → no topics → buildStaticRoutingImage returns null.
    MetadataService svc(metaReplica.get(), kSelf);

    ASSERT_TRUE(svc.committedMap().empty());

    auto img = svc.buildStaticRoutingImage();
    EXPECT_EQ(img, nullptr)
        << "buildStaticRoutingImage must return null when no topics are configured "
           "(gateway falls back to Path B — cluster-leader routing)";
}

// ===========================================================================
// Tests 4 + 5: acks=All bootstrap rejection + acks=Leader local serve.
//
// These tests require an embedded Aeron media driver to satisfy the
// ClientGateway constructor (the ingress subscription is created at startup).
// The ingress sub is never polled; all produces are injected via
// applyProduceForTest (test seam added for MD1-13 Round-2).
// ===========================================================================
#ifdef RUNE_AERON_AVAILABLE

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <Aeron.h>
extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <map>
#include <optional>
#include <thread>
namespace pc = rune::protocol::client;

// Minimal in-process Aeron media driver for test use.
// Mirrors the Gr5ClientDriver pattern: manual-threading mode + background
// work loop so ingress subscriptions in ClientGateway resolve promptly.
struct BrtAeronDriver {
    aeron_driver_context_t* ctx     = nullptr;
    aeron_driver_t*         driver  = nullptr;
    std::atomic<bool>       running{false};
    std::thread             workThread;
    std::shared_ptr<aeron::Aeron> aeron;

    explicit BrtAeronDriver(const std::string& aeronDir) {
        fs::create_directories(aeronDir);
        aeron_driver_context_init(&ctx);
        aeron_driver_context_set_dir(ctx, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(ctx, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx, true);
        aeron_driver_context_set_shared_idle_strategy(ctx, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx, true);
        aeron_driver_context_set_term_buffer_length(ctx, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(ctx, 64 * 1024);

        EXPECT_EQ(aeron_driver_init(&driver, ctx), 0);
        EXPECT_EQ(aeron_driver_start(driver, /*manual_main_loop=*/true), 0);

        running.store(true, std::memory_order_release);
        workThread = std::thread([this]() {
            while (running.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(
                    driver, aeron_driver_main_do_work(driver));
            }
        });

        // No caller-side pumping here: aeron_driver_init/aeron_driver_start already
        // wrote cnc.dat and stamped the heartbeat synchronously (aeron_driver.c:864,
        // :911) before workThread even starts, so a client can connect immediately.
        // A second thread pumping the conductor here — even just to poll for
        // cnc.dat — would race workThread on the single-pumper driver contract.
        const std::string cncPath = aeronDir + "/cnc.dat";
        EXPECT_TRUE(fs::exists(cncPath)) << "Aeron cnc.dat not created by aeron_driver_init/start";

        aeron::Context aeronCtx;
        aeronCtx.aeronDir(aeronDir);
        aeron = aeron::Aeron::connect(aeronCtx);
    }

    ~BrtAeronDriver() {
        aeron.reset();
        running.store(false, std::memory_order_release);
        if (workThread.joinable()) workThread.join();
        if (driver) { aeron_driver_close(driver); driver = nullptr; }
        if (ctx)    { aeron_driver_context_close(ctx); ctx = nullptr; }
    }
};

static std::vector<uint8_t> makeProdBatch(int32_t records = 1, int64_t base = 0) {
    rune::protocol::RecordBatchBuilder b(base, 1000LL);
    for (int32_t i = 0; i < records; ++i) b.append("k", "v", 1000LL + i);
    return b.build();
}

// ===========================================================================
// Test 4: acks=All (msg.acks=1) + committedMetadataReady_=false
//         → ErrorCode::NotLeader (retriable, code=2), log untouched.
// ===========================================================================
TEST(BootstrapRoutingImage, AcksAll_Bootstrap_NotLeader) {
    BrtTempDir dataDir("t4data"), aeronDir("t4aeron");

    BrtAeronDriver drv(aeronDir.path);  // starts driver + Aeron client (drv.aeron)

    // TopicRegistry: 1 topic, 1 partition.
    rune::node::TopicSpec nodeSpec;
    nodeSpec.name           = "events";
    nodeSpec.topicId        = 0;
    nodeSpec.partitionCount = 1;
    auto topicReg = std::make_unique<rune::gateway::TopicRegistry>(
        std::vector<rune::node::TopicSpec>{nodeSpec}, dataDir.path);

    // ClientGateway on IPC channel (ingress never polled in this test).
    constexpr int32_t kClientStreamId = 9800;
    const std::string clientChannel = "aeron:ipc";
    constexpr int32_t kThisNodeId = 1;

    auto gw = std::make_unique<rune::gateway::ClientGateway>(
        drv.aeron, clientChannel, kClientStreamId, kThisNodeId,
        rune::gateway::ClientGateway::kDefaultSessionTimeoutNs,
        /* leaderView: always leader */ []() -> rune::gateway::LeaderView {
            return rune::gateway::LeaderView{true, ""};
        });
    gw->setTopicRegistry(topicReg.get());

    // Wire bootstrap routing image (StaticBalancer, 1 partition → node 1).
    {
        std::vector<rune::metadata::TopicSpec> mbTopics = {
            rune::metadata::TopicSpec{0, 1, 1, "", "events"}
        };
        const std::vector<NodeId> nodes = {kThisNodeId};
        const auto records = rune::metadata::StaticBalancer::assign(mbTopics, nodes);
        ASSERT_FALSE(records.empty());

        std::vector<rune::metadata::RoutingImage::BuildEntry> entries;
        entries.reserve(records.size());
        for (const auto& br : records) {
            const int32_t gpid = rune::metadata::gpidOf(br.rec.topicId, br.rec.partition);
            entries.push_back({gpid, br.rec.leaderNode, br.rec.leaderEpoch});
        }
        auto img = rune::metadata::RoutingImage::build(entries);
        gw->setRoutingImageSnapshot(std::move(img));
        // committedMetadataReady_ stays false — NOT calling setCommittedMetadataReady.
    }

    // Capture egress frames.
    struct Captured {
        std::optional<pc::ProduceResponse> produceResp;
        std::optional<pc::ErrorResponse>   errorResp;
    } cap;

    gw->setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        {
            auto d = pc::ProduceResponse::decode(buf, len);
            if (d) { cap.produceResp = *d; return; }
        }
        {
            auto d = pc::ErrorResponse::decode(buf, len);
            if (d) { cap.errorResp = *d; return; }
        }
    });

    const int64_t sid = gw->injectTestSession();

    // Get the Log pointer so we can check it was not appended.
    rune::storage::Log* userLog = topicReg->log(0, 0);
    ASSERT_NE(userLog, nullptr);
    const int64_t leoBeforeProduce = userLog->logEndOffset();

    // Encode an acks=All produce.
    auto batch = makeProdBatch(1);
    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 99;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.acks           = 1;    // acks=All
    prod.producerId     = -1;
    prod.key            = std::span<const uint8_t>{};
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    static constexpr std::size_t kScratch = 64 * 1024;
    std::vector<uint8_t> scratch(kScratch, 0);
    const std::size_t encLen = prod.encode(scratch.data());
    ASSERT_GT(encLen, 0u);

    cap.produceResp.reset();
    cap.errorResp.reset();

    gw->applyProduceForTest(scratch.data(), encLen);

    // Must have got a response (either ErrorResponse or ProduceResponse).
    ASSERT_TRUE(cap.errorResp.has_value() || cap.produceResp.has_value())
        << "acks=All during bootstrap must produce a response (NotLeader retriable)";

    if (cap.errorResp.has_value()) {
        EXPECT_EQ(cap.errorResp->code,
                  static_cast<int16_t>(pc::ErrorCode::NotLeader))
            << "acks=All bootstrap response must be ErrorCode::NotLeader (2)";
    } else {
        // If encoded as ProduceResponse, errorCode must NOT be OK.
        EXPECT_NE(cap.produceResp->errorCode,
                  static_cast<int16_t>(pc::ErrorCode::OK))
            << "acks=All during bootstrap must NOT respond OK — "
               "that silently violates the majority-durability contract";
    }

    // Log must NOT have been appended (NEVER falsely ack invariant).
    EXPECT_EQ(userLog->logEndOffset(), leoBeforeProduce)
        << "acks=All bootstrap rejection must not advance LEO — "
           "no append, no partial state";

    gw.reset();
}

// ===========================================================================
// Test 5: acks=Leader (msg.acks=0) + committedMetadataReady_=false
//         → ProduceResponse{OK, baseOffset=0} (immediate local ack).
// ===========================================================================
TEST(BootstrapRoutingImage, AcksLeader_Bootstrap_ServesLocally) {
    BrtTempDir dataDir("t5data"), aeronDir("t5aeron");

    BrtAeronDriver drv(aeronDir.path);  // starts driver + Aeron client (drv.aeron)

    rune::node::TopicSpec nodeSpec;
    nodeSpec.name           = "events";
    nodeSpec.topicId        = 0;
    nodeSpec.partitionCount = 1;
    auto topicReg = std::make_unique<rune::gateway::TopicRegistry>(
        std::vector<rune::node::TopicSpec>{nodeSpec}, dataDir.path);

    constexpr int32_t kThisNodeId = 1;
    auto gw = std::make_unique<rune::gateway::ClientGateway>(
        drv.aeron, "aeron:ipc", 9801, kThisNodeId,
        rune::gateway::ClientGateway::kDefaultSessionTimeoutNs,
        []() -> rune::gateway::LeaderView { return {true, ""}; });
    gw->setTopicRegistry(topicReg.get());

    // Bootstrap routing image — same as Test 4.
    {
        std::vector<rune::metadata::TopicSpec> mbTopics = {
            rune::metadata::TopicSpec{0, 1, 1, "", "events"}
        };
        const auto records = rune::metadata::StaticBalancer::assign(
            mbTopics, {kThisNodeId});
        ASSERT_FALSE(records.empty());
        std::vector<rune::metadata::RoutingImage::BuildEntry> entries;
        for (const auto& br : records) {
            entries.push_back({rune::metadata::gpidOf(br.rec.topicId, br.rec.partition),
                               br.rec.leaderNode, br.rec.leaderEpoch});
        }
        gw->setRoutingImageSnapshot(rune::metadata::RoutingImage::build(entries));
        // committedMetadataReady_ stays false.
    }

    std::optional<pc::ProduceResponse> prodResp;
    gw->setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto d = pc::ProduceResponse::decode(buf, len);
        if (d) prodResp = *d;
    });

    const int64_t sid = gw->injectTestSession();

    auto batch = makeProdBatch(1);
    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 100;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.acks           = 0;    // acks=Leader
    prod.producerId     = -1;
    prod.key            = std::span<const uint8_t>{};
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> scratch(64 * 1024, 0);
    const std::size_t encLen = prod.encode(scratch.data());
    ASSERT_GT(encLen, 0u);

    gw->applyProduceForTest(scratch.data(), encLen);

    // acks=Leader during bootstrap → immediate OK.
    ASSERT_TRUE(prodResp.has_value())
        << "acks=Leader during bootstrap must return a ProduceResponse";
    EXPECT_EQ(prodResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK))
        << "acks=Leader during bootstrap must be ProduceResponse{OK} — "
           "local ack is correct (acks=Leader does not promise majority durability)";
    EXPECT_EQ(prodResp->baseOffset, 0)
        << "first produce must have baseOffset=0";

    rune::storage::Log* userLog = topicReg->log(0, 0);
    ASSERT_NE(userLog, nullptr);
    EXPECT_EQ(userLog->logEndOffset(), 1)
        << "acks=Leader bootstrap produce must append 1 record (LEO advances to 1)";

    gw.reset();
}

#endif // RUNE_AERON_AVAILABLE
