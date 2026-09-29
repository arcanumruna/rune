// ---------------------------------------------------------------------------
// RuntimeTopicReplicaTest.cpp — Task 8b (RULING-4, SINGLE-NODE): a RUNTIME-
// defined topic (created via DefineTopic → AdminWorker → control-hook
// createTopic) becomes produce/consume-capable because the control thread
// creates + registers its user PartitionReplica AND emits its assignment so the
// bridge promotes the replica to Leader.
//
// This proves the 8b gap fix WITHOUT Aeron (single-node, InMemoryTransport):
// the CONTROL-THREAD mechanism is exercised directly, mirroring exactly what
// BrokerNode's admin create-topic hook now does:
//
//   1. Register a user PartitionReplica for the new topic in the ReplicaRegistry
//      (as BrokerNode::createRuntimeUserReplicas does — bumps generation so
//      WorkerPool::reconcile picks it up; here we drive the bridge directly).
//   2. Call MetadataService::addRuntimeTopic(spec) — the leader emits a static
//      assignment record for the new topic to the __cluster_metadata replica.
//   3. Drive control cycles: the metadata record commits → committedMap() gains
//      the topic → the ReplicationMetadataBridge (Step B, applyUserPartitionRoles)
//      promotes the runtime replica to Leader.
//   4. Produce (append) to the runtime replica succeeds → LEO advances.
//   5. Consume (read) reads the record back.
//
// Marked *Regression* so it runs with the regression gate.
//
// Multi-node partition placement / schema replication are OUT OF SCOPE (deferred).
// No Aeron, no busy-spin: bounded drive loops only.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/replication/PartitionReplica.h>  // also declares InMemoryTransport
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

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

namespace {

static constexpr int kDriveCap = 2000;

std::string uniqDir(const char* tag) {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path().string() + "/rune_rtr_" + tag + "_"
           + std::to_string(::getpid()) + "_"
           + std::to_string(seq.fetch_add(1)) + "_" + std::to_string(ts);
    fs::create_directories(p);
    return p;
}

PartitionConfig makeMetaCfg(NodeId selfId) {
    PartitionConfig cfg;
    cfg.topicId             = -3;  // __cluster_metadata sentinel
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;    // single-node: leader-local commit
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

PartitionConfig makeUserCfg(NodeId selfId, int32_t topicId, int32_t part) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = static_cast<PartitionId>(part);
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;    // single-node: leader-local commit
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.cleanupPolicy       = CleanupPolicy::Delete;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

std::vector<uint8_t> makeBatch(std::string_view payload) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, payload, /*timestamp=*/0);
    return b.build();
}

} // namespace

// ---------------------------------------------------------------------------
// Single-node: DefineTopic-created runtime topic gets a user replica, is
// promoted to Leader via the emitted assignment, and a Produce APPENDS + a
// Consume reads it back.
// ---------------------------------------------------------------------------
TEST(RuntimeTopicReplicaRegression, RuntimeTopicBecomesProduceConsumeCapable) {
    const NodeId kNode = 1;
    const std::vector<NodeId> allNodes = {kNode};

    // ── Metadata replica (__cluster_metadata), single-node Leader ──────────────
    const std::string metaDir = uniqDir("meta");
    rune::storage::Log metaLog(metaDir);
    metaLog.recover();

    InMemoryTransport metaTransport;
    auto metaReplica = std::make_unique<PartitionReplica>(
        makeMetaCfg(kNode), Role::Leader, /*epoch=*/1, allNodes, allNodes,
        &metaLog, &metaTransport);
    metaTransport.setLeader(metaReplica.get());

    // ── ReplicaRegistry + bridge (drives user-partition roles from committedMap) ─
    ReplicaRegistry registry;
    registry.add(-3, 0, std::move(metaReplica));
    PartitionReplica* metaRaw = registry.lookup(-3, 0);

    ReplicationMetadataBridge bridge(kNode, allNodes, registry);

    // ── MetadataService (leader), configured with NO boot topics ───────────────
    MetadataService svc(metaRaw, kNode);
    svc.configure(/*topics=*/{}, allNodes);
    svc.setMinIsr(1);

    // Wire the MSM into the bridge so Step B (applyUserPartitionRoles) fires.
    bridge.setMsm(&svc.msmForTest());

    // Drive a few cycles so the metadata replica is a stable single-node leader
    // (bridge Step A applies internal-partition leadership from cluster leader=1).
    int64_t clock = 0;
    for (int i = 0; i < 10; ++i) {
        clock += 2'000'000LL;
        metaRaw->doWork(clock);
        metaTransport.pump(clock);
        bridge.doWork(/*currentLeaderId=*/kNode, /*currentEpoch=*/1);
        svc.doWork(clock);
    }
    ASSERT_EQ(metaRaw->role(), Role::Leader)
        << "single-node metadata replica must be Leader";

    // ── SIMULATE the control-thread create-topic hook for a RUNTIME topic ──────
    // TopicRegistry::createTopic() has assigned id=0 and created the dir/log.
    const int32_t kRuntimeTopicId = 0;
    const std::string userDir = uniqDir("runtime");
    rune::storage::Log userLog(userDir);
    userLog.recover();

    InMemoryTransport userTransport;
    // (1) Create + register the user PartitionReplica (Follower / epoch 0), as
    //     BrokerNode::createRuntimeUserReplicas does.  add() bumps generation.
    auto userReplica = std::make_unique<PartitionReplica>(
        makeUserCfg(kNode, kRuntimeTopicId, /*part=*/0),
        Role::Follower, /*epoch=*/0, allNodes, allNodes, &userLog, &userTransport);
    userTransport.setLeader(userReplica.get());
    const std::uint64_t genBefore = registry.generation();
    registry.add(kRuntimeTopicId, 0, std::move(userReplica));
    EXPECT_GT(registry.generation(), genBefore)
        << "registering the runtime replica must bump the registry generation "
           "(so WorkerPool::reconcile assigns it)";
    PartitionReplica* userRaw = registry.lookup(kRuntimeTopicId, 0);
    ASSERT_NE(userRaw, nullptr);
    ASSERT_EQ(userRaw->role(), Role::Follower)
        << "runtime replica starts Follower (not yet promoted)";

    // (2) Register the runtime topic with the MetadataService → leader emits the
    //     static assignment record to __cluster_metadata.
    rune::metadata::TopicSpec bs;
    bs.topicId        = kRuntimeTopicId;
    bs.partitionCount = 1;
    bs.rf             = 1;
    svc.addRuntimeTopic(bs);

    // ── (3) Drive control cycles: assignment commits → bridge promotes replica ──
    bool promoted = false;
    for (int i = 0; i < kDriveCap && !promoted; ++i) {
        clock += 1'000'000LL;
        metaRaw->doWork(clock);
        metaTransport.pump(clock);
        svc.doWork(clock);                                // apply committed records
        bridge.doWork(/*currentLeaderId=*/kNode, /*currentEpoch=*/1);  // Step B
        if (userRaw->role() == Role::Leader) promoted = true;
    }
    ASSERT_TRUE(promoted)
        << "runtime topic's user replica must be promoted to Leader via the "
           "emitted assignment + bridge (single-node) within " << kDriveCap
        << " cycles";

    // Sanity: the committed assignment map now contains the runtime topic.
    ASSERT_NE(svc.assignment(kRuntimeTopicId, 0), nullptr)
        << "committed assignment map must contain the runtime topic partition";

    // ── (4) PRODUCE: append a batch to the runtime replica → LEO advances ──────
    const Offset leoBefore = userRaw->logEndOffset();
    auto batch = makeBatch("runtime-hello");
    const Offset base = userRaw->append(
        std::span<const uint8_t>(batch.data(), batch.size()), /*acksAll=*/false);
    EXPECT_GE(base, 0) << "append to the runtime-topic leader replica must succeed";

    // Drive so the append commits (single-node leader-local HW).
    for (int i = 0; i < 50; ++i) {
        clock += 1'000'000LL;
        userRaw->doWork(clock);
        userTransport.pump(clock);
    }
    EXPECT_GT(userRaw->logEndOffset(), leoBefore)
        << "log end offset must advance after a produce to the runtime topic";
    EXPECT_GT(userRaw->highWatermark(), 0)
        << "the produced record must commit (single-node HW advances)";

    // ── (5) CONSUME: read the committed record back ────────────────────────────
    const std::vector<uint8_t> readBack = userRaw->read(/*fetchOffset=*/0, 1 << 20);
    ASSERT_FALSE(readBack.empty())
        << "consume must read back the produced record from the runtime topic";

    // The read-back bytes must contain the produced payload.
    const std::string_view hay(
        reinterpret_cast<const char*>(readBack.data()), readBack.size());
    EXPECT_NE(hay.find("runtime-hello"), std::string_view::npos)
        << "the consumed record must contain the produced payload";

    // Cleanup.
    std::error_code ec;
    fs::remove_all(metaDir, ec);
    fs::remove_all(userDir, ec);
}
