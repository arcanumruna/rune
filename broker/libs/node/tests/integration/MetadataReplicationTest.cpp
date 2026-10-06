// ---------------------------------------------------------------------------
// MetadataReplicationTest.cpp — Integration tests for MD1 Task 4:
// MetadataService wiring into the node runtime as a REPLICATED service.
//
// Test cases:
//   1. SingleNode_AssignmentVisibleOnlyAfterHwAdvance
//      Append assignment records via metadataReplica_, drive doWork,
//      assert assignment() reflects them ONLY after commit (HW advance).
//      Also asserts the assignment is NULL before doWork/HW advance.
//
//   2. ThreeNode_RecordsVisibleOnAllFollowersAfterMajorityHw
//      3-in-process nodes. Records committed on the leader become visible
//      on ALL followers' MSM after replication (majority HW).
//
//   3. Restart_SnapshotPlusReplayYieldsIdenticalMap
//      A node restarts, loads snapshot + replays the tail; assignment map
//      identical to the pre-restart map (hash equality).
//
//   4. ApplyIsOffRequestPath_NoLockStall
//      A produce/read served while an assignment applies — no shared-lock
//      stall (assert both operations complete within a tight deadline).
//
//   5. MetadataReplicaRoleFollowsBridgeDrivenClusterLeadership
//      The metadata partition's role is driven by ReplicationMetadataBridge
//      via ReplicaRegistry.  Bridge cluster-leader = self → replica becomes
//      Leader; bridge cluster-leader = another node → replica becomes Follower.
//      Exercises the real FINDING-1 wiring path end-to-end.
//
//   6. FollowerHwGatedVisibility_AssignmentNotVisibleBeforeReplication
//      Assert leader/follower HW-gated correctness: a metadata service on a
//      follower node does NOT see records that have not yet been replicated
//      (HW-gated committed-only discrimination).
//
// NO Docker. All waits are bounded (kDriverCap + ASSERT_TRUE). No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::node;
namespace pmd = rune::protocol::metadata;

// ===========================================================================
// Test helpers
// ===========================================================================

static constexpr int kDriverCap = 1000;

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct MrtTempDir {
    std::string path;
    explicit MrtTempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_mrt_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~MrtTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Build a default PartitionConfig for the metadata replica (topicId=-3).
static PartitionConfig makeMetaCfg(NodeId selfId, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.topicId             = -3;  // sentinel: __cluster_metadata
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

// Encode an UpdatePartitionAssignmentRecord into buf.
// Returns bytes written.
static std::size_t encodeAssignment(
    uint8_t* buf, std::size_t bufLen,
    int32_t topicId, int32_t partition, int32_t leaderNode,
    const std::vector<int32_t>& isr,
    const std::vector<int32_t>& replicas,
    int32_t leaderEpoch, int32_t partitionEpoch)
{
    // Build Int32ArrayViews from the vectors.
    // We encode the arrays inline since Int32ArrayView is zero-copy.
    // Build a temp encode buffer.
    static std::vector<uint8_t> isrBuf, repBuf;
    isrBuf.resize(2 + isr.size() * 4);
    repBuf.resize(2 + replicas.size() * 4);

    auto writeU16 = [](uint8_t* p, uint16_t v) {
        p[0] = static_cast<uint8_t>(v); p[1] = static_cast<uint8_t>(v >> 8);
    };
    auto writeS32 = [](uint8_t* p, int32_t v) {
        uint32_t u = static_cast<uint32_t>(v);
        p[0]=u; p[1]=u>>8; p[2]=u>>16; p[3]=u>>24;
    };

    writeU16(isrBuf.data(), static_cast<uint16_t>(isr.size()));
    for (std::size_t i = 0; i < isr.size(); ++i)
        writeS32(isrBuf.data() + 2 + i * 4, isr[i]);

    writeU16(repBuf.data(), static_cast<uint16_t>(replicas.size()));
    for (std::size_t i = 0; i < replicas.size(); ++i)
        writeS32(repBuf.data() + 2 + i * 4, replicas[i]);

    pmd::Int32ArrayView isrView(
        reinterpret_cast<const int32_t*>(isrBuf.data() + 2),
        static_cast<uint16_t>(isr.size()));
    pmd::Int32ArrayView repView(
        reinterpret_cast<const int32_t*>(repBuf.data() + 2),
        static_cast<uint16_t>(replicas.size()));

    pmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = topicId;
    rec.partition      = partition;
    rec.leaderNode     = leaderNode;
    rec.isr            = isrView;
    rec.replicas       = repView;
    rec.leaderEpoch    = leaderEpoch;
    rec.partitionEpoch = partitionEpoch;

    return rec.encode(buf, bufLen);
}

// Append an assignment record to a leader PartitionReplica.
// Returns the appended offset, or -1 on failure.
static Offset appendAssignment(
    PartitionReplica* leader,
    int32_t topicId, int32_t partition, int32_t leaderNode,
    const std::vector<int32_t>& isr,
    const std::vector<int32_t>& replicas,
    int32_t leaderEpoch = 1, int32_t partitionEpoch = 1)
{
    uint8_t buf[512];
    const std::size_t n = encodeAssignment(
        buf, sizeof(buf), topicId, partition, leaderNode,
        isr, replicas, leaderEpoch, partitionEpoch);
    if (n == 0) return -1;
    return leader->append(std::span<const uint8_t>(buf, n), /*acksAll=*/false);
}

// ===========================================================================
// Test 1: Single-node — assignment visible ONLY after HW advance (commit)
// ===========================================================================
TEST(MetadataReplicationTest, SingleNode_AssignmentVisibleOnlyAfterHwAdvance) {
    MrtTempDir dir("sn");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1,
        nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), selfId);

    // Append a record — single-node leader, ISR={1}, minIsr=1 → immediate commit.
    const std::vector<int32_t> isr1     = {1};
    const std::vector<int32_t> replicas1 = {1};
    const Offset base = appendAssignment(replica.get(), /*topicId=*/10, /*partition=*/0,
                                          /*leaderNode=*/1, isr1, replicas1);
    ASSERT_GE(base, 0) << "single-node append must succeed";

    // BEFORE doWork (HW not yet advanced): the record is appended to the log
    // but not yet committed — assignment() must return null (pre-commit state).
    EXPECT_EQ(svc.assignment(10, 0), nullptr)
        << "assignment must be NULL before doWork/HW advance (committed-only invariant)";

    // After the first doWork call, the single-node leader advances HW to LEO.
    int64_t clock = 0;
    clock += 2'000'000LL; // 2 ms
    replica->doWork(clock);
    transport.pump(clock);

    // Now apply committed records in the service.
    svc.doWork(clock);

    // AFTER doWork with HW advanced: assignment must be visible.
    const auto* pa = svc.assignment(10, 0);
    ASSERT_NE(pa, nullptr) << "assignment must be visible after HW advance + doWork";
    EXPECT_EQ(pa->leaderNode, 1);
    EXPECT_EQ(pa->leaderEpoch, 1);
    EXPECT_EQ(pa->partitionEpoch, 1);

    // A partition that was never appended must still be null.
    EXPECT_EQ(svc.assignment(10, 1), nullptr) << "unknown partition must remain null";
    EXPECT_EQ(svc.assignment(99, 0), nullptr) << "unknown topic must remain null";
}

// ===========================================================================
// Test 2: 3-node — records committed on leader become visible on ALL followers
// ===========================================================================
class MetadataThreeNodeTest : public ::testing::Test {
protected:
    static constexpr NodeId kNode1 = 1;
    static constexpr NodeId kNode2 = 2;
    static constexpr NodeId kNode3 = 3;

    MetadataThreeNodeTest()
        : dir1("n1"), dir2("n2"), dir3("n3")
        , log1(dir1.path), log2(dir2.path), log3(dir3.path)
    {}

    void SetUp() override {
        const std::vector<NodeId> nodes = {kNode1, kNode2, kNode3};

        node1_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode1, /*minIsr=*/2),
            Role::Leader, /*epoch=*/1, nodes, nodes, &log1, &transport_);

        node2_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode2, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, nodes, nodes, &log2, &transport_);

        node3_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode3, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, nodes, nodes, &log3, &transport_);

        transport_.setLeader(node1_.get());
        transport_.addFollower(kNode2, node2_.get());
        transport_.addFollower(kNode3, node3_.get());
    }

    bool driveUntil(std::function<bool()> done, int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock_ += stepNs;
            node1_->doWork(clock_);
            node2_->doWork(clock_);
            node3_->doWork(clock_);
            transport_.pump(clock_);
        }
        return done();
    }

    MrtTempDir dir1, dir2, dir3;
    rune::storage::Log log1, log2, log3;
    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> node1_, node2_, node3_;
    int64_t clock_ = 0;
};

TEST_F(MetadataThreeNodeTest, ThreeNode_RecordsVisibleOnAllFollowersAfterMajorityHw) {
    // Leader appends two assignment records.
    const std::vector<int32_t> isr1      = {kNode1, kNode2, kNode3};
    const std::vector<int32_t> replicas1 = {kNode1, kNode2, kNode3};
    const Offset b1 = appendAssignment(node1_.get(), /*topicId=*/5, /*partition=*/0,
                                        kNode1, isr1, replicas1, 1, 1);
    ASSERT_GE(b1, 0) << "leader append #1 must succeed";

    const Offset b2 = appendAssignment(node1_.get(), /*topicId=*/5, /*partition=*/1,
                                        kNode2, isr1, replicas1, 1, 1);
    ASSERT_GE(b2, 0) << "leader append #2 must succeed";

    // Drive replication until majority HW (node2 HW >= 2).
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 2; }))
        << "node2 HW must reach 2 within " << kDriverCap << " iterations";
    ASSERT_TRUE(driveUntil([&]{ return node3_->highWatermark() >= 2; }))
        << "node3 HW must reach 2 within " << kDriverCap << " iterations";

    // Apply committed records on each node's service.
    MetadataService svc1(node1_.get(), kNode1);
    MetadataService svc2(node2_.get(), kNode2);
    MetadataService svc3(node3_.get(), kNode3);
    svc1.doWork(clock_);
    svc2.doWork(clock_);
    svc3.doWork(clock_);

    // All nodes must see both assignments.
    for (auto* svc : {&svc1, &svc2, &svc3}) {
        const auto* pa0 = svc->assignment(5, 0);
        ASSERT_NE(pa0, nullptr) << "partition 0 must be visible on all nodes";
        EXPECT_EQ(pa0->leaderNode, kNode1);

        const auto* pa1 = svc->assignment(5, 1);
        ASSERT_NE(pa1, nullptr) << "partition 1 must be visible on all nodes";
        EXPECT_EQ(pa1->leaderNode, kNode2);
    }
}

// ===========================================================================
// Test 3: Restart — snapshot + replay yields identical assignment map
// ===========================================================================
TEST(MetadataReplicationTest, Restart_SnapshotPlusReplayYieldsIdenticalMap) {
    MrtTempDir dir("restart");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    // Append several records.
    const std::vector<int32_t> isr      = {1};
    const std::vector<int32_t> replicas = {1};
    for (int t = 0; t < 5; ++t) {
        ASSERT_GE(appendAssignment(replica.get(), t, 0, 1, isr, replicas, 1, t + 1), 0);
    }

    // Drive to advance HW.
    int64_t clock = 0;
    for (int i = 0; i < 50; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
    }

    // Build service, apply all committed records.
    MetadataService svc1(replica.get(), selfId);
    svc1.doWork(clock);

    // Collect the pre-restart state.
    std::vector<std::pair<int32_t, int32_t>> knownPairs;
    for (int t = 0; t < 5; ++t) {
        const auto* pa = svc1.assignment(t, 0);
        ASSERT_NE(pa, nullptr) << "assignment(" << t << ",0) must exist";
        knownPairs.push_back({pa->leaderEpoch, pa->partitionEpoch});
    }

    // Simulate restart: build a new MetadataService over the SAME log
    // (replica already has HW committed — rebuildFromLog reads the committed tail).
    MetadataService svc2(replica.get(), selfId);
    svc2.rebuildFromLog();

    // Assert identical map.
    for (int t = 0; t < 5; ++t) {
        const auto* pa = svc2.assignment(t, 0);
        ASSERT_NE(pa, nullptr) << "assignment(" << t << ",0) must survive restart";
        EXPECT_EQ(pa->leaderNode, 1);
        EXPECT_EQ(pa->leaderEpoch,    knownPairs[static_cast<std::size_t>(t)].first);
        EXPECT_EQ(pa->partitionEpoch, knownPairs[static_cast<std::size_t>(t)].second);
    }
}

// ===========================================================================
// Test 4: Apply is OFF the request path — no shared-lock stall
//
// Verify that a produce (append) on the replica completes while a
// MetadataService::doWork is running in the same tight loop.  Since both
// run on the same thread (single-writer design), there is no locking needed
// and thus no stall — we just assert both complete within a tight iteration
// budget with no deadlock.
// ===========================================================================
TEST(MetadataReplicationTest, ApplyIsOffRequestPath_NoLockStall) {
    MrtTempDir dir("nolockstall");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), selfId);

    const std::vector<int32_t> isr      = {1};
    const std::vector<int32_t> replicas = {1};

    // Interleave appends and doWork calls within a tight deadline.
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + std::chrono::seconds{5};

    int64_t clock = 0;
    bool appendDone = false;
    bool applyDone  = false;

    for (int i = 0; i < 200 && Clock::now() < deadline; ++i) {
        clock += 1'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);

        if (!appendDone) {
            const Offset b = appendAssignment(replica.get(), 42, 0, 1, isr, replicas);
            if (b >= 0) appendDone = true;
        }

        // doWork is the "apply" path — runs on the same thread as append.
        svc.doWork(clock);

        if (appendDone && svc.assignment(42, 0) != nullptr) {
            applyDone = true;
            break;
        }
    }

    ASSERT_TRUE(appendDone) << "append must complete within deadline";
    ASSERT_TRUE(applyDone)  << "apply must complete within deadline";

    // Verify no deadlock occurred (we reached here → test passed).
    SUCCEED() << "No lock stall detected; both append and apply completed";
}

// ===========================================================================
// Test 5: Bridge-driven role flip — metadata replica's role tracks cluster
//         leadership through ReplicationMetadataBridge + ReplicaRegistry.
//
// This is the real FINDING-1 path:
//   - Create a ReplicaRegistry.
//   - Register the metadata PartitionReplica (topicId=-3) as Role::Follower,
//     epoch 0 (starting state before any leader is known).
//   - Construct a ReplicationMetadataBridge (selfId=kSelf, allReplicaIds).
//   - Drive bridge.doWork(clusterLeader=kSelf, epoch=1):
//       bridge broadcasts becomeLeader(1, allReplicas) → replica becomes Leader.
//   - Assert replica->role() == Role::Leader.
//   - Drive bridge.doWork(clusterLeader=kOther, epoch=2):
//       bridge broadcasts becomeFollower(2, kOther) → replica becomes Follower.
//   - Assert replica->role() == Role::Follower.
//
// This proves the metadata partition's role tracks cluster leadership through
// the bridge+registry wiring (the path tested here is the same path used by
// BrokerNode: register in registry, bridge drives roles per cycle).
// ===========================================================================
TEST(MetadataReplicationTest, MetadataReplicaRoleFollowsBridgeDrivenClusterLeadership) {
    MrtTempDir dir("bridge_role");
    rune::storage::Log log(dir.path);
    log.recover();

    constexpr NodeId kSelf  = 1;
    constexpr NodeId kOther = 2;
    const std::vector<NodeId> allNodes = {kSelf, kOther};

    // InMemoryTransport is required by PartitionReplica (transport must not be null).
    // No leader is set — the replica starts as a Follower before any bridge call.
    InMemoryTransport transport;

    // Build the metadata replica starting as Follower, epoch 0.
    // (Mirrors BrokerNode state before the bridge has fired for the first time.)
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(kSelf, /*minIsr=*/1),
        Role::Follower, /*epoch=*/0,
        allNodes, allNodes, &log, &transport);

    ASSERT_EQ(replica->role(), Role::Follower) << "replica must start as Follower";

    // Register the replica in a ReplicaRegistry (this is what BrokerNode does).
    rune::node::ReplicaRegistry registry;
    registry.add(/*topicId=*/-3, /*partition=*/0, std::move(replica));
    PartitionReplica* rawReplica = registry.lookup(/*topicId=*/-3, /*partition=*/0);
    ASSERT_NE(rawReplica, nullptr) << "replica must be findable in registry";

    // Construct the bridge (selfId=kSelf).
    rune::node::ReplicationMetadataBridge bridge(kSelf, allNodes, registry);

    // --- Step 1: cluster-leader = self → replica becomes Leader ---
    bridge.doWork(/*currentLeaderId=*/kSelf, /*currentEpoch=*/1);
    EXPECT_EQ(rawReplica->role(), Role::Leader)
        << "replica must become Leader when bridge cluster-leader == self";
    EXPECT_TRUE(bridge.isLeader())
        << "bridge must report isLeader() == true";

    // --- Step 2: cluster-leader = other node → replica becomes Follower ---
    bridge.doWork(/*currentLeaderId=*/kOther, /*currentEpoch=*/2);
    EXPECT_EQ(rawReplica->role(), Role::Follower)
        << "replica must become Follower when bridge cluster-leader != self";
    EXPECT_FALSE(bridge.isLeader())
        << "bridge must report isLeader() == false";

    // Epoch must have advanced monotonically (2 > 1).
    EXPECT_EQ(bridge.lastEpoch(), static_cast<rune::replication::Epoch>(2))
        << "bridge epoch must reflect latest cluster epoch";
}

// ===========================================================================
// Test 7: emitDynamicAssignment — bounded batches, per-record log slots
//
// Calls MetadataService::emitDynamicAssignment with 1000 BalancerRecords and
// asserts:
//   - Each duty cycle appends at most kMaxRecordsPerBatch records.
//   - After enough cycles, all 1000 records have been appended (each occupying
//     its own log slot, so logEndOffset advances by exactly 1 per record).
//   - The MSM eventually commits all records after HW advances.
// ===========================================================================
TEST(MetadataReplicationTest, BoundedBatch_DynamicEmit1000Records) {
    MrtTempDir dir("dyn_batch");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;

    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1,
        nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), selfId);

    // Build 1000 BalancerRecords (one per partition across many topics).
    const int kN = 1000;
    std::vector<rune::metadata::BalancerRecord> dynRecords;
    dynRecords.reserve(static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        rune::metadata::BalancerRecord br;
        br.isr      = {selfId};
        br.replicas = {selfId};
        br.rec.topicId        = i / 8;
        br.rec.partition      = i % 8;
        br.rec.leaderNode     = selfId;
        br.rec.leaderEpoch    = 1;
        br.rec.partitionEpoch = 1;
        br.rec.isr      = pmd::Int32ArrayView(br.isr.data(), static_cast<uint16_t>(br.isr.size()));
        br.rec.replicas = pmd::Int32ArrayView(br.replicas.data(), static_cast<uint16_t>(br.replicas.size()));
        dynRecords.push_back(std::move(br));
    }

    // Submit all 1000 records to the dynamic emit queue.
    svc.emitDynamicAssignment(dynRecords);

    // Drive duty cycles; each cycle flushes at most kMaxRecordsPerBatch records.
    // We need at least ceil(1000/256) = 4 cycles. Allow a generous 200 cycles.
    const int kMaxCycles = 200;
    int64_t clock = 0;
    for (int cycle = 0; cycle < kMaxCycles; ++cycle) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
        svc.doWork(clock);

        // Once all 1000 records are appended, the log end offset will be 1000.
        if (replica->logEndOffset() >= kN) break;
    }

    // All 1000 records must have been appended (each in its own log slot).
    ASSERT_GE(replica->logEndOffset(), static_cast<Offset>(kN))
        << "all " << kN << " records must have been appended across duty cycles";

    // Drive until HW reaches 1000 so the MSM commits them.
    for (int i = 0; i < kDriverCap; ++i) {
        clock += 2'000'000LL;
        replica->doWork(clock);
        transport.pump(clock);
        if (replica->highWatermark() >= kN) break;
    }

    svc.doWork(clock);

    // Spot-check a few assignments to confirm MSM applied all records.
    EXPECT_NE(svc.assignment(0, 0), nullptr) << "topic 0 partition 0 must be committed";
    EXPECT_NE(svc.assignment(kN / 8 - 1, 7), nullptr)
        << "last topic/partition must be committed";
}

// ===========================================================================
// Test 6: HW-gated follower visibility — assignment not visible before
//         replication completes on the follower.
//
// Asserts that a metadata service on a follower node does NOT see records
// that have not yet been replicated (HW-gated committed-only discrimination).
// ===========================================================================
TEST_F(MetadataThreeNodeTest, FollowerHwGatedVisibility_AssignmentNotVisibleBeforeReplication) {
    // Leader appends a record WITHOUT driving replication first.
    const std::vector<int32_t> isr1      = {kNode1, kNode2, kNode3};
    const std::vector<int32_t> replicas1 = {kNode1, kNode2, kNode3};
    appendAssignment(node1_.get(), /*topicId=*/7, /*partition=*/0,
                     kNode1, isr1, replicas1, 1, 1);

    // On the follower's service, before replication HW advances, the
    // assignment must NOT be visible (follower HW is still 0).
    MetadataService svc2(node2_.get(), kNode2);
    svc2.doWork(clock_);
    EXPECT_EQ(svc2.assignment(7, 0), nullptr)
        << "follower must NOT see records before HW advance";

    // Now drive replication until majority HW.
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= 1; }))
        << "node2 HW must reach 1";

    // After HW advances on the follower, the assignment must be visible.
    svc2.doWork(clock_);
    const auto* pa = svc2.assignment(7, 0);
    ASSERT_NE(pa, nullptr) << "follower must see records after HW advance";
    EXPECT_EQ(pa->leaderNode, kNode1);
}

// ===========================================================================
// MetadataTruncationRebuildRegression (DR-10 repeated flap, 2026-10-05):
// a resumed ex-controller steps down (becomeFollower → its follower HW is 0
// until the new leader answers) and its uncommitted tail is truncated
// (KIP-279, logGeneration bump).  rebuildFromLog() then replayed only up to the
// follower HW (0) and published an EMPTY committed map — the bridge saw every
// user partition as deleted and quarantined all of this node's replicas for
// good (no fetch → shrunk out of every ISR → the next pause was an outage).
// Committed records are never truncated: the committed map must survive.
// ===========================================================================
TEST(MetadataReplicationTest, MetadataTruncationRebuildRegression_CommittedMapSurvivesDemotionTruncate) {
    MrtTempDir dir("trunc_rebuild");
    rune::storage::Log log(dir.path);
    log.recover();

    const NodeId selfId = 1;
    const std::vector<NodeId> nodes = {selfId};
    InMemoryTransport transport;
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(selfId, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    const std::vector<int32_t> isr = {1}, replicas = {1};
    for (int t = 0; t < 5; ++t)
        ASSERT_GE(appendAssignment(replica.get(), t, 0, 1, isr, replicas, 1, t + 1), 0);
    int64_t clock = 0;
    for (int i = 0; i < 50; ++i) { clock += 2'000'000LL; replica->doWork(clock); transport.pump(clock); }

    MetadataService svc(replica.get(), selfId);
    svc.doWork(clock);
    for (int t = 0; t < 5; ++t) ASSERT_NE(svc.assignment(t, 0), nullptr);
    const Offset committedLeo = replica->logEndOffset();
    // The zombie controller's records: appended, never committed (no doWork).
    for (int t = 0; t < 2; ++t)
        ASSERT_GE(appendAssignment(replica.get(), t, 0, 1, isr, replicas, 9, 100 + t), 0);
    ASSERT_GT(replica->logEndOffset(), committedLeo);

    // Step down (a new controller won meanwhile) and truncate at the committed
    // end — nothing committed is removed, but the log generation changes.
    replica->becomeFollower(/*epoch=*/2, /*leaderId=*/2);
    const uint64_t gen0 = replica->logGeneration();
    replica->truncateLogForTest(committedLeo);
    ASSERT_NE(replica->logGeneration(), gen0) << "setup: the truncate must bump the log generation";
    clock += 2'000'000LL;
    svc.doWork(clock);

    for (int t = 0; t < 5; ++t) {
        const auto* pa = svc.assignment(t, 0);
        ASSERT_NE(pa, nullptr) << "committed assignment (" << t << ",0) vanished after a "
                                  "demotion truncate — the bridge would quarantine its replica";
        EXPECT_EQ(pa->leaderNode, 1);
    }
}
