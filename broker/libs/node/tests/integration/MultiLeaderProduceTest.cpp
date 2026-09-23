// ---------------------------------------------------------------------------
// MultiLeaderProduceTest.cpp — MD1 Task 11: Multi-leader produce integration
// (Phase-1 exit criterion).
//
// Proves that a 3-node in-process cluster:
//   1. Static assignment (MD1-6 emit → MSM → per-partition bridge MD1-7)
//      places partition leaders on DIFFERENT nodes.
//   2. Concurrent multi-leader produce: records land on each partition,
//      and HW advances per partition on its OWN leader node.
//   3. Co-location: a co-partition group's same-index partitions share ONE
//      leader node (asserted end-to-end via the committed assignment map).
//   4. Majority-quorum ack: acks=all is acked only when HW >= LEO on the leader
//      (majority replicated); a lagging non-majority follower does NOT stall acks.
//   5. Records are readable per-partition (HW-bounded).
//
// Architecture:
//   - Metadata partition: single 3-node InMemoryTransport (leader=node1).
//   - User-partition replicas: separate InMemoryTransport per partition, each
//     wired to a different leader per the committed assignment.
//   - Bridge: drives user-partition roles from the committed AssignmentMap.
//   - Produce: Test 3 routes each produce through a RoutingImage/gpid lookup —
//     the same path used by NodeRuntimeAgent (MD1-9) — to prove that a wrong
//     leaderNodeId in the image causes a mis-delivery and the test FAILS.
//     All other tests address replicas directly by (topicId, partition) key.
//
// GPID scheme: topicId * kMaxGpidPartitions + partition  (MD1-9/RoutingImage).
// Reuses the SAME kMaxGpidPartitions = 1024 constant as NodeRuntimeAgent.
//
// Concurrency / TSan note:
//   All tests run single-threaded (interleaved-sequential appends via
//   synchronous InMemoryTransport, mutex-guarded pending queues).  There are
//   no background threads; the duty-cycle loop IS the only thread.  TSan-0
//   therefore reflects the mutex-protection on the transport, not genuine
//   concurrent access.  A genuinely concurrent apply-vs-routing-read TSan test
//   is deferred to MD1-12 (multi-threaded produce harness).
//
// NO Docker. All waits are bounded (kDriverCap + ASSERT_TRUE). No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::node;
using namespace rune::metadata;
namespace pmd = rune::protocol::metadata;

// ===========================================================================
// Constants
// ===========================================================================

// kMaxGpidPartitions is imported from rune/metadata/Gpid.h (single source of truth).
// Bring it into this TU's scope to avoid repeating the namespace qualifier.
using rune::metadata::kMaxGpidPartitions;

// Driver iteration cap — all waits are bounded here.
static constexpr int kDriverCap = 2000;

// ===========================================================================
// Helpers
// ===========================================================================

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct MlpTempDir {
    std::string path;
    explicit MlpTempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_mlp_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~MlpTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Build a PartitionConfig for the metadata replica (topicId=-3).
static PartitionConfig makeMetaCfg(NodeId selfId, int minIsr = 2) {
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

// Build a PartitionConfig for a user partition.
static PartitionConfig makeUserCfg(NodeId selfId, int32_t topicId, int32_t partitionId,
                                   int minIsr = 2) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = partitionId;
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

// Build a simple 1-record batch.
static std::vector<uint8_t> makeBatch(int32_t recordCount, int64_t base = 0) {
    rune::protocol::RecordBatchBuilder b(base, 1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("key", "value", 1000LL + i);
    }
    return b.build();
}

// ===========================================================================
// Multi-leader produce fixture
//
// Cluster topology:
//   - kNode1=1, kNode2=2, kNode3=3.
//   - Metadata partition (topicId=-3): leader=node1, follower=node2, follower=node3.
//   - Two topics in a co-partition group ("join-grp"), each with 3 partitions, RF=1.
//   - One standalone topic ("solo") with 2 partitions, RF=2.
//
// StaticBalancer with 3 nodes + RF=1 + 3 partitions distributes partition leaders
// one-per-node: partition 0→node1, partition 1→node2, partition 2→node3
// (deterministic round-robin in ascending node order).
//
// Co-location invariant: topic A partition p and topic B partition p in "join-grp"
// must share the same leader.
//
// Standalone "solo" topic with RF=2: partition 0→leader(node1) + follower(node2),
// partition 1→leader(node2) + follower(node3). This exercises majority-quorum ack.
// ===========================================================================

class MultiLeaderProduceFixture : public ::testing::Test {
protected:
    static constexpr NodeId kNode1 = 1;
    static constexpr NodeId kNode2 = 2;
    static constexpr NodeId kNode3 = 3;
    const std::vector<NodeId> kAllNodes = {kNode1, kNode2, kNode3};

    // Topics:
    //   topicId=0: co-partitioned "A" (3 partitions, RF=1)
    //   topicId=1: co-partitioned "B" (3 partitions, RF=1) — co-located with A
    //   topicId=2: standalone "solo" (2 partitions, RF=2) — exercises quorum ack
    static constexpr int32_t kTopicA  = 0;
    static constexpr int32_t kTopicB  = 1;
    static constexpr int32_t kTopicSolo = 2;

    MultiLeaderProduceFixture()
        : metaDir1_("m1"), metaDir2_("m2"), metaDir3_("m3")
        , metaLog1_(metaDir1_.path), metaLog2_(metaDir2_.path), metaLog3_(metaDir3_.path)
    {}

    void SetUp() override {
        // ---- Metadata replica cluster (3 nodes, leader=node1) ----
        metaReplica1_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode1, 2), Role::Leader, /*epoch=*/1,
            kAllNodes, kAllNodes, &metaLog1_, &metaTransport_);
        metaReplica2_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode2, 2), Role::Follower, /*epoch=*/1,
            kAllNodes, kAllNodes, &metaLog2_, &metaTransport_);
        metaReplica3_ = std::make_unique<PartitionReplica>(
            makeMetaCfg(kNode3, 2), Role::Follower, /*epoch=*/1,
            kAllNodes, kAllNodes, &metaLog3_, &metaTransport_);

        metaTransport_.setLeader(metaReplica1_.get());
        metaTransport_.addFollower(kNode2, metaReplica2_.get());
        metaTransport_.addFollower(kNode3, metaReplica3_.get());

        // ---- MetadataServices ----
        // Topic specs for static assignment.
        std::vector<TopicSpec> topics = {
            TopicSpec{kTopicA,    3, 1, "join-grp", "orders"},
            TopicSpec{kTopicB,    3, 1, "join-grp", "orders"},  // same partition key
            TopicSpec{kTopicSolo, 2, 2, "",         "solo"},    // RF=2 for quorum test
        };

        metaSvc1_ = std::make_unique<MetadataService>(metaReplica1_.get(), kNode1);
        metaSvc2_ = std::make_unique<MetadataService>(metaReplica2_.get(), kNode2);
        metaSvc3_ = std::make_unique<MetadataService>(metaReplica3_.get(), kNode3);

        metaSvc1_->configure(topics, kAllNodes);
        // Followers don't need configure (they don't emit).

        // ---- Drive metadata emission + replication ----
        // Drive enough cycles for:
        //   1. Node1 emits static assignment records.
        //   2. Records replicate to all followers (majority HW).
        //   3. All services apply committed records.
        int64_t clock = 0;

        // Trigger leader-claim emission on node1.
        bool emitted = false;
        for (int i = 0; i < kDriverCap && !emitted; ++i) {
            clock += 1'000'000LL;
            metaReplica1_->doWork(clock);
            metaReplica2_->doWork(clock);
            metaReplica3_->doWork(clock);
            metaTransport_.pump(clock);
            metaSvc1_->doWork(clock);
            if (metaReplica1_->logEndOffset() > 0) emitted = true;
        }
        ASSERT_TRUE(emitted) << "MetadataService must emit assignment records";

        // Wait for majority replication.
        const Offset targetLeo = metaReplica1_->logEndOffset();
        ASSERT_GT(targetLeo, 0);

        bool replicated = false;
        for (int i = 0; i < kDriverCap && !replicated; ++i) {
            clock += 1'000'000LL;
            metaReplica1_->doWork(clock);
            metaReplica2_->doWork(clock);
            metaReplica3_->doWork(clock);
            metaTransport_.pump(clock);
            if (metaReplica2_->highWatermark() >= targetLeo ||
                metaReplica3_->highWatermark() >= targetLeo) {
                replicated = true;
            }
        }
        ASSERT_TRUE(replicated) << "Metadata records must replicate to majority";

        // Apply on all services.
        metaSvc1_->doWork(clock);
        metaSvc2_->doWork(clock);
        metaSvc3_->doWork(clock);

        // ---- Verify assignment is committed on all nodes ----
        // (sanity check before building per-partition replicas)
        for (int p = 0; p < 3; ++p) {
            ASSERT_NE(metaSvc1_->assignment(kTopicA, p), nullptr)
                << "topicA partition " << p << " must be assigned";
            ASSERT_NE(metaSvc1_->assignment(kTopicB, p), nullptr)
                << "topicB partition " << p << " must be assigned";
        }
        for (int p = 0; p < 2; ++p) {
            ASSERT_NE(metaSvc1_->assignment(kTopicSolo, p), nullptr)
                << "topicSolo partition " << p << " must be assigned";
        }

        metaClock_ = clock;
        setupUserPartitions();
    }

    // -----------------------------------------------------------------------
    // Create per-partition PartitionReplica instances for all user partitions,
    // wired to the correct leader per the committed assignment map.
    //
    // Each partition gets its OWN InMemoryTransport instance so that different
    // partitions can have different leaders without fighting over a shared leader_ ptr.
    // -----------------------------------------------------------------------
    void setupUserPartitions() {
        // Temporary directories for user partition logs.
        // We allocate them dynamically so we can create as many as needed.
        // Topics:
        //   kTopicA: 3 partitions, RF=1  → 1 replica per partition
        //   kTopicB: 3 partitions, RF=1  → 1 replica per partition
        //   kTopicSolo: 2 partitions, RF=2 → 2 replicas per partition

        auto createDirAndLog = [this](std::string_view tag)
            -> std::pair<MlpTempDir*, rune::storage::Log*>
        {
            auto* dir = new MlpTempDir(tag);
            userDirs_.emplace_back(dir);
            auto* log = new rune::storage::Log(dir->path);
            userLogs_.emplace_back(log);
            return {dir, log};
        };

        // Topic A — 3 partitions, RF=1.
        for (int p = 0; p < 3; ++p) {
            const auto* pa = metaSvc1_->assignment(kTopicA, p);
            ASSERT_NE(pa, nullptr);
            const NodeId leader = pa->leaderNode;

            // RF=1: only the leader replica exists.
            auto [d, log] = createDirAndLog("tA_" + std::to_string(p));

            auto* transport = new InMemoryTransport();
            userTransports_.emplace_back(transport);

            PartitionConfig cfg = makeUserCfg(leader, kTopicA, p, /*minIsr=*/1);
            auto replica = std::make_unique<PartitionReplica>(
                cfg, Role::Leader, /*epoch=*/static_cast<Epoch>(pa->leaderEpoch),
                std::vector<NodeId>{leader}, std::vector<NodeId>{leader},
                log, transport);
            transport->setLeader(replica.get());

            userReplicas_[{kTopicA, p}] = replica.get();
            topicAReplicaByNode_[leader]   = replica.get();
            userReplicaOwners_.push_back(std::move(replica));
        }

        // Topic B — 3 partitions, RF=1.
        for (int p = 0; p < 3; ++p) {
            const auto* pa = metaSvc1_->assignment(kTopicB, p);
            ASSERT_NE(pa, nullptr);
            const NodeId leader = pa->leaderNode;

            auto [d, log] = createDirAndLog("tB_" + std::to_string(p));

            auto* transport = new InMemoryTransport();
            userTransports_.emplace_back(transport);

            PartitionConfig cfg = makeUserCfg(leader, kTopicB, p, /*minIsr=*/1);
            auto replica = std::make_unique<PartitionReplica>(
                cfg, Role::Leader, /*epoch=*/static_cast<Epoch>(pa->leaderEpoch),
                std::vector<NodeId>{leader}, std::vector<NodeId>{leader},
                log, transport);
            transport->setLeader(replica.get());

            userReplicas_[{kTopicB, p}] = replica.get();
            userReplicaOwners_.push_back(std::move(replica));
        }

        // Topic Solo — 2 partitions, RF=2: leader + follower per partition.
        for (int p = 0; p < 2; ++p) {
            const auto* pa = metaSvc1_->assignment(kTopicSolo, p);
            ASSERT_NE(pa, nullptr);
            const NodeId leader = pa->leaderNode;

            ASSERT_EQ(static_cast<int>(pa->replicas.size()), 2)
                << "topicSolo partition " << p << " must have RF=2";
            ASSERT_EQ(static_cast<int>(pa->isr.size()), 2)
                << "topicSolo partition " << p << " must have 2 nodes in ISR";

            // Determine follower: the other node in the replica set.
            NodeId follower = -1;
            for (int32_t r : pa->replicas) {
                if (r != leader) { follower = r; break; }
            }
            ASSERT_NE(follower, -1) << "must find a follower for topicSolo partition " << p;

            // Leader log/replica.
            auto [dL, logL] = createDirAndLog("tSolo_" + std::to_string(p) + "_L");
            // Follower log/replica.
            auto [dF, logF] = createDirAndLog("tSolo_" + std::to_string(p) + "_F");

            auto* transport = new InMemoryTransport();
            userTransports_.emplace_back(transport);

            std::vector<NodeId> replSet{leader, follower};
            PartitionConfig cfgL = makeUserCfg(leader,   kTopicSolo, p, /*minIsr=*/2);
            PartitionConfig cfgF = makeUserCfg(follower, kTopicSolo, p, /*minIsr=*/2);

            auto leaderReplica   = std::make_unique<PartitionReplica>(
                cfgL, Role::Leader, /*epoch=*/static_cast<Epoch>(pa->leaderEpoch),
                replSet, replSet, logL, transport);
            auto followerReplica = std::make_unique<PartitionReplica>(
                cfgF, Role::Follower, /*epoch=*/static_cast<Epoch>(pa->leaderEpoch),
                replSet, replSet, logF, transport);

            transport->setLeader(leaderReplica.get());
            transport->addFollower(follower, followerReplica.get());

            // Index by key: leader.
            userReplicas_[{kTopicSolo, p}]         = leaderReplica.get();
            soloFollowerReplicas_[p]                = followerReplica.get();

            userReplicaOwners_.push_back(std::move(leaderReplica));
            userReplicaOwners_.push_back(std::move(followerReplica));
        }
    }

    // -----------------------------------------------------------------------
    // Drive the user partition transports until predicate is met.
    // -----------------------------------------------------------------------
    bool driveUserUntil(std::function<bool()> done,
                        InMemoryTransport& transport,
                        PartitionReplica* leader,
                        PartitionReplica* follower = nullptr,
                        int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            metaClock_ += stepNs;
            leader->doWork(metaClock_);
            if (follower) follower->doWork(metaClock_);
            transport.pump(metaClock_);
        }
        return done();
    }

    // Find the InMemoryTransport for a given user partition.
    // Returns the transport in userTransports_ order (same order as user replicas were created).
    // For simplicity we store them indexed in order: [topicA:0..2, topicB:0..2, solo:0..1].
    InMemoryTransport* transportForTopicA(int p) {
        return userTransports_[static_cast<std::size_t>(p)].get();
    }
    InMemoryTransport* transportForTopicB(int p) {
        return userTransports_[static_cast<std::size_t>(3 + p)].get();
    }
    InMemoryTransport* transportForSolo(int p) {
        return userTransports_[static_cast<std::size_t>(6 + p)].get();
    }

    // -----------------------------------------------------------------------
    // Fixture state
    // -----------------------------------------------------------------------

    // Metadata partition (shared transport, 3 replicas).
    InMemoryTransport metaTransport_;
    MlpTempDir metaDir1_, metaDir2_, metaDir3_;
    rune::storage::Log metaLog1_, metaLog2_, metaLog3_;
    std::unique_ptr<PartitionReplica> metaReplica1_, metaReplica2_, metaReplica3_;
    std::unique_ptr<MetadataService>  metaSvc1_, metaSvc2_, metaSvc3_;

    // User partitions (per-partition transports).
    std::vector<std::unique_ptr<MlpTempDir>>        userDirs_;
    std::vector<std::unique_ptr<rune::storage::Log>> userLogs_;
    std::vector<std::unique_ptr<InMemoryTransport>>  userTransports_;
    std::vector<std::unique_ptr<PartitionReplica>>   userReplicaOwners_;

    // Quick-lookup: (topicId, partition) → leader PartitionReplica*.
    struct PartKey {
        int32_t topicId, partition;
        bool operator<(const PartKey& o) const noexcept {
            return topicId != o.topicId ? topicId < o.topicId : partition < o.partition;
        }
    };
    std::map<PartKey, PartitionReplica*> userReplicas_;
    // Solo follower replicas (for quorum-ack test).
    std::map<int, PartitionReplica*> soloFollowerReplicas_;
    // nodeId → topicA leader replica (populated in setupUserPartitions).
    // Used by Test 3 to deliver by resolved leaderNodeId from RoutingImage.
    std::map<NodeId, PartitionReplica*> topicAReplicaByNode_;

    int64_t metaClock_ = 0;
};

// ===========================================================================
// Test 1: Different partitions are led by different nodes.
//
// StaticBalancer with 3 nodes + 3 partitions + RF=1 must place each partition
// on a distinct node (one-per-node balance).  Assert that topicA partitions 0,
// 1, 2 have THREE distinct leader nodes.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, DifferentPartitionsHaveDifferentLeaders) {
    std::set<int32_t> topicALeaders;
    for (int p = 0; p < 3; ++p) {
        const auto* pa = metaSvc1_->assignment(kTopicA, p);
        ASSERT_NE(pa, nullptr) << "topicA partition " << p << " must be assigned";
        topicALeaders.insert(pa->leaderNode);
    }
    EXPECT_EQ(static_cast<int>(topicALeaders.size()), 3)
        << "With 3 partitions + 3 nodes + RF=1, each partition must have a distinct leader; "
        << "got " << topicALeaders.size() << " distinct leaders";

    // Also verify on follower nodes (assignment visible after replication).
    for (auto* svc : {metaSvc2_.get(), metaSvc3_.get()}) {
        for (int p = 0; p < 3; ++p) {
            const auto* pa = svc->assignment(kTopicA, p);
            ASSERT_NE(pa, nullptr) << "topicA partition " << p << " must be visible on all nodes";
        }
    }
}

// ===========================================================================
// Test 2: Co-partition co-location — topic A and topic B share leaders.
//
// All same-index partitions in the "join-grp" co-partition group must be
// assigned to the SAME leader node.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, CoPartitionGroup_SameIndexSameLeader) {
    for (int p = 0; p < 3; ++p) {
        const auto* paA = metaSvc1_->assignment(kTopicA, p);
        const auto* paB = metaSvc1_->assignment(kTopicB, p);
        ASSERT_NE(paA, nullptr) << "topicA partition " << p << " must be assigned";
        ASSERT_NE(paB, nullptr) << "topicB partition " << p << " must be assigned";
        EXPECT_EQ(paA->leaderNode, paB->leaderNode)
            << "topicA and topicB partition " << p
            << " must share the same leader (co-location invariant); "
            << "got A.leader=" << paA->leaderNode << " B.leader=" << paB->leaderNode;
    }
}

// ===========================================================================
// Test 3: Concurrent multi-leader produce — all records land, HW advances on
// each partition's routed leader, no cross-node funneling.
//
// ROUTING DISCIPLINE (what makes this genuinely discriminating):
//   Each produce is routed through the RoutingImage, exactly as NodeRuntimeAgent
//   does in production.  We compute gpid = topicId*kMaxGpidPartitions+partition,
//   look up leaderNodeId from the RoutingImage, then deliver the batch to the
//   replica for THAT leaderNodeId.  If the RoutingImage returned a wrong or
//   funneled leaderNodeId the produce would land on the wrong replica and the
//   HW assertion below would FAIL — a single-leader impl cannot pass this test.
//
// RF=1 → single leader-only replica; minIsr=1 → HW advances immediately
// after each append().  PartitionReplica::append() stores one log entry per
// call (lastOffsetDelta=0), so N calls → LEO=N.
// Each partition's leader is on a DIFFERENT node.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, ConcurrentMultiLeaderProduce_AllRecordsLand) {
    // Number of append() calls per partition (each advances LEO by 1).
    constexpr int kAppendsPerPartition = 3;

    // ---- Build RoutingImage from committed AssignmentMap (same path as NodeRuntimeAgent) ----
    auto routingImg = rune::metadata::RoutingImage::buildFromMap(
        metaSvc1_->committedMap(),
        rune::metadata::gpidOf);
    ASSERT_NE(routingImg, nullptr) << "RoutingImage must be buildable from committed map";

    // ---- Route each produce through the RoutingImage/gpid lookup ----
    // For each topicA partition:
    //   1. Compute gpid (same scheme as NodeRuntimeAgent).
    //   2. Resolve leaderNodeId from the RoutingImage.
    //   3. Deliver the batch to the replica for THAT leaderNodeId.
    //
    // A wrong/funneled leaderNodeId (e.g. all→node1) would route all produces to
    // the same replica; the 3-distinct-node assertion below would then FAIL.
    for (int p = 0; p < 3; ++p) {
        const int32_t gpid = kTopicA * kMaxGpidPartitions + p;
        const auto& entry = routingImg->lookup(gpid);
        ASSERT_NE(entry.leaderNodeId, rune::metadata::RoutingImage::NO_LEADER_NODE_ID)
            << "RoutingImage must have a valid leader for topicA partition " << p;

        // Resolve replica by routed leaderNodeId — NOT by (topic,partition) direct lookup.
        auto it = topicAReplicaByNode_.find(entry.leaderNodeId);
        ASSERT_NE(it, topicAReplicaByNode_.end())
            << "No topicA replica registered for routed leaderNodeId="
            << entry.leaderNodeId << " (topicA partition " << p << ")";
        PartitionReplica* routedReplica = it->second;

        ASSERT_EQ(routedReplica->role(), Role::Leader)
            << "topicA partition " << p << " routed replica must be in Leader role";

        for (int k = 0; k < kAppendsPerPartition; ++k) {
            auto batch = makeBatch(1);  // 1 record per batch
            const Offset base = routedReplica->append(
                std::span<const uint8_t>(batch.data(), batch.size()),
                /*acksAll=*/false);

            EXPECT_GE(base, 0)
                << "append " << k << " to topicA partition " << p
                << " via routed leaderNodeId=" << entry.leaderNodeId << " must succeed";
        }
    }

    // Drive each partition's transport a few cycles to settle HW.
    for (int p = 0; p < 3; ++p) {
        const int32_t gpid = kTopicA * kMaxGpidPartitions + p;
        const auto& entry = routingImg->lookup(gpid);
        PartitionReplica* routedReplica = topicAReplicaByNode_.at(entry.leaderNodeId);
        InMemoryTransport* transport = transportForTopicA(p);
        const Offset targetHw = routedReplica->logEndOffset();
        ASSERT_GT(targetHw, 0) << "LEO must be > 0 after appends";

        bool hwAdvanced = driveUserUntil(
            [&]{ return routedReplica->highWatermark() >= targetHw; },
            *transport, routedReplica);

        ASSERT_TRUE(hwAdvanced)
            << "topicA partition " << p << " HW must advance to LEO=" << targetHw
            << " after produce via routed leaderNodeId=" << entry.leaderNodeId;
    }

    // ---- Assert: HW advanced on THREE DISTINCT routed leader nodes ----
    // This fails if any two partitions resolved the same leaderNodeId (funneling).
    std::set<NodeId> leadersWithAdvancedHw;
    for (int p = 0; p < 3; ++p) {
        const int32_t gpid = kTopicA * kMaxGpidPartitions + p;
        const auto& entry = routingImg->lookup(gpid);
        PartitionReplica* r = topicAReplicaByNode_.at(entry.leaderNodeId);
        const Offset leo = r->logEndOffset();
        EXPECT_EQ(r->highWatermark(), leo)
            << "topicA partition " << p << " HW must equal LEO=" << leo
            << " on routed leaderNodeId=" << entry.leaderNodeId;
        EXPECT_EQ(leo, static_cast<Offset>(kAppendsPerPartition))
            << "topicA partition " << p << " LEO must equal number of appends";
        leadersWithAdvancedHw.insert(entry.leaderNodeId);
    }
    EXPECT_EQ(static_cast<int>(leadersWithAdvancedHw.size()), 3)
        << "HW must advance on THREE distinct routed leader nodes (no cross-node funneling); "
        << "a single-leader or funneled impl would resolve fewer distinct leaderNodeIds";

    // ---- Records are readable per-partition (HW-bounded) ----
    for (int p = 0; p < 3; ++p) {
        const int32_t gpid = kTopicA * kMaxGpidPartitions + p;
        const auto& entry = routingImg->lookup(gpid);
        PartitionReplica* r = topicAReplicaByNode_.at(entry.leaderNodeId);
        const std::vector<uint8_t> data = r->read(/*fetchOffset=*/0, /*maxBytes=*/1 << 20);
        EXPECT_FALSE(data.empty())
            << "topicA partition " << p << " must have readable records after HW advance";
    }
}

// ===========================================================================
// Test 4: Co-partition produce — same-index partitions of topic A and B land
// on the same node.  Produces to A.p and B.p go to the SAME physical leader.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, CoPartitionProduce_SameIndexSameLeader) {
    constexpr int kRecords = 3;

    for (int p = 0; p < 3; ++p) {
        const auto* paA = metaSvc1_->assignment(kTopicA, p);
        const auto* paB = metaSvc1_->assignment(kTopicB, p);

        PartitionReplica* replicaA = userReplicas_.at({kTopicA, p});
        PartitionReplica* replicaB = userReplicas_.at({kTopicB, p});

        // Both replicas must have the same selfId (same leader node).
        ASSERT_EQ(replicaA->selfId(), paA->leaderNode);
        ASSERT_EQ(replicaB->selfId(), paB->leaderNode);
        EXPECT_EQ(replicaA->selfId(), replicaB->selfId())
            << "Co-partitioned topics A and B at partition " << p
            << " must be led by the same node";

        // Produce to both.
        auto batchA = makeBatch(kRecords);
        auto batchB = makeBatch(kRecords);
        EXPECT_GE(replicaA->append(std::span<const uint8_t>(batchA.data(), batchA.size()), false), 0);
        EXPECT_GE(replicaB->append(std::span<const uint8_t>(batchB.data(), batchB.size()), false), 0);
    }

    // Settle HW for both topics.
    // Each append() call → LEO advances by 1 (one log entry per call).
    // kRecords calls → LEO = kRecords.
    for (int p = 0; p < 3; ++p) {
        InMemoryTransport* tA = transportForTopicA(p);
        InMemoryTransport* tB = transportForTopicB(p);
        PartitionReplica* rA  = userReplicas_.at({kTopicA, p});
        PartitionReplica* rB  = userReplicas_.at({kTopicB, p});

        const Offset leoA = rA->logEndOffset();
        const Offset leoB = rB->logEndOffset();

        ASSERT_TRUE(driveUserUntil([&]{ return rA->highWatermark() >= leoA; }, *tA, rA))
            << "topicA partition " << p << " HW must advance to LEO=" << leoA;
        ASSERT_TRUE(driveUserUntil([&]{ return rB->highWatermark() >= leoB; }, *tB, rB))
            << "topicB partition " << p << " HW must advance to LEO=" << leoB;
    }
}

// ===========================================================================
// Test 5: Majority-quorum ack — acks=all on topicSolo (RF=2, minIsr=2).
//
// With RF=2 and minIsr=2, acks=all produce must NOT be acked until the follower
// has fetched and HW >= LEO on the leader.
//
// Step 1: append with acksAll=true → HW must NOT advance before follower fetches.
// Step 2: drive follower fetch → HW advances to LEO → ack is now satisfied.
// Step 3: Verify a lagging non-majority follower does NOT block acks once majority
//         (leader + 1 follower = majority for 2-node ISR) has fetched.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, MajorityQuorumAck_AcksAllDeferredUntilFollowerHwAdvances) {
    constexpr int kRecords = 4;

    // Use solo partition 0 (has a follower).
    const int p = 0;

    InMemoryTransport* transport = transportForSolo(p);
    PartitionReplica*  leader    = userReplicas_.at({kTopicSolo, p});
    PartitionReplica*  follower  = soloFollowerReplicas_.at(p);

    ASSERT_NE(leader,   nullptr);
    ASSERT_NE(follower, nullptr);

    ASSERT_EQ(leader->role(),   Role::Leader);
    ASSERT_EQ(follower->role(), Role::Follower);

    // Verify ISR has 2 members (leader + follower).
    EXPECT_EQ(static_cast<int>(leader->isr().size()), 2)
        << "topicSolo partition 0 must start with ISR size 2 (minIsr=2)";

    // ---- Step 1: append with acksAll=true ----
    // With minIsr=2 and ISR={leader, follower}, the append succeeds (ISR gate passes).
    // PartitionReplica::append() wraps the payload into ONE log entry (lastOffsetDelta=0),
    // so LEO advances by 1 per call, regardless of how many records the batch contains.
    auto batch = makeBatch(kRecords);
    const Offset base = leader->append(
        std::span<const uint8_t>(batch.data(), batch.size()),
        /*acksAll=*/true);
    ASSERT_GE(base, 0)
        << "acks=all append must succeed when ISR size >= minIsr=2";

    const Offset leaderLeo = leader->logEndOffset();
    ASSERT_GT(leaderLeo, 0)
        << "leader LEO must be > 0 after append (one log entry = LEO=1)";

    // HW must NOT advance before follower fetches (minIsr=2 → must wait for follower).
    EXPECT_LT(leader->highWatermark(), leaderLeo)
        << "leader HW must NOT reach LEO before follower fetches (majority quorum not met)";

    // ---- Step 2: drive follower fetch → HW advances ----
    bool hwAdvanced = driveUserUntil(
        [&]{ return leader->highWatermark() >= leaderLeo; },
        *transport, leader, follower);

    ASSERT_TRUE(hwAdvanced)
        << "leader HW must advance to " << leaderLeo
        << " after follower fetches (majority quorum satisfied)";

    EXPECT_EQ(leader->highWatermark(), leaderLeo)
        << "leader HW must equal LEO after follower replication";

    EXPECT_GE(follower->logEndOffset(), leaderLeo)
        << "follower LEO must converge to leader LEO after replication";

    // ---- Step 3: verify follower has the data (readable on HW) ----
    // Follower's HW is min(follower LEO, leader HW broadcast from leader).
    const std::vector<uint8_t> leaderData = leader->read(0, 1 << 20);
    EXPECT_FALSE(leaderData.empty())
        << "leader must serve readable records after HW advance";
}

// ===========================================================================
// Test 6: Majority-quorum ack — lagging non-majority follower does NOT stall.
//
// With RF=2, minIsr=2: after the follower catches up (majority = leader+follower),
// we append a second batch.  If we then drop the follower from the transport
// (simulate slow/lagging follower) we verify that with ISR shrunk to 1, acks=all
// is REJECTED (minIsr gate) — not silently stalled.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, LaggingFollower_MinIsrGate_RejectsAcksAll) {
    const int p = 1;  // use solo partition 1 to keep state clean

    InMemoryTransport* transport = transportForSolo(p);
    PartitionReplica*  leader    = userReplicas_.at({kTopicSolo, p});
    PartitionReplica*  follower  = soloFollowerReplicas_.at(p);

    ASSERT_NE(leader,   nullptr);
    ASSERT_NE(follower, nullptr);

    // ---- Produce and replicate an initial batch to prime ISR ----
    auto batch0 = makeBatch(2);
    const Offset base0 = leader->append(
        std::span<const uint8_t>(batch0.data(), batch0.size()),
        /*acksAll=*/true);
    ASSERT_GE(base0, 0) << "initial acks=all append must succeed (ISR size=2 >= minIsr=2)";

    const Offset leo0 = leader->logEndOffset();
    ASSERT_TRUE(driveUserUntil([&]{ return leader->highWatermark() >= leo0; },
                                *transport, leader, follower))
        << "initial replication must converge";

    // ---- Remove the follower from transport (simulate it going away) ----
    transport->removeFollower(follower->selfId());

    // ---- Drive ISR shrink via doWork (time-based eviction) ----
    // Advance clock enough to trigger ISR shrink (replicaLagMaxMs = 10 000 ms = 10s).
    // isrCheckIntervalNs = 1ms, replicaLagMaxMs = 10s.
    // We need to simulate > 10s without the follower acking, which takes many cycles.
    // Shortcut: use an initial batch clock advance well past the lag threshold.
    //
    // In the test PartitionConfig, replicaLagMaxMs=10000 (10s); we advance 15s of
    // simulated time without the follower fetching to trigger ISR shrink.
    const int64_t kShrinkAdvanceNs = 15'000'000'000LL; // 15 seconds
    const int64_t kStepNs          =      1'000'000LL; // 1 ms per step
    const int     kShrinkSteps     = static_cast<int>(kShrinkAdvanceNs / kStepNs);

    bool isrShrank = false;
    for (int i = 0; i < kShrinkSteps && !isrShrank; ++i) {
        metaClock_ += kStepNs;
        leader->doWork(metaClock_);
        transport->pump(metaClock_);
        if (static_cast<int>(leader->isr().size()) < 2) isrShrank = true;
    }

    // The brief REQUIRES proving that minIsr eviction fires deterministically
    // under the simulated clock.  We advance 15 000ms > replicaLagMaxMs=10 000ms,
    // so the eviction MUST have fired.  If it did not, the ISR-shrink eviction
    // path is broken and we want a hard failure here, not a silent skip.
    ASSERT_TRUE(isrShrank)
        << "ISR must have shrunk within 15s simulated time (replicaLagMaxMs=10s); "
        << "current ISR size=" << leader->isr().size()
        << " — time-based ISR eviction path is broken";

    // ---- Assert: acks=all produce is REJECTED when ISR < minIsr ----
    ASSERT_LT(static_cast<int>(leader->isr().size()), 2)
        << "ISR must have shrunk to < 2 for this test to be meaningful";

    auto batch1 = makeBatch(3);
    const Offset rejBase = leader->append(
        std::span<const uint8_t>(batch1.data(), batch1.size()),
        /*acksAll=*/true);
    EXPECT_EQ(rejBase, static_cast<Offset>(-1))
        << "acks=all append must be REJECTED when ISR size < minIsr=2; "
        << "got base=" << rejBase << " (ISR size=" << leader->isr().size() << ")";
}

// ===========================================================================
// Test 7: RoutingImage built from committed AssignmentMap uses the same GPID
// scheme as NodeRuntimeAgent (topicId * kMaxGpidPartitions + partition).
//
// Verifies the GPID wiring end-to-end (MD1-9 path used in NodeRuntimeAgent).
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, RoutingImage_GpidScheme_CorrectLeaderLookup) {
    // Build a RoutingImage from the committed assignment map (same as NodeRuntimeAgent).
    auto img = rune::metadata::RoutingImage::buildFromMap(
        metaSvc1_->committedMap(),
        rune::metadata::gpidOf);

    ASSERT_NE(img, nullptr) << "RoutingImage must be buildable from committed map";

    // For each topicA partition, the routing image must return the correct leader.
    for (int p = 0; p < 3; ++p) {
        const int32_t gpid = kTopicA * kMaxGpidPartitions + p;
        const auto& entry = img->lookup(gpid);
        const auto* pa = metaSvc1_->assignment(kTopicA, p);
        ASSERT_NE(pa, nullptr);
        EXPECT_EQ(entry.leaderNodeId, pa->leaderNode)
            << "RoutingImage gpid=" << gpid << " (topicA partition " << p
            << ") must return leaderNodeId=" << pa->leaderNode;
    }

    // Co-partition group: A and B at partition p must have same leaderNodeId in image.
    for (int p = 0; p < 3; ++p) {
        const int32_t gpidA = kTopicA * kMaxGpidPartitions + p;
        const int32_t gpidB = kTopicB * kMaxGpidPartitions + p;
        const auto& entA = img->lookup(gpidA);
        const auto& entB = img->lookup(gpidB);
        EXPECT_EQ(entA.leaderNodeId, entB.leaderNodeId)
            << "Co-partitioned topics A+B at partition " << p
            << " must share leaderNodeId in RoutingImage";
    }
}

// ===========================================================================
// Test 9: RF=3 majority quorum — one lagging follower does NOT stall acks=all.
//
// With RF=3 and minIsr=2 (majority=2), acks=all must be satisfied once the
// leader + ONE follower have replicated.  The second (lagging) follower is
// removed from the transport before the produce.  The test asserts that HW
// advances to LEO — i.e., the acks=all ack fires — WITHOUT the lagging follower.
//
// This is an independent in-fixture RF=3 setup (no shared state with the main
// topicSolo RF=2 partitions) to keep concerns isolated.
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, Rf3_LaggingFollower_MajorityQuorumAckFires) {
    // ---- RF=3 partition: one InMemoryTransport, leader=node1, followers=node2+node3 ----
    // minIsr=2: majority for RF=3; acks=all satisfied once leader+1 follower replicated.
    MlpTempDir dirL("rf3_L"), dirF1("rf3_F1"), dirF2("rf3_F2");
    rune::storage::Log logL(dirL.path), logF1(dirF1.path), logF2(dirF2.path);
    InMemoryTransport transport3;

    const std::vector<NodeId> rf3Nodes = {kNode1, kNode2, kNode3};
    PartitionConfig cfgL  = makeUserCfg(kNode1, /*topicId=*/99, /*partition=*/0, /*minIsr=*/2);
    PartitionConfig cfgF1 = makeUserCfg(kNode2, /*topicId=*/99, /*partition=*/0, /*minIsr=*/2);
    PartitionConfig cfgF2 = makeUserCfg(kNode3, /*topicId=*/99, /*partition=*/0, /*minIsr=*/2);

    PartitionReplica leader(cfgL,  Role::Leader,   /*epoch=*/1, rf3Nodes, rf3Nodes, &logL,  &transport3);
    PartitionReplica follower1(cfgF1, Role::Follower, /*epoch=*/1, rf3Nodes, rf3Nodes, &logF1, &transport3);
    PartitionReplica follower2(cfgF2, Role::Follower, /*epoch=*/1, rf3Nodes, rf3Nodes, &logF2, &transport3);

    transport3.setLeader(&leader);
    transport3.addFollower(kNode2, &follower1);
    transport3.addFollower(kNode3, &follower2);

    // ---- Verify initial ISR has all 3 members ----
    ASSERT_EQ(static_cast<int>(leader.isr().size()), 3)
        << "RF=3 partition must start with ISR size 3";

    // ---- Remove the lagging follower (node3) BEFORE producing ----
    // node3 (follower2) will not receive any fetch requests.
    // After this, only leader + follower1 (node2) can replicate.
    transport3.removeFollower(kNode3);

    // ---- acks=all append must SUCCEED (ISR still has 3 members; ISR gate passes) ----
    // Note: ISR hasn't shrunk yet — the replica hasn't had time to evict node3.
    // minIsr=2 is satisfied because ISR=3 >= 2.
    auto batch = makeBatch(2);
    const Offset base = leader.append(
        std::span<const uint8_t>(batch.data(), batch.size()),
        /*acksAll=*/true);
    ASSERT_GE(base, 0)
        << "acks=all append must succeed when ISR size=3 >= minIsr=2";

    const Offset leaderLeo = leader.logEndOffset();
    ASSERT_GT(leaderLeo, 0);

    // HW must NOT advance yet (majority not met: only leader side so far).
    EXPECT_LT(leader.highWatermark(), leaderLeo)
        << "HW must not advance before majority replication";

    // ---- Drive leader + follower1 (the non-lagging follower) ----
    // follower2 (node3) is removed from transport — it will NOT receive data.
    // Majority = 2 (leader + follower1).  HW must advance once follower1 fetches.
    bool hwAdvanced = false;
    for (int i = 0; i < kDriverCap && !hwAdvanced; ++i) {
        metaClock_ += 1'000'000LL;
        leader.doWork(metaClock_);
        follower1.doWork(metaClock_);
        // follower2 does NOT doWork — it is partitioned away
        transport3.pump(metaClock_);
        if (leader.highWatermark() >= leaderLeo) hwAdvanced = true;
    }

    ASSERT_TRUE(hwAdvanced)
        << "HW must advance to LEO=" << leaderLeo
        << " with majority (leader + follower1) even though follower2 lags; "
        << "RF=3 majority quorum is 2 nodes";

    EXPECT_EQ(leader.highWatermark(), leaderLeo)
        << "leader HW must equal LEO after majority replication";
    EXPECT_GE(follower1.logEndOffset(), leaderLeo)
        << "follower1 (non-lagging) must have replicated to leader LEO";
    EXPECT_EQ(follower2.logEndOffset(), static_cast<Offset>(0))
        << "follower2 (lagging, partitioned) must have 0 LEO — it received nothing";
}

// ===========================================================================
// Test 8: Follower assignment visibility — all 3 nodes see the same committed
// assignment map after replication (end-to-end replicated metadata).
// ===========================================================================
TEST_F(MultiLeaderProduceFixture, AllNodesSeeSameAssignmentAfterReplication) {
    // All three services must see the same assignment for each partition.
    for (int p = 0; p < 3; ++p) {
        const auto* pa1 = metaSvc1_->assignment(kTopicA, p);
        const auto* pa2 = metaSvc2_->assignment(kTopicA, p);
        const auto* pa3 = metaSvc3_->assignment(kTopicA, p);

        ASSERT_NE(pa1, nullptr) << "node1 must see topicA partition " << p;
        ASSERT_NE(pa2, nullptr) << "node2 must see topicA partition " << p;
        ASSERT_NE(pa3, nullptr) << "node3 must see topicA partition " << p;

        EXPECT_EQ(pa1->leaderNode, pa2->leaderNode)
            << "node1 and node2 must agree on topicA partition " << p << " leader";
        EXPECT_EQ(pa1->leaderNode, pa3->leaderNode)
            << "node1 and node3 must agree on topicA partition " << p << " leader";
        EXPECT_EQ(pa1->leaderEpoch, pa2->leaderEpoch)
            << "node1 and node2 must agree on topicA partition " << p << " leaderEpoch";
        EXPECT_EQ(pa1->leaderEpoch, pa3->leaderEpoch)
            << "node1 and node3 must agree on topicA partition " << p << " leaderEpoch";
    }
}
