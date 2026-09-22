// StaticBalancerTest.cpp — TDD exhaustive tests for StaticBalancer.
//
// ALL tests are written BEFORE implementation — they must FAIL first.
// Covers every case from the task brief:
//   1. Even leadership distribution (≤1 spread per node).
//   2. Co-location: same-group, same-index partitions share a leader node.
//   3. Leader ∈ ISR.
//   4. RF > liveNodes ⇒ exception (BalancerError).
//   5. Co-partition group partition-count mismatch ⇒ exception.
//   6. Co-partition group partition-key mismatch ⇒ exception.
//   7. Single node — all partitions on it.
//   8. Determinism: identical inputs ⇒ byte-identical records.
//   9. leaderEpoch = 0 and partitionEpoch = 0 on initial assignment.
//  10. Replicas spread: no two replicas of the same partition on the same node.
//  11. RF replicas per partition.

#include "rune/metadata/StaticBalancer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

using namespace rune::metadata;
using namespace rune::protocol::metadata;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a map: (topicId, partition) → pointer-to-record, for easy lookup in
/// tests.  BalancerRecord is move-only (copying would dangle the Int32ArrayViews
/// inside rec), so we store const pointers into the owning vector.  The vector
/// must outlive the returned map.
static std::map<std::pair<int32_t,int32_t>, const BalancerRecord*>
indexRecords(const std::vector<BalancerRecord>& recs)
{
    std::map<std::pair<int32_t,int32_t>, const BalancerRecord*> m;
    for (const auto& r : recs)
        m[{r.rec.topicId, r.rec.partition}] = &r;
    return m;
}

/// Count how many partitions each node leads across the whole result set.
static std::map<int32_t, int32_t> leaderCounts(const std::vector<BalancerRecord>& recs)
{
    std::map<int32_t, int32_t> counts;
    for (const auto& r : recs)
        counts[r.rec.leaderNode]++;
    return counts;
}

// ---------------------------------------------------------------------------
// 1. Even leadership distribution across nodes (counts differ by ≤1)
// ---------------------------------------------------------------------------

TEST(StaticBalancer, EvenLeadershipDistribution_2Nodes_4Partitions)
{
    // 4 partitions, RF=1, 2 nodes → each node leads exactly 2 partitions
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 1, "", "key"}
    };
    std::vector<NodeId> nodes = {10, 20};

    auto recs = StaticBalancer::assign(topics, nodes);

    ASSERT_EQ(recs.size(), 4u);

    std::map<int32_t,int32_t> counts;
    for (const auto& r : recs) counts[r.rec.leaderNode]++;

    int32_t minCount = INT32_MAX, maxCount = INT32_MIN;
    for (auto& [node, cnt] : counts) {
        minCount = std::min(minCount, cnt);
        maxCount = std::max(maxCount, cnt);
    }
    EXPECT_LE(maxCount - minCount, 1) << "leadership spread > 1";
}

TEST(StaticBalancer, EvenLeadershipDistribution_3Nodes_7Partitions)
{
    // 7 partitions, RF=1, 3 nodes → can't be perfectly even; spread ≤ 1
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 7, 1, "", "key"}
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 7u);

    std::map<int32_t,int32_t> counts;
    for (const auto& r : recs) counts[r.rec.leaderNode]++;

    // Every live node must appear (none starved)
    for (NodeId n : nodes)
        EXPECT_GE(counts[n], 1) << "node " << n << " got zero partitions";

    int32_t minC = INT32_MAX, maxC = INT32_MIN;
    for (auto& [n, c] : counts) { minC = std::min(minC,c); maxC = std::max(maxC,c); }
    EXPECT_LE(maxC - minC, 1);
}

TEST(StaticBalancer, EvenLeadershipDistribution_MultipleTopics)
{
    // 2 topics × 3 partitions each = 6 total; 3 nodes → 2 each
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 3, 1, "", "k"},
        TopicSpec{2, 3, 1, "", "k"},
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 6u);

    std::map<int32_t,int32_t> counts;
    for (const auto& r : recs) counts[r.rec.leaderNode]++;

    int32_t minC = INT32_MAX, maxC = INT32_MIN;
    for (auto& [n,c] : counts) { minC=std::min(minC,c); maxC=std::max(maxC,c); }
    EXPECT_LE(maxC - minC, 1);
}

// ---------------------------------------------------------------------------
// 2. Co-location: all members of a co-partition group at the SAME index share
//    the same leader node.
// ---------------------------------------------------------------------------

TEST(StaticBalancer, CoLocation_TwoTopics_SameGroup)
{
    // topicA and topicB both in group "grp1", 4 partitions each.
    // For every index i ∈ [0,3]: topicA-p[i] and topicB-p[i] must share leader.
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 1, "grp1", "key"},
        TopicSpec{2, 4, 1, "grp1", "key"},
    };
    std::vector<NodeId> nodes = {10, 20, 30, 40};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 8u);

    auto idx = indexRecords(recs);

    for (int32_t p = 0; p < 4; ++p) {
        int32_t leaderA = idx.at({1, p})->rec.leaderNode;
        int32_t leaderB = idx.at({2, p})->rec.leaderNode;
        EXPECT_EQ(leaderA, leaderB)
            << "partition " << p << ": topicA leader=" << leaderA
            << " topicB leader=" << leaderB;
    }
}

TEST(StaticBalancer, CoLocation_ThreeTopics_SameGroup)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 3, 1, "grp", "k"},
        TopicSpec{2, 3, 1, "grp", "k"},
        TopicSpec{3, 3, 1, "grp", "k"},
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 9u);

    auto idx = indexRecords(recs);
    for (int32_t p = 0; p < 3; ++p) {
        int32_t l1 = idx.at({1, p})->rec.leaderNode;
        int32_t l2 = idx.at({2, p})->rec.leaderNode;
        int32_t l3 = idx.at({3, p})->rec.leaderNode;
        EXPECT_EQ(l1, l2) << "p=" << p;
        EXPECT_EQ(l1, l3) << "p=" << p;
    }
}

TEST(StaticBalancer, CoLocation_TwoGroups_Independent)
{
    // grpA and grpB are independent; within each group co-location must hold.
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 2, 1, "grpA", "k"},
        TopicSpec{2, 2, 1, "grpA", "k"},
        TopicSpec{3, 2, 1, "grpB", "k"},
        TopicSpec{4, 2, 1, "grpB", "k"},
    };
    std::vector<NodeId> nodes = {1, 2, 3, 4};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 8u);

    auto idx = indexRecords(recs);
    // grpA co-location
    for (int32_t p = 0; p < 2; ++p)
        EXPECT_EQ(idx.at({1,p})->rec.leaderNode, idx.at({2,p})->rec.leaderNode)
            << "grpA p=" << p;
    // grpB co-location
    for (int32_t p = 0; p < 2; ++p)
        EXPECT_EQ(idx.at({3,p})->rec.leaderNode, idx.at({4,p})->rec.leaderNode)
            << "grpB p=" << p;
}

// ---------------------------------------------------------------------------
// 3. Leader ∈ ISR; ISR ⊆ replicas; |replicas| = RF; |ISR| = RF on init
// ---------------------------------------------------------------------------

TEST(StaticBalancer, LeaderInISR_RF1)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 3, 1, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    for (const auto& r : recs) {
        // leader must be in ISR
        bool found = false;
        for (int32_t n : r.isr) if (n == r.rec.leaderNode) { found = true; break; }
        EXPECT_TRUE(found) << "leader " << r.rec.leaderNode << " not in ISR";
        // ISR ⊆ replicas
        for (int32_t n : r.isr) {
            bool inRep = false;
            for (int32_t rn : r.replicas) if (rn == n) { inRep = true; break; }
            EXPECT_TRUE(inRep) << "ISR node " << n << " not in replicas";
        }
    }
}

TEST(StaticBalancer, LeaderInISR_RF3)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 4, 3, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3, 4};

    auto recs = StaticBalancer::assign(topics, nodes);
    for (const auto& r : recs) {
        EXPECT_EQ((int)r.replicas.size(), 3) << "wrong RF";
        EXPECT_EQ((int)r.isr.size(), 3) << "ISR should equal RF on init";

        bool leaderInIsr = false;
        for (int32_t n : r.isr)
            if (n == r.rec.leaderNode) { leaderInIsr = true; break; }
        EXPECT_TRUE(leaderInIsr);
    }
}

// ---------------------------------------------------------------------------
// 4. RF > liveNodes ⇒ BalancerError thrown
// ---------------------------------------------------------------------------

TEST(StaticBalancer, RejectRFGreaterThanNodes)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 3, 4, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3};  // 3 nodes, RF=4

    EXPECT_THROW(StaticBalancer::assign(topics, nodes), BalancerError);
}

TEST(StaticBalancer, RejectRFGreaterThanNodes_ExactlyOne)
{
    // RF=2, 1 node → must throw
    std::vector<TopicSpec> topics = { TopicSpec{1, 1, 2, "", "k"} };
    std::vector<NodeId> nodes = {42};

    EXPECT_THROW(StaticBalancer::assign(topics, nodes), BalancerError);
}

TEST(StaticBalancer, AcceptRFEqualsNodes)
{
    // RF == nodes.size() is valid (every node gets a replica)
    std::vector<TopicSpec> topics = { TopicSpec{1, 2, 3, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3};

    EXPECT_NO_THROW(StaticBalancer::assign(topics, nodes));
}

// ---------------------------------------------------------------------------
// 5. Co-partition group partition-count mismatch ⇒ BalancerError
// ---------------------------------------------------------------------------

TEST(StaticBalancer, RejectCoPartitionGroup_PartitionCountMismatch)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 1, "grp", "k"},
        TopicSpec{2, 8, 1, "grp", "k"},  // different partitionCount
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    EXPECT_THROW(StaticBalancer::assign(topics, nodes), BalancerError);
}

TEST(StaticBalancer, RejectCoPartitionGroup_PartitionCountMismatch_ThreeTopics)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 1, "grp", "k"},
        TopicSpec{2, 4, 1, "grp", "k"},
        TopicSpec{3, 6, 1, "grp", "k"},  // mismatch
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    EXPECT_THROW(StaticBalancer::assign(topics, nodes), BalancerError);
}

// ---------------------------------------------------------------------------
// 6. Co-partition group partition-key mismatch ⇒ BalancerError
// ---------------------------------------------------------------------------

TEST(StaticBalancer, RejectCoPartitionGroup_KeyMismatch)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 1, "grp", "userId"},
        TopicSpec{2, 4, 1, "grp", "orderId"},  // different key
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    EXPECT_THROW(StaticBalancer::assign(topics, nodes), BalancerError);
}

// ---------------------------------------------------------------------------
// 7. Single node — all partitions on it
// ---------------------------------------------------------------------------

TEST(StaticBalancer, SingleNode_AllPartitionsOnIt)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 5, 1, "", "k"},
    };
    std::vector<NodeId> nodes = {42};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 5u);

    for (const auto& r : recs) {
        EXPECT_EQ(r.rec.leaderNode, 42);
        EXPECT_EQ((int)r.replicas.size(), 1);
        EXPECT_EQ(r.replicas[0], 42);
    }
}

// ---------------------------------------------------------------------------
// 8. Determinism: identical inputs ⇒ byte-identical records
// ---------------------------------------------------------------------------

TEST(StaticBalancer, Determinism_SameInputSameOutput)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 6, 2, "grp", "k"},
        TopicSpec{2, 6, 2, "grp", "k"},
        TopicSpec{3, 4, 1, "",    "k"},
    };
    std::vector<NodeId> nodes = {10, 20, 30, 40};

    auto recs1 = StaticBalancer::assign(topics, nodes);
    auto recs2 = StaticBalancer::assign(topics, nodes);

    ASSERT_EQ(recs1.size(), recs2.size());
    for (std::size_t i = 0; i < recs1.size(); ++i) {
        EXPECT_EQ(recs1[i].rec.topicId,        recs2[i].rec.topicId)        << "i=" << i;
        EXPECT_EQ(recs1[i].rec.partition,       recs2[i].rec.partition)      << "i=" << i;
        EXPECT_EQ(recs1[i].rec.leaderNode,      recs2[i].rec.leaderNode)     << "i=" << i;
        EXPECT_EQ(recs1[i].isr,                 recs2[i].isr)                << "i=" << i;
        EXPECT_EQ(recs1[i].replicas,            recs2[i].replicas)           << "i=" << i;
        EXPECT_EQ(recs1[i].rec.leaderEpoch,     recs2[i].rec.leaderEpoch)    << "i=" << i;
        EXPECT_EQ(recs1[i].rec.partitionEpoch,  recs2[i].rec.partitionEpoch) << "i=" << i;
    }
}

TEST(StaticBalancer, Determinism_NodeOrderMatters_SameOrderSameOutput)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 4, 2, "", "k"} };
    std::vector<NodeId> nodes = {5, 3, 1, 7};

    auto r1 = StaticBalancer::assign(topics, nodes);
    auto r2 = StaticBalancer::assign(topics, nodes);

    ASSERT_EQ(r1.size(), r2.size());
    for (std::size_t i = 0; i < r1.size(); ++i)
        EXPECT_EQ(r1[i].rec.leaderNode, r2[i].rec.leaderNode) << "i=" << i;
}

// ---------------------------------------------------------------------------
// 9. leaderEpoch=0 and partitionEpoch=0 on initial assignment
// ---------------------------------------------------------------------------

TEST(StaticBalancer, InitialEpochsAreZero)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 3, 2, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    for (const auto& r : recs) {
        EXPECT_EQ(r.rec.leaderEpoch,    0) << "leaderEpoch must be 0";
        EXPECT_EQ(r.rec.partitionEpoch, 0) << "partitionEpoch must be 0";
    }
}

// ---------------------------------------------------------------------------
// 10. Replica spread: no two replicas on the same node (RF ≤ nodes.size())
// ---------------------------------------------------------------------------

TEST(StaticBalancer, ReplicaSpread_NoDuplicateNodePerPartition)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 6, 3, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3, 4, 5};

    auto recs = StaticBalancer::assign(topics, nodes);
    for (const auto& r : recs) {
        std::set<int32_t> seen(r.replicas.begin(), r.replicas.end());
        EXPECT_EQ(seen.size(), r.replicas.size())
            << "duplicate replica node for topic=" << r.rec.topicId
            << " partition=" << r.rec.partition;
    }
}

// ---------------------------------------------------------------------------
// 11. RF replicas per partition (explicit count check)
// ---------------------------------------------------------------------------

TEST(StaticBalancer, RFReplicasPerPartition)
{
    std::vector<TopicSpec> topics = { TopicSpec{1, 4, 2, "", "k"} };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    for (const auto& r : recs)
        EXPECT_EQ((int)r.replicas.size(), 2) << "wrong RF";
}

// ---------------------------------------------------------------------------
// 12. Empty topics list → empty result (no crash)
// ---------------------------------------------------------------------------

TEST(StaticBalancer, EmptyTopics_EmptyResult)
{
    std::vector<TopicSpec> topics;
    std::vector<NodeId> nodes = {1, 2, 3};

    EXPECT_NO_THROW({
        auto recs = StaticBalancer::assign(topics, nodes);
        EXPECT_TRUE(recs.empty());
    });
}

// ---------------------------------------------------------------------------
// 13. Co-location with RF>1: same-index partitions in a group share leader
//     even when replicas can differ (we only require leader match)
// ---------------------------------------------------------------------------

TEST(StaticBalancer, CoLocation_WithRF2_LeaderAligned)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 4, 2, "grp", "k"},
        TopicSpec{2, 4, 2, "grp", "k"},
    };
    std::vector<NodeId> nodes = {1, 2, 3, 4};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 8u);

    auto idx = indexRecords(recs);
    for (int32_t p = 0; p < 4; ++p) {
        EXPECT_EQ(idx.at({1,p})->rec.leaderNode, idx.at({2,p})->rec.leaderNode)
            << "p=" << p;
    }
}

// ---------------------------------------------------------------------------
// 14. Output ordering: one record per (topicId, partition) covering all partitions
// ---------------------------------------------------------------------------

TEST(StaticBalancer, OutputCoversAllPartitions)
{
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 3, 1, "", "k"},
        TopicSpec{2, 2, 1, "", "k"},
    };
    std::vector<NodeId> nodes = {1, 2, 3};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 5u);  // 3 + 2

    auto idx = indexRecords(recs);
    for (int32_t p = 0; p < 3; ++p)
        EXPECT_TRUE(idx.count({1, p})) << "topic 1 partition " << p << " missing";
    for (int32_t p = 0; p < 2; ++p)
        EXPECT_TRUE(idx.count({2, p})) << "topic 2 partition " << p << " missing";
}

// ---------------------------------------------------------------------------
// 15. Discriminating balance tests for co-located uneven-division case.
//
// Scenario: 2 topics, 3 partitions, 2 nodes (partitionCount % nodeCount != 0).
// nodes (sorted): [0, 1], stripeOffset=0 for the group.
//   p=0: leaderIdx=(0+0)%2=0 → node 0  (both topics)
//   p=1: leaderIdx=(0+1)%2=1 → node 1  (both topics)
//   p=2: leaderIdx=(0+2)%2=0 → node 0  (both topics)
// Per-node leader counts: node0=4, node1=2  (spread=2 = group member count M=2).
// This IS the correct, documented behaviour: co-location is preserved exactly
// (all same-index partitions share one leader), and the residual balance skew
// is bounded by M (the group's member count) — an inherent consequence of
// the co-location constraint when partitionCount % nodeCount != 0.
// ---------------------------------------------------------------------------

TEST(StaticBalancer, CoLocation_UnevenDivision_CoLocationHolds)
{
    // 2 topics, 3 partitions, 2 nodes: partitionCount(3) % nodeCount(2) != 0.
    // Co-location MUST still hold: every same-index pair shares a leader.
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 3, 1, "grp", "k"},
        TopicSpec{2, 3, 1, "grp", "k"},
    };
    std::vector<NodeId> nodes = {0, 1};

    auto recs = StaticBalancer::assign(topics, nodes);
    ASSERT_EQ(recs.size(), 6u);

    auto idx = indexRecords(recs);
    for (int32_t p = 0; p < 3; ++p) {
        EXPECT_EQ(idx.at({1, p})->rec.leaderNode, idx.at({2, p})->rec.leaderNode)
            << "co-location violated at partition " << p;
    }
}

TEST(StaticBalancer, CoLocation_UnevenDivision_PerNodeCountsAreDocumentedResidual)
{
    // 2 topics, 3 partitions, 2 nodes: partitionCount(3) % nodeCount(2) != 0.
    // Concrete expected per-node leader counts derived from the algorithm:
    //   ring=[0,1], stripeOffset=0
    //   p=0→node0 (×2 topics), p=1→node1 (×2 topics), p=2→node0 (×2 topics)
    //   node0=4, node1=2  →  spread = M = 2 (group member count).
    // This is the inherent, accepted co-location residual documented in the spec.
    std::vector<TopicSpec> topics = {
        TopicSpec{1, 3, 1, "grp", "k"},
        TopicSpec{2, 3, 1, "grp", "k"},
    };
    std::vector<NodeId> nodes = {0, 1};

    auto recs = StaticBalancer::assign(topics, nodes);

    auto counts = leaderCounts(recs);
    EXPECT_EQ(counts[0], 4) << "node0 should lead 4 partitions (co-location residual)";
    EXPECT_EQ(counts[1], 2) << "node1 should lead 2 partitions (co-location residual)";
    // Spread = M = 2 (group member count): this is the documented bound.
    EXPECT_EQ(counts[0] - counts[1], 2)
        << "spread should equal group member count (2) for the uneven-division case";
}

TEST(StaticBalancer, Balance_EvenDivision_SingletonAndGroup_SpreadAtMostOne)
{
    // Confirms that per-node balance IS ≤1 for:
    //   (a) singleton topics (any partitionCount, any nodeCount)
    //   (b) co-located group where partitionCount % nodeCount == 0

    // (a) Singleton: 6 partitions, 3 nodes → each node leads exactly 2.
    {
        std::vector<TopicSpec> topics = { TopicSpec{1, 6, 1, "", "k"} };
        std::vector<NodeId> nodes = {1, 2, 3};
        auto recs = StaticBalancer::assign(topics, nodes);
        auto counts = leaderCounts(recs);
        int32_t minC = INT32_MAX, maxC = INT32_MIN;
        for (auto& [n, c] : counts) { minC = std::min(minC, c); maxC = std::max(maxC, c); }
        EXPECT_LE(maxC - minC, 1) << "singleton: spread > 1";
    }

    // (b) Co-located group, partitionCount(6) % nodeCount(3) == 0:
    //     2 topics × 6 partitions × 3 nodes → each node leads exactly 4.
    {
        std::vector<TopicSpec> topics = {
            TopicSpec{10, 6, 1, "g", "k"},
            TopicSpec{11, 6, 1, "g", "k"},
        };
        std::vector<NodeId> nodes = {1, 2, 3};
        auto recs = StaticBalancer::assign(topics, nodes);
        auto counts = leaderCounts(recs);
        int32_t minC = INT32_MAX, maxC = INT32_MIN;
        for (auto& [n, c] : counts) { minC = std::min(minC, c); maxC = std::max(maxC, c); }
        EXPECT_LE(maxC - minC, 1) << "evenly-dividing co-located group: spread > 1";
    }
}
