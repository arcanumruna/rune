#include <rune/metadata/ShardRouter.h>
#include <gtest/gtest.h>

using rune::metadata::ShardRouter;
using rune::metadata::ShardId;
using rune::metadata::shardIdOf;
using rune::metadata::workerIdForShard;

TEST(ShardRouter, SingletonTopicShardsByTopicId) {
    ShardRouter r(4);
    // topic 7 has no group registered -> groupKey falls back to topicId.
    EXPECT_EQ(r.groupKeyForTopic(7), 7);
    const ShardId s = r.shardForPartition(/*topicId=*/7, /*partition=*/2);
    EXPECT_EQ(s.groupKey, 7);
    EXPECT_EQ(s.partitionIndex, 2);
}

TEST(ShardRouter, CoGroupSameIndexSameWorker) {
    ShardRouter r(4);
    // topics 0 and 1 co-partitioned -> share group key -100.
    r.mapTopicToGroupKey(0, -100);
    r.mapTopicToGroupKey(1, -100);
    // Same partition index across group members -> same shard -> same worker.
    for (int32_t idx = 0; idx < 8; ++idx) {
        EXPECT_EQ(r.shardForPartition(0, idx), r.shardForPartition(1, idx));
        EXPECT_EQ(r.workerForPartition(0, idx), r.workerForPartition(1, idx));
    }
}

TEST(ShardRouter, DeterministicAndTotal) {
    ShardRouter r(3);
    for (int32_t t = 0; t < 5; ++t) {
        for (int32_t p = 0; p < 6; ++p) {
            const int32_t w1 = r.workerForPartition(t, p);
            const int32_t w2 = r.workerForPartition(t, p);
            EXPECT_EQ(w1, w2);                 // deterministic
            EXPECT_GE(w1, 0);                  // total: in range
            EXPECT_LT(w1, r.workerCount());
        }
    }
}

TEST(ShardRouter, WorkerIdInRangeForAllShards) {
    for (int32_t wc = 1; wc <= 8; ++wc) {
        for (int32_t g = -5; g < 20; ++g) {
            for (int32_t p = 0; p < 10; ++p) {
                const int32_t w = workerIdForShard(shardIdOf(g, p), wc);
                EXPECT_GE(w, 0);
                EXPECT_LT(w, wc);
            }
        }
    }
}

TEST(ShardRouter, DifferentIndexMayDifferButIsStable) {
    ShardRouter r(4);
    r.mapTopicToGroupKey(0, -100);
    // Shard identity differs by index; the mapping is stable across calls.
    const ShardId a = r.shardForPartition(0, 0);
    const ShardId b = r.shardForPartition(0, 1);
    EXPECT_FALSE(a == b);
}

// Catalog history Task 17 fix round 1 (C1): the group key is a pure function of the group NAME, so two brokers with
// different boot histories (different sets / orders of groups known) agree on every worker.
// RED mutation: make the key depend on creation order (e.g. -1 - number of distinct names seen so far): X and Y disagree.
TEST(ShardRouterRegression, CoPartitionGroupKeyIsAPureFunctionOfTheNameRegression) {
    using rune::metadata::coPartitionGroupKey;
    EXPECT_EQ(coPartitionGroupKey("grp"), coPartitionGroupKey("grp"));
    for (const char* g : {"grp", "a", "b", ""}) {
        EXPECT_LT(coPartitionGroupKey(g), 0) << g;
        EXPECT_GE(coPartitionGroupKey(g), -(1 << 30)) << g;
    }
    EXPECT_NE(coPartitionGroupKey("a"), coPartitionGroupKey("b"));

    for (int32_t wc : {2, 4, 7}) {
        ShardRouter xx(wc), y(wc);   // X: "b" then "a"; Y: the opposite order
        xx.mapTopicToGroupKey(0, coPartitionGroupKey("b"));
        xx.mapTopicToGroupKey(1, coPartitionGroupKey("a"));
        y.mapTopicToGroupKey(1, coPartitionGroupKey("a"));      // "a" known first ...
        y.mapTopicToGroupKey(0, coPartitionGroupKey("b"));      // ... then "b" (the opposite order)
        for (int32_t t : {0, 1})
            for (int32_t p = 0; p < 16; ++p)
                EXPECT_EQ(xx.workerForPartition(t, p), y.workerForPartition(t, p)) << "wc " << wc << " t " << t << " p " << p;
    }
}
