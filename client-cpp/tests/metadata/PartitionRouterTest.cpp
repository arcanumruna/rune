// tests/metadata/PartitionRouterTest.cpp
//
// MD1 Task 10 — PartitionRouter unit tests.
//
// Test matrix:
//   PR-1: RouteKey_KnownTopicAndLeader_ReturnsValidResult
//   PR-2: RouteKey_UnknownTopic_ReturnsInvalidResult
//   PR-3: RouteKey_NoLeaderForPartition_ReturnsInvalidResult
//   PR-4: RouteKey_LeaderKnownButNoEndpoint_ReturnsInvalidResult
//   PR-5: RoutePartition_ExplicitPartition_Routed
//   PR-6: HashRouting_Deterministic_SameKeyAlwaysSamePartition
//   PR-7: ZeroRTT_AfterCacheUpdate_NewRoutingApplied
//   PR-8: PartitionCountFor_UnknownTopic_ReturnsZero

#include <gtest/gtest.h>

#include <rune/client/Key.h>
#include <rune/client/metadata/MetadataCache.h>
#include <rune/client/metadata/PartitionRouter.h>

namespace mc = rune::client::metadata;
using rune::client::Key;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static mc::BrokerEntry makeBroker(int32_t nodeId, std::string host, int32_t port) {
    mc::BrokerEntry b;
    b.nodeId = nodeId;
    b.host   = std::move(host);
    b.port   = port;
    return b;
}

static mc::PartitionEntry makePartition(int32_t topicId, int32_t partition,
                                         int32_t leaderId, int32_t leaderEpoch) {
    mc::PartitionEntry p;
    p.topicId     = topicId;
    p.partition   = partition;
    p.leaderId    = leaderId;
    p.leaderEpoch = leaderEpoch;
    return p;
}

// ===========================================================================
// PR-1: routeKey — known topic + leader — returns a valid RouteResult
// ===========================================================================
TEST(PartitionRouter, RouteKey_KnownTopicAndLeader_ReturnsValidResult) {
    mc::MetadataCache cache;

    std::vector<mc::BrokerEntry>    brokers    = { makeBroker(0, "node0", 9000) };
    std::vector<mc::PartitionEntry> partitions = { makePartition(1, 0, 0, 1),
                                                    makePartition(1, 1, 0, 1) };
    cache.updateFromResponse(brokers, partitions);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(1, 2);

    const auto result = router.routeKey(1, Key(uint64_t{0}));
    EXPECT_TRUE(result.valid());
    EXPECT_EQ(result.leaderNodeId, 0);
    EXPECT_EQ(result.channel, "node0:9000");
    EXPECT_GE(result.partition, 0);
    EXPECT_LT(result.partition, 2);
}

// ===========================================================================
// PR-2: routeKey — unknown topic — returns invalid
// ===========================================================================
TEST(PartitionRouter, RouteKey_UnknownTopic_ReturnsInvalidResult) {
    mc::MetadataCache   cache;
    mc::PartitionRouter router(cache);
    // No topic 999 registered.
    const auto result = router.routeKey(999, Key(uint64_t{0}));
    EXPECT_FALSE(result.valid());
    EXPECT_EQ(result.leaderNodeId, mc::MetadataCache::kNoLeader);
}

// ===========================================================================
// PR-3: routeKey — partition has no known leader — invalid
// ===========================================================================
TEST(PartitionRouter, RouteKey_NoLeaderForPartition_ReturnsInvalidResult) {
    mc::MetadataCache   cache;
    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(5, 1);
    // partition (5, 0) has no entry in cache.
    const auto result = router.routeKey(5, Key(uint64_t{0}));
    EXPECT_FALSE(result.valid());
}

// ===========================================================================
// PR-4: routeKey — leader known but endpoint unknown — invalid
// ===========================================================================
TEST(PartitionRouter, RouteKey_LeaderKnownButNoEndpoint_ReturnsInvalidResult) {
    mc::MetadataCache   cache;
    // Set partition leader but no broker endpoint.
    cache.updatePartitionLeader(3, 0, 7, 1);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(3, 1);

    const auto result = router.routeKey(3, Key(uint64_t{0}));
    EXPECT_FALSE(result.valid()) << "no endpoint for node 7, should be invalid";
    EXPECT_EQ(result.leaderNodeId, 7);
    EXPECT_TRUE(result.channel.empty());
}

// ===========================================================================
// PR-5: routePartition — explicit partition bypass — routed
// ===========================================================================
TEST(PartitionRouter, RoutePartition_ExplicitPartition_Routed) {
    mc::MetadataCache cache;

    std::vector<mc::BrokerEntry>    brokers    = { makeBroker(2, "n2", 9002) };
    std::vector<mc::PartitionEntry> partitions = { makePartition(10, 3, 2, 1) };
    cache.updateFromResponse(brokers, partitions);

    mc::PartitionRouter router(cache);
    // No need to set partition count for explicit-partition routing.

    const auto result = router.routePartition(10, 3);
    EXPECT_TRUE(result.valid());
    EXPECT_EQ(result.partition, 3);
    EXPECT_EQ(result.leaderNodeId, 2);
    EXPECT_EQ(result.channel, "n2:9002");
}

// ===========================================================================
// PR-6: Hash routing is deterministic — same key always maps to same partition
// ===========================================================================
TEST(PartitionRouter, HashRouting_Deterministic_SameKeyAlwaysSamePartition) {
    mc::MetadataCache cache;

    // 4 partitions, all on node 0 for simplicity.
    std::vector<mc::BrokerEntry> brokers = { makeBroker(0, "n0", 9000) };
    std::vector<mc::PartitionEntry> partitions;
    for (int i = 0; i < 4; ++i) partitions.push_back(makePartition(1, i, 0, 1));
    cache.updateFromResponse(brokers, partitions);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(1, 4);

    Key k(uint64_t{12345});
    const int32_t p0 = router.routeKey(1, k).partition;
    const int32_t p1 = router.routeKey(1, k).partition;
    const int32_t p2 = router.routeKey(1, k).partition;

    EXPECT_EQ(p0, p1) << "determinism violation";
    EXPECT_EQ(p1, p2) << "determinism violation";
    EXPECT_GE(p0, 0);
    EXPECT_LT(p0, 4);
}

// ===========================================================================
// PR-7: Zero-RTT — after MetadataCache update, new routing is immediately applied
// ===========================================================================
TEST(PartitionRouter, ZeroRTT_AfterCacheUpdate_NewRoutingApplied) {
    mc::MetadataCache cache;

    std::vector<mc::BrokerEntry> brokers = { makeBroker(0, "n0", 9000),
                                              makeBroker(1, "n1", 9001) };
    std::vector<mc::PartitionEntry> partitions = { makePartition(7, 0, 0, 1) };
    cache.updateFromResponse(brokers, partitions);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(7, 1);

    // Before zero-RTT: routes to node 0.
    auto before = router.routePartition(7, 0);
    EXPECT_EQ(before.leaderNodeId, 0);
    EXPECT_EQ(before.channel, "n0:9000");

    // Zero-RTT reroute: partition (7,0) leader is now node 1, epoch 2.
    cache.updatePartitionLeader(7, 0, 1, 2);

    // After zero-RTT: routes to node 1 — same router instance, no full refresh.
    auto after = router.routePartition(7, 0);
    EXPECT_EQ(after.leaderNodeId, 1) << "zero-RTT route must now point to new leader";
    EXPECT_EQ(after.channel, "n1:9001");
}

// ===========================================================================
// PR-8: partitionCountFor returns 0 for unknown topics
// ===========================================================================
TEST(PartitionRouter, PartitionCountFor_UnknownTopic_ReturnsZero) {
    mc::MetadataCache   cache;
    mc::PartitionRouter router(cache);
    EXPECT_EQ(router.partitionCountFor(9999), 0);
}
