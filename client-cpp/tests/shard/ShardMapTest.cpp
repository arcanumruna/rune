// tests/shard/ShardMapTest.cpp — T15a: the pure, deterministic partition -> shard
// map and the per-shard egress port offset (design §2).  Aeron-free.
#include <rune/client/shard/ShardMap.h>

#include <gtest/gtest.h>

#include <set>
#include <stdexcept>

using rune::client::shard::channelWithPortOffset;
using rune::client::shard::parseChannelEndpoint;
using rune::client::shard::shardFor;

static_assert(shardFor(1, 2, 4) == 3u, "shardFor is constexpr");
static_assert(shardFor(7, 9, 1) == 0u && shardFor(7, 9, 0) == 0u);

TEST(ShardMapRegression, SingleShardOwnsEverything) {
    for (int32_t t = 0; t < 4; ++t)
        for (int32_t p = 0; p < 64; ++p) {
            EXPECT_EQ(shardFor(t, p, 1), 0u);
            EXPECT_EQ(shardFor(t, p, 0), 0u) << "n <= 1 -> 0";
        }
}

TEST(ShardMapRegression, DeterministicAndInRange) {
    for (uint32_t n = 1; n <= 17; ++n)
        for (int32_t t = -2; t < 6; ++t)
            for (int32_t p = 0; p < 128; ++p) {
                const uint32_t s = shardFor(t, p, n);
                EXPECT_LT(s, n);
                EXPECT_EQ(s, shardFor(t, p, n));
                EXPECT_EQ(s, (static_cast<uint32_t>(t) + static_cast<uint32_t>(p)) % n)
                    << "the documented formula, nothing else (no metadata, no worker count)";
            }
}

TEST(ShardMapRegression, EveryShardOwnsAPartitionOfAnNPartitionTopic) {
    // Consecutive residues: one topic with P partitions gives every shard < P at
    // least one partition — the reason AUTO caps N at max(partitionCount).
    for (int32_t topic = 0; topic < 5; ++topic)
        for (int32_t partitions = 1; partitions <= 24; ++partitions)
            for (uint32_t n = 1; n <= static_cast<uint32_t>(partitions); ++n) {
                std::set<uint32_t> owners;
                for (int32_t p = 0; p < partitions; ++p) owners.insert(shardFor(topic, p, n));
                EXPECT_EQ(owners.size(), n) << "topic " << topic << " P " << partitions << " n " << n;
            }
}

TEST(ShardMapRegression, PortOffsetRewritesOnlyEndpointPort) {
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=127.0.0.1:40400", 2),
              "aeron:udp?endpoint=127.0.0.1:40402");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=host:40400|mtu=8192", 1),
              "aeron:udp?endpoint=host:40401|mtu=8192");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?mtu=8192|endpoint=h:9", 3),
              "aeron:udp?mtu=8192|endpoint=h:12");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?control=h:5000|endpoint=h:40400|term-length=65536", 4),
              "aeron:udp?control=h:5000|endpoint=h:40404|term-length=65536")
        << "control port and every other param untouched";
    EXPECT_EQ(channelWithPortOffset("aeron:ipc", 5), "aeron:ipc");
    EXPECT_EQ(channelWithPortOffset("aeron:ipc?term-length=65536", 5), "aeron:ipc?term-length=65536");
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=h:40400", 0), "aeron:udp?endpoint=h:40400");

    auto ep = parseChannelEndpoint("aeron:udp?mtu=8192|endpoint=10.0.0.5:40400|x=y");
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.5");
    EXPECT_EQ(ep->port, 40400);
    EXPECT_FALSE(parseChannelEndpoint("aeron:ipc").has_value());
}

TEST(ShardMapRegression, PortOffsetRejectsMalformedOrOverflowing) {
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host", 1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:abc", 1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:40400x", 1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:", 1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:65535", 1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:65530", 6), std::invalid_argument);
    EXPECT_EQ(channelWithPortOffset("aeron:udp?endpoint=host:65530", 5), "aeron:udp?endpoint=host:65535");
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:40400", -1), std::invalid_argument);
    EXPECT_THROW((void)channelWithPortOffset("aeron:udp?endpoint=host:99999999999999999999", 1),
                 std::invalid_argument);
}
