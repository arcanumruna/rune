// tests/metadata/MetadataIntegrationTest.cpp
//
// MD1 Task 10 — MetadataResponse → cache → routing integration tests.
//
// These tests use the Codec layer to build wire frames and verify the decode
// path (MetadataResponseDecoded) feeds correctly into MetadataCache, and that
// the resulting routing state is correct.  No Aeron needed (in-process only).
//
// Test matrix:
//   MI-1: WireRoundTrip_MetadataResponse_PopulatesCache
//   MI-2: DecodeMetadataResponse_EmptyBrokers_EmptyPartitions
//   MI-3: MetadataResponse_Then_Route_CorrectNode
//   MI-4: ZeroRTT_AfterRedirect_CacheUpdated_RouteChanged
//   MI-5: MetadataResponse_StalenessGate_LowerEpochDropped

#include <gtest/gtest.h>

#include <rune/client/codec/Codec.h>
#include <rune/client/metadata/MetadataCache.h>
#include <rune/client/metadata/PartitionRouter.h>
#include <rune/client/Key.h>

#include <rune/protocol/client/MetadataMessages.h>

#include <cstdint>
#include <string>
#include <vector>

namespace rcc = rune::client::codec;
namespace mc  = rune::client::metadata;
namespace rpc = rune::protocol::client;
using rune::client::Key;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static mc::BrokerEntry makeBrokerEntry(int32_t nodeId, std::string host, int32_t port) {
    mc::BrokerEntry b;
    b.nodeId = nodeId;
    b.host   = std::move(host);
    b.port   = port;
    return b;
}

static mc::PartitionEntry makePartitionEntry(int32_t topicId, int32_t partition,
                                              int32_t leaderId, int32_t leaderEpoch) {
    mc::PartitionEntry p;
    p.topicId     = topicId;
    p.partition   = partition;
    p.leaderId    = leaderId;
    p.leaderEpoch = leaderEpoch;
    return p;
}

// Build a MetadataResponse wire frame and decode it into MetadataResponseDecoded.
// The response is built using the rune-protocol MetadataResponse codec directly.
// The decoded views point INTO `buf` (zero-copy): the caller owns it and keeps
// it alive while the result is used (a helper-local buffer was a heap
// use-after-free — ASan).
static std::optional<rcc::MetadataResponseDecoded>
buildAndDecodeMetadataResponse(
    std::vector<uint8_t>&                  buf,
    const std::vector<mc::BrokerEntry>&    brokers,
    const std::vector<mc::PartitionEntry>& partitions,
    int64_t sessionId     = 1,
    int64_t correlationId = 100)
{
    // Convert to rpc::BrokerEntry / PartitionEntry (namespace-level, not nested).
    std::vector<rpc::BrokerEntry>    rpBrokers;
    std::vector<rpc::PartitionEntry> rpParts;

    for (const auto& b : brokers) {
        rpc::BrokerEntry rb;
        rb.nodeId = b.nodeId;
        rb.port   = b.port;
        rb.host   = b.host;
        rpBrokers.push_back(std::move(rb));
    }
    for (const auto& p : partitions) {
        rpc::PartitionEntry rp;
        rp.topicId     = p.topicId;
        rp.partition   = p.partition;
        rp.leaderId    = p.leaderId;
        rp.leaderEpoch = p.leaderEpoch;
        rpParts.push_back(rp);
    }

    rpc::MetadataResponse msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.brokers       = std::span<const rpc::BrokerEntry>(rpBrokers);
    msg.partitions    = std::span<const rpc::PartitionEntry>(rpParts);

    buf.assign(4096, 0);
    const std::size_t n = msg.encode(buf.data());
    if (n == 0) return std::nullopt;

    return rcc::decodeMetadataResponse(buf.data(), n);
}

// Populate MetadataCache from a decoded response.
static void populateCache(mc::MetadataCache&                      cache,
                           const rcc::MetadataResponseDecoded&    resp)
{
    // Convert to MetadataCache types.
    std::vector<mc::BrokerEntry>    cb;
    std::vector<mc::PartitionEntry> cp;

    for (const auto& b : resp.brokers) {
        mc::BrokerEntry e;
        e.nodeId = b.nodeId;
        e.port   = b.port;
        e.host   = std::string(b.host);
        cb.push_back(std::move(e));
    }
    for (const auto& p : resp.partitions) {
        mc::PartitionEntry e;
        e.topicId     = p.topicId;
        e.partition   = p.partition;
        e.leaderId    = p.leaderId;
        e.leaderEpoch = p.leaderEpoch;
        cp.push_back(e);
    }

    cache.updateFromResponse(cb, cp);
}

// ===========================================================================
// MI-1: Wire round-trip MetadataResponse populates cache correctly
// ===========================================================================
TEST(MetadataIntegration, WireRoundTrip_MetadataResponse_PopulatesCache) {
    std::vector<mc::BrokerEntry>    brokers    = { makeBrokerEntry(0, "n0", 9000),
                                                    makeBrokerEntry(1, "n1", 9001) };
    std::vector<mc::PartitionEntry> partitions = { makePartitionEntry(1, 0, 0, 3),
                                                    makePartitionEntry(1, 1, 1, 3) };

    std::vector<uint8_t> wire;  // the decoded views point into it
    auto decoded = buildAndDecodeMetadataResponse(wire, brokers, partitions);
    ASSERT_TRUE(decoded.has_value()) << "decode failed";

    mc::MetadataCache cache;
    populateCache(cache, *decoded);

    EXPECT_EQ(cache.leaderFor(1, 0), 0);
    EXPECT_EQ(cache.leaderFor(1, 1), 1);
    EXPECT_EQ(cache.epochFor(1, 0), 3);
    EXPECT_EQ(cache.endpointFor(0), "n0:9000");
    EXPECT_EQ(cache.endpointFor(1), "n1:9001");
}

// ===========================================================================
// MI-2: Empty brokers and partitions in MetadataResponse
// ===========================================================================
TEST(MetadataIntegration, DecodeMetadataResponse_EmptyBrokers_EmptyPartitions) {
    std::vector<uint8_t> wire;
    auto decoded = buildAndDecodeMetadataResponse(wire, {}, {});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->brokers.empty());
    EXPECT_TRUE(decoded->partitions.empty());

    mc::MetadataCache cache;
    populateCache(cache, *decoded);
    EXPECT_EQ(cache.partitionCount(), 0u);
    EXPECT_EQ(cache.brokerCount(), 0u);
}

// ===========================================================================
// MI-3: MetadataResponse → cache → route produces correct node
// ===========================================================================
TEST(MetadataIntegration, MetadataResponse_Then_Route_CorrectNode) {
    std::vector<mc::BrokerEntry>    brokers    = { makeBrokerEntry(2, "server2", 7002) };
    std::vector<mc::PartitionEntry> partitions = { makePartitionEntry(5, 0, 2, 1) };

    std::vector<uint8_t> wire;  // the decoded views point into it
    auto decoded = buildAndDecodeMetadataResponse(wire, brokers, partitions);
    ASSERT_TRUE(decoded.has_value());

    mc::MetadataCache   cache;
    populateCache(cache, *decoded);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(5, 1);

    const auto result = router.routePartition(5, 0);
    EXPECT_TRUE(result.valid());
    EXPECT_EQ(result.leaderNodeId, 2);
    EXPECT_EQ(result.channel, "server2:7002");
}

// ===========================================================================
// MI-4: Zero-RTT — after REDIRECT, cache updated, route changes immediately
// ===========================================================================
TEST(MetadataIntegration, ZeroRTT_AfterRedirect_CacheUpdated_RouteChanged) {
    std::vector<mc::BrokerEntry>    brokers    = { makeBrokerEntry(0, "a0", 9000),
                                                    makeBrokerEntry(1, "a1", 9001) };
    std::vector<mc::PartitionEntry> partitions = { makePartitionEntry(8, 0, 0, 1) };

    std::vector<uint8_t> wire;  // the decoded views point into it
    auto decoded = buildAndDecodeMetadataResponse(wire, brokers, partitions);
    ASSERT_TRUE(decoded.has_value());

    mc::MetadataCache   cache;
    populateCache(cache, *decoded);

    mc::PartitionRouter router(cache);
    router.setTopicPartitionCount(8, 1);

    // Before: routed to node 0.
    auto before = router.routePartition(8, 0);
    EXPECT_EQ(before.leaderNodeId, 0);

    // Simulate a REDIRECT response: new leader = node 1, epoch = 5.
    // This is zero-RTT: update cache directly, no new MetadataRequest.
    cache.updatePartitionLeader(8, 0, 1, 5);

    // After: immediately routed to node 1.
    auto after = router.routePartition(8, 0);
    EXPECT_TRUE(after.valid());
    EXPECT_EQ(after.leaderNodeId, 1);
    EXPECT_EQ(after.channel, "a1:9001");
}

// ===========================================================================
// MI-5: MetadataResponse with stale epoch is dropped (full response path)
// ===========================================================================
TEST(MetadataIntegration, MetadataResponse_StalenessGate_LowerEpochDropped) {
    mc::MetadataCache cache;

    // Install a high-epoch entry directly.
    cache.updatePartitionLeader(3, 0, 5, 10);
    ASSERT_EQ(cache.leaderFor(3, 0), 5);
    ASSERT_EQ(cache.epochFor(3, 0), 10);

    // A MetadataResponse comes in with a lower epoch (e.g. 7) saying leader=9.
    std::vector<mc::BrokerEntry>    brokers    = { makeBrokerEntry(9, "stale", 9009) };
    std::vector<mc::PartitionEntry> partitions = { makePartitionEntry(3, 0, 9, 7) };

    std::vector<uint8_t> wire;  // the decoded views point into it
    auto decoded = buildAndDecodeMetadataResponse(wire, brokers, partitions);
    ASSERT_TRUE(decoded.has_value());

    populateCache(cache, *decoded);

    // Partition (3,0) should still point to old leader (5), epoch still 10.
    EXPECT_EQ(cache.leaderFor(3, 0), 5) << "stale response must not overwrite higher-epoch entry";
    EXPECT_EQ(cache.epochFor(3, 0), 10);
}
