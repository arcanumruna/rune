// tests/metadata/MetadataCacheTest.cpp
//
// MD1 Task 10 — MetadataCache unit tests.
//
// Test matrix:
//   MC-1: UpdateFromResponse_PopulatesPartitionsAndBrokers
//   MC-2: StalenessRule_RejectsLowerEpoch
//   MC-3: StalenessRule_AcceptsEqualEpoch
//   MC-4: StalenessRule_AcceptsHigherEpoch
//   MC-5: ZeroRTT_UpdatePartitionLeader_UpdatesWithoutFullRefresh
//   MC-6: ZeroRTT_UpdatePartitionLeader_StalenessRuleApplied
//   MC-7: UnknownPartition_ReturnsNoLeader
//   MC-8: UnknownBroker_ReturnsEmptyEndpoint
//   MC-9: UpdateBrokerEndpoint_UpdatesWithoutTouchingPartitions
//  MC-10: Clear_RemovesAllState
//  MC-11: MultipleTopics_PartitionsAreIndependent
//  MC-12: NoLeader_NegativeLeaderIdStoredAndReturned

#include <gtest/gtest.h>

#include <rune/client/metadata/MetadataCache.h>

#include <string>
#include <vector>

namespace mc = rune::client::metadata;

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
// MC-1: UpdateFromResponse populates partitions and brokers
// ===========================================================================
TEST(MetadataCache, UpdateFromResponse_PopulatesPartitionsAndBrokers) {
    mc::MetadataCache cache;

    std::vector<mc::BrokerEntry>    brokers    = { makeBroker(0, "node0", 9000),
                                                    makeBroker(1, "node1", 9001) };
    std::vector<mc::PartitionEntry> partitions = { makePartition(42, 0, 0, 1),
                                                    makePartition(42, 1, 1, 1) };

    cache.updateFromResponse(brokers, partitions);

    EXPECT_EQ(cache.leaderFor(42, 0), 0);
    EXPECT_EQ(cache.leaderFor(42, 1), 1);
    EXPECT_EQ(cache.epochFor(42, 0), 1);
    EXPECT_EQ(cache.epochFor(42, 1), 1);

    EXPECT_EQ(cache.endpointFor(0), "node0:9000");
    EXPECT_EQ(cache.endpointFor(1), "node1:9001");

    EXPECT_EQ(cache.partitionCount(), 2u);
    EXPECT_EQ(cache.brokerCount(), 2u);
}

// ===========================================================================
// MC-2: Staleness rule — reject lower epoch
// ===========================================================================
TEST(MetadataCache, StalenessRule_RejectsLowerEpoch) {
    mc::MetadataCache cache;

    // Install epoch=5 for (topicId=1, partition=0), leader=2.
    cache.updatePartitionLeader(1, 0, 2, 5);
    EXPECT_EQ(cache.leaderFor(1, 0), 2);
    EXPECT_EQ(cache.epochFor(1, 0), 5);

    // Attempt lower-epoch update: epoch=3, leader=7 — must be ignored.
    cache.updatePartitionLeader(1, 0, 7, 3);
    EXPECT_EQ(cache.leaderFor(1, 0), 2) << "lower-epoch update must be rejected";
    EXPECT_EQ(cache.epochFor(1, 0), 5)  << "epoch must not decrease";
}

// ===========================================================================
// MC-3: Staleness rule — accept equal epoch (leader may change at same epoch)
// ===========================================================================
TEST(MetadataCache, StalenessRule_AcceptsEqualEpoch) {
    mc::MetadataCache cache;

    cache.updatePartitionLeader(1, 0, 2, 5);

    // Equal epoch update: accept (the epoch-equal case is treated as valid).
    cache.updatePartitionLeader(1, 0, 3, 5);
    EXPECT_EQ(cache.leaderFor(1, 0), 3) << "equal-epoch update must be accepted";
    EXPECT_EQ(cache.epochFor(1, 0), 5);
}

// ===========================================================================
// MC-4: Staleness rule — accept higher epoch
// ===========================================================================
TEST(MetadataCache, StalenessRule_AcceptsHigherEpoch) {
    mc::MetadataCache cache;

    cache.updatePartitionLeader(1, 0, 2, 5);
    cache.updatePartitionLeader(1, 0, 4, 8);

    EXPECT_EQ(cache.leaderFor(1, 0), 4) << "higher-epoch update must be accepted";
    EXPECT_EQ(cache.epochFor(1, 0), 8);
}

// ===========================================================================
// MC-5: Zero-RTT — updatePartitionLeader updates without a full refresh
// ===========================================================================
TEST(MetadataCache, ZeroRTT_UpdatePartitionLeader_UpdatesWithoutFullRefresh) {
    mc::MetadataCache cache;

    // Populate via a full response first.
    std::vector<mc::BrokerEntry>    brokers    = { makeBroker(0, "n0", 9000),
                                                    makeBroker(1, "n1", 9001) };
    std::vector<mc::PartitionEntry> partitions = { makePartition(10, 0, 0, 1) };
    cache.updateFromResponse(brokers, partitions);

    ASSERT_EQ(cache.leaderFor(10, 0), 0);

    // Zero-RTT reroute: NotLeader/Redirect says leader is now node 1, epoch 2.
    cache.updatePartitionLeader(10, 0, 1, 2);

    EXPECT_EQ(cache.leaderFor(10, 0), 1) << "zero-RTT leader update must apply";
    EXPECT_EQ(cache.epochFor(10, 0), 2);

    // Other partitions must be unaffected — no collateral damage.
    EXPECT_EQ(cache.partitionCount(), 1u);
}

// ===========================================================================
// MC-6: Zero-RTT — staleness rule still applies in zero-RTT path
// ===========================================================================
TEST(MetadataCache, ZeroRTT_UpdatePartitionLeader_StalenessRuleApplied) {
    mc::MetadataCache cache;
    cache.updatePartitionLeader(2, 3, 5, 10);
    ASSERT_EQ(cache.leaderFor(2, 3), 5);

    // Stale update must be rejected.
    cache.updatePartitionLeader(2, 3, 9, 7);
    EXPECT_EQ(cache.leaderFor(2, 3), 5) << "zero-RTT stale update must be rejected";
    EXPECT_EQ(cache.epochFor(2, 3), 10);
}

// ===========================================================================
// MC-7: Unknown partition returns kNoLeader
// ===========================================================================
TEST(MetadataCache, UnknownPartition_ReturnsNoLeader) {
    mc::MetadataCache cache;
    EXPECT_EQ(cache.leaderFor(999, 0), mc::MetadataCache::kNoLeader);
    EXPECT_EQ(cache.epochFor(999, 0), -1);
    EXPECT_FALSE(cache.hasLeader(999, 0));
}

// ===========================================================================
// MC-8: Unknown broker returns empty endpoint
// ===========================================================================
TEST(MetadataCache, UnknownBroker_ReturnsEmptyEndpoint) {
    mc::MetadataCache cache;
    EXPECT_TRUE(cache.endpointFor(42).empty());
    EXPECT_FALSE(cache.hasEndpoint(42));
}

// ===========================================================================
// MC-9: updateBrokerEndpoint updates without touching partitions
// ===========================================================================
TEST(MetadataCache, UpdateBrokerEndpoint_UpdatesWithoutTouchingPartitions) {
    mc::MetadataCache cache;
    cache.updatePartitionLeader(1, 0, 7, 3);
    cache.updateBrokerEndpoint(7, "broker7:9007");

    EXPECT_EQ(cache.leaderFor(1, 0), 7);
    EXPECT_EQ(cache.endpointFor(7), "broker7:9007");
}

// ===========================================================================
// MC-10: clear() removes all state
// ===========================================================================
TEST(MetadataCache, Clear_RemovesAllState) {
    mc::MetadataCache cache;
    cache.updatePartitionLeader(1, 0, 2, 5);
    cache.updateBrokerEndpoint(2, "node2:9002");
    ASSERT_TRUE(cache.hasPartitionData());
    ASSERT_TRUE(cache.hasEndpoint(2));

    cache.clear();

    EXPECT_FALSE(cache.hasPartitionData());
    EXPECT_FALSE(cache.hasEndpoint(2));
    EXPECT_EQ(cache.partitionCount(), 0u);
    EXPECT_EQ(cache.brokerCount(), 0u);
}

// ===========================================================================
// MC-11: Multiple topics — partitions are independent
// ===========================================================================
TEST(MetadataCache, MultipleTopics_PartitionsAreIndependent) {
    mc::MetadataCache cache;

    cache.updatePartitionLeader(1, 0, 10, 1);
    cache.updatePartitionLeader(2, 0, 20, 1);
    cache.updatePartitionLeader(1, 1, 11, 1);

    EXPECT_EQ(cache.leaderFor(1, 0), 10);
    EXPECT_EQ(cache.leaderFor(2, 0), 20);
    EXPECT_EQ(cache.leaderFor(1, 1), 11);
    EXPECT_EQ(cache.partitionCount(), 3u);

    // Update topic 1, partition 0: should not affect topic 2 or (1,1).
    cache.updatePartitionLeader(1, 0, 12, 5);
    EXPECT_EQ(cache.leaderFor(1, 0), 12);
    EXPECT_EQ(cache.leaderFor(2, 0), 20);
    EXPECT_EQ(cache.leaderFor(1, 1), 11);
}

// ===========================================================================
// MC-12: kNoLeader value is stored and returned correctly
// ===========================================================================
TEST(MetadataCache, NoLeader_NegativeLeaderIdStoredAndReturned) {
    mc::MetadataCache cache;

    // A valid partition entry with no leader (-1).
    cache.updatePartitionLeader(5, 2, -1, 0);
    EXPECT_EQ(cache.leaderFor(5, 2), mc::MetadataCache::kNoLeader);
    EXPECT_EQ(cache.epochFor(5, 2), 0);
}

// ===========================================================================
// Re-review 1 NEW-1: the redirect ordering rule.  A Redirect's provisional
// leader (epoch confirmed + 1) is undone only by an answer to a request sent
// AFTER the redirect (M5: a spurious redirect restores the old leader) or by a
// higher epoch; an equal-epoch answer requested before it (or unsolicited)
// is discarded.
// ===========================================================================
TEST(MetadataCache, RedirectOrderingRule_EqualEpochAnswerMustPostdateTheRedirectRegression) {
    mc::MetadataCache cache;
    const std::vector<mc::BrokerEntry> brokers;
    const std::vector<mc::PartitionEntry> old = {makePartition(1, 0, /*leader=*/0, /*epoch=*/4)};
    cache.updateFromResponse(brokers, old, /*requestCorrId=*/10);
    cache.updatePartitionLeaderProvisional(1, 0, /*leader=*/1, /*epoch=*/5, /*nextRequestCorrId=*/20);
    ASSERT_EQ(cache.leaderFor(1, 0), 1);

    cache.updateFromResponse(brokers, old, 19);  // requested before the redirect
    EXPECT_EQ(cache.leaderFor(1, 0), 1) << "a pre-redirect answer undid the redirect";
    EXPECT_EQ(cache.epochFor(1, 0), 5);
    cache.updateFromResponse(brokers, old, 0);   // unsolicited
    EXPECT_EQ(cache.leaderFor(1, 0), 1) << "an unsolicited old-epoch answer undid the redirect";

    cache.updateFromResponse(brokers, old, 20);  // requested after it: spurious redirect (M5)
    EXPECT_EQ(cache.leaderFor(1, 0), 0) << "M5: the post-redirect answer must restore the old leader";
    EXPECT_EQ(cache.epochFor(1, 0), 4);

    // A higher epoch is accepted whatever its request order.
    cache.updatePartitionLeaderProvisional(1, 0, 1, 5, 30);
    const std::vector<mc::PartitionEntry> moved = {makePartition(1, 0, 2, 6)};
    cache.updateFromResponse(brokers, moved, 25);
    EXPECT_EQ(cache.leaderFor(1, 0), 2);
    EXPECT_EQ(cache.epochFor(1, 0), 6);
    // Confirmed again: plain epoch ordering, the equal epoch is accepted at any order.
    cache.updateFromResponse(brokers, moved, 0);
    EXPECT_EQ(cache.epochFor(1, 0), 6);
    cache.updateFromResponse(brokers, old, 99);
    EXPECT_EQ(cache.leaderFor(1, 0), 2) << "a lower epoch is never accepted";
}
