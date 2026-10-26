// ---------------------------------------------------------------------------
// RoleCounterTest — unit tests for the pure node-role metric encoding (RuneMetrics.h).
//
// Verifies the key encode/decode round-trip and the RoleCode <-> value mapping
// that the node-role metric relies on.  No Aeron driver required — this
// is the machine-readable leadership contract between BrokerNode (writer) and
// rune_role (reader), so it must be exactly round-trippable.
// ---------------------------------------------------------------------------
#include <rune/node/RuneMetrics.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

using namespace rune::node;

TEST(RoleCounterTest, KeyLengthIsFixedFourBytes) {
    EXPECT_EQ(kRoleKeyLength, 4u);
}

TEST(RoleCounterTest, EncodeDecodeKeyRoundTrip) {
    for (std::int32_t nodeId : {0, 1, 2, 7, 42, 2147483647, -1}) {
        std::uint8_t buf[kRoleKeyLength] = {};
        encodeRoleKey(buf, nodeId);
        std::int32_t out = -12345;
        ASSERT_TRUE(decodeRoleKey(buf, kRoleKeyLength, out));
        EXPECT_EQ(out, nodeId) << "nodeId round-trip failed";
    }
}

TEST(RoleCounterTest, DecodeRejectsShortBuffer) {
    std::uint8_t buf[kRoleKeyLength] = {};
    encodeRoleKey(buf, 3);
    std::int32_t out = -1;
    EXPECT_FALSE(decodeRoleKey(buf, kRoleKeyLength - 1, out));
    EXPECT_FALSE(decodeRoleKey(nullptr, kRoleKeyLength, out));
}

TEST(RoleCounterTest, RoleCodesMirrorAeronClusterRole) {
    // Rune mirrors Aeron Cluster.Role codes exactly (FOLLOWER=0, CANDIDATE=1,
    // LEADER=2) with a NONE=-1 sentinel.
    EXPECT_EQ(static_cast<std::int64_t>(RoleCode::Follower), 0);
    EXPECT_EQ(static_cast<std::int64_t>(RoleCode::Candidate), 1);
    EXPECT_EQ(static_cast<std::int64_t>(RoleCode::Leader), 2);
    EXPECT_EQ(static_cast<std::int64_t>(RoleCode::None), -1);
}

TEST(RoleCounterTest, RoleCodeFromValueMapsKnownAndUnknown) {
    EXPECT_EQ(roleCodeFromValue(0), RoleCode::Follower);
    EXPECT_EQ(roleCodeFromValue(1), RoleCode::Candidate);
    EXPECT_EQ(roleCodeFromValue(2), RoleCode::Leader);
    EXPECT_EQ(roleCodeFromValue(-1), RoleCode::None);
    // Unknown/garbage values decode to None (never a spurious leader).
    EXPECT_EQ(roleCodeFromValue(99), RoleCode::None);
    EXPECT_EQ(roleCodeFromValue(-7), RoleCode::None);
}

TEST(RoleCounterTest, RoleCodeNameStrings) {
    EXPECT_STREQ(roleCodeName(RoleCode::Leader), "LEADER");
    EXPECT_STREQ(roleCodeName(RoleCode::Follower), "FOLLOWER");
    EXPECT_STREQ(roleCodeName(RoleCode::Candidate), "CANDIDATE");
    EXPECT_STREQ(roleCodeName(RoleCode::None), "NONE");
}

TEST(RoleCounterTest, TypeIdsAreDistinctAndInRuneRange) {
    EXPECT_NE(typeIdOf(RuneMetric::Role), typeIdOf(RuneMetric::Term));
    // In Rune's own application range, not Aeron's reserved 200-221.
    EXPECT_GE(typeIdOf(RuneMetric::Role), 1'000'000);
    EXPECT_GE(typeIdOf(RuneMetric::Term), 1'000'000);
}

// D61: the type ids and label prefixes a reader of the old cnc.dat counters matched stay the same in the metrics file
// (the node writes "<labelPrefix> node=<id>").
TEST(RoleCounterTest, RoleAndTermRowsKeepTheirTypeIdsAndLabels) {
    EXPECT_EQ(typeIdOf(RuneMetric::Role), 1'000'201);
    EXPECT_EQ(typeIdOf(RuneMetric::Term), 1'000'203);
    EXPECT_EQ(typeIdOf(RuneMetric::OutOfSyncPartitions), 1'000'211);
    EXPECT_STREQ(kRuneMetrics[static_cast<std::size_t>(runeMetricRow(RuneMetric::Role))].labelPrefix, "rune-cluster-node-role");
    EXPECT_STREQ(kRuneMetrics[static_cast<std::size_t>(runeMetricRow(RuneMetric::Term))].labelPrefix,
                 "rune-cluster-leadership-term");
    EXPECT_EQ(kRuneMetrics[static_cast<std::size_t>(runeMetricRow(RuneMetric::Role))].owner, MetricOwner::Observer);
}
