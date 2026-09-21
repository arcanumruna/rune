// ---------------------------------------------------------------------------
// RoleCounterTest — unit tests for the pure RoleCounter wire encoding.
//
// Verifies the key encode/decode round-trip and the RoleCode <-> value mapping
// that the Aeron node-role counter relies on.  No Aeron driver required — this
// is the machine-readable leadership contract between BrokerNode (writer) and
// rune_role (reader), so it must be exactly round-trippable.
// ---------------------------------------------------------------------------
#include <rune/node/RoleCounter.h>

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
    EXPECT_NE(kRoleCounterTypeId, kTermCounterTypeId);
    // In Rune's own application range, not Aeron's reserved 200-221.
    EXPECT_GE(kRoleCounterTypeId, 1'000'000);
    EXPECT_GE(kTermCounterTypeId, 1'000'000);
}

TEST(RoleCounterTest, LabelsIncludeNodeId) {
    EXPECT_NE(roleCounterLabel(5).find("node=5"), std::string::npos);
    EXPECT_NE(termCounterLabel(5).find("node=5"), std::string::npos);
}
