// ---------------------------------------------------------------------------
// CreateTopicRecordTest.cpp — version-gated durability field round-trip tests.
//
// Tests that CreateTopicRecord correctly:
//   - Encodes and decodes the `durability` byte when recordVersion >= 1
//   - Omits the `durability` byte at recordVersion == 0
//   - Defaults `durability` to 0 (Majority) when absent in v0 records
// ---------------------------------------------------------------------------

#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;

TEST(CreateTopicRecord, DurabilityRoundTripsAtVersion1) {
    CreateTopicRecord in;
    in.topicId = 7; in.name = "wallet"; in.partitionCount = 4; in.rf = 3;
    in.partitionKey = "userId"; in.placementMode = 1; in.coPartitionGroup = "acct";
    in.recordVersion = 1;
    in.durability = 1; // AllInSync
    uint8_t buf[512];
    const std::size_t n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);
    auto out = CreateTopicRecord::decode(buf, n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->recordVersion, 1);
    EXPECT_EQ(out->durability, 1);
    EXPECT_EQ(out->coPartitionGroup, "acct");
}

TEST(CreateTopicRecord, Version0OmitsDurabilityAndDefaultsToMajority) {
    CreateTopicRecord in;
    in.topicId = 1; in.name = "chat"; in.partitionCount = 1; in.rf = 1;
    in.recordVersion = 0; in.durability = 1; // must NOT be written at v0
    uint8_t buf[256];
    const std::size_t n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);
    auto out = CreateTopicRecord::decode(buf, n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->recordVersion, 0);
    EXPECT_EQ(out->durability, 0) << "absent field must default to Majority";
}
