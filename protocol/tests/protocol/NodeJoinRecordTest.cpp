// NodeJoinRecordTest.cpp — TDD tests for NodeJoinRecord v1 (incarnationId + brokerEpoch).
//
// These tests verify version-gated encode/decode for the v1 wire extension:
//   - incarnationId (int64 LE, only when recordVersion >= 1)
//   - brokerEpoch   (int64 LE, only when recordVersion >= 1)
//
// v0 decode must return incarnationId=0, brokerEpoch=0 (defaults).

#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;

TEST(NodeJoinRecord, V1CarriesIncarnationAndBrokerEpoch) {
    NodeJoinRecord in; in.nodeId = 3; in.endpoint = "h:9"; in.recordVersion = 1;
    in.incarnationId = 0x1122334455667788LL; in.brokerEpoch = 4096;
    uint8_t buf[128]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = NodeJoinRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->incarnationId, 0x1122334455667788LL);
    EXPECT_EQ(out->brokerEpoch, 4096);
}

TEST(NodeJoinRecord, V0DecodesWithoutNewFields) {
    NodeJoinRecord in; in.nodeId = 3; in.endpoint = "h:9"; in.recordVersion = 0;
    uint8_t buf[64]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = NodeJoinRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->incarnationId, 0);
    EXPECT_EQ(out->brokerEpoch, 0);
}

TEST(NodeJoinRecord, V1NodeIdAndEndpointPreserved) {
    NodeJoinRecord in;
    in.nodeId = 42; in.endpoint = "node42.cluster:9090"; in.recordVersion = 1;
    in.incarnationId = 0xDEADBEEFCAFEBABELL; in.brokerEpoch = 999;
    uint8_t buf[256]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = NodeJoinRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->nodeId, 42);
    EXPECT_EQ(out->endpoint, "node42.cluster:9090");
    EXPECT_EQ(out->recordVersion, 1u);
    EXPECT_EQ(out->incarnationId, 0xDEADBEEFCAFEBABELL);
    EXPECT_EQ(out->brokerEpoch, 999);
}

TEST(NodeJoinRecord, V1TruncatedMidNewFieldsReturnsNullopt) {
    // A v1 record whose buf is cut short mid-incarnationId must return nullopt.
    NodeJoinRecord in;
    in.nodeId = 1; in.endpoint = "h:1"; in.recordVersion = 1;
    in.incarnationId = 100; in.brokerEpoch = 200;
    uint8_t buf[256]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    // Truncate by 1 byte (removes last byte of brokerEpoch).
    EXPECT_FALSE(NodeJoinRecord::decode(buf, n - 1).has_value());
}

TEST(NodeJoinRecord, V1UnknownVersionAbove1ReturnsNullopt) {
    NodeJoinRecord in;
    in.nodeId = 1; in.endpoint = "h:1"; in.recordVersion = 1;
    in.incarnationId = 1; in.brokerEpoch = 1;
    uint8_t buf[256]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    buf[2] = 99; // corrupt version
    EXPECT_FALSE(NodeJoinRecord::decode(buf, n).has_value());
}
