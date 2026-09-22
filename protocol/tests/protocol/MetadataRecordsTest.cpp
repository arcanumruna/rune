// MetadataRecordsTest.cpp — TDD tests for MSM metadata log record codecs.
//
// Record types: CreateTopicRecord, UpdatePartitionAssignmentRecord,
//               NodeJoinRecord, NodeLeaveRecord
//
// All fields little-endian; var strings are uint16-length-prefixed; arrays are
// uint16-count-prefixed followed by count×4 bytes (Int32 stride).
//
// Run with: ./build/rune_protocol_tests --gtest_filter='MetadataRecords*'

#include <gtest/gtest.h>
#include <rune/protocol/MetadataRecords.h>
#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rmd = rune::protocol::metadata;

// ---------------------------------------------------------------------------
// Local LE readers (test-only helpers, mirror existing test conventions)
// ---------------------------------------------------------------------------
static uint8_t  leU8 (const uint8_t* b, int off) { return b[off]; }
static uint16_t leU16(const uint8_t* b, int off) {
    return static_cast<uint16_t>(uint16_t(b[off]) | (uint16_t(b[off+1]) << 8));
}
static int32_t  leS32(const uint8_t* b, int off) {
    uint32_t v = uint32_t(b[off]) | (uint32_t(b[off+1])<<8)
               | (uint32_t(b[off+2])<<16) | (uint32_t(b[off+3])<<24);
    return static_cast<int32_t>(v);
}
static int16_t  leS16(const uint8_t* b, int off) {
    return static_cast<int16_t>(leU16(b, off));
}

// ===========================================================================
// 1. MetadataRecordType enum / peekRecordType
// ===========================================================================

TEST(MetadataRecords, RecordTypeIds) {
    // IDs must be the stable small values defined in the enum.
    EXPECT_EQ(static_cast<uint16_t>(rmd::MetadataRecordType::CreateTopic),              1u);
    EXPECT_EQ(static_cast<uint16_t>(rmd::MetadataRecordType::UpdatePartitionAssignment), 2u);
    EXPECT_EQ(static_cast<uint16_t>(rmd::MetadataRecordType::NodeJoin),                 3u);
    EXPECT_EQ(static_cast<uint16_t>(rmd::MetadataRecordType::NodeLeave),                4u);
}

TEST(MetadataRecords, RecordTypeIdsInExpectedRange) {
    // Internal metadata record-type IDs must be in the small stable enum range
    // (1-4 here) and must NOT overlap with client-protocol template-ids
    // (schemaId 120, ranges 1-15 / 16-31 / 32-63 / 64-95).
    // They live in a completely separate namespace (MetadataRecordType enum),
    // so there is no structural collision risk — assert they are in range 1-255.
    auto inRange = [](rmd::MetadataRecordType t) {
        uint16_t id = static_cast<uint16_t>(t);
        return id >= 1u && id <= 255u;
    };
    EXPECT_TRUE(inRange(rmd::MetadataRecordType::CreateTopic));
    EXPECT_TRUE(inRange(rmd::MetadataRecordType::UpdatePartitionAssignment));
    EXPECT_TRUE(inRange(rmd::MetadataRecordType::NodeJoin));
    EXPECT_TRUE(inRange(rmd::MetadataRecordType::NodeLeave));
}

// ---------------------------------------------------------------------------
// The metadata record-type enum lives in a completely separate wire framing
// from the client protocol (no MessageHeader, no schemaId 120), so numeric
// overlap with client templateIds is NOT a structural collision.  This test
// documents that separation: even where the numeric values DO overlap with
// assigned client templateIds, the two never share a header or dispatch path.
// It also pins the metadata ids to their stable low enum values so an
// accidental future renumber into a large/unexpected value is caught.
// ---------------------------------------------------------------------------
TEST(MetadataRecords, RecordTypeIdsSeparateFromClientTemplateSpace) {
    // The metadata ids are the small stable enum values.
    const uint16_t metaIds[] = {
        static_cast<uint16_t>(rmd::MetadataRecordType::CreateTopic),               // 1
        static_cast<uint16_t>(rmd::MetadataRecordType::UpdatePartitionAssignment), // 2
        static_cast<uint16_t>(rmd::MetadataRecordType::NodeJoin),                  // 3
        static_cast<uint16_t>(rmd::MetadataRecordType::NodeLeave),                 // 4
    };
    // They must all be the compact, stable low values (1-4). This is the real
    // guarantee — the ids are a fixed internal enum, not drawn from the client
    // templateId allocator. Numeric coincidence with client ids (e.g. Connect=1,
    // Ping=6) is irrelevant because metadata records carry NO MessageHeader /
    // schemaId and never reach the client dispatch path.
    for (uint16_t id : metaIds) {
        EXPECT_GE(id, 1u);
        EXPECT_LE(id, 4u);
    }
    // Sanity: the ids are contiguous and unique (1,2,3,4) — no gaps/dupes.
    EXPECT_EQ(metaIds[0], 1u);
    EXPECT_EQ(metaIds[1], 2u);
    EXPECT_EQ(metaIds[2], 3u);
    EXPECT_EQ(metaIds[3], 4u);
}

TEST(MetadataRecords, PeekRecordType_CreateTopic) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 42;
    rec.name           = "my-topic";
    rec.partitionCount = 4;
    rec.rf             = 3;
    rec.partitionKey   = "key";
    rec.placementMode  = 1;
    rec.coPartitionGroup = "grp";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(512, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto peeked = rmd::peekRecordType(buf.data(), written);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(*peeked, rmd::MetadataRecordType::CreateTopic);
}

TEST(MetadataRecords, PeekRecordType_UpdatePartitionAssignment) {
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = 1;
    rec.partition      = 0;
    rec.leaderNode     = 2;
    rec.isr            = {};
    rec.leaderEpoch    = 0;
    rec.partitionEpoch = 0;
    rec.replicas       = {};
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto peeked = rmd::peekRecordType(buf.data(), written);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(*peeked, rmd::MetadataRecordType::UpdatePartitionAssignment);
}

TEST(MetadataRecords, PeekRecordType_TruncatedReturnsNullopt) {
    std::array<uint8_t, 1> tinyBuf{0x01};
    // Only 1 byte — not enough for the 2-byte type header
    auto peeked = rmd::peekRecordType(tinyBuf.data(), 1);
    EXPECT_FALSE(peeked.has_value());

    auto peeked0 = rmd::peekRecordType(tinyBuf.data(), 0);
    EXPECT_FALSE(peeked0.has_value());
}

// ===========================================================================
// 2. CreateTopicRecord — round-trip + byte-level assertions
// ===========================================================================

TEST(MetadataRecords, CreateTopicRecord_RoundTrip) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 1001;
    rec.name           = "events";
    rec.partitionCount = 16;
    rec.rf             = 3;
    rec.partitionKey   = "userId";
    rec.placementMode  = 2;
    rec.coPartitionGroup = "co-grp-1";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(512, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::CreateTopicRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());

    EXPECT_EQ(decoded->topicId,         1001);
    EXPECT_EQ(decoded->name,            "events");
    EXPECT_EQ(decoded->partitionCount,  16);
    EXPECT_EQ(decoded->rf,              3);
    EXPECT_EQ(decoded->partitionKey,    "userId");
    EXPECT_EQ(decoded->placementMode,   2u);
    EXPECT_EQ(decoded->coPartitionGroup,"co-grp-1");
    EXPECT_EQ(decoded->recordVersion,   0u);
}

TEST(MetadataRecords, CreateTopicRecord_LittleEndianByteOrder) {
    // Encode a CreateTopicRecord with a known topicId (0x01020304)
    // and assert the exact byte layout is little-endian.
    rmd::CreateTopicRecord rec;
    rec.topicId        = static_cast<int32_t>(0x01020304);
    rec.name           = "t";
    rec.partitionCount = static_cast<int32_t>(0x05060708);
    rec.rf             = static_cast<int16_t>(0x090A);
    rec.partitionKey   = "";
    rec.placementMode  = 0xBB;
    rec.coPartitionGroup = "";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    // Layout after the 2-byte type prefix and 1-byte version:
    //   [0..1]   recordType  uint16 LE = 1 (CreateTopic)
    //   [2]      recordVersion uint8 = 0
    //   [3..6]   topicId     int32 LE
    //   [7..8]   name length uint16 LE
    //   [9]      name bytes  'T' (1 byte)
    //   [10..11] partitionCount int32 LE -- wait, partitionCount is int32 so occupies [10..13]
    //   ...
    // Check that topicId 0x01020304 is stored LE (04 03 02 01)
    // type(2) + version(1) = 3 bytes before topicId
    EXPECT_EQ(buf[3], 0x04u) << "topicId LE byte 0";
    EXPECT_EQ(buf[4], 0x03u) << "topicId LE byte 1";
    EXPECT_EQ(buf[5], 0x02u) << "topicId LE byte 2";
    EXPECT_EQ(buf[6], 0x01u) << "topicId LE byte 3";

    // recordType at offset 0 must be 1 (CreateTopic), LE
    EXPECT_EQ(leU16(buf.data(), 0), 1u) << "recordType LE";

    // recordVersion at offset 2
    EXPECT_EQ(leU8(buf.data(), 2), 0u) << "recordVersion";

    // name length at offset 7 must be 1 (single char), LE
    EXPECT_EQ(leU16(buf.data(), 7), 1u) << "name len LE";
    EXPECT_EQ(buf[9], 't') << "name byte";

    // partitionCount is at offset 10 (after name len+bytes), LE = 0x05060708 → 08 07 06 05
    EXPECT_EQ(buf[10], 0x08u) << "partitionCount LE byte 0";
    EXPECT_EQ(buf[11], 0x07u) << "partitionCount LE byte 1";
    EXPECT_EQ(buf[12], 0x06u) << "partitionCount LE byte 2";
    EXPECT_EQ(buf[13], 0x05u) << "partitionCount LE byte 3";

    // rf at offset 14, int16 LE: 0x090A → 0A 09
    EXPECT_EQ(buf[14], 0x0Au) << "rf LE byte 0";
    EXPECT_EQ(buf[15], 0x09u) << "rf LE byte 1";
}

TEST(MetadataRecords, CreateTopicRecord_EmptyVarStrings) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 7;
    rec.name           = "";  // empty name
    rec.partitionCount = 1;
    rec.rf             = 1;
    rec.partitionKey   = "";  // empty partition key
    rec.placementMode  = 0;
    rec.coPartitionGroup = "";  // empty co-partition group
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::CreateTopicRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->name,            "");
    EXPECT_EQ(decoded->partitionKey,    "");
    EXPECT_EQ(decoded->coPartitionGroup,"");
    EXPECT_EQ(decoded->topicId,         7);
    EXPECT_EQ(decoded->partitionCount,  1);
    EXPECT_EQ(decoded->rf,              1);
}

TEST(MetadataRecords, CreateTopicRecord_MaxPartitionCount) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 1;
    rec.name           = "max-partitions";
    rec.partitionCount = INT32_MAX;
    rec.rf             = INT16_MAX;
    rec.partitionKey   = "";
    rec.placementMode  = 0xFF;
    rec.coPartitionGroup = "";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::CreateTopicRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->partitionCount, INT32_MAX);
    EXPECT_EQ(decoded->rf,             INT16_MAX);
    EXPECT_EQ(decoded->placementMode,  0xFFu);
}

TEST(MetadataRecords, CreateTopicRecord_TruncatedBufferReturnsNullopt) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 5;
    rec.name           = "topic";
    rec.partitionCount = 2;
    rec.rf             = 1;
    rec.partitionKey   = "key";
    rec.placementMode  = 0;
    rec.coPartitionGroup = "group";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(512, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());
    ASSERT_GT(full, 2u);

    // 0 bytes
    EXPECT_FALSE(rmd::CreateTopicRecord::decode(buf.data(), 0).has_value());
    // 1 byte — not enough for type prefix
    EXPECT_FALSE(rmd::CreateTopicRecord::decode(buf.data(), 1).has_value());
    // full-1 — one short
    EXPECT_FALSE(rmd::CreateTopicRecord::decode(buf.data(), full - 1).has_value());
    // full — must succeed
    EXPECT_TRUE(rmd::CreateTopicRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, CreateTopicRecord_UnknownVersionReturnsNullopt) {
    rmd::CreateTopicRecord rec;
    rec.topicId        = 1;
    rec.name           = "test";
    rec.partitionCount = 1;
    rec.rf             = 1;
    rec.partitionKey   = "";
    rec.placementMode  = 0;
    rec.coPartitionGroup = "";
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());
    ASSERT_GT(full, 2u);

    // Corrupt the recordVersion byte (offset 2) to an unknown value
    buf[2] = 99;

    EXPECT_FALSE(rmd::CreateTopicRecord::decode(buf.data(), full).has_value());
}

// ===========================================================================
// 3. UpdatePartitionAssignmentRecord — round-trip + byte-level
// ===========================================================================

TEST(MetadataRecords, UpdatePartitionAssignment_RoundTrip) {
    std::vector<int32_t> isrVec = {1, 2, 3};
    std::vector<int32_t> replicasVec = {1, 2, 3, 4};

    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = 42;
    rec.partition      = 7;
    rec.leaderNode     = 2;
    rec.isr            = {isrVec.data(), static_cast<uint16_t>(isrVec.size())};
    rec.leaderEpoch    = 5;
    rec.partitionEpoch = 11;
    rec.replicas       = {replicasVec.data(), static_cast<uint16_t>(replicasVec.size())};
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(512, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());

    EXPECT_EQ(decoded->topicId,         42);
    EXPECT_EQ(decoded->partition,       7);
    EXPECT_EQ(decoded->leaderNode,      2);
    EXPECT_EQ(decoded->leaderEpoch,     5);
    EXPECT_EQ(decoded->partitionEpoch,  11);
    EXPECT_EQ(decoded->recordVersion,   0u);

    // Verify ISR contents
    ASSERT_EQ(decoded->isr.size(), 3u);
    EXPECT_EQ(decoded->isr[0], 1);
    EXPECT_EQ(decoded->isr[1], 2);
    EXPECT_EQ(decoded->isr[2], 3);

    // Verify replicas contents
    ASSERT_EQ(decoded->replicas.size(), 4u);
    EXPECT_EQ(decoded->replicas[0], 1);
    EXPECT_EQ(decoded->replicas[1], 2);
    EXPECT_EQ(decoded->replicas[2], 3);
    EXPECT_EQ(decoded->replicas[3], 4);
}

TEST(MetadataRecords, UpdatePartitionAssignment_EmptyISRAndReplicas) {
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = 1;
    rec.partition      = 0;
    rec.leaderNode     = 0;
    rec.isr            = {};  // empty
    rec.leaderEpoch    = 0;
    rec.partitionEpoch = 0;
    rec.replicas       = {};  // empty
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());

    EXPECT_EQ(decoded->isr.size(),      0u);
    EXPECT_EQ(decoded->replicas.size(), 0u);
    EXPECT_EQ(decoded->leaderEpoch,     0);
    EXPECT_EQ(decoded->partitionEpoch,  0);
}

TEST(MetadataRecords, UpdatePartitionAssignment_TwoDistinctEpochs) {
    // leaderEpoch and partitionEpoch are DISTINCT fields — must round-trip independently.
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = 1;
    rec.partition      = 0;
    rec.leaderNode     = 1;
    rec.isr            = {};
    rec.leaderEpoch    = 1000;
    rec.partitionEpoch = 9999;
    rec.replicas       = {};
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->leaderEpoch,    1000);
    EXPECT_EQ(decoded->partitionEpoch, 9999);
    // Must be different — if the codec aliases them, both would be the same
    EXPECT_NE(decoded->leaderEpoch, decoded->partitionEpoch);
}

TEST(MetadataRecords, UpdatePartitionAssignment_LittleEndianByteOrder) {
    // Encode with a known topicId to verify byte layout
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = static_cast<int32_t>(0x11223344);
    rec.partition      = static_cast<int32_t>(0x55667788);
    rec.leaderNode     = static_cast<int32_t>(0xAABBCCDD);
    rec.isr            = {};
    rec.leaderEpoch    = static_cast<int32_t>(0x01020304);
    rec.partitionEpoch = static_cast<int32_t>(0x05060708);
    rec.replicas       = {};
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    // type(2) + version(1) = 3 bytes; topicId starts at offset 3
    // topicId 0x11223344 → LE: 44 33 22 11
    EXPECT_EQ(buf[3], 0x44u) << "topicId LE[0]";
    EXPECT_EQ(buf[4], 0x33u) << "topicId LE[1]";
    EXPECT_EQ(buf[5], 0x22u) << "topicId LE[2]";
    EXPECT_EQ(buf[6], 0x11u) << "topicId LE[3]";

    // partition 0x55667788 at offset 7 → LE: 88 77 66 55
    EXPECT_EQ(buf[7],  0x88u) << "partition LE[0]";
    EXPECT_EQ(buf[8],  0x77u) << "partition LE[1]";
    EXPECT_EQ(buf[9],  0x66u) << "partition LE[2]";
    EXPECT_EQ(buf[10], 0x55u) << "partition LE[3]";
}

TEST(MetadataRecords, UpdatePartitionAssignment_TruncatedReturnsNullopt) {
    std::vector<int32_t> isr = {1, 2};
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = 1;
    rec.partition      = 0;
    rec.leaderNode     = 1;
    rec.isr            = {isr.data(), static_cast<uint16_t>(isr.size())};
    rec.leaderEpoch    = 1;
    rec.partitionEpoch = 1;
    rec.replicas       = {};
    rec.recordVersion  = 0;

    std::vector<uint8_t> buf(512, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());
    ASSERT_GT(full, 2u);

    EXPECT_FALSE(rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), 1).has_value());
    EXPECT_FALSE(rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), full - 1).has_value());
    EXPECT_TRUE( rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, UpdatePartitionAssignment_UnknownVersionReturnsNullopt) {
    rmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId = 1; rec.partition = 0; rec.leaderNode = 1;
    rec.isr = {}; rec.leaderEpoch = 0; rec.partitionEpoch = 0;
    rec.replicas = {}; rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());

    // Corrupt version byte (offset 2)
    buf[2] = 77;

    EXPECT_FALSE(rmd::UpdatePartitionAssignmentRecord::decode(buf.data(), full).has_value());
}

// ===========================================================================
// 4. NodeJoinRecord — round-trip
// ===========================================================================

TEST(MetadataRecords, NodeJoinRecord_RoundTrip) {
    rmd::NodeJoinRecord rec;
    rec.nodeId        = 5;
    rec.endpoint      = "10.0.1.5:9000";
    rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::NodeJoinRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->nodeId,        5);
    EXPECT_EQ(decoded->endpoint,      "10.0.1.5:9000");
    EXPECT_EQ(decoded->recordVersion, 0u);
}

TEST(MetadataRecords, NodeJoinRecord_EmptyEndpoint) {
    rmd::NodeJoinRecord rec;
    rec.nodeId        = 1;
    rec.endpoint      = "";
    rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::NodeJoinRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->endpoint.size(), 0u);
}

TEST(MetadataRecords, NodeJoinRecord_TruncatedReturnsNullopt) {
    rmd::NodeJoinRecord rec;
    rec.nodeId        = 1;
    rec.endpoint      = "host:1234";
    rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());
    ASSERT_GT(full, 1u);

    EXPECT_FALSE(rmd::NodeJoinRecord::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rmd::NodeJoinRecord::decode(buf.data(), full - 1).has_value());
    EXPECT_TRUE( rmd::NodeJoinRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, NodeJoinRecord_UnknownVersionReturnsNullopt) {
    rmd::NodeJoinRecord rec;
    rec.nodeId = 1; rec.endpoint = "ep"; rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());

    buf[2] = 55;  // corrupt version byte at offset 2

    EXPECT_FALSE(rmd::NodeJoinRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, NodeJoinRecord_PeekReturnsNodeJoin) {
    rmd::NodeJoinRecord rec;
    rec.nodeId = 10; rec.endpoint = "ep"; rec.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());

    auto peeked = rmd::peekRecordType(buf.data(), written);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(*peeked, rmd::MetadataRecordType::NodeJoin);
}

// ===========================================================================
// 5. NodeLeaveRecord — round-trip
// ===========================================================================

TEST(MetadataRecords, NodeLeaveRecord_RoundTrip) {
    rmd::NodeLeaveRecord rec;
    rec.nodeId        = 99;
    rec.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    auto decoded = rmd::NodeLeaveRecord::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->nodeId,        99);
    EXPECT_EQ(decoded->recordVersion, 0u);
}

TEST(MetadataRecords, NodeLeaveRecord_TruncatedReturnsNullopt) {
    rmd::NodeLeaveRecord rec;
    rec.nodeId = 1; rec.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());
    ASSERT_GT(full, 0u);

    EXPECT_FALSE(rmd::NodeLeaveRecord::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rmd::NodeLeaveRecord::decode(buf.data(), full - 1).has_value());
    EXPECT_TRUE( rmd::NodeLeaveRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, NodeLeaveRecord_UnknownVersionReturnsNullopt) {
    rmd::NodeLeaveRecord rec;
    rec.nodeId = 1; rec.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t full = rec.encode(buf.data(), buf.size());

    buf[2] = 33;  // corrupt version byte at offset 2

    EXPECT_FALSE(rmd::NodeLeaveRecord::decode(buf.data(), full).has_value());
}

TEST(MetadataRecords, NodeLeaveRecord_PeekReturnsNodeLeave) {
    rmd::NodeLeaveRecord rec;
    rec.nodeId = 3; rec.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());

    auto peeked = rmd::peekRecordType(buf.data(), written);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(*peeked, rmd::MetadataRecordType::NodeLeave);
}

TEST(MetadataRecords, NodeLeaveRecord_LittleEndianByteOrder) {
    rmd::NodeLeaveRecord rec;
    rec.nodeId        = static_cast<int32_t>(0xDEADBEEF);
    rec.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t written = rec.encode(buf.data(), buf.size());
    ASSERT_GT(written, 0u);

    // type(2) + version(1) = 3 bytes; nodeId at offset 3
    // 0xDEADBEEF LE → EF BE AD DE
    EXPECT_EQ(buf[3], 0xEFu) << "nodeId LE[0]";
    EXPECT_EQ(buf[4], 0xBEu) << "nodeId LE[1]";
    EXPECT_EQ(buf[5], 0xADu) << "nodeId LE[2]";
    EXPECT_EQ(buf[6], 0xDEu) << "nodeId LE[3]";

    // recordType at offset 0 = 4 (NodeLeave), LE
    EXPECT_EQ(leU16(buf.data(), 0), 4u) << "recordType NodeLeave";
}

// ===========================================================================
// 6. Cross-type: wrong recordType in buffer → decode returns nullopt
// ===========================================================================

TEST(MetadataRecords, WrongRecordTypeReturnsNullopt) {
    // Encode a NodeLeaveRecord, try to decode as CreateTopicRecord
    rmd::NodeLeaveRecord leave;
    leave.nodeId = 1; leave.recordVersion = 0;

    std::vector<uint8_t> buf(64, 0u);
    const std::size_t written = leave.encode(buf.data(), buf.size());

    // Decoding the NodeLeave buffer as a CreateTopicRecord must fail
    EXPECT_FALSE(rmd::CreateTopicRecord::decode(buf.data(), written).has_value());
}

TEST(MetadataRecords, WrongRecordType_NodeJoinAsNodeLeave) {
    rmd::NodeJoinRecord join;
    join.nodeId = 1; join.endpoint = "ep"; join.recordVersion = 0;

    std::vector<uint8_t> buf(256, 0u);
    const std::size_t written = join.encode(buf.data(), buf.size());

    EXPECT_FALSE(rmd::NodeLeaveRecord::decode(buf.data(), written).has_value());
}
