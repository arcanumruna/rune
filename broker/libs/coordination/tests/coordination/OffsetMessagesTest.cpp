#include <gtest/gtest.h>
#include <rune/coordination/OffsetMessages.h>

#include <array>
#include <cstring>
#include <string_view>

using namespace rune::coordination;

// ---------------------------------------------------------------------------
// Buffer for encode/decode — 4 KiB is plenty for these small messages.
// ---------------------------------------------------------------------------
static constexpr std::size_t kBufSize = 4096;

// ===========================================================================
// OffsetMessageHeader
// ===========================================================================

TEST(OffsetMessages, HeaderSchemaId) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetMessageHeader::encode(buf.data(), 0, 10, 1);
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.schemaId, kOffsetSchemaId);
    EXPECT_EQ(h.schemaId, uint16_t{113});
}

TEST(OffsetMessages, HeaderVersion) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetMessageHeader::encode(buf.data(), 0, 10, 2);
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.version, kOffsetSchemaVersion);
    EXPECT_EQ(h.version, uint16_t{1});
}

TEST(OffsetMessages, HeaderBlockLengthAndTemplateId) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetMessageHeader::encode(buf.data(), 0, 22, 4);
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.blockLength, 22u);
    EXPECT_EQ(h.templateId, 4u);
}

TEST(OffsetMessages, HeaderEncodedSize) {
    EXPECT_EQ(OffsetMessageHeader::ENCODED_SIZE, std::size_t{8});
}

TEST(OffsetMessages, PeekTemplateId) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetMessageHeader::encode(buf.data(), 0, 12, 3);
    EXPECT_EQ(peekOffsetTemplateId(buf.data(), 0), uint16_t{3});
}

// Little-endian: templateId at bytes [2..3], schemaId at [4..5], version at [6..7].
TEST(OffsetMessages, HeaderLittleEndianLayout) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetMessageHeader::encode(buf.data(), 0, /*blockLen=*/0x0102, /*tmplId=*/0x0304);
    // blockLength at byte 0: 0x0102 little-endian → [0x02, 0x01]
    EXPECT_EQ(buf[0], 0x02u);
    EXPECT_EQ(buf[1], 0x01u);
    // templateId at byte 2: 0x0304 → [0x04, 0x03]
    EXPECT_EQ(buf[2], 0x04u);
    EXPECT_EQ(buf[3], 0x03u);
    // schemaId at byte 4: 113 = 0x0071 → [0x71, 0x00]
    EXPECT_EQ(buf[4], 0x71u);
    EXPECT_EQ(buf[5], 0x00u);
    // version at byte 6: 1 = 0x0001 → [0x01, 0x00]
    EXPECT_EQ(buf[6], 0x01u);
    EXPECT_EQ(buf[7], 0x00u);
}

// ===========================================================================
// OffsetCommitRequest (templateId = 1)
// ===========================================================================

TEST(OffsetMessages, CommitRequestTemplateId) {
    EXPECT_EQ(OffsetCommitRequestMsg::TEMPLATE_ID, uint16_t{1});
}

TEST(OffsetMessages, CommitRequestBlockLength) {
    EXPECT_EQ(OffsetCommitRequestMsg::BLOCK_LENGTH, uint16_t{12});
}

TEST(OffsetMessages, CommitRequestRoundTrip) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitRequestMsg::encode(buf.data(), 0,
                                    /*offset=*/12345LL,
                                    /*partition=*/7,
                                    "my-group", "my-topic", "some-metadata");
    auto m = OffsetCommitRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.offset,    12345LL);
    EXPECT_EQ(m.partition, 7);
    EXPECT_EQ(m.group,     "my-group");
    EXPECT_EQ(m.topic,     "my-topic");
    EXPECT_EQ(m.metadata,  "some-metadata");
}

TEST(OffsetMessages, CommitRequestRoundTripEmptyMeta) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitRequestMsg::encode(buf.data(), 0, 0LL, 0, "g", "t", "");
    auto m = OffsetCommitRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.group,    "g");
    EXPECT_EQ(m.topic,    "t");
    EXPECT_EQ(m.metadata, "");
    EXPECT_EQ(m.offset,   0LL);
}

TEST(OffsetMessages, CommitRequestLargeOffset) {
    std::array<uint8_t, kBufSize> buf{};
    constexpr int64_t bigOff = int64_t{1} << 50;
    OffsetCommitRequestMsg::encode(buf.data(), 0, bigOff, -1, "g", "t", "m");
    auto m = OffsetCommitRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.offset,    bigOff);
    EXPECT_EQ(m.partition, -1);
}

TEST(OffsetMessages, CommitRequestHeaderFields) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitRequestMsg::encode(buf.data(), 0, 1LL, 0, "g", "t", "m");
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{1});
    EXPECT_EQ(h.schemaId,    uint16_t{113});
    EXPECT_EQ(h.version,     uint16_t{1});
    EXPECT_EQ(h.blockLength, uint16_t{12});
}

TEST(OffsetMessages, CommitRequestLongGroupTopic) {
    std::array<uint8_t, kBufSize> buf{};
    std::string longGroup(200, 'A');
    std::string longTopic(200, 'B');
    std::string longMeta(500, 'C');
    OffsetCommitRequestMsg::encode(buf.data(), 0, 999LL, 3,
                                    longGroup, longTopic, longMeta);
    auto m = OffsetCommitRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.group,    longGroup);
    EXPECT_EQ(m.topic,    longTopic);
    EXPECT_EQ(m.metadata, longMeta);
    EXPECT_EQ(m.offset,   999LL);
    EXPECT_EQ(m.partition, 3);
}

TEST(OffsetMessages, CommitRequestNonZeroOffset) {
    std::array<uint8_t, kBufSize> buf{};
    // Encode at non-zero buffer offset to check bufOffset parameter.
    OffsetCommitRequestMsg::encode(buf.data(), 100, 55LL, 2, "grp", "top", "meta");
    auto m = OffsetCommitRequestMsg::decode(buf.data(), 100);
    EXPECT_EQ(m.offset, 55LL);
    EXPECT_EQ(m.group,  "grp");
}

// Byte-exact: offset field must be at byte 8 (after 8-byte header).
TEST(OffsetMessages, CommitRequestOffsetLittleEndian) {
    std::array<uint8_t, kBufSize> buf{};
    // offset = 0x0102030405060708LL
    int64_t testOff = 0x0102030405060708LL;
    OffsetCommitRequestMsg::encode(buf.data(), 0, testOff, 0, "g", "t", "");
    // Bytes [8..15] = little-endian of testOff
    uint8_t expected[8];
    std::memcpy(expected, &testOff, 8); // native LE
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(buf[8 + i], expected[i]) << "byte offset " << (8 + i);
    }
}

// ===========================================================================
// OffsetCommitResponse (templateId = 2)
// ===========================================================================

TEST(OffsetMessages, CommitResponseTemplateId) {
    EXPECT_EQ(OffsetCommitResponseMsg::TEMPLATE_ID, uint16_t{2});
}

TEST(OffsetMessages, CommitResponseBlockLength) {
    EXPECT_EQ(OffsetCommitResponseMsg::BLOCK_LENGTH, uint16_t{6});
}

TEST(OffsetMessages, CommitResponseRoundTrip) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitResponseMsg::encode(buf.data(), 0, /*errorCode=*/0, /*partition=*/3,
                                     "consumer-group", "partition-topic");
    auto m = OffsetCommitResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.errorCode, 0);
    EXPECT_EQ(m.partition, 3);
    EXPECT_EQ(m.group,     "consumer-group");
    EXPECT_EQ(m.topic,     "partition-topic");
}

TEST(OffsetMessages, CommitResponseErrorCode) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitResponseMsg::encode(buf.data(), 0, /*errorCode=*/42, 0, "g", "t");
    auto m = OffsetCommitResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.errorCode, 42);
}

TEST(OffsetMessages, CommitResponseHeaderFields) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitResponseMsg::encode(buf.data(), 0, 0, 0, "g", "t");
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{2});
    EXPECT_EQ(h.schemaId,    uint16_t{113});
    EXPECT_EQ(h.version,     uint16_t{1});
    EXPECT_EQ(h.blockLength, uint16_t{6});
}

TEST(OffsetMessages, CommitResponseEmptyGroupTopic) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetCommitResponseMsg::encode(buf.data(), 0, 0, 0, "", "");
    auto m = OffsetCommitResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.group, "");
    EXPECT_EQ(m.topic, "");
}

// ===========================================================================
// OffsetFetchRequest (templateId = 3)
// ===========================================================================

TEST(OffsetMessages, FetchRequestTemplateId) {
    EXPECT_EQ(OffsetFetchRequestMsg::TEMPLATE_ID, uint16_t{3});
}

TEST(OffsetMessages, FetchRequestBlockLength) {
    EXPECT_EQ(OffsetFetchRequestMsg::BLOCK_LENGTH, uint16_t{4});
}

TEST(OffsetMessages, FetchRequestRoundTrip) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchRequestMsg::encode(buf.data(), 0, /*partition=*/5, "grp", "topic");
    auto m = OffsetFetchRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.partition, 5);
    EXPECT_EQ(m.group,     "grp");
    EXPECT_EQ(m.topic,     "topic");
}

TEST(OffsetMessages, FetchRequestHeaderFields) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchRequestMsg::encode(buf.data(), 0, 0, "g", "t");
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{3});
    EXPECT_EQ(h.schemaId,    uint16_t{113});
    EXPECT_EQ(h.version,     uint16_t{1});
    EXPECT_EQ(h.blockLength, uint16_t{4});
}

TEST(OffsetMessages, FetchRequestEmptyStrings) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchRequestMsg::encode(buf.data(), 0, 0, "", "");
    auto m = OffsetFetchRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.group, "");
    EXPECT_EQ(m.topic, "");
}

TEST(OffsetMessages, FetchRequestLongStrings) {
    std::array<uint8_t, kBufSize> buf{};
    std::string longG(100, 'X');
    std::string longT(100, 'Y');
    OffsetFetchRequestMsg::encode(buf.data(), 0, 99, longG, longT);
    auto m = OffsetFetchRequestMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.group,     longG);
    EXPECT_EQ(m.topic,     longT);
    EXPECT_EQ(m.partition, 99);
}

// Byte-exact: partition at bytes [8..11] (after 8-byte header).
TEST(OffsetMessages, FetchRequestPartitionLittleEndian) {
    std::array<uint8_t, kBufSize> buf{};
    int32_t testPart = 0x01020304;
    OffsetFetchRequestMsg::encode(buf.data(), 0, testPart, "g", "t");
    uint8_t expected[4];
    std::memcpy(expected, &testPart, 4);
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(buf[8 + i], expected[i]) << "byte " << (8 + i);
    }
}

// ===========================================================================
// OffsetFetchResponse (templateId = 4)
// ===========================================================================

TEST(OffsetMessages, FetchResponseTemplateId) {
    EXPECT_EQ(OffsetFetchResponseMsg::TEMPLATE_ID, uint16_t{4});
}

TEST(OffsetMessages, FetchResponseBlockLength) {
    // int64(offset) + int64(ts) + int16(err) + int32(partition) = 22
    EXPECT_EQ(OffsetFetchResponseMsg::BLOCK_LENGTH, uint16_t{22});
}

TEST(OffsetMessages, FetchResponseRoundTrip) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchResponseMsg::encode(buf.data(), 0,
                                    /*offset=*/9999LL,
                                    /*commitTimestampNs=*/888888LL,
                                    /*errorCode=*/0,
                                    /*partition=*/2,
                                    "grp", "topic", "meta");
    auto m = OffsetFetchResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.offset,           9999LL);
    EXPECT_EQ(m.commitTimestampNs, 888888LL);
    EXPECT_EQ(m.errorCode,        0);
    EXPECT_EQ(m.partition,        2);
    EXPECT_EQ(m.group,            "grp");
    EXPECT_EQ(m.topic,            "topic");
    EXPECT_EQ(m.metadata,         "meta");
}

TEST(OffsetMessages, FetchResponseNotFoundSentinel) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchResponseMsg::encode(buf.data(), 0, -1LL, 0LL, 1, 0, "g", "t", "");
    auto m = OffsetFetchResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.offset,    -1LL);
    EXPECT_EQ(m.errorCode, 1);
}

TEST(OffsetMessages, FetchResponseEmptyMetadata) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchResponseMsg::encode(buf.data(), 0, 0LL, 0LL, 0, 0, "g", "t", "");
    auto m = OffsetFetchResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.metadata, "");
}

TEST(OffsetMessages, FetchResponseHeaderFields) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchResponseMsg::encode(buf.data(), 0, 0LL, 0LL, 0, 0, "g", "t", "m");
    auto h = OffsetMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{4});
    EXPECT_EQ(h.schemaId,    uint16_t{113});
    EXPECT_EQ(h.version,     uint16_t{1});
    EXPECT_EQ(h.blockLength, uint16_t{22});
}

TEST(OffsetMessages, FetchResponseLongMetadata) {
    std::array<uint8_t, kBufSize> buf{};
    std::string longMeta(800, 'Z');
    OffsetFetchResponseMsg::encode(buf.data(), 0, 1LL, 2LL, 0, 5, "grp", "top", longMeta);
    auto m = OffsetFetchResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.metadata,  longMeta);
    EXPECT_EQ(m.offset,    1LL);
    EXPECT_EQ(m.partition, 5);
}

// Byte-exact: offset at bytes [8..15] in FetchResponse.
TEST(OffsetMessages, FetchResponseOffsetLittleEndian) {
    std::array<uint8_t, kBufSize> buf{};
    int64_t testOff = 0xDEADBEEFCAFEBABELL;
    OffsetFetchResponseMsg::encode(buf.data(), 0, testOff, 0LL, 0, 0, "g", "t", "");
    uint8_t expected[8];
    std::memcpy(expected, &testOff, 8);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(buf[8 + i], expected[i]) << "offset byte " << (8 + i);
    }
}

// Byte-exact: commitTimestampNs at bytes [16..23].
TEST(OffsetMessages, FetchResponseTimestampLittleEndian) {
    std::array<uint8_t, kBufSize> buf{};
    int64_t testTs = 0x0FEDCBA987654321LL;
    OffsetFetchResponseMsg::encode(buf.data(), 0, 0LL, testTs, 0, 0, "g", "t", "");
    uint8_t expected[8];
    std::memcpy(expected, &testTs, 8);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(buf[16 + i], expected[i]) << "timestamp byte " << (16 + i);
    }
}

TEST(OffsetMessages, FetchResponseNonZeroBufOffset) {
    std::array<uint8_t, kBufSize> buf{};
    OffsetFetchResponseMsg::encode(buf.data(), 200, 42LL, 99LL, 0, 1, "grp", "t", "m");
    auto m = OffsetFetchResponseMsg::decode(buf.data(), 200);
    EXPECT_EQ(m.offset,            42LL);
    EXPECT_EQ(m.commitTimestampNs, 99LL);
    EXPECT_EQ(m.group,             "grp");
}

// ===========================================================================
// Cross-message: peekOffsetTemplateId dispatches correctly
// ===========================================================================

TEST(OffsetMessages, PeekTemplateIdForAllMessages) {
    std::array<uint8_t, kBufSize> buf{};

    OffsetCommitRequestMsg::encode(buf.data(), 0, 0, 0, "g", "t", "");
    EXPECT_EQ(peekOffsetTemplateId(buf.data(), 0), uint16_t{1});

    OffsetCommitResponseMsg::encode(buf.data(), 0, 0, 0, "g", "t");
    EXPECT_EQ(peekOffsetTemplateId(buf.data(), 0), uint16_t{2});

    OffsetFetchRequestMsg::encode(buf.data(), 0, 0, "g", "t");
    EXPECT_EQ(peekOffsetTemplateId(buf.data(), 0), uint16_t{3});

    OffsetFetchResponseMsg::encode(buf.data(), 0, 0, 0, 0, 0, "g", "t", "");
    EXPECT_EQ(peekOffsetTemplateId(buf.data(), 0), uint16_t{4});
}
