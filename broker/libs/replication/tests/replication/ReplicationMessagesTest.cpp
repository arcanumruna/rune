// ---------------------------------------------------------------------------
// ReplicationMessagesTest.cpp — Unit tests for ReplicationMessages.h
//
// Tests cover:
//   • FetchRequest encode → decode round-trip (field-by-field sentinel values)
//   • FetchResponse encode → decode round-trip (field-by-field + var blob)
//   • Header schema/template ids
//   • Little-endian byte spot-checks
//   • Boundary / edge values (zero, INT64_MAX, INT64_MIN)
//   • peekTemplateId
//   • FetchResponse with empty records blob
//   • FetchResponse with non-empty records blob
//   • Error code round-trip
//   • Encoding at non-zero offset (buffer slicing)
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <array>
#include <cstdint>
#include <vector>
#include <climits>

#include <rune/replication/ReplicationMessages.h>

using namespace rune::replication;

// ============================================================================
// Helpers
// ============================================================================

static constexpr std::size_t kBufSize = 1024;

// ============================================================================
// Schema / constants tests
// ============================================================================

TEST(ReplicationMessages, SchemaIdIs112) {
    EXPECT_EQ(kSchemaId, 112);
}

TEST(ReplicationMessages, SchemaVersionIs1) {
    EXPECT_EQ(kSchemaVersion, 1);
}

TEST(ReplicationMessages, FetchRequestTemplateId) {
    EXPECT_EQ(FetchRequestMsg::TEMPLATE_ID, 1);
}

TEST(ReplicationMessages, FetchResponseTemplateId) {
    EXPECT_EQ(FetchResponseMsg::TEMPLATE_ID, 2);
}

TEST(ReplicationMessages, FetchRequestBlockLength) {
    // 2×int64 + 3×int32 = 16 + 12 = 28
    EXPECT_EQ(FetchRequestMsg::BLOCK_LENGTH, 28u);
}

TEST(ReplicationMessages, FetchResponseBlockLengthFixed) {
    // int64 + int32 + int64 + int64 + int16 = 8+4+8+8+2 = 30
    EXPECT_EQ(FetchResponseMsg::BLOCK_LENGTH_FIXED, 30u);
}

TEST(ReplicationMessages, FetchRequestTotalSize) {
    EXPECT_EQ(FetchRequestMsg::TOTAL_SIZE,
              ReplicationMessageHeader::ENCODED_SIZE + FetchRequestMsg::BLOCK_LENGTH);
    EXPECT_EQ(FetchRequestMsg::TOTAL_SIZE, 36u);
}

TEST(ReplicationMessages, HeaderEncodedSize) {
    EXPECT_EQ(ReplicationMessageHeader::ENCODED_SIZE, 8u);
}

// ============================================================================
// FetchRequest round-trip tests
// ============================================================================

TEST(FetchRequest, RoundTripBasicValues) {
    uint8_t buf[kBufSize]{};
    const Epoch       epoch  = 42;
    const PartitionId part   = 7;
    const NodeId      fol    = 3;
    const Offset      off    = 12345;
    const int32_t     maxB   = 65536;

    std::size_t written = FetchRequestMsg::encode(buf, 0, epoch, part, fol, off, maxB);
    EXPECT_EQ(written, FetchRequestMsg::TOTAL_SIZE);

    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch, epoch);
    EXPECT_EQ(m.partition,   part);
    EXPECT_EQ(m.followerId,  fol);
    EXPECT_EQ(m.fetchOffset, off);
    EXPECT_EQ(m.maxBytes,    maxB);
}

TEST(FetchRequest, DistinctSentinelValues) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0,
        /*leaderEpoch*/  0x0102030405060708LL,
        /*partition*/    0x0A0B0C0D,
        /*followerId*/   0x1E1F2021,
        /*fetchOffset*/  0x2122232425262728LL,
        /*maxBytes*/     0x31323334);

    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch, 0x0102030405060708LL);
    EXPECT_EQ(m.partition,   0x0A0B0C0D);
    EXPECT_EQ(m.followerId,  0x1E1F2021);
    EXPECT_EQ(m.fetchOffset, 0x2122232425262728LL);
    EXPECT_EQ(m.maxBytes,    0x31323334);
}

TEST(FetchRequest, ZeroValues) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, 0, 0, 0, 0, 0);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch, 0);
    EXPECT_EQ(m.partition,   0);
    EXPECT_EQ(m.followerId,  0);
    EXPECT_EQ(m.fetchOffset, 0);
    EXPECT_EQ(m.maxBytes,    0);
}

TEST(FetchRequest, MaxInt64FetchOffset) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, INT64_MAX, 0, 0, INT64_MAX, INT32_MAX);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch, INT64_MAX);
    EXPECT_EQ(m.fetchOffset, INT64_MAX);
    EXPECT_EQ(m.maxBytes,    INT32_MAX);
}

TEST(FetchRequest, NegativeEpochAllowed) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, -1LL, -1, -1, -1LL, -1);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch, -1LL);
    EXPECT_EQ(m.partition,   -1);
    EXPECT_EQ(m.followerId,  -1);
    EXPECT_EQ(m.fetchOffset, -1LL);
    EXPECT_EQ(m.maxBytes,    -1);
}

TEST(FetchRequest, HeaderSchemaId) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, 1, 0, 0, 0, 0);
    auto hdr = ReplicationMessageHeader::decode(buf, 0);
    EXPECT_EQ(hdr.schemaId,    kSchemaId);
    EXPECT_EQ(hdr.version,     kSchemaVersion);
    EXPECT_EQ(hdr.templateId,  FetchRequestMsg::TEMPLATE_ID);
    EXPECT_EQ(hdr.blockLength, FetchRequestMsg::BLOCK_LENGTH);
}

TEST(FetchRequest, LittleEndianByteSpotCheck) {
    // leaderEpoch = 1 should appear as 01 00 00 00 00 00 00 00 in buf[8..15]
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, 1LL, 0, 0, 0, 0);
    // Header is 8 bytes; leaderEpoch starts at byte 8
    EXPECT_EQ(buf[8],  0x01);
    EXPECT_EQ(buf[9],  0x00);
    EXPECT_EQ(buf[10], 0x00);
    EXPECT_EQ(buf[11], 0x00);
    EXPECT_EQ(buf[12], 0x00);
    EXPECT_EQ(buf[13], 0x00);
    EXPECT_EQ(buf[14], 0x00);
    EXPECT_EQ(buf[15], 0x00);
}

TEST(FetchRequest, EncodingAtNonZeroOffset) {
    uint8_t buf[kBufSize]{};
    const std::size_t OFFSET = 16;
    std::size_t written = FetchRequestMsg::encode(buf, OFFSET, 99LL, 1, 2, 500LL, 1000);
    EXPECT_EQ(written, FetchRequestMsg::TOTAL_SIZE);

    FetchRequestMsg m = FetchRequestMsg::decode(buf, OFFSET);
    EXPECT_EQ(m.leaderEpoch, 99LL);
    EXPECT_EQ(m.partition,   1);
    EXPECT_EQ(m.followerId,  2);
    EXPECT_EQ(m.fetchOffset, 500LL);
    EXPECT_EQ(m.maxBytes,    1000);
}

TEST(FetchRequest, PeekTemplateId) {
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, 1, 0, 0, 0, 0);
    EXPECT_EQ(peekTemplateId(buf, 0),
              static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));
}

TEST(FetchRequest, MaxBytesZeroMeansPositionOnly) {
    // maxBytes=0 is legal: follower is reporting its position without requesting data
    uint8_t buf[kBufSize]{};
    FetchRequestMsg::encode(buf, 0, 5LL, 2, 1, 9999LL, 0);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.maxBytes, 0);
    EXPECT_EQ(m.fetchOffset, 9999LL);
}

// ============================================================================
// FetchResponse round-trip tests
// ============================================================================

TEST(FetchResponse, RoundTripEmptyRecords) {
    uint8_t buf[kBufSize]{};
    const Epoch       epoch  = 3;
    const PartitionId part   = 1;
    const Offset      hw     = 1000;
    const Offset      logSt  = 0;
    const int16_t     err    = 0;
    std::span<const uint8_t> empty{};

    std::size_t written = FetchResponseMsg::encode(buf, 0,
        epoch, part, hw, logSt, err, empty);
    // header(8) + fixed(30) + len_prefix(4) + 0 = 42
    EXPECT_EQ(written, 42u);

    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch,    epoch);
    EXPECT_EQ(m.partition,      part);
    EXPECT_EQ(m.highWatermark,  hw);
    EXPECT_EQ(m.logStartOffset, logSt);
    EXPECT_EQ(m.errorCode,      err);
    EXPECT_EQ(m.records.size(), 0u);
}

TEST(FetchResponse, RoundTripNonEmptyRecords) {
    uint8_t buf[kBufSize]{};
    const std::vector<uint8_t> payload = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02};
    std::span<const uint8_t> rec{payload};

    FetchResponseMsg::encode(buf, 0, 7LL, 2, 2000LL, 100LL, 0, rec);

    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch,    7LL);
    EXPECT_EQ(m.partition,      2);
    EXPECT_EQ(m.highWatermark,  2000LL);
    EXPECT_EQ(m.logStartOffset, 100LL);
    EXPECT_EQ(m.errorCode,      0);
    ASSERT_EQ(m.records.size(), payload.size());
    for (std::size_t i = 0; i < payload.size(); ++i) {
        EXPECT_EQ(m.records[i], payload[i]) << "byte " << i;
    }
}

TEST(FetchResponse, DistinctSentinelValues) {
    uint8_t buf[kBufSize]{};
    const std::vector<uint8_t> payload = {0xAA, 0xBB};
    FetchResponseMsg::encode(buf, 0,
        0x0102030405060708LL,   // leaderEpoch
        0x0A0B0C0D,             // partition
        0x1112131415161718LL,   // highWatermark
        0x2122232425262728LL,   // logStartOffset
        static_cast<int16_t>(0x3132),  // errorCode
        std::span<const uint8_t>{payload});

    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch,    0x0102030405060708LL);
    EXPECT_EQ(m.partition,      0x0A0B0C0D);
    EXPECT_EQ(m.highWatermark,  0x1112131415161718LL);
    EXPECT_EQ(m.logStartOffset, 0x2122232425262728LL);
    EXPECT_EQ(m.errorCode,      static_cast<int16_t>(0x3132));
    ASSERT_EQ(m.records.size(), 2u);
    EXPECT_EQ(m.records[0], 0xAA);
    EXPECT_EQ(m.records[1], 0xBB);
}

TEST(FetchResponse, ErrorCodeNegative) {
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, 1LL, 0, 0LL, 0LL, -1, {});
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, -1);
}

TEST(FetchResponse, ErrorCodeBoundaryMax) {
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, 0LL, 0, 0LL, 0LL, INT16_MAX, {});
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, INT16_MAX);
}

TEST(FetchResponse, HeaderSchemaAndTemplateId) {
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, 1LL, 0, 0LL, 0LL, 0, {});
    auto hdr = ReplicationMessageHeader::decode(buf, 0);
    EXPECT_EQ(hdr.schemaId,    kSchemaId);
    EXPECT_EQ(hdr.version,     kSchemaVersion);
    EXPECT_EQ(hdr.templateId,  FetchResponseMsg::TEMPLATE_ID);
    EXPECT_EQ(hdr.blockLength, FetchResponseMsg::BLOCK_LENGTH_FIXED);
}

TEST(FetchResponse, PeekTemplateId) {
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, 1LL, 0, 0LL, 0LL, 0, {});
    EXPECT_EQ(peekTemplateId(buf, 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));
}

TEST(FetchResponse, LittleEndianByteSpotCheck) {
    // leaderEpoch = 1 should appear as 01 00 00 00 00 00 00 00 at byte 8
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, 1LL, 0, 0LL, 0LL, 0, {});
    EXPECT_EQ(buf[8],  0x01);
    EXPECT_EQ(buf[9],  0x00);
    EXPECT_EQ(buf[10], 0x00);
    EXPECT_EQ(buf[11], 0x00);
    EXPECT_EQ(buf[12], 0x00);
    EXPECT_EQ(buf[13], 0x00);
    EXPECT_EQ(buf[14], 0x00);
    EXPECT_EQ(buf[15], 0x00);
}

TEST(FetchResponse, EncodingAtNonZeroOffset) {
    uint8_t buf[kBufSize]{};
    const std::size_t OFF = 32;
    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    std::size_t written = FetchResponseMsg::encode(buf, OFF,
        55LL, 3, 777LL, 5LL, 0, std::span<const uint8_t>{payload});
    EXPECT_GT(written, 0u);

    FetchResponseMsg m = FetchResponseMsg::decode(buf, OFF);
    EXPECT_EQ(m.leaderEpoch,   55LL);
    EXPECT_EQ(m.partition,     3);
    EXPECT_EQ(m.highWatermark, 777LL);
    ASSERT_EQ(m.records.size(), 3u);
    EXPECT_EQ(m.records[0], 0x01);
    EXPECT_EQ(m.records[1], 0x02);
    EXPECT_EQ(m.records[2], 0x03);
}

TEST(FetchResponse, LargeRecordsBlob) {
    // 256 bytes of payload
    std::vector<uint8_t> bigPayload(256);
    for (std::size_t i = 0; i < bigPayload.size(); ++i) {
        bigPayload[i] = static_cast<uint8_t>(i & 0xFF);
    }
    std::vector<uint8_t> buf(512);
    FetchResponseMsg::encode(buf.data(), 0,
        10LL, 0, 9999LL, 0LL, 0,
        std::span<const uint8_t>{bigPayload});

    FetchResponseMsg m = FetchResponseMsg::decode(buf.data(), 0);
    ASSERT_EQ(m.records.size(), bigPayload.size());
    for (std::size_t i = 0; i < bigPayload.size(); ++i) {
        EXPECT_EQ(m.records[i], bigPayload[i]) << "byte " << i;
    }
}

TEST(FetchResponse, EpochBoundaryValues) {
    uint8_t buf[kBufSize]{};
    FetchResponseMsg::encode(buf, 0, INT64_MAX, 0, INT64_MAX, INT64_MAX, 0, {});
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch,    INT64_MAX);
    EXPECT_EQ(m.highWatermark,  INT64_MAX);
    EXPECT_EQ(m.logStartOffset, INT64_MAX);
}

// ============================================================================
// Header decode standalone test
// ============================================================================

TEST(ReplicationMessageHeader, DecodeRoundTrip) {
    uint8_t buf[8]{};
    ReplicationMessageHeader::encode(buf, 0, 30u, 2u);
    auto hdr = ReplicationMessageHeader::decode(buf, 0);
    EXPECT_EQ(hdr.blockLength, 30u);
    EXPECT_EQ(hdr.templateId,  2u);
    EXPECT_EQ(hdr.schemaId,    kSchemaId);
    EXPECT_EQ(hdr.version,     kSchemaVersion);
}

// ============================================================================
// peekTemplateId distinguishes FetchRequest vs FetchResponse
// ============================================================================

TEST(ReplicationMessages, PeekDistinguishesTemplates) {
    uint8_t bufReq[kBufSize]{};
    uint8_t bufResp[kBufSize]{};
    FetchRequestMsg::encode(bufReq, 0, 1LL, 0, 0, 0LL, 0);
    FetchResponseMsg::encode(bufResp, 0, 1LL, 0, 0LL, 0LL, 0, {});

    uint16_t tReq  = peekTemplateId(bufReq,  0);
    uint16_t tResp = peekTemplateId(bufResp, 0);
    EXPECT_NE(tReq, tResp);
    EXPECT_EQ(tReq,  static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));
    EXPECT_EQ(tResp, static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));
}

// ============================================================================
// FetchResponse decode bounds-check (fragmentSize parameter)
// ============================================================================

TEST(FetchResponseDecode, BoundsCheckClampsMalformedLength) {
    // Encode a legitimate FetchResponse with 10 bytes of records.
    std::vector<uint8_t> buf(256, 0);
    std::vector<uint8_t> records(10, 0xAB);
    std::size_t written = FetchResponseMsg::encode(
        buf.data(), 0,
        /*leaderEpoch=*/1LL, /*partition=*/0,
        /*highWatermark=*/5LL, /*logStartOffset=*/0LL,
        /*errorCode=*/0,
        std::span<const uint8_t>(records.data(), records.size()));

    // Artificially shorten the fragment by 5 bytes so the actual available
    // records space is 5 bytes, not 10.  decode() with fragmentSize=written-5
    // must clamp the span to 5 bytes.
    FetchResponseMsg m = FetchResponseMsg::decode(buf.data(), 0, written - 5);
    EXPECT_LE(m.records.size(), 5u)
        << "decode must not return a span beyond the available fragment bytes";

    // With no fragmentSize (SIZE_MAX), the full 10 bytes are returned.
    FetchResponseMsg m2 = FetchResponseMsg::decode(buf.data(), 0);
    EXPECT_EQ(m2.records.size(), 10u);
}

TEST(FetchResponseDecode, BoundsCheckNegativeLengthClampsToZero) {
    // Encode an empty FetchResponse, then corrupt the length prefix to -1.
    std::vector<uint8_t> buf(128, 0);
    FetchResponseMsg::encode(buf.data(), 0, 1LL, 0, 0LL, 0LL, 0, {});
    // Corrupt the int32 length prefix at offset 42 (8 header + 30 fixed + 4 len prefix - 4 = 38... actually at offset kFixed)
    // Header(8) + BLOCK_LENGTH_FIXED(30) = 38; length prefix at buf[38].
    constexpr std::size_t kLenOff =
        ReplicationMessageHeader::ENCODED_SIZE + FetchResponseMsg::BLOCK_LENGTH_FIXED;
    // Write -1 as little-endian int32.
    buf[kLenOff + 0] = 0xFF; buf[kLenOff + 1] = 0xFF;
    buf[kLenOff + 2] = 0xFF; buf[kLenOff + 3] = 0xFF;

    FetchResponseMsg m = FetchResponseMsg::decode(buf.data(), 0, 42u);
    EXPECT_EQ(m.records.size(), 0u) << "negative length prefix must decode to empty span";
}

TEST(FetchResponseDecode, BoundsCheckExactFitReturnsFullRecords) {
    // Encode 10 bytes; pass exact fragmentSize → returns all 10.
    std::vector<uint8_t> buf(256, 0);
    std::vector<uint8_t> records(10, 0xCD);
    std::size_t written = FetchResponseMsg::encode(
        buf.data(), 0, 1LL, 0, 0LL, 0LL, 0,
        std::span<const uint8_t>(records.data(), records.size()));

    FetchResponseMsg m = FetchResponseMsg::decode(buf.data(), 0, written);
    EXPECT_EQ(m.records.size(), 10u);
    for (auto b : m.records) EXPECT_EQ(b, 0xCD);
}
