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
#include <cstring>
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

TEST(ReplicationMessages, SchemaVersionIs3) {
    // DR-BUG-5: 1→2 (topicId); KIP-320/595: 2→3 (FetchRequest.lastFetchedEpoch,
    // FetchResponse.divergingEpoch / divergingEndOffset).
    EXPECT_EQ(kSchemaVersion, 3);
}

TEST(ReplicationMessages, FetchRequestTemplateId) {
    EXPECT_EQ(FetchRequestMsg::TEMPLATE_ID, 1);
}

TEST(ReplicationMessages, FetchResponseTemplateId) {
    EXPECT_EQ(FetchResponseMsg::TEMPLATE_ID, 2);
}

TEST(ReplicationMessages, FetchRequestBlockLength) {
    // v3: 3×int64 + 5×int32 = 24 + 20 = 44 (lastFetchedEpoch + KIP-903 incarnation; v2 was 32)
    EXPECT_EQ(FetchRequestMsg::BLOCK_LENGTH, 44u);
    EXPECT_EQ(FetchRequestMsg::BLOCK_LENGTH_V2, 32u);
}

TEST(ReplicationMessages, FetchResponseBlockLengthFixed) {
    // SBE tight-packs the block (no alignment pad): int64+int32+int64+int64+int16+int32
    // = 8+4+8+8+2+4 = 34.  (The pre-SBE hand-rolled layout inserted a 2-byte pad after
    // errorCode → 36; SBE owns sizing now, and both nodes run the same generated codec.)
    // Unchanged in v3 (the DivergingEpoch answer rides in an error frame's payload).
    EXPECT_EQ(FetchResponseMsg::BLOCK_LENGTH_FIXED, 34u);
}

TEST(ReplicationMessages, FetchRequestTotalSize) {
    EXPECT_EQ(FetchRequestMsg::TOTAL_SIZE,
              ReplicationMessageHeader::ENCODED_SIZE + FetchRequestMsg::BLOCK_LENGTH);
    // v3: 8 (header) + 44 (block) = 52; a v2 frame (40) is still decodable.
    EXPECT_EQ(FetchRequestMsg::TOTAL_SIZE, 52u);
    EXPECT_EQ(FetchRequestMsg::MIN_SIZE, 40u);
}

// KIP-320/595 wire compatibility: a v3 FetchRequest carries lastFetchedEpoch;
// a v2 frame (32-byte block) decodes as lastFetchedEpoch = -1 (no validation).
TEST(ReplicationMessages, FetchRequestLastFetchedEpochV3RoundTripAndV2Absent) {
    uint8_t buf[64]{};
    ASSERT_EQ(FetchRequestMsg::encode(buf, 0, 4, 1, 2, 77, 1 << 20, 9, /*lastFetchedEpoch=*/3), 52u);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.fetchOffset, 77);
    EXPECT_EQ(m.topicId, 9);
    EXPECT_EQ(m.lastFetchedEpoch, 3);
    // A v2 sender: blockLength 32 in the header, no field after topicId.
    uint8_t v2[64]{};
    (void)FetchRequestMsg::encode(v2, 0, 4, 1, 2, 77, 1 << 20, 9, 3);
    v2[0] = 32; v2[1] = 0;                            // header blockLength = 32
    v2[6] = 2;  v2[7] = 0;                            // header version = 2
    std::memset(v2 + 8 + 32, 0x5A, 4);                // garbage past the v2 block
    m = FetchRequestMsg::decode(v2, 0);
    EXPECT_EQ(m.fetchOffset, 77);
    EXPECT_EQ(m.lastFetchedEpoch, kLastFetchedEpochV2Frame) << "a v2 frame is marked (fail closed)";
}

// KIP-320/595 (fix round 1): data frames keep their 46-byte framing; the
// DivergingEpoch / OffsetOutOfRange answer is a 12-byte payload of a record-less
// error frame.  A v2 frame is refused (fail closed): decoded as
// kErrUnsupportedWireVersion with no records.
TEST(ReplicationMessages, FetchResponseErrorPayloadV3AndV2FailClosed) {
    EXPECT_EQ(FetchResponseMsg::encodedSize(0), 46u) << "data-frame framing must stay 46 B";
    uint8_t buf[256]{};
    ErrorPayloadBuf epb;
    const std::size_t n = FetchResponseMsg::encode(buf, 0, 4, 1, 10, 0, static_cast<int16_t>(-7),
                                                   epb.set(2, 33), 9);
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0, n);
    EXPECT_EQ(m.errorCode, -7);
    ErrorPayload ep{};
    ASSERT_TRUE(ErrorPayload::decode(m.records, ep));
    EXPECT_EQ(ep.epoch, 2);
    EXPECT_EQ(ep.endOffset, 33);
    // The same frame stamped as a v2 sender's: refused.
    buf[6] = 2; buf[7] = 0;
    m = FetchResponseMsg::decode(buf, 0, n);
    EXPECT_EQ(m.errorCode, kErrUnsupportedWireVersion);
    EXPECT_TRUE(m.records.empty());
}

// AlterIsr (template 7) round-trip.
TEST(ReplicationMessages, AlterIsrRoundTrip) {
    AlterIsrMsg a{};
    a.topicId = 3; a.partition = 2; a.leaderId = 1; a.leaderEpoch = 9; a.isrCount = 2;
    a.isr[0] = 1; a.isr[1] = 3;
    a.partitionEpoch = 17;
    EXPECT_EQ(AlterIsrMsg::TOTAL_SIZE, 8u + 92u);
    uint8_t buf[AlterIsrMsg::TOTAL_SIZE]{};
    ASSERT_EQ(a.encode(buf, sizeof buf), AlterIsrMsg::TOTAL_SIZE);
    EXPECT_EQ(peekTemplateId(buf), AlterIsrMsg::TEMPLATE_ID);
    AlterIsrMsg b{};
    ASSERT_TRUE(AlterIsrMsg::decode(buf, sizeof buf, b));
    EXPECT_EQ(b.topicId, 3); EXPECT_EQ(b.partition, 2); EXPECT_EQ(b.leaderId, 1);
    EXPECT_EQ(b.leaderEpoch, 9); EXPECT_EQ(b.isrCount, 2); EXPECT_EQ(b.isr[1], 3);
    EXPECT_EQ(b.partitionEpoch, 17);
}

// AlterIsrResponse / InternalIsr / InternalIsrAck (templates 8-10) round-trips.
TEST(ReplicationMessages, AlterIsrResponseAndInternalIsrRoundTrip) {
    AlterIsrResponseMsg r{};
    r.topicId = 3; r.partition = 2; r.leaderId = 1; r.leaderEpoch = 9; r.partitionEpoch = 12;
    r.errorCode = kAlterIsrIneligibleReplica;
    uint8_t rb[AlterIsrResponseMsg::TOTAL_SIZE]{};
    ASSERT_EQ(r.encode(rb, sizeof rb), AlterIsrResponseMsg::TOTAL_SIZE);
    EXPECT_EQ(peekTemplateId(rb), AlterIsrResponseMsg::TEMPLATE_ID);
    AlterIsrResponseMsg r2{};
    ASSERT_TRUE(AlterIsrResponseMsg::decode(rb, sizeof rb, r2));
    EXPECT_EQ(r2.leaderEpoch, 9); EXPECT_EQ(r2.partitionEpoch, 12);
    EXPECT_EQ(r2.errorCode, kAlterIsrIneligibleReplica);

    InternalIsrMsg i{};
    i.topicId = -3; i.partition = 0; i.leaderId = 2; i.leaderEpoch = 7; i.version = 41;
    i.isrCount = 2; i.isr[0] = 2; i.isr[1] = 3;
    uint8_t ib[InternalIsrMsg::TOTAL_SIZE]{};
    ASSERT_EQ(i.encode(ib, sizeof ib), InternalIsrMsg::TOTAL_SIZE);
    InternalIsrMsg i2{};
    ASSERT_TRUE(InternalIsrMsg::decode(ib, sizeof ib, i2));
    EXPECT_EQ(i2.topicId, -3); EXPECT_EQ(i2.version, 41); EXPECT_EQ(i2.isrCount, 2); EXPECT_EQ(i2.isr[1], 3);

    InternalIsrAckMsg k{};
    k.topicId = -3; k.partition = 0; k.followerId = 3; k.leaderEpoch = 7; k.version = 41;
    uint8_t kb[InternalIsrAckMsg::TOTAL_SIZE]{};
    ASSERT_EQ(k.encode(kb, sizeof kb), InternalIsrAckMsg::TOTAL_SIZE);
    InternalIsrAckMsg k2{};
    ASSERT_TRUE(InternalIsrAckMsg::decode(kb, sizeof kb, k2));
    EXPECT_EQ(k2.followerId, 3); EXPECT_EQ(k2.version, 41); EXPECT_EQ(k2.leaderEpoch, 7);
}

// KIP-903: the FetchRequest carries the follower's process incarnation.
TEST(ReplicationMessages, FetchRequestCarriesIncarnation) {
    uint8_t buf[FetchRequestMsg::TOTAL_SIZE]{};
    ASSERT_EQ(FetchRequestMsg::encode(buf, 0, 4, 1, 2, 99, 1024, 7, 3, 0x1234567890LL),
              FetchRequestMsg::TOTAL_SIZE);
    const auto m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.lastFetchedEpoch, 3);
    EXPECT_EQ(m.incarnation, 0x1234567890LL);
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
    // SBE: header(8) + block(34) + len_prefix(4) + 0 records = 46
    EXPECT_EQ(written, 46u);

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
    // Corrupt the int32 length prefix at offset kFixed = Header(8) + BLOCK_LENGTH_FIXED(36) = 44.
    // DR-BUG-5: BLOCK_LENGTH_FIXED grew from 30→36; kLenOff is now 44 (was 38).
    constexpr std::size_t kLenOff =
        ReplicationMessageHeader::ENCODED_SIZE + FetchResponseMsg::BLOCK_LENGTH_FIXED;
    // Write -1 as little-endian int32.
    buf[kLenOff + 0] = 0xFF; buf[kLenOff + 1] = 0xFF;
    buf[kLenOff + 2] = 0xFF; buf[kLenOff + 3] = 0xFF;

    // Pass fragmentSize = kLenOff + 4 = 48 (header + fixed + length prefix, no records body).
    FetchResponseMsg m = FetchResponseMsg::decode(buf.data(), 0,
        ReplicationMessageHeader::ENCODED_SIZE + FetchResponseMsg::BLOCK_LENGTH_FIXED + FetchResponseMsg::VAR_HEADER_SIZE);
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

// ============================================================================
// DR-BUG-5: topicId round-trip tests
// These tests verify that the topicId field is correctly encoded and decoded
// for both FetchRequest and FetchResponse (added in schema version 2).
// ============================================================================

TEST(FetchRequest, TopicIdRoundTrip) {
    uint8_t buf[kBufSize]{};
    // Encode with a non-zero topicId.
    FetchRequestMsg::encode(buf, 0,
        /*leaderEpoch=*/1LL, /*partition=*/0,
        /*followerId=*/2, /*fetchOffset=*/100LL,
        /*maxBytes=*/4096, /*topicId=*/7);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.topicId, 7);
    // Other fields must be unaffected.
    EXPECT_EQ(m.leaderEpoch, 1LL);
    EXPECT_EQ(m.partition,   0);
    EXPECT_EQ(m.followerId,  2);
    EXPECT_EQ(m.fetchOffset, 100LL);
    EXPECT_EQ(m.maxBytes,    4096);
}

TEST(FetchRequest, TopicIdDefaultIsZero) {
    uint8_t buf[kBufSize]{};
    // Call without topicId (default parameter = 0).
    FetchRequestMsg::encode(buf, 0, 5LL, 3, 1, 50LL, 512);
    FetchRequestMsg m = FetchRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.topicId, 0) << "default topicId must be 0";
}

TEST(FetchRequest, TopicIdDistinguishesTwoTopics) {
    // Encode two different topicIds for the same partition — ensure decode gives different values.
    uint8_t buf0[kBufSize]{};
    uint8_t buf1[kBufSize]{};
    FetchRequestMsg::encode(buf0, 0, 1LL, 0, 1, 0LL, 0, /*topicId=*/0);
    FetchRequestMsg::encode(buf1, 0, 1LL, 0, 1, 0LL, 0, /*topicId=*/1);
    auto m0 = FetchRequestMsg::decode(buf0, 0);
    auto m1 = FetchRequestMsg::decode(buf1, 0);
    EXPECT_EQ(m0.partition, m1.partition) << "same partition index";
    EXPECT_NE(m0.topicId,   m1.topicId)   << "different topicId must produce different decoded value";
    EXPECT_EQ(m0.topicId, 0);
    EXPECT_EQ(m1.topicId, 1);
}

TEST(FetchResponse, TopicIdRoundTrip) {
    uint8_t buf[kBufSize]{};
    std::vector<uint8_t> payload = {0x01, 0x02};
    FetchResponseMsg::encode(buf, 0,
        /*leaderEpoch=*/2LL, /*partition=*/0,
        /*highWatermark=*/500LL, /*logStartOffset=*/0LL,
        /*errorCode=*/0,
        std::span<const uint8_t>{payload},
        /*topicId=*/42);
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.topicId, 42);
    EXPECT_EQ(m.leaderEpoch,   2LL);
    EXPECT_EQ(m.partition,     0);
    EXPECT_EQ(m.highWatermark, 500LL);
    EXPECT_EQ(m.records.size(), 2u);
}

TEST(FetchResponse, TopicIdDefaultIsZero) {
    uint8_t buf[kBufSize]{};
    // Call without topicId (default parameter = 0).
    FetchResponseMsg::encode(buf, 0, 1LL, 0, 0LL, 0LL, 0, {});
    FetchResponseMsg m = FetchResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.topicId, 0) << "default topicId must be 0";
}

TEST(FetchResponse, TopicIdDistinguishesTwoTopics) {
    uint8_t buf0[kBufSize]{};
    uint8_t buf1[kBufSize]{};
    FetchResponseMsg::encode(buf0, 0, 1LL, 0, 0LL, 0LL, 0, {}, /*topicId=*/0);
    FetchResponseMsg::encode(buf1, 0, 1LL, 0, 0LL, 0LL, 0, {}, /*topicId=*/99);
    auto m0 = FetchResponseMsg::decode(buf0, 0);
    auto m1 = FetchResponseMsg::decode(buf1, 0);
    EXPECT_NE(m0.topicId, m1.topicId);
    EXPECT_EQ(m0.topicId, 0);
    EXPECT_EQ(m1.topicId, 99);
}

// ============================================================================
// DR-BUG-6: OffsetsForLeaderEpoch request/response round-trip tests (KIP-101).
// ============================================================================

TEST(OffsetsForLeaderEpoch, RequestTemplateId) {
    EXPECT_EQ(OffsetsForLeaderEpochReqMsg::TEMPLATE_ID, 3);
}

TEST(OffsetsForLeaderEpoch, ResponseTemplateId) {
    EXPECT_EQ(OffsetsForLeaderEpochRespMsg::TEMPLATE_ID, 4);
}

TEST(OffsetsForLeaderEpoch, RequestRoundTrip) {
    uint8_t buf[kBufSize]{};
    std::size_t written = OffsetsForLeaderEpochReqMsg::encode(
        buf, 0,
        /*currentEpoch=*/9LL, /*partition=*/4,
        /*followerId=*/2, /*requestedEpoch=*/7LL,
        /*followerLeo=*/12345LL, /*topicId=*/11);
    EXPECT_EQ(written, OffsetsForLeaderEpochReqMsg::TOTAL_SIZE);

    auto m = OffsetsForLeaderEpochReqMsg::decode(buf, 0);
    EXPECT_EQ(m.currentEpoch,   9LL);
    EXPECT_EQ(m.partition,      4);
    EXPECT_EQ(m.followerId,     2);
    EXPECT_EQ(m.requestedEpoch, 7LL);
    EXPECT_EQ(m.followerLeo,    12345LL);
    EXPECT_EQ(m.topicId,        11);
}

TEST(OffsetsForLeaderEpoch, RequestDistinctSentinels) {
    uint8_t buf[kBufSize]{};
    OffsetsForLeaderEpochReqMsg::encode(buf, 0,
        0x0102030405060708LL, 0x0A0B0C0D, 0x1E1F2021,
        0x2122232425262728LL, 0x3132333435363738LL, 0x41424344);
    auto m = OffsetsForLeaderEpochReqMsg::decode(buf, 0);
    EXPECT_EQ(m.currentEpoch,   0x0102030405060708LL);
    EXPECT_EQ(m.partition,      0x0A0B0C0D);
    EXPECT_EQ(m.followerId,     0x1E1F2021);
    EXPECT_EQ(m.requestedEpoch, 0x2122232425262728LL);
    EXPECT_EQ(m.followerLeo,    0x3132333435363738LL);
    EXPECT_EQ(m.topicId,        0x41424344);
}

TEST(OffsetsForLeaderEpoch, ResponseRoundTrip) {
    uint8_t buf[kBufSize]{};
    std::size_t written = OffsetsForLeaderEpochRespMsg::encode(
        buf, 0,
        /*leaderEpoch=*/9LL, /*partition=*/4,
        /*responseEpoch=*/7LL, /*endOffset=*/250LL,
        /*errorCode=*/0, /*topicId=*/11);
    EXPECT_EQ(written, OffsetsForLeaderEpochRespMsg::TOTAL_SIZE);

    auto m = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
    EXPECT_EQ(m.leaderEpoch,   9LL);
    EXPECT_EQ(m.partition,     4);
    EXPECT_EQ(m.responseEpoch, 7LL);
    EXPECT_EQ(m.endOffset,     250LL);
    EXPECT_EQ(m.errorCode,     0);
    EXPECT_EQ(m.topicId,       11);
}

TEST(OffsetsForLeaderEpoch, ResponseNegativeResponseEpoch) {
    // responseEpoch = -1 means "no shared epoch" (divergent).
    uint8_t buf[kBufSize]{};
    OffsetsForLeaderEpochRespMsg::encode(buf, 0, 3LL, 0, -1LL, 0LL, 0, 0);
    auto m = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
    EXPECT_EQ(m.responseEpoch, -1LL);
    EXPECT_EQ(m.endOffset,     0LL);
}

TEST(OffsetsForLeaderEpoch, ResponseErrorCode) {
    uint8_t buf[kBufSize]{};
    OffsetsForLeaderEpochRespMsg::encode(buf, 0, 3LL, 0, 2LL, 100LL, -2, 0);
    auto m = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, -2);
}

TEST(OffsetsForLeaderEpoch, PeekDistinguishesFromFetch) {
    uint8_t bufReq[kBufSize]{};
    uint8_t bufResp[kBufSize]{};
    uint8_t bufFetch[kBufSize]{};
    OffsetsForLeaderEpochReqMsg::encode(bufReq, 0, 1LL, 0, 0, 0LL, 0LL, 0);
    OffsetsForLeaderEpochRespMsg::encode(bufResp, 0, 1LL, 0, 0LL, 0LL, 0, 0);
    FetchRequestMsg::encode(bufFetch, 0, 1LL, 0, 0, 0LL, 0);
    uint16_t tReq   = peekTemplateId(bufReq, 0);
    uint16_t tResp  = peekTemplateId(bufResp, 0);
    uint16_t tFetch = peekTemplateId(bufFetch, 0);
    EXPECT_EQ(tReq,  3u);
    EXPECT_EQ(tResp, 4u);
    EXPECT_NE(tReq,  tFetch);
    EXPECT_NE(tResp, tFetch);
}

TEST(OffsetsForLeaderEpoch, RequestEncodeAtNonZeroOffset) {
    uint8_t buf[kBufSize]{};
    const std::size_t OFF = 24;
    OffsetsForLeaderEpochReqMsg::encode(buf, OFF, 2LL, 1, 3, 2LL, 99LL, 5);
    auto m = OffsetsForLeaderEpochReqMsg::decode(buf, OFF);
    EXPECT_EQ(m.requestedEpoch, 2LL);
    EXPECT_EQ(m.followerLeo,    99LL);
    EXPECT_EQ(m.topicId,        5);
}

// ============================================================================
// D9 — cross-node transaction markers (templates 11 / 12).
// ============================================================================

// WriteTxnMarker round-trip at the maximum marker length; the marker bytes are a
// zero-copy view into the frame.
TEST(D9TxnMarkerWireRegression, WriteTxnMarkerRoundTripAtMaxMarkerLength) {
    std::array<uint8_t, WriteTxnMarkerMsg::kMaxMarkerBytes> marker{};
    for (std::size_t i = 0; i < marker.size(); ++i) marker[i] = static_cast<uint8_t>(0xA0 + i);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 2; m.coordinatorEpoch = 7; m.token = 0x0102030405060708LL;
    m.topicId = 5; m.partition = 3; m.producerId = 4242; m.commit = 1; m.leaderEpoch = 17;
    m.marker = std::span<const uint8_t>(marker);
    std::array<uint8_t, WriteTxnMarkerMsg::kMaxSize> buf{};
    const std::size_t n = m.encode(buf.data(), buf.size());
    ASSERT_EQ(n, WriteTxnMarkerMsg::kFixedSize + marker.size());
    EXPECT_EQ(n, WriteTxnMarkerMsg::kMaxSize);
    EXPECT_EQ(peekTemplateId(buf.data()), WriteTxnMarkerMsg::TEMPLATE_ID);
    WriteTxnMarkerMsg d{};
    ASSERT_TRUE(WriteTxnMarkerMsg::decode(buf.data(), n, d));
    EXPECT_EQ(d.coordinatorNodeId, 2); EXPECT_EQ(d.coordinatorEpoch, 7);
    EXPECT_EQ(d.token, 0x0102030405060708LL); EXPECT_EQ(d.topicId, 5); EXPECT_EQ(d.partition, 3);
    EXPECT_EQ(d.producerId, 4242); EXPECT_EQ(d.commit, 1);
    EXPECT_EQ(d.leaderEpoch, 17) << "re-review MEDIUM-3: the target leader epoch travels";
    ASSERT_EQ(d.marker.size(), marker.size());
    EXPECT_EQ(std::memcmp(d.marker.data(), marker.data(), marker.size()), 0);
    EXPECT_EQ(d.marker.data(), buf.data() + WriteTxnMarkerMsg::kFixedSize) << "zero-copy view";
}

TEST(D9TxnMarkerWireRegression, WriteTxnMarkerRejectsOversizeTruncatedAndForeignFrames) {
    std::array<uint8_t, WriteTxnMarkerMsg::kMaxMarkerBytes + 1> big{};
    WriteTxnMarkerMsg m{};
    m.marker = std::span<const uint8_t>(big);
    std::array<uint8_t, WriteTxnMarkerMsg::kMaxSize + 16> buf{};
    EXPECT_EQ(m.encode(buf.data(), buf.size()), 0u) << "a marker above the max length is not encoded";

    const std::array<uint8_t, 61> marker{};
    m.marker = std::span<const uint8_t>(marker);
    EXPECT_EQ(m.encode(buf.data(), WriteTxnMarkerMsg::kFixedSize + marker.size() - 1), 0u) << "cap too small";
    const std::size_t n = m.encode(buf.data(), buf.size());
    ASSERT_EQ(n, WriteTxnMarkerMsg::kFixedSize + marker.size());
    WriteTxnMarkerMsg d{};
    EXPECT_FALSE(WriteTxnMarkerMsg::decode(buf.data(), n - 1, d)) << "truncated marker bytes";
    EXPECT_FALSE(WriteTxnMarkerMsg::decode(buf.data(), WriteTxnMarkerMsg::kFixedSize - 1, d)) << "truncated block";
    EXPECT_FALSE(WriteTxnMarkerMsg::decode(buf.data(), 4, d)) << "truncated header";
    // A forged length above the max is refused even when the bytes are there.
    detail::write_le<uint32_t>(buf.data() + ReplicationMessageHeader::ENCODED_SIZE + 36, WriteTxnMarkerMsg::kMaxMarkerBytes + 1);
    EXPECT_FALSE(WriteTxnMarkerMsg::decode(buf.data(), buf.size(), d)) << "forged marker length";
    detail::write_le<uint32_t>(buf.data() + ReplicationMessageHeader::ENCODED_SIZE + 36, static_cast<uint32_t>(marker.size()));
    ASSERT_TRUE(WriteTxnMarkerMsg::decode(buf.data(), n, d));
    stampTemplateId(buf.data(), 0, WriteTxnMarkerResponseMsg::TEMPLATE_ID);
    EXPECT_FALSE(WriteTxnMarkerMsg::decode(buf.data(), n, d)) << "another template id";
}

TEST(D9TxnMarkerWireRegression, WriteTxnMarkerResponseRoundTripAndTruncation) {
    WriteTxnMarkerResponseMsg r{};
    r.token = -77; r.topicId = 9; r.partition = 1; r.producerId = 31337; r.coordinatorEpoch = 4;
    r.errorCode = 6; r.offset = 123456789012LL;
    std::array<uint8_t, WriteTxnMarkerResponseMsg::TOTAL_SIZE> buf{};
    ASSERT_EQ(r.encode(buf.data(), buf.size()), WriteTxnMarkerResponseMsg::TOTAL_SIZE);
    EXPECT_EQ(r.encode(buf.data(), buf.size() - 1), 0u);
    EXPECT_EQ(peekTemplateId(buf.data()), WriteTxnMarkerResponseMsg::TEMPLATE_ID);
    WriteTxnMarkerResponseMsg d{};
    ASSERT_TRUE(WriteTxnMarkerResponseMsg::decode(buf.data(), buf.size(), d));
    EXPECT_EQ(d.token, -77); EXPECT_EQ(d.topicId, 9); EXPECT_EQ(d.partition, 1);
    EXPECT_EQ(d.producerId, 31337); EXPECT_EQ(d.coordinatorEpoch, 4); EXPECT_EQ(d.errorCode, 6);
    EXPECT_EQ(d.offset, 123456789012LL);
    EXPECT_FALSE(WriteTxnMarkerResponseMsg::decode(buf.data(), buf.size() - 1, d)) << "truncated";
    stampTemplateId(buf.data(), 0, WriteTxnMarkerMsg::TEMPLATE_ID);
    EXPECT_FALSE(WriteTxnMarkerResponseMsg::decode(buf.data(), buf.size(), d)) << "another template id";
}
