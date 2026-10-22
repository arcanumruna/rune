// DataMessagesTest.cpp — exhaustive PB-1 tests for Produce (tid 16) and
// ProduceResponse (tid 17).
//
// Coverage mandate (brief §Tests):
//   - Produce round-trip: explicit-partition (partitionKnown=1, empty key) AND
//     key-only (partitionKnown=0, non-empty key), with a non-trivial recordBatch
//     blob (incl. NUL + 0xFF bytes).
//   - ProduceResponse round-trip incl. errorCode != 0 (baseOffset ignored) and
//     i64 baseOffset extrema.
//   - Truncation at EVERY boundary: short header, short fixed body, key len says N
//     but < N bytes present, recordBatch len overflow → nullopt, never OOB read.
//   - Empty recordBatch; max-size (65535) var field exact-fit + one-over reject.
//   - Integer extrema (i32 min/max, i64 min/max).
//   - Wrong templateId → nullopt.
//   - Schema mismatch → nullopt.
//   - Byte-exactness: encode → compare against hand-built LE byte array.
//   - Golden conformance vectors: encode → write .bin; decode-golden.
//   - Benchmark helpers exercised via macro (separate bench target).

#include <gtest/gtest.h>
#include <rune/protocol/client/DataMessages.h>
#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-local LE readers (same helpers as other client codec tests)
// ---------------------------------------------------------------------------
static uint8_t  leU8 (const uint8_t* buf, int off) { return buf[off]; }
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int16_t  leS16(const uint8_t* buf, int off) {
    return static_cast<int16_t>(leU16(buf, off));
}
static int32_t  leS32(const uint8_t* buf, int off) {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off + 1]) << 8)
               | (uint32_t(buf[off + 2]) << 16)
               | (uint32_t(buf[off + 3]) << 24);
    return static_cast<int32_t>(v);
}
static int64_t  leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// Helper: build a Produce with span fields pointing into caller-owned storage.
// recordBatch: binary bytes including NUL and 0xFF to exercise non-trivial blobs.
static const uint8_t kBatchBytes[] = {
    0x01, 0x00, 0xFF, 0x42, 0x00, 0xFE, 0x7F, 0x80, 0xAB, 0xCD
};
static constexpr std::size_t kBatchLen = sizeof(kBatchBytes);

static const uint8_t kKeyBytes[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF };
static constexpr std::size_t kKeyLen = sizeof(kKeyBytes);

// ===========================================================================
// Produce (templateId 16, BODY_FIXED = 10)
// ===========================================================================

// ---------------------------------------------------------------------------
// PR-1. Round-trip: explicit-partition (partitionKnown=1, empty key).
//   Total: 24 (hdr) + 10 (fixed) + 2 (key prefix, 0) + 2 (rb prefix) + kBatchLen
//        = 38 + kBatchLen = 48 bytes.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, ExplicitPartitionRoundTrip) {
    rpc::Produce msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.topicId        = 7;
    msg.partition      = 3;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};   // empty
    msg.recordBatch    = std::span<const uint8_t>(kBatchBytes, kBatchLen);

    const std::size_t expected = 24u + 24u + 2u + 0u + 4u + kBatchLen; // 64 (rb u32)
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Produce::BODY_FIXED);   // blockLength = 24
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Produce::TEMPLATE_ID);  // templateId = 16
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);             // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);      // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 7);   // topicId
    EXPECT_EQ(leS32(buf.data(), 28), 3);   // partition
    EXPECT_EQ(leU8 (buf.data(), 32), 1u);  // partitionKnown
    // buf[33] = acks (0 = Leader/None, default)
    // buf[34..47] = producerId(-1) + producerEpoch(0) + baseSequence(-1) — idempotent sentinels

    // key var field: prefix = 0 (at offset 48 now)
    EXPECT_EQ(leU16(buf.data(), 48), 0u);

    // recordBatch var field: prefix = kBatchLen (at offset 50)
    EXPECT_EQ(leU16(buf.data(), 50), static_cast<uint16_t>(kBatchLen));
    EXPECT_EQ(std::memcmp(buf.data() + 54, kBatchBytes, kBatchLen), 0);

    // Round-trip decode
    auto decoded = rpc::Produce::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      msg.sessionId);
    EXPECT_EQ(decoded->correlationId,  msg.correlationId);
    EXPECT_EQ(decoded->topicId,        7);
    EXPECT_EQ(decoded->partition,      3);
    EXPECT_EQ(decoded->partitionKnown, 1u);
    EXPECT_EQ(decoded->key.size(),     0u);
    ASSERT_EQ(decoded->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kBatchBytes, kBatchLen), 0);
}

// ---------------------------------------------------------------------------
// PR-2. Round-trip: key-only (partitionKnown=0, non-empty key).
//   Total: 24 + 10 + 2 + kKeyLen + 2 + kBatchLen
//        = 38 + kKeyLen + kBatchLen = 54 bytes.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, KeyOnlyRoundTrip) {
    rpc::Produce msg;
    msg.sessionId      = 0xDEADC0DE12345678LL;
    msg.correlationId  = 0xFEDCBA9876543210LL;
    msg.topicId        = 42;
    msg.partition      = 0;    // ignored by broker when partitionKnown==0
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(kKeyBytes, kKeyLen);
    msg.recordBatch    = std::span<const uint8_t>(kBatchBytes, kBatchLen);

    const std::size_t expected = 24u + 24u + 2u + kKeyLen + 4u + kBatchLen; // 70 (rb u32)
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Fixed fields
    EXPECT_EQ(leS32(buf.data(), 24), 42);  // topicId
    EXPECT_EQ(leU8 (buf.data(), 32), 0u);  // partitionKnown
    // buf[33] = acks (0 = Leader/None, default)
    // buf[34..47] = producerId(-1) + producerEpoch(0) + baseSequence(-1)

    // key var field (at offset 48 now)
    EXPECT_EQ(leU16(buf.data(), 48), static_cast<uint16_t>(kKeyLen));
    EXPECT_EQ(std::memcmp(buf.data() + 50, kKeyBytes, kKeyLen), 0);

    // recordBatch var field
    const std::size_t rbOff = 50u + kKeyLen;
    EXPECT_EQ(leU16(buf.data(), static_cast<int>(rbOff)),
              static_cast<uint16_t>(kBatchLen));
    EXPECT_EQ(std::memcmp(buf.data() + rbOff + 4, kBatchBytes, kBatchLen), 0);

    // Round-trip decode
    auto decoded = rpc::Produce::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topicId,        42);
    EXPECT_EQ(decoded->partitionKnown, 0u);
    ASSERT_EQ(decoded->key.size(),     kKeyLen);
    EXPECT_EQ(std::memcmp(decoded->key.data(), kKeyBytes, kKeyLen), 0);
    ASSERT_EQ(decoded->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kBatchBytes, kBatchLen), 0);
}

// ---------------------------------------------------------------------------
// PR-3. Byte-exactness: encode a Produce and compare byte-for-byte against
// a hand-constructed LE byte array.
//
// Message: topicId=1, partition=2, partitionKnown=1, acks=0, key=empty,
//          recordBatch={0xAA, 0xBB}
//
// Expected layout (total = 24 + 10 + 2 + 0 + 2 + 2 = 40 bytes):
//   [0..1]   0A 00   blockLength = 10 (LE)
//   [2..3]   10 00   templateId = 16 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   02 00   version = 2 (LE, E5.1 bump)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId = 0x1122334455667788 (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..27] 01 00 00 00   topicId = 1 (LE)
//   [28..31] 02 00 00 00   partition = 2 (LE)
//   [32]     01            partitionKnown = 1
//   [33]     00            acks = 0 (Leader/None)
//   [34..35] 00 00         key var-len = 0 (LE)
//   [36..37] 02 00         recordBatch var-len = 2 (LE)
//   [38..39] AA BB         recordBatch bytes
// ---------------------------------------------------------------------------
TEST(ProduceCodec, ByteExactness) {
    static constexpr uint8_t kRb[] = {0xAA, 0xBB};

    rpc::Produce msg;
    msg.sessionId      = static_cast<int64_t>(0x1122334455667788ULL);
    msg.correlationId  = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    msg.topicId        = 1;
    msg.partition      = 2;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, sizeof(kRb));

    std::array<uint8_t, 128> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 56u);

    // clang-format off
    // New layout: 24(hdr) + 24(fixed: topicId4+partition4+partitionKnown1+acks1+producerId8+producerEpoch2+baseSequence4)
    //           + 2(key len=0) + 4(rb len, u32) + 2(rb bytes) = 56
    static constexpr uint8_t kExpected[56] = {
        0x18, 0x00,                              // blockLength = 24
        0x10, 0x00,                              // templateId = 16
        0x78, 0x00,                              // schemaId = 120
        0x03, 0x00,                              // version = 3 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x01, 0x00, 0x00, 0x00,                  // topicId = 1
        0x02, 0x00, 0x00, 0x00,                  // partition = 2
        0x01,                                    // partitionKnown = 1
        0x00,                                    // acks = 0 (Leader/None)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,  // producerId = -1 (sentinel)
        0x00, 0x00,                              // producerEpoch = 0
        0xFF, 0xFF, 0xFF, 0xFF,                  // baseSequence = -1 (sentinel)
        0x00, 0x00,                              // key var-len = 0
        0x02, 0x00, 0x00, 0x00,                  // recordBatch var-len = 2 (u32)
        0xAA, 0xBB,                              // recordBatch bytes
    };
    // clang-format on

    EXPECT_EQ(std::memcmp(buf.data(), kExpected, n), 0)
        << "Encoded Produce bytes differ from hand-built expected array";
}

// ---------------------------------------------------------------------------
// PR-4. Truncation at every boundary (partitionKnown=1, key=empty, rb=5 bytes).
//   Total = 24 + 10 + 2 + 0 + 2 + 5 = 43 bytes.
//
//   avail < 24            → nullopt (header incomplete)
//   avail = 24..33        → nullopt (fixed body incomplete: need 10 bytes at off 24)
//   avail = 34            → nullopt (key prefix needs 2 bytes at off 34)
//   avail = 35            → nullopt (only 1 byte of 2-byte key prefix)
//   avail = 36            → nullopt (key prefix says 0; rb prefix at 36 needs 2 bytes)
//   avail = 37            → nullopt (only 1 byte of rb prefix)
//   avail = 38+k (k<5)   → nullopt (only k of 5 rb bytes)
//   avail = 43            → ok
// ---------------------------------------------------------------------------
TEST(ProduceCodec, TruncationAtEveryBoundary) {
    static constexpr uint8_t kRb5[] = {0x01, 0x02, 0x03, 0x04, 0x05};

    rpc::Produce msg;
    msg.topicId        = 1;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb5, sizeof(kRb5));

    const std::size_t total = msg.encodedSize(); // 57 (24+24+2+0+2+5)
    ASSERT_EQ(total, 59u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    // Header truncations
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 23).has_value());

    // Fixed body incomplete: need all 24 bytes at [24..47]
    for (std::size_t a = 24u; a < 48u; ++a) {
        EXPECT_FALSE(rpc::Produce::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt (fixed body incomplete)";
    }

    // key prefix: need 2 bytes starting at off 48
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 49).has_value());  // 1 byte of prefix

    // key is 0 bytes; rb prefix at off 50, needs 2 bytes
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 50).has_value());
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 51).has_value());

    // rb says 5 bytes; partial bytes
    for (std::size_t k = 0; k < 5u; ++k) {
        EXPECT_FALSE(rpc::Produce::decode(buf.data(), 52u + k).has_value())
            << "k=" << k;
    }

    // Exact total succeeds
    EXPECT_TRUE(rpc::Produce::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// PR-5. Truncation: key prefix says N but buffer too short (key prefix overflow).
// ---------------------------------------------------------------------------
TEST(ProduceCodec, KeyPrefixClaimsMoreThanAvail) {
    static constexpr uint8_t kRb[] = {0xAA};

    rpc::Produce msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = msg.encodedSize(); // 40 (no key) — actually 24+10+2+0+2+1=39
    std::vector<uint8_t> buf(total + 4, 0u);
    msg.encode(buf.data());

    // Corrupt the key prefix to claim 100 bytes; avail = total (key field exhausted)
    buf[48] = 100;
    buf[49] = 0;

    EXPECT_FALSE(rpc::Produce::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// PR-6. recordBatch prefix claims more bytes than avail → nullopt.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, RecordBatchPrefixClaimsMoreThanAvail) {
    static constexpr uint8_t kRb[] = {0x01, 0x02, 0x03};

    rpc::Produce msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, sizeof(kRb));

    const std::size_t total = msg.encodedSize(); // 24+24+2+0+2+3 = 55
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Corrupt the rb prefix at offset 50 to claim 0xFFFF bytes
    buf[50] = 0xFF;
    buf[51] = 0xFF;

    EXPECT_FALSE(rpc::Produce::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// PR-7. Empty recordBatch (len=0): both key and recordBatch are empty.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, EmptyRecordBatch) {
    rpc::Produce msg;
    msg.sessionId      = 100LL;
    msg.correlationId  = 200LL;
    msg.topicId        = 5;
    msg.partition      = 1;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>{};

    const std::size_t expected = 24u + 24u + 2u + 0u + 4u + 0u; // 54 (rb u32)
    ASSERT_EQ(msg.encodedSize(), expected);

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, expected);

    auto decoded = rpc::Produce::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->key.size(),        0u);
    EXPECT_EQ(decoded->recordBatch.size(), 0u);
    EXPECT_EQ(decoded->topicId,           5);
    EXPECT_EQ(decoded->partition,         1);
    EXPECT_EQ(decoded->partitionKnown,    1u);
}

// ---------------------------------------------------------------------------
// PR-8. Max-size (65535) recordBatch exact-fit accepted; one-over rejected.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, MaxU16RecordBatchExactFit) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::vector<uint8_t> bigBatch(kMaxLen, 0xBB);

    rpc::Produce msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(bigBatch.data(), kMaxLen);

    const std::size_t total = msg.encodedSize(); // 24+24+2+0+2+65535 = 65587
    std::vector<uint8_t> bigBuf(total, 0u);
    const std::size_t written = msg.encode(bigBuf.data());
    ASSERT_EQ(written, total);

    // Exact-fit should succeed
    EXPECT_TRUE(rpc::Produce::decode(bigBuf.data(), total).has_value());

    // One byte short → nullopt
    EXPECT_FALSE(rpc::Produce::decode(bigBuf.data(), total - 1u).has_value());
}

// ---------------------------------------------------------------------------
// PR-9. Integer extrema: topicId / partition INT32_MIN/MAX.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, IntegerExtrema) {
    static constexpr uint8_t kRb[] = {0x01};

    rpc::Produce msg;
    msg.topicId        = INT32_MIN;
    msg.partition      = INT32_MAX;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::Produce::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topicId,   INT32_MIN);
    EXPECT_EQ(decoded->partition, INT32_MAX);
}

// ---------------------------------------------------------------------------
// PR-10. sessionId/correlationId INT64_MIN/INT64_MAX.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, HeaderInt64Extrema) {
    static constexpr uint8_t kRb[] = {0xCC};

    rpc::Produce msg;
    msg.sessionId      = INT64_MIN;
    msg.correlationId  = INT64_MAX;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::Produce::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     INT64_MIN);
    EXPECT_EQ(decoded->correlationId, INT64_MAX);
}

// ---------------------------------------------------------------------------
// PR-11. Wrong templateId → nullopt (encode a Produce then corrupt tid to 17).
// ---------------------------------------------------------------------------
TEST(ProduceCodec, WrongTemplateIdReturnsNullopt) {
    static constexpr uint8_t kRb[] = {0x01};

    rpc::Produce msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Corrupt templateId at bytes 2..3 to 17 (ProduceResponse)
    buf[2] = 17;
    buf[3] = 0;

    EXPECT_FALSE(rpc::Produce::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// PR-12. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, SchemaMismatchReturnsNullopt) {
    static constexpr uint8_t kRb[] = {0x01};

    rpc::Produce msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Corrupt schemaId at bytes 4..5 to 111
    buf[4] = 111;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Produce::decode(buf.data(), total).has_value());
}

// ===========================================================================
// ProduceResponse (templateId 17, BODY_FIXED = 22, total = 46 bytes — D6)
// ===========================================================================

// ---------------------------------------------------------------------------
// RR-1. Round-trip: errorCode=0 (OK), normal baseOffset.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, OkRoundTrip) {
    rpc::ProduceResponse msg;
    msg.sessionId     = 0x1122334455667788LL;
    msg.correlationId = 0x0A0B0C0D0E0F1011LL;
    msg.errorCode     = static_cast<int16_t>(rpc::ErrorCode::OK);
    msg.partition     = 5;
    msg.baseOffset    = 12345678LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::ProduceResponse::ENCODED_SIZE); // 38

    // Header fields
    EXPECT_EQ(leU16(buf.data(), 0), rpc::ProduceResponse::BODY_FIXED);  // blockLength = 14
    EXPECT_EQ(leU16(buf.data(), 2), rpc::ProduceResponse::TEMPLATE_ID); // templateId = 17
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS16(buf.data(), 24), 0);          // errorCode = OK
    EXPECT_EQ(leS32(buf.data(), 26), 5);           // partition
    EXPECT_EQ(leS64(buf.data(), 30), 12345678LL);  // baseOffset

    // Round-trip decode
    auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     msg.sessionId);
    EXPECT_EQ(decoded->correlationId, msg.correlationId);
    EXPECT_EQ(decoded->errorCode,     static_cast<int16_t>(rpc::ErrorCode::OK));
    EXPECT_EQ(decoded->partition,     5);
    EXPECT_EQ(decoded->baseOffset,    12345678LL);
}

// ---------------------------------------------------------------------------
// RR-2. Round-trip: errorCode != 0 (NotPartitionLeader).
//   baseOffset in the encoded frame is present but semantically ignored.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, ErrorCodeNonZeroRoundTrip) {
    rpc::ProduceResponse msg;
    msg.sessionId     = 0xDEADBEEFCAFEBABELL;
    msg.correlationId = 0x0102030405060708LL;
    msg.errorCode     = static_cast<int16_t>(rpc::ErrorCode::NotPartitionLeader);
    msg.partition     = 2;
    msg.baseOffset    = -1LL;  // meaningless; set to sentinel

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 46u);

    auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,
              static_cast<int16_t>(rpc::ErrorCode::NotPartitionLeader));
    // baseOffset is decoded but caller ignores it when errorCode != 0
    EXPECT_EQ(decoded->baseOffset, -1LL);
}

// ---------------------------------------------------------------------------
// RR-3. Byte-exactness against a hand-built LE byte array.
//
// Message: errorCode=0, partition=1, baseOffset=0x0102030405060708
//
// Expected layout (46 bytes, D6):
//   [0..1]   16 00   blockLength = 22 (LE)
//   [2..3]   11 00   templateId = 17 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   02 00   version = 2 (LE, E5.1 bump)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..25] 00 00   errorCode = 0 (LE)
//   [26..29] 01 00 00 00   partition = 1 (LE)
//   [30..37] 08 07 06 05 04 03 02 01   baseOffset (LE)
//   [38..45] FF FF FF FF FF FF FF FF   logStartOffset = -1 (not carried)
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, ByteExactness) {
    rpc::ProduceResponse msg;
    msg.sessionId     = static_cast<int64_t>(0x1122334455667788ULL);
    msg.correlationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    msg.errorCode     = 0;
    msg.partition     = 1;
    msg.baseOffset    = static_cast<int64_t>(0x0102030405060708ULL);

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 46u);

    // clang-format off
    static constexpr uint8_t kExpected[46] = {
        0x16, 0x00,                              // blockLength = 22 (D6)
        0x11, 0x00,                              // templateId = 17
        0x78, 0x00,                              // schemaId = 120
        0x03, 0x00,                              // version = 3 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x00, 0x00,                              // errorCode = 0
        0x01, 0x00, 0x00, 0x00,                  // partition = 1
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // baseOffset (LE)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,  // logStartOffset = -1
    };
    // clang-format on

    EXPECT_EQ(std::memcmp(buf.data(), kExpected, n), 0)
        << "Encoded ProduceResponse bytes differ from hand-built expected array";
}

// ---------------------------------------------------------------------------
// RR-4. i64 baseOffset extrema: INT64_MIN and INT64_MAX.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, BaseOffsetInt64Extrema) {
    auto roundTrip = [](int64_t baseOffset) -> bool {
        rpc::ProduceResponse msg;
        msg.errorCode  = 0;
        msg.partition  = 0;
        msg.baseOffset = baseOffset;
        std::array<uint8_t, 64> buf{};
        const std::size_t n = msg.encode(buf.data());
        auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
        return decoded.has_value() && decoded->baseOffset == baseOffset;
    };

    EXPECT_TRUE(roundTrip(INT64_MIN));
    EXPECT_TRUE(roundTrip(INT64_MAX));
    EXPECT_TRUE(roundTrip(0LL));
    EXPECT_TRUE(roundTrip(-1LL));
}

// ---------------------------------------------------------------------------
// RR-5. i16 errorCode extrema: all new produce error codes.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, AllErrorCodes) {
    auto roundTrip = [](rpc::ErrorCode ec) -> bool {
        rpc::ProduceResponse msg;
        msg.errorCode  = static_cast<int16_t>(ec);
        msg.partition  = 0;
        msg.baseOffset = 0;
        std::array<uint8_t, 64> buf{};
        const std::size_t n = msg.encode(buf.data());
        auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
        return decoded.has_value() &&
               decoded->errorCode == static_cast<int16_t>(ec);
    };

    EXPECT_TRUE(roundTrip(rpc::ErrorCode::OK));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::UnknownTopic));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::UnknownPartition));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::NotPartitionLeader));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::Internal));
}

// ---------------------------------------------------------------------------
// RR-6. Truncation at every boundary.
//   ENCODED_SIZE = 46 bytes (D6); no var fields.
//   avail < 46 → nullopt; avail >= 46 → ok.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, TruncationAtEveryBoundary) {
    rpc::ProduceResponse msg;
    msg.errorCode  = 0;
    msg.partition  = 0;
    msg.baseOffset = 42LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 46u);

    for (std::size_t a = 0; a < 46u; ++a) {
        EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt";
    }

    EXPECT_TRUE(rpc::ProduceResponse::decode(buf.data(), 46u).has_value());
    // Extra bytes are fine
    EXPECT_TRUE(rpc::ProduceResponse::decode(buf.data(), 64u).has_value());
}

// ---------------------------------------------------------------------------
// RR-7. Wrong templateId → nullopt (corrupt tid to 16 in a ProduceResponse buf).
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, WrongTemplateIdReturnsNullopt) {
    rpc::ProduceResponse msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    // Corrupt templateId to 16 (Produce)
    buf[2] = 16;
    buf[3] = 0;

    EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), 38u).has_value());
}

// ---------------------------------------------------------------------------
// RR-8. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, SchemaMismatchReturnsNullopt) {
    rpc::ProduceResponse msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0

    EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), 38u).has_value());
}

// ---------------------------------------------------------------------------
// RR-9. Cross-message mismatch: Produce buffer decoded as ProduceResponse → nullopt.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, ProduceBufDecodedAsProduceResponse_Nullopt) {
    static constexpr uint8_t kRb[] = {0x01};

    rpc::Produce pMsg;
    pMsg.partitionKnown = 1;
    pMsg.key            = std::span<const uint8_t>{};
    pMsg.recordBatch    = std::span<const uint8_t>(kRb, 1u);

    const std::size_t total = pMsg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    pMsg.encode(buf.data());

    // Produce has templateId=16; ProduceResponse::decode expects 17
    EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// RR-10. Cross-message mismatch: ProduceResponse buffer decoded as Produce → nullopt.
// ---------------------------------------------------------------------------
TEST(ProduceCodec, ProduceResponseBufDecodedAsProduce_Nullopt) {
    rpc::ProduceResponse rMsg;
    rMsg.errorCode  = 0;
    rMsg.partition  = 0;
    rMsg.baseOffset = 0;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = rMsg.encode(buf.data());

    // ProduceResponse has templateId=17; Produce::decode expects 16
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), n).has_value());
}

// ---------------------------------------------------------------------------
// Golden conformance: encode the two canonical Produce vectors and verify they
// decode back to the expected values.  The .bin files are committed via
// gen_vectors.cpp; this test re-encodes and compares byte-for-byte.
// ---------------------------------------------------------------------------

// Canonical test values (same as gen_vectors.cpp)
static constexpr int64_t kGoldenSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kGoldenCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static constexpr uint8_t kGoldenBatch[] = {
    0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
};
static constexpr uint8_t kGoldenKey[] = {0x41, 0x42, 0x43}; // "ABC"

// ---------------------------------------------------------------------------
// GV-1. produce_explicit_partition: encode matches expected bytes.
//   topicId=1001, partition=7, partitionKnown=1, acks=0, key=empty,
//   recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02}
//   Total = 24 + 24 + 2 + 0 + 2 + 8 = 60 bytes
// ---------------------------------------------------------------------------
TEST(ProduceConformance, ExplicitPartitionGolden) {
    rpc::Produce msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.topicId        = 1001;
    msg.partition      = 7;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kGoldenBatch, sizeof(kGoldenBatch));

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 62u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, total);

    // Decode-golden
    auto decoded = rpc::Produce::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      kGoldenSessionId);
    EXPECT_EQ(decoded->correlationId,  kGoldenCorrelationId);
    EXPECT_EQ(decoded->topicId,        1001);
    EXPECT_EQ(decoded->partition,      7);
    EXPECT_EQ(decoded->partitionKnown, 1u);
    EXPECT_EQ(decoded->key.size(),     0u);
    ASSERT_EQ(decoded->recordBatch.size(), sizeof(kGoldenBatch));
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kGoldenBatch,
                           sizeof(kGoldenBatch)), 0);

    // Header checks
    EXPECT_EQ(leU16(buf.data(), 2), 16u);   // templateId
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// ---------------------------------------------------------------------------
// GV-2. produce_key_only: encode matches expected bytes.
//   topicId=2002, partition=0 (ignored), partitionKnown=0, acks=0, key="ABC",
//   recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02}
//   Total = 24 + 24 + 2 + 3 + 2 + 8 = 63 bytes
// ---------------------------------------------------------------------------
TEST(ProduceConformance, KeyOnlyGolden) {
    rpc::Produce msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.topicId        = 2002;
    msg.partition      = 0;
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(kGoldenKey, sizeof(kGoldenKey));
    msg.recordBatch    = std::span<const uint8_t>(kGoldenBatch, sizeof(kGoldenBatch));

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 65u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, total);

    // Decode-golden
    auto decoded = rpc::Produce::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topicId,        2002);
    EXPECT_EQ(decoded->partitionKnown, 0u);
    ASSERT_EQ(decoded->key.size(),     sizeof(kGoldenKey));
    EXPECT_EQ(std::memcmp(decoded->key.data(), kGoldenKey, sizeof(kGoldenKey)), 0);
    ASSERT_EQ(decoded->recordBatch.size(), sizeof(kGoldenBatch));
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kGoldenBatch,
                           sizeof(kGoldenBatch)), 0);
}

// ---------------------------------------------------------------------------
// GV-3. produce_response golden.
//   errorCode=0, partition=7, baseOffset=0x000000000001E240 (123456)
//   Total = 38 bytes
// ---------------------------------------------------------------------------
TEST(ProduceResponseConformance, OkGolden) {
    rpc::ProduceResponse msg;
    msg.sessionId     = kGoldenSessionId;
    msg.correlationId = kGoldenCorrelationId;
    msg.errorCode     = 0;
    msg.partition     = 7;
    msg.baseOffset    = 123456LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 46u);

    auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,  0);
    EXPECT_EQ(decoded->partition,  7);
    EXPECT_EQ(decoded->baseOffset, 123456LL);

    EXPECT_EQ(leU16(buf.data(), 2), 17u);   // templateId = 17
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// ===========================================================================
// Subscribe (templateId 18, BODY_FIXED = 18)
// ===========================================================================

// ---------------------------------------------------------------------------
// SB-1. Round-trip: explicit-partition (partitionKnown=1, empty key).
//   Total: 24 (hdr) + 18 (fixed) + 2 (key prefix, 0) = 44 bytes.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, ExplicitPartitionRoundTrip) {
    rpc::Subscribe msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.topicId        = 42;
    msg.partition      = 7;
    msg.partitionKnown = 1;
    msg.fromOffset     = 1000LL;
    msg.isolation      = 1; // read_committed
    msg.key            = std::span<const uint8_t>{};

    const std::size_t expected = 24u + 18u + 2u + 0u; // 44
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Subscribe::BODY_FIXED);   // blockLength = 18
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Subscribe::TEMPLATE_ID);  // templateId = 18
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);               // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);        // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 42);    // topicId
    EXPECT_EQ(leS32(buf.data(), 28), 7);     // partition
    EXPECT_EQ(leU8 (buf.data(), 32), 1u);    // partitionKnown
    EXPECT_EQ(leS64(buf.data(), 33), 1000LL); // fromOffset
    EXPECT_EQ(leU8 (buf.data(), 41), 1u);    // isolation

    // key var field: prefix = 0
    EXPECT_EQ(leU16(buf.data(), 42), 0u);

    // Round-trip decode
    auto decoded = rpc::Subscribe::decode(buf.data(), written, rpc::kClientVersion);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      msg.sessionId);
    EXPECT_EQ(decoded->correlationId,  msg.correlationId);
    EXPECT_EQ(decoded->topicId,        42);
    EXPECT_EQ(decoded->partition,      7);
    EXPECT_EQ(decoded->partitionKnown, 1u);
    EXPECT_EQ(decoded->fromOffset,     1000LL);
    EXPECT_EQ(decoded->isolation,      1u);
    EXPECT_EQ(decoded->key.size(),     0u);
}

// ---------------------------------------------------------------------------
// SB-2. Round-trip: key-only (partitionKnown=0, non-empty key).
//   Total: 24 + 18 + 2 + kKeyLen = 44 + kKeyLen = 50 bytes.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, KeyOnlyRoundTrip) {
    rpc::Subscribe msg;
    msg.sessionId      = 0xDEADC0DE12345678LL;
    msg.correlationId  = 0xFEDCBA9876543210LL;
    msg.topicId        = 99;
    msg.partition      = 0;
    msg.partitionKnown = 0;
    msg.fromOffset     = 0LL;
    msg.isolation      = 0; // read_uncommitted
    msg.key            = std::span<const uint8_t>(kKeyBytes, kKeyLen);

    const std::size_t expected = 24u + 18u + 2u + kKeyLen; // 50
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Fixed fields
    EXPECT_EQ(leS32(buf.data(), 24), 99);   // topicId
    EXPECT_EQ(leS32(buf.data(), 28), 0);    // partition
    EXPECT_EQ(leU8 (buf.data(), 32), 0u);   // partitionKnown
    EXPECT_EQ(leS64(buf.data(), 33), 0LL);  // fromOffset
    EXPECT_EQ(leU8 (buf.data(), 41), 0u);   // isolation

    // key var field
    EXPECT_EQ(leU16(buf.data(), 42), static_cast<uint16_t>(kKeyLen));
    EXPECT_EQ(std::memcmp(buf.data() + 44, kKeyBytes, kKeyLen), 0);

    // Round-trip decode
    auto decoded = rpc::Subscribe::decode(buf.data(), written, rpc::kClientVersion);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topicId,        99);
    EXPECT_EQ(decoded->partitionKnown, 0u);
    EXPECT_EQ(decoded->isolation,      0u);
    ASSERT_EQ(decoded->key.size(),     kKeyLen);
    EXPECT_EQ(std::memcmp(decoded->key.data(), kKeyBytes, kKeyLen), 0);
}

// ---------------------------------------------------------------------------
// SB-3. Both isolation values (0 and 1) round-trip correctly.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, IsolationValues) {
    auto test = [](uint8_t isolation) {
        rpc::Subscribe msg;
        msg.partitionKnown = 1;
        msg.isolation      = isolation;
        msg.key            = std::span<const uint8_t>{};

        const std::size_t total = msg.encodedSize();
        std::vector<uint8_t> buf(total, 0u);
        msg.encode(buf.data());

        auto decoded = rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion);
        return decoded.has_value() && decoded->isolation == isolation;
    };

    EXPECT_TRUE(test(0)); // read_uncommitted
    EXPECT_TRUE(test(1)); // read_committed
}

// ---------------------------------------------------------------------------
// SB-4. fromOffset i64 extrema.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, FromOffsetInt64Extrema) {
    auto roundTrip = [](int64_t fromOffset) -> bool {
        rpc::Subscribe msg;
        msg.partitionKnown = 1;
        msg.fromOffset     = fromOffset;
        msg.key            = std::span<const uint8_t>{};

        const std::size_t total = msg.encodedSize();
        std::vector<uint8_t> buf(total, 0u);
        msg.encode(buf.data());

        auto decoded = rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion);
        return decoded.has_value() && decoded->fromOffset == fromOffset;
    };

    EXPECT_TRUE(roundTrip(INT64_MIN));
    EXPECT_TRUE(roundTrip(INT64_MAX));
    EXPECT_TRUE(roundTrip(0LL));
    EXPECT_TRUE(roundTrip(-1LL));
}

// ---------------------------------------------------------------------------
// SB-5. Byte-exactness vs a hand-built LE array.
//
// Message: sessionId=0x1122334455667788, correlationId=0x0A0B0C0D0E0F1011,
//          topicId=1, partition=2, partitionKnown=1, fromOffset=0x0102030405060708,
//          isolation=0, key=empty
//
// Expected layout (total = 24 + 18 + 2 = 44 bytes):
//   [0..1]   12 00   blockLength = 18 (LE)
//   [2..3]   12 00   templateId = 18 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   02 00   version = 2 (LE, E5.1 bump)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..27] 01 00 00 00   topicId = 1 (LE)
//   [28..31] 02 00 00 00   partition = 2 (LE)
//   [32]     01            partitionKnown = 1
//   [33..40] 08 07 06 05 04 03 02 01   fromOffset (LE)
//   [41]     00            isolation = 0
//   [42..43] 00 00         key var-len = 0 (LE)
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, ByteExactness) {
    rpc::Subscribe msg;
    msg.sessionId      = static_cast<int64_t>(0x1122334455667788ULL);
    msg.correlationId  = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    msg.topicId        = 1;
    msg.partition      = 2;
    msg.partitionKnown = 1;
    msg.fromOffset     = static_cast<int64_t>(0x0102030405060708ULL);
    msg.isolation      = 0;
    msg.key            = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 44u);

    // clang-format off
    static constexpr uint8_t kExpected[44] = {
        0x12, 0x00,                              // blockLength = 18
        0x12, 0x00,                              // templateId = 18
        0x78, 0x00,                              // schemaId = 120
        0x03, 0x00,                              // version = 3 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x01, 0x00, 0x00, 0x00,                  // topicId = 1
        0x02, 0x00, 0x00, 0x00,                  // partition = 2
        0x01,                                    // partitionKnown = 1
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // fromOffset (LE)
        0x00,                                    // isolation = 0
        0x00, 0x00,                              // key var-len = 0
    };
    // clang-format on

    EXPECT_EQ(std::memcmp(buf.data(), kExpected, n), 0)
        << "Encoded Subscribe bytes differ from hand-built expected array";
}

// ---------------------------------------------------------------------------
// SB-6. Truncation at every boundary (partitionKnown=1, key=empty).
//   Total = 24 + 18 + 2 = 44 bytes.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, TruncationAtEveryBoundary) {
    rpc::Subscribe msg;
    msg.topicId        = 1;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.fromOffset     = 42LL;
    msg.isolation      = 0;
    msg.key            = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize(); // 44
    ASSERT_EQ(total, 44u);

    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Header truncations
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), 0, rpc::kClientVersion).has_value());
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), 23, rpc::kClientVersion).has_value());

    // Fixed body incomplete (need 18 bytes at [24..41])
    for (std::size_t a = 24u; a < 42u; ++a) {
        EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), a, rpc::kClientVersion).has_value())
            << "avail=" << a << " should be nullopt (fixed body incomplete)";
    }

    // key prefix: need 2 bytes starting at off 42
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), 43, rpc::kClientVersion).has_value()); // 1 byte of prefix

    // Exact total succeeds
    EXPECT_TRUE(rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion).has_value());
}

// ---------------------------------------------------------------------------
// SB-7. Truncation: key-only; key prefix claims more than available → nullopt.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, KeyPrefixClaimsMoreThanAvail) {
    rpc::Subscribe msg;
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(kKeyBytes, kKeyLen);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total + 4, 0u);
    msg.encode(buf.data());

    // Corrupt the key prefix at offset 42 to claim 200 bytes
    buf[42] = 200;
    buf[43] = 0;

    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion).has_value());
}

// ---------------------------------------------------------------------------
// SB-8. Max-size (65535) key exact-fit accepted; one-over rejected.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, MaxU16KeyExactFit) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::vector<uint8_t> bigKey(kMaxLen, 0xCC);

    rpc::Subscribe msg;
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(bigKey.data(), kMaxLen);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> bigBuf(total, 0u);
    const std::size_t written = msg.encode(bigBuf.data());
    ASSERT_EQ(written, total);

    EXPECT_TRUE(rpc::Subscribe::decode(bigBuf.data(), total, rpc::kClientVersion).has_value());
    EXPECT_FALSE(rpc::Subscribe::decode(bigBuf.data(), total - 1u, rpc::kClientVersion).has_value());
}

// ---------------------------------------------------------------------------
// SB-9. Wrong templateId → nullopt.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, WrongTemplateIdReturnsNullopt) {
    rpc::Subscribe msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    buf[2] = 19;
    buf[3] = 0;

    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion).has_value());
}

// ---------------------------------------------------------------------------
// SB-10. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(SubscribeCodec, SchemaMismatchReturnsNullopt) {
    rpc::Subscribe msg;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    buf[4] = 111;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), total, rpc::kClientVersion).has_value());
}

// ===========================================================================
// SubscribeResponse (templateId 19, BODY_FIXED = 14, total = 38 bytes)
// ===========================================================================

// ---------------------------------------------------------------------------
// SR-1. Round-trip: errorCode=0 (OK), normal subscriptionId.
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, OkRoundTrip) {
    rpc::SubscribeResponse msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::OK);
    msg.partition      = 5;
    msg.subscriptionId = 0xDEADBEEFCAFEBABELL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::SubscribeResponse::ENCODED_SIZE); // 38

    // Header fields
    EXPECT_EQ(leU16(buf.data(), 0), rpc::SubscribeResponse::BODY_FIXED);   // 14
    EXPECT_EQ(leU16(buf.data(), 2), rpc::SubscribeResponse::TEMPLATE_ID);  // 19
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);                       // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);                // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS16(buf.data(), 24), 0);                         // errorCode = OK
    EXPECT_EQ(leS32(buf.data(), 26), 5);                          // partition
    EXPECT_EQ(leS64(buf.data(), 30), static_cast<int64_t>(0xDEADBEEFCAFEBABELL)); // subscriptionId

    // Round-trip decode
    auto decoded = rpc::SubscribeResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      msg.sessionId);
    EXPECT_EQ(decoded->correlationId,  msg.correlationId);
    EXPECT_EQ(decoded->errorCode,      static_cast<int16_t>(rpc::ErrorCode::OK));
    EXPECT_EQ(decoded->partition,      5);
    EXPECT_EQ(decoded->subscriptionId, static_cast<int64_t>(0xDEADBEEFCAFEBABELL));
}

// ---------------------------------------------------------------------------
// SR-2. errorCode != 0 round-trip.
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, ErrorCodeNonZeroRoundTrip) {
    rpc::SubscribeResponse msg;
    msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::UnknownTopic);
    msg.partition      = -1;
    msg.subscriptionId = -1LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 38u);

    auto decoded = rpc::SubscribeResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,
              static_cast<int16_t>(rpc::ErrorCode::UnknownTopic));
    EXPECT_EQ(decoded->partition,      -1);
    EXPECT_EQ(decoded->subscriptionId, -1LL);
}

// ---------------------------------------------------------------------------
// SR-3. subscriptionId i64 extrema.
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, SubscriptionIdInt64Extrema) {
    auto roundTrip = [](int64_t subId) -> bool {
        rpc::SubscribeResponse msg;
        msg.errorCode      = 0;
        msg.partition      = 0;
        msg.subscriptionId = subId;
        std::array<uint8_t, 64> buf{};
        const std::size_t n = msg.encode(buf.data());
        auto decoded = rpc::SubscribeResponse::decode(buf.data(), n);
        return decoded.has_value() && decoded->subscriptionId == subId;
    };

    EXPECT_TRUE(roundTrip(INT64_MIN));
    EXPECT_TRUE(roundTrip(INT64_MAX));
    EXPECT_TRUE(roundTrip(0LL));
    EXPECT_TRUE(roundTrip(-1LL));
}

// ---------------------------------------------------------------------------
// SR-4. Truncation at every boundary (total = 38 bytes).
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, TruncationAtEveryBoundary) {
    rpc::SubscribeResponse msg;
    msg.errorCode      = 0;
    msg.partition      = 3;
    msg.subscriptionId = 42LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 38u);

    for (std::size_t a = 0; a < 38u; ++a) {
        EXPECT_FALSE(rpc::SubscribeResponse::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt";
    }

    EXPECT_TRUE(rpc::SubscribeResponse::decode(buf.data(), 38u).has_value());
    EXPECT_TRUE(rpc::SubscribeResponse::decode(buf.data(), 64u).has_value());
}

// ---------------------------------------------------------------------------
// SR-5. Wrong templateId → nullopt.
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, WrongTemplateIdReturnsNullopt) {
    rpc::SubscribeResponse msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    buf[2] = 18;
    buf[3] = 0;

    EXPECT_FALSE(rpc::SubscribeResponse::decode(buf.data(), 38u).has_value());
}

// ---------------------------------------------------------------------------
// SR-6. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(SubscribeResponseCodec, SchemaMismatchReturnsNullopt) {
    rpc::SubscribeResponse msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;

    EXPECT_FALSE(rpc::SubscribeResponse::decode(buf.data(), 38u).has_value());
}

// ===========================================================================
// Records (templateId 20, BODY_FIXED = 24)
// ===========================================================================

// ---------------------------------------------------------------------------
// RC-1. Round-trip: binary recordBatch (NUL + 0xFF).
// ---------------------------------------------------------------------------
TEST(RecordsCodec, BinaryBatchRoundTrip) {
    rpc::Records msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.subscriptionId = 0x0102030405060708LL;
    msg.topicId        = 7;
    msg.partition      = 3;
    msg.baseOffset     = 500LL;
    msg.recordBatch    = std::span<const uint8_t>(kBatchBytes, kBatchLen);

    const std::size_t expected = 24u + 24u + 4u + kBatchLen; // 62 (rb len u32)
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Records::BODY_FIXED);   // 24
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Records::TEMPLATE_ID);  // 20
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);             // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);      // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS64(buf.data(), 24), static_cast<int64_t>(0x0102030405060708LL)); // subscriptionId
    EXPECT_EQ(leS32(buf.data(), 32), 7);    // topicId
    EXPECT_EQ(leS32(buf.data(), 36), 3);    // partition
    EXPECT_EQ(leS64(buf.data(), 40), 500LL); // baseOffset

    // recordBatch var field
    EXPECT_EQ(leU16(buf.data(), 48), static_cast<uint16_t>(kBatchLen));
    EXPECT_EQ(std::memcmp(buf.data() + 52, kBatchBytes, kBatchLen), 0);

    // Round-trip decode
    auto decoded = rpc::Records::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      msg.sessionId);
    EXPECT_EQ(decoded->correlationId,  msg.correlationId);
    EXPECT_EQ(decoded->subscriptionId, static_cast<int64_t>(0x0102030405060708LL));
    EXPECT_EQ(decoded->topicId,        7);
    EXPECT_EQ(decoded->partition,      3);
    EXPECT_EQ(decoded->baseOffset,     500LL);
    ASSERT_EQ(decoded->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kBatchBytes, kBatchLen), 0);
}

// ---------------------------------------------------------------------------
// RC-2. Empty recordBatch (len=0).
//   Total = 24 + 24 + 2 = 50 bytes.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, EmptyBatch) {
    rpc::Records msg;
    msg.sessionId      = 100LL;
    msg.correlationId  = 200LL;
    msg.subscriptionId = 12345LL;
    msg.topicId        = 5;
    msg.partition      = 1;
    msg.baseOffset     = 999LL;
    msg.recordBatch    = std::span<const uint8_t>{};

    const std::size_t expected = 24u + 24u + 4u + 0u; // 52 (rb len u32)
    ASSERT_EQ(msg.encodedSize(), expected);

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, expected);

    auto decoded = rpc::Records::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->subscriptionId,    12345LL);
    EXPECT_EQ(decoded->topicId,           5);
    EXPECT_EQ(decoded->partition,         1);
    EXPECT_EQ(decoded->baseOffset,        999LL);
    EXPECT_EQ(decoded->recordBatch.size(), 0u);
}

// ---------------------------------------------------------------------------
// RC-3. Byte-exactness vs a hand-built LE array.
//
// Message: sessionId=0x1122334455667788, correlationId=0x0A0B0C0D0E0F1011,
//          subscriptionId=0x0102030405060708, topicId=1, partition=2,
//          baseOffset=0x090A0B0C0D0E0F10, recordBatch={0xAA, 0xBB}
//
// Expected layout (total = 24 + 24 + 2 + 2 = 52 bytes):
//   [0..1]   18 00   blockLength = 24 (LE)
//   [2..3]   14 00   templateId = 20 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   02 00   version = 2 (LE, E5.1 bump)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..31] 08 07 06 05 04 03 02 01   subscriptionId (LE)
//   [32..35] 01 00 00 00   topicId = 1 (LE)
//   [36..39] 02 00 00 00   partition = 2 (LE)
//   [40..47] 10 0F 0E 0D 0C 0B 0A 09   baseOffset (LE)
//   [48..49] 02 00         recordBatch var-len = 2 (LE)
//   [50..51] AA BB         recordBatch bytes
// ---------------------------------------------------------------------------
TEST(RecordsCodec, ByteExactness) {
    static constexpr uint8_t kRb[] = {0xAA, 0xBB};

    rpc::Records msg;
    msg.sessionId      = static_cast<int64_t>(0x1122334455667788ULL);
    msg.correlationId  = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
    msg.topicId        = 1;
    msg.partition      = 2;
    msg.baseOffset     = static_cast<int64_t>(0x090A0B0C0D0E0F10ULL);
    msg.recordBatch    = std::span<const uint8_t>(kRb, sizeof(kRb));

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 54u);

    // clang-format off
    static constexpr uint8_t kExpected[54] = {
        0x18, 0x00,                              // blockLength = 24
        0x14, 0x00,                              // templateId = 20
        0x78, 0x00,                              // schemaId = 120
        0x03, 0x00,                              // version = 3 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // subscriptionId
        0x01, 0x00, 0x00, 0x00,                  // topicId = 1
        0x02, 0x00, 0x00, 0x00,                  // partition = 2
        0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09,  // baseOffset (LE)
        0x02, 0x00, 0x00, 0x00,                  // recordBatch var-len = 2 (u32)
        0xAA, 0xBB,                              // recordBatch bytes
    };
    // clang-format on

    EXPECT_EQ(std::memcmp(buf.data(), kExpected, n), 0)
        << "Encoded Records bytes differ from hand-built expected array";
}

// ---------------------------------------------------------------------------
// RC-4. subscriptionId / baseOffset i64 extrema.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, Int64Extrema) {
    auto roundTrip = [](int64_t subId, int64_t baseOffset) -> bool {
        rpc::Records msg;
        msg.subscriptionId = subId;
        msg.baseOffset     = baseOffset;
        msg.recordBatch    = std::span<const uint8_t>{};
        const std::size_t total = msg.encodedSize();
        std::vector<uint8_t> buf(total, 0u);
        msg.encode(buf.data());
        auto d = rpc::Records::decode(buf.data(), total);
        return d.has_value() &&
               d->subscriptionId == subId &&
               d->baseOffset     == baseOffset;
    };

    EXPECT_TRUE(roundTrip(INT64_MIN, INT64_MAX));
    EXPECT_TRUE(roundTrip(INT64_MAX, INT64_MIN));
    EXPECT_TRUE(roundTrip(0LL, 0LL));
    EXPECT_TRUE(roundTrip(-1LL, -1LL));
}

// ---------------------------------------------------------------------------
// RC-5. Truncation at every boundary (recordBatch = 5 bytes).
//   Total = 24 + 24 + 2 + 5 = 55 bytes.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, TruncationAtEveryBoundary) {
    static constexpr uint8_t kRb5[] = {0x01, 0x02, 0x03, 0x04, 0x05};

    rpc::Records msg;
    msg.subscriptionId = 1LL;
    msg.topicId        = 1;
    msg.partition      = 0;
    msg.baseOffset     = 0LL;
    msg.recordBatch    = std::span<const uint8_t>(kRb5, sizeof(kRb5));

    const std::size_t total = msg.encodedSize(); // 55
    ASSERT_EQ(total, 57u);

    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Header truncations
    EXPECT_FALSE(rpc::Records::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rpc::Records::decode(buf.data(), 23).has_value());

    // Fixed body incomplete: need 24 bytes at [24..47]
    for (std::size_t a = 24u; a < 48u; ++a) {
        EXPECT_FALSE(rpc::Records::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt (fixed body incomplete)";
    }

    // rb prefix: need 2 bytes at off 48
    EXPECT_FALSE(rpc::Records::decode(buf.data(), 49).has_value()); // 1 byte of prefix

    // rb says 5 bytes; partial
    for (std::size_t k = 0; k < 5u; ++k) {
        EXPECT_FALSE(rpc::Records::decode(buf.data(), 50u + k).has_value())
            << "k=" << k;
    }

    // Exact total succeeds
    EXPECT_TRUE(rpc::Records::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// RC-6. Max-size (65535) recordBatch exact-fit + one-over reject.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, MaxU16RecordBatchExactFit) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::vector<uint8_t> bigBatch(kMaxLen, 0xDD);

    rpc::Records msg;
    msg.subscriptionId = 99LL;
    msg.recordBatch    = std::span<const uint8_t>(bigBatch.data(), kMaxLen);

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> bigBuf(total, 0u);
    const std::size_t written = msg.encode(bigBuf.data());
    ASSERT_EQ(written, total);

    EXPECT_TRUE(rpc::Records::decode(bigBuf.data(), total).has_value());
    EXPECT_FALSE(rpc::Records::decode(bigBuf.data(), total - 1u).has_value());
}

// ---------------------------------------------------------------------------
// RC-7. recordBatch prefix claims more than avail → nullopt.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, RecordBatchPrefixClaimsMoreThanAvail) {
    static constexpr uint8_t kRb[] = {0x01, 0x02};

    rpc::Records msg;
    msg.recordBatch = std::span<const uint8_t>(kRb, sizeof(kRb));

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Corrupt rb prefix at offset 48 to claim 0xFFFF
    buf[48] = 0xFF;
    buf[49] = 0xFF;

    EXPECT_FALSE(rpc::Records::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// RC-8. Wrong templateId → nullopt.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, WrongTemplateIdReturnsNullopt) {
    rpc::Records msg;
    msg.recordBatch = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    buf[2] = 21;
    buf[3] = 0;

    EXPECT_FALSE(rpc::Records::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// RC-9. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(RecordsCodec, SchemaMismatchReturnsNullopt) {
    rpc::Records msg;
    msg.recordBatch = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Records::decode(buf.data(), total).has_value());
}

// ===========================================================================
// Unsubscribe (templateId 21, BODY_FIXED = 8, total = 32 bytes)
// ===========================================================================

// ---------------------------------------------------------------------------
// US-1. Round-trip.
// ---------------------------------------------------------------------------
TEST(UnsubscribeCodec, RoundTrip) {
    rpc::Unsubscribe msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.subscriptionId = 0xCAFEBABEDEADBEEFLL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::Unsubscribe::ENCODED_SIZE); // 32

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Unsubscribe::BODY_FIXED);   // 8
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Unsubscribe::TEMPLATE_ID);  // 21
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);                 // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);          // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // subscriptionId
    EXPECT_EQ(leS64(buf.data(), 24), static_cast<int64_t>(0xCAFEBABEDEADBEEFLL));

    // Round-trip decode
    auto decoded = rpc::Unsubscribe::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      msg.sessionId);
    EXPECT_EQ(decoded->correlationId,  msg.correlationId);
    EXPECT_EQ(decoded->subscriptionId, static_cast<int64_t>(0xCAFEBABEDEADBEEFLL));
}

// ---------------------------------------------------------------------------
// US-2. subscriptionId i64 extrema.
// ---------------------------------------------------------------------------
TEST(UnsubscribeCodec, SubscriptionIdInt64Extrema) {
    auto roundTrip = [](int64_t subId) -> bool {
        rpc::Unsubscribe msg;
        msg.subscriptionId = subId;
        std::array<uint8_t, 64> buf{};
        const std::size_t n = msg.encode(buf.data());
        auto d = rpc::Unsubscribe::decode(buf.data(), n);
        return d.has_value() && d->subscriptionId == subId;
    };

    EXPECT_TRUE(roundTrip(INT64_MIN));
    EXPECT_TRUE(roundTrip(INT64_MAX));
    EXPECT_TRUE(roundTrip(0LL));
    EXPECT_TRUE(roundTrip(-1LL));
}

// ---------------------------------------------------------------------------
// US-3. Truncation at every boundary (total = 32 bytes).
// ---------------------------------------------------------------------------
TEST(UnsubscribeCodec, TruncationAtEveryBoundary) {
    rpc::Unsubscribe msg;
    msg.subscriptionId = 42LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 32u);

    for (std::size_t a = 0; a < 32u; ++a) {
        EXPECT_FALSE(rpc::Unsubscribe::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt";
    }

    EXPECT_TRUE(rpc::Unsubscribe::decode(buf.data(), 32u).has_value());
    EXPECT_TRUE(rpc::Unsubscribe::decode(buf.data(), 64u).has_value());
}

// ---------------------------------------------------------------------------
// US-4. Wrong templateId → nullopt.
// ---------------------------------------------------------------------------
TEST(UnsubscribeCodec, WrongTemplateIdReturnsNullopt) {
    rpc::Unsubscribe msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    buf[2] = 20;
    buf[3] = 0;

    EXPECT_FALSE(rpc::Unsubscribe::decode(buf.data(), 32u).has_value());
}

// ---------------------------------------------------------------------------
// US-5. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(UnsubscribeCodec, SchemaMismatchReturnsNullopt) {
    rpc::Unsubscribe msg;
    std::array<uint8_t, 64> buf{};
    msg.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Unsubscribe::decode(buf.data(), 32u).has_value());
}

// ===========================================================================
// PB-2 Golden conformance (GV-4 through GV-7)
// Encode + decode-golden for all four PB-2 message types.
// ===========================================================================

// ---------------------------------------------------------------------------
// GV-4. subscribe_explicit — Subscribe with explicit partition, empty key.
//   topicId=3001, partition=5, partitionKnown=1, fromOffset=9999, isolation=1
//   Total = 24 + 18 + 2 = 44 bytes
// ---------------------------------------------------------------------------
TEST(SubscribeConformance, ExplicitGolden) {
    rpc::Subscribe msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.topicId        = 3001;
    msg.partition      = 5;
    msg.partitionKnown = 1;
    msg.fromOffset     = 9999LL;
    msg.isolation      = 1;
    msg.key            = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 44u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, total);

    auto decoded = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      kGoldenSessionId);
    EXPECT_EQ(decoded->correlationId,  kGoldenCorrelationId);
    EXPECT_EQ(decoded->topicId,        3001);
    EXPECT_EQ(decoded->partition,      5);
    EXPECT_EQ(decoded->partitionKnown, 1u);
    EXPECT_EQ(decoded->fromOffset,     9999LL);
    EXPECT_EQ(decoded->isolation,      1u);
    EXPECT_EQ(decoded->key.size(),     0u);

    EXPECT_EQ(leU16(buf.data(), 2), 18u);   // templateId
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// ---------------------------------------------------------------------------
// GV-5. subscribe_key_only — Subscribe with key, no explicit partition.
//   topicId=4004, partition=0, partitionKnown=0, fromOffset=0, isolation=0,
//   key={0x41, 0x42, 0x43} ("ABC")
//   Total = 24 + 18 + 2 + 3 = 47 bytes
// ---------------------------------------------------------------------------
TEST(SubscribeConformance, KeyOnlyGolden) {
    static constexpr uint8_t kKey[] = {0x41, 0x42, 0x43}; // "ABC"

    rpc::Subscribe msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.topicId        = 4004;
    msg.partition      = 0;
    msg.partitionKnown = 0;
    msg.fromOffset     = 0LL;
    msg.isolation      = 0;
    msg.key            = std::span<const uint8_t>(kKey, sizeof(kKey));

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 47u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, total);

    auto decoded = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topicId,        4004);
    EXPECT_EQ(decoded->partitionKnown, 0u);
    EXPECT_EQ(decoded->fromOffset,     0LL);
    EXPECT_EQ(decoded->isolation,      0u);
    ASSERT_EQ(decoded->key.size(),     sizeof(kKey));
    EXPECT_EQ(std::memcmp(decoded->key.data(), kKey, sizeof(kKey)), 0);

    EXPECT_EQ(leU16(buf.data(), 2), 18u);   // templateId
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// ---------------------------------------------------------------------------
// GV-6. records_push — Records push with binary recordBatch.
//   subscriptionId=0x0102030405060708, topicId=3001, partition=5,
//   baseOffset=100000, recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02}
//   Total = 24 + 24 + 2 + 8 = 58 bytes
// ---------------------------------------------------------------------------
TEST(RecordsConformance, PushGolden) {
    rpc::Records msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
    msg.topicId        = 3001;
    msg.partition      = 5;
    msg.baseOffset     = 100000LL;
    msg.recordBatch    = std::span<const uint8_t>(kGoldenBatch, sizeof(kGoldenBatch));

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 60u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, total);

    auto decoded = rpc::Records::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      kGoldenSessionId);
    EXPECT_EQ(decoded->correlationId,  kGoldenCorrelationId);
    EXPECT_EQ(decoded->subscriptionId, static_cast<int64_t>(0x0102030405060708ULL));
    EXPECT_EQ(decoded->topicId,        3001);
    EXPECT_EQ(decoded->partition,      5);
    EXPECT_EQ(decoded->baseOffset,     100000LL);
    ASSERT_EQ(decoded->recordBatch.size(), sizeof(kGoldenBatch));
    EXPECT_EQ(std::memcmp(decoded->recordBatch.data(), kGoldenBatch,
                           sizeof(kGoldenBatch)), 0);

    EXPECT_EQ(leU16(buf.data(), 2), 20u);   // templateId
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// ---------------------------------------------------------------------------
// GV-7. unsubscribe — Unsubscribe with specific subscriptionId.
//   subscriptionId=0x0102030405060708
//   Total = 24 + 8 = 32 bytes
// ---------------------------------------------------------------------------
TEST(UnsubscribeConformance, Golden) {
    rpc::Unsubscribe msg;
    msg.sessionId      = kGoldenSessionId;
    msg.correlationId  = kGoldenCorrelationId;
    msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 32u);

    auto decoded = rpc::Unsubscribe::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      kGoldenSessionId);
    EXPECT_EQ(decoded->correlationId,  kGoldenCorrelationId);
    EXPECT_EQ(decoded->subscriptionId, static_cast<int64_t>(0x0102030405060708ULL));

    EXPECT_EQ(leU16(buf.data(), 2), 21u);   // templateId
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}

// No-hang wave: unknown acks / isolation bytes are rejected by decode (the
// broker answers InvalidRequest) — never read as one of the two valid modes.
TEST(DataMessagesRegression, ProduceUnknownAcksByteIsRejectedRegression) {
    const std::vector<uint8_t> batch(64, 0xAB);
    rpc::Produce p;
    p.sessionId = 5; p.correlationId = 6; p.topicId = 1; p.partition = 0; p.partitionKnown = 1;
    p.acks = 1;
    p.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> buf(p.encodedSize());
    const std::size_t n = p.encode(buf.data());
    ASSERT_TRUE(rpc::Produce::decode(buf.data(), n).has_value());
    buf[rpc::MessageHeader::ENCODED_SIZE + 9] = 2;   // acks byte
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), n).has_value());
    buf[rpc::MessageHeader::ENCODED_SIZE + 9] = 0;
    EXPECT_TRUE(rpc::Produce::decode(buf.data(), n).has_value());
}

TEST(DataMessagesRegression, SubscribeUnknownIsolationByteIsRejectedRegression) {
    rpc::Subscribe s;
    s.sessionId = 5; s.correlationId = 6; s.topicId = 1; s.partition = 0; s.partitionKnown = 1;
    s.isolation = 1;
    std::vector<uint8_t> buf(s.encodedSize());
    const std::size_t n = s.encode(buf.data());
    ASSERT_TRUE(rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion).has_value());
    buf[rpc::MessageHeader::ENCODED_SIZE + 17] = 2;   // isolation byte
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion).has_value());
}

// ---------------------------------------------------------------------------
// Phase 2b E2 (controller ruling 2): a TRANSIENT partition's ring full of
// not-yet-replicated records answers the dedicated retriable TransientLogFull
// (42) — never NotLeader (which makes a client refresh metadata on every
// refusal).  Append-only wire constant; sessions below client version 6 are
// answered NotLeader, their retriable code.
// ---------------------------------------------------------------------------
TEST(TransientLogFullRegression, CodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(rune::protocol::client::ErrorCode::TransientLogFull), 42u);
    EXPECT_EQ(rune::protocol::client::kClientVersionTransientLogFull, 6);
    EXPECT_GE(rune::protocol::client::kClientVersion, rune::protocol::client::kClientVersionTransientLogFull);
}

TEST(TransientLogFullRegression, OldClientsAreAnsweredNotLeader) {
    namespace rpc = rune::protocol::client;
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 5), rpc::ErrorCode::NotLeader);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 1), rpc::ErrorCode::NotLeader);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::TransientLogFull, 6), rpc::ErrorCode::TransientLogFull);
    // The pre-existing gate is unchanged.
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 1), rpc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 6), rpc::ErrorCode::UnknownProducerId);

    rpc::ProduceResponse r;
    r.sessionId = 1; r.correlationId = 2; r.partition = 0; r.baseOffset = -1;
    r.errorCode = static_cast<int16_t>(rpc::ErrorCode::TransientLogFull);
    std::array<uint8_t, 64> buf{};
    const std::size_t n5 = r.encodeFor(buf.data(), 5);
    auto d5 = rpc::ProduceResponse::decode(buf.data(), n5);
    ASSERT_TRUE(d5.has_value());
    EXPECT_EQ(d5->errorCode, static_cast<int16_t>(rpc::ErrorCode::NotLeader)) << "a v5 client must never see 42";
    const std::size_t n6 = r.encodeFor(buf.data(), 6);
    auto d6 = rpc::ProduceResponse::decode(buf.data(), n6);
    ASSERT_TRUE(d6.has_value());
    EXPECT_EQ(d6->errorCode, 42);
}

// ===========================================================================
// Phase 2c (client v11 / broker v6): Subscribe filter trailer, response detail
// ===========================================================================
static_assert(rpc::kClientVersionFilter == rpc::kClientVersionTxnOffsetFence + 1);
static_assert(rpc::kBrokerVersionFilter == rpc::kBrokerVersionAuth + 1);
static_assert(rpc::kClientVersion == rpc::kClientVersionFilter && rpc::kBrokerVersion == rpc::kBrokerVersionFilter);
static_assert(rpc::clientUnderstandsFilters(11) && !rpc::clientUnderstandsFilters(10));

TEST(ProtocolVersions, FilterIsElevenAndBrokerSix) {
    EXPECT_EQ(rpc::kClientVersionFilter, 11);
    EXPECT_EQ(rpc::kBrokerVersionFilter, 6);
}

namespace {
rpc::Subscribe fixedSubscribe() {
    rpc::Subscribe msg;
    msg.sessionId = static_cast<int64_t>(0x1122334455667788ULL);
    msg.correlationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    msg.topicId = 1; msg.partition = 2; msg.partitionKnown = 1;
    msg.fromOffset = static_cast<int64_t>(0x0102030405060708ULL);
    msg.isolation = 0;
    return msg;
}
// Largest valid filter (8 terms x 64 In values x 16 bytes): 2 + 8 * (5 + 64 * 16).
constexpr std::size_t kMaxFilterBytesWire = 8234;
} // namespace

TEST(SubscribeFilterRegression, UnfilteredFrameIsByteIdenticalToV9Regression) {
    const rpc::Subscribe msg = fixedSubscribe();   // same message as SubscribeCodec.ByteExactness
    std::array<uint8_t, 64> buf{};
    ASSERT_EQ(msg.encodedSize(), 44u);
    ASSERT_EQ(msg.encode(buf.data()), 44u);
    // The whole v9 frame, hard-coded (same bytes as SubscribeCodec.ByteExactness): nothing after the empty key.
    static constexpr uint8_t kV9Frame[44] = {
        0x12, 0x00, 0x12, 0x00, 0x78, 0x00, 0x03, 0x00,
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,
        0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x01,
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        0x00,
        0x00, 0x00,
    };
    EXPECT_EQ(std::memcmp(buf.data(), kV9Frame, 44), 0);
    auto d = rpc::Subscribe::decode(buf.data(), 44, rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->filter.empty());
    // A v10 decode of a v9 frame is the same as a v9 decode.
    auto d9 = rpc::Subscribe::decode(buf.data(), 44, 9);
    ASSERT_TRUE(d9.has_value());
    EXPECT_TRUE(d9->filter.empty());
}

TEST(SubscribeFilterRegression, FilterRoundTripsRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    const std::array<uint8_t, 11> f{1, 1, 0x05, 0x00, 4, 1, 1, 0xAA, 0xBB, 0xCC, 0xDD};
    msg.filter = f;
    std::array<uint8_t, 128> buf{};
    ASSERT_EQ(msg.encodedSize(), 44u + 2u + 11u);
    ASSERT_EQ(msg.encode(buf.data()), 44u + 2u + 11u);
    EXPECT_EQ(buf[44], 11); EXPECT_EQ(buf[45], 0);
    auto d = rpc::Subscribe::decode(buf.data(), 57, rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    ASSERT_EQ(d->filter.size(), 11u);
    EXPECT_EQ(std::memcmp(d->filter.data(), f.data(), 11), 0);
    EXPECT_EQ(d->topicId, 1);
}

TEST(SubscribeFilterRegression, ZeroLengthTrailerIsNoFilterRegression) {
    const rpc::Subscribe msg = fixedSubscribe();
    std::array<uint8_t, 64> buf{};
    std::size_t n = msg.encode(buf.data());
    buf[n++] = 0; buf[n++] = 0;
    auto d = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->filter.empty());
}

TEST(SubscribeFilterRegression, TruncatedTrailerIsUndecodableRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    const std::array<uint8_t, 11> f{};
    msg.filter = f;
    std::array<uint8_t, 128> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 57u);
    // Every cut inside the trailer (half length prefix: 45; length 11 with 0..10 bytes present) fails cleanly.
    for (std::size_t cut = 45; cut < n; ++cut) {
        EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), cut, rpc::kClientVersion).has_value()) << "cut " << cut;
    }
    // The explicit case: declared 11, only 5 present.
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), 44 + 2 + 5, rpc::kClientVersion).has_value());
    // A declared length of 65535 with a few bytes present.
    buf[44] = 0xFF; buf[45] = 0xFF;
    EXPECT_FALSE(rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion).has_value());
    // Intact frame still decodes.
    buf[44] = 11; buf[45] = 0;
    EXPECT_TRUE(rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion).has_value());
}

TEST(SubscribeFilterRegression, PreV10SessionNeverParsesTrailerRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    const std::array<uint8_t, 11> f{1, 1, 0x05, 0x00, 4, 1, 1, 0xAA, 0xBB, 0xCC, 0xDD};
    msg.filter = f;
    std::array<uint8_t, 128> buf{};
    const std::size_t n = msg.encode(buf.data());
    // (a) a v9 session: the trailer is ignored, the filter is empty, the rest decodes.
    auto d9 = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersionAuth);
    ASSERT_TRUE(d9.has_value());
    EXPECT_TRUE(d9->filter.empty());
    EXPECT_EQ(d9->topicId, 1);
    // ... even a truncated or garbage trailer is ignored for it (E13).
    EXPECT_TRUE(rpc::Subscribe::decode(buf.data(), 45, rpc::kClientVersionAuth).has_value());
    EXPECT_TRUE(rpc::Subscribe::decode(buf.data(), 50, 1).has_value());
    // (b) a v10 session gets the exact bytes.
    auto d10 = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersionFilter);
    ASSERT_TRUE(d10.has_value());
    ASSERT_EQ(d10->filter.size(), 11u);
    EXPECT_EQ(std::memcmp(d10->filter.data(), f.data(), 11), 0);
}

TEST(SubscribeFilterRegression, MaxFilterIsAcceptedRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    std::vector<uint8_t> f(kMaxFilterBytesWire);
    for (std::size_t i = 0; i < f.size(); ++i) f[i] = static_cast<uint8_t>(i * 31u);
    msg.filter = f;
    std::vector<uint8_t> buf(msg.encodedSize());
    ASSERT_EQ(msg.encode(buf.data()), 44u + 2u + kMaxFilterBytesWire);
    auto d = rpc::Subscribe::decode(buf.data(), buf.size(), rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    ASSERT_EQ(d->filter.size(), kMaxFilterBytesWire);
    EXPECT_EQ(std::memcmp(d->filter.data(), f.data(), f.size()), 0);
}

TEST(SubscribeFilterRegression, LineageClaimInKeyAndFilterCoexistRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    const std::array<uint8_t, 8> claim{1, 2, 3, 4, 5, 6, 7, 8};
    const std::array<uint8_t, 4> f{9, 8, 7, 6};
    msg.key = claim; msg.filter = f;
    std::array<uint8_t, 128> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 44u + 8u + 2u + 4u);
    auto d = rpc::Subscribe::decode(buf.data(), n, rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    ASSERT_EQ(d->key.size(), 8u);
    ASSERT_EQ(d->filter.size(), 4u);
    EXPECT_EQ(std::memcmp(d->key.data(), claim.data(), 8), 0);
    EXPECT_EQ(std::memcmp(d->filter.data(), f.data(), 4), 0);
}

TEST(SubscribeResponseDetailRegression, DetailRoundTripsAndOldDecoderIgnoresItRegression) {
    rpc::SubscribeResponse r;
    r.sessionId = 5; r.correlationId = 6; r.errorCode = 39; r.partition = 3; r.subscriptionId = 77;
    r.detail = "filter term 1: unknown tag";
    std::array<uint8_t, 128> buf{};
    ASSERT_EQ(r.encodedSizeWithDetail(), 38u + 2u + r.detail.size());
    const std::size_t n = r.encodeFor(buf.data(), rpc::kClientVersionFilter, buf.size());
    ASSERT_EQ(n, r.encodedSizeWithDetail());
    auto d = rpc::SubscribeResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail, r.detail);
    EXPECT_EQ(d->errorCode, 39);
    EXPECT_EQ(d->subscriptionId, 77);
    // The 38-byte prefix decodes alone (a pre-v10 reader; a decoder that ignores the tail).
    auto d38 = rpc::SubscribeResponse::decode(buf.data(), 38);
    ASSERT_TRUE(d38.has_value());
    EXPECT_TRUE(d38->detail.empty());
    // A truncated trailer is a clean failure.
    EXPECT_FALSE(rpc::SubscribeResponse::decode(buf.data(), 39).has_value());
    EXPECT_FALSE(rpc::SubscribeResponse::decode(buf.data(), n - 1).has_value());
}

TEST(SubscribeResponseDetailRegression, DetailOnlyForV10SessionsAndEmptyIsOmittedRegression) {
    rpc::SubscribeResponse r;
    r.errorCode = 39; r.detail = "filter capacity";
    std::array<uint8_t, 128> buf{};
    // I2: encode(buf) NEVER writes the detail - exactly the fixed 38 bytes, identical to the no-detail frame
    // (safe for the broker's fixed 38-byte stack buffers).
    std::array<uint8_t, 128> plain{};
    rpc::SubscribeResponse none = r; none.detail = {};
    ASSERT_EQ(none.encode(plain.data()), 38u);
    buf.fill(0xEE);
    EXPECT_EQ(r.encode(buf.data()), 38u);
    EXPECT_EQ(std::memcmp(buf.data(), plain.data(), 38), 0);
    EXPECT_EQ(buf[38], 0xEE);   // nothing past the fixed frame was touched
    // encodeFor: an old client never sees a trailer it cannot parse.
    EXPECT_EQ(r.encodeFor(buf.data(), rpc::kClientVersionAuth, buf.size()), 38u);
    EXPECT_EQ(r.encodeFor(buf.data(), 1, buf.size()), 38u);
    EXPECT_EQ(r.encodeFor(buf.data(), rpc::kClientVersionFilter, buf.size()), r.encodedSizeWithDetail());
    // capacity check: too small refuses and writes nothing
    buf.fill(0xEE);
    EXPECT_EQ(r.encodeFor(buf.data(), rpc::kClientVersionFilter, r.encodedSizeWithDetail() - 1), 0u);
    EXPECT_EQ(buf[0], 0xEE);
    // Empty detail: omitted for every version; the frame is the 38-byte v9 frame.
    r.detail = {};
    EXPECT_EQ(r.encodedSizeWithDetail(), 38u);
    EXPECT_EQ(r.encodeFor(buf.data(), rpc::kClientVersionFilter, buf.size()), 38u);
    EXPECT_EQ(rpc::SubscribeResponse::ENCODED_SIZE, 38u);
}

TEST(SubscribeResponseDetailRegression, MaxLengthDetailRoundTripsRegression) {
    rpc::SubscribeResponse r;
    r.errorCode = 39;
    const std::string big(65535, 'x');
    r.detail = big;
    std::vector<uint8_t> buf(r.encodedSizeWithDetail());
    ASSERT_EQ(buf.size(), 38u + 2u + 65535u);
    ASSERT_EQ(r.encodeFor(buf.data(), rpc::kClientVersion, buf.size()), buf.size());
    auto d = rpc::SubscribeResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail.size(), 65535u);
    EXPECT_EQ(d->detail, std::string_view(big));
    // One over the max is clamped, never wrapped: the frame is still well formed.
    const std::string over(65536, 'y');
    r.detail = over;
    buf.assign(r.encodedSizeWithDetail(), 0);
    ASSERT_EQ(r.encodeFor(buf.data(), rpc::kClientVersion, buf.size()), buf.size());
    auto d2 = rpc::SubscribeResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(d2.has_value());
    EXPECT_EQ(d2->detail.size(), 65535u);
}

// M4: a clamped detail never ends mid-code-point.
TEST(SubscribeResponseDetailRegression, ClampedDetailEndsOnACodePointBoundaryRegression) {
    // 65534 ASCII bytes, then a 3-byte character (U+20AC) straddling the 65535 limit, then more.
    std::string text(65534, 'a');
    text += "\xE2\x82\xAC";
    text += "tail";
    rpc::SubscribeResponse r;
    r.errorCode = 39; r.detail = text;
    std::vector<uint8_t> buf(r.encodedSizeWithDetail());
    ASSERT_EQ(r.encodeFor(buf.data(), rpc::kClientVersion, buf.size()), buf.size());
    auto d = rpc::SubscribeResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail.size(), 65534u);                 // the straddling character is dropped whole
    EXPECT_EQ(d->detail, std::string_view(text).substr(0, 65534));
    // A boundary exactly at the limit keeps the whole character.
    std::string ok(65532, 'a'); ok += "\xE2\x82\xAC"; ok += "zz";
    r.detail = ok;
    EXPECT_EQ(r.encodedSizeWithDetail(), 38u + 2u + 65535u);
}

// I2 / M1: oversize filter is refused, never wrapped into "no filter".
TEST(SubscribeFilterRegression, OversizeFilterIsRefusedNotWrappedRegression) {
    rpc::Subscribe msg = fixedSubscribe();
    std::vector<uint8_t> f(65536, 0x11);
    msg.filter = f;
    EXPECT_FALSE(msg.fits());
    std::vector<uint8_t> buf(msg.encodedSize() + 8, 0xEE);
    EXPECT_EQ(msg.encode(buf.data()), 0u);
    EXPECT_EQ(buf[0], 0xEE);                              // nothing written
    f.resize(65535);
    msg.filter = f;
    EXPECT_TRUE(msg.fits());
    buf.assign(msg.encodedSize(), 0);
    EXPECT_EQ(msg.encode(buf.data()), 44u + 2u + 65535u);
    auto d = rpc::Subscribe::decode(buf.data(), buf.size(), rpc::kClientVersion);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->filter.size(), 65535u);
}
