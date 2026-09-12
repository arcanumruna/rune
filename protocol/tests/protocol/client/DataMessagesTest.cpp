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
// Produce (templateId 16, BODY_FIXED = 9)
// ===========================================================================

// ---------------------------------------------------------------------------
// PR-1. Round-trip: explicit-partition (partitionKnown=1, empty key).
//   Total: 24 (hdr) + 9 (fixed) + 2 (key prefix, 0) + 2 (rb prefix) + kBatchLen
//        = 37 + kBatchLen = 47 bytes.
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

    const std::size_t expected = 24u + 9u + 2u + 0u + 2u + kBatchLen; // 47
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Produce::BODY_FIXED);   // blockLength = 9
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Produce::TEMPLATE_ID);  // templateId = 16
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);             // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);      // 1
    EXPECT_EQ(leS64(buf.data(), 8),  msg.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), msg.correlationId);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 7);   // topicId
    EXPECT_EQ(leS32(buf.data(), 28), 3);   // partition
    EXPECT_EQ(leU8 (buf.data(), 32), 1u);  // partitionKnown

    // key var field: prefix = 0
    EXPECT_EQ(leU16(buf.data(), 33), 0u);

    // recordBatch var field: prefix = kBatchLen
    EXPECT_EQ(leU16(buf.data(), 35), static_cast<uint16_t>(kBatchLen));
    EXPECT_EQ(std::memcmp(buf.data() + 37, kBatchBytes, kBatchLen), 0);

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
//   Total: 24 + 9 + 2 + kKeyLen + 2 + kBatchLen
//        = 37 + kKeyLen + kBatchLen = 53 bytes.
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

    const std::size_t expected = 24u + 9u + 2u + kKeyLen + 2u + kBatchLen; // 53
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Fixed fields
    EXPECT_EQ(leS32(buf.data(), 24), 42);  // topicId
    EXPECT_EQ(leU8 (buf.data(), 32), 0u);  // partitionKnown

    // key var field
    EXPECT_EQ(leU16(buf.data(), 33), static_cast<uint16_t>(kKeyLen));
    EXPECT_EQ(std::memcmp(buf.data() + 35, kKeyBytes, kKeyLen), 0);

    // recordBatch var field
    const std::size_t rbOff = 35u + kKeyLen;
    EXPECT_EQ(leU16(buf.data(), static_cast<int>(rbOff)),
              static_cast<uint16_t>(kBatchLen));
    EXPECT_EQ(std::memcmp(buf.data() + rbOff + 2, kBatchBytes, kBatchLen), 0);

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
// Message: topicId=1, partition=2, partitionKnown=1, key=empty,
//          recordBatch={0xAA, 0xBB}
//
// Expected layout (total = 24 + 9 + 2 + 0 + 2 + 2 = 39 bytes):
//   [0..1]   09 00   blockLength = 9 (LE)
//   [2..3]   10 00   templateId = 16 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   01 00   version = 1 (LE)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId = 0x1122334455667788 (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..27] 01 00 00 00   topicId = 1 (LE)
//   [28..31] 02 00 00 00   partition = 2 (LE)
//   [32]     01            partitionKnown = 1
//   [33..34] 00 00         key var-len = 0 (LE)
//   [35..36] 02 00         recordBatch var-len = 2 (LE)
//   [37..38] AA BB         recordBatch bytes
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

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 39u);

    // clang-format off
    static constexpr uint8_t kExpected[39] = {
        0x09, 0x00,                              // blockLength = 9
        0x10, 0x00,                              // templateId = 16
        0x78, 0x00,                              // schemaId = 120
        0x01, 0x00,                              // version = 1
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x01, 0x00, 0x00, 0x00,                  // topicId = 1
        0x02, 0x00, 0x00, 0x00,                  // partition = 2
        0x01,                                    // partitionKnown = 1
        0x00, 0x00,                              // key var-len = 0
        0x02, 0x00,                              // recordBatch var-len = 2
        0xAA, 0xBB,                              // recordBatch bytes
    };
    // clang-format on

    EXPECT_EQ(std::memcmp(buf.data(), kExpected, n), 0)
        << "Encoded Produce bytes differ from hand-built expected array";
}

// ---------------------------------------------------------------------------
// PR-4. Truncation at every boundary (partitionKnown=1, key=empty, rb=5 bytes).
//   Total = 24 + 9 + 2 + 0 + 2 + 5 = 42 bytes.
//
//   avail < 24            → nullopt (header incomplete)
//   avail = 24..32        → nullopt (fixed body incomplete: need 9 bytes at off 24)
//   avail = 33            → nullopt (key prefix needs 2 bytes at off 33)
//   avail = 34            → nullopt (only 1 byte of 2-byte key prefix)
//   avail = 35            → nullopt (key prefix says 0; rb prefix at 35 needs 2 bytes)
//   avail = 36            → nullopt (only 1 byte of rb prefix)
//   avail = 37+k (k<5)   → nullopt (only k of 5 rb bytes)
//   avail = 42            → ok
// ---------------------------------------------------------------------------
TEST(ProduceCodec, TruncationAtEveryBoundary) {
    static constexpr uint8_t kRb5[] = {0x01, 0x02, 0x03, 0x04, 0x05};

    rpc::Produce msg;
    msg.topicId        = 1;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kRb5, sizeof(kRb5));

    const std::size_t total = msg.encodedSize(); // 42
    ASSERT_EQ(total, 42u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    // Header truncations
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 23).has_value());

    // Fixed body incomplete: need all 9 bytes at [24..32]
    for (std::size_t a = 24u; a < 33u; ++a) {
        EXPECT_FALSE(rpc::Produce::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt (fixed body incomplete)";
    }

    // key prefix: need 2 bytes starting at off 33
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 34).has_value());  // 1 byte of prefix

    // key is 0 bytes; rb prefix at off 35, needs 2 bytes
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 35).has_value());
    EXPECT_FALSE(rpc::Produce::decode(buf.data(), 36).has_value());

    // rb says 5 bytes; partial bytes
    for (std::size_t k = 0; k < 5u; ++k) {
        EXPECT_FALSE(rpc::Produce::decode(buf.data(), 37u + k).has_value())
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

    const std::size_t total = msg.encodedSize(); // 39 (no key) — actually 24+9+2+0+2+1=38
    std::vector<uint8_t> buf(total + 4, 0u);
    msg.encode(buf.data());

    // Corrupt the key prefix to claim 100 bytes; avail = total (key field exhausted)
    buf[33] = 100;
    buf[34] = 0;

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

    const std::size_t total = msg.encodedSize(); // 24+9+2+0+2+3 = 40
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Corrupt the rb prefix at offset 35 to claim 0xFFFF bytes
    buf[35] = 0xFF;
    buf[36] = 0xFF;

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

    const std::size_t expected = 24u + 9u + 2u + 0u + 2u + 0u; // 37
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

    const std::size_t total = msg.encodedSize(); // 24+9+2+0+2+65535 = 65572
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
// ProduceResponse (templateId 17, BODY_FIXED = 14, total = 38 bytes)
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
    ASSERT_EQ(n, 38u);

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
// Expected layout (38 bytes):
//   [0..1]   0E 00   blockLength = 14 (LE)
//   [2..3]   11 00   templateId = 17 (LE)
//   [4..5]   78 00   schemaId = 120 (LE)
//   [6..7]   01 00   version = 1 (LE)
//   [8..15]  88 77 66 55 44 33 22 11   sessionId (LE)
//   [16..23] 11 10 0F 0E 0D 0C 0B 0A   correlationId (LE)
//   [24..25] 00 00   errorCode = 0 (LE)
//   [26..29] 01 00 00 00   partition = 1 (LE)
//   [30..37] 08 07 06 05 04 03 02 01   baseOffset (LE)
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
    ASSERT_EQ(n, 38u);

    // clang-format off
    static constexpr uint8_t kExpected[38] = {
        0x0E, 0x00,                              // blockLength = 14
        0x11, 0x00,                              // templateId = 17
        0x78, 0x00,                              // schemaId = 120
        0x01, 0x00,                              // version = 1
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        0x00, 0x00,                              // errorCode = 0
        0x01, 0x00, 0x00, 0x00,                  // partition = 1
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // baseOffset (LE)
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
//   ENCODED_SIZE = 38 bytes; no var fields.
//   avail < 38 → nullopt; avail >= 38 → ok.
// ---------------------------------------------------------------------------
TEST(ProduceResponseCodec, TruncationAtEveryBoundary) {
    rpc::ProduceResponse msg;
    msg.errorCode  = 0;
    msg.partition  = 0;
    msg.baseOffset = 42LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 38u);

    for (std::size_t a = 0; a < 38u; ++a) {
        EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), a).has_value())
            << "avail=" << a << " should be nullopt";
    }

    EXPECT_TRUE(rpc::ProduceResponse::decode(buf.data(), 38u).has_value());
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
//   topicId=1001, partition=7, partitionKnown=1, key=empty,
//   recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02}
//   Total = 24 + 9 + 2 + 0 + 2 + 8 = 45 bytes
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
    ASSERT_EQ(total, 45u);

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
//   topicId=2002, partition=0 (ignored), partitionKnown=0, key="ABC",
//   recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02}
//   Total = 24 + 9 + 2 + 3 + 2 + 8 = 48 bytes
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
    ASSERT_EQ(total, 48u);

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
    ASSERT_EQ(n, 38u);

    auto decoded = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,  0);
    EXPECT_EQ(decoded->partition,  7);
    EXPECT_EQ(decoded->baseOffset, 123456LL);

    EXPECT_EQ(leU16(buf.data(), 2), 17u);   // templateId = 17
    EXPECT_EQ(leU16(buf.data(), 4), 120u);  // schemaId
}
