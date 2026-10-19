#include <gtest/gtest.h>
#include <rune/protocol/client/MessageHeader.h>
#include <cstdint>
#include <climits>
#include <array>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Little-endian field readers (test-local, mirror RecordBatchTest.cpp style)
// ---------------------------------------------------------------------------
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}

static int64_t leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// ---------------------------------------------------------------------------
// Byte-exact round-trip: encode a header with distinct values, assert each
// field lands at the correct LE offset, then decode and compare all fields.
// ---------------------------------------------------------------------------
TEST(MessageHeaderCodec, ByteExactRoundTrip) {
    rpc::MessageHeader h;
    h.blockLength   = 0x1234u;
    h.templateId    = static_cast<uint16_t>(rpc::TemplateId::Connect);
    h.schemaId      = rpc::kSchemaId;
    h.version       = rpc::kProtocolVersion;
    h.sessionId     = 0x0102030405060708LL;
    h.correlationId = 0x0A0B0C0D0E0F1011LL;

    std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
    h.encode(buf.data());

    // -- Byte-level assertions (correct LE offsets) --
    // blockLength @ 0
    EXPECT_EQ(leU16(buf.data(), 0), 0x1234u);
    // templateId @ 2
    EXPECT_EQ(leU16(buf.data(), 2), static_cast<uint16_t>(rpc::TemplateId::Connect));
    // schemaId @ 4
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);
    // version @ 6
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);
    // sessionId @ 8
    EXPECT_EQ(leS64(buf.data(), 8), 0x0102030405060708LL);
    // correlationId @ 16
    EXPECT_EQ(leS64(buf.data(), 16), 0x0A0B0C0D0E0F1011LL);

    // -- decode and verify all fields --
    auto decoded = rpc::MessageHeader::decode(buf.data(), buf.size());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->blockLength,   h.blockLength);
    EXPECT_EQ(decoded->templateId,    h.templateId);
    EXPECT_EQ(decoded->schemaId,      h.schemaId);
    EXPECT_EQ(decoded->version,       h.version);
    EXPECT_EQ(decoded->sessionId,     h.sessionId);
    EXPECT_EQ(decoded->correlationId, h.correlationId);
}

// ---------------------------------------------------------------------------
// Truncated buffer: decode with avail < 24 must return nullopt.
// ---------------------------------------------------------------------------
TEST(MessageHeaderCodec, TruncatedBufferReturnsNullopt) {
    rpc::MessageHeader h;
    std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
    h.encode(buf.data());

    // exactly 23 bytes — one short
    auto result = rpc::MessageHeader::decode(buf.data(), 23);
    EXPECT_FALSE(result.has_value());

    // 0 bytes
    EXPECT_FALSE(rpc::MessageHeader::decode(buf.data(), 0).has_value());

    // 24 bytes — must succeed
    EXPECT_TRUE(rpc::MessageHeader::decode(buf.data(), 24).has_value());
}

// ---------------------------------------------------------------------------
// Defaults: default-constructed header has schemaId==120, version==1.
// ---------------------------------------------------------------------------
TEST(MessageHeaderCodec, DefaultValues) {
    rpc::MessageHeader h;
    EXPECT_EQ(h.schemaId,      rpc::kSchemaId);        // 120
    EXPECT_EQ(h.version,       rpc::kProtocolVersion); // 1
    EXPECT_EQ(h.blockLength,   0u);
    EXPECT_EQ(h.templateId,    0u);
    EXPECT_EQ(h.sessionId,     0LL);
    EXPECT_EQ(h.correlationId, 0LL);
}

// ---------------------------------------------------------------------------
// Boundary: min/max uint16 and int64 values round-trip exactly, including
// negative sessionId and correlationId.
// ---------------------------------------------------------------------------
TEST(MessageHeaderCodec, BoundaryValues) {
    // Max uint16 for blockLength and templateId
    {
        rpc::MessageHeader h;
        h.blockLength = UINT16_MAX;
        h.templateId  = UINT16_MAX;
        std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
        h.encode(buf.data());
        auto d = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->blockLength, UINT16_MAX);
        EXPECT_EQ(d->templateId,  UINT16_MAX);
    }

    // INT64_MAX sessionId and correlationId
    {
        rpc::MessageHeader h;
        h.sessionId     = INT64_MAX;
        h.correlationId = INT64_MAX;
        std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
        h.encode(buf.data());
        auto d = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->sessionId,     INT64_MAX);
        EXPECT_EQ(d->correlationId, INT64_MAX);
    }

    // INT64_MIN (most negative)
    {
        rpc::MessageHeader h;
        h.sessionId     = INT64_MIN;
        h.correlationId = INT64_MIN;
        std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
        h.encode(buf.data());
        auto d = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->sessionId,     INT64_MIN);
        EXPECT_EQ(d->correlationId, INT64_MIN);
    }

    // Negative session/correlation IDs (-1, -42)
    {
        rpc::MessageHeader h;
        h.sessionId     = -1LL;
        h.correlationId = -42LL;
        std::array<uint8_t, rpc::MessageHeader::ENCODED_SIZE> buf{};
        h.encode(buf.data());
        auto d = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->sessionId,     -1LL);
        EXPECT_EQ(d->correlationId, -42LL);
    }
}
