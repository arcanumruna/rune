#include <gtest/gtest.h>
#include <rune/protocol/client/ControlMessages.h>
#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-local LE readers (mirrors MessageHeaderTest.cpp style)
// ---------------------------------------------------------------------------
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int16_t leS16(const uint8_t* buf, int off) {
    return static_cast<int16_t>(leU16(buf, off));
}
static int32_t leS32(const uint8_t* buf, int off) {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off + 1]) << 8)
               | (uint32_t(buf[off + 2]) << 16)
               | (uint32_t(buf[off + 3]) << 24);
    return static_cast<int32_t>(v);
}
static int64_t leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// ---------------------------------------------------------------------------
// ConnectRequest — round-trip with realistic egressChannel
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, RoundTripRealistic) {
    std::string_view channel = "aeron:udp?endpoint=192.168.1.42:40123";

    rpc::ConnectRequest req;
    req.sessionId      = 0x0102030405060708LL;
    req.correlationId  = 0x0A0B0C0D0E0F1011LL;
    req.egressStreamId = 7;
    req.clientVersion  = 42;
    req.egressChannel  = channel;

    // Total: 24 (header) + 8 (fixed) + 2 (var len) + channel.size()
    const std::size_t expectedTotal = 24 + 8 + 2 + channel.size();

    std::array<uint8_t, 256> buf{};
    const std::size_t written = req.encode(buf.data());
    EXPECT_EQ(written, expectedTotal);

    // Header fields
    EXPECT_EQ(leU16(buf.data(), 0), rpc::ConnectRequest::BODY_FIXED);   // blockLength
    EXPECT_EQ(leU16(buf.data(), 2), rpc::ConnectRequest::TEMPLATE_ID);  // templateId
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);                    // schemaId

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 7);   // egressStreamId @ 24
    EXPECT_EQ(leS32(buf.data(), 28), 42);  // clientVersion  @ 28

    // Var length prefix
    EXPECT_EQ(leU16(buf.data(), 32), static_cast<uint16_t>(channel.size())); // var len @ 32

    // Var bytes content
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(buf.data() + 34), channel.size()), channel);

    // Decode
    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      req.sessionId);
    EXPECT_EQ(decoded->correlationId,  req.correlationId);
    EXPECT_EQ(decoded->egressStreamId, req.egressStreamId);
    EXPECT_EQ(decoded->clientVersion,  req.clientVersion);
    EXPECT_EQ(decoded->egressChannel,  req.egressChannel);
}

// ---------------------------------------------------------------------------
// ConnectRequest — empty egressChannel round-trips
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, EmptyEgressChannel) {
    rpc::ConnectRequest req;
    req.egressChannel  = "";
    req.egressStreamId = 1;
    req.clientVersion  = 1;

    std::array<uint8_t, 64> buf{};
    const std::size_t written = req.encode(buf.data());
    EXPECT_EQ(written, 24u + 8u + 2u);  // 34 total

    // Var length == 0
    EXPECT_EQ(leU16(buf.data(), 32), 0u);

    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->egressChannel.size(), 0u);
    EXPECT_EQ(decoded->egressStreamId, 1);
    EXPECT_EQ(decoded->clientVersion,  1);
}

// ---------------------------------------------------------------------------
// ConnectRequest — long (>256 B) egressChannel round-trips
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, LongEgressChannel) {
    // Build a channel string > 256 bytes
    const std::string longChannel(300, 'x');

    rpc::ConnectRequest req;
    req.egressStreamId = 99;
    req.clientVersion  = 7;
    req.egressChannel  = longChannel;

    std::array<uint8_t, 512> buf{};
    const std::size_t written = req.encode(buf.data());
    EXPECT_EQ(written, 24u + 8u + 2u + 300u);

    EXPECT_EQ(leU16(buf.data(), 32), 300u);

    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->egressChannel, longChannel);
    EXPECT_EQ(decoded->egressStreamId, 99);
    EXPECT_EQ(decoded->clientVersion,  7);
}

// ---------------------------------------------------------------------------
// ConnectResponse — round-trip with distinct values
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, RoundTrip) {
    rpc::ConnectResponse resp;
    resp.sessionId         =  0x1122334455667788LL;
    resp.correlationId     = -1LL;
    resp.code              = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
    resp.leaderMemberId    = -3;
    resp.assignedSessionId = 0x7FFFFFFFFFFFFFFFLL;

    EXPECT_EQ(rpc::ConnectResponse::ENCODED_SIZE, 38u);

    std::array<uint8_t, 64> buf{};
    const std::size_t written = resp.encode(buf.data());
    EXPECT_EQ(written, 38u);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::ConnectResponse::BODY_FIXED);   // blockLength
    EXPECT_EQ(leU16(buf.data(), 2), rpc::ConnectResponse::TEMPLATE_ID);  // templateId

    // Fixed body byte offsets
    EXPECT_EQ(leS16(buf.data(), 24), static_cast<int16_t>(rpc::ErrorCode::NotLeader)); // code @ 24
    EXPECT_EQ(leS32(buf.data(), 26), -3);                                              // leaderMemberId @ 26
    EXPECT_EQ(leS64(buf.data(), 30), 0x7FFFFFFFFFFFFFFFLL);                           // assignedSessionId @ 30

    // Decode
    auto decoded = rpc::ConnectResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,          resp.sessionId);
    EXPECT_EQ(decoded->correlationId,      resp.correlationId);
    EXPECT_EQ(decoded->code,               resp.code);
    EXPECT_EQ(decoded->leaderMemberId,     resp.leaderMemberId);
    EXPECT_EQ(decoded->assignedSessionId,  resp.assignedSessionId);
}

// ---------------------------------------------------------------------------
// ConnectResponse — large assignedSessionId + various negative values
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, LargeAndNegativeValues) {
    rpc::ConnectResponse resp;
    resp.code              = static_cast<int16_t>(rpc::ErrorCode::OK);
    resp.leaderMemberId    = -99;
    resp.assignedSessionId = 0x0FEDCBA987654321LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t written = resp.encode(buf.data());
    ASSERT_EQ(written, 38u);

    auto decoded = rpc::ConnectResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->leaderMemberId,     -99);
    EXPECT_EQ(decoded->assignedSessionId,  0x0FEDCBA987654321LL);
}

// ---------------------------------------------------------------------------
// Truncation tests — decode returns nullopt at each boundary
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, TruncationReturnsNullopt) {
    std::string_view channel = "aeron:ipc";

    rpc::ConnectRequest req;
    req.egressStreamId = 1;
    req.clientVersion  = 1;
    req.egressChannel  = channel;

    std::array<uint8_t, 128> buf{};
    const std::size_t full = req.encode(buf.data());

    // Header short (< 24)
    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), 23).has_value());
    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), 0).has_value());

    // Fixed body short (24..31)
    for (std::size_t avail = 24; avail < 32; ++avail)
        EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), avail).has_value())
            << "avail=" << avail;

    // Var length prefix cut (only 1 byte of the 2-byte length prefix)
    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), 33).has_value());

    // Var length claims more than remaining
    // Corrupt the var length to claim huge size, but avail == full
    std::array<uint8_t, 128> corrupt = buf;
    corrupt[32] = 0xFF;
    corrupt[33] = 0xFF;  // length = 65535 but buf is much shorter
    EXPECT_FALSE(rpc::ConnectRequest::decode(corrupt.data(), full).has_value());

    // Full buffer must succeed
    EXPECT_TRUE(rpc::ConnectRequest::decode(buf.data(), full).has_value());
}

TEST(ConnectResponseCodec, TruncationReturnsNullopt) {
    rpc::ConnectResponse resp;
    resp.code              = 0;
    resp.leaderMemberId    = 1;
    resp.assignedSessionId = 2;

    std::array<uint8_t, 64> buf{};
    resp.encode(buf.data());

    // Header short
    EXPECT_FALSE(rpc::ConnectResponse::decode(buf.data(), 23).has_value());
    EXPECT_FALSE(rpc::ConnectResponse::decode(buf.data(), 0).has_value());

    // Fixed body short (24..37)
    for (std::size_t avail = 24; avail < 38; ++avail)
        EXPECT_FALSE(rpc::ConnectResponse::decode(buf.data(), avail).has_value())
            << "avail=" << avail;

    // Exact full size must succeed
    EXPECT_TRUE(rpc::ConnectResponse::decode(buf.data(), 38).has_value());
}

// ---------------------------------------------------------------------------
// Template mismatch — feeding wrong templateId returns nullopt
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, TemplateMismatchReturnsNullopt) {
    // Encode a ConnectResponse, try to decode as ConnectRequest
    rpc::ConnectResponse resp;
    resp.leaderMemberId = 0;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = resp.encode(buf.data());

    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    EXPECT_FALSE(decoded.has_value());
}

TEST(ConnectResponseCodec, TemplateMismatchReturnsNullopt) {
    // Encode a ConnectRequest, try to decode as ConnectResponse
    rpc::ConnectRequest req;
    req.egressChannel = "aeron:ipc";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = req.encode(buf.data());

    auto decoded = rpc::ConnectResponse::decode(buf.data(), written);
    EXPECT_FALSE(decoded.has_value());
}

// ---------------------------------------------------------------------------
// Schema mismatch — wrong schemaId returns nullopt
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, SchemaMismatchReturnsNullopt) {
    rpc::ConnectRequest req;
    req.egressChannel = "aeron:ipc";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = req.encode(buf.data());

    // Corrupt schemaId (byte 4 and 5)
    buf[4] = 0x00;
    buf[5] = 0x00;  // schemaId = 0, not 120

    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// getVarBytes bounds — length prefix larger than remaining buffer → false
// ---------------------------------------------------------------------------
TEST(VarFieldHelper, GetVarBytesBoundsCheck) {
    // Buffer: 2-byte length prefix of 100, but only 10 bytes of content follow
    std::array<uint8_t, 12> buf{};
    buf[0] = 100;  // LE uint16: length = 100
    buf[1] = 0;
    // only buf[2..11] available (10 bytes, not 100)

    const uint8_t* outData = nullptr;
    uint16_t outLen = 0;
    std::size_t pos = 0;
    bool ok = rpc::getVarBytes(buf.data(), buf.size(), pos, outData, outLen);
    EXPECT_FALSE(ok);   // must not over-read

    // Also check: pos == 2 (we consumed the length prefix, but then rejected)
    // Actually per spec: "reads u16 len then len bytes at pos; advances pos; false if truncated"
    // The implementation advances past the length then fails — pos may be 2.
    // We only care that ok==false and no UB.
}

TEST(VarFieldHelper, GetVarBytesSuccessPath) {
    // Buffer: length=3, then "abc"
    std::array<uint8_t, 5> buf{3, 0, 'a', 'b', 'c'};
    const uint8_t* outData = nullptr;
    uint16_t outLen = 0;
    std::size_t pos = 0;
    bool ok = rpc::getVarBytes(buf.data(), buf.size(), pos, outData, outLen);
    ASSERT_TRUE(ok);
    EXPECT_EQ(outLen, 3u);
    EXPECT_EQ(pos, 5u);
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(outData), outLen), "abc");
}

TEST(VarFieldHelper, GetVarBytesLengthPrefixTruncated) {
    // Only 1 byte available — can't even read the 2-byte length prefix
    std::array<uint8_t, 1> buf{0xFF};
    const uint8_t* outData = nullptr;
    uint16_t outLen = 0;
    std::size_t pos = 0;
    bool ok = rpc::getVarBytes(buf.data(), 1, pos, outData, outLen);
    EXPECT_FALSE(ok);
}
