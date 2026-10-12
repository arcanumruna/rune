#include <gtest/gtest.h>
#include <rune/protocol/client/ControlMessages.h>
#include <array>
#include <cstdint>
#include <climits>
#include <cstring>
#include <span>
#include <string>
#include <vector>

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

    EXPECT_EQ(rpc::ConnectResponse::ENCODED_SIZE, 42u);     // since broker version 3
    EXPECT_EQ(rpc::ConnectResponse::ENCODED_SIZE_V0, 38u);  // an older broker's frame

    std::array<uint8_t, 64> buf{};
    const std::size_t written = resp.encode(buf.data());
    EXPECT_EQ(written, 42u);
    EXPECT_EQ(leS32(buf.data(), 38), rpc::kBrokerVersion);  // brokerVersion @ 38

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
    EXPECT_EQ(decoded->brokerVersion,      rpc::kBrokerVersion);
}

// ---------------------------------------------------------------------------
// ConnectResponse — the 38-byte frame of a broker that predates brokerVersion
// (blockLength 14) still decodes, as broker version 0.
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, OlderBrokerFrameDecodesAsVersionZeroRegression) {
    rpc::ConnectResponse resp;
    resp.code              = static_cast<int16_t>(rpc::ErrorCode::OK);
    resp.leaderMemberId    = 4;
    resp.assignedSessionId = 77;
    std::array<uint8_t, 64> buf{};
    (void)resp.encode(buf.data());
    rpc::detail::putLE16(buf.data(), 0, rpc::ConnectResponse::BODY_FIXED_V0);  // the v0 frame
    auto decoded = rpc::ConnectResponse::decode(buf.data(), rpc::ConnectResponse::ENCODED_SIZE_V0);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->brokerVersion, 0);
    EXPECT_EQ(decoded->leaderMemberId, 4);
    EXPECT_EQ(decoded->assignedSessionId, 77);
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
    ASSERT_EQ(written, 42u);

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

// ===========================================================================
// CP-2 exhaustive edge-case additions
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. Max-length egressChannel = u16 max (65535 bytes)
//    encode→decode round-trips exactly; total size == 34 + 65535 == 65569.
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, MaxU16EgressChannelRoundTrip) {
    // 65535 bytes — the maximum a uint16 length prefix can encode.
    const std::size_t kMaxLen = 65535u;

    // Build channel string outside the heap path; use a vector for the backing
    // store so the string_view remains valid.
    const std::string bigChannel(kMaxLen, 'A');

    rpc::ConnectRequest req;
    req.sessionId      = 0xDEADBEEFCAFEBABELL;
    req.correlationId  = 0x0102030405060708LL;
    req.egressStreamId = 12345;
    req.clientVersion  = 99;
    req.egressChannel  = bigChannel;

    // Total: header(24) + fixed(8) + var-prefix(2) + data(65535) = 65569
    constexpr std::size_t kExpected = 24u + 8u + 2u + 65535u;
    ASSERT_EQ(kExpected, 65569u);

    std::vector<uint8_t> buf(kExpected, 0u);
    const std::size_t written = req.encode(buf.data());
    EXPECT_EQ(written, kExpected);

    // The var-length prefix at byte 32 must be 65535 (0xFFFF LE).
    EXPECT_EQ(leU16(buf.data(), 32), static_cast<uint16_t>(65535u));

    // Round-trip decode.
    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,      req.sessionId);
    EXPECT_EQ(decoded->correlationId,  req.correlationId);
    EXPECT_EQ(decoded->egressStreamId, req.egressStreamId);
    EXPECT_EQ(decoded->clientVersion,  req.clientVersion);
    EXPECT_EQ(decoded->egressChannel.size(), kMaxLen);
    EXPECT_EQ(decoded->egressChannel,  bigChannel);
}

// ---------------------------------------------------------------------------
// 2. getVarBytes exact-fit off-by-one boundary:
//    (a) prefix value == exactly remaining bytes → ACCEPTED, full run returned.
//    (b) prefix value == remaining bytes + 1     → REJECTED.
// ---------------------------------------------------------------------------
TEST(VarFieldHelper, GetVarBytesExactFitAccepted) {
    // Build: 2-byte length prefix (=5) + 5 bytes of data, total 7 bytes.
    // When pos=0, avail=7: prefix says 5, remaining after reading prefix = 5 → exact fit.
    std::array<uint8_t, 7> buf{};
    buf[0] = 5; buf[1] = 0;   // LE uint16 = 5
    buf[2] = 'h'; buf[3] = 'e'; buf[4] = 'l'; buf[5] = 'l'; buf[6] = 'o';

    const uint8_t* outData = nullptr;
    uint16_t outLen = 0;
    std::size_t pos = 0;
    bool ok = rpc::getVarBytes(buf.data(), buf.size(), pos, outData, outLen);
    ASSERT_TRUE(ok);                          // exact fit → accepted
    EXPECT_EQ(outLen, 5u);
    EXPECT_EQ(pos, 7u);                       // consumed everything
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(outData), outLen), "hello");
}

TEST(VarFieldHelper, GetVarBytesOnePastExactFitRejected) {
    // prefix says 6, but only 5 bytes of data follow (total buf = 7 bytes).
    // pos+2 = 2, remaining after length read = 7-2 = 5 < 6 → rejected.
    std::array<uint8_t, 7> buf{};
    buf[0] = 6; buf[1] = 0;   // LE uint16 = 6
    buf[2] = 'h'; buf[3] = 'e'; buf[4] = 'l'; buf[5] = 'l'; buf[6] = 'o';

    const uint8_t* outData = nullptr;
    uint16_t outLen = 0;
    std::size_t pos = 0;
    bool ok = rpc::getVarBytes(buf.data(), buf.size(), pos, outData, outLen);
    EXPECT_FALSE(ok);    // one-past-exact → rejected
}

// ---------------------------------------------------------------------------
// 3. Full integer boundary values — all round-trip byte-exact.
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, IntBoundaryValuesAllExtrema) {
    struct TestCase {
        int16_t  code;
        int32_t  leaderMemberId;
        int64_t  assignedSessionId;
    };

    const TestCase cases[] = {
        { INT16_MIN,  INT32_MIN,  INT64_MIN },
        { INT16_MAX,  INT32_MAX,  INT64_MAX },
        { 0,         -1,          0         },
        { INT16_MIN,  INT32_MAX,  INT64_MIN },
        { INT16_MAX,  INT32_MIN,  INT64_MAX },
    };

    for (const auto& tc : cases) {
        rpc::ConnectResponse resp;
        resp.sessionId         = 0x123456789ABCDEF0LL;
        resp.correlationId     = -42LL;
        resp.code              = tc.code;
        resp.leaderMemberId    = tc.leaderMemberId;
        resp.assignedSessionId = tc.assignedSessionId;

        std::array<uint8_t, 64> buf{};
        const std::size_t written = resp.encode(buf.data());
        ASSERT_EQ(written, 42u) << "code=" << tc.code;

        // Byte-level verification of field positions.
        EXPECT_EQ(leS16(buf.data(), 24), tc.code)              << "code byte check";
        EXPECT_EQ(leS32(buf.data(), 26), tc.leaderMemberId)    << "leaderMemberId byte check";
        EXPECT_EQ(leS64(buf.data(), 30), tc.assignedSessionId) << "assignedSessionId byte check";

        auto decoded = rpc::ConnectResponse::decode(buf.data(), written);
        ASSERT_TRUE(decoded.has_value()) << "decode failed for code=" << tc.code;
        EXPECT_EQ(decoded->code,              tc.code);
        EXPECT_EQ(decoded->leaderMemberId,    tc.leaderMemberId);
        EXPECT_EQ(decoded->assignedSessionId, tc.assignedSessionId);
    }
}

TEST(ConnectRequestCodec, IntBoundaryValuesFixedFields) {
    struct TestCase {
        int32_t egressStreamId;
        int32_t clientVersion;
    };

    const TestCase cases[] = {
        { INT32_MIN, INT32_MIN },
        { INT32_MAX, INT32_MAX },
        { INT32_MIN, INT32_MAX },
        { INT32_MAX, INT32_MIN },
        { 0,         -1        },
    };

    for (const auto& tc : cases) {
        rpc::ConnectRequest req;
        req.sessionId      = INT64_MIN;
        req.correlationId  = INT64_MAX;
        req.egressStreamId = tc.egressStreamId;
        req.clientVersion  = tc.clientVersion;
        req.egressChannel  = "x";   // 1-byte channel so we have a var field

        constexpr std::size_t kExpected = 24u + 8u + 2u + 1u;
        std::array<uint8_t, 64> buf{};
        const std::size_t written = req.encode(buf.data());
        ASSERT_EQ(written, kExpected);

        // Byte-level verification.
        EXPECT_EQ(leS32(buf.data(), 24), tc.egressStreamId) << "egressStreamId byte check";
        EXPECT_EQ(leS32(buf.data(), 28), tc.clientVersion)  << "clientVersion byte check";

        auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->egressStreamId, tc.egressStreamId);
        EXPECT_EQ(decoded->clientVersion,  tc.clientVersion);
        EXPECT_EQ(decoded->sessionId,      INT64_MIN);
        EXPECT_EQ(decoded->correlationId,  INT64_MAX);
    }
}

// ---------------------------------------------------------------------------
// 4. ConnectResponse schema mismatch (schemaId != 120) → nullopt.
//    Also ConnectRequest with wrong schemaId → nullopt.
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, SchemaMismatchReturnsNullopt) {
    rpc::ConnectResponse resp;
    resp.code              = 0;
    resp.leaderMemberId    = 1;
    resp.assignedSessionId = 2;

    std::array<uint8_t, 64> buf{};
    resp.encode(buf.data());

    // Corrupt schemaId (bytes 4-5) to 111 (internal broker schema).
    buf[4] = 111;
    buf[5] = 0;

    EXPECT_FALSE(rpc::ConnectResponse::decode(buf.data(), 38u).has_value());
}

TEST(ConnectRequestCodec, SchemaMismatchExplicitValue) {
    // Same as existing SchemaMismatchReturnsNullopt but tests schemaId = 111 explicitly
    // (the internal broker schema, distinct from client 120).
    rpc::ConnectRequest req;
    req.egressChannel = "aeron:udp?endpoint=10.0.0.1:9000";

    const std::size_t kLen = 24u + 8u + 2u + req.egressChannel.size();
    std::vector<uint8_t> buf(kLen, 0u);
    req.encode(buf.data());

    buf[4] = 111;  // schemaId = 111 ≠ 120
    buf[5] = 0;

    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), kLen).has_value());
}

// ---------------------------------------------------------------------------
// 5a. Header carries correct templateId but blockLength inconsistent with
//     BODY_FIXED.  The codec trusts its own BODY_FIXED constant (not the
//     header's blockLength field), so a corrupted blockLength does NOT cause
//     rejection — the decode succeeds.  Document this.
// ---------------------------------------------------------------------------
TEST(ConnectResponseCodec, CorruptBlockLengthStillDecodes) {
    // Current behavior: ConnectResponse::decode uses a hard-coded BODY_FIXED
    // constant and does NOT re-validate blockLength from the header.
    // A corrupt blockLength field therefore does not cause rejection.
    rpc::ConnectResponse resp;
    resp.code              = 0;
    resp.leaderMemberId    = 7;
    resp.assignedSessionId = 99;

    std::array<uint8_t, 64> buf{};
    resp.encode(buf.data());

    // Corrupt blockLength (bytes 0-1) to 0 instead of 14.
    buf[0] = 0;
    buf[1] = 0;

    // Expected behavior: decode still succeeds because it reads fixed body at
    // a compile-time offset, independent of the header's blockLength field.
    auto decoded = rpc::ConnectResponse::decode(buf.data(), 38u);
    ASSERT_TRUE(decoded.has_value())
        << "Documented: corrupt blockLength not detected — fixed-offset read wins";
    EXPECT_EQ(decoded->leaderMemberId,    7);
    EXPECT_EQ(decoded->assignedSessionId, 99);
}

// ---------------------------------------------------------------------------
// 5b. avail == ENCODED_SIZE with a var field whose prefix claims more bytes
//     than available (ConnectRequest with 0 data bytes but prefix says 1).
// ---------------------------------------------------------------------------
TEST(ConnectRequestCodec, VarFieldClaimsMoreThanAvailAtExactHeaderPlusFixed) {
    // Build a valid ConnectRequest with an empty channel.
    rpc::ConnectRequest req;
    req.egressStreamId = 1;
    req.clientVersion  = 1;
    req.egressChannel  = "";

    std::array<uint8_t, 64> buf{};
    const std::size_t written = req.encode(buf.data());
    // written == 34 (header24 + fixed8 + var2)

    // Corrupt the var-length prefix at byte 32 to claim 1 byte (but none follow).
    buf[32] = 1;
    buf[33] = 0;

    // avail == 34: the prefix claims 1 byte but pos after reading prefix is 34,
    // leaving 0 bytes — rejected.
    EXPECT_FALSE(rpc::ConnectRequest::decode(buf.data(), written).has_value());
}

// ===========================================================================
// client<->broker TLS/AEAD (Task 4) — additive v4 crypto fields +
// HandshakeContinue/HandshakeContinueResponse.
// ===========================================================================

// ---------------------------------------------------------------------------
// ConnectRequest — v4 client offering crypto fields round-trips, and an old
// (<v4) client's request is unaffected (no crypto fields, byte-identical to
// pre-Task-4 wire format).
// ---------------------------------------------------------------------------
TEST(ConnectRequest, V4CipherListHandshakeAuthTokenRoundTrip) {
    const uint8_t ciphers[] = {1, 2, 3};

    rpc::ConnectRequest req;
    req.sessionId      = 11;
    req.correlationId  = 22;
    req.egressStreamId = 7;
    req.clientVersion  = rpc::kClientVersionCrypto;  // 4
    req.egressChannel  = "aeron:ipc";
    req.cipherList      = std::span<const uint8_t>(ciphers, 3);
    req.handshakeBytes  = "\x16\x03\x03hello";
    req.authToken       = "tok-123";

    std::array<uint8_t, 256> buf{};
    const std::size_t written = req.encode(buf.data());
    // Must be larger than the old (no-crypto) size for the same channel.
    EXPECT_GT(written, 24u + 8u + 2u + req.egressChannel.size());

    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->egressChannel, req.egressChannel);
    ASSERT_EQ(decoded->cipherList.size(), 3u);
    EXPECT_EQ(decoded->cipherList[0], 1);
    EXPECT_EQ(decoded->cipherList[1], 2);
    EXPECT_EQ(decoded->cipherList[2], 3);
    EXPECT_EQ(decoded->handshakeBytes, req.handshakeBytes);
    EXPECT_EQ(decoded->authToken, req.authToken);
}

TEST(ConnectRequest, OldClientNoCryptoFieldsByteIdenticalToPreTask4) {
    // A client that never sets cipherList/handshakeBytes/authToken (the
    // default) must encode to exactly the legacy frame size — no trailing
    // bytes added — regardless of clientVersion.
    rpc::ConnectRequest req;
    req.egressStreamId = 3;
    req.clientVersion  = 1;  // pre-crypto client
    req.egressChannel  = "aeron:ipc";

    std::array<uint8_t, 128> buf{};
    const std::size_t written = req.encode(buf.data());
    EXPECT_EQ(written, 24u + 8u + 2u + req.egressChannel.size());

    auto decoded = rpc::ConnectRequest::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->cipherList.empty());
    EXPECT_TRUE(decoded->handshakeBytes.empty());
    EXPECT_TRUE(decoded->authToken.empty());
}

// ---------------------------------------------------------------------------
// ConnectResponse — additive v4 crypto field (OldDecoderIgnoresNewCryptoField).
//
// A v4 ConnectResponse with chosenCipher/handshakeBytes set still decodes
// its V0 fields when avail is truncated to the old (V0) size — the new
// crypto fields default (absent), proving back-compat gating.
// ---------------------------------------------------------------------------
TEST(ConnectResponse, OldDecoderIgnoresNewCryptoField) {
    rpc::ConnectResponse resp;
    resp.sessionId         = 5;
    resp.correlationId     = 6;
    resp.code              = static_cast<int16_t>(rpc::ErrorCode::OK);
    resp.leaderMemberId    = 1;
    resp.assignedSessionId = 100;
    resp.chosenCipher      = 2;
    resp.handshakeBytes    = "serverhello";

    std::array<uint8_t, 128> buf{};
    const std::size_t written = resp.encode(buf.data());
    ASSERT_GT(written, rpc::ConnectResponse::ENCODED_SIZE);

    // Full decode sees the new crypto fields.
    auto full = rpc::ConnectResponse::decode(buf.data(), written);
    ASSERT_TRUE(full.has_value());
    EXPECT_EQ(full->chosenCipher, 2);
    EXPECT_EQ(full->handshakeBytes, "serverhello");

    // Truncated to the old (V0) size: new fields default, old fields intact.
    auto truncated = rpc::ConnectResponse::decode(buf.data(), rpc::ConnectResponse::ENCODED_SIZE);
    ASSERT_TRUE(truncated.has_value());
    EXPECT_EQ(truncated->chosenCipher, 0);
    EXPECT_TRUE(truncated->handshakeBytes.empty());
    EXPECT_EQ(truncated->leaderMemberId, 1);
    EXPECT_EQ(truncated->assignedSessionId, 100);
}

TEST(ConnectResponse, NoChosenCipherByteIdenticalToPreTask4) {
    // Default chosenCipher/handshakeBytes must encode to exactly the legacy
    // 38-byte frame — unaffected by this task.
    rpc::ConnectResponse resp;
    resp.code              = 0;
    resp.leaderMemberId    = 1;
    resp.assignedSessionId = 2;

    std::array<uint8_t, 64> buf{};
    const std::size_t written = resp.encode(buf.data());
    EXPECT_EQ(written, rpc::ConnectResponse::ENCODED_SIZE);  // 38

    auto decoded = rpc::ConnectResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->chosenCipher, 0);
    EXPECT_TRUE(decoded->handshakeBytes.empty());
}

// ---------------------------------------------------------------------------
// HandshakeContinue — round trip via the free encodeHandshakeContinue helper.
// ---------------------------------------------------------------------------
TEST(HandshakeContinue, RoundTrip) {
    uint8_t buf[256];
    const char* hs = "\x16\x03\x03...";  // arbitrary handshake bytes

    rpc::HandshakeContinue m;
    m.sessionId     = 5;
    m.correlationId = 9;
    m.handshakeBytes = {hs, 3};

    std::size_t n = rpc::encodeHandshakeContinue(buf, sizeof buf, m);
    ASSERT_GT(n, 0u);

    auto d = rpc::HandshakeContinue::decode(buf, n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->sessionId, 5);
    EXPECT_EQ(d->correlationId, 9);
    ASSERT_EQ(d->handshakeBytes.size(), 3u);
    EXPECT_EQ(d->handshakeBytes, std::string_view(hs, 3));
}

TEST(HandshakeContinue, EncodeRejectsUndersizedBuffer) {
    rpc::HandshakeContinue m;
    m.handshakeBytes = "abcdefgh";  // 8 bytes

    uint8_t small[10];  // too small for header(24)+4+8
    EXPECT_EQ(rpc::encodeHandshakeContinue(small, sizeof small, m), 0u);
}

TEST(HandshakeContinue, TemplateIdIsTen) {
    EXPECT_EQ(rpc::HandshakeContinue::TEMPLATE_ID, static_cast<uint16_t>(rpc::TemplateId::HandshakeContinue));
    EXPECT_EQ(rpc::HandshakeContinue::TEMPLATE_ID, 10);
}

// ---------------------------------------------------------------------------
// HandshakeContinueResponse — round trip, including a terminal
// ErrorCode::HandshakeFailed code with no further handshake bytes.
// ---------------------------------------------------------------------------
TEST(HandshakeContinueResponse, RoundTrip) {
    uint8_t buf[256];
    rpc::HandshakeContinueResponse m;
    m.sessionId      = 1;
    m.correlationId  = 2;
    m.code           = static_cast<int16_t>(rpc::ErrorCode::OK);
    m.handshakeBytes = "serverfinished";

    std::size_t n = rpc::encodeHandshakeContinueResponse(buf, sizeof buf, m);
    ASSERT_GT(n, 0u);

    auto d = rpc::HandshakeContinueResponse::decode(buf, n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->sessionId, 1);
    EXPECT_EQ(d->correlationId, 2);
    EXPECT_EQ(d->code, static_cast<int16_t>(rpc::ErrorCode::OK));
    EXPECT_EQ(d->handshakeBytes, "serverfinished");
}

TEST(HandshakeContinueResponse, TerminalHandshakeFailedRoundTrip) {
    uint8_t buf[64];
    rpc::HandshakeContinueResponse m;
    m.sessionId     = 3;
    m.correlationId = 4;
    m.code          = static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed);
    // No further handshake bytes on failure.

    std::size_t n = rpc::encodeHandshakeContinueResponse(buf, sizeof buf, m);
    ASSERT_GT(n, 0u);

    auto d = rpc::HandshakeContinueResponse::decode(buf, n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->code, static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed));
    EXPECT_EQ(static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed), 29);
    EXPECT_TRUE(d->handshakeBytes.empty());
}

TEST(HandshakeContinueResponse, TemplateIdIsEleven) {
    EXPECT_EQ(rpc::HandshakeContinueResponse::TEMPLATE_ID,
              static_cast<uint16_t>(rpc::TemplateId::HandshakeContinueResponse));
    EXPECT_EQ(rpc::HandshakeContinueResponse::TEMPLATE_ID, 11);
}

// ---------------------------------------------------------------------------
// Version gate constants — kClientVersion/kBrokerVersion bumped to 4;
// kProtocolVersion (the MessageHeader wire version) stays at 3.
// ---------------------------------------------------------------------------
TEST(VersionGates, ClientBrokerVersionBumpedProtocolVersionUnchanged) {
    EXPECT_EQ(rpc::kClientVersion, 5);   // no-hang wave: understands InvalidRequest
    EXPECT_EQ(rpc::kBrokerVersion, 4);
    EXPECT_EQ(rpc::kClientVersionCrypto, 4);
    EXPECT_EQ(rpc::kBrokerVersionCrypto, 4);
    EXPECT_EQ(rpc::kProtocolVersion, 3);
    EXPECT_EQ(rpc::kCryptoSchemaId, 121);
}
