#include <gtest/gtest.h>
#include <rune/protocol/client/ControlMessages.h>
#include <array>
#include <cstdint>
#include <climits>
#include <cstring>
#include <string>
#include <vector>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-local LE readers (mirrors ControlMessagesConnectTest.cpp style)
// ---------------------------------------------------------------------------
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int16_t leS16(const uint8_t* buf, int off) {
    return static_cast<int16_t>(leU16(buf, off));
}
static int64_t leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// ===========================================================================
// Keepalive (templateId 3, BODY_FIXED=0, ENCODED_SIZE=24)
// ===========================================================================

// ---------------------------------------------------------------------------
// K-1. Header-only round-trip; all byte offsets verified.
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, RoundTrip) {
    rpc::Keepalive ka;
    ka.sessionId     = 0x0102030405060708LL;
    ka.correlationId = 0x0A0B0C0D0E0F1011LL;

    EXPECT_EQ(rpc::Keepalive::ENCODED_SIZE, 24u);

    std::array<uint8_t, 32> buf{};
    const std::size_t written = ka.encode(buf.data());
    EXPECT_EQ(written, 24u);

    // blockLength == 0 at offset 0
    EXPECT_EQ(leU16(buf.data(), 0), 0u);
    // templateId == 3 at offset 2
    EXPECT_EQ(leU16(buf.data(), 2), 3u);
    // schemaId == 120 at offset 4
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);
    // version == 1 at offset 6
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);
    // sessionId at offset 8
    EXPECT_EQ(leS64(buf.data(), 8), ka.sessionId);
    // correlationId at offset 16
    EXPECT_EQ(leS64(buf.data(), 16), ka.correlationId);

    auto decoded = rpc::Keepalive::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     ka.sessionId);
    EXPECT_EQ(decoded->correlationId, ka.correlationId);
}

// ---------------------------------------------------------------------------
// K-2. Truncation at 23, 12, 0 all return nullopt.
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, TruncationReturnsNullopt) {
    rpc::Keepalive ka;
    ka.sessionId     = 1LL;
    ka.correlationId = 2LL;

    std::array<uint8_t, 32> buf{};
    ka.encode(buf.data());

    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), 23).has_value());
    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), 12).has_value());
    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), 0).has_value());

    // Exact size succeeds
    EXPECT_TRUE(rpc::Keepalive::decode(buf.data(), 24).has_value());
}

// ---------------------------------------------------------------------------
// K-3. Template mismatch: decode a Disconnect buffer as Keepalive → nullopt.
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, TemplateMismatch_DisconnectBuf_ReturnsNullopt) {
    rpc::Disconnect dis;
    dis.reason = 42;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = dis.encode(buf.data());

    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// K-4. Schema mismatch (schemaId = 111) → nullopt.
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, SchemaMismatchReturnsNullopt) {
    rpc::Keepalive ka;
    std::array<uint8_t, 32> buf{};
    ka.encode(buf.data());

    buf[4] = 111;
    buf[5] = 0;  // schemaId = 111 ≠ 120

    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), 24).has_value());
}

// ---------------------------------------------------------------------------
// K-5. Default struct carries schemaId=120 / version=kProtocolVersion when encoded.
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, DefaultSchemaIdAndVersion) {
    rpc::Keepalive ka;
    std::array<uint8_t, 32> buf{};
    ka.encode(buf.data());

    EXPECT_EQ(leU16(buf.data(), 4), 120u);               // kSchemaId
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);  // version=2 (E5.1 bump)
}

// ---------------------------------------------------------------------------
// K-6. Negative sessionId / correlationId round-trip (INT64_MIN).
// ---------------------------------------------------------------------------
TEST(KeepaliveCodec, NegativeSessionAndCorrelationId) {
    rpc::Keepalive ka;
    ka.sessionId     = INT64_MIN;
    ka.correlationId = INT64_MAX;

    std::array<uint8_t, 32> buf{};
    ka.encode(buf.data());

    auto decoded = rpc::Keepalive::decode(buf.data(), 24u);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     INT64_MIN);
    EXPECT_EQ(decoded->correlationId, INT64_MAX);
}

// ===========================================================================
// Disconnect (templateId 4, BODY_FIXED=2, ENCODED_SIZE=26)
// ===========================================================================

// ---------------------------------------------------------------------------
// D-1. Round-trip with distinct values; verify byte offsets exactly.
// ---------------------------------------------------------------------------
TEST(DisconnectCodec, RoundTrip) {
    rpc::Disconnect dis;
    dis.sessionId     = 0x1122334455667788LL;
    dis.correlationId = -99LL;
    dis.reason        = -7;

    EXPECT_EQ(rpc::Disconnect::ENCODED_SIZE, 26u);

    std::array<uint8_t, 64> buf{};
    const std::size_t written = dis.encode(buf.data());
    EXPECT_EQ(written, 26u);

    // Header checks
    EXPECT_EQ(leU16(buf.data(), 0), rpc::Disconnect::BODY_FIXED);   // blockLength = 2
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Disconnect::TEMPLATE_ID);  // templateId = 4
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);                 // schemaId = 120
    // sessionId at byte 8
    EXPECT_EQ(leS64(buf.data(), 8),  dis.sessionId);
    // correlationId at byte 16
    EXPECT_EQ(leS64(buf.data(), 16), dis.correlationId);
    // reason at byte 24
    EXPECT_EQ(leS16(buf.data(), 24), dis.reason);

    auto decoded = rpc::Disconnect::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     dis.sessionId);
    EXPECT_EQ(decoded->correlationId, dis.correlationId);
    EXPECT_EQ(decoded->reason,        dis.reason);
}

// ---------------------------------------------------------------------------
// D-2. reason field boundary values: INT16_MIN, INT16_MAX, 0, -1.
// ---------------------------------------------------------------------------
TEST(DisconnectCodec, ReasonBoundaryValues) {
    const int16_t reasons[] = { INT16_MIN, INT16_MAX, 0, -1 };

    for (int16_t r : reasons) {
        rpc::Disconnect dis;
        dis.sessionId     = 0xDEADBEEFLL;
        dis.correlationId = 0xCAFEBABELL;
        dis.reason        = r;

        std::array<uint8_t, 64> buf{};
        const std::size_t written = dis.encode(buf.data());
        ASSERT_EQ(written, 26u) << "reason=" << r;

        // Byte-level: reason at offset 24
        EXPECT_EQ(leS16(buf.data(), 24), r) << "byte-level reason=" << r;

        auto decoded = rpc::Disconnect::decode(buf.data(), written);
        ASSERT_TRUE(decoded.has_value()) << "reason=" << r;
        EXPECT_EQ(decoded->reason, r) << "decoded reason=" << r;
    }
}

// ---------------------------------------------------------------------------
// D-3. Truncation at avail=24 (fixed body short) and avail=25 → nullopt.
// ---------------------------------------------------------------------------
TEST(DisconnectCodec, TruncationReturnsNullopt) {
    rpc::Disconnect dis;
    dis.reason = 1;
    std::array<uint8_t, 64> buf{};
    dis.encode(buf.data());

    // Header short
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), 23).has_value());
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), 0).has_value());

    // Fixed body short: avail=24 means only header, reason byte missing
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), 24).has_value());
    // avail=25 means only 1 of 2 reason bytes present
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), 25).has_value());

    // Exact full size succeeds
    EXPECT_TRUE(rpc::Disconnect::decode(buf.data(), 26).has_value());
}

// ---------------------------------------------------------------------------
// D-4. Template mismatch: feed a Keepalive buffer → nullopt.
// ---------------------------------------------------------------------------
TEST(DisconnectCodec, TemplateMismatch_KeepaliveBuf_ReturnsNullopt) {
    rpc::Keepalive ka;
    ka.sessionId = 42LL;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ka.encode(buf.data());

    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// D-5. Schema mismatch → nullopt.
// ---------------------------------------------------------------------------
TEST(DisconnectCodec, SchemaMismatchReturnsNullopt) {
    rpc::Disconnect dis;
    dis.reason = 0;
    std::array<uint8_t, 64> buf{};
    dis.encode(buf.data());

    buf[4] = 111;
    buf[5] = 0;  // schemaId = 111

    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), 26u).has_value());
}

// ===========================================================================
// ErrorResponse (templateId 5, BODY_FIXED=2)
// ===========================================================================

// ---------------------------------------------------------------------------
// E-1. Round-trip with realistic message; verify exact byte offsets.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, RoundTripRealistic) {
    std::string_view msg = "cluster not available";

    rpc::ErrorResponse err;
    err.sessionId     = 0xAABBCCDDEEFF0011LL;
    err.correlationId = 0x0102030405060708LL;
    err.code          = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
    err.message       = msg;

    // Total: 24 (header) + 2 (code) + 2 (var prefix) + msg.size()
    const std::size_t expectedTotal = 24u + 2u + 2u + msg.size();

    std::array<uint8_t, 256> buf{};
    const std::size_t written = err.encode(buf.data());
    EXPECT_EQ(written, expectedTotal);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), rpc::ErrorResponse::BODY_FIXED);  // blockLength = 2
    EXPECT_EQ(leU16(buf.data(), 2), rpc::ErrorResponse::TEMPLATE_ID); // templateId = 5
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);                   // schemaId = 120
    // sessionId at byte 8
    EXPECT_EQ(leS64(buf.data(), 8),  err.sessionId);
    // correlationId at byte 16
    EXPECT_EQ(leS64(buf.data(), 16), err.correlationId);
    // code at byte 24
    EXPECT_EQ(leS16(buf.data(), 24), static_cast<int16_t>(rpc::ErrorCode::NotLeader));
    // var length prefix at byte 26
    EXPECT_EQ(leU16(buf.data(), 26), static_cast<uint16_t>(msg.size()));
    // var bytes at byte 28
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(buf.data() + 28), msg.size()), msg);

    auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     err.sessionId);
    EXPECT_EQ(decoded->correlationId, err.correlationId);
    EXPECT_EQ(decoded->code,          err.code);
    EXPECT_EQ(decoded->message,       msg);
}

// ---------------------------------------------------------------------------
// E-2. code boundary values: INT16_MIN, INT16_MAX.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, CodeBoundaryIntExtrema) {
    const int16_t codes[] = { INT16_MIN, INT16_MAX };

    for (int16_t c : codes) {
        rpc::ErrorResponse err;
        err.code    = c;
        err.message = "x";

        std::array<uint8_t, 64> buf{};
        const std::size_t written = err.encode(buf.data());
        ASSERT_EQ(written, 24u + 2u + 2u + 1u) << "code=" << c;

        EXPECT_EQ(leS16(buf.data(), 24), c) << "byte-level code=" << c;

        auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
        ASSERT_TRUE(decoded.has_value()) << "code=" << c;
        EXPECT_EQ(decoded->code, c) << "decoded code=" << c;
    }
}

// ---------------------------------------------------------------------------
// E-3. code == each ErrorCode enum value 0..6; all round-trip.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, CodeAllErrorCodeEnumValues) {
    const rpc::ErrorCode codes[] = {
        rpc::ErrorCode::OK,
        rpc::ErrorCode::ProtocolVersion,
        rpc::ErrorCode::NotLeader,
        rpc::ErrorCode::Redirect,
        rpc::ErrorCode::SessionExpired,
        rpc::ErrorCode::UnknownSession,
        rpc::ErrorCode::Internal,
    };

    for (auto ec : codes) {
        rpc::ErrorResponse err;
        err.sessionId     = 123LL;
        err.correlationId = 456LL;
        err.code          = static_cast<int16_t>(ec);
        err.message       = "err";

        std::array<uint8_t, 64> buf{};
        const std::size_t written = err.encode(buf.data());

        auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
        ASSERT_TRUE(decoded.has_value()) << "ErrorCode=" << static_cast<int>(ec);
        EXPECT_EQ(decoded->code, static_cast<int16_t>(ec)) << "ErrorCode=" << static_cast<int>(ec);
        EXPECT_EQ(decoded->message, "err");
    }
}

// ---------------------------------------------------------------------------
// E-4. Empty message (len=0) round-trips; var prefix == 0 at byte 26.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, EmptyMessage) {
    rpc::ErrorResponse err;
    err.code    = 0;
    err.message = "";

    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    EXPECT_EQ(written, 24u + 2u + 2u);  // 28 total

    EXPECT_EQ(leU16(buf.data(), 26), 0u);  // var prefix = 0

    auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->message.size(), 0u);
    EXPECT_EQ(decoded->code, 0);
}

// ---------------------------------------------------------------------------
// E-5. Short (non-empty) message round-trips.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, ShortMessage) {
    rpc::ErrorResponse err;
    err.code    = static_cast<int16_t>(rpc::ErrorCode::Internal);
    err.message = "hi";

    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    EXPECT_EQ(written, 24u + 2u + 2u + 2u);  // 30 total

    auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->message, "hi");
}

// ---------------------------------------------------------------------------
// E-6. u16-max (65535-byte) message round-trips; var prefix = 0xFFFF LE.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, MaxU16MessageRoundTrip) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::string bigMsg(kMaxLen, 'Z');

    rpc::ErrorResponse err;
    err.sessionId     = 0xDEADC0DEDEADC0DELL;
    err.correlationId = 0x0FEDCBA987654321LL;
    err.code          = static_cast<int16_t>(rpc::ErrorCode::Internal);
    err.message       = bigMsg;

    // Total: 24 + 2 + 2 + 65535 = 65563
    constexpr std::size_t kExpected = 24u + 2u + 2u + 65535u;
    ASSERT_EQ(kExpected, 65563u);

    std::vector<uint8_t> buf(kExpected, 0u);
    const std::size_t written = err.encode(buf.data());
    EXPECT_EQ(written, kExpected);

    // var prefix at byte 26 must be 0xFFFF
    EXPECT_EQ(leU16(buf.data(), 26), static_cast<uint16_t>(65535u));

    auto decoded = rpc::ErrorResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->message.size(), kMaxLen);
    EXPECT_EQ(decoded->code, static_cast<int16_t>(rpc::ErrorCode::Internal));
    EXPECT_EQ(decoded->sessionId,     err.sessionId);
    EXPECT_EQ(decoded->correlationId, err.correlationId);
    EXPECT_EQ(decoded->message,       bigMsg);
}

// ---------------------------------------------------------------------------
// E-7. Exact-fit off-by-one for message var-length:
//   (a) pos+len == avail → accepted.
//   (b) pos+len == avail+1 (prefix claims one extra byte) → rejected.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, ExactFitMessageAccepted) {
    rpc::ErrorResponse err;
    err.code    = 0;
    err.message = "hello";  // 5 bytes

    constexpr std::size_t kExpected = 24u + 2u + 2u + 5u;  // 33
    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    // Exact avail == total → accepted
    EXPECT_TRUE(rpc::ErrorResponse::decode(buf.data(), kExpected).has_value());
}

TEST(ErrorResponseCodec, OnePastExactFitMessageRejected) {
    rpc::ErrorResponse err;
    err.code    = 0;
    err.message = "hello";  // 5 bytes; prefix at byte 26 says 5

    constexpr std::size_t kExpected = 24u + 2u + 2u + 5u;  // 33
    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    // Corrupt prefix to claim 6 bytes but avail == 33 (only 5 follow byte 28)
    buf[26] = 6;
    buf[27] = 0;

    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// E-8. Truncation at every boundary:
//   header < 24, fixed-body short at 25, var prefix present but bytes short.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, TruncationAtEveryBoundary) {
    std::string_view msg = "truncation-test";  // 15 bytes

    rpc::ErrorResponse err;
    err.code    = static_cast<int16_t>(rpc::ErrorCode::SessionExpired);
    err.message = msg;

    const std::size_t total = 24u + 2u + 2u + msg.size();  // 43
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = err.encode(buf.data());
    ASSERT_EQ(written, total);

    // Header truncated
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 23).has_value());
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 0).has_value());

    // Fixed body short: avail=24 means no room for 2-byte code field
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 24).has_value());
    // avail=25 means only 1 of 2 code bytes present
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 25).has_value());

    // Var prefix present (bytes 26-27) but only 1 byte of var prefix
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 27).has_value());

    // Var prefix fully present but message bytes short
    // avail=28 → prefix consumed, 0 of 15 message bytes follow → rejected
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 28).has_value());
    // avail=29 → 1 of 15 message bytes → rejected
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 29).has_value());
    // avail=42 → 14 of 15 bytes → rejected
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), 42).has_value());

    // Exact total succeeds
    EXPECT_TRUE(rpc::ErrorResponse::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// E-9. Var-length prefix claims more than avail → nullopt.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, VarPrefixClaimsMoreThanAvail) {
    rpc::ErrorResponse err;
    err.code    = 0;
    err.message = "abc";

    const std::size_t total = 24u + 2u + 2u + 3u;  // 31
    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    ASSERT_EQ(written, total);

    // Corrupt var prefix to 0xFFFF (65535) — buf is only 64 bytes total
    buf[26] = 0xFF;
    buf[27] = 0xFF;

    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// E-10. Template mismatch: Keepalive buffer decoded as ErrorResponse → nullopt.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, TemplateMismatch_KeepaliveBuf_ReturnsNullopt) {
    rpc::Keepalive ka;
    ka.sessionId = 77LL;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ka.encode(buf.data());

    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// E-11. Schema mismatch (schemaId != 120) → nullopt.
// ---------------------------------------------------------------------------
TEST(ErrorResponseCodec, SchemaMismatchReturnsNullopt) {
    rpc::ErrorResponse err;
    err.code    = 0;
    err.message = "schema-check";

    const std::size_t total = 24u + 2u + 2u + 12u;
    std::vector<uint8_t> buf(total, 0u);
    err.encode(buf.data());

    buf[4] = 111;
    buf[5] = 0;  // schemaId = 111

    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), total).has_value());
}

// ===========================================================================
// Cross-message template mismatch pairs
// ===========================================================================

// ---------------------------------------------------------------------------
// X-1. Keepalive buffer decoded as Disconnect → nullopt.
// ---------------------------------------------------------------------------
TEST(CrossMessageMismatch, KeepaliveAsDisconnect) {
    rpc::Keepalive ka;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ka.encode(buf.data());
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-2. Disconnect buffer decoded as ErrorResponse → nullopt.
// ---------------------------------------------------------------------------
TEST(CrossMessageMismatch, DisconnectAsErrorResponse) {
    rpc::Disconnect dis;
    dis.reason = 5;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = dis.encode(buf.data());
    EXPECT_FALSE(rpc::ErrorResponse::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-3. ErrorResponse buffer decoded as Keepalive → nullopt.
// ---------------------------------------------------------------------------
TEST(CrossMessageMismatch, ErrorResponseAsKeepalive) {
    rpc::ErrorResponse err;
    err.message = "cross";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-4. Disconnect buffer decoded as Keepalive → nullopt.
// ---------------------------------------------------------------------------
TEST(CrossMessageMismatch, DisconnectAsKeepalive) {
    rpc::Disconnect dis;
    dis.reason = 0;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = dis.encode(buf.data());
    EXPECT_FALSE(rpc::Keepalive::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-5. ErrorResponse buffer decoded as Disconnect → nullopt.
// ---------------------------------------------------------------------------
TEST(CrossMessageMismatch, ErrorResponseAsDisconnect) {
    rpc::ErrorResponse err;
    err.message = "wrong";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = err.encode(buf.data());
    EXPECT_FALSE(rpc::Disconnect::decode(buf.data(), written).has_value());
}
