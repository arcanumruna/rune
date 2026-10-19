// PingMessagesTest.cpp — exhaustive CP-4 tests for Ping (tid 6) and Pong (tid 7).
//
// Coverage mandate (brief §Tests):
//   - Round-trip: empty echo, short echo, u16-max echo (65535 bytes)
//   - Binary payload: NUL + 0xFF bytes round-trip byte-exact
//   - Exact-fit off-by-one: pos+len==avail accepted / +1 rejected
//   - Truncation at every boundary: avail<24, avail==24 with pending var, partial var bytes
//   - Template mismatch: Pong buf → Ping::decode → nullopt, and vice-versa
//   - Schema mismatch: schemaId=111 → nullopt for both
//   - Header passthrough: sessionId/correlationId incl. INT64_MIN/INT64_MAX

#include <gtest/gtest.h>
#include <rune/protocol/client/PingMessages.h>
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
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int64_t leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// ===========================================================================
// Ping (templateId 6, BODY_FIXED=0)
// ===========================================================================

// ---------------------------------------------------------------------------
// P-1. Round-trip: empty echo (len=0).
//   Total: 24 (header) + 2 (var prefix) = 26 bytes.
//   Var prefix at byte 24 must be 0.
// ---------------------------------------------------------------------------
TEST(PingCodec, EmptyEchoRoundTrip) {
    rpc::Ping ping;
    ping.sessionId     = 0x1122334455667788LL;
    ping.correlationId = 0xAABBCCDDEEFF0011LL;
    ping.echo          = std::string_view{};

    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());

    // Total size
    EXPECT_EQ(written, 26u);

    // Header fields
    EXPECT_EQ(leU16(buf.data(), 0), 0u);                    // blockLength = 0
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Ping::TEMPLATE_ID); // templateId = 6
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);          // schemaId = 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);   // version = 1
    EXPECT_EQ(leS64(buf.data(), 8),  ping.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), ping.correlationId);

    // Var prefix at byte 24 == 0
    EXPECT_EQ(leU16(buf.data(), 24), 0u);

    auto decoded = rpc::Ping::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     ping.sessionId);
    EXPECT_EQ(decoded->correlationId, ping.correlationId);
    EXPECT_EQ(decoded->echo.size(),   0u);
}

// ---------------------------------------------------------------------------
// P-2. Round-trip: short echo ("hello", 5 bytes).
//   Total: 24 + 2 + 5 = 31 bytes.
//   Var prefix at byte 24 == 5; payload at bytes 26..30.
// ---------------------------------------------------------------------------
TEST(PingCodec, ShortEchoRoundTrip) {
    const std::string_view payload = "hello";

    rpc::Ping ping;
    ping.sessionId     = 42LL;
    ping.correlationId = 99LL;
    ping.echo          = payload;

    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());

    EXPECT_EQ(written, 24u + 2u + 5u);

    // Var prefix
    EXPECT_EQ(leU16(buf.data(), 24), static_cast<uint16_t>(5u));

    // Payload bytes byte-exact
    EXPECT_EQ(std::memcmp(buf.data() + 26, payload.data(), 5), 0);

    auto decoded = rpc::Ping::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->echo, payload);
    EXPECT_EQ(decoded->sessionId,     42LL);
    EXPECT_EQ(decoded->correlationId, 99LL);
}

// ---------------------------------------------------------------------------
// P-3. Round-trip: u16-max echo (65535 bytes).
//   Total: 24 + 2 + 65535 = 65561 bytes.
//   Var prefix at byte 24 == 0xFFFF.
// ---------------------------------------------------------------------------
TEST(PingCodec, MaxU16EchoRoundTrip) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::string bigEcho(kMaxLen, 'X');

    rpc::Ping ping;
    ping.sessionId     = 0xDEADC0DE12345678LL;
    ping.correlationId = 0x0FEDCBA987654321LL;
    ping.echo          = bigEcho;

    constexpr std::size_t kExpected = 24u + 2u + 65535u; // 65561
    std::vector<uint8_t> buf(kExpected, 0u);
    const std::size_t written = ping.encode(buf.data());
    EXPECT_EQ(written, kExpected);

    // Var prefix == 0xFFFF
    EXPECT_EQ(leU16(buf.data(), 24), static_cast<uint16_t>(65535u));

    // Full round-trip
    auto decoded = rpc::Ping::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->echo.size(),   kMaxLen);
    EXPECT_EQ(decoded->sessionId,     ping.sessionId);
    EXPECT_EQ(decoded->correlationId, ping.correlationId);
    EXPECT_EQ(decoded->echo,          bigEcho);
}

// ---------------------------------------------------------------------------
// P-4. Binary payload: echo contains embedded NUL bytes and 0xFF — round-trips byte-exact.
// ---------------------------------------------------------------------------
TEST(PingCodec, BinaryPayloadRoundTrip) {
    // Build a payload with NUL, 0xFF, and regular bytes
    std::string payload;
    payload.push_back('\x00');
    payload.push_back('\xFF');
    payload.push_back('A');
    payload.push_back('\x00');
    payload.push_back('\xFE');
    payload.push_back('Z');

    rpc::Ping ping;
    ping.sessionId     = 7LL;
    ping.correlationId = 13LL;
    ping.echo          = payload;

    const std::size_t total = 24u + 2u + payload.size();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = ping.encode(buf.data());
    EXPECT_EQ(written, total);

    auto decoded = rpc::Ping::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->echo.size(), payload.size());
    // Byte-exact comparison (can't use string == since string_view compares chars, NUL-safe)
    EXPECT_EQ(std::memcmp(decoded->echo.data(), payload.data(), payload.size()), 0);
}

// ---------------------------------------------------------------------------
// P-5. Exact-fit off-by-one: pos+len == avail is accepted; prefix claims +1 byte is rejected.
// ---------------------------------------------------------------------------
TEST(PingCodec, ExactFitEchoAccepted) {
    rpc::Ping ping;
    ping.echo = "abcde"; // 5 bytes

    constexpr std::size_t kExpected = 24u + 2u + 5u; // 31
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    // Exact avail == total → accepted
    EXPECT_TRUE(rpc::Ping::decode(buf.data(), kExpected).has_value());
}

TEST(PingCodec, OnePastExactFitEchoRejected) {
    rpc::Ping ping;
    ping.echo = "abcde"; // 5 bytes; prefix at byte 24 says 5

    constexpr std::size_t kExpected = 24u + 2u + 5u; // 31
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    // Corrupt the var prefix to claim 6 bytes; avail stays at 31 (only 5 follow byte 26)
    buf[24] = 6;
    buf[25] = 0;

    EXPECT_FALSE(rpc::Ping::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// P-6. Truncation at every boundary:
//   avail<24 → nullopt (header incomplete)
//   avail==24 → nullopt (var prefix needs 2 more bytes)
//   avail==25 → nullopt (only 1 byte of 2-byte prefix)
//   avail==26+k (0 <= k < echoLen) → nullopt (echo bytes short)
//   avail==total → ok
// ---------------------------------------------------------------------------
TEST(PingCodec, TruncationAtEveryBoundary) {
    rpc::Ping ping;
    ping.echo = "trunctest"; // 9 bytes

    const std::size_t total = 24u + 2u + 9u; // 35
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = ping.encode(buf.data());
    ASSERT_EQ(written, total);

    // Header truncated
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), 23).has_value());

    // avail==24: header ok, but var prefix (2 bytes at pos 24) missing
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), 24).has_value());

    // avail==25: only 1 of 2 var-prefix bytes present
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), 25).has_value());

    // avail==26: prefix consumed (says 9 bytes), but 0 echo bytes follow → rejected
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), 26).has_value());

    // avail==26+k for k=1..8 → only k of 9 bytes present → rejected
    for (std::size_t k = 1; k < 9u; ++k) {
        EXPECT_FALSE(rpc::Ping::decode(buf.data(), 26u + k).has_value())
            << "k=" << k;
    }

    // Exact total succeeds
    EXPECT_TRUE(rpc::Ping::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// P-7. Template mismatch: decode a Pong-tid buffer with Ping::decode → nullopt.
// ---------------------------------------------------------------------------
TEST(PingCodec, TemplateMismatch_PongBuf_ReturnsNullopt) {
    rpc::Pong pong;
    pong.echo = "ping-pong";

    const std::size_t total = 24u + 2u + 9u;
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = pong.encode(buf.data());

    // Buffer has templateId=7; Ping::decode expects templateId=6 → nullopt
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// P-8. Schema mismatch: schemaId=111 (non-120) → nullopt.
// ---------------------------------------------------------------------------
TEST(PingCodec, SchemaMismatchReturnsNullopt) {
    rpc::Ping ping;
    ping.echo = "schema-check";

    const std::size_t total = 24u + 2u + 12u;
    std::vector<uint8_t> buf(total, 0u);
    ping.encode(buf.data());

    // Overwrite schemaId at bytes 4..5 with 111
    buf[4] = 111;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Ping::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// P-9. Header passthrough: sessionId/correlationId including INT64_MIN/INT64_MAX.
// ---------------------------------------------------------------------------
TEST(PingCodec, NegativeSessionAndCorrelationId) {
    rpc::Ping ping;
    ping.sessionId     = INT64_MIN;
    ping.correlationId = INT64_MAX;
    ping.echo          = "boundary";

    const std::size_t total = 24u + 2u + 8u;
    std::vector<uint8_t> buf(total, 0u);
    ping.encode(buf.data());

    auto decoded = rpc::Ping::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     INT64_MIN);
    EXPECT_EQ(decoded->correlationId, INT64_MAX);
}

// ---------------------------------------------------------------------------
// P-10. Var-prefix present but claims more bytes than avail → nullopt.
// ---------------------------------------------------------------------------
TEST(PingCodec, VarPrefixClaimsMoreThanAvail) {
    rpc::Ping ping;
    ping.echo = "abc"; // 3 bytes

    const std::size_t total = 24u + 2u + 3u; // 29
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());
    ASSERT_EQ(written, total);

    // Corrupt var prefix to 0xFFFF — far beyond avail
    buf[24] = 0xFF;
    buf[25] = 0xFF;

    EXPECT_FALSE(rpc::Ping::decode(buf.data(), written).has_value());
}

// ===========================================================================
// Pong (templateId 7, BODY_FIXED=0) — mirrors Ping tests exactly
// ===========================================================================

// ---------------------------------------------------------------------------
// Q-1. Round-trip: empty echo.
// ---------------------------------------------------------------------------
TEST(PongCodec, EmptyEchoRoundTrip) {
    rpc::Pong pong;
    pong.sessionId     = 0x9988776655443322LL;
    pong.correlationId = 0x1020304050607080LL;
    pong.echo          = std::string_view{};

    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());

    EXPECT_EQ(written, 26u);

    // Header
    EXPECT_EQ(leU16(buf.data(), 0), 0u);                    // blockLength = 0
    EXPECT_EQ(leU16(buf.data(), 2), rpc::Pong::TEMPLATE_ID); // templateId = 7
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);
    EXPECT_EQ(leS64(buf.data(), 8),  pong.sessionId);
    EXPECT_EQ(leS64(buf.data(), 16), pong.correlationId);

    // Var prefix == 0
    EXPECT_EQ(leU16(buf.data(), 24), 0u);

    auto decoded = rpc::Pong::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     pong.sessionId);
    EXPECT_EQ(decoded->correlationId, pong.correlationId);
    EXPECT_EQ(decoded->echo.size(),   0u);
}

// ---------------------------------------------------------------------------
// Q-2. Round-trip: short echo.
// ---------------------------------------------------------------------------
TEST(PongCodec, ShortEchoRoundTrip) {
    const std::string_view payload = "world";

    rpc::Pong pong;
    pong.sessionId     = 100LL;
    pong.correlationId = 200LL;
    pong.echo          = payload;

    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());

    EXPECT_EQ(written, 24u + 2u + 5u);
    EXPECT_EQ(leU16(buf.data(), 24), static_cast<uint16_t>(5u));
    EXPECT_EQ(std::memcmp(buf.data() + 26, payload.data(), 5), 0);

    auto decoded = rpc::Pong::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->echo, payload);
    EXPECT_EQ(decoded->sessionId,     100LL);
    EXPECT_EQ(decoded->correlationId, 200LL);
}

// ---------------------------------------------------------------------------
// Q-3. Round-trip: u16-max echo (65535 bytes).
// ---------------------------------------------------------------------------
TEST(PongCodec, MaxU16EchoRoundTrip) {
    constexpr std::size_t kMaxLen = 65535u;
    const std::string bigEcho(kMaxLen, 'Y');

    rpc::Pong pong;
    pong.sessionId     = 0xCAFEBABEDEADBEEFLL;
    pong.correlationId = 0x0123456789ABCDEFLL;
    pong.echo          = bigEcho;

    constexpr std::size_t kExpected = 24u + 2u + 65535u; // 65561
    std::vector<uint8_t> buf(kExpected, 0u);
    const std::size_t written = pong.encode(buf.data());
    EXPECT_EQ(written, kExpected);

    // Var prefix == 0xFFFF
    EXPECT_EQ(leU16(buf.data(), 24), static_cast<uint16_t>(65535u));

    auto decoded = rpc::Pong::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->echo.size(),   kMaxLen);
    EXPECT_EQ(decoded->sessionId,     pong.sessionId);
    EXPECT_EQ(decoded->correlationId, pong.correlationId);
    EXPECT_EQ(decoded->echo,          bigEcho);
}

// ---------------------------------------------------------------------------
// Q-4. Binary payload: NUL + 0xFF + other bytes round-trip byte-exact.
// ---------------------------------------------------------------------------
TEST(PongCodec, BinaryPayloadRoundTrip) {
    std::string payload;
    payload.push_back('\xFF');
    payload.push_back('\x00');
    payload.push_back('\xFE');
    payload.push_back('\x01');
    payload.push_back('\x00');

    rpc::Pong pong;
    pong.sessionId     = -1LL;
    pong.correlationId = -2LL;
    pong.echo          = payload;

    const std::size_t total = 24u + 2u + payload.size();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = pong.encode(buf.data());
    EXPECT_EQ(written, total);

    auto decoded = rpc::Pong::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->echo.size(), payload.size());
    EXPECT_EQ(std::memcmp(decoded->echo.data(), payload.data(), payload.size()), 0);
}

// ---------------------------------------------------------------------------
// Q-5. Exact-fit off-by-one: accepted / rejected.
// ---------------------------------------------------------------------------
TEST(PongCodec, ExactFitEchoAccepted) {
    rpc::Pong pong;
    pong.echo = "fghij"; // 5 bytes

    constexpr std::size_t kExpected = 24u + 2u + 5u;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    EXPECT_TRUE(rpc::Pong::decode(buf.data(), kExpected).has_value());
}

TEST(PongCodec, OnePastExactFitEchoRejected) {
    rpc::Pong pong;
    pong.echo = "fghij"; // 5 bytes; prefix at byte 24 says 5

    constexpr std::size_t kExpected = 24u + 2u + 5u;
    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());
    ASSERT_EQ(written, kExpected);

    // Corrupt prefix to claim 6 bytes; avail stays at 31 (only 5 follow)
    buf[24] = 6;
    buf[25] = 0;

    EXPECT_FALSE(rpc::Pong::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// Q-6. Truncation at every boundary.
// ---------------------------------------------------------------------------
TEST(PongCodec, TruncationAtEveryBoundary) {
    rpc::Pong pong;
    pong.echo = "pongtest"; // 8 bytes

    const std::size_t total = 24u + 2u + 8u; // 34
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = pong.encode(buf.data());
    ASSERT_EQ(written, total);

    // Header truncated
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), 0).has_value());
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), 23).has_value());

    // avail==24: var prefix missing
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), 24).has_value());

    // avail==25: only 1 byte of 2-byte prefix
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), 25).has_value());

    // avail==26: prefix consumed (says 8 bytes), 0 echo bytes follow → rejected
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), 26).has_value());

    // k=1..7: partial echo bytes
    for (std::size_t k = 1; k < 8u; ++k) {
        EXPECT_FALSE(rpc::Pong::decode(buf.data(), 26u + k).has_value())
            << "k=" << k;
    }

    // Exact total succeeds
    EXPECT_TRUE(rpc::Pong::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// Q-7. Template mismatch: decode a Ping-tid buffer with Pong::decode → nullopt.
// ---------------------------------------------------------------------------
TEST(PongCodec, TemplateMismatch_PingBuf_ReturnsNullopt) {
    rpc::Ping ping;
    ping.echo = "ping-pong";

    const std::size_t total = 24u + 2u + 9u;
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = ping.encode(buf.data());

    // Buffer has templateId=6; Pong::decode expects templateId=7 → nullopt
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// Q-8. Schema mismatch: schemaId=111 → nullopt.
// ---------------------------------------------------------------------------
TEST(PongCodec, SchemaMismatchReturnsNullopt) {
    rpc::Pong pong;
    pong.echo = "schema-bad";

    const std::size_t total = 24u + 2u + 10u;
    std::vector<uint8_t> buf(total, 0u);
    pong.encode(buf.data());

    buf[4] = 111;
    buf[5] = 0;

    EXPECT_FALSE(rpc::Pong::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// Q-9. Header passthrough: INT64_MIN/INT64_MAX.
// ---------------------------------------------------------------------------
TEST(PongCodec, NegativeSessionAndCorrelationId) {
    rpc::Pong pong;
    pong.sessionId     = INT64_MIN;
    pong.correlationId = INT64_MAX;
    pong.echo          = "edge";

    const std::size_t total = 24u + 2u + 4u;
    std::vector<uint8_t> buf(total, 0u);
    pong.encode(buf.data());

    auto decoded = rpc::Pong::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     INT64_MIN);
    EXPECT_EQ(decoded->correlationId, INT64_MAX);
}

// ---------------------------------------------------------------------------
// Q-10. Var-prefix claims more bytes than avail → nullopt.
// ---------------------------------------------------------------------------
TEST(PongCodec, VarPrefixClaimsMoreThanAvail) {
    rpc::Pong pong;
    pong.echo = "xyz";

    const std::size_t total = 24u + 2u + 3u; // 29
    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());
    ASSERT_EQ(written, total);

    buf[24] = 0xFF;
    buf[25] = 0xFF;

    EXPECT_FALSE(rpc::Pong::decode(buf.data(), written).has_value());
}

// ===========================================================================
// Cross-message mismatch pairs (Ping ↔ Pong ↔ existing messages)
// ===========================================================================

// ---------------------------------------------------------------------------
// X-1. Pong buffer decoded as Ping → nullopt (covered also in P-7 / Q-7 above).
//   Belt-and-suspenders: named cross-message test.
// ---------------------------------------------------------------------------
TEST(PingPongCrossMismatch, PongAsPing) {
    rpc::Pong pong;
    pong.echo = "cross";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());
    EXPECT_FALSE(rpc::Ping::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-2. Ping buffer decoded as Pong → nullopt.
// ---------------------------------------------------------------------------
TEST(PingPongCrossMismatch, PingAsPong) {
    rpc::Ping ping;
    ping.echo = "cross";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());
    EXPECT_FALSE(rpc::Pong::decode(buf.data(), written).has_value());
}

// ---------------------------------------------------------------------------
// X-3. Schema mismatch for both messages using the other tid's schema corruption.
//   Set schemaId=0 (not 111, not 120) → nullopt for Ping.
// ---------------------------------------------------------------------------
TEST(PingPongCrossMismatch, SchemaZeroRejectedForPing) {
    rpc::Ping ping;
    ping.echo = "s";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = ping.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0

    EXPECT_FALSE(rpc::Ping::decode(buf.data(), written).has_value());
}

TEST(PingPongCrossMismatch, SchemaZeroRejectedForPong) {
    rpc::Pong pong;
    pong.echo = "s";
    std::array<uint8_t, 64> buf{};
    const std::size_t written = pong.encode(buf.data());

    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0

    EXPECT_FALSE(rpc::Pong::decode(buf.data(), written).has_value());
}
