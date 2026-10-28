// tests/ConformanceTest.cpp
//
// CC-2 — CP-5 golden conformance vectors through the client adapter layer.
//
// For each of the 10 committed .bin vectors this harness asserts BOTH:
//   1. Decode-golden:  load file bytes → decode via client adapter →
//                      every field equals conformance/manifest.md values.
//   2. Encode-golden:  encode those same canonical values via client adapter →
//                      bytes are byte-identical to the golden file (memcmp, exact length).
//
// Plus a session-flow test that decodes each frame in order, checks the
// templateId sequence [1, 2, 3, 6, 7, 4], and verifies the round-trip
// fields match through the adapter layer.
//
// RUNE_CONFORMANCE_DIR is injected as a compile-time string by CMakeLists.txt
// and points at the sibling rune-protocol/conformance directory.
//
// The test will fail loudly if the directory or any vector file is missing —
// the vectors are the contract.

#include <gtest/gtest.h>

// Client adapter layer — these are the adapters under test (not rune-protocol
// headers directly).
#include <rune/client/codec/Codec.h>

#include <array>
#include <span>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

// ---------------------------------------------------------------------------
// Compile-time conformance dir (injected by CMakeLists.txt)
// ---------------------------------------------------------------------------
#ifndef RUNE_CONFORMANCE_DIR
#  error "RUNE_CONFORMANCE_DIR must be defined by CMakeLists.txt — \
point it at the sibling rune-protocol/conformance directory"
#endif

namespace cc = rune::client::codec;

// ---------------------------------------------------------------------------
// Canonical values from conformance/manifest.md
// ---------------------------------------------------------------------------
static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static constexpr uint16_t kSchemaId     = cc::kSchemaId;          // 120
static constexpr uint16_t kVersion      = cc::kProtocolVersion;   // 1

// Ping/Pong echo payload — binary; contains embedded NUL bytes and 0xFF.
static constexpr uint8_t kEchoBytes[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00};
static constexpr std::size_t kEchoLen = sizeof(kEchoBytes);

// ---------------------------------------------------------------------------
// Helper: read an entire golden .bin file; fatal-fail if missing or empty.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> readVector(const char* name) {
    std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << "Cannot open golden vector: " << path
        << "\n  Check that rune-protocol sibling repo is present and RUNE_CONFORMANCE_DIR is set.";
    if (!f) return {};

    std::fseek(f, 0, SEEK_END);
    const long sz = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    EXPECT_GT(sz, 0) << "Empty vector file: " << path;
    if (sz <= 0) { std::fclose(f); return {}; }

    std::vector<uint8_t> buf(static_cast<std::size_t>(sz));
    const std::size_t got = std::fread(buf.data(), 1, buf.size(), f);
    std::fclose(f);
    EXPECT_EQ(got, buf.size()) << "Short read: " << path;
    return buf;
}

// ===========================================================================
// 1. connect_request.bin  (67 bytes)
//    ConnectRequest{ egressStreamId=1001, clientVersion=1,
//                   egressChannel="aeron:udp?endpoint=10.0.0.5:40456" }
// ===========================================================================
TEST(ClientCodecConformance, ConnectRequest) {
    const auto golden = readVector("connect_request.bin");
    ASSERT_EQ(golden.size(), 67u);

    // --- Frame check ---
    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Connect));
    EXPECT_EQ(fc.version,    kVersion);

    // --- Peek template ---
    auto tid = cc::peekTemplate(golden.data(), golden.size());
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(cc::TemplateId::Connect));

    // --- Decode-golden ---
    auto view = cc::decodeConnectRequest(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->egressStreamId, 1001);
    EXPECT_EQ(view->clientVersion,  1);
    EXPECT_EQ(view->egressChannel,  "aeron:udp?endpoint=10.0.0.5:40456");

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeConnectRequest(
        enc.data(), enc.size(),
        "aeron:udp?endpoint=10.0.0.5:40456", 1001, 1,
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 67u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_request.bin";
}

// ===========================================================================
// 2. connect_request_empty_channel.bin  (34 bytes)
//    ConnectRequest{ egressStreamId=1001, clientVersion=1, egressChannel="" }
// ===========================================================================
TEST(ClientCodecConformance, ConnectRequestEmptyChannel) {
    const auto golden = readVector("connect_request_empty_channel.bin");
    ASSERT_EQ(golden.size(), 34u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Connect));

    auto view = cc::decodeConnectRequest(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->egressStreamId, 1001);
    EXPECT_EQ(view->clientVersion,  1);
    EXPECT_EQ(view->egressChannel.size(), 0u);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeConnectRequest(
        enc.data(), enc.size(),
        "", 1001, 1,
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 34u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_request_empty_channel.bin";
}

// ===========================================================================
// 3. connect_response.bin  (38 bytes)
//    ConnectResponse{ code=0(OK), leaderMemberId=2,
//                    assignedSessionId=0x00000000DEADBEEF }
// ===========================================================================
TEST(ClientCodecConformance, ConnectResponse) {
    const auto golden = readVector("connect_response.bin");
    ASSERT_EQ(golden.size(), 38u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::ConnectResponse));

    auto view = cc::decodeConnectResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,         kSessionId);
    EXPECT_EQ(view->correlationId,     kCorrelationId);
    EXPECT_EQ(view->code,              static_cast<int16_t>(cc::ErrorCode::OK));
    EXPECT_EQ(view->leaderMemberId,    2);
    EXPECT_EQ(view->assignedSessionId, static_cast<int64_t>(0x00000000DEADBEEFULL));

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeConnectResponse(
        enc.data(), enc.size(),
        static_cast<int16_t>(cc::ErrorCode::OK),
        2,
        static_cast<int64_t>(0x00000000DEADBEEFULL),
        kSessionId, kCorrelationId, /*brokerVersion=*/-1);  // the golden v0 (38-byte) form
    ASSERT_EQ(n, 38u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_response.bin";
    EXPECT_EQ(view->brokerVersion, 0) << "the v0 frame advertises no capability";
}

// CG round 4 (A-MAJOR-2): the 42-byte golden every broker >= 3 sends.
TEST(ClientCodecConformance, ConnectResponseV3Regression) {
    const auto golden = readVector("connect_response_v3.bin");
    ASSERT_EQ(golden.size(), 42u);
    const auto view = cc::decodeConnectResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->brokerVersion, 3);
    EXPECT_EQ(view->leaderMemberId, 2);
    EXPECT_EQ(view->assignedSessionId, static_cast<int64_t>(0x00000000DEADBEEFULL));
    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeConnectResponse(enc.data(), enc.size(),
                                                    static_cast<int16_t>(cc::ErrorCode::OK), 2,
                                                    static_cast<int64_t>(0x00000000DEADBEEFULL),
                                                    kSessionId, kCorrelationId, /*brokerVersion=*/3);
    ASSERT_EQ(n, 42u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0) << "differs from golden connect_response_v3.bin";
}

// MAJOR-2 (CG round 3): the current form appends brokerVersion; the first 38
// bytes (bar blockLength) are the v0 form, and it decodes to version 3.
TEST(ClientCodecConformance, ConnectResponseCarriesTheBrokerVersionRegression) {
    const auto golden = readVector("connect_response.bin");
    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeConnectResponse(enc.data(), enc.size(),
                                                    static_cast<int16_t>(cc::ErrorCode::OK), 2,
                                                    static_cast<int64_t>(0x00000000DEADBEEFULL),
                                                    kSessionId, kCorrelationId);
    ASSERT_EQ(n, 42u);
    EXPECT_EQ(std::memcmp(enc.data() + 2, golden.data() + 2, 36), 0);
    const auto view = cc::decodeConnectResponse(enc.data(), n);
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->brokerVersion, rune::protocol::client::kBrokerVersion);
    EXPECT_EQ(view->assignedSessionId, static_cast<int64_t>(0x00000000DEADBEEFULL));
}

// ===========================================================================
// 4. connect_response_redirect.bin  (38 bytes)
//    ConnectResponse{ code=3(Redirect), leaderMemberId=1, assignedSessionId=0 }
// ===========================================================================
TEST(ClientCodecConformance, ConnectResponseRedirect) {
    const auto golden = readVector("connect_response_redirect.bin");
    ASSERT_EQ(golden.size(), 38u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::ConnectResponse));

    auto view = cc::decodeConnectResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,         kSessionId);
    EXPECT_EQ(view->correlationId,     kCorrelationId);
    EXPECT_EQ(view->code,              static_cast<int16_t>(cc::ErrorCode::Redirect));
    EXPECT_EQ(view->leaderMemberId,    1);
    EXPECT_EQ(view->assignedSessionId, 0LL);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeConnectResponse(
        enc.data(), enc.size(),
        static_cast<int16_t>(cc::ErrorCode::Redirect),
        1,
        0LL,
        kSessionId, kCorrelationId, /*brokerVersion=*/-1);
    ASSERT_EQ(n, 38u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_response_redirect.bin";
}

// ===========================================================================
// 5. keepalive.bin  (24 bytes)
//    Keepalive{} — header only, no body fields
// ===========================================================================
TEST(ClientCodecConformance, Keepalive) {
    const auto golden = readVector("keepalive.bin");
    ASSERT_EQ(golden.size(), 24u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Keepalive));

    auto view = cc::decodeKeepalive(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);

    std::array<uint8_t, 32> enc{};
    const std::size_t n = cc::encodeKeepalive(enc.data(), enc.size(),
                                               kSessionId, kCorrelationId);
    ASSERT_EQ(n, 24u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden keepalive.bin";
}

// ===========================================================================
// 6. disconnect.bin  (26 bytes)
//    Disconnect{ reason=-1 }
// ===========================================================================
TEST(ClientCodecConformance, Disconnect) {
    const auto golden = readVector("disconnect.bin");
    ASSERT_EQ(golden.size(), 26u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Disconnect));

    auto view = cc::decodeDisconnect(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    EXPECT_EQ(view->reason,        static_cast<int16_t>(-1));

    std::array<uint8_t, 32> enc{};
    const std::size_t n = cc::encodeDisconnect(enc.data(), enc.size(),
                                                static_cast<int16_t>(-1),
                                                kSessionId, kCorrelationId);
    ASSERT_EQ(n, 26u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden disconnect.bin";
}

// ===========================================================================
// 7. error_response.bin  (53 bytes)
//    ErrorResponse{ code=4(SessionExpired),
//                  message="session 0x1122... expired" (25 bytes) }
// ===========================================================================
TEST(ClientCodecConformance, ErrorResponse) {
    static constexpr char kMsg[] = "session 0x1122... expired";
    static constexpr std::size_t kMsgLen = sizeof(kMsg) - 1; // 25

    const auto golden = readVector("error_response.bin");
    ASSERT_EQ(golden.size(), 53u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::ErrorResponse));

    auto view = cc::decodeErrorResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    EXPECT_EQ(view->code,          static_cast<int16_t>(cc::ErrorCode::SessionExpired));
    ASSERT_EQ(view->message.size(), kMsgLen);
    EXPECT_EQ(view->message, std::string_view(kMsg, kMsgLen));

    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeErrorResponse(
        enc.data(), enc.size(),
        static_cast<int16_t>(cc::ErrorCode::SessionExpired),
        std::string_view(kMsg, kMsgLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 53u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden error_response.bin";
}

// ===========================================================================
// 8. error_response_empty.bin  (28 bytes)
//    ErrorResponse{ code=6(Internal), message="" }
// ===========================================================================
TEST(ClientCodecConformance, ErrorResponseEmpty) {
    const auto golden = readVector("error_response_empty.bin");
    ASSERT_EQ(golden.size(), 28u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::ErrorResponse));

    auto view = cc::decodeErrorResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    EXPECT_EQ(view->code,          static_cast<int16_t>(cc::ErrorCode::Internal));
    EXPECT_EQ(view->message.size(), 0u);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeErrorResponse(
        enc.data(), enc.size(),
        static_cast<int16_t>(cc::ErrorCode::Internal),
        std::string_view{},
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 28u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden error_response_empty.bin";
}

// ===========================================================================
// 9. ping.bin  (32 bytes)
//    Ping{ echo = {0x00,0x01,0x02,0xFF,0xFE,0x00} }
//    Binary payload — not UTF-8; embedded NUL and 0xFF intentional.
// ===========================================================================
TEST(ClientCodecConformance, Ping) {
    const auto golden = readVector("ping.bin");
    ASSERT_EQ(golden.size(), 32u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Ping));

    auto view = cc::decodePing(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    ASSERT_EQ(view->echo.size(), kEchoLen);
    EXPECT_EQ(std::memcmp(view->echo.data(), kEchoBytes, kEchoLen), 0)
        << "Decoded Ping echo bytes mismatch";

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodePing(
        enc.data(), enc.size(),
        std::string_view(reinterpret_cast<const char*>(kEchoBytes), kEchoLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden ping.bin";
}

// ===========================================================================
// 10. pong.bin  (32 bytes)
//     Pong{ echo = same bytes as ping: {0x00,0x01,0x02,0xFF,0xFE,0x00} }
// ===========================================================================
TEST(ClientCodecConformance, Pong) {
    const auto golden = readVector("pong.bin");
    ASSERT_EQ(golden.size(), 32u);

    auto fc = cc::checkFrame(golden.data(), golden.size());
    EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Pong));

    auto view = cc::decodePong(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    ASSERT_EQ(view->echo.size(), kEchoLen);
    EXPECT_EQ(std::memcmp(view->echo.data(), kEchoBytes, kEchoLen), 0)
        << "Decoded Pong echo bytes mismatch";

    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodePong(
        enc.data(), enc.size(),
        std::string_view(reinterpret_cast<const char*>(kEchoBytes), kEchoLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden pong.bin";
}

// ===========================================================================
// Session-flow test
//
// Models a real client session at the wire level through the client adapters:
//   ConnectRequest → ConnectResponse → Keepalive → Ping → Pong → Disconnect
//   Expected templateId sequence: [1, 2, 3, 6, 7, 4]
//
// Also verifies that decoded fields from each frame match manifest values.
// ===========================================================================
TEST(ClientCodecSessionFlow, RoundTripSequence) {
    // Step 1 — ConnectRequest (client sends)
    {
        const auto buf = readVector("connect_request.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Connect));
        auto v = cc::decodeConnectRequest(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->sessionId,      kSessionId);
        EXPECT_EQ(v->correlationId,  kCorrelationId);
        EXPECT_EQ(v->egressStreamId, 1001);
        EXPECT_EQ(v->clientVersion,  1);
    }

    // Step 2 — ConnectResponse (server replies)
    {
        const auto buf = readVector("connect_response.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::ConnectResponse));
        auto v = cc::decodeConnectResponse(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->code,              static_cast<int16_t>(cc::ErrorCode::OK));
        EXPECT_EQ(v->leaderMemberId,    2);
        EXPECT_EQ(v->assignedSessionId, static_cast<int64_t>(0x00000000DEADBEEFULL));
    }

    // Step 3 — Keepalive (client heartbeats)
    {
        const auto buf = readVector("keepalive.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Keepalive));
        auto v = cc::decodeKeepalive(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->sessionId,     kSessionId);
        EXPECT_EQ(v->correlationId, kCorrelationId);
    }

    // Step 4 — Ping (client sends)
    {
        const auto buf = readVector("ping.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Ping));
        auto v = cc::decodePing(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        ASSERT_EQ(v->echo.size(), kEchoLen);
        EXPECT_EQ(std::memcmp(v->echo.data(), kEchoBytes, kEchoLen), 0);
    }

    // Step 5 — Pong (server echoes back)
    {
        const auto buf = readVector("pong.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Pong));
        auto v = cc::decodePong(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        ASSERT_EQ(v->echo.size(), kEchoLen);
        EXPECT_EQ(std::memcmp(v->echo.data(), kEchoBytes, kEchoLen), 0);
    }

    // Step 6 — Disconnect (client closes)
    {
        const auto buf = readVector("disconnect.bin");
        ASSERT_FALSE(buf.empty());
        auto fc = cc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status,     cc::FrameStatus::Ok);
        EXPECT_EQ(fc.templateId, static_cast<uint16_t>(cc::TemplateId::Disconnect));
        auto v = cc::decodeDisconnect(buf.data(), buf.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->reason, static_cast<int16_t>(-1));
    }

    // Assert the exact templateId sequence [1, 2, 3, 6, 7, 4]
    struct FlowFrame {
        const char* filename;
        uint16_t    expectedTemplateId;
    };
    static constexpr FlowFrame kFlow[] = {
        { "connect_request.bin",  static_cast<uint16_t>(cc::TemplateId::Connect)         },  // 1
        { "connect_response.bin", static_cast<uint16_t>(cc::TemplateId::ConnectResponse) },  // 2
        { "keepalive.bin",        static_cast<uint16_t>(cc::TemplateId::Keepalive)       },  // 3
        { "ping.bin",             static_cast<uint16_t>(cc::TemplateId::Ping)            },  // 6
        { "pong.bin",             static_cast<uint16_t>(cc::TemplateId::Pong)            },  // 7
        { "disconnect.bin",       static_cast<uint16_t>(cc::TemplateId::Disconnect)      },  // 4
    };
    constexpr std::size_t kFlowLen = sizeof(kFlow) / sizeof(kFlow[0]);
    ASSERT_EQ(kFlowLen, 6u);

    constexpr uint16_t kExpectedSeq[kFlowLen] = {1, 2, 3, 6, 7, 4};
    for (std::size_t i = 0; i < kFlowLen; ++i) {
        const auto buf = readVector(kFlow[i].filename);
        ASSERT_FALSE(buf.empty()) << "Could not read " << kFlow[i].filename;
        auto tid = cc::peekTemplate(buf.data(), buf.size());
        ASSERT_TRUE(tid.has_value()) << "peekTemplate failed for " << kFlow[i].filename;
        EXPECT_EQ(*tid, kExpectedSeq[i])
            << "TemplateId mismatch at flow step " << i
            << " (" << kFlow[i].filename << ")";
        EXPECT_EQ(*tid, kFlow[i].expectedTemplateId)
            << "TemplateId mismatch (enum) at flow step " << i;
    }
}

// ===========================================================================
// v16 ConnectRequest: the buffer bound must be the codec's own encodedSize() (crypto block + flag byte at >= 16)
// ===========================================================================
TEST(ClientCodecConnectRegression, ConnectRequestCodecV16BufferRegression) {
    const std::string_view channel = "aeron:udp?endpoint=localhost:40200";
    const std::size_t oldFormula = 24u + 8u + 2u + channel.size();   // pre-v16 size: too small for a v16 frame
    {
        std::vector<uint8_t> buf(oldFormula + 32u, 0xAB);
        const std::size_t n = cc::encodeConnectRequest(buf.data(), oldFormula, channel, 1001, 16, kSessionId,
                                                       kCorrelationId);
        EXPECT_EQ(n, 0u) << "a v16 frame does not fit the pre-v16 capacity";
        for (std::size_t i = 0; i < buf.size(); ++i) ASSERT_EQ(buf[i], 0xAB) << "wrote at " << i;
    }
    {
        const std::size_t need = oldFormula + 2u + 4u + 2u + 1u;
        std::vector<uint8_t> buf(need + 32u, 0xAB);
        const std::size_t n = cc::encodeConnectRequest(buf.data(), need, channel, 1001, 16, kSessionId,
                                                       kCorrelationId);
        ASSERT_EQ(n, need);
        for (std::size_t i = need; i < buf.size(); ++i) ASSERT_EQ(buf[i], 0xAB) << "guard overwritten at " << i;
        const auto view = cc::decodeConnectRequest(buf.data(), n);
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(view->clientVersion, 16);
        EXPECT_EQ(view->egressChannel, channel);
    }
    {
        // pre-v16 capacity is still exact
        std::vector<uint8_t> buf(oldFormula, 0u);
        EXPECT_EQ(cc::encodeConnectRequest(buf.data(), oldFormula, channel, 1001, 15, kSessionId, kCorrelationId),
                  oldFormula);
    }
    {
        // the crypto encoder sizes the vector from encodedSize(): exact at v16 with and without a flag
        const uint8_t cipher = 2;
        std::vector<uint8_t> buf;
        const std::size_t n = cc::encodeConnectRequestCrypto(buf, channel, 1001, 16, kSessionId, kCorrelationId,
                                                             std::span<const uint8_t>(&cipher, 1), "hs", "");
        EXPECT_EQ(buf.size(), n);
        EXPECT_EQ(n, oldFormula + 2u + 1u + 4u + 2u + 2u + 1u);
        const auto dec = rune::protocol::client::ConnectRequest::decode(buf.data(), n);
        ASSERT_TRUE(dec.has_value());
        EXPECT_FALSE(dec->responseCorrelated);
        EXPECT_EQ(dec->handshakeBytes, "hs");
        EXPECT_EQ(dec->egressChannel, channel);
    }
}
