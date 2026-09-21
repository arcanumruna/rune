// tests/protocol/client/ConformanceTest.cpp
//
// CP-5 / PB-3 conformance harness — golden vector tests.
//
// For each committed .bin vector the harness asserts BOTH:
//   1. Decode-golden:  read file bytes → decode → every field equals expected.
//   2. Encode-golden:  build message from expected values → encode → bytes identical
//                      to the committed .bin (pins the wire format).
//   3. Header check:   templateId / schemaId / version / sessionId / correlationId.
//
// Session-flow tests:
//   - Control flow: [Connect, ConnectResponse, Keepalive, Ping, Pong, Disconnect]
//   - Produce→records flow: Produce → ProduceResponse → Subscribe → Records → Unsubscribe
//     Uses checkFrame + peekTemplateId for each step; asserts Ok + correct templateId.
//
// RUNE_CONFORMANCE_DIR is injected as a compile-time string by CMakeLists.txt so
// the test locates vectors regardless of the working directory ctest uses.

#include <gtest/gtest.h>

#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

// ---------------------------------------------------------------------------
// Compile-time path root (injected via -DRUNE_CONFORMANCE_DIR="...")
// ---------------------------------------------------------------------------
#ifndef RUNE_CONFORMANCE_DIR
#  error "RUNE_CONFORMANCE_DIR must be defined by CMakeLists.txt"
#endif

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Common test values — must match conformance/manifest.md exactly.
// ---------------------------------------------------------------------------
static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static constexpr uint16_t kSchemaId     = rpc::kSchemaId;          // 120
static constexpr uint16_t kVersion      = rpc::kProtocolVersion;   // 1

// Ping/Pong echo payload (binary, contains embedded NUL and 0xFF).
static constexpr uint8_t kEchoBytes[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00};
static constexpr std::size_t kEchoLen = sizeof(kEchoBytes);

// ---------------------------------------------------------------------------
// Helper: read entire file into a vector; fatal-fail on error.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> readVector(const char* name) {
    std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << "Cannot open golden vector: " << path;
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

// ---------------------------------------------------------------------------
// Helper: assert header fields on the decoded header from a raw buffer.
// ---------------------------------------------------------------------------
static void assertHeader(const std::vector<uint8_t>& buf,
                         uint16_t expectedTemplateId,
                         uint16_t expectedBlockLength) {
    auto hdrOpt = rpc::MessageHeader::decode(buf.data(), buf.size());
    ASSERT_TRUE(hdrOpt.has_value()) << "MessageHeader::decode failed";
    EXPECT_EQ(hdrOpt->templateId,    expectedTemplateId);
    EXPECT_EQ(hdrOpt->blockLength,   expectedBlockLength);
    EXPECT_EQ(hdrOpt->schemaId,      kSchemaId);
    EXPECT_EQ(hdrOpt->version,       kVersion);
    EXPECT_EQ(hdrOpt->sessionId,     kSessionId);
    EXPECT_EQ(hdrOpt->correlationId, kCorrelationId);
}

// ===========================================================================
// 1. connect_request.bin
//    ConnectRequest{ egressStreamId=1001, clientVersion=1,
//                   egressChannel="aeron:udp?endpoint=10.0.0.5:40456" }
//    Expected byte length: 24 + 8 + 2 + 33 = 67
// ===========================================================================
TEST(ConformanceGolden, ConnectRequest) {
    const auto golden = readVector("connect_request.bin");
    ASSERT_EQ(golden.size(), 67u);

    // --- Header ---
    assertHeader(golden, rpc::ConnectRequest::TEMPLATE_ID, rpc::ConnectRequest::BODY_FIXED);

    // --- Decode-golden ---
    auto msgOpt = rpc::ConnectRequest::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->egressStreamId, 1001);
    EXPECT_EQ(msgOpt->clientVersion,  1);
    EXPECT_EQ(msgOpt->egressChannel,  "aeron:udp?endpoint=10.0.0.5:40456");

    // --- Encode-golden ---
    rpc::ConnectRequest msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.egressStreamId = 1001;
    msg.clientVersion  = 1;
    msg.egressChannel  = "aeron:udp?endpoint=10.0.0.5:40456";

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 67u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_request.bin";
}

// ===========================================================================
// 2. connect_request_empty_channel.bin
//    ConnectRequest{ egressStreamId=1001, clientVersion=1, egressChannel="" }
//    Expected byte length: 24 + 8 + 2 + 0 = 34
// ===========================================================================
TEST(ConformanceGolden, ConnectRequestEmptyChannel) {
    const auto golden = readVector("connect_request_empty_channel.bin");
    ASSERT_EQ(golden.size(), 34u);

    assertHeader(golden, rpc::ConnectRequest::TEMPLATE_ID, rpc::ConnectRequest::BODY_FIXED);

    auto msgOpt = rpc::ConnectRequest::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->egressStreamId, 1001);
    EXPECT_EQ(msgOpt->clientVersion,  1);
    EXPECT_EQ(msgOpt->egressChannel.size(), 0u);

    rpc::ConnectRequest msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.egressStreamId = 1001;
    msg.clientVersion  = 1;
    msg.egressChannel  = "";

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 34u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_request_empty_channel.bin";
}

// ===========================================================================
// 3. connect_response.bin
//    ConnectResponse{ code=0(OK), leaderMemberId=2,
//                    assignedSessionId=0x00000000DEADBEEF }
//    Expected byte length: 24 + 14 = 38
// ===========================================================================
TEST(ConformanceGolden, ConnectResponse) {
    const auto golden = readVector("connect_response.bin");
    ASSERT_EQ(golden.size(), 38u);

    assertHeader(golden, rpc::ConnectResponse::TEMPLATE_ID, rpc::ConnectResponse::BODY_FIXED);

    auto msgOpt = rpc::ConnectResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,         kSessionId);
    EXPECT_EQ(msgOpt->correlationId,     kCorrelationId);
    EXPECT_EQ(msgOpt->code,              static_cast<int16_t>(rpc::ErrorCode::OK));
    EXPECT_EQ(msgOpt->leaderMemberId,    2);
    EXPECT_EQ(msgOpt->assignedSessionId, static_cast<int64_t>(0x00000000DEADBEEFULL));

    rpc::ConnectResponse msg;
    msg.sessionId         = kSessionId;
    msg.correlationId     = kCorrelationId;
    msg.code              = static_cast<int16_t>(rpc::ErrorCode::OK);
    msg.leaderMemberId    = 2;
    msg.assignedSessionId = static_cast<int64_t>(0x00000000DEADBEEFULL);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 38u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_response.bin";
}

// ===========================================================================
// 4. connect_response_redirect.bin
//    ConnectResponse{ code=3(Redirect), leaderMemberId=1, assignedSessionId=0 }
//    Expected byte length: 38
// ===========================================================================
TEST(ConformanceGolden, ConnectResponseRedirect) {
    const auto golden = readVector("connect_response_redirect.bin");
    ASSERT_EQ(golden.size(), 38u);

    assertHeader(golden, rpc::ConnectResponse::TEMPLATE_ID, rpc::ConnectResponse::BODY_FIXED);

    auto msgOpt = rpc::ConnectResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,         kSessionId);
    EXPECT_EQ(msgOpt->correlationId,     kCorrelationId);
    EXPECT_EQ(msgOpt->code,              static_cast<int16_t>(rpc::ErrorCode::Redirect));
    EXPECT_EQ(msgOpt->leaderMemberId,    1);
    EXPECT_EQ(msgOpt->assignedSessionId, 0LL);

    rpc::ConnectResponse msg;
    msg.sessionId         = kSessionId;
    msg.correlationId     = kCorrelationId;
    msg.code              = static_cast<int16_t>(rpc::ErrorCode::Redirect);
    msg.leaderMemberId    = 1;
    msg.assignedSessionId = 0;

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 38u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden connect_response_redirect.bin";
}

// ===========================================================================
// 5. keepalive.bin
//    Keepalive{} (header only, BODY_FIXED = 0)
//    Expected byte length: 24
// ===========================================================================
TEST(ConformanceGolden, Keepalive) {
    const auto golden = readVector("keepalive.bin");
    ASSERT_EQ(golden.size(), 24u);

    assertHeader(golden, rpc::Keepalive::TEMPLATE_ID, rpc::Keepalive::BODY_FIXED);

    auto msgOpt = rpc::Keepalive::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);

    rpc::Keepalive msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;

    std::array<uint8_t, 32> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 24u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden keepalive.bin";
}

// ===========================================================================
// 6. disconnect.bin
//    Disconnect{ reason=-1 }
//    Expected byte length: 24 + 2 = 26
// ===========================================================================
TEST(ConformanceGolden, Disconnect) {
    const auto golden = readVector("disconnect.bin");
    ASSERT_EQ(golden.size(), 26u);

    assertHeader(golden, rpc::Disconnect::TEMPLATE_ID, rpc::Disconnect::BODY_FIXED);

    auto msgOpt = rpc::Disconnect::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->reason,        static_cast<int16_t>(-1));

    rpc::Disconnect msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.reason        = -1;

    std::array<uint8_t, 32> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 26u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden disconnect.bin";
}

// ===========================================================================
// 7. error_response.bin
//    ErrorResponse{ code=4(SessionExpired), message="session 0x1122... expired" }
//    message length = 25 bytes
//    Expected byte length: 24 + 2 + 2 + 25 = 53
// ===========================================================================
TEST(ConformanceGolden, ErrorResponse) {
    static constexpr char kMsg[] = "session 0x1122... expired";
    static constexpr std::size_t kMsgLen = sizeof(kMsg) - 1; // 25

    const auto golden = readVector("error_response.bin");
    ASSERT_EQ(golden.size(), 53u);

    assertHeader(golden, rpc::ErrorResponse::TEMPLATE_ID, rpc::ErrorResponse::BODY_FIXED);

    auto msgOpt = rpc::ErrorResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->code,          static_cast<int16_t>(rpc::ErrorCode::SessionExpired));
    ASSERT_EQ(msgOpt->message.size(), kMsgLen);
    EXPECT_EQ(msgOpt->message, std::string_view(kMsg, kMsgLen));

    rpc::ErrorResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.code          = static_cast<int16_t>(rpc::ErrorCode::SessionExpired);
    msg.message       = std::string_view(kMsg, kMsgLen);

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 53u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden error_response.bin";
}

// ===========================================================================
// 8. error_response_empty.bin
//    ErrorResponse{ code=6(Internal), message="" }
//    Expected byte length: 24 + 2 + 2 + 0 = 28
// ===========================================================================
TEST(ConformanceGolden, ErrorResponseEmpty) {
    const auto golden = readVector("error_response_empty.bin");
    ASSERT_EQ(golden.size(), 28u);

    assertHeader(golden, rpc::ErrorResponse::TEMPLATE_ID, rpc::ErrorResponse::BODY_FIXED);

    auto msgOpt = rpc::ErrorResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->code,          static_cast<int16_t>(rpc::ErrorCode::Internal));
    EXPECT_EQ(msgOpt->message.size(), 0u);

    rpc::ErrorResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.code          = static_cast<int16_t>(rpc::ErrorCode::Internal);
    msg.message       = "";

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 28u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden error_response_empty.bin";
}

// ===========================================================================
// 9. ping.bin
//    Ping{ echo = {0x00,0x01,0x02,0xFF,0xFE,0x00} }
//    Expected byte length: 24 + 2 + 6 = 32
// ===========================================================================
TEST(ConformanceGolden, Ping) {
    const auto golden = readVector("ping.bin");
    ASSERT_EQ(golden.size(), 32u);

    assertHeader(golden, rpc::Ping::TEMPLATE_ID, rpc::Ping::BODY_FIXED);

    auto msgOpt = rpc::Ping::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    ASSERT_EQ(msgOpt->echo.size(), kEchoLen);
    EXPECT_EQ(std::memcmp(msgOpt->echo.data(), kEchoBytes, kEchoLen), 0)
        << "Decoded echo bytes mismatch";

    rpc::Ping msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.echo          = std::string_view(reinterpret_cast<const char*>(kEchoBytes), kEchoLen);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden ping.bin";
}

// ===========================================================================
// 10. pong.bin
//     Pong{ echo = same bytes as ping: {0x00,0x01,0x02,0xFF,0xFE,0x00} }
//     Expected byte length: 24 + 2 + 6 = 32
// ===========================================================================
TEST(ConformanceGolden, Pong) {
    const auto golden = readVector("pong.bin");
    ASSERT_EQ(golden.size(), 32u);

    assertHeader(golden, rpc::Pong::TEMPLATE_ID, rpc::Pong::BODY_FIXED);

    auto msgOpt = rpc::Pong::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    ASSERT_EQ(msgOpt->echo.size(), kEchoLen);
    EXPECT_EQ(std::memcmp(msgOpt->echo.data(), kEchoBytes, kEchoLen), 0)
        << "Decoded echo bytes mismatch";

    rpc::Pong msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.echo          = std::string_view(reinterpret_cast<const char*>(kEchoBytes), kEchoLen);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden pong.bin";
}

// ===========================================================================
// PB-1 Golden conformance: Produce / ProduceResponse vectors
// ===========================================================================

// Golden record batch shared by the PB-1 produce vectors.
static constexpr uint8_t kProduceBatch[] = {
    0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
};
static constexpr uint8_t kProduceKey[] = {0x41, 0x42, 0x43}; // "ABC"

// ===========================================================================
// 11. produce_explicit_partition.bin — 60 bytes (TX-1: BODY_FIXED expanded to 24)
//     Produce{ topicId=1001, partition=7, partitionKnown=1, acks=0, key=empty,
//              recordBatch={DE AD BE EF 00 FF 01 02},
//              producerId=-1 (sentinel), producerEpoch=0, baseSequence=-1 }
// ===========================================================================
TEST(ConformanceGolden, ProduceExplicitPartition) {
    const auto golden = readVector("produce_explicit_partition.bin");
    ASSERT_EQ(golden.size(), 60u);

    assertHeader(golden, rpc::Produce::TEMPLATE_ID, rpc::Produce::BODY_FIXED);

    auto msgOpt = rpc::Produce::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,        1001);
    EXPECT_EQ(msgOpt->partition,      7);
    EXPECT_EQ(msgOpt->partitionKnown, 1u);
    EXPECT_EQ(msgOpt->key.size(),     0u);
    ASSERT_EQ(msgOpt->recordBatch.size(), sizeof(kProduceBatch));
    EXPECT_EQ(std::memcmp(msgOpt->recordBatch.data(), kProduceBatch, sizeof(kProduceBatch)), 0);

    rpc::Produce msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 1001;
    msg.partition      = 7;
    msg.partitionKnown = 1;
    msg.key            = std::span<const uint8_t>{};
    msg.recordBatch    = std::span<const uint8_t>(kProduceBatch, sizeof(kProduceBatch));

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 60u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_explicit_partition.bin";
}

// ===========================================================================
// 12. produce_key_only.bin — 63 bytes (TX-1: BODY_FIXED expanded to 24)
//     Produce{ topicId=2002, partition=0, partitionKnown=0, acks=0, key="ABC",
//              recordBatch={DE AD BE EF 00 FF 01 02},
//              producerId=-1 (sentinel), producerEpoch=0, baseSequence=-1 }
// ===========================================================================
TEST(ConformanceGolden, ProduceKeyOnly) {
    const auto golden = readVector("produce_key_only.bin");
    ASSERT_EQ(golden.size(), 63u);

    assertHeader(golden, rpc::Produce::TEMPLATE_ID, rpc::Produce::BODY_FIXED);

    auto msgOpt = rpc::Produce::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,        2002);
    EXPECT_EQ(msgOpt->partition,      0);
    EXPECT_EQ(msgOpt->partitionKnown, 0u);
    ASSERT_EQ(msgOpt->key.size(),     sizeof(kProduceKey));
    EXPECT_EQ(std::memcmp(msgOpt->key.data(), kProduceKey, sizeof(kProduceKey)), 0);
    ASSERT_EQ(msgOpt->recordBatch.size(), sizeof(kProduceBatch));
    EXPECT_EQ(std::memcmp(msgOpt->recordBatch.data(), kProduceBatch, sizeof(kProduceBatch)), 0);

    rpc::Produce msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 2002;
    msg.partition      = 0;
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(kProduceKey, sizeof(kProduceKey));
    msg.recordBatch    = std::span<const uint8_t>(kProduceBatch, sizeof(kProduceBatch));

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 63u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_key_only.bin";
}

// ===========================================================================
// 13. produce_response.bin — 38 bytes
//     ProduceResponse{ errorCode=0(OK), partition=7, baseOffset=123456 }
// ===========================================================================
TEST(ConformanceGolden, ProduceResponse) {
    const auto golden = readVector("produce_response.bin");
    ASSERT_EQ(golden.size(), 38u);

    assertHeader(golden, rpc::ProduceResponse::TEMPLATE_ID, rpc::ProduceResponse::BODY_FIXED);

    auto msgOpt = rpc::ProduceResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->errorCode,     static_cast<int16_t>(rpc::ErrorCode::OK));
    EXPECT_EQ(msgOpt->partition,     7);
    EXPECT_EQ(msgOpt->baseOffset,    123456LL);

    rpc::ProduceResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = kCorrelationId;
    msg.errorCode     = static_cast<int16_t>(rpc::ErrorCode::OK);
    msg.partition     = 7;
    msg.baseOffset    = 123456LL;

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 38u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_response.bin";
}

// ===========================================================================
// PB-2 Golden conformance: Subscribe / Records / Unsubscribe vectors
// ===========================================================================

// Golden batch bytes shared by the PB-2 records_push vector.
static constexpr uint8_t kGoldenBatch[] = {
    0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02
};

// ===========================================================================
// 14. subscribe_explicit.bin
//     Subscribe{ topicId=3001, partition=5, partitionKnown=1, fromOffset=9999,
//                isolation=1, key=empty }
//     Expected byte length: 24 + 18 + 2 + 0 = 44
// ===========================================================================
TEST(ConformanceGolden, SubscribeExplicit) {
    const auto golden = readVector("subscribe_explicit.bin");
    ASSERT_EQ(golden.size(), 44u);

    assertHeader(golden, rpc::Subscribe::TEMPLATE_ID, rpc::Subscribe::BODY_FIXED);

    auto msgOpt = rpc::Subscribe::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,        3001);
    EXPECT_EQ(msgOpt->partition,      5);
    EXPECT_EQ(msgOpt->partitionKnown, 1u);
    EXPECT_EQ(msgOpt->fromOffset,     9999LL);
    EXPECT_EQ(msgOpt->isolation,      1u);
    EXPECT_EQ(msgOpt->key.size(),     0u);

    rpc::Subscribe msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 3001;
    msg.partition      = 5;
    msg.partitionKnown = 1;
    msg.fromOffset     = 9999LL;
    msg.isolation      = 1;
    msg.key            = std::span<const uint8_t>{};

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 44u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden subscribe_explicit.bin";
}

// ===========================================================================
// 15. subscribe_key_only.bin
//     Subscribe{ topicId=4004, partition=0, partitionKnown=0, fromOffset=0,
//                isolation=0, key="ABC" }
//     Expected byte length: 24 + 18 + 2 + 3 = 47
// ===========================================================================
TEST(ConformanceGolden, SubscribeKeyOnly) {
    static constexpr uint8_t kKey[] = {0x41, 0x42, 0x43}; // "ABC"

    const auto golden = readVector("subscribe_key_only.bin");
    ASSERT_EQ(golden.size(), 47u);

    assertHeader(golden, rpc::Subscribe::TEMPLATE_ID, rpc::Subscribe::BODY_FIXED);

    auto msgOpt = rpc::Subscribe::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,        4004);
    EXPECT_EQ(msgOpt->partition,      0);
    EXPECT_EQ(msgOpt->partitionKnown, 0u);
    EXPECT_EQ(msgOpt->fromOffset,     0LL);
    EXPECT_EQ(msgOpt->isolation,      0u);
    ASSERT_EQ(msgOpt->key.size(),     sizeof(kKey));
    EXPECT_EQ(std::memcmp(msgOpt->key.data(), kKey, sizeof(kKey)), 0);

    rpc::Subscribe msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 4004;
    msg.partition      = 0;
    msg.partitionKnown = 0;
    msg.fromOffset     = 0LL;
    msg.isolation      = 0;
    msg.key            = std::span<const uint8_t>(kKey, sizeof(kKey));

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 47u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden subscribe_key_only.bin";
}

// ===========================================================================
// 16. records_push.bin
//     Records{ subscriptionId=0x0102030405060708, topicId=3001, partition=5,
//              baseOffset=100000, recordBatch={0xDE,0xAD,0xBE,0xEF,0x00,0xFF,0x01,0x02} }
//     Expected byte length: 24 + 24 + 2 + 8 = 58
// ===========================================================================
TEST(ConformanceGolden, RecordsPush) {
    const auto golden = readVector("records_push.bin");
    ASSERT_EQ(golden.size(), 58u);

    assertHeader(golden, rpc::Records::TEMPLATE_ID, rpc::Records::BODY_FIXED);

    auto msgOpt = rpc::Records::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->subscriptionId, static_cast<int64_t>(0x0102030405060708ULL));
    EXPECT_EQ(msgOpt->topicId,        3001);
    EXPECT_EQ(msgOpt->partition,      5);
    EXPECT_EQ(msgOpt->baseOffset,     100000LL);
    ASSERT_EQ(msgOpt->recordBatch.size(), sizeof(kGoldenBatch));
    EXPECT_EQ(std::memcmp(msgOpt->recordBatch.data(), kGoldenBatch,
                           sizeof(kGoldenBatch)), 0);

    rpc::Records msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
    msg.topicId        = 3001;
    msg.partition      = 5;
    msg.baseOffset     = 100000LL;
    msg.recordBatch    = std::span<const uint8_t>(kGoldenBatch, sizeof(kGoldenBatch));

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 58u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden records_push.bin";
}

// ===========================================================================
// 17. unsubscribe.bin
//     Unsubscribe{ subscriptionId=0x0102030405060708 }
//     Expected byte length: 24 + 8 = 32
// ===========================================================================
TEST(ConformanceGolden, Unsubscribe) {
    const auto golden = readVector("unsubscribe.bin");
    ASSERT_EQ(golden.size(), 32u);

    assertHeader(golden, rpc::Unsubscribe::TEMPLATE_ID, rpc::Unsubscribe::BODY_FIXED);

    auto msgOpt = rpc::Unsubscribe::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->subscriptionId, static_cast<int64_t>(0x0102030405060708ULL));

    rpc::Unsubscribe msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden unsubscribe.bin";
}

// ===========================================================================
// Session-flow test
// Sequence: connect_request, connect_response, keepalive, ping, pong, disconnect
// Expected templateId sequence: [1, 2, 3, 6, 7, 4]
// ===========================================================================
TEST(ConformanceSessionFlow, TemplateIdSequence) {
    struct Frame {
        const char*  filename;
        uint16_t     expectedTemplateId;
    };

    static constexpr Frame kFlow[] = {
        { "connect_request.bin",   static_cast<uint16_t>(rpc::TemplateId::Connect)         },  // 1
        { "connect_response.bin",  static_cast<uint16_t>(rpc::TemplateId::ConnectResponse) },  // 2
        { "keepalive.bin",         static_cast<uint16_t>(rpc::TemplateId::Keepalive)       },  // 3
        { "ping.bin",              static_cast<uint16_t>(rpc::TemplateId::Ping)            },  // 6
        { "pong.bin",              static_cast<uint16_t>(rpc::TemplateId::Pong)            },  // 7
        { "disconnect.bin",        static_cast<uint16_t>(rpc::TemplateId::Disconnect)      },  // 4
    };

    for (const auto& frame : kFlow) {
        const auto buf = readVector(frame.filename);
        ASSERT_FALSE(buf.empty()) << "Could not read " << frame.filename;

        // Get templateId from MessageHeader::decode (CP-6 peekTemplateId not yet available)
        auto hdrOpt = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(hdrOpt.has_value()) << "Header decode failed for " << frame.filename;
        EXPECT_EQ(hdrOpt->templateId, frame.expectedTemplateId)
            << "TemplateId mismatch for " << frame.filename;
    }

    // Also verify the exact sequence as an array comparison.
    constexpr std::size_t kFlowLen = sizeof(kFlow) / sizeof(kFlow[0]);
    ASSERT_EQ(kFlowLen, 6u);

    uint16_t observedIds[kFlowLen];
    for (std::size_t i = 0; i < kFlowLen; ++i) {
        const auto buf = readVector(kFlow[i].filename);
        auto hdrOpt = rpc::MessageHeader::decode(buf.data(), buf.size());
        ASSERT_TRUE(hdrOpt.has_value());
        observedIds[i] = hdrOpt->templateId;
    }

    constexpr uint16_t kExpectedSequence[kFlowLen] = {1, 2, 3, 6, 7, 4};
    for (std::size_t i = 0; i < kFlowLen; ++i) {
        EXPECT_EQ(observedIds[i], kExpectedSequence[i])
            << "Session-flow templateId mismatch at index " << i;
    }
}

// ===========================================================================
// Produce→Records session-flow script (PB-3)
//
// Models a client produce + subscribe round-trip at the wire level:
//   Produce (16) → ProduceResponse (17) → Subscribe (18) → Records (20)
//   → Unsubscribe (21)
//
// Each frame is loaded from the golden .bin, round-tripped through
// checkFrame + peekTemplateId (must return Ok + correct id), and the
// encoded form is verified byte-exact against the golden file.
// Expected templateId sequence: [16, 17, 18, 20, 21]
// ===========================================================================
TEST(ConformanceSessionFlow, ProduceRecordsFlow) {
    struct Frame {
        const char* filename;
        uint16_t    expectedTemplateId;
    };

    static constexpr Frame kFlow[] = {
        { "produce_explicit_partition.bin", static_cast<uint16_t>(rpc::TemplateId::Produce)         },  // 16
        { "produce_response.bin",           static_cast<uint16_t>(rpc::TemplateId::ProduceResponse) },  // 17
        { "subscribe_explicit.bin",         static_cast<uint16_t>(rpc::TemplateId::Subscribe)       },  // 18
        { "records_push.bin",               static_cast<uint16_t>(rpc::TemplateId::Records)         },  // 20
        { "unsubscribe.bin",                static_cast<uint16_t>(rpc::TemplateId::Unsubscribe)     },  // 21
    };
    constexpr std::size_t kFlowLen = sizeof(kFlow) / sizeof(kFlow[0]);

    // Each frame must pass checkFrame (Ok) and peekTemplateId must return the correct id.
    for (const auto& frame : kFlow) {
        const auto buf = readVector(frame.filename);
        ASSERT_FALSE(buf.empty()) << "Could not read " << frame.filename;

        const auto fc = rpc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status, rpc::FrameStatus::Ok)
            << "checkFrame != Ok for " << frame.filename;
        EXPECT_EQ(fc.templateId, frame.expectedTemplateId)
            << "checkFrame templateId mismatch for " << frame.filename;

        const auto peeked = rpc::peekTemplateId(buf.data(), buf.size());
        ASSERT_TRUE(peeked.has_value())
            << "peekTemplateId returned nullopt for " << frame.filename;
        EXPECT_EQ(*peeked, frame.expectedTemplateId)
            << "peekTemplateId mismatch for " << frame.filename;
    }

    // Verify the exact templateId sequence as an array.
    constexpr uint16_t kExpectedSequence[kFlowLen] = {16, 17, 18, 20, 21};
    uint16_t observedIds[kFlowLen];
    for (std::size_t i = 0; i < kFlowLen; ++i) {
        const auto buf = readVector(kFlow[i].filename);
        const auto fc  = rpc::checkFrame(buf.data(), buf.size());
        ASSERT_EQ(fc.status, rpc::FrameStatus::Ok);
        observedIds[i] = fc.templateId;
    }
    for (std::size_t i = 0; i < kFlowLen; ++i) {
        EXPECT_EQ(observedIds[i], kExpectedSequence[i])
            << "Produce→Records session-flow templateId mismatch at index " << i;
    }

    // Encode-golden round-trip for each frame using the data codecs.
    // step 0: Produce (explicit partition)
    {
        const auto golden = readVector("produce_explicit_partition.bin");
        rpc::Produce msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 1001;
        msg.partition      = 7;
        msg.partitionKnown = 1;
        msg.key            = std::span<const uint8_t>{};
        msg.recordBatch    = std::span<const uint8_t>(kProduceBatch, sizeof(kProduceBatch));
        std::array<uint8_t, 128> enc{};
        const std::size_t n = msg.encode(enc.data());
        ASSERT_EQ(n, golden.size());
        EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
            << "Produce round-trip differs from golden";
    }
    // step 1: ProduceResponse (simulated broker response: baseOffset=123456)
    {
        const auto golden = readVector("produce_response.bin");
        rpc::ProduceResponse msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.errorCode     = static_cast<int16_t>(rpc::ErrorCode::OK);
        msg.partition     = 7;
        msg.baseOffset    = 123456LL;
        std::array<uint8_t, 64> enc{};
        const std::size_t n = msg.encode(enc.data());
        ASSERT_EQ(n, golden.size());
        EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
            << "ProduceResponse round-trip differs from golden";
    }
    // step 2: Subscribe (explicit partition, fromOffset after baseOffset)
    {
        const auto golden = readVector("subscribe_explicit.bin");
        rpc::Subscribe msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.topicId        = 3001;
        msg.partition      = 5;
        msg.partitionKnown = 1;
        msg.fromOffset     = 9999LL;
        msg.isolation      = 1;
        msg.key            = std::span<const uint8_t>{};
        std::array<uint8_t, 64> enc{};
        const std::size_t n = msg.encode(enc.data());
        ASSERT_EQ(n, golden.size());
        EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
            << "Subscribe round-trip differs from golden";
    }
    // step 3: Records push (broker pushes matching records)
    {
        const auto golden = readVector("records_push.bin");
        rpc::Records msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
        msg.topicId        = 3001;
        msg.partition      = 5;
        msg.baseOffset     = 100000LL;
        msg.recordBatch    = std::span<const uint8_t>(kGoldenBatch, sizeof(kGoldenBatch));
        std::array<uint8_t, 128> enc{};
        const std::size_t n = msg.encode(enc.data());
        ASSERT_EQ(n, golden.size());
        EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
            << "Records round-trip differs from golden";
    }
    // step 4: Unsubscribe
    {
        const auto golden = readVector("unsubscribe.bin");
        rpc::Unsubscribe msg;
        msg.sessionId      = kSessionId;
        msg.correlationId  = kCorrelationId;
        msg.subscriptionId = static_cast<int64_t>(0x0102030405060708ULL);
        std::array<uint8_t, 64> enc{};
        const std::size_t n = msg.encode(enc.data());
        ASSERT_EQ(n, golden.size());
        EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
            << "Unsubscribe round-trip differs from golden";
    }
}

// ===========================================================================
// CG-1 Golden conformance tests (file-backed, non-self-referential)
//
// All vectors committed in conformance/vectors/ by gen_cg1_vectors.cpp.
// Each test:
//   1. Reads the committed .bin file.
//   2. Asserts header fields (templateId, schemaId, version, session, corr).
//   3. Decode-golden: decodes + asserts every field.
//   4. Encode-golden: re-encodes from expected values, asserts byte-identical.
//
// Golden field values shared across all CG-1 vectors.
// ===========================================================================

static const uint8_t kCG1GroupId[]  = {'g', 'r', 'p', '2'};
static const uint8_t kCG1MemberId[] = {'m', 'b', 'r', 'X'};
static const uint8_t kCG1Meta[]     = {0xDE, 0xAD, 0xBE, 0xEF};

// ===========================================================================
// CG-1-1. cg1_join_group.bin  (templateId 32, 50 bytes)
//
// JoinGroup{ sessionTimeoutMs=30000, groupId="grp2", memberId="mbrX",
//            subscribedTopicIds=[10, 20] }
//
// Frame layout:
//   [0..23]  header: blockLen=4, tid=32, schema=120, ver=1
//   [24..27] sessionTimeoutMs = 30000
//   [28..29] groupId len = 4;  [30..33] "grp2"
//   [34..35] memberId len = 4; [36..39] "mbrX"
//   [40..41] topicIds count = 2
//   [42..45] topicId[0] = 10 (LE32)
//   [46..49] topicId[1] = 20 (LE32)
// ===========================================================================
TEST(GroupConformance, JoinGroupFileGolden) {
    const auto golden = readVector("cg1_join_group.bin");
    ASSERT_EQ(golden.size(), 50u);

    assertHeader(golden, rpc::JoinGroup::TEMPLATE_ID, rpc::JoinGroup::BODY_FIXED);

    // Decode-golden
    auto msgOpt = rpc::JoinGroup::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,        kSessionId);
    EXPECT_EQ(msgOpt->correlationId,    kCorrelationId);
    EXPECT_EQ(msgOpt->sessionTimeoutMs, 30000);
    ASSERT_EQ(msgOpt->groupId.size(),   sizeof(kCG1GroupId));
    EXPECT_EQ(std::memcmp(msgOpt->groupId.data(), kCG1GroupId, sizeof(kCG1GroupId)), 0);
    ASSERT_EQ(msgOpt->memberId.size(),  sizeof(kCG1MemberId));
    EXPECT_EQ(std::memcmp(msgOpt->memberId.data(), kCG1MemberId, sizeof(kCG1MemberId)), 0);
    ASSERT_EQ(msgOpt->subscribedTopicIds.size(), 2u);
    EXPECT_EQ(msgOpt->subscribedTopicIds[0], 10);
    EXPECT_EQ(msgOpt->subscribedTopicIds[1], 20);

    // Encode-golden
    const std::span<const uint8_t> grp(kCG1GroupId, sizeof(kCG1GroupId));
    const std::span<const uint8_t> mbr(kCG1MemberId, sizeof(kCG1MemberId));
    const int32_t topicIds[] = {10, 20};
    const std::span<const int32_t> topicSpan(topicIds, 2);

    rpc::JoinGroup enc;
    enc.sessionId        = kSessionId;
    enc.correlationId    = kCorrelationId;
    enc.sessionTimeoutMs = 30000;
    enc.groupId          = grp;
    enc.memberId         = mbr;

    std::array<uint8_t, 64> encBuf{};
    const std::size_t n = enc.encode(encBuf.data(), topicSpan);
    ASSERT_EQ(n, 50u);
    EXPECT_EQ(std::memcmp(encBuf.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden cg1_join_group.bin";
}

// ===========================================================================
// CG-1-2. cg1_sync_group_response.bin  (templateId 35, 44 bytes)
//
// SyncGroupResponse{ errorCode=0,
//                    assignment=[(topicId=10,partition=0),(topicId=20,partition=3)] }
//
// Frame layout:
//   [0..23]  header: blockLen=2, tid=35, schema=120, ver=1
//   [24..25] errorCode = 0
//   [26..27] assignment count = 2
//   [28..31] topicId[0]=10  [32..35] partition[0]=0
//   [36..39] topicId[1]=20  [40..43] partition[1]=3
// ===========================================================================
TEST(GroupConformance, SyncGroupResponseFileGolden) {
    const auto golden = readVector("cg1_sync_group_response.bin");
    ASSERT_EQ(golden.size(), 44u);

    assertHeader(golden, rpc::SyncGroupResponse::TEMPLATE_ID, rpc::SyncGroupResponse::BODY_FIXED);

    // Decode-golden
    auto msgOpt = rpc::SyncGroupResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->errorCode,     0);
    ASSERT_EQ(msgOpt->assignment.size(), 2u);
    EXPECT_EQ(msgOpt->assignment[0].topicId,   10);
    EXPECT_EQ(msgOpt->assignment[0].partition, 0);
    EXPECT_EQ(msgOpt->assignment[1].topicId,   20);
    EXPECT_EQ(msgOpt->assignment[1].partition, 3);

    // Encode-golden
    const rpc::TopicPartition32 asgn[] = { {10, 0}, {20, 3} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 2);

    rpc::SyncGroupResponse enc;
    enc.sessionId     = kSessionId;
    enc.correlationId = kCorrelationId;
    enc.errorCode     = 0;

    std::array<uint8_t, 64> encBuf{};
    const std::size_t n = enc.encode(encBuf.data(), asgnSpan);
    ASSERT_EQ(n, 44u);
    EXPECT_EQ(std::memcmp(encBuf.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden cg1_sync_group_response.bin";
}

// ===========================================================================
// CG-1-3. cg1_offset_commit.bin  (templateId 40, 64 bytes)
//
// OffsetCommit{ generationId=5, groupId="grp2", memberId="mbrX",
//               entries=[(topicId=10,partition=0,offset=1000,meta={DE,AD,BE,EF})] }
//
// Frame layout:
//   [0..23]  header: blockLen=4, tid=40, schema=120, ver=1
//   [24..27] generationId = 5
//   [28..29] groupId len = 4;  [30..33] "grp2"
//   [34..35] memberId len = 4; [36..39] "mbrX"
//   [40..41] entries count = 1
//   [42..45] topicId = 10  [46..49] partition = 0
//   [50..57] offset = 1000
//   [58..59] meta len = 4; [60..63] DE AD BE EF
// ===========================================================================
TEST(OffsetConformance, OffsetCommitFileGolden) {
    const auto golden = readVector("cg1_offset_commit.bin");
    ASSERT_EQ(golden.size(), 64u);

    assertHeader(golden, rpc::OffsetCommit::TEMPLATE_ID, rpc::OffsetCommit::BODY_FIXED);

    // Decode-golden
    auto msgOpt = rpc::OffsetCommit::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,       kSessionId);
    EXPECT_EQ(msgOpt->correlationId,   kCorrelationId);
    EXPECT_EQ(msgOpt->generationId,    5);
    ASSERT_EQ(msgOpt->groupId.size(),  sizeof(kCG1GroupId));
    EXPECT_EQ(std::memcmp(msgOpt->groupId.data(), kCG1GroupId, sizeof(kCG1GroupId)), 0);
    ASSERT_EQ(msgOpt->memberId.size(), sizeof(kCG1MemberId));
    EXPECT_EQ(std::memcmp(msgOpt->memberId.data(), kCG1MemberId, sizeof(kCG1MemberId)), 0);
    ASSERT_EQ(msgOpt->entries.size(),  1u);

    const auto ref0 = msgOpt->entries[0];
    EXPECT_EQ(ref0.topicId,   10);
    EXPECT_EQ(ref0.partition, 0);
    EXPECT_EQ(ref0.offset,    1000LL);
    ASSERT_EQ(ref0.metadata.size(), sizeof(kCG1Meta));
    EXPECT_EQ(std::memcmp(ref0.metadata.data(), kCG1Meta, sizeof(kCG1Meta)), 0);

    // Encode-golden
    const std::span<const uint8_t> grp(kCG1GroupId, sizeof(kCG1GroupId));
    const std::span<const uint8_t> mbr(kCG1MemberId, sizeof(kCG1MemberId));

    rpc::OffsetCommitEntry e;
    e.topicId   = 10;
    e.partition = 0;
    e.offset    = 1000LL;
    e.metadata  = std::span<const uint8_t>(kCG1Meta, sizeof(kCG1Meta));

    const rpc::OffsetCommitEntry entries[] = {e};
    const std::span<const rpc::OffsetCommitEntry> entrySpan(entries, 1);

    rpc::OffsetCommit enc;
    enc.sessionId     = kSessionId;
    enc.correlationId = kCorrelationId;
    enc.generationId  = 5;
    enc.groupId       = grp;
    enc.memberId      = mbr;

    std::array<uint8_t, 128> encBuf{};
    const std::size_t n = enc.encode(encBuf.data(), entrySpan);
    ASSERT_EQ(n, 64u);
    EXPECT_EQ(std::memcmp(encBuf.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden cg1_offset_commit.bin";
}

// ===========================================================================
// CG-1-4. cg1_assign_revoke.bin  (templateId 44, 30 bytes)
//
// Assign{ generationId=7, assignment=[] (empty = revoke all) }
//
// Frame layout:
//   [0..23]  header: blockLen=4, tid=44, schema=120, ver=1
//   [24..27] generationId = 7
//   [28..29] assignment count = 0
// ===========================================================================
TEST(GroupConformance, AssignRevokeFileGolden) {
    const auto golden = readVector("cg1_assign_revoke.bin");
    ASSERT_EQ(golden.size(), 30u);

    assertHeader(golden, rpc::Assign::TEMPLATE_ID, rpc::Assign::BODY_FIXED);

    // Decode-golden
    auto msgOpt = rpc::Assign::decode(golden.data(), golden.size());
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,         kSessionId);
    EXPECT_EQ(msgOpt->correlationId,     kCorrelationId);
    EXPECT_EQ(msgOpt->generationId,      7);
    EXPECT_EQ(msgOpt->assignment.size(), 0u);  // empty = revoke all
    EXPECT_TRUE(msgOpt->assignment.empty());

    // Encode-golden
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::Assign enc;
    enc.sessionId     = kSessionId;
    enc.correlationId = kCorrelationId;
    enc.generationId  = 7;

    std::array<uint8_t, 64> encBuf{};
    const std::size_t n = enc.encode(encBuf.data(), empty);
    ASSERT_EQ(n, 30u);
    EXPECT_EQ(std::memcmp(encBuf.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden cg1_assign_revoke.bin";
}
