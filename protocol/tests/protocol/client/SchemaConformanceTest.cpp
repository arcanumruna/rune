// tests/protocol/client/SchemaConformanceTest.cpp
//
// 0b schema-registry conformance vectors (inline golden bytes).
//
// Each test asserts BOTH:
//   1. Decode-golden:  golden bytes → decode → every field equals expected.
//   2. Encode-golden:  build message from expected values → encode → bytes
//                      identical to the golden (pins the wire format).
//   3. Header check:   templateId / schemaId / version / sessionId / correlationId.
//
// Golden values use the same session/correlation ids as ConformanceTest.cpp.

#include <gtest/gtest.h>

#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <array>
#include <cstdint>
#include <cstring>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Common test values — identical to ConformanceTest.cpp
// ---------------------------------------------------------------------------
static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static constexpr uint16_t kSchemaId     = rpc::kSchemaId;          // 120
static constexpr uint16_t kVersion      = rpc::kProtocolVersion;   // 3

// ---------------------------------------------------------------------------
// Helper: assert standard header fields on the raw buffer.
// ---------------------------------------------------------------------------
static void assertHeader(const uint8_t* buf, std::size_t n,
                         uint16_t expectedTemplateId,
                         uint16_t expectedBlockLength) {
    auto hdrOpt = rpc::MessageHeader::decode(buf, n);
    ASSERT_TRUE(hdrOpt.has_value()) << "MessageHeader::decode failed";
    EXPECT_EQ(hdrOpt->templateId,    expectedTemplateId);
    EXPECT_EQ(hdrOpt->blockLength,   expectedBlockLength);
    EXPECT_EQ(hdrOpt->schemaId,      kSchemaId);
    EXPECT_EQ(hdrOpt->version,       kVersion);
    EXPECT_EQ(hdrOpt->sessionId,     kSessionId);
    EXPECT_EQ(hdrOpt->correlationId, kCorrelationId);
}

// ===========================================================================
// DefineTopic golden vector
//
// Fields:
//   topicName     = "schema_test"  (11 bytes)
//   partitionCount = 4
//   keyField      = "id"           (2 bytes)
//   validate      = true
//   descriptorBlob = {0xAA, 0xBB, 0xCC}  (3 bytes)
//
// Wire layout (53 bytes total):
//   [0..1]   blockLength=5         : 05 00
//   [2..3]   templateId=64         : 40 00
//   [4..5]   schemaId=120          : 78 00
//   [6..7]   version=3             : 03 00
//   [8..15]  sessionId  (LE64)     : 88 77 66 55 44 33 22 11
//   [16..23] correlationId (LE64)  : 11 10 0F 0E 0D 0C 0B 0A
//   [24..27] partitionCount=4(LE32): 04 00 00 00
//   [28]     validate=1            : 01
//   [29..30] topicName len=11(LE16): 0B 00
//   [31..41] "schema_test"
//   [42..43] keyField len=2 (LE16) : 02 00
//   [44..45] "id"
//   [46..49] descriptorLen=3(LE32) : 03 00 00 00
//   [50..52] AA BB CC
// ===========================================================================

static constexpr uint8_t kDefineTopicGolden[] = {
    // header
    0x05, 0x00,              // blockLength = 5
    0x40, 0x00,              // templateId  = 64
    0x78, 0x00,              // schemaId    = 120
    0x03, 0x00,              // version     = 3
    0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
    0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
    // body fixed (5 bytes)
    0x04, 0x00, 0x00, 0x00,  // partitionCount = 4
    0x01,                    // validate = 1
    // topicName varstr
    0x0B, 0x00,              // len = 11
    0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x5F, 0x74, 0x65, 0x73, 0x74,  // "schema_test"
    // keyField varstr
    0x02, 0x00,              // len = 2
    0x69, 0x64,              // "id"
    // descriptorBlob varbytes32
    0x03, 0x00, 0x00, 0x00,  // len = 3
    0xAA, 0xBB, 0xCC,
};
static constexpr std::size_t kDefineTopicGoldenSize = sizeof(kDefineTopicGolden);  // 53

TEST(SchemaConformance, DefineTopicDecodeGolden) {
    ASSERT_EQ(kDefineTopicGoldenSize, 53u);

    assertHeader(kDefineTopicGolden, kDefineTopicGoldenSize,
                 rpc::DefineTopic::TEMPLATE_ID, rpc::DefineTopic::BODY_FIXED);

    auto msgOpt = rpc::DefineTopic::decode(kDefineTopicGolden, kDefineTopicGoldenSize);
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,      kSessionId);
    EXPECT_EQ(msgOpt->correlationId,  kCorrelationId);
    EXPECT_EQ(msgOpt->topicName,      "schema_test");
    EXPECT_EQ(msgOpt->partitionCount, 4u);
    EXPECT_EQ(msgOpt->keyField,       "id");
    EXPECT_TRUE(msgOpt->validate);
    ASSERT_EQ(msgOpt->descriptorLen,  3u);
    static constexpr uint8_t kBlob[] = {0xAA, 0xBB, 0xCC};
    EXPECT_EQ(std::memcmp(msgOpt->descriptorBlob, kBlob, 3), 0);
}

TEST(SchemaConformance, DefineTopicEncodeGolden) {
    static constexpr uint8_t kBlob[] = {0xAA, 0xBB, 0xCC};

    rpc::DefineTopic msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicName      = "schema_test";
    msg.partitionCount = 4;
    msg.keyField       = "id";
    msg.validate       = true;
    msg.descriptorBlob = kBlob;
    msg.descriptorLen  = 3;

    ASSERT_EQ(msg.encodedSize(), kDefineTopicGoldenSize);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, kDefineTopicGoldenSize);
    EXPECT_EQ(std::memcmp(enc.data(), kDefineTopicGolden, n), 0)
        << "Encoded DefineTopic bytes differ from golden";
}

// Also verify checkFrame/peekTemplateId accept the golden bytes.
TEST(SchemaConformance, DefineTopicFrameCheck) {
    const auto fc = rpc::checkFrame(kDefineTopicGolden, kDefineTopicGoldenSize);
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::DefineTopic));

    const auto tid = rpc::peekTemplateId(kDefineTopicGolden, kDefineTopicGoldenSize);
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(rpc::TemplateId::DefineTopic));
}

// ===========================================================================
// TopicSchemaResponse golden vector
//
// Fields:
//   topicId        = 42
//   version        = 1
//   errorCode      = 0 (OK)
//   descriptorBlob = {0xDE, 0xAD, 0xBE, 0xEF}  (4 bytes)
//
// Wire layout (40 bytes total):
//   [0..1]   blockLength=8         : 08 00
//   [2..3]   templateId=67         : 43 00
//   [4..5]   schemaId=120          : 78 00
//   [6..7]   version=3             : 03 00
//   [8..15]  sessionId  (LE64)     : 88 77 66 55 44 33 22 11
//   [16..23] correlationId (LE64)  : 11 10 0F 0E 0D 0C 0B 0A
//   [24..27] topicId=42 (LE32)     : 2A 00 00 00
//   [28..29] version=1 (LE16)      : 01 00
//   [30..31] errorCode=0 (LE16)    : 00 00
//   [32..35] descriptorLen=4(LE32) : 04 00 00 00
//   [36..39] DE AD BE EF
// ===========================================================================

static constexpr uint8_t kTopicSchemaResponseGolden[] = {
    // header
    0x08, 0x00,              // blockLength = 8
    0x43, 0x00,              // templateId  = 67
    0x78, 0x00,              // schemaId    = 120
    0x03, 0x00,              // version     = 3
    0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
    0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
    // body fixed (8 bytes)
    0x2A, 0x00, 0x00, 0x00,  // topicId = 42
    0x01, 0x00,              // version = 1
    0x00, 0x00,              // errorCode = 0
    // descriptorBlob varbytes32
    0x04, 0x00, 0x00, 0x00,  // len = 4
    0xDE, 0xAD, 0xBE, 0xEF,
};
static constexpr std::size_t kTopicSchemaResponseGoldenSize = sizeof(kTopicSchemaResponseGolden);  // 40

TEST(SchemaConformance, TopicSchemaResponseDecodeGolden) {
    ASSERT_EQ(kTopicSchemaResponseGoldenSize, 40u);

    assertHeader(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize,
                 rpc::TopicSchemaResponse::TEMPLATE_ID, rpc::TopicSchemaResponse::BODY_FIXED);

    auto msgOpt = rpc::TopicSchemaResponse::decode(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize);
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,       42);
    EXPECT_EQ(msgOpt->version,       1u);
    EXPECT_EQ(msgOpt->errorCode,     0u);
    ASSERT_EQ(msgOpt->descriptorLen, 4u);
    static constexpr uint8_t kBlob[] = {0xDE, 0xAD, 0xBE, 0xEF};
    EXPECT_EQ(std::memcmp(msgOpt->descriptorBlob, kBlob, 4), 0);
}

TEST(SchemaConformance, TopicSchemaResponseEncodeGolden) {
    static constexpr uint8_t kBlob[] = {0xDE, 0xAD, 0xBE, 0xEF};

    rpc::TopicSchemaResponse msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 42;
    msg.version        = 1;
    msg.errorCode      = 0;
    msg.descriptorBlob = kBlob;
    msg.descriptorLen  = 4;

    ASSERT_EQ(msg.encodedSize(), kTopicSchemaResponseGoldenSize);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, kTopicSchemaResponseGoldenSize);
    EXPECT_EQ(std::memcmp(enc.data(), kTopicSchemaResponseGolden, n), 0)
        << "Encoded TopicSchemaResponse bytes differ from golden";
}

// Also verify checkFrame/peekTemplateId accept the golden bytes.
TEST(SchemaConformance, TopicSchemaResponseFrameCheck) {
    const auto fc = rpc::checkFrame(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize);
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::TopicSchemaResponse));

    const auto tid = rpc::peekTemplateId(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize);
    ASSERT_TRUE(tid.has_value());
    EXPECT_EQ(*tid, static_cast<uint16_t>(rpc::TemplateId::TopicSchemaResponse));
}
