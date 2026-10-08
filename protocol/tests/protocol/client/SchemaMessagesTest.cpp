#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "rune/protocol/client/SchemaMessages.h"
#include "rune/protocol/client/Dispatch.h"

using namespace rune::protocol::client;

TEST(SchemaMessages, DefineTopicRoundTrip) {
    std::vector<uint8_t> blob = {1, 2, 3, 4, 5};  // opaque descriptor bytes
    DefineTopic in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicName = "characters";
    in.partitionCount = 16;
    in.keyField = "userId";
    in.validate = true;
    in.descriptorBlob = blob.data();
    in.descriptorLen = static_cast<uint32_t>(blob.size());

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::DefineTopic));

    auto out = DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 7);
    EXPECT_EQ(out->correlationId, 99);
    EXPECT_EQ(out->topicName, "characters");
    EXPECT_EQ(out->partitionCount, 16u);
    EXPECT_EQ(out->keyField, "userId");
    EXPECT_TRUE(out->validate);
    ASSERT_EQ(out->descriptorLen, blob.size());
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), blob.size()));
}

TEST(SchemaMessages, DefineTopicResponseRoundTrip) {
    DefineTopicResponse in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicId = 0x40000000; in.version = 1;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = DefineTopicResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 0x40000000);
    EXPECT_EQ(out->version, 1);
    EXPECT_EQ(out->errorCode, 0);
}

TEST(SchemaMessages, GetTopicSchemaRoundTrip) {
    GetTopicSchema in; in.sessionId = 1; in.correlationId = 2;
    in.topicName = "characters";
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = GetTopicSchema::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicName, "characters");
}

TEST(SchemaMessages, TopicSchemaResponseRoundTrip) {
    std::vector<uint8_t> blob = {9, 8, 7};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.version = 1;
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = TopicSchemaResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 5);
    ASSERT_EQ(out->descriptorLen, 3u);
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), 3));
}

TEST(SchemaMessages, DecodeRejectsTruncated) {
    DefineTopic in; in.topicName = "x"; in.keyField = "";
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size() - 1).has_value());
}

TEST(SchemaMessages, AlterTopicRoundTrip) {
    std::vector<uint8_t> blob = {0x11, 0x22, 0x33, 0x44};  // opaque ops blob
    AlterTopic in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicName = "characters";
    in.expectedVersion = 4;
    in.opsBlob = blob.data();
    in.opsLen = static_cast<uint32_t>(blob.size());

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::AlterTopic));

    auto out = AlterTopic::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 7);
    EXPECT_EQ(out->correlationId, 99);
    EXPECT_EQ(out->topicName, "characters");
    EXPECT_EQ(out->expectedVersion, 4u);
    ASSERT_EQ(out->opsLen, blob.size());
    EXPECT_EQ(0, std::memcmp(out->opsBlob, blob.data(), blob.size()));
}

TEST(SchemaMessages, AlterTopicResponseRoundTrip) {
    AlterTopicResponse in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicId = 0x40000000; in.newVersion = 5;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = AlterTopicResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 0x40000000);
    EXPECT_EQ(out->newVersion, 5);
    EXPECT_EQ(out->errorCode, 0);
}

TEST(SchemaMessages, AlterTopicDecodeRejectsTruncated) {
    AlterTopic in; in.topicName = "x"; in.expectedVersion = 1;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(AlterTopic::decode(buf.data(), buf.size() - 1).has_value());
}
