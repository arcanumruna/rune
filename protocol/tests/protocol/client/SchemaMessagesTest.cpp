#include <gtest/gtest.h>
#include <array>
#include <cstdio>
#include <string>
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
    in.requestedFingerprint = 0x0102030405060708ULL;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    auto out = GetTopicSchema::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicName, "characters");
    EXPECT_EQ(out->requestedFingerprint, 0x0102030405060708ULL);
}

TEST(SchemaMessages, GetTopicSchemaRoundTripLatest) {
    // requestedFingerprint = 0 means "latest" — must also survive round-trip.
    GetTopicSchema in; in.sessionId = 1; in.correlationId = 2;
    in.topicName = "characters";
    in.requestedFingerprint = 0;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = GetTopicSchema::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->requestedFingerprint, 0u);
}

TEST(SchemaMessages, GetTopicSchemaDecodeRejectsTruncated) {
    GetTopicSchema in; in.topicName = "x"; in.requestedFingerprint = 0xAABBCCDDEEFF0011ULL;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    // Truncate within the var topicName field.
    EXPECT_FALSE(GetTopicSchema::decode(buf.data(), buf.size() - 1).has_value());
    // Truncate within the fixed requestedFingerprint field itself.
    EXPECT_FALSE(GetTopicSchema::decode(buf.data(), MessageHeader::ENCODED_SIZE + 4).has_value());
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

// Phase 2b Part C: the topic's partition count rides as a TRAILING i32 after
// the descriptor blob (append-only: BODY_FIXED/blockLength unchanged, so a
// pre-2b decoder — which stops after the blob — ignores it).
TEST(SchemaMessagesRegression, TopicSchemaResponseCarriesTrailingPartitionCount) {
    std::vector<uint8_t> blob = {9, 8, 7};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.version = 3; in.errorCode = 0;
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.partitionCount = 8;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    ASSERT_EQ(n, MessageHeader::ENCODED_SIZE + TopicSchemaResponse::BODY_FIXED + 4u + 3u + 4u);
    EXPECT_EQ(TopicSchemaResponse::BODY_FIXED, 8u) << "fixed block must not grow (append-only)";
    auto out = TopicSchemaResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 5);
    EXPECT_EQ(out->version, 3u);
    ASSERT_EQ(out->descriptorLen, 3u);
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), 3));
    EXPECT_EQ(out->partitionCount, 8);
}

// A pre-2b broker's frame has no trailing bytes: it must still decode, with
// partitionCount = 0 ("not reported") — never a reject, never garbage.  A
// frame cut inside the trailing field (1-3 bytes) is treated the same way.
TEST(SchemaMessagesRegression, TopicSchemaResponseWithoutTrailingPartitionCountDecodesAsNotReported) {
    std::vector<uint8_t> blob = {1, 2, 3, 4};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 6; in.version = 1;
    in.descriptorBlob = blob.data(); in.descriptorLen = 4;
    in.partitionCount = 8;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());

    auto noTrailer = TopicSchemaResponse::decode(buf.data(), n - 4);
    ASSERT_TRUE(noTrailer.has_value());
    EXPECT_EQ(noTrailer->topicId, 6);
    ASSERT_EQ(noTrailer->descriptorLen, 4u);
    EXPECT_EQ(noTrailer->partitionCount, 0);

    auto cut = TopicSchemaResponse::decode(buf.data(), n - 2);
    ASSERT_TRUE(cut.has_value());
    EXPECT_EQ(cut->partitionCount, 0);
}

// ── Phase 2b E: appended persistence byte ────────────────────────────────────
TEST(SchemaMessagesRegression, DefineTopicPersistenceRoundTrips) {
    std::vector<uint8_t> blob = {9, 8, 7};
    DefineTopic in;
    in.sessionId = 1; in.correlationId = 2;
    in.topicName = "presence"; in.partitionCount = 4; in.keyField = "userId";
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.persistence = DefineTopic::kPersistenceTransient;

    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto out = DefineTopic::decode(buf.data(), buf.size());
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->persistence, DefineTopic::kPersistenceTransient);
    EXPECT_EQ(out->topicName, "presence");
    ASSERT_EQ(out->descriptorLen, 3u);
}

// Old clients send nothing after the blob: that is a DISK topic, and a disk frame
// from a new client is byte-identical to an old one (no trailing byte written).
TEST(SchemaMessagesRegression, DefineTopicWithoutPersistenceByteDecodesAsDisk) {
    std::vector<uint8_t> blob = {1};
    DefineTopic disk;
    disk.topicName = "t"; disk.descriptorBlob = blob.data(); disk.descriptorLen = 1;
    DefineTopic transientMsg = disk;
    transientMsg.persistence = DefineTopic::kPersistenceTransient;
    EXPECT_EQ(transientMsg.encodedSize(), disk.encodedSize() + 1)
        << "the persistence byte is written only for a non-disk topic";

    std::vector<uint8_t> buf(disk.encodedSize());
    disk.encode(buf.data());
    auto out = DefineTopic::decode(buf.data(), buf.size());
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->persistence, DefineTopic::kPersistenceDisk);
}

TEST(SchemaMessagesRegression, DefineTopicUnknownPersistenceValueIsRejected) {
    std::vector<uint8_t> blob = {1};
    DefineTopic in;
    in.topicName = "t"; in.descriptorBlob = blob.data(); in.descriptorLen = 1;
    in.persistence = DefineTopic::kPersistenceTransient;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    buf.back() = 2;   // one past Transient: append-only enum, never cast
    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size()).has_value());
}

// Checked writes (client version 17): the 14-byte TopicKeyFeatures block trails
// DefineTopic (after the v8 extension) and TopicSchemaResponse (after partitionCount).
namespace {
rune::protocol::TopicKeyFeatures keyIndexFeatures() {
    rune::protocol::TopicKeyFeatures f;
    f.keyIndex = true;
    return f;
}
DefineTopic defineTopicWithExt() {
    DefineTopic m;
    m.sessionId = 7; m.correlationId = 99;
    m.topicName = "events"; m.partitionCount = 4;
    m.hasExt = true; m.replicationFactor = 3;
    m.ext.statement = "topic events;"; m.ext.clientName = "cli";
    return m;
}
}   // namespace

TEST(CheckedWritesSchemaWire, KeyFeaturesBlockRoundTrips) {
    const auto f = keyIndexFeatures();
    ASSERT_EQ(f.autoIncrementMax, (uint64_t{1} << 53) - 1);
    ASSERT_EQ(f.autoIncrementReserve, 4096u);
    std::array<uint8_t, rune::protocol::kTopicKeyFeaturesWireLen> wire{};
    rune::protocol::putTopicKeyFeatures(wire.data(), f);
    const std::array<uint8_t, 14> want{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F, 0x00, 0x00, 0x10, 0x00, 0x00};
    EXPECT_EQ(wire, want);
    rune::protocol::TopicKeyFeatures back;
    ASSERT_TRUE(rune::protocol::getTopicKeyFeatures(wire.data(), wire.size(), back));
    EXPECT_EQ(back, f);

    auto bad = wire; bad[0] = 0x04;
    EXPECT_FALSE(rune::protocol::getTopicKeyFeatures(bad.data(), bad.size(), back));
    bad = wire; bad[1] = 2;
    EXPECT_FALSE(rune::protocol::getTopicKeyFeatures(bad.data(), bad.size(), back));
    EXPECT_FALSE(rune::protocol::getTopicKeyFeatures(wire.data(), 13, back));

    auto both = f; both.autoIncrement = true; both.partitioner = rune::protocol::Partitioner::Modulo;
    rune::protocol::putTopicKeyFeatures(wire.data(), both);
    EXPECT_EQ(wire[0], 0x03); EXPECT_EQ(wire[1], 1);
    ASSERT_TRUE(rune::protocol::getTopicKeyFeatures(wire.data(), wire.size(), back));
    EXPECT_EQ(back, both);
}

TEST(CheckedWritesSchemaWire, DefineTopicCarriesKeyFeaturesAfterTheExtension) {
    DefineTopic plain = defineTopicWithExt();
    DefineTopic m = plain;
    m.hasKeyFeatures = true; m.keyFeatures = keyIndexFeatures();
    ASSERT_EQ(m.encodedSize(), plain.encodedSize() + 14u);
    std::vector<uint8_t> buf(m.encodedSize());
    ASSERT_EQ(m.encode(buf.data()), buf.size());

    auto d = DefineTopic::decode(buf.data(), buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->hasKeyFeatures);
    EXPECT_EQ(d->keyFeatures, m.keyFeatures);
    EXPECT_TRUE(d->hasExt);
    EXPECT_EQ(d->ext, m.ext);
    EXPECT_EQ(d->replicationFactor, 3);

    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size() - 1).has_value());
    buf.push_back(0);
    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size()).has_value());

    std::vector<uint8_t> noBlock(plain.encodedSize());
    ASSERT_EQ(plain.encode(noBlock.data()), noBlock.size());
    auto p = DefineTopic::decode(noBlock.data(), noBlock.size());
    ASSERT_TRUE(p.has_value());
    EXPECT_FALSE(p->hasKeyFeatures);
}

TEST(CheckedWritesSchemaWireRegression, DefineTopicWithoutFeaturesIsByteIdenticalRegression) {
    DefineTopic m;
    m.sessionId = static_cast<int64_t>(0x1122334455667788ULL); m.correlationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
    m.topicName = "events"; m.partitionCount = 4; m.persistence = 0; m.replicationFactor = 3;
    m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
    m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
    m.ext.requestId = {.hi = 0xC11E, .lo = 77}; m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src";
    m.ext.replayedOffset = 5;
    ASSERT_FALSE(m.hasKeyFeatures);
    std::vector<uint8_t> got(m.encodedSize());
    ASSERT_EQ(m.encode(got.data()), got.size());

    const std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/define_topic_v8.bin";
    std::FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr) << path;
    std::vector<uint8_t> golden(4096);
    golden.resize(std::fread(golden.data(), 1, golden.size(), f));
    std::fclose(f);
    EXPECT_EQ(got, golden);
}

TEST(CheckedWritesSchemaWire, KeyFeaturesNeedTheExtension) {
    DefineTopic m;
    m.topicName = "t";
    m.hasKeyFeatures = true; m.keyFeatures = keyIndexFeatures();
    EXPECT_FALSE(m.fits());
    std::vector<uint8_t> buf(m.encodedSize() + 14);
    EXPECT_EQ(m.encode(buf.data()), 0u);
}

TEST(CheckedWritesSchemaWire, TopicSchemaResponseTrailer) {
    std::vector<uint8_t> blob = {9, 8, 7};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.version = 3;
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.partitionCount = 8;

    std::vector<uint8_t> plain(in.encodedSize());
    ASSERT_EQ(in.encode(plain.data()), plain.size());
    auto p = TopicSchemaResponse::decode(plain.data(), plain.size());
    ASSERT_TRUE(p.has_value());
    EXPECT_FALSE(p->hasKeyFeatures);

    in.hasKeyFeatures = true; in.keyFeatures = keyIndexFeatures();
    ASSERT_EQ(in.encodedSize(), plain.size() + 14u);
    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto d = TopicSchemaResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->hasKeyFeatures);
    EXPECT_EQ(d->keyFeatures, in.keyFeatures);
    EXPECT_EQ(d->partitionCount, 8);

    // A 1-13 byte remainder is not a block: an older or garbled tail decodes as none.
    for (std::size_t extra = 1; extra <= 13; ++extra) {
        auto cut = TopicSchemaResponse::decode(buf.data(), plain.size() + extra);
        ASSERT_TRUE(cut.has_value()) << extra;
        EXPECT_FALSE(cut->hasKeyFeatures) << extra;
        EXPECT_EQ(cut->partitionCount, 8) << extra;
    }
}

TEST(CheckedWritesSchemaWireRegression, TopicSchemaResponseRefusesAMalformedKeyFeaturesBlock) {
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.partitionCount = 8;
    in.hasKeyFeatures = true; in.keyFeatures = keyIndexFeatures();
    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    ASSERT_TRUE(TopicSchemaResponse::decode(buf.data(), buf.size()).has_value());
    const std::size_t block = buf.size() - 14;

    auto badFlags = buf; badFlags[block] = 0x04;
    EXPECT_FALSE(TopicSchemaResponse::decode(badFlags.data(), badFlags.size()).has_value());
    auto badPartitioner = buf; badPartitioner[block + 1] = 2;
    EXPECT_FALSE(TopicSchemaResponse::decode(badPartitioner.data(), badPartitioner.size()).has_value());
}

TEST(CheckedWritesSchemaWireRegression, DefineTopicRefusesAMalformedKeyFeaturesBlock) {
    DefineTopic m = defineTopicWithExt();
    m.hasKeyFeatures = true; m.keyFeatures = keyIndexFeatures();
    std::vector<uint8_t> buf(m.encodedSize());
    ASSERT_EQ(m.encode(buf.data()), buf.size());
    const std::size_t block = buf.size() - 14;

    auto badFlags = buf; badFlags[block] = 0x04;
    EXPECT_FALSE(DefineTopic::decode(badFlags.data(), badFlags.size()).has_value());
    auto badPartitioner = buf; badPartitioner[block + 1] = 2;
    EXPECT_FALSE(DefineTopic::decode(badPartitioner.data(), badPartitioner.size()).has_value());
}
