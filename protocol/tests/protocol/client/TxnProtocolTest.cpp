// tests/protocol/client/TxnProtocolTest.cpp
// TX-1 — Transaction protocol codec tests.
// Organized by brief task order: templateIds/errors → Produce fields →
// InitProducerId/EndTxn → AddPartitions/AddOffsets/TxnOffsetCommit + Dispatch + conformance.

#include <gtest/gtest.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/TxnMessages.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Task 1: templateIds + error codes
// ---------------------------------------------------------------------------

TEST(TxnProtocol, TemplateIdsInReservedRange) {
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::InitProducerId), 45);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::EndTxnResponse), 54);
    EXPECT_LE(static_cast<uint16_t>(rpc::TemplateId::EndTxnResponse), 63); // groups/txn range
}

TEST(TxnProtocol, TxnTemplateIdsContinuous) {
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::InitProducerId),          45);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::InitProducerIdResponse),  46);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxn),      47);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxnResponse), 48);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::AddOffsetsToTxn),         49);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::AddOffsetsToTxnResponse), 50);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::TxnOffsetCommit),         51);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::TxnOffsetCommitResponse), 52);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::EndTxn),                  53);
    EXPECT_EQ(static_cast<uint16_t>(rpc::TemplateId::EndTxnResponse),          54);
}

TEST(TxnProtocol, TxnErrorCodes) {
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::InvalidProducerEpoch), 17);
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState),      19);
}

TEST(TxnProtocol, AllTxnErrorCodes) {
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::InvalidProducerEpoch), 17);
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::ProducerFenced),       18);
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState),      19);
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::OutOfOrderSequence),   20);
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::DuplicateSequence),    21);
}

// ---------------------------------------------------------------------------
// Task 2: Produce idempotent framing fields
// ---------------------------------------------------------------------------

TEST(DataMessages, ProduceCarriesIdempotentFraming) {
    rpc::Produce in;
    in.sessionId = 1; in.topicId = 7; in.partition = 0; in.acks = 1;
    in.producerId = 4242; in.producerEpoch = 3; in.baseSequence = 100;
    const uint8_t body[] = {0xAA, 0xBB};
    in.recordBatch = {body, sizeof(body)};
    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto out = rpc::Produce::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId, 4242);
    EXPECT_EQ(out->producerEpoch, 3);
    EXPECT_EQ(out->baseSequence, 100);
    EXPECT_EQ(out->acks, 1);
    ASSERT_EQ(out->recordBatch.size(), 2u);
}

TEST(DataMessages, ProduceDefaultsAreNonIdempotent) {
    rpc::Produce in; in.topicId = 1;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    auto out = rpc::Produce::decode(buf.data(), buf.size());
    ASSERT_TRUE(out); EXPECT_EQ(out->producerId, -1); // sentinel
}

TEST(DataMessages, ProduceIdempotentEncodedSizeIncludesNewFields) {
    // BODY_FIXED is now 10 + 8 + 2 + 4 = 24
    // encodedSize = 24(hdr) + 24(fixed) + 2(key len) + 2(rb len) = 52 with empty fields
    rpc::Produce msg;
    EXPECT_EQ(rpc::Produce::BODY_FIXED, 24u);
    // empty key + empty recordBatch: 24 + 24 + 2 + 2 = 52
    EXPECT_EQ(msg.encodedSize(), 52u);
}

// ---------------------------------------------------------------------------
// Task 3: InitProducerId / EndTxn codecs
// ---------------------------------------------------------------------------

TEST(TxnMessages, InitProducerIdRoundTrip) {
    const uint8_t txnId[] = {'m', 'y', '-', 't', 'x', 'n'};
    rpc::InitProducerId in;
    in.sessionId           = 0x1122334455667788LL;
    in.correlationId       = 0x0A0B0C0D0E0F1011LL;
    in.transactionalId     = {txnId, sizeof(txnId)};
    in.transactionTimeoutMs = 30000;

    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());

    auto out = rpc::InitProducerId::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->sessionId,            in.sessionId);
    EXPECT_EQ(out->correlationId,        in.correlationId);
    EXPECT_EQ(out->transactionTimeoutMs, 30000);
    ASSERT_EQ(out->transactionalId.size(), sizeof(txnId));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), txnId, sizeof(txnId)), 0);
}

TEST(TxnMessages, InitProducerIdEmptyTransactionalId) {
    rpc::InitProducerId in;
    in.sessionId            = 1;
    in.correlationId        = 2;
    in.transactionalId      = {}; // empty => idempotent-only producer
    in.transactionTimeoutMs = 0;

    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());

    auto out = rpc::InitProducerId::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->transactionalId.size(), 0u);
    EXPECT_EQ(out->transactionTimeoutMs,   0);
}

TEST(TxnMessages, InitProducerIdTruncation) {
    const uint8_t txnId[] = {'t', 'x', 'n'};
    rpc::InitProducerId in;
    in.sessionId           = 1;
    in.correlationId       = 2;
    in.transactionalId     = {txnId, sizeof(txnId)};
    in.transactionTimeoutMs = 5000;

    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());

    // One byte short must return nullopt
    EXPECT_FALSE(rpc::InitProducerId::decode(buf.data(), buf.size() - 1));
}

TEST(TxnMessages, InitProducerIdResponseRoundTrip) {
    rpc::InitProducerIdResponse in;
    in.sessionId     = 10;
    in.correlationId = 20;
    in.producerId    = 999999LL;
    in.producerEpoch = 7;
    in.errorCode     = 0;

    std::vector<uint8_t> buf(rpc::InitProducerIdResponse::ENCODED_SIZE);
    ASSERT_EQ(in.encode(buf.data()), rpc::InitProducerIdResponse::ENCODED_SIZE);

    auto out = rpc::InitProducerIdResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->sessionId,     in.sessionId);
    EXPECT_EQ(out->correlationId, in.correlationId);
    EXPECT_EQ(out->producerId,    999999LL);
    EXPECT_EQ(out->producerEpoch, 7);
    EXPECT_EQ(out->errorCode,     0);
}

TEST(TxnMessages, EndTxnRoundTrip) {
    const uint8_t txnId[] = {'e', 'n', 'd', '-', 't', 'x', 'n'};
    rpc::EndTxn in;
    in.sessionId       = 0x1122334455667788LL;
    in.correlationId   = 0x0A0B0C0D0E0F1011LL;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 12345LL;
    in.producerEpoch   = 2;
    in.commit          = 1; // commit=1

    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());

    auto out = rpc::EndTxn::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->sessionId,     in.sessionId);
    EXPECT_EQ(out->correlationId, in.correlationId);
    EXPECT_EQ(out->producerId,    12345LL);
    EXPECT_EQ(out->producerEpoch, 2);
    EXPECT_EQ(out->commit,        1u);
    ASSERT_EQ(out->transactionalId.size(), sizeof(txnId));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), txnId, sizeof(txnId)), 0);
}

TEST(TxnMessages, EndTxnAbortFlag) {
    const uint8_t txnId[] = {'t'};
    rpc::EndTxn in;
    in.sessionId       = 1;
    in.correlationId   = 2;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 1LL;
    in.producerEpoch   = 0;
    in.commit          = 0; // abort

    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());

    auto out = rpc::EndTxn::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->commit, 0u);
}

TEST(TxnMessages, EndTxnTruncation) {
    const uint8_t txnId[] = {'x'};
    rpc::EndTxn in;
    in.sessionId       = 1;
    in.correlationId   = 2;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 1LL;
    in.producerEpoch   = 0;
    in.commit          = 1;

    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());

    EXPECT_FALSE(rpc::EndTxn::decode(buf.data(), buf.size() - 1));
}

TEST(TxnMessages, EndTxnResponseRoundTrip) {
    rpc::EndTxnResponse in;
    in.sessionId     = 5;
    in.correlationId = 6;
    in.errorCode     = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);

    std::vector<uint8_t> buf(rpc::EndTxnResponse::ENCODED_SIZE);
    ASSERT_EQ(in.encode(buf.data()), rpc::EndTxnResponse::ENCODED_SIZE);

    auto out = rpc::EndTxnResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->errorCode, static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState));
}

// ---------------------------------------------------------------------------
// Task 4: AddPartitionsToTxn / AddOffsetsToTxn / TxnOffsetCommit + Dispatch
// ---------------------------------------------------------------------------

TEST(TxnMessages, AddPartitionsToTxnRoundTrip) {
    const uint8_t txnId[] = {'a', 'p', 't'};
    rpc::AddPartitionsToTxn in;
    in.sessionId       = 100;
    in.correlationId   = 200;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 7777LL;
    in.producerEpoch   = 5;

    const rpc::TxnTopicPartition partitions[] = {{10, 0}, {20, 3}};
    const std::span<const rpc::TxnTopicPartition> partSpan(partitions, 2);

    std::vector<uint8_t> buf(rpc::AddPartitionsToTxn::encodedSizeFor(
        {txnId, sizeof(txnId)}, 2));
    ASSERT_EQ(in.encode(buf.data(), partSpan), buf.size());

    auto out = rpc::AddPartitionsToTxn::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    7777LL);
    EXPECT_EQ(out->producerEpoch, 5);
    ASSERT_EQ(out->partitions.size(), 2u);
    EXPECT_EQ(out->partitions[0].topicId,   10);
    EXPECT_EQ(out->partitions[0].partition, 0);
    EXPECT_EQ(out->partitions[1].topicId,   20);
    EXPECT_EQ(out->partitions[1].partition, 3);
}

TEST(TxnMessages, AddPartitionsToTxnResponseRoundTrip) {
    rpc::AddPartitionsToTxnResponse in;
    in.sessionId     = 1;
    in.correlationId = 2;

    const rpc::TxnPartitionResult results[] = {{10, 0, 0}, {20, 3, static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState)}};
    const std::span<const rpc::TxnPartitionResult> resSpan(results, 2);

    std::vector<uint8_t> buf(rpc::AddPartitionsToTxnResponse::encodedSizeFor(2));
    ASSERT_EQ(in.encode(buf.data(), resSpan), buf.size());

    auto out = rpc::AddPartitionsToTxnResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    ASSERT_EQ(out->results.size(), 2u);
    EXPECT_EQ(out->results[0].topicId,   10);
    EXPECT_EQ(out->results[0].partition, 0);
    EXPECT_EQ(out->results[0].errorCode, 0);
    EXPECT_EQ(out->results[1].topicId,   20);
    EXPECT_EQ(out->results[1].partition, 3);
    EXPECT_EQ(out->results[1].errorCode,
              static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState));
}

TEST(TxnMessages, AddOffsetsToTxnRoundTrip) {
    const uint8_t txnId[] = {'a', 'o', 't'};
    const uint8_t grpId[] = {'g', 'r', 'p', '1'};
    rpc::AddOffsetsToTxn in;
    in.sessionId       = 11;
    in.correlationId   = 22;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 88LL;
    in.producerEpoch   = 1;
    in.groupId         = {grpId, sizeof(grpId)};

    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());

    auto out = rpc::AddOffsetsToTxn::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    88LL);
    EXPECT_EQ(out->producerEpoch, 1);
    ASSERT_EQ(out->groupId.size(), sizeof(grpId));
    EXPECT_EQ(std::memcmp(out->groupId.data(), grpId, sizeof(grpId)), 0);
    ASSERT_EQ(out->transactionalId.size(), sizeof(txnId));
}

TEST(TxnMessages, AddOffsetsToTxnResponseRoundTrip) {
    rpc::AddOffsetsToTxnResponse in;
    in.sessionId     = 3;
    in.correlationId = 4;
    in.errorCode     = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);

    std::vector<uint8_t> buf(rpc::AddOffsetsToTxnResponse::ENCODED_SIZE);
    ASSERT_EQ(in.encode(buf.data()), rpc::AddOffsetsToTxnResponse::ENCODED_SIZE);

    auto out = rpc::AddOffsetsToTxnResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->errorCode, static_cast<int16_t>(rpc::ErrorCode::ProducerFenced));
}

TEST(TxnMessages, TxnOffsetCommitRoundTrip) {
    const uint8_t txnId[]  = {'t', 'o', 'c'};
    const uint8_t grpId[]  = {'g', 'r', 'p', '2'};
    const uint8_t meta[]   = {0xDE, 0xAD};

    rpc::TxnOffsetCommit in;
    in.sessionId       = 55;
    in.correlationId   = 66;
    in.transactionalId = {txnId, sizeof(txnId)};
    in.producerId      = 321LL;
    in.producerEpoch   = 4;
    in.groupId         = {grpId, sizeof(grpId)};

    const rpc::TxnOffsetCommitEntry entries[] = {
        {10, 0, 500LL, {meta, sizeof(meta)}},
        {20, 3, 700LL, {}}
    };
    const std::span<const rpc::TxnOffsetCommitEntry> entSpan(entries, 2);

    std::vector<uint8_t> buf(in.encodedSizeFor(
        {txnId, sizeof(txnId)}, {grpId, sizeof(grpId)}, entSpan));
    ASSERT_EQ(in.encode(buf.data(), entSpan), buf.size());

    auto out = rpc::TxnOffsetCommit::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    321LL);
    EXPECT_EQ(out->producerEpoch, 4);
    ASSERT_EQ(out->groupId.size(), sizeof(grpId));
    ASSERT_EQ(out->entries.size(), 2u);

    auto it = out->entries.begin();
    auto e0 = *it;
    EXPECT_EQ(e0.topicId,   10);
    EXPECT_EQ(e0.partition, 0);
    EXPECT_EQ(e0.offset,    500LL);
    ASSERT_EQ(e0.metadata.size(), sizeof(meta));
    EXPECT_EQ(std::memcmp(e0.metadata.data(), meta, sizeof(meta)), 0);

    ++it;
    auto e1 = *it;
    EXPECT_EQ(e1.topicId,   20);
    EXPECT_EQ(e1.partition, 3);
    EXPECT_EQ(e1.offset,    700LL);
    EXPECT_EQ(e1.metadata.size(), 0u);
}

TEST(TxnMessages, TxnOffsetCommitResponseRoundTrip) {
    rpc::TxnOffsetCommitResponse in;
    in.sessionId     = 7;
    in.correlationId = 8;

    const rpc::TxnPartitionResult results[] = {{5, 2, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence)}};
    const std::span<const rpc::TxnPartitionResult> resSpan(results, 1);

    std::vector<uint8_t> buf(rpc::TxnOffsetCommitResponse::encodedSizeFor(1));
    ASSERT_EQ(in.encode(buf.data(), resSpan), buf.size());

    auto out = rpc::TxnOffsetCommitResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    ASSERT_EQ(out->results.size(), 1u);
    EXPECT_EQ(out->results[0].errorCode,
              static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
}

// ---------------------------------------------------------------------------
// Task 4: Dispatch — txn template ids 45-54 must be known
// ---------------------------------------------------------------------------

TEST(TxnDispatch, TxnTemplateIdsAreKnown) {
    for (uint16_t tid = 45; tid <= 54; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid " << tid << " should be known";
    }
}

TEST(TxnDispatch, TemplateIdBeforeTxnRangeUnknown) {
    // 45 is the first txn id — nothing new between 45 (exclusive) and 44 (last group)
    // Gap between 44 (Assign) and 45 is empty so just verify 44 is still known.
    EXPECT_TRUE(rpc::isKnownTemplate(44)); // Assign
    EXPECT_FALSE(rpc::isKnownTemplate(55)); // first unknown above txn range
}

// Build a valid frame for any templateId, assert peekTemplateId round-trips.
static std::vector<uint8_t> makeTxnFrame(uint16_t tid) {
    // Use InitProducerIdResponse shape (fixed-only, ENCODED_SIZE = 24 + 14 = 38)
    // but override the templateId in the header bytes manually.
    rpc::InitProducerIdResponse msg;
    msg.sessionId     = 0x1122334455667788LL;
    msg.correlationId = 0x0A0B0C0D0E0F1011LL;
    msg.producerId    = 0;
    msg.producerEpoch = 0;
    msg.errorCode     = 0;
    std::vector<uint8_t> buf(rpc::InitProducerIdResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    // Patch templateId bytes [2..3] (LE16)
    buf[2] = static_cast<uint8_t>(tid & 0xFF);
    buf[3] = static_cast<uint8_t>((tid >> 8) & 0xFF);
    return buf;
}

TEST(TxnDispatch, PeekTemplateIdReturnsTxnIds) {
    for (uint16_t tid = 45; tid <= 54; ++tid) {
        // Use InitProducerIdResponse BODY_FIXED but patched tid
        auto buf = makeTxnFrame(tid);
        // Also patch blockLength to match what's expected for this decode size
        // (we just need checkFrame to accept it — it only checks schema/version/tid)
        auto fc = rpc::checkFrame(buf.data(), buf.size());
        EXPECT_EQ(fc.status, rpc::FrameStatus::Ok)
            << "checkFrame failed for txn templateId " << tid;
        EXPECT_EQ(fc.templateId, tid);
        auto peeked = rpc::peekTemplateId(buf.data(), buf.size());
        ASSERT_TRUE(peeked.has_value()) << "peekTemplateId nullopt for tid " << tid;
        EXPECT_EQ(*peeked, tid);
    }
}

// ---------------------------------------------------------------------------
// Task 4: Conformance vectors — inline golden byte arrays
// (mirrors the pattern in ConformanceTest.cpp but without file I/O;
//  the golden bytes are computed by hand from the field values below)
// ---------------------------------------------------------------------------

// Common conformance values (same as ConformanceTest.cpp)
static constexpr int64_t kSessId = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

// Helper: read LE16 from raw bytes
static uint16_t leU16c(const uint8_t* b, int o) {
    return static_cast<uint16_t>(uint16_t(b[o]) | (uint16_t(b[o+1]) << 8));
}
static int64_t leS64c(const uint8_t* b, int o) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[o+i]) << (i*8);
    return static_cast<int64_t>(v);
}

// --- InitProducerId conformance ---
// InitProducerId{ transactionalId="txn1", transactionTimeoutMs=60000 }
// Frame: 24(hdr) + 4(fixed) + 2+4(txnId) + = 34 bytes
// BODY_FIXED = 4 (transactionTimeoutMs)
static constexpr uint8_t kTxnId1[] = {'t', 'x', 'n', '1'};
static constexpr int32_t kTxnTimeoutMs = 60000;

TEST(TxnConformance, InitProducerIdGolden) {
    rpc::InitProducerId msg;
    msg.sessionId            = kSessId;
    msg.correlationId        = kCorrId;
    msg.transactionalId      = {kTxnId1, sizeof(kTxnId1)};
    msg.transactionTimeoutMs = kTxnTimeoutMs;

    std::vector<uint8_t> enc(msg.encodedSize());
    const std::size_t n = msg.encode(enc.data());
    // 24 + 4 + 2 + 4 = 34
    ASSERT_EQ(n, 34u);

    // Header checks
    EXPECT_EQ(leU16c(enc.data(), 0), rpc::InitProducerId::BODY_FIXED);   // blockLength=4
    EXPECT_EQ(leU16c(enc.data(), 2), rpc::InitProducerId::TEMPLATE_ID);  // 45
    EXPECT_EQ(leU16c(enc.data(), 4), rpc::kSchemaId);                    // 120
    EXPECT_EQ(leU16c(enc.data(), 6), rpc::kProtocolVersion);             // 1
    EXPECT_EQ(leS64c(enc.data(), 8),  kSessId);
    EXPECT_EQ(leS64c(enc.data(), 16), kCorrId);

    // Decode round-trip
    auto out = rpc::InitProducerId::decode(enc.data(), enc.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->sessionId,            kSessId);
    EXPECT_EQ(out->correlationId,        kCorrId);
    EXPECT_EQ(out->transactionTimeoutMs, kTxnTimeoutMs);
    ASSERT_EQ(out->transactionalId.size(), sizeof(kTxnId1));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), kTxnId1, sizeof(kTxnId1)), 0);
}

// --- AddPartitionsToTxn conformance ---
// AddPartitionsToTxn{ transactionalId="txn1", producerId=9999, producerEpoch=2,
//                     partitions=[(topicId=100,partition=1)] }
TEST(TxnConformance, AddPartitionsToTxnGolden) {
    static constexpr rpc::TxnTopicPartition kParts[] = {{100, 1}};

    rpc::AddPartitionsToTxn msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;

    const std::span<const rpc::TxnTopicPartition> ps(kParts, 1);
    std::vector<uint8_t> enc(rpc::AddPartitionsToTxn::encodedSizeFor(
        {kTxnId1, sizeof(kTxnId1)}, 1));
    const std::size_t n = msg.encode(enc.data(), ps);

    // BODY_FIXED = 10 (producerId8 + producerEpoch2), var: txnId(2+4), count(2), 1*8
    EXPECT_GT(n, 0u); // sanity

    auto out = rpc::AddPartitionsToTxn::decode(enc.data(), enc.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    ASSERT_EQ(out->partitions.size(), 1u);
    EXPECT_EQ(out->partitions[0].topicId,   100);
    EXPECT_EQ(out->partitions[0].partition, 1);
    EXPECT_EQ(leU16c(enc.data(), 2), rpc::AddPartitionsToTxn::TEMPLATE_ID); // 47
}

// --- TxnOffsetCommit conformance ---
// TxnOffsetCommit{ transactionalId="txn1", producerId=9999, producerEpoch=2,
//                  groupId="grp1", entries=[(topicId=5,partition=0,offset=200,meta={0xAB})] }
TEST(TxnConformance, TxnOffsetCommitGolden) {
    static constexpr uint8_t kGrpId[] = {'g', 'r', 'p', '1'};
    static constexpr uint8_t kMeta[]  = {0xAB};

    rpc::TxnOffsetCommit msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;
    msg.groupId         = {kGrpId, sizeof(kGrpId)};

    const rpc::TxnOffsetCommitEntry entries[] = {{5, 0, 200LL, {kMeta, sizeof(kMeta)}}};
    const std::span<const rpc::TxnOffsetCommitEntry> es(entries, 1);

    std::vector<uint8_t> enc(msg.encodedSizeFor(
        {kTxnId1, sizeof(kTxnId1)}, {kGrpId, sizeof(kGrpId)}, es));
    const std::size_t n = msg.encode(enc.data(), es);
    EXPECT_GT(n, 0u);

    auto out = rpc::TxnOffsetCommit::decode(enc.data(), enc.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    ASSERT_EQ(out->entries.size(), 1u);

    auto e0 = out->entries[0];
    EXPECT_EQ(e0.topicId,   5);
    EXPECT_EQ(e0.partition, 0);
    EXPECT_EQ(e0.offset,    200LL);
    ASSERT_EQ(e0.metadata.size(), sizeof(kMeta));
    EXPECT_EQ(e0.metadata[0], 0xABu);
    EXPECT_EQ(leU16c(enc.data(), 2), rpc::TxnOffsetCommit::TEMPLATE_ID); // 51
}

// --- EndTxn conformance ---
// EndTxn{ transactionalId="txn1", producerId=9999, producerEpoch=2, commit=1 }
TEST(TxnConformance, EndTxnGolden) {
    rpc::EndTxn msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;
    msg.commit          = 1;

    std::vector<uint8_t> enc(msg.encodedSize());
    const std::size_t n = msg.encode(enc.data());
    EXPECT_GT(n, 0u);

    auto out = rpc::EndTxn::decode(enc.data(), enc.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    EXPECT_EQ(out->commit,        1u);
    EXPECT_EQ(leU16c(enc.data(), 2), rpc::EndTxn::TEMPLATE_ID); // 53
    EXPECT_EQ(leS64c(enc.data(), 8),  kSessId);
    EXPECT_EQ(leS64c(enc.data(), 16), kCorrId);
}

// ===========================================================================
// TX-7: Fixed golden conformance vectors for txn messages (templateIds 45–54).
//
// The tests above (TxnConformance.*Golden) are self-referential — they encode()
// then decode() the same bytes and check field round-trips.  These new tests are
// INDEPENDENT: the expected wire layout is hand-derived byte-by-byte from the
// documented field layout in TxnMessages.h and are compared via memcmp.
//
// Common wire values for all TX-7 golden tests:
//   sessionId     = 0x1122334455667788  LE64 → 88 77 66 55 44 33 22 11
//   correlationId = 0x0A0B0C0D0E0F1011  LE64 → 11 10 0F 0E 0D 0C 0B 0A
//   schemaId      = 120 (0x0078)        LE16 → 78 00
//   version       = 1                   LE16 → 01 00
//
// The MessageHeader (24 bytes) uses these offsets:
//   [0..1]  blockLength  LE16
//   [2..3]  templateId   LE16
//   [4..5]  schemaId     LE16  = 78 00
//   [6..7]  version      LE16  = 01 00
//   [8..15] sessionId    LE64
//   [16..23] correlationId LE64
// ===========================================================================

// ---------------------------------------------------------------------------
// TX-7-1: InitProducerId byte-exact golden
//
// Fields: transactionalId="txn1"(4), transactionTimeoutMs=60000
// Layout:
//   [0..1]  blockLength=4         → 04 00
//   [2..3]  templateId=45         → 2D 00
//   [4..5]  schemaId=120          → 78 00
//   [6..7]  version=1             → 01 00
//   [8..15] sessionId             → 88 77 66 55 44 33 22 11
//   [16..23] correlationId        → 11 10 0F 0E 0D 0C 0B 0A
//   [24..27] transactionTimeoutMs=60000 (0xEA60) LE32 → 60 EA 00 00
//   [28..29] txnId len=4          → 04 00
//   [30..33] "txn1"               → 74 78 6E 31
// Total = 34 bytes
// ---------------------------------------------------------------------------
TEST(TxnConformance, ByteExactInitProducerId) {
    // Hand-derived independent golden vector.
    static constexpr uint8_t kExpected[34] = {
        // MessageHeader (24 bytes)
        0x04, 0x00,                          // blockLength = 4
        0x2D, 0x00,                          // templateId  = 45
        0x78, 0x00,                          // schemaId    = 120
        0x02, 0x00,                          // version     = 2 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        // Fixed body (4 bytes): transactionTimeoutMs = 60000 = 0x0000EA60
        0x60, 0xEA, 0x00, 0x00,
        // Var: txnId len=4, "txn1"
        0x04, 0x00,
        0x74, 0x78, 0x6E, 0x31,
    };

    // 1. encode() output must match the golden exactly.
    rpc::InitProducerId msg;
    msg.sessionId            = kSessId;
    msg.correlationId        = kCorrId;
    msg.transactionalId      = {kTxnId1, sizeof(kTxnId1)};
    msg.transactionTimeoutMs = kTxnTimeoutMs;

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, sizeof(kExpected));
    EXPECT_EQ(std::memcmp(enc.data(), kExpected, sizeof(kExpected)), 0)
        << "InitProducerId encode() differs from hand-derived golden";

    // 2. decode(golden) must yield the expected fields.
    auto out = rpc::InitProducerId::decode(kExpected, sizeof(kExpected));
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId,            kSessId);
    EXPECT_EQ(out->correlationId,        kCorrId);
    EXPECT_EQ(out->transactionTimeoutMs, kTxnTimeoutMs);
    ASSERT_EQ(out->transactionalId.size(), sizeof(kTxnId1));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), kTxnId1, sizeof(kTxnId1)), 0);
}

// ---------------------------------------------------------------------------
// TX-7-2: AddPartitionsToTxn byte-exact golden
//
// Fields: producerId=9999, producerEpoch=2, transactionalId="txn1"(4),
//         partitions=[(topicId=100, partition=1)]
// Layout:
//   [0..1]  blockLength=10        → 0A 00
//   [2..3]  templateId=47         → 2F 00
//   [4..5]  schemaId=120          → 78 00
//   [6..7]  version=2             → 02 00  (E5.1 bump)
//   [8..15] sessionId             → 88 77 66 55 44 33 22 11
//   [16..23] correlationId        → 11 10 0F 0E 0D 0C 0B 0A
//   [24..31] producerId=9999 LE64 → 0F 27 00 00 00 00 00 00
//   [32..33] producerEpoch=2 LE16 → 02 00
//   [34..35] txnId len=4          → 04 00
//   [36..39] "txn1"               → 74 78 6E 31
//   [40..41] partitions count=1   → 01 00
//   [42..45] topicId=100 LE32     → 64 00 00 00
//   [46..49] partition=1 LE32     → 01 00 00 00
// Total = 50 bytes
// ---------------------------------------------------------------------------
TEST(TxnConformance, ByteExactAddPartitionsToTxn) {
    static constexpr uint8_t kExpected[50] = {
        // MessageHeader (24 bytes)
        0x0A, 0x00,                          // blockLength = 10
        0x2F, 0x00,                          // templateId  = 47
        0x78, 0x00,                          // schemaId    = 120
        0x02, 0x00,                          // version     = 2 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        // Fixed body (10 bytes): producerId(8) + producerEpoch(2)
        0x0F, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // producerId = 9999
        0x02, 0x00,                                        // producerEpoch = 2
        // Var: txnId len=4, "txn1"
        0x04, 0x00,
        0x74, 0x78, 0x6E, 0x31,
        // Array: count=1, entry (topicId=100, partition=1) each LE32
        0x01, 0x00,
        0x64, 0x00, 0x00, 0x00,  // topicId = 100
        0x01, 0x00, 0x00, 0x00,  // partition = 1
    };

    static constexpr rpc::TxnTopicPartition kParts[] = {{100, 1}};

    // 1. encode() output must match the golden exactly.
    rpc::AddPartitionsToTxn msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;

    const std::span<const rpc::TxnTopicPartition> ps(kParts, 1);
    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data(), ps);
    ASSERT_EQ(n, sizeof(kExpected));
    EXPECT_EQ(std::memcmp(enc.data(), kExpected, sizeof(kExpected)), 0)
        << "AddPartitionsToTxn encode() differs from hand-derived golden";

    // 2. decode(golden) must yield the expected fields.
    auto out = rpc::AddPartitionsToTxn::decode(kExpected, sizeof(kExpected));
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId,     kSessId);
    EXPECT_EQ(out->correlationId, kCorrId);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    ASSERT_EQ(out->transactionalId.size(), sizeof(kTxnId1));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), kTxnId1, sizeof(kTxnId1)), 0);
    ASSERT_EQ(out->partitions.size(), 1u);
    EXPECT_EQ(out->partitions[0].topicId,   100);
    EXPECT_EQ(out->partitions[0].partition, 1);
}

// ---------------------------------------------------------------------------
// TX-7-3: TxnOffsetCommit byte-exact golden
//
// Fields: producerId=9999, producerEpoch=2, transactionalId="txn1"(4),
//         groupId="grp1"(4),
//         entries=[(topicId=5, partition=0, offset=200, metadata={0xAB})]
// Layout:
//   [0..1]  blockLength=10        → 0A 00
//   [2..3]  templateId=51         → 33 00
//   [4..5]  schemaId=120          → 78 00
//   [6..7]  version=2             → 02 00  (E5.1 bump)
//   [8..15] sessionId             → 88 77 66 55 44 33 22 11
//   [16..23] correlationId        → 11 10 0F 0E 0D 0C 0B 0A
//   [24..31] producerId=9999 LE64 → 0F 27 00 00 00 00 00 00
//   [32..33] producerEpoch=2 LE16 → 02 00
//   [34..35] txnId len=4          → 04 00
//   [36..39] "txn1"               → 74 78 6E 31
//   [40..41] groupId len=4        → 04 00
//   [42..45] "grp1"               → 67 72 70 31
//   [46..47] entries count=1      → 01 00
//   Entry 0:
//   [48..51] topicId=5 LE32       → 05 00 00 00
//   [52..55] partition=0 LE32     → 00 00 00 00
//   [56..63] offset=200 LE64      → C8 00 00 00 00 00 00 00
//   [64..65] meta len=1           → 01 00
//   [66]     0xAB
// Total = 67 bytes
// ---------------------------------------------------------------------------
TEST(TxnConformance, ByteExactTxnOffsetCommit) {
    static constexpr uint8_t kGrpId[] = {'g', 'r', 'p', '1'};
    static constexpr uint8_t kMeta[]  = {0xAB};

    static constexpr uint8_t kExpected[67] = {
        // MessageHeader (24 bytes)
        0x0A, 0x00,                          // blockLength = 10
        0x33, 0x00,                          // templateId  = 51
        0x78, 0x00,                          // schemaId    = 120
        0x02, 0x00,                          // version     = 2 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        // Fixed body (10 bytes): producerId(8) + producerEpoch(2)
        0x0F, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // producerId = 9999
        0x02, 0x00,                                        // producerEpoch = 2
        // Var: txnId len=4, "txn1"
        0x04, 0x00,
        0x74, 0x78, 0x6E, 0x31,
        // Var: groupId len=4, "grp1"
        0x04, 0x00,
        0x67, 0x72, 0x70, 0x31,
        // entries count=1
        0x01, 0x00,
        // Entry 0: topicId=5(LE32), partition=0(LE32), offset=200(LE64), metaLen=1, 0xAB
        0x05, 0x00, 0x00, 0x00,              // topicId = 5
        0x00, 0x00, 0x00, 0x00,              // partition = 0
        0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // offset = 200
        0x01, 0x00,                          // meta len = 1
        0xAB,                                // metadata byte
    };

    // 1. encode() output must match the golden exactly.
    rpc::TxnOffsetCommit msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;
    msg.groupId         = {kGrpId, sizeof(kGrpId)};

    const rpc::TxnOffsetCommitEntry entries[] = {{5, 0, 200LL, {kMeta, sizeof(kMeta)}}};
    const std::span<const rpc::TxnOffsetCommitEntry> es(entries, 1);

    std::array<uint8_t, 128> enc{};
    const std::size_t n = msg.encode(enc.data(), es);
    ASSERT_EQ(n, sizeof(kExpected));
    EXPECT_EQ(std::memcmp(enc.data(), kExpected, sizeof(kExpected)), 0)
        << "TxnOffsetCommit encode() differs from hand-derived golden";

    // 2. decode(golden) must yield the expected fields.
    auto out = rpc::TxnOffsetCommit::decode(kExpected, sizeof(kExpected));
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId,     kSessId);
    EXPECT_EQ(out->correlationId, kCorrId);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    ASSERT_EQ(out->transactionalId.size(), sizeof(kTxnId1));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), kTxnId1, sizeof(kTxnId1)), 0);
    ASSERT_EQ(out->groupId.size(), sizeof(kGrpId));
    EXPECT_EQ(std::memcmp(out->groupId.data(), kGrpId, sizeof(kGrpId)), 0);
    ASSERT_EQ(out->entries.size(), 1u);
    auto e0 = out->entries[0];
    EXPECT_EQ(e0.topicId,   5);
    EXPECT_EQ(e0.partition, 0);
    EXPECT_EQ(e0.offset,    200LL);
    ASSERT_EQ(e0.metadata.size(), sizeof(kMeta));
    EXPECT_EQ(e0.metadata[0], 0xABu);
}

// ---------------------------------------------------------------------------
// TX-7-4: EndTxn byte-exact golden
//
// Fields: producerId=9999, producerEpoch=2, commit=1, transactionalId="txn1"(4)
// Layout:
//   [0..1]  blockLength=11        → 0B 00
//   [2..3]  templateId=53         → 35 00
//   [4..5]  schemaId=120          → 78 00
//   [6..7]  version=2             → 02 00  (E5.1 bump)
//   [8..15] sessionId             → 88 77 66 55 44 33 22 11
//   [16..23] correlationId        → 11 10 0F 0E 0D 0C 0B 0A
//   [24..31] producerId=9999 LE64 → 0F 27 00 00 00 00 00 00
//   [32..33] producerEpoch=2 LE16 → 02 00
//   [34]     commit=1             → 01
//   [35..36] txnId len=4          → 04 00
//   [37..40] "txn1"               → 74 78 6E 31
// Total = 41 bytes
// ---------------------------------------------------------------------------
TEST(TxnConformance, ByteExactEndTxn) {
    static constexpr uint8_t kExpected[41] = {
        // MessageHeader (24 bytes)
        0x0B, 0x00,                          // blockLength = 11
        0x35, 0x00,                          // templateId  = 53
        0x78, 0x00,                          // schemaId    = 120
        0x02, 0x00,                          // version     = 2 (E5.1 bump)
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
        0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
        // Fixed body (11 bytes): producerId(8) + producerEpoch(2) + commit(1)
        0x0F, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // producerId = 9999
        0x02, 0x00,                                        // producerEpoch = 2
        0x01,                                              // commit = 1
        // Var: txnId len=4, "txn1"
        0x04, 0x00,
        0x74, 0x78, 0x6E, 0x31,
    };

    // 1. encode() output must match the golden exactly.
    rpc::EndTxn msg;
    msg.sessionId       = kSessId;
    msg.correlationId   = kCorrId;
    msg.transactionalId = {kTxnId1, sizeof(kTxnId1)};
    msg.producerId      = 9999LL;
    msg.producerEpoch   = 2;
    msg.commit          = 1;

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, sizeof(kExpected));
    EXPECT_EQ(std::memcmp(enc.data(), kExpected, sizeof(kExpected)), 0)
        << "EndTxn encode() differs from hand-derived golden";

    // 2. decode(golden) must yield the expected fields.
    auto out = rpc::EndTxn::decode(kExpected, sizeof(kExpected));
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId,     kSessId);
    EXPECT_EQ(out->correlationId, kCorrId);
    EXPECT_EQ(out->producerId,    9999LL);
    EXPECT_EQ(out->producerEpoch, 2);
    EXPECT_EQ(out->commit,        1u);
    ASSERT_EQ(out->transactionalId.size(), sizeof(kTxnId1));
    EXPECT_EQ(std::memcmp(out->transactionalId.data(), kTxnId1, sizeof(kTxnId1)), 0);
}
