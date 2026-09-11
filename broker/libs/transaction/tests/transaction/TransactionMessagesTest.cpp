// ---------------------------------------------------------------------------
// TransactionMessagesTest.cpp — Unit tests for TransactionMessages SBE codecs.
//
// TDD: byte-exact LE round-trip for ALL messages; header fields; static layout.
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionMessages.h>

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

using namespace rune::transaction;

namespace {

// Buffer size (generous upper bound for any message).
constexpr std::size_t kBufSize = 4096;

// Build a TopicPartition.
static TopicPartition tp(std::string t, int32_t p) {
    return TopicPartition{std::move(t), p};
}

} // anonymous namespace

// ===========================================================================
// Header tests
// ===========================================================================

TEST(TransactionMessagesTest, HeaderEncodedSize) {
    static_assert(TxnMessageHeader::ENCODED_SIZE == 8, "header must be 8 bytes");
    SUCCEED();
}

TEST(TransactionMessagesTest, HeaderRoundTrip) {
    uint8_t buf[8] = {};
    TxnMessageHeader::encode(buf, 0, /*blockLen=*/12, /*tmplId=*/3);
    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.blockLength, 12);
    EXPECT_EQ(h.templateId,  3);
    EXPECT_EQ(h.schemaId,    kTxnSchemaId);
    EXPECT_EQ(h.version,     kTxnSchemaVersion);
}

TEST(TransactionMessagesTest, PeekTemplateId) {
    uint8_t buf[8] = {};
    TxnMessageHeader::encode(buf, 0, 4, static_cast<uint16_t>(TxnTemplateId::EndTxnRequest));
    EXPECT_EQ(peekTxnTemplateId(buf), static_cast<uint16_t>(TxnTemplateId::EndTxnRequest));
}

TEST(TransactionMessagesTest, SchemaConstants) {
    EXPECT_EQ(kTxnSchemaId,      114u);
    EXPECT_EQ(kTxnSchemaVersion, 1u);
}

// ===========================================================================
// InitProducerIdRequest (templateId = 1)
// ===========================================================================

TEST(TransactionMessagesTest, InitProducerIdRequestBlockLength) {
    static_assert(InitProducerIdRequestMsg::BLOCK_LENGTH == 4);
    SUCCEED();
}

TEST(TransactionMessagesTest, InitProducerIdRequestRoundTrip) {
    uint8_t buf[kBufSize] = {};
    auto n = InitProducerIdRequestMsg::encode(buf, 0, 30'000, "my-txn-id");
    EXPECT_GT(n, 0u);

    auto m = InitProducerIdRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.txnTimeoutMs, 30'000);
    EXPECT_EQ(m.transactionalId, "my-txn-id");
}

TEST(TransactionMessagesTest, InitProducerIdRequestEmptyTxnId) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdRequestMsg::encode(buf, 0, 60'000, "");

    auto m = InitProducerIdRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.txnTimeoutMs, 60'000);
    EXPECT_EQ(m.transactionalId, "");
}

TEST(TransactionMessagesTest, InitProducerIdRequestLongTxnId) {
    uint8_t buf[kBufSize] = {};
    std::string longId(200, 'x');
    InitProducerIdRequestMsg::encode(buf, 0, 1'000, longId);

    auto m = InitProducerIdRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.transactionalId, longId);
}

TEST(TransactionMessagesTest, InitProducerIdRequestHeaderFields) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdRequestMsg::encode(buf, 0, 1'000, "test");

    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.templateId,  static_cast<uint16_t>(TxnTemplateId::InitProducerIdRequest));
    EXPECT_EQ(h.schemaId,    kTxnSchemaId);
    EXPECT_EQ(h.blockLength, InitProducerIdRequestMsg::BLOCK_LENGTH);
}

// ===========================================================================
// InitProducerIdResponse (templateId = 2)
// ===========================================================================

TEST(TransactionMessagesTest, InitProducerIdResponseBlockLength) {
    static_assert(InitProducerIdResponseMsg::BLOCK_LENGTH == 12);
    SUCCEED();
}

TEST(TransactionMessagesTest, InitProducerIdResponseRoundTrip) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdResponseMsg::encode(buf, 0, /*pid=*/123456789LL,
                                       /*epoch=*/7, /*errorCode=*/0);

    auto m = InitProducerIdResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.producerId,    123456789LL);
    EXPECT_EQ(m.producerEpoch, 7);
    EXPECT_EQ(m.errorCode,     0);
}

TEST(TransactionMessagesTest, InitProducerIdResponseErrorCode) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdResponseMsg::encode(buf, 0, -1, -1, /*errorCode=*/5);

    auto m = InitProducerIdResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.producerId,    -1);
    EXPECT_EQ(m.producerEpoch, -1);
    EXPECT_EQ(m.errorCode,     5);
}

TEST(TransactionMessagesTest, InitProducerIdResponseHeaderFields) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdResponseMsg::encode(buf, 0, 1, 0, 0);

    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.templateId,  static_cast<uint16_t>(TxnTemplateId::InitProducerIdResponse));
    EXPECT_EQ(h.blockLength, InitProducerIdResponseMsg::BLOCK_LENGTH);
}

// ===========================================================================
// AddPartitionsToTxnRequest (templateId = 3)
// ===========================================================================

TEST(TransactionMessagesTest, AddPartitionsBlockLength) {
    static_assert(AddPartitionsToTxnRequestMsg::BLOCK_LENGTH == 10);
    SUCCEED();
}

TEST(TransactionMessagesTest, AddPartitionsRoundTripEmpty) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts;
    AddPartitionsToTxnRequestMsg::encode(buf, 0, 99LL, 2, "txn-x", parts);

    auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.producerId,       99LL);
    EXPECT_EQ(m.producerEpoch,    2);
    EXPECT_EQ(m.transactionalId,  "txn-x");
    EXPECT_EQ(m.partitions.size(), 0u);
}

TEST(TransactionMessagesTest, AddPartitionsRoundTripOne) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("my-topic", 5)};
    AddPartitionsToTxnRequestMsg::encode(buf, 0, 1LL, 0, "txn-y", parts);

    auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0);
    ASSERT_EQ(m.partitions.size(), 1u);
    EXPECT_EQ(m.partitions[0].topic,     "my-topic");
    EXPECT_EQ(m.partitions[0].partition, 5);
}

TEST(TransactionMessagesTest, AddPartitionsRoundTripMany) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts;
    for (int i = 0; i < 20; ++i) {
        parts.push_back(tp("t" + std::to_string(i), i));
    }
    AddPartitionsToTxnRequestMsg::encode(buf, 0, 5LL, 1, "txn-many", parts);

    auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0);
    ASSERT_EQ(m.partitions.size(), 20u);
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(m.partitions[i].partition, i);
    }
}

TEST(TransactionMessagesTest, AddPartitionsHeaderFields) {
    uint8_t buf[kBufSize] = {};
    AddPartitionsToTxnRequestMsg::encode(buf, 0, 1LL, 0, "t", {});

    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.templateId,  static_cast<uint16_t>(TxnTemplateId::AddPartitionsToTxnRequest));
    EXPECT_EQ(h.blockLength, AddPartitionsToTxnRequestMsg::BLOCK_LENGTH);
}

// ===========================================================================
// AddPartitionsToTxnResponse (templateId = 4)
// ===========================================================================

TEST(TransactionMessagesTest, AddPartitionsResponseBlockLength) {
    static_assert(AddPartitionsToTxnResponseMsg::BLOCK_LENGTH == 2);
    SUCCEED();
}

TEST(TransactionMessagesTest, AddPartitionsResponseRoundTrip) {
    uint8_t buf[kBufSize] = {};
    AddPartitionsToTxnResponseMsg::encode(buf, 0, /*errorCode=*/0);

    auto m = AddPartitionsToTxnResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, 0);
}

TEST(TransactionMessagesTest, AddPartitionsResponseError) {
    uint8_t buf[kBufSize] = {};
    AddPartitionsToTxnResponseMsg::encode(buf, 0, /*errorCode=*/3);

    auto m = AddPartitionsToTxnResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, 3);
}

// ===========================================================================
// EndTxnRequest (templateId = 5)
// ===========================================================================

TEST(TransactionMessagesTest, EndTxnRequestBlockLength) {
    static_assert(EndTxnRequestMsg::BLOCK_LENGTH == 12);
    SUCCEED();
}

TEST(TransactionMessagesTest, EndTxnRequestRoundTripCommit) {
    uint8_t buf[kBufSize] = {};
    EndTxnRequestMsg::encode(buf, 0, 42LL, 3, /*commit=*/true, "txn-commit");

    auto m = EndTxnRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.producerId,       42LL);
    EXPECT_EQ(m.producerEpoch,    3);
    EXPECT_TRUE(m.isCommit());
    EXPECT_EQ(m.transactionalId,  "txn-commit");
}

TEST(TransactionMessagesTest, EndTxnRequestRoundTripAbort) {
    uint8_t buf[kBufSize] = {};
    EndTxnRequestMsg::encode(buf, 0, 42LL, 3, /*commit=*/false, "txn-abort");

    auto m = EndTxnRequestMsg::decode(buf, 0);
    EXPECT_FALSE(m.isCommit());
}

TEST(TransactionMessagesTest, EndTxnRequestHeaderFields) {
    uint8_t buf[kBufSize] = {};
    EndTxnRequestMsg::encode(buf, 0, 1LL, 0, true, "t");

    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.templateId,  static_cast<uint16_t>(TxnTemplateId::EndTxnRequest));
    EXPECT_EQ(h.blockLength, EndTxnRequestMsg::BLOCK_LENGTH);
}

// ===========================================================================
// EndTxnResponse (templateId = 6)
// ===========================================================================

TEST(TransactionMessagesTest, EndTxnResponseBlockLength) {
    static_assert(EndTxnResponseMsg::BLOCK_LENGTH == 2);
    SUCCEED();
}

TEST(TransactionMessagesTest, EndTxnResponseRoundTrip) {
    uint8_t buf[kBufSize] = {};
    EndTxnResponseMsg::encode(buf, 0, /*errorCode=*/0);

    auto m = EndTxnResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, 0);
}

TEST(TransactionMessagesTest, EndTxnResponseErrorCode) {
    uint8_t buf[kBufSize] = {};
    EndTxnResponseMsg::encode(buf, 0, /*errorCode=*/4);

    auto m = EndTxnResponseMsg::decode(buf, 0);
    EXPECT_EQ(m.errorCode, 4);
}

// ===========================================================================
// WriteTxnMarkersRequest (templateId = 7)
// ===========================================================================

TEST(TransactionMessagesTest, WriteTxnMarkersRequestBlockLength) {
    static_assert(WriteTxnMarkersRequestMsg::BLOCK_LENGTH == 14);
    SUCCEED();
}

TEST(TransactionMessagesTest, WriteTxnMarkersRequestRoundTripCommit) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("p1", 0), tp("p2", 1)};
    WriteTxnMarkersRequestMsg::encode(buf, 0, 77LL, 2, ControlType::Commit, 5,
                                       "txn-mark", parts);

    auto m = WriteTxnMarkersRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.producerId,        77LL);
    EXPECT_EQ(m.producerEpoch,     2);
    EXPECT_EQ(m.controlTypeEnum(), ControlType::Commit);
    EXPECT_EQ(m.coordinatorEpoch,  5);
    EXPECT_EQ(m.transactionalId,   "txn-mark");
    ASSERT_EQ(m.partitions.size(), 2u);
    EXPECT_EQ(m.partitions[0].topic, "p1");
    EXPECT_EQ(m.partitions[1].topic, "p2");
}

TEST(TransactionMessagesTest, WriteTxnMarkersRequestRoundTripAbort) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("x", 7)};
    WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Abort, 0,
                                       "t", parts);

    auto m = WriteTxnMarkersRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.controlTypeEnum(), ControlType::Abort);
}

TEST(TransactionMessagesTest, WriteTxnMarkersRequestEmpty) {
    uint8_t buf[kBufSize] = {};
    WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Commit, 0, "t", {});

    auto m = WriteTxnMarkersRequestMsg::decode(buf, 0);
    EXPECT_EQ(m.partitions.size(), 0u);
}

TEST(TransactionMessagesTest, WriteTxnMarkersHeaderFields) {
    uint8_t buf[kBufSize] = {};
    WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Commit, 0, "t", {});

    auto h = TxnMessageHeader::decode(buf, 0);
    EXPECT_EQ(h.templateId,  static_cast<uint16_t>(TxnTemplateId::WriteTxnMarkersRequest));
    EXPECT_EQ(h.blockLength, WriteTxnMarkersRequestMsg::BLOCK_LENGTH);
}

TEST(TransactionMessagesTest, WriteTxnMarkersLargePartitionList) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts;
    for (int i = 0; i < 50; ++i) {
        parts.push_back(tp("topic-" + std::to_string(i), i));
    }
    WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Commit, 0, "t", parts);

    auto m = WriteTxnMarkersRequestMsg::decode(buf, 0);
    ASSERT_EQ(m.partitions.size(), 50u);
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(m.partitions[i].partition, i);
    }
}

// ===========================================================================
// Byte-exactness: verify little-endian layout at byte level
// ===========================================================================

TEST(TransactionMessagesTest, InitProducerIdResponseByteLayout) {
    uint8_t buf[kBufSize] = {};
    // pid = 0x0102030405060708, epoch = 0x090A, errorCode = 0x000B
    int64_t pid   = 0x0102030405060708LL;
    int16_t epoch = 0x090A;
    int16_t err   = 0x000B;
    InitProducerIdResponseMsg::encode(buf, 0, pid, epoch, err);

    // After the 8-byte header: pid at [8..15], epoch at [16..17], err at [18..19]
    // LE: pid[0]=0x08, pid[1]=0x07, ..., pid[7]=0x01
    EXPECT_EQ(buf[8],  0x08);
    EXPECT_EQ(buf[9],  0x07);
    EXPECT_EQ(buf[10], 0x06);
    EXPECT_EQ(buf[11], 0x05);
    EXPECT_EQ(buf[12], 0x04);
    EXPECT_EQ(buf[13], 0x03);
    EXPECT_EQ(buf[14], 0x02);
    EXPECT_EQ(buf[15], 0x01);
    EXPECT_EQ(buf[16], 0x0A);
    EXPECT_EQ(buf[17], 0x09);
    EXPECT_EQ(buf[18], 0x0B);
    EXPECT_EQ(buf[19], 0x00);
}

TEST(TransactionMessagesTest, EndTxnRequestByteLayout) {
    uint8_t buf[kBufSize] = {};
    // pid = 0x01, epoch = 0x0002, commit = 1 (=0x0001)
    EndTxnRequestMsg::encode(buf, 0, int64_t{1}, int16_t{2}, true, "ab");
    // After 8-byte header:
    //   pid      at [8..15]  = 0x01 LE = 01 00 00 00 00 00 00 00
    //   epoch    at [16..17] = 0x02 LE = 02 00
    //   commit   at [18..19] = 0x01 LE = 01 00
    //   txnIdLen at [20..21] = 2 LE   = 02 00
    //   txnId    at [22..23] = 'a','b'
    EXPECT_EQ(buf[8],  0x01);
    EXPECT_EQ(buf[9],  0x00);
    EXPECT_EQ(buf[16], 0x02);
    EXPECT_EQ(buf[17], 0x00);
    EXPECT_EQ(buf[18], 0x01);
    EXPECT_EQ(buf[22], 'a');
    EXPECT_EQ(buf[23], 'b');
}

TEST(TransactionMessagesTest, ControlTypeValues) {
    EXPECT_EQ(static_cast<int16_t>(ControlType::Commit), 0);
    EXPECT_EQ(static_cast<int16_t>(ControlType::Abort),  1);
}
