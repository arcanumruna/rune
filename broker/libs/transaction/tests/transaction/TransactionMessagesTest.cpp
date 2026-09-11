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

    auto mOpt = InitProducerIdRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
    EXPECT_EQ(m.txnTimeoutMs, 30'000);
    EXPECT_EQ(m.transactionalId, "my-txn-id");
}

TEST(TransactionMessagesTest, InitProducerIdRequestEmptyTxnId) {
    uint8_t buf[kBufSize] = {};
    InitProducerIdRequestMsg::encode(buf, 0, 60'000, "");

    auto mOpt = InitProducerIdRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
    EXPECT_EQ(m.txnTimeoutMs, 60'000);
    EXPECT_EQ(m.transactionalId, "");
}

TEST(TransactionMessagesTest, InitProducerIdRequestLongTxnId) {
    uint8_t buf[kBufSize] = {};
    std::string longId(200, 'x');
    InitProducerIdRequestMsg::encode(buf, 0, 1'000, longId);

    auto mOpt = InitProducerIdRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = AddPartitionsToTxnRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
    EXPECT_EQ(m.producerId,       99LL);
    EXPECT_EQ(m.producerEpoch,    2);
    EXPECT_EQ(m.transactionalId,  "txn-x");
    EXPECT_EQ(m.partitions.size(), 0u);
}

TEST(TransactionMessagesTest, AddPartitionsRoundTripOne) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("my-topic", 5)};
    AddPartitionsToTxnRequestMsg::encode(buf, 0, 1LL, 0, "txn-y", parts);

    auto mOpt = AddPartitionsToTxnRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = AddPartitionsToTxnRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = EndTxnRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
    EXPECT_EQ(m.producerId,       42LL);
    EXPECT_EQ(m.producerEpoch,    3);
    EXPECT_TRUE(m.isCommit());
    EXPECT_EQ(m.transactionalId,  "txn-commit");
}

TEST(TransactionMessagesTest, EndTxnRequestRoundTripAbort) {
    uint8_t buf[kBufSize] = {};
    EndTxnRequestMsg::encode(buf, 0, 42LL, 3, /*commit=*/false, "txn-abort");

    auto mOpt = EndTxnRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = WriteTxnMarkersRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = WriteTxnMarkersRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
    EXPECT_EQ(m.controlTypeEnum(), ControlType::Abort);
}

TEST(TransactionMessagesTest, WriteTxnMarkersRequestEmpty) {
    uint8_t buf[kBufSize] = {};
    WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Commit, 0, "t", {});

    auto mOpt = WriteTxnMarkersRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

    auto mOpt = WriteTxnMarkersRequestMsg::decode(buf, 0, kBufSize);
    ASSERT_TRUE(mOpt.has_value());
    auto& m = *mOpt;
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

// ===========================================================================
// FIX 3 — SBE bounds-checked read_str / read_partitions (truncated buffer)
// A malformed/short buffer must not over-read; must return error/empty/false.
// ===========================================================================

// read_str_safe: reads a uint16-prefixed string from buf[pos..end).
// If the declared length extends past end, returns false and does NOT advance pos.
TEST(TransactionMessagesTest, ReadStrSafeTruncatedHeaderReturnsError) {
    // Only 1 byte in buffer — not enough for the uint16 length prefix.
    uint8_t buf[1] = {0xFF};
    std::size_t pos = 0;
    std::size_t end = 1;
    bool ok = txn_detail::read_str_safe(buf, pos, end, /*out*/ nullptr);
    EXPECT_FALSE(ok);
    EXPECT_EQ(pos, 0u); // pos must not advance on error
}

TEST(TransactionMessagesTest, ReadStrSafeTruncatedBodyReturnsError) {
    // Declare length=10, but only 3 bytes of body present.
    uint8_t buf[2 + 3] = {};
    // write length prefix = 10
    buf[0] = 10; buf[1] = 0;
    // only 3 bytes of body follow
    buf[2] = 'a'; buf[3] = 'b'; buf[4] = 'c';
    std::size_t pos = 0;
    std::size_t end = sizeof(buf);
    std::string_view sv;
    bool ok = txn_detail::read_str_safe(buf, pos, end, &sv);
    EXPECT_FALSE(ok);
    EXPECT_EQ(pos, 0u);
}

TEST(TransactionMessagesTest, ReadStrSafeValidBufferSucceeds) {
    uint8_t buf[2 + 5] = {};
    buf[0] = 5; buf[1] = 0; // length = 5
    buf[2] = 'h'; buf[3] = 'e'; buf[4] = 'l'; buf[5] = 'l'; buf[6] = 'o';
    std::size_t pos = 0;
    std::size_t end = sizeof(buf);
    std::string_view sv;
    bool ok = txn_detail::read_str_safe(buf, pos, end, &sv);
    ASSERT_TRUE(ok);
    EXPECT_EQ(sv, "hello");
    EXPECT_EQ(pos, 7u);
}

TEST(TransactionMessagesTest, ReadPartitionsSafeTruncatedCountReturnsError) {
    // Only 1 byte — not enough for uint16 count.
    uint8_t buf[1] = {0xFF};
    std::size_t pos = 0;
    std::size_t end = 1;
    std::vector<TopicPartition> parts;
    bool ok = txn_detail::read_partitions_safe(buf, pos, end, parts);
    EXPECT_FALSE(ok);
    EXPECT_EQ(pos, 0u);
    EXPECT_TRUE(parts.empty());
}

TEST(TransactionMessagesTest, ReadPartitionsSafeTruncatedTupleReturnsError) {
    // count = 1, but tuple is incomplete (only 2 bytes of the 2+topicLen+4 tuple).
    uint8_t buf[4] = {};
    buf[0] = 1; buf[1] = 0;  // count = 1
    buf[2] = 5; buf[3] = 0;  // topicLen = 5, but no topic bytes follow
    std::size_t pos = 0;
    std::size_t end = sizeof(buf);
    std::vector<TopicPartition> parts;
    bool ok = txn_detail::read_partitions_safe(buf, pos, end, parts);
    EXPECT_FALSE(ok);
    EXPECT_EQ(pos, 0u);
    EXPECT_TRUE(parts.empty());
}

TEST(TransactionMessagesTest, ReadPartitionsSafeValidBufferSucceeds) {
    // count=1, topicLen=3, topic="abc", partition=42
    constexpr std::size_t sz = 2 + 2 + 3 + 4; // count + topicLen + "abc" + int32
    uint8_t buf[sz] = {};
    buf[0] = 1;  buf[1] = 0;  // count = 1
    buf[2] = 3;  buf[3] = 0;  // topicLen = 3
    buf[4] = 'a'; buf[5] = 'b'; buf[6] = 'c';
    buf[7] = 42; buf[8] = 0; buf[9] = 0; buf[10] = 0; // partition = 42 LE
    std::size_t pos = 0;
    std::size_t end = sz;
    std::vector<TopicPartition> parts;
    bool ok = txn_detail::read_partitions_safe(buf, pos, end, parts);
    ASSERT_TRUE(ok);
    ASSERT_EQ(parts.size(), 1u);
    EXPECT_EQ(parts[0].topic, "abc");
    EXPECT_EQ(parts[0].partition, 42);
    EXPECT_EQ(pos, sz);
}

// ===========================================================================
// A3/A4 — PUBLIC decode() bounds-safety (Part B: ASAN/truncated public decode).
//
// Decode a truncated frame via the PUBLIC decode() of EACH var-length request
// message and assert a clean nullopt (no over-read). Under ASAN these trip on
// any heap-buffer-overflow; without ASAN they still assert the all-or-nothing
// failure contract. Every field boundary is exercised.
// ===========================================================================

TEST(TransactionMessagesTest, PublicDecodeInitProducerIdTruncatedPerField) {
    uint8_t buf[kBufSize] = {};
    auto n = InitProducerIdRequestMsg::encode(buf, 0, 30'000, "my-txn-id");
    ASSERT_GT(n, 0u);
    ASSERT_TRUE(InitProducerIdRequestMsg::decode(buf, 0, n).has_value());
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto m = InitProducerIdRequestMsg::decode(buf, 0, avail);
        EXPECT_FALSE(m.has_value()) << "avail=" << avail;
    }
    EXPECT_FALSE(InitProducerIdRequestMsg::decode(buf, 0, 3).has_value());
}

TEST(TransactionMessagesTest, PublicDecodeAddPartitionsTruncatedPerField) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("topic-a", 1), tp("topic-b", 2)};
    auto n = AddPartitionsToTxnRequestMsg::encode(buf, 0, 7LL, 3, "txn", parts);
    ASSERT_GT(n, 0u);
    ASSERT_TRUE(AddPartitionsToTxnRequestMsg::decode(buf, 0, n).has_value());
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0, avail);
        EXPECT_FALSE(m.has_value()) << "avail=" << avail;
    }
}

// Crafted huge partition count with a short body must NOT over-read.
TEST(TransactionMessagesTest, PublicDecodeAddPartitionsCraftedHugeCount) {
    uint8_t buf[kBufSize] = {};
    auto n = AddPartitionsToTxnRequestMsg::encode(buf, 0, 1LL, 0, "t",
                                                  std::vector<TopicPartition>{});
    ASSERT_GT(n, 0u);
    buf[n - 2] = 0xFF; buf[n - 1] = 0xFF;   // partition-count prefix := 0xFFFF
    auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0, n);
    EXPECT_FALSE(m.has_value());
}

TEST(TransactionMessagesTest, PublicDecodeEndTxnTruncatedPerField) {
    uint8_t buf[kBufSize] = {};
    auto n = EndTxnRequestMsg::encode(buf, 0, 9LL, 1, true, "txn-e");
    ASSERT_GT(n, 0u);
    ASSERT_TRUE(EndTxnRequestMsg::decode(buf, 0, n).has_value());
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto m = EndTxnRequestMsg::decode(buf, 0, avail);
        EXPECT_FALSE(m.has_value()) << "avail=" << avail;
    }
}

TEST(TransactionMessagesTest, PublicDecodeWriteTxnMarkersTruncatedPerField) {
    uint8_t buf[kBufSize] = {};
    std::vector<TopicPartition> parts = {tp("p1", 0), tp("p2", 1)};
    auto n = WriteTxnMarkersRequestMsg::encode(buf, 0, 5LL, 2, ControlType::Abort,
                                               3, "txn-m", parts);
    ASSERT_GT(n, 0u);
    ASSERT_TRUE(WriteTxnMarkersRequestMsg::decode(buf, 0, n).has_value());
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto m = WriteTxnMarkersRequestMsg::decode(buf, 0, avail);
        EXPECT_FALSE(m.has_value()) << "avail=" << avail;
    }
}

// Empty txnId round-trips on each var-length request (Part B: empty txnId).
TEST(TransactionMessagesTest, PublicDecodeEmptyTxnIdRoundTrips) {
    uint8_t buf[kBufSize] = {};
    {
        auto n = EndTxnRequestMsg::encode(buf, 0, 1LL, 0, false, "");
        auto m = EndTxnRequestMsg::decode(buf, 0, n);
        ASSERT_TRUE(m.has_value());
        EXPECT_EQ(m->transactionalId, "");
    }
    {
        std::memset(buf, 0, sizeof(buf));
        auto n = AddPartitionsToTxnRequestMsg::encode(buf, 0, 1LL, 0, "",
                     std::vector<TopicPartition>{tp("t", 0)});
        auto m = AddPartitionsToTxnRequestMsg::decode(buf, 0, n);
        ASSERT_TRUE(m.has_value());
        EXPECT_EQ(m->transactionalId, "");
        ASSERT_EQ(m->partitions.size(), 1u);
    }
    {
        std::memset(buf, 0, sizeof(buf));
        auto n = WriteTxnMarkersRequestMsg::encode(buf, 0, 1LL, 0, ControlType::Commit,
                     0, "", std::vector<TopicPartition>{});
        auto m = WriteTxnMarkersRequestMsg::decode(buf, 0, n);
        ASSERT_TRUE(m.has_value());
        EXPECT_EQ(m->transactionalId, "");
    }
}

// A5: SBE encode REJECTS an over-long txnId (>65535) rather than truncate; a
// max-legal 65535-byte txnId round-trips (Part B: SBE max fields).
TEST(TransactionMessagesTest, EncodeRejectsOverLongTxnIdAndAcceptsMax) {
    std::vector<uint8_t> buf(200000, 0);
    std::string huge(65536, 'z');  // one past uint16 max
    EXPECT_EQ(InitProducerIdRequestMsg::encode(buf.data(), 0, 1000, huge), 0u);
    EXPECT_EQ(EndTxnRequestMsg::encode(buf.data(), 0, 1LL, 0, true, huge), 0u);
    EXPECT_EQ(AddPartitionsToTxnRequestMsg::encode(buf.data(), 0, 1LL, 0, huge, {}), 0u);
    EXPECT_EQ(WriteTxnMarkersRequestMsg::encode(buf.data(), 0, 1LL, 0,
                 ControlType::Commit, 0, huge, {}), 0u);

    std::string maxId(65535, 'q');
    auto n = InitProducerIdRequestMsg::encode(buf.data(), 0, 1000, maxId);
    ASSERT_GT(n, 0u);
    auto m = InitProducerIdRequestMsg::decode(buf.data(), 0, n);
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->transactionalId.size(), 65535u);
}

// ===========================================================================
// L2 — bounds-checked Response decode() overloads (avail-based, returns
// optional). These are the coordinator→client response decoders: templateId
// 2 (InitProducerIdResponse), 4 (AddPartitionsToTxnResponse), 6 (EndTxnResponse).
//
// All three are fixed-size-only (no var-length fields). Tests verify:
//   (a) a fully-present buffer decodes correctly (same values as existing decode),
//   (b) every truncation (avail in [0, full_size-1]) returns nullopt — no over-read.
// ===========================================================================

TEST(TransactionMessagesTest, L2InitProducerIdResponseDecodeBoundsChecked) {
    uint8_t buf[kBufSize] = {};
    // Encode a valid response.
    auto n = InitProducerIdResponseMsg::encode(buf, 0, /*pid=*/42LL,
                                               /*epoch=*/7, /*err=*/0);
    ASSERT_GT(n, 0u);

    // (a) Full buffer: avail = n → decodes correctly.
    auto opt = InitProducerIdResponseMsg::decode(buf, 0, n);
    ASSERT_TRUE(opt.has_value()) << "full-buffer decode must succeed";
    EXPECT_EQ(opt->producerId,    42LL);
    EXPECT_EQ(opt->producerEpoch, 7);
    EXPECT_EQ(opt->errorCode,     0);

    // (b) Truncated: every avail in [0, n-1] must return nullopt.
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto bad = InitProducerIdResponseMsg::decode(buf, 0, avail);
        EXPECT_FALSE(bad.has_value()) << "truncated at avail=" << avail;
    }
}

TEST(TransactionMessagesTest, L2AddPartitionsToTxnResponseDecodeBoundsChecked) {
    uint8_t buf[kBufSize] = {};
    auto n = AddPartitionsToTxnResponseMsg::encode(buf, 0, /*errorCode=*/3);
    ASSERT_GT(n, 0u);

    // (a) Full buffer decodes.
    auto opt = AddPartitionsToTxnResponseMsg::decode(buf, 0, n);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->errorCode, 3);

    // (b) Every truncation → nullopt.
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto bad = AddPartitionsToTxnResponseMsg::decode(buf, 0, avail);
        EXPECT_FALSE(bad.has_value()) << "truncated at avail=" << avail;
    }
}

TEST(TransactionMessagesTest, L2EndTxnResponseDecodeBoundsChecked) {
    uint8_t buf[kBufSize] = {};
    auto n = EndTxnResponseMsg::encode(buf, 0, /*errorCode=*/5);
    ASSERT_GT(n, 0u);

    // (a) Full buffer decodes.
    auto opt = EndTxnResponseMsg::decode(buf, 0, n);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->errorCode, 5);

    // (b) Every truncation → nullopt.
    for (std::size_t avail = 0; avail < n; ++avail) {
        auto bad = EndTxnResponseMsg::decode(buf, 0, avail);
        EXPECT_FALSE(bad.has_value()) << "truncated at avail=" << avail;
    }
}

// L2: existing callers use the 2-param (no-avail) decode; verify they still
// compile and return the expected values (behaviour-preservation).
TEST(TransactionMessagesTest, L2LegacyTwoParamDecodeUnchanged) {
    uint8_t buf[kBufSize] = {};
    {
        InitProducerIdResponseMsg::encode(buf, 0, 99LL, 3, 0);
        auto m = InitProducerIdResponseMsg::decode(buf, 0);
        EXPECT_EQ(m.producerId,    99LL);
        EXPECT_EQ(m.producerEpoch, 3);
        EXPECT_EQ(m.errorCode,     0);
    }
    {
        AddPartitionsToTxnResponseMsg::encode(buf, 0, 7);
        auto m = AddPartitionsToTxnResponseMsg::decode(buf, 0);
        EXPECT_EQ(m.errorCode, 7);
    }
    {
        EndTxnResponseMsg::encode(buf, 0, 2);
        auto m = EndTxnResponseMsg::decode(buf, 0);
        EXPECT_EQ(m.errorCode, 2);
    }
}
