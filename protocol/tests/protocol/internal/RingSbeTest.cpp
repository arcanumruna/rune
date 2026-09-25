// RingSbeTest.cpp — round-trip tests for the SBE-generated internal ring messages
// (WorkerOp / WorkerReturn / FlushRequest / FlushCompletion).  Validates the
// generated flyweights: wrap-and-apply-header encode, wrap-for-decode, every
// scalar field, the varData tail, and computeLength() (used by ring tryClaim).
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>
#include <rune_protocol_internal_sbe/FlushRequest.h>
#include <rune_protocol_internal_sbe/FlushCompletion.h>
#include <rune_protocol_internal_sbe/OpType.h>
#include <rune_protocol_internal_sbe/ReturnType.h>
#include <rune_protocol_internal_sbe/TailKind.h>

#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <string>

namespace sbe = rune::protocol::internal::sbe;

namespace {

// Decode helper: read the SBE header, confirm templateId, position the decoder.
template <typename Msg>
Msg decodeAt(char* buf, std::size_t cap) {
    sbe::MessageHeader hdr;
    hdr.wrap(buf, 0, 0, cap);
    EXPECT_EQ(hdr.schemaId(), Msg::sbeSchemaId());
    EXPECT_EQ(hdr.templateId(), Msg::sbeTemplateId());
    Msg dec;
    dec.wrapForDecode(buf, sbe::MessageHeader::encodedLength(),
                      hdr.blockLength(), hdr.version(), cap);
    return dec;
}

TEST(RingSbe, WorkerOpRoundTripWithTail) {
    std::array<char, 4096> buf{};
    const std::string tail = "record-batch-bytes-xyz";
    const std::size_t total = sbe::WorkerOp::computeLength(tail.size());
    ASSERT_LE(total, buf.size());

    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::PRODUCE)
       .topicId(7)
       .partition(3)
       .replica(0xABCD0000ULL)
       .logPtr(0x1234ULL)
       .sessionId(42)
       .correlationId(99)
       .acks(1)
       .producerId(555)
       .producerEpoch(2)
       .baseSequence(10)
       .lastOffsetDelta(4)
       .recordCount(5)
       .batchAttrs(0x10)
       .deferAck(1)
       .durabilityTier(2)
       .enqueuedAtNs(123456789)
       .subscriptionId(0)
       .nextOffset(0)
       .hwBound(0)
       .isolation(0)
       .isGroup(0)
       .groupCarrierReceived(0)
       .roleEpoch(0)
       .roleLeaderId(-1)
       .replLeaderEpoch(0).replPartition(0).replHighWatermark(0)
       .replLogStartOffset(0).replErrorCode(0).replTopicId(0)
       .frLeaderEpoch(0).frFollowerId(-1).frFetchOffset(0).frMaxBytes(0).frNowNs(0)
       .oleCurrentEpoch(0).oleFollowerId(-1).oleRequestedEpoch(0).oleFollowerLeo(0)
       .tailKind(sbe::TailKind::Value::RECORD_BATCH);
    enc.putTail(tail);
    EXPECT_EQ(enc.encodedLength(), total) << "computeLength must match encodedLength";

    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::PRODUCE);
    EXPECT_EQ(dec.topicId(), 7);
    EXPECT_EQ(dec.partition(), 3);
    EXPECT_EQ(dec.replica(), 0xABCD0000ULL);
    EXPECT_EQ(dec.logPtr(), 0x1234ULL);
    EXPECT_EQ(dec.sessionId(), 42);
    EXPECT_EQ(dec.correlationId(), 99);
    EXPECT_EQ(dec.acks(), 1);
    EXPECT_EQ(dec.producerId(), 555);
    EXPECT_EQ(dec.recordCount(), 5);
    EXPECT_EQ(dec.batchAttrs(), 0x10);
    EXPECT_EQ(dec.deferAck(), 1);
    EXPECT_EQ(dec.durabilityTier(), 2);
    EXPECT_EQ(dec.enqueuedAtNs(), 123456789);
    EXPECT_EQ(dec.roleLeaderId(), -1);
    EXPECT_EQ(dec.tailKind(), sbe::TailKind::Value::RECORD_BATCH);
    ASSERT_EQ(dec.tailLength(), tail.size());
    EXPECT_EQ(0, std::memcmp(dec.tail(), tail.data(), tail.size()));
}

TEST(RingSbe, WorkerOpNoTail) {
    std::array<char, 512> buf{};
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::SUBSCRIBE)
       .topicId(10).partition(2).subscriptionId(100).nextOffset(5)
       .isolation(1).isGroup(1).groupCarrierReceived(0)
       .tailKind(sbe::TailKind::Value::NONE);
    enc.putTail(nullptr, 0);

    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::SUBSCRIBE);
    EXPECT_EQ(dec.subscriptionId(), 100);
    EXPECT_EQ(dec.nextOffset(), 5);
    EXPECT_EQ(dec.isolation(), 1);
    EXPECT_EQ(dec.isGroup(), 1);
    EXPECT_EQ(dec.tailLength(), 0u);
}

TEST(RingSbe, WorkerReturnRoundTripWithChunk) {
    std::array<char, 4096> buf{};
    const std::string chunk = "pushed-records-chunk";
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.returnType(sbe::ReturnType::Value::PUSH_READ_RESULT)
       .sessionId(1).correlationId(2).partition(3).baseOffset(4).lastOffset(8)
       .errorCode(0).deferAck(0).replica(0xDEADULL)
       .producerId(-1).producerEpoch(0).baseSequence(0).recordCount(5).topicId(9)
       .batchAttrs(0).enqueuedAtNs(0)
       .subscriptionId(77).readTopicId(9).readPartition(3).newNextOffset(9)
       .hwBound(9).isolation(0).isGroup(0).groupCarrierReceived(0);
    enc.putChunk(chunk);

    auto dec = decodeAt<sbe::WorkerReturn>(buf.data(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::PUSH_READ_RESULT);
    EXPECT_EQ(dec.subscriptionId(), 77);
    EXPECT_EQ(dec.readTopicId(), 9);
    EXPECT_EQ(dec.readPartition(), 3);
    EXPECT_EQ(dec.newNextOffset(), 9);
    ASSERT_EQ(dec.chunkLength(), chunk.size());
    EXPECT_EQ(0, std::memcmp(dec.chunk(), chunk.data(), chunk.size()));
}

TEST(RingSbe, FlushRequestAndCompletionRoundTrip) {
    {
        std::array<char, 128> buf{};
        sbe::FlushRequest enc;
        enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
        enc.fd(42).flushBoundary(1000).token(7);
        auto dec = decodeAt<sbe::FlushRequest>(buf.data(), buf.size());
        EXPECT_EQ(dec.fd(), 42);
        EXPECT_EQ(dec.flushBoundary(), 1000);
        EXPECT_EQ(dec.token(), 7);
    }
    {
        std::array<char, 128> buf{};
        sbe::FlushCompletion enc;
        enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
        enc.token(7).flushBoundary(1000).errorCode(0);
        auto dec = decodeAt<sbe::FlushCompletion>(buf.data(), buf.size());
        EXPECT_EQ(dec.token(), 7);
        EXPECT_EQ(dec.flushBoundary(), 1000);
        EXPECT_EQ(dec.errorCode(), 0);
    }
}

} // namespace
