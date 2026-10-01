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

TEST(RingSbe, SessionAnnounceCarriesEgressChannelAndStream) {
    std::array<char, 1024> buf{};
    const std::string ch = "aeron:udp?endpoint=10.0.0.9:40900";
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::SESSION_ANNOUNCE)
       .sessionId(77)
       .egressStreamId(9)
       .tailKind(sbe::TailKind::Value::EGRESS_CHANNEL);
    enc.putTail(ch);
    EXPECT_EQ(enc.encodedLength(), sbe::WorkerOp::computeLength(ch.size()));

    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::SESSION_ANNOUNCE);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::SESSION_ANNOUNCE), 13);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::SESSION_CLOSE), 14);
    EXPECT_EQ(dec.sessionId(), 77);
    EXPECT_EQ(dec.egressStreamId(), 9);
    EXPECT_EQ(dec.tailKind(), sbe::TailKind::Value::EGRESS_CHANNEL);
    const std::uint32_t len = dec.tailLength();
    const char* p = dec.tail();
    EXPECT_EQ(std::string(p, len), ch);
}

TEST(RingSbe, ProduceNotifyReturnTypeRoundTrips) {
    std::array<char, 512> buf{};
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.returnType(sbe::ReturnType::Value::PRODUCE_NOTIFY)
       .sessionId(0).correlationId(0).partition(3).baseOffset(1234).lastOffset(1240)
       .errorCode(0).deferAck(0).replica(0).producerId(55).producerEpoch(1)
       .baseSequence(10).recordCount(7).topicId(2).batchAttrs(0x10).enqueuedAtNs(0)
       .subscriptionId(0).readTopicId(0).readPartition(0).newNextOffset(0).hwBound(0)
       .isolation(0).isGroup(0).groupCarrierReceived(0);
    enc.putChunk(nullptr, 0);
    auto dec = decodeAt<sbe::WorkerReturn>(buf.data(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::PRODUCE_NOTIFY);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::PRODUCE_NOTIFY), 2);
    EXPECT_EQ(dec.producerId(), 55);
    EXPECT_EQ(dec.baseOffset(), 1234);
    EXPECT_EQ(dec.batchAttrs(), 0x10);
}

TEST(RingSbe, StorageFailureReturnRoundTripsRegression) {
    // Task 10 fix 1 (fail-stop): STORAGE_FAILURE must decode (an unknown value
    // would throw E103) and carry topicId / partition / errno / the log dir.
    std::array<char, 512> buf{};
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.returnType(sbe::ReturnType::Value::STORAGE_FAILURE)
       .sessionId(0).correlationId(0).partition(4).baseOffset(0).lastOffset(0)
       .errorCode(5).deferAck(0).replica(0).producerId(-1).producerEpoch(0)
       .baseSequence(0).recordCount(0).topicId(9).batchAttrs(0).enqueuedAtNs(0)
       .subscriptionId(0).readTopicId(0).readPartition(0).newNextOffset(0).hwBound(0)
       .isolation(0).isGroup(0).groupCarrierReceived(0);
    const char dir[] = "/data/t/4";
    enc.putChunk(dir, sizeof(dir) - 1);
    auto dec = decodeAt<sbe::WorkerReturn>(buf.data(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::STORAGE_FAILURE);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::STORAGE_FAILURE), 3);
    EXPECT_EQ(dec.topicId(), 9);
    EXPECT_EQ(dec.partition(), 4);
    EXPECT_EQ(dec.errorCode(), 5);
    EXPECT_EQ(dec.getChunkAsString(), "/data/t/4");
}

TEST(RingSbe, TestTaskOpCarriesOpaquePointersRegression) {
    // F1: test-only owner-thread hook.  TEST_TASK must round-trip through the
    // generated enum decoder (an unknown value would throw E103) and carry the
    // replica + task pointers in the replica / egressPub uint64 slots.
    std::array<char, 512> buf{};
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::TEST_TASK)
       .topicId(3)
       .partition(1)
       .replica(0x1122334455667788ULL)
       .egressPub(0x0102030405060708ULL)
       .tailKind(sbe::TailKind::Value::NONE);
    enc.putTail(nullptr, 0);
    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::TEST_TASK);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::TEST_TASK), 200);
    EXPECT_EQ(dec.topicId(), 3);
    EXPECT_EQ(dec.partition(), 1);
    EXPECT_EQ(dec.replica(), 0x1122334455667788ULL);
    EXPECT_EQ(dec.egressPub(), 0x0102030405060708ULL);
}

TEST(RingSbe, OwnershipGuardFieldsAndReturnsRoundTripRegression) {
    // Task 10 fix 3: replicaId / incarnation / ownershipGen ride every op; the
    // OWNERSHIP_ACK and CONTROL_RECORD_REFUSED returns decode (no E103).
    std::array<char, 512> buf{};
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::PRODUCE).topicId(2).partition(5)
       .targetReplicaId(77).incarnation(9).ownershipGen(123456789ULL)
       .tailKind(sbe::TailKind::Value::NONE);
    enc.putTail(nullptr, 0);
    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.targetReplicaId(), 77u);
    EXPECT_EQ(dec.incarnation(), 9u);
    EXPECT_EQ(dec.ownershipGen(), 123456789ULL);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::OWNERSHIP_ACK), 4);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::CONTROL_RECORD_REFUSED), 5);
    EXPECT_EQ(sbe::ReturnType::get(4), sbe::ReturnType::Value::OWNERSHIP_ACK);
    EXPECT_EQ(sbe::ReturnType::get(5), sbe::ReturnType::Value::CONTROL_RECORD_REFUSED);
}

} // namespace

TEST(RingSbe, ControlRecordAppendedCarriesIdentityAndOffsetRegression) {
    // Task 10 round 4: CONTROL_RECORD_APPENDED (the marker's authoritative
    // offset) and the replica identity (targetReplicaId / incarnation) on every
    // WorkerReturn; the new follower-side op types decode (unknown → E103).
    std::array<char, 512> buf{};
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.returnType(sbe::ReturnType::Value::CONTROL_RECORD_APPENDED)
       .sessionId(0).correlationId(42).partition(1).baseOffset(3).lastOffset(0)
       .errorCode(0).deferAck(0).replica(0).producerId(7).producerEpoch(0)
       .baseSequence(0).recordCount(0).topicId(2).batchAttrs(0).enqueuedAtNs(0)
       .subscriptionId(0).readTopicId(0).readPartition(0).newNextOffset(0).hwBound(0)
       .isolation(0).isGroup(0).groupCarrierReceived(0)
       .targetReplicaId(9).incarnation(0xFFFFFFFEu);
    enc.putChunk(nullptr, 0);
    auto dec = decodeAt<sbe::WorkerReturn>(buf.data(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::CONTROL_RECORD_APPENDED);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::CONTROL_RECORD_APPENDED), 6);
    EXPECT_EQ(dec.correlationId(), 42);
    EXPECT_EQ(dec.baseOffset(), 3);
    EXPECT_EQ(dec.targetReplicaId(), 9u);
    EXPECT_EQ(dec.incarnation(), 0xFFFFFFFEu);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::OFFSETS_FOR_LEADER_EPOCH_RESP), 15);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::RECONCILE_FETCH_REQ), 16);
    EXPECT_EQ(sbe::OpType::get(15), sbe::OpType::Value::OFFSETS_FOR_LEADER_EPOCH_RESP);
    EXPECT_EQ(sbe::OpType::get(16), sbe::OpType::Value::RECONCILE_FETCH_REQ);
}
