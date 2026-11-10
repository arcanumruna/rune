// WorkerOpWireRegressionTest.cpp — Phase 2c Task 5: the internal-ring wire values are append-only.
// OpType::FILTER_SCHEMA = 25 (control -> every worker: install a typed topic's filter schema) and
// TailKind::FILTER = 11 (the schema bytes / a Subscribe op's filter bytes) must never move.
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/OpType.h>
#include <rune_protocol_internal_sbe/ReturnType.h>
#include <rune_protocol_internal_sbe/TailKind.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>

#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReturnCodec.h>

#include <gtest/gtest.h>

#include <array>

namespace sbe = rune::protocol::internal::sbe;

TEST(WorkerOpWireRegression, FilterOpAndTailValuesAreAppendedRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::FILTER_SCHEMA), 25);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::FILTER), 11);
    // The values before them are unchanged (append-only).
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::PEER_LINK_KEYS), 22);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::READ_LEASE), 24);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::PEER_KEYS), 9);
}

TEST(WorkerOpWireRegression, ClusterFeatureLevelRoundTripsRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::CLUSTER_FEATURE_LEVEL), 31);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::ClusterFeatureLevel), 31);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::NS_VIEW_MEM_OVER), 30);
}

TEST(WorkerOpWireRegression, WriteSpecUpdateRoundTripsRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::WRITE_SPEC_UPDATE), 32);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::WRITE_SPEC), 12);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::WriteSpecUpdate), 32);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::FILTER), 11);
}

// Lease topics Task 11a: the keepalive / TimeToLive / scan ops, their tail kind and the three results are append-only values,
// numbered after checked writes Part 11 (TxnVote = 36, TxnVoteResult = 11).
TEST(WorkerOpWireRegression, LeaseOpsRoundTripRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::LEASE_KEEP_ALIVE), 37);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::LEASE_TIME_TO_LIVE), 38);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::SCAN_LEASES), 39);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::NS_LEASES_OVER), 40);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::TRANSIENT_LINEAGE), 41);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::LEASE_REQUEST), 14);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::LeaseKeepAlive), 37);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::LeaseTimeToLive), 38);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::ScanLeases), 39);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::NsLeasesOver), 40);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::TransientLineage), 41);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerReturn::Type::LeaseKeepAliveResult), 12);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerReturn::Type::LeaseTimeToLiveResult), 13);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerReturn::Type::ScanLeasesResult), 14);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::TXN_VOTE), 36) << "the value before them is unchanged";
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::TXN_VOTE_RESULT), 11) << "the value before them is unchanged";
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::GET_STATS), 35);
}

TEST(CheckedWritesTxnRing, TxnVoteOpRoundTrips) {
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::TxnVote), static_cast<int>(sbe::OpType::Value::TXN_VOTE));
    std::array<char, 512> buf{};
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(static_cast<sbe::OpType::Value>(static_cast<std::uint8_t>(rune::node::WorkerOp::Type::TxnVote)))
       .topicId(9).partition(1).correlationId(41).producerId(77).producerEpoch(3).roleEpoch(6).sessionId(2)
       .tailKind(sbe::TailKind::Value::NONE);
    enc.putTail(nullptr, 0);

    sbe::MessageHeader hdr;
    hdr.wrap(buf.data(), 0, 0, buf.size());
    ASSERT_EQ(hdr.templateId(), sbe::WorkerOp::sbeTemplateId());
    sbe::WorkerOp dec;
    dec.wrapForDecode(buf.data(), sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::TXN_VOTE);
    EXPECT_EQ(dec.topicId(), 9);
    EXPECT_EQ(dec.partition(), 1);
    EXPECT_EQ(dec.correlationId(), 41);
    EXPECT_EQ(dec.producerId(), 77);
    EXPECT_EQ(dec.producerEpoch(), 3);
    EXPECT_EQ(dec.roleEpoch(), 6);
    EXPECT_EQ(dec.sessionId(), 2);
    EXPECT_EQ(dec.tailLength(), 0u);
}

TEST(CheckedWritesTxnRing, TxnVoteResultMapsTheScalars) {
    rune::node::WorkerReturn ret;
    ret.type          = rune::node::WorkerReturn::Type::TxnVoteResult;
    ret.errorCode     = 0;
    ret.recordCount   = 55;
    ret.newNextOffset = 0x00070000;
    ret.baseOffset    = 123;
    ret.lastOffset    = 99;
    std::array<char, 512> buf{};
    ASSERT_LE(rune::node::workerReturnEncodedLength(ret), buf.size());
    rune::node::encodeWorkerReturnSbe(buf.data(), buf.size(), ret);

    sbe::MessageHeader hdr;
    hdr.wrap(buf.data(), 0, 0, buf.size());
    ASSERT_EQ(hdr.templateId(), sbe::WorkerReturn::sbeTemplateId());
    sbe::WorkerReturn dec;
    dec.wrapForDecode(buf.data(), sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::TXN_VOTE_RESULT);
    EXPECT_EQ(dec.errorCode(), 0);
    EXPECT_EQ(dec.recordCount(), 55);
    EXPECT_EQ(dec.newNextOffset(), 0x00070000);
    EXPECT_EQ(dec.baseOffset(), 123);
    EXPECT_EQ(dec.lastOffset(), 99);
}
