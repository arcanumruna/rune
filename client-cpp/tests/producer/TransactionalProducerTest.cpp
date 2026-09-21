// tests/producer/TransactionalProducerTest.cpp
//
// TX-5 — Tier-3 TransactionalProducer unit tests.
//
// Test strategy: in-process mock gateway over Aeron IPC (same pattern as
// ProducerTest.cpp).  The gateway decodes wire frames and feeds responses.
// Tests drive doWork() directly (no background runner).
//
// Test matrix:
//   1. InitTransactionsAcquiresPidEpoch
//        initTransactions() emits InitProducerId → response → pid/epoch stored.
//   2. IdempotentRetryReusesBaseSequence (IMPORTANT 3 fix)
//        Two sends to the same partition: assert actual baseSequence values on
//        the wire (0 and 1).  Feed a NotLeader ProduceResponse to trigger a
//        retry; verify the retry carries the ORIGINAL baseSequence=0.
//   3. BeginSendNewPartitionEmitsAddPartitions
//        beginTransaction() + send to a new partition → AddPartitionsToTxn
//        emitted exactly once for that partition.
//   4. SendOffsetsToTransactionEmitsAddOffsetsThenTxnOffsetCommit
//        sendOffsetsToTransaction() emits AddOffsetsToTxn then TxnOffsetCommit.
//   5. CommitTransactionEmitsEndTxnCommit
//        commitTransaction() emits EndTxn(commit=1).
//   6. AbortTransactionEmitsEndTxnAbort
//        abortTransaction() emits EndTxn(commit=0).
//   7. ProducerFencedSurfacesError
//        InitProducerIdResponse{ProducerFenced} → future throws;
//        producerId remains -1.
//   8. GroupConsumerReadCommittedConfig
//        GroupConfig{isolation=ReadCommitted} stores isolation=ReadCommitted
//        and the impl's cfg carries isolation=1.
//   9. GroupConsumerReadCommittedHub — hub preserves isolation config.
//  10. GroupConsumerReadCommittedWireIsolation (IMPORTANT 4 fix)
//        After SyncGroupResponse, a ReadCommitted group consumer emits
//        Subscribe frames with isolation=1 on the wire.

#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/producer/TransactionalProducerImpl.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
using namespace rune::client::producer;

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static constexpr int64_t kSessionId = 7777;

/// Peek the templateId from an encoded frame (bytes [2..3] LE).
static uint16_t peekTemplate(const std::vector<uint8_t>& buf) {
    if (buf.size() < 4) return 0;
    return static_cast<uint16_t>(buf[2]) | (static_cast<uint16_t>(buf[3]) << 8);
}

/// Build an InitProducerIdResponse frame.
static std::vector<uint8_t> makeInitProducerIdResponse(
    int64_t corrId, int64_t pid, int16_t epoch, int16_t errorCode = 0)
{
    rpc::InitProducerIdResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.producerId    = pid;
    msg.producerEpoch = epoch;
    msg.errorCode     = errorCode;

    std::vector<uint8_t> buf(rpc::InitProducerIdResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

/// Build an AddPartitionsToTxnResponse frame (all-OK).
static std::vector<uint8_t> makeAddPartitionsToTxnResponse(int64_t corrId)
{
    rpc::AddPartitionsToTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    // empty results = OK

    const std::size_t sz = rpc::AddPartitionsToTxnResponse::encodedSizeFor(0);
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), {});
    return buf;
}

/// Build an AddOffsetsToTxnResponse frame.
static std::vector<uint8_t> makeAddOffsetsToTxnResponse(int64_t corrId,
                                                          int16_t errorCode = 0)
{
    rpc::AddOffsetsToTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.errorCode     = errorCode;

    std::vector<uint8_t> buf(rpc::AddOffsetsToTxnResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

/// Build a TxnOffsetCommitResponse frame (empty results = OK).
static std::vector<uint8_t> makeTxnOffsetCommitResponse(int64_t corrId)
{
    rpc::TxnOffsetCommitResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;

    const std::size_t sz = rpc::TxnOffsetCommitResponse::encodedSizeFor(0);
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), {});
    return buf;
}

/// Build an EndTxnResponse frame.
static std::vector<uint8_t> makeEndTxnResponse(int64_t corrId, int16_t errorCode = 0)
{
    rpc::EndTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.errorCode     = errorCode;

    std::vector<uint8_t> buf(rpc::EndTxnResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

/// Build a ProduceResponse frame.
static std::vector<uint8_t> makeProduceResponse(int64_t corrId,
                                                  int16_t errorCode = 0,
                                                  int64_t baseOffset = 0)
{
    rpc::ProduceResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.errorCode     = errorCode;
    msg.partition     = 0;
    msg.baseOffset    = baseOffset;

    std::vector<uint8_t> buf(rpc::ProduceResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

// ---------------------------------------------------------------------------
// CaptureSink — captures frames sent by the impl.
// ---------------------------------------------------------------------------
struct CaptureSink {
    std::vector<std::vector<uint8_t>> frames;
    bool returnOk = true;

    std::function<bool(const uint8_t*, std::size_t)> fn() {
        return [this](const uint8_t* buf, std::size_t len) -> bool {
            if (returnOk) frames.emplace_back(buf, buf + len);
            return returnOk;
        };
    }

    void clear() { frames.clear(); }

    uint16_t templateId(std::size_t i) const {
        if (i >= frames.size()) return 0;
        return peekTemplate(frames[i]);
    }
};

// ---------------------------------------------------------------------------
// TransactionalProducerTest fixture
// ---------------------------------------------------------------------------
class TransactionalProducerTest : public ::testing::Test {
protected:
    std::atomic<int64_t> nextCorrId{1};

    std::shared_ptr<TransactionalProducerImpl> makeImpl(
        const std::string& txnId = "test-txn-id",
        ProducerConfig cfg = {})
    {
        cfg.acks = Acks::All;
        return std::make_shared<TransactionalProducerImpl>(
            txnId, cfg, nextCorrId, /*txnTimeoutMs=*/60'000);
    }
};

// ===========================================================================
// Test 1: InitTransactionsAcquiresPidEpoch
//   initTransactions() emits InitProducerId; response stores pid/epoch.
// ===========================================================================
TEST_F(TransactionalProducerTest, InitTransactionsAcquiresPidEpoch)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // Before init: producerId is -1.
    EXPECT_EQ(tp.producerId(), TransactionalProducerImpl::kNoPid);

    // App thread: enqueue initTransactions.
    auto fut = tp.initTransactions();

    // Agent: doWork sends InitProducerId.
    int work = impl->doWork(kSessionId, sink.fn());
    ASSERT_GE(work, 1) << "doWork must send at least one frame";
    ASSERT_GE(sink.frames.size(), 1u);
    EXPECT_EQ(sink.templateId(0),
              static_cast<uint16_t>(rpc::TemplateId::InitProducerId))
        << "First frame must be InitProducerId (templateId 45)";

    // Decode the InitProducerId to get correlationId.
    auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
    ASSERT_TRUE(opt.has_value()) << "InitProducerId must decode successfully";
    const int64_t corrId = opt->correlationId;
    EXPECT_GT(corrId, 0);

    // Also verify transactionalId is encoded correctly.
    const std::string txnIdStr(
        reinterpret_cast<const char*>(opt->transactionalId.data()),
        opt->transactionalId.size());
    EXPECT_EQ(txnIdStr, "test-txn-id");

    // Simulate broker response: InitProducerIdResponse{pid=42, epoch=1}.
    auto resp = makeInitProducerIdResponse(corrId, /*pid=*/42, /*epoch=*/1);
    impl->onInitProducerIdResponse(resp.data(), resp.size());

    // Future must resolve.
    ASSERT_EQ(fut.wait_for(0s), std::future_status::ready)
        << "initTransactions() future must resolve after response";
    EXPECT_NO_THROW(fut.get());

    // Producer identity stored.
    EXPECT_EQ(tp.producerId(), 42);
    EXPECT_EQ(tp.producerEpoch(), 1);
}

// ---------------------------------------------------------------------------
// Decode baseSequence from a Produce wire frame's embedded RecordBatch.
// RecordBatch layout: baseSequence is at kBaseSequenceOffset=53 within the batch.
// ---------------------------------------------------------------------------
static int32_t decodeBaseSeqFromProduce(const std::vector<uint8_t>& frame)
{
    using namespace rune::protocol::batch;
    auto popt = rune::client::codec::decodeProduce(frame.data(), frame.size());
    if (!popt) return -999;
    const auto& rb = popt->recordBatch;
    if (rb.size() < static_cast<std::size_t>(kBaseSequenceOffset + 4)) return -999;
    const uint8_t* d = rb.data();
    return static_cast<int32_t>(
        static_cast<uint32_t>(d[kBaseSequenceOffset])       |
       (static_cast<uint32_t>(d[kBaseSequenceOffset + 1]) << 8) |
       (static_cast<uint32_t>(d[kBaseSequenceOffset + 2]) << 16) |
       (static_cast<uint32_t>(d[kBaseSequenceOffset + 3]) << 24));
}

// ---------------------------------------------------------------------------
// Collect correlationIds from all Produce frames in a sink.
// ---------------------------------------------------------------------------
static std::vector<int64_t> collectProduceCorrIds(const CaptureSink& sink)
{
    std::vector<int64_t> ids;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::Produce)) continue;
        auto opt = rpc::Produce::decode(f.data(), f.size());
        if (opt) ids.push_back(opt->correlationId);
    }
    return ids;
}

// ===========================================================================
// Test 2: IdempotentRetryReusesBaseSequence
//
//   Part A — Wire sequence values:
//     Two successive sends to the same (topic,partition).  Each gets a distinct
//     baseSequence (0 and 1).  Verify by decoding the embedded RecordBatch.
//
//   Part B — Genuine retry:
//     Feed a ProduceResponse with errorCode=NotLeader for the first send.
//     The impl must re-send the SAME wire frame with the SAME baseSequence (0).
//     A new fresh batch would use baseSequence=2 — any sequence != 0 is a bug.
// ===========================================================================
TEST_F(TransactionalProducerTest, IdempotentRetryReusesBaseSequence)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // ---- Acquire producer identity -------------------------------------------
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        ASSERT_GE(sink.frames.size(), 1u);
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, 99, 0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        ASSERT_EQ(fut.wait_for(0s), std::future_status::ready);
        sink.clear();
    }

    tp.beginTransaction();

    TopicHandle topic{1, 1};
    Key key(uint64_t{0});
    const std::vector<uint8_t> val = {0xAA, 0xBB};

    // ---- Part A: two sends produce baseSeq 0 and 1 on the wire --------------

    tp.send(topic, key, std::span<const uint8_t>(val)); // first  → baseSeq 0
    tp.send(topic, key, std::span<const uint8_t>(val)); // second → baseSeq 1
    impl->doWork(kSessionId, sink.fn());

    // Filter produce frames.
    std::vector<int32_t> seqsSeen;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) {
            seqsSeen.push_back(decodeBaseSeqFromProduce(f));
        }
    }
    ASSERT_EQ(seqsSeen.size(), 2u)
        << "Expected 2 Produce frames (one per send to the same partition)";
    EXPECT_EQ(seqsSeen[0], 0) << "First send must have baseSequence=0";
    EXPECT_EQ(seqsSeen[1], 1) << "Second send must have baseSequence=1";

    // Grab correlationIds for the two sends.
    auto corrIds = collectProduceCorrIds(sink);
    ASSERT_EQ(corrIds.size(), 2u);
    const int64_t firstCorrId  = corrIds[0];
    const int64_t secondCorrId = corrIds[1];
    EXPECT_NE(firstCorrId, 0) << "Produce must use a real correlationId (not 0)";
    EXPECT_NE(secondCorrId, 0);
    EXPECT_NE(firstCorrId, secondCorrId) << "Each send must get a unique correlationId";

    // Acknowledge second send (success) — nothing interesting about it.
    {
        auto ackResp = makeProduceResponse(secondCorrId, 0, /*baseOffset=*/1);
        impl->onProduceResponse(ackResp.data(), ackResp.size(), sink.fn());
    }

    // ---- Part B: retriable error on first send → retry must reuse baseSeq=0 --

    sink.clear();

    // Simulate NotLeader (errorCode=2) for the first send.
    {
        const int16_t kNotLeader = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
        auto errResp = makeProduceResponse(firstCorrId, kNotLeader);
        impl->onProduceResponse(errResp.data(), errResp.size(), sink.fn());
    }

    // The impl must have re-sent the same wire frame (same corrId, same baseSeq=0).
    ASSERT_GE(sink.frames.size(), 1u)
        << "A retry Produce must be emitted on retriable error";
    const int32_t retryBaseSeq = decodeBaseSeqFromProduce(sink.frames[0]);
    EXPECT_EQ(retryBaseSeq, 0)
        << "Retry MUST reuse the ORIGINAL baseSequence=0 (not a fresh 2). "
           "A mismatch here means Critical-1 is broken: the retry allocated a "
           "new sequence, defeating broker dedup.";

    // The retried frame must use the SAME correlationId as the original.
    auto retryOpt = rpc::Produce::decode(sink.frames[0].data(), sink.frames[0].size());
    ASSERT_TRUE(retryOpt.has_value());
    EXPECT_EQ(retryOpt->correlationId, firstCorrId)
        << "Retry must carry the ORIGINAL correlationId so the response can be matched";

    // producerId must still be valid (no fencing on retriable error).
    EXPECT_EQ(tp.producerId(), 99LL)
        << "producerId must remain valid after a retriable NotLeader error";
}

// ===========================================================================
// Test 3: BeginSendNewPartitionEmitsAddPartitions
//   beginTransaction() + send to a new (topic,partition) → AddPartitionsToTxn
//   emitted exactly once for that partition.
// ===========================================================================
TEST_F(TransactionalProducerTest, BeginSendNewPartitionEmitsAddPartitions)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // Acquire pid.
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, 55, 2);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        fut.get();
        sink.clear();
    }

    tp.beginTransaction();
    EXPECT_TRUE(tp.transactionActive());

    // First send to (topicId=10, partition=0) → must trigger AddPartitionsToTxn.
    TopicHandle topic{10, 1};
    Key key(uint64_t{0});
    const std::vector<uint8_t> val = {0x01};

    tp.send(topic, key, std::span<const uint8_t>(val));

    // Agent processes: should emit AddPartitionsToTxn + Produce.
    impl->doWork(kSessionId, sink.fn());

    // Find AddPartitionsToTxn frame.
    bool foundAddParts = false;
    int64_t addPartsCorr = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxn)) {
            foundAddParts = true;
            auto opt = rpc::AddPartitionsToTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            addPartsCorr = opt->correlationId;
            // Verify it carries the correct pid/epoch/txnId.
            EXPECT_EQ(opt->producerId, 55);
            EXPECT_EQ(opt->producerEpoch, 2);
            const std::string txnStr(
                reinterpret_cast<const char*>(opt->transactionalId.data()),
                opt->transactionalId.size());
            EXPECT_EQ(txnStr, "test-txn-id");
            // Verify partition is in the list.
            EXPECT_GE(opt->partitions.size(), 1u);
            break;
        }
    }
    EXPECT_TRUE(foundAddParts)
        << "AddPartitionsToTxn must be emitted on first send to a new partition";

    // Simulate response.
    if (foundAddParts && addPartsCorr != 0) {
        auto resp = makeAddPartitionsToTxnResponse(addPartsCorr);
        impl->onAddPartitionsToTxnResponse(resp.data(), resp.size());
    }

    sink.clear();

    // Second send to the same partition → NO AddPartitionsToTxn (already added).
    tp.send(topic, key, std::span<const uint8_t>(val));
    impl->doWork(kSessionId, sink.fn());

    for (const auto& f : sink.frames) {
        EXPECT_NE(peekTemplate(f),
                  static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxn))
            << "AddPartitionsToTxn must NOT be emitted again for the same partition";
    }
}

// ===========================================================================
// Test 4: SendOffsetsToTransactionEmitsAddOffsetsThenTxnOffsetCommit
//   sendOffsetsToTransaction() emits AddOffsetsToTxn then TxnOffsetCommit.
// ===========================================================================
TEST_F(TransactionalProducerTest, SendOffsetsToTransactionEmitsAddOffsetsThenTxnOffsetCommit)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // Acquire pid.
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, 77, 0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        fut.get();
        sink.clear();
    }

    tp.beginTransaction();

    // Submit sendOffsetsToTransaction.
    std::map<TopicPartitionKey, int64_t> offsets;
    offsets[{42, 0}] = 100;
    offsets[{42, 1}] = 200;

    auto fut = tp.sendOffsetsToTransaction(offsets, "my-consumer-group");

    // doWork → should emit AddOffsetsToTxn.
    impl->doWork(kSessionId, sink.fn());

    bool foundAddOffsets = false;
    int64_t addOffsetCorr = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::AddOffsetsToTxn)) {
            foundAddOffsets = true;
            auto opt = rpc::AddOffsetsToTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            addOffsetCorr = opt->correlationId;
            EXPECT_EQ(opt->producerId, 77);
            const std::string grpStr(
                reinterpret_cast<const char*>(opt->groupId.data()),
                opt->groupId.size());
            EXPECT_EQ(grpStr, "my-consumer-group");
            break;
        }
    }
    EXPECT_TRUE(foundAddOffsets) << "AddOffsetsToTxn must be emitted";

    // Simulate AddOffsetsToTxnResponse → triggers TxnOffsetCommit.
    if (addOffsetCorr != 0) {
        auto resp = makeAddOffsetsToTxnResponse(addOffsetCorr);
        sink.clear();
        impl->onAddOffsetsToTxnResponse(resp.data(), resp.size(), kSessionId, sink.fn());
    }

    // Now TxnOffsetCommit should have been sent.
    bool foundTxnOC = false;
    int64_t txnOCCorr = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::TxnOffsetCommit)) {
            foundTxnOC = true;
            auto opt = rpc::TxnOffsetCommit::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            txnOCCorr = opt->correlationId;
            EXPECT_EQ(opt->producerId, 77);
            EXPECT_EQ(opt->entries.size(), 2u);
            break;
        }
    }
    EXPECT_TRUE(foundTxnOC) << "TxnOffsetCommit must be emitted after AddOffsetsToTxnResponse";

    // Simulate TxnOffsetCommitResponse → future resolved.
    if (txnOCCorr != 0) {
        auto resp = makeTxnOffsetCommitResponse(txnOCCorr);
        impl->onTxnOffsetCommitResponse(resp.data(), resp.size());
    }

    ASSERT_EQ(fut.wait_for(0s), std::future_status::ready)
        << "sendOffsetsToTransaction future must resolve";
    EXPECT_NO_THROW(fut.get());
}

// ===========================================================================
// Test 5: CommitTransactionEmitsEndTxnCommit
//   commitTransaction() emits EndTxn(commit=1).
// ===========================================================================
TEST_F(TransactionalProducerTest, CommitTransactionEmitsEndTxnCommit)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // Acquire pid.
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, 88, 1);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        fut.get();
        sink.clear();
    }

    tp.beginTransaction();

    // Commit the transaction.
    auto commitFut = tp.commitTransaction();

    // doWork → EndTxn(commit=1).
    impl->doWork(kSessionId, sink.fn());

    bool foundEndTxn = false;
    int64_t endTxnCorr = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
            foundEndTxn = true;
            auto opt = rpc::EndTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            endTxnCorr = opt->correlationId;
            EXPECT_EQ(opt->producerId, 88);
            EXPECT_EQ(opt->producerEpoch, 1);
            EXPECT_EQ(opt->commit, uint8_t{1}) << "commit=1 means COMMIT";
            const std::string txnStr(
                reinterpret_cast<const char*>(opt->transactionalId.data()),
                opt->transactionalId.size());
            EXPECT_EQ(txnStr, "test-txn-id");
            break;
        }
    }
    EXPECT_TRUE(foundEndTxn) << "EndTxn(commit=1) must be emitted";

    // Simulate EndTxnResponse.
    if (endTxnCorr != 0) {
        auto resp = makeEndTxnResponse(endTxnCorr, 0);
        impl->onEndTxnResponse(resp.data(), resp.size());
    }

    ASSERT_EQ(commitFut.wait_for(0s), std::future_status::ready)
        << "commitTransaction future must resolve";
    EXPECT_NO_THROW(commitFut.get());

    // After commit, txnActive must be false.
    EXPECT_FALSE(tp.transactionActive());
}

// ===========================================================================
// Test 6: AbortTransactionEmitsEndTxnAbort
//   abortTransaction() emits EndTxn(commit=0).
// ===========================================================================
TEST_F(TransactionalProducerTest, AbortTransactionEmitsEndTxnAbort)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // Acquire pid.
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, 66, 0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        fut.get();
        sink.clear();
    }

    tp.beginTransaction();

    // Abort.
    auto abortFut = tp.abortTransaction();
    impl->doWork(kSessionId, sink.fn());

    bool foundEndTxn = false;
    int64_t endTxnCorr = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
            foundEndTxn = true;
            auto opt = rpc::EndTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            endTxnCorr = opt->correlationId;
            EXPECT_EQ(opt->commit, uint8_t{0}) << "commit=0 means ABORT";
            break;
        }
    }
    EXPECT_TRUE(foundEndTxn) << "EndTxn(commit=0) must be emitted for abort";

    if (endTxnCorr != 0) {
        auto resp = makeEndTxnResponse(endTxnCorr, 0);
        impl->onEndTxnResponse(resp.data(), resp.size());
    }

    ASSERT_EQ(abortFut.wait_for(0s), std::future_status::ready)
        << "abortTransaction future must resolve";
    EXPECT_NO_THROW(abortFut.get());
    EXPECT_FALSE(tp.transactionActive());
}

// ===========================================================================
// Test 7: ProducerFencedSurfacesError
//   InitProducerIdResponse{ProducerFenced} → future throws; producerId stays -1.
// ===========================================================================
TEST_F(TransactionalProducerTest, ProducerFencedSurfacesError)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    auto fut = tp.initTransactions();
    impl->doWork(kSessionId, sink.fn());

    ASSERT_GE(sink.frames.size(), 1u);
    auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
    ASSERT_TRUE(opt.has_value());

    // Respond with ProducerFenced (errorCode 18).
    const int16_t kProducerFenced = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);
    auto resp = makeInitProducerIdResponse(opt->correlationId, -1, 0, kProducerFenced);
    impl->onInitProducerIdResponse(resp.data(), resp.size());

    ASSERT_EQ(fut.wait_for(0s), std::future_status::ready)
        << "Future must be resolved (as failure) on ProducerFenced";

    EXPECT_THROW({ fut.get(); }, std::runtime_error)
        << "ProducerFenced must surface as std::runtime_error";

    // producerId must remain -1 (not acquired).
    EXPECT_EQ(tp.producerId(), TransactionalProducerImpl::kNoPid)
        << "producerId must remain -1 after ProducerFenced";
}

// ===========================================================================
// Test 8: GroupConsumerReadCommittedConfig
//   GroupConfig{isolation=ReadCommitted} stores isolation correctly.
//   The isolation value must be accessible from the impl and equal ReadCommitted.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, IsolationLevelStoredCorrectly)
{
    using namespace rune::client;
    using namespace rune::client::group;

    GroupConfig cfg;
    cfg.autoCommit           = false;
    cfg.sessionTimeoutMs     = 10000;
    cfg.isolation            = IsolationLevel::ReadCommitted;

    // Build a GroupConsumerImpl with this config.
    auto impl = std::make_shared<GroupConsumerImpl>();
    impl->groupId  = "test-group";
    impl->topicIds = {42};
    impl->cfg      = cfg;

    // Verify isolation is stored as ReadCommitted (=1).
    EXPECT_EQ(impl->cfg.isolation, IsolationLevel::ReadCommitted)
        << "GroupConfig must store IsolationLevel::ReadCommitted";
    EXPECT_EQ(static_cast<uint8_t>(impl->cfg.isolation), uint8_t{1})
        << "IsolationLevel::ReadCommitted wire value must be 1";

    // Verify ReadUncommitted default.
    GroupConfig defaultCfg;
    EXPECT_EQ(defaultCfg.isolation, IsolationLevel::ReadUncommitted)
        << "Default isolation must be ReadUncommitted";
    EXPECT_EQ(static_cast<uint8_t>(defaultCfg.isolation), uint8_t{0})
        << "IsolationLevel::ReadUncommitted wire value must be 0";
}

// ===========================================================================
// Test 9: GroupConsumerReadCommittedHub
//   Register a GroupConsumer with isolation=ReadCommitted; verify the impl's
//   cfg is stored with the correct isolation level via the hub.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, HubPreservesIsolationConfig)
{
    using namespace rune::client;
    using namespace rune::client::group;

    GroupConsumerHub hub;

    GroupConfig cfg;
    cfg.isolation = IsolationLevel::ReadCommitted;
    cfg.autoCommit = false;

    auto impl = std::make_shared<GroupConsumerImpl>();
    impl->groupId  = "rc-group";
    impl->topicIds = {10};
    impl->cfg      = cfg;
    impl->active.store(true, std::memory_order_relaxed);

    hub.add(impl);

    // Verify that the hub registered the impl with the correct isolation.
    // We access the impl directly (hub does not expose iteration).
    EXPECT_EQ(impl->cfg.isolation, IsolationLevel::ReadCommitted)
        << "Hub must preserve isolation config in registered impl";
}

// ===========================================================================
// Test 10: GroupConsumerReadCommittedWireIsolation  (IMPORTANT 4 fix)
//
//   A GroupConsumerHub with a ReadCommitted consumer, after receiving a
//   SyncGroupResponse that assigns a partition, must emit a Subscribe frame
//   on the wire with isolation=1 (ReadCommitted).
//
//   This test drives GroupConsumerHub directly via its public entry points
//   (no background thread) and asserts the Subscribe templateId and isolation
//   byte are correct in the captured output.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, WireIsolationOnSyncGroupResponse)
{
    using namespace rune::client;
    using namespace rune::client::group;

    namespace rpc = rune::protocol::client;

    // Build a GroupConsumerHub with a ReadCommitted consumer.
    GroupConsumerHub hub;

    GroupConfig cfg;
    cfg.isolation   = IsolationLevel::ReadCommitted;
    cfg.autoCommit  = false;
    cfg.sessionTimeoutMs = 10000;

    auto impl = std::make_shared<GroupConsumerImpl>();
    impl->groupId   = "rc-wire-group";
    impl->topicIds  = {42};
    impl->cfg       = cfg;
    impl->memberId  = "member-1";
    impl->generationId = 1;
    impl->state     = GroupConsumerState::Syncing; // ready to receive SyncGroupResponse
    impl->active.store(true, std::memory_order_relaxed);
    impl->joinDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

    hub.add(impl);

    // Capture all frames sent by the hub.
    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](const uint8_t* buf, std::size_t len) -> bool {
        capturedFrames.emplace_back(buf, buf + len);
        return true;
    };

    // Build a SyncGroupResponse assigning partition (topicId=42, partition=0).
    rpc::SyncGroupResponse syncResp;
    syncResp.sessionId     = kSessionId;
    syncResp.correlationId = 0;
    syncResp.errorCode     = 0; // OK

    rpc::TopicPartition32 asgn;
    asgn.topicId   = 42;
    asgn.partition = 0;
    const std::span<const rpc::TopicPartition32> asgnSpan{&asgn, 1};

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(1);
    std::vector<uint8_t> syncBuf(sz);
    const std::size_t written = syncResp.encode(syncBuf.data(), asgnSpan);
    ASSERT_GT(written, 0u);

    // Feed the SyncGroupResponse into the hub.
    hub.onSyncGroupResponse(syncBuf.data(), written, kSessionId, sendFn);

    // The hub must now be Stable and must have sent a Subscribe frame.
    // Filter for Subscribe frames.
    bool foundSubscribe = false;
    for (const auto& f : capturedFrames) {
        const uint16_t tid = peekTemplate(f);
        if (tid != static_cast<uint16_t>(rpc::TemplateId::Subscribe)) continue;

        // Decode and check isolation.
        auto subOpt = rune::client::codec::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(subOpt.has_value()) << "Subscribe frame must be decodable";
        EXPECT_EQ(subOpt->topicId, 42) << "Subscribe must target topicId=42";
        EXPECT_EQ(subOpt->partition, 0) << "Subscribe must target partition=0";
        EXPECT_EQ(subOpt->partitionKnown, uint8_t{1}) << "partitionKnown must be 1";
        EXPECT_EQ(subOpt->isolation, uint8_t{1})
            << "isolation MUST be 1 (ReadCommitted) on the wire. "
               "A value of 0 here means Critical-2 is broken: "
               "the broker sees ReadUncommitted and delivers open-txn records.";
        foundSubscribe = true;
        break;
    }
    EXPECT_TRUE(foundSubscribe)
        << "GroupConsumer must emit a Subscribe frame after SyncGroupResponse "
           "so the broker applies the correct isolation level";

    // Sanity: consumer is now Stable.
    EXPECT_EQ(impl->state, GroupConsumerState::Stable);
}

// ===========================================================================
// Test 11: GroupConsumerReadUncommittedWireIsolation
//
//   A GroupConsumerHub with a ReadUncommitted (default) consumer emits
//   Subscribe frames with isolation=0.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, WireIsolationReadUncommittedIsZero)
{
    using namespace rune::client;
    using namespace rune::client::group;

    namespace rpc = rune::protocol::client;

    GroupConsumerHub hub;

    GroupConfig cfg;  // default: ReadUncommitted
    cfg.autoCommit       = false;
    cfg.sessionTimeoutMs = 10000;

    auto impl = std::make_shared<GroupConsumerImpl>();
    impl->groupId    = "ru-wire-group";
    impl->topicIds   = {10};
    impl->cfg        = cfg;
    impl->memberId   = "m-2";
    impl->generationId = 1;
    impl->state      = GroupConsumerState::Syncing;
    impl->active.store(true, std::memory_order_relaxed);
    impl->joinDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

    hub.add(impl);

    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](const uint8_t* buf, std::size_t len) -> bool {
        capturedFrames.emplace_back(buf, buf + len);
        return true;
    };

    rpc::SyncGroupResponse syncResp;
    syncResp.sessionId     = kSessionId;
    syncResp.correlationId = 0;
    syncResp.errorCode     = 0;

    rpc::TopicPartition32 asgn;
    asgn.topicId   = 10;
    asgn.partition = 0;
    const std::span<const rpc::TopicPartition32> asgnSpan{&asgn, 1};

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(1);
    std::vector<uint8_t> syncBuf(sz);
    syncResp.encode(syncBuf.data(), asgnSpan);

    hub.onSyncGroupResponse(syncBuf.data(), syncBuf.size(), kSessionId, sendFn);

    for (const auto& f : capturedFrames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::Subscribe)) continue;
        auto subOpt = rune::client::codec::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(subOpt.has_value());
        EXPECT_EQ(subOpt->isolation, uint8_t{0})
            << "Default (ReadUncommitted) consumer must send isolation=0";
        break;
    }
}

// ===========================================================================
// Test 12: TX-6b EndTxnDeferredUntilAllProduceBatchesAcked
//
//   The bug (pre-fix): commitTransaction() / EndTxn could be sent to the broker
//   while Produce batches were still in-flight (pendingProduceBatches_ non-empty).
//   The broker would write the commit control marker BETWEEN data batches — at
//   offset N, with more data at N+1 — re-opening the txn in LsoTracker so the
//   LSO never advanced and a read_committed consumer would see only records that
//   landed before the marker.
//
//   The fix (TX-6b): doWork() defers EndTxn processing while pendingSends_ OR
//   pendingProduceBatches_ is non-empty, re-queuing the command for the next tick.
//
//   Wire-order assertion:
//     Given: 2 sends in-flight (pendingProduceBatches_ has 2 entries).
//     When:  commitTransaction() is enqueued.
//     Then:  doWork() must NOT emit EndTxn (deferred).
//     When:  both ProduceResponses are fed (batches acked).
//     Then:  doWork() MUST emit EndTxn (all batches clear).
//
//   RED (without fix): EndTxn is emitted immediately while batches are in-flight.
//   GREEN (with fix):  EndTxn is emitted only after all batches are acked.
// ===========================================================================
TEST_F(TransactionalProducerTest, EndTxnDeferredUntilAllProduceBatchesAcked)
{
    CaptureSink sink;
    auto impl = makeImpl();
    TransactionalProducer tp(impl);

    // ---- Acquire producer identity ------------------------------------------
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        ASSERT_GE(sink.frames.size(), 1u);
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, /*pid=*/42, /*epoch=*/0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        ASSERT_EQ(fut.wait_for(0s), std::future_status::ready);
        sink.clear();
    }

    tp.beginTransaction();

    TopicHandle topic{5, 1};
    Key key(uint64_t{0});
    const std::vector<uint8_t> val = {0xDE, 0xAD};

    // ---- Enqueue 2 sends + commit BEFORE any doWork so they're all pending ----
    tp.send(topic, key, std::span<const uint8_t>(val)); // send #1
    tp.send(topic, key, std::span<const uint8_t>(val)); // send #2
    auto commitFut = tp.commitTransaction();             // EndTxn enqueued

    // ---- First doWork: drains sends, queues produce batches in-flight ---------
    // The sends are drained into Produce frames and stored in pendingProduceBatches_.
    // The EndTxn command should be re-queued (deferred) because batches are in-flight.
    impl->doWork(kSessionId, sink.fn());

    // Collect all Produce corrIds from this cycle.
    std::vector<int64_t> produceCorrIds;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) {
            auto opt = rpc::Produce::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            produceCorrIds.push_back(opt->correlationId);
        }
    }
    ASSERT_EQ(produceCorrIds.size(), 2u)
        << "Both Produce frames must be sent in the first doWork cycle";

    // ---- CRITICAL assertion: EndTxn must NOT be in the first doWork output ----
    // With the bug (pre-fix): EndTxn would appear here (before batches acked).
    // With the fix:           EndTxn is deferred; not present.
    {
        bool endTxnSentEarly = false;
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
                endTxnSentEarly = true;
                break;
            }
        }
        EXPECT_FALSE(endTxnSentEarly)
            << "TX-6b BUG: EndTxn was sent BEFORE all in-flight Produce batches were "
               "acked (pendingProduceBatches_ non-empty).  commitTransaction() must "
               "flush all batches first (Kafka semantics).";
    }

    // Commit future must NOT be ready yet (EndTxn not sent, response not received).
    EXPECT_NE(commitFut.wait_for(0s), std::future_status::ready)
        << "commitTransaction() future must not resolve while Produce batches "
           "are still in-flight";

    sink.clear();

    // ---- Second doWork: still deferred (no acks yet) -------------------------
    // Simulate another tick without acks — EndTxn must still not be sent.
    impl->doWork(kSessionId, sink.fn());
    {
        bool endTxnStillDeferred = true;
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
                endTxnStillDeferred = false;
                break;
            }
        }
        EXPECT_TRUE(endTxnStillDeferred)
            << "EndTxn must remain deferred after a second doWork tick with no acks";
    }
    sink.clear();

    // ---- Ack the first Produce batch -----------------------------------------
    {
        auto ack = makeProduceResponse(produceCorrIds[0], /*errorCode=*/0, /*baseOffset=*/0);
        impl->onProduceResponse(ack.data(), ack.size(), sink.fn());
    }
    // Still one batch in-flight — EndTxn must remain deferred.
    impl->doWork(kSessionId, sink.fn());
    {
        bool endTxnAfterFirstAck = false;
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
                endTxnAfterFirstAck = true;
                break;
            }
        }
        EXPECT_FALSE(endTxnAfterFirstAck)
            << "EndTxn must NOT be sent after only the first of two Produce batches "
               "is acked (second batch still in-flight)";
    }
    sink.clear();

    // ---- Ack the second Produce batch ----------------------------------------
    {
        auto ack = makeProduceResponse(produceCorrIds[1], /*errorCode=*/0, /*baseOffset=*/1);
        impl->onProduceResponse(ack.data(), ack.size(), sink.fn());
    }

    // ---- Third doWork: all batches acked → EndTxn MUST be sent ---------------
    impl->doWork(kSessionId, sink.fn());

    bool endTxnSent = false;
    int64_t endTxnCorrId = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
            endTxnSent = true;
            auto opt = rpc::EndTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            endTxnCorrId = opt->correlationId;
            EXPECT_EQ(opt->commit, uint8_t{1})
                << "EndTxn must be a COMMIT (commit=1)";
            EXPECT_EQ(opt->producerId, 42)
                << "EndTxn must carry the correct producerId";
            break;
        }
    }
    ASSERT_TRUE(endTxnSent)
        << "TX-6b FIX NOT EFFECTIVE: EndTxn was never sent even after all "
           "Produce batches were acked.";

    // ---- Feed EndTxnResponse → commitTransaction() future must resolve --------
    {
        auto resp = makeEndTxnResponse(endTxnCorrId, /*errorCode=*/0);
        impl->onEndTxnResponse(resp.data(), resp.size());
    }

    ASSERT_EQ(commitFut.wait_for(0s), std::future_status::ready)
        << "commitTransaction() future must resolve after EndTxnResponse";
    EXPECT_NO_THROW(commitFut.get());
    EXPECT_FALSE(tp.transactionActive());
}

// ===========================================================================
// Test 13: TX-6b CommitFailsWhenProduceBatchTimesOut
//
//   Follow-up to Test 12 (flush-before-commit).  Test 12 proves EndTxn is
//   deferred while a batch is in-flight.  This test proves the ESCAPE path is
//   safe: if an in-flight Produce batch is never acked and its produceTimeout
//   expires, sweepTimedOut() drops it — but the deferred EndTxnCommit must then
//   FAIL with an exception, NOT be sent.  Otherwise a commit control marker
//   would be written over data that never became durable (the exact bug TX-6b
//   set out to prevent, just triggered by timeout instead of a race).
//
//   RED (without poison flag): the dropped batch unblocks the deferral and
//   EndTxn(commit=1) is emitted; commitFut resolves successfully.
//   GREEN (with txnFailed_):   no EndTxn is emitted; commitFut throws.
// ===========================================================================
TEST_F(TransactionalProducerTest, CommitFailsWhenProduceBatchTimesOut)
{
    CaptureSink sink;
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;  // expire in-flight batches quickly
    auto impl = makeImpl("timeout-txn-id", cfg);
    TransactionalProducer tp(impl);

    // ---- Acquire producer identity ------------------------------------------
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        ASSERT_GE(sink.frames.size(), 1u);
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, /*pid=*/42, /*epoch=*/0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        ASSERT_EQ(fut.wait_for(0s), std::future_status::ready);
        sink.clear();
    }

    tp.beginTransaction();

    TopicHandle topic{5, 1};
    Key key(uint64_t{0});
    const std::vector<uint8_t> val = {0xDE, 0xAD};

    // One send + commit, all enqueued before any doWork.
    tp.send(topic, key, std::span<const uint8_t>(val));
    auto commitFut = tp.commitTransaction();

    // First doWork: drains the send into a Produce frame (in-flight), defers EndTxn.
    impl->doWork(kSessionId, sink.fn());
    {
        int produceCount = 0;
        bool endTxnEarly = false;
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) ++produceCount;
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) endTxnEarly = true;
        }
        ASSERT_EQ(produceCount, 1) << "the send must produce one in-flight batch";
        ASSERT_FALSE(endTxnEarly) << "EndTxn must be deferred while the batch is in-flight";
    }
    sink.clear();

    // ---- Never ack the batch.  Wait past produceTimeoutMs, then tick doWork. --
    // sweepTimedOut() runs at the END of doWork, so the first post-timeout tick
    // drops the batch and sets txnFailed_ (the EndTxn is still deferred this
    // cycle because the batch is dropped after the command queue is processed).
    // The next tick observes txnFailed_ and fails the commit.
    std::this_thread::sleep_for(40ms);
    impl->doWork(kSessionId, sink.fn());  // sweeps + poisons
    sink.clear();
    impl->doWork(kSessionId, sink.fn());  // observes poison, fails commit

    // ---- CRITICAL: EndTxn must NOT be sent (would commit over lost data) ------
    {
        bool endTxnSent = false;
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
                endTxnSent = true;
                break;
            }
        }
        EXPECT_FALSE(endTxnSent)
            << "TX-6b BUG: EndTxn(commit) was sent after an in-flight Produce batch "
               "timed out without a durable ACK — the commit marker would be written "
               "over data that never landed (flush-before-commit invariant broken).";
    }

    // ---- The commit future must resolve with an EXCEPTION --------------------
    ASSERT_EQ(commitFut.wait_for(0s), std::future_status::ready)
        << "commitTransaction() future must resolve (with an exception) once the "
           "txn is poisoned by a timed-out batch";
    EXPECT_THROW(commitFut.get(), std::runtime_error)
        << "commitTransaction() must throw when a data batch was not durably acked";
    EXPECT_FALSE(tp.transactionActive())
        << "the poisoned txn must no longer be active";
}

// ===========================================================================
// Test 14: TX-6b BeginTransactionClearsPoisonFlag  (stuck-latch regression)
//
//   The stuck-latch scenario (without the fix):
//     1. TXN-1 is active.  A Produce batch times out → sweepTimedOut() sets
//        txnFailed_=true.  No commitTransaction() is called, so the EndTxn
//        poison-check path (which already resets txnFailed_) is never reached.
//     2. The app abandons TXN-1 (e.g. catches an error and retries).
//     3. beginTransaction() is called for TXN-2.  Without the fix,
//        beginTransaction() does NOT clear txnFailed_ — it is still true.
//     4. TXN-2 sends a record that is promptly acked.
//     5. commitTransaction() runs: txnFailed_=true && cmd==EndTxnCommit →
//        SPURIOUS poison exception.  Producer must be reconstructed to recover.
//
//   With the fix:
//     beginTransaction() resets txnFailed_=false (and addedPartitions_), so
//     TXN-2's commit succeeds normally.
//
//   RED trigger: remove the `txnFailed_ = false` line from beginTransaction().
//   GREEN criterion: TXN-2's commit resolves successfully + EndTxn(commit=1)
//     was emitted on the wire.
//
//   The test also validates the EndTxn send-failure branch fix: the impl
//   aborts TXN-1 via a failing send (sink.returnOk=false during EndTxn), which
//   exercises the else-branch added by Fix 2.  Without Fix 2, txnActive_ would
//   remain true after the send failure, leaving the producer wedged.
// ===========================================================================
TEST_F(TransactionalProducerTest, BeginTransactionClearsPoisonFlag)
{
    CaptureSink sink;
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;  // expire in-flight batches quickly
    auto impl = makeImpl("poison-reset-txn-id", cfg);
    TransactionalProducer tp(impl);

    // ---- Acquire producer identity ------------------------------------------
    {
        auto fut = tp.initTransactions();
        impl->doWork(kSessionId, sink.fn());
        ASSERT_GE(sink.frames.size(), 1u);
        auto opt = rpc::InitProducerId::decode(sink.frames[0].data(), sink.frames[0].size());
        ASSERT_TRUE(opt.has_value());
        auto resp = makeInitProducerIdResponse(opt->correlationId, /*pid=*/42, /*epoch=*/0);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        ASSERT_EQ(fut.wait_for(0s), std::future_status::ready);
        sink.clear();
    }

    // ---- TXN-1: produce a batch, let it time out (sets txnFailed_) ----------
    // We do NOT call commitTransaction() here — the app abandons TXN-1 without
    // going through the poison-check path, so txnFailed_ is left dirty.
    tp.beginTransaction();

    TopicHandle topic{5, 1};
    Key key(uint64_t{0});
    const std::vector<uint8_t> val = {0xDE, 0xAD};

    tp.send(topic, key, std::span<const uint8_t>(val));
    // NO commitTransaction() — batch will just time out.

    // First doWork: drains the send into an in-flight batch.
    impl->doWork(kSessionId, sink.fn());
    sink.clear();

    // Wait for the batch to time out, then tick doWork to run sweepTimedOut().
    // After this tick: txnFailed_=true (set by sweepTimedOut), txnActive_=true.
    std::this_thread::sleep_for(40ms);
    impl->doWork(kSessionId, sink.fn());  // sweeps expired batch → txnFailed_=true
    sink.clear();

    // txnActive_ is still true (no EndTxn was issued).  Now the app "abandons"
    // TXN-1: exercises the EndTxn send-failure path (Fix 2) by attempting an
    // abort with a failing transport.  This leaves txnActive_=false (with Fix 2).
    {
        sink.returnOk = false;  // make the EndTxn send fail
        auto abortFut = tp.abortTransaction();
        impl->doWork(kSessionId, sink.fn());  // EndTxn send fails → else-branch
        sink.returnOk = true;

        // With Fix 2: txnActive_=false, txnFailed_=false after the else-branch.
        // The abort future must be resolved (with exception: send failed).
        ASSERT_EQ(abortFut.wait_for(0s), std::future_status::ready)
            << "TXN-1 abort: future must resolve after EndTxn send failure";
        EXPECT_THROW(abortFut.get(), std::runtime_error)
            << "TXN-1 abort: must throw on transport send failure";
        // Fix 2 guarantees txnActive_ is false now.
        EXPECT_FALSE(tp.transactionActive())
            << "TXN-1: txnActive_ must be false after EndTxn send failure (Fix 2)";
        sink.clear();
    }

    // Now deliberately SET txnFailed_ again to simulate it being dirty —
    // the only way to do this in the in-process harness is to begin a new txn,
    // send + timeout again, without issuing any EndTxn.
    tp.beginTransaction();
    tp.send(topic, key, std::span<const uint8_t>(val));
    impl->doWork(kSessionId, sink.fn());   // Produce in-flight
    sink.clear();
    std::this_thread::sleep_for(40ms);
    impl->doWork(kSessionId, sink.fn());   // sweepTimedOut → txnFailed_=true (again)
    sink.clear();
    // txnFailed_=true, txnActive_=true — this is the stuck-latch state.

    // ---- TXN-2: beginTransaction() MUST clear txnFailed_ --------------------
    // Without Fix 1 (beginTransaction reset), txnFailed_ stays true.
    // With Fix 1, txnFailed_ is cleared and the subsequent healthy commit works.
    tp.beginTransaction();
    EXPECT_TRUE(tp.transactionActive());

    tp.send(topic, key, std::span<const uint8_t>(val));
    auto commitFut2 = tp.commitTransaction();

    // First doWork for TXN-2: emits Produce, defers EndTxn.
    impl->doWork(kSessionId, sink.fn());

    std::vector<int64_t> prodCorrIds;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) {
            auto opt = rpc::Produce::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            prodCorrIds.push_back(opt->correlationId);
        }
    }
    ASSERT_EQ(prodCorrIds.size(), 1u) << "TXN-2: exactly one Produce frame expected";
    sink.clear();

    // Ack the Produce batch.
    {
        auto ack = makeProduceResponse(prodCorrIds[0], /*errorCode=*/0, /*baseOffset=*/10);
        impl->onProduceResponse(ack.data(), ack.size(), sink.fn());
    }
    sink.clear();

    // Next doWork: all batches acked → EndTxn(commit=1) MUST be emitted.
    // Without Fix 1: txnFailed_=true → EndTxn is NOT sent; commitFut2 fails with
    // "commit aborted — a data batch was not durably acknowledged" (spurious).
    impl->doWork(kSessionId, sink.fn());

    bool endTxnSent = false;
    int64_t endTxnCorrId = 0;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
            endTxnSent = true;
            auto opt = rpc::EndTxn::decode(f.data(), f.size());
            ASSERT_TRUE(opt.has_value());
            endTxnCorrId = opt->correlationId;
            EXPECT_EQ(opt->commit, uint8_t{1})
                << "TXN-2: EndTxn must be a COMMIT — not a stuck-poison refusal";
            break;
        }
    }
    ASSERT_TRUE(endTxnSent)
        << "TXN-2: EndTxn(commit=1) must be emitted. "
           "FAIL here = beginTransaction() did NOT clear txnFailed_ (stuck-latch).";

    // Feed EndTxnResponse → commit future must resolve successfully.
    {
        auto resp = makeEndTxnResponse(endTxnCorrId, /*errorCode=*/0);
        impl->onEndTxnResponse(resp.data(), resp.size());
    }

    ASSERT_EQ(commitFut2.wait_for(0s), std::future_status::ready)
        << "TXN-2: commit future must resolve after EndTxnResponse";
    EXPECT_NO_THROW(commitFut2.get())
        << "TXN-2: commit must SUCCEED — beginTransaction() cleared txnFailed_ (Fix 1). "
           "A throw here means the stuck-poison latch was NOT fixed.";
    EXPECT_FALSE(tp.transactionActive())
        << "TXN-2: txn must be inactive after successful commit";
}
