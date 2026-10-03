// tests/producer/TransactionalProducerTest.cpp
//
// TX-5 / F8c C5 / F14 — Tier-3 TransactionalProducer unit tests.
//
// Test strategy: the TransactionalProducerImpl is the duty-side state machine.
// The rig plays the duty thread on the test thread: the handle's sinks apply
// every command synchronously (applyCommand), completions go to handler
// targets recorded in a ledger (exactly-once counts per token), doWork() is
// driven explicitly with an injected duty clock (no sleeps), the frames the
// impl sends are captured, and broker responses are fed back by hand.
//
// Test matrix (ported TX-5 tests, now on completion tokens):
//   InitTransactionsAcquiresPidEpoch, IdempotentRetryReusesBaseSequence,
//   BeginSendNewPartitionEmitsAddPartitions, SendOffsetsToTransaction...,
//   CommitTransactionEmitsEndTxnCommit, AbortTransactionEmitsEndTxnAbort,
//   EndTxnDeferredUntilAllProduceBatchesAcked, CommitFailsWhenProduceBatchTimesOut,
//   BeginTransactionClearsPoisonFlag, SendOnInvalidTopicThrowsRegression,
//   and the read_committed group-consumer config tests.
// F8c C5 (spec §5): InitTxnFencedCompletesFencedRegression,
//   CommitAbortCompleteOnceRegression, SendOffsetsTwoPhaseCompletesOnceRegression
//   (TxnCloseCompletesAllClosedRegression is Client-level: InvokerClientTest).
// F14: one ConcurrentTransactions (27) retry regression per request type,
//   AddPartitions-error gating, NotCoordinator re-discovery, EndTxn retried
//   until 0/19/17/18, the op deadline as the only bound.

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

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
using namespace rune::client::producer;

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

static constexpr int64_t kSessionId = 7777;

static constexpr int16_t ecOf(rpc::ErrorCode e) { return static_cast<int16_t>(e); }

static uint16_t peekTemplate(const std::vector<uint8_t>& buf) {
    if (buf.size() < 4) return 0;
    return static_cast<uint16_t>(buf[2]) | (static_cast<uint16_t>(buf[3]) << 8);
}

static std::vector<uint8_t> makeInitProducerIdResponse(int64_t corrId, int64_t pid, int16_t epoch,
                                                       int16_t errorCode = 0) {
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

/// AddPartitionsToTxnResponse: errorCode != 0 adds one failed result.
static std::vector<uint8_t> makeAddPartitionsToTxnResponse(int64_t corrId, int16_t errorCode = 0) {
    rpc::AddPartitionsToTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    std::vector<rpc::TxnPartitionResult> res;
    if (errorCode != 0) res.push_back(rpc::TxnPartitionResult{0, 0, errorCode});
    std::vector<uint8_t> buf(rpc::AddPartitionsToTxnResponse::encodedSizeFor(res.size()));
    msg.encode(buf.data(), std::span<const rpc::TxnPartitionResult>(res));
    return buf;
}

static std::vector<uint8_t> makeAddOffsetsToTxnResponse(int64_t corrId, int16_t errorCode = 0) {
    rpc::AddOffsetsToTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.errorCode     = errorCode;
    std::vector<uint8_t> buf(rpc::AddOffsetsToTxnResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

/// TxnOffsetCommitResponse: errorCode != 0 adds one failed result.
static std::vector<uint8_t> makeTxnOffsetCommitResponse(int64_t corrId, int16_t errorCode = 0) {
    rpc::TxnOffsetCommitResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    std::vector<rpc::TxnPartitionResult> res;
    if (errorCode != 0) res.push_back(rpc::TxnPartitionResult{42, 0, errorCode});
    std::vector<uint8_t> buf(rpc::TxnOffsetCommitResponse::encodedSizeFor(res.size()));
    msg.encode(buf.data(), std::span<const rpc::TxnPartitionResult>(res));
    return buf;
}

static std::vector<uint8_t> makeEndTxnResponse(int64_t corrId, int16_t errorCode = 0) {
    rpc::EndTxnResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = corrId;
    msg.errorCode     = errorCode;
    std::vector<uint8_t> buf(rpc::EndTxnResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

static std::vector<uint8_t> makeProduceResponse(int64_t corrId, int16_t errorCode = 0,
                                                int64_t baseOffset = 0) {
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

static int64_t corrIdOf(const std::vector<uint8_t>& f) {
    auto h = rpc::MessageHeader::decode(f.data(), f.size());
    return h ? h->correlationId : 0;
}

// ---------------------------------------------------------------------------
// CaptureSink — the frames the impl sends.
// ---------------------------------------------------------------------------
struct CaptureSink {
    std::vector<std::vector<uint8_t>> frames;
    bool returnOk = true;

    TransactionalProducerImpl::SendFn fn() {
        return [this](int32_t, const uint8_t* buf, std::size_t len) -> bool {
            if (returnOk) frames.emplace_back(buf, buf + len);
            return returnOk;
        };
    }
    void clear() { frames.clear(); }
    uint16_t templateId(std::size_t i) const { return i < frames.size() ? peekTemplate(frames[i]) : 0; }
    [[nodiscard]] int count(rpc::TemplateId t) const {
        int n = 0;
        for (const auto& f : frames) n += peekTemplate(f) == static_cast<uint16_t>(t) ? 1 : 0;
        return n;
    }
    /// The last frame of template `t` (empty if none).
    [[nodiscard]] std::vector<uint8_t> last(rpc::TemplateId t) const {
        for (std::size_t i = frames.size(); i-- > 0;) {
            if (peekTemplate(frames[i]) == static_cast<uint16_t>(t)) return frames[i];
        }
        return {};
    }
};

// ---------------------------------------------------------------------------
// TxnRig — the test thread plays the duty thread.
// ---------------------------------------------------------------------------
struct TxnLedger {
    struct Entry { int times = 0; Completion c{}; std::string text; };
    std::map<Token, Entry> e;
    static void on(void* ctx, const Completion& c, std::span<const uint8_t> p) noexcept {
        auto& x = static_cast<TxnLedger*>(ctx)->e[c.token];
        ++x.times;
        x.c = c;
        x.text.assign(reinterpret_cast<const char*>(p.data()), p.size());
    }
};

struct TxnOp {
    TxnLedger* l = nullptr;
    Token      t = 0;
    OpResult   r = OpResult::InvalidArgument;
    [[nodiscard]] bool ready() const { return l->e.count(t) != 0; }
    [[nodiscard]] int  times() const { return ready() ? l->e.at(t).times : 0; }
    [[nodiscard]] OpStatus status() const { return l->e.at(t).c.status; }
    [[nodiscard]] const Completion& c() const { return l->e.at(t).c; }
};

struct TxnRig {
    int64_t   nextCorrId = 1;  // the Client's duty-owned counter
    int64_t   now        = 1'000'000'000;
    int       rediscovers = 0;
    TxnLedger ledger;
    Token     nextToken = 0;
    CaptureSink sink;
    // F8c C7: the rig owns the impl's memory (its handle never frees it: no
    // duty side ever detaches here); tp is destroyed first.
    std::unique_ptr<TransactionalProducerImpl> impl;
    TransactionalProducer tp;

    explicit TxnRig(const std::string& txnId = "test-txn-id", ProducerConfig cfg = {})
        : impl(makeImpl(txnId, cfg, nextCorrId)),
          tp(impl.get(), &TxnRig::hubSink, this, {}, &TxnRig::opSink, nullptr) {
        impl->setRediscoverHook([](void* ctx) noexcept { ++static_cast<TxnRig*>(ctx)->rediscovers; }, this);
    }
    ~TxnRig() {
        for (const auto& [t, x] : ledger.e) EXPECT_EQ(x.times, 1) << "token " << t << " completed " << x.times;
    }

    static std::unique_ptr<TransactionalProducerImpl> makeImpl(const std::string& id, ProducerConfig cfg,
                                                               int64_t& corr) {
        cfg.acks = Acks::All;
        return std::make_unique<TransactionalProducerImpl>(id, cfg, corr, /*txnTimeoutMs=*/60'000);
    }
    static rune::client::detail::HubSubmit hubSink(void* ctx, const rune::client::detail::HubCommand& c) {
        auto* r = static_cast<TxnRig*>(ctx);
        if (c.op == rune::client::detail::HubCommand::Op::HandleRelease) {
            r->impl->applyHandleRelease();  // F8c C7: the destructor's closeImmediately (as the Client does)
            return rune::client::detail::HubSubmit::Accepted;
        }
        r->impl->applyCommand(c, r->now);
        return rune::client::detail::HubSubmit::Accepted;
    }
    static OpResult opSink(void* ctx, rune::client::detail::HubCommand& c, const CompletionTarget& target,
                           Token token, OpKind) {
        if (target.handler.fn == nullptr) return OpResult::InvalidArgument;
        rune::client::detail::OpTarget t;
        t.handler = target.handler;
        c.a = static_cast<int64_t>(token);
        rune::client::detail::encodeTarget(t, c);
        return hubSink(ctx, c) == rune::client::detail::HubSubmit::Accepted ? OpResult::Accepted
                                                                            : OpResult::Closed;
    }

    CompletionHandler handler() { return CompletionHandler{&TxnLedger::on, &ledger}; }
    TxnOp init() { const Token t = ++nextToken; return TxnOp{&ledger, t, tp.initTransactions(handler(), t)}; }
    TxnOp commit() { const Token t = ++nextToken; return TxnOp{&ledger, t, tp.commitTransaction(handler(), t)}; }
    TxnOp abort() { const Token t = ++nextToken; return TxnOp{&ledger, t, tp.abortTransaction(handler(), t)}; }
    TxnOp close() { const Token t = ++nextToken; return TxnOp{&ledger, t, tp.close(handler(), t)}; }
    TxnOp sendOffsets(const std::map<TopicPartitionKey, int64_t>& o, const std::string& g) {
        const Token t = ++nextToken;
        return TxnOp{&ledger, t, tp.sendOffsetsToTransaction(o, g, handler(), t)};
    }

    int doWork() { return impl->doWork(now, kSessionId, sink.fn()); }
    /// doWork() n times; returns how many frames of `t` were sent meanwhile.
    int cycles(int n, rpc::TemplateId t) {
        const int before = sink.count(t);
        for (int i = 0; i < n; ++i) doWork();
        return sink.count(t) - before;
    }

    /// Acquire (pid, epoch) through a full InitProducerId round trip.
    void acquire(int64_t pid = 42, int16_t epoch = 0) {
        auto op = init();
        ASSERT_EQ(op.r, OpResult::Accepted);
        doWork();
        const auto f = sink.last(rpc::TemplateId::InitProducerId);
        ASSERT_FALSE(f.empty());
        auto resp = makeInitProducerIdResponse(corrIdOf(f), pid, epoch);
        impl->onInitProducerIdResponse(resp.data(), resp.size());
        ASSERT_TRUE(op.ready());
        ASSERT_EQ(op.status(), OpStatus::Ok);
        sink.clear();
    }
    /// begin + one record fully added and acked: a STARTED transaction (an
    /// AddPartitions acknowledged OK — B-1: only those send an EndTxn).
    void startTxn(TopicHandle topic = TopicHandle{5, 1}) {
        tp.beginTransaction();
        const std::vector<uint8_t> v{0x5A};
        tp.send(topic, Key(uint64_t{0}), std::span<const uint8_t>(v));
        doWork();
        addPartitionsOk();
        doWork();
        for (const auto& f : sink.frames) {
            if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::Produce)) continue;
            rpc::ProduceResponse a;
            a.sessionId = kSessionId;
            a.correlationId = corrIdOf(f);
            std::vector<uint8_t> b(rpc::ProduceResponse::ENCODED_SIZE);
            a.encode(b.data());
            impl->onProduceResponse(b.data(), b.size());
        }
        sink.clear();
    }
    /// Answer the KIP-360 epoch-bump InitProducerId (after a doWork that sent it).
    void bumpOk(int64_t pid, int16_t epoch) {
        const auto f = sink.last(rpc::TemplateId::InitProducerId);
        ASSERT_FALSE(f.empty()) << "no epoch-bump InitProducerId was sent";
        rpc::InitProducerIdResponse m;
        m.sessionId = kSessionId;
        m.correlationId = corrIdOf(f);
        m.producerId = pid;
        m.producerEpoch = epoch;
        std::vector<uint8_t> b(rpc::InitProducerIdResponse::ENCODED_SIZE);
        m.encode(b.data());
        impl->onInitProducerIdResponse(b.data(), b.size());
    }
    /// Answer the outstanding AddPartitions OK (after a doWork that sent it).
    void addPartitionsOk() {
        const auto f = sink.last(rpc::TemplateId::AddPartitionsToTxn);
        ASSERT_FALSE(f.empty()) << "no AddPartitionsToTxn was sent";
        auto resp = makeAddPartitionsToTxnResponse(corrIdOf(f));
        impl->onAddPartitionsToTxnResponse(resp.data(), resp.size());
    }
};

// ---------------------------------------------------------------------------
// Produce-frame helpers
// ---------------------------------------------------------------------------
static int32_t decodeBaseSeqFromProduce(const std::vector<uint8_t>& frame) {
    using namespace rune::protocol::batch;
    auto popt = rune::client::codec::decodeProduce(frame.data(), frame.size());
    if (!popt) return -999;
    const auto& rb = popt->recordBatch;
    if (rb.size() < static_cast<std::size_t>(kBaseSequenceOffset + 4)) return -999;
    const uint8_t* d = rb.data();
    return static_cast<int32_t>(
        static_cast<uint32_t>(d[kBaseSequenceOffset]) |
        (static_cast<uint32_t>(d[kBaseSequenceOffset + 1]) << 8) |
        (static_cast<uint32_t>(d[kBaseSequenceOffset + 2]) << 16) |
        (static_cast<uint32_t>(d[kBaseSequenceOffset + 3]) << 24));
}

static std::vector<int64_t> collectProduceCorrIds(const CaptureSink& sink) {
    std::vector<int64_t> ids;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::Produce)) continue;
        auto opt = rpc::Produce::decode(f.data(), f.size());
        if (opt) ids.push_back(opt->correlationId);
    }
    return ids;
}

static const TopicHandle kTopic{5, 1};
static const std::vector<uint8_t> kVal = {0xDE, 0xAD};

// ===========================================================================
// Ported TX-5 tests
// ===========================================================================

TEST(TransactionalProducerTest, InitTransactionsAcquiresPidEpoch) {
    TxnRig r;
    EXPECT_EQ(r.tp.producerId(), TransactionalProducerImpl::kNoPid);

    auto op = r.init();
    ASSERT_EQ(op.r, OpResult::Accepted);
    ASSERT_GE(r.doWork(), 1);
    ASSERT_GE(r.sink.frames.size(), 1u);
    EXPECT_EQ(r.sink.templateId(0), static_cast<uint16_t>(rpc::TemplateId::InitProducerId));
    auto opt = rpc::InitProducerId::decode(r.sink.frames[0].data(), r.sink.frames[0].size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_GT(opt->correlationId, 0);
    EXPECT_EQ(opt->sessionId, kSessionId) << "the session is stamped at send time";
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(opt->transactionalId.data()),
                          opt->transactionalId.size()),
              "test-txn-id");
    EXPECT_FALSE(op.ready());

    auto resp = makeInitProducerIdResponse(opt->correlationId, 42, 1);
    r.impl->onInitProducerIdResponse(resp.data(), resp.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(op.c().value, 42) << "value = producerId";
    EXPECT_EQ(op.c().aux, 1) << "aux = producerEpoch";
    EXPECT_EQ(r.tp.producerId(), 42);
    EXPECT_EQ(r.tp.producerEpoch(), 1);
}

// Two sends to one partition carry baseSequence 0 and 1; a retriable produce
// error re-sends the SAME batch (same baseSequence) as a new attempt.
TEST(TransactionalProducerTest, IdempotentRetryReusesBaseSequence) {
    TxnRig r;
    r.acquire(99, 0);
    r.tp.beginTransaction();
    const Key key(uint64_t{0});
    r.tp.send(TopicHandle{1, 1}, key, std::span<const uint8_t>(kVal));
    r.tp.send(TopicHandle{1, 1}, key, std::span<const uint8_t>(kVal));
    r.doWork();  // AddPartitionsToTxn first (F14 gate)
    r.addPartitionsOk();
    r.sink.clear();
    r.doWork();  // now the data

    std::vector<int32_t> seqs;
    for (const auto& f : r.sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) seqs.push_back(decodeBaseSeqFromProduce(f));
    }
    ASSERT_EQ(seqs.size(), 2u);
    EXPECT_EQ(seqs[0], 0);
    EXPECT_EQ(seqs[1], 1);
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 2u);
    EXPECT_NE(ids[0], ids[1]);
    {
        auto ack = makeProduceResponse(ids[1], 0, 1);
        r.impl->onProduceResponse(ack.data(), ack.size());
    }
    r.sink.clear();
    {
        auto err = makeProduceResponse(ids[0], ecOf(rpc::ErrorCode::NotLeader));
        r.impl->onProduceResponse(err.data(), err.size());
    }
    r.impl->onCoordinatorRediscovered();  // NEW-3: a NotLeader re-offers after fresh metadata
    r.doWork();  // the retry goes out on the next cycle
    const auto retry = r.sink.last(rpc::TemplateId::Produce);
    ASSERT_FALSE(retry.empty()) << "a retriable error must re-send";
    EXPECT_EQ(decodeBaseSeqFromProduce(retry), 0) << "the retry MUST reuse baseSequence 0";
    // EOS follow-ups I-2: same bytes and baseSequence, but a NEW attempt id
    // (an answer to the first attempt can never decide the retry).
    EXPECT_NE(corrIdOf(retry), ids[0]) << "the retry is a new attempt";
    EXPECT_NE(corrIdOf(retry), ids[1]);
    EXPECT_EQ(r.tp.producerId(), 99) << "no fencing on a retriable error";
}

// The first send to a new partition emits AddPartitionsToTxn exactly once; its
// record is produced only once that is OK (F14).
TEST(TransactionalProducerTest, BeginSendNewPartitionEmitsAddPartitions) {
    TxnRig r;
    r.acquire(55, 2);
    r.tp.beginTransaction();
    EXPECT_TRUE(r.tp.transactionActive());

    const Key key(uint64_t{0});
    r.tp.send(TopicHandle{10, 1}, key, std::span<const uint8_t>(kVal));
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 1);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 0) << "data before its AddPartitions result";
    const auto ap = r.sink.last(rpc::TemplateId::AddPartitionsToTxn);
    auto opt = rpc::AddPartitionsToTxn::decode(ap.data(), ap.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->producerId, 55);
    EXPECT_EQ(opt->producerEpoch, 2);
    EXPECT_GE(opt->partitions.size(), 1u);

    r.addPartitionsOk();
    r.sink.clear();
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 1);

    r.sink.clear();
    r.tp.send(TopicHandle{10, 1}, key, std::span<const uint8_t>(kVal));
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 0) << "not again for the same partition";
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 1);
}

TEST(TransactionalProducerTest, SendOffsetsToTransactionEmitsAddOffsetsThenTxnOffsetCommit) {
    TxnRig r;
    r.acquire(77, 0);
    r.tp.beginTransaction();
    std::map<TopicPartitionKey, int64_t> offsets{{{42, 0}, 100}, {{42, 1}, 200}};
    auto op = r.sendOffsets(offsets, "my-consumer-group");
    ASSERT_EQ(op.r, OpResult::Accepted);
    r.doWork();
    const auto ao = r.sink.last(rpc::TemplateId::AddOffsetsToTxn);
    ASSERT_FALSE(ao.empty());
    auto aopt = rpc::AddOffsetsToTxn::decode(ao.data(), ao.size());
    ASSERT_TRUE(aopt.has_value());
    EXPECT_EQ(aopt->producerId, 77);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(aopt->groupId.data()), aopt->groupId.size()),
              "my-consumer-group");
    EXPECT_EQ(r.sink.count(rpc::TemplateId::TxnOffsetCommit), 0);

    auto resp = makeAddOffsetsToTxnResponse(aopt->correlationId);
    r.impl->onAddOffsetsToTxnResponse(resp.data(), resp.size());
    r.doWork();
    const auto oc = r.sink.last(rpc::TemplateId::TxnOffsetCommit);
    ASSERT_FALSE(oc.empty()) << "TxnOffsetCommit after AddOffsetsToTxnResponse";
    auto oopt = rpc::TxnOffsetCommit::decode(oc.data(), oc.size());
    ASSERT_TRUE(oopt.has_value());
    EXPECT_EQ(oopt->producerId, 77);
    EXPECT_EQ(oopt->entries.size(), 2u);
    EXPECT_FALSE(op.ready());

    auto done = makeTxnOffsetCommitResponse(oopt->correlationId);
    r.impl->onTxnOffsetCommitResponse(done.data(), done.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
}

TEST(TransactionalProducerTest, CommitTransactionEmitsEndTxnCommit) {
    TxnRig r;
    r.acquire(88, 1);
    r.startTxn();  // B-1: a started transaction (an EndTxn is sent)
    auto op = r.commit();
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    auto opt = rpc::EndTxn::decode(et.data(), et.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->producerId, 88);
    EXPECT_EQ(opt->producerEpoch, 1);
    EXPECT_EQ(opt->commit, uint8_t{1});
    auto resp = makeEndTxnResponse(opt->correlationId, 0);
    r.impl->onEndTxnResponse(resp.data(), resp.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_FALSE(r.tp.transactionActive());
}

TEST(TransactionalProducerTest, AbortTransactionEmitsEndTxnAbort) {
    TxnRig r;
    r.acquire(66, 0);
    r.startTxn();  // B-1: a started transaction (an EndTxn is sent)
    auto op = r.abort();
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    auto opt = rpc::EndTxn::decode(et.data(), et.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->commit, uint8_t{0});
    auto resp = makeEndTxnResponse(opt->correlationId, 0);
    r.impl->onEndTxnResponse(resp.data(), resp.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_FALSE(r.tp.transactionActive());
}

// F8c C5 (was ProducerFencedSurfacesError) + fix round 1 (I-2):
// InitProducerId{ProducerFenced} completes the init Fenced and the producer is
// FatalError — terminal: misuse while Initializing is refused at once, and
// after the fence every op completes Fenced and nothing is ever sent again.
TEST(TransactionalProducerTest, InitTxnFencedCompletesFencedRegression) {
    TxnRig r;
    auto init = r.init();
    ASSERT_EQ(r.tp.state(), TxnState::Initializing);
    // Gated by state while Initializing: refused at once (exactly once).
    auto other  = r.sendOffsets({{{1, 0}, 5}}, "g");
    auto commit = r.commit();
    auto init2  = r.init();
    for (auto* op : {&other, &commit, &init2}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::BrokerError);
        EXPECT_EQ(op->c().errorCode, static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState));
    }
    r.doWork();
    const auto f = r.sink.last(rpc::TemplateId::InitProducerId);
    ASSERT_FALSE(f.empty());
    ASSERT_EQ(r.sink.count(rpc::TemplateId::InitProducerId), 1) << "the refused init sent nothing";
    auto resp = makeInitProducerIdResponse(corrIdOf(f), -1, 0, ecOf(rpc::ErrorCode::ProducerFenced));
    r.impl->onInitProducerIdResponse(resp.data(), resp.size());
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::Fenced);
    r.impl->onInitProducerIdResponse(resp.data(), resp.size());  // late duplicate: ignored
    EXPECT_EQ(init.times(), 1);
    EXPECT_EQ(r.tp.producerId(), TransactionalProducerImpl::kNoPid);
    EXPECT_EQ(r.tp.state(), TxnState::FatalError);
    EXPECT_EQ(r.impl->testPendingRequests(), 0u);
}

// Fix round 1 (I-2): fencing is terminal (Kafka ProducerFencedException):
// after 18 on EndTxn, initTransactions is refused Fenced and NO InitProducerId
// (no network op at all) is ever sent — a zombie can never bump the epoch
// and fence the legitimate new instance.
TEST(TransactionalProducerTest, FencedProducerRefusesInitRegression) {
    TxnRig r;
    r.acquire();
    r.startTxn();  // B-1: a started transaction (an EndTxn is sent)
    auto commit = r.commit();
    r.doWork();
    auto fenced = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)),
                                     ecOf(rpc::ErrorCode::ProducerFenced));
    r.impl->onEndTxnResponse(fenced.data(), fenced.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Fenced);
    EXPECT_EQ(r.tp.state(), TxnState::FatalError);
    r.sink.clear();

    auto init = r.init();
    ASSERT_TRUE(init.ready()) << "refused at once";
    EXPECT_EQ(init.status(), OpStatus::Fenced);
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{1}), std::span<const uint8_t>(kVal));
    auto abort = r.abort();
    auto offs  = r.sendOffsets({{{1, 0}, 1}}, "g");
    for (auto* op : {&abort, &offs}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::Fenced);
    }
    for (int i = 0; i < 64; ++i) r.doWork();
    EXPECT_TRUE(r.sink.frames.empty()) << "a fenced producer sent a frame";
    EXPECT_EQ(r.tp.state(), TxnState::FatalError);
    EXPECT_EQ(r.tp.producerId(), 42) << "identity kept for diagnostics; never used again";
}

// TX-6b: EndTxn is deferred until every in-flight Produce batch is acked.
TEST(TransactionalProducerTest, EndTxnDeferredUntilAllProduceBatchesAcked) {
    TxnRig r;
    r.acquire(42, 0);
    r.tp.beginTransaction();
    const Key key(uint64_t{0});
    r.tp.send(kTopic, key, std::span<const uint8_t>(kVal));
    r.tp.send(kTopic, key, std::span<const uint8_t>(kVal));
    auto commit = r.commit();
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "AddPartitions not even answered";
    r.addPartitionsOk();
    r.sink.clear();
    r.doWork();
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "EndTxn BEFORE the batches were acked";
    EXPECT_FALSE(commit.ready());
    EXPECT_EQ(r.cycles(3, rpc::TemplateId::EndTxn), 0);
    {
        auto ack = makeProduceResponse(ids[0], 0, 0);
        r.impl->onProduceResponse(ack.data(), ack.size());
    }
    EXPECT_EQ(r.cycles(1, rpc::TemplateId::EndTxn), 0) << "one batch still in flight";
    {
        auto ack = makeProduceResponse(ids[1], 0, 1);
        r.impl->onProduceResponse(ack.data(), ack.size());
    }
    ASSERT_EQ(r.cycles(1, rpc::TemplateId::EndTxn), 1) << "every batch acked: EndTxn now";
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    auto opt = rpc::EndTxn::decode(et.data(), et.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->commit, uint8_t{1});
    EXPECT_EQ(opt->producerId, 42);
    auto resp = makeEndTxnResponse(opt->correlationId, 0);
    r.impl->onEndTxnResponse(resp.data(), resp.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    EXPECT_FALSE(r.tp.transactionActive());
}

// TX-6b + fix round 1 (C-1): a batch that times out (duty clock — no sleep)
// poisons the txn: the commit completes DeliveryFailed without an EndTxn and
// the producer is AbortableError — the broker transaction is still open, so
// it stays "active" until the app aborts and EndTxn(abort) is answered.
TEST(TransactionalProducerTest, CommitFailsWhenProduceBatchTimesOut) {
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;
    TxnRig r("timeout-txn-id", cfg);
    r.acquire(42, 0);
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::Produce), 1);
    r.now += 15'000'000;
    auto commit = r.commit();
    r.now += 10'000'000;  // the batch (20 ms) expired
    r.sink.clear();
    r.doWork();
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "commit marker over lost data";
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed);
    EXPECT_EQ(r.tp.state(), TxnState::AbortableError);
    EXPECT_TRUE(r.tp.transactionActive()) << "the broker transaction is still open";
    auto abort = r.abort();
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, uint8_t{0});
    EXPECT_TRUE(r.tp.transactionActive()) << "reset only on EndTxn's final answer";
    auto ok = makeEndTxnResponse(corrIdOf(et), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    // Fix round 2 (B-3, KIP-360): the lost batch left a sequence gap — the
    // abort completes only after the epoch bump.
    EXPECT_FALSE(abort.ready());
    r.doWork();
    r.bumpOk(42, 1);
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
    EXPECT_FALSE(r.tp.transactionActive());
}

// Fix round 1 (C-1; was BeginTransactionClearsPoisonFlag — SPEC CORRECTION:
// that test asserted a begin could clear the poison and the next commit
// succeed, which on the broker commits the poisoned transaction's orphaned
// data).  Kafka: a poisoned transaction must be ABORTED.  While
// AbortableError: begin is refused, sends are refused (never produced), a
// commit completes with the transaction's error and sends nothing; only
// abortTransaction() proceeds (EndTxn(abort), a refused offer re-offered),
// and the next transaction starts only after its final answer.
TEST(TransactionalProducerTest, PoisonedTransactionMustBeAbortedBeforeTheNextRegression) {
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;
    TxnRig r("poison-txn-id", cfg);
    r.acquire(42, 0);

    // TXN-1: one record lands (acked), a second times out: poisoned.
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    auto first = collectProduceCorrIds(r.sink);
    ASSERT_EQ(first.size(), 1u);
    auto ack = makeProduceResponse(first[0], 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    r.now += 40'000'000;
    r.doWork();  // the second batch expires
    ASSERT_EQ(r.tp.state(), TxnState::AbortableError);
    r.sink.clear();

    // The app tries to carry on: refused, nothing produced, no EndTxn(commit).
    const uint64_t refusedBegins = r.impl->testRefusedBegins();
    r.tp.beginTransaction();
    EXPECT_EQ(r.impl->testRefusedBegins(), refusedBegins + 1) << "begin while AbortableError";
    r.tp.send(kTopic, Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    auto commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed) << "commit completes with the txn's error";
    for (int i = 0; i < 8; ++i) r.doWork();
    EXPECT_TRUE(r.sink.frames.empty()) << "a poisoned transaction sent a frame";

    // Abort: the first EndTxn(abort) offer is refused, then re-offered.
    r.sink.returnOk = false;
    auto abort = r.abort();
    r.doWork();
    EXPECT_FALSE(abort.ready());
    EXPECT_EQ(r.tp.state(), TxnState::AbortingTransaction);
    r.sink.returnOk = true;
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty()) << "the refused EndTxn was never re-offered";
    EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, uint8_t{0});
    // Still in the transaction until the final answer: a begin is refused.
    r.tp.beginTransaction();
    EXPECT_EQ(r.impl->testRefusedBegins(), refusedBegins + 2);
    auto okAbort = makeEndTxnResponse(corrIdOf(et), 0);
    r.impl->onEndTxnResponse(okAbort.data(), okAbort.size());
    r.doWork();
    r.bumpOk(42, 1);  // B-3: the lost batch's sequence gap — epoch bumped
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
    r.sink.clear();

    // TXN-2 commits normally.
    r.tp.beginTransaction();
    EXPECT_TRUE(r.tp.transactionActive());
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit2 = r.commit();
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 1u);
    auto ack2 = makeProduceResponse(ids[0], 0, 10);
    r.impl->onProduceResponse(ack2.data(), ack2.size());
    r.doWork();
    const auto et2 = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et2.empty());
    EXPECT_EQ(rpc::EndTxn::decode(et2.data(), et2.size())->commit, uint8_t{1});
    auto ok2 = makeEndTxnResponse(corrIdOf(et2), 0);
    r.impl->onEndTxnResponse(ok2.data(), ok2.size());
    ASSERT_TRUE(commit2.ready());
    EXPECT_EQ(commit2.status(), OpStatus::Ok);
    EXPECT_FALSE(r.tp.transactionActive());
}

// Fix round 1 (C-1): after a poisoned commit (AddPartitions refused for one
// partition while another's record landed), NO later commit can carry the
// orphaned record: the first EndTxn ever sent is an abort, and the next
// transaction's EndTxn(commit) follows its answer.
TEST(TransactionalProducerTest, PoisonedCommitNeverCommitsOrphanedDataRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    const TopicHandle good{5, 1};
    const TopicHandle bad{9, 1};
    r.tp.send(good, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.tp.send(bad, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit = r.commit();
    r.doWork();
    int64_t badCorr = 0, goodCorr = 0;
    for (const auto& f : r.sink.frames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxn)) continue;
        auto ap = rpc::AddPartitionsToTxn::decode(f.data(), f.size());
        (ap->partitions[0].topicId == bad.topicId ? badCorr : goodCorr) = ap->correlationId;
    }
    auto okResp = makeAddPartitionsToTxnResponse(goodCorr);
    r.impl->onAddPartitionsToTxnResponse(okResp.data(), okResp.size());
    r.doWork();  // the good partition's record lands
    auto ackIds = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ackIds.size(), 1u);
    auto ack = makeProduceResponse(ackIds[0], 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    auto refused = makeAddPartitionsToTxnResponse(badCorr, ecOf(rpc::ErrorCode::UnknownTopic));
    r.impl->onAddPartitionsToTxnResponse(refused.data(), refused.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);

    // The app retries the whole thing as a "new" transaction — refused until aborted.
    r.tp.beginTransaction();
    r.tp.send(good, Key(uint64_t{1}), std::span<const uint8_t>(kVal));
    auto commitAgain = r.commit();
    ASSERT_TRUE(commitAgain.ready());
    EXPECT_EQ(commitAgain.status(), OpStatus::BrokerError);
    for (int i = 0; i < 8; ++i) r.doWork();
    auto abort = r.abort();
    for (int i = 0; i < 4; ++i) r.doWork();
    std::vector<uint8_t> commits;  // the commit flag of every EndTxn, in wire order
    for (const auto& f : r.sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::EndTxn)) {
            commits.push_back(rpc::EndTxn::decode(f.data(), f.size())->commit);
        }
    }
    ASSERT_EQ(commits.size(), 1u);
    EXPECT_EQ(commits[0], 0) << "the orphaned record must be ABORTED, never committed";
    auto okAbort = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(okAbort.data(), okAbort.size());
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
}

// Fix round 1 (I-1): begin is refused while a commit is pending (pipelined
// token ops: begin, send, commit, begin, send, commit): the second begin and
// its send are refused (never produced), and the first commits only its own
// record.  Fix round 3 (m13, Kafka): the second commit JOINS the pending one
// (one EndTxn, the same result for both tokens) instead of failing 19.
TEST(TransactionalProducerTest, BeginWhileCommittingIsRefusedRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit1 = r.commit();
    r.tp.beginTransaction();  // while CommittingTransaction
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit2 = r.commit();
    EXPECT_EQ(r.impl->testRefusedBegins(), 1u);
    EXPECT_EQ(r.impl->testRefusedSends(), 1u);
    EXPECT_FALSE(commit2.ready()) << "m13: joined the pending commit";
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 1u) << "only the first transaction's record";
    auto ack = makeProduceResponse(ids[0], 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    r.doWork();
    auto ok = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    ASSERT_TRUE(commit1.ready());
    EXPECT_EQ(commit1.status(), OpStatus::Ok);
    ASSERT_TRUE(commit2.ready());
    EXPECT_EQ(commit2.status(), OpStatus::Ok);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1) << "one EndTxn for both";
    EXPECT_EQ(r.cycles(8, rpc::TemplateId::Produce), 0) << "the refused send was produced later";
}

// Fix round 1 (I-1): a send outside a transaction (before begin, and while
// the EndTxn is in flight) is refused: no AddPartitions, no Produce frame,
// no transactional bit on the wire, ever.
TEST(TransactionalProducerTest, SendOutsideATransactionIsRefusedRegression) {
    TxnRig r;
    r.acquire();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));  // Ready: no txn
    for (int i = 0; i < 8; ++i) r.doWork();
    EXPECT_TRUE(r.sink.frames.empty()) << "a record outside a transaction reached the wire";
    EXPECT_EQ(r.impl->testRefusedSends(), 1u);
    EXPECT_EQ(r.impl->testWaitingRecords(), 0u);
    r.startTxn();  // B-1: a started transaction (an EndTxn is sent)
    auto commit = r.commit();
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1);
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));  // EndTxn in flight
    for (int i = 0; i < 8; ++i) r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 0);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 0);
    EXPECT_EQ(r.impl->testRefusedSends(), 2u);
    auto ok = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// Fix round 2 (B-2, Kafka m1 — supersedes round 1's "EndTxn timeout ->
// AbortableError"): an EndTxn that times out is IN DOUBT — the producer stays
// Committing (never Ready over an open transaction, never an abort that could
// contradict a commit that landed).  abort and begin are refused; the app
// retries commitTransaction, which re-sends EndTxn(commit) for the same
// transaction (the F15 broker answers a same-direction retry 0).  The same
// holds for an in-doubt abort (stays Aborting; retry abort).
TEST(TransactionalProducerTest, EndTxnTimeoutStaysInDoubtAndIsRetriedRegression) {
    for (const bool commitDir : {true, false}) {
        SCOPED_TRACE(commitDir ? "commit" : "abort");
        TxnRig r;
        r.acquire();
        r.startTxn();
        auto first = commitDir ? r.commit() : r.abort();
        r.doWork();
        ASSERT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1);  // never answered
        r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
        r.doWork();
        ASSERT_TRUE(first.ready());
        EXPECT_EQ(first.status(), OpStatus::Timeout);
        const TxnState inDoubt = commitDir ? TxnState::CommittingTransaction : TxnState::AbortingTransaction;
        EXPECT_EQ(r.tp.state(), inDoubt) << "in doubt: never Ready / AbortableError";
        EXPECT_TRUE(r.tp.transactionActive());
        r.tp.beginTransaction();
        EXPECT_EQ(r.impl->testRefusedBegins(), 1u);
        if (commitDir) {
            auto abort = r.abort();  // Kafka: COMMITTING -> ABORTING is invalid
            ASSERT_TRUE(abort.ready());
            EXPECT_EQ(abort.status(), OpStatus::BrokerError);
        }
        EXPECT_EQ(r.cycles(64, rpc::TemplateId::EndTxn), 0) << "re-sent before the app retried";
        auto retry = commitDir ? r.commit() : r.abort();
        ASSERT_EQ(retry.r, OpResult::Accepted);
        r.doWork();
        ASSERT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 2) << "the retry re-sends EndTxn";
        const auto et = r.sink.last(rpc::TemplateId::EndTxn);
        EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, commitDir ? uint8_t{1} : uint8_t{0})
            << "the same direction";
        auto ok = makeEndTxnResponse(corrIdOf(et), 0);
        r.impl->onEndTxnResponse(ok.data(), ok.size());
        ASSERT_TRUE(retry.ready());
        EXPECT_EQ(retry.status(), OpStatus::Ok);
        EXPECT_EQ(r.tp.state(), TxnState::Ready);
    }
}

// Fix round 2 (B-1, Kafka transactionStarted): a transaction in which nothing
// was ever added (no AddPartitions / AddOffsets acknowledged) ends LOCALLY —
// no EndTxn (the coordinator answers 19 for a non-Ongoing transaction, which
// is fatal).  Also an abort after the only AddPartitions was refused.
TEST(TransactionalProducerTest, EmptyTransactionSendsNoEndTxnRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    auto commit = r.commit();
    r.doWork();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
    r.tp.beginTransaction();
    auto abort = r.abort();
    r.doWork();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    // The only partition refused: never started -> local abort.
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    auto refused = makeAddPartitionsToTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::AddPartitionsToTxn)),
                                                  ecOf(rpc::ErrorCode::UnknownTopic));
    r.impl->onAddPartitionsToTxnResponse(refused.data(), refused.size());
    ASSERT_EQ(r.tp.state(), TxnState::AbortableError);
    auto abort2 = r.abort();
    r.doWork();
    ASSERT_TRUE(abort2.ready());
    EXPECT_EQ(abort2.status(), OpStatus::Ok);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "an EndTxn for a transaction that never started";
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
}

// Fix round 2 (B-3, KIP-360): a batch lost to its deadline leaves a sequence
// gap on the broker.  The abort then bumps the epoch (InitProducerId after
// EndTxn(abort)) and restarts every sequence, so the next transaction on that
// partition is NOT poisoned: its record goes out at baseSequence 0 under the
// bumped epoch and commits.
TEST(TransactionalProducerTest, LostBatchAbortBumpsEpochAndRestartsSequencesRegression) {
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;
    TxnRig r("bump-txn-id", cfg);
    r.acquire(42, 0);
    r.startTxn(kTopic);  // seq 0 acked
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();  // seq 1 in flight — never acked
    r.now += 40'000'000;
    r.doWork();  // lost: a gap at seq 1
    ASSERT_EQ(r.tp.state(), TxnState::AbortableError);
    r.sink.clear();
    auto abort = r.abort();
    r.doWork();
    auto okAbort = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(okAbort.data(), okAbort.size());
    EXPECT_FALSE(abort.ready()) << "the abort waits for the epoch bump";
    EXPECT_EQ(r.tp.state(), TxnState::AbortingTransaction);
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::InitProducerId), 1) << "no KIP-360 epoch bump";
    r.bumpOk(42, 1);
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
    EXPECT_EQ(r.tp.producerEpoch(), 1);
    r.sink.clear();

    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit = r.commit();
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    const auto pf = r.sink.last(rpc::TemplateId::Produce);
    ASSERT_FALSE(pf.empty());
    EXPECT_EQ(decodeBaseSeqFromProduce(pf), 0) << "the sequence restarts under the bumped epoch";
    auto pv = rpc::Produce::decode(pf.data(), pf.size());
    ASSERT_TRUE(pv.has_value());
    EXPECT_EQ(pv->producerEpoch, 1);
    auto ack = makeProduceResponse(corrIdOf(pf), 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    r.doWork();
    auto okCommit = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(okCommit.data(), okCommit.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok) << "the next transaction on that partition is not poisoned";
}

// Fix round 2 (m5): abort while an abort is pending joins it — one EndTxn,
// both tokens complete with its result, exactly once.
TEST(TransactionalProducerTest, AbortWhileAbortingJoinsThePendingAbortRegression) {
    TxnRig r;
    r.acquire();
    r.startTxn();
    auto a1 = r.abort();
    auto a2 = r.abort();
    EXPECT_FALSE(a2.ready()) << "joined, not refused";
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1);
    auto ok = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    for (auto* op : {&a1, &a2}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::Ok);
        EXPECT_EQ(op->times(), 1);
    }
}

// Fix round 2 (m3, Kafka InitProducerIdHandler): a non-retriable
// InitProducerId error is fatal — init is not retried into a zombie.
TEST(TransactionalProducerTest, NonRetriableInitProducerIdErrorIsFatalRegression) {
    TxnRig r;
    auto init = r.init();
    r.doWork();
    auto bad = makeInitProducerIdResponse(corrIdOf(r.sink.last(rpc::TemplateId::InitProducerId)), -1, 0,
                                          ecOf(rpc::ErrorCode::Internal));
    r.impl->onInitProducerIdResponse(bad.data(), bad.size());
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::BrokerError);
    EXPECT_EQ(r.tp.state(), TxnState::FatalError);
    auto again = r.init();
    ASSERT_TRUE(again.ready());
    EXPECT_EQ(again.status(), OpStatus::Fenced);
}

// Fix round 1 (I-4): EndTxn gets its own transactionOpTimeoutMs budget from
// the moment it is created — time spent waiting for the data does not eat
// its 27-retry budget.
TEST(TransactionalProducerTest, EndTxnDeadlineStartsAtItsCreationRegression) {
    ProducerConfig cfg;
    cfg.produceTimeoutMs       = 100'000;  // the batch outlives the waits below
    cfg.transactionOpTimeoutMs = 10'000;
    TxnRig r("deadline-txn-id", cfg);
    r.acquire();
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto commit = r.commit();  // applied at t0 (op deadline t0 + 10 s)
    r.doWork();
    r.addPartitionsOk();
    r.doWork();
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 1u);
    r.now += 9'000'000'000LL;  // the data takes 9 s to be acked
    auto ack = makeProduceResponse(ids[0], 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    r.doWork();  // EndTxn created now (t0 + 9 s): its own 10 s budget
    const int64_t et = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
    ASSERT_NE(et, 0);
    auto busy = makeEndTxnResponse(et, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    r.impl->onEndTxnResponse(busy.data(), busy.size());
    r.now += 5'000'000'000LL;  // t0 + 14 s: past the apply-time deadline, inside EndTxn's
    r.doWork();
    r.doWork();
    EXPECT_FALSE(commit.ready()) << "EndTxn timed out on the data-wait clock";
    auto ok = makeEndTxnResponse(et, 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// fix1 I8: an invalid topic is never silently dropped — it throws.
TEST(TransactionalProducerTest, SendOnInvalidTopicThrowsRegression) {
    TxnRig r;
    EXPECT_THROW(r.tp.send(TopicHandle{}, Key(uint64_t{1}), std::span<const uint8_t>(kVal)),
                 std::invalid_argument);
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
    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
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

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId  = "rc-group";
    impl->topicIds = {10};
    impl->cfg      = cfg;
    impl->active = true;

    hub.add(impl.get());

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

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId   = "rc-wire-group";
    impl->topicIds  = {42};
    impl->cfg       = cfg;
    impl->memberId  = "member-1";
    impl->generationId = 1;
    impl->state     = GroupConsumerState::Syncing; // ready to receive SyncGroupResponse
    impl->active = true;
    impl->joinDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

    hub.add(impl.get());

    // Capture all frames sent by the hub.
    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
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

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId    = "ru-wire-group";
    impl->topicIds   = {10};
    impl->cfg        = cfg;
    impl->memberId   = "m-2";
    impl->generationId = 1;
    impl->state      = GroupConsumerState::Syncing;
    impl->active = true;
    impl->joinDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

    hub.add(impl.get());

    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
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
// F8c C5 — exactly-once
// ===========================================================================

// commit and abort each complete exactly once: through a 27 retry, and with
// duplicate / late responses ignored.
TEST(TransactionalProducerTest, CommitAbortCompleteOnceRegression) {
    for (const bool doCommit : {true, false}) {
        SCOPED_TRACE(doCommit ? "commit" : "abort");
        TxnRig r;
        r.acquire();
        r.startTxn();  // B-1: a started transaction (an EndTxn is sent)
        auto op = doCommit ? r.commit() : r.abort();
        ASSERT_EQ(op.r, OpResult::Accepted);
        r.doWork();
        const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
        ASSERT_NE(corr, 0);
        auto busy = makeEndTxnResponse(corr, ecOf(rpc::ErrorCode::ConcurrentTransactions));
        r.impl->onEndTxnResponse(busy.data(), busy.size());
        EXPECT_FALSE(op.ready()) << "27 is retriable, not a completion";
        ASSERT_EQ(r.cycles(2, rpc::TemplateId::EndTxn), 1) << "re-sent after the backoff";
        auto ok = makeEndTxnResponse(corr, 0);
        r.impl->onEndTxnResponse(ok.data(), ok.size());
        r.impl->onEndTxnResponse(ok.data(), ok.size());     // duplicate
        r.impl->onEndTxnResponse(busy.data(), busy.size()); // late
        r.doWork();
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::Ok);
        EXPECT_EQ(op.times(), 1);
        EXPECT_EQ(r.cycles(4, rpc::TemplateId::EndTxn), 0) << "nothing re-sent after completion";
        EXPECT_EQ(r.impl->testPendingOps(), 0u);
    }
}

// sendOffsets is two-phase (AddOffsetsToTxn -> TxnOffsetCommit) and completes
// exactly once — never at phase 1, never twice on duplicates.
TEST(TransactionalProducerTest, SendOffsetsTwoPhaseCompletesOnceRegression) {
    TxnRig r;
    r.acquire(77, 3);
    r.tp.beginTransaction();
    auto op = r.sendOffsets({{{42, 0}, 9}}, "grp");
    r.doWork();
    const int64_t ao = corrIdOf(r.sink.last(rpc::TemplateId::AddOffsetsToTxn));
    ASSERT_NE(ao, 0);
    auto aoOk = makeAddOffsetsToTxnResponse(ao);
    r.impl->onAddOffsetsToTxnResponse(aoOk.data(), aoOk.size());
    EXPECT_FALSE(op.ready()) << "phase 1 is not the completion";
    r.impl->onAddOffsetsToTxnResponse(aoOk.data(), aoOk.size());  // duplicate: ignored
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::TxnOffsetCommit), 1) << "phase 2 started exactly once";
    const int64_t oc = corrIdOf(r.sink.last(rpc::TemplateId::TxnOffsetCommit));
    // A commit waits for the offsets (they are part of the transaction).
    auto commit = r.commit();
    EXPECT_EQ(r.cycles(2, rpc::TemplateId::EndTxn), 0) << "EndTxn before the offsets landed";
    auto ocOk = makeTxnOffsetCommitResponse(oc);
    r.impl->onTxnOffsetCommitResponse(ocOk.data(), ocOk.size());
    r.impl->onTxnOffsetCommitResponse(ocOk.data(), ocOk.size());  // duplicate
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(op.times(), 1);
    EXPECT_EQ(r.cycles(1, rpc::TemplateId::EndTxn), 1) << "now the EndTxn";
    auto et = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(et.data(), et.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// ===========================================================================
// F14 — ConcurrentTransactions (27) is retriable for every txn request
// ===========================================================================

// InitProducerId: 27 -> re-sent after min(2^attempt, cap) duty cycles (1, 2,
// 4, ...) — counted in cycles, no wall clock — then Ok.
TEST(TransactionalProducerTest, InitProducerIdRetriesConcurrentTransactionsRegression) {
    TxnRig r;
    auto op = r.init();
    r.doWork();
    const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::InitProducerId));
    auto busy = makeInitProducerIdResponse(corr, -1, 0, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    for (int attempt = 0; attempt < 4; ++attempt) {
        SCOPED_TRACE(attempt);
        r.impl->onInitProducerIdResponse(busy.data(), busy.size());
        ASSERT_FALSE(op.ready()) << "27 must not complete the op";
        const int wait = 1 << attempt;
        EXPECT_EQ(r.cycles(wait - 1, rpc::TemplateId::InitProducerId), 0) << "re-sent before its backoff";
        ASSERT_EQ(r.cycles(1, rpc::TemplateId::InitProducerId), 1) << "not re-sent after 2^" << attempt << " cycles";
        EXPECT_EQ(corrIdOf(r.sink.last(rpc::TemplateId::InitProducerId)), corr) << "the retained frame";
    }
    auto ok = makeInitProducerIdResponse(corr, 4242, 7);
    r.impl->onInitProducerIdResponse(ok.data(), ok.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.producerId(), 4242);
}

// The backoff is capped (kRetryBackoffCapCycles) and the op's own deadline
// is the only time bound: 27 forever -> Timeout, exactly once.
TEST(TransactionalProducerTest, ConcurrentTransactionsRetryIsBoundedOnlyByTheOpDeadlineRegression) {
    TxnRig r;
    auto op = r.init();
    r.doWork();
    const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::InitProducerId));
    auto busy = makeInitProducerIdResponse(corr, -1, 0, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    int resends = 0;
    for (int attempt = 0; attempt < 14; ++attempt) {
        r.impl->onInitProducerIdResponse(busy.data(), busy.size());
        int waited = 0;
        while (r.cycles(1, rpc::TemplateId::InitProducerId) == 0) {
            ASSERT_LE(++waited, static_cast<int>(TransactionalProducerImpl::kRetryBackoffCapCycles))
                << "backoff beyond the cap";
        }
        ++resends;
    }
    EXPECT_EQ(resends, 14);
    EXPECT_FALSE(op.ready()) << "no wall-clock bound: still retrying";
    r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;  // the op's deadline
    r.doWork();
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Timeout);
    EXPECT_EQ(r.impl->testPendingRequests(), 0u);
    EXPECT_EQ(r.cycles(2'048, rpc::TemplateId::InitProducerId), 0) << "nothing after the timeout";
}

TEST(TransactionalProducerTest, AddPartitionsRetriesConcurrentTransactionsRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::AddPartitionsToTxn));
    ASSERT_NE(corr, 0);
    auto busy = makeAddPartitionsToTxnResponse(corr, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    r.impl->onAddPartitionsToTxnResponse(busy.data(), busy.size());
    EXPECT_EQ(r.cycles(4, rpc::TemplateId::Produce), 0) << "no data while AddPartitions is retried";
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 2) << "27 re-sent the AddPartitions";
    r.impl->onAddPartitionsToTxnResponse(busy.data(), busy.size());
    ASSERT_EQ(r.cycles(2, rpc::TemplateId::AddPartitionsToTxn), 1);
    r.addPartitionsOk();
    EXPECT_EQ(r.cycles(1, rpc::TemplateId::Produce), 1) << "the record after its AddPartitions OK";
}

TEST(TransactionalProducerTest, AddOffsetsRetriesConcurrentTransactionsRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    auto op = r.sendOffsets({{{42, 0}, 1}}, "g");
    r.doWork();
    const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::AddOffsetsToTxn));
    auto busy = makeAddOffsetsToTxnResponse(corr, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    r.impl->onAddOffsetsToTxnResponse(busy.data(), busy.size());
    EXPECT_FALSE(op.ready());
    ASSERT_EQ(r.cycles(1, rpc::TemplateId::AddOffsetsToTxn), 1) << "27 re-sent the AddOffsets";
    EXPECT_EQ(r.sink.count(rpc::TemplateId::TxnOffsetCommit), 0);
    auto ok = makeAddOffsetsToTxnResponse(corr);
    r.impl->onAddOffsetsToTxnResponse(ok.data(), ok.size());
    ASSERT_EQ(r.cycles(1, rpc::TemplateId::TxnOffsetCommit), 1);
    auto done = makeTxnOffsetCommitResponse(corrIdOf(r.sink.last(rpc::TemplateId::TxnOffsetCommit)));
    r.impl->onTxnOffsetCommitResponse(done.data(), done.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
}

TEST(TransactionalProducerTest, TxnOffsetCommitRetriesConcurrentTransactionsRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    auto op = r.sendOffsets({{{42, 0}, 1}}, "g");
    r.doWork();
    auto ok = makeAddOffsetsToTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::AddOffsetsToTxn)));
    r.impl->onAddOffsetsToTxnResponse(ok.data(), ok.size());
    r.doWork();
    const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::TxnOffsetCommit));
    ASSERT_NE(corr, 0);
    auto busy = makeTxnOffsetCommitResponse(corr, ecOf(rpc::ErrorCode::ConcurrentTransactions));
    r.impl->onTxnOffsetCommitResponse(busy.data(), busy.size());
    EXPECT_FALSE(op.ready()) << "27 must not complete the op";
    ASSERT_EQ(r.cycles(1, rpc::TemplateId::TxnOffsetCommit), 1) << "27 re-sent the TxnOffsetCommit";
    auto done = makeTxnOffsetCommitResponse(corr);
    r.impl->onTxnOffsetCommitResponse(done.data(), done.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
}

// EndTxn is retried until 0 / 19 / 17 / 18: 27, and anything else (10
// MinIsrViolation, 6 Internal), is retried; 19 completes BrokerError, 18
// completes Fenced.
TEST(TransactionalProducerTest, EndTxnRetriesConcurrentTransactionsUntilTerminalRegression) {
    struct Case { int16_t terminal; OpStatus expect; };
    for (const Case cs : {Case{0, OpStatus::Ok},
                          Case{ecOf(rpc::ErrorCode::InvalidTxnState), OpStatus::BrokerError},
                          Case{ecOf(rpc::ErrorCode::ProducerFenced), OpStatus::Fenced},
                          Case{ecOf(rpc::ErrorCode::InvalidProducerEpoch), OpStatus::Fenced}}) {
        SCOPED_TRACE(cs.terminal);
        TxnRig r;
        r.acquire();
        r.startTxn();
        auto op = r.commit();
        r.doWork();
        const int64_t corr = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
        for (const int16_t ec : {ecOf(rpc::ErrorCode::ConcurrentTransactions),
                                 ecOf(rpc::ErrorCode::MinIsrViolation), ecOf(rpc::ErrorCode::Internal),
                                 ecOf(rpc::ErrorCode::ConcurrentTransactions)}) {
            auto resp = makeEndTxnResponse(corr, ec);
            r.impl->onEndTxnResponse(resp.data(), resp.size());
            ASSERT_FALSE(op.ready()) << "EndTxn completed on retriable code " << ec;
            int waited = 0;
            while (r.cycles(1, rpc::TemplateId::EndTxn) == 0) ASSERT_LE(++waited, 64);
        }
        auto fin = makeEndTxnResponse(corr, cs.terminal);
        r.impl->onEndTxnResponse(fin.data(), fin.size());
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), cs.expect);
        if (cs.expect == OpStatus::BrokerError) EXPECT_EQ(op.c().errorCode, static_cast<uint16_t>(cs.terminal));
        EXPECT_EQ(r.cycles(64, rpc::TemplateId::EndTxn), 0);
    }
}

// ===========================================================================
// F14 — AddPartitions errors are no longer ignored
// ===========================================================================

// A non-retriable AddPartitions error: the partition's records are NEVER
// produced, later records for it are dropped, and a commit fails
// (BrokerError with that code) instead of writing a marker over them; other
// partitions are unaffected.  Fix round 1 (C-1, SPEC CORRECTION — the old
// tail ran a new transaction straight after the failed commit, which commits
// the orphaned record on the broker): the failed transaction must be aborted
// (EndTxn(abort), once the in-flight record settled) before the next begins.
TEST(TransactionalProducerTest, AddPartitionsErrorGatesPartitionDataRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    const TopicHandle bad{9, 1};
    const TopicHandle good{5, 1};
    r.tp.send(bad, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.tp.send(good, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 2);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 0) << "data before AddPartitions OK";
    int64_t badCorr = 0, goodCorr = 0;
    for (const auto& f : r.sink.frames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::AddPartitionsToTxn)) continue;
        auto ap = rpc::AddPartitionsToTxn::decode(f.data(), f.size());
        ASSERT_TRUE(ap.has_value());
        (ap->partitions[0].topicId == bad.topicId ? badCorr : goodCorr) = ap->correlationId;
    }
    auto okResp = makeAddPartitionsToTxnResponse(goodCorr);
    r.impl->onAddPartitionsToTxnResponse(okResp.data(), okResp.size());
    r.doWork();
    const auto ids = collectProduceCorrIds(r.sink);
    ASSERT_EQ(ids.size(), 1u) << "only the accepted partition's record";
    auto pv = rune::client::codec::decodeProduce(r.sink.last(rpc::TemplateId::Produce).data(),
                                                 r.sink.last(rpc::TemplateId::Produce).size());
    ASSERT_TRUE(pv.has_value());
    EXPECT_EQ(pv->topicId, good.topicId);
    auto refused = makeAddPartitionsToTxnResponse(badCorr, ecOf(rpc::ErrorCode::UnknownTopic));
    r.impl->onAddPartitionsToTxnResponse(refused.data(), refused.size());
    EXPECT_EQ(r.tp.state(), TxnState::AbortableError);
    r.sink.clear();
    r.tp.send(bad, Key(uint64_t{0}), std::span<const uint8_t>(kVal));  // refused
    auto commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(commit.c().errorCode, static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic));
    for (int i = 0; i < 4; ++i) r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 0);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "a commit marker over a refused partition";
    EXPECT_EQ(r.impl->testWaitingRecords(), 0u);

    // Abort it: EndTxn(abort) once the in-flight record settled.
    auto abort = r.abort();
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "the in-flight batch has not settled";
    auto ack = makeProduceResponse(ids[0], 0, 0);
    r.impl->onProduceResponse(ack.data(), ack.size());
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, uint8_t{0});
    auto okAbort = makeEndTxnResponse(corrIdOf(et), 0);
    r.impl->onEndTxnResponse(okAbort.data(), okAbort.size());
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);

    // The next transaction starts clean: the partition is added again.
    r.sink.clear();
    r.tp.beginTransaction();
    r.tp.send(bad, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 1);
    r.addPartitionsOk();
    EXPECT_EQ(r.cycles(1, rpc::TemplateId::Produce), 1);
}

// ===========================================================================
// F14 — NotCoordinator: re-discover, then retry
// ===========================================================================

// NotCoordinator never just re-sends into the same (loading / moved)
// coordinator: it asks the Client to re-discover (once per generation, however
// many requests wait), waits for the re-discovery, then backs off and re-sends.
// For every request type.
TEST(TransactionalProducerTest, NotCoordinatorRediscoversThenRetriesRegression) {
    TxnRig r;
    const int16_t nc = ecOf(rpc::ErrorCode::NotCoordinator);
    // InitProducerId
    auto init = r.init();
    r.doWork();
    const int64_t ic = corrIdOf(r.sink.last(rpc::TemplateId::InitProducerId));
    auto ncInit = makeInitProducerIdResponse(ic, -1, 0, nc);
    r.impl->onInitProducerIdResponse(ncInit.data(), ncInit.size());
    EXPECT_EQ(r.rediscovers, 1);
    EXPECT_EQ(r.cycles(2'048, rpc::TemplateId::InitProducerId), 0) << "re-sent before the re-discovery";
    r.impl->onCoordinatorRediscovered();
    ASSERT_EQ(r.cycles(2, rpc::TemplateId::InitProducerId), 1) << "not re-sent after the re-discovery";
    auto ok = makeInitProducerIdResponse(ic, 5, 0);
    r.impl->onInitProducerIdResponse(ok.data(), ok.size());
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::Ok);

    // AddPartitions + AddOffsets wait on ONE re-discovery.
    r.tp.beginTransaction();
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    auto offs = r.sendOffsets({{{42, 0}, 1}}, "g");
    r.doWork();
    const int64_t apc = corrIdOf(r.sink.last(rpc::TemplateId::AddPartitionsToTxn));
    const int64_t aoc = corrIdOf(r.sink.last(rpc::TemplateId::AddOffsetsToTxn));
    auto ncAp = makeAddPartitionsToTxnResponse(apc, nc);
    auto ncAo = makeAddOffsetsToTxnResponse(aoc, nc);
    r.impl->onAddPartitionsToTxnResponse(ncAp.data(), ncAp.size());
    r.impl->onAddOffsetsToTxnResponse(ncAo.data(), ncAo.size());
    EXPECT_EQ(r.rediscovers, 2) << "one re-discovery request per generation";
    r.sink.clear();
    EXPECT_EQ(r.cycles(64, rpc::TemplateId::AddPartitionsToTxn), 0);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddOffsetsToTxn), 0);
    r.impl->onCoordinatorRediscovered();
    r.cycles(2, rpc::TemplateId::AddPartitionsToTxn);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 1);
    EXPECT_EQ(r.sink.count(rpc::TemplateId::AddOffsetsToTxn), 1);
    r.addPartitionsOk();
    auto aoOk = makeAddOffsetsToTxnResponse(aoc);
    r.impl->onAddOffsetsToTxnResponse(aoOk.data(), aoOk.size());
    r.doWork();
    // TxnOffsetCommit
    const int64_t occ = corrIdOf(r.sink.last(rpc::TemplateId::TxnOffsetCommit));
    ASSERT_NE(occ, 0);
    auto ncOc = makeTxnOffsetCommitResponse(occ, nc);
    r.impl->onTxnOffsetCommitResponse(ncOc.data(), ncOc.size());
    EXPECT_EQ(r.rediscovers, 3);
    EXPECT_EQ(r.cycles(64, rpc::TemplateId::TxnOffsetCommit), 0);
    r.impl->onCoordinatorRediscovered();
    ASSERT_EQ(r.cycles(4, rpc::TemplateId::TxnOffsetCommit), 1);
    auto ocOk = makeTxnOffsetCommitResponse(occ);
    r.impl->onTxnOffsetCommitResponse(ocOk.data(), ocOk.size());
    ASSERT_TRUE(offs.ready());
    EXPECT_EQ(offs.status(), OpStatus::Ok);
    // Ack the record, then EndTxn hits a loading coordinator.
    for (int64_t id : collectProduceCorrIds(r.sink)) {
        auto a = makeProduceResponse(id, 0, 0);
        r.impl->onProduceResponse(a.data(), a.size());
    }
    auto commit = r.commit();
    r.doWork();
    const int64_t ec = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
    ASSERT_NE(ec, 0);
    auto ncEt = makeEndTxnResponse(ec, nc);
    r.impl->onEndTxnResponse(ncEt.data(), ncEt.size());
    EXPECT_EQ(r.rediscovers, 4);
    EXPECT_EQ(r.cycles(64, rpc::TemplateId::EndTxn), 0);
    r.impl->onCoordinatorRediscovered();
    ASSERT_EQ(r.cycles(8, rpc::TemplateId::EndTxn), 1);
    auto etOk = makeEndTxnResponse(ec, 0);
    r.impl->onEndTxnResponse(etOk.data(), etOk.size());
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// A request in flight when the session changes gets no answer on the old
// one: it is re-sent, re-stamped, on the new session (same correlationId).
TEST(TransactionalProducerTest, InFlightRequestIsResentOnANewSessionRegression) {
    TxnRig r;
    auto op = r.init();
    r.doWork();
    const auto first = r.sink.last(rpc::TemplateId::InitProducerId);
    r.sink.clear();
    r.impl->doWork(r.now, kSessionId + 1, r.sink.fn());
    const auto again = r.sink.last(rpc::TemplateId::InitProducerId);
    ASSERT_FALSE(again.empty()) << "not re-sent on the new session";
    auto h = rpc::MessageHeader::decode(again.data(), again.size());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->sessionId, kSessionId + 1);
    EXPECT_EQ(h->correlationId, corrIdOf(first));
    auto ok = makeInitProducerIdResponse(h->correlationId, 1, 0);
    r.impl->onInitProducerIdResponse(ok.data(), ok.size());
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
}

// ===========================================================================
// Fix round 3
// ===========================================================================

// B-4: abort Ok -> the KIP-360 bump in flight -> the op's deadline -> the app
// retries abort: ONLY the bump (InitProducerId) is re-sent — never EndTxn
// under the stale epoch (which the coordinator, already at epoch+1 if the
// first bump landed, would answer InvalidProducerEpoch -> fatal) — and the
// producer reaches Ready under the bumped epoch.
TEST(TransactionalProducerTest, InDoubtBumpRetryResendsOnlyTheBumpRegression) {
    ProducerConfig cfg;
    cfg.produceTimeoutMs = 20;
    TxnRig r("b4-txn-id", cfg);
    r.acquire(42, 0);
    r.startTxn(kTopic);
    r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    r.now += 40'000'000;
    r.doWork();  // the batch is lost: a sequence gap
    ASSERT_EQ(r.tp.state(), TxnState::AbortableError);
    auto abort = r.abort();
    r.doWork();
    auto okAbort = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)), 0);
    r.impl->onEndTxnResponse(okAbort.data(), okAbort.size());
    r.doWork();
    const auto firstBump = r.sink.last(rpc::TemplateId::InitProducerId);
    ASSERT_FALSE(firstBump.empty());
    // The bump lands on the broker but its answer is lost: the op expires.
    r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
    r.doWork();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Timeout);
    EXPECT_EQ(r.tp.state(), TxnState::AbortingTransaction) << "in doubt";
    r.sink.clear();
    auto retry = r.abort();
    ASSERT_EQ(retry.r, OpResult::Accepted);
    for (int i = 0; i < 4; ++i) r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "EndTxn re-sent under the stale epoch";
    ASSERT_EQ(r.sink.count(rpc::TemplateId::InitProducerId), 1) << "the bump was not re-sent";
    // A late answer to the FIRST bump is ignored (its request is gone).
    auto lateFirst = makeInitProducerIdResponse(corrIdOf(firstBump), 42, 1);
    r.impl->onInitProducerIdResponse(lateFirst.data(), lateFirst.size());
    EXPECT_FALSE(retry.ready());
    r.bumpOk(42, 2);  // the coordinator bumped again
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready) << "never Fatal";
    EXPECT_EQ(r.tp.producerEpoch(), 2);
}

// m15: an AddPartitions SENT but unanswered past its deadline may have opened
// the transaction on the coordinator.  The abort then SENDS EndTxn(abort):
// (a) the broker had it open -> 0 -> aborted, Ready;
// (b) the broker had nothing -> 19 -> in this ambiguous case only, the abort
//     completes Ok (not fatal), Ready.
TEST(TransactionalProducerTest, TimedOutAddPartitionsAbortsPossiblyStartedTxnRegression) {
    for (const int16_t answer : {int16_t{0}, ecOf(rpc::ErrorCode::InvalidTxnState)}) {
        SCOPED_TRACE(answer);
        TxnRig r;
        r.acquire();
        r.tp.beginTransaction();
        r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
        r.doWork();  // AddPartitions sent, never answered
        ASSERT_EQ(r.sink.count(rpc::TemplateId::AddPartitionsToTxn), 1);
        r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
        r.doWork();
        ASSERT_EQ(r.tp.state(), TxnState::AbortableError);
        r.sink.clear();
        auto abort = r.abort();
        r.doWork();
        const auto et = r.sink.last(rpc::TemplateId::EndTxn);
        ASSERT_FALSE(et.empty()) << "a possibly-started transaction must be aborted on the broker";
        EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, uint8_t{0});
        auto resp = makeEndTxnResponse(corrIdOf(et), answer);
        r.impl->onEndTxnResponse(resp.data(), resp.size());
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
        EXPECT_EQ(r.tp.state(), TxnState::Ready) << "19 here means nothing was open: not fatal";
    }
    // Outside the ambiguous case 19 on EndTxn stays fatal.
    TxnRig r;
    r.acquire();
    r.startTxn();
    auto abort = r.abort();
    r.doWork();
    auto bad = makeEndTxnResponse(corrIdOf(r.sink.last(rpc::TemplateId::EndTxn)),
                                  ecOf(rpc::ErrorCode::InvalidTxnState));
    r.impl->onEndTxnResponse(bad.data(), bad.size());
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::BrokerError);
    EXPECT_EQ(r.tp.state(), TxnState::FatalError);
}

// m15 (AddOffsets): a sendOffsets whose AddOffsetsToTxn was sent but timed
// out makes the transaction possibly started: the abort sends EndTxn(abort).
TEST(TransactionalProducerTest, TimedOutAddOffsetsAbortsPossiblyStartedTxnRegression) {
    TxnRig r;
    r.acquire();
    r.tp.beginTransaction();
    auto offs = r.sendOffsets({{{42, 0}, 1}}, "g");
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::AddOffsetsToTxn), 1);  // never answered
    r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
    r.doWork();
    ASSERT_TRUE(offs.ready());
    EXPECT_EQ(offs.status(), OpStatus::Timeout);
    r.sink.clear();
    auto abort = r.abort();
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    auto none = makeEndTxnResponse(corrIdOf(et), ecOf(rpc::ErrorCode::InvalidTxnState));
    r.impl->onEndTxnResponse(none.data(), none.size());
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
}

// m12: the ORIGINAL in-doubt EndTxn's late answer, arriving after the app
// retried, completes nothing (its request is gone): only the retry's answer
// completes, and exactly one Ok is delivered.
TEST(TransactionalProducerTest, LateOriginalEndTxnAnswerAfterRetryIsIgnoredRegression) {
    TxnRig r;
    r.acquire();
    r.startTxn();
    auto first = r.commit();
    r.doWork();
    const int64_t original = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
    r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
    r.doWork();
    ASSERT_TRUE(first.ready());
    EXPECT_EQ(first.status(), OpStatus::Timeout);
    auto retry = r.commit();
    r.doWork();
    const int64_t again = corrIdOf(r.sink.last(rpc::TemplateId::EndTxn));
    ASSERT_NE(again, original);
    auto late = makeEndTxnResponse(original, 0);
    r.impl->onEndTxnResponse(late.data(), late.size());
    EXPECT_FALSE(retry.ready()) << "completed by the stale answer";
    auto ok = makeEndTxnResponse(again, 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    r.impl->onEndTxnResponse(late.data(), late.size());
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.status(), OpStatus::Ok);
    EXPECT_EQ(retry.times(), 1);
    EXPECT_EQ(first.times(), 1);
    EXPECT_EQ(r.tp.state(), TxnState::Ready);
}

// ===========================================================================
// F8c C7 — TransactionalProducer close() / closeImmediately() (user decision 2)
// ===========================================================================

// close() aborts an open broker transaction before it reports closed: EndTxn
// (abort) is sent; the token completes only after its answer (Ok); the
// producer is closed afterwards (later ops rejected Closed, the status
// channel finished).  RED (mutation): finishing the close at once (skipping
// the abort in closeStep_) sends no EndTxn and completes the token before it.
TEST(TransactionalProducerTest, TxnCloseAbortsOpenBrokerTransactionBeforeReportingClosedRegression) {
    TxnRig r;
    r.acquire(77, 0);
    r.startTxn();  // started: AddPartitions acknowledged, its record acked
    ASSERT_TRUE(r.tp.transactionActive());
    auto cl = r.close();
    ASSERT_EQ(cl.r, OpResult::Accepted);
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty()) << "close() must end the open transaction";
    EXPECT_EQ(rpc::EndTxn::decode(et.data(), et.size())->commit, uint8_t{0}) << "with EndTxn(abort)";
    EXPECT_FALSE(cl.ready()) << "not closed before the abort's answer";
    EXPECT_FALSE(r.impl->closedDuty());
    auto ok = makeEndTxnResponse(corrIdOf(et), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    r.doWork();
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
    EXPECT_EQ(cl.c().op, OpKind::TxnClose);
    EXPECT_TRUE(r.impl->closedDuty());
    EXPECT_EQ(r.tp.commitTransaction(r.handler(), 99), OpResult::Closed) << "rejected after close";
    EXPECT_EQ(r.cycles(16, rpc::TemplateId::EndTxn), 0) << "nothing sent after close";
}

// close() with nothing open on the broker (Ready; a transaction that never
// started; Uninitialized) closes at once: Ok, no EndTxn.  A second close
// completes Closed exactly once.
TEST(TransactionalProducerTest, TxnCloseWithNothingOpenClosesAtOnceRegression) {
    {
        TxnRig r;  // Uninitialized
        auto cl = r.close();
        ASSERT_TRUE(cl.ready());
        EXPECT_EQ(cl.status(), OpStatus::Ok);
        EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0);
    }
    {
        TxnRig r;
        r.acquire();
        r.tp.beginTransaction();  // begun, nothing added: ends locally
        auto cl = r.close();
        r.doWork();
        ASSERT_TRUE(cl.ready());
        EXPECT_EQ(cl.status(), OpStatus::Ok);
        EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "never started: no EndTxn";
        auto again = r.impl.get();
        rune::client::detail::HubCommand c2;  // a second close reaching the duty side
        c2.op = rune::client::detail::HubCommand::Op::TxnClose;
        c2.a  = 555;
        rune::client::detail::OpTarget t;
        t.handler = r.handler();
        rune::client::detail::encodeTarget(t, c2);
        again->applyCommand(c2, r.now);
        ASSERT_EQ(r.ledger.e.count(555), 1u);
        EXPECT_EQ(r.ledger.e.at(555).c.status, OpStatus::Closed);
        EXPECT_EQ(r.ledger.e.at(555).times, 1);
    }
}

// close() never aborts a commit already running (Kafka: COMMITTING ->
// ABORTING is invalid): it waits for the commit's answer, then closes Ok —
// the commit completes Ok on its own.
TEST(TransactionalProducerTest, TxnCloseWaitsForARunningCommitRegression) {
    TxnRig r;
    r.acquire();
    r.startTxn();
    auto commit = r.commit();
    r.doWork();
    const auto et = r.sink.last(rpc::TemplateId::EndTxn);
    ASSERT_FALSE(et.empty());
    auto cl = r.close();
    r.doWork();
    EXPECT_FALSE(cl.ready());
    EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1) << "no EndTxn(abort) over a running commit";
    auto ok = makeEndTxnResponse(corrIdOf(et), 0);
    r.impl->onEndTxnResponse(ok.data(), ok.size());
    r.doWork();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
    EXPECT_TRUE(r.impl->closedDuty());
}

// close() reports a failed abort (EndTxn never answered: Timeout at
// transactionOpTimeoutMs) and the producer is closed anyway.
TEST(TransactionalProducerTest, TxnCloseReportsAFailedAbortAndClosesAnywayRegression) {
    TxnRig r;
    r.acquire();
    r.startTxn();
    auto cl = r.close();
    r.doWork();
    ASSERT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 1);  // never answered
    r.now += ProducerConfig{}.transactionOpTimeoutMs * 1'000'000LL;
    r.doWork();
    r.doWork();
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Timeout);
    EXPECT_EQ(cl.times(), 1);
    EXPECT_TRUE(r.impl->closedDuty());
}

// closeImmediately() discards locally: no EndTxn, every pending op (and a
// pending close token) completes Closed exactly once; the producer is closed.
// The handle's destructor does the same (here: the rig's sink maps the
// HandleRelease to the duty side's applyHandleRelease, as the Client does).
TEST(TransactionalProducerTest, TxnCloseImmediatelyDiscardsLocallyRegression) {
    {
        TxnRig r;
        r.acquire();
        r.startTxn();
        auto offs = r.sendOffsets({{{5, 0}, 3}}, "g");
        auto cl   = r.close();  // the abort is in flight
        r.doWork();
        const int endTxnsBefore = r.sink.count(rpc::TemplateId::EndTxn);
        EXPECT_EQ(r.tp.closeImmediately(), OpResult::Accepted);
        for (auto* op : {&offs, &cl}) {
            ASSERT_TRUE(op->ready());
            EXPECT_EQ(op->status(), OpStatus::Closed);
            EXPECT_EQ(op->times(), 1);
        }
        EXPECT_TRUE(r.impl->closedDuty());
        EXPECT_EQ(r.cycles(16, rpc::TemplateId::EndTxn), 0) << "nothing more sent";
        EXPECT_LE(endTxnsBefore, 1);
        EXPECT_EQ(r.tp.closeImmediately(), OpResult::Closed) << "idempotent; already closed";
    }
    {
        TxnRig r;
        r.acquire();
        r.startTxn();
        auto offs = r.sendOffsets({{{5, 0}, 3}}, "g");
        r.impl->applyHandleRelease();  // the destructor's duty side (the Client's HandleRelease)
        ASSERT_TRUE(offs.ready());
        EXPECT_EQ(offs.status(), OpStatus::Closed);
        EXPECT_TRUE(r.impl->closedDuty());
        EXPECT_EQ(r.sink.count(rpc::TemplateId::EndTxn), 0) << "the destructor never sends EndTxn";
    }
}

// ===========================================================================
// EOS follow-ups fix round 1 (eos-followups-review.md I-1 / M-2 / I-2)
// ===========================================================================
namespace {
std::vector<std::vector<uint8_t>> produceFrames(const CaptureSink& sink) {
    std::vector<std::vector<uint8_t>> out;
    for (const auto& f : sink.frames) {
        if (peekTemplate(f) == static_cast<uint16_t>(rpc::TemplateId::Produce)) out.push_back(f);
    }
    return out;
}
void answer(TxnRig& r, int64_t corr, int16_t ec, int64_t off = 0) {
    auto a = makeProduceResponse(corr, ec, off);
    r.impl->onProduceResponse(a.data(), a.size());
}
// begin + n records on one partition, AddPartitions OK, the data sent.
std::vector<std::vector<uint8_t>> sendData(TxnRig& r, int n, TopicHandle topic = TopicHandle{1, 1}) {
    r.tp.beginTransaction();
    for (int i = 0; i < n; ++i) r.tp.send(topic, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.doWork();
    r.addPartitionsOk();
    r.sink.clear();
    r.doWork();
    return produceFrames(r.sink);
}
constexpr int16_t kTxnOos = static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence);
} // namespace

// I-1 (Kafka parity, ruling 3): OutOfOrderSequence on a batch BEHIND an
// unresolved older batch means an earlier frame was lost — retried once the
// older batch resolves, never a poisoned (abortable) transaction.  RED
// (pre-fix): the first such OOS poisons the transaction (AbortableError).
TEST(TransactionalProducerTest, OutOfOrderBehindAnOlderBatchIsRetriedNotPoisonedRegression) {
    TxnRig r;
    r.acquire(77, 0);
    const auto first = sendData(r, 3);
    ASSERT_EQ(first.size(), 3u);
    answer(r, corrIdOf(first[1]), kTxnOos);  // seq 0 was lost: seq 1 and 2 meet the gap
    answer(r, corrIdOf(first[2]), kTxnOos);
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction) << "an OOS behind a live older batch is retried, not poisoned";
    r.sink.clear();
    r.doWork();
    // Fast retransmit of the gap frame (seq 0) only; seq 1 and 2 wait for it.
    auto again = produceFrames(r.sink);
    ASSERT_EQ(again.size(), 1u);
    EXPECT_EQ(decodeBaseSeqFromProduce(again[0]), 0);
    answer(r, corrIdOf(again[0]), 0, 100);
    r.sink.clear();
    r.doWork();  // the parked batches go out again, in sequence order
    again = produceFrames(r.sink);
    ASSERT_EQ(again.size(), 2u);
    EXPECT_EQ(decodeBaseSeqFromProduce(again[0]), 1);
    EXPECT_EQ(decodeBaseSeqFromProduce(again[1]), 2);
    answer(r, corrIdOf(again[0]), 0, 101);
    answer(r, corrIdOf(again[1]), 0, 102);
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction);
    EXPECT_EQ(r.impl->testInFlightBatches(), 0u);
}

// M-2 (ruling 1): the transactional producer's in-flight batches per partition
// are clamped to kIdempotentMaxInFlightBatches (the broker's dedup window); the
// rest wait.  RED (pre-fix): all 40 went out at once.
TEST(TransactionalProducerTest, InFlightPerPartitionClampedToTheBrokerWindowRegression) {
    TxnRig r;
    r.acquire(77, 0);
    const auto first = sendData(r, 40);
    constexpr std::size_t kWindow = static_cast<std::size_t>(rpc::kIdempotentMaxInFlightBatches);
    ASSERT_EQ(first.size(), kWindow) << "at most the broker's window in flight per partition";
    EXPECT_EQ(r.impl->testInFlightBatches(), kWindow);
    answer(r, corrIdOf(first[0]), 0, 100);
    r.sink.clear();
    r.doWork();
    const auto more = produceFrames(r.sink);
    ASSERT_EQ(more.size(), 1u) << "one acked -> one more";
    EXPECT_EQ(decodeBaseSeqFromProduce(more[0]), static_cast<int32_t>(kWindow));
}

// I-2: an error answer of a SUPERSEDED attempt never decides the batch.  The
// batch is retried after a retriable error; the first attempt's late OOS (an
// old route) arrives while the retry may be stored — it is ignored, and the
// retry's OK completes the batch.  RED (pre-fix: one correlationId for every
// attempt): the late OOS poisons the transaction.
TEST(TransactionalProducerTest, ErrorOfASupersededAttemptIsIgnoredRegression) {
    TxnRig r;
    r.acquire(77, 0);
    const auto first = sendData(r, 1);
    ASSERT_EQ(first.size(), 1u);
    answer(r, corrIdOf(first[0]), ecOf(rpc::ErrorCode::NotLeader));  // retriable
    r.impl->onCoordinatorRediscovered();  // NEW-3: re-offered after fresh metadata
    r.sink.clear();
    r.doWork();
    const auto retry = produceFrames(r.sink);
    ASSERT_EQ(retry.size(), 1u);
    answer(r, corrIdOf(first[0]), kTxnOos);  // the FIRST attempt's late answer
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction) << "a superseded attempt's error is ignored";
    answer(r, corrIdOf(retry[0]), 0, 100);
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction);
    EXPECT_EQ(r.impl->testInFlightBatches(), 0u);
}

// Sequence overflow (broker-agent concern): the per-partition int32 sequence
// never overflows — the client chooses not to wrap (the broker accepts the Kafka
// wrap since 37564ba).  The txn producer's epoch belongs to the coordinator, so before the
// counter would pass INT32_MAX the transaction becomes abortable with an epoch
// bump (InitProducerId restarts every sequence at 0); no frame ever carries a
// wrapped (negative) sequence.  RED (pre-fix): the third record went out at a
// negative baseSequence (signed overflow) and the transaction stayed live.
TEST(TransactionalProducerTest, SequenceCounterNeverOverflowsInt32Regression) {
    TxnRig r;
    r.acquire(77, 0);
    const auto first = sendData(r, 1);
    ASSERT_EQ(first.size(), 1u);
    answer(r, corrIdOf(first[0]), 0, 100);
    r.impl->testSetNextSequenceAll(INT32_MAX - 1);
    for (int i = 0; i < 3; ++i) r.tp.send(TopicHandle{1, 1}, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
    r.sink.clear();
    r.doWork();
    const auto frames = produceFrames(r.sink);
    for (const auto& f : frames) EXPECT_GE(decodeBaseSeqFromProduce(f), 0) << "a wrapped sequence went out";
    ASSERT_EQ(frames.size(), 1u) << "only INT32_MAX - 1 fits (the next counter value must stay <= INT32_MAX)";
    EXPECT_EQ(decodeBaseSeqFromProduce(frames[0]), INT32_MAX - 1);
    EXPECT_EQ(r.tp.state(), TxnState::AbortableError) << "abortable with an epoch bump before the overflow";
}

// Re-review 1 NEW-3: ProduceResponse{NotLeader} is a routine partition move,
// never a reason to abort the transaction (Kafka NOT_LEADER_OR_FOLLOWER:
// refresh metadata, retry).  The batch parks — not re-sent to the old owner,
// no retry budget spent, nothing new produced on the partition — until a
// MetadataResponse arrived (the Client asks for one on every NotLeader); then
// it is re-offered (on the partition's new route, via the send function) with
// the SAME frame identity (producerId, epoch, baseSequence) as a NEW attempt
// (fresh correlationId).  RED (pre-fix): re-sent at once to the old owner and,
// after kMaxRetries NotLeaders, the transaction became abortable.
TEST(TransactionalProducerTest, NotLeaderParksUntilMetadataThenReoffersSameIdentityRegression) {
    TxnRig r;
    r.acquire(77, 3);
    auto cur = sendData(r, 2);
    ASSERT_EQ(cur.size(), 2u);
    const auto first = cur;
    for (int round = 0; round < 8; ++round) {
        SCOPED_TRACE(round);
        for (const auto& f : cur) answer(r, corrIdOf(f), ecOf(rpc::ErrorCode::NotLeader));
        if (round == 0) r.tp.send(TopicHandle{1, 1}, Key(uint64_t{0}), std::span<const uint8_t>(kVal));
        r.sink.clear();
        for (int i = 0; i < 5; ++i) r.doWork();
        EXPECT_TRUE(produceFrames(r.sink).empty()) << "re-sent (or a new batch sent) before fresh metadata";
        ASSERT_EQ(r.tp.state(), TxnState::InTransaction) << "a partition move aborted the transaction";
        r.impl->onCoordinatorRediscovered();  // the MetadataResponse arrived (new route installed)
        r.doWork();
        cur = produceFrames(r.sink);
        ASSERT_EQ(cur.size(), 2u) << "both batches re-offered, nothing new before them";
        for (std::size_t i = 0; i < cur.size(); ++i) {
            const auto pv = rune::client::codec::decodeProduce(cur[i].data(), cur[i].size());
            ASSERT_TRUE(pv);
            EXPECT_EQ(pv->producerId, 77);
            EXPECT_EQ(pv->producerEpoch, 3);
            EXPECT_EQ(pv->baseSequence, static_cast<int32_t>(i)) << "same identity, in order";
            EXPECT_EQ(decodeBaseSeqFromProduce(cur[i]), static_cast<int32_t>(i));
            EXPECT_NE(corrIdOf(cur[i]), corrIdOf(first[i])) << "a new attempt";
        }
    }
    for (const auto& f : cur) answer(r, corrIdOf(f), 0, 100);
    r.sink.clear();
    r.doWork();  // the record held while parked goes out now
    const auto held = produceFrames(r.sink);
    ASSERT_EQ(held.size(), 1u);
    EXPECT_EQ(decodeBaseSeqFromProduce(held[0]), 2);
    answer(r, corrIdOf(held[0]), 0, 102);
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction);
    EXPECT_EQ(r.impl->testInFlightBatches(), 0u);
}

// Re-review 1 NEW-4(a): DuplicateSequence (Kafka DUPLICATE_SEQUENCE_NUMBER) is a
// stored batch — the transaction neither retries nor aborts.  RED (pre-fix):
// re-sent as a generic retriable error, then abortable after kMaxRetries.
TEST(TransactionalProducerTest, DuplicateSequenceIsAStoredBatchRegression) {
    TxnRig r;
    r.acquire(77, 0);
    const auto first = sendData(r, 1);
    ASSERT_EQ(first.size(), 1u);
    answer(r, corrIdOf(first[0]), ecOf(rpc::ErrorCode::DuplicateSequence));
    EXPECT_EQ(r.impl->testInFlightBatches(), 0u) << "a DuplicateSequence batch is stored";
    r.sink.clear();
    r.doWork();
    EXPECT_TRUE(produceFrames(r.sink).empty()) << "a stored batch was re-sent";
    EXPECT_EQ(r.tp.state(), TxnState::InTransaction);
}

// Backlog B6: a transactional record (key + value) over Producer::kMaxRecordBytes
// is rejected at append (std::length_error), never sent.
TEST(TransactionalProducerTest, RecordOver16KiBIsRejectedAtAppend) {
    TxnRig r;
    r.acquire();
    r.startTxn();
    const std::vector<uint8_t> over(rune::client::Producer::kMaxRecordBytes - 7, 0x09);  // + 8-byte key
    EXPECT_THROW(r.tp.send(kTopic, Key(uint64_t{0}), std::span<const uint8_t>(over)), std::length_error);
    for (int i = 0; i < 8; ++i) r.doWork();
    EXPECT_EQ(r.sink.count(rpc::TemplateId::Produce), 0);
}
