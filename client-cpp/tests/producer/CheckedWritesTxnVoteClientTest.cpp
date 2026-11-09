// tests/producer/CheckedWritesTxnVoteClientTest.cpp - Part 11 Task 11: the client side of the transaction vote.  A no vote
// on EndTxn(commit) completes the commit with the vote's code, constraint id and offset (and commitTransactionAndWait
// returns it); intents are sent inside a transaction; Tier-2 batches carry kAttrAtomic and an atomic batch's one trailer
// entry is its one completion.
//
// A bare ProducerImpl behind a Producer handle, wired to a FakeCoordinator through a ProducerHub; the test thread is the
// duty thread and the clock is injected (no sleeps, no threads).
#include "../support/FakeCoordinator.h"

#include <rune/client/Completion.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/CoordinatorLink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace rc  = rune::client;
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
namespace rb  = rune::protocol::batch;
using rc::Acks;
using rc::OpStatus;
using rc::Token;
using rc::TxnState;
using rp::ProducerImpl;
using rp::TxnControl;
using test_support::AnswerFrame;
using test_support::FakeCoordinator;
using test_support::OfferedFrame;

namespace {

constexpr int64_t  kMs         = 1'000'000;
constexpr int64_t  kT0         = 1'000'000'000;
constexpr int32_t  kTopic      = 1;
constexpr uint16_t kNoVote     = 55;
constexpr uint32_t kConstraint = 0x00070000;
constexpr int64_t  kFailing    = 123;

rc::ProducerConfig cfgOf(Acks acks, bool transactional) {
    rc::ProducerConfig c;
    c.acks                   = acks;
    c.lingerUs               = 0;
    c.batchBytes             = 64 * 1024;
    c.maxInFlight            = 5;
    c.produceTimeoutMs       = 10'000;
    c.deliveryTimeoutMs      = 20'000;
    c.transactionOpTimeoutMs = 1'000;
    c.transactionTimeoutMs   = 30'000;
    if (transactional) c.transactionalId = "txn-vote";
    return c;
}

struct OpLedger {
    struct Entry { int times = 0; rc::Completion c{}; };
    std::map<Token, Entry> e;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto& x = static_cast<OpLedger*>(ctx)->e[c.token];
        ++x.times;
        x.c = c;
    }
};
struct Op {
    OpLedger* l = nullptr;
    Token     t = 0;
    [[nodiscard]] bool ready() const { return l->e.count(t) != 0; }
    [[nodiscard]] const rc::Completion& c() const { return l->e.at(t).c; }
};

struct WireBatch {
    int64_t              corrId = 0;
    int16_t              attrs  = 0;
    uint8_t              acks   = 0;
    std::vector<uint8_t> recordAttrs;
};

/// Tier-2 batch completions per token (the onAck full-handler form).
struct AckCapture {
    std::map<Token, rc::Completion> done;
    std::map<Token, int>            times;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<AckCapture*>(ctx);
        self->done[c.token] = c;
        ++self->times[c.token];
    }
};

struct Rig {
    FakeCoordinator        fake;
    rp::ProducerHub        hub;
    ProducerImpl*          impl;
    rc::Producer           p;
    OpLedger               ops;
    AckCapture             acks;
    std::vector<WireBatch> wire;
    ProducerImpl::SendFn   sink;
    AnswerFrame            endAnswerFrame;   // what the stub wait answers EndTxn with
    int64_t                now  = kT0;
    Token                  next = 0;
    rc::ProducerConfig     cfg;

    explicit Rig(rc::ProducerConfig c) : impl(new ProducerImpl(c, 1 << 16)), p(impl), cfg(std::move(c)) {
        hub.setCoordinatorSink(fake.sink());
        impl->setOnAck(&AckCapture::on, &acks);
        ProducerImpl::Hooks h;
        h.ctx     = this;
        h.andWait = &Rig::waitStub;
        impl->setHooks(h);
        hub.add(impl);
        sink = [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rc::codec::decodeProduce(b, n);
            if (!v) return false;
            WireBatch w;
            w.corrId = v->correlationId;
            w.acks   = v->acks;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            w.attrs = r.attributes();
            r.forEachRecord([&](std::string_view, std::string_view, int64_t, int64_t, uint8_t a) { w.recordAttrs.push_back(a); });
            wire.push_back(std::move(w));
            return true;
        };
        if (cfg.transactionalId.empty() && cfg.acks != Acks::None) plainIdentity();
    }
    // answers the InitProducerId the last tick offered
    void answerInit() {
        const OfferedFrame* f = last(rpc::InitProducerId::TEMPLATE_ID);
        ASSERT_NE(f, nullptr);
        rpc::InitProducerIdResponse m;
        m.correlationId = f->corrId;
        m.producerId    = 42;
        m.producerEpoch = 0;
        AnswerFrame a;
        a.len        = m.encode(a.buf.data());
        a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
        answer(a);
    }
    // an acked plain producer holds its records until the broker assigned its id
    void plainIdentity() {
        tick();
        answerInit();
    }
    ~Rig() { hub.remove(impl); }

    // ---- blocking stub: issue the op, run duty passes, answer EndTxn with endAnswerFrame ----
    static rc::Completion waitStub(void* ctx, const char*, std::chrono::nanoseconds, rc::detail::AndWaitIssue issue,
                                   void* issueCtx, std::string*) {
        auto* r = static_cast<Rig*>(ctx);
        const Token t = ++r->next;
        EXPECT_EQ(issue(issueCtx, rc::CompletionTarget(rc::CompletionHandler{&OpLedger::on, &r->ops}), t), rc::OpResult::Accepted);
        for (int i = 0; i < 8 && r->ops.e.count(t) == 0; ++i) {
            r->tick();
            if (const OfferedFrame* f = r->last(rpc::EndTxn::TEMPLATE_ID); f != nullptr && r->endsAnswered == 0) {
                ++r->endsAnswered;
                AnswerFrame a = r->endAnswerFrame;
                r->setCorr(a, f->corrId);
                r->answer(a);
            }
        }
        return r->ops.e.at(t).c;
    }
    int endsAnswered = 0;

    static void setCorr(AnswerFrame& a, int64_t corr) {
        for (int i = 0; i < 8; ++i) a.buf[16 + i] = static_cast<uint8_t>(corr >> (8 * i));   // MessageHeader.correlationId
    }

    Op apply(TxnControl::Kind k) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&OpLedger::on, &ops};
        c.token          = ++next;
        impl->applyTxnControl(c, now);
        return Op{&ops, c.token};
    }
    Op commit() { return apply(TxnControl::Kind::Commit); }
    void begin() {
        TxnControl c;
        c.kind = TxnControl::Kind::Begin;
        impl->applyTxnControl(c, now);
        impl->setTxnGate(ProducerImpl::TxnGate::Open);
    }
    void initOk() {
        Op op = apply(TxnControl::Kind::Init);
        tick();
        answerInit();
        ASSERT_TRUE(op.ready());
        ASSERT_EQ(op.c().status, OpStatus::Ok);
    }
    /// begin, and the transaction counts as started on the coordinator.
    void startTxn() {
        begin();
        impl->testMarkStarted();
    }

    int tick(int64_t advanceNs = 0) {
        now += advanceNs;
        return hub.drainAndFlush(now, 42, sink);
    }
    void answer(const AnswerFrame& f) { ASSERT_TRUE(hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now)); }

    [[nodiscard]] const OfferedFrame* last(uint16_t tid) const {
        for (std::size_t i = fake.offered.size(); i-- > 0;) if (fake.offered[i].templateId == tid) return &fake.offered[i];
        return nullptr;
    }
    [[nodiscard]] std::size_t endTxns() const {
        std::size_t n = 0;
        for (const auto& f : fake.offered) n += f.templateId == rpc::EndTxn::TEMPLATE_ID ? 1 : 0;
        return n;
    }
    void answerLastAdd(int16_t code = 0) {
        for (int i = 0; i < 3 && last(rpc::AddPartitionsToTxn::TEMPLATE_ID) == nullptr; ++i) tick();
        const OfferedFrame* f = last(rpc::AddPartitionsToTxn::TEMPLATE_ID);
        ASSERT_NE(f, nullptr);
        answer(fake.answerAdd(f->corrId, code));
    }
    [[nodiscard]] TxnState state() const { return impl->txnStateDuty(); }
};

/// An EndTxn answer (a session at client 17): `code`, with the vote trailer when `vote`.
AnswerFrame endAnswer(int16_t code, bool vote, uint32_t constraintId = 0, int64_t failingOffset = -1) {
    rpc::EndTxnResponse m;
    m.errorCode     = code;
    m.constraintId  = constraintId;
    m.failingOffset = failingOffset;
    m.hasVote       = vote;
    AnswerFrame a;
    a.len        = m.encodeFor(a.buf.data(), rpc::kClientVersionCheckedWrites);
    a.templateId = rpc::EndTxnResponse::TEMPLATE_ID;
    return a;
}
void answerEnd(Rig& r, const AnswerFrame& proto) {
    AnswerFrame a = proto;
    Rig::setCorr(a, r.last(rpc::EndTxn::TEMPLATE_ID)->corrId);
    r.answer(a);
}

/// One 8-byte verdict entry.
std::vector<uint8_t> entry(uint16_t index, uint16_t code, uint32_t constraintId) {
    std::vector<uint8_t> b{uint8_t(index), uint8_t(index >> 8), uint8_t(code), uint8_t(code >> 8)};
    for (int k = 0; k < 4; ++k) b.push_back(uint8_t(constraintId >> (8 * k)));
    return b;
}

const std::vector<uint8_t> kVal(16, 0x11);

}  // namespace

TEST(CheckedWritesTxnVoteClient, NoVoteCompletesTheCommitWithItsFields) {
    Rig r(cfgOf(Acks::All, true));
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    answerEnd(r, endAnswer(kNoVote, true, kConstraint, kFailing));
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(r.ops.e.at(commit.t).times, 1);
    EXPECT_EQ(commit.c().op, rc::OpKind::CommitTransaction);
    EXPECT_EQ(commit.c().status, OpStatus::BrokerError);
    EXPECT_EQ(commit.c().errorCode, kNoVote);
    EXPECT_EQ(commit.c().reserved0, kConstraint);
    EXPECT_EQ(commit.c().value, kFailing);
    EXPECT_EQ(r.state(), TxnState::Ready);
    r.begin();
    EXPECT_EQ(r.state(), TxnState::InTransaction) << "beginTransaction() succeeds after a no vote";
}

TEST(CheckedWritesTxnVoteClient, CommitAndWaitReturnsTheResult) {
    {
        Rig r(cfgOf(Acks::All, true));
        r.initOk();
        r.endAnswerFrame = endAnswer(kNoVote, true, kConstraint, kFailing);
        r.p.beginTransaction();
        r.tick();
        r.impl->testMarkStarted();
        rc::TxnCommitResult res;
        ASSERT_NO_THROW(res = r.p.commitTransactionAndWait());
        EXPECT_FALSE(res.ok);
        EXPECT_EQ(res.code, kNoVote);
        EXPECT_EQ(res.constraintId, kConstraint);
        EXPECT_EQ(res.failingOffset, kFailing);
        EXPECT_EQ(r.state(), TxnState::Ready);
    }
    {
        Rig r(cfgOf(Acks::All, true));
        r.initOk();
        r.endAnswerFrame = endAnswer(0, false);
        r.p.beginTransaction();
        r.tick();
        r.impl->testMarkStarted();
        const rc::TxnCommitResult res = r.p.commitTransactionAndWait();
        EXPECT_TRUE(res.ok);
        EXPECT_EQ(res.code, 0);
        EXPECT_EQ(res.failingOffset, -1);
    }
    {   // any other failure still throws
        Rig r(cfgOf(Acks::All, true));
        r.initOk();
        r.endAnswerFrame = endAnswer(static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState), false);
        r.p.beginTransaction();
        r.tick();
        r.impl->testMarkStarted();
        EXPECT_THROW((void)r.p.commitTransactionAndWait(), std::runtime_error);
    }
}

TEST(CheckedWritesTxnVoteClientRegression, NoVoteOnAResentCommitIsDefiniteNeverFatalRegression) {
    Rig r(cfgOf(Acks::All, true));
    r.initOk();
    r.startTxn();
    Op first = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);   // never answered: in doubt, the first commit may have landed
    ASSERT_TRUE(first.ready());
    ASSERT_EQ(first.c().status, OpStatus::Timeout);
    Op retry = r.commit();                         // re-sends EndTxn(commit)
    r.tick();
    ASSERT_EQ(r.endTxns(), 2u);
    answerEnd(r, endAnswer(kNoVote, true, kConstraint, kFailing));
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.c().status, OpStatus::BrokerError);
    EXPECT_EQ(retry.c().errorCode, kNoVote);
    EXPECT_EQ(retry.c().reserved0, kConstraint);
    EXPECT_EQ(retry.c().value, kFailing);
    EXPECT_EQ(r.state(), TxnState::Ready) << "the vote is definite: the transaction is aborted, not fatal";
}

TEST(CheckedWritesTxnVoteClient, EveryVoteCodeClassifiesVotedNo) {
    using rp::CoordinatorLink;
    using rp::ReqKind;
    using rp::Verdict;
    for (const rpc::ErrorCode c : {rpc::ErrorCode::ConstraintViolation, rpc::ErrorCode::KeyExists, rpc::ErrorCode::KeyNotFound,
                                   rpc::ErrorCode::TxnTooLarge, rpc::ErrorCode::TxnConflict}) {
        EXPECT_EQ(CoordinatorLink::classify(ReqKind::EndTxn, static_cast<int16_t>(c)), Verdict::VotedNo) << static_cast<int>(c);
    }
    EXPECT_EQ(CoordinatorLink::classify(ReqKind::EndTxn, static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState)), Verdict::Fatal);
    EXPECT_EQ(CoordinatorLink::classify(ReqKind::EndTxn, static_cast<int16_t>(rpc::ErrorCode::InvalidRequest)), Verdict::Reject);
}

TEST(CheckedWritesTxnVoteClient, IntentsInATransactionAreSent) {
    Rig r(cfgOf(Acks::All, true));
    r.initOk();
    r.p.beginTransaction();
    r.tick();
    const rc::TopicHandle topic{kTopic, 1, rc::TopicKind::Typed};
    ASSERT_EQ(r.p.trySend(topic, rc::Key(uint64_t{7}), kVal, rc::WriteIntent::Update), rc::SendResult::Ok);
    r.tick();
    r.answerLastAdd();
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    ASSERT_EQ(r.wire[0].recordAttrs.size(), 1u);
    EXPECT_EQ(r.wire[0].recordAttrs[0] & rune::protocol::record::kAttrIntentMask, 2);
    EXPECT_EQ(r.wire[0].acks, 1) << "All, no View";
    EXPECT_NE(r.wire[0].attrs & rb::kAttrTransactional, 0);
}

TEST(CheckedWritesTxnVoteClient, Tier2BatchCarriesTheAtomicBit) {
    {   // a plain producer
        Rig r(cfgOf(Acks::All | Acks::View, false));
        rc::BatchBuilder b(r.impl, kTopic, 0);
        b.add(kVal).add(kVal);
        ASSERT_EQ(b.send(rc::AckToken{1}), rc::OpResult::Accepted);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        EXPECT_EQ(r.wire[0].attrs, rb::kAttrAtomic);
    }
    {   // acks=None: no verdict, the broker still judges the batch as a unit
        Rig r(cfgOf(Acks::None, false));
        rc::BatchBuilder b(r.impl, kTopic, 0);
        b.add(kVal).add(kVal);
        ASSERT_EQ(b.send(rc::AckToken{1}), rc::OpResult::Accepted);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        EXPECT_EQ(r.wire[0].attrs, rb::kAttrAtomic);
    }
    {   // a transactional producer: the transaction bit and the atomic bit
        Rig r(cfgOf(Acks::All, true));
        r.initOk();
        r.p.beginTransaction();
        r.tick();
        rc::BatchBuilder b(r.impl, kTopic, 0);
        b.add(kVal).add(kVal);
        ASSERT_EQ(b.send(rc::AckToken{1}), rc::OpResult::Accepted);
        r.tick();
        r.answerLastAdd();
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        EXPECT_EQ(r.wire[0].attrs, rb::kAttrTransactional | rb::kAttrAtomic);
    }
    {   // a Tier-1 record carries neither
        Rig r(cfgOf(Acks::All | Acks::View, false));
        ASSERT_EQ(r.impl->trySubmit(kTopic, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::Ok);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        EXPECT_EQ(r.wire[0].attrs, 0);
    }
}

TEST(CheckedWritesTxnVoteClient, RejectedAtomicBatchCompletesOnceWithTheEntry) {
    Rig r(cfgOf(Acks::All | Acks::View, false));
    rc::BatchBuilder b(r.impl, kTopic, 0);
    b.add(kVal).add(kVal).add(kVal);
    ASSERT_EQ(b.send(rc::AckToken{9}), rc::OpResult::Accepted);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    r.impl->onProduceResponse(r.wire[0].corrId, 100, 0, -1, true, entry(0, kNoVote, kConstraint));
    ASSERT_EQ(r.acks.times[9], 1);
    const rc::Completion& c = r.acks.done.at(9);
    EXPECT_EQ(c.op, rc::OpKind::Tier2Batch);
    EXPECT_EQ(c.status, OpStatus::BrokerError);
    EXPECT_EQ(c.errorCode, kNoVote);
    EXPECT_EQ(c.reserved0, kConstraint);
    EXPECT_EQ(c.value, 100) << "the batch's base offset";
    EXPECT_EQ(c.aux, 3);
}

TEST(CheckedWritesTxnVoteClientRegression, RejectedAtomicBatchCompletesOnceForAnyEntryCountRegression) {
    Rig r(cfgOf(Acks::All | Acks::View, false));
    rc::BatchBuilder b(r.impl, kTopic, 0);
    b.add(kVal).add(kVal).add(kVal);
    ASSERT_EQ(b.send(rc::AckToken{9}), rc::OpResult::Accepted);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    std::vector<uint8_t> three;
    for (uint16_t i = 0; i < 3; ++i) {
        const std::vector<uint8_t> e = entry(i, kNoVote, kConstraint);
        three.insert(three.end(), e.begin(), e.end());
    }
    r.impl->onProduceResponse(r.wire[0].corrId, 100, 0, -1, true, three);
    ASSERT_EQ(r.acks.times[9], 1);
    const rc::Completion& c = r.acks.done.at(9);
    EXPECT_EQ(c.status, OpStatus::BrokerError);
    EXPECT_EQ(c.errorCode, kNoVote);
    EXPECT_EQ(c.reserved0, kConstraint);
    EXPECT_EQ(c.value, 100);
}
