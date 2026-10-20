// tests/producer/ProducerTxnOffsetsTest.cpp - Phase 2b F Task 7: sendOffsetsToTransaction on the unified producer.
//
// AddOffsetsToTxn, then TxnOffsetCommit (entries split across requests per FD5), completing Ok once every entry is
// OK; failures per FD6; the limits (1024 offsets, 255-byte group id); the state gates; zero allocation.
//
// A bare transactional ProducerImpl wired to a capturing coordinator through a ProducerHub (the Client's wiring,
// minus the transport).  The test thread IS the duty thread: every op is applied through applyTxnControl, every
// pass is hub.drainAndFlush on an injected duty clock (no sleeps, no threads, no atomics).
#include "../support/AllocCounter.h"
#include "../support/FakeCoordinator.h"

#include <rune/client/Completion.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace rc  = rune::client;
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
using rc::OpKind;
using rc::OpStatus;
using rc::Token;
using rc::TxnState;
using rc::OffsetEntry;
using rp::ProducerImpl;
using rp::TxnControl;
using test_support::FakeCoordinator;

namespace {

constexpr int64_t  kMs  = 1'000'000;
constexpr int64_t  kT0  = 1'000'000'000;
constexpr uint16_t uec(rpc::ErrorCode e) { return static_cast<uint16_t>(e); }
constexpr int16_t  ec(rpc::ErrorCode e) { return static_cast<int16_t>(e); }
constexpr uint16_t kInvalidTxnState = uec(rpc::ErrorCode::InvalidTxnState);

rc::ProducerConfig txnCfg() {
    rc::ProducerConfig c;
    c.acks                   = rc::Acks::All;
    c.lingerUs               = 0;
    c.batchBytes             = 64 * 1024;
    c.maxInFlight            = 5;
    c.produceTimeoutMs       = 10'000;
    c.deliveryTimeoutMs      = 20'000;
    c.transactionOpTimeoutMs = 1'000;
    c.transactionalId        = "txn-offsets";
    c.transactionTimeoutMs   = 30'000;
    return c;
}

/// Every coordinator frame the link offers, whole (an arena; reserved up front so the zero-alloc test sees none).
struct Capture {
    struct Frame { int64_t corrId; uint16_t templateId; std::size_t at; std::size_t len; };
    std::vector<Frame>   frames;
    std::vector<uint8_t> arena;
    int64_t              sid = 77;
    Capture() { frames.reserve(1 << 14); arena.reserve(std::size_t{4} << 20); }
    [[nodiscard]] rp::CoordinatorSink sink() noexcept { return {&offer, &session, this}; }
    static rp::CoordinatorSink::Offer offer(void* ctx, const uint8_t* b, std::size_t n) noexcept {
        auto* self = static_cast<Capture*>(ctx);
        const auto h = rpc::MessageHeader::decode(b, n);
        if (h) {
            self->frames.push_back({h->correlationId, h->templateId, self->arena.size(), n});
            self->arena.insert(self->arena.end(), b, b + n);
        }
        return rp::CoordinatorSink::Offer::Accepted;
    }
    static int64_t session(void* ctx) noexcept { return static_cast<Capture*>(ctx)->sid; }
    [[nodiscard]] const uint8_t* data(const Frame& f) const noexcept { return arena.data() + f.at; }
    [[nodiscard]] std::vector<Frame> of(uint16_t tid) const {
        std::vector<Frame> v;
        for (const auto& f : frames) if (f.templateId == tid) v.push_back(f);
        return v;
    }
};

/// One txn op's completion (the handler form): how often, the status, the text.
struct Done {
    int         times = 0;
    OpStatus    status = OpStatus::Ok;
    uint16_t    code = 0;
    OpKind      op = OpKind::SendOffsets;
    std::string text;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t> t) noexcept {
        auto* d = static_cast<Done*>(ctx);
        ++d->times;
        d->status = c.status;
        d->code   = c.errorCode;
        d->op     = c.op;
        d->text.assign(reinterpret_cast<const char*>(t.data()), t.size());
    }
};

std::vector<OffsetEntry> offsetsOf(int n, int32_t topic = 1) {
    std::vector<OffsetEntry> v(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) v[static_cast<std::size_t>(i)] = OffsetEntry{topic, i % 7, 1000 + i};
    return v;
}

struct Rig {
    Capture                       cap;
    rp::ProducerHub               hub;
    std::unique_ptr<ProducerImpl> impl;
    ProducerImpl::SendFn          sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) { return true; };
    int64_t                       now = kT0;
    Token                         next = 0;
    std::array<Done, 64>          done{};   // by token

    explicit Rig(rc::ProducerConfig c = txnCfg()) {
        hub.setCoordinatorSink(cap.sink());
        impl = std::make_unique<ProducerImpl>(std::move(c), 1 << 16);
        hub.add(impl.get());
    }
    ~Rig() { hub.remove(impl.get()); }

    Token apply(TxnControl::Kind k, std::span<const OffsetEntry> offs = {}, std::string_view group = {}) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&Done::on, &done[++next]};
        c.token          = next;
        c.offsets        = offs;
        c.groupId        = group;
        impl->applyTxnControl(c, now);
        return next;
    }
    Done& d(Token t) { return done[t]; }
    Token sendOffsets(std::span<const OffsetEntry> offs, std::string_view group = "grp-1") {
        return apply(TxnControl::Kind::SendOffsets, offs, group);
    }
    void tick(int64_t advanceNs = 0) {
        now += advanceNs;
        hub.drainAndFlush(now, 42, sink);
    }
    void answer(const test_support::AnswerFrame& f) {
        ASSERT_TRUE(hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now));
    }
    void answerFrame(const Capture::Frame& f, rp::ReqKind kind, int16_t code) {
        answer(FakeCoordinator::answer(kind, f.corrId, code));
    }
    /// initTransactions answered OK, then beginTransaction().
    void ready() {
        const Token t = apply(TxnControl::Kind::Init);
        tick();
        const auto inits = cap.of(rpc::InitProducerId::TEMPLATE_ID);
        ASSERT_EQ(inits.size(), 1u);
        test_support::AnswerFrame a;
        rpc::InitProducerIdResponse m;
        m.correlationId = inits[0].corrId;
        m.producerId    = 42;
        m.producerEpoch = 3;
        a.len        = m.encode(a.buf.data());
        a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
        answer(a);
        ASSERT_EQ(d(t).times, 1);
        ASSERT_EQ(d(t).status, OpStatus::Ok);
    }
    void begin() {
        apply(TxnControl::Kind::Begin);
        impl->setTxnGate(ProducerImpl::TxnGate::Open);
    }
    void inTxn() {
        ready();
        begin();
        ASSERT_EQ(impl->txnStateDuty(), TxnState::InTransaction);
    }
    /// The one outstanding frame of `tid` after a pass (failing the test when absent).
    [[nodiscard]] Capture::Frame lastOf(uint16_t tid) {
        const auto v = cap.of(tid);
        EXPECT_FALSE(v.empty());
        return v.empty() ? Capture::Frame{} : v.back();
    }
    /// A full sendOffsets: AddOffsets answered OK, the TxnOffsetCommit requests all answered OK.
    void sendOk(Token t) {
        tick();
        answerFrame(lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
        tick();
        for (const auto& f : cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID)) {
            if (d(t).times == 0) answerFrame(f, rp::ReqKind::TxnOffsetCommit, 0);
        }
    }
};

}  // namespace

// ---- the happy path ------------------------------------------------------------------------------------------

TEST(ProducerTxnOffsets, AddOffsetsThenTxnOffsetCommitThenOk) {
    Rig r;
    r.inTxn();
    const auto offs = offsetsOf(3);
    const Token t = r.sendOffsets(offs, "grp-1");
    EXPECT_EQ(r.d(t).times, 0);

    r.tick();   // AddOffsetsToTxn goes out first; the TxnOffsetCommit waits for its OK
    const auto adds = r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_EQ(adds.size(), 1u);
    EXPECT_TRUE(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).empty());
    const auto add = rpc::AddOffsetsToTxn::decode(r.cap.data(adds[0]), adds[0].len);
    ASSERT_TRUE(add);
    EXPECT_EQ(add->producerId, 42);
    EXPECT_EQ(add->producerEpoch, 3);
    EXPECT_EQ(std::string(add->transactionalId.begin(), add->transactionalId.end()), "txn-offsets");
    EXPECT_EQ(std::string(add->groupId.begin(), add->groupId.end()), "grp-1");
    EXPECT_EQ(r.d(t).times, 0);

    r.answerFrame(adds[0], rp::ReqKind::AddOffsets, 0);   // TxnStarted::Yes
    EXPECT_EQ(r.d(t).times, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_EQ(commits.size(), 1u);
    const auto oc = rpc::TxnOffsetCommit::decode(r.cap.data(commits[0]), commits[0].len);
    ASSERT_TRUE(oc);
    EXPECT_EQ(oc->producerId, 42);
    EXPECT_EQ(std::string(oc->groupId.begin(), oc->groupId.end()), "grp-1");
    ASSERT_EQ(oc->entries.size(), 3u);
    std::size_t i = 0;
    for (const auto& e : oc->entries) {
        EXPECT_EQ(e.topicId, offs[i].topicId);
        EXPECT_EQ(e.partition, offs[i].partition);
        EXPECT_EQ(e.offset, offs[i].offset);
        ++i;
    }
    EXPECT_EQ(r.d(t).times, 0);

    r.answerFrame(commits[0], rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    EXPECT_EQ(r.d(t).op, OpKind::SendOffsets);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction);

    // The AddOffsets OK started the transaction (B-1): a commit now sends EndTxn(commit).
    const Token c = r.apply(TxnControl::Kind::Commit);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(c).status, OpStatus::Ok);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
}

TEST(ProducerTxnOffsets, ManyOffsetsSplitAcrossRequestsAllOrError) {
    Rig r;
    r.inTxn();
    const auto offs = offsetsOf(1024);   // the limit: 18 bytes each cannot fit one 4096-byte frame
    const Token t = r.sendOffsets(offs, "grp-many");
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_GE(commits.size(), 2u);
    ASSERT_LE(commits.size(), rp::CoordinatorLink::kMaxCoordRequests);
    // Every entry exactly once, in order, each request within one frame.
    std::size_t total = 0;
    for (const auto& f : commits) {
        EXPECT_LE(f.len, rp::CoordinatorLink::kCoordFrameCap);
        const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(f), f.len);
        ASSERT_TRUE(m);
        for (const auto& e : m->entries) {
            ASSERT_LT(total, offs.size());
            EXPECT_EQ(e.topicId, offs[total].topicId);
            EXPECT_EQ(e.partition, offs[total].partition);
            EXPECT_EQ(e.offset, offs[total].offset);
            ++total;
        }
    }
    EXPECT_EQ(total, offs.size());
    // OK only once EVERY request is OK.
    for (std::size_t i = 0; i + 1 < commits.size(); ++i) {
        r.answerFrame(commits[i], rp::ReqKind::TxnOffsetCommit, 0);
        EXPECT_EQ(r.d(t).times, 0) << "completed before request " << (commits.size() - 1) << " was answered";
    }
    r.answerFrame(commits.back(), rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
}

TEST(ProducerTxnOffsets, OneFailedRequestOfManyFailsTheCallAndPoisonsTheTransaction) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(1024), "grp-many");
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_GE(commits.size(), 3u);
    r.answerFrame(commits[0], rp::ReqKind::TxnOffsetCommit, 0);
    r.answerFrame(commits[1], rp::ReqKind::TxnOffsetCommit, ec(rpc::ErrorCode::InvalidTxnState));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError);
    // A late OK of another request of the same call changes nothing (completed exactly once).
    r.answerFrame(commits[2], rp::ReqKind::TxnOffsetCommit, 0);
    r.tick();
    EXPECT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), commits.size()) << "nothing is re-sent";
}

TEST(ProducerTxnOffsets, NoEntriesCompletesAfterAddOffsets) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets({}, "grp-1");
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    EXPECT_TRUE(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).empty());
}

// ---- limits and gates (regressions) ---------------------------------------------------------------------------

TEST(ProducerTxnOffsetsRegression, TooManyOffsetsIsInvalidArgumentNotTruncatedRegression) {
    Rig r;
    r.inTxn();
    const auto tooMany = offsetsOf(1025);
    const Token t = r.sendOffsets(tooMany);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::InvalidArgument);
    EXPECT_NE(r.d(t).text.find("1024"), std::string::npos) << r.d(t).text;
    r.tick(10 * kMs);
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty()) << "nothing was sent, not even a truncated prefix";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction) << "an argument error does not poison";

    const std::string longGroup(256, 'g');
    const Token g = r.sendOffsets(offsetsOf(1), longGroup);
    ASSERT_EQ(r.d(g).times, 1);
    EXPECT_EQ(r.d(g).status, OpStatus::InvalidArgument);
    EXPECT_NE(r.d(g).text.find("255"), std::string::npos) << r.d(g).text;
    const Token e = r.sendOffsets(offsetsOf(1), "");
    EXPECT_EQ(r.d(e).status, OpStatus::InvalidArgument);

    // The limits themselves are accepted.
    const std::string maxGroup(255, 'g');
    const Token ok = r.sendOffsets(offsetsOf(1024), maxGroup);
    EXPECT_EQ(r.d(ok).times, 0);
    r.tick();
    const auto adds = r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_EQ(adds.size(), 1u);
    const auto m = rpc::AddOffsetsToTxn::decode(r.cap.data(adds[0]), adds[0].len);
    ASSERT_TRUE(m);
    EXPECT_EQ(m->groupId.size(), 255u);
}

TEST(ProducerTxnOffsetsRegression, OffsetsOutsideATransactionCompleteInvalidTxnStateRegression) {
    Rig r;
    // Uninitialized
    Token t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    // Ready (initialized, no beginTransaction)
    r.ready();
    t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    r.tick(10 * kMs);
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty()) << "nothing was sent outside a transaction";
    // Committing
    r.begin();
    r.apply(TxnControl::Kind::Commit);   // unstarted: ends locally
    r.tick();
    t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty());
}

TEST(ProducerTxnOffsetsRegression, PoisonedTransactionRefusesOffsetsWithThePoisonRegression) {
    Rig r;
    r.inTxn();
    r.impl->testPoison(OpStatus::BrokerError, 77, rp::AbortKind::Plain);
    const Token t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, 77u);
    r.tick(10 * kMs);
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty());
}

TEST(ProducerTxnOffsetsRegression, AnotherCallWhileOneRunsIsRefusedNotInterleavedRegression) {
    Rig r;
    r.inTxn();
    const Token a = r.sendOffsets(offsetsOf(2), "grp-a");
    const Token b = r.sendOffsets(offsetsOf(2), "grp-b");
    ASSERT_EQ(r.d(b).times, 1);
    EXPECT_EQ(r.d(b).code, kInvalidTxnState);
    EXPECT_EQ(r.d(a).times, 0);
    r.sendOk(a);
    ASSERT_EQ(r.d(a).times, 1);
    EXPECT_EQ(r.d(a).status, OpStatus::Ok);
    // The scratch is free again.
    const Token c = r.sendOffsets(offsetsOf(2), "grp-c");
    EXPECT_EQ(r.d(c).times, 0);
}

// ---- failures (FD6) --------------------------------------------------------------------------------------------

TEST(ProducerTxnOffsets, NotAuthorizedOnAddOffsetsIsAbortableNotFatal) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 45);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, 45u);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError);
    r.tick(10 * kMs);
    EXPECT_TRUE(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).empty()) << "no TxnOffsetCommit after a failed AddOffsets";
    // The app can abort and carry on.
    const Token ab = r.apply(TxnControl::Kind::Abort);
    r.tick();   // never started on the coordinator: the abort ends locally (B-1), no EndTxn
    ASSERT_EQ(r.d(ab).times, 1);
    EXPECT_EQ(r.d(ab).status, OpStatus::Ok);
    EXPECT_TRUE(r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty());
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
}

TEST(ProducerTxnOffsets, NotAuthorizedOnTxnOffsetCommitIsAbortable) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    r.answerFrame(r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID), rp::ReqKind::TxnOffsetCommit, 45);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, 45u);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError);
    // AddOffsets was OK: the transaction exists on the coordinator, so the abort sends EndTxn(abort).
    r.apply(TxnControl::Kind::Abort);
    r.tick();
    EXPECT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
}

TEST(ProducerTxnOffsets, FencedOnAddOffsetsIsFatal) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, ec(rpc::ErrorCode::ProducerFenced));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Fenced);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::FatalError);
}

TEST(ProducerTxnOffsets, ConcurrentTransactionsOnAddOffsetsIsRetriedByTheLink) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    const auto first = r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    r.answerFrame(first, rp::ReqKind::AddOffsets, ec(rpc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(r.d(t).times, 0);
    r.tick(5 * kMs);   // past the 1 ms backoff
    const auto adds = r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_EQ(adds.size(), 2u);
    EXPECT_EQ(adds[1].corrId, first.corrId) << "the same request is re-sent";
}

TEST(ProducerTxnOffsets, TimeoutCompletesTimeoutAndPoisons) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();   // AddOffsets on the wire, never answered
    r.tick(1'500 * kMs);   // transactionOpTimeoutMs = 1000
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Timeout);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError);
    // The AddOffsets was on the wire: the transaction may exist - the abort sends EndTxn(abort) (m15).
    r.apply(TxnControl::Kind::Abort);
    r.tick();
    EXPECT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.tick(5 * kMs);
    EXPECT_EQ(r.d(t).times, 1) << "completed exactly once";
}

// ---- interactions with the rest of the machine ------------------------------------------------------------------

TEST(ProducerTxnOffsets, CommitWaitsForARunningOffsetsCall) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    const Token c = r.apply(TxnControl::Kind::Commit);
    r.tick();
    EXPECT_TRUE(r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty()) << "EndTxn waits for the offsets";
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    r.answerFrame(r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID), rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(c).status, OpStatus::Ok);
}

TEST(ProducerTxnOffsets, FailedOffsetsFailTheCommitInProgress) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    const Token c = r.apply(TxnControl::Kind::Commit);
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 45);
    EXPECT_EQ(r.d(t).code, 45u);
    ASSERT_EQ(r.d(c).times, 1);
    EXPECT_EQ(r.d(c).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(c).code, 45u);
    EXPECT_TRUE(r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty()) << "never a commit marker over failed offsets";
}

TEST(ProducerTxnOffsets, AbortCancelsARunningOffsetsCall) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();   // AddOffsets on the wire
    const Token ab = r.apply(TxnControl::Kind::Abort);
    r.tick();   // the abort cancels the running call; the next pass sends EndTxn
    r.tick();
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);   // the abort's outcome (InvalidTxnState)
    // AddOffsets was on the wire: EndTxn(abort) is sent (the transaction may exist).
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(ab).status, OpStatus::Ok);
    // A late AddOffsets answer finds no op and changes nothing.
    r.answerFrame(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).back(), rp::ReqKind::AddOffsets, 0);
    EXPECT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
}

TEST(ProducerTxnOffsets, SessionRevokedCompletesARunningOffsetsCall) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    r.impl->onSessionRevoked(46);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, 46u);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::FatalError);
}

TEST(ProducerTxnOffsets, CloseImmediatelyCompletesARunningOffsetsCall) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();
    (void)r.impl->closeImmediately();
    r.tick();
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Closed);
}

// ---- zero allocation ---------------------------------------------------------------------------------------------

TEST(ProducerTxnOffsets, AllocatesNothingInSteadyState) {
    Rig r;
    r.inTxn();
    const auto offs = offsetsOf(16);
    // Warm up: one full call (first use of the scratch, the link slots, the completion path).
    for (int i = 0; i < 4; ++i) {
        const Token t = r.sendOffsets(offs, "grp-alloc");
        r.sendOk(t);
        ASSERT_EQ(r.d(t).times, 1);
        ASSERT_EQ(r.d(t).status, OpStatus::Ok);
        r.next = 0;   // reuse the Done slots (the counters are per call)
        r.done = {};
    }
    // The measured calls reuse Done slots 1..: only counters change in them.
    alloc_counter::startThisThread();
    for (int i = 0; i < 1000; ++i) {
        r.next = 0;
        const Token t = r.sendOffsets(offs, "grp-alloc");
        r.tick();
        r.answerFrame(r.cap.frames[r.cap.frames.size() - 1], rp::ReqKind::AddOffsets, 0);
        r.tick();
        r.answerFrame(r.cap.frames[r.cap.frames.size() - 1], rp::ReqKind::TxnOffsetCommit, 0);
        if (r.d(t).times != static_cast<int>(i) + 1) {
            alloc_counter::stopThisThread();
            FAIL() << "call " << i << " did not complete Ok exactly once";
        }
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "sendOffsets allocated in steady state";
}
