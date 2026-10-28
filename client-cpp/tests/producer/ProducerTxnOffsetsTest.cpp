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
#include <rune/client/Key.h>
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
    int                  rediscovers = 0;   ///< re-discovery requests (NotCoordinator answered)
    [[nodiscard]] rp::CoordinatorSink sink() noexcept { return {&offer, &session, this, &rediscover}; }
    static bool rediscover(void* ctx) noexcept {
        ++static_cast<Capture*>(ctx)->rediscovers;
        return true;
    }
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

    explicit Rig(rc::ProducerConfig c = txnCfg(), int32_t ringCap = 1 << 16) {
        hub.setCoordinatorSink(cap.sink());
        impl = std::make_unique<ProducerImpl>(std::move(c), ringCap);
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
    /// Phase 2b F Task 8: the HANDLE side of the ring path - writes a kMsgTxnControl (the handler form); the next
    /// pass drains and applies it in ring order.
    struct Issued { Token token; rc::OpResult result; };
    Issued issue(TxnControl::Kind k, std::span<const OffsetEntry> offs = {}, std::string_view group = {}) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&Done::on, &done[++next]};
        c.token          = next;
        c.offsets        = offs;
        c.groupId        = group;
        return {next, impl->issueTxnControl(c)};
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

// #348: changed from "completes after an AddOffsetsToTxn round trip" - an empty list completes Ok locally.
TEST(ProducerTxnOffsets, NoEntriesCompletesOkLocallyWithoutAddOffsets) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets({}, "grp-1");
    ASSERT_EQ(r.d(t).times, 1) << "completes at once, before any pass";
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    r.tick();
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty());
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
    // Committing: a started transaction (AddOffsets answered OK), the commit's EndTxn on the wire and unanswered.
    r.begin();
    const Token first = r.sendOffsets(offsetsOf(1));
    r.sendOk(first);
    ASSERT_EQ(r.d(first).status, OpStatus::Ok);
    r.apply(TxnControl::Kind::Commit);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::CommittingTransaction);
    t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    r.tick();
    EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u) << "nothing new was sent while committing";
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
    // Aborting: the same, with the abort's EndTxn unanswered.
    r.begin();
    const Token second = r.sendOffsets(offsetsOf(1));
    r.sendOk(second);
    ASSERT_EQ(r.d(second).status, OpStatus::Ok);
    r.apply(TxnControl::Kind::Abort);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 2u);
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::AbortingTransaction);
    t = r.sendOffsets(offsetsOf(2));
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).code, kInvalidTxnState);
    r.tick();
    EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 2u);
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

TEST(ProducerTxnOffsetsRegression, AbortAfterASessionChangeStillEndsAnOfferedTxnRegression) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(2));
    r.tick();   // AddOffsets offered: the coordinator may have opened the transaction
    ASSERT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u);
    r.cap.sid = 0;
    r.hub.onCoordinatorSessionChanged();   // InFlight -> Unsent: no longer "in flight", but it WAS offered
    const Token ab = r.apply(TxnControl::Kind::Abort);
    r.tick();
    r.tick();
    EXPECT_EQ(r.d(t).times, 1);
    r.cap.sid = 78;   // reconnected
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u) << "the abort must not end locally over an offered leg";
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(ab).status, OpStatus::Ok);
}

TEST(ProducerTxnOffsetsRegression, AbortAfterASessionChangeStillEndsAnOfferedAddPartitionsRegression) {
    Rig r;
    r.inTxn();
    const std::vector<uint8_t> v(16, 0x5A);
    ASSERT_EQ(r.impl->trySubmit(1, 0, rc::Key(uint64_t{7}), v, 0), rc::SendResult::Ok);
    for (int i = 0; i < 3 && r.cap.of(rpc::AddPartitionsToTxn::TEMPLATE_ID).empty(); ++i) r.tick();   // AddPartitionsToTxn offered
    ASSERT_EQ(r.cap.of(rpc::AddPartitionsToTxn::TEMPLATE_ID).size(), 1u);
    r.cap.sid = 0;
    r.hub.onCoordinatorSessionChanged();
    const Token ab = r.apply(TxnControl::Kind::Abort);
    r.tick();
    r.tick();
    r.cap.sid = 78;
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u) << "the abort must not end locally over an offered leg";
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(ab).status, OpStatus::Ok);
}

TEST(ProducerTxnOffsetsRegression, AnotherCallWhileOneRunsIsRefusedNotInterleavedRegression) {
    Rig r;
    r.inTxn();
    const Token a = r.sendOffsets(offsetsOf(2), "grp-a");
    const Token b = r.sendOffsets(offsetsOf(2), "grp-b");
    ASSERT_EQ(r.d(b).times, 1);
    EXPECT_EQ(r.d(b).status, OpStatus::InvalidArgument);   // #347: was BrokerError(19)
    EXPECT_EQ(r.d(b).code, 0);
    EXPECT_EQ(r.d(a).times, 0);
    r.sendOk(a);
    ASSERT_EQ(r.d(a).times, 1);
    EXPECT_EQ(r.d(a).status, OpStatus::Ok);
    // The scratch is free again.
    const Token c = r.sendOffsets(offsetsOf(2), "grp-c");
    EXPECT_EQ(r.d(c).times, 0);
}

// #347: a second concurrent call is refused CLEARLY (InvalidArgument with the reason), the first completes normally and
// the transaction is not poisoned - the commit that follows succeeds.
TEST(ProducerTxnOffsetsRegression, ASecondConcurrentSendOffsetsIsRefusedClearlyAndTheTxnStaysGoodRegression) {
    Rig r;
    r.inTxn();
    const Token a = r.sendOffsets(offsetsOf(2), "grp-a");
    const Token b = r.sendOffsets(offsetsOf(3), "grp-b");
    ASSERT_EQ(r.d(b).times, 1);
    EXPECT_EQ(r.d(b).status, OpStatus::InvalidArgument);
    EXPECT_EQ(r.d(b).code, 0) << "not the state-error code 19";
    EXPECT_EQ(r.d(b).text, "a sendOffsetsToTransaction is already in flight on this producer; wait for its completion");
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction) << "the refusal poisons nothing";
    EXPECT_EQ(r.d(a).times, 0);
    r.sendOk(a);
    ASSERT_EQ(r.d(a).times, 1);
    EXPECT_EQ(r.d(a).status, OpStatus::Ok) << "the first call completes normally";
    EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u) << "the refused call sent nothing";
    const Token c = r.apply(TxnControl::Kind::Commit);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    ASSERT_EQ(r.d(c).times, 1);
    EXPECT_EQ(r.d(c).status, OpStatus::Ok) << "the transaction was never poisoned";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
}

// #348: an empty offsets list completes Ok locally, with no wire traffic, and does not open the transaction on the
// coordinator - a commit with nothing else in the transaction ends locally too (B-1), exactly like an empty one.
TEST(ProducerTxnOffsetsRegression, EmptySendOffsetsCompletesLocallyWithNoWireTrafficRegression) {
    Rig r;
    r.inTxn();
    const std::size_t framesBefore = r.cap.frames.size();
    const Token t = r.sendOffsets({}, "grp-1");
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    EXPECT_EQ(r.d(t).op, OpKind::SendOffsets);
    for (int i = 0; i < 5; ++i) r.tick();
    EXPECT_EQ(r.cap.frames.size(), framesBefore) << "no wire traffic";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction);
    const Token c = r.apply(TxnControl::Kind::Commit);
    for (int i = 0; i < 3; ++i) r.tick();
    ASSERT_EQ(r.d(c).times, 1);
    EXPECT_EQ(r.d(c).status, OpStatus::Ok);
    EXPECT_TRUE(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).empty());
    EXPECT_TRUE(r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty()) << "the coordinator never saw the transaction: no EndTxn";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
}

// #348 / #698: an empty call while a real one runs is refused (InvalidArgument; one call at a time) and leaves the
// running call alone.
TEST(ProducerTxnOffsetsRegression, EmptySendOffsetsDuringARunningCallIsRefusedAndLeavesItAloneRegression) {
    Rig r;
    r.inTxn();
    const Token a = r.sendOffsets(offsetsOf(2), "grp-a");
    const Token e = r.sendOffsets({}, "grp-e");
    ASSERT_EQ(r.d(e).times, 1);
    EXPECT_EQ(r.d(e).status, OpStatus::InvalidArgument);
    EXPECT_EQ(r.d(a).times, 0);
    r.sendOk(a);
    ASSERT_EQ(r.d(a).times, 1);
    EXPECT_EQ(r.d(a).status, OpStatus::Ok);
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

TEST(ProducerTxnOffsetsRegression, AddOffsetsRetriesConcurrentTransactionsRegression) {
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

TEST(ProducerTxnOffsets, PoisonWhileRunningEndsTheCallWithThePoisonAndSendsNoMoreRequests) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(1024), "grp-many");
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    // Another leg fails before the first TxnOffsetCommit goes out.
    r.impl->testPoison(OpStatus::BrokerError, 77, rp::AbortKind::Plain);
    r.tick();
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(t).code, 77u);
    EXPECT_TRUE(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).empty()) << "no request after the poison";
    // Poisoned between the requests of one call: the unsent rest is never sent.
    Rig q;
    q.inTxn();
    const Token u = q.sendOffsets(offsetsOf(1024), "grp-many");
    q.tick();
    q.answerFrame(q.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    q.cap.sid = 0;   // not connected: nothing is offered, but a poison arrives
    q.tick();
    q.impl->testPoison(OpStatus::BrokerError, 78, rp::AbortKind::Plain);
    q.cap.sid = 77;
    const std::size_t before = q.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size();
    q.tick();
    ASSERT_EQ(q.d(u).times, 1);
    EXPECT_EQ(q.d(u).code, 78u);
    EXPECT_EQ(q.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), before);
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

// ---- Task 8: the ring path (kMsgTxnControl) -------------------------------------------------------------------

namespace {
/// Ring mode: `inTxn()` for a rig whose Begin went through the ring (the gate opens when its message is written).
void ringReady(Rig& r) {
    r.ready();
    const auto b = r.issue(TxnControl::Kind::Begin);
    ASSERT_EQ(b.result, rc::OpResult::Accepted);
    EXPECT_EQ(r.impl->txnGate(), ProducerImpl::TxnGate::Open) << "Begin opens the gate once its message is written";
    r.tick();
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction);
}
}  // namespace

TEST(ProducerTxnControl, ControlOpIsAppliedInRingOrderBetweenRecords) {
    Rig r;
    ringReady(r);
    const rc::Key key(uint64_t{1});
    const std::string v = "x";
    const std::span<const uint8_t> val(reinterpret_cast<const uint8_t*>(v.data()), 1);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(r.impl->trySubmit(1, 0, key, val, 0), rc::SendResult::Ok);
    const auto c = r.issue(TxnControl::Kind::Commit);
    ASSERT_EQ(c.result, rc::OpResult::Accepted);
    EXPECT_EQ(r.impl->txnGate(), ProducerImpl::TxnGate::Closed) << "the commit closes the gate at issue";
    EXPECT_EQ(r.impl->trySubmit(1, 0, key, val, 0), rc::SendResult::NotInTransaction);
    // A produce that raced past the gate lands in the ring AFTER the commit message: the drain applies the commit
    // first (the three records before it are in it), so this one reaches the duty side outside the transaction.
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    ASSERT_EQ(r.impl->trySubmit(1, 0, key, val, 0), rc::SendResult::Ok);
    r.tick();
    r.tick();
    EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 1u) << "the record after the commit was refused, never produced";
    EXPECT_EQ(r.impl->txnPartitionCount(), 1u) << "the three before it joined the transaction";
    r.tick();
    EXPECT_EQ(r.d(c.token).times, 1) << "the commit completes (with the refusal's error: the record after it never joined)";
    EXPECT_NE(r.d(c.token).status, OpStatus::Ok);
}

TEST(ProducerTxnControlRegression, ControlOpAfterCloseCompletesClosedRegression) {
    // Every kind that carries a completion, written to the ring and then discarded by closeImmediately: each token
    // completes Closed exactly once (never dropped silently), CQ credit and all.
    for (const TxnControl::Kind kind : {TxnControl::Kind::Init, TxnControl::Kind::SendOffsets, TxnControl::Kind::Commit,
                                        TxnControl::Kind::Abort, TxnControl::Kind::Flush}) {
        SCOPED_TRACE(static_cast<int>(kind));
        Rig r;
        const OffsetEntry e{1, 0, 5};
        const auto i = r.issue(kind, std::span<const OffsetEntry>(&e, 1), "g");
        ASSERT_EQ(i.result, rc::OpResult::Accepted);
        ASSERT_EQ(r.d(i.token).times, 0);
        ASSERT_TRUE(r.impl->closeImmediately() == rc::OpResult::Accepted);   // Discarding: the ring is discarded next pass
        r.tick();
        EXPECT_EQ(r.d(i.token).times, 1) << "the op in the ring completed";
        EXPECT_EQ(r.d(i.token).status, OpStatus::Closed);
        r.tick();
        EXPECT_EQ(r.d(i.token).times, 1) << "exactly once";
        // Issued after the close: the intake is closed, nothing is accepted, nothing completes later.
        const auto late = r.issue(kind, std::span<const OffsetEntry>(&e, 1), "g");
        EXPECT_NE(late.result, rc::OpResult::Accepted);
        r.tick();
        EXPECT_EQ(r.d(late.token).times, 0);
    }
}

TEST(ProducerTxnControlRegression, SendOffsetsTravelsTheRingIntoTheDutyScratchRegression) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    const auto offs = offsetsOf(3);
    const auto i = r.issue(TxnControl::Kind::SendOffsets, offs, "grp-ring");
    ASSERT_EQ(i.result, rc::OpResult::Accepted);
    r.tick();   // the drain applies the op ...
    r.tick();   // ... and the next pass's txnStep sends AddOffsetsToTxn
    const auto adds = r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_EQ(adds.size(), 1u);
    r.answerFrame(adds[0], rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_EQ(commits.size(), 1u);
    const auto oc = rpc::TxnOffsetCommit::decode(r.cap.data(commits[0]), commits[0].len);
    ASSERT_TRUE(oc);
    EXPECT_EQ(std::string(oc->groupId.begin(), oc->groupId.end()), "grp-ring");
    ASSERT_EQ(oc->entries.size(), 3u);
    std::size_t k = 0;
    for (const auto& e : oc->entries) {
        EXPECT_EQ(e.topicId, offs[k].topicId);
        EXPECT_EQ(e.partition, offs[k].partition);
        EXPECT_EQ(e.offset, offs[k].offset);
        ++k;
    }
    r.answerFrame(commits[0], rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(i.token).times, 1);
    EXPECT_EQ(r.d(i.token).status, OpStatus::Ok);
    // A second call is refused (InvalidArgument, #347) while one runs: documented, never queued.
    const auto second = r.issue(TxnControl::Kind::SendOffsets, offsetsOf(2), "grp-ring");
    const auto third  = r.issue(TxnControl::Kind::SendOffsets, offsetsOf(2), "grp-ring");
    r.tick();
    EXPECT_EQ(r.d(third.token).status, OpStatus::InvalidArgument);
    EXPECT_EQ(r.d(third.token).code, 0);
    EXPECT_EQ(r.d(third.token).times, 1);
    EXPECT_EQ(r.d(second.token).times, 0) << "the first of the two is running";
}

TEST(ProducerTxnControlRegression, OverCapSendOffsetsCompletesInvalidArgumentThroughTheRingRegression) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    const std::size_t framesBefore = r.cap.frames.size();
    const auto tooMany = offsetsOf(static_cast<int>(rp::kMaxTxnOffsets) + 1);
    const std::string longGroup(rp::kMaxTxnGroupIdBytes + 1, 'g');
    const auto a = r.issue(TxnControl::Kind::SendOffsets, tooMany, "grp");
    const auto b = r.issue(TxnControl::Kind::SendOffsets, offsetsOf(2), longGroup);
    const auto c = r.issue(TxnControl::Kind::SendOffsets, offsetsOf(2), "");
    ASSERT_EQ(a.result, rc::OpResult::Accepted);
    ASSERT_EQ(b.result, rc::OpResult::Accepted);
    ASSERT_EQ(c.result, rc::OpResult::Accepted);
    r.tick();
    for (const Token t : {a.token, b.token, c.token}) {
        EXPECT_EQ(r.d(t).times, 1) << t;
        EXPECT_EQ(r.d(t).status, OpStatus::InvalidArgument) << t;
    }
    EXPECT_EQ(r.cap.frames.size(), framesBefore) << "nothing reached the coordinator";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction) << "an invalid argument does not poison the transaction";
    // A ring too small to ever carry the message is a plain InvalidArgument (no completion).
    Rig tiny;
    ringReady(tiny);
    const auto big = tiny.issue(TxnControl::Kind::SendOffsets, offsetsOf(1024), "grp");
    EXPECT_EQ(big.result, rc::OpResult::InvalidArgument);
    tiny.tick();
    EXPECT_EQ(tiny.d(big.token).times, 0);
}

TEST(ProducerTxnControl, IssuingAndApplyingControlOpsAllocatesNothing) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    const auto offs = offsetsOf(1024);
    for (int i = 0; i < 3; ++i) {   // warm up: first use of everything
        const auto f = r.issue(TxnControl::Kind::Flush);
        r.tick();
        r.tick();
        ASSERT_EQ(r.d(f.token).times, 1);
        r.next = 0;
        r.done = {};
    }
    alloc_counter::startThisThread();
    for (int i = 0; i < 500; ++i) {
        r.next = 0;
        const auto f = r.issue(TxnControl::Kind::Flush);
        r.tick();
        r.tick();
        if (r.d(f.token).times != i + 1) {
            alloc_counter::stopThisThread();
            FAIL() << "flush " << i << " did not complete once";
        }
    }
    const long flushAllocs = alloc_counter::stopThisThread();
    EXPECT_EQ(flushAllocs, 0) << "a flush control op allocated on the issue side or the duty side";
    // The widest message: 1024 offsets + a 1-byte group, written to the ring and applied (the issue side alone).
    static int counted = 0;
    const rc::CompletionHandler h{[](void*, const rc::Completion&, std::span<const uint8_t>) noexcept { ++counted; }, nullptr};
    TxnControl wide;
    wide.kind           = TxnControl::Kind::SendOffsets;
    wide.target.handler = h;
    wide.token          = 50;
    wide.offsets        = offs;
    wide.groupId        = "g";
    alloc_counter::startThisThread();
    for (int i = 0; i < 4; ++i) {
        const rc::OpResult res = r.impl->issueTxnControl(wide);
        if (res != rc::OpResult::Accepted) {
            alloc_counter::stopThisThread();
            FAIL() << "wide control op refused";
        }
        r.impl->drainRing(r.now, 256);   // applied on the duty side: copied into the preallocated scratch
        r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "a sendOffsets control op allocated on the issue side or the duty side";
}

TEST(ProducerTxnControlRegression, LiveProducerFailAllCompletesControlOpsWithTheLossStatusNotClosedRegression) {
    Rig r;
    ringReady(r);
    const rc::Key key(uint64_t{1});
    const std::string v = "x";
    const std::span<const uint8_t> val(reinterpret_cast<const uint8_t*>(v.data()), 1);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(r.impl->trySubmit(1, 0, key, val, 0), rc::SendResult::Ok);
    const auto commit = r.issue(TxnControl::Kind::Commit);
    ASSERT_EQ(commit.result, rc::OpResult::Accepted);
    // The session was lost: ProducerHub::failAll(SessionLost) fails everything the producer holds - the ring too -
    // while the producer stays OPEN.
    r.impl->failAllPending(OpStatus::SessionLost);
    EXPECT_EQ(r.d(commit.token).times, 1);
    EXPECT_EQ(r.d(commit.token).status, OpStatus::SessionLost) << "the loss status, never Closed";
    EXPECT_TRUE(r.impl->lifeOpen()) << "the producer is still open";
    // Still usable: the records ahead of the commit poisoned the transaction; abort it, begin a new one, produce.
    const auto ab = r.issue(TxnControl::Kind::Abort);
    r.tick();
    r.tick();
    EXPECT_EQ(r.d(ab.token).times, 1);
    EXPECT_EQ(r.d(ab.token).status, OpStatus::Ok);
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::Ready);
    ASSERT_EQ(r.issue(TxnControl::Kind::Begin).result, rc::OpResult::Accepted);
    r.tick();
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction);
    EXPECT_EQ(r.impl->trySubmit(1, 0, key, val, 0), rc::SendResult::Ok);
    // A void Begin discarded by a fail-all is not swallowed: it is counted like any refused void begin.
    Rig b;
    b.ready();
    const std::uint64_t before = b.impl->txnRefusedBegins();
    ASSERT_EQ(b.issue(TxnControl::Kind::Begin).result, rc::OpResult::Accepted);
    b.impl->failAllPending(OpStatus::SessionLost);
    EXPECT_EQ(b.impl->txnRefusedBegins(), before + 1);
}

// ============================================================================================================
// Phase 2b F Task 9: the offsets cases of the removed transactional producer's test file, on the unified Producer
// (old -> new names are in the Task 9 commit message).
// ============================================================================================================

namespace {
/// A late / duplicate answer: routed to us (ours), changes nothing.
void answerLate(Rig& r, const Capture::Frame& f, rp::ReqKind kind, int16_t code) {
    const auto a = FakeCoordinator::answer(kind, f.corrId, code);
    (void)r.hub.onCoordinatorResponse(a.templateId, a.data(), a.len, r.now);
}
}  // namespace

// sendOffsets is two-phase (AddOffsetsToTxn -> TxnOffsetCommit) and completes exactly once - never at phase 1,
// never twice on duplicates; a commit waits for the offsets (they are part of the transaction).
TEST(ProducerTxnOffsetsRegression, SendOffsetsTwoPhaseCompletesOnceRegression) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(1), "grp");
    r.tick();
    const auto ao = r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_NE(ao.corrId, 0);
    r.answerFrame(ao, rp::ReqKind::AddOffsets, 0);
    EXPECT_EQ(r.d(t).times, 0) << "phase 1 is not the completion";
    answerLate(r, ao, rp::ReqKind::AddOffsets, 0);   // duplicate: ignored
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), 1u) << "phase 2 started exactly once";
    const auto oc = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
    // A commit waits for the offsets (they are part of the transaction).
    const Token commit = r.apply(TxnControl::Kind::Commit);
    r.tick();
    r.tick();
    EXPECT_TRUE(r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty()) << "EndTxn before the offsets landed";
    r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, 0);
    answerLate(r, oc, rp::ReqKind::TxnOffsetCommit, 0);   // duplicate
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u) << "now the EndTxn";
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(commit).status, OpStatus::Ok);
    EXPECT_EQ(r.d(t).times, 1);
}

TEST(ProducerTxnOffsetsRegression, TxnOffsetCommitRetriesConcurrentTransactionsRegression) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(1), "g");
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto oc = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_NE(oc.corrId, 0);
    r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, ec(rpc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(r.d(t).times, 0) << "27 must not complete the op";
    r.tick(5 * kMs);   // past the 1 ms backoff
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_EQ(commits.size(), 2u) << "27 re-sent the TxnOffsetCommit";
    EXPECT_EQ(commits[1].corrId, oc.corrId) << "the same request is re-sent";
    r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
}

// F14: NotCoordinator never just re-sends into the same (loading / moved) coordinator: it asks to re-discover
// (once per generation), waits for the move, then backs off and re-sends - on both offsets legs.
TEST(ProducerTxnOffsetsRegression, NotCoordinatorRediscoversThenRetriesRegression) {
    Rig r;
    r.inTxn();
    const Token t = r.sendOffsets(offsetsOf(1), "g");
    r.tick();
    const auto ao = r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    r.answerFrame(ao, rp::ReqKind::AddOffsets, ec(rpc::ErrorCode::NotCoordinator));
    EXPECT_EQ(r.cap.rediscovers, 1);
    for (int i = 0; i < 16; ++i) r.tick(10 * kMs);   // 160 ms: inside the 1 s op deadline
    EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u) << "re-sent before the coordinator moved";
    EXPECT_EQ(r.d(t).times, 0);
    r.hub.onCoordinatorMoved();
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 2u) << "not re-sent after the re-discovery";
    r.answerFrame(ao, rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto oc = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_NE(oc.corrId, 0);
    r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, ec(rpc::ErrorCode::NotCoordinator));
    EXPECT_EQ(r.cap.rediscovers, 2);
    for (int i = 0; i < 16; ++i) r.tick(10 * kMs);   // 160 ms: inside the 1 s op deadline
    EXPECT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), 1u);
    r.hub.onCoordinatorMoved();
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), 2u);
    r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
}

// No-hang fix round 2: AddOffsetsToTxn / TxnOffsetCommit answered InvalidRequest (the broker could not decode them)
// complete their operation with that error at once - never pending, never re-sent.
TEST(ProducerTxnOffsetsRegression, AddOffsetsAndTxnOffsetCommitInvalidRequestCompleteRegression) {
    constexpr int16_t kInv = ec(rpc::ErrorCode::InvalidRequest);
    for (const bool atAddOffsets : {true, false}) {
        SCOPED_TRACE(atAddOffsets ? "AddOffsetsToTxn" : "TxnOffsetCommit");
        Rig r;
        r.inTxn();
        const Token t = r.sendOffsets(offsetsOf(1), "g");
        r.tick();
        const auto ao = r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID);
        ASSERT_NE(ao.corrId, 0);
        if (atAddOffsets) {
            r.answerFrame(ao, rp::ReqKind::AddOffsets, kInv);
            ASSERT_EQ(r.d(t).times, 1);
            EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
            for (int i = 0; i < 64; ++i) r.tick(10 * kMs);
            EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u);
            continue;
        }
        r.answerFrame(ao, rp::ReqKind::AddOffsets, 0);
        r.tick();
        const auto oc = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
        ASSERT_NE(oc.corrId, 0);
        r.answerFrame(oc, rp::ReqKind::TxnOffsetCommit, kInv);
        ASSERT_EQ(r.d(t).times, 1);
        EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
        for (int i = 0; i < 64; ++i) r.tick(10 * kMs);
        EXPECT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), 1u);
    }
}

// m15 (AddOffsets): a sendOffsets whose AddOffsetsToTxn was sent but timed out makes the transaction possibly
// started: the abort sends EndTxn(abort); an answer of 19 there (nothing was open) ends the abort Ok, not fatal.
TEST(ProducerTxnOffsetsRegression, TimedOutAddOffsetsAbortsPossiblyStartedTxnRegression) {
    Rig r;
    r.inTxn();
    const Token offs = r.sendOffsets(offsetsOf(1), "g");
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u);   // never answered
    r.tick(1'500 * kMs);   // transactionOpTimeoutMs = 1000
    ASSERT_EQ(r.d(offs).times, 1);
    EXPECT_EQ(r.d(offs).status, OpStatus::Timeout);
    const Token abort = r.apply(TxnControl::Kind::Abort);
    r.tick();
    ASSERT_EQ(r.cap.of(rpc::EndTxn::TEMPLATE_ID).size(), 1u);
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, ec(rpc::ErrorCode::InvalidTxnState));
    r.tick();
    ASSERT_EQ(r.d(abort).times, 1);
    EXPECT_EQ(r.d(abort).status, OpStatus::Ok);
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::Ready) << "19 here means nothing was open: not fatal";
}

// ===========================================================================
// Issue #125 (KIP-447): sendOffsetsToTransaction(offsets, GroupMetadata) - every TxnOffsetCommit request of the
// split carries the member block; the legacy form carries none; a fenced member is abortable, never retried.
// ===========================================================================
namespace {
rc::OpResult issueWithMember(Rig& r, Token& token, std::span<const OffsetEntry> offs, std::string_view group,
                             int32_t generation, std::string_view member) {
    TxnControl c;
    c.kind           = TxnControl::Kind::SendOffsets;
    c.target.handler = rc::CompletionHandler{&Done::on, &r.done[++r.next]};
    c.token          = r.next;
    c.offsets        = offs;
    c.groupId        = group;
    c.generationId   = generation;
    c.memberId       = member;
    token            = r.next;
    return r.impl->issueTxnControl(c);
}
}  // namespace

TEST(ProducerTxnOffsetsRegression, EveryRequestOfASplitCarriesTheMemberBlockThroughTheRingRegression) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    const auto offs = offsetsOf(1024);
    const std::string member = "grp-m-7-0123456789abcdef0123456789abcdef";
    Token t = 0;
    ASSERT_EQ(issueWithMember(r, t, offs, "grp-m", 7, member), rc::OpResult::Accepted);
    r.tick();
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_GE(commits.size(), 2u);
    std::size_t total = 0;
    for (const auto& f : commits) {
        EXPECT_LE(f.len, rp::CoordinatorLink::kCoordFrameCap);
        const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(f), f.len);
        ASSERT_TRUE(m);
        EXPECT_TRUE(m->hasMemberBlock) << "a request of the split went out without the member";
        EXPECT_EQ(m->generationId, 7);
        EXPECT_EQ(std::string(m->memberId.begin(), m->memberId.end()), member);
        for (const auto& e : m->entries) {
            ASSERT_LT(total, offs.size());
            EXPECT_EQ(e.offset, offs[total].offset);
            ++total;
        }
    }
    EXPECT_EQ(total, offs.size());
    for (const auto& f : commits) r.answerFrame(f, rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::Ok);
}

TEST(ProducerTxnOffsetsRegression, TheGroupIdFormAndANegativeGenerationSendNoMemberBlockRegression) {
    for (const int32_t generation : {-1, -5}) {
        Rig r(txnCfg(), 1 << 20);
        ringReady(r);
        Token t = 0;
        ASSERT_EQ(issueWithMember(r, t, offsetsOf(2), "grp-l", generation, "ignored"), rc::OpResult::Accepted);
        r.tick();
        r.tick();
        r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
        r.tick();
        const auto f = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
        const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(f), f.len);
        ASSERT_TRUE(m);
        EXPECT_FALSE(m->hasMemberBlock) << "generation " << generation;
        EXPECT_EQ(m->entries.size(), 2u);
    }
}

TEST(ProducerTxnOffsetsRegression, AFencedMemberIsAbortableAndNeverRetriedRegression) {
    for (const rpc::ErrorCode code : {rpc::ErrorCode::IllegalGeneration, rpc::ErrorCode::UnknownMember}) {
        Rig r(txnCfg(), 1 << 20);
        ringReady(r);
        Token t = 0;
        ASSERT_EQ(issueWithMember(r, t, offsetsOf(2), "grp-f", 3, "m"), rc::OpResult::Accepted);
        r.tick();
        r.tick();
        r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
        r.tick();
        r.answerFrame(r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID), rp::ReqKind::TxnOffsetCommit, ec(code));
        ASSERT_EQ(r.d(t).times, 1);
        EXPECT_EQ(r.d(t).status, OpStatus::BrokerError);
        EXPECT_EQ(r.d(t).code, uec(code));
        EXPECT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError) << "the transaction must abort (the app rejoins)";
        for (int i = 0; i < 32; ++i) r.tick(10 * kMs);
        EXPECT_EQ(r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID).size(), 1u) << "a fenced member was re-sent";
        EXPECT_EQ(rp::CoordinatorLink::classify(rp::ReqKind::TxnOffsetCommit, ec(code)), rp::Verdict::Abortable);
    }
}

TEST(ProducerTxnOffsetsRegression, AMemberIdOverTheCapCompletesInvalidArgumentRegression) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    const std::size_t framesBefore = r.cap.frames.size();
    const std::string longMember(rp::kMaxTxnMemberIdBytes + 1, 'm');
    Token t = 0;
    ASSERT_EQ(issueWithMember(r, t, offsetsOf(2), "grp", 1, longMember), rc::OpResult::Accepted);
    r.tick();
    ASSERT_EQ(r.d(t).times, 1);
    EXPECT_EQ(r.d(t).status, OpStatus::InvalidArgument);
    EXPECT_EQ(r.cap.frames.size(), framesBefore);
    // At the cap exactly: accepted and carried whole.
    const std::string maxMember(rp::kMaxTxnMemberIdBytes, 'x');
    Token ok = 0;
    ASSERT_EQ(issueWithMember(r, ok, offsetsOf(1), "grp", 1, maxMember), rc::OpResult::Accepted);
    r.tick();
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto f = r.lastOf(rpc::TxnOffsetCommit::TEMPLATE_ID);
    const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(f), f.len);
    ASSERT_TRUE(m && m->hasMemberBlock);
    EXPECT_EQ(m->memberId.size(), rp::kMaxTxnMemberIdBytes);
}

// #350: the widest legal control message through every txn control op that rides writeTxnControl_.  Only SendOffsets
// carries variable fields; the others are header-only and are issued behind it so the ring holds the widest message
// next to the narrowest.  Every byte of the group id and the member id differs from its neighbours, so an overflow, a
// shifted field or a clobbered tail shows as a mismatch in the frames the coordinator receives.
// RED (mutation): `alignas(8) uint8_t buf[kTxnCtlMaxBytes - 64]` in ProducerImpl::writeTxnControl_.
TEST(ProducerTxnControlRegression, MaxSizeControlMessageIsEncodedWithoutOverflowRegression) {
    Rig r(txnCfg(), 1 << 20);
    ringReady(r);
    std::string group(rp::kMaxTxnGroupIdBytes, 'g');
    for (std::size_t i = 0; i < group.size(); ++i) group[i] = static_cast<char>('a' + (i * 7) % 26);
    std::string member(rp::kMaxTxnMemberIdBytes, 'm');
    for (std::size_t i = 0; i < member.size(); ++i) member[i] = static_cast<char>('A' + (i * 11) % 26);
    const auto offs = offsetsOf(static_cast<int>(rp::kMaxTxnOffsets));
    ASSERT_EQ(offs.size(), rp::kMaxTxnOffsets);

    // The widest: group id, member id and offsets all at their caps (the member form and the legacy form).
    Token wide = 0;
    ASSERT_EQ(issueWithMember(r, wide, offs, group, 0x7fffffff, member), rc::OpResult::Accepted);
    r.tick();
    r.tick();
    const auto adds = r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID);
    ASSERT_EQ(adds.size(), 1u);
    const auto add = rpc::AddOffsetsToTxn::decode(r.cap.data(adds[0]), adds[0].len);
    ASSERT_TRUE(add);
    EXPECT_EQ(std::string(add->groupId.begin(), add->groupId.end()), group);
    r.answerFrame(adds[0], rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_GE(commits.size(), 2u) << "1024 entries are split across requests (FD5)";
    std::size_t total = 0;
    for (const auto& f : commits) {
        const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(f), f.len);
        ASSERT_TRUE(m);
        EXPECT_EQ(std::string(m->groupId.begin(), m->groupId.end()), group);
        EXPECT_TRUE(m->hasMemberBlock);
        EXPECT_EQ(m->generationId, 0x7fffffff);
        EXPECT_EQ(std::string(m->memberId.begin(), m->memberId.end()), member);
        for (const auto& e : m->entries) {
            ASSERT_LT(total, offs.size());
            EXPECT_EQ(e.topicId, offs[total].topicId) << total;
            EXPECT_EQ(e.partition, offs[total].partition) << total;
            EXPECT_EQ(e.offset, offs[total].offset) << total;
            ++total;
        }
    }
    EXPECT_EQ(total, offs.size()) << "every offset of the widest message reached the coordinator, in order";
    for (const auto& f : commits) r.answerFrame(f, rp::ReqKind::TxnOffsetCommit, 0);
    ASSERT_EQ(r.d(wide).times, 1);
    EXPECT_EQ(r.d(wide).status, OpStatus::Ok);

    // The legacy (group id only) widest form, then every header-only kind behind it in the same ring.
    Token legacy = 0;
    ASSERT_EQ(issueWithMember(r, legacy, offs, group, -1, {}), rc::OpResult::Accepted);
    const auto fl = r.issue(TxnControl::Kind::Flush);
    ASSERT_EQ(fl.result, rc::OpResult::Accepted);
    r.tick();
    r.tick();
    if (r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size() > 1) {   // a second AddOffsets for the second call
        r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
        r.tick();
    }
    const auto commits2 = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_GT(commits2.size(), commits.size());
    std::size_t total2 = 0;
    for (std::size_t k = commits.size(); k < commits2.size(); ++k) {
        const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(commits2[k]), commits2[k].len);
        ASSERT_TRUE(m);
        EXPECT_EQ(std::string(m->groupId.begin(), m->groupId.end()), group);
        EXPECT_FALSE(m->hasMemberBlock);
        for (const auto& e : m->entries) {
            ASSERT_LT(total2, offs.size());
            EXPECT_EQ(e.offset, offs[total2].offset) << total2;
            ++total2;
        }
    }
    EXPECT_EQ(total2, offs.size());
    for (std::size_t k = commits.size(); k < commits2.size(); ++k) {
        r.answerFrame(commits2[k], rp::ReqKind::TxnOffsetCommit, 0);
    }
    EXPECT_EQ(r.d(legacy).times, 1);
    EXPECT_EQ(r.d(legacy).status, OpStatus::Ok);
    r.tick();
    EXPECT_EQ(r.d(fl.token).times, 1);
    EXPECT_EQ(r.d(fl.token).status, OpStatus::Ok);
    const auto commit = r.issue(TxnControl::Kind::Commit);
    ASSERT_EQ(commit.result, rc::OpResult::Accepted);
    r.tick();
    r.tick();
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    EXPECT_EQ(r.d(commit.token).times, 1);
    EXPECT_EQ(r.d(commit.token).status, OpStatus::Ok);
    // The remaining header-only kinds ride the same writer next to the widest message's leftovers: each is written,
    // applied and completed exactly once (the machine is Ready: both are refused 19, none is lost or garbled).
    for (const TxnControl::Kind k : {TxnControl::Kind::Abort, TxnControl::Kind::Init}) {
        const auto c = r.issue(k);
        ASSERT_EQ(c.result, rc::OpResult::Accepted);
        r.tick();
        EXPECT_EQ(r.d(c.token).times, 1) << static_cast<int>(k);
        EXPECT_EQ(r.d(c.token).status, OpStatus::BrokerError) << static_cast<int>(k);
        EXPECT_EQ(r.d(c.token).code, kInvalidTxnState) << static_cast<int>(k);
    }
}

// ============================================================================================================
// C15 review - adversarial pass over #347 / #348 (every txn state x an empty / a second sendOffsets)
// ============================================================================================================

// #348 x protocol state: an EMPTY sendOffsets is Ok locally only inside a good transaction.  Every other state answers
// as a non-empty call would (Uninitialized / Ready / Committing / Aborting: 19; AbortableError: the poison), and an
// invalid group is still InvalidArgument (validation before emptiness).  No state sends anything for it.
// RED (mutation): the empty-offsets early return moved ahead of the state gates in applyTxnControl (Ok in Ready).
TEST(ProducerTxnOffsetsRegression, EmptySendOffsetsInEveryTxnStateAdversarialRegression) {
    Rig r;
    auto expect19 = [&](Token t, const char* where) {
        ASSERT_EQ(r.d(t).times, 1) << where;
        EXPECT_EQ(r.d(t).status, OpStatus::BrokerError) << where;
        EXPECT_EQ(r.d(t).code, kInvalidTxnState) << where;
    };
    expect19(r.sendOffsets({}, "grp"), "Uninitialized");
    r.ready();
    expect19(r.sendOffsets({}, "grp"), "Ready");
    r.begin();
    for (const std::string& bad : {std::string(), std::string(rp::kMaxTxnGroupIdBytes + 1, 'g')}) {
        const Token t = r.sendOffsets({}, bad);
        ASSERT_EQ(r.d(t).times, 1);
        EXPECT_EQ(r.d(t).status, OpStatus::InvalidArgument) << "group of " << bad.size() << " bytes";
    }
    const Token ok = r.sendOffsets({}, std::string(rp::kMaxTxnGroupIdBytes, 'g'));
    ASSERT_EQ(r.d(ok).times, 1);
    EXPECT_EQ(r.d(ok).status, OpStatus::Ok) << "a max-length group is valid";
    // Committing (EndTxn on the wire).
    const Token a = r.sendOffsets(offsetsOf(1));
    r.sendOk(a);
    ASSERT_EQ(r.d(a).status, OpStatus::Ok);
    r.apply(TxnControl::Kind::Commit);
    r.tick();
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::CommittingTransaction);
    expect19(r.sendOffsets({}, "grp"), "Committing");
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    // Aborting.
    r.begin();
    const Token b = r.sendOffsets(offsetsOf(1));
    r.sendOk(b);
    r.apply(TxnControl::Kind::Abort);
    r.tick();
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::AbortingTransaction);
    expect19(r.sendOffsets({}, "grp"), "Aborting");
    r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    // AbortableError: the poison, not Ok.
    r.begin();
    r.impl->testPoison(OpStatus::BrokerError, 77, rp::AbortKind::Plain);
    const Token p = r.sendOffsets({}, "grp");
    ASSERT_EQ(r.d(p).times, 1);
    EXPECT_EQ(r.d(p).status, OpStatus::BrokerError) << "an empty call does not hide the poison";
    EXPECT_EQ(r.d(p).code, 77u);
    r.tick(10 * kMs);
    EXPECT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 2u) << "only the two real calls sent AddOffsets";
}

// #698: one sendOffsets at a time applies to an EMPTY second call too (the in-flight check runs before the empty-list
// shortcut): InvalidArgument, no frame; the first one is untouched and completes Ok when answered.  An empty call with
// nothing in flight is still Ok locally.
// RED (mutation): the empty-list check moved back ahead of the in-flight check (the empty call completes Ok).
TEST(ProducerTxnOffsetsRegression, AnEmptySendOffsetsWhileOneIsInFlightIsInvalidArgumentRegression) {
    Rig r;
    r.inTxn();
    const Token first = r.sendOffsets(offsetsOf(2), "grp-a");
    r.tick();   // AddOffsetsToTxn on the wire, unanswered: the first call is in flight
    ASSERT_EQ(r.cap.of(rpc::AddOffsetsToTxn::TEMPLATE_ID).size(), 1u);
    const std::size_t framesBefore = r.cap.frames.size();
    const Token second = r.sendOffsets({}, "grp-a");
    ASSERT_EQ(r.d(second).times, 1);
    EXPECT_EQ(r.d(second).status, OpStatus::InvalidArgument) << "a second call, even an empty one, is refused";
    r.tick();
    EXPECT_EQ(r.cap.frames.size(), framesBefore) << "the refusal sends nothing";
    EXPECT_EQ(r.d(first).times, 0);
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    for (const auto& f : r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID)) {
        if (r.d(first).times == 0) r.answerFrame(f, rp::ReqKind::TxnOffsetCommit, 0);
    }
    ASSERT_EQ(r.d(first).times, 1);
    EXPECT_EQ(r.d(first).status, OpStatus::Ok);
    EXPECT_EQ(r.d(second).times, 1) << "the refused call is never completed again";
    EXPECT_EQ(r.impl->txnStateDuty(), TxnState::InTransaction) << "the transaction is not poisoned";
    const Token third = r.sendOffsets({}, "grp-a");
    ASSERT_EQ(r.d(third).times, 1);
    EXPECT_EQ(r.d(third).status, OpStatus::Ok) << "nothing in flight: an empty list completes Ok locally";
}

// #347 x concurrency: the same refused call repeated while the first runs, then the first FAILS.  Each refusal
// completes once with InvalidArgument and is never touched again; the first's failure alone poisons the transaction
// (its code, not the refusal's); a later call gets the poison; after the abort a new transaction's call runs (the
// scratch was freed).  RED (mutation): the in-flight check after the scratch copy (the refused call overwrites the
// running call's group: the TxnOffsetCommit names grp-b).
TEST(ProducerTxnOffsetsRegression, RefusedSecondCallsDuringAFailingFirstKeepOnlyTheFirstsOutcomeAdversarialRegression) {
    Rig r;
    r.inTxn();
    const Token a = r.sendOffsets(offsetsOf(2), "grp-a");
    std::vector<Token> refused;
    for (int i = 0; i < 3; ++i) {
        refused.push_back(r.sendOffsets(offsetsOf(1), "grp-b"));
        ASSERT_EQ(r.d(refused.back()).times, 1);
        EXPECT_EQ(r.d(refused.back()).status, OpStatus::InvalidArgument);
    }
    r.tick();
    r.answerFrame(r.lastOf(rpc::AddOffsetsToTxn::TEMPLATE_ID), rp::ReqKind::AddOffsets, 0);
    r.tick();
    const auto commits = r.cap.of(rpc::TxnOffsetCommit::TEMPLATE_ID);
    ASSERT_EQ(commits.size(), 1u);
    const auto m = rpc::TxnOffsetCommit::decode(r.cap.data(commits[0]), commits[0].len);
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(m->groupId.data()), m->groupId.size()), "grp-a")
        << "the refused calls never touched the running call's scratch";
    r.answerFrame(commits[0], rp::ReqKind::TxnOffsetCommit, ec(rpc::ErrorCode::NotAuthorized));
    ASSERT_EQ(r.d(a).times, 1);
    EXPECT_EQ(r.d(a).status, OpStatus::BrokerError);
    EXPECT_EQ(r.d(a).code, uec(rpc::ErrorCode::NotAuthorized));
    ASSERT_EQ(r.impl->txnStateDuty(), TxnState::AbortableError);
    for (const Token t : refused) {
        EXPECT_EQ(r.d(t).times, 1) << "a refused call completed again";
        EXPECT_EQ(r.d(t).status, OpStatus::InvalidArgument);
    }
    const Token late = r.sendOffsets(offsetsOf(1), "grp-c");
    ASSERT_EQ(r.d(late).times, 1);
    EXPECT_EQ(r.d(late).code, uec(rpc::ErrorCode::NotAuthorized)) << "a call after the failure gets the poison";
    const Token ab = r.apply(TxnControl::Kind::Abort);
    r.tick();
    r.tick();
    if (!r.cap.of(rpc::EndTxn::TEMPLATE_ID).empty()) {
        r.answerFrame(r.lastOf(rpc::EndTxn::TEMPLATE_ID), rp::ReqKind::EndTxn, 0);
    }
    ASSERT_EQ(r.d(ab).times, 1);
    EXPECT_EQ(r.d(ab).status, OpStatus::Ok);
    r.begin();
    const Token d = r.sendOffsets(offsetsOf(1), "grp-d");
    EXPECT_EQ(r.d(d).times, 0) << "the next transaction's call runs";
    r.sendOk(d);
    ASSERT_EQ(r.d(d).times, 1);
    EXPECT_EQ(r.d(d).status, OpStatus::Ok);
}
