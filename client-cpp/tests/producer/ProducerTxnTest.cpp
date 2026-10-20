// tests/producer/ProducerTxnTest.cpp - Phase 2b F Task 5: the transaction state machine inside ProducerImpl
// (duty side): init / begin / commit / abort / flush / close / fencing.
//
// A bare transactional ProducerImpl wired to a FakeCoordinator through a ProducerHub (the Client's wiring,
// minus the transport).  The test thread IS the duty thread: every op is applied through applyTxnControl, every
// pass is hub.drainAndFlush on an injected duty clock (no sleeps, no threads, no atomics), completions land in a
// handler ledger that counts them per token (exactly once is asserted for every token at teardown).
//
// Ported from the removed transactional-producer test (lifecycle-only cases; the data side - AddPartitions, records in a
// transaction - is Task 6, offsets Task 7):
//   InitTransactionsAcquiresPidEpoch                       -> InitAssignsIdAndEpochAndGoesReady
//   CommitTransactionEmitsEndTxnCommit                      -> CommitTransactionEmitsEndTxnCommit
//   EndTxnDeferredUntilAllProduceBatchesAcked               -> CommitWaitsForDataThenSendsEndTxnCommit
//   AbortTransactionEmitsEndTxnAbort                        -> AbortSendsEndTxnAbort
//   EndTxnTimeoutStaysInDoubtAndIsRetriedRegression         -> CommitTimeoutIsInDoubtAndARetriedCommitResendsEndTxn
//   EmptyTransactionSendsNoEndTxnRegression                 -> BeginCommitOfAnEmptyTransactionEndsLocallyWithoutEndTxn
//   LostBatchAbortBumpsEpochAndRestartsSequencesRegression  -> SequenceGapAbortBumpsTheEpoch
//   EndTxnRetriesConcurrentTransactionsUntilTerminalRegression -> ConcurrentTransactionsOnEndTxnRetriesUntilFinal
//   FencedProducerRefusesInitRegression                     -> FencedByASecondProducerFailsEverythingRegression
//   TxnCloseAbortsOpenBrokerTransactionBeforeReportingClosedRegression -> CloseAbortsAnOpenTransactionFirstRegression
//   TransientLogFullBackoffIsWallClockTimeRegression        -> TransientLogFullBackoffIsWallClockTimeRegression
//   (same names) EndTxnNotAuthorizedFailsTheCommitWithoutRetryRegression, InitTxnFencedCompletesFencedRegression,
//   FlushWithNothingPendingCompletesAtOnce, FlushDuringCommitSendsOneEndTxn, AbortWhileAbortingJoinsThePendingAbort-
//   Regression, NonRetriableInitProducerIdErrorIsFatalRegression, EndTxnDeadlineStartsAtItsCreationRegression,
//   BeginWhileCommittingIsRefusedRegression, CommitAbortCompleteOnceRegression, InitProducerIdRetriesConcurrent-
//   TransactionsRegression, ConcurrentTransactionsRetryIsBoundedOnlyByTheOpDeadlineRegression, NotCoordinator-
//   RediscoversThenRetriesRegression (init + EndTxn legs), InFlightRequestIsResentOnANewSessionRegression,
//   InDoubtBumpRetryResendsOnlyTheBumpRegression, LateOriginalEndTxnAnswerAfterRetryIsIgnoredRegression,
//   TxnCloseWithNothingOpenClosesAtOnceRegression, TxnCloseWaitsForARunningCommitRegression,
//   TxnCloseReportsAFailedAbortAndClosesAnywayRegression, TxnCloseImmediatelyDiscardsLocallyRegression,
//   EndTxnInvalidRequestFailsAtOnceNotRetriedRegression, InitProducerIdInvalidRequestCompletesWithErrorRegression,
//   PoisonedTransactionMustBeAbortedBeforeTheNextRegression (lifecycle part),
//   TimedOutAddPartitionsAbortsPossiblyStartedTxnRegression (the EndTxn half: TxnStarted::Maybe).
#include "../support/AckLedger.h"
#include "../support/FakeCoordinator.h"

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace rc  = rune::client;
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
namespace rcc = rune::client::codec;
using rc::Acks;
using rc::Key;
using rc::OpKind;
using rc::OpStatus;
using rc::Token;
using rc::TxnState;
using rp::ProducerImpl;
using rp::TxnControl;
using test_support::AckLedger;
using test_support::AnswerFrame;
using test_support::FakeCoordinator;
using test_support::OfferedFrame;

namespace {

constexpr int64_t kMs = 1'000'000;
constexpr int64_t kT0 = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x5A);

constexpr int16_t ec(rpc::ErrorCode e) { return static_cast<int16_t>(e); }
constexpr uint16_t uec(rpc::ErrorCode e) { return static_cast<uint16_t>(e); }
constexpr uint16_t kInvalidTxnState = static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState);

rc::ProducerConfig txnCfg(std::string id = "txn-1") {
    rc::ProducerConfig c;
    c.acks              = Acks::All;
    c.lingerUs          = 0;
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = 5;
    c.produceTimeoutMs  = 10'000;
    c.deliveryTimeoutMs = 20'000;
    c.transactionOpTimeoutMs = 1'000;
    c.transactionalId   = std::move(id);
    c.transactionTimeoutMs = 30'000;
    return c;
}

/// Completions per token (exactly-once is checked at teardown).
struct Ledger {
    struct Entry { int times = 0; rc::Completion c{}; std::string text; };
    std::map<Token, Entry> e;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t> p) noexcept {
        auto& x = static_cast<Ledger*>(ctx)->e[c.token];
        ++x.times;
        x.c = c;
        x.text.assign(reinterpret_cast<const char*>(p.data()), p.size());
    }
};

struct Op {
    Ledger* l = nullptr;
    Token   t = 0;
    [[nodiscard]] bool ready() const { return l->e.count(t) != 0; }
    [[nodiscard]] int  times() const { return ready() ? l->e.at(t).times : 0; }
    [[nodiscard]] OpStatus status() const { return l->e.at(t).c.status; }
    [[nodiscard]] const rc::Completion& c() const { return l->e.at(t).c; }
    [[nodiscard]] uint16_t code() const { return l->e.at(t).c.errorCode; }
};

struct Wire {
    int32_t partition = 0;
    int64_t pid       = 0;
    int16_t epoch     = 0;
    int32_t baseSeq   = 0;
    int64_t corrId    = 0;
};

AnswerFrame initAnswer(int64_t corr, int64_t pid, int16_t epoch, int16_t code = 0) {
    AnswerFrame a;
    rpc::InitProducerIdResponse m;
    m.correlationId = corr;
    m.producerId    = code == 0 ? pid : -1;
    m.producerEpoch = epoch;
    m.errorCode     = code;
    a.len        = m.encode(a.buf.data());
    a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
    return a;
}

/// A transactional producer wired to a fake coordinator through a hub; the test thread is the duty thread.
struct Rig {
    FakeCoordinator               fake;
    rp::ProducerHub               hub;
    std::unique_ptr<ProducerImpl> impl;
    Ledger                        ledger;
    std::vector<Wire>             wire;
    ProducerImpl::SendFn          sink;
    int64_t                       now  = kT0;
    Token                         next = 0;
    rc::ProducerConfig            cfg;

    explicit Rig(rc::ProducerConfig c = txnCfg(), bool direct = false) : cfg(c) {
        hub.setCoordinatorSink(fake.sink());
        impl = std::make_unique<ProducerImpl>(cfg, 1 << 16);
        if (direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
        hub.add(impl.get());
        sink = [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            const auto h = rpc::MessageHeader::decode(b, n);
            wire.push_back({p, r.producerId(), r.producerEpoch(), r.baseSequence(), h ? h->correlationId : 0});
            return true;
        };
    }
    ~Rig() {
        for (const auto& [t, x] : ledger.e) EXPECT_EQ(x.times, 1) << "token " << t << " completed " << x.times;
        hub.remove(impl.get());
    }

    // ---- ops (applied on the duty thread, as the ring drain / an Invoker owner does) ----
    TxnControl ctl(TxnControl::Kind k, Token t) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&Ledger::on, &ledger};
        c.token          = t;
        return c;
    }
    Op apply(TxnControl::Kind k) {
        const Token t = ++next;
        impl->applyTxnControl(ctl(k, t), now);
        return Op{&ledger, t};
    }
    Op init() { return apply(TxnControl::Kind::Init); }
    Op commit() { return apply(TxnControl::Kind::Commit); }
    Op abort() { return apply(TxnControl::Kind::Abort); }
    Op flush() { return apply(TxnControl::Kind::Flush); }
    Op sendOffsets() { return apply(TxnControl::Kind::SendOffsets); }
    /// The void beginTransaction(): no target, never completes (a refused one is counted).
    void begin() {
        TxnControl c;
        c.kind = TxnControl::Kind::Begin;
        impl->applyTxnControl(c, now);
    }
    Op close() {
        const Token t = ++next;
        rc::detail::OpTarget target;
        target.handler = rc::CompletionHandler{&Ledger::on, &ledger};
        (void)impl->closeIntake();   // the issuer closes the intake (Producer::close), the duty side drains
        impl->applyBeginClose(target, t, now);
        return Op{&ledger, t};
    }

    // ---- duty passes ----
    int tick(int64_t advanceNs = 0) {
        now += advanceNs;
        return hub.drainAndFlush(now, 42, sink);
    }
    void ticks(int n, int64_t advanceNs = 0) { for (int i = 0; i < n; ++i) tick(advanceNs); }
    void answer(const AnswerFrame& f) { ASSERT_TRUE(hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now)); }
    /// An answer whose request is gone (late / duplicate): routed (ours), changes nothing.
    void answerLate(const AnswerFrame& f) { (void)hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now); }

    // ---- what the coordinator saw ----
    [[nodiscard]] std::size_t count(uint16_t tid) const {
        std::size_t n = 0;
        for (const auto& f : fake.offered) n += f.templateId == tid ? 1 : 0;
        return n;
    }
    [[nodiscard]] std::size_t inits() const { return count(rpc::InitProducerId::TEMPLATE_ID); }
    [[nodiscard]] std::size_t endTxns() const { return count(rpc::EndTxn::TEMPLATE_ID); }
    [[nodiscard]] const OfferedFrame* last(uint16_t tid) const {
        for (std::size_t i = fake.offered.size(); i-- > 0;) {
            if (fake.offered[i].templateId == tid) return &fake.offered[i];
        }
        return nullptr;
    }
    [[nodiscard]] int64_t lastInitCorr() const { return last(rpc::InitProducerId::TEMPLATE_ID)->corrId; }
    [[nodiscard]] int64_t lastEndCorr() const { return last(rpc::EndTxn::TEMPLATE_ID)->corrId; }
    [[nodiscard]] std::optional<rpc::EndTxn> lastEnd() const {
        const OfferedFrame* f = last(rpc::EndTxn::TEMPLATE_ID);
        if (f == nullptr) return std::nullopt;
        return rpc::EndTxn::decode(f->head.data(), std::min<std::size_t>(f->len, f->head.size()));
    }
    void endAnswer(int16_t code = 0) { answer(FakeCoordinator::answer(rp::ReqKind::EndTxn, lastEndCorr(), code)); }

    /// initTransactions through a full InitProducerId round trip.
    void initOk(int64_t pid = 42, int16_t epoch = 0) {
        Op op = init();
        tick();
        ASSERT_NE(last(rpc::InitProducerId::TEMPLATE_ID), nullptr);
        answer(initAnswer(lastInitCorr(), pid, epoch));
        ASSERT_TRUE(op.ready());
        ASSERT_EQ(op.status(), OpStatus::Ok);
    }
    /// begin, and the transaction counts as started on the coordinator (Task 6 / 7 legs mark it).
    void startTxn(rp::TxnStarted s = rp::TxnStarted::Yes) {
        begin();
        impl->testMarkStarted(s);
    }
    /// One record through the producer's own data path (the gate opened as the handle does at begin).  Ring
    /// mode: drained into its builder at once - a record ahead of a control op in the producer ring is drained
    /// before the op applies (FD1), which is the order the rig's direct applyTxnControl stands in for.
    bool sendRecord(int32_t part = 0) {
        impl->setTxnGate(ProducerImpl::TxnGate::Open);
        if (impl->ingressMode() == ProducerImpl::IngressMode::Direct) {
            return impl->trySubmit(1, part, Key(uint64_t{1}), kVal, 0) == rc::SendResult::Ok;
        }
        if (!impl->submitSendRecord(1, part, Key(uint64_t{1}), kVal, false, 0)) return false;
        return impl->drainRing(now, 16) == 1;
    }
    /// Task 6: the partitions of the records sent so far join the transaction - their AddPartitionsToTxn is
    /// answered OK (the data is held until then).
    void addOk() {
        for (int i = 0; i < 3 && pendingAdd() == nullptr; ++i) tick();
        const OfferedFrame* f = pendingAdd();
        ASSERT_NE(f, nullptr) << "no AddPartitionsToTxn for the transaction's records";
        answeredAdds.push_back(f->corrId);
        answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, f->corrId, 0));
    }
    [[nodiscard]] const OfferedFrame* pendingAdd() const {
        for (const auto& f : fake.offered) {
            if (f.templateId == rpc::AddPartitionsToTxn::TEMPLATE_ID &&
                std::find(answeredAdds.begin(), answeredAdds.end(), f.corrId) == answeredAdds.end()) {
                return &f;
            }
        }
        return nullptr;
    }
    std::vector<int64_t> answeredAdds;
    void ackLastData(int16_t code = 0, int64_t base = 0) {
        ASSERT_FALSE(wire.empty());
        hub.onProduceResponse(wire.back().corrId, code == 0 ? base : -1, code);
    }
    [[nodiscard]] TxnState state() const { return impl->txnStateDuty(); }
};

}  // namespace

// ============================================================================================================
// CLAUDE.md #3: every state holds exactly its own variables - no scattered booleans.
// ============================================================================================================
namespace nobool {
/// Converts to anything: counts an aggregate's direct members (brace-init with N of these).
struct Any {
    template <class U>
    operator U() const noexcept;
};
template <class T>
constexpr std::size_t fields() {
    if constexpr (requires { T{Any{}, Any{}, Any{}, Any{}, Any{}, Any{}}; }) return 6;
    else if constexpr (requires { T{Any{}, Any{}, Any{}, Any{}, Any{}}; }) return 5;
    else if constexpr (requires { T{Any{}, Any{}, Any{}, Any{}}; }) return 4;
    else if constexpr (requires { T{Any{}, Any{}, Any{}}; }) return 3;
    else if constexpr (requires { T{Any{}, Any{}}; }) return 2;
    else if constexpr (requires { T{Any{}}; }) return 1;
    else return 0;
}
template <class M>
constexpr bool notBool() { return !std::is_same_v<std::remove_cvref_t<M>, bool>; }
template <class T>
constexpr bool noBoolMembers() {
    constexpr std::size_t n = fields<T>();
    static_assert(n <= 6, "extend the binder below");
    // Structured bindings name each member's declared type (decltype of a binding).
    constexpr auto check = [](T& t) {
        if constexpr (n == 0) {
            return true;
        } else if constexpr (n == 1) {
            auto& [a] = t;
            return notBool<decltype(a)>();
        } else if constexpr (n == 2) {
            auto& [a, b] = t;
            return notBool<decltype(a)>() && notBool<decltype(b)>();
        } else if constexpr (n == 3) {
            auto& [a, b, c] = t;
            return notBool<decltype(a)>() && notBool<decltype(b)>() && notBool<decltype(c)>();
        } else if constexpr (n == 4) {
            auto& [a, b, c, d] = t;
            return notBool<decltype(a)>() && notBool<decltype(b)>() && notBool<decltype(c)>() &&
                   notBool<decltype(d)>();
        } else if constexpr (n == 5) {
            auto& [a, b, c, d, e] = t;
            return notBool<decltype(a)>() && notBool<decltype(b)>() && notBool<decltype(c)>() &&
                   notBool<decltype(d)>() && notBool<decltype(e)>();
        } else {
            auto& [a, b, c, d, e, f] = t;
            return notBool<decltype(a)>() && notBool<decltype(b)>() && notBool<decltype(c)>() &&
                   notBool<decltype(d)>() && notBool<decltype(e)>() && notBool<decltype(f)>();
        }
    };
    T t{};
    return check(t);
}
template <class V, std::size_t... I>
constexpr bool everyAlternative(std::index_sequence<I...>) {
    return (noBoolMembers<std::variant_alternative_t<I, V>>() && ...);
}
template <class V>
constexpr bool everyAlternative() { return everyAlternative<V>(std::make_index_sequence<std::variant_size_v<V>>{}); }
}  // namespace nobool

TEST(ProducerTxn, MachineStatesCarryNoBooleans) {
    static_assert(nobool::everyAlternative<rp::TxnMachine>(), "a TxnMachine state carries a bool member");
    static_assert(nobool::everyAlternative<rp::EndPhase>(), "an EndPhase carries a bool member");
    static_assert(nobool::noBoolMembers<rp::TxnOp>(), "TxnOp carries a bool member");
    static_assert(nobool::noBoolMembers<rp::TxnJoiners>(), "TxnJoiners carries a bool member");
    static_assert(nobool::everyAlternative<rp::TxnMembership>(), "a TxnMembership state carries a bool member");
    static_assert(nobool::noBoolMembers<rp::TxnOutcome>(), "TxnOutcome carries a bool member");
    static_assert(std::variant_size_v<rp::TxnMachine> == static_cast<std::size_t>(TxnState::FatalError) + 1);
    SUCCEED();
}

// ============================================================================================================
// init
// ============================================================================================================

TEST(ProducerTxn, InitAssignsIdAndEpochAndGoesReady) {
    Rig r;
    EXPECT_EQ(r.state(), TxnState::Uninitialized);
    EXPECT_EQ(r.impl->producerId(), -1) << "no id before initTransactions";
    r.ticks(4, kMs);
    EXPECT_TRUE(r.fake.offered.empty()) << "a transactional producer asks for its id only on initTransactions";
    Op op = r.init();
    EXPECT_EQ(r.state(), TxnState::Initializing);
    r.tick();
    ASSERT_EQ(r.inits(), 1u);
    const auto req = rpc::InitProducerId::decode(r.fake.offered[0].head.data(), r.fake.offered[0].len);
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(req->transactionalId.data()), req->transactionalId.size()),
              "txn-1") << "initTransactions sends the transactional id";
    EXPECT_EQ(req->transactionTimeoutMs, 30'000);
    EXPECT_TRUE(std::holds_alternative<rp::IdRequested>(r.impl->identity())) << "the identity machine is reused";
    EXPECT_FALSE(op.ready());
    r.answer(initAnswer(r.lastInitCorr(), 42, 1));
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(op.c().op, OpKind::InitTransactions);
    EXPECT_EQ(op.c().value, 42) << "value = producerId";
    EXPECT_EQ(op.c().aux, 1) << "aux = producerEpoch";
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_EQ(r.impl->producerId(), 42);
    EXPECT_EQ(r.impl->producerEpoch(), 1);
    // FD9: the status ring carries the identity and the state to the bound app thread.
    EXPECT_EQ(r.impl->appProducerId(), 42);
    EXPECT_EQ(r.impl->appTxnState(), TxnState::Ready);
    EXPECT_FALSE(r.impl->appTxnActive());
    r.begin();
    EXPECT_EQ(r.impl->appTxnState(), TxnState::InTransaction);
    EXPECT_TRUE(r.impl->appTxnActive());
}

// Fix round 1 (I-2): InitProducerId{ProducerFenced} completes the init Fenced and the producer is FatalError -
// terminal.  Misuse while Initializing is refused at once (19); after the fence every op completes Fenced.
TEST(ProducerTxn, InitTxnFencedCompletesFencedRegression) {
    Rig r;
    Op init = r.init();
    ASSERT_EQ(r.state(), TxnState::Initializing);
    Op offs   = r.sendOffsets();
    Op commit = r.commit();
    Op init2  = r.init();
    for (Op* op : {&offs, &commit, &init2}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::BrokerError);
        EXPECT_EQ(op->code(), kInvalidTxnState);
    }
    r.tick();
    ASSERT_EQ(r.inits(), 1u) << "the refused init sent nothing";
    const AnswerFrame fenced = initAnswer(r.lastInitCorr(), -1, 0, ec(rpc::ErrorCode::ProducerFenced));
    r.answer(fenced);
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::Fenced);
    r.answerLate(fenced);   // late duplicate: ignored
    EXPECT_EQ(init.times(), 1);
    EXPECT_EQ(r.impl->producerId(), -1);
    EXPECT_EQ(r.state(), TxnState::FatalError);
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
}

// Fix round 2 (m3, Kafka InitProducerIdHandler): a non-retriable InitProducerId error is fatal.
TEST(ProducerTxn, NonRetriableInitProducerIdErrorIsFatalRegression) {
    Rig r;
    Op init = r.init();
    r.tick();
    r.answer(initAnswer(r.lastInitCorr(), -1, 0, ec(rpc::ErrorCode::Internal)));
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::BrokerError);
    EXPECT_EQ(init.code(), uec(rpc::ErrorCode::Internal));
    EXPECT_EQ(r.state(), TxnState::FatalError);
    Op again = r.init();
    ASSERT_TRUE(again.ready());
    EXPECT_EQ(again.status(), OpStatus::BrokerError) << "FatalError: every op completes with its status";
    EXPECT_EQ(again.code(), uec(rpc::ErrorCode::Internal));
}

TEST(ProducerTxn, InitProducerIdInvalidRequestCompletesWithErrorRegression) {
    Rig r;
    Op init = r.init();
    r.tick();
    r.answer(initAnswer(r.lastInitCorr(), -1, 0, ec(rpc::ErrorCode::InvalidRequest)));
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::BrokerError);
    EXPECT_EQ(init.code(), uec(rpc::ErrorCode::InvalidRequest));
    r.ticks(16, 10 * kMs);
    EXPECT_EQ(r.inits(), 1u) << "re-sent a request the broker cannot decode";
}

// F14 / FD5: 27 on InitProducerId is re-sent after min(2^attempt ms, 100 ms) of DUTY-CLOCK time, the retained
// frame (same correlation id), then Ok.
TEST(ProducerTxn, InitProducerIdRetriesConcurrentTransactionsRegression) {
    Rig r;
    Op op = r.init();
    r.tick();
    const int64_t corr = r.lastInitCorr();
    const AnswerFrame busy = initAnswer(corr, -1, 0, ec(rpc::ErrorCode::ConcurrentTransactions));
    for (int attempt = 0; attempt < 4; ++attempt) {
        SCOPED_TRACE(attempt);
        r.answer(busy);
        ASSERT_FALSE(op.ready()) << "27 must not complete the op";
        const std::size_t before = r.inits();
        const int64_t wait = (int64_t{1} << attempt) * kMs;
        r.tick(wait - 1);
        EXPECT_EQ(r.inits(), before) << "re-sent before its backoff";
        r.tick(1);
        ASSERT_EQ(r.inits(), before + 1) << "not re-sent after 2^" << attempt << " ms";
        EXPECT_EQ(r.lastInitCorr(), corr) << "the retained frame";
    }
    r.answer(initAnswer(corr, 4242, 7));
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(r.impl->producerId(), 4242);
}

// The backoff is capped and the op's own deadline is the only time bound: 27 forever -> Timeout, exactly once,
// nothing sent after it; init may then be retried (Uninitialized).
TEST(ProducerTxn, ConcurrentTransactionsRetryIsBoundedOnlyByTheOpDeadlineRegression) {
    Rig r;
    Op op = r.init();
    r.tick();
    const int64_t corr = r.lastInitCorr();
    const AnswerFrame busy = initAnswer(corr, -1, 0, ec(rpc::ErrorCode::ConcurrentTransactions));
    int resends = 0;
    while (r.now + 100 * kMs < kT0 + r.cfg.transactionOpTimeoutMs * kMs) {
        r.answer(busy);
        const std::size_t before = r.inits();
        int waited = 0;
        while (r.inits() == before) {
            r.tick(kMs);
            ASSERT_LE(++waited, 101) << "backoff beyond the 100 ms cap";
        }
        ++resends;
    }
    EXPECT_GE(resends, 8);
    EXPECT_FALSE(op.ready()) << "still retrying inside the deadline";
    r.answer(busy);
    r.now = kT0 + r.cfg.transactionOpTimeoutMs * kMs;
    r.tick();
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Timeout);
    EXPECT_EQ(r.state(), TxnState::Uninitialized) << "init may be retried";
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
    const std::size_t after = r.inits();
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.inits(), after) << "nothing after the timeout";
}

// F14 / FD5: NotCoordinator never re-sends into the same coordinator: the request waits for the Client's
// coordinator move (onCoordinatorMoved), then re-sends.  InitProducerId and EndTxn.
TEST(ProducerTxn, NotCoordinatorRediscoversThenRetriesRegression) {
    Rig r;
    Op init = r.init();
    r.tick();
    const int64_t ic = r.lastInitCorr();
    r.answer(FakeCoordinator::errorResponse(ic, rpc::ErrorCode::NotCoordinator, "127.0.0.1:9"));
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.inits(), 1u) << "re-sent before the coordinator moved";
    r.hub.onCoordinatorMoved();
    r.tick();
    ASSERT_EQ(r.inits(), 2u);
    r.answer(initAnswer(ic, 5, 0));
    ASSERT_TRUE(init.ready());
    EXPECT_EQ(init.status(), OpStatus::Ok);

    r.startTxn();
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.endAnswer(ec(rpc::ErrorCode::NotCoordinator));
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.endTxns(), 1u);
    r.hub.onCoordinatorMoved();
    r.tick();
    ASSERT_EQ(r.endTxns(), 2u);
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// A request in flight when the coordinator session changes is re-sent on the new session (same corrId).
TEST(ProducerTxn, InFlightRequestIsResentOnANewSessionRegression) {
    Rig r;
    Op op = r.init();
    r.tick();
    const OfferedFrame first = r.fake.offered.back();
    r.fake.sid = 78;
    r.hub.onCoordinatorSessionChanged();
    r.tick();
    ASSERT_EQ(r.inits(), 2u) << "not re-sent on the new session";
    EXPECT_EQ(r.fake.offered.back().sessionId, 78);
    EXPECT_EQ(r.fake.offered.back().corrId, first.corrId);
    r.answer(initAnswer(first.corrId, 1, 0));
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
}

// ============================================================================================================
// begin / commit / abort
// ============================================================================================================

// B-1 (Kafka transactionStarted): nothing added on the coordinator -> the transaction ends LOCALLY, no EndTxn
// (the coordinator would answer 19 for a transaction that is not Ongoing, which is fatal).  Commit and abort.
TEST(ProducerTxn, BeginCommitOfAnEmptyTransactionEndsLocallyWithoutEndTxn) {
    Rig r;
    r.initOk();
    r.begin();
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    Op commit = r.commit();
    r.tick();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    EXPECT_EQ(commit.c().op, OpKind::CommitTransaction);
    EXPECT_EQ(r.state(), TxnState::Ready);
    r.begin();
    Op abort = r.abort();
    r.tick();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(abort.c().op, OpKind::AbortTransaction);
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_EQ(r.endTxns(), 0u) << "an EndTxn for a transaction that never started";
}

TEST(ProducerTxn, CommitTransactionEmitsEndTxnCommit) {
    Rig r;
    r.initOk(88, 1);
    r.startTxn();
    Op op = r.commit();
    EXPECT_EQ(r.state(), TxnState::CommittingTransaction);
    r.tick();
    const auto et = r.lastEnd();
    ASSERT_TRUE(et.has_value());
    EXPECT_EQ(et->producerId, 88);
    EXPECT_EQ(et->producerEpoch, 1);
    EXPECT_EQ(et->commit, uint8_t{1});
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(et->transactionalId.data()), et->transactionalId.size()),
              "txn-1");
    EXPECT_FALSE(op.ready());
    EXPECT_EQ(r.state(), TxnState::CommittingTransaction) << "reset only on EndTxn's final answer";
    r.endAnswer();
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_FALSE(r.impl->appTxnActive());
}

// TX-6b: EndTxn waits for every record the transaction accepted to be acknowledged (the producer's own data
// path: the commit flushes the builders at once, the EndTxn follows the last ack).
TEST(ProducerTxn, CommitWaitsForDataThenSendsEndTxnCommit) {
    rc::ProducerConfig cfg = txnCfg();
    cfg.lingerUs = 10'000'000;   // the records would sit in their builders for 10 s: the commit flushes them
    Rig r(cfg);
    r.initOk(42, 0);
    r.startTxn();
    ASSERT_TRUE(r.sendRecord(0));
    ASSERT_TRUE(r.sendRecord(1));
    r.addOk();
    ASSERT_TRUE(r.wire.empty()) << "linger: held in the builders";
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.wire.size(), 2u) << "the commit flushes what the transaction accepted";
    EXPECT_EQ(r.wire[0].pid, 42);
    EXPECT_EQ(r.endTxns(), 0u) << "EndTxn BEFORE the batches were acked";
    r.ticks(3, kMs);
    EXPECT_EQ(r.endTxns(), 0u);
    r.hub.onProduceResponse(r.wire[0].corrId, 0, 0);
    r.tick();
    EXPECT_EQ(r.endTxns(), 0u) << "one batch still in flight";
    r.hub.onProduceResponse(r.wire[1].corrId, 0, 0);
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u) << "every batch acked: EndTxn now";
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{1});
    EXPECT_FALSE(commit.ready());
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// I-4: EndTxn gets its own transactionOpTimeoutMs budget from the moment it is created - the data wait does not
// eat its retry budget.
TEST(ProducerTxn, EndTxnDeadlineStartsAtItsCreationRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    ASSERT_TRUE(r.sendRecord());
    r.addOk();
    Op commit = r.commit();   // applied at t0: op deadline t0 + 1 s
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    r.tick(900 * kMs);        // the data takes 0.9 s to be acked
    r.ackLastData();
    r.tick();                 // EndTxn created now (t0 + 0.9 s): its own 1 s budget
    ASSERT_EQ(r.endTxns(), 1u);
    r.endAnswer(ec(rpc::ErrorCode::ConcurrentTransactions));
    r.tick(500 * kMs);        // t0 + 1.4 s: past the apply-time deadline, inside EndTxn's
    r.tick();
    EXPECT_FALSE(commit.ready()) << "EndTxn timed out on the data-wait clock";
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

TEST(ProducerTxn, AbortSendsEndTxnAbort) {
    Rig r;
    r.initOk(66, 0);
    r.startTxn();
    Op op = r.abort();
    EXPECT_EQ(r.state(), TxnState::AbortingTransaction);
    r.tick();
    const auto et = r.lastEnd();
    ASSERT_TRUE(et.has_value());
    EXPECT_EQ(et->commit, uint8_t{0});
    EXPECT_EQ(et->producerId, 66);
    r.endAnswer();
    ASSERT_TRUE(op.ready());
    EXPECT_EQ(op.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
}

// Kafka: COMMITTING -> ABORTING is invalid.  An abort while committing completes BrokerError(19) and changes
// nothing: the commit still completes Ok on its own answer.
TEST(ProducerTxn, AbortIsRefusedWhileCommitting) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    Op abort = r.abort();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::BrokerError);
    EXPECT_EQ(abort.code(), kInvalidTxnState);
    EXPECT_EQ(r.state(), TxnState::CommittingTransaction);
    r.ticks(4, kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "no EndTxn(abort) over a running commit";
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// B-2 (Kafka m1): an EndTxn that times out is IN DOUBT - the producer stays Committing / Aborting, begin and
// (while committing) abort are refused, nothing is re-sent until the app retries the SAME direction, which
// re-sends EndTxn.
TEST(ProducerTxn, CommitTimeoutIsInDoubtAndARetriedCommitResendsEndTxn) {
    for (const bool commitDir : {true, false}) {
        SCOPED_TRACE(commitDir ? "commit" : "abort");
        Rig r;
        r.initOk();
        r.startTxn();
        Op first = commitDir ? r.commit() : r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);   // never answered
        r.tick(r.cfg.transactionOpTimeoutMs * kMs);
        ASSERT_TRUE(first.ready());
        EXPECT_EQ(first.status(), OpStatus::Timeout);
        const TxnState inDoubt = commitDir ? TxnState::CommittingTransaction : TxnState::AbortingTransaction;
        EXPECT_EQ(r.state(), inDoubt) << "in doubt: never Ready / AbortableError";
        EXPECT_TRUE(r.impl->appTxnActive());
        const uint64_t refused = r.impl->txnRefusedBegins();
        r.begin();
        EXPECT_EQ(r.impl->txnRefusedBegins(), refused + 1);
        if (commitDir) {
            Op abort = r.abort();
            ASSERT_TRUE(abort.ready());
            EXPECT_EQ(abort.status(), OpStatus::BrokerError);
        }
        r.ticks(64, 10 * kMs);
        EXPECT_EQ(r.endTxns(), 1u) << "re-sent before the app retried";
        Op retry = commitDir ? r.commit() : r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 2u) << "the retry re-sends EndTxn";
        EXPECT_EQ(r.lastEnd()->commit, commitDir ? uint8_t{1} : uint8_t{0}) << "the same direction";
        r.endAnswer();
        ASSERT_TRUE(retry.ready());
        EXPECT_EQ(retry.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready);
    }
}

// m12: the ORIGINAL in-doubt EndTxn's late answer, after the app retried, completes nothing.
TEST(ProducerTxn, LateOriginalEndTxnAnswerAfterRetryIsIgnoredRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op first = r.commit();
    r.tick();
    const int64_t original = r.lastEndCorr();
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);
    ASSERT_TRUE(first.ready());
    EXPECT_EQ(first.status(), OpStatus::Timeout);
    Op retry = r.commit();
    r.tick();
    const int64_t again = r.lastEndCorr();
    ASSERT_NE(again, original);
    const AnswerFrame late = FakeCoordinator::answer(rp::ReqKind::EndTxn, original, 0);
    r.answerLate(late);
    EXPECT_FALSE(retry.ready()) << "completed by the stale answer";
    r.answer(FakeCoordinator::answer(rp::ReqKind::EndTxn, again, 0));
    r.answerLate(late);
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.status(), OpStatus::Ok);
    EXPECT_EQ(retry.times(), 1);
    EXPECT_EQ(first.times(), 1);
    EXPECT_EQ(r.state(), TxnState::Ready);
}

// commit and abort each complete exactly once: through a 27 retry, and with duplicate / late answers ignored.
TEST(ProducerTxn, CommitAbortCompleteOnceRegression) {
    for (const bool doCommit : {true, false}) {
        SCOPED_TRACE(doCommit ? "commit" : "abort");
        Rig r;
        r.initOk();
        r.startTxn();
        Op op = doCommit ? r.commit() : r.abort();
        r.tick();
        const int64_t corr = r.lastEndCorr();
        const AnswerFrame busy = FakeCoordinator::answer(rp::ReqKind::EndTxn, corr, ec(rpc::ErrorCode::ConcurrentTransactions));
        r.answer(busy);
        EXPECT_FALSE(op.ready()) << "27 is retriable, not a completion";
        r.tick(kMs);
        ASSERT_EQ(r.endTxns(), 2u) << "re-sent after the backoff";
        const AnswerFrame ok = FakeCoordinator::answer(rp::ReqKind::EndTxn, corr, 0);
        r.answer(ok);
        r.answerLate(ok);     // duplicate
        r.answerLate(busy);   // late
        r.tick();
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::Ok);
        EXPECT_EQ(op.times(), 1);
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.endTxns(), 2u) << "nothing re-sent after completion";
        EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
    }
}

// EndTxn is retried until 0 / 19 / 17 / 18: 27 and every other code (10 MinIsrViolation, 6 Internal) are
// retried; 19 completes BrokerError (fatal), 17 / 18 Fenced.
TEST(ProducerTxn, ConcurrentTransactionsOnEndTxnRetriesUntilFinal) {
    struct Case { int16_t terminal; OpStatus expect; TxnState after; };
    for (const Case cs : {Case{0, OpStatus::Ok, TxnState::Ready},
                          Case{ec(rpc::ErrorCode::InvalidTxnState), OpStatus::BrokerError, TxnState::FatalError},
                          Case{ec(rpc::ErrorCode::ProducerFenced), OpStatus::Fenced, TxnState::FatalError},
                          Case{ec(rpc::ErrorCode::InvalidProducerEpoch), OpStatus::Fenced, TxnState::FatalError}}) {
        SCOPED_TRACE(cs.terminal);
        Rig r;
        r.initOk();
        r.startTxn();
        Op op = r.commit();
        r.tick();
        const int64_t corr = r.lastEndCorr();
        for (const int16_t code : {ec(rpc::ErrorCode::ConcurrentTransactions), ec(rpc::ErrorCode::MinIsrViolation),
                                   ec(rpc::ErrorCode::Internal), ec(rpc::ErrorCode::ConcurrentTransactions)}) {
            const std::size_t before = r.endTxns();
            r.answer(FakeCoordinator::answer(rp::ReqKind::EndTxn, corr, code));
            ASSERT_FALSE(op.ready()) << "EndTxn completed on retriable code " << code;
            int waited = 0;
            while (r.endTxns() == before) {
                r.tick(kMs);
                ASSERT_LE(++waited, 101);
            }
        }
        r.answer(FakeCoordinator::answer(rp::ReqKind::EndTxn, corr, cs.terminal));
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), cs.expect);
        if (cs.expect == OpStatus::BrokerError) EXPECT_EQ(op.code(), static_cast<uint16_t>(cs.terminal));
        EXPECT_EQ(r.state(), cs.after);
        const std::size_t after = r.endTxns();
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.endTxns(), after);
    }
}

// Authentication: an EndTxn answered NotAuthorized (45) / PasswordChangeRequired (47) is NOT retried: the
// commit fails with the code, exactly one EndTxn was sent.
TEST(ProducerTxn, EndTxnNotAuthorizedFailsTheCommitWithoutRetryRegression) {
    for (const auto code : {rpc::ErrorCode::NotAuthorized, rpc::ErrorCode::PasswordChangeRequired}) {
        SCOPED_TRACE(static_cast<int>(code));
        Rig r;
        r.initOk();
        r.startTxn();
        Op op = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        r.endAnswer(ec(code));
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::BrokerError);
        EXPECT_EQ(op.code(), static_cast<uint16_t>(code));
        r.ticks(50, 10 * kMs);
        EXPECT_EQ(r.endTxns(), 1u) << "never re-sent";
    }
}

// FD6: an EndTxn the broker cannot decode (39) is never re-sent: the commit completes BrokerError(39) and the
// transaction is abortable (a decode failure is not a fence).
TEST(ProducerTxn, EndTxnInvalidRequestFailsAtOnceNotRetriedRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op op = r.commit();
    r.tick();
    r.endAnswer(ec(rpc::ErrorCode::InvalidRequest));
    ASSERT_TRUE(op.ready()) << "an InvalidRequest EndTxn answer left the commit pending";
    EXPECT_EQ(op.status(), OpStatus::BrokerError);
    EXPECT_EQ(op.code(), uec(rpc::ErrorCode::InvalidRequest));
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "re-sent a request the broker cannot decode";
}

// m5: abort while an abort is pending joins it - one EndTxn, both tokens complete with its result, once.
TEST(ProducerTxn, AbortWhileAbortingJoinsThePendingAbortRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op a1 = r.abort();
    Op a2 = r.abort();
    EXPECT_FALSE(a2.ready()) << "joined, not refused";
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.endAnswer();
    for (Op* op : {&a1, &a2}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::Ok);
        EXPECT_EQ(op->times(), 1);
    }
}

// I-1 / m13: begin is refused while a commit is pending; a second commit JOINS the pending one (one EndTxn, the
// same result for both tokens).
TEST(ProducerTxn, BeginWhileCommittingIsRefusedRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit1 = r.commit();
    r.begin();   // while CommittingTransaction
    Op commit2 = r.commit();
    EXPECT_EQ(r.impl->txnRefusedBegins(), 1u);
    EXPECT_FALSE(commit2.ready()) << "m13: joined the pending commit";
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit1.ready());
    EXPECT_EQ(commit1.status(), OpStatus::Ok);
    ASSERT_TRUE(commit2.ready());
    EXPECT_EQ(commit2.status(), OpStatus::Ok);
    EXPECT_EQ(r.endTxns(), 1u) << "one EndTxn for both";
    EXPECT_EQ(r.state(), TxnState::Ready) << "the refused begin opened nothing";
}

// m15: a transaction only POSSIBLY started (a leg sent but unanswered, Task 6 / 7): the abort SENDS
// EndTxn(abort), and 19 (nothing open on the broker) completes it Ok in this ambiguous case only.
TEST(ProducerTxn, PossiblyStartedAbortAnswered19IsDoneRegression) {
    for (const int16_t answer : {int16_t{0}, ec(rpc::ErrorCode::InvalidTxnState)}) {
        SCOPED_TRACE(answer);
        Rig r;
        r.initOk();
        r.startTxn(rp::TxnStarted::Maybe);
        Op abort = r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u) << "a possibly-started transaction must be aborted on the broker";
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer(answer);
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready) << "19 here means nothing was open: not fatal";
    }
    Rig r;   // outside the ambiguous case 19 on EndTxn stays fatal
    r.initOk();
    r.startTxn();
    Op abort = r.abort();
    r.tick();
    r.endAnswer(ec(rpc::ErrorCode::InvalidTxnState));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::BrokerError);
    EXPECT_EQ(r.state(), TxnState::FatalError);
}

// ============================================================================================================
// AbortableError / B-3 epoch bump
// ============================================================================================================

// C-1: a poisoned transaction must be ABORTED: begin is refused, a commit completes with the transaction's error
// and sends nothing; only abort proceeds (EndTxn(abort)); the next transaction commits normally.
TEST(ProducerTxn, PoisonedTransactionMustBeAbortedBeforeTheNextRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::Plain);
    ASSERT_EQ(r.state(), TxnState::AbortableError);
    EXPECT_TRUE(r.impl->appTxnActive()) << "the broker transaction is still open";
    const uint64_t refused = r.impl->txnRefusedBegins();
    r.begin();
    EXPECT_EQ(r.impl->txnRefusedBegins(), refused + 1) << "begin while AbortableError";
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed) << "commit completes with the txn's error";
    Op offs = r.sendOffsets();
    ASSERT_TRUE(offs.ready());
    EXPECT_EQ(offs.status(), OpStatus::DeliveryFailed);
    Op flush = r.flush();
    r.tick();
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::DeliveryFailed) << "a flush completes with the poison status";
    r.ticks(8, kMs);
    EXPECT_EQ(r.endTxns(), 0u) << "a poisoned transaction sent a commit";
    Op abort = r.abort();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
    r.begin();
    EXPECT_EQ(r.impl->txnRefusedBegins(), refused + 2) << "still in the transaction until the final answer";
    r.endAnswer();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    r.startTxn();
    Op commit2 = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 2u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{1});
    r.endAnswer();
    ASSERT_TRUE(commit2.ready());
    EXPECT_EQ(commit2.status(), OpStatus::Ok);
}

// Phase 2b F Task 9 (the tail of the removed PoisonedTransactionMustBeAbortedBeforeTheNext test): the first
// EndTxn(abort) offer is refused by the transport and re-offered on a later pass; the abort stays pending (still
// AbortingTransaction) until the final answer.
TEST(ProducerTxnRegression, RefusedEndTxnAbortOfferIsReofferedRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::Plain);
    r.fake.next = rp::CoordinatorSink::Offer::Retry;
    Op abort = r.abort();
    r.ticks(3);
    EXPECT_FALSE(abort.ready());
    EXPECT_EQ(r.state(), TxnState::AbortingTransaction);
    EXPECT_EQ(r.endTxns(), 0u) << "a refused offer is not on the wire";
    r.fake.next = rp::CoordinatorSink::Offer::Accepted;
    r.tick(5 * kMs);
    ASSERT_EQ(r.endTxns(), 1u) << "the refused EndTxn was never re-offered";
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
    r.endAnswer();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
}

// B-3 (KIP-360): a sequence gap makes the abort bump the epoch - InitProducerId (the transactional id) after
// EndTxn(abort); the abort completes after the bump, under the new epoch, with sequences restarted.
TEST(ProducerTxn, SequenceGapAbortBumpsTheEpoch) {
    Rig r;
    r.initOk(42, 0);
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
    ASSERT_EQ(r.state(), TxnState::AbortableError);
    Op abort = r.abort();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.endAnswer();
    EXPECT_FALSE(abort.ready()) << "the abort waits for the epoch bump";
    EXPECT_EQ(r.state(), TxnState::AbortingTransaction);
    r.tick();
    ASSERT_EQ(r.inits(), 2u) << "no KIP-360 epoch bump";
    const auto req = rpc::InitProducerId::decode(r.last(rpc::InitProducerId::TEMPLATE_ID)->head.data(),
                                                 r.last(rpc::InitProducerId::TEMPLATE_ID)->len);
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->transactionalId.size(), 5u) << "the bump names the transactional id";
    r.answer(initAnswer(r.lastInitCorr(), 42, 1));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_EQ(r.impl->producerEpoch(), 1);
    // The next transaction's data goes out under the bumped epoch from sequence 0.
    r.startTxn();
    ASSERT_TRUE(r.sendRecord());
    r.addOk();   // Task 6: the partition joins the transaction
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    EXPECT_EQ(r.wire[0].epoch, 1);
    EXPECT_EQ(r.wire[0].baseSeq, 0);
    r.ackLastData();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// B-4: abort Ok -> the KIP-360 bump in flight -> the op's deadline -> the app retries abort: ONLY the bump is
// re-sent, never EndTxn under the stale epoch; a late answer to the first bump is ignored.
TEST(ProducerTxn, InDoubtBumpRetryResendsOnlyTheBumpRegression) {
    Rig r;
    r.initOk(42, 0);
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
    Op abort = r.abort();
    r.tick();
    r.endAnswer();
    r.tick();
    ASSERT_EQ(r.inits(), 2u);
    const int64_t firstBump = r.lastInitCorr();
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);   // the bump's answer is lost
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Timeout);
    EXPECT_EQ(r.state(), TxnState::AbortingTransaction) << "in doubt";
    Op retry = r.abort();
    r.ticks(4, kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "EndTxn re-sent under the stale epoch";
    ASSERT_EQ(r.inits(), 3u) << "the bump was not re-sent";
    r.answerLate(initAnswer(firstBump, 42, 1));
    EXPECT_FALSE(retry.ready());
    r.answer(initAnswer(r.lastInitCorr(), 42, 2));
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready) << "never Fatal";
    EXPECT_EQ(r.impl->producerEpoch(), 2);
}

// ============================================================================================================
// flush
// ============================================================================================================

TEST(ProducerTxn, FlushWithNothingPendingCompletesAtOnce) {
    Rig r;
    r.initOk();
    Op flush = r.flush();
    r.tick();
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::Ok);
    EXPECT_EQ(flush.c().op, OpKind::Flush);
}

// A flush waits for every record accepted before it to be acknowledged; it never ends the transaction.
TEST(ProducerTxn, FlushWaitsForTheRecordsAcceptedBeforeIt) {
    Rig r;
    r.initOk();
    r.startTxn();
    ASSERT_TRUE(r.sendRecord());
    r.addOk();   // Task 6: the partition joins the transaction
    Op flush = r.flush();
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    EXPECT_FALSE(flush.ready()) << "the record is in flight";
    r.ackLastData();
    r.tick();
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_EQ(r.endTxns(), 0u);
}

// The op deadline times the flush out; that never poisons the transaction.
TEST(ProducerTxn, FlushTimeoutDoesNotPoison) {
    Rig r;
    r.initOk();
    r.startTxn();
    ASSERT_TRUE(r.sendRecord());
    r.addOk();   // Task 6: the partition joins the transaction
    r.tick();                                      // produced, ack held
    Op flush = r.flush();
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::Timeout);
    EXPECT_EQ(r.state(), TxnState::InTransaction) << "a flush timeout is not a transaction failure";
}

// A flush issued while committing neither delays nor duplicates the EndTxn.
TEST(ProducerTxn, FlushDuringCommitSendsOneEndTxn) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    Op flush = r.flush();
    r.ticks(3);
    EXPECT_EQ(r.endTxns(), 1u);
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::Ok);
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
}

// ============================================================================================================
// Regressions named by the plan (Task 5)
// ============================================================================================================

// Review Focus #5: 18 on EndTxn (a second producer with the same transactional id bumped the epoch): the commit
// completes Fenced, the producer is FatalError, every later op completes Fenced, a record is never produced and
// no frame - coordinator or data - is offered afterwards.
TEST(ProducerTxnRegression, FencedByASecondProducerFailsEverythingRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    r.endAnswer(ec(rpc::ErrorCode::ProducerFenced));
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Fenced);
    EXPECT_EQ(r.state(), TxnState::FatalError);
    EXPECT_EQ(r.impl->appTxnState(), TxnState::FatalError);
    const std::size_t offered = r.fake.offered.size();

    Op init = r.init();
    ASSERT_TRUE(init.ready()) << "refused at once";
    EXPECT_EQ(init.status(), OpStatus::Fenced);
    const uint64_t refused = r.impl->txnRefusedBegins();
    r.begin();
    EXPECT_EQ(r.impl->txnRefusedBegins(), refused + 1);
    (void)r.sendRecord();   // a record the handle let through (FD2): never produced
    for (Op op : {r.commit(), r.abort(), r.flush(), r.sendOffsets()}) {
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::Fenced);
    }
    r.fake.sid = 99;
    r.hub.onCoordinatorSessionChanged();
    r.hub.onCoordinatorMoved();
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.fake.offered.size(), offered) << "a fenced producer offered a coordinator frame";
    EXPECT_TRUE(r.wire.empty()) << "a fenced producer produced a record";
    EXPECT_EQ(r.state(), TxnState::FatalError);
}

// Misuse never hangs: begin twice (the second is refused, counted, no side effect), commit in Ready, init twice:
// each token op completes BrokerError(19) exactly once (checked for every token at teardown).
TEST(ProducerTxnRegression, MisuseCompletesInvalidTxnStateNeverHangsRegression) {
    Rig r;
    Op commitUninit = r.commit();
    Op abortUninit  = r.abort();
    r.begin();   // before init
    EXPECT_EQ(r.impl->txnRefusedBegins(), 1u);
    r.initOk();
    Op init2 = r.init();
    Op commitReady = r.commit();
    Op abortReady  = r.abort();
    Op offsReady   = r.sendOffsets();
    r.begin();
    r.begin();   // twice
    EXPECT_EQ(r.impl->txnRefusedBegins(), 2u);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    for (Op* op : {&commitUninit, &abortUninit, &init2, &commitReady, &abortReady, &offsReady}) {
        ASSERT_TRUE(op->ready());
        EXPECT_EQ(op->status(), OpStatus::BrokerError);
        EXPECT_EQ(op->code(), kInvalidTxnState);
        EXPECT_EQ(op->times(), 1);
    }
    r.ticks(8, kMs);
    EXPECT_EQ(r.inits(), 1u) << "the refused init sent nothing";
    EXPECT_EQ(r.endTxns(), 0u);
}

// FD6 (auth): 45 NotAuthorized on InitProducerId is FATAL (BrokerError 45; every later op completes with it);
// 45 on AddOffsets (any transaction leg) only fails the transaction: AbortableError(BrokerError 45), the commit
// completes with it, the abort works.
TEST(ProducerTxnRegression, NotAuthorizedOnInitIsFatalOnAddOffsetsAbortableRegression) {
    {
        Rig r;
        Op init = r.init();
        r.tick();
        r.answer(initAnswer(r.lastInitCorr(), -1, 0, ec(rpc::ErrorCode::NotAuthorized)));
        ASSERT_TRUE(init.ready());
        EXPECT_EQ(init.status(), OpStatus::BrokerError);
        EXPECT_EQ(init.code(), uec(rpc::ErrorCode::NotAuthorized));
        EXPECT_EQ(r.state(), TxnState::FatalError);
        Op again = r.init();
        ASSERT_TRUE(again.ready());
        EXPECT_EQ(again.status(), OpStatus::BrokerError);
        EXPECT_EQ(again.code(), uec(rpc::ErrorCode::NotAuthorized));
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.inits(), 1u) << "never re-sent";
    }
    {
        Rig r;
        r.initOk();
        r.startTxn();
        r.impl->testLegAnswer(rp::ReqKind::AddOffsets, ec(rpc::ErrorCode::NotAuthorized));
        ASSERT_EQ(r.state(), TxnState::AbortableError);
        Op commit = r.commit();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::BrokerError);
        EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::NotAuthorized));
        Op abort = r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready);
    }
}

// Auth D26: a revoked session is terminal.  The pending commit completes BrokerError(SessionRevoked), the producer
// is FatalError with that status, no coordinator request is ever re-sent (not even on a session change), and every
// later op completes BrokerError(46).
TEST(ProducerTxnRegression, SessionRevokedIsFatalAndNothingIsResentRegression) {
    constexpr uint16_t kRevoked = static_cast<uint16_t>(rpc::ErrorCode::SessionRevoked);
    {
        // A pending flush (its record unanswered) completes with the revocation too.
        Rig r;
        r.initOk();
        r.startTxn();
        ASSERT_TRUE(r.sendRecord());
        r.addOk();   // Task 6: the partition joins the transaction
        r.tick();
        Op flush = r.flush();
        r.tick();
        ASSERT_FALSE(flush.ready());
        r.hub.onSessionRevoked(kRevoked);
        ASSERT_TRUE(flush.ready());
        EXPECT_EQ(flush.status(), OpStatus::BrokerError);
        EXPECT_EQ(flush.code(), kRevoked);
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.wire.size(), 1u) << "the batch was re-sent after the revocation";
    }
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);   // in flight
    r.hub.onSessionRevoked(kRevoked);
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(commit.code(), kRevoked);
    EXPECT_EQ(r.state(), TxnState::FatalError);
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
    const std::size_t offered = r.fake.offered.size();
    r.fake.sid = 99;
    r.hub.onCoordinatorSessionChanged();
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.fake.offered.size(), offered) << "a revoked session re-sent a request";
    for (Op op : {r.init(), r.abort(), r.commit()}) {
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::BrokerError);
        EXPECT_EQ(op.code(), kRevoked);
    }
}

// close() on an open transaction aborts it FIRST: EndTxn(abort) goes out, the close token completes only after
// its answer (then the producer drains and closes Ok); later ops complete Closed and nothing more is sent.
TEST(ProducerTxnRegression, CloseAbortsAnOpenTransactionFirstRegression) {
    Rig r;
    r.initOk(77, 0);
    r.startTxn();
    Op cl = r.close();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u) << "close() must end the open transaction";
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0}) << "with EndTxn(abort)";
    EXPECT_FALSE(cl.ready()) << "not closed before the abort's answer";
    r.endAnswer();
    r.tick();
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
    EXPECT_EQ(cl.c().op, OpKind::ProducerClose);
    EXPECT_EQ(r.state(), TxnState::Ready);
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Closed) << "rejected after close";
    r.ticks(16, 10 * kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "nothing sent after close";
}

// close() with nothing open on the broker (Uninitialized; Ready; begun but never started) closes at once: Ok, no
// EndTxn.  A second close completes Closed exactly once.
TEST(ProducerTxn, TxnCloseWithNothingOpenClosesAtOnceRegression) {
    {
        Rig r;
        Op cl = r.close();
        r.tick();
        ASSERT_TRUE(cl.ready());
        EXPECT_EQ(cl.status(), OpStatus::Ok);
    }
    {
        Rig r;
        r.initOk();
        r.begin();   // nothing added: ends locally
        Op cl = r.close();
        r.tick();
        ASSERT_TRUE(cl.ready());
        EXPECT_EQ(cl.status(), OpStatus::Ok);
        EXPECT_EQ(r.endTxns(), 0u) << "never started: no EndTxn";
        Op again = r.close();
        ASSERT_TRUE(again.ready());
        EXPECT_EQ(again.status(), OpStatus::Closed);
    }
}

// close() never aborts a running commit (COMMITTING -> ABORTING is invalid): it waits for its answer, then closes.
TEST(ProducerTxn, TxnCloseWaitsForARunningCommitRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    Op cl = r.close();
    r.tick();
    EXPECT_FALSE(cl.ready());
    EXPECT_EQ(r.endTxns(), 1u) << "no EndTxn(abort) over a running commit";
    r.endAnswer();
    r.tick();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
}

// close() reports a failed abort (EndTxn never answered: Timeout at transactionOpTimeoutMs) and closes anyway.
TEST(ProducerTxn, TxnCloseReportsAFailedAbortAndClosesAnywayRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op cl = r.close();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);   // never answered
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);
    r.tick();
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Timeout);
    EXPECT_EQ(cl.times(), 1);
    EXPECT_FALSE(r.impl->lifeOpen());
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Closed);
}

// closeImmediately() leaves the open transaction to the coordinator's timeout: no EndTxn; every pending txn op and
// a pending close token complete Closed exactly once; nothing more is sent.
TEST(ProducerTxn, TxnCloseImmediatelyDiscardsLocallyRegression) {
    {
        Rig r;
        r.initOk();
        r.startTxn();
        Op cl = r.close();   // the close's abort is in flight
        r.tick();
        const std::size_t ends = r.endTxns();
        EXPECT_EQ(ends, 1u);
        r.impl->applyCloseImmediately(OpStatus::Closed);
        ASSERT_TRUE(cl.ready());
        EXPECT_EQ(cl.status(), OpStatus::Closed);
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.endTxns(), ends) << "nothing more sent";
        EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
    }
    {
        Rig r;
        r.initOk();
        r.startTxn();
        Op commit = r.commit();
        Op flush  = r.flush();
        r.impl->applyCloseImmediately(OpStatus::Closed);   // the handle's destructor semantics
        for (Op* op : {&commit, &flush}) {
            ASSERT_TRUE(op->ready());
            EXPECT_EQ(op->status(), OpStatus::Closed);
        }
        r.ticks(16, 10 * kMs);
        EXPECT_EQ(r.endTxns(), 0u) << "closeImmediately never sends EndTxn";
    }
}

// E2 final fix wave (M3), ported: a 42 TransientLogFull on a transactional producer's batch is re-offered after
// kTransientLogFullBackoffNs of DUTY-CLOCK time, never after a cycle count (5000 busy-spin cycles 1 us apart
// must not re-offer it), and never fails the transaction.
TEST(ProducerTxnRegression, TransientLogFullBackoffIsWallClockTimeRegression) {
    Rig r;
    r.initOk(77, 0);
    r.startTxn();
    ASSERT_TRUE(r.sendRecord());
    r.addOk();   // Task 6: the partition joins the transaction
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    for (int answer = 0; answer < 12; ++answer) {   // a long full ring: answered 42 again and again
        SCOPED_TRACE(answer);
        r.ackLastData(ec(rpc::ErrorCode::TransientLogFull));
        const std::size_t sent = r.wire.size();
        const int64_t answeredAt = r.now;
        for (int c = 0; c < 5000; ++c) {
            r.tick(1'000);   // 1 us per duty cycle (busy spin)
            ASSERT_EQ(r.wire.size(), sent) << "re-offered after " << c + 1 << " cycles: a cycle-count backoff";
        }
        r.now = answeredAt + ProducerImpl::kTransientLogFullBackoffNs;
        r.tick();
        ASSERT_EQ(r.wire.size(), sent + 1) << "not re-offered once the backoff elapsed";
        EXPECT_EQ(r.wire.back().baseSeq, r.wire.front().baseSeq) << "same identity";
        ASSERT_EQ(r.state(), TxnState::InTransaction);
    }
    r.ackLastData(0, 7);
    Op commit = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// Part B's abandon rule on the transaction path: an abort that arrives while an Invoker (Direct) claim is open
// abandons it - the claim's commit() returns false, its bytes are quarantined, nothing reaches a frame.
TEST(ProducerTxnRegression, AbortAbandonsAnOpenDirectClaimRegression) {
    for (const TxnControl::Kind end : {TxnControl::Kind::Abort, TxnControl::Kind::Commit}) {
        SCOPED_TRACE(end == TxnControl::Kind::Abort ? "abort" : "commit");
        Rig r(txnCfg(), /*direct=*/true);
        r.initOk();
        r.startTxn();
        r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
        const auto g = r.impl->claimRecord(1, 0, Key(uint64_t{3}), 16, 0);
        ASSERT_EQ(g.result, rc::SendResult::Ok);
        std::memset(g.base, 0xC1, 16);
        Op op = r.apply(end);
        std::memset(g.base, 0xC2, 16);   // the caller still writes its span: harmless (quarantined)
        EXPECT_FALSE(r.impl->commitClaim(g.token)) << "a claim open across the end of its transaction committed";
        EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
        r.ticks(4, kMs);
        EXPECT_TRUE(r.wire.empty()) << "the abandoned claim's bytes reached a frame";
        if (r.endTxns() > 0) r.endAnswer();
        ASSERT_TRUE(op.ready());
        EXPECT_EQ(op.status(), OpStatus::Ok);
    }
}

// ============================================================================================================
// Fix round 1 (review I-1): the facts the old booleans carried survive an in-doubt timeout
// ============================================================================================================

// endSent: a poison (a late data / leg failure, Task 6 / 7) after EndTxn(commit) went out - answered never, so the
// commit is InDoubt{AfterEnd} - must not turn the commit into an abort: the state stays Committing, abort stays
// refused, no EndTxn(abort) ever leaves, and the retried commit re-sends EndTxn(commit) (also when the poison lands
// in the window before the re-send).  Before EndTxn went out (InDoubt{BeforeEnd}) the same poison does fail it.
TEST(ProducerTxnRegression, PoisonAfterAnInDoubtCommitEndTxnNeverAbortsRegression) {
    {
        Rig r;
        r.initOk();
        r.startTxn();
        Op first = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        r.tick(r.cfg.transactionOpTimeoutMs * kMs);
        ASSERT_EQ(first.status(), OpStatus::Timeout);
        r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
        EXPECT_EQ(r.state(), TxnState::CommittingTransaction) << "a commit that may have landed was poisoned";
        Op abort = r.abort();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::BrokerError);
        Op retry = r.commit();   // ResendEnd: the re-send is pending
        r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::Plain);
        EXPECT_EQ(r.state(), TxnState::CommittingTransaction);
        r.tick();
        ASSERT_EQ(r.endTxns(), 2u);
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{1}) << "an EndTxn(abort) over a commit that may have landed";
        r.endAnswer();
        ASSERT_TRUE(retry.ready());
        EXPECT_EQ(retry.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready);
    }
    {
        Rig r;   // InDoubt{BeforeEnd}: the data never settled, nothing was sent - the poison fails the transaction
        r.initOk();
        r.startTxn();
        ASSERT_TRUE(r.sendRecord());
        r.addOk();   // Task 6: the partition joins the transaction
        Op first = r.commit();
        r.tick();
        r.tick(r.cfg.transactionOpTimeoutMs * kMs);
        ASSERT_EQ(first.status(), OpStatus::Timeout);
        ASSERT_EQ(r.endTxns(), 0u);
        r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::Plain);
        EXPECT_EQ(r.state(), TxnState::AbortableError);
        r.ackLastData();
        Op abort = r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
    }
}

// ended (B-4): EndTxn(abort) answered, the bump times out (InDoubt{Bump}), then a LATE leg answer arrives OK.  It must
// not revive the transaction: the retried abort re-sends only the bump (never EndTxn, never stuck on the unassigned
// identity) and completes.
TEST(ProducerTxnRegression, LateLegAnswerAfterABumpTimeoutStillRetriesOnlyTheBumpRegression) {
    Rig r;
    r.initOk(42, 0);
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
    Op abort = r.abort();
    r.tick();
    r.endAnswer();
    r.tick();
    ASSERT_EQ(r.inits(), 2u);
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);
    ASSERT_EQ(abort.status(), OpStatus::Timeout);
    r.impl->testLegAnswer(rp::ReqKind::AddPartitions, 0);   // late OK
    r.impl->testMarkStarted(rp::TxnStarted::Yes);
    Op retry = r.abort();
    r.ticks(4, kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "the late leg answer sent the retry down the EndTxn branch";
    ASSERT_EQ(r.inits(), 3u) << "the bump was not re-sent";
    r.answer(initAnswer(r.lastInitCorr(), 42, 1));
    ASSERT_TRUE(retry.ready());
    EXPECT_EQ(retry.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_EQ(r.impl->producerEpoch(), 1);
}

// ended, through close(): close's own abort of an InDoubt{Bump} abort resumes at the bump, never re-sends EndTxn.
TEST(ProducerTxnRegression, CloseAfterAnInDoubtBumpRetriesOnlyTheBumpRegression) {
    Rig r;
    r.initOk(42, 0);
    r.startTxn();
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
    Op abort = r.abort();
    r.tick();
    r.endAnswer();
    r.tick();
    r.tick(r.cfg.transactionOpTimeoutMs * kMs);
    ASSERT_EQ(abort.status(), OpStatus::Timeout);
    // Task 6 (re-review Minor 2): a LATE leg OK before close().  The old encoding (started = No once EndTxn(abort)
    // was answered) let it mark the transaction started again, and close's abort then re-sent EndTxn.
    r.impl->testLegAnswer(rp::ReqKind::AddPartitions, 0);
    Op cl = r.close();
    r.ticks(3, kMs);
    EXPECT_EQ(r.endTxns(), 1u) << "close re-sent EndTxn under the stale epoch";
    ASSERT_EQ(r.inits(), 3u);
    r.answer(initAnswer(r.lastInitCorr(), 42, 1));
    r.tick();
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
}

// bumpEpoch: a sequence gap found while the abort's EndTxn is already in flight still bumps the epoch (sticky).
TEST(ProducerTxnRegression, SequenceGapFoundWhileAbortingStillBumpsRegression) {
    Rig r;
    r.initOk(42, 0);
    r.startTxn();
    Op abort = r.abort();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.impl->testPoison(OpStatus::DeliveryFailed, 0, rp::AbortKind::BumpEpoch);
    EXPECT_EQ(r.state(), TxnState::AbortingTransaction);
    r.endAnswer();
    EXPECT_FALSE(abort.ready()) << "the gap's epoch bump was dropped";
    r.tick();
    ASSERT_EQ(r.inits(), 2u);
    r.answer(initAnswer(r.lastInitCorr(), 42, 1));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.impl->producerEpoch(), 1);
}

// started: a leg answer arriving after its transaction ended (Ready) changes nothing - the next, empty transaction
// still ends locally (no EndTxn) and a late failure poisons nothing.
TEST(ProducerTxnRegression, LateLegAnswerAfterTheTransactionEndedChangesNothingRegression) {
    Rig r;
    r.initOk();
    r.begin();
    Op c1 = r.commit();
    r.tick();
    ASSERT_EQ(c1.status(), OpStatus::Ok);
    r.impl->testLegAnswer(rp::ReqKind::AddPartitions, 0);
    r.impl->testLegAnswer(rp::ReqKind::AddOffsets, ec(rpc::ErrorCode::InvalidTxnState));
    EXPECT_EQ(r.state(), TxnState::Ready);
    r.begin();
    Op c2 = r.commit();
    r.tick();
    ASSERT_TRUE(c2.ready());
    EXPECT_EQ(c2.status(), OpStatus::Ok);
    EXPECT_EQ(r.endTxns(), 0u) << "a late leg answer marked the next transaction started";
}

// A late fencing answer after closeImmediately detached everything changes nothing (no FatalError, no completion).
TEST(ProducerTxnRegression, LateFencedAnswerAfterCloseImmediatelyChangesNothingRegression) {
    Rig r;
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.impl->applyCloseImmediately(OpStatus::Closed);
    ASSERT_EQ(commit.status(), OpStatus::Closed);
    r.impl->testLegAnswer(rp::ReqKind::AddPartitions, ec(rpc::ErrorCode::ProducerFenced));
    EXPECT_NE(r.state(), TxnState::FatalError);
    EXPECT_EQ(commit.times(), 1);
}

// Part B, through close(): close's own abort abandons an open Direct claim (commit() false, nothing in a frame).
TEST(ProducerTxnRegression, CloseOwnAbortAbandonsAnOpenDirectClaimRegression) {
    Rig r(txnCfg(), /*direct=*/true);
    r.initOk();
    r.startTxn();
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    const auto g = r.impl->claimRecord(1, 0, Key(uint64_t{3}), 16, 0);
    ASSERT_EQ(g.result, rc::SendResult::Ok);
    Op cl = r.close();
    std::memset(g.base, 0xC2, 16);
    EXPECT_FALSE(r.impl->commitClaim(g.token)) << "a claim open across close's abort committed";
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    r.endAnswer();
    r.ticks(2, kMs);
    EXPECT_TRUE(r.wire.empty()) << "the abandoned claim's bytes reached a frame";
    ASSERT_TRUE(cl.ready());
    EXPECT_EQ(cl.status(), OpStatus::Ok);
}


// Task 6 (Task 5 re-review, out of scope there): after an EndTxn(commit) may have landed, only a definite answer ends
// the commit.  A non-retriable answer to its RE-SEND - 39, or an ErrorResponse with code 0 (read as 39) - is
// FatalError with that code, never AbortableError: the app could otherwise abort over a commit that landed.  The
// FIRST EndTxn(commit) answered 39 stays abortable (FD6): nothing can have landed before it.
TEST(ProducerTxnRegression, RefusedResentCommitIsFatalNeverAbortableRegression) {
    for (const bool errorResponse : {false, true}) {
        SCOPED_TRACE(errorResponse ? "ErrorResponse code 0" : "EndTxnResponse 39");
        Rig r;
        r.initOk();
        r.startTxn();
        Op first = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        r.tick(r.cfg.transactionOpTimeoutMs * kMs);   // never answered: in doubt - it may have landed
        ASSERT_EQ(first.status(), OpStatus::Timeout);
        Op retry = r.commit();                         // re-sends EndTxn(commit)
        r.tick();
        ASSERT_EQ(r.endTxns(), 2u);
        if (errorResponse) {
            r.answer(FakeCoordinator::errorResponse(r.lastEndCorr(), static_cast<rpc::ErrorCode>(0), ""));
        } else {
            r.endAnswer(ec(rpc::ErrorCode::InvalidRequest));
        }
        ASSERT_TRUE(retry.ready());
        EXPECT_EQ(retry.status(), OpStatus::BrokerError);
        EXPECT_EQ(retry.code(), uec(rpc::ErrorCode::InvalidRequest));
        EXPECT_EQ(r.state(), TxnState::FatalError) << "a commit that may have landed became abortable";
        Op abort = r.abort();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::BrokerError);
        r.ticks(4, kMs);
        EXPECT_EQ(r.endTxns(), 2u) << "an EndTxn(abort) over a commit that may have landed";
    }
    Rig r;   // the first EndTxn(commit) refused: nothing landed - abortable, as FD6 says
    r.initOk();
    r.startTxn();
    Op commit = r.commit();
    r.tick();
    r.endAnswer(ec(rpc::ErrorCode::InvalidRequest));
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(r.state(), TxnState::AbortableError);
}

// Fix round 1 (review I1): the LINK's own re-send of EndTxn(commit) - after a coordinator move (NotCoordinator,
// then the Client re-points the coordinator) or a session change - also may have landed: the first offer reached
// a coordinator and its answer was lost or redirected.  A refusal (39) of the re-offer is FatalError, never
// AbortableError, and no EndTxn(abort) ever leaves.
TEST(ProducerTxnRegression, LinkResentCommitRefusedIsFatalNeverAbortableRegression) {
    for (const bool moved : {true, false}) {
        SCOPED_TRACE(moved ? "coordinator moved" : "session changed");
        Rig r;
        r.initOk();
        r.startTxn();
        Op commit = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        const int64_t corr = r.lastEndCorr();
        if (moved) {
            r.endAnswer(ec(rpc::ErrorCode::NotCoordinator));   // the link waits for the coordinator move
            r.hub.onCoordinatorMoved();
        } else {
            r.hub.onCoordinatorSessionChanged();                // in flight: re-offered on the new session
        }
        r.tick();
        ASSERT_EQ(r.endTxns(), 2u) << "the link did not re-offer EndTxn(commit)";
        ASSERT_EQ(r.lastEndCorr(), corr) << "the same request";
        r.endAnswer(ec(rpc::ErrorCode::InvalidRequest));
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::BrokerError);
        EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::InvalidRequest));
        EXPECT_EQ(r.state(), TxnState::FatalError) << "a re-offered commit that may have landed became abortable";
        Op abort = r.abort();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::BrokerError);
        r.ticks(4, kMs);
        EXPECT_EQ(r.endTxns(), 2u) << "an EndTxn(abort) over a commit that may have landed";
    }
}
