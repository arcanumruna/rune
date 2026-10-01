// tests/producer/ProducerCloseTest.cpp — F8c C6a: non-blocking begin-close.
//
// Producer::close(target, token) closes the intake SYNCHRONOUSLY (the producer
// ring's tail-bit CAS — linearizable with every claim; Direct: owner-local);
// the duty side drains the ring up to the closed tail (a record claimed before
// the close but committed after it is still read), flushes, waits for every
// accepted record, then completes the token Ok / Timeout / Closed.
//
// The TSan rigs drive a bare ProducerImpl: app threads claim / send, a control
// thread closes the intake (the linearization point), and ONE duty thread does
// everything duty-side (drain, flush, acks, applyBeginClose / failClose /
// closeImmediately — handed to it through test-side flags, the way the Client's
// command ring hands it the lifecycle command).  Nothing sleeps; every wait is
// an event wait or an iteration cap.
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using rc::AckToken;
using rc::OpResult;
using rc::OpStatus;
using rc::SendResult;
using rc::producer::ProducerImpl;
namespace ackhandle = rc::producer::ackhandle;
using test_support::AckLedger;

namespace {
std::atomic<int> g_ctr{0};
const std::vector<uint8_t> kVal(16, 0x77);
constexpr int64_t kNow = 1'000'000'000;

rc::ProducerConfig ackCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::Leader;
    c.lingerUs = 60'000'000;  // only a flush (or a closing producer) ends a batch
    c.batchBytes = 64 * 1024;
    c.maxInFlight = 8;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

/// The begin-close token's completions (a CompletionHandler target).
struct CloseLedger {
    std::vector<rc::Completion> got;
    size_t ackedAtClose = static_cast<size_t>(-1);  // record acks seen when the token completed
    const AckLedger* acks = nullptr;
    static void fn(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<CloseLedger*>(ctx);
        self->got.push_back(c);
        if (self->acks != nullptr) self->ackedAtClose = self->acks->recs.size();
    }
    rc::CompletionHandler handler() { return rc::CompletionHandler{&CloseLedger::fn, this}; }
    rc::detail::OpTarget target() {
        rc::detail::OpTarget t;
        t.handler = handler();
        return t;
    }
};

/// The batch ids a sink saw (a duty thread acks them).
struct CorrSink {
    std::vector<int64_t> corr;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rc::codec::decodeProduce(b, n);
            if (v) corr.push_back(v->correlationId);
            return true;
        };
    }
};

/// One duty pass of a bare producer: drain + flush + ack everything sent +
/// the close check (ProducerHub::drainAndFlush's order).
int dutyPass(ProducerImpl& impl, CorrSink& cs, const ProducerImpl::SendFn& sink, int64_t now = kNow,
             bool ack = true) {
    int w = impl.drainRing(now, 256);
    w += impl.flushBatches(now, 42, sink);
    if (ack) {
        for (const int64_t c : cs.corr) impl.onProduceResponse(c, 100);
        w += static_cast<int>(cs.corr.size());
        cs.corr.clear();
    }
    if (!impl.lifeOpen()) w += impl.closeDuty(now);
    return w;
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/producer_close_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P>
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            client->doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};
} // namespace

// ---------------------------------------------------------------------------
// Single-threaded semantics (bare producer, Ring and Direct)
// ---------------------------------------------------------------------------

// Regression: once begin-close returns, every later send is rejected Closed
// synchronously (void, token and Tier-2 forms) — and none of them completes.
TEST(ProducerClose, BeginCloseRejectsLaterSendsSynchronouslyRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        auto impl = std::make_shared<ProducerImpl>(ackCfg());
        if (direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
        rc::Producer p(impl);
        AckLedger L;
        L.attach(*impl);
        CloseLedger CL;
        const rc::TopicHandle topic{1, 1};
        ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{1}), SendResult::Ok);
        ASSERT_EQ(p.close(rc::CompletionTarget(CL.handler()), 77), OpResult::Accepted);
        EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Closed);
        EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{2}), SendResult::Closed);
        auto b = p.batch(topic, rc::Key(uint64_t{1}));
        b.add(kVal);
        EXPECT_EQ(b.send(AckToken{3}), OpResult::Closed);
        EXPECT_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal), std::logic_error);
        CorrSink cs;
        const auto sink = cs.fn();
        for (int i = 0; i < 100 && CL.got.empty(); ++i) (void)dutyPass(*impl, cs, sink);
        ASSERT_EQ(CL.got.size(), 1u);
        EXPECT_EQ(CL.got[0].status, OpStatus::Ok);
        EXPECT_EQ(L.times(1), 1);
        EXPECT_EQ(L.times(2), 0);
        EXPECT_EQ(L.times(3), 0);
    }
}

// Regression: the token completes Ok only AFTER the last accepted record's
// ack (the drain flushes at once — no linger — and waits for every ack), and
// strictly after every record completion.
TEST(ProducerClose, BeginCloseTokenCompletesAfterLastAckRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        ProducerImpl impl(ackCfg());
        if (direct) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
        AckLedger L;
        L.attach(impl);
        CloseLedger CL;
        CL.acks = &L;
        for (rc::Token t = 1; t <= 5; ++t) {
            ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(t), kVal, AckLedger::hi(t)), SendResult::Ok);
        }
        ASSERT_EQ(impl.beginClose(rc::CompletionTarget(CL.handler()), 9), OpResult::Accepted);
        CorrSink cs;
        const auto sink = cs.fn();
        (void)dutyPass(impl, cs, sink, kNow, /*ack=*/false);  // sent despite the 60 s linger
        ASSERT_EQ(cs.corr.size(), 1u) << "a closing producer flushes at once";
        EXPECT_TRUE(CL.got.empty()) << "records still in flight";
        (void)dutyPass(impl, cs, sink, kNow, /*ack=*/false);
        EXPECT_TRUE(CL.got.empty());
        impl.onProduceResponse(cs.corr[0], 500);  // the last ack
        cs.corr.clear();
        EXPECT_TRUE(CL.got.empty()) << "decided by the next duty pass, not inside the ack";
        (void)dutyPass(impl, cs, sink);
        ASSERT_EQ(CL.got.size(), 1u);
        EXPECT_EQ(CL.got[0].status, OpStatus::Ok);
        EXPECT_EQ(CL.got[0].op, rc::OpKind::ProducerClose);
        EXPECT_EQ(CL.got[0].token, 9u);
        EXPECT_EQ(CL.ackedAtClose, 5u) << "every record completed before the token";
        EXPECT_TRUE(L.exactlyOnce(1, 6));
        for (int i = 0; i < 10; ++i) (void)dutyPass(impl, cs, sink);
        EXPECT_EQ(CL.got.size(), 1u) << "exactly once";
    }
}

// Regression: a second begin-close (and one after closeImmediately) is
// rejected Closed synchronously with no completion; the first token completes
// exactly once.
TEST(ProducerClose, SecondBeginCloseIsClosedNoCompletionRegression) {
    ProducerImpl impl(ackCfg());
    CloseLedger first, second, third;
    ASSERT_EQ(impl.beginClose(rc::CompletionTarget(first.handler()), 1), OpResult::Accepted);
    EXPECT_EQ(impl.beginClose(rc::CompletionTarget(second.handler()), 2), OpResult::Closed);
    CorrSink cs;
    const auto sink = cs.fn();
    for (int i = 0; i < 10; ++i) (void)dutyPass(impl, cs, sink);
    ASSERT_EQ(first.got.size(), 1u);
    EXPECT_EQ(first.got[0].status, OpStatus::Ok);
    EXPECT_EQ(impl.closeImmediately(), OpResult::Accepted);  // idempotent, no token
    EXPECT_EQ(impl.beginClose(rc::CompletionTarget(third.handler()), 3), OpResult::Closed);
    for (int i = 0; i < 10; ++i) (void)dutyPass(impl, cs, sink);
    EXPECT_EQ(first.got.size(), 1u);
    EXPECT_TRUE(second.got.empty());
    EXPECT_TRUE(third.got.empty());
}

// Regression: begin-close's deadline (deliveryTimeoutMs from the duty side's
// apply, on the injected duty clock) completes the token Timeout when a
// record never acks; the record then completes on its own (DeliveryFailed).
TEST(ProducerClose, BeginCloseDeadlineCompletesTimeoutRegression) {
    ProducerImpl impl(ackCfg());
    AckLedger L;
    L.attach(impl);
    CloseLedger CL;
    ASSERT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    CorrSink cs;
    const auto sink = cs.fn();
    (void)dutyPass(impl, cs, sink, kNow, false);
    impl.applyBeginClose(CL.target(), 4, kNow);
    (void)impl.closeIntake();
    (void)dutyPass(impl, cs, sink, kNow + 9'999'000'000LL, false);
    EXPECT_TRUE(CL.got.empty());
    (void)dutyPass(impl, cs, sink, kNow + 10'000'000'000LL, false);
    ASSERT_EQ(CL.got.size(), 1u);
    EXPECT_EQ(CL.got[0].status, OpStatus::Timeout);
    EXPECT_EQ(L.times(1), 0) << "the record is still pending: Timeout only stops the close";
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{kNow + 20'000'000'000LL}});
    EXPECT_EQ(L.times(1), 1);
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
}

// ---------------------------------------------------------------------------
// Concurrency (TSan): claims racing the tail-bit close
// ---------------------------------------------------------------------------

// Regression (R3): app thread CLAIMS a record, the control thread begin-closes,
// the app thread COMMITS afterwards.  The token completes Ok only after that
// record was read, sent and acked — never before.  1000 iterations.
TEST(ProducerClose, BeginCloseWaitsForClaimedUncommittedRecordRegression) {
    for (int iter = 0; iter < 1000; ++iter) {
        ProducerImpl impl(ackCfg(), 64 * 1024);
        AckLedger L;  // written by the duty thread only
        L.attach(impl);
        CloseLedger CL;
        CL.acks = &L;
        std::atomic<int> phase{0};  // 1 claimed, 2 intake closed, 3 committed
        std::atomic<bool> applyClose{false};
        aeron::util::index_t idx = -1;
        std::thread app([&] {
            idx = impl.testClaimSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, AckLedger::h(1));
            phase.store(1, std::memory_order_release);
            while (phase.load(std::memory_order_acquire) < 2) std::this_thread::yield();  // event wait
            impl.testCommitClaim(idx);
            phase.store(3, std::memory_order_release);
        });
        std::thread control([&] {
            while (phase.load(std::memory_order_acquire) < 1) std::this_thread::yield();
            ASSERT_TRUE(impl.closeIntake());  // the linearization point: after the claim
            phase.store(2, std::memory_order_release);
            applyClose.store(true, std::memory_order_release);  // "the lifecycle command"
        });
        bool closeApplied = false;
        std::thread duty([&] {
            CorrSink cs;
            const auto sink = cs.fn();
            for (int i = 0; i < 2'000'000 && CL.got.empty(); ++i) {
                if (!closeApplied && applyClose.load(std::memory_order_acquire)) {
                    impl.applyBeginClose(CL.target(), 7, kNow);
                    closeApplied = true;
                }
                (void)dutyPass(impl, cs, sink);
                if (i % 64 == 0) std::this_thread::yield();
            }
        });
        app.join();
        control.join();
        duty.join();
        ASSERT_GE(idx, 0) << "iter " << iter;
        ASSERT_EQ(CL.got.size(), 1u) << "iter " << iter;
        EXPECT_EQ(CL.got[0].status, OpStatus::Ok) << "iter " << iter;
        ASSERT_EQ(L.times(1), 1) << "iter " << iter;
        EXPECT_EQ(L.status(1), OpStatus::Ok);
        EXPECT_EQ(CL.ackedAtClose, 1u) << "the record's ack precedes the token, iter " << iter;
    }
}

// Regression: app threads send acked records while a begin-close and then a
// closeImmediately race them; the duty thread applies both.  Every ACCEPTED
// record completes exactly once (Ok if acked before the abort, else Closed),
// every rejected one never; the token completes once (Ok or Closed).
TEST(ProducerClose, BeginCloseThenCloseImmediatelyCompletesEachRecordOnceRegression) {
    for (int iter = 0; iter < 200; ++iter) {
        ProducerImpl impl(ackCfg(), 64 * 1024);
        AckLedger L;
        L.attach(impl);
        CloseLedger CL;
        constexpr int kThreads = 2, kPer = 200;
        std::atomic<int> accepted[kThreads] = {};
        std::vector<std::vector<rc::Token>> acceptedToks(kThreads);
        std::atomic<bool> go{false}, beginClose{false}, closeNow{false};
        std::atomic<int>  appsDone{0};
        std::vector<std::thread> apps;
        for (int a = 0; a < kThreads; ++a) {
            apps.emplace_back([&, a] {
                while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
                for (int i = 0; i < kPer; ++i) {
                    const rc::Token t = static_cast<rc::Token>(a * 100'000 + i + 1);
                    const SendResult r = impl.trySubmit(1, a, rc::Key(t), kVal, AckLedger::hi(t));
                    if (r == SendResult::Ok) {
                        acceptedToks[a].push_back(t);
                        accepted[a].fetch_add(1);
                    }
                }
                appsDone.fetch_add(1, std::memory_order_release);
            });
        }
        std::thread control([&] {
            go.store(true, std::memory_order_release);
            for (int i = 0; i < iter % 50; ++i) std::this_thread::yield();
            (void)impl.closeIntake();
            beginClose.store(true, std::memory_order_release);
            for (int i = 0; i < iter % 7; ++i) std::this_thread::yield();
            closeNow.store(true, std::memory_order_release);
        });
        std::thread duty([&] {
            CorrSink cs;
            const auto sink = cs.fn();
            bool began = false, aborted = false;
            for (int i = 0; i < 4'000'000; ++i) {
                if (!began && beginClose.load(std::memory_order_acquire)) {
                    impl.applyBeginClose(CL.target(), 5, kNow);
                    began = true;
                }
                if (began && !aborted && closeNow.load(std::memory_order_acquire)) {
                    impl.applyCloseImmediately(OpStatus::Closed);
                    aborted = true;
                }
                (void)dutyPass(impl, cs, sink, kNow, /*ack=*/(i % 3) == 0);
                if (aborted && appsDone.load(std::memory_order_acquire) == kThreads &&
                    impl.testRingHead() >= impl.testRingTail()) {
                    break;
                }
            }
        });
        for (auto& t : apps) t.join();
        control.join();
        duty.join();
        ASSERT_EQ(CL.got.size(), 1u) << "iter " << iter;
        EXPECT_TRUE(CL.got[0].status == OpStatus::Ok || CL.got[0].status == OpStatus::Closed);
        size_t total = 0;
        for (int a = 0; a < kThreads; ++a) {
            for (const rc::Token t : acceptedToks[a]) {
                ASSERT_EQ(L.times(t), 1) << "accepted token " << t << " iter " << iter;
                const OpStatus st = L.status(t);
                EXPECT_TRUE(st == OpStatus::Ok || st == OpStatus::Closed);
            }
            total += acceptedToks[a].size();
        }
        EXPECT_EQ(L.recs.size(), total) << "only accepted records complete, iter " << iter;
        EXPECT_TRUE(L.noneDuplicated());
    }
}

// Regression: the DUTY thread tail-closes the intake on its failure path
// (failClose — T15: Aeron client closed) while app threads keep claiming.
// Every claim made before the close (including one committed after it)
// completes Closed exactly once; every send after it is rejected and never
// completes; an app begin-close racing it either wins (token Closed via the
// failure) or loses with OpResult::Closed.
TEST(ProducerClose, DutyTailCloseOnFailureCompletesPreCloseClaimsClosedOnceRegression) {
    for (int iter = 0; iter < 200; ++iter) {
        ProducerImpl impl(ackCfg(), 64 * 1024);
        AckLedger L;
        L.attach(impl);
        CloseLedger CL;
        constexpr int kThreads = 2, kPer = 300;
        std::vector<std::vector<rc::Token>> acceptedToks(kThreads);
        std::atomic<int>  appsDone{0};
        std::atomic<bool> go{false}, failNow{false};
        std::atomic<int>  beginRes{-1};
        std::atomic<bool> applyBegin{false};
        std::vector<std::thread> apps;
        for (int a = 0; a < kThreads; ++a) {
            apps.emplace_back([&, a] {
                while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
                for (int i = 0; i < kPer; ++i) {
                    const rc::Token t = static_cast<rc::Token>(a * 100'000 + i + 1);
                    if (i == kPer / 2 && a == 0) {
                        // a claim that commits late (possibly after the close)
                        const auto idx = impl.testClaimSendRecord(1, a, rc::Key(t), kVal, AckLedger::h(t));
                        for (int y = 0; y < 50; ++y) std::this_thread::yield();
                        if (idx >= 0) {
                            impl.testCommitClaim(idx);
                            acceptedToks[a].push_back(t);
                        }
                        continue;
                    }
                    if (impl.trySubmit(1, a, rc::Key(t), kVal, AckLedger::hi(t)) == SendResult::Ok) {
                        acceptedToks[a].push_back(t);
                    }
                }
                appsDone.fetch_add(1, std::memory_order_release);
            });
        }
        std::thread control([&] {
            go.store(true, std::memory_order_release);
            for (int i = 0; i < iter % 40; ++i) std::this_thread::yield();
            failNow.store(true, std::memory_order_release);
            // An app begin-close racing the failure: the CAS decides.
            const bool won = impl.closeIntake();
            beginRes.store(won ? 1 : 0, std::memory_order_release);
            if (won) applyBegin.store(true, std::memory_order_release);
        });
        std::thread duty([&] {
            CorrSink cs;
            const auto sink = cs.fn();
            bool failed = false, began = false;
            for (int i = 0; i < 4'000'000; ++i) {
                if (!began && applyBegin.load(std::memory_order_acquire)) {
                    impl.applyBeginClose(CL.target(), 8, kNow);
                    began = true;
                }
                if (!failed && failNow.load(std::memory_order_acquire) &&
                    beginRes.load(std::memory_order_acquire) >= 0) {
                    impl.failClose(OpStatus::Closed);  // idempotent tail close + fail all
                    failed = true;
                }
                (void)dutyPass(impl, cs, sink, kNow, /*ack=*/false);  // nothing acks: all must close
                const int br = beginRes.load(std::memory_order_acquire);
                const bool beginSettled = br == 0 || (br == 1 && began);
                if (failed && beginSettled && appsDone.load(std::memory_order_acquire) == kThreads &&
                    impl.testRingHead() >= impl.testRingTail()) {
                    break;
                }
            }
        });
        for (auto& t : apps) t.join();
        control.join();
        duty.join();
        const bool won = beginRes.load() == 1;
        EXPECT_EQ(CL.got.size(), won ? 1u : 0u) << "iter " << iter;
        // Won: Closed when the failure aborted the drain; Ok only if the drain
        // already found nothing outstanding (no record accepted before the close).
        if (won && !CL.got.empty()) {
            EXPECT_TRUE(CL.got[0].status == OpStatus::Closed ||
                        (CL.got[0].status == OpStatus::Ok && acceptedToks[0].empty() && acceptedToks[1].empty()))
                << "iter " << iter;
        }
        size_t total = 0;
        for (int a = 0; a < kThreads; ++a) {
            for (const rc::Token t : acceptedToks[a]) {
                ASSERT_EQ(L.times(t), 1) << "accepted token " << t << " iter " << iter;
                EXPECT_EQ(L.status(t), OpStatus::Closed);
            }
            total += acceptedToks[a].size();
        }
        EXPECT_EQ(L.recs.size(), total) << "only claims before the close complete, iter " << iter;
    }
}

// ---------------------------------------------------------------------------
// Through the Client (Invoker): the CQ target, the blocking close and close
// interacting with Client::close().
// ---------------------------------------------------------------------------

// Regression: Producer::close(cq, token) completes Ok into the queue after
// the records' acks; the blocking close() of a second producer returns Ok;
// a begin-close pending at Client::close() completes Closed exactly once.
TEST(ProducerClose, ClientBeginCloseCqBlockingCloseAndClientCloseRegression) {
    Rig r(invokerCfg(4420, 4421));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    test_support::TestCq cq(*r.client);
    auto rec = cq.send(p, topic, rc::Key(uint64_t{1}), kVal);
    ASSERT_TRUE(rec.accepted());
    auto tok = cq.issue([&](rc::Completions& q, rc::Token t) { return p.close(q, t); });
    ASSERT_TRUE(tok.accepted()) << tok.describe();
    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Closed);
    ASSERT_TRUE(r.drive([&] { return tok.ready(); }));
    EXPECT_EQ(tok.status(), OpStatus::Ok);
    ASSERT_TRUE(rec.ready()) << "the record completed before the token";
    EXPECT_EQ(rec.status(), OpStatus::Ok);
    auto again = cq.issue([&](rc::Completions& q, rc::Token t) { return p.close(q, t); });
    EXPECT_EQ(again.issued(), OpResult::Closed);
    EXPECT_EQ(p.close(), OpStatus::Closed) << "blocking close of a closed producer returns at once";

    auto p2 = r.client->producer(pcfg);
    auto rec2 = cq.send(p2, topic, rc::Key(uint64_t{2}), kVal);
    ASSERT_TRUE(rec2.accepted());
    p2.flush();
    ASSERT_TRUE(r.drive([&] { return rec2.ready(); }));
    EXPECT_EQ(rec2.status(), OpStatus::Ok);
    // Nothing outstanding: the blocking close pumps the owner's doWork() and
    // returns as soon as the drain completes.
    EXPECT_EQ(p2.close(), OpStatus::Ok);

    r.gw.replyAcks = false;
    auto p3 = r.client->producer(pcfg);
    auto rec3 = cq.send(p3, topic, rc::Key(uint64_t{3}), kVal);
    auto tok3 = cq.issue([&](rc::Completions& q, rc::Token t) { return p3.close(q, t); });
    ASSERT_TRUE(tok3.accepted());
    for (int i = 0; i < 2'000; ++i) { r.client->doWork(); r.gw.pump(); }
    r.client->close();
    ASSERT_TRUE(rec3.ready());
    EXPECT_EQ(rec3.status(), OpStatus::Closed);
    ASSERT_TRUE(tok3.ready());
    EXPECT_EQ(tok3.status(), OpStatus::Closed);
    EXPECT_EQ(cq.timesCompleted(tok3.token()), 1);
    EXPECT_EQ(p3.closeImmediately(), OpResult::Closed) << "after Client close: never touches the Client";
    EXPECT_EQ(p3.close(), OpStatus::Closed);
}

// Regression: closeImmediately in Thread mode (applied by the runner through
// the lifecycle command) fails every unacknowledged record Closed and aborts a
// pending begin-close (token Closed), exactly once each.
TEST(ProducerClose, ThreadModeCloseImmediatelyAbortsDrainRegression) {
    rc::ClientConfig c = invokerCfg(4422, 4423);
    c.runnerMode = rc::RunnerMode::Thread;
    rc::Client client(c);  // never connected: nothing can be sent
    auto p = client.producer(ackCfg());
    const auto topic = client.topic("t");
    test_support::TestCq cq(client);
    std::vector<test_support::PendingOp> recs;
    for (int i = 0; i < 20; ++i) recs.push_back(cq.send(p, topic, rc::Key(static_cast<uint64_t>(i)), kVal));
    auto tok = cq.issue([&](rc::Completions& q, rc::Token t) { return p.close(q, t); });
    ASSERT_TRUE(tok.accepted());
    EXPECT_EQ(p.closeImmediately(), OpResult::Accepted);
    ASSERT_TRUE(tok.waitReady());
    EXPECT_EQ(tok.status(), OpStatus::Closed);
    for (const auto& o : recs) {
        ASSERT_TRUE(o.accepted());
        ASSERT_TRUE(o.waitReady()) << o.describe();
        EXPECT_EQ(o.status(), OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(o.token()), 1);
    }
    client.close();
    EXPECT_EQ(cq.timesCompleted(tok.token()), 1);
}
