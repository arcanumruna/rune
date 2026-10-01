// tests/producer/ProducerAckTokenTest.cpp — F8c C6: producer acks as tagged
// 64-bit tokens (no PendingAck / std::function / promise), delivered to a
// Completions queue or the producer's onAck handler, exactly once.
//
// Bare-ProducerImpl rigs are single-threaded (the test is the duty thread);
// Client rigs are Invoker mode against the single-threaded InvokerGateway.
// Every drive loop is iteration-capped; nothing sleeps.
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
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
const std::vector<uint8_t> kVal(16, 0x5A);
constexpr int64_t kNow = 1'000'000'000;

rc::ProducerConfig ackCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::Leader;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = 8;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg, int32_t partitions = 1) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/ack_token_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = partitions;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    // cap < 0 (default): drive until pred holds, bounded by a 10 s deadline that
    // only a FAILING test reaches (iteration counts depend on CPU speed).
    template <class P>
    bool drive(P&& pred, int cap = -1) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int i = 0; cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap; ++i) {
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

/// The batch ids a sink saw (bare rigs ack them by hand).
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

int pumpBare(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int64_t now = kNow) {
    int f = 0;
    for (int i = 0; i < 10'000; ++i) {
        const int d = impl.drainRing(now);
        f += impl.flushBatches(now, 42, sink);
        if (d == 0) break;
    }
    return f;
}

/// A standalone Completions as the CQ registry of a bare ProducerImpl: the
/// hooks deliver producer completions straight into its ring (slot 0).
struct BareCq {
    rc::Completions q{[] {
        rc::CompletionsConfig c;
        c.capacityBytes = 1u << 16;
        c.maxResultBytes = 64;
        c.maxErrorText = 64;
        return c;
    }()};
    uint64_t delivered = 0;
    static void deliver(void* ctx, rc::CqRef, const rc::Completion& c) noexcept {
        auto* self = static_cast<BareCq*>(ctx);
        (void)self->q.ring().deliver(c, {});
        ++self->delivered;
    }
    void attach(ProducerImpl& impl) {
        ProducerImpl::Hooks h;
        h.ctx = this;
        h.cqDeliver = &BareCq::deliver;
        impl.setHooks(h);
    }
};
} // namespace

// ---------------------------------------------------------------------------
// The encoding
// ---------------------------------------------------------------------------

// Regression: every (CqRef, Token) and every handler Token round-trips, the
// two target kinds never collide, and no ack handle is ever 0 (0 = void).
// RED (mutation): kRefShift 47 (the spec sketch's overlapping layout) breaks
// the max-token round trip; dropping kHandlerBit makes forHandler(0) == 0.
TEST(ProducerAckToken, AckHandleEncodingRoundTripsAndNeverCollidesRegression) {
    const rc::Token tokens[] = {0, 1, 0xABCDEF, rc::kMaxToken};
    const rc::CqRef refs[]   = {0, 1, rc::makeCqRef(1023, 31), rc::makeCqRef(5, 7)};
    for (const rc::Token t : tokens) {
        const uint64_t hh = ackhandle::forHandler(t);
        EXPECT_NE(hh, 0u);
        EXPECT_FALSE(ackhandle::isCq(hh));
        EXPECT_EQ(ackhandle::token(hh), t);
        for (const rc::CqRef r : refs) {
            const uint64_t hc = ackhandle::forCq(r, t);
            EXPECT_NE(hc, 0u);
            EXPECT_TRUE(ackhandle::isCq(hc));
            EXPECT_EQ(ackhandle::cqRef(hc), r);
            EXPECT_EQ(ackhandle::token(hc), t);
            EXPECT_NE(hc, hh);
        }
    }
}

// Regression: a Completions-target ack and a handler-target ack with the SAME
// token, plus a second queue with the same token, each complete exactly once
// in their own target (value = the record's own offset).
TEST(ProducerAckToken, AckTokenDecodedToRightTargetRegression) {
    Rig r(invokerCfg(4400, 4401));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    auto p = r.client->producer(pcfg);
    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    rc::Completions a(*r.client), b(*r.client);
    const auto topic = r.client->topic("t");
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, a, 5), SendResult::Ok);
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, b, 5), SendResult::Ok);
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{5}), SendResult::Ok);
    std::vector<rc::Completion> ga, gb;
    auto pollAll = [&] {
        (void)a.poll([&](const rc::Completion& c, std::span<const uint8_t>) { ga.push_back(c); });
        (void)b.poll([&](const rc::Completion& c, std::span<const uint8_t>) { gb.push_back(c); });
        return ga.size() + gb.size() + L.recs.size() >= 3;
    };
    ASSERT_TRUE(r.drive(pollAll));
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); (void)pollAll(); }
    ASSERT_EQ(ga.size(), 1u);
    ASSERT_EQ(gb.size(), 1u);
    ASSERT_EQ(L.recs.size(), 1u);
    EXPECT_EQ(ga[0].token, 5u);
    EXPECT_EQ(gb[0].token, 5u);
    EXPECT_EQ(L.recs[0].token, 5u);
    EXPECT_EQ(ga[0].op, rc::OpKind::Send);
    EXPECT_EQ(ga[0].status, OpStatus::Ok);
    EXPECT_EQ(gb[0].status, OpStatus::Ok);
    EXPECT_EQ(L.recs[0].status, OpStatus::Ok);
    // Three distinct offsets (each record's own).
    EXPECT_NE(ga[0].value, gb[0].value);
    EXPECT_NE(ga[0].value, L.recs[0].value);
    EXPECT_EQ(a.outstanding(), 0u);
    EXPECT_EQ(b.outstanding(), 0u);
    EXPECT_EQ(a.reservedBytes(), 0u) << "credit returned on poll";
}

// Regression: a rejected trySend (ring / builder full, closed, bad token, no
// credit) returns its credit at once and never completes.  RED (mutation):
// dropping the unreserve() on the rejected path leaves reservedBytes != 0.
TEST(ProducerAckToken, RejectedTrySendReturnsCreditAndNoCompletionRegression) {
    Rig r(invokerCfg(4402, 4403));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;   // nothing leaves: the builder fills
    pcfg.batchBytes = 256;
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    rc::Completions cq(*r.client);
    rc::Token t = 1;
    SendResult res = SendResult::Ok;
    uint32_t accepted = 0;
    for (; t < 10'000 && res == SendResult::Ok; ++t) {
        res = p.trySend(topic, rc::Key(uint64_t{1}), kVal, cq, t);
        if (res == SendResult::Ok) ++accepted;
    }
    ASSERT_EQ(res, SendResult::BackPressured);
    const rc::Token rejectedTok = t - 1;
    EXPECT_EQ(cq.outstanding(), accepted) << "the rejected send returned its reservation";
    EXPECT_EQ(cq.reservedBytes(), accepted * rc::producer::kAckRecordBytes);

    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, cq, rc::kMaxToken + 1), SendResult::InvalidArgument);
    EXPECT_EQ(cq.outstanding(), accepted);

    // No credit: a queue whose capacity holds only a few records.
    rc::CompletionsConfig tiny;
    tiny.capacityBytes = 1024;
    tiny.maxResultBytes = 64;
    tiny.maxErrorText = 64;
    rc::Completions small(*r.client, tiny);
    auto p2 = r.client->producer(ackCfg());
    SendResult sr = SendResult::Ok;
    uint32_t smallAccepted = 0;
    for (rc::Token k = 1; k < 1'000 && sr == SendResult::Ok; ++k) {
        sr = p2.trySend(topic, rc::Key(uint64_t{1}), kVal, small, k);
        if (sr == SendResult::Ok) ++smallAccepted;
    }
    EXPECT_EQ(sr, SendResult::QueueFull);
    EXPECT_EQ(small.outstanding(), smallAccepted);

    // Everything accepted completes exactly once; the rejected token never.
    p.flush();
    std::vector<rc::Token> got;
    ASSERT_TRUE(r.drive([&] {
        (void)cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got.push_back(c.token); });
        return got.size() >= accepted;
    }));
    for (int i = 0; i < 1'000; ++i) {
        r.client->doWork();
        r.gw.pump();
        (void)cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got.push_back(c.token); });
    }
    EXPECT_EQ(got.size(), accepted);
    EXPECT_EQ(std::count(got.begin(), got.end(), rejectedTok), 0);
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// Regression: a delivery timeout completes DeliveryFailed exactly once — for
// an in-flight batch, a never-sent builder record and a never-sent Tier-2
// batch — and a late OK ack afterwards completes nothing.
TEST(ProducerAckToken, DeliveryTimeoutCompletesDeliveryFailedOnceRegression) {
    auto impl = std::make_shared<ProducerImpl>(ackCfg());
    AckLedger L;
    L.attach(*impl);
    CorrSink cs;
    const auto sink = cs.fn();
    ASSERT_TRUE(impl->submitSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    pumpBare(*impl, sink);  // in flight
    ASSERT_EQ(cs.corr.size(), 1u);
    ASSERT_TRUE(impl->submitSendRecord(1, 1, rc::Key(uint64_t{1}), kVal, true, AckLedger::hi(2)));
    rc::BatchBuilder bb(impl, 1, 2);
    bb.add(kVal);
    ASSERT_EQ(bb.send(AckToken{3}), OpResult::Accepted);
    for (int i = 0; i < 10 && impl->drainRing(kNow) > 0; ++i) {}
    (void)impl->flushBatches(kNow, /*sessionId=*/0, sink);  // no session: 2 and 3 never sent
    impl->sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{kNow + 10'000'000'000LL}});
    for (rc::Token t = 1; t <= 3; ++t) {
        ASSERT_EQ(L.times(t), 1) << t;
        EXPECT_EQ(L.status(t), OpStatus::DeliveryFailed) << t;
        EXPECT_EQ(L.value(t), -1) << t;
    }
    impl->onProduceResponse(cs.corr[0], 100);  // late OK ack
    for (rc::Token t = 1; t <= 3; ++t) EXPECT_EQ(L.times(t), 1) << t;
    EXPECT_TRUE(L.noneDuplicated());
}

// Regression (Task 14 arena): an ack-token send made from inside the
// producer's own duty work is deferred, applied after the walk, sent, and
// completes exactly once — and a deferred record that can never be applied
// completes DeliveryFailed exactly once.
TEST(ProducerAckToken, DeferredSendAckCompletesOnceRegression) {
    rc::ProducerConfig cfg = ackCfg();
    ProducerImpl impl(cfg);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
    test_support::ScriptedAcks S;
    S.attach(impl);
    CorrSink cs;
    const auto sink = cs.fn();
    int64_t inner = 0;
    int     innerFires = 0;
    const int64_t outer = S.cb([&](int64_t) {
        const int64_t h = S.cb([&](int64_t off) {
            inner = off;
            ++innerFires;
        });
        ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{2}), kVal, h), SendResult::Ok)
            << "an in-duty send is deferred, not rejected";
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, outer), SendResult::Ok);
    (void)impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cs.corr.size(), 1u);
    impl.onProduceResponse(cs.corr[0], 100);  // outer fires inside duty work: inner deferred
    EXPECT_EQ(impl.deferredCount(), 0u) << "applied at the end of the duty scope";
    (void)impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cs.corr.size(), 2u);
    impl.onProduceResponse(cs.corr[1], 200);
    impl.onProduceResponse(cs.corr[1], 200);  // duplicate
    EXPECT_EQ(innerFires, 1);
    EXPECT_EQ(inner, 200);
    EXPECT_TRUE(S.noneDuplicated());

    // Forced apply failure: completes DeliveryFailed (-1) exactly once.
    int lostFires = 0;
    int64_t lost = 0;
    const int64_t outer2 = S.cb([&](int64_t) {
        const int64_t h = S.cb([&](int64_t off) {
            lost = off;
            ++lostFires;
        });
        (void)impl.trySubmit(1, 3, rc::Key(uint64_t{3}), kVal, h);
        impl.debugFailDeferredApplies(1, 1);
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, outer2), SendResult::Ok);
    (void)impl.flushBatches(kNow, 42, sink);
    impl.onProduceResponse(cs.corr.back(), 300);
    EXPECT_EQ(lostFires, 1);
    EXPECT_EQ(lost, -1);
    impl.failAllPending(OpStatus::Closed);
    EXPECT_EQ(lostFires, 1);
    EXPECT_TRUE(S.noneDuplicated());
}

// Regression: a Tier-2 batch acks through a token — CQ form (op Tier2Batch,
// value = baseOffset, aux = recordCount) and handler form — exactly once.
TEST(ProducerAckToken, Tier2AckViaTokenRegression) {
    Rig r(invokerCfg(4404, 4405));
    r.connect();
    auto p = r.client->producer(ackCfg());
    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    const auto topic = r.client->topic("t");
    test_support::TestCq cq(*r.client);
    auto b1 = p.batch(topic, rc::Key(uint64_t{1}));
    for (int i = 0; i < 3; ++i) b1.add(kVal);
    auto f = cq.send(b1);
    ASSERT_TRUE(f.accepted());
    auto b2 = p.batch(topic, rc::Key(uint64_t{1}));
    for (int i = 0; i < 4; ++i) b2.add(kVal);
    ASSERT_EQ(b2.send(AckToken{9}), OpResult::Accepted);
    EXPECT_THROW(b2.send(AckToken{10}), std::logic_error) << "a consumed batch cannot be re-sent";
    ASSERT_TRUE(r.drive([&] { return f.ready() && L.times(9) == 1; }));
    EXPECT_EQ(f.outcome().c.op, rc::OpKind::Tier2Batch);
    EXPECT_EQ(f.outcome().c.aux, 3);
    EXPECT_GE(f.offset(), 0);
    EXPECT_EQ(L.status(9), OpStatus::Ok);
    EXPECT_EQ(L.value(9), f.offset() + 3) << "the batches land back to back";
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(L.times(9), 1);
}

// Regression: Client close completes every accepted ack token Closed exactly
// once — records still in the producer ring, in a builder, a queued Tier-2
// batch and in-flight batches — BEFORE the queue is handed back (finished()
// after draining, nothing dropped as stale).
TEST(ProducerAckToken, CloseFailsAllInflightAcksClosedRegression) {
    Rig r(invokerCfg(4406, 4407, 2));
    r.connect();
    r.gw.replyAcks = false;  // in-flight batches stay in flight
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.maxInFlight = 1;
    auto direct = r.client->producer(pcfg);
    auto ring   = r.client->testProducerWithRingCap(pcfg, 64 * 1024);
    const auto topic = r.client->topic("t");
    test_support::TestCq cq(*r.client);
    std::vector<test_support::PendingOp> ops;
    for (int i = 0; i < 3; ++i) ops.push_back(cq.send(direct, topic, rc::Key(uint64_t{1}), kVal));
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty(); }));  // one in flight, the rest built
    auto b = direct.batch(topic, rc::Key(uint64_t{1}));
    b.add(kVal);
    ops.push_back(cq.send(b));                                        // queued Tier-2
    for (int i = 0; i < 5; ++i) ops.push_back(cq.send(ring, topic, rc::Key(uint64_t{2}), kVal));  // in the ring
    for (const auto& o : ops) ASSERT_TRUE(o.accepted()) << o.describe();
    const uint64_t stale0 = r.client->testStaleCqDrops();
    r.client->close();
    EXPECT_EQ(r.client->testStaleCqDrops(), stale0) << "no producer completion after the hand-back";
    for (const auto& o : ops) {
        ASSERT_TRUE(o.ready()) << o.describe();
        EXPECT_EQ(o.status(), OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(o.token()), 1);
    }
    EXPECT_TRUE(cq.q().finished());
    EXPECT_EQ(cq.q().outstanding(), 0u);
}

// Regression (C6 slot quarantine): a released queue's registry slot is not
// recycled while a producer op addressed to it is still in the producer ring
// (producer ops travel the producer ring, not the command ring, so the I3'
// fence cannot order them): its stale completion is dropped and only then is
// the slot reused.  RED (mutation): ignoring prodIssued in the quarantine
// check hands the slot to the churned queues at once.
TEST(ProducerAckToken, ReleasedCqSlotQuarantinedUntilProducerOpDeliveredRegression) {
    Rig r(invokerCfg(4408, 4409));
    r.connect();
    rc::CompletionsConfig tiny;
    tiny.capacityBytes = 1024;
    tiny.maxResultBytes = 64;
    tiny.maxErrorText = 64;
    std::vector<std::unique_ptr<rc::Completions>> live;
    for (uint32_t i = 0; i < rc::kMaxCqs - 2; ++i) live.push_back(std::make_unique<rc::Completions>(*r.client, tiny));
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;
    auto ring = r.client->testProducerWithRingCap(pcfg, 64 * 1024);  // nothing drains until doWork
    const auto topic = r.client->topic("t");
    auto a = std::make_unique<rc::Completions>(*r.client, tiny);
    const uint32_t aIdx = rc::cqRefIndex(a->cqRef());
    ASSERT_EQ(ring.trySend(topic, rc::Key(uint64_t{1}), kVal, *a, 1), SendResult::Ok);
    a.reset();  // released with the op still in the producer ring (Invoker owner: applied directly)
    for (int i = 0; i < 64; ++i) {
        rc::Completions churn(*r.client, tiny);
        EXPECT_NE(rc::cqRefIndex(churn.cqRef()), aIdx) << "quarantined slot reused at churn " << i;
    }
    const uint64_t stale0 = r.client->testStaleCqDrops();
    ring.flush();
    ASSERT_TRUE(r.drive([&] { return r.client->testStaleCqDrops() > stale0; })) << "stale completion dropped";
    bool reused = false;
    for (int i = 0; i < 8 && !reused; ++i) {
        rc::Completions churn(*r.client, tiny);
        reused = rc::cqRefIndex(churn.cqRef()) == aIdx;
    }
    EXPECT_TRUE(reused) << "the slot returns to the free ring once its producer op is accounted";
}

// ---------------------------------------------------------------------------
// Zero allocations on the acked path (extends ProducerAllocRegressionTest)
// ---------------------------------------------------------------------------

namespace {
template <class Send>
long ackedSteadyStateAllocs(ProducerImpl& impl, Send&& send) {
    CorrSink cs;
    cs.corr.reserve(4096);
    const auto sink = cs.fn();
    auto cycle = [&](uint64_t k) {
        send(k);
        pumpBare(impl, sink);
        impl.onProduceResponse(cs.corr.back(), static_cast<int64_t>(k));
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);  // warm-up
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    return alloc_counter::stop();
}
} // namespace

// Regression: every acked send allocated one PendingAck (new + std::function
// or std::promise) and freed it at completion.  The token path allocates
// nothing: handler form, Ring and Direct.  RED: the parent's registerCallback
// path measured 2 allocations per acked record with the same harness (see the
// C6 report).
TEST(ProducerAllocRegression, AckedHandlerPathAllocatesNothingRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        ProducerImpl impl(ackCfg());
        if (direct) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
        uint64_t fired = 0;
        impl.setOnAck([](void* c, rc::Token, int64_t, OpStatus, uint16_t) noexcept { ++*static_cast<uint64_t*>(c); },
                      &fired);
        const long allocs = ackedSteadyStateAllocs(impl, [&](uint64_t k) {
            ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(k), kVal, static_cast<int64_t>(ackhandle::forHandler(k))),
                      SendResult::Ok);
        });
        EXPECT_EQ(allocs, 0L) << "acked (handler) steady state must not allocate";
        EXPECT_EQ(fired, 1064u);
        EXPECT_FALSE(impl.hasInFlight());
    }
}

// Same for the Completions form: the duty side writes each completion into the
// queue's preallocated ring; the app polls it (no allocation either side).
TEST(ProducerAllocRegression, AckedCqPathAllocatesNothingRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        ProducerImpl impl(ackCfg());
        if (direct) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
        BareCq bq;
        bq.attach(impl);
        uint64_t polled = 0;
        const long allocs = ackedSteadyStateAllocs(impl, [&](uint64_t k) {
            ASSERT_EQ(bq.q.reserve(rc::producer::kAckRecordBytes), OpResult::Accepted);
            ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(k), kVal, static_cast<int64_t>(ackhandle::forCq(0, k))),
                      SendResult::Ok);
            polled += static_cast<uint64_t>(bq.q.poll([](const rc::Completion&, std::span<const uint8_t>) {}));
        });
        polled += static_cast<uint64_t>(bq.q.poll([](const rc::Completion&, std::span<const uint8_t>) {}));
        EXPECT_EQ(allocs, 0L) << "acked (Completions) steady state must not allocate";
        EXPECT_EQ(bq.delivered, 1064u);
        EXPECT_EQ(polled, 1064u);
        EXPECT_EQ(bq.q.ring().lostCompletions(), 0u);
        EXPECT_EQ(bq.q.outstanding(), 0u);
    }
}

// ---------------------------------------------------------------------------
// F8c fix round 1 (review I2): batchAcksTo / batchAckOf / the BatchAck credit
// and "every accepted record in exactly one BatchAck".
// ---------------------------------------------------------------------------

namespace {
rc::CompletionsConfig smallCq(uint32_t cap = 1u << 16) {
    rc::CompletionsConfig c;
    c.capacityBytes = cap;
    c.maxResultBytes = 64;
    c.maxErrorText = 64;
    return c;
}
struct BatchSeen {
    std::vector<rc::BatchAck> acks;
    std::vector<rc::Completion> raw;
    int64_t records() const {
        int64_t n = 0;
        for (const auto& a : acks) n += a.recordCount;
        return n;
    }
};
int pollBatches(rc::Completions& q, BatchSeen& s) {
    return q.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
        s.raw.push_back(c);
        if (c.op == rc::OpKind::BatchAck) s.acks.push_back(rc::batchAckOf(c));
    });
}
} // namespace

// Regression: a bound producer's void, acked and Tier-2 sends each reserve one
// unit; once acked and polled every unit is returned (outstanding 0, reserved
// 0), batchAckOf decodes topic / partition / sequence / count, and the
// BatchAcks cover every record exactly once.  RED (mutation): consumeCredit_
// returning one op per BatchAck (not reserved0) leaves outstanding != 0.
TEST(ProducerBatchAcks, BatchAcksToRoundTripCreditExactRegression) {
    Rig r(invokerCfg(4410, 4411, 2));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;
    auto p = r.client->producer(pcfg);
    rc::Completions bq(*r.client, smallCq());
    p.batchAcksTo(bq);
    test_support::TestCq cq(*r.client);
    const auto topic = r.client->topic("t");
    const rc::Key k0 = [] { for (uint64_t k = 0;; ++k) if (rc::Key(k).partition(2) == 0) return rc::Key(k); }();
    const rc::Key k1 = [] { for (uint64_t k = 0;; ++k) if (rc::Key(k).partition(2) == 1) return rc::Key(k); }();
    for (int i = 0; i < 4; ++i) ASSERT_EQ(p.trySend(topic, k0, kVal), SendResult::Ok);
    auto acked = cq.send(p, topic, k0, kVal);
    ASSERT_TRUE(acked.accepted());
    for (int i = 0; i < 2; ++i) ASSERT_EQ(p.trySend(topic, k1, kVal), SendResult::Ok);
    p.flush();
    ASSERT_TRUE(r.drive([&] { return acked.ready(); }));
    auto b = p.batch(topic, k1);
    b.add(kVal);
    b.add(kVal);
    b.add(kVal);
    auto t2 = cq.send(b);
    ASSERT_TRUE(t2.accepted());
    EXPECT_EQ(bq.outstanding(), 8u) << "4 + 1 + 2 Tier-1 records + 1 Tier-2 batch";
    BatchSeen seen;
    ASSERT_TRUE(r.drive([&] {
        (void)pollBatches(bq, seen);
        return t2.ready() && seen.records() >= 10;
    }));
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); (void)pollBatches(bq, seen); }
    EXPECT_EQ(seen.records(), 10) << "5 + 2 Tier-1 records + a 3-record Tier-2 batch, each once";
    EXPECT_EQ(bq.outstanding(), 0u);
    EXPECT_EQ(bq.reservedBytes(), 0u);
    EXPECT_EQ(bq.ring().lostCompletions(), 0u);
    int64_t p0 = 0, p1 = 0;
    for (const auto& a : seen.acks) {
        EXPECT_EQ(a.topicId, 1);
        EXPECT_EQ(a.errorCode, 0);
        EXPECT_GE(a.baseOffset, 0);
        EXPECT_GE(a.baseSequence, 0);
        (a.partition == 0 ? p0 : p1) += a.recordCount;
    }
    EXPECT_EQ(p0, 5);
    EXPECT_EQ(p1, 5);
    for (const auto& c : seen.raw) EXPECT_EQ(c.status, OpStatus::Ok);
    EXPECT_EQ(cq.timesCompleted(acked.token()), 1);
}

// Regression: at the credit limit a VOID send returns QueueFull with nothing
// accepted (credit unchanged); a non-owner thread gets WrongThread; binding
// (and onBatchAck) after a send throws.  RED (mutation): skipping the unit
// reservation on the void path never reports QueueFull.
TEST(ProducerBatchAcks, BatchAcksToQueueFullWrongThreadAndSetupPhaseRegression) {
    Rig r(invokerCfg(4412, 4413));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;
    auto p = r.client->testProducerWithRingCap(pcfg, 64 * 1024);
    rc::Completions bq(*r.client, smallCq(1024));
    p.batchAcksTo(bq);
    const auto topic = r.client->topic("t");
    SendResult res = SendResult::Ok;
    uint32_t accepted = 0;
    for (int i = 0; i < 1'000 && res == SendResult::Ok; ++i) {
        res = p.trySend(topic, rc::Key(uint64_t{1}), kVal);
        if (res == SendResult::Ok) ++accepted;
    }
    EXPECT_EQ(res, SendResult::QueueFull);
    EXPECT_GT(accepted, 0u);
    EXPECT_EQ(bq.outstanding(), accepted) << "the rejected send reserved nothing";
    SendResult other = SendResult::Ok;
    std::thread t([&] { other = p.trySend(topic, rc::Key(uint64_t{1}), kVal); });
    t.join();
    EXPECT_EQ(other, SendResult::WrongThread);
    EXPECT_EQ(bq.outstanding(), accepted);
    rc::Completions bq2(*r.client, smallCq());
    EXPECT_THROW(p.batchAcksTo(bq2), std::logic_error) << "second binding";
    auto p2 = r.client->testProducerWithRingCap(pcfg, 64 * 1024);
    ASSERT_EQ(p2.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
    EXPECT_THROW(p2.batchAcksTo(bq2), std::logic_error) << "after a send";
    EXPECT_THROW(p2.onBatchAck([](void*, const rc::BatchAck&) noexcept {}, nullptr), std::logic_error)
        << "review m1: onBatchAck is setup-phase too";
}

// Regression: records that never got (or never finished) a batch are still
// reported in exactly one BatchAck — closeImmediately (in flight, builder,
// queued Tier-2) and Client close (records still in the producer ring) — and
// the credit balances on both bound queues.  RED (mutation): dropping the
// never-sent BatchAck in failBuilder_ leaves records unreported and credit
// outstanding.
TEST(ProducerBatchAcks, EveryAcceptedRecordInExactlyOneBatchAckThroughCloseRegression) {
    Rig r(invokerCfg(4414, 4415));
    r.connect();
    r.gw.replyAcks = false;
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.maxInFlight = 1;
    auto direct = r.client->producer(pcfg);
    auto ring   = r.client->testProducerWithRingCap(pcfg, 64 * 1024);
    rc::Completions bqD(*r.client, smallCq()), bqR(*r.client, smallCq());
    direct.batchAcksTo(bqD);
    ring.batchAcksTo(bqR);
    const auto topic = r.client->topic("t");
    ASSERT_EQ(direct.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty(); }));  // in flight (never acked)
    for (int i = 0; i < 3; ++i) ASSERT_EQ(direct.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
    auto b = direct.batch(topic, rc::Key(uint64_t{1}));
    b.add(kVal);
    b.add(kVal);
    ASSERT_EQ(b.send(), OpResult::Accepted);                        // queued Tier-2
    for (int i = 0; i < 4; ++i) ASSERT_EQ(ring.trySend(topic, rc::Key(uint64_t{2}), kVal), SendResult::Ok);
    ASSERT_EQ(direct.closeImmediately(), OpResult::Accepted);
    BatchSeen sD, sR;
    (void)pollBatches(bqD, sD);
    EXPECT_EQ(sD.records(), 6) << "1 in flight + 3 never sent + a 2-record Tier-2 batch";
    for (const auto& c : sD.raw) EXPECT_EQ(c.status, OpStatus::Closed);
    EXPECT_EQ(bqD.outstanding(), 0u);
    EXPECT_EQ(bqD.reservedBytes(), 0u);
    r.client->close();  // the ring producer's 4 records are still in its ring
    (void)pollBatches(bqR, sR);
    EXPECT_EQ(sR.records(), 4);
    for (const auto& a : sR.acks) {
        EXPECT_EQ(a.baseSequence, -1);
        EXPECT_EQ(a.errorCode, rc::BatchAck::kClientFailure);
    }
    EXPECT_EQ(bqR.outstanding(), 0u);
    EXPECT_EQ(bqR.reservedBytes(), 0u);
    EXPECT_EQ(bqD.ring().lostCompletions() + bqR.ring().lostCompletions(), 0u);
}

// Regression: a bound batch-ack queue released with BatchAck units still owed
// stays quarantined until those units are delivered (dropped) — a BatchAck
// accounts reserved0 producer ops.  RED (mutation): counting a BatchAck as one
// producer op never recycles the slot.
TEST(ProducerBatchAcks, ReleasedBatchAckQueueQuarantinedUntilUnitsDeliveredRegression) {
    Rig r(invokerCfg(4416, 4417));
    r.connect();
    rc::CompletionsConfig tiny = smallCq(1024);
    std::vector<std::unique_ptr<rc::Completions>> live;
    for (uint32_t i = 0; i < rc::kMaxCqs - 2; ++i) live.push_back(std::make_unique<rc::Completions>(*r.client, tiny));
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;
    auto ring = r.client->testProducerWithRingCap(pcfg, 64 * 1024);
    const auto topic = r.client->topic("t");
    auto a = std::make_unique<rc::Completions>(*r.client, tiny);
    const uint32_t aIdx = rc::cqRefIndex(a->cqRef());
    ring.batchAcksTo(*a);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(ring.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
    a.reset();  // 3 units owed, all still in the producer ring
    for (int i = 0; i < 64; ++i) {
        rc::Completions churn(*r.client, tiny);
        EXPECT_NE(rc::cqRefIndex(churn.cqRef()), aIdx) << "quarantined slot reused at churn " << i;
    }
    const uint64_t stale0 = r.client->testStaleCqDrops();
    ring.flush();
    ASSERT_TRUE(r.drive([&] { return r.client->testStaleCqDrops() > stale0; }));
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); }
    bool reused = false;
    for (int i = 0; i < 8 && !reused; ++i) {
        rc::Completions churn(*r.client, tiny);
        reused = rc::cqRefIndex(churn.cqRef()) == aIdx;
    }
    EXPECT_TRUE(reused) << "the slot returns once the BatchAck's 3 units are accounted";
}

// Regression (review I1, through the Client): Producer::closeImmediately()
// from the producer's own onAck handler (the Invoker owner, inside doWork) —
// each token once, the acked batch reported once.
TEST(ProducerBatchAcks, ClientCloseImmediatelyFromOnAckHandlerExactlyOnceRegression) {
    Rig r(invokerCfg(4418, 4419));
    r.connect();
    rc::ProducerConfig pcfg = ackCfg();
    pcfg.lingerUs = 60'000'000;
    auto p = r.client->producer(pcfg);
    rc::Completions bq(*r.client, smallCq());
    p.batchAcksTo(bq);
    struct Ctx {
        rc::Producer*         p;
        std::vector<AckLedger::Rec> recs;
    } ctx{&p, {}};
    p.onAck([](void* c, rc::Token t, int64_t v, OpStatus st, uint16_t ec) noexcept {
        auto* x = static_cast<Ctx*>(c);
        x->recs.push_back({t, v, st, ec});
        if (t == 1) (void)x->p->closeImmediately();
    }, &ctx);
    const auto topic = r.client->topic("t");
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{1}), SendResult::Ok);
    p.flush();
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty(); }));
    r.gw.replyAcks = false;  // what follows never acks
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{2}), SendResult::Ok);
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{3}), SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return ctx.recs.size() >= 3; }));
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); }
    ASSERT_EQ(ctx.recs.size(), 3u);
    EXPECT_EQ(ctx.recs[0].token, 1u);
    EXPECT_EQ(ctx.recs[0].status, OpStatus::Ok);
    for (int i = 1; i < 3; ++i) EXPECT_EQ(ctx.recs[i].status, OpStatus::Closed);
    BatchSeen s;
    (void)pollBatches(bq, s);
    EXPECT_EQ(s.records(), 3);
    EXPECT_EQ(bq.outstanding(), 0u);
    EXPECT_EQ(bq.reservedBytes(), 0u);
}
