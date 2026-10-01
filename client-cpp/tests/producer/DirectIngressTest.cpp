// tests/producer/DirectIngressTest.cpp — invoker-mode (Direct) producer ingress.
//
// Task 14 (spec §4.1/§4.2): in RunnerMode::Invoker the app thread IS the duty-cycle
// thread, so send() appends straight into the partition builder (no ring hop) and
// the records of one pass leave as one frame per partition at the end of the pass.
// Every drive loop is iteration-capped; nothing sleeps.
#include "../support/AckLedger.h"
#include "../support/TestCompletions.h"
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using rc::producer::CountingSink;
using rc::producer::ProducerImpl;
using namespace std::chrono_literals;

namespace {
std::atomic<int> g_ctr{0};
const std::vector<uint8_t> kVal(16, 0x33);

rc::ProducerConfig voidCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg, int32_t partitions) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/direct_ingress_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = partitions;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
             c.egressStreamId) {}

    template <class P>
    // cap < 0 (default): drive until pred holds, bounded by a 10 s deadline that
    // only a FAILING test reaches (iteration counts depend on CPU speed).
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
} // namespace

// ---------------------------------------------------------------------------
// ProducerImpl in isolation
// ---------------------------------------------------------------------------

TEST(DirectIngress, SendAppendsWithoutTheRing) {
    ProducerImpl impl(voidCfg(), /*ringCap=*/4 * 1024);  // the ring could hold ~60 records
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    EXPECT_EQ(impl.ingressMode(), ProducerImpl::IngressMode::Direct);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (uint64_t i = 0; i < 10'000; ++i) {
        ASSERT_EQ(impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0), rc::SendResult::Ok);
        if (i % 64 == 63) impl.flushBatches(1, 42, sink);  // end of an app "pass"
    }
    impl.flushBatches(1, 42, sink);
    EXPECT_EQ(impl.recordsSent(), 10'000u);
    EXPECT_GT(frames, 0u);
}

TEST(DirectIngress, DefaultIsRingIngress) {
    ProducerImpl impl(voidCfg(), 4 * 1024);
    EXPECT_EQ(impl.ingressMode(), ProducerImpl::IngressMode::Ring);
}

TEST(DirectIngress, OffOwnerThreadIsRejected) {
    ProducerImpl impl(voidCfg());
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    rc::SendResult r = rc::SendResult::Ok;
    std::thread other([&] { r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0); });
    other.join();
    EXPECT_EQ(r, rc::SendResult::WrongThread);
}

TEST(DirectIngress, TrySendAllocatesNothing) {
    ProducerImpl impl(voidCfg());
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (uint64_t i = 0; i < 256; ++i) {  // warm-up: partitions, builders, scratch
        (void)impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0);
        impl.flushBatches(1, 42, sink);
    }
    alloc_counter::start();
    for (uint64_t i = 0; i < 1000; ++i) {
        (void)impl.trySubmit(1, static_cast<int32_t>(i % 4), rc::Key(i), kVal, 0);
        impl.flushBatches(1, 42, sink);
    }
    EXPECT_EQ(alloc_counter::stop(), 0L);
}

// Direct appends never grow a builder past batchBytes: with no ring drain gate
// between sends (the hub flushes only at the end of the pass), a full builder
// reports BackPressured even when the in-flight window has room, so no frame is
// ever bigger than one batch.
TEST(DirectIngress, FullBuilderBackPressuresUntilThePassFlushesRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.batchBytes = 256;
    ProducerImpl impl(cfg);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    int accepted = 0;
    while (accepted < 10'000 &&
           impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0) == rc::SendResult::Ok) {
        ++accepted;
    }
    ASSERT_LT(accepted, 10'000) << "a Direct builder must back-pressure at batchBytes";
    const auto* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_GE(ps->builderBytes, cfg.batchBytes);
    EXPECT_LT(ps->builderBytes, cfg.batchBytes + 64) << "at most one record past the cap";

    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    impl.flushBatches(1, 42, sink);  // end of the pass
    EXPECT_EQ(frames, 1u);
    EXPECT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::Ok);
}

// A bare Direct producer with no pump hook has no one to drain it: a back-pressured
// blocking send cannot wait for anybody and must throw, never hang.
TEST(DirectIngress, BlockingSendWithoutPumpThrowsInsteadOfHangingRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.batchBytes = 256;
    ProducerImpl impl(cfg);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    rc::SendResult r = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) {
        r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0);
    }
    ASSERT_EQ(r, rc::SendResult::BackPressured);
    EXPECT_THROW(impl.sendBlocking(r, 1, 0, rc::Key(uint64_t{1}), kVal), std::runtime_error);
}

TEST(DirectIngress, ClosedProducerReportsClosedRegression) {
    ProducerImpl impl(voidCfg());
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    impl.markRingClosed();
    EXPECT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::Closed);
    EXPECT_EQ(impl.partitionState(1, 0), nullptr) << "a closed producer appends nothing";
    // Off the owner thread: WrongThread is decided first — Direct state (incl. the
    // closed bool) is owner-thread-only, so a foreign thread never reads it.
    rc::SendResult r = rc::SendResult::Ok;
    std::thread other([&] { r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0); });
    other.join();
    EXPECT_EQ(r, rc::SendResult::WrongThread);
    EXPECT_THROW(impl.sendBlocking(rc::SendResult::BackPressured, 1, 0, rc::Key(uint64_t{1}), kVal),
                 std::logic_error) << "closed: a blocking send never pumps, never appends";
}

// ---------------------------------------------------------------------------
// Real Client in RunnerMode::Invoker
// ---------------------------------------------------------------------------

TEST(DirectIngress, InvokerProducerIsDirectAndOwnerBound) {
    Rig r(invokerCfg(3900, 3901, 1));
    auto p = r.client->producer(voidCfg());
    const auto topic = r.client->topic("t");
    rc::SendResult off = rc::SendResult::Ok;
    std::thread other([&] { off = p.trySend(topic, rc::Key(uint64_t{1}), kVal); });
    other.join();
    EXPECT_EQ(off, rc::SendResult::WrongThread) << "Invoker producer() must be Direct, owner-bound";
    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Ok);
}

// Spec §4.2: the records sent during one pass leave as ONE frame per partition at
// the end of the pass.
TEST(DirectIngress, OnePassLeavesAsOneFramePerPartition) {
    Rig r(invokerCfg(3902, 3903, 2));
    r.connect();
    rc::ProducerConfig pcfg = voidCfg();
    pcfg.acks = rc::Acks::Leader;
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    const rc::Key k0 = keyFor(0, 2), k1 = keyFor(1, 2);
    for (int i = 0; i < 10; ++i) ASSERT_EQ(p.trySend(topic, k0, kVal), rc::SendResult::Ok);
    for (int i = 0; i < 7; ++i) ASSERT_EQ(p.trySend(topic, k1, kVal), rc::SendResult::Ok);
    EXPECT_TRUE(r.gw.produced.empty()) << "nothing leaves before the pass";
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 2; }));
    for (int i = 0; i < 2'000; ++i) { r.client->doWork(); r.gw.pump(); }
    ASSERT_EQ(r.gw.produced.size(), 2u);
    int32_t p0 = 0, p1 = 0;
    for (const auto& pr : r.gw.produced) (pr.partition == 0 ? p0 : p1) += pr.records;
    EXPECT_EQ(p0, 10);
    EXPECT_EQ(p1, 7);
}

TEST(DirectIngress, AckFutureAndTier2BatchAndFlushWork) {
    Rig r(invokerCfg(3904, 3905, 1));
    r.connect();
    rc::ProducerConfig pcfg = voidCfg();
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 60'000'000;  // only flush() / size / Tier-2 can end the batch
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    test_support::TestCq cq(*r.client);
    auto f1 = cq.send(p, topic, rc::Key(uint64_t{1}), kVal);
    ASSERT_TRUE(f1.accepted());
    for (int i = 0; i < 2'000; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_TRUE(r.gw.produced.empty()) << "linger holds the batch";
    p.flush();
    ASSERT_TRUE(r.drive([&] { return f1.ready(); }));
    EXPECT_GE(f1.offset(), 0);

    auto b = p.batch(topic, rc::Key(uint64_t{1}));
    b.add(kVal);
    b.add(kVal);
    auto f2 = cq.send(b);
    ASSERT_TRUE(f2.accepted());
    ASSERT_TRUE(r.drive([&] { return f2.ready(); }));
    EXPECT_GE(f2.offset(), 0);
    EXPECT_EQ(f2.outcome().c.op, rc::OpKind::Tier2Batch);
    EXPECT_EQ(f2.outcome().c.aux, 2) << "aux = recordCount";
}

// The owner thread outside any duty cycle: a back-pressured blocking send pumps
// the owner's own duty cycle (which flushes the full builder) and succeeds.
TEST(DirectIngress, BlockingSendPumpsOwnDutyCycleRegression) {
    Rig r(invokerCfg(3906, 3907, 1));
    r.connect();
    rc::ProducerConfig pcfg = voidCfg();
    pcfg.batchBytes = 256;
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    rc::SendResult res = rc::SendResult::Ok;
    uint64_t sent = 0;
    for (; sent < 10'000 && res == rc::SendResult::Ok; ++sent) {
        res = p.trySend(topic, rc::Key(uint64_t{1}), kVal);
    }
    ASSERT_EQ(res, rc::SendResult::BackPressured);
    EXPECT_NO_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal));
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty(); }));
}

// Review Focus 2: an ack callback runs INSIDE the invoker duty cycle.  A void
// send there that hits back-pressure cannot wait for a drain that only this
// thread can perform, and must not re-enter the duty cycle: it throws.
TEST(DirectIngress, VoidSendInsideDutyCycleCallbackThrowsRegression) {
    Rig r(invokerCfg(3908, 3909, 2));
    r.connect();

    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    pcfg.maxInFlight = 1;
    pcfg.batchBytes = 256;
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    const rc::Key k0 = keyFor(0, 2), k1 = keyFor(1, 2);

    r.gw.holdPartitions = {1};  // partition 1's single in-flight batch is never acked
    ASSERT_EQ(p.trySend(topic, k1, kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    rc::SendResult res = rc::SendResult::Ok;
    for (int i = 0; i < 1000 && res == rc::SendResult::Ok; ++i) res = p.trySend(topic, k1, kVal);
    ASSERT_EQ(res, rc::SendResult::BackPressured) << "partition 1: window full + builder at batchBytes";

    // Fix round 1: inside the producer's own duty work a Direct send is DEFERRED
    // (accepted into the owner-thread deferred buffer, appended after the walk).
    // Only once that buffer is full does it back-pressure — then the void send
    // cannot be drained by anyone but this (mid-cycle) thread: it throws.
    bool threw = false;
    bool returned = false;
    int deferred = 0;
    rc::SendResult tokenFull = rc::SendResult::Ok;
    test_support::ScriptedAcks S;
    S.attach(p);
    p.send(topic, k0, kVal, S.tok([&](int64_t) {
        while (deferred < 100'000 && p.trySend(topic, k1, kVal) == rc::SendResult::Ok) ++deferred;
        tokenFull = p.trySend(topic, k1, kVal, rc::AckToken{999});  // full: rejected, no completion
        try {
            p.send(topic, k1, kVal);  // void, back-pressured, inside the duty cycle
            returned = true;
        } catch (const std::runtime_error&) {
            threw = true;
        }
    }));
    ASSERT_TRUE(r.drive([&] { return threw || returned; }))
        << "must throw, not hang or re-enter doWork()";
    EXPECT_GT(deferred, 0) << "in-duty sends are deferred, not rejected";
    EXPECT_EQ(tokenFull, rc::SendResult::BackPressured) << "the full arena rejects synchronously";
    EXPECT_EQ(S.fired.count(999), 0u) << "a rejected token never completes (exactly-once rule)";
    EXPECT_TRUE(threw);
    EXPECT_FALSE(returned);
    EXPECT_EQ(r.gw.held.size(), 1u) << "no extra partition-1 frame: the pump never re-entered";
}

TEST(DirectIngress, SendAfterClientCloseReturnsClosedRegression) {
    Rig r(invokerCfg(3910, 3911, 1));
    auto p = r.client->producer(voidCfg());
    const auto topic = r.client->topic("t");
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Ok);
    r.client->closeImmediately();  // C7: teardown semantics (close() drains)
    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
    EXPECT_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal), std::logic_error);
}

// The Producer outlives its Client: Direct sends must not touch the destroyed
// Client (pump hook context) nor append anywhere — Closed / logic_error.
TEST(DirectIngress, ProducerOutlivesClientNoUseAfterFreeRegression) {
    rc::ClientConfig cfg = invokerCfg(3912, 3913, 1);
    auto client = std::make_unique<rc::Client>(cfg);
    rc::ProducerConfig pcfg = voidCfg();
    pcfg.batchBytes = 256;
    auto p = client->producer(pcfg);
    const auto topic = client->topic("t");
    rc::SendResult res = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && res == rc::SendResult::Ok; ++i) {
        res = p.trySend(topic, rc::Key(uint64_t{1}), kVal);
    }
    ASSERT_EQ(res, rc::SendResult::BackPressured);

    client.reset();  // the Producer (shared_ptr<ProducerImpl>) survives

    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
    EXPECT_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal), std::logic_error);
    p.flush();  // no-op, never touches the dead Client
}

// ===========================================================================
// Task 14 fix round 1 — C1: a Direct send from inside a user callback that the
// producer fires mid-walk / mid-reset must be DEFERRED (owner-thread buffer)
// and applied after the walk: never wiped, never acked before it is encoded,
// never pushed into partitions_ / ackRing under a live iterator or reference.
// ===========================================================================

namespace {
ProducerImpl::DirectIngress ownerDirect(int64_t nowNs = 1) {
    return {std::this_thread::get_id(), nullptr, nullptr, nowNs};
}
int32_t builderRecords(const ProducerImpl& impl, int32_t partition) {
    const auto* ps = impl.partitionState(1, partition);
    return (ps && ps->builder) ? ps->builder->recordCount() : 0;
}
} // namespace

// C1(a): acks=None completes the sent slice BEFORE resetting the builder. A send
// from that callback used to land in the already-sent builder, get "sent" (-1)
// immediately, and then be wiped by the reset.
TEST(DirectIngress, DirectSendFromAcksNoneCallbackIsNotDroppedRegression) {
    ProducerImpl impl(voidCfg());
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect());
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    bool innerAcked = false;
    rc::SendResult innerR = rc::SendResult::Closed;
    int32_t recordsSeenInCallback = -1;
    const int64_t outer = S.cb([&](int64_t) {
        const int64_t h = S.cb([&](int64_t) { innerAcked = true; });
        innerR = impl.trySubmit(1, 0, rc::Key(uint64_t{2}), kVal, h);
        if (innerR != rc::SendResult::Ok) S.rejected(h);
        recordsSeenInCallback = builderRecords(impl, 0);
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, outer), rc::SendResult::Ok);
    impl.flushBatches(1, 42, sink);
    EXPECT_EQ(innerR, rc::SendResult::Ok) << "an in-duty send is deferred, not rejected";
    EXPECT_EQ(recordsSeenInCallback, 1) << "deferred: not appended into the builder being sent";
    EXPECT_FALSE(innerAcked) << "a record must never be acked before it is encoded";
    EXPECT_EQ(builderRecords(impl, 0), 1) << "applied after the walk, into the fresh builder";
    impl.flushBatches(2, 42, sink);
    EXPECT_EQ(frames, 2u);
    EXPECT_TRUE(innerAcked);
}

// C1(b): sweepTimedOut's never-sent branch fails the builder slice and THEN
// resets the builder: a retry from the failure callback used to be discarded
// (and its PendingAck leaked).
TEST(DirectIngress, DirectRetryFromTimeoutCallbackIsNotDroppedRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.acks = rc::Acks::Leader;
    cfg.deliveryTimeoutMs = 10;
    ProducerImpl impl(cfg);
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect(1'000));
    bool retryFired = false;
    rc::SendResult retryR = rc::SendResult::Closed;
    int64_t failedWith = 0;
    const int64_t h = S.cb([&](int64_t off) {
        failedWith = off;
        const int64_t h2 = S.cb([&](int64_t) { retryFired = true; });
        retryR = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h2);
        if (retryR != rc::SendResult::Ok) S.rejected(h2);
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h), rc::SendResult::Ok);
    const int64_t sweepNs = 1'000 + 10'000'000 + 1;  // never sent (no flush), past delivery
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{sweepNs}});
    EXPECT_EQ(failedWith, -1);
    EXPECT_EQ(retryR, rc::SendResult::Ok);
    EXPECT_EQ(builderRecords(impl, 0), 1) << "the retry must survive the builder reset";
    EXPECT_FALSE(retryFired) << "not failed, not acked: still pending";
    const auto* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->batchStartNs, sweepNs) << "the retry's delivery clock starts at the sweep";
}

// C1(c): a callback sending to NEW partitions during a range-for over
// partitions_ (here: sweepTimedOut) used to push_back -> reallocate -> leave the
// walk on a dangling iterator.  100 fresh partitions force the reallocation.
TEST(DirectIngress, DirectSendToNewPartitionFromSweepCallbackRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.acks = rc::Acks::Leader;
    cfg.deliveryTimeoutMs = 10;
    ProducerImpl impl(cfg);
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect(1'000));
    constexpr int kFresh = 100;
    int acceptedButNotApplied = 0;
    const int64_t h = S.cb([&](int64_t) {
        for (int32_t part = 1; part <= kFresh; ++part) {
            if (impl.trySubmit(1, part, rc::Key(uint64_t{1}), kVal, 0) == rc::SendResult::Ok &&
                impl.partitionState(1, part) == nullptr) {
                ++acceptedButNotApplied;
            }
        }
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h), rc::SendResult::Ok);
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{1'000 + 10'000'000 + 1}});
    EXPECT_EQ(acceptedButNotApplied, kFresh) << "no partitions_ growth under the sweep's walk";
    for (int32_t part = 1; part <= kFresh; ++part) {
        EXPECT_EQ(builderRecords(impl, part), 1) << "partition " << part;
    }
}

// C1(d): failAllPending fires callbacks while walking every partition (and used
// to hold a reference into ackRing across complete()).  Sends from those
// callbacks are deferred and applied after the walk — not dropped by it.
TEST(DirectIngress, DirectSendFromFailAllPendingCallbackIsDeferredRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.acks = rc::Acks::Leader;
    ProducerImpl impl(cfg);
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect());
    constexpr int kFresh = 50;
    int acceptedButNotApplied = 0;
    int resentAcked = 0;
    auto resend = [&](int32_t base) {
        for (int32_t part = base; part < base + kFresh; ++part) {
            const int64_t h2 = S.cb([&](int64_t) { ++resentAcked; });
            if (impl.trySubmit(1, part, rc::Key(uint64_t{1}), kVal, h2) != rc::SendResult::Ok) {
                S.rejected(h2);
            } else if (impl.partitionState(1, part) == nullptr) {
                ++acceptedButNotApplied;
            }
        }
    };
    const int64_t h0 = S.cb([&](int64_t) { resend(100); });
    const int64_t h1 = S.cb([&](int64_t) { resend(200); });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h0), rc::SendResult::Ok);
    ASSERT_EQ(impl.trySubmit(1, 1, rc::Key(uint64_t{1}), kVal, h1), rc::SendResult::Ok);
    impl.failAllPending(rc::OpStatus::SessionLost);
    EXPECT_EQ(acceptedButNotApplied, 2 * kFresh);
    EXPECT_EQ(resentAcked, 0) << "resent records are pending, not failed by the same walk";
    for (int32_t part = 100; part < 100 + kFresh; ++part) EXPECT_EQ(builderRecords(impl, part), 1);
    for (int32_t part = 200; part < 200 + kFresh; ++part) EXPECT_EQ(builderRecords(impl, part), 1);
}

// Deferred records already queued when failAllPending runs are "accepted" too:
// they are failed and dropped with everything else (never applied afterwards).
TEST(DirectIngress, FailAllPendingFailsDeferredRecordsRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.acks = rc::Acks::Leader;
    cfg.maxInFlight = 1;
    cfg.batchBytes = 256;
    ProducerImpl impl(cfg);
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect());
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    // Partition 0: one batch in flight (window full) + a full builder, so a
    // deferred record for it cannot be applied (ABORT) and stays deferred.
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::Ok);
    // Partition 1: one acked-able batch whose ack callback defers a send to p0.
    int64_t deferredOutcome = 0;
    const int64_t hOuter = S.cb([&](int64_t) {
        const int64_t h = S.cb([&](int64_t off) { deferredOutcome = off == -1 ? -1 : 1; });
        if (impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h) != rc::SendResult::Ok) S.rejected(h);
    });
    ASSERT_EQ(impl.trySubmit(1, 1, rc::Key(uint64_t{1}), kVal, hOuter), rc::SendResult::Ok);
    impl.flushBatches(1, 42, sink);
    ASSERT_EQ(frames, 2u);
    rc::SendResult r = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0);
    ASSERT_EQ(r, rc::SendResult::BackPressured);
    const int64_t p1Corr = const_cast<rc::producer::PartitionState*>(impl.partitionState(1, 1))->inflight.oldestLive()->corrId;
    impl.onProduceResponse(p1Corr, 100);  // hOuter fires inside the producer's duty work
    EXPECT_EQ(deferredOutcome, 0) << "accepted into the deferred buffer, still pending";
    impl.failAllPending(rc::OpStatus::SessionLost);
    EXPECT_EQ(deferredOutcome, -1) << "a deferred record is failed like every accepted record";
    EXPECT_EQ(builderRecords(impl, 0), 0) << "and dropped, never applied afterwards";
}

// Drained deferred records keep submission order across partitions (partition
// slots are created in the order the deferred sends were made).
TEST(DirectIngress, DeferredRecordsDrainInSubmissionOrderRegression) {
    ProducerImpl impl(voidCfg());
    test_support::ScriptedAcks S;
    S.attach(impl);
    impl.enableDirectIngress(ownerDirect());
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    const int64_t h = S.cb([&](int64_t) {
        for (int32_t part : {7, 5, 9}) (void)impl.trySubmit(1, part, rc::Key(uint64_t{1}), kVal, 0);
    });
    ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, h), rc::SendResult::Ok);
    impl.flushBatches(1, 42, sink);
    ASSERT_NE(impl.partitionState(1, 7), nullptr);
    ASSERT_NE(impl.partitionState(1, 5), nullptr);
    ASSERT_NE(impl.partitionState(1, 9), nullptr);
    EXPECT_LT(impl.partitionState(1, 7)->slotIndex, impl.partitionState(1, 5)->slotIndex);
    EXPECT_LT(impl.partitionState(1, 5)->slotIndex, impl.partitionState(1, 9)->slotIndex);
}

// M2: batchRecords is enforced in Direct mode too (no flush runs between sends).
TEST(DirectIngress, DirectBatchRecordsCapBackPressuresRegression) {
    rc::ProducerConfig cfg = voidCfg();
    cfg.batchRecords = 5;
    ProducerImpl impl(cfg);
    impl.enableDirectIngress(ownerDirect());
    for (int i = 0; i < 5; ++i) ASSERT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::Ok);
    EXPECT_EQ(impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0), rc::SendResult::BackPressured);
    EXPECT_EQ(builderRecords(impl, 0), 5);
}

// I1: a record must never overtake a pending Tier-2 batch on the same partition
// (window full when the Tier-2 batch was queued).  Direct: the record
// back-pressures until the Tier-2 batch is sent.  Ring: the record stays in the
// ring until then.
TEST(DirectIngress, DirectRecordAfterTier2KeepsSubmissionOrderRegression) {
    for (const bool direct : {true, false}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        Rig r(invokerCfg(direct ? 3914 : 3916, direct ? 3915 : 3917, 1));
        r.connect();
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::Leader;
        pcfg.lingerUs = 0;
        pcfg.maxInFlight = 1;
        auto p = direct ? r.client->producer(pcfg) : r.client->testProducerWithRingCap(pcfg, 64 * 1024);
        const auto topic = r.client->topic("t");
        r.gw.holdPartitions = {0};
        ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Ok);  // A
        ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
        auto b = p.batch(topic, rc::Key(uint64_t{1}));  // T: 2 records
        b.add(kVal);
        b.add(kVal);
        ASSERT_EQ(b.send(), rc::OpResult::Accepted);
        rc::SendResult rb = p.trySend(topic, rc::Key(uint64_t{1}), kVal);  // B
        if (direct) {
            EXPECT_EQ(rb, rc::SendResult::BackPressured) << "B must not overtake the pending Tier-2 batch";
        }
        for (int i = 0; i < 2'000; ++i) { r.client->doWork(); r.gw.pump(); }
        r.gw.holdPartitions.clear();
        r.gw.releaseHeld();
        ASSERT_TRUE(r.drive([&] {
            if (rb != rc::SendResult::Ok) rb = p.trySend(topic, rc::Key(uint64_t{1}), kVal);
            return rb == rc::SendResult::Ok && r.gw.produced.size() >= 3;
        }));
        ASSERT_EQ(r.gw.produced.size(), 3u);
        EXPECT_EQ(r.gw.produced[1].records, 2) << "Tier-2 batch T goes second";
        EXPECT_EQ(r.gw.produced[2].records, 1) << "record B goes last";
    }
}

namespace {
struct DutyBoom : std::exception {};
void throwDutyBoom() { throw DutyBoom{}; }
} // namespace

// I2: an exception from a pumped duty cycle propagates out of send() exactly as
// it would out of doWork() (no noexcept terminate, no conversion), and the
// in-cycle state is restored so the next blocking send can pump again.
TEST(DirectIngress, PumpedDutyCycleThrowPropagatesToSendRegression) {
    Rig r(invokerCfg(3918, 3919, 1));
    r.connect();
    rc::ProducerConfig pcfg = voidCfg();
    pcfg.batchBytes = 256;
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    rc::SendResult res = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && res == rc::SendResult::Ok; ++i) res = p.trySend(topic, rc::Key(uint64_t{1}), kVal);
    ASSERT_EQ(res, rc::SendResult::BackPressured);
    r.client->testSetDutyCycleHook(&throwDutyBoom);
    EXPECT_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal), DutyBoom);
    r.client->testSetDutyCycleHook(nullptr);
    EXPECT_NO_THROW(p.send(topic, rc::Key(uint64_t{1}), kVal)) << "in-cycle flag restored by RAII";
}

// M3 (F8c C6): the ack handler is a noexcept function pointer by type, so a
// throwing user callback no longer exists (the old "uncaught throw is caught +
// counted" test is gone).  What remains: inside the handler a back-pressured
// trySend reports BackPressured (never throws, never re-enters), and the
// producer stays fully usable afterwards.
TEST(DirectIngress, BackPressuredTrySendInAckHandlerReportsAndProducerStaysUsableRegression) {
    Rig r(invokerCfg(3920, 3921, 2));
    r.connect();
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    pcfg.maxInFlight = 1;
    pcfg.batchBytes = 256;
    pcfg.directDeferredBytes = 0;  // clamped to the minimum: fills fast
    auto p = r.client->producer(pcfg);
    const auto topic = r.client->topic("t");
    const rc::Key k0 = keyFor(0, 2), k1 = keyFor(1, 2);
    r.gw.holdPartitions = {1};
    ASSERT_EQ(p.trySend(topic, k1, kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    bool ran = false;
    rc::SendResult last = rc::SendResult::Ok;
    test_support::ScriptedAcks S;
    S.attach(p);
    p.send(topic, k0, kVal, S.tok([&](int64_t) {
        ran = true;
        for (int i = 0; i < 100'000 && last == rc::SendResult::Ok; ++i) last = p.trySend(topic, k1, kVal);
    }));
    ASSERT_TRUE(r.drive([&] { return ran; }));
    EXPECT_EQ(last, rc::SendResult::BackPressured);
    // Still fully usable.  Deferred records apply in submission order (like a
    // Ring drain, a blocked head holds the rest), so release partition 1 first.
    r.gw.holdPartitions.clear();
    r.gw.releaseHeld();
    test_support::TestCq cq(*r.client);
    auto f = cq.send(p, topic, k0, kVal);
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_GE(f.offset(), 0);
}

// ===========================================================================
// Task 14 fix round 2 — per-partition deferral, deferred delivery deadline,
// flush() covers deferred records, partition validated before deferring.
// ===========================================================================

namespace {
// A bare Direct producer with a frame-capturing sink: every frame's records
// (value marker byte) are recorded per partition, in send order.
struct Fx {
    ProducerImpl         impl;
    test_support::ScriptedAcks S;  // F8c C6: per-token scripted ack reactions
    std::map<int32_t, std::string> sent;  // partition -> marker bytes in wire order
    ProducerImpl::SendFn sink;
    explicit Fx(rc::ProducerConfig cfg) : impl(cfg) {
        impl.enableDirectIngress(ownerDirect());
        S.attach(impl);
        sink = [this](int32_t, int32_t part, int32_t, const uint8_t* buf, std::size_t len) {
            const auto pv = rc::codec::decodeProduce(buf, len);
            if (!pv) return false;
            rune::protocol::RecordBatchReader r(pv->recordBatch.data(), pv->recordBatch.size());
            r.forEach([&](std::string_view, std::string_view v, int64_t, int64_t) {
                sent[part].push_back(v.empty() ? '?' : v[0]);
            });
            return true;
        };
    }
    rc::SendResult send(int32_t part, char marker, int64_t h = 0,
                        rc::producer::RejectReason* why = nullptr) {
        std::vector<uint8_t> v(16, static_cast<uint8_t>(marker));
        return impl.trySubmit(1, part, rc::Key(uint64_t{1}), v, h, why);
    }
    void flush(int64_t now) { impl.flushBatches(now, 42, sink); }
    int64_t inflightCorr(int32_t part) {
        auto* ps = const_cast<rc::producer::PartitionState*>(impl.partitionState(1, part));
        return ps->inflight.oldestLive()->corrId;
    }
    /// Partition `part`: one batch in flight (maxInFlight 1) + a full builder.
    void stall(int32_t part, char filler, int64_t now) {
        ASSERT_EQ(send(part, filler), rc::SendResult::Ok);
        flush(now);
        rc::SendResult r = rc::SendResult::Ok;
        for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) r = send(part, filler);
        ASSERT_EQ(r, rc::SendResult::BackPressured);
    }
    /// Runs fn INSIDE the producer's own duty work (an ack callback of a batch
    /// on partition `trigger`).
    template <class F>
    void inDuty(int32_t trigger, int64_t now, F fn) {
        const int64_t h = S.cb([fn](int64_t) mutable { fn(); });
        ASSERT_EQ(send(trigger, 't', h), rc::SendResult::Ok);
        impl.submitFlush();
        flush(now);
        impl.onProduceResponse(inflightCorr(trigger), 100);
    }
};
rc::ProducerConfig stallCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::Leader;
    c.lingerUs = 0;
    c.maxInFlight = 1;
    c.batchBytes = 256;
    return c;
}
} // namespace

// A deferred record for a STALLED partition must not hold back sends to any
// other partition (no cross-partition head-of-line blocking).
TEST(DirectIngress, StalledPartitionDoesNotBlockOtherPartitionsRegression) {
    Fx fx(stallCfg());
    fx.stall(0, 'f', 1);
    fx.inDuty(1, 2, [&] { for (int i = 0; i < 3; ++i) (void)fx.send(0, 'd'); });
    ASSERT_EQ(fx.impl.deferredCount(), 3u) << "p0 is stalled: its records stay deferred";
    for (int32_t part = 2; part <= 6; ++part) {
        EXPECT_EQ(fx.send(part, 'x'), rc::SendResult::Ok);
        EXPECT_EQ(builderRecords(fx.impl, part), 1) << "partition " << part << ": applied, not deferred";
    }
    EXPECT_EQ(fx.impl.deferredCount(), 3u);
    EXPECT_EQ(fx.send(0, 'e'), rc::SendResult::BackPressured) << "p0 never overtakes its deferred records";
}

TEST(DirectIngress, PerPartitionOrderPreservedAcrossDeferralRegression) {
    Fx fx(stallCfg());
    fx.stall(0, 'f', 1);
    fx.inDuty(1, 2, [&] {
        for (char m : {'A', 'B', 'C', 'D'}) (void)fx.send((m == 'A' || m == 'C') ? 0 : 1, m);
    });
    EXPECT_EQ(builderRecords(fx.impl, 1), 2) << "p1's B, D applied at once (not behind stalled p0)";
    EXPECT_EQ(fx.impl.deferredCount(), 2u);
    EXPECT_EQ(fx.send(0, 'E'), rc::SendResult::BackPressured);
    // Unblock p0: ack its in-flight batch; the next pass sends the full builder
    // and then applies A, C; E follows.
    fx.impl.onProduceResponse(fx.inflightCorr(0), 100);
    fx.flush(3);
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
    ASSERT_EQ(fx.send(0, 'E'), rc::SendResult::Ok);
    fx.impl.onProduceResponse(fx.inflightCorr(0), 200);
    fx.impl.onProduceResponse(fx.inflightCorr(1), 300);
    fx.impl.submitFlush();
    fx.flush(4);
    const std::string& p0 = fx.sent[0];
    const std::string& p1 = fx.sent[1];
    ASSERT_GE(p0.size(), 3u);
    EXPECT_EQ(p0.substr(p0.size() - 3), "ACE") << p0;
    EXPECT_NE(p1.find("BD"), std::string::npos) << p1;
}

TEST(DirectIngress, StalledPartitionCannotExhaustArenaRegression) {
    rc::ProducerConfig cfg = stallCfg();
    cfg.directDeferredBytes = 64 * 1024;
    Fx fx(cfg);
    fx.stall(0, 'f', 1);
    int p0Deferred = 0;
    rc::SendResult other = rc::SendResult::Closed;
    fx.inDuty(1, 2, [&] {
        while (p0Deferred < 100'000 && fx.send(0, 'd') == rc::SendResult::Ok) ++p0Deferred;
        other = fx.send(2, 'o');
    });
    EXPECT_GT(p0Deferred, 0);
    EXPECT_EQ(other, rc::SendResult::Ok) << "one stalled partition must not exhaust the arena";
}

// A deferred record is a sent record: it fails at deliveryTimeoutMs measured
// from ACCEPTANCE, even if its partition never unblocks.
TEST(DirectIngress, DeferredRecordFailsWithinDeliveryTimeoutRegression) {
    rc::ProducerConfig cfg = stallCfg();
    cfg.deliveryTimeoutMs = 10;
    Fx fx(cfg);
    constexpr int64_t t0 = 1'000;
    constexpr int64_t D  = 10'000'000;
    fx.stall(0, 'f', t0);
    int64_t outcome = 0;
    fx.inDuty(1, t0, [&] {
        const int64_t h = fx.S.cb([&](int64_t off) { outcome = off == -1 ? -1 : 1; });
        if (fx.send(0, 'd', h) != rc::SendResult::Ok) fx.S.rejected(h);
    });
    ASSERT_EQ(fx.impl.deferredCount(), 1u);
    fx.impl.sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{t0 + D - 1}});
    EXPECT_EQ(outcome, 0) << "not before the deadline";
    fx.impl.sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{t0 + D}});
    EXPECT_EQ(outcome, -1) << "failed at deliveryTimeoutMs from acceptance";
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
}

// Applied into a fresh builder, a deferred record's batch clock starts at its
// acceptance time (Kafka accumulator semantics), not at the apply time.
TEST(DirectIngress, DeferredRecordBatchStartIsAcceptanceTimeRegression) {
    Fx fx(stallCfg());
    fx.stall(0, 'f', 1'000);
    fx.inDuty(1, 1'000, [&] { (void)fx.send(0, 'd'); });
    ASSERT_EQ(fx.impl.deferredCount(), 1u);
    fx.impl.onProduceResponse(fx.inflightCorr(0), 100);
    fx.flush(5'000);  // sends the full builder, then applies the deferred record
    ASSERT_EQ(fx.impl.deferredCount(), 0u);
    ASSERT_EQ(builderRecords(fx.impl, 0), 1);
    EXPECT_EQ(fx.impl.partitionState(1, 0)->batchStartNs, 1'000);
}

// flush() covers records already accepted into the deferred buffer: they are
// flushed when applied, even with a long linger.
TEST(DirectIngress, FlushIncludesDeferredRecordsRegression) {
    rc::ProducerConfig cfg = stallCfg();
    cfg.lingerUs = 60'000'000;
    Fx fx(cfg);
    rc::SendResult r = rc::SendResult::Ok;
    bool first = true;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) {
        int64_t h = 0;
        if (first) {
            h = fx.S.cb([&](int64_t) { (void)fx.send(0, 'R'); });  // in-duty: deferred
            first = false;
        }
        r = fx.send(0, 'f', h);
        if (r != rc::SendResult::Ok && h != 0) fx.S.rejected(h);
    }
    fx.flush(1'000);  // size-full: sent (window 1/1)
    r = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) r = fx.send(0, 'g');
    fx.impl.onProduceResponse(fx.inflightCorr(0), 100);  // callback: R deferred (builder full)
    ASSERT_EQ(fx.impl.deferredCount(), 1u);
    fx.impl.submitFlush();  // must cover R
    fx.flush(2'000);        // sends the 'g' builder, then applies R
    ASSERT_EQ(builderRecords(fx.impl, 0), 1);
    fx.impl.onProduceResponse(fx.inflightCorr(0), 200);
    fx.flush(3'000);  // linger 60 s: only the flush request can send R now
    ASSERT_FALSE(fx.sent[0].empty());
    EXPECT_EQ(fx.sent[0].back(), 'R');
}

// An accepted deferred record is never silently dropped: a partition the
// producer can never address is rejected AT SEND TIME (InvalidTopic), not
// accepted and then lost at apply time.
TEST(DirectIngress, UnaddressablePartitionRejectedBeforeDeferralRegression) {
    Fx fx(stallCfg());
    fx.impl.debugSetMaxPartitionsForTest(2);
    rc::SendResult inDutyR = rc::SendResult::Ok;
    fx.inDuty(0, 1, [&] { inDutyR = fx.send(5, 'z'); });  // p0 created; p5 would be the 2nd
    EXPECT_EQ(inDutyR, rc::SendResult::Ok) << "room for one more partition";
    rc::SendResult inDuty2 = rc::SendResult::Ok;
    fx.inDuty(0, 2, [&] { inDuty2 = fx.send(6, 'z'); });   // p0 + p5 exist: table full
    EXPECT_EQ(inDuty2, rc::SendResult::InvalidTopic);
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
    EXPECT_EQ(fx.impl.deferredApplyFailures(), 0u);
}

// ===========================================================================
// Task 14 fix round 3
// ===========================================================================

// 1: a loss-path callback (an accepted deferred record that could not be
// applied) that calls flush() and close() must never see the apply pass's
// half-compacted arena: lost handles are completed AFTER the pass, and the
// arena stays parseable (a dead filler spans any gap).
TEST(DirectIngress, LossCallbackFlushAndCloseMidPassNoCorruptionRegression) {
    Fx fx(stallCfg());
    fx.stall(0, 'f', 1);
    bool lossFired = false;
    bool consistentInCallback = false;
    bool consistentAfterFlush = false;
    int64_t dOutcome = 0;
    // Arm (inside the callback, i.e. before the pass that runs at the end of
    // this duty work): the pass's apply ATTEMPTS are numbered from 1 — A (1,
    // applied), B (2, ABORTs: p0 is stalled -> kept), C (3) — so attempt 3, C,
    // takes the loss path.  D makes no attempt (p0 already ABORTed this pass).
    fx.inDuty(1, 2, [&] {
        (void)fx.send(2, 'A');  // applied (fresh partition) -> leaves a gap
        (void)fx.send(0, 'B');  // kept (p0 stalled) -> slid forward over A's slot
        const int64_t h = fx.S.cb([&](int64_t off) {
            lossFired = (off == -1);
            consistentInCallback = fx.impl.debugArenaConsistent();
            fx.impl.submitFlush();
            consistentAfterFlush = fx.impl.debugArenaConsistent();
            fx.impl.markRingClosed();
            fx.impl.failAllPending(rc::OpStatus::Closed);
        });
        if (fx.send(3, 'C', h) != rc::SendResult::Ok) fx.S.rejected(h);
        const int64_t hd = fx.S.cb([&](int64_t off) { dOutcome = off == -1 ? -1 : 1; });
        if (fx.send(0, 'D', hd) != rc::SendResult::Ok) fx.S.rejected(hd);
        fx.impl.debugFailDeferredApplies(3, 1);  // attempt 3 (C) fails
    });
    EXPECT_TRUE(lossFired);
    EXPECT_TRUE(consistentInCallback) << "the loss callback must never see a half-compacted arena";
    EXPECT_TRUE(consistentAfterFlush);
    EXPECT_EQ(dOutcome, -1) << "close() from the callback fails the still-deferred D exactly once";
    EXPECT_TRUE(fx.impl.debugArenaConsistent());
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
    EXPECT_EQ(fx.impl.deferredApplyFailures(), 1u);
}

// 2: a refused send to a partition whose deferred records already failed to
// apply in the latest pass (nothing changed since, outside duty work) is O(1):
// no futile apply pass per attempt.
TEST(DirectIngress, RefusedSendToStalledPartitionRunsNoPassRegression) {
    Fx fx(stallCfg());
    fx.stall(0, 'f', 1);
    fx.inDuty(1, 2, [&] { (void)fx.send(0, 'd'); });
    ASSERT_EQ(fx.impl.deferredCount(), 1u);
    const uint64_t before = fx.impl.debugDeferPasses();
    for (int i = 0; i < 100; ++i) ASSERT_EQ(fx.send(0, 'e'), rc::SendResult::BackPressured);
    EXPECT_EQ(fx.impl.debugDeferPasses(), before) << "no pass while p0's state cannot have changed";
    // Once p0 can move (its in-flight batch acked + the next pass), it applies.
    fx.impl.onProduceResponse(fx.inflightCorr(0), 100);
    fx.flush(3);
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
}

// 3: records for not-yet-created partitions share ONE aggregate cap (the
// per-partition share) — a burst to fresh partitions cannot exhaust the arena.
TEST(DirectIngress, UnknownPartitionDeferralsShareAnAggregateCapRegression) {
    rc::ProducerConfig cfg = stallCfg();
    cfg.directDeferredBytes = 64 * 1024;
    Fx fx(cfg);
    int accepted = 0;
    bool hitDeferredFull = false;
    fx.inDuty(0, 1, [&] {
        for (int32_t part = 100; part < 2'000; ++part) {
            rc::producer::RejectReason why = rc::producer::RejectReason::None;
            if (fx.send(part, 'u', 0, &why) != rc::SendResult::Ok) {
                hitDeferredFull = (why == rc::producer::RejectReason::DeferredFull);
                break;
            }
            ++accepted;
        }
    });
    EXPECT_TRUE(hitDeferredFull);
    const auto recSize = static_cast<int>(rc::producer::DeferredArena::recordSize(
        static_cast<std::size_t>(rc::producer::kRecHeaderSize) + kVal.size()));
    EXPECT_LE(accepted * recSize, static_cast<int>(fx.impl.debugDeferPartitionCap()));
    EXPECT_LT(accepted * recSize, cfg.directDeferredBytes / 2);
}

// 4: a Tier-2 batch rejected because its partition still has deferred records
// (or is unaddressable) says so — not "ring full".
TEST(DirectIngress, Tier2RejectReasonIsSpecificRegression) {
    auto* impl = new ProducerImpl(stallCfg());
    impl->enableDirectIngress(ownerDirect());
    rc::Producer p(impl);
    const rc::TopicHandle topic{1, 4};
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    const rc::Key k0 = keyFor(0, 4), k1 = keyFor(1, 4);
    // Stall p0 (one batch in flight, full builder) and defer a record for it.
    ASSERT_EQ(p.trySend(topic, k0, kVal), rc::SendResult::Ok);
    impl->flushBatches(1, 42, sink);
    rc::SendResult r = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) r = p.trySend(topic, k0, kVal);
    ASSERT_EQ(r, rc::SendResult::BackPressured);
    test_support::ScriptedAcks S;
    S.attach(p);
    p.send(topic, k1, kVal, S.tok([&](int64_t) { (void)p.trySend(topic, k0, kVal); }));
    impl->flushBatches(2, 42, sink);
    auto* ps1 = const_cast<rc::producer::PartitionState*>(impl->partitionState(1, 1));
    impl->onProduceResponse(ps1->inflight.oldestLive()->corrId, 100);
    ASSERT_EQ(impl->deferredCount(), 1u);

    auto b = p.batch(topic, k0);
    b.add(kVal);
    EXPECT_EQ(b.send(rc::AckToken{50}), rc::OpResult::BackPressured)
        << "its partition still has deferred records: retryable back-pressure";
    EXPECT_EQ(S.fired.count(50), 0u) << "a rejected batch never completes";

    impl->debugSetMaxPartitionsForTest(2);  // p0, p1 exist: p2 is unaddressable
    auto b2 = p.batch(topic, keyFor(2, 4));
    b2.add(kVal);
    EXPECT_EQ(b2.send(rc::AckToken{51}), rc::OpResult::InvalidArgument) << "unaddressable partition";
    EXPECT_EQ(S.fired.count(51), 0u);
}

// ===========================================================================
// Task 14 fix round 4 — reject reasons are RETURNED (no shared member state).
// ===========================================================================

// A Tier-2 batch on a closed Direct producer says "closed" (not a stale
// reason left over from an earlier reject, nor "ring full").
TEST(DirectIngress, Tier2RejectAfterCloseSaysClosedRegression) {
    auto* impl = new ProducerImpl(stallCfg());
    impl->enableDirectIngress(ownerDirect());
    rc::Producer p(impl);
    const rc::TopicHandle topic{1, 4};
    impl->debugSetMaxPartitionsForTest(0);  // leave an "unaddressable" reject behind first
    auto b0 = p.batch(topic, keyFor(0, 4));
    b0.add(kVal);
    EXPECT_EQ(b0.send(), rc::OpResult::InvalidArgument);
    impl->markRingClosed();
    auto b = p.batch(topic, keyFor(0, 4));
    b.add(kVal);
    EXPECT_EQ(b.send(), rc::OpResult::Closed);
}

// A Tier-2 batch sent from a foreign thread on a Direct producer says "wrong
// thread" — decided from the returned reason, no producer member is read
// (TSan: the owner thread keeps rejecting / sending concurrently).
TEST(DirectIngress, Tier2FromForeignThreadSaysWrongThreadRegression) {
    auto* impl = new ProducerImpl(stallCfg());
    impl->enableDirectIngress(ownerDirect());
    rc::Producer p(impl);
    const rc::TopicHandle topic{1, 4};
    std::atomic<bool> done{false};
    rc::OpResult res = rc::OpResult::Accepted;
    std::thread other([&] {
        auto b = p.batch(topic, keyFor(1, 4));
        b.add(kVal);
        res = b.send();
        done.store(true, std::memory_order_release);
    });
    // The owner keeps producing rejects of its own meanwhile.
    while (!done.load(std::memory_order_acquire)) {
        (void)p.trySend(topic, keyFor(0, 4), kVal);
        std::this_thread::yield();
    }
    other.join();
    EXPECT_EQ(res, rc::OpResult::WrongThread);
}

// More than 16 losses in one pass: the first 16 are completed after the pass,
// the rest are KEPT (not lost) and applied by the next pass; the arena stays
// consistent throughout.
TEST(DirectIngress, MoreLossesThanTheLostBufferKeepsTheRestRegression) {
    Fx fx(stallCfg());
    constexpr int kN = 20;
    int failed = 0;
    int succeeded = 0;
    fx.inDuty(0, 1, [&] {
        for (int i = 0; i < kN; ++i) {
            const int64_t h = fx.S.cb([&](int64_t off) { (off == -1 ? failed : succeeded)++; });
            if (fx.send(100 + i, 'L', h) != rc::SendResult::Ok) fx.S.rejected(h);
        }
        fx.impl.debugFailDeferredApplies(1, kN);  // every apply in the coming pass fails
    });
    EXPECT_EQ(failed, 16) << "the lost-handle buffer holds 16 per pass";
    EXPECT_TRUE(fx.impl.debugArenaConsistent());
    EXPECT_EQ(fx.impl.deferredCount(), static_cast<uint32_t>(kN - 16)) << "the rest are kept, not lost";
    fx.flush(2);  // next pass: no forced failures
    EXPECT_TRUE(fx.impl.debugArenaConsistent());
    EXPECT_EQ(fx.impl.deferredCount(), 0u);
    for (int i = 16; i < kN; ++i) EXPECT_EQ(builderRecords(fx.impl, 100 + i), 1) << i;
    EXPECT_EQ(failed, 16);
}
