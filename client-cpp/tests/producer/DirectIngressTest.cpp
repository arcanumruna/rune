// tests/producer/DirectIngressTest.cpp — invoker-mode (Direct) producer ingress.
//
// Task 14 (spec §4.1/§4.2): in RunnerMode::Invoker the app thread IS the duty-cycle
// thread, so send() appends straight into the partition builder (no ring hop) and
// the records of one pass leave as one frame per partition at the end of the pass.
// Every drive loop is iteration-capped; nothing sleeps.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
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
        auto f = client->connect();
        ASSERT_TRUE(drive([&] { return f.wait_for(0s) == std::future_status::ready; }));
        f.get();
    }
};
} // namespace

// ---------------------------------------------------------------------------
// ProducerImpl in isolation
// ---------------------------------------------------------------------------

TEST(DirectIngress, SendAppendsWithoutTheRing) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr, /*ringCap=*/4 * 1024);  // the ring could hold ~60 records
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
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr, 4 * 1024);
    EXPECT_EQ(impl.ingressMode(), ProducerImpl::IngressMode::Ring);
}

TEST(DirectIngress, OffOwnerThreadIsRejected) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    rc::SendResult r = rc::SendResult::Ok;
    std::thread other([&] { r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0); });
    other.join();
    EXPECT_EQ(r, rc::SendResult::WrongThread);
}

TEST(DirectIngress, TrySendAllocatesNothing) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr);
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
    std::atomic<int64_t> corr{1};
    rc::ProducerConfig cfg = voidCfg();
    cfg.batchBytes = 256;
    ProducerImpl impl(cfg, corr);
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
    std::atomic<int64_t> corr{1};
    rc::ProducerConfig cfg = voidCfg();
    cfg.batchBytes = 256;
    ProducerImpl impl(cfg, corr);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    rc::SendResult r = rc::SendResult::Ok;
    for (int i = 0; i < 10'000 && r == rc::SendResult::Ok; ++i) {
        r = impl.trySubmit(1, 0, rc::Key(uint64_t{1}), kVal, 0);
    }
    ASSERT_EQ(r, rc::SendResult::BackPressured);
    EXPECT_THROW(impl.sendBlocking(r, 1, 0, rc::Key(uint64_t{1}), kVal), std::runtime_error);
}

TEST(DirectIngress, ClosedProducerReportsClosedRegression) {
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(voidCfg(), corr);
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
    auto f1 = p.send(topic, rc::Key(uint64_t{1}), kVal, rc::want_future);
    for (int i = 0; i < 2'000; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_TRUE(r.gw.produced.empty()) << "linger holds the batch";
    p.flush();
    ASSERT_TRUE(r.drive([&] { return f1.wait_for(0s) == std::future_status::ready; }));
    EXPECT_GE(f1.get(), 0);

    auto b = p.batch(topic, rc::Key(uint64_t{1}));
    b.add(kVal);
    b.add(kVal);
    auto f2 = b.send();
    ASSERT_TRUE(r.drive([&] { return f2.wait_for(0s) == std::future_status::ready; }));
    EXPECT_GE(f2.get(), 0);
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

    bool threw = false;
    bool returned = false;
    p.send(topic, k0, kVal, [&](int64_t) {
        try {
            p.send(topic, k1, kVal);  // void, back-pressured, inside the duty cycle
            returned = true;
        } catch (const std::runtime_error&) {
            threw = true;
        }
    });
    ASSERT_TRUE(r.drive([&] { return threw || returned; }))
        << "must throw, not hang or re-enter doWork()";
    EXPECT_TRUE(threw);
    EXPECT_FALSE(returned);
    EXPECT_EQ(r.gw.held.size(), 1u) << "no extra partition-1 frame: the pump never re-entered";
}

TEST(DirectIngress, SendAfterClientCloseReturnsClosedRegression) {
    Rig r(invokerCfg(3910, 3911, 1));
    auto p = r.client->producer(voidCfg());
    const auto topic = r.client->topic("t");
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Ok);
    r.client->close();
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
