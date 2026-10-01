// tests/producer/ProducerSendTest.cpp — back-pressure is propagated, never dropped.
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::Producer;
using rune::client::ProducerConfig;
using rune::client::SendResult;
using rune::client::TopicHandle;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;

namespace {
ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks = Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}
int64_t steadyNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
const TopicHandle kTopic{1, 4};
const std::vector<uint8_t> kVal(64, 0x42);

// Fills `impl`'s ring (via `p`) until trySend reports back-pressure. Returns
// the number of records accepted. Bounded: fails the test (ASSERT) rather
// than looping forever if the ring never fills (a test-config bug, not real
// back-pressure).
uint64_t fillUntilBackPressured(Producer& p) {
    uint64_t ok = 0;
    for (int i = 0; i < 1'000'000; ++i) {
        if (p.trySend(kTopic, Key(static_cast<uint64_t>(i)), kVal) != SendResult::Ok) return ok;
        ++ok;
    }
    ADD_FAILURE() << "ring never filled";
    return ok;
}
} // namespace

// Regression: Producer::send(void) returned silently when the producer ring was
// full — the record was dropped.  It must back off until the runner drains.
TEST(ProducerSend, VoidSendNeverDropsOnFullRingRegression) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, /*ringCap=*/4 * 1024);
    Producer p(impl);
    constexpr uint64_t kRecords = 2000;
    std::atomic<bool> sendsDone{false};
    std::thread drainer([&] {  // plays the runner thread
        uint64_t frames = 0, bytes = 0;
        const CountingSink sink{&frames, &bytes};
        // NOTE: drainRing() reads at most ONE message per call and can return 0
        // on a call that only consumed ring wrap-around padding (the real next
        // message becomes visible on the VERY NEXT call) — d==0 does not mean
        // "ring empty, nothing left to do". So the only reliable stop condition
        // is recordsSent() reaching kRecords; the 20M bound is just a circuit
        // breaker against a genuine hang.
        for (int i = 0; i < 20'000'000 && impl->recordsSent() < kRecords; ++i) {
            const int64_t now = steadyNs();
            (void)impl->drainRing(now);
            impl->flushBatches(now, 42, sink);
        }
    });
    for (uint64_t i = 0; i < kRecords; ++i) p.send(kTopic, Key(i), kVal);
    sendsDone.store(true, std::memory_order_release);
    drainer.join();
    EXPECT_EQ(impl->recordsSent(), kRecords);
}

// Regression (ruling: void send never block-waits on the heartbeat forever).
// With no pump hook wired (the ProducerImpl-only contract used by Thread mode
// and plain unit tests), a back-pressured blocking send() must still be
// BOUNDED: once the ring's consumer heartbeat has not advanced for longer than
// agentLivenessTimeoutMs, it throws rather than hanging forever.
TEST(ProducerSend, VoidSendThrowsWhenTheRunnerStopsDrainingRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.agentLivenessTimeoutMs = 5;
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(cfg, corr, 4 * 1024);
    // Injected clock: +1 ms per read, so liveness expires after a few back-offs.
    impl->setClockForTesting(+[]() noexcept -> int64_t {
        static int64_t t = 0;
        return t += 1'000'000;
    });
    Producer p(impl);
    for (int i = 0; i < 1000 && p.trySend(kTopic, Key(uint64_t{1}), kVal) == SendResult::Ok; ++i) {}
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), kVal), std::runtime_error);
}

TEST(ProducerSend, TrySendReportsBackPressureAndLosesNothing) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, 4 * 1024);
    Producer p(impl);
    uint64_t ok = 0;
    SendResult last = SendResult::Ok;
    for (int i = 0; i < 1000 && last == SendResult::Ok; ++i) {
        last = p.trySend(kTopic, Key(static_cast<uint64_t>(i)), kVal);
        if (last == SendResult::Ok) ++ok;
    }
    ASSERT_EQ(last, SendResult::BackPressured);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int i = 0; i < 100'000 && impl->drainRing(1) > 0; ++i) impl->flushBatches(1, 42, sink);
    EXPECT_EQ(impl->recordsSent(), ok);
}

TEST(ProducerSend, InvalidInputIsReportedNeverSilentlyDropped) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr);
    Producer p(impl);
    const std::vector<uint8_t> tooBig(Producer::kMaxSendValueBytes + 1, 0);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), tooBig), SendResult::ValueTooLarge);
    EXPECT_EQ(p.trySend(TopicHandle{}, Key(uint64_t{1}), kVal), SendResult::InvalidTopic);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), tooBig), std::length_error);
    EXPECT_THROW(p.send(TopicHandle{}, Key(uint64_t{1}), kVal), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Invoker-mode owner-thread ruling: a void send() that backs off on a full
// ring must NOT block-wait on a consumer that IS the calling thread (Invoker
// mode: the owner thread drains the ring via Client::doWork()). When a pump
// hook is wired (Client::producer() wires one in Invoker mode), BackPressured
// pumps the duty cycle once per retry THROUGH THE HOOK instead of idling on
// the ring's heartbeat — see also InvokerClientTest.cpp for the real Client
// end-to-end wiring of this hook.
// ---------------------------------------------------------------------------

// Regression: a wired pump hook that CAN make progress (owner thread, outside
// a duty-cycle callback) is used instead of idling/waiting on the heartbeat —
// the blocking send succeeds immediately, with no liveness-timeout wait at all.
TEST(ProducerSend, VoidSendPumpsInsteadOfWaitingOnItselfRegression) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    struct PumpCtx {
        ProducerImpl* impl;
        int           calls = 0;
    } ctx{impl.get()};
    impl->setPumpFn(
        +[](void* raw) noexcept -> bool {
            auto* c = static_cast<PumpCtx*>(raw);
            ++c->calls;
            uint64_t frames = 0, bytes = 0;
            const CountingSink sink{&frames, &bytes};
            c->impl->drainRing(steadyNs());
            c->impl->flushBatches(steadyNs(), 42, sink);
            return true; // "owner thread, outside a duty-cycle callback": pump ran
        },
        &ctx);

    EXPECT_NO_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal));
    EXPECT_GT(ctx.calls, 0) << "the pump hook must be used instead of idling on the heartbeat";
}

// Regression: when the pump hook DECLINES (e.g. called from inside a
// duty-cycle callback, or off the owner thread — the hook itself enforces
// that gate), the blocking send must fail FAST with std::runtime_error, never
// spin and never silently wait out the liveness timeout.
TEST(ProducerSend, VoidSendThrowsInsteadOfPumpingWhenPumpDeclinesRegression) {
    std::atomic<int64_t> corr{1};
    auto impl = std::make_shared<ProducerImpl>(voidCfg(), corr, 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    int declineCalls = 0;
    impl->setPumpFn(
        +[](void* raw) noexcept -> bool {
            ++*static_cast<int*>(raw);
            return false; // the hook's own gate said no (callback / non-owner thread)
        },
        &declineCalls);

    EXPECT_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal), std::runtime_error);
    EXPECT_EQ(declineCalls, 1) << "must fail on the FIRST decline, never retry/spin";
}
