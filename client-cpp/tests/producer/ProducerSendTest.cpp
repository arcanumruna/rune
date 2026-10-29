// tests/producer/ProducerSendTest.cpp — back-pressure is propagated, never dropped.
#include "../support/AckLedger.h"
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <span>
#include <string_view>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::Producer;
using rune::client::ProducerConfig;
using rune::client::SendResult;
using rune::client::TopicHandle;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;
using rune::client::producer::PumpResult;

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
    ProducerConfig cfg = voidCfg();
    // fix1 minor 14: generous — this test's own drainer thread is the only
    // consumer, and a loaded CI/TSan run can lag it enough to trip the default
    // 1 s liveness window, turning a slow-but-fine drain into a spurious throw.
    cfg.agentLivenessTimeoutMs = 30'000;
    auto* impl = new ProducerImpl(cfg, /*ringCap=*/4 * 1024);
    Producer p(impl);
    constexpr uint64_t kRecords = 2000;
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
    auto* impl = new ProducerImpl(cfg, 4 * 1024);
    // Injected clock: +1 ms per read, so liveness expires after a few back-offs.
    impl->setClockForTesting(+[]() noexcept -> int64_t {
        static int64_t t = 0;
        return t += 1'000'000;
    });
    Producer p(impl);
    for (int i = 0; i < 1000 && p.trySend(kTopic, Key(uint64_t{1}), kVal) == SendResult::Ok; ++i) {}
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), kVal), std::runtime_error);
}

// D7 review (blocking): a bulk ns->ms rename also hit the producer's DEFAULT liveness clock (steadyNowNs_), not
// only record timestamps: it returned steady MILLISECONDS while the ring heartbeat and livenessNs are steady
// NANOSECONDS -> the dead-runner check never fired (send() on a wedged runner blocked forever).  Tests inject
// nanoClock, so only this asserts the default clock's unit.
TEST(ProducerSend, DefaultLivenessClockIsSteadyNanosecondsRegression) {
    const int64_t before = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    const int64_t got = ProducerImpl::defaultClockNowForTest();
    const int64_t after = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    EXPECT_GE(got, before);
    EXPECT_LE(got, after);
}

TEST(ProducerSend, TrySendReportsBackPressureAndLosesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
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

TEST(ProducerSend, InvalidInputIsReportedNeverSilentlyDroppedRegression) {
    auto* impl = new ProducerImpl(voidCfg());
    Producer p(impl);
    const std::vector<uint8_t> tooBig(Producer::kMaxRecordBytes + 1, 0);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), tooBig), SendResult::MessageTooLarge);  // B6
    EXPECT_EQ(p.trySend(TopicHandle{}, Key(uint64_t{1}), kVal), SendResult::InvalidTopic);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), tooBig), std::length_error);
    EXPECT_THROW(p.send(TopicHandle{}, Key(uint64_t{1}), kVal), std::invalid_argument);
}

// fix1 minor 11: a value that fits kMaxSendValueBytes but can never fit a tiny
// ring's maxMsgLength must map to ValueTooLarge (not BackPressured) — otherwise
// sendBlocking would retry it forever (nothing ever makes room for it).
TEST(ProducerSend, ValueLargerThanRingMaxMsgLengthIsValueTooLargeNotBackPressuredRegression) {
    // Smallest usable ring: 64 B usable capacity -> maxMsgLength = 64/8 = 8 B,
    // far smaller than kMaxSendValueBytes.
    auto* impl = new ProducerImpl(voidCfg(), /*ringCap=*/64);
    Producer p(impl);
    const std::vector<uint8_t> val(32, 0x11); // fits kMaxSendValueBytes, not this ring
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), val), SendResult::ValueTooLarge);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), val), std::length_error);
}

// fix1 I6: a controlledRead() call that only consumes wrap-around padding must
// not report 0 work while a real record waits right past the wrap — drainRing
// must continue into it within the SAME call.
TEST(ProducerSend, RingWrapPaddingDoesNotReportIdleRegression) {
    // 48-byte records (38-byte header + 2-byte value, aligned to 8): a 512 B
    // ring holds exactly 10 (480 B) before the 11th hits real back-pressure
    // (not the wrap case this test targets). Draining 5 reopens total capacity
    // but leaves only 32 B contiguous to the physical buffer end, so the next
    // send pads that remainder and wraps to offset 0 — the scenario under test.
    auto* impl = new ProducerImpl(voidCfg(), /*ringCap=*/512);
    Producer p(impl);
    const std::vector<uint8_t> val(2, 0x22);
    for (uint64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(p.trySend(kTopic, Key(i), val), SendResult::Ok);
    }

    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(impl->drainRing(1), 1);
        impl->flushBatches(1, 42, sink);
    }

    // Wraps: physical tail is 32 B from the buffer end (< 48 B/record).
    ASSERT_EQ(p.trySend(kTopic, Key(uint64_t{10}), val), SendResult::Ok);

    // Drain records 5..9 normally, then the wrapped record 10 — whose
    // drainRing() call first consumes ONLY the padding. Before the fix, that
    // call returned 0 (reporting idle) even though record 10 sat right past
    // the wrap; it must now be delivered within the SAME call.
    constexpr uint64_t kTotal = 11;
    for (int i = 0; i < 6 && impl->recordsSent() < kTotal; ++i) {
        const int drained = impl->drainRing(1);
        impl->flushBatches(1, 42, sink);
        ASSERT_NE(drained, 0) << "drainRing reported idle (0) with "
                              << (kTotal - impl->recordsSent())
                              << " records still undrained — the ring-wrap padding false-idle regressed";
    }
    EXPECT_EQ(impl->recordsSent(), kTotal);
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
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    struct PumpCtx {
        ProducerImpl* impl;
        int           calls = 0;
    } ctx{impl};
    impl->setPumpFn(
        +[](void* raw) noexcept -> PumpResult {
            auto* c = static_cast<PumpCtx*>(raw);
            ++c->calls;
            uint64_t frames = 0, bytes = 0;
            const CountingSink sink{&frames, &bytes};
            c->impl->drainRing(steadyNs());
            c->impl->flushBatches(steadyNs(), 42, sink);
            return PumpResult::Pumped; // "owner thread, outside a duty-cycle callback"
        },
        &ctx, std::this_thread::get_id());

    EXPECT_NO_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal));
    EXPECT_GT(ctx.calls, 0) << "the pump hook must be used instead of idling on the heartbeat";
}

// Regression: when the pump hook DECLINES because we are inside a duty-cycle
// callback (the hook itself enforces that gate), the blocking send must fail
// FAST with std::runtime_error, never spin and never silently wait out the
// liveness timeout.
TEST(ProducerSend, VoidSendThrowsInsteadOfPumpingWhenPumpDeclinesRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    int declineCalls = 0;
    impl->setPumpFn(
        +[](void* raw) noexcept -> PumpResult {
            ++*static_cast<int*>(raw);
            return PumpResult::DeclinedInCycle; // e.g. called from inside a duty-cycle callback
        },
        &declineCalls, std::this_thread::get_id());

    EXPECT_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal), std::runtime_error);
    EXPECT_EQ(declineCalls, 1) << "must fail on the FIRST decline, never retry/spin";
}

// Regression (fix1 C1 + fix2 Important-2): NotOwner is now decided INSIDE
// ProducerImpl by comparing the calling thread against the duty-thread id
// copied in by setPumpFn() — BEFORE the hook is ever invoked. A thread that
// does not match (the test thread here, vs. a duty-thread id that can never
// equal any real thread's id) must never call the hook at all (so it never
// dereferences the hook's context, which could otherwise race a concurrent
// destruction of whatever owns that context), and falls back to the SAME
// heartbeat-bounded wait as "no hook wired" — this must NOT throw immediately
// the way DeclinedInCycle does.
TEST(ProducerSend, NonDutyThreadNeverInvokesPumpHookFallsBackToHeartbeatWaitRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.agentLivenessTimeoutMs = 5;
    auto* impl = new ProducerImpl(cfg, 4 * 1024);
    impl->setClockForTesting(+[]() noexcept -> int64_t {
        static int64_t t = 0;
        return t += 1'000'000;
    });
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    int pumpCalls = 0;
    // std::thread::id{} (the default, "no thread") can never equal this test
    // thread's real id — every call from here is therefore off the duty thread.
    impl->setPumpFn(
        +[](void* raw) noexcept -> PumpResult {
            ++*static_cast<int*>(raw);
            return PumpResult::Pumped; // would succeed instantly if ever called
        },
        &pumpCalls, std::thread::id{});

    EXPECT_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal), std::runtime_error);
    EXPECT_EQ(pumpCalls, 0) << "a non-duty thread must never call the pump hook";
}

// Regression (fix1 C2/I3): a Producer that outlives its (now-closed) ring
// never touches the pump hook again — trySubmit intercepts via the ring's
// closed word before sendBlocking could ever call a (potentially dangling)
// pumpFn_/pumpCtx_.
TEST(ProducerSend, ClosedRingNeverInvokesThePumpHookRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    int pumpCalls = 0;
    impl->setPumpFn(
        +[](void* raw) noexcept -> PumpResult {
            ++*static_cast<int*>(raw);
            return PumpResult::Pumped;
        },
        &pumpCalls, std::this_thread::get_id());

    impl->markRingClosed();

    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{ok}), kVal), SendResult::Closed);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{ok}), kVal), std::logic_error);
    EXPECT_EQ(pumpCalls, 0) << "a closed ring must never reach the pump hook";
}

// ---------------------------------------------------------------------------
// Task 13 fix1 I8 / minor 13
// ---------------------------------------------------------------------------

// F8c C6 (replaces the fix1 I8 callback test): a token send on an invalid
// topic is rejected synchronously — the exactly-once rule: nothing that was
// not accepted ever completes (no "-1 callback" any more).
TEST(ProducerSend, AckTokenSendOnInvalidTopicIsRejectedWithNoCompletionRegression) {
    auto* impl = new ProducerImpl(voidCfg());
    Producer p(impl);
    test_support::AckLedger L;
    L.attach(*impl);
    EXPECT_EQ(p.trySend(TopicHandle{}, Key(uint64_t{1}), kVal, rune::client::AckToken{1}),
              SendResult::InvalidTopic);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), kVal, rune::client::AckToken{rune::client::kMaxToken + 1}),
              SendResult::InvalidArgument);
    impl->failAllPending(rune::client::OpStatus::Closed);
    EXPECT_TRUE(L.recs.empty()) << "a rejected send never completes";
}

// fix1 minor 13 (F8c C6 form): the rejection of a token send is the
// SendResult itself — BackPressured and ValueTooLarge stay distinct, and
// neither ever completes.
TEST(ProducerSend, RejectedAckTokenSendResultIsDistinctPerCauseRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attach(*impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{ok}), kVal, rune::client::AckToken{1}), SendResult::BackPressured);
    const std::vector<uint8_t> tooBig(Producer::kMaxRecordBytes + 1, 0);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{ok}), tooBig, rune::client::AckToken{2}), SendResult::MessageTooLarge);  // B6
    impl->failAllPending(rune::client::OpStatus::Closed);  // completes only what was accepted (void here)
    EXPECT_EQ(L.times(1), 0);
    EXPECT_EQ(L.times(2), 0);
}

// ---------------------------------------------------------------------------
// fix2 minor: close() arriving while a (Thread-mode-style) app thread is
// already blocked in sendBlocking's heartbeat-wait must be decided promptly
// by the in-loop closed check — never by waiting out agentLivenessTimeoutMs.
//
// A real Thread-mode Client's background runner drains a ring this small
// almost instantly in real time, making the exact interleaving ("blocked,
// then close() arrives") impossible to force deterministically through a live
// Client. Exercised directly at the ProducerImpl level instead: with no pump
// hook wired, sendBlocking takes the heartbeat-wait path deterministically
// (nothing ever drains this ring) — then markRingClosed() is flipped WHILE it
// is blocked there, exactly what Client::closeImpl_'s closeAllRings() does to
// a live producer's ring.
// ---------------------------------------------------------------------------
TEST(ProducerSend, CloseWhileSendBlockingIsWaitingThrowsPromptlyRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.agentLivenessTimeoutMs = 60'000; // would hang ~a minute if the in-loop check regressed
    auto* impl = new ProducerImpl(cfg, 4 * 1024);
    Producer p(impl);
    const uint64_t ok = fillUntilBackPressured(p);
    ASSERT_GT(ok, 0u);

    // No pump hook wired (equivalent to NotOwner — see
    // NonDutyThreadNeverInvokesPumpHookFallsBackToHeartbeatWaitRegression,
    // above, for the thread-id-mismatch path that reaches the same branch
    // through a real Client): sendBlocking takes the heartbeat-wait path
    // deterministically, forever (nothing ever drains this ring, so the
    // heartbeat is never published either) — until markRingClosed() below.

    std::atomic<bool> threw{false};
    std::atomic<bool> enteredWait{false};
    const auto t0 = std::chrono::steady_clock::now();
    std::thread blocker([&] {
        enteredWait.store(true, std::memory_order_release);
        try {
            p.send(kTopic, Key(uint64_t{ok}), kVal);
        } catch (const std::logic_error&) {
            threw.store(true, std::memory_order_release);
        }
    });

    while (!enteredWait.load(std::memory_order_acquire)) std::this_thread::yield();
    // Bounded settle: give the blocker a few real backoff iterations so it is
    // genuinely inside the wait loop (not merely about to enter it) before the
    // close signal arrives — this does not wait out any timeout, just a few µs
    // of BackoffIdleStrategy's initial spin.
    for (int i = 0; i < 1000; ++i) std::this_thread::yield();

    impl->markRingClosed(); // simulates Client::closeImpl_'s closeAllRings()

    blocker.join();
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    EXPECT_TRUE(threw.load()) << "must throw std::logic_error(\"client closed\")";
    EXPECT_LT(elapsedMs, 5000)
        << "sendBlocking waited instead of being decided promptly by the in-loop closed "
           "check (agentLivenessTimeoutMs=60s would otherwise make this take a minute)";
}

// ---------------------------------------------------------------------------
// F8c C6 (replaces fix3 I3 / fix5 minor 5, whose failUnsubmitted() is gone):
// an app thread's REJECTED token sends (ring full) never complete — on any
// thread — while the duty thread completes the ACCEPTED ones (failAllPending).
// The ack handler runs on the duty thread only, so the ledger (plain state) is
// written by one thread; TSan checks that no app-thread path touches it.
// ---------------------------------------------------------------------------
TEST(ProducerSend, RejectedTokenSendsNeverCompleteWhileDutyFailsAcceptedRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attach(*impl);

    // 1. Accepted token sends: completed later by the duty thread.
    constexpr int kAccepted = 3;
    for (int i = 0; i < kAccepted; ++i) {
        ASSERT_EQ(p.trySend(kTopic, Key(static_cast<uint64_t>(i)), kVal,
                            rune::client::AckToken{static_cast<rune::client::Token>(i + 1)}),
                  SendResult::Ok);
    }
    // 2. Fill the remainder: from here every send back-pressures.
    ASSERT_GT(fillUntilBackPressured(p), 0u);

    constexpr int kRejected = 50;
    std::atomic<bool> appDone{false};
    std::atomic<int>  rejected{0};
    std::thread duty([&] {
        while (!appDone.load(std::memory_order_acquire)) std::this_thread::yield();  // event wait
        impl->failAllPending(rune::client::OpStatus::Closed);
    });
    std::thread app([&] {
        for (int i = 0; i < kRejected; ++i) {
            if (p.trySend(kTopic, Key(static_cast<uint64_t>(1000 + i)), kVal,
                          rune::client::AckToken{static_cast<rune::client::Token>(1000 + i)}) ==
                SendResult::BackPressured) {
                rejected.fetch_add(1);
            }
        }
        appDone.store(true, std::memory_order_release);
    });
    app.join();
    duty.join();

    EXPECT_EQ(rejected.load(), kRejected) << "every app-thread send must have been rejected";
    EXPECT_TRUE(L.exactlyOnce(1, kAccepted + 1)) << "each accepted token completes exactly once";
    for (int i = 0; i < kRejected; ++i) EXPECT_EQ(L.times(1000 + i), 0) << "rejected token " << i;
    EXPECT_EQ(L.recs.size(), static_cast<std::size_t>(kAccepted));
}

// ---------------------------------------------------------------------------
// Backlog B6 (user decision 2026-10-03): a record (key + value bytes) larger
// than Producer::kMaxRecordBytes = 16 KiB is rejected AT APPEND with
// MessageTooLarge — non-retriable (never BackPressured) — on the Ring and the
// Direct path and by a Tier-2 BatchBuilder; the largest legal record is
// produced intact.  (The broker's hard limit stays the batch limit.)
// ---------------------------------------------------------------------------
#include <rune/client/producer/BatchBuilder.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

TEST(ProducerSend, RecordOver16KiBIsMessageTooLargeAtAppend) {
    static_assert(Producer::kMaxRecordBytes == 16 * 1024);
    auto* impl = new ProducerImpl(voidCfg());
    Producer p(impl);
    const Key k8(uint64_t{7});
    ASSERT_EQ(k8.size(), 8u);
    const std::vector<uint8_t> fits(Producer::kMaxRecordBytes - k8.size(), 0x5C);
    const std::vector<uint8_t> over(fits.size() + 1, 0x5D);
    EXPECT_EQ(p.trySend(kTopic, k8, over), SendResult::MessageTooLarge);
    EXPECT_THROW(p.send(kTopic, k8, over), std::length_error);
    const std::vector<uint8_t> overNoKey(Producer::kMaxRecordBytes + 1, 0x5E);
    EXPECT_EQ(p.trySend(kTopic, Key{}, overNoKey), SendResult::MessageTooLarge);
    EXPECT_EQ(p.trySend(kTopic, Key{}, std::span<const uint8_t>(overNoKey).first(Producer::kMaxRecordBytes)),
              SendResult::Ok);
    EXPECT_GE(Producer::kMaxSendValueBytes + Key::kMaxInline, Producer::kMaxRecordBytes - 0u);
    EXPECT_EQ(p.trySend(kTopic, k8, fits), SendResult::Ok);

    std::vector<std::size_t> valueSizes;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        const auto f = rune::protocol::client::Produce::decode(b, n);
        EXPECT_TRUE(f);
        if (!f) return true;
        rune::protocol::RecordBatchReader rd(f->recordBatch.data(), f->recordBatch.size());
        EXPECT_TRUE(rd.ensureValid());
        rd.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) { valueSizes.push_back(v.size()); });
        return true;
    };
    for (int i = 0; i < 100 && valueSizes.size() < 2; ++i) {
        impl->drainRing(1, 16);
        impl->flushBatches(1, 42, sink);
    }
    std::sort(valueSizes.begin(), valueSizes.end(), std::greater<>());
    EXPECT_EQ(impl->recordsSent(), 2u);
    ASSERT_EQ(valueSizes.size(), 2u);
    EXPECT_EQ(valueSizes[0], Producer::kMaxRecordBytes);
    EXPECT_EQ(valueSizes[1], fits.size());
}

TEST(ProducerSend, DirectAppendRejectsARecordOver16KiB) {
    ProducerImpl impl(voidCfg());
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    const std::vector<uint8_t> over(Producer::kMaxRecordBytes, 0x01);  // + 8-byte key = 16 KiB + 8
    EXPECT_EQ(impl.trySubmit(1, 0, Key(uint64_t{1}), over, 0), SendResult::MessageTooLarge);
    EXPECT_EQ(impl.trySubmit(1, 0, Key{}, over, 0), SendResult::Ok);
}

TEST(ProducerSend, Tier2BatchBuilderRejectsARecordOver16KiB) {
    auto* impl = new ProducerImpl(voidCfg());
    Producer p(impl);
    rune::client::BatchBuilder bb(impl, 1, 0);
    const std::vector<uint8_t> v(Producer::kMaxRecordBytes - 2, 0x02);
    EXPECT_THROW(bb.add(std::span<const uint8_t>(v), "abc"), std::length_error);
    EXPECT_NO_THROW(bb.add(std::span<const uint8_t>(v), "ab"));
}

// Checked writes Part 02: a Key longer than Key::kMaxInline is refused at every producer entry, never truncated.
TEST(CheckedWritesKey, OverLongKeyIsRefusedEverywhere) {
    auto* impl = new ProducerImpl(voidCfg());
    Producer p(impl);
    const std::vector<uint8_t> longKey(17, 0x33);
    const Key tooLong{std::span<const uint8_t>(longKey)};
    ASSERT_TRUE(tooLong.tooLong());
    EXPECT_EQ(p.trySend(kTopic, tooLong, kVal), SendResult::KeyTooLong);
    try {
        p.send(kTopic, tooLong, kVal);
        FAIL() << "send accepted an over-long key";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("key is longer than 16 bytes"), std::string::npos) << e.what();
    }
    auto claim = p.tryClaim(kTopic, tooLong, 8);
    EXPECT_FALSE(claim);
    EXPECT_EQ(claim.result(), SendResult::KeyTooLong);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    EXPECT_EQ(impl->drainRing(1), 0) << "nothing was queued";
    impl->flushBatches(1, 42, sink);
    EXPECT_EQ(impl->recordsSent(), 0u);
    // A 16-byte key is still accepted.
    const Key full{std::span<const uint8_t>(longKey.data(), 16)};
    EXPECT_EQ(p.trySend(kTopic, full, kVal), SendResult::Ok);
}
