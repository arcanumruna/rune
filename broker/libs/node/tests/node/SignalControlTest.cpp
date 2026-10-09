// SignalControlTest.cpp — F9a: signals become ring events (no atomics, no
// signal-handler globals).  A dedicated sigwait thread posts Stop / FrDump; the
// halt path posts Halt{exitCode} on the same ring; main is the one consumer.
#include <rune/node/SignalControl.h>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace std::chrono_literals;

namespace {
struct Ev { std::int32_t type; std::int64_t a; };

// Poll the control ring (as main does) until `n` events arrived or 5 s passed.
std::vector<Ev> pollEvents(ControlEventRing& ring, std::size_t n,
                           std::chrono::milliseconds budget = 5000ms) {
    std::vector<Ev> out;
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (out.size() < n && std::chrono::steady_clock::now() < deadline) {
        ring.poll([&](std::int32_t t, std::int64_t a, std::int64_t) { out.push_back({t, a}); });
        if (out.size() < n) std::this_thread::sleep_for(1ms);
    }
    return out;
}

struct MaskGuard {   // block for this test, restore the gtest thread's mask after
    sigset_t old{};
    MaskGuard() { const sigset_t s = controlSignalSet(); ::pthread_sigmask(SIG_BLOCK, &s, &old); }
    ~MaskGuard() { ::pthread_sigmask(SIG_SETMASK, &old, nullptr); }
};
} // namespace

TEST(SignalControlRegression, SignalsArriveAsRingEventsInOrder) {
    MaskGuard mask;
    ControlEventRing ring;
    {
        SignalThread sig(ring);
        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGUSR1), 0);
        auto e1 = pollEvents(ring, 1);
        ASSERT_EQ(e1.size(), 1u);
        EXPECT_EQ(e1[0].type, kFrDumpEvent);

        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGUSR2), 0);   // external wake: ignored
        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGTERM), 0);
        auto e2 = pollEvents(ring, 1);
        ASSERT_EQ(e2.size(), 1u) << "signal thread must survive an external SIGUSR2; got types " << (e2.size() > 0 ? e2[0].type : -1) << "," << (e2.size() > 1 ? e2[1].type : -1) << " a=" << (e2.size() > 0 ? e2[0].a : -1) << "," << (e2.size() > 1 ? e2[1].a : -1);
        EXPECT_EQ(e2[0].type, kStopEvent);
        EXPECT_EQ(e2[0].a, SIGTERM);

        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGINT), 0);
        auto e3 = pollEvents(ring, 1);
        ASSERT_EQ(e3.size(), 1u);
        EXPECT_EQ(e3[0].type, kStopEvent);
        EXPECT_EQ(e3[0].a, SIGINT);
    }   // dtor: Quit command + SIGUSR2 wake + join — must not hang
    EXPECT_TRUE(pollEvents(ring, 1, 200ms).empty()) << "shutdown posts no event";
}

// Task 9 (RUNE_TLS SIGHUP reload): SIGHUP must become kTlsReloadEvent, not
// kStopEvent — a renewal signal must never stop the broker.
TEST(SignalControlRegression, SighupArrivesAsTlsReloadEvent) {
    MaskGuard mask;
    ControlEventRing ring;
    {
        SignalThread sig(ring);
        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGHUP), 0);
        auto ev = pollEvents(ring, 1);
        ASSERT_EQ(ev.size(), 1u);
        EXPECT_EQ(ev[0].type, kTlsReloadEvent);

        // The signal thread must keep servicing SIGINT/SIGTERM afterwards —
        // SIGHUP must not have left it in some special reload-only state.
        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGTERM), 0);
        auto ev2 = pollEvents(ring, 1);
        ASSERT_EQ(ev2.size(), 1u);
        EXPECT_EQ(ev2[0].type, kStopEvent);
    }
}

// The halt exit code travels by ring (from any thread), not a global atomic.
TEST(SignalControlRegression, HaltCarriesExitCodeOverTheRing) {
    ControlEventRing ring;
    std::thread runtime([&ring] { EXPECT_TRUE(postHalt(ring, 74)); });
    runtime.join();
    auto ev = pollEvents(ring, 1);
    ASSERT_EQ(ev.size(), 1u);
    EXPECT_EQ(ev[0].type, kHaltEvent);
    EXPECT_EQ(ev[0].a, 74);
}

// A shell starts background jobs with SIGINT ignored (POSIX).  An IGNORED signal
// is discarded at generation even when blocked, so sigwait() would never see it
// and Ctrl-C / `kill -INT` would no longer stop the broker (the old
// std::signal(SIGINT, handler) overrode the inherited SIG_IGN).
// blockControlSignals() must reset the control signals to SIG_DFL first.
TEST(SignalControlRegression, InheritedIgnoredSigintStillStops) {
    struct sigaction ign{};
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    struct sigaction oldInt{}, oldTerm{}, oldUsr1{}, oldUsr2{};
    ::sigaction(SIGINT, &ign, &oldInt);   // as inherited from `cmd &` in a script
    ::sigaction(SIGTERM, nullptr, &oldTerm);
    ::sigaction(SIGUSR1, nullptr, &oldUsr1);
    ::sigaction(SIGUSR2, nullptr, &oldUsr2);
    sigset_t oldMask{};
    ::pthread_sigmask(SIG_SETMASK, nullptr, &oldMask);

    ASSERT_EQ(blockControlSignals(), 0);   // the production entry point
    ControlEventRing ring;
    {
        SignalThread sig(ring);
        ASSERT_EQ(::pthread_kill(sig.nativeHandle(), SIGINT), 0);
        auto ev = pollEvents(ring, 1);
        ASSERT_EQ(ev.size(), 1u) << "an inherited SIG_IGN must not swallow SIGINT";
        EXPECT_EQ(ev[0].type, kStopEvent);
    }
    ::pthread_sigmask(SIG_SETMASK, &oldMask, nullptr);
    ::sigaction(SIGINT, &oldInt, nullptr);
    ::sigaction(SIGTERM, &oldTerm, nullptr);
    ::sigaction(SIGUSR1, &oldUsr1, nullptr);
    ::sigaction(SIGUSR2, &oldUsr2, nullptr);
}
