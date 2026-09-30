// tests/transport/InvokerTransportTest.cpp — AeronTransport in DriverMode::Invoker.
// Every drive loop is bounded by kPumpCap duty cycles and needs no sleep: the
// test thread IS the driver.
#include <rune/client/transport/AeronTransport.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using Transport  = rune::client::transport::AeronTransport;
using Config     = rune::client::transport::TransportConfig;
using DriverMode = rune::client::transport::DriverMode;

namespace {

std::atomic<int> g_ctr{0};

std::string uniqueDir(const char* prefix) {
    return std::string("/tmp/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) +
           "_" + std::to_string(g_ctr.fetch_add(1));
}

Config invokerConfig(const std::string& ingress, const std::string& egress, int32_t stream) {
    Config c;
    c.ingressChannel  = ingress;
    c.ingressStreamId = stream;
    c.egressChannel   = egress;
    c.egressStreamId  = stream;
    c.dataDir         = uniqueDir("inv_tr");
    c.driverMode      = DriverMode::Invoker;
    return c;
}

constexpr int kPumpCap = 200'000;

template <class Pred>
bool pumpUntil(Transport& t, Pred&& pred) {
    for (int i = 0; i < kPumpCap; ++i) {
        t.dutyCycle();
        if (pred()) return true;
    }
    return false;
}

} // namespace

// Regression: invoker mode has no background thread, so start() must pump its
// OWN driver + conductor while resolving; a sleep-until-found loop would wait
// for a thread that does not exist (spec §3.6).
TEST(InvokerTransport, StartResolvesWithoutBackgroundThreadsRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3100)};
    t.start();
    EXPECT_FALSE(t.usesDriverThread());
    EXPECT_EQ(t.callerPumpViolations(), 0);
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    t.stop();
}

// Proof that nothing but the owner's dutyCycle() moves data: UDP needs the
// driver's sender and receiver agents.
TEST(InvokerTransport, UdpLoopbackMovesOnlyWhenOwnerRunsDutyCycle) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24721";
    Transport t{invokerConfig(ch, ch, 3101)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    const uint8_t payload[] = {1, 2, 3, 4, 5, 6, 7, 8};
    ASSERT_TRUE(pumpUntil(t, [&] { return t.offer(payload, sizeof payload); }));

    int stolen = 0;
    for (int i = 0; i < 10'000; ++i) {
        stolen += t.poll([](const uint8_t*, std::size_t) {}, 10);
    }
    EXPECT_EQ(stolen, 0) << "a background thread moved UDP data in invoker mode";

    std::vector<uint8_t> echo;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.poll([&](const uint8_t* b, std::size_t n) { echo.assign(b, b + n); }, 10);
        return !echo.empty();
    }));
    EXPECT_EQ(echo, std::vector<uint8_t>(std::begin(payload), std::end(payload)));
    t.stop();
}

// Regression (Review Focus 1): an owner stall past the client-liveness timeout
// must close the Aeron client and report closed() — never ::exit(-1) the process
// (Aeron's default error handler) and never hang.  This is the one test that
// needs a real pause: Aeron's ClientConductor reads its own epoch clock
// (ClientConductor.h:436) and cannot be injected.
TEST(InvokerTransport, StalledOwnerSurfacesClosedInsteadOfExitingRegression) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3102);
    // The smallest liveness validate() accepts (1 s = 2x the client's 500 ms
    // keepalive).  Sleep 2.5x it: > liveness trips the client conductor's
    // inter-service check (strict '>' on a millisecond epoch clock) and the
    // driver's client-timeout scan (every liveness/10) with ample margin.
    c.clientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    Transport t{c};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(2'500));
    ASSERT_TRUE(pumpUntil(t, [&] { return t.closed(); }))
        << "inter-service timeout must close the client and be observable";
    const uint8_t b[] = {1};
    EXPECT_FALSE(t.offer(b, 1));
    t.stop();
}

TEST(InvokerTransport, ThreadModeIsUnchanged) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3103);
    c.driverMode = DriverMode::Thread;
    Transport t{c};
    t.start();
    EXPECT_TRUE(t.usesDriverThread());
    EXPECT_EQ(t.dutyCycle(), 0) << "thread mode: the driver thread owns the duty cycle";
    t.stop();
}

// Regression: validate() accepts any clientLivenessTimeoutNs > 0, but Aeron's
// driver rejects liveness <= timer_interval_ns (default 1 s) and
// publication_unblock_timeout_ns (default 15 s) <= liveness
// (aeron_driver_context.c:1322-1344).  The transport must derive those driver
// timers from the liveness so every validated config starts.
TEST(InvokerTransport, LongLivenessTimeoutStartsDriverRegression) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3104);
    c.clientLivenessTimeoutNs = 20'000'000'000LL;  // 20 s > Aeron's 15 s unblock default
    Transport t{c};
    ASSERT_NO_THROW(t.start());
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    t.stop();
}

// Regression (Task 6 review I2): the smallest liveness validate() accepts must
// start a real driver — the derived timer_interval must stay strictly below it
// (aeron_driver_context.c:1335 rejects liveness <= timer_interval).
TEST(InvokerTransport, MinimumAcceptedLivenessStartsDriverRegression) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3105);
    c.clientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    Transport t{c};
    ASSERT_NO_THROW(t.start());
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    EXPECT_FALSE(t.closed());
    t.stop();
}
