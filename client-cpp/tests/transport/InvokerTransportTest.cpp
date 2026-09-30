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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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

// Injected Resolution clock (single-threaded: the test thread is the owner).
int64_t g_nowNs = 0;
int64_t fakeNow() noexcept { return g_nowNs; }
// >= AeronTransport's 100 ms Resolution retry backoff.
constexpr int64_t kPastRetryBackoffNs = 200'000'000LL;

// Holds 127.0.0.1:port with a plain (non-SO_REUSEADDR) UDP socket so the
// driver's unicast bind of that port fails with EADDRINUSE: a real, transient
// driver rejection that ends when the socket is closed.
int holdUdpPort(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_port        = htons(port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
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

// Regression: offer(streamId) used to add a per-worker publication and SLEEP up to
// 5 s for the conductor.  In invoker mode nobody else runs the conductor, so the
// caller blocked for the full timeout.  It must return at once (back-pressure
// while the per-worker publication resolves — never the default publication, see
// ResolvingPerWorkerStreamBackPressuresNeverFallsBackRegression) and the handle
// must resolve through the owner's duty cycle.
TEST(InvokerTransport, PerStreamPublicationResolvesWithoutBlockingRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3200)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected(); }));
    const uint8_t b[] = {9};
    EXPECT_FALSE(t.offer(3201, b, 1)) << "unresolved per-worker stream back-pressures";
    const Transport::PubHandle h = t.publicationHandle("aeron:ipc", 3201);
    EXPECT_FALSE(t.publicationReady(h)) << "offer() must not have blocked until resolution";
    ASSERT_TRUE(pumpUntil(t, [&] { t.offerOn(h, b, 1); return t.publicationReady(h); }));
    EXPECT_EQ(t.publicationHandle("aeron:ipc", 3201), h) << "handles are stable and deduplicated";
    t.stop();
}

TEST(InvokerTransport, EgressSubscriptionResolvesAsynchronouslyAndDelivers) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3300)};
    t.start();
    t.addEgressSubscription("aeron:ipc", 3301);  // returns immediately
    const Transport::PubHandle h = t.publicationHandle("aeron:ipc", 3301);
    const uint8_t payload[] = {0xAB, 0xCD};
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.offerOn(h, payload, sizeof payload);
        t.poll([&](const uint8_t* p, std::size_t n) {
            if (n == sizeof payload) got.assign(p, p + n);
        }, 10);
        return !got.empty();
    }));
    EXPECT_EQ(got, std::vector<uint8_t>(std::begin(payload), std::end(payload)));
    t.stop();
}

TEST(InvokerTransport, OfferToUnknownEndpointNeverBlocks) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3400)};
    t.start();
    const uint8_t b[] = {1};
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(t.offerTo("aeron:udp?endpoint=127.0.0.1:24741", 3401, b, 1));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(500))
        << "offerTo must return while the publication resolves (was a 5 s sleep loop)";
    t.stop();
}

// Regression (Task 6 review carry-over): Aeron::connect() runs Aeron's own
// mapCncFile sleep loops (wait for cnc.dat, CnC-ready version, driver heartbeat;
// 10 s media-driver timeout).  In invoker mode nothing else can make the driver
// progress, so start() must bring the driver up — aeron_driver_init writes the
// CnC, signals it ready and stamps the to-driver heartbeat synchronously
// (aeron_driver.c:911-912) — BEFORE connect.  If that ordering ever regressed,
// connect would wait out the media-driver timeout and throw.
TEST(InvokerTransport, StartBringsDriverUpBeforeClientConnectRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3500)};
    const auto t0 = std::chrono::steady_clock::now();
    ASSERT_NO_THROW(t.start());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2))
        << "connect waited on a driver nobody was driving";
    EXPECT_FALSE(t.usesDriverThread());
    EXPECT_FALSE(t.closed());
    t.stop();
}

// Regression (Task 7 review I3, controller ruling): while a per-worker
// publication is still resolving, offer(streamId) used to fall back to the
// DEFAULT publication.  With maxInFlight > 1 one partition's consecutive batches
// then travel on two Aeron streams and can be processed out of order (ordering +
// idempotence break).  It must back-pressure (false, nothing sent) instead, and
// once Ready every frame arrives on the per-worker stream, in order.
TEST(InvokerTransport, ResolvingPerWorkerStreamBackPressuresNeverFallsBackRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3700)};  // default pub loops to egress
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    t.addEgressSubscription("aeron:ipc", 3701);
    std::vector<uint8_t> got;
    auto drain = [&] {
        t.poll([&](const uint8_t* p, std::size_t n) { if (n == 1) got.push_back(p[0]); }, 10);
    };
    // No duty cycle between offers: the conductor cannot answer, so the per-worker
    // publication stays Resolving for all of these.
    const uint8_t zero[] = {0};
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(t.offer(3701, zero, 1)) << "attempt " << i << " must back-pressure";
    }
    for (int i = 0; i < 2'000; ++i) {  // a fallback frame would loop back on 3700 here
        t.dutyCycle();
        drain();
    }
    EXPECT_TRUE(got.empty()) << "a frame leaked onto the default publication";

    constexpr uint8_t kFrames = 16;
    uint8_t next = 0;
    ASSERT_TRUE(pumpUntil(t, [&] {
        if (next < kFrames) {
            const uint8_t b[] = {next};
            if (t.offer(3701, b, 1)) ++next;
        }
        drain();
        return got.size() >= kFrames;
    }));
    std::vector<uint8_t> want(kFrames);
    for (uint8_t i = 0; i < kFrames; ++i) want[i] = i;
    EXPECT_EQ(got, want) << "exactly the offered frames, once each, in order";
    t.stop();
}

// Regression (Task 7 review I2): a driver rejection used to be terminal, so one
// transient error (here: the control port briefly held by another socket) left
// offerTo() failing until stop() and every batch hit produce timeout.  After the
// backoff (injected clock) the slot must register afresh and become Ready.
TEST(InvokerTransport, PublicationRecoversFromTransientRejectionRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3800)};
    t.start();
    g_nowNs = 1'000'000'000LL;
    t.setResolveClock(&fakeNow);
    const int fd = holdUdpPort(24761);
    ASSERT_GE(fd, 0);
    const std::string ch = "aeron:udp?control=127.0.0.1:24761|control-mode=dynamic";
    const Transport::PubHandle h = t.publicationHandle(ch, 3801);
    const uint8_t b[] = {1};
    bool anySent = false;
    for (int i = 0; i < 20'000; ++i) {  // port held: the driver rejects the bind
        t.dutyCycle();
        anySent |= t.offerOn(h, b, 1);
    }
    EXPECT_FALSE(anySent);
    EXPECT_FALSE(t.publicationReady(h)) << "control port held: must not resolve";
    ::close(fd);
    for (int i = 0; i < 2'000; ++i) {  // clock frozen: backoff not elapsed, stays down
        t.dutyCycle();
        t.offerOn(h, b, 1);
    }
    EXPECT_FALSE(t.publicationReady(h)) << "re-registration must wait for the backoff";
    g_nowNs += kPastRetryBackoffNs;
    EXPECT_TRUE(pumpUntil(t, [&] { t.offerOn(h, b, 1); return t.publicationReady(h); }))
        << "a transiently rejected publication must recover after the backoff";
    t.setResolveClock(nullptr);
    t.stop();
}

// Regression (Task 7 review I2): same for a per-worker egress subscription —
// addEgressSubscription() sees the existing (rejected) slot and returns, so the
// worker's egress used to be dead for good.
TEST(InvokerTransport, EgressSubscriptionRecoversFromTransientRejectionRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3900)};
    t.start();
    g_nowNs = 1'000'000'000LL;
    t.setResolveClock(&fakeNow);
    const int fd = holdUdpPort(24762);
    ASSERT_GE(fd, 0);
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24762";
    t.addEgressSubscription(ch, 3901);
    const Transport::PubHandle h = t.publicationHandle(ch, 3901);
    const uint8_t payload[] = {0x5A, 0xA5};
    std::vector<uint8_t> got;
    auto step = [&] {
        t.dutyCycle();
        t.offerOn(h, payload, sizeof payload);
        t.poll([&](const uint8_t* p, std::size_t n) {
            if (n == sizeof payload) got.assign(p, p + n);
        }, 10);
    };
    for (int i = 0; i < 20'000; ++i) step();  // endpoint held: subscription rejected
    EXPECT_TRUE(got.empty()) << "endpoint port held: nothing can be received";
    ::close(fd);
    t.addEgressSubscription(ch, 3901);  // the idempotent re-add the client issues
    for (int i = 0; i < 2'000; ++i) step();
    EXPECT_TRUE(got.empty()) << "re-registration must wait for the backoff";
    g_nowNs += kPastRetryBackoffNs;
    ASSERT_TRUE(pumpUntil(t, [&] { step(); return !got.empty(); }))
        << "a transiently rejected egress subscription must recover after the backoff";
    EXPECT_EQ(got, std::vector<uint8_t>(std::begin(payload), std::end(payload)));
    t.setResolveClock(nullptr);
    t.stop();
}

// The template poll keeps multi-fragment reassembly (IPC MTU 1408 → 20 KiB = 15 fragments).
TEST(InvokerTransport, FragmentedMessageStillReassemblesThroughTemplatePoll) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3600)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    std::vector<uint8_t> big(20 * 1024);
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<uint8_t>(i * 31);
    ASSERT_TRUE(pumpUntil(t, [&] { return t.offer(big.data(), big.size()); }));
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpUntil(t, [&] {
        t.poll([&](const uint8_t* b, std::size_t n) { got.assign(b, b + n); }, 32);
        return !got.empty();
    }));
    EXPECT_EQ(got, big);
    t.stop();
}
