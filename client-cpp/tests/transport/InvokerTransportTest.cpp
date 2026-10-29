// tests/transport/InvokerTransportTest.cpp — AeronTransport in DriverMode::Invoker.
// Every drive loop is bounded by a 10 s failure deadline and needs no sleep: the
// test thread IS the driver.
#include <filesystem>
#include <rune/client/transport/AeronTransport.h>
#include <rune/client/transport/ResponseChannel.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)

#include "../support/FreePort.h"
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <string>
#include <thread>
#include <arpa/inet.h>
#include <csignal>
#include <sys/wait.h>
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
    return std::string(std::filesystem::temp_directory_path().string() + "/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) +
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

// Real Aeron setup needs real progress, not a CPU-speed-dependent iteration
// count: loops run until pred holds; the 10 s deadline only ends a FAILING test.

template <class Pred>
bool pumpUntil(Transport& t, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
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
// (Aeron's default error handler) and never hang.  Aeron's ClientConductor reads
// its own epoch clock (Aeron.cpp:43 hard-wires currentTimeMillis) and cannot be
// injected, so the stall waits on THAT clock for THAT condition — no tuned sleep
// (F8c C9 fix round 1).
TEST(InvokerTransport, StalledOwnerSurfacesClosedInsteadOfExitingRegression) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3102);
    // The smallest liveness validate() accepts (1 s = 2x the client's 500 ms
    // keepalive).
    c.clientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    Transport t{c};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    // Every conductor doWork checks ClientConductor.h:436
    //   nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs   (nowMs = currentTimeMillis())
    // and t0 is read after pumpUntil's last doWork (t0 >= timeOfLastDoWorkMs), so
    // once currentTimeMillis() > t0 + liveness the next doWork must trip it.
    const long long livenessMs = c.clientLivenessTimeoutNs / 1'000'000;
    const long long t0 = aeron::currentTimeMillis();
    while (aeron::currentTimeMillis() <= t0 + livenessMs) std::this_thread::yield();
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

// ---------------------------------------------------------------------------
// F8c C0: one pump owner, enforced structurally (no atomic).  The driver /
// conductor of an invoker transport may be pumped by exactly one thread at a
// time; a duty thread that takes over after a happens-before handoff binds
// itself first (Client's runner does, at the top of runLoop()).
// ---------------------------------------------------------------------------

// A real second pumper — another thread calling dutyCycle() on a transport it
// does not own — is caught deterministically in debug builds (abort), every
// run, independent of timing.  RED on the parent: no owner check, no death.
TEST(InvokerTransportDeathTest, SecondPumperThreadAbortsRegression) {
#ifdef NDEBUG
    GTEST_SKIP() << "the pump-owner check is a debug-build assertion";
#else
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3120)};
            t.start();                                  // this thread owns the pump
            std::thread second([&t] { t.dutyCycle(); });  // never bound: a second pumper
            second.join();
        },
        "pumped by a second thread");
#endif
}

// The legitimate handoff: the owner starts the transport, a duty thread binds
// itself and pumps it to ready, the owner joins it and stops (stop() rebinds
// the caller for the final drain).  No violation, no abort.
TEST(InvokerTransport, PumpOwnershipHandoffViaBindRegression) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3121)};
    t.start();
    bool connected = false;
    std::thread duty([&] {
        t.bindPumpThread();
        connected = pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); });
    });
    duty.join();
    EXPECT_TRUE(connected);
    t.stop();
    EXPECT_EQ(t.callerPumpViolations(), 0);
}

// ---------------------------------------------------------------------------
// Backlog B1 (LAT-1 h): CRC on accept.  A Produce frame handed over with its
// RecordBatch CRC unset is sealed by the transport only once Aeron accepted
// it — inside tryClaim (<= one MTU payload) or inside the offer's
// reserved-value supplier (multi-fragment) — into the term AND the source.
// ---------------------------------------------------------------------------
#include <rune/client/producer/ProduceFrameBuilder.h>
#include <rune/client/transport/FrameCrc.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
namespace {

using rune::client::producer::ProduceFrameBuilder;

// An unsealed Produce frame with `records` records of `valueBytes` bytes.
ProduceFrameBuilder unsealedFrame(int records, std::size_t valueBytes) {
    ProduceFrameBuilder fb(0, 1'000);
    for (int i = 0; i < records; ++i) {
        fb.append("k" + std::to_string(i), std::string(valueBytes, static_cast<char>('a' + i % 26)), 1'000 + i);
    }
    fb.setProducerId(77);
    fb.setProducerEpoch(3);
    fb.setBaseSequence(9);
    rune::protocol::client::Produce h;
    h.sessionId = 5; h.correlationId = 6; h.topicId = 1; h.partitionKnown = 1;
    h.producerId = 77; h.producerEpoch = 3; h.baseSequence = 9;
    (void)fb.seal(h);
    return fb;
}

uint32_t storedCrc(const uint8_t* frame) {
    return rune::protocol::detail::getLE32(frame + ProduceFrameBuilder::frameCrc().crcAt);
}

bool batchValid(const uint8_t* frame, std::size_t len) {
    const auto p = rune::protocol::client::Produce::decode(frame, len);
    if (!p) return false;
    rune::protocol::RecordBatchReader rd(p->recordBatch.data(), p->recordBatch.size());
    return rd.ensureValid();
}

}  // namespace

TEST(InvokerTransportCrc, SealsOnAcceptInTheClaimAndTheMultiFragmentPaths) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24761";  // default MTU 1408
    Transport t{invokerConfig(ch, ch, 3701)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    for (const auto [records, value] : {std::pair{2, std::size_t{200}},       // one claim
                                        std::pair{40, std::size_t{1024}}}) {  // ~30 fragments
        ProduceFrameBuilder fb = unsealedFrame(records, value);
        ASSERT_EQ(storedCrc(fb.data()), 0u);
        ASSERT_TRUE(pumpUntil(t, [&] { return t.offer(fb.data(), fb.size(), ProduceFrameBuilder::frameCrc()); }));
        // The source (the producer's retained frame) is sealed too.
        EXPECT_TRUE(batchValid(fb.data(), fb.size())) << "records=" << records;
        std::vector<uint8_t> got;
        ASSERT_TRUE(pumpUntil(t, [&] {
            t.poll([&](const uint8_t* b, std::size_t n) { got.assign(b, b + n); }, 64);
            return !got.empty();
        }));
        EXPECT_EQ(got, std::vector<uint8_t>(fb.data(), fb.data() + fb.size())) << "records=" << records;
        EXPECT_TRUE(batchValid(got.data(), got.size())) << "records=" << records;
    }
    t.stop();
}

TEST(InvokerTransportCrc, RefusedOfferNeverComputesTheCrc) {
    // Publisher on an endpoint nobody listens on yet: every offer is refused
    // (NOT_CONNECTED) by Aeron's position < limit check — the CRC must stay
    // unset.  Then a receiver appears and the same frames are accepted + sealed.
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24763";
    Transport pub{invokerConfig(ch, "aeron:ipc", 3702)};
    pub.start();
    ProduceFrameBuilder small = unsealedFrame(2, 200);
    ProduceFrameBuilder big   = unsealedFrame(40, 1024);
    for (int i = 0; i < 200; ++i) {
        pub.dutyCycle();
        ASSERT_FALSE(pub.offer(small.data(), small.size(), ProduceFrameBuilder::frameCrc()));
        ASSERT_FALSE(pub.offer(big.data(), big.size(), ProduceFrameBuilder::frameCrc()));
    }
    EXPECT_EQ(storedCrc(small.data()), 0u) << "a refused claim computed the CRC";
    EXPECT_EQ(storedCrc(big.data()), 0u) << "a refused multi-fragment offer computed the CRC";

    Config rc = invokerConfig("aeron:ipc", ch, 3702);
    rc.dataDir = uniqueDir("inv_tr_rx");
    Transport rx{rc};
    rx.start();
    auto both = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            pub.dutyCycle();
            rx.dutyCycle();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(both([&] { return pub.offer(small.data(), small.size(), ProduceFrameBuilder::frameCrc()); }));
    ASSERT_TRUE(both([&] { return pub.offer(big.data(), big.size(), ProduceFrameBuilder::frameCrc()); }));
    EXPECT_TRUE(batchValid(small.data(), small.size()));
    EXPECT_TRUE(batchValid(big.data(), big.size()));
    int got = 0;
    ASSERT_TRUE(both([&] {
        rx.poll([&](const uint8_t* b, std::size_t n) { got += batchValid(b, n) ? 1 : 100; }, 64);
        return got >= 2;
    }));
    EXPECT_EQ(got, 2);
    rx.stop();
    pub.stop();
}

// ---------------------------------------------------------------------------
// A7 park-until-work (rune-broker findings §4.12): an INVOKER transport exposes its driver's
// transport-poller epoll fds so the owner can park in ppoll() until a datagram arrives, instead of
// polling every socket each pass (a pegged core).  Linux only; a Thread-mode driver exposes none.
// ---------------------------------------------------------------------------
#include <poll.h>

TEST(InvokerTransportWake, ThreadModeExposesNoWakeFds) {
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3190);
    c.driverMode = DriverMode::Thread;
    Transport t{c};
    t.start();
    int fds[4];
    EXPECT_EQ(t.wakeFds(fds, 4), 0) << "the driver's own thread consumes the sockets";
    t.stop();
}

// A7 re-review LOW-2: coverage (the wake worked before the C1 fix) — not a regression test.
TEST(InvokerTransportWake, ParkedOwnerWakesOnInboundDatagram) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only (Aeron's transport pollers use epoll there)";
#else
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24731";
    Transport t{invokerConfig(ch, ch, 3191)};
    t.start();
    ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
    for (int i = 0; i < 200; ++i) t.dutyCycle();   // settle setup / status traffic
    int fds[4];
    const int n = t.wakeFds(fds, 4);
    ASSERT_EQ(n, 2) << "receiver + sender transport pollers";
    pollfd pfds[4];
    for (int i = 0; i < n; ++i) pfds[i] = pollfd{fds[i], POLLIN, 0};

    // Quiet: nothing readable, a park waits out its timeout.
    timespec quiet{0, 20'000'000};
    EXPECT_EQ(::ppoll(pfds, static_cast<nfds_t>(n), &quiet, nullptr), 0);

    // A datagram on the driver's socket (a plain sender: not an Aeron frame, the receiver drops it)
    // ends a 2 s park immediately.
    const int s = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET; a.sin_port = htons(24731); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const char junk[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    ASSERT_EQ(::sendto(s, junk, sizeof junk, 0, reinterpret_cast<sockaddr*>(&a), sizeof a), 8);
    ::close(s);
    const timespec ts{2, 0};
    const auto t0 = std::chrono::steady_clock::now();
    const int r = ::ppoll(pfds, static_cast<nfds_t>(n), &ts, nullptr);
    const auto waitedMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_GT(r, 0) << "the inbound datagram must make a wake fd readable";
    EXPECT_LT(waitedMs, 1'000);

    // The owner's pass drains it: quiet again (level-triggered, no sticky wake).
    for (int i = 0; i < 10; ++i) t.dutyCycle();
    EXPECT_EQ(::ppoll(pfds, static_cast<nfds_t>(n), &quiet, nullptr), 0);
    t.stop();
    EXPECT_EQ(t.wakeFds(fds, 4), 0) << "no fds once the driver is closed";
#endif
}

// A7 epoll-first polling: installed by default on Linux (off elsewhere / on RUNE_CLIENT_AERON_EPOLL_POLL=0),
// and a driver whose receiver poller holds > 1 UDP transport still delivers everything.
TEST(InvokerTransportEpollPoll, InstalledWhereSupportedAndConfigured) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:24741";
    Transport t{invokerConfig(ch, ch, 3192)};
    t.start();
#if defined(__linux__)
    EXPECT_TRUE(t.epollPollActive());
#else
    EXPECT_FALSE(t.epollPollActive());
#endif
    t.stop();
    Config off = invokerConfig(ch, ch, 3193);
    off.epollPoll = false;
    Transport s{off};
    s.start();
    EXPECT_FALSE(s.epollPollActive()) << "the A/B off arm: Aeron's stock poller";
    s.stop();
}

// A7 review L2: epoll-first polling reads Aeron internals — pinned to the verified release.  An Aeron upgrade
// fails HERE (re-verify rune_driver_wake.c against the new poller / bindings layout, then move the pin); at
// runtime any other release keeps the stock polling.
TEST(InvokerTransportEpollPoll, AeronIsTheReleaseEpollFirstWasVerifiedAgainst) {
    EXPECT_TRUE(Transport::epollPollSupportsAeron(aeron_version_major(), aeron_version_minor(), aeron_version_patch()))
        << "runtime Aeron " << aeron_version_full() << " is not the epoll-first pin";
    EXPECT_FALSE(Transport::epollPollSupportsAeron(1, 42, 0));
    EXPECT_FALSE(Transport::epollPollSupportsAeron(1, 41, 5));
    EXPECT_FALSE(Transport::epollPollSupportsAeron(2, 41, 4));
}

// A7 review C1: publications to a peer that is down (closed port, host up) get ICMP port-unreachable on their
// CONNECTED send sockets; the kernel parks the error (EPOLLERR without EPOLLIN).  Epoll-first polling must consume
// it in the pass, or the owner parked on wakeFds() wakes at once forever (100% CPU while a broker node is down).
// Linux only.
namespace {
template <class Pred>
bool pumpFor(Transport& t, std::chrono::milliseconds limit, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        t.dutyCycle();
        if (pred()) return true;
    }
    return false;
}
} // namespace

TEST(InvokerTransportWakeRegression, PeerDownNeverLeavesWakeFdsReadyAfterAnIdlePassRegression) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only (Aeron's transport pollers use epoll there)";
#else
    const std::string down1 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::client::testing::allocFreeUdpPort());
    const std::string down2 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::client::testing::allocFreeUdpPort());
    Transport t{invokerConfig(down1, "aeron:ipc", 3194)};
    t.start();
    const Transport::PubHandle h = t.publicationHandle(down2, 3195);   // a 2nd send socket: the epoll-first branch
    const uint8_t one[1] = {1};
    ASSERT_TRUE(pumpUntil(t, [&] { (void)t.offerOn(h, one, 1); return t.publicationReady(h); }));   // resolves lazily
    int fds[4];
    const int n = t.wakeFds(fds, 4);
    ASSERT_EQ(n, 2);
    auto wakeReady = [&] {
        pollfd pf[4];
        for (int i = 0; i < n; ++i) pf[i] = pollfd{fds[i], POLLIN, 0};
        const timespec zero{0, 0};
        return ::ppoll(pf, static_cast<nfds_t>(n), &zero, nullptr) > 0;
    };
    // A7 re-review LOW-1: the positive control — an ICMP error was actually delivered at least once.  Without it
    // the quiet-park assertions below would pass vacuously if setup frames / ICMP stopped (an Aeron upgrade, a
    // sysctl, a route).  Two witnesses: a wake fd readable between passes (the error pending), or an epoll_wait of
    // the SENDER poller that selected a socket (epollSelectedForTest: these send-only sockets have no peer, so a
    // selection is an EPOLLERR the pass consumed).  The second does not depend on catching the error between two
    // sub-microsecond passes — the next pass's poll usually consumes it first (seen in a Linux container).
    (void)t.epollSelectedForTest(/*reset=*/true);
    int pendingSeen = 0;
    for (int round = 0; round < 3; ++round) {
        // an ICMP error pending, or 350 ms (>= 3 setup frames per publication)
        if (pumpFor(t, std::chrono::milliseconds(350), wakeReady)) ++pendingSeen;
        ASSERT_TRUE(pumpFor(t, std::chrono::seconds(5), [&] { return t.dutyCycle() == 0; }));
        pollfd pf[4];
        for (int i = 0; i < n; ++i) pf[i] = pollfd{fds[i], POLLIN, 0};
        const timespec park{0, 30'000'000};
        EXPECT_EQ(::ppoll(pf, static_cast<nfds_t>(n), &park, nullptr), 0)
            << "round " << round << ": wake fd left ready by an unconsumed EPOLLERR";
    }
    const int errorsConsumed = t.epollSelectedForTest(/*reset=*/false);
    t.stop();
    // Some network namespaces never deliver the port-unreachable error to the sender — that environment cannot
    // exercise the path, so it SKIPS (loudly) instead of passing vacuously.
    if (pendingSeen == 0 && errorsConsumed == 0)
        GTEST_SKIP() << "no ICMP port-unreachable error was ever pending in this network namespace — "
                        "the peer-down path cannot be exercised here";
#endif
}

// Review M2: a client driver whose receiver poller holds > 1 UDP transport (the egress subscription plus two
// more) delivers everything AND the epoll-first branch demonstrably ran (an epoll_wait selected a socket); the
// stock arm delivers the same without it.
// A7 re-review LOW-2: coverage (delivery worked before the fix) — not a regression test.
TEST(InvokerTransportEpollPoll, MultiTransportReceiverDeliversThroughEpollFirst) {
    for (bool epoll : {true, false}) {
        const uint16_t p0 = rune::client::testing::allocFreeUdpPort(), p1 = rune::client::testing::allocFreeUdpPort(), p2 = rune::client::testing::allocFreeUdpPort();
        const std::string c0 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p0);
        const std::string c1 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p1);
        const std::string c2 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p2);
        Config cfg = invokerConfig(c0, c0, 3196);
        cfg.epollPoll = epoll;
        Transport t{cfg};
        t.start();
        t.addEgressSubscription(c1, 3196);
        t.addEgressSubscription(c2, 3196);
        ASSERT_TRUE(pumpUntil(t, [&] { return t.ingressConnected() && t.egressConnected(); }));
        const Transport::PubHandle h1 = t.publicationHandle(c1, 3196);
        const Transport::PubHandle h2 = t.publicationHandle(c2, 3196);
        // Publications resolve lazily in offerOn, the extra egress subscriptions inside poll(): offer and poll
        // together (an offer to a lane that is not connected yet back-pressures and is retried).
        for (int i = 0; i < 200; ++i) t.dutyCycle();   // setup / status traffic settles
        (void)t.epollSelectedForTest(/*reset=*/true);
        constexpr int kPer = 30;
        const uint8_t b[16] = {7};
        int sent = 0, got = 0;
        EXPECT_TRUE(pumpUntil(t, [&] {
            if (sent < kPer) { if (t.offer(b, sizeof b)) ++sent; }
            else if (sent < 2 * kPer) { if (t.offerOn(h1, b, sizeof b)) ++sent; }
            else if (sent < 3 * kPer) { if (t.offerOn(h2, b, sizeof b)) ++sent; }
            got += t.poll([](const uint8_t*, std::size_t) {}, 64);
            return sent == 3 * kPer && got >= 3 * kPer;
        })) << "epoll=" << epoll << " sent " << sent << " got " << got;
        EXPECT_EQ(got, 3 * kPer) << "epoll=" << epoll;
#if defined(__linux__)
        if (epoll) EXPECT_GT(t.epollSelectedForTest(false), 0) << "the epoll-first branch ran";
        else       EXPECT_EQ(t.epollSelectedForTest(false), 0) << "stock <= 5 path only";
#endif
        t.stop();
    }
}

// #654: a dir is owned through an exclusive lock, not judged from Aeron's 1 s heartbeat.
// `a` is never pumped after start(), so its heartbeat may be arbitrarily stale; it is still live.
TEST(InvokerTransportDirLockRegression, ALiveDriverDirIsRefusedWhateverItsHeartbeatAgeRegression) {
    Config cfg = invokerConfig("aeron:ipc", "aeron:ipc", 3200);
    Transport a{cfg};
    a.start();
    Transport b{cfg};
    EXPECT_THROW(b.start(), std::runtime_error);
    b.stop();
    a.stop();
}

TEST(InvokerTransportDirLockRegression, ADirIsReusableRightAfterItsHolderIsKilledRegression) {
    const Config cfg = invokerConfig("aeron:ipc", "aeron:ipc", 3201);
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::close(fds[0]);
        try {
            Transport t{cfg};
            t.start();
            const char byte = 1;
            if (::write(fds[1], &byte, 1) != 1) ::_exit(2);
            for (;;) ::pause();
        } catch (...) {
            ::_exit(3);
        }
    }
    ::close(fds[1]);
    char byte = 0;
    ASSERT_EQ(::read(fds[0], &byte, 1), 1) << "the child never got its driver up";
    ::close(fds[0]);
    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    // The killed holder's dir (cnc.dat, fresh heartbeat) is still on disk; the kernel dropped its lock.
    Transport b{cfg};
    EXPECT_NO_THROW(b.start());
    b.stop();
}

TEST(InvokerTransportDirLockRegression, ADirIsReusableAfterItsHolderIsStoppedRegression) {
    const Config cfg = invokerConfig("aeron:ipc", "aeron:ipc", 3202);
    {
        Transport a{cfg};
        a.start();
        a.stop();
    }
    Transport b{cfg};
    EXPECT_NO_THROW(b.start());
    b.stop();
}

// #654 review: a live driver that holds no lock (a pre-lock client or broker, a stock aeronmd, a raw C-API driver)
// is still refused, from the pid in its cnc.dat; its dir is never wiped.
TEST(InvokerTransportDirLockRegression, ALiveDriverThatHoldsNoLockIsRefusedRegression) {
    const Config cfg = invokerConfig("aeron:ipc", "aeron:ipc", 3203);
    const std::string aeronDir = (std::filesystem::path{cfg.dataDir} / "aeron").string();
    std::filesystem::create_directories(cfg.dataDir);
    aeron_driver_context_t* ctx = nullptr;
    aeron_driver_t* raw = nullptr;
    ASSERT_EQ(aeron_driver_context_init(&ctx), 0);
    aeron_driver_context_set_dir(ctx, aeronDir.c_str());
    aeron_driver_context_set_threading_mode(ctx, AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_term_buffer_length(ctx, 64 * 1024);
    ASSERT_EQ(aeron_driver_init(&raw, ctx), 0);
    ASSERT_EQ(aeron_driver_start(raw, /*manual_main_loop=*/true), 0);
    for (int i = 0; i < 16; ++i) aeron_driver_main_do_work(raw);
    ASSERT_TRUE(aeron_is_driver_active(aeronDir.c_str(), 10000, [](const char*) {}));
    {
        Transport b{cfg};
        EXPECT_THROW(b.start(), std::runtime_error) << "a second driver started on a live lock-less driver's dir";
        b.stop();
    }
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path{aeronDir} / "cnc.dat")) << "the live driver's dir was wiped";
    aeron_driver_close(raw);
    aeron_driver_context_close(ctx);   // aeron_driver_close leaves the context to its owner
    std::error_code ec;
    std::filesystem::remove_all(cfg.dataDir, ec);
}

// #654 review: the lock is keyed by the canonical dir, so "dir/." and a symlink alias share the live holder's lock.
TEST(InvokerTransportDirLockRegression, AnotherSpellingOfTheSameDirIsRefusedRegression) {
    const Config cfg = invokerConfig("aeron:ipc", "aeron:ipc", 3204);
    Transport a{cfg};
    a.start();
    const std::filesystem::path cnc = std::filesystem::path{cfg.dataDir} / "aeron" / "cnc.dat";
    ASSERT_TRUE(std::filesystem::exists(cnc));
    // The lock sits next to the aeron dir, so the aliasing spelling is a data dir whose "aeron" IS a symlink to the
    // live driver's dir (its "aeron.lock" would be a different file without canonicalisation).
    const std::string link = uniqueDir("inv_tr_link");
    std::filesystem::create_directories(link);
    std::filesystem::create_directory_symlink(std::filesystem::path{cfg.dataDir} / "aeron", std::filesystem::path{link} / "aeron");
    for (const std::string& alias : {cfg.dataDir + "/.", link}) {
        Config c = cfg;
        c.dataDir = alias;
        Transport b{c};
        // The refusal must come from the SHARED LOCK (not from Aeron's own 1 s heartbeat check, which `a` -- never
        // pumped after start() -- would fail once stale).
        try {
            b.start();
            ADD_FAILURE() << "no refusal for " << alias;
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("is held by a live driver"), std::string::npos) << alias << ": " << e.what();
        }
        b.stop();
        EXPECT_TRUE(std::filesystem::exists(cnc)) << "starting on '" << alias << "' wiped the live driver's dir";
    }
    a.stop();
    std::error_code ec;
    std::filesystem::remove_all(link, ec);
    std::filesystem::remove(link + "/aeron.lock", ec);
    std::filesystem::remove_all(cfg.dataDir, ec);
}

// ===========================================================================
// NAT-safe egress (Task 7): Aeron response channels.  The client opens a response subscription
// (control=<server response-control>|control-mode=response) BEFORE the request publication, which names the
// subscription's registration id.  A raw Aeron "server" (its own client on the transport's embedded driver)
// plays the broker: it answers on a response publication correlated with the request IMAGE.
// ===========================================================================
namespace {

std::string loopback(uint16_t port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

Config responseConfig(const std::string& ingress, uint16_t responsePort, int32_t stream) {
    Config c = invokerConfig(ingress, loopback(rune::client::testing::allocFreeUdpPort()), stream);
    c.gatewayResponseControlPort = responsePort;
    return c;
}

// The raw broker: a subscription on a request endpoint, and a response publication per request image.
struct RawServer {
    std::shared_ptr<aeron::Aeron>                aeron;
    std::shared_ptr<aeron::Subscription>         sub;
    std::shared_ptr<aeron::ExclusivePublication> resp;
    int64_t subReg  = -1;
    int64_t respReg = -1;
    std::string ctl;       // "127.0.0.1:<response-control port>"
    int32_t     respStream = 0;
    std::vector<std::vector<uint8_t>> received;   // frames that reached the request subscription

    RawServer(const std::string& clientDataDir, const std::string& requestChannel, int32_t requestStream,
              uint16_t responsePort, int32_t respStreamId)
        : ctl("127.0.0.1:" + std::to_string(responsePort)), respStream(respStreamId) {
        aeron::Context ctx;
        ctx.aeronDir(clientDataDir + "/aeron");
        ctx.useConductorAgentInvoker(true);
        ctx.errorHandler([](const std::exception&) {});
        aeron  = aeron::Aeron::connect(ctx);
        subReg = aeron->addSubscription(requestChannel, requestStream);
    }
    ~RawServer() { resp.reset(); sub.reset(); aeron.reset(); }

    // One duty cycle of the server's own client: resolve, read requests, open the response publication on the image.
    void pump() {
        aeron->conductorAgentInvoker().invoke();
        if (!sub) sub = aeron->findSubscription(subReg);
        if (sub) {
            sub->poll([this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                             aeron::Header&) {
                received.emplace_back(b.buffer() + off, b.buffer() + off + len);
            }, 16);
            if (respReg < 0 && sub->imageCount() > 0) {
                const auto image = sub->imageByIndex(0);
                respReg = aeron->addExclusivePublication(
                    "aeron:udp?control=" + ctl + "|control-mode=response|response-correlation-id=" +
                        std::to_string(image->correlationId()),
                    respStream);
            }
        }
        if (respReg >= 0 && !resp) resp = aeron->findExclusivePublication(respReg);
    }
    bool respond(const std::vector<uint8_t>& f) {
        if (!resp) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()), static_cast<aeron::util::index_t>(f.size()));
        return resp->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }
};

template <class Pred>
bool pumpBoth(Transport& t, RawServer& srv, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        t.dutyCycle();
        srv.pump();
        if (pred()) return true;
    }
    return false;
}

} // namespace

TEST(InvokerTransportResponseRegression, SeedPublicationIsCorrelatedWithItsResponseSubscription) {
    const uint16_t reqPort = rune::client::testing::allocFreeUdpPort();
    const uint16_t rPort   = rune::client::testing::allocFreeUdpPort();
    Transport t{responseConfig(loopback(reqPort), rPort, 3500)};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    const int64_t regId = t.seedResponseSubscriptionIdForTest();
    ASSERT_NE(regId, 0);
    // The request publication names exactly the response subscription's registration id, the plain channel stays the identity.
    EXPECT_EQ(t.seedPublicationChannelForTest(),
              loopback(reqPort) + "|response-correlation-id=" + std::to_string(regId));
    EXPECT_EQ(t.ingressChannel(), loopback(reqPort));
    t.stop();
}

TEST(InvokerTransportResponseRegression, NoResponseSubscriptionForIpcOrPortZero) {
    {   // ipc gateway, a port configured: nothing to pair
        Transport t{responseConfig("aeron:ipc", 40163, 3501)};
        t.start();
        EXPECT_FALSE(t.seedResponseCorrelated());
        EXPECT_EQ(t.seedResponseSubscriptionIdForTest(), 0);
        EXPECT_EQ(t.seedPublicationChannelForTest(), "aeron:ipc");
        t.stop();
    }
    {   // udp gateway, port 0 = the declared path only
        const std::string ch = loopback(rune::client::testing::allocFreeUdpPort());
        Transport t{responseConfig(ch, 0, 3502)};
        t.start();
        EXPECT_FALSE(t.seedResponseCorrelated());
        EXPECT_EQ(t.seedResponseSubscriptionIdForTest(), 0);
        EXPECT_EQ(t.seedPublicationChannelForTest(), ch);
        t.stop();
    }
}

TEST(InvokerTransportResponseRegression, RealAeronResponseReachesTheClientOverItsOwnFlow) {
    const uint16_t reqPort = rune::client::testing::allocFreeUdpPort();
    const uint16_t rPort   = rune::client::testing::allocFreeUdpPort();
    Config c = responseConfig(loopback(reqPort), rPort, 3510);
    Transport t{c};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    RawServer srv(c.dataDir, loopback(reqPort), c.ingressStreamId, rPort, c.egressStreamId);

    const uint8_t request[] = {1, 2, 3, 4, 5};
    ASSERT_TRUE(pumpBoth(t, srv, [&] { return t.offer(request, sizeof request); }));
    ASSERT_TRUE(pumpBoth(t, srv, [&] { return srv.received.size() == 1; }));
    EXPECT_EQ(srv.received[0], std::vector<uint8_t>(std::begin(request), std::end(request)));

    // The server answers on a response publication correlated with the request image: the client's poll gets it
    // byte-equal, although its DECLARED egress (never served here) is not involved.
    const std::vector<uint8_t> reply = {9, 8, 7, 6, 5, 4, 3, 2, 1};
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpBoth(t, srv, [&] {
        if (got.empty()) (void)srv.respond(reply);
        t.poll([&](const uint8_t* b, std::size_t n) { if (got.empty()) got.assign(b, b + n); }, 10);
        return !got.empty();
    }));
    EXPECT_EQ(got, reply);
    t.stop();
}

TEST(InvokerTransportResponseRegression, ResponsePublicationHandleIsNotReadyUntilBothResolveThenCarriesBothDirections) {
    const uint16_t reqPort = rune::client::testing::allocFreeUdpPort();   // the "worker" ingress
    const uint16_t rPort   = rune::client::testing::allocFreeUdpPort();   // the worker's response-control port
    Config c = invokerConfig("aeron:ipc", "aeron:ipc", 3520);
    Transport t{c};
    t.start();
    const std::string worker = loopback(reqPort);
    const auto respCtl = rune::client::transport::responseControlChannelFor(worker, rPort);
    ASSERT_TRUE(respCtl.has_value());
    const Transport::PubHandle h = t.responsePublicationHandle(worker, 3521, *respCtl);
    ASSERT_NE(h, Transport::kNoPub);
    EXPECT_EQ(t.responsePublicationHandle(worker, 3521, *respCtl), h) << "one slot per (channel, stream)";
    EXPECT_FALSE(t.publicationResponseCorrelated(h)) << "phase 1: still awaiting the response subscription";
    RawServer srv(c.dataDir, worker, 3521, rPort, c.egressStreamId);

    const uint8_t bind[] = {0xB1, 0xD0};
    EXPECT_FALSE(t.offerOn(h, bind, sizeof bind)) << "never ready on the first call: the response subscription resolves first";
    EXPECT_FALSE(t.publicationReady(h));
    bool sent = false;
    ASSERT_TRUE(pumpBoth(t, srv, [&] {
        if (!sent) sent = t.offerOn(h, bind, sizeof bind);
        return sent;
    }));
    EXPECT_TRUE(t.publicationResponseCorrelated(h));
    ASSERT_TRUE(pumpBoth(t, srv, [&] { return srv.received.size() == 1; }));
    EXPECT_EQ(srv.received[0], std::vector<uint8_t>(std::begin(bind), std::end(bind)));

    const std::vector<uint8_t> ack = {0xAC, 0x4B};
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpBoth(t, srv, [&] {
        if (got.empty()) (void)srv.respond(ack);
        t.poll([&](const uint8_t* b, std::size_t n) { if (got.empty()) got.assign(b, b + n); }, 10);
        return !got.empty();
    }));
    EXPECT_EQ(got, ack) << "the worker's reply arrives on the response subscription";
    t.stop();
}

TEST(InvokerTransportResponseRegression, ResponseHandleForAnExistingPlainKeyReturnsThePlainSlot) {
    Transport t{invokerConfig("aeron:ipc", "aeron:ipc", 3530)};
    t.start();
    const std::string worker = loopback(rune::client::testing::allocFreeUdpPort());
    const Transport::PubHandle plain = t.publicationHandle(worker, 3531);
    const auto respCtl = rune::client::transport::responseControlChannelFor(worker, 40999);
    ASSERT_TRUE(respCtl.has_value());
    EXPECT_EQ(t.responsePublicationHandle(worker, 3531, *respCtl), plain) << "never both a plain and a correlated slot";
    EXPECT_FALSE(t.publicationResponseCorrelated(plain));
    // An empty response channel is simply the plain handle.
    EXPECT_EQ(t.responsePublicationHandle(worker, 3532, {}), t.publicationHandle(worker, 3532));
    t.stop();
}

// I1 (fix round 1a) revisited (1b): Aeron pins a response subscription to the FIRST response publication session that
// reaches it, so two request publications can never share one: the seed pair and a correlated publication to the SAME
// endpoint each own their response subscription; retargeting the seed away releases only the seed's.
TEST(AeronTransportRegression, SeedAndLeaderPairsOwnTheirOwnResponseSubscriptionRegression) {
    const uint16_t reqA = rune::client::testing::allocFreeUdpPort(), rA = rune::client::testing::allocFreeUdpPort();
    const uint16_t reqB = rune::client::testing::allocFreeUdpPort(), rB = rune::client::testing::allocFreeUdpPort();
    Config c = responseConfig(loopback(reqA), rA, 3550);
    Transport t{c};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    const int32_t slot = t.seedResponseSlotForTest();
    ASSERT_GE(slot, 0);
    EXPECT_EQ(t.subSlotOwnersForTest(slot), 1u);

    // A second owner: a leader session's correlated control publication to the SAME gateway.
    const std::string gwA = loopback(reqA);
    const auto respCtl = rune::client::transport::responseControlChannelFor(gwA, rA);
    ASSERT_TRUE(respCtl.has_value());
    const Transport::PubHandle h = t.responsePublicationHandle(gwA, 3551, *respCtl);
    ASSERT_NE(h, Transport::kNoPub);
    EXPECT_EQ(t.subSlotOwnersForTest(slot), 1u) << "the seed's response subscription is never shared (one-shot per pair)";

    // The seed fails over to B: only the seed's share is released.
    ASSERT_TRUE(t.retarget(loopback(reqB), rB));
    for (int i = 0; i < 5; ++i) { t.dutyCycle(); t.poll([](const uint8_t*, std::size_t) {}, 10); }   // reap pass
    EXPECT_FALSE(t.subSlotLiveForTest(slot)) << "the seed's own subscription is released with its pair";

    // ... and the second owner's publication still offers and its reply still arrives.
    RawServer srv(c.dataDir, gwA, 3551, rA, c.egressStreamId);
    const uint8_t frame[] = {0xC1, 0x01};
    bool sent = false;
    ASSERT_TRUE(pumpBoth(t, srv, [&] {
        if (!sent) sent = t.offerOn(h, frame, sizeof frame);
        return sent;
    }));
    ASSERT_TRUE(pumpBoth(t, srv, [&] { return srv.received.size() == 1; }));
    const std::vector<uint8_t> reply = {0xAC, 0x01};
    std::vector<uint8_t> got;
    ASSERT_TRUE(pumpBoth(t, srv, [&] {
        if (got.empty()) (void)srv.respond(reply);
        t.poll([&](const uint8_t* b, std::size_t n) { if (got.empty()) got.assign(b, b + n); }, 10);
        return !got.empty();
    }));
    EXPECT_EQ(got, reply);
    t.stop();
}

TEST(InvokerTransportResponseRegression, RetargetPairsTheNewResponseSubscriptionAndFallsBackToPlain) {
    const uint16_t reqA = rune::client::testing::allocFreeUdpPort(), rA = rune::client::testing::allocFreeUdpPort();
    const uint16_t reqB = rune::client::testing::allocFreeUdpPort(), rB = rune::client::testing::allocFreeUdpPort();
    Transport t{responseConfig(loopback(reqA), rA, 3540)};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    const int64_t regA = t.seedResponseSubscriptionIdForTest();

    ASSERT_TRUE(t.retarget(loopback(reqB), rB));
    EXPECT_TRUE(t.seedResponseCorrelated());
    const int64_t regB = t.seedResponseSubscriptionIdForTest();
    EXPECT_NE(regB, 0);
    EXPECT_NE(regB, regA) << "a new response subscription for the new gateway";
    EXPECT_EQ(t.seedPublicationChannelForTest(), loopback(reqB) + "|response-correlation-id=" + std::to_string(regB));
    EXPECT_EQ(t.ingressChannel(), loopback(reqB));

    // Port 0 / a redirect without a port: back on the plain (declared) path.
    ASSERT_TRUE(t.retarget(loopback(reqA)));
    EXPECT_FALSE(t.seedResponseCorrelated());
    EXPECT_EQ(t.seedPublicationChannelForTest(), loopback(reqA));
    // The retired response subscription is released at the next poll (never inside a callback).
    for (int i = 0; i < 5; ++i) { t.dutyCycle(); t.poll([](const uint8_t*, std::size_t) {}, 10); }
    t.stop();
}

// ---- I-A: the seed re-pair is asynchronous and touches only the seed pair -------------------------------------------

// rePairSeedResponse only registers the new response subscription: it never pumps the conductor, never sleeps.  The new
// pair is built by the following offer()/duty cycles; the swap retires the old subscription.
TEST(SeedRePairRegression, RePairNeverPumpsRegression) {
    const uint16_t reqPort = rune::client::testing::allocFreeUdpPort();
    const uint16_t rPort   = rune::client::testing::allocFreeUdpPort();
    Config c = responseConfig(loopback(reqPort), rPort, 3560);
    Transport t{c};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    RawServer srv(c.dataDir, loopback(reqPort), c.ingressStreamId, rPort, c.egressStreamId);
    const uint8_t frame[] = {1, 2, 3, 4};
    ASSERT_TRUE(pumpBoth(t, srv, [&] { return t.offer(frame, sizeof frame); })) << "the first pair never connected";
    const int64_t oldReg  = t.seedResponseSubscriptionIdForTest();
    const int32_t oldSlot = t.seedResponseSlotForTest();
    ASSERT_NE(oldReg, 0);

    ASSERT_TRUE(t.rePairSeedResponse(rPort));
    // Returned at once: the conductor was not pumped, so nothing has resolved and the OLD pair is still in force.
    EXPECT_TRUE(t.seedRePairingForTest());
    EXPECT_EQ(t.seedResponseSubscriptionIdForTest(), oldReg) << "the call must not complete the re-pair itself";
    EXPECT_EQ(t.seedResponseSlotForTest(), oldSlot);
    EXPECT_TRUE(t.rePairSeedResponse(rPort)) << "one re-pair in flight: a second call is a no-op";

    bool sent = false;
    for (int i = 0; i < 200000 && !sent; ++i) {   // hard cap
        t.dutyCycle();
        srv.pump();
        sent = t.offer(frame, sizeof frame) && t.seedResponseSubscriptionIdForTest() != oldReg;
    }
    ASSERT_TRUE(sent) << "the asynchronous re-pair never completed";
    EXPECT_FALSE(t.seedRePairingForTest());
    EXPECT_NE(t.seedResponseSubscriptionIdForTest(), oldReg);
    for (int i = 0; i < 5; ++i) { t.dutyCycle(); t.poll([](const uint8_t*, std::size_t) {}, 10); }   // reap pass
    EXPECT_FALSE(t.subSlotLiveForTest(oldSlot) && t.seedResponseSlotForTest() == oldSlot)
        << "the spent subscription is retired";
    t.stop();
}

// A correlated publication to the SAME channel as the seed is not the seed's business: the seed re-pair leaves it
// (same publication object, still Ready).
TEST(SeedRePairRegression, RePairLeavesOtherPublicationsAloneRegression) {
    const uint16_t reqPort = rune::client::testing::allocFreeUdpPort();
    const uint16_t rPort   = rune::client::testing::allocFreeUdpPort();
    Config c = responseConfig(loopback(reqPort), rPort, 3570);
    Transport t{c};
    t.start();
    ASSERT_TRUE(t.seedResponseCorrelated());
    const std::string gw = loopback(reqPort);
    const auto respCtl = rune::client::transport::responseControlChannelFor(gw, rPort);
    ASSERT_TRUE(respCtl.has_value());
    const Transport::PubHandle h = t.responsePublicationHandle(gw, 3571, *respCtl);
    ASSERT_NE(h, Transport::kNoPub);
    RawServer srv(c.dataDir, gw, 3571, rPort, c.egressStreamId);
    const uint8_t frame[] = {0xC1, 0x02};
    bool sent = false;
    ASSERT_TRUE(pumpBoth(t, srv, [&] { if (!sent) sent = t.offerOn(h, frame, sizeof frame); return sent; }));
    ASSERT_TRUE(t.publicationReady(h));
    const void* const before = t.publicationIdentityForTest(h);
    ASSERT_NE(before, nullptr);
    const int64_t oldReg = t.seedResponseSubscriptionIdForTest();

    ASSERT_TRUE(t.rePairSeedResponse(rPort));
    bool swapped = false;
    for (int i = 0; i < 200000 && !swapped; ++i) {   // hard cap; the seed's request side has no server, so only offer() advances it
        t.dutyCycle();
        srv.pump();
        (void)t.offer(frame, sizeof frame);
        swapped = t.seedResponseSubscriptionIdForTest() != oldReg;
    }
    ASSERT_TRUE(swapped) << "the seed re-pair never completed";
    EXPECT_EQ(t.publicationIdentityForTest(h), before) << "the seed re-pair reset another publication on the same channel";
    EXPECT_TRUE(t.publicationReady(h));
    t.stop();
}
