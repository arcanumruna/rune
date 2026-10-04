// tests/transport/InvokerTransportTest.cpp — AeronTransport in DriverMode::Invoker.
// Every drive loop is bounded by a 10 s failure deadline and needs no sleep: the
// test thread IS the driver.
#include <rune/client/transport/AeronTransport.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)

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

TEST(InvokerTransportWakeRegression, ParkedOwnerWakesOnInboundDatagram) {
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
