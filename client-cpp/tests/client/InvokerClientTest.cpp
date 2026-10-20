// tests/client/InvokerClientTest.cpp — Client in RunnerMode::Invoker against the
// single-threaded InvokerGateway.  The test thread is the only thread: every drive
// loop is iteration-capped and advances an injected clock instead of sleeping.
#include "../support/AckLedger.h"
#include "../support/TestCompletions.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/detail/CpuAffinity.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <future>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

std::atomic<int> g_ctr{0};
int64_t          g_fakeNow = 0;  // test thread only
int64_t fakeNow() noexcept { return g_fakeNow; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// fix2 I4: a SEPARATE, genuinely thread-safe injected clock for the two tests
// below, which (unlike every other test in this file) have a real second
// thread (an app thread blocked in sendBlocking) reading the clock
// concurrently with the owner thread advancing it — g_fakeNow/fakeNow() above
// are "test thread only" and would be a real data race if shared across
// threads.
std::atomic<int64_t> g_fakeNowMt{1};
int64_t fakeNowMt() noexcept { return g_fakeNowMt.load(std::memory_order_relaxed); }

/// A transactional producer's config (Phase 2b F: a Producer with a transactionalId; acks=all as before).
rc::ProducerConfig txnPcfg(std::string id) {
    rc::ProducerConfig c;
    c.acks            = rc::Acks::All;
    c.transactionalId = std::move(id);
    return c;
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/inv_client_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}


struct Rig {
    rc::Client                   client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId,
                        c.egressChannel, c.egressStreamId) {}

    template <class P>
    // cap < 0 (default): drive until pred holds — real Aeron work (connects,
    // IPC images) needs real progress, not a CPU-speed-dependent iteration
    // count; the 10 s deadline only bounds a FAILING test.  An explicit cap is
    // a deliberate "within N cycles" bound and stays strict.
    bool drive(P&& pred, int cap = -1, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int i = 0; cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap; ++i) {
            g_fakeNow += stepNs;
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }

    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
};

const std::vector<uint8_t> kVal{1, 2, 3};

} // namespace

TEST(InvokerClient, ConnectsAndProducesWithNoThreads) {
    Rig r(invokerCfg(3700, 3701));
    r.connect();
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    auto fut = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(fut.accepted());
    ASSERT_TRUE(r.drive([&] { return fut.ready(); }));
    EXPECT_EQ(fut.offset(), 100);
}

TEST(InvokerClient, NothingProgressesUnlessTheOwnerCallsDoWork) {
    Rig r(invokerCfg(3702, 3703));
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(f.accepted());
    for (int i = 0; i < 10'000; ++i) r.gw.pump();
    EXPECT_EQ(r.gw.connects, 0) << "no background runner may drain the connect command";
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
}

TEST(InvokerClient, InjectedClockDrivesRetransmitAndDeliveryTimeout) {
    rc::ClientConfig c = invokerCfg(3710, 3711);
    g_fakeNow   = realNow();
    c.nanoClock = &fakeNow;
    Rig r(c);
    r.connect();
    r.gw.replyAcks = false;
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    pcfg.produceTimeoutMs = 50;
    pcfg.deliveryTimeoutMs = 120;
    auto p = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    auto fut = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(fut.accepted());
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; }));
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 2; }, 1'000, 1'000'000))
        << "retransmit after produceTimeoutMs of INJECTED time";
    ASSERT_TRUE(r.drive([&] { return fut.ready(); }, 1'000, 1'000'000));
    EXPECT_EQ(fut.status(), rc::OpStatus::DeliveryFailed);
}

// Regression: a transactional producer's registration crosses through the agent ring like every other
// app->runner event (it used to be a mutex-guarded vector the runner locked and copied every duty cycle).
TEST(InvokerClient, TxnProducerRegistersThroughTheRingRegression) {
    Rig r(invokerCfg(3720, 3721));
    auto tp = r.client.producer(txnPcfg("tx-1"));
    EXPECT_EQ(r.client.testProducerCount(), 0u);
    r.client.doWork();
    EXPECT_EQ(r.client.testProducerCount(), 1u);
}

TEST(InvokerClient, DoWorkIsRejectedInThreadMode) {
    rc::ClientConfig c = invokerCfg(3730, 3731);
    c.runnerMode = rc::RunnerMode::Thread;
    rc::Client client(c);
    EXPECT_THROW(client.doWork(), std::logic_error);
}

TEST(CpuAffinity, PinsTheCallingThreadOnLinux) {
#if defined(__linux__)
    bool pinned = false;
    int  core   = -1;
    std::thread t([&] {
        pinned = rune::client::detail::pinCurrentThreadToCore(0);
        core   = rune::client::detail::currentCore();
    });
    t.join();
    EXPECT_TRUE(pinned);
    EXPECT_EQ(core, 0);
#else
    EXPECT_FALSE(rune::client::detail::pinCurrentThreadToCore(0));
    GTEST_SKIP() << "thread affinity is unsupported on this OS (macOS)";
#endif
}

// ---------------------------------------------------------------------------
// Task 12 fix round 1
// ---------------------------------------------------------------------------

// I1: a producer / txn producer whose registration is still in the agent ring
// when close() runs must be adopted (holder freed) and then failed — never hang.
TEST(InvokerClient, CloseFailsUnadoptedProducerRegression) {
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(mode == rc::RunnerMode::Invoker ? "Invoker" : "Thread");
        rc::ClientConfig c = invokerCfg(mode == rc::RunnerMode::Invoker ? 3740 : 3742,
                                        mode == rc::RunnerMode::Invoker ? 3741 : 3743);
        c.runnerMode = mode;
        rc::Client client(c);
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::Leader;
        pcfg.lingerUs = 0;
        auto p = client.producer(pcfg);
        test_support::TestCq cq(client);
        auto fut = cq.send(p, client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
        ASSERT_TRUE(fut.accepted());
        auto tp    = client.producer(txnPcfg("tx-close"));
        auto initF = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
        ASSERT_TRUE(initF.accepted()) << initF.describe();
        client.closeImmediately();  // F8c C7: the discarding close (close() would drain first)
        ASSERT_TRUE(fut.ready()) << "the produce token must be completed by close(), not hang";
        EXPECT_EQ(fut.status(), rc::OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(fut.token()), 1);
        ASSERT_TRUE(initF.ready()) << "txn initTransactions must be completed by close()";
        EXPECT_EQ(initF.status(), rc::OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(initF.token()), 1);
        // Adoption == the ring registration was consumed (F8c C7: then handed
        // back to its handle — no shared_ptr holder, nothing leaked).
        EXPECT_EQ(client.testImplsHandedBack(), 2u);
        EXPECT_EQ(client.testProducerCount(), 0u);
    }
}

// I2: a registration retry must never pump the duty cycle from inside a duty-cycle
// callback (re-entrant agent/transport poll) nor from a non-owner thread.
TEST(InvokerClient, RegistrationFromCallbackWithFullRingFailsFastRegression) {
    Rig r(invokerCfg(3750, 3751));
    bool inCallbackThrew = false;
    r.client.onConnected([&](int64_t) {
        r.client.testFillCommandRing();
        try {
            (void)r.client.producer();
        } catch (const std::logic_error&) {
            inCallbackThrew = true;
        }
    });
    r.connect();
    EXPECT_TRUE(inCallbackThrew);

    ASSERT_TRUE(r.drive([&] { return r.client.testProducerCount() == 0; }, 1'000));
    r.client.testFillCommandRing();
    bool otherThreadThrew = false;
    std::thread t([&] {
        try {
            (void)r.client.producer();
        } catch (const std::logic_error&) {
            otherThreadThrew = true;
        }
    });
    t.join();
    EXPECT_TRUE(otherThreadThrew);
}

// I3: the owner-thread contract is enforced in release builds too.
TEST(InvokerClient, DoWorkFromNonOwnerThreadThrowsRegression) {
    Rig r(invokerCfg(3752, 3753));
    bool threw = false;
    std::thread t([&] {
        try {
            r.client.doWork();
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    t.join();
    EXPECT_TRUE(threw);
}

// M5: close() from a duty-cycle callback would tear the agent down under its own
// poll — rejected with logic_error, and the client keeps working.
TEST(InvokerClient, CloseFromCallbackThrowsRegression) {
    Rig r(invokerCfg(3754, 3755));
    bool threw = false;
    r.client.onConnected([&](int64_t) {
        try {
            r.client.close();
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    r.connect();
    EXPECT_TRUE(threw);
    EXPECT_EQ(r.client.state(), rune::client::session::SessionState::Connected);
    EXPECT_FALSE(r.client.transportClosed());
}

// I4: a ProduceResponse for X and a (redirect-reconnect) ConnectResponse in ONE
// poll: X's ack must reach the hub before the resend path runs, so X is not resent.
TEST(InvokerClient, AckThenConnectResponseInOnePollDoesNotResendRegression) {
    Rig r(invokerCfg(3760, 3761));
    r.connect();
    r.gw.replyAcks = false;
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p  = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    auto fx = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; }));
    auto fy = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 2; }));
    const auto X = r.gw.produced[0];
    const auto Y = r.gw.produced[1];
    ASSERT_NE(X.corrId, Y.corrId);
    r.gw.held.clear();
    r.gw.ackBeforeNextConnect.push_back(X);

    namespace rcc = rune::client::codec;
    uint8_t buf[256];
    const std::size_t n = rcc::encodeErrorResponse(
        buf, sizeof buf, static_cast<int16_t>(rcc::ErrorCode::Redirect), "aeron:ipc",
        r.gw.assignedSessionId, Y.corrId);
    ASSERT_GT(n, 0u);
    r.gw.send(0, buf, n);

    auto countCorr = [&](int64_t corr) {
        int k = 0;
        // EOS follow-ups I-2: a re-send is a new attempt of the same batch id.
        namespace bc = rune::client::producer::batchcorr;
        for (const auto& pr : r.gw.produced) k += (bc::base(pr.corrId) == bc::base(corr)) ? 1 : 0;
        return k;
    };
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 2 && fx.ready(); }));
    ASSERT_TRUE(r.drive([&] { return countCorr(Y.corrId) >= 2; }))
        << "the redirected batch Y is resent after the reconnect";
    EXPECT_EQ(countCorr(X.corrId), 1) << "acked batch X must not be resent";
    EXPECT_EQ(fx.offset(), 100);
}

// I5 coverage.
TEST(InvokerClient, DoWorkReentryReturnsZeroRegression) {
    Rig r(invokerCfg(3762, 3763));
    bool called = false;
    int  nested = -1;
    r.client.onConnected([&](int64_t) {
        called = true;
        nested = r.client.doWork();
    });
    r.connect();
    EXPECT_TRUE(called);
    EXPECT_EQ(nested, 0);
}

TEST(InvokerClient, InvokerRingFullRetryPumpsRegression) {
    Rig r(invokerCfg(3764, 3765));
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    r.client.testFillCommandRing();
    // No runner thread exists: the only way this registration lands is the retry
    // loop running the duty cycle (which drains the ring) on this thread.
    auto p = r.client.producer();
    ASSERT_TRUE(r.drive([&] { return r.client.testProducerCount() == 1; }, 1'000));
}

TEST(InvokerClient, TxnAndRegularProducerAcksBothRouteRegression) {
    Rig r(invokerCfg(3766, 3767));
    auto tp = r.client.producer(txnPcfg("tx-2"));
    r.connect();
    ASSERT_EQ(r.client.testProducerCount(), 1u);
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p   = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    auto fut = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(r.drive([&] { return fut.ready(); }));
    EXPECT_EQ(fut.offset(), 100);
}

// ---------------------------------------------------------------------------
// Task 12 fix round 2
// ---------------------------------------------------------------------------

// ~Client must never throw: an Invoker client built and driven on a worker thread,
// joined, then destroyed on another thread tears down (futures failed, driver
// stopped — its dir_delete_on_shutdown removes the aeron dir) with no terminate.
TEST(InvokerClient, DestroyInvokerClientOffOwnerThreadTearsDownRegression) {
    const rc::ClientConfig c = invokerCfg(3770, 3771);
    std::unique_ptr<rc::Client> client;
    test_support::AckLedger     L;  // the ack handler runs on the closing thread (main)
    rc::SendResult              sent = rc::SendResult::Closed;
    std::thread a([&] {
        client = std::make_unique<rc::Client>(c);
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::Leader;
        pcfg.lingerUs = 0;
        auto p = client->producer(pcfg);
        p.onAck(&test_support::AckLedger::onAck, &L);
        sent = p.trySend(client->topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                         rc::AckToken{1});
        for (int i = 0; i < 100; ++i) client->doWork();
    });
    a.join();
    ASSERT_EQ(sent, rc::SendResult::Ok);
    ASSERT_TRUE(std::filesystem::exists(c.dataDir + "/aeron"));
    client.reset();  // NOT the owner thread
    ASSERT_EQ(L.times(1), 1) << "the produce token is completed by the teardown";
    EXPECT_EQ(L.status(1), rc::OpStatus::Closed);
    EXPECT_FALSE(std::filesystem::exists(c.dataDir + "/aeron")) << "embedded driver not stopped";
}

// Destroying the client from inside its own duty-cycle callback is a programming
// error that would otherwise be a silent use-after-free: fail loud.
TEST(InvokerClientDeathTest, DestroyFromCallbackAbortsRegression) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            const rc::ClientConfig c = invokerCfg(3772, 3773);
            auto client = std::make_unique<rc::Client>(c);
            test_support::InvokerGateway gw(c.dataDir + "/aeron", c.gatewayChannel,
                                            c.gatewayStreamId, c.egressChannel,
                                            c.egressStreamId);
            client->onConnected([&](int64_t) { client.reset(); });
            for (int i = 0; i < 200'000 && !gw.ready(); ++i) {
                client->doWork();
                gw.pump();
            }
            test_support::TestCq cq(*client);
            auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
            for (int i = 0; i < 200'000 && client; ++i) {
                client->doWork();
                gw.pump();
            }
        },
        "destroyed from inside its own duty-cycle callback");
}

// Minor 1: registration after close() fails fast (no leak, no 5 s park).
TEST(InvokerClient, ProducerAfterCloseThrowsRegression) {
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        rc::ClientConfig c = invokerCfg(mode == rc::RunnerMode::Invoker ? 3774 : 3776,
                                        mode == rc::RunnerMode::Invoker ? 3775 : 3777);
        c.runnerMode = mode;
        rc::Client client(c);
        client.close();
        EXPECT_THROW((void)client.producer(), std::logic_error);
        EXPECT_THROW((void)client.producer(txnPcfg("tx-after-close")), std::logic_error);
        EXPECT_EQ(client.testProducerCount(), 0u);
    }
}

// Minor 3: the produceResendFn_ (multi-node, a leader session reached Connected)
// path routes acks earlier in the same poll before re-driving in-flight batches.
// "aeron:ipc?alias=leaderN" is a distinct route endpoint (its own session) that
// still reaches the gateway's aeron:ipc subscription.
TEST(InvokerClient, AckThenResendFnInOnePollDoesNotResendRegression) {
    Rig r(invokerCfg(3778, 3779));
    r.connect();
    r.gw.replyAcks = false;
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p = r.client.producer(pcfg);
    r.client.testSetRoute(1, 0, "aeron:ipc?alias=leader1");
    test_support::TestCq cq(r.client);
    auto fx = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 2 && !r.gw.produced.empty(); })) << "connects=" << r.gw.connects << " produced=" << r.gw.produced.size() << " fx=" << fx.describe();
    for (int i = 0; i < 2'000; ++i) { r.client.doWork(); r.gw.pump(); }  // settle
    const auto X = r.gw.produced.front();
    auto countCorr = [&](int64_t corr) {
        int k = 0;
        // EOS follow-ups I-2: a re-send is a new attempt of the same batch id.
        namespace bc = rune::client::producer::batchcorr;
        for (const auto& pr : r.gw.produced) k += (bc::base(pr.corrId) == bc::base(corr)) ? 1 : 0;
        return k;
    };
    const int base = countCorr(X.corrId);
    ASSERT_GE(base, 1);
    r.gw.held.clear();

    // A second leader session connects; X's ack precedes its ConnectResponse.
    r.gw.ackBeforeNextConnect.push_back(X);
    r.client.testSetRoute(1, 0, "aeron:ipc?alias=leader2");
    auto fy = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal));
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 3 && fx.ready(); }))
        << "connects=" << r.gw.connects << " fx=" << fx.describe();
    for (int i = 0; i < 2'000; ++i) { r.client.doWork(); r.gw.pump(); }  // let any resend land
    EXPECT_EQ(countCorr(X.corrId), base) << "acked batch X must not be re-driven by the resend fn";
    EXPECT_EQ(fx.offset(), 100);
}

// ---------------------------------------------------------------------------
// Task 13: void send() never drops — RING-SEAM tests.
//
// Since Task 14, Client::producer() in Invoker mode returns a DIRECT-ingress
// producer (no ring; see tests/producer/DirectIngressTest.cpp for the Direct
// versions of these scenarios).  The tests below deliberately use
// Client::testProducerWithRingCap(), which returns a RING-ingress producer in
// either runner mode, to keep the ring paths covered on an Invoker client: the
// pump hook (setPumpFn) when the owner thread is the ring's only consumer, the
// ring's closed word, and the consumer-heartbeat wait for a foreign app thread.
// They are prefixed "RingSeam" for that reason.  See also ProducerSendTest.cpp
// for the hook's gating logic tested in isolation.
// ---------------------------------------------------------------------------

// Regression: owner thread, outside any duty-cycle callback, back-pressured on
// a full producer ring — the blocking send() must pump this Client's OWN duty
// cycle (not wait on the heartbeat of a consumer that is this very thread) and
// succeed, with no runner thread ever existing.
TEST(InvokerClient, RingSeamBlockingSendPumpsOwnDutyCycleWhenBackPressuredRegression) {
    Rig r(invokerCfg(3780, 3781));
    rc::ProducerConfig pcfg;
    pcfg.acks     = rc::Acks::None;
    pcfg.lingerUs = 0;
    auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);  // small ring: fills fast
    auto topic = r.client.topic("t");

    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u) << "ring never filled";

    // No runner thread exists in Invoker mode: if this blocked on the ring's
    // consumer heartbeat (instead of pumping), it would hang until
    // agentLivenessTimeoutMs (default 1s) and then throw. It must not throw at all.
    EXPECT_NO_THROW(p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal)));

    // The pump actually made progress: another fill-to-backpressure round still
    // succeeds (the ring recovers rather than staying permanently stuck).
    uint64_t ok2 = 0;
    for (; ok2 < 10'000; ++ok2) {
        if (p.trySend(topic, rc::Key(ok2), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    EXPECT_GT(ok2, 0u);
}

// Regression: a blocking send() invoked FROM INSIDE a duty-cycle callback
// (onConnected fires on the duty-cycle thread, with inDutyCycle_ set) must
// fail fast with std::runtime_error, never pump (which would re-enter the
// agent's own poll) and never spin waiting out the liveness timeout.
TEST(InvokerClient, RingSeamBlockingSendFromDutyCycleCallbackThrowsRegression) {
    Rig r(invokerCfg(3782, 3783));
    bool threwInCallback = false;
    bool ranAtAll        = false;
    r.client.onConnected([&](int64_t) {
        ranAtAll = true;
        rc::ProducerConfig pcfg;
        pcfg.acks     = rc::Acks::None;
        pcfg.lingerUs = 0;
        auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);
        auto topic = r.client.topic("t");
        uint64_t ok = 0;
        for (; ok < 10'000; ++ok) {
            if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
        }
        try {
            p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal));
        } catch (const std::runtime_error&) {
            threwInCallback = true;
        }
    });
    r.connect();
    EXPECT_TRUE(ranAtAll);
    EXPECT_TRUE(threwInCallback);
}

// Regression (fix1 C1): same scenario, but the handler does NOT catch. The
// process must survive (Client::invokeUserCallback_ catches around the
// callback dispatch), and the exception count must reflect it.
TEST(InvokerClient, RingSeamBlockingSendFromCallbackDoesNotTerminateInvokerRegression) {
    Rig r(invokerCfg(3784, 3785));
    bool ran = false;
    r.client.onConnected([&](int64_t) {
        ran = true;
        rc::ProducerConfig pcfg;
        pcfg.acks     = rc::Acks::None;
        pcfg.lingerUs = 0;
        auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);
        auto topic = r.client.topic("t");
        uint64_t ok = 0;
        for (; ok < 10'000; ++ok) {
            if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
        }
        p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal)); // NOT wrapped in try/catch
    });
    r.connect();
    EXPECT_TRUE(ran);
    EXPECT_GT(r.client.testCallbackExceptionCount(), 0u);
    // The duty-cycle thread (the test thread, in Invoker mode) survived: the
    // client is still fully usable (F8c C7: the callback's producer was
    // destroyed with the callback — a new one registers).
    auto again = r.client.producer();
    ASSERT_TRUE(r.drive([&] { return r.client.testProducerCount() >= 1; }));
}

// ---------------------------------------------------------------------------
// Task 13 fix1 C2/I3: a ring "closed" word, so a Producer that outlives (or
// races) its Client's close() gets Closed, never a use-after-free / silent
// accept.
// ---------------------------------------------------------------------------

TEST(InvokerClient, RingSeamSendAfterClientCloseReturnsClosedRegression) {
    Rig r(invokerCfg(3786, 3787));
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::None;
    auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);
    auto topic = r.client.topic("t");
    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u);

    r.client.closeImmediately();  // C7: teardown semantics (close() drains)

    EXPECT_EQ(p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)), rc::SendResult::Closed);
    EXPECT_THROW(p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal)), std::logic_error);
}

// Destroy the Client entirely (not just close()) while a Producer outlives it,
// then exercise the full back-pressure path (trySend to fill, blocking send()
// to retry). trySubmit's closed-word check intercepts before sendBlocking ever
// touches the pump hook's context (the destroyed Client) — no UAF. No ASan
// build exists in this repo; this relies on TSan (run separately) plus a clean
// pass here for the use-after-free proof.
TEST(InvokerClient, RingSeamProducerOutlivesClientNoUseAfterFreeRegression) {
    auto cfg = invokerCfg(3788, 3789);
    auto client = std::make_unique<rc::Client>(cfg);
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::None;
    auto p = client->testProducerWithRingCap(pcfg, 4 * 1024);
    auto topic = client->topic("t");
    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u);

    client.reset(); // the Producer (shared_ptr<ProducerImpl>) survives

    EXPECT_EQ(p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)), rc::SendResult::Closed);
    EXPECT_THROW(p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal)), std::logic_error);
}

// ---------------------------------------------------------------------------
// fix2 I4: clock coherence — the heartbeat's writer (the duty cycle, via
// cfg_.nanoClock) and its reader (sendBlocking's liveness check, via
// ProducerImpl::clock_, wired by Client::producer()/testProducerWithRingCap()
// calling setClockForTesting(cfg_.nanoClock)) must agree on "now". Without
// that wiring, sendBlocking would fall back to a real steady_clock read
// against a heartbeat published on an unrelated injected timeline, producing
// a spurious immediate throw even while the runner is actively draining.
// Both tests use a thread-safe injected clock (fakeNowMt/g_fakeNowMt) deliberately
// far from real steady_clock's epoch, so a wiring regression (falling back to
// steady_clock on either side) would misbehave immediately and obviously.
// ---------------------------------------------------------------------------

// fix4: no sleep_for anywhere (tests never sleep/spin to pass time). The
// fix3 version held off draining for 150ms of REAL wall-clock time to force
// a real reader/writer gap — effective, but a forbidden sleep.
//
// Redesigned to need NO real-time gap at all: agentLivenessTimeoutMs is set
// to 1 — the smallest POSITIVE unit the ms-granularity config allows. (0 was
// tried first: it works for the "stays frozen, never throws" phase below, but
// then makes the later "finish draining" phase itself flaky — the instant the
// clock resumes advancing, there is an inherent, unavoidable microseconds-
// scale window between advancing it and this test's own doWork() call
// republishing the heartbeat at the new value, and a ZERO-tolerance
// threshold has no slack for that window even in CORRECTLY wired code. 1 ms
// is ample slack for that real-world microseconds-scale window while still
// being far smaller than any realistic real-steady_clock fallback delay.)
// With the injected clock FROZEN and a FRESH heartbeat just published at that
// same frozen value, a correctly-wired reader's gap is EXACTLY 0 forever
// (`0 > 1ms-in-ns` is false, so it never throws, no matter how many times or
// how long it is checked). A reader that wrongly fell back to real
// steady_clock reads real elapsed time directly: real time reliably exceeds
// 1 ms within a handful of this producer's BackoffIdleStrategy retries (spin
// -> yield -> park, capped at 100 us/park), so it throws quickly. The test's
// own wait is bounded by a plain iteration cap (yields, no sleep) — ample
// either way.
TEST(InvokerClient, RingSeamHeartbeatWriterAndReaderAgreeNoSpuriousThrowWhileDrainingRegression) {
    rc::ClientConfig c = invokerCfg(3790, 3791);
    g_fakeNowMt.store(1, std::memory_order_relaxed);
    c.nanoClock = &fakeNowMt;
    Rig r(c);

    rc::ProducerConfig pcfg;
    pcfg.acks                   = rc::Acks::None;
    pcfg.lingerUs               = 0;
    pcfg.agentLivenessTimeoutMs = 1; // smallest positive unit: see the comment above
                                     // (phase 2 below steps the clock 500 us: a 2x margin)
    auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);
    auto topic = r.client.topic("t");

    // Adopt the registration and publish a FRESH heartbeat at the current
    // (frozen) fake-clock value — ProducerHub::drainAndFlush publishes a
    // heartbeat for every registered producer every cycle, even with nothing
    // to drain yet.
    ASSERT_TRUE(r.drive([&] { return r.client.testProducerCount() >= 1; }));

    // Fill to back-pressure.
    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u);

    std::atomic<bool> threw{false};
    std::atomic<bool> succeeded{false};
    // C-F22: catch EVERYTHING and keep the message — an exception escaping this
    // thread used to std::terminate the whole binary ("Subprocess aborted", no
    // output) instead of failing this test with a diagnosis.
    std::string what;   // written only by appThread, read after join()
    std::thread appThread([&] {
        try {
            p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal));
            succeeded.store(true, std::memory_order_release);
        } catch (const std::exception& e) {
            what = e.what();
            threw.store(true, std::memory_order_release);
        } catch (...) {
            what = "non-std exception";
            threw.store(true, std::memory_order_release);
        }
    });

    // The injected clock is NEVER touched here (stays frozen) and nothing
    // drains the ring — bounded purely by an iteration cap (yields, not a
    // sleep): a wrongly-wired reader throws within the first couple of its
    // own retry iterations, so this cap only needs to be "long enough to let
    // appThread actually run a few times", not "long enough to pass a real
    // time threshold".
    for (int i = 0; i < 100'000 &&
                    !threw.load(std::memory_order_acquire) &&
                    !succeeded.load(std::memory_order_acquire);
         ++i) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(threw.load(std::memory_order_acquire))
        << "spurious timeout: the heartbeat reader is not using the injected (frozen) clock "
           "(fell back to real steady_clock)";
    EXPECT_FALSE(succeeded.load(std::memory_order_acquire))
        << "nothing drains the ring above — a success here is unexpected regardless of wiring";

    // Now actually let it succeed (drain for real), so appThread can be
    // joined cleanly. fix5 minor 4: step the injected clock by 500 us, HALF
    // the 1 ms liveness window. Each doWork() republishes the heartbeat at
    // the just-advanced value, so the largest gap the reader can ever observe
    // (it reads the clock between this fetch_add and the doWork() that
    // follows) is one step = 500 us < 1 ms — a real 2x margin. (A 1 ms step
    // made that worst-case gap EQUAL to the window, passing only because the
    // check is a strict `>`.)
    // C-F22: drain until the app thread finishes — bounded by progress, with a
    // real-time hang guard only for a FAILING run (a fixed 2000-iteration cap
    // could end before a starved app thread ran, leaving it blocked on a frozen
    // clock).  On a hang, close() first so the blocked sender exits (Closed →
    // logic_error, caught above) and join() can never wait forever.
    const auto hangGuard = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!succeeded.load(std::memory_order_acquire) &&
           !threw.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < hangGuard) {
        g_fakeNowMt.fetch_add(500'000, std::memory_order_relaxed); // +500us injected
        r.client.doWork();
        r.gw.pump();
    }
    const bool hung = !succeeded.load(std::memory_order_acquire) &&
                      !threw.load(std::memory_order_acquire);
    if (hung) r.client.close();
    appThread.join();

    EXPECT_FALSE(hung) << "app thread never completed while the duty cycle drained (hang guard)";
    EXPECT_TRUE(succeeded.load()) << "send failed: " << what;
    EXPECT_FALSE(threw.load()) << "spurious failure in send(): " << what;
}

// fix3 I2: a deliberately large (60 s) liveness window that the injected
// clock crosses in a fraction of a real second (pure atomic increments, no
// sleep) but a real steady_clock fallback could only cross by actually
// waiting 60 REAL seconds — so a wiring regression is detected.
//
// fix5 Important 1: the fix3/fix4 version stopped advancing the injected
// clock at a 120 s injected "safety stop" — under load the app thread could
// be descheduled long enough for the advancing loop to hit that stop before
// sendBlocking ever re-read the clock, after which nothing advanced the clock
// again and the test HUNG in join(). Now the clock keeps advancing until the
// app thread's future is ready; the ONLY bound is a real-time hang guard
// (wait_for(100 us) per iteration, 5 s total) — the one real-time bound this
// suite permits, never needed in a passing run (a correctly-wired reader
// throws as soon as it next reads an injected time 60 s past its start). On
// failure, close() runs BEFORE join() so the blocked sender exits promptly
// through the ring's closed word (SendResult::Closed -> std::logic_error)
// instead of waiting out 60 real seconds; the thread catches everything so a
// logic_error can never terminate the process.
TEST(InvokerClient, RingSeamHeartbeatTimeoutFiresWhenRunnerIsWedgedWithInjectedClockRegression) {
    rc::ClientConfig c = invokerCfg(3792, 3793);
    g_fakeNowMt.store(1, std::memory_order_relaxed);
    c.nanoClock = &fakeNowMt;
    Rig r(c);

    rc::ProducerConfig pcfg;
    pcfg.acks                   = rc::Acks::None;
    pcfg.lingerUs               = 0;
    pcfg.agentLivenessTimeoutMs = 60'000; // only the injected clock can cross this quickly
    auto p = r.client.testProducerWithRingCap(pcfg, 4 * 1024);
    auto topic = r.client.topic("t");
    uint64_t ok = 0;
    for (; ok < 10'000; ++ok) {
        if (p.trySend(topic, rc::Key(ok), std::span<const uint8_t>(kVal)) != rc::SendResult::Ok) break;
    }
    ASSERT_GT(ok, 0u);

    enum class Outcome { Timeout, Succeeded, OtherException };
    std::promise<Outcome> outcomePromise;
    std::future<Outcome>  outcomeFuture = outcomePromise.get_future();
    std::thread appThread([&] {
        Outcome o = Outcome::Succeeded;
        try {
            p.send(topic, rc::Key(ok), std::span<const uint8_t>(kVal));
        } catch (const std::runtime_error&) {
            o = Outcome::Timeout;          // the liveness timeout: expected
        } catch (...) {
            o = Outcome::OtherException;   // e.g. logic_error("client closed") after close()
        }
        outcomePromise.set_value(o);
    });

    // The runner NEVER runs here (no doWork() calls — simulating "wedged"):
    // advance the SAME injected clock until the app thread finishes. Real time
    // only bounds the loop as a hang guard (see above).
    const auto hangGuard = std::chrono::steady_clock::now() + 5s;
    bool ready = false;
    while (!(ready = outcomeFuture.wait_for(100us) == std::future_status::ready) &&
           std::chrono::steady_clock::now() < hangGuard) {
        // +100 ms injected per iteration: each iteration also waits (up to)
        // 100 us of real time in wait_for above, so a 1 ms step would need
        // >= 60'000 iterations (>= 6 REAL seconds) to cross the 60 s window —
        // longer than the hang guard itself. 100 ms crosses it in ~600.
        g_fakeNowMt.fetch_add(100'000'000, std::memory_order_relaxed);
    }

    EXPECT_TRUE(ready)
        << "appThread did not finish within the 5 s real-time hang guard — sendBlocking's "
           "reader is likely not using the injected clock (a steady_clock fallback needs 60 "
           "REAL seconds to cross this test's liveness window)";
    if (!ready) r.client.close(); // closed word -> the blocked sender exits now, not in 60 s
    appThread.join();
    if (ready) {
        EXPECT_EQ(outcomeFuture.get(), Outcome::Timeout)
            << "must time out (std::runtime_error) once the injected clock exceeds "
               "agentLivenessTimeoutMs";
    }
}

// ---------------------------------------------------------------------------
// fix5 Important-2(c): Client::close() must terminate even when the agent
// command ring holds an ORPHANED claim (claimed, never committed) in front of
// a committed producer registration — and that registration must still be
// adopted and its ring marked closed. Pre-fix: close() hung forever in
// drainRegistrationsForClose().
// ---------------------------------------------------------------------------
TEST(InvokerClient, CloseTerminatesPastAnOrphanedClaimAndClosesTheRegisteredRingRegression) {
    Rig r(invokerCfg(3794, 3795));

    ASSERT_GE(r.client.testClaimCommandRingWithoutCommitting(8), 0); // the orphan
    rc::ProducerConfig pcfg;
    pcfg.acks     = rc::Acks::None;
    pcfg.lingerUs = 0;
    auto p = r.client.producer(pcfg); // registration committed BEHIND the orphan; no doWork()
    auto topic = r.client.topic("t");
    ASSERT_EQ(r.client.testProducerCount(), 0u) << "not adopted yet: nothing drained the ring";
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal)),
              rc::SendResult::Ok);

    r.client.testSetCloseDrainUnblockAfterYields(64); // production default: 1M — same path
    // F8c C7: closeImmediately — the teardown drain under test.  (close()'s
    // producer drain is a command queued BEHIND the orphan like any other.)
    r.client.closeImmediately();                      // must return

    EXPECT_EQ(r.client.testCloseDrainUnblockCalls(), 1);
    EXPECT_EQ(r.client.testImplsHandedBack(), 1u)
        << "the registration behind the orphan is adopted (then handed back to its handle)";
    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{2}), std::span<const uint8_t>(kVal)),
              rc::SendResult::Closed)
        << "the adopted producer's ring is marked closed";
}

// ---------------------------------------------------------------------------
// fix5 (txn symmetry): the registration-vs-close race, forced
// deterministically. close() runs AFTER submitRegistration_'s up-front
// stopped_ fast-fail check but BEFORE the registration reaches the ring (the
// test hook fires in exactly that window), so close()'s drain has already run
// and can never adopt it. Only the post-submit compare_exchange re-check can
// then notice that close() won and self-close the new producer — otherwise it
// silently accepts work that nothing will ever process (sends accepted into a
// dead ring; txn futures that never resolve).
// ---------------------------------------------------------------------------
TEST(InvokerClient, ProducerRegisteredAfterCloseDrainSelfClosesRegression) {
    Rig r(invokerCfg(3796, 3797));
    r.client.testSetBeforeRegistrationSubmitHook([&] { r.client.close(); });
    rc::ProducerConfig pcfg;
    pcfg.acks     = rc::Acks::None;
    pcfg.lingerUs = 0;
    auto p = r.client.producer(pcfg); // close() ran inside, after the fast-fail check
    EXPECT_EQ(r.client.testProducerCount(), 0u) << "the drain ran before the submit";
    EXPECT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal)),
              rc::SendResult::Closed);
}

TEST(InvokerClient, TxnProducerRegisteredAfterCloseDrainSelfClosesRegression) {
    Rig r(invokerCfg(3798, 3799));
    r.client.testSetBeforeRegistrationSubmitHook([&] { r.client.close(); });
    auto tp = r.client.producer(txnPcfg("tx-race")); // close() ran inside
    EXPECT_EQ(r.client.testProducerCount(), 0u) << "the drain ran before the submit";

    // Every operation must report the close promptly — never hang, never
    // silently accept: rejected synchronously (no completion will follow).
    int calls = 0;
    const rc::CompletionHandler h{[](void* c, const rc::Completion&, std::span<const uint8_t>) noexcept {
                                      ++*static_cast<int*>(c);
                                  },
                                  &calls};
    EXPECT_EQ(tp.initTransactions(h, 1), rc::OpResult::Closed);
    EXPECT_THROW(tp.beginTransaction(), std::logic_error);   // the void begin throws on a closed producer
    EXPECT_THROW(tp.send(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal)),
                 std::logic_error);
    EXPECT_EQ(tp.sendOffsetsToTransaction({}, "g", h, 2), rc::OpResult::Closed);
    EXPECT_EQ(tp.commitTransaction(h, 3), rc::OpResult::Closed);
    EXPECT_EQ(tp.abortTransaction(h, 4), rc::OpResult::Closed);
    EXPECT_THROW(tp.commitTransactionAndWait(1s), std::runtime_error);
    EXPECT_EQ(calls, 0);
}

// The ADOPTED path is symmetric too: a txn producer that close() adopted and
// failed must also fail every LATER operation promptly (Producer gets
// SendResult::Closed via its ring's closed word; the txn producer had no such
// state, so a post-close commitTransaction() future hung forever).
TEST(InvokerClient, TxnProducerOpsAfterCloseFailPromptlyRegression) {
    Rig r(invokerCfg(3802, 3803));
    auto tp = r.client.producer(txnPcfg("tx-adopted"));
    r.client.close();
    EXPECT_EQ(r.client.testImplsHandedBack(), 1u) << "adopted by close()'s drain (then handed back)";
    int calls = 0;
    const rc::CompletionHandler h{[](void* c, const rc::Completion&, std::span<const uint8_t>) noexcept {
                                      ++*static_cast<int*>(c);
                                  },
                                  &calls};
    EXPECT_EQ(tp.commitTransaction(h, 1), rc::OpResult::Closed) << "must be rejected at once, not wait forever";
    EXPECT_EQ(calls, 0);
    EXPECT_THROW(tp.send(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal)),
                 std::logic_error);
}
