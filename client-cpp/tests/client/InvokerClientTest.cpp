// tests/client/InvokerClientTest.cpp — Client in RunnerMode::Invoker against the
// single-threaded InvokerGateway.  The test thread is the only thread: every drive
// loop is iteration-capped and advances an injected clock instead of sleeping.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
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

template <class F>
bool ready(F& f) { return f.wait_for(0s) == std::future_status::ready; }

struct Rig {
    rc::Client                   client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId,
                        c.egressChannel, c.egressStreamId) {}

    template <class P>
    bool drive(P&& pred, int cap = 200'000, int64_t stepNs = 0) {
        for (int i = 0; i < cap; ++i) {
            g_fakeNow += stepNs;
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }

    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = client.connect();
        ASSERT_TRUE(drive([&] { return ready(f); }));
        f.get();
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
    auto fut = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                      rc::want_future);
    ASSERT_TRUE(r.drive([&] { return ready(fut); }));
    EXPECT_EQ(fut.get(), 100);
}

TEST(InvokerClient, NothingProgressesUnlessTheOwnerCallsDoWork) {
    Rig r(invokerCfg(3702, 3703));
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto f = r.client.connect();
    for (int i = 0; i < 10'000; ++i) r.gw.pump();
    EXPECT_EQ(r.gw.connects, 0) << "no background runner may drain the connect command";
    ASSERT_TRUE(r.drive([&] { return ready(f); }));
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
    auto fut = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                      rc::want_future);
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; }));
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 2; }, 1'000, 1'000'000))
        << "retransmit after produceTimeoutMs of INJECTED time";
    ASSERT_TRUE(r.drive([&] { return ready(fut); }, 1'000, 1'000'000));
    EXPECT_THROW(fut.get(), std::runtime_error);
}

// Regression: transactionalProducer() pushed into a mutex-guarded vector that the
// runner locked and copied every duty cycle.  Registration now crosses through the
// agent ring like every other app->runner event.
TEST(InvokerClient, TxnProducerRegistersThroughTheRingRegression) {
    Rig r(invokerCfg(3720, 3721));
    auto tp = r.client.transactionalProducer("tx-1");
    EXPECT_EQ(r.client.testTxnProducerCount(), 0u);
    r.client.doWork();
    EXPECT_EQ(r.client.testTxnProducerCount(), 1u);
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
        auto fut = p.send(client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                          rc::want_future);
        auto tp    = client.transactionalProducer("tx-close");
        auto initF = tp.initTransactions();
        client.close();
        ASSERT_TRUE(ready(fut)) << "produce future must be failed by close(), not hang";
        EXPECT_THROW(fut.get(), std::runtime_error);
        ASSERT_TRUE(ready(initF)) << "txn initTransactions future must be failed by close()";
        EXPECT_THROW(initF.get(), std::runtime_error);
        // Adoption == the ring holder was consumed and freed (no leak).
        EXPECT_EQ(client.testProducerCount(), 1u);
        EXPECT_EQ(client.testTxnProducerCount(), 1u);
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
    auto fx = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                     rc::want_future);
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; }));
    auto fy = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                     rc::want_future);
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
        for (const auto& pr : r.gw.produced) k += (pr.corrId == corr) ? 1 : 0;
        return k;
    };
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 2 && ready(fx); }));
    ASSERT_TRUE(r.drive([&] { return countCorr(Y.corrId) >= 2; }))
        << "the redirected batch Y is resent after the reconnect";
    EXPECT_EQ(countCorr(X.corrId), 1) << "acked batch X must not be resent";
    EXPECT_EQ(fx.get(), 100);
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
    auto tp = r.client.transactionalProducer("tx-2");
    r.connect();
    ASSERT_EQ(r.client.testTxnProducerCount(), 1u);  // forwardProduceResponses on
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p   = r.client.producer(pcfg);
    auto fut = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                      rc::want_future);
    ASSERT_TRUE(r.drive([&] { return ready(fut); }));
    EXPECT_EQ(fut.get(), 100);
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
    std::future<int64_t> fut;
    std::thread a([&] {
        client = std::make_unique<rc::Client>(c);
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::Leader;
        pcfg.lingerUs = 0;
        auto p = client->producer(pcfg);
        fut = p.send(client->topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                     rc::want_future);
        for (int i = 0; i < 100; ++i) client->doWork();
    });
    a.join();
    ASSERT_TRUE(std::filesystem::exists(c.dataDir + "/aeron"));
    client.reset();  // NOT the owner thread
    ASSERT_TRUE(ready(fut));
    EXPECT_THROW(fut.get(), std::runtime_error);
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
            auto f = client->connect();
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
        EXPECT_THROW((void)client.transactionalProducer("tx-after-close"), std::logic_error);
        EXPECT_EQ(client.testProducerCount(), 0u);
        EXPECT_EQ(client.testTxnProducerCount(), 0u);
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
    auto fx = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                     rc::want_future);
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 2 && !r.gw.produced.empty(); }));
    for (int i = 0; i < 2'000; ++i) { r.client.doWork(); r.gw.pump(); }  // settle
    const auto X = r.gw.produced.front();
    auto countCorr = [&](int64_t corr) {
        int k = 0;
        for (const auto& pr : r.gw.produced) k += (pr.corrId == corr) ? 1 : 0;
        return k;
    };
    const int base = countCorr(X.corrId);
    ASSERT_GE(base, 1);
    r.gw.held.clear();

    // A second leader session connects; X's ack precedes its ConnectResponse.
    r.gw.ackBeforeNextConnect.push_back(X);
    r.client.testSetRoute(1, 0, "aeron:ipc?alias=leader2");
    auto fy = p.send(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(kVal),
                     rc::want_future);
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 3 && ready(fx); }));
    for (int i = 0; i < 2'000; ++i) { r.client.doWork(); r.gw.pump(); }  // let any resend land
    EXPECT_EQ(countCorr(X.corrId), base) << "acked batch X must not be re-driven by the resend fn";
    EXPECT_EQ(fx.get(), 100);
}

// ---------------------------------------------------------------------------
// Task 13: void send() never drops. In Invoker mode the owner thread IS the
// producer ring's only consumer, so a blocking send() must not wait on a
// consumer that is itself — Client::producer() wires ProducerImpl's pump hook
// (setPumpFn) to this Client for exactly that reason. See also
// ProducerSendTest.cpp for the hook's gating logic tested in isolation.
// ---------------------------------------------------------------------------

// Regression: owner thread, outside any duty-cycle callback, back-pressured on
// a full producer ring — the blocking send() must pump this Client's OWN duty
// cycle (not wait on the heartbeat of a consumer that is this very thread) and
// succeed, with no runner thread ever existing.
TEST(InvokerClient, BlockingSendPumpsOwnDutyCycleWhenBackPressuredRegression) {
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
TEST(InvokerClient, BlockingSendFromDutyCycleCallbackThrowsRegression) {
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
