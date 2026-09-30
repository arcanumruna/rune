// tests/client/InvokerClientTest.cpp — Client in RunnerMode::Invoker against the
// single-threaded InvokerGateway.  The test thread is the only thread: every drive
// loop is iteration-capped and advances an injected clock instead of sleeping.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/detail/CpuAffinity.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
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
