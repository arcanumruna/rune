// AgentRunnerStopRingTest.cpp — F9a: the agent thread is stopped by a Stop
// COMMAND on a ControlRing (no atomic flag), drained after doWork() and BEFORE
// idling; plus the ControlRing contract itself.
//
// Test-side synchronisation (std::promise/future, atomics in fixtures) is test
// harness code — the production classes under test contain none.
#include <gtest/gtest.h>

#include <rune/consensus/ConsensusAgent.h>  // Agent (before AgentRunner)
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/ControlRing.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>

#include <chrono>
#include <future>
#include <thread>
#include <vector>

using namespace rune::consensus;
using namespace std::chrono_literals;

namespace {

// Counts idle() calls; only ever touched by the agent thread while it runs and
// read by the test after join() (join = happens-before).
struct CountingIdle final : IdleStrategy {
    void idle(int) override { ++idles; }
    void reset() override {}
    int idles = 0;
};

// First doWork() parks until the test has posted stop(); every call is counted.
struct GateAgent final : Agent {
    std::promise<void> entered;
    std::shared_future<void> release;
    int calls = 0;
    explicit GateAgent(std::shared_future<void> r) : release(std::move(r)) {}
    int doWork(int64_t) override {
        if (++calls == 1) {
            entered.set_value();
            release.wait();
        }
        return 0;
    }
};

} // namespace

// The stop command posted WHILE doWork() runs must be seen right after that
// doWork() returns — the agent exits without idling and without another cycle.
// (Agrona's volatile-flag loop idles once more before re-checking the flag.)
TEST(AgentRunnerRegression, StopCommandIsDrainedBeforeIdling) {
    std::promise<void> releaseP;
    GateAgent          agent{releaseP.get_future().share()};
    CountingIdle       idle;
    MonotonicNanoClock clock;

    AgentRunner runner{agent, idle, clock};
    auto entered = agent.entered.get_future();
    runner.start();
    ASSERT_EQ(entered.wait_for(5s), std::future_status::ready);
    runner.stop();             // Stop command is now on the ring
    releaseP.set_value();      // let the first doWork() return
    runner.join();

    EXPECT_EQ(agent.calls, 1) << "no further duty cycle after Stop";
    EXPECT_EQ(idle.idles, 0) << "Stop must be drained before idling";
    EXPECT_FALSE(runner.isRunning());
}

// stop() before start() is a no-op: start() still runs the agent (old flag
// semantics, where start() re-armed the flag), and a later stop() ends it.
TEST(AgentRunnerRegression, StopBeforeStartDoesNotPreventRun) {
    struct CountAgent final : Agent {
        std::promise<void> ran;
        bool signalled = false;
        int doWork(int64_t) override {
            if (!signalled) { signalled = true; ran.set_value(); }
            return 0;
        }
    } agent;
    BackoffIdleStrategy idle;
    MonotonicNanoClock  clock;
    AgentRunner runner{agent, idle, clock};
    runner.stop();
    auto ran = agent.ran.get_future();
    runner.start();
    EXPECT_EQ(ran.wait_for(5s), std::future_status::ready);
    runner.stopAndJoin();
    EXPECT_FALSE(runner.isRunning());
}

// Repeated stop() calls are idempotent (one command) and join returns.
TEST(AgentRunnerRegression, RepeatedStopIsIdempotent) {
    struct NoOp final : Agent { int doWork(int64_t) override { return 0; } } agent;
    BackoffIdleStrategy idle;
    MonotonicNanoClock  clock;
    AgentRunner runner{agent, idle, clock};
    runner.start();
    for (int i = 0; i < 1000; ++i) runner.stop();   // would overflow a 256 B ring if not idempotent
    runner.join();
    SUCCEED();
}

// ---------------------------------------------------------------------------
// ControlRing contract
// ---------------------------------------------------------------------------
TEST(ControlRing, FifoRoundTripAndBodyIntegrity) {
    ControlRing<1024> r;
    ASSERT_TRUE(r.offer(1, 11, -11));
    ASSERT_TRUE(r.offer(2, INT64_MIN, INT64_MAX));
    std::vector<std::tuple<int32_t, int64_t, int64_t>> got;
    EXPECT_EQ(r.poll([&](int32_t t, int64_t a, int64_t b) { got.emplace_back(t, a, b); }), 2);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], std::make_tuple(1, int64_t{11}, int64_t{-11}));
    EXPECT_EQ(got[1], std::make_tuple(2, INT64_MIN, INT64_MAX));
    EXPECT_EQ(r.poll([](int32_t, int64_t, int64_t) {}), 0) << "empty after drain";
}

TEST(ControlRing, FullRingRefusesOfferNeverOverwrites) {
    ControlRing<256> r;   // 256 B / 24 B records = 10 messages
    int accepted = 0;
    while (r.offer(7, accepted, 0)) ++accepted;
    EXPECT_GT(accepted, 0);
    EXPECT_FALSE(r.offer(7, 999, 0));
    int64_t expect = 0;
    r.poll([&](int32_t, int64_t a, int64_t) { EXPECT_EQ(a, expect++); });
    EXPECT_EQ(expect, accepted) << "every accepted message delivered, in order";
    EXPECT_TRUE(r.offer(7, 1, 0)) << "space reclaimed after drain";
}

TEST(ControlRing, ManyProducersOneConsumerNoLoss) {
    ControlRing<4096> r;
    constexpr int kProducers = 4;
    constexpr int kPer       = 5000;
    std::vector<std::thread> ps;
    for (int p = 0; p < kProducers; ++p) {
        ps.emplace_back([&r, p] {
            for (int i = 0; i < kPer; ++i)
                while (!r.offer(p + 1, i, 0)) std::this_thread::yield();
        });
    }
    std::vector<int64_t> next(kProducers, 0);
    int total = 0;
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (total < kProducers * kPer && std::chrono::steady_clock::now() < deadline) {
        total += r.poll([&](int32_t t, int64_t a, int64_t) {
            EXPECT_EQ(a, next[static_cast<std::size_t>(t - 1)]++) << "per-producer FIFO";
        });
    }
    for (auto& t : ps) t.join();
    EXPECT_EQ(total, kProducers * kPer);
}

// A message that lands right after a wrap (behind a padding record) must be
// delivered by the SAME poll() — agrona's read() stops at the buffer end.
TEST(ControlRingRegression, PollDeliversAcrossTheWrapInOneCall) {
    ControlRing<256> r;   // 24 B records: 10 fit, 16 B tail → padding on wrap
    for (int round = 0; round < 50; ++round) {
        ASSERT_TRUE(r.offer(1, round, 0));
        int got = 0;
        int64_t a = -1;
        EXPECT_EQ(r.poll([&](int32_t, int64_t x, int64_t) { ++got; a = x; }), 1)
            << "round " << round;
        EXPECT_EQ(got, 1);
        EXPECT_EQ(a, round);
    }
}
